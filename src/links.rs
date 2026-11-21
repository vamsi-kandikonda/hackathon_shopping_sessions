use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::hash::{hash_bytes, prefetch_read};
use crate::io_chunks::{
    MappedFile, for_each_record, list_csv_files, plan_chunks,
};

const LINK_SHARDS: usize = 64;

/// Immutable canonical name storage for all linked components (`0..num_components`).
pub struct CanonNames {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl CanonNames {
    pub fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            offsets: vec![0],
        }
    }

    #[inline(always)]
    pub fn get(&self, comp_id: u32) -> &[u8] {
        let i = comp_id as usize;
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        &self.bytes[s..e]
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct LinkSlot {
    uh: u64,
    comp_id: u32,
    _pad: u32,
}

pub(crate) struct LinkShard {
    slots: Vec<LinkSlot>,
    mask: usize,
}

impl LinkShard {
    fn empty() -> Self {
        Self {
            slots: Vec::new(),
            mask: 0,
        }
    }
}

const BLOOM_WORDS: usize = 262_144; // 2 MB = 16,777,216 bits
const BLOOM_MASK: usize = (BLOOM_WORDS * 64) - 1;

/// 64-shard read-only open-addressed lookup table: linked `user_id` hash -> `comp_id: u32`.
pub struct LinkLookup {
    bloom: Vec<u64>,
    shards: Vec<LinkShard>,
    empty: bool,
}

const MAX_BATCH: usize = 64;

impl LinkLookup {
    pub fn empty() -> Self {
        Self {
            bloom: Vec::new(),
            shards: Vec::new(),
            empty: true,
        }
    }

    pub fn new(shards: Vec<LinkShard>, bloom: Vec<u64>) -> Self {
        let empty = shards.is_empty() || shards.iter().all(|s| s.slots.is_empty());
        Self { bloom, shards, empty }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    #[inline(always)]
    pub fn may_contain(&self, h64: u64) -> bool {
        if self.empty {
            return false;
        }
        let h1 = (h64 as usize) & BLOOM_MASK;
        let b1 = unsafe { *self.bloom.get_unchecked(h1 >> 6) };
        if ((b1 >> (h1 & 63)) & 1) == 0 {
            return false;
        }
        let h2 = ((h64 >> 32) as usize) & BLOOM_MASK;
        let b2 = unsafe { *self.bloom.get_unchecked(h2 >> 6) };
        if ((b2 >> (h2 & 63)) & 1) == 0 {
            return false;
        }
        let h3 = (h64.rotate_left(16) as usize) & BLOOM_MASK;
        let b3 = unsafe { *self.bloom.get_unchecked(h3 >> 6) };
        ((b3 >> (h3 & 63)) & 1) != 0
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn lookup(&self, _uid: &[u8], h64: u64) -> Option<u32> {
        self.lookup_hash(h64)
    }

    #[inline(always)]
    pub fn lookup_hash(&self, h64: u64) -> Option<u32> {
        if !self.may_contain(h64) {
            return None;
        }
        let sh = unsafe { self.shards.get_unchecked((h64 >> 58) as usize) };
        if sh.slots.is_empty() {
            return None;
        }
        let mut idx = (h64 as usize) & sh.mask;
        loop {
            let slot = unsafe { *sh.slots.get_unchecked(idx) };
            if slot.comp_id == u32::MAX {
                return None;
            }
            if slot.uh == h64 {
                return Some(slot.comp_id);
            }
            idx = (idx + 1) & sh.mask;
        }
    }

    #[inline(always)]
    pub fn lookup_batch_hashes(&self, hs: &[u64], out: &mut [Option<u32>]) {
        let n = hs.len();
        debug_assert!(n <= MAX_BATCH && out.len() >= n);
        if self.empty {
            out[..n].fill(None);
            return;
        }
        for i in 0..n {
            let h = hs[i];
            if self.may_contain(h) {
                let sh = unsafe { self.shards.get_unchecked((h >> 58) as usize) };
                if !sh.slots.is_empty() {
                    let idx = (h as usize) & sh.mask;
                    prefetch_read(unsafe { sh.slots.as_ptr().add(idx) });
                }
            }
        }
        for i in 0..n {
            let h = hs[i];
            if self.may_contain(h) {
                let sh = unsafe { self.shards.get_unchecked((h >> 58) as usize) };
                if sh.slots.is_empty() {
                    out[i] = None;
                } else {
                    let mut idx = (h as usize) & sh.mask;
                    let mut found = None;
                    loop {
                        let slot = unsafe { *sh.slots.get_unchecked(idx) };
                        if slot.comp_id == u32::MAX {
                            break;
                        }
                        if slot.uh == h {
                            found = Some(slot.comp_id);
                            break;
                        }
                        idx = (idx + 1) & sh.mask;
                    }
                    out[i] = found;
                }
            } else {
                out[i] = None;
            }
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn lookup_batch(&self, _uids: &[&[u8]], hs: &[u64], out: &mut [Option<u32>]) {
        self.lookup_batch_hashes(hs, out);
    }
}

struct Dsu {
    parent: Vec<u32>,
    rank: Vec<u8>,
}

impl Dsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as u32).collect(),
            rank: vec![0u8; n],
        }
    }

    #[inline(always)]
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }

    #[inline(always)]
    fn union(&mut self, a: u32, b: u32) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        let rka = self.rank[ra as usize];
        let rkb = self.rank[rb as usize];
        if rka < rkb {
            self.parent[ra as usize] = rb;
        } else if rka > rkb {
            self.parent[rb as usize] = ra;
        } else {
            self.parent[rb as usize] = ra;
            self.rank[ra as usize] = rka + 1;
        }
    }
}

#[derive(Clone, Copy)]
struct MappedSpan {
    pref0: u64,
    pref1: u64,
    file_idx: u16,
    len: u16,
    off: u32,
}

const EMPTY_SPAN: MappedSpan = MappedSpan {
    pref0: 0,
    pref1: 0,
    file_idx: 0,
    len: 0,
    off: 0,
};

impl MappedSpan {
    #[inline(always)]
    fn new(file_idx: u16, len: usize, off: u32, slice: &[u8]) -> Self {
        let mut pref0 = [0u8; 8];
        let mut pref1 = [0u8; 8];
        let c0 = len.min(8);
        pref0[..c0].copy_from_slice(&slice[..c0]);
        if len > 8 {
            let c1 = (len - 8).min(8);
            pref1[..c1].copy_from_slice(&slice[8..8 + c1]);
        }
        Self {
            pref0: u64::from_be_bytes(pref0),
            pref1: u64::from_be_bytes(pref1),
            file_idx,
            len: len as u16,
            off,
        }
    }

    #[inline(always)]
    fn slice<'a>(&self, mapped: &'a [MappedFile]) -> &'a [u8] {
        let data = mapped[self.file_idx as usize].as_slice();
        unsafe { data.get_unchecked(self.off as usize..self.off as usize + self.len as usize) }
    }

    #[inline(always)]
    fn is_less(&self, other: &Self, mapped: &[MappedFile]) -> bool {
        if self.pref0 != other.pref0 {
            self.pref0 < other.pref0
        } else if self.pref1 != other.pref1 {
            self.pref1 < other.pref1
        } else if self.len <= 16 && other.len <= 16 {
            self.len < other.len
        } else {
            self.slice(mapped) < other.slice(mapped)
        }
    }
}

#[derive(Clone, Copy)]
struct EndpointRef {
    edge_tag: u32,
    span: MappedSpan,
    h64: u64,
}

pub fn load_links(data_dir: &Path, ncpu: usize) -> io::Result<(LinkLookup, CanonNames)> {
    let files = list_csv_files(&data_dir.join("links"))?;
    if files.is_empty() {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }
    let (mapped, chunks, total_bytes) = plan_chunks(&files, 1024 * 1024)?;
    if chunks.is_empty() || total_bytes == 0 {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }

    let workers = ncpu.min(chunks.len()).max(1);
    let next_chunk = AtomicUsize::new(0);
    let edge_counter = AtomicUsize::new(0);
    let shard_lists: Vec<Mutex<Vec<EndpointRef>>> =
        (0..LINK_SHARDS).map(|_| Mutex::new(Vec::new())).collect();

    let total_edges = {
        let _span = crate::prof::Span::new(crate::prof::LINKS_SCAN);
        thread::scope(|s| {
            for _ in 0..workers {
                let mapped = &mapped;
                let chunks = &chunks;
                let next_chunk = &next_chunk;
                let edge_counter = &edge_counter;
                let shard_lists = &shard_lists;
                s.spawn(move || {
                    const BATCH_SIZE: usize = 4096;
                    let mut batch: [(MappedSpan, u64, MappedSpan, u64); BATCH_SIZE] = [(
                        EMPTY_SPAN,
                        0,
                        EMPTY_SPAN,
                        0,
                    ); BATCH_SIZE];
                    let mut batch_len = 0usize;
                    let mut local_shards: Vec<Vec<EndpointRef>> =
                        (0..LINK_SHARDS).map(|_| Vec::new()).collect();

                    let flush_batch = |batch: &[(MappedSpan, u64, MappedSpan, u64)],
                                           batch_len: usize,
                                           local_shards: &mut [Vec<EndpointRef>]| {
                        if batch_len == 0 {
                            return;
                        }
                        let base = edge_counter.fetch_add(batch_len, Ordering::Relaxed) as u32;
                        for (ei, &(sa, ha, sb, hb)) in batch[..batch_len].iter().enumerate() {
                            let g_edge = base + ei as u32;
                            local_shards[(ha >> 58) as usize].push(EndpointRef {
                                edge_tag: g_edge,
                                span: sa,
                                h64: ha,
                            });
                            local_shards[(hb >> 58) as usize].push(EndpointRef {
                                edge_tag: g_edge | 0x8000_0000,
                                span: sb,
                                h64: hb,
                            });
                        }
                    };

                    loop {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = chunks.get(idx) else {
                            break;
                        };
                        let mf = &mapped[chunk.file_idx];
                        let base_ptr = mf.as_slice().as_ptr() as usize;
                        let fidx = chunk.file_idx as u16;
                        for_each_record::<2>(mf, chunk, |[a, b]| {
                            if a.is_empty() || b.is_empty() || a == b {
                                return;
                            }
                            let oa = (a.as_ptr() as usize - base_ptr) as u32;
                            let ob = (b.as_ptr() as usize - base_ptr) as u32;
                            let ha = hash_bytes(a);
                            let hb = hash_bytes(b);
                            batch[batch_len] = (
                                MappedSpan::new(fidx, a.len(), oa, a),
                                ha,
                                MappedSpan::new(fidx, b.len(), ob, b),
                                hb,
                            );
                            batch_len += 1;
                            if batch_len == BATCH_SIZE {
                                flush_batch(&batch, batch_len, &mut local_shards);
                                batch_len = 0;
                            }
                        });
                    }
                    flush_batch(&batch, batch_len, &mut local_shards);

                    for (sh, vec) in local_shards.into_iter().enumerate() {
                        if !vec.is_empty() {
                            shard_lists[sh].lock().unwrap().extend_from_slice(&vec);
                        }
                    }
                });
            }
        });
        edge_counter.into_inner()
    };
    if total_edges == 0 {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }

    let shard_inputs: Vec<Vec<EndpointRef>> = shard_lists
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect();

    Ok(build_links_from_shards(&mapped, shard_inputs, total_edges, ncpu))
}

fn build_shard_table(
    items: &[(MappedSpan, u64)],
    comp_ids: &[u32],
) -> LinkShard {
    if items.is_empty() {
        return LinkShard::empty();
    }
    let cap = (items.len() * 2).next_power_of_two().max(8);
    let mask = cap - 1;
    let mut slots = vec![
        LinkSlot {
            uh: 0,
            comp_id: u32::MAX,
            _pad: 0,
        };
        cap
    ];

    for (i, &(_, h64)) in items.iter().enumerate() {
        let comp_id = comp_ids[i];
        let mut idx = (h64 as usize) & mask;
        while slots[idx].comp_id != u32::MAX {
            idx = (idx + 1) & mask;
        }
        slots[idx] = LinkSlot {
            uh: h64,
            comp_id,
            _pad: 0,
        };
    }

    LinkShard { slots, mask }
}

fn build_links_from_shards(
    mapped: &[MappedFile],
    shard_inputs: Vec<Vec<EndpointRef>>,
    total_edges: usize,
    ncpu: usize,
) -> (LinkLookup, CanonNames) {
    struct ShardOut {
        spans: Vec<(MappedSpan, u64)>,
        refs: Vec<(u32, u32)>,
    }

    let shard_outs: Vec<Mutex<Option<ShardOut>>> =
        (0..LINK_SHARDS).map(|_| Mutex::new(None)).collect();
    let next_shard = AtomicUsize::new(0);

    {
        let _span = crate::prof::Span::new(crate::prof::LINKS_DEDUP);
        thread::scope(|s| {
            for _ in 0..ncpu.min(LINK_SHARDS) {
                let shard_inputs = &shard_inputs;
                let shard_outs = &shard_outs;
                let next_shard = &next_shard;
                s.spawn(move || loop {
                    let sh = next_shard.fetch_add(1, Ordering::Relaxed);
                    if sh >= LINK_SHARDS {
                        break;
                    }
                    let items = &shard_inputs[sh];
                    let est = (items.len() / 2).max(16);
                    let mut spans: Vec<(MappedSpan, u64)> = Vec::with_capacity(est);
                    let mut refs: Vec<(u32, u32)> = Vec::with_capacity(items.len());

                    #[derive(Clone, Copy)]
                    struct ShardDedupSlot {
                        h64: u64,
                        span: MappedSpan,
                        local_id: u32,
                    }
                    let table_cap = (items.len() * 2).next_power_of_two().max(32);
                    let mask = table_cap - 1;
                    let mut table = vec![
                        ShardDedupSlot {
                            h64: 0,
                            span: EMPTY_SPAN,
                            local_id: u32::MAX,
                        };
                        table_cap
                    ];

                    for item in items {
                        let slice = item.span.slice(mapped);
                        let mut idx = (item.h64 as usize) & mask;
                        let local_id = loop {
                            let entry = unsafe { table.get_unchecked_mut(idx) };
                            if entry.span.len == 0 {
                                let id = spans.len() as u32;
                                *entry = ShardDedupSlot {
                                    h64: item.h64,
                                    span: item.span,
                                    local_id: id,
                                };
                                spans.push((item.span, item.h64));
                                break id;
                            }
                            if entry.h64 == item.h64
                                && entry.span.pref0 == item.span.pref0
                                && entry.span.pref1 == item.span.pref1
                                && (entry.span.len <= 16 || entry.span.slice(mapped) == slice)
                            {
                                break entry.local_id;
                            }
                            idx = (idx + 1) & mask;
                        };
                        refs.push((item.edge_tag, local_id));
                    }

                    *shard_outs[sh].lock().unwrap() = Some(ShardOut { spans, refs });
                });
            }
        });
    }
    drop(shard_inputs);

    let shard_outs: Vec<ShardOut> = shard_outs
        .into_iter()
        .map(|m| m.into_inner().unwrap().unwrap())
        .collect();

    // Compute per-shard ID bases and assemble edge_pairs
    let (shard_bases, id_to_comp, canon_names) = {
        let _span = crate::prof::Span::new(crate::prof::LINKS_DSU);
        let mut shard_bases = Vec::with_capacity(LINK_SHARDS + 1);
        let mut total_unique = 0usize;
        for out in &shard_outs {
            shard_bases.push(total_unique);
            total_unique += out.spans.len();
        }
        shard_bases.push(total_unique);

        let mut edge_pairs = vec![(0u32, 0u32); total_edges];
        let edge_pairs_ptr = edge_pairs.as_mut_ptr() as usize;
        let next_sh = AtomicUsize::new(0);
        thread::scope(|s| {
            for _ in 0..ncpu.min(LINK_SHARDS) {
                let next_sh = &next_sh;
                let shard_outs = &shard_outs;
                let shard_bases = &shard_bases;
                s.spawn(move || loop {
                    let sh = next_sh.fetch_add(1, Ordering::Relaxed);
                    if sh >= LINK_SHARDS {
                        break;
                    }
                    let id_base = shard_bases[sh] as u32;
                    let u32_ptr = edge_pairs_ptr as *mut u32;
                    for &(edge_tag, local_id) in &shard_outs[sh].refs {
                        let gid = id_base + local_id;
                        let idx = (edge_tag & 0x7FFF_FFFF) as usize;
                        let offset = idx * 2 + ((edge_tag >> 31) & 1) as usize;
                        unsafe {
                            *u32_ptr.add(offset) = gid;
                        }
                    }
                });
            }
        });

        // Flat vector of all spans indexed by gid
        let mut all_spans = Vec::with_capacity(total_unique);
        for out in &shard_outs {
            for &(span, _) in &out.spans {
                all_spans.push(span);
            }
        }

        // DSU over `0..total_unique`
        let mut dsu = Dsu::new(total_unique);
        for &(u, v) in &edge_pairs {
            dsu.union(u, v);
        }
        drop(edge_pairs);

        // Find canonical span (minimum span) for each component root
        let mut root_best = vec![u32::MAX; total_unique];
        for gid in 0..total_unique {
            let r = dsu.find(gid as u32) as usize;
            let current = root_best[r];
            if current == u32::MAX || all_spans[gid].is_less(&all_spans[current as usize], mapped) {
                root_best[r] = gid as u32;
            }
        }

        // Assign compact component IDs to each unique root and extract canonical spans
        let mut root_to_comp = vec![u32::MAX; total_unique];
        let mut comp_min_span = Vec::with_capacity(total_unique / 2);
        for gid in 0..total_unique {
            let r = dsu.parent[gid] as usize;
            if root_to_comp[r] == u32::MAX {
                let cid = comp_min_span.len() as u32;
                root_to_comp[r] = cid;
                let best_gid = root_best[r] as usize;
                comp_min_span.push(all_spans[best_gid]);
            }
        }

        let mut id_to_comp = vec![0u32; total_unique];
        for gid in 0..total_unique {
            id_to_comp[gid] = root_to_comp[dsu.parent[gid] as usize];
        }
        drop(dsu);
        drop(root_to_comp);
        drop(root_best);
        drop(all_spans);

        // Build CanonNames
        let total_canon_bytes: usize = comp_min_span.iter().map(|s| s.len as usize).sum();
        let mut canon_bytes = Vec::with_capacity(total_canon_bytes);
        let mut canon_offsets = Vec::with_capacity(comp_min_span.len() + 1);
        canon_offsets.push(0);
        for &span in &comp_min_span {
            canon_bytes.extend_from_slice(span.slice(mapped));
            canon_offsets.push(canon_bytes.len() as u32);
        }
        let canon_names = CanonNames {
            bytes: canon_bytes,
            offsets: canon_offsets,
        };
        (shard_bases, id_to_comp, canon_names)
    };

    // Build all 64 LinkShard open-addressed tables IN PARALLEL!
    let (shards, bloom) = {
        let _span = crate::prof::Span::new(crate::prof::LINKS_TABLE);
        let built_shards: Vec<Mutex<Option<LinkShard>>> =
            (0..LINK_SHARDS).map(|_| Mutex::new(None)).collect();
        let next_sh = AtomicUsize::new(0);

        thread::scope(|s| {
            for _ in 0..ncpu.min(LINK_SHARDS) {
                let shard_outs = &shard_outs;
                let shard_bases = &shard_bases;
                let id_to_comp = &id_to_comp;
                let built_shards = &built_shards;
                let next_sh = &next_sh;
                s.spawn(move || loop {
                    let sh = next_sh.fetch_add(1, Ordering::Relaxed);
                    if sh >= LINK_SHARDS {
                        break;
                    }
                    let base = shard_bases[sh];
                    let end = shard_bases[sh + 1];
                    let table =
                        build_shard_table(&shard_outs[sh].spans, &id_to_comp[base..end]);
                    *built_shards[sh].lock().unwrap() = Some(table);
                });
            }
        });

        let shards: Vec<LinkShard> = built_shards
            .into_iter()
            .map(|m| m.into_inner().unwrap().unwrap())
            .collect();

        let mut bloom = vec![0u64; BLOOM_WORDS];
        for out in &shard_outs {
            for &(_, h64) in &out.spans {
                let h1 = (h64 as usize) & BLOOM_MASK;
                let h2 = ((h64 >> 32) as usize) & BLOOM_MASK;
                let h3 = (h64.rotate_left(16) as usize) & BLOOM_MASK;
                bloom[h1 >> 6] |= 1 << (h1 & 63);
                bloom[h2 >> 6] |= 1 << (h2 & 63);
                bloom[h3 >> 6] |= 1 << (h3 & 63);
            }
        }
        (shards, bloom)
    };

    (LinkLookup::new(shards, bloom), canon_names)
}
