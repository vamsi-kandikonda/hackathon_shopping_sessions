use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use hashbrown::HashMap;


use crate::hash::{fast_build_hasher, hash_bytes, prefetch_read};
use crate::io_chunks::{
    DEFAULT_CHUNK_SIZE, MappedFile, for_each_record, list_csv_files, plan_chunks,
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

/// 16-byte slot storing user IDs up to 10 bytes INLINE (zero second cache miss!).
/// `comp_id == u32::MAX` denotes an empty slot.
#[derive(Clone, Copy)]
#[repr(C)]
struct LinkSlot {
    comp_id: u32,
    len: u16,
    data: [u8; 10],
}

struct LinkShard {
    slots: Vec<LinkSlot>,
    mask: usize,
    overflow: Vec<u8>,
}

impl LinkShard {
    fn empty() -> Self {
        Self {
            slots: Vec::new(),
            mask: 0,
            overflow: Vec::new(),
        }
    }
}

/// 64-shard read-only open-addressed lookup table: linked `user_id` -> `comp_id: u32`.
/// Built in parallel across all cores and supports software prefetching.
pub struct LinkLookup {
    shards: Vec<LinkShard>,
    empty: bool,
    /// Blocked Bloom filter over all linked IDs: 512-bit blocks, two bits per
    /// ID. Most unlinked IDs are rejected here without touching the big table.
    bloom: Vec<[u64; 8]>,
    bloom_mask: usize,
}

const MAX_BATCH: usize = 64;

impl LinkLookup {
    pub fn empty() -> Self {
        Self {
            shards: Vec::new(),
            empty: true,
            bloom: Vec::new(),
            bloom_mask: 0,
        }
    }

    fn with_bloom(shards: Vec<LinkShard>, hashes: impl Iterator<Item = u64>, n: usize) -> Self {
        // About 16 bits per ID: roughly 2 to 3% false positives.
        let blocks = ((n * 16).div_ceil(512)).next_power_of_two().max(1);
        let mut bloom = vec![[0u64; 8]; blocks];
        let mask = blocks - 1;
        for h in hashes {
            let (b, i1, i2) = Self::bloom_pos(h, mask);
            bloom[b][i1 >> 6] |= 1 << (i1 & 63);
            bloom[b][i2 >> 6] |= 1 << (i2 & 63);
        }
        Self {
            shards,
            empty: false,
            bloom,
            bloom_mask: mask,
        }
    }

    #[inline(always)]
    fn bloom_pos(h: u64, mask: usize) -> (usize, usize, usize) {
        (((h >> 16) as usize) & mask, ((h >> 40) & 511) as usize, ((h >> 49) & 511) as usize)
    }

    #[inline(always)]
    fn bloom_has(&self, h: u64) -> bool {
        let (b, i1, i2) = Self::bloom_pos(h, self.bloom_mask);
        let blk = unsafe { self.bloom.get_unchecked(b) };
        (blk[i1 >> 6] >> (i1 & 63)) & (blk[i2 >> 6] >> (i2 & 63)) & 1 != 0
    }

    /// Resolve up to 64 IDs: prefetch Bloom blocks, then prefetch table slots
    /// only for IDs that pass the filter, then probe.
    #[inline]
    pub fn lookup_batch(&self, uids: &[&[u8]], hs: &[u64], out: &mut [Option<u32>]) {
        self.lookup_batch_lazy(hs, out, |i| uids[i]);
    }

    /// Like `lookup_batch`, but fetches `uid` lazily ONLY for IDs that pass
    /// the Bloom filter. Avoids fetching user IDs from memory on Bloom rejects.
    #[inline]
    pub fn lookup_batch_lazy<'a>(
        &self,
        hs: &[u64],
        out: &mut [Option<u32>],
        mut get_uid: impl FnMut(usize) -> &'a [u8],
    ) {
        let n = hs.len();
        debug_assert!(n <= MAX_BATCH && out.len() >= n);
        if self.empty {
            out[..n].fill(None);
            return;
        }
        for &h in hs {
            let (b, _, _) = Self::bloom_pos(h, self.bloom_mask);
            prefetch_read(unsafe { self.bloom.as_ptr().add(b) });
        }
        let mut maybe = [false; MAX_BATCH];
        for i in 0..n {
            if self.bloom_has(hs[i]) {
                maybe[i] = true;
                self.prefetch(hs[i]);
            }
        }
        for i in 0..n {
            out[i] = if maybe[i] {
                let uid = get_uid(i);
                self.probe(uid, hs[i])
            } else {
                None
            };
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    #[inline(always)]
    pub fn prefetch(&self, h64: u64) {
        if self.empty {
            return;
        }
        let sh = unsafe { self.shards.get_unchecked((h64 >> 58) as usize) };
        if !sh.slots.is_empty() {
            let idx = (h64 as usize) & sh.mask;
            prefetch_read(unsafe { sh.slots.as_ptr().add(idx) });
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn lookup(&self, uid: &[u8], h64: u64) -> Option<u32> {
        if self.empty || !self.bloom_has(h64) {
            return None;
        }
        self.probe(uid, h64)
    }

    #[inline(always)]
    fn probe(&self, uid: &[u8], h64: u64) -> Option<u32> {
        let sh = unsafe { self.shards.get_unchecked((h64 >> 58) as usize) };
        if sh.slots.is_empty() {
            return None;
        }
        let ulen = uid.len();
        let mut idx = (h64 as usize) & sh.mask;
        if ulen <= 10 {
            let ulen16 = ulen as u16;
            loop {
                let slot = unsafe { *sh.slots.get_unchecked(idx) };
                if slot.comp_id == u32::MAX {
                    return None;
                }
                if slot.len == ulen16 && unsafe { slot.data.get_unchecked(..ulen) } == uid {
                    return Some(slot.comp_id);
                }
                idx = (idx + 1) & sh.mask;
            }
        } else {
            loop {
                let slot = unsafe { *sh.slots.get_unchecked(idx) };
                if slot.comp_id == u32::MAX {
                    return None;
                }
                if slot.len == 0xFFFF {
                    let off = u32::from_ne_bytes(slot.data[0..4].try_into().unwrap()) as usize;
                    let slen = u32::from_ne_bytes(slot.data[4..8].try_into().unwrap()) as usize;
                    if slen == ulen && &sh.overflow[off..off + slen] == uid {
                        return Some(slot.comp_id);
                    }
                }
                idx = (idx + 1) & sh.mask;
            }
        }
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
    file_idx: u16,
    len: u16,
    off: u32,
}

impl MappedSpan {
    #[inline(always)]
    fn slice<'a>(&self, mapped: &'a [MappedFile]) -> &'a [u8] {
        let data = mapped[self.file_idx as usize].as_slice();
        unsafe { data.get_unchecked(self.off as usize..self.off as usize + self.len as usize) }
    }
}

pub fn load_links(data_dir: &Path, ncpu: usize) -> io::Result<(LinkLookup, CanonNames)> {
    let files = list_csv_files(&data_dir.join("links"))?;
    if files.is_empty() {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }
    let (mapped, chunks, total_bytes) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    if chunks.is_empty() || total_bytes == 0 {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }

    let workers = ncpu.min(chunks.len()).max(1);
    let next_chunk = AtomicUsize::new(0);
    let batches: Vec<Mutex<Vec<(MappedSpan, MappedSpan)>>> =
        (0..workers).map(|_| Mutex::new(Vec::new())).collect();

    thread::scope(|s| {
        for tid in 0..workers {
            let mapped = &mapped;
            let chunks = &chunks;
            let next_chunk = &next_chunk;
            let batches = &batches;
            s.spawn(move || {
                let mut local_edges = Vec::new();
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
                        local_edges.push((
                            MappedSpan {
                                file_idx: fidx,
                                len: a.len() as u16,
                                off: oa,
                            },
                            MappedSpan {
                                file_idx: fidx,
                                len: b.len() as u16,
                                off: ob,
                            },
                        ));
                    });
                }
                *batches[tid].lock().unwrap() = local_edges;
            });
        }
    });

    let batches: Vec<Vec<(MappedSpan, MappedSpan)>> = batches
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect();
    let total_edges: usize = batches.iter().map(|b| b.len()).sum();
    if total_edges == 0 {
        return Ok((LinkLookup::empty(), CanonNames::empty()));
    }

    if total_edges < 50_000 || ncpu <= 1 {
        Ok(build_links_single(&mapped, &batches, total_edges))
    } else {
        Ok(build_links_parallel(&mapped, &batches, total_edges, ncpu))
    }
}

fn build_shard_table(
    mapped: &[MappedFile],
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
            comp_id: u32::MAX,
            len: 0,
            data: [0u8; 10],
        };
        cap
    ];
    let mut overflow = Vec::new();

    for (i, &(span, h64)) in items.iter().enumerate() {
        let comp_id = comp_ids[i];
        let slice = span.slice(mapped);
        let mut idx = (h64 as usize) & mask;
        while slots[idx].comp_id != u32::MAX {
            idx = (idx + 1) & mask;
        }
        if slice.len() <= 10 {
            let mut data = [0u8; 10];
            data[..slice.len()].copy_from_slice(slice);
            slots[idx] = LinkSlot {
                comp_id,
                len: slice.len() as u16,
                data,
            };
        } else {
            let ov_off = overflow.len() as u32;
            overflow.extend_from_slice(slice);
            let mut data = [0u8; 10];
            data[0..4].copy_from_slice(&ov_off.to_ne_bytes());
            data[4..8].copy_from_slice(&(slice.len() as u32).to_ne_bytes());
            slots[idx] = LinkSlot {
                comp_id,
                len: 0xFFFF,
                data,
            };
        }
    }

    LinkShard {
        slots,
        mask,
        overflow,
    }
}

fn build_links_single(
    mapped: &[MappedFile],
    batches: &[Vec<(MappedSpan, MappedSpan)>],
    total_edges: usize,
) -> (LinkLookup, CanonNames) {
    let mut id_spans: Vec<(MappedSpan, u64)> = Vec::with_capacity(total_edges);
    let mut map: HashMap<&[u8], u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(total_edges, fast_build_hasher());
    let mut edge_pairs: Vec<(u32, u32)> = Vec::with_capacity(total_edges);

    for batch in batches {
        for &(sa, sb) in batch {
            let a = sa.slice(mapped);
            let u = if let Some(&id) = map.get(a) {
                id
            } else {
                let id = id_spans.len() as u32;
                id_spans.push((sa, hash_bytes(a)));
                map.insert(a, id);
                id
            };
            let b = sb.slice(mapped);
            let v = if let Some(&id) = map.get(b) {
                id
            } else {
                let id = id_spans.len() as u32;
                id_spans.push((sb, hash_bytes(b)));
                map.insert(b, id);
                id
            };
            edge_pairs.push((u, v));
        }
    }
    drop(map);

    let num_unique = id_spans.len();
    let mut dsu = Dsu::new(num_unique);
    for &(u, v) in &edge_pairs {
        dsu.union(u, v);
    }

    let mut root_to_comp = vec![u32::MAX; num_unique];
    let mut comp_min_id: Vec<u32> = Vec::new();
    let mut id_to_comp = vec![0u32; num_unique];

    for i in 0..num_unique {
        let r = dsu.find(i as u32) as usize;
        let cid = if root_to_comp[r] == u32::MAX {
            let cid = comp_min_id.len() as u32;
            root_to_comp[r] = cid;
            comp_min_id.push(i as u32);
            cid
        } else {
            let cid = root_to_comp[r];
            let cur_best = comp_min_id[cid as usize] as usize;
            if id_spans[i].0.slice(mapped) < id_spans[cur_best].0.slice(mapped) {
                comp_min_id[cid as usize] = i as u32;
            }
            cid
        };
        id_to_comp[i] = cid;
    }

    let total_canon_bytes: usize = comp_min_id
        .iter()
        .map(|&id| id_spans[id as usize].0.len as usize)
        .sum();
    let mut canon_bytes = Vec::with_capacity(total_canon_bytes);
    let mut canon_offsets = Vec::with_capacity(comp_min_id.len() + 1);
    canon_offsets.push(0);
    for &best_id in &comp_min_id {
        canon_bytes.extend_from_slice(id_spans[best_id as usize].0.slice(mapped));
        canon_offsets.push(canon_bytes.len() as u32);
    }
    let canon_names = CanonNames {
        bytes: canon_bytes,
        offsets: canon_offsets,
    };

    // Bucket into 64 shards by top 6 bits of h64
    let mut shard_items: Vec<Vec<(MappedSpan, u64)>> = (0..LINK_SHARDS).map(|_| Vec::new()).collect();
    let mut shard_comps: Vec<Vec<u32>> = (0..LINK_SHARDS).map(|_| Vec::new()).collect();
    for (i, &(span, h64)) in id_spans.iter().enumerate() {
        let sh = (h64 >> 58) as usize;
        shard_items[sh].push((span, h64));
        shard_comps[sh].push(id_to_comp[i]);
    }
    let shards: Vec<LinkShard> = (0..LINK_SHARDS)
        .map(|sh| build_shard_table(mapped, &shard_items[sh], &shard_comps[sh]))
        .collect();

    (
        LinkLookup::with_bloom(shards, id_spans.iter().map(|&(_, h)| h), id_spans.len()),
        canon_names,
    )
}

fn build_links_parallel(
    mapped: &[MappedFile],
    batches: &[Vec<(MappedSpan, MappedSpan)>],
    total_edges: usize,
    ncpu: usize,
) -> (LinkLookup, CanonNames) {
    let mut batch_edge_base = Vec::with_capacity(batches.len());
    let mut acc = 0u32;
    for b in batches {
        batch_edge_base.push(acc);
        acc += b.len() as u32;
    }

    #[derive(Clone, Copy)]
    struct EndpointRef {
        edge_tag: u32,
        span: MappedSpan,
        h64: u64,
    }

    // Partition by TOP 6 bits (`h64 >> 58`) so each shard directly becomes one `LinkShard`!
    let shard_lists: Vec<Mutex<Vec<EndpointRef>>> =
        (0..LINK_SHARDS).map(|_| Mutex::new(Vec::new())).collect();

    thread::scope(|s| {
        for (bi, batch) in batches.iter().enumerate() {
            let base = batch_edge_base[bi];
            let shard_lists = &shard_lists;
            s.spawn(move || {
                let mut local: Vec<Vec<EndpointRef>> =
                    (0..LINK_SHARDS).map(|_| Vec::new()).collect();
                for (ei, &(sa, sb)) in batch.iter().enumerate() {
                    let g_edge = base + ei as u32;
                    let ha = hash_bytes(sa.slice(mapped));
                    local[(ha >> 58) as usize].push(EndpointRef {
                        edge_tag: g_edge,
                        span: sa,
                        h64: ha,
                    });
                    let hb = hash_bytes(sb.slice(mapped));
                    local[(hb >> 58) as usize].push(EndpointRef {
                        edge_tag: g_edge | 0x8000_0000,
                        span: sb,
                        h64: hb,
                    });
                }
                for (sh, vec) in local.into_iter().enumerate() {
                    if !vec.is_empty() {
                        shard_lists[sh].lock().unwrap().extend_from_slice(&vec);
                    }
                }
            });
        }
    });

    struct ShardOut {
        spans: Vec<(MappedSpan, u64)>,
        refs: Vec<(u32, u32)>,
    }

    let shard_inputs: Vec<Vec<EndpointRef>> = shard_lists
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect();
    let shard_outs: Vec<Mutex<Option<ShardOut>>> =
        (0..LINK_SHARDS).map(|_| Mutex::new(None)).collect();
    let next_shard = AtomicUsize::new(0);

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
                let mut map: HashMap<&[u8], u32, ahash::RandomState> =
                    HashMap::with_capacity_and_hasher(est, fast_build_hasher());

                for item in items {
                    let slice = item.span.slice(mapped);
                    let local_id = if let Some(&id) = map.get(slice) {
                        id
                    } else {
                        let id = spans.len() as u32;
                        spans.push((item.span, item.h64));
                        map.insert(slice, id);
                        id
                    };
                    refs.push((item.edge_tag, local_id));
                }

                *shard_outs[sh].lock().unwrap() = Some(ShardOut { spans, refs });
            });
        }
    });
    drop(shard_inputs);

    let shard_outs: Vec<ShardOut> = shard_outs
        .into_iter()
        .map(|m| m.into_inner().unwrap().unwrap())
        .collect();

    // Compute per-shard ID bases and assemble edge_pairs
    let mut shard_bases = Vec::with_capacity(LINK_SHARDS + 1);
    let mut total_unique = 0usize;
    for out in &shard_outs {
        shard_bases.push(total_unique);
        total_unique += out.spans.len();
    }
    shard_bases.push(total_unique);

    let mut edge_pairs = vec![(0u32, 0u32); total_edges];
    for (sh, out) in shard_outs.iter().enumerate() {
        let id_base = shard_bases[sh] as u32;
        for &(edge_tag, local_id) in &out.refs {
            let gid = id_base + local_id;
            let idx = (edge_tag & 0x7FFF_FFFF) as usize;
            if (edge_tag & 0x8000_0000) == 0 {
                edge_pairs[idx].0 = gid;
            } else {
                edge_pairs[idx].1 = gid;
            }
        }
    }

    // DSU over `0..total_unique`
    let mut dsu = Dsu::new(total_unique);
    for &(u, v) in &edge_pairs {
        dsu.union(u, v);
    }
    drop(edge_pairs);

    // Find byte-wise smallest canonical name per component
    let mut root_to_comp = vec![u32::MAX; total_unique];
    let mut comp_min_span: Vec<MappedSpan> = Vec::new();
    let mut id_to_comp = vec![0u32; total_unique];

    for (sh, out) in shard_outs.iter().enumerate() {
        let base = shard_bases[sh];
        for (li, &(span, _)) in out.spans.iter().enumerate() {
            let gid = base + li;
            let r = dsu.find(gid as u32) as usize;
            let cid = if root_to_comp[r] == u32::MAX {
                let cid = comp_min_span.len() as u32;
                root_to_comp[r] = cid;
                comp_min_span.push(span);
                cid
            } else {
                let cid = root_to_comp[r];
                if span.slice(mapped) < comp_min_span[cid as usize].slice(mapped) {
                    comp_min_span[cid as usize] = span;
                }
                cid
            };
            id_to_comp[gid] = cid;
        }
    }
    drop(dsu);
    drop(root_to_comp);

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

    // Build all 64 LinkShard open-addressed tables IN PARALLEL!
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
                    build_shard_table(mapped, &shard_outs[sh].spans, &id_to_comp[base..end]);
                *built_shards[sh].lock().unwrap() = Some(table);
            });
        }
    });

    let shards: Vec<LinkShard> = built_shards
        .into_iter()
        .map(|m| m.into_inner().unwrap().unwrap())
        .collect();

    (
        LinkLookup::with_bloom(
            shards,
            shard_outs.iter().flat_map(|o| o.spans.iter().map(|&(_, h)| h)),
            total_unique,
        ),
        canon_names,
    )
}
