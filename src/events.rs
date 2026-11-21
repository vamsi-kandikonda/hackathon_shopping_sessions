use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::arena::{Arena, FixedBuf, RawChunk};
use crate::hash::{
    EVENT_PARTS, PERSON_PARTS, write_raw, event_part, hash_bytes, person_part_from_comp,
    person_part_from_hash, prefetch_read, rd_u32, rd_u64, read_varint, Cur,
};
use crate::io_chunks::{
    DEFAULT_CHUNK_SIZE, for_each_record, for_each_record_stream, list_csv_files, plan_chunks,
    use_pread,
};
use crate::links::LinkLookup;
use crate::lookups::Lookups;
use crate::timestamp::{
    LATE_CUTOFF_MS, parse_f1_sec_fast, parse_price_cents, parse_timestamp_in_window,
};

const TAG_VALID: u8 = 1;
const TAG_PURCHASE: u8 = 2;

const VIEW_U32: u32 = u32::from_ne_bytes(*b"view");
const CART_U32: u32 = u32::from_ne_bytes(*b"cart");
const PURCH_U64: u64 = u64::from_ne_bytes(*b"purchase");

#[inline(always)]
fn parse_etype(etype: &[u8]) -> Option<bool> {
    if etype.len() == 4 {
        let v = u32::from_ne_bytes(unsafe { *(etype.as_ptr() as *const [u8; 4]) });
        if v == VIEW_U32 || v == CART_U32 {
            return Some(false);
        }
    } else if etype.len() == 8 {
        let v = u64::from_ne_bytes(unsafe { *(etype.as_ptr() as *const [u8; 8]) });
        if v == PURCH_U64 {
            return Some(true);
        }
    }
    None
}

const PARSE_FLUSH: usize = 32 * 1024;
const ROUTE_FLUSH: usize = 64 * 1024;
const PREFETCH_BATCH: usize = 64;

/// One surviving event, packed so that sorting the keys sorts by timestamp.
/// `u64` (compact) holds `ts_rel` in 32 bits, the category in 8 and the price
/// in 24; it is used whenever the window spans under 2^32 ms and there are at
/// most 255 categories. `u128` (wide) covers everything else.
pub trait EvKey: Copy + Ord + Send + Sync + 'static {
    const BYTES: usize;
    const TS_BYTES: usize;
    fn pack(ts_rel: u64, price: u32, cat: u32) -> Self;
    fn ts_rel(self) -> u64;
    fn price(self) -> u32;
    fn cat(self) -> u32;
    fn write_to(self, c: &mut Cur);
    fn read_from(data: &[u8], pos: usize) -> Self;
    fn write_ts(c: &mut Cur, ts_rel: u64);
    fn read_ts(data: &[u8], pos: usize) -> u64;
}

impl EvKey for u64 {
    const BYTES: usize = 8;
    const TS_BYTES: usize = 4;
    #[inline(always)]
    fn pack(ts_rel: u64, price: u32, cat: u32) -> Self {
        (ts_rel << 32) | ((cat as u64) << 24) | price as u64
    }
    #[inline(always)]
    fn ts_rel(self) -> u64 {
        self >> 32
    }
    #[inline(always)]
    fn price(self) -> u32 {
        (self as u32) & 0x00FF_FFFF
    }
    #[inline(always)]
    fn cat(self) -> u32 {
        (self as u32) >> 24
    }
    #[inline(always)]
    fn write_to(self, c: &mut Cur) {
        c.u64(self)
    }
    #[inline(always)]
    fn read_from(data: &[u8], pos: usize) -> Self {
        rd_u64(data, pos)
    }
    #[inline(always)]
    fn write_ts(c: &mut Cur, ts_rel: u64) {
        c.u32(ts_rel as u32)
    }
    #[inline(always)]
    fn read_ts(data: &[u8], pos: usize) -> u64 {
        rd_u32(data, pos) as u64
    }
}

impl EvKey for u128 {
    const BYTES: usize = 16;
    const TS_BYTES: usize = 8;
    #[inline(always)]
    fn pack(ts_rel: u64, price: u32, cat: u32) -> Self {
        ((ts_rel as u128) << 64) | ((cat as u128) << 32) | price as u128
    }
    #[inline(always)]
    fn ts_rel(self) -> u64 {
        (self >> 64) as u64
    }
    #[inline(always)]
    fn price(self) -> u32 {
        self as u32
    }
    #[inline(always)]
    fn cat(self) -> u32 {
        (self >> 32) as u32
    }
    #[inline(always)]
    fn write_to(self, c: &mut Cur) {
        c.u64(self as u64);
        c.u64((self >> 64) as u64);
    }
    #[inline(always)]
    fn read_from(data: &[u8], pos: usize) -> Self {
        (rd_u64(data, pos) as u128) | ((rd_u64(data, pos + 8) as u128) << 64)
    }
    #[inline(always)]
    fn write_ts(c: &mut Cur, ts_rel: u64) {
        c.u64(ts_rel)
    }
    #[inline(always)]
    fn read_ts(data: &[u8], pos: usize) -> u64 {
        rd_u64(data, pos)
    }
}

/// One person partition's events. Linked records are `[comp: u32, key: K]`;
/// singleton records are `[uid_hash: u64, uid_len: varint, uid_bytes, key: K]`. Chunks live
/// in the arenas owned by `PersonEvents`.
pub struct PersonPart<K: EvKey> {
    pub linked: Vec<RawChunk>,
    pub linked_count: usize,
    pub singletons: Vec<RawChunk>,
    pub singleton_count: usize,
    _k: std::marker::PhantomData<K>,
}

impl<K: EvKey> Default for PersonPart<K> {
    fn default() -> Self {
        Self {
            linked: Vec::new(),
            linked_count: 0,
            singletons: Vec::new(),
            singleton_count: 0,
            _k: std::marker::PhantomData,
        }
    }
}

/// Person partitions plus the arenas their chunks point into.
pub struct PersonEvents<K: EvKey> {
    pub parts: Vec<PersonPart<K>>,
    pub arenas: Vec<Arena>,
}

impl<K: EvKey> PersonEvents<K> {
    /// Dead parse memory left over after routing, for the click phase to
    /// reuse. The spans stay valid as long as `self` is alive.
    pub fn take_spare(&mut self) -> Vec<crate::arena::Span> {
        self.arenas.iter_mut().flat_map(|a| a.take_spare()).collect()
    }
}

impl<K: EvKey> PersonPart<K> {
    pub fn event_count(&self) -> usize {
        self.linked_count + self.singleton_count
    }
}

/// Raw parsed events, partitioned by `hash(event_id)`. Records are
/// `[tag, eid_len, eid, ingest_sec: u64]` plus, for valid rows,
/// `[ts_rel, (price: u32, cat: u32 if purchase), uid_len, uid]`.
/// Invalid rows stay as tombstones so an invalid earliest copy still wins dedup.
pub struct ParsedEvents {
    parts: Vec<Vec<RawChunk>>,
    counts: Vec<usize>,
    /// Owns the memory behind every chunk in `parts`.
    _arenas: Vec<Arena>,
}

/// Phase 2a: parse, validate and drop late events. Needs no link data, so it
/// runs while links are still loading.
pub fn parse_events<K: EvKey>(
    data_dir: &Path,
    lookups: &Lookups,
    ncpu: usize,
) -> io::Result<ParsedEvents> {
    let files = list_csv_files(&data_dir.join("events"))?;
    let (mapped, chunks, _) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    let parts: Vec<Mutex<Vec<RawChunk>>> =
        (0..EVENT_PARTS).map(|_| Mutex::new(Vec::new())).collect();
    let counts: Vec<AtomicUsize> = (0..EVENT_PARTS).map(|_| AtomicUsize::new(0)).collect();
    let arenas: Mutex<Vec<Arena>> = Mutex::new(Vec::new());

    if !chunks.is_empty() {
        let next_chunk = AtomicUsize::new(0);
        let workers = ncpu.min(chunks.len()).max(1);
        let win_start = lookups.window_start_ms;
        let win_end = lookups.window_end_ms;
        let ym = lookups.ym_cache;
        let pread = use_pread();

        thread::scope(|s| {
            for _ in 0..workers {
                let (mapped, chunks, next_chunk, parts, counts, arenas) =
                    (&mapped, &chunks, &next_chunk, &parts, &counts, &arenas);
                s.spawn(move || {
                    let mut arena = Arena::default();
                    let mut bufs: Vec<FixedBuf> = (0..EVENT_PARTS).map(|_| FixedBuf::empty()).collect();
                    let mut loc_counts = [0usize; EVENT_PARTS];
                    let mut rbuf: Vec<u8> = Vec::new();
                    loop {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = chunks.get(idx) else {
                            break;
                        };
                        let mut on_rec = |fields: [&[u8]; 7]| {
                            let [eid, uid, ts_b, ingest_b, etype, pid, price_b] = fields;
                            if eid.is_empty() {
                                return;
                            }
                            let Some(ingest_sec) = parse_f1_sec_fast(ingest_b, &ym) else {
                                return;
                            };

                            // Validate the copy now; dedup later keeps the earliest copy
                            // whether or not it is valid.
                            let mut valid = false;
                            let mut purchase = false;
                            let mut ts_ms = 0i64;
                            let mut price = 0u32;
                            let mut cat = 0u32;
                            if !uid.is_empty() && !pid.is_empty() {
                                if let Some(is_purchase) = parse_etype(etype) {
                                    if let Some(ts) =
                                        parse_timestamp_in_window(ts_b, win_start, win_end, &ym)
                                    {
                                        let ingest_ms = ingest_sec.saturating_mul(1000);
                                        if ingest_ms.saturating_sub(ts) <= LATE_CUTOFF_MS {
                                            if is_purchase {
                                                if let Some(p) = parse_price_cents(price_b) {
                                                    valid = true;
                                                    purchase = true;
                                                    ts_ms = ts;
                                                    price = p;
                                                    cat = lookups.lookup_product_cat(pid);
                                                }
                                            } else {
                                                valid = true;
                                                ts_ms = ts;
                                            }
                                        }
                                    }
                                }
                            }

                            let eh = hash_bytes(eid);
                            let ep = event_part(eh);
                            unsafe { *loc_counts.get_unchecked_mut(ep) += 1 };
                            let buf = unsafe { bufs.get_unchecked_mut(ep) };
                            if let Some(full) = buf.reserve(eid.len() + uid.len() + 72, PARSE_FLUSH, &mut arena) {
                                parts[ep].lock().unwrap().push(full);
                            }
                            let n = unsafe {
                                write_raw(buf.tail(), |c| {
                                    if !valid {
                                        c.u8(0);
                                        c.varint(eid.len());
                                        c.bytes(eid);
                                        c.u64(eh);
                                        c.u64(ingest_sec as u64);
                                    } else {
                                        c.u8(if purchase { TAG_VALID | TAG_PURCHASE } else { TAG_VALID });
                                        c.varint(eid.len());
                                        c.bytes(eid);
                                        c.u64(eh);
                                        c.u64(ingest_sec as u64);
                                        K::write_ts(c, (ts_ms - win_start) as u64);
                                        if purchase {
                                            c.u32(price);
                                            c.u32(cat);
                                        }
                                        c.varint(uid.len());
                                        c.bytes(uid);
                                    }
                                })
                            };
                            buf.advance(n);
                        };
                        let mf = &mapped[chunk.file_idx];
                        if pread {
                            for_each_record_stream::<7>(mf, chunk, &mut rbuf, &mut on_rec);
                        } else {
                            for_each_record::<7>(mf, chunk, &mut on_rec);
                        }
                    }
                    for (ep, buf) in bufs.iter_mut().enumerate() {
                        if buf.len() > 0 {
                            parts[ep].lock().unwrap().push(buf.take());
                        }
                        if loc_counts[ep] > 0 {
                            counts[ep].fetch_add(loc_counts[ep], Ordering::Relaxed);
                        }
                    }
                    arenas.lock().unwrap().push(arena);
                });
            }
        });
    }

    Ok(ParsedEvents {
        parts: parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        counts: counts.into_iter().map(|c| c.into_inner()).collect(),
        _arenas: arenas.into_inner().unwrap(),
    })
}

/// Dedup table entry. Bit 31 of `chunk_flags` marks a valid kept copy.
#[derive(Clone, Copy)]
struct Rec<K: EvKey> {
    key: K,
    chunk_flags: u32,
    eid_off: u32,
    uid_off: u32,
    eid_len: u16,
    uid_len: u16,
}

impl<K: EvKey> Rec<K> {
    #[inline(always)]
    fn valid(&self) -> bool {
        (self.chunk_flags & (1 << 31)) != 0
    }
    #[inline(always)]
    fn chunk(&self) -> usize {
        (self.chunk_flags & 0x7FFF_FFFF) as usize
    }
    #[inline(always)]
    fn ingest_sec(&self, chunks: &[&[u8]]) -> i64 {
        let d = chunks[self.chunk()];
        rd_u64(d, self.eid_off as usize + self.eid_len as usize + 8) as i64
    }
    #[inline(always)]
    fn eid<'a>(&self, chunks: &'a [&'a [u8]]) -> &'a [u8] {
        let d = &chunks[self.chunk()];
        &d[self.eid_off as usize..self.eid_off as usize + self.eid_len as usize]
    }
    #[inline(always)]
    fn uid<'a>(&self, chunks: &'a [&'a [u8]]) -> &'a [u8] {
        let d = &chunks[self.chunk()];
        &d[self.uid_off as usize..self.uid_off as usize + self.uid_len as usize]
    }
}

/// Keep, per `event_id`, the copy with the smallest `ingest_ts`.
fn dedup_partition<K: EvKey>(
    chunks: &[&[u8]],
    est_records: usize,
    slots: &mut Vec<(u32, u32)>,
    entries: &mut Vec<Rec<K>>,
    batch: &mut Vec<(u64, Rec<K>)>,
) {
    entries.clear();
    let cap = ((est_records * 10 / 7).max(64)).next_power_of_two().max(1024);
    slots.clear();
    slots.resize(cap, (0, 0));
    let mask = cap - 1;
    batch.clear();

    let insert_batch = |batch: &mut Vec<(u64, Rec<K>)>,
                        slots: &mut Vec<(u32, u32)>,
                        entries: &mut Vec<Rec<K>>| {
        for (h, rec) in batch.drain(..) {
            let tag = ((h >> 32) as u32) | 1;
            let mut idx = (h as usize) & mask;
            loop {
                let (stag, sidx) = unsafe { *slots.get_unchecked(idx) };
                if stag == 0 {
                    entries.push(rec);
                    unsafe { *slots.get_unchecked_mut(idx) = (tag, entries.len() as u32) };
                    break;
                }
                if stag == tag {
                    let e = unsafe { entries.get_unchecked_mut((sidx - 1) as usize) };
                    if e.eid_len == rec.eid_len && e.eid(chunks) == rec.eid(chunks) {
                        if rec.ingest_sec(chunks) < e.ingest_sec(chunks) {
                            *e = rec;
                        }
                        break;
                    }
                }
                idx = (idx + 1) & mask;
            }
        }
    };

    for (ci, data) in chunks.iter().enumerate() {
        let data: &[u8] = data;
        let mut pos = 0usize;
        while pos < data.len() {
            let tag = data[pos];
            pos += 1;
            let eid_len = read_varint(data, &mut pos);
            let eid_off = pos;
            pos += eid_len;
            let h = rd_u64(data, pos);
            pos += 16;

            let valid = (tag & TAG_VALID) != 0;
            let mut key = K::pack(0, 0, 0);
            let mut uid_off = 0usize;
            let mut uid_len = 0usize;
            if valid {
                let ts_rel = K::read_ts(data, pos);
                pos += K::TS_BYTES;
                let (price, cat) = if (tag & TAG_PURCHASE) != 0 {
                    let p = rd_u32(data, pos);
                    let c = rd_u32(data, pos + 4);
                    pos += 8;
                    (p, c)
                } else {
                    (0, 0)
                };
                key = K::pack(ts_rel, price, cat);
                uid_len = read_varint(data, &mut pos);
                uid_off = pos;
                pos += uid_len;
            }

            prefetch_read(unsafe { slots.as_ptr().add((h as usize) & mask) });
            batch.push((
                h,
                Rec {
                    key,
                    chunk_flags: (ci as u32) | ((valid as u32) << 31),
                    eid_off: eid_off as u32,
                    uid_off: uid_off as u32,
                    eid_len: eid_len as u16,
                    uid_len: uid_len as u16,
                },
            ));
            if batch.len() == PREFETCH_BATCH {
                insert_batch(batch, slots, entries);
            }
        }
    }
    if !batch.is_empty() {
        insert_batch(batch, slots, entries);
    }
}

/// Phase 2b: dedup each `event_id` partition, resolve each surviving event's
/// person through the link table and route it to its person partition.
pub fn route_events<K: EvKey>(
    parsed: ParsedEvents,
    link_lookup: &LinkLookup,
    ncpu: usize,
) -> PersonEvents<K> {
    // Routed output is written into parse chunks that are already consumed,
    // so the parse arenas move into `PersonEvents` and live until the end.
    let ParsedEvents { parts, counts, _arenas: parse_arenas } = parsed;
    let inputs: Vec<Mutex<Vec<RawChunk>>> = parts.into_iter().map(Mutex::new).collect();
    let out: Vec<Mutex<PersonPart<K>>> =
        (0..PERSON_PARTS).map(|_| Mutex::new(PersonPart::default())).collect();
    let out_arenas: Mutex<Vec<Arena>> = Mutex::new(Vec::new());
    let next = AtomicUsize::new(0);
    let exp = std::env::var("SS_EXP").unwrap_or_default();
    let (exp_nolookup, exp_nowrite) = (exp == "nolookup", exp == "nowrite");
    let has_links = !link_lookup.is_empty() && !exp_nolookup;

    thread::scope(|s| {
        for _ in 0..ncpu.max(1) {
            let (inputs, counts, out, next, out_arenas) = (&inputs, &counts, &out, &next, &out_arenas);
            s.spawn(move || {
                let mut arena = Arena::default();
                let mut slots: Vec<(u32, u32)> = Vec::new();
                let mut entries: Vec<Rec<K>> = Vec::new();
                let mut batch: Vec<(u64, Rec<K>)> = Vec::with_capacity(PREFETCH_BATCH);
                let mut loc_linked: Vec<FixedBuf> = (0..PERSON_PARTS).map(|_| FixedBuf::empty()).collect();
                let mut loc_sing: Vec<FixedBuf> = (0..PERSON_PARTS).map(|_| FixedBuf::empty()).collect();
                let mut n_linked = vec![0usize; PERSON_PARTS];
                let mut n_sing = vec![0usize; PERSON_PARTS];
                let mut hashes = [0u64; PREFETCH_BATCH];

                loop {
                    let ep = next.fetch_add(1, Ordering::Relaxed);
                    if ep >= EVENT_PARTS {
                        break;
                    }
                    let raw = std::mem::take(&mut *inputs[ep].lock().unwrap());
                    if raw.is_empty() {
                        continue;
                    }
                    let chunks: Vec<&[u8]> = raw.iter().map(|c| c.as_slice()).collect();
                    crate::prof::time(crate::prof::ROUTE_DEDUP, || {
                        dedup_partition::<K>(&chunks, counts[ep], &mut slots, &mut entries, &mut batch)
                    });
                    let _scatter = crate::prof::Span::new(crate::prof::ROUTE_SCATTER);
                    let n = entries.len();
                    let mut i = 0;
                    let mut idx = [0u32; PREFETCH_BATCH];
                    let mut uids: [&[u8]; PREFETCH_BATCH] = [&[]; PREFETCH_BATCH];
                    let mut comps = [None; PREFETCH_BATCH];
                    while i < n {
                        let end = (i + PREFETCH_BATCH).min(n);
                        let mut m = 0usize;
                        for j in i..end {
                            let e = &entries[j];
                            if e.valid() {
                                let uid = e.uid(&chunks);
                                idx[m] = j as u32;
                                uids[m] = uid;
                                hashes[m] = hash_bytes(uid);
                                m += 1;
                            }
                        }
                        if has_links {
                            link_lookup.lookup_batch(&uids[..m], &hashes[..m], &mut comps[..m]);
                        }
                        for k in 0..m {
                            let e = entries[idx[k] as usize];
                            let uid = uids[k];
                            let h = hashes[k];
                            let comp = if has_links { comps[k] } else { None };
                            if exp_nowrite {
                                std::hint::black_box((comp, uid.len()));
                                continue;
                            }
                            let (pp, buf, need) = match comp {
                                Some(comp) => {
                                    let pp = person_part_from_comp(comp);
                                    n_linked[pp] += 1;
                                    (pp, unsafe { loc_linked.get_unchecked_mut(pp) }, 4 + K::BYTES)
                                }
                                None => {
                                    let pp = person_part_from_hash(h);
                                    n_sing[pp] += 1;
                                    (pp, unsafe { loc_sing.get_unchecked_mut(pp) }, uid.len() + K::BYTES + 18)
                                }
                            };
                            if let Some(full) = buf.reserve(need, ROUTE_FLUSH, &mut arena) {
                                let mut g = out[pp].lock().unwrap();
                                if comp.is_some() {
                                    g.linked.push(full);
                                } else {
                                    g.singletons.push(full);
                                }
                            }
                            let w = unsafe {
                                write_raw(buf.tail(), |c| match comp {
                                    Some(comp) => {
                                        c.u32(comp);
                                        e.key.write_to(c);
                                    }
                                    None => {
                                        c.u64(h);
                                        c.varint(uid.len());
                                        c.bytes(uid);
                                        e.key.write_to(c);
                                    }
                                })
                            };
                            buf.advance(w);
                        }
                        i = end;
                    }
                    drop(_scatter);
                    drop(chunks);
                    // This partition's parse chunks are dead now: reuse them for output.
                    for c in raw {
                        arena.recycle(c);
                    }
                }

                for pp in 0..PERSON_PARTS {
                    let mut g = out[pp].lock().unwrap();
                    if loc_linked[pp].len() > 0 {
                        g.linked.push(loc_linked[pp].take());
                    }
                    if loc_sing[pp].len() > 0 {
                        g.singletons.push(loc_sing[pp].take());
                    }
                    g.linked_count += n_linked[pp];
                    g.singleton_count += n_sing[pp];
                }
                out_arenas.lock().unwrap().push(arena);
            });
        }
    });

    let mut arenas = out_arenas.into_inner().unwrap();
    arenas.extend(parse_arenas);
    PersonEvents {
        parts: out.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        arenas,
    }
}
