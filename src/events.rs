use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::csv_parse::split_fields;
use crate::hash::{
    ActiveBloom, EVENT_PARTS, PERSON_PARTS, event_part, hash_bytes, mix_u32, person_part_from_comp,
    person_part_from_hash, prefetch_read, read_varint, write_varint,
};
use crate::io_chunks::{DEFAULT_CHUNK_SIZE, for_each_line_in_chunk, list_csv_files, plan_chunks};
use crate::links::LinkLookup;
use crate::lookups::Lookups;
use crate::timestamp::{
    LATE_CUTOFF_MS, parse_f1_sec_fast, parse_price_cents, parse_timestamp_in_window,
};

const TAG_VALID: u8 = 1 << 0;
const TAG_LINKED: u8 = 1 << 1;
const TAG_PURCHASE: u8 = 1 << 2;

const FLUSH_THRESHOLD: usize = 64 * 1024;
const PREFETCH_BATCH: usize = 32;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct LinkedCompactEvent {
    pub comp_id: u32,
    pub ts_rel: u32,
    pub price_cat: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct LinkedWideEvent {
    pub comp_id: u32,
    pub price_cents: u32,
    pub cat_id: u32,
    pub _pad: u32,
    pub ts_ms: i64,
}

pub struct CompactPersonPart {
    pub linked: Vec<LinkedCompactEvent>,
    /// Chunks of `[uid_len: varint, uid_bytes, ts_rel: u32, price_cat: u32]`
    pub singletons: Vec<Vec<u8>>,
    pub singleton_count: usize,
}

impl Default for CompactPersonPart {
    fn default() -> Self {
        Self {
            linked: Vec::new(),
            singletons: Vec::new(),
            singleton_count: 0,
        }
    }
}

pub struct WidePersonPart {
    pub linked: Vec<LinkedWideEvent>,
    /// Chunks of `[uid_len: varint, uid_bytes, ts_ms: i64, price_cents: u32, cat_id: u32]`
    pub singletons: Vec<Vec<u8>>,
    pub singleton_count: usize,
}

impl Default for WidePersonPart {
    fn default() -> Self {
        Self {
            linked: Vec::new(),
            singletons: Vec::new(),
            singleton_count: 0,
        }
    }
}

pub enum PersonEventStore {
    Compact(Vec<CompactPersonPart>),
    Wide(Vec<WidePersonPart>),
}

impl PersonEventStore {
    pub fn part_event_count(&self, pp: usize) -> usize {
        match self {
            PersonEventStore::Compact(v) => v[pp].linked.len() + v[pp].singleton_count,
            PersonEventStore::Wide(v) => v[pp].linked.len() + v[pp].singleton_count,
        }
    }
}

/// Pending parsed valid event waiting for prefetched `LinkLookup` slot to land in L1 cache.
#[derive(Clone, Copy)]
struct PendingEvent<'a> {
    eid: &'a [u8],
    uid: &'a [u8],
    uid_h: u64,
    ep: usize,
    ingest_sec: i64,
    ts_ms: i64,
    price_cents: u32,
    cat_id: u32,
    is_purchase: bool,
}

#[inline(always)]
fn flush_pending_batch(
    pending: &mut Vec<PendingEvent<'_>>,
    local_bufs: &mut [Vec<u8>],
    ev_parts: &[Mutex<Vec<Vec<u8>>>],
    link_lookup: &LinkLookup,
    win_start: i64,
    compact_mode: bool,
) {
    let mut stack = [0u8; 128];
    for p in pending.drain(..) {
        let linked = link_lookup.lookup(p.uid, p.uid_h);
        let mut tag = TAG_VALID;
        if linked.is_some() {
            tag |= TAG_LINKED;
        }
        if p.is_purchase {
            tag |= TAG_PURCHASE;
        }
        let buf = unsafe { local_bufs.get_unchecked_mut(p.ep) };

        // Fast stack-buffered single-copy path for typical records (99.999%+ of rows)
        if p.eid.len() < 128 && p.uid.len() < 128 && (32 + p.eid.len() + p.uid.len()) <= 128 {
            let mut n = 0usize;
            unsafe {
                *stack.get_unchecked_mut(n) = tag;
                n += 1;
                *stack.get_unchecked_mut(n) = p.eid.len() as u8;
                n += 1;
                std::ptr::copy_nonoverlapping(p.eid.as_ptr(), stack.as_mut_ptr().add(n), p.eid.len());
                n += p.eid.len();
                std::ptr::copy_nonoverlapping(
                    p.ingest_sec.to_le_bytes().as_ptr(),
                    stack.as_mut_ptr().add(n),
                    8,
                );
                n += 8;
                if compact_mode {
                    let ts_rel = (p.ts_ms - win_start) as u32;
                    std::ptr::copy_nonoverlapping(
                        ts_rel.to_le_bytes().as_ptr(),
                        stack.as_mut_ptr().add(n),
                        4,
                    );
                    n += 4;
                    if p.is_purchase {
                        let price_cat = (p.cat_id << 24) | p.price_cents;
                        std::ptr::copy_nonoverlapping(
                            price_cat.to_le_bytes().as_ptr(),
                            stack.as_mut_ptr().add(n),
                            4,
                        );
                        n += 4;
                    }
                } else {
                    std::ptr::copy_nonoverlapping(
                        p.ts_ms.to_le_bytes().as_ptr(),
                        stack.as_mut_ptr().add(n),
                        8,
                    );
                    n += 8;
                    if p.is_purchase {
                        std::ptr::copy_nonoverlapping(
                            p.price_cents.to_le_bytes().as_ptr(),
                            stack.as_mut_ptr().add(n),
                            4,
                        );
                        n += 4;
                        std::ptr::copy_nonoverlapping(
                            p.cat_id.to_le_bytes().as_ptr(),
                            stack.as_mut_ptr().add(n),
                            4,
                        );
                        n += 4;
                    }
                }
                if let Some(comp_id) = linked {
                    std::ptr::copy_nonoverlapping(
                        comp_id.to_le_bytes().as_ptr(),
                        stack.as_mut_ptr().add(n),
                        4,
                    );
                    n += 4;
                } else {
                    *stack.get_unchecked_mut(n) = p.uid.len() as u8;
                    n += 1;
                    std::ptr::copy_nonoverlapping(
                        p.uid.as_ptr(),
                        stack.as_mut_ptr().add(n),
                        p.uid.len(),
                    );
                    n += p.uid.len();
                }
            }
            buf.extend_from_slice(&stack[..n]);
        } else {
            buf.push(tag);
            write_varint(buf, p.eid.len());
            buf.extend_from_slice(p.eid);
            buf.extend_from_slice(&p.ingest_sec.to_le_bytes());
            if compact_mode {
                let ts_rel = (p.ts_ms - win_start) as u32;
                buf.extend_from_slice(&ts_rel.to_le_bytes());
                if p.is_purchase {
                    let price_cat = (p.cat_id << 24) | p.price_cents;
                    buf.extend_from_slice(&price_cat.to_le_bytes());
                }
            } else {
                buf.extend_from_slice(&p.ts_ms.to_le_bytes());
                if p.is_purchase {
                    buf.extend_from_slice(&p.price_cents.to_le_bytes());
                    buf.extend_from_slice(&p.cat_id.to_le_bytes());
                }
            }
            if let Some(comp_id) = linked {
                buf.extend_from_slice(&comp_id.to_le_bytes());
            } else {
                write_varint(buf, p.uid.len());
                buf.extend_from_slice(p.uid);
            }
        }

        if buf.len() >= FLUSH_THRESHOLD {
            let flushed = std::mem::replace(buf, Vec::with_capacity(FLUSH_THRESHOLD + 256));
            ev_parts[p.ep].lock().unwrap().push(flushed);
        }
    }
}

/// Phase 2: Parse all `events/*.csv`, dedup on `event_id` keeping smallest `ingest_ts`,
/// validate & drop late events, and route surviving events into `PERSON_PARTS` buckets.
pub fn process_events(
    data_dir: &Path,
    lookups: &Lookups,
    link_lookup: &LinkLookup,
    ncpu: usize,
) -> io::Result<(PersonEventStore, ActiveBloom)> {
    let files = list_csv_files(&data_dir.join("events"))?;
    let (opened, chunks, total_bytes) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    let bloom = ActiveBloom::new_for_bytes(total_bytes);

    if chunks.is_empty() || total_bytes == 0 {
        let store = if lookups.compact_mode {
            PersonEventStore::Compact((0..PERSON_PARTS).map(|_| Default::default()).collect())
        } else {
            PersonEventStore::Wide((0..PERSON_PARTS).map(|_| Default::default()).collect())
        };
        return Ok((store, bloom));
    }

    let ev_parts: Vec<Mutex<Vec<Vec<u8>>>> = (0..EVENT_PARTS)
        .map(|_| Mutex::new(Vec::new()))
        .collect();
    let next_chunk = AtomicUsize::new(0);
    let workers = ncpu.min(chunks.len()).max(1);

    let win_start = lookups.window_start_ms;
    let win_end = lookups.window_end_ms;
    let compact_mode = lookups.compact_mode;
    let ym_cache = lookups.ym_cache;
    let has_links = !link_lookup.is_empty();
    let t_ev0 = std::time::Instant::now();

    thread::scope(|s| {
        for _ in 0..workers {
            let opened = &opened;
            let chunks = &chunks;
            let next_chunk = &next_chunk;
            let ev_parts = &ev_parts;
            s.spawn(move || {
                let mut local_bufs: Vec<Vec<u8>> = (0..EVENT_PARTS)
                    .map(|_| Vec::with_capacity(FLUSH_THRESHOLD + 256))
                    .collect();

                loop {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = chunks.get(idx) else {
                        break;
                    };
                    let mut pending: Vec<PendingEvent<'_>> = Vec::with_capacity(PREFETCH_BATCH);
                    for_each_line_in_chunk(&opened[chunk.file_idx], chunk, |line| {
                        let Some([eid, uid, ts_b, ingest_b, etype, pid, price_b]) =
                            split_fields::<7>(line)
                        else {
                            return;
                        };
                        if eid.is_empty() {
                            return;
                        }
                        let Some(ingest_sec) = parse_f1_sec_fast(ingest_b, &ym_cache) else {
                            return;
                        };

                        let eh = hash_bytes(eid);
                        let ep = event_part(eh);

                        let mut is_valid = false;
                        let mut is_purchase = false;
                        let mut ts_ms = 0i64;
                        let mut price_cents = 0u32;
                        let mut cat_id = 0u32;

                        if !uid.is_empty() && !pid.is_empty() {
                            let valid_type = match etype {
                                b"view" | b"cart" => Some(false),
                                b"purchase" => Some(true),
                                _ => None,
                            };
                            if let Some(purch) = valid_type {
                                if let Some(ts) = parse_timestamp_in_window(
                                    ts_b, win_start, win_end, &ym_cache,
                                ) {
                                    let ingest_ms = ingest_sec.saturating_mul(1000);
                                    if ingest_ms.saturating_sub(ts) <= LATE_CUTOFF_MS {
                                        if purch {
                                            if let Some(p) = parse_price_cents(price_b) {
                                                is_valid = true;
                                                is_purchase = true;
                                                ts_ms = ts;
                                                price_cents = p;
                                                cat_id = lookups.lookup_product_cat(pid);
                                            }
                                        } else {
                                            is_valid = true;
                                            ts_ms = ts;
                                        }
                                    }
                                }
                            }
                        }

                        if !is_valid {
                            let buf = unsafe { local_bufs.get_unchecked_mut(ep) };
                            buf.push(0u8);
                            write_varint(buf, eid.len());
                            buf.extend_from_slice(eid);
                            buf.extend_from_slice(&ingest_sec.to_le_bytes());
                            if buf.len() >= FLUSH_THRESHOLD {
                                let flushed = std::mem::replace(
                                    buf,
                                    Vec::with_capacity(FLUSH_THRESHOLD + 256),
                                );
                                ev_parts[ep].lock().unwrap().push(flushed);
                            }
                        } else if has_links {
                            let uid_h = hash_bytes(uid);
                            link_lookup.prefetch(uid_h);
                            pending.push(PendingEvent {
                                eid,
                                uid,
                                uid_h,
                                ep,
                                ingest_sec,
                                ts_ms,
                                price_cents,
                                cat_id,
                                is_purchase,
                            });
                            if pending.len() == PREFETCH_BATCH {
                                flush_pending_batch(
                                    &mut pending,
                                    &mut local_bufs,
                                    ev_parts,
                                    link_lookup,
                                    win_start,
                                    compact_mode,
                                );
                            }
                        } else {
                            // No links at all: write singleton directly without hash/lookup
                            let buf = unsafe { local_bufs.get_unchecked_mut(ep) };
                            let mut tag = TAG_VALID;
                            if is_purchase {
                                tag |= TAG_PURCHASE;
                            }
                            buf.push(tag);
                            write_varint(buf, eid.len());
                            buf.extend_from_slice(eid);
                            buf.extend_from_slice(&ingest_sec.to_le_bytes());
                            if compact_mode {
                                let ts_rel = (ts_ms - win_start) as u32;
                                buf.extend_from_slice(&ts_rel.to_le_bytes());
                                if is_purchase {
                                    let price_cat = (cat_id << 24) | price_cents;
                                    buf.extend_from_slice(&price_cat.to_le_bytes());
                                }
                            } else {
                                buf.extend_from_slice(&ts_ms.to_le_bytes());
                                if is_purchase {
                                    buf.extend_from_slice(&price_cents.to_le_bytes());
                                    buf.extend_from_slice(&cat_id.to_le_bytes());
                                }
                            }
                            write_varint(buf, uid.len());
                            buf.extend_from_slice(uid);
                            if buf.len() >= FLUSH_THRESHOLD {
                                let flushed = std::mem::replace(
                                    buf,
                                    Vec::with_capacity(FLUSH_THRESHOLD + 256),
                                );
                                ev_parts[ep].lock().unwrap().push(flushed);
                            }
                        }
                    });
                    if !pending.is_empty() {
                        flush_pending_batch(
                            &mut pending,
                            &mut local_bufs,
                            ev_parts,
                            link_lookup,
                            win_start,
                            compact_mode,
                        );
                    }
                }

                for (ep, buf) in local_bufs.into_iter().enumerate() {
                    if !buf.is_empty() {
                        ev_parts[ep].lock().unwrap().push(buf);
                    }
                }
            });
        }
    });

    drop(opened);
    let t_ev1 = std::time::Instant::now();
    if std::env::var("SS_TIMING").is_ok() {
        eprintln!(
            "  events_2a_parse={:.1}ms",
            (t_ev1 - t_ev0).as_secs_f64() * 1000.0
        );
    }

    // Phase 2b: Parallel dedup across EVENT_PARTS -> scatter surviving events to PERSON_PARTS
    let next_ep = AtomicUsize::new(0);

    if compact_mode {
        let p_parts: Vec<Mutex<CompactPersonPart>> = (0..PERSON_PARTS)
            .map(|_| Mutex::new(CompactPersonPart::default()))
            .collect();

        thread::scope(|s| {
            for _ in 0..ncpu.max(1) {
                let ev_parts = &ev_parts;
                let next_ep = &next_ep;
                let p_parts = &p_parts;
                let bloom = &bloom;
                s.spawn(move || {
                    let mut dedup_slots: Vec<(u32, u32)> = Vec::new();
                    let mut entries: Vec<DedupRecCompact> = Vec::new();
                    let mut local_linked: Vec<Vec<LinkedCompactEvent>> =
                        (0..PERSON_PARTS).map(|_| Vec::new()).collect();
                    let mut local_sing: Vec<(Vec<u8>, usize)> =
                        (0..PERSON_PARTS).map(|_| (Vec::new(), 0)).collect();

                    loop {
                        let ep = next_ep.fetch_add(1, Ordering::Relaxed);
                        if ep >= EVENT_PARTS {
                            break;
                        }
                        let chunks = std::mem::take(&mut *ev_parts[ep].lock().unwrap());
                        if chunks.is_empty() {
                            continue;
                        }

                        dedup_compact_partition(&chunks, &mut dedup_slots, &mut entries);

                        for rec in &entries {
                            if !rec.is_valid() {
                                continue;
                            }
                            if rec.is_linked() {
                                let comp_id = rec.user_a;
                                let pp = person_part_from_comp(comp_id);
                                bloom.insert(mix_u32(comp_id));
                                let v = unsafe { local_linked.get_unchecked_mut(pp) };
                                v.push(LinkedCompactEvent {
                                    comp_id,
                                    ts_rel: rec.ts_rel,
                                    price_cat: rec.price_cat,
                                });
                                if v.len() >= 4096 {
                                    let mut g = p_parts[pp].lock().unwrap();
                                    g.linked.extend_from_slice(v);
                                    v.clear();
                                }
                            } else {
                                let chunk_idx = rec.chunk_idx();
                                let uid_off = rec.user_a as usize;
                                let uid_len = rec.user_b as usize;
                                let uid = &chunks[chunk_idx][uid_off..uid_off + uid_len];
                                let uh = hash_bytes(uid);
                                let pp = person_part_from_hash(uh);
                                bloom.insert(uh);
                                let (buf, cnt) = unsafe { local_sing.get_unchecked_mut(pp) };
                                write_varint(buf, uid_len);
                                buf.extend_from_slice(uid);
                                buf.extend_from_slice(&rec.ts_rel.to_le_bytes());
                                buf.extend_from_slice(&rec.price_cat.to_le_bytes());
                                *cnt += 1;
                                if buf.len() >= FLUSH_THRESHOLD {
                                    let flushed = std::mem::replace(
                                        buf,
                                        Vec::with_capacity(FLUSH_THRESHOLD),
                                    );
                                    let c = *cnt;
                                    *cnt = 0;
                                    let mut g = p_parts[pp].lock().unwrap();
                                    g.singletons.push(flushed);
                                    g.singleton_count += c;
                                }
                            }
                        }
                    }

                    for pp in 0..PERSON_PARTS {
                        if !local_linked[pp].is_empty() || local_sing[pp].1 > 0 {
                            let mut g = p_parts[pp].lock().unwrap();
                            if !local_linked[pp].is_empty() {
                                g.linked.extend_from_slice(&local_linked[pp]);
                            }
                            if local_sing[pp].1 > 0 {
                                let buf = std::mem::take(&mut local_sing[pp].0);
                                g.singletons.push(buf);
                                g.singleton_count += local_sing[pp].1;
                            }
                        }
                    }
                });
            }
        });

        let parts = p_parts
            .into_iter()
            .map(|m| m.into_inner().unwrap())
            .collect();
        Ok((PersonEventStore::Compact(parts), bloom))
    } else {
        let p_parts: Vec<Mutex<WidePersonPart>> = (0..PERSON_PARTS)
            .map(|_| Mutex::new(WidePersonPart::default()))
            .collect();

        thread::scope(|s| {
            for _ in 0..ncpu.max(1) {
                let ev_parts = &ev_parts;
                let next_ep = &next_ep;
                let p_parts = &p_parts;
                let bloom = &bloom;
                s.spawn(move || {
                    let mut dedup_slots: Vec<(u32, u32)> = Vec::new();
                    let mut entries: Vec<DedupRecWide> = Vec::new();
                    let mut local_linked: Vec<Vec<LinkedWideEvent>> =
                        (0..PERSON_PARTS).map(|_| Vec::new()).collect();
                    let mut local_sing: Vec<(Vec<u8>, usize)> =
                        (0..PERSON_PARTS).map(|_| (Vec::new(), 0)).collect();

                    loop {
                        let ep = next_ep.fetch_add(1, Ordering::Relaxed);
                        if ep >= EVENT_PARTS {
                            break;
                        }
                        let chunks = std::mem::take(&mut *ev_parts[ep].lock().unwrap());
                        if chunks.is_empty() {
                            continue;
                        }

                        dedup_wide_partition(&chunks, &mut dedup_slots, &mut entries);

                        for rec in &entries {
                            if !rec.valid {
                                continue;
                            }
                            if rec.linked {
                                let comp_id = rec.user_a;
                                let pp = person_part_from_comp(comp_id);
                                bloom.insert(mix_u32(comp_id));
                                local_linked[pp].push(LinkedWideEvent {
                                    comp_id,
                                    price_cents: rec.price_cents,
                                    cat_id: rec.cat_id,
                                    _pad: 0,
                                    ts_ms: rec.ts_ms,
                                });
                            } else {
                                let chunk_idx = rec.chunk_idx as usize;
                                let uid_off = rec.user_a as usize;
                                let uid_len = rec.user_b as usize;
                                let uid = &chunks[chunk_idx][uid_off..uid_off + uid_len];
                                let uh = hash_bytes(uid);
                                let pp = person_part_from_hash(uh);
                                bloom.insert(uh);
                                let (buf, cnt) = &mut local_sing[pp];
                                write_varint(buf, uid_len);
                                buf.extend_from_slice(uid);
                                buf.extend_from_slice(&rec.ts_ms.to_le_bytes());
                                buf.extend_from_slice(&rec.price_cents.to_le_bytes());
                                buf.extend_from_slice(&rec.cat_id.to_le_bytes());
                                *cnt += 1;
                            }
                        }
                    }

                    for pp in 0..PERSON_PARTS {
                        if !local_linked[pp].is_empty() || local_sing[pp].1 > 0 {
                            let mut g = p_parts[pp].lock().unwrap();
                            if !local_linked[pp].is_empty() {
                                g.linked.extend_from_slice(&local_linked[pp]);
                            }
                            if local_sing[pp].1 > 0 {
                                let buf = std::mem::take(&mut local_sing[pp].0);
                                g.singletons.push(buf);
                                g.singleton_count += local_sing[pp].1;
                            }
                        }
                    }
                });
            }
        });

        let parts = p_parts
            .into_iter()
            .map(|m| m.into_inner().unwrap())
            .collect();
        Ok((PersonEventStore::Wide(parts), bloom))
    }
}

/// 32-byte compact dedup entry (top 2 bits of `chunk_flags`: bit 31 = valid, bit 30 = linked).
#[derive(Clone, Copy)]
#[repr(C)]
struct DedupRecCompact {
    ingest_sec: i64,
    chunk_flags: u32,
    eid_off: u32,
    eid_len: u32,
    ts_rel: u32,
    price_cat: u32,
    user_a: u32,
    user_b: u32,
}

impl DedupRecCompact {
    #[inline(always)]
    fn make_chunk_flags(chunk_idx: usize, valid: bool, linked: bool) -> u32 {
        (chunk_idx as u32) | ((valid as u32) << 31) | ((linked as u32) << 30)
    }
    #[inline(always)]
    fn is_valid(&self) -> bool {
        (self.chunk_flags & (1 << 31)) != 0
    }
    #[inline(always)]
    fn is_linked(&self) -> bool {
        (self.chunk_flags & (1 << 30)) != 0
    }
    #[inline(always)]
    fn chunk_idx(&self) -> usize {
        (self.chunk_flags & 0x3FFF_FFFF) as usize
    }
}

#[derive(Clone, Copy)]
struct DedupRecWide {
    ingest_sec: i64,
    ts_ms: i64,
    chunk_idx: u32,
    eid_off: u32,
    eid_len: u32,
    price_cents: u32,
    cat_id: u32,
    user_a: u32,
    user_b: u32,
    valid: bool,
    linked: bool,
}

#[inline(always)]
fn grow_dedup_table(slots: &mut Vec<(u32, u32)>, rehash_fn: impl Fn(usize) -> u64, count: usize) {
    let new_cap = (slots.len() * 2).max(1024);
    slots.clear();
    slots.resize(new_cap, (0, 0));
    let mask = new_cap - 1;
    for i in 0..count {
        let h = rehash_fn(i);
        let tag = ((h >> 32) as u32) | 1;
        let mut idx = (h as usize) & mask;
        while slots[idx].0 != 0 {
            idx = (idx + 1) & mask;
        }
        slots[idx] = (tag, (i as u32) + 1);
    }
}

fn dedup_compact_partition(
    chunks: &[Vec<u8>],
    slots: &mut Vec<(u32, u32)>,
    entries: &mut Vec<DedupRecCompact>,
) {
    entries.clear();
    let total_bytes: usize = chunks.iter().map(|c| c.len()).sum();
    let est_items = (total_bytes / 26).max(64);
    let init_cap = (est_items * 2).next_power_of_two().max(1024);
    slots.clear();
    slots.resize(init_cap, (0, 0));
    let mut mask = init_cap - 1;
    let mut threshold = (init_cap * 7) / 10;

    // Batched decode + prefetch into dedup hash table to hide L3 cache misses!
    let mut batch: Vec<(u64, DedupRecCompact)> = Vec::with_capacity(PREFETCH_BATCH);

    let flush_dedup_batch = |batch: &mut Vec<(u64, DedupRecCompact)>,
                             slots: &mut Vec<(u32, u32)>,
                             entries: &mut Vec<DedupRecCompact>,
                             mask: &mut usize,
                             threshold: &mut usize| {
        for (h, rec) in batch.drain(..) {
            if entries.len() >= *threshold {
                grow_dedup_table(
                    slots,
                    |i| {
                        let e = &entries[i];
                        let s = &chunks[e.chunk_idx()]
                            [e.eid_off as usize..(e.eid_off + e.eid_len) as usize];
                        hash_bytes(s)
                    },
                    entries.len(),
                );
                *mask = slots.len() - 1;
                *threshold = (slots.len() * 7) / 10;
            }

            let htag = ((h >> 32) as u32) | 1;
            let mut idx = (h as usize) & *mask;
            let eid = &chunks[rec.chunk_idx()]
                [rec.eid_off as usize..(rec.eid_off + rec.eid_len) as usize];
            loop {
                let (stag, sidx) = unsafe { *slots.get_unchecked(idx) };
                if stag == 0 {
                    let new_idx = entries.len() as u32;
                    entries.push(rec);
                    unsafe {
                        *slots.get_unchecked_mut(idx) = (htag, new_idx + 1);
                    }
                    break;
                }
                if stag == htag {
                    let e = unsafe { entries.get_unchecked_mut((sidx - 1) as usize) };
                    if e.eid_len == rec.eid_len {
                        let existing_eid = &chunks[e.chunk_idx()]
                            [e.eid_off as usize..(e.eid_off + e.eid_len) as usize];
                        if existing_eid == eid {
                            if rec.ingest_sec < e.ingest_sec {
                                *e = rec;
                            }
                            break;
                        }
                    }
                }
                idx = (idx + 1) & *mask;
            }
        }
    };

    for (ci, chunk) in chunks.iter().enumerate() {
        let data = chunk.as_slice();
        let mut pos = 0usize;
        let len = data.len();
        while pos < len {
            let tag = unsafe { *data.get_unchecked(pos) };
            pos += 1;
            let eid_len = read_varint(data, &mut pos);
            let eid_off = pos;
            let eid = unsafe { data.get_unchecked(eid_off..eid_off + eid_len) };
            pos += eid_len;
            let ingest_sec =
                i64::from_le_bytes(unsafe { *(data.as_ptr().add(pos) as *const [u8; 8]) });
            pos += 8;

            let valid = (tag & TAG_VALID) != 0;
            let linked = (tag & TAG_LINKED) != 0;
            let is_purchase = (tag & TAG_PURCHASE) != 0;

            let mut ts_rel = 0u32;
            let mut price_cat = 0u32;
            let mut user_a = 0u32;
            let mut user_b = 0u32;

            if valid {
                ts_rel =
                    u32::from_le_bytes(unsafe { *(data.as_ptr().add(pos) as *const [u8; 4]) });
                pos += 4;
                if is_purchase {
                    price_cat =
                        u32::from_le_bytes(unsafe { *(data.as_ptr().add(pos) as *const [u8; 4]) });
                    pos += 4;
                }
                if linked {
                    user_a =
                        u32::from_le_bytes(unsafe { *(data.as_ptr().add(pos) as *const [u8; 4]) });
                    pos += 4;
                } else {
                    let ulen = read_varint(data, &mut pos);
                    user_a = pos as u32;
                    user_b = ulen as u32;
                    pos += ulen;
                }
            }

            let h = hash_bytes(eid);
            prefetch_read(unsafe { slots.as_ptr().add((h as usize) & mask) });
            batch.push((
                h,
                DedupRecCompact {
                    ingest_sec,
                    chunk_flags: DedupRecCompact::make_chunk_flags(ci, valid, linked),
                    eid_off: eid_off as u32,
                    eid_len: eid_len as u32,
                    ts_rel,
                    price_cat,
                    user_a,
                    user_b,
                },
            ));
            if batch.len() == PREFETCH_BATCH {
                flush_dedup_batch(&mut batch, slots, entries, &mut mask, &mut threshold);
            }
        }
    }
    if !batch.is_empty() {
        flush_dedup_batch(&mut batch, slots, entries, &mut mask, &mut threshold);
    }
}

fn dedup_wide_partition(
    chunks: &[Vec<u8>],
    slots: &mut Vec<(u32, u32)>,
    entries: &mut Vec<DedupRecWide>,
) {
    entries.clear();
    let total_bytes: usize = chunks.iter().map(|c| c.len()).sum();
    let est_items = (total_bytes / 32).max(64);
    let init_cap = (est_items * 2).next_power_of_two().max(1024);
    slots.clear();
    slots.resize(init_cap, (0, 0));
    let mut mask = init_cap - 1;
    let mut threshold = (init_cap * 7) / 10;

    for (ci, chunk) in chunks.iter().enumerate() {
        let data = chunk.as_slice();
        let mut pos = 0usize;
        let len = data.len();
        while pos < len {
            let tag = data[pos];
            pos += 1;
            let eid_len = read_varint(data, &mut pos);
            let eid_off = pos;
            let eid = &data[eid_off..eid_off + eid_len];
            pos += eid_len;
            let ingest_sec = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let valid = (tag & TAG_VALID) != 0;
            let linked = (tag & TAG_LINKED) != 0;
            let is_purchase = (tag & TAG_PURCHASE) != 0;

            let mut ts_ms = 0i64;
            let mut price_cents = 0u32;
            let mut cat_id = 0u32;
            let mut user_a = 0u32;
            let mut user_b = 0u32;

            if valid {
                ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                if is_purchase {
                    price_cents = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    cat_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                }
                if linked {
                    user_a = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                } else {
                    let ulen = read_varint(data, &mut pos);
                    user_a = pos as u32;
                    user_b = ulen as u32;
                    pos += ulen;
                }
            }

            if entries.len() >= threshold {
                grow_dedup_table(
                    slots,
                    |i| {
                        let e = &entries[i];
                        let s = &chunks[e.chunk_idx as usize]
                            [e.eid_off as usize..(e.eid_off + e.eid_len) as usize];
                        hash_bytes(s)
                    },
                    entries.len(),
                );
                mask = slots.len() - 1;
                threshold = (slots.len() * 7) / 10;
            }

            let h = hash_bytes(eid);
            let htag = ((h >> 32) as u32) | 1;
            let mut idx = (h as usize) & mask;
            loop {
                let (stag, sidx) = unsafe { *slots.get_unchecked(idx) };
                if stag == 0 {
                    let new_idx = entries.len() as u32;
                    entries.push(DedupRecWide {
                        ingest_sec,
                        ts_ms,
                        chunk_idx: ci as u32,
                        eid_off: eid_off as u32,
                        eid_len: eid_len as u32,
                        price_cents,
                        cat_id,
                        user_a,
                        user_b,
                        valid,
                        linked,
                    });
                    slots[idx] = (htag, new_idx + 1);
                    break;
                }
                if stag == htag {
                    let e = &mut entries[(sidx - 1) as usize];
                    if e.eid_len as usize == eid_len {
                        let existing_eid = &chunks[e.chunk_idx as usize]
                            [e.eid_off as usize..(e.eid_off + e.eid_len) as usize];
                        if existing_eid == eid {
                            if ingest_sec < e.ingest_sec {
                                *e = DedupRecWide {
                                    ingest_sec,
                                    ts_ms,
                                    chunk_idx: ci as u32,
                                    eid_off: eid_off as u32,
                                    eid_len: eid_len as u32,
                                    price_cents,
                                    cat_id,
                                    user_a,
                                    user_b,
                                    valid,
                                    linked,
                                };
                            }
                            break;
                        }
                    }
                }
                idx = (idx + 1) & mask;
            }
        }
    }
}
