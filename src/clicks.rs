use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::arena::{Arena, FixedBuf, RawChunk, Span};
use crate::hash::{PERSON_PARTS, write_raw, hash_bytes, person_part_from_comp, person_part_from_hash};
use crate::io_chunks::{
    DEFAULT_CHUNK_SIZE, for_each_record, for_each_record_read, list_csv_files, plan_chunks,
    use_pread,
};
use crate::links::LinkLookup;
use crate::lookups::Lookups;
use crate::timestamp::{YmCache, parse_timestamp_in_window};

const FLUSH: usize = 64 * 1024;
const PREFETCH_BATCH: usize = 64;

#[derive(Default)]
pub struct PersonClickPart {
    /// Chunks of `[comp_id: u32, ts_ms: u64, camp_idx: u32, cid_len: varint, cid_bytes]`
    pub linked_chunks: Vec<RawChunk>,
    pub linked_count: usize,
    /// Chunks of `[uid_hash: u64, uid_len: varint, uid_bytes, ts_ms: u64, camp_idx: u32, cid_len: varint, cid_bytes]`
    pub singleton_chunks: Vec<RawChunk>,
    pub singleton_count: usize,
}

/// Click partitions plus the arenas their chunks point into.
pub struct PersonClicks {
    pub parts: Vec<PersonClickPart>,
    pub arenas: Vec<Arena>,
}

impl PersonClickPart {
    pub fn total_clicks(&self) -> usize {
        self.linked_count + self.singleton_count
    }
}

#[derive(Clone, Copy)]
struct Pending<'a> {
    click_id: &'a [u8],
    uid: &'a [u8],
    uid_h: u64,
    ts_ms: i64,
    camp_idx: u32,
}

struct LocalBufs {
    arena: Arena,
    linked: Vec<(FixedBuf, usize)>,
    sing: Vec<(FixedBuf, usize)>,
}

struct Ctx<'c> {
    lookups: &'c Lookups,
    link_lookup: &'c LinkLookup,
    parts: &'c [Mutex<PersonClickPart>],
    has_links: bool,
    c_start: i64,
    c_end: i64,
    ym: YmCache,
}

#[inline(always)]
fn push_click(c: &Pending<'_>, comp: Option<u32>, local: &mut LocalBufs, parts: &[Mutex<PersonClickPart>]) {
    let (pp, slot, need) = match comp {
        Some(comp) => {
            let pp = person_part_from_comp(comp);
            (pp, unsafe { local.linked.get_unchecked_mut(pp) }, c.click_id.len() + 32)
        }
        None => {
            let pp = person_part_from_hash(c.uid_h);
            (pp, unsafe { local.sing.get_unchecked_mut(pp) }, c.click_id.len() + c.uid.len() + 48)
        }
    };
    let (buf, cnt) = slot;
    if let Some(full) = buf.reserve(need, FLUSH, &mut local.arena) {
        let mut g = parts[pp].lock().unwrap();
        if comp.is_some() {
            g.linked_chunks.push(full);
        } else {
            g.singleton_chunks.push(full);
        }
    }
    let w = unsafe {
        write_raw(buf.tail(), |w| match comp {
            Some(comp) => {
                w.u32(comp);
                w.u64(c.ts_ms as u64);
                w.u32(c.camp_idx);
                w.varint(c.click_id.len());
                w.bytes(c.click_id);
            }
            None => {
                w.u64(c.uid_h);
                w.varint(c.uid.len());
                w.bytes(c.uid);
                w.u64(c.ts_ms as u64);
                w.u32(c.camp_idx);
                w.varint(c.click_id.len());
                w.bytes(c.click_id);
            }
        })
    };
    buf.advance(w);
    *cnt += 1;
}

/// Validate and route every click of one chunk. `records` drives the line
/// iteration (mmap or pread) and calls back with each well-formed line.
fn click_chunk<'x>(
    records: impl FnOnce(&mut dyn FnMut([&'x [u8]; 4])),
    ctx: &Ctx<'_>,
    local: &mut LocalBufs,
) {
    let mut pending: Vec<Pending<'x>> = Vec::with_capacity(PREFETCH_BATCH);
    records(&mut |[click_id, uid, camp_id, ts_b]: [&'x [u8]; 4]| {
        if click_id.is_empty() || uid.is_empty() {
            return;
        }
        let Some(camp_idx) = ctx.lookups.lookup_campaign(camp_id) else {
            return;
        };
        let Some(ts_ms) = parse_timestamp_in_window(ts_b, ctx.c_start, ctx.c_end, &ctx.ym) else {
            return;
        };
        let p = Pending {
            click_id,
            uid,
            uid_h: hash_bytes(uid),
            ts_ms,
            camp_idx,
        };
        if ctx.has_links {
            pending.push(p);
            if pending.len() == PREFETCH_BATCH {
                flush_pending(&mut pending, ctx, local);
            }
        } else {
            push_click(&p, None, local, ctx.parts);
        }
    });
    flush_pending(&mut pending, ctx, local);
}

/// Resolve a batch of pending clicks through the link table and route them.
fn flush_pending(pending: &mut Vec<Pending<'_>>, ctx: &Ctx<'_>, local: &mut LocalBufs) {
    let n = pending.len();
    if n == 0 {
        return;
    }
    let mut uids: [&[u8]; PREFETCH_BATCH] = [&[]; PREFETCH_BATCH];
    let mut hs = [0u64; PREFETCH_BATCH];
    let mut comps = [None; PREFETCH_BATCH];
    for (k, c) in pending.iter().enumerate() {
        uids[k] = c.uid;
        hs[k] = c.uid_h;
    }
    ctx.link_lookup.lookup_batch(&uids[..n], &hs[..n], &mut comps[..n]);
    for (k, c) in pending.drain(..).enumerate() {
        push_click(&c, comps[k], local, ctx.parts);
    }
}

/// Phase 3: parse and validate `clicks/*.csv` and route each click to its
/// person partition. Clicks of people with no surviving events are dropped
/// later in the session phase. Dedup on `click_id` happens there too, because
/// copies of a click always carry identical values.
pub fn process_clicks(
    data_dir: &Path,
    lookups: &Lookups,
    link_lookup: &LinkLookup,
    ncpu: usize,
    spare: Vec<Span>,
) -> io::Result<PersonClicks> {
    let files = list_csv_files(&data_dir.join("clicks"))?;
    let (mapped, chunks, _) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    let parts: Vec<Mutex<PersonClickPart>> =
        (0..PERSON_PARTS).map(|_| Mutex::new(PersonClickPart::default())).collect();
    let arenas: Mutex<Vec<Arena>> = Mutex::new(Vec::new());

    if !chunks.is_empty() {
        let next_chunk = AtomicUsize::new(0);
        let workers = ncpu.min(chunks.len()).max(1);
        let ctx = Ctx {
            lookups,
            link_lookup,
            parts: &parts,
            has_links: !link_lookup.is_empty(),
            c_start: lookups.click_window_start_ms,
            c_end: lookups.window_end_ms,
            ym: lookups.ym_cache,
        };
        let pread = use_pread();
        // Deal the spare spans out round-robin so every worker gets a share.
        let mut shares: Vec<Vec<Span>> = (0..workers).map(|_| Vec::new()).collect();
        for (i, sp) in spare.into_iter().enumerate() {
            shares[i % workers].push(sp);
        }
        let shares: Vec<Mutex<Vec<Span>>> = shares.into_iter().map(Mutex::new).collect();

        thread::scope(|s| {
            for tid in 0..workers {
                let (mapped, chunks, next_chunk, parts, ctx, arenas, shares) =
                    (&mapped, &chunks, &next_chunk, &parts, &ctx, &arenas, &shares);
                s.spawn(move || {
                    let mut arena = Arena::default();
                    arena.add_spare(std::mem::take(&mut *shares[tid].lock().unwrap()));
                    let mut local = LocalBufs {
                        arena,
                        linked: (0..PERSON_PARTS).map(|_| (FixedBuf::empty(), 0)).collect(),
                        sing: (0..PERSON_PARTS).map(|_| (FixedBuf::empty(), 0)).collect(),
                    };
                    let mut rbuf: Vec<u8> = Vec::new();
                    loop {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = chunks.get(idx) else {
                            break;
                        };
                        let mf = &mapped[chunk.file_idx];
                        if pread {
                            click_chunk(
                                |cb| for_each_record_read::<4>(mf, chunk, &mut rbuf, |r| cb(r)),
                                ctx,
                                &mut local,
                            );
                        } else {
                            click_chunk(|cb| for_each_record::<4>(mf, chunk, |r| cb(r)), ctx, &mut local);
                        }
                    }

                    for pp in 0..PERSON_PARTS {
                        let (lb, lc) = &mut local.linked[pp];
                        let (sb, sc) = &mut local.sing[pp];
                        if *lc > 0 || *sc > 0 {
                            let mut g = parts[pp].lock().unwrap();
                            if lb.len() > 0 {
                                g.linked_chunks.push(lb.take());
                            }
                            if sb.len() > 0 {
                                g.singleton_chunks.push(sb.take());
                            }
                            g.linked_count += *lc;
                            g.singleton_count += *sc;
                        }
                    }
                    arenas.lock().unwrap().push(local.arena);
                });
            }
        });
    }

    Ok(PersonClicks {
        parts: parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        arenas: arenas.into_inner().unwrap(),
    })
}
