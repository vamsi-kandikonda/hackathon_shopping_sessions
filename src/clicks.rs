use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::hash::{PERSON_PARTS, hash_id, person_part_from_comp, person_part_from_hash};
use crate::io_chunks::{
    DEFAULT_CHUNK_SIZE, for_each_record, list_csv_files, plan_chunks,
};
use crate::links::LinkLookup;
use crate::lookups::Lookups;
use crate::timestamp::{YmCache, parse_timestamp_in_window};

const PREFETCH_BATCH: usize = 64;

#[derive(Clone, Copy)]
pub struct LinkedClick {
    pub comp_id: u32,
    pub camp_idx: u32,
    pub ts_ms: i64,
    pub cid_len: u8,
    pub _pad: [u8; 7],
    pub cid_bytes: [u8; 16],
}

impl LinkedClick {
    #[inline(always)]
    pub fn cid(&self) -> &[u8] {
        &self.cid_bytes[..self.cid_len as usize]
    }
}

#[derive(Clone, Copy)]
pub struct SingClick {
    pub uid_h: u64,
    pub ts_ms: i64,
    pub camp_idx: u32,
    pub cid_len: u8,
    pub _pad: [u8; 3],
    pub cid_bytes: [u8; 16],
}

impl SingClick {
    #[inline(always)]
    pub fn cid(&self) -> &[u8] {
        &self.cid_bytes[..self.cid_len as usize]
    }
}

#[derive(Default)]
pub struct PersonClickPart {
    pub linked: Vec<LinkedClick>,
    pub singletons: Vec<SingClick>,
}

impl PersonClickPart {
    #[inline(always)]
    pub fn total_clicks(&self) -> usize {
        self.linked.len() + self.singletons.len()
    }
}

pub struct PersonClicks {
    pub parts: Vec<PersonClickPart>,
}

#[derive(Clone, Copy)]
struct Pending {
    uid_h: u64,
    ts_ms: i64,
    camp_idx: u32,
    cid_len: u8,
    _pad: [u8; 3],
    cid_bytes: [u8; 16],
}

struct LocalBufs {
    linked: Vec<Vec<LinkedClick>>,
    sing: Vec<Vec<SingClick>>,
}

pub struct ClickCtx<'c> {
    pub lookups: &'c Lookups,
    pub link_lookup: &'c LinkLookup,
    pub has_links: bool,
    pub c_start: i64,
    pub c_end: i64,
    pub ym: YmCache,
}

#[inline(always)]
fn push_click(c: &Pending, comp: Option<u32>, local: &mut LocalBufs) {
    match comp {
        Some(comp) => {
            let pp = person_part_from_comp(comp);
            local.linked[pp].push(LinkedClick {
                comp_id: comp,
                camp_idx: c.camp_idx,
                ts_ms: c.ts_ms,
                cid_len: c.cid_len,
                _pad: [0; 7],
                cid_bytes: c.cid_bytes,
            });
        }
        None => {
            let pp = person_part_from_hash(c.uid_h);
            local.sing[pp].push(SingClick {
                uid_h: c.uid_h,
                ts_ms: c.ts_ms,
                camp_idx: c.camp_idx,
                cid_len: c.cid_len,
                _pad: [0; 3],
                cid_bytes: c.cid_bytes,
            });
        }
    }
}

fn flush_pending(pending: &mut Vec<Pending>, ctx: &ClickCtx<'_>, local: &mut LocalBufs) {
    let n = pending.len();
    if n == 0 {
        return;
    }
    let mut hs = [0u64; PREFETCH_BATCH];
    let mut comps = [None; PREFETCH_BATCH];
    for (k, c) in pending.iter().enumerate() {
        hs[k] = c.uid_h;
    }
    ctx.link_lookup.lookup_batch_hashes(&hs[..n], &mut comps[..n]);
    for (k, c) in pending.drain(..).enumerate() {
        push_click(&c, comps[k], local);
    }
}

pub struct ClickWorker {
    local: LocalBufs,
    pending: Vec<Pending>,
    last_h: u64,
    last_may: bool,
}

impl ClickWorker {
    pub fn new(cap_linked: usize, cap_sing: usize) -> Self {
        Self {
            local: LocalBufs {
                linked: (0..PERSON_PARTS).map(|_| Vec::with_capacity(cap_linked)).collect(),
                sing: (0..PERSON_PARTS).map(|_| Vec::with_capacity(cap_sing)).collect(),
            },
            pending: Vec::with_capacity(PREFETCH_BATCH),
            last_h: u64::MAX,
            last_may: false,
        }
    }

    pub fn process_chunk(&mut self, mf: &crate::io_chunks::MappedFile, chunk: &crate::io_chunks::FileChunk, ctx: &ClickCtx<'_>) {
        for_each_record::<4>(mf, chunk, |[click_id, uid, camp_id, ts_b]| {
            if click_id.is_empty() || uid.is_empty() {
                return;
            }
            let Some(camp_idx) = ctx.lookups.lookup_campaign(camp_id) else {
                return;
            };
            let Some(ts_ms) = parse_timestamp_in_window(ts_b, ctx.c_start, ctx.c_end, &ctx.ym) else {
                return;
            };

            let cid_len = click_id.len().min(16) as u8;
            let mut cid_bytes = [0u8; 16];
            let clen = cid_len as usize;
            if (8..=16).contains(&clen) {
                unsafe {
                    let p = click_id.as_ptr();
                    let dest = cid_bytes.as_mut_ptr();
                    *(dest as *mut u64) = *(p as *const u64);
                    *(dest.add(clen - 8) as *mut u64) = *(p.add(clen - 8) as *const u64);
                }
            } else {
                cid_bytes[..clen].copy_from_slice(&click_id[..clen]);
            }

            let uid_h = hash_id(uid);
            let p = Pending {
                uid_h,
                ts_ms,
                camp_idx,
                cid_len,
                _pad: [0; 3],
                cid_bytes,
            };

            if ctx.has_links {
                let may = if uid_h == self.last_h {
                    self.last_may
                } else {
                    let m = ctx.link_lookup.may_contain(uid_h);
                    self.last_h = uid_h;
                    self.last_may = m;
                    m
                };
                if may {
                    self.pending.push(p);
                    if self.pending.len() == PREFETCH_BATCH {
                        flush_pending(&mut self.pending, ctx, &mut self.local);
                    }
                } else {
                    push_click(&p, None, &mut self.local);
                }
            } else {
                push_click(&p, None, &mut self.local);
            }
        });
    }

    pub fn finish(mut self, parts: &[Mutex<PersonClickPart>], ctx: &ClickCtx<'_>) {
        flush_pending(&mut self.pending, ctx, &mut self.local);
        for pp in 0..PERSON_PARTS {
            let mut g = parts[pp].lock().unwrap();
            if !self.local.linked[pp].is_empty() {
                g.linked.extend_from_slice(&self.local.linked[pp]);
            }
            if !self.local.sing[pp].is_empty() {
                g.singletons.extend_from_slice(&self.local.sing[pp]);
            }
        }
    }
}

pub fn process_clicks(
    data_dir: &Path,
    lookups: &Lookups,
    link_lookup: &LinkLookup,
    ncpu: usize,
) -> io::Result<PersonClicks> {
    let files = list_csv_files(&data_dir.join("clicks"))?;
    let (mapped, chunks, total_bytes) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    let workers = ncpu.min(chunks.len()).max(1);
    let est_clicks = (total_bytes / 45) as usize;
    let per_click_part = (est_clicks / PERSON_PARTS).max(256);
    let per_click_worker = (per_click_part / workers).max(64) * 12 / 10;
    let cap_linked = (per_click_worker * 40 / 100).max(512);
    let cap_sing = (per_click_worker * 140 / 100).max(1024);

    let parts: Vec<Mutex<PersonClickPart>> = (0..PERSON_PARTS)
        .map(|_| Mutex::new(PersonClickPart::default()))
        .collect();

    if !chunks.is_empty() {
        let next_chunk = AtomicUsize::new(0);
        let ctx = ClickCtx {
            lookups,
            link_lookup,
            has_links: !link_lookup.is_empty(),
            c_start: lookups.click_window_start_ms,
            c_end: lookups.window_end_ms,
            ym: lookups.ym_cache,
        };

        thread::scope(|s| {
            for _ in 0..workers {
                let (mapped, chunks, next_chunk, parts, ctx) =
                    (&mapped, &chunks, &next_chunk, &parts, &ctx);
                s.spawn(move || {
                    let mut worker = ClickWorker::new(cap_linked, cap_sing);
                    loop {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = chunks.get(idx) else {
                            break;
                        };
                        worker.process_chunk(&mapped[chunk.file_idx], chunk, ctx);
                    }
                    worker.finish(parts, ctx);
                });
            }
        });
    }

    Ok(PersonClicks {
        parts: parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
    })
}
