use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::csv_parse::split_fields;
use crate::hash::{
    ActiveBloom, PERSON_PARTS, hash_bytes, mix_u32, person_part_from_comp, person_part_from_hash,
    write_varint,
};
use crate::io_chunks::{DEFAULT_CHUNK_SIZE, for_each_line_in_chunk, list_csv_files, plan_chunks};
use crate::links::LinkLookup;
use crate::lookups::Lookups;
use crate::timestamp::parse_timestamp_in_window;

const FLUSH_THRESHOLD: usize = 64 * 1024;
const PREFETCH_BATCH: usize = 32;

#[derive(Default)]
pub struct PersonClickPart {
    /// Chunks of `[comp_id: u32, ts_ms: i64, camp_idx: u32, cid_len: varint, cid_bytes]`
    pub linked_chunks: Vec<Vec<u8>>,
    pub linked_count: usize,
    /// Chunks of `[uid_len: varint, uid_bytes, ts_ms: i64, camp_idx: u32, cid_len: varint, cid_bytes]`
    pub singleton_chunks: Vec<Vec<u8>>,
    pub singleton_count: usize,
}

impl PersonClickPart {
    pub fn total_clicks(&self) -> usize {
        self.linked_count + self.singleton_count
    }
}

#[derive(Clone, Copy)]
struct PendingClick<'a> {
    click_id: &'a [u8],
    uid: &'a [u8],
    uid_h: u64,
    ts_ms: i64,
    camp_idx: u32,
}

#[inline(always)]
fn flush_click_batch(
    pending: &mut Vec<PendingClick<'_>>,
    local_linked: &mut [(Vec<u8>, usize)],
    local_sing: &mut [(Vec<u8>, usize)],
    p_parts: &[Mutex<PersonClickPart>],
    link_lookup: &LinkLookup,
    bloom: &ActiveBloom,
) {
    for c in pending.drain(..) {
        if let Some(comp_id) = link_lookup.lookup(c.uid, c.uid_h) {
            if !bloom.may_contain(mix_u32(comp_id)) {
                continue;
            }
            let pp = person_part_from_comp(comp_id);
            let (buf, cnt) = unsafe { local_linked.get_unchecked_mut(pp) };
            buf.extend_from_slice(&comp_id.to_le_bytes());
            buf.extend_from_slice(&c.ts_ms.to_le_bytes());
            buf.extend_from_slice(&c.camp_idx.to_le_bytes());
            write_varint(buf, c.click_id.len());
            buf.extend_from_slice(c.click_id);
            *cnt += 1;
            if buf.len() >= FLUSH_THRESHOLD {
                let flushed = std::mem::replace(buf, Vec::with_capacity(FLUSH_THRESHOLD + 128));
                let n = *cnt;
                *cnt = 0;
                let mut g = p_parts[pp].lock().unwrap();
                g.linked_chunks.push(flushed);
                g.linked_count += n;
            }
        } else {
            if !bloom.may_contain(c.uid_h) {
                continue;
            }
            let pp = person_part_from_hash(c.uid_h);
            let (buf, cnt) = unsafe { local_sing.get_unchecked_mut(pp) };
            write_varint(buf, c.uid.len());
            buf.extend_from_slice(c.uid);
            buf.extend_from_slice(&c.ts_ms.to_le_bytes());
            buf.extend_from_slice(&c.camp_idx.to_le_bytes());
            write_varint(buf, c.click_id.len());
            buf.extend_from_slice(c.click_id);
            *cnt += 1;
            if buf.len() >= FLUSH_THRESHOLD {
                let flushed = std::mem::replace(buf, Vec::with_capacity(FLUSH_THRESHOLD + 128));
                let n = *cnt;
                *cnt = 0;
                let mut g = p_parts[pp].lock().unwrap();
                g.singleton_chunks.push(flushed);
                g.singleton_count += n;
            }
        }
    }
}

/// Phase 3: Parse all `clicks/*.csv`, validate strictly, skip people with zero surviving
/// events via `ActiveBloom`, and route clicks into `PERSON_PARTS`.
pub fn process_clicks(
    data_dir: &Path,
    lookups: &Lookups,
    link_lookup: &LinkLookup,
    bloom: &ActiveBloom,
    ncpu: usize,
) -> io::Result<Vec<PersonClickPart>> {
    let files = list_csv_files(&data_dir.join("clicks"))?;
    let (opened, chunks, total_bytes) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;

    if chunks.is_empty() || total_bytes == 0 {
        return Ok((0..PERSON_PARTS).map(|_| PersonClickPart::default()).collect());
    }

    let p_parts: Vec<Mutex<PersonClickPart>> = (0..PERSON_PARTS)
        .map(|_| Mutex::new(PersonClickPart::default()))
        .collect();
    let next_chunk = AtomicUsize::new(0);
    let workers = ncpu.min(chunks.len()).max(1);

    let c_start = lookups.click_window_start_ms;
    let c_end = lookups.window_end_ms;
    let ym_cache = lookups.ym_cache;
    let has_links = !link_lookup.is_empty();

    thread::scope(|s| {
        for _ in 0..workers {
            let opened = &opened;
            let chunks = &chunks;
            let next_chunk = &next_chunk;
            let p_parts = &p_parts;
            s.spawn(move || {
                let mut local_linked: Vec<(Vec<u8>, usize)> =
                    (0..PERSON_PARTS).map(|_| (Vec::new(), 0)).collect();
                let mut local_sing: Vec<(Vec<u8>, usize)> =
                    (0..PERSON_PARTS).map(|_| (Vec::new(), 0)).collect();

                loop {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = chunks.get(idx) else {
                        break;
                    };
                    let mut pending: Vec<PendingClick<'_>> = Vec::with_capacity(PREFETCH_BATCH);
                    for_each_line_in_chunk(&opened[chunk.file_idx], chunk, |line| {
                        let Some([click_id, uid, camp_id, ts_b]) = split_fields::<4>(line) else {
                            return;
                        };
                        if click_id.is_empty() || uid.is_empty() {
                            return;
                        }
                        let Some(camp_idx) = lookups.lookup_campaign(camp_id) else {
                            return;
                        };
                        let Some(ts_ms) =
                            parse_timestamp_in_window(ts_b, c_start, c_end, &ym_cache)
                        else {
                            return;
                        };

                        let uid_h = hash_bytes(uid);
                        if has_links {
                            link_lookup.prefetch(uid_h);
                            pending.push(PendingClick {
                                click_id,
                                uid,
                                uid_h,
                                ts_ms,
                                camp_idx,
                            });
                            if pending.len() == PREFETCH_BATCH {
                                flush_click_batch(
                                    &mut pending,
                                    &mut local_linked,
                                    &mut local_sing,
                                    p_parts,
                                    link_lookup,
                                    bloom,
                                );
                            }
                        } else {
                            if !bloom.may_contain(uid_h) {
                                return;
                            }
                            let pp = person_part_from_hash(uid_h);
                            let (buf, cnt) = unsafe { local_sing.get_unchecked_mut(pp) };
                            write_varint(buf, uid.len());
                            buf.extend_from_slice(uid);
                            buf.extend_from_slice(&ts_ms.to_le_bytes());
                            buf.extend_from_slice(&camp_idx.to_le_bytes());
                            write_varint(buf, click_id.len());
                            buf.extend_from_slice(click_id);
                            *cnt += 1;
                            if buf.len() >= FLUSH_THRESHOLD {
                                let flushed = std::mem::replace(
                                    buf,
                                    Vec::with_capacity(FLUSH_THRESHOLD + 128),
                                );
                                let c = *cnt;
                                *cnt = 0;
                                let mut g = p_parts[pp].lock().unwrap();
                                g.singleton_chunks.push(flushed);
                                g.singleton_count += c;
                            }
                        }
                    });
                    if !pending.is_empty() {
                        flush_click_batch(
                            &mut pending,
                            &mut local_linked,
                            &mut local_sing,
                            p_parts,
                            link_lookup,
                            bloom,
                        );
                    }
                }

                for pp in 0..PERSON_PARTS {
                    if local_linked[pp].1 > 0 || local_sing[pp].1 > 0 {
                        let mut g = p_parts[pp].lock().unwrap();
                        if local_linked[pp].1 > 0 {
                            let buf = std::mem::take(&mut local_linked[pp].0);
                            g.linked_chunks.push(buf);
                            g.linked_count += local_linked[pp].1;
                        }
                        if local_sing[pp].1 > 0 {
                            let buf = std::mem::take(&mut local_sing[pp].0);
                            g.singleton_chunks.push(buf);
                            g.singleton_count += local_sing[pp].1;
                        }
                    }
                }
            });
        }
    });

    Ok(p_parts
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect())
}
