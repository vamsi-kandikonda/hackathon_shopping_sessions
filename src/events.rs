use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::clicks::{ClickCtx, ClickWorker, PersonClickPart, PersonClicks};
use crate::hash::{
    EVENT_PARTS, PERSON_PARTS, event_part, hash_bytes, hash_id, person_part_from_comp,
    person_part_from_hash, prefetch_read,
};
use crate::io_chunks::{
    DEFAULT_CHUNK_SIZE, for_each_record_read, list_csv_files, plan_chunks,
};
use crate::links::{CanonNames, LinkLookup};
use crate::lookups::Lookups;
use crate::timestamp::{
    LATE_CUTOFF_MS, parse_civil_19_sec_with_entry, parse_f1_sec_fast_entry, parse_price_cents,
    parse_time_of_day_sec, parse_timestamp_in_window,
};

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

const PREFETCH_BATCH: usize = 64;

pub trait EvKey: Copy + Ord + Send + Sync + 'static {
    const BYTES: usize;
    fn pack(ts_rel: u64, price: u32, cat: u32) -> Self;
    fn ts_rel(self) -> u64;
    fn price(self) -> u32;
    fn cat(self) -> u32;
}

impl EvKey for u64 {
    const BYTES: usize = 8;
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
}

impl EvKey for u128 {
    const BYTES: usize = 16;
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
}

#[derive(Clone, Copy)]
pub struct LinkedRec<K: EvKey> {
    pub comp: u32,
    pub key: K,
}

#[derive(Clone, Copy)]
pub struct SingRec<K: EvKey> {
    pub key: K,
    pub uh: u64,
}

pub struct PersonPart<K: EvKey> {
    pub linked: Vec<LinkedRec<K>>,
    pub singletons: Vec<SingRec<K>>,
    pub uid_names: Vec<(u64, u8, [u8; 16])>,
}

impl<K: EvKey> Default for PersonPart<K> {
    fn default() -> Self {
        Self {
            linked: Vec::new(),
            singletons: Vec::new(),
            uid_names: Vec::new(),
        }
    }
}

impl<K: EvKey> PersonPart<K> {
    pub fn new(cap_sing: usize, cap_linked: usize) -> Self {
        Self {
            linked: Vec::with_capacity(cap_linked),
            singletons: Vec::with_capacity(cap_sing),
            uid_names: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn event_count(&self) -> usize {
        self.linked.len() + self.singletons.len()
    }
}

pub struct PersonEvents<K: EvKey> {
    pub parts: Vec<PersonPart<K>>,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct EventRec<K: EvKey> {
    pub key: K,
    pub eh: u64,
    pub uh: u64,
    pub ingest_sec: u32,
    pub valid: u8,
    pub _pad: [u8; 3],
}

pub struct ParsedEvents<K: EvKey> {
    pub parts: Vec<Vec<Vec<EventRec<K>>>>,
    pub uid_names: Vec<Vec<(u64, u8, [u8; 16])>>,
}

const EVENT_BUF: usize = 32;

#[derive(Clone, Copy)]
struct EventBuf<K: EvKey> {
    items: [EventRec<K>; EVENT_BUF],
    len: usize,
}

impl<K: EvKey> Default for EventBuf<K> {
    fn default() -> Self {
        Self {
            items: [EventRec {
                key: K::pack(0, 0, 0),
                eh: 0,
                uh: 0,
                ingest_sec: 0,
                valid: 0,
                _pad: [0; 3],
            }; EVENT_BUF],
            len: 0,
        }
    }
}

pub fn load_and_parse_all<K: EvKey>(
    data_dir: &Path,
    lookups: &Lookups,
    ncpu: usize,
) -> io::Result<(LinkLookup, CanonNames, PersonClicks, ParsedEvents<K>)> {
    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let (mapped_clicks, click_chunks, total_click_bytes) = plan_chunks(&click_files, DEFAULT_CHUNK_SIZE)?;
    let est_clicks = (total_click_bytes / 45) as usize;
    let per_click_part = (est_clicks / PERSON_PARTS).max(256);
    let workers = ncpu.max(1);
    let n_start_clicks = if workers >= 8 { 4 } else if workers >= 4 { 1 } else { 0 };
    let links_ncpu = if workers >= 8 { 3 } else { 1 };
    let active_click_workers = n_start_clicks.max(1);
    let per_click_worker = (per_click_part / active_click_workers).max(64) * 13 / 10;
    let cap_linked = (per_click_worker * 40 / 100).max(512);
    let cap_sing = (per_click_worker * 140 / 100).max(1024);
    let click_parts: Vec<Mutex<PersonClickPart>> = (0..PERSON_PARTS)
        .map(|_| Mutex::new(PersonClickPart::default()))
        .collect();

    let event_files = list_csv_files(&data_dir.join("events"))?;
    let (mapped_events, event_chunks, total_event_bytes) = plan_chunks(&event_files, DEFAULT_CHUNK_SIZE)?;
    let est_events = (total_event_bytes / 75) as usize;
    let per_event_part = (est_events / EVENT_PARTS).max(1024) * 12 / 10;
    let per_part_worker = (per_event_part / workers).max(256) * 13 / 10;
    let event_parts: Vec<Mutex<Vec<Vec<EventRec<K>>>>> = (0..EVENT_PARTS)
        .map(|_| Mutex::new(Vec::new()))
        .collect();
    let uid_names_parts: Vec<Mutex<Vec<(u64, u8, [u8; 16])>>> = (0..PERSON_PARTS)
        .map(|_| Mutex::new(Vec::new()))
        .collect();

    let next_click = AtomicUsize::new(0);
    let next_event = AtomicUsize::new(0);

    let win_start = lookups.window_start_ms;
    let win_end = lookups.window_end_ms;
    let win_start_sec = win_start / 1000;
    let win_end_sec = win_end / 1000;
    let ym = lookups.ym_cache;

    let links_cell = std::sync::OnceLock::new();
    let links_err = Mutex::new(None);

    thread::scope(|s| {
        for tid in 0..workers {
            let mapped_clicks = &mapped_clicks;
            let click_chunks = &click_chunks;
            let click_parts = &click_parts;
            let next_click = &next_click;

            let mapped_events = &mapped_events;
            let event_chunks = &event_chunks;
            let event_parts = &event_parts;
            let uid_names_parts = &uid_names_parts;
            let next_event = &next_event;
            let links_cell = &links_cell;
            let links_err = &links_err;

            s.spawn(move || {
                let process_clicks_fn = |click_ctx: &ClickCtx<'_>| {
                    let _span = crate::prof::Span::new(crate::prof::CLICKS_SCAN);
                    let mut worker = ClickWorker::new(cap_linked, cap_sing);
                    let mut processed = 0usize;
                    loop {
                        let idx = next_click.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = click_chunks.get(idx) else {
                            break;
                        };
                        worker.process_chunk(&mapped_clicks[chunk.file_idx], chunk, click_ctx);
                        processed += 1;
                    }
                    if processed > 0 {
                        worker.finish(click_parts, click_ctx);
                    }
                };

                let mut read_buf: Vec<u8> = Vec::with_capacity(DEFAULT_CHUNK_SIZE + 128 * 1024);
                let mut loc_parts: Vec<Vec<EventRec<K>>> =
                    (0..EVENT_PARTS).map(|_| Vec::with_capacity(per_part_worker)).collect();
                let mut loc_names: Vec<Vec<(u64, u8, [u8; 16])>> =
                    (0..PERSON_PARTS).map(|_| Vec::with_capacity(1024)).collect();
                let mut seen_table: Vec<u64> = vec![0; 262_144];
                let seen_mask = 262_143usize;
                let mut ev_bufs = vec![EventBuf::<K>::default(); EVENT_PARTS];
                let mut processed_events = 0usize;

                let mut run_events_until = |stop_when_links_ready: bool| {
                    let _span = crate::prof::Span::new(crate::prof::EVENTS_SCAN);
                    loop {
                        if stop_when_links_ready && links_cell.get().is_some() {
                            break;
                        }
                        let idx = next_event.fetch_add(1, Ordering::Relaxed);
                        let Some(chunk) = event_chunks.get(idx) else {
                            break;
                        };
                        let mf = &mapped_events[chunk.file_idx];

                        for_each_record_read::<7>(mf, chunk, &mut read_buf, |[eid, uid, ts_b, ingest_b, etype, pid, price_b]| {
                            if eid.is_empty() {
                                return;
                            }
                            let Some((ingest_sec_i64, ym_entry, ingest_day_start)) = parse_f1_sec_fast_entry(ingest_b, &ym) else {
                                return;
                            };
                            let ingest_sec = ingest_sec_i64 as u32;

                            let mut valid = false;
                            let mut key = K::pack(0, 0, 0);
                            if let Some(is_purchase) = parse_etype(etype) {
                                if !uid.is_empty() && !pid.is_empty() {
                                    let mut ts_opt = None;
                                    if ts_b.len() == 20 && unsafe { *ts_b.get_unchecked(19) } == b'Z' {
                                        let same_day = unsafe { *(ts_b.as_ptr() as *const u64) == *(ingest_b.as_ptr() as *const u64) }
                                            && unsafe { *(ts_b.as_ptr().add(8) as *const u16) == *(ingest_b.as_ptr().add(8) as *const u16) };

                                        if same_day {
                                            if let Some(tod) = parse_time_of_day_sec(ts_b) {
                                                let s = ingest_day_start + tod as i64;
                                                if s >= win_start_sec && s < win_end_sec && ingest_sec_i64.saturating_sub(s) <= 86_400 {
                                                    ts_opt = Some(s * 1000);
                                                }
                                            }
                                        }
                                        if ts_opt.is_none()
                                            && ym_entry.ym_u64 != 0
                                            && unsafe { *(ts_b.as_ptr() as *const u64) } == ym_entry.ym_u64
                                        {
                                            if let Some(s) = parse_civil_19_sec_with_entry(ts_b, &ym_entry) {
                                                if s >= win_start_sec && s < win_end_sec && ingest_sec_i64.saturating_sub(s) <= 86_400 {
                                                    ts_opt = Some(s * 1000);
                                                }
                                            }
                                        }
                                    } else if ts_b.len() == 13 {
                                        let mut ts = 0u64;
                                        let mut ok = true;
                                        for &byte in ts_b {
                                            let d = byte.wrapping_sub(b'0');
                                            if d > 9 { ok = false; break; }
                                            ts = ts * 10 + d as u64;
                                        }
                                        if ok {
                                            let ts_i64 = ts as i64;
                                            if ts_i64 >= win_start && ts_i64 < win_end {
                                                let ingest_ms = ingest_sec_i64.saturating_mul(1000);
                                                if ingest_ms.saturating_sub(ts_i64) <= LATE_CUTOFF_MS {
                                                    ts_opt = Some(ts_i64);
                                                }
                                            }
                                        }
                                    }
                                    if ts_opt.is_none() {
                                        if let Some(ts) = parse_timestamp_in_window(ts_b, win_start, win_end, &ym) {
                                            if ts >= win_start && ts < win_end {
                                                let ingest_ms = ingest_sec_i64.saturating_mul(1000);
                                                if ingest_ms.saturating_sub(ts) <= LATE_CUTOFF_MS {
                                                    ts_opt = Some(ts);
                                                }
                                            }
                                        }
                                    }

                                    if let Some(ts) = ts_opt {
                                        if is_purchase {
                                            if let Some(p) = parse_price_cents(price_b) {
                                                valid = true;
                                                let cat = lookups.lookup_product_cat(pid);
                                                key = K::pack((ts - win_start) as u64, p, cat);
                                            }
                                        } else {
                                            valid = true;
                                            key = K::pack((ts - win_start) as u64, 0, 0);
                                        }
                                    }
                                }
                            }

                            let eh = hash_id(eid);
                            let ep = event_part(eh);
                            let uh = if valid {
                                let h = hash_id(uid);
                                let mut idx = (h as usize) & seen_mask;
                                let is_new = loop {
                                    let slot = unsafe { *seen_table.get_unchecked(idx) };
                                    if slot == h {
                                        break false;
                                    }
                                    if slot == 0 {
                                        unsafe { *seen_table.get_unchecked_mut(idx) = h };
                                        break true;
                                    }
                                    idx = (idx + 1) & seen_mask;
                                };
                                if is_new {
                                    let mut uid_bytes = [0u8; 16];
                                    let ulen = uid.len().min(16);
                                    uid_bytes[..ulen].copy_from_slice(&uid[..ulen]);
                                    let pp = person_part_from_hash(h);
                                    loc_names[pp].push((h, ulen as u8, uid_bytes));
                                }
                                h
                            } else {
                                0
                            };

                            let buf = unsafe { ev_bufs.get_unchecked_mut(ep) };
                            unsafe {
                                *buf.items.get_unchecked_mut(buf.len) = EventRec {
                                    key,
                                    eh,
                                    uh,
                                    ingest_sec,
                                    valid: valid as u8,
                                    _pad: [0; 3],
                                };
                            }
                            buf.len += 1;
                            if buf.len == EVENT_BUF {
                                let vec = unsafe { loc_parts.get_unchecked_mut(ep) };
                                let old_len = vec.len();
                                if old_len + EVENT_BUF <= vec.capacity() {
                                    unsafe {
                                        std::ptr::copy_nonoverlapping(
                                            buf.items.as_ptr(),
                                            vec.as_mut_ptr().add(old_len),
                                            EVENT_BUF,
                                        );
                                        vec.set_len(old_len + EVENT_BUF);
                                    }
                                } else {
                                    vec.extend_from_slice(&buf.items);
                                }
                                buf.len = 0;
                            }
                        });
                        processed_events += 1;
                    }
                };

                if tid == 0 {
                    match crate::links::load_links(data_dir, links_ncpu) {
                        Ok(res) => {
                            links_cell.set(res).ok();
                        }
                        Err(e) => {
                            *links_err.lock().unwrap() = Some(e);
                        }
                    }
                    if let Some((link_lookup, _)) = links_cell.get() {
                        let click_ctx = ClickCtx {
                            lookups,
                            link_lookup,
                            has_links: !link_lookup.is_empty(),
                            c_start: lookups.click_window_start_ms,
                            c_end: lookups.window_end_ms,
                            ym: lookups.ym_cache,
                        };
                        process_clicks_fn(&click_ctx);
                    }
                    run_events_until(false);
                } else if tid < n_start_clicks {
                    run_events_until(true);
                    if let Some((link_lookup, _)) = links_cell.get() {
                        let click_ctx = ClickCtx {
                            lookups,
                            link_lookup,
                            has_links: !link_lookup.is_empty(),
                            c_start: lookups.click_window_start_ms,
                            c_end: lookups.window_end_ms,
                            ym: lookups.ym_cache,
                        };
                        process_clicks_fn(&click_ctx);
                    }
                    run_events_until(false);
                } else {
                    run_events_until(false);
                    if let Some((link_lookup, _)) = links_cell.get() {
                        if next_click.load(Ordering::Relaxed) < click_chunks.len() {
                            let click_ctx = ClickCtx {
                                lookups,
                                link_lookup,
                                has_links: !link_lookup.is_empty(),
                                c_start: lookups.click_window_start_ms,
                                c_end: lookups.window_end_ms,
                                ym: lookups.ym_cache,
                            };
                            process_clicks_fn(&click_ctx);
                        }
                    }
                }

                if processed_events > 0 {
                    for ep in 0..EVENT_PARTS {
                        let buf = unsafe { ev_bufs.get_unchecked_mut(ep) };
                        if buf.len > 0 {
                            loc_parts[ep].extend_from_slice(&buf.items[..buf.len]);
                            buf.len = 0;
                        }
                    }

                    for ep in 0..EVENT_PARTS {
                        if !loc_parts[ep].is_empty() {
                            event_parts[ep].lock().unwrap().push(std::mem::take(&mut loc_parts[ep]));
                        }
                    }
                    for pp in 0..PERSON_PARTS {
                        if !loc_names[pp].is_empty() {
                            uid_names_parts[pp].lock().unwrap().extend(loc_names[pp].drain(..));
                        }
                    }
                }
            });
        }
    });

    if let Some(e) = links_err.into_inner().unwrap() {
        return Err(e);
    }
    let (link_lookup, canon) = links_cell.into_inner().unwrap();

    Ok((
        link_lookup,
        canon,
        PersonClicks {
            parts: click_parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        },
        ParsedEvents {
            parts: event_parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
            uid_names: uid_names_parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        },
    ))
}

pub fn parse_events<K: EvKey>(
    data_dir: &Path,
    lookups: &Lookups,
    ncpu: usize,
) -> io::Result<ParsedEvents<K>> {
    let ((), parsed) = parse_events_pipelined::<K, _>(data_dir, lookups, ncpu, |_| ())?;
    Ok(parsed)
}

pub fn parse_events_pipelined<K: EvKey, R: Send>(
    data_dir: &Path,
    lookups: &Lookups,
    ncpu: usize,
    bg_task: impl FnOnce(usize) -> R + Send,
) -> io::Result<(R, ParsedEvents<K>)> {
    let files = list_csv_files(&data_dir.join("events"))?;
    let (mapped, chunks, total_bytes) = plan_chunks(&files, DEFAULT_CHUNK_SIZE)?;
    let workers = ncpu.min(chunks.len()).max(1);
    let est_events = (total_bytes / 75) as usize;
    let per_part = (est_events / EVENT_PARTS).max(1024) * 12 / 10;
    let n_bg = if workers >= 8 { 2 } else { 1 };
    let n_fg = workers - n_bg;
    let per_part_worker = (per_part / n_fg.max(1)).max(256) * 13 / 10;

    let parts: Vec<Mutex<Vec<Vec<EventRec<K>>>>> =
        (0..EVENT_PARTS).map(|_| Mutex::new(Vec::new())).collect();
    let uid_names_parts: Vec<Mutex<Vec<(u64, u8, [u8; 16])>>> =
        (0..PERSON_PARTS).map(|_| Mutex::new(Vec::new())).collect();

    let bg_res = if !chunks.is_empty() {
        let next_chunk = AtomicUsize::new(0);
        let win_start = lookups.window_start_ms;
        let win_end = lookups.window_end_ms;
        let win_start_sec = win_start / 1000;
        let win_end_sec = win_end / 1000;
        let ym = lookups.ym_cache;

        let run_worker = |parts: &[Mutex<Vec<Vec<EventRec<K>>>>], names: &[Mutex<Vec<(u64, u8, [u8; 16])>>]| {
            let mut read_buf: Vec<u8> = Vec::with_capacity(DEFAULT_CHUNK_SIZE + 128 * 1024);
            let mut loc_parts: Vec<Vec<EventRec<K>>> =
                (0..EVENT_PARTS).map(|_| Vec::with_capacity(per_part_worker)).collect();
            let mut loc_names: Vec<Vec<(u64, u8, [u8; 16])>> =
                (0..PERSON_PARTS).map(|_| Vec::with_capacity(1024)).collect();
            let mut seen_table: Vec<u64> = vec![0; 262_144];
            let seen_mask = 262_143usize;
            let mut ev_bufs = vec![EventBuf::<K>::default(); EVENT_PARTS];
            loop {
                let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                let Some(chunk) = chunks.get(idx) else {
                    break;
                };
                let mf = &mapped[chunk.file_idx];

                for_each_record_read::<7>(mf, chunk, &mut read_buf, |[eid, uid, ts_b, ingest_b, etype, pid, price_b]| {
                    if eid.is_empty() {
                        return;
                    }
                    let Some((ingest_sec_i64, ym_entry, ingest_day_start)) = parse_f1_sec_fast_entry(ingest_b, &ym) else {
                        return;
                    };
                    let ingest_sec = ingest_sec_i64 as u32;

                    let mut valid = false;
                    let mut key = K::pack(0, 0, 0);
                    if let Some(is_purchase) = parse_etype(etype) {
                        if !uid.is_empty() && !pid.is_empty() {
                            let mut ts_opt = None;
                            if ts_b.len() == 13 {
                                let mut ts = 0u64;
                                let mut ok = true;
                                for &byte in ts_b {
                                    let d = byte.wrapping_sub(b'0');
                                    if d > 9 { ok = false; break; }
                                    ts = ts * 10 + d as u64;
                                }
                                if ok {
                                    let ts_i64 = ts as i64;
                                    if ts_i64 >= win_start && ts_i64 < win_end {
                                        let ingest_ms = ingest_sec_i64.saturating_mul(1000);
                                        if ingest_ms.saturating_sub(ts_i64) <= LATE_CUTOFF_MS {
                                            ts_opt = Some(ts_i64);
                                        }
                                    }
                                }
                            } else if ts_b.len() == 20 && unsafe { *ts_b.get_unchecked(19) } == b'Z' {
                                let same_day = unsafe { *(ts_b.as_ptr() as *const u64) == *(ingest_b.as_ptr() as *const u64) }
                                    && unsafe { *(ts_b.as_ptr().add(8) as *const u16) == *(ingest_b.as_ptr().add(8) as *const u16) };

                                if same_day {
                                    if let Some(tod) = parse_time_of_day_sec(ts_b) {
                                        let s = ingest_day_start + tod as i64;
                                        if s >= win_start_sec && s < win_end_sec && ingest_sec_i64.saturating_sub(s) <= 86_400 {
                                            ts_opt = Some(s * 1000);
                                        }
                                    }
                                }
                                if ts_opt.is_none()
                                    && ym_entry.ym_u64 != 0
                                    && unsafe { *(ts_b.as_ptr() as *const u64) } == ym_entry.ym_u64
                                {
                                    if let Some(s) = parse_civil_19_sec_with_entry(ts_b, &ym_entry) {
                                        if s >= win_start_sec && s < win_end_sec && ingest_sec_i64.saturating_sub(s) <= 86_400 {
                                            ts_opt = Some(s * 1000);
                                        }
                                    }
                                }
                            }
                            if ts_opt.is_none() {
                                if let Some(ts) = parse_timestamp_in_window(ts_b, win_start, win_end, &ym) {
                                    if ts >= win_start && ts < win_end {
                                        let ingest_ms = ingest_sec_i64.saturating_mul(1000);
                                        if ingest_ms.saturating_sub(ts) <= LATE_CUTOFF_MS {
                                            ts_opt = Some(ts);
                                        }
                                    }
                                }
                            }

                            if let Some(ts) = ts_opt {
                                if is_purchase {
                                    if let Some(p) = parse_price_cents(price_b) {
                                        valid = true;
                                        let cat = lookups.lookup_product_cat(pid);
                                        key = K::pack((ts - win_start) as u64, p, cat);
                                    }
                                } else {
                                    valid = true;
                                    key = K::pack((ts - win_start) as u64, 0, 0);
                                }
                            }
                        }
                    }

                    let eh = hash_bytes(eid);
                    let ep = event_part(eh);
                    let uh = if valid {
                        let h = hash_bytes(uid);
                        let mut idx = (h as usize) & seen_mask;
                        let is_new = loop {
                            let slot = unsafe { *seen_table.get_unchecked(idx) };
                            if slot == h {
                                break false;
                            }
                            if slot == 0 {
                                unsafe { *seen_table.get_unchecked_mut(idx) = h };
                                break true;
                            }
                            idx = (idx + 1) & seen_mask;
                        };
                        if is_new {
                            let mut uid_bytes = [0u8; 16];
                            let ulen = uid.len().min(16);
                            uid_bytes[..ulen].copy_from_slice(&uid[..ulen]);
                            let pp = person_part_from_hash(h);
                            loc_names[pp].push((h, ulen as u8, uid_bytes));
                        }
                        h
                    } else {
                        0
                    };

                    let buf = unsafe { ev_bufs.get_unchecked_mut(ep) };
                    unsafe {
                        *buf.items.get_unchecked_mut(buf.len) = EventRec {
                            key,
                            eh,
                            uh,
                            ingest_sec,
                            valid: valid as u8,
                            _pad: [0; 3],
                        };
                    }
                    buf.len += 1;
                    if buf.len == EVENT_BUF {
                        let vec = unsafe { loc_parts.get_unchecked_mut(ep) };
                        let old_len = vec.len();
                        if old_len + EVENT_BUF <= vec.capacity() {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    buf.items.as_ptr(),
                                    vec.as_mut_ptr().add(old_len),
                                    EVENT_BUF,
                                );
                                vec.set_len(old_len + EVENT_BUF);
                            }
                        } else {
                            vec.extend_from_slice(&buf.items);
                        }
                        buf.len = 0;
                    }
                });
            }

            for ep in 0..EVENT_PARTS {
                let buf = unsafe { ev_bufs.get_unchecked_mut(ep) };
                if buf.len > 0 {
                    loc_parts[ep].extend_from_slice(&buf.items[..buf.len]);
                    buf.len = 0;
                }
            }

            for ep in 0..EVENT_PARTS {
                if !loc_parts[ep].is_empty() {
                    parts[ep].lock().unwrap().push(std::mem::take(&mut loc_parts[ep]));
                }
            }
            for pp in 0..PERSON_PARTS {
                if !loc_names[pp].is_empty() {
                    names[pp].lock().unwrap().extend(loc_names[pp].drain(..));
                }
            }
        };

        let parts_ref = &parts;
        let names_ref = &uid_names_parts;
        thread::scope(|s| {
            let bg_handle = s.spawn(move || {
                let r = bg_task(n_bg);
                run_worker(parts_ref, names_ref);
                r
            });

            for _ in 0..n_fg {
                s.spawn(move || {
                    run_worker(parts_ref, names_ref);
                });
            }
            bg_handle.join().unwrap()
        })
    } else {
        bg_task(ncpu)
    };

    Ok((
        bg_res,
        ParsedEvents {
            parts: parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
            uid_names: uid_names_parts.into_iter().map(|m| m.into_inner().unwrap()).collect(),
        },
    ))
}

fn dedup_in_place<K: EvKey>(
    chunks: &mut [Vec<EventRec<K>>],
    slots: &mut Vec<(u32, u32)>,
) {
    let total_records: usize = chunks.iter().map(|c| c.len()).sum();
    let cap = ((total_records * 10 / 7).max(64)).next_power_of_two().max(1024);
    if slots.len() < cap {
        slots.resize(cap, (0, 0));
    }
    slots[..cap].fill((0, 0));
    let mask = cap - 1;

    const PREFETCH_DIST: usize = 16;
    #[derive(Clone, Copy)]
    struct QueueItem {
        c_idx: u32,
        r_idx: u32,
        eh: u64,
        ingest_sec: u32,
        idx: usize,
    }

    let default_item = QueueItem {
        c_idx: 0,
        r_idx: 0,
        eh: 0,
        ingest_sec: 0,
        idx: 0,
    };
    let mut queue = [default_item; PREFETCH_DIST];
    let mut q_head = 0usize;
    let mut q_count = 0usize;

    let chunks_ptrs: Vec<*mut EventRec<K>> = chunks.iter_mut().map(|c| c.as_mut_ptr()).collect();

    #[inline(always)]
    fn insert_slot<K: EvKey>(
        item: QueueItem,
        chunks_ptrs: &[*mut EventRec<K>],
        slots: &mut [(u32, u32)],
        mask: usize,
    ) {
        let tag = ((item.eh >> 32) as u32) | 1;
        let mut idx = item.idx;
        let c_tag = item.c_idx << 20;
        loop {
            let (stag, sidx) = unsafe { *slots.get_unchecked(idx) };
            if stag == 0 {
                unsafe { *slots.get_unchecked_mut(idx) = (tag, c_tag | item.r_idx) };
                break;
            }
            if stag == tag {
                let old_c = (sidx >> 20) as usize;
                let old_r = (sidx & 0xF_FFFF) as usize;
                let old_rec = unsafe { &mut *chunks_ptrs[old_c].add(old_r) };
                if old_rec.eh == item.eh {
                    let cur_rec = unsafe { &mut *chunks_ptrs[item.c_idx as usize].add(item.r_idx as usize) };
                    if item.ingest_sec < old_rec.ingest_sec {
                        old_rec.valid = 0;
                        unsafe { *slots.get_unchecked_mut(idx) = (tag, c_tag | item.r_idx) };
                    } else {
                        cur_rec.valid = 0;
                    }
                    break;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    for c_idx in 0..chunks.len() {
        let chunk = &chunks[c_idx];
        for r_idx in 0..chunk.len() {
            let rec = unsafe { chunk.get_unchecked(r_idx) };
            let idx = (rec.eh as usize) & mask;
            prefetch_read(unsafe { slots.as_ptr().add(idx) });

            let new_item = QueueItem {
                c_idx: c_idx as u32,
                r_idx: r_idx as u32,
                eh: rec.eh,
                ingest_sec: rec.ingest_sec,
                idx,
            };
            if q_count == PREFETCH_DIST {
                let popped = queue[q_head];
                queue[q_head] = new_item;
                q_head = (q_head + 1) & (PREFETCH_DIST - 1);
                insert_slot::<K>(popped, &chunks_ptrs, slots, mask);
            } else {
                let tail = (q_head + q_count) & (PREFETCH_DIST - 1);
                queue[tail] = new_item;
                q_count += 1;
            }
        }
    }

    while q_count > 0 {
        let popped = queue[q_head];
        q_head = (q_head + 1) & (PREFETCH_DIST - 1);
        q_count -= 1;
        insert_slot::<K>(popped, &chunks_ptrs, slots, mask);
    }
}

pub fn route_events<K: EvKey>(
    parsed: ParsedEvents<K>,
    link_lookup: &LinkLookup,
    ncpu: usize,
) -> PersonEvents<K> {
    let ParsedEvents { parts, uid_names } = parsed;
    let workers = ncpu.max(1);
    let total_events: usize = parts.iter().map(|p| p.iter().map(|c| c.len()).sum::<usize>()).sum();
    let max_part_size = parts.iter().map(|p| p.iter().map(|c| c.len()).sum::<usize>()).max().unwrap_or(1024);
    let slots_cap = ((max_part_size * 10 / 7).max(64)).next_power_of_two().max(1024);

    let cap_sing_part = (total_events / PERSON_PARTS).max(1024) * 14 / 10;
    let cap_linked_part = (cap_sing_part / 4).max(512);

    let parts: Vec<Mutex<Vec<Vec<EventRec<K>>>>> = parts.into_iter().map(Mutex::new).collect();
    let out: Vec<Mutex<PersonPart<K>>> = uid_names
        .into_iter()
        .map(|names| {
            let mut part = PersonPart::new(cap_sing_part, cap_linked_part);
            part.uid_names = names;
            Mutex::new(part)
        })
        .collect();
    let next = AtomicUsize::new(0);
    let has_links = !link_lookup.is_empty();

    thread::scope(|s| {
        for _ in 0..workers {
            let (parts, out, next) = (&parts, &out, &next);
            s.spawn(move || {
                let mut slots: Vec<(u32, u32)> = vec![(0, 0); slots_cap];
                let cap_sing = (max_part_size / PERSON_PARTS).max(512) * 14 / 10;
                let cap_linked = (cap_sing / 4).max(256);
                let mut local_sing: Vec<Vec<SingRec<K>>> =
                    (0..PERSON_PARTS).map(|_| Vec::with_capacity(cap_sing)).collect();
                let mut local_linked: Vec<Vec<LinkedRec<K>>> =
                    (0..PERSON_PARTS).map(|_| Vec::with_capacity(cap_linked)).collect();

                let mut s_bufs = vec![[SingRec::<K> { key: K::pack(0, 0, 0), uh: 0 }; 16]; PERSON_PARTS];
                let mut s_lens = [0u8; PERSON_PARTS];
                let mut cache_uh = [u64::MAX; 128];
                let mut cache_comp = [None; 128];

                loop {
                    let ep = next.fetch_add(1, Ordering::Relaxed);
                    if ep >= EVENT_PARTS {
                        break;
                    }
                    let mut chunks = std::mem::take(&mut *parts[ep].lock().unwrap());
                    if chunks.is_empty() {
                        continue;
                    }
                    crate::prof::time(crate::prof::ROUTE_DEDUP, || {
                        dedup_in_place::<K>(&mut chunks, &mut slots);
                    });

                    let _scatter = crate::prof::Span::new(crate::prof::ROUTE_SCATTER);
                    for chunk in &chunks {
                        for e in chunk {
                            if e.valid != 0 {
                                let uh = e.uh;
                                let c_idx = (uh as usize) & 127;
                                let comp = if unsafe { *cache_uh.get_unchecked(c_idx) } == uh {
                                    unsafe { *cache_comp.get_unchecked(c_idx) }
                                } else {
                                    let c = if has_links { link_lookup.lookup_hash(uh) } else { None };
                                    unsafe {
                                        *cache_uh.get_unchecked_mut(c_idx) = uh;
                                        *cache_comp.get_unchecked_mut(c_idx) = c;
                                    }
                                    c
                                };

                                if let Some(comp) = comp {
                                    let pp = person_part_from_comp(comp);
                                    local_linked[pp].push(LinkedRec { comp, key: e.key });
                                    continue;
                                }
                                let pp = person_part_from_hash(uh);
                                let blen = unsafe { *s_lens.get_unchecked(pp) } as usize;
                                unsafe {
                                    *s_bufs.get_unchecked_mut(pp).get_unchecked_mut(blen) = SingRec {
                                        key: e.key,
                                        uh,
                                    };
                                }
                                if blen + 1 == 16 {
                                    local_sing[pp].extend_from_slice(unsafe { s_bufs.get_unchecked(pp) });
                                    unsafe { *s_lens.get_unchecked_mut(pp) = 0 };
                                } else {
                                    unsafe { *s_lens.get_unchecked_mut(pp) = (blen + 1) as u8 };
                                }
                            }
                        }
                    }

                    for pp in 0..PERSON_PARTS {
                        let blen = unsafe { *s_lens.get_unchecked(pp) } as usize;
                        if blen > 0 {
                            local_sing[pp].extend_from_slice(unsafe { &s_bufs.get_unchecked(pp)[..blen] });
                            unsafe { *s_lens.get_unchecked_mut(pp) = 0 };
                        }
                        let mut g = out[pp].lock().unwrap();
                        if !local_sing[pp].is_empty() {
                            g.singletons.extend_from_slice(&local_sing[pp]);
                            local_sing[pp].clear();
                        }
                        if !local_linked[pp].is_empty() {
                            g.linked.extend_from_slice(&local_linked[pp]);
                            local_linked[pp].clear();
                        }
                    }
                }
            });
        }
    });

    PersonEvents {
        parts: out.into_iter().map(|m| m.into_inner().unwrap()).collect(),
    }
}
