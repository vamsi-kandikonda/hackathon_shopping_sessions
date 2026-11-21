use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use hashbrown::HashMap;

use crate::clicks::PersonClickPart;
use crate::events::{CompactPersonPart, PersonEventStore, WidePersonPart};
use crate::hash::{PERSON_PARTS, fast_build_hasher, read_varint};
use crate::links::CanonNames;
use crate::lookups::Lookups;
use crate::output::{CampaignDailyRow, UserDailyWriter, write_campaign_daily, write_empty_user_daily};
use crate::timestamp::{CLICK_REACH_MS, MS_PER_DAY, SESSION_GAP_MS};

#[derive(Clone, Copy, Default)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

enum CampaignAcc {
    Dense {
        window_start_day: i32,
        num_days: usize,
        num_campaigns: usize,
        cells: Vec<CampDayAgg>,
    },
    Sparse(HashMap<(u32, i32), CampDayAgg, ahash::RandomState>),
}

impl CampaignAcc {
    fn new(lookups: &Lookups) -> Self {
        let num_campaigns = lookups.campaigns.len();
        let num_days = lookups.num_window_days;
        if num_days > 0 && num_days <= 400 && num_campaigns.saturating_mul(num_days) <= 2_000_000 {
            CampaignAcc::Dense {
                window_start_day: lookups.window_start_day,
                num_days,
                num_campaigns,
                cells: vec![CampDayAgg::default(); num_campaigns * num_days],
            }
        } else {
            CampaignAcc::Sparse(HashMap::with_hasher(fast_build_hasher()))
        }
    }

    #[inline(always)]
    fn add_credit(&mut self, camp_idx: usize, day: i32, has_purchase: bool, revenue: i64) {
        match self {
            CampaignAcc::Dense {
                window_start_day,
                num_days,
                cells,
                ..
            } => {
                let d = (day - *window_start_day) as usize;
                let cell = unsafe { cells.get_unchecked_mut(camp_idx * *num_days + d) };
                cell.sessions += 1;
                cell.purchasing_sessions += has_purchase as i64;
                cell.revenue_cents += revenue;
            }
            CampaignAcc::Sparse(map) => {
                let cell = map.entry((camp_idx as u32, day)).or_default();
                cell.sessions += 1;
                cell.purchasing_sessions += has_purchase as i64;
                cell.revenue_cents += revenue;
            }
        }
    }

    #[inline(always)]
    fn add_assist(&mut self, camp_idx: usize, day: i32) {
        match self {
            CampaignAcc::Dense {
                window_start_day,
                num_days,
                cells,
                ..
            } => {
                let d = (day - *window_start_day) as usize;
                let cell = unsafe { cells.get_unchecked_mut(camp_idx * *num_days + d) };
                cell.assisted_sessions += 1;
            }
            CampaignAcc::Sparse(map) => {
                let cell = map.entry((camp_idx as u32, day)).or_default();
                cell.assisted_sessions += 1;
            }
        }
    }

    fn merge_from(&mut self, other: CampaignAcc) {
        match (self, other) {
            (
                CampaignAcc::Dense { cells: a, .. },
                CampaignAcc::Dense { cells: b, .. },
            ) => {
                for (ca, cb) in a.iter_mut().zip(b.iter()) {
                    ca.sessions += cb.sessions;
                    ca.purchasing_sessions += cb.purchasing_sessions;
                    ca.revenue_cents += cb.revenue_cents;
                    ca.assisted_sessions += cb.assisted_sessions;
                }
            }
            (CampaignAcc::Sparse(a), CampaignAcc::Sparse(b)) => {
                for (k, cb) in b {
                    let ca = a.entry(k).or_default();
                    ca.sessions += cb.sessions;
                    ca.purchasing_sessions += cb.purchasing_sessions;
                    ca.revenue_cents += cb.revenue_cents;
                    ca.assisted_sessions += cb.assisted_sessions;
                }
            }
            _ => unreachable!(),
        }
    }

    fn into_rows(self) -> Vec<CampaignDailyRow> {
        let mut rows = Vec::new();
        match self {
            CampaignAcc::Dense {
                window_start_day,
                num_days,
                num_campaigns,
                cells,
            } => {
                for c in 0..num_campaigns {
                    let base = c * num_days;
                    for d in 0..num_days {
                        let agg = cells[base + d];
                        if agg.sessions > 0 || agg.assisted_sessions > 0 {
                            rows.push(CampaignDailyRow {
                                camp_idx: c,
                                day: window_start_day + d as i32,
                                sessions: agg.sessions,
                                purchasing_sessions: agg.purchasing_sessions,
                                revenue_cents: agg.revenue_cents,
                                assisted_sessions: agg.assisted_sessions,
                            });
                        }
                    }
                }
            }
            CampaignAcc::Sparse(map) => {
                for ((c, day), agg) in map {
                    if agg.sessions > 0 || agg.assisted_sessions > 0 {
                        rows.push(CampaignDailyRow {
                            camp_idx: c as usize,
                            day,
                            sessions: agg.sessions,
                            purchasing_sessions: agg.purchasing_sessions,
                            revenue_cents: agg.revenue_cents,
                            assisted_sessions: agg.assisted_sessions,
                        });
                    }
                }
            }
        }
        rows
    }
}

#[derive(Clone, Copy)]
struct ClickRef {
    ts_ms: i64,
    camp_idx: u32,
    chunk_idx: u32,
    cid_off: u32,
    cid_len: u32,
}

#[derive(Clone, Copy)]
struct WideEventRef {
    ts_ms: i64,
    price_cents: u32,
    cat_id: u32,
}

/// Reusable per-thread scratch state for zero-allocation per-person sessionization & attribution.
struct WorkerScratch {
    // Small-click assist dedup (generation-stamped)
    assist_seen: Vec<u32>,
    assist_gen: u32,
    // Heavy-click O(1) sliding window assist tracker
    camp_win_cnt: Vec<u32>,
    camp_win_pos: Vec<u32>,
    active_camps: Vec<u32>,
    // Per-day category revenue accumulator
    cat_rev: Vec<u64>,
    touched_cats: Vec<u32>,
}

impl WorkerScratch {
    fn new(num_campaigns: usize, num_categories: usize) -> Self {
        Self {
            assist_seen: vec![0u32; num_campaigns],
            assist_gen: 0,
            camp_win_cnt: vec![0u32; num_campaigns],
            camp_win_pos: vec![0u32; num_campaigns],
            active_camps: Vec::with_capacity(64),
            cat_rev: vec![0u64; num_categories],
            touched_cats: Vec::with_capacity(16),
        }
    }

    #[inline(always)]
    fn next_assist_gen(&mut self) -> u32 {
        self.assist_gen = self.assist_gen.wrapping_add(1);
        if self.assist_gen == 0 {
            self.assist_seen.fill(0);
            self.assist_gen = 1;
        }
        self.assist_gen
    }
}

/// Sort clicks by `(ts_ms ASC, click_id DESC)` and deduplicate identical `click_id`s.
#[inline]
fn sort_and_dedup_clicks(clks: &mut [ClickRef], click_chunks: &[Vec<u8>]) -> usize {
    let n = clks.len();
    if n <= 1 {
        return n;
    }
    clks.sort_unstable_by_key(|c| c.ts_ms);

    let mut write = 0usize;
    let mut i = 0usize;
    while i < n {
        let ts = clks[i].ts_ms;
        let mut j = i + 1;
        while j < n && clks[j].ts_ms == ts {
            j += 1;
        }
        if j - i == 1 {
            clks[write] = clks[i];
            write += 1;
        } else {
            // Sort equal-ts run by click_id DESCENDING so smallest click_id is LAST (credit winner!)
            clks[i..j].sort_unstable_by(|a, b| {
                let ca = &click_chunks[a.chunk_idx as usize]
                    [a.cid_off as usize..(a.cid_off + a.cid_len) as usize];
                let cb = &click_chunks[b.chunk_idx as usize]
                    [b.cid_off as usize..(b.cid_off + b.cid_len) as usize];
                cb.cmp(ca)
            });
            // Dedup consecutive equal click_ids
            clks[write] = clks[i];
            let mut prev = &click_chunks[clks[i].chunk_idx as usize]
                [clks[i].cid_off as usize..(clks[i].cid_off + clks[i].cid_len) as usize];
            write += 1;
            for k in (i + 1)..j {
                let cur = &click_chunks[clks[k].chunk_idx as usize]
                    [clks[k].cid_off as usize..(clks[k].cid_off + clks[k].cid_len) as usize];
                if cur != prev {
                    clks[write] = clks[k];
                    write += 1;
                    prev = cur;
                }
            }
        }
        i = j;
    }
    write
}

#[inline(always)]
fn flush_user_day(
    writer: &mut UserDailyWriter,
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    person_name: &[u8],
    day: i32,
    day_sessions: i64,
    day_events: i64,
    day_revenue: i64,
) -> io::Result<()> {
    let top_cat = if scratch.touched_cats.is_empty() {
        None
    } else {
        let mut best_cat = scratch.touched_cats[0];
        let mut best_rev = scratch.cat_rev[best_cat as usize];
        scratch.cat_rev[best_cat as usize] = 0;
        for &c in &scratch.touched_cats[1..] {
            let r = scratch.cat_rev[c as usize];
            scratch.cat_rev[c as usize] = 0;
            if r > best_rev || (r == best_rev && c < best_cat) {
                best_rev = r;
                best_cat = c;
            }
        }
        scratch.touched_cats.clear();
        Some(lookups.categories[best_cat as usize].as_str())
    };
    writer.push(
        person_name,
        day,
        day_sessions,
        day_events,
        day_revenue,
        top_cat,
    )
}

#[inline]
fn credit_and_assist_small(
    clks: &[ClickRef],
    left: &mut usize,
    right: &mut usize,
    session_start: i64,
    session_day: i32,
    has_purchase: bool,
    sess_rev: i64,
    organic_idx: usize,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
) {
    let n = clks.len();
    while *right < n && clks[*right].ts_ms <= session_start {
        *right += 1;
    }
    let cutoff = session_start - CLICK_REACH_MS;
    while *left < *right && clks[*left].ts_ms < cutoff {
        *left += 1;
    }
    if *left == *right {
        camp_acc.add_credit(organic_idx, session_day, has_purchase, sess_rev);
    } else {
        let win_camp = clks[*right - 1].camp_idx as usize;
        camp_acc.add_credit(win_camp, session_day, has_purchase, sess_rev);
        let gen = scratch.next_assist_gen();
        for c in &clks[*left..*right] {
            let ci = c.camp_idx as usize;
            if scratch.assist_seen[ci] != gen {
                scratch.assist_seen[ci] = gen;
                camp_acc.add_assist(ci, session_day);
            }
        }
    }
}

#[inline]
fn credit_and_assist_heavy(
    clks: &[ClickRef],
    left: &mut usize,
    right: &mut usize,
    session_start: i64,
    session_day: i32,
    has_purchase: bool,
    sess_rev: i64,
    organic_idx: usize,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
) {
    let n = clks.len();
    while *right < n && clks[*right].ts_ms <= session_start {
        let ci = clks[*right].camp_idx as usize;
        if scratch.camp_win_cnt[ci] == 0 {
            scratch.camp_win_pos[ci] = scratch.active_camps.len() as u32;
            scratch.active_camps.push(ci as u32);
        }
        scratch.camp_win_cnt[ci] += 1;
        *right += 1;
    }
    let cutoff = session_start - CLICK_REACH_MS;
    while *left < *right && clks[*left].ts_ms < cutoff {
        let ci = clks[*left].camp_idx as usize;
        scratch.camp_win_cnt[ci] -= 1;
        if scratch.camp_win_cnt[ci] == 0 {
            let pos = scratch.camp_win_pos[ci] as usize;
            let last = scratch.active_camps.pop().unwrap();
            if pos < scratch.active_camps.len() {
                scratch.active_camps[pos] = last;
                scratch.camp_win_pos[last as usize] = pos as u32;
            }
        }
        *left += 1;
    }
    if *left == *right {
        camp_acc.add_credit(organic_idx, session_day, has_purchase, sess_rev);
    } else {
        let win_camp = clks[*right - 1].camp_idx as usize;
        camp_acc.add_credit(win_camp, session_day, has_purchase, sess_rev);
        for &ci in &scratch.active_camps {
            camp_acc.add_assist(ci as usize, session_day);
        }
    }
}

/// Process one person's events (compact u64 packed: `(ts_rel << 32) | (cat_id << 24) | price_cents`)
/// and optional clicks.
fn process_person_compact(
    person_name: &[u8],
    evs: &mut [u64],
    clks: &mut [ClickRef],
    click_chunks: &[Vec<u8>],
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    if evs.is_empty() {
        return Ok(());
    }
    if evs.len() > 1 {
        evs.sort_unstable();
    }
    let clk_len = sort_and_dedup_clicks(clks, click_chunks);
    let clks = &clks[..clk_len];
    let heavy_clicks = clk_len > 32;

    let win_start = lookups.window_start_ms;
    let organic_idx = lookups.organic_idx as usize;
    let mut c_left = 0usize;
    let mut c_right = 0usize;

    let mut has_day = false;
    let mut cur_day = 0i32;
    let mut day_sessions = 0i64;
    let mut day_events = 0i64;
    let mut day_revenue = 0i64;

    let n = evs.len();
    let mut i = 0usize;
    while i < n {
        let first = evs[i];
        let sess_start_rel = (first >> 32) as u32;
        let sess_start_ms = win_start + (sess_start_rel as i64);
        let sess_day = sess_start_ms.div_euclid(MS_PER_DAY) as i32;

        if has_day && sess_day != cur_day {
            flush_user_day(
                writer,
                lookups,
                scratch,
                person_name,
                cur_day,
                day_sessions,
                day_events,
                day_revenue,
            )?;
            day_sessions = 0;
            day_events = 0;
            day_revenue = 0;
        }
        has_day = true;
        cur_day = sess_day;

        let mut prev_rel = sess_start_rel;
        let mut sess_events = 0i64;
        let mut sess_rev = 0i64;
        let mut has_purchase = false;

        loop {
            let ev = evs[i];
            let ts_rel = (ev >> 32) as u32;
            if i > 0 && sess_events > 0 && (ts_rel - prev_rel) as i64 > SESSION_GAP_MS {
                break;
            }
            prev_rel = ts_rel;
            sess_events += 1;
            let pc = ev as u32;
            let price = pc & 0x00FF_FFFF;
            if price != 0 {
                has_purchase = true;
                sess_rev += price as i64;
                let cat = (pc >> 24) as usize;
                if scratch.cat_rev[cat] == 0 {
                    scratch.touched_cats.push(cat as u32);
                }
                scratch.cat_rev[cat] += price as u64;
            }
            i += 1;
            if i == n {
                break;
            }
        }

        day_sessions += 1;
        day_events += sess_events;
        day_revenue += sess_rev;

        if clks.is_empty() {
            camp_acc.add_credit(organic_idx, sess_day, has_purchase, sess_rev);
        } else if !heavy_clicks {
            credit_and_assist_small(
                clks,
                &mut c_left,
                &mut c_right,
                sess_start_ms,
                sess_day,
                has_purchase,
                sess_rev,
                organic_idx,
                scratch,
                camp_acc,
            );
        } else {
            credit_and_assist_heavy(
                clks,
                &mut c_left,
                &mut c_right,
                sess_start_ms,
                sess_day,
                has_purchase,
                sess_rev,
                organic_idx,
                scratch,
                camp_acc,
            );
        }
    }

    if heavy_clicks {
        for &ci in &scratch.active_camps {
            scratch.camp_win_cnt[ci as usize] = 0;
        }
        scratch.active_camps.clear();
    }

    if has_day {
        flush_user_day(
            writer,
            lookups,
            scratch,
            person_name,
            cur_day,
            day_sessions,
            day_events,
            day_revenue,
        )?;
    }

    Ok(())
}

fn process_person_wide(
    person_name: &[u8],
    evs: &mut [WideEventRef],
    clks: &mut [ClickRef],
    click_chunks: &[Vec<u8>],
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    if evs.is_empty() {
        return Ok(());
    }
    if evs.len() > 1 {
        evs.sort_unstable_by_key(|e| e.ts_ms);
    }
    let clk_len = sort_and_dedup_clicks(clks, click_chunks);
    let clks = &clks[..clk_len];
    let heavy_clicks = clk_len > 32;

    let organic_idx = lookups.organic_idx as usize;
    let mut c_left = 0usize;
    let mut c_right = 0usize;

    let mut has_day = false;
    let mut cur_day = 0i32;
    let mut day_sessions = 0i64;
    let mut day_events = 0i64;
    let mut day_revenue = 0i64;

    let n = evs.len();
    let mut i = 0usize;
    while i < n {
        let sess_start_ms = evs[i].ts_ms;
        let sess_day = sess_start_ms.div_euclid(MS_PER_DAY) as i32;

        if has_day && sess_day != cur_day {
            flush_user_day(
                writer,
                lookups,
                scratch,
                person_name,
                cur_day,
                day_sessions,
                day_events,
                day_revenue,
            )?;
            day_sessions = 0;
            day_events = 0;
            day_revenue = 0;
        }
        has_day = true;
        cur_day = sess_day;

        let mut prev_ts = sess_start_ms;
        let mut sess_events = 0i64;
        let mut sess_rev = 0i64;
        let mut has_purchase = false;

        loop {
            let ev = evs[i];
            if sess_events > 0 && ev.ts_ms - prev_ts > SESSION_GAP_MS {
                break;
            }
            prev_ts = ev.ts_ms;
            sess_events += 1;
            if ev.price_cents != 0 {
                has_purchase = true;
                sess_rev += ev.price_cents as i64;
                let cat = ev.cat_id as usize;
                if scratch.cat_rev[cat] == 0 {
                    scratch.touched_cats.push(cat as u32);
                }
                scratch.cat_rev[cat] += ev.price_cents as u64;
            }
            i += 1;
            if i == n {
                break;
            }
        }

        day_sessions += 1;
        day_events += sess_events;
        day_revenue += sess_rev;

        if clks.is_empty() {
            camp_acc.add_credit(organic_idx, sess_day, has_purchase, sess_rev);
        } else if !heavy_clicks {
            credit_and_assist_small(
                clks,
                &mut c_left,
                &mut c_right,
                sess_start_ms,
                sess_day,
                has_purchase,
                sess_rev,
                organic_idx,
                scratch,
                camp_acc,
            );
        } else {
            credit_and_assist_heavy(
                clks,
                &mut c_left,
                &mut c_right,
                sess_start_ms,
                sess_day,
                has_purchase,
                sess_rev,
                organic_idx,
                scratch,
                camp_acc,
            );
        }
    }

    if heavy_clicks {
        for &ci in &scratch.active_camps {
            scratch.camp_win_cnt[ci as usize] = 0;
        }
        scratch.active_camps.clear();
    }

    if has_day {
        flush_user_day(
            writer,
            lookups,
            scratch,
            person_name,
            cur_day,
            day_sessions,
            day_events,
            day_revenue,
        )?;
    }

    Ok(())
}

fn run_compact_partition(
    ev_part: CompactPersonPart,
    clk_part: PersonClickPart,
    canon_names: &CanonNames,
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    // --- 1. Linked people in this partition ---
    if !ev_part.linked.is_empty() {
        let mut comp_to_local: HashMap<u32, u32, ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());
        let mut comp_ids: Vec<u32> = Vec::new();
        let mut ev_counts: Vec<u32> = Vec::new();

        for ev in &ev_part.linked {
            if let Some(&lidx) = comp_to_local.get(&ev.comp_id) {
                ev_counts[lidx as usize] += 1;
            } else {
                let lidx = comp_ids.len() as u32;
                comp_to_local.insert(ev.comp_id, lidx);
                comp_ids.push(ev.comp_id);
                ev_counts.push(1);
            }
        }

        let num_people = comp_ids.len();
        let mut ev_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            ev_offsets[i + 1] = ev_offsets[i] + ev_counts[i] as usize;
        }
        let mut ev_cursor = ev_offsets[..num_people].to_vec();
        let mut ev_csr = vec![0u64; ev_part.linked.len()];
        for ev in &ev_part.linked {
            let lidx = *comp_to_local.get(&ev.comp_id).unwrap() as usize;
            let pos = ev_cursor[lidx];
            ev_cursor[lidx] = pos + 1;
            ev_csr[pos] = ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
        }

        // Gather linked clicks for active people in this partition
        let mut clk_counts = vec![0u32; num_people];
        let mut parsed_clicks: Vec<(u32, ClickRef)> = Vec::with_capacity(clk_part.linked_count);
        for (ci, chunk) in clk_part.linked_chunks.iter().enumerate() {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let comp_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let camp_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let cid_len = read_varint(data, &mut pos);
                let cid_off = pos;
                pos += cid_len;
                if let Some(&lidx) = comp_to_local.get(&comp_id) {
                    clk_counts[lidx as usize] += 1;
                    parsed_clicks.push((
                        lidx,
                        ClickRef {
                            ts_ms,
                            camp_idx,
                            chunk_idx: ci as u32,
                            cid_off: cid_off as u32,
                            cid_len: cid_len as u32,
                        },
                    ));
                }
            }
        }

        let mut clk_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            clk_offsets[i + 1] = clk_offsets[i] + clk_counts[i] as usize;
        }
        let mut clk_cursor = clk_offsets[..num_people].to_vec();
        let mut clk_csr = vec![
            ClickRef {
                ts_ms: 0,
                camp_idx: 0,
                chunk_idx: 0,
                cid_off: 0,
                cid_len: 0,
            };
            parsed_clicks.len()
        ];
        for (lidx, cref) in parsed_clicks {
            let pos = clk_cursor[lidx as usize];
            clk_cursor[lidx as usize] = pos + 1;
            clk_csr[pos] = cref;
        }

        for p in 0..num_people {
            let name = canon_names.get(comp_ids[p]);
            let ev_slice = &mut ev_csr[ev_offsets[p]..ev_offsets[p + 1]];
            let clk_slice = &mut clk_csr[clk_offsets[p]..clk_offsets[p + 1]];
            process_person_compact(
                name,
                ev_slice,
                clk_slice,
                &clk_part.linked_chunks,
                lookups,
                scratch,
                camp_acc,
                writer,
            )?;
        }
    }

    // --- 2. Singleton people in this partition ---
    if ev_part.singleton_count > 0 {
        let mut uid_to_local: HashMap<&[u8], u32, ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());
        let mut uid_slices: Vec<&[u8]> = Vec::new();
        let mut ev_counts: Vec<u32> = Vec::new();
        let mut parsed_evs: Vec<(u32, u64)> = Vec::with_capacity(ev_part.singleton_count);

        for chunk in &ev_part.singletons {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen;
                let ts_rel = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let price_cat = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let lidx = if let Some(&idx) = uid_to_local.get(uid) {
                    ev_counts[idx as usize] += 1;
                    idx
                } else {
                    let idx = uid_slices.len() as u32;
                    uid_to_local.insert(uid, idx);
                    uid_slices.push(uid);
                    ev_counts.push(1);
                    idx
                };
                parsed_evs.push((lidx, ((ts_rel as u64) << 32) | (price_cat as u64)));
            }
        }

        let num_people = uid_slices.len();
        let mut ev_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            ev_offsets[i + 1] = ev_offsets[i] + ev_counts[i] as usize;
        }
        let mut ev_cursor = ev_offsets[..num_people].to_vec();
        let mut ev_csr = vec![0u64; parsed_evs.len()];
        for (lidx, packed) in parsed_evs {
            let pos = ev_cursor[lidx as usize];
            ev_cursor[lidx as usize] = pos + 1;
            ev_csr[pos] = packed;
        }

        let mut clk_counts = vec![0u32; num_people];
        let mut parsed_clicks: Vec<(u32, ClickRef)> = Vec::with_capacity(clk_part.singleton_count);
        for (ci, chunk) in clk_part.singleton_chunks.iter().enumerate() {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen;
                let ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let camp_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let cid_len = read_varint(data, &mut pos);
                let cid_off = pos;
                pos += cid_len;
                if let Some(&lidx) = uid_to_local.get(uid) {
                    clk_counts[lidx as usize] += 1;
                    parsed_clicks.push((
                        lidx,
                        ClickRef {
                            ts_ms,
                            camp_idx,
                            chunk_idx: ci as u32,
                            cid_off: cid_off as u32,
                            cid_len: cid_len as u32,
                        },
                    ));
                }
            }
        }

        let mut clk_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            clk_offsets[i + 1] = clk_offsets[i] + clk_counts[i] as usize;
        }
        let mut clk_cursor = clk_offsets[..num_people].to_vec();
        let mut clk_csr = vec![
            ClickRef {
                ts_ms: 0,
                camp_idx: 0,
                chunk_idx: 0,
                cid_off: 0,
                cid_len: 0,
            };
            parsed_clicks.len()
        ];
        for (lidx, cref) in parsed_clicks {
            let pos = clk_cursor[lidx as usize];
            clk_cursor[lidx as usize] = pos + 1;
            clk_csr[pos] = cref;
        }

        for p in 0..num_people {
            let name = uid_slices[p];
            let ev_slice = &mut ev_csr[ev_offsets[p]..ev_offsets[p + 1]];
            let clk_slice = &mut clk_csr[clk_offsets[p]..clk_offsets[p + 1]];
            process_person_compact(
                name,
                ev_slice,
                clk_slice,
                &clk_part.singleton_chunks,
                lookups,
                scratch,
                camp_acc,
                writer,
            )?;
        }
    }

    Ok(())
}

fn run_wide_partition(
    ev_part: WidePersonPart,
    clk_part: PersonClickPart,
    canon_names: &CanonNames,
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    camp_acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    if !ev_part.linked.is_empty() {
        let mut comp_to_local: HashMap<u32, u32, ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());
        let mut comp_ids: Vec<u32> = Vec::new();
        let mut ev_counts: Vec<u32> = Vec::new();

        for ev in &ev_part.linked {
            if let Some(&lidx) = comp_to_local.get(&ev.comp_id) {
                ev_counts[lidx as usize] += 1;
            } else {
                let lidx = comp_ids.len() as u32;
                comp_to_local.insert(ev.comp_id, lidx);
                comp_ids.push(ev.comp_id);
                ev_counts.push(1);
            }
        }

        let num_people = comp_ids.len();
        let mut ev_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            ev_offsets[i + 1] = ev_offsets[i] + ev_counts[i] as usize;
        }
        let mut ev_cursor = ev_offsets[..num_people].to_vec();
        let mut ev_csr = vec![
            WideEventRef {
                ts_ms: 0,
                price_cents: 0,
                cat_id: 0,
            };
            ev_part.linked.len()
        ];
        for ev in &ev_part.linked {
            let lidx = *comp_to_local.get(&ev.comp_id).unwrap() as usize;
            let pos = ev_cursor[lidx];
            ev_cursor[lidx] = pos + 1;
            ev_csr[pos] = WideEventRef {
                ts_ms: ev.ts_ms,
                price_cents: ev.price_cents,
                cat_id: ev.cat_id,
            };
        }

        let mut clk_counts = vec![0u32; num_people];
        let mut parsed_clicks: Vec<(u32, ClickRef)> = Vec::with_capacity(clk_part.linked_count);
        for (ci, chunk) in clk_part.linked_chunks.iter().enumerate() {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let comp_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let camp_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let cid_len = read_varint(data, &mut pos);
                let cid_off = pos;
                pos += cid_len;
                if let Some(&lidx) = comp_to_local.get(&comp_id) {
                    clk_counts[lidx as usize] += 1;
                    parsed_clicks.push((
                        lidx,
                        ClickRef {
                            ts_ms,
                            camp_idx,
                            chunk_idx: ci as u32,
                            cid_off: cid_off as u32,
                            cid_len: cid_len as u32,
                        },
                    ));
                }
            }
        }

        let mut clk_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            clk_offsets[i + 1] = clk_offsets[i] + clk_counts[i] as usize;
        }
        let mut clk_cursor = clk_offsets[..num_people].to_vec();
        let mut clk_csr = vec![
            ClickRef {
                ts_ms: 0,
                camp_idx: 0,
                chunk_idx: 0,
                cid_off: 0,
                cid_len: 0,
            };
            parsed_clicks.len()
        ];
        for (lidx, cref) in parsed_clicks {
            let pos = clk_cursor[lidx as usize];
            clk_cursor[lidx as usize] = pos + 1;
            clk_csr[pos] = cref;
        }

        for p in 0..num_people {
            let name = canon_names.get(comp_ids[p]);
            let ev_slice = &mut ev_csr[ev_offsets[p]..ev_offsets[p + 1]];
            let clk_slice = &mut clk_csr[clk_offsets[p]..clk_offsets[p + 1]];
            process_person_wide(
                name,
                ev_slice,
                clk_slice,
                &clk_part.linked_chunks,
                lookups,
                scratch,
                camp_acc,
                writer,
            )?;
        }
    }

    if ev_part.singleton_count > 0 {
        let mut uid_to_local: HashMap<&[u8], u32, ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());
        let mut uid_slices: Vec<&[u8]> = Vec::new();
        let mut ev_counts: Vec<u32> = Vec::new();
        let mut parsed_evs: Vec<(u32, WideEventRef)> = Vec::with_capacity(ev_part.singleton_count);

        for chunk in &ev_part.singletons {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen;
                let ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let price_cents = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let cat_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let lidx = if let Some(&idx) = uid_to_local.get(uid) {
                    ev_counts[idx as usize] += 1;
                    idx
                } else {
                    let idx = uid_slices.len() as u32;
                    uid_to_local.insert(uid, idx);
                    uid_slices.push(uid);
                    ev_counts.push(1);
                    idx
                };
                parsed_evs.push((
                    lidx,
                    WideEventRef {
                        ts_ms,
                        price_cents,
                        cat_id,
                    },
                ));
            }
        }

        let num_people = uid_slices.len();
        let mut ev_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            ev_offsets[i + 1] = ev_offsets[i] + ev_counts[i] as usize;
        }
        let mut ev_cursor = ev_offsets[..num_people].to_vec();
        let mut ev_csr = vec![
            WideEventRef {
                ts_ms: 0,
                price_cents: 0,
                cat_id: 0,
            };
            parsed_evs.len()
        ];
        for (lidx, ev) in parsed_evs {
            let pos = ev_cursor[lidx as usize];
            ev_cursor[lidx as usize] = pos + 1;
            ev_csr[pos] = ev;
        }

        let mut clk_counts = vec![0u32; num_people];
        let mut parsed_clicks: Vec<(u32, ClickRef)> = Vec::with_capacity(clk_part.singleton_count);
        for (ci, chunk) in clk_part.singleton_chunks.iter().enumerate() {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            let len = data.len();
            while pos < len {
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen;
                let ts_ms = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let camp_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let cid_len = read_varint(data, &mut pos);
                let cid_off = pos;
                pos += cid_len;
                if let Some(&lidx) = uid_to_local.get(uid) {
                    clk_counts[lidx as usize] += 1;
                    parsed_clicks.push((
                        lidx,
                        ClickRef {
                            ts_ms,
                            camp_idx,
                            chunk_idx: ci as u32,
                            cid_off: cid_off as u32,
                            cid_len: cid_len as u32,
                        },
                    ));
                }
            }
        }

        let mut clk_offsets = vec![0usize; num_people + 1];
        for i in 0..num_people {
            clk_offsets[i + 1] = clk_offsets[i] + clk_counts[i] as usize;
        }
        let mut clk_cursor = clk_offsets[..num_people].to_vec();
        let mut clk_csr = vec![
            ClickRef {
                ts_ms: 0,
                camp_idx: 0,
                chunk_idx: 0,
                cid_off: 0,
                cid_len: 0,
            };
            parsed_clicks.len()
        ];
        for (lidx, cref) in parsed_clicks {
            let pos = clk_cursor[lidx as usize];
            clk_cursor[lidx as usize] = pos + 1;
            clk_csr[pos] = cref;
        }

        for p in 0..num_people {
            let name = uid_slices[p];
            let ev_slice = &mut ev_csr[ev_offsets[p]..ev_offsets[p + 1]];
            let clk_slice = &mut clk_csr[clk_offsets[p]..clk_offsets[p + 1]];
            process_person_wide(
                name,
                ev_slice,
                clk_slice,
                &clk_part.singleton_chunks,
                lookups,
                scratch,
                camp_acc,
                writer,
            )?;
        }
    }

    Ok(())
}

/// Phase 4: Process all person partitions in LPT (largest-first) order across `ncpu` threads,
/// streaming `user_daily` directly to parallel Parquet files and merging `campaign_daily`.
pub fn run_sessions_and_write(
    events_store: PersonEventStore,
    clicks_parts: Vec<PersonClickPart>,
    canon_names: &CanonNames,
    lookups: &Lookups,
    output_dir: &Path,
    ncpu: usize,
) -> io::Result<()> {
    let user_daily_dir = output_dir.join("user_daily");
    let campaign_daily_dir = output_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir)?;
    fs::create_dir_all(&campaign_daily_dir)?;

    // Build LPT work order (only non-empty event partitions need work!)
    let mut order: Vec<(usize, usize)> = (0..PERSON_PARTS)
        .filter_map(|pp| {
            let ev_cnt = events_store.part_event_count(pp);
            if ev_cnt > 0 {
                let weight = ev_cnt + clicks_parts[pp].total_clicks();
                Some((weight, pp))
            } else {
                None
            }
        })
        .collect();

    if order.is_empty() {
        write_empty_user_daily(&user_daily_dir)?;
        write_campaign_daily(&campaign_daily_dir, &lookups.campaigns, &[])?;
        return Ok(());
    }

    // Largest partitions first so whales and shared-device kiosks start immediately at t=0.
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work_pps: Vec<usize> = order.into_iter().map(|(_, pp)| pp).collect();

    let workers = ncpu.min(work_pps.len()).max(1);
    let next_idx = AtomicUsize::new(0);
    let io_err: Mutex<Option<io::Error>> = Mutex::new(None);
    let thread_camps: Vec<Mutex<Option<CampaignAcc>>> =
        (0..workers).map(|_| Mutex::new(None)).collect();
    let total_user_rows = AtomicUsize::new(0);

    let clicks_slots: Vec<Mutex<Option<PersonClickPart>>> = clicks_parts
        .into_iter()
        .map(|p| Mutex::new(Some(p)))
        .collect();

    match events_store {
        PersonEventStore::Compact(ev_parts) => {
            let ev_slots: Vec<Mutex<Option<CompactPersonPart>>> = ev_parts
                .into_iter()
                .map(|p| Mutex::new(Some(p)))
                .collect();

            thread::scope(|s| {
                for tid in 0..workers {
                    let ev_slots = &ev_slots;
                    let clicks_slots = &clicks_slots;
                    let work_pps = &work_pps;
                    let next_idx = &next_idx;
                    let io_err = &io_err;
                    let thread_camps = &thread_camps;
                    let total_user_rows = &total_user_rows;
                    let out_path = user_daily_dir.join(format!("part-{tid:03}.parquet"));

                    s.spawn(move || {
                        let mut scratch =
                            WorkerScratch::new(lookups.campaigns.len(), lookups.categories.len());
                        let mut camp_acc = CampaignAcc::new(lookups);
                        let mut writer = UserDailyWriter::new(out_path);

                        loop {
                            let wi = next_idx.fetch_add(1, Ordering::Relaxed);
                            let Some(&pp) = work_pps.get(wi) else {
                                break;
                            };
                            let ev_part = ev_slots[pp].lock().unwrap().take().unwrap();
                            let clk_part = clicks_slots[pp].lock().unwrap().take().unwrap();
                            if let Err(e) = run_compact_partition(
                                ev_part,
                                clk_part,
                                canon_names,
                                lookups,
                                &mut scratch,
                                &mut camp_acc,
                                &mut writer,
                            ) {
                                let mut g = io_err.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(e);
                                }
                                return;
                            }
                        }

                        match writer.finish(tid == 0) {
                            Ok(n) => {
                                total_user_rows.fetch_add(n, Ordering::Relaxed);
                            }
                            Err(e) => {
                                let mut g = io_err.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(e);
                                }
                                return;
                            }
                        }
                        *thread_camps[tid].lock().unwrap() = Some(camp_acc);
                    });
                }
            });
        }
        PersonEventStore::Wide(ev_parts) => {
            let ev_slots: Vec<Mutex<Option<WidePersonPart>>> = ev_parts
                .into_iter()
                .map(|p| Mutex::new(Some(p)))
                .collect();

            thread::scope(|s| {
                for tid in 0..workers {
                    let ev_slots = &ev_slots;
                    let clicks_slots = &clicks_slots;
                    let work_pps = &work_pps;
                    let next_idx = &next_idx;
                    let io_err = &io_err;
                    let thread_camps = &thread_camps;
                    let total_user_rows = &total_user_rows;
                    let out_path = user_daily_dir.join(format!("part-{tid:03}.parquet"));

                    s.spawn(move || {
                        let mut scratch =
                            WorkerScratch::new(lookups.campaigns.len(), lookups.categories.len());
                        let mut camp_acc = CampaignAcc::new(lookups);
                        let mut writer = UserDailyWriter::new(out_path);

                        loop {
                            let wi = next_idx.fetch_add(1, Ordering::Relaxed);
                            let Some(&pp) = work_pps.get(wi) else {
                                break;
                            };
                            let ev_part = ev_slots[pp].lock().unwrap().take().unwrap();
                            let clk_part = clicks_slots[pp].lock().unwrap().take().unwrap();
                            if let Err(e) = run_wide_partition(
                                ev_part,
                                clk_part,
                                canon_names,
                                lookups,
                                &mut scratch,
                                &mut camp_acc,
                                &mut writer,
                            ) {
                                let mut g = io_err.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(e);
                                }
                                return;
                            }
                        }

                        match writer.finish(tid == 0) {
                            Ok(n) => {
                                total_user_rows.fetch_add(n, Ordering::Relaxed);
                            }
                            Err(e) => {
                                let mut g = io_err.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(e);
                                }
                                return;
                            }
                        }
                        *thread_camps[tid].lock().unwrap() = Some(camp_acc);
                    });
                }
            });
        }
    }

    if let Some(e) = io_err.into_inner().unwrap() {
        return Err(e);
    }

    // Merge campaign_daily across workers
    let mut merged = CampaignAcc::new(lookups);
    for m in thread_camps {
        if let Some(acc) = m.into_inner().unwrap() {
            merged.merge_from(acc);
        }
    }
    let rows = merged.into_rows();
    write_campaign_daily(&campaign_daily_dir, &lookups.campaigns, &rows)?;

    Ok(())
}
