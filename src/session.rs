use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use hashbrown::{HashMap, HashTable};

use crate::clicks::{PersonClickPart, PersonClicks};
use crate::events::{EvKey, PersonEvents, PersonPart};
use crate::hash::{PERSON_PARTS, fast_build_hasher, rd_u32, rd_u64, read_varint};
use crate::links::CanonNames;
use crate::lookups::Lookups;
use crate::output::{CampaignDailyRow, UserDailyWriter, write_campaign_daily, write_empty_user_daily};
use crate::timestamp::{CLICK_REACH_MS, MS_PER_DAY, SESSION_GAP_MS};

/// Above this many clicks, a person's assists use the O(1) sliding-window tracker.
const HEAVY_CLICKS: usize = 32;

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
    fn cell(&mut self, camp_idx: usize, day: i32) -> &mut CampDayAgg {
        match self {
            CampaignAcc::Dense {
                window_start_day,
                num_days,
                cells,
                ..
            } => {
                let d = (day - *window_start_day) as usize;
                let idx = camp_idx * *num_days + d;
                debug_assert!(idx < cells.len());
                unsafe { cells.get_unchecked_mut(idx) }
            }
            CampaignAcc::Sparse(map) => map.entry((camp_idx as u32, day)).or_default(),
        }
    }

    #[inline(always)]
    fn add_credit(&mut self, camp_idx: usize, day: i32, has_purchase: bool, revenue: i64) {
        let cell = self.cell(camp_idx, day);
        cell.sessions += 1;
        cell.purchasing_sessions += has_purchase as i64;
        cell.revenue_cents += revenue;
    }

    #[inline(always)]
    fn add_assist(&mut self, camp_idx: usize, day: i32) {
        self.cell(camp_idx, day).assisted_sessions += 1;
    }

    fn merge_from(&mut self, other: CampaignAcc) {
        let add = |a: &mut CampDayAgg, b: &CampDayAgg| {
            a.sessions += b.sessions;
            a.purchasing_sessions += b.purchasing_sessions;
            a.revenue_cents += b.revenue_cents;
            a.assisted_sessions += b.assisted_sessions;
        };
        match (self, other) {
            (CampaignAcc::Dense { cells: a, .. }, CampaignAcc::Dense { cells: b, .. }) => {
                for (ca, cb) in a.iter_mut().zip(b.iter()) {
                    add(ca, cb);
                }
            }
            (CampaignAcc::Sparse(a), CampaignAcc::Sparse(b)) => {
                for (k, cb) in b {
                    add(a.entry(k).or_default(), &cb);
                }
            }
            _ => unreachable!(),
        }
    }

    fn into_rows(self) -> Vec<CampaignDailyRow> {
        let mut rows = Vec::new();
        let mut push = |camp_idx: usize, day: i32, agg: CampDayAgg| {
            if agg.sessions > 0 || agg.assisted_sessions > 0 {
                rows.push(CampaignDailyRow {
                    camp_idx,
                    day,
                    sessions: agg.sessions,
                    purchasing_sessions: agg.purchasing_sessions,
                    revenue_cents: agg.revenue_cents,
                    assisted_sessions: agg.assisted_sessions,
                });
            }
        };
        match self {
            CampaignAcc::Dense {
                window_start_day,
                num_days,
                num_campaigns,
                cells,
            } => {
                for c in 0..num_campaigns {
                    for d in 0..num_days {
                        push(c, window_start_day + d as i32, cells[c * num_days + d]);
                    }
                }
            }
            CampaignAcc::Sparse(map) => {
                for ((c, day), agg) in map {
                    push(c as usize, day, agg);
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

impl ClickRef {
    #[inline(always)]
    fn cid<'a>(&self, chunks: &'a [&'a [u8]]) -> &'a [u8] {
        &chunks[self.chunk_idx as usize][self.cid_off as usize..(self.cid_off + self.cid_len) as usize]
    }
}

/// Per-thread scratch state, reused across people so the hot loop never allocates.
struct WorkerScratch {
    assist_seen: Vec<u32>,
    assist_gen: u32,
    camp_win_cnt: Vec<u32>,
    camp_win_pos: Vec<u32>,
    active_camps: Vec<u32>,
    cat_rev: Vec<u64>,
    touched_cats: Vec<u32>,
    /// Component ID to local person index; `u32::MAX` when unused.
    comp_local: Vec<u32>,
}

impl WorkerScratch {
    fn new(num_campaigns: usize, num_categories: usize, num_comps: usize) -> Self {
        Self {
            assist_seen: vec![0; num_campaigns],
            assist_gen: 0,
            camp_win_cnt: vec![0; num_campaigns],
            camp_win_pos: vec![0; num_campaigns],
            active_camps: Vec::with_capacity(64),
            cat_rev: vec![0; num_categories],
            touched_cats: Vec::with_capacity(16),
            comp_local: vec![u32::MAX; num_comps],
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

/// Sort clicks by `(ts ASC, click_id DESC)` and drop repeated `click_id`s, so the
/// last click at or before a session start is the one that gets the credit.
fn sort_and_dedup_clicks(clks: &mut [ClickRef], chunks: &[&[u8]]) -> usize {
    let n = clks.len();
    if n <= 1 {
        return n;
    }
    if n == 2 {
        if clks[1].ts_ms < clks[0].ts_ms {
            clks.swap(0, 1);
        }
        if clks[0].ts_ms == clks[1].ts_ms {
            let c0 = clks[0].cid(chunks);
            let c1 = clks[1].cid(chunks);
            if c0 < c1 {
                clks.swap(0, 1);
            } else if c0 == c1 {
                return 1;
            }
        }
        return 2;
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
            clks[i..j].sort_unstable_by(|a, b| b.cid(chunks).cmp(a.cid(chunks)));
            clks[write] = clks[i];
            write += 1;
            for k in (i + 1)..j {
                if clks[k].cid(chunks) != clks[k - 1].cid(chunks) {
                    clks[write] = clks[k];
                    write += 1;
                }
            }
        }
        i = j;
    }
    write
}

struct DayAcc {
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
}

fn flush_user_day(
    writer: &mut UserDailyWriter,
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    name: &[u8],
    acc: &DayAcc,
) -> io::Result<()> {
    let top = if scratch.touched_cats.is_empty() {
        None
    } else {
        let mut best = scratch.touched_cats[0];
        let mut best_rev = scratch.cat_rev[best as usize];
        scratch.cat_rev[best as usize] = 0;
        for &c in &scratch.touched_cats[1..] {
            let r = scratch.cat_rev[c as usize];
            scratch.cat_rev[c as usize] = 0;
            if r > best_rev || (r == best_rev && c < best) {
                best_rev = r;
                best = c;
            }
        }
        scratch.touched_cats.clear();
        Some(lookups.categories[best as usize].as_str())
    };
    writer.push(
        name,
        lookups.all_ids_utf8,
        acc.day,
        acc.sessions,
        acc.events,
        acc.revenue,
        top,
    )
}

/// Credit one session and record its assists. `left..right` is the window of
/// clicks with `start - 7d <= ts <= start`; both pointers only move forward.
#[inline(always)]
fn attribute(
    clks: &[ClickRef],
    heavy: bool,
    left: &mut usize,
    right: &mut usize,
    start: i64,
    day: i32,
    has_purchase: bool,
    revenue: i64,
    organic: usize,
    scratch: &mut WorkerScratch,
    acc: &mut CampaignAcc,
) {
    let n = clks.len();
    while *right < n && unsafe { clks.get_unchecked(*right).ts_ms } <= start {
        if heavy {
            let ci = unsafe { clks.get_unchecked(*right).camp_idx } as usize;
            if scratch.camp_win_cnt[ci] == 0 {
                scratch.camp_win_pos[ci] = scratch.active_camps.len() as u32;
                scratch.active_camps.push(ci as u32);
            }
            scratch.camp_win_cnt[ci] += 1;
        }
        *right += 1;
    }
    let cutoff = start - CLICK_REACH_MS;
    while *left < *right && unsafe { clks.get_unchecked(*left).ts_ms } < cutoff {
        if heavy {
            let ci = unsafe { clks.get_unchecked(*left).camp_idx } as usize;
            scratch.camp_win_cnt[ci] -= 1;
            if scratch.camp_win_cnt[ci] == 0 {
                let pos = scratch.camp_win_pos[ci] as usize;
                let last = scratch.active_camps.pop().unwrap();
                if pos < scratch.active_camps.len() {
                    scratch.active_camps[pos] = last;
                    scratch.camp_win_pos[last as usize] = pos as u32;
                }
            }
        }
        *left += 1;
    }
    if *left == *right {
        acc.add_credit(organic, day, has_purchase, revenue);
        return;
    }
    let win_len = *right - *left;
    let credited = unsafe { clks.get_unchecked(*right - 1).camp_idx } as usize;
    acc.add_credit(credited, day, has_purchase, revenue);
    if win_len == 1 {
        acc.add_assist(credited, day);
    } else if win_len == 2 {
        let c0 = unsafe { clks.get_unchecked(*left).camp_idx } as usize;
        acc.add_assist(credited, day);
        if c0 != credited {
            acc.add_assist(c0, day);
        }
    } else if win_len == 3 {
        let c0 = unsafe { clks.get_unchecked(*left).camp_idx } as usize;
        let c1 = unsafe { clks.get_unchecked(*left + 1).camp_idx } as usize;
        acc.add_assist(credited, day);
        if c0 != credited {
            acc.add_assist(c0, day);
        }
        if c1 != credited && c1 != c0 {
            acc.add_assist(c1, day);
        }
    } else if heavy {
        for &ci in &scratch.active_camps {
            acc.add_assist(ci as usize, day);
        }
    } else {
        let gen = scratch.next_assist_gen();
        for c in unsafe { clks.get_unchecked(*left..*right) } {
            let ci = c.camp_idx as usize;
            if unsafe { *scratch.assist_seen.get_unchecked(ci) } != gen {
                unsafe { *scratch.assist_seen.get_unchecked_mut(ci) = gen };
                acc.add_assist(ci, day);
            }
        }
    }
}

fn process_person<K: EvKey>(
    name: &[u8],
    evs: &mut [K],
    clks: &mut [ClickRef],
    click_chunks: &[&[u8]],
    lookups: &Lookups,
    scratch: &mut WorkerScratch,
    acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    let n = evs.len();
    match n {
        0 => return Ok(()),
        1 => {}
        2 => {
            if evs[1] < evs[0] {
                evs.swap(0, 1);
            }
        }
        3 => {
            if evs[1] < evs[0] {
                evs.swap(0, 1);
            }
            if evs[2] < evs[1] {
                evs.swap(1, 2);
                if evs[1] < evs[0] {
                    evs.swap(0, 1);
                }
            }
        }
        4 => {
            if evs[1] < evs[0] { evs.swap(0, 1); }
            if evs[3] < evs[2] { evs.swap(2, 3); }
            if evs[2] < evs[0] { evs.swap(0, 2); }
            if evs[3] < evs[1] { evs.swap(1, 3); }
            if evs[2] < evs[1] { evs.swap(1, 2); }
        }
        _ => evs.sort_unstable(),
    }
    let clk_len = sort_and_dedup_clicks(clks, click_chunks);
    let clks = &clks[..clk_len];
    let heavy = clk_len > HEAVY_CLICKS;
    let win_start = lookups.window_start_ms;
    let organic = lookups.organic_idx as usize;
    let (mut left, mut right) = (0usize, 0usize);
    let mut day: Option<DayAcc> = None;

    let mut i = 0usize;
    while i < n {
        let start_rel = evs[i].ts_rel();
        let start_ms = win_start + start_rel as i64;
        let sess_day = if lookups.compact_mode {
            lookups.window_start_day + (start_rel / (MS_PER_DAY as u64)) as i32
        } else {
            start_ms.div_euclid(MS_PER_DAY) as i32
        };

        match &mut day {
            Some(d) if d.day == sess_day => {}
            _ => {
                if let Some(d) = day.take() {
                    flush_user_day(writer, lookups, scratch, name, &d)?;
                }
                day = Some(DayAcc {
                    day: sess_day,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                });
            }
        }

        let mut prev = start_rel;
        let mut sess_events = 0i64;
        let mut sess_rev = 0i64;
        let mut has_purchase = false;
        while i < n {
            let k = evs[i];
            let ts = k.ts_rel();
            if ts - prev > SESSION_GAP_MS as u64 {
                break;
            }
            prev = ts;
            sess_events += 1;
            let price = k.price();
            if price != 0 {
                has_purchase = true;
                sess_rev += price as i64;
                let cat = k.cat() as usize;
                if scratch.cat_rev[cat] == 0 {
                    scratch.touched_cats.push(cat as u32);
                }
                scratch.cat_rev[cat] += price as u64;
            }
            i += 1;
        }

        let d = day.as_mut().unwrap();
        d.sessions += 1;
        d.events += sess_events;
        d.revenue += sess_rev;

        if clks.is_empty() {
            acc.add_credit(organic, sess_day, has_purchase, sess_rev);
        } else {
            attribute(
                clks, heavy, &mut left, &mut right, start_ms, sess_day, has_purchase, sess_rev,
                organic, scratch, acc,
            );
        }
    }

    if heavy {
        for &ci in &scratch.active_camps {
            scratch.camp_win_cnt[ci as usize] = 0;
        }
        scratch.active_camps.clear();
    }
    if let Some(d) = day {
        flush_user_day(writer, lookups, scratch, name, &d)?;
    }
    Ok(())
}

/// Group `(local_person, item)` pairs into contiguous per-person slices.
fn build_csr<T: Copy>(
    items: &[(u32, T)],
    num_people: usize,
    offsets: &mut Vec<usize>,
    cursor: &mut Vec<usize>,
    out: &mut Vec<T>,
) {
    offsets.clear();
    offsets.resize(num_people + 1, 0);
    for &(l, _) in items {
        offsets[l as usize + 1] += 1;
    }
    for p in 0..num_people {
        offsets[p + 1] += offsets[p];
    }
    out.clear();
    if items.is_empty() {
        return;
    }
    cursor.clear();
    cursor.extend_from_slice(&offsets[..num_people]);
    out.resize(items.len(), items[0].1);
    for &(l, v) in items {
        let pos = &mut cursor[l as usize];
        out[*pos] = v;
        *pos += 1;
    }
}

/// Decode click chunks, keeping the clicks whose owner maps to a local person.
/// `linked` chunks start with a `comp_id`; singleton chunks with the user ID.
fn gather_clicks(
    chunks: &[&[u8]],
    expected: usize,
    linked: bool,
    out: &mut Vec<(u32, ClickRef)>,
    mut local_of: impl FnMut(u32, &[u8], u64) -> Option<u32>,
) {
    out.clear();
    out.reserve(expected);
    for (ci, data) in chunks.iter().enumerate() {
        let data: &[u8] = data;
        let mut pos = 0usize;
        while pos < data.len() {
            let local = if linked {
                let comp = rd_u32(data, pos);
                pos += 4;
                local_of(comp, &[], 0)
            } else {
                let h = rd_u64(data, pos);
                pos += 8;
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen;
                local_of(0, uid, h)
            };
            let ts_ms = rd_u64(data, pos) as i64;
            let camp_idx = rd_u32(data, pos + 8);
            pos += 12;
            let cid_len = read_varint(data, &mut pos);
            let cid_off = pos;
            pos += cid_len;
            if let Some(l) = local {
                out.push((
                    l,
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
}

/// Second grouping pass: walk the chunks again in the same order and drop
/// each event's key into its person's slot. `read` decodes one record at
/// `pos` and returns the key and the next position.
fn place_keys<K: EvKey>(
    chunks: &[crate::arena::RawChunk],
    lids: &[u32],
    counts: &[u32],
    offsets: &mut Vec<usize>,
    cursor: &mut Vec<u32>,
    keys: &mut Vec<K>,
    read: impl Fn(&[u8], usize) -> (K, usize),
) {
    let people = counts.len();
    offsets.clear();
    offsets.reserve(people + 1);
    offsets.push(0);
    for p in 0..people {
        offsets.push(offsets[p] + counts[p] as usize);
    }
    cursor.clear();
    cursor.extend(offsets[..people].iter().map(|&o| o as u32));
    keys.clear();
    keys.resize(lids.len(), K::pack(0, 0, 0));
    let mut k = 0usize;
    for chunk in chunks {
        let data = chunk.as_slice();
        let mut pos = 0usize;
        while pos < data.len() {
            let (key, next) = read(data, pos);
            pos = next;
            let c = unsafe { cursor.get_unchecked_mut(*lids.get_unchecked(k) as usize) };
            unsafe { *keys.get_unchecked_mut(*c as usize) = key };
            *c += 1;
            k += 1;
        }
    }
    debug_assert_eq!(k, lids.len());
}

struct PartitionScratch<K: EvKey> {
    comp_ids: Vec<u32>,
    counts: Vec<u32>,
    lids: Vec<u32>,
    uid_h: Vec<u64>,
    uid_ptrs: Vec<(*const u8, u32)>,
    table: HashTable<u32>,
    clicks: Vec<(u32, ClickRef)>,
    ck_out: Vec<ClickRef>,
    offsets: Vec<usize>,
    ck_offsets: Vec<usize>,
    cursor: Vec<u32>,
    csr_cursor: Vec<usize>,
    keys: Vec<K>,
}

impl<K: EvKey> PartitionScratch<K> {
    fn new() -> Self {
        Self {
            comp_ids: Vec::with_capacity(4096),
            counts: Vec::with_capacity(4096),
            lids: Vec::with_capacity(32768),
            uid_h: Vec::with_capacity(4096),
            uid_ptrs: Vec::with_capacity(4096),
            table: HashTable::with_capacity(4096),
            clicks: Vec::with_capacity(8192),
            ck_out: Vec::with_capacity(8192),
            offsets: Vec::with_capacity(4096),
            ck_offsets: Vec::with_capacity(4096),
            cursor: Vec::with_capacity(4096),
            csr_cursor: Vec::with_capacity(4096),
            keys: Vec::with_capacity(32768),
        }
    }
}

struct Ctx<'a> {
    canon: &'a CanonNames,
    lookups: &'a Lookups,
}

fn run_partition<K: EvKey>(
    ev: PersonPart<K>,
    clk: PersonClickPart,
    ctx: &Ctx<'_>,
    scratch: &mut WorkerScratch,
    ps: &mut PartitionScratch<K>,
    acc: &mut CampaignAcc,
    writer: &mut UserDailyWriter,
) -> io::Result<()> {
    // Linked people, keyed by component ID through a dense per-thread array.
    if ev.linked_count > 0 {
        let group = crate::prof::Span::new(crate::prof::SESS_GROUP);
        let comp_local = &mut scratch.comp_local;
        ps.comp_ids.clear();
        ps.counts.clear();
        ps.lids.clear();
        ps.lids.reserve(ev.linked_count);

        let rec = 4 + K::BYTES;
        for chunk in &ev.linked {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            while pos < data.len() {
                let comp = rd_u32(data, pos);
                pos += rec;
                let slot = &mut comp_local[comp as usize];
                if *slot == u32::MAX {
                    *slot = ps.comp_ids.len() as u32;
                    ps.comp_ids.push(comp);
                    ps.counts.push(0);
                }
                ps.counts[*slot as usize] += 1;
                ps.lids.push(*slot);
            }
        }
        let people = ps.comp_ids.len();
        place_keys::<K>(
            &ev.linked,
            &ps.lids,
            &ps.counts,
            &mut ps.offsets,
            &mut ps.cursor,
            &mut ps.keys,
            |data, pos| {
                let key = K::read_from(data, pos + 4);
                (key, pos + rec)
            },
        );
        let comp_local = &scratch.comp_local;
        let lchunks: Vec<&[u8]> = clk.linked_chunks.iter().map(|c| c.as_slice()).collect();
        gather_clicks(&lchunks, clk.linked_count, true, &mut ps.clicks, |comp, _, _| {
            let l = comp_local[comp as usize];
            (l != u32::MAX).then_some(l)
        });
        for &c in &ps.comp_ids {
            scratch.comp_local[c as usize] = u32::MAX;
        }
        build_csr(&ps.clicks, people, &mut ps.ck_offsets, &mut ps.csr_cursor, &mut ps.ck_out);
        drop(group);

        let _p = crate::prof::Span::new(crate::prof::SESS_PROCESS);
        for p in 0..people {
            process_person(
                ctx.canon.get(ps.comp_ids[p]),
                &mut ps.keys[ps.offsets[p]..ps.offsets[p + 1]],
                &mut ps.ck_out[ps.ck_offsets[p]..ps.ck_offsets[p + 1]],
                &lchunks,
                ctx.lookups,
                scratch,
                acc,
                writer,
            )?;
        }
    }

    // People with a single unlinked ID, keyed by the ID itself.
    if ev.singleton_count > 0 {
        let group = crate::prof::Span::new(crate::prof::SESS_GROUP);
        ps.table.clear();
        ps.uid_ptrs.clear();
        ps.uid_h.clear();
        ps.counts.clear();
        ps.lids.clear();
        ps.lids.reserve(ev.singleton_count);

        for chunk in &ev.singletons {
            let data = chunk.as_slice();
            let mut pos = 0usize;
            while pos < data.len() {
                let h = rd_u64(data, pos);
                pos += 8;
                let ulen = read_varint(data, &mut pos);
                let uid = &data[pos..pos + ulen];
                pos += ulen + K::BYTES;
                let th = h.rotate_left(32);
                let l = match ps.table.find(th, |&l| {
                    ps.uid_h[l as usize] == h && {
                        let (p, len) = ps.uid_ptrs[l as usize];
                        unsafe { std::slice::from_raw_parts(p, len as usize) == uid }
                    }
                }) {
                    Some(&l) => l,
                    None => {
                        let l = ps.uid_ptrs.len() as u32;
                        ps.uid_ptrs.push((uid.as_ptr(), uid.len() as u32));
                        ps.uid_h.push(h);
                        ps.counts.push(0);
                        ps.table.insert_unique(th, l, |&l| ps.uid_h[l as usize].rotate_left(32));
                        l
                    }
                };
                ps.counts[l as usize] += 1;
                ps.lids.push(l);
            }
        }
        let people = ps.uid_ptrs.len();
        place_keys::<K>(
            &ev.singletons,
            &ps.lids,
            &ps.counts,
            &mut ps.offsets,
            &mut ps.cursor,
            &mut ps.keys,
            |data, mut pos| {
                pos += 8;
                let ulen = read_varint(data, &mut pos);
                pos += ulen;
                (K::read_from(data, pos), pos + K::BYTES)
            },
        );
        let schunks: Vec<&[u8]> = clk.singleton_chunks.iter().map(|c| c.as_slice()).collect();
        gather_clicks(&schunks, clk.singleton_count, false, &mut ps.clicks, |_, uid, h| {
            ps.table
                .find(h.rotate_left(32), |&l| {
                    ps.uid_h[l as usize] == h && {
                        let (p, len) = ps.uid_ptrs[l as usize];
                        unsafe { std::slice::from_raw_parts(p, len as usize) == uid }
                    }
                })
                .copied()
        });
        build_csr(&ps.clicks, people, &mut ps.ck_offsets, &mut ps.csr_cursor, &mut ps.ck_out);
        drop(group);

        let _p = crate::prof::Span::new(crate::prof::SESS_PROCESS);
        for p in 0..people {
            let (ptr, len) = ps.uid_ptrs[p];
            let uid = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            process_person(
                uid,
                &mut ps.keys[ps.offsets[p]..ps.offsets[p + 1]],
                &mut ps.ck_out[ps.ck_offsets[p]..ps.ck_offsets[p + 1]],
                &schunks,
                ctx.lookups,
                scratch,
                acc,
                writer,
            )?;
        }
    }
    Ok(())
}

/// Phase 4: process person partitions largest-first across `ncpu` threads,
/// streaming `user_daily` to one Parquet file per thread and merging
/// `campaign_daily` at the end.
pub fn run_sessions_and_write<K: EvKey>(
    person_events: PersonEvents<K>,
    person_clicks: PersonClicks,
    canon: &CanonNames,
    lookups: &Lookups,
    output_dir: &Path,
    ncpu: usize,
) -> io::Result<()> {
    // The arenas back every chunk in `events`; they drop when this function returns.
    let PersonEvents { parts: events, arenas: _keep_arenas } = person_events;
    let PersonClicks { parts: clicks, arenas: _keep_click_arenas } = person_clicks;
    let user_dir = output_dir.join("user_daily");
    let camp_dir = output_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&camp_dir)?;

    let mut order: Vec<(usize, usize)> = (0..PERSON_PARTS)
        .filter(|&pp| events[pp].event_count() > 0)
        .map(|pp| (events[pp].event_count() + clicks[pp].total_clicks(), pp))
        .collect();
    if order.is_empty() {
        write_empty_user_daily(&user_dir)?;
        write_campaign_daily(&camp_dir, &lookups.campaigns, &[])?;
        return Ok(());
    }
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work: Vec<usize> = order.into_iter().map(|(_, pp)| pp).collect();

    let workers = ncpu.min(work.len()).max(1);
    let next = AtomicUsize::new(0);
    let err: Mutex<Option<io::Error>> = Mutex::new(None);
    let accs: Vec<Mutex<Option<CampaignAcc>>> = (0..workers).map(|_| Mutex::new(None)).collect();
    let ev_slots: Vec<Mutex<Option<PersonPart<K>>>> =
        events.into_iter().map(|p| Mutex::new(Some(p))).collect();
    let ck_slots: Vec<Mutex<Option<PersonClickPart>>> =
        clicks.into_iter().map(|p| Mutex::new(Some(p))).collect();
    let ctx = Ctx { canon, lookups };

    thread::scope(|s| {
        for tid in 0..workers {
            let (ev_slots, ck_slots, work, next, err, accs, ctx) =
                (&ev_slots, &ck_slots, &work, &next, &err, &accs, &ctx);
            let path = user_dir.join(format!("part-{tid:03}.parquet"));
            s.spawn(move || {
                let mut scratch = WorkerScratch::new(
                    lookups.campaigns.len(),
                    lookups.categories.len(),
                    ctx.canon.len(),
                );
                let mut ps = PartitionScratch::<K>::new();
                let mut acc = CampaignAcc::new(lookups);
                let mut writer = UserDailyWriter::new(path);
                let result = (|| -> io::Result<()> {
                    loop {
                        let wi = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&pp) = work.get(wi) else {
                            break;
                        };
                        let ev = ev_slots[pp].lock().unwrap().take().unwrap();
                        let ck = ck_slots[pp].lock().unwrap().take().unwrap();
                        run_partition(ev, ck, ctx, &mut scratch, &mut ps, &mut acc, &mut writer)?;
                    }
                    crate::prof::time(crate::prof::SESS_FINISH, || writer.finish(tid == 0))?;
                    Ok(())
                })();
                match result {
                    Ok(()) => *accs[tid].lock().unwrap() = Some(acc),
                    Err(e) => {
                        err.lock().unwrap().get_or_insert(e);
                    }
                }
            });
        }
    });

    if let Some(e) = err.into_inner().unwrap() {
        return Err(e);
    }
    let mut merged = CampaignAcc::new(lookups);
    for m in accs {
        if let Some(a) = m.into_inner().unwrap() {
            merged.merge_from(a);
        }
    }
    write_campaign_daily(&camp_dir, &lookups.campaigns, &merged.into_rows())
}
