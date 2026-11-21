//! Optional section timers, enabled with `SS_TIMING`. Each section sums the
//! wall time its threads spent inside it, which approximates CPU time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

pub static ENABLED: AtomicBool = AtomicBool::new(false);

pub const NAMES: [&str; 11] = [
    "route.dedup",
    "route.scatter",
    "sess.group",
    "sess.process",
    "sess.finish",
    "links.scan",
    "links.dedup",
    "links.dsu",
    "links.table",
    "clicks.scan",
    "events.scan",
];
pub const ROUTE_DEDUP: usize = 0;
pub const ROUTE_SCATTER: usize = 1;
pub const SESS_GROUP: usize = 2;
pub const SESS_PROCESS: usize = 3;
pub const SESS_FINISH: usize = 4;
pub const LINKS_SCAN: usize = 5;
pub const LINKS_DEDUP: usize = 6;
pub const LINKS_DSU: usize = 7;
pub const LINKS_TABLE: usize = 8;
pub const CLICKS_SCAN: usize = 9;
pub const EVENTS_SCAN: usize = 10;

static NANOS: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];

#[inline(always)]
pub fn time<T>(section: usize, f: impl FnOnce() -> T) -> T {
    if !ENABLED.load(Ordering::Relaxed) {
        return f();
    }
    let t = Instant::now();
    let r = f();
    NANOS[section].fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    r
}

/// Adds the time until drop to `section`.
pub struct Span(usize, Option<Instant>);

impl Span {
    #[inline(always)]
    pub fn new(section: usize) -> Self {
        Span(section, ENABLED.load(Ordering::Relaxed).then(Instant::now))
    }
}

impl Drop for Span {
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(t) = self.1 {
            NANOS[self.0].fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

pub fn report() {
    for (i, name) in NAMES.iter().enumerate() {
        let ns = NANOS[i].load(Ordering::Relaxed);
        if ns > 0 {
            eprintln!("    {name:<20} {:>8.1} ms thread-time", ns as f64 / 1e6);
        }
    }
}
