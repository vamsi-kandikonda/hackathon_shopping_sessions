mod arena;
mod clicks;
mod csv_parse;
mod events;
mod hash;
mod io_chunks;
mod links;
mod lookups;
mod output;
mod prof;
mod session;
mod timestamp;

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::Instant;

use mimalloc::MiMalloc;

use events::EvKey;
use lookups::Lookups;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn resolve_paths() -> (PathBuf, PathBuf, usize) {
    let args: Vec<String> = env::args().skip(1).collect();
    let (data_dir, output_dir) = if args.len() >= 2 {
        (
            PathBuf::from(&args[args.len() - 2]),
            PathBuf::from(&args[args.len() - 1]),
        )
    } else {
        (
            PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string())),
            PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string())),
        )
    };
    let ncpu = env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    (data_dir, output_dir, ncpu)
}

struct Timer {
    on: bool,
    last: Instant,
    last_cpu: f64,
    last_sys: f64,
    last_minflt: i64,
}

fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

fn fault_counts() -> (i64, i64, f64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let sys = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6;
    (ru.ru_majflt as i64, ru.ru_minflt as i64, sys)
}

impl Timer {
    fn lap(&mut self, label: &str) {
        if self.on {
            let now = Instant::now();
            let cpu = cpu_seconds();
            let (_, minf, sys) = fault_counts();
            eprintln!(
                "  {label:<22} {:>8.1} ms wall {:>8.1} ms cpu {:>8.1} ms sys {:>8} minflt",
                (now - self.last).as_secs_f64() * 1000.0,
                (cpu - self.last_cpu) * 1000.0,
                (sys - self.last_sys) * 1000.0,
                minf - self.last_minflt
            );
            self.last = now;
            self.last_cpu = cpu;
            self.last_sys = sys;
            self.last_minflt = minf;
        }
    }
}

fn pipeline<K: EvKey>(
    data_dir: &Path,
    output_dir: &Path,
    lookups: &Lookups,
    ncpu: usize,
    timer: &mut Timer,
) -> io::Result<()> {
    // SS_SERIAL=1 runs every phase alone, for honest per-phase CPU numbers.
    if env::var("SS_SERIAL").is_ok() {
        let (link_lookup, canon) = links::load_links(data_dir, ncpu)?;
        timer.lap("links");
        let parsed = events::parse_events::<K>(data_dir, lookups, ncpu)?;
        timer.lap("event parse");
        let mut person_events = events::route_events::<K>(parsed, &link_lookup, ncpu);
        timer.lap("event route");
        let spare = person_events.take_spare();
        let person_clicks = clicks::process_clicks(data_dir, lookups, &link_lookup, ncpu, spare)?;
        timer.lap("clicks");
        drop(link_lookup);
        session::run_sessions_and_write::<K>(person_events, person_clicks, &canon, lookups, output_dir, ncpu)?;
        timer.lap("sessions + write");
        return Ok(());
    }
    // Phase 1 + 2a: links load while events parse; parsing needs no link data.
    let t_overlap = Instant::now();
    let (links, parsed) = thread::scope(|s| {
        let h = s.spawn(|| {
            let r = links::load_links(data_dir, ncpu);
            (r, t_overlap.elapsed())
        });
        let parsed = events::parse_events::<K>(data_dir, lookups, ncpu);
        let parse_done = t_overlap.elapsed();
        let (r, links_done) = h.join().expect("links thread panicked");
        if timer.on {
            eprintln!(
                "  (overlap: links done at {:.1} ms, parse done at {:.1} ms)",
                links_done.as_secs_f64() * 1000.0,
                parse_done.as_secs_f64() * 1000.0
            );
        }
        (r, parsed)
    });
    let (link_lookup, canon) = links?;
    let parsed = parsed?;
    timer.lap("links + event parse");

    // Phase 2b, then 3: run alone. Overlapping these two on 8 vCPUs costs
    // more CPU than it hides.
    let mut person_events = events::route_events::<K>(parsed, &link_lookup, ncpu);
    timer.lap("event route");
    // Clicks write into parse memory that routing no longer needs.
    let spare = person_events.take_spare();
    let person_clicks = clicks::process_clicks(data_dir, lookups, &link_lookup, ncpu, spare)?;
    drop(link_lookup);
    timer.lap("clicks");

    // Phase 4: sessions, attribution and output.
    session::run_sessions_and_write::<K>(
        person_events,
        person_clicks,
        &canon,
        lookups,
        output_dir,
        ncpu,
    )?;
    timer.lap("sessions + write");
    Ok(())
}

fn run() -> io::Result<()> {
    let t0 = Instant::now();
    let mut timer = Timer {
        on: env::var("SS_TIMING").is_ok(),
        last: t0,
        last_cpu: cpu_seconds(),
        last_sys: fault_counts().2,
        last_minflt: fault_counts().1,
    };
    prof::ENABLED.store(timer.on, std::sync::atomic::Ordering::Relaxed);
    let (data_dir, output_dir, ncpu) = resolve_paths();
    let lookups = Lookups::load(&data_dir)?;
    timer.lap("lookups");

    if lookups.compact_mode {
        pipeline::<u64>(&data_dir, &output_dir, &lookups, ncpu, &mut timer)?;
    } else {
        pipeline::<u128>(&data_dir, &output_dir, &lookups, ncpu, &mut timer)?;
    }
    if timer.on {
        prof::report();
        let (maj, min, sys) = fault_counts();
        eprintln!("  faults major={maj} minor={min} sys={:.0} ms", sys * 1000.0);
        eprintln!(
            "  total (ncpu={ncpu}, compact={}) {:.1} ms",
            lookups.compact_mode,
            t0.elapsed().as_secs_f64() * 1000.0
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("shopping-sessions error: {e}");
            ExitCode::FAILURE
        }
    }
}
