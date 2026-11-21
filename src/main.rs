#![allow(dead_code)]

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
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
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
        let person_events = events::route_events::<K>(parsed, &link_lookup, ncpu);
        timer.lap("event route");
        let person_clicks = clicks::process_clicks(data_dir, lookups, &link_lookup, ncpu)?;
        timer.lap("clicks");
        drop(link_lookup);
        session::run_sessions_and_write::<K>(person_events, person_clicks, &canon, lookups, output_dir, ncpu)?;
        timer.lap("sessions + write");
        return Ok(());
    }
    // Phase 1 + 2a: links load while events parse; parsing needs no link data.
    // As soon as links finish, clicks start immediately in the background,
    // overlapping with remaining event parse and event route.
    let t_overlap = Instant::now();
    let timer_on = timer.on;
    let (tx_clicks, rx_clicks) = sync_channel::<io::Result<Arc<links::LinkLookup>>>(1);
    let (tx_main, rx_main) = sync_channel::<io::Result<(Arc<links::LinkLookup>, Arc<links::CanonNames>)>>(1);

    let (person_events, person_clicks, canon) = thread::scope(|s| {
        s.spawn(move || {
            let r = links::load_links(data_dir, ncpu);
            let links_done = t_overlap.elapsed();
            match r {
                Ok((lookup, canon)) => {
                    let lookup = Arc::new(lookup);
                    let canon = Arc::new(canon);
                    let _ = tx_clicks.send(Ok(Arc::clone(&lookup)));
                    let _ = tx_main.send(Ok((lookup, canon)));
                }
                Err(e) => {
                    let e2 = io::Error::new(e.kind(), e.to_string());
                    let _ = tx_clicks.send(Err(e));
                    let _ = tx_main.send(Err(e2));
                }
            }
            if timer_on {
                eprintln!(
                    "  (overlap: links done at {:.1} ms)",
                    links_done.as_secs_f64() * 1000.0
                );
            }
        });

        let h_clicks = s.spawn(move || {
            let link_lookup = rx_clicks.recv().expect("links channel closed")?;
            clicks::process_clicks(data_dir, lookups, &link_lookup, ncpu)
        });

        let parsed = events::parse_events::<K>(data_dir, lookups, ncpu)?;
        if timer.on {
            eprintln!(
                "  (overlap: parse done at {:.1} ms)",
                t_overlap.elapsed().as_secs_f64() * 1000.0
            );
        }
        let (link_lookup, canon) = rx_main.recv().expect("links channel closed")?;
        timer.lap("links + event parse");

        // Phase 2b: route events with all cores.
        let person_events = events::route_events::<K>(parsed, &link_lookup, ncpu);
        timer.lap("event route");

        // Wait for background clicks to finish.
        let person_clicks = h_clicks.join().expect("clicks thread panicked")?;
        drop(link_lookup);
        timer.lap("clicks");

        Ok::<_, io::Error>((person_events, person_clicks, canon))
    })?;

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
