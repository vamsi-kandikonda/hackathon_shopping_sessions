mod clicks;
mod csv_parse;
mod events;
mod hash;
mod io_chunks;
mod links;
mod lookups;
mod output;
mod session;
mod timestamp;

use std::env;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use mimalloc::MiMalloc;

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
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });
    (data_dir, output_dir, ncpu)
}

fn run() -> io::Result<()> {
    let verbose = env::var("SS_TIMING").is_ok();
    let t0 = Instant::now();
    let (data_dir, output_dir, ncpu) = resolve_paths();

    // Phase 0: Load manifest, products, campaigns
    let lookups = lookups::Lookups::load(&data_dir)?;
    let t1 = Instant::now();

    // Phase 1: Parse links & resolve transitive connected components
    let (link_lookup, canon_names) = links::load_links(&data_dir, ncpu)?;
    let t2 = Instant::now();

    // Phase 2: Parse events, dedup by event_id (smallest ingest_ts), validate, drop late, route to persons
    let (person_events, active_bloom) =
        events::process_events(&data_dir, &lookups, &link_lookup, ncpu)?;
    let t3 = Instant::now();

    // Phase 3: Parse clicks, validate, filter by active persons, route to persons
    let person_clicks =
        clicks::process_clicks(&data_dir, &lookups, &link_lookup, &active_bloom, ncpu)?;
    drop(link_lookup);
    drop(active_bloom);
    let t4 = Instant::now();

    // Phase 4: Sessionize, credit, assist, aggregate, and stream Parquet outputs in parallel
    session::run_sessions_and_write(
        person_events,
        person_clicks,
        &canon_names,
        &lookups,
        &output_dir,
        ncpu,
    )?;
    let t5 = Instant::now();

    if verbose {
        eprintln!(
            "timing (ncpu={ncpu}, compact={}, comps={}): lookups={:.1}ms links={:.1}ms events={:.1}ms clicks={:.1}ms sessions+write={:.1}ms total={:.1}ms",
            lookups.compact_mode,
            canon_names.len(),
            (t1 - t0).as_secs_f64() * 1000.0,
            (t2 - t1).as_secs_f64() * 1000.0,
            (t3 - t2).as_secs_f64() * 1000.0,
            (t4 - t3).as_secs_f64() * 1000.0,
            (t5 - t4).as_secs_f64() * 1000.0,
            (t5 - t0).as_secs_f64() * 1000.0,
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
