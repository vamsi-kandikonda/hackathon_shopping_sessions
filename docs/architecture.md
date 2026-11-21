# Shopping sessions Rust pipeline

This project computes the `user_daily` and `campaign_daily` Parquet reports specified in `rules.md`.

## Pipeline phases

1. Load lookups (`src/lookups.rs`): reads `manifest.json`, `products.csv`, and `campaigns.csv`, sorts categories byte-wise for integer tie-breaking, and builds a `YYYY-MM-` timestamp prefix cache covering the active window.
2. Resolve linked IDs (`src/links.rs`): memory-maps `links/*.csv`, parses in parallel, runs union-find with path compression and rank union across all transitive chains and shared-device components, extracts the byte-wise smallest canonical name per component, and builds a 64-shard read-only lookup table with 10-byte inline keys.
3. Process events (`src/events.rs`): scans `events/*.csv` in 4 MB newline-aligned chunks using SIMD bitmask CSV parsing (AVX2 on `amd64`, NEON on `arm64`), validates timestamps (F1, F2, F3) and prices strictly against ASCII digits, partitions into 128 `event_id` buckets, deduplicates keeping the smallest `ingest_ts` (with invalid tombstones preventing later valid copies from rescuing bad earliest rows), drops late events, routes surviving compact events into 128 person partitions, and populates an active-person Bloom filter.
4. Process clicks (`src/clicks.rs`): parses and validates `clicks/*.csv` in parallel, skips users absent from the active-person Bloom filter, and routes qualifying clicks into the matching 128 person partitions.
5. Sessionize, attribute, and write (`src/session.rs`, `src/output.rs`): schedules the 128 person partitions largest-first (LPT) so whale users and shared-device components start immediately, deduplicates per-person clicks on equal timestamps, runs a two-pointer attribution scan with an `O(1)` sliding-window assist tracker for heavy clickers, and streams `user_daily` Parquet files per worker thread plus a merged `campaign_daily` Parquet file.

## Local commands

- Run unit tests: `cargo test`
- Run all 36 fixtures natively: `make fixtures-native`
- Build and run the `linux/amd64` scorer image: `make build`
- Run all 36 fixtures in Docker: `make fixtures`
- Run and check the 1M-event dev set in Docker: `make run-dev && make check-dev`
- Enable per-phase timing logs: `SS_TIMING=1 ./target/release/shopping-sessions <data_dir> <output_dir>`
