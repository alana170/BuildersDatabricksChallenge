# Shopping Sessions — native C++ pipeline

One static C++20 binary reads the CSVs in `/data` and writes both Parquet tables directly.
The runtime image is `FROM scratch`: no shell, no Python, nothing to start up.

## Why the previous version was slow (14.7 s on the practice data)

Measured on an 8M-event dataset (`tools/gen_dirty.cpp`) with 1 and 2 threads:

| step (previous version)                         | 1 thread | 2 threads |
| ----------------------------------------------- | -------- | --------- |
| event parse + dedup + validate                  | 6.25 s   | 5.57 s    |
| everything else                                 | 2.5 s    | 1.8 s     |

The event step barely sped up with more threads: about 80% of it ran on one thread.

* `count_data_lines` read every input byte on one thread before parsing began.
* Every parsed row took a mutex on a dedup shard, then jumped to a random spot in a ~1 GB table
  (two cache misses per row), so the "parallel" parse ran at the speed of memory latency.
* After dedup, one thread validated all 20M rows and interned each `user_id` in one
  `std::unordered_map` (a cache miss per row).
* Partitioning by person, the click equivalents, and the Polars CSV→Parquet step were also serial.

Scaled to 20M events, that serial work alone is about 12 s.

## Design now

Every stage runs on all cores and no hot loop takes a lock.

1. **Links.** Parallel parse → ID table partitioned by hash → lock-free union-find
   (CAS; a root is only linked under a smaller index, so no cycles) → the byte-wise smallest name
   per component, found by a parallel CAS-min. Each ID-table entry is one 64-byte cache line holding
   the ID and its person's name, so resolving a `user_id` costs one memory access.
2. **Events.** A two-stage AVX2 scanner: stage 1 finds `,` `"` `\n` 64 bytes at a time and writes
   every comma offset to an array, plus, per line end, how many commas and quotes precede it;
   stage 2 walks whole lines (exactly N-1 commas; quotes checked per line by count) and applies the
   dialect rules exactly. Timestamps are checked 16 bytes at a time (F1/F2 by template masks, F3 by
   SWAR digit conversion). Each row is validated on the spot, since validity depends only on the
   row, then written (streaming stores) to a thread-local block list chosen by `hash(event_id)`.
   The link step runs on its own thread(s) meanwhile.
3. **Dedup.** Each of 1024 event-ID partitions is deduplicated with a table that fits in L2:
   the earliest `ingest_ts` wins, and the kept copy's validity decides, as the rules require. Losing
   copies are marked in place, then the partition is read again in storage order; each survivor's
   person is resolved (ID-table lookups prefetched in batches of 16) and it goes to one of 1024
   person partitions chosen by `hash(person name)`.
4. **Clicks.** Parsed, validated, resolved and sent straight to the person partitions. Copies of a
   click have identical values, so a duplicate can change neither the latest click nor the set of
   assisting campaigns: no click dedup is needed.
5. **People.** Each person partition gives every row a local person id (exact name), stably
   radix-sorts all its events and clicks by `ts` (three 11-bit digits), then stably scatters them by
   person, so every person's rows come out in `ts` order with no per-person sort. One sweep per
   person then finds sessions, the last-click credit (among clicks at the latest qualifying `ts`, the
   smallest `click_id`) and the set of campaigns active in the 7-day window for assists.
6. **Output.** A small hand-written Parquet writer (PLAIN, uncompressed, `day` as `DATE`, nullable
   `top_category`). Each worker writes its own `user_daily/part-N.parquet`. Read back with Polars,
   pyarrow and DuckDB.

Names up to 16 bytes travel inline in every record, so grouping and output never re-read the CSV
bytes. Every comparison is exact: hash, then length, then bytes. Row buffers live in huge-page
arenas populated in 4 MB slabs, which avoids roughly a quarter million page faults.

## Results (8M events, 2-CPU VM, same output as before)

| version                        | wall time | CPU time |
| ------------------------------ | --------- | -------- |
| previous (C++ + Polars)        | 7.5 s     | ~8 s     |
| this version                   | ~1.5 s    | ~2.9 s   |

On 8 vCPUs the previous version could not scale (serial work); this one scales with cores.

## Build and run

```bash
docker build --platform linux/amd64 -t shopping-sessions .
scripts/docker_run.sh /path/to/data /path/to/out      # same flags as the scorer
```

Without Docker:

```bash
g++ -std=c++20 -O3 -DNDEBUG -pthread -march=native src/pipeline.cpp -o pipeline
DATA_DIR=/path/to/data OUTPUT_DIR=/path/to/out NCPU=8 ./pipeline
```

Per-phase timings go to stderr (`timing phase=... elapsed_ms=...`).

## Testing

* Starter fixtures: `python3 scripts/run_fixtures.py ../fixtures -- ./pipeline`
* Dev set: `python3 scripts/compare.py dev-answer/ out/`
* Differential fuzzing against the previous implementation (commit `4bb39b4`, which passed all 36
  fixtures): `tools/diff_test.sh 1 300`. `tools/fuzz.py` builds tiny datasets dense in edge cases:
  quoting, `\r`, malformed lines, missing final newline, zero-byte files, all timestamp forms and bad
  forms, Feb 29, duplicates with earlier invalid copies and different users, late events, gaps of
  exactly 30:00.000 and 30:00.001, click boundaries at exactly 7 days, at the session start and
  1 ms off, same-ms ties, link chains and self-links, and assist-only campaigns.
  Set `CHUNK=1` to split every line into its own work chunk.
* Independent reference: `tools/reference.py DATA OUT` is a slow, plain-Python implementation
  written straight from `rules.md` (no code shared with the C++). It agrees with the native binary
  on 1,000 fuzz seeds and on a 1M-event synthetic set:
  `python3 tools/reference.py DATA ref/ && python3 scripts/compare.py ref/ out/`.
* Large synthetic data: `g++ -O2 -std=c++20 tools/gen_dirty.cpp -o gen && ./gen SEED EVENTS CLICKS OUTDIR`
  (follows the generator description and dirt rates in `rules.md`).
