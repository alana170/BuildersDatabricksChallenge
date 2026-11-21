# Shopping Sessions — partitioned C++ pipeline

This repository contains a purpose-built C++20 event pipeline and a small Polars Parquet writer.

## Design

1. Parse the link files first and build connected components with Union-Find, path compression, and union-by-size. Each component tracks the byte-wise smallest ID separately from its DSU root.
2. Parse CSV using the challenge's exact dialect. Event IDs are deduplicated by earliest `ingest_ts` before validation. Click IDs are deduplicated before validation.
3. Store event/click IDs as views into memory-mapped source files during deduplication, avoiding a heap allocation per CSV field.
4. Convert validated event and click user IDs to their canonical person IDs.
5. Hash-partition people into 64 independent buckets. Sort events by `(person, ts)` and clicks by `(person, ts, click_id)` within each bucket, in parallel.
6. Process each person's sorted timeline once. Sessionization and rolling seven-day campaign attribution run together. A per-person sliding click queue maintains campaign frequencies and the latest click; active campaigns are emitted once per session.
7. Emit user/day aggregates as each person finishes. Campaign/day aggregates are small fixed-size arrays merged across partitions. No session table or temporal range join is materialized.
8. Polars converts the intermediate CSV aggregates to Parquet with `day` as a DATE.

The code uses the generator's stated category count (20 plus `unknown`) and a maximum 32-day manifest window. The supplied challenge generator uses 14 days.

## Build and run

```bash
docker build --platform linux/amd64 -t shopping-sessions .
docker run --rm --network none \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  -e NCPU=8 -e MEM_GB=24 \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" -v "$PWD/scratch:/scratch" \
  shopping-sessions
```

The image installs Polars at build time. The runtime needs no network. All intermediate files are written to `/scratch`; the only final data files are Parquet files under `/output/user_daily/` and `/output/campaign_daily/`.

Each run writes per-phase timings to stderr in `timing phase=<name> elapsed_ms=<milliseconds> status=<completed|failed>` format. The C++ pipeline reports manifest setup, lookups, links, event and click processing, partitioning, bucket processing, campaign aggregation, and total runtime. The converter separately reports output preparation, each Parquet write, cleanup, and total conversion time. Capture the measurements while running the image with:

```bash
scripts/docker_run.sh /path/to/data /path/to/output 2>timings.log
```

## Fixture validation

The C++ core can be run without Docker and without Python packages:

```bash
g++ -std=c++20 -O3 -DNDEBUG -pthread -march=native src/pipeline.cpp -o pipeline
python3 scripts/check_csv_fixtures.py
```

The local fixture checker compares the intermediate CSVs with all 36 hand-written fixture outputs. For the scored image, the usual `scripts/run_fixtures.py` checker can be copied from the Polars starter and run against the Docker image.

## Important validation status

The C++ core has been checked against all 36 supplied fixtures. The container image and Parquet conversion have not been built or timed in this environment (Docker and network package installation are unavailable here). Run the starter's full fixture harness and the 1M dev-set comparison before submitting. No 20M/60M runtime is claimed.
