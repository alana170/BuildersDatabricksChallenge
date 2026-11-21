#!/usr/bin/env python3
"""Convert the pipeline's compact CSV aggregates to the required Parquet tables."""
import glob
import os
import shutil
import sys
import time
from pathlib import Path

NCPU = os.environ.get("NCPU", "8")
os.environ.setdefault("POLARS_MAX_THREADS", NCPU)
os.environ.setdefault("POLARS_TEMP_DIR", os.environ.get("SCRATCH_DIR", "/scratch"))

import polars as pl  # noqa: E402

SCRATCH = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/output"))


class PhaseTimer:
    def __init__(self, phase):
        self.phase = phase

    def __enter__(self):
        self.started = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed_ms = (time.perf_counter() - self.started) * 1000
        status = "failed" if exc_type is not None else "completed"
        print(
            f"timing phase={self.phase} elapsed_ms={elapsed_ms:.3f} status={status}",
            file=sys.stderr,
            flush=True,
        )
        return False


USER_SCHEMA = {
    "user_id": pl.String,
    "day": pl.String,
    "sessions": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "top_category": pl.String,
}
CAMPAIGN_SCHEMA = {
    "campaign_id": pl.String,
    "channel": pl.String,
    "day": pl.String,
    "sessions": pl.Int64,
    "purchasing_sessions": pl.Int64,
    "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}


def write_table(paths, schema, output_path, columns):
    if not paths:
        raise RuntimeError(f"no intermediate CSV files for {output_path}")
    lazy = pl.scan_csv(
        paths,
        schema_overrides=schema,
        null_values="",
        infer_schema_length=0,
        low_memory=True,
    )
    lazy = lazy.with_columns(pl.col("day").str.to_date(format="%Y-%m-%d"))
    lazy = lazy.select(columns)
    lazy.sink_parquet(
        output_path,
        compression="uncompressed",
        row_group_size=262_144,
        maintain_order=False,
    )


def main():
    with PhaseTimer("convert_total"):
        with PhaseTimer("discover_outputs_and_prepare_dirs"):
            user_files = sorted(glob.glob(str(SCRATCH / "user_*.csv")))
            campaign_file = SCRATCH / "campaign_daily.csv"
            user_dir = OUTPUT / "user_daily"
            campaign_dir = OUTPUT / "campaign_daily"
            user_dir.mkdir(parents=True, exist_ok=True)
            campaign_dir.mkdir(parents=True, exist_ok=True)

        with PhaseTimer("write_user_parquet"):
            write_table(
                user_files,
                USER_SCHEMA,
                user_dir / "part-0.parquet",
                ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
            )
        with PhaseTimer("write_campaign_parquet"):
            write_table(
                [str(campaign_file)],
                CAMPAIGN_SCHEMA,
                campaign_dir / "part-0.parquet",
                ["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
            )

        with PhaseTimer("cleanup_intermediate_csvs"):
            # Intermediate CSVs live only in /scratch and are no longer needed.
            for path in user_files + [str(campaign_file)]:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    main()
