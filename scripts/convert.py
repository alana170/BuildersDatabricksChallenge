#!/usr/bin/env python3
"""Convert the pipeline's compact CSV aggregates to the required Parquet tables."""
import glob
import os
import shutil
from pathlib import Path

NCPU = os.environ.get("NCPU", "8")
os.environ.setdefault("POLARS_MAX_THREADS", NCPU)
os.environ.setdefault("POLARS_TEMP_DIR", os.environ.get("SCRATCH_DIR", "/scratch"))

import polars as pl  # noqa: E402

SCRATCH = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/output"))

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
    user_files = sorted(glob.glob(str(SCRATCH / "user_*.csv")))
    campaign_file = SCRATCH / "campaign_daily.csv"
    user_dir = OUTPUT / "user_daily"
    campaign_dir = OUTPUT / "campaign_daily"
    user_dir.mkdir(parents=True, exist_ok=True)
    campaign_dir.mkdir(parents=True, exist_ok=True)

    write_table(
        user_files,
        USER_SCHEMA,
        user_dir / "part-0.parquet",
        ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
    )
    write_table(
        [str(campaign_file)],
        CAMPAIGN_SCHEMA,
        campaign_dir / "part-0.parquet",
        ["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
    )

    # Intermediate CSVs live only in /scratch and are no longer needed.
    for path in user_files + [str(campaign_file)]:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
