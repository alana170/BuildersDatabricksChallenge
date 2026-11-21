#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}" "${NCPU:=8}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR NCPU
export HOME="$SCRATCH_DIR" TMPDIR="$SCRATCH_DIR" POLARS_TEMP_DIR="$SCRATCH_DIR"
export PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
/app/pipeline
python3 /app/convert.py
