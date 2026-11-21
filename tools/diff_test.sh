#!/bin/sh
# Differential test: the new native pipeline vs the original implementation (commit 4bb39b4,
# which passed all 36 starter fixtures) on tiny adversarial datasets from tools/fuzz.py.
#   tools/diff_test.sh FIRST_SEED LAST_SEED      (env: NCPU, CHUNK = chunk size in bytes)
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-/tmp/difftest}; mkdir -p "$W"
if [ ! -x "$W/ref" ]; then
  git -C "$ROOT" show 4bb39b4:src/pipeline.cpp > "$W/ref.cpp"
  git -C "$ROOT" show 4bb39b4:scripts/convert.py > "$W/ref_convert.py"
  g++ -std=c++20 -O2 -pthread -march=native "$W/ref.cpp" -o "$W/ref"
fi
[ -x "$W/new" ] || g++ -std=c++20 -O2 -pthread -march=native "$ROOT/src/pipeline.cpp" -o "$W/new"
fails=0
for s in $(seq "$1" "$2"); do
  D=$W/d$s; rm -rf "$D" "$W/o$s" "$W/n$s" "$W/scr"; mkdir -p "$W/o$s" "$W/n$s" "$W/scr"
  python3 "$ROOT/tools/fuzz.py" "$s" "$D"
  DATA_DIR=$D OUTPUT_DIR=$W/o$s SCRATCH_DIR=$W/scr NCPU=3 "$W/ref" 2>/dev/null
  DATA_DIR=$D OUTPUT_DIR=$W/o$s SCRATCH_DIR=$W/scr python3 "$W/ref_convert.py" 2>/dev/null
  env DATA_DIR="$D" OUTPUT_DIR="$W/n$s" NCPU="${NCPU:-3}" ${CHUNK:+PIPELINE_CHUNK_BYTES=$CHUNK} "$W/new" 2>/dev/null
  if ! (cd "$ROOT/scripts" && python3 compare.py "$W/o$s" "$W/n$s") >"$W/c$s" 2>&1; then
    echo "DIFF seed $s (see $W/c$s)"; fails=$((fails + 1))
  fi
done
echo "seeds $1..$2: $fails differences"
[ "$fails" -eq 0 ]
