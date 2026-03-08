#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="${BENCH_DIR:-$ROOT/.tmp/bench}"
CASES_FILE="${CASES_FILE:-$ROOT/bench/cases.tsv}"
DATA="${DATA:-$BENCH_DIR/data_x6.ndjson}"
DEFAULT_ZQ_BIN="$ROOT/target/release/zq"
ZQ_BIN="${ZQ_BIN:-$DEFAULT_ZQ_BIN}"
OUT_DIR="${OUT_DIR:-$BENCH_DIR/profiles}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-2}"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  (cd "$ROOT" && cargo build --release -q)
fi

if [[ ! -f "$CASES_FILE" ]]; then
  echo "missing cases file: $CASES_FILE" >&2
  exit 1
fi
if [[ ! -f "$DATA" ]]; then
  echo "missing data file: $DATA" >&2
  echo "hint: DATA=$BENCH_DIR/data.ndjson $ROOT/bench/profile_each_case.sh" >&2
  exit 1
fi
if [[ ! -x "$ZQ_BIN" ]]; then
  echo "missing zq binary: $ZQ_BIN" >&2
  exit 1
fi

if [[ "$ZQ_BIN" == "$DEFAULT_ZQ_BIN" ]]; then
  expected="$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/Cargo.toml" | head -n 1)"
  actual="$("$ZQ_BIN" --version 2>/dev/null | awk '{print $NF}' | head -n 1)"
  if [[ -n "${expected}" && "${actual}" != "${expected}" ]]; then
    echo "stale zq binary at $ZQ_BIN: got ${actual:-unknown}, expected $expected" >&2
    echo "hint: rebuild with BUILD_RELEASE=1 (default) or run cargo build --release" >&2
    exit 1
  fi
fi

mkdir -p "$OUT_DIR"

profile_with_sample() {
  local name="$1"
  local query="$2"
  "$ZQ_BIN" -c -- "$query" < "$DATA" > /dev/null &
  local pid=$!
  sample "$pid" "$SAMPLE_SECONDS" 1 -mayDie -file "$OUT_DIR/${name}.sample.txt" > /dev/null 2>&1 || true
  wait "$pid" || true
}

profile_with_perf() {
  local name="$1"
  local query="$2"
  perf record -F 999 -g --output "$OUT_DIR/${name}.perf.data" -- "$ZQ_BIN" -c -- "$query" < "$DATA" > /dev/null 2>&1 || true
  perf report --stdio --input "$OUT_DIR/${name}.perf.data" > "$OUT_DIR/${name}.perf.txt" 2>/dev/null || true
}

if command -v sample >/dev/null 2>&1; then
  profiler="sample"
elif command -v perf >/dev/null 2>&1; then
  profiler="perf"
else
  echo "no supported profiler found (sample/perf)" >&2
  exit 1
fi

while IFS=$'\t' read -r name query; do
  if [[ -z "${name:-}" ]] || [[ "$name" == \#* ]]; then
    continue
  fi
  echo "[profile] $name" >&2
  if [[ "$profiler" == "sample" ]]; then
    profile_with_sample "$name" "$query"
  else
    profile_with_perf "$name" "$query"
  fi
done < "$CASES_FILE"

echo "profiles written to $OUT_DIR"
