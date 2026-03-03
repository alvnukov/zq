#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="${BENCH_DIR:-$ROOT/.tmp/bench}"
CASES_FILE="${CASES_FILE:-$ROOT/bench/cases.tsv}"
DATA="${DATA:-$BENCH_DIR/data.ndjson}"
RESULTS_FILE="${RESULTS_FILE:-$BENCH_DIR/stdin_results.tsv}"
ZQ_BIN="${ZQ_BIN:-$ROOT/target/release/zq}"
REPEATS="${REPEATS:-9}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  (cd "$ROOT" && cargo build --release -q)
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "missing jq in PATH" >&2
  exit 1
fi
if [[ ! -f "$CASES_FILE" ]]; then
  echo "missing cases file: $CASES_FILE" >&2
  exit 1
fi
if [[ ! -f "$DATA" ]]; then
  echo "missing data file: $DATA" >&2
  echo "generate with: $ROOT/bench/gen_data_ndjson.sh $DATA" >&2
  exit 1
fi
if [[ ! -x "$ZQ_BIN" ]]; then
  echo "missing zq binary: $ZQ_BIN" >&2
  exit 1
fi

hash_stream() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    sha256sum | awk '{print $1}'
  fi
}

calc_stats() {
  local f="$1"
  sort -n "$f" | awk '
    {a[NR]=$1; s+=$1}
    END {
      if (NR==0) exit 1;
      if (NR%2==1) m=a[(NR+1)/2];
      else m=(a[NR/2]+a[NR/2+1])/2;
      printf "%.6f %.6f\n", m, s/NR;
    }
  '
}

run_engine() {
  local engine="$1"
  local query="$2"
  if [[ "$engine" == "jq" ]]; then
    cat "$DATA" | jq -c "$query"
  else
    cat "$DATA" | "$ZQ_BIN" -c -- "$query"
  fi
}

mkdir -p "$BENCH_DIR"
: > "$RESULTS_FILE"
printf 'case\tjq_median\tjq_mean\tzq_median\tzq_mean\tzq_vs_jq\n' > "$RESULTS_FILE"

TIMEFORMAT='%R'
while IFS=$'\t' read -r name query; do
  if [[ -z "${name:-}" ]] || [[ "$name" == \#* ]]; then
    continue
  fi
  if [[ -z "${query:-}" ]]; then
    echo "invalid case without query: $name" >&2
    exit 1
  fi

  echo "[verify] $name" >&2
  jq_hash="$(run_engine jq "$query" | jq -cS . | hash_stream)"
  zq_hash="$(run_engine zq "$query" | jq -cS . | hash_stream)"
  if [[ "$jq_hash" != "$zq_hash" ]]; then
    echo "mismatch for $name" >&2
    echo "query: $query" >&2
    echo "jq_hash=$jq_hash" >&2
    echo "zq_hash=$zq_hash" >&2
    exit 2
  fi

  # Warmup (unmeasured).
  run_engine jq "$query" > /dev/null
  run_engine zq "$query" > /dev/null

  jq_t="$BENCH_DIR/jq_${name}.times"
  zq_t="$BENCH_DIR/zq_${name}.times"
  : > "$jq_t"
  : > "$zq_t"

  echo "[bench] $name jq" >&2
  for _ in $(seq 1 "$REPEATS"); do
    t=$({ time run_engine jq "$query" > /dev/null; } 2>&1)
    printf '%s\n' "$t" >> "$jq_t"
  done

  echo "[bench] $name zq" >&2
  for _ in $(seq 1 "$REPEATS"); do
    t=$({ time run_engine zq "$query" > /dev/null; } 2>&1)
    printf '%s\n' "$t" >> "$zq_t"
  done

  read -r jq_med jq_mean < <(calc_stats "$jq_t")
  read -r zq_med zq_mean < <(calc_stats "$zq_t")
  ratio=$(awk -v z="$zq_med" -v j="$jq_med" 'BEGIN { if (j==0) print "inf"; else printf "%.3f", z/j }')
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$jq_med" "$jq_mean" "$zq_med" "$zq_mean" "$ratio" >> "$RESULTS_FILE"
done < "$CASES_FILE"

cat "$RESULTS_FILE"
