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
BENCH_MODE="${BENCH_MODE:-full}" # full|zq-only
ZQ_ONLY="${ZQ_ONLY:-0}"          # deprecated alias for BENCH_MODE=zq-only
VERIFY_HASHES_FILE="${VERIFY_HASHES_FILE:-$BENCH_DIR/stdin_verify_hashes.tsv}"

if [[ "$ZQ_ONLY" == "1" ]]; then
  BENCH_MODE="zq-only"
fi

if [[ "$BENCH_MODE" != "full" && "$BENCH_MODE" != "zq-only" ]]; then
  echo "invalid BENCH_MODE: $BENCH_MODE (expected full|zq-only)" >&2
  exit 1
fi

if [[ "$BUILD_RELEASE" == "1" ]]; then
  (cd "$ROOT" && cargo build --release -q)
fi

if [[ "$BENCH_MODE" == "full" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    echo "missing jq in PATH (required in BENCH_MODE=full)" >&2
    exit 1
  fi
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
if [[ "$BENCH_MODE" == "zq-only" && ! -f "$VERIFY_HASHES_FILE" ]]; then
  echo "missing verify hashes file: $VERIFY_HASHES_FILE" >&2
  echo "hint: run BENCH_MODE=full once to generate it" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1 && ! command -v jq >/dev/null 2>&1; then
  echo "missing canonicalizer: need python3 or jq in PATH" >&2
  exit 1
fi

hash_stream() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    sha256sum | awk '{print $1}'
  fi
}

canonicalize_json_stream() {
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import json,sys
for line in sys.stdin:
    line=line.strip()
    if not line:
        continue
    value=json.loads(line)
    sys.stdout.write(json.dumps(value, sort_keys=True, separators=(",",":")) + "\n")'
  else
    jq -cS .
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

lookup_expected_hash() {
  local case_name="$1"
  awk -F $'\t' -v c="$case_name" 'NR>1 && $1==c { print $2; exit }' "$VERIFY_HASHES_FILE"
}

validate_verify_hashes_file() {
  local header
  header="$(head -n 1 "$VERIFY_HASHES_FILE" || true)"
  if [[ "$header" != $'case\tjq_canonical_hash' ]]; then
    echo "invalid verify hashes file format: $VERIFY_HASHES_FILE" >&2
    echo "expected header: case<TAB>jq_canonical_hash" >&2
    echo "hint: regenerate with BENCH_MODE=full" >&2
    exit 1
  fi
}

is_number() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

mkdir -p "$BENCH_DIR"
: > "$RESULTS_FILE"
printf 'case\tjq_median\tjq_mean\tzq_median\tzq_mean\tzq_vs_jq\n' > "$RESULTS_FILE"
if [[ "$BENCH_MODE" == "full" ]]; then
  printf 'case\tjq_canonical_hash\n' > "$VERIFY_HASHES_FILE"
else
  validate_verify_hashes_file
fi

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
  if [[ "$BENCH_MODE" == "full" ]]; then
    jq_hash="$(run_engine jq "$query" | canonicalize_json_stream | hash_stream)"
    zq_hash="$(run_engine zq "$query" | canonicalize_json_stream | hash_stream)"
    if [[ "$jq_hash" != "$zq_hash" ]]; then
      echo "mismatch for $name" >&2
      echo "query: $query" >&2
      echo "jq_hash=$jq_hash" >&2
      echo "zq_hash=$zq_hash" >&2
      exit 2
    fi
    printf '%s\t%s\n' "$name" "$jq_hash" >> "$VERIFY_HASHES_FILE"
  else
    expected_hash="$(lookup_expected_hash "$name")"
    if [[ -z "${expected_hash:-}" ]]; then
      echo "missing expected hash for case $name in $VERIFY_HASHES_FILE" >&2
      exit 2
    fi
    zq_hash="$(run_engine zq "$query" | canonicalize_json_stream | hash_stream)"
    if [[ "$zq_hash" != "$expected_hash" ]]; then
      echo "mismatch for $name (zq-only verification)" >&2
      echo "query: $query" >&2
      echo "expected_hash=$expected_hash" >&2
      echo "actual_hash=$zq_hash" >&2
      exit 2
    fi
  fi

  # Warmup (unmeasured).
  if [[ "$BENCH_MODE" == "full" ]]; then
    run_engine jq "$query" > /dev/null
  fi
  run_engine zq "$query" > /dev/null

  zq_t="$BENCH_DIR/zq_${name}.times"
  : > "$zq_t"

  jq_med="NA"
  jq_mean="NA"
  if [[ "$BENCH_MODE" == "full" ]]; then
    jq_t="$BENCH_DIR/jq_${name}.times"
    : > "$jq_t"
    echo "[bench] $name jq" >&2
    for _ in $(seq 1 "$REPEATS"); do
      t=$({ time run_engine jq "$query" > /dev/null; } 2>&1)
      printf '%s\n' "$t" >> "$jq_t"
    done
    read -r jq_med jq_mean < <(calc_stats "$jq_t")
  fi

  echo "[bench] $name zq" >&2
  for _ in $(seq 1 "$REPEATS"); do
    t=$({ time run_engine zq "$query" > /dev/null; } 2>&1)
    printf '%s\n' "$t" >> "$zq_t"
  done

  read -r zq_med zq_mean < <(calc_stats "$zq_t")
  ratio="NA"
  if is_number "$jq_med"; then
    ratio=$(awk -v z="$zq_med" -v j="$jq_med" 'BEGIN { if (j==0) print "inf"; else printf "%.3f", z/j }')
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$jq_med" "$jq_mean" "$zq_med" "$zq_mean" "$ratio" >> "$RESULTS_FILE"
done < "$CASES_FILE"

cat "$RESULTS_FILE"
if [[ "$BENCH_MODE" == "full" ]]; then
  echo "verification hashes written to $VERIFY_HASHES_FILE" >&2
fi
