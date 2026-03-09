# Benchmarks

Scripts in this directory benchmark `zq` vs `jq` on NDJSON input and can profile each case.
Native engine (`seq` vs `par`) benchmarks are provided via `cargo bench`.

## Prerequisites

- `jq` must be available in `PATH` only for `BENCH_MODE=full`.
- `zq` release binary is expected at `target/release/zq` (scripts build it automatically by default).
- For profiling:
  - macOS: `sample`
  - Linux: `perf`

## Files

- `cases.tsv`: benchmark case name + jq filter.
- `gen_data_ndjson.sh`: deterministic NDJSON dataset generator.
- `run_stdin_bench.sh`: benchmark table with two modes:
  - `BENCH_MODE=full` (default): verify parity vs `jq`, bench `jq` + `zq`.
  - `BENCH_MODE=zq-only`: bench only `zq`, verify output against saved canonical `jq` hashes from previous full run.
- `profile_each_case.sh`: profiles each benchmark case (`sample` on macOS, `perf` on Linux).
- `benches/native_engine.rs`: native-engine benchmark (`ZQ_NATIVE_PAR=0` vs `1`) via `cargo bench`.
- `benches/parsing.rs`: parse-only benchmark (`serde_json`, `serde_yaml`, `zq auto-detect`, `zq auto-detect native-value path`).

## Quick Start

```bash
# 1) generate dataset
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson

# optional larger input for profiling
ROWS=1200000 bench/gen_data_ndjson.sh .tmp/bench/data_x6.ndjson

# 2) run full benchmark table (jq + zq) and refresh verify hashes
REPEATS=9 bench/run_stdin_bench.sh

# 2b) run zq-only benchmark using hashes from previous full run
BENCH_MODE=zq-only REPEATS=9 bench/run_stdin_bench.sh

# 3) profile every case
bench/profile_each_case.sh

# 4) native engine benchmark (cargo bench), seq vs par
BENCH_ROWS=200000 cargo bench --bench native_engine

# 5) parse-only benchmark (library + zq parser layers)
BENCH_ROWS=60000 cargo bench --bench parsing
```

Outputs are written to `.tmp/bench/`:

- `stdin_results.tsv`
- `stdin_verify_hashes.tsv` (written by full run as canonical `jq` output hashes, consumed by zq-only mode)
- `jq_*.times`, `zq_*.times`
- `jq_*.rss`, `zq_*.rss` (max RSS per run, normalized to KiB)
- `profiles/*`

`stdin_results.tsv` now contains both latency and memory columns:
- `*_median`, `*_mean` for wall time (seconds)
- `*_maxrss_median_kib`, `*_maxrss_mean_kib` for peak RSS
- `zq_vs_jq` and `zq_vs_jq_maxrss` ratios (zq/jq)

## Useful env vars

- `ROWS`: number of generated rows for `gen_data_ndjson.sh` (default `200000`)
- `REPEATS`: benchmark repeats for each case in `run_stdin_bench.sh` (default `9`)
- `BENCH_MODE`: benchmark mode for `run_stdin_bench.sh` (`full` or `zq-only`)
- `ZQ_ONLY=1`: alias for `BENCH_MODE=zq-only`
- `VERIFY_HASHES_FILE`: path to expected output hashes for zq-only verification
- `DATA`: dataset path for benchmark/profile scripts
- `BUILD_RELEASE=0`: skip rebuilding `zq` if binary is already present
- `ZQ_BIN`: custom `zq` binary path
- `OUT_DIR`: profile output directory for `profile_each_case.sh`
- `BENCH_ROWS`: input row count for `cargo bench --bench native_engine` (default `200000`)
- `BENCH_ROWS`: input row count for parsing/native cargo benchmarks
