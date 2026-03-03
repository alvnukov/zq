# Benchmarks

Scripts in this directory benchmark `zq` vs `jq` on NDJSON input and can profile each case.

## Prerequisites

- `jq` must be available in `PATH`.
- `zq` release binary is expected at `target/release/zq` (scripts build it automatically by default).
- For profiling:
  - macOS: `sample`
  - Linux: `perf`

## Files

- `cases.tsv`: benchmark case name + jq filter.
- `gen_data_ndjson.sh`: deterministic NDJSON dataset generator.
- `run_stdin_bench.sh`: verifies semantic parity with `jq`, then measures runtime.
- `profile_each_case.sh`: profiles each benchmark case (`sample` on macOS, `perf` on Linux).

## Quick Start

```bash
# 1) generate dataset
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson

# optional larger input for profiling
ROWS=1200000 bench/gen_data_ndjson.sh .tmp/bench/data_x6.ndjson

# 2) run benchmark table
REPEATS=9 bench/run_stdin_bench.sh

# 3) profile every case
bench/profile_each_case.sh
```

Outputs are written to `.tmp/bench/`:

- `stdin_results.tsv`
- `jq_*.times`, `zq_*.times`
- `profiles/*`

## Useful env vars

- `ROWS`: number of generated rows for `gen_data_ndjson.sh` (default `200000`)
- `REPEATS`: benchmark repeats for each case in `run_stdin_bench.sh` (default `9`)
- `DATA`: dataset path for benchmark/profile scripts
- `BUILD_RELEASE=0`: skip rebuilding `zq` if binary is already present
- `ZQ_BIN`: custom `zq` binary path
- `OUT_DIR`: profile output directory for `profile_each_case.sh`
