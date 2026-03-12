# Development

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- `jq` (for compatibility tests and benchmark comparisons)

## Common commands

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features --locked
cargo test --locked --test hardcode_guard --test hardcode_guard_clusters --test jq_oracle_baseline
ZQ_RUN_JQ_UPSTREAM=1 ZQ_JQ_AUTO_CLONE=1 ZQ_JQ_SUITES=jq171 cargo test --test jq_upstream -- --nocapture
```

Notes:
- `ZQ_JQ_SUITES` accepts comma-separated suite names: `jq171` (default), `jq`.
- `ZQ_JQ_SKIP_SHTEST=1` skips upstream `shtest` scripts when you need only `.test` parity.

## Benchmark commands

```bash
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson
REPEATS=9 bench/run_stdin_bench.sh
```

## Testing strategy

- Unit tests for module-level behavior and edge cases
- Integration tests for CLI, API, and format behavior
- Golden/compat tests for jq parity and regression prevention

## Compatibility policy

- Public CLI flags and exit codes are stable within minor releases.
- Public Rust API must remain backward-compatible unless major version bump.
