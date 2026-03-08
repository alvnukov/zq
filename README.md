# zq

[![CI](https://github.com/alvnukov/zq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alvnukov/zq/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/alvnukov/zq/main/.github/badges/coverage.svg)](https://github.com/alvnukov/zq/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/tag/alvnukov/zq?sort=semver&label=release)](https://github.com/alvnukov/zq/releases)
[![Homebrew Tap](https://img.shields.io/badge/homebrew-alvnukov%2Ftap-2e7d32?logo=homebrew)](https://github.com/alvnukov/homebrew-tap)
[![License](https://img.shields.io/github/license/alvnukov/zq)](LICENSE)

`zq` is a standalone jq-compatible query engine with a native in-repo runtime.

## Current Capabilities

- Runs jq filters on JSON and YAML input.
- Supports JSON and YAML output (`--output-format json|yaml`).
- Supports jq-style `.test` execution (`--run-tests`).
- Supports semantic diff mode (`--diff`) with `diff|json|jsonl|summary` formats.
- Supports shell completion generation (`zq completion <shell>`).
- Supports jq compatibility args: `--arg`, `--argjson`, `--slurpfile`, `--rawfile`, `--args`, `--jsonargs`.

Compatibility notes:

- Query language is jq only (`yq` language mode is not supported).
- `--seq`, `--stream`, `--stream-errors` are available with partial jq compatibility.
- No `jaq` dependency.

## Install

Homebrew tap:

```bash
brew tap alvnukov/tap
brew install zq
```

Prebuilt binaries and packages:

- [GitHub Releases](https://github.com/alvnukov/zq/releases)

Windows binary from Releases:

- Download `zq-<version>-x86_64-pc-windows-msvc.zip`.
- Unpack and add `zq.exe` to `PATH`.

Build from source:

```bash
cargo build --release --locked
```

## Quick Start

Query mode (default):

```bash
# query YAML
zq '.apps[] | .name' values.yaml

# query JSON
zq '.apps[] | .name' values.json

# stdin + raw strings
cat values.yaml | zq '.global.env' -r

# filter from file
zq -f filter.jq values.yaml
```

Compatibility args:

```bash
# named args
zq -n -c --arg env prod --argjson limit 10 '{env: $env, limit: $limit}'

# positional args
zq -n '$ARGS.positional' --args a b
zq -n '$ARGS.positional' --jsonargs 1 '{"a":2}'
```

## CLI Modes

### Query Mode

Default mode: `zq [OPTIONS] [FILTER] [FILE]`

- `[FILTER]` defaults to `.` when omitted.
- `[FILE]` defaults to stdin (`-`) when omitted.
- `--doc-mode first|all|index` controls YAML document selection.
- `--doc-index` is required when `--doc-mode=index`.

### Diff Mode

Semantic diff mode: `zq --diff [LEFT] RIGHT`

```bash
# file vs file
zq --diff left.yaml right.json

# stdin vs file
cat left.json | zq --diff right.yaml

# machine formats
zq --diff --diff-format json left.yaml right.yaml
zq --diff --diff-format jsonl left.yaml right.yaml
zq --diff --diff-format summary left.yaml right.yaml
```

Diff behavior:

- Exit `0` when inputs are semantically equal.
- Exit `1` when differences are found.
- `diff` format is human-readable and colorized on TTY.
- `-C` forces color, `-M` disables color.

### Run-Tests Mode

jq `.test` runner: `zq --run-tests [FILE ...]`

```bash
zq --run-tests tests/jq.test
zq --run-tests .tmp/jq/tests/jq.test,.tmp/jq/tests/man.test
zq --run-tests .tmp/jq/tests/jq.test --skip 100 --take 50
```

Run-tests behavior:

- Supports repeated `--run-tests` and comma-separated file lists.
- Supports stdin (`--run-tests` without value, or `--run-tests -`).
- Supports `--skip` and `--take`.
- Supports `-L/--library-path`.

### Completion

```bash
# bash
source <(zq completion bash)

# zsh
source <(zq completion zsh)

# fish
zq completion fish | source

# powershell
zq completion powershell | Out-String | Invoke-Expression
```

## Exit Codes

- `0`: success.
- `1`: query with `-e` where last result is `false`/`null`, or `--diff` found differences, or run-tests failed.
- `2`: I/O error (for example missing input file); run-tests skip overflow case.
- `3`: compile error.
- `4`: query with `-e` and no outputs; `halt_error(N)` may return custom code `N`.
- `5`: runtime/usage error.

## Output and Color Notes

- `--output-format yaml` is incompatible with `--raw-output`, `--join-output`, `--raw-output0`, and `--compact-output`.
- `--raw-output0` is incompatible with `--join-output`.
- `--stream` / `--stream-errors` are incompatible with `--raw-input`.
- JSON color output honors:
  - `-C` force color
  - `-M` disable color
  - `NO_COLOR` (disables auto-color)
  - `JQ_COLORS` palette (invalid value prints warning)
  - `ZQ_COLOR_COMPAT=jq171` for legacy compact color behavior

## Full CLI Reference

See [docs/cli.md](docs/cli.md).

## Benchmarks

```bash
# generate deterministic NDJSON benchmark data
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson

# run jq vs zq benchmark table (semantic parity is verified first)
REPEATS=9 bench/run_stdin_bench.sh

# profile each case (sample/perf)
bench/profile_each_case.sh
```

Detailed benchmark docs: [bench/README.md](bench/README.md)

## Code Layout

Core runtime modules are intentionally kept focused, with large test suites split into dedicated
test-only files:

- `src/query_native.rs`: public/native query pipeline and parsing entry points.
- `src/query_native/test_support.rs`: jq fixture compatibility and large test-only helpers.
- `src/native_engine/vm_core/mod.rs`: VM core wiring entry points.
- `src/native_engine/vm_core/tests.rs`: VM core integration tests.
- `src/native_engine/vm_core/vm.rs`: VM execution logic.
- `src/native_engine/vm_core/vm/tests.rs`: VM execution unit tests.
- `src/service.rs`: CLI/service runtime.
- `src/service/tests.rs`: service-level tests.
- `src/engine.rs`: engine-facing API layer.
- `src/engine/tests.rs`: engine API tests.

## Library Usage

Add dependency:

```toml
[dependencies]
zq = { git = "https://github.com/alvnukov/zq", tag = "v<latest-release-tag>" }
```

Minimal embedding example:

```rust
let options = zq::QueryOptions {
    doc_mode: zq::parse_doc_mode("first", None)?,
    library_path: Vec::new(),
};

let out = zq::run_jq(".global.env", input, options)?;
let text = zq::format_output_json_lines(&out, false, true)?;
```

Selected public API:

- `zq::run_jq`
- `zq::run_jq_native`
- `zq::run_jq_stream_with_paths_options_native`
- `zq::try_run_jq_native_stream_json_text_options_native`
- `zq::QueryOptions`
- `zq::DocMode`
- `zq::parse_doc_mode`
- `zq::format_output_json_lines`
- `zq::format_output_yaml_documents_native`
- `zq::format_query_error`

## Core Migration

Architecture and migration contract: [docs/new-core-migration.md](docs/new-core-migration.md)
