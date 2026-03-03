# zq

[![CI](https://github.com/alvnukov/zq/actions/workflows/ci.yml/badge.svg)](https://github.com/alvnukov/zq/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/alvnukov/zq)](https://codecov.io/gh/alvnukov/zq)
[![Release](https://img.shields.io/github/v/release/alvnukov/zq?sort=semver)](https://github.com/alvnukov/zq/releases)
[![Homebrew](https://img.shields.io/homebrew/v/alvnukov/tap/zq?label=homebrew)](https://github.com/alvnukov/homebrew-tap)
[![License](https://img.shields.io/github/license/alvnukov/zq)](LICENSE)

`zq` is a standalone jq-compatible query engine with native in-repo runtime.

- Query language is jq only (`yq` language mode is not supported).
- Input format can be JSON or YAML (auto-detected).
- No `jaq` dependency.

## Install

Homebrew tap:

```bash
brew tap alvnukov/tap
brew install zq
```

Prebuilt binaries and packages:

- [GitHub Releases](https://github.com/alvnukov/zq/releases)

Build from source:

```bash
cargo build --release --locked
```

## CLI

jq query mode:

```bash
# jq query over YAML input
zq '.apps[] | .name' values.yaml

# jq query over JSON input
zq '.apps[] | .name' values.json

# read from stdin
cat values.yaml | zq '.global.env' -r
```

Semantic diff mode (`--diff`, dyff-like):

```bash
# compare two files (JSON/YAML)
zq --diff left.yaml right.json

# compare stdin vs file
cat left.json | zq --diff right.yaml
```

`--diff` contract:

- semantic compare for JSON and YAML
- path-based report (`+` added, `-` removed, `~` changed)
- exit code `0` when equal, `1` when different
- mode does not use jq FILTER execution

jq `.test` run-tests mode:

```bash
zq --run-tests tests/jq.test
zq --run-tests .tmp/jq/tests/jq.test,.tmp/jq/tests/man.test
zq --run-tests .tmp/jq/tests/jq.test --skip 100 --take 50
```

`--run-tests` contract:

- compatible with jq `.test` format (`%%FAIL`, `%%FAIL IGNORE MSG`)
- supports multiple files (repeated flag or comma list)
- supports `--skip N` and `--take N`
- supports `-L/--library-path`

Output modes:

- `--output-format json` (default)
- `--output-format yaml`
- `-c` / `--compact-output` for compact JSON
- `-r` / `--raw-output` for raw strings

## Benchmarks

```bash
# generate deterministic NDJSON benchmark data
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson

# run jq vs zq benchmark table (semantic parity is verified first)
REPEATS=9 bench/run_stdin_bench.sh

# profile every benchmark case (sample/perf)
bench/profile_each_case.sh
```

Benchmark scripts and details: [bench/README.md](bench/README.md)

## Library Usage

Add dependency:

```toml
[dependencies]
zq = { git = "https://github.com/alvnukov/zq", tag = "v1.1.0" }
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
- `zq::QueryOptions`
- `zq::DocMode`
- `zq::parse_doc_mode`
- `zq::jsonish_equal`
- `zq::format_output_json_lines`
- `zq::format_output_yaml_documents`
- `zq::format_query_error`
