# zq

[![CI](https://github.com/alvnukov/zq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alvnukov/zq/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/alvnukov/zq/main/.github/badges/coverage.svg)](https://github.com/alvnukov/zq/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/tag/alvnukov/zq?sort=semver&label=release)](https://github.com/alvnukov/zq/releases)
[![Homebrew Tap](https://img.shields.io/badge/homebrew-alvnukov%2Ftap-2e7d32?logo=homebrew)](https://github.com/alvnukov/homebrew-tap)
[![License](https://img.shields.io/github/license/alvnukov/zq)](LICENSE)

`zq` is a standalone jq-compatible query engine with a native in-repo runtime.

## Project Standards

- Rust MSRV: `1.94` (see `rust-toolchain.toml`)
- Security policy: [SECURITY.md](SECURITY.md)
- Contributing guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Full docs index: [docs/README.md](docs/README.md)

## Current Capabilities

- Runs jq filters on JSON, YAML, TOML, CSV, and XML input.
- Supports JSON, YAML, TOML, CSV, and XML output (`--output-format json|yaml|toml|csv|xml`).
- Supports explicit input selection (`--input-format auto|json|yaml|toml|csv|xml`).
- Supports jq-style `.test` execution (`--run-tests`).
- Supports semantic diff mode (`--diff`) with `diff|patch|json|jsonl|summary` formats.
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

# query TOML
zq '.service.port' config.toml

# query CSV
zq '.name' users.csv

# query XML
zq '.catalog.book[0].title' books.xml

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

Default mode: `zq [OPTIONS] [FILTER] [FILE ...]`

- `[FILTER]` defaults to `.` when omitted.
- `[FILE ...]` defaults to stdin (`-`) when omitted.
- `--input-format auto|json|yaml|toml|csv|xml` controls input parser selection.
- `--csv-parse-json-cells` makes CSV input parser decode JSON literals inside cells.
- `--doc-mode first|all|index` controls YAML document selection.
- `--doc-index` is required when `--doc-mode=index`.
- `--yaml-anchors` enables YAML anchors/aliases for repeated values (only with `--output-format=yaml`), prioritizing repeated structures and larger strings to keep output readable.
- `--yaml-anchor-name-mode friendly|strict-friendly` controls anchor naming style (strict-friendly requires `--yaml-anchors`).
  - `friendly` keeps more semantic tokens.
  - `strict-friendly` applies stronger shortening and dictionary normalization.
- XML mapping conventions:
  - attributes map to keys with `@` prefix (for example `@id`)
  - element text maps to `#text` when mixed with attributes/children
  - repeated sibling tags become arrays
  - XML text is always parsed as string (no implicit bool/number/null coercion)

### Diff Mode

Semantic diff mode: `zq --diff [LEFT] RIGHT`

```bash
# file vs file
zq --diff left.yaml right.json

# stdin vs file
cat left.json | zq --diff right.yaml

# machine formats
zq --diff --diff-format patch left.yaml right.yaml
zq --diff --diff-format json left.yaml right.yaml
zq --diff --diff-format jsonl left.yaml right.yaml
zq --diff --diff-format summary left.yaml right.yaml
```

Diff behavior:

- Exit `0` when inputs are semantically equal.
- Exit `1` when differences are found.
- Parses JSON/YAML/TOML/CSV/XML (auto or forced by `--input-format`).
- `diff` format is human-readable and colorized on TTY.
- `patch` format is unified patch-style by semantic path.
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

- Non-JSON output formats (`yaml`, `toml`, `csv`, `xml`) are incompatible with `--raw-output`, `--join-output`, `--raw-output0`, and `--compact-output`.
- `--yaml-anchors` is available only with `--output-format=yaml`.
- `--yaml-anchor-name-mode` is available only with `--output-format=yaml` and requires `--yaml-anchors`.
- `--raw-output0` is incompatible with `--join-output`.
- `--stream` / `--stream-errors` are incompatible with `--raw-input`.
- Structured color output (JSON/YAML/TOML on TTY) honors:
  - `-C` force color
  - `-M` disable color
  - `NO_COLOR` (disables auto-color)
- `JQ_COLORS` palette (shared with JSON palette; invalid value prints warning)
- `ZQ_COLOR_COMPAT=jq171` for legacy compact color behavior
- `--output-format csv` is kept plain (no ANSI colors) to preserve valid CSV.
- `--output-format xml` is kept plain (no ANSI colors) to preserve valid XML.

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

let yaml = zq::format_output_yaml_documents_with_options(
    &out,
    zq::YamlFormatOptions::default()
        .with_yaml_anchors(true)
        .with_anchor_name_mode(zq::YamlAnchorNameMode::StrictFriendly)
        .with_anchor_single_token_enrichment(true),
)?;
```

### YAML Anchor Dictionaries Asset (API and CLI)

YAML anchor naming (`--yaml-anchors` / `YamlFormatOptions`) uses dictionary assets from
`assets/yaml_anchor/` at build time:

- `stopwords_common.txt.zst`
- `stopwords_strict.txt.zst`
- `canonical_common.tsv.zst`
- `canonical_strict.tsv.zst`

Recommended asset directory layout:

```text
<asset-root>/yaml_anchor/
  stopwords_common.txt.zst
  stopwords_strict.txt.zst
  canonical_common.tsv.zst
  canonical_strict.tsv.zst
```
These `.zst` assets are embedded into the binary at build time (`include_bytes!`).
At runtime they are decompressed in memory and cached (`OnceLock`) with no temp/intermediate files.

If you change dictionary files, rebuild `zq` so updated assets are embedded into the binary.

How to extend dictionaries:

1. Edit source files in `assets/yaml_anchor/`:
   - `stopwords_*.txt`: one token per line, `#` for comments.
   - `canonical_*.tsv`: `from<TAB>to` mapping per line.
2. Rebuild compressed assets:

```bash
zstd -q -f -22 --ultra assets/yaml_anchor/stopwords_common.txt -o assets/yaml_anchor/stopwords_common.txt.zst
zstd -q -f -22 --ultra assets/yaml_anchor/stopwords_strict.txt -o assets/yaml_anchor/stopwords_strict.txt.zst
zstd -q -f -22 --ultra assets/yaml_anchor/canonical_common.tsv -o assets/yaml_anchor/canonical_common.tsv.zst
zstd -q -f -22 --ultra assets/yaml_anchor/canonical_strict.tsv -o assets/yaml_anchor/canonical_strict.tsv.zst
```

3. Rebuild binary/tests (`cargo build`, `cargo test`) to embed and validate updates.

Anchor naming logic (high level):

1. Path/key tokenization (supports separators, CamelCase, acronym boundaries, alnum boundaries).
2. Stopword filtering (`common` for all modes, `strict` additionally for strict-friendly).
3. Canonical normalization (`common` + strict overrides in strict-friendly).
4. Token cleanup (dedupe, compacting, length limits, stable uniqueness suffixes).
5. Optional strict single-token enrichment (`with_anchor_single_token_enrichment(true)`).

Selected public API:

- `zq::run_jq`
- `zq::run_jq_native`
- `zq::run_jq_stream_with_paths_options_native`
- `zq::try_run_jq_native_stream_json_text_options_native`
- `zq::QueryOptions`
- `zq::DocMode`
- `zq::parse_doc_mode`
- `zq::parse_native_input_values_with_format`
- `zq::NativeInputFormat`
- `zq::format_output_json_lines`
- `zq::format_output_yaml_documents_native`
- `zq::format_output_yaml_documents_native_with_options`
- `zq::YamlFormatOptions`
- `zq::YamlAnchorNameMode`
- `zq::YamlFormatOptions::with_yaml_anchors`
- `zq::YamlFormatOptions::with_anchor_name_mode`
- `zq::YamlFormatOptions::with_anchor_single_token_enrichment`
- `zq::format_query_error`

## Core Migration

Architecture and migration contract: [docs/new-core-migration.md](docs/new-core-migration.md)
