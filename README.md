# zq

`zq` is a standalone jq-compatible query engine extracted from `happ`.

Query language is **jq only**. `yq` language mode is not supported.

Input can be JSON or YAML (auto-detected).

## Engine source layout

`zq` uses a native in-repo query runtime (no `jaq` dependency).

## CLI usage

```bash
# jq query over YAML input
zq '.apps[] | .name' values.yaml

# jq query over JSON input
zq '.apps[] | .name' values.json

# read from stdin
cat values.yaml | zq '.global.env' -r

# run jq-compatible .test suites
zq --run-tests tests/jq.test
```

Output modes:

- `--output-format json` (default)
  - pretty JSON (default)
  - compact JSON with `-c` / `--compact-output`
  - raw string output with `--raw-output`
- `--output-format yaml`
  - YAML output (single doc or multi-doc separated by `---`)

`--run-tests` mode:

- compatible with jq `.test` format (`%%FAIL`, `%%FAIL IGNORE MSG`)
- supports `--skip N` and `--take N`
- supports `-L/--library-path` flag syntax for jq compatibility

## Benchmarks

```bash
# generate deterministic NDJSON benchmark data
bench/gen_data_ndjson.sh .tmp/bench/data.ndjson

# run jq vs zq benchmark table (semantic parity is verified first)
REPEATS=9 bench/run_stdin_bench.sh

# profile every benchmark case (sample/perf)
bench/profile_each_case.sh
```

Benchmark scripts and details: [`bench/README.md`](bench/README.md)

## Library usage

Add dependency:

```toml
[dependencies]
zq = { git = "https://github.com/alvnukov/zq", tag = "v1.0.0" }
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

Public API for integration:

- `zq::run_jq`
- `zq::QueryOptions`
- `zq::DocMode`
- `zq::parse_doc_mode`
- `zq::format_output_json_lines`
- `zq::format_output_yaml_documents`
- `zq::format_query_error`
