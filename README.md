# zq

`zq` is a standalone jq-compatible query engine extracted from `happ`.

Query language is **jq only**. `yq` language mode is not supported.

Input can be JSON or YAML (auto-detected).

## Engine source layout

The query runtime is integrated into this repository as first-party crates:

- `crates/jaq-core`
- `crates/jaq-std`
- `crates/jaq-json`
- `crates/jaq-fmts`
- `crates/jaq-all`

`zq` uses local `path` dependencies to these crates, so engine behavior can be changed directly in this repo.

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

## Library usage

Add dependency:

```toml
[dependencies]
zq = { git = "https://github.com/alvnukov/zq", tag = "v0.1.0" }
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
