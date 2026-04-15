# zq CLI Reference

This document reflects the current CLI behavior of `zq`.

## Synopsis

```text
zq [OPTIONS] [FILTER] [FILE ...]
zq [OPTIONS] -f FILTER_FILE [FILE ...]
zq --diff [LEFT] RIGHT
zq --run-tests [FILE ...]
zq completion <SHELL>
```

## Positional Arguments

- `FILTER`: jq filter expression (defaults to `.` when omitted).
- `FILE`: input path (defaults to stdin `-` when omitted).

## Subcommands

- `completion <SHELL>`: generate shell completion script (`bash`, `zsh`, `fish`, `powershell`, ...).

## Main Options

### Input and Parsing

- `--input-format <auto|json|yaml|toml|csv|xml>`: input parser selection (default: `auto`).
- `--csv-parse-json-cells`: when CSV is used as input, parse JSON literals in cells (objects/arrays/scalars).
- `--doc-mode <first|all|index>`: YAML document selection mode (default: `first`).
- `--doc-index <N>`: required with `--doc-mode=index`.
- XML mapping conventions:
  - attributes map to keys with `@` prefix
  - element text maps to `#text` when mixed with attributes/children
  - repeated sibling tags become arrays
  - XML text is always parsed as string (no implicit bool/number/null coercion)
- `-R, --raw-input`: read raw text lines instead of structured parsing.
- `-s, --slurp`: slurp all inputs into one array (structured mode) or one string (raw mode).
- `-n, --null-input`: run filter with `null` input.
- `--seq`: parse JSON text sequence (`application/json-seq`) (partial jq compatibility).
- `--stream`: emit jq-style stream events (partial jq compatibility).
- `--stream-errors`: like `--stream`, but emit parser errors as values.

### Query and Modules

- `-f, --from-file <FILE>`: read filter from file.
- `-L, --library-path <DIR>`: add module search path (repeatable).

### Output

- `--output-format <json|yaml|toml|csv|xml>`: output format (default: `json`).
- `--yaml-anchors`: enable YAML anchors/aliases for repeated values (`--output-format=yaml` only), primarily for repeated structures and larger strings to avoid noisy output.
- `--yaml-anchor-name-mode <friendly|strict-friendly>`: anchor naming mode (`strict-friendly` requires `--yaml-anchors`).
  - naming quality depends on dictionary assets in `assets/yaml_anchor/*.zst`, embedded into the binary at build time.
- `-c, --compact-output` / `--compact`: compact JSON output.
- `--indent <0..7>`: pretty JSON indent size.
- `-r, --raw-output`: print strings without JSON quoting.
- `-j, --join-output`: like `-r`, but without trailing newline per result.
- `--raw-output0`: like `-r`, but NUL (`\0`) delimiter.
- `-C, --color-output`: force color output.
- `-M, --monochrome-output`: disable color output.
- `-e, --exit-status`: jq-compatible exit status based on last result.

### Other Modes

- `--diff`: semantic diff mode (compare JSON/YAML structures).
- `--diff-format <diff|patch|json|jsonl|summary>`: diff output format.
- `--run-tests [FILE ...]`: run jq `.test` files.
- `--skip <N>`: skip first `N` tests in run-tests mode.
- `--take <N>`: run only `N` tests in run-tests mode.

## jq Compatibility Args

`zq` accepts jq-style arg flags:

- `--arg name value`
- `--argjson name value`
- `--slurpfile name file`
- `--rawfile name file`
- `--args`
- `--jsonargs`

Examples:

```bash
zq -n -c --arg env prod --argjson limit 10 '{env: $env, limit: $limit}'
zq -n '$ARGS.positional' --args a b
zq -n '$ARGS.positional' --jsonargs 1 '{"a":2}'
```

## Diff Mode

Usage patterns:

```bash
# LEFT vs RIGHT files
zq --diff left.yaml right.json

# stdin LEFT vs file RIGHT
cat left.json | zq --diff right.yaml
```

Behavior:

- Parses both sides as JSON/YAML/TOML/CSV/XML (auto by content/path or forced with `--input-format`).
- Reports semantic differences by path.
- Exit `0` if equal, `1` if different.
- Supports output formats:
  - `diff`: human-readable (`+` added, `-` removed, `~` changed)
  - `patch`: unified patch-style (`---/+++`, `@@ path @@`, `-` removed, `+` added)
  - `json`: single JSON payload (`equal`, `summary`, `differences`)
  - `jsonl`: stream of diff events + final summary event
  - `summary`: single line `equal=... total=... changed=... added=... removed=...`

Restrictions:

- Cannot combine with `--run-tests`.
- Cannot combine with `-f/--from-file`.
- Cannot read both sides from stdin (`--diff - -` is rejected).

## Run-Tests Mode

Behavior:

- jq `.test` compatible execution.
- Accepts repeated `--run-tests` and comma-separated file lists.
- `--run-tests` with no value defaults to stdin (`-`).
- Supports `--skip` and `--take`.
- Automatically resolves `<test-dir>/modules` if `-L` is not provided.

Restrictions:

- Cannot combine with `FILTER`, `FILE`, `-f/--from-file`, or `--input`.

## Flag Compatibility Rules

- Non-JSON output formats (`yaml`, `toml`, `csv`, `xml`) cannot be used with:
  - `--raw-output`
  - `--join-output`
  - `--raw-output0`
  - `--compact-output`
- `--yaml-anchors` can be used only with `--output-format=yaml`.
- `--yaml-anchor-name-mode` can be used only with `--output-format=yaml` and requires `--yaml-anchors`.
- `--raw-output0` cannot be combined with `--join-output`.
- `--stream` / `--stream-errors` cannot be combined with `--raw-input`.

## Exit Codes

- `0`: success.
- `1`: `-e` with last output `false`/`null`, or diff found differences, or tests failed.
- `2`: I/O error (for example missing file); run-tests skip overflow case.
- `3`: compile error.
- `4`: `-e` with no outputs; `halt_error(N)` can return custom `N`.
- `5`: runtime/usage error.

## Environment Variables

- `NO_COLOR`: disables automatic color.
- `JQ_COLORS`: jq-compatible color palette for structured output highlighting (JSON/YAML/TOML).
- `--output-format=csv` is intentionally emitted without ANSI colors to keep CSV valid.
- `--output-format=xml` is intentionally emitted without ANSI colors to keep XML valid.
- `ZQ_COLOR_COMPAT=jq171`: enables legacy jq171 compact color behavior.
- YAML anchor dictionaries are loaded from embedded assets (no runtime directory/env configuration, no temporary files).

## Hidden Compatibility Flags

Accepted but not advertised in `--help`:

- `--input <FILE>`: legacy alias for positional `FILE`.
- `-b, --binary`: compatibility no-op outside Windows CRLF context.
- `--debug-dump-disasm`: emits internal disassembly labels for supported queries.
