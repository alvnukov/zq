# Architecture

## High-level modules

- `src/main.rs`: CLI entrypoint
- `src/cli.rs`: argument parsing and mode wiring
- `src/service/*`: I/O orchestration, output formatting, diff mode, run-tests mode
- `src/query_native.rs`: parser/runner facade for native engine and format adapters
- `src/native_engine/*`: jq-compatible runtime (parser, IR, VM)
- `src/value.rs`: internal value model
- `src/yamlmerge.rs`: YAML merge-key behavior

## Runtime flow

1. Resolve CLI mode and options.
2. Parse input with explicit or auto-detected format.
3. Compile query and execute through native VM.
4. Render output in requested format and color mode.
5. Return stable exit code contract.

## Design constraints

- Reliability first: no silent behavior changes.
- Compatibility first for documented jq semantics.
- Streaming where possible; avoid unnecessary full buffering.
- Fast path optimizations must preserve output determinism.
