#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat > "$TMP_DIR/Cargo.toml" <<EOF
[package]
name = "zq_public_api_smoke"
version = "0.1.0"
edition = "2021"

[dependencies]
zq = { path = "$ROOT" }
EOF

mkdir -p "$TMP_DIR/src"
cat > "$TMP_DIR/src/main.rs" <<'EOF'
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = r#"{"global":{"env":"prod"}}"#;
    let options = zq::QueryOptions {
        doc_mode: zq::parse_doc_mode("first", None)?,
        library_path: Vec::new(),
    };

    let out = zq::run_jq(".global.env", input, options)?;
    let _json = zq::format_output_json_lines(&out, false, true)?;
    let _yaml = zq::format_output_yaml_documents(&out)?;
    Ok(())
}
EOF

cargo check --manifest-path "$TMP_DIR/Cargo.toml" -q
echo "public API smoke check passed"
