use std::path::PathBuf;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixtures_dir() -> PathBuf {
    root().join("tests/parity/fixtures")
}

fn fixture(name: &str) -> String {
    fixtures_dir().join(name).to_string_lossy().to_string()
}

fn run_zq(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(root())
        .output()
        .expect("run zq")
}

fn stdout_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n")
}

fn stderr_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n")
}

fn assert_ok(out: &Output, context: &str) {
    assert!(
        out.status.success(),
        "{context}\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(out),
        stderr_text(out)
    );
}

fn assert_fail(out: &Output, context: &str) {
    assert!(
        !out.status.success(),
        "{context}\nexpected failure, got success\nstdout:\n{}\nstderr:\n{}",
        stdout_text(out),
        stderr_text(out)
    );
}

#[test]
fn parity_help_contract() {
    let out = run_zq(&["--help"]);
    assert_ok(&out, "--help");
    let text = stdout_text(&out);
    for token in [
        "FILTER",
        "FILE",
        "--output-format",
        "--doc-mode",
        "-c, --compact-output",
        "-r, --raw-output",
        "json",
        "yaml",
    ] {
        assert!(text.contains(token), "help must include token: {token}");
    }
    assert!(
        !text.contains(" yq "),
        "help must not advertise yq language mode"
    );
}

#[test]
fn parity_jq_query_on_yaml_input_contract() {
    let out = run_zq(&[
        ".global.env",
        &fixture("valid-values.yaml"),
        "--doc-mode",
        "first",
        "--raw-output",
    ]);
    assert_ok(&out, "jq query over yaml input");
    assert_eq!(stdout_text(&out).trim(), "dev");
}

#[test]
fn parity_output_format_yaml_contract() {
    let out = run_zq(&[
        ".global",
        &fixture("valid-values.yaml"),
        "--output-format",
        "yaml",
    ]);
    assert_ok(&out, "yaml output");
    let text = stdout_text(&out);
    assert!(
        text.contains("env: dev"),
        "yaml output must contain env key"
    );
}

#[test]
fn parity_doc_mode_index_requires_doc_index() {
    let out = run_zq(&[".", &fixture("valid-values.yaml"), "--doc-mode", "index"]);
    assert_fail(&out, "doc-mode index without doc-index");
    assert!(
        stderr_text(&out).contains("--doc-index is required"),
        "stderr must mention missing --doc-index"
    );
}

#[test]
fn parity_doc_mode_rejects_invalid_value() {
    let out = run_zq(&[".", &fixture("valid-values.yaml"), "--doc-mode", "weird"]);
    assert_fail(&out, "invalid doc-mode");
    assert!(
        stderr_text(&out).contains("invalid --doc-mode"),
        "stderr must mention invalid doc-mode"
    );
}

#[test]
fn parity_yaml_output_rejects_raw_output_flag() {
    let out = run_zq(&[
        ".global.env",
        &fixture("valid-values.yaml"),
        "--output-format",
        "yaml",
        "--raw-output",
    ]);
    assert_fail(&out, "yaml + raw-output must fail");
    assert!(
        stderr_text(&out).contains("--raw-output is supported only with --output-format=json"),
        "stderr must mention incompatible flags"
    );
}

#[test]
fn parity_supports_legacy_input_flag() {
    let out = run_zq(&[
        ".global.env",
        "--input",
        &fixture("valid-values.yaml"),
        "-r",
    ]);
    assert_ok(&out, "legacy --input");
    assert_eq!(stdout_text(&out).trim(), "dev");
}
