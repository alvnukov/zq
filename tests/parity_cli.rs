use std::path::PathBuf;
use std::process::{Command, Output};
use std::io::Write;
use std::process::Stdio;

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

fn run_zq_stdin(args: &[&str], stdin_data: &str) -> Output {
    let mut child = Command::new(bin())
        .args(args)
        .current_dir(root())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn zq");
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_data.as_bytes())
            .expect("write stdin");
    }
    child.wait_with_output().expect("wait zq")
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
        "--raw-output0",
        "-R, --raw-input",
        "-s, --slurp",
        "-n, --null-input",
        "-e, --exit-status",
        "--stream",
        "--stream-errors",
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

#[test]
fn parity_accepts_binary_flag_as_noop() {
    let out = run_zq(&["-b", ".global.env", &fixture("valid-values.yaml"), "-r"]);
    assert_ok(&out, "-b compatibility flag");
    assert_eq!(stdout_text(&out).trim(), "dev");
}

#[test]
fn parity_supports_raw_input_and_slurp_modes() {
    let out = run_zq(&[
        "-Rse",
        r#". == "a\nb\nc\n""#,
        &fixture("raw-lines.txt"),
    ]);
    assert_ok(&out, "-Rse");
    assert_eq!(stdout_text(&out).trim(), "true");

    let out = run_zq(&[
        "-Rne",
        r#"[inputs] == ["a","b","c"]"#,
        &fixture("raw-lines.txt"),
    ]);
    assert_ok(&out, "-Rne");
    assert_eq!(stdout_text(&out).trim(), "true");
}

#[test]
fn parity_exit_status_matches_jq_contract() {
    let out = run_zq(&["-en", "false"]);
    assert_eq!(out.status.code(), Some(1), "false should exit 1");

    let out = run_zq(&["-en", "empty"]);
    assert_eq!(out.status.code(), Some(4), "empty should exit 4");

    let out = run_zq(&["-en", "true"]);
    assert_eq!(out.status.code(), Some(0), "true should exit 0");
}

#[test]
fn parity_accepts_debug_dump_disasm_flag() {
    let out = run_zq(&["-n", "--debug-dump-disasm", "1+1"]);
    assert_ok(&out, "--debug-dump-disasm compatibility");
}

#[test]
fn parity_accepts_seq_flag() {
    let out = run_zq(&["-n", "--seq", "1"]);
    assert_ok(&out, "--seq compatibility");
}

#[test]
fn parity_runtime_errors_match_jq_format() {
    let out = run_zq_stdin(&[".a"], "1\n");
    assert_fail(&out, "runtime error format");
    assert_eq!(out.status.code(), Some(5));
    assert_eq!(
        stderr_text(&out).trim(),
        "jq: error (at <stdin>:1): Cannot index number with string \"a\""
    );

    let out = run_zq_stdin(&[".[1]"], "{}\n");
    assert_fail(&out, "index runtime error format");
    assert_eq!(out.status.code(), Some(5));
    assert_eq!(
        stderr_text(&out).trim(),
        "jq: error (at <stdin>:1): Cannot index object with number"
    );
}
