use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
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
        stdin.write_all(stdin_data.as_bytes()).expect("write stdin");
    }
    child.wait_with_output().expect("wait zq")
}

fn run_zq_stdin_env(args: &[&str], stdin_data: &str, envs: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args)
        .current_dir(root())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn zq");
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(stdin_data.as_bytes()).expect("write stdin");
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

fn assert_exit_code(out: &Output, expected: i32, context: &str) {
    assert_eq!(
        out.status.code(),
        Some(expected),
        "{context}\nstdout:\n{}\nstderr:\n{}",
        stdout_text(out),
        stderr_text(out)
    );
}

fn assert_stdout_trim_eq(out: &Output, expected: &str, context: &str) {
    let actual = stdout_text(out);
    assert_eq!(
        actual.trim(),
        expected,
        "{context}\nstdout:\n{actual}\nstderr:\n{}",
        stderr_text(out)
    );
}

fn assert_stderr_contains(out: &Output, needle: &str, context: &str) {
    let stderr = stderr_text(out);
    assert!(
        stderr.contains(needle),
        "{context}\nstderr must contain: {needle}\nactual stderr:\n{stderr}\nstdout:\n{}",
        stdout_text(out)
    );
}

fn assert_stderr_trim_eq(out: &Output, expected: &str, context: &str) {
    let stderr = stderr_text(out);
    assert_eq!(
        stderr.trim(),
        expected,
        "{context}\nactual stderr:\n{stderr}\nstdout:\n{}",
        stdout_text(out)
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
        "-L, --library-path",
        "-f, --from-file",
        "--output-format",
        "--doc-mode",
        "-c, --compact-output",
        "-r, --raw-output",
        "-j, --join-output",
        "--raw-output0",
        "-R, --raw-input",
        "-s, --slurp",
        "-n, --null-input",
        "-e, --exit-status",
        "-C, --color-output",
        "-M, --monochrome-output",
        "--seq",
        "--stream",
        "--stream-errors",
        "--arg name value",
        "--argjson name value",
        "--slurpfile name file",
        "--rawfile name file",
        "--args",
        "--jsonargs",
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
    assert_stdout_trim_eq(&out, "dev", "jq query over yaml input");
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
    assert_stderr_contains(
        &out,
        "--doc-index is required",
        "doc-mode index without doc-index",
    );
}

#[test]
fn parity_doc_mode_rejects_invalid_value() {
    let out = run_zq(&[".", &fixture("valid-values.yaml"), "--doc-mode", "weird"]);
    assert_fail(&out, "invalid doc-mode");
    assert_stderr_contains(&out, "invalid --doc-mode", "invalid doc-mode");
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
    assert_stderr_contains(
        &out,
        "--raw-output is supported only with --output-format=json",
        "yaml + raw-output must fail",
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
    assert_stdout_trim_eq(&out, "dev", "legacy --input");
}

#[test]
fn parity_accepts_binary_flag_as_noop() {
    let out = run_zq(&["-b", ".global.env", &fixture("valid-values.yaml"), "-r"]);
    assert_ok(&out, "-b compatibility flag");
    assert_stdout_trim_eq(&out, "dev", "-b compatibility flag");
}

#[test]
fn parity_supports_raw_input_and_slurp_modes() {
    let out = run_zq(&["-Rse", r#". == "a\nb\nc\n""#, &fixture("raw-lines.txt")]);
    assert_ok(&out, "-Rse");
    assert_stdout_trim_eq(&out, "true", "-Rse");

    let out = run_zq(&[
        "-Rne",
        r#"[inputs] == ["a","b","c"]"#,
        &fixture("raw-lines.txt"),
    ]);
    assert_ok(&out, "-Rne");
    assert_stdout_trim_eq(&out, "true", "-Rne");
}

#[test]
fn parity_exit_status_matches_jq_contract() {
    let out = run_zq(&["-en", "false"]);
    assert_exit_code(&out, 1, "false should exit 1");

    let out = run_zq(&["-en", "empty"]);
    assert_exit_code(&out, 4, "empty should exit 4");

    let out = run_zq(&["-en", "true"]);
    assert_exit_code(&out, 0, "true should exit 0");
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
    assert_exit_code(&out, 5, "runtime error format");
    assert_stderr_trim_eq(
        &out,
        "jq: error (at <stdin>:1): Cannot index number with string \"a\"",
        "runtime error format",
    );

    let out = run_zq_stdin(&[".[1]"], "{}\n");
    assert_fail(&out, "index runtime error format");
    assert_exit_code(&out, 5, "index runtime error format");
    assert_stderr_trim_eq(
        &out,
        "jq: error (at <stdin>:1): Cannot index object with number",
        "index runtime error format",
    );
}

#[test]
fn parity_rejects_incompatible_flags() {
    let out = run_zq(&["-rn", "--raw-output0", "--join-output", "."]);
    assert_fail(&out, "raw-output0 + join-output");
    assert_exit_code(&out, 5, "raw-output0 + join-output");
    assert_stderr_contains(
        &out,
        "--raw-output0 is incompatible with --join-output",
        "raw-output0 + join-output",
    );

    let out = run_zq(&["-R", "--stream", "."]);
    assert_fail(&out, "--stream with --raw-input");
    assert_exit_code(&out, 5, "--stream with --raw-input");
    assert_stderr_contains(
        &out,
        "incompatible with --raw-input",
        "--stream with --raw-input",
    );

    let out = run_zq(&[
        ".global.env",
        &fixture("valid-values.yaml"),
        "--output-format",
        "yaml",
        "--compact-output",
    ]);
    assert_fail(&out, "yaml + compact");
    assert_exit_code(&out, 5, "yaml + compact");
    assert_stderr_contains(
        &out,
        "--compact is supported only with --output-format=json",
        "yaml + compact",
    );
}

#[test]
fn parity_cli_compat_args_modes() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let slurp = td.path().join("slurp.json");
    let raw = td.path().join("raw.txt");
    std::fs::write(&slurp, "1\n2\n").expect("write slurpfile");
    std::fs::write(&raw, "ab\ncd\n").expect("write rawfile");
    let slurp_s = slurp.to_string_lossy().into_owned();
    let raw_s = raw.to_string_lossy().into_owned();

    let out = run_zq(&[
        "-n",
        "-c",
        "--arg",
        "foo",
        "x",
        "--argjson",
        "bar",
        "2",
        "{$foo, $bar}",
    ]);
    assert_ok(&out, "arg + argjson");
    assert_stdout_trim_eq(&out, "{\"foo\":\"x\",\"bar\":2}", "arg + argjson");

    let out = run_zq(&[
        "-n",
        "-c",
        "--slurpfile",
        "foo",
        &slurp_s,
        "--rawfile",
        "bar",
        &raw_s,
        "{$foo, $bar}",
    ]);
    assert_ok(&out, "slurpfile + rawfile");
    assert_stdout_trim_eq(
        &out,
        "{\"foo\":[1,2],\"bar\":\"ab\\ncd\\n\"}",
        "slurpfile + rawfile",
    );

    let out = run_zq(&["-n", "-c", "$ARGS.positional", "--args", "a", "b"]);
    assert_ok(&out, "--args positional");
    assert_stdout_trim_eq(&out, "[\"a\",\"b\"]", "--args positional");

    let out = run_zq(&[
        "-n",
        "-c",
        "$ARGS.positional",
        "--jsonargs",
        "1",
        "{\"a\":2}",
    ]);
    assert_ok(&out, "--jsonargs positional");
    assert_stdout_trim_eq(&out, "[1,{\"a\":2}]", "--jsonargs positional");
}

#[test]
fn parity_halt_and_compile_error_exit_contract() {
    let out = run_zq_stdin(&["\"abc\"|halt_error(4)"], "null\n");
    assert_fail(&out, "halt_error status");
    assert_exit_code(&out, 4, "halt_error status");
    assert_eq!(stderr_text(&out), "abc", "halt_error payload");

    let out = run_zq(&["if"]);
    assert_fail(&out, "compile error exit");
    assert_exit_code(&out, 3, "compile error exit");
    assert_stderr_contains(&out, "jq: 1 compile error", "compile error exit");
}

#[test]
fn parity_stream_errors_and_color_flags_contract() {
    let out = run_zq_stdin(&["--stream-errors", "-c", "."], "{\"a\":1");
    assert_ok(&out, "--stream-errors output");
    assert_stdout_trim_eq(
        &out,
        "[\"Unfinished JSON term at EOF at line 1, column 6\",[\"a\"]]",
        "--stream-errors output",
    );

    let out = run_zq_stdin_env(&["-C", "."], "{\"a\":1}\n", &[("JQ_COLORS", "invalid")]);
    assert_ok(&out, "-C invalid JQ_COLORS");
    assert_stderr_contains(&out, "Failed to set $JQ_COLORS", "-C invalid JQ_COLORS");
}

#[test]
fn parity_missing_input_file_exits_with_io_code() {
    let missing = root().join("tests/parity/fixtures/__missing_input__.json");
    let missing_s = missing.to_string_lossy().into_owned();
    let out = run_zq(&[".", &missing_s]);
    assert_fail(&out, "missing input file");
    assert_exit_code(&out, 2, "missing input file");
    assert_stderr_contains(&out, "jq: error:", "missing input file");
}
