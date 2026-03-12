use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn run_zq(args: &[&str]) -> Output {
    Command::new(bin()).args(args).output().expect("run zq")
}

fn stdout_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n")
}

fn stderr_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n")
}

fn assert_success(out: &Output) {
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(out),
        stderr_text(out)
    );
}

fn write_test(path: &Path, body: &str) {
    fs::write(path, body).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

fn path_arg(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[test]
fn run_tests_mode_basic_pass() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("basic.test");
    write_test(&tf, ".\nnull\nnull\n\n.[0]\n[1,2]\n1\n\n");

    let tf_arg = path_arg(&tf);
    let out = run_zq(&["--run-tests", &tf_arg]);
    assert_success(&out);
    let text = stdout_text(&out);
    assert!(text.contains("2 of 2 tests passed"), "stdout:\n{text}");
}

#[test]
fn run_tests_mode_supports_fail_ignore_msg() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("fail-ignore.test");
    write_test(&tf, "%%FAIL IGNORE MSG\n@\nplaceholder\n");

    let tf_arg = path_arg(&tf);
    let out = run_zq(&["--run-tests", &tf_arg]);
    assert_success(&out);
    let text = stdout_text(&out);
    assert!(text.contains("1 of 1 tests passed"), "stdout:\n{text}");
}

#[test]
fn run_tests_mode_skip_and_take() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("skip-take.test");
    write_test(&tf, ".\nnull\nnull\n\n.\n1\n1\n\n.\n2\n2\n\n");
    let tf_arg = path_arg(&tf);

    let out = run_zq(&["--run-tests", &tf_arg, "--skip", "1", "--take", "1"]);
    assert_success(&out);
    let text = stdout_text(&out);
    assert!(text.contains("Skipped 1 tests"), "stdout:\n{text}");
    assert!(text.contains("1 of 1 tests passed"), "stdout:\n{text}");
}

#[test]
fn run_tests_mode_multiple_files_repeated_flag() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf1 = td.path().join("a.test");
    let tf2 = td.path().join("b.test");
    write_test(&tf1, ".\nnull\nnull\n\n");
    write_test(&tf2, ".\n1\n1\n\n");
    let tf1_arg = path_arg(&tf1);
    let tf2_arg = path_arg(&tf2);

    let out = run_zq(&["--run-tests", &tf1_arg, "--run-tests", &tf2_arg]);
    assert_success(&out);

    let text = stdout_text(&out);
    assert!(text.contains("== run-tests [1/2]:"), "stdout:\n{text}");
    assert!(text.contains("== run-tests [2/2]:"), "stdout:\n{text}");
    assert_eq!(text.matches("1 of 1 tests passed").count(), 2, "stdout:\n{text}");
}

#[test]
fn run_tests_mode_multiple_files_comma_list() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf1 = td.path().join("a.test");
    let tf2 = td.path().join("b.test");
    write_test(&tf1, ".\nnull\nnull\n\n");
    write_test(&tf2, ".\n1\n1\n\n");
    let list_arg = format!("{},{}", path_arg(&tf1), path_arg(&tf2));

    let out = run_zq(&["--run-tests", &list_arg]);
    assert_success(&out);
    let text = stdout_text(&out);
    assert_eq!(text.matches("1 of 1 tests passed").count(), 2, "stdout:\n{text}");
}

#[test]
fn run_tests_mode_compat_jq_help_suite() {
    let tf = format!("{}/tests/compat/jq_help.test", env!("CARGO_MANIFEST_DIR"));
    let out = run_zq(&["--run-tests", &tf]);
    assert_success(&out);
    let text = stdout_text(&out);
    assert!(text.contains("5 of 5 tests passed"), "stdout:\n{text}");
}
