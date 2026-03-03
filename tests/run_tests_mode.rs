use std::fs;
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

#[test]
fn run_tests_mode_basic_pass() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("basic.test");
    fs::write(&tf, ".\nnull\nnull\n\n.[0]\n[1,2]\n1\n\n").expect("write test file");

    let out = run_zq(&["--run-tests", tf.to_str().expect("path")]);
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(&out),
        stderr_text(&out)
    );
    assert!(
        stdout_text(&out).contains("2 of 2 tests passed"),
        "stdout:\n{}",
        stdout_text(&out)
    );
}

#[test]
fn run_tests_mode_supports_fail_ignore_msg() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("fail-ignore.test");
    fs::write(&tf, "%%FAIL IGNORE MSG\n@\nplaceholder\n").expect("write test file");

    let out = run_zq(&["--run-tests", tf.to_str().expect("path")]);
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(&out),
        stderr_text(&out)
    );
    assert!(
        stdout_text(&out).contains("1 of 1 tests passed"),
        "stdout:\n{}",
        stdout_text(&out)
    );
}

#[test]
fn run_tests_mode_skip_and_take() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf = td.path().join("skip-take.test");
    fs::write(&tf, ".\nnull\nnull\n\n.\n1\n1\n\n.\n2\n2\n\n").expect("write test file");

    let out = run_zq(&[
        "--run-tests",
        tf.to_str().expect("path"),
        "--skip",
        "1",
        "--take",
        "1",
    ]);
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(&out),
        stderr_text(&out)
    );
    let text = stdout_text(&out);
    assert!(text.contains("Skipped 1 tests"), "stdout:\n{text}");
    assert!(text.contains("1 of 1 tests passed"), "stdout:\n{text}");
}

#[test]
fn run_tests_mode_multiple_files_repeated_flag() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf1 = td.path().join("a.test");
    let tf2 = td.path().join("b.test");
    fs::write(&tf1, ".\nnull\nnull\n\n").expect("write a.test");
    fs::write(&tf2, ".\n1\n1\n\n").expect("write b.test");

    let out = run_zq(&[
        "--run-tests",
        tf1.to_str().expect("path a"),
        "--run-tests",
        tf2.to_str().expect("path b"),
    ]);
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(&out),
        stderr_text(&out)
    );

    let text = stdout_text(&out);
    assert!(text.contains("== run-tests [1/2]:"), "stdout:\n{text}");
    assert!(text.contains("== run-tests [2/2]:"), "stdout:\n{text}");
    assert_eq!(
        text.matches("1 of 1 tests passed").count(),
        2,
        "stdout:\n{text}"
    );
}

#[test]
fn run_tests_mode_multiple_files_comma_list() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let tf1 = td.path().join("a.test");
    let tf2 = td.path().join("b.test");
    fs::write(&tf1, ".\nnull\nnull\n\n").expect("write a.test");
    fs::write(&tf2, ".\n1\n1\n\n").expect("write b.test");
    let list_arg = format!(
        "{},{}",
        tf1.to_str().expect("path a"),
        tf2.to_str().expect("path b")
    );

    let out = run_zq(&["--run-tests", &list_arg]);
    assert!(
        out.status.success(),
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        stdout_text(&out),
        stderr_text(&out)
    );
    let text = stdout_text(&out);
    assert_eq!(
        text.matches("1 of 1 tests passed").count(),
        2,
        "stdout:\n{text}"
    );
}
