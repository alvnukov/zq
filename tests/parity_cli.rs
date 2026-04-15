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
    Command::new(bin()).args(args).current_dir(root()).output().expect("run zq")
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
        "--diff",
        "--diff-format",
        "completion",
        "--arg name value",
        "--argjson name value",
        "--slurpfile name file",
        "--rawfile name file",
        "--args",
        "--jsonargs",
        "json",
        "yaml",
        "jsonl",
        "summary",
    ] {
        assert!(text.contains(token), "help must include token: {token}");
    }
    assert!(!text.contains(" yq "), "help must not advertise yq language mode");
}

#[test]
fn parity_completion_bash_contract() {
    let out = run_zq(&["completion", "bash"]);
    assert_ok(&out, "completion bash");
    let text = stdout_text(&out);
    assert!(text.contains("_zq()"), "bash completion must define _zq function");
    assert!(text.contains("complete -F _zq"), "bash completion must register zq completer");
}

#[test]
fn parity_completion_zsh_contract() {
    let out = run_zq(&["completion", "zsh"]);
    assert_ok(&out, "completion zsh");
    let text = stdout_text(&out);
    assert!(text.contains("#compdef zq"), "zsh completion must declare compdef header");
    assert!(text.contains("compdef _zq zq"), "zsh completion must register zq completer");
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
fn parity_type_builtin_on_yaml_repos_uses_jq_names() {
    let yaml = r#"
repos:
  - !!str https://example.com/charts
  - !!map {name: stable, url: https://charts.example.com}
"#;
    let out = run_zq_stdin(&["--input-format", "yaml", "-c", ".repos[] | type"], yaml);
    assert_ok(&out, "yaml type names must match jq");
    assert_stdout_trim_eq(&out, "\"string\"\n\"object\"", "yaml type names must match jq");
}

#[test]
fn parity_jq_query_on_xml_input_contract() {
    let out = run_zq_stdin(
        &["--input-format", "xml", "-r", ".catalog.book.title"],
        "<catalog><book><title>Rust</title></book></catalog>",
    );
    assert_ok(&out, "jq query over xml input");
    assert_stdout_trim_eq(&out, "Rust", "jq query over xml input");
}

#[test]
fn parity_xml_scalars_stay_strings_contract() {
    let out = run_zq_stdin(
        &["--input-format", "xml", "-c", "[(.root.n|type),(.root.flag|type),(.root.none|type)]"],
        "<root><n>10</n><flag>true</flag><none>null</none></root>",
    );
    assert_ok(&out, "xml scalars stay strings");
    assert_stdout_trim_eq(&out, "[\"string\",\"string\",\"string\"]", "xml scalars stay strings");
}

#[test]
fn parity_small_json_transform_cases_contract() {
    struct Case {
        id: &'static str,
        query: &'static str,
        input_json: &'static str,
        expected_lines: &'static [&'static str],
    }

    let cases = [
        Case {
            id: "jq_identity",
            query: ".",
            input_json: r#"{"a":1,"b":[1,2,3]}"#,
            expected_lines: &[r#"{"a":1,"b":[1,2,3]}"#],
        },
        Case {
            id: "jq_field",
            query: ".a",
            input_json: r#"{"a":1,"b":2}"#,
            expected_lines: &["1"],
        },
        Case {
            id: "jq_nested_field",
            query: ".a.b",
            input_json: r#"{"a":{"b":3}}"#,
            expected_lines: &["3"],
        },
    ];

    for case in cases {
        let out = run_zq_stdin(&["-c", case.query], &format!("{}\n", case.input_json));
        assert_ok(&out, case.id);
        let actual_lines: Vec<String> = stdout_text(&out)
            .lines()
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let expected_lines: Vec<String> =
            case.expected_lines.iter().map(ToString::to_string).collect();
        assert_eq!(
            actual_lines,
            expected_lines,
            "{}\nstdout:\n{}\nstderr:\n{}",
            case.id,
            stdout_text(&out),
            stderr_text(&out)
        );
    }
}

#[test]
fn parity_output_format_yaml_contract() {
    let out = run_zq(&[".global", &fixture("valid-values.yaml"), "--output-format", "yaml"]);
    assert_ok(&out, "yaml output");
    let text = stdout_text(&out);
    assert!(text.contains("env: dev"), "yaml output must contain env key");
}

#[test]
fn parity_output_format_yaml_with_anchors_contract() {
    let out = run_zq_stdin(
        &[".", "--input-format", "json", "--output-format", "yaml", "--yaml-anchors"],
        "{\"a\":{\"x\":[1,2]},\"b\":{\"x\":[1,2]}}\n",
    );
    assert_ok(&out, "yaml output with anchors");
    let text = stdout_text(&out);
    assert!(text.contains("&a"), "yaml output with anchors must define readable anchor");
    assert!(text.contains("*a"), "yaml output with anchors must define readable alias");
}

#[test]
fn parity_output_format_yaml_with_strict_friendly_anchor_names_contract() {
    let out = run_zq_stdin(
        &[
            ".",
            "--input-format",
            "json",
            "--output-format",
            "yaml",
            "--yaml-anchors",
            "--yaml-anchor-name-mode",
            "strict-friendly",
        ],
        "{\"cluster-metrics-apiversion\":{\"x\":[1,2]},\"other\":{\"x\":[1,2]}}\n",
    );
    assert_ok(&out, "yaml output with strict-friendly anchors");
    let text = stdout_text(&out);
    assert!(text.contains('&'), "yaml output with strict-friendly anchors must define anchor");
    assert!(text.contains('*'), "yaml output with strict-friendly anchors must define alias");
}

#[test]
fn parity_output_format_xml_contract() {
    let out = run_zq_stdin(
        &["--input-format", "json", "--output-format", "xml", "."],
        "{\"catalog\":{\"book\":{\"title\":\"Rust\"}}}\n",
    );
    assert_ok(&out, "xml output");
    let text = stdout_text(&out);
    assert!(
        text.contains("<catalog><book><title>Rust</title></book></catalog>"),
        "xml output must contain catalog/book/title\nstdout:\n{text}\nstderr:\n{}",
        stderr_text(&out)
    );
}

#[test]
fn parity_yaml_to_csv_ragged_arrays_contract() {
    let input = "- id: a\n  vals: [1, 2]\n- id: b\n  vals: [3]\n";
    let out =
        run_zq_stdin(&["--input-format", "yaml", "--output-format", "csv", ".[] | .vals"], input);
    assert_ok(&out, "yaml to csv with ragged arrays");
    assert_stdout_trim_eq(&out, "1,2\n3,", "yaml to csv with ragged arrays");
}

#[test]
fn parity_forced_csv_stdin_single_column_contract() {
    let out = run_zq_stdin(&["--input-format", "csv", "--output-format", "yaml"], "cases\nx\n");
    assert_ok(&out, "forced csv on stdin (single column)");
    let text = stdout_text(&out);
    assert!(
        text.contains("cases: x"),
        "forced csv on stdin (single column)\nstdout:\n{text}\nstderr:\n{}",
        stderr_text(&out)
    );
}

#[test]
fn parity_csv_parse_json_cells_roundtrip_contract() {
    let input = "cases:\n- id: jq_identity\n  query: .\n  input_json: '{\"a\":1}'\n";
    let csv_out = run_zq_stdin(&["--input-format", "yaml", "--output-format", "csv"], input);
    assert_ok(&csv_out, "yaml to csv");

    let recovered = run_zq_stdin(
        &["--input-format", "csv", "--csv-parse-json-cells", "-r", ".cases[0].id"],
        &stdout_text(&csv_out),
    );
    assert_ok(&recovered, "csv json-cell roundtrip");
    assert_stdout_trim_eq(&recovered, "jq_identity", "csv json-cell roundtrip");
}

#[test]
fn parity_doc_mode_index_requires_doc_index() {
    let out = run_zq(&[".", &fixture("valid-values.yaml"), "--doc-mode", "index"]);
    assert_fail(&out, "doc-mode index without doc-index");
    assert_stderr_contains(&out, "--doc-index is required", "doc-mode index without doc-index");
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
    let out = run_zq(&[".global.env", "--input", &fixture("valid-values.yaml"), "-r"]);
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

    let out = run_zq(&["-Rne", r#"[inputs] == ["a","b","c"]"#, &fixture("raw-lines.txt")]);
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
        "zq: error (at <stdin>:1): Cannot index number with string \"a\"",
        "runtime error format",
    );

    let out = run_zq_stdin(&[".[1]"], "{}\n");
    assert_fail(&out, "index runtime error format");
    assert_exit_code(&out, 5, "index runtime error format");
    assert_stderr_trim_eq(
        &out,
        "zq: error (at <stdin>:1): Cannot index object with number",
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
    assert_stderr_contains(&out, "incompatible with --raw-input", "--stream with --raw-input");

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

    let out = run_zq(&["-n", "-c", "--arg", "foo", "x", "--argjson", "bar", "2", "{$foo, $bar}"]);
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
    assert_stdout_trim_eq(&out, "{\"foo\":[1,2],\"bar\":\"ab\\ncd\\n\"}", "slurpfile + rawfile");

    let out = run_zq(&["-n", "-c", "$ARGS.positional", "--args", "a", "b"]);
    assert_ok(&out, "--args positional");
    assert_stdout_trim_eq(&out, "[\"a\",\"b\"]", "--args positional");

    let out = run_zq(&["-n", "-c", "$ARGS.positional", "--jsonargs", "1", "{\"a\":2}"]);
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
    assert_stderr_contains(&out, "zq: 1 compile error", "compile error exit");
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
    assert_stderr_contains(&out, "zq: error:", "missing input file");
}

#[test]
fn parity_filter_mode_accepts_multiple_input_files() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let a = td.path().join("a.json");
    let b = td.path().join("b.json");
    std::fs::write(&a, "{\"a\":1}\n").expect("write a");
    std::fs::write(&b, "{\"b\":2}\n").expect("write b");

    let a_s = a.to_string_lossy().into_owned();
    let b_s = b.to_string_lossy().into_owned();
    let out = run_zq(&["-c", ".", &a_s, &b_s]);
    assert_ok(&out, "multi-file filter mode");
    assert_stdout_trim_eq(&out, "{\"a\":1}\n{\"b\":2}", "multi-file filter mode");
}

#[test]
fn parity_diff_mode_reports_semantic_equality() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.yaml");
    let right = td.path().join("right.json");
    std::fs::write(&left, "a: 1\nb:\n  c: [1,2]\n").expect("write left");
    std::fs::write(&right, "{\"b\":{\"c\":[1,2]},\"a\":1}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", &left_s, &right_s]);
    assert_ok(&out, "--diff semantic equality");
    assert_exit_code(&out, 0, "--diff semantic equality");
    assert_stdout_trim_eq(&out, "No semantic differences.", "--diff semantic equality");
}

#[test]
fn parity_diff_mode_reports_structural_changes() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\"a\":1,\"b\":[1,2],\"drop\":true}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2,\"b\":[1,3,4],\"add\":\"x\"}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", &left_s, &right_s]);
    assert_fail(&out, "--diff structural changes");
    assert_exit_code(&out, 1, "--diff structural changes");
    let text = stdout_text(&out);
    assert!(text.contains("Found"), "stdout:\n{text}");
    assert!(text.contains("~ $.a"), "stdout:\n{text}");
    assert!(text.contains("~ $.b[1]"), "stdout:\n{text}");
    assert!(text.contains("+ $.add"), "stdout:\n{text}");
    assert!(text.contains("- $.drop"), "stdout:\n{text}");
}

#[test]
fn parity_diff_mode_json_format_reports_structured_payload() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\"a\":1,\"b\":[1,2],\"drop\":true}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2,\"b\":[1,3,4],\"add\":\"x\"}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "json", &left_s, &right_s]);
    assert_fail(&out, "--diff --diff-format json");
    assert_exit_code(&out, 1, "--diff --diff-format json");
    let payload: serde_json::Value =
        serde_json::from_str(stdout_text(&out).trim()).expect("valid diff json");
    assert_eq!(payload.get("equal"), Some(&serde_json::Value::Bool(false)));
    assert_eq!(payload.pointer("/summary/total"), Some(&serde_json::Value::from(5u64)));
    assert_eq!(payload.pointer("/summary/changed"), Some(&serde_json::Value::from(2u64)));
    assert_eq!(payload.pointer("/summary/added"), Some(&serde_json::Value::from(2u64)));
    assert_eq!(payload.pointer("/summary/removed"), Some(&serde_json::Value::from(1u64)));
}

#[test]
fn parity_diff_mode_jsonl_format_emits_summary_for_equal_inputs() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.yaml");
    let right = td.path().join("right.json");
    std::fs::write(&left, "x: [1,2]\n").expect("write left");
    std::fs::write(&right, "{\"x\":[1,2]}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "jsonl", &left_s, &right_s]);
    assert_ok(&out, "--diff --diff-format jsonl equal");
    assert_exit_code(&out, 0, "--diff --diff-format jsonl equal");
    let text = stdout_text(&out);
    let lines = text.lines().map(str::trim).filter(|line| !line.is_empty()).collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "stdout:\n{text}");
    let summary: serde_json::Value = serde_json::from_str(lines[0]).expect("jsonl summary");
    assert_eq!(summary.get("type"), Some(&serde_json::Value::String("summary".to_string())));
    assert_eq!(summary.get("equal"), Some(&serde_json::Value::Bool(true)));
}

#[test]
fn parity_diff_mode_summary_format_is_machine_friendly() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\"a\":1}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "summary", &left_s, &right_s]);
    assert_fail(&out, "--diff --diff-format summary");
    assert_exit_code(&out, 1, "--diff --diff-format summary");
    assert_stdout_trim_eq(
        &out,
        "equal=false total=1 changed=1 added=0 removed=0",
        "--diff --diff-format summary",
    );
}

#[test]
fn parity_diff_mode_patch_format_is_unified_style() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\"a\":1,\"drop\":true}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2,\"add\":[1,2]}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "patch", &left_s, &right_s]);
    assert_fail(&out, "--diff --diff-format patch");
    assert_exit_code(&out, 1, "--diff --diff-format patch");
    let text = stdout_text(&out);
    assert!(text.contains("--- left"), "stdout:\n{text}");
    assert!(text.contains("+++ right"), "stdout:\n{text}");
    assert!(text.contains("@@ $.a @@"), "stdout:\n{text}");
    assert!(text.contains("-1"), "stdout:\n{text}");
    assert!(text.contains("+2"), "stdout:\n{text}");
    assert!(text.contains("@@ $.add @@"), "stdout:\n{text}");
    assert!(text.contains("+[1,2]"), "stdout:\n{text}");
}

#[test]
fn parity_diff_mode_diff_format_supports_forced_color_and_monochrome_override() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\"a\":1,\"drop\":true}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2,\"add\":1}\n").expect("write right");
    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();

    let forced = run_zq(&["--diff", "--diff-format", "diff", "-C", &left_s, &right_s]);
    assert_fail(&forced, "--diff color forced");
    let forced_stdout = stdout_text(&forced);
    assert!(forced_stdout.contains("\u{1b}[33m~\u{1b}[0m"), "stdout:\n{forced_stdout}");

    let no_color = run_zq(&["--diff", "--diff-format", "diff", "-C", "-M", &left_s, &right_s]);
    assert_fail(&no_color, "--diff monochrome override");
    assert!(!stdout_text(&no_color).contains("\u{1b}["), "stdout:\n{}", stdout_text(&no_color));
}

#[test]
fn parity_diff_mode_supports_stdin_vs_file() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let right = td.path().join("right.yaml");
    std::fs::write(&right, "x: [1, 2]\n").expect("write right");
    let right_s = right.to_string_lossy().into_owned();

    let out = run_zq_stdin(&["--diff", &right_s], "{\"x\":[1,2]}\n");
    assert_ok(&out, "--diff stdin vs file");
    assert_exit_code(&out, 0, "--diff stdin vs file");
    assert_stdout_trim_eq(&out, "No semantic differences.", "--diff stdin vs file");
}

#[test]
fn parity_diff_mode_rejects_double_stdin() {
    let out = run_zq(&["--diff", "-", "-"]);
    assert_fail(&out, "--diff - -");
    assert_exit_code(&out, 5, "--diff - -");
    assert_stderr_contains(&out, "does not support reading both sides from stdin", "--diff - -");
}

#[test]
fn parity_diff_mode_reports_left_parse_failure_with_side_label() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{\n").expect("write invalid left");
    std::fs::write(&right, "{}\n").expect("write valid right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", &left_s, &right_s]);
    assert_fail(&out, "--diff parse failure on left");
    assert_exit_code(&out, 5, "--diff parse failure on left");
    assert_stderr_contains(&out, "--diff: cannot parse LEFT input", "--diff parse failure on left");
    assert_stderr_contains(&out, &left_s, "--diff parse failure on left");
}

#[test]
fn parity_diff_mode_reports_right_parse_failure_with_side_label() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&left, "{}\n").expect("write valid left");
    std::fs::write(&right, "{\n").expect("write invalid right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", &left_s, &right_s]);
    assert_fail(&out, "--diff parse failure on right");
    assert_exit_code(&out, 5, "--diff parse failure on right");
    assert_stderr_contains(
        &out,
        "--diff: cannot parse RIGHT input",
        "--diff parse failure on right",
    );
    assert_stderr_contains(&out, &right_s, "--diff parse failure on right");
}

#[test]
fn parity_diff_mode_rejects_from_file_flag() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let query = td.path().join("query.jq");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");
    std::fs::write(&query, ".").expect("write query");
    std::fs::write(&left, "{}\n").expect("write left");
    std::fs::write(&right, "{}\n").expect("write right");

    let query_s = query.to_string_lossy().into_owned();
    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "-f", &query_s, &left_s, &right_s]);
    assert_fail(&out, "--diff with -f");
    assert_exit_code(&out, 5, "--diff with -f");
    assert_stderr_contains(
        &out,
        "--diff mode cannot be combined with -f/--from-file",
        "--diff with -f",
    );
}

#[test]
fn parity_diff_mode_indexes_multi_document_streams() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.jsonl");
    let right = td.path().join("right.jsonl");
    std::fs::write(&left, "{\"a\":1}\n{\"b\":1}\n").expect("write left");
    std::fs::write(&right, "{\"a\":2}\n{\"b\":1}\n{\"c\":3}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "diff", &left_s, &right_s]);
    assert_fail(&out, "--diff multi-doc");
    assert_exit_code(&out, 1, "--diff multi-doc");
    let text = stdout_text(&out);
    assert!(text.contains("~ $[0].a"), "stdout:\n{text}");
    assert!(text.contains("+ $[2]"), "stdout:\n{text}");
}

#[test]
fn parity_diff_mode_rejects_malformed_inputs_across_supported_formats() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let valid = td.path().join("valid.json");
    std::fs::write(&valid, "{}\n").expect("write valid baseline");
    let valid_s = valid.to_string_lossy().into_owned();

    let cases = [
        ("broken.json", "{\n"),
        ("broken.yaml", "a: [\n"),
        ("broken.toml", "a = [\n"),
        ("broken.xml", "<root>\n"),
    ];

    for (name, payload) in cases {
        let left = td.path().join(name);
        std::fs::write(&left, payload).expect("write malformed input");
        let left_s = left.to_string_lossy().into_owned();
        let out = run_zq(&["--diff", &left_s, &valid_s]);
        assert_fail(&out, name);
        assert_exit_code(&out, 5, name);
        assert_stderr_contains(&out, "--diff: cannot parse LEFT input", name);
        assert_stderr_contains(&out, &left_s, name);
    }
}

#[test]
fn parity_diff_mode_csv_quote_heavy_input_stays_stable() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.csv");
    let right = td.path().join("right.json");
    std::fs::write(&left, "a,\"b\"c\n").expect("write csv");
    std::fs::write(&right, "{}\n").expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", &left_s, &right_s]);
    assert_fail(&out, "--diff csv quote-heavy input");
    assert_exit_code(&out, 1, "--diff csv quote-heavy input");
    let text = stdout_text(&out);
    assert!(text.contains("Found 1 semantic differences:"), "stdout:\n{text}");
    assert!(text.contains("~ $:"), "stdout:\n{text}");
}

#[test]
fn parity_diff_mode_stress_summary_handles_large_nested_inputs() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.json");
    let right = td.path().join("right.json");

    let mut left_items = Vec::with_capacity(128);
    let mut right_items = Vec::with_capacity(128);
    for idx in 0..128usize {
        left_items.push(serde_json::json!({
            "id": idx,
            "meta": { "group": idx % 5, "flags": [idx, idx + 1, idx + 2] }
        }));
        right_items.push(serde_json::json!({
            "id": idx,
            "meta": {
                "group": if idx % 31 == 0 { 99 } else { idx % 5 },
                "flags": [idx, idx + 1, idx + 2]
            }
        }));
    }
    right_items.push(serde_json::json!({"id": 999, "meta": {"group": 1, "flags": []}}));

    std::fs::write(&left, serde_json::to_string(&left_items).expect("left json"))
        .expect("write left");
    std::fs::write(&right, serde_json::to_string(&right_items).expect("right json"))
        .expect("write right");

    let left_s = left.to_string_lossy().into_owned();
    let right_s = right.to_string_lossy().into_owned();
    let out = run_zq(&["--diff", "--diff-format", "summary", &left_s, &right_s]);
    assert_fail(&out, "--diff stress summary");
    assert_exit_code(&out, 1, "--diff stress summary");
    assert_stdout_trim_eq(
        &out,
        "equal=false total=6 changed=5 added=1 removed=0",
        "--diff stress summary",
    );
}

#[test]
fn parity_diff_mode_stdin_spool_is_cleaned_after_process_exit() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let spool_root = td.path().join("spool-root");
    let right = td.path().join("right.yaml");
    std::fs::write(&right, "x: [1, 2]\n").expect("write right");
    let right_s = right.to_string_lossy().into_owned();

    let out = run_zq_stdin_env(
        &["--diff", &right_s],
        "{\"x\":[1,2]}\n",
        &[("ZQ_SPOOL_DIR", spool_root.to_string_lossy().as_ref())],
    );
    assert_ok(&out, "--diff stdin spool cleanup");
    assert_exit_code(&out, 0, "--diff stdin spool cleanup");

    let root = spool_root.join("v1");
    assert!(root.exists(), "spool root must exist after stdin-backed diff");
    let run_dirs = std::fs::read_dir(&root)
        .expect("read spool root")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("run-"))
        .collect::<Vec<_>>();
    assert!(run_dirs.is_empty(), "run dirs must be cleaned up after process exit");
}
