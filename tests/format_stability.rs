use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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

#[test]
fn forced_input_formats_are_stable_on_success_and_error() {
    struct OkCase<'a> {
        id: &'a str,
        format: &'a str,
        query: &'a str,
        input: &'a str,
        expected: &'a str,
    }

    let ok_cases = [
        OkCase {
            id: "json_ok",
            format: "json",
            query: ".a",
            input: "{\"a\":1}\n",
            expected: "1",
        },
        OkCase {
            id: "yaml_ok",
            format: "yaml",
            query: ".a",
            input: "a: 1\n",
            expected: "1",
        },
        OkCase {
            id: "toml_ok",
            format: "toml",
            query: ".a",
            input: "a = 1\n",
            expected: "1",
        },
        OkCase {
            id: "csv_ok",
            format: "csv",
            query: ".a",
            input: "a,b\n1,2\n",
            expected: "1",
        },
        OkCase {
            id: "xml_ok",
            format: "xml",
            query: ".root.a",
            input: "<root><a>1</a></root>",
            expected: "1",
        },
    ];

    for case in ok_cases {
        let out = run_zq_stdin(
            &["--input-format", case.format, "-r", case.query],
            case.input,
        );
        assert_ok(&out, case.id);
        assert_stdout_trim_eq(&out, case.expected, case.id);
    }

    // CSV reader is intentionally permissive in this project, so here we
    // assert strict failures for formats that are expected to reject malformed
    // payloads.
    struct ErrCase<'a> {
        id: &'a str,
        format: &'a str,
        input: &'a str,
        marker: &'a str,
    }

    let err_cases = [
        ErrCase {
            id: "json_err",
            format: "json",
            input: "{\n",
            marker: "parse error",
        },
        ErrCase {
            id: "yaml_err",
            format: "yaml",
            input: "a: [1\n",
            marker: "yaml:",
        },
        ErrCase {
            id: "toml_err",
            format: "toml",
            input: "a =\n",
            marker: "toml:",
        },
        ErrCase {
            id: "xml_err",
            format: "xml",
            input: "<root><a></root>",
            marker: "xml:",
        },
    ];

    for case in err_cases {
        let out = run_zq_stdin(&["--input-format", case.format, "."], case.input);
        assert_fail(&out, case.id);
        assert_stderr_contains(&out, case.marker, case.id);
    }

    let permissive_csv = run_zq_stdin(&["--input-format", "csv", "-c", "."], "a,b\n1,\"2\n");
    assert_ok(&permissive_csv, "csv_permissive_mode");
    assert_stdout_trim_eq(
        &permissive_csv,
        r#"{"a":"1","b":"2\n"}"#,
        "csv_permissive_mode",
    );
}

#[test]
fn output_formats_emit_machine_parseable_payloads() {
    let input_json = "{\"a\":1,\"b\":\"x\"}\n";

    let json_out = run_zq_stdin(
        &[
            "--input-format",
            "json",
            "--output-format",
            "json",
            "-c",
            ".",
        ],
        input_json,
    );
    assert_ok(&json_out, "json output");
    let parsed_json: serde_json::Value =
        serde_json::from_str(stdout_text(&json_out).trim()).expect("parse json output");
    assert_eq!(parsed_json, serde_json::json!({"a": 1, "b": "x"}));

    let yaml_out = run_zq_stdin(
        &["--input-format", "json", "--output-format", "yaml", "."],
        input_json,
    );
    assert_ok(&yaml_out, "yaml output");
    let parsed_yaml: serde_yaml::Value =
        serde_yaml::from_str(&stdout_text(&yaml_out)).expect("parse yaml output");
    assert_eq!(parsed_yaml["a"], serde_yaml::Value::Number(1.into()));

    let toml_out = run_zq_stdin(
        &["--input-format", "json", "--output-format", "toml", "."],
        input_json,
    );
    assert_ok(&toml_out, "toml output");
    let parsed_toml: toml::Value =
        toml::from_str(&stdout_text(&toml_out)).expect("parse toml output");
    assert_eq!(parsed_toml["a"], toml::Value::Integer(1));

    let csv_out = run_zq_stdin(
        &["--input-format", "json", "--output-format", "csv", "."],
        input_json,
    );
    assert_ok(&csv_out, "csv output");
    let csv_text = stdout_text(&csv_out);
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_text.as_bytes());
    let rows = csv_reader
        .records()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse csv output");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("1"));
    assert_eq!(rows[0].get(1), Some("x"));

    let xml_out = run_zq_stdin(
        &["--input-format", "json", "--output-format", "xml", "."],
        input_json,
    );
    assert_ok(&xml_out, "xml output");
    let xml_text = stdout_text(&xml_out);
    let xml_doc = roxmltree::Document::parse(xml_text.trim()).expect("parse xml output");
    assert_eq!(xml_doc.root_element().tag_name().name(), "root");
}
