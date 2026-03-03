use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use zq::{run_jq_stream_with_paths_options, EngineRunOptions};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn oracle_path() -> PathBuf {
    root().join("tests/compat/jq_oracle_hardcode.json")
}

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

#[derive(Debug, Deserialize)]
struct OracleFile {
    cases: Vec<OracleCase>,
}

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    query: String,
    #[serde(default)]
    null_input: bool,
    #[serde(default)]
    input_stream: Vec<JsonValue>,
    #[serde(default)]
    expected_output: Vec<JsonValue>,
    expected_error_contains: Option<String>,
}

fn run_cli(query: &str, input_stream: &[JsonValue], null_input: bool) -> Output {
    let mut cmd = Command::new(bin());
    cmd.arg("-c");
    if null_input {
        cmd.arg("-n");
    }
    cmd.arg(query)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn zq");
    if !null_input {
        let mut stdin = child.stdin.take().expect("stdin");
        for value in input_stream {
            let line = serde_json::to_string(value).expect("json input");
            stdin.write_all(line.as_bytes()).expect("write stdin");
            stdin.write_all(b"\n").expect("write newline");
        }
    }
    child.wait_with_output().expect("wait zq")
}

fn parse_cli_stdout_json_lines(stdout: &[u8]) -> Vec<JsonValue> {
    let text = String::from_utf8_lossy(stdout).replace("\r\n", "\n");
    if text.trim().is_empty() {
        return Vec::new();
    }
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<JsonValue>(line).expect("stdout line json"))
        .collect()
}

#[test]
fn jq_oracle_hardcode_baseline_contract() {
    let raw = fs::read_to_string(oracle_path()).expect("read oracle file");
    let oracle: OracleFile = serde_json::from_str(&raw).expect("parse oracle file");
    assert!(!oracle.cases.is_empty(), "oracle cases must not be empty");

    for case in oracle.cases {
        let lib = run_jq_stream_with_paths_options(
            &case.query,
            case.input_stream.clone(),
            &[],
            EngineRunOptions {
                null_input: case.null_input,
            },
        );
        let cli_out = run_cli(&case.query, &case.input_stream, case.null_input);

        if let Some(needle) = &case.expected_error_contains {
            let lib_err = lib.expect_err("library should fail");
            assert!(
                lib_err.to_string().contains(needle),
                "library error mismatch for case `{}`\nneedle: {}\nactual: {}",
                case.id,
                needle,
                lib_err
            );
            assert!(
                !cli_out.status.success(),
                "cli should fail for case `{}`\nstdout:\n{}\nstderr:\n{}",
                case.id,
                String::from_utf8_lossy(&cli_out.stdout),
                String::from_utf8_lossy(&cli_out.stderr)
            );
            let stderr = String::from_utf8_lossy(&cli_out.stderr);
            assert!(
                stderr.contains(needle),
                "cli stderr mismatch for case `{}`\nneedle: {}\nactual:\n{}",
                case.id,
                needle,
                stderr
            );
            continue;
        }

        let lib_out = lib.unwrap_or_else(|e| panic!("library failed for `{}`: {e}", case.id));
        assert_eq!(
            lib_out, case.expected_output,
            "library output mismatch for case `{}`\nquery: {}",
            case.id, case.query
        );
        assert!(
            cli_out.status.success(),
            "cli failed for case `{}`\nstdout:\n{}\nstderr:\n{}",
            case.id,
            String::from_utf8_lossy(&cli_out.stdout),
            String::from_utf8_lossy(&cli_out.stderr)
        );
        let cli_out_values = parse_cli_stdout_json_lines(&cli_out.stdout);
        assert_eq!(
            cli_out_values, case.expected_output,
            "cli output mismatch for case `{}`\nquery: {}",
            case.id, case.query
        );
    }
}
