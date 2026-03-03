use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn zq_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn has_jq() -> bool {
    Command::new("jq")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn run_program(program: &str, args: &[String], stdin: &[u8], cwd: &Path) -> Output {
    let mut child = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn `{program}` failed: {e}"));
    if let Some(mut child_stdin) = child.stdin.take() {
        child_stdin
            .write_all(stdin)
            .unwrap_or_else(|e| panic!("stdin write for `{program}` failed: {e}"));
    }
    child
        .wait_with_output()
        .unwrap_or_else(|e| panic!("wait for `{program}` failed: {e}"))
}

fn normalize_newlines(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'\r' && bytes[i + 1] == b'\n' {
            out.push(b'\n');
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

fn cmd_line(program: &str, args: &[String]) -> String {
    let mut rendered = String::from(program);
    for arg in args {
        rendered.push(' ');
        rendered.push_str(arg);
    }
    rendered
}

#[derive(Debug)]
struct ParityCase {
    name: &'static str,
    args: Vec<String>,
    stdin: Vec<u8>,
    compare_stderr: bool,
}

#[test]
fn jq_help_lists_core_options_that_zq_covers() {
    if !has_jq() {
        eprintln!("skip jq help parity: jq is not installed");
        return;
    }

    let cwd = Path::new(env!("CARGO_MANIFEST_DIR"));
    let jq_help = run_program("jq", &[String::from("--help")], &[], cwd);
    let zq_help = run_program(zq_bin(), &[String::from("--help")], &[], cwd);
    assert!(jq_help.status.success(), "jq --help must succeed");
    assert!(zq_help.status.success(), "zq --help must succeed");

    let jq_text = String::from_utf8_lossy(&jq_help.stdout);
    let zq_text = String::from_utf8_lossy(&zq_help.stdout);
    let covered_tokens = [
        ("--null-input", "--null-input"),
        ("--raw-input", "--raw-input"),
        ("--slurp", "--slurp"),
        ("--compact-output", "--compact-output"),
        ("--raw-output", "--raw-output"),
        ("--raw-output0", "--raw-output0"),
        ("--join-output", "--join-output"),
        ("--stream", "--stream"),
        ("--stream-errors", "--stream-errors"),
        ("--seq", "--seq"),
        ("--from-file", "--from-file"),
        ("-L", "--library-path"),
        ("--arg", "--arg"),
        ("--argjson", "--argjson"),
        ("--slurpfile", "--slurpfile"),
        ("--rawfile", "--rawfile"),
        ("--args", "--args"),
        ("--jsonargs", "--jsonargs"),
        ("--exit-status", "--exit-status"),
    ];

    for (jq_token, zq_token) in covered_tokens {
        assert!(
            jq_text.contains(jq_token),
            "jq help must include token `{jq_token}`"
        );
        assert!(
            zq_text.contains(zq_token),
            "zq help must include token `{zq_token}`"
        );
    }
}

#[test]
fn jq_help_option_behavior_matches_jq() {
    if !has_jq() {
        eprintln!("skip jq help parity: jq is not installed");
        return;
    }

    let cwd = Path::new(env!("CARGO_MANIFEST_DIR"));
    let td = tempfile::TempDir::new().expect("tempdir");

    let input_file = td.path().join("input.json");
    fs::write(&input_file, "{\"a\":7}\n").expect("write input");

    let number_file = td.path().join("number.json");
    fs::write(&number_file, "1\n").expect("write number");

    let query_file = td.path().join("query.jq");
    fs::write(&query_file, ".a\n").expect("write query");

    let module_dir = td.path().join("modules");
    fs::create_dir_all(&module_dir).expect("create modules dir");
    fs::write(module_dir.join("m.jq"), "def inc: . + 1;\n").expect("write module");

    let slurp_file = td.path().join("slurp.json");
    fs::write(&slurp_file, "1\n2\n").expect("write slurpfile");

    let raw_file = td.path().join("raw.txt");
    fs::write(&raw_file, "alpha\nbeta\n").expect("write rawfile");

    let input_file_s = input_file.to_string_lossy().to_string();
    let number_file_s = number_file.to_string_lossy().to_string();
    let query_file_s = query_file.to_string_lossy().to_string();
    let module_dir_s = module_dir.to_string_lossy().to_string();
    let slurp_file_s = slurp_file.to_string_lossy().to_string();
    let raw_file_s = raw_file.to_string_lossy().to_string();

    let cases = vec![
        ParityCase {
            name: "null_input_compact",
            args: vec!["-nc".to_string(), "[1,2,3]".to_string()],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "raw_input",
            args: vec!["-R".to_string(), ".".to_string()],
            stdin: b"a\nb\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "raw_input_slurp",
            args: vec!["-Rs".to_string(), ".".to_string()],
            stdin: b"a\nb\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "slurp_json",
            args: vec!["-s".to_string(), ".".to_string()],
            stdin: b"1\n2\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "raw_output",
            args: vec!["-r".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "join_output",
            args: vec!["-j".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "raw_output0",
            args: vec!["--raw-output0".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "indent",
            args: vec![
                "-n".to_string(),
                "--indent".to_string(),
                "4".to_string(),
                "{\"a\":1,\"b\":2}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "from_file_filter",
            args: vec!["-f".to_string(), query_file_s.clone(), input_file_s.clone()],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "library_path_flag",
            args: vec![
                "-L".to_string(),
                module_dir_s.clone(),
                ".".to_string(),
                number_file_s.clone(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "arg",
            args: vec![
                "-n".to_string(),
                "--arg".to_string(),
                "foo".to_string(),
                "bar".to_string(),
                "--arg".to_string(),
                "bar".to_string(),
                "baz".to_string(),
                "{$foo, $bar}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "argjson",
            args: vec![
                "-n".to_string(),
                "--argjson".to_string(),
                "foo".to_string(),
                "2".to_string(),
                "--argjson".to_string(),
                "bar".to_string(),
                "{\"x\":1}".to_string(),
                "{$foo, $bar}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "slurpfile",
            args: vec![
                "-n".to_string(),
                "--slurpfile".to_string(),
                "foo".to_string(),
                slurp_file_s.clone(),
                "--arg".to_string(),
                "bar".to_string(),
                "ok".to_string(),
                "{$foo, $bar}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "rawfile",
            args: vec![
                "-n".to_string(),
                "--rawfile".to_string(),
                "foo".to_string(),
                raw_file_s.clone(),
                "--argjson".to_string(),
                "bar".to_string(),
                "1".to_string(),
                "{$foo, $bar}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "args_mode",
            args: vec![
                "-n".to_string(),
                "--args".to_string(),
                "$ARGS.positional".to_string(),
                "x".to_string(),
                "y".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "jsonargs_mode",
            args: vec![
                "-n".to_string(),
                "--jsonargs".to_string(),
                "$ARGS.positional".to_string(),
                "1".to_string(),
                "{\"a\":2}".to_string(),
            ],
            stdin: vec![],
            compare_stderr: true,
        },
        ParityCase {
            name: "stream",
            args: vec!["--stream".to_string(), ".".to_string()],
            stdin: b"{\"a\":[1,2]}".to_vec(),
            compare_stderr: true,
        },
        ParityCase {
            name: "stream_errors",
            args: vec!["--stream-errors".to_string(), ".".to_string()],
            stdin: b"{\"a\":1".to_vec(),
            compare_stderr: true,
        },
    ];

    for case in cases {
        let jq_out = run_program("jq", &case.args, &case.stdin, cwd);
        let zq_out = run_program(zq_bin(), &case.args, &case.stdin, cwd);
        let cmd = cmd_line("jq", &case.args);
        assert_eq!(
            zq_out.status.code(),
            jq_out.status.code(),
            "status mismatch for case `{}`\ncommand: {}\njq stdout:\n{}\njq stderr:\n{}\nzq stdout:\n{}\nzq stderr:\n{}",
            case.name,
            cmd,
            String::from_utf8_lossy(&jq_out.stdout),
            String::from_utf8_lossy(&jq_out.stderr),
            String::from_utf8_lossy(&zq_out.stdout),
            String::from_utf8_lossy(&zq_out.stderr),
        );
        assert_eq!(
            normalize_newlines(&zq_out.stdout),
            normalize_newlines(&jq_out.stdout),
            "stdout mismatch for case `{}`\ncommand: {}\njq stdout:\n{}\njq stderr:\n{}\nzq stdout:\n{}\nzq stderr:\n{}",
            case.name,
            cmd,
            String::from_utf8_lossy(&jq_out.stdout),
            String::from_utf8_lossy(&jq_out.stderr),
            String::from_utf8_lossy(&zq_out.stdout),
            String::from_utf8_lossy(&zq_out.stderr),
        );
        if case.compare_stderr {
            assert_eq!(
                normalize_newlines(&zq_out.stderr),
                normalize_newlines(&jq_out.stderr),
                "stderr mismatch for case `{}`\ncommand: {}\njq stdout:\n{}\njq stderr:\n{}\nzq stdout:\n{}\nzq stderr:\n{}",
                case.name,
                cmd,
                String::from_utf8_lossy(&jq_out.stdout),
                String::from_utf8_lossy(&jq_out.stderr),
                String::from_utf8_lossy(&zq_out.stdout),
                String::from_utf8_lossy(&zq_out.stderr),
            );
        }
    }
}
