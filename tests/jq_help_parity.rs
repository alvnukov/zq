use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn zq_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
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
    child.wait_with_output().unwrap_or_else(|e| panic!("wait for `{program}` failed: {e}"))
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
struct SmokeCase {
    name: &'static str,
    args: Vec<String>,
    stdin: Vec<u8>,
}

#[test]
fn jq_help_lists_core_options_that_zq_covers() {
    let cwd = Path::new(env!("CARGO_MANIFEST_DIR"));
    let zq_help = run_program(zq_bin(), &[String::from("--help")], &[], cwd);
    assert!(zq_help.status.success(), "zq --help must succeed");

    let zq_text = String::from_utf8_lossy(&zq_help.stdout);
    let covered_tokens = [
        "--null-input",
        "--raw-input",
        "--slurp",
        "--compact-output",
        "--raw-output",
        "--raw-output0",
        "--join-output",
        "--stream",
        "--stream-errors",
        "--seq",
        "--from-file",
        "-L",
        "--arg",
        "--argjson",
        "--slurpfile",
        "--rawfile",
        "--args",
        "--jsonargs",
        "--exit-status",
    ];

    for token in covered_tokens {
        assert!(zq_text.contains(token), "zq help must include `{token}`");
    }
}

#[test]
fn jq_help_option_behavior_smoke() {
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
        SmokeCase {
            name: "null_input_compact",
            args: vec!["-nc".to_string(), "[1,2,3]".to_string()],
            stdin: vec![],
        },
        SmokeCase {
            name: "raw_input",
            args: vec!["-R".to_string(), ".".to_string()],
            stdin: b"a\nb\n".to_vec(),
        },
        SmokeCase {
            name: "raw_input_slurp",
            args: vec!["-Rs".to_string(), ".".to_string()],
            stdin: b"a\nb\n".to_vec(),
        },
        SmokeCase {
            name: "slurp_json",
            args: vec!["-s".to_string(), ".".to_string()],
            stdin: b"1\n2\n".to_vec(),
        },
        SmokeCase {
            name: "raw_output",
            args: vec!["-r".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
        },
        SmokeCase {
            name: "join_output",
            args: vec!["-j".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
        },
        SmokeCase {
            name: "raw_output0",
            args: vec!["--raw-output0".to_string(), ".".to_string()],
            stdin: b"\"a\"\n\"b\"\n".to_vec(),
        },
        SmokeCase {
            name: "indent",
            args: vec![
                "-n".to_string(),
                "--indent".to_string(),
                "4".to_string(),
                "{\"a\":1,\"b\":2}".to_string(),
            ],
            stdin: vec![],
        },
        SmokeCase {
            name: "from_file_filter",
            args: vec!["-f".to_string(), query_file_s.clone(), input_file_s.clone()],
            stdin: vec![],
        },
        SmokeCase {
            name: "library_path_flag",
            args: vec![
                "-L".to_string(),
                module_dir_s.clone(),
                ".".to_string(),
                number_file_s.clone(),
            ],
            stdin: vec![],
        },
        SmokeCase {
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
        },
        SmokeCase {
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
        },
        SmokeCase {
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
        },
        SmokeCase {
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
        },
        SmokeCase {
            name: "args_mode",
            args: vec![
                "-n".to_string(),
                "--args".to_string(),
                "$ARGS.positional".to_string(),
                "x".to_string(),
                "y".to_string(),
            ],
            stdin: vec![],
        },
        SmokeCase {
            name: "jsonargs_mode",
            args: vec![
                "-n".to_string(),
                "--jsonargs".to_string(),
                "$ARGS.positional".to_string(),
                "1".to_string(),
                "{\"a\":2}".to_string(),
            ],
            stdin: vec![],
        },
        SmokeCase {
            name: "stream",
            args: vec!["--stream".to_string(), ".".to_string()],
            stdin: b"{\"a\":[1,2]}".to_vec(),
        },
        SmokeCase {
            name: "stream_errors",
            args: vec!["--stream-errors".to_string(), ".".to_string()],
            stdin: b"{\"a\":1".to_vec(),
        },
    ];

    for case in cases {
        let out = run_program(zq_bin(), &case.args, &case.stdin, cwd);
        let cmd = cmd_line("zq", &case.args);
        assert!(
            out.status.success(),
            "non-zero status for case `{}`\ncommand: {}\nstdout:\n{}\nstderr:\n{}",
            case.name,
            cmd,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
}
