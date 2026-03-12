use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const JQ_REPO_URL: &str = "https://github.com/jqlang/jq.git";

#[derive(Debug, Clone, Copy)]
struct JqSuiteSpec {
    name: &'static str,
    dir: &'static str,
    git_ref: &'static str,
    color_compat: Option<&'static str>,
}

const JQ_SUITE_SPECS: &[JqSuiteSpec] = &[
    JqSuiteSpec { name: "jq", dir: ".tmp/jq", git_ref: "jq-1.6", color_compat: None },
    JqSuiteSpec {
        name: "jq171",
        dir: ".tmp/jq171",
        git_ref: "jq-1.7.1",
        color_compat: Some("jq171"),
    },
];

#[derive(Debug)]
struct JqSuite {
    root: PathBuf,
    tests_dir: PathBuf,
    modules_dir: PathBuf,
    color_compat: Option<&'static str>,
}

#[derive(Debug, Deserialize)]
struct CompatCasesFile {
    cases: Vec<CompatCase>,
}

#[derive(Debug, Deserialize)]
struct CompatCase {
    id: String,
    query: String,
    input_json: String,
    #[serde(default)]
    null_input: bool,
}

fn zq_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn jq_bin() -> String {
    std::env::var("ZQ_JQ_BIN").unwrap_or_else(|_| "jq".to_string())
}

fn run_cmd(cmd: &mut Command, ctx: &str, timeout: Duration) {
    let out = run_cmd_capture_timeout(cmd, timeout, ctx).unwrap_or_else(|err| panic!("{err}"));
    assert!(
        out.status.success(),
        "{ctx}\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        summarize_output(&out.stdout),
        summarize_output(&out.stderr)
    );
}

fn run_cmd_capture_timeout(
    cmd: &mut Command,
    timeout: Duration,
    ctx: &str,
) -> Result<Output, String> {
    let stdout_file = tempfile::NamedTempFile::new()
        .map_err(|e| format!("{ctx}\nstdout temp file failed: {e}"))?;
    let stderr_file = tempfile::NamedTempFile::new()
        .map_err(|e| format!("{ctx}\nstderr temp file failed: {e}"))?;
    let stdout_path = stdout_file.path().to_path_buf();
    let stderr_path = stderr_file.path().to_path_buf();
    let stdout_writer =
        stdout_file.reopen().map_err(|e| format!("{ctx}\nstdout reopen failed: {e}"))?;
    let stderr_writer =
        stderr_file.reopen().map_err(|e| format!("{ctx}\nstderr reopen failed: {e}"))?;

    let mut child = cmd
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::from(stderr_writer))
        .spawn()
        .map_err(|e| format!("{ctx}\nspawn failed: {e}"))?;

    let start = Instant::now();
    let status = loop {
        if let Some(status) =
            child.try_wait().map_err(|e| format!("{ctx}\ntry_wait failed: {e}"))?
        {
            break status;
        }
        if start.elapsed() > timeout {
            let _ = child.kill();
            let _ = child.wait();
            let (stdout, stderr) = read_output_files(&stdout_path, &stderr_path)?;
            return Err(format!(
                "timeout after {:?}: {}\nstdout:\n{}\nstderr:\n{}",
                timeout,
                ctx,
                summarize_output(&stdout),
                summarize_output(&stderr)
            ));
        }
        thread::sleep(Duration::from_millis(200));
    };

    let (stdout, stderr) = read_output_files(&stdout_path, &stderr_path)?;
    Ok(Output { status, stdout, stderr })
}

fn read_output_files(stdout_path: &Path, stderr_path: &Path) -> Result<(Vec<u8>, Vec<u8>), String> {
    let stdout = fs::read(stdout_path).map_err(io_err("read stdout file"))?;
    let stderr = fs::read(stderr_path).map_err(io_err("read stderr file"))?;
    Ok((stdout, stderr))
}

fn io_err(op: &'static str) -> impl Fn(io::Error) -> String {
    move |e| format!("{op}: {e}")
}

#[test]
fn jq_upstream_suite() {
    if std::env::var("ZQ_RUN_JQ_UPSTREAM").ok().as_deref() != Some("1") {
        eprintln!("skip jq upstream suite: set ZQ_RUN_JQ_UPSTREAM=1");
        return;
    }

    let jq_version = assert_jq_binary_available();
    assert_jq_version_matches_selected_suites(&jq_version);

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let run_tests_timeout = read_timeout("ZQ_JQ_RUN_TESTS_TIMEOUT_SECS", 1800);
    let shtest_timeout = read_timeout("ZQ_JQ_SHTEST_TIMEOUT_SECS", 5400);
    let suites = ensure_jq_suites(root, read_env_bool("ZQ_JQ_AUTO_CLONE", true));

    let mut failures = Vec::new();
    failures.extend(run_upstream_run_tests(&suites, run_tests_timeout));
    if !read_env_bool("ZQ_JQ_SKIP_SHTEST", false) {
        failures.extend(run_upstream_shtest(&suites, shtest_timeout));
    }
    failures.extend(run_jq_oracle_compat_cases(root));
    assert_compat_failures_empty(failures);
}

fn assert_compat_failures_empty(failures: Vec<String>) {
    if failures.is_empty() {
        return;
    }
    let summary = failures
        .iter()
        .enumerate()
        .map(|(idx, item)| format!("#{} {}\n{}", idx + 1, "-".repeat(60), item))
        .collect::<Vec<_>>()
        .join("\n");
    panic!("jq compatibility suite has {} failure(s)\n{}", failures.len(), summary);
}

fn read_timeout(env_name: &str, default_secs: u64) -> Duration {
    let secs = std::env::var(env_name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

fn read_env_bool(name: &str, default_value: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"),
        Err(_) => default_value,
    }
}

fn assert_jq_binary_available() -> String {
    let jq = jq_bin();
    let out = Command::new(&jq)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("failed to execute `{jq} --version`: {e}"));
    assert!(
        out.status.success(),
        "`{jq} --version` failed\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn assert_jq_version_matches_selected_suites(jq_version: &str) {
    let override_check = read_env_bool("ZQ_ALLOW_JQ_VERSION_MISMATCH", false);
    if override_check {
        return;
    }

    let expected = expected_jq_version_prefix_for_selected_suites();
    if let Some(prefix) = expected {
        assert!(
            jq_version.starts_with(prefix),
            "jq version `{jq_version}` does not match required prefix `{prefix}` for selected suites"
        );
    }
}

fn expected_jq_version_prefix_for_selected_suites() -> Option<&'static str> {
    let selected = selected_jq_suite_specs();
    if selected.is_empty() {
        return None;
    }
    if selected.iter().all(|spec| spec.name == "jq171") {
        return Some("jq-1.7.1");
    }
    if selected.iter().all(|spec| spec.name == "jq") {
        return Some("jq-1.6");
    }
    None
}

fn ensure_jq_suites(root: &Path, auto_clone: bool) -> Vec<JqSuite> {
    let selected_specs = selected_jq_suite_specs();
    let mut suites = Vec::with_capacity(selected_specs.len());

    for spec in selected_specs {
        let suite_root = root.join(spec.dir);
        let tests_dir = suite_root.join("tests");

        if !tests_dir.is_dir() {
            assert!(
                auto_clone,
                "missing jq suite at {}. Set ZQ_JQ_AUTO_CLONE=1 or provide local clones",
                suite_root.display()
            );
            clone_jq_suite(spec, &suite_root);
        }

        let tests_dir = suite_root.join("tests");
        let modules_dir = tests_dir.join("modules");
        assert!(tests_dir.is_dir(), "jq suite has no tests directory: {}", tests_dir.display());
        assert!(
            modules_dir.is_dir(),
            "jq suite has no modules directory: {}",
            modules_dir.display()
        );

        suites.push(JqSuite {
            root: suite_root,
            tests_dir,
            modules_dir,
            color_compat: spec.color_compat,
        });
    }

    suites
}

fn selected_jq_suite_specs() -> Vec<&'static JqSuiteSpec> {
    let selected_raw = std::env::var("ZQ_JQ_SUITES").unwrap_or_else(|_| "jq171".to_string());
    let mut out = Vec::new();
    for token in selected_raw.split(',').map(str::trim).filter(|token| !token.is_empty()) {
        let Some(spec) = JQ_SUITE_SPECS.iter().find(|spec| spec.name == token) else {
            panic!("unknown jq suite `{token}` in ZQ_JQ_SUITES");
        };
        out.push(spec);
    }
    assert!(!out.is_empty(), "ZQ_JQ_SUITES selects no suites");
    out
}

fn clone_jq_suite(spec: &JqSuiteSpec, destination: &Path) {
    if destination.exists() {
        fs::remove_dir_all(destination).unwrap_or_else(|e| {
            panic!("failed to remove existing path before clone {}: {e}", destination.display())
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|e| panic!("failed to create parent dir {}: {e}", parent.display()));
    }

    let ctx = format!("clone jq suite {} to {}", spec.git_ref, destination.display());
    let mut cmd = Command::new("git");
    cmd.arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--branch")
        .arg(spec.git_ref)
        .arg(JQ_REPO_URL)
        .arg(destination);
    run_cmd(&mut cmd, &ctx, Duration::from_secs(900));
}

fn run_upstream_run_tests(suites: &[JqSuite], timeout: Duration) -> Vec<String> {
    let mut files_total = 0usize;
    let mut failures = Vec::new();
    for suite in suites {
        let test_files = collect_test_files(&suite.tests_dir);
        assert!(!test_files.is_empty(), "no .test files found under {}", suite.tests_dir.display());
        files_total += test_files.len();

        eprintln!("running jq run-tests from {}", suite.root.display());
        failures.extend(run_test_files_sequential(
            test_files,
            &suite.modules_dir,
            &suite.tests_dir,
            timeout,
            suite.color_compat,
        ));
    }
    assert!(files_total > 0, "jq upstream run-tests set is empty");
    failures
}

fn run_upstream_shtest(suites: &[JqSuite], timeout: Duration) -> Vec<String> {
    let mut failures = Vec::new();
    for suite in suites {
        let shtest = suite.tests_dir.join("shtest");
        if !shtest.is_file() {
            continue;
        }

        let ctx = format!("run jq shtest via zq for {}", suite.root.display());
        let mut cmd = Command::new("sh");
        cmd.arg(&shtest).env("JQ", zq_bin()).env("PAGER", "less").current_dir(&suite.tests_dir);
        apply_deterministic_runtime_env(&mut cmd);
        apply_jq_tool_compat_env(&mut cmd);
        if let Some(color) = suite.color_compat {
            cmd.env("ZQ_COLOR_COMPAT", color);
        }
        run_cmd_collect_failure(&mut cmd, &ctx, timeout, &mut failures);
    }
    failures
}

fn run_test_files_sequential(
    test_files: Vec<PathBuf>,
    modules_dir: &Path,
    tests_dir: &Path,
    timeout: Duration,
    color_compat: Option<&str>,
) -> Vec<String> {
    let total = test_files.len();
    eprintln!("running {total} jq run-tests files sequentially");
    let mut failures = Vec::new();

    for tf in test_files {
        let ctx = format!("run-tests via zq for {}", tf.display());
        let mut cmd = Command::new(zq_bin());
        cmd.arg("-L")
            .arg(modules_dir)
            .arg("--run-tests")
            .arg(&tf)
            .env("PAGER", "less")
            .current_dir(tests_dir);
        apply_deterministic_runtime_env(&mut cmd);
        apply_jq_tool_compat_env(&mut cmd);
        if let Some(color) = color_compat {
            cmd.env("ZQ_COLOR_COMPAT", color);
        }
        run_cmd_collect_failure(&mut cmd, &ctx, timeout, &mut failures);
    }
    failures
}

fn run_cmd_collect_failure(
    cmd: &mut Command,
    ctx: &str,
    timeout: Duration,
    failures: &mut Vec<String>,
) {
    match run_cmd_capture_timeout(cmd, timeout, ctx) {
        Ok(out) if out.status.success() => {}
        Ok(out) => failures.push(format!(
            "{}\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
            ctx,
            out.status.code(),
            summarize_output(&out.stdout),
            summarize_output(&out.stderr)
        )),
        Err(err) => failures.push(err),
    }
}

fn collect_test_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let rd = std::fs::read_dir(&current).expect("read tests dir");
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|x| x.to_str()) == Some("test") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn run_jq_oracle_compat_cases(root: &Path) -> Vec<String> {
    let path = root.join("tests/compat/jq-cases.yaml");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read compat cases {}: {e}", path.display()));
    let suite: CompatCasesFile = serde_yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse compat cases {}: {e}", path.display()));
    assert!(!suite.cases.is_empty(), "compat case list is empty");
    let mut failures = Vec::new();
    eprintln!("running jq oracle compat cases from {}", path.display());

    for case in suite.cases {
        let jq = run_filter_binary(&jq_bin(), &case.query, &case.input_json, case.null_input);
        let zq = run_filter_binary(zq_bin(), &case.query, &case.input_json, case.null_input);

        if jq.status.code() != zq.status.code() {
            failures.push(format!(
                "exit code mismatch for case `{}`\nquery: {}\n jq={:?}\n zq={:?}",
                case.id,
                case.query,
                jq.status.code(),
                zq.status.code()
            ));
            continue;
        }

        let jq_stderr = normalize_stderr(&jq.stderr);
        let zq_stderr = normalize_stderr(&zq.stderr);
        if jq_stderr != zq_stderr {
            failures.push(format!(
                "stderr mismatch for case `{}`\nquery: {}\njq stderr:\n{}\nzq stderr:\n{}",
                case.id, case.query, jq_stderr, zq_stderr
            ));
            continue;
        }

        if !jq.status.success() {
            failures.push(format!(
                "jq failed for case `{}`\nquery: {}\nstdout:\n{}\nstderr:\n{}",
                case.id,
                case.query,
                summarize_output(&jq.stdout),
                summarize_output(&jq.stderr)
            ));
            continue;
        }
        if !zq.status.success() {
            failures.push(format!(
                "zq failed for case `{}`\nquery: {}\nstdout:\n{}\nstderr:\n{}",
                case.id,
                case.query,
                summarize_output(&zq.stdout),
                summarize_output(&zq.stderr)
            ));
            continue;
        }

        let jq_out = parse_json_lines(&jq.stdout, "jq", &case.id, &case.query);
        let zq_out = parse_json_lines(&zq.stdout, "zq", &case.id, &case.query);
        if zq_out != jq_out {
            failures.push(format!(
                "jq compatibility mismatch for case `{}`\nquery: {}\nexpected(jq): {:?}\nactual(zq): {:?}",
                case.id, case.query, jq_out, zq_out
            ));
        }
    }
    failures
}

fn run_filter_binary(binary: &str, query: &str, input_json: &str, null_input: bool) -> Output {
    let mut cmd = Command::new(binary);
    cmd.arg("-c");
    if null_input {
        cmd.arg("-n");
    }
    cmd.arg("--").arg(query).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    apply_deterministic_runtime_env(&mut cmd);
    if binary == zq_bin() {
        apply_jq_tool_compat_env(&mut cmd);
    }

    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn `{binary}` for query `{query}`: {e}"));
    if !null_input {
        let mut stdin = child.stdin.take().expect("child stdin");
        stdin.write_all(input_json.as_bytes()).unwrap_or_else(|e| {
            panic!("failed to write stdin for `{binary}` and query `{query}`: {e}")
        });
        stdin.write_all(b"\n").unwrap_or_else(|e| {
            panic!("failed to write stdin newline for `{binary}` and query `{query}`: {e}")
        });
    }

    child
        .wait_with_output()
        .unwrap_or_else(|e| panic!("failed to wait `{binary}` for query `{query}`: {e}"))
}

fn parse_json_lines(stdout: &[u8], tool: &str, case_id: &str, query: &str) -> Vec<JsonValue> {
    let text = String::from_utf8_lossy(stdout).replace("\r\n", "\n");
    if text.trim().is_empty() {
        return Vec::new();
    }

    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<JsonValue>(line).unwrap_or_else(|e| {
                panic!(
                    "{tool} returned non-JSON line in case `{case_id}`\nquery: {query}\nline: {line}\nerror: {e}"
                )
            })
        })
        .collect()
}

fn summarize_output(bytes: &[u8]) -> String {
    const MAX_CHARS: usize = 6000;
    const MAX_LINES: usize = 120;

    let text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let lines = text.lines().collect::<Vec<_>>();
    if text.len() <= MAX_CHARS && lines.len() <= MAX_LINES {
        return text;
    }
    let tail = lines.iter().rev().take(MAX_LINES).rev().copied().collect::<Vec<_>>().join("\n");
    let mut trimmed = tail;
    if trimmed.len() > MAX_CHARS {
        trimmed = trimmed[trimmed.len().saturating_sub(MAX_CHARS)..].to_string();
    }
    format!(
        "[truncated output: total_lines={}, total_chars={}]\n{}",
        lines.len(),
        text.len(),
        trimmed
    )
}

fn apply_deterministic_runtime_env(cmd: &mut Command) {
    cmd.env("LC_ALL", "C").env("LANG", "C").env("TZ", "UTC");
}

fn apply_jq_tool_compat_env(cmd: &mut Command) {
    cmd.env("ZQ_COMPAT_TOOL", "jq");
}

fn normalize_stderr(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr).replace("\r\n", "\n").trim().to_string()
}
