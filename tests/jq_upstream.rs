use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn zq_bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn run_cmd(cmd: &mut Command, ctx: &str, timeout: Duration) {
    let out = run_cmd_capture_timeout(cmd, timeout, ctx).unwrap_or_else(|err| panic!("{err}"));
    assert!(
        out.status.success(),
        "{ctx}\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
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
    let stdout_writer = stdout_file
        .reopen()
        .map_err(|e| format!("{ctx}\nstdout reopen failed: {e}"))?;
    let stderr_writer = stderr_file
        .reopen()
        .map_err(|e| format!("{ctx}\nstderr reopen failed: {e}"))?;

    let mut child = cmd
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::from(stderr_writer))
        .spawn()
        .map_err(|e| format!("{ctx}\nspawn failed: {e}"))?;

    let start = Instant::now();
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("{ctx}\ntry_wait failed: {e}"))?
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
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
        thread::sleep(Duration::from_millis(200));
    };

    let (stdout, stderr) = read_output_files(&stdout_path, &stderr_path)?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
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
    let help = Command::new(zq_bin())
        .arg("--help")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();
    if !help.contains("--run-tests") {
        eprintln!("skip jq upstream suite: zq does not expose --run-tests mode");
        return;
    }

    let run_tests_timeout = read_timeout("ZQ_JQ_RUN_TESTS_TIMEOUT_SECS", 900);
    let shtest_timeout = read_timeout("ZQ_JQ_SHTEST_TIMEOUT_SECS", 3600);
    if !run_local_jq_clones(run_tests_timeout, shtest_timeout) {
        eprintln!("skip jq upstream suite: local clones are required at .tmp/jq and/or .tmp/jq171");
    }
}

fn read_timeout(env_name: &str, default_secs: u64) -> Duration {
    let secs = std::env::var(env_name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

fn run_local_jq_clones(run_tests_timeout: Duration, shtest_timeout: Duration) -> bool {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let clones = [root.join(".tmp/jq"), root.join(".tmp/jq171")];
    let skip_shtest = std::env::var("ZQ_JQ_SKIP_SHTEST").ok().as_deref() == Some("1");
    let mut ran_any = false;

    for clone_root in clones {
        let tests_dir = clone_root.join("tests");
        if !tests_dir.is_dir() {
            continue;
        }
        let test_files = collect_test_files(&tests_dir);
        if test_files.is_empty() {
            continue;
        }

        ran_any = true;
        let modules_dir = tests_dir.join("modules");
        let is_jq171 = clone_root.file_name().and_then(|n| n.to_str()) == Some("jq171");
        eprintln!("running local jq suite from {}", clone_root.display());
        run_test_files_sequential(
            test_files,
            &modules_dir,
            &tests_dir,
            run_tests_timeout,
            is_jq171,
        );

        let shtest = tests_dir.join("shtest");
        if skip_shtest {
            eprintln!(
                "skip local jq shtest for {}: set ZQ_JQ_SKIP_SHTEST=0 (or unset) to run",
                clone_root.display()
            );
            continue;
        }
        let run_jq171_shtest = std::env::var("ZQ_JQ_RUN_JQ171_SHTEST").ok().as_deref() == Some("1");
        if is_jq171 && !run_jq171_shtest {
            eprintln!(
                "skip local jq171 shtest for {}: set ZQ_JQ_RUN_JQ171_SHTEST=1 to run",
                clone_root.display()
            );
            continue;
        }
        if !shtest.is_file() {
            continue;
        }
        let ctx = format!("run local jq shtest via zq for {}", clone_root.display());
        let mut cmd = Command::new("sh");
        cmd.arg(&shtest)
            .env("JQ", zq_bin())
            .env("PAGER", "less")
            .current_dir(&tests_dir);
        if is_jq171 {
            cmd.env("ZQ_COLOR_COMPAT", "jq171");
        }
        run_cmd(&mut cmd, &ctx, shtest_timeout);
    }

    ran_any
}

fn run_test_files_sequential(
    test_files: Vec<PathBuf>,
    modules_dir: &Path,
    tests_dir: &Path,
    timeout: Duration,
    is_jq171: bool,
) {
    let total = test_files.len();
    eprintln!("running {total} jq run-tests files sequentially");

    for tf in test_files {
        let ctx = format!("run-tests via zq for {}", tf.display());
        let mut cmd = Command::new(zq_bin());
        cmd.arg("-L")
            .arg(modules_dir)
            .arg("--run-tests")
            .arg(&tf)
            .env("PAGER", "less")
            .current_dir(tests_dir);
        if is_jq171 {
            cmd.env("ZQ_COLOR_COMPAT", "jq171");
        }
        run_cmd(&mut cmd, &ctx, timeout);
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
