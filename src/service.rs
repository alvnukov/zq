use clap::Parser;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::time::{Duration, Instant};

use crate::cli::{Cli, OutputFormat};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("query: {0}")]
    Query(String),
}

pub fn run() -> Result<i32, Error> {
    let cli = Cli::parse();
    run_with(cli)
}

fn run_with(cli: Cli) -> Result<i32, Error> {
    if let Some(path) = cli.run_tests.as_deref() {
        return run_tests_mode(&cli, path);
    }

    let query = cli
        .query
        .as_deref()
        .ok_or_else(|| Error::Query("missing jq filter expression".to_string()))?;
    let input_path = resolve_input_path(&cli)?;
    let doc_mode = zq::parse_doc_mode(&cli.doc_mode, cli.doc_index)
        .map_err(|e| Error::Query(e.to_string()))?;
    let has_explicit_input_path = cli.input_file.is_some() || cli.input_legacy.is_some();

    if matches!(cli.output_format, OutputFormat::Yaml) && cli.raw_output {
        return Err(Error::Query(
            "--raw-output is supported only with --output-format=json".to_string(),
        ));
    }
    if matches!(cli.output_format, OutputFormat::Yaml) && cli.compact {
        return Err(Error::Query(
            "--compact is supported only with --output-format=json".to_string(),
        ));
    }

    let input = if cli.null_input && !has_explicit_input_path {
        String::new()
    } else {
        read_input(&input_path)?
    };

    let out = if cli.raw_input || cli.slurp || cli.null_input {
        let inputs = build_custom_input_stream(&cli, &input, doc_mode)
            .map_err(|e| Error::Query(render_engine_error("jq", &input, e)))?;
        zq::run_jq_stream_with_paths_options(
            query,
            inputs,
            &cli.library_path,
            zq::EngineRunOptions {
                null_input: cli.null_input,
            },
        )
        .map_err(|e| Error::Query(render_engine_error("jq", &input, e)))?
    } else {
        let options = zq::QueryOptions {
            doc_mode,
            library_path: cli.library_path.clone(),
        };
        zq::run_jq(query, &input, options)
            .map_err(|e| Error::Query(render_engine_error("jq", &input, e)))?
    };

    let rendered = match cli.output_format {
        OutputFormat::Json => zq::format_output_json_lines(&out, cli.compact, cli.raw_output)
            .map_err(|e| Error::Query(e.to_string()))?,
        OutputFormat::Yaml => {
            zq::format_output_yaml_documents(&out).map_err(|e| Error::Query(e.to_string()))?
        }
    };

    if !rendered.is_empty() {
        println!("{rendered}");
    }

    if cli.exit_status {
        return Ok(exit_status_from_outputs(&out));
    }
    Ok(0)
}

fn build_custom_input_stream(
    cli: &Cli,
    input: &str,
    doc_mode: zq::DocMode,
) -> Result<Vec<JsonValue>, zq::EngineError> {
    if cli.raw_input {
        if cli.slurp {
            return Ok(vec![JsonValue::String(input.to_string())]);
        }
        return Ok(raw_input_lines(input)
            .into_iter()
            .map(JsonValue::String)
            .collect());
    }

    let mut docs = zq::parse_jq_input_values(input, doc_mode, "jq")?;
    if cli.slurp {
        docs = vec![JsonValue::Array(docs)];
    }
    Ok(docs)
}

fn raw_input_lines(input: &str) -> Vec<String> {
    input
        .split_terminator('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect()
}

fn exit_status_from_outputs(outputs: &[JsonValue]) -> i32 {
    let Some(last) = outputs.last() else {
        return 4;
    };
    match last {
        JsonValue::Null => 1,
        JsonValue::Bool(false) => 1,
        _ => 0,
    }
}

fn run_tests_mode(cli: &Cli, path: &str) -> Result<i32, Error> {
    if cli.query.is_some() || cli.input_file.is_some() || cli.input_legacy.is_some() {
        return Err(Error::Query(
            "--run-tests mode cannot be combined with FILTER/FILE/--input".to_string(),
        ));
    }

    let content = read_input(path)?;
    let run_tests_library_paths = resolve_run_tests_library_paths(cli, path);
    let mut cursor = TestCursor::new(&content);

    let tests_to_skip = cli.run_tests_skip.unwrap_or(0);
    let mut skip_remaining = tests_to_skip;
    let tests_to_take = cli.run_tests_take;
    let mut take_remaining = tests_to_take;
    let mut skip_reported = false;

    let mut stats = RunTestsStats::default();
    let mut compile_cache: HashMap<String, PreparedCaseQuery> = HashMap::new();
    let mut timings = Vec::new();
    let run_started = Instant::now();

    while let Some(case) = cursor.next_case_program() {
        if skip_remaining > 0 {
            skip_remaining -= 1;
            cursor.skip_case_payload(case.mode);
            continue;
        }
        if !skip_reported && tests_to_skip > 0 {
            println!("Skipped {tests_to_skip} tests");
            skip_reported = true;
        }

        if let Some(rem) = take_remaining {
            if rem == 0 {
                println!(
                    "Hit the number of tests limit ({}), breaking",
                    tests_to_take.unwrap_or(0)
                );
                break;
            }
            take_remaining = Some(rem.saturating_sub(1));
        }

        stats.tests += 1;
        println!(
            "Test #{}: '{}' at line number {}",
            stats.tests + tests_to_skip,
            case.program,
            case.program_line_no
        );

        let payload = match cursor.read_case_payload(case.mode) {
            Some(v) => v,
            None => {
                stats.invalid += 1;
                break;
            }
        };

        let case_started = Instant::now();
        let passed_before = stats.passed;
        let invalid_before = stats.invalid;

        match payload {
            CasePayload::CompileFail(payload) => {
                run_compile_fail_case(
                    &case,
                    payload,
                    &run_tests_library_paths,
                    &mut compile_cache,
                    &mut stats,
                );
            }
            CasePayload::Query(payload) => {
                run_query_case(
                    &case,
                    payload,
                    &run_tests_library_paths,
                    &mut compile_cache,
                    &mut stats,
                );
            }
        }

        let elapsed = case_started.elapsed();
        let passed = stats.passed > passed_before && stats.invalid == invalid_before;
        let verdict = if passed { "PASS" } else { "FAIL" };
        println!("  -> {verdict} in {}", format_duration(elapsed));
        timings.push(TestTiming {
            number: stats.tests + tests_to_skip,
            line: case.program_line_no,
            program: case.program.clone(),
            duration: elapsed,
            passed,
        });
    }

    let total_skipped = tests_to_skip.saturating_sub(skip_remaining);
    println!(
        "{} of {} tests passed ({} malformed, {} skipped)",
        stats.passed, stats.tests, stats.invalid, total_skipped
    );
    println!("Total run time: {}", format_duration(run_started.elapsed()));
    print_heavy_cases(&timings);

    if skip_remaining > 0 {
        println!("WARN: skipped past the end of file, exiting with status 2");
        return Ok(2);
    }
    if stats.passed != stats.tests {
        return Ok(1);
    }
    Ok(0)
}

fn run_compile_fail_case(
    case: &TestCaseProgram,
    payload: CompileFailPayload,
    library_paths: &[String],
    compile_cache: &mut HashMap<String, PreparedCaseQuery>,
    stats: &mut RunTestsStats,
) {
    let prepared = get_or_prepare_case_query(&case.program, library_paths, compile_cache);
    match prepared {
        PreparedCaseQuery::Ready(_) => {
            println!(
                "*** Test program compiled that should not have at line {}: {}",
                case.program_line_no, case.program
            );
            stats.invalid += 1;
            return;
        }
        PreparedCaseQuery::CompileError(rendered) => {
            if case.mode.check_message() {
                let actual_err = format!("jq: error: {rendered}");
                if actual_err != payload.expected_error_line {
                    println!(
                        "*** Erroneous test program failed with wrong message ({}) at line {}: {}",
                        actual_err, case.program_line_no, case.program
                    );
                    stats.invalid += 1;
                    return;
                }
            }
        }
    }

    stats.passed += 1;
}

fn run_query_case(
    case: &TestCaseProgram,
    payload: QueryCasePayload,
    library_paths: &[String],
    compile_cache: &mut HashMap<String, PreparedCaseQuery>,
    stats: &mut RunTestsStats,
) {
    let prepared = get_or_prepare_case_query(&case.program, library_paths, compile_cache);
    let PreparedCaseQuery::Ready(prepared) = prepared else {
        println!(
            "*** Test program failed to compile at line {}: {}",
            case.program_line_no, case.program
        );
        stats.invalid += 1;
        return;
    };

    let input_line = strip_bom_prefix(&payload.input_line).to_string();
    if zq::normalize_jsonish_line(&input_line).is_err() {
        println!(
            "*** Input is invalid on line {}: {}",
            payload.input_line_no, input_line
        );
        stats.invalid += 1;
        return;
    }

    let actual = match prepared.run_jsonish_lines_lenient(&input_line) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "*** Test program failed to run at line {}: {} ({})",
                case.program_line_no, case.program, e
            );
            stats.invalid += 1;
            return;
        }
    };

    let mut pass = true;
    let mut idx = 0usize;
    for (expected_line_no, expected_line) in payload.expected_lines {
        let expected = match zq::normalize_jsonish_line(&expected_line) {
            Ok(v) => v,
            Err(_) => {
                println!(
                    "*** Expected result is invalid on line {}: {}",
                    expected_line_no, expected_line
                );
                stats.invalid += 1;
                continue;
            }
        };

        let Some(actual_value) = actual.get(idx) else {
            println!(
                "*** Insufficient results for test at line number {}: {}",
                expected_line_no, case.program
            );
            pass = false;
            break;
        };

        let equal = zq::jsonish_equal(&expected, actual_value).unwrap_or_default();
        if !equal {
            println!(
                "*** Expected {}, but got {} for test at line number {}: {}",
                shorten_for_report(&expected),
                shorten_for_report(actual_value),
                expected_line_no,
                case.program
            );
            pass = false;
        }
        idx += 1;
    }

    if pass {
        if let Some(extra) = actual.get(idx) {
            println!(
                "*** Superfluous result: {} for test at line number {}, {}",
                shorten_for_report(extra),
                case.program_line_no,
                case.program
            );
            pass = false;
        }
    }

    if pass {
        stats.passed += 1;
    }
}

fn read_input(path: &str) -> Result<String, Error> {
    if path == "-" {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        return Ok(s);
    }
    Ok(fs::read_to_string(path)?)
}

fn resolve_input_path(cli: &Cli) -> Result<String, Error> {
    if cli.input_file.is_some() && cli.input_legacy.is_some() {
        return Err(Error::Query(
            "input path is specified twice (use either positional FILE or --input)".to_string(),
        ));
    }
    if let Some(path) = &cli.input_file {
        return Ok(path.clone());
    }
    if let Some(path) = &cli.input_legacy {
        return Ok(path.clone());
    }
    Ok("-".to_string())
}

fn resolve_run_tests_library_paths(cli: &Cli, path: &str) -> Vec<String> {
    if !cli.library_path.is_empty() {
        return cli.library_path.clone();
    }
    if path == "-" {
        return Vec::new();
    }
    let mut out = Vec::new();
    if let Some(parent) = std::path::Path::new(path).parent() {
        let modules = parent.join("modules");
        if modules.is_dir() {
            out.push(modules.to_string_lossy().to_string());
        }
    }
    out
}

fn render_engine_error(tool: &str, input: &str, err: zq::EngineError) -> String {
    match err {
        zq::EngineError::Query(inner) => zq::format_query_error(tool, input, &inner),
        other => other.to_string(),
    }
}

fn render_validation_error_without_engine_prefix(err: &zq::EngineError) -> String {
    match err {
        zq::EngineError::Query(zq::QueryError::Unsupported(msg)) => msg.clone(),
        zq::EngineError::Query(inner) => inner.to_string(),
        other => other.to_string(),
    }
}

fn get_or_prepare_case_query<'a>(
    program: &str,
    library_paths: &[String],
    compile_cache: &'a mut HashMap<String, PreparedCaseQuery>,
) -> &'a PreparedCaseQuery {
    use std::collections::hash_map::Entry;

    match compile_cache.entry(program.to_string()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let prepared = match zq::prepare_jq_query_with_paths(program, library_paths) {
                Ok(compiled) => PreparedCaseQuery::Ready(compiled),
                Err(err) => {
                    PreparedCaseQuery::CompileError(render_validation_error_without_engine_prefix(
                        &err,
                    ))
                }
            };
            entry.insert(prepared)
        }
    }
}

fn is_skipline(line: &str) -> bool {
    let trimmed = line.trim_start_matches([' ', '\t']);
    trimmed.is_empty() || trimmed.starts_with('#')
}

fn is_fail_marker(line: &str) -> bool {
    let t = line.trim();
    t == "%%FAIL" || t == "%%FAIL IGNORE MSG"
}

fn is_fail_with_message(line: &str) -> bool {
    line.trim() == "%%FAIL"
}

fn is_blank(line: &str) -> bool {
    line.trim().is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunTestMode {
    Query,
    CompileFail { check_message: bool },
}

impl RunTestMode {
    fn check_message(self) -> bool {
        matches!(
            self,
            Self::CompileFail {
                check_message: true
            }
        )
    }
}

#[derive(Debug)]
struct TestCaseProgram {
    program_line_no: usize,
    program: String,
    mode: RunTestMode,
}

#[derive(Debug)]
struct CompileFailPayload {
    expected_error_line: String,
}

#[derive(Debug)]
struct QueryCasePayload {
    input_line_no: usize,
    input_line: String,
    expected_lines: Vec<(usize, String)>,
}

#[derive(Debug)]
enum CasePayload {
    CompileFail(CompileFailPayload),
    Query(QueryCasePayload),
}

#[derive(Debug, Default)]
struct RunTestsStats {
    tests: usize,
    passed: usize,
    invalid: usize,
}

enum PreparedCaseQuery {
    Ready(zq::PreparedJq),
    CompileError(String),
}

#[derive(Debug, Clone)]
struct TestTiming {
    number: usize,
    line: usize,
    program: String,
    duration: Duration,
    passed: bool,
}

struct TestCursor {
    lines: Vec<String>,
    idx: usize,
    pending_mode: RunTestMode,
}

impl TestCursor {
    fn new(input: &str) -> Self {
        let lines = input
            .lines()
            .map(|l| l.trim_end_matches('\r').to_string())
            .collect();
        Self {
            lines,
            idx: 0,
            pending_mode: RunTestMode::Query,
        }
    }

    fn next_line(&mut self) -> Option<(usize, String)> {
        if self.idx >= self.lines.len() {
            return None;
        }
        let line_no = self.idx + 1;
        let out = self.lines[self.idx].clone();
        self.idx += 1;
        Some((line_no, out))
    }

    fn next_case_program(&mut self) -> Option<TestCaseProgram> {
        while let Some((line_no, line)) = self.next_line() {
            if is_skipline(&line) {
                continue;
            }
            if is_fail_marker(&line) {
                self.pending_mode = RunTestMode::CompileFail {
                    check_message: is_fail_with_message(&line),
                };
                continue;
            }

            let mode = self.pending_mode;
            self.pending_mode = RunTestMode::Query;
            return Some(TestCaseProgram {
                program_line_no: line_no,
                program: line,
                mode,
            });
        }
        None
    }

    fn read_case_payload(&mut self, mode: RunTestMode) -> Option<CasePayload> {
        match mode {
            RunTestMode::CompileFail { .. } => {
                let expected_error_line = self.next_line().map(|(_, line)| line)?;
                self.skip_until_separator();
                Some(CasePayload::CompileFail(CompileFailPayload {
                    expected_error_line,
                }))
            }
            RunTestMode::Query => {
                let (input_line_no, input_line) = self.next_line()?;
                let mut expected_lines = Vec::new();
                while let Some((line_no, line)) = self.next_line() {
                    if is_skipline(&line) {
                        break;
                    }
                    expected_lines.push((line_no, line));
                }
                Some(CasePayload::Query(QueryCasePayload {
                    input_line_no,
                    input_line,
                    expected_lines,
                }))
            }
        }
    }

    fn skip_test_payload(&mut self) {
        while let Some((_line_no, line)) = self.next_line() {
            if is_blank(&line) {
                break;
            }
        }
    }

    fn skip_case_payload(&mut self, _mode: RunTestMode) {
        self.skip_test_payload();
    }

    fn skip_until_separator(&mut self) {
        while let Some((_line_no, line)) = self.next_line() {
            if is_blank(&line) {
                break;
            }
        }
    }
}

fn strip_bom_prefix(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

fn shorten_for_report(s: &str) -> String {
    const MAX: usize = 240;
    let len = s.chars().count();
    if len <= MAX {
        return s.to_string();
    }
    let head: String = s.chars().take(120).collect();
    let tail: String = s
        .chars()
        .rev()
        .take(80)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}...[{} chars omitted]...{tail}", len - 200)
}

fn format_duration(d: Duration) -> String {
    if d.as_secs() == 0 {
        return format!("{}ms", d.as_millis());
    }
    format!("{:.3}s", d.as_secs_f64())
}

fn print_heavy_cases(timings: &[TestTiming]) {
    if timings.is_empty() {
        return;
    }
    let mut sorted = timings.to_vec();
    sorted.sort_by(|a, b| b.duration.cmp(&a.duration));
    println!("Slowest cases (top 10):");
    for t in sorted.into_iter().take(10) {
        let verdict = if t.passed { "PASS" } else { "FAIL" };
        println!(
            "  #{} line {} [{}] {} {}",
            t.number,
            t.line,
            verdict,
            format_duration(t.duration),
            shorten_for_report(&t.program)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_parses_compile_fail_case_mode() {
        let mut cursor = TestCursor::new("%%FAIL\n@\nplaceholder\n\n.\nnull\nnull\n");
        let fail_case = cursor.next_case_program().expect("first case");
        assert!(matches!(
            fail_case.mode,
            RunTestMode::CompileFail {
                check_message: true
            }
        ));
        assert_eq!(fail_case.program, "@");

        let payload = cursor
            .read_case_payload(fail_case.mode)
            .expect("fail payload");
        match payload {
            CasePayload::CompileFail(payload) => {
                assert_eq!(payload.expected_error_line, "placeholder");
            }
            CasePayload::Query(_) => panic!("unexpected payload kind"),
        }

        let next_case = cursor.next_case_program().expect("next case");
        assert!(matches!(next_case.mode, RunTestMode::Query));
        assert_eq!(next_case.program, ".");
    }

    #[test]
    fn cursor_reads_query_payload_until_separator() {
        let mut cursor = TestCursor::new(".\n1\n1\n2\n\n");
        let case = cursor.next_case_program().expect("case");
        let payload = cursor.read_case_payload(case.mode).expect("payload");
        match payload {
            CasePayload::CompileFail(_) => panic!("unexpected payload kind"),
            CasePayload::Query(payload) => {
                assert_eq!(payload.input_line_no, 2);
                assert_eq!(payload.input_line, "1");
                assert_eq!(
                    payload.expected_lines,
                    vec![(3usize, "1".to_string()), (4usize, "2".to_string())]
                );
            }
        }
    }

    #[test]
    fn cursor_skip_case_payload_moves_to_next_case() {
        let mut cursor = TestCursor::new(".\nnull\nnull\n\n.[0]\n[1,2]\n1\n\n");
        let first = cursor.next_case_program().expect("first");
        cursor.skip_case_payload(first.mode);

        let second = cursor.next_case_program().expect("second");
        assert_eq!(second.program, ".[0]");
        assert!(matches!(second.mode, RunTestMode::Query));
    }

    #[test]
    fn raw_input_lines_follow_jq_semantics() {
        assert_eq!(
            raw_input_lines("a\nb\nc\n"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert_eq!(
            raw_input_lines("a\r\nb\r\nc"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(raw_input_lines("").is_empty());
    }

    #[test]
    fn exit_status_contract_matches_jq() {
        assert_eq!(exit_status_from_outputs(&[]), 4);
        assert_eq!(exit_status_from_outputs(&[JsonValue::Null]), 1);
        assert_eq!(exit_status_from_outputs(&[JsonValue::Bool(false)]), 1);
        assert_eq!(exit_status_from_outputs(&[JsonValue::Bool(true)]), 0);
        assert_eq!(exit_status_from_outputs(&[JsonValue::Number(1.into())]), 0);
    }
}
