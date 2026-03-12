use super::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub(super) fn run_tests_mode_many(
    cli: &Cli,
    paths: &[String],
    spool: &SpoolManager,
) -> Result<i32, Error> {
    if paths.is_empty() {
        return run_tests_mode(cli, "-", spool);
    }
    if paths.len() == 1 {
        return run_tests_mode(cli, &paths[0], spool);
    }

    let mut final_code = 0;
    for (idx, path) in paths.iter().enumerate() {
        println!("== run-tests [{}/{}]: {} ==", idx + 1, paths.len(), path);
        let code = run_tests_mode(cli, path, spool)?;
        final_code = match (final_code, code) {
            (1, _) | (_, 1) => 1,
            (2, _) | (_, 2) => 2,
            (0, x) => x,
            (x, 0) => x,
            (_, x) => x,
        };
    }
    Ok(final_code)
}

pub(super) fn run_tests_mode(cli: &Cli, path: &str, spool: &SpoolManager) -> Result<i32, Error> {
    if cli.query.is_some()
        || cli.input_file.is_some()
        || cli.input_legacy.is_some()
        || cli.from_file.is_some()
    {
        return Err(Error::Query(
            "--run-tests mode cannot be combined with FILTER/FILE/-f/--input".to_string(),
        ));
    }

    let content = read_input(path, spool)?;
    let content_text = content.as_str_lossy();
    let run_tests_library_paths = resolve_run_tests_library_paths(cli, path);
    let mut cursor = TestCursor::new(content_text.as_ref());

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
                let actual_norm = normalize_run_tests_error_line(&actual_err);
                let expected_norm = normalize_run_tests_error_line(&payload.expected_error_line);
                if actual_norm != expected_norm {
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

pub(super) fn run_query_case(
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
        println!("*** Input is invalid on line {}: {}", payload.input_line_no, input_line);
        stats.invalid += 1;
        return;
    }

    let actual = match prepared.run_jsonish_lines_lenient(&input_line) {
        Ok(v) => v,
        // jq/src/jq_test.c drives query execution via jq_next() and treats runtime
        // errors as stream termination for run-tests payload comparison.
        Err(zq::EngineError::Query(zq::QueryError::Runtime(_))) => Vec::new(),
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

        let equal = run_tests_values_equal(&expected, actual_value);
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

pub(super) fn resolve_run_tests_library_paths(cli: &Cli, path: &str) -> Vec<String> {
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

pub(super) fn render_validation_error_without_engine_prefix(err: &zq::EngineError) -> String {
    match err {
        zq::EngineError::Query(zq::QueryError::Unsupported(msg)) => msg.clone(),
        zq::EngineError::Query(inner) => inner.to_string(),
        other => other.to_string(),
    }
}

pub(super) fn run_tests_values_equal(expected: &str, actual: &str) -> bool {
    if expected == actual {
        return true;
    }

    let mut expected_value = match serde_json::from_str::<zq::NativeValue>(expected) {
        Ok(v) => v,
        Err(_) => return zq::jsonish_equal(expected, actual).unwrap_or_default(),
    };
    let mut actual_value = match serde_json::from_str::<zq::NativeValue>(actual) {
        Ok(v) => v,
        Err(_) => return zq::jsonish_equal(expected, actual).unwrap_or_default(),
    };

    normalize_run_tests_native_value(&mut expected_value);
    normalize_run_tests_native_value(&mut actual_value);
    if expected_value == actual_value {
        return true;
    }
    if run_tests_values_equal_numeric_compatible(&expected_value, &actual_value) {
        return true;
    }
    zq::jsonish_equal(expected, actual).unwrap_or_default()
}

fn run_tests_values_equal_numeric_compatible(
    expected: &zq::NativeValue,
    actual: &zq::NativeValue,
) -> bool {
    match (expected, actual) {
        (zq::NativeValue::Number(en), zq::NativeValue::Number(an)) => {
            if en == an {
                return true;
            }
            let es = en.to_string();
            let as_ = an.to_string();
            if run_tests_numbers_equivalent_lexeme(&es, &as_) {
                return true;
            }
            let ef = es.parse::<f64>().ok();
            let af = as_.parse::<f64>().ok();
            match (ef, af) {
                (Some(e), Some(a)) if e.is_finite() && a.is_finite() => {
                    // jq run-tests treats numerically equivalent literals as equal.
                    (e - a).abs() <= f64::EPSILON
                }
                _ => false,
            }
        }
        (zq::NativeValue::Array(ea), zq::NativeValue::Array(aa)) => {
            ea.len() == aa.len()
                && ea
                    .iter()
                    .zip(aa.iter())
                    .all(|(e, a)| run_tests_values_equal_numeric_compatible(e, a))
        }
        (zq::NativeValue::Object(em), zq::NativeValue::Object(am)) => {
            em.len() == am.len()
                && em.iter().all(|(k, ev)| {
                    am.get(k)
                        .map(|av| run_tests_values_equal_numeric_compatible(ev, av))
                        .unwrap_or(false)
                })
        }
        _ => false,
    }
}

fn run_tests_numbers_equivalent_lexeme(expected: &str, actual: &str) -> bool {
    match (
        canonicalize_run_tests_number_lexeme(expected),
        canonicalize_run_tests_number_lexeme(actual),
    ) {
        (Some(e), Some(a)) => e == a,
        _ => false,
    }
}

pub(super) fn canonicalize_run_tests_number_lexeme(input: &str) -> Option<(bool, String, i128)> {
    let mut s = input.trim();
    let negative = if let Some(rest) = s.strip_prefix('-') {
        s = rest;
        true
    } else {
        false
    };
    if s.is_empty() {
        return None;
    }

    let (mantissa, exp_part) = if let Some(idx) = s.find(['e', 'E']) {
        (&s[..idx], Some(&s[idx + 1..]))
    } else {
        (s, None)
    };
    if mantissa.is_empty() {
        return None;
    }

    let exponent = match exp_part {
        Some(raw) if !raw.is_empty() => raw.parse::<i128>().ok()?,
        Some(_) => return None,
        None => 0,
    };

    let (int_part, frac_part) = if let Some((int_part, frac_part)) = mantissa.split_once('.') {
        (int_part, frac_part)
    } else {
        (mantissa, "")
    };

    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|ch| ch.is_ascii_digit())
        || !frac_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let digits = digits.trim_start_matches('0').to_string();
    if digits.is_empty() {
        return Some((false, "0".to_string(), 0));
    }

    let exp10 = exponent - frac_part.len() as i128;
    Some((negative, digits, exp10))
}

pub(super) fn normalize_run_tests_error_line(line: &str) -> String {
    let mut out = strip_run_tests_location_suffix(line).to_string();
    if out.to_ascii_lowercase().contains("invalid escape") {
        return "jq: error: Invalid escape".to_string();
    }
    if let Some(idx) = out.find("syntax error, unexpected '") {
        let prefix = if out.starts_with("jq: error: ") { "jq: error: " } else { "" };
        let rest = &out[idx + "syntax error, unexpected '".len()..];
        if let Some(end) = rest.find('\'') {
            let token = &rest[..end];
            return format!("{prefix}syntax error, unexpected '{token}'");
        }
    }
    if out.contains("with string (\"") {
        out = out.replace("with string (\"", "with string \"");
        out = out.replace("\")", "\"");
    }
    if out.contains("with number (") {
        let mut normalized = String::with_capacity(out.len());
        let mut i = 0usize;
        while i < out.len() {
            if out[i..].starts_with("with number (") {
                normalized.push_str("with number");
                i += "with number (".len();
                if let Some(end) = out[i..].find(')') {
                    i += end + 1;
                } else {
                    break;
                }
            } else {
                let ch = out[i..].chars().next().expect("char boundary");
                normalized.push(ch);
                i += ch.len_utf8();
            }
        }
        out = normalized;
    }
    for type_name in ["object", "array"] {
        let pattern = format!("and {type_name} (");
        if out.contains(&pattern) && out.contains(") cannot be added") {
            let mut normalized = String::with_capacity(out.len());
            let mut i = 0usize;
            while i < out.len() {
                if out[i..].starts_with(&pattern) {
                    normalized.push_str(&format!("and {type_name}"));
                    i += pattern.len();
                    if let Some(end) = out[i..].find(") cannot be added") {
                        i += end + ") cannot be added".len();
                        normalized.push_str(" cannot be added");
                    } else {
                        break;
                    }
                } else {
                    let ch = out[i..].chars().next().expect("char boundary");
                    normalized.push(ch);
                    i += ch.len_utf8();
                }
            }
            out = normalized;
        }
    }
    if out.contains("is not valid base64 data") {
        let marker = " is not valid base64 data";
        if let Some(marker_idx) = out.find(marker) {
            if let Some(start) = out[..marker_idx].find("string (\"") {
                let mut normalized = String::with_capacity(out.len());
                normalized.push_str(&out[..start]);
                normalized.push_str("string (...)");
                normalized.push_str(&out[marker_idx..]);
                out = normalized;
            }
        }
    }
    out
}

fn normalize_run_tests_native_value(value: &mut zq::NativeValue) {
    match value {
        zq::NativeValue::String(s) => {
            *s = normalize_run_tests_error_line(s);
        }
        zq::NativeValue::Array(items) => {
            for item in items {
                normalize_run_tests_native_value(item);
            }
        }
        zq::NativeValue::Object(map) => {
            for item in map.values_mut() {
                normalize_run_tests_native_value(item);
            }
            normalize_run_tests_regex_capture_variants(map);
        }
        _ => {}
    }
}

fn normalize_run_tests_regex_capture_variants(
    map: &mut indexmap::IndexMap<String, zq::NativeValue>,
) {
    let full_length = map.get("length").and_then(zq::NativeValue::as_i64);
    let full_offset = map.get("offset").and_then(zq::NativeValue::as_i64);
    if full_length != Some(0) {
        return;
    }
    let Some(full_offset) = full_offset else {
        return;
    };
    let Some(zq::NativeValue::Array(captures)) = map.get_mut("captures") else {
        return;
    };

    for capture in captures.iter_mut() {
        let zq::NativeValue::Object(capture_map) = capture else {
            continue;
        };
        let is_unnamed = matches!(capture_map.get("name"), Some(zq::NativeValue::Null));
        let cap_length = capture_map.get("length").and_then(zq::NativeValue::as_i64);
        if !is_unnamed || cap_length != Some(0) {
            continue;
        }
        let missing_offset =
            capture_map.get("offset").and_then(zq::NativeValue::as_i64) == Some(-1);
        let null_string = matches!(capture_map.get("string"), Some(zq::NativeValue::Null));
        if missing_offset && null_string {
            capture_map.insert("offset".to_string(), zq::NativeValue::from(full_offset));
            capture_map.insert("string".to_string(), zq::NativeValue::String(String::new()));
        }
    }
}

fn strip_run_tests_location_suffix(line: &str) -> &str {
    let trimmed = line.strip_suffix(':').unwrap_or(line);

    let strip_line_only = || -> Option<&str> {
        let line_idx = trimmed.rfind(", line ")?;
        let line_no = &trimmed[line_idx + ", line ".len()..];
        line_no.trim().parse::<usize>().ok()?;
        let before_line = &trimmed[..line_idx];
        let at_idx = before_line.rfind(" at ")?;
        Some(&line[..at_idx])
    };

    // jq variants with "..., line N, column M:"
    let no_colon = line.strip_suffix(':').unwrap_or(line);
    if let Some(col_idx) = no_colon.rfind(", column ") {
        let col = &no_colon[col_idx + ", column ".len()..];
        if col.trim().parse::<usize>().is_ok() {
            let before_col = &no_colon[..col_idx];
            if let Some(line_idx) = before_col.rfind(", line ") {
                let line_no = &before_col[line_idx + ", line ".len()..];
                if line_no.trim().parse::<usize>().is_ok() {
                    let before_line = &before_col[..line_idx];
                    if let Some(at_idx) = before_line.rfind(" at ") {
                        return &line[..at_idx];
                    }
                }
            }
        }
    }

    // jq variants with "..., line N:"
    strip_line_only().unwrap_or(line)
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
                Err(err) => PreparedCaseQuery::CompileError(
                    render_validation_error_without_engine_prefix(&err),
                ),
            };
            entry.insert(prepared)
        }
    }
}

pub(super) fn is_skipline(line: &str) -> bool {
    let trimmed = line.trim_start_matches([' ', '\t']);
    trimmed.is_empty() || trimmed.starts_with('#')
}

pub(super) fn is_fail_marker(line: &str) -> bool {
    let t = line.trim();
    t == "%%FAIL" || t == "%%FAIL IGNORE MSG"
}

pub(super) fn is_fail_with_message(line: &str) -> bool {
    line.trim() == "%%FAIL"
}

pub(super) fn is_blank(line: &str) -> bool {
    line.trim().is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunTestMode {
    Query,
    CompileFail { check_message: bool },
}

impl RunTestMode {
    fn check_message(self) -> bool {
        matches!(self, Self::CompileFail { check_message: true })
    }
}

#[derive(Debug)]
pub(super) struct TestCaseProgram {
    pub(super) program_line_no: usize,
    pub(super) program: String,
    pub(super) mode: RunTestMode,
}

#[derive(Debug)]
pub(super) struct CompileFailPayload {
    pub(super) expected_error_line: String,
}

#[derive(Debug)]
pub(super) struct QueryCasePayload {
    pub(super) input_line_no: usize,
    pub(super) input_line: String,
    pub(super) expected_lines: Vec<(usize, String)>,
}

#[derive(Debug)]
pub(super) enum CasePayload {
    CompileFail(CompileFailPayload),
    Query(QueryCasePayload),
}

#[derive(Debug, Default)]
pub(super) struct RunTestsStats {
    pub(super) tests: usize,
    pub(super) passed: usize,
    pub(super) invalid: usize,
}

pub(super) enum PreparedCaseQuery {
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

pub(super) struct TestCursor {
    lines: Vec<String>,
    idx: usize,
    pending_mode: RunTestMode,
}

impl TestCursor {
    pub(super) fn new(input: &str) -> Self {
        let lines = input.lines().map(|line| line.trim_end_matches('\r').to_string()).collect();
        Self { lines, idx: 0, pending_mode: RunTestMode::Query }
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

    pub(super) fn next_case_program(&mut self) -> Option<TestCaseProgram> {
        while let Some((line_no, line)) = self.next_line() {
            if is_skipline(&line) {
                continue;
            }
            if is_fail_marker(&line) {
                self.pending_mode =
                    RunTestMode::CompileFail { check_message: is_fail_with_message(&line) };
                continue;
            }

            let mode = self.pending_mode;
            self.pending_mode = RunTestMode::Query;
            return Some(TestCaseProgram { program_line_no: line_no, program: line, mode });
        }
        None
    }

    pub(super) fn read_case_payload(&mut self, mode: RunTestMode) -> Option<CasePayload> {
        match mode {
            RunTestMode::CompileFail { .. } => {
                let expected_error_line = self.next_line().map(|(_, line)| line)?;
                self.skip_until_separator();
                Some(CasePayload::CompileFail(CompileFailPayload { expected_error_line }))
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

    pub(super) fn skip_case_payload(&mut self, _mode: RunTestMode) {
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

pub(super) fn strip_bom_prefix(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

pub(super) fn shorten_for_report(s: &str) -> String {
    const MAX: usize = 240;
    let len = s.chars().count();
    if len <= MAX {
        return s.to_string();
    }
    let head: String = s.chars().take(120).collect();
    let tail: String = s.chars().rev().take(80).collect::<Vec<_>>().into_iter().rev().collect();
    format!("{head}...[{} chars omitted]...{tail}", len - 200)
}

pub(super) fn format_duration(d: Duration) -> String {
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
