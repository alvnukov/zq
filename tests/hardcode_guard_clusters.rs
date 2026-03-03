use serde_json::Value as JsonValue;
use std::io::Write;
use std::process::{Command, Output, Stdio};
use zq::{run_jq_stream_with_paths_options, EngineRunOptions};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn run_lib(
    query: &str,
    input_stream: Vec<JsonValue>,
    null_input: bool,
) -> Result<Vec<JsonValue>, String> {
    run_jq_stream_with_paths_options(query, input_stream, &[], EngineRunOptions { null_input })
        .map_err(|e| e.to_string())
}

fn run_cli(query: &str, input_stream: &[JsonValue], null_input: bool) -> Output {
    let mut cmd = Command::new(bin());
    cmd.arg("-c");
    if null_input {
        cmd.arg("-n");
    }
    // Ensure filters starting with '-' are not parsed as CLI flags.
    cmd.arg("--")
        .arg(query)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn zq");
    if !null_input {
        let mut stdin = child.stdin.take().expect("stdin");
        for value in input_stream {
            let line = serde_json::to_string(value).expect("encode input json");
            stdin.write_all(line.as_bytes()).expect("write stdin");
            stdin.write_all(b"\n").expect("write newline");
        }
    }
    child.wait_with_output().expect("wait zq")
}

fn parse_cli_json_lines(stdout: &[u8]) -> Vec<JsonValue> {
    let text = String::from_utf8_lossy(stdout).replace("\r\n", "\n");
    if text.trim().is_empty() {
        return Vec::new();
    }
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<JsonValue>(line).expect("stdout json line"))
        .collect()
}

fn assert_success_expected(
    query: &str,
    input_stream: Vec<JsonValue>,
    null_input: bool,
    expected: Vec<JsonValue>,
) {
    let lib = run_lib(query, input_stream.clone(), null_input)
        .unwrap_or_else(|e| panic!("library failed for `{query}`: {e}"));
    assert_eq!(lib, expected, "library mismatch for `{query}`");

    let cli = run_cli(query, &input_stream, null_input);
    assert!(
        cli.status.success(),
        "cli failed for `{query}`\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cli.stdout),
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_values = parse_cli_json_lines(&cli.stdout);
    assert_eq!(cli_values, expected, "cli mismatch for `{query}`");
}

fn assert_success_expected_lib(
    query: &str,
    input_stream: Vec<JsonValue>,
    null_input: bool,
    expected: Vec<JsonValue>,
) {
    let lib = run_lib(query, input_stream, null_input)
        .unwrap_or_else(|e| panic!("library failed for `{query}`: {e}"));
    assert_eq!(lib, expected, "library mismatch for `{query}`");
}

fn assert_error_contains(
    query: &str,
    input_stream: Vec<JsonValue>,
    null_input: bool,
    needle: &str,
) {
    let lib = run_lib(query, input_stream.clone(), null_input).expect_err("library must fail");
    assert!(
        lib.contains(needle),
        "library error mismatch for `{query}`\nneedle: {needle}\nactual: {lib}"
    );

    let cli = run_cli(query, &input_stream, null_input);
    assert!(
        !cli.status.success(),
        "cli should fail for `{query}`\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cli.stdout),
        String::from_utf8_lossy(&cli.stderr)
    );
    let stderr = String::from_utf8_lossy(&cli.stderr);
    assert!(
        stderr.contains(needle),
        "cli error mismatch for `{query}`\nneedle: {needle}\nstderr:\n{stderr}"
    );
}

#[derive(Clone, Copy, Debug)]
enum NumTok {
    Int(i64),
    PosInf,
    NegInf,
    Nan,
}

impl NumTok {
    fn as_query(self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::PosInf => "infinite".to_string(),
            Self::NegInf => "-infinite".to_string(),
            Self::Nan => "nan".to_string(),
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(v) => v as f64,
            Self::PosInf => f64::INFINITY,
            Self::NegInf => f64::NEG_INFINITY,
            Self::Nan => f64::NAN,
        }
    }
}

fn stream_expr(tokens: &[NumTok]) -> String {
    if tokens.len() == 1 {
        return tokens[0].as_query();
    }
    let inner = tokens
        .iter()
        .map(|t| t.as_query())
        .collect::<Vec<_>>()
        .join(",");
    format!("({inner})")
}

fn jq_dtoi_compat(v: f64) -> i64 {
    if v < i64::MIN as f64 {
        i64::MIN
    } else if -v < i64::MIN as f64 {
        i64::MAX
    } else {
        v as i64
    }
}

fn jq_mod_compat(lhs: f64, rhs: f64) -> Result<f64, &'static str> {
    if lhs.is_nan() || rhs.is_nan() {
        return Ok(f64::NAN);
    }
    let rhs_i = jq_dtoi_compat(rhs);
    if rhs_i == 0 {
        return Err("divisor is zero");
    }
    if rhs_i == -1 {
        return Ok(0.0);
    }
    Ok((jq_dtoi_compat(lhs) % rhs_i) as f64)
}

fn to_json_number_lossy(v: f64) -> JsonValue {
    if !v.is_finite() {
        return JsonValue::Null;
    }
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        return JsonValue::from(v as i64);
    }
    JsonValue::Number(
        serde_json::Number::from_f64(v).unwrap_or_else(|| panic!("invalid finite number: {v}")),
    )
}

fn expected_modulo(
    lhs: &[NumTok],
    rhs: &[NumTok],
    apply_isnan: bool,
) -> Result<JsonValue, &'static str> {
    let mut row = Vec::new();
    // jq stream binary op order: RHS outer, LHS inner.
    for r in rhs {
        for l in lhs {
            let v = jq_mod_compat(l.as_f64(), r.as_f64())?;
            if apply_isnan {
                row.push(JsonValue::Bool(v.is_nan()));
            } else {
                row.push(to_json_number_lossy(v));
            }
        }
    }
    Ok(JsonValue::Array(row))
}

fn escape_html(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn escape_jq_string(s: &str) -> String {
    let mut out = String::new();
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

fn slice_bounds(len: usize, start: Option<isize>, end: Option<isize>) -> (usize, usize) {
    let norm = |idx: isize| -> usize {
        if idx < 0 {
            let x = len as isize + idx;
            if x < 0 {
                0
            } else {
                x as usize
            }
        } else if idx as usize > len {
            len
        } else {
            idx as usize
        }
    };
    let s = start.map(norm).unwrap_or(0);
    let e = end.map(norm).unwrap_or(len);
    if e < s {
        (s, s)
    } else {
        (s, e)
    }
}

fn slice_string(s: &str, start: Option<isize>, end: Option<isize>) -> String {
    let chars = s.chars().collect::<Vec<_>>();
    let (si, ei) = slice_bounds(chars.len(), start, end);
    chars[si..ei].iter().collect::<String>()
}

fn substring_positions(haystack: &str, needle: &str) -> Vec<i64> {
    if needle.is_empty() {
        return Vec::new();
    }
    let hb = haystack.as_bytes();
    let nb = needle.as_bytes();
    if nb.len() > hb.len() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut i = 0_usize;
    while i + nb.len() <= hb.len() {
        if &hb[i..i + nb.len()] == nb {
            out.push(i as i64);
            // jq indices() for strings reports non-overlapping matches.
            i += nb.len();
        } else {
            i += 1;
        }
    }
    out
}

#[derive(Clone, Copy)]
enum Selector {
    Index(isize),
    Slice(Option<isize>, Option<isize>),
}

fn apply_delete_selectors(input: &[JsonValue], selectors: &[Selector]) -> Vec<JsonValue> {
    let mut removed = vec![false; input.len()];
    for sel in selectors {
        match sel {
            Selector::Index(i) => {
                let idx = if *i < 0 {
                    input.len() as isize + *i
                } else {
                    *i
                };
                if idx >= 0 {
                    let idx = idx as usize;
                    if idx < removed.len() {
                        removed[idx] = true;
                    }
                }
            }
            Selector::Slice(start, end) => {
                let (s, e) = slice_bounds(input.len(), *start, *end);
                for slot in removed.iter_mut().take(e).skip(s) {
                    *slot = true;
                }
            }
        }
    }
    input
        .iter()
        .enumerate()
        .filter_map(|(idx, v)| if removed[idx] { None } else { Some(v.clone()) })
        .collect()
}

#[test]
fn hardcode_guard_cluster_modulo_matrix() {
    let lhs_cases = vec![
        vec![NumTok::PosInf, NumTok::NegInf],
        vec![NumTok::Nan, NumTok::Int(1)],
        vec![NumTok::Int(-5), NumTok::Int(0), NumTok::Int(7)],
        vec![NumTok::Int(42)],
    ];
    let rhs_cases = vec![
        vec![
            NumTok::Int(1),
            NumTok::Int(-1),
            NumTok::Int(2),
            NumTok::Int(-2),
        ],
        vec![NumTok::Int(3), NumTok::Int(-3), NumTok::PosInf],
        vec![NumTok::Nan, NumTok::Int(7)],
        vec![NumTok::Int(5)],
    ];

    for lhs in &lhs_cases {
        for rhs in &rhs_cases {
            for apply_isnan in [false, true] {
                let suffix = if apply_isnan { "|isnan" } else { "" };
                let query = format!("[{}%{}{}]", stream_expr(lhs), stream_expr(rhs), suffix);
                let expected =
                    vec![expected_modulo(lhs, rhs, apply_isnan).expect("modulo expected")];
                assert_success_expected(&query, Vec::new(), true, expected);
            }
        }
    }

    assert_error_contains(
        "[(infinite,-infinite)%(0)]",
        Vec::new(),
        true,
        "divisor is zero",
    );
}

#[test]
fn hardcode_guard_cluster_html_and_shorthand() {
    let prefixes = vec!["<b>", "pre:", "[", "", "x&y="];
    let suffixes = vec!["</b>", ":post", "]", "", "|end"];
    let inputs = vec!["<x&y>", "\"quote\"", "alpha'beta", "plain", "&<>\"'"];

    for prefix in &prefixes {
        for suffix in &suffixes {
            for input in &inputs {
                let query = format!(
                    "@html \"{}\\(.){}\"",
                    escape_jq_string(prefix),
                    escape_jq_string(suffix)
                );
                let expected = vec![JsonValue::String(format!(
                    "{}{}{}",
                    prefix,
                    escape_html(input),
                    suffix
                ))];
                // Exhaustive matrix: validate semantics in library fast path.
                assert_success_expected_lib(
                    &query,
                    vec![JsonValue::String((*input).to_string())],
                    false,
                    expected,
                );
            }
        }
    }

    for n in 0_i64..=30 {
        let query = format!(r#"{{"a","b$\(1+{n})","c\("x"+"y")"}}"#);
        let query_ws = format!(r#"{{ "a" , "b$\(1+{n})" , "c\("x"+"y")" }}"#);
        let dynamic_key = format!("b${}", 1 + n);
        let input = serde_json::json!({"a":10, dynamic_key.clone():20, "cxy":30});
        let expected = vec![serde_json::json!({"a":10, dynamic_key:20, "cxy":30})];
        assert_success_expected_lib(&query, vec![input.clone()], false, expected.clone());
        assert_success_expected_lib(&query_ws, vec![input], false, expected);
    }

    // Sampled CLI parity checks keep CLI path covered without per-case process spawn cost.
    let sampled_html = vec![
        ("<b>", "</b>", "<x&y>"),
        ("pre:", ":post", "\"quote\""),
        ("x&y=", "|end", "&<>\"'"),
    ];
    for (prefix, suffix, input) in sampled_html {
        let query = format!(
            "@html \"{}\\(.){}\"",
            escape_jq_string(prefix),
            escape_jq_string(suffix)
        );
        let expected = vec![JsonValue::String(format!(
            "{}{}{}",
            prefix,
            escape_html(input),
            suffix
        ))];
        assert_success_expected(
            &query,
            vec![JsonValue::String(input.to_string())],
            false,
            expected,
        );
    }

    for n in [0_i64, 7_i64, 30_i64] {
        let query = format!(r#"{{"a","b$\(1+{n})","c\("x"+"y")"}}"#);
        let query_ws = format!(r#"{{ "a" , "b$\(1+{n})" , "c\("x"+"y")" }}"#);
        let dynamic_key = format!("b${}", 1 + n);
        let input = serde_json::json!({"a":10, dynamic_key.clone():20, "cxy":30});
        let expected = vec![serde_json::json!({"a":10, dynamic_key:20, "cxy":30})];
        assert_success_expected(&query, vec![input.clone()], false, expected.clone());
        assert_success_expected(&query_ws, vec![input], false, expected);
    }
}

#[test]
fn hardcode_guard_cluster_array_slice_ops() {
    let arrays = vec![
        serde_json::json!([10, 20, 30]),
        serde_json::json!([1]),
        serde_json::json!([1, 2, 3, 4, 5]),
    ];
    let index_sets = vec![vec![-1, 0, 1, 2, 5], vec![-3, -2, -1, 0], vec![0, 0, 1, 99]];

    for arr in &arrays {
        let input = arr.as_array().expect("array input");
        for indexes in &index_sets {
            let query = format!(
                "[.[{}]]",
                indexes
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let mut row = Vec::new();
            for idx in indexes {
                let resolved = if *idx < 0 {
                    let p = input.len() as isize + *idx;
                    if p < 0 {
                        None
                    } else {
                        Some(p as usize)
                    }
                } else {
                    Some(*idx as usize)
                };
                row.push(
                    resolved
                        .and_then(|p| input.get(p).cloned())
                        .unwrap_or(JsonValue::Null),
                );
            }
            assert_success_expected(
                &query,
                vec![arr.clone()],
                false,
                vec![JsonValue::Array(row)],
            );
        }
    }

    let index_cases = vec![
        ("ababa", vec!["a", "b"]),
        ("aaaa", vec!["aa", "a"]),
        ("xyz", vec!["x", "zz"]),
        ("a,b|c,d,e||f,g,h,|,|,i,j", vec![",", "|"]),
    ];
    for (s, needles) in index_cases {
        let args = needles
            .iter()
            .map(|n| serde_json::to_string(n).expect("string literal"))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("[(index({args}), rindex({args})), indices({args})]");

        let positions = needles
            .iter()
            .map(|needle| substring_positions(s, needle))
            .collect::<Vec<_>>();
        let mut row = Vec::new();
        for p in &positions {
            row.push(
                p.first()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
            );
        }
        for p in &positions {
            row.push(
                p.last()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
            );
        }
        for p in positions {
            row.push(JsonValue::Array(
                p.into_iter().map(JsonValue::from).collect(),
            ));
        }
        assert_success_expected(
            &query,
            vec![JsonValue::String(s.to_string())],
            false,
            vec![JsonValue::Array(row)],
        );
    }

    let slice_ops = vec![
        (
            "abcdef",
            vec![
                (Some(1), Some(4), None),
                (None, Some(2), None),
                (Some(-3), None, None),
            ],
        ),
        (
            "abcdef",
            vec![
                (Some(1), Some(3), Some((Some(1), None))),
                (Some(10), None, None),
            ],
        ),
        (
            "0123456789",
            vec![
                (Some(3), Some(8), Some((Some(2), Some(4)))),
                (Some(-5), Some(-1), None),
            ],
        ),
    ];
    for (s, ops) in slice_ops {
        let mut parts = Vec::new();
        let mut expected_row = Vec::new();
        for (start, end, second) in ops {
            let s0 = start.map(|v| v.to_string()).unwrap_or_default();
            let e0 = end.map(|v| v.to_string()).unwrap_or_default();
            let mut part = format!(".[{s0}:{e0}]");
            let first = slice_string(s, start, end);
            let out = if let Some((s1, e1)) = second {
                let s1s = s1.map(|v| v.to_string()).unwrap_or_default();
                let e1s = e1.map(|v| v.to_string()).unwrap_or_default();
                part.push_str(&format!("[{s1s}:{e1s}]"));
                slice_string(&first, s1, e1)
            } else {
                first
            };
            parts.push(part);
            expected_row.push(JsonValue::String(out));
        }
        let query = format!("[{}]", parts.join(","));
        assert_success_expected(
            &query,
            vec![JsonValue::String(s.to_string())],
            false,
            vec![JsonValue::Array(expected_row)],
        );
    }
}

#[test]
fn hardcode_guard_cluster_delete_assign_reduce() {
    let delete_cases = vec![
        (
            vec![10, 20, 30, 40, 50],
            vec![
                Selector::Slice(Some(1), Some(3)),
                Selector::Index(0),
                Selector::Slice(Some(-1), None),
            ],
        ),
        (
            vec![1, 2, 3, 4, 5, 6],
            vec![
                Selector::Index(-2),
                Selector::Slice(None, Some(1)),
                Selector::Slice(Some(4), Some(99)),
            ],
        ),
    ];
    for (arr_raw, selectors) in delete_cases {
        let input = arr_raw.into_iter().map(JsonValue::from).collect::<Vec<_>>();
        let mut selector_parts = Vec::new();
        for sel in &selectors {
            match sel {
                Selector::Index(i) => selector_parts.push(format!(".[{i}]")),
                Selector::Slice(s, e) => {
                    let ss = s.map(|v| v.to_string()).unwrap_or_default();
                    let ee = e.map(|v| v.to_string()).unwrap_or_default();
                    selector_parts.push(format!(".[{ss}:{ee}]"));
                }
            }
        }
        let query = format!("del({})", selector_parts.join(","));
        let expected = apply_delete_selectors(&input, &selectors);
        assert_success_expected(
            &query,
            vec![JsonValue::Array(input)],
            false,
            vec![JsonValue::Array(expected)],
        );
    }

    let assign_cases = vec![
        (
            2,
            4,
            vec![vec![], vec!["a", "b"], vec!["a", "b", "c"]],
            vec![1, 2, 3, 4, 5],
        ),
        (1, 3, vec![vec!["x"], vec!["x", "y"]], vec![10, 20, 30, 40]),
    ];
    for (start, end, reps_raw, arr_raw) in assign_cases {
        let input = arr_raw.into_iter().map(JsonValue::from).collect::<Vec<_>>();
        let mut rep_parts = Vec::new();
        let mut expected = Vec::new();
        let (s, e) = slice_bounds(input.len(), Some(start), Some(end));
        let prefix = input[..s].to_vec();
        let suffix = input[e..].to_vec();
        for rep in reps_raw {
            let rep_json = JsonValue::Array(
                rep.into_iter()
                    .map(|v| JsonValue::String(v.to_string()))
                    .collect(),
            );
            rep_parts.push(serde_json::to_string(&rep_json).expect("encode replacement"));
            let mut merged = prefix.clone();
            if let JsonValue::Array(items) = rep_json {
                merged.extend(items);
            }
            merged.extend(suffix.clone());
            expected.push(JsonValue::Array(merged));
        }
        let query = format!(".[{start}:{end}] = ({})", rep_parts.join(","));
        assert_success_expected(&query, vec![JsonValue::Array(input)], false, expected);
    }

    let reduce_cases = vec![(6, 2, -1, 2), (10, 4, -2, 4), (9, 0, -3, 3), (0, 6, 2, 1)];
    for (start, stop, step, tail) in reduce_cases {
        let query =
            format!("reduce range({start};{stop};{step}) as $i ([]; .[$i] = $i)|.[{tail}:]");
        let mut values = Vec::new();
        if step > 0 {
            let mut cur = start;
            while cur < stop {
                values.push(cur);
                cur += step;
            }
        } else {
            let mut cur = start;
            while cur > stop {
                values.push(cur);
                cur += step;
            }
        }
        let mut acc = Vec::new();
        for v in values {
            if v < 0 {
                continue;
            }
            let idx = v as usize;
            if idx >= acc.len() {
                acc.resize(idx + 1, JsonValue::Null);
            }
            acc[idx] = JsonValue::from(v);
        }
        let expected_tail = if tail as usize >= acc.len() {
            Vec::new()
        } else {
            acc[tail as usize..].to_vec()
        };
        assert_success_expected(
            &query,
            vec![JsonValue::Null],
            false,
            vec![JsonValue::Array(expected_tail)],
        );
    }
}

#[test]
fn hardcode_guard_cluster_def_and_tonumber() {
    for mult in ["0", "1", "2", "3", "4", "7", "1.5", "-2", "-0.5"] {
        let query = format!(
            "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*{mult})"
        );
        let inputs = vec![
            serde_json::json!([1, 2, 3]),
            serde_json::json!([0, -1, 5, 10]),
            serde_json::json!([1.25, 2.5, -3.75]),
        ];
        for input in inputs {
            assert_success_expected_lib(&query, vec![input], false, vec![JsonValue::Bool(true)]);
        }
    }

    for mult in ["3", "-0.5"] {
        let query = format!(
            "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*{mult})"
        );
        assert_success_expected(
            &query,
            vec![serde_json::json!([1, 2, 3, 4])],
            false,
            vec![JsonValue::Bool(true)],
        );
    }

    let add_cases = vec![
        ("1", "10", JsonValue::from(4), to_json_number_lossy(15.0)),
        (
            "-2",
            "3.5",
            JsonValue::String("1.5".to_string()),
            to_json_number_lossy(3.0),
        ),
        (
            "0.5",
            "-1.25",
            JsonValue::from(-3),
            to_json_number_lossy(-3.75),
        ),
    ];
    for (left, right_lit, input, expected) in add_cases {
        let query = format!("{left} + tonumber + (\"{right_lit}\" | tonumber)");
        assert_success_expected(&query, vec![input], false, vec![expected]);
    }
    assert_error_contains(
        "1 + tonumber + (\"10\" | tonumber)",
        vec![serde_json::json!({"a":1})],
        false,
        "cannot be parsed as a number",
    );
}
