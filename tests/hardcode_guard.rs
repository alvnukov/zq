use serde_json::{Map as JsonMap, Value as JsonValue};
use std::io::Write;
use std::process::{Command, Stdio};
use zq::{run_jq_stream_with_paths_options, EngineRunOptions};

#[path = "common/c_numeric.rs"]
mod c_numeric;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_zq")
}

fn run_lib(query: &str, input_stream: Vec<JsonValue>, null_input: bool) -> Vec<JsonValue> {
    run_jq_stream_with_paths_options(query, input_stream, &[], EngineRunOptions { null_input })
        .unwrap_or_else(|e| panic!("library run failed for `{query}`: {e}"))
}

fn run_cli_json(query: &str, input_stream: &[JsonValue], null_input: bool) -> Vec<JsonValue> {
    let mut cmd = Command::new(bin());
    cmd.arg("-c");
    if null_input {
        cmd.arg("-n");
    }
    // Ensure filters starting with '-' are interpreted as filters, not flags.
    cmd.arg("--")
        .arg(query)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("spawn zq failed for `{query}`: {e}"));
    if !null_input {
        let mut stdin = child.stdin.take().expect("stdin handle");
        for value in input_stream {
            let line = serde_json::to_string(value).expect("encode input json");
            stdin
                .write_all(line.as_bytes())
                .unwrap_or_else(|e| panic!("write stdin failed for `{query}`: {e}"));
            stdin
                .write_all(b"\n")
                .unwrap_or_else(|e| panic!("write stdin newline failed for `{query}`: {e}"));
        }
    }
    let out = child
        .wait_with_output()
        .unwrap_or_else(|e| panic!("wait zq failed for `{query}`: {e}"));
    assert!(
        out.status.success(),
        "cli run failed for `{query}`\nstatus={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    if text.trim().is_empty() {
        return Vec::new();
    }
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<JsonValue>(line)
                .unwrap_or_else(|e| panic!("stdout line is not json `{line}`: {e}"))
        })
        .collect()
}

fn assert_query_variants_equivalent(
    canonical: &str,
    variant: &str,
    input_stream: Vec<JsonValue>,
    null_input: bool,
) {
    let lib_a = run_lib(canonical, input_stream.clone(), null_input);
    let lib_b = run_lib(variant, input_stream.clone(), null_input);
    assert_eq!(
        lib_a, lib_b,
        "library mismatch between canonical and variant\ncanonical: {canonical}\nvariant: {variant}"
    );

    let cli_a = run_cli_json(canonical, &input_stream, null_input);
    let cli_b = run_cli_json(variant, &input_stream, null_input);
    assert_eq!(
        cli_a, cli_b,
        "cli mismatch between canonical and variant\ncanonical: {canonical}\nvariant: {variant}"
    );

    assert_eq!(lib_a, cli_a, "library and cli mismatch\nquery: {canonical}");
}

#[test]
fn hardcode_guard_metamorphic_query_variants() {
    assert_query_variants_equivalent(
        "[(infinite, -infinite) % (1, -1, infinite)]",
        "[ ( infinite , -infinite ) % ( 1 , -1 , infinite ) ]",
        Vec::new(),
        true,
    );
    assert_query_variants_equivalent(
        "[nan % 1, 1 % nan | isnan]",
        "[ nan % 1 , 1 % nan | isnan ]",
        Vec::new(),
        true,
    );
    assert_query_variants_equivalent(
        "[.[]|tojson|fromjson]",
        "[ .[] | tojson | fromjson ]",
        vec![serde_json::json!([1, "x", {"k": true}])],
        false,
    );
    assert_query_variants_equivalent(
        "{a,b,(.d):.a,e:.b}",
        "{ a , b , ( .d ) : .a , e : .b }",
        vec![serde_json::json!({"a":10, "b":20, "d":"dyn"})],
        false,
    );
    assert_query_variants_equivalent(
        "[.[1:4], .[:2], .[-3:], .[1:3][1:]]",
        "[ .[1:4] , .[:2] , .[-3:] , .[1:3][1:] ]",
        vec![serde_json::json!("abcdef")],
        false,
    );
    assert_query_variants_equivalent(
        "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*3)",
        "def x(a;b): a as $a | b as $b | $a + $b;\ndef y($a;$b): $a + $b;\ndef check(a;b): [x(a;b)] == [y(a;b)];\ncheck(.[];.[]*3)",
        vec![serde_json::json!([1, 2, 3, 4])],
        false,
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
    fn as_query_token(self) -> String {
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

fn jq_number_json_compat(v: f64) -> JsonValue {
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

fn expected_modulo_array(
    lhs_values: &[NumTok],
    rhs_values: &[NumTok],
    apply_isnan: bool,
) -> Result<JsonValue, &'static str> {
    let mut out = Vec::new();
    // jq binary op stream order: RHS outer, LHS inner.
    for rhs in rhs_values {
        for lhs in lhs_values {
            let value = c_numeric::mod_compat(lhs.as_f64(), rhs.as_f64())?;
            if apply_isnan {
                out.push(JsonValue::Bool(value.is_nan()));
            } else {
                out.push(jq_number_json_compat(value));
            }
        }
    }
    Ok(JsonValue::Array(out))
}

fn as_stream_expr(values: &[NumTok]) -> String {
    if values.len() == 1 {
        return values[0].as_query_token();
    }
    let joined = values
        .iter()
        .map(|t| t.as_query_token())
        .collect::<Vec<_>>()
        .join(",");
    format!("({joined})")
}

#[test]
fn hardcode_guard_modulo_parametric_semantics() {
    let cases = vec![
        (
            vec![NumTok::PosInf, NumTok::NegInf],
            vec![
                NumTok::Int(1),
                NumTok::Int(-1),
                NumTok::Int(2),
                NumTok::Int(-2),
            ],
            false,
        ),
        (
            vec![NumTok::Nan, NumTok::Int(1)],
            vec![NumTok::Int(1), NumTok::Nan],
            false,
        ),
        (
            vec![NumTok::Nan, NumTok::Int(1)],
            vec![NumTok::Int(1), NumTok::Nan],
            true,
        ),
    ];

    for (lhs, rhs, apply_isnan) in cases {
        let suffix = if apply_isnan { " | isnan" } else { "" };
        let query = format!(
            "[{} % {}{}]",
            as_stream_expr(&lhs),
            as_stream_expr(&rhs),
            suffix
        );
        let expected =
            expected_modulo_array(&lhs, &rhs, apply_isnan).unwrap_or_else(|e| panic!("{e}"));
        let out = run_lib(&query, Vec::new(), true);
        assert_eq!(
            out,
            vec![expected],
            "parametric modulo failed for `{query}`"
        );
    }

    let zero_div_query = "[(infinite, -infinite) % (0)]";
    let err = run_jq_stream_with_paths_options(
        zero_div_query,
        Vec::new(),
        &[],
        EngineRunOptions { null_input: true },
    )
    .expect_err("zero divisor must fail");
    assert!(
        err.to_string().contains("divisor is zero"),
        "unexpected zero divisor error: {err}"
    );
}

fn html_escape(s: &str) -> String {
    let mut out = String::new();
    for ch in s.chars() {
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

#[test]
fn hardcode_guard_parametric_html_and_object_shorthand() {
    let html_cases = vec![
        ("<b>", "</b>", "<x&y>"),
        ("pre:", ":post", "\"quote\""),
        ("[", "]", "alpha'beta"),
    ];
    for (prefix, suffix, input) in html_cases {
        let query = format!("@html \"{prefix}\\(.){suffix}\"");
        let expected = JsonValue::String(format!("{prefix}{}{suffix}", html_escape(input)));
        let out = run_lib(&query, vec![JsonValue::String(input.to_string())], false);
        assert_eq!(out, vec![expected], "html template mismatch for `{query}`");
    }

    for n in [2_i64, 3, 5, 8] {
        let expr = format!("1+{n}");
        let query = format!(r#"{{"a","b$\({expr})","c\("x"+"y")"}}"#);
        let mut query_variant = format!(r#"{{ "a" , "b$\({expr})" , "c\("x"+"y")" }}"#);
        // Inject some harmless outer whitespace to stress exact-string matches.
        query_variant.insert(0, ' ');
        query_variant.push(' ');

        let dynamic_key = format!("b${}", 1 + n);
        let input = serde_json::json!({
            "a": 10,
            dynamic_key.clone(): 20,
            "cxy": 30
        });

        let mut expected_obj = JsonMap::new();
        expected_obj.insert("a".to_string(), JsonValue::from(10));
        expected_obj.insert(dynamic_key, JsonValue::from(20));
        expected_obj.insert("cxy".to_string(), JsonValue::from(30));
        let expected = JsonValue::Object(expected_obj);

        let out_a = run_lib(&query, vec![input.clone()], false);
        let out_b = run_lib(&query_variant, vec![input], false);
        assert_eq!(out_a, vec![expected.clone()], "object shorthand mismatch");
        assert_eq!(
            out_a, out_b,
            "object shorthand canonical/variant mismatch\ncanonical: {query}\nvariant: {query_variant}"
        );
    }
}

#[test]
fn hardcode_guard_cli_library_cross_path_cases() {
    let cases = vec![
        (
            ".",
            vec![serde_json::json!({"a":1}), serde_json::json!([1, 2, 3])],
            false,
        ),
        (
            ".a",
            vec![serde_json::json!({"a":7}), serde_json::json!({"a":8})],
            false,
        ),
        (".[]", vec![serde_json::json!([0, 1, 2, 3])], false),
        (
            "[.[]|tojson|fromjson]",
            vec![serde_json::json!([1, "x", {"k":true}])],
            false,
        ),
        (
            "@html \"<i>\\(.)!</i>\"",
            vec![JsonValue::String("<x>".to_string())],
            false,
        ),
        (
            "{a,b,(.d):.a,e:.b}",
            vec![serde_json::json!({"a":1,"b":2,"d":"k"})],
            false,
        ),
        (
            "[(infinite, -infinite) % (1, -1, infinite)]",
            Vec::new(),
            true,
        ),
        ("[nan % 1, 1 % nan | isnan]", Vec::new(), true),
        (
            "[.[1:4], .[:2], .[-3:], .[1:3][1:]]",
            vec![JsonValue::String("abcdef".to_string())],
            false,
        ),
    ];

    for (query, input_stream, null_input) in cases {
        let lib = run_lib(query, input_stream.clone(), null_input);
        let cli = run_cli_json(query, &input_stream, null_input);
        assert_eq!(lib, cli, "cli/library mismatch for query `{query}`");
    }
}
