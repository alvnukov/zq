use super::*;
use crate::value::ZqValue;
use indexmap::IndexMap;
use serde_json::json;
use std::path::PathBuf;

fn canonical_module_dir(path: &str) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path))
}

#[test]
fn identity_roundtrip() {
    let program = compile(".").expect("compile");
    let out = execute(&program, &ZqValue::from(42)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);
}

#[test]
fn dynamic_slice_bounds_from_expression_match_jq_shape() {
    let q = compile(r#".[:rindex("x")]"#).expect("compile");
    let out = execute(&q, &ZqValue::String("abxcdx".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("abxcd".to_string())]);
}

#[test]
fn module_directive_forms_parse_at_toplevel() {
    let q = compile(r#"module {"name":"m"}; def f: 1; f"#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile(r#"import "src/native_engine/vm_core/test_modules/m" as mod; mod::one"#)
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile(r#"include "src/native_engine/vm_core/test_modules/m"; one"#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile(
            r#"import "src/native_engine/vm_core/test_modules/data" as $d; [$d[0].value, $d::d[0].value]"#,
        )
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([7, 7]))]);

    let q = compile(
            r#"import "alt" as mod {"search":"src/native_engine/vm_core/test_modules/search"}; mod::value"#,
        )
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(99)]);

    let q = compile(
        r#"include "src/native_engine/vm_core/test_modules/does_not_exist" {"optional":true}; 1"#,
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q =
        compile(r#"import "src/native_engine/vm_core/test_modules/raw" as $r {"raw":true}; $r"#)
            .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::String("this is raw text, not JSON\n".to_string())]
    );
}

#[test]
fn module_library_paths_can_be_provided_explicitly() {
    let err = compile(r#"import "alt" as mod; mod::value"#).expect_err("must fail");
    assert!(err.contains("module not found"), "err={err}");

    let q = compile_with_module_dirs(
        r#"import "alt" as mod; mod::value"#,
        vec![std::path::PathBuf::from(
            "src/native_engine/vm_core/test_modules/search",
        )],
    )
    .expect("compile with library path");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(99)]);
}

#[test]
fn module_linker_order_and_shadowing_match_jq_cases() {
    let linker_dir = std::path::PathBuf::from("src/native_engine/vm_core/test_modules/linker");

    let q = compile_with_module_dirs(
        r#"include "shadow1"; include "shadow2"; e"#,
        vec![linker_dir.clone()],
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(3)]);

    let q = compile_with_module_dirs(
        r#"import "shadow1" as f; import "shadow2" as f; import "shadow1" as e; [e::e, f::e]"#,
        vec![linker_dir.clone()],
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([2, 3]))]);

    let q = compile_with_module_dirs(
        r#"import "data" as $a; import "data" as $b; {$a, $b}"#,
        vec![linker_dir.clone()],
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!({
            "a": [{"this":"is a test","that":"is too"}],
            "b": [{"this":"is a test","that":"is too"}]
        }))]
    );

    let q = compile_with_module_dirs(
        r#"import "test_bind_order" as check; check::check"#,
        vec![linker_dir],
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn modulemeta_matches_upstream_shape_for_module_descriptor() {
    let modulemeta_dir = canonical_module_dir("src/native_engine/vm_core/test_modules/modulemeta");
    let q = compile_with_module_dirs(r#""c" | modulemeta"#, vec![modulemeta_dir]).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!({
            "whatever": null,
            "deps": [
                {"as":"foo","is_data":false,"relpath":"a"},
                {"search":"./","as":"d","is_data":false,"relpath":"d"},
                {"search":"./","as":"d2","is_data":false,"relpath":"d"},
                {"as":"d","is_data":true,"relpath":"data"}
            ],
            "defs": ["a/0","c/0"]
        }))]
    );
}

#[test]
fn modulemeta_matches_upstream_modules_fixture_with_search_paths() {
    let modulemeta_dir =
        canonical_module_dir("src/native_engine/vm_core/test_modules/modulemeta_upstream");
    let q = compile_with_module_dirs(r#""c" | modulemeta"#, vec![modulemeta_dir]).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!({
            "whatever": null,
            "deps": [
                {"as":"foo","is_data":false,"relpath":"a"},
                {"search":"./","as":"d","is_data":false,"relpath":"d"},
                {"search":"./","as":"d2","is_data":false,"relpath":"d"},
                {"search":"./../lib/jq","as":"e","is_data":false,"relpath":"e"},
                {"search":"./../lib/jq","as":"f","is_data":false,"relpath":"f"},
                {"as":"d","is_data":true,"relpath":"data"}
            ],
            "defs": ["a/0","c/0"]
        }))]
    );
}

#[test]
fn modulemeta_requires_string_input_like_jq() {
    let q = compile("modulemeta").expect("compile");
    let err = execute(&q, &ZqValue::Null).expect_err("must fail");
    assert_eq!(err, "modulemeta input module name must be a string");
}

#[test]
fn modulemeta_returns_null_for_parse_failures_like_jq() {
    let modulemeta_dir = canonical_module_dir("src/native_engine/vm_core/test_modules/modulemeta");
    let q = compile_with_module_dirs(r#""syntaxerror" | modulemeta"#, vec![modulemeta_dir])
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn env_builtin_exposes_process_environment_object() {
    let q = compile("env | type").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("object".to_string())]);
}

#[test]
fn get_search_list_uses_compile_library_paths() {
    let modulemeta_dir = canonical_module_dir("src/native_engine/vm_core/test_modules/modulemeta");
    let q =
        compile_with_module_dirs("get_search_list", vec![modulemeta_dir.clone()]).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    let rendered = out[0].clone().into_json();
    let values = rendered
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>();
    assert!(
        values.contains(&modulemeta_dir.to_string_lossy().as_ref()),
        "values={values:?}"
    );
}

#[test]
fn get_prog_origin_is_null_for_inline_queries() {
    let q = compile("get_prog_origin").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn jq_origin_and_runtime_flags_builtins_are_defined() {
    let q = compile("[get_jq_origin | type, have_decnum, have_literal_numbers]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    let payload = out[0].clone().into_json();
    let arr = payload.as_array().expect("array");
    assert_eq!(arr.get(1), Some(&json!(true)));
    assert_eq!(arr.get(2), Some(&json!(true)));
    let Some(origin_type) = arr.first().and_then(|v| v.as_str()) else {
        panic!("unexpected payload: {payload}");
    };
    assert!(
        origin_type == "string" || origin_type == "null",
        "payload={payload}"
    );
}

#[test]
fn now_builtin_returns_number() {
    let q = compile("now | type").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("number".to_string())]);
}

#[test]
fn input_location_builtins_are_defined() {
    let q = compile("[input_filename, input_line_number]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([null, null]))]);
}

#[test]
fn halt_builtin_emits_halt_error_payload() {
    let q = compile("halt").expect("compile");
    let err = execute(&q, &ZqValue::Null).expect_err("must fail");
    let decoded = decode_halt_error(&err).expect("halt payload");
    assert_eq!(decoded.0, 0);
    assert_eq!(decoded.1, "");
}

#[test]
fn builtins_contract_matches_upstream_jq_tests() {
    let q = compile("builtins|length > 10").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile(r#""-1"|IN(builtins[] / "/"|.[1])"#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile(r#"all(builtins[] / "/"; .[1]|tonumber >= 0)"#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile(r#"builtins|any(.[:1] == "_")"#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn internal_binop_and_negate_aliases_match_jq_cfuncs() {
    let q = compile("_plus(1;2)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(3)]);

    let q = compile("_minus(10;4)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(6)]);

    let q = compile("_equal(1;1), _notequal(1;2), _less(1;2), _greatereq(2;2)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
        ]
    );

    let q = compile("_negate").expect("compile");
    let out = execute(&q, &ZqValue::from(3)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(-3)]);
}

#[test]
fn number_literal_overflow_preserves_decnum_text_like_jq() {
    let q = compile("1E+1000 | tojson").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("1E+1000".to_string())]);

    let q = compile("-1E+1000 | tojson").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("-1E+1000".to_string())]);
}

#[test]
fn huge_input_numbers_keep_numeric_arithmetic_like_jq_non_decnum() {
    let huge_json = serde_json::from_str::<serde_json::Value>(
        "12345678901234567890123456789012345678901234567890",
    )
    .expect("parse");
    let input = ZqValue::from_json(huge_json);

    let q = compile("try (. * 1000000000) catch .").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert!(
        matches!(out.as_slice(), [ZqValue::Number(_)]),
        "unexpected output: {out:?}"
    );

    let q = compile("try (-.) catch .").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert!(
        matches!(out.as_slice(), [ZqValue::Number(_)]),
        "unexpected output: {out:?}"
    );
}

#[test]
fn libm_unary_builtins_are_exposed_in_builtins_list() {
    let q = compile(
        r#"
            ["acos/0","acosh/0","asin/0","asinh/0","atanh/0",
             "cbrt/0","ceil/0","cosh/0","exp/0","exp2/0","expm1/0",
             "log/0","log10/0","log1p/0","sinh/0","tan/0","tanh/0"]
            | all(.[]; builtins | index(.) != null)
            "#,
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn libm_binary_and_ternary_builtins_are_exposed_in_builtins_list() {
    let q = compile(
        r#"
            ["atan2/2","copysign/2","drem/2","fdim/2","fma/3","fmax/2","fmin/2",
             "fmod/2","hypot/2","ldexp/2","nextafter/2","nexttoward/2",
             "remainder/2","scalb/2","scalbln/2"]
            | all(.[]; builtins | index(.) != null)
            "#,
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn libm_unary_builtins_follow_basic_numeric_sanity() {
    let q = compile("exp | log").expect("compile");
    let out = execute(&q, &ZqValue::from(2)).expect("execute");
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 2.0).abs() < 1e-12, "value={value}");

    let q = compile("atanh | tanh").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(0.5))).expect("execute");
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 0.5).abs() < 1e-12, "value={value}");

    let q = compile("cbrt").expect("compile");
    let out = execute(&q, &ZqValue::from(27)).expect("execute");
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 3.0).abs() < 1e-12, "value={value}");
}

#[test]
fn libm_binary_and_ternary_builtins_from_jq_are_supported() {
    let q = compile("atan2(0;1)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    let value = out[0].as_f64().expect("numeric output");
    assert!(value.abs() < 1e-12, "value={value}");

    let q = compile("hypot(3;4)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 5.0).abs() < 1e-12, "value={value}");

    let q = compile(
        "copysign(1;-2), fdim(5;2), fmod(5.5;2), remainder(5.5;2), \
             ldexp(3;2), scalb(3;2), scalbln(3;2), fma(2;3;4)",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!(-1)),
            ZqValue::from_json(json!(3)),
            ZqValue::from_json(json!(1.5)),
            ZqValue::from_json(json!(-0.5)),
            ZqValue::from_json(json!(12)),
            ZqValue::from_json(json!(12)),
            ZqValue::from_json(json!(12)),
            ZqValue::from_json(json!(10)),
        ]
    );

    let q = compile(
        "[nextafter(1;2) > 1, nextafter(1;2) < 2, nexttoward(1;2) > 1, nexttoward(1;2) < 2]",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([true, true, true, true]))]
    );
}

#[test]
fn namespaced_function_identifiers_roundtrip_parser_and_vm() {
    let q = compile("def mod::inc: . + 1; mod::inc").expect("compile");
    let out = execute(&q, &ZqValue::from(41)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);
}

#[test]
fn not_is_builtin_filter_like_in_jq() {
    let q = compile("not").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let out = execute(&q, &ZqValue::from(0)).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile(".[] | not").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([true, false, null, 0]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(false)
        ]
    );
}

#[test]
fn term_postfix_access_matches_jq_shape() {
    let q = compile("not .foo").expect("compile");
    let err = execute(&q, &ZqValue::Null).expect_err("must fail");
    assert_eq!(err, "Cannot index boolean with string \"foo\"");

    let q = compile("(\"abc\")[1]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("b".to_string())]);

    let q = compile("({\"a\": 1}).a").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn string_fractional_index_reports_number_index_error_like_jq() {
    let q = compile(r#"try ("foobar" | .[1.5]) catch ."#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!("Cannot index string with number (1.5)")]
    );
}

#[test]
fn object_field_lookup() {
    let mut map = IndexMap::new();
    map.insert("foo".to_string(), ZqValue::from(7));
    let input = ZqValue::Object(map);
    let program = compile(".foo").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(7)]);
}

#[test]
fn dot_identifier_with_whitespace_requires_bracket_form() {
    let err = compile(". foo").expect_err("must fail");
    assert_eq!(
        err,
        "parse error: try .[\"field\"] instead of .field for unusually named fields"
    );
}

#[test]
fn object_field_missing_returns_null() {
    let input = ZqValue::Object(IndexMap::new());
    let program = compile(".foo").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn non_object_field_lookup_errors_like_jq_family() {
    let program = compile(".foo").expect("compile");
    let err = execute(&program, &ZqValue::from(1)).expect_err("must fail");
    assert!(
        err.contains("Cannot index number with string \"foo\""),
        "err={err}"
    );
}

#[test]
fn bracket_string_field_lookup_matches_jq_forms() {
    let input = ZqValue::from_json(json!({"foo":{"bar":42},"bar":"badvalue"}));

    let program = compile(".[\"foo\"]").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!({"bar":42}))]);

    let program = compile(".[\"foo\"].bar").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);
}

#[test]
fn optional_bracket_string_suppresses_non_object_errors() {
    let program = compile(".[\"foo\"]?").expect("compile");
    let out = execute(&program, &ZqValue::from_json(json!({"foo":42}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let out = execute(&program, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn field_access_forms_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile(".foo").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"foo": 42, "bar": "less interesting data"})),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let out = execute(
        &q,
        &ZqValue::from_json(json!({"notfoo": true, "alsonotfoo": false})),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);

    let q = compile(".foo?").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"foo": 42, "bar": "less interesting data"})),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let out = execute(
        &q,
        &ZqValue::from_json(json!({"notfoo": true, "alsonotfoo": false})),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);

    let q = compile(".[\"foo\"]?").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 42}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let q = compile("[.foo?]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn quoted_dot_field_access_matches_jq() {
    let input = ZqValue::from_json(json!({"foo":{"bar":20}}));
    let program = compile(".\"foo\".\"bar\"").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(20)]);

    let program = compile(".\"foo\"?").expect("compile");
    let out = execute(&program, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn iterate_array_values() {
    let input = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)]);
    let program = compile(".[]").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);
}

#[test]
fn iterate_optional_suppresses_type_error() {
    let program = compile(".[]?").expect("compile");
    let out = execute(&program, &ZqValue::from(1)).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn iterate_non_optional_errors_like_jq_family() {
    let program = compile(".[]").expect("compile");
    let err = execute(&program, &ZqValue::from(1)).expect_err("must fail");
    assert_eq!(err, "Cannot iterate over number (1)");
}

#[test]
fn iteration_and_projection_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile(".[]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"name":"JSON", "good":true},
            {"name":"XML", "good":false}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!({"name":"JSON", "good":true}),
            json!({"name":"XML", "good":false}),
        ]
    );

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");

    let q = compile(".foo[]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo":[1,2,3]}))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]
    );

    let q = compile(".[]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1,"b":1}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(1)]);

    let q = compile(".[] | .name").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"name":"JSON", "good":true},
            {"name":"XML", "good":false}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("JSON".to_string()),
            ZqValue::String("XML".to_string())
        ]
    );

    let q = compile(".user, .projects[]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"user":"stedolan", "projects": ["jq", "wikiflow"]})),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("stedolan".to_string()),
            ZqValue::String("jq".to_string()),
            ZqValue::String("wikiflow".to_string())
        ]
    );

    let q = compile("[.user, .projects[]]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"user":"stedolan", "projects": ["jq", "wikiflow"]})),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!(["stedolan", "jq", "wikiflow"])
    );
}

#[test]
fn comma_operator_emits_from_same_input() {
    let mut map = IndexMap::new();
    map.insert("foo".to_string(), ZqValue::from(1));
    map.insert("bar".to_string(), ZqValue::from(2));
    let input = ZqValue::Object(map);
    let program = compile(".foo,.bar").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);
}

#[test]
fn defined_or_operator_matches_jq_core_semantics() {
    let q = compile("(false, 1) // 2").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile("(false, null) // 2").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2)]);

    let q = compile("(1, 2) // 3").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);

    let q = compile("empty // 7").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(7)]);

    let q = compile("1, null // 2").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);

    let q = compile("false // null // 5").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(5)]);

    let q = compile(".a // .b").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 0, "b": 2}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(0)]);

    // jq/tests/man.test
    let q = compile("empty // 42").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let q = compile(".foo // 42").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 19}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(19)]);

    let out = execute(&q, &ZqValue::from_json(json!({}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let q = compile("(false, null, 1) // 42").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile("(false, null, 1) | . // 42").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from(42), ZqValue::from(42), ZqValue::from(1)]
    );
}

#[test]
fn and_or_operators_follow_jq_stream_semantics() {
    let q = compile("(true, false) and true").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true), ZqValue::Bool(false)]);

    let q = compile("(false, 1) and (2, 3)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(true)
        ]
    );

    let q = compile("1 and empty").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert!(out.is_empty(), "out={out:?}");

    let q = compile("(true, false) or (false, true)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Bool(true)
        ]
    );
}

#[test]
fn and_or_truth_table_matches_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile(".[] | [.[0] and .[1], .[0] or .[1]]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[true, []], [false, 1], [42, null], [null, false]])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!([true, true]),
            json!([false, true]),
            json!([false, true]),
            json!([false, false]),
        ]
    );
}

#[test]
fn boolean_ops_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("42 and \"a string\"").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("(true, false) or false").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true), ZqValue::Bool(false)]);

    let q = compile("(true, true) and (true, false)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(false)
        ]
    );
}

#[test]
fn pipeline_composes_stage_outputs() {
    let mut inner = IndexMap::new();
    inner.insert("bar".to_string(), ZqValue::from(9));
    let mut outer = IndexMap::new();
    outer.insert("foo".to_string(), ZqValue::Object(inner));
    let input = ZqValue::Object(outer);
    let program = compile(".foo | .bar").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);
}

#[test]
fn hash_comments_match_jq_lexer_behavior() {
    let input = ZqValue::from_json(json!({"foo":{"bar":9}}));

    let q = compile(".foo # comment\n| .bar").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);

    let q = compile(".foo # comment with continuation\\\nstill comment\n| .bar").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);
}

#[test]
fn optional_field_suppresses_non_object_error() {
    let program = compile(".foo?").expect("compile");
    let out = execute(&program, &ZqValue::from(1)).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn array_index_returns_value() {
    let input = ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(20)]);
    let program = compile(".[1]").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(20)]);
}

#[test]
fn array_negative_index_returns_value() {
    let input = ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(20)]);
    let program = compile(".[-1]").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(20)]);
}

#[test]
fn string_index_returns_character() {
    let program = compile(".[1]").expect("compile");
    let out = execute(&program, &ZqValue::String("abc".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("b".to_string())]);
}

#[test]
fn optional_index_suppresses_non_indexable_error() {
    let program = compile(".[0]?").expect("compile");
    let out = execute(&program, &ZqValue::Bool(true)).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn array_slice_forms_match_jq_core_behavior() {
    let input = ZqValue::from_json(json!([0, 1, 2, 3, 4]));

    let q = compile(".[1:4]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 3]));

    let q = compile(".[:2]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1]));

    let q = compile(".[3:]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3, 4]));

    let q = compile(".[-2:]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3, 4]));

    let q = compile(".[4:1]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn string_slice_and_optional_slice_match_jq_core_behavior() {
    let q = compile(".[1:4]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abcdef"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("bcd"))]);

    let q = compile(".[-3:]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abcdef"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("def"))]);

    let q = compile(".[1:2]?").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(123))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn index_and_slice_forms_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile(".[0]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"name":"JSON", "good":true},
            {"name":"XML", "good":false}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"name":"JSON", "good":true})
    );

    let q = compile(".[2]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"name":"JSON", "good":true},
            {"name":"XML", "good":false}
        ])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);

    let q = compile(".[-2]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2)]);

    let q = compile(".[2:4]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "b", "c", "d", "e"]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["c", "d"]));

    let q = compile(".[2:4]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abcdefghi"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!("cd"));

    let q = compile(".[:3]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "b", "c", "d", "e"]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a", "b", "c"]));

    let q = compile(".[-2:]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "b", "c", "d", "e"]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["d", "e"]));

    let q = compile(".[4,2]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "b", "c", "d", "e"]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("e".to_string()),
            ZqValue::String("c".to_string())
        ]
    );

    let q = compile(".[1+2]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "b", "c", "d", "e"]))).expect("execute");
    assert_eq!(out, vec![ZqValue::String("d".to_string())]);
}

#[test]
fn dynamic_index_expression_matches_jq_shape() {
    let q = compile(".[.key]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"key":"foo","foo":42}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);

    let q = compile(".arr[.idx]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"idx":1,"arr":[10,20]}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(20)]);
}

#[test]
fn optional_dynamic_index_suppresses_key_type_errors() {
    let q = compile(".[.missing]?").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 1}))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn index_out_of_range_returns_null() {
    let input = ZqValue::Array(vec![ZqValue::from(10)]);
    let program = compile(".[3]").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn integer_literal_ignores_input() {
    let program = compile("42").expect("compile");
    let out = execute(&program, &ZqValue::from(1)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(42)]);
}

#[test]
fn string_literal_with_escape() {
    let program = compile("\"a\\n\"").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("a\n".to_string())]);
}

#[test]
fn boolean_and_null_literals() {
    let program = compile("true,false,null").expect("compile");
    let out = execute(&program, &ZqValue::from(1)).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::Bool(true), ZqValue::Bool(false), ZqValue::Null]
    );
}

#[test]
fn arithmetic_respects_precedence() {
    let program = compile("1 + 2 * 3").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(7)]);
}

#[test]
fn arithmetic_parentheses_override_precedence() {
    let program = compile("(1 + 2) * 3").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);
}

#[test]
fn unary_minus_works() {
    let program = compile("-1 + 2").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn add_null_is_identity() {
    let program = compile(". + null").expect("compile");
    let out = execute(&program, &ZqValue::from(5)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(5)]);
}

#[test]
fn add_strings_concatenates() {
    let program = compile("\"ab\" + \"cd\"").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("abcd".to_string())]);
}

#[test]
fn add_arrays_concatenates() {
    let input = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)]);
    let program = compile(". + .").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::Array(vec![
            ZqValue::from(1),
            ZqValue::from(2),
            ZqValue::from(1),
            ZqValue::from(2)
        ])]
    );
}

#[test]
fn divide_numbers_returns_fractional_number() {
    let program = compile("1 / 2").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(0.5));
}

#[test]
fn divide_by_zero_returns_jq_style_error() {
    let program = compile("1 / 0").expect("compile");
    let err = execute(&program, &ZqValue::Null).expect_err("must fail");
    assert_eq!(
        err,
        "number (1) and number (0) cannot be divided because the divisor is zero"
    );
}

#[test]
fn divide_strings_splits() {
    let program = compile("\"a,b\" / \",\"").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a", "b"]));
}

#[test]
fn arithmetic_and_merge_examples_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("(. + 2) * 5").expect("compile");
    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(15)]);

    let q = compile(".a + 1").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 7}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(8)]);
    let out = execute(&q, &ZqValue::from_json(json!({}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile(".a + .b").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": [1,2], "b": [3,4]}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 3, 4]));

    let q = compile(".a + null").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 1}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile("{a: 1} + {b: 2} + {c: 3} + {a: 42}").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a": 42, "b": 2, "c": 3}));

    let q = compile("4 - .a").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 3}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile(". - [\"xml\", \"yaml\"]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["xml", "yaml", "json"]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["json"]));

    let q = compile("10 / . * 3").expect("compile");
    let out = execute(&q, &ZqValue::from(5)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(6)]);

    let q = compile(". / \", \"").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("a, b,c,d, e"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a", "b,c,d", "e"]));

    let q =
        compile("{\"k\": {\"a\": 1, \"b\": 2}} * {\"k\": {\"a\": 0, \"c\": 3}}").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"k": {"a": 0, "b": 2, "c": 3}})
    );
}

#[test]
fn string_repeat_limit_matches_upstream_jq_guard() {
    // jq/tests/jq.test
    let q = compile("try (. * 1000000000) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("Repeat string result too long")]
    );

    // jq/tests/jq.test
    let q = compile(". * 1000000000").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(""))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("")]
    );
}

#[test]
fn comparisons_follow_jq_contract() {
    let program = compile("1 < 2, 2 <= 2, 3 > 2, 3 >= 3, 2 == 2, 2 != 3").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
        ]
    );
}

#[test]
fn comparisons_preserve_large_integer_precision_like_jq() {
    let q = compile("13911860366432393 == 13911860366432392").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn large_integer_comparison_and_addition_follow_jq_shape() {
    let q = compile(". as $big | [$big, $big + 1] | map(. > 10000000000000000000000000000000)")
        .expect("compile");
    let input = ZqValue::Number(serde_json::Number::from_string_unchecked(
        "10000000000000000000000000000001".to_string(),
    ));
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, false]));
}

#[test]
fn logical_and_or_are_short_circuiting() {
    let program = compile("false and (1/0), true or (1/0)").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false), ZqValue::Bool(true)]);
}

#[test]
fn field_names_and_and_or_are_supported() {
    let mut map = IndexMap::new();
    map.insert("and".to_string(), ZqValue::from(11));
    map.insert("or".to_string(), ZqValue::from(22));
    let input = ZqValue::Object(map);
    let program = compile(".and,.or").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(11), ZqValue::from(22)]);
}

#[test]
fn modulo_numbers_uses_jq_compat() {
    let program = compile("5 % 2").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn modulo_by_zero_uses_jq_error_text() {
    let program = compile("5 % 0").expect("compile");
    let err = execute(&program, &ZqValue::Null).expect_err("must fail");
    assert_eq!(
        err,
        "number (5) and number (0) cannot be divided (remainder) because the divisor is zero"
    );
}

#[test]
fn not_filter_uses_upstream_jq_forms() {
    // jq/tests/man.test
    let program = compile("[true, false | not]").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, true]));

    // jq/tests/jq.test
    let program = compile("[.[] | not]").expect("compile");
    let out = execute(
        &program,
        &ZqValue::from_json(json!([1, 0, false, null, true, "hello"])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([false, false, true, true, false, false])
    );
}

#[test]
fn array_literal_builds_from_expressions() {
    let program = compile("[1, .foo, .bar + 1]").expect("compile");
    let mut map = IndexMap::new();
    map.insert("foo".to_string(), ZqValue::from(2));
    map.insert("bar".to_string(), ZqValue::from(3));
    let out = execute(&program, &ZqValue::Object(map)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 4]));
}

#[test]
fn object_literal_builds_from_expressions() {
    let program = compile("{a: .foo, b: .bar * 2, \"c\": true}").expect("compile");
    let mut map = IndexMap::new();
    map.insert("foo".to_string(), ZqValue::from(2));
    map.insert("bar".to_string(), ZqValue::from(3));
    let out = execute(&program, &ZqValue::Object(map)).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"a": 2, "b": 6, "c": true})
    );
}

#[test]
fn object_literal_supports_jq_shorthand_fields() {
    let program = compile("{foo, \"bar\", if, elif}").expect("compile");
    let input = ZqValue::from_json(json!({"foo": 1, "bar": 2, "if": 3, "elif": 4}));
    let out = execute(&program, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"foo":1, "bar":2, "if":3, "elif":4})
    );

    let program = compile("{missing}").expect("compile");
    let out = execute(&program, &ZqValue::from_json(json!({}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"missing": null}));
}

#[test]
fn object_literal_accepts_all_jq_keyword_keys() {
    let q = compile("{as, def, module, import, include, reduce, foreach, label, break}")
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({
            "as": 1,
            "def": 2,
            "module": 3,
            "import": 4,
            "include": 5,
            "reduce": 6,
            "foreach": 7,
            "label": 8,
            "break": 9
        })),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({
            "as": 1,
            "def": 2,
            "module": 3,
            "import": 4,
            "include": 5,
            "reduce": 6,
            "foreach": 7,
            "label": 8,
            "break": 9
        })
    );
}

#[test]
fn object_literal_unblocks_object_add_merge() {
    let program = compile(". + {x: 7}").expect("compile");
    let mut map = IndexMap::new();
    map.insert("a".to_string(), ZqValue::from(1));
    let out = execute(&program, &ZqValue::Object(map)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a": 1, "x": 7}));
}

#[test]
fn object_literal_value_supports_pipe_expression() {
    let q = compile("{a: . + 1 | tostring, b: 2}").expect("compile");
    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a": "2", "b": 2}));
}

#[test]
fn object_literal_supports_parenthesized_dynamic_keys() {
    let q = compile("{(.k): .v}").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"k":"answer","v":42}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"answer": 42}));
}

#[test]
fn object_builders_match_upstream_man_cases() {
    // jq/tests/man.test
    let input = ZqValue::from_json(json!({
        "user": "stedolan",
        "titles": ["JQ Primer", "More JQ"]
    }));

    let q = compile("{user, title: .titles[]}").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!({"user":"stedolan", "title":"JQ Primer"}),
            json!({"user":"stedolan", "title":"More JQ"}),
        ]
    );

    let q = compile("{(.user): .titles}").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"stedolan": ["JQ Primer", "More JQ"]})
    );
}

#[test]
fn object_literal_constant_parenthesized_key_validates_like_jq() {
    let q = compile("{(\"k\"): 2}").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"k": 2}));

    let err = compile("{(1): 2}").expect_err("must fail");
    assert_eq!(err, "Cannot use number (1) as object key");
}

#[test]
fn object_literal_uses_jq_cartesian_stream_semantics() {
    let q = compile("{a: (1, 2), b: (3, 4)}").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!({"a":1, "b":3}),
            json!({"a":1, "b":4}),
            json!({"a":2, "b":3}),
            json!({"a":2, "b":4}),
        ]
    );
}

#[test]
fn object_literal_dynamic_key_requires_string_output() {
    let q = compile("{(.k): 1}").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!({"k": 1}))).expect_err("must fail");
    assert_eq!(err, "Cannot use number as object key");
}

#[test]
fn object_string_nonterminal_keys_follow_jq_parser_rules() {
    // jq/parser.y DictPair:
    // String ':' DictExpr
    // String
    let q = compile(r#"{"a\(1)"}"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a1": 7}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a1": 7}));

    let q = compile(r#"{"a\(1)": .value}"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"value": 42}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a1": 42}));
}

#[test]
fn object_pattern_string_keys_follow_jq_objpat_rules() {
    // jq/parser.y ObjPat:
    // String ':' Pattern
    let q = compile(r#". as {"a\(1)": $x} | $x"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a1": 9}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);
}

#[test]
fn object_key_expression_requires_parentheses_like_jq() {
    // jq/parser.y DictPair/ObjPat:
    // error ':' ... => "May need parentheses around object key expression"
    let err = compile("{1: .}").expect_err("must fail");
    assert_eq!(err, "May need parentheses around object key expression");

    let err = compile("{1+2: .}").expect_err("must fail");
    assert_eq!(err, "May need parentheses around object key expression");

    let err = compile(". as {1: $x} | $x").expect_err("must fail");
    assert_eq!(err, "May need parentheses around object key expression");
}

#[test]
fn import_path_must_be_constant_before_unresolved_call_error() {
    // jq/tests/jq.test %%FAIL:
    // include "\(a)"; 0
    let err = compile(r#"include "\(a)"; 0"#).expect_err("must fail");
    assert_eq!(err, "Import path must be constant");
}

#[test]
fn parser_reports_top_level_percent_and_rbrace_like_jq() {
    // jq/tests/jq.test %%FAIL
    let err = compile("%::wat").expect_err("must fail");
    assert_eq!(err, "syntax error, unexpected '%', expecting end of file");

    let err = compile("}").expect_err("must fail");
    assert_eq!(
        err,
        "syntax error, unexpected INVALID_CHARACTER, expecting end of file"
    );

    let err = compile("{").expect_err("must fail");
    assert_eq!(err, "syntax error, unexpected end of file");
}

#[test]
fn top_level_program_is_required_for_defs_only_like_jq() {
    let err = compile("def foo: 1;").expect_err("must fail");
    assert_eq!(err, "Top-level program not given (try \".\")");
}

#[test]
fn parser_enforces_jq_function_parameter_limit() {
    let params = (0..4097)
        .map(|i| format!("$a{i}"))
        .collect::<Vec<_>>()
        .join("; ");
    let args = (0..4097)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    let query = format!("def f({params}): .; f({args})");
    let err = compile(query.as_str()).expect_err("must fail");
    assert_eq!(
        err,
        "too many function parameters or local function definitions (max 4095)"
    );
}

#[test]
fn parser_enforces_jq_local_function_limit() {
    let defs = (0..4097)
        .map(|i| format!("def f{i}: {i};"))
        .collect::<Vec<_>>()
        .join(" ");
    let query = format!("{defs} 0");
    let err = compile(query.as_str()).expect_err("must fail");
    assert_eq!(
        err,
        "too many function parameters or local function definitions (max 4095)"
    );
}

#[test]
fn ident_call_arity_errors_follow_jq_parser_term_rules() {
    // jq/parser.y:
    // IDENT              => gen_call(name, gen_noop())
    // IDENT '(' Args ')' => gen_call(name, args)
    // and undefined calls report name/arity.
    let err = compile("foo").expect_err("must fail");
    assert_eq!(err, "foo/0 is not defined");

    let err = compile("foo(1)").expect_err("must fail");
    assert_eq!(err, "foo/1 is not defined");

    let err = compile("limit(1)").expect_err("must fail");
    assert_eq!(err, "limit/1 is not defined");

    let err = compile("length(1)").expect_err("must fail");
    assert_eq!(err, "length/1 is not defined");

    let err = compile("range(1;2;3;4)").expect_err("must fail");
    assert_eq!(err, "range/4 is not defined");
}

#[test]
fn def_without_params_matches_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("def f: (1000,2000); f").expect("compile");
    let out = execute(&q, &ZqValue::from(123412345)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1000), ZqValue::from(2000)]);

    // jq/tests/jq.test (precedence of def vs pipe)
    let q = compile("def a: 0; . | a").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(0)]);

    // jq: user `def` overrides builtin by name/arity.
    let q = compile("def length: 1; length").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn recursive_def_without_params_matches_upstream_jq_case() {
    // jq/tests/jq.test
    let q = compile("def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]")
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([1, 2, 6, 24]))]);
}

#[test]
fn def_with_params_matches_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])")
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([2, 2, 1, 1, 1, 1]))]);

    // jq/tests/jq.test
    let q = compile("def f(x): x | x; f([.], . + [42])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!([[[1, 2, 3]]])),
            ZqValue::from_json(json!([[1, 2, 3], 42])),
            ZqValue::from_json(json!([[1, 2, 3, 42]])),
            ZqValue::from_json(json!([1, 2, 3, 42, 42])),
        ]
    );

    // jq/tests/jq.test (many arguments)
    let q = compile(
        "def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; \
             f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])",
    )
    .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]))]
    );
}

#[test]
fn def_binding_params_match_upstream_jq_case() {
    // jq/tests/jq.test
    let q = compile(
        "def x(a;b): a as $a | b as $b | $a + $b; \
             def y($a;$b): $a + $b; \
             def check(a;b): [x(a;b)] == [y(a;b)]; \
             check(.[];.[]*2)",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn def_call_arity_for_params_follows_jq_rules() {
    let err = compile("def f(a): a; f").expect_err("must fail");
    assert_eq!(err, "f/0 is not defined");

    let err = compile("def f(a): a; f(1;2)").expect_err("must fail");
    assert_eq!(err, "f/2 is not defined");
}

#[test]
fn function_rebinding_is_lexically_bound_like_jq() {
    // jq/tests/jq.test
    let q = compile("def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]")
        .expect("compile");
    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([33, 101]))]);
}

#[test]
fn local_def_prefix_query_forms_match_upstream_jq() {
    // jq/parser.y Query:
    // FuncDef Query %prec FUNCDEF
    let q = compile("1, def f: 2; f").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);

    let q = compile("1 | def f: . + 1; f").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2)]);
}

#[test]
fn nested_and_scoped_defs_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(3.0))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(106), ZqValue::from(105)]);

    // jq/tests/jq.test
    let q = compile(
        "def f: 1; \
             def g: f, def f: 2; def g: 3; f, def f: g; f, g; \
             def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([4, 1, 2, 3, 3, 5, 4, 1, 2, 3, 3]))]
    );
}

#[test]
fn closures_and_lexical_scoping_match_upstream_jq_case() {
    // jq/tests/jq.test
    let q = compile(
        "def id(x):x; \
             2000 as $x | \
             def f(x):1 as $x | id([$x, x, x]); \
             def g(x): 100 as $x | f($x,$x+x); \
             g($x)",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("more testing"))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([1, 100, 2100, 100, 2100]))]
    );
}

#[test]
fn backtracking_through_function_calls_matches_upstream_jq_case() {
    // jq/tests/jq.test
    let q = compile(
        "[[20,10][1,0] as $x | \
             def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; \
             f[0] | [f][0][1] | f]",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(999999999))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([
            [110, 130],
            [210, 130],
            [110, 230],
            [210, 230],
            [120, 160],
            [220, 160],
            [120, 260],
            [220, 260]
        ]))]
    );
}

#[test]
fn function_forward_reference_fails_like_jq() {
    // jq/tests behavior: top-level defs are resolved in definition order.
    let err = compile("def g: f + 1; def f: . * 2; g").expect_err("must fail");
    assert_eq!(err, "f/0 is not defined");
}

#[test]
fn if_then_else_uses_jq_truthiness() {
    let program = compile("if .flag then .yes else .no end").expect("compile");
    let mut map_true = IndexMap::new();
    map_true.insert("flag".to_string(), ZqValue::Bool(true));
    map_true.insert("yes".to_string(), ZqValue::from(1));
    map_true.insert("no".to_string(), ZqValue::from(2));
    let out_true = execute(&program, &ZqValue::Object(map_true)).expect("execute");
    assert_eq!(out_true, vec![ZqValue::from(1)]);

    let mut map_false = IndexMap::new();
    map_false.insert("flag".to_string(), ZqValue::Null);
    map_false.insert("yes".to_string(), ZqValue::from(1));
    map_false.insert("no".to_string(), ZqValue::from(2));
    let out_false = execute(&program, &ZqValue::Object(map_false)).expect("execute");
    assert_eq!(out_false, vec![ZqValue::from(2)]);
}

#[test]
fn if_keywords_can_be_used_as_field_names() {
    let mut map = IndexMap::new();
    map.insert("if".to_string(), ZqValue::from(10));
    map.insert("then".to_string(), ZqValue::from(20));
    map.insert("else".to_string(), ZqValue::from(30));
    map.insert("elif".to_string(), ZqValue::from(35));
    map.insert("end".to_string(), ZqValue::from(40));
    let input = ZqValue::Object(map);
    let program = compile(".if,.then,.else,.elif,.end").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(10),
            ZqValue::from(20),
            ZqValue::from(30),
            ZqValue::from(35),
            ZqValue::from(40)
        ]
    );
}

#[test]
fn if_with_elif_matches_jq_branching() {
    let q = compile("if .a then 1 elif .b then 2 else 3 end").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": true, "b": false}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let out = execute(&q, &ZqValue::from_json(json!({"a": false, "b": true}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2)]);

    let out = execute(&q, &ZqValue::from_json(json!({"a": false, "b": false}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(3)]);
}

#[test]
fn if_without_else_defaults_to_identity_like_jq() {
    let q = compile("if . then 1 end").expect("compile");
    let out = execute(&q, &ZqValue::Bool(true)).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let out = execute(&q, &ZqValue::Bool(false)).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn if_uses_stream_semantics_like_jq() {
    let q = compile("if (true, false) then \"T\" else \"F\" end").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("T".to_string()),
            ZqValue::String("F".to_string())
        ]
    );

    let q = compile("if empty then 1 else 2 end").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn if_branch_expressions_can_emit_multiple_results() {
    let q = compile("if true then (1, 2) else 3 end").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);

    let q = compile("if false then 1 else (2, 3) end").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(3)]);

    let q = compile("if .[] then . else 0 end").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!([0, 1])),
            ZqValue::from_json(json!([0, 1]))
        ]
    );
}

#[test]
fn if_conditionals_match_upstream_jq_conditionals_block() {
    // jq/tests/jq.test
    let q = compile("[if 1,null,2 then 3 else 4 end]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3, 4, 3]));

    let q = compile("[if empty then 3 else 4 end]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));

    let q = compile("[if 1 then 3,4 else 5 end]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3, 4]));

    let q = compile("[if null then 3 else 5,6 end]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([5, 6]));

    let q = compile("[if true then 3 end]").expect("compile");
    let out = execute(&q, &ZqValue::from(7)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3]));

    let q = compile("[if false then 3 end]").expect("compile");
    let out = execute(&q, &ZqValue::from(7)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([7]));

    let q = compile("[if false then 3 elif false then 4 else . end]").expect("compile");
    let out = execute(&q, &ZqValue::from(7)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([7]));
}

#[test]
fn if_field_truthiness_and_elif_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let input = ZqValue::from_json(json!([
        {"foo":0},
        {"foo":1},
        {"foo":[]},
        {"foo":true},
        {"foo":false},
        {"foo":null},
        {"foo":"foo"},
        {}
    ]));

    let q = compile("[.[] | if .foo then \"yep\" else \"nope\" end]").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!(["yep", "yep", "yep", "yep", "nope", "nope", "yep", "nope"])
    );

    let q = compile("[.[] | if .baz then \"strange\" elif .foo then \"yep\" else \"nope\" end]")
        .expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!(["yep", "yep", "yep", "yep", "nope", "nope", "yep", "nope"])
    );
}

#[test]
fn if_elif_scalar_case_matches_upstream_man_test() {
    // jq/tests/man.test
    let q = compile("if . == 0 then \"zero\" elif . == 1 then \"one\" else \"many\" end")
        .expect("compile");

    let out = execute(&q, &ZqValue::from(0)).expect("execute");
    assert_eq!(out, vec![ZqValue::String("zero".to_string())]);

    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(out, vec![ZqValue::String("one".to_string())]);

    let out = execute(&q, &ZqValue::from(2)).expect("execute");
    assert_eq!(out, vec![ZqValue::String("many".to_string())]);
}

#[test]
fn pipe_is_supported_inside_parentheses_and_if_subqueries() {
    let q = compile("(1 | . + 1), (2 | . * 3)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(6)]);

    let q = compile("if (.[] | . > 0) then . else 0 end").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from(0), ZqValue::from_json(json!([0, 1]))]
    );
}

#[test]
fn pipe_is_supported_in_function_arguments() {
    let q = compile("map(. + 1 | tostring)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["2", "3"]));
}

#[test]
fn array_literal_accepts_full_query_body() {
    let q = compile("[1 | . + 1]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2]));

    let q = compile("[(1, 2) | . * 10]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([10, 20]));
}

#[test]
fn empty_produces_no_values() {
    let program = compile("empty").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn try_catch_handles_error_value() {
    let program = compile("try error(\"boom\") catch .").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("boom".to_string())]);
}

#[test]
fn error_argument_accepts_subquery() {
    let q = compile("try error(1 | tostring) catch .").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("1".to_string())]);
}

#[test]
fn try_catch_handles_runtime_error() {
    let program = compile("try (1 / 0) catch .").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::String(
            "number (1) and number (0) cannot be divided because the divisor is zero".to_string()
        )]
    );
}

#[test]
fn try_does_not_invoke_catch_on_empty() {
    let program = compile("try empty catch 1").expect("compile");
    let out = execute(&program, &ZqValue::Null).expect("execute");
    assert!(out.is_empty(), "out={out:?}");
}

#[test]
fn try_without_catch_matches_jq_backtrack_form() {
    let q = compile("try (1 / 0)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert!(out.is_empty(), "out={out:?}");

    let q = compile(".[] | try (1 / .)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 0, -1]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(-1)]);
}

#[test]
fn postfix_optional_matches_jq_try_backtrack_semantics() {
    // man.test:
    // .[] | (1 / .)?
    let q = compile(".[] | (1 / .)?").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 0, -1]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(-1)]);

    // jq.test:
    // [.[] | tonumber?]
    let q = compile("[.[] | tonumber?]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["1", "x", 2]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2]));

    // jq.test:
    // [if error then 1 else 2 end?]
    let q = compile("[if error then 1 else 2 end?]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn try_and_optional_forms_match_upstream_man_cases() {
    // jq/tests/man.test:
    // try .a catch ". is not an object"
    let q = compile("try .a catch \". is not an object\"").expect("compile");
    let out = execute(&q, &ZqValue::Bool(true)).expect("execute");
    assert_eq!(out, vec![ZqValue::String(". is not an object".to_string())]);

    // jq/tests/man.test:
    // [.[]|try .a]
    let q = compile("[.[]|try .a]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{}, true, {"a": 1}]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([null, 1]));

    // jq/tests/man.test:
    // [.[] | .a?]
    let q = compile("[.[] | .a?]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{}, true, {"a": 1}]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([null, 1]));

    // jq/tests/man.test:
    // [.[] | tonumber?]
    let q = compile("[.[] | tonumber?]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["1", "invalid", "3", 4]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 3, 4]));

    // jq/tests/man.test:
    // try error("some exception") catch .
    let q = compile("try error(\"some exception\") catch .").expect("compile");
    let out = execute(&q, &ZqValue::Bool(true)).expect("execute");
    assert_eq!(out, vec![ZqValue::String("some exception".to_string())]);
}

#[test]
fn try_catch_streaming_and_nesting_match_upstream_jq_cases() {
    // jq/tests/jq.test:
    // 1, try error(2), 3
    let q = compile("1, try error(2), 3").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(3)]);

    // jq/tests/jq.test:
    // 1 + try 2 catch 3 + 4
    let q = compile("1 + try 2 catch 3 + 4").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from(7)]);

    // jq/tests/jq.test:
    // {x: try 1, y: try error catch 2, z: if true then 3 end}
    let q = compile("{x: try 1, y: try error catch 2, z: if true then 3 end}").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"x": 1, "y": 2, "z": 3}));

    // jq/tests/jq.test:
    // .[] | try error catch .
    let q = compile(".[] | try error catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, null, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::Null, ZqValue::from(2)]);

    // jq/tests/jq.test:
    // try ["OK", (.[] | error)] catch ["KO", .]
    let q = compile("try [\"OK\", (.[] | error)] catch [\"KO\", .]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": ["b"], "c": ["d"]}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["KO", ["b"]]));

    // jq/tests/jq.test #1859:
    // "foo" | try ((try . catch "caught too much") | error) catch "caught just right"
    let q = compile(
        "\"foo\" | try ((try . catch \"caught too much\") | error) catch \"caught just right\"",
    )
    .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("caught just right".to_string())]);
}

#[test]
fn try_and_catch_can_be_field_names() {
    let mut map = IndexMap::new();
    map.insert("try".to_string(), ZqValue::from(5));
    map.insert("catch".to_string(), ZqValue::from(6));
    let input = ZqValue::Object(map);
    let program = compile(".try,.catch").expect("compile");
    let out = execute(&program, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(5), ZqValue::from(6)]);
}

#[test]
fn length_builtin_matches_jq_core_cases() {
    let q = compile("length").expect("compile");
    assert_eq!(
        execute(&q, &ZqValue::Null).expect("execute"),
        vec![ZqValue::from(0)]
    );
    assert_eq!(
        execute(
            &q,
            &ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)])
        )
        .expect("execute"),
        vec![ZqValue::from(3)]
    );
    assert_eq!(
        execute(&q, &ZqValue::String("abc".to_string())).expect("execute"),
        vec![ZqValue::from(3)]
    );
}

#[test]
fn length_bool_is_error_like_jq() {
    let q = compile("length").expect("compile");
    let err = execute(&q, &ZqValue::Bool(true)).expect_err("must fail");
    assert_eq!(err, "boolean (true) has no length");
}

#[test]
fn abs_matches_upstream_definition_cases() {
    let q = compile("abs").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!("abc"));

    let q = compile("map(abs)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([-10, -1.1, -0.1]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([10, 1.1, 0.1]));

    let q = compile("map(abs == length) | unique").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([-10, -1.1, -0.1]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true]));
}

#[test]
fn fabs_matches_libm_behavior_and_errors() {
    let q = compile("map(fabs)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([-10, -1.1, -0.1]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([10, 1.1, 0.1]));

    let q = compile("fabs").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("abc"))).expect_err("must fail");
    assert_eq!(err, "string (\"abc\") number required");
}

#[test]
fn numeric_predicates_match_jq_core_rules() {
    let input = ZqValue::from_json(json!([0, 1, -2, 1e-310, "x", null]));

    let q = compile(".[] | isinfinite").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![false, false, false, false, false, false]
            .into_iter()
            .map(ZqValue::Bool)
            .collect::<Vec<_>>()
    );

    let q = compile(".[] | isnan").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![false, false, false, false, false, false]
            .into_iter()
            .map(ZqValue::Bool)
            .collect::<Vec<_>>()
    );

    let q = compile(".[] | isnormal").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![false, true, true, false, false, false]
            .into_iter()
            .map(ZqValue::Bool)
            .collect::<Vec<_>>()
    );

    let q = compile(".[] | isfinite").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![true, true, true, true, false, false]
            .into_iter()
            .map(ZqValue::Bool)
            .collect::<Vec<_>>()
    );
}

#[test]
fn special_number_literals_follow_jq_non_decnum_forms() {
    let q = compile("[nan, -nan, infinite, -infinite] | map([isnan, isinfinite, isfinite])")
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!([
            [true, false, false],
            [true, false, false],
            [false, true, false],
            [false, true, false]
        ])]
    );

    let q = compile(r#"[. * (nan, -nan)]"#).expect("compile");
    let out = execute(&q, &ZqValue::String("abc".to_string())).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!([null, null])]
    );
}

#[test]
fn nan_indices_and_slices_match_upstream_jq_cases() {
    let q = compile("[range(3)] | .[nan:1]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!([0])]
    );

    let q = compile("[range(3)] | .[1:nan]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!([1, 2])]
    );

    let q = compile("[range(3)] | .[nan]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!(null)]
    );

    let q = compile(r#"try ([range(3)] | .[nan] = 9) catch ."#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!("Cannot set array element at NaN index")]
    );

    let q = compile("del(.[nan])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!([1, 2, 3])]
    );
}

#[test]
fn finites_and_normals_follow_builtin_jq_definitions() {
    let q = compile(".[] | finites").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, "x", null, -2]))).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!(0), json!(1), json!(-2)]
    );

    let q = compile(".[] | normals").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, "x", -2, 1e-310]))).expect("execute");
    assert_eq!(
        out.into_iter().map(|v| v.into_json()).collect::<Vec<_>>(),
        vec![json!(1), json!(-2)]
    );
}

#[test]
fn select_filter_keeps_only_truthy_inputs() {
    let q = compile(".[] | select(. > 1)").expect("compile");
    let input = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]);
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(3)]);
}

#[test]
fn map_filter_matches_jq_shape() {
    let q = compile("map(. + 1)").expect("compile");
    let input = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]);
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, 3, 4]));
}

#[test]
fn map_on_object_maps_values_in_order() {
    let q = compile("map(. + 1)").expect("compile");
    let mut map = IndexMap::new();
    map.insert("a".to_string(), ZqValue::from(1));
    map.insert("b".to_string(), ZqValue::from(2));
    let out = execute(&q, &ZqValue::Object(map)).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, 3]));
}

#[test]
fn map_non_iterable_returns_jq_style_error() {
    let q = compile("map(. + 1)").expect("compile");
    let err = execute(&q, &ZqValue::from(1)).expect_err("must fail");
    assert_eq!(err, "Cannot iterate over number (1)");
}

#[test]
fn type_builtin_matches_jq_names() {
    let q = compile("type").expect("compile");
    assert_eq!(
        execute(&q, &ZqValue::Object(IndexMap::new())).expect("execute"),
        vec![ZqValue::String("object".to_string())]
    );
    assert_eq!(
        execute(&q, &ZqValue::Array(Vec::new())).expect("execute"),
        vec![ZqValue::String("array".to_string())]
    );
    assert_eq!(
        execute(&q, &ZqValue::String("x".to_string())).expect("execute"),
        vec![ZqValue::String("string".to_string())]
    );
}

#[test]
fn add_builtin_follows_jq_reduce_definition() {
    let q = compile("add").expect("compile");
    let out = execute(
        &q,
        &ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from(6)]);

    let out = execute(&q, &ZqValue::Array(Vec::new())).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn add_builtin_on_null_errors_like_jq() {
    let q = compile("add").expect("compile");
    let err = execute(&q, &ZqValue::Null).expect_err("must fail");
    assert_eq!(err, "Cannot iterate over null (null)");
}

#[test]
fn add_filter_form_matches_upstream_cases() {
    let q = compile("[add(null), add(range(range(10))), add(empty), add(10,range(10))]")
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([null, 120, null, 55]));

    let q = compile("map(add)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            [],
            [1, 2, 3],
            ["a", "b", "c"],
            [[3], [4, 5], [6]],
            [{"a": 1}, {"b": 2}, {"a": 3}]
        ])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([null, 6, "abc", [3, 4, 5, 6], {"a": 3, "b": 2}])
    );
}

#[test]
fn keys_and_keys_unsorted_match_jq_behavior() {
    let q_keys = compile("keys").expect("compile");
    let q_unsorted = compile("keys_unsorted").expect("compile");
    let mut map = IndexMap::new();
    map.insert("b".to_string(), ZqValue::from(2));
    map.insert("a".to_string(), ZqValue::from(1));
    let input = ZqValue::Object(map);

    let sorted = execute(&q_keys, &input).expect("execute");
    assert_eq!(sorted[0].clone().into_json(), json!(["a", "b"]));

    let unsorted = execute(&q_unsorted, &input).expect("execute");
    assert_eq!(unsorted[0].clone().into_json(), json!(["b", "a"]));
}

#[test]
fn entries_builtins_follow_jq_defs() {
    let to_entries = compile("to_entries").expect("compile");
    let out_obj = execute(&to_entries, &ZqValue::from_json(json!({"a":1,"b":2}))).expect("execute");
    assert_eq!(
        out_obj[0].clone().into_json(),
        json!([{"key":"a","value":1},{"key":"b","value":2}])
    );

    let out_arr = execute(&to_entries, &ZqValue::from_json(json!([10, 20]))).expect("execute");
    assert_eq!(
        out_arr[0].clone().into_json(),
        json!([{"key":0,"value":10},{"key":1,"value":20}])
    );

    let from_entries = compile("from_entries").expect("compile");
    let out_from = execute(
        &from_entries,
        &ZqValue::from_json(json!([{"key":"a","value":1},{"Key":"b","Value":2}])),
    )
    .expect("execute");
    assert_eq!(out_from[0].clone().into_json(), json!({"a":1,"b":2}));
}

#[test]
fn with_entries_matches_jq_definition() {
    let q = compile("with_entries({key:(\"KEY_\" + .key), value:.value})").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1,"b":2}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"KEY_a":1,"KEY_b":2}));

    let q = compile("with_entries(empty)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1,"b":2}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({}));

    let q = compile("try with_entries(.value) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1}))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "Cannot index number with string \"key\""
        ))]
    );
}

#[test]
fn recurse_forms_match_upstream_core_cases() {
    let q = compile("recurse(.foo[])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"foo":[{"foo": []}, {"foo":[{"foo":[]}]}]})),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter()
            .map(|v| v.into_json())
            .collect::<Vec<serde_json::Value>>(),
        vec![
            json!({"foo":[{"foo":[]},{"foo":[{"foo":[]}]}]}),
            json!({"foo":[]}),
            json!({"foo":[{"foo":[]}]}),
            json!({"foo":[]}),
        ]
    );

    let q = compile("recurse").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":0,"b":[1]}))).expect("execute");
    assert_eq!(
        out.into_iter()
            .map(|v| v.into_json())
            .collect::<Vec<serde_json::Value>>(),
        vec![json!({"a":0,"b":[1]}), json!(0), json!([1]), json!(1)]
    );

    let q = compile("recurse(. * .; . < 20)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(2))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from(2), ZqValue::from(4), ZqValue::from(16)]
    );

    let q = compile("recurse").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn recursive_descent_operator_matches_jq_cases() {
    // jq.test:
    // [..]
    let q = compile("[..]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, [[2]], {"a":[1]}]))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([[1, [[2]], {"a":[1]}], 1, [[2]], [2], 2, {"a":[1]}, [1], 1])
    );

    // man.test:
    // .. | .a?
    let q = compile(".. | .a?").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[{"a":1}]]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn map_values_follows_jq_assignment_style() {
    let q = compile("map_values(.+1)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1,"b":2}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({"a":2,"b":3}));

    let q = compile("map_values(select(. > 1))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, 3]));

    let q = compile("map_values(empty)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a":1}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!({}));
}

#[test]
fn has_builtin_matches_upstream_cases() {
    let q = compile("map(has(\"foo\"))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{"foo":42}, {}]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, false]));

    let q = compile("map(has(2))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[0, 1], ["a", "b", "c"]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, true]));

    let q = compile("has(\"foo\")").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn has_builtin_errors_match_jq() {
    let q = compile("has(0)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!({"a":1}))).expect_err("must fail");
    assert_eq!(err, "Cannot check whether object has a number key");

    let q = compile("has(\"a\")").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect_err("must fail");
    assert_eq!(err, "Cannot check whether array has a string key");
}

#[test]
fn in_builtin_matches_upstream_definition() {
    let q = compile(".[] | in({\"foo\":42})").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["foo", "bar"]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true), ZqValue::Bool(false)]);

    let q = compile("map(in([0,1]))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([2, 0]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, true]));
}

#[test]
fn uppercase_in_matches_jq_sql_style_definition() {
    // jq/tests/jq.test
    let q = compile("range(5;10)|IN(range(10))").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(true)
        ]
    );

    let q = compile("range(10;12)|IN(range(10))").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false), ZqValue::Bool(false)]);

    let q = compile("IN(range(10;20); range(10))").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile("IN(range(5;20); range(10))").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn index_and_join_match_upstream_sql_style_cases() {
    // jq/tests/jq.test
    let q = compile("INDEX(range(5)|[., \"foo\\(.)\"]; .[0])").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"0":[0,"foo0"],"1":[1,"foo1"],"2":[2,"foo2"],"3":[3,"foo3"],"4":[4,"foo4"]})
    );

    let q = compile(
            "JOIN({\"0\":[0,\"abc\"],\"1\":[1,\"bcd\"],\"2\":[2,\"def\"],\"3\":[3,\"efg\"],\"4\":[4,\"fgh\"]}; .[0]|tostring)",
        )
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[5, "foo"], [3, "bar"], [1, "foobar"]])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            [[5, "foo"], null],
            [[3, "bar"], [3, "efg"]],
            [[1, "foobar"], [1, "bcd"]]
        ])
    );
}

#[test]
fn index_and_join_overloads_match_builtin_jq_definitions() {
    let q = compile("INDEX(.[0])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[0, "a"], [1, "b"], [2, "c"]])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"0":[0,"a"],"1":[1,"b"],"2":[2,"c"]})
    );

    let q = compile("JOIN({\"0\":\"zero\",\"1\":\"one\"}; .[]; tostring)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 0, 2]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1, "one"]), json!([0, "zero"]), json!([2, null])]
    );

    let q = compile("JOIN({\"0\":\"zero\",\"1\":\"one\"}; .[]; tostring; .[1])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 0, 2]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("one"), json!("zero"), json!(null)]
    );
}

#[test]
fn combinations_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("combinations").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[1, 2], [3, 4]]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1, 3]), json!([1, 4]), json!([2, 3]), json!([2, 4])]
    );

    let q = compile("combinations(2)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([0, 0]), json!([0, 1]), json!([1, 0]), json!([1, 1])]
    );
}

#[test]
fn combinations_empty_and_type_errors_match_jq_definition() {
    let q = compile("combinations").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([]))]);

    let err = execute(&q, &ZqValue::from_json(json!([1]))).expect_err("must fail");
    assert_eq!(err, "Cannot iterate over number (1)");
}

#[test]
fn truncate_stream_and_fromstream_match_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("truncate_stream([[0],\"a\"],[[1,0],\"b\"],[[1,0]],[[1]])").expect("compile");
    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([[0], "b"]), json!([[0]])]
    );

    let q = compile("fromstream(1|truncate_stream([[0],\"a\"],[[1,0],\"b\"],[[1,0]],[[1]]))")
        .expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!(["b"]))]);
}

#[test]
fn tostream_fromstream_roundtrip_matches_upstream_man_case() {
    // jq/tests/man.test
    let q = compile(". as $dot|fromstream($dot|tostream)|.==$dot").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, {"a": 1}, {"b": 2}]]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn tostream_event_sequence_matches_jq_definition_shape() {
    // jq/src/builtin.jq tostream definition expanded:
    // emits leaf events [path,value] and container close markers [path].
    let q = compile("tostream").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, {"a": 1}, {"b": 2}]]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!([[0], 0]),
            json!([[1, 0], 1]),
            json!([[1, 1, "a"], 1]),
            json!([[1, 1, "a"]]),
            json!([[1, 2, "b"], 2]),
            json!([[1, 2, "b"]]),
            json!([[1, 2]]),
            json!([[1]])
        ]
    );
}

#[test]
fn fromstream_reports_malformed_stream_errors() {
    let q = compile("fromstream(.[])").expect("compile");

    let err = execute(&q, &ZqValue::from_json(json!([1]))).expect_err("must fail");
    assert_eq!(err, "fromstream: stream event must be an array");

    let err = execute(&q, &ZqValue::from_json(json!([[[0], 1, 2]]))).expect_err("must fail");
    assert_eq!(err, "fromstream: invalid stream event shape");

    let err = execute(&q, &ZqValue::from_json(json!([[[0.5], 1]]))).expect_err("must fail");
    assert_eq!(err, "fromstream: path index must be a non-negative integer");

    let err = execute(&q, &ZqValue::from_json(json!([[[]]]))).expect_err("must fail");
    assert_eq!(err, "fromstream: invalid root close marker");
}

#[test]
fn debug_matches_builtin_jq_output_semantics() {
    // jq/src/builtin.jq:
    // def debug(msgs): (msgs | debug | empty), .;
    let q = compile("debug(.foo)").expect("compile");
    let input = ZqValue::from_json(json!({"foo":"trace","x":1}));
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![input]);

    let q = compile("debug(empty)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([1, 2]))]);
}

#[test]
fn debug_propagates_message_filter_errors_like_jq_definition() {
    let q = compile("try debug(error(\"boom\")) catch .").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("boom"))]);
}

#[test]
fn fromdate_and_todate_iso8601_match_upstream_core_cases() {
    // jq/tests/man.test
    let q = compile("fromdate").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("2015-03-05T23:51:47Z"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1425599507i64)]);

    let q = compile("fromdateiso8601").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("2015-03-05T23:51:47Z"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1425599507i64)]);

    let q = compile("todateiso8601").expect("compile");
    let out = execute(&q, &ZqValue::from(1425599507i64)).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("2015-03-05T23:51:47Z"))]);

    let q = compile("todate").expect("compile");
    let out = execute(&q, &ZqValue::from(1425599507i64)).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("2015-03-05T23:51:47Z"))]);
}

#[test]
fn fromdate_and_todate_errors_match_expected_shapes() {
    let q = compile("try fromdate catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("2015-03-05"))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "date \"2015-03-05\" does not match format \"%Y-%m-%dT%H:%M:%SZ\""
        ))]
    );

    let q = compile("try todateiso8601 catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("bad"))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "strftime/1 requires parsed datetime inputs"
        ))]
    );
}

#[test]
fn time_builtins_match_upstream_jq_cases() {
    // jq/tests/jq.test + jq/tests/man.test
    let q = compile("gmtime").expect("compile");
    let out = execute(&q, &ZqValue::from(1425599507i64)).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([2015, 2, 5, 23, 51, 47, 4, 63]))]
    );

    let q = compile("gmtime[5]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(serde_json::from_str::<serde_json::Value>("1425599507.25").unwrap()),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(
            serde_json::from_str::<serde_json::Value>("47.25").unwrap()
        )]
    );

    let q = compile(r#"strftime("%Y-%m-%dT%H:%M:%SZ")"#).expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([2015, 2, 5, 23, 51, 47, 4, 63])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("2015-03-05T23:51:47Z"))]);

    let q = compile(r#"strftime("%A, %B %d, %Y")"#).expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(
            serde_json::from_str::<serde_json::Value>("1435677542.822351").unwrap(),
        ),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!("Tuesday, June 30, 2015"))]
    );

    let q = compile("mktime").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([2024, 8, 21]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1726876800i64)]);

    let q = compile(r#"[strptime("%Y-%m-%dT%H:%M:%SZ")|(.,mktime)]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("2015-03-05T23:51:47Z"))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([
            [2015, 2, 5, 23, 51, 47, 4, 63],
            1425599507
        ]))]
    );
}

#[test]
fn repeat_matches_upstream_man_case_with_optional_backtrack() {
    // jq/tests/man.test
    let q = compile("[repeat(.*2, error)?]").expect("compile");
    let out = execute(&q, &ZqValue::from(1)).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([2]))]);
}

#[test]
fn try_preserves_values_emitted_before_error() {
    // This is required for repeat(...)? semantics and matches jq stream behavior.
    let q = compile("[(1, error(\"boom\"))?]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([1]))]);
}

#[test]
fn input_and_inputs_follow_builtin_jq_shape_in_single_input_runtime() {
    let q = compile("try input catch .").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("break"))]);

    let q = compile("inputs").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"x": 1}))).expect("execute");
    assert!(out.is_empty(), "out={out:?}");

    let q = compile("[inputs]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([]))]);
}

#[test]
fn halt_error_builtin_forms_match_jq_builtin_contract() {
    // jq/src/builtin.jq: def halt_error: halt_error(5);
    let q = compile("halt_error").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("xy"))).expect_err("must halt");
    let (code, stderr) = decode_halt_error(&err).expect("halt payload");
    assert_eq!(code, 5);
    assert_eq!(stderr, "xy");

    let q = compile("halt_error(11)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!({"a":"xyz"}))).expect_err("must halt");
    let (code, stderr) = decode_halt_error(&err).expect("halt payload");
    assert_eq!(code, 11);
    assert_eq!(stderr, "{\"a\":\"xyz\"}");

    let q = compile("halt_error(1)").expect("compile");
    let err = execute(&q, &ZqValue::Null).expect_err("must halt");
    let (code, stderr) = decode_halt_error(&err).expect("halt payload");
    assert_eq!(code, 1);
    assert_eq!(stderr, "");
}

#[test]
fn halt_error_is_not_caught_by_try_catch() {
    let q = compile("try halt_error(1) catch .").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("xy"))).expect_err("must halt");
    let (code, stderr) = decode_halt_error(&err).expect("halt payload");
    assert_eq!(code, 1);
    assert_eq!(stderr, "xy");
}

#[test]
fn onig_match_and_test_core_cases() {
    // jq/tests/onig.test
    let q = compile("[match(\"( )*\"; \"g\")]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            {"offset":0,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},
            {"offset":1,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},
            {"offset":2,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},
            {"offset":3,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]}
        ])
    );

    let q = compile("[match(\"( )*\"; \"gn\")]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));

    let q = compile("[match([\"(bar)\"])]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("foo bar"))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([{"offset":4,"length":3,"string":"bar","captures":[{"offset":4,"length":3,"string":"bar","name":null}]}])
    );

    let q = compile("[test(\"( )*\"; \"gn\")]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false]));
}

#[test]
fn onig_capture_and_scan_core_cases() {
    // jq/tests/onig.test
    let q = compile("\"a\",\"b\",\"c\" | capture(\"(?<x>a)?b?\")").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"x":"a"}), json!({"x":null}), json!({"x":null}),]
    );

    let q = compile("[.[] | scan(\", \")]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["a,b, c, d, e,f", ", a,b, c, d, e,f, "])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([", ", ", ", ", ", ", ", ", ", ", ", ", ", ", "])
    );
}

#[test]
fn onig_sub_and_gsub_core_cases() {
    // jq/tests/onig.test
    let q = compile(r#"[.[] | sub(", "; ":")]"#).expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["a,b, c, d, e,f", ", a,b, c, d, e,f, "])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!(["a,b:c, d, e,f", ":a,b, c, d, e,f, "])
    );

    let q = compile(r#"sub("^(?<head>.)"; "Head=\(.head) Tail=")"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abcdef"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!("Head=a Tail=bcdef"));

    let q = compile(r#"gsub("(?<d>\\d)"; ":\(.d);")"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("a1b2"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!("a:1;b:2;"));

    let q = compile(r#"gsub("(?=u)"; "u")"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("qux"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!("quux"));

    let q = compile(r#"[gsub("a"; "b", "c")]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("a"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["b", "c"]));

    let q = compile(r#"[sub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)", "c")]"#)
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("aB"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["AB", "aB", "cB"]));

    let q = compile(r#"[gsub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)", "c")]"#)
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("aB"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["AB", "ab", "cc"]));
}

#[test]
fn onig_splits_and_split_two_args_match_upstream() {
    // jq/tests/onig.test and jq/tests/manonig.test
    let q = compile(r#"[splits("")]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("ab"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["", "a", "b", ""]));

    let q = compile(r#"[splits("c")]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("ab"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["ab"]));

    let q = compile(r#"[splits("a+"; "i")]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abAABBabA"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["", "b", "BB", "b", ""]));

    let q = compile(r#"[splits("b+"; "i")]"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abAABBabA"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a", "AA", "a", "A"]));

    let q = compile(r#"split(", *"; null)"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("ab,cd, ef"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["ab", "cd", "ef"]));

    let q = compile(r#"splits(",? *"; "n")"#).expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("ab,cd ef,  gh"))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("ab"), json!("cd"), json!("ef"), json!("gh")]
    );
}

#[test]
fn tonumber_builtin_matches_upstream_happy_path() {
    let q = compile(".[] | tonumber").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, "1"]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(1)]);
}

#[test]
fn tonumber_builtin_errors_like_jq() {
    let q = compile("tonumber").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("abc"))).expect_err("must fail");
    assert_eq!(err, "string (\"abc\") cannot be parsed as a number");

    let err = execute(&q, &ZqValue::from_json(json!([]))).expect_err("must fail");
    assert_eq!(err, "array ([]) cannot be parsed as a number");
}

#[test]
fn tostring_builtin_matches_upstream_happy_path() {
    let q = compile(".[] | tostring").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, "1", [1]]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("1".to_string()),
            ZqValue::String("1".to_string()),
            ZqValue::String("[1]".to_string())
        ]
    );
}

#[test]
fn toboolean_builtin_matches_upstream_happy_path() {
    let q = compile(".[] | toboolean").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["true", "false", true, false])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(false)
        ]
    );
}

#[test]
fn toboolean_builtin_errors_like_jq() {
    let q = compile("toboolean").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(0))).expect_err("must fail");
    assert_eq!(err, "number (0) cannot be parsed as a boolean");

    let err = execute(&q, &ZqValue::from_json(json!("tru"))).expect_err("must fail");
    assert_eq!(err, "string (\"tru\") cannot be parsed as a boolean");
}

#[test]
fn tojson_and_fromjson_roundtrip_matches_upstream() {
    let q = compile(".[] | tojson | fromjson").expect("compile");
    let input = ZqValue::from_json(json!(["foo", 1, ["a", 1, "b", 2, {"foo":"bar"}]]));
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("foo")),
            ZqValue::from_json(json!(1)),
            ZqValue::from_json(json!(["a", 1, "b", 2, {"foo":"bar"}]))
        ]
    );
}

#[test]
fn tojson_encodes_like_jq_dump_string() {
    let q = compile(".[] | tojson").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, "foo", ["foo"]]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("1".to_string()),
            ZqValue::String("\"foo\"".to_string()),
            ZqValue::String("[\"foo\"]".to_string())
        ]
    );

    let q = compile("tojson").expect("compile");
    let scientific = ZqValue::Number(serde_json::Number::from_string_unchecked(
        "100e-2".to_string(),
    ));
    let out = execute(&q, &scientific).expect("execute");
    assert_eq!(out, vec![ZqValue::String("1.00".to_string())]);
}

#[test]
fn fromjson_requires_string_like_jq() {
    let q = compile("fromjson").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!({"a":1}))).expect_err("must fail");
    assert_eq!(err, "object ({\"a\":1}) only strings can be parsed");
}

#[test]
fn fromjson_accepts_nan_suffix_and_isnan_matches_jq() {
    let q = compile("fromjson | isnan").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("nan1234"))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("fromjson").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("nan1234"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(null));
}

#[test]
fn utf8bytelength_matches_upstream_and_errors() {
    let q = compile("utf8bytelength").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("asdfμ"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(6)]);

    let err = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect_err("must fail");
    assert_eq!(err, "array ([1,2]) only strings have UTF-8 byte length");
}

#[test]
fn startswith_and_endswith_match_upstream_cases() {
    let q = compile(".[] | startswith(\"foo\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["fo", "foo", "barfoo", "foobar", "barfoob"])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(false)
        ]
    );

    let q = compile(".[] | endswith(\"foo\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["fo", "foo", "barfoo", "foobar", "barfoob"])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::Bool(false),
            ZqValue::Bool(true),
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Bool(false)
        ]
    );
}

#[test]
fn startswith_and_endswith_type_errors_match_jq() {
    let q = compile("startswith(\"x\")").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "startswith() requires string inputs");

    let q = compile("endswith(\"x\")").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "endswith() requires string inputs");
}

#[test]
fn split_matches_upstream_cases() {
    let q = compile(".[] | split(\", \")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["a,b, c, d, e,f", ", a,b, c, d, e,f, "])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a,b", "c", "d", "e,f"]));
    assert_eq!(
        out[1].clone().into_json(),
        json!(["", "a,b", "c", "d", "e,f", ""])
    );

    let q = compile("split(\"\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("abc"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(["a", "b", "c"]));
}

#[test]
fn split_type_error_matches_jq() {
    let q = compile("split(\",\")").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "split input and separator must be strings");
}

#[test]
fn explode_and_implode_match_upstream_cases() {
    let explode = compile("explode").expect("compile");
    let out = execute(&explode, &ZqValue::from_json(json!("foobar"))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([102, 111, 111, 98, 97, 114])
    );

    let implode = compile("implode").expect("compile");
    let out = execute(&implode, &ZqValue::from_json(json!([65, 66, 67]))).expect("execute");
    assert_eq!(out, vec![ZqValue::String("ABC".to_string())]);
}

#[test]
fn implode_edge_behavior_matches_jq() {
    let q = compile("implode|explode").expect("compile");
    let input = ZqValue::from_json(json!([
        -1, 0, 1, 2, 3, 1114111, 1114112, 55295, 55296, 57343, 57344, 1.1, 1.9
    ]));
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([65533, 0, 1, 2, 3, 1114111, 65533, 55295, 65533, 65533, 57344, 1, 1])
    );
}

#[test]
fn implode_errors_match_jq() {
    let q = compile("implode").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(123))).expect_err("must fail");
    assert_eq!(err, "implode input must be an array");

    let err = execute(&q, &ZqValue::from_json(json!(["a"]))).expect_err("must fail");
    assert_eq!(
        err,
        "string (\"a\") can't be imploded, unicode codepoint needs to be numeric"
    );
}

#[test]
fn trimstr_family_matches_upstream_cases() {
    let q = compile(".[] | ltrimstr(\"foo\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["fo", "foo", "barfoo", "foobar", "afoo"])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("fo")),
            ZqValue::from_json(json!("")),
            ZqValue::from_json(json!("barfoo")),
            ZqValue::from_json(json!("bar")),
            ZqValue::from_json(json!("afoo"))
        ]
    );

    let q = compile(".[] | rtrimstr(\"foo\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["fo", "foo", "barfoo", "foobar", "foob"])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("fo")),
            ZqValue::from_json(json!("")),
            ZqValue::from_json(json!("bar")),
            ZqValue::from_json(json!("foobar")),
            ZqValue::from_json(json!("foob"))
        ]
    );

    let q = compile(".[] | trimstr(\"foo\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["fo", "foo", "barfoo", "foobarfoo", "foob"])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("fo")),
            ZqValue::from_json(json!("")),
            ZqValue::from_json(json!("bar")),
            ZqValue::from_json(json!("bar")),
            ZqValue::from_json(json!("b"))
        ]
    );

    let q = compile(".[] | trimstr(\"\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["a", "xx", ""]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("a")),
            ZqValue::from_json(json!("xx")),
            ZqValue::from_json(json!(""))
        ]
    );
}

#[test]
fn trimstr_family_type_errors_match_jq() {
    let q = compile("ltrimstr(1)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("hi"))).expect_err("must fail");
    assert_eq!(err, "startswith() requires string inputs");

    let q = compile("rtrimstr(1)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("hi"))).expect_err("must fail");
    assert_eq!(err, "endswith() requires string inputs");

    let q = compile("ltrimstr(\"x\")").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "startswith() requires string inputs");
}

#[test]
fn trim_family_matches_upstream_cases() {
    let q = compile("map(trim), map(ltrim), map(rtrim)").expect("compile");
    let input = ZqValue::from_json(json!([
        " \n\t\r\u{000C}\u{000B}",
        "",
        "  ",
        "a",
        " a ",
        "abc",
        "  abc  ",
        "  abc",
        "abc  "
    ]));
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!(["", "", "", "a", "a", "abc", "abc", "abc", "abc"])
    );
    assert_eq!(
        out[1].clone().into_json(),
        json!(["", "", "", "a", "a ", "abc", "abc  ", "abc", "abc  "])
    );
    assert_eq!(
        out[2].clone().into_json(),
        json!(["", "", "", "a", " a", "abc", "  abc", "  abc", "abc"])
    );
}

#[test]
fn trim_family_type_error_matches_jq() {
    let q = compile("try trim catch ., try ltrim catch ., try rtrim catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(123))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("trim input must be a string")),
            ZqValue::from_json(json!("trim input must be a string")),
            ZqValue::from_json(json!("trim input must be a string"))
        ]
    );
}

#[test]
fn indices_index_rindex_match_upstream_string_cases() {
    let q = compile("[(index(\",\")), (rindex(\",\")), (indices(\",\"))]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("a,bc,def,ghij,klmno"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 13, [1, 4, 8, 13]]));

    let q = compile("[(index(\"aba\")), (rindex(\"aba\")), (indices(\"aba\"))]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("xababababax"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 7, [1, 3, 5, 7]]));
}

#[test]
fn indices_index_rindex_match_upstream_array_cases() {
    let q = compile("indices(1)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, 1, 2, 3, 4, 1, 5]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 6]));

    let q = compile("indices([1,2])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 1, 4, 2, 5, 1, 2, 6, 7])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 8]));

    let q = compile("index([1,2]), rindex([1,2])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 1, 4, 2, 5, 1, 2, 6, 7])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(8)]);
}

#[test]
fn indices_unicode_positions_follow_jq_codepoint_logic() {
    let q = compile("indices(\"o\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("🇬🇧oo"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, 3]));

    let out = execute(&q, &ZqValue::from_json(json!("ƒoo"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2]));
}

#[test]
fn index_empty_pattern_matches_upstream_regression() {
    let q = compile("index(\"\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(""))).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);
}

#[test]
fn contains_matches_upstream_string_cases() {
    let q = compile("contains(\"bar\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("foobar"))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("[contains(\"\"), contains(\"\\u0000\")]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("\u{0000}"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, true]));

    let q = compile("[contains(\"@\"), contains(\"\\u0000@\"), contains(\"\\u0000what\")]")
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("ab\u{0000}cd"))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, false, false]));
}

#[test]
fn contains_matches_upstream_array_and_object_cases() {
    let q = compile("contains([\"baz\", \"bar\"])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["foobar", "foobaz", "blarp"])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("contains([\"bazzzzz\", \"bar\"])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["foobar", "foobaz", "blarp"])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile("contains({foo: 12, bar: [{barp: 12}]})").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]})),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn inside_matches_upstream_definition() {
    let q = compile("inside(\"foobar\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("bar"))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("inside([\"foobar\", \"foobaz\", \"blarp\"])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["baz", "bar"]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let out = execute(&q, &ZqValue::from_json(json!(["bazzzzz", "bar"]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn contains_type_mismatch_errors_like_jq() {
    let q = compile("contains(1)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!("x"))).expect_err("must fail");
    assert_eq!(
        err,
        "string (\"x\") and number (1) cannot have their containment checked"
    );
}

#[test]
fn join_matches_upstream_cases() {
    let q = compile("join(\",\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(["1", 2, true, false, 3.4]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("1,2,true,false,3.4"))]);

    let q = compile(".[] | join(\",\")").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[], [null], [null, null], [null, null, null]])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!("")),
            ZqValue::from_json(json!("")),
            ZqValue::from_json(json!(",")),
            ZqValue::from_json(json!(",,"))
        ]
    );
}

#[test]
fn join_type_errors_match_upstream() {
    let q = compile("try join(\",\") catch .").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!(["1", "2", {"a":{"b":{"c":33}}}])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "string (\"1,2,\") and object ({\"a\":{\"b\":{\"c\":33}}}) cannot be added"
        ))]
    );
}

#[test]
fn reverse_and_ascii_case_match_upstream() {
    let q = compile("reverse").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([4, 3, 2, 1]))]);

    let q = compile("ascii_upcase").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("useful but not for é"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("USEFUL BUT NOT FOR é"))]);

    let q = compile("ascii_downcase").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("USEFUL BUT NOT FOR é"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("useful but not for é"))]);
}

#[test]
fn transpose_matches_upstream_core_cases() {
    let q = compile("transpose").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[1], [2, 3]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([[1, 2], [null, 3]]));

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn transpose_follows_jq_definition_on_non_arrays() {
    let q = compile("transpose").expect("compile");

    let out = execute(&q, &ZqValue::from_json(json!({"a":[1], "b":[2,3]}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([[1, 2], [null, 3]]));

    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "Cannot iterate over number (1)");

    let err = execute(&q, &ZqValue::from_json(json!([1, [2, 3]]))).expect_err("must fail");
    assert_eq!(err, "Cannot index number with number (0)");
}

#[test]
fn walk_matches_upstream_man_case() {
    let q = compile("walk(if type == \"array\" then sort else (.) end)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[4, 1, 7], [8, 5, 2], [3, 6, 9]])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([[1, 4, 7], [2, 5, 8], [3, 6, 9]])
    );
}

#[test]
fn walk_recurses_through_objects_and_arrays() {
    let q = compile("walk(if type == \"array\" then sort else (.) end)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"a":[3,1,2],"b":{"c":[2,1]}})),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!({"a":[1,2,3],"b":{"c":[1,2]}})
    );

    let q = compile("walk(if type == \"number\" then . + 1 else (.) end)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, [2], {"a": 3}, "x"]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, [3], {"a": 4}, "x"]));
}

#[test]
fn flatten_matches_upstream_core_cases() {
    let q = compile("flatten(2)").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, [2]], [1, [[3], 2]]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2, 1, [3], 2]));

    let q = compile("flatten").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, [2]], [1, [[3], 2]]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2, 1, 3, 2]));
}

#[test]
fn flatten_depth_errors_match_jq_definition() {
    let q = compile("try flatten(-1) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, [1], [[2]], [[[3]]]]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "flatten depth must not be negative"
        ))]
    );

    let q = compile("try flatten(\"a\") catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, [2]]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "string (\"a\") and number (1) cannot be subtracted"
        ))]
    );

    let q = compile("try flatten catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!("Cannot iterate over number (1)"))]
    );
}

#[test]
fn type_error_preview_truncation_matches_upstream_jq_cases() {
    let input = ZqValue::from_json(json!("very-long-long-long-long-string"));

    let q = compile("try -. catch .").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "string (\"very-long-long-long-long...\") cannot be negated"
        ))]
    );

    let q = compile("try (.-.) catch .").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
            out,
            vec![ZqValue::from_json(json!(
                "string (\"very-long-long-long-long...\") and string (\"very-long-long-long-long...\") cannot be subtracted"
            ))]
        );
}

#[test]
fn flatten_internal_helper_matches_builtin_jq_definition() {
    let q = compile("_flatten(-1)").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, [2]], [1, [[3], 2]]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2, 1, 3, 2]));

    let q = compile("_flatten(1)").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, [1, [2]], [1, [[3], 2]]]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, [2], 1, [[3], 2]]));
}

#[test]
fn first_last_nth_match_jq_index_aliases() {
    let q = compile("[first,last,nth(5)]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 9, 5]));

    let q = compile("nth(-1)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(3));

    let q = compile("nth(\"a\")").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 42}))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!(42));
}

#[test]
fn first_last_nth_type_errors_match_dynamic_indexing() {
    let q = compile("first").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "Cannot index number with number (0)");

    let q = compile("last").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "Cannot index number with number (-1)");

    let q = compile("nth(true)").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect_err("must fail");
    assert_eq!(err, "Cannot index array with boolean (true)");
}

#[test]
fn array_literal_collects_stream_values_like_jq() {
    let q = compile("[.[], .[]]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 1, 2]));

    let q = compile("[empty, 1, empty, 2]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2]));
}

#[test]
fn first_last_nth_generator_forms_match_upstream() {
    let q = compile("[first(.[]), last(.[]), nth(5; .[])]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 9, 5]));

    let q = compile("[first(empty), last(empty), nth(5; empty)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn first_last_nth_generator_short_circuit_and_errors() {
    let q = compile("first(1,error(\"foo\"))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile("nth(1; 0,1,error(\"foo\"))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);

    let q = compile("try nth(-1; .[]) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "nth doesn't support negative indices"
        ))]
    );

    let q = compile("try last(1,error(\"foo\")) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("foo"))]);
}

#[test]
fn limit_and_skip_match_upstream_cases() {
    let q = compile("[limit(3; .[])]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2]));

    let q = compile("[skip(3; .[])]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([3, 4, 5, 6, 7, 8, 9]));

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn limit_and_skip_support_comma_counts_like_jq() {
    let q = compile("[skip(0,2,3,4; .[])]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 3, 3]));

    let q = compile("[limit(5,7; .[])]").expect("compile");
    let out =
        execute(&q, &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8]))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])
    );
}

#[test]
fn limit_and_skip_negative_count_errors_match_jq() {
    let q = compile("try skip(-1; error) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "skip doesn't support negative count"
        ))]
    );

    let q = compile("try limit(-1; error) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "limit doesn't support negative count"
        ))]
    );
}

#[test]
fn range_matches_upstream_core_cases() {
    let q = compile("range(2; 4)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(3)]);

    let q = compile("[range(2; 4)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([2, 3]));

    let q = compile("[range(4)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2, 3]));

    let q = compile("[range(0; 10; 3)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 3, 6, 9]));

    let q = compile("[range(0; 10; -1)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));

    let q = compile("[range(0; -5; -1)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, -1, -2, -3, -4]));
}

#[test]
fn range_supports_stream_arguments_cartesian_products() {
    let q = compile("[range(0,1;3,4)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([0, 1, 2, 0, 1, 2, 3, 1, 2, 1, 2, 3])
    );

    let q = compile("[range(0,1,2;4,3,2;2,3)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([0, 2, 0, 3, 0, 2, 0, 0, 0, 1, 3, 1, 1, 1, 1, 1, 2, 2, 2, 2])
    );

    let q = compile("[range(3,5)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([0, 1, 2, 0, 1, 2, 3, 4]));
}

#[test]
fn range_zero_step_and_type_errors_match_jq_definition() {
    let q = compile("[range(0; 10; 0)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));

    let q = compile("try [range(0; 3; \"x\")] catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "number (0) and string (\"x\") cannot be added"
        ))]
    );
}

#[test]
fn while_matches_upstream_generator_case() {
    let q = compile("[while(.<100; .*2)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 4, 8, 16, 32, 64]));

    let q = compile("[while(.<0; .+1)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn until_matches_upstream_factorial_case() {
    let q = compile(".[]|[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, 5]))).expect("execute");
    assert_eq!(
        out,
        vec![1, 2, 6, 24, 120]
            .into_iter()
            .map(ZqValue::from)
            .collect::<Vec<_>>()
    );

    let q = compile("[until(. > 5; . + 2)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(1))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([7]));
}

#[test]
fn while_until_require_semicolon_forms() {
    let err = compile("while(. < 10)").expect_err("must fail");
    assert_eq!(err, "parse error: expected `;` in while/2 call");

    let err = compile("until(. < 10)").expect_err("must fail");
    assert_eq!(err, "parse error: expected `;` in until/2 call");
}

#[test]
fn any_all_generator_forms_match_upstream_cases() {
    let q = compile("any(.[]; not)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([1, 2, 3, 4, true, false, 1, 2, 3, 4, 5])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, true]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile("all(.[]; .)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([1, 2, 3, 4, true, false, 1, 2, 3, 4, 5])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, true]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn any_all_short_circuit_like_jq() {
    let q = compile("any(true, error; .)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("badness"))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("all(false, error; .)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("badness"))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
}

#[test]
fn any_all_alias_forms_match_upstream() {
    let q = compile("any(not)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);
    let out = execute(&q, &ZqValue::from_json(json!([false]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("all(not)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
    let out = execute(&q, &ZqValue::from_json(json!([false]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("[any,all]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, true]));

    let out = execute(&q, &ZqValue::from_json(json!([true]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, true]));

    let out = execute(&q, &ZqValue::from_json(json!([false]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([false, false]));

    let out = execute(&q, &ZqValue::from_json(json!([true, false]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, false]));

    let out = execute(&q, &ZqValue::from_json(json!([null, null, true]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([true, false]));
}

#[test]
fn parser_rejects_non_jq_not_dot_forms() {
    let err = compile("any(not .)").expect_err("must fail");
    assert!(
        err.contains("parse error"),
        "expected parse error for non-jq syntax, got: {err}"
    );
}

#[test]
fn type_selector_builtins_match_upstream() {
    let input = ZqValue::from_json(json!([1, 2, "foo", [], [3, []], {}, true, false, null]));

    let q = compile(".[]|arrays").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!([])),
            ZqValue::from_json(json!([3, []]))
        ]
    );

    let q = compile(".[]|objects").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!({}))]);

    let q = compile(".[]|iterables").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!([])),
            ZqValue::from_json(json!([3, []])),
            ZqValue::from_json(json!({}))
        ]
    );

    let q = compile(".[]|scalars").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(1),
            ZqValue::from(2),
            ZqValue::from_json(json!("foo")),
            ZqValue::Bool(true),
            ZqValue::Bool(false),
            ZqValue::Null
        ]
    );

    let q = compile(".[]|values").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(1),
            ZqValue::from(2),
            ZqValue::from_json(json!("foo")),
            ZqValue::from_json(json!([])),
            ZqValue::from_json(json!([3, []])),
            ZqValue::from_json(json!({})),
            ZqValue::Bool(true),
            ZqValue::Bool(false),
        ]
    );

    let q = compile(".[]|booleans").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true), ZqValue::Bool(false)]);

    let q = compile(".[]|nulls").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::Null]);

    let q = compile(".[]|numbers").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from(1), ZqValue::from(2)]);

    let q = compile(".[]|strings").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("foo"))]);
}

#[test]
fn isempty_matches_upstream_cases() {
    let q = compile("isempty(empty)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(0))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);

    let q = compile("isempty(.[])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(true)]);
}

#[test]
fn isempty_short_circuits_like_jq() {
    let q = compile("isempty(1,error(\"foo\"))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::Bool(false)]);

    let q = compile("try isempty(error(\"foo\")) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(null))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!("foo"))]);
}

#[test]
fn sort_matches_upstream_mixed_value_order() {
    let q = compile("sort").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            42,
            [2, 5, 3, 11],
            10,
            {"a":42,"b":2},
            {"a":42},
            true,
            2,
            [2, 6],
            "hello",
            null,
            [2, 5, 6],
            {"a":[],"b":1},
            "abc",
            "ab",
            [3, 10],
            {},
            false,
            "abcd",
            null
        ])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            null,
            null,
            false,
            true,
            2,
            10,
            42,
            "ab",
            "abc",
            "abcd",
            "hello",
            [2, 5, 3, 11],
            [2, 5, 6],
            [2, 6],
            [3, 10],
            {},
            {"a":42},
            {"a":42,"b":2},
            {"a":[],"b":1}
        ])
    );
}

#[test]
fn sort_by_and_group_by_match_upstream_cases() {
    // jq/tests/jq.test
    let input = ZqValue::from_json(json!([
        {"a": 1, "b": 4, "c": 14},
        {"a": 4, "b": 1, "c": 3},
        {"a": 1, "b": 4, "c": 3},
        {"a": 0, "b": 2, "c": 43}
    ]));

    let q = compile("sort_by(.a, .b)").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            {"a":0,"b":2,"c":43},
            {"a":1,"b":4,"c":14},
            {"a":1,"b":4,"c":3},
            {"a":4,"b":1,"c":3}
        ])
    );

    let q = compile("sort_by(.b, .c)").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            {"a":4,"b":1,"c":3},
            {"a":0,"b":2,"c":43},
            {"a":1,"b":4,"c":3},
            {"a":1,"b":4,"c":14}
        ])
    );

    let q = compile("group_by(.b)").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            [{"a":4,"b":1,"c":3}],
            [{"a":0,"b":2,"c":43}],
            [{"a":1,"b":4,"c":14},{"a":1,"b":4,"c":3}]
        ])
    );

    let q = compile("group_by(.a + .b - .c == 2)").expect("compile");
    let out = execute(&q, &input).expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            [{"a":1,"b":4,"c":14},{"a":0,"b":2,"c":43}],
            [{"a":4,"b":1,"c":3},{"a":1,"b":4,"c":3}]
        ])
    );
}

#[test]
fn unique_matches_upstream() {
    let q = compile("unique").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 5, 3, 5, 3, 1, 3]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([1, 2, 3, 5]));

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([]));
}

#[test]
fn unique_by_matches_upstream_cases() {
    // jq/tests/man.test
    let q = compile("unique_by(.foo)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"foo": 1, "bar": 10},
            {"foo": 3, "bar": 100},
            {"foo": 1, "bar": 1}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([{"foo":1,"bar":10},{"foo":3,"bar":100}])
    );

    let q = compile("unique_by(length)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            [1, 2],
            ["a", "b", "c"],
            [5, 6],
            ["foo", "bar"],
            [7]
        ])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([[7], [1, 2], ["a", "b", "c"]])
    );
}

#[test]
fn sort_unique_type_errors_match_jq() {
    let q = compile("sort").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "number (1) cannot be sorted, as it is not an array");

    let q = compile("unique").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(true))).expect_err("must fail");
    assert_eq!(
        err,
        "boolean (true) cannot be sorted, as it is not an array"
    );
}

#[test]
fn sort_by_impl_type_errors_match_jq() {
    let q = compile("try _sort_by_impl(0) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "array ([]) and number (0) cannot be sorted, as they are not both arrays"
        ))]
    );

    let q = compile("try _sort_by_impl([0]) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 1}))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "object ({\"a\":1}) and array ([0]) cannot be sorted, as they are not both arrays"
        ))]
    );
}

#[test]
fn min_max_match_upstream_cases() {
    let q = compile("[min,max]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[4, 2, "a"], [3, 1, "a"], [2, 4, "a"], [1, 3, "a"]])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([[1, 3, "a"], [4, 2, "a"]])
    );

    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([null, null]));
}

#[test]
fn min_by_max_by_match_upstream_cases() {
    // jq/tests/jq.test
    let q = compile("[min, max, min_by(.[1]), max_by(.[1]), min_by(.[2]), max_by(.[2])]")
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[4, 2, "a"], [3, 1, "a"], [2, 4, "a"], [1, 3, "a"]])),
    )
    .expect("execute");
    assert_eq!(
        out[0].clone().into_json(),
        json!([
            [1, 3, "a"],
            [4, 2, "a"],
            [3, 1, "a"],
            [2, 4, "a"],
            [4, 2, "a"],
            [1, 3, "a"]
        ])
    );

    let q = compile("[min,max,min_by(.),max_by(.)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([]))).expect("execute");
    assert_eq!(out[0].clone().into_json(), json!([null, null, null, null]));
}

#[test]
fn min_max_type_error_matches_jq() {
    let q = compile("min").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!(1))).expect_err("must fail");
    assert_eq!(err, "number (1) and number (1) cannot be iterated over");
}

#[test]
fn min_max_by_impl_errors_match_jq() {
    let q = compile("try _min_by_impl([0]) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "array ([1,2]) and array ([0]) have wrong length"
        ))]
    );

    let q = compile("try _max_by_impl(1) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!(
            "array ([1,2]) and number (1) cannot be iterated over"
        ))]
    );
}

#[test]
fn bsearch_matches_upstream_scalar_and_multivalue_cases() {
    let q = compile("bsearch(0,1,2,3,4)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(-1),
            ZqValue::from(0),
            ZqValue::from(1),
            ZqValue::from(2),
            ZqValue::from(-4)
        ]
    );

    let q = compile("bsearch(0)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(0)]);

    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(-1)]);
}

#[test]
fn bsearch_matches_upstream_object_case() {
    let q = compile("bsearch({x:1})").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([{ "x": 0 }, { "x": 1 }, { "x": 2 }])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn bsearch_type_error_matches_upstream() {
    let q = compile("try [\"OK\", bsearch(0)] catch [\"KO\",.]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("aa"))).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([
            "KO",
            "string (\"aa\") cannot be searched from"
        ]))]
    );
}

#[test]
fn as_binding_shadowing_matches_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile(".bar as $x | .foo | . + $x").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo":10, "bar":200}))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(210)]);

    let q = compile(". as $i|[(.*2|. as $i| $i), $i]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!(5))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([10, 5]))]);
}

#[test]
fn as_destructure_array_patterns_match_upstream_cases() {
    // jq/tests/man.test
    let q = compile(". as [$a, $b, {c: $c}] | $a + $b + $c").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([2, 3, {"c": 4, "d": 5}]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(9)]);

    let q = compile(".[] as [$a, $b] | {a: $a, b: $b}").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[0], [0, 1], [2, 1, 0]]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!({"a":0,"b":null})),
            ZqValue::from_json(json!({"a":0,"b":1})),
            ZqValue::from_json(json!({"a":2,"b":1}))
        ]
    );
}

#[test]
fn as_destructure_alternatives_fill_missing_vars_with_null() {
    // jq/tests/man.test
    let q = compile(".[] as {$a, $b, c: {$d}} ?// {$a, $b, c: [{$e}]} | {$a, $b, $d, $e}")
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"a": 1, "b": 2, "c": {"d": 3, "e": 4}},
            {"a": 1, "b": 2, "c": [{"d": 3, "e": 4}]}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!({"a":1,"b":2,"d":3,"e":null})),
            ZqValue::from_json(json!({"a":1,"b":2,"d":null,"e":4}))
        ]
    );

    // jq/tests/jq.test
    let q = compile(
        ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
    )
    .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"a":1, "b":[2,{"d":3}]},
            [4, {"b":5, "c":6}, 7, 8, 9],
            "foo"
        ])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!([1, null, 2, 3, null, null]),
            json!([4, 5, null, null, 7, null]),
            json!([null, null, null, null, null, "foo"]),
        ]
    );

    // jq/tests/jq.test
    let q = compile(".[] as {a:$a} ?// {a:$a} ?// $a | $a").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[3], [4], [5], 6]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([3]), json!([4]), json!([5]), json!(6)]
    );

    // jq/tests/jq.test
    let q = compile(".[] as $a ?// {a:$a} ?// {a:$a} | $a").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[3], [4], [5], 6]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([3]), json!([4]), json!([5]), json!(6)]
    );

    // jq/tests/jq.test (runtime error shape)
    let q = compile(".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a").expect("compile");
    let err = execute(&q, &ZqValue::from_json(json!([[3], [4], [5], 6]))).expect_err("error");
    assert!(err.contains("Cannot index array with string"), "err={err}");
}

#[test]
fn object_literal_binding_keys_and_shorthand_match_jq_shape() {
    let q = compile(". as $k | {$k: 1, $k}").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!("name"))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!({"name":1,"k":"name"}))]);
}

#[test]
fn as_pattern_key_constant_validation_matches_upstream() {
    // jq/tests/jq.test %%FAIL case
    let err = compile(". as {(true):$foo} | $foo").expect_err("must fail");
    assert_eq!(err, "Cannot use boolean (true) as object key");
}

#[test]
fn as_pattern_rejects_empty_array_and_object_patterns() {
    // jq/tests/jq.test %%FAIL cases
    let err = compile(". as [] | null").expect_err("must fail");
    assert!(err.contains("unexpected ']'"), "err={err}");

    let err = compile(". as {} | null").expect_err("must fail");
    assert!(err.contains("unexpected '}'"), "err={err}");
}

#[test]
fn reduce_matches_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("reduce .[] as $item (0; . + $item)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, 5]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(15)]);

    let q = compile("reduce .[] as [$i,$j] (0; . + $i * $j)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[1, 2], [3, 4], [5, 6]]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from(44)]);
}

#[test]
fn foreach_matches_upstream_man_cases() {
    // jq/tests/man.test
    let q = compile("foreach .[] as $item (0; . + $item)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, 5]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(1),
            ZqValue::from(3),
            ZqValue::from(6),
            ZqValue::from(10),
            ZqValue::from(15)
        ]
    );

    let q = compile("foreach .[] as $item (0; . + $item; [$item, . * 2])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3, 4, 5]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from_json(json!([1, 2])),
            ZqValue::from_json(json!([2, 6])),
            ZqValue::from_json(json!([3, 12])),
            ZqValue::from_json(json!([4, 20])),
            ZqValue::from_json(json!([5, 30]))
        ]
    );
}

#[test]
fn unary_and_binary_stream_semantics_match_upstream_jq_cases() {
    // jq/tests/jq.test:
    // [-reduce -.[] as $x (0; . + $x)]
    let q = compile("[-reduce -.[] as $x (0; . + $x)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([6]))]);

    // jq/tests/jq.test:
    // [-foreach -.[] as $x (0; . + $x)]
    let q = compile("[-foreach -.[] as $x (0; . + $x)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([1, 3, 6]))]);

    // jq/tests/jq.test:
    // [reduce .[] / .[] as $i (0; . + $i)]
    let q = compile("[reduce .[] / .[] as $i (0; . + $i)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([4.5]))]);

    // jq/tests/jq.test:
    // [foreach .[] / .[] as $i (0; . + $i)]
    let q = compile("[foreach .[] / .[] as $i (0; . + $i)]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([1, 3, 3.5, 4.5]))]);

    // jq/tests/jq.test:
    // [.[] % 7]
    let q = compile("[.[] % 7]").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([-7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7])),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!([
            0, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 0
        ]))]
    );

    // jq runtime ordering for stream binary ops:
    // .[] / .[] on [1,2] => 1,2,0.5,1
    let q = compile(".[] / .[]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(1),
            ZqValue::from(2),
            ZqValue::from_json(json!(0.5)),
            ZqValue::from(1)
        ]
    );
}

#[test]
fn getpath_setpath_delpaths_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("[\"foo\",1] as $p | getpath($p), setpath($p; 20), delpaths([$p])")
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"bar": 42, "foo": ["a", "b", "c", "d"]})),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!("b"),
            json!({"bar": 42, "foo": ["a", 20, "c", "d"]}),
            json!({"bar": 42, "foo": ["a", "c", "d"]}),
        ]
    );

    let out = execute(&q, &ZqValue::from_json(json!({"bar": false}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!(null),
            json!({"bar":false, "foo":[null, 20]}),
            json!({"bar":false})
        ]
    );

    // jq/tests/jq.test
    let q =
        compile("map(getpath([2])), map(setpath([2]; 42)), map(delpaths([[2]]))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[0], [0, 1], [0, 1, 2]]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!([null, null, 2]),
            json!([[0, null, 42], [0, 1, 42], [0, 1, 42]]),
            json!([[0], [0, 1], [0, 1]])
        ]
    );

    // jq/tests/jq.test
    let q = compile("map(delpaths([[0,\"foo\"]]))").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([[{"foo":2, "x":1}], [{"bar":2}]])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([[{"x":1}], [{"bar":2}]])]
    );

    // jq/tests/jq.test
    let q = compile("delpaths([[-200]])").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2, 3]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1, 2, 3])]
    );

    // jq/tests/jq.test
    let q = compile("try delpaths(0) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("Paths must be specified as an array")]
    );

    let q = compile("try delpaths([0]) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("Path must be specified as array, not number")]
    );

    // jq/tests/jq.test
    let q = compile("setpath([-1]; 1)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1])]
    );
}

#[test]
fn del_and_pick_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile(
            "del(.), del(empty), del((.foo,.bar,.baz) | .[2,3,0]), del(.foo[0], .bar[0], .foo, .baz.bar[0].x)",
        )
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"foo": [0,1,2,3,4], "bar": [0,1]})),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!(null),
            json!({"foo": [0,1,2,3,4], "bar": [0,1]}),
            json!({"foo": [1,4], "bar": [1]}),
            json!({"bar": [1]}),
        ]
    );

    // jq/tests/jq.test
    let q = compile("del(.[1], .[-6], .[2], .[-3:9])").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([0, 3, 5, 6, 9])]
    );

    // jq/tests/man.test
    let q = compile("pick(.a, .b.c, .x)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4})),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a": 1, "b": {"c": 2}, "x": null})]
    );

    // jq/tests/jq.test
    let q = compile("pick(.a.b.c)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a": {"b": {"c": null}}})]
    );

    // jq/tests/jq.test
    let q = compile("pick(first)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1])]
    );

    // jq/tests/jq.test
    let q = compile("pick(first|first)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([[10, 20], 30]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([[10]])]
    );

    // jq/tests/jq.test
    let q = compile("try pick(last) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 2]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!("Out of bounds negative array index")]
    );
}

#[test]
fn assign_operator_matches_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile(".message = \"goodbye\"").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"message": "hello"}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"message": "goodbye"})]
    );

    // jq/tests/jq.test
    let q = compile(".foo = .bar").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"bar": 42}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo": 42, "bar": 42})]
    );

    // jq/tests/jq.test
    let q = compile(".[2][3] = 1").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([4]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([4, null, [null, null, null, 1]])]
    );

    // jq/tests/jq.test
    let q = compile(".foo[2].bar = 1").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo":[11], "bar":42}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo":[11, null, {"bar":1}], "bar":42})]
    );

    // jq manual v1.7: `=` uses all RHS values.
    let q = compile("(.a, .b) = range(3)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!({"a":0,"b":0}),
            json!({"a":1,"b":1}),
            json!({"a":2,"b":2}),
        ]
    );
}

#[test]
fn update_assignment_operators_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile(".foo |= .+1").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 42}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo": 43})]
    );

    // jq/manual: `|=` uses the current LHS value as input.
    let q = compile(".a |= .b").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": {"b": 10}, "b": 20}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a": 10, "b": 20})]
    );

    // jq/tests/jq.test
    let q = compile(".[] += 2, .[] *= 2, .[] -= 2, .[] /= 2, .[] %= 2").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 3, 5]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!([3, 5, 7]),
            json!([2, 6, 10]),
            json!([-1, 1, 3]),
            json!([0.5, 1.5, 2.5]),
            json!([1, 1, 1]),
        ]
    );

    // jq/tests/jq.test
    let q = compile(".foo += .foo").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 2}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo": 4})]
    );

    // jq/tests/jq.test
    let q = compile(".[0].a |= {\"old\":., \"new\":(.+1)}").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{"a":1,"b":2}]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([{"a":{"old":1, "new":2},"b":2}])]
    );

    // jq/tests/jq.test
    let q = compile("def inc(x): x |= .+1; inc(.[].a)").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            {"a":1,"b":2},
            {"a":2,"b":4},
            {"a":7,"b":8}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([
            {"a":2,"b":2},
            {"a":3,"b":4},
            {"a":8,"b":8}
        ])]
    );

    // jq/tests/jq.test
    let q = compile("(.[] | select(. >= 2)) |= empty").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 5, 3, 0, 7]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1, 0])]
    );

    // jq/tests/jq.test
    let q = compile(".[] |= select(. % 2 == 0)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, 2, 3, 4, 5]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([0, 2, 4])]
    );

    // jq/tests/jq.test
    let q = compile(".foo[1,4,2,3] |= empty").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo":[0,1,2,3,4,5]}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo":[0,5]})]
    );

    // jq manual v1.7: for `|=` only first RHS value is used.
    let q = compile("(.a, .b) |= range(3)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a":0,"b":0})]
    );

    let q = compile("(.a, .b) += range(3)").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a":0,"b":0})]
    );

    // jq manual v1.7: arithmetic `op=` includes `//=`.
    let q = compile(".foo //= 7").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"foo": null}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo": 7})]
    );
    let out = execute(&q, &ZqValue::from_json(json!({"foo": 2}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"foo": 2})]
    );

    // jq/tests/jq.test #1358: getpath/1 works in assignment path expressions.
    let q = compile(".[] | try (getpath([\"a\",0,\"b\"]) |= 5) catch .").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([
            null,
            {"b":0},
            {"a":0},
            {"a":null},
            {"a":[0,1]},
            {"a":{"b":1}},
            {"a":[{}]},
            {"a":[{"c":3}]}
        ])),
    )
    .expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![
            json!({"a":[{"b":5}]}),
            json!({"b":0,"a":[{"b":5}]}),
            json!("Cannot index number with number (0)"),
            json!({"a":[{"b":5}]}),
            json!("Cannot index number with string \"b\""),
            json!("Cannot index object with number (0)"),
            json!({"a":[{"b":5}]}),
            json!({"a":[{"c":3,"b":5}]})
        ]
    );

    // jq/tests/jq.test: invalid path expression diagnostics are preserved for assignment.
    let q = compile("try ((map(select(.a == 1))[].b) = 10) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{"a":0},{"a":1}]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!(
            "Invalid path expression near attempt to iterate through [{\"a\":1}]"
        )]
    );

    let q = compile("try ((map(select(.a == 1))[].a) |= .+1) catch .").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([{"a":0},{"a":1}]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!(
            "Invalid path expression near attempt to iterate through [{\"a\":1}]"
        )]
    );
}

#[test]
fn assign_and_modify_internal_helpers_follow_builtin_jq_defs() {
    // jq/src/builtin.jq
    let q = compile("_assign(.a; 7)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 1, "b": 2}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a": 7, "b": 2})]
    );

    let q = compile("_modify(.a; .+1)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 1, "b": 2}))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!({"a": 2, "b": 2})]
    );
}

#[test]
fn path_and_paths_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("path(.foo[0,1])").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!(["foo", 0]), json!(["foo", 1])]
    );

    let q = compile("path(.[] | select(.>3))").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, 5, 3]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([1])]
    );

    let q = compile("path(.)").expect("compile");
    let out = execute(&q, &ZqValue::from(42)).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([])]
    );

    let q = compile("[paths]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, [[], {"a": 2}]]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([[0], [1], [1, 0], [1, 1], [1, 1, "a"]])]
    );

    // jq/tests/man.test
    let q = compile("[paths(type == \"number\")]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([1, [[], {"a": 2}]]))).expect("execute");
    assert_eq!(
        out.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![json!([[0], [1, 1, "a"]])]
    );

    // jq/tests/jq.test via del(empty): path(empty) yields no paths.
    let q = compile("path(empty)").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"x": 1}))).expect("execute");
    assert!(out.is_empty());
}

#[test]
fn label_break_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("[(label $here | .[] | if .>1 then break $here else . end), \"hi!\"]")
        .expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([0, 1, "hi!"]))]);

    let out = execute(&q, &ZqValue::from_json(json!([0, 2, 1]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([0, "hi!"]))]);

    let q =
        compile("[ label $if | range(10) | ., (select(. == 5) | break $if) ]").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([0, 1, 2, 3, 4, 5]))]);
}

#[test]
fn label_break_with_foreach_matches_upstream_jq_case() {
    // jq/tests/jq.test
    let q = compile(
            "[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]",
        )
        .expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!([11, 22, 33, 44, 55, 66, 77, 88, 99])),
    )
    .expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([11, 22, 33]))]);
}

#[test]
fn break_without_label_binding_errors() {
    // jq/tests/jq.test %%FAIL
    let err = compile(". as $foo | break $foo").expect_err("must fail");
    assert_eq!(err, "$*label-foo is not defined");
}

#[test]
fn break_requires_label_parse_error() {
    let err = compile("break").expect_err("must fail");
    assert_eq!(err, "parse error: break requires a label to break to");
}

#[test]
fn loc_token_matches_upstream_jq_shape() {
    // jq/tests/jq.test:
    // { a, $__loc__, c }
    let q = compile("{ a, $__loc__, c }").expect("compile");
    let out = execute(
        &q,
        &ZqValue::from_json(json!({"a":[1,2,3], "c":{"hi":"hey"}})),
    )
    .expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!({
            "a":[1,2,3],
            "__loc__":{"file":"<top-level>","line":1},
            "c":{"hi":"hey"}
        }))]
    );

    let q = compile("$__loc__").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::from_json(json!({"file":"<top-level>","line":1}))]
    );
}

#[test]
fn decimal_and_exponent_literals_follow_jq_lexer_forms() {
    // jq lexer.l:
    // ([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?
    let q = compile("1e+0 + 0.001e3").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out.len(), 1);
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 2.0).abs() < f64::EPSILON, "value={value}");

    let q = compile("1 / 1e-17").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out.len(), 1);
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 1e17).abs() < 0.5, "value={value}");

    let q = compile("[.[] | . + 0.5]").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!([0, 1, 2]))).expect("execute");
    assert_eq!(out, vec![ZqValue::from_json(json!([0.5, 1.5, 2.5]))]);

    let q = compile(".5 + .5").expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out.len(), 1);
    let value = out[0].as_f64().expect("numeric output");
    assert!((value - 1.0).abs() < f64::EPSILON, "value={value}");
}

#[test]
fn qqstring_interpolation_matches_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile(r#""inter\("pol" + "ation")""#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(out, vec![ZqValue::String("interpolation".to_string())]);

    // jq/tests/man.test
    let q = compile(r#""The input was \(.), which is one less than \(.+1)""#).expect("compile");
    let out = execute(&q, &ZqValue::from(42)).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::String(
            "The input was 42, which is one less than 43".to_string()
        )]
    );
}

#[test]
fn format_filters_and_stringstart_format_follow_jq_shapes() {
    // jq/tests/jq.test format filters include @json/@uri/@html etc.
    let q = compile("@json").expect("compile");
    let out = execute(&q, &ZqValue::from_json(json!({"a": 1}))).expect("execute");
    assert_eq!(out, vec![ZqValue::String("{\"a\":1}".to_string())]);

    let q = compile(r#"@json "x\(.)""#).expect("compile");
    let out = execute(&q, &ZqValue::String("a".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("x\"a\"".to_string())]);

    let q = compile(r#"@html "<b>\(.)</b>""#).expect("compile");
    let out = execute(&q, &ZqValue::String("<x>".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("<b>&lt;x&gt;</b>".to_string())]);

    let q = compile("@uri | @urid").expect("compile");
    let out = execute(&q, &ZqValue::String("a b".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("a b".to_string())]);
}

#[test]
fn loc_inside_qqstring_interpolation_matches_upstream_shape() {
    // jq/tests/jq.test and jq/tests/man.test:
    // try error("\($__loc__)") catch .
    let q = compile(r#"try error("\($__loc__)") catch ."#).expect("compile");
    let out = execute(&q, &ZqValue::Null).expect("execute");
    assert_eq!(
        out,
        vec![ZqValue::String(
            "{\"file\":\"<top-level>\",\"line\":1}".to_string()
        )]
    );
}

#[test]
fn format_pipeline_combo_matches_upstream_jq_block() {
    // jq/tests/jq.test
    let q = compile("@text,@json,([1,.]|@csv,@tsv),@html,(@uri|.,@urid),@sh,(@base64|.,@base64d)")
        .expect("compile");
    let out = execute(&q, &ZqValue::String("!()<>&'\"\t".to_string())).expect("execute");
    assert_eq!(
        out,
        vec![
            ZqValue::String("!()<>&'\"\t".to_string()),
            ZqValue::String("\"!()<>&'\\\"\\t\"".to_string()),
            ZqValue::String("1,\"!()<>&'\"\"\t\"".to_string()),
            ZqValue::String("1\t!()<>&'\"\\t".to_string()),
            ZqValue::String("!()&lt;&gt;&amp;&apos;&quot;\t".to_string()),
            ZqValue::String("%21%28%29%3C%3E%26%27%22%09".to_string()),
            ZqValue::String("!()<>&'\"\t".to_string()),
            ZqValue::String("'!()<>&'\\''\"\t'".to_string()),
            ZqValue::String("ISgpPD4mJyIJ".to_string()),
            ZqValue::String("!()<>&'\"\t".to_string()),
        ]
    );
}

#[test]
fn base64_and_uri_filters_match_upstream_jq_cases() {
    // jq/tests/jq.test
    let q = compile("@base64").expect("compile");
    let out = execute(&q, &ZqValue::String("foóbar\n".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("Zm/Ds2Jhcgo=".to_string())]);

    let q = compile("@base64d").expect("compile");
    let out = execute(&q, &ZqValue::String("Zm/Ds2Jhcgo=".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("foóbar\n".to_string())]);

    let q = compile("@uri").expect("compile");
    let out = execute(&q, &ZqValue::String("μ".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("%CE%BC".to_string())]);

    let q = compile("@urid").expect("compile");
    let out = execute(&q, &ZqValue::String("%CE%BC".to_string())).expect("execute");
    assert_eq!(out, vec![ZqValue::String("μ".to_string())]);
}

#[test]
fn invalid_escape_in_string_reports_lex_error() {
    // jq/tests/jq.test %%FAIL:
    // "u\vw"
    let err = compile(r#""u\vw""#).expect_err("must fail");
    assert!(err.contains("invalid string literal"), "err={err}");
}
