use super::*;

#[test]
fn parse_doc_mode_contract() {
    assert_eq!(
        parse_doc_mode("first", None).expect("first"),
        DocMode::First
    );
    assert_eq!(parse_doc_mode("all", None).expect("all"), DocMode::All);
    assert_eq!(
        parse_doc_mode("index", Some(3)).expect("index"),
        DocMode::Index(3)
    );
    assert!(matches!(
        parse_doc_mode("index", None),
        Err(Error::MissingDocIndex)
    ));
    assert!(matches!(
        parse_doc_mode("x", None),
        Err(Error::InvalidDocMode(_))
    ));
}

#[test]
fn run_jq_api_works_on_yaml_input() {
    let input = "a: 1\n";
    let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!(1)]);
    let native = run_jq_native(".a", input, QueryOptions::default()).expect("run jq native");
    assert_eq!(
        native
            .into_iter()
            .map(ZqValue::into_json)
            .collect::<Vec<_>>(),
        vec![serde_json::json!(1)]
    );
}

#[test]
fn run_jq_api_reads_json_stream_even_with_default_doc_mode() {
    let input = "{\"a\":1}\n{\"a\":2}\n";
    let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!(1), serde_json::json!(2)]);
    let native = run_jq_native(".a", input, QueryOptions::default()).expect("run jq native stream");
    assert_eq!(
        native
            .into_iter()
            .map(ZqValue::into_json)
            .collect::<Vec<_>>(),
        vec![serde_json::json!(1), serde_json::json!(2)]
    );
}

#[test]
fn yaml_output_for_multiple_values_is_multidoc() {
    let out =
        format_output_yaml_documents(&[serde_json::json!({"a":1}), serde_json::json!({"b":2})])
            .expect("yaml output");
    assert!(out.contains("a: 1"));
    assert!(out.contains("---"));
    assert!(out.contains("b: 2"));
}

#[test]
fn format_query_error_control_character_matches_jq_text() {
    let err = serde_json::from_str::<serde_json::Value>("\"\u{1}\"")
        .expect_err("must fail on unescaped control char");
    let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
    assert_eq!(
            msg,
            "jq: parse error: Invalid string: control characters from U+0000 through U+001F must be escaped at line 1, column 3"
        );
}

#[test]
fn format_query_error_colon_in_object_matches_jq_text() {
    let err = serde_json::from_str::<serde_json::Value>("{\"a\":1,\"b\",")
        .expect_err("must fail on malformed object");
    let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Objects must consist of key:value pairs at line 1, column 11"
    );
}

#[test]
fn format_query_error_string_key_after_object_start_matches_jq_text() {
    let input = "{{\"a\":\"b\"}}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after '{', not '{' at line 1, column 2"
    );
}

#[test]
fn format_query_error_string_key_after_comma_matches_jq_text() {
    let input = "{\"x\":\"y\",{\"a\":\"b\"}}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after ',' in object, not '{' at line 1, column 10"
    );
}

#[test]
fn format_query_error_string_key_array_after_object_start_matches_jq_text() {
    let input = "{[\"a\",\"b\"]}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after '{', not '[' at line 1, column 2"
    );
}

#[test]
fn format_query_error_string_key_array_after_comma_matches_jq_text() {
    let input = "{\"x\":\"y\",[\"a\",\"b\"]}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after ',' in object, not '[' at line 1, column 10"
    );
}

#[test]
fn strip_serde_line_col_suffix_only_removes_valid_suffix() {
    assert_eq!(
        strip_serde_line_col_suffix("expected value at line 1 column 2"),
        "expected value"
    );
    assert_eq!(
        strip_serde_line_col_suffix("hello at line nope column 2"),
        "hello at line nope column 2"
    );
}

#[test]
fn json_output_escapes_del_like_jq() {
    let out = format_output_json_lines(&[serde_json::json!(" ~\u{007f}")], true, false)
        .expect("json output");
    assert_eq!(out, "\" ~\\u007f\"");
}

#[test]
fn json_output_pretty_mode_matches_contract() {
    let out = format_output_json_lines(&[serde_json::json!({"a": 1})], false, false)
        .expect("pretty json output");
    assert_eq!(out, "{\n  \"a\": 1\n}");

    let out = format_output_json_lines(&[serde_json::json!(" ~\u{007f}")], false, false)
        .expect("pretty json string output");
    assert_eq!(out, "\" ~\\u007f\"");
}

#[test]
fn format_runtime_error_matches_jq_prefix() {
    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Runtime("Cannot index object with number".to_string()),
    );
    assert_eq!(
        msg,
        "jq: error (at <stdin>:1): Cannot index object with number"
    );
}

#[test]
fn engine_wrapper_helpers_cover_contract() {
    assert!(validate_jq_query(".").is_ok());
    assert!(validate_jq_query_with_paths(".", &[]).is_ok());
    assert!(validate_jq_query("if").is_err());

    let prepared = prepare_jq_query_with_paths(".", &[]).expect("prepare query");
    assert_eq!(
        prepared.run_jsonish_lines("1").expect("prepared run"),
        vec!["1".to_string()]
    );
    assert_eq!(
        prepared
            .run_jsonish_lines_lenient("1")
            .expect("prepared run lenient"),
        vec!["1".to_string()]
    );
    assert_eq!(
        run_jq_jsonish_lines(".", "1", &[]).expect("run jsonish lines"),
        vec!["1".to_string()]
    );
    assert_eq!(
        normalize_jsonish_line("{\"a\":1}").expect("normalize jsonish"),
        "{\"a\":1}".to_string()
    );
    assert!(jsonish_equal("1", "1").expect("jsonish compare"));
}

#[test]
fn parse_json_only_and_doc_selection_contract() {
    let values = parse_jq_json_values_only("1\n2\n").expect("parse json stream");
    assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)]);
    let values_native =
        parse_jq_json_values_only_native("1\n2\n").expect("parse json stream native");
    assert_eq!(
        values_native
            .into_iter()
            .map(ZqValue::into_json)
            .collect::<Vec<_>>(),
        vec![serde_json::json!(1), serde_json::json!(2)]
    );
    assert!(parse_jq_json_values_only("a: 1\n").is_err());

    let yaml_docs = "a: 1\n---\na: 2\n";
    let selected = parse_jq_input_values(yaml_docs, DocMode::Index(1), "jq")
        .expect("select yaml doc by index");
    assert_eq!(selected, vec![serde_json::json!({"a": 2})]);
    let selected_native = parse_jq_input_values_native(yaml_docs, DocMode::Index(1), "jq")
        .expect("select yaml doc by index native");
    assert_eq!(
        selected_native
            .into_iter()
            .map(ZqValue::into_json)
            .collect::<Vec<_>>(),
        vec![serde_json::json!({"a": 2})]
    );

    let out_of_range = parse_jq_input_values(yaml_docs, DocMode::Index(3), "jq")
        .expect_err("doc index out of range");
    assert!(matches!(
        out_of_range,
        Error::DocIndexOutOfRange {
            tool: "jq",
            index: 3,
            total: 2
        }
    ));
}

#[test]
fn stream_wrappers_and_output_formatters_contract() {
    let out = run_jq_stream_with_paths_options(
        ".",
        vec![serde_json::json!(1)],
        &[],
        RunOptions { null_input: true },
    )
    .expect("run jq stream with null-input");
    assert_eq!(out, vec![JsonValue::Null]);
    let out_native = run_jq_stream_with_paths_options_native(
        ".",
        vec![ZqValue::from_json(serde_json::json!(1))],
        &[],
        RunOptions { null_input: true },
    )
    .expect("run jq stream native with null-input");
    assert_eq!(
        out_native
            .into_iter()
            .map(ZqValue::into_json)
            .collect::<Vec<_>>(),
        vec![JsonValue::Null]
    );

    let mut emitted = Vec::new();
    let status = try_run_jq_native_stream_with_paths_options(
        ".a",
        &[serde_json::json!({"a": 1})],
        RunOptions::default(),
        |v| {
            emitted.push(v);
            Ok(())
        },
    )
    .expect("native stream executes");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(emitted, vec![serde_json::json!(1)]);

    let unsupported = try_run_jq_native_stream_with_paths_options(
        "label $out | .",
        &[serde_json::json!(1)],
        RunOptions::default(),
        |_| Ok(()),
    )
    .expect("unsupported native stream status");
    assert_eq!(unsupported, NativeStreamStatus::Executed);

    let sink_error = try_run_jq_native_stream_with_paths_options(
        ".",
        &[serde_json::json!(1)],
        RunOptions::default(),
        |_| Err("sink failed".to_string()),
    )
    .expect_err("sink error must surface");
    assert_eq!(
        format!("{sink_error}"),
        "sink failed",
        "native sink error must map to runtime error"
    );

    let json_out = format_output_json_lines(
        &[
            serde_json::json!("x"),
            serde_json::json!(1),
            serde_json::json!(" ~\u{007f}"),
        ],
        true,
        true,
    )
    .expect("format json");
    assert_eq!(json_out, "x\n1\n ~\u{7f}");

    assert_eq!(
        format_output_yaml_documents(&[]).expect("yaml empty"),
        String::new()
    );
}

#[test]
fn format_query_error_adds_context_and_compile_forms() {
    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Unsupported("Top-level program not given (try \".\")".to_string()),
    );
    assert!(msg.contains("jq: 1 compile error"));

    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Unsupported(
            "too many function parameters or local function definitions (max 4095)".to_string(),
        ),
    );
    assert!(msg.contains("jq: 1 compile error"));

    let input = "line1\nline2\nline3\n";
    let msg = format_query_error(
        "jq",
        input,
        &crate::QueryError::Unsupported("boom at line 2, column 3".to_string()),
    );
    assert!(msg.contains("--> <stdin>:2:3"));
    assert!(msg.contains("2 | line2"));
    assert!(msg.contains("^"));

    let query = "lineA\nlineB\nlineC\n";
    let msg = format_query_error_with_sources(
        "jq",
        query,
        input,
        &crate::QueryError::Unsupported("boom at line 2, column 3".to_string()),
    );
    assert!(msg.contains("--> <query>:2:3"));
    assert!(msg.contains("2 | lineB"));
    assert!(msg.contains("^"));

    let msg = format_query_error_with_sources(
        "jq",
        ".foo | if",
        input,
        &crate::QueryError::Unsupported(
            "query is not supported by native engine: .foo | if".to_string(),
        ),
    );
    assert!(!msg.contains("unsupported query"));
    assert!(!msg.contains("not supported"));
    assert!(msg.contains("jq: error:"));
    assert!(msg.contains("--> <query>:1:1"));
    assert!(msg.contains("jq: 1 compile error"));

    let unterminated = "[\n  try if .\n         then 1\n         else 2\n  catch ]";
    let msg = format_query_error_with_sources(
        "jq",
        unterminated,
        "",
        &crate::QueryError::Unsupported("parse error: expected EndKw, found Catch".to_string()),
    );
    assert!(msg.contains("unexpected catch"));
    assert!(msg.contains("Possibly unterminated 'if' statement"));
    assert!(msg.contains("Possibly unterminated 'try' statement"));
    assert!(msg.contains("jq: 3 compile errors"));

    let msg = format_query_error_with_sources(
        "jq",
        "if\n",
        "",
        &crate::QueryError::Unsupported("syntax error, unexpected end of file".to_string()),
    );
    assert!(msg.contains("unexpected end of file at <top-level>"));
    assert!(msg.contains("line 1, column 3"));
    assert!(msg.contains("jq: 1 compile error"));
}
