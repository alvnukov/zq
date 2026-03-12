use serde_json::json;
use zq::{
    jsonish_equal, normalize_jsonish_line, parse_doc_mode, prepare_jq_query_with_paths, run_jq,
    run_jq_jsonish_lines, try_run_jq_native_stream_json_text_options,
    try_run_jq_native_stream_with_paths_options, validate_jq_query, validate_jq_query_with_paths,
    EngineRunOptions, NativeStreamStatus, QueryOptions, YamlAnchorNameMode, YamlFormatOptions,
};

#[test]
fn readme_public_api_example_contract() {
    let input = r#"{"apps":[{"name":"a"},{"name":"b"}],"global":{"env":"prod"}}"#;
    let options = QueryOptions {
        doc_mode: parse_doc_mode("first", None).expect("parse doc mode"),
        library_path: Vec::new(),
    };

    let out = run_jq(".global.env", input, options).expect("run jq");
    assert_eq!(out, vec![json!("prod")]);

    let json_lines = zq::format_output_json_lines(&out, false, true).expect("format json");
    assert_eq!(json_lines, "prod");

    let yaml_docs = zq::format_output_yaml_documents(&out).expect("format yaml");
    assert!(yaml_docs.contains("prod"), "yaml output must contain value");

    let yaml_docs_anchored = zq::format_output_yaml_documents_with_options(
        &out,
        YamlFormatOptions::default()
            .with_yaml_anchors(true)
            .with_anchor_name_mode(YamlAnchorNameMode::StrictFriendly)
            .with_anchor_single_token_enrichment(true),
    )
    .expect("format yaml with options");
    assert!(yaml_docs_anchored.contains("prod"), "yaml output must contain value");
}

#[test]
fn jq_public_helpers_contract() {
    validate_jq_query(".apps[] | .name").expect("validate jq query");
    validate_jq_query_with_paths(".apps[] | .name", &[]).expect("validate jq query with paths");

    let prepared = prepare_jq_query_with_paths(".apps[] | .name", &[]).expect("prepare jq query");
    let lines = prepared
        .run_jsonish_lines(r#"{"apps":[{"name":"a"},{"name":"b"}]}"#)
        .expect("run prepared jsonish lines");
    assert_eq!(lines, vec![r#""a""#, r#""b""#]);

    let lines2 = run_jq_jsonish_lines(".apps[] | .name", r#"{"apps":[{"name":"a"}]}"#, &[])
        .expect("run jq jsonish lines");
    assert_eq!(lines2, vec![r#""a""#]);

    let normalized = normalize_jsonish_line(r#"{ "b": 2, "a": 1 }"#).expect("normalize jsonish");
    assert!(
        jsonish_equal(&normalized, r#"{"a":1,"b":2}"#).expect("semantic jsonish compare"),
        "normalized line must be semantically equal"
    );
}

#[test]
fn native_stream_public_entry_points_contract() {
    let mut out_values = Vec::new();
    let status = try_run_jq_native_stream_with_paths_options(
        ".a",
        &[json!({"a": 1}), json!({"a": 2})],
        EngineRunOptions { null_input: false },
        |v| {
            out_values.push(v);
            Ok(())
        },
    )
    .expect("native stream by values");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(out_values, vec![json!(1), json!(2)]);

    let mut out_text = Vec::new();
    let status = try_run_jq_native_stream_json_text_options(
        ".a",
        r#"{"a":3}{"a":4}"#,
        EngineRunOptions { null_input: false },
        |v| {
            out_text.push(v);
            Ok(())
        },
    )
    .expect("native stream by json text");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(out_text, vec![json!(3), json!(4)]);
}
