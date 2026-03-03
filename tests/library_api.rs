use zq::{
    jsonish_equal, parse_native_input_values_auto, run_jq, run_jq_stream_with_paths_options,
    run_native_json_query, run_native_query_stream_with_paths_and_options, run_native_yaml_query,
    EngineRunOptions, NativeInputKind, NativeRunOptions, QueryOptions,
};

#[test]
fn library_native_yaml_json_processing_contract() {
    let out = run_native_json_query(".a", r#"{"a":1}"#).expect("native json query");
    assert_eq!(out, vec![serde_json::json!(1)]);

    let out = run_native_yaml_query(".a", "a: 2\n").expect("native yaml query");
    assert_eq!(out, vec![serde_json::json!(2)]);

    let parsed = parse_native_input_values_auto("a: 1\n---\na: 2\n").expect("auto parse");
    assert_eq!(parsed.kind, NativeInputKind::YamlDocs);
    assert_eq!(
        parsed.values,
        vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
    );
}

#[test]
fn library_jq_functions_contract() {
    let out = run_jq(".a", "a: 3\n", QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!(3)]);

    let out = run_jq_stream_with_paths_options(
        ".a",
        vec![serde_json::json!({"a": 4})],
        &[],
        EngineRunOptions { null_input: false },
    )
    .expect("run jq stream");
    assert_eq!(out, vec![serde_json::json!(4)]);
}

#[test]
fn library_semantic_compare_contract() {
    assert!(
        jsonish_equal(r#"{"a":[1,2],"b":3}"#, r#"{"b":3,"a":[1,2]}"#)
            .expect("semantic object compare")
    );
    assert!(!jsonish_equal("1", "2").expect("semantic inequality compare"));

    let out = run_native_query_stream_with_paths_and_options(
        "1+1",
        vec![],
        &[],
        NativeRunOptions { null_input: true },
    )
    .expect("native stream query");
    assert_eq!(out, vec![serde_json::json!(2)]);
}
