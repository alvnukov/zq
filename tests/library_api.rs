use zq::{
    jsonish_equal, parse_native_input_values_auto, parse_native_input_values_with_format, run_jq,
    run_jq_stream_with_paths_options, run_native_json_query,
    run_native_query_stream_with_paths_and_options, run_native_yaml_query, EngineRunOptions,
    NativeInputFormat, NativeInputKind, NativeRunOptions, QueryOptions,
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

#[test]
fn library_auto_parse_supports_toml_and_csv() {
    let toml = "name = \"svc\"\nport = 8080\n";
    let parsed_toml = parse_native_input_values_auto(toml).expect("toml auto parse");
    assert_eq!(parsed_toml.kind, NativeInputKind::JsonStream);
    assert_eq!(
        parsed_toml.values,
        vec![serde_json::json!({"name": "svc", "port": 8080})]
    );

    let csv = "name,port\nsvc-a,8080\nsvc-b,9090\n";
    let parsed_csv = parse_native_input_values_auto(csv).expect("csv auto parse");
    assert_eq!(parsed_csv.kind, NativeInputKind::JsonStream);
    assert_eq!(
        parsed_csv.values,
        vec![
            serde_json::json!({"name": "svc-a", "port": "8080"}),
            serde_json::json!({"name": "svc-b", "port": "9090"})
        ]
    );

    let xml = "<catalog><book><title>Rust</title></book></catalog>";
    let parsed_xml = parse_native_input_values_auto(xml).expect("xml auto parse");
    assert_eq!(parsed_xml.kind, NativeInputKind::JsonStream);
    assert_eq!(
        parsed_xml.values,
        vec![serde_json::json!({"catalog":{"book":{"title":"Rust"}}})]
    );

    let xml_scalars = "<root><n>10</n><flag>true</flag><none>null</none></root>";
    let parsed_xml_scalars = parse_native_input_values_auto(xml_scalars).expect("xml scalar parse");
    assert_eq!(
        parsed_xml_scalars.values,
        vec![serde_json::json!({"root":{"n":"10","flag":"true","none":"null"}})]
    );

    let mixed = "<p>Hello <b>xml</b> world</p>";
    let parsed_mixed = parse_native_input_values_auto(mixed).expect("xml mixed parse");
    assert_eq!(
        parsed_mixed.values,
        vec![serde_json::json!({"p":{"b":"xml","#text":"Hello world"}})]
    );
}

#[test]
fn library_forced_input_format_contract() {
    let parsed = parse_native_input_values_with_format("a;b\n1;2\n", NativeInputFormat::Csv)
        .expect("forced csv parse");
    assert_eq!(parsed.kind, NativeInputKind::JsonStream);
    assert_eq!(parsed.values, vec![serde_json::json!({"a":"1","b":"2"})]);

    let one_column = "cases\n\"[{\"\"id\"\":\"\"jq_identity\"\"}]\"\n";
    let parsed_one_column =
        parse_native_input_values_with_format(one_column, NativeInputFormat::Csv)
            .expect("forced one-column csv parse");
    assert_eq!(parsed_one_column.kind, NativeInputKind::JsonStream);
    assert_eq!(
        parsed_one_column.values,
        vec![serde_json::json!({"cases":"[{\"id\":\"jq_identity\"}]"})]
    );

    let parsed_xml = parse_native_input_values_with_format(
        "<catalog><book><title>Rust</title></book></catalog>",
        NativeInputFormat::Xml,
    )
    .expect("forced xml parse");
    assert_eq!(parsed_xml.kind, NativeInputKind::JsonStream);
    assert_eq!(
        parsed_xml.values,
        vec![serde_json::json!({"catalog":{"book":{"title":"Rust"}}})]
    );

    let parsed_xml_scalars = parse_native_input_values_with_format(
        "<root><n>10</n><flag>true</flag><none>null</none></root>",
        NativeInputFormat::Xml,
    )
    .expect("forced xml scalar parse");
    assert_eq!(
        parsed_xml_scalars.values,
        vec![serde_json::json!({"root":{"n":"10","flag":"true","none":"null"}})]
    );
}

#[test]
fn library_auto_xml_error_is_explicit_contract() {
    let err = parse_native_input_values_auto("<root><a></root>").expect_err("must fail");
    match err {
        zq::QueryError::Runtime(message) => {
            assert!(message.contains("xml:"), "unexpected message: {message}");
        }
        other => panic!("expected xml runtime error, got: {other}"),
    }
}
