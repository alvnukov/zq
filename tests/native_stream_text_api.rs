use zq::{EngineRunOptions, NativeStreamStatus, NativeValue};

#[test]
fn native_text_stream_api_executes_supported_query() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_text_options_native(
        ".a",
        "{\"a\":1}\n{\"a\":2}\n",
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native text stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(
        out,
        vec![
            NativeValue::from_json(serde_json::json!(1)),
            NativeValue::from_json(serde_json::json!(2)),
        ]
    );
}

#[test]
fn native_text_stream_api_preserves_inputs_semantics() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_text_options_native(
        "inputs",
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n",
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native text stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(
        out,
        vec![
            NativeValue::from_json(serde_json::json!({"a":2})),
            NativeValue::from_json(serde_json::json!({"a":3})),
        ]
    );
}

#[test]
fn native_text_stream_api_keeps_auto_fallback_for_yaml() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_text_options_native(
        ".a",
        "a: 1\n",
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native text stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(out, vec![NativeValue::from_json(serde_json::json!(1))]);
}

#[test]
fn native_text_stream_api_supports_input_line_number() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_text_options_native(
        "input_line_number",
        "{\"a\":1}\n{\"a\":2}\n",
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native text stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(
        out,
        vec![
            NativeValue::from_json(serde_json::json!(1)),
            NativeValue::from_json(serde_json::json!(2)),
        ]
    );
}

#[test]
fn native_reader_stream_api_preserves_inputs_semantics() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_reader_options_native(
        "inputs",
        std::io::Cursor::new(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".to_vec()),
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native reader stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(
        out,
        vec![
            NativeValue::from_json(serde_json::json!({"a":2})),
            NativeValue::from_json(serde_json::json!({"a":3})),
        ]
    );
}

#[test]
fn native_reader_stream_api_supports_input_line_number() {
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_json_reader_options_native(
        "input_line_number",
        std::io::Cursor::new(b"{\"a\":1}\n{\"a\":2}\n".to_vec()),
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native reader stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(
        out,
        vec![
            NativeValue::from_json(serde_json::json!(1)),
            NativeValue::from_json(serde_json::json!(2)),
        ]
    );
}
