use zq::{EngineRunOptions, NativeStreamStatus, NativeValue};

#[test]
fn native_stream_api_executes_supported_query() {
    let input = vec![NativeValue::from_json(serde_json::json!({"a": 1}))];
    let mut out = Vec::new();
    let status = zq::try_run_jq_native_stream_with_paths_options_native(
        ".a",
        &input,
        EngineRunOptions { null_input: false },
        |value| {
            out.push(value);
            Ok(())
        },
    )
    .expect("native stream run");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(out, vec![NativeValue::from_json(serde_json::json!(1))]);
}
