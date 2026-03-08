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
