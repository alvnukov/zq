use zq::{format_output_yaml_documents, format_output_yaml_documents_native, NativeValue};

#[test]
fn native_yaml_formatter_matches_json_yaml_formatter() {
    let big_num: serde_json::Value =
        serde_json::from_str(r#"{"big": 1234567890123456789012345678901234567890}"#)
            .expect("parse big number");
    let json_values =
        vec![serde_json::json!({"a": 1, "b": [true, null]}), serde_json::json!([1, 2, 3]), big_num];
    let native_values = json_values.iter().cloned().map(NativeValue::from_json).collect::<Vec<_>>();
    let json_rendered = format_output_yaml_documents(&json_values).expect("json yaml format");
    let native_rendered =
        format_output_yaml_documents_native(&native_values).expect("native yaml format");
    assert_eq!(native_rendered, json_rendered);
}
