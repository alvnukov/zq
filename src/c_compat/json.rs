// c-ref: jq JSON parse/print compatibility helpers (jv_parse/jv_print behavior).

use crate::c_compat::math as c_math;
use crate::c_compat::value as c_value;
use crate::value::ZqValue;
use serde::Deserialize;

pub(crate) fn parse_json_value_for_fromjson(
    text: &str,
) -> Result<serde_json::Value, serde_json::Error> {
    let mut de = serde_json::Deserializer::from_str(text);
    de.disable_recursion_limit();
    let value = serde_json::Value::deserialize(&mut de)?;
    de.end()?;
    Ok(value)
}

pub(crate) fn check_fromjson_depth_limit(text: &str) -> Result<(), String> {
    // jq parser depth guard: keep 9999-level payloads parseable while rejecting deeper JSON.
    const MAX_PARSE_DEPTH: usize = 10_000;

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for ch in text.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '[' | '{' => {
                depth += 1;
                if depth > MAX_PARSE_DEPTH {
                    return Err("Exceeds depth limit for parsing".to_string());
                }
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn parse_jq_single_quote_json_error(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if !trimmed.contains('\'') {
        return None;
    }
    let first = trimmed.find('\'')?;
    let second = trimmed[first + 1..].find('\'').map(|idx| first + 1 + idx);
    let bad_pos = second.unwrap_or(first);
    let column = bad_pos + 2;
    Some(format!(
        "Invalid string literal; expected \", but got ' at line 1, column {column} (while parsing '{trimmed}')"
    ))
}

pub(crate) fn normalize_for_tojson(value: ZqValue) -> ZqValue {
    normalize_for_tojson_with_depth(value, 1)
}

// moved-from: src/native_engine/vm_core/vm.rs::run_tonumber
pub(crate) fn tonumber_filter_jq(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(_) => Ok(input),
        ZqValue::String(s) => {
            if let Some(parsed) = c_math::parse_finite_number_literal_jq(&s) {
                Ok(c_math::number_to_value(parsed))
            } else {
                Err(format!("string ({s:?}) cannot be parsed as a number"))
            }
        }
        other => Err(format!(
            "{} ({}) cannot be parsed as a number",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_tostring
pub(crate) fn tostring_filter_jq(input: ZqValue) -> Result<ZqValue, String> {
    if let ZqValue::String(_) = input {
        return Ok(input);
    }
    Ok(ZqValue::String(tostring_value_jq(&input)?))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_toboolean
pub(crate) fn toboolean_filter_jq(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Bool(_) => Ok(input),
        ZqValue::String(s) => match s.as_str() {
            "true" => Ok(ZqValue::Bool(true)),
            "false" => Ok(ZqValue::Bool(false)),
            _ => Err(format!("string ({s:?}) cannot be parsed as a boolean")),
        },
        other => Err(format!(
            "{} ({}) cannot be parsed as a boolean",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_tojson
pub(crate) fn tojson_filter_jq(input: ZqValue) -> Result<ZqValue, String> {
    let normalized = normalize_for_tojson(input);
    serde_json::to_string(&normalized.into_json())
        .map(ZqValue::String)
        .map_err(|e| format!("encode value: {e}"))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_fromjson
pub(crate) fn fromjson_filter_jq(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::String(s) => {
            check_fromjson_depth_limit(&s)?;
            match parse_json_value_for_fromjson(&s) {
                Ok(value) => Ok(ZqValue::from_json(value)),
                Err(err) => match c_math::parse_jq_non_finite_number(&s) {
                    Ok(Some(value)) => Ok(ZqValue::Number(value)),
                    Ok(None) => {
                        if let Some(msg) = parse_jq_single_quote_json_error(&s) {
                            Err(msg)
                        } else {
                            Err(err.to_string())
                        }
                    }
                    Err(msg) => Err(msg),
                },
            }
        }
        other => Err(format!(
            "{} ({}) only strings can be parsed",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::jq_tostring_value
pub(crate) fn tostring_value_jq(value: &ZqValue) -> Result<String, String> {
    match value {
        ZqValue::String(s) => Ok(s.clone()),
        ZqValue::Null => Ok("null".to_string()),
        ZqValue::Bool(b) => Ok(b.to_string()),
        ZqValue::Number(n) => Ok(n.to_string()),
        ZqValue::Array(_) | ZqValue::Object(_) => serde_json::to_string(&value.clone().into_json())
            .map_err(|e| format!("encode value: {e}")),
    }
}

fn normalize_for_tojson_with_depth(value: ZqValue, depth: usize) -> ZqValue {
    // jq/src/jv_print.c pretty-printer protects against excessively deep values.
    // Keep depth 10001 representable (used by jq fixtures) and truncate deeper levels.
    const TOJSON_MAX_DEPTH: usize = 10_001;
    if depth > TOJSON_MAX_DEPTH {
        return ZqValue::String("<skipped: too deep>".to_string());
    }
    match value {
        ZqValue::Array(items) => ZqValue::Array(
            items
                .into_iter()
                .map(|item| normalize_for_tojson_with_depth(item, depth + 1))
                .collect(),
        ),
        ZqValue::Object(map) => ZqValue::Object(
            map.into_iter()
                .map(|(k, v)| (k, normalize_for_tojson_with_depth(v, depth + 1)))
                .collect(),
        ),
        ZqValue::Number(number) => ZqValue::Number(c_math::normalize_number_for_tojson(number)),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_fromjson_depth_limit_rejects_too_deep_payloads() {
        let mut deep = String::new();
        for _ in 0..10_001 {
            deep.push('[');
        }
        for _ in 0..10_001 {
            deep.push(']');
        }
        let err = check_fromjson_depth_limit(&deep).expect_err("must reject");
        assert_eq!(err, "Exceeds depth limit for parsing");
    }

    #[test]
    fn parse_single_quote_json_error_returns_jq_shape() {
        let msg = parse_jq_single_quote_json_error("{'a':1}").expect("must detect");
        assert!(msg.contains("expected \", but got '"));
        assert!(msg.contains("line 1, column"));
    }

    #[test]
    fn normalize_for_tojson_expands_negative_exponent_numbers() {
        let value = ZqValue::Number(serde_json::Number::from_string_unchecked(
            "1e-2".to_string(),
        ));
        let out = normalize_for_tojson(value);
        assert_eq!(out.into_json(), serde_json::json!(0.01));
    }

    #[test]
    fn tostring_value_jq_formats_scalars_and_arrays() {
        assert_eq!(tostring_value_jq(&ZqValue::Null).expect("null"), "null");
        assert_eq!(
            tostring_value_jq(&ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)]))
                .expect("array"),
            "[1,2]"
        );
    }

    #[test]
    fn scalar_conversion_filters_follow_jq_shapes() {
        let n = tonumber_filter_jq(ZqValue::String("2.5".to_string())).expect("tonumber");
        assert_eq!(n.into_json(), serde_json::json!(2.5));

        let s = tostring_filter_jq(ZqValue::from(1)).expect("tostring");
        assert_eq!(s.into_json(), serde_json::json!("1"));

        let b = toboolean_filter_jq(ZqValue::String("true".to_string())).expect("toboolean");
        assert_eq!(b.into_json(), serde_json::json!(true));

        let json = tojson_filter_jq(ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)]))
            .expect("tojson");
        assert_eq!(json.into_json(), serde_json::json!("[1,2]"));

        let parsed = fromjson_filter_jq(ZqValue::String("[1,2]".to_string())).expect("fromjson");
        assert_eq!(parsed.into_json(), serde_json::json!([1, 2]));
    }
}
