use crate::c_compat::math as c_math;
use crate::value::ZqValue;

use super::super::ast::{BinaryOp, Stage};

pub(super) fn parse_number_literal(raw: &str) -> Result<ZqValue, String> {
    if parse_json_number(raw).is_some() {
        return Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(
            raw.to_string(),
        )));
    }

    if raw.starts_with('.') {
        let adjusted = format!("0{raw}");
        if parse_json_number(&adjusted).is_some() {
            return Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(
                adjusted,
            )));
        }
    }

    if let Some(adjusted) = normalize_jq_float_text(raw) {
        if parse_json_number(&adjusted).is_some() {
            return Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(
                adjusted,
            )));
        }
    }

    let parsed = raw
        .parse::<f64>()
        .map_err(|_| format!("parse error: invalid number literal `{raw}`"))?;
    if parsed.is_nan() {
        return Err(format!("parse error: invalid number literal `{raw}`"));
    }

    let Some(number) = serde_json::Number::from_f64(parsed) else {
        return Err(format!("parse error: invalid number literal `{raw}`"));
    };
    Ok(ZqValue::Number(number))
}

pub(super) fn fold_large_integer_literal_equality(
    lhs: &Stage,
    rhs: &Stage,
    op: BinaryOp,
) -> Option<Stage> {
    if !matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
        return None;
    }
    let (Stage::Literal(ZqValue::Number(left)), Stage::Literal(ZqValue::Number(right))) =
        (lhs, rhs)
    else {
        return None;
    };

    let left_raw = left.to_string();
    let right_raw = right.to_string();
    if !is_large_plain_integer_text(&left_raw) || !is_large_plain_integer_text(&right_raw) {
        return None;
    }

    let left_norm = left_raw.strip_prefix('+').unwrap_or(&left_raw);
    let right_norm = right_raw.strip_prefix('+').unwrap_or(&right_raw);
    let eq = left_norm == right_norm;
    Some(Stage::Literal(ZqValue::Bool(
        if matches!(op, BinaryOp::Eq) { eq } else { !eq },
    )))
}

fn is_large_plain_integer_text(raw: &str) -> bool {
    c_math::plain_integer_digit_count(raw).is_some_and(|digits| digits > 15)
}

pub(super) fn special_number_literal(raw: &str) -> ZqValue {
    ZqValue::Number(serde_json::Number::from_string_unchecked(raw.to_string()))
}

fn parse_json_number(text: &str) -> Option<serde_json::Number> {
    let mut de = serde_json::Deserializer::from_str(text);
    let number = <serde_json::Number as serde::Deserialize>::deserialize(&mut de).ok()?;
    de.end().ok()?;
    Some(number)
}

pub(super) fn normalize_jq_float_text(raw: &str) -> Option<String> {
    if raw.ends_with('.') {
        return Some(format!("{raw}0"));
    }
    if let Some(idx) = raw.find(".e").or_else(|| raw.find(".E")) {
        let mut s = raw.to_string();
        s.insert(idx + 1, '0');
        return Some(s);
    }
    None
}

pub(super) fn const_object_key_error(value: &ZqValue) -> Option<String> {
    if matches!(value, ZqValue::String(_)) {
        return None;
    }
    let rendered = value.clone().into_json().to_string();
    Some(format!(
        "Cannot use {} ({rendered}) as object key",
        zq_type_name(value)
    ))
}

pub(super) fn zq_type_name(value: &ZqValue) -> &'static str {
    match value {
        ZqValue::Null => "null",
        ZqValue::Bool(_) => "boolean",
        ZqValue::Number(_) => "number",
        ZqValue::String(_) => "string",
        ZqValue::Array(_) => "array",
        ZqValue::Object(_) => "object",
    }
}
