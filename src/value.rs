use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;

#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    #[error("yaml object key must be string, got: {0}")]
    NonStringObjectKey(String),
    #[error("unsupported yaml number: {0}")]
    UnsupportedYamlNumber(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ZqValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<ZqValue>),
    Object(IndexMap<String, ZqValue>),
}

impl ZqValue {
    pub fn from_json(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => ZqValue::Null,
            JsonValue::Bool(v) => ZqValue::Bool(v),
            JsonValue::Number(v) => ZqValue::Number(v),
            JsonValue::String(v) => ZqValue::String(v),
            JsonValue::Array(items) => {
                ZqValue::Array(items.into_iter().map(ZqValue::from_json).collect())
            }
            JsonValue::Object(map) => {
                let mut out = IndexMap::with_capacity(map.len());
                for (k, v) in map {
                    out.insert(k, ZqValue::from_json(v));
                }
                ZqValue::Object(out)
            }
        }
    }

    pub fn try_from_yaml(value: YamlValue) -> Result<Self, ValueError> {
        match value {
            YamlValue::Null => Ok(ZqValue::Null),
            YamlValue::Bool(v) => Ok(ZqValue::Bool(v)),
            YamlValue::Number(v) => yaml_number_to_json_number(v).map(ZqValue::Number),
            YamlValue::String(v) => Ok(ZqValue::String(v)),
            YamlValue::Sequence(items) => items
                .into_iter()
                .map(ZqValue::try_from_yaml)
                .collect::<Result<Vec<_>, _>>()
                .map(ZqValue::Array),
            YamlValue::Mapping(map) => {
                let mut out = IndexMap::with_capacity(map.len());
                for (k, v) in map {
                    let key = match k {
                        YamlValue::String(key) => key,
                        other => {
                            let rendered = serde_yaml::to_string(&other)
                                .map(|s| s.trim().to_string())
                                .unwrap_or_else(|_| "<non-string-key>".to_string());
                            return Err(ValueError::NonStringObjectKey(rendered));
                        }
                    };
                    out.insert(key, ZqValue::try_from_yaml(v)?);
                }
                Ok(ZqValue::Object(out))
            }
            YamlValue::Tagged(tagged) => ZqValue::try_from_yaml(tagged.value),
        }
    }

    pub fn into_json(self) -> JsonValue {
        match self {
            ZqValue::Null => JsonValue::Null,
            ZqValue::Bool(v) => JsonValue::Bool(v),
            ZqValue::Number(v) => {
                if is_non_finite_json_number(&v) {
                    JsonValue::Null
                } else {
                    JsonValue::Number(v)
                }
            }
            ZqValue::String(v) => JsonValue::String(v),
            ZqValue::Array(items) => {
                JsonValue::Array(items.into_iter().map(ZqValue::into_json).collect())
            }
            ZqValue::Object(fields) => {
                let mut out = JsonMap::with_capacity(fields.len());
                for (k, v) in fields {
                    out.insert(k, v.into_json());
                }
                JsonValue::Object(out)
            }
        }
    }

    pub fn is_array_or_object(&self) -> bool {
        matches!(self, ZqValue::Array(_) | ZqValue::Object(_))
    }
}

fn is_non_finite_json_number(number: &JsonNumber) -> bool {
    let raw = number.to_string();
    let unsigned = raw
        .strip_prefix('-')
        .or_else(|| raw.strip_prefix('+'))
        .unwrap_or(&raw);
    let lower = unsigned.to_ascii_lowercase();
    lower == "nan" || lower == "inf" || lower == "infinity"
}

fn yaml_number_to_json_number(number: serde_yaml::Number) -> Result<JsonNumber, ValueError> {
    if let Some(v) = number.as_i64() {
        return Ok(JsonNumber::from(v));
    }
    if let Some(v) = number.as_u64() {
        return Ok(JsonNumber::from(v));
    }
    if let Some(v) = number.as_f64() {
        if let Some(n) = JsonNumber::from_f64(v) {
            return Ok(n);
        }
    }
    Err(ValueError::UnsupportedYamlNumber(number.to_string()))
}

impl ZqValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ZqValue::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            ZqValue::Number(n) => n.as_u64(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ZqValue::Number(n) => n.as_f64(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ZqValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&ZqValue> {
        match self {
            ZqValue::Object(map) => map.get(key),
            _ => None,
        }
    }
}

impl From<i64> for ZqValue {
    fn from(value: i64) -> Self {
        ZqValue::Number(JsonNumber::from(value))
    }
}

impl From<i32> for ZqValue {
    fn from(value: i32) -> Self {
        ZqValue::Number(JsonNumber::from(value))
    }
}

impl From<u64> for ZqValue {
    fn from(value: u64) -> Self {
        ZqValue::Number(JsonNumber::from(value))
    }
}

impl PartialEq<JsonValue> for ZqValue {
    fn eq(&self, other: &JsonValue) -> bool {
        self.clone().into_json() == *other
    }
}

impl PartialEq<ZqValue> for JsonValue {
    fn eq(&self, other: &ZqValue) -> bool {
        *self == other.clone().into_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_yaml_to_native_and_back_to_json() {
        let yaml = r#"
a: 1
b:
  - x
  - true
"#;
        let parsed: YamlValue = serde_yaml::from_str(yaml).expect("yaml parse");
        let native = ZqValue::try_from_yaml(parsed).expect("to native");
        let json = native.into_json();
        assert_eq!(json["a"], serde_json::json!(1));
        assert_eq!(json["b"], serde_json::json!(["x", true]));
    }

    #[test]
    fn rejects_non_string_yaml_keys() {
        let parsed: YamlValue = serde_yaml::from_str("{1: x}").expect("yaml parse");
        let err = ZqValue::try_from_yaml(parsed).expect_err("must fail");
        assert!(matches!(err, ValueError::NonStringObjectKey(_)));
    }

    #[test]
    fn serde_json_roundtrips_as_plain_json_value() {
        let input = r#"{"b":[1,true,null],"a":{"x":"y"}}"#;
        let native: ZqValue = serde_json::from_str(input).expect("deserialize native");
        let encoded = serde_json::to_string(&native).expect("serialize native");
        let reparsed: serde_json::Value = serde_json::from_str(&encoded).expect("reparse json");
        let expected: serde_json::Value = serde_json::from_str(input).expect("parse expected");
        assert_eq!(reparsed, expected);
    }
}
