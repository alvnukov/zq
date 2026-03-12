use indexmap::IndexMap;
use serde::de::{self, DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;
use std::cell::Cell;
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    #[error("yaml object key must be string, got: {0}")]
    NonStringObjectKey(String),
    #[error("unsupported yaml number: {0}")]
    UnsupportedYamlNumber(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ZqValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<ZqValue>),
    Object(IndexMap<String, ZqValue>),
}

const OBJECT_MAP_POOL_LIMIT: usize = 256;
const OBJECT_MAP_RETAIN_CAP: usize = 128;
const VALUE_VEC_POOL_LIMIT: usize = 256;
const VALUE_VEC_RETAIN_CAP: usize = 128;

thread_local! {
    static ACTIVE_RECYCLE_CONTEXT: Cell<usize> = const { Cell::new(0) };
}

pub struct ActiveRecycleContextGuard {
    prev: usize,
}

impl Drop for ActiveRecycleContextGuard {
    fn drop(&mut self) {
        ACTIVE_RECYCLE_CONTEXT.with(|slot| slot.set(self.prev));
    }
}

#[derive(Default)]
pub struct NativeValueRecycleContext {
    stack: Vec<ZqValue>,
    object_maps: Vec<IndexMap<String, ZqValue>>,
    value_vecs: Vec<Vec<ZqValue>>,
}

pub fn install_active_native_value_recycle_context(
    ctx: &mut NativeValueRecycleContext,
) -> ActiveRecycleContextGuard {
    let ptr = (ctx as *mut NativeValueRecycleContext).cast::<()>() as usize;
    let prev = ACTIVE_RECYCLE_CONTEXT.with(|slot| {
        let prev = slot.get();
        slot.set(ptr);
        prev
    });
    ActiveRecycleContextGuard { prev }
}

fn with_active_recycle_context<R>(
    f: impl FnOnce(&mut NativeValueRecycleContext) -> R,
) -> Option<R> {
    ACTIVE_RECYCLE_CONTEXT.with(|slot| {
        let ptr = slot.get();
        if ptr == 0 {
            return None;
        }
        let ctx_ptr = ptr as *mut NativeValueRecycleContext;
        // SAFETY: pointer is installed from a live stack context and restored by
        // ActiveRecycleContextGuard; all access is on the same thread.
        Some(unsafe { f(&mut *ctx_ptr) })
    })
}

pub(crate) fn take_pooled_object_map_with_capacity(
    min_capacity: usize,
) -> IndexMap<String, ZqValue> {
    if let Some(map) =
        with_active_recycle_context(|ctx| ctx.take_object_map_with_capacity(min_capacity))
    {
        return map;
    }
    IndexMap::with_capacity(min_capacity)
}

pub(crate) fn take_pooled_value_vec_with_capacity(min_capacity: usize) -> Vec<ZqValue> {
    if let Some(items) =
        with_active_recycle_context(|ctx| ctx.take_value_vec_with_capacity(min_capacity))
    {
        return items;
    }
    Vec::with_capacity(min_capacity)
}

impl NativeValueRecycleContext {
    pub fn take_object_map_with_capacity(
        &mut self,
        min_capacity: usize,
    ) -> IndexMap<String, ZqValue> {
        let mut map = self.object_maps.pop().unwrap_or_default();
        if map.capacity() < min_capacity {
            map.reserve(min_capacity - map.capacity());
        }
        map
    }

    pub fn take_value_vec_with_capacity(&mut self, min_capacity: usize) -> Vec<ZqValue> {
        let mut items = self.value_vecs.pop().unwrap_or_default();
        if items.capacity() < min_capacity {
            items.reserve(min_capacity - items.capacity());
        }
        items
    }

    pub fn recycle(&mut self, value: ZqValue) {
        self.stack.push(value);
        while let Some(current) = self.stack.pop() {
            match current {
                ZqValue::Array(mut items) => {
                    self.stack.extend(items.drain(..));
                    self.recycle_value_vec(items);
                }
                ZqValue::Object(mut map) => {
                    for (_, child) in map.drain(..) {
                        self.stack.push(child);
                    }
                    self.recycle_object_map(map);
                }
                ZqValue::Null | ZqValue::Bool(_) | ZqValue::Number(_) | ZqValue::String(_) => {}
            }
        }
    }

    fn recycle_object_map(&mut self, mut map: IndexMap<String, ZqValue>) {
        if map.capacity() > OBJECT_MAP_RETAIN_CAP {
            return;
        }
        map.clear();
        if self.object_maps.len() < OBJECT_MAP_POOL_LIMIT {
            self.object_maps.push(map);
        }
    }

    fn recycle_value_vec(&mut self, mut items: Vec<ZqValue>) {
        if items.capacity() > VALUE_VEC_RETAIN_CAP {
            return;
        }
        items.clear();
        if self.value_vecs.len() < VALUE_VEC_POOL_LIMIT {
            self.value_vecs.push(items);
        }
    }
}

impl<'de> Deserialize<'de> for ZqValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct KeyClassifier;

        enum KeyClass {
            Map(String),
            Number,
        }

        impl<'de> DeserializeSeed<'de> for KeyClassifier {
            type Value = KeyClass;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_str(self)
            }
        }

        impl<'de> Visitor<'de> for KeyClassifier {
            type Value = KeyClass;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string key")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "$serde_json::private::Number" => Ok(KeyClass::Number),
                    _ => Ok(KeyClass::Map(value.to_owned())),
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value.as_str() {
                    "$serde_json::private::Number" => Ok(KeyClass::Number),
                    _ => Ok(KeyClass::Map(value)),
                }
            }
        }

        struct ZqValueVisitor;

        impl<'de> Visitor<'de> for ZqValueVisitor {
            type Value = ZqValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON-compatible value")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(ZqValue::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(ZqValue::Number(JsonNumber::from(value)))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(ZqValue::Number(JsonNumber::from(value)))
            }

            fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                let Some(number) = JsonNumber::from_i128(value) else {
                    return Err(E::custom("number out of range"));
                };
                Ok(ZqValue::Number(number))
            }

            fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                let Some(number) = JsonNumber::from_u128(value) else {
                    return Err(E::custom("number out of range"));
                };
                Ok(ZqValue::Number(number))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                let Some(number) = JsonNumber::from_f64(value) else {
                    return Err(E::custom("non-finite number is not supported"));
                };
                Ok(ZqValue::Number(number))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(ZqValue::String(value.to_owned()))
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(ZqValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(ZqValue::String(value))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(ZqValue::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(ZqValue::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                ZqValue::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = take_pooled_value_vec_with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(item) = seq.next_element::<ZqValue>()? {
                    items.push(item);
                }
                Ok(ZqValue::Array(items))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                match map.next_key_seed(KeyClassifier)? {
                    Some(KeyClass::Number) => {
                        let raw = map.next_value::<String>()?;
                        let number = raw.parse::<JsonNumber>().map_err(A::Error::custom)?;
                        Ok(ZqValue::Number(number))
                    }
                    Some(KeyClass::Map(first_key)) => {
                        let mut fields = take_pooled_object_map_with_capacity(
                            map.size_hint().unwrap_or(0).saturating_add(1),
                        );
                        fields.insert(first_key, map.next_value::<ZqValue>()?);
                        while let Some((key, value)) = map.next_entry::<String, ZqValue>()? {
                            fields.insert(key, value);
                        }
                        Ok(ZqValue::Object(fields))
                    }
                    None => {
                        let fields = take_pooled_object_map_with_capacity(0);
                        Ok(ZqValue::Object(fields))
                    }
                }
            }
        }

        deserializer.deserialize_any(ZqValueVisitor)
    }
}

pub fn recycle_native_value(value: ZqValue) {
    let mut recycle = NativeValueRecycleContext::default();
    recycle.recycle(value);
}

pub fn recycle_native_values(values: impl IntoIterator<Item = ZqValue>) {
    let mut recycle = NativeValueRecycleContext::default();
    for value in values {
        recycle.recycle(value);
    }
}

pub fn recycle_native_values_with_context(
    ctx: &mut NativeValueRecycleContext,
    values: impl IntoIterator<Item = ZqValue>,
) {
    for value in values {
        ctx.recycle(value);
    }
}

impl ZqValue {
    pub const fn jq_type_name(&self) -> &'static str {
        match self {
            ZqValue::Null => "null",
            ZqValue::Bool(_) => "boolean",
            ZqValue::Number(_) => "number",
            ZqValue::String(_) => "string",
            ZqValue::Array(_) => "array",
            ZqValue::Object(_) => "object",
        }
    }

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
    let unsigned = raw.strip_prefix('-').or_else(|| raw.strip_prefix('+')).unwrap_or(&raw);
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

    #[test]
    fn serde_json_deserializes_top_level_decimal_as_number() {
        let native: ZqValue = serde_json::from_str("-1.1").expect("deserialize native");
        let ZqValue::Number(number) = native else {
            panic!("expected number");
        };
        assert_eq!(number.to_string(), "-1.1");
    }

    #[test]
    fn serde_json_deserializes_nested_decimal_as_number() {
        let native: ZqValue = serde_json::from_str("[-1.1]").expect("deserialize native");
        let ZqValue::Array(items) = native else {
            panic!("expected array");
        };
        assert_eq!(items.len(), 1);
        let ZqValue::Number(number) = &items[0] else {
            panic!("expected number");
        };
        assert_eq!(number.to_string(), "-1.1");
    }

    #[test]
    fn serde_json_deserializes_large_integer_as_number() {
        let native: ZqValue = serde_json::from_str("10000000000000000000000000000000000002")
            .expect("deserialize native");
        let ZqValue::Number(number) = native else {
            panic!("expected number");
        };
        assert_eq!(number.to_string(), "10000000000000000000000000000000000002");
    }

    #[test]
    fn jq_type_name_matches_jq_contract() {
        assert_eq!(ZqValue::Null.jq_type_name(), "null");
        assert_eq!(ZqValue::Bool(true).jq_type_name(), "boolean");
        assert_eq!(ZqValue::from(1).jq_type_name(), "number");
        assert_eq!(ZqValue::String("x".to_string()).jq_type_name(), "string");
        assert_eq!(ZqValue::Array(Vec::new()).jq_type_name(), "array");
        assert_eq!(ZqValue::Object(IndexMap::new()).jq_type_name(), "object");
    }

    #[test]
    fn recycle_context_reuses_object_maps() {
        let mut ctx = NativeValueRecycleContext::default();
        let mut map = IndexMap::with_capacity(8);
        map.insert("a".to_string(), ZqValue::from(1));

        ctx.recycle(ZqValue::Object(map));

        let reused = ctx.take_object_map_with_capacity(1);
        assert!(reused.is_empty());
        assert!(reused.capacity() >= 1);
    }

    #[test]
    fn recycle_context_respects_requested_capacity() {
        let mut ctx = NativeValueRecycleContext::default();
        let map = ctx.take_object_map_with_capacity(32);
        assert!(map.capacity() >= 32);
    }

    #[test]
    fn active_recycle_context_drives_object_map_pool() {
        let mut ctx = NativeValueRecycleContext::default();
        {
            let _guard = install_active_native_value_recycle_context(&mut ctx);
            let mut map = take_pooled_object_map_with_capacity(16);
            map.insert("k".to_string(), ZqValue::from(1));
            ctx.recycle(ZqValue::Object(map));

            let reused = take_pooled_object_map_with_capacity(8);
            assert!(reused.is_empty());
            assert!(reused.capacity() >= 8);
        }

        // Outside installed context fallback should still provide a usable map.
        let fallback = take_pooled_object_map_with_capacity(4);
        assert!(fallback.capacity() >= 4);
    }

    #[test]
    fn active_recycle_context_drives_value_vec_pool() {
        let mut ctx = NativeValueRecycleContext::default();
        {
            let _guard = install_active_native_value_recycle_context(&mut ctx);
            let mut items = take_pooled_value_vec_with_capacity(8);
            items.push(ZqValue::from(1));
            items.push(ZqValue::from(2));
            ctx.recycle(ZqValue::Array(items));

            let reused = take_pooled_value_vec_with_capacity(4);
            assert!(reused.is_empty());
            assert!(reused.capacity() >= 4);
        }

        let fallback = take_pooled_value_vec_with_capacity(2);
        assert!(fallback.capacity() >= 2);
    }
}
