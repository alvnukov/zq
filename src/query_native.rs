#[cfg(test)]
use crate::c_compat::{math as c_math, time as c_time};
use crate::value::{
    install_active_native_value_recycle_context, NativeValueRecycleContext,
    ValueError as NativeValueError, ZqValue,
};
#[cfg(test)]
use base64::Engine as _;
#[cfg(test)]
use regex::Regex;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{Arc, Mutex};

#[path = "query_native/csv_input.rs"]
mod csv_input;
#[path = "query_native/jsonish.rs"]
mod jsonish;
#[path = "query_native/xml_input.rs"]
mod xml_input;

use self::csv_input::{parse_csv_native_rows, parse_csv_native_rows_auto};
#[cfg(test)]
use self::jsonish::parse_jsonish;
use self::jsonish::{
    canonicalize_jsonish_tokens, normalize_legacy_number_tokens, parse_jsonish_value_native,
    replace_non_finite_number_tokens, stringify_jsonish_value_native,
};
#[cfg(test)]
use self::jsonish::{parse_jsonish_value, stringify_jsonish_value};
use self::xml_input::parse_xml_native_doc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported query: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Runtime(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("{0}")]
    Thrown(JsonValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    JsonStream,
    YamlDocs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    Auto,
    Json,
    Yaml,
    Toml,
    Csv,
    Xml,
}

#[derive(Debug, Clone)]
pub struct ParsedInput {
    pub kind: InputKind,
    pub values: Vec<JsonValue>,
}

#[derive(Debug, Clone)]
pub struct ParsedNativeInput {
    pub kind: InputKind,
    pub values: Vec<ZqValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOptions {
    pub null_input: bool,
}

#[allow(dead_code)]
pub fn run_json_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    run_json_query_native(query, input).map(native_values_to_json)
}

#[allow(dead_code)]
pub fn run_json_query_native(query: &str, input: &str) -> Result<Vec<ZqValue>, Error> {
    let input_value = parse_single_native_json_first(input)?;
    run_query_stream_native(query, vec![input_value])
}

#[allow(dead_code)]
pub fn run_yaml_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    run_yaml_query_native(query, input).map(native_values_to_json)
}

#[allow(dead_code)]
pub fn run_yaml_query_native(query: &str, input: &str) -> Result<Vec<ZqValue>, Error> {
    let as_native = parse_single_native_yaml_first(input)?;
    run_query_stream_native(query, vec![as_native])
}

pub fn run_query_stream(
    query: &str,
    input_stream: Vec<JsonValue>,
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, &[], RunOptions::default())
}

pub fn run_query_stream_native(
    query: &str,
    input_stream: Vec<ZqValue>,
) -> Result<Vec<ZqValue>, Error> {
    run_query_stream_native_with_paths_and_options(query, input_stream, &[], RunOptions::default())
}

pub fn run_query_stream_with_paths(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(
        query,
        input_stream,
        library_paths,
        RunOptions::default(),
    )
}

pub fn run_query_stream_native_with_paths(
    query: &str,
    input_stream: Vec<ZqValue>,
    library_paths: &[String],
) -> Result<Vec<ZqValue>, Error> {
    run_query_stream_native_with_paths_and_options(
        query,
        input_stream,
        library_paths,
        RunOptions::default(),
    )
}

pub fn run_query_stream_with_paths_and_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    let native_inputs = json_values_to_native(input_stream);
    run_query_stream_native_with_paths_and_options(query, native_inputs, library_paths, run_options)
        .map(native_values_to_json)
}

pub fn run_query_stream_native_with_paths_and_options(
    query: &str,
    input_stream: Vec<ZqValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<ZqValue>, Error> {
    let query = strip_jq_comments(query);
    let query = query.as_str();
    match crate::native_engine::try_execute_native_with_paths_owned(
        query,
        input_stream,
        library_paths,
        crate::native_engine::RunOptions { null_input: run_options.null_input },
    ) {
        crate::native_engine::TryExecuteNative::Executed(Ok(values)) => Ok(values),
        crate::native_engine::TryExecuteNative::Executed(Err(e)) => Err(Error::Runtime(e)),
        crate::native_engine::TryExecuteNative::Unsupported => {
            let compile_error =
                crate::native_engine::try_compile_error_with_paths(query, library_paths)
                    .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
            Err(Error::Unsupported(compile_error))
        }
    }
}

pub fn run_query_stream_jsonish(
    query: &str,
    input_jsonish: &str,
    library_paths: &[String],
) -> Result<Vec<String>, Error> {
    let prepared = prepare_query_with_paths(query, library_paths)?;
    prepared.run_jsonish(input_jsonish)
}

pub struct PreparedQuery {
    query: String,
    library_paths: Vec<String>,
}

impl PreparedQuery {
    pub fn run_jsonish(&self, input_jsonish: &str) -> Result<Vec<String>, Error> {
        let input = parse_jsonish_value_native(input_jsonish)?;
        let out = run_query_stream_native_with_paths_and_options(
            &self.query,
            vec![input],
            &self.library_paths,
            RunOptions::default(),
        )?;
        out.into_iter().map(|v| stringify_jsonish_value_native(&v)).collect::<Result<Vec<_>, _>>()
    }

    pub fn run_jsonish_lenient(&self, input_jsonish: &str) -> Result<Vec<String>, Error> {
        self.run_jsonish(input_jsonish)
    }
}

pub fn prepare_query_with_paths(
    query: &str,
    library_paths: &[String],
) -> Result<PreparedQuery, Error> {
    validate_query_with_paths(query, library_paths)?;
    Ok(PreparedQuery { query: query.to_string(), library_paths: library_paths.to_vec() })
}

pub fn validate_query(query: &str) -> Result<(), Error> {
    validate_query_with_paths(query, &[])
}

pub fn validate_query_with_paths(query: &str, library_paths: &[String]) -> Result<(), Error> {
    let query = strip_jq_comments(query);
    let query = query.as_str();
    match crate::native_engine::try_compile_error_with_paths(query, library_paths) {
        None => Ok(()),
        Some(msg) => Err(Error::Unsupported(msg)),
    }
}

pub fn normalize_jsonish_line(line: &str) -> Result<String, Error> {
    let value = parse_jsonish_value_native(line)?;
    stringify_jsonish_value_native(&value)
}

pub fn jsonish_equal(left: &str, right: &str) -> Result<bool, Error> {
    let left = parse_jsonish_value_native(left)?;
    let right = parse_jsonish_value_native(right)?;
    Ok(left == right)
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_json(input: &str) -> Result<Vec<JsonValue>, Error> {
    parse_input_docs_prefer_json_native(input).map(native_values_to_json)
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_json_native(input: &str) -> Result<Vec<ZqValue>, Error> {
    match parse_json_value_stream_native(input) {
        Ok(values) => Ok(values),
        Err(json_err) => match parse_yaml_native_docs_with_merge(input) {
            Ok(values) => {
                if !yaml_native_docs_compatible_with_json_preference(&values) {
                    return Err(Error::Json(json_err));
                }
                Ok(values)
            }
            Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

pub fn parse_input_values_auto(input: &str) -> Result<ParsedInput, Error> {
    parse_input_values_with_format(input, InputFormat::Auto)
}

pub fn parse_input_values_with_format(
    input: &str,
    format: InputFormat,
) -> Result<ParsedInput, Error> {
    let parsed = parse_input_values_with_format_native(input, format)?;
    Ok(ParsedInput { kind: parsed.kind, values: native_values_to_json(parsed.values) })
}

pub fn parse_input_values_auto_native(input: &str) -> Result<ParsedNativeInput, Error> {
    parse_input_values_with_format_native(input, InputFormat::Auto)
}

pub fn parse_input_values_with_format_native(
    input: &str,
    format: InputFormat,
) -> Result<ParsedNativeInput, Error> {
    match format {
        InputFormat::Auto => parse_input_values_auto_native_impl(input),
        InputFormat::Json => parse_json_value_stream_native(input)
            .map(|values| ParsedNativeInput { kind: InputKind::JsonStream, values })
            .map_err(Error::Json),
        InputFormat::Yaml => parse_yaml_native_docs_with_merge(input)
            .map(|values| ParsedNativeInput { kind: InputKind::YamlDocs, values }),
        InputFormat::Toml => parse_toml_native_doc(input)
            .map(|value| ParsedNativeInput { kind: InputKind::JsonStream, values: vec![value] }),
        // Forced CSV must accept valid one-column CSV as well; strict delimiter
        // probing is reserved for auto-detection to avoid false positives.
        InputFormat::Csv => parse_csv_native_rows(input, false)
            .map(|values| ParsedNativeInput { kind: InputKind::JsonStream, values }),
        InputFormat::Xml => parse_xml_native_doc(input)
            .map(|value| ParsedNativeInput { kind: InputKind::JsonStream, values: vec![value] }),
    }
}

pub fn parse_json_values_only(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    parse_json_values_only_native(input).map(native_values_to_json)
}

pub fn parse_json_values_only_native(input: &str) -> Result<Vec<ZqValue>, serde_json::Error> {
    parse_json_value_stream_native(input)
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_yaml(input: &str) -> Result<Vec<JsonValue>, Error> {
    match parse_input_docs_prefer_yaml_native(input) {
        Ok(values) => Ok(native_values_to_json(values)),
        Err(Error::Yaml(yaml_err)) => {
            let mut recycle_ctx = NativeValueRecycleContext::default();
            let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
            match serde_json::from_str::<ZqValue>(input) {
                Ok(v) => Ok(vec![v.into_json()]),
                Err(_) => Err(Error::Yaml(yaml_err)),
            }
        }
        Err(e) => Err(e),
    }
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_yaml_native(input: &str) -> Result<Vec<ZqValue>, Error> {
    parse_yaml_native_docs_with_merge(input)
}

fn parse_yaml_native_with_merge(input: &str) -> Result<ZqValue, Error> {
    let raw: serde_yaml::Value = serde_yaml::from_str(input).map_err(Error::Yaml)?;
    let normalized = crate::yamlmerge::normalize_value_from_source(input, raw);
    ZqValue::try_from_yaml(normalized).map_err(yaml_to_native_error)
}

fn parse_single_native_json_first(input: &str) -> Result<ZqValue, Error> {
    let mut recycle_ctx = NativeValueRecycleContext::default();
    let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
    match serde_json::from_str::<ZqValue>(input) {
        Ok(v) => Ok(v),
        Err(json_err) => match parse_yaml_native_with_merge(input) {
            Ok(v) if v.is_array_or_object() => Ok(v),
            Ok(_) | Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

fn parse_single_native_yaml_first(input: &str) -> Result<ZqValue, Error> {
    match parse_yaml_native_with_merge(input) {
        Ok(v) => Ok(v),
        Err(Error::Yaml(yaml_err)) => {
            let mut recycle_ctx = NativeValueRecycleContext::default();
            let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
            match serde_json::from_str::<ZqValue>(input) {
                Ok(v) => Ok(v),
                Err(_) => Err(Error::Yaml(yaml_err)),
            }
        }
        Err(e) => Err(e),
    }
}

fn parse_yaml_native_docs_with_merge(input: &str) -> Result<Vec<ZqValue>, Error> {
    let docs = crate::yamlmerge::normalize_documents(input).map_err(Error::Yaml)?;
    docs.into_iter()
        .map(|value| ZqValue::try_from_yaml(value).map_err(yaml_to_native_error))
        .collect()
}

fn parse_input_values_auto_native_impl(input: &str) -> Result<ParsedNativeInput, Error> {
    match parse_json_value_stream_strict_native(input) {
        Ok(values) => Ok(ParsedNativeInput { kind: InputKind::JsonStream, values }),
        Err(strict_json_err) => {
            match parse_yaml_native_docs_with_merge(input) {
                Ok(values) => {
                    if yaml_native_docs_compatible_with_json_preference(&values) {
                        return Ok(ParsedNativeInput { kind: InputKind::YamlDocs, values });
                    }
                }
                Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => {}
                Err(e) => return Err(e),
            }

            match parse_json_value_stream_from_strict_failure(input, strict_json_err) {
                Ok(values) => Ok(ParsedNativeInput { kind: InputKind::JsonStream, values }),
                Err(json_err) => {
                    if let Ok(value) = parse_toml_native_doc(input) {
                        return Ok(ParsedNativeInput {
                            kind: InputKind::JsonStream,
                            values: vec![value],
                        });
                    }
                    let xml_result = if looks_like_xml_input(input) {
                        Some(parse_xml_native_doc(input))
                    } else {
                        None
                    };
                    if let Some(Ok(value)) = xml_result.as_ref() {
                        return Ok(ParsedNativeInput {
                            kind: InputKind::JsonStream,
                            values: vec![value.clone()],
                        });
                    }
                    if let Some(values) = parse_csv_native_rows_auto(input) {
                        return Ok(ParsedNativeInput { kind: InputKind::JsonStream, values });
                    }
                    if let Some(Err(xml_err)) = xml_result {
                        return Err(xml_err);
                    }
                    Err(Error::Json(json_err))
                }
            }
        }
    }
}

fn looks_like_xml_input(input: &str) -> bool {
    input.trim_start().starts_with('<')
}

fn parse_toml_native_doc(input: &str) -> Result<ZqValue, Error> {
    let value: toml::Value =
        toml::from_str(input).map_err(|e| Error::Runtime(format!("toml: {e}")))?;
    let json_value =
        serde_json::to_value(value).map_err(|e| Error::Runtime(format!("toml: {e}")))?;
    Ok(ZqValue::from_json(json_value))
}

fn yaml_to_native_error(err: NativeValueError) -> Error {
    Error::Unsupported(format!("yaml to native conversion failed: {err}"))
}

fn json_values_to_native(values: Vec<JsonValue>) -> Vec<ZqValue> {
    values.into_iter().map(ZqValue::from_json).collect()
}

fn native_values_to_json(values: Vec<ZqValue>) -> Vec<JsonValue> {
    values.into_iter().map(ZqValue::into_json).collect()
}

#[cfg(test)]
fn native_values_to_json_slice(values: &[ZqValue]) -> Vec<JsonValue> {
    values.iter().cloned().map(ZqValue::into_json).collect()
}

fn yaml_native_docs_compatible_with_json_preference(values: &[ZqValue]) -> bool {
    values.len() > 1 || values.iter().any(ZqValue::is_array_or_object)
}

fn parse_json_value_stream_native(input: &str) -> Result<Vec<ZqValue>, serde_json::Error> {
    match parse_json_value_stream_strict_native(input) {
        Ok(values) => Ok(values),
        Err(strict_err) => parse_json_value_stream_from_strict_failure(input, strict_err),
    }
}

fn parse_json_value_stream_from_strict_failure(
    input: &str,
    strict_err: serde_json::Error,
) -> Result<Vec<ZqValue>, serde_json::Error> {
    let normalized = normalize_legacy_number_tokens(input);
    if let Cow::Owned(norm) = &normalized {
        if let Ok(values) = parse_json_value_stream_strict_native(norm) {
            return Ok(values);
        }
    }
    // jq accepts legacy NaN payload tokens on input (e.g. Nan4000)
    // and non-finite markers; map them to JSON null for the native
    // engine path (same internal representation as existing non-finite
    // jsonish handling).
    let canonical = canonicalize_jsonish_tokens(normalized.as_ref());
    let json_compatible = replace_non_finite_number_tokens(&canonical);
    if json_compatible != normalized.as_ref() {
        if let Ok(values) = parse_json_value_stream_strict_native(&json_compatible) {
            return Ok(values);
        }
    }
    Err(strict_err)
}

fn parse_json_value_stream_strict_native(input: &str) -> Result<Vec<ZqValue>, serde_json::Error> {
    let mut recycle_ctx = NativeValueRecycleContext::default();
    let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
    let mut out = Vec::new();
    for next in serde_json::Deserializer::from_str(input).into_iter::<ZqValue>() {
        out.push(next?);
    }
    Ok(out)
}

fn strip_jq_comments(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;
    let mut trailing_backslashes = 0usize;

    for ch in query.chars() {
        if in_comment {
            match ch {
                '\n' => {
                    let continues = trailing_backslashes % 2 == 1;
                    trailing_backslashes = 0;
                    if continues {
                        continue;
                    }
                    in_comment = false;
                    out.push('\n');
                }
                '\r' => {}
                '\\' => trailing_backslashes += 1,
                _ => trailing_backslashes = 0,
            }
            continue;
        }

        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }

        if ch == '#' {
            in_comment = true;
            trailing_backslashes = 0;
            continue;
        }

        out.push(ch);
    }

    out
}

#[cfg(test)]
#[path = "query_native/test_support.rs"]
mod test_support;
