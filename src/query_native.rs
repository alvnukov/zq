#[cfg(test)]
use crate::c_compat::{math as c_math, time as c_time};
use crate::value::{ValueError as NativeValueError, ZqValue};
#[cfg(test)]
use base64::Engine as _;
#[cfg(test)]
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{Arc, Mutex};

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
    match crate::native_engine::try_execute_native_with_paths(
        query,
        &input_stream,
        library_paths,
        crate::native_engine::RunOptions {
            null_input: run_options.null_input,
        },
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
        out.into_iter()
            .map(|v| stringify_jsonish_value_native(&v))
            .collect::<Result<Vec<_>, _>>()
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
    Ok(PreparedQuery {
        query: query.to_string(),
        library_paths: library_paths.to_vec(),
    })
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
    Ok(ParsedInput {
        kind: parsed.kind,
        values: native_values_to_json(parsed.values),
    })
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
            .map(|values| ParsedNativeInput {
                kind: InputKind::JsonStream,
                values,
            })
            .map_err(Error::Json),
        InputFormat::Yaml => {
            parse_yaml_native_docs_with_merge(input).map(|values| ParsedNativeInput {
                kind: InputKind::YamlDocs,
                values,
            })
        }
        InputFormat::Toml => parse_toml_native_doc(input).map(|value| ParsedNativeInput {
            kind: InputKind::JsonStream,
            values: vec![value],
        }),
        // Forced CSV must accept valid one-column CSV as well; strict delimiter
        // probing is reserved for auto-detection to avoid false positives.
        InputFormat::Csv => parse_csv_native_rows(input, false).map(|values| ParsedNativeInput {
            kind: InputKind::JsonStream,
            values,
        }),
        InputFormat::Xml => parse_xml_native_doc(input).map(|value| ParsedNativeInput {
            kind: InputKind::JsonStream,
            values: vec![value],
        }),
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
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str::<ZqValue>(input) {
            Ok(v) => Ok(vec![v.into_json()]),
            Err(_) => Err(Error::Yaml(yaml_err)),
        },
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
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str::<ZqValue>(input) {
            Ok(v) => Ok(v),
            Err(_) => Err(Error::Yaml(yaml_err)),
        },
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
        Ok(values) => Ok(ParsedNativeInput {
            kind: InputKind::JsonStream,
            values,
        }),
        Err(strict_json_err) => {
            match parse_yaml_native_docs_with_merge(input) {
                Ok(values) => {
                    if yaml_native_docs_compatible_with_json_preference(&values) {
                        return Ok(ParsedNativeInput {
                            kind: InputKind::YamlDocs,
                            values,
                        });
                    }
                }
                Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => {}
                Err(e) => return Err(e),
            }

            match parse_json_value_stream_from_strict_failure(input, strict_json_err) {
                Ok(values) => Ok(ParsedNativeInput {
                    kind: InputKind::JsonStream,
                    values,
                }),
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
                        return Ok(ParsedNativeInput {
                            kind: InputKind::JsonStream,
                            values,
                        });
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

fn parse_xml_native_doc(input: &str) -> Result<ZqValue, Error> {
    let document =
        roxmltree::Document::parse(input).map_err(|e| Error::Runtime(format!("xml: {e}")))?;
    let root = document.root_element();
    let mut out = serde_json::Map::new();
    out.insert(
        root.tag_name().name().to_string(),
        xml_element_to_json_value(root),
    );
    Ok(ZqValue::from_json(JsonValue::Object(out)))
}

fn xml_element_to_json_value(node: roxmltree::Node<'_, '_>) -> JsonValue {
    let mut object = serde_json::Map::new();

    for attr in node.attributes() {
        object.insert(
            format!("@{}", attr.name()),
            JsonValue::String(attr.value().to_string()),
        );
    }

    for child in node.children().filter(|child| child.is_element()) {
        let key = child.tag_name().name().to_string();
        let child_value = xml_element_to_json_value(child);
        if let Some(existing) = object.get_mut(&key) {
            if let JsonValue::Array(items) = existing {
                items.push(child_value);
            } else {
                let previous = std::mem::replace(existing, JsonValue::Null);
                *existing = JsonValue::Array(vec![previous, child_value]);
            }
        } else {
            object.insert(key, child_value);
        }
    }

    if let Some(text) = collect_xml_text_content(node) {
        if object.is_empty() {
            return JsonValue::String(text);
        }
        object.insert("#text".to_string(), JsonValue::String(text));
    }

    if object.is_empty() {
        JsonValue::String(String::new())
    } else {
        JsonValue::Object(object)
    }
}

fn collect_xml_text_content(node: roxmltree::Node<'_, '_>) -> Option<String> {
    let parts = node
        .children()
        .filter(|child| child.is_text())
        .filter_map(|child| child.text())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

fn parse_csv_native_rows(
    input: &str,
    require_delimited_shape: bool,
) -> Result<Vec<ZqValue>, Error> {
    let delimiter = if require_delimited_shape {
        detect_csv_delimiter(input, true)
            .ok_or_else(|| Error::Runtime("csv: cannot detect delimiter".to_string()))?
    } else {
        detect_csv_delimiter(input, false).unwrap_or(b',')
    };
    parse_csv_native_rows_with_delimiter(input, delimiter)
}

fn parse_csv_native_rows_auto(input: &str) -> Option<Vec<ZqValue>> {
    parse_csv_native_rows(input, true).ok()
}

fn parse_csv_native_rows_with_delimiter(input: &str, delimiter: u8) -> Result<Vec<ZqValue>, Error> {
    let mut probe = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .from_reader(input.as_bytes());
    let mut sample = Vec::with_capacity(2);
    for next in probe.records().take(2) {
        sample.push(next.map_err(|e| Error::Runtime(format!("csv: {e}")))?);
    }
    let has_headers = csv_rows_look_like_header(&sample);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .delimiter(delimiter)
        .from_reader(input.as_bytes());
    let headers = if has_headers {
        Some(
            reader
                .headers()
                .map_err(|e| Error::Runtime(format!("csv: {e}")))?
                .clone(),
        )
    } else {
        None
    };

    let mut out = Vec::new();
    for next in reader.records() {
        let record = next.map_err(|e| Error::Runtime(format!("csv: {e}")))?;
        if let Some(headers) = headers.as_ref() {
            let mut obj = serde_json::Map::with_capacity(headers.len());
            for (idx, key) in headers.iter().enumerate() {
                let value = record.get(idx).unwrap_or_default();
                obj.insert(key.to_string(), JsonValue::String(value.to_string()));
            }
            out.push(ZqValue::from_json(JsonValue::Object(obj)));
        } else {
            let arr = record
                .iter()
                .map(|value| JsonValue::String(value.to_string()))
                .collect::<Vec<_>>();
            out.push(ZqValue::from_json(JsonValue::Array(arr)));
        }
    }
    Ok(out)
}

fn csv_rows_look_like_header(rows: &[csv::StringRecord]) -> bool {
    if rows.len() < 2 {
        return false;
    }
    let header = &rows[0];
    let first_data = &rows[1];
    if header.is_empty() || header.len() != first_data.len() {
        return false;
    }
    let mut seen = std::collections::HashSet::with_capacity(header.len());
    for key in header.iter() {
        let trimmed = key.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            return false;
        }
    }
    header.iter().zip(first_data.iter()).any(|(h, v)| h != v)
}

fn detect_csv_delimiter(input: &str, require_multiple_lines: bool) -> Option<u8> {
    let lines = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(8)
        .collect::<Vec<_>>();
    if lines.is_empty() || (require_multiple_lines && lines.len() < 2) {
        return None;
    }

    let candidates = [b',', b';', b'\t'];
    let mut best: Option<(u8, usize)> = None;
    for delimiter in candidates {
        let split_count = |line: &str| line.split(delimiter as char).count();
        let counts = lines
            .iter()
            .map(|line| split_count(line))
            .collect::<Vec<_>>();
        let max_fields = counts.iter().copied().max().unwrap_or(1);
        if max_fields < 2 {
            continue;
        }
        let matching_lines = counts.iter().filter(|&&count| count == max_fields).count();
        if require_multiple_lines && matching_lines < 2 {
            continue;
        }
        if !require_multiple_lines && matching_lines < 1 {
            continue;
        }
        let score = max_fields * matching_lines;
        if best
            .map(|(_, best_score)| score > best_score)
            .unwrap_or(true)
        {
            best = Some((delimiter, score));
        }
    }
    best.map(|(delimiter, _)| delimiter)
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
    let mut out = Vec::new();
    for next in serde_json::Deserializer::from_str(input).into_iter::<serde_json::Value>() {
        out.push(ZqValue::from_json(next?));
    }
    Ok(out)
}

fn normalize_legacy_number_tokens(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut out: Option<String> = None;
    let mut copied_until = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if b == b'"' {
            in_string = true;
            i += 1;
            continue;
        }

        if b == b'-' || b.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < bytes.len() {
                let nb = bytes[i];
                if nb.is_ascii_digit() || matches!(nb, b'.' | b'e' | b'E' | b'+' | b'-') {
                    i += 1;
                } else {
                    break;
                }
            }

            let token = &input[start..i];
            let normalized = normalize_legacy_number_token(token);
            if normalized != token {
                if out.is_none() {
                    out = Some(String::with_capacity(input.len()));
                }
                let dst = out.as_mut().expect("output allocated");
                dst.push_str(&input[copied_until..start]);
                dst.push_str(&normalized);
                copied_until = i;
            }
            continue;
        }

        i += 1;
    }

    if let Some(mut out) = out {
        out.push_str(&input[copied_until..]);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(input)
    }
}

fn normalize_legacy_number_token(token: &str) -> String {
    let (sign, rest) = if let Some(r) = token.strip_prefix('-') {
        ("-", r)
    } else {
        ("", token)
    };

    let int_end = rest.find(['.', 'e', 'E']).unwrap_or(rest.len());
    let int_part = &rest[..int_end];
    let tail = &rest[int_end..];
    if int_part.len() <= 1
        || !int_part.starts_with('0')
        || !int_part.chars().all(|c| c.is_ascii_digit())
    {
        return token.to_string();
    }
    let stripped = int_part.trim_start_matches('0');
    let normalized_int = if stripped.is_empty() { "0" } else { stripped };
    format!("{sign}{normalized_int}{tail}")
}

#[cfg(test)]
fn parse_jsonish_value(input: &str) -> Result<JsonValue, Error> {
    parse_jsonish(input)
}

fn parse_jsonish_value_native(input: &str) -> Result<ZqValue, Error> {
    parse_jsonish(input)
}

fn parse_jsonish<T: DeserializeOwned>(input: &str) -> Result<T, Error> {
    let canonical = canonicalize_jsonish_tokens(input);
    let json_compatible = replace_non_finite_number_tokens(&canonical);
    serde_json::from_str::<T>(&json_compatible).map_err(Error::Json)
}

#[cfg(test)]
#[allow(dead_code)]
fn stringify_jsonish_value(value: &JsonValue) -> Result<String, Error> {
    serde_json::to_string(value).map_err(Error::Json)
}

fn stringify_jsonish_value_native(value: &ZqValue) -> Result<String, Error> {
    serde_json::to_string(value).map_err(Error::Json)
}

fn canonicalize_jsonish_tokens(input: &str) -> String {
    fn is_token_boundary(ch: Option<char>) -> bool {
        match ch {
            None => true,
            Some(c) => !(c.is_ascii_alphanumeric() || c == '_' || c == '.'),
        }
    }
    fn starts_with_ci(rest: &[char], pat: &str) -> bool {
        if rest.len() < pat.len() {
            return false;
        }
        rest.iter()
            .zip(pat.chars())
            .all(|(l, r)| l.eq_ignore_ascii_case(&r))
    }
    fn match_special(rest: &[char]) -> Option<(&'static str, usize)> {
        if starts_with_ci(rest, "nan") {
            let mut len = 3usize;
            while len < rest.len() && rest[len].is_ascii_digit() {
                len += 1;
            }
            return Some(("NaN", len));
        }
        if starts_with_ci(rest, "infinity") {
            return Some(("Infinity", 8));
        }
        if starts_with_ci(rest, "infinite") {
            return Some(("Infinity", 8));
        }
        None
    }

    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while i < chars.len() {
        let c = chars[i];

        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if c == '"' {
            in_string = true;
            out.push(c);
            i += 1;
            continue;
        }

        let rest = &chars[i..];
        let prev = i.checked_sub(1).and_then(|p| chars.get(p)).copied();
        if is_token_boundary(prev) {
            if let Some(sign @ ('+' | '-')) = rest.first().copied() {
                if let Some((canon, len)) = match_special(&rest[1..]) {
                    let next = chars.get(i + 1 + len).copied();
                    if is_token_boundary(next) {
                        if canon == "Infinity" && sign == '-' {
                            out.push_str("-Infinity");
                        } else {
                            out.push_str(canon);
                        }
                        i += 1 + len;
                        continue;
                    }
                }
            } else if let Some((canon, len)) = match_special(rest) {
                let next = chars.get(i + len).copied();
                if is_token_boundary(next) {
                    out.push_str(canon);
                    i += len;
                    continue;
                }
            }
        }

        out.push(c);
        i += 1;
    }
    out
}

fn replace_non_finite_number_tokens(input: &str) -> String {
    fn is_token_boundary(ch: Option<char>) -> bool {
        match ch {
            None => true,
            Some(c) => !(c.is_ascii_alphanumeric() || c == '_' || c == '.'),
        }
    }

    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if c == '"' {
            in_string = true;
            out.push(c);
            i += 1;
            continue;
        }

        let prev = i.checked_sub(1).and_then(|p| chars.get(p)).copied();
        if is_token_boundary(prev) {
            let rest: String = chars[i..].iter().collect();
            if rest.starts_with("-Infinity")
                && is_token_boundary(chars.get(i + "-Infinity".len()).copied())
            {
                out.push_str("null");
                i += "-Infinity".len();
                continue;
            }
            if rest.starts_with("Infinity")
                && is_token_boundary(chars.get(i + "Infinity".len()).copied())
            {
                out.push_str("null");
                i += "Infinity".len();
                continue;
            }
            if rest.starts_with("NaN") && is_token_boundary(chars.get(i + "NaN".len()).copied()) {
                out.push_str("null");
                i += "NaN".len();
                continue;
            }
        }

        out.push(c);
        i += 1;
    }
    out
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
