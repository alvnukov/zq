use serde_json::Value as JsonValue;
use std::borrow::Cow;

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

#[derive(Debug, Clone)]
pub struct ParsedInput {
    pub kind: InputKind,
    pub values: Vec<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOptions {
    pub null_input: bool,
}

#[allow(dead_code)]
pub fn run_json_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    let input_value: JsonValue = match serde_json::from_str(input) {
        Ok(v) => v,
        Err(json_err) => match parse_yaml_json_with_merge(input) {
            Ok(v) => {
                if !yaml_value_fallback_is_compatible(&v) {
                    return Err(Error::Json(json_err));
                }
                v
            }
            Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => return Err(Error::Json(json_err)),
            Err(e) => return Err(e),
        },
    };
    run_query_stream(query, vec![input_value])
}

#[allow(dead_code)]
pub fn run_yaml_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    let as_json: JsonValue = match parse_yaml_json_with_merge(input) {
        Ok(v) => v,
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str(input) {
            Ok(v) => v,
            Err(_) => return Err(Error::Yaml(yaml_err)),
        },
        Err(e) => return Err(e),
    };
    run_query_stream(query, vec![as_json])
}

pub fn run_query_stream(query: &str, input_stream: Vec<JsonValue>) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, &[], RunOptions::default())
}

pub fn run_query_stream_with_paths(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, library_paths, RunOptions::default())
}

pub fn run_query_stream_with_paths_and_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    let _ = library_paths;
    if let Some(result) = execute_special_query(query, &input_stream, run_options)? {
        return Ok(result);
    }
    match crate::native_engine::try_execute(
        query,
        &input_stream,
        crate::native_engine::RunOptions {
            null_input: run_options.null_input,
        },
    ) {
        crate::native_engine::TryExecute::Executed(Ok(values)) => Ok(values),
        crate::native_engine::TryExecute::Executed(Err(e)) => Err(Error::Runtime(e)),
        crate::native_engine::TryExecute::Unsupported => {
            Err(Error::Unsupported(format!("query is not supported by native engine: {query}")))
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
        let input = parse_jsonish_value(input_jsonish)?;
        let out = run_query_stream_with_paths_and_options(
            &self.query,
            vec![input],
            &self.library_paths,
            RunOptions::default(),
        )?;
        out.iter()
            .map(stringify_jsonish_value)
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn run_jsonish_lenient(&self, input_jsonish: &str) -> Result<Vec<String>, Error> {
        self.run_jsonish(input_jsonish)
    }
}

pub fn prepare_query_with_paths(query: &str, library_paths: &[String]) -> Result<PreparedQuery, Error> {
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
    let _ = library_paths;
    if is_special_supported(query) || crate::native_engine::is_supported(query) {
        Ok(())
    } else {
        Err(Error::Unsupported(format!(
            "query is not supported by native engine: {query}"
        )))
    }
}

pub fn normalize_jsonish_line(line: &str) -> Result<String, Error> {
    let value = parse_jsonish_value(line)?;
    stringify_jsonish_value(&value)
}

pub fn jsonish_equal(left: &str, right: &str) -> Result<bool, Error> {
    let left = parse_jsonish_value(left)?;
    let right = parse_jsonish_value(right)?;
    Ok(left == right)
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_json(input: &str) -> Result<Vec<JsonValue>, Error> {
    match parse_json_value_stream(input) {
        Ok(v) => Ok(v),
        Err(json_err) => match parse_yaml_json_docs_with_merge(input) {
            Ok(v) => {
                if !yaml_docs_fallback_is_compatible(&v) {
                    return Err(Error::Json(json_err));
                }
                Ok(v)
            }
            Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

pub fn parse_input_values_auto(input: &str) -> Result<ParsedInput, Error> {
    match parse_json_value_stream(input) {
        Ok(values) => Ok(ParsedInput {
            kind: InputKind::JsonStream,
            values,
        }),
        Err(json_err) => match parse_yaml_json_docs_with_merge(input) {
            Ok(values) => {
                if !yaml_docs_fallback_is_compatible(&values) {
                    return Err(Error::Json(json_err));
                }
                Ok(ParsedInput {
                    kind: InputKind::YamlDocs,
                    values,
                })
            }
            Err(Error::Yaml(_)) | Err(Error::Unsupported(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_yaml(input: &str) -> Result<Vec<JsonValue>, Error> {
    match parse_yaml_json_docs_with_merge(input) {
        Ok(v) => Ok(v),
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str::<JsonValue>(input) {
            Ok(v) => Ok(vec![v]),
            Err(_) => Err(Error::Yaml(yaml_err)),
        },
        Err(e) => Err(e),
    }
}

fn parse_yaml_json_with_merge(input: &str) -> Result<JsonValue, Error> {
    let raw: serde_yaml::Value = serde_yaml::from_str(input).map_err(Error::Yaml)?;
    let normalized = crate::yamlmerge::normalize_value_from_source(input, raw);
    serde_json::to_value(normalized)
        .map_err(|e| Error::Unsupported(format!("yaml to json conversion failed: {e}")))
}

fn parse_yaml_json_docs_with_merge(input: &str) -> Result<Vec<JsonValue>, Error> {
    let docs = crate::yamlmerge::normalize_documents(input).map_err(Error::Yaml)?;
    docs.into_iter()
        .map(|v| {
            serde_json::to_value(v)
                .map_err(|e| Error::Unsupported(format!("yaml to json conversion failed: {e}")))
        })
        .collect()
}

fn yaml_value_fallback_is_compatible(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Array(_) | JsonValue::Object(_))
}

fn yaml_docs_fallback_is_compatible(values: &[JsonValue]) -> bool {
    values.len() > 1 || values.iter().any(yaml_value_fallback_is_compatible)
}

fn parse_json_value_stream(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    match parse_json_value_stream_strict(input) {
        Ok(values) => Ok(values),
        Err(strict_err) => {
            let normalized = normalize_legacy_number_tokens(input);
            if let Cow::Owned(norm) = &normalized {
                if let Ok(values) = parse_json_value_stream_strict(norm) {
                    return Ok(values);
                }
            }
            Err(strict_err)
        }
    }
}

fn parse_json_value_stream_strict(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    let mut stream = serde_json::Deserializer::from_str(input).into_iter::<JsonValue>();
    let mut out = Vec::new();
    while let Some(next) = stream.next() {
        out.push(next?);
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

    let int_end = rest
        .find(|c: char| c == '.' || c == 'e' || c == 'E')
        .unwrap_or(rest.len());
    let int_part = &rest[..int_end];
    let tail = &rest[int_end..];
    if int_part.len() <= 1 || !int_part.starts_with('0') || !int_part.chars().all(|c| c.is_ascii_digit()) {
        return token.to_string();
    }
    let stripped = int_part.trim_start_matches('0');
    let normalized_int = if stripped.is_empty() { "0" } else { stripped };
    format!("{sign}{normalized_int}{tail}")
}

fn parse_jsonish_value(input: &str) -> Result<JsonValue, Error> {
    let canonical = canonicalize_jsonish_tokens(input);
    serde_json::from_str::<JsonValue>(&canonical).map_err(Error::Json)
}

fn stringify_jsonish_value(value: &JsonValue) -> Result<String, Error> {
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

fn execute_special_query(
    query: &str,
    input_stream: &[JsonValue],
    run_options: RunOptions,
) -> Result<Option<Vec<JsonValue>>, Error> {
    let q = query.trim();
    if q == "empty" {
        return Ok(Some(Vec::new()));
    }

    if let Some(sum) = parse_simple_addition(q) {
        return Ok(Some(vec![sum]));
    }

    if let Some(rhs) = parse_eq_rhs(q, ".") {
        let stream = if run_options.null_input {
            vec![JsonValue::Null]
        } else {
            input_stream.to_vec()
        };
        let out = stream
            .into_iter()
            .map(|v| JsonValue::Bool(v == rhs))
            .collect::<Vec<_>>();
        return Ok(Some(out));
    }

    if let Some(rhs) = parse_eq_rhs(q, "[inputs]") {
        let rhs_array = match rhs {
            JsonValue::Array(arr) => JsonValue::Array(arr),
            _ => {
                return Err(Error::Unsupported(
                    "right side of [inputs] == must be an array literal".to_string(),
                ));
            }
        };
        let lhs = JsonValue::Array(input_stream.to_vec());
        return Ok(Some(vec![JsonValue::Bool(lhs == rhs_array)]));
    }

    Ok(None)
}

fn is_special_supported(query: &str) -> bool {
    let q = query.trim();
    q == "empty"
        || parse_simple_addition(q).is_some()
        || parse_eq_rhs(q, ".").is_some()
        || parse_eq_rhs(q, "[inputs]").is_some()
}

fn parse_eq_rhs(query: &str, expected_lhs: &str) -> Option<JsonValue> {
    let (lhs, rhs) = query.split_once("==")?;
    if lhs.trim() != expected_lhs {
        return None;
    }
    parse_jsonish_value(rhs.trim()).ok()
}

fn parse_simple_addition(query: &str) -> Option<JsonValue> {
    let (lhs, rhs) = query.split_once('+')?;
    let lhs = parse_jsonish_value(lhs.trim()).ok()?;
    let rhs = parse_jsonish_value(rhs.trim()).ok()?;
    let (Some(a), Some(b)) = (lhs.as_i64(), rhs.as_i64()) else {
        return None;
    };
    Some(JsonValue::from(a + b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_supported_and_unsupported_queries() {
        assert!(validate_query(".a | .b").is_ok());
        assert!(matches!(validate_query("map(.)"), Err(Error::Unsupported(_))));
    }

    #[test]
    fn run_query_stream_uses_native_engine_only() {
        let out = run_query_stream(".a", vec![serde_json::json!({"a": 1})]).expect("run");
        assert_eq!(out, vec![serde_json::json!(1)]);

        let unsupported = run_query_stream("map(.)", vec![serde_json::json!([1, 2, 3])]);
        assert!(matches!(unsupported, Err(Error::Unsupported(_))));
    }

    #[test]
    fn parse_input_values_auto_detects_json_stream() {
        let parsed = parse_input_values_auto("{\"a\":1}\n{\"a\":2}\n").expect("parse");
        assert_eq!(parsed.kind, InputKind::JsonStream);
        assert_eq!(parsed.values.len(), 2);
    }

    #[test]
    fn parse_input_values_auto_detects_yaml_docs() {
        let parsed = parse_input_values_auto("a: 1\n---\na: 2\n").expect("parse");
        assert_eq!(parsed.kind, InputKind::YamlDocs);
        assert_eq!(parsed.values.len(), 2);
    }

    #[test]
    fn parse_input_fallback_keeps_json_error_for_non_string_yaml_key() {
        assert!(matches!(
            parse_input_values_auto("{{\"a\":\"b\"}}"),
            Err(Error::Json(_))
        ));
        assert!(matches!(
            parse_input_docs_prefer_json("{{\"a\":\"b\"}}"),
            Err(Error::Json(_))
        ));
    }

    #[test]
    fn normalize_legacy_numbers_in_json_stream() {
        let docs = parse_input_docs_prefer_json("[0,01]\n").expect("parse");
        assert_eq!(docs, vec![serde_json::json!([0, 1])]);
    }

    #[test]
    fn normalize_jsonish_line_roundtrips_json() {
        let normalized = normalize_jsonish_line("{\"a\":1,\"b\":[2,3]}").expect("normalize");
        assert_eq!(normalized, "{\"a\":1,\"b\":[2,3]}");
    }

    #[test]
    fn jsonish_equal_compares_semantics() {
        assert!(jsonish_equal("{\"a\":1}", "{\"a\":1}").expect("compare"));
        assert!(!jsonish_equal("{\"a\":1}", "{\"a\":2}").expect("compare"));
    }

    #[test]
    fn special_empty_query_returns_no_results() {
        let out = run_query_stream_with_paths_and_options(
            "empty",
            vec![serde_json::json!(1)],
            &[],
            RunOptions::default(),
        )
        .expect("run");
        assert!(out.is_empty());
    }

    #[test]
    fn special_addition_query_is_supported() {
        let out = run_query_stream_with_paths_and_options(
            "1+1",
            vec![],
            &[],
            RunOptions { null_input: true },
        )
        .expect("run");
        assert_eq!(out, vec![serde_json::json!(2)]);
    }

    #[test]
    fn special_dot_equality_query_compares_input() {
        let out = run_query_stream_with_paths_and_options(
            r#". == "a\nb\nc\n""#,
            vec![serde_json::json!("a\nb\nc\n")],
            &[],
            RunOptions::default(),
        )
        .expect("run");
        assert_eq!(out, vec![serde_json::json!(true)]);
    }

    #[test]
    fn special_inputs_equality_query_compares_stream() {
        let out = run_query_stream_with_paths_and_options(
            r#"[inputs] == ["a","b","c"]"#,
            vec![
                serde_json::json!("a"),
                serde_json::json!("b"),
                serde_json::json!("c"),
            ],
            &[],
            RunOptions { null_input: true },
        )
        .expect("run");
        assert_eq!(out, vec![serde_json::json!(true)]);
    }
}
