use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocMode {
    First,
    All,
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub doc_mode: DocMode,
    pub library_path: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOptions {
    pub null_input: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            doc_mode: DocMode::First,
            library_path: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Query(#[from] crate::QueryError),
    #[error("--doc-index is required when --doc-mode=index")]
    MissingDocIndex,
    #[error("invalid --doc-mode '{0}' (expected first|all|index)")]
    InvalidDocMode(String),
    #[error("{tool}: --doc-index={index} is out of range for {total} document(s)")]
    DocIndexOutOfRange {
        tool: &'static str,
        index: usize,
        total: usize,
    },
    #[error("encode json: {0}")]
    OutputEncode(String),
    #[error("encode yaml: {0}")]
    OutputYamlEncode(String),
}

pub fn parse_doc_mode(doc_mode: &str, doc_index: Option<usize>) -> Result<DocMode, Error> {
    match doc_mode.trim().to_ascii_lowercase().as_str() {
        "" | "first" => Ok(DocMode::First),
        "all" => Ok(DocMode::All),
        "index" => match doc_index {
            Some(i) => Ok(DocMode::Index(i)),
            None => Err(Error::MissingDocIndex),
        },
        other => Err(Error::InvalidDocMode(other.to_string())),
    }
}

pub fn run_jq(query: &str, input: &str, options: QueryOptions) -> Result<Vec<JsonValue>, Error> {
    let stream = parse_jq_input_values(input, options.doc_mode, "jq")?;
    Ok(crate::query::run_query_stream_with_paths(
        query,
        stream,
        &options.library_path,
    )?)
}

pub fn run_jq_stream_with_paths_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_path: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    Ok(crate::query::run_query_stream_with_paths_and_options(
        query,
        input_stream,
        library_path,
        crate::query::RunOptions {
            null_input: run_options.null_input,
        },
    )?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeStreamStatus {
    Unsupported,
    Executed,
}

pub fn try_run_jq_native_stream_with_paths_options<F>(
    query: &str,
    input_stream: &[JsonValue],
    run_options: RunOptions,
    mut emit: F,
) -> Result<NativeStreamStatus, Error>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    match crate::native_engine::try_execute_stream(
        query,
        input_stream,
        crate::native_engine::RunOptions {
            null_input: run_options.null_input,
        },
        |value| emit(value),
    ) {
        crate::native_engine::TryExecuteStream::Unsupported => Ok(NativeStreamStatus::Unsupported),
        crate::native_engine::TryExecuteStream::Executed(Ok(())) => Ok(NativeStreamStatus::Executed),
        crate::native_engine::TryExecuteStream::Executed(Err(err)) => {
            Err(Error::Query(crate::QueryError::Runtime(err)))
        }
    }
}

pub fn parse_jq_input_values(
    input: &str,
    doc_mode: DocMode,
    tool: &'static str,
) -> Result<Vec<JsonValue>, Error> {
    let parsed = crate::query::parse_input_values_auto(input)?;
    match parsed.kind {
        crate::query::InputKind::JsonStream => Ok(parsed.values),
        crate::query::InputKind::YamlDocs => select_docs(parsed.values, doc_mode, tool),
    }
}

pub fn validate_jq_query(query: &str) -> Result<(), Error> {
    crate::query::validate_query(query).map_err(Error::Query)
}

pub fn validate_jq_query_with_paths(query: &str, library_path: &[String]) -> Result<(), Error> {
    crate::query::validate_query_with_paths(query, library_path).map_err(Error::Query)
}

pub struct PreparedJq {
    inner: crate::query::PreparedQuery,
}

impl PreparedJq {
    pub fn run_jsonish_lines(&self, input: &str) -> Result<Vec<String>, Error> {
        self.inner.run_jsonish(input).map_err(Error::Query)
    }

    pub fn run_jsonish_lines_lenient(&self, input: &str) -> Result<Vec<String>, Error> {
        self.inner.run_jsonish_lenient(input).map_err(Error::Query)
    }
}

pub fn prepare_jq_query_with_paths(query: &str, library_path: &[String]) -> Result<PreparedJq, Error> {
    crate::query::prepare_query_with_paths(query, library_path)
        .map(|inner| PreparedJq { inner })
        .map_err(Error::Query)
}

pub fn run_jq_jsonish_lines(
    query: &str,
    input: &str,
    library_path: &[String],
) -> Result<Vec<String>, Error> {
    crate::query::run_query_stream_jsonish(query, input, library_path).map_err(Error::Query)
}

pub fn normalize_jsonish_line(line: &str) -> Result<String, Error> {
    crate::query::normalize_jsonish_line(line).map_err(Error::Query)
}

pub fn jsonish_equal(left: &str, right: &str) -> Result<bool, Error> {
    crate::query::jsonish_equal(left, right).map_err(Error::Query)
}

pub fn format_output_json_lines(
    values: &[JsonValue],
    compact: bool,
    raw_output: bool,
) -> Result<String, Error> {
    let mut lines = Vec::with_capacity(values.len());
    for v in values {
        if raw_output {
            if let Some(s) = v.as_str() {
                lines.push(s.to_string());
                continue;
            }
        }
        if compact {
            let line = serde_json::to_string(v).map_err(|e| Error::OutputEncode(e.to_string()))?;
            lines.push(jq_style_escape_del(&line));
        } else {
            let line =
                serde_json::to_string_pretty(v).map_err(|e| Error::OutputEncode(e.to_string()))?;
            lines.push(jq_style_escape_del(&line));
        }
    }
    Ok(lines.join("\n"))
}

fn jq_style_escape_del(line: &str) -> String {
    if line.bytes().any(|b| b == 0x7f) {
        line.replace('\u{007f}', "\\u007f")
    } else {
        line.to_string()
    }
}

pub fn format_output_yaml_documents(values: &[JsonValue]) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for v in values {
        let yv = json_to_yaml_value(v)?;
        docs.push(serde_yaml::to_string(&yv).map_err(|e| Error::OutputYamlEncode(e.to_string()))?);
    }
    if docs.is_empty() {
        return Ok(String::new());
    }
    if docs.len() == 1 {
        return Ok(docs.remove(0).trim_end().to_string());
    }
    let joined = docs
        .into_iter()
        .map(|d| d.trim_end().to_string())
        .collect::<Vec<_>>()
        .join("\n---\n");
    Ok(joined)
}

fn json_to_yaml_value(v: &JsonValue) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Mapping, Number, Value as YamlValue};
    match v {
        JsonValue::Null => Ok(YamlValue::Null),
        JsonValue::Bool(b) => Ok(YamlValue::Bool(*b)),
        JsonValue::Number(n) => {
            let token = n.to_string();
            if let Ok(i) = token.parse::<i64>() {
                return Ok(YamlValue::Number(Number::from(i)));
            }
            if let Ok(u) = token.parse::<u64>() {
                return Ok(YamlValue::Number(Number::from(u)));
            }
            if let Ok(f) = token.parse::<f64>() {
                if let Ok(yv) = serde_yaml::to_value(f) {
                    return Ok(yv);
                }
            }
            serde_yaml::from_str::<YamlValue>(&token)
                .map_err(|e| Error::OutputYamlEncode(e.to_string()))
        }
        JsonValue::String(s) => Ok(YamlValue::String(s.clone())),
        JsonValue::Array(arr) => {
            let mut seq = Vec::with_capacity(arr.len());
            for item in arr {
                seq.push(json_to_yaml_value(item)?);
            }
            Ok(YamlValue::Sequence(seq))
        }
        JsonValue::Object(obj) => {
            let mut map = Mapping::new();
            for (k, val) in obj {
                map.insert(YamlValue::String(k.clone()), json_to_yaml_value(val)?);
            }
            Ok(YamlValue::Mapping(map))
        }
    }
}

pub fn format_query_error(tool: &str, input: &str, err: &crate::QueryError) -> String {
    if let crate::QueryError::Json(json_err) = err {
        return format_json_parse_error(tool, input, json_err);
    }
    if let crate::QueryError::Runtime(msg) = err {
        return format!("{tool}: error (at <stdin>:1): {msg}");
    }
    if let crate::QueryError::Unsupported(msg) = err {
        if msg.starts_with("Top-level program not given (try \".\")") {
            return format!(
                "{tool}: error: Top-level program not given (try \".\")\n{tool}: 1 compile error"
            );
        }
        if msg.starts_with("too many function parameters or local function definitions (max 4095)") {
            return format!(
                "{tool}: error: too many function parameters or local function definitions (max 4095)\n{tool}: 1 compile error"
            );
        }
    }

    let base = format!("{tool}: {err}");
    let Some((line, col)) = extract_line_col(&base) else {
        return base;
    };
    let ctx = render_input_context(input, line, col);
    if ctx.is_empty() {
        base
    } else {
        format!("{base}\n{ctx}")
    }
}

fn format_json_parse_error(tool: &str, input: &str, err: &serde_json::Error) -> String {
    let raw = err.to_string();
    let mut col = err.column();
    let message = if raw.starts_with("control character (\\u0000-\\u001F) found while parsing a string") {
        // jq reports this one column later than serde_json.
        col = col.saturating_add(1);
        "Invalid string: control characters from U+0000 through U+001F must be escaped".to_string()
    } else if raw.starts_with("key must be a string") {
        format_object_key_parse_error(input, err).unwrap_or_else(|| "key must be a string".to_string())
    } else if raw.starts_with("expected `:`") {
        "Objects must consist of key:value pairs".to_string()
    } else if raw.starts_with("EOF while parsing a string") {
        "Unfinished string at EOF".to_string()
    } else if raw.starts_with("EOF while parsing") {
        "Unfinished JSON term at EOF".to_string()
    } else {
        strip_serde_line_col_suffix(&raw).to_string()
    };

    format!(
        "{tool}: parse error: {message} at line {}, column {col}",
        err.line()
    )
}

fn format_object_key_parse_error(input: &str, err: &serde_json::Error) -> Option<String> {
    let offending = char_at_line_col(input, err.line(), err.column())?;
    let prev = prev_significant_char_before(input, err.line(), err.column())?;
    let offending = offending.to_string();
    match prev {
        '{' => Some(format!("Expected string key after '{{', not '{offending}'")),
        ',' => Some(format!(
            "Expected string key after ',' in object, not '{offending}'"
        )),
        _ => None,
    }
}

fn prev_significant_char_before(input: &str, line: usize, col: usize) -> Option<char> {
    let idx = line_col_to_byte_index(input, line, col)?;
    input[..idx].chars().rev().find(|ch| !ch.is_whitespace())
}

fn char_at_line_col(input: &str, line: usize, col: usize) -> Option<char> {
    let idx = line_col_to_byte_index(input, line, col)?;
    input[idx..].chars().next()
}

fn line_col_to_byte_index(input: &str, line: usize, col: usize) -> Option<usize> {
    if line == 0 || col == 0 {
        return None;
    }
    let mut cur_line = 1usize;
    let mut cur_col = 1usize;
    for (idx, ch) in input.char_indices() {
        if cur_line == line && cur_col == col {
            return Some(idx);
        }
        if ch == '\n' {
            cur_line += 1;
            cur_col = 1;
        } else {
            cur_col += 1;
        }
    }
    None
}

fn strip_serde_line_col_suffix(msg: &str) -> &str {
    let marker = " at line ";
    let Some(idx) = msg.rfind(marker) else {
        return msg;
    };
    let suffix = &msg[idx + marker.len()..];
    let Some((line, col_part)) = suffix.split_once(" column ") else {
        return msg;
    };
    if line.trim().parse::<usize>().is_ok() && col_part.trim().parse::<usize>().is_ok() {
        &msg[..idx]
    } else {
        msg
    }
}

fn select_docs(
    mut docs: Vec<JsonValue>,
    mode: DocMode,
    tool: &'static str,
) -> Result<Vec<JsonValue>, Error> {
    match mode {
        DocMode::All => Ok(docs),
        DocMode::First => Ok(docs.into_iter().next().into_iter().collect()),
        DocMode::Index(i) => {
            if i >= docs.len() {
                return Err(Error::DocIndexOutOfRange {
                    tool,
                    index: i,
                    total: docs.len(),
                });
            }
            Ok(vec![docs.swap_remove(i)])
        }
    }
}

fn extract_line_col(msg: &str) -> Option<(usize, usize)> {
    use std::sync::OnceLock;

    static PATTERNS: OnceLock<Vec<regex::Regex>> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        vec![
            regex::Regex::new(r"(?:at\s+)?line\s+(\d+)\s+column\s+(\d+)").expect("regex"),
            regex::Regex::new(r"(?:at\s+)?line\s+(\d+)\s*,\s*column\s+(\d+)").expect("regex"),
            regex::Regex::new(r"line\s*:\s*(\d+)\s*,\s*column\s*:\s*(\d+)").expect("regex"),
        ]
    });
    for re in patterns {
        if let Some(caps) = re.captures(msg) {
            let line = caps.get(1)?.as_str().parse::<usize>().ok()?;
            let col = caps.get(2)?.as_str().parse::<usize>().ok()?;
            return Some((line, col));
        }
    }
    None
}

fn render_input_context(input: &str, line: usize, col: usize) -> String {
    let lines: Vec<&str> = input.lines().collect();
    if lines.is_empty() || line == 0 {
        return String::new();
    }
    let from = line.saturating_sub(2).max(1);
    let to = (line + 2).min(lines.len());
    let mut out = String::new();
    out.push_str("input context:\n");
    for i in from..=to {
        let marker = if i == line { '>' } else { ' ' };
        let text = lines.get(i - 1).copied().unwrap_or_default();
        out.push_str(&format!("{marker} {:>5} | {text}\n", i));
        if i == line {
            let caret_pad = col.saturating_sub(1);
            out.push_str(&format!("  {:>5} | {}^\n", "", " ".repeat(caret_pad)));
        }
    }
    out.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_doc_mode_contract() {
        assert_eq!(
            parse_doc_mode("first", None).expect("first"),
            DocMode::First
        );
        assert_eq!(parse_doc_mode("all", None).expect("all"), DocMode::All);
        assert_eq!(
            parse_doc_mode("index", Some(3)).expect("index"),
            DocMode::Index(3)
        );
        assert!(matches!(
            parse_doc_mode("index", None),
            Err(Error::MissingDocIndex)
        ));
        assert!(matches!(
            parse_doc_mode("x", None),
            Err(Error::InvalidDocMode(_))
        ));
    }

    #[test]
    fn run_jq_api_works_on_yaml_input() {
        let input = "a: 1\n";
        let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
        assert_eq!(out, vec![serde_json::json!(1)]);
    }

    #[test]
    fn run_jq_api_reads_json_stream_even_with_default_doc_mode() {
        let input = "{\"a\":1}\n{\"a\":2}\n";
        let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
        assert_eq!(out, vec![serde_json::json!(1), serde_json::json!(2)]);
    }

    #[test]
    fn yaml_output_for_multiple_values_is_multidoc() {
        let out =
            format_output_yaml_documents(&[serde_json::json!({"a":1}), serde_json::json!({"b":2})])
                .expect("yaml output");
        assert!(out.contains("a: 1"));
        assert!(out.contains("---"));
        assert!(out.contains("b: 2"));
    }

    #[test]
    fn format_query_error_control_character_matches_jq_text() {
        let err = serde_json::from_str::<serde_json::Value>("\"\u{1}\"")
            .expect_err("must fail on unescaped control char");
        let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Invalid string: control characters from U+0000 through U+001F must be escaped at line 1, column 3"
        );
    }

    #[test]
    fn format_query_error_colon_in_object_matches_jq_text() {
        let err = serde_json::from_str::<serde_json::Value>("{\"a\":1,\"b\",")
            .expect_err("must fail on malformed object");
        let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Objects must consist of key:value pairs at line 1, column 11"
        );
    }

    #[test]
    fn format_query_error_string_key_after_object_start_matches_jq_text() {
        let input = "{{\"a\":\"b\"}}";
        let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
        let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Expected string key after '{', not '{' at line 1, column 2"
        );
    }

    #[test]
    fn format_query_error_string_key_after_comma_matches_jq_text() {
        let input = "{\"x\":\"y\",{\"a\":\"b\"}}";
        let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
        let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Expected string key after ',' in object, not '{' at line 1, column 10"
        );
    }

    #[test]
    fn format_query_error_string_key_array_after_object_start_matches_jq_text() {
        let input = "{[\"a\",\"b\"]}";
        let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
        let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Expected string key after '{', not '[' at line 1, column 2"
        );
    }

    #[test]
    fn format_query_error_string_key_array_after_comma_matches_jq_text() {
        let input = "{\"x\":\"y\",[\"a\",\"b\"]}";
        let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
        let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
        assert_eq!(
            msg,
            "jq: parse error: Expected string key after ',' in object, not '[' at line 1, column 10"
        );
    }

    #[test]
    fn strip_serde_line_col_suffix_only_removes_valid_suffix() {
        assert_eq!(
            strip_serde_line_col_suffix("expected value at line 1 column 2"),
            "expected value"
        );
        assert_eq!(
            strip_serde_line_col_suffix("hello at line nope column 2"),
            "hello at line nope column 2"
        );
    }

    #[test]
    fn json_output_escapes_del_like_jq() {
        let out = format_output_json_lines(&[serde_json::json!(" ~\u{007f}")], true, false)
            .expect("json output");
        assert_eq!(out, "\" ~\\u007f\"");
    }

    #[test]
    fn format_runtime_error_matches_jq_prefix() {
        let msg = format_query_error(
            "jq",
            "",
            &crate::QueryError::Runtime("Cannot index object with number".to_string()),
        );
        assert_eq!(msg, "jq: error (at <stdin>:1): Cannot index object with number");
    }
}
