use crate::value::ZqValue;
use serde_json::Value as JsonValue;
use std::io::{Read, Write};

mod yaml_output;

#[cfg(test)]
use self::yaml_output::{
    anchor_name_dictionaries, canonicalize_anchor_token, normalize_anchor_component,
    split_anchor_tokens,
};

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

pub type NativeJsonWriteOptions = crate::native_engine::JsonWriteOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YamlFormatOptions {
    pub use_anchors: bool,
    pub anchor_name_mode: YamlAnchorNameMode,
    pub anchor_enrich_single_token: bool,
}

impl YamlFormatOptions {
    pub const fn with_yaml_anchors(mut self, enabled: bool) -> Self {
        self.use_anchors = enabled;
        self
    }

    pub const fn with_anchor_name_mode(mut self, mode: YamlAnchorNameMode) -> Self {
        self.anchor_name_mode = mode;
        self
    }

    pub const fn with_anchor_single_token_enrichment(mut self, enabled: bool) -> Self {
        self.anchor_enrich_single_token = enabled;
        self
    }
}

impl Default for YamlFormatOptions {
    fn default() -> Self {
        Self {
            use_anchors: false,
            anchor_name_mode: YamlAnchorNameMode::Friendly,
            anchor_enrich_single_token: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum YamlAnchorNameMode {
    #[default]
    Friendly,
    StrictFriendly,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self { doc_mode: DocMode::First, library_path: Vec::new() }
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
    DocIndexOutOfRange { tool: &'static str, index: usize, total: usize },
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
    Ok(native_values_to_json(run_jq_native(query, input, options)?))
}

pub fn run_jq_native(
    query: &str,
    input: &str,
    options: QueryOptions,
) -> Result<Vec<ZqValue>, Error> {
    let stream = parse_jq_input_values_native(input, options.doc_mode, "jq")?;
    Ok(crate::query::run_query_stream_native_with_paths(query, stream, &options.library_path)?)
}

pub fn run_jq_stream_with_paths_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_path: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    let native_inputs = json_values_to_native(input_stream);
    Ok(native_values_to_json(crate::query::run_query_stream_native_with_paths_and_options(
        query,
        native_inputs,
        library_path,
        crate::query::RunOptions { null_input: run_options.null_input },
    )?))
}

pub fn run_jq_stream_with_paths_options_native(
    query: &str,
    input_stream: Vec<ZqValue>,
    library_path: &[String],
    run_options: RunOptions,
) -> Result<Vec<ZqValue>, Error> {
    Ok(crate::query::run_query_stream_native_with_paths_and_options(
        query,
        input_stream,
        library_path,
        crate::query::RunOptions { null_input: run_options.null_input },
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
    let native_inputs = json_slice_to_native(input_stream);
    try_run_jq_native_stream_with_paths_options_native(
        query,
        &native_inputs,
        run_options,
        |value| emit(value.into_json()),
    )
}

pub fn try_run_jq_native_stream_with_paths_options_native<F>(
    query: &str,
    input_stream: &[ZqValue],
    run_options: RunOptions,
    emit: F,
) -> Result<NativeStreamStatus, Error>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    match crate::native_engine::try_execute_stream_native(
        query,
        input_stream,
        crate::native_engine::RunOptions { null_input: run_options.null_input },
        emit,
    ) {
        crate::native_engine::TryExecuteStream::Unsupported => {
            let compile_error = crate::native_engine::try_compile_error(query)
                .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
            Err(Error::Query(crate::QueryError::Unsupported(compile_error)))
        }
        crate::native_engine::TryExecuteStream::Executed(Ok(())) => {
            Ok(NativeStreamStatus::Executed)
        }
        crate::native_engine::TryExecuteStream::Executed(Err(err)) => {
            Err(Error::Query(crate::QueryError::Runtime(err)))
        }
    }
}

pub fn try_run_jq_native_stream_json_text_options<F>(
    query: &str,
    input: &str,
    run_options: RunOptions,
    mut emit: F,
) -> Result<NativeStreamStatus, Error>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    try_run_jq_native_stream_json_text_options_native(query, input, run_options, |value| {
        emit(value.into_json())
    })
}

pub fn try_run_jq_native_stream_json_text_options_native<F>(
    query: &str,
    input: &str,
    run_options: RunOptions,
    mut emit: F,
) -> Result<NativeStreamStatus, Error>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let Some(program) = crate::native_engine::try_compile(query) else {
        let compile_error = crate::native_engine::try_compile_error(query)
            .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
        return Err(Error::Query(crate::QueryError::Unsupported(compile_error)));
    };
    let mut wrapped_emit = |value: ZqValue| emit(value);

    if run_options.null_input {
        program
            .execute_input_native(ZqValue::Null, &mut wrapped_emit)
            .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
        return Ok(NativeStreamStatus::Executed);
    }

    if is_strict_json_stream(input) {
        program
            .execute_json_text_stream_auto_native(input, &mut wrapped_emit)
            .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
        return Ok(NativeStreamStatus::Executed);
    }

    let parsed = crate::query::parse_input_values_auto_native(input).map_err(Error::Query)?;
    program
        .execute_slice_native_owned(
            parsed.values,
            crate::native_engine::RunOptions { null_input: false },
            &mut wrapped_emit,
        )
        .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
    Ok(NativeStreamStatus::Executed)
}

pub fn try_run_jq_native_stream_json_reader_options_native<R, F>(
    query: &str,
    reader: R,
    run_options: RunOptions,
    mut emit: F,
) -> Result<NativeStreamStatus, Error>
where
    R: Read + Send + 'static,
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let Some(program) = crate::native_engine::try_compile(query) else {
        let compile_error = crate::native_engine::try_compile_error(query)
            .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
        return Err(Error::Query(crate::QueryError::Unsupported(compile_error)));
    };
    let mut wrapped_emit = |value: ZqValue| emit(value);

    if run_options.null_input {
        program
            .execute_input_native(ZqValue::Null, &mut wrapped_emit)
            .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
        return Ok(NativeStreamStatus::Executed);
    }

    program
        .execute_json_reader_stream_auto_native(Box::new(reader), &mut wrapped_emit)
        .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
    Ok(NativeStreamStatus::Executed)
}

pub fn supports_native_stream_json_direct_write(query: &str) -> bool {
    crate::native_engine::supports_direct_json_stream_write(query)
}

pub fn try_run_jq_native_stream_json_reader_write_options_native<R, W>(
    query: &str,
    reader: R,
    run_options: RunOptions,
    writer: &mut W,
    write_options: NativeJsonWriteOptions,
) -> Result<NativeStreamStatus, Error>
where
    R: Read + Send + 'static,
    W: Write,
{
    let Some(program) = crate::native_engine::try_compile(query) else {
        let compile_error = crate::native_engine::try_compile_error(query)
            .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
        return Err(Error::Query(crate::QueryError::Unsupported(compile_error)));
    };

    if run_options.null_input || !program.supports_direct_json_stream_write() {
        return Err(Error::Query(crate::QueryError::Unsupported(format!(
            "query is not supported by direct json stream writer: {query}"
        ))));
    }

    program
        .execute_json_reader_stream_direct_write(Box::new(reader), writer, write_options)
        .map_err(|e| Error::Query(crate::QueryError::Runtime(e)))?;
    Ok(NativeStreamStatus::Executed)
}

fn is_strict_json_stream(input: &str) -> bool {
    for item in serde_json::Deserializer::from_str(input).into_iter::<serde::de::IgnoredAny>() {
        if item.is_err() {
            return false;
        }
    }
    true
}

pub fn parse_jq_input_values(
    input: &str,
    doc_mode: DocMode,
    tool: &'static str,
) -> Result<Vec<JsonValue>, Error> {
    parse_jq_input_values_with_format(input, doc_mode, tool, crate::query::InputFormat::Auto)
}

pub fn parse_jq_input_values_with_format(
    input: &str,
    doc_mode: DocMode,
    tool: &'static str,
    input_format: crate::query::InputFormat,
) -> Result<Vec<JsonValue>, Error> {
    Ok(native_values_to_json(parse_jq_input_values_with_format_native(
        input,
        doc_mode,
        tool,
        input_format,
    )?))
}

fn json_values_to_native(values: Vec<JsonValue>) -> Vec<ZqValue> {
    values.into_iter().map(ZqValue::from_json).collect()
}

fn json_slice_to_native(values: &[JsonValue]) -> Vec<ZqValue> {
    values.iter().cloned().map(ZqValue::from_json).collect()
}

fn native_values_to_json(values: Vec<ZqValue>) -> Vec<JsonValue> {
    values.into_iter().map(ZqValue::into_json).collect()
}

pub fn parse_jq_input_values_native(
    input: &str,
    doc_mode: DocMode,
    tool: &'static str,
) -> Result<Vec<ZqValue>, Error> {
    parse_jq_input_values_with_format_native(input, doc_mode, tool, crate::query::InputFormat::Auto)
}

pub fn parse_jq_input_values_with_format_native(
    input: &str,
    doc_mode: DocMode,
    tool: &'static str,
    input_format: crate::query::InputFormat,
) -> Result<Vec<ZqValue>, Error> {
    let parsed = crate::query::parse_input_values_with_format_native(input, input_format)?;
    match parsed.kind {
        crate::query::InputKind::JsonStream => Ok(parsed.values),
        crate::query::InputKind::YamlDocs => select_docs_native(parsed.values, doc_mode, tool),
    }
}

pub fn parse_jq_json_values_only(input: &str) -> Result<Vec<JsonValue>, Error> {
    Ok(native_values_to_json(parse_jq_json_values_only_native(input)?))
}

pub fn parse_jq_json_values_only_native(input: &str) -> Result<Vec<ZqValue>, Error> {
    crate::query::parse_json_values_only_native(input)
        .map_err(|e| Error::Query(crate::QueryError::Json(e)))
}

pub fn validate_jq_query(query: &str) -> Result<(), Error> {
    crate::query::validate_query(query).map_err(Error::Query)
}

pub fn validate_jq_query_with_paths(query: &str, library_path: &[String]) -> Result<(), Error> {
    crate::query::validate_query_with_paths(query, library_path).map_err(Error::Query)
}

pub fn debug_dump_disasm_function_labels(
    query: &str,
    library_path: &[String],
) -> Result<Vec<String>, Error> {
    let Some(program) = crate::native_engine::try_compile_with_paths(query, library_path) else {
        let compile_error = crate::native_engine::try_compile_error_with_paths(query, library_path)
            .unwrap_or_else(|| format!("query is not supported by native engine: {query}"));
        return Err(Error::Query(crate::QueryError::Unsupported(compile_error)));
    };
    Ok(program.debug_disasm_function_labels())
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

pub fn prepare_jq_query_with_paths(
    query: &str,
    library_path: &[String],
) -> Result<PreparedJq, Error> {
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
    yaml_output::format_output_yaml_documents(values)
}

pub fn format_output_yaml_documents_with_options(
    values: &[JsonValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    yaml_output::format_output_yaml_documents_with_options(values, options)
}

pub fn format_output_yaml_documents_native(values: &[ZqValue]) -> Result<String, Error> {
    yaml_output::format_output_yaml_documents_native(values)
}

pub fn format_output_yaml_documents_native_with_options(
    values: &[ZqValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    yaml_output::format_output_yaml_documents_native_with_options(values, options)
}

pub fn format_query_error(tool: &str, input: &str, err: &crate::QueryError) -> String {
    format_query_error_with_sources(tool, "", input, err)
}

pub fn format_query_error_with_sources(
    tool: &str,
    query: &str,
    input: &str,
    err: &crate::QueryError,
) -> String {
    if let crate::QueryError::Json(json_err) = err {
        return format_json_parse_error(tool, input, json_err);
    }
    if let crate::QueryError::Runtime(msg) = err {
        return format!("{tool}: error (at <stdin>:1): {msg}");
    }
    if let crate::QueryError::Unsupported(msg) = err {
        if let Some(formatted) =
            format_unterminated_try_if_compile_errors(tool, query, msg.as_str())
        {
            return formatted;
        }
        if let Some(formatted) = format_unexpected_end_of_file_compile_error(tool, query, msg) {
            return formatted;
        }
        if msg.starts_with("Top-level program not given (try \".\")") {
            return format!(
                "{tool}: error: Top-level program not given (try \".\")\n{tool}: 1 compile error"
            );
        }
        if msg.starts_with("too many function parameters or local function definitions (max 4095)")
        {
            return format!(
                "{tool}: error: too many function parameters or local function definitions (max 4095)\n{tool}: 1 compile error"
            );
        }
        return format_unsupported_compile_error(tool, query, input, msg);
    }

    let base = format!("{tool}: {err}");
    let Some((line, col)) = extract_line_col(&base) else {
        return base;
    };
    let (source_name, source_text) = match err {
        crate::QueryError::Unsupported(_) if !query.is_empty() => ("query", query),
        _ => ("input", input),
    };
    let ctx = render_labeled_context(source_name, source_text, line, col);
    if ctx.is_empty() {
        base
    } else {
        format!("{base}\n{ctx}")
    }
}

fn format_unterminated_try_if_compile_errors(tool: &str, query: &str, msg: &str) -> Option<String> {
    if query.is_empty() || !msg.contains("expected EndKw, found Catch") {
        return None;
    }
    let catch = find_keyword_location(query, "catch")?;
    let if_kw = find_keyword_location(query, "if")?;
    let try_kw = find_keyword_location(query, "try")?;

    let catch_ctx = render_jq_keyword_context(query, catch, 5)?;
    let if_ctx = render_jq_keyword_context(query, if_kw, 4)?;
    let try_ctx = render_jq_keyword_context(query, try_kw, 8)?;

    Some(format!(
        "{tool}: error: syntax error, unexpected catch, expecting end or '|' or ',' at <top-level>, line {}, column {}:\n{}\n{tool}: error: Possibly unterminated 'if' statement at <top-level>, line {}, column {}:\n{}\n{tool}: error: Possibly unterminated 'try' statement at <top-level>, line {}, column {}:\n{}\n{tool}: 3 compile errors",
        catch.line,
        catch.col,
        catch_ctx,
        if_kw.line,
        if_kw.col,
        if_ctx,
        try_kw.line,
        try_kw.col,
        try_ctx
    ))
}

fn format_unexpected_end_of_file_compile_error(
    tool: &str,
    query: &str,
    msg: &str,
) -> Option<String> {
    if query.is_empty() || !msg.starts_with("syntax error, unexpected end of file") {
        return None;
    }
    let (line_no, line_text) = query
        .lines()
        .enumerate()
        .last()
        .map(|(idx, line)| (idx + 1, line.trim_end_matches('\r')))?;
    let col = line_text.chars().count() + 1;
    let pointer_pad = " ".repeat(col.saturating_sub(1));
    Some(format!(
        "{tool}: error: syntax error, unexpected end of file at <top-level>, line {line_no}, column {col}:\n    {line_text}\n    {pointer_pad}^\n{tool}: 1 compile error"
    ))
}

#[derive(Debug, Clone, Copy)]
struct KeywordLocation {
    line: usize,
    col: usize,
}

fn find_keyword_location(query: &str, keyword: &str) -> Option<KeywordLocation> {
    for (line_idx, raw_line) in query.lines().enumerate() {
        let line = raw_line.trim_end_matches('\r');
        let mut start = 0usize;
        while let Some(rel) = line[start..].find(keyword) {
            let col = start + rel;
            let before_ok = col == 0
                || !line[..col]
                    .chars()
                    .last()
                    .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
            let after_idx = col + keyword.len();
            let after_ok = after_idx >= line.len()
                || !line[after_idx..]
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
            if before_ok && after_ok {
                return Some(KeywordLocation { line: line_idx + 1, col: col + 1 });
            }
            start = after_idx;
        }
    }
    None
}

fn render_jq_keyword_context(
    query: &str,
    location: KeywordLocation,
    caret_len: usize,
) -> Option<String> {
    let line = query.lines().nth(location.line.saturating_sub(1))?.trim_end_matches('\r');
    let pointer_pad = " ".repeat(location.col.saturating_sub(1));
    let carets = "^".repeat(caret_len);
    Some(format!("    {line}\n    {pointer_pad}{carets}"))
}

fn format_unsupported_compile_error(tool: &str, query: &str, input: &str, msg: &str) -> String {
    let (source_name, source_text) =
        if !query.is_empty() { ("<query>", query) } else { ("<stdin>", input) };
    let display_msg = normalize_unsupported_message(msg);
    let (line, col) = find_error_start(msg, source_text).unwrap_or((1, 1));
    let ctx = render_cargo_like_context(source_name, source_text, line, col);
    if ctx.is_empty() {
        format!("{tool}: error: {display_msg}\n{tool}: 1 compile error")
    } else {
        format!("{tool}: error: {display_msg}\n{ctx}\n{tool}: 1 compile error")
    }
}

fn normalize_unsupported_message(msg: &str) -> String {
    if msg.starts_with("query is not supported by native engine:") {
        return "syntax error, cannot compile this query fragment".to_string();
    }
    msg.to_string()
}

fn find_error_start(msg: &str, source: &str) -> Option<(usize, usize)> {
    if let Some((line, col)) = extract_line_col(msg) {
        return Some((line, col));
    }
    if source.is_empty() {
        return Some((1, 1));
    }
    if let Some(token) = extract_token_from_error(msg) {
        if let Some((line, col)) = find_token_line_col(source, token.as_str()) {
            return Some((line, col));
        }
    }
    first_non_whitespace_line_col(source).or(Some((1, 1)))
}

fn extract_token_from_error(msg: &str) -> Option<String> {
    use std::sync::OnceLock;

    static UNEXPECTED_RE: OnceLock<regex::Regex> = OnceLock::new();
    static UNDEFINED_RE: OnceLock<regex::Regex> = OnceLock::new();
    static OBJECT_KEY_RE: OnceLock<regex::Regex> = OnceLock::new();

    let unexpected_re =
        UNEXPECTED_RE.get_or_init(|| regex::Regex::new(r"unexpected '([^']+)'").expect("regex"));
    if let Some(caps) = unexpected_re.captures(msg) {
        return Some(caps.get(1)?.as_str().to_string());
    }

    let undefined_re = UNDEFINED_RE.get_or_init(|| {
        regex::Regex::new(r"(\$[A-Za-z0-9_\-*]+)\s+is not defined").expect("regex")
    });
    if let Some(caps) = undefined_re.captures(msg) {
        return Some(caps.get(1)?.as_str().to_string());
    }

    let object_key_re = OBJECT_KEY_RE.get_or_init(|| {
        regex::Regex::new(r"Cannot use [^()]+\(([^)]+)\) as object key").expect("regex")
    });
    if let Some(caps) = object_key_re.captures(msg) {
        return Some(caps.get(1)?.as_str().to_string());
    }

    None
}

fn find_token_line_col(source: &str, token: &str) -> Option<(usize, usize)> {
    let idx = source.find(token)?;
    Some(byte_index_to_line_col(source, idx))
}

fn first_non_whitespace_line_col(source: &str) -> Option<(usize, usize)> {
    for (idx, ch) in source.char_indices() {
        if !ch.is_whitespace() {
            return Some(byte_index_to_line_col(source, idx));
        }
    }
    None
}

fn byte_index_to_line_col(source: &str, byte_idx: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut col = 1usize;
    for (idx, ch) in source.char_indices() {
        if idx >= byte_idx {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

fn render_cargo_like_context(
    source_name: &str,
    source_text: &str,
    line: usize,
    col: usize,
) -> String {
    if source_text.is_empty() || line == 0 {
        return String::new();
    }
    let lines: Vec<&str> = source_text.lines().collect();
    let line_text = lines.get(line.saturating_sub(1)).copied().unwrap_or_default();
    let width = line.to_string().len().max(1);
    let caret_pad = col.saturating_sub(1);
    format!(
        "  --> {source_name}:{line}:{col}\n   |\n{line:>width$} | {line_text}\n   | {}^",
        " ".repeat(caret_pad),
        width = width
    )
}

fn format_json_parse_error(tool: &str, input: &str, err: &serde_json::Error) -> String {
    let raw = err.to_string();
    let mut col = err.column();
    let message = if raw
        .starts_with("control character (\\u0000-\\u001F) found while parsing a string")
    {
        // jq reports this one column later than serde_json.
        col = col.saturating_add(1);
        "Invalid string: control characters from U+0000 through U+001F must be escaped".to_string()
    } else if raw.starts_with("key must be a string") {
        format_object_key_parse_error(input, err)
            .unwrap_or_else(|| "key must be a string".to_string())
    } else if raw.starts_with("expected `:`") {
        "Objects must consist of key:value pairs".to_string()
    } else if raw.starts_with("EOF while parsing a string") {
        "Unfinished string at EOF".to_string()
    } else if raw.starts_with("EOF while parsing") {
        "Unfinished JSON term at EOF".to_string()
    } else {
        strip_serde_line_col_suffix(&raw).to_string()
    };

    format!("{tool}: parse error: {message} at line {}, column {col}", err.line())
}

fn format_object_key_parse_error(input: &str, err: &serde_json::Error) -> Option<String> {
    let offending = char_at_line_col(input, err.line(), err.column())?;
    let prev = prev_significant_char_before(input, err.line(), err.column())?;
    let offending = offending.to_string();
    match prev {
        '{' => Some(format!("Expected string key after '{{', not '{offending}'")),
        ',' => Some(format!("Expected string key after ',' in object, not '{offending}'")),
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

fn select_docs_native(
    mut docs: Vec<ZqValue>,
    mode: DocMode,
    tool: &'static str,
) -> Result<Vec<ZqValue>, Error> {
    match mode {
        DocMode::All => Ok(docs),
        DocMode::First => Ok(docs.into_iter().next().into_iter().collect()),
        DocMode::Index(i) => {
            if i >= docs.len() {
                return Err(Error::DocIndexOutOfRange { tool, index: i, total: docs.len() });
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

fn render_labeled_context(label: &str, text: &str, line: usize, col: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() || line == 0 {
        return String::new();
    }
    let from = line.saturating_sub(2).max(1);
    let to = (line + 2).min(lines.len());
    let mut out = String::new();
    out.push_str(label);
    out.push_str(" context:\n");
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
mod tests;
