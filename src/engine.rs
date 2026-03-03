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
            lines.push(serde_json::to_string(v).map_err(|e| Error::OutputEncode(e.to_string()))?);
        } else {
            lines.push(
                serde_json::to_string_pretty(v).map_err(|e| Error::OutputEncode(e.to_string()))?,
            );
        }
    }
    Ok(lines.join("\n"))
}

pub fn format_output_yaml_documents(values: &[JsonValue]) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for v in values {
        docs.push(serde_yaml::to_string(v).map_err(|e| Error::OutputYamlEncode(e.to_string()))?);
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

pub fn format_query_error(tool: &str, input: &str, err: &crate::QueryError) -> String {
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
}
