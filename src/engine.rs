use crate::value::ZqValue;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::OnceLock;

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
    Ok(native_values_to_json(run_jq_native(query, input, options)?))
}

pub fn run_jq_native(
    query: &str,
    input: &str,
    options: QueryOptions,
) -> Result<Vec<ZqValue>, Error> {
    let stream = parse_jq_input_values_native(input, options.doc_mode, "jq")?;
    Ok(crate::query::run_query_stream_native_with_paths(
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
    let native_inputs = json_values_to_native(input_stream);
    Ok(native_values_to_json(
        crate::query::run_query_stream_native_with_paths_and_options(
            query,
            native_inputs,
            library_path,
            crate::query::RunOptions {
                null_input: run_options.null_input,
            },
        )?,
    ))
}

pub fn run_jq_stream_with_paths_options_native(
    query: &str,
    input_stream: Vec<ZqValue>,
    library_path: &[String],
    run_options: RunOptions,
) -> Result<Vec<ZqValue>, Error> {
    Ok(
        crate::query::run_query_stream_native_with_paths_and_options(
            query,
            input_stream,
            library_path,
            crate::query::RunOptions {
                null_input: run_options.null_input,
            },
        )?,
    )
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
        crate::native_engine::RunOptions {
            null_input: run_options.null_input,
        },
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
    Ok(native_values_to_json(
        parse_jq_input_values_with_format_native(input, doc_mode, tool, input_format)?,
    ))
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
    Ok(native_values_to_json(parse_jq_json_values_only_native(
        input,
    )?))
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
    format_output_yaml_documents_with_options(values, YamlFormatOptions::default())
}

pub fn format_output_yaml_documents_with_options(
    values: &[JsonValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(json_to_yaml_value(value)?);
    }
    if options.use_anchors {
        format_yaml_documents_with_anchors(
            &docs,
            options.anchor_name_mode,
            options.anchor_enrich_single_token,
        )
    } else {
        format_yaml_documents_plain(&docs)
    }
}

pub fn format_output_yaml_documents_native(values: &[ZqValue]) -> Result<String, Error> {
    format_output_yaml_documents_native_with_options(values, YamlFormatOptions::default())
}

pub fn format_output_yaml_documents_native_with_options(
    values: &[ZqValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(native_to_yaml_value(value)?);
    }
    if options.use_anchors {
        format_yaml_documents_with_anchors(
            &docs,
            options.anchor_name_mode,
            options.anchor_enrich_single_token,
        )
    } else {
        format_yaml_documents_plain(&docs)
    }
}

fn format_yaml_documents_plain(values: &[serde_yaml::Value]) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        let mut rendered =
            serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
        while rendered.ends_with('\n') {
            rendered.pop();
        }
        if rendered.is_empty() {
            rendered.push_str("null");
        }
        docs.push(rendered);
    }

    if docs.is_empty() {
        return Ok(String::new());
    }
    if docs.len() == 1 {
        return Ok(docs.remove(0));
    }
    Ok(docs.join("\n---\n"))
}

fn format_yaml_documents_with_anchors(
    values: &[serde_yaml::Value],
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Result<String, Error> {
    if values.is_empty() {
        return Ok(String::new());
    }

    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(render_yaml_document_with_anchors(
            value,
            name_mode,
            enrich_single_token,
        )?);
    }

    if docs.len() == 1 {
        return Ok(docs.remove(0));
    }
    Ok(docs.join("\n---\n"))
}

fn render_yaml_document_with_anchors(
    value: &serde_yaml::Value,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Result<String, Error> {
    let mut plan = YamlAnchorPlan::new(value, name_mode, enrich_single_token);
    let mut out = String::new();
    emit_yaml_value_standalone(value, 0, true, &mut out, &mut plan)?;
    if out.is_empty() {
        out.push_str("null");
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum YamlFingerprint {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Sequence(Vec<u32>),
    Mapping(Vec<(u32, u32)>),
    Tagged(String, u32),
}

#[derive(Default)]
struct YamlAnchorAnalyzer {
    interner: HashMap<YamlFingerprint, u32>,
    fingerprints: Vec<YamlFingerprint>,
    node_ids: HashMap<usize, u32>,
    value_counts: Vec<usize>,
}

impl YamlAnchorAnalyzer {
    fn analyze_document(
        value: &serde_yaml::Value,
        name_mode: YamlAnchorNameMode,
        enrich_single_token: bool,
    ) -> YamlAnchorPlan {
        let mut analyzer = Self::default();
        analyzer.analyze_node(value, true);

        let mut first_seen = vec![usize::MAX; analyzer.fingerprints.len()];
        let mut cursor = 0usize;
        fill_first_seen(
            value,
            true,
            &analyzer.node_ids,
            &mut first_seen,
            &mut cursor,
        );
        let mut first_hints = vec![None; analyzer.fingerprints.len()];
        let mut path = Vec::new();
        fill_first_hints(
            value,
            true,
            &analyzer.node_ids,
            &mut first_hints,
            &mut path,
            name_mode,
            enrich_single_token,
        );

        let mut repeated_ids = analyzer
            .value_counts
            .iter()
            .enumerate()
            .filter_map(|(id, count)| {
                if should_emit_anchor(&analyzer.fingerprints[id], *count) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        repeated_ids.sort_by_key(|id| (first_seen[*id], *id));

        let mut selected_ids = HashSet::<usize>::new();
        let mut selected_order = Vec::<usize>::new();
        for id in repeated_ids {
            let exposed = count_exposed_occurrences(
                value,
                &analyzer.node_ids,
                &selected_ids,
                analyzer.fingerprints.len(),
                id,
            );
            if exposed >= 2 {
                selected_ids.insert(id);
                selected_order.push(id);
            }
        }

        let mut anchor_names = vec![None; analyzer.fingerprints.len()];
        let mut used_names = HashSet::<String>::new();
        for id in selected_order {
            let hint = first_hints[id].as_deref();
            let base = build_anchor_base_name(
                hint,
                &analyzer.fingerprints[id],
                name_mode,
                enrich_single_token,
            );
            let name = unique_anchor_name(base, &mut used_names);
            anchor_names[id] = Some(name);
        }

        YamlAnchorPlan {
            node_ids: analyzer.node_ids,
            anchor_names,
            emitted: vec![false; analyzer.fingerprints.len()],
        }
    }

    fn analyze_node(&mut self, value: &serde_yaml::Value, value_position: bool) -> u32 {
        use serde_yaml::Value as YamlValue;

        let fingerprint = match value {
            YamlValue::Null => YamlFingerprint::Null,
            YamlValue::Bool(v) => YamlFingerprint::Bool(*v),
            YamlValue::Number(v) => YamlFingerprint::Number(v.to_string()),
            YamlValue::String(v) => YamlFingerprint::String(v.clone()),
            YamlValue::Sequence(items) => {
                let mut child_ids = Vec::with_capacity(items.len());
                for item in items {
                    child_ids.push(self.analyze_node(item, value_position));
                }
                YamlFingerprint::Sequence(child_ids)
            }
            YamlValue::Mapping(map) => {
                let mut pairs = Vec::with_capacity(map.len());
                for (key, val) in map {
                    let key_id = self.analyze_node(key, false);
                    let val_id = self.analyze_node(val, value_position);
                    pairs.push((key_id, val_id));
                }
                YamlFingerprint::Mapping(pairs)
            }
            YamlValue::Tagged(tagged) => {
                let inner_id = self.analyze_node(&tagged.value, value_position);
                YamlFingerprint::Tagged(tagged.tag.to_string(), inner_id)
            }
        };

        let id = if let Some(id) = self.interner.get(&fingerprint) {
            *id
        } else {
            let next = self.fingerprints.len() as u32;
            self.interner.insert(fingerprint.clone(), next);
            self.fingerprints.push(fingerprint);
            self.value_counts.push(0);
            next
        };

        self.node_ids.insert(value as *const _ as usize, id);
        if value_position {
            self.value_counts[id as usize] += 1;
        }
        id
    }
}

fn should_emit_anchor(fingerprint: &YamlFingerprint, count: usize) -> bool {
    if count < 2 {
        return false;
    }

    match fingerprint {
        // Anchoring tiny scalars hurts readability more than it helps.
        YamlFingerprint::Null | YamlFingerprint::Bool(_) | YamlFingerprint::Number(_) => false,
        // Require repeated long strings to avoid noisy aliases for values like "dev".
        YamlFingerprint::String(s) => count >= 3 && s.len() >= 8,
        // Structured repeated values usually yield meaningful size savings.
        YamlFingerprint::Sequence(_)
        | YamlFingerprint::Mapping(_)
        | YamlFingerprint::Tagged(_, _) => true,
    }
}

fn count_exposed_occurrences(
    value: &serde_yaml::Value,
    node_ids: &HashMap<usize, u32>,
    selected_ids: &HashSet<usize>,
    id_count: usize,
    target_id: usize,
) -> usize {
    let mut emitted_selected = vec![false; id_count];
    let mut count = 0usize;
    count_exposed_occurrences_rec(
        value,
        node_ids,
        selected_ids,
        &mut emitted_selected,
        target_id,
        &mut count,
    );
    count
}

fn count_exposed_occurrences_rec(
    value: &serde_yaml::Value,
    node_ids: &HashMap<usize, u32>,
    selected_ids: &HashSet<usize>,
    emitted_selected: &mut [bool],
    target_id: usize,
    count: &mut usize,
) {
    use serde_yaml::Value as YamlValue;

    let Some(id) = node_ids.get(&(value as *const _ as usize)).copied() else {
        return;
    };
    let idx = id as usize;
    if selected_ids.contains(&idx) {
        if emitted_selected[idx] {
            return;
        }
        emitted_selected[idx] = true;
    }

    if idx == target_id {
        *count += 1;
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                count_exposed_occurrences_rec(
                    item,
                    node_ids,
                    selected_ids,
                    emitted_selected,
                    target_id,
                    count,
                );
            }
        }
        YamlValue::Mapping(map) => {
            for (_, val) in map {
                count_exposed_occurrences_rec(
                    val,
                    node_ids,
                    selected_ids,
                    emitted_selected,
                    target_id,
                    count,
                );
            }
        }
        YamlValue::Tagged(tagged) => {
            count_exposed_occurrences_rec(
                &tagged.value,
                node_ids,
                selected_ids,
                emitted_selected,
                target_id,
                count,
            );
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

fn fill_first_hints(
    value: &serde_yaml::Value,
    value_position: bool,
    node_ids: &HashMap<usize, u32>,
    first_hints: &mut [Option<String>],
    path: &mut Vec<String>,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) {
    use serde_yaml::Value as YamlValue;

    if value_position {
        if let Some(id) = node_ids.get(&(value as *const _ as usize)) {
            let idx = *id as usize;
            if first_hints[idx].is_none() {
                first_hints[idx] = Some(anchor_hint_from_path(path));
            }
        }
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                fill_first_hints(
                    item,
                    value_position,
                    node_ids,
                    first_hints,
                    path,
                    name_mode,
                    enrich_single_token,
                );
            }
        }
        YamlValue::Mapping(map) => {
            for (key, val) in map {
                if let Some(segment) = yaml_key_hint_segment(key, name_mode, enrich_single_token) {
                    path.push(segment);
                    fill_first_hints(
                        val,
                        value_position,
                        node_ids,
                        first_hints,
                        path,
                        name_mode,
                        enrich_single_token,
                    );
                    path.pop();
                } else {
                    fill_first_hints(
                        val,
                        value_position,
                        node_ids,
                        first_hints,
                        path,
                        name_mode,
                        enrich_single_token,
                    );
                }
            }
        }
        YamlValue::Tagged(tagged) => {
            fill_first_hints(
                &tagged.value,
                value_position,
                node_ids,
                first_hints,
                path,
                name_mode,
                enrich_single_token,
            );
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

fn anchor_hint_from_path(path: &[String]) -> String {
    if path.is_empty() {
        return "root".to_string();
    }
    let take = path.len().min(2);
    let mut parts = path[path.len() - take..].to_vec();
    if parts.len() == 2 {
        if parts[1].starts_with(&parts[0]) {
            parts.remove(0);
        } else if parts[0].starts_with(&parts[1]) {
            parts.remove(1);
        }
    }
    parts.join("_")
}

fn yaml_key_hint_segment(
    key: &serde_yaml::Value,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Option<String> {
    use serde_yaml::Value as YamlValue;
    let raw = match key {
        YamlValue::String(s) => s.as_str().to_string(),
        YamlValue::Number(n) => n.to_string(),
        YamlValue::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        YamlValue::Null => "null".to_string(),
        YamlValue::Sequence(_) | YamlValue::Mapping(_) | YamlValue::Tagged(_) => return None,
    };
    let normalized =
        normalize_anchor_component_with_enrichment(raw.as_str(), name_mode, enrich_single_token);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn build_anchor_base_name(
    path_hint: Option<&str>,
    fingerprint: &YamlFingerprint,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let kind = match fingerprint {
        YamlFingerprint::Null
        | YamlFingerprint::Bool(_)
        | YamlFingerprint::Number(_)
        | YamlFingerprint::String(_) => None,
        YamlFingerprint::Sequence(_) => Some("list"),
        YamlFingerprint::Mapping(_) => Some("map"),
        YamlFingerprint::Tagged(_, _) => Some("tag"),
    };
    let mut base = path_hint
        .map(|raw| normalize_anchor_component_with_enrichment(raw, name_mode, enrich_single_token))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "root".to_string());
    if let Some(kind_suffix) = kind {
        if !base.ends_with(kind_suffix) {
            base.push('_');
            base.push_str(kind_suffix);
        }
    }
    normalize_anchor_name_with_enrichment(base.as_str(), name_mode, enrich_single_token)
}

fn unique_anchor_name(base: String, used_names: &mut HashSet<String>) -> String {
    if used_names.insert(base.clone()) {
        return base;
    }
    for idx in 2.. {
        let candidate = format!("{base}_{idx}");
        if used_names.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("infinite suffix loop")
}

fn normalize_anchor_name_with_enrichment(
    raw: &str,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let mut out = normalize_anchor_component_with_enrichment(raw, name_mode, enrich_single_token);
    if out.is_empty() {
        out.push_str("anchor");
    }
    if !out
        .chars()
        .next()
        .map(|ch| ch.is_ascii_alphabetic() || ch == '_')
        .unwrap_or(false)
    {
        out.insert_str(0, "key_");
    }
    let max_len = match name_mode {
        YamlAnchorNameMode::Friendly => 40,
        YamlAnchorNameMode::StrictFriendly => 28,
    };
    if out.len() > max_len {
        out.truncate(max_len);
        while out.ends_with('_') {
            out.pop();
        }
        if out.is_empty() {
            out.push_str("anchor");
        }
    }
    out
}

#[cfg(test)]
fn normalize_anchor_component(raw: &str, name_mode: YamlAnchorNameMode) -> String {
    normalize_anchor_component_with_enrichment(raw, name_mode, true)
}

fn normalize_anchor_component_with_enrichment(
    raw: &str,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let tokens = split_anchor_tokens(raw);
    if tokens.is_empty() {
        return String::new();
    }

    let mut filtered = tokens
        .iter()
        .enumerate()
        .filter(|(idx, token)| *idx == 0 || !is_anchor_stopword(token, name_mode))
        .map(|(_, token)| token.clone())
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        filtered = tokens.iter().cloned().collect();
    }

    let mut normalized = filtered
        .into_iter()
        .map(|token| canonicalize_anchor_token_readable(token, name_mode))
        .collect::<Vec<_>>();
    normalized = squash_anchor_tokens(normalized);
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) && normalized.len() > 1 {
        let without_single_chars = normalized
            .iter()
            .filter(|token| token.len() > 1)
            .cloned()
            .collect::<Vec<_>>();
        if !without_single_chars.is_empty() {
            normalized = without_single_chars;
        }
    }
    if enrich_single_token
        && matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && normalized.len() == 1
        && should_enrich_strict_single_token(normalized[0].as_str())
    {
        for raw_token in tokens.iter().skip(1) {
            let candidate = canonicalize_anchor_token_readable(raw_token.clone(), name_mode);
            if candidate.is_empty()
                || candidate == normalized[0]
                || candidate.chars().all(|c| c.is_ascii_digit())
                || matches!(
                    candidate.as_str(),
                    "root" | "map" | "list" | "tag" | "item" | "value" | "object"
                )
            {
                continue;
            }
            normalized.push(candidate);
            break;
        }
    }
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) && normalized.len() > 1 {
        let without_single_chars = normalized
            .iter()
            .filter(|token| token.len() > 1)
            .cloned()
            .collect::<Vec<_>>();
        if !without_single_chars.is_empty() {
            normalized = without_single_chars;
        }
    }
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && normalized.len() == 1
        && normalized[0].len() == 1
    {
        normalized[0] = "field".to_string();
    }

    // Keep names compact and readable: first token + most specific tail tokens.
    let max_parts = match name_mode {
        YamlAnchorNameMode::Friendly => 3,
        YamlAnchorNameMode::StrictFriendly => 2,
    };
    let compact = if normalized.len() <= max_parts {
        normalized
    } else {
        match max_parts {
            2 => vec![
                normalized[0].clone(),
                normalized[normalized.len() - 1].clone(),
            ],
            _ => vec![
                normalized[0].clone(),
                normalized[normalized.len() - 2].clone(),
                normalized[normalized.len() - 1].clone(),
            ],
        }
    };

    compact
        .into_iter()
        .map(|mut token| {
            let max_part_len = match name_mode {
                YamlAnchorNameMode::Friendly => 14,
                YamlAnchorNameMode::StrictFriendly => 12,
            };
            if token.len() > max_part_len {
                token.truncate(max_part_len);
            }
            token
        })
        .collect::<Vec<_>>()
        .join("_")
}

fn split_anchor_tokens(raw: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let chars = raw.chars().collect::<Vec<_>>();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }

        // Split CamelCase, digit/alpha boundaries, and acronym tails:
        // `apiVersion` -> `api version`, `HTTPRoute` -> `http route`, `ipv6Addr` -> `ipv 6 addr`.
        let should_split = if idx == 0 {
            false
        } else {
            let prev = chars[idx - 1];
            let next = chars.get(idx + 1).copied();
            prev.is_ascii_alphanumeric()
                && ((prev.is_ascii_lowercase() && ch.is_ascii_uppercase())
                    || (prev.is_ascii_digit() && ch.is_ascii_alphabetic())
                    || (prev.is_ascii_alphabetic() && ch.is_ascii_digit())
                    || (prev.is_ascii_uppercase()
                        && ch.is_ascii_uppercase()
                        && next.map(|n| n.is_ascii_lowercase()).unwrap_or(false)))
        };
        if should_split && !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }

        current.push(ch.to_ascii_lowercase());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn is_anchor_stopword(token: &str, name_mode: YamlAnchorNameMode) -> bool {
    let dicts = anchor_name_dictionaries();
    dicts.stopwords_common.contains(token)
        || (matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
            && dicts.stopwords_strict.contains(token))
}

fn canonicalize_anchor_token(token: String, name_mode: YamlAnchorNameMode) -> String {
    let dicts = anchor_name_dictionaries();
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) {
        if let Some(mapped) = dicts.canonical_strict.get(token.as_str()) {
            return mapped.clone();
        }
    }
    if let Some(mapped) = dicts.canonical_common.get(token.as_str()) {
        return mapped.clone();
    }
    token
}

fn canonicalize_anchor_token_readable(token: String, name_mode: YamlAnchorNameMode) -> String {
    let canonical = canonicalize_anchor_token(token.clone(), name_mode);
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && canonical.len() == 1
        && token.len() >= 4
    {
        return token;
    }
    canonical
}

fn should_enrich_strict_single_token(token: &str) -> bool {
    matches!(
        token,
        "meta" | "kind" | "spec" | "status" | "data" | "api" | "cfg" | "config"
    )
}

#[derive(Default)]
struct AnchorNameDictionaries {
    stopwords_common: HashSet<String>,
    stopwords_strict: HashSet<String>,
    canonical_common: HashMap<String, String>,
    canonical_strict: HashMap<String, String>,
}

fn anchor_name_dictionaries() -> &'static AnchorNameDictionaries {
    static DICTS: OnceLock<AnchorNameDictionaries> = OnceLock::new();
    DICTS.get_or_init(load_anchor_name_dictionaries)
}

fn load_anchor_name_dictionaries() -> AnchorNameDictionaries {
    let stopwords_common = parse_stopword_dict_zstd(
        include_bytes!("../assets/yaml_anchor/stopwords_common.txt.zst"),
        "stopwords_common",
    );
    let stopwords_strict = parse_stopword_dict_zstd(
        include_bytes!("../assets/yaml_anchor/stopwords_strict.txt.zst"),
        "stopwords_strict",
    );
    let canonical_common = parse_canonical_dict_zstd(
        include_bytes!("../assets/yaml_anchor/canonical_common.tsv.zst"),
        "canonical_common",
    );
    let canonical_strict = parse_canonical_dict_zstd(
        include_bytes!("../assets/yaml_anchor/canonical_strict.tsv.zst"),
        "canonical_strict",
    );
    AnchorNameDictionaries {
        stopwords_common,
        stopwords_strict,
        canonical_common,
        canonical_strict,
    }
}

fn parse_stopword_dict_zstd(bytes: &[u8], label: &str) -> HashSet<String> {
    decode_zstd_utf8(bytes, label)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.to_ascii_lowercase())
        .collect()
}

fn parse_canonical_dict_zstd(bytes: &[u8], label: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let text = decode_zstd_utf8(bytes, label);
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let Some(from) = parts.next() else {
            continue;
        };
        let Some(to) = parts.next() else {
            continue;
        };
        let from = from.trim().to_ascii_lowercase();
        let to = to.trim().to_ascii_lowercase();
        if !from.is_empty() && !to.is_empty() {
            out.insert(from, to);
        }
    }
    out
}

fn decode_zstd_utf8(bytes: &[u8], label: &str) -> String {
    let decoded = zstd::stream::decode_all(bytes)
        .unwrap_or_else(|e| panic!("decode yaml anchor dictionary `{label}`: {e}"));
    String::from_utf8(decoded)
        .unwrap_or_else(|e| panic!("yaml anchor dictionary `{label}` is not utf-8: {e}"))
}

fn squash_anchor_tokens(tokens: Vec<String>) -> Vec<String> {
    let mut out = Vec::<String>::new();
    for token in tokens {
        if token.is_empty() {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if *last == token {
                continue;
            }
            if token.starts_with(last.as_str()) && token.len() > last.len() + 1 {
                *last = token;
                continue;
            }
        }
        out.push(token);
    }
    out
}

fn fill_first_seen(
    value: &serde_yaml::Value,
    value_position: bool,
    node_ids: &HashMap<usize, u32>,
    first_seen: &mut [usize],
    cursor: &mut usize,
) {
    use serde_yaml::Value as YamlValue;

    if value_position {
        if let Some(id) = node_ids.get(&(value as *const _ as usize)) {
            let idx = *id as usize;
            if first_seen[idx] == usize::MAX {
                first_seen[idx] = *cursor;
            }
            *cursor += 1;
        }
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                fill_first_seen(item, value_position, node_ids, first_seen, cursor);
            }
        }
        YamlValue::Mapping(map) => {
            for (key, val) in map {
                fill_first_seen(key, false, node_ids, first_seen, cursor);
                fill_first_seen(val, value_position, node_ids, first_seen, cursor);
            }
        }
        YamlValue::Tagged(tagged) => {
            fill_first_seen(&tagged.value, value_position, node_ids, first_seen, cursor);
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

struct YamlAnchorPlan {
    node_ids: HashMap<usize, u32>,
    anchor_names: Vec<Option<String>>,
    emitted: Vec<bool>,
}

impl YamlAnchorPlan {
    fn new(
        value: &serde_yaml::Value,
        name_mode: YamlAnchorNameMode,
        enrich_single_token: bool,
    ) -> Self {
        YamlAnchorAnalyzer::analyze_document(value, name_mode, enrich_single_token)
    }

    fn anchor_action(&mut self, value: &serde_yaml::Value, value_position: bool) -> AnchorAction {
        if !value_position {
            return AnchorAction::None;
        }

        let Some(id) = self.node_ids.get(&(value as *const _ as usize)).copied() else {
            return AnchorAction::None;
        };
        let idx = id as usize;
        let Some(name) = self
            .anchor_names
            .get(idx)
            .and_then(|name| name.as_ref())
            .cloned()
        else {
            return AnchorAction::None;
        };
        if self.emitted[idx] {
            AnchorAction::Alias(name)
        } else {
            self.emitted[idx] = true;
            AnchorAction::Define(name)
        }
    }
}

enum AnchorAction {
    None,
    Define(String),
    Alias(String),
}

const YAML_INDENT_STEP: usize = 2;

fn emit_yaml_value_standalone(
    value: &serde_yaml::Value,
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    use serde_yaml::Value as YamlValue;

    let action = plan.anchor_action(value, value_position);
    if let AnchorAction::Alias(name) = &action {
        write_indent(out, indent);
        out.push('*');
        out.push_str(name);
        return Ok(());
    }

    match value {
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {
            write_indent(out, indent);
            emit_scalar_with_anchor(value, action, out)?;
        }
        YamlValue::Sequence(items) => {
            if items.is_empty() {
                write_indent(out, indent);
                emit_empty_with_anchor("[]", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                write_indent(out, indent);
                out.push('&');
                out.push_str(&name);
                out.push('\n');
            }
            emit_yaml_sequence_body(items, indent, value_position, out, plan)?;
        }
        YamlValue::Mapping(map) => {
            if map.is_empty() {
                write_indent(out, indent);
                emit_empty_with_anchor("{}", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                write_indent(out, indent);
                out.push('&');
                out.push_str(&name);
                out.push('\n');
            }
            emit_yaml_mapping_body(map, indent, value_position, out, plan)?;
        }
        YamlValue::Tagged(_) => {
            write_indent(out, indent);
            emit_scalar_with_anchor(value, action, out)?;
        }
    }
    Ok(())
}

fn emit_yaml_sequence_body(
    items: &[serde_yaml::Value],
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        write_indent(out, indent);
        out.push('-');
        emit_yaml_value_inline(item, indent, value_position, out, plan)?;
    }
    Ok(())
}

fn emit_yaml_mapping_body(
    map: &serde_yaml::Mapping,
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    for (idx, (key, value)) in map.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        write_indent(out, indent);
        out.push_str(&render_yaml_key(key)?);
        out.push(':');
        emit_yaml_value_inline(value, indent, value_position, out, plan)?;
    }
    Ok(())
}

fn emit_yaml_value_inline(
    value: &serde_yaml::Value,
    parent_indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    use serde_yaml::Value as YamlValue;

    let action = plan.anchor_action(value, value_position);
    if let AnchorAction::Alias(name) = &action {
        out.push(' ');
        out.push('*');
        out.push_str(name);
        return Ok(());
    }

    match value {
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {
            out.push(' ');
            emit_scalar_with_anchor(value, action, out)?;
        }
        YamlValue::Sequence(items) => {
            if items.is_empty() {
                out.push(' ');
                emit_empty_with_anchor("[]", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                out.push(' ');
                out.push('&');
                out.push_str(&name);
            }
            out.push('\n');
            emit_yaml_sequence_body(
                items,
                parent_indent + YAML_INDENT_STEP,
                value_position,
                out,
                plan,
            )?;
        }
        YamlValue::Mapping(map) => {
            if map.is_empty() {
                out.push(' ');
                emit_empty_with_anchor("{}", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                out.push(' ');
                out.push('&');
                out.push_str(&name);
            }
            out.push('\n');
            emit_yaml_mapping_body(
                map,
                parent_indent + YAML_INDENT_STEP,
                value_position,
                out,
                plan,
            )?;
        }
        YamlValue::Tagged(_) => {
            out.push(' ');
            emit_scalar_with_anchor(value, action, out)?;
        }
    }
    Ok(())
}

fn emit_empty_with_anchor(token: &str, action: AnchorAction, out: &mut String) {
    match action {
        AnchorAction::None => out.push_str(token),
        AnchorAction::Define(name) => {
            out.push('&');
            out.push_str(&name);
            out.push(' ');
            out.push_str(token);
        }
        AnchorAction::Alias(name) => {
            out.push('*');
            out.push_str(&name);
        }
    }
}

fn emit_scalar_with_anchor(
    value: &serde_yaml::Value,
    action: AnchorAction,
    out: &mut String,
) -> Result<(), Error> {
    if let AnchorAction::Define(name) = action {
        out.push('&');
        out.push_str(&name);
        out.push(' ');
    }
    out.push_str(&render_yaml_scalar(value)?);
    Ok(())
}

fn render_yaml_key(value: &serde_yaml::Value) -> Result<String, Error> {
    use serde_yaml::Value as YamlValue;
    match value {
        YamlValue::String(s) => Ok(render_yaml_string(s)),
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) => render_yaml_scalar(value),
        _ => {
            let mut rendered =
                serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
            while rendered.ends_with('\n') {
                rendered.pop();
            }
            if rendered.contains('\n') {
                return Ok(render_yaml_quoted_string(&rendered.replace('\n', " ")));
            }
            Ok(rendered)
        }
    }
}

fn render_yaml_scalar(value: &serde_yaml::Value) -> Result<String, Error> {
    use serde_yaml::Value as YamlValue;
    match value {
        YamlValue::Null => Ok("null".to_string()),
        YamlValue::Bool(v) => Ok(if *v { "true" } else { "false" }.to_string()),
        YamlValue::Number(v) => Ok(v.to_string()),
        YamlValue::String(s) => Ok(render_yaml_string(s)),
        YamlValue::Tagged(_) => {
            let mut rendered =
                serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
            while rendered.ends_with('\n') {
                rendered.pop();
            }
            if rendered.is_empty() {
                rendered.push_str("null");
            }
            Ok(rendered)
        }
        YamlValue::Sequence(_) | YamlValue::Mapping(_) => Err(Error::OutputYamlEncode(
            "internal yaml emitter received non-scalar value where scalar was expected".to_string(),
        )),
    }
}

fn render_yaml_string(value: &str) -> String {
    if can_render_plain_yaml_string(value) {
        value.to_string()
    } else {
        render_yaml_quoted_string(value)
    }
}

fn can_render_plain_yaml_string(value: &str) -> bool {
    if value.is_empty() || value.trim() != value {
        return false;
    }
    if value.chars().any(|ch| ch.is_control()) {
        return false;
    }

    let lower = value.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "null"
            | "~"
            | "true"
            | "false"
            | "yes"
            | "no"
            | "on"
            | "off"
            | "y"
            | "n"
            | ".nan"
            | ".inf"
            | "-.inf"
    ) {
        return false;
    }

    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if first.is_ascii_digit() || matches!(first, '-' | '+' | '?' | ':' | '!' | '&' | '*') {
        return false;
    }
    if !is_plain_yaml_char(first) {
        return false;
    }
    chars.all(is_plain_yaml_char)
}

fn is_plain_yaml_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/')
}

fn render_yaml_quoted_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            other if other.is_control() => {
                let _ = write!(out, "\\u{:04X}", other as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

fn write_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push(' ');
    }
}

fn json_to_yaml_value(v: &JsonValue) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Mapping, Value as YamlValue};
    match v {
        JsonValue::Null => Ok(YamlValue::Null),
        JsonValue::Bool(b) => Ok(YamlValue::Bool(*b)),
        JsonValue::Number(n) => number_token_to_yaml_value(&n.to_string()),
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

fn native_to_yaml_value(v: &ZqValue) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Mapping as YamlMap, Value as YamlValue};
    match v {
        ZqValue::Null => Ok(YamlValue::Null),
        ZqValue::Bool(b) => Ok(YamlValue::Bool(*b)),
        ZqValue::Number(n) => number_token_to_yaml_value(&n.to_string()),
        ZqValue::String(s) => Ok(YamlValue::String(s.clone())),
        ZqValue::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(native_to_yaml_value(item)?);
            }
            Ok(YamlValue::Sequence(out))
        }
        ZqValue::Object(obj) => {
            let mut map = YamlMap::new();
            for (k, val) in obj {
                map.insert(YamlValue::String(k.clone()), native_to_yaml_value(val)?);
            }
            Ok(YamlValue::Mapping(map))
        }
    }
}

fn number_token_to_yaml_value(token: &str) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Number, Value as YamlValue};

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
    serde_yaml::from_str::<YamlValue>(token).map_err(|e| Error::OutputYamlEncode(e.to_string()))
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
                return Some(KeywordLocation {
                    line: line_idx + 1,
                    col: col + 1,
                });
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
    let line = query
        .lines()
        .nth(location.line.saturating_sub(1))?
        .trim_end_matches('\r');
    let pointer_pad = " ".repeat(location.col.saturating_sub(1));
    let carets = "^".repeat(caret_len);
    Some(format!("    {line}\n    {pointer_pad}{carets}"))
}

fn format_unsupported_compile_error(tool: &str, query: &str, input: &str, msg: &str) -> String {
    let (source_name, source_text) = if !query.is_empty() {
        ("<query>", query)
    } else {
        ("<stdin>", input)
    };
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
    let line_text = lines
        .get(line.saturating_sub(1))
        .copied()
        .unwrap_or_default();
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
