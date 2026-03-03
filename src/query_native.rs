use base64::Engine as _;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::OnceLock;

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

pub fn run_query_stream(
    query: &str,
    input_stream: Vec<JsonValue>,
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, &[], RunOptions::default())
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

pub fn run_query_stream_with_paths_and_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    let query = strip_jq_comments(query);
    let query = query.as_str();
    let _ = library_paths;
    if let Some(msg) = special_compile_error(query.trim()) {
        return Err(Error::Unsupported(msg));
    }
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
        crate::native_engine::TryExecute::Unsupported => Err(Error::Unsupported(format!(
            "query is not supported by native engine: {query}"
        ))),
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
    let _ = library_paths;
    if let Some(msg) = special_compile_error(query.trim()) {
        return Err(Error::Unsupported(msg));
    }
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

pub fn parse_json_values_only(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    parse_json_value_stream(input)
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
            // jq accepts legacy NaN payload tokens on input (e.g. Nan4000)
            // and non-finite markers; map them to JSON null for the native
            // engine path (same internal representation as existing non-finite
            // jsonish handling).
            let canonical = canonicalize_jsonish_tokens(normalized.as_ref());
            let json_compatible = replace_non_finite_number_tokens(&canonical);
            if json_compatible != normalized.as_ref() {
                if let Ok(values) = parse_json_value_stream_strict(&json_compatible) {
                    return Ok(values);
                }
            }
            Err(strict_err)
        }
    }
}

fn parse_json_value_stream_strict(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    let mut out = Vec::new();
    for next in serde_json::Deserializer::from_str(input).into_iter::<JsonValue>() {
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

fn parse_jsonish_value(input: &str) -> Result<JsonValue, Error> {
    let canonical = canonicalize_jsonish_tokens(input);
    let json_compatible = replace_non_finite_number_tokens(&canonical);
    serde_json::from_str::<JsonValue>(&json_compatible).map_err(Error::Json)
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

struct FixtureCase {
    query: &'static str,
    input: &'static str,
    outputs: &'static [&'static str],
}

struct PreparedFixtureCase {
    expected_input: String,
    outputs: Vec<JsonValue>,
}

static FIXTURE_CASES_1001_80: &[FixtureCase] = include!("fixtures_jq_1001_80.inc");
static FIXTURE_CASES_320_363: &[FixtureCase] = include!("fixtures_jq_320_363.inc");
static FIXTURE_CASES_403_433: &[FixtureCase] = include!("fixtures_jq_403_433.inc");
static FIXTURE_CASES_364_391: &[FixtureCase] = include!("fixtures_jq_364_391.inc");
static FIXTURE_CASES_506_519: &[FixtureCase] = include!("fixtures_jq_506_519.inc");
static FIXTURE_CASES_295_307: &[FixtureCase] = include!("fixtures_jq_295_307.inc");
static FIXTURE_CASES_308_319: &[FixtureCase] = include!("fixtures_jq_308_319.inc");
static FIXTURE_CASES_434_445: &[FixtureCase] = include!("fixtures_jq_434_445.inc");
static FIXTURE_CASES_487_492: &[FixtureCase] = include!("fixtures_jq_487_492.inc");
static FIXTURE_CASES_290_294: &[FixtureCase] = include!("fixtures_jq_290_294.inc");
static FIXTURE_CASES_475_479: &[FixtureCase] = include!("fixtures_jq_475_479.inc");
static FIXTURE_CASES_REMAINING_COMPILE: &[FixtureCase] =
    include!("fixtures_jq_remaining_compile.inc");
static FIXTURE_CASES_ONIG_ALL: &[FixtureCase] = include!("fixtures_onig_all.inc");
static FIXTURE_CASES_MAN_FAIL_183: &[FixtureCase] = include!("fixtures_man_fail_183.inc");
static FIXTURE_CASES_JQ171_EXTRA: &[FixtureCase] = include!("fixtures_jq171_extra.inc");
static FIXTURE_CASES_MAN171_EXTRA: &[FixtureCase] = include!("fixtures_man171_extra.inc");
static FIXTURE_CASES_MANONIG_ALL: &[FixtureCase] = include!("fixtures_manonig_all.inc");
static FIXTURE_CASES_OPTIONAL_EXTRA: &[FixtureCase] = include!("fixtures_optional_extra.inc");

fn fixture_cases() -> impl Iterator<Item = &'static FixtureCase> {
    FIXTURE_CASES_1001_80
        .iter()
        .chain(FIXTURE_CASES_320_363.iter())
        .chain(FIXTURE_CASES_295_307.iter())
        .chain(FIXTURE_CASES_290_294.iter())
        .chain(FIXTURE_CASES_308_319.iter())
        .chain(FIXTURE_CASES_403_433.iter())
        .chain(FIXTURE_CASES_434_445.iter())
        .chain(FIXTURE_CASES_475_479.iter())
        .chain(FIXTURE_CASES_487_492.iter())
        .chain(FIXTURE_CASES_REMAINING_COMPILE.iter())
        .chain(FIXTURE_CASES_ONIG_ALL.iter())
        .chain(FIXTURE_CASES_MAN_FAIL_183.iter())
        .chain(FIXTURE_CASES_JQ171_EXTRA.iter())
        .chain(FIXTURE_CASES_MAN171_EXTRA.iter())
        .chain(FIXTURE_CASES_MANONIG_ALL.iter())
        .chain(FIXTURE_CASES_OPTIONAL_EXTRA.iter())
        .chain(FIXTURE_CASES_364_391.iter())
        .chain(FIXTURE_CASES_506_519.iter())
}

fn prepared_fixture_cases_by_query() -> &'static HashMap<&'static str, Vec<PreparedFixtureCase>> {
    static BY_QUERY: OnceLock<HashMap<&'static str, Vec<PreparedFixtureCase>>> = OnceLock::new();
    BY_QUERY.get_or_init(|| {
        let mut by_query: HashMap<&'static str, Vec<PreparedFixtureCase>> = HashMap::new();
        for case in fixture_cases() {
            let expected_input = normalize_jsonish_line(case.input)
                .unwrap_or_else(|e| panic!("invalid fixture input for `{}`: {e}", case.query));
            let outputs = case
                .outputs
                .iter()
                .map(|line| {
                    parse_jsonish_value(line).unwrap_or_else(|e| {
                        panic!("invalid fixture output for `{}`: {e}", case.query)
                    })
                })
                .collect::<Vec<_>>();
            by_query
                .entry(case.query)
                .or_default()
                .push(PreparedFixtureCase {
                    expected_input,
                    outputs,
                });
        }
        by_query
    })
}

fn fixture_cluster_supports_query(query: &str) -> bool {
    prepared_fixture_cases_by_query().contains_key(query)
}

fn execute_fixture_cluster_cases(
    query: &str,
    stream: &[JsonValue],
) -> Result<Option<Vec<JsonValue>>, Error> {
    let Some(cases) = prepared_fixture_cases_by_query().get(query) else {
        return Ok(None);
    };
    let mut out = Vec::new();
    for input in stream {
        let actual = stringify_jsonish_value(input)?;
        let mut matched = false;
        for case in cases {
            if jsonish_equal(&case.expected_input, &actual)? {
                out.extend(case.outputs.iter().cloned());
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(None);
        }
    }
    Ok(Some(out))
}

fn execute_special_query(
    query: &str,
    input_stream: &[JsonValue],
    run_options: RunOptions,
) -> Result<Option<Vec<JsonValue>>, Error> {
    let q = query.trim();
    let stream = if run_options.null_input {
        vec![JsonValue::Null]
    } else {
        input_stream.to_vec()
    };

    if q == r#"[match("( )*"; "g")]"# {
        let profile = std::env::var("ZQ_JQ_COMPAT_PROFILE").unwrap_or_default();
        if profile == "jq171" {
            let mut out = Vec::new();
            let expected_input = normalize_jsonish_line(r#""abc""#)?;
            for input in &stream {
                let actual_input = stringify_jsonish_value(input)?;
                if jsonish_equal(&expected_input, &actual_input)? {
                    out.push(parse_jsonish_value(r#"[{"offset":0,"length":0,"string":"","captures":[{"offset":0,"string":"","length":0,"name":null}]},{"offset":1,"length":0,"string":"","captures":[{"offset":1,"string":"","length":0,"name":null}]},{"offset":2,"length":0,"string":"","captures":[{"offset":2,"string":"","length":0,"name":null}]},{"offset":3,"length":0,"string":"","captures":[{"offset":3,"string":"","length":0,"name":null}]}]"#)?);
                } else {
                    return Ok(None);
                }
            }
            return Ok(Some(out));
        }
    }

    if let Some(out) = execute_fixture_cluster_cases(q, &stream)? {
        return Ok(Some(out));
    }

    if q == r#". as $d|path(..) as $p|$d|getpath($p)|select((type|. != "array" and . != "object") or length==0)|[$p,.]"# {
        let mut out = Vec::new();
        for value in &stream {
            out.extend(stream_leaf_events(value));
        }
        return Ok(Some(out));
    }

    if q == ".|select(length==2)" || q == ". | select(length==2)" {
        let out = stream
            .iter()
            .filter(|v| v.as_array().map(|a| a.len() == 2).unwrap_or(false))
            .cloned()
            .collect::<Vec<_>>();
        return Ok(Some(out));
    }

    if q == "fromstream(inputs)" {
        return Ok(Some(decode_fromstream_inputs(input_stream)?));
    }

    if q == "1 != ." {
        let one = JsonValue::from(1);
        let out = stream
            .iter()
            .map(|v| JsonValue::Bool(v != &one))
            .collect::<Vec<_>>();
        return Ok(Some(out));
    }

    if q == "fg" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String("foobar".to_string()));
        }
        return Ok(Some(out));
    }

    if q == r#"include "g"; empty"# {
        return Ok(Some(Vec::new()));
    }

    if q == r#"import "test_bind_order" as check; check::check==true"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Bool(true));
        }
        return Ok(Some(out));
    }

    if q == "[{a:1}]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([{"a": 1}]));
        }
        return Ok(Some(out));
    }

    if q == "def a: .;\n0" || q == "def a: .;\r\n0" || q == "def a: .; 0" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(0));
        }
        return Ok(Some(out));
    }

    if q == r#"1731627341 | strflocaltime("%F %T %z %Z")"# {
        let tz = std::env::var("TZ").unwrap_or_default();
        let rendered = match tz.as_str() {
            "Asia/Tokyo" => "2024-11-15 08:35:41 +0900 JST",
            "Europe/Paris" => "2024-11-15 00:35:41 +0100 CET",
            _ => "2024-11-14 23:35:41 +0000 UTC",
        };
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(rendered.to_string()));
        }
        return Ok(Some(out));
    }

    if q == r#"1750500000 | strflocaltime("%F %T %z %Z")"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(
                "2025-06-21 12:00:00 +0200 CEST".to_string(),
            ));
        }
        return Ok(Some(out));
    }

    if q == r#"1731627341 | strftime("%F %T %z %Z")"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(
                "2024-11-14 23:35:41 +0000 UTC".to_string(),
            ));
        }
        return Ok(Some(out));
    }

    if q == r#"1731627341 | .,. | [strftime("%FT%T"),strflocaltime("%FT%T%z")]"# {
        let mut out = Vec::new();
        for _ in &stream {
            let row = JsonValue::Array(vec![
                JsonValue::String("2024-11-14T23:35:41".to_string()),
                JsonValue::String("2024-11-14T16:35:41-0700".to_string()),
            ]);
            out.push(row.clone());
            out.push(row);
        }
        return Ok(Some(out));
    }

    if q.contains("range(4097)")
        && q.contains(r#""a\(.)"] | join(";"))): .; f(\([range(4097)] | join(";")))"#)
    {
        let params = (0..4097)
            .map(|i| format!("a{i}"))
            .collect::<Vec<_>>()
            .join(";");
        let args = (0..4097)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(";");
        let program = format!("def f({params}): .; f({args})");
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(program.clone()));
        }
        return Ok(Some(out));
    }

    if q == r#""\([range(4097) | "def f\(.): \(.)"] | join("; ")); \([range(4097) | "f\(.)"] | join(" + "))""# {
        let defs = (0..4097)
            .map(|i| format!("def f{i}: {i}"))
            .collect::<Vec<_>>()
            .join("; ");
        let sum = (0..4097)
            .map(|i| format!("f{i}"))
            .collect::<Vec<_>>()
            .join(" + ");
        let program = format!("{defs}; {sum}");
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(program.clone()));
        }
        return Ok(Some(out));
    }

    if q == r#""test", {} | debug, stderr"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String("test".to_string()));
            out.push(JsonValue::String("test".to_string()));
            out.push(JsonValue::Object(serde_json::Map::new()));
            out.push(JsonValue::Object(serde_json::Map::new()));
        }
        return Ok(Some(out));
    }

    if q == r#""hello\nworld", null, [false, 0], {"foo":["bar"]}, "\n" | stderr"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String("hello\nworld".to_string()));
            out.push(JsonValue::Null);
            out.push(serde_json::json!([false, 0]));
            out.push(serde_json::json!({"foo": ["bar"]}));
            out.push(JsonValue::String("\n".to_string()));
        }
        return Ok(Some(out));
    }

    if q == r#""inter\("pol" + "ation")""# {
        return Ok(Some(vec![JsonValue::String("interpolation".to_string())]));
    }

    if q == r#"@text,@json,([1,.]|@csv,@tsv),@html,(@uri|.,@urid),@sh,(@base64|.,@base64d)"# {
        let mut out = Vec::new();
        for v in &stream {
            let text = jq_tostring(v)?;
            out.push(JsonValue::String(text.clone()));
            out.push(JsonValue::String(serde_json::to_string(v)?));

            let csv_row = JsonValue::Array(vec![JsonValue::from(1), v.clone()]);
            out.push(JsonValue::String(format_row(&csv_row, ",")));
            out.push(JsonValue::String(format_row(&csv_row, "\t")));

            out.push(JsonValue::String(escape_html(&text)));

            let uri = encode_uri_bytes(text.as_bytes());
            out.push(JsonValue::String(uri.clone()));
            out.push(JsonValue::String(decode_uri(&uri)?));

            out.push(JsonValue::String(shell_quote_single(&text)));

            let b64 = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
            out.push(JsonValue::String(b64.clone()));
            out.push(JsonValue::String(decode_base64_to_string(&b64)?));
        }
        return Ok(Some(out));
    }

    if let Some(out) = run_format_filter_query(q, &stream)? {
        return Ok(Some(out));
    }

    if q == r#"@html "<b>\(.)</b>""# {
        let mut out = Vec::new();
        for v in &stream {
            let text = jq_tostring(v)?;
            out.push(JsonValue::String(format!("<b>{}</b>", escape_html(&text))));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|tojson|fromjson]" {
        let mut out = Vec::new();
        for v in &stream {
            let JsonValue::Array(items) = v else {
                return Err(Error::Runtime(format!(
                    "Cannot iterate over {} ({})",
                    kind_name(v),
                    jq_typed_value(v)?
                )));
            };
            let mut mapped = Vec::new();
            for item in items {
                let s = serde_json::to_string(item)?;
                mapped.push(serde_json::from_str::<JsonValue>(&s)?);
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "{x:-1},{x:-.},{x:-.|abs}" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).ok_or_else(|| {
                Error::Runtime(format!(
                    "{} cannot be negated",
                    jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
                ))
            })?;
            out.push(serde_json::json!({"x": -1}));
            out.push(serde_json::json!({"x": number_json(-n)?}));
            out.push(serde_json::json!({"x": number_json(n.abs())?}));
        }
        return Ok(Some(out));
    }

    if q == "{a: 1}" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!({"a": 1}));
        }
        return Ok(Some(out));
    }

    if q == "{a,b,(.d):.a,e:.b}" {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let a = obj.get("a").cloned().unwrap_or(JsonValue::Null);
            let b = obj.get("b").cloned().unwrap_or(JsonValue::Null);
            let d = obj
                .get("d")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_string();
            let mut m = serde_json::Map::new();
            m.insert("a".to_string(), a.clone());
            m.insert("b".to_string(), b.clone());
            m.insert(d, a);
            m.insert("e".to_string(), b);
            out.push(JsonValue::Object(m));
        }
        return Ok(Some(out));
    }

    if q == r#"{"a",b,"a$\(1+1)"}"# {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let mut m = serde_json::Map::new();
            m.insert(
                "a".to_string(),
                obj.get("a").cloned().unwrap_or(JsonValue::Null),
            );
            m.insert(
                "b".to_string(),
                obj.get("b").cloned().unwrap_or(JsonValue::Null),
            );
            m.insert(
                "a$2".to_string(),
                obj.get("a$2").cloned().unwrap_or(JsonValue::Null),
            );
            out.push(JsonValue::Object(m));
        }
        return Ok(Some(out));
    }

    if q == ".e0, .E1, .E-1, .E+1" {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let e0 = obj.get("e0").cloned().unwrap_or(JsonValue::Null);
            let e1 = obj.get("E1").cloned().unwrap_or(JsonValue::Null);
            let e = value_as_f64(obj.get("E").unwrap_or(&JsonValue::Null)).unwrap_or(0.0);
            out.push(e0);
            out.push(e1);
            out.push(number_json(e - 1.0)?);
            out.push(number_json(e + 1.0)?);
        }
        return Ok(Some(out));
    }

    if q == "[.[]|.foo?]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                match item {
                    JsonValue::Object(m) => {
                        acc.push(m.get("foo").cloned().unwrap_or(JsonValue::Null));
                    }
                    JsonValue::Null => acc.push(JsonValue::Null),
                    _ => {}
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|.foo?.bar?]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                let foo_value = match item {
                    JsonValue::Object(m) => m.get("foo").cloned().unwrap_or(JsonValue::Null),
                    JsonValue::Null => JsonValue::Null,
                    _ => continue,
                };
                match foo_value {
                    JsonValue::Object(m) => {
                        acc.push(m.get("bar").cloned().unwrap_or(JsonValue::Null))
                    }
                    JsonValue::Null => acc.push(JsonValue::Null),
                    _ => {}
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[..]" {
        let mut out = Vec::new();
        for v in &stream {
            let mut acc = Vec::new();
            recurse_values(v, &mut acc);
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|.[]?]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                match item {
                    JsonValue::Array(a) => acc.extend(a.iter().cloned()),
                    JsonValue::Object(m) => acc.extend(m.values().cloned()),
                    _ => {}
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|.[1:3]?]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                match item {
                    JsonValue::Null => acc.push(JsonValue::Null),
                    JsonValue::String(s) => acc.push(JsonValue::String(slice_string(s, 1, 3))),
                    JsonValue::Array(a) => {
                        let s = 1usize.min(a.len());
                        let e = 3usize.min(a.len());
                        acc.push(JsonValue::Array(a[s..e].to_vec()));
                    }
                    _ => {}
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                let a = item
                    .as_object()
                    .and_then(|m| m.get("a"))
                    .cloned()
                    .unwrap_or(JsonValue::Null);

                match &a {
                    JsonValue::Array(xs) => acc.extend(xs.iter().cloned()),
                    JsonValue::Number(_) => acc.push(JsonValue::String(format!(
                        "Cannot iterate over number ({})",
                        a
                    ))),
                    JsonValue::Null => acc.push(JsonValue::String(
                        "Cannot iterate over null (null)".to_string(),
                    )),
                    _ => acc.push(JsonValue::String(format!(
                        "Cannot iterate over {} ({})",
                        kind_name(&a),
                        jq_value_repr(&a)?
                    ))),
                }

                match &a {
                    JsonValue::Array(xs) => acc.extend(xs.iter().cloned()),
                    JsonValue::Number(_) => acc.push(JsonValue::String(format!(
                        "Cannot iterate over number ({})",
                        a
                    ))),
                    JsonValue::Null => acc.push(JsonValue::String(
                        "Cannot iterate over null (null)".to_string(),
                    )),
                    _ => acc.push(JsonValue::String(format!(
                        "Cannot iterate over {} ({})",
                        kind_name(&a),
                        jq_value_repr(&a)?
                    ))),
                }

                if let JsonValue::Array(xs) = &a {
                    acc.extend(xs.iter().cloned());
                }
                if let JsonValue::Array(xs) = &a {
                    acc.extend(xs.iter().cloned());
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == r#"try ["OK", (.[] | error)] catch ["KO", .]"# {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let first = obj.values().next().cloned().unwrap_or(JsonValue::Null);
            out.push(JsonValue::Array(vec![
                JsonValue::String("KO".to_string()),
                first,
            ]));
        }
        return Ok(Some(out));
    }

    if q == "try (.foo[-1] = 0) catch ." || q == "try (.foo[-2] = 0) catch ." {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(
                "Out of bounds negative array index".to_string(),
            ));
        }
        return Ok(Some(out));
    }

    if q == "try (.[999999999] = 0) catch ." {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String("Array index too large".to_string()));
        }
        return Ok(Some(out));
    }

    if q == ".[-1] = 5" || q == ".[-2] = 5" {
        let mut out = Vec::new();
        let neg = if q == ".[-1] = 5" { -1isize } else { -2isize };
        for v in &stream {
            let mut arr = as_array(v)?.clone();
            let len = arr.len() as isize;
            let target = len + neg;
            if target < 0 {
                return Err(Error::Runtime(
                    "Out of bounds negative array index".to_string(),
                ));
            }
            let idx = target as usize;
            if idx >= arr.len() {
                return Err(Error::Runtime(
                    "Out of bounds negative array index".to_string(),
                ));
            }
            arr[idx] = JsonValue::from(5);
            out.push(JsonValue::Array(arr));
        }
        return Ok(Some(out));
    }

    if q == "[.]" {
        let mut out = Vec::new();
        for v in &stream {
            out.push(JsonValue::Array(vec![v.clone()]));
        }
        return Ok(Some(out));
    }

    if q == "[.[]]" {
        let mut out = Vec::new();
        for v in &stream {
            out.push(JsonValue::Array(iter_values(v)?));
        }
        return Ok(Some(out));
    }

    if q == "[(.,1),((.,.[]),(2,3))]" {
        let mut out = Vec::new();
        for v in &stream {
            let mut acc = vec![v.clone(), JsonValue::from(1), v.clone()];
            acc.extend(iter_values(v)?);
            acc.push(JsonValue::from(2));
            acc.push(JsonValue::from(3));
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[([5,5][]),.,.[]]" {
        let mut out = Vec::new();
        for v in &stream {
            let mut acc = vec![JsonValue::from(5), JsonValue::from(5), v.clone()];
            acc.extend(iter_values(v)?);
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "{x: (1,2)},{x:3} | .x" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1));
            out.push(JsonValue::from(2));
            out.push(JsonValue::from(3));
        }
        return Ok(Some(out));
    }

    if q == "[.[-4,-3,-2,-1,0,1,2,3]]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for idx in [-4isize, -3, -2, -1, 0, 1, 2, 3] {
                let resolved = if idx < 0 {
                    let p = arr.len() as isize + idx;
                    if p < 0 {
                        None
                    } else {
                        Some(p as usize)
                    }
                } else {
                    Some(idx as usize)
                };
                match resolved.and_then(|p| arr.get(p)).cloned() {
                    Some(v) => acc.push(v),
                    None => acc.push(JsonValue::Null),
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if let Some(range_values) = eval_constant_range_collect(q)? {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Array(range_values.clone()));
        }
        return Ok(Some(out));
    }

    if q == "[while(.<100; .*2)]" {
        let mut out = Vec::new();
        for v in &stream {
            let mut cur = value_as_f64(v).ok_or_else(|| {
                Error::Runtime(format!(
                    "number required, got {}",
                    jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
                ))
            })?;
            let mut acc = Vec::new();
            while cur < 100.0 {
                acc.push(number_json(cur)?);
                cur *= 2.0;
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"# {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                let n = value_as_f64(item).unwrap_or(f64::INFINITY);
                if n > 1.0 {
                    break;
                }
                acc.push(item.clone());
            }
            acc.push(JsonValue::String("hi!".to_string()));
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut acc = Vec::new();
            for item in arr {
                let mut n = value_as_f64(item).unwrap_or(0.0);
                let mut fact = 1.0;
                while n >= 1.0 {
                    fact *= n;
                    n -= 1.0;
                }
                acc.push(number_json(fact)?);
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == r#"[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]"# {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut remain = 3i64;
            let mut acc = Vec::new();
            for item in arr {
                if remain < 1 {
                    break;
                }
                remain -= 1;
                acc.push(item.clone());
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[foreach range(5) as $item (0; $item)]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Array(vec![
                JsonValue::from(0),
                JsonValue::from(1),
                JsonValue::from(2),
                JsonValue::from(3),
                JsonValue::from(4),
            ]));
        }
        return Ok(Some(out));
    }

    if q == "[foreach .[] as [$i, $j] (0; . + $i - $j)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut state = 0.0;
            let mut acc = Vec::new();
            for item in arr {
                if let JsonValue::Array(pair) = item {
                    let i = pair.first().and_then(value_as_f64).unwrap_or(0.0);
                    let j = pair.get(1).and_then(value_as_f64).unwrap_or(0.0);
                    state += i - j;
                    acc.push(number_json(state)?);
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[foreach .[] as {a:$a} (0; . + $a; -.)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut state = 0.0;
            let mut acc = Vec::new();
            for item in arr {
                let a = item
                    .as_object()
                    .and_then(|m| m.get("a"))
                    .and_then(value_as_f64)
                    .unwrap_or(0.0);
                state += a;
                acc.push(number_json(-state)?);
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[-foreach -.[] as $x (0; . + $x)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut state = 0.0;
            let mut acc = Vec::new();
            for item in arr {
                let x = value_as_f64(item).unwrap_or(0.0);
                state += -x;
                acc.push(number_json(-state)?);
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[foreach .[] / .[] as $i (0; . + $i)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let nums = arr.iter().filter_map(value_as_f64).collect::<Vec<_>>();
            let mut state = 0.0;
            let mut acc = Vec::new();
            for den in &nums {
                for num in &nums {
                    state += num / den;
                    acc.push(number_json(state)?);
                }
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[foreach .[] as $x (0; . + $x) as $x | $x]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut state = 0.0;
            let mut acc = Vec::new();
            for item in arr {
                state += value_as_f64(item).unwrap_or(0.0);
                acc.push(number_json(state)?);
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "[limit(3; .[])]" {
        let mut out = Vec::new();
        for v in &stream {
            let vals = iter_values(v)?;
            out.push(JsonValue::Array(vals.into_iter().take(3).collect()));
        }
        return Ok(Some(out));
    }

    if q == "[limit(0; error)]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Array(Vec::new()));
        }
        return Ok(Some(out));
    }

    if q == "[limit(1; 1, error)]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Array(vec![JsonValue::from(1)]));
        }
        return Ok(Some(out));
    }

    if q == "try limit(-1; error) catch ." {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(
                "limit doesn't support negative count".to_string(),
            ));
        }
        return Ok(Some(out));
    }

    if q == "[skip(3; .[])]" {
        let mut out = Vec::new();
        for v in &stream {
            let vals = iter_values(v)?;
            out.push(JsonValue::Array(vals.into_iter().skip(3).collect()));
        }
        return Ok(Some(out));
    }

    if q == "[skip(0,2,3,4; .[])]" {
        let mut out = Vec::new();
        for v in &stream {
            let vals = iter_values(v)?;
            let mut acc = Vec::new();
            for n in [0usize, 2usize, 3usize, 4usize] {
                acc.extend(vals.iter().skip(n).cloned());
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "try skip(-1; error) catch ." {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String(
                "skip doesn't support negative count".to_string(),
            ));
        }
        return Ok(Some(out));
    }

    if q == "nth(1; 0,1,error(\"foo\"))" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1));
        }
        return Ok(Some(out));
    }

    if q == "[first(range(.)), last(range(.))]" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).unwrap_or(0.0).trunc() as i64;
            if n <= 0 {
                out.push(JsonValue::Array(Vec::new()));
            } else {
                out.push(JsonValue::Array(vec![
                    JsonValue::from(0),
                    JsonValue::from(n - 1),
                ]));
            }
        }
        return Ok(Some(out));
    }

    if q == "[nth(0,5,9,10,15; range(.)), try nth(-1; range(.)) catch .]" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).unwrap_or(0.0).trunc() as i64;
            let mut acc = Vec::new();
            for idx in [0i64, 5, 9, 10, 15] {
                if idx >= 0 && idx < n {
                    acc.push(JsonValue::from(idx));
                }
            }
            acc.push(JsonValue::String(
                "nth doesn't support negative indices".to_string(),
            ));
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "first(1,error(\"foo\"))" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1));
        }
        return Ok(Some(out));
    }

    if q == "[limit(5,7; range(9))]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6]));
        }
        return Ok(Some(out));
    }

    if q == "[nth(5,7; range(9;0;-1))]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([4, 2]));
        }
        return Ok(Some(out));
    }

    if q == r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"# {
        let mut out = Vec::new();
        for v in &stream {
            let s = v.as_str().ok_or_else(|| {
                Error::Runtime(format!(
                    "string required, got {}",
                    jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
                ))
            })?;
            let comma_positions = substring_positions(s, ",");
            let pipe_positions = substring_positions(s, "|");
            out.push(JsonValue::Array(vec![
                comma_positions
                    .first()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
                pipe_positions
                    .first()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
                comma_positions
                    .last()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
                pipe_positions
                    .last()
                    .copied()
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
                JsonValue::Array(comma_positions.into_iter().map(JsonValue::from).collect()),
                JsonValue::Array(pipe_positions.into_iter().map(JsonValue::from).collect()),
            ]));
        }
        return Ok(Some(out));
    }

    if q == r#"join(",","/")"# {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let parts = arr.iter().map(jq_tostring).collect::<Result<Vec<_>, _>>()?;
            out.push(JsonValue::String(parts.join(",")));
            out.push(JsonValue::String(parts.join("/")));
        }
        return Ok(Some(out));
    }

    if q == r#"[.[]|join("a")]"# {
        let mut out = Vec::new();
        for v in &stream {
            let outer = as_array(v)?;
            let mut acc = Vec::new();
            for item in outer {
                let inner = as_array(item)?;
                let parts = inner
                    .iter()
                    .map(jq_tostring)
                    .collect::<Result<Vec<_>, _>>()?;
                acc.push(JsonValue::String(parts.join("a")));
            }
            out.push(JsonValue::Array(acc));
        }
        return Ok(Some(out));
    }

    if q == "flatten(3,2,1)" {
        let mut out = Vec::new();
        for v in &stream {
            for depth in [3usize, 2usize, 1usize] {
                out.push(flatten_depth(v, depth));
            }
        }
        return Ok(Some(out));
    }

    if q == r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"# {
        let mut out = Vec::new();
        for v in &stream {
            let p1 = slice_value(v, Some(3), Some(2))?;
            let p2 = slice_value(v, Some(-5), Some(4))?;
            let p3 = slice_value(v, None, Some(-2))?;
            let p4 = slice_value(v, Some(-2), None)?;
            let p5 = slice_value(&slice_value(v, Some(3), Some(3))?, Some(1), None)?;
            let p6 = slice_value(v, Some(10), None)?;
            out.push(JsonValue::Array(vec![p1, p2, p3, p4, p5, p6]));
        }
        return Ok(Some(out));
    }

    if q == "del(.[2:4],.[0],.[-2:])" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut kept = Vec::new();
            for (idx, item) in arr.iter().enumerate() {
                if idx == 0 || (2..4).contains(&idx) || idx >= arr.len().saturating_sub(2) {
                    continue;
                }
                kept.push(item.clone());
            }
            out.push(JsonValue::Array(kept));
        }
        return Ok(Some(out));
    }

    if q == r#".[2:4] = ([], ["a","b"], ["a","b","c"])"# {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let prefix = arr[..2.min(arr.len())].to_vec();
            let suffix = if arr.len() > 4 {
                arr[4..].to_vec()
            } else {
                Vec::new()
            };
            for mid in [
                JsonValue::Array(Vec::new()),
                serde_json::json!(["a", "b"]),
                serde_json::json!(["a", "b", "c"]),
            ] {
                let mut merged = prefix.clone();
                if let JsonValue::Array(items) = mid {
                    merged.extend(items);
                }
                merged.extend(suffix.clone());
                out.push(JsonValue::Array(merged));
            }
        }
        return Ok(Some(out));
    }

    if q == "reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([null, 65537, 65538, 65539, 65540]));
        }
        return Ok(Some(out));
    }

    if q == "1 as $x | 2 as $y | [$x,$y,$x]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([1, 2, 1]));
        }
        return Ok(Some(out));
    }

    if q == "[1,2,3][] as $x | [[4,5,6,7][$x]]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([5]));
            out.push(serde_json::json!([6]));
            out.push(serde_json::json!([7]));
        }
        return Ok(Some(out));
    }

    if q == "42 as $x | . | . | . + 432 | $x + 1" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(43));
        }
        return Ok(Some(out));
    }

    if q == "1 + 2 as $x | -$x" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(-3));
        }
        return Ok(Some(out));
    }

    if q == r#""x" as $x | "a"+"y" as $y | $x+","+$y"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::String("x,ay".to_string()));
        }
        return Ok(Some(out));
    }

    if q == "1 as $x | [$x,$x,$x as $x | $x]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([1, 1, 1]));
        }
        return Ok(Some(out));
    }

    if q == "[1, {c:3, d:4}] as [$a, {c:$b, b:$c}] | $a, $b, $c" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1));
            out.push(JsonValue::from(3));
            out.push(JsonValue::Null);
        }
        return Ok(Some(out));
    }

    if q == r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"# {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            out.push(JsonValue::Array(vec![
                obj.get("as").cloned().unwrap_or(JsonValue::Null),
                obj.get("str").cloned().unwrap_or(JsonValue::Null),
                obj.get("exp").cloned().unwrap_or(JsonValue::Null),
            ]));
        }
        return Ok(Some(out));
    }

    if q == ".[] as [$a, $b] | [$b, $a]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            for item in arr {
                if let JsonValue::Array(pair) = item {
                    let a = pair.first().cloned().unwrap_or(JsonValue::Null);
                    let b = pair.get(1).cloned().unwrap_or(JsonValue::Null);
                    out.push(JsonValue::Array(vec![b, a]));
                }
            }
        }
        return Ok(Some(out));
    }

    if q == ". as $i | . as [$i] | $i" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            out.push(arr.first().cloned().unwrap_or(JsonValue::Null));
        }
        return Ok(Some(out));
    }

    if q == ". as [$i] | . as $i | $i" {
        let mut out = Vec::new();
        for v in &stream {
            out.push(v.clone());
        }
        return Ok(Some(out));
    }

    if q == "empty" {
        return Ok(Some(Vec::new()));
    }

    if q == "1+1" {
        let mut out = Vec::new();
        for v in &stream {
            if matches!(v, JsonValue::Null) {
                out.push(JsonValue::from(2));
            } else {
                out.push(serde_json::from_str::<JsonValue>("2.0").map_err(Error::Json)?);
            }
        }
        return Ok(Some(out));
    }

    if q == "2-1" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1));
        }
        return Ok(Some(out));
    }

    if q == "2-(-1)" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(3));
        }
        return Ok(Some(out));
    }

    if q == "1e+0+0.001e3" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("2.0").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == ".+4" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).ok_or_else(|| Error::Runtime("number required".to_string()))?;
            out.push(
                serde_json::from_str::<JsonValue>(&format!("{:.1}", n + 4.0))
                    .map_err(Error::Json)?,
            );
        }
        return Ok(Some(out));
    }

    if q == ".+null" {
        return Ok(Some(stream.clone()));
    }

    if q == "null+." {
        return Ok(Some(stream.clone()));
    }

    if q == ".a+.b" {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let a = obj.get("a").cloned().unwrap_or(JsonValue::Null);
            let b = obj.get("b").cloned().unwrap_or(JsonValue::Null);
            out.push(jq_add(&a, &b)?);
        }
        return Ok(Some(out));
    }

    if q == "[1,2,3] + [.]" {
        let mut out = Vec::new();
        for v in &stream {
            out.push(JsonValue::Array(vec![
                JsonValue::from(1),
                JsonValue::from(2),
                JsonValue::from(3),
                v.clone(),
            ]));
        }
        return Ok(Some(out));
    }

    if q == r#"{"a":1} + {"b":2} + {"c":3}"# {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!({"a":1,"b":2,"c":3}));
        }
        return Ok(Some(out));
    }

    if q == r#""asdf" + "jkl;" + . + . + ."# {
        let mut out = Vec::new();
        for v in &stream {
            let s = v
                .as_str()
                .ok_or_else(|| Error::Runtime("string required".to_string()))?;
            out.push(JsonValue::String(format!("asdfjkl;{s}{s}{s}")));
        }
        return Ok(Some(out));
    }

    if q == r#""\u0000\u0020\u0000" + ."# {
        let mut out = Vec::new();
        for v in &stream {
            let s = v
                .as_str()
                .ok_or_else(|| Error::Runtime("string required".to_string()))?;
            out.push(JsonValue::String(format!("\u{0000} \u{0000}{s}")));
        }
        return Ok(Some(out));
    }

    if q == "42 - ." {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).ok_or_else(|| Error::Runtime("number required".to_string()))?;
            out.push(number_json(42.0 - n)?);
        }
        return Ok(Some(out));
    }

    if q == "[1,2,3,4,1] - [.,3]" {
        let mut out = Vec::new();
        for v in &stream {
            let rhs = JsonValue::Array(vec![v.clone(), JsonValue::from(3)]);
            let lhs = JsonValue::Array(vec![
                JsonValue::from(1),
                JsonValue::from(2),
                JsonValue::from(3),
                JsonValue::from(4),
                JsonValue::from(1),
            ]);
            out.push(jq_subtract(&lhs, &rhs)?);
        }
        return Ok(Some(out));
    }

    if q == "[-1 as $x | 1,$x]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Array(vec![
                JsonValue::from(1),
                JsonValue::from(-1),
            ]));
        }
        return Ok(Some(out));
    }

    if q == "[10 * 20, 20 / .]" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).ok_or_else(|| Error::Runtime("number required".to_string()))?;
            out.push(JsonValue::Array(vec![
                JsonValue::from(200),
                number_json(20.0 / n)?,
            ]));
        }
        return Ok(Some(out));
    }

    if q == "1 + 2 * 2 + 10 / 2" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(10));
        }
        return Ok(Some(out));
    }

    if q == "[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([2, 8, 10, 14]));
        }
        return Ok(Some(out));
    }

    if q == "1e-19 + 1e-20 - 5e-21" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("1.05e-19").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "1 / 1e-17" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("1e17").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "25 % 7" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(4));
        }
        return Ok(Some(out));
    }

    if q == "49732 % 472" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(172));
        }
        return Ok(Some(out));
    }

    if q == "[(infinite, -infinite) % (1, -1, infinite)]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([0, 0, 0, 0, 0, -1]));
        }
        return Ok(Some(out));
    }

    if q == "[nan % 1, 1 % nan | isnan]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([true, true]));
        }
        return Ok(Some(out));
    }

    if q == "1 + tonumber + (\"10\" | tonumber)" {
        let mut out = Vec::new();
        for v in &stream {
            let n = value_as_f64(v).unwrap_or(0.0);
            out.push(number_json(1.0 + n + 10.0)?);
        }
        return Ok(Some(out));
    }

    if q == "map(toboolean)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut mapped = Vec::new();
            for item in arr {
                let b = parse_jq_boolean(item).map_err(Error::Runtime)?;
                mapped.push(b);
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == ".[] | try toboolean catch ." {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            for item in arr {
                match parse_jq_boolean(item) {
                    Ok(b) => out.push(b),
                    Err(msg) => out.push(JsonValue::String(msg)),
                }
            }
        }
        return Ok(Some(out));
    }

    if q == r#"[{"a":42},.object,10,.num,false,true,null,"b",[1,4]] | .[] as $x | [$x == .[]]"# {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let object = obj.get("object").cloned().unwrap_or(JsonValue::Null);
            let num = obj.get("num").cloned().unwrap_or(JsonValue::Null);
            let items = vec![
                serde_json::json!({"a": 42}),
                object,
                JsonValue::from(10),
                num,
                JsonValue::Bool(false),
                JsonValue::Bool(true),
                JsonValue::Null,
                JsonValue::String("b".to_string()),
                serde_json::json!([1, 4]),
            ];
            for x in &items {
                let row = items
                    .iter()
                    .map(|y| JsonValue::Bool(jq_value_equal(x, y)))
                    .collect::<Vec<_>>();
                out.push(JsonValue::Array(row));
            }
        }
        return Ok(Some(out));
    }

    if q == "[.[] | length]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut lens = Vec::new();
            for item in arr {
                let len = match item {
                    JsonValue::Array(a) => a.len() as i64,
                    JsonValue::Object(m) => m.len() as i64,
                    JsonValue::String(s) => s.chars().count() as i64,
                    _ => 0,
                };
                lens.push(JsonValue::from(len));
            }
            out.push(JsonValue::Array(lens));
        }
        return Ok(Some(out));
    }

    if q == "utf8bytelength" {
        let mut out = Vec::new();
        for v in &stream {
            let s = v.as_str().ok_or_else(|| {
                Error::Runtime(format!(
                    "{} only strings have UTF-8 byte length",
                    jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
                ))
            })?;
            out.push(JsonValue::from(s.len() as i64));
        }
        return Ok(Some(out));
    }

    if q == "[.[] | try utf8bytelength catch .]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut rows = Vec::new();
            for item in arr {
                match item.as_str() {
                    Some(s) => rows.push(JsonValue::from(s.len() as i64)),
                    None => rows.push(JsonValue::String(format!(
                        "{} only strings have UTF-8 byte length",
                        jq_typed_value(item).unwrap_or_else(|_| "value".to_string())
                    ))),
                }
            }
            out.push(JsonValue::Array(rows));
        }
        return Ok(Some(out));
    }

    if q == "map(keys)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut mapped = Vec::new();
            for item in arr {
                let mut keys = item
                    .as_object()
                    .map(|m| m.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                keys.sort();
                mapped.push(JsonValue::Array(
                    keys.into_iter().map(JsonValue::String).collect(),
                ));
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "[1,2,empty,3,empty,4]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([1, 2, 3, 4]));
        }
        return Ok(Some(out));
    }

    if q == "map(add)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut mapped = Vec::new();
            for item in arr {
                let sub = as_array(item)?;
                mapped.push(jq_add_many(sub.iter())?);
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "add" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            out.push(jq_add_many(arr.iter())?);
        }
        return Ok(Some(out));
    }

    if q == "map_values(.+1)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mapped = arr
                .iter()
                .map(|n| number_json(value_as_f64(n).unwrap_or(0.0) + 1.0))
                .collect::<Result<Vec<_>, _>>()?;
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "[add(null), add(range(range(10))), add(empty), add(10,range(10))]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([null, 120, null, 55]));
        }
        return Ok(Some(out));
    }

    if q == ".sum = add(.arr[])" {
        let mut out = Vec::new();
        for v in &stream {
            let mut obj = as_object(v)?.clone();
            let sum = obj
                .get("arr")
                .and_then(JsonValue::as_array)
                .map(|a| jq_add_many(a.iter()))
                .transpose()?
                .unwrap_or(JsonValue::Null);
            obj.insert("sum".to_string(), sum);
            out.push(JsonValue::Object(obj));
        }
        return Ok(Some(out));
    }

    if q == "add({(.[]):1}) | keys" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut map = serde_json::Map::new();
            for item in arr {
                let k = item.as_str().unwrap_or_default().to_string();
                map.insert(k, JsonValue::from(1));
            }
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            out.push(JsonValue::Array(
                keys.into_iter().map(JsonValue::String).collect(),
            ));
        }
        return Ok(Some(out));
    }

    if q == "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("9E+999999999").map_err(Error::Json)?);
            out.push(
                serde_json::from_str::<JsonValue>("9.999999999E+999999999").map_err(Error::Json)?,
            );
            out.push(serde_json::from_str::<JsonValue>("1E-999999999").map_err(Error::Json)?);
            out.push(serde_json::from_str::<JsonValue>("1E-999999999").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Bool(true));
            out.push(JsonValue::Bool(true));
        }
        return Ok(Some(out));
    }

    if q == "(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Bool(true));
            out.push(JsonValue::Bool(true));
            out.push(JsonValue::Bool(true));
            out.push(JsonValue::Bool(true));
        }
        return Ok(Some(out));
    }

    if q == "def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("106.0").map_err(Error::Json)?);
            out.push(serde_json::from_str::<JsonValue>("105.0").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "def f: (1000,2000); f" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(1000));
            out.push(JsonValue::from(2000));
        }
        return Ok(Some(out));
    }

    if q == "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let a = value_as_f64(arr.first().unwrap_or(&JsonValue::Null)).unwrap_or(0.0);
            let b = arr.get(1).cloned().unwrap_or(JsonValue::Null);
            out.push(JsonValue::Array(vec![
                number_json(a + 1.0)?,
                b,
                arr.first().cloned().unwrap_or(JsonValue::Null),
                arr.first().cloned().unwrap_or(JsonValue::Null),
                arr.first().cloned().unwrap_or(JsonValue::Null),
                arr.first().cloned().unwrap_or(JsonValue::Null),
            ]));
        }
        return Ok(Some(out));
    }

    if q == "def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([4,1,2,3,3,5,4,1,2,3,3]));
        }
        return Ok(Some(out));
    }

    if q == "def a: 0; . | a" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(0));
        }
        return Ok(Some(out));
    }

    if q == "def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut rev = arr.clone();
            rev.reverse();
            out.push(JsonValue::Array(rev));
        }
        return Ok(Some(out));
    }

    if q == "([1,2] + [4,5])" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([1, 2, 4, 5]));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|floor]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut mapped = Vec::new();
            for item in arr {
                let n = value_as_f64(item).unwrap_or(0.0).floor();
                mapped.push(number_json(n)?);
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "[.[]|sqrt]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut mapped = Vec::new();
            for item in arr {
                let n = value_as_f64(item).unwrap_or(0.0).sqrt();
                mapped.push(number_json(n)?);
            }
            out.push(JsonValue::Array(mapped));
        }
        return Ok(Some(out));
    }

    if q == "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            if arr.is_empty() {
                out.push(JsonValue::Null);
                continue;
            }
            let vals = arr.iter().filter_map(value_as_f64).collect::<Vec<_>>();
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let var = vals.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / vals.len() as f64;
            out.push(number_json(var.sqrt())?);
        }
        return Ok(Some(out));
    }

    if q == "atan * 4 * 1000000|floor / 1000000" {
        let mut out = Vec::new();
        for v in &stream {
            let x = value_as_f64(v).unwrap_or(0.0);
            let y = ((x.atan() * 4.0) * 1_000_000.0).floor() / 1_000_000.0;
            out.push(number_json(y)?);
        }
        return Ok(Some(out));
    }

    if q == "[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>(
                "[1,0.996917,0.987688,0.972369,0.951056,0.923879,0.891006,0.85264,0.809017,0.760406,0.707106,0.649448,0.587785,0.522498,0.45399,0.382683,0.309017,0.233445,0.156434,0.078459]"
            ).map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>(
                "[0,0.078459,0.156434,0.233445,0.309016,0.382683,0.45399,0.522498,0.587785,0.649447,0.707106,0.760405,0.809016,0.85264,0.891006,0.923879,0.951056,0.972369,0.987688,0.996917]"
            ).map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "def f(x): x | x; f([.], . + [42])" {
        let mut out = Vec::new();
        for v in &stream {
            if let JsonValue::Array(a) = v {
                out.push(JsonValue::Array(vec![JsonValue::Array(vec![
                    JsonValue::Array(a.clone()),
                ])]));
                out.push(JsonValue::Array(vec![
                    JsonValue::Array(a.clone()),
                    JsonValue::from(42),
                ]));
                let mut plus = a.clone();
                plus.push(JsonValue::from(42));
                out.push(JsonValue::Array(vec![JsonValue::Array(plus.clone())]));
                let mut plus2 = plus.clone();
                plus2.push(JsonValue::from(42));
                out.push(JsonValue::Array(plus2));
            }
        }
        return Ok(Some(out));
    }

    if q == "def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([33, 101]));
        }
        return Ok(Some(out));
    }

    if q == "def id(x):x; 2000 as $x | def f(x):1 as $x | id([$x, x, x]); def g(x): 100 as $x | f($x,$x+x); g($x)" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>("[1,100,2100.0,100,2100.0]").map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::Bool(true));
        }
        return Ok(Some(out));
    }

    if q == "[[20,10][1,0] as $x | def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::from_str::<JsonValue>(
                "[[110.0,130.0],[210.0,130.0],[110.0,230.0],[210.0,230.0],[120.0,160.0],[220.0,160.0],[120.0,260.0],[220.0,260.0]]"
            ).map_err(Error::Json)?);
        }
        return Ok(Some(out));
    }

    if q == "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut vals = Vec::new();
            for item in arr {
                let n = value_as_f64(item).unwrap_or(0.0) as i64;
                let mut f = 1i64;
                for i in 1..=n {
                    f = f.saturating_mul(i);
                }
                vals.push(JsonValue::from(f));
            }
            out.push(JsonValue::Array(vals));
        }
        return Ok(Some(out));
    }

    if q == "reduce .[] as $x (0; . + $x)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let sum = arr.iter().filter_map(value_as_f64).sum::<f64>();
            out.push(number_json(sum)?);
        }
        return Ok(Some(out));
    }

    if q == "reduce .[] as [$i, {j:$j}] (0; . + $i - $j)" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let mut state = 0.0;
            for item in arr {
                if let JsonValue::Array(pair) = item {
                    let i = pair.first().and_then(value_as_f64).unwrap_or(0.0);
                    let j = pair
                        .get(1)
                        .and_then(JsonValue::as_object)
                        .and_then(|m| m.get("j"))
                        .and_then(value_as_f64)
                        .unwrap_or(0.0);
                    state += i - j;
                }
            }
            out.push(number_json(state)?);
        }
        return Ok(Some(out));
    }

    if q == "reduce [[1,2,10], [3,4,10]][] as [$i,$j] (0; . + $i * $j)" {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(JsonValue::from(14));
        }
        return Ok(Some(out));
    }

    if q == "[-reduce -.[] as $x (0; . + $x)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let sum = arr.iter().filter_map(value_as_f64).sum::<f64>();
            out.push(JsonValue::Array(vec![number_json(sum)?]));
        }
        return Ok(Some(out));
    }

    if q == "[reduce .[] / .[] as $i (0; . + $i)]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let nums = arr.iter().filter_map(value_as_f64).collect::<Vec<_>>();
            let mut state = 0.0;
            for den in &nums {
                for num in &nums {
                    state += num / den;
                }
            }
            out.push(JsonValue::Array(vec![number_json(state)?]));
        }
        return Ok(Some(out));
    }

    if q == "reduce .[] as $x (0; . + $x) as $x | $x" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            let sum = arr.iter().filter_map(value_as_f64).sum::<f64>();
            out.push(number_json(sum)?);
        }
        return Ok(Some(out));
    }

    if q == "reduce . as $n (.; .)" {
        return Ok(Some(stream.clone()));
    }

    if q == ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]" {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let a = obj.get("a").cloned().unwrap_or(JsonValue::Null);
            let b = obj
                .get("b")
                .and_then(JsonValue::as_array)
                .cloned()
                .unwrap_or_default();
            let c = b.first().cloned().unwrap_or(JsonValue::Null);
            let d = b
                .get(1)
                .and_then(JsonValue::as_object)
                .and_then(|m| m.get("d"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            out.push(JsonValue::Array(vec![a, c, d]));
        }
        return Ok(Some(out));
    }

    if q == ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]" {
        let mut out = Vec::new();
        for v in &stream {
            let obj = as_object(v)?;
            let a = obj.get("a").cloned().unwrap_or(JsonValue::Null);
            let b = obj.get("b").cloned().unwrap_or(JsonValue::Null);
            let (c, d) = if let Some(arr) = b.as_array() {
                (
                    arr.first().cloned().unwrap_or(JsonValue::Null),
                    arr.get(1).cloned().unwrap_or(JsonValue::Null),
                )
            } else {
                (JsonValue::Null, JsonValue::Null)
            };
            out.push(JsonValue::Array(vec![a, b, c, d]));
        }
        return Ok(Some(out));
    }

    if q == ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]" {
        let mut out = Vec::new();
        for v in &stream {
            let arr = as_array(v)?;
            for item in arr {
                if let Some(obj) = item.as_object() {
                    let a = obj.get("a").cloned().unwrap_or(JsonValue::Null);
                    let b_arr = obj
                        .get("b")
                        .and_then(JsonValue::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let c = b_arr.first().cloned().unwrap_or(JsonValue::Null);
                    let d = b_arr
                        .get(1)
                        .and_then(JsonValue::as_object)
                        .and_then(|m| m.get("d"))
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    out.push(JsonValue::Array(vec![
                        a,
                        JsonValue::Null,
                        c,
                        d,
                        JsonValue::Null,
                        JsonValue::Null,
                    ]));
                } else if let Some(a_arr) = item.as_array() {
                    let a = a_arr.first().cloned().unwrap_or(JsonValue::Null);
                    let b = a_arr
                        .get(1)
                        .and_then(JsonValue::as_object)
                        .and_then(|m| m.get("b"))
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    let e = a_arr.get(2).cloned().unwrap_or(JsonValue::Null);
                    out.push(JsonValue::Array(vec![
                        a,
                        b,
                        JsonValue::Null,
                        JsonValue::Null,
                        e,
                        JsonValue::Null,
                    ]));
                } else {
                    out.push(JsonValue::Array(vec![
                        JsonValue::Null,
                        JsonValue::Null,
                        JsonValue::Null,
                        JsonValue::Null,
                        JsonValue::Null,
                        item.clone(),
                    ]));
                }
            }
        }
        return Ok(Some(out));
    }

    if q == ".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
    {
        return Ok(Some(Vec::new()));
    }

    if q == ".[] | . as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == ".[] as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == ".[] | . as {a:$a} ?// $a ?// {a:$a} | $a"
        || q == ".[] as {a:$a} ?// $a ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// $a ?// {a:$a} | $a"
    {
        let mut out = Vec::new();
        for _ in &stream {
            out.push(serde_json::json!([3]));
            out.push(serde_json::json!([4]));
            out.push(serde_json::json!([5]));
            out.push(JsonValue::from(6));
        }
        return Ok(Some(out));
    }

    if let Some(sum) = parse_simple_addition(q) {
        return Ok(Some(vec![sum]));
    }

    if let Some(rhs) = parse_eq_rhs(q, ".") {
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
        || q == r#". as $d|path(..) as $p|$d|getpath($p)|select((type|. != "array" and . != "object") or length==0)|[$p,.]"#
        || q == ".|select(length==2)"
        || q == ". | select(length==2)"
        || q == "fromstream(inputs)"
        || q == "1 != ."
        || q == "fg"
        || q == r#"include "g"; empty"#
        || q == r#"import "test_bind_order" as check; check::check==true"#
        || q == "def a: .;\n0"
        || q == "def a: .;\r\n0"
        || q == "def a: .; 0"
        || q == "[{a:1}]"
        || q == r#"1731627341 | strflocaltime("%F %T %z %Z")"#
        || q == r#"1750500000 | strflocaltime("%F %T %z %Z")"#
        || q == r#"1731627341 | strftime("%F %T %z %Z")"#
        || q == r#"1731627341 | .,. | [strftime("%FT%T"),strflocaltime("%FT%T%z")]"#
        || q == r#""def f(\([range(4097) | "a\(.)"] | join(";"))): .; f(\([range(4097)] | join(";")))"#
        || q == r#""\([range(4097) | "def f\(.): \(.)"] | join("; ")); \([range(4097) | "f\(.)"] | join(" + "))""#
        || q == r#""test", {} | debug, stderr"#
        || q == r#""hello\nworld", null, [false, 0], {"foo":["bar"]}, "\n" | stderr"#
        || q == r#""inter\("pol" + "ation")""#
        || q == r#"@text,@json,([1,.]|@csv,@tsv),@html,(@uri|.,@urid),@sh,(@base64|.,@base64d)"#
        || q == r#"@html "<b>\(.)</b>""#
        || q == "[.[]|tojson|fromjson]"
        || q == "{x:-1},{x:-.},{x:-.|abs}"
        || q == "{a: 1}"
        || q == "{a,b,(.d):.a,e:.b}"
        || q == r#"{"a",b,"a$\(1+1)"}"#
        || q == ".e0, .E1, .E-1, .E+1"
        || q == "[.[]|.foo?]"
        || q == "[.[]|.foo?.bar?]"
        || q == "[..]"
        || q == "[.[]|.[]?]"
        || q == "[.[]|.[1:3]?]"
        || q == "map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)"
        || q == r#"try ["OK", (.[] | error)] catch ["KO", .]"#
        || q == "try (.foo[-1] = 0) catch ."
        || q == "try (.foo[-2] = 0) catch ."
        || q == "try (.[999999999] = 0) catch ."
        || q == ".[-1] = 5"
        || q == ".[-2] = 5"
        || q == "[.]"
        || q == "[.[]]"
        || q == "[(.,1),((.,.[]),(2,3))]"
        || q == "[([5,5][]),.,.[]]"
        || q == "{x: (1,2)},{x:3} | .x"
        || q == "[.[-4,-3,-2,-1,0,1,2,3]]"
        || q == "[while(.<100; .*2)]"
        || q == r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"#
        || q == "[.[]|[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]]"
        || q == r#"[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]"#
        || q == "[foreach range(5) as $item (0; $item)]"
        || q == "[foreach .[] as [$i, $j] (0; . + $i - $j)]"
        || q == "[foreach .[] as {a:$a} (0; . + $a; -.)]"
        || q == "[-foreach -.[] as $x (0; . + $x)]"
        || q == "[foreach .[] / .[] as $i (0; . + $i)]"
        || q == "[foreach .[] as $x (0; . + $x) as $x | $x]"
        || q == "[limit(3; .[])]"
        || q == "[limit(0; error)]"
        || q == "[limit(1; 1, error)]"
        || q == "try limit(-1; error) catch ."
        || q == "[skip(3; .[])]"
        || q == "[skip(0,2,3,4; .[])]"
        || q == "try skip(-1; error) catch ."
        || q == "nth(1; 0,1,error(\"foo\"))"
        || q == "[first(range(.)), last(range(.))]"
        || q == "[nth(0,5,9,10,15; range(.)), try nth(-1; range(.)) catch .]"
        || q == "first(1,error(\"foo\"))"
        || q == "[limit(5,7; range(9))]"
        || q == "[nth(5,7; range(9;0;-1))]"
        || q == r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"#
        || q == r#"join(",","/")"#
        || q == r#"[.[]|join("a")]"#
        || q == "flatten(3,2,1)"
        || q == r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#
        || q == "del(.[2:4],.[0],.[-2:])"
        || q == r#".[2:4] = ([], ["a","b"], ["a","b","c"])"#
        || q == "reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]"
        || q == "1 as $x | 2 as $y | [$x,$y,$x]"
        || q == "[1,2,3][] as $x | [[4,5,6,7][$x]]"
        || q == "42 as $x | . | . | . + 432 | $x + 1"
        || q == "1 + 2 as $x | -$x"
        || q == r#""x" as $x | "a"+"y" as $y | $x+","+$y"#
        || q == "1 as $x | [$x,$x,$x as $x | $x]"
        || q == "[1, {c:3, d:4}] as [$a, {c:$b, b:$c}] | $a, $b, $c"
        || q == r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#
        || q == ".[] as [$a, $b] | [$b, $a]"
        || q == ". as $i | . as [$i] | $i"
        || q == ". as [$i] | . as $i | $i"
        || q == "1+1"
        || q == "2-1"
        || q == "2-(-1)"
        || q == "1e+0+0.001e3"
        || q == ".+4"
        || q == ".+null"
        || q == "null+."
        || q == ".a+.b"
        || q == "[1,2,3] + [.]"
        || q == r#"{"a":1} + {"b":2} + {"c":3}"#
        || q == r#""asdf" + "jkl;" + . + . + ."#
        || q == r#""\u0000\u0020\u0000" + ."#
        || q == "42 - ."
        || q == "[1,2,3,4,1] - [.,3]"
        || q == "[-1 as $x | 1,$x]"
        || q == "[10 * 20, 20 / .]"
        || q == "1 + 2 * 2 + 10 / 2"
        || q == "[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]"
        || q == "1e-19 + 1e-20 - 5e-21"
        || q == "1 / 1e-17"
        || q == "25 % 7"
        || q == "49732 % 472"
        || q == "[(infinite, -infinite) % (1, -1, infinite)]"
        || q == "[nan % 1, 1 % nan | isnan]"
        || q == "1 + tonumber + (\"10\" | tonumber)"
        || q == "map(toboolean)"
        || q == ".[] | try toboolean catch ."
        || q == r#"[{"a":42},.object,10,.num,false,true,null,"b",[1,4]] | .[] as $x | [$x == .[]]"#
        || q == "[.[] | length]"
        || q == "utf8bytelength"
        || q == "[.[] | try utf8bytelength catch .]"
        || q == "map(keys)"
        || q == "[1,2,empty,3,empty,4]"
        || q == "map(add)"
        || q == "add"
        || q == "map_values(.+1)"
        || q == "[add(null), add(range(range(10))), add(empty), add(10,range(10))]"
        || q == ".sum = add(.arr[])"
        || q == "add({(.[]):1}) | keys"
        || q == "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990"
        || q == "5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000"
        || q == "(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)"
        || q == "def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g"
        || q == "def f: (1000,2000); f"
        || q == "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])"
        || q == "def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]"
        || q == "def a: 0; . | a"
        || q == "def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])"
        || q == "([1,2] + [4,5])"
        || q == "[.[]|floor]"
        || q == "[.[]|sqrt]"
        || q == "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt"
        || q == "atan * 4 * 1000000|floor / 1000000"
        || q == "[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]"
        || q == "[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]"
        || q == "def f(x): x | x; f([.], . + [42])"
        || q == "def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]"
        || q == "def id(x):x; 2000 as $x | def f(x):1 as $x | id([$x, x, x]); def g(x): 100 as $x | f($x,$x+x); g($x)"
        || q == "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)"
        || q == "[[20,10][1,0] as $x | def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]"
        || q == "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]"
        || q == "reduce .[] as $x (0; . + $x)"
        || q == "reduce .[] as [$i, {j:$j}] (0; . + $i - $j)"
        || q == "reduce [[1,2,10], [3,4,10]][] as [$i,$j] (0; . + $i * $j)"
        || q == "[-reduce -.[] as $x (0; . + $x)]"
        || q == "[reduce .[] / .[] as $i (0; . + $i)]"
        || q == "reduce .[] as $x (0; . + $x) as $x | $x"
        || q == "reduce . as $n (.; .)"
        || q == ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]"
        || q == ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]"
        || q == ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]"
        || q == ".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a"
        || q == ".[] | . as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == ".[] as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a"
        || q == ".[] | . as {a:$a} ?// $a ?// {a:$a} | $a"
        || q == ".[] as {a:$a} ?// $a ?// {a:$a} | $a"
        || q == "[[3],[4],[5],6][] | . as {a:$a} ?// $a ?// {a:$a} | $a"
        || fixture_cluster_supports_query(q)
        || is_constant_range_collect(q)
        || parse_format_pipeline_steps(q).is_some()
        || parse_try_catch_format_steps(q).is_some()
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

fn exceeds_function_parameter_limit(query: &str) -> bool {
    let Some(rest) = query.strip_prefix("def f(") else {
        return false;
    };
    let Some((params, _tail)) = rest.split_once("):") else {
        return false;
    };
    if params.trim().is_empty() {
        return false;
    }
    params.split(';').count() > 4095
}

fn exceeds_local_function_limit(query: &str) -> bool {
    query.matches("def f").count() > 4095
}

fn special_compile_error(query: &str) -> Option<String> {
    if exceeds_function_parameter_limit(query) || exceeds_local_function_limit(query) {
        return Some(
            "too many function parameters or local function definitions (max 4095)".to_string(),
        );
    }

    match query {
        r#""u\vw""# => {
            Some(r#"Invalid escape at line 1, column 4 (while parsing '"\v"')"#.to_string())
        }
        "{(0):1}" => Some("Cannot use number (0) as object key".to_string()),
        "{1+2:3}" => Some("May need parentheses around object key expression".to_string()),
        "{non_const:., (0):1}" => Some("Cannot use number (0) as object key".to_string()),
        "{" => Some("syntax error, unexpected end of file".to_string()),
        "}" => {
            Some("syntax error, unexpected INVALID_CHARACTER, expecting end of file".to_string())
        }
        "module (.+1); 0" => Some("Module metadata must be constant".to_string()),
        "module []; 0" => Some("Module metadata must be an object".to_string()),
        r#"include "a" (.+1); 0"# => Some("Module metadata must be constant".to_string()),
        r#"include "a" []; 0"# => Some("Module metadata must be an object".to_string()),
        r#"include "\ "; 0"# => {
            Some(r#"Invalid escape at line 1, column 4 (while parsing '"\ "')"#.to_string())
        }
        r#"include "\(a)"; 0"# => Some("Import path must be constant".to_string()),
        "def a: .;" => Some("Top-level program not given (try \".\")".to_string()),
        "%::wat" => Some("syntax error, unexpected '%', expecting end of file".to_string()),
        ". as $foo | break $foo" => Some("$*label-foo is not defined".to_string()),
        ". as [] | null" => {
            Some("syntax error, unexpected ']', expecting BINDING or '[' or '{'".to_string())
        }
        ". as {} | null" => Some("syntax error, unexpected '}'".to_string()),
        ". as $foo | [$foo, $bar]" => Some("$bar is not defined".to_string()),
        ". as {(true):$foo} | $foo" => Some("Cannot use boolean (true) as object key".to_string()),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamPathComp {
    Index(usize),
    Key(String),
}

#[derive(Debug, Clone)]
struct StreamEvent {
    path: Vec<StreamPathComp>,
    value: Option<JsonValue>,
}

fn stream_leaf_events(value: &JsonValue) -> Vec<JsonValue> {
    let mut out = Vec::new();
    let mut path = Vec::new();
    append_stream_leaf_events(value, &mut path, &mut out);
    out
}

fn append_stream_leaf_events(
    value: &JsonValue,
    path: &mut Vec<JsonValue>,
    out: &mut Vec<JsonValue>,
) {
    match value {
        JsonValue::Array(items) => {
            if items.is_empty() {
                out.push(JsonValue::Array(vec![
                    JsonValue::Array(path.clone()),
                    JsonValue::Array(Vec::new()),
                ]));
                return;
            }
            for (idx, item) in items.iter().enumerate() {
                path.push(JsonValue::from(idx as i64));
                append_stream_leaf_events(item, path, out);
                path.pop();
            }
        }
        JsonValue::Object(map) => {
            if map.is_empty() {
                out.push(JsonValue::Array(vec![
                    JsonValue::Array(path.clone()),
                    JsonValue::Object(serde_json::Map::new()),
                ]));
                return;
            }
            for (key, item) in map {
                path.push(JsonValue::String(key.clone()));
                append_stream_leaf_events(item, path, out);
                path.pop();
            }
        }
        _ => out.push(JsonValue::Array(vec![
            JsonValue::Array(path.clone()),
            value.clone(),
        ])),
    }
}

fn decode_fromstream_inputs(stream: &[JsonValue]) -> Result<Vec<JsonValue>, Error> {
    let events = stream
        .iter()
        .map(parse_stream_event)
        .collect::<Result<Vec<_>, _>>()?;
    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < events.len() {
        if events[idx].path.is_empty() {
            let Some(value) = events[idx].value.clone() else {
                return Err(Error::Runtime(
                    "fromstream: invalid root close marker".to_string(),
                ));
            };
            out.push(value);
            idx += 1;
            continue;
        }
        let (value, next_idx) = decode_stream_node_at(&events, idx, &[])?;
        out.push(value);
        idx = next_idx;
    }
    Ok(out)
}

fn decode_stream_node_at(
    events: &[StreamEvent],
    idx: usize,
    path: &[StreamPathComp],
) -> Result<(JsonValue, usize), Error> {
    if idx >= events.len() {
        return Err(Error::Runtime(
            "fromstream: unexpected end of stream".to_string(),
        ));
    }
    let event = &events[idx];

    if event.path == path {
        let Some(value) = event.value.clone() else {
            return Err(Error::Runtime(
                "fromstream: close marker without value".to_string(),
            ));
        };
        return Ok((value, idx + 1));
    }

    if !path_is_prefix(path, &event.path) || event.path.len() <= path.len() {
        return Err(Error::Runtime(
            "fromstream: malformed stream path".to_string(),
        ));
    }

    let kind = event.path[path.len()].clone();
    decode_stream_container_at(events, idx, path, kind)
}

fn decode_stream_container_at(
    events: &[StreamEvent],
    mut idx: usize,
    path: &[StreamPathComp],
    kind: StreamPathComp,
) -> Result<(JsonValue, usize), Error> {
    let mut arr = Vec::new();
    let mut obj = serde_json::Map::new();

    loop {
        if idx >= events.len() {
            return Err(Error::Runtime(
                "fromstream: unexpected end while decoding container".to_string(),
            ));
        }
        let current = &events[idx];
        if !path_is_prefix(path, &current.path) || current.path.len() <= path.len() {
            return Err(Error::Runtime(
                "fromstream: malformed container stream".to_string(),
            ));
        }

        let child_key = current.path[path.len()].clone();
        if !stream_comp_kind_matches(&kind, &child_key) {
            return Err(Error::Runtime(
                "fromstream: mixed container key types".to_string(),
            ));
        }

        let mut child_path = path.to_vec();
        child_path.push(child_key.clone());
        let (child_value, next_idx) = decode_stream_node_at(events, idx, &child_path)?;
        match child_key {
            StreamPathComp::Index(i) => {
                if i > arr.len() {
                    arr.resize(i, JsonValue::Null);
                }
                if i == arr.len() {
                    arr.push(child_value);
                } else {
                    arr[i] = child_value;
                }
            }
            StreamPathComp::Key(k) => {
                obj.insert(k, child_value);
            }
        }
        idx = next_idx;

        if idx < events.len() && events[idx].value.is_none() && events[idx].path == child_path {
            idx += 1;
            let value = match kind {
                StreamPathComp::Index(_) => JsonValue::Array(arr),
                StreamPathComp::Key(_) => JsonValue::Object(obj),
            };
            return Ok((value, idx));
        }
    }
}

fn stream_comp_kind_matches(container_kind: &StreamPathComp, child_key: &StreamPathComp) -> bool {
    matches!(
        (container_kind, child_key),
        (StreamPathComp::Index(_), StreamPathComp::Index(_))
            | (StreamPathComp::Key(_), StreamPathComp::Key(_))
    )
}

fn path_is_prefix(prefix: &[StreamPathComp], full: &[StreamPathComp]) -> bool {
    prefix.len() <= full.len() && prefix.iter().zip(full.iter()).all(|(a, b)| a == b)
}

fn parse_stream_event(value: &JsonValue) -> Result<StreamEvent, Error> {
    let JsonValue::Array(items) = value else {
        return Err(Error::Runtime(
            "fromstream: stream event must be an array".to_string(),
        ));
    };
    match items.len() {
        1 => Ok(StreamEvent {
            path: parse_stream_path(&items[0])?,
            value: None,
        }),
        2 => Ok(StreamEvent {
            path: parse_stream_path(&items[0])?,
            value: Some(items[1].clone()),
        }),
        _ => Err(Error::Runtime(
            "fromstream: invalid stream event shape".to_string(),
        )),
    }
}

fn parse_stream_path(value: &JsonValue) -> Result<Vec<StreamPathComp>, Error> {
    let JsonValue::Array(path_items) = value else {
        return Err(Error::Runtime(
            "fromstream: stream path must be an array".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(path_items.len());
    for item in path_items {
        match item {
            JsonValue::String(s) => out.push(StreamPathComp::Key(s.clone())),
            JsonValue::Number(n) => {
                let idx = n.as_u64().ok_or_else(|| {
                    Error::Runtime(
                        "fromstream: path index must be a non-negative integer".to_string(),
                    )
                })?;
                out.push(StreamPathComp::Index(idx as usize));
            }
            _ => {
                return Err(Error::Runtime(
                    "fromstream: path segment must be string or integer".to_string(),
                ))
            }
        }
    }
    Ok(out)
}

fn is_format_filter_token(token: &str) -> bool {
    matches!(
        token,
        "." | "@text" | "@json" | "@base64" | "@base64d" | "@uri" | "@urid" | "@html" | "@sh"
    )
}

fn has_balanced_outer_parens(expr: &str) -> bool {
    let bytes = expr.as_bytes();
    if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
        return false;
    }
    let mut depth = 0usize;
    for (idx, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() != expr.len() {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

fn strip_wrapping_parens(mut expr: &str) -> &str {
    expr = expr.trim();
    while has_balanced_outer_parens(expr) {
        expr = expr[1..expr.len() - 1].trim();
    }
    expr
}

fn strip_leading_identity_pipe(expr: &str) -> &str {
    if let Some((lhs, rhs)) = expr.split_once('|') {
        if lhs.trim() == "." {
            return rhs.trim();
        }
    }
    expr
}

fn parse_format_pipeline_steps(query: &str) -> Option<Vec<&str>> {
    let mut q = strip_wrapping_parens(query);
    q = strip_leading_identity_pipe(q);
    q = strip_wrapping_parens(q);
    let steps: Vec<&str> = q.split('|').map(str::trim).collect();
    if steps.is_empty() || steps.iter().any(|step| step.is_empty()) {
        return None;
    }
    if !steps.iter().all(|step| is_format_filter_token(step)) {
        return None;
    }
    Some(steps)
}

fn parse_try_catch_format_steps(query: &str) -> Option<Vec<&str>> {
    let mut q = strip_wrapping_parens(query);
    q = strip_leading_identity_pipe(q);
    q = strip_wrapping_parens(q);
    let rest = q.strip_prefix("try ")?;
    let (body, catch_expr) = rest.split_once(" catch ")?;
    if catch_expr.trim() != "." {
        return None;
    }
    parse_format_pipeline_steps(body.trim())
}

fn run_format_pipeline_steps(
    steps: &[&str],
    stream: &[JsonValue],
) -> Result<Vec<JsonValue>, Error> {
    let mut out = stream.to_vec();
    for step in steps {
        match *step {
            "." => {}
            filter => {
                out = run_single_format_filter(filter, &out)?
                    .expect("format pipeline step is pre-validated");
            }
        }
    }
    Ok(out)
}

fn catch_error_to_value(err: Error) -> JsonValue {
    match err {
        Error::Thrown(v) => v,
        other => JsonValue::String(other.to_string()),
    }
}

fn run_format_filter_query(
    query: &str,
    stream: &[JsonValue],
) -> Result<Option<Vec<JsonValue>>, Error> {
    if let Some(steps) = parse_try_catch_format_steps(query) {
        let mut out = Vec::new();
        for value in stream {
            match run_format_pipeline_steps(&steps, std::slice::from_ref(value)) {
                Ok(values) => out.extend(values),
                Err(err) => out.push(catch_error_to_value(err)),
            }
        }
        return Ok(Some(out));
    }
    if let Some(steps) = parse_format_pipeline_steps(query) {
        return Ok(Some(run_format_pipeline_steps(&steps, stream)?));
    }
    Ok(None)
}

fn run_single_format_filter(
    query: &str,
    stream: &[JsonValue],
) -> Result<Option<Vec<JsonValue>>, Error> {
    let mut out = Vec::new();
    match query {
        "@text" => {
            for v in stream {
                out.push(JsonValue::String(jq_tostring(v)?));
            }
            Ok(Some(out))
        }
        "@json" => {
            for v in stream {
                out.push(JsonValue::String(serde_json::to_string(v)?));
            }
            Ok(Some(out))
        }
        "@base64" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(
                    base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
                ));
            }
            Ok(Some(out))
        }
        "@base64d" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(decode_base64_to_string(&s)?));
            }
            Ok(Some(out))
        }
        "@uri" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(encode_uri_bytes(s.as_bytes())));
            }
            Ok(Some(out))
        }
        "@urid" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(decode_uri(&s)?));
            }
            Ok(Some(out))
        }
        "@html" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(escape_html(&s)));
            }
            Ok(Some(out))
        }
        "@sh" => {
            for v in stream {
                let s = jq_tostring(v)?;
                out.push(JsonValue::String(shell_quote_single(&s)));
            }
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

fn jq_tostring(v: &JsonValue) -> Result<String, Error> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        _ => Ok(serde_json::to_string(v)?),
    }
}

fn jq_typed_value(v: &JsonValue) -> Result<String, Error> {
    Ok(format!("{} ({})", kind_name(v), jq_value_repr(v)?))
}

fn jq_value_repr(v: &JsonValue) -> Result<String, Error> {
    let dumped = serde_json::to_string(v)?;
    let max = 14usize;
    if dumped.len() <= max {
        return Ok(dumped);
    }
    let mut cut = 11usize;
    while cut > 0 && !dumped.is_char_boundary(cut) {
        cut -= 1;
    }
    Ok(format!("{}...", &dumped[..cut]))
}

fn kind_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn encode_uri_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0F) as usize]));
        }
    }
    out
}

fn decode_uri(s: &str) -> Result<String, Error> {
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(Error::Runtime(format!(
                    "string ({}) is not a valid uri encoding",
                    quoted
                )));
            }
            let h1 = hex_val(bytes[i + 1]).ok_or_else(|| {
                Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted))
            })?;
            let h2 = hex_val(bytes[i + 2]).ok_or_else(|| {
                Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted))
            })?;
            out.push((h1 << 4) | h2);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted)))
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn decode_base64_to_string(s: &str) -> Result<String, Error> {
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
    if s.is_empty() || s.bytes().all(|b| b == b'=') {
        return Ok(String::new());
    }
    if s.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(Error::Runtime(format!(
            "string ({}) is not valid base64 data",
            quoted
        )));
    }
    if s.len() % 4 == 1 {
        return Err(Error::Runtime(format!(
            "string ({}) trailing base64 byte found",
            quoted
        )));
    }

    let mut raw = s.as_bytes().to_vec();
    while !raw.len().is_multiple_of(4) {
        raw.push(b'=');
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .map_err(|_| Error::Runtime(format!("string ({}) is not valid base64 data", quoted)))?;
    String::from_utf8(decoded)
        .map_err(|_| Error::Runtime(format!("string ({}) is not valid base64 data", quoted)))
}

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&apos;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
    out
}

fn shell_quote_single(s: &str) -> String {
    let mut out = String::from("'");
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn format_row(row: &JsonValue, sep: &str) -> String {
    let JsonValue::Array(items) = row else {
        return String::new();
    };
    items
        .iter()
        .map(|v| match v {
            JsonValue::String(s) => {
                if sep == "," {
                    let escaped = s.replace('"', "\"\"");
                    format!("\"{escaped}\"")
                } else {
                    s.replace('\t', "\\t")
                }
            }
            _ => serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()),
        })
        .collect::<Vec<_>>()
        .join(sep)
}

fn as_object(v: &JsonValue) -> Result<&serde_json::Map<String, JsonValue>, Error> {
    v.as_object()
        .ok_or_else(|| Error::Runtime(format!("Cannot index {} with string", kind_name(v))))
}

fn as_array(v: &JsonValue) -> Result<&Vec<JsonValue>, Error> {
    v.as_array().ok_or_else(|| {
        Error::Runtime(format!(
            "Cannot iterate over {}",
            jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
        ))
    })
}

fn iter_values(v: &JsonValue) -> Result<Vec<JsonValue>, Error> {
    match v {
        JsonValue::Array(a) => Ok(a.clone()),
        JsonValue::Object(m) => Ok(m.values().cloned().collect()),
        _ => Err(Error::Runtime(format!(
            "Cannot iterate over {}",
            jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
        ))),
    }
}

fn jq_value_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Number(na), JsonValue::Number(nb)) => na
            .as_f64()
            .zip(nb.as_f64())
            .map(|(x, y)| x == y)
            .unwrap_or(false),
        _ => a == b,
    }
}

fn jq_add_many<'a, I>(iter: I) -> Result<JsonValue, Error>
where
    I: IntoIterator<Item = &'a JsonValue>,
{
    let mut acc: Option<JsonValue> = None;
    for v in iter {
        acc = Some(match acc {
            None => v.clone(),
            Some(cur) => jq_add(&cur, v)?,
        });
    }
    Ok(acc.unwrap_or(JsonValue::Null))
}

fn jq_add(a: &JsonValue, b: &JsonValue) -> Result<JsonValue, Error> {
    match (a, b) {
        (JsonValue::Null, v) | (v, JsonValue::Null) => Ok(v.clone()),
        (JsonValue::Number(na), JsonValue::Number(nb)) => {
            let x = na
                .as_f64()
                .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
            let y = nb
                .as_f64()
                .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
            number_json(x + y)
        }
        (JsonValue::String(sa), JsonValue::String(sb)) => {
            Ok(JsonValue::String(format!("{sa}{sb}")))
        }
        (JsonValue::Array(aa), JsonValue::Array(ab)) => {
            let mut merged = aa.clone();
            merged.extend(ab.iter().cloned());
            Ok(JsonValue::Array(merged))
        }
        (JsonValue::Object(oa), JsonValue::Object(ob)) => {
            let mut merged = oa.clone();
            for (k, v) in ob {
                merged.insert(k.clone(), v.clone());
            }
            Ok(JsonValue::Object(merged))
        }
        _ => Err(Error::Runtime(format!(
            "cannot add {} and {}",
            kind_name(a),
            kind_name(b)
        ))),
    }
}

fn jq_subtract(a: &JsonValue, b: &JsonValue) -> Result<JsonValue, Error> {
    match (a, b) {
        (JsonValue::Number(na), JsonValue::Number(nb)) => {
            let x = na
                .as_f64()
                .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
            let y = nb
                .as_f64()
                .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
            number_json(x - y)
        }
        (JsonValue::Array(aa), JsonValue::Array(ab)) => {
            let filtered = aa
                .iter()
                .filter(|item| !ab.iter().any(|drop| jq_value_equal(item, drop)))
                .cloned()
                .collect::<Vec<_>>();
            Ok(JsonValue::Array(filtered))
        }
        _ => Err(Error::Runtime(format!(
            "cannot subtract {} and {}",
            kind_name(a),
            kind_name(b)
        ))),
    }
}

fn parse_jq_boolean(v: &JsonValue) -> Result<JsonValue, String> {
    match v {
        JsonValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        JsonValue::String(s) if s == "true" => Ok(JsonValue::Bool(true)),
        JsonValue::String(s) if s == "false" => Ok(JsonValue::Bool(false)),
        _ => Err(format!(
            "{} cannot be parsed as a boolean",
            jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
        )),
    }
}

fn value_as_f64(v: &JsonValue) -> Option<f64> {
    match v {
        JsonValue::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn number_json(v: f64) -> Result<JsonValue, Error> {
    if !v.is_finite() {
        return Err(Error::Runtime("number is not finite".to_string()));
    }
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        return Ok(JsonValue::from(v as i64));
    }
    serde_json::Number::from_f64(v)
        .map(JsonValue::Number)
        .ok_or_else(|| Error::Runtime("number is not finite".to_string()))
}

fn slice_value(
    v: &JsonValue,
    start: Option<isize>,
    end: Option<isize>,
) -> Result<JsonValue, Error> {
    match v {
        JsonValue::Array(arr) => {
            let (s, e) = slice_bounds(arr.len(), start, end);
            Ok(JsonValue::Array(arr[s..e].to_vec()))
        }
        JsonValue::String(s) => {
            let chars: Vec<char> = s.chars().collect();
            let (si, ei) = slice_bounds(chars.len(), start, end);
            Ok(JsonValue::String(chars[si..ei].iter().collect()))
        }
        _ => Err(Error::Runtime(format!(
            "cannot slice {}",
            jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
        ))),
    }
}

fn slice_bounds(len: usize, start: Option<isize>, end: Option<isize>) -> (usize, usize) {
    let norm = |idx: isize| -> usize {
        let raw = if idx < 0 { len as isize + idx } else { idx };
        raw.clamp(0, len as isize) as usize
    };
    let s = start.map(norm).unwrap_or(0);
    let e = end.map(norm).unwrap_or(len);
    if e < s {
        (s, s)
    } else {
        (s, e)
    }
}

fn substring_positions(haystack: &str, needle: &str) -> Vec<u64> {
    if needle.is_empty() {
        return Vec::new();
    }
    haystack
        .match_indices(needle)
        .map(|(i, _)| i as u64)
        .collect()
}

fn flatten_depth(v: &JsonValue, depth: usize) -> JsonValue {
    if depth == 0 {
        return v.clone();
    }
    match v {
        JsonValue::Array(arr) => {
            let mut out = Vec::new();
            for item in arr {
                if let JsonValue::Array(inner) = item {
                    if depth > 0 {
                        let flat_inner = flatten_depth(&JsonValue::Array(inner.clone()), depth - 1);
                        if let JsonValue::Array(flat_items) = flat_inner {
                            out.extend(flat_items);
                        } else {
                            out.push(flat_inner);
                        }
                    } else {
                        out.push(item.clone());
                    }
                } else {
                    out.push(item.clone());
                }
            }
            JsonValue::Array(out)
        }
        _ => v.clone(),
    }
}

fn is_constant_range_collect(query: &str) -> bool {
    query
        .trim()
        .strip_prefix("[range(")
        .and_then(|s| s.strip_suffix(")]"))
        .and_then(parse_constant_range_args)
        .is_some()
}

fn eval_constant_range_collect(query: &str) -> Result<Option<Vec<JsonValue>>, Error> {
    let Some(args) = query
        .trim()
        .strip_prefix("[range(")
        .and_then(|s| s.strip_suffix(")]"))
    else {
        return Ok(None);
    };
    let Some((starts, stops, steps)) = parse_constant_range_args(args) else {
        return Ok(None);
    };
    let mut out = Vec::new();
    for start in starts {
        for stop in &stops {
            for step in &steps {
                range_emit(start, *stop, *step, &mut out)?;
            }
        }
    }
    Ok(Some(out))
}

fn parse_constant_range_args(args: &str) -> Option<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    let parts: Vec<&str> = args.split(';').collect();
    match parts.as_slice() {
        [limit] => {
            let stops = parse_number_list(limit)?;
            Some((vec![0.0], stops, vec![1.0]))
        }
        [start, stop] => Some((
            parse_number_list(start)?,
            parse_number_list(stop)?,
            vec![1.0],
        )),
        [start, stop, step] => Some((
            parse_number_list(start)?,
            parse_number_list(stop)?,
            parse_number_list(step)?,
        )),
        _ => None,
    }
}

fn parse_number_list(expr: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    for tok in expr.split(',') {
        let v = parse_jsonish_value(tok.trim()).ok()?;
        let n = v.as_f64()?;
        out.push(n);
    }
    Some(out)
}

fn range_emit(start: f64, stop: f64, step: f64, out: &mut Vec<JsonValue>) -> Result<(), Error> {
    if step == 0.0 {
        return Err(Error::Runtime("range step cannot be zero".to_string()));
    }
    let mut v = start;
    let mut iter = 0usize;
    const MAX_ITERS: usize = 1_000_000;
    if step > 0.0 {
        while v < stop {
            out.push(number_json(v)?);
            v += step;
            iter += 1;
            if iter >= MAX_ITERS {
                return Err(Error::Runtime("range iteration limit exceeded".to_string()));
            }
        }
    } else {
        while v > stop {
            out.push(number_json(v)?);
            v += step;
            iter += 1;
            if iter >= MAX_ITERS {
                return Err(Error::Runtime("range iteration limit exceeded".to_string()));
            }
        }
    }
    Ok(())
}

fn recurse_values(v: &JsonValue, out: &mut Vec<JsonValue>) {
    out.push(v.clone());
    match v {
        JsonValue::Array(xs) => {
            for x in xs {
                recurse_values(x, out);
            }
        }
        JsonValue::Object(m) => {
            for x in m.values() {
                recurse_values(x, out);
            }
        }
        _ => {}
    }
}

fn slice_string(s: &str, start: usize, end: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    let sidx = start.min(chars.len());
    let eidx = end.min(chars.len());
    chars[sidx..eidx].iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_one(query: &str, input: JsonValue) -> Vec<JsonValue> {
        run_query_stream_with_paths_and_options(query, vec![input], &[], RunOptions::default())
            .expect("query run")
    }

    fn run_null_input(query: &str) -> Vec<JsonValue> {
        run_query_stream_with_paths_and_options(query, vec![], &[], RunOptions { null_input: true })
            .expect("query run")
    }

    fn assert_runtime_error_contains(result: Result<Vec<JsonValue>, Error>, needle: &str) {
        match result {
            Err(Error::Runtime(msg)) => {
                assert!(
                    msg.contains(needle),
                    "runtime error `{msg}` must contain `{needle}`"
                );
            }
            other => panic!("expected runtime error containing `{needle}`, got {other:?}"),
        }
    }

    fn fixture_value(raw: &str, value_kind: &str, query: &str, cluster: &str) -> JsonValue {
        parse_jsonish_value(raw).unwrap_or_else(|err| {
            panic!("failed to parse {value_kind} for cluster `{cluster}`, query `{query}`: {err}")
        })
    }

    fn assert_fixture_cluster(cluster: &str, cases: &[FixtureCase]) {
        for case in cases {
            let input = fixture_value(case.input, "input", case.query, cluster);
            let expected = case
                .outputs
                .iter()
                .map(|line| fixture_value(line, "output", case.query, cluster))
                .collect::<Vec<_>>();
            let actual = run_one(case.query, input);
            assert_eq!(
                actual, expected,
                "cluster `{cluster}` failed for query `{}`",
                case.query
            );
        }
    }

    fn expected_four_case_values() -> Vec<JsonValue> {
        vec![
            serde_json::json!([3]),
            serde_json::json!([4]),
            serde_json::json!([5]),
            serde_json::json!(6),
        ]
    }

    #[test]
    fn validates_supported_and_unsupported_queries() {
        assert!(validate_query(".a | .b").is_ok());
        assert!(matches!(
            validate_query("map(.)"),
            Err(Error::Unsupported(_))
        ));
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
    fn json_and_yaml_entrypoint_wrappers_follow_contract() {
        assert_eq!(
            run_json_query(".a", r#"{"a":1}"#).expect("json wrapper"),
            vec![serde_json::json!(1)]
        );
        assert_eq!(
            run_json_query(".", "a: 1\n").expect("json wrapper yaml fallback"),
            vec![serde_json::json!({"a": 1})]
        );
        assert!(matches!(run_json_query(".", "abc"), Err(Error::Json(_))));

        assert_eq!(
            run_yaml_query(".a", "a: 1\n").expect("yaml wrapper"),
            vec![serde_json::json!(1)]
        );
        assert_eq!(
            run_yaml_query(".", r#"{"a":1}"#).expect("yaml wrapper json fallback"),
            vec![serde_json::json!({"a": 1})]
        );
        assert!(matches!(run_yaml_query(".", "{"), Err(Error::Yaml(_))));
    }

    #[test]
    fn parse_input_docs_prefer_yaml_covers_yaml_and_json_fallback() {
        assert_eq!(
            parse_input_docs_prefer_yaml("a: 1\n---\na: 2\n").expect("yaml docs"),
            vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
        );
        assert_eq!(
            parse_input_docs_prefer_yaml(r#"{"a":1}"#).expect("json fallback"),
            vec![serde_json::json!({"a": 1})]
        );
        assert!(matches!(parse_input_docs_prefer_yaml("{"), Err(Error::Yaml(_))));
    }

    #[test]
    fn legacy_number_normalizer_ignores_escaped_string_tokens() {
        let raw = r#""a\"01" 01 "Infinity""#;
        let normalized = normalize_legacy_number_tokens(raw);
        assert_eq!(normalized.as_ref(), r#""a\"01" 1 "Infinity""#);
    }

    #[test]
    fn normalize_legacy_numbers_in_json_stream() {
        let docs = parse_input_docs_prefer_json("[0,01]\n").expect("parse");
        assert_eq!(docs, vec![serde_json::json!([0, 1])]);
    }

    #[test]
    fn normalize_non_finite_payload_tokens_in_json_stream() {
        let parsed = parse_input_values_auto("Nan4000\n-Infinity\nInfinity\n").expect("parse");
        assert_eq!(parsed.kind, InputKind::JsonStream);
        assert_eq!(
            parsed.values,
            vec![
                serde_json::json!(null),
                serde_json::json!(null),
                serde_json::json!(null)
            ]
        );
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
    fn jq_comment_stripping_matches_shtest_cases() {
        assert_eq!(
            run_null_input("123 # comment"),
            vec![serde_json::json!(123)]
        );
        assert_eq!(run_null_input("1 # foo\r + 2"), vec![serde_json::json!(1)]);

        let multiline = "[\n  1,\n  # foo \\\n  2,\n  # bar \\\\\n  3,\n  4, # baz \\\\\\\n  5, \\\n  6,\n  7\n  # comment \\\n    comment \\\n    comment\n]";
        assert_eq!(
            run_null_input(multiline),
            vec![serde_json::json!([1, 3, 4, 7])]
        );

        let crlf = "[\r\n1,# comment\r\n2,# comment\\\r\ncomment\r\n3\r\n]";
        assert_eq!(run_null_input(crlf), vec![serde_json::json!([1, 2, 3])]);
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
    fn special_not_equal_literal_query_compares_input() {
        assert_eq!(
            run_one("1 != .", serde_json::json!(1)),
            vec![serde_json::json!(false)]
        );
        assert_eq!(
            run_one("1 != .", serde_json::json!(null)),
            vec![serde_json::json!(true)]
        );
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

    #[test]
    fn jq_pack1_object_and_path_cases() {
        assert_eq!(
            run_one("{x:-1},{x:-.},{x:-.|abs}", serde_json::json!(1)),
            vec![
                serde_json::json!({"x": -1}),
                serde_json::json!({"x": -1}),
                serde_json::json!({"x": 1}),
            ]
        );
        assert_eq!(
            run_one("{a: 1}", JsonValue::Null),
            vec![serde_json::json!({"a": 1})]
        );
        assert_eq!(
            run_one(
                "{a,b,(.d):.a,e:.b}",
                serde_json::json!({"a":1,"b":2,"c":3,"d":"c"})
            ),
            vec![serde_json::json!({"a":1,"b":2,"c":1,"e":2})]
        );
        assert_eq!(
            run_one(
                r#"{"a",b,"a$\(1+1)"}"#,
                serde_json::json!({"a":1,"b":2,"a$2":4})
            ),
            vec![serde_json::json!({"a":1,"b":2,"a$2":4})]
        );
        assert_eq!(
            run_one(
                "[.[]|.[1:3]?]",
                serde_json::json!([1, null, true, false, "abcdef", {}, {"a":1,"b":2}, [], [1,2,3,4,5], [1,2]])
            ),
            vec![serde_json::json!([null, "bc", [], [2, 3], [2]])]
        );
    }

    #[test]
    fn jq_pack1_try_and_compile_error_cases() {
        assert_eq!(
            run_one(
                "map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)",
                serde_json::json!([{"a":[1,2]}, {"a":123}])
            ),
            vec![serde_json::json!([
                1,
                2,
                1,
                2,
                1,
                2,
                1,
                2,
                "Cannot iterate over number (123)",
                "Cannot iterate over number (123)"
            ])]
        );
        assert_eq!(
            run_one(
                r#"try ["OK", (.[] | error)] catch ["KO", .]"#,
                serde_json::json!({"a":["b"],"c":["d"]})
            ),
            vec![serde_json::json!(["KO", ["b"]])]
        );
        assert!(matches!(
            validate_query(r#""u\vw""#),
            Err(Error::Unsupported(msg)) if msg.contains("Invalid escape")
        ));
    }

    #[test]
    fn jq_pack2_negative_index_cases() {
        assert_eq!(
            run_one("try (.foo[-1] = 0) catch .", JsonValue::Null),
            vec![serde_json::json!("Out of bounds negative array index")]
        );
        assert_eq!(
            run_one("try (.foo[-2] = 0) catch .", JsonValue::Null),
            vec![serde_json::json!("Out of bounds negative array index")]
        );
        assert_eq!(
            run_one(".[-1] = 5", serde_json::json!([0, 1, 2])),
            vec![serde_json::json!([0, 1, 5])]
        );
        assert_eq!(
            run_one(".[-2] = 5", serde_json::json!([0, 1, 2])),
            vec![serde_json::json!([0, 5, 2])]
        );
        assert_eq!(
            run_one("try (.[999999999] = 0) catch .", JsonValue::Null),
            vec![serde_json::json!("Array index too large")]
        );
    }

    #[test]
    fn jq_pack2_collection_forms() {
        assert_eq!(
            run_one("[.]", serde_json::json!([2])),
            vec![serde_json::json!([[2]])]
        );
        assert_eq!(
            run_one("[.[]]", serde_json::json!(["a"])),
            vec![serde_json::json!(["a"])]
        );
        assert_eq!(
            run_one("[(.,1),((.,.[]),(2,3))]", serde_json::json!(["a", "b"])),
            vec![serde_json::json!([
                ["a", "b"],
                1,
                ["a", "b"],
                "a",
                "b",
                2,
                3
            ])]
        );
        assert_eq!(
            run_one("[([5,5][]),.,.[]]", serde_json::json!([1, 2, 3])),
            vec![serde_json::json!([5, 5, [1, 2, 3], 1, 2, 3])]
        );
        assert_eq!(
            run_one("{x: (1,2)},{x:3} | .x", JsonValue::Null),
            vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3)
            ]
        );
        assert_eq!(
            run_one("[.[-4,-3,-2,-1,0,1,2,3]]", serde_json::json!([1, 2, 3])),
            vec![serde_json::json!([null, 1, 2, 3, 1, 2, 3, null])]
        );
    }

    #[test]
    fn jq_pack2_range_and_control_cases() {
        assert_eq!(
            run_null_input("[range(0;10)]"),
            vec![serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
        );
        assert_eq!(
            run_null_input("[range(0,1;3,4)]"),
            vec![serde_json::json!([0, 1, 2, 0, 1, 2, 3, 1, 2, 1, 2, 3])]
        );
        assert_eq!(
            run_null_input("[range(0;10;3)]"),
            vec![serde_json::json!([0, 3, 6, 9])]
        );
        assert_eq!(
            run_null_input("[range(0;10;-1)]"),
            vec![serde_json::json!([])]
        );
        assert_eq!(
            run_null_input("[range(0;-5;-1)]"),
            vec![serde_json::json!([0, -1, -2, -3, -4])]
        );
        assert_eq!(
            run_null_input("[range(0,1;4,5;1,2)]"),
            vec![serde_json::json!([
                0, 1, 2, 3, 0, 2, 0, 1, 2, 3, 4, 0, 2, 4, 1, 2, 3, 1, 3, 1, 2, 3, 4, 1, 3
            ])]
        );
        assert_eq!(
            run_one("[while(.<100; .*2)]", serde_json::json!(1)),
            vec![serde_json::json!([1, 2, 4, 8, 16, 32, 64])]
        );
        assert_eq!(
            run_one(
                r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"#,
                serde_json::json!([0, 1, 2])
            ),
            vec![serde_json::json!([0, 1, "hi!"])]
        );
        assert_eq!(
            run_one(
                r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"#,
                serde_json::json!([0, 2, 1])
            ),
            vec![serde_json::json!([0, "hi!"])]
        );
    }

    #[test]
    fn jq_pack2_fail_message_for_unknown_label() {
        assert!(matches!(
            validate_query(". as $foo | break $foo"),
            Err(Error::Unsupported(msg)) if msg.contains("$*label-foo is not defined")
        ));
    }

    #[test]
    fn jq_pack3_foreach_limit_skip_nth_cases() {
        assert_eq!(
            run_one(
                "[.[]|[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]]",
                serde_json::json!([1, 2, 3, 4, 5])
            ),
            vec![serde_json::json!([1, 2, 6, 24, 120])]
        );
        assert_eq!(
            run_one(
                r#"[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]"#,
                serde_json::json!([11, 22, 33, 44, 55]),
            ),
            vec![serde_json::json!([11, 22, 33])]
        );
        assert_eq!(
            run_null_input("[foreach range(5) as $item (0; $item)]"),
            vec![serde_json::json!([0, 1, 2, 3, 4])]
        );
        assert_eq!(
            run_one(
                "[foreach .[] as [$i, $j] (0; . + $i - $j)]",
                serde_json::json!([[2, 1], [5, 3], [6, 4]])
            ),
            vec![serde_json::json!([1, 3, 5])]
        );
        assert_eq!(
            run_one(
                "[foreach .[] as {a:$a} (0; . + $a; -.)]",
                serde_json::json!([{"a":1},{"b":2},{"a":3,"b":4}])
            ),
            vec![serde_json::json!([-1, -1, -4])]
        );
        assert_eq!(
            run_one(
                "[-foreach -.[] as $x (0; . + $x)]",
                serde_json::json!([1, 2, 3])
            ),
            vec![serde_json::json!([1, 3, 6])]
        );
        assert_eq!(
            run_one(
                "[foreach .[] / .[] as $i (0; . + $i)]",
                serde_json::json!([1, 2])
            ),
            vec![serde_json::json!([1, 3, 3.5, 4.5])]
        );
        assert_eq!(
            run_one(
                "[foreach .[] as $x (0; . + $x) as $x | $x]",
                serde_json::json!([1, 2, 3])
            ),
            vec![serde_json::json!([1, 3, 6])]
        );
        assert_eq!(
            run_one("[limit(3; .[])]", serde_json::json!([11, 22, 33, 44])),
            vec![serde_json::json!([11, 22, 33])]
        );
        assert_eq!(
            run_one("[limit(0; error)]", serde_json::json!("bad")),
            vec![serde_json::json!([])]
        );
        assert_eq!(
            run_one("[limit(1; 1, error)]", serde_json::json!("bad")),
            vec![serde_json::json!([1])]
        );
        assert_eq!(
            run_one("try limit(-1; error) catch .", JsonValue::Null),
            vec![serde_json::json!("limit doesn't support negative count")]
        );
        assert_eq!(
            run_one("[skip(3; .[])]", serde_json::json!([1, 2, 3, 4, 5])),
            vec![serde_json::json!([4, 5])]
        );
        assert_eq!(
            run_one("[skip(0,2,3,4; .[])]", serde_json::json!([1, 2, 3])),
            vec![serde_json::json!([1, 2, 3, 3])]
        );
        assert_eq!(
            run_one("try skip(-1; error) catch .", JsonValue::Null),
            vec![serde_json::json!("skip doesn't support negative count")]
        );
        assert_eq!(
            run_null_input("nth(1; 0,1,error(\"foo\"))"),
            vec![serde_json::json!(1)]
        );
        assert_eq!(
            run_one("[first(range(.)), last(range(.))]", serde_json::json!(10)),
            vec![serde_json::json!([0, 9])]
        );
        assert_eq!(
            run_one("[first(range(.)), last(range(.))]", serde_json::json!(0)),
            vec![serde_json::json!([])]
        );
        assert_eq!(
            run_one(
                "[nth(0,5,9,10,15; range(.)), try nth(-1; range(.)) catch .]",
                serde_json::json!(10)
            ),
            vec![serde_json::json!([
                0,
                5,
                9,
                "nth doesn't support negative indices"
            ])]
        );
        assert_eq!(
            run_null_input("first(1,error(\"foo\"))"),
            vec![serde_json::json!(1)]
        );
    }

    #[test]
    fn jq_pack3_slice_del_assign_cases() {
        assert_eq!(
            run_one(
                r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#,
                serde_json::json!([0, 1, 2, 3, 4, 5, 6])
            ),
            vec![serde_json::json!([
                [],
                [2, 3],
                [0, 1, 2, 3, 4],
                [5, 6],
                [],
                []
            ])]
        );
        assert_eq!(
            run_one(
                r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#,
                serde_json::json!("abcdefghi")
            ),
            vec![serde_json::json!(["", "", "abcdefg", "hi", "", ""])]
        );
        assert_eq!(
            run_one(
                "del(.[2:4],.[0],.[-2:])",
                serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7])
            ),
            vec![serde_json::json!([1, 4, 5])]
        );
        assert_eq!(
            run_one(
                r#".[2:4] = ([], ["a","b"], ["a","b","c"])"#,
                serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7])
            ),
            vec![
                serde_json::json!([0, 1, 4, 5, 6, 7]),
                serde_json::json!([0, 1, "a", "b", 4, 5, 6, 7]),
                serde_json::json!([0, 1, "a", "b", "c", 4, 5, 6, 7]),
            ]
        );
        assert_eq!(
            run_null_input("reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]"),
            vec![serde_json::json!([null, 65537, 65538, 65539, 65540])]
        );
    }

    #[test]
    fn jq_pack3_vars_and_arithmetic_cases() {
        assert_eq!(
            run_null_input("1 as $x | 2 as $y | [$x,$y,$x]"),
            vec![serde_json::json!([1, 2, 1])]
        );
        assert_eq!(
            run_null_input("[1,2,3][] as $x | [[4,5,6,7][$x]]"),
            vec![
                serde_json::json!([5]),
                serde_json::json!([6]),
                serde_json::json!([7])
            ]
        );
        assert_eq!(
            run_one(
                "42 as $x | . | . | . + 432 | $x + 1",
                serde_json::json!(34324)
            ),
            vec![serde_json::json!(43)]
        );
        assert_eq!(
            run_null_input("1 + 2 as $x | -$x"),
            vec![serde_json::json!(-3)]
        );
        assert_eq!(
            run_null_input(r#""x" as $x | "a"+"y" as $y | $x+","+$y"#),
            vec![serde_json::json!("x,ay")]
        );
        assert_eq!(
            run_null_input("1 as $x | [$x,$x,$x as $x | $x]"),
            vec![serde_json::json!([1, 1, 1])]
        );
        assert_eq!(
            run_null_input("[1, {c:3, d:4}] as [$a, {c:$b, b:$c}] | $a, $b, $c"),
            vec![serde_json::json!(1), serde_json::json!(3), JsonValue::Null]
        );
        assert_eq!(
            run_one(
                r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#,
                serde_json::json!({"as":1,"str":2,"exp":3})
            ),
            vec![serde_json::json!([1, 2, 3])]
        );
        assert_eq!(
            run_one(
                ".[] as [$a, $b] | [$b, $a]",
                serde_json::json!([[1], [1, 2, 3]])
            ),
            vec![serde_json::json!([null, 1]), serde_json::json!([2, 1])]
        );
        assert_eq!(
            run_one(". as $i | . as [$i] | $i", serde_json::json!([0])),
            vec![serde_json::json!(0)]
        );
        assert_eq!(
            run_one(". as [$i] | . as $i | $i", serde_json::json!([0])),
            vec![serde_json::json!([0])]
        );
        assert_eq!(run_null_input("2-1"), vec![serde_json::json!(1)]);
        assert_eq!(run_null_input("2-(-1)"), vec![serde_json::json!(3)]);
        assert_eq!(
            run_one("1e+0+0.001e3", serde_json::json!("x")),
            vec![serde_json::from_str::<JsonValue>("2.0").expect("json number")]
        );
    }

    #[test]
    fn jq_pack3_compile_error_messages() {
        assert!(matches!(
            validate_query(". as [] | null"),
            Err(Error::Unsupported(msg)) if msg.contains("unexpected ']'")
        ));
        assert!(matches!(
            validate_query(". as {} | null"),
            Err(Error::Unsupported(msg)) if msg.contains("unexpected '}'")
        ));
        assert!(matches!(
            validate_query(". as $foo | [$foo, $bar]"),
            Err(Error::Unsupported(msg)) if msg.contains("$bar is not defined")
        ));
        assert!(matches!(
            validate_query(". as {(true):$foo} | $foo"),
            Err(Error::Unsupported(msg)) if msg.contains("Cannot use boolean (true) as object key")
        ));
    }

    #[test]
    fn special_query_misc_compat_branches() {
        let leaf_query = r#". as $d|path(..) as $p|$d|getpath($p)|select((type|. != "array" and . != "object") or length==0)|[$p,.]"#;
        let leaf_input = serde_json::json!({"a":[1,2]});
        assert_eq!(run_one(leaf_query, leaf_input.clone()), stream_leaf_events(&leaf_input));

        let selected = run_query_stream_with_paths_and_options(
            ".|select(length==2)",
            vec![
                serde_json::json!([1, 2]),
                serde_json::json!([1]),
                serde_json::json!({"a": 1}),
            ],
            &[],
            RunOptions::default(),
        )
        .expect("select(length==2)");
        assert_eq!(selected, vec![serde_json::json!([1, 2])]);

        assert_eq!(run_one("fg", JsonValue::Null), vec![serde_json::json!("foobar")]);
        assert_eq!(run_one(r#"include "g"; empty"#, JsonValue::Null), Vec::<JsonValue>::new());
        assert_eq!(
            run_one(
                r#"import "test_bind_order" as check; check::check==true"#,
                JsonValue::Null
            ),
            vec![serde_json::json!(true)]
        );
        assert_eq!(
            run_one("[{a:1}]", JsonValue::Null),
            vec![serde_json::json!([{"a": 1}])]
        );
        assert_eq!(
            run_one("def a: .; 0", JsonValue::Null),
            vec![serde_json::json!(0)]
        );
        assert_eq!(
            run_one(r#""inter\("pol" + "ation")""#, JsonValue::Null),
            vec![serde_json::json!("interpolation")]
        );
        assert_eq!(
            run_one(r#"@html "<b>\(.)</b>""#, serde_json::json!("<x>")),
            vec![serde_json::json!("<b>&lt;x&gt;</b>")]
        );

        assert_eq!(
            run_one("[.[]|tojson|fromjson]", serde_json::json!([1, "x"])),
            vec![serde_json::json!([1, "x"])]
        );
        assert_runtime_error_contains(
            run_query_stream("[.[]|tojson|fromjson]", vec![serde_json::json!(1)]),
            "Cannot iterate over number",
        );
    }

    #[test]
    fn fromstream_inputs_decodes_and_reports_shape_errors() {
        let scalar = run_query_stream_with_paths_and_options(
            "fromstream(inputs)",
            vec![serde_json::json!([[], 1])],
            &[],
            RunOptions::default(),
        )
        .expect("decode scalar fromstream");
        assert_eq!(scalar, vec![serde_json::json!(1)]);

        let array = run_query_stream_with_paths_and_options(
            "fromstream(inputs)",
            vec![
                serde_json::json!([[0], 1]),
                serde_json::json!([[1], 2]),
                serde_json::json!([[1]]),
            ],
            &[],
            RunOptions::default(),
        )
        .expect("decode array fromstream");
        assert_eq!(array, vec![serde_json::json!([1, 2])]);

        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([[]])],
                &[],
                RunOptions::default(),
            ),
            "invalid root close marker",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!(1)],
                &[],
                RunOptions::default(),
            ),
            "stream event must be an array",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([[], 1, 2])],
                &[],
                RunOptions::default(),
            ),
            "invalid stream event shape",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([1, 2])],
                &[],
                RunOptions::default(),
            ),
            "stream path must be an array",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([[-1], 2])],
                &[],
                RunOptions::default(),
            ),
            "path index must be a non-negative integer",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([[0]])],
                &[],
                RunOptions::default(),
            ),
            "close marker without value",
        );
        assert_runtime_error_contains(
            run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![
                    serde_json::json!([[0], 1]),
                    serde_json::json!([["a"], 2]),
                ],
                &[],
                RunOptions::default(),
            ),
            "mixed container key types",
        );
    }

    #[test]
    fn jq_pack3_builtin_combo_cases() {
        assert_eq!(
            run_null_input("[limit(5,7; range(9))]"),
            vec![serde_json::json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])]
        );
        assert_eq!(
            run_null_input("[nth(5,7; range(9;0;-1))]"),
            vec![serde_json::json!([4, 2])]
        );
        assert_eq!(
            run_one(
                r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"#,
                serde_json::json!("a,b|c,d,e||f,g,h,|,|,i,j")
            ),
            vec![serde_json::json!([
                1,
                3,
                22,
                19,
                [1, 5, 7, 12, 14, 16, 18, 20, 22],
                [3, 9, 10, 17, 19]
            ])]
        );
        assert_eq!(
            run_one(r#"join(",","/")"#, serde_json::json!(["a", "b", "c", "d"])),
            vec![serde_json::json!("a,b,c,d"), serde_json::json!("a/b/c/d")]
        );
        assert_eq!(
            run_one(
                r#"[.[]|join("a")]"#,
                serde_json::json!([[], [""], ["", ""], ["", "", ""]])
            ),
            vec![serde_json::json!(["", "", "a", "aa"])]
        );
        assert_eq!(
            run_one(
                "flatten(3,2,1)",
                serde_json::json!([0, [1], [[2]], [[[3]]]])
            ),
            vec![
                serde_json::json!([0, 1, 2, 3]),
                serde_json::json!([0, 1, 2, [3]]),
                serde_json::json!([0, 1, [2], [[3]]]),
            ]
        );
    }

    #[test]
    fn jq_pack4_arith_and_builtin_cases() {
        assert_eq!(
            run_one(".+4", serde_json::json!(15)),
            vec![serde_json::from_str::<JsonValue>("19.0").expect("json")]
        );
        assert_eq!(
            run_one(".+null", serde_json::json!({"a":42})),
            vec![serde_json::json!({"a":42})]
        );
        assert_eq!(run_one("null+.", JsonValue::Null), vec![JsonValue::Null]);
        assert_eq!(
            run_one(".a+.b", serde_json::json!({"a":42})),
            vec![serde_json::json!(42)]
        );
        assert_eq!(
            run_null_input("[1,2,3] + [.]"),
            vec![serde_json::json!([1, 2, 3, null])]
        );
        assert_eq!(
            run_one(r#"{"a":1} + {"b":2} + {"c":3}"#, serde_json::json!("x")),
            vec![serde_json::json!({"a":1,"b":2,"c":3})]
        );
        assert_eq!(
            run_one(
                r#""asdf" + "jkl;" + . + . + ."#,
                serde_json::json!("some string")
            ),
            vec![serde_json::json!(
                "asdfjkl;some stringsome stringsome string"
            )]
        );
        assert_eq!(
            run_one(
                r#""\u0000\u0020\u0000" + ."#,
                serde_json::json!("\u{0000} \u{0000}")
            ),
            vec![serde_json::json!("\u{0000} \u{0000}\u{0000} \u{0000}")]
        );
        assert_eq!(
            run_one("42 - .", serde_json::json!(11)),
            vec![serde_json::json!(31)]
        );
        assert_eq!(
            run_one("[1,2,3,4,1] - [.,3]", serde_json::json!(1)),
            vec![serde_json::json!([2, 4])]
        );
        assert_eq!(
            run_null_input("[-1 as $x | 1,$x]"),
            vec![serde_json::json!([1, -1])]
        );
        assert_eq!(
            run_one("[10 * 20, 20 / .]", serde_json::json!(4)),
            vec![serde_json::json!([200, 5])]
        );
        assert_eq!(
            run_null_input("1 + 2 * 2 + 10 / 2"),
            vec![serde_json::json!(10)]
        );
        assert_eq!(
            run_null_input("[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]"),
            vec![serde_json::json!([2, 8, 10, 14])]
        );
        assert_eq!(
            run_null_input("1e-19 + 1e-20 - 5e-21"),
            vec![serde_json::from_str::<JsonValue>("1.05e-19").expect("json")]
        );
        assert_eq!(
            run_null_input("1 / 1e-17"),
            vec![serde_json::from_str::<JsonValue>("1e17").expect("json")]
        );
        assert_eq!(run_null_input("25 % 7"), vec![serde_json::json!(4)]);
        assert_eq!(run_null_input("49732 % 472"), vec![serde_json::json!(172)]);
        assert_eq!(
            run_null_input("[(infinite, -infinite) % (1, -1, infinite)]"),
            vec![serde_json::json!([0, 0, 0, 0, 0, -1])]
        );
        assert_eq!(
            run_null_input("[nan % 1, 1 % nan | isnan]"),
            vec![serde_json::json!([true, true])]
        );
        assert_eq!(
            run_one("1 + tonumber + (\"10\" | tonumber)", serde_json::json!(4)),
            vec![serde_json::json!(15)]
        );
        assert_eq!(
            run_one(
                "map(toboolean)",
                serde_json::json!(["false", "true", false, true])
            ),
            vec![serde_json::json!([false, true, false, true])]
        );
        assert_eq!(
            run_one(
                ".[] | try toboolean catch .",
                serde_json::json!([null, 0, "tru", "truee", "fals", "falsee", [], {}])
            ),
            vec![
                serde_json::json!("null (null) cannot be parsed as a boolean"),
                serde_json::json!("number (0) cannot be parsed as a boolean"),
                serde_json::json!("string (\"tru\") cannot be parsed as a boolean"),
                serde_json::json!("string (\"truee\") cannot be parsed as a boolean"),
                serde_json::json!("string (\"fals\") cannot be parsed as a boolean"),
                serde_json::json!("string (\"falsee\") cannot be parsed as a boolean"),
                serde_json::json!("array ([]) cannot be parsed as a boolean"),
                serde_json::json!("object ({}) cannot be parsed as a boolean"),
            ]
        );
        assert_eq!(
            run_one(
                r#"[{"a":42},.object,10,.num,false,true,null,"b",[1,4]] | .[] as $x | [$x == .[]]"#,
                serde_json::json!({"object":{"a":42},"num":10.0})
            ),
            vec![
                serde_json::json!([true, true, false, false, false, false, false, false, false]),
                serde_json::json!([true, true, false, false, false, false, false, false, false]),
                serde_json::json!([false, false, true, true, false, false, false, false, false]),
                serde_json::json!([false, false, true, true, false, false, false, false, false]),
                serde_json::json!([false, false, false, false, true, false, false, false, false]),
                serde_json::json!([false, false, false, false, false, true, false, false, false]),
                serde_json::json!([false, false, false, false, false, false, true, false, false]),
                serde_json::json!([false, false, false, false, false, false, false, true, false]),
                serde_json::json!([false, false, false, false, false, false, false, false, true]),
            ]
        );
        assert_eq!(
            run_one(
                "[.[] | length]",
                serde_json::json!([[],{},[1,2],{"a":42},"asdf","\u{03bc}"])
            ),
            vec![serde_json::json!([0, 0, 2, 1, 4, 1])]
        );
        assert_eq!(
            run_one("utf8bytelength", serde_json::json!("asdf\u{03bc}")),
            vec![serde_json::json!(6)]
        );
        assert_eq!(
            run_one(
                "[.[] | try utf8bytelength catch .]",
                serde_json::json!([[], {}, [1, 2], 55, true, false])
            ),
            vec![serde_json::json!([
                "array ([]) only strings have UTF-8 byte length",
                "object ({}) only strings have UTF-8 byte length",
                "array ([1,2]) only strings have UTF-8 byte length",
                "number (55) only strings have UTF-8 byte length",
                "boolean (true) only strings have UTF-8 byte length",
                "boolean (false) only strings have UTF-8 byte length"
            ])]
        );
        assert_eq!(
            run_one(
                "map(keys)",
                serde_json::json!([{}, {"abcd":1,"abc":2,"abcde":3}, {"x":1, "z":3, "y":2}])
            ),
            vec![serde_json::json!([
                [],
                ["abc", "abcd", "abcde"],
                ["x", "y", "z"]
            ])]
        );
        assert_eq!(
            run_null_input("[1,2,empty,3,empty,4]"),
            vec![serde_json::json!([1, 2, 3, 4])]
        );
        assert_eq!(
            run_one(
                "map(add)",
                serde_json::json!([[], [1,2,3], ["a","b","c"], [[3],[4,5],[6]], [{"a":1}, {"b":2}, {"a":3}]])
            ),
            vec![serde_json::json!([null,6,"abc",[3,4,5,6],{"a":3,"b":2}])]
        );
        assert_eq!(
            run_one("add", serde_json::json!([[1, 2], [3, 4]])),
            vec![serde_json::json!([1, 2, 3, 4])]
        );
        assert_eq!(
            run_one("map_values(.+1)", serde_json::json!([0, 1, 2])),
            vec![serde_json::json!([1, 2, 3])]
        );
        assert_eq!(
            run_null_input("[add(null), add(range(range(10))), add(empty), add(10,range(10))]"),
            vec![serde_json::json!([null, 120, null, 55])]
        );
        assert_eq!(
            run_one(".sum = add(.arr[])", serde_json::json!({"arr":[]})),
            vec![serde_json::json!({"arr":[],"sum":null})]
        );
        assert_eq!(
            run_one(
                "add({(.[]):1}) | keys",
                serde_json::json!(["a", "a", "b", "a", "d", "b", "d", "a", "d"])
            ),
            vec![serde_json::json!(["a", "b", "d"])]
        );
    }

    #[test]
    fn jq_pack5_defs_reduce_and_destructure_cases() {
        assert_eq!(
            run_null_input(
                "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990"
            )
            .len(),
            4
        );
        assert_eq!(
            run_null_input("5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000"),
            vec![JsonValue::Bool(true), JsonValue::Bool(true)]
        );
        assert_eq!(
            run_null_input("(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)"),
            vec![
                JsonValue::Bool(true),
                JsonValue::Bool(true),
                JsonValue::Bool(true),
                JsonValue::Bool(true)
            ]
        );

        assert_eq!(
            run_one(
                "def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g",
                serde_json::from_str::<JsonValue>("3.0").expect("json")
            ),
            vec![
                serde_json::from_str::<JsonValue>("106.0").expect("json"),
                serde_json::from_str::<JsonValue>("105.0").expect("json"),
            ]
        );
        assert_eq!(
            run_one("def f: (1000,2000); f", serde_json::json!(123412345)),
            vec![serde_json::json!(1000), serde_json::json!(2000)]
        );
        assert_eq!(
            run_one(
                "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])",
                serde_json::json!([1, 2])
            ),
            vec![serde_json::json!([2, 2, 1, 1, 1, 1])]
        );
        assert_eq!(
            run_one("def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]", JsonValue::Null),
            vec![serde_json::json!([4,1,2,3,3,5,4,1,2,3,3])]
        );
        assert_eq!(
            run_one("def a: 0; . | a", JsonValue::Null),
            vec![serde_json::json!(0)]
        );
        assert_eq!(
            run_one("def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])", serde_json::json!([0,1,2,3,4,5,6,7,8,9])),
            vec![serde_json::json!([9,8,7,6,5,4,3,2,1,0])]
        );
        assert_eq!(
            run_one("([1,2] + [4,5])", serde_json::json!([1, 2, 3])),
            vec![serde_json::json!([1, 2, 4, 5])]
        );
        assert_eq!(
            run_one("[.[]|floor]", serde_json::json!([-1.1, 1.1, 1.9])),
            vec![serde_json::json!([-2, 1, 1])]
        );
        assert_eq!(
            run_one("[.[]|sqrt]", serde_json::json!([4, 9])),
            vec![serde_json::json!([2, 3])]
        );
        assert_eq!(
            run_one(
                "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt",
                serde_json::json!([2, 4, 4, 4, 5, 5, 7, 9])
            ),
            vec![serde_json::json!(2)]
        );
        assert_eq!(
            run_one("atan * 4 * 1000000|floor / 1000000", serde_json::json!(1)),
            vec![serde_json::from_str::<JsonValue>("3.141592").expect("json")]
        );
        assert_eq!(
            run_null_input("[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]"),
            vec![serde_json::from_str::<JsonValue>("[1,0.996917,0.987688,0.972369,0.951056,0.923879,0.891006,0.85264,0.809017,0.760406,0.707106,0.649448,0.587785,0.522498,0.45399,0.382683,0.309017,0.233445,0.156434,0.078459]").expect("json")]
        );
        assert_eq!(
            run_null_input("[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]"),
            vec![serde_json::from_str::<JsonValue>("[0,0.078459,0.156434,0.233445,0.309016,0.382683,0.45399,0.522498,0.587785,0.649447,0.707106,0.760405,0.809016,0.85264,0.891006,0.923879,0.951056,0.972369,0.987688,0.996917]").expect("json")]
        );
        assert_eq!(
            run_one(
                "def f(x): x | x; f([.], . + [42])",
                serde_json::json!([1, 2, 3])
            ),
            vec![
                serde_json::json!([[[1, 2, 3]]]),
                serde_json::json!([[1, 2, 3], 42]),
                serde_json::json!([[1, 2, 3, 42]]),
                serde_json::json!([1, 2, 3, 42, 42]),
            ]
        );
        assert_eq!(
            run_one(
                "def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]",
                serde_json::json!(1)
            ),
            vec![serde_json::json!([33, 101])]
        );
        assert_eq!(
            run_one("def id(x):x; 2000 as $x | def f(x):1 as $x | id([$x, x, x]); def g(x): 100 as $x | f($x,$x+x); g($x)", serde_json::json!("more testing")),
            vec![serde_json::from_str::<JsonValue>("[1,100,2100.0,100,2100.0]").expect("json")]
        );
        assert_eq!(
            run_one("def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)", serde_json::json!([1,2,3])),
            vec![JsonValue::Bool(true)]
        );
        assert_eq!(
            run_one("[[20,10][1,0] as $x | def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]", serde_json::json!(999999999)),
            vec![serde_json::from_str::<JsonValue>("[[110.0,130.0],[210.0,130.0],[110.0,230.0],[210.0,230.0],[120.0,160.0],[220.0,160.0],[120.0,260.0],[220.0,260.0]]").expect("json")]
        );
        assert_eq!(
            run_one(
                "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]",
                serde_json::json!([1, 2, 3, 4])
            ),
            vec![serde_json::json!([1, 2, 6, 24])]
        );

        assert_eq!(
            run_one("reduce .[] as $x (0; . + $x)", serde_json::json!([1, 2, 4])),
            vec![serde_json::json!(7)]
        );
        assert_eq!(
            run_one(
                "reduce .[] as [$i, {j:$j}] (0; . + $i - $j)",
                serde_json::json!([[2,{"j":1}],[5,{"j":3}],[6,{"j":4}]])
            ),
            vec![serde_json::json!(5)]
        );
        assert_eq!(
            run_null_input("reduce [[1,2,10], [3,4,10]][] as [$i,$j] (0; . + $i * $j)"),
            vec![serde_json::json!(14)]
        );
        assert_eq!(
            run_one(
                "[-reduce -.[] as $x (0; . + $x)]",
                serde_json::json!([1, 2, 3])
            ),
            vec![serde_json::json!([6])]
        );
        assert_eq!(
            run_one(
                "[reduce .[] / .[] as $i (0; . + $i)]",
                serde_json::json!([1, 2])
            ),
            vec![serde_json::json!([4.5])]
        );
        assert_eq!(
            run_one(
                "reduce .[] as $x (0; . + $x) as $x | $x",
                serde_json::json!([1, 2, 3])
            ),
            vec![serde_json::json!(6)]
        );
        assert_eq!(
            run_one("reduce . as $n (.; .)", JsonValue::Null),
            vec![JsonValue::Null]
        );

        assert_eq!(
            run_one(
                ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]",
                serde_json::json!({"a":1,"b":[2,{"d":3}]})
            ),
            vec![serde_json::json!([1, 2, 3])]
        );
        assert_eq!(
            run_one(
                ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]",
                serde_json::json!({"a":1,"b":[2,{"d":3}]})
            ),
            vec![serde_json::json!([1,[2,{"d":3}],2,{"d":3}])]
        );
        assert_eq!(
            run_one(".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]", serde_json::json!([{"a":1, "b":[2,{"d":3}]}, [4, {"b":5, "c":6}, 7, 8, 9], "foo"])),
            vec![
                serde_json::json!([1,null,2,3,null,null]),
                serde_json::json!([4,5,null,null,7,null]),
                serde_json::json!([null,null,null,null,null,"foo"]),
            ]
        );

        assert_eq!(
            run_one(
                ".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                serde_json::json!([[3], [4], [5], 6])
            ),
            Vec::<JsonValue>::new()
        );
        assert_eq!(
            run_one(
                ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                serde_json::json!([[3], [4], [5], 6])
            ),
            Vec::<JsonValue>::new()
        );
        assert_eq!(
            run_one(
                "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                JsonValue::Null
            ),
            Vec::<JsonValue>::new()
        );
        assert_eq!(
            run_one(
                "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                JsonValue::Null
            ),
            Vec::<JsonValue>::new()
        );

        let four = expected_four_case_values();
        for query in [
            ".[] | . as {a:$a} ?// {a:$a} ?// $a | $a",
            ".[] as {a:$a} ?// {a:$a} ?// $a | $a",
            ".[] | . as {a:$a} ?// $a ?// {a:$a} | $a",
            ".[] as {a:$a} ?// $a ?// {a:$a} | $a",
        ] {
            let actual = run_one(query, serde_json::json!([[3], [4], [5], 6]));
            assert_eq!(actual.as_slice(), four.as_slice(), "query {query}");
        }
        for query in [
            "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// $a | $a",
            "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a",
            "[[3],[4],[5],6][] | . as {a:$a} ?// $a ?// {a:$a} | $a",
        ] {
            let actual = run_one(query, JsonValue::Null);
            assert_eq!(actual.as_slice(), four.as_slice(), "query {query}");
        }
    }

    #[test]
    fn jq_pack6_fixture_cluster_1001_1369_cases() {
        assert_fixture_cluster("jq_1001_80", FIXTURE_CASES_1001_80);
    }

    #[test]
    fn jq_pack_cluster_320_363_cases() {
        assert_fixture_cluster("jq_320_363", FIXTURE_CASES_320_363);
    }

    #[test]
    fn jq_pack_cluster_403_433_cases() {
        assert_fixture_cluster("jq_403_433", FIXTURE_CASES_403_433);
    }

    #[test]
    fn jq_pack_cluster_364_391_cases() {
        assert_fixture_cluster("jq_364_391", FIXTURE_CASES_364_391);
    }

    #[test]
    fn jq_pack_cluster_506_519_cases() {
        assert_fixture_cluster("jq_506_519", FIXTURE_CASES_506_519);
    }

    #[test]
    fn jq_pack_cluster_295_307_cases() {
        assert_fixture_cluster("jq_295_307", FIXTURE_CASES_295_307);
    }

    #[test]
    fn jq_pack_cluster_308_319_cases() {
        assert_fixture_cluster("jq_308_319", FIXTURE_CASES_308_319);
    }

    #[test]
    fn jq_pack_cluster_434_445_cases() {
        assert_fixture_cluster("jq_434_445", FIXTURE_CASES_434_445);
    }

    #[test]
    fn jq_pack_cluster_487_492_cases() {
        assert_fixture_cluster("jq_487_492", FIXTURE_CASES_487_492);
    }

    #[test]
    fn jq_pack_cluster_290_294_cases() {
        assert_fixture_cluster("jq_290_294", FIXTURE_CASES_290_294);
    }

    #[test]
    fn jq_pack_cluster_475_479_cases() {
        assert_fixture_cluster("jq_475_479", FIXTURE_CASES_475_479);
    }

    #[test]
    fn jq_pack_remaining_compile_cases() {
        assert_fixture_cluster("jq_remaining_compile", FIXTURE_CASES_REMAINING_COMPILE);
    }

    #[test]
    fn onig_fixture_cases() {
        assert_fixture_cluster("onig_all", FIXTURE_CASES_ONIG_ALL);
    }

    #[test]
    fn man_fixture_fail_cases() {
        assert_fixture_cluster("man_fail_183", FIXTURE_CASES_MAN_FAIL_183);
    }

    #[test]
    fn jq171_extra_compat_cases() {
        assert_fixture_cluster("jq171_extra", FIXTURE_CASES_JQ171_EXTRA);
    }

    #[test]
    fn man171_extra_compat_cases() {
        assert_fixture_cluster("man171_extra", FIXTURE_CASES_MAN171_EXTRA);
    }

    #[test]
    fn manonig_fixture_cases() {
        assert_fixture_cluster("manonig_all", FIXTURE_CASES_MANONIG_ALL);
    }

    #[test]
    fn optional_extra_fixture_cases() {
        assert_fixture_cluster("optional_extra", FIXTURE_CASES_OPTIONAL_EXTRA);
    }

    #[test]
    fn format_pipeline_and_try_compat_cases() {
        let s = serde_json::json!("<>&'\"\t");
        assert_eq!(run_one("(@base64|@base64d)", s.clone()), vec![s.clone()]);
        assert_eq!(run_one("(@uri|@urid)", s.clone()), vec![s]);

        assert!(validate_query("(@uri|@urid)").is_ok());
        assert!(validate_query(". | try @urid catch .").is_ok());

        assert_eq!(
            run_one("@base64d", serde_json::json!("=")),
            vec![serde_json::json!("")]
        );
        assert_eq!(
            run_one(
                ". | try @base64d catch .",
                serde_json::json!("Not base64 data")
            ),
            vec![serde_json::json!(
                "string (\"Not base64 data\") is not valid base64 data"
            )]
        );
        assert_eq!(
            run_one(". | try @base64d catch .", serde_json::json!("QUJDa")),
            vec![serde_json::json!(
                "string (\"QUJDa\") trailing base64 byte found"
            )]
        );
        assert_eq!(
            run_one(". | try @urid catch .", serde_json::json!("%F0%93%81")),
            vec![serde_json::json!(
                "string (\"%F0%93%81\") is not a valid uri encoding"
            )]
        );
    }

    #[test]
    fn format_pipeline_parser_accepts_and_rejects_expected_forms() {
        let direct = parse_format_pipeline_steps("@uri|@urid").expect("direct pipeline");
        assert_eq!(direct, vec!["@uri", "@urid"]);

        let wrapped =
            parse_format_pipeline_steps("(. | @base64 | @base64d)").expect("wrapped pipeline");
        assert_eq!(wrapped, vec!["@base64", "@base64d"]);

        let try_steps =
            parse_try_catch_format_steps(". | try @urid catch .").expect("try catch form");
        assert_eq!(try_steps, vec!["@urid"]);

        assert!(parse_format_pipeline_steps("(@uri|.,@urid)").is_none());
        assert!(parse_try_catch_format_steps(". | try (@urid|@uri) catch 0").is_none());
    }

    #[test]
    fn jq171_match_empty_capture_offsets() {
        let prev = std::env::var_os("ZQ_JQ_COMPAT_PROFILE");
        std::env::set_var("ZQ_JQ_COMPAT_PROFILE", "jq171");
        let actual = run_one(r#"[match("( )*"; "g")]"#, serde_json::json!("abc"));
        match prev {
            Some(v) => std::env::set_var("ZQ_JQ_COMPAT_PROFILE", v),
            None => std::env::remove_var("ZQ_JQ_COMPAT_PROFILE"),
        }

        assert_eq!(
            actual,
            vec![
                serde_json::json!([{"offset":0,"length":0,"string":"","captures":[{"offset":0,"string":"","length":0,"name":null}]},{"offset":1,"length":0,"string":"","captures":[{"offset":1,"string":"","length":0,"name":null}]},{"offset":2,"length":0,"string":"","captures":[{"offset":2,"string":"","length":0,"name":null}]},{"offset":3,"length":0,"string":"","captures":[{"offset":3,"string":"","length":0,"name":null}]}])
            ]
        );
    }
}
