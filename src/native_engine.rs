use regex::Regex;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    pub null_input: bool,
}

#[derive(Debug)]
pub enum TryExecute {
    Unsupported,
    Executed(Result<Vec<JsonValue>, String>),
}

#[derive(Debug)]
pub enum TryExecuteStream {
    Unsupported,
    Executed(Result<(), String>),
}

pub fn is_supported(query: &str) -> bool {
    parse_pipeline(query).is_some()
}

#[derive(Debug, Clone)]
enum Stage {
    Literal(JsonValue),
    Path(Vec<Accessor>),
    SelectTest {
        path: Vec<Accessor>,
        regex: Regex,
    },
    SelectCompare {
        path: Vec<Accessor>,
        op: CompareOp,
        rhs: JsonValue,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Accessor {
    Field(String),
    FieldOpt(String),
    Index(i64),
    IndexOpt(i64),
    Iter,
    IterOpt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
}

pub fn try_execute(query: &str, inputs: &[JsonValue], run_options: RunOptions) -> TryExecute {
    let mut out = Vec::new();
    match try_execute_stream(query, inputs, run_options, |value| {
        out.push(value);
        Ok(())
    }) {
        TryExecuteStream::Unsupported => TryExecute::Unsupported,
        TryExecuteStream::Executed(Ok(())) => TryExecute::Executed(Ok(out)),
        TryExecuteStream::Executed(Err(e)) => TryExecute::Executed(Err(e)),
    }
}

pub fn try_execute_stream<F>(
    query: &str,
    inputs: &[JsonValue],
    run_options: RunOptions,
    mut emit: F,
) -> TryExecuteStream
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    let Some(stages) = parse_pipeline(query) else {
        return TryExecuteStream::Unsupported;
    };

    let mut stream = if run_options.null_input {
        vec![JsonValue::Null]
    } else {
        inputs.to_vec()
    };

    for (stage_idx, terms) in stages.iter().enumerate() {
        let last_stage = stage_idx + 1 == stages.len();
        if last_stage {
            for value in stream {
                if let Err(e) = run_terms_emit(terms, value, &mut emit) {
                    return TryExecuteStream::Executed(Err(e));
                }
            }
            return TryExecuteStream::Executed(Ok(()));
        }

        let mut out = Vec::new();
        for value in stream {
            if let Err(e) = run_terms_collect(terms, value, &mut out) {
                return TryExecuteStream::Executed(Err(e));
            }
        }
        stream = out;
    }

    TryExecuteStream::Executed(Ok(()))
}

fn run_terms_collect(
    terms: &[Stage],
    value: JsonValue,
    out: &mut Vec<JsonValue>,
) -> Result<(), String> {
    let terms_len = terms.len();
    if terms_len == 1 {
        out.extend(run_stage(&terms[0], value)?);
        return Ok(());
    }

    let mut original = Some(value);
    for (idx, term) in terms.iter().enumerate() {
        let input = if idx + 1 == terms_len {
            original.take().expect("value available for last branch")
        } else {
            original
                .as_ref()
                .expect("value available for branch clone")
                .clone()
        };
        out.extend(run_stage(term, input)?);
    }
    Ok(())
}

fn run_terms_emit<F>(terms: &[Stage], value: JsonValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    let terms_len = terms.len();
    if terms_len == 1 {
        for result in run_stage(&terms[0], value)? {
            emit(result)?;
        }
        return Ok(());
    }

    let mut original = Some(value);
    for (idx, term) in terms.iter().enumerate() {
        let input = if idx + 1 == terms_len {
            original.take().expect("value available for last branch")
        } else {
            original
                .as_ref()
                .expect("value available for branch clone")
                .clone()
        };
        for result in run_stage(term, input)? {
            emit(result)?;
        }
    }
    Ok(())
}

fn run_stage(stage: &Stage, input: JsonValue) -> Result<Vec<JsonValue>, String> {
    match stage {
        Stage::Literal(v) => Ok(vec![v.clone()]),
        Stage::Path(accessors) => run_path(accessors, input),
        Stage::SelectTest { path, regex } => run_select_test(path, regex, input),
        Stage::SelectCompare { path, op, rhs } => run_select_compare(path, *op, rhs, input),
    }
}

fn run_select_test(
    path: &[Accessor],
    regex: &Regex,
    input: JsonValue,
) -> Result<Vec<JsonValue>, String> {
    let values = run_path(path, input.clone())?;
    let mut matches_count = 0usize;
    for value in values {
        let JsonValue::String(s) = value else {
            return Err(format!(
                "{} ({}) cannot be matched, as it is not a string",
                type_name(&value),
                value_for_error(&value)
            ));
        };
        if regex.is_match(&s) {
            matches_count += 1;
        }
    }
    let mut out = Vec::with_capacity(matches_count);
    if matches_count == 0 {
        return Ok(out);
    }
    for _ in 1..matches_count {
        out.push(input.clone());
    }
    out.push(input);
    Ok(out)
}

fn run_select_compare(
    path: &[Accessor],
    op: CompareOp,
    rhs: &JsonValue,
    input: JsonValue,
) -> Result<Vec<JsonValue>, String> {
    let values = run_path(path, input.clone())?;
    let mut matches_count = 0usize;
    for lhs in values {
        let is_match = match op {
            CompareOp::Eq => lhs == *rhs,
            CompareOp::Ne => lhs != *rhs,
        };
        if is_match {
            matches_count += 1;
        }
    }

    let mut out = Vec::with_capacity(matches_count);
    if matches_count == 0 {
        return Ok(out);
    }
    for _ in 1..matches_count {
        out.push(input.clone());
    }
    out.push(input);
    Ok(out)
}

fn run_path(accessors: &[Accessor], input: JsonValue) -> Result<Vec<JsonValue>, String> {
    let mut current = vec![input];
    for accessor in accessors {
        let mut next = Vec::new();
        for value in current {
            apply_accessor_into(value, accessor, &mut next)?;
        }
        current = next;
    }
    Ok(current)
}

fn apply_accessor_into(
    value: JsonValue,
    accessor: &Accessor,
    out: &mut Vec<JsonValue>,
) -> Result<(), String> {
    let result = match accessor {
        Accessor::Field(name) | Accessor::FieldOpt(name) => match value {
            JsonValue::Object(map) => {
                out.push(map.get(name).cloned().unwrap_or(JsonValue::Null));
                Ok(())
            }
            JsonValue::Null => {
                out.push(JsonValue::Null);
                Ok(())
            }
            other => Err(format!(
                "Cannot index {} with string \"{}\"",
                type_name(&other),
                name
            )),
        },
        Accessor::Index(index) | Accessor::IndexOpt(index) => match value {
            JsonValue::Array(arr) => {
                let len = arr.len() as i64;
                let idx = if *index < 0 { len + *index } else { *index };
                if idx < 0 || idx >= len {
                    out.push(JsonValue::Null);
                    return Ok(());
                }
                out.push(arr[idx as usize].clone());
                Ok(())
            }
            JsonValue::Null => {
                out.push(JsonValue::Null);
                Ok(())
            }
            other => Err(format!("Cannot index {} with number", type_name(&other))),
        },
        Accessor::Iter | Accessor::IterOpt => match value {
            JsonValue::Array(arr) => {
                out.extend(arr);
                Ok(())
            }
            JsonValue::Object(map) => {
                out.extend(map.into_iter().map(|(_, v)| v));
                Ok(())
            }
            other => Err(format!(
                "Cannot iterate over {} ({})",
                type_name(&other),
                value_for_error(&other)
            )),
        },
    };

    result.or_else(|e| match accessor {
        Accessor::FieldOpt(_) | Accessor::IndexOpt(_) | Accessor::IterOpt => Ok(()),
        _ => Err(e),
    })
}

fn type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn value_for_error(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".to_string(),
        JsonValue::String(s) => format!("{s:?}"),
        _ => serde_json::to_string(v).unwrap_or_else(|_| "<invalid>".to_string()),
    }
}

fn parse_pipeline(query: &str) -> Option<Vec<Vec<Stage>>> {
    let mut chars = query.chars().peekable();
    let mut out = Vec::new();
    loop {
        skip_ws(&mut chars);
        if chars.peek().is_none() {
            break;
        }
        out.push(parse_terms(&mut chars)?);
        skip_ws(&mut chars);
        match chars.peek().copied() {
            Some('|') => {
                chars.next();
            }
            Some(_) => return None,
            None => break,
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_terms(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Vec<Stage>> {
    let mut out = Vec::new();
    loop {
        skip_ws(chars);
        out.push(parse_stage(chars)?);
        skip_ws(chars);
        if chars.peek().copied() == Some(',') {
            chars.next();
            continue;
        }
        break;
    }
    Some(out)
}

fn parse_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    if chars.peek().copied() == Some('.') {
        return parse_path_stage(chars);
    }

    if chars.peek().copied() == Some('s') {
        let mut probe = chars.clone();
        if let Some(stage) = parse_select_stage(&mut probe) {
            *chars = probe;
            return Some(stage);
        }
    }

    let lit = parse_literal(chars)?;
    Some(Stage::Literal(lit))
}

fn parse_path_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    Some(Stage::Path(parse_path_accessors(chars)?))
}

fn parse_path_accessors(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<Vec<Accessor>> {
    if chars.next() != Some('.') {
        return None;
    }
    let mut accessors = Vec::new();
    if matches!(chars.peek().copied(), Some('"')) || ident_start(chars.peek().copied()) {
        let field = Accessor::Field(parse_field_after_dot(chars)?);
        accessors.push(if take_optional(chars) {
            make_optional(field)
        } else {
            field
        });
    }
    loop {
        skip_ws(chars);
        match chars.peek().copied() {
            Some('.') => {
                chars.next();
                let name = parse_field_after_dot(chars)?;
                accessors.push(if take_optional(chars) {
                    Accessor::FieldOpt(name)
                } else {
                    Accessor::Field(name)
                });
            }
            Some('[') => {
                chars.next();
                skip_ws(chars);
                let acc = if chars.peek().copied() == Some(']') {
                    Accessor::Iter
                } else if chars.peek().copied() == Some('"') {
                    Accessor::Field(parse_string(chars)?)
                } else {
                    Accessor::Index(parse_i64(chars)?)
                };
                skip_ws(chars);
                if chars.next() != Some(']') {
                    return None;
                }
                let acc = if take_optional(chars) {
                    make_optional(acc)
                } else {
                    acc
                };
                accessors.push(acc);
            }
            _ => break,
        }
    }
    Some(accessors)
}

fn parse_select_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    parse_keyword(chars, "select")?;
    skip_ws(chars);
    if chars.next() != Some('(') {
        return None;
    }
    skip_ws(chars);
    let path = parse_path_accessors(chars)?;
    skip_ws(chars);
    match chars.peek().copied() {
        Some('|') => {
            chars.next();
            skip_ws(chars);
            parse_keyword(chars, "test")?;
            skip_ws(chars);
            if chars.next() != Some('(') {
                return None;
            }
            skip_ws(chars);
            let pattern = parse_string(chars)?;
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            let regex = Regex::new(&pattern).ok()?;
            Some(Stage::SelectTest { path, regex })
        }
        _ => {
            let op = parse_select_compare_op(chars)?;
            let rhs = parse_select_rhs_literal(chars)?;
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            Some(Stage::SelectCompare { path, op, rhs })
        }
    }
}

fn parse_select_compare_op(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<CompareOp> {
    skip_ws(chars);
    let first = chars.next()?;
    let second = chars.next()?;
    match (first, second) {
        ('=', '=') => Some(CompareOp::Eq),
        ('!', '=') => Some(CompareOp::Ne),
        _ => None,
    }
}

fn parse_select_rhs_literal(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<JsonValue> {
    skip_ws(chars);
    if chars.peek().copied() == Some('"') {
        return Some(JsonValue::String(parse_string(chars)?));
    }

    let mut raw = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch == ')' || ch.is_ascii_whitespace() {
            break;
        }
        raw.push(ch);
        chars.next();
    }

    if raw.is_empty() {
        return None;
    }
    serde_json::from_str(raw.trim()).ok()
}

fn parse_keyword(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    expected: &str,
) -> Option<()> {
    for ch in expected.chars() {
        if chars.next() != Some(ch) {
            return None;
        }
    }
    Some(())
}

fn ident_start(ch: Option<char>) -> bool {
    matches!(ch, Some(c) if c.is_ascii_alphabetic() || c == '_')
}

fn parse_field_after_dot(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    if chars.peek().copied() == Some('"') {
        return parse_string(chars);
    }

    let mut out = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    let mut raw = String::new();
    if chars.next() != Some('"') {
        return None;
    }
    raw.push('"');

    let mut escaped = false;
    for ch in chars.by_ref() {
        raw.push(ch);
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            let parsed: String = serde_json::from_str(&raw).ok()?;
            return Some(parsed);
        }
    }
    None
}

fn take_optional(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    skip_ws(chars);
    if chars.peek().copied() == Some('?') {
        chars.next();
        true
    } else {
        false
    }
}

fn make_optional(acc: Accessor) -> Accessor {
    match acc {
        Accessor::Field(s) => Accessor::FieldOpt(s),
        Accessor::Index(i) => Accessor::IndexOpt(i),
        Accessor::Iter => Accessor::IterOpt,
        other => other,
    }
}

fn parse_i64(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<i64> {
    let mut raw = String::new();
    if chars.peek().copied() == Some('-') {
        raw.push('-');
        chars.next();
    }
    let mut has_digit = false;
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            has_digit = true;
            raw.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if !has_digit {
        return None;
    }
    raw.parse::<i64>().ok()
}

fn parse_literal(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<JsonValue> {
    let mut raw = String::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut depth = 0i32;

    while let Some(ch) = chars.peek().copied() {
        if !in_string && depth == 0 && (ch == '|' || ch == ',' || ch.is_ascii_whitespace()) {
            break;
        }
        raw.push(ch);
        chars.next();

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '[' | '{' => depth += 1,
            ']' | '}' => depth -= 1,
            _ => {}
        }
    }

    if raw.is_empty() || depth != 0 || in_string {
        return None;
    }
    serde_json::from_str(raw.trim()).ok()
}

fn skip_ws(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_runs_simple_paths() {
        let input = vec![serde_json::json!({"a":[{"b":1},{"b":2}]})];
        let out = try_execute(".a[1].b", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn runs_pipeline_with_literal() {
        let input = vec![serde_json::json!({"a": 7})];
        let out = try_execute(".a | 10", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(10)]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn uses_null_input_mode() {
        let input = vec![serde_json::json!({"a": 7})];
        let out = try_execute(".", &input, RunOptions { null_input: true });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_constructs() {
        assert!(matches!(
            try_execute(
                "map(.)",
                &[JsonValue::Null],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
    }

    #[test]
    fn supports_iteration_and_comma_outputs() {
        let input = vec![serde_json::json!([1, 2, 3])];
        let out = try_execute(".[]", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(1),
                        serde_json::json!(2),
                        serde_json::json!(3)
                    ]
                )
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute("1, 2", &[JsonValue::Null], RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)])
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_optional_path_access() {
        let out = try_execute(
            ".a?",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[]?",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".missing?",
            &[serde_json::json!({"a": 1})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_test_regex_filter() {
        let input = vec![serde_json::json!([
            {"id": 1, "name": "user0999"},
            {"id": 2, "name": "user1000"},
            {"id": 3, "name": "user1999"},
            {"id": 4, "name": "user2000"}
        ])];
        let out = try_execute(
            ".[] | select(.name | test(\"user1[0-9]{3}$\")) | .id",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(2), serde_json::json!(3)])
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_compare_eq_filter() {
        let input = vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
            serde_json::json!({"i": 3}),
        ];
        let out = try_execute("select(.i==2)", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!({"i": 2})]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_compare_ne_filter() {
        let input = vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
            serde_json::json!({"i": 3}),
        ];
        let out = try_execute("select(.i!=2)", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![serde_json::json!({"i": 1}), serde_json::json!({"i": 3})]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn select_test_errors_on_non_string_like_jq() {
        let input = vec![serde_json::json!({"name": null})];
        let out = try_execute(
            "select(.name | test(\"x\"))",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "null (null) cannot be matched, as it is not a string")
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn unsupported_regex_syntax_falls_back() {
        let out = try_execute(
            "select(.name | test(\"(?=x)\"))",
            &[serde_json::json!({"name":"x"})],
            RunOptions { null_input: false },
        );
        assert!(matches!(out, TryExecute::Unsupported));
    }

    #[test]
    fn native_runtime_errors_match_jq_wording() {
        let out = try_execute(
            ".[1]",
            &[serde_json::json!({})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "Cannot index object with number");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[]",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "Cannot iterate over number (1)");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn try_execute_stream_matches_collect_mode() {
        let input = vec![serde_json::json!([{"id":1},{"id":2},{"id":3}])];
        let collected = try_execute(".[] | .id", &input, RunOptions { null_input: false });
        let mut streamed = Vec::new();
        let streamed_out =
            try_execute_stream(".[] | .id", &input, RunOptions { null_input: false }, |v| {
                streamed.push(v);
                Ok(())
            });

        match (collected, streamed_out) {
            (TryExecute::Executed(Ok(a)), TryExecuteStream::Executed(Ok(()))) => {
                assert_eq!(a, streamed);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn try_execute_stream_propagates_sink_error() {
        let input = vec![serde_json::json!([1, 2, 3])];
        let out = try_execute_stream(".[]", &input, RunOptions { null_input: false }, |_v| {
            Err("sink failed".to_string())
        });
        match out {
            TryExecuteStream::Executed(Err(err)) => assert_eq!(err, "sink failed"),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn select_reemits_input_for_each_match() {
        let row = serde_json::json!({"tags":[1,1,2], "name":"aaxx"});
        let input = vec![row.clone()];

        let out = try_execute(
            "select(.tags[]==1)",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row.clone(), row.clone()]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            "select(.name | test(\"a\"))",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn object_iteration_and_optional_accessors_follow_contract() {
        let out = try_execute(
            ".[]",
            &[serde_json::json!({"a": 1, "b": 2})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(".[]?", &[serde_json::json!(true)], RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(".[2]", &[serde_json::json!([0, 1])], RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[1]?",
            &[serde_json::json!({"a": 1})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn parser_supports_quoted_paths_and_rejects_broken_forms() {
        let out = try_execute(
            ".\"a b\"",
            &[serde_json::json!({"a b": 7})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert!(matches!(
            try_execute(".[", &[JsonValue::Null], RunOptions { null_input: false }),
            TryExecute::Unsupported
        ));
        assert!(matches!(
            try_execute(
                "select(.a | test(\"x\")",
                &[JsonValue::Null],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
        assert!(matches!(
            try_execute(
                "select(.a=1)",
                &[serde_json::json!({"a": 1})],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
    }

    #[test]
    fn comma_terms_in_intermediate_pipeline_clone_original_input() {
        let input = vec![serde_json::json!({"a": 1})];
        let out = try_execute("., .a | .", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!({"a": 1}), serde_json::json!(1)]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn support_probe_matches_parser() {
        assert!(is_supported(".a | .b"));
        assert!(!is_supported("map(.)"));
    }
}
