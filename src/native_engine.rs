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

#[derive(Debug, Clone, PartialEq)]
enum Stage {
    Literal(JsonValue),
    Path(Vec<Accessor>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Accessor {
    Field(String),
    Index(i64),
    Iter,
}

pub fn try_execute(query: &str, inputs: &[JsonValue], run_options: RunOptions) -> TryExecute {
    let Some(stages) = parse_pipeline(query) else {
        return TryExecute::Unsupported;
    };

    let mut stream = if run_options.null_input {
        vec![JsonValue::Null]
    } else {
        inputs.to_vec()
    };

    for terms in stages {
        let mut out = Vec::new();
        for value in stream {
            for term in &terms {
                match run_stage(term, value.clone()) {
                    Ok(values) => out.extend(values),
                    Err(e) => return TryExecute::Executed(Err(e)),
                }
            }
        }
        stream = out;
    }
    TryExecute::Executed(Ok(stream))
}

fn run_stage(stage: &Stage, input: JsonValue) -> Result<Vec<JsonValue>, String> {
    match stage {
        Stage::Literal(v) => Ok(vec![v.clone()]),
        Stage::Path(accessors) => run_path(accessors, input),
    }
}

fn run_path(accessors: &[Accessor], input: JsonValue) -> Result<Vec<JsonValue>, String> {
    let mut current = vec![input];
    for accessor in accessors {
        let mut next = Vec::new();
        for value in current {
            let mut values = apply_accessor(value, accessor)?;
            next.append(&mut values);
        }
        current = next;
    }
    Ok(current)
}

fn apply_accessor(value: JsonValue, accessor: &Accessor) -> Result<Vec<JsonValue>, String> {
    match accessor {
        Accessor::Field(name) => match value {
            JsonValue::Object(map) => Ok(vec![map.get(name).cloned().unwrap_or(JsonValue::Null)]),
            JsonValue::Null => Ok(vec![JsonValue::Null]),
            other => Err(format!(
                "Cannot index {} with string \"{}\"",
                type_name(&other),
                name
            )),
        },
        Accessor::Index(index) => match value {
            JsonValue::Array(arr) => {
                let len = arr.len() as i64;
                let idx = if *index < 0 { len + *index } else { *index };
                if idx < 0 || idx >= len {
                    return Ok(vec![JsonValue::Null]);
                }
                Ok(vec![arr[idx as usize].clone()])
            }
            JsonValue::Null => Ok(vec![JsonValue::Null]),
            other => Err(format!(
                "Cannot index {} with number {}",
                type_name(&other),
                index
            )),
        },
        Accessor::Iter => match value {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(map) => Ok(map.into_iter().map(|(_, v)| v).collect()),
            other => Err(format!("Cannot iterate over {}", type_name(&other))),
        },
    }
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
        chars.next();
        let mut accessors = Vec::new();
        if matches!(chars.peek().copied(), Some('"')) || ident_start(chars.peek().copied()) {
            accessors.push(Accessor::Field(parse_field_after_dot(chars)?));
        }
        loop {
            skip_ws(chars);
            match chars.peek().copied() {
                Some('.') => {
                    chars.next();
                    accessors.push(Accessor::Field(parse_field_after_dot(chars)?));
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
                    accessors.push(acc);
                }
                _ => break,
            }
        }
        return Some(Stage::Path(accessors));
    }

    let lit = parse_literal(chars)?;
    Some(Stage::Literal(lit))
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
            try_execute("map(.)", &[JsonValue::Null], RunOptions { null_input: false }),
            TryExecute::Unsupported
        ));
        assert!(matches!(
            try_execute(".a?", &[JsonValue::Null], RunOptions { null_input: false }),
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
}
