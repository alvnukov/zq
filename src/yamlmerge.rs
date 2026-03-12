use serde::Deserialize;
use serde_yaml::{Mapping, Value};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PathSegment {
    Key(String),
    Index(usize),
}

type Path = Vec<PathSegment>;

#[derive(Debug, Default, Clone)]
struct MergeStyleHints {
    plain_merge_paths: HashSet<Path>,
    nonplain_merge_paths: HashSet<Path>,
}

pub fn normalize_value(v: Value) -> Value {
    normalize_value_with_hints(v, None, &mut Vec::new())
}

#[allow(dead_code)]
pub fn normalize_value_from_source(input: &str, v: Value) -> Value {
    let hints = collect_merge_style_hints(input).ok();
    normalize_value_with_hints(v, hints.as_ref().and_then(|v| v.first()), &mut Vec::new())
}

pub fn normalize_documents(input: &str) -> Result<Vec<Value>, serde_yaml::Error> {
    let docs: Vec<Value> = serde_yaml::Deserializer::from_str(input)
        .map(Value::deserialize)
        .collect::<Result<Vec<_>, _>>()?;
    let hints = collect_merge_style_hints(input).ok();
    Ok(docs
        .into_iter()
        .enumerate()
        .map(|(i, doc)| {
            normalize_value_with_hints(doc, hints.as_ref().and_then(|v| v.get(i)), &mut Vec::new())
        })
        .filter(|v| !v.is_null())
        .collect())
}

fn normalize_value_with_hints(v: Value, hints: Option<&MergeStyleHints>, path: &mut Path) -> Value {
    match v {
        Value::Mapping(map) => normalize_mapping_merge(map, hints, path),
        Value::Sequence(seq) => {
            let mut out = Vec::with_capacity(seq.len());
            for (i, item) in seq.into_iter().enumerate() {
                path.push(PathSegment::Index(i));
                out.push(normalize_value_with_hints(item, hints, path));
                path.pop();
            }
            Value::Sequence(out)
        }
        other => other,
    }
}

fn normalize_mapping_merge(
    map: Mapping,
    hints: Option<&MergeStyleHints>,
    path: &mut Path,
) -> Value {
    let merge_key = Value::String("<<".to_string());
    let has_merge_key = map.contains_key(&merge_key);
    let should_merge = has_merge_key && should_apply_merge(hints, path);

    let mut out = Mapping::new();
    if should_merge {
        if let Some(merge_source) = map.get(&merge_key).cloned() {
            apply_merge_source(&mut out, merge_source);
        }
    }

    for (k, v) in map {
        if matches!(&k, Value::String(s) if s == "<<") && should_merge {
            continue;
        }
        let key_seg = mapping_key_segment(&k);
        path.push(PathSegment::Key(key_seg));
        let nv = normalize_value_with_hints(v, hints, path);
        path.pop();
        out.insert(k, nv);
    }
    Value::Mapping(out)
}

fn should_apply_merge(hints: Option<&MergeStyleHints>, path: &Path) -> bool {
    let Some(hints) = hints else {
        return true;
    };
    if hints.plain_merge_paths.contains(path) {
        return true;
    }
    if hints.nonplain_merge_paths.contains(path) {
        return false;
    }
    true
}

fn mapping_key_segment(k: &Value) -> String {
    if let Some(s) = k.as_str() {
        return s.to_string();
    }
    serde_yaml::to_string(k)
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "<non-string-key>".to_string())
}

fn apply_merge_source(target: &mut Mapping, source: Value) {
    match normalize_value(source) {
        Value::Mapping(m) => merge_mapping_into(target, m),
        Value::Sequence(seq) => {
            for item in seq {
                if let Value::Mapping(m) = normalize_value(item) {
                    merge_mapping_into(target, m);
                }
            }
        }
        _ => {}
    }
}

fn merge_mapping_into(target: &mut Mapping, source: Mapping) {
    for (k, v) in source {
        target.entry(k).or_insert(v);
    }
}

fn collect_merge_style_hints(input: &str) -> Result<Vec<MergeStyleHints>, String> {
    let mut all_hints = Vec::new();
    let mut current = MergeStyleHints::default();
    let mut key_stack: Vec<(usize, String)> = Vec::new();
    let mut saw_content = false;

    for raw_line in input.lines() {
        let line = raw_line.trim_end();
        let mut trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if trimmed == "---" {
            if saw_content {
                all_hints.push(std::mem::take(&mut current));
            }
            key_stack.clear();
            saw_content = false;
            continue;
        }
        if trimmed == "..." {
            if saw_content {
                all_hints.push(std::mem::take(&mut current));
            }
            key_stack.clear();
            saw_content = false;
            continue;
        }
        saw_content = true;

        let mut indent = line.len() - trimmed.len();
        while let Some(rest) = trimmed.strip_prefix("- ") {
            trimmed = rest.trim_start();
            indent += 2;
        }

        while key_stack.last().is_some_and(|(level, _)| *level >= indent) {
            key_stack.pop();
        }

        let Some(key_token) = extract_mapping_key_token(trimmed) else {
            continue;
        };
        let path: Path = key_stack.iter().map(|(_, key)| PathSegment::Key(key.clone())).collect();
        if key_token == "<<" {
            current.plain_merge_paths.insert(path);
        } else if key_token == "\"<<\"" || key_token == "'<<'" {
            current.nonplain_merge_paths.insert(path);
        }
        key_stack.push((indent, normalize_mapping_key_token(key_token)));
    }

    if saw_content || all_hints.is_empty() {
        all_hints.push(current);
    };
    Ok(all_hints)
}

fn extract_mapping_key_token(line: &str) -> Option<&str> {
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    for (idx, ch) in line.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == '"' {
                in_double = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            ':' => return Some(line[..idx].trim()),
            _ => {}
        }
    }
    None
}

fn normalize_mapping_key_token(key: &str) -> String {
    let trimmed = key.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0];
        let last = trimmed.as_bytes()[trimmed.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_inline_merge_map() {
        let src = r#"
obj:
  <<: { foo: 123, bar: 456 }
  baz: 999
"#;
        let v: Value = serde_yaml::from_str(src).expect("parse");
        let n = normalize_value_from_source(src, v);
        let j = serde_json::to_value(n).expect("json");
        assert_eq!(j["obj"]["foo"], 123);
        assert_eq!(j["obj"]["bar"], 456);
        assert_eq!(j["obj"]["baz"], 999);
        assert!(j["obj"].get("<<").is_none());
        let line = serde_json::to_string(&j["obj"]).expect("json");
        assert_eq!(line, r#"{"foo":123,"bar":456,"baz":999}"#);
    }

    #[test]
    fn merge_sequence_earlier_source_overrides_later_source() {
        let src = r#"
base1: &base1
  x: first
base2: &base2
  x: second
obj:
  <<: [*base1, *base2]
"#;
        let v: Value = serde_yaml::from_str(src).expect("parse");
        let n = normalize_value_from_source(src, v);
        let j = serde_json::to_value(n).expect("json");
        assert_eq!(j["obj"]["x"], "first");
    }

    #[test]
    fn explicit_key_overrides_merged_value() {
        let src = r#"
base: &base
  image: nginx
  replicas: 2
obj:
  <<: *base
  replicas: 3
"#;
        let v: Value = serde_yaml::from_str(src).expect("parse");
        let n = normalize_value_from_source(src, v);
        let j = serde_json::to_value(n).expect("json");
        assert_eq!(j["obj"]["image"], "nginx");
        assert_eq!(j["obj"]["replicas"], 3);
    }

    #[test]
    fn quoted_merge_key_is_treated_as_regular_key() {
        let src = r#"
obj:
  "<<": { foo: 1 }
  baz: 2
"#;
        let v: Value = serde_yaml::from_str(src).expect("parse");
        let n = normalize_value_from_source(src, v);
        let j = serde_json::to_value(n).expect("json");
        assert_eq!(j["obj"]["<<"]["foo"], 1);
        assert_eq!(j["obj"]["baz"], 2);
    }

    #[test]
    fn merge_precedence_property_like_regression() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..200 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let a = (seed % 1000) as i64;
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let b = (seed % 1000) as i64;
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let c = (seed % 1000) as i64;

            let src = format!(
                r#"
base1: &base1
  x: {a}
  y: {b}
base2: &base2
  y: {c}
  z: {a}
obj:
  <<: [*base1, *base2]
  z: {b}
"#
            );
            let v: Value = serde_yaml::from_str(&src).expect("parse");
            let n = normalize_value_from_source(&src, v);
            let j = serde_json::to_value(n).expect("json");
            assert_eq!(j["obj"]["x"], a, "x mismatch for source:\n{src}");
            assert_eq!(j["obj"]["y"], b, "y precedence mismatch for source:\n{src}");
            assert_eq!(j["obj"]["z"], b, "local override mismatch for source:\n{src}");
        }
    }
}
