#[cfg(test)]
use super::JsonValue;
use super::{Error, ZqValue};
use serde::de::DeserializeOwned;
use std::borrow::Cow;

pub(super) fn normalize_legacy_number_tokens(input: &str) -> Cow<'_, str> {
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
    let (sign, rest) = if let Some(r) = token.strip_prefix('-') { ("-", r) } else { ("", token) };

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
pub(super) fn parse_jsonish_value(input: &str) -> Result<JsonValue, Error> {
    parse_jsonish(input)
}

pub(super) fn parse_jsonish_value_native(input: &str) -> Result<ZqValue, Error> {
    parse_jsonish(input)
}

pub(super) fn parse_jsonish<T: DeserializeOwned>(input: &str) -> Result<T, Error> {
    let canonical = canonicalize_jsonish_tokens(input);
    let json_compatible = replace_non_finite_number_tokens(&canonical);
    serde_json::from_str::<T>(&json_compatible).map_err(Error::Json)
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) fn stringify_jsonish_value(value: &JsonValue) -> Result<String, Error> {
    serde_json::to_string(value).map_err(Error::Json)
}

pub(super) fn stringify_jsonish_value_native(value: &ZqValue) -> Result<String, Error> {
    serde_json::to_string(&value.clone().into_json()).map_err(Error::Json)
}

pub(super) fn canonicalize_jsonish_tokens(input: &str) -> String {
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
        rest.iter().zip(pat.chars()).all(|(l, r)| l.eq_ignore_ascii_case(&r))
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

pub(super) fn replace_non_finite_number_tokens(input: &str) -> String {
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
