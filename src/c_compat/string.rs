// c-ref: C string/printing compatibility helpers (jv_print.c family).

use crate::c_compat::{json as c_json, math as c_math, value as c_value};
use crate::value::ZqValue;
use base64::Engine;

// jq171-port: jq171/src/jv_print.c:jv_dump_string_trunc()
pub(crate) fn dump_value_string_trunc_legacy(value: &ZqValue, bufsize: usize) -> String {
    if bufsize == 0 {
        return String::new();
    }

    let dumped = serde_json::to_string(&value.clone().into_json())
        .unwrap_or_else(|_| "<invalid>".to_string());
    let mut out = dumped
        .chars()
        .take(bufsize.saturating_sub(1))
        .collect::<String>();
    if dumped.len() > bufsize.saturating_sub(1) && bufsize >= 4 {
        let dots_from = bufsize.saturating_sub(4);
        if dots_from <= out.len() {
            out.replace_range(dots_from.., "...");
        }
    }
    out
}

// jq-port: jq/src/jv_print.c:jv_dump_string_trunc()
pub(crate) fn dump_value_string_trunc_modern(value: &ZqValue, bufsize: usize) -> String {
    if bufsize == 0 {
        return String::new();
    }
    let dumped = serde_json::to_string(&value.clone().into_json())
        .unwrap_or_else(|_| "<invalid>".to_string());
    let len = dumped.len();
    if len > bufsize.saturating_sub(1) && bufsize >= 8 {
        let delim = match dumped.as_bytes().first().copied() {
            Some(b'"') => Some('"'),
            Some(b'[') => Some(']'),
            Some(b'{') => Some('}'),
            _ => None,
        };
        let mut prefix_len = bufsize - if delim.is_some() { 5 } else { 4 };
        prefix_len = prefix_len.min(len);
        while prefix_len > 0 && !dumped.is_char_boundary(prefix_len) {
            prefix_len -= 1;
        }

        let mut out = String::with_capacity(bufsize.saturating_sub(1));
        out.push_str(&dumped[..prefix_len]);
        out.push_str("...");
        if let Some(closing) = delim {
            out.push(closing);
        }
        return out;
    }

    let mut keep = len.min(bufsize.saturating_sub(1));
    while keep > 0 && !dumped.is_char_boundary(keep) {
        keep -= 1;
    }
    dumped[..keep].to_string()
}

// Mirrors jq/src/builtin.c:binop_multiply conversion before jv_string_repeat().
pub(crate) fn string_repeat_count_jq(count: f64) -> i32 {
    if count.is_nan() || count < 0.0 {
        -1
    } else if count > i32::MAX as f64 {
        i32::MAX
    } else {
        count as i32
    }
}

// Mirrors jq/src/jv.c:jv_string_repeat() guard:
//   if (res_len >= INT_MAX) -> "Repeat string result too long"
pub(crate) fn string_repeat_jq(s: String, repeat: i32) -> Result<ZqValue, String> {
    if repeat < 0 {
        return Ok(ZqValue::Null);
    }
    let len = s.len() as i128;
    let res_len = len * i128::from(repeat);
    if res_len >= i128::from(i32::MAX) {
        return Err("Repeat string result too long".to_string());
    }
    if res_len == 0 {
        return Ok(ZqValue::String(String::new()));
    }
    Ok(ZqValue::String(s.repeat(repeat as usize)))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_startswith
pub(crate) fn startswith_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(text), ZqValue::String(prefix)) = (a, b) else {
        return Err("startswith() requires string inputs".to_string());
    };
    Ok(ZqValue::Bool(text.starts_with(&prefix)))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_endswith
pub(crate) fn endswith_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(text), ZqValue::String(suffix)) = (a, b) else {
        return Err("endswith() requires string inputs".to_string());
    };
    Ok(ZqValue::Bool(text.ends_with(&suffix)))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_split
pub(crate) fn split_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(text), ZqValue::String(separator)) = (a, b) else {
        return Err("split input and separator must be strings".to_string());
    };
    if separator.is_empty() {
        let chars = text
            .chars()
            .map(|ch| ZqValue::String(ch.to_string()))
            .collect();
        return Ok(ZqValue::Array(chars));
    }
    Ok(ZqValue::Array(
        text.split(&separator)
            .map(|part| ZqValue::String(part.to_string()))
            .collect(),
    ))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_ltrimstr
pub(crate) fn ltrimstr_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(text), ZqValue::String(prefix)) = (a, b) else {
        return Err("startswith() requires string inputs".to_string());
    };
    if text.starts_with(&prefix) {
        Ok(ZqValue::String(text[prefix.len()..].to_string()))
    } else {
        Ok(ZqValue::String(text))
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_rtrimstr
pub(crate) fn rtrimstr_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(text), ZqValue::String(suffix)) = (a, b) else {
        return Err("endswith() requires string inputs".to_string());
    };
    if text.ends_with(&suffix) {
        let end = text.len().saturating_sub(suffix.len());
        Ok(ZqValue::String(text[..end].to_string()))
    } else {
        Ok(ZqValue::String(text))
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_trimstr
pub(crate) fn trimstr_jq(a: ZqValue, b: ZqValue) -> Result<ZqValue, String> {
    let left = ltrimstr_jq(a, b.clone())?;
    rtrimstr_jq(left, b)
}

// moved-from: src/native_engine/vm_core/vm.rs::TrimMode
#[derive(Debug, Clone, Copy)]
pub(crate) enum TrimMode {
    Left,
    Right,
    Both,
}

// moved-from: src/native_engine/vm_core/vm.rs::run_trim
pub(crate) fn trim_whitespace_jq(input: ZqValue, mode: TrimMode) -> Result<ZqValue, String> {
    let ZqValue::String(text) = input else {
        return Err("trim input must be a string".to_string());
    };
    let trimmed = match mode {
        TrimMode::Left => text.trim_start_matches(char::is_whitespace).to_string(),
        TrimMode::Right => text.trim_end_matches(char::is_whitespace).to_string(),
        TrimMode::Both => text.trim_matches(char::is_whitespace).to_string(),
    };
    Ok(ZqValue::String(trimmed))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_ascii_case
pub(crate) fn ascii_case_jq(input: ZqValue, upcase: bool) -> Result<ZqValue, String> {
    match input {
        ZqValue::String(s) => {
            let mapped = s
                .chars()
                .map(|ch| {
                    if upcase && ch.is_ascii_lowercase() {
                        (ch as u8 - b'a' + b'A') as char
                    } else if !upcase && ch.is_ascii_uppercase() {
                        (ch as u8 - b'A' + b'a') as char
                    } else {
                        ch
                    }
                })
                .collect::<String>();
            Ok(ZqValue::String(mapped))
        }
        _ => Err("explode input must be a string".to_string()),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_explode
pub(crate) fn explode_jq(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::String(s) => Ok(ZqValue::Array(
            s.chars().map(|ch| ZqValue::from(ch as i64)).collect(),
        )),
        _ => Err("explode input must be a string".to_string()),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_utf8bytelength
pub(crate) fn utf8_byte_length_jq(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::String(s) => Ok(ZqValue::from(s.len() as i64)),
        other => Err(format!(
            "{} ({}) only strings have UTF-8 byte length",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_implode
pub(crate) fn implode_jq(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Array(values) = input else {
        return Err("implode input must be an array".to_string());
    };
    let mut out = String::new();
    for value in values {
        let ZqValue::Number(number) = value else {
            let ty = if matches!(value, ZqValue::Null) {
                "number"
            } else {
                c_value::type_name_jq(&value)
            };
            return Err(format!(
                "{} ({}) can't be imploded, unicode codepoint needs to be numeric",
                ty,
                c_value::value_for_error_jq(&value)
            ));
        };
        let Some(raw) = number.as_f64() else {
            let value = ZqValue::Number(number);
            return Err(format!(
                "{} ({}) can't be imploded, unicode codepoint needs to be numeric",
                c_value::type_name_jq(&value),
                c_value::value_for_error_jq(&value)
            ));
        };
        if raw.is_nan() {
            let value = ZqValue::Null;
            return Err(format!(
                "number ({}) can't be imploded, unicode codepoint needs to be numeric",
                c_value::value_for_error_jq(&value)
            ));
        }
        let mut codepoint = c_math::dtoi_compat(raw);
        if !(0..=0x10FFFF).contains(&codepoint) || (0xD800..=0xDFFF).contains(&(codepoint as i32)) {
            codepoint = 0xFFFD;
        }
        let ch = char::from_u32(codepoint as u32).unwrap_or('\u{FFFD}');
        out.push(ch);
    }
    Ok(ZqValue::String(out))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_format
pub(crate) fn format_value_jq(fmt: &str, value: &ZqValue) -> Result<String, String> {
    match fmt {
        // jq parser.y StringStart defaults to "text" for string interpolation.
        "text" => c_json::tostring_value_jq(value),
        "json" => serde_json::to_string(&value.clone().into_json())
            .map_err(|e| format!("encode value: {e}")),
        "base64" => Ok(base64::engine::general_purpose::STANDARD
            .encode(c_json::tostring_value_jq(value)?.as_bytes())),
        "base64d" => decode_base64_to_string_jq(&c_json::tostring_value_jq(value)?),
        "uri" => Ok(encode_uri_bytes_jq(
            c_json::tostring_value_jq(value)?.as_bytes(),
        )),
        "urid" => decode_uri_jq(&c_json::tostring_value_jq(value)?),
        "html" => Ok(escape_html_jq(&c_json::tostring_value_jq(value)?)),
        "sh" => Ok(shell_quote_single_jq(&c_json::tostring_value_jq(value)?)),
        "csv" => format_row_jq(value, ","),
        "tsv" => format_row_jq(value, "\t"),
        _ => Err(format!("{fmt} is not a valid format")),
    }
}

fn format_row_jq(row: &ZqValue, sep: &str) -> Result<String, String> {
    let ZqValue::Array(items) = row else {
        return Err(format!(
            "cannot use {} ({}) as {}-formatted value",
            c_value::type_name_jq(row),
            c_value::value_for_error_jq(row),
            sep_name_jq(sep)
        ));
    };

    Ok(items
        .iter()
        .map(|v| match v {
            ZqValue::String(s) => {
                if sep == "," {
                    let escaped = s.replace('"', "\"\"");
                    format!("\"{escaped}\"")
                } else {
                    s.replace('\t', "\\t")
                }
            }
            _ => {
                serde_json::to_string(&v.clone().into_json()).unwrap_or_else(|_| "null".to_string())
            }
        })
        .collect::<Vec<_>>()
        .join(sep))
}

fn sep_name_jq(sep: &str) -> &'static str {
    if sep == "," {
        "csv"
    } else {
        "tsv"
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::normalize_index
pub(crate) fn normalize_index_jq(len: usize, index: i64) -> Option<usize> {
    let idx = if index >= 0 {
        index
    } else {
        len as i64 + index
    };
    if (0..len as i64).contains(&idx) {
        Some(idx as usize)
    } else {
        None
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::string_index_like_jq
pub(crate) fn string_index_like_jq(s: &str, index: i64) -> Option<ZqValue> {
    let chars = s.chars().collect::<Vec<_>>();
    let idx = normalize_index_jq(chars.len(), index)?;
    Some(ZqValue::String(chars[idx].to_string()))
}

// moved-from: src/native_engine/vm_core/vm.rs::encode_uri_bytes
pub(crate) fn encode_uri_bytes_jq(bytes: &[u8]) -> String {
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

// moved-from: src/native_engine/vm_core/vm.rs::decode_uri
pub(crate) fn decode_uri_jq(s: &str) -> Result<String, String> {
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(format!("string ({quoted}) is not a valid uri encoding"));
            }
            let h1 = hex_val(bytes[i + 1])
                .ok_or_else(|| format!("string ({quoted}) is not a valid uri encoding"))?;
            let h2 = hex_val(bytes[i + 2])
                .ok_or_else(|| format!("string ({quoted}) is not a valid uri encoding"))?;
            out.push((h1 << 4) | h2);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| format!("string ({quoted}) is not a valid uri encoding"))
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::decode_base64_to_string
pub(crate) fn decode_base64_to_string_jq(s: &str) -> Result<String, String> {
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
    if s.is_empty() || s.bytes().all(|b| b == b'=') {
        return Ok(String::new());
    }
    if s.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(format!("string ({quoted}) is not valid base64 data"));
    }
    if s.len() % 4 == 1 {
        return Err(format!("string ({quoted}) trailing base64 byte found"));
    }

    let mut raw = s.as_bytes().to_vec();
    while !raw.len().is_multiple_of(4) {
        raw.push(b'=');
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .map_err(|_| format!("string ({quoted}) is not valid base64 data"))?;
    String::from_utf8(decoded).map_err(|_| format!("string ({quoted}) is not valid base64 data"))
}

// moved-from: src/native_engine/vm_core/vm.rs::escape_html
pub(crate) fn escape_html_jq(s: &str) -> String {
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

// moved-from: src/native_engine/vm_core/vm.rs::shell_quote_single
pub(crate) fn shell_quote_single_jq(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_encode_decode_roundtrip() {
        let encoded = encode_uri_bytes_jq("a b".as_bytes());
        assert_eq!(encoded, "a%20b");
        let decoded = decode_uri_jq(&encoded).expect("decode");
        assert_eq!(decoded, "a b");
    }

    #[test]
    fn decode_base64_to_string_reports_invalid_payloads() {
        let err = decode_base64_to_string_jq("***").expect_err("invalid base64");
        assert!(err.contains("not valid base64 data"));
    }

    #[test]
    fn html_and_shell_escaping_follow_expected_shapes() {
        assert_eq!(escape_html_jq("<a&b>"), "&lt;a&amp;b&gt;".to_string());
        assert_eq!(shell_quote_single_jq("a b"), "'a b'".to_string());
    }

    #[test]
    fn split_and_trim_family_follow_jq_shapes() {
        let split = split_jq(
            ZqValue::String("a,b".to_string()),
            ZqValue::String(",".to_string()),
        )
        .expect("split");
        assert_eq!(split.into_json(), serde_json::json!(["a", "b"]));

        let split_chars = split_jq(
            ZqValue::String("ab".to_string()),
            ZqValue::String(String::new()),
        )
        .expect("split chars");
        assert_eq!(split_chars.into_json(), serde_json::json!(["a", "b"]));

        let trimmed =
            trim_whitespace_jq(ZqValue::String("  a  ".to_string()), TrimMode::Both).expect("trim");
        assert_eq!(trimmed.into_json(), serde_json::json!("a"));

        let ltrim = ltrimstr_jq(
            ZqValue::String("foobar".to_string()),
            ZqValue::String("foo".to_string()),
        )
        .expect("ltrimstr");
        assert_eq!(ltrim.into_json(), serde_json::json!("bar"));

        let starts = startswith_jq(
            ZqValue::String("foobar".to_string()),
            ZqValue::String("foo".to_string()),
        )
        .expect("startswith");
        assert_eq!(starts.into_json(), serde_json::json!(true));

        let ends = endswith_jq(
            ZqValue::String("foobar".to_string()),
            ZqValue::String("bar".to_string()),
        )
        .expect("endswith");
        assert_eq!(ends.into_json(), serde_json::json!(true));

        assert_eq!(normalize_index_jq(5, 1), Some(1));
        assert_eq!(normalize_index_jq(5, -1), Some(4));
        assert_eq!(normalize_index_jq(5, -6), None);

        let idx = string_index_like_jq("abc", -1).expect("index");
        assert_eq!(idx.into_json(), serde_json::json!("c"));

        let up = ascii_case_jq(ZqValue::String("AbC".to_string()), true).expect("upcase");
        assert_eq!(up.into_json(), serde_json::json!("ABC"));

        let exploded = explode_jq(ZqValue::String("A".to_string())).expect("explode");
        assert_eq!(exploded.into_json(), serde_json::json!([65]));

        let utf8_len =
            utf8_byte_length_jq(ZqValue::String("a✓".to_string())).expect("utf8 byte len");
        assert_eq!(utf8_len.into_json(), serde_json::json!(4));

        let imploded = implode_jq(ZqValue::Array(vec![ZqValue::from(65), ZqValue::from(66)]))
            .expect("implode");
        assert_eq!(imploded.into_json(), serde_json::json!("AB"));

        let csv = format_value_jq(
            "csv",
            &ZqValue::Array(vec![ZqValue::String("a,b".to_string()), ZqValue::from(2)]),
        )
        .expect("csv");
        assert_eq!(csv, "\"a,b\",2".to_string());
    }
}
