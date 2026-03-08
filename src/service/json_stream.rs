use serde_json::Value as JsonValue;

use super::{
    index_to_line_col, strip_serde_line_col_suffix, unfinished_abandoned_at_eof_message,
    SeqParseResult, SeqParseResultNative,
};

pub(super) fn parse_json_seq_input_native(input: &str) -> SeqParseResultNative {
    let mut result = SeqParseResultNative::default();
    let rs = '\u{1e}';
    let rs_positions = input
        .char_indices()
        .filter_map(|(idx, ch)| (ch == rs).then_some(idx))
        .collect::<Vec<_>>();

    if rs_positions.is_empty() {
        if !input.trim().is_empty() {
            let msg = unfinished_abandoned_at_eof_message(input);
            result.errors.push(msg);
        }
        return result;
    }

    for (i, &rs_idx) in rs_positions.iter().enumerate() {
        let start = rs_idx + rs.len_utf8();
        let end = rs_positions.get(i + 1).copied().unwrap_or(input.len());
        let chunk = &input[start..end];
        if chunk.trim().is_empty() {
            continue;
        }

        let mut parse_error = false;
        for next in serde_json::Deserializer::from_str(chunk).into_iter::<zq::NativeValue>() {
            match next {
                Ok(v) => result.values.push(v),
                Err(_) => {
                    parse_error = true;
                    break;
                }
            }
        }

        if parse_error {
            if end == input.len() {
                let (line, col) = index_to_line_col(input, end, true);
                result.errors.push(format!(
                    "Unfinished abandoned text at EOF at line {line}, column {col}"
                ));
            } else {
                let (line, col) = index_to_line_col(input, end, false);
                result
                    .errors
                    .push(format!("Truncated value at line {line}, column {col}"));
            }
        }
    }

    result
}

#[cfg(test)]
pub(super) fn parse_json_seq_input(input: &str) -> SeqParseResult {
    let parsed = parse_json_seq_input_native(input);
    SeqParseResult {
        values: parsed
            .values
            .into_iter()
            .map(zq::NativeValue::into_json)
            .collect(),
        errors: parsed.errors,
    }
}

#[cfg(test)]
pub(super) fn stream_json_values(values: Vec<JsonValue>) -> Vec<JsonValue> {
    stream_native_values(
        values
            .into_iter()
            .map(zq::NativeValue::from_json)
            .collect::<Vec<_>>(),
    )
    .into_iter()
    .map(zq::NativeValue::into_json)
    .collect()
}

pub(super) fn stream_native_values(values: Vec<zq::NativeValue>) -> Vec<zq::NativeValue> {
    let mut out = Vec::new();
    for value in values {
        let mut path = Vec::new();
        append_stream_events_native(&value, &mut path, &mut out);
    }
    out
}

fn append_stream_events_native(
    value: &zq::NativeValue,
    path: &mut Vec<zq::NativeValue>,
    out: &mut Vec<zq::NativeValue>,
) {
    match value {
        zq::NativeValue::Array(items) => {
            if items.is_empty() {
                out.push(zq::NativeValue::Array(vec![
                    zq::NativeValue::Array(path.clone()),
                    zq::NativeValue::Array(Vec::new()),
                ]));
                return;
            }
            for (idx, item) in items.iter().enumerate() {
                path.push(zq::NativeValue::from(idx as i64));
                append_stream_events_native(item, path, out);
                path.pop();
            }
            let last = items.len() - 1;
            path.push(zq::NativeValue::from(last as i64));
            out.push(zq::NativeValue::Array(vec![zq::NativeValue::Array(
                path.clone(),
            )]));
            path.pop();
        }
        zq::NativeValue::Object(map) => {
            if map.is_empty() {
                out.push(zq::NativeValue::Array(vec![
                    zq::NativeValue::Array(path.clone()),
                    zq::NativeValue::Object(Default::default()),
                ]));
                return;
            }
            let mut last_key = None::<String>;
            for (key, item) in map {
                path.push(zq::NativeValue::String(key.clone()));
                append_stream_events_native(item, path, out);
                path.pop();
                last_key = Some(key.clone());
            }
            if let Some(last_key) = last_key {
                path.push(zq::NativeValue::String(last_key));
                out.push(zq::NativeValue::Array(vec![zq::NativeValue::Array(
                    path.clone(),
                )]));
                path.pop();
            }
        }
        _ => {
            out.push(zq::NativeValue::Array(vec![
                zq::NativeValue::Array(path.clone()),
                value.clone(),
            ]));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JsonArrayState {
    ValueOrEnd,
    CommaOrEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JsonObjectState {
    KeyOrEnd,
    Colon,
    Value,
    CommaOrEnd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum JsonScanFrame {
    Array {
        index: usize,
        state: JsonArrayState,
    },
    Object {
        key: Option<String>,
        state: JsonObjectState,
    },
}

#[cfg(test)]
pub(super) fn stream_error_value_from_json_error(
    input: &str,
    err: &serde_json::Error,
) -> JsonValue {
    JsonValue::Array(vec![
        JsonValue::String(json_parse_error_message(err)),
        JsonValue::Array(stream_error_path_from_input(input, err)),
    ])
}

pub(super) fn stream_error_value_from_json_error_native(
    input: &str,
    err: &serde_json::Error,
) -> zq::NativeValue {
    zq::NativeValue::Array(vec![
        zq::NativeValue::String(json_parse_error_message(err)),
        zq::NativeValue::Array(stream_error_path_from_input_native(input, err)),
    ])
}

fn stream_error_path_from_input_native(
    input: &str,
    err: &serde_json::Error,
) -> Vec<zq::NativeValue> {
    let idx = line_col_to_byte_index(input, err.line(), err.column()).unwrap_or(input.len());
    let frames = scan_stream_error_frames(input, idx.min(input.len()));
    if frames.is_empty() {
        return vec![zq::NativeValue::from(0u64)];
    }

    let mut path = Vec::new();
    for frame in frames {
        match frame {
            JsonScanFrame::Array { index, .. } => {
                path.push(zq::NativeValue::from(index as u64));
            }
            JsonScanFrame::Object { key, state } => match state {
                JsonObjectState::Value | JsonObjectState::CommaOrEnd => match key {
                    Some(k) => path.push(zq::NativeValue::String(k)),
                    None => path.push(zq::NativeValue::Null),
                },
                JsonObjectState::KeyOrEnd | JsonObjectState::Colon => {
                    path.push(zq::NativeValue::Null)
                }
            },
        }
    }
    if path.is_empty() {
        path.push(zq::NativeValue::from(0u64));
    }
    path
}

#[cfg(test)]
fn stream_error_path_from_input(input: &str, err: &serde_json::Error) -> Vec<JsonValue> {
    stream_error_path_from_input_native(input, err)
        .into_iter()
        .map(zq::NativeValue::into_json)
        .collect()
}

pub(super) fn line_col_to_byte_index(input: &str, line: usize, col: usize) -> Option<usize> {
    if line == 0 || col == 0 {
        return None;
    }
    let mut current_line = 1usize;
    let mut current_col = 1usize;
    for (idx, ch) in input.char_indices() {
        if current_line == line && current_col == col {
            return Some(idx);
        }
        if ch == '\n' {
            current_line = current_line.saturating_add(1);
            current_col = 1;
        } else {
            current_col = current_col.saturating_add(1);
        }
    }
    if current_line == line && current_col == col {
        return Some(input.len());
    }
    Some(input.len())
}

fn scan_stream_error_frames(input: &str, limit: usize) -> Vec<JsonScanFrame> {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_started = false;
    let mut root_done = false;

    while i < limit {
        skip_json_ws(bytes, &mut i, limit);
        if i >= limit || root_done {
            break;
        }
        if !root_started {
            if !scan_json_value(bytes, &mut i, limit, &mut frames, &mut root_done) {
                break;
            }
            root_started = true;
            continue;
        }
        if !advance_json_scan(bytes, &mut i, limit, &mut frames, &mut root_done) {
            break;
        }
    }

    frames
}

pub(super) fn advance_json_scan(
    bytes: &[u8],
    i: &mut usize,
    limit: usize,
    frames: &mut Vec<JsonScanFrame>,
    root_done: &mut bool,
) -> bool {
    let Some(top) = frames.last().cloned() else {
        *root_done = true;
        return true;
    };

    match top {
        JsonScanFrame::Array { state, .. } => match state {
            JsonArrayState::ValueOrEnd => {
                if *i < limit && bytes[*i] == b']' {
                    *i += 1;
                    close_json_container(frames, root_done);
                    true
                } else {
                    scan_json_value(bytes, i, limit, frames, root_done)
                }
            }
            JsonArrayState::CommaOrEnd => {
                if *i < limit && bytes[*i] == b',' {
                    if let Some(JsonScanFrame::Array { index, state }) = frames.last_mut() {
                        *index = index.saturating_add(1);
                        *state = JsonArrayState::ValueOrEnd;
                    }
                    *i += 1;
                    true
                } else if *i < limit && bytes[*i] == b']' {
                    *i += 1;
                    close_json_container(frames, root_done);
                    true
                } else {
                    false
                }
            }
        },
        JsonScanFrame::Object { state, .. } => match state {
            JsonObjectState::KeyOrEnd => {
                if *i < limit && bytes[*i] == b'}' {
                    *i += 1;
                    close_json_container(frames, root_done);
                    true
                } else if let Some(key) = scan_json_string(bytes, i, limit) {
                    if let Some(JsonScanFrame::Object {
                        key: frame_key,
                        state,
                    }) = frames.last_mut()
                    {
                        *frame_key = Some(key);
                        *state = JsonObjectState::Colon;
                    }
                    true
                } else {
                    false
                }
            }
            JsonObjectState::Colon => {
                if *i < limit && bytes[*i] == b':' {
                    if let Some(JsonScanFrame::Object { state, .. }) = frames.last_mut() {
                        *state = JsonObjectState::Value;
                    }
                    *i += 1;
                    true
                } else {
                    false
                }
            }
            JsonObjectState::Value => scan_json_value(bytes, i, limit, frames, root_done),
            JsonObjectState::CommaOrEnd => {
                if *i < limit && bytes[*i] == b',' {
                    if let Some(JsonScanFrame::Object { key, state }) = frames.last_mut() {
                        *key = None;
                        *state = JsonObjectState::KeyOrEnd;
                    }
                    *i += 1;
                    true
                } else if *i < limit && bytes[*i] == b'}' {
                    *i += 1;
                    close_json_container(frames, root_done);
                    true
                } else {
                    false
                }
            }
        },
    }
}

pub(super) fn scan_json_value(
    bytes: &[u8],
    i: &mut usize,
    limit: usize,
    frames: &mut Vec<JsonScanFrame>,
    root_done: &mut bool,
) -> bool {
    if *i >= limit {
        return false;
    }

    match bytes[*i] {
        b'{' => {
            frames.push(JsonScanFrame::Object {
                key: None,
                state: JsonObjectState::KeyOrEnd,
            });
            *i += 1;
            true
        }
        b'[' => {
            frames.push(JsonScanFrame::Array {
                index: 0,
                state: JsonArrayState::ValueOrEnd,
            });
            *i += 1;
            true
        }
        b'"' => {
            if scan_json_string(bytes, i, limit).is_none() {
                return false;
            }
            complete_json_value(frames, root_done);
            true
        }
        b'-' | b'0'..=b'9' => {
            if !scan_json_number(bytes, i, limit) {
                return false;
            }
            complete_json_value(frames, root_done);
            true
        }
        b't' => {
            if !scan_json_literal(bytes, i, limit, b"true") {
                return false;
            }
            complete_json_value(frames, root_done);
            true
        }
        b'f' => {
            if !scan_json_literal(bytes, i, limit, b"false") {
                return false;
            }
            complete_json_value(frames, root_done);
            true
        }
        b'n' => {
            if !scan_json_literal(bytes, i, limit, b"null") {
                return false;
            }
            complete_json_value(frames, root_done);
            true
        }
        _ => false,
    }
}

fn skip_json_ws(bytes: &[u8], i: &mut usize, limit: usize) {
    while *i < limit && bytes[*i].is_ascii_whitespace() {
        *i += 1;
    }
}

pub(super) fn scan_json_string(bytes: &[u8], i: &mut usize, limit: usize) -> Option<String> {
    if *i >= limit || bytes[*i] != b'"' {
        return None;
    }
    let start = *i;
    *i += 1;
    let mut escaped = false;
    while *i < limit {
        let b = bytes[*i];
        if escaped {
            escaped = false;
            *i += 1;
            continue;
        }
        if b == b'\\' {
            escaped = true;
            *i += 1;
            continue;
        }
        if b == b'"' {
            *i += 1;
            return serde_json::from_slice::<String>(&bytes[start..*i]).ok();
        }
        *i += 1;
    }
    None
}

pub(super) fn scan_json_number(bytes: &[u8], i: &mut usize, limit: usize) -> bool {
    let start = *i;

    if *i < limit && bytes[*i] == b'-' {
        *i += 1;
    }
    if *i >= limit {
        return false;
    }

    match bytes[*i] {
        b'0' => *i += 1,
        b'1'..=b'9' => {
            *i += 1;
            while *i < limit && bytes[*i].is_ascii_digit() {
                *i += 1;
            }
        }
        _ => return false,
    }

    if *i < limit && bytes[*i] == b'.' {
        *i += 1;
        if *i >= limit || !bytes[*i].is_ascii_digit() {
            return false;
        }
        while *i < limit && bytes[*i].is_ascii_digit() {
            *i += 1;
        }
    }

    if *i < limit && matches!(bytes[*i], b'e' | b'E') {
        *i += 1;
        if *i < limit && matches!(bytes[*i], b'+' | b'-') {
            *i += 1;
        }
        if *i >= limit || !bytes[*i].is_ascii_digit() {
            return false;
        }
        while *i < limit && bytes[*i].is_ascii_digit() {
            *i += 1;
        }
    }

    *i > start
}

pub(super) fn scan_json_literal(bytes: &[u8], i: &mut usize, limit: usize, lit: &[u8]) -> bool {
    if *i + lit.len() > limit {
        return false;
    }
    if bytes[*i..*i + lit.len()] == *lit {
        *i += lit.len();
        true
    } else {
        false
    }
}

pub(super) fn complete_json_value(frames: &mut [JsonScanFrame], root_done: &mut bool) {
    if let Some(top) = frames.last_mut() {
        match top {
            JsonScanFrame::Array { state, .. } => {
                if *state == JsonArrayState::ValueOrEnd {
                    *state = JsonArrayState::CommaOrEnd;
                }
            }
            JsonScanFrame::Object { state, .. } => {
                if *state == JsonObjectState::Value {
                    *state = JsonObjectState::CommaOrEnd;
                }
            }
        }
        return;
    }
    *root_done = true;
}

fn close_json_container(frames: &mut Vec<JsonScanFrame>, root_done: &mut bool) {
    let _ = frames.pop();
    complete_json_value(frames, root_done);
}

pub(super) fn json_parse_error_message(err: &serde_json::Error) -> String {
    let raw = err.to_string();
    let mut col = err.column();
    let message = if raw
        .starts_with("control character (\\u0000-\\u001F) found while parsing a string")
    {
        col = col.saturating_add(1);
        "Invalid string: control characters from U+0000 through U+001F must be escaped".to_string()
    } else if raw.starts_with("expected `:`") {
        "Objects must consist of key:value pairs".to_string()
    } else if raw.starts_with("EOF while parsing a string") {
        "Unfinished string at EOF".to_string()
    } else if raw.starts_with("EOF while parsing") {
        "Unfinished JSON term at EOF".to_string()
    } else {
        strip_serde_line_col_suffix(&raw).to_string()
    };

    format!("{message} at line {}, column {col}", err.line())
}
