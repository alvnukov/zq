use super::*;

pub(super) fn run_tostream(input: ZqValue) -> Vec<ZqValue> {
    let mut out = Vec::new();
    let mut path = Vec::new();
    append_tostream_events(&input, &mut path, &mut out);
    out
}

fn append_tostream_events(value: &ZqValue, path: &mut Vec<ZqValue>, out: &mut Vec<ZqValue>) {
    match value {
        ZqValue::Array(items) => {
            if items.is_empty() {
                out.push(ZqValue::Array(vec![
                    ZqValue::Array(path.clone()),
                    ZqValue::Array(Vec::new()),
                ]));
                return;
            }
            for (idx, item) in items.iter().enumerate() {
                path.push(ZqValue::from(idx as i64));
                append_tostream_events(item, path, out);
                let _ = path.pop();
            }
            let mut close_path = path.clone();
            close_path.push(ZqValue::from((items.len() - 1) as i64));
            out.push(ZqValue::Array(vec![ZqValue::Array(close_path)]));
        }
        ZqValue::Object(map) => {
            if map.is_empty() {
                out.push(ZqValue::Array(vec![
                    ZqValue::Array(path.clone()),
                    ZqValue::Object(IndexMap::new()),
                ]));
                return;
            }
            let mut last_key = String::new();
            for (key, item) in map {
                last_key = key.clone();
                path.push(ZqValue::String(key.clone()));
                append_tostream_events(item, path, out);
                let _ = path.pop();
            }
            let mut close_path = path.clone();
            close_path.push(ZqValue::String(last_key));
            out.push(ZqValue::Array(vec![ZqValue::Array(close_path)]));
        }
        _ => out.push(ZqValue::Array(vec![
            ZqValue::Array(path.clone()),
            value.clone(),
        ])),
    }
}

pub(super) fn run_truncate_stream(stream: &Op, n: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq/src/builtin.jq:
    // . as $n | null | stream | . as $input
    // | if (.[0]|length) > $n then setpath([0]; $input[0][$n:]) else empty end;
    let stream_values = eval_many(stream, &ZqValue::Null)?;
    let mut out = Vec::new();
    for event in stream_values {
        if let Some(event) = truncate_stream_event(event, &n)? {
            out.push(event);
        }
    }
    Ok(out)
}

fn truncate_stream_event(event: ZqValue, n: &ZqValue) -> Result<Option<ZqValue>, String> {
    let ZqValue::Array(items) = event else {
        return Err("truncate_stream: stream event must be an array".to_string());
    };
    if items.is_empty() || items.len() > 2 {
        return Err("truncate_stream: invalid stream event shape".to_string());
    }
    let ZqValue::Array(path) = &items[0] else {
        return Err("truncate_stream: stream path must be an array".to_string());
    };

    let path_len = ZqValue::from(path.len() as i64);
    if jq_cmp(&path_len, n) != Ordering::Greater {
        return Ok(None);
    }

    let start = truncate_stream_start_index(n, path.len())?;
    let truncated_path = ZqValue::Array(path[start..].to_vec());
    let mut out_items = Vec::with_capacity(items.len());
    out_items.push(truncated_path);
    if items.len() == 2 {
        out_items.push(items[1].clone());
    }
    Ok(Some(ZqValue::Array(out_items)))
}

fn truncate_stream_start_index(n: &ZqValue, len: usize) -> Result<usize, String> {
    let ZqValue::Number(number) = n else {
        return Err(format!("Cannot index array with {}", type_name(n)));
    };
    let Some(raw) = number.as_f64() else {
        return Err("number is out of range".to_string());
    };
    let mut start = c_math::dtoi_compat(raw);
    let len_i64 = len as i64;
    if start < 0 {
        start += len_i64;
    }
    if start < 0 {
        start = 0;
    }
    if start > len_i64 {
        start = len_i64;
    }
    Ok(start as usize)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamPathComp {
    Index(usize),
    Key(String),
}

#[derive(Debug, Clone)]
struct StreamEventValue {
    path: Vec<StreamPathComp>,
    value: Option<ZqValue>,
}

pub(super) fn run_fromstream(stream: &Op, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    let raw_events = eval_many(stream, &input)?;
    let events = raw_events
        .into_iter()
        .map(parse_stream_event_value)
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < events.len() {
        if events[idx].path.is_empty() {
            let Some(value) = events[idx].value.clone() else {
                return Err("fromstream: invalid root close marker".to_string());
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
    events: &[StreamEventValue],
    idx: usize,
    path: &[StreamPathComp],
) -> Result<(ZqValue, usize), String> {
    if idx >= events.len() {
        return Err("fromstream: unexpected end of stream".to_string());
    }
    let event = &events[idx];

    if event.path == path {
        let Some(value) = event.value.clone() else {
            return Err("fromstream: close marker without value".to_string());
        };
        return Ok((value, idx + 1));
    }

    if !stream_path_is_prefix(path, &event.path) || event.path.len() <= path.len() {
        return Err("fromstream: malformed stream path".to_string());
    }

    let kind = event.path[path.len()].clone();
    decode_stream_container_at(events, idx, path, kind)
}

fn decode_stream_container_at(
    events: &[StreamEventValue],
    mut idx: usize,
    path: &[StreamPathComp],
    kind: StreamPathComp,
) -> Result<(ZqValue, usize), String> {
    let mut array = Vec::new();
    let mut object = IndexMap::new();

    loop {
        if idx >= events.len() {
            return Err("fromstream: unexpected end while decoding container".to_string());
        }
        let current = &events[idx];
        if !stream_path_is_prefix(path, &current.path) || current.path.len() <= path.len() {
            return Err("fromstream: malformed container stream".to_string());
        }

        let child_key = current.path[path.len()].clone();
        if !stream_comp_kind_matches(&kind, &child_key) {
            return Err("fromstream: mixed container key types".to_string());
        }

        let mut child_path = path.to_vec();
        child_path.push(child_key.clone());
        let (child_value, next_idx) = decode_stream_node_at(events, idx, &child_path)?;
        match child_key {
            StreamPathComp::Index(i) => {
                if i > array.len() {
                    array.resize(i, ZqValue::Null);
                }
                if i == array.len() {
                    array.push(child_value);
                } else {
                    array[i] = child_value;
                }
            }
            StreamPathComp::Key(key) => {
                object.insert(key, child_value);
            }
        }
        idx = next_idx;

        if idx < events.len() && events[idx].value.is_none() && events[idx].path == child_path {
            idx += 1;
            return Ok(match kind {
                StreamPathComp::Index(_) => (ZqValue::Array(array), idx),
                StreamPathComp::Key(_) => (ZqValue::Object(object), idx),
            });
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

fn stream_path_is_prefix(prefix: &[StreamPathComp], full: &[StreamPathComp]) -> bool {
    prefix.len() <= full.len() && prefix.iter().zip(full.iter()).all(|(a, b)| a == b)
}

fn parse_stream_event_value(value: ZqValue) -> Result<StreamEventValue, String> {
    let ZqValue::Array(items) = value else {
        return Err("fromstream: stream event must be an array".to_string());
    };
    match items.len() {
        1 => Ok(StreamEventValue {
            path: parse_stream_path_value(items[0].clone())?,
            value: None,
        }),
        2 => Ok(StreamEventValue {
            path: parse_stream_path_value(items[0].clone())?,
            value: Some(items[1].clone()),
        }),
        _ => Err("fromstream: invalid stream event shape".to_string()),
    }
}

fn parse_stream_path_value(value: ZqValue) -> Result<Vec<StreamPathComp>, String> {
    let ZqValue::Array(segments) = value else {
        return Err("fromstream: stream path must be an array".to_string());
    };
    let mut out = Vec::with_capacity(segments.len());
    for segment in segments {
        match segment {
            ZqValue::String(key) => out.push(StreamPathComp::Key(key)),
            ZqValue::Number(index) => {
                let Some(index) = index.as_u64() else {
                    return Err("fromstream: path index must be a non-negative integer".to_string());
                };
                out.push(StreamPathComp::Index(index as usize));
            }
            _ => return Err("fromstream: path segment must be string or integer".to_string()),
        }
    }
    Ok(out)
}
