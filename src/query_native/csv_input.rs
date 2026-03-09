use super::{Error, JsonValue, ZqValue};
use std::collections::HashSet;

pub(super) fn parse_csv_native_rows(
    input: &str,
    require_delimited_shape: bool,
) -> Result<Vec<ZqValue>, Error> {
    let delimiter = if require_delimited_shape {
        detect_csv_delimiter(input, true)
            .ok_or_else(|| Error::Runtime("csv: cannot detect delimiter".to_string()))?
    } else {
        detect_csv_delimiter(input, false).unwrap_or(b',')
    };
    parse_csv_native_rows_with_delimiter(input, delimiter)
}

pub(super) fn parse_csv_native_rows_auto(input: &str) -> Option<Vec<ZqValue>> {
    parse_csv_native_rows(input, true).ok()
}

fn parse_csv_native_rows_with_delimiter(input: &str, delimiter: u8) -> Result<Vec<ZqValue>, Error> {
    let mut probe = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .from_reader(input.as_bytes());
    let mut sample = Vec::with_capacity(2);
    for next in probe.records().take(2) {
        sample.push(next.map_err(|e| Error::Runtime(format!("csv: {e}")))?);
    }
    let has_headers = csv_rows_look_like_header(&sample);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .delimiter(delimiter)
        .from_reader(input.as_bytes());
    let headers = if has_headers {
        Some(
            reader
                .headers()
                .map_err(|e| Error::Runtime(format!("csv: {e}")))?
                .clone(),
        )
    } else {
        None
    };

    let mut out = Vec::new();
    for next in reader.records() {
        let record = next.map_err(|e| Error::Runtime(format!("csv: {e}")))?;
        if let Some(headers) = headers.as_ref() {
            let mut obj = serde_json::Map::with_capacity(headers.len());
            for (idx, key) in headers.iter().enumerate() {
                let value = record.get(idx).unwrap_or_default();
                obj.insert(key.to_string(), JsonValue::String(value.to_string()));
            }
            out.push(ZqValue::from_json(JsonValue::Object(obj)));
        } else {
            let arr = record
                .iter()
                .map(|value| JsonValue::String(value.to_string()))
                .collect::<Vec<_>>();
            out.push(ZqValue::from_json(JsonValue::Array(arr)));
        }
    }
    Ok(out)
}

fn csv_rows_look_like_header(rows: &[csv::StringRecord]) -> bool {
    if rows.len() < 2 {
        return false;
    }
    let header = &rows[0];
    let first_data = &rows[1];
    if header.is_empty() || header.len() != first_data.len() {
        return false;
    }
    let mut seen = HashSet::with_capacity(header.len());
    for key in header.iter() {
        let trimmed = key.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            return false;
        }
    }
    header.iter().zip(first_data.iter()).any(|(h, v)| h != v)
}

fn detect_csv_delimiter(input: &str, require_multiple_lines: bool) -> Option<u8> {
    let lines = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(8)
        .collect::<Vec<_>>();
    if lines.is_empty() || (require_multiple_lines && lines.len() < 2) {
        return None;
    }

    let candidates = [b',', b';', b'\t'];
    let mut best: Option<(u8, usize)> = None;
    for delimiter in candidates {
        let split_count = |line: &str| line.split(delimiter as char).count();
        let counts = lines
            .iter()
            .map(|line| split_count(line))
            .collect::<Vec<_>>();
        let max_fields = counts.iter().copied().max().unwrap_or(1);
        if max_fields < 2 {
            continue;
        }
        let matching_lines = counts.iter().filter(|&&count| count == max_fields).count();
        if require_multiple_lines && matching_lines < 2 {
            continue;
        }
        if !require_multiple_lines && matching_lines < 1 {
            continue;
        }
        let score = max_fields * matching_lines;
        if best
            .map(|(_, best_score)| score > best_score)
            .unwrap_or(true)
        {
            best = Some((delimiter, score));
        }
    }
    best.map(|(delimiter, _)| delimiter)
}
