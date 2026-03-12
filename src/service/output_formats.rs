use super::Error;

pub(super) fn render_toml_output_native(values: &[zq::NativeValue]) -> Result<String, Error> {
    if values.is_empty() {
        return Ok(String::new());
    }
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        let mut toml_value = native_to_toml_value(value)?;
        if !matches!(toml_value, toml::Value::Table(_)) {
            let mut wrapped = toml::map::Map::new();
            wrapped.insert("value".to_string(), toml_value);
            toml_value = toml::Value::Table(wrapped);
        }
        let rendered = toml::to_string_pretty(&toml_value)
            .map_err(|e| Error::Query(format!("encode toml: {e}")))?;
        docs.push(rendered);
    }
    Ok(docs.join("\n"))
}

fn native_to_toml_value(value: &zq::NativeValue) -> Result<toml::Value, Error> {
    match value {
        zq::NativeValue::Null => {
            Err(Error::Query("encode toml: null is not supported in TOML output".to_string()))
        }
        zq::NativeValue::Bool(v) => Ok(toml::Value::Boolean(*v)),
        zq::NativeValue::Number(v) => {
            if let Some(i) = v.as_i64() {
                return Ok(toml::Value::Integer(i));
            }
            if let Some(u) = v.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    return Ok(toml::Value::Integer(i));
                }
            }
            if let Some(f) = v.as_f64() {
                return Ok(toml::Value::Float(f));
            }
            Err(Error::Query(format!("encode toml: unsupported number `{v}`")))
        }
        zq::NativeValue::String(v) => Ok(toml::Value::String(v.clone())),
        zq::NativeValue::Array(values) => {
            let converted =
                values.iter().map(native_to_toml_value).collect::<Result<Vec<_>, _>>()?;
            Ok(toml::Value::Array(converted))
        }
        zq::NativeValue::Object(fields) => {
            let mut table = toml::map::Map::new();
            for (key, value) in fields {
                table.insert(key.clone(), native_to_toml_value(value)?);
            }
            Ok(toml::Value::Table(table))
        }
    }
}

pub(super) fn render_csv_output_native(values: &[zq::NativeValue]) -> Result<String, Error> {
    let mut out = Vec::new();
    {
        let mut writer = csv::WriterBuilder::new().from_writer(&mut out);
        if values.iter().all(|value| matches!(value, zq::NativeValue::Object(_))) {
            let headers = collect_csv_headers(values);
            writer
                .write_record(headers.iter())
                .map_err(|e| Error::Query(format!("encode csv: {e}")))?;
            for value in values {
                let zq::NativeValue::Object(obj) = value else {
                    continue;
                };
                let row = headers
                    .iter()
                    .map(|header| {
                        obj.get(header)
                            .map(native_to_csv_cell)
                            .transpose()
                            .map(|cell| cell.unwrap_or_default())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                writer
                    .write_record(row.iter())
                    .map_err(|e| Error::Query(format!("encode csv: {e}")))?;
            }
        } else {
            // RFC-style CSV expects a stable column count for all records.
            let width = values
                .iter()
                .map(|value| match value {
                    zq::NativeValue::Array(items) => items.len(),
                    _ => 1,
                })
                .max()
                .unwrap_or(1)
                .max(1);
            for value in values {
                let mut row = match value {
                    zq::NativeValue::Array(items) => {
                        items.iter().map(native_to_csv_cell).collect::<Result<Vec<_>, _>>()?
                    }
                    other => {
                        let cell = native_to_csv_cell(other)?;
                        vec![cell]
                    }
                };
                if row.len() < width {
                    row.resize(width, String::new());
                }
                writer
                    .write_record(row.iter())
                    .map_err(|e| Error::Query(format!("encode csv: {e}")))?;
            }
        }
        writer.flush().map_err(|e| Error::Query(format!("encode csv: {e}")))?;
    }
    String::from_utf8(out).map_err(|e| Error::Query(format!("encode csv: {e}")))
}

fn collect_csv_headers(values: &[zq::NativeValue]) -> Vec<String> {
    let mut headers = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for value in values {
        let zq::NativeValue::Object(obj) = value else {
            continue;
        };
        for key in obj.keys() {
            if seen.insert(key.clone()) {
                headers.push(key.clone());
            }
        }
    }
    headers
}

fn native_to_csv_cell(value: &zq::NativeValue) -> Result<String, Error> {
    match value {
        zq::NativeValue::Null => Ok(String::new()),
        zq::NativeValue::Bool(v) => Ok(v.to_string()),
        zq::NativeValue::Number(v) => Ok(v.to_string()),
        zq::NativeValue::String(v) => Ok(v.clone()),
        zq::NativeValue::Array(_) | zq::NativeValue::Object(_) => {
            serde_json::to_string(value).map_err(|e| Error::Query(format!("encode csv: {e}")))
        }
    }
}
