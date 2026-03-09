use crate::cli::OutputFormat;

use super::{json_color::JsonColorPalette, Error};

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
        zq::NativeValue::Null => Err(Error::Query(
            "encode toml: null is not supported in TOML output".to_string(),
        )),
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
            Err(Error::Query(format!(
                "encode toml: unsupported number `{v}`"
            )))
        }
        zq::NativeValue::String(v) => Ok(toml::Value::String(v.clone())),
        zq::NativeValue::Array(values) => {
            let converted = values
                .iter()
                .map(native_to_toml_value)
                .collect::<Result<Vec<_>, _>>()?;
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
        if values
            .iter()
            .all(|value| matches!(value, zq::NativeValue::Object(_)))
        {
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
                    zq::NativeValue::Array(items) => items
                        .iter()
                        .map(native_to_csv_cell)
                        .collect::<Result<Vec<_>, _>>()?,
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
        writer
            .flush()
            .map_err(|e| Error::Query(format!("encode csv: {e}")))?;
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

pub(super) fn render_xml_output_native(values: &[zq::NativeValue]) -> Result<String, Error> {
    if values.is_empty() {
        return Ok(String::new());
    }
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(render_xml_doc_native(value)?);
    }
    Ok(docs.join("\n"))
}

fn render_xml_doc_native(value: &zq::NativeValue) -> Result<String, Error> {
    let mut out = String::new();
    match value {
        zq::NativeValue::Object(map) if map.len() == 1 => {
            let (root, content) = map
                .iter()
                .next()
                .expect("single-key object must have one entry");
            if root != "#text" && !root.starts_with('@') && is_valid_xml_name(root) {
                write_xml_field_native(&mut out, root, content)?;
            } else {
                write_xml_field_native(&mut out, "root", value)?;
            }
        }
        _ => write_xml_field_native(&mut out, "root", value)?,
    }
    Ok(out)
}

fn write_xml_field_native(
    out: &mut String,
    name: &str,
    value: &zq::NativeValue,
) -> Result<(), Error> {
    match value {
        zq::NativeValue::Array(items) => {
            for item in items {
                write_xml_element_native(out, name, item)?;
            }
        }
        _ => write_xml_element_native(out, name, value)?,
    }
    Ok(())
}

fn write_xml_element_native(
    out: &mut String,
    name: &str,
    value: &zq::NativeValue,
) -> Result<(), Error> {
    if !is_valid_xml_name(name) {
        return Err(Error::Query(format!(
            "encode xml: invalid element name `{name}`"
        )));
    }

    match value {
        zq::NativeValue::Null => {
            out.push('<');
            out.push_str(name);
            out.push_str("/>");
            Ok(())
        }
        zq::NativeValue::Bool(_) | zq::NativeValue::Number(_) | zq::NativeValue::String(_) => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            out.push_str(&escape_xml_text(&xml_scalar_text(value)?));
            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
        zq::NativeValue::Array(items) => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            for item in items {
                write_xml_field_native(out, "item", item)?;
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
        zq::NativeValue::Object(fields) => {
            out.push('<');
            out.push_str(name);

            for (key, attr_value) in fields.iter().filter(|(k, _)| k.starts_with('@')) {
                let attr_name = &key[1..];
                if attr_name.is_empty() || !is_valid_xml_name(attr_name) {
                    return Err(Error::Query(format!(
                        "encode xml: invalid attribute name `{key}`"
                    )));
                }
                out.push(' ');
                out.push_str(attr_name);
                out.push_str("=\"");
                out.push_str(&escape_xml_attribute(&xml_scalar_text(attr_value)?));
                out.push('"');
            }

            let children = fields
                .iter()
                .filter(|(k, _)| *k != "#text" && !k.starts_with('@'))
                .collect::<Vec<_>>();
            let text = fields.get("#text");

            if children.is_empty() && text.is_none() {
                out.push_str("/>");
                return Ok(());
            }

            out.push('>');

            if let Some(text_value) = text {
                out.push_str(&escape_xml_text(&xml_scalar_text(text_value)?));
            }

            for (child_name, child_value) in children {
                write_xml_field_native(out, child_name, child_value)?;
            }

            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
    }
}

fn xml_scalar_text(value: &zq::NativeValue) -> Result<String, Error> {
    match value {
        zq::NativeValue::Null => Ok(String::new()),
        zq::NativeValue::Bool(v) => Ok(v.to_string()),
        zq::NativeValue::Number(v) => Ok(v.to_string()),
        zq::NativeValue::String(v) => Ok(v.clone()),
        zq::NativeValue::Array(_) | zq::NativeValue::Object(_) => Err(Error::Query(
            "encode xml: scalar value expected".to_string(),
        )),
    }
}

fn is_valid_xml_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().expect("name is not empty");
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

fn escape_xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_xml_attribute(value: &str) -> String {
    escape_xml_text(value)
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub(super) fn colorize_structured_output(
    format: OutputFormat,
    rendered: &str,
    enabled: bool,
    jq_colors: Option<&str>,
) -> String {
    if !enabled {
        return rendered.to_string();
    }
    let palette = JsonColorPalette::from_jq_colors(jq_colors);
    match format {
        OutputFormat::Yaml => colorize_key_value_text(rendered, ':', &palette),
        OutputFormat::Toml => colorize_key_value_text(rendered, '=', &palette),
        // Keep CSV machine-valid even on TTY: no ANSI in delimiters/fields.
        OutputFormat::Csv => rendered.to_string(),
        // Keep XML machine-valid even on TTY: no ANSI in tags/text.
        OutputFormat::Xml => rendered.to_string(),
        OutputFormat::Json => rendered.to_string(),
    }
}

fn colorize_key_value_text(rendered: &str, separator: char, palette: &JsonColorPalette) -> String {
    let mut out = String::with_capacity(rendered.len() + rendered.len() / 8);
    for part in rendered.split_inclusive('\n') {
        let (line, newline) = if let Some(prefix) = part.strip_suffix('\n') {
            (prefix, "\n")
        } else {
            (part, "")
        };
        if line.trim().is_empty() {
            out.push_str(line);
            out.push_str(newline);
            continue;
        }

        if line.trim_start().starts_with("---") {
            out.push_str(&palette.obj);
            out.push_str(line);
            out.push_str(&palette.reset);
            out.push_str(newline);
            continue;
        }

        if line.trim_start().starts_with('#') {
            out.push_str(&palette.null);
            out.push_str(line);
            out.push_str(&palette.reset);
            out.push_str(newline);
            continue;
        }

        let (body, comment) = split_unquoted_comment(line);
        let mut body_out = String::new();

        if separator == '=' && looks_like_toml_section_header(body.trim()) {
            body_out.push_str(&colorize_toml_section_header(body, palette));
        } else if let Some(idx) = find_unquoted_separator(body, separator) {
            let (left, right_with_sep) = body.split_at(idx);
            let mut key = left;
            let mut prefix = "";
            if separator == ':' {
                let trimmed = left.trim_start();
                if trimmed.starts_with("- ") {
                    let leading_ws = left.len() - trimmed.len();
                    prefix = &left[..leading_ws + 2];
                    key = &left[leading_ws + 2..];
                }
            }
            if !key.trim().is_empty() {
                body_out.push_str(prefix);
                body_out.push_str(&palette.key);
                body_out.push_str(key);
                body_out.push_str(&palette.reset);
                let sep_len = separator.len_utf8();
                body_out.push_str(&palette.obj);
                body_out.push_str(&right_with_sep[..sep_len]);
                body_out.push_str(&palette.reset);
                body_out.push_str(&colorize_value_tokens(&right_with_sep[sep_len..], palette));
            } else {
                body_out.push_str(&colorize_value_tokens(body, palette));
            }
        } else {
            body_out.push_str(&colorize_value_tokens(body, palette));
        }

        out.push_str(&body_out);
        if let Some(comment) = comment {
            out.push_str(&palette.null);
            out.push_str(comment);
            out.push_str(&palette.reset);
        }
        out.push_str(newline);
    }
    out
}

fn colorize_value_tokens(input: &str, palette: &JsonColorPalette) -> String {
    let mut out = String::with_capacity(input.len() + input.len() / 8);
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '"' || ch == '\'' {
            let quote = ch;
            let start = i;
            i += 1;
            let mut escaped = false;
            while i < chars.len() {
                let c = chars[i];
                if quote == '"' {
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if c == quote {
                        i += 1;
                        break;
                    }
                } else if c == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            let token = chars[start..i].iter().collect::<String>();
            out.push_str(&palette.str);
            out.push_str(&token);
            out.push_str(&palette.reset);
            continue;
        }

        if ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.' | '_') {
            let start = i;
            i += 1;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '+' | '-' | '.' | '_'))
            {
                i += 1;
            }
            let token = chars[start..i].iter().collect::<String>();
            let lower = token.to_ascii_lowercase();
            if lower == "true" {
                out.push_str(&palette.r#true);
                out.push_str(&token);
                out.push_str(&palette.reset);
            } else if lower == "false" {
                out.push_str(&palette.r#false);
                out.push_str(&token);
                out.push_str(&palette.reset);
            } else if lower == "null" || lower == "nil" {
                out.push_str(&palette.null);
                out.push_str(&token);
                out.push_str(&palette.reset);
            } else if looks_like_number_token(&token) {
                out.push_str(&palette.num);
                out.push_str(&token);
                out.push_str(&palette.reset);
            } else {
                out.push_str(&token);
            }
            continue;
        }

        if matches!(ch, '[' | ']' | '{' | '}' | '(' | ')' | ',' | ':' | '=') {
            out.push_str(&palette.obj);
            out.push(ch);
            out.push_str(&palette.reset);
        } else {
            out.push(ch);
        }
        i += 1;
    }
    out
}

fn find_unquoted_separator(line: &str, separator: char) -> Option<usize> {
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;
    for (idx, ch) in line.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
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
        if ch == '"' {
            in_double = true;
            continue;
        }
        if ch == '\'' {
            in_single = true;
            continue;
        }
        if ch == separator {
            return Some(idx);
        }
    }
    None
}

fn split_unquoted_comment(line: &str) -> (&str, Option<&str>) {
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;
    for (idx, ch) in line.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
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
        if ch == '"' {
            in_double = true;
            continue;
        }
        if ch == '\'' {
            in_single = true;
            continue;
        }
        if ch == '#' {
            return (&line[..idx], Some(&line[idx..]));
        }
    }
    (line, None)
}

fn looks_like_toml_section_header(line: &str) -> bool {
    line.starts_with('[') && line.ends_with(']') && line.len() >= 2
}

fn colorize_toml_section_header(line: &str, palette: &JsonColorPalette) -> String {
    let leading_ws_len = line.len() - line.trim_start().len();
    let trailing_ws_len = line.len() - line.trim_end().len();
    let trimmed = line.trim();
    let prefix = &line[..leading_ws_len];
    let suffix = &line[line.len() - trailing_ws_len..];
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut out = String::with_capacity(line.len() + 32);
    out.push_str(prefix);
    out.push_str(&palette.obj);
    out.push('[');
    out.push_str(&palette.reset);
    out.push_str(&palette.key);
    out.push_str(inner);
    out.push_str(&palette.reset);
    out.push_str(&palette.obj);
    out.push(']');
    out.push_str(&palette.reset);
    out.push_str(suffix);
    out
}

fn looks_like_number_token(token: &str) -> bool {
    if token.is_empty() || token == "+" || token == "-" || token == "." {
        return false;
    }
    let canonical = token.replace('_', "");
    canonical.parse::<i64>().is_ok()
        || canonical.parse::<u64>().is_ok()
        || canonical.parse::<f64>().is_ok()
}
