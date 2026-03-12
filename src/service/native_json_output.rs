use super::json_color::{render_json_value_colored, write_jq_style_escaped_del, JsonColorOptions};
use super::Error;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::io::{self, Write};

pub(super) fn write_json_native_value_line<W: Write>(
    writer: &mut W,
    value: &zq::NativeValue,
    compact: bool,
    raw_output: bool,
    scratch: &mut Vec<u8>,
    pretty_indent: Option<&[u8]>,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    if raw_output {
        if let Some(s) = value.as_str() {
            writer.write_all(s.as_bytes())?;
            return Ok(());
        }
    }

    if color_opts.enabled {
        let json_value = native_value_to_cli_json_compat(value);
        let rendered = render_json_value_colored(
            &json_value,
            compact,
            color_opts.jq_colors.as_deref(),
            color_opts.indent,
            color_opts.legacy_compact_colors,
        )?;
        writer.write_all(&rendered)?;
        return Ok(());
    }

    scratch.clear();
    if compact {
        let mut serializer = serde_json::Serializer::new(&mut *scratch);
        NativeCliJsonCompat(value)
            .serialize(&mut serializer)
            .map_err(|e| Error::Query(format!("encode output: {e}")))?;
    } else {
        let indent = pretty_indent.unwrap_or(&[]);
        let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
        let mut serializer = serde_json::Serializer::with_formatter(&mut *scratch, formatter);
        NativeCliJsonCompat(value)
            .serialize(&mut serializer)
            .map_err(|e| Error::Query(format!("encode output: {e}")))?;
    }
    write_jq_style_escaped_del(writer, scratch)?;
    Ok(())
}

pub(super) fn render_raw_output0_native(
    values: &[zq::NativeValue],
    compact: bool,
) -> Result<(Vec<u8>, Option<Error>), Error> {
    let tool = super::cli_error_tool_name();
    let mut out = Vec::new();
    let mut scratch = Vec::new();
    for value in values {
        if let Some(s) = value.as_str() {
            if s.contains('\0') {
                return Ok((
                    out,
                    Some(Error::Query(
                        format!(
                            "{tool}: error (at <stdin>:0): Cannot dump a string containing NUL with --raw-output0 option"
                        ),
                    )),
                ));
            }
            out.extend_from_slice(s.as_bytes());
            out.push(0);
            continue;
        }

        scratch.clear();
        if compact {
            let mut serializer = serde_json::Serializer::new(&mut scratch);
            NativeCliJsonCompat(value)
                .serialize(&mut serializer)
                .map_err(|e| Error::Query(format!("encode json: {e}")))?;
        } else {
            let formatter = serde_json::ser::PrettyFormatter::with_indent(b"  ");
            let mut serializer = serde_json::Serializer::with_formatter(&mut scratch, formatter);
            NativeCliJsonCompat(value)
                .serialize(&mut serializer)
                .map_err(|e| Error::Query(format!("encode json: {e}")))?;
        }
        out.extend_from_slice(&scratch);
        out.push(0);
    }
    Ok((out, None))
}

#[cfg(test)]
pub(super) fn render_native_value_colored(
    value: &zq::NativeValue,
    compact: bool,
    jq_colors: Option<&str>,
    indent: usize,
) -> Result<Vec<u8>, Error> {
    let json_value = native_value_to_cli_json_compat(value);
    render_json_value_colored(&json_value, compact, jq_colors, indent, false)
}

struct NativeCliJsonCompat<'a>(&'a zq::NativeValue);

impl Serialize for NativeCliJsonCompat<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            zq::NativeValue::Null => serializer.serialize_unit(),
            zq::NativeValue::Bool(v) => serializer.serialize_bool(*v),
            zq::NativeValue::Number(number) => {
                serialize_native_number_cli_compat(number, serializer)
            }
            zq::NativeValue::String(text) => serializer.serialize_str(text),
            zq::NativeValue::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&NativeCliJsonCompat(item))?;
                }
                seq.end()
            }
            zq::NativeValue::Object(map) => {
                let mut object = serializer.serialize_map(Some(map.len()))?;
                for (key, value) in map {
                    object.serialize_entry(key, &NativeCliJsonCompat(value))?;
                }
                object.end()
            }
        }
    }
}

fn serialize_native_number_cli_compat<S>(
    number: &serde_json::Number,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if number.is_i64() || number.is_u64() || number.is_f64() {
        return number.serialize(serializer);
    }

    let raw = number.to_string();
    let unsigned = raw.strip_prefix('-').or_else(|| raw.strip_prefix('+')).unwrap_or(&raw);
    let lower = unsigned.to_ascii_lowercase();

    if lower.starts_with("nan") {
        return serializer.serialize_unit();
    }

    if lower == "inf" || lower == "infinity" {
        let finite = if raw.starts_with('-') {
            "-1.7976931348623157e+308"
        } else {
            "1.7976931348623157e+308"
        };
        let finite_number = serde_json::Number::from_string_unchecked(finite.to_string());
        return finite_number.serialize(serializer);
    }

    number.serialize(serializer)
}

fn native_value_to_cli_json_compat(value: &zq::NativeValue) -> JsonValue {
    match value {
        zq::NativeValue::Null => JsonValue::Null,
        zq::NativeValue::Bool(v) => JsonValue::Bool(*v),
        zq::NativeValue::Number(number) => native_number_to_cli_json_compat(number),
        zq::NativeValue::String(text) => JsonValue::String(text.clone()),
        zq::NativeValue::Array(items) => {
            JsonValue::Array(items.iter().map(native_value_to_cli_json_compat).collect::<Vec<_>>())
        }
        zq::NativeValue::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (key, value) in map {
                out.insert(key.clone(), native_value_to_cli_json_compat(value));
            }
            JsonValue::Object(out)
        }
    }
}

fn native_number_to_cli_json_compat(number: &serde_json::Number) -> JsonValue {
    if number.is_i64() || number.is_u64() || number.is_f64() {
        return JsonValue::Number(number.clone());
    }

    let raw = number.to_string();
    let unsigned = raw.strip_prefix('-').or_else(|| raw.strip_prefix('+')).unwrap_or(&raw);
    let lower = unsigned.to_ascii_lowercase();

    if lower.starts_with("nan") {
        return JsonValue::Null;
    }

    if lower == "inf" || lower == "infinity" {
        let finite = if raw.starts_with('-') {
            "-1.7976931348623157e+308"
        } else {
            "1.7976931348623157e+308"
        };
        return JsonValue::Number(serde_json::Number::from_string_unchecked(finite.to_string()));
    }

    JsonValue::Number(number.clone())
}

pub(super) fn write_json_output_native<W: Write>(
    writer: &mut W,
    values: &[zq::NativeValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    let mut scratch = Vec::new();
    let pretty_indent =
        if compact || color_opts.enabled { None } else { Some(vec![b' '; color_opts.indent]) };
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 && !join_output {
            writer.write_all(b"\n")?;
        }
        write_json_native_value_line(
            writer,
            value,
            compact,
            raw_output,
            &mut scratch,
            pretty_indent.as_deref(),
            color_opts,
        )?;
    }
    if !values.is_empty() && !join_output {
        writer.write_all(b"\n")?;
    }
    Ok(())
}

pub(super) fn write_json_output_lines_native(
    values: &[zq::NativeValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    const IO_BUFFER_CAP: usize = 64 * 1024;
    let stdout = io::stdout();
    let mut writer = io::BufWriter::with_capacity(IO_BUFFER_CAP, stdout.lock());
    write_json_output_native(&mut writer, values, compact, raw_output, join_output, color_opts)?;
    writer.flush()?;
    Ok(())
}

pub(super) fn exit_status_from_outputs_native(outputs: &[zq::NativeValue]) -> i32 {
    match outputs.last() {
        None => 4,
        Some(zq::NativeValue::Null) => 1,
        Some(zq::NativeValue::Bool(false)) => 1,
        Some(_) => 0,
    }
}
