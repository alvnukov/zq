use super::Error;
use crate::cli::Cli;
#[cfg(test)]
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::io::{self, IsTerminal, Write};

#[derive(Debug, Clone)]
pub(super) struct JsonColorOptions {
    pub(super) enabled: bool,
    pub(super) jq_colors: Option<String>,
    pub(super) warn_invalid: bool,
    pub(super) indent: usize,
    pub(super) legacy_compact_colors: bool,
}

impl Default for JsonColorOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            jq_colors: None,
            warn_invalid: false,
            indent: 2,
            legacy_compact_colors: false,
        }
    }
}

pub(super) fn resolve_json_color_options(cli: &Cli) -> JsonColorOptions {
    let indent = cli.indent.unwrap_or(2) as usize;
    let enabled = if cli.monochrome_output {
        false
    } else if cli.color_output {
        true
    } else if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
        false
    } else {
        io::stdout().is_terminal()
    };

    if !enabled {
        return JsonColorOptions {
            indent,
            ..JsonColorOptions::default()
        };
    }

    let mut out = JsonColorOptions {
        enabled: true,
        jq_colors: None,
        warn_invalid: false,
        indent,
        legacy_compact_colors: false,
    };

    if let Ok(raw) = std::env::var("JQ_COLORS") {
        if validate_jq_colors(&raw) {
            out.jq_colors = Some(raw);
        } else {
            out.warn_invalid = true;
        }
    }

    // jq171 uses a slightly different compact color-token emission pattern.
    // Keep modern jq behavior by default; enable legacy mode explicitly.
    if std::env::var("ZQ_COLOR_COMPAT").ok().as_deref() == Some("jq171") {
        out.legacy_compact_colors = true;
    }

    out
}

pub(super) fn validate_jq_colors(raw: &str) -> bool {
    raw.split(':').all(validate_jq_color_style)
}

fn validate_jq_color_style(style: &str) -> bool {
    if style.is_empty() {
        return true;
    }
    if !style.chars().all(|ch| ch.is_ascii_digit() || ch == ';') {
        return false;
    }
    style
        .split(';')
        // jq accepts empty fields (e.g. ":" or "1;;31"), but reject absurdly
        // large numeric atoms used by jq171 invalid-palette stress tests.
        .all(|atom| atom.is_empty() || atom.parse::<u8>().is_ok())
}

#[cfg(test)]
pub(super) fn render_raw_output0(
    values: &[JsonValue],
    compact: bool,
) -> Result<(Vec<u8>, Option<Error>), Error> {
    let mut out = Vec::new();
    for value in values {
        if let Some(s) = value.as_str() {
            if s.contains('\0') {
                return Ok((
                    out,
                    Some(Error::Query(
                        "zq: error (at <stdin>:0): Cannot dump a string containing NUL with --raw-output0 option".to_string(),
                    )),
                ));
            }
            out.extend_from_slice(s.as_bytes());
            out.push(0);
            continue;
        }

        let rendered = if compact {
            serde_json::to_string(value).map_err(|e| Error::Query(format!("encode json: {e}")))?
        } else {
            serde_json::to_string_pretty(value)
                .map_err(|e| Error::Query(format!("encode json: {e}")))?
        };
        out.extend_from_slice(rendered.as_bytes());
        out.push(0);
    }
    Ok((out, None))
}

#[cfg(test)]
pub(super) fn write_json_output<W: Write>(
    writer: &mut W,
    values: &[JsonValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    if values.is_empty() {
        return Ok(());
    }
    let mut json_scratch = Vec::new();

    for (idx, value) in values.iter().enumerate() {
        if idx > 0 && !join_output {
            writer.write_all(b"\n")?;
        }
        write_json_value_line(
            writer,
            value,
            compact,
            raw_output,
            &mut json_scratch,
            color_opts,
        )?;
    }
    if !join_output {
        writer.write_all(b"\n")?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn write_json_value_line<W: Write>(
    writer: &mut W,
    value: &JsonValue,
    compact: bool,
    raw_output: bool,
    scratch: &mut Vec<u8>,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    if raw_output {
        if let Some(s) = value.as_str() {
            writer.write_all(s.as_bytes())?;
            return Ok(());
        }
    }
    if color_opts.enabled {
        let rendered = render_json_value_colored(
            value,
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
        serde_json::to_writer(&mut *scratch, value)
            .map_err(|e| Error::Query(format!("encode json: {e}")))?;
    } else if color_opts.indent == 2 {
        serde_json::to_writer_pretty(&mut *scratch, value)
            .map_err(|e| Error::Query(format!("encode json: {e}")))?;
    } else {
        let indent = vec![b' '; color_opts.indent];
        let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent);
        let mut serializer = serde_json::Serializer::with_formatter(&mut *scratch, formatter);
        value
            .serialize(&mut serializer)
            .map_err(|e| Error::Query(format!("encode json: {e}")))?;
    }
    write_jq_style_escaped_del(writer, scratch)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(super) struct JsonColorPalette {
    pub(super) null: String,
    pub(super) r#false: String,
    pub(super) r#true: String,
    pub(super) num: String,
    pub(super) str: String,
    pub(super) arr: String,
    pub(super) obj: String,
    pub(super) key: String,
    pub(super) reset: String,
}

impl JsonColorPalette {
    pub(super) fn from_jq_colors(raw: Option<&str>) -> Self {
        let mut styles = [
            "0;90".to_string(),
            "0;39".to_string(),
            "0;39".to_string(),
            "0;39".to_string(),
            "0;32".to_string(),
            "1;39".to_string(),
            "1;39".to_string(),
            "1;34".to_string(),
        ];
        if let Some(raw) = raw {
            for (idx, style) in raw.split(':').enumerate() {
                if idx >= styles.len() {
                    break;
                }
                styles[idx] = style.to_string();
            }
        }
        Self::from_styles([
            styles[0].as_str(),
            styles[1].as_str(),
            styles[2].as_str(),
            styles[3].as_str(),
            styles[4].as_str(),
            styles[5].as_str(),
            styles[6].as_str(),
            styles[7].as_str(),
        ])
    }

    fn from_styles(styles: [&str; 8]) -> Self {
        let paint = |style: &str| format!("\x1b[{style}m");
        Self {
            null: paint(styles[0]),
            r#false: paint(styles[1]),
            r#true: paint(styles[2]),
            num: paint(styles[3]),
            str: paint(styles[4]),
            arr: paint(styles[5]),
            obj: paint(styles[6]),
            key: paint(styles[7]),
            reset: "\x1b[0m".to_string(),
        }
    }
}

pub(super) fn render_json_value_colored(
    value: &JsonValue,
    compact: bool,
    jq_colors: Option<&str>,
    indent: usize,
    legacy_compact_colors: bool,
) -> Result<Vec<u8>, Error> {
    let palette = JsonColorPalette::from_jq_colors(jq_colors);
    let mut out = Vec::new();
    if legacy_compact_colors && compact {
        write_json_value_colored_legacy_compact(&mut out, value, None, &palette)?;
    } else {
        write_json_value_colored(&mut out, value, compact, 0, indent, &palette)?;
    }
    Ok(out)
}

fn write_json_value_colored_legacy_compact<W: Write>(
    writer: &mut W,
    value: &JsonValue,
    parent_style: Option<&str>,
    palette: &JsonColorPalette,
) -> Result<(), Error> {
    match value {
        JsonValue::Null => {
            write_colored_scalar_legacy(writer, "null", &palette.null, &palette.reset, parent_style)
        }
        JsonValue::Bool(true) => write_colored_scalar_legacy(
            writer,
            "true",
            &palette.r#true,
            &palette.reset,
            parent_style,
        ),
        JsonValue::Bool(false) => write_colored_scalar_legacy(
            writer,
            "false",
            &palette.r#false,
            &palette.reset,
            parent_style,
        ),
        JsonValue::Number(n) => write_colored_scalar_legacy(
            writer,
            n.to_string().as_str(),
            &palette.num,
            &palette.reset,
            parent_style,
        ),
        JsonValue::String(s) => {
            let rendered =
                serde_json::to_string(s).map_err(|e| Error::Query(format!("encode json: {e}")))?;
            write_colored_scalar_legacy(
                writer,
                &rendered,
                &palette.str,
                &palette.reset,
                parent_style,
            )
        }
        JsonValue::Array(items) => {
            writer.write_all(palette.arr.as_bytes())?;
            writer.write_all(b"[")?;
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    writer.write_all(b",")?;
                }
                write_json_value_colored_legacy_compact(writer, item, Some(&palette.arr), palette)?;
            }
            writer.write_all(palette.arr.as_bytes())?;
            writer.write_all(b"]")?;
            writer.write_all(palette.reset.as_bytes())?;
            if let Some(parent) = parent_style {
                writer.write_all(parent.as_bytes())?;
            }
            Ok(())
        }
        JsonValue::Object(map) => {
            writer.write_all(palette.obj.as_bytes())?;
            writer.write_all(b"{")?;
            for (idx, (key, item)) in map.iter().enumerate() {
                if idx > 0 {
                    writer.write_all(b",")?;
                }
                let rendered_key = serde_json::to_string(key)
                    .map_err(|e| Error::Query(format!("encode json: {e}")))?;
                write_colored_scalar_legacy(
                    writer,
                    &rendered_key,
                    &palette.key,
                    &palette.reset,
                    Some(&palette.obj),
                )?;
                writer.write_all(b":")?;
                writer.write_all(palette.reset.as_bytes())?;
                write_json_value_colored_legacy_compact(writer, item, Some(&palette.obj), palette)?;
            }
            writer.write_all(palette.obj.as_bytes())?;
            writer.write_all(b"}")?;
            writer.write_all(palette.reset.as_bytes())?;
            if let Some(parent) = parent_style {
                writer.write_all(parent.as_bytes())?;
            }
            Ok(())
        }
    }
}

fn write_colored_scalar_legacy<W: Write>(
    writer: &mut W,
    token: &str,
    style: &str,
    reset: &str,
    parent_style: Option<&str>,
) -> Result<(), Error> {
    if parent_style.is_some() && !style.starts_with("\x1b[0") {
        writer.write_all(reset.as_bytes())?;
    }
    writer.write_all(style.as_bytes())?;
    writer.write_all(token.as_bytes())?;
    writer.write_all(reset.as_bytes())?;
    if let Some(parent) = parent_style {
        writer.write_all(parent.as_bytes())?;
    }
    Ok(())
}

fn write_json_value_colored<W: Write>(
    writer: &mut W,
    value: &JsonValue,
    compact: bool,
    depth: usize,
    indent: usize,
    palette: &JsonColorPalette,
) -> Result<(), Error> {
    match value {
        JsonValue::Null => write_colored_token(writer, "null", &palette.null, &palette.reset),
        JsonValue::Bool(true) => {
            write_colored_token(writer, "true", &palette.r#true, &palette.reset)
        }
        JsonValue::Bool(false) => {
            write_colored_token(writer, "false", &palette.r#false, &palette.reset)
        }
        JsonValue::Number(n) => {
            write_colored_token(writer, &n.to_string(), &palette.num, &palette.reset)
        }
        JsonValue::String(s) => {
            let rendered =
                serde_json::to_string(s).map_err(|e| Error::Query(format!("encode json: {e}")))?;
            write_colored_token(writer, &rendered, &palette.str, &palette.reset)
        }
        JsonValue::Array(items) => {
            write_colored_token(writer, "[", &palette.arr, &palette.reset)?;
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    write_colored_token(writer, ",", &palette.arr, &palette.reset)?;
                }
                if !compact {
                    writer.write_all(b"\n")?;
                    writer.write_all(" ".repeat((depth + 1) * indent).as_bytes())?;
                }
                write_json_value_colored(writer, item, compact, depth + 1, indent, palette)?;
            }
            if !compact && !items.is_empty() {
                writer.write_all(b"\n")?;
                writer.write_all(" ".repeat(depth * indent).as_bytes())?;
            }
            write_colored_token(writer, "]", &palette.arr, &palette.reset)
        }
        JsonValue::Object(map) => {
            write_colored_token(writer, "{", &palette.obj, &palette.reset)?;
            for (idx, (key, item)) in map.iter().enumerate() {
                if idx > 0 {
                    write_colored_token(writer, ",", &palette.obj, &palette.reset)?;
                }
                if !compact {
                    writer.write_all(b"\n")?;
                    writer.write_all(" ".repeat((depth + 1) * indent).as_bytes())?;
                }
                let rendered_key = serde_json::to_string(key)
                    .map_err(|e| Error::Query(format!("encode json: {e}")))?;
                write_colored_token(writer, &rendered_key, &palette.key, &palette.reset)?;
                write_colored_token(writer, ":", &palette.obj, &palette.reset)?;
                if !compact {
                    writer.write_all(b" ")?;
                }
                write_json_value_colored(writer, item, compact, depth + 1, indent, palette)?;
            }
            if !compact && !map.is_empty() {
                writer.write_all(b"\n")?;
                writer.write_all(" ".repeat(depth * indent).as_bytes())?;
            }
            write_colored_token(writer, "}", &palette.obj, &palette.reset)
        }
    }
}

fn write_colored_token<W: Write>(
    writer: &mut W,
    token: &str,
    style: &str,
    reset: &str,
) -> Result<(), Error> {
    writer.write_all(style.as_bytes())?;
    writer.write_all(token.as_bytes())?;
    writer.write_all(reset.as_bytes())?;
    Ok(())
}

#[cfg(test)]
pub(super) fn render_json_line(
    value: &JsonValue,
    compact: bool,
    raw_output: bool,
) -> Result<String, Error> {
    let mut out = Vec::new();
    let mut scratch = Vec::new();
    write_json_value_line(
        &mut out,
        value,
        compact,
        raw_output,
        &mut scratch,
        &JsonColorOptions::default(),
    )?;
    String::from_utf8(out).map_err(|e| Error::Query(format!("encode json: {e}")))
}

pub(super) fn write_jq_style_escaped_del<W: Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    for &b in bytes {
        if b == 0x7f {
            writer.write_all(b"\\u007f")?;
        } else {
            writer.write_all(&[b])?;
        }
    }
    Ok(())
}
