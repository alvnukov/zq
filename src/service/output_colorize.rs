use crate::cli::OutputFormat;

use super::json_color::JsonColorPalette;

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
