use super::*;

#[derive(Debug, Clone)]
struct RegexCaptureGroup {
    offset: i64,
    length: i64,
    string: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone)]
struct RegexMatchRecord {
    offset: i64,
    length: i64,
    string: String,
    captures: Vec<RegexCaptureGroup>,
}

#[derive(Debug)]
pub(super) struct CachedRegex {
    compiled: Regex,
    capture_names: Vec<Option<String>>,
}

impl CachedRegex {
    fn compile(pattern: &str) -> Result<Self, String> {
        let compiled = Regex::new(pattern).map_err(|err| format!("Regex failure: {err}"))?;
        let capture_names = compiled
            .capture_names()
            .map(|name| name.map(|name| name.to_string()))
            .collect::<Vec<_>>();
        Ok(Self {
            compiled,
            capture_names,
        })
    }
}

pub(super) fn run_regex_match(
    input: &ZqValue,
    spec: ZqValue,
    flags: Option<ZqValue>,
    test_mode: bool,
    tuple_mode: bool,
) -> Result<Vec<ZqValue>, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(input),
            value_for_error(input)
        ));
    };
    let (pattern, mode) = resolve_regex_spec_and_mode(spec, flags, tuple_mode)?;
    let config = parse_regex_mode(&mode)?;
    if test_mode {
        return Ok(vec![ZqValue::Bool(regex_has_match(
            text, &pattern, &config,
        )?)]);
    }
    let records = regex_collect_matches(text, &pattern, &config)?;
    Ok(records.into_iter().map(regex_record_to_value).collect())
}

pub(super) fn run_regex_capture(
    input: &ZqValue,
    spec: ZqValue,
    flags: Option<ZqValue>,
    tuple_mode: bool,
) -> Result<Vec<ZqValue>, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(input),
            value_for_error(input)
        ));
    };
    let (pattern, mode) = resolve_regex_spec_and_mode(spec, flags, tuple_mode)?;
    let config = parse_regex_mode(&mode)?;
    let records = regex_collect_matches(text, &pattern, &config)?;
    let mut out = Vec::with_capacity(records.len());
    for record in records {
        let mut object = IndexMap::new();
        for capture in record.captures {
            let Some(name) = capture.name else {
                continue;
            };
            let value = capture.string.map(ZqValue::String).unwrap_or(ZqValue::Null);
            object.insert(name, value);
        }
        out.push(ZqValue::Object(object));
    }
    Ok(out)
}

pub(super) fn run_regex_scan(
    input: &ZqValue,
    regex: ZqValue,
    flags: Option<ZqValue>,
) -> Result<Vec<ZqValue>, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(input),
            value_for_error(input)
        ));
    };
    let pattern = expect_regex_string(regex)?;
    // jq/src/builtin.jq: def scan($re; $flags): match($re; "g" + $flags)
    let mode_value = apply_binary(
        BinaryOp::Add,
        ZqValue::String("g".to_string()),
        flags.unwrap_or(ZqValue::Null),
    )?;
    let mode = parse_regex_modifier_value(mode_value)?;
    let config = parse_regex_mode(&mode)?;
    let records = regex_collect_matches(text, &pattern, &config)?;
    let mut out = Vec::with_capacity(records.len());
    for record in records {
        if record.captures.is_empty() {
            out.push(ZqValue::String(record.string));
        } else {
            out.push(ZqValue::Array(
                record
                    .captures
                    .into_iter()
                    .map(|capture| capture.string.map(ZqValue::String).unwrap_or(ZqValue::Null))
                    .collect(),
            ));
        }
    }
    Ok(out)
}

pub(super) fn run_regex_splits(
    input: &ZqValue,
    regex: ZqValue,
    flags: Option<ZqValue>,
) -> Result<Vec<ZqValue>, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(input),
            value_for_error(input)
        ));
    };
    let pattern = expect_regex_string(regex)?;
    // jq/src/builtin.jq: def splits($re; $flags): match($re; $flags + "g")
    let mode_value = apply_binary(
        BinaryOp::Add,
        flags.unwrap_or(ZqValue::Null),
        ZqValue::String("g".to_string()),
    )?;
    let mode = parse_regex_modifier_value(mode_value)?;
    let config = parse_regex_mode(&mode)?;
    let records = regex_collect_matches(text, &pattern, &config)?;
    let cp_to_byte = utf8_codepoint_to_byte_index_table(text);
    let total_cp = cp_to_byte.len().saturating_sub(1);

    let mut previous = 0usize;
    let mut out = Vec::with_capacity(records.len() + 1);
    for record in records {
        let offset =
            usize::try_from(record.offset).map_err(|_| "regex offset out of range".to_string())?;
        if offset > total_cp || offset < previous {
            return Err("regex offset out of range".to_string());
        }
        out.push(ZqValue::String(codepoint_slice(
            text,
            &cp_to_byte,
            previous,
            offset,
        )?));

        let length =
            usize::try_from(record.length).map_err(|_| "regex length out of range".to_string())?;
        previous = offset
            .checked_add(length)
            .ok_or_else(|| "regex length out of range".to_string())?;
        if previous > total_cp {
            return Err("regex length out of range".to_string());
        }
    }
    out.push(ZqValue::String(codepoint_slice(
        text,
        &cp_to_byte,
        previous,
        total_cp,
    )?));
    Ok(out)
}

pub(super) fn run_regex_sub(
    input: &ZqValue,
    regex: ZqValue,
    replacement: &Op,
    flags: ZqValue,
    global: bool,
) -> Result<Vec<ZqValue>, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(input),
            value_for_error(input)
        ));
    };
    let pattern = expect_regex_string(regex)?;
    let mode_value = if global {
        apply_binary(BinaryOp::Add, flags, ZqValue::String("g".to_string()))?
    } else {
        flags
    };
    let mode = parse_regex_modifier_value(mode_value)?;
    let config = parse_regex_mode(&mode)?;
    let edits = regex_collect_matches(text, &pattern, &config)?;
    if edits.is_empty() {
        return Ok(vec![ZqValue::String(text.to_string())]);
    }

    let cp_to_byte = utf8_codepoint_to_byte_index_table(text);
    let total_cp = cp_to_byte.len().saturating_sub(1);
    let mut previous = 0usize;
    let mut result_prefixes: Vec<ZqValue> = Vec::new();

    for edit in edits {
        let offset =
            usize::try_from(edit.offset).map_err(|_| "regex offset out of range".to_string())?;
        if offset > total_cp || offset < previous {
            return Err("regex offset out of range".to_string());
        }
        let gap = ZqValue::String(codepoint_slice(text, &cp_to_byte, previous, offset)?);

        let capture_object = regex_named_capture_object(&edit);
        let inserts = eval_many(replacement, &capture_object)?;
        for (idx, insert) in inserts.into_iter().enumerate() {
            let addition = binop_add(gap.clone(), insert, false)?;
            if idx >= result_prefixes.len() {
                result_prefixes.resize(idx + 1, ZqValue::Null);
            }
            let current = result_prefixes[idx].clone();
            result_prefixes[idx] = binop_add(current, addition, false)?;
        }

        let length =
            usize::try_from(edit.length).map_err(|_| "regex length out of range".to_string())?;
        previous = offset
            .checked_add(length)
            .ok_or_else(|| "regex length out of range".to_string())?;
        if previous > total_cp {
            return Err("regex length out of range".to_string());
        }
    }

    if result_prefixes.is_empty() {
        return Ok(vec![ZqValue::String(text.to_string())]);
    }

    let tail = ZqValue::String(codepoint_slice(text, &cp_to_byte, previous, total_cp)?);
    let mut out = Vec::with_capacity(result_prefixes.len());
    for prefix in result_prefixes {
        out.push(binop_add(prefix, tail.clone(), false)?);
    }
    Ok(out)
}

fn regex_named_capture_object(record: &RegexMatchRecord) -> ZqValue {
    let mut object = IndexMap::new();
    for capture in &record.captures {
        let Some(name) = &capture.name else {
            continue;
        };
        let value = capture
            .string
            .as_ref()
            .map(|value| ZqValue::String(value.clone()))
            .unwrap_or(ZqValue::Null);
        object.insert(name.clone(), value);
    }
    ZqValue::Object(object)
}

fn resolve_regex_spec_and_mode(
    spec: ZqValue,
    flags: Option<ZqValue>,
    tuple_mode: bool,
) -> Result<(String, Option<String>), String> {
    if let Some(flags) = flags {
        return Ok((
            expect_regex_string(spec)?,
            parse_regex_modifier_value(flags)?,
        ));
    }
    if !tuple_mode {
        return Ok((expect_regex_string(spec)?, None));
    }

    match spec {
        ZqValue::String(pattern) => Ok((pattern, None)),
        ZqValue::Array(values) if values.len() > 1 => Ok((
            expect_regex_string(values[0].clone())?,
            parse_regex_modifier_value(values[1].clone())?,
        )),
        ZqValue::Array(values) if !values.is_empty() => {
            Ok((expect_regex_string(values[0].clone())?, None))
        }
        other => Err(format!("{} not a string or array", type_name(&other))),
    }
}

fn expect_regex_string(value: ZqValue) -> Result<String, String> {
    match value {
        ZqValue::String(s) => Ok(s),
        other => Err(format!(
            "{} ({}) is not a string",
            type_name(&other),
            value_for_error(&other)
        )),
    }
}

fn parse_regex_modifier_value(value: ZqValue) -> Result<Option<String>, String> {
    match value {
        ZqValue::Null => Ok(None),
        ZqValue::String(s) => Ok(Some(s)),
        other => Err(format!(
            "{} ({}) is not a string",
            type_name(&other),
            value_for_error(&other)
        )),
    }
}

#[derive(Debug, Clone)]
pub(super) struct RegexModeConfig {
    pub(super) global: bool,
    pub(super) no_empty: bool,
    pub(super) case_insensitive: bool,
    pub(super) multi_line: bool,
    pub(super) dot_matches_new_line: bool,
    pub(super) ignore_whitespace: bool,
}

fn parse_regex_mode(mode: &Option<String>) -> Result<RegexModeConfig, String> {
    let mut out = RegexModeConfig {
        global: false,
        no_empty: false,
        case_insensitive: false,
        multi_line: false,
        dot_matches_new_line: false,
        ignore_whitespace: false,
    };
    if let Some(mode) = mode {
        for ch in mode.chars() {
            match ch {
                'g' => out.global = true,
                'n' => out.no_empty = true,
                'i' => out.case_insensitive = true,
                'm' => out.multi_line = true,
                's' => out.dot_matches_new_line = true,
                'x' => out.ignore_whitespace = true,
                // jq maps `p` to multiline + singleline in Oniguruma.
                'p' => {
                    out.multi_line = true;
                    out.dot_matches_new_line = true;
                }
                // jq accepts `l` (find-longest); fancy-regex has no direct equivalent.
                'l' => {}
                _ => return Err(format!("{mode} is not a valid modifier string")),
            }
        }
    }
    Ok(out)
}

fn with_cached_regex<T, F>(pattern: &str, config: &RegexModeConfig, f: F) -> Result<T, String>
where
    F: FnOnce(&CachedRegex) -> Result<T, String>,
{
    let normalized = normalize_named_capture_syntax(pattern);
    let flagged_pattern = apply_regex_inline_flags(normalized.as_ref(), config);
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(cached) = cache.get(flagged_pattern.as_ref()) {
            return f(cached);
        }

        if cache.len() >= REGEX_CACHE_LIMIT {
            if let Some(stale_key) = cache.keys().next().cloned() {
                cache.remove(&stale_key);
            }
        }

        let cache_key = flagged_pattern.into_owned();
        let compiled = CachedRegex::compile(&cache_key)?;
        let cached = cache.entry(cache_key).or_insert(compiled);
        f(cached)
    })
}

fn regex_collect_matches(
    input: &str,
    pattern: &str,
    config: &RegexModeConfig,
) -> Result<Vec<RegexMatchRecord>, String> {
    with_cached_regex(pattern, config, |cached| {
        regex_collect_matches_compiled(input, &cached.compiled, &cached.capture_names, config)
    })
}

pub(super) fn regex_has_match(
    input: &str,
    pattern: &str,
    config: &RegexModeConfig,
) -> Result<bool, String> {
    with_cached_regex(pattern, config, |cached| {
        regex_has_match_compiled(input, &cached.compiled, config)
    })
}

fn regex_has_match_compiled(
    input: &str,
    regex: &Regex,
    config: &RegexModeConfig,
) -> Result<bool, String> {
    if !config.global {
        if let Some(captures) = regex
            .captures(input)
            .map_err(|err| format!("Regex failure: {err}"))?
        {
            let Some(full) = captures.get(0) else {
                return Ok(false);
            };
            return Ok(!(config.no_empty && full.as_str().is_empty()));
        }
        return Ok(false);
    }

    let mut search_start = 0usize;
    while search_start <= input.len() {
        let captures = regex
            .captures_from_pos(input, search_start)
            .map_err(|err| format!("Regex failure: {err}"))?;
        let Some(captures) = captures else {
            break;
        };
        let Some(full) = captures.get(0) else {
            break;
        };
        let empty_match = full.start() == full.end();
        if !(config.no_empty && empty_match) {
            return Ok(true);
        }
        if empty_match {
            search_start = advance_regex_search_start(input, full.end());
        } else {
            search_start = full.end();
        }
    }

    Ok(false)
}

fn regex_collect_matches_compiled(
    input: &str,
    regex: &Regex,
    names: &[Option<String>],
    config: &RegexModeConfig,
) -> Result<Vec<RegexMatchRecord>, String> {
    let mut cp_table: Option<Vec<i64>> = None;
    let mut out = Vec::new();
    if !config.global {
        if let Some(captures) = regex
            .captures(input)
            .map_err(|err| format!("Regex failure: {err}"))?
        {
            let Some(full) = captures.get(0) else {
                return Ok(Vec::new());
            };
            if !(config.no_empty && full.as_str().is_empty()) {
                let cp_table = cp_table.get_or_insert_with(|| utf8_codepoint_index_table(input));
                out.push(regex_captures_to_record(&captures, names, cp_table));
            }
        }
        return Ok(out);
    }

    let mut search_start = 0usize;
    while search_start <= input.len() {
        let captures = regex
            .captures_from_pos(input, search_start)
            .map_err(|err| format!("Regex failure: {err}"))?;
        let Some(captures) = captures else {
            break;
        };
        let Some(full) = captures.get(0) else {
            break;
        };
        let empty_match = full.start() == full.end();
        if !(config.no_empty && empty_match) {
            let cp_table = cp_table.get_or_insert_with(|| utf8_codepoint_index_table(input));
            out.push(regex_captures_to_record(&captures, names, cp_table));
        }
        if empty_match {
            search_start = advance_regex_search_start(input, full.end());
        } else {
            search_start = full.end();
        }
    }

    Ok(out)
}

fn advance_regex_search_start(input: &str, pos: usize) -> usize {
    if pos >= input.len() {
        return input.len().saturating_add(1);
    }
    let Some(ch) = input[pos..].chars().next() else {
        return input.len().saturating_add(1);
    };
    pos.saturating_add(ch.len_utf8())
}

fn apply_regex_inline_flags<'a>(pattern: &'a str, config: &RegexModeConfig) -> Cow<'a, str> {
    let mut flags = String::new();
    if config.case_insensitive {
        flags.push('i');
    }
    if config.multi_line {
        flags.push('m');
    }
    if config.dot_matches_new_line {
        flags.push('s');
    }
    if config.ignore_whitespace {
        flags.push('x');
    }
    if flags.is_empty() {
        Cow::Borrowed(pattern)
    } else if config.ignore_whitespace {
        // In x-mode, trailing `# ...` comments must terminate before wrapper close.
        Cow::Owned(format!("(?{flags}:{pattern}\n)"))
    } else {
        Cow::Owned(format!("(?{flags}:{pattern})"))
    }
}

pub(super) fn normalize_named_capture_syntax<'a>(pattern: &'a str) -> Cow<'a, str> {
    if !pattern.contains("(?<") && !pattern.contains("(?'") {
        return Cow::Borrowed(pattern);
    }

    let mut out: Option<String> = None;
    let mut copied_until = 0usize;
    let mut i = 0usize;
    while i < pattern.len() {
        if pattern[i..].starts_with("(?<") {
            let next = pattern[i + 3..].chars().next();
            if next.is_some_and(is_regex_group_name_start) {
                if out.is_none() {
                    out = Some(String::with_capacity(pattern.len() + 8));
                }
                let dst = out.as_mut().expect("output must exist");
                dst.push_str(&pattern[copied_until..i]);
                dst.push_str("(?P<");
                copied_until = i + 3;
                i += 3;
                continue;
            }
        }
        if pattern[i..].starts_with("(?'") {
            let mut cursor = i + 3;
            let name_start = cursor;
            let mut has_name = false;
            let mut valid = true;
            while cursor < pattern.len() {
                let ch = pattern[cursor..]
                    .chars()
                    .next()
                    .expect("index is in-bounds");
                if ch == '\'' {
                    break;
                }
                if !has_name {
                    if !is_regex_group_name_start(ch) {
                        valid = false;
                        break;
                    }
                    has_name = true;
                } else if !is_regex_group_name_char(ch) {
                    valid = false;
                    break;
                }
                cursor += ch.len_utf8();
            }
            if valid && has_name && cursor < pattern.len() {
                if out.is_none() {
                    out = Some(String::with_capacity(pattern.len() + 8));
                }
                let dst = out.as_mut().expect("output must exist");
                dst.push_str(&pattern[copied_until..i]);
                dst.push_str("(?P<");
                dst.push_str(&pattern[name_start..cursor]);
                dst.push('>');
                copied_until = cursor + 1;
                i = cursor + 1;
                continue;
            }
        }
        let ch_len = pattern[i..]
            .chars()
            .next()
            .expect("index is in-bounds")
            .len_utf8();
        i += ch_len;
    }
    if let Some(mut out) = out {
        out.push_str(&pattern[copied_until..]);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(pattern)
    }
}

fn is_regex_group_name_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_regex_group_name_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn utf8_codepoint_index_table(input: &str) -> Vec<i64> {
    let mut table = vec![0i64; input.len() + 1];
    let mut cp = 0i64;
    let mut cursor = 0usize;
    for (idx, ch) in input.char_indices() {
        while cursor <= idx {
            table[cursor] = cp;
            cursor += 1;
        }
        cp += 1;
        let next = idx + ch.len_utf8();
        while cursor <= next && cursor < table.len() {
            table[cursor] = cp;
            cursor += 1;
        }
    }
    while cursor < table.len() {
        table[cursor] = cp;
        cursor += 1;
    }
    table
}

fn utf8_codepoint_to_byte_index_table(input: &str) -> Vec<usize> {
    let mut out = Vec::with_capacity(input.chars().count() + 1);
    out.push(0);
    for (start, ch) in input.char_indices() {
        out.push(start + ch.len_utf8());
    }
    out
}

fn codepoint_slice(
    input: &str,
    cp_to_byte: &[usize],
    start_cp: usize,
    end_cp: usize,
) -> Result<String, String> {
    let Some(&start) = cp_to_byte.get(start_cp) else {
        return Err("regex offset out of range".to_string());
    };
    let Some(&end) = cp_to_byte.get(end_cp) else {
        return Err("regex offset out of range".to_string());
    };
    if end < start || end > input.len() {
        return Err("regex offset out of range".to_string());
    }
    Ok(input[start..end].to_string())
}

fn regex_captures_to_record(
    captures: &FancyCaptures<'_>,
    names: &[Option<String>],
    cp_table: &[i64],
) -> RegexMatchRecord {
    let full = captures.get(0).expect("capture 0 always exists");
    let offset = cp_table[full.start()];
    let length = cp_table[full.end()] - cp_table[full.start()];
    let string = full.as_str().to_string();

    let mut groups = Vec::new();
    for i in 1..captures.len() {
        let name = names.get(i).cloned().unwrap_or(None);
        if let Some(group) = captures.get(i) {
            let group_offset = cp_table[group.start()];
            let group_length = cp_table[group.end()] - cp_table[group.start()];
            groups.push(RegexCaptureGroup {
                offset: group_offset,
                length: group_length,
                string: Some(group.as_str().to_string()),
                name,
            });
        } else {
            groups.push(RegexCaptureGroup {
                offset: -1,
                length: 0,
                string: None,
                name,
            });
        }
    }

    RegexMatchRecord {
        offset,
        length,
        string,
        captures: groups,
    }
}

fn regex_record_to_value(record: RegexMatchRecord) -> ZqValue {
    let mut object = IndexMap::new();
    object.insert("offset".to_string(), ZqValue::from(record.offset));
    object.insert("length".to_string(), ZqValue::from(record.length));
    object.insert("string".to_string(), ZqValue::String(record.string));
    object.insert(
        "captures".to_string(),
        ZqValue::Array(
            record
                .captures
                .into_iter()
                .map(|capture| {
                    let mut object = IndexMap::new();
                    object.insert("offset".to_string(), ZqValue::from(capture.offset));
                    object.insert(
                        "string".to_string(),
                        match capture.string {
                            Some(string) => ZqValue::String(string),
                            None => ZqValue::Null,
                        },
                    );
                    object.insert("length".to_string(), ZqValue::from(capture.length));
                    object.insert(
                        "name".to_string(),
                        match capture.name {
                            Some(name) => ZqValue::String(name),
                            None => ZqValue::Null,
                        },
                    );
                    ZqValue::Object(object)
                })
                .collect(),
        ),
    );
    ZqValue::Object(object)
}
