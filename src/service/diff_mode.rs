use super::*;
use std::io::IsTerminal;

pub(super) fn run_diff_mode(cli: &Cli, spool: &SpoolManager) -> Result<i32, Error> {
    if cli.from_file.is_some() {
        return Err(Error::Query(
            "--diff mode cannot be combined with -f/--from-file".to_string(),
        ));
    }
    if cli.input_legacy.is_some() {
        return Err(Error::Query(
            "--diff mode cannot be combined with --input (use positional LEFT RIGHT)".to_string(),
        ));
    }
    let (left_path, right_path) = resolve_diff_paths(cli)?;
    let left_format = resolve_effective_input_format(cli.input_format, &left_path);
    let right_format = resolve_effective_input_format(cli.input_format, &right_path);

    let left_input = read_input(&left_path, spool)?;
    let left_text = left_input.as_str_lossy();
    let left_docs = parse_diff_docs(
        left_text.as_ref(),
        &left_path,
        "LEFT",
        left_format,
        cli.csv_parse_json_cells,
    )?;

    let right_input = read_input(&right_path, spool)?;
    let right_text = right_input.as_str_lossy();
    let right_docs = parse_diff_docs(
        right_text.as_ref(),
        &right_path,
        "RIGHT",
        right_format,
        cli.csv_parse_json_cells,
    )?;

    let diffs = collect_semantic_doc_diffs(&left_docs, &right_docs);
    let summary = SemanticDiffSummary::from_diffs(&diffs);
    print_semantic_diff_report(
        &diffs,
        summary,
        cli.diff_format,
        cli.compact,
        diff_color_enabled(cli),
    )?;
    Ok(if summary.equal() { 0 } else { 1 })
}

fn diff_color_enabled(cli: &Cli) -> bool {
    if cli.monochrome_output {
        return false;
    }
    if cli.color_output {
        return true;
    }
    io::stdout().is_terminal()
}

pub(super) fn resolve_diff_paths(cli: &Cli) -> Result<(String, String), Error> {
    match (&cli.query, &cli.input_file) {
        (Some(left), Some(right)) => {
            if left == "-" && right == "-" {
                return Err(Error::Query(
                    "--diff mode does not support reading both sides from stdin".to_string(),
                ));
            }
            Ok((left.clone(), right.clone()))
        }
        (Some(right), None) => {
            if right == "-" {
                return Err(Error::Query(
                    "--diff mode requires at least one file path".to_string(),
                ));
            }
            Ok(("-".to_string(), right.clone()))
        }
        (None, Some(_)) => Err(Error::Query(
            "--diff mode expects positional paths as LEFT RIGHT".to_string(),
        )),
        (None, None) => Err(Error::Query(
            "--diff mode expects LEFT RIGHT (or a single RIGHT to compare stdin against RIGHT)"
                .to_string(),
        )),
    }
}

fn parse_diff_docs(
    input: &str,
    path: &str,
    side: &str,
    format: zq::NativeInputFormat,
    csv_parse_json_cells: bool,
) -> Result<Vec<zq::NativeValue>, Error> {
    zq::parse_native_input_values_with_format_native(input, format)
        .map(|mut parsed| {
            if csv_parse_json_cells && matches!(format, zq::NativeInputFormat::Csv) {
                decode_csv_json_cells_in_place(&mut parsed.values);
            }
            parsed.values
        })
        .map_err(|err| {
            Error::Query(format!(
                "--diff: cannot parse {side} input `{path}`: {}",
                zq::format_query_error("zq", input, &err)
            ))
        })
}

#[cfg(test)]
pub(super) fn build_custom_input_stream(
    cli: &Cli,
    input: &str,
    doc_mode: zq::DocMode,
) -> Result<Vec<JsonValue>, zq::EngineError> {
    let format = resolve_effective_input_format(cli.input_format, "-");
    build_custom_input_stream_native(cli, input, doc_mode, format).map(|values| {
        values
            .into_iter()
            .map(zq::NativeValue::into_json)
            .collect::<Vec<_>>()
    })
}

pub(super) fn build_custom_input_stream_native(
    cli: &Cli,
    input: &str,
    doc_mode: zq::DocMode,
    input_format: zq::NativeInputFormat,
) -> Result<Vec<zq::NativeValue>, zq::EngineError> {
    if cli.raw_input {
        if cli.slurp {
            return Ok(vec![zq::NativeValue::String(input.to_string())]);
        }
        return Ok(raw_input_lines(input)
            .into_iter()
            .map(zq::NativeValue::String)
            .collect());
    }

    let mut parsed =
        zq::parse_jq_input_values_with_format_native(input, doc_mode, "zq", input_format)?;
    if cli.csv_parse_json_cells && matches!(input_format, zq::NativeInputFormat::Csv) {
        decode_csv_json_cells_in_place(&mut parsed);
    }
    Ok(parsed)
}

fn decode_csv_json_cells_in_place(values: &mut [zq::NativeValue]) {
    for value in values {
        decode_csv_json_cells_value_in_place(value);
    }
}

fn decode_csv_json_cells_value_in_place(value: &mut zq::NativeValue) {
    match value {
        zq::NativeValue::String(cell) => {
            if let Ok(parsed) = serde_json::from_str::<zq::NativeValue>(cell) {
                *value = parsed;
            }
        }
        zq::NativeValue::Array(items) => {
            for item in items {
                decode_csv_json_cells_value_in_place(item);
            }
        }
        zq::NativeValue::Object(fields) => {
            for field in fields.values_mut() {
                decode_csv_json_cells_value_in_place(field);
            }
        }
        zq::NativeValue::Null | zq::NativeValue::Bool(_) | zq::NativeValue::Number(_) => {}
    }
}
