use clap::CommandFactory;
#[cfg(test)]
use clap::Parser;
use clap_complete::generate;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::fs;
use std::io::{self, IsTerminal, Write};

#[cfg(test)]
use crate::cli::DiffOutputFormat;
use crate::cli::{
    Cli, CliCommand, InputFormat, OutputFormat, YamlAnchorNameMode as CliYamlAnchorNameMode,
};

mod cli_compat;
mod input_resolve;
mod json_color;
mod json_stream;
mod output_formats;
mod run_tests;
mod semantic_diff;
mod spool;

#[cfg(test)]
use self::cli_compat::extract_cli_compat_args;
use self::cli_compat::{build_query_with_cli_compat, parse_cli_with_compat_args, CliCompatArgs};
#[cfg(test)]
use self::input_resolve::requires_filter_for_interactive_stdin;
use self::input_resolve::{
    resolve_base_query, resolve_effective_input_format, resolve_input_path,
    resolve_positional_input,
};
#[cfg(test)]
use self::json_color::{
    render_json_line, render_raw_output0, validate_jq_colors, write_json_output,
};
use self::json_color::{
    render_json_value_colored, resolve_json_color_options, write_jq_style_escaped_del,
    JsonColorOptions, JsonColorPalette,
};
#[cfg(test)]
use self::json_stream::{
    advance_json_scan, complete_json_value, parse_json_seq_input, scan_json_literal,
    scan_json_number, scan_json_string, scan_json_value, stream_error_value_from_json_error,
    stream_json_values, JsonArrayState, JsonObjectState, JsonScanFrame,
};
#[cfg(test)]
use self::json_stream::{json_parse_error_message, line_col_to_byte_index};
use self::json_stream::{
    parse_json_seq_input_native, stream_error_value_from_json_error_native, stream_native_values,
};
use self::output_formats::{
    colorize_structured_output, render_csv_output_native, render_toml_output_native,
    render_xml_output_native,
};
use self::run_tests::run_tests_mode_many;
#[cfg(test)]
use self::run_tests::{
    canonicalize_run_tests_number_lexeme, format_duration, is_blank, is_fail_marker,
    is_fail_with_message, is_skipline, normalize_run_tests_error_line,
    render_validation_error_without_engine_prefix, resolve_run_tests_library_paths, run_query_case,
    run_tests_mode, run_tests_values_equal, shorten_for_report, strip_bom_prefix, CasePayload,
    QueryCasePayload, RunTestMode, RunTestsStats, TestCaseProgram, TestCursor,
};
use self::semantic_diff::{
    collect_semantic_doc_diffs, print_semantic_diff_report, SemanticDiffSummary,
};
#[cfg(test)]
use self::semantic_diff::{write_semantic_diff_report, SemanticDiff, SemanticDiffKind};
use self::spool::SpoolManager;
#[cfg(test)]
use self::spool::{remove_spool_run_dir_if_safe, resolve_spool_root_dir};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("query: {0}")]
    Query(String),
}

pub fn run() -> Result<i32, Error> {
    let Some((cli, compat_args)) = parse_cli_with_compat_args()? else {
        return Ok(0);
    };
    if let Some(command) = &cli.command {
        return run_cli_command(command);
    }
    run_with(cli, compat_args)
}

fn run_cli_command(command: &CliCommand) -> Result<i32, Error> {
    match command {
        CliCommand::Completion { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            generate(*shell, &mut cmd, name, &mut io::stdout());
            Ok(0)
        }
    }
}

fn query_uses_any_builtin(query: &str, builtins: &[&str]) -> bool {
    let mut in_string = false;
    let mut escaped = false;
    let mut token = String::new();

    let flush = |token: &mut String| {
        let out = builtins.contains(&token.as_str());
        token.clear();
        out
    };

    for ch in query.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            if flush(&mut token) {
                return true;
            }
            in_string = true;
            continue;
        }

        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch);
        } else if flush(&mut token) {
            return true;
        }
    }

    flush(&mut token)
}

fn query_uses_inputs_builtin(query: &str) -> bool {
    query_uses_any_builtin(query, &["input", "inputs"])
}

fn query_uses_stderr_builtin(query: &str) -> bool {
    query_uses_any_builtin(query, &["stderr"])
}

#[cfg(test)]
#[derive(Debug, Default)]
struct SeqParseResult {
    values: Vec<JsonValue>,
    errors: Vec<String>,
}

#[derive(Debug, Default)]
struct SeqParseResultNative {
    values: Vec<zq::NativeValue>,
    errors: Vec<String>,
}

enum InputData {
    Owned(String),
    Mapped(memmap2::Mmap),
}

impl InputData {
    fn as_str_lossy(&self) -> Cow<'_, str> {
        match self {
            Self::Owned(s) => Cow::Borrowed(s.as_str()),
            // jq treats raw byte input as parseable text streams; lossy decode avoids
            // hard I/O failures on arbitrary bytes in --seq fuzz tests.
            Self::Mapped(m) => String::from_utf8_lossy(m),
        }
    }
}

fn run_with(cli: Cli, compat_args: CliCompatArgs) -> Result<i32, Error> {
    let spool = SpoolManager::new()?;

    if cli.diff && !cli.run_tests.is_empty() {
        return Err(Error::Query(
            "--diff mode cannot be combined with --run-tests".to_string(),
        ));
    }
    if !cli.run_tests.is_empty() {
        let mut paths: Vec<String> = cli
            .run_tests
            .iter()
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect();
        if paths.is_empty() {
            paths.push("-".to_string());
        }
        return run_tests_mode_many(&cli, &paths, &spool);
    }
    if cli.diff {
        return run_diff_mode(&cli, &spool);
    }

    let positional_input = resolve_positional_input(&cli)?;
    let base_query = resolve_base_query(&cli)?;
    let raw_output = cli.raw_output || cli.join_output;
    let force_stderr_text = query_uses_stderr_builtin(base_query.as_str());
    let effective_raw_output = raw_output || force_stderr_text;
    let query = build_query_with_cli_compat(base_query.as_str(), &compat_args)?;
    let input_path = resolve_input_path(&cli, positional_input.as_deref())?;
    let effective_input_format = resolve_effective_input_format(cli.input_format, &input_path);
    let doc_mode = zq::parse_doc_mode(&cli.doc_mode, cli.doc_index)
        .map_err(|e| Error::Query(e.to_string()))?;

    if !matches!(cli.output_format, OutputFormat::Json) && raw_output {
        return Err(Error::Query(
            "--raw-output is supported only with --output-format=json".to_string(),
        ));
    }
    if !matches!(cli.output_format, OutputFormat::Json) && cli.raw_output0 {
        return Err(Error::Query(
            "--raw-output0 is supported only with --output-format=json".to_string(),
        ));
    }
    if !matches!(cli.output_format, OutputFormat::Json) && cli.compact {
        return Err(Error::Query(
            "--compact is supported only with --output-format=json".to_string(),
        ));
    }
    if cli.yaml_anchors && !matches!(cli.output_format, OutputFormat::Yaml) {
        return Err(Error::Query(
            "--yaml-anchors is supported only with --output-format=yaml".to_string(),
        ));
    }
    if !matches!(cli.output_format, OutputFormat::Yaml)
        && !matches!(cli.yaml_anchor_name_mode, CliYamlAnchorNameMode::Friendly)
    {
        return Err(Error::Query(
            "--yaml-anchor-name-mode is supported only with --output-format=yaml".to_string(),
        ));
    }
    if !cli.yaml_anchors && !matches!(cli.yaml_anchor_name_mode, CliYamlAnchorNameMode::Friendly) {
        return Err(Error::Query(
            "--yaml-anchor-name-mode requires --yaml-anchors".to_string(),
        ));
    }
    if cli.raw_output0 && cli.join_output {
        return Err(Error::Query(
            "--raw-output0 is incompatible with --join-output".to_string(),
        ));
    }

    let color_opts = resolve_json_color_options(&cli);
    let text_color_enabled = color_opts.enabled;
    if color_opts.warn_invalid {
        eprintln!("Failed to set $JQ_COLORS");
    }

    if cli.debug_dump_disasm {
        let labels = zq::debug_dump_disasm_function_labels(query.as_str(), &cli.library_path)
            .map_err(|e| Error::Query(render_engine_error("zq", query.as_str(), "", e)))?;
        if !labels.is_empty() {
            let stdout = io::stdout();
            let mut writer = io::BufWriter::new(stdout.lock());
            for label in labels {
                writeln!(writer, "{label}")?;
            }
            writeln!(writer)?;
            writer.flush()?;
        }
    }

    let skip_input_read = cli.null_input
        && !cli.raw_input
        && !cli.slurp
        && !cli.seq
        && !cli.stream
        && !cli.stream_errors
        && !query_uses_inputs_builtin(base_query.as_str());
    let input_data = if skip_input_read {
        InputData::Owned(String::new())
    } else {
        read_input(&input_path, &spool)?
    };
    let input_text = input_data.as_str_lossy();
    let input = input_text.as_ref();

    let can_native_stream_direct = matches!(cli.output_format, OutputFormat::Json)
        && matches!(effective_input_format, zq::NativeInputFormat::Auto)
        && !cli.raw_output0
        && !cli.exit_status
        && !cli.raw_input
        && !cli.slurp
        && !cli.null_input
        && !cli.seq
        && !cli.stream
        && !cli.stream_errors;

    if can_native_stream_direct {
        let stdout = io::stdout();
        let mut writer = io::BufWriter::new(stdout.lock());
        let mut wrote_any = false;
        let mut json_scratch = Vec::new();
        let native_pretty_indent = if cli.compact || color_opts.enabled {
            None
        } else {
            Some(vec![b' '; color_opts.indent])
        };

        let native_status = zq::try_run_jq_native_stream_json_text_options_native(
            query.as_str(),
            input,
            zq::EngineRunOptions { null_input: false },
            |value| {
                if wrote_any && !cli.join_output {
                    writer.write_all(b"\n").map_err(|e| e.to_string())?;
                }
                write_json_native_value_line(
                    &mut writer,
                    &value,
                    cli.compact,
                    effective_raw_output,
                    &mut json_scratch,
                    native_pretty_indent.as_deref(),
                    &color_opts,
                )
                .map_err(|e| e.to_string())?;
                wrote_any = true;
                Ok(())
            },
        )
        .map_err(|e| Error::Query(render_engine_error("zq", query.as_str(), input, e)))?;

        if matches!(native_status, zq::NativeStreamStatus::Executed) {
            if wrote_any {
                if !cli.join_output {
                    writer.write_all(b"\n")?;
                }
                writer.flush()?;
            }
            return Ok(0);
        }

        let err = zq::validate_jq_query_with_paths(query.as_str(), &cli.library_path)
            .err()
            .unwrap_or_else(|| {
                zq::EngineError::Query(zq::QueryError::Unsupported(format!(
                    "query is not supported by native engine: {}",
                    query.as_str()
                )))
            });
        return Err(Error::Query(render_engine_error(
            "jq",
            query.as_str(),
            input,
            err,
        )));
    }

    let out_native = if cli.raw_input
        || cli.slurp
        || cli.null_input
        || cli.seq
        || cli.stream
        || cli.stream_errors
        || !matches!(effective_input_format, zq::NativeInputFormat::Auto)
    {
        if (cli.stream || cli.stream_errors) && cli.raw_input {
            return Err(Error::Query(
                "--stream and --stream-errors are incompatible with --raw-input".to_string(),
            ));
        }

        let mut seq_errors = Vec::new();
        let mut inputs = if cli.seq && !cli.raw_input {
            let parsed = parse_json_seq_input_native(input);
            seq_errors = parsed.errors;
            parsed.values
        } else if cli.stream || cli.stream_errors {
            match zq::parse_jq_json_values_only_native(input) {
                Ok(values) => stream_native_values(values),
                Err(zq::EngineError::Query(zq::QueryError::Json(json_err)))
                    if cli.stream_errors =>
                {
                    vec![stream_error_value_from_json_error_native(input, &json_err)]
                }
                Err(err) => {
                    return Err(Error::Query(render_engine_error(
                        "zq",
                        query.as_str(),
                        input,
                        err,
                    )))
                }
            }
        } else {
            build_custom_input_stream_native(&cli, input, doc_mode, effective_input_format)
                .map_err(|e| Error::Query(render_engine_error("zq", query.as_str(), input, e)))?
        };

        if cli.seq && cli.null_input && query_uses_inputs_builtin(&query) && !seq_errors.is_empty()
        {
            return Err(Error::Query(format!(
                "zq: error (at <stdin>:1): {}",
                seq_errors[0]
            )));
        }
        for err in &seq_errors {
            eprintln!("zq: ignoring parse error: {err}");
        }

        if cli.slurp && !cli.raw_input {
            inputs = vec![zq::NativeValue::Array(inputs)];
        }
        let native_out = zq::run_jq_stream_with_paths_options_native(
            query.as_str(),
            inputs,
            &cli.library_path,
            zq::EngineRunOptions {
                null_input: cli.null_input,
            },
        )
        .map_err(|e| Error::Query(render_engine_error("zq", query.as_str(), input, e)))?;
        native_out
    } else {
        let options = zq::QueryOptions {
            doc_mode,
            library_path: cli.library_path.clone(),
        };
        let native_out = zq::run_jq_native(query.as_str(), input, options)
            .map_err(|e| Error::Query(render_engine_error("zq", query.as_str(), input, e)))?;
        native_out
    };
    if cli.raw_output0 {
        let (rendered, maybe_error) = render_raw_output0_native(&out_native, cli.compact)?;
        if !rendered.is_empty() {
            let mut stdout = io::stdout();
            stdout.write_all(&rendered)?;
            stdout.flush()?;
        }
        if let Some(err) = maybe_error {
            return Err(err);
        }
    } else {
        match cli.output_format {
            OutputFormat::Json => write_json_output_lines_native(
                &out_native,
                cli.compact,
                effective_raw_output,
                cli.join_output,
                &color_opts,
            )?,
            OutputFormat::Yaml => {
                let rendered = zq::format_output_yaml_documents_native_with_options(
                    &out_native,
                    zq::YamlFormatOptions {
                        use_anchors: cli.yaml_anchors,
                        anchor_name_mode: cli_yaml_anchor_name_mode_to_native(
                            cli.yaml_anchor_name_mode,
                        ),
                        ..zq::YamlFormatOptions::default()
                    },
                )
                .map_err(|e| Error::Query(e.to_string()))?;
                if !rendered.is_empty() {
                    let colored = colorize_structured_output(
                        OutputFormat::Yaml,
                        &rendered,
                        text_color_enabled,
                        color_opts.jq_colors.as_deref(),
                    );
                    println!("{colored}");
                }
            }
            OutputFormat::Toml => {
                let rendered = render_toml_output_native(&out_native)?;
                if !rendered.is_empty() {
                    let colored = colorize_structured_output(
                        OutputFormat::Toml,
                        &rendered,
                        text_color_enabled,
                        color_opts.jq_colors.as_deref(),
                    );
                    print!("{colored}");
                }
            }
            OutputFormat::Csv => {
                let rendered = render_csv_output_native(&out_native)?;
                if !rendered.is_empty() {
                    let colored = colorize_structured_output(
                        OutputFormat::Csv,
                        &rendered,
                        text_color_enabled,
                        color_opts.jq_colors.as_deref(),
                    );
                    print!("{colored}");
                }
            }
            OutputFormat::Xml => {
                let rendered = render_xml_output_native(&out_native)?;
                if !rendered.is_empty() {
                    let colored = colorize_structured_output(
                        OutputFormat::Xml,
                        &rendered,
                        text_color_enabled,
                        color_opts.jq_colors.as_deref(),
                    );
                    print!("{colored}");
                }
            }
        }
    }

    if matches!(cli.output_format, OutputFormat::Json) && !cli.raw_output0 && force_stderr_text {
        let stderr = io::stderr();
        let mut writer = io::BufWriter::new(stderr.lock());
        write_json_output_native(
            &mut writer,
            &out_native,
            cli.compact,
            effective_raw_output,
            cli.join_output,
            &JsonColorOptions::default(),
        )?;
        writer.flush()?;
    }

    if cli.exit_status {
        return Ok(exit_status_from_outputs_native(&out_native));
    }
    Ok(0)
}

fn run_diff_mode(cli: &Cli, spool: &SpoolManager) -> Result<i32, Error> {
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

fn resolve_diff_paths(cli: &Cli) -> Result<(String, String), Error> {
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
fn build_custom_input_stream(
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

fn build_custom_input_stream_native(
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

#[cfg(test)]
fn render_json_output(
    values: &[JsonValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
) -> Result<String, Error> {
    let mut out = Vec::new();
    write_json_output(
        &mut out,
        values,
        compact,
        raw_output,
        join_output,
        &JsonColorOptions::default(),
    )?;
    String::from_utf8(out).map_err(|e| Error::Query(format!("encode json: {e}")))
}

fn strip_serde_line_col_suffix(msg: &str) -> &str {
    let marker = " at line ";
    let Some(idx) = msg.rfind(marker) else {
        return msg;
    };
    let suffix = &msg[idx + marker.len()..];
    let Some((line, col_part)) = suffix.split_once(" column ") else {
        return msg;
    };
    if line.trim().parse::<usize>().is_ok() && col_part.trim().parse::<usize>().is_ok() {
        &msg[..idx]
    } else {
        msg
    }
}

fn unfinished_abandoned_at_eof_message(input: &str) -> String {
    let mut err_pos: Option<(usize, usize)> = None;
    for next in serde_json::Deserializer::from_str(input).into_iter::<zq::NativeValue>() {
        if let Err(e) = next {
            err_pos = Some((e.line(), e.column()));
            break;
        }
    }

    let (line, col) = if let Some((line, col)) = err_pos {
        (line, col)
    } else {
        index_to_line_col(input, input.len(), true)
    };
    format!("Unfinished abandoned text at EOF at line {line}, column {col}")
}

fn index_to_line_col(s: &str, idx: usize, eof: bool) -> (usize, usize) {
    let mut line = 1usize;
    let mut col0 = 0usize;
    for (byte_idx, ch) in s.char_indices() {
        if byte_idx >= idx {
            break;
        }
        if ch == '\n' {
            line += 1;
            col0 = 0;
        } else {
            col0 += 1;
        }
    }
    let col = if eof { col0 } else { col0 + 1 };
    (line, col)
}

fn raw_input_lines(input: &str) -> Vec<String> {
    input
        .split_terminator('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect()
}

#[cfg(test)]
fn exit_status_from_outputs(outputs: &[JsonValue]) -> i32 {
    let Some(last) = outputs.last() else {
        return 4;
    };
    match last {
        JsonValue::Null => 1,
        JsonValue::Bool(false) => 1,
        _ => 0,
    }
}

fn read_input(path: &str, spool: &SpoolManager) -> Result<InputData, Error> {
    if path == "-" {
        return spool.read_stdin_into_mmap();
    }
    let file = fs::File::open(path)?;
    // Memory-map regular files to avoid a full read+copy before parsing.
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    Ok(InputData::Mapped(mmap))
}

fn cli_yaml_anchor_name_mode_to_native(mode: CliYamlAnchorNameMode) -> zq::YamlAnchorNameMode {
    match mode {
        CliYamlAnchorNameMode::Friendly => zq::YamlAnchorNameMode::Friendly,
        CliYamlAnchorNameMode::StrictFriendly => zq::YamlAnchorNameMode::StrictFriendly,
    }
}

fn render_engine_error(tool: &str, query: &str, input: &str, err: zq::EngineError) -> String {
    match err {
        zq::EngineError::Query(inner) => {
            zq::format_query_error_with_sources(tool, query, input, &inner)
        }
        other => other.to_string(),
    }
}

fn write_json_native_value_line<W: Write>(
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

fn render_raw_output0_native(
    values: &[zq::NativeValue],
    compact: bool,
) -> Result<(Vec<u8>, Option<Error>), Error> {
    let mut out = Vec::new();
    let mut scratch = Vec::new();
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
fn render_native_value_colored(
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
    let unsigned = raw
        .strip_prefix('-')
        .or_else(|| raw.strip_prefix('+'))
        .unwrap_or(&raw);
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
        zq::NativeValue::Array(items) => JsonValue::Array(
            items
                .iter()
                .map(native_value_to_cli_json_compat)
                .collect::<Vec<_>>(),
        ),
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
    let unsigned = raw
        .strip_prefix('-')
        .or_else(|| raw.strip_prefix('+'))
        .unwrap_or(&raw);
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
        return JsonValue::Number(serde_json::Number::from_string_unchecked(
            finite.to_string(),
        ));
    }

    JsonValue::Number(number.clone())
}

fn write_json_output_native<W: Write>(
    writer: &mut W,
    values: &[zq::NativeValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    let mut scratch = Vec::new();
    let pretty_indent = if compact || color_opts.enabled {
        None
    } else {
        Some(vec![b' '; color_opts.indent])
    };
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

fn write_json_output_lines_native(
    values: &[zq::NativeValue],
    compact: bool,
    raw_output: bool,
    join_output: bool,
    color_opts: &JsonColorOptions,
) -> Result<(), Error> {
    let stdout = io::stdout();
    let mut writer = io::BufWriter::new(stdout.lock());
    write_json_output_native(
        &mut writer,
        values,
        compact,
        raw_output,
        join_output,
        color_opts,
    )?;
    writer.flush()?;
    Ok(())
}

fn exit_status_from_outputs_native(outputs: &[zq::NativeValue]) -> i32 {
    match outputs.last() {
        None => 4,
        Some(zq::NativeValue::Null) => 1,
        Some(zq::NativeValue::Bool(false)) => 1,
        Some(_) => 0,
    }
}

#[cfg(test)]
mod tests;
