use clap::CommandFactory;
#[cfg(test)]
use clap::Parser;
use clap_complete::generate;
#[cfg(test)]
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::fs;
use std::io::{self, Write};

#[cfg(test)]
use crate::cli::DiffOutputFormat;
use crate::cli::{
    Cli, CliCommand, InputFormat, OutputFormat, YamlAnchorNameMode as CliYamlAnchorNameMode,
};

mod cli_compat;
mod diff_mode;
mod input_resolve;
mod json_color;
mod json_stream;
mod native_json_output;
mod output_colorize;
mod output_formats;
mod output_xml;
mod run_tests;
mod semantic_diff;
mod spool;
mod text_utils;

#[cfg(test)]
use self::cli_compat::extract_cli_compat_args;
use self::cli_compat::{build_query_with_cli_compat, parse_cli_with_compat_args, CliCompatArgs};
#[cfg(test)]
use self::diff_mode::build_custom_input_stream;
#[cfg(test)]
use self::diff_mode::resolve_diff_paths;
use self::diff_mode::{build_custom_input_stream_native, run_diff_mode};
#[cfg(test)]
use self::input_resolve::requires_filter_for_interactive_stdin;
use self::input_resolve::{
    resolve_base_query, resolve_effective_input_format, resolve_input_path,
    resolve_positional_input,
};
#[cfg(test)]
use self::json_color::{
    render_json_line, render_json_value_colored, render_raw_output0, validate_jq_colors,
    write_json_output,
};
use self::json_color::{resolve_json_color_options, JsonColorOptions};
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
#[cfg(test)]
use self::native_json_output::render_native_value_colored;
use self::native_json_output::{
    exit_status_from_outputs_native, render_raw_output0_native, write_json_native_value_line,
    write_json_output_lines_native, write_json_output_native,
};
use self::output_colorize::colorize_structured_output;
use self::output_formats::{render_csv_output_native, render_toml_output_native};
use self::output_xml::render_xml_output_native;
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
#[cfg(test)]
use self::text_utils::{raw_input_lines, strip_serde_line_col_suffix};

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

#[cfg(test)]
mod tests;
