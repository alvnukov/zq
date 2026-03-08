use clap::{error::ErrorKind, CommandFactory, Parser};
use clap_complete::generate;
use fs2::FileExt;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use crate::cli::DiffOutputFormat;
use crate::cli::{
    Cli, CliCommand, InputFormat, OutputFormat, YamlAnchorNameMode as CliYamlAnchorNameMode,
};

mod json_stream;
mod output_formats;
mod run_tests;
mod semantic_diff;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionalArgsMode {
    String,
    Json,
}

#[derive(Debug, Default, Clone)]
struct CliCompatArgs {
    named_vars: indexmap::IndexMap<String, zq::NativeValue>,
    named_args: indexmap::IndexMap<String, zq::NativeValue>,
    positional_args: Vec<zq::NativeValue>,
}

impl CliCompatArgs {
    fn is_empty(&self) -> bool {
        self.named_vars.is_empty() && self.named_args.is_empty() && self.positional_args.is_empty()
    }
}

fn parse_cli_with_compat_args() -> Result<Option<(Cli, CliCompatArgs)>, Error> {
    let raw_args = std::env::args().collect::<Vec<_>>();
    let (filtered_args, compat_args) = extract_cli_compat_args(raw_args)?;
    let cli = match Cli::try_parse_from(filtered_args) {
        Ok(cli) => cli,
        Err(e) => match e.kind() {
            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                print!("{e}");
                return Ok(None);
            }
            _ => return Err(invalid_cli_arg(e.to_string())),
        },
    };
    Ok(Some((cli, compat_args)))
}

fn extract_cli_compat_args(args: Vec<String>) -> Result<(Vec<String>, CliCompatArgs), Error> {
    if args.is_empty() {
        return Ok((Vec::new(), CliCompatArgs::default()));
    }

    let mut filtered = Vec::with_capacity(args.len());
    filtered.push(args[0].clone());

    let mut compat = CliCompatArgs::default();
    let mut i = 1usize;
    let mut mode: Option<PositionalArgsMode> = None;
    let mut seen_query = false;
    let mut expect_query_after_double_dash = false;

    while i < args.len() {
        let arg = args[i].as_str();

        if let Some(current_mode) = mode {
            if arg == "--args" {
                mode = Some(PositionalArgsMode::String);
                i += 1;
                continue;
            }
            if arg == "--jsonargs" {
                mode = Some(PositionalArgsMode::Json);
                i += 1;
                continue;
            }
            if arg == "--" {
                if !seen_query {
                    filtered.push(args[i].clone());
                    i += 1;
                    expect_query_after_double_dash = true;
                    continue;
                }
                i += 1;
                while i < args.len() {
                    let v = parse_positional_value(current_mode, &args[i])?;
                    compat.positional_args.push(v);
                    i += 1;
                }
                break;
            }

            if !seen_query {
                filtered.push(args[i].clone());
                if expect_query_after_double_dash || !arg.starts_with('-') {
                    seen_query = true;
                    expect_query_after_double_dash = false;
                }
                i += 1;
                continue;
            }

            let v = parse_positional_value(current_mode, &args[i])?;
            compat.positional_args.push(v);
            i += 1;
            continue;
        }

        match arg {
            "--arg" => {
                let (name_raw, value_raw) =
                    parse_named_arg_pair(&args, i, "--arg requires two arguments: NAME VALUE")?;
                let name = name_raw.to_string();
                let value = zq::NativeValue::String(value_raw.to_string());
                compat.named_vars.insert(name.clone(), value.clone());
                compat.named_args.insert(name, value);
                i += 3;
            }
            "--argjson" => {
                let (name_raw, value_raw) =
                    parse_named_arg_pair(&args, i, "--argjson requires two arguments: NAME JSON")?;
                let name = name_raw.to_string();
                let value = parse_named_json("--argjson", value_raw)?;
                compat.named_vars.insert(name.clone(), value.clone());
                compat.named_args.insert(name, value);
                i += 3;
            }
            "--slurpfile" => {
                let (name_raw, path_raw) = parse_named_arg_pair(
                    &args,
                    i,
                    "--slurpfile requires two arguments: NAME FILE",
                )?;
                let name = name_raw.to_string();
                let values = read_slurpfile_values(path_raw)?;
                compat
                    .named_vars
                    .insert(name, zq::NativeValue::Array(values));
                i += 3;
            }
            "--rawfile" => {
                let (name_raw, path_raw) =
                    parse_named_arg_pair(&args, i, "--rawfile requires two arguments: NAME FILE")?;
                let name = name_raw.to_string();
                let value = fs::read_to_string(path_raw)?;
                compat
                    .named_vars
                    .insert(name, zq::NativeValue::String(value));
                i += 3;
            }
            "--args" => {
                mode = Some(PositionalArgsMode::String);
                i += 1;
            }
            "--jsonargs" => {
                mode = Some(PositionalArgsMode::Json);
                i += 1;
            }
            _ => {
                filtered.push(args[i].clone());
                if !arg.starts_with('-') && !seen_query {
                    seen_query = true;
                }
                i += 1;
            }
        }
    }

    Ok((filtered, compat))
}

fn parse_named_json(flag: &str, raw: &str) -> Result<zq::NativeValue, Error> {
    serde_json::from_str::<zq::NativeValue>(raw)
        .map_err(|e| invalid_cli_arg(format!("{flag}: invalid JSON text `{raw}`: {e}")))
}

fn parse_positional_value(mode: PositionalArgsMode, raw: &str) -> Result<zq::NativeValue, Error> {
    match mode {
        PositionalArgsMode::String => Ok(zq::NativeValue::String(raw.to_string())),
        PositionalArgsMode::Json => serde_json::from_str::<zq::NativeValue>(raw)
            .map_err(|e| invalid_cli_arg(format!("--jsonargs: invalid JSON text `{raw}`: {e}"))),
    }
}

fn invalid_cli_arg(msg: impl Into<String>) -> Error {
    Error::Io(io::Error::new(io::ErrorKind::InvalidInput, msg.into()))
}

fn parse_named_arg_pair<'a>(
    args: &'a [String],
    i: usize,
    error_message: &'static str,
) -> Result<(&'a str, &'a str), Error> {
    if i + 2 >= args.len() {
        return Err(invalid_cli_arg(error_message));
    }
    Ok((args[i + 1].as_str(), args[i + 2].as_str()))
}

fn read_slurpfile_values(path: &str) -> Result<Vec<zq::NativeValue>, Error> {
    let input = fs::read_to_string(path)?;
    let mut values = Vec::new();
    for next in serde_json::Deserializer::from_str(&input).into_iter::<zq::NativeValue>() {
        values.push(next.map_err(|e| {
            invalid_cli_arg(format!("--slurpfile {path}: invalid JSON input: {e}"))
        })?);
    }
    Ok(values)
}

fn is_valid_jq_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn build_query_with_cli_compat(query: &str, compat: &CliCompatArgs) -> Result<String, Error> {
    let needs_args_var = query.contains("$ARGS");
    if compat.is_empty() && !needs_args_var {
        return Ok(query.to_string());
    }

    for name in compat.named_vars.keys() {
        if !is_valid_jq_identifier(name) {
            return Err(invalid_cli_arg(format!(
                "invalid variable name for --arg/--argjson/--slurpfile/--rawfile: {name}"
            )));
        }
    }

    let named_vars_json = serde_json::to_string(&compat.named_vars)
        .map_err(|e| Error::Query(format!("encode named variables: {e}")))?;
    let args_json = serde_json::to_string(&serde_json::json!({
        "named": &compat.named_args,
        "positional": &compat.positional_args,
    }))
    .map_err(|e| Error::Query(format!("encode $ARGS: {e}")))?;

    let mut wrapped = format!("({named_vars_json}) as $__zq_named | ({args_json}) as $ARGS | ");
    for name in compat.named_vars.keys() {
        let key = serde_json::to_string(name)
            .map_err(|e| Error::Query(format!("encode argument name: {e}")))?;
        wrapped.push_str(&format!("($__zq_named[{key}]) as ${name} | "));
    }
    wrapped.push_str(query);
    Ok(wrapped)
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

struct SpoolManager {
    root_dir: PathBuf,
    run_dir: PathBuf,
    run_lock: Option<fs::File>,
    next_file_id: AtomicU64,
}

impl SpoolManager {
    fn new() -> Result<Self, Error> {
        let root_dir = resolve_spool_root_dir();
        fs::create_dir_all(&root_dir)?;
        Self::sweep_stale_runs(&root_dir)?;
        let (run_dir, run_lock) = Self::create_run_dir_with_lock(&root_dir)?;
        Ok(Self {
            root_dir,
            run_dir,
            run_lock: Some(run_lock),
            next_file_id: AtomicU64::new(0),
        })
    }

    fn read_stdin_into_mmap(&self) -> Result<InputData, Error> {
        let next_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        let stdin_file_path = self.run_dir.join(format!("stdin-{next_id}.bin"));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&stdin_file_path)?;
        {
            let mut stdin = io::stdin().lock();
            io::copy(&mut stdin, &mut file)?;
        }
        let len = file.metadata()?.len();
        if len == 0 {
            drop(file);
            let _ = fs::remove_file(stdin_file_path);
            return Ok(InputData::Owned(String::new()));
        }
        file.flush()?;
        // SAFETY: the file remains open for the lifetime of this function call; the returned
        // mapping owns the OS mapping handle and is read-only.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(InputData::Mapped(mmap))
    }

    fn sweep_stale_runs(root_dir: &Path) -> Result<(), Error> {
        let cleanup_lock_path = root_dir.join("cleanup.lock");
        let cleanup_lock = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(cleanup_lock_path)?;
        if cleanup_lock.try_lock_exclusive().is_err() {
            return Ok(());
        }

        for entry in fs::read_dir(root_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with("run-") {
                continue;
            }

            let run_dir = entry.path();
            let run_lock_path = run_dir.join("run.lock");
            let run_lock = match fs::OpenOptions::new()
                .create(false)
                .read(true)
                .write(true)
                .open(&run_lock_path)
            {
                Ok(file) => file,
                Err(_) => continue,
            };

            if run_lock.try_lock_exclusive().is_ok() {
                let _ = run_lock.unlock();
                let _ = remove_spool_run_dir_if_safe(root_dir, &run_dir);
            }
        }

        let _ = cleanup_lock.unlock();
        Ok(())
    }

    fn create_run_dir_with_lock(root_dir: &Path) -> Result<(PathBuf, fs::File), Error> {
        let pid = std::process::id();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..64u32 {
            let run_dir = root_dir.join(format!("run-{pid}-{now}-{attempt}"));
            match fs::create_dir(&run_dir) {
                Ok(()) => {
                    let run_lock_path = run_dir.join("run.lock");
                    let run_lock = fs::OpenOptions::new()
                        .create_new(true)
                        .read(true)
                        .write(true)
                        .open(run_lock_path)?;
                    run_lock.try_lock_exclusive()?;
                    return Ok((run_dir, run_lock));
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Err(Error::Query(
            "failed to allocate run spool directory".to_string(),
        ))
    }
}

impl Drop for SpoolManager {
    fn drop(&mut self) {
        if let Some(run_lock) = self.run_lock.take() {
            let _ = run_lock.unlock();
            drop(run_lock);
        }
        let _ = remove_spool_run_dir_if_safe(&self.root_dir, &self.run_dir);
    }
}

fn resolve_spool_root_dir() -> PathBuf {
    let base = std::env::var("ZQ_SPOOL_DIR")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("zq-spool"));
    base.join("v1")
}

fn remove_spool_run_dir_if_safe(root_dir: &Path, run_dir: &Path) -> io::Result<()> {
    if !run_dir.exists() {
        return Ok(());
    }
    let canonical_root = root_dir.canonicalize()?;
    let canonical_run = run_dir.canonicalize()?;
    if canonical_run.starts_with(&canonical_root) {
        fs::remove_dir_all(canonical_run)?;
    }
    Ok(())
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

#[derive(Debug, Clone)]
struct JsonColorOptions {
    enabled: bool,
    jq_colors: Option<String>,
    warn_invalid: bool,
    indent: usize,
    legacy_compact_colors: bool,
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

fn resolve_json_color_options(cli: &Cli) -> JsonColorOptions {
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

fn validate_jq_colors(raw: &str) -> bool {
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
fn render_raw_output0(
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
fn write_json_output<W: Write>(
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
fn write_json_value_line<W: Write>(
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
struct JsonColorPalette {
    null: String,
    r#false: String,
    r#true: String,
    num: String,
    str: String,
    arr: String,
    obj: String,
    key: String,
    reset: String,
}

impl JsonColorPalette {
    fn from_jq_colors(raw: Option<&str>) -> Self {
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

fn render_json_value_colored(
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
fn render_json_line(value: &JsonValue, compact: bool, raw_output: bool) -> Result<String, Error> {
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

fn write_jq_style_escaped_del<W: Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    let mut start = 0usize;
    for (idx, byte) in bytes.iter().enumerate() {
        if *byte == 0x7f {
            if start < idx {
                writer.write_all(&bytes[start..idx])?;
            }
            writer.write_all(b"\\u007f")?;
            start = idx + 1;
        }
    }
    if start < bytes.len() {
        writer.write_all(&bytes[start..])?;
    }
    Ok(())
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

fn resolve_effective_input_format(cli_format: InputFormat, path: &str) -> zq::NativeInputFormat {
    if !matches!(cli_format, InputFormat::Auto) {
        return cli_input_format_to_native(cli_format);
    }
    detect_input_format_from_path(path).unwrap_or(zq::NativeInputFormat::Auto)
}

fn cli_input_format_to_native(format: InputFormat) -> zq::NativeInputFormat {
    match format {
        InputFormat::Auto => zq::NativeInputFormat::Auto,
        InputFormat::Json => zq::NativeInputFormat::Json,
        InputFormat::Yaml => zq::NativeInputFormat::Yaml,
        InputFormat::Toml => zq::NativeInputFormat::Toml,
        InputFormat::Csv => zq::NativeInputFormat::Csv,
        InputFormat::Xml => zq::NativeInputFormat::Xml,
    }
}

fn cli_yaml_anchor_name_mode_to_native(mode: CliYamlAnchorNameMode) -> zq::YamlAnchorNameMode {
    match mode {
        CliYamlAnchorNameMode::Friendly => zq::YamlAnchorNameMode::Friendly,
        CliYamlAnchorNameMode::StrictFriendly => zq::YamlAnchorNameMode::StrictFriendly,
    }
}

fn detect_input_format_from_path(path: &str) -> Option<zq::NativeInputFormat> {
    if path == "-" {
        return None;
    }
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|value| value.to_str())?
        .to_ascii_lowercase();
    match ext.as_str() {
        "json" | "jsonl" | "ndjson" => Some(zq::NativeInputFormat::Json),
        "yaml" | "yml" => Some(zq::NativeInputFormat::Yaml),
        "toml" => Some(zq::NativeInputFormat::Toml),
        "csv" | "tsv" => Some(zq::NativeInputFormat::Csv),
        "xml" => Some(zq::NativeInputFormat::Xml),
        _ => None,
    }
}

fn resolve_input_path(cli: &Cli, positional_input: Option<&str>) -> Result<String, Error> {
    if cli.input_file.is_some() && cli.input_legacy.is_some() {
        return Err(Error::Query(
            "input path is specified twice (use either positional FILE or --input)".to_string(),
        ));
    }
    if let Some(path) = positional_input {
        return Ok(path.to_string());
    }
    if let Some(path) = &cli.input_legacy {
        return Ok(path.clone());
    }
    Ok("-".to_string())
}

fn resolve_positional_input(cli: &Cli) -> Result<Option<String>, Error> {
    if cli.from_file.is_none() {
        return Ok(cli.input_file.clone());
    }

    match (&cli.query, &cli.input_file) {
        (Some(_), Some(_)) => Err(Error::Query(
            "too many positional arguments with -f (expected optional FILE)".to_string(),
        )),
        (Some(path), None) => Ok(Some(path.clone())),
        (None, maybe_file) => Ok(maybe_file.clone()),
    }
}

fn resolve_base_query(cli: &Cli) -> Result<String, Error> {
    if let Some(path) = cli.from_file.as_deref() {
        return fs::read_to_string(path).map_err(Error::from);
    }
    if requires_filter_for_interactive_stdin(cli, io::stdin().is_terminal()) {
        return Err(Error::Query(
            "zq: error: missing FILTER (run with a filter like '.' or pipe input into zq)"
                .to_string(),
        ));
    }
    Ok(cli.query.clone().unwrap_or_else(|| ".".to_string()))
}

fn requires_filter_for_interactive_stdin(cli: &Cli, stdin_is_terminal: bool) -> bool {
    stdin_is_terminal
        && cli.query.is_none()
        && cli.from_file.is_none()
        && cli.input_file.is_none()
        && cli.input_legacy.is_none()
        && !cli.null_input
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
