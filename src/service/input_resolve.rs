use super::*;
use std::io::IsTerminal;

pub(super) fn resolve_effective_input_format(
    cli_format: InputFormat,
    path: &str,
) -> zq::NativeInputFormat {
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

pub(super) fn resolve_input_paths(
    cli: &Cli,
    positional_inputs: &[String],
) -> Result<Vec<String>, Error> {
    if !positional_inputs.is_empty() && cli.input_legacy.is_some() {
        return Err(Error::Query(
            "input path is specified twice (use either positional FILE or --input)".to_string(),
        ));
    }
    if !positional_inputs.is_empty() {
        return Ok(positional_inputs.to_vec());
    }
    if let Some(path) = &cli.input_legacy {
        return Ok(vec![path.clone()]);
    }
    Ok(vec!["-".to_string()])
}

pub(super) fn resolve_positional_inputs(cli: &Cli) -> Result<Vec<String>, Error> {
    if cli.from_file.is_none() {
        return Ok(cli.input_files.clone());
    }

    let mut paths = Vec::new();
    if let Some(path) = &cli.query {
        paths.push(path.clone());
    }
    paths.extend(cli.input_files.iter().cloned());
    Ok(paths)
}

pub(super) fn resolve_base_query(cli: &Cli) -> Result<String, Error> {
    if let Some(path) = cli.from_file.as_deref() {
        return fs::read_to_string(path).map_err(Error::from);
    }
    if requires_filter_for_interactive_stdin(cli, io::stdin().is_terminal()) {
        let tool = cli_error_tool_name();
        return Err(Error::Query(format!(
            "{tool}: error: missing FILTER (run with a filter like '.' or pipe input into {tool})"
        )));
    }
    Ok(cli.query.clone().unwrap_or_else(|| ".".to_string()))
}

pub(super) fn requires_filter_for_interactive_stdin(cli: &Cli, stdin_is_terminal: bool) -> bool {
    stdin_is_terminal
        && cli.query.is_none()
        && cli.from_file.is_none()
        && cli.input_files.is_empty()
        && cli.input_legacy.is_none()
        && !cli.null_input
}
