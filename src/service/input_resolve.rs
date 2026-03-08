use super::*;

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

pub(super) fn resolve_input_path(
    cli: &Cli,
    positional_input: Option<&str>,
) -> Result<String, Error> {
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

pub(super) fn resolve_positional_input(cli: &Cli) -> Result<Option<String>, Error> {
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

pub(super) fn resolve_base_query(cli: &Cli) -> Result<String, Error> {
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

pub(super) fn requires_filter_for_interactive_stdin(cli: &Cli, stdin_is_terminal: bool) -> bool {
    stdin_is_terminal
        && cli.query.is_none()
        && cli.from_file.is_none()
        && cli.input_file.is_none()
        && cli.input_legacy.is_none()
        && !cli.null_input
}
