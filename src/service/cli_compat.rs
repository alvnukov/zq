use super::Error;
use crate::cli::Cli;
use clap::{error::ErrorKind, Parser};
use std::fs;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionalArgsMode {
    String,
    Json,
}

#[derive(Debug, Default, Clone)]
pub(super) struct CliCompatArgs {
    pub(super) named_vars: indexmap::IndexMap<String, zq::NativeValue>,
    pub(super) named_args: indexmap::IndexMap<String, zq::NativeValue>,
    pub(super) positional_args: Vec<zq::NativeValue>,
}

impl CliCompatArgs {
    pub(super) fn is_empty(&self) -> bool {
        self.named_vars.is_empty() && self.named_args.is_empty() && self.positional_args.is_empty()
    }
}

pub(super) fn parse_cli_with_compat_args() -> Result<Option<(Cli, CliCompatArgs)>, Error> {
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

pub(super) fn extract_cli_compat_args(
    args: Vec<String>,
) -> Result<(Vec<String>, CliCompatArgs), Error> {
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
                compat.named_vars.insert(name, zq::NativeValue::Array(values));
                i += 3;
            }
            "--rawfile" => {
                let (name_raw, path_raw) =
                    parse_named_arg_pair(&args, i, "--rawfile requires two arguments: NAME FILE")?;
                let name = name_raw.to_string();
                let value = fs::read_to_string(path_raw)?;
                compat.named_vars.insert(name, zq::NativeValue::String(value));
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

pub(super) fn build_query_with_cli_compat(
    query: &str,
    compat: &CliCompatArgs,
) -> Result<String, Error> {
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
