use clap::Parser;
use std::fs;
use std::io::{self, Read};

use crate::cli::{Cli, OutputFormat};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("query: {0}")]
    Query(String),
}

pub fn run() -> Result<(), Error> {
    let cli = Cli::parse();
    run_with(cli)
}

fn run_with(cli: Cli) -> Result<(), Error> {
    let input_path = resolve_input_path(&cli)?;

    if matches!(cli.output_format, OutputFormat::Yaml) && cli.raw_output {
        return Err(Error::Query(
            "--raw-output is supported only with --output-format=json".to_string(),
        ));
    }
    if matches!(cli.output_format, OutputFormat::Yaml) && cli.compact {
        return Err(Error::Query(
            "--compact is supported only with --output-format=json".to_string(),
        ));
    }

    let input = read_input(&input_path)?;
    let options = zq::QueryOptions {
        doc_mode: zq::parse_doc_mode(&cli.doc_mode, cli.doc_index)
            .map_err(|e| Error::Query(e.to_string()))?,
    };
    let out = zq::run_jq(&cli.query, &input, options)
        .map_err(|e| Error::Query(render_engine_error("jq", &input, e)))?;

    let rendered = match cli.output_format {
        OutputFormat::Json => zq::format_output_json_lines(&out, cli.compact, cli.raw_output)
            .map_err(|e| Error::Query(e.to_string()))?,
        OutputFormat::Yaml => {
            zq::format_output_yaml_documents(&out).map_err(|e| Error::Query(e.to_string()))?
        }
    };

    if !rendered.is_empty() {
        println!("{rendered}");
    }
    Ok(())
}

fn read_input(path: &str) -> Result<String, Error> {
    if path == "-" {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        return Ok(s);
    }
    Ok(fs::read_to_string(path)?)
}

fn resolve_input_path(cli: &Cli) -> Result<String, Error> {
    if cli.input_file.is_some() && cli.input_legacy.is_some() {
        return Err(Error::Query(
            "input path is specified twice (use either positional FILE or --input)".to_string(),
        ));
    }
    if let Some(path) = &cli.input_file {
        return Ok(path.clone());
    }
    if let Some(path) = &cli.input_legacy {
        return Ok(path.clone());
    }
    Ok("-".to_string())
}

fn render_engine_error(tool: &str, input: &str, err: zq::EngineError) -> String {
    match err {
        zq::EngineError::Query(inner) => zq::format_query_error(tool, input, &inner),
        other => other.to_string(),
    }
}
