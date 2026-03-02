use clap::{ArgAction, Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "zq", about = "zq runs jq queries on JSON or YAML input")]
pub struct Cli {
    #[arg(value_name = "FILTER", help = "jq filter expression")]
    pub query: String,
    #[arg(
        value_name = "FILE",
        help = "Input file path. If omitted, stdin is used ('-'). Supports JSON and YAML."
    )]
    pub input_file: Option<String>,
    #[arg(
        long = "input",
        hide = true,
        help = "Legacy alias for input file path (use positional FILE)"
    )]
    pub input_legacy: Option<String>,
    #[arg(
        long = "doc-mode",
        default_value = "first",
        help = "Document selection for YAML streams: first|all|index"
    )]
    pub doc_mode: String,
    #[arg(
        long = "doc-index",
        help = "Zero-based document index when --doc-mode=index"
    )]
    pub doc_index: Option<usize>,
    #[arg(
        short = 'c',
        long = "compact-output",
        visible_alias = "compact",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub compact: bool,
    #[arg(
        short = 'r',
        long = "raw-output",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub raw_output: bool,
    #[arg(
        long = "output-format",
        value_enum,
        default_value_t = OutputFormat::Json,
        help = "Output format: json (jq-like output) or yaml"
    )]
    pub output_format: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Json,
    Yaml,
}
