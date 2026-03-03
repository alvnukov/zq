use clap::{ArgAction, Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "zq", about = "zq runs jq queries on JSON or YAML input")]
pub struct Cli {
    #[arg(
        value_name = "FILTER",
        help = "jq filter expression",
        required_unless_present = "run_tests"
    )]
    pub query: Option<String>,
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
        short = 'L',
        long = "library-path",
        value_name = "DIR",
        action = ArgAction::Append,
        help = "Module search path (accepted for jq compatibility)"
    )]
    pub library_path: Vec<String>,
    #[arg(
        short = 'b',
        long = "binary",
        default_value_t = false,
        action = ArgAction::SetTrue,
        hide = true,
        help = "Preserve line endings (jq compatibility no-op on non-Windows)"
    )]
    pub binary_noop: bool,
    #[arg(
        long = "run-tests",
        value_name = "FILE",
        num_args = 0..=1,
        default_missing_value = "-",
        help = "Run jq-style test file from FILE or stdin"
    )]
    pub run_tests: Option<String>,
    #[arg(
        long = "skip",
        value_name = "N",
        requires = "run_tests",
        help = "Skip the first N tests in --run-tests mode"
    )]
    pub run_tests_skip: Option<usize>,
    #[arg(
        long = "take",
        value_name = "N",
        requires = "run_tests",
        help = "Run only N tests in --run-tests mode"
    )]
    pub run_tests_take: Option<usize>,
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
        short = 'R',
        long = "raw-input",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub raw_input: bool,
    #[arg(
        short = 's',
        long = "slurp",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub slurp: bool,
    #[arg(
        short = 'n',
        long = "null-input",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub null_input: bool,
    #[arg(
        short = 'e',
        long = "exit-status",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub exit_status: bool,
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
