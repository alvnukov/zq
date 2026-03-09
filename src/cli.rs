use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

const JQ_COMPAT_HELP: &str = "\
jq compatibility options accepted by zq:
      --arg name value      set $name to string value
      --argjson name value  set $name to JSON value
      --slurpfile name file set $name to array of JSON values from file
      --rawfile name file   set $name to file contents as string
      --args                consume remaining args as string values
      --jsonargs            consume remaining args as JSON values
";

#[derive(Parser, Debug)]
#[command(
    name = "zq",
    about = "zq runs jq queries on structured input (JSON/YAML/TOML/CSV/XML)",
    after_help = JQ_COMPAT_HELP,
    version,
    args_override_self = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<CliCommand>,

    #[arg(
        value_name = "FILTER",
        help = "jq filter expression (defaults to . when omitted)"
    )]
    pub query: Option<String>,
    #[arg(
        value_name = "FILE",
        help = "Input file path. If omitted, stdin is used ('-')."
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
        long = "input-format",
        value_enum,
        default_value_t = InputFormat::Auto,
        help = "Input format: auto, json, yaml, toml, csv, xml"
    )]
    pub input_format: InputFormat,
    #[arg(
        long = "csv-parse-json-cells",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "When reading CSV, parse JSON literals in cells (arrays/objects/scalars)"
    )]
    pub csv_parse_json_cells: bool,
    #[arg(
        short = 'L',
        long = "library-path",
        value_name = "DIR",
        action = ArgAction::Append,
        help = "Module search path (accepted for jq compatibility)"
    )]
    pub library_path: Vec<String>,
    #[arg(
        short = 'f',
        long = "from-file",
        value_name = "FILE",
        help = "Read filter from a file"
    )]
    pub from_file: Option<String>,
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
        action = ArgAction::Append,
        value_delimiter = ',',
        num_args = 0..,
        default_missing_value = "-",
        help = "Run jq-style test file(s) from FILE or stdin"
    )]
    pub run_tests: Vec<String>,
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
        long = "diff",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Semantic diff mode (compare JSON/YAML inputs like dyff)"
    )]
    pub diff: bool,
    #[arg(
        long = "diff-format",
        value_enum,
        default_value_t = DiffOutputFormat::Diff,
        requires = "diff",
        help = "Diff report format: diff, patch, json, jsonl, summary"
    )]
    pub diff_format: DiffOutputFormat,
    #[arg(
        short = 'c',
        long = "compact-output",
        visible_alias = "compact",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub compact: bool,
    #[arg(
        long = "indent",
        value_name = "N",
        value_parser = clap::value_parser!(u8).range(0..=7),
        help = "Use N spaces for indentation (0-7)"
    )]
    pub indent: Option<u8>,
    #[arg(
        short = 'C',
        long = "color-output",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Colorize JSON output"
    )]
    pub color_output: bool,
    #[arg(
        short = 'M',
        long = "monochrome-output",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Disable colorized JSON output"
    )]
    pub monochrome_output: bool,
    #[arg(
        short = 'r',
        long = "raw-output",
        default_value_t = false,
        action = ArgAction::SetTrue
    )]
    pub raw_output: bool,
    #[arg(
        short = 'j',
        long = "join-output",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Like --raw-output, but without trailing newline after each output"
    )]
    pub join_output: bool,
    #[arg(
        long = "raw-output0",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Like --raw-output, but print NUL after each output"
    )]
    pub raw_output0: bool,
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
        long = "debug-dump-disasm",
        default_value_t = false,
        action = ArgAction::SetTrue,
        hide = true,
        help = "Print disassembly (accepted for jq compatibility; currently unsupported)"
    )]
    pub debug_dump_disasm: bool,
    #[arg(
        long = "seq",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Use application/json-seq framing (jq compatibility, partial support)"
    )]
    pub seq: bool,
    #[arg(
        long = "stream",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Parse the input in streaming mode (jq compatibility, partial support)"
    )]
    pub stream: bool,
    #[arg(
        long = "stream-errors",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Like --stream, but output parser errors as data"
    )]
    pub stream_errors: bool,
    #[arg(
        long = "output-format",
        value_enum,
        default_value_t = OutputFormat::Json,
        help = "Output format: json, yaml, toml, csv, xml"
    )]
    pub output_format: OutputFormat,
    #[arg(
        long = "yaml-anchors",
        default_value_t = false,
        action = ArgAction::SetTrue,
        help = "Enable YAML anchors/aliases for repeated values in --output-format=yaml"
    )]
    pub yaml_anchors: bool,
    #[arg(
        long = "yaml-anchor-name-mode",
        value_enum,
        default_value_t = YamlAnchorNameMode::Friendly,
        help = "Anchor naming mode: friendly or strict-friendly (requires --yaml-anchors)"
    )]
    pub yaml_anchor_name_mode: YamlAnchorNameMode,
}

#[derive(Subcommand, Debug, Clone)]
pub enum CliCommand {
    #[command(about = "Generate shell completion scripts (kubectl style)")]
    Completion {
        #[arg(value_enum, value_name = "SHELL")]
        shell: Shell,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Json,
    Yaml,
    Toml,
    Csv,
    Xml,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum InputFormat {
    Auto,
    Json,
    Yaml,
    Toml,
    Csv,
    Xml,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DiffOutputFormat {
    Diff,
    Patch,
    Json,
    Jsonl,
    Summary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum YamlAnchorNameMode {
    Friendly,
    StrictFriendly,
}
