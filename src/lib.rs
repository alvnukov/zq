mod native_engine;
#[path = "query_native.rs"]
mod query;
mod yamlmerge;

pub mod engine;

pub use engine::{
    format_output_json_lines, format_output_yaml_documents, format_query_error, jsonish_equal,
    normalize_jsonish_line, parse_doc_mode, parse_jq_input_values, prepare_jq_query_with_paths,
    run_jq, run_jq_jsonish_lines, run_jq_stream_with_paths_options,
    try_run_jq_native_stream_with_paths_options, validate_jq_query, validate_jq_query_with_paths,
    DocMode, Error as EngineError, NativeStreamStatus, PreparedJq, QueryOptions,
    RunOptions as EngineRunOptions,
};
pub use query::Error as QueryError;
