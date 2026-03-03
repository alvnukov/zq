mod native_engine;
#[path = "query_native.rs"]
mod query;
mod yamlmerge;

pub mod engine;

pub use engine::{
    format_output_json_lines, format_output_yaml_documents, format_query_error, jsonish_equal,
    normalize_jsonish_line, parse_doc_mode, parse_jq_input_values, parse_jq_json_values_only,
    prepare_jq_query_with_paths, run_jq, run_jq_jsonish_lines, run_jq_stream_with_paths_options,
    try_run_jq_native_stream_json_text_options, try_run_jq_native_stream_with_paths_options,
    validate_jq_query, validate_jq_query_with_paths, DocMode, Error as EngineError,
    NativeStreamStatus, PreparedJq, QueryOptions, RunOptions as EngineRunOptions,
};
pub use query::Error as QueryError;
pub use query::{
    parse_input_docs_prefer_json as parse_native_input_docs_prefer_json,
    parse_input_docs_prefer_yaml as parse_native_input_docs_prefer_yaml,
    parse_input_values_auto as parse_native_input_values_auto,
    parse_json_values_only as parse_native_json_values_only,
    prepare_query_with_paths as prepare_native_query_with_paths,
    run_json_query as run_native_json_query, run_query_stream as run_native_query_stream,
    run_query_stream_jsonish as run_native_query_stream_jsonish,
    run_query_stream_with_paths as run_native_query_stream_with_paths,
    run_query_stream_with_paths_and_options as run_native_query_stream_with_paths_and_options,
    run_yaml_query as run_native_yaml_query, validate_query as validate_native_query,
    validate_query_with_paths as validate_native_query_with_paths, InputKind as NativeInputKind,
    ParsedInput as NativeParsedInput, PreparedQuery as NativePreparedQuery,
    RunOptions as NativeRunOptions,
};
