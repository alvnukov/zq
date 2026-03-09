mod c_compat;
mod native_engine;
#[path = "query_native.rs"]
mod query;
mod value;
mod yamlmerge;

pub mod engine;

pub use engine::{
    debug_dump_disasm_function_labels, format_output_json_lines, format_output_yaml_documents,
    format_output_yaml_documents_native, format_output_yaml_documents_native_with_options,
    format_output_yaml_documents_with_options, format_query_error, format_query_error_with_sources,
    jsonish_equal, normalize_jsonish_line, parse_doc_mode, parse_jq_input_values,
    parse_jq_input_values_native, parse_jq_input_values_with_format,
    parse_jq_input_values_with_format_native, parse_jq_json_values_only,
    parse_jq_json_values_only_native, prepare_jq_query_with_paths, run_jq, run_jq_jsonish_lines,
    run_jq_native, run_jq_stream_with_paths_options, run_jq_stream_with_paths_options_native,
    try_run_jq_native_stream_json_reader_options_native,
    try_run_jq_native_stream_json_text_options, try_run_jq_native_stream_json_text_options_native,
    try_run_jq_native_stream_with_paths_options,
    try_run_jq_native_stream_with_paths_options_native, validate_jq_query,
    validate_jq_query_with_paths, DocMode, Error as EngineError, NativeStreamStatus, PreparedJq,
    QueryOptions, RunOptions as EngineRunOptions, YamlAnchorNameMode, YamlFormatOptions,
};
pub use query::Error as QueryError;
pub use query::{
    parse_input_docs_prefer_json as parse_native_input_docs_prefer_json,
    parse_input_docs_prefer_json_native as parse_native_input_docs_prefer_json_native,
    parse_input_docs_prefer_yaml as parse_native_input_docs_prefer_yaml,
    parse_input_docs_prefer_yaml_native as parse_native_input_docs_prefer_yaml_native,
    parse_input_values_auto as parse_native_input_values_auto,
    parse_input_values_auto_native as parse_native_input_values_auto_native,
    parse_input_values_with_format as parse_native_input_values_with_format,
    parse_input_values_with_format_native as parse_native_input_values_with_format_native,
    parse_json_values_only as parse_native_json_values_only,
    parse_json_values_only_native as parse_native_json_values_only_native,
    prepare_query_with_paths as prepare_native_query_with_paths,
    run_json_query as run_native_json_query, run_json_query_native as run_native_json_query_native,
    run_query_stream as run_native_query_stream,
    run_query_stream_jsonish as run_native_query_stream_jsonish,
    run_query_stream_native as run_native_query_stream_native,
    run_query_stream_native_with_paths as run_native_query_stream_native_with_paths,
    run_query_stream_native_with_paths_and_options as run_native_query_stream_native_with_paths_and_options,
    run_query_stream_with_paths as run_native_query_stream_with_paths,
    run_query_stream_with_paths_and_options as run_native_query_stream_with_paths_and_options,
    run_yaml_query as run_native_yaml_query, run_yaml_query_native as run_native_yaml_query_native,
    validate_query as validate_native_query,
    validate_query_with_paths as validate_native_query_with_paths,
    InputFormat as NativeInputFormat, InputKind as NativeInputKind,
    ParsedInput as NativeParsedInput, ParsedNativeInput as NativeParsedNativeInput,
    PreparedQuery as NativePreparedQuery, RunOptions as NativeRunOptions,
};
pub use value::ZqValue as NativeValue;

pub fn decode_native_halt_error(err: &str) -> Option<(i32, String)> {
    native_engine::decode_halt_error(err)
}
