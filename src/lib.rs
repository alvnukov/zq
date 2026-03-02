mod query;
mod yamlmerge;

pub mod engine;

pub use engine::{
    format_output_json_lines, format_output_yaml_documents, format_query_error, parse_doc_mode,
    run_jq, DocMode, Error as EngineError, QueryOptions,
};
pub use query::Error as QueryError;
