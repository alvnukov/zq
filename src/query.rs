use jaq_all::data::{self, Filter as JaqFilter, Runner as JaqRunner};
use jaq_all::fmts::read::{self as jaq_read, json as jaq_json_read};
use jaq_all::jaq_core::{
    compile::{Compiler, Undefined as CompileUndefined},
    load::{
        self as core_load, import,
        lex as core_lex,
        lex::StrPart,
        parse::{self as core_parse, BinaryOp, Term},
        Arena, File, Loader, Modules,
    },
    Vars,
};
use jaq_all::json::Val as JaqValue;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

const JQ_PRELUDE: &str = include_str!("jq_prelude.jq");
const DEFAULT_LIBRARY_PATHS: [&str; 3] = ["~/.jq", "$ORIGIN/../lib/jq", "$ORIGIN/../lib"];
const MODULEMETA_GLOBAL: &str = "$__zq_modulemeta";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported query: {0}")]
    Unsupported(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("{0}")]
    Thrown(JsonValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    JsonStream,
    YamlDocs,
}

#[derive(Debug, Clone)]
pub struct ParsedInput {
    pub kind: InputKind,
    pub values: Vec<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOptions {
    pub null_input: bool,
}

#[allow(dead_code)]
pub fn run_json_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    let input_value: JsonValue = match serde_json::from_str(input) {
        Ok(v) => v,
        Err(json_err) => match parse_yaml_json_with_merge(input) {
            Ok(v) => v,
            Err(Error::Yaml(_)) => return Err(Error::Json(json_err)),
            Err(e) => return Err(e),
        },
    };
    run_query_stream(query, vec![input_value])
}

#[allow(dead_code)]
pub fn run_yaml_query(query: &str, input: &str) -> Result<Vec<JsonValue>, Error> {
    let as_json: JsonValue = match parse_yaml_json_with_merge(input) {
        Ok(v) => v,
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str(input) {
            Ok(v) => v,
            Err(_) => return Err(Error::Yaml(yaml_err)),
        },
        Err(e) => return Err(e),
    };
    run_query_stream(query, vec![as_json])
}

pub fn run_query_stream(
    query: &str,
    input_stream: Vec<JsonValue>,
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, &[], RunOptions::default())
}

pub fn run_query_stream_with_paths(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
) -> Result<Vec<JsonValue>, Error> {
    run_query_stream_with_paths_and_options(query, input_stream, library_paths, RunOptions::default())
}

pub fn run_query_stream_with_paths_and_options(
    query: &str,
    input_stream: Vec<JsonValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<JsonValue>, Error> {
    let inputs = input_stream
        .into_iter()
        .map(json_to_jaq)
        .collect::<Result<Vec<_>, _>>()?;
    let outputs = execute_query(query, inputs, library_paths, run_options)?;
    outputs
        .iter()
        .map(jaq_to_json)
        .collect::<Result<Vec<_>, _>>()
}

pub fn run_query_stream_jsonish(
    query: &str,
    input_jsonish: &str,
    library_paths: &[String],
) -> Result<Vec<String>, Error> {
    let outputs = run_query_stream_jsonish_values(query, input_jsonish, library_paths)?;
    outputs
        .iter()
        .map(stringify_jsonish_value)
        .collect::<Result<Vec<_>, _>>()
}

pub fn run_query_stream_jsonish_values(
    query: &str,
    input_jsonish: &str,
    library_paths: &[String],
) -> Result<Vec<JaqValue>, Error> {
    let prepared = prepare_query_with_paths(query, library_paths)?;
    prepared.run_jsonish_values(input_jsonish)
}

pub struct PreparedQuery {
    compiled: CompiledProgram,
}

impl PreparedQuery {
    pub fn run_jsonish(&self, input_jsonish: &str) -> Result<Vec<String>, Error> {
        let outputs = self.run_jsonish_values(input_jsonish)?;
        outputs
            .iter()
            .map(stringify_jsonish_value)
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn run_jsonish_values(&self, input_jsonish: &str) -> Result<Vec<JaqValue>, Error> {
        let input = parse_jsonish_value(input_jsonish)?;
        run_compiled_query(&self.compiled, vec![input], RunOptions::default())
    }

    pub fn run_jsonish_lenient(&self, input_jsonish: &str) -> Result<Vec<String>, Error> {
        let outputs = self.run_jsonish_values_lenient(input_jsonish)?;
        outputs
            .iter()
            .map(stringify_jsonish_value)
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn run_jsonish_values_lenient(&self, input_jsonish: &str) -> Result<Vec<JaqValue>, Error> {
        let input = parse_jsonish_value(input_jsonish)?;
        run_compiled_query_lenient(&self.compiled, vec![input])
    }
}

pub fn prepare_query_with_paths(query: &str, library_paths: &[String]) -> Result<PreparedQuery, Error> {
    let compiled = compile_program(query, library_paths)?;
    Ok(PreparedQuery { compiled })
}

pub fn validate_query(query: &str) -> Result<(), Error> {
    validate_query_with_paths(query, &[])
}

pub fn validate_query_with_paths(query: &str, library_paths: &[String]) -> Result<(), Error> {
    compile_program(query, library_paths).map(|_| ())
}

pub fn normalize_jsonish_line(line: &str) -> Result<String, Error> {
    let value = parse_jsonish_value(line)?;
    stringify_jsonish_value(&value)
}

pub fn jsonish_equal(left: &str, right: &str) -> Result<bool, Error> {
    let left = parse_jsonish_value(left)?;
    let right = parse_jsonish_value(right)?;
    Ok(left == right)
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_json(input: &str) -> Result<Vec<JsonValue>, Error> {
    match parse_json_value_stream(input) {
        Ok(v) => Ok(v),
        Err(json_err) => match parse_yaml_json_docs_with_merge(input) {
            Ok(v) => Ok(v),
            Err(Error::Yaml(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

pub fn parse_input_values_auto(input: &str) -> Result<ParsedInput, Error> {
    match parse_json_value_stream(input) {
        Ok(values) => Ok(ParsedInput {
            kind: InputKind::JsonStream,
            values,
        }),
        Err(json_err) => match parse_yaml_json_docs_with_merge(input) {
            Ok(values) => Ok(ParsedInput {
                kind: InputKind::YamlDocs,
                values,
            }),
            Err(Error::Yaml(_)) => Err(Error::Json(json_err)),
            Err(e) => Err(e),
        },
    }
}

#[allow(dead_code)]
pub fn parse_input_docs_prefer_yaml(input: &str) -> Result<Vec<JsonValue>, Error> {
    match parse_yaml_json_docs_with_merge(input) {
        Ok(v) => Ok(v),
        Err(Error::Yaml(yaml_err)) => match serde_json::from_str::<JsonValue>(input) {
            Ok(v) => Ok(vec![v]),
            Err(_) => Err(Error::Yaml(yaml_err)),
        },
        Err(e) => Err(e),
    }
}

fn execute_query(
    query: &str,
    inputs: Vec<JaqValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> Result<Vec<JaqValue>, Error> {
    let inputs_json = inputs
        .iter()
        .map(jaq_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    match crate::native_engine::try_execute(
        query,
        &inputs_json,
        crate::native_engine::RunOptions {
            null_input: run_options.null_input,
        },
    ) {
        crate::native_engine::TryExecute::Executed(Ok(values)) => {
            return values.into_iter().map(json_to_jaq).collect();
        }
        crate::native_engine::TryExecute::Executed(Err(e)) => {
            return Err(Error::Unsupported(e));
        }
        crate::native_engine::TryExecute::Unsupported => {}
    }

    let compiled = compile_program(query, library_paths)?;
    run_compiled_query(&compiled, inputs, run_options)
}

#[allow(dead_code)]
fn parse_yaml_json_with_merge(input: &str) -> Result<JsonValue, Error> {
    let raw: serde_yaml::Value = serde_yaml::from_str(input).map_err(Error::Yaml)?;
    let normalized = crate::yamlmerge::normalize_value_from_source(input, raw);
    serde_json::to_value(normalized)
        .map_err(|e| Error::Unsupported(format!("yaml to json conversion failed: {e}")))
}

fn parse_yaml_json_docs_with_merge(input: &str) -> Result<Vec<JsonValue>, Error> {
    let docs = crate::yamlmerge::normalize_documents(input).map_err(Error::Yaml)?;
    docs.into_iter()
        .map(|v| {
            serde_json::to_value(v)
                .map_err(|e| Error::Unsupported(format!("yaml to json conversion failed: {e}")))
        })
        .collect()
}

struct CompiledProgram {
    filter: JaqFilter,
    runtime_vars: Vec<JaqValue>,
}

fn compile_program(query: &str, library_paths: &[String]) -> Result<CompiledProgram, Error> {
    let arena = Arena::default();
    let paths = resolve_library_paths(library_paths);
    let loader = Loader::new(jaq_all::defs()).with_std_read(&paths);
    let preprocessed_query = preprocess_query(query);
    let wrapped_query = inject_prelude_after_module_directives(&preprocessed_query);
    let precheck_err = || precheck_query_error(&preprocessed_query, &paths);
    let modules = match loader.load(
        &arena,
        File {
            path: PathBuf::from("<inline>"),
            code: &wrapped_query,
        },
    ) {
        Ok(modules) => modules,
        Err(errs) => {
            let msg = precheck_err().unwrap_or_else(|| format_first_load_error(errs));
            return Err(Error::Unsupported(msg));
        }
    };

    let mut modulemeta = build_modulemeta_table(&modules);
    if query.contains("modulemeta") {
        enrich_modulemeta_from_search_paths(&mut modulemeta, &paths);
    }
    let modulemeta = json_to_jaq(JsonValue::Object(modulemeta))?;
    let mut module_vars = Vec::new();
    import(&modules, |path| {
        let data_path = path.find(&paths, "json")?;
        let value = jaq_read::json_array(data_path).map_err(|e| e.to_string())?;
        module_vars.push(value);
        Ok(())
    })
    .map_err(|errs| Error::Unsupported(format_first_load_error(errs)))?;

    let filter = match Compiler::default()
        .with_funs(data::funs())
        .with_global_vars([MODULEMETA_GLOBAL])
        .compile(modules)
    {
        Ok(filter) => filter,
        Err(errs) => {
            let msg = precheck_err().unwrap_or_else(|| format_first_compile_error(errs));
            return Err(Error::Unsupported(msg));
        }
    };

    let mut runtime_vars = Vec::with_capacity(module_vars.len() + 1);
    runtime_vars.push(modulemeta);
    runtime_vars.extend(module_vars);

    Ok(CompiledProgram {
        filter,
        runtime_vars,
    })
}

fn run_compiled_query(
    compiled: &CompiledProgram,
    inputs: Vec<JaqValue>,
    run_options: RunOptions,
) -> Result<Vec<JaqValue>, Error> {
    let runner = JaqRunner {
        null_input: run_options.null_input,
        ..JaqRunner::default()
    };
    let vars = Vars::new(compiled.runtime_vars.clone());
    let input_iter = inputs.into_iter().map(Ok::<JaqValue, String>);

    let mut out = Vec::new();
    data::run(
        &runner,
        &compiled.filter,
        vars,
        input_iter,
        Error::Unsupported,
        |result| {
            let value = result.map_err(|e| Error::Unsupported(e.to_string()))?;
            out.push(value);
            Ok(())
        },
    )?;
    Ok(out)
}

fn parse_json_value_stream(input: &str) -> Result<Vec<JsonValue>, serde_json::Error> {
    let mut stream = serde_json::Deserializer::from_str(input).into_iter::<JsonValue>();
    let mut out = Vec::new();
    while let Some(next) = stream.next() {
        out.push(next?);
    }
    Ok(out)
}

fn run_compiled_query_lenient(
    compiled: &CompiledProgram,
    inputs: Vec<JaqValue>,
) -> Result<Vec<JaqValue>, Error> {
    enum Stop {
        Input(String),
        Stream,
    }

    let runner = JaqRunner::default();
    let vars = Vars::new(compiled.runtime_vars.clone());
    let input_iter = inputs.into_iter().map(Ok::<JaqValue, String>);

    let mut out = Vec::new();
    let result = data::run(
        &runner,
        &compiled.filter,
        vars,
        input_iter,
        Stop::Input,
        |step| match step {
            Ok(v) => {
                out.push(v);
                Ok(())
            }
            Err(_e) => Err(Stop::Stream),
        },
    );

    match result {
        Ok(()) | Err(Stop::Stream) => Ok(out),
        Err(Stop::Input(e)) => Err(Error::Unsupported(e)),
    }
}

fn module_key_from_path(path: &PathBuf) -> Option<String> {
    if path.as_os_str().is_empty() {
        return None;
    }
    path.file_stem().map(|s| s.to_string_lossy().to_string())
}

fn split_commas<'a>(t: &'a Term<&'a str>, out: &mut Vec<&'a Term<&'a str>>) {
    if let Term::BinOp(l, BinaryOp::Comma, r) = t {
        split_commas(l, out);
        split_commas(r, out);
    } else {
        out.push(t);
    }
}

fn const_string_term(t: &Term<&str>) -> Option<String> {
    match t {
        Term::Str(None, parts) => {
            let mut s = String::new();
            for p in parts {
                match p {
                    StrPart::Str(p) => s.push_str(p),
                    StrPart::Char(c) => s.push(*c),
                    StrPart::Term(_) => return None,
                }
            }
            Some(s)
        }
        _ => None,
    }
}

fn const_json_term(t: &Term<&str>) -> Option<JsonValue> {
    match t {
        Term::Call("null", args) if args.is_empty() => Some(JsonValue::Null),
        Term::Call("true", args) if args.is_empty() => Some(JsonValue::Bool(true)),
        Term::Call("false", args) if args.is_empty() => Some(JsonValue::Bool(false)),
        Term::Num(n) => serde_json::from_str::<JsonValue>(n).ok(),
        Term::Neg(inner) => match &**inner {
            Term::Num(n) => serde_json::from_str::<JsonValue>(&format!("-{n}")).ok(),
            _ => None,
        },
        Term::Str(None, _) => const_string_term(t).map(JsonValue::String),
        Term::Arr(None) => Some(JsonValue::Array(Vec::new())),
        Term::Arr(Some(items)) => {
            let mut parts = Vec::new();
            split_commas(items, &mut parts);
            let mut arr = Vec::with_capacity(parts.len());
            for item in parts {
                arr.push(const_json_term(item)?);
            }
            Some(JsonValue::Array(arr))
        }
        Term::Obj(entries) => {
            let mut obj = JsonMap::new();
            for (k, v) in entries {
                let key = const_string_term(k)?;
                let value = match v {
                    Some(v) => const_json_term(v)?,
                    None => const_json_term(k)?,
                };
                obj.insert(key, value);
            }
            Some(JsonValue::Object(obj))
        }
        _ => None,
    }
}

fn build_modulemeta_table(modules: &Modules<&str, PathBuf>) -> JsonMap<String, JsonValue> {
    let mut table = JsonMap::new();

    for (file, module) in modules {
        let Some(key) = module_key_from_path(&file.path) else {
            continue;
        };

        let mut module_obj = module
            .meta()
            .and_then(const_json_term)
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();

        let mut deps = Vec::new();
        for dep in module.deps() {
            let mut dep_obj = dep
                .meta()
                .and_then(const_json_term)
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default();

            if let Some(alias) = dep.as_() {
                let alias = if dep.is_data() {
                    alias.strip_prefix('$').unwrap_or(alias)
                } else {
                    alias
                };
                dep_obj.insert("as".to_string(), JsonValue::String(alias.to_string()));
            }
            dep_obj.insert("is_data".to_string(), JsonValue::Bool(dep.is_data()));
            dep_obj.insert(
                "relpath".to_string(),
                JsonValue::String(dep.path().to_string()),
            );
            deps.push(JsonValue::Object(dep_obj));
        }

        let defs = module
            .body()
            .iter()
            .filter(|d| d.name != "main")
            .map(|d| JsonValue::String(format!("{}/{}", d.name, d.args.len())))
            .collect::<Vec<_>>();

        module_obj.insert("deps".to_string(), JsonValue::Array(deps));
        module_obj.insert("defs".to_string(), JsonValue::Array(defs));
        table.insert(key, JsonValue::Object(module_obj));
    }

    table
}

fn collect_jq_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = fs::read_dir(root) else {
        return;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_jq_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "jq") {
            out.push(path);
        }
    }
}

fn enrich_modulemeta_from_search_paths(
    table: &mut JsonMap<String, JsonValue>,
    search_paths: &[PathBuf],
) {
    let mut candidates = BTreeSet::new();
    for root in search_paths {
        if !root.is_dir() {
            continue;
        }

        let mut files = Vec::new();
        collect_jq_files(root, &mut files);
        for file in files {
            let Some(key) = module_key_from_path(&file) else {
                continue;
            };
            candidates.insert(key);
        }
    }

    for key in candidates {
        if table.contains_key(&key) {
            continue;
        }
        let probe = format!("import \"{key}\" as __zq_probe; .");
        let arena = Arena::default();
        let code = &*arena.alloc(probe);
        let loader = Loader::new(jaq_all::defs()).with_std_read(search_paths);
        let Ok(modules) = loader.load(
            &arena,
            File {
                path: PathBuf::from("<modulemeta-probe>"),
                code,
            },
        ) else {
            continue;
        };
        let extra = build_modulemeta_table(&modules);
        if let Some(v) = extra.get(&key) {
            table.insert(key, v.clone());
        }
    }
}

fn resolve_library_paths(library_paths: &[String]) -> Vec<PathBuf> {
    if library_paths.is_empty() {
        DEFAULT_LIBRARY_PATHS
            .into_iter()
            .map(PathBuf::from)
            .collect()
    } else {
        library_paths.iter().map(PathBuf::from).collect()
    }
}

fn jq_path_label(path: &PathBuf) -> String {
    let s = path.to_string_lossy();
    if s.is_empty() || s == "<inline>" || s == "<modulemeta-probe>" {
        "<top-level>".to_string()
    } else {
        s.to_string()
    }
}

fn offset_in(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return haystack.len();
    }
    let hs = haystack.as_ptr() as usize;
    let ns = needle.as_ptr() as usize;
    if ns >= hs && ns <= hs + haystack.len() {
        return ns - hs;
    }
    haystack.find(needle).unwrap_or(haystack.len())
}

fn line_col_at(code: &str, offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut col = 1usize;
    for (i, ch) in code.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

fn with_file_loc(message: String, file: &File<&str, PathBuf>, found: &str) -> String {
    let offset = offset_in(file.code, found);
    let (line, col) = line_col_at(file.code, offset);
    format!(
        "{message} at {}, line {line}, column {col}:",
        jq_path_label(&file.path)
    )
}

fn format_lex_message(expected: &core_lex::Expect<&str>, found: &str) -> String {
    match expected {
        core_lex::Expect::Escape => {
            let got = found.chars().next().unwrap_or(' ');
            format!("Invalid escape at line 1, column 4 (while parsing '\"\\{got}\"')")
        }
        core_lex::Expect::Token => {
            if found.is_empty() {
                "syntax error, unexpected end of file".to_string()
            } else if found.starts_with('}') {
                "syntax error, unexpected INVALID_CHARACTER, expecting end of file".to_string()
            } else {
                let tok = found.chars().next().unwrap_or('?');
                format!("syntax error, unexpected '{tok}', expecting end of file")
            }
        }
        core_lex::Expect::Delim("{") => "syntax error, unexpected end of file".to_string(),
        _ => format!("expected {}", expected.as_str()),
    }
}

fn format_parse_message(expected: &core_parse::Expect<&str>, found: &str) -> String {
    match expected {
        core_parse::Expect::Custom(s) => s.to_string(),
        core_parse::Expect::Key => {
            if found.starts_with('}') {
                "syntax error, unexpected '}'".to_string()
            } else if found
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '.')
            {
                "May need parentheses around object key expression".to_string()
            } else if found.starts_with('+')
                || found.starts_with('-')
                || found.starts_with('*')
                || found.starts_with('/')
                || found.starts_with('%')
            {
                "May need parentheses around object key expression".to_string()
            } else {
                "expected key".to_string()
            }
        }
        core_parse::Expect::Pattern if found.starts_with(']') => {
            "syntax error, unexpected ']', expecting BINDING or '[' or '{'".to_string()
        }
        core_parse::Expect::Term if found.starts_with('%') => {
            "syntax error, unexpected '%', expecting end of file".to_string()
        }
        core_parse::Expect::Nothing => {
            if found.is_empty() {
                "syntax error, unexpected end of file".to_string()
            } else if found.starts_with('}') {
                "syntax error, unexpected INVALID_CHARACTER, expecting end of file".to_string()
            } else {
                let tok = found.chars().next().unwrap_or('?');
                format!("syntax error, unexpected '{tok}', expecting end of file")
            }
        }
        _ => format!("expected {}", expected.as_str()),
    }
}

fn format_compile_message(found: &str, undefined: &CompileUndefined) -> String {
    let wnoa = |exp, got| format!("wrong number of arguments (expected {exp}, found {got})");
    match (found, undefined) {
        ("reduce", CompileUndefined::Filter(arity)) => wnoa("2", arity),
        ("foreach", CompileUndefined::Filter(arity)) => wnoa("2 or 3", arity),
        (_, CompileUndefined::Var) => format!("{found} is not defined"),
        (_, CompileUndefined::Label) => {
            let stem = found.strip_prefix('$').unwrap_or(found);
            format!("$*label-{stem} is not defined")
        }
        (_, CompileUndefined::ObjKey(kind)) => {
            format!("Cannot use {kind} ({found}) as object key")
        }
        (_, CompileUndefined::Filter(_arity)) => format!("undefined filter"),
        (_, CompileUndefined::Mod) => format!("undefined module"),
        (_, _) => format!("undefined symbol {found}"),
    }
}

fn format_first_load_error(errs: core_load::Errors<&str, PathBuf>) -> String {
    let Some((file, err)) = errs.into_iter().next() else {
        return "unknown load error".to_string();
    };
    match err {
        core_load::Error::Io(errs) => {
            let Some((path, err)) = errs.into_iter().next() else {
                return "io error".to_string();
            };
            format!("could not load file {path}: {err}")
        }
        core_load::Error::Lex(errs) => {
            let Some((expected, found)) = errs.into_iter().next() else {
                return "lex error".to_string();
            };
            let message = format_lex_message(&expected, found);
            let loc_slice = if matches!(expected, core_lex::Expect::Escape) {
                let off = offset_in(file.code, found);
                let back = off.saturating_sub(1);
                &file.code[back..]
            } else if let core_lex::Expect::Delim(open) = expected {
                let off = offset_in(file.code, open);
                &file.code[off..]
            } else {
                found
            };
            with_file_loc(message, &file, loc_slice)
        }
        core_load::Error::Parse(errs) => {
            let Some((expected, found)) = errs.into_iter().next() else {
                return "parse error".to_string();
            };
            let message = format_parse_message(&expected, found);
            let loc_slice = if message == "May need parentheses around object key expression" {
                let mut off = offset_in(file.code, found);
                while off > 0 {
                    let prev = file.code[..off].chars().next_back().unwrap_or(' ');
                    if prev == '{' || prev == ',' || prev == '(' || prev == ':' {
                        break;
                    }
                    off -= prev.len_utf8();
                }
                &file.code[off..]
            } else {
                found
            };
            with_file_loc(message, &file, loc_slice)
        }
    }
}

fn format_first_compile_error(errs: core_load::Errors<&str, PathBuf, Vec<(&str, CompileUndefined)>>) -> String {
    let Some((file, errs)) = errs.into_iter().next() else {
        return "compile error".to_string();
    };
    let Some((found, undefined)) = errs.into_iter().next() else {
        return "compile error".to_string();
    };
    let message = format_compile_message(found, &undefined);
    let loc_slice = if matches!(undefined, CompileUndefined::Label) {
        let off = offset_in(file.code, found);
        if let Some(pos) = file.code[..off].rfind("break") {
            &file.code[pos..]
        } else {
            found
        }
    } else {
        found
    };
    with_file_loc(message, &file, loc_slice)
}

fn precheck_query_error(query: &str, paths: &[PathBuf]) -> Option<String> {
    let arena = Arena::default();
    let loader = Loader::new(jaq_all::defs()).with_std_read(paths);
    let modules = match loader.load(
        &arena,
        File {
            path: PathBuf::from("<inline>"),
            code: query,
        },
    ) {
        Ok(modules) => modules,
        Err(errs) => return Some(format_first_load_error(errs)),
    };

    match Compiler::default()
        .with_funs(data::funs())
        .with_global_vars([MODULEMETA_GLOBAL])
        .compile(modules)
    {
        Ok(_) => None,
        Err(errs) => Some(format_first_compile_error(errs)),
    }
}

fn parse_jsonish_value(input: &str) -> Result<JaqValue, Error> {
    let canonical = canonicalize_jsonish_tokens(input);
    jaq_json_read::parse_single(canonical.as_bytes())
        .map_err(|e| Error::Unsupported(format!("json: {e}")))
}

fn stringify_jsonish_value(value: &JaqValue) -> Result<String, Error> {
    Ok(String::from_utf8_lossy(&value.to_json()).to_string())
}

fn json_to_jaq(value: JsonValue) -> Result<JaqValue, Error> {
    let encoded = serde_json::to_vec(&value)?;
    jaq_json_read::parse_single(&encoded)
        .map_err(|e| Error::Unsupported(format!("json conversion failed: {e}")))
}

fn jaq_to_json(value: &JaqValue) -> Result<JsonValue, Error> {
    let encoded = stringify_jsonish_value(value)?;
    serde_json::from_str(&encoded)
        .map_err(|_| Error::Unsupported(format!("result is not valid JSON: {encoded}")))
}

fn canonicalize_jsonish_tokens(input: &str) -> String {
    fn is_token_boundary(ch: Option<char>) -> bool {
        match ch {
            None => true,
            Some(c) => !(c.is_ascii_alphanumeric() || c == '_' || c == '.'),
        }
    }
    fn starts_with_ci(rest: &[char], pat: &str) -> bool {
        if rest.len() < pat.len() {
            return false;
        }
        rest.iter()
            .zip(pat.chars())
            .all(|(l, r)| l.eq_ignore_ascii_case(&r))
    }
    fn match_special(rest: &[char]) -> Option<(&'static str, usize)> {
        if starts_with_ci(rest, "nan") {
            return Some(("NaN", 3));
        }
        if starts_with_ci(rest, "infinity") {
            return Some(("Infinity", 8));
        }
        if starts_with_ci(rest, "infinite") {
            return Some(("Infinity", 8));
        }
        None
    }

    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while i < chars.len() {
        let c = chars[i];

        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if c == '"' {
            in_string = true;
            out.push(c);
            i += 1;
            continue;
        }

        let rest = &chars[i..];
        let prev = i.checked_sub(1).and_then(|p| chars.get(p)).copied();
        if is_token_boundary(prev) {
            if let Some(sign @ ('+' | '-')) = rest.first().copied() {
                if let Some((canon, len)) = match_special(&rest[1..]) {
                    let next = chars.get(i + 1 + len).copied();
                    if is_token_boundary(next) {
                        if canon == "Infinity" && sign == '-' {
                            out.push_str("-Infinity");
                        } else {
                            out.push_str(canon);
                        }
                        i += 1 + len;
                        continue;
                    }
                }
            } else if let Some((canon, len)) = match_special(rest) {
                let next = chars.get(i + len).copied();
                if is_token_boundary(next) {
                    out.push_str(canon);
                    i += len;
                    continue;
                }
            }
        }

        out.push(c);
        i += 1;
    }
    out
}

fn preprocess_query(query: &str) -> String {
    fn is_ident_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '_'
    }
    fn token_boundary(ch: Option<char>) -> bool {
        match ch {
            None => true,
            Some(c) => !is_ident_char(c),
        }
    }
    fn numeric_prefix_boundary(ch: Option<char>) -> bool {
        match ch {
            None => true,
            Some(c) => {
                matches!(
                    c,
                    ' ' | '\t'
                        | '\n'
                        | '\r'
                        | '('
                        | '['
                        | '{'
                        | ','
                        | ';'
                        | ':'
                        | '+'
                        | '-'
                        | '*'
                        | '/'
                        | '%'
                        | '<'
                        | '>'
                        | '='
                        | '!'
                        | '|'
                )
            }
        }
    }

    const LOC_EXPR: &str = "({\"file\":\"<top-level>\",\"line\":1})";
    const LOC_FIELD_EXPR: &str = "\"__loc__\":({\"file\":\"<top-level>\",\"line\":1})";

    let query = query
        .replace(
            "\\($__loc__)",
            "\\(({\"file\":\"<top-level>\",\"line\":1}))",
        )
        .replace("\\(__loc__)", "\\(({\"file\":\"<top-level>\",\"line\":1}))");

    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len());
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if c == '"' {
            in_string = true;
            out.push(c);
            i += 1;
            continue;
        }

        let prev = i.checked_sub(1).and_then(|p| chars.get(p)).copied();
        let next = chars.get(i + 1).copied();
        let prev_non_ws = {
            let mut p = i;
            let mut out = None;
            while p > 0 {
                p -= 1;
                let c = chars[p];
                if !c.is_whitespace() {
                    out = Some(c);
                    break;
                }
            }
            out
        };
        let next_non_ws = |from: usize| {
            let mut p = from;
            while p < chars.len() {
                let c = chars[p];
                if !c.is_whitespace() {
                    return Some(c);
                }
                p += 1;
            }
            None
        };

        if chars[i..].starts_with(&['$', '_', '_', 'l', 'o', 'c', '_', '_'])
            && token_boundary(prev)
            && token_boundary(chars.get(i + 8).copied())
        {
            if matches!(prev_non_ws, None | Some('{') | Some(','))
                && matches!(next_non_ws(i + 8), Some('}') | Some(','))
            {
                out.push_str(LOC_FIELD_EXPR);
            } else {
                out.push_str(LOC_EXPR);
            }
            i += 8;
            continue;
        }
        if chars[i..].starts_with(&['_', '_', 'l', 'o', 'c', '_', '_'])
            && token_boundary(prev)
            && token_boundary(chars.get(i + 7).copied())
        {
            out.push_str(LOC_EXPR);
            i += 7;
            continue;
        }
        if chars[i..].starts_with(&['$', 'E', 'N', 'V'])
            && token_boundary(prev)
            && token_boundary(chars.get(i + 4).copied())
        {
            out.push_str("env");
            i += 4;
            continue;
        }

        if c == '.' && next.is_some_and(|d| d.is_ascii_digit()) && numeric_prefix_boundary(prev) {
            out.push_str("0.");
            i += 1;
            continue;
        }

        out.push(c);
        i += 1;
    }

    out
}

fn inject_prelude_after_module_directives(query: &str) -> String {
    let insert_at = leading_module_directives_len(query);
    if insert_at == 0 {
        return format!("{JQ_PRELUDE}\n{query}");
    }

    let (head, tail) = query.split_at(insert_at);
    let mut out = String::with_capacity(query.len() + JQ_PRELUDE.len() + 2);
    out.push_str(head);
    if !head.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(JQ_PRELUDE);
    if !tail.is_empty() && !tail.starts_with('\n') {
        out.push('\n');
    }
    out.push_str(tail);
    out
}

fn leading_module_directives_len(query: &str) -> usize {
    let mut cursor = 0usize;
    let mut saw_module_directive = false;

    loop {
        let stmt_start = skip_whitespace_and_comments(query, cursor);
        if stmt_start >= query.len() {
            return if saw_module_directive { query.len() } else { 0 };
        }

        let (stmt_end, has_semicolon) = find_statement_end(query, stmt_start);
        let statement = query[stmt_start..stmt_end].trim();

        if !is_module_directive(statement) {
            return if saw_module_directive { stmt_start } else { 0 };
        }

        saw_module_directive = true;
        cursor = if has_semicolon {
            stmt_end + 1
        } else {
            stmt_end
        };
    }
}

fn skip_whitespace_and_comments(query: &str, mut idx: usize) -> usize {
    while idx < query.len() {
        let mut chars = query[idx..].chars();
        let Some(ch) = chars.next() else { break };
        if ch.is_whitespace() {
            idx += ch.len_utf8();
            continue;
        }
        if ch == '#' {
            idx += ch.len_utf8();
            while idx < query.len() {
                let mut comment_chars = query[idx..].chars();
                let Some(comment_ch) = comment_chars.next() else {
                    break;
                };
                idx += comment_ch.len_utf8();
                if comment_ch == '\n' {
                    break;
                }
            }
            continue;
        }
        break;
    }
    idx
}

fn find_statement_end(query: &str, start: usize) -> (usize, bool) {
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;

    for (offset, ch) in query[start..].char_indices() {
        let idx = start + offset;
        if in_comment {
            if ch == '\n' {
                in_comment = false;
            }
            continue;
        }

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '#' => in_comment = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.saturating_sub(1),
            ';' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                return (idx, true);
            }
            _ => {}
        }
    }

    (query.len(), false)
}

fn is_module_directive(statement: &str) -> bool {
    let s = statement.trim_start();
    s.starts_with("import ") || s.starts_with("include ") || s.starts_with("module ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn run_query_stream_basic() {
        let out = run_query_stream(".a", vec![serde_json::json!({"a": 1, "b": 2})])
            .expect("query should run");
        assert_eq!(out, vec![serde_json::json!(1)]);
    }

    #[test]
    fn normalize_jsonish_keeps_decimal_style() {
        let value = normalize_jsonish_line("2.0").expect("normalize");
        assert_eq!(value, "2.0");
    }

    #[test]
    fn parse_input_docs_supports_yaml_stream() {
        let docs = parse_input_docs_prefer_json("a: 1\n---\na: 2\n").expect("yaml docs");
        assert_eq!(
            docs,
            vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
        );
    }

    #[test]
    fn parse_input_docs_supports_json_stream() {
        let docs = parse_input_docs_prefer_json("{\"a\":1}\n{\"a\":2}\n").expect("json stream");
        assert_eq!(
            docs,
            vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
        );
    }

    #[test]
    fn parse_input_values_auto_reports_source_kind() {
        let json = parse_input_values_auto("{\"a\":1}\n{\"a\":2}\n").expect("json stream");
        assert_eq!(json.kind, InputKind::JsonStream);
        assert_eq!(json.values.len(), 2);

        let yaml = parse_input_values_auto("a: 1\n---\na: 2\n").expect("yaml docs");
        assert_eq!(yaml.kind, InputKind::YamlDocs);
        assert_eq!(yaml.values.len(), 2);
    }

    #[test]
    fn prelude_index_and_in_are_available() {
        let indexed = run_query_stream(
            "INDEX(.id)",
            vec![serde_json::json!([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])],
        )
        .expect("index query");
        assert_eq!(
            indexed,
            vec![serde_json::json!({
                "1": {"id": 1, "name": "a"},
                "2": {"id": 2, "name": "b"}
            })]
        );
    }

    #[test]
    fn canonicalizes_non_json_numeric_literals() {
        let line = r#"["nan",-nan,+nan,infinite,-infinite,"nan"]"#;
        let normalized = normalize_jsonish_line(line).expect("normalize");
        assert_eq!(normalized, r#"["nan",NaN,NaN,Infinity,-Infinity,"nan"]"#);
    }

    #[test]
    fn canonicalizes_case_insensitive_non_json_literals() {
        let line = r#"[NaN,-NaN,+NaN,INFINITE,-INFINITE,Infinity,-Infinity]"#;
        let normalized = normalize_jsonish_line(line).expect("normalize");
        assert_eq!(
            normalized,
            r#"[NaN,NaN,NaN,Infinity,-Infinity,Infinity,-Infinity]"#
        );
    }

    #[test]
    fn canonicalize_jsonish_preserves_string_content() {
        let line = r#"{"expr":"?//","query":"map(try .a[] catch ., .a[]?)","x":nan}"#;
        let normalized = normalize_jsonish_line(line).expect("normalize");
        assert_eq!(
            normalized,
            r#"{"expr":"?//","query":"map(try .a[] catch ., .a[]?)","x":NaN}"#
        );
    }

    #[test]
    fn preprocesses_loc_and_decimal_literals() {
        let query = r#"{ $__loc__, x: .0005, y: __loc__ }"#;
        let pre = preprocess_query(query);
        assert!(pre.contains("{\"file\":\"<top-level>\",\"line\":1}"));
        assert!(pre.contains("\"__loc__\":({\"file\":\"<top-level>\",\"line\":1})"));
        assert!(pre.contains("0.0005"));
        assert!(!pre.contains("$__loc__"));
        assert!(!pre.contains(" __loc__ "));
    }

    #[test]
    fn preprocesses_loc_in_string_interpolation() {
        let query = r#"try error("\($__loc__)") catch ."#;
        let pre = preprocess_query(query);
        assert!(pre.contains(r#"\(({"file":"<top-level>","line":1}))"#));
        assert!(!pre.contains("$__loc__"));
    }

    #[test]
    fn preprocesses_env_variable_to_builtin_env() {
        let query = "$ENV.PAGER, $ENV";
        let pre = preprocess_query(query);
        assert_eq!(pre, "env.PAGER, env");
    }

    #[test]
    fn prelude_is_injected_after_imports() {
        let query = r#"import "a" as foo; foo::a"#;
        let wrapped = inject_prelude_after_module_directives(query);
        assert!(wrapped.starts_with(r#"import "a" as foo;"#));
        assert!(wrapped.contains("jq-compat prelude"));
        assert!(wrapped.ends_with("foo::a"));
    }

    #[test]
    fn prelude_is_injected_for_plain_queries() {
        let wrapped = inject_prelude_after_module_directives(".foo");
        assert!(wrapped.starts_with("# jq-compat prelude"));
        assert!(wrapped.ends_with(".foo"));
    }

    #[test]
    fn imports_module_from_name_directory_fallback() {
        let td = tempfile::tempdir().expect("tempdir");
        let modules = td.path().join("modules");
        let bdir = modules.join("b");
        fs::create_dir_all(&bdir).expect("create module dir");
        fs::write(bdir.join("b.jq"), "def a: \"b\";\n").expect("write module");

        let out = run_query_stream_with_paths(
            r#"import "b" as b; b::a"#,
            vec![serde_json::json!(null)],
            &[modules.to_string_lossy().to_string()],
        )
        .expect("run import query");

        assert_eq!(out, vec![serde_json::json!("b")]);
    }

    #[test]
    fn supports_namespaced_data_import_variable_alias() {
        let td = tempfile::tempdir().expect("tempdir");
        let modules = td.path().join("modules");
        fs::create_dir_all(&modules).expect("create modules dir");
        fs::write(
            modules.join("data.json"),
            r#"{"this":"is a test","that":"is too"}"#,
        )
        .expect("write data import");

        let out = run_query_stream_with_paths(
            r#"import "data" as $d; [$d[0].this, $d::d[0].that]"#,
            vec![serde_json::json!(null)],
            &[modules.to_string_lossy().to_string()],
        )
        .expect("run data import query");

        assert_eq!(
            out,
            vec![serde_json::json!(["is a test", "is too"])]
        );
    }
}
