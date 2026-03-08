#![allow(dead_code)]

pub(crate) mod ast;
pub(crate) mod ir;
pub(crate) mod lexer;
pub(crate) mod parser;
pub(crate) mod vm;

use crate::value::ZqValue;

fn canonicalize_module_search_dirs(
    module_search_dirs: Vec<std::path::PathBuf>,
) -> Vec<std::path::PathBuf> {
    let mut out = Vec::with_capacity(module_search_dirs.len());
    for dir in module_search_dirs {
        let normalized = std::fs::canonicalize(&dir).unwrap_or(dir);
        if !out.iter().any(|seen| seen == &normalized) {
            out.push(normalized);
        }
    }
    out
}

pub(crate) fn compile(query: &str) -> Result<ir::Program, String> {
    let ast = parser::parse_query(query)?;
    let mut program = ir::compile(&ast);
    program.module_search_dirs = parser::default_module_search_dirs();
    Ok(program)
}

pub(crate) fn compile_with_module_dirs(
    query: &str,
    module_search_dirs: Vec<std::path::PathBuf>,
) -> Result<ir::Program, String> {
    let module_search_dirs = canonicalize_module_search_dirs(module_search_dirs);
    let ast = parser::parse_query_with_module_dirs(query, module_search_dirs.clone())?;
    let mut program = ir::compile(&ast);
    program.module_search_dirs = module_search_dirs;
    Ok(program)
}

pub(crate) fn execute(program: &ir::Program, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    vm::execute(program, input)
}

pub(crate) fn execute_prepared(
    program: &ir::Program,
    input: ZqValue,
) -> Result<Vec<ZqValue>, String> {
    vm::execute_prepared(program, input)
}

pub(crate) fn install_program_context(program: &ir::Program) -> vm::ProgramContextGuard {
    vm::install_program_context(program)
}

pub(crate) fn install_input_stream(inputs: &[ZqValue]) -> vm::InputStateGuard {
    vm::install_input_stream(inputs)
}

pub(crate) fn set_input_cursor(cursor: usize) {
    vm::set_input_cursor(cursor);
}

pub(crate) fn decode_halt_error(err: &str) -> Option<(i32, String)> {
    vm::decode_halt_error(err)
}

#[cfg(test)]
mod tests;
