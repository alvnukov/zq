use super::ast::{BinaryOp, Builtin, MathBinaryOp, MathTernaryOp};
use super::ir::{
    Op, OpBindingKeySpec, OpBindingPattern, OpObjectBindingEntry, OpObjectKey, Program,
    ProgramFunction,
};
use super::parser;
use crate::c_compat::{
    container as c_container, json as c_json, math as c_math, string as c_string, time as c_time,
    value as c_value,
};
use crate::value::ZqValue;
use fancy_regex::{Captures as FancyCaptures, Regex};
use indexmap::IndexMap;
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

mod builtins;
mod path_ops;
mod regex_ops;
mod stream_ops;

use self::builtins::{
    run_builtin, run_env, run_join, run_length, run_math_binary, run_math_ternary, run_strftime,
    run_strptime,
};
use self::path_ops::{
    run_delpaths, run_getpath, run_modify, run_path, run_paths_builtin, run_setpath,
};
#[cfg(test)]
use self::regex_ops::{normalize_named_capture_syntax, regex_has_match, RegexModeConfig};
use self::regex_ops::{
    run_regex_capture, run_regex_match, run_regex_scan, run_regex_splits, run_regex_sub,
    CachedRegex,
};
use self::stream_ops::{run_fromstream, run_tostream, run_truncate_stream};
use serde::Deserialize;

thread_local! {
    static BINDING_STACK: RefCell<Vec<(String, ZqValue)>> = const { RefCell::new(Vec::new()) };
    static FUNCTION_TABLE: RefCell<Vec<ProgramFunction>> = const { RefCell::new(Vec::new()) };
    static CALL_FRAME_STACK: RefCell<Vec<CallFrame>> = const { RefCell::new(Vec::new()) };
    static MODULE_SEARCH_DIRS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
    static INPUT_STATE: RefCell<InputState> = RefCell::new(InputState::default());
    static APPLY_OP_DEPTH: RefCell<usize> = const { RefCell::new(0) };
    static ROOT_APPLY_CONTEXT: RefCell<usize> = const { RefCell::new(0) };
    static REGEX_CACHE: RefCell<HashMap<String, CachedRegex>> = RefCell::new(HashMap::new());
    static VALUE_VEC_POOL: RefCell<Vec<Vec<ZqValue>>> = const { RefCell::new(Vec::new()) };
}

static NEXT_LABEL_ID: AtomicU64 = AtomicU64::new(1);
const THROWN_VALUE_PREFIX: &str = "\u{1f}zq-throw:";
const HALT_VALUE_PREFIX: &str = "\u{1f}zq-halt:";
const REGEX_CACHE_LIMIT: usize = 1024;
const VALUE_VEC_POOL_LIMIT: usize = 64;
const VALUE_VEC_RETAIN_CAP: usize = 4096;

type JsonInputReader = Box<dyn std::io::Read + Send>;
type JsonInputParser = serde_json::Deserializer<serde_json::de::IoRead<JsonInputReader>>;

struct BindingGuard {
    depth_added: usize,
}

#[derive(Clone)]
struct CallFrame {
    params: Vec<(usize, CapturedFilter)>,
}

#[derive(Clone)]
struct CapturedFilter {
    op: Op,
    bindings: Vec<(String, ZqValue)>,
}

struct CallFrameGuard {
    pushed: bool,
}

struct ApplyOpDepthGuard;

struct RootApplyContextGuard;

struct PooledValueVec {
    inner: Vec<ZqValue>,
}

impl PooledValueVec {
    fn acquire() -> Self {
        let inner = VALUE_VEC_POOL
            .with(|pool| pool.borrow_mut().pop())
            .unwrap_or_default();
        Self { inner }
    }

    fn into_vec(mut self) -> Vec<ZqValue> {
        std::mem::take(&mut self.inner)
    }
}

impl Deref for PooledValueVec {
    type Target = Vec<ZqValue>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledValueVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledValueVec {
    fn drop(&mut self) {
        if self.inner.capacity() > VALUE_VEC_RETAIN_CAP {
            return;
        }
        self.inner.clear();
        let mut returned = Vec::new();
        std::mem::swap(&mut returned, &mut self.inner);
        VALUE_VEC_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < VALUE_VEC_POOL_LIMIT {
                pool.push(returned);
            }
        });
    }
}

impl Drop for BindingGuard {
    fn drop(&mut self) {
        if self.depth_added == 0 {
            return;
        }
        BINDING_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            let keep_len = stack.len().saturating_sub(self.depth_added);
            stack.truncate(keep_len);
        });
    }
}

impl Drop for CallFrameGuard {
    fn drop(&mut self) {
        if !self.pushed {
            return;
        }
        CALL_FRAME_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            let _ = stack.pop();
        });
    }
}

impl Drop for ApplyOpDepthGuard {
    fn drop(&mut self) {
        APPLY_OP_DEPTH.with(|depth| {
            let mut depth = depth.borrow_mut();
            *depth = depth.saturating_sub(1);
        });
    }
}

impl Drop for RootApplyContextGuard {
    fn drop(&mut self) {
        ROOT_APPLY_CONTEXT.with(|depth| {
            let mut depth = depth.borrow_mut();
            *depth = depth.saturating_sub(1);
        });
    }
}

struct FunctionTableGuard {
    previous: Vec<ProgramFunction>,
}

struct ModuleSearchDirsGuard {
    previous: Vec<PathBuf>,
}

pub(crate) struct ProgramContextGuard {
    _function_table_guard: FunctionTableGuard,
    _module_search_dirs_guard: ModuleSearchDirsGuard,
}

#[derive(Default)]
struct InputState {
    source: InputSource,
    cursor: usize,
    has_stream_context: bool,
}

#[derive(Default)]
enum InputSource {
    #[default]
    None,
    Buffered(Vec<ZqValue>),
    Stream(JsonTextInputSource),
}

struct JsonTextInputSource {
    replay: VecDeque<ZqValue>,
    parser: JsonInputParser,
}

impl JsonTextInputSource {
    fn from_reader(reader: JsonInputReader, replay: Vec<ZqValue>) -> Self {
        let parser = serde_json::Deserializer::from_reader(reader);
        Self {
            replay: replay.into(),
            parser,
        }
    }

    fn from_json_text(input: &str, replay: Vec<ZqValue>) -> Self {
        let cursor = std::io::Cursor::new(input.as_bytes().to_vec());
        Self::from_reader(Box::new(cursor), replay)
    }
}

impl Default for JsonTextInputSource {
    fn default() -> Self {
        Self::from_json_text("", Vec::new())
    }
}

pub(crate) struct InputStateGuard {
    previous: InputState,
}

impl Drop for FunctionTableGuard {
    fn drop(&mut self) {
        FUNCTION_TABLE.with(|table| {
            let mut table = table.borrow_mut();
            *table = std::mem::take(&mut self.previous);
        });
    }
}

impl Drop for ModuleSearchDirsGuard {
    fn drop(&mut self) {
        MODULE_SEARCH_DIRS.with(|dirs| {
            let mut dirs = dirs.borrow_mut();
            *dirs = std::mem::take(&mut self.previous);
        });
    }
}

impl Drop for InputStateGuard {
    fn drop(&mut self) {
        INPUT_STATE.with(|state| {
            let mut state = state.borrow_mut();
            *state = std::mem::take(&mut self.previous);
        });
    }
}

pub(crate) fn execute(program: &Program, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    let _program_context_guard = install_program_context(program);
    execute_prepared(program, input.clone())
}

pub(crate) fn install_program_context(program: &Program) -> ProgramContextGuard {
    ProgramContextGuard {
        _function_table_guard: install_function_table(&program.functions),
        _module_search_dirs_guard: install_module_search_dirs(&program.module_search_dirs),
    }
}

pub(crate) fn execute_prepared(program: &Program, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    let mut out = Vec::new();
    execute_prepared_with(program, input, &mut |value| {
        out.push(value);
        Ok(())
    })?;
    Ok(out)
}

pub(crate) fn execute_prepared_with<F>(
    program: &Program,
    input: ZqValue,
    emit: &mut F,
) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    if let Some((last_branch, head_branches)) = program.branches.split_last() {
        for branch in head_branches {
            execute_branch(branch, input.clone(), emit)?;
        }
        execute_branch(last_branch, input, emit)?;
    }
    Ok(())
}

fn execute_branch<F>(branch: &super::ir::Branch, input: ZqValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let mut current = PooledValueVec::acquire();
    let mut next = PooledValueVec::acquire();
    current.push(input);
    for (op_index, op) in branch.ops.iter().enumerate() {
        let is_last_op = op_index + 1 == branch.ops.len();
        if is_last_op {
            let mut hard_error: Option<String> = None;
            for value in current.drain(..) {
                let _root_apply_guard = push_root_apply_context();
                let mut emitted = false;
                let mut tracked_emit = |produced: ZqValue| {
                    emitted = true;
                    emit(produced)
                };
                if let Err(err) = apply_op_terminal(op, value, &mut tracked_emit) {
                    if !emitted {
                        hard_error = Some(render_public_error(err));
                    }
                    // Preserve outputs produced before terminal failure.
                    break;
                }
            }
            if let Some(err) = hard_error {
                return Err(err);
            }
            return Ok(());
        }

        next.clear();
        let mut hard_error: Option<String> = None;
        for value in current.drain(..) {
            let _root_apply_guard = push_root_apply_context();
            if let Err(err) = apply_op(op, value, &mut next) {
                hard_error = Some(render_public_error(err));
                break;
            }
        }
        if let Some(err) = hard_error {
            return Err(err);
        }
        std::mem::swap(current.deref_mut(), next.deref_mut());
    }
    for value in current.drain(..) {
        emit(value)?;
    }
    Ok(())
}

fn apply_op_terminal<F>(op: &Op, input: ZqValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    // jq_next() yields top-level results one-by-one while continuing execution
    // through backtracking points. The terminal path mirrors that style here:
    // emit as soon as values appear instead of accumulating full stage output.
    match op {
        Op::Repeat(arg) => {
            let _apply_depth_guard = push_apply_op_depth();
            run_repeat_terminal(arg, input, emit)
        }
        Op::Pipe(stages) => {
            let _apply_depth_guard = push_apply_op_depth();
            apply_pipe_stages_terminal(stages, input, emit)
        }
        Op::Chain(steps) => {
            let _apply_depth_guard = push_apply_op_depth();
            let root = input.clone();
            apply_chain_steps_terminal(steps, &root, input, emit)
        }
        Op::Comma(items) => {
            let _apply_depth_guard = push_apply_op_depth();
            for item in items {
                apply_op_terminal(item, input.clone(), emit)?;
            }
            Ok(())
        }
        Op::Call {
            function_id,
            param_id,
            name,
            args,
        } => {
            let _apply_depth_guard = push_apply_op_depth();
            let arity = args.len();
            if function_id.is_none() {
                if let Some(arg_filter) = param_id.and_then(|id| lookup_param_closure(id, arity)) {
                    let _guard = push_bindings(arg_filter.bindings.clone());
                    let mut captured_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(&arg_filter.op, &input, &mut captured_values)?;
                    for value in captured_values.drain(..) {
                        emit(value)?;
                    }
                    return Ok(());
                }
                return Err(format!("{name}/{arity} is not defined"));
            }
            let Some(function) = lookup_function_by_id(function_id.expect("checked above")) else {
                return Err(format!("{name}/{arity} is not defined"));
            };
            if function.param_ids.len() != arity {
                return Err(format!("{name}/{arity} is not defined"));
            }
            let captured_args: Vec<CapturedFilter> =
                args.iter().map(capture_call_argument).collect();
            let frame = CallFrame {
                params: function.param_ids.into_iter().zip(captured_args).collect(),
            };
            let _call_frame_guard = push_call_frame(frame);
            let mut body_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(&function.body, &input, &mut body_values)?;
            for value in body_values.drain(..) {
                emit(value)?;
            }
            Ok(())
        }
        Op::Label { name, body } => {
            let _apply_depth_guard = push_apply_op_depth();
            let label_id = NEXT_LABEL_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let marker = format!("\u{1f}zq-label:{name}:{label_id}");
            let label_var = format!("*label-{name}");
            let _guard = push_bindings(vec![(label_var, ZqValue::String(marker.clone()))]);
            match apply_op_terminal(body, input, emit) {
                Ok(()) => Ok(()),
                Err(err) if err == marker => Ok(()),
                Err(err) => Err(err),
            }
        }
        Op::TryCatch { inner, catcher } => {
            let _apply_depth_guard = push_apply_op_depth();
            let mut inner_values = PooledValueVec::acquire();
            match apply_op_with_borrowed_fast_path(inner, &input, &mut inner_values) {
                Ok(()) => {
                    for value in inner_values.drain(..) {
                        emit(value)?;
                    }
                    Ok(())
                }
                Err(err) => {
                    for value in inner_values.drain(..) {
                        emit(value)?;
                    }
                    if decode_halt_error(&err).is_some() {
                        return Err(err);
                    }
                    let catch_input =
                        decode_thrown_value(&err).unwrap_or_else(|| ZqValue::String(err));
                    apply_op_terminal(catcher, catch_input, emit)
                }
            }
        }
        _ => {
            let mut stage_out = PooledValueVec::acquire();
            let stage_err = apply_op(op, input, &mut stage_out).err();
            for value in stage_out.drain(..) {
                emit(value)?;
            }
            if let Some(err) = stage_err {
                return Err(err);
            }
            Ok(())
        }
    }
}

fn push_apply_op_depth() -> ApplyOpDepthGuard {
    APPLY_OP_DEPTH.with(|depth| {
        let mut depth = depth.borrow_mut();
        *depth += 1;
    });
    ApplyOpDepthGuard
}

fn current_apply_op_depth() -> usize {
    APPLY_OP_DEPTH.with(|depth| *depth.borrow())
}

fn push_root_apply_context() -> RootApplyContextGuard {
    ROOT_APPLY_CONTEXT.with(|depth| {
        let mut depth = depth.borrow_mut();
        *depth += 1;
    });
    RootApplyContextGuard
}

fn in_root_apply_context() -> bool {
    ROOT_APPLY_CONTEXT.with(|depth| *depth.borrow() > 0)
}

fn push_bindings(bindings: Vec<(String, ZqValue)>) -> BindingGuard {
    let depth_added = bindings.len();
    if depth_added > 0 {
        BINDING_STACK.with(|stack| stack.borrow_mut().extend(bindings));
    }
    BindingGuard { depth_added }
}

fn push_call_frame(frame: CallFrame) -> CallFrameGuard {
    CALL_FRAME_STACK.with(|stack| stack.borrow_mut().push(frame));
    CallFrameGuard { pushed: true }
}

fn install_function_table(functions: &[ProgramFunction]) -> FunctionTableGuard {
    let previous = FUNCTION_TABLE.with(|table| {
        let mut table = table.borrow_mut();
        std::mem::replace(&mut *table, functions.to_vec())
    });
    FunctionTableGuard { previous }
}

fn install_module_search_dirs(dirs: &[PathBuf]) -> ModuleSearchDirsGuard {
    let previous = MODULE_SEARCH_DIRS.with(|search_dirs| {
        let mut search_dirs = search_dirs.borrow_mut();
        std::mem::replace(&mut *search_dirs, dirs.to_vec())
    });
    ModuleSearchDirsGuard { previous }
}

pub(crate) fn install_input_stream(inputs: &[ZqValue]) -> InputStateGuard {
    let previous = INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        std::mem::replace(
            &mut *state,
            InputState {
                source: InputSource::Buffered(inputs.to_vec()),
                cursor: 0,
                has_stream_context: !inputs.is_empty(),
            },
        )
    });
    InputStateGuard { previous }
}

pub(crate) fn install_input_stream_json_text(
    remaining_input: &str,
    replay: Vec<ZqValue>,
    has_stream_context: bool,
) -> InputStateGuard {
    let previous = INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        std::mem::replace(
            &mut *state,
            InputState {
                source: InputSource::Stream(JsonTextInputSource::from_json_text(
                    remaining_input,
                    replay,
                )),
                cursor: 0,
                has_stream_context,
            },
        )
    });
    InputStateGuard { previous }
}

pub(crate) fn install_input_stream_json_reader(
    reader: JsonInputReader,
    replay: Vec<ZqValue>,
    has_stream_context: bool,
) -> InputStateGuard {
    install_input_stream_json_parser(
        serde_json::Deserializer::from_reader(reader),
        replay,
        has_stream_context,
    )
}

pub(crate) fn install_input_stream_json_parser(
    parser: JsonInputParser,
    replay: Vec<ZqValue>,
    has_stream_context: bool,
) -> InputStateGuard {
    let previous = INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        std::mem::replace(
            &mut *state,
            InputState {
                source: InputSource::Stream(JsonTextInputSource {
                    replay: replay.into(),
                    parser,
                }),
                cursor: 0,
                has_stream_context,
            },
        )
    });
    InputStateGuard { previous }
}

pub(crate) fn install_input_metadata_context() -> InputStateGuard {
    let previous = INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        std::mem::replace(
            &mut *state,
            InputState {
                source: InputSource::None,
                cursor: 0,
                has_stream_context: true,
            },
        )
    });
    InputStateGuard { previous }
}

pub(crate) fn set_input_cursor(cursor: usize) {
    INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.cursor = match &state.source {
            InputSource::Buffered(values) => cursor.min(values.len()),
            _ => cursor,
        };
    });
}

fn next_input_value() -> Option<ZqValue> {
    INPUT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        let cursor = state.cursor;
        match &mut state.source {
            InputSource::None => None,
            InputSource::Buffered(values) => {
                let next = values.get(cursor).cloned();
                if next.is_some() {
                    state.cursor = cursor + 1;
                }
                next
            }
            InputSource::Stream(stream) => {
                let value = if let Some(replay) = stream.replay.pop_front() {
                    Some(replay)
                } else {
                    next_json_text_input_value(stream)
                };
                if value.is_some() {
                    state.cursor += 1;
                }
                value
            }
        }
    })
}

fn input_line_number_value() -> Option<i64> {
    INPUT_STATE.with(|state| {
        let state = state.borrow();
        if !state.has_stream_context {
            None
        } else {
            Some((state.cursor as i64) + 1)
        }
    })
}

fn next_json_text_input_value(stream: &mut JsonTextInputSource) -> Option<ZqValue> {
    match serde_json::Value::deserialize(&mut stream.parser) {
        Ok(value) => Some(ZqValue::from_json(value)),
        Err(err) if err.is_eof() => None,
        Err(_) => None,
    }
}

fn current_module_search_dirs() -> Vec<PathBuf> {
    MODULE_SEARCH_DIRS.with(|dirs| {
        let dirs = dirs.borrow();
        if dirs.is_empty() {
            parser::default_module_search_dirs()
        } else {
            dirs.clone()
        }
    })
}

fn lookup_binding(name: &str) -> Option<ZqValue> {
    let found = BINDING_STACK.with(|stack| {
        let stack = stack.borrow();
        stack
            .iter()
            .rev()
            .find_map(|(key, value)| (key == name).then(|| value.clone()))
    });
    if found.is_some() {
        return found;
    }
    if name == "ENV" {
        return run_env(ZqValue::Null).ok();
    }
    None
}

fn lookup_function_by_id(id: usize) -> Option<ProgramFunction> {
    FUNCTION_TABLE.with(|table| {
        let table = table.borrow();
        table
            .iter()
            .find_map(|function| (function.id == id).then(|| function.clone()))
    })
}

fn lookup_param_closure(param_id: usize, arity: usize) -> Option<CapturedFilter> {
    if arity != 0 {
        return None;
    }
    CALL_FRAME_STACK.with(|stack| {
        let stack = stack.borrow();
        stack.iter().rev().find_map(|frame| {
            frame
                .params
                .iter()
                .rev()
                .find_map(|(id, arg)| (*id == param_id).then(|| arg.clone()))
        })
    })
}

fn snapshot_bindings() -> Vec<(String, ZqValue)> {
    BINDING_STACK.with(|stack| stack.borrow().clone())
}

fn capture_call_argument(arg: &Op) -> CapturedFilter {
    match arg {
        Op::Call {
            function_id: None,
            param_id: Some(param_id),
            args,
            ..
        } if args.is_empty() => lookup_param_closure(*param_id, 0)
            .filter(|captured| {
                !matches!(
                    &captured.op,
                    Op::Call {
                        function_id: None,
                        param_id: Some(captured_id),
                        args,
                        ..
                    } if captured_id == param_id && args.is_empty()
                )
            })
            .unwrap_or_else(|| CapturedFilter {
                op: arg.clone(),
                bindings: snapshot_bindings(),
            }),
        _ => CapturedFilter {
            op: arg.clone(),
            bindings: snapshot_bindings(),
        },
    }
}

fn map_with_shared_input<T, F>(
    input: ZqValue,
    mut items: Vec<T>,
    mut f: F,
) -> Result<Vec<ZqValue>, String>
where
    F: FnMut(ZqValue, T) -> Result<ZqValue, String>,
{
    let mut out = Vec::with_capacity(items.len());
    if let Some(last) = items.pop() {
        for item in items {
            out.push(f(input.clone(), item)?);
        }
        out.push(f(input, last)?);
    }
    Ok(out)
}

enum BorrowedEvalSingle {
    Value(ZqValue),
    Empty,
    Unsupported,
}

fn eval_single_borrowed(op: &Op, input: &ZqValue) -> Result<BorrowedEvalSingle, String> {
    match op {
        Op::Literal(value) => Ok(BorrowedEvalSingle::Value(value.clone())),
        Op::Var(name) => {
            if let Some(value) = lookup_binding(name) {
                Ok(BorrowedEvalSingle::Value(value))
            } else {
                Err(format!("variable ${name} is not defined"))
            }
        }
        Op::GetField { name, optional } => {
            let result = match input {
                ZqValue::Object(map) => {
                    BorrowedEvalSingle::Value(map.get(name).cloned().unwrap_or(ZqValue::Null))
                }
                ZqValue::Null => BorrowedEvalSingle::Value(ZqValue::Null),
                other => {
                    if *optional {
                        BorrowedEvalSingle::Empty
                    } else {
                        return Err(format!(
                            "Cannot index {} with string {:?}",
                            type_name(other),
                            name
                        ));
                    }
                }
            };
            Ok(result)
        }
        Op::GetIndex { index, optional } => {
            let result = match input {
                ZqValue::Array(values) => BorrowedEvalSingle::Value(
                    c_string::normalize_index_jq(values.len(), *index)
                        .and_then(|idx| values.get(idx))
                        .cloned()
                        .unwrap_or(ZqValue::Null),
                ),
                ZqValue::String(s) => BorrowedEvalSingle::Value(
                    c_string::string_index_like_jq(s, *index).unwrap_or(ZqValue::Null),
                ),
                ZqValue::Null => BorrowedEvalSingle::Value(ZqValue::Null),
                other => {
                    if *optional {
                        BorrowedEvalSingle::Empty
                    } else {
                        return Err(format!("Cannot index {} with number", type_name(other)));
                    }
                }
            };
            Ok(result)
        }
        Op::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) =>
        {
            let lhs_value = match eval_single_borrowed(lhs, input)? {
                BorrowedEvalSingle::Value(value) => value,
                BorrowedEvalSingle::Empty => return Ok(BorrowedEvalSingle::Empty),
                BorrowedEvalSingle::Unsupported => return Ok(BorrowedEvalSingle::Unsupported),
            };
            let rhs_value = match eval_single_borrowed(rhs, input)? {
                BorrowedEvalSingle::Value(value) => value,
                BorrowedEvalSingle::Empty => return Ok(BorrowedEvalSingle::Empty),
                BorrowedEvalSingle::Unsupported => return Ok(BorrowedEvalSingle::Unsupported),
            };
            Ok(BorrowedEvalSingle::Value(apply_binary_with_flags(
                *op, lhs_value, rhs_value, false, false,
            )?))
        }
        _ => Ok(BorrowedEvalSingle::Unsupported),
    }
}

fn apply_op_with_borrowed_fast_path(
    op: &Op,
    input: &ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    match eval_single_borrowed(op, input)? {
        BorrowedEvalSingle::Value(value) => {
            out.push(value);
            Ok(())
        }
        BorrowedEvalSingle::Empty => Ok(()),
        BorrowedEvalSingle::Unsupported => apply_op(op, input.clone(), out),
    }
}

fn apply_op(op: &Op, input: ZqValue, out: &mut Vec<ZqValue>) -> Result<(), String> {
    let _apply_depth_guard = push_apply_op_depth();
    match op {
        Op::Identity => {
            out.push(input);
            Ok(())
        }
        Op::Chain(steps) => {
            let root = input.clone();
            apply_chain_steps(steps, &root, input, out)
        }
        Op::Pipe(stages) => apply_pipe_stages(stages, input, out),
        Op::Call {
            function_id,
            param_id,
            name,
            args,
        } => {
            let arity = args.len();
            if function_id.is_none() {
                if let Some(arg_filter) = param_id.and_then(|id| lookup_param_closure(id, arity)) {
                    let _guard = push_bindings(arg_filter.bindings.clone());
                    let mut captured_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(&arg_filter.op, &input, &mut captured_values)?;
                    out.extend(captured_values.drain(..));
                    return Ok(());
                }
                return Err(format!("{name}/{arity} is not defined"));
            }
            let Some(function) = lookup_function_by_id(function_id.expect("checked above")) else {
                return Err(format!("{name}/{arity} is not defined"));
            };
            if function.param_ids.len() != arity {
                return Err(format!("{name}/{arity} is not defined"));
            }
            let captured_args: Vec<CapturedFilter> =
                args.iter().map(capture_call_argument).collect();
            let frame = CallFrame {
                params: function.param_ids.into_iter().zip(captured_args).collect(),
            };
            let _call_frame_guard = push_call_frame(frame);
            let mut body_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(&function.body, &input, &mut body_values)?;
            out.extend(body_values.drain(..));
            Ok(())
        }
        Op::Var(name) => {
            if let Some(value) = lookup_binding(name) {
                out.push(value);
                Ok(())
            } else {
                Err(format!("variable ${name} is not defined"))
            }
        }
        Op::Label { name, body } => {
            let label_id = NEXT_LABEL_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let marker = format!("\u{1f}zq-label:{name}:{label_id}");
            let label_var = format!("*label-{name}");
            let _guard = push_bindings(vec![(label_var, ZqValue::String(marker.clone()))]);
            let mut body_out = PooledValueVec::acquire();
            match apply_op(body, input, &mut body_out) {
                Ok(()) => {
                    out.extend(body_out.drain(..));
                    Ok(())
                }
                Err(err) if err == marker => {
                    out.extend(body_out.drain(..));
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        Op::Break(name) => {
            let label_var = format!("*label-{name}");
            let Some(label_value) = lookup_binding(&label_var) else {
                return Err(format!("variable ${label_var} is not defined"));
            };
            let marker = match label_value {
                ZqValue::String(s) => s,
                other => c_json::tostring_value_jq(&other)?,
            };
            Err(marker)
        }
        Op::Literal(value) => {
            out.push(value.clone());
            Ok(())
        }
        Op::Comma(items) => {
            for item in items {
                let mut item_values = PooledValueVec::acquire();
                apply_op_with_borrowed_fast_path(item, &input, &mut item_values)?;
                out.extend(item_values.drain(..));
            }
            Ok(())
        }
        Op::ArrayLiteral(items) => {
            let mut values = PooledValueVec::acquire();
            for item in items {
                let mut item_values = PooledValueVec::acquire();
                apply_op_with_borrowed_fast_path(item, &input, &mut item_values)?;
                values.extend(item_values.drain(..));
            }
            out.push(ZqValue::Array(values.into_vec()));
            Ok(())
        }
        Op::ObjectLiteral(entries) => {
            // Fast path for the common projection form: `{a: .a, b: .b, ...}`.
            if !entries.is_empty()
                && entries
                    .iter()
                    .all(|(key_expr, _)| matches!(key_expr, OpObjectKey::Static(_)))
            {
                let mut object = IndexMap::with_capacity(entries.len());
                let mut fast_path = true;
                for (key_expr, value_expr) in entries {
                    let OpObjectKey::Static(key) = key_expr else {
                        unreachable!("checked above");
                    };
                    match eval_single_borrowed(value_expr, &input)? {
                        BorrowedEvalSingle::Value(value) => {
                            object.insert(key.clone(), value);
                        }
                        BorrowedEvalSingle::Empty | BorrowedEvalSingle::Unsupported => {
                            fast_path = false;
                            break;
                        }
                    }
                }
                if fast_path {
                    out.push(ZqValue::Object(object));
                    return Ok(());
                }
            }

            let mut prepared = Vec::with_capacity(entries.len());
            let mut single_output_candidate = !entries.is_empty();
            for (key_expr, value_expr) in entries {
                let keys: Vec<String> = match key_expr {
                    OpObjectKey::Static(name) => vec![name.clone()],
                    OpObjectKey::Expr(expr) => {
                        let mut keys = Vec::new();
                        let mut key_values = PooledValueVec::acquire();
                        apply_op_with_borrowed_fast_path(expr, &input, &mut key_values)?;
                        for key in key_values.drain(..) {
                            match key {
                                ZqValue::String(name) => keys.push(name),
                                other => {
                                    return Err(format!(
                                        "Cannot use {} as object key",
                                        type_name(&other)
                                    ))
                                }
                            }
                        }
                        keys
                    }
                };
                let mut value_stream = PooledValueVec::acquire();
                apply_op_with_borrowed_fast_path(value_expr, &input, &mut value_stream)?;
                let values = value_stream.into_vec();
                if keys.len() != 1 || values.len() != 1 {
                    single_output_candidate = false;
                }
                prepared.push((keys, values));
            }

            if single_output_candidate {
                let mut object = IndexMap::with_capacity(prepared.len());
                for (keys, mut values) in prepared {
                    let key = keys.into_iter().next().expect("single key");
                    let value = values.pop().expect("single value");
                    object.insert(key, value);
                }
                out.push(ZqValue::Object(object));
                return Ok(());
            }

            let mut objects = vec![IndexMap::new()];
            for (keys, values) in prepared {
                if keys.is_empty() || values.is_empty() {
                    objects.clear();
                    break;
                }

                let mut next_objects = Vec::with_capacity(
                    objects
                        .len()
                        .saturating_mul(keys.len())
                        .saturating_mul(values.len()),
                );
                for object in &objects {
                    for key in &keys {
                        for value in &values {
                            let mut next = object.clone();
                            next.insert(key.clone(), value.clone());
                            next_objects.push(next);
                        }
                    }
                }
                objects = next_objects;
            }

            out.reserve(objects.len());
            for object in objects {
                out.push(ZqValue::Object(object));
            }
            Ok(())
        }
        Op::Builtin(filter) => {
            out.push(run_builtin(*filter, input)?);
            Ok(())
        }
        Op::Has(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, keys, c_container::has_jq)?);
            Ok(())
        }
        Op::In(arg) => {
            let containers = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                containers,
                |key, container| c_container::has_jq(container, key),
            )?);
            Ok(())
        }
        Op::StartsWith(arg) => {
            let prefixes = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                prefixes,
                c_string::startswith_jq,
            )?);
            Ok(())
        }
        Op::EndsWith(arg) => {
            let suffixes = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                suffixes,
                c_string::endswith_jq,
            )?);
            Ok(())
        }
        Op::Split(arg) => {
            let separators = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                separators,
                c_string::split_jq,
            )?);
            Ok(())
        }
        Op::Join(arg) => {
            let separators = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, separators, run_join)?);
            Ok(())
        }
        Op::LTrimStr(arg) => {
            let patterns = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                patterns,
                c_string::ltrimstr_jq,
            )?);
            Ok(())
        }
        Op::RTrimStr(arg) => {
            let patterns = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                patterns,
                c_string::rtrimstr_jq,
            )?);
            Ok(())
        }
        Op::TrimStr(arg) => {
            let patterns = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                patterns,
                c_string::trimstr_jq,
            )?);
            Ok(())
        }
        Op::Indices(arg) => {
            let needles = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, needles, run_indices)?);
            Ok(())
        }
        Op::IndexOf(arg) => {
            let needles = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, needles, |value, needle| {
                let positions = run_indices(value, needle)?;
                jq_get_dynamic(positions, ZqValue::from(0))
            })?);
            Ok(())
        }
        Op::RIndexOf(arg) => {
            let needles = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, needles, |value, needle| {
                let positions = run_indices(value, needle)?;
                jq_get_dynamic(positions, ZqValue::from(-1))
            })?);
            Ok(())
        }
        Op::Contains(arg) => {
            let needles = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                needles,
                c_container::contains_jq,
            )?);
            Ok(())
        }
        Op::Inside(arg) => {
            let containers = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                containers,
                |value, container| c_container::contains_jq(container, value),
            )?);
            Ok(())
        }
        Op::BSearch(arg) => {
            let targets = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                targets,
                c_container::bsearch_jq,
            )?);
            Ok(())
        }
        Op::SortByImpl(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, keys, c_container::sort_by_jq)?);
            Ok(())
        }
        Op::GroupByImpl(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                keys,
                c_container::group_by_jq,
            )?);
            Ok(())
        }
        Op::UniqueByImpl(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(
                input,
                keys,
                c_container::unique_by_jq,
            )?);
            Ok(())
        }
        Op::MinByImpl(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, keys, |value, key| {
                c_container::minmax_by_jq(value, key, true)
            })?);
            Ok(())
        }
        Op::MaxByImpl(arg) => {
            let keys = eval_many(arg, &input)?;
            out.extend(map_with_shared_input(input, keys, |value, key| {
                c_container::minmax_by_jq(value, key, false)
            })?);
            Ok(())
        }
        Op::RegexMatch {
            spec,
            flags,
            test,
            tuple_mode,
        } => {
            let mut specs = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(spec, &input, &mut specs)?;
            let mut flag_values = PooledValueVec::acquire();
            if let Some(flags) = flags {
                apply_op_with_borrowed_fast_path(flags, &input, &mut flag_values)?;
            } else {
                flag_values.push(ZqValue::Null);
            }
            for spec_value in specs.drain(..) {
                for flag_value in flag_values.iter() {
                    out.extend(run_regex_match(
                        &input,
                        spec_value.clone(),
                        flags.as_ref().map(|_| flag_value.clone()),
                        *test,
                        *tuple_mode,
                    )?);
                }
            }
            Ok(())
        }
        Op::RegexCapture {
            spec,
            flags,
            tuple_mode,
        } => {
            let mut specs = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(spec, &input, &mut specs)?;
            let mut flag_values = PooledValueVec::acquire();
            if let Some(flags) = flags {
                apply_op_with_borrowed_fast_path(flags, &input, &mut flag_values)?;
            } else {
                flag_values.push(ZqValue::Null);
            }
            for spec_value in specs.drain(..) {
                for flag_value in flag_values.iter() {
                    out.extend(run_regex_capture(
                        &input,
                        spec_value.clone(),
                        flags.as_ref().map(|_| flag_value.clone()),
                        *tuple_mode,
                    )?);
                }
            }
            Ok(())
        }
        Op::RegexScan { regex, flags } => {
            let mut regex_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(regex, &input, &mut regex_values)?;
            let mut flag_values = PooledValueVec::acquire();
            if let Some(flags) = flags {
                apply_op_with_borrowed_fast_path(flags, &input, &mut flag_values)?;
            } else {
                flag_values.push(ZqValue::Null);
            }
            for regex_value in regex_values.drain(..) {
                for flag_value in flag_values.iter() {
                    out.extend(run_regex_scan(
                        &input,
                        regex_value.clone(),
                        flags.as_ref().map(|_| flag_value.clone()),
                    )?);
                }
            }
            Ok(())
        }
        Op::RegexSplits { regex, flags } => {
            let mut regex_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(regex, &input, &mut regex_values)?;
            let mut flag_values = PooledValueVec::acquire();
            if let Some(flags) = flags {
                apply_op_with_borrowed_fast_path(flags, &input, &mut flag_values)?;
            } else {
                flag_values.push(ZqValue::Null);
            }
            for regex_value in regex_values.drain(..) {
                for flag_value in flag_values.iter() {
                    out.extend(run_regex_splits(
                        &input,
                        regex_value.clone(),
                        flags.as_ref().map(|_| flag_value.clone()),
                    )?);
                }
            }
            Ok(())
        }
        Op::RegexSub {
            regex,
            replacement,
            flags,
            global,
        } => {
            let mut regex_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(regex, &input, &mut regex_values)?;
            let mut flag_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(flags, &input, &mut flag_values)?;
            for regex_value in regex_values.drain(..) {
                for flag_value in flag_values.iter() {
                    out.extend(run_regex_sub(
                        &input,
                        regex_value.clone(),
                        replacement,
                        flag_value.clone(),
                        *global,
                    )?);
                }
            }
            Ok(())
        }
        Op::Path(arg) => {
            out.extend(run_path(arg, &input)?);
            Ok(())
        }
        Op::Paths => {
            out.extend(run_paths_builtin(&input));
            Ok(())
        }
        Op::GetPath(arg) => {
            let mut paths = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(arg, &input, &mut paths)?;
            for path in paths.drain(..) {
                out.push(run_getpath(input.clone(), path)?);
            }
            Ok(())
        }
        Op::SetPath(path, value) => {
            let mut paths = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(path, &input, &mut paths)?;
            let mut values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(value, &input, &mut values)?;
            for path in paths.iter() {
                for value in values.iter() {
                    out.push(run_setpath(input.clone(), path.clone(), value.clone())?);
                }
            }
            Ok(())
        }
        Op::Modify(path, update) => {
            out.push(run_modify(path, update, input)?);
            Ok(())
        }
        Op::DelPaths(arg) => {
            let mut paths = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(arg, &input, &mut paths)?;
            for path in paths.drain(..) {
                out.push(run_delpaths(input.clone(), path)?);
            }
            Ok(())
        }
        Op::TruncateStream(arg) => {
            out.extend(run_truncate_stream(arg, input)?);
            Ok(())
        }
        Op::FromStream(arg) => {
            out.extend(run_fromstream(arg, input)?);
            Ok(())
        }
        Op::ToStream => {
            out.extend(run_tostream(input));
            Ok(())
        }
        Op::Flatten(arg) => {
            let depths = eval_many(arg, &input)?;
            for depth in depths {
                out.push(run_flatten(input.clone(), Some(depth))?);
            }
            Ok(())
        }
        Op::FlattenRaw(arg) => {
            let depths = eval_many(arg, &input)?;
            for depth in depths {
                out.push(run_flatten_raw(input.clone(), depth)?);
            }
            Ok(())
        }
        Op::Nth(arg) => {
            let indices = eval_many(arg, &input)?;
            for index in indices {
                out.push(run_nth(input.clone(), index)?);
            }
            Ok(())
        }
        Op::NthBy(index, source) => {
            let indices = eval_many(index, &input)?;
            for index in indices {
                if let Some(value) = run_nth_by(index, source, &input)? {
                    out.push(value);
                }
            }
            Ok(())
        }
        Op::LimitBy(count, source) => {
            let counts = eval_many(count, &input)?;
            for count in counts {
                out.extend(run_limit_by(count, source, &input)?);
            }
            Ok(())
        }
        Op::SkipBy(count, source) => {
            let counts = eval_many(count, &input)?;
            for count in counts {
                out.extend(run_skip_by(count, source, &input)?);
            }
            Ok(())
        }
        Op::Range(init, upto, by) => {
            let init_values = eval_many(init, &input)?;
            let upto_values = eval_many(upto, &input)?;
            let by_values = eval_many(by, &input)?;
            for init in init_values {
                for upto in &upto_values {
                    for by in &by_values {
                        out.extend(run_range(init.clone(), upto.clone(), by.clone())?);
                    }
                }
            }
            Ok(())
        }
        Op::While(cond, update) => {
            out.extend(run_while(cond, update, input)?);
            Ok(())
        }
        Op::Until(cond, next) => {
            out.extend(run_until(cond, next, input)?);
            Ok(())
        }
        Op::Reduce {
            source,
            pattern,
            init,
            update,
        } => {
            out.extend(run_reduce(source, pattern, init, update, &input)?);
            Ok(())
        }
        Op::Foreach {
            source,
            pattern,
            init,
            update,
            extract,
        } => {
            run_foreach(source, pattern, init, update, extract, &input, out)?;
            Ok(())
        }
        Op::Any(generator, condition) => {
            out.push(run_any(generator, condition, &input)?);
            Ok(())
        }
        Op::All(generator, condition) => {
            out.push(run_all(generator, condition, &input)?);
            Ok(())
        }
        Op::FirstBy(source) => {
            if let Some(value) = run_first_by(source, &input)? {
                out.push(value);
            }
            Ok(())
        }
        Op::LastBy(source) => {
            if let Some(value) = run_last_by(source, &input)? {
                out.push(value);
            }
            Ok(())
        }
        Op::IsEmpty(arg) => {
            out.push(run_isempty(arg, &input)?);
            Ok(())
        }
        Op::AddBy(arg) => {
            out.push(run_add_by(arg, &input)?);
            Ok(())
        }
        Op::Select(arg) => {
            let mut predicates = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(arg, &input, &mut predicates)?;
            let truthy_count = predicates.iter().filter(|pred| jq_truthy(pred)).count();
            if truthy_count == 0 {
                return Ok(());
            }
            out.reserve(truthy_count);
            for _ in 1..truthy_count {
                out.push(input.clone());
            }
            out.push(input);
            Ok(())
        }
        Op::Map(arg) => {
            let values = iter_values_like_jq(input)?;
            let mut mapped = PooledValueVec::acquire();
            for value in values {
                let mut mapped_item = PooledValueVec::acquire();
                apply_op(arg, value, &mut mapped_item)?;
                mapped.extend(mapped_item.drain(..));
            }
            out.push(ZqValue::Array(mapped.into_vec()));
            Ok(())
        }
        Op::MapValues(arg) => {
            out.push(run_map_values(arg, input)?);
            Ok(())
        }
        Op::WithEntries(arg) => {
            out.push(run_with_entries(arg, input)?);
            Ok(())
        }
        Op::RecurseBy(arg) => {
            out.extend(run_recurse(arg, None, input)?);
            Ok(())
        }
        Op::RecurseByCond(arg, cond) => {
            out.extend(run_recurse(arg, Some(cond), input)?);
            Ok(())
        }
        Op::Walk(arg) => {
            out.extend(run_walk(arg, input)?);
            Ok(())
        }
        Op::Combinations => {
            out.extend(run_combinations(input)?);
            Ok(())
        }
        Op::Repeat(arg) => run_repeat(arg, input, out),
        Op::Input => {
            if let Some(value) = next_input_value() {
                out.push(value);
                Ok(())
            } else {
                Err(encode_thrown_value(&ZqValue::String("break".to_string()))?)
            }
        }
        Op::Format { fmt, expr } => {
            let mut values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(expr, &input, &mut values)?;
            for value in values.drain(..) {
                out.push(ZqValue::String(c_string::format_value_jq(fmt, &value)?));
            }
            Ok(())
        }
        Op::Strptime(format) => {
            let mut formats = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(format, &input, &mut formats)?;
            for format in formats.drain(..) {
                out.push(run_strptime(input.clone(), format)?);
            }
            Ok(())
        }
        Op::Strftime { format, local } => {
            let mut formats = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(format, &input, &mut formats)?;
            for format in formats.drain(..) {
                out.push(run_strftime(input.clone(), format, *local)?);
            }
            Ok(())
        }
        Op::Empty => Ok(()),
        Op::Error(inner) => {
            let err_value = eval_single(inner, &input)?;
            Err(encode_thrown_value(&err_value)?)
        }
        Op::HaltError(inner) => {
            let code_value = eval_single(inner, &input)?;
            Err(encode_halt_error(code_value, input)?)
        }
        Op::UnaryMinus(inner) => {
            let mut inner_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(inner, &input, &mut inner_values)?;
            for inner_value in inner_values.drain(..) {
                out.push(unary_negate(inner_value)?);
            }
            Ok(())
        }
        Op::UnaryNot(inner) => {
            let mut inner_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(inner, &input, &mut inner_values)?;
            for inner_value in inner_values.drain(..) {
                out.push(ZqValue::Bool(!jq_truthy(&inner_value)));
            }
            Ok(())
        }
        Op::IfElse {
            cond,
            then_expr,
            else_expr,
        } => {
            let mut cond_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(cond, &input, &mut cond_values)?;
            for cond_value in cond_values.drain(..) {
                if jq_truthy(&cond_value) {
                    let mut then_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(then_expr, &input, &mut then_values)?;
                    out.extend(then_values.drain(..));
                } else {
                    let mut else_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(else_expr, &input, &mut else_values)?;
                    out.extend(else_values.drain(..));
                }
            }
            Ok(())
        }
        Op::TryCatch { inner, catcher } => {
            let mut inner_values = PooledValueVec::acquire();
            match apply_op_with_borrowed_fast_path(inner, &input, &mut inner_values) {
                Ok(()) => {
                    out.extend(inner_values.drain(..));
                    Ok(())
                }
                Err(err) => {
                    out.extend(inner_values.drain(..));
                    if decode_halt_error(&err).is_some() {
                        return Err(err);
                    }
                    let catch_input =
                        decode_thrown_value(&err).unwrap_or_else(|| ZqValue::String(err));
                    apply_op(catcher, catch_input, out)
                }
            }
        }
        Op::Binary { op, lhs, rhs } => {
            match op {
                BinaryOp::DefinedOr => {
                    let mut lhs_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(lhs, &input, &mut lhs_values)?;
                    let mut kept = PooledValueVec::acquire();
                    for value in lhs_values.drain(..) {
                        if !matches!(value, ZqValue::Null | ZqValue::Bool(false)) {
                            kept.push(value);
                        }
                    }
                    if kept.is_empty() {
                        let mut rhs_values = PooledValueVec::acquire();
                        apply_op_with_borrowed_fast_path(rhs, &input, &mut rhs_values)?;
                        out.extend(rhs_values.drain(..));
                    } else {
                        out.extend(kept.drain(..));
                    }
                    return Ok(());
                }
                BinaryOp::And => {
                    let mut lhs_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(lhs, &input, &mut lhs_values)?;
                    for lhs_value in lhs_values.drain(..) {
                        if !jq_truthy(&lhs_value) {
                            out.push(ZqValue::Bool(false));
                        } else {
                            let mut rhs_values = PooledValueVec::acquire();
                            apply_op_with_borrowed_fast_path(rhs, &input, &mut rhs_values)?;
                            for rhs_value in rhs_values.drain(..) {
                                out.push(ZqValue::Bool(jq_truthy(&rhs_value)));
                            }
                        }
                    }
                    return Ok(());
                }
                BinaryOp::Or => {
                    let mut lhs_values = PooledValueVec::acquire();
                    apply_op_with_borrowed_fast_path(lhs, &input, &mut lhs_values)?;
                    for lhs_value in lhs_values.drain(..) {
                        if jq_truthy(&lhs_value) {
                            out.push(ZqValue::Bool(true));
                        } else {
                            let mut rhs_values = PooledValueVec::acquire();
                            apply_op_with_borrowed_fast_path(rhs, &input, &mut rhs_values)?;
                            for rhs_value in rhs_values.drain(..) {
                                out.push(ZqValue::Bool(jq_truthy(&rhs_value)));
                            }
                        }
                    }
                    return Ok(());
                }
                _ => {}
            };

            // jq executes binary operators over streams from both operands and
            // emits the cartesian product in rhs-major order.
            let force_add_float = matches!(op, BinaryOp::Add)
                && matches!(lhs.as_ref(), Op::Identity)
                && matches!(
                    rhs.as_ref(),
                    Op::Literal(ZqValue::Number(n)) if n.is_i64() || n.is_u64()
                )
                && in_root_apply_context()
                && current_apply_op_depth() == 1;
            let force_numeric_float = matches!(
                (lhs.as_ref(), rhs.as_ref()),
                (Op::Literal(ZqValue::Number(a)), Op::Literal(ZqValue::Number(b)))
                    if a.is_f64() || b.is_f64()
            );
            match (
                eval_single_borrowed(lhs, &input)?,
                eval_single_borrowed(rhs, &input)?,
            ) {
                (BorrowedEvalSingle::Empty, _) | (_, BorrowedEvalSingle::Empty) => {
                    return Ok(());
                }
                (BorrowedEvalSingle::Value(lhs_value), BorrowedEvalSingle::Value(rhs_value)) => {
                    out.push(apply_binary_with_flags(
                        *op,
                        lhs_value,
                        rhs_value,
                        force_add_float,
                        force_numeric_float,
                    )?);
                    return Ok(());
                }
                _ => {}
            }
            let mut lhs_values = PooledValueVec::acquire();
            let mut rhs_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(lhs, &input, &mut lhs_values)?;
            apply_op_with_borrowed_fast_path(rhs, &input, &mut rhs_values)?;
            if lhs_values.is_empty() || rhs_values.is_empty() {
                return Ok(());
            }

            if lhs_values.len() == 1 && rhs_values.len() == 1 {
                let lhs_value = lhs_values.pop().expect("single lhs");
                let rhs_value = rhs_values.pop().expect("single rhs");
                out.push(apply_binary_with_flags(
                    *op,
                    lhs_value,
                    rhs_value,
                    force_add_float,
                    force_numeric_float,
                )?);
                return Ok(());
            }

            out.reserve(lhs_values.len().saturating_mul(rhs_values.len()));
            for rhs_value in rhs_values.drain(..) {
                if let Some((last_lhs, head_lhs)) = lhs_values.split_last() {
                    for lhs_value in head_lhs {
                        out.push(apply_binary_with_flags(
                            *op,
                            lhs_value.clone(),
                            rhs_value.clone(),
                            force_add_float,
                            force_numeric_float,
                        )?);
                    }
                    out.push(apply_binary_with_flags(
                        *op,
                        last_lhs.clone(),
                        rhs_value,
                        force_add_float,
                        force_numeric_float,
                    )?);
                }
            }
            Ok(())
        }
        Op::MathBinary { op, lhs, rhs } => {
            // Follow jq stream cartesian semantics (rhs-major order).
            match (
                eval_single_borrowed(lhs, &input)?,
                eval_single_borrowed(rhs, &input)?,
            ) {
                (BorrowedEvalSingle::Empty, _) | (_, BorrowedEvalSingle::Empty) => {
                    return Ok(());
                }
                (BorrowedEvalSingle::Value(lhs_value), BorrowedEvalSingle::Value(rhs_value)) => {
                    out.push(run_math_binary(*op, lhs_value, rhs_value)?);
                    return Ok(());
                }
                _ => {}
            }
            let mut lhs_values = PooledValueVec::acquire();
            let mut rhs_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(lhs, &input, &mut lhs_values)?;
            apply_op_with_borrowed_fast_path(rhs, &input, &mut rhs_values)?;
            if lhs_values.is_empty() || rhs_values.is_empty() {
                return Ok(());
            }

            if lhs_values.len() == 1 && rhs_values.len() == 1 {
                let lhs_value = lhs_values.pop().expect("single lhs");
                let rhs_value = rhs_values.pop().expect("single rhs");
                out.push(run_math_binary(*op, lhs_value, rhs_value)?);
                return Ok(());
            }

            out.reserve(lhs_values.len().saturating_mul(rhs_values.len()));
            for rhs_value in rhs_values.drain(..) {
                if let Some((last_lhs, head_lhs)) = lhs_values.split_last() {
                    for lhs_value in head_lhs {
                        out.push(run_math_binary(*op, lhs_value.clone(), rhs_value.clone())?);
                    }
                    out.push(run_math_binary(*op, last_lhs.clone(), rhs_value)?);
                }
            }
            Ok(())
        }
        Op::MathTernary { op, a, b, c } => {
            // Follow jq stream cartesian semantics in argument order with last arg outermost.
            match (
                eval_single_borrowed(a, &input)?,
                eval_single_borrowed(b, &input)?,
                eval_single_borrowed(c, &input)?,
            ) {
                (BorrowedEvalSingle::Empty, _, _)
                | (_, BorrowedEvalSingle::Empty, _)
                | (_, _, BorrowedEvalSingle::Empty) => {
                    return Ok(());
                }
                (
                    BorrowedEvalSingle::Value(a_value),
                    BorrowedEvalSingle::Value(b_value),
                    BorrowedEvalSingle::Value(c_value),
                ) => {
                    out.push(run_math_ternary(*op, a_value, b_value, c_value)?);
                    return Ok(());
                }
                _ => {}
            }
            let mut a_values = PooledValueVec::acquire();
            let mut b_values = PooledValueVec::acquire();
            let mut c_values = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(a, &input, &mut a_values)?;
            apply_op_with_borrowed_fast_path(b, &input, &mut b_values)?;
            apply_op_with_borrowed_fast_path(c, &input, &mut c_values)?;
            if a_values.is_empty() || b_values.is_empty() || c_values.is_empty() {
                return Ok(());
            }
            if a_values.len() == 1 && b_values.len() == 1 && c_values.len() == 1 {
                out.push(run_math_ternary(
                    *op,
                    a_values.pop().expect("single a"),
                    b_values.pop().expect("single b"),
                    c_values.pop().expect("single c"),
                )?);
                return Ok(());
            }

            out.reserve(
                a_values
                    .len()
                    .saturating_mul(b_values.len())
                    .saturating_mul(c_values.len()),
            );
            for c_value in c_values.drain(..) {
                for b_value in b_values.iter() {
                    if let Some((last_a, head_a)) = a_values.split_last() {
                        for a_value in head_a {
                            out.push(run_math_ternary(
                                *op,
                                a_value.clone(),
                                b_value.clone(),
                                c_value.clone(),
                            )?);
                        }
                        out.push(run_math_ternary(
                            *op,
                            last_a.clone(),
                            b_value.clone(),
                            c_value.clone(),
                        )?);
                    }
                }
            }
            Ok(())
        }
        Op::GetField { name, optional } => {
            let res = match input {
                ZqValue::Object(map) => {
                    out.push(map.get(name).cloned().unwrap_or(ZqValue::Null));
                    Ok(())
                }
                ZqValue::Null => {
                    out.push(ZqValue::Null);
                    Ok(())
                }
                other => Err(format!(
                    "Cannot index {} with string {:?}",
                    type_name(&other),
                    name
                )),
            };
            if *optional {
                res.or(Ok(()))
            } else {
                res
            }
        }
        Op::GetIndex { index, optional } => {
            let res = match input {
                ZqValue::Array(arr) => {
                    if let Some(idx) = c_string::normalize_index_jq(arr.len(), *index) {
                        out.push(arr[idx].clone());
                    } else {
                        out.push(ZqValue::Null);
                    }
                    Ok(())
                }
                ZqValue::String(s) => {
                    out.push(c_string::string_index_like_jq(&s, *index).unwrap_or(ZqValue::Null));
                    Ok(())
                }
                ZqValue::Null => {
                    out.push(ZqValue::Null);
                    Ok(())
                }
                other => Err(format!("Cannot index {} with number", type_name(&other))),
            };
            if *optional {
                res.or(Ok(()))
            } else {
                res
            }
        }
        Op::DynamicIndex { key, optional } => {
            run_dynamic_index(input.clone(), key, &input, *optional, out)?;
            Ok(())
        }
        Op::Slice {
            start,
            end,
            optional,
        } => {
            let res = run_slice(input, *start, *end).map(|value| {
                out.push(value);
            });
            if *optional {
                res.or(Ok(()))
            } else {
                res
            }
        }
        Op::Bind {
            source,
            pattern,
            body,
        } => {
            let mut bindings_source = PooledValueVec::acquire();
            apply_op_with_borrowed_fast_path(source, &input, &mut bindings_source)?;
            for bound in bindings_source.drain(..) {
                if let Some(values) = eval_bind_body_with_pattern(pattern, &bound, body, &input)? {
                    out.extend(values);
                }
            }
            Ok(())
        }
        Op::Iterate { optional } => match input {
            ZqValue::Array(values) => {
                out.extend(values);
                Ok(())
            }
            ZqValue::Object(map) => {
                out.extend(map.into_iter().map(|(_, value)| value));
                Ok(())
            }
            other => {
                if *optional {
                    Ok(())
                } else {
                    Err(format!(
                        "Cannot iterate over {} ({})",
                        type_name(&other),
                        value_for_error(&other)
                    ))
                }
            }
        },
    }
}

fn apply_pipe_stages(stages: &[Op], input: ZqValue, out: &mut Vec<ZqValue>) -> Result<(), String> {
    let Some((stage, rest)) = stages.split_first() else {
        out.push(input);
        return Ok(());
    };

    let mut stage_out = PooledValueVec::acquire();
    let stage_err = apply_op(stage, input, &mut stage_out).err();
    for value in stage_out.drain(..) {
        apply_pipe_stages(rest, value, out)?;
    }
    if let Some(err) = stage_err {
        return Err(err);
    }
    Ok(())
}

fn apply_pipe_stages_terminal<F>(stages: &[Op], input: ZqValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let Some((stage, rest)) = stages.split_first() else {
        emit(input)?;
        return Ok(());
    };
    if rest.is_empty() {
        return apply_op_terminal(stage, input, emit);
    }

    let mut stage_out = PooledValueVec::acquire();
    let stage_err = apply_op(stage, input, &mut stage_out).err();
    for value in stage_out.drain(..) {
        apply_pipe_stages_terminal(rest, value, emit)?;
    }
    if let Some(err) = stage_err {
        return Err(err);
    }
    Ok(())
}

fn apply_chain_steps(
    steps: &[Op],
    root: &ZqValue,
    input: ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    let Some((step, rest)) = steps.split_first() else {
        out.push(input);
        return Ok(());
    };

    let mut stage_out = PooledValueVec::acquire();
    let stage_result = match step {
        Op::DynamicIndex { key, optional } => {
            run_dynamic_index(input, key, root, *optional, &mut stage_out)
        }
        _ => apply_op(step, input, &mut stage_out),
    };
    for value in stage_out.drain(..) {
        apply_chain_steps(rest, root, value, out)?;
    }
    stage_result
}

fn apply_chain_steps_terminal<F>(
    steps: &[Op],
    root: &ZqValue,
    input: ZqValue,
    emit: &mut F,
) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let Some((step, rest)) = steps.split_first() else {
        emit(input)?;
        return Ok(());
    };
    if rest.is_empty() {
        if let Op::DynamicIndex { key, optional } = step {
            let mut stage_out = PooledValueVec::acquire();
            let stage_result = run_dynamic_index(input, key, root, *optional, &mut stage_out);
            for value in stage_out.drain(..) {
                emit(value)?;
            }
            return stage_result;
        }
        return apply_op_terminal(step, input, emit);
    }

    let mut stage_out = PooledValueVec::acquire();
    let stage_result = match step {
        Op::DynamicIndex { key, optional } => {
            run_dynamic_index(input, key, root, *optional, &mut stage_out)
        }
        _ => apply_op(step, input, &mut stage_out),
    };
    for value in stage_out.drain(..) {
        apply_chain_steps_terminal(rest, root, value, emit)?;
    }
    stage_result
}

fn eval_many_pooled(op: &Op, input: &ZqValue) -> Result<PooledValueVec, String> {
    let mut out = PooledValueVec::acquire();
    apply_op(op, input.clone(), &mut out)?;
    Ok(out)
}

fn eval_many(op: &Op, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    Ok(eval_many_pooled(op, input)?.into_vec())
}

fn encode_thrown_value(value: &ZqValue) -> Result<String, String> {
    let payload = serde_json::to_string(&value.clone().into_json())
        .map_err(|e| format!("cannot encode thrown value: {e}"))?;
    Ok(format!("{THROWN_VALUE_PREFIX}{payload}"))
}

fn encode_halt_error(code_value: ZqValue, input: ZqValue) -> Result<String, String> {
    let code = halt_error_code(code_value)?;
    let stderr = halt_error_stderr_payload(&input)?;
    let payload = serde_json::to_string(&(code, stderr))
        .map_err(|e| format!("cannot encode halt error payload: {e}"))?;
    Ok(format!("{HALT_VALUE_PREFIX}{payload}"))
}

fn decode_thrown_value(err: &str) -> Option<ZqValue> {
    let payload = err.strip_prefix(THROWN_VALUE_PREFIX)?;
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    Some(ZqValue::from_json(value))
}

pub(crate) fn decode_halt_error(err: &str) -> Option<(i32, String)> {
    let payload = err.strip_prefix(HALT_VALUE_PREFIX)?;
    serde_json::from_str(payload).ok()
}

fn halt_error_code(value: ZqValue) -> Result<i32, String> {
    let ZqValue::Number(number) = value else {
        return Err(format!(
            "halt_error/1 requires numeric exit code, got {} ({})",
            type_name(&value),
            value_for_error(&value)
        ));
    };
    let Some(raw) = number.as_f64() else {
        return Err("number is out of range".to_string());
    };
    if !raw.is_finite() {
        return Err("number is not finite".to_string());
    }
    Ok(c_math::dtoi_compat(raw) as i32)
}

fn halt_error_stderr_payload(input: &ZqValue) -> Result<String, String> {
    match input {
        ZqValue::Null => Ok(String::new()),
        ZqValue::String(s) => Ok(s.clone()),
        other => serde_json::to_string(&other.clone().into_json())
            .map_err(|e| format!("cannot encode halt error stderr payload: {e}")),
    }
}

fn render_public_error(err: String) -> String {
    if decode_halt_error(&err).is_some() {
        return err;
    }
    decode_thrown_value(&err)
        .map(error_to_string)
        .unwrap_or(err)
}

fn eval_single(op: &Op, input: &ZqValue) -> Result<ZqValue, String> {
    let mut out = eval_many_pooled(op, input)?;
    match out.len() {
        0 => Err("expression produced no values".to_string()),
        1 => Ok(out.pop().expect("single value")),
        n => Err(format!("expression produced {n} values")),
    }
}

fn eval_bind_body_with_pattern(
    pattern: &OpBindingPattern,
    bound: &ZqValue,
    body: &Op,
    input: &ZqValue,
) -> Result<Option<Vec<ZqValue>>, String> {
    match pattern {
        OpBindingPattern::Alternatives(alternatives) => {
            let mut vars = Vec::new();
            for alternative in alternatives {
                collect_pattern_vars(alternative, &mut vars);
            }

            let mut last_err = None;
            for alternative in alternatives {
                let bindings = match bind_pattern(alternative, bound) {
                    Ok(bindings) => bindings,
                    Err(err) => {
                        last_err = Some(err);
                        continue;
                    }
                };
                let merged = merge_bindings_with_null_defaults(bindings, &vars);
                let _guard = push_bindings(merged);
                match eval_many(body, input) {
                    Ok(values) => return Ok(Some(values)),
                    Err(err) => last_err = Some(err),
                }
            }

            Err(last_err.unwrap_or_else(|| "pattern alternatives did not match".to_string()))
        }
        _ => {
            let Ok(bindings) = bind_pattern(pattern, bound) else {
                return Ok(None);
            };
            let _guard = push_bindings(bindings);
            Ok(Some(eval_many(body, input)?))
        }
    }
}

fn bind_pattern(
    pattern: &OpBindingPattern,
    value: &ZqValue,
) -> Result<Vec<(String, ZqValue)>, String> {
    match pattern {
        OpBindingPattern::Var(name) => Ok(vec![(name.clone(), value.clone())]),
        OpBindingPattern::Array(items) => {
            let elements = match value {
                ZqValue::Array(values) => Some(values),
                ZqValue::Null => None,
                other => return Err(format!("Cannot index {} with number", type_name(other))),
            };
            let mut out = Vec::new();
            for (idx, item_pattern) in items.iter().enumerate() {
                let item = elements
                    .and_then(|values| values.get(idx))
                    .cloned()
                    .unwrap_or(ZqValue::Null);
                out.extend(bind_pattern(item_pattern, &item)?);
            }
            Ok(out)
        }
        OpBindingPattern::Object(entries) => {
            let map = match value {
                ZqValue::Object(map) => Some(map),
                ZqValue::Null => None,
                other => {
                    let key = entries
                        .first()
                        .and_then(|entry| match &entry.key {
                            OpBindingKeySpec::Literal(s) => Some(s.as_str()),
                            OpBindingKeySpec::Expr(_) => None,
                        })
                        .unwrap_or("");
                    return Err(format!(
                        "Cannot index {} with string {:?}",
                        type_name(other),
                        key
                    ));
                }
            };

            let mut out = Vec::new();
            for entry in entries {
                let key = eval_binding_key(&entry.key, value)?;
                let entry_value = map
                    .and_then(|obj| obj.get(&key))
                    .cloned()
                    .unwrap_or(ZqValue::Null);
                if let Some(name) = &entry.store_var {
                    out.push((name.clone(), entry_value.clone()));
                }
                out.extend(bind_pattern(&entry.pattern, &entry_value)?);
            }
            Ok(out)
        }
        OpBindingPattern::Alternatives(alternatives) => {
            let mut vars = Vec::new();
            for alternative in alternatives {
                collect_pattern_vars(alternative, &mut vars);
            }

            let mut last_error = None;
            for alternative in alternatives {
                match bind_pattern(alternative, value) {
                    Ok(bindings) => {
                        return Ok(merge_bindings_with_null_defaults(bindings, &vars));
                    }
                    Err(err) => last_error = Some(err),
                }
            }

            Err(last_error.unwrap_or_else(|| "pattern alternatives did not match".to_string()))
        }
    }
}

fn collect_pattern_vars(pattern: &OpBindingPattern, out: &mut Vec<String>) {
    match pattern {
        OpBindingPattern::Var(name) => push_unique_var(out, name),
        OpBindingPattern::Array(items) => {
            for item in items {
                collect_pattern_vars(item, out);
            }
        }
        OpBindingPattern::Object(entries) => {
            for entry in entries {
                collect_object_pattern_vars(entry, out);
            }
        }
        OpBindingPattern::Alternatives(alternatives) => {
            for alternative in alternatives {
                collect_pattern_vars(alternative, out);
            }
        }
    }
}

fn collect_object_pattern_vars(entry: &OpObjectBindingEntry, out: &mut Vec<String>) {
    if let Some(store_var) = &entry.store_var {
        push_unique_var(out, store_var);
    }
    collect_pattern_vars(&entry.pattern, out);
}

fn push_unique_var(vars: &mut Vec<String>, name: &str) {
    if vars.iter().all(|existing| existing != name) {
        vars.push(name.to_string());
    }
}

fn merge_bindings_with_null_defaults(
    bindings: Vec<(String, ZqValue)>,
    vars: &[String],
) -> Vec<(String, ZqValue)> {
    let mut merged = vars
        .iter()
        .map(|name| (name.clone(), ZqValue::Null))
        .collect::<Vec<_>>();
    for (name, value) in bindings {
        if let Some((_, slot)) = merged.iter_mut().find(|(existing, _)| existing == &name) {
            *slot = value;
        } else {
            merged.push((name, value));
        }
    }
    merged
}

fn eval_binding_key(key: &OpBindingKeySpec, input: &ZqValue) -> Result<String, String> {
    match key {
        OpBindingKeySpec::Literal(s) => Ok(s.clone()),
        OpBindingKeySpec::Expr(program) => {
            let mut values = eval_many_pooled(program, input)?;
            let Some(first) = values.drain(..).next() else {
                return Err("object pattern key expression produced no value".to_string());
            };
            c_container::object_key_to_string_jq(first)
        }
    }
}

fn error_to_string(value: ZqValue) -> String {
    match value {
        ZqValue::String(s) => s,
        other => serde_json::to_string(&other.into_json())
            .unwrap_or_else(|_| "<invalid-error-value>".to_string()),
    }
}

fn apply_binary(op: BinaryOp, lhs: ZqValue, rhs: ZqValue) -> Result<ZqValue, String> {
    apply_binary_with_flags(op, lhs, rhs, false, false)
}

fn apply_binary_with_flags(
    op: BinaryOp,
    lhs: ZqValue,
    rhs: ZqValue,
    force_add_float: bool,
    force_numeric_float: bool,
) -> Result<ZqValue, String> {
    match op {
        BinaryOp::Add => binop_add(lhs, rhs, force_add_float || force_numeric_float),
        BinaryOp::Sub => binop_sub(lhs, rhs, force_numeric_float),
        BinaryOp::Mul => binop_mul(lhs, rhs, force_numeric_float),
        BinaryOp::Div => binop_div(lhs, rhs, force_numeric_float),
        BinaryOp::Mod => binop_mod(lhs, rhs, force_numeric_float),
        BinaryOp::Pow => binop_pow(lhs, rhs, force_numeric_float),
        BinaryOp::Eq => Ok(ZqValue::Bool(jq_cmp(&lhs, &rhs) == Ordering::Equal)),
        BinaryOp::Ne => Ok(ZqValue::Bool(jq_cmp(&lhs, &rhs) != Ordering::Equal)),
        BinaryOp::Gt => Ok(ZqValue::Bool(jq_cmp(&lhs, &rhs) == Ordering::Greater)),
        BinaryOp::Ge => {
            let ord = jq_cmp(&lhs, &rhs);
            Ok(ZqValue::Bool(
                ord == Ordering::Greater || ord == Ordering::Equal,
            ))
        }
        BinaryOp::Lt => Ok(ZqValue::Bool(jq_cmp(&lhs, &rhs) == Ordering::Less)),
        BinaryOp::Le => {
            let ord = jq_cmp(&lhs, &rhs);
            Ok(ZqValue::Bool(
                ord == Ordering::Less || ord == Ordering::Equal,
            ))
        }
        BinaryOp::And | BinaryOp::Or | BinaryOp::DefinedOr => {
            unreachable!("boolean/defined-or ops handled separately")
        }
    }
}

fn run_indices(haystack: ZqValue, needle: ZqValue) -> Result<ZqValue, String> {
    match (haystack, needle) {
        (ZqValue::Array(values), ZqValue::Array(pattern)) => {
            Ok(c_container::indices_array_jq(values, pattern))
        }
        (ZqValue::Array(values), other) => Ok(c_container::indices_array_jq(values, vec![other])),
        (ZqValue::String(text), ZqValue::String(pattern)) => {
            Ok(c_container::indices_string_jq(text, pattern))
        }
        (container, key) => jq_get_dynamic(container, key),
    }
}

fn run_flatten(input: ZqValue, depth: Option<ZqValue>) -> Result<ZqValue, String> {
    // jq builtin.jq:
    // def _flatten($x): reduce .[] as $i ([]; if $i | type == "array" and $x != 0 then . + ($i | _flatten($x-1)) else . + [$i] end);
    // def flatten($x): if $x < 0 then error("flatten depth must not be negative") else _flatten($x) end;
    // def flatten: _flatten(-1);
    let depth = match depth {
        Some(depth) => {
            if jq_cmp(&depth, &ZqValue::from(0)) == Ordering::Less {
                return Err("flatten depth must not be negative".to_string());
            }
            depth
        }
        None => ZqValue::from(-1),
    };
    flatten_impl(input, depth)
}

fn run_flatten_raw(input: ZqValue, depth: ZqValue) -> Result<ZqValue, String> {
    flatten_impl(input, depth)
}

fn flatten_impl(input: ZqValue, depth: ZqValue) -> Result<ZqValue, String> {
    struct FlattenFrame {
        values: Vec<ZqValue>,
        index: usize,
        depth: ZqValue,
    }

    let mut out = Vec::new();
    let mut stack = vec![FlattenFrame {
        values: iter_values_like_jq(input)?,
        index: 0,
        depth,
    }];

    while let Some(frame) = stack.last_mut() {
        if frame.index >= frame.values.len() {
            stack.pop();
            continue;
        }

        let value = frame.values[frame.index].clone();
        frame.index += 1;

        if matches!(value, ZqValue::Array(_))
            && jq_cmp(&frame.depth, &ZqValue::from(0)) != Ordering::Equal
        {
            let next_depth = apply_binary(BinaryOp::Sub, frame.depth.clone(), ZqValue::from(1))?;
            stack.push(FlattenFrame {
                values: iter_values_like_jq(value)?,
                index: 0,
                depth: next_depth,
            });
        } else {
            out.push(value);
        }
    }

    Ok(ZqValue::Array(out))
}

fn run_transpose(input: ZqValue) -> Result<ZqValue, String> {
    // jq definition:
    // [range(0; map(length)|max // 0) as $i | [.[][$i]]]
    let rows = iter_values_like_jq(input)?;
    let mut max_len = 0.0f64;
    for row in &rows {
        let length = run_length(row.clone())?;
        let ZqValue::Number(n) = length else {
            return Err("length() result must be numeric".to_string());
        };
        let Some(value) = n.as_f64() else {
            return Err("number is out of range".to_string());
        };
        if value > max_len {
            max_len = value;
        }
    }

    let mut out = Vec::new();
    let mut i = 0usize;
    while (i as f64) < max_len {
        let mut col = Vec::with_capacity(rows.len());
        for row in &rows {
            col.push(jq_get_dynamic(row.clone(), ZqValue::from(i as i64))?);
        }
        out.push(ZqValue::Array(col));
        i += 1;
    }
    Ok(ZqValue::Array(out))
}

fn run_nth(input: ZqValue, key: ZqValue) -> Result<ZqValue, String> {
    jq_get_dynamic(input, key)
}

fn run_range(init: ZqValue, upto: ZqValue, by: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq builtin.jq:
    // def range($init; $upto; $by):
    //   if $by > 0 then $init|while(. < $upto; . + $by)
    //   elif $by < 0 then $init|while(. > $upto; . + $by)
    //   else empty end;
    let mut out = Vec::new();
    let mut current = init;
    let zero = ZqValue::from(0);
    let by_ord = jq_cmp(&by, &zero);
    match by_ord {
        Ordering::Greater => {
            while jq_cmp(&current, &upto) == Ordering::Less {
                out.push(current.clone());
                current = apply_binary(BinaryOp::Add, current, by.clone())?;
            }
        }
        Ordering::Less => {
            while jq_cmp(&current, &upto) == Ordering::Greater {
                out.push(current.clone());
                current = apply_binary(BinaryOp::Add, current, by.clone())?;
            }
        }
        Ordering::Equal => {}
    }
    Ok(out)
}

fn run_combinations(input: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq/src/builtin.jq
    // def combinations:
    //   if length == 0 then [] else
    //     .[0][] as $x
    //     | (.[1:] | combinations) as $y
    //     | [$x] + $y
    //   end;
    let len = run_length(input.clone())?;
    if jq_cmp(&len, &ZqValue::from(0)) == Ordering::Equal {
        return Ok(vec![ZqValue::Array(Vec::new())]);
    }

    let head = jq_get_dynamic(input.clone(), ZqValue::from(0))?;
    let head_values = iter_values_like_jq(head)?;
    let tail = run_slice(input, Some(1), None)?;
    let tails = run_combinations(tail)?;
    let mut out = Vec::new();
    for head_value in head_values {
        for tail_value in &tails {
            out.push(binop_add(
                ZqValue::Array(vec![head_value.clone()]),
                tail_value.clone(),
                false,
            )?);
        }
    }
    Ok(out)
}

fn run_repeat(arg: &Op, input: ZqValue, out: &mut Vec<ZqValue>) -> Result<(), String> {
    loop {
        let mut cycle = PooledValueVec::acquire();
        match apply_op(arg, input.clone(), &mut cycle) {
            Ok(()) => out.extend(cycle.drain(..)),
            Err(err) => {
                out.extend(cycle.drain(..));
                return Err(err);
            }
        }
    }
}

fn run_repeat_terminal<F>(arg: &Op, input: ZqValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    loop {
        let mut cycle = PooledValueVec::acquire();
        match apply_op(arg, input.clone(), &mut cycle) {
            Ok(()) => {
                for value in cycle.drain(..) {
                    emit(value)?;
                }
            }
            Err(err) => {
                for value in cycle.drain(..) {
                    emit(value)?;
                }
                return Err(err);
            }
        }
    }
}

fn run_while(cond: &Op, update: &Op, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq builtin.jq:
    // def while(cond; update):
    //   def _while:
    //     if cond then ., (update | _while) else empty end;
    //   _while;
    let mut out = Vec::new();
    run_while_inner(cond, update, input, &mut out)?;
    Ok(out)
}

fn run_while_inner(
    cond: &Op,
    update: &Op,
    input: ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    let cond_value = eval_single(cond, &input)?;
    if jq_truthy(&cond_value) {
        out.push(input.clone());
        let next_values = eval_many(update, &input)?;
        for next in next_values {
            run_while_inner(cond, update, next, out)?;
        }
    }
    Ok(())
}

fn run_until(cond: &Op, next: &Op, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq builtin.jq:
    // def until(cond; next):
    //   def _until:
    //     if cond then . else (next|_until) end;
    //   _until;
    let mut out = Vec::new();
    run_until_inner(cond, next, input, &mut out)?;
    Ok(out)
}

fn run_until_inner(
    cond: &Op,
    next: &Op,
    input: ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    let cond_value = eval_single(cond, &input)?;
    if jq_truthy(&cond_value) {
        out.push(input);
    } else {
        let next_values = eval_many(next, &input)?;
        for value in next_values {
            run_until_inner(cond, next, value, out)?;
        }
    }
    Ok(())
}

fn run_reduce(
    source: &Op,
    pattern: &OpBindingPattern,
    init: &Op,
    update: &Op,
    input: &ZqValue,
) -> Result<Vec<ZqValue>, String> {
    // jq parser.y:
    // reduce Expr as Patterns (Query; Query)
    let source_values = eval_many(source, input)?;
    let mut states = eval_many(init, input)?;

    for value in source_values {
        let Ok(bindings) = bind_pattern(pattern, &value) else {
            continue;
        };
        let mut next_states = Vec::new();
        for state in &states {
            let _guard = push_bindings(bindings.clone());
            next_states.extend(eval_many(update, state)?);
        }
        states = next_states;
    }

    Ok(states)
}

fn run_foreach(
    source: &Op,
    pattern: &OpBindingPattern,
    init: &Op,
    update: &Op,
    extract: &Op,
    input: &ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    // jq parser.y:
    // foreach Expr as Patterns (Query; Query; Query?)
    let source_values = eval_many(source, input)?;
    for init_state in eval_many(init, input)? {
        let mut states = vec![init_state];
        for value in &source_values {
            let Ok(bindings) = bind_pattern(pattern, value) else {
                continue;
            };
            let mut next_states = Vec::new();
            for state in &states {
                let _guard = push_bindings(bindings.clone());
                let updated_states = eval_many(update, state)?;
                for updated in updated_states {
                    out.extend(eval_many(extract, &updated)?);
                    next_states.push(updated);
                }
            }
            states = next_states;
        }
    }

    Ok(())
}

fn run_any(generator: &Op, condition: &Op, input: &ZqValue) -> Result<ZqValue, String> {
    // jq builtin.jq:
    // def any(generator; condition): isempty(generator|condition or empty)|not;
    let mut found_true = false;
    foreach_stream_value(generator, input, &mut |value| {
        let cond_values = eval_many(condition, &value)?;
        for cond_value in cond_values {
            if jq_truthy(&cond_value) {
                found_true = true;
                return Ok(false);
            }
        }
        Ok(true)
    })?;
    Ok(ZqValue::Bool(found_true))
}

fn run_all(generator: &Op, condition: &Op, input: &ZqValue) -> Result<ZqValue, String> {
    // jq builtin.jq:
    // def all(generator; condition): isempty(generator|condition and empty);
    let mut found_false = false;
    foreach_stream_value(generator, input, &mut |value| {
        let cond_values = eval_many(condition, &value)?;
        for cond_value in cond_values {
            if !jq_truthy(&cond_value) {
                found_false = true;
                return Ok(false);
            }
        }
        Ok(true)
    })?;
    Ok(ZqValue::Bool(!found_false))
}

fn run_first_by(source: &Op, input: &ZqValue) -> Result<Option<ZqValue>, String> {
    let mut first = None;
    foreach_stream_value(source, input, &mut |value| {
        first = Some(value);
        Ok(false)
    })?;
    Ok(first)
}

fn run_last_by(source: &Op, input: &ZqValue) -> Result<Option<ZqValue>, String> {
    let mut last = None;
    foreach_stream_value(source, input, &mut |value| {
        last = Some(value);
        Ok(true)
    })?;
    Ok(last)
}

fn run_nth_by(index: ZqValue, source: &Op, input: &ZqValue) -> Result<Option<ZqValue>, String> {
    if jq_cmp(&index, &ZqValue::from(0)) == Ordering::Less {
        return Err("nth doesn't support negative indices".to_string());
    }

    // jq definition:
    // nth($n; g): if $n < 0 then error(...) else first(skip($n; g)) end;
    // skip($n; g): foreach g as $item ($n; . - 1; if . < 0 then $item else empty end)
    let mut state = index;
    let mut out = None;
    foreach_stream_value(source, input, &mut |value| {
        state = apply_binary(BinaryOp::Sub, state.clone(), ZqValue::from(1))?;
        if jq_cmp(&state, &ZqValue::from(0)) == Ordering::Less {
            out = Some(value);
            Ok(false)
        } else {
            Ok(true)
        }
    })?;
    Ok(out)
}

fn run_limit_by(count: ZqValue, source: &Op, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    match jq_cmp(&count, &ZqValue::from(0)) {
        Ordering::Greater => {
            // jq:
            // foreach expr as $item ($n; . - 1; $item, if . <= 0 then break else empty end)
            let mut state = count;
            let mut out = Vec::new();
            foreach_stream_value(source, input, &mut |value| {
                state = apply_binary(BinaryOp::Sub, state.clone(), ZqValue::from(1))?;
                out.push(value);
                let ord = jq_cmp(&state, &ZqValue::from(0));
                Ok(ord == Ordering::Greater)
            })?;
            Ok(out)
        }
        Ordering::Equal => Ok(Vec::new()),
        Ordering::Less => Err("limit doesn't support negative count".to_string()),
    }
}

fn run_skip_by(count: ZqValue, source: &Op, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    match jq_cmp(&count, &ZqValue::from(0)) {
        Ordering::Greater => {
            // jq:
            // foreach expr as $item ($n; . - 1; if . < 0 then $item else empty end)
            let mut state = count;
            let mut out = Vec::new();
            foreach_stream_value(source, input, &mut |value| {
                state = apply_binary(BinaryOp::Sub, state.clone(), ZqValue::from(1))?;
                if jq_cmp(&state, &ZqValue::from(0)) == Ordering::Less {
                    out.push(value);
                }
                Ok(true)
            })?;
            Ok(out)
        }
        Ordering::Equal => eval_many(source, input),
        Ordering::Less => Err("skip doesn't support negative count".to_string()),
    }
}

fn foreach_stream_value<F>(op: &Op, input: &ZqValue, f: &mut F) -> Result<(), String>
where
    F: FnMut(ZqValue) -> Result<bool, String>,
{
    match op {
        Op::Comma(items) => {
            for item in items {
                if !foreach_stream_value_inner(item, input, f)? {
                    return Ok(());
                }
            }
            Ok(())
        }
        _ => {
            let _ = foreach_stream_value_inner(op, input, f)?;
            Ok(())
        }
    }
}

fn foreach_stream_value_inner<F>(op: &Op, input: &ZqValue, f: &mut F) -> Result<bool, String>
where
    F: FnMut(ZqValue) -> Result<bool, String>,
{
    match op {
        Op::Comma(items) => {
            for item in items {
                if !foreach_stream_value_inner(item, input, f)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => {
            let values = eval_many(op, input)?;
            for value in values {
                if !f(value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn run_isempty(arg: &Op, input: &ZqValue) -> Result<ZqValue, String> {
    // jq def: isempty(g): first((g|false), true);
    // Must short-circuit on first produced value.
    match arg {
        Op::Comma(items) => {
            for item in items {
                let values = eval_many(item, input)?;
                if !values.is_empty() {
                    return Ok(ZqValue::Bool(false));
                }
            }
            Ok(ZqValue::Bool(true))
        }
        _ => {
            let values = eval_many(arg, input)?;
            Ok(ZqValue::Bool(values.is_empty()))
        }
    }
}

fn jq_get_dynamic(container: ZqValue, key: ZqValue) -> Result<ZqValue, String> {
    jq_get_dynamic_ref(&container, &key)
}

fn jq_get_dynamic_ref(container: &ZqValue, key: &ZqValue) -> Result<ZqValue, String> {
    match (container, key) {
        (ZqValue::Object(map), ZqValue::String(name)) => {
            Ok(map.get(name).cloned().unwrap_or(ZqValue::Null))
        }
        (ZqValue::Array(values), ZqValue::Number(n)) => {
            let Some(raw) = n.as_f64() else {
                return Ok(ZqValue::Null);
            };
            if raw.is_nan() {
                return Ok(ZqValue::Null);
            }
            let mut idx = c_math::dtoi_compat(raw);
            if idx < 0 {
                idx += values.len() as i64;
            }
            if idx < 0 || idx as usize >= values.len() {
                Ok(ZqValue::Null)
            } else {
                Ok(values[idx as usize].clone())
            }
        }
        (ZqValue::String(text), ZqValue::Number(n)) => {
            let raw = c_math::number_to_f64_lossy_for_index(
                n,
                "Array/string slice indices must be integers",
            )?;
            if raw.is_nan() {
                return Ok(ZqValue::Null);
            }
            if raw.fract() != 0.0 {
                return Err(format!(
                    "Cannot index string with number ({})",
                    value_for_error(key)
                ));
            }
            let idx = c_math::dtoi_compat(raw);
            Ok(c_string::string_index_like_jq(text, idx).unwrap_or(ZqValue::Null))
        }
        // jq-port: jq/src/jv_aux.c:jv_get() with object key as slice descriptor.
        (ZqValue::Array(values), ZqValue::Object(slice)) => {
            let (start, end) = parse_slice_key_like_jq(values.len(), slice)?;
            Ok(ZqValue::Array(values[start..end].to_vec()))
        }
        // jq-port: jq/src/jv_aux.c:jv_get() with object key as slice descriptor.
        (ZqValue::String(text), ZqValue::Object(slice)) => {
            let chars = text.chars().collect::<Vec<_>>();
            let (start, end) = parse_slice_key_like_jq(chars.len(), slice)?;
            Ok(ZqValue::String(chars[start..end].iter().collect()))
        }
        (ZqValue::Array(values), ZqValue::Array(pattern)) => Ok(c_container::indices_array_jq(
            values.clone(),
            pattern.clone(),
        )),
        (ZqValue::Null, ZqValue::String(_) | ZqValue::Number(_) | ZqValue::Object(_)) => {
            Ok(ZqValue::Null)
        }
        (container, key) => {
            if let ZqValue::String(name) = key {
                return Err(format!(
                    "Cannot index {} with string {:?}",
                    type_name(container),
                    name,
                ));
            }
            Err(format!(
                "Cannot index {} with {} ({})",
                type_name(container),
                type_name(key),
                value_for_error(key)
            ))
        }
    }
}

fn parse_slice_key_like_jq(
    len: usize,
    key: &IndexMap<String, ZqValue>,
) -> Result<(usize, usize), String> {
    let start = match key.get("start") {
        None | Some(ZqValue::Null) => None,
        Some(ZqValue::Number(number)) => Some(c_math::number_to_f64_lossy_for_index(
            number,
            "Array/string slice indices must be integers",
        )?),
        _ => return Err("Array/string slice indices must be integers".to_string()),
    };
    let end = match key.get("end") {
        None | Some(ZqValue::Null) => None,
        Some(ZqValue::Number(number)) => Some(c_math::number_to_f64_lossy_for_index(
            number,
            "Array/string slice indices must be integers",
        )?),
        _ => return Err("Array/string slice indices must be integers".to_string()),
    };
    Ok(c_math::slice_bounds_from_f64_like_jq(len, start, end))
}

fn run_add(input: ZqValue) -> Result<ZqValue, String> {
    let values = iter_values_like_jq(input)?;
    let mut acc = ZqValue::Null;
    for value in values {
        acc = binop_add(acc, value, false)?;
    }
    Ok(acc)
}

fn run_add_by(arg: &Op, input: &ZqValue) -> Result<ZqValue, String> {
    // jq builtin.jq:
    // def add(f): reduce f as $x (null; . + $x);
    let values = eval_many(arg, input)?;
    let mut acc = ZqValue::Null;
    for value in values {
        acc = binop_add(acc, value, false)?;
    }
    Ok(acc)
}

fn run_map_values(arg: &Op, input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let mapped = eval_many(arg, &value)?;
                if let Some(first) = mapped.into_iter().next() {
                    out.push(first);
                }
            }
            Ok(ZqValue::Array(out))
        }
        ZqValue::Object(map) => {
            let mut out = IndexMap::with_capacity(map.len());
            for (key, value) in map {
                let mapped = eval_many(arg, &value)?;
                if let Some(first) = mapped.into_iter().next() {
                    out.insert(key, first);
                }
            }
            Ok(ZqValue::Object(out))
        }
        other => Err(format!(
            "Cannot iterate over {} ({})",
            type_name(&other),
            value_for_error(&other)
        )),
    }
}

fn run_with_entries(arg: &Op, input: ZqValue) -> Result<ZqValue, String> {
    // jq builtin.jq:
    // def with_entries(f): to_entries | map(f) | from_entries;
    let entries = c_container::to_entries_jq(input)?;
    let values = iter_values_like_jq(entries)?;
    let mut mapped = Vec::new();
    for value in values {
        mapped.extend(eval_many(arg, &value)?);
    }
    c_container::from_entries_jq(ZqValue::Array(mapped))
}

fn run_recurse(next: &Op, cond: Option<&Op>, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    let mut out = Vec::new();
    run_recurse_inner(next, cond, input, &mut out)?;
    Ok(out)
}

fn run_recurse_inner(
    next: &Op,
    cond: Option<&Op>,
    input: ZqValue,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    out.push(input.clone());
    let next_values = eval_many(next, &input)?;
    match cond {
        Some(cond) => {
            for next_value in next_values {
                let truthy_count = eval_many(cond, &next_value)?
                    .into_iter()
                    .filter(jq_truthy)
                    .count();
                if truthy_count == 0 {
                    continue;
                }
                for _ in 1..truthy_count {
                    run_recurse_inner(next, Some(cond), next_value.clone(), out)?;
                }
                run_recurse_inner(next, Some(cond), next_value, out)?;
            }
        }
        None => {
            for next_value in next_values {
                run_recurse_inner(next, None, next_value, out)?;
            }
        }
    }
    Ok(())
}

fn run_walk(arg: &Op, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    // jq builtin.jq:
    // def walk(f):
    //   def w:
    //     if type == "object" then map_values(w)
    //     elif type == "array" then map(w)
    //     else .
    //     end
    //     | f;
    //   w;
    let recursed = match input {
        ZqValue::Array(values) => {
            let mut mapped = Vec::new();
            for value in values {
                mapped.extend(run_walk(arg, value)?);
            }
            ZqValue::Array(mapped)
        }
        ZqValue::Object(map) => {
            let mut mapped = IndexMap::with_capacity(map.len());
            for (key, value) in map {
                let walked = run_walk(arg, value)?;
                if let Some(first) = walked.into_iter().next() {
                    mapped.insert(key, first);
                }
            }
            ZqValue::Object(mapped)
        }
        other => other,
    };
    eval_many(arg, &recursed)
}

fn iter_values_like_jq(input: ZqValue) -> Result<Vec<ZqValue>, String> {
    c_container::iter_values_like_jq(input)
}

fn binop_add(lhs: ZqValue, rhs: ZqValue, force_add_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Null, r) => Ok(r),
        (l, ZqValue::Null) => Ok(l),
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(c_math::number_to_value_with_hint(af + bf, force_add_float))
        }
        (ZqValue::String(a), ZqValue::String(b)) => Ok(ZqValue::String(format!("{a}{b}"))),
        (ZqValue::Array(mut a), ZqValue::Array(b)) => {
            a.extend(b);
            Ok(ZqValue::Array(a))
        }
        (ZqValue::Object(mut a), ZqValue::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
            Ok(ZqValue::Object(a))
        }
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be added",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn binop_sub(lhs: ZqValue, rhs: ZqValue, force_numeric_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(c_math::number_to_value_with_hint(
                af - bf,
                force_numeric_float,
            ))
        }
        (ZqValue::Array(a), ZqValue::Array(b)) => {
            let mut out = Vec::with_capacity(a.len());
            for value in a {
                if !b.iter().any(|candidate| candidate == &value) {
                    out.push(value);
                }
            }
            Ok(ZqValue::Array(out))
        }
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be subtracted",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn binop_mul(lhs: ZqValue, rhs: ZqValue, force_numeric_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(c_math::number_to_value_with_hint(
                af * bf,
                force_numeric_float,
            ))
        }
        (ZqValue::String(s), ZqValue::Number(n)) | (ZqValue::Number(n), ZqValue::String(s)) => {
            let count = c_math::jq_number_to_f64_lossy(&n)
                .ok_or_else(|| "number is out of range".to_string())?;
            let repeat = c_string::string_repeat_count_jq(count);
            c_string::string_repeat_jq(s, repeat)
        }
        (ZqValue::Object(a), ZqValue::Object(b)) => Ok(ZqValue::Object(
            c_container::object_merge_recursive_jq(a, b),
        )),
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be multiplied",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn binop_div(lhs: ZqValue, rhs: ZqValue, force_numeric_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            if bf == 0.0 {
                let left = ZqValue::Number(a);
                let right = ZqValue::Number(b);
                return Err(format!(
                    "{} ({}) and {} ({}) cannot be divided because the divisor is zero",
                    type_name(&left),
                    value_for_error(&left),
                    type_name(&right),
                    value_for_error(&right)
                ));
            }
            Ok(c_math::number_to_value_with_hint(
                af / bf,
                force_numeric_float,
            ))
        }
        (ZqValue::String(a), ZqValue::String(b)) => {
            c_string::split_jq(ZqValue::String(a), ZqValue::String(b))
        }
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be divided",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn binop_mod(lhs: ZqValue, rhs: ZqValue, force_numeric_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            if bf == 0.0 {
                let left = ZqValue::Number(a);
                let right = ZqValue::Number(b);
                return Err(format!(
                    "{} ({}) and {} ({}) cannot be divided (remainder) because the divisor is zero",
                    type_name(&left),
                    value_for_error(&left),
                    type_name(&right),
                    value_for_error(&right)
                ));
            }
            let v = c_math::mod_compat(af, bf).map_err(ToString::to_string)?;
            Ok(c_math::number_to_value_with_hint(v, force_numeric_float))
        }
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be divided (remainder)",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn binop_pow(lhs: ZqValue, rhs: ZqValue, force_numeric_float: bool) -> Result<ZqValue, String> {
    match (lhs, rhs) {
        (ZqValue::Number(a), ZqValue::Number(b)) => {
            let af = c_math::jq_number_to_f64_lossy(&a)
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = c_math::jq_number_to_f64_lossy(&b)
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(c_math::number_to_value_with_hint(
                af.powf(bf),
                force_numeric_float,
            ))
        }
        (l, r) => Err(format!(
            "{} ({}) and {} ({}) cannot be exponentiated",
            type_name(&l),
            value_for_error(&l),
            type_name(&r),
            value_for_error(&r)
        )),
    }
}

fn run_slice(input: ZqValue, start: Option<i64>, end: Option<i64>) -> Result<ZqValue, String> {
    match input {
        ZqValue::Array(values) => {
            let (s, e) = slice_bounds(values.len(), start, end);
            Ok(ZqValue::Array(values[s..e].to_vec()))
        }
        ZqValue::String(text) => {
            let chars = text.chars().collect::<Vec<_>>();
            let (s, e) = slice_bounds(chars.len(), start, end);
            Ok(ZqValue::String(chars[s..e].iter().collect()))
        }
        ZqValue::Null => Ok(ZqValue::Null),
        other => Err(format!("Cannot index {} with object", type_name(&other))),
    }
}

fn run_dynamic_index(
    indexed: ZqValue,
    key_op: &Op,
    key_input: &ZqValue,
    optional: bool,
    out: &mut Vec<ZqValue>,
) -> Result<(), String> {
    let mut keys = eval_many(key_op, key_input)?;
    let Some(last_key) = keys.pop() else {
        return Ok(());
    };

    for dynamic_key in keys {
        let res = jq_get_dynamic(indexed.clone(), dynamic_key);
        if optional {
            if let Ok(value) = res {
                out.push(value);
            }
        } else {
            out.push(res?);
        }
    }

    let res = jq_get_dynamic(indexed, last_key);
    if optional {
        if let Ok(value) = res {
            out.push(value);
        }
    } else {
        out.push(res?);
    }
    Ok(())
}

fn slice_bounds(len: usize, start: Option<i64>, end: Option<i64>) -> (usize, usize) {
    c_math::slice_bounds_from_f64_like_jq(len, start.map(|v| v as f64), end.map(|v| v as f64))
}

fn unary_negate(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => {
            if let Some(special) = c_math::negate_special_number_literal(&n) {
                return Ok(ZqValue::Number(special));
            }
            // Keep exact decimal/integer textual form for finite JSON numbers.
            let raw = n.to_string();
            let negated_raw = if let Some(rest) = raw.strip_prefix('-') {
                rest.to_string()
            } else {
                format!("-{raw}")
            };
            if raw.contains('e') || raw.contains('E') {
                return Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(
                    negated_raw,
                )));
            }
            if let Ok(serde_json::Value::Number(number)) =
                serde_json::from_str::<serde_json::Value>(&negated_raw)
            {
                return Ok(ZqValue::Number(number));
            }
            let value = c_math::jq_number_to_f64_lossy(&n)
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(c_math::number_to_value(-value))
        }
        other => Err(format!(
            "{} ({}) cannot be negated",
            type_name(&other),
            value_for_error(&other)
        )),
    }
}

fn jq_truthy(value: &ZqValue) -> bool {
    !matches!(value, ZqValue::Null | ZqValue::Bool(false))
}

fn jq_cmp(lhs: &ZqValue, rhs: &ZqValue) -> Ordering {
    c_value::compare_jq(lhs, rhs)
}

fn type_name(value: &ZqValue) -> &'static str {
    c_value::type_name_jq(value)
}

fn value_for_error(value: &ZqValue) -> String {
    c_value::value_for_error_jq(value)
}

#[cfg(test)]
mod tests;
