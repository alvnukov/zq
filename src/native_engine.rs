mod doc_tape;
mod json_fast_path;

use crate::value::{
    install_active_native_value_recycle_context, NativeValueRecycleContext, ZqValue,
};
use serde::Deserialize;
#[cfg(test)]
use serde_json::Value as RawJsonValue;
use std::collections::{BTreeSet, VecDeque};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    pub null_input: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonWriteOptions {
    pub compact: bool,
    pub raw_output: bool,
    pub join_output: bool,
    pub indent: usize,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug)]
pub enum TryExecute {
    Unsupported,
    Executed(Result<Vec<RawJsonValue>, String>),
}

#[derive(Debug)]
pub enum TryExecuteNative {
    Unsupported,
    Executed(Result<Vec<ZqValue>, String>),
}

#[derive(Debug)]
pub enum TryExecuteStream {
    Unsupported,
    Executed(Result<(), String>),
}

#[derive(Debug, Clone)]
pub struct CompiledProgram {
    program: vm_core::ir::Program,
}

const VM_WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;
const VM_POOL_MAX_WORKERS: usize = 8;
const VM_POOL_ENV: &str = "ZQ_VM_POOL";
const PAR_EXEC_MIN_INPUTS: usize = 128;
const PAR_EXEC_OVERRIDE_ENV: &str = "ZQ_NATIVE_PAR";

type JsonInputReader = Box<dyn Read + Send>;
type JsonInputParser = serde_json::Deserializer<serde_json::de::IoRead<JsonInputReader>>;

struct VmRunTask {
    program: CompiledProgram,
    inputs: Vec<ZqValue>,
    run_options: RunOptions,
    result_tx: mpsc::Sender<Result<Vec<ZqValue>, String>>,
}

struct VmWorkerPool {
    sender: mpsc::Sender<VmRunTask>,
}

static VM_WORKER_POOL: OnceLock<Result<VmWorkerPool, String>> = OnceLock::new();

impl VmWorkerPool {
    fn new() -> Result<Self, String> {
        let worker_count = configured_vm_pool_size();
        let (sender, receiver) = mpsc::channel::<VmRunTask>();
        let receiver = Arc::new(Mutex::new(receiver));

        for worker_index in 0..worker_count {
            let worker_rx = Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("zq-native-vm-{worker_index}"))
                .stack_size(VM_WORKER_STACK_SIZE)
                .spawn(move || vm_worker_loop(worker_rx))
                .map_err(|err| format!("failed to start native VM worker pool: {err}"))?;
        }

        Ok(Self { sender })
    }

    fn submit(
        &self,
        program: CompiledProgram,
        inputs: Vec<ZqValue>,
        run_options: RunOptions,
    ) -> Result<mpsc::Receiver<Result<Vec<ZqValue>, String>>, String> {
        let (result_tx, result_rx) = mpsc::channel();
        self.sender
            .send(VmRunTask { program, inputs, run_options, result_tx })
            .map_err(|_| "native VM worker pool is unavailable".to_string())?;
        Ok(result_rx)
    }

    fn run(
        &self,
        program: CompiledProgram,
        inputs: Vec<ZqValue>,
        run_options: RunOptions,
    ) -> Result<Vec<ZqValue>, String> {
        let result_rx = self.submit(program, inputs, run_options)?;
        result_rx.recv().map_err(|_| "native VM worker disconnected".to_string())?
    }
}

fn vm_worker_loop(receiver: Arc<Mutex<mpsc::Receiver<VmRunTask>>>) {
    loop {
        let task = {
            let Ok(lock) = receiver.lock() else {
                return;
            };
            let Ok(task) = lock.recv() else {
                return;
            };
            task
        };
        run_vm_task(task);
    }
}

fn run_vm_task(task: VmRunTask) {
    let VmRunTask { program, inputs, run_options, result_tx } = task;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        execute_slice_native_inner(program, inputs, run_options)
    }))
    .unwrap_or_else(|_| Err("native VM worker panicked".to_string()));
    let _ = result_tx.send(result);
}

fn execute_slice_native_inner(
    program: CompiledProgram,
    inputs: Vec<ZqValue>,
    run_options: RunOptions,
) -> Result<Vec<ZqValue>, String> {
    let mut out = Vec::new();
    let mut emit = |value| {
        out.push(value);
        Ok(())
    };
    program.execute_slice_native_owned(inputs, run_options, &mut emit)?;
    Ok(out)
}

fn configured_vm_pool_size() -> usize {
    let default_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, VM_POOL_MAX_WORKERS);

    let Ok(raw) = std::env::var(VM_POOL_ENV) else {
        return default_workers;
    };
    let Some(parsed) = raw.trim().parse::<usize>().ok() else {
        return default_workers;
    };
    parsed.clamp(1, VM_POOL_MAX_WORKERS)
}

fn parse_parallel_override_mode() -> Option<bool> {
    let raw = std::env::var(PAR_EXEC_OVERRIDE_ENV).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Some(true),
        "0" | "off" | "false" => Some(false),
        _ => None,
    }
}

fn vm_worker_pool() -> Result<&'static VmWorkerPool, String> {
    match VM_WORKER_POOL.get_or_init(VmWorkerPool::new) {
        Ok(pool) => Ok(pool),
        Err(err) => Err(err.clone()),
    }
}

#[allow(dead_code)]
pub fn is_supported(query: &str) -> bool {
    vm_core::compile(query).is_ok()
}

pub fn try_compile_error(query: &str) -> Option<String> {
    vm_core::compile(query).err()
}

pub fn try_compile_error_with_paths(query: &str, library_paths: &[String]) -> Option<String> {
    if library_paths.is_empty() {
        return try_compile_error(query);
    }
    vm_core::compile_with_module_dirs(query, module_search_dirs_from_cli_paths(library_paths)).err()
}

pub fn try_compile(query: &str) -> Option<CompiledProgram> {
    vm_core::compile(query).ok().map(|program| CompiledProgram { program })
}

pub fn try_compile_with_paths(query: &str, library_paths: &[String]) -> Option<CompiledProgram> {
    if library_paths.is_empty() {
        return try_compile(query);
    }
    vm_core::compile_with_module_dirs(query, module_search_dirs_from_cli_paths(library_paths))
        .ok()
        .map(|program| CompiledProgram { program })
}

#[cfg(test)]
#[allow(dead_code)]
pub fn try_execute(query: &str, inputs: &[RawJsonValue], run_options: RunOptions) -> TryExecute {
    let native_inputs = inputs.iter().cloned().map(ZqValue::from_json).collect::<Vec<_>>();
    match try_execute_native(query, &native_inputs, run_options) {
        TryExecuteNative::Unsupported => TryExecute::Unsupported,
        TryExecuteNative::Executed(Ok(values)) => {
            TryExecute::Executed(Ok(values.into_iter().map(ZqValue::into_json).collect()))
        }
        TryExecuteNative::Executed(Err(err)) => TryExecute::Executed(Err(err)),
    }
}

#[allow(dead_code)]
pub fn try_execute_native(
    query: &str,
    inputs: &[ZqValue],
    run_options: RunOptions,
) -> TryExecuteNative {
    let Some(program) = try_compile(query) else {
        return TryExecuteNative::Unsupported;
    };
    match execute_slice_native_collect_on_large_stack(program, inputs.to_vec(), run_options) {
        Ok(values) => TryExecuteNative::Executed(Ok(values)),
        Err(err) => TryExecuteNative::Executed(Err(err)),
    }
}

#[allow(dead_code)]
pub fn try_execute_native_with_paths(
    query: &str,
    inputs: &[ZqValue],
    library_paths: &[String],
    run_options: RunOptions,
) -> TryExecuteNative {
    try_execute_native_with_paths_owned(query, inputs.to_vec(), library_paths, run_options)
}

pub fn try_execute_native_with_paths_owned(
    query: &str,
    inputs: Vec<ZqValue>,
    library_paths: &[String],
    run_options: RunOptions,
) -> TryExecuteNative {
    let Some(program) = try_compile_with_paths(query, library_paths) else {
        return TryExecuteNative::Unsupported;
    };
    match execute_slice_native_collect_on_large_stack(program, inputs, run_options) {
        Ok(values) => TryExecuteNative::Executed(Ok(values)),
        Err(err) => TryExecuteNative::Executed(Err(err)),
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub fn try_execute_stream<F>(
    query: &str,
    inputs: &[RawJsonValue],
    run_options: RunOptions,
    mut emit: F,
) -> TryExecuteStream
where
    F: FnMut(RawJsonValue) -> Result<(), String>,
{
    let native_inputs = inputs.iter().cloned().map(ZqValue::from_json).collect::<Vec<_>>();
    try_execute_stream_native(query, &native_inputs, run_options, |value| emit(value.into_json()))
}

pub fn try_execute_stream_native<F>(
    query: &str,
    inputs: &[ZqValue],
    run_options: RunOptions,
    mut emit: F,
) -> TryExecuteStream
where
    F: FnMut(ZqValue) -> Result<(), String>,
{
    let Some(program) = try_compile(query) else {
        return TryExecuteStream::Unsupported;
    };

    match execute_slice_native_collect_on_large_stack(program, inputs.to_vec(), run_options) {
        Ok(values) => {
            for value in values {
                if let Err(err) = emit(value) {
                    return TryExecuteStream::Executed(Err(err));
                }
            }
            TryExecuteStream::Executed(Ok(()))
        }
        Err(err) => TryExecuteStream::Executed(Err(err)),
    }
}

pub fn decode_halt_error(err: &str) -> Option<(i32, String)> {
    vm_core::decode_halt_error(err)
}

pub fn supports_direct_json_stream_write(query: &str) -> bool {
    try_compile(query).is_some_and(|program| program.supports_direct_json_stream_write())
}

impl CompiledProgram {
    pub(crate) fn uses_input_op(&self) -> bool {
        program_uses_input_op(&self.program)
    }

    pub(crate) fn uses_input_stream_metadata(&self) -> bool {
        program_uses_input_stream_metadata(&self.program)
    }

    pub(crate) fn reads_inputs_as_stream_events(&self) -> bool {
        program_reads_inputs_as_stream_events(&self.program)
    }

    pub(crate) fn supports_direct_json_stream_write(&self) -> bool {
        !self.uses_input_op()
            && !self.uses_input_stream_metadata()
            && json_fast_path::FastProgram::compile(&self.program).is_some()
    }

    pub(crate) fn execute_json_text_stream_auto_native<F>(
        &self,
        input: &str,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        if self.uses_input_op() {
            return self.execute_json_text_stream_with_inputs_native(input, emit);
        }
        if self.uses_input_stream_metadata() {
            return self.execute_json_text_stream_with_metadata_native(input, emit);
        }
        self.execute_json_text_stream_native(input, emit)
    }

    pub(crate) fn execute_json_reader_stream_auto_native<F>(
        &self,
        reader: JsonInputReader,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        if self.uses_input_op() {
            return self.execute_json_reader_stream_with_inputs_native(reader, emit);
        }
        if self.uses_input_stream_metadata() {
            return self.execute_json_reader_stream_with_metadata_native(reader, emit);
        }
        self.execute_json_reader_stream_native(reader, emit)
    }

    pub(crate) fn execute_json_text_stream_native<F>(
        &self,
        input: &str,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        if let Some(plan) = json_fast_path::FastProgram::compile(&self.program) {
            return plan.execute_json_text_stream(input, emit);
        }
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let mut recycle_ctx = NativeValueRecycleContext::default();
        let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
        for item in serde_json::Deserializer::from_str(input).into_iter::<ZqValue>() {
            let root = item.map_err(|e| format!("json parse error: {e}"))?;
            vm_core::execute_prepared_with(&self.program, root, emit)?;
        }
        Ok(())
    }

    fn execute_json_reader_stream_native<F>(
        &self,
        reader: JsonInputReader,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        if let Some(plan) = json_fast_path::FastProgram::compile(&self.program) {
            return plan.execute_json_reader_stream(reader, emit);
        }
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let mut recycle_ctx = NativeValueRecycleContext::default();
        let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
        let mut parser = serde_json::Deserializer::from_reader(reader);
        loop {
            match ZqValue::deserialize(&mut parser) {
                Ok(root) => vm_core::execute_prepared_with(&self.program, root, emit)?,
                Err(err) if err.is_eof() => break,
                Err(err) => return Err(format!("json parse error: {err}")),
            }
        }
        Ok(())
    }

    pub(crate) fn execute_json_reader_stream_direct_write<W: Write>(
        &self,
        reader: JsonInputReader,
        writer: &mut W,
        options: JsonWriteOptions,
    ) -> Result<(), String> {
        if !self.supports_direct_json_stream_write() {
            return Err("query is not supported by direct json stream writer".to_string());
        }
        let plan = json_fast_path::FastProgram::compile(&self.program)
            .ok_or_else(|| "query is not supported by direct json stream writer".to_string())?;
        plan.execute_json_reader_stream_write_json(reader, writer, options)
    }

    fn execute_json_text_stream_with_inputs_native<F>(
        &self,
        input: &str,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let reads_as_events = self.reads_inputs_as_stream_events();
        let (first, remaining) = parse_first_json_stream_value(input)?;
        let has_first = first.is_some();
        let root = first.unwrap_or(ZqValue::Null);
        let replay = if reads_as_events && has_first { vec![root.clone()] } else { Vec::new() };
        let _input_guard = vm_core::install_input_stream_json_text(remaining, replay, has_first);
        let cursor_start = if reads_as_events { 0 } else { usize::from(has_first) };
        vm_core::set_input_cursor(cursor_start);
        self.execute_input_native_prepared(root, emit)
    }

    fn execute_json_reader_stream_with_inputs_native<F>(
        &self,
        reader: JsonInputReader,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let reads_as_events = self.reads_inputs_as_stream_events();
        let (first, parser) = parse_first_json_stream_value_reader(reader)?;
        let has_first = first.is_some();
        let root = first.unwrap_or(ZqValue::Null);
        let replay = if reads_as_events && has_first { vec![root.clone()] } else { Vec::new() };
        let _input_guard = vm_core::install_input_stream_json_parser(parser, replay, has_first);
        let cursor_start = if reads_as_events { 0 } else { usize::from(has_first) };
        vm_core::set_input_cursor(cursor_start);
        self.execute_input_native_prepared(root, emit)
    }

    fn execute_json_text_stream_with_metadata_native<F>(
        &self,
        input: &str,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let _input_guard = vm_core::install_input_metadata_context();
        let mut recycle_ctx = NativeValueRecycleContext::default();
        let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
        let values = serde_json::Deserializer::from_str(input).into_iter::<ZqValue>();
        for (index, item) in values.enumerate() {
            vm_core::set_input_cursor(index);
            let root = item.map_err(|e| format!("json parse error: {e}"))?;
            self.execute_input_native_prepared(root, emit)?;
        }
        Ok(())
    }

    fn execute_json_reader_stream_with_metadata_native<F>(
        &self,
        reader: JsonInputReader,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let _program_context_guard = vm_core::install_program_context(&self.program);
        let _input_guard = vm_core::install_input_metadata_context();
        let mut recycle_ctx = NativeValueRecycleContext::default();
        let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
        let mut parser = serde_json::Deserializer::from_reader(reader);
        let mut index = 0usize;
        loop {
            match ZqValue::deserialize(&mut parser) {
                Ok(root) => {
                    vm_core::set_input_cursor(index);
                    index += 1;
                    self.execute_input_native_prepared(root, emit)?;
                }
                Err(err) if err.is_eof() => break,
                Err(err) => return Err(format!("json parse error: {err}")),
            }
        }
        Ok(())
    }

    pub(crate) fn debug_disasm_function_labels(&self) -> Vec<String> {
        let reachable = reachable_function_ids(&self.program);
        self.program
            .functions
            .iter()
            .filter(|func| reachable.contains(&func.id))
            .map(|func| format!("{}:{}:", func.name, func.id))
            .collect()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn execute_input<F>(&self, root: RawJsonValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(RawJsonValue) -> Result<(), String>,
    {
        self.execute_input_native(ZqValue::from_json(root), &mut |value| emit(value.into_json()))
    }

    pub fn execute_input_native<F>(&self, root: ZqValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let _program_context_guard = vm_core::install_program_context(&self.program);
        vm_core::execute_prepared_with(&self.program, root, emit)
    }

    fn execute_input_native_prepared<F>(&self, root: ZqValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        vm_core::execute_prepared_with(&self.program, root, emit)
    }

    #[allow(dead_code)]
    pub fn execute_slice_native<F>(
        &self,
        inputs: &[ZqValue],
        run_options: RunOptions,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        self.execute_slice_native_owned(inputs.to_vec(), run_options, emit)
    }

    pub fn execute_slice_native_owned<F>(
        &self,
        inputs: Vec<ZqValue>,
        run_options: RunOptions,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let uses_input_op = program_uses_input_op(&self.program);
        let uses_input_metadata = program_uses_input_stream_metadata(&self.program);
        let uses_input_stream_state = uses_input_op || uses_input_metadata;
        let _program_context_guard = vm_core::install_program_context(&self.program);

        if run_options.null_input {
            let _input_guard =
                uses_input_stream_state.then(|| vm_core::install_input_stream(&inputs));
            if uses_input_stream_state {
                vm_core::set_input_cursor(0);
            }
            self.execute_input_native_prepared(ZqValue::Null, emit)?;
            return Ok(());
        }

        if uses_input_stream_state {
            let _input_guard = vm_core::install_input_stream(&inputs);
            if uses_input_op {
                let root = inputs.first().cloned().unwrap_or(ZqValue::Null);
                let cursor_start = if program_reads_inputs_as_stream_events(&self.program) {
                    0
                } else {
                    usize::from(!inputs.is_empty())
                };
                vm_core::set_input_cursor(cursor_start);
                self.execute_input_native_prepared(root, emit)?;
                return Ok(());
            }
            for (index, input) in inputs.into_iter().enumerate() {
                vm_core::set_input_cursor(index);
                self.execute_input_native_prepared(input, emit)?;
            }
            return Ok(());
        }

        for input in inputs {
            self.execute_input_native_prepared(input, emit)?;
        }
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn execute_slice<F>(
        &self,
        inputs: &[RawJsonValue],
        run_options: RunOptions,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(RawJsonValue) -> Result<(), String>,
    {
        let native_inputs = inputs.iter().cloned().map(ZqValue::from_json).collect::<Vec<_>>();
        self.execute_slice_native(&native_inputs, run_options, &mut |value| emit(value.into_json()))
    }
}

fn parse_first_json_stream_value(input: &str) -> Result<(Option<ZqValue>, &str), String> {
    let mut recycle_ctx = NativeValueRecycleContext::default();
    let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
    let mut values = serde_json::Deserializer::from_str(input).into_iter::<ZqValue>();
    let Some(first) = values.next() else {
        return Ok((None, input));
    };
    let first = first.map_err(|e| format!("json parse error: {e}"))?;
    let offset = values.byte_offset();
    Ok((Some(first), &input[offset..]))
}

fn parse_first_json_stream_value_reader(
    reader: JsonInputReader,
) -> Result<(Option<ZqValue>, JsonInputParser), String> {
    let mut recycle_ctx = NativeValueRecycleContext::default();
    let _recycle_guard = install_active_native_value_recycle_context(&mut recycle_ctx);
    let mut parser = serde_json::Deserializer::from_reader(reader);
    match ZqValue::deserialize(&mut parser) {
        Ok(first) => Ok((Some(first), parser)),
        Err(err) if err.is_eof() => Ok((None, parser)),
        Err(err) => Err(format!("json parse error: {err}")),
    }
}

fn reachable_function_ids(program: &vm_core::ir::Program) -> BTreeSet<usize> {
    let mut pending = VecDeque::new();
    for branch in &program.branches {
        for op in &branch.ops {
            enqueue_call_ids_from_debug_repr(&format!("{op:?}"), &mut pending);
        }
    }

    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop_front() {
        if !seen.insert(id) {
            continue;
        }
        if let Some(function) = program.functions.iter().find(|func| func.id == id) {
            enqueue_call_ids_from_debug_repr(&format!("{:?}", function.body), &mut pending);
        }
    }
    seen
}

fn enqueue_call_ids_from_debug_repr(debug: &str, pending: &mut VecDeque<usize>) {
    let needle = "function_id: Some(";
    let mut rest = debug;
    while let Some(pos) = rest.find(needle) {
        rest = &rest[(pos + needle.len())..];
        let digits = rest.bytes().take_while(|b| b.is_ascii_digit()).collect::<Vec<_>>();
        if digits.is_empty() {
            continue;
        }
        if let Ok(text) = std::str::from_utf8(&digits) {
            if let Ok(id) = text.parse::<usize>() {
                pending.push_back(id);
            }
        }
        rest = &rest[digits.len()..];
    }
}

fn execute_slice_native_collect_on_large_stack(
    program: CompiledProgram,
    inputs: Vec<ZqValue>,
    run_options: RunOptions,
) -> Result<Vec<ZqValue>, String> {
    if should_parallelize_inputs(&program.program, &inputs, run_options) {
        return execute_slice_parallel_inputs(program, inputs);
    }
    // Keep large-stack execution only for recursive programs that can exceed
    // the default process stack; run common short filters inline.
    if program_requires_large_stack(&program.program) {
        return vm_worker_pool()?.run(program, inputs, run_options);
    }
    execute_slice_native_inner(program, inputs, run_options)
}

fn should_parallelize_inputs(
    program: &vm_core::ir::Program,
    inputs: &[ZqValue],
    run_options: RunOptions,
) -> bool {
    if run_options.null_input || inputs.len() < 2 {
        return false;
    }
    if program_uses_input_op(program) || program_uses_input_stream_metadata(program) {
        return false;
    }
    match parse_parallel_override_mode() {
        Some(force) => force,
        None => inputs.len() >= PAR_EXEC_MIN_INPUTS,
    }
}

fn program_requires_large_stack(program: &vm_core::ir::Program) -> bool {
    program.branches.iter().any(|branch| branch.ops.iter().any(op_requires_large_stack))
        || program.functions.iter().any(|function| op_requires_large_stack(&function.body))
}

fn op_requires_large_stack(op: &vm_core::ir::Op) -> bool {
    use vm_core::ir::Op;
    match op {
        Op::While(_, _)
        | Op::Until(_, _)
        | Op::Reduce { .. }
        | Op::Foreach { .. }
        | Op::RecurseBy(_)
        | Op::RecurseByCond(_, _)
        | Op::Walk(_)
        | Op::Combinations
        | Op::Repeat(_) => true,
        Op::Chain(items) | Op::Pipe(items) | Op::Comma(items) | Op::ArrayLiteral(items) => {
            items.iter().any(op_requires_large_stack)
        }
        Op::Call { args, .. } => args.iter().any(op_requires_large_stack),
        Op::ObjectLiteral(fields) => fields.iter().any(|(key, value)| {
            op_object_key_requires_large_stack(key) || op_requires_large_stack(value)
        }),
        Op::Has(arg)
        | Op::In(arg)
        | Op::StartsWith(arg)
        | Op::EndsWith(arg)
        | Op::Split(arg)
        | Op::Join(arg)
        | Op::LTrimStr(arg)
        | Op::RTrimStr(arg)
        | Op::TrimStr(arg)
        | Op::Indices(arg)
        | Op::IndexOf(arg)
        | Op::RIndexOf(arg)
        | Op::Contains(arg)
        | Op::Inside(arg)
        | Op::BSearch(arg)
        | Op::SortByImpl(arg)
        | Op::GroupByImpl(arg)
        | Op::UniqueByImpl(arg)
        | Op::MinByImpl(arg)
        | Op::MaxByImpl(arg)
        | Op::Path(arg)
        | Op::GetPath(arg)
        | Op::DelPaths(arg)
        | Op::TruncateStream(arg)
        | Op::FromStream(arg)
        | Op::Flatten(arg)
        | Op::FlattenRaw(arg)
        | Op::Nth(arg)
        | Op::FirstBy(arg)
        | Op::LastBy(arg)
        | Op::IsEmpty(arg)
        | Op::AddBy(arg)
        | Op::Select(arg)
        | Op::Map(arg)
        | Op::MapValues(arg)
        | Op::WithEntries(arg)
        | Op::Format { expr: arg, .. }
        | Op::Strptime(arg)
        | Op::Error(arg)
        | Op::HaltError(arg)
        | Op::UnaryMinus(arg)
        | Op::UnaryNot(arg)
        | Op::Label { body: arg, .. } => op_requires_large_stack(arg),
        Op::RegexMatch { spec, flags, .. } | Op::RegexCapture { spec, flags, .. } => {
            op_requires_large_stack(spec) || flags.as_deref().is_some_and(op_requires_large_stack)
        }
        Op::RegexScan { regex, flags } | Op::RegexSplits { regex, flags } => {
            op_requires_large_stack(regex) || flags.as_deref().is_some_and(op_requires_large_stack)
        }
        Op::RegexSub { regex, replacement, flags, .. } => {
            op_requires_large_stack(regex)
                || op_requires_large_stack(replacement)
                || op_requires_large_stack(flags)
        }
        Op::SetPath(path, value)
        | Op::Modify(path, value)
        | Op::Any(path, value)
        | Op::All(path, value)
        | Op::LimitBy(path, value)
        | Op::SkipBy(path, value)
        | Op::NthBy(path, value) => op_requires_large_stack(path) || op_requires_large_stack(value),
        Op::Bind { source, body, .. } => {
            op_requires_large_stack(source) || op_requires_large_stack(body)
        }
        Op::DynamicIndex { key, .. } => op_requires_large_stack(key),
        Op::Range(a, b, c) => {
            op_requires_large_stack(a) || op_requires_large_stack(b) || op_requires_large_stack(c)
        }
        Op::TryCatch { inner, catcher } => {
            op_requires_large_stack(inner) || op_requires_large_stack(catcher)
        }
        Op::IfElse { cond, then_expr, else_expr } => {
            op_requires_large_stack(cond)
                || op_requires_large_stack(then_expr)
                || op_requires_large_stack(else_expr)
        }
        Op::Binary { lhs, rhs, .. } | Op::MathBinary { lhs, rhs, .. } => {
            op_requires_large_stack(lhs) || op_requires_large_stack(rhs)
        }
        Op::MathTernary { a, b, c, .. } => {
            op_requires_large_stack(a) || op_requires_large_stack(b) || op_requires_large_stack(c)
        }
        Op::Strftime { format, .. } => op_requires_large_stack(format),
        _ => false,
    }
}

fn execute_slice_parallel_inputs(
    program: CompiledProgram,
    inputs: Vec<ZqValue>,
) -> Result<Vec<ZqValue>, String> {
    let pool = vm_worker_pool()?;
    let worker_slots = configured_vm_pool_size();
    let task_count = inputs.len().min(worker_slots.saturating_mul(2).max(1));
    let chunk_size = inputs.len().div_ceil(task_count);

    let mut results = Vec::with_capacity(task_count);
    let mut iter = inputs.into_iter();
    loop {
        let chunk = iter.by_ref().take(chunk_size).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let rx = pool.submit(program.clone(), chunk, RunOptions { null_input: false })?;
        results.push(rx);
    }

    let mut out = Vec::new();
    for rx in results {
        let mut batch = rx.recv().map_err(|_| "native VM worker disconnected".to_string())??;
        out.append(&mut batch);
    }
    Ok(out)
}

fn module_search_dirs_from_cli_paths(library_paths: &[String]) -> Vec<PathBuf> {
    let mut dirs = Vec::with_capacity(library_paths.len() + 1);
    dirs.push(PathBuf::from("."));
    for path in library_paths {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = PathBuf::from(trimmed);
        if dirs.iter().any(|seen| seen == &candidate) {
            continue;
        }
        dirs.push(candidate);
    }
    dirs
}

fn program_uses_input_op(program: &vm_core::ir::Program) -> bool {
    program.branches.iter().any(|branch| branch.ops.iter().any(op_uses_input))
}

fn program_uses_input_stream_metadata(program: &vm_core::ir::Program) -> bool {
    program.branches.iter().any(|branch| {
        branch.ops.iter().any(|op| {
            let debug = format!("{op:?}");
            debug.contains("InputLineNumber") || debug.contains("InputFilename")
        })
    })
}

fn program_reads_inputs_as_stream_events(program: &vm_core::ir::Program) -> bool {
    let mut has_input = false;
    for branch in &program.branches {
        for op in &branch.ops {
            if op_uses_input_outside_fromstream(op, false, &mut has_input) {
                return false;
            }
        }
    }
    has_input
}

fn op_uses_input(op: &vm_core::ir::Op) -> bool {
    use vm_core::ir::Op;
    match op {
        Op::Input => true,
        Op::Chain(items) | Op::Pipe(items) | Op::Comma(items) | Op::ArrayLiteral(items) => {
            items.iter().any(op_uses_input)
        }
        Op::Call { args, .. } => args.iter().any(op_uses_input),
        Op::ObjectLiteral(fields) => {
            fields.iter().any(|(key, value)| op_object_key_uses_input(key) || op_uses_input(value))
        }
        Op::Has(arg)
        | Op::In(arg)
        | Op::StartsWith(arg)
        | Op::EndsWith(arg)
        | Op::Split(arg)
        | Op::Join(arg)
        | Op::LTrimStr(arg)
        | Op::RTrimStr(arg)
        | Op::TrimStr(arg)
        | Op::Indices(arg)
        | Op::IndexOf(arg)
        | Op::RIndexOf(arg)
        | Op::Contains(arg)
        | Op::Inside(arg)
        | Op::BSearch(arg)
        | Op::SortByImpl(arg)
        | Op::GroupByImpl(arg)
        | Op::UniqueByImpl(arg)
        | Op::MinByImpl(arg)
        | Op::MaxByImpl(arg)
        | Op::Path(arg)
        | Op::GetPath(arg)
        | Op::DelPaths(arg)
        | Op::TruncateStream(arg)
        | Op::FromStream(arg)
        | Op::Flatten(arg)
        | Op::FlattenRaw(arg)
        | Op::Nth(arg)
        | Op::FirstBy(arg)
        | Op::LastBy(arg)
        | Op::IsEmpty(arg)
        | Op::AddBy(arg)
        | Op::Select(arg)
        | Op::Map(arg)
        | Op::MapValues(arg)
        | Op::WithEntries(arg)
        | Op::RecurseBy(arg)
        | Op::Walk(arg)
        | Op::Repeat(arg)
        | Op::Format { expr: arg, .. }
        | Op::Strptime(arg)
        | Op::Error(arg)
        | Op::HaltError(arg)
        | Op::UnaryMinus(arg)
        | Op::UnaryNot(arg)
        | Op::Label { body: arg, .. } => op_uses_input(arg),
        Op::RegexMatch { spec, flags, .. } | Op::RegexCapture { spec, flags, .. } => {
            op_uses_input(spec) || flags.as_deref().is_some_and(op_uses_input)
        }
        Op::RegexScan { regex, flags } | Op::RegexSplits { regex, flags } => {
            op_uses_input(regex) || flags.as_deref().is_some_and(op_uses_input)
        }
        Op::RegexSub { regex, replacement, flags, .. } => {
            op_uses_input(regex) || op_uses_input(replacement) || op_uses_input(flags)
        }
        Op::SetPath(path, value)
        | Op::Modify(path, value)
        | Op::Any(path, value)
        | Op::All(path, value)
        | Op::While(path, value)
        | Op::Until(path, value) => op_uses_input(path) || op_uses_input(value),
        Op::Bind { source, body, .. } => op_uses_input(source) || op_uses_input(body),
        Op::DynamicIndex { key, .. } => op_uses_input(key),
        Op::Range(a, b, c) => op_uses_input(a) || op_uses_input(b) || op_uses_input(c),
        Op::Reduce { source, init, update, .. } => {
            op_uses_input(source) || op_uses_input(init) || op_uses_input(update)
        }
        Op::Foreach { source, init, update, extract, .. } => {
            op_uses_input(source)
                || op_uses_input(init)
                || op_uses_input(update)
                || op_uses_input(extract)
        }
        Op::TryCatch { inner, catcher } => op_uses_input(inner) || op_uses_input(catcher),
        Op::IfElse { cond, then_expr, else_expr } => {
            op_uses_input(cond) || op_uses_input(then_expr) || op_uses_input(else_expr)
        }
        Op::Binary { lhs, rhs, .. } | Op::MathBinary { lhs, rhs, .. } => {
            op_uses_input(lhs) || op_uses_input(rhs)
        }
        Op::MathTernary { a, b, c, .. } => op_uses_input(a) || op_uses_input(b) || op_uses_input(c),
        Op::Strftime { format, .. } => op_uses_input(format),
        _ => false,
    }
}

fn op_uses_input_outside_fromstream(
    op: &vm_core::ir::Op,
    inside_fromstream: bool,
    has_input: &mut bool,
) -> bool {
    use vm_core::ir::Op;
    match op {
        Op::Input => {
            *has_input = true;
            !inside_fromstream
        }
        Op::FromStream(arg) => op_uses_input_outside_fromstream(arg, true, has_input),
        Op::Chain(items) | Op::Pipe(items) | Op::Comma(items) | Op::ArrayLiteral(items) => items
            .iter()
            .any(|item| op_uses_input_outside_fromstream(item, inside_fromstream, has_input)),
        Op::Call { args, .. } => args
            .iter()
            .any(|arg| op_uses_input_outside_fromstream(arg, inside_fromstream, has_input)),
        Op::ObjectLiteral(fields) => fields.iter().any(|(key, value)| {
            op_object_key_uses_input_outside_fromstream(key, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(value, inside_fromstream, has_input)
        }),
        Op::Has(arg)
        | Op::In(arg)
        | Op::StartsWith(arg)
        | Op::EndsWith(arg)
        | Op::Split(arg)
        | Op::Join(arg)
        | Op::LTrimStr(arg)
        | Op::RTrimStr(arg)
        | Op::TrimStr(arg)
        | Op::Indices(arg)
        | Op::IndexOf(arg)
        | Op::RIndexOf(arg)
        | Op::Contains(arg)
        | Op::Inside(arg)
        | Op::BSearch(arg)
        | Op::SortByImpl(arg)
        | Op::GroupByImpl(arg)
        | Op::UniqueByImpl(arg)
        | Op::MinByImpl(arg)
        | Op::MaxByImpl(arg)
        | Op::Path(arg)
        | Op::GetPath(arg)
        | Op::DelPaths(arg)
        | Op::TruncateStream(arg)
        | Op::Flatten(arg)
        | Op::FlattenRaw(arg)
        | Op::Nth(arg)
        | Op::FirstBy(arg)
        | Op::LastBy(arg)
        | Op::IsEmpty(arg)
        | Op::AddBy(arg)
        | Op::Select(arg)
        | Op::Map(arg)
        | Op::MapValues(arg)
        | Op::WithEntries(arg)
        | Op::RecurseBy(arg)
        | Op::Walk(arg)
        | Op::Repeat(arg)
        | Op::Format { expr: arg, .. }
        | Op::Strptime(arg)
        | Op::Error(arg)
        | Op::HaltError(arg)
        | Op::UnaryMinus(arg)
        | Op::UnaryNot(arg)
        | Op::Label { body: arg, .. } => {
            op_uses_input_outside_fromstream(arg, inside_fromstream, has_input)
        }
        Op::RegexMatch { spec, flags, .. } | Op::RegexCapture { spec, flags, .. } => {
            op_uses_input_outside_fromstream(spec, inside_fromstream, has_input)
                || flags.as_deref().is_some_and(|value| {
                    op_uses_input_outside_fromstream(value, inside_fromstream, has_input)
                })
        }
        Op::RegexScan { regex, flags } | Op::RegexSplits { regex, flags } => {
            op_uses_input_outside_fromstream(regex, inside_fromstream, has_input)
                || flags.as_deref().is_some_and(|value| {
                    op_uses_input_outside_fromstream(value, inside_fromstream, has_input)
                })
        }
        Op::RegexSub { regex, replacement, flags, .. } => {
            op_uses_input_outside_fromstream(regex, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(replacement, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(flags, inside_fromstream, has_input)
        }
        Op::SetPath(path, value)
        | Op::Modify(path, value)
        | Op::Any(path, value)
        | Op::All(path, value)
        | Op::While(path, value)
        | Op::Until(path, value) => {
            op_uses_input_outside_fromstream(path, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(value, inside_fromstream, has_input)
        }
        Op::Bind { source, body, .. } => {
            op_uses_input_outside_fromstream(source, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(body, inside_fromstream, has_input)
        }
        Op::DynamicIndex { key, .. } => {
            op_uses_input_outside_fromstream(key, inside_fromstream, has_input)
        }
        Op::Range(a, b, c) => {
            op_uses_input_outside_fromstream(a, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(b, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(c, inside_fromstream, has_input)
        }
        Op::Reduce { source, init, update, .. } => {
            op_uses_input_outside_fromstream(source, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(init, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(update, inside_fromstream, has_input)
        }
        Op::Foreach { source, init, update, extract, .. } => {
            op_uses_input_outside_fromstream(source, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(init, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(update, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(extract, inside_fromstream, has_input)
        }
        Op::TryCatch { inner, catcher } => {
            op_uses_input_outside_fromstream(inner, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(catcher, inside_fromstream, has_input)
        }
        Op::IfElse { cond, then_expr, else_expr } => {
            op_uses_input_outside_fromstream(cond, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(then_expr, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(else_expr, inside_fromstream, has_input)
        }
        Op::Binary { lhs, rhs, .. } | Op::MathBinary { lhs, rhs, .. } => {
            op_uses_input_outside_fromstream(lhs, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(rhs, inside_fromstream, has_input)
        }
        Op::MathTernary { a, b, c, .. } => {
            op_uses_input_outside_fromstream(a, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(b, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(c, inside_fromstream, has_input)
        }
        Op::Strftime { format, .. } => {
            op_uses_input_outside_fromstream(format, inside_fromstream, has_input)
        }
        _ => false,
    }
}

fn op_object_key_uses_input(key: &vm_core::ir::OpObjectKey) -> bool {
    match key {
        vm_core::ir::OpObjectKey::Static(_) => false,
        vm_core::ir::OpObjectKey::Expr(expr) => op_uses_input(expr),
    }
}

fn op_object_key_uses_input_outside_fromstream(
    key: &vm_core::ir::OpObjectKey,
    inside_fromstream: bool,
    has_input: &mut bool,
) -> bool {
    match key {
        vm_core::ir::OpObjectKey::Static(_) => false,
        vm_core::ir::OpObjectKey::Expr(expr) => {
            op_uses_input_outside_fromstream(expr, inside_fromstream, has_input)
        }
    }
}

fn op_object_key_requires_large_stack(key: &vm_core::ir::OpObjectKey) -> bool {
    match key {
        vm_core::ir::OpObjectKey::Static(_) => false,
        vm_core::ir::OpObjectKey::Expr(expr) => op_requires_large_stack(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_stack_detector_marks_recursive_queries() {
        let recurse = vm_core::compile("recurse(.a?)").expect("compile recurse");
        assert!(program_requires_large_stack(&recurse));

        let while_loop = vm_core::compile("while(. < 3; . + 1)").expect("compile while");
        assert!(program_requires_large_stack(&while_loop));
    }

    #[test]
    fn large_stack_detector_keeps_simple_queries_inline() {
        let add = vm_core::compile(".a + .b").expect("compile add");
        assert!(!program_requires_large_stack(&add));

        let project = vm_core::compile("{id,group,value}").expect("compile project");
        assert!(!program_requires_large_stack(&project));
    }

    #[test]
    fn large_stack_detector_scans_function_bodies() {
        let with_function =
            vm_core::compile("def walker: recurse(.a?); walker").expect("compile function");
        assert!(program_requires_large_stack(&with_function));
    }
}

mod vm_core;
