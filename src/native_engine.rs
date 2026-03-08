use crate::value::ZqValue;
#[cfg(test)]
use serde_json::Value as RawJsonValue;
use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    pub null_input: bool,
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
            .send(VmRunTask {
                program,
                inputs,
                run_options,
                result_tx,
            })
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
        result_rx
            .recv()
            .map_err(|_| "native VM worker disconnected".to_string())?
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
    let VmRunTask {
        program,
        inputs,
        run_options,
        result_tx,
    } = task;
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
    program.execute_slice_native(&inputs, run_options, &mut emit)?;
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
    vm_core::compile(query)
        .ok()
        .map(|program| CompiledProgram { program })
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
    let native_inputs = inputs
        .iter()
        .cloned()
        .map(ZqValue::from_json)
        .collect::<Vec<_>>();
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

pub fn try_execute_native_with_paths(
    query: &str,
    inputs: &[ZqValue],
    library_paths: &[String],
    run_options: RunOptions,
) -> TryExecuteNative {
    let Some(program) = try_compile_with_paths(query, library_paths) else {
        return TryExecuteNative::Unsupported;
    };
    match execute_slice_native_collect_on_large_stack(program, inputs.to_vec(), run_options) {
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
    let native_inputs = inputs
        .iter()
        .cloned()
        .map(ZqValue::from_json)
        .collect::<Vec<_>>();
    try_execute_stream_native(query, &native_inputs, run_options, |value| {
        emit(value.into_json())
    })
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

impl CompiledProgram {
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
        self.execute_input_native(ZqValue::from_json(root), &mut |value| {
            emit(value.into_json())
        })
    }

    pub fn execute_input_native<F>(&self, root: ZqValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        for value in vm_core::execute(&self.program, &root)? {
            emit(value)?;
        }
        Ok(())
    }

    fn execute_input_native_prepared<F>(&self, root: ZqValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        for value in vm_core::execute_prepared(&self.program, root)? {
            emit(value)?;
        }
        Ok(())
    }

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
        let native_inputs = inputs
            .iter()
            .cloned()
            .map(ZqValue::from_json)
            .collect::<Vec<_>>();
        self.execute_slice_native(&native_inputs, run_options, &mut |value| {
            emit(value.into_json())
        })
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
        let digits = rest
            .bytes()
            .take_while(|b| b.is_ascii_digit())
            .collect::<Vec<_>>();
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
    // Deep jq-compatible expressions can build >10k nesting depth (for example,
    // reduce/range+flatten fixtures). Keep a dedicated large-stack worker pool
    // and submit jobs there instead of spawning a fresh thread per query.
    if should_parallelize_inputs(&program.program, &inputs, run_options) {
        return execute_slice_parallel_inputs(program, inputs);
    }
    vm_worker_pool()?.run(program, inputs, run_options)
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
        let rx = pool.submit(
            program.clone(),
            chunk,
            RunOptions { null_input: false },
        )?;
        results.push(rx);
    }

    let mut out = Vec::new();
    for rx in results {
        let mut batch = rx
            .recv()
            .map_err(|_| "native VM worker disconnected".to_string())??;
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
    program
        .branches
        .iter()
        .any(|branch| branch.ops.iter().any(op_uses_input))
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
        Op::ObjectLiteral(fields) => fields
            .iter()
            .any(|(key, value)| op_object_key_uses_input(key) || op_uses_input(value)),
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
        Op::RegexSub {
            regex,
            replacement,
            flags,
            ..
        } => op_uses_input(regex) || op_uses_input(replacement) || op_uses_input(flags),
        Op::SetPath(path, value)
        | Op::Modify(path, value)
        | Op::Any(path, value)
        | Op::All(path, value)
        | Op::While(path, value)
        | Op::Until(path, value) => op_uses_input(path) || op_uses_input(value),
        Op::Bind { source, body, .. } => op_uses_input(source) || op_uses_input(body),
        Op::DynamicIndex { key, .. } => op_uses_input(key),
        Op::Range(a, b, c) => op_uses_input(a) || op_uses_input(b) || op_uses_input(c),
        Op::Reduce {
            source,
            init,
            update,
            ..
        } => op_uses_input(source) || op_uses_input(init) || op_uses_input(update),
        Op::Foreach {
            source,
            init,
            update,
            extract,
            ..
        } => {
            op_uses_input(source)
                || op_uses_input(init)
                || op_uses_input(update)
                || op_uses_input(extract)
        }
        Op::TryCatch { inner, catcher } => op_uses_input(inner) || op_uses_input(catcher),
        Op::IfElse {
            cond,
            then_expr,
            else_expr,
        } => op_uses_input(cond) || op_uses_input(then_expr) || op_uses_input(else_expr),
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
        Op::RegexSub {
            regex,
            replacement,
            flags,
            ..
        } => {
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
        Op::Reduce {
            source,
            init,
            update,
            ..
        } => {
            op_uses_input_outside_fromstream(source, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(init, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(update, inside_fromstream, has_input)
        }
        Op::Foreach {
            source,
            init,
            update,
            extract,
            ..
        } => {
            op_uses_input_outside_fromstream(source, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(init, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(update, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(extract, inside_fromstream, has_input)
        }
        Op::TryCatch { inner, catcher } => {
            op_uses_input_outside_fromstream(inner, inside_fromstream, has_input)
                || op_uses_input_outside_fromstream(catcher, inside_fromstream, has_input)
        }
        Op::IfElse {
            cond,
            then_expr,
            else_expr,
        } => {
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

mod vm_core;
