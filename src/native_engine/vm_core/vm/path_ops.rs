use super::*;

pub(super) fn run_getpath(input: ZqValue, path_value: ZqValue) -> Result<ZqValue, String> {
    let path = parse_path_array(path_value, "Path must be specified as an array")?;
    let mut current = input;
    for component in path {
        if matches!(current, ZqValue::Null) {
            continue;
        }
        current = jq_get_dynamic_ref(&current, &component)?;
    }
    Ok(current)
}

pub(super) fn run_setpath(
    input: ZqValue,
    path_value: ZqValue,
    new_value: ZqValue,
) -> Result<ZqValue, String> {
    let path = parse_path_array(path_value, "Path must be specified as an array")?;
    set_path_recursive(input, &path, new_value)
}

pub(super) fn run_modify(
    path_expr: &Op,
    update_expr: &Op,
    input: ZqValue,
) -> Result<ZqValue, String> {
    let path_values = run_path(path_expr, &input)?;
    let mut current = input;
    let mut pending_deletes: Vec<Vec<ZqValue>> = Vec::new();

    for path_value in path_values {
        let path = parse_path_array(path_value, "Path must be specified as an array")?;
        let old_value = run_getpath(current.clone(), ZqValue::Array(path.clone()))?;
        let updates = eval_many(update_expr, &old_value)?;
        if let Some(first) = updates.into_iter().next() {
            current = run_setpath(current, ZqValue::Array(path), first)?;
        } else {
            pending_deletes.push(path);
        }
    }

    if pending_deletes.is_empty() {
        Ok(current)
    } else {
        run_delpaths(
            current,
            ZqValue::Array(pending_deletes.into_iter().map(ZqValue::Array).collect()),
        )
    }
}

pub(super) fn run_delpaths(input: ZqValue, paths_value: ZqValue) -> Result<ZqValue, String> {
    let mut paths = parse_paths_array(paths_value)?;
    if paths.is_empty() {
        return Ok(input);
    }
    if paths.iter().any(|path| path.is_empty()) {
        return Ok(ZqValue::Null);
    }

    paths = canonicalize_delete_paths(&input, paths)?;
    paths.sort_by(|a, b| compare_delete_paths_desc(a.as_slice(), b.as_slice()));

    let mut current = input;
    for path in paths {
        let _ = delete_path_recursive(&mut current, &path)?;
    }
    Ok(current)
}

#[derive(Debug, Clone)]
struct PathTrace {
    path: Vec<ZqValue>,
    value: ZqValue,
}

pub(super) fn run_path(op: &Op, input: &ZqValue) -> Result<Vec<ZqValue>, String> {
    let traces = eval_path_expr(op, input.clone(), Vec::new(), input)?;
    Ok(traces.into_iter().map(|trace| ZqValue::Array(trace.path)).collect())
}

pub(super) fn run_paths_builtin(input: &ZqValue) -> Vec<ZqValue> {
    let mut out = Vec::new();
    let mut path = Vec::new();
    collect_all_paths(input, &mut path, &mut out);
    out
}

fn path_with_component(path: &[ZqValue], component: ZqValue) -> Vec<ZqValue> {
    let mut next = Vec::with_capacity(path.len() + 1);
    next.extend(path.iter().cloned());
    next.push(component);
    next
}

fn collect_all_paths(value: &ZqValue, path: &mut Vec<ZqValue>, out: &mut Vec<ZqValue>) {
    match value {
        ZqValue::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                path.push(ZqValue::from(idx as i64));
                out.push(ZqValue::Array(path.clone()));
                collect_all_paths(item, path, out);
                path.pop();
            }
        }
        ZqValue::Object(map) => {
            for (key, item) in map {
                path.push(ZqValue::String(key.clone()));
                out.push(ZqValue::Array(path.clone()));
                collect_all_paths(item, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn eval_path_expr(
    op: &Op,
    current: ZqValue,
    path: Vec<ZqValue>,
    root: &ZqValue,
) -> Result<Vec<PathTrace>, String> {
    match op {
        Op::Identity => Ok(vec![PathTrace { path, value: current }]),
        Op::Chain(steps) => eval_path_chain(steps, current, path, root),
        Op::Pipe(stages) => {
            let mut traces = vec![PathTrace { path, value: current }];
            for (idx, stage) in stages.iter().enumerate() {
                if path_stage_supported_for_tracing(stage) {
                    let mut next = Vec::new();
                    for trace in traces {
                        next.extend(eval_path_expr(stage, trace.value, trace.path, root)?);
                    }
                    traces = next;
                    continue;
                }

                let mut values = Vec::new();
                for trace in traces {
                    values.extend(eval_many(stage, &trace.value)?);
                }
                let sample = if values.is_empty() {
                    ZqValue::Array(Vec::new())
                } else if values.len() == 1 {
                    values.into_iter().next().expect("single value exists for len == 1")
                } else {
                    ZqValue::Array(values)
                };
                if let Some(next_stage) = stages.get(idx + 1) {
                    if let Some(message) = format_invalid_path_near(next_stage, &sample) {
                        return Err(message);
                    }
                }
                let rendered = if matches!(&sample, ZqValue::Array(items) if items.is_empty()) {
                    "empty".to_string()
                } else {
                    value_for_error(&sample)
                };
                return Err(format!("Invalid path expression with result {rendered}"));
            }
            Ok(traces)
        }
        Op::Call { function_id: None, param_id: Some(param_id), args, .. } if args.is_empty() => {
            let Some(arg_filter) = lookup_param_closure(*param_id, 0) else {
                return Err(format!("Invalid path expression with result ${param_id}"));
            };
            let _guard = push_bindings(arg_filter.bindings.clone());
            eval_path_expr(&arg_filter.op, current, path, root)
        }
        Op::Call { function_id: Some(function_id), name, args, .. } => {
            let arity = args.len();
            let Some(function) = lookup_function_by_id(*function_id) else {
                return Err(format!("{name}/{arity} is not defined"));
            };
            if function.param_ids.len() != arity {
                return Err(format!("{name}/{arity} is not defined"));
            }
            let captured_args: Vec<CapturedFilter> =
                args.iter().map(capture_call_argument).collect();
            let frame =
                CallFrame { params: function.param_ids.into_iter().zip(captured_args).collect() };
            let _call_frame_guard = push_call_frame(frame);
            eval_path_expr(&function.body, current, path, root)
        }
        Op::Bind { source, pattern, body } => {
            let source_values = eval_many(source, &current)?;
            let mut out = Vec::new();
            let mut source_iter = source_values.into_iter().peekable();
            let mut current_slot = Some(current);
            let mut path_slot = Some(path);
            while let Some(bound) = source_iter.next() {
                let Ok(bindings) = bind_pattern(pattern, &bound) else {
                    continue;
                };
                let _guard = push_bindings(bindings);
                let is_last = source_iter.peek().is_none();
                let next_current = if is_last {
                    current_slot.take().expect("current value still available")
                } else {
                    current_slot.as_ref().expect("current value still available").clone()
                };
                let next_path = if is_last {
                    path_slot.take().expect("path still available")
                } else {
                    path_slot.as_ref().expect("path still available").clone()
                };
                out.extend(eval_path_expr(body, next_current, next_path, root)?);
            }
            Ok(out)
        }
        Op::Comma(items) => {
            let mut out = Vec::new();
            let Some((last, head)) = items.split_last() else {
                return Ok(out);
            };
            for item in head {
                out.extend(eval_path_expr(item, current.clone(), path.clone(), root)?);
            }
            out.extend(eval_path_expr(last, current, path, root)?);
            Ok(out)
        }
        Op::Empty => Ok(Vec::new()),
        Op::RecurseBy(next) => eval_path_recurse(next, None, current, path, root),
        Op::RecurseByCond(next, cond) => {
            eval_path_recurse(next, Some(cond.as_ref()), current, path, root)
        }
        Op::Select(cond) => {
            let truthy_count = eval_many(cond, &current)?.into_iter().filter(jq_truthy).count();
            if truthy_count == 0 {
                return Ok(Vec::new());
            }
            let mut out = Vec::with_capacity(truthy_count);
            for _ in 1..truthy_count {
                out.push(PathTrace { path: path.clone(), value: current.clone() });
            }
            out.push(PathTrace { path, value: current });
            Ok(out)
        }
        Op::GetField { name, optional } => eval_path_field(current, path, name, *optional),
        Op::GetIndex { index, optional } => eval_path_index(current, path, *index, *optional),
        Op::GetPath(arg) => eval_path_getpath(current, path, arg),
        Op::Slice { start, end, optional } => {
            eval_path_slice(current, path, *start, *end, *optional)
        }
        Op::DynamicIndex { key, optional } => {
            eval_path_dynamic_index(current, path, key, *optional, root)
        }
        Op::Iterate { optional } => eval_path_iterate(current, path, *optional),
        Op::Builtin(Builtin::First) => eval_path_first(current, path),
        Op::Builtin(Builtin::Last) => eval_path_last(current, path),
        other => {
            let values = eval_many(other, &current)?;
            let rendered = if values.is_empty() {
                "empty".to_string()
            } else if values.len() == 1 {
                value_for_error(&values[0])
            } else {
                value_for_error(&ZqValue::Array(values))
            };
            Err(format!("Invalid path expression with result {rendered}"))
        }
    }
}

fn path_stage_supported_for_tracing(op: &Op) -> bool {
    match op {
        Op::Identity
        | Op::Chain(_)
        | Op::Pipe(_)
        | Op::Comma(_)
        | Op::Empty
        | Op::Select(_)
        | Op::GetField { .. }
        | Op::GetIndex { .. }
        | Op::GetPath(_)
        | Op::Slice { .. }
        | Op::DynamicIndex { .. }
        | Op::Iterate { .. }
        | Op::RecurseBy(_)
        | Op::RecurseByCond(_, _)
        | Op::Builtin(Builtin::First)
        | Op::Builtin(Builtin::Last)
        | Op::Bind { .. } => true,
        Op::Call { function_id: None, param_id: Some(_), args, .. } if args.is_empty() => true,
        Op::Call { function_id: Some(_), .. } => true,
        _ => false,
    }
}

fn eval_path_recurse(
    next: &Op,
    cond: Option<&Op>,
    current: ZqValue,
    path: Vec<ZqValue>,
    root: &ZqValue,
) -> Result<Vec<PathTrace>, String> {
    let mut out = Vec::new();
    eval_path_recurse_inner(next, cond, current, path, root, &mut out)?;
    Ok(out)
}

fn eval_path_recurse_inner(
    next: &Op,
    cond: Option<&Op>,
    current: ZqValue,
    path: Vec<ZqValue>,
    root: &ZqValue,
    out: &mut Vec<PathTrace>,
) -> Result<(), String> {
    out.push(PathTrace { path: path.clone(), value: current.clone() });
    let next_traces = eval_path_expr(next, current, path, root)?;
    for trace in next_traces {
        if let Some(cond) = cond {
            let cond_values = eval_many(cond, &trace.value)?;
            if !cond_values.into_iter().any(|v| jq_truthy(&v)) {
                continue;
            }
        }
        eval_path_recurse_inner(next, cond, trace.value, trace.path, root, out)?;
    }
    Ok(())
}

fn format_invalid_path_near(next: &Op, value: &ZqValue) -> Option<String> {
    match next {
        Op::Chain(steps) => steps.first().and_then(|first| format_invalid_path_near(first, value)),
        Op::Pipe(stages) => stages.first().and_then(|first| format_invalid_path_near(first, value)),
        Op::Comma(items) => items.first().and_then(|first| format_invalid_path_near(first, value)),
        Op::GetIndex { index, .. } => Some(format!(
            "Invalid path expression near attempt to access element {} of {}",
            index,
            value_for_error(value)
        )),
        Op::GetField { name, .. } => Some(format!(
            "Invalid path expression near attempt to access element \"{}\" of {}",
            name,
            value_for_error(value)
        )),
        Op::Slice { start, end, .. } => {
            let rendered = value_for_error(&slice_path_component_value(*start, *end));
            Some(format!(
                "Invalid path expression near attempt to access element {rendered} of {}",
                value_for_error(value)
            ))
        }
        Op::Iterate { .. } => Some(format!(
            "Invalid path expression near attempt to iterate through {}",
            value_for_error(value)
        )),
        _ => None,
    }
}

fn eval_path_chain(
    steps: &[Op],
    current: ZqValue,
    path: Vec<ZqValue>,
    root: &ZqValue,
) -> Result<Vec<PathTrace>, String> {
    let Some((first, rest)) = steps.split_first() else {
        return Ok(vec![PathTrace { path, value: current }]);
    };
    if !path_stage_supported_for_tracing(first) {
        let values = eval_many(first, &current)?;
        let sample = if values.is_empty() {
            ZqValue::Array(Vec::new())
        } else if values.len() == 1 {
            values.into_iter().next().expect("single value exists for len == 1")
        } else {
            ZqValue::Array(values)
        };
        if let Some(next_stage) = rest.first() {
            if let Some(message) = format_invalid_path_near(next_stage, &sample) {
                return Err(message);
            }
        }
        let rendered = if matches!(&sample, ZqValue::Array(items) if items.is_empty()) {
            "empty".to_string()
        } else {
            value_for_error(&sample)
        };
        return Err(format!("Invalid path expression with result {rendered}"));
    }
    let mut out = Vec::new();
    for trace in eval_path_expr(first, current, path, root)? {
        if rest.is_empty() {
            out.push(trace);
        } else {
            out.extend(eval_path_chain(rest, trace.value, trace.path, root)?);
        }
    }
    Ok(out)
}

fn eval_path_field(
    current: ZqValue,
    mut path: Vec<ZqValue>,
    name: &str,
    optional: bool,
) -> Result<Vec<PathTrace>, String> {
    let value = match current {
        ZqValue::Object(map) => map.get(name).cloned().unwrap_or(ZqValue::Null),
        ZqValue::Null => ZqValue::Null,
        other => {
            if optional {
                return Ok(Vec::new());
            }
            return Err(format!("Cannot index {} with string {:?}", type_name(&other), name));
        }
    };
    path.push(ZqValue::String(name.to_string()));
    Ok(vec![PathTrace { path, value }])
}

fn eval_path_index(
    current: ZqValue,
    mut path: Vec<ZqValue>,
    index: i64,
    optional: bool,
) -> Result<Vec<PathTrace>, String> {
    let value = match current {
        ZqValue::Array(values) => c_string::normalize_index_jq(values.len(), index)
            .and_then(|idx| values.get(idx).cloned())
            .unwrap_or(ZqValue::Null),
        ZqValue::String(text) => {
            c_string::string_index_like_jq(&text, index).unwrap_or(ZqValue::Null)
        }
        ZqValue::Null => ZqValue::Null,
        other => {
            if optional {
                return Ok(Vec::new());
            }
            return Err(format!("Cannot index {} with number ({})", type_name(&other), index));
        }
    };
    path.push(ZqValue::from(index));
    Ok(vec![PathTrace { path, value }])
}

fn eval_path_getpath(
    current: ZqValue,
    path: Vec<ZqValue>,
    arg: &Op,
) -> Result<Vec<PathTrace>, String> {
    let path_values = eval_many(arg, &current)?;
    let mut out = Vec::new();
    let mut path_values_iter = path_values.into_iter().peekable();
    let mut current_slot = Some(current);
    let mut path_slot = Some(path);
    while let Some(path_value) = path_values_iter.next() {
        let components = parse_path_array(path_value, "Path must be specified as an array")?;
        let is_last = path_values_iter.peek().is_none();
        let mut next_value = if is_last {
            current_slot.take().expect("current value still available")
        } else {
            current_slot.as_ref().expect("current value still available").clone()
        };
        let mut next_path = if is_last {
            path_slot.take().expect("path still available")
        } else {
            path_slot.as_ref().expect("path still available").clone()
        };
        next_path.reserve(components.len());
        for component in components {
            if !matches!(next_value, ZqValue::Null) {
                next_value = jq_get_dynamic_ref(&next_value, &component)?;
            }
            next_path.push(component);
        }
        out.push(PathTrace { path: next_path, value: next_value });
    }
    Ok(out)
}

fn eval_path_slice(
    current: ZqValue,
    mut path: Vec<ZqValue>,
    start: Option<i64>,
    end: Option<i64>,
    optional: bool,
) -> Result<Vec<PathTrace>, String> {
    let key = slice_path_component_value(start, end);
    match run_slice(current, start, end) {
        Ok(value) => {
            path.push(key);
            Ok(vec![PathTrace { path, value }])
        }
        Err(_err) if optional => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn slice_path_component_value(start: Option<i64>, end: Option<i64>) -> ZqValue {
    let mut key = IndexMap::new();
    key.insert("start".to_string(), start.map_or(ZqValue::Null, ZqValue::from));
    key.insert("end".to_string(), end.map_or(ZqValue::Null, ZqValue::from));
    ZqValue::Object(key)
}

fn eval_path_dynamic_index(
    current: ZqValue,
    path: Vec<ZqValue>,
    key: &Op,
    optional: bool,
    root: &ZqValue,
) -> Result<Vec<PathTrace>, String> {
    let keys = eval_many(key, root)?;
    let mut out = Vec::new();
    let mut keys_iter = keys.into_iter().peekable();
    let mut path_slot = Some(path);
    while let Some(key_value) = keys_iter.next() {
        if !matches!(key_value, ZqValue::String(_) | ZqValue::Number(_) | ZqValue::Object(_)) {
            let rendered = value_for_error(&key_value);
            return Err(format!(
                "Invalid path expression near attempt to access element {rendered} of {}",
                value_for_error(&current)
            ));
        }
        let value = jq_get_dynamic_ref(&current, &key_value);
        match value {
            Ok(value) => {
                let is_last = keys_iter.peek().is_none();
                let mut next_path = if is_last {
                    path_slot.take().expect("path still available")
                } else {
                    path_slot.as_ref().expect("path still available").clone()
                };
                next_path.push(key_value);
                out.push(PathTrace { path: next_path, value });
            }
            Err(err) if optional => {
                let _ = err;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}

fn eval_path_iterate(
    current: ZqValue,
    path: Vec<ZqValue>,
    optional: bool,
) -> Result<Vec<PathTrace>, String> {
    match current {
        ZqValue::Array(values) => Ok(values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| PathTrace {
                path: path_with_component(&path, ZqValue::from(idx as i64)),
                value,
            })
            .collect()),
        ZqValue::Object(map) => Ok(map
            .into_iter()
            .map(|(key, value)| PathTrace {
                path: path_with_component(&path, ZqValue::String(key)),
                value,
            })
            .collect()),
        other => {
            if optional {
                Ok(Vec::new())
            } else {
                Err(format!(
                    "Cannot iterate over {} ({})",
                    type_name(&other),
                    value_for_error(&other)
                ))
            }
        }
    }
}

fn eval_path_first(current: ZqValue, path: Vec<ZqValue>) -> Result<Vec<PathTrace>, String> {
    match current {
        ZqValue::Array(values) => {
            if let Some(first) = values.into_iter().next() {
                let mut next_path = path;
                next_path.push(ZqValue::from(0));
                Ok(vec![PathTrace { path: next_path, value: first }])
            } else {
                Ok(Vec::new())
            }
        }
        other => {
            if let Some(first) = iter_values_like_jq(other)?.into_iter().next() {
                Ok(vec![PathTrace { path, value: first }])
            } else {
                Ok(Vec::new())
            }
        }
    }
}

fn eval_path_last(current: ZqValue, path: Vec<ZqValue>) -> Result<Vec<PathTrace>, String> {
    match current {
        ZqValue::Array(values) => {
            if let Some(last) = values.into_iter().last() {
                let mut next_path = path;
                next_path.push(ZqValue::from(-1));
                Ok(vec![PathTrace { path: next_path, value: last }])
            } else {
                Ok(Vec::new())
            }
        }
        other => {
            let stream = iter_values_like_jq(other)?;
            if let Some(last) = stream.into_iter().last() {
                Ok(vec![PathTrace { path, value: last }])
            } else {
                Ok(Vec::new())
            }
        }
    }
}

fn parse_path_array(path_value: ZqValue, err_msg: &str) -> Result<Vec<ZqValue>, String> {
    match path_value {
        ZqValue::Array(path) => Ok(path),
        _ => Err(err_msg.to_string()),
    }
}

fn parse_paths_array(paths_value: ZqValue) -> Result<Vec<Vec<ZqValue>>, String> {
    let ZqValue::Array(paths) = paths_value else {
        return Err("Paths must be specified as an array".to_string());
    };
    paths
        .into_iter()
        .map(|path| match path {
            ZqValue::Array(path) => Ok(path),
            other => Err(format!("Path must be specified as array, not {}", type_name(&other))),
        })
        .collect()
}

fn set_path_recursive(
    value: ZqValue,
    path: &[ZqValue],
    new_value: ZqValue,
) -> Result<ZqValue, String> {
    let Some((head, tail)) = path.split_first() else {
        return Ok(new_value);
    };

    match head {
        ZqValue::String(key) => match value {
            ZqValue::Object(mut map) => {
                let child = map.shift_remove(key).unwrap_or(ZqValue::Null);
                let next = set_path_recursive(child, tail, new_value)?;
                map.insert(key.clone(), next);
                Ok(ZqValue::Object(map))
            }
            ZqValue::Null => {
                let next = set_path_recursive(ZqValue::Null, tail, new_value)?;
                let mut map = IndexMap::new();
                map.insert(key.clone(), next);
                Ok(ZqValue::Object(map))
            }
            other => Err(format!("Cannot index {} with string {:?}", type_name(&other), key)),
        },
        ZqValue::Number(index) => {
            let Some(raw) = c_math::path_number_for_set(index)? else {
                return Err("Cannot set array element at NaN index".to_string());
            };
            match value {
                ZqValue::Array(mut items) => {
                    let mut target = raw;
                    if target < 0 {
                        target += items.len() as i64;
                        if target < 0 {
                            return Err("Out of bounds negative array index".to_string());
                        }
                    }
                    let target = target as usize;
                    c_math::ensure_array_set_index_not_too_large(target)?;
                    if target >= items.len() {
                        items.resize(target + 1, ZqValue::Null);
                    }
                    let child = items[target].clone();
                    items[target] = set_path_recursive(child, tail, new_value)?;
                    Ok(ZqValue::Array(items))
                }
                ZqValue::Null => {
                    if raw < 0 {
                        return Err("Out of bounds negative array index".to_string());
                    }
                    let target = raw as usize;
                    c_math::ensure_array_set_index_not_too_large(target)?;
                    let mut items = vec![ZqValue::Null; target + 1];
                    let child = items[target].clone();
                    items[target] = set_path_recursive(child, tail, new_value)?;
                    Ok(ZqValue::Array(items))
                }
                other => Err(format!("Cannot index {} with number ({raw})", type_name(&other))),
            }
        }
        // jq-port: jq/src/jv_aux.c:jv_set() slice assignment branch.
        ZqValue::Object(_) => {
            let Some((start, end)) = c_math::parse_slice_component_for_delete(
                head,
                "Array/string slice indices must be integers",
            )?
            else {
                return match value {
                    ZqValue::Array(_) | ZqValue::String(_) => {
                        Err("Array/string slice indices must be integers".to_string())
                    }
                    current => Err(format!(
                        "Cannot update field at object index of {}",
                        type_name(&current)
                    )),
                };
            };
            let mut items = match value {
                ZqValue::Array(items) => items,
                ZqValue::Null => Vec::new(),
                ZqValue::String(_) => return Err("Cannot update string slices".to_string()),
                current => {
                    return Err(format!(
                        "Cannot update field at object index of {}",
                        type_name(&current)
                    ));
                }
            };
            let (slice_start, slice_end) =
                c_math::slice_bounds_from_f64_like_jq(items.len(), start, end);
            let replacement =
                if tail.is_empty() {
                    match new_value {
                        ZqValue::Array(values) => values,
                        _ => {
                            return Err("A slice of an array can only be assigned another array"
                                .to_string());
                        }
                    }
                } else {
                    let sub = ZqValue::Array(items[slice_start..slice_end].to_vec());
                    match set_path_recursive(sub, tail, new_value)? {
                        ZqValue::Array(values) => values,
                        _ => {
                            return Err("A slice of an array can only be assigned another array"
                                .to_string());
                        }
                    }
                };
            items.splice(slice_start..slice_end, replacement);
            Ok(ZqValue::Array(items))
        }
        other => match value {
            current if matches!(other, ZqValue::Array(_)) => {
                Err(format!("Cannot update field at array index of {}", type_name(&current)))
            }
            ZqValue::Array(_) | ZqValue::String(_) => {
                let _ = other;
                Err("Array/string slice indices must be integers".to_string())
            }
            current => {
                Err(format!("Cannot index {} with {}", type_name(&current), type_name(other)))
            }
        },
    }
}

fn canonicalize_delete_paths(
    root: &ZqValue,
    paths: Vec<Vec<ZqValue>>,
) -> Result<Vec<Vec<ZqValue>>, String> {
    paths.into_iter().map(|path| canonicalize_delete_path(root, &path)).collect()
}

fn canonicalize_delete_path(root: &ZqValue, path: &[ZqValue]) -> Result<Vec<ZqValue>, String> {
    let mut current = root.clone();
    let mut out = Vec::with_capacity(path.len());
    for component in path {
        let canonical = canonicalize_delete_component(&current, component)?;
        out.push(canonical.clone());
        if let Some(next) = next_value_for_delete_path(&current, &canonical) {
            current = next;
        } else {
            // Preserve original error behavior in deletion by avoiding eager
            // indexing failures during canonicalization.
            current = ZqValue::Null;
        }
    }
    Ok(out)
}

fn canonicalize_delete_component(
    current: &ZqValue,
    component: &ZqValue,
) -> Result<ZqValue, String> {
    match component {
        ZqValue::Number(number) => {
            let Some(mut idx) = c_math::path_number_for_delete(number) else {
                return Ok(component.clone());
            };
            if idx < 0 {
                if let ZqValue::Array(items) = current {
                    idx += items.len() as i64;
                }
            }
            Ok(ZqValue::from(idx))
        }
        ZqValue::Object(_) => {
            let Some((start, end)) = c_math::parse_slice_component_for_delete(
                component,
                "Array/string slice indices must be integers",
            )?
            else {
                return Ok(component.clone());
            };
            let (start, end) = c_math::canonicalize_slice_bounds_for_container(current, start, end);
            Ok(slice_path_component_value(start, end))
        }
        _ => Ok(component.clone()),
    }
}

fn next_value_for_delete_path(current: &ZqValue, component: &ZqValue) -> Option<ZqValue> {
    match (current, component) {
        (ZqValue::Null, _) => Some(ZqValue::Null),
        (ZqValue::Object(map), ZqValue::String(key)) => {
            Some(map.get(key).cloned().unwrap_or(ZqValue::Null))
        }
        (ZqValue::Array(items), ZqValue::Number(number)) => {
            let mut idx = c_math::path_number_for_delete(number)?;
            if idx < 0 {
                idx += items.len() as i64;
            }
            if idx < 0 || idx as usize >= items.len() {
                Some(ZqValue::Null)
            } else {
                Some(items[idx as usize].clone())
            }
        }
        (ZqValue::String(text), ZqValue::Number(number)) => {
            let idx = c_math::path_number_for_delete(number)?;
            Some(c_string::string_index_like_jq(text, idx).unwrap_or(ZqValue::Null))
        }
        (ZqValue::Array(_) | ZqValue::String(_), ZqValue::Object(_)) => {
            let (start, end) = c_math::parse_slice_component_for_delete(
                component,
                "Array/string slice indices must be integers",
            )
            .ok()??;
            let (start, end) = c_math::canonicalize_slice_bounds_for_container(current, start, end);
            run_slice(current.clone(), start, end).ok()
        }
        _ => None,
    }
}

fn compare_delete_paths_desc(a: &[ZqValue], b: &[ZqValue]) -> Ordering {
    let common = a.len().min(b.len());
    for idx in 0..common {
        let ord = compare_delete_components_desc(&a[idx], &b[idx]);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    b.len().cmp(&a.len())
}

fn compare_delete_components_desc(a: &ZqValue, b: &ZqValue) -> Ordering {
    match (a, b) {
        (ZqValue::Number(na), ZqValue::Number(nb)) => {
            let ia = c_math::path_number_for_delete(na).unwrap_or(0);
            let ib = c_math::path_number_for_delete(nb).unwrap_or(0);
            ib.cmp(&ia)
        }
        _ => jq_cmp(b, a),
    }
}

fn delete_path_recursive(value: &mut ZqValue, path: &[ZqValue]) -> Result<bool, String> {
    let Some((head, tail)) = path.split_first() else {
        return Ok(true);
    };

    match value {
        ZqValue::Null => Ok(false),
        ZqValue::Object(map) => match head {
            ZqValue::String(key) => {
                if tail.is_empty() {
                    let _ = map.shift_remove(key);
                    return Ok(false);
                }
                if let Some(child) = map.get_mut(key) {
                    let delete_child = delete_path_recursive(child, tail)?;
                    if delete_child {
                        let _ = map.shift_remove(key);
                    }
                }
                Ok(false)
            }
            ZqValue::Number(_) => Err("Cannot delete number field of object".to_string()),
            other => Err(format!("Cannot delete {} field of object", type_name(other))),
        },
        ZqValue::Array(items) => match head {
            ZqValue::Number(index) => {
                let Some(mut target) = c_math::path_number_for_delete(index) else {
                    return Ok(false);
                };
                if target < 0 {
                    target += items.len() as i64;
                }
                if target < 0 || target as usize >= items.len() {
                    return Ok(false);
                }
                let target = target as usize;
                if tail.is_empty() {
                    items.remove(target);
                } else {
                    let delete_child = delete_path_recursive(&mut items[target], tail)?;
                    if delete_child {
                        items.remove(target);
                    }
                }
                Ok(false)
            }
            ZqValue::Object(_) => {
                let Some((start, end)) = c_math::parse_slice_component_for_delete(
                    head,
                    "Array/string slice indices must be integers",
                )?
                else {
                    return Err("Array/string slice indices must be integers".to_string());
                };
                let (slice_start, slice_end) =
                    c_math::slice_bounds_from_f64_like_jq(items.len(), start, end);
                if tail.is_empty() {
                    items.drain(slice_start..slice_end);
                    return Ok(false);
                }
                for idx in (slice_start..slice_end).rev() {
                    let delete_child = delete_path_recursive(&mut items[idx], tail)?;
                    if delete_child {
                        items.remove(idx);
                    }
                }
                Ok(false)
            }
            ZqValue::String(_) => Err("Cannot delete string element of array".to_string()),
            _ => Err("Array/string slice indices must be integers".to_string()),
        },
        other => Err(format!("Cannot delete fields from {}", type_name(other))),
    }
}
