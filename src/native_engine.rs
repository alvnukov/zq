use regex::Regex;
use serde_json::Value as JsonValue;

static JSON_NULL: JsonValue = JsonValue::Null;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    pub null_input: bool,
}

#[derive(Debug)]
pub enum TryExecute {
    Unsupported,
    Executed(Result<Vec<JsonValue>, String>),
}

#[derive(Debug)]
pub enum TryExecuteStream {
    Unsupported,
    Executed(Result<(), String>),
}

pub fn is_supported(query: &str) -> bool {
    try_compile(query).is_some()
}

pub struct CompiledProgram {
    stages: Vec<Vec<Stage>>,
}

pub fn try_compile(query: &str) -> Option<CompiledProgram> {
    let stages = parse_pipeline(query)?;
    Some(CompiledProgram { stages })
}

#[derive(Debug, Clone)]
enum Stage {
    Literal(JsonValue),
    Path(Vec<Accessor>),
    PathAdd {
        left: Vec<Accessor>,
        right: Vec<Accessor>,
    },
    PathMod {
        path: Vec<Accessor>,
        rhs: f64,
    },
    ObjectPick {
        fields: Vec<String>,
    },
    Length,
    Gsub {
        regex: Regex,
        replacement: String,
    },
    SelectTest {
        path: Vec<Accessor>,
        regex: Regex,
    },
    SelectCompare {
        path: Vec<Accessor>,
        op: CompareOp,
        rhs: JsonValue,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Accessor {
    Field(String),
    FieldOpt(String),
    Index(i64),
    IndexOpt(i64),
    Iter,
    IterOpt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

pub fn try_execute(query: &str, inputs: &[JsonValue], run_options: RunOptions) -> TryExecute {
    let mut out = Vec::new();
    match try_execute_stream(query, inputs, run_options, |value| {
        out.push(value);
        Ok(())
    }) {
        TryExecuteStream::Unsupported => TryExecute::Unsupported,
        TryExecuteStream::Executed(Ok(())) => TryExecute::Executed(Ok(out)),
        TryExecuteStream::Executed(Err(e)) => TryExecute::Executed(Err(e)),
    }
}

pub fn try_execute_stream<F>(
    query: &str,
    inputs: &[JsonValue],
    run_options: RunOptions,
    mut emit: F,
) -> TryExecuteStream
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    let Some(program) = try_compile(query) else {
        return TryExecuteStream::Unsupported;
    };

    if let Err(e) = program.execute_slice(inputs, run_options, &mut emit) {
        return TryExecuteStream::Executed(Err(e));
    }
    TryExecuteStream::Executed(Ok(()))
}

impl CompiledProgram {
    pub fn execute_input<F>(&self, root: JsonValue, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(JsonValue) -> Result<(), String>,
    {
        if self.stages.len() == 1 {
            return run_terms_emit(&self.stages[0], root, emit);
        }
        if self.stages.len() == 2 {
            let mut mid = Vec::new();
            run_terms_collect(&self.stages[0], root, &mut mid)?;
            for value in mid {
                run_terms_emit(&self.stages[1], value, emit)?;
            }
            return Ok(());
        }

        // Evaluate the full pipeline per root value. This avoids building
        // large cross-input intermediate vectors for queries like `.x | length`.
        let mut stream = vec![root];
        for (stage_idx, terms) in self.stages.iter().enumerate() {
            let last_stage = stage_idx + 1 == self.stages.len();
            if last_stage {
                for value in stream {
                    run_terms_emit(terms, value, emit)?;
                }
                return Ok(());
            }

            let mut out = Vec::new();
            for value in stream {
                run_terms_collect(terms, value, &mut out)?;
            }
            stream = out;
        }
        Ok(())
    }

    pub fn execute_slice<F>(
        &self,
        inputs: &[JsonValue],
        run_options: RunOptions,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(JsonValue) -> Result<(), String>,
    {
        if run_options.null_input {
            self.execute_input(JsonValue::Null, emit)?;
            return Ok(());
        }
        for root in inputs {
            self.execute_input(root.clone(), emit)?;
        }
        Ok(())
    }
}

fn run_terms_collect(
    terms: &[Stage],
    value: JsonValue,
    out: &mut Vec<JsonValue>,
) -> Result<(), String> {
    let terms_len = terms.len();
    if terms_len == 1 {
        out.extend(run_stage(&terms[0], value)?);
        return Ok(());
    }

    let mut original = Some(value);
    for (idx, term) in terms.iter().enumerate() {
        let input = if idx + 1 == terms_len {
            original.take().expect("value available for last branch")
        } else {
            original
                .as_ref()
                .expect("value available for branch clone")
                .clone()
        };
        out.extend(run_stage(term, input)?);
    }
    Ok(())
}

fn run_terms_emit<F>(terms: &[Stage], value: JsonValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    let terms_len = terms.len();
    if terms_len == 1 {
        run_stage_emit(&terms[0], value, emit)?;
        return Ok(());
    }

    let mut original = Some(value);
    for (idx, term) in terms.iter().enumerate() {
        let input = if idx + 1 == terms_len {
            original.take().expect("value available for last branch")
        } else {
            original
                .as_ref()
                .expect("value available for branch clone")
                .clone()
        };
        run_stage_emit(term, input, emit)?;
    }
    Ok(())
}

fn run_stage_emit<F>(stage: &Stage, input: JsonValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    match stage {
        Stage::Literal(v) => emit(v.clone()),
        Stage::Path(accessors) => run_path_emit(accessors, input, emit),
        Stage::PathAdd { left, right } => {
            for value in run_path_add(left, right, input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::PathMod { path, rhs } => {
            for value in run_path_mod(path, *rhs, input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::ObjectPick { fields } => {
            for value in run_object_pick(fields, input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::Length => {
            for value in run_length(input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::Gsub { regex, replacement } => {
            for value in run_gsub(regex, replacement, input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::SelectTest { path, regex } => {
            for value in run_select_test(path, regex, input)? {
                emit(value)?;
            }
            Ok(())
        }
        Stage::SelectCompare { path, op, rhs } => {
            for value in run_select_compare(path, *op, rhs, input)? {
                emit(value)?;
            }
            Ok(())
        }
    }
}

fn run_path_emit<F>(accessors: &[Accessor], input: JsonValue, emit: &mut F) -> Result<(), String>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    if accessors.is_empty() {
        emit(input)?;
        return Ok(());
    }

    if accessors.len() == 1 {
        apply_accessor_emit(input, &accessors[0], emit)?;
        return Ok(());
    }

    for value in run_path(accessors, input)? {
        emit(value)?;
    }
    Ok(())
}

fn run_stage(stage: &Stage, input: JsonValue) -> Result<Vec<JsonValue>, String> {
    match stage {
        Stage::Literal(v) => Ok(vec![v.clone()]),
        Stage::Path(accessors) => run_path(accessors, input),
        Stage::PathAdd { left, right } => run_path_add(left, right, input),
        Stage::PathMod { path, rhs } => run_path_mod(path, *rhs, input),
        Stage::ObjectPick { fields } => run_object_pick(fields, input),
        Stage::Length => run_length(input),
        Stage::Gsub { regex, replacement } => run_gsub(regex, replacement, input),
        Stage::SelectTest { path, regex } => run_select_test(path, regex, input),
        Stage::SelectCompare { path, op, rhs } => run_select_compare(path, *op, rhs, input),
    }
}

fn run_object_pick(fields: &[String], input: JsonValue) -> Result<Vec<JsonValue>, String> {
    match input {
        JsonValue::Object(map) => {
            let mut out = serde_json::Map::with_capacity(fields.len());
            for field in fields {
                out.insert(
                    field.clone(),
                    map.get(field).cloned().unwrap_or(JsonValue::Null),
                );
            }
            Ok(vec![JsonValue::Object(out)])
        }
        JsonValue::Null => {
            let mut out = serde_json::Map::with_capacity(fields.len());
            for field in fields {
                out.insert(field.clone(), JsonValue::Null);
            }
            Ok(vec![JsonValue::Object(out)])
        }
        other => Err(format!(
            "Cannot index {} with string \"{}\"",
            type_name(&other),
            fields.first().map(String::as_str).unwrap_or("")
        )),
    }
}

fn run_path_add(
    left: &[Accessor],
    right: &[Accessor],
    input: JsonValue,
) -> Result<Vec<JsonValue>, String> {
    if path_is_single_valued(left) && path_is_single_valued(right) {
        let lhs = run_path_single_ref(left, &input)?;
        let rhs = run_path_single_ref(right, &input)?;
        return match (lhs, rhs) {
            (Some(l), Some(r)) => Ok(vec![binop_add(l.into_owned(), r.into_owned())?]),
            _ => Ok(Vec::new()),
        };
    }

    let lhs_values = run_path(left, input.clone())?;
    let rhs_values = run_path(right, input)?;
    let mut out = Vec::with_capacity(lhs_values.len().saturating_mul(rhs_values.len()));
    for rhs in rhs_values {
        for lhs in &lhs_values {
            out.push(binop_add(lhs.clone(), rhs.clone())?);
        }
    }
    Ok(out)
}

fn binop_add(lhs: JsonValue, rhs: JsonValue) -> Result<JsonValue, String> {
    match (lhs, rhs) {
        (JsonValue::Null, r) => Ok(r),
        (l, JsonValue::Null) => Ok(l),
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let af = a
                .as_f64()
                .ok_or_else(|| "number is out of range".to_string())?;
            let bf = b
                .as_f64()
                .ok_or_else(|| "number is out of range".to_string())?;
            Ok(number_to_json(af + bf))
        }
        (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::String(format!("{a}{b}"))),
        (JsonValue::Array(mut a), JsonValue::Array(b)) => {
            a.extend(b);
            Ok(JsonValue::Array(a))
        }
        (JsonValue::Object(mut a), JsonValue::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
            Ok(JsonValue::Object(a))
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

fn run_length(input: JsonValue) -> Result<Vec<JsonValue>, String> {
    let out = match input {
        JsonValue::Null => JsonValue::from(0),
        JsonValue::Array(arr) => JsonValue::from(arr.len() as i64),
        JsonValue::Object(map) => JsonValue::from(map.len() as i64),
        JsonValue::String(s) => JsonValue::from(s.chars().count() as i64),
        JsonValue::Number(n) => {
            let Some(v) = n.as_f64() else {
                return Err("number is out of range".to_string());
            };
            number_to_json(v.abs())
        }
        JsonValue::Bool(b) => {
            return Err(format!("boolean ({b}) has no length"));
        }
    };
    Ok(vec![out])
}

fn run_gsub(regex: &Regex, replacement: &str, input: JsonValue) -> Result<Vec<JsonValue>, String> {
    let JsonValue::String(s) = input else {
        return Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name(&input),
            value_for_error(&input)
        ));
    };
    let out = regex.replace_all(&s, replacement).to_string();
    Ok(vec![JsonValue::String(out)])
}

fn run_select_test(
    path: &[Accessor],
    regex: &Regex,
    input: JsonValue,
) -> Result<Vec<JsonValue>, String> {
    if path_is_single_valued(path) {
        let Some(value) = run_path_single_ref(path, &input)? else {
            return Ok(Vec::new());
        };
        let JsonValue::String(s) = value.as_ref() else {
            return Err(format!(
                "{} ({}) cannot be matched, as it is not a string",
                type_name(value.as_ref()),
                value_for_error(value.as_ref())
            ));
        };
        if regex.is_match(s) {
            return Ok(vec![input]);
        }
        return Ok(Vec::new());
    }

    let values = run_path(path, input.clone())?;
    let mut matches_count = 0usize;
    for value in values {
        let JsonValue::String(s) = value else {
            return Err(format!(
                "{} ({}) cannot be matched, as it is not a string",
                type_name(&value),
                value_for_error(&value)
            ));
        };
        if regex.is_match(&s) {
            matches_count += 1;
        }
    }
    let mut out = Vec::with_capacity(matches_count);
    if matches_count == 0 {
        return Ok(out);
    }
    for _ in 1..matches_count {
        out.push(input.clone());
    }
    out.push(input);
    Ok(out)
}

fn run_select_compare(
    path: &[Accessor],
    op: CompareOp,
    rhs: &JsonValue,
    input: JsonValue,
) -> Result<Vec<JsonValue>, String> {
    if path_is_single_valued(path) {
        let Some(lhs) = run_path_single_ref(path, &input)? else {
            return Ok(Vec::new());
        };
        if compare_with_op(lhs.as_ref(), rhs, op) {
            return Ok(vec![input]);
        }
        return Ok(Vec::new());
    }

    let values = run_path(path, input.clone())?;
    let mut matches_count = 0usize;
    for lhs in values {
        let is_match = compare_with_op(&lhs, rhs, op);
        if is_match {
            matches_count += 1;
        }
    }

    let mut out = Vec::with_capacity(matches_count);
    if matches_count == 0 {
        return Ok(out);
    }
    for _ in 1..matches_count {
        out.push(input.clone());
    }
    out.push(input);
    Ok(out)
}

fn compare_with_op(lhs: &JsonValue, rhs: &JsonValue, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Gt => jq_cmp(lhs, rhs) == std::cmp::Ordering::Greater,
        CompareOp::Ge => {
            let ord = jq_cmp(lhs, rhs);
            ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal
        }
        CompareOp::Lt => jq_cmp(lhs, rhs) == std::cmp::Ordering::Less,
        CompareOp::Le => {
            let ord = jq_cmp(lhs, rhs);
            ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
        }
    }
}

// Mirrors jq's ordering contract from src/jv_aux.c:jv_cmp().
fn jq_cmp(lhs: &JsonValue, rhs: &JsonValue) -> std::cmp::Ordering {
    let lrank = jq_kind_rank(lhs);
    let rrank = jq_kind_rank(rhs);
    if lrank != rrank {
        return lrank.cmp(&rrank);
    }

    match (lhs, rhs) {
        (JsonValue::Null, JsonValue::Null) => std::cmp::Ordering::Equal,
        (JsonValue::Bool(_), JsonValue::Bool(_)) => std::cmp::Ordering::Equal,
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let af = a.as_f64().unwrap_or(f64::NAN);
            let bf = b.as_f64().unwrap_or(f64::NAN);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (JsonValue::String(a), JsonValue::String(b)) => a.cmp(b),
        (JsonValue::Array(a), JsonValue::Array(b)) => {
            for (la, lb) in a.iter().zip(b.iter()) {
                let ord = jq_cmp(la, lb);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            a.len().cmp(&b.len())
        }
        (JsonValue::Object(a), JsonValue::Object(b)) => {
            let mut akeys = a.keys().cloned().collect::<Vec<_>>();
            let mut bkeys = b.keys().cloned().collect::<Vec<_>>();
            akeys.sort();
            bkeys.sort();
            let key_ord = akeys.cmp(&bkeys);
            if key_ord != std::cmp::Ordering::Equal {
                return key_ord;
            }
            for k in akeys {
                let ord = jq_cmp(
                    a.get(&k).expect("key from object A"),
                    b.get(&k).expect("key from object B"),
                );
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn jq_kind_rank(v: &JsonValue) -> i32 {
    match v {
        JsonValue::Null => 1,
        JsonValue::Bool(false) => 2,
        JsonValue::Bool(true) => 3,
        JsonValue::Number(_) => 4,
        JsonValue::String(_) => 5,
        JsonValue::Array(_) => 6,
        JsonValue::Object(_) => 7,
    }
}

fn run_path_mod(path: &[Accessor], rhs: f64, input: JsonValue) -> Result<Vec<JsonValue>, String> {
    if path_is_single_valued(path) {
        let Some(lhs) = run_path_single_ref(path, &input)? else {
            return Ok(Vec::new());
        };
        let JsonValue::Number(num) = lhs.as_ref() else {
            let rhs_json = number_to_json(rhs);
            return Err(format!(
                "{} ({}) and number ({}) cannot be divided (remainder)",
                type_name(lhs.as_ref()),
                value_for_error(lhs.as_ref()),
                value_for_error(&rhs_json)
            ));
        };
        let Some(lhs_f64) = num.as_f64() else {
            return Err("number is out of range".to_string());
        };
        let v = jq_mod_compat(lhs_f64, rhs)?;
        return Ok(vec![number_to_json(v)]);
    }

    let values = run_path(path, input)?;
    let mut out = Vec::with_capacity(values.len());
    let rhs_json = number_to_json(rhs);
    for lhs in values {
        let JsonValue::Number(num) = lhs else {
            return Err(format!(
                "{} ({}) and number ({}) cannot be divided (remainder)",
                type_name(&lhs),
                value_for_error(&lhs),
                value_for_error(&rhs_json)
            ));
        };
        let Some(lhs_f64) = num.as_f64() else {
            return Err("number is out of range".to_string());
        };
        let v = jq_mod_compat(lhs_f64, rhs)?;
        out.push(number_to_json(v));
    }
    Ok(out)
}

fn jq_dtoi_compat(v: f64) -> i64 {
    if v < i64::MIN as f64 {
        i64::MIN
    } else if -v < i64::MIN as f64 {
        i64::MAX
    } else {
        v as i64
    }
}

fn jq_mod_compat(lhs: f64, rhs: f64) -> Result<f64, String> {
    if lhs.is_nan() || rhs.is_nan() {
        return Ok(f64::NAN);
    }
    let rhs_i = jq_dtoi_compat(rhs);
    if rhs_i == 0 {
        return Err("cannot be divided (remainder) because the divisor is zero".to_string());
    }
    if rhs_i == -1 {
        return Ok(0.0);
    }
    Ok((jq_dtoi_compat(lhs) % rhs_i) as f64)
}

fn number_to_json(v: f64) -> JsonValue {
    if !v.is_finite() {
        return JsonValue::Null;
    }
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        return JsonValue::from(v as i64);
    }
    let n = serde_json::Number::from_f64(v).expect("finite number");
    JsonValue::Number(n)
}

fn path_is_single_valued(path: &[Accessor]) -> bool {
    !path
        .iter()
        .any(|a| matches!(a, Accessor::Iter | Accessor::IterOpt))
}

#[derive(Debug, Clone, Copy)]
enum SinglePathValue<'a> {
    Borrowed(&'a JsonValue),
    Null,
}

impl<'a> SinglePathValue<'a> {
    fn as_ref(self) -> &'a JsonValue {
        match self {
            SinglePathValue::Borrowed(v) => v,
            SinglePathValue::Null => &JSON_NULL,
        }
    }

    fn into_owned(self) -> JsonValue {
        match self {
            SinglePathValue::Borrowed(v) => v.clone(),
            SinglePathValue::Null => JsonValue::Null,
        }
    }
}

fn run_path_single_ref<'a>(
    path: &[Accessor],
    input: &'a JsonValue,
) -> Result<Option<SinglePathValue<'a>>, String> {
    let mut current = SinglePathValue::Borrowed(input);
    for accessor in path {
        current = match accessor {
            Accessor::Field(name) | Accessor::FieldOpt(name) => match current {
                SinglePathValue::Null => SinglePathValue::Null,
                SinglePathValue::Borrowed(JsonValue::Object(map)) => map
                    .get(name)
                    .map(SinglePathValue::Borrowed)
                    .unwrap_or(SinglePathValue::Null),
                SinglePathValue::Borrowed(other) => {
                    if matches!(accessor, Accessor::FieldOpt(_)) {
                        return Ok(None);
                    }
                    return Err(format!(
                        "Cannot index {} with string \"{}\"",
                        type_name(other),
                        name
                    ));
                }
            },
            Accessor::Index(index) | Accessor::IndexOpt(index) => match current {
                SinglePathValue::Null => SinglePathValue::Null,
                SinglePathValue::Borrowed(JsonValue::Array(arr)) => {
                    let len = arr.len() as i64;
                    let idx = if *index < 0 { len + *index } else { *index };
                    if idx < 0 || idx >= len {
                        SinglePathValue::Null
                    } else {
                        SinglePathValue::Borrowed(&arr[idx as usize])
                    }
                }
                SinglePathValue::Borrowed(other) => {
                    if matches!(accessor, Accessor::IndexOpt(_)) {
                        return Ok(None);
                    }
                    return Err(format!("Cannot index {} with number", type_name(other)));
                }
            },
            Accessor::Iter | Accessor::IterOpt => {
                unreachable!("iter accessors are not single-valued")
            }
        };
    }
    Ok(Some(current))
}

fn run_path(accessors: &[Accessor], input: JsonValue) -> Result<Vec<JsonValue>, String> {
    if accessors.is_empty() {
        return Ok(vec![input]);
    }

    if accessors.len() == 1 {
        let mut out = Vec::with_capacity(1);
        apply_accessor_into(input, &accessors[0], &mut out)?;
        return Ok(out);
    }

    let mut current = Vec::with_capacity(1);
    current.push(input);
    let mut next = Vec::new();

    for accessor in accessors {
        next.clear();
        if next.capacity() < current.len() {
            next.reserve(current.len() - next.capacity());
        }
        for value in current.drain(..) {
            apply_accessor_into(value, accessor, &mut next)?;
        }
        std::mem::swap(&mut current, &mut next);
    }

    Ok(current)
}

fn apply_accessor_into(
    value: JsonValue,
    accessor: &Accessor,
    out: &mut Vec<JsonValue>,
) -> Result<(), String> {
    let result = match accessor {
        Accessor::Field(name) | Accessor::FieldOpt(name) => match value {
            JsonValue::Object(map) => {
                out.push(map.get(name).cloned().unwrap_or(JsonValue::Null));
                Ok(())
            }
            JsonValue::Null => {
                out.push(JsonValue::Null);
                Ok(())
            }
            other => Err(format!(
                "Cannot index {} with string \"{}\"",
                type_name(&other),
                name
            )),
        },
        Accessor::Index(index) | Accessor::IndexOpt(index) => match value {
            JsonValue::Array(arr) => {
                let len = arr.len() as i64;
                let idx = if *index < 0 { len + *index } else { *index };
                if idx < 0 || idx >= len {
                    out.push(JsonValue::Null);
                    return Ok(());
                }
                out.push(arr[idx as usize].clone());
                Ok(())
            }
            JsonValue::Null => {
                out.push(JsonValue::Null);
                Ok(())
            }
            other => Err(format!("Cannot index {} with number", type_name(&other))),
        },
        Accessor::Iter | Accessor::IterOpt => match value {
            JsonValue::Array(arr) => {
                out.extend(arr);
                Ok(())
            }
            JsonValue::Object(map) => {
                out.extend(map.into_iter().map(|(_, v)| v));
                Ok(())
            }
            other => Err(format!(
                "Cannot iterate over {} ({})",
                type_name(&other),
                value_for_error(&other)
            )),
        },
    };

    result.or_else(|e| match accessor {
        Accessor::FieldOpt(_) | Accessor::IndexOpt(_) | Accessor::IterOpt => Ok(()),
        _ => Err(e),
    })
}

fn apply_accessor_emit<F>(value: JsonValue, accessor: &Accessor, emit: &mut F) -> Result<(), String>
where
    F: FnMut(JsonValue) -> Result<(), String>,
{
    let result = match accessor {
        Accessor::Field(name) | Accessor::FieldOpt(name) => match value {
            JsonValue::Object(map) => emit(map.get(name).cloned().unwrap_or(JsonValue::Null)),
            JsonValue::Null => emit(JsonValue::Null),
            other => Err(format!(
                "Cannot index {} with string \"{}\"",
                type_name(&other),
                name
            )),
        },
        Accessor::Index(index) | Accessor::IndexOpt(index) => match value {
            JsonValue::Array(arr) => {
                let len = arr.len() as i64;
                let idx = if *index < 0 { len + *index } else { *index };
                if idx < 0 || idx >= len {
                    return emit(JsonValue::Null);
                }
                emit(arr[idx as usize].clone())
            }
            JsonValue::Null => emit(JsonValue::Null),
            other => Err(format!("Cannot index {} with number", type_name(&other))),
        },
        Accessor::Iter | Accessor::IterOpt => match value {
            JsonValue::Array(arr) => {
                for item in arr {
                    emit(item)?;
                }
                Ok(())
            }
            JsonValue::Object(map) => {
                for (_, item) in map {
                    emit(item)?;
                }
                Ok(())
            }
            other => Err(format!(
                "Cannot iterate over {} ({})",
                type_name(&other),
                value_for_error(&other)
            )),
        },
    };

    result.or_else(|e| match accessor {
        Accessor::FieldOpt(_) | Accessor::IndexOpt(_) | Accessor::IterOpt => Ok(()),
        _ => Err(e),
    })
}

fn type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn value_for_error(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".to_string(),
        JsonValue::String(s) => format!("{s:?}"),
        _ => serde_json::to_string(v).unwrap_or_else(|_| "<invalid>".to_string()),
    }
}

fn parse_pipeline(query: &str) -> Option<Vec<Vec<Stage>>> {
    let mut chars = query.chars().peekable();
    let mut out = Vec::new();
    loop {
        skip_ws(&mut chars);
        if chars.peek().is_none() {
            break;
        }
        out.push(parse_terms(&mut chars)?);
        skip_ws(&mut chars);
        match chars.peek().copied() {
            Some('|') => {
                chars.next();
            }
            Some(_) => return None,
            None => break,
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_terms(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Vec<Stage>> {
    let mut out = Vec::new();
    loop {
        skip_ws(chars);
        out.push(parse_stage(chars)?);
        skip_ws(chars);
        if chars.peek().copied() == Some(',') {
            chars.next();
            continue;
        }
        break;
    }
    Some(out)
}

fn parse_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    if chars.peek().copied() == Some('{') {
        let mut probe = chars.clone();
        if let Some(stage) = parse_object_pick_stage(&mut probe) {
            *chars = probe;
            return Some(stage);
        }
    }

    if chars.peek().copied() == Some('.') {
        return parse_path_stage(chars);
    }

    if chars.peek().copied() == Some('s') {
        let mut probe = chars.clone();
        if let Some(stage) = parse_select_stage(&mut probe) {
            *chars = probe;
            return Some(stage);
        }
    }

    if chars.peek().copied() == Some('l') {
        let mut probe = chars.clone();
        if let Some(stage) = parse_length_stage(&mut probe) {
            *chars = probe;
            return Some(stage);
        }
    }

    if chars.peek().copied() == Some('g') {
        let mut probe = chars.clone();
        if let Some(stage) = parse_gsub_stage(&mut probe) {
            *chars = probe;
            return Some(stage);
        }
    }

    let lit = parse_literal(chars)?;
    Some(Stage::Literal(lit))
}

fn parse_path_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    let left = parse_path_accessors(chars)?;
    skip_ws(chars);
    if chars.peek().copied() == Some('+') {
        chars.next();
        skip_ws(chars);
        let right = parse_path_accessors(chars)?;
        return Some(Stage::PathAdd { left, right });
    }
    let path = left;
    if chars.peek().copied() == Some('%') {
        chars.next();
        skip_ws(chars);
        let rhs = parse_literal(chars)?.as_f64()?;
        return Some(Stage::PathMod { path, rhs });
    }
    Some(Stage::Path(path))
}

fn parse_object_pick_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    if chars.next() != Some('{') {
        return None;
    }
    let mut fields = Vec::new();
    loop {
        skip_ws(chars);
        if chars.peek().copied() == Some('}') {
            chars.next();
            break;
        }
        let name = parse_field_after_dot(chars)?;
        fields.push(name);
        skip_ws(chars);
        match chars.peek().copied() {
            Some(',') => {
                chars.next();
            }
            Some('}') => {
                chars.next();
                break;
            }
            _ => return None,
        }
    }
    Some(Stage::ObjectPick { fields })
}

fn parse_path_accessors(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<Vec<Accessor>> {
    if chars.next() != Some('.') {
        return None;
    }
    let mut accessors = Vec::new();
    if matches!(chars.peek().copied(), Some('"')) || ident_start(chars.peek().copied()) {
        let field = Accessor::Field(parse_field_after_dot(chars)?);
        accessors.push(if take_optional(chars) {
            make_optional(field)
        } else {
            field
        });
    }
    loop {
        skip_ws(chars);
        match chars.peek().copied() {
            Some('.') => {
                chars.next();
                let name = parse_field_after_dot(chars)?;
                accessors.push(if take_optional(chars) {
                    Accessor::FieldOpt(name)
                } else {
                    Accessor::Field(name)
                });
            }
            Some('[') => {
                chars.next();
                skip_ws(chars);
                let acc = if chars.peek().copied() == Some(']') {
                    Accessor::Iter
                } else if chars.peek().copied() == Some('"') {
                    Accessor::Field(parse_string(chars)?)
                } else {
                    Accessor::Index(parse_i64(chars)?)
                };
                skip_ws(chars);
                if chars.next() != Some(']') {
                    return None;
                }
                let acc = if take_optional(chars) {
                    make_optional(acc)
                } else {
                    acc
                };
                accessors.push(acc);
            }
            _ => break,
        }
    }
    Some(accessors)
}

fn parse_select_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    parse_keyword(chars, "select")?;
    skip_ws(chars);
    if chars.next() != Some('(') {
        return None;
    }
    skip_ws(chars);
    let path = parse_path_accessors(chars)?;
    skip_ws(chars);
    match chars.peek().copied() {
        Some('|') => {
            chars.next();
            skip_ws(chars);
            parse_keyword(chars, "test")?;
            skip_ws(chars);
            if chars.next() != Some('(') {
                return None;
            }
            skip_ws(chars);
            let pattern = parse_string(chars)?;
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            let regex = Regex::new(&pattern).ok()?;
            Some(Stage::SelectTest { path, regex })
        }
        _ => {
            let op = parse_select_compare_op(chars)?;
            let rhs = parse_select_rhs_literal(chars)?;
            skip_ws(chars);
            if chars.next() != Some(')') {
                return None;
            }
            Some(Stage::SelectCompare { path, op, rhs })
        }
    }
}

fn parse_select_compare_op(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<CompareOp> {
    skip_ws(chars);
    let first = chars.next()?;
    match first {
        '=' => {
            if chars.next() == Some('=') {
                Some(CompareOp::Eq)
            } else {
                None
            }
        }
        '!' => {
            if chars.next() == Some('=') {
                Some(CompareOp::Ne)
            } else {
                None
            }
        }
        '>' => {
            if chars.peek().copied() == Some('=') {
                chars.next();
                Some(CompareOp::Ge)
            } else {
                Some(CompareOp::Gt)
            }
        }
        '<' => {
            if chars.peek().copied() == Some('=') {
                chars.next();
                Some(CompareOp::Le)
            } else {
                Some(CompareOp::Lt)
            }
        }
        _ => None,
    }
}

fn parse_length_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    parse_keyword(chars, "length")?;
    if matches!(chars.peek().copied(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some(Stage::Length)
}

fn parse_gsub_stage(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<Stage> {
    parse_keyword(chars, "gsub")?;
    skip_ws(chars);
    if chars.next() != Some('(') {
        return None;
    }
    skip_ws(chars);
    let pattern = parse_string(chars)?;
    skip_ws(chars);
    if chars.next() != Some(';') {
        return None;
    }
    skip_ws(chars);
    let replacement = parse_string(chars)?;
    skip_ws(chars);
    if chars.next() != Some(')') {
        return None;
    }
    let regex = Regex::new(&pattern).ok()?;
    Some(Stage::Gsub { regex, replacement })
}

fn parse_select_rhs_literal(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<JsonValue> {
    skip_ws(chars);
    if chars.peek().copied() == Some('"') {
        return Some(JsonValue::String(parse_string(chars)?));
    }

    let mut raw = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch == ')' || ch.is_ascii_whitespace() {
            break;
        }
        raw.push(ch);
        chars.next();
    }

    if raw.is_empty() {
        return None;
    }
    serde_json::from_str(raw.trim()).ok()
}

fn parse_keyword(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    expected: &str,
) -> Option<()> {
    for ch in expected.chars() {
        if chars.next() != Some(ch) {
            return None;
        }
    }
    Some(())
}

fn ident_start(ch: Option<char>) -> bool {
    matches!(ch, Some(c) if c.is_ascii_alphabetic() || c == '_')
}

fn parse_field_after_dot(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    if chars.peek().copied() == Some('"') {
        return parse_string(chars);
    }

    let mut out = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    let mut raw = String::new();
    if chars.next() != Some('"') {
        return None;
    }
    raw.push('"');

    let mut escaped = false;
    for ch in chars.by_ref() {
        raw.push(ch);
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            let parsed: String = serde_json::from_str(&raw).ok()?;
            return Some(parsed);
        }
    }
    None
}

fn take_optional(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    skip_ws(chars);
    if chars.peek().copied() == Some('?') {
        chars.next();
        true
    } else {
        false
    }
}

fn make_optional(acc: Accessor) -> Accessor {
    match acc {
        Accessor::Field(s) => Accessor::FieldOpt(s),
        Accessor::Index(i) => Accessor::IndexOpt(i),
        Accessor::Iter => Accessor::IterOpt,
        other => other,
    }
}

fn parse_i64(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<i64> {
    let mut raw = String::new();
    if chars.peek().copied() == Some('-') {
        raw.push('-');
        chars.next();
    }
    let mut has_digit = false;
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            has_digit = true;
            raw.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if !has_digit {
        return None;
    }
    raw.parse::<i64>().ok()
}

fn parse_literal(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<JsonValue> {
    let mut raw = String::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut depth = 0i32;

    while let Some(ch) = chars.peek().copied() {
        if !in_string && depth == 0 && (ch == '|' || ch == ',' || ch.is_ascii_whitespace()) {
            break;
        }
        raw.push(ch);
        chars.next();

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
            '[' | '{' => depth += 1,
            ']' | '}' => depth -= 1,
            _ => {}
        }
    }

    if raw.is_empty() || depth != 0 || in_string {
        return None;
    }
    serde_json::from_str(raw.trim()).ok()
}

fn skip_ws(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_runs_simple_paths() {
        let input = vec![serde_json::json!({"a":[{"b":1},{"b":2}]})];
        let out = try_execute(".a[1].b", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn runs_pipeline_with_literal() {
        let input = vec![serde_json::json!({"a": 7})];
        let out = try_execute(".a | 10", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(10)]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn uses_null_input_mode() {
        let input = vec![serde_json::json!({"a": 7})];
        let out = try_execute(".", &input, RunOptions { null_input: true });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_constructs() {
        assert!(matches!(
            try_execute(
                "map(.)",
                &[JsonValue::Null],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
    }

    #[test]
    fn supports_iteration_and_comma_outputs() {
        let input = vec![serde_json::json!([1, 2, 3])];
        let out = try_execute(".[]", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(1),
                        serde_json::json!(2),
                        serde_json::json!(3)
                    ]
                )
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute("1, 2", &[JsonValue::Null], RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)])
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_optional_path_access() {
        let out = try_execute(
            ".a?",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[]?",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".missing?",
            &[serde_json::json!({"a": 1})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_test_regex_filter() {
        let input = vec![serde_json::json!([
            {"id": 1, "name": "user0999"},
            {"id": 2, "name": "user1000"},
            {"id": 3, "name": "user1999"},
            {"id": 4, "name": "user2000"}
        ])];
        let out = try_execute(
            ".[] | select(.name | test(\"user1[0-9]{3}$\")) | .id",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(2), serde_json::json!(3)])
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_compare_eq_filter() {
        let input = vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
            serde_json::json!({"i": 3}),
        ];
        let out = try_execute("select(.i==2)", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!({"i": 2})]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_compare_ne_filter() {
        let input = vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
            serde_json::json!({"i": 3}),
        ];
        let out = try_execute("select(.i!=2)", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![serde_json::json!({"i": 1}), serde_json::json!({"i": 3})]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_select_compare_ordering_filter() {
        let input = vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
            serde_json::json!({"i": 3}),
        ];
        let out = try_execute("select(.i>2)", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!({"i": 3})]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn supports_path_mod_literal_length_and_gsub() {
        let input = vec![serde_json::json!({
            "value": 11,
            "tags": [1, 2, 3],
            "text": "alpha-beta"
        })];

        let out = try_execute(".value % 7", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(4)]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(".tags | length", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(3)]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".text | gsub(\"[aeiou]\";\"\")",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!("lph-bt")])
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn select_test_errors_on_non_string_like_jq() {
        let input = vec![serde_json::json!({"name": null})];
        let out = try_execute(
            "select(.name | test(\"x\"))",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "null (null) cannot be matched, as it is not a string")
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn unsupported_regex_syntax_falls_back() {
        let out = try_execute(
            "select(.name | test(\"(?=x)\"))",
            &[serde_json::json!({"name":"x"})],
            RunOptions { null_input: false },
        );
        assert!(matches!(out, TryExecute::Unsupported));
    }

    #[test]
    fn native_runtime_errors_match_jq_wording() {
        let out = try_execute(
            ".[1]",
            &[serde_json::json!({})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "Cannot index object with number");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[]",
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "Cannot iterate over number (1)");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn try_execute_stream_matches_collect_mode() {
        let input = vec![serde_json::json!([{"id":1},{"id":2},{"id":3}])];
        let collected = try_execute(".[] | .id", &input, RunOptions { null_input: false });
        let mut streamed = Vec::new();
        let streamed_out =
            try_execute_stream(".[] | .id", &input, RunOptions { null_input: false }, |v| {
                streamed.push(v);
                Ok(())
            });

        match (collected, streamed_out) {
            (TryExecute::Executed(Ok(a)), TryExecuteStream::Executed(Ok(()))) => {
                assert_eq!(a, streamed);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn try_execute_stream_propagates_sink_error() {
        let input = vec![serde_json::json!([1, 2, 3])];
        let out = try_execute_stream(".[]", &input, RunOptions { null_input: false }, |_v| {
            Err("sink failed".to_string())
        });
        match out {
            TryExecuteStream::Executed(Err(err)) => assert_eq!(err, "sink failed"),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn select_reemits_input_for_each_match() {
        let row = serde_json::json!({"tags":[1,1,2], "name":"aaxx"});
        let input = vec![row.clone()];

        let out = try_execute(
            "select(.tags[]==1)",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row.clone(), row.clone()]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            "select(.name | test(\"a\"))",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row]),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn object_iteration_and_optional_accessors_follow_contract() {
        let out = try_execute(
            ".[]",
            &[serde_json::json!({"a": 1, "b": 2})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[]?",
            &[serde_json::json!(true)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[2]",
            &[serde_json::json!([0, 1])],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".[1]?",
            &[serde_json::json!({"a": 1})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn parser_supports_quoted_paths_and_rejects_broken_forms() {
        let out = try_execute(
            ".\"a b\"",
            &[serde_json::json!({"a b": 7})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert!(matches!(
            try_execute(".[", &[JsonValue::Null], RunOptions { null_input: false }),
            TryExecute::Unsupported
        ));
        assert!(matches!(
            try_execute(
                "select(.a | test(\"x\")",
                &[JsonValue::Null],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
        assert!(matches!(
            try_execute(
                "select(.a=1)",
                &[serde_json::json!({"a": 1})],
                RunOptions { null_input: false }
            ),
            TryExecute::Unsupported
        ));
    }

    #[test]
    fn comma_terms_in_intermediate_pipeline_clone_original_input() {
        let input = vec![serde_json::json!({"a": 1})];
        let out = try_execute("., .a | .", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![serde_json::json!({"a": 1}), serde_json::json!(1)]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn length_semantics_follow_jq_builtin_c() {
        let input = vec![
            serde_json::json!(null),
            serde_json::json!([1, 2]),
            serde_json::json!({"a": 1}),
            serde_json::json!("abμ"),
            serde_json::json!(-3.5),
        ];
        let out = try_execute("length", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(0),
                        serde_json::json!(2),
                        serde_json::json!(1),
                        serde_json::json!(3),
                        serde_json::json!(3.5),
                    ]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            "length",
            &[serde_json::json!(true)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "boolean (true) has no length");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn modulo_semantics_follow_jq_builtin_c() {
        let input = vec![
            serde_json::json!({"v": -5}),
            serde_json::json!({"v": 0}),
            serde_json::json!({"v": 7}),
        ];
        let out = try_execute(".v % 2", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(-1),
                        serde_json::json!(0),
                        serde_json::json!(1)
                    ]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(".v % -1", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(0),
                        serde_json::json!(0),
                        serde_json::json!(0)
                    ]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(".v % 0", &input, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(
                    err,
                    "cannot be divided (remainder) because the divisor is zero"
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            ".v % 2",
            &[serde_json::json!({"v":"x"})],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(
                    err,
                    "string (\"x\") and number (2) cannot be divided (remainder)"
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn select_ordering_semantics_follow_jv_cmp() {
        let input = vec![
            serde_json::json!({"v": null}),
            serde_json::json!({"v": false}),
            serde_json::json!({"v": true}),
            serde_json::json!({"v": 0}),
            serde_json::json!({"v": "0"}),
            serde_json::json!({"v": []}),
            serde_json::json!({"v": {}}),
        ];
        let out = try_execute(
            "select(.v > false) | .v",
            &input,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![
                        serde_json::json!(true),
                        serde_json::json!(0),
                        serde_json::json!("0"),
                        serde_json::json!([]),
                        serde_json::json!({}),
                    ]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let arrs = vec![
            serde_json::json!({"v":[1,2]}),
            serde_json::json!({"v":[1,2,0]}),
            serde_json::json!({"v":[1,3]}),
        ];
        let out = try_execute(
            "select(.v > [1,2]) | .v",
            &arrs,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![serde_json::json!([1, 2, 0]), serde_json::json!([1, 3])]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let objs = vec![
            serde_json::json!({"v":{"a":1}}),
            serde_json::json!({"v":{"a":1,"b":0}}),
            serde_json::json!({"v":{"a":2}}),
        ];
        let out = try_execute(
            r#"select(.v > {"a":1}) | .v"#,
            &objs,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(
                    values,
                    vec![serde_json::json!({"a":1,"b":0}), serde_json::json!({"a":2})]
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn gsub_basic_and_error_semantics_follow_jq() {
        let out = try_execute(
            r#"gsub("a";"x")"#,
            &[serde_json::json!("banana")],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => {
                assert_eq!(values, vec![serde_json::json!("bxnxnx")]);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            r#"gsub("a";"x")"#,
            &[serde_json::json!(1)],
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Err(err)) => {
                assert_eq!(err, "number (1) cannot be matched, as it is not a string");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn bench_regression_queries_are_covered() {
        let single = vec![serde_json::json!({
            "id": 8,
            "active": true,
            "group": 3,
            "value": 77,
            "a": 8,
            "b": 4,
            "text": "alpha-beta",
            "tags": [1, 2, 3]
        })];
        let many = vec![
            serde_json::json!({"id": 1, "active": true}),
            serde_json::json!({"id": 3, "active": true}),
            serde_json::json!({"id": 8, "active": true}),
        ];

        let out = try_execute(".value % 7", &single, RunOptions { null_input: false });
        assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

        let out = try_execute(
            r#".text | gsub("[aeiou]";"")"#,
            &single,
            RunOptions { null_input: false },
        );
        assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

        let out = try_execute(".tags | length", &single, RunOptions { null_input: false });
        assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

        let out = try_execute("select(.id > 2)", &many, RunOptions { null_input: false });
        assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

        let out = try_execute(".a + .b", &single, RunOptions { null_input: false });
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(12)]),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let out = try_execute(
            "{id,group,value}",
            &single,
            RunOptions { null_input: false },
        );
        match out {
            TryExecute::Executed(Ok(values)) => assert_eq!(
                values,
                vec![serde_json::json!({"id":8,"group":3,"value":77})]
            ),
            other => panic!("unexpected outcome: {other:?}"),
        }

        // Exact complex filter from stdin benchmark that previously failed as unsupported.
        let complex = r#"select(.active and (.id % 7 == 0)) | {id,group,score:(.a*3 + .b - (.value/10)),txt:(.text|ascii_downcase),ok:(.tags|length>2)}"#;
        let out = try_execute(complex, &single, RunOptions { null_input: false });
        assert!(matches!(out, TryExecute::Unsupported), "{out:?}");
    }

    #[test]
    fn support_probe_matches_parser() {
        assert!(is_supported(".a | .b"));
        assert!(!is_supported("map(.)"));
    }
}
