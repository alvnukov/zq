use super::doc_tape::{
    write_json_evaluated_line, DocNumberRef, DocTape, EvaluatedNode, JsonDocScratch,
    RootFieldFilter,
};
use super::JsonWriteOptions;
use super::vm_core::ast::{BinaryOp, Builtin};
use super::vm_core::ir::{Branch, Op, OpObjectKey, Program};
use super::vm_core::vm::{apply_binary, jq_run_length, jq_truthy};
use crate::c_compat::math as c_math;
use crate::c_compat::value::{type_name_jq, value_for_error_jq};
use crate::value::ZqValue;
use std::collections::BTreeSet;
use std::cmp::Ordering;
use std::io::Write;

#[derive(Clone)]
pub(crate) struct FastProgram {
    branches: Vec<FastBranch>,
    root_field_filter: Option<RootFieldFilter>,
}

#[derive(Clone)]
struct FastBranch {
    stages: Vec<FastStage>,
}

#[derive(Clone)]
enum FastStage {
    Expr(FastExpr),
    Select(FastExpr),
}

#[derive(Clone)]
enum FastExpr {
    Input,
    Literal(ZqValue),
    Pipe(Vec<FastExpr>),
    Path(Vec<PathStep>),
    Object(Vec<(String, FastExpr)>),
    Length(Box<FastExpr>),
    LiteralTest(String),
    Binary { op: BinaryOp, lhs: Box<FastExpr>, rhs: Box<FastExpr> },
}

#[derive(Clone)]
enum PathStep {
    Field { name: String, optional: bool },
    Index { index: i64, optional: bool },
}

impl FastProgram {
    pub(crate) fn compile(program: &Program) -> Option<Self> {
        if !program.functions.is_empty() {
            return None;
        }
        let mut branches = Vec::with_capacity(program.branches.len());
        for branch in &program.branches {
            branches.push(compile_branch(branch)?);
        }
        let root_field_filter = compile_root_field_filter(&branches);
        Some(Self { branches, root_field_filter })
    }

    pub(crate) fn execute_json_text_stream<F>(
        &self,
        input: &str,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let mut parser = serde_json::Deserializer::from_str(input);
        let mut scratch = JsonDocScratch::default();
        loop {
            match scratch.parse_json_with_root_filter(&mut parser, self.root_field_filter.as_ref()) {
                Ok(doc) => {
                    self.execute_doc(&doc, emit)?;
                    scratch.recycle(doc);
                }
                Err(err) if err.is_eof() => break,
                Err(err) => return Err(format!("json parse error: {err}")),
            }
        }
        Ok(())
    }

    pub(crate) fn execute_json_reader_stream<F>(
        &self,
        reader: Box<dyn std::io::Read + Send>,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        let mut parser = serde_json::Deserializer::from_reader(reader);
        let mut scratch = JsonDocScratch::default();
        loop {
            match scratch.parse_json_with_root_filter(&mut parser, self.root_field_filter.as_ref()) {
                Ok(doc) => {
                    self.execute_doc(&doc, emit)?;
                    scratch.recycle(doc);
                }
                Err(err) if err.is_eof() => break,
                Err(err) => return Err(format!("json parse error: {err}")),
            }
        }
        Ok(())
    }

    pub(crate) fn execute_json_reader_stream_write_json<W: Write>(
        &self,
        reader: Box<dyn std::io::Read + Send>,
        writer: &mut W,
        options: JsonWriteOptions,
    ) -> Result<(), String> {
        let mut parser = serde_json::Deserializer::from_reader(reader);
        let mut scratch = JsonDocScratch::default();
        let mut json_scratch = Vec::new();
        let pretty_indent = (!options.compact).then(|| vec![b' '; options.indent]);
        let mut wrote_any = false;
        loop {
            match scratch.parse_json_with_root_filter(&mut parser, self.root_field_filter.as_ref()) {
                Ok(doc) => {
                    self.execute_doc_write_json(
                        &doc,
                        writer,
                        &mut wrote_any,
                        &mut json_scratch,
                        pretty_indent.as_deref(),
                        options,
                    )?;
                    scratch.recycle(doc);
                }
                Err(err) if err.is_eof() => break,
                Err(err) => return Err(format!("json parse error: {err}")),
            }
        }
        if wrote_any && !options.join_output {
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn execute_doc<F>(&self, doc: &DocTape, emit: &mut F) -> Result<(), String>
    where
        F: FnMut(ZqValue) -> Result<(), String>,
    {
        for branch in &self.branches {
            if let Some(value) = branch.eval(doc)? {
                emit(value.into_owned())?;
            }
        }
        Ok(())
    }

    fn execute_doc_write_json<W: Write>(
        &self,
        doc: &DocTape,
        writer: &mut W,
        wrote_any: &mut bool,
        scratch: &mut Vec<u8>,
        pretty_indent: Option<&[u8]>,
        options: JsonWriteOptions,
    ) -> Result<(), String> {
        for branch in &self.branches {
            if let Some(value) = branch.eval(doc)? {
                if *wrote_any && !options.join_output {
                    writer.write_all(b"\n").map_err(|e| e.to_string())?;
                }
                write_json_evaluated_line(
                    writer,
                    &value,
                    options.compact,
                    options.raw_output,
                    scratch,
                    pretty_indent,
                )?;
                *wrote_any = true;
            }
        }
        Ok(())
    }
}

fn compile_root_field_filter(branches: &[FastBranch]) -> Option<RootFieldFilter> {
    let mut fields = BTreeSet::new();
    for branch in branches {
        let Some(branch_fields) = branch.required_root_fields() else {
            return None;
        };
        fields.extend(branch_fields);
    }
    Some(RootFieldFilter::from_names(fields.into_iter().collect()))
}

impl FastBranch {
    fn required_root_fields(&self) -> Option<BTreeSet<String>> {
        let mut fields = BTreeSet::new();
        let mut current_is_root = true;
        for stage in &self.stages {
            match stage {
                FastStage::Expr(expr) => {
                    let (stage_fields, next_is_root) = analyze_expr_root_fields(expr, current_is_root)?;
                    fields.extend(stage_fields);
                    current_is_root = next_is_root;
                }
                FastStage::Select(predicate) => {
                    let (predicate_fields, _) =
                        analyze_expr_root_fields(predicate, current_is_root)?;
                    fields.extend(predicate_fields);
                }
            }
        }
        if current_is_root {
            return None;
        }
        Some(fields)
    }

    fn eval<'a>(&self, doc: &'a DocTape) -> Result<Option<EvaluatedNode<'a>>, String> {
        let mut current = Some(EvaluatedNode::Node(doc.root()));
        for stage in &self.stages {
            let Some(input) = current.take() else {
                return Ok(None);
            };
            match stage {
                FastStage::Expr(expr) => {
                    current = eval_expr(expr, input)?;
                }
                FastStage::Select(predicate) => {
                    let Some(result) = eval_expr(predicate, input.clone())? else {
                        return Ok(None);
                    };
                    if jq_truthy_evaluated(&result) {
                        current = Some(input);
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(current)
    }
}

fn analyze_expr_root_fields(
    expr: &FastExpr,
    current_is_root: bool,
) -> Option<(BTreeSet<String>, bool)> {
    match expr {
        FastExpr::Input => Some((BTreeSet::new(), current_is_root)),
        FastExpr::Literal(_) => Some((BTreeSet::new(), false)),
        FastExpr::Pipe(items) => {
            let mut fields = BTreeSet::new();
            let mut current_is_root = current_is_root;
            for item in items {
                let (item_fields, next_is_root) = analyze_expr_root_fields(item, current_is_root)?;
                fields.extend(item_fields);
                current_is_root = next_is_root;
            }
            Some((fields, current_is_root))
        }
        FastExpr::Path(steps) => {
            if current_is_root {
                match steps.first() {
                    Some(PathStep::Field { name, .. }) => {
                        Some((BTreeSet::from([name.clone()]), false))
                    }
                    Some(PathStep::Index { .. }) => None,
                    None => Some((BTreeSet::new(), false)),
                }
            } else {
                Some((BTreeSet::new(), false))
            }
        }
        FastExpr::Object(fields) => {
            let mut deps = BTreeSet::new();
            for (_, value) in fields {
                let (field_deps, _) = analyze_expr_root_fields(value, current_is_root)?;
                deps.extend(field_deps);
            }
            Some((deps, false))
        }
        FastExpr::Length(inner) => {
            let (deps, output_is_root) = analyze_expr_root_fields(inner, current_is_root)?;
            (!output_is_root).then_some((deps, false))
        }
        FastExpr::LiteralTest(_) => (!current_is_root).then_some((BTreeSet::new(), false)),
        FastExpr::Binary { op: _, lhs, rhs } => {
            let (lhs_deps, lhs_is_root) = analyze_expr_root_fields(lhs, current_is_root)?;
            let (rhs_deps, rhs_is_root) = analyze_expr_root_fields(rhs, current_is_root)?;
            if lhs_is_root || rhs_is_root {
                return None;
            }
            let mut deps = lhs_deps;
            deps.extend(rhs_deps);
            Some((deps, false))
        }
    }
}

fn compile_branch(branch: &Branch) -> Option<FastBranch> {
    let mut stages = Vec::new();
    for op in &branch.ops {
        compile_stage(op, &mut stages)?;
    }
    Some(FastBranch { stages })
}

fn compile_stage(op: &Op, out: &mut Vec<FastStage>) -> Option<()> {
    match op {
        Op::Pipe(items) => {
            for item in items {
                compile_stage(item, out)?;
            }
            Some(())
        }
        Op::Select(predicate) => {
            out.push(FastStage::Select(compile_expr(predicate)?));
            Some(())
        }
        _ => {
            out.push(FastStage::Expr(compile_expr(op)?));
            Some(())
        }
    }
}

fn compile_expr(op: &Op) -> Option<FastExpr> {
    match op {
        Op::Identity => Some(FastExpr::Input),
        Op::Literal(value) => Some(FastExpr::Literal(value.clone())),
        Op::Pipe(items) => {
            let mut compiled = Vec::with_capacity(items.len());
            for item in items {
                compiled.push(compile_expr(item)?);
            }
            Some(FastExpr::Pipe(compiled))
        }
        Op::Builtin(Builtin::Length) => Some(FastExpr::Length(Box::new(FastExpr::Input))),
        Op::RegexMatch { spec, flags, test, .. } if *test && flags.is_none() => {
            let Op::Literal(ZqValue::String(pattern)) = spec.as_ref() else {
                return None;
            };
            if !is_plain_literal_pattern(pattern) {
                return None;
            }
            Some(FastExpr::LiteralTest(pattern.clone()))
        }
        Op::Binary { op, lhs, rhs } if is_supported_binary(*op) => Some(FastExpr::Binary {
            op: *op,
            lhs: Box::new(compile_expr(lhs)?),
            rhs: Box::new(compile_expr(rhs)?),
        }),
        Op::ObjectLiteral(entries) => {
            let mut fields = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let OpObjectKey::Static(name) = key else {
                    return None;
                };
                fields.push((name.clone(), compile_expr(value)?));
            }
            Some(FastExpr::Object(fields))
        }
        Op::GetField { .. } | Op::GetIndex { .. } | Op::Chain(_) => {
            Some(FastExpr::Path(compile_path_steps(op)?))
        }
        _ => None,
    }
}

fn compile_path_steps(op: &Op) -> Option<Vec<PathStep>> {
    match op {
        Op::GetField { name, optional } => {
            Some(vec![PathStep::Field { name: name.clone(), optional: *optional }])
        }
        Op::GetIndex { index, optional } => {
            Some(vec![PathStep::Index { index: *index, optional: *optional }])
        }
        Op::Chain(items) => {
            let mut steps = Vec::with_capacity(items.len());
            for item in items {
                steps.extend(compile_path_steps(item)?);
            }
            Some(steps)
        }
        _ => None,
    }
}

fn is_supported_binary(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Pow
            | BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
    )
}

fn eval_expr<'a>(
    expr: &FastExpr,
    input: EvaluatedNode<'a>,
) -> Result<Option<EvaluatedNode<'a>>, String> {
    match expr {
        FastExpr::Input => Ok(Some(input)),
        FastExpr::Literal(value) => Ok(Some(EvaluatedNode::Owned(value.clone()))),
        FastExpr::Pipe(items) => {
            let mut current = Some(input);
            for item in items {
                let Some(value) = current.take() else {
                    return Ok(None);
                };
                current = eval_expr(item, value)?;
            }
            Ok(current)
        }
        FastExpr::Path(steps) => eval_path(input, steps),
        FastExpr::Object(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for (key, value_expr) in fields {
                let Some(value) = eval_expr(value_expr, input.clone())? else {
                    return Ok(None);
                };
                out.push((key.clone(), value));
            }
            Ok(Some(EvaluatedNode::ProjectedObject(out)))
        }
        FastExpr::Length(inner) => {
            let Some(value) = eval_expr(inner, input)? else {
                return Ok(None);
            };
            let output = match value {
                EvaluatedNode::Node(node) => node.jq_length()?,
                EvaluatedNode::ProjectedObject(entries) => ZqValue::from(entries.len() as i64),
                EvaluatedNode::Owned(value) => jq_run_length(value)?,
            };
            Ok(Some(EvaluatedNode::Owned(output)))
        }
        FastExpr::LiteralTest(pattern) => {
            Ok(Some(EvaluatedNode::Owned(ZqValue::Bool(literal_test(input, pattern)?))))
        }
        FastExpr::Binary { op, lhs, rhs } => {
            let Some(lhs) = eval_expr(lhs, input.clone())? else {
                return Ok(None);
            };
            let Some(rhs) = eval_expr(rhs, input)? else {
                return Ok(None);
            };
            Ok(Some(EvaluatedNode::Owned(apply_binary_evaluated(*op, lhs, rhs)?)))
        }
    }
}

fn apply_binary_evaluated(
    op: BinaryOp,
    lhs: EvaluatedNode<'_>,
    rhs: EvaluatedNode<'_>,
) -> Result<ZqValue, String> {
    match op {
        BinaryOp::Eq => Ok(ZqValue::Bool(compare_evaluated_jq(&lhs, &rhs) == Ordering::Equal)),
        BinaryOp::Ne => Ok(ZqValue::Bool(compare_evaluated_jq(&lhs, &rhs) != Ordering::Equal)),
        BinaryOp::Gt => Ok(ZqValue::Bool(compare_evaluated_jq(&lhs, &rhs) == Ordering::Greater)),
        BinaryOp::Ge => {
            let ord = compare_evaluated_jq(&lhs, &rhs);
            Ok(ZqValue::Bool(ord == Ordering::Greater || ord == Ordering::Equal))
        }
        BinaryOp::Lt => Ok(ZqValue::Bool(compare_evaluated_jq(&lhs, &rhs) == Ordering::Less)),
        BinaryOp::Le => {
            let ord = compare_evaluated_jq(&lhs, &rhs);
            Ok(ZqValue::Bool(ord == Ordering::Less || ord == Ordering::Equal))
        }
        BinaryOp::Add => {
            if let Some(value) = fast_add_evaluated(&lhs, &rhs)? {
                return Ok(value);
            }
            apply_binary(op, lhs.into_owned(), rhs.into_owned())
        }
        BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod | BinaryOp::Pow => {
            if let Some(value) = fast_numeric_binary_evaluated(op, &lhs, &rhs)? {
                return Ok(value);
            }
            apply_binary(op, lhs.into_owned(), rhs.into_owned())
        }
        BinaryOp::And | BinaryOp::Or | BinaryOp::DefinedOr => {
            unreachable!("boolean/defined-or ops handled separately")
        }
    }
}

fn compare_evaluated_jq(lhs: &EvaluatedNode<'_>, rhs: &EvaluatedNode<'_>) -> Ordering {
    let lrank = evaluated_kind_rank(lhs);
    let rrank = evaluated_kind_rank(rhs);
    if lrank != rrank {
        return lrank.cmp(&rrank);
    }

    match (scalar_ref(lhs), scalar_ref(rhs)) {
        (Some(ScalarRef::Null), Some(ScalarRef::Null)) => Ordering::Equal,
        (Some(ScalarRef::Bool), Some(ScalarRef::Bool)) => Ordering::Equal,
        (Some(ScalarRef::Number(a)), Some(ScalarRef::Number(b))) => a.compare_jq(b),
        (Some(ScalarRef::String(a)), Some(ScalarRef::String(b))) => a.cmp(b),
        _ => crate::c_compat::value::compare_jq(&lhs.clone().into_owned(), &rhs.clone().into_owned()),
    }
}

fn fast_add_evaluated(
    lhs: &EvaluatedNode<'_>,
    rhs: &EvaluatedNode<'_>,
) -> Result<Option<ZqValue>, String> {
    if let (Some(left), Some(right)) = (number_ref(lhs), number_ref(rhs)) {
        let af = left.to_f64_lossy().ok_or_else(|| "number is out of range".to_string())?;
        let bf = right.to_f64_lossy().ok_or_else(|| "number is out of range".to_string())?;
        return Ok(Some(c_math::number_to_value_with_hint(af + bf, false)));
    }

    if let (Some(left), Some(right)) = (string_ref(lhs), string_ref(rhs)) {
        let mut out = String::with_capacity(left.len() + right.len());
        out.push_str(left);
        out.push_str(right);
        return Ok(Some(ZqValue::String(out)));
    }

    Ok(None)
}

fn fast_numeric_binary_evaluated(
    op: BinaryOp,
    lhs: &EvaluatedNode<'_>,
    rhs: &EvaluatedNode<'_>,
) -> Result<Option<ZqValue>, String> {
    let (Some(left), Some(right)) = (number_ref(lhs), number_ref(rhs)) else {
        return Ok(None);
    };
    let af = left.to_f64_lossy().ok_or_else(|| "number is out of range".to_string())?;
    let bf = right.to_f64_lossy().ok_or_else(|| "number is out of range".to_string())?;

    let result = match op {
        BinaryOp::Sub => Some(c_math::number_to_value_with_hint(af - bf, false)),
        BinaryOp::Mul => Some(c_math::number_to_value_with_hint(af * bf, false)),
        BinaryOp::Div => {
            if bf == 0.0 {
                None
            } else {
                Some(c_math::number_to_value_with_hint(af / bf, false))
            }
        }
        BinaryOp::Mod => {
            if bf == 0.0 {
                None
            } else {
                Some(c_math::number_to_value_with_hint(c_math::mod_compat(af, bf)?, false))
            }
        }
        BinaryOp::Pow => Some(c_math::number_to_value_with_hint(af.powf(bf), false)),
        _ => None,
    };
    Ok(result)
}

fn evaluated_kind_rank(value: &EvaluatedNode<'_>) -> i32 {
    match value {
        EvaluatedNode::Node(node) => node.jq_kind_rank(),
        EvaluatedNode::ProjectedObject(_) => 7,
        EvaluatedNode::Owned(value) => match value {
            ZqValue::Null => 1,
            ZqValue::Bool(false) => 2,
            ZqValue::Bool(true) => 3,
            ZqValue::Number(_) => 4,
            ZqValue::String(_) => 5,
            ZqValue::Array(_) => 6,
            ZqValue::Object(_) => 7,
        },
    }
}

#[derive(Clone, Copy)]
enum ScalarRef<'a> {
    Null,
    Bool,
    Number(NumberRef<'a>),
    String(&'a str),
}

fn scalar_ref<'a>(value: &'a EvaluatedNode<'a>) -> Option<ScalarRef<'a>> {
    match value {
        EvaluatedNode::Node(node) => {
            if node.is_null() {
                Some(ScalarRef::Null)
            } else if node.as_bool().is_some() {
                Some(ScalarRef::Bool)
            } else if let Some(number) = node.as_number() {
                Some(ScalarRef::Number(NumberRef::Doc(number)))
            } else {
                node.as_str().map(ScalarRef::String)
            }
        }
        EvaluatedNode::ProjectedObject(_) => None,
        EvaluatedNode::Owned(ZqValue::Null) => Some(ScalarRef::Null),
        EvaluatedNode::Owned(ZqValue::Bool(_)) => Some(ScalarRef::Bool),
        EvaluatedNode::Owned(ZqValue::Number(number)) => Some(ScalarRef::Number(NumberRef::Owned(number))),
        EvaluatedNode::Owned(ZqValue::String(text)) => Some(ScalarRef::String(text)),
        EvaluatedNode::Owned(ZqValue::Array(_) | ZqValue::Object(_)) => None,
    }
}

fn number_ref<'a>(value: &'a EvaluatedNode<'a>) -> Option<NumberRef<'a>> {
    match value {
        EvaluatedNode::Node(node) => node.as_number().map(NumberRef::Doc),
        EvaluatedNode::ProjectedObject(_) => None,
        EvaluatedNode::Owned(ZqValue::Number(number)) => Some(NumberRef::Owned(number)),
        EvaluatedNode::Owned(_) => None,
    }
}

#[derive(Clone, Copy)]
enum NumberRef<'a> {
    Doc(DocNumberRef<'a>),
    Owned(&'a serde_json::Number),
}

impl NumberRef<'_> {
    fn to_f64_lossy(self) -> Option<f64> {
        match self {
            NumberRef::Doc(number) => number.to_f64_lossy(),
            NumberRef::Owned(number) => c_math::jq_number_to_f64_lossy(number),
        }
    }

    fn compare_jq(self, other: Self) -> Ordering {
        match (self, other) {
            (NumberRef::Doc(left), NumberRef::Doc(right)) => left.compare_jq(right),
            (NumberRef::Owned(left), NumberRef::Owned(right)) => {
                c_math::compare_json_numbers_like_jq(left, right)
            }
            (left, right) => c_math::compare_json_numbers_like_jq(
                &left.to_json_number(),
                &right.to_json_number(),
            ),
        }
    }

    fn to_json_number(self) -> serde_json::Number {
        match self {
            NumberRef::Doc(number) => number.to_json_number(),
            NumberRef::Owned(number) => number.clone(),
        }
    }
}

fn string_ref<'a>(value: &'a EvaluatedNode<'a>) -> Option<&'a str> {
    match value {
        EvaluatedNode::Node(node) => node.as_str(),
        EvaluatedNode::ProjectedObject(_) => None,
        EvaluatedNode::Owned(ZqValue::String(text)) => Some(text.as_str()),
        EvaluatedNode::Owned(_) => None,
    }
}

fn jq_truthy_evaluated(value: &EvaluatedNode<'_>) -> bool {
    match value {
        EvaluatedNode::Node(node) => node.jq_truthy(),
        EvaluatedNode::ProjectedObject(_) => true,
        EvaluatedNode::Owned(value) => jq_truthy(value),
    }
}

fn literal_test<'a>(input: EvaluatedNode<'a>, pattern: &str) -> Result<bool, String> {
    match input {
        EvaluatedNode::Node(node) => match node.as_str() {
            Some(text) => Ok(text.contains(pattern)),
            None => {
                let value = node.materialize();
                Err(format!(
                    "{} ({}) cannot be matched, as it is not a string",
                    type_name_jq(&value),
                    value_for_error_jq(&value)
                ))
            }
        },
        EvaluatedNode::ProjectedObject(other) => {
            let value = EvaluatedNode::ProjectedObject(other).into_owned();
            Err(format!(
                "{} ({}) cannot be matched, as it is not a string",
                type_name_jq(&value),
                value_for_error_jq(&value)
            ))
        }
        EvaluatedNode::Owned(ZqValue::String(text)) => Ok(text.contains(pattern)),
        EvaluatedNode::Owned(other) => Err(format!(
            "{} ({}) cannot be matched, as it is not a string",
            type_name_jq(&other),
            value_for_error_jq(&other)
        )),
    }
}

fn is_plain_literal_pattern(pattern: &str) -> bool {
    !pattern.contains(|ch| {
        matches!(
            ch,
            '\\' | '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
        )
    })
}

fn eval_path<'a>(
    input: EvaluatedNode<'a>,
    steps: &[PathStep],
) -> Result<Option<EvaluatedNode<'a>>, String> {
    let mut current = input;
    for step in steps {
        let Some(next) = eval_path_step(current, step)? else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

fn eval_path_step<'a>(
    input: EvaluatedNode<'a>,
    step: &PathStep,
) -> Result<Option<EvaluatedNode<'a>>, String> {
    match (input, step) {
        (EvaluatedNode::Node(node), PathStep::Field { name, optional }) => {
            match node.lookup_field(name) {
                Some(value) => Ok(Some(EvaluatedNode::Node(value))),
                None if node.type_name() == "object" || node.type_name() == "null" => {
                    Ok(Some(EvaluatedNode::Owned(ZqValue::Null)))
                }
                None if *optional => Ok(None),
                None => Err(format!("Cannot index {} with string {:?}", node.type_name(), name)),
            }
        }
        (EvaluatedNode::Node(node), PathStep::Index { index, optional }) => {
            match node.lookup_index(*index) {
                Some(value) => Ok(Some(value)),
                None if node.type_name() == "array"
                    || node.type_name() == "string"
                    || node.type_name() == "null" =>
                {
                    Ok(Some(EvaluatedNode::Owned(ZqValue::Null)))
                }
                None if *optional => Ok(None),
                None => Err(format!("Cannot index {} with number", node.type_name())),
            }
        }
        (EvaluatedNode::ProjectedObject(entries), step) => {
            eval_path_step_owned(EvaluatedNode::ProjectedObject(entries).into_owned(), step)
        }
        (EvaluatedNode::Owned(value), step) => eval_path_step_owned(value, step),
    }
}

fn eval_path_step_owned<'a>(
    input: ZqValue,
    step: &PathStep,
) -> Result<Option<EvaluatedNode<'a>>, String> {
    match (input, step) {
        (ZqValue::Object(mut map), PathStep::Field { name, .. }) => {
            Ok(Some(EvaluatedNode::Owned(map.swap_remove(name).unwrap_or(ZqValue::Null))))
        }
        (ZqValue::Null, PathStep::Field { .. }) => Ok(Some(EvaluatedNode::Owned(ZqValue::Null))),
        (other, PathStep::Field { name, optional }) => {
            if *optional {
                Ok(None)
            } else {
                Err(format!("Cannot index {} with string {:?}", other.jq_type_name(), name))
            }
        }
        (ZqValue::Array(mut items), PathStep::Index { index, .. }) => {
            if let Some(normalized) =
                crate::c_compat::string::normalize_index_jq(items.len(), *index)
            {
                Ok(Some(EvaluatedNode::Owned(items.swap_remove(normalized))))
            } else {
                Ok(Some(EvaluatedNode::Owned(ZqValue::Null)))
            }
        }
        (ZqValue::String(text), PathStep::Index { index, .. }) => Ok(Some(EvaluatedNode::Owned(
            crate::c_compat::string::string_index_like_jq(&text, *index).unwrap_or(ZqValue::Null),
        ))),
        (ZqValue::Null, PathStep::Index { .. }) => Ok(Some(EvaluatedNode::Owned(ZqValue::Null))),
        (other, PathStep::Index { optional, .. }) => {
            if *optional {
                Ok(None)
            } else {
                Err(format!("Cannot index {} with number", other.jq_type_name()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_engine::vm_core::compile;
    use serde_json::json;

    fn eval_fast(query: &str, input: serde_json::Value) -> Vec<ZqValue> {
        let program = compile(query).expect("compile");
        let fast = FastProgram::compile(&program).expect("fast compile");
        let mut emit = Vec::new();
        let text = serde_json::to_string(&input).expect("json");
        fast.execute_json_text_stream(&text, &mut |value| {
            emit.push(value);
            Ok(())
        })
        .expect("fast execute");
        emit
    }

    #[test]
    fn compiles_projection_and_select_subset() {
        let projection = compile("{id,group,value}").expect("compile");
        assert!(FastProgram::compile(&projection).is_some());

        let select = compile("select(.id > 2) | .id").expect("compile");
        assert!(FastProgram::compile(&select).is_some());

        let literal_test = compile(".text | test(\"alpha\")").expect("compile");
        assert!(FastProgram::compile(&literal_test).is_some());

        let regex = compile(".text | test(\"a.*\")").expect("compile");
        assert!(FastProgram::compile(&regex).is_none());
    }

    #[test]
    fn executes_projection_without_materializing_input_tree() {
        let out = eval_fast("{id,group,value}", json!({"id":7,"group":2,"value":42,"skip":9}));
        assert_eq!(
            out,
            vec![ZqValue::Object(indexmap::indexmap! {
                "id".to_string() => ZqValue::from(7),
                "group".to_string() => ZqValue::from(2),
                "value".to_string() => ZqValue::from(42),
            })]
        );
    }

    #[test]
    fn executes_select_and_length_subset() {
        let selected = eval_fast("select(.id > 2) | .id", json!({"id":3,"tags":[1,2,3]}));
        assert_eq!(selected, vec![ZqValue::from(3)]);

        let length = eval_fast(".tags | length", json!({"tags":[1,2,3]}));
        assert_eq!(length, vec![ZqValue::from(3)]);
    }

    #[test]
    fn executes_borrowed_scalar_binary_subset() {
        let arith = eval_fast(".a + .b", json!({"a": 1, "b": 2}));
        assert_eq!(arith, vec![ZqValue::from(3)]);

        let modulo = eval_fast(".value % 7", json!({"value": 42}));
        assert_eq!(modulo, vec![ZqValue::from(0)]);

        let selected = eval_fast("select(.id == 3) | .id", json!({"id": 3}));
        assert_eq!(selected, vec![ZqValue::from(3)]);
    }

    #[test]
    fn executes_large_raw_integer_compare_subset() {
        let program = compile("select(.n > 1) | .n").expect("compile");
        let fast = FastProgram::compile(&program).expect("fast compile");
        let mut emit = Vec::new();
        let text = r#"{"n":123456789012345678901234567890}"#;
        fast.execute_json_text_stream(text, &mut |value| {
            emit.push(value);
            Ok(())
        })
        .expect("fast execute");
        assert_eq!(
            emit,
            vec![ZqValue::Number(serde_json::Number::from_string_unchecked(
                "123456789012345678901234567890".to_string()
            ))]
        );
    }

    #[test]
    fn executes_literal_test_subset() {
        let selected = eval_fast(
            "select(.text | test(\"alpha\")) | .id",
            json!({"id":7,"text":"pre-alpha-post"}),
        );
        assert_eq!(selected, vec![ZqValue::from(7)]);

        let rejected =
            eval_fast("select(.text | test(\"alpha\")) | .id", json!({"id":7,"text":"beta"}));
        assert!(rejected.is_empty());
    }

    #[test]
    fn literal_test_subset_matches_jq_error_shape() {
        let program = compile(".text | test(\"alpha\")").expect("compile");
        let fast = FastProgram::compile(&program).expect("fast compile");
        let text = serde_json::to_string(&json!({"text": null})).expect("json");
        let err =
            fast.execute_json_text_stream(&text, &mut |_value| Ok(())).expect_err("must fail");
        assert_eq!(err, "null (null) cannot be matched, as it is not a string");
    }
}
