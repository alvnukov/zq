use super::doc_tape::{DocTape, EvaluatedNode, JsonDocScratch};
use super::vm_core::ast::{BinaryOp, Builtin};
use super::vm_core::ir::{Branch, Op, OpObjectKey, Program};
use super::vm_core::vm::{apply_binary, jq_run_length, jq_truthy};
use crate::c_compat::value::{type_name_jq, value_for_error_jq};
use crate::value::{take_pooled_object_map_with_capacity, ZqValue};

#[derive(Clone)]
pub(crate) struct FastProgram {
    branches: Vec<FastBranch>,
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
        Some(Self { branches })
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
            match scratch.parse_json(&mut parser) {
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
            match scratch.parse_json(&mut parser) {
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
}

impl FastBranch {
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
                    if jq_truthy(&result.into_owned()) {
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
            let mut out = take_pooled_object_map_with_capacity(fields.len());
            for (key, value_expr) in fields {
                let Some(value) = eval_expr(value_expr, input.clone())? else {
                    return Ok(None);
                };
                out.insert(key.clone(), value.into_owned());
            }
            Ok(Some(EvaluatedNode::Owned(ZqValue::Object(out))))
        }
        FastExpr::Length(inner) => {
            let Some(value) = eval_expr(inner, input)? else {
                return Ok(None);
            };
            Ok(Some(EvaluatedNode::Owned(jq_run_length(value.into_owned())?)))
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
            Ok(Some(EvaluatedNode::Owned(apply_binary(*op, lhs.into_owned(), rhs.into_owned())?)))
        }
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
