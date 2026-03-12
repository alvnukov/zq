use super::super::ast::{BinaryOp, Builtin, Stage};
use super::BracketBound;
use crate::value::ZqValue;
use indexmap::IndexMap;

pub(super) fn select_stage(predicate: Stage) -> Stage {
    Stage::Select(Box::new(predicate))
}

pub(super) fn type_eq_stage(expected: &str) -> Stage {
    Stage::Binary {
        op: BinaryOp::Eq,
        lhs: Box::new(Stage::Builtin(Builtin::Type)),
        rhs: Box::new(Stage::Literal(ZqValue::String(expected.to_string()))),
    }
}

pub(super) fn type_ne_stage(expected: &str) -> Stage {
    Stage::Binary {
        op: BinaryOp::Ne,
        lhs: Box::new(Stage::Builtin(Builtin::Type)),
        rhs: Box::new(Stage::Literal(ZqValue::String(expected.to_string()))),
    }
}

pub(super) fn by_impl_keys_stage(filter: Stage) -> Stage {
    // jq/src/builtin.jq:
    // map([f]) evaluates `f` for each item and captures its whole output stream as a key tuple.
    Stage::Map(Box::new(Stage::ArrayLiteral(vec![filter])))
}

pub(super) fn abs_stage() -> Stage {
    // jq builtin.jq:
    // def abs: if . < 0 then - . else . end;
    Stage::IfElse {
        cond: Box::new(Stage::Binary {
            op: BinaryOp::Lt,
            lhs: Box::new(Stage::Identity),
            rhs: Box::new(Stage::Literal(ZqValue::from(0))),
        }),
        then_expr: Box::new(Stage::UnaryMinus(Box::new(Stage::Identity))),
        else_expr: Box::new(Stage::Identity),
    }
}

pub(super) fn loc_stage(line: usize) -> Stage {
    Stage::Literal(ZqValue::Object(IndexMap::from([
        ("file".to_string(), ZqValue::String("<top-level>".to_string())),
        ("line".to_string(), ZqValue::from(line as i64)),
    ])))
}

pub(super) fn bracket_bound_to_stage(bound: BracketBound) -> Stage {
    match bound {
        BracketBound::Static(v) => Stage::Literal(ZqValue::from(v)),
        BracketBound::Dynamic(expr) => expr,
    }
}

pub(super) fn append_chain_stage(lhs: Stage, rhs: Stage) -> Stage {
    match lhs {
        Stage::Chain(mut stages) => {
            stages.push(rhs);
            Stage::Chain(stages)
        }
        other => Stage::Chain(vec![other, rhs]),
    }
}

pub(super) fn isfinite_stage() -> Stage {
    // jq builtin.jq:
    // def isfinite: type == "number" and (isinfinite | not) and (isnan | not);
    Stage::Binary {
        op: BinaryOp::And,
        lhs: Box::new(type_eq_stage("number")),
        rhs: Box::new(Stage::Binary {
            op: BinaryOp::And,
            lhs: Box::new(Stage::UnaryNot(Box::new(Stage::Builtin(Builtin::IsInfinite)))),
            rhs: Box::new(Stage::UnaryNot(Box::new(Stage::Builtin(Builtin::IsNan)))),
        }),
    }
}
