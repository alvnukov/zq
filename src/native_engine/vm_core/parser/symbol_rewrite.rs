use crate::value::ZqValue;
use std::collections::BTreeMap;

use super::super::ast::{BindingKeySpec, BindingPattern, ObjectKey, Stage};

pub(super) fn wrap_with_import_bindings(mut stage: Stage, bindings: &[(String, ZqValue)]) -> Stage {
    for (name, value) in bindings.iter().rev() {
        stage = Stage::Bind {
            source: Box::new(Stage::Literal(value.clone())),
            pattern: BindingPattern::Var(name.clone()),
            body: Box::new(stage),
        };
    }
    stage
}

pub(super) fn rewrite_binding_key_spec_symbol_ids(
    key: &mut BindingKeySpec,
    function_id_map: &BTreeMap<usize, usize>,
    function_name_map: &BTreeMap<usize, String>,
    param_id_map: &BTreeMap<usize, usize>,
) {
    if let BindingKeySpec::Expr(expr) = key {
        rewrite_stage_symbol_ids(expr, function_id_map, function_name_map, param_id_map);
    }
}

pub(super) fn rewrite_binding_pattern_symbol_ids(
    pattern: &mut BindingPattern,
    function_id_map: &BTreeMap<usize, usize>,
    function_name_map: &BTreeMap<usize, String>,
    param_id_map: &BTreeMap<usize, usize>,
) {
    match pattern {
        BindingPattern::Var(_) => {}
        BindingPattern::Array(items) | BindingPattern::Alternatives(items) => {
            for item in items {
                rewrite_binding_pattern_symbol_ids(
                    item,
                    function_id_map,
                    function_name_map,
                    param_id_map,
                );
            }
        }
        BindingPattern::Object(entries) => {
            for entry in entries {
                rewrite_binding_key_spec_symbol_ids(
                    &mut entry.key,
                    function_id_map,
                    function_name_map,
                    param_id_map,
                );
                rewrite_binding_pattern_symbol_ids(
                    &mut entry.pattern,
                    function_id_map,
                    function_name_map,
                    param_id_map,
                );
            }
        }
    }
}

pub(super) fn rewrite_stage_symbol_ids(
    stage: &mut Stage,
    function_id_map: &BTreeMap<usize, usize>,
    function_name_map: &BTreeMap<usize, String>,
    param_id_map: &BTreeMap<usize, usize>,
) {
    match stage {
        Stage::Call { function_id, param_id, name, args } => {
            if let Some(old) = *function_id {
                if let Some(new_id) = function_id_map.get(&old).copied() {
                    *function_id = Some(new_id);
                    if let Some(new_name) = function_name_map.get(&old) {
                        *name = new_name.clone();
                    }
                }
            }
            if let Some(old) = *param_id {
                if let Some(new_id) = param_id_map.get(&old).copied() {
                    *param_id = Some(new_id);
                }
            }
            for arg in args {
                rewrite_stage_symbol_ids(arg, function_id_map, function_name_map, param_id_map);
            }
        }
        Stage::Chain(items)
        | Stage::Pipe(items)
        | Stage::Comma(items)
        | Stage::ArrayLiteral(items) => {
            for item in items {
                rewrite_stage_symbol_ids(item, function_id_map, function_name_map, param_id_map);
            }
        }
        Stage::ObjectLiteral(entries) => {
            for (key, value) in entries {
                if let ObjectKey::Expr(expr) = key {
                    rewrite_stage_symbol_ids(
                        expr,
                        function_id_map,
                        function_name_map,
                        param_id_map,
                    );
                }
                rewrite_stage_symbol_ids(value, function_id_map, function_name_map, param_id_map);
            }
        }
        Stage::RegexMatch { spec, flags, .. } | Stage::RegexCapture { spec, flags, .. } => {
            rewrite_stage_symbol_ids(spec, function_id_map, function_name_map, param_id_map);
            if let Some(flags) = flags {
                rewrite_stage_symbol_ids(flags, function_id_map, function_name_map, param_id_map);
            }
        }
        Stage::RegexScan { regex, flags } | Stage::RegexSplits { regex, flags } => {
            rewrite_stage_symbol_ids(regex, function_id_map, function_name_map, param_id_map);
            if let Some(flags) = flags {
                rewrite_stage_symbol_ids(flags, function_id_map, function_name_map, param_id_map);
            }
        }
        Stage::RegexSub { regex, replacement, flags, .. } => {
            rewrite_stage_symbol_ids(regex, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(replacement, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(flags, function_id_map, function_name_map, param_id_map);
        }
        Stage::Label { body, .. } | Stage::Format { expr: body, .. } => {
            rewrite_stage_symbol_ids(body, function_id_map, function_name_map, param_id_map);
        }
        Stage::Has(inner)
        | Stage::In(inner)
        | Stage::StartsWith(inner)
        | Stage::EndsWith(inner)
        | Stage::Split(inner)
        | Stage::Join(inner)
        | Stage::LTrimStr(inner)
        | Stage::RTrimStr(inner)
        | Stage::TrimStr(inner)
        | Stage::Indices(inner)
        | Stage::IndexOf(inner)
        | Stage::RIndexOf(inner)
        | Stage::Contains(inner)
        | Stage::Inside(inner)
        | Stage::BSearch(inner)
        | Stage::SortByImpl(inner)
        | Stage::GroupByImpl(inner)
        | Stage::UniqueByImpl(inner)
        | Stage::MinByImpl(inner)
        | Stage::MaxByImpl(inner)
        | Stage::Path(inner)
        | Stage::GetPath(inner)
        | Stage::DelPaths(inner)
        | Stage::TruncateStream(inner)
        | Stage::FromStream(inner)
        | Stage::Flatten(inner)
        | Stage::FlattenRaw(inner)
        | Stage::Nth(inner)
        | Stage::FirstBy(inner)
        | Stage::LastBy(inner)
        | Stage::IsEmpty(inner)
        | Stage::AddBy(inner)
        | Stage::Select(inner)
        | Stage::Map(inner)
        | Stage::MapValues(inner)
        | Stage::WithEntries(inner)
        | Stage::RecurseBy(inner)
        | Stage::Walk(inner)
        | Stage::Repeat(inner)
        | Stage::Strptime(inner)
        | Stage::Error(inner)
        | Stage::HaltError(inner)
        | Stage::UnaryMinus(inner)
        | Stage::UnaryNot(inner) => {
            rewrite_stage_symbol_ids(inner, function_id_map, function_name_map, param_id_map);
        }
        Stage::SetPath(lhs, rhs)
        | Stage::Modify(lhs, rhs)
        | Stage::NthBy(lhs, rhs)
        | Stage::LimitBy(lhs, rhs)
        | Stage::SkipBy(lhs, rhs)
        | Stage::While(lhs, rhs)
        | Stage::Until(lhs, rhs)
        | Stage::Any(lhs, rhs)
        | Stage::All(lhs, rhs)
        | Stage::RecurseByCond(lhs, rhs) => {
            rewrite_stage_symbol_ids(lhs, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(rhs, function_id_map, function_name_map, param_id_map);
        }
        Stage::Range(start, end, step) => {
            rewrite_stage_symbol_ids(start, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(end, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(step, function_id_map, function_name_map, param_id_map);
        }
        Stage::Reduce { source, pattern, init, update } => {
            rewrite_stage_symbol_ids(source, function_id_map, function_name_map, param_id_map);
            rewrite_binding_pattern_symbol_ids(
                pattern,
                function_id_map,
                function_name_map,
                param_id_map,
            );
            rewrite_stage_symbol_ids(init, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(update, function_id_map, function_name_map, param_id_map);
        }
        Stage::Foreach { source, pattern, init, update, extract } => {
            rewrite_stage_symbol_ids(source, function_id_map, function_name_map, param_id_map);
            rewrite_binding_pattern_symbol_ids(
                pattern,
                function_id_map,
                function_name_map,
                param_id_map,
            );
            rewrite_stage_symbol_ids(init, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(update, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(extract, function_id_map, function_name_map, param_id_map);
        }
        Stage::TryCatch { inner, catcher } => {
            rewrite_stage_symbol_ids(inner, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(catcher, function_id_map, function_name_map, param_id_map);
        }
        Stage::IfElse { cond, then_expr, else_expr } => {
            rewrite_stage_symbol_ids(cond, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(then_expr, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(else_expr, function_id_map, function_name_map, param_id_map);
        }
        Stage::Binary { lhs, rhs, .. } => {
            rewrite_stage_symbol_ids(lhs, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(rhs, function_id_map, function_name_map, param_id_map);
        }
        Stage::MathBinary { lhs, rhs, .. } => {
            rewrite_stage_symbol_ids(lhs, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(rhs, function_id_map, function_name_map, param_id_map);
        }
        Stage::MathTernary { a, b, c, .. } => {
            rewrite_stage_symbol_ids(a, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(b, function_id_map, function_name_map, param_id_map);
            rewrite_stage_symbol_ids(c, function_id_map, function_name_map, param_id_map);
        }
        Stage::Bind { source, pattern, body } => {
            rewrite_stage_symbol_ids(source, function_id_map, function_name_map, param_id_map);
            rewrite_binding_pattern_symbol_ids(
                pattern,
                function_id_map,
                function_name_map,
                param_id_map,
            );
            rewrite_stage_symbol_ids(body, function_id_map, function_name_map, param_id_map);
        }
        _ => {}
    }
}

pub(super) fn collect_pattern_bindings(pattern: &BindingPattern, out: &mut Vec<String>) {
    match pattern {
        BindingPattern::Var(name) => push_unique_binding(out, name),
        BindingPattern::Array(items) => {
            for item in items {
                collect_pattern_bindings(item, out);
            }
        }
        BindingPattern::Object(entries) => {
            for entry in entries {
                if let Some(name) = &entry.store_var {
                    push_unique_binding(out, name);
                }
                collect_pattern_bindings(&entry.pattern, out);
            }
        }
        BindingPattern::Alternatives(items) => {
            for item in items {
                collect_pattern_bindings(item, out);
            }
        }
    }
}

pub(super) fn push_unique_binding(out: &mut Vec<String>, name: &str) {
    if out.iter().all(|existing| existing != name) {
        out.push(name.to_string());
    }
}
