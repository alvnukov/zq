use super::*;

impl Parser {
    fn function_by_id(&self, function_id: usize) -> Option<&FunctionDef> {
        self.functions
            .iter()
            .find(|function| function.id == function_id)
    }

    fn stage_is_param_filter_call(stage: &Stage, param_id: usize) -> bool {
        matches!(
            stage,
            Stage::Call {
                function_id: None,
                param_id: Some(id),
                args,
                ..
            } if *id == param_id && args.is_empty()
        )
    }

    fn stage_is_zero_arity_self_call(stage: &Stage, function_id: usize) -> bool {
        matches!(
            stage,
            Stage::Call {
                function_id: Some(id),
                param_id: None,
                args,
                ..
            } if *id == function_id && args.is_empty()
        )
    }

    pub(super) fn is_canonical_while_def(&self, function_id: usize) -> bool {
        let Some(function) = self.function_by_id(function_id) else {
            return false;
        };
        if function.name != "while" || function.arity != 2 || function.param_ids.len() != 2 {
            return false;
        }
        let cond_param_id = function.param_ids[0];
        let update_param_id = function.param_ids[1];
        let helper_id = match &function.body {
            Stage::Call {
                function_id: Some(helper_id),
                param_id: None,
                args,
                ..
            } if args.is_empty() => *helper_id,
            _ => return false,
        };
        let Some(helper) = self.function_by_id(helper_id) else {
            return false;
        };
        if helper.arity != 0 {
            return false;
        }
        match &helper.body {
            Stage::IfElse {
                cond,
                then_expr,
                else_expr,
            } => {
                if !Self::stage_is_param_filter_call(cond, cond_param_id)
                    || !matches!(else_expr.as_ref(), Stage::Empty)
                {
                    return false;
                }
                let Stage::Comma(items) = then_expr.as_ref() else {
                    return false;
                };
                if items.len() != 2 || !matches!(&items[0], Stage::Identity) {
                    return false;
                }
                let Stage::Pipe(stages) = &items[1] else {
                    return false;
                };
                if stages.len() != 2 {
                    return false;
                }
                Self::stage_is_param_filter_call(&stages[0], update_param_id)
                    && Self::stage_is_zero_arity_self_call(&stages[1], helper_id)
            }
            _ => false,
        }
    }

    pub(super) fn is_canonical_until_def(&self, function_id: usize) -> bool {
        let Some(function) = self.function_by_id(function_id) else {
            return false;
        };
        if function.name != "until" || function.arity != 2 || function.param_ids.len() != 2 {
            return false;
        }
        let cond_param_id = function.param_ids[0];
        let next_param_id = function.param_ids[1];
        let helper_id = match &function.body {
            Stage::Call {
                function_id: Some(helper_id),
                param_id: None,
                args,
                ..
            } if args.is_empty() => *helper_id,
            _ => return false,
        };
        let Some(helper) = self.function_by_id(helper_id) else {
            return false;
        };
        if helper.arity != 0 {
            return false;
        }
        match &helper.body {
            Stage::IfElse {
                cond,
                then_expr,
                else_expr,
            } => {
                if !Self::stage_is_param_filter_call(cond, cond_param_id)
                    || !matches!(then_expr.as_ref(), Stage::Identity)
                {
                    return false;
                }
                let Stage::Pipe(stages) = else_expr.as_ref() else {
                    return false;
                };
                if stages.len() != 2 {
                    return false;
                }
                Self::stage_is_param_filter_call(&stages[0], next_param_id)
                    && Self::stage_is_zero_arity_self_call(&stages[1], helper_id)
            }
            _ => false,
        }
    }
}
