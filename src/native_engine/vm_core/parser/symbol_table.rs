use super::*;

impl Parser {
    pub(super) fn pop_def_scope(&mut self) {
        let Some(bindings) = self.def_scope_stack.pop() else {
            return;
        };
        for signature in bindings.into_iter().rev() {
            let remove_key = if let Some(stack) = self.function_bindings.get_mut(&signature) {
                let _ = stack.pop();
                stack.is_empty()
            } else {
                false
            };
            if remove_key {
                self.function_bindings.remove(&signature);
            }
        }
    }

    pub(super) fn push_function_binding(&mut self, signature: (String, usize), function_id: usize) {
        self.function_bindings
            .entry(signature.clone())
            .or_default()
            .push(function_id);
        if let Some(scope) = self.def_scope_stack.last_mut() {
            scope.push(signature);
        }
    }

    pub(super) fn resolve_param_call(&self, name: &str, arity: usize) -> Option<usize> {
        if arity != 0 {
            return None;
        }
        let signature = (name.to_string(), arity);
        self.local_param_scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(&signature).copied())
    }

    pub(super) fn resolve_user_function(&self, name: &str, arity: usize) -> Option<usize> {
        self.function_bindings
            .get(&(name.to_string(), arity))
            .and_then(|stack| stack.last().copied())
    }
}
