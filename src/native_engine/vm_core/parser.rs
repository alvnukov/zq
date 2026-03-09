use super::ast::{
    BinaryOp, BindingKeySpec, BindingPattern, Builtin, FunctionDef, MathBinaryOp, MathTernaryOp,
    ObjectBindingEntry, ObjectKey, Pipeline, Query, Stage,
};
use super::lexer::{lex, Token};
use crate::value::ZqValue;
use indexmap::IndexMap;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

mod builtin_lowering;
mod literal_utils;
mod module_directives;
mod module_loading;
mod module_resolve;
mod stage_helpers;
mod symbol_rewrite;
mod symbol_table;

use self::literal_utils::{
    const_object_key_error, fold_large_integer_literal_equality, parse_number_literal,
    special_number_literal,
};
#[cfg(test)]
use self::literal_utils::{normalize_jq_float_text, zq_type_name};
pub(crate) use self::module_resolve::default_module_search_dirs;
use self::module_resolve::{
    canonicalize_module_candidate, const_stage_value, ensure_const_object_metadata,
    home_dir_like_jq, import_metadata_from_object, jq_origin_dir, module_code_candidates,
    module_data_candidates, module_path_literal, normalize_search_root_like_jq,
    validate_module_relpath,
};
use self::stage_helpers::{
    abs_stage, append_chain_stage, bracket_bound_to_stage, by_impl_keys_stage, isfinite_stage,
    loc_stage, select_stage, type_eq_stage, type_ne_stage,
};
#[cfg(test)]
use self::symbol_rewrite::rewrite_binding_pattern_symbol_ids;
use self::symbol_rewrite::{
    collect_pattern_bindings, push_unique_binding, rewrite_stage_symbol_ids,
    wrap_with_import_bindings,
};

const MAX_LOCAL_FUNCTION_PARAMETERS_OR_DEFS: usize = 4095;

pub(crate) fn parse_query(query: &str) -> Result<Query, String> {
    let tokens = lex(query).map_err(|e| format!("lex error at {}: {}", e.position, e.message))?;
    let mut parser = Parser::new(tokens);
    parser.parse_query()
}

pub(crate) fn parse_query_with_module_dirs(
    query: &str,
    module_search_dirs: Vec<PathBuf>,
) -> Result<Query, String> {
    let tokens = lex(query).map_err(|e| format!("lex error at {}: {}", e.position, e.message))?;
    let mut parser = Parser::new_with_context(tokens, module_search_dirs, None, 0);
    parser.parse_query()
}

pub(crate) fn load_module_meta(
    module: &str,
    module_search_dirs: Vec<PathBuf>,
) -> Result<ZqValue, String> {
    let resolver = Parser::new_with_context(Vec::new(), module_search_dirs.clone(), None, 0);
    let module_path = resolver.resolve_module_code_path(module, None)?;
    // jq/src/linker.c:load_module_meta() returns null on parse/load failures,
    // but propagates resolution errors (module not found).
    let source = match fs::read_to_string(&module_path) {
        Ok(source) => source,
        Err(_) => return Ok(ZqValue::Null),
    };
    let tokens = match lex(&source) {
        Ok(tokens) => tokens,
        Err(_) => return Ok(ZqValue::Null),
    };
    let current_module_dir = module_path.parent().map(Path::to_path_buf);
    let mut parser = Parser::new_with_context(tokens, module_search_dirs, current_module_dir, 0);
    parser.resolve_imports = false;
    parser.allow_unresolved_calls = true;
    if parser.parse_query().is_err() {
        return Ok(ZqValue::Null);
    }
    Ok(parser.module_descriptor_value())
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    function_bindings: BTreeMap<(String, usize), Vec<usize>>,
    next_function_id: usize,
    local_param_scopes: Vec<BTreeMap<(String, usize), usize>>,
    next_param_id: usize,
    functions: Vec<FunctionDef>,
    def_scope_stack: Vec<Vec<(String, usize)>>,
    label_stack: Vec<String>,
    binding_scopes: Vec<Vec<String>>,
    imported_bindings: Vec<(String, ZqValue)>,
    module_search_dirs: Vec<PathBuf>,
    current_module_dir: Option<PathBuf>,
    module_load_depth: usize,
    module_query_cache: Rc<RefCell<BTreeMap<PathBuf, Query>>>,
    resolve_imports: bool,
    allow_unresolved_calls: bool,
    module_decl_meta: Option<ZqValue>,
    module_decl_deps: Vec<ZqValue>,
    local_function_defs: Vec<String>,
}

#[derive(Debug, Clone)]
enum BracketBound {
    Static(i64),
    Dynamic(Stage),
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self::new_with_context_and_cache(
            tokens,
            default_module_search_dirs(),
            None,
            0,
            Rc::new(RefCell::new(BTreeMap::new())),
        )
    }

    fn new_with_context(
        tokens: Vec<Token>,
        module_search_dirs: Vec<PathBuf>,
        current_module_dir: Option<PathBuf>,
        module_load_depth: usize,
    ) -> Self {
        Self::new_with_context_and_cache(
            tokens,
            module_search_dirs,
            current_module_dir,
            module_load_depth,
            Rc::new(RefCell::new(BTreeMap::new())),
        )
    }

    fn new_with_context_and_cache(
        tokens: Vec<Token>,
        module_search_dirs: Vec<PathBuf>,
        current_module_dir: Option<PathBuf>,
        module_load_depth: usize,
        module_query_cache: Rc<RefCell<BTreeMap<PathBuf, Query>>>,
    ) -> Self {
        Self {
            tokens,
            pos: 0,
            function_bindings: BTreeMap::new(),
            next_function_id: 0,
            local_param_scopes: Vec::new(),
            next_param_id: 0,
            functions: Vec::new(),
            def_scope_stack: Vec::new(),
            label_stack: Vec::new(),
            binding_scopes: Vec::new(),
            imported_bindings: Vec::new(),
            module_search_dirs,
            current_module_dir,
            module_load_depth,
            module_query_cache,
            resolve_imports: true,
            allow_unresolved_calls: false,
            module_decl_meta: None,
            module_decl_deps: Vec::new(),
            local_function_defs: Vec::new(),
        }
    }

    fn parse_query(&mut self) -> Result<Query, String> {
        self.binding_scopes.push(Vec::new());
        self.preload_home_module_functions()?;
        let mut stage = self.parse_query_with_defs()?;
        self.expect(Token::End)?;
        let _ = self.binding_scopes.pop();
        let imported_bindings = std::mem::take(&mut self.imported_bindings);
        if !imported_bindings.is_empty() {
            stage = wrap_with_import_bindings(stage, &imported_bindings);
            for function in &mut self.functions {
                function.body =
                    wrap_with_import_bindings(function.body.clone(), &imported_bindings);
            }
        }
        Ok(Query {
            branches: vec![Pipeline {
                stages: vec![stage],
            }],
            functions: std::mem::take(&mut self.functions),
        })
    }

    fn preload_home_module_functions(&mut self) -> Result<(), String> {
        // jq/src/linker.c:load_program() implicitly imports ~/.jq as optional
        // top-level library named "".
        if !self.resolve_imports || self.module_load_depth != 0 {
            return Ok(());
        }
        let Some(home_dir) = home_dir_like_jq() else {
            return Ok(());
        };
        let search_dirs = vec![home_dir];
        let query = match self.load_module_query("", Some(search_dirs.as_slice())) {
            Ok(query) => query,
            Err(err) if err.starts_with("module not found:") => return Ok(()),
            Err(err) => return Err(err),
        };
        self.import_module_functions(query.functions, None)
    }

    fn parse_query_with_defs(&mut self) -> Result<Stage, String> {
        self.def_scope_stack.push(Vec::new());
        let mut saw_top_level_declaration = false;
        loop {
            match self.peek() {
                Token::DefKw => {
                    saw_top_level_declaration = true;
                    let function = self.parse_function_def()?;
                    let signature = format!("{}/{}", function.name, function.arity);
                    if !self
                        .local_function_defs
                        .iter()
                        .any(|seen| seen == &signature)
                    {
                        self.local_function_defs.push(signature);
                        if self.local_function_defs.len() > MAX_LOCAL_FUNCTION_PARAMETERS_OR_DEFS {
                            return Err(
                                "too many function parameters or local function definitions (max 4095)"
                                    .to_string(),
                            );
                        }
                    }
                    self.functions.push(function);
                }
                Token::ModuleKw | Token::ImportKw | Token::IncludeKw => {
                    saw_top_level_declaration = true;
                    self.parse_module_directive()?;
                }
                _ => break,
            }
        }
        let stage = if self.peek() == &Token::End {
            if saw_top_level_declaration
                && self.module_load_depth == 0
                && self.current_module_dir.is_none()
            {
                Err("Top-level program not given (try \".\")".to_string())
            } else {
                Ok(Stage::Identity)
            }
        } else {
            self.parse_pipe_expr()
        };
        self.pop_def_scope();
        stage
    }

    fn import_module_functions(
        &mut self,
        functions: Vec<FunctionDef>,
        namespace: Option<&str>,
    ) -> Result<(), String> {
        let mut function_id_map = BTreeMap::new();
        let mut function_name_map = BTreeMap::new();
        for function in &functions {
            let new_id = self.next_function_id;
            self.next_function_id += 1;
            function_id_map.insert(function.id, new_id);
            let new_name = match namespace {
                Some(ns) => format!("{ns}::{}", function.name),
                None => function.name.clone(),
            };
            function_name_map.insert(function.id, new_name);
        }

        let mut param_id_map = BTreeMap::new();
        for function in &functions {
            for param_id in &function.param_ids {
                if !param_id_map.contains_key(param_id) {
                    let new_param_id = self.next_param_id;
                    self.next_param_id += 1;
                    param_id_map.insert(*param_id, new_param_id);
                }
            }
        }

        for mut function in functions {
            let old_id = function.id;
            function.id = *function_id_map
                .get(&old_id)
                .expect("imported function id must be mapped");
            function.name = function_name_map
                .get(&old_id)
                .expect("imported function name must be mapped")
                .clone();
            for param_id in &mut function.param_ids {
                *param_id = *param_id_map
                    .get(param_id)
                    .expect("imported param id must be mapped");
            }
            rewrite_stage_symbol_ids(
                &mut function.body,
                &function_id_map,
                &function_name_map,
                &param_id_map,
            );
            self.push_function_binding((function.name.clone(), function.arity), function.id);
            self.functions.push(function);
        }
        Ok(())
    }

    fn parse_function_def(&mut self) -> Result<FunctionDef, String> {
        self.expect(Token::DefKw)?;
        let name = match self.peek() {
            Token::Ident(name) => {
                let name = name.clone();
                self.bump();
                name
            }
            other => {
                return Err(format!(
                    "parse error: expected function name after `def`, found {:?}",
                    other
                ))
            }
        };
        let mut params = Vec::new();
        let mut regular_params = Vec::new();
        if self.peek() == &Token::LParen {
            self.bump();
            if self.peek() == &Token::RParen {
                return Err("parse error: expected parameter name before `)`".to_string());
            }
            loop {
                match self.peek() {
                    Token::Binding(param) => {
                        let param = param.clone();
                        self.bump();
                        regular_params.push(param.clone());
                        params.push(param);
                    }
                    Token::Ident(param) => {
                        let param = param.clone();
                        self.bump();
                        params.push(param);
                    }
                    other => {
                        return Err(format!(
                            "parse error: expected parameter name in function definition, found {:?}",
                            other
                        ));
                    }
                }
                if self.peek() == &Token::Semi {
                    self.bump();
                    continue;
                }
                break;
            }
            self.expect(Token::RParen)?;
        }
        if params.len() > MAX_LOCAL_FUNCTION_PARAMETERS_OR_DEFS {
            return Err(
                "too many function parameters or local function definitions (max 4095)".to_string(),
            );
        }
        self.expect(Token::Colon)?;
        let function_id = self.next_function_id;
        self.next_function_id += 1;
        self.push_function_binding((name.clone(), params.len()), function_id);
        let mut local_scope = BTreeMap::new();
        let mut param_ids = BTreeMap::new();
        let mut param_ids_in_order = Vec::with_capacity(params.len());
        for param in &params {
            let param_id = self.next_param_id;
            self.next_param_id += 1;
            local_scope.insert((param.clone(), 0), param_id);
            param_ids.insert(param.clone(), param_id);
            param_ids_in_order.push(param_id);
        }
        self.local_param_scopes.push(local_scope);
        let mut param_bindings = Vec::new();
        for param in &regular_params {
            push_unique_binding(&mut param_bindings, param);
        }
        self.binding_scopes.push(param_bindings);
        let body_result = self.parse_query_with_defs();
        let _ = self.binding_scopes.pop();
        self.local_param_scopes.pop();
        let mut body = body_result?;
        self.expect(Token::Semi)?;
        for param in regular_params.into_iter().rev() {
            let Some(param_id) = param_ids.get(&param).copied() else {
                return Err(format!(
                    "parse error: missing function parameter binding for `{param}`"
                ));
            };
            body = Stage::Bind {
                source: Box::new(Stage::Call {
                    function_id: None,
                    param_id: Some(param_id),
                    name: param.clone(),
                    args: Vec::new(),
                }),
                pattern: BindingPattern::Var(param.clone()),
                body: Box::new(body),
            };
        }
        Ok(FunctionDef {
            id: function_id,
            name,
            arity: params.len(),
            params,
            param_ids: param_ids_in_order,
            body,
        })
    }

    fn parse_defined_or_expr(&mut self) -> Result<Stage, String> {
        let lhs = self.parse_assignment_expr()?;
        if self.peek() == &Token::DefinedOr {
            self.bump();
            let rhs = self.parse_defined_or_expr()?;
            Ok(Stage::Binary {
                op: BinaryOp::DefinedOr,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            })
        } else {
            Ok(lhs)
        }
    }

    fn parse_assignment_expr(&mut self) -> Result<Stage, String> {
        let lhs = self.parse_or_expr()?;
        match self.peek() {
            Token::Assign => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_assign(lhs, rhs))
            }
            Token::SetPipe => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_modify(lhs, rhs))
            }
            Token::SetPlus => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_update_assign(lhs, rhs, BinaryOp::Add))
            }
            Token::SetMinus => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_update_assign(lhs, rhs, BinaryOp::Sub))
            }
            Token::SetMult => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_update_assign(lhs, rhs, BinaryOp::Mul))
            }
            Token::SetDiv => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_update_assign(lhs, rhs, BinaryOp::Div))
            }
            Token::SetMod => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_update_assign(lhs, rhs, BinaryOp::Mod))
            }
            Token::SetDefinedOr => {
                self.bump();
                let rhs = self.parse_or_expr()?;
                Ok(self.expand_defined_or_assign(lhs, rhs))
            }
            _ => Ok(lhs),
        }
    }

    fn parse_or_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_and_expr()?;
        while self.peek() == &Token::Or {
            self.bump();
            let rhs = self.parse_and_expr()?;
            lhs = Stage::Binary {
                op: BinaryOp::Or,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_and_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_equality_expr()?;
        while self.peek() == &Token::And {
            self.bump();
            let rhs = self.parse_equality_expr()?;
            lhs = Stage::Binary {
                op: BinaryOp::And,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_equality_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_comparison_expr()?;
        loop {
            let op = match self.peek() {
                Token::EqEq => BinaryOp::Eq,
                Token::NotEq => BinaryOp::Ne,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_comparison_expr()?;
            if let Some(folded) = fold_large_integer_literal_equality(&lhs, &rhs, op) {
                lhs = folded;
            } else {
                lhs = Stage::Binary {
                    op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                };
            }
        }
        Ok(lhs)
    }

    fn parse_comparison_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_additive_expr()?;
        loop {
            let op = match self.peek() {
                Token::Lt => BinaryOp::Lt,
                Token::Lte => BinaryOp::Le,
                Token::Gt => BinaryOp::Gt,
                Token::Gte => BinaryOp::Ge,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_additive_expr()?;
            lhs = Stage::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_additive_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_multiplicative_expr()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_multiplicative_expr()?;
            lhs = Stage::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Stage, String> {
        let mut lhs = self.parse_unary_expr()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                Token::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_unary_expr()?;
            lhs = Stage::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_unary_expr(&mut self) -> Result<Stage, String> {
        if self.peek() == &Token::Minus {
            self.bump();
            let inner = self.parse_unary_expr()?;
            return Ok(Stage::UnaryMinus(Box::new(inner)));
        }
        let mut stage = self.parse_primary()?;
        stage = self.parse_postfix_suffixes(stage)?;
        while self.peek() == &Token::Question {
            self.bump();
            // jq parser.y:
            // Term '?' => gen_try(term, BACKTRACK)
            // BACKTRACK corresponds to producing no values on failure.
            stage = Stage::TryCatch {
                inner: Box::new(stage),
                catcher: Box::new(Stage::Empty),
            };
        }
        Ok(stage)
    }

    fn parse_postfix_suffixes(&mut self, mut stage: Stage) -> Result<Stage, String> {
        loop {
            match self.peek() {
                Token::Field(name) => {
                    let name = name.clone();
                    self.bump();
                    let optional = self.consume_optional();
                    stage = append_chain_stage(stage, Stage::Field { name, optional });
                }
                Token::Dot => {
                    self.bump();
                    let next = self.parse_postfix_dot_suffix()?;
                    stage = append_chain_stage(stage, next);
                }
                Token::LBracket => {
                    let next = self.parse_bracket_stage()?;
                    stage = append_chain_stage(stage, next);
                }
                _ => break,
            }
        }
        Ok(stage)
    }

    fn parse_primary(&mut self) -> Result<Stage, String> {
        match self.peek() {
            Token::Field(name) => {
                let name = name.clone();
                self.bump();
                Ok(Stage::Field {
                    name,
                    optional: self.consume_optional(),
                })
            }
            Token::Dot => self.parse_dot_stage(),
            Token::Rec => {
                self.bump();
                Ok(Stage::RecurseBy(Box::new(Stage::Iterate {
                    optional: true,
                })))
            }
            Token::BreakKw => self.parse_break_expr(),
            Token::Int(value) => {
                let value = *value;
                self.bump();
                Ok(Stage::Literal(ZqValue::from(value)))
            }
            Token::Num(value) => {
                let value = value.clone();
                self.bump();
                Ok(Stage::Literal(parse_number_literal(&value)?))
            }
            Token::Loc(line) => {
                let line = *line;
                self.bump();
                Ok(loc_stage(line))
            }
            Token::Format(fmt) => {
                let fmt = fmt.clone();
                self.bump();
                if self.peek() == &Token::QQStringStart {
                    self.parse_qq_string(fmt)
                } else {
                    Ok(Stage::Format {
                        fmt,
                        expr: Box::new(Stage::Identity),
                    })
                }
            }
            Token::QQStringStart => self.parse_qq_string("text".to_string()),
            Token::Binding(name) => {
                let name = name.clone();
                self.bump();
                if !self.is_binding_defined(&name) && !self.allow_unresolved_calls {
                    return Err(format!("${name} is not defined"));
                }
                Ok(Stage::Var(name))
            }
            Token::Ident(name) if self.peek_n(1) == &Token::LParen => {
                let call = name.clone();
                self.parse_call(&call)
            }
            Token::Str(value) => {
                let value = value.clone();
                self.bump();
                Ok(Stage::Literal(ZqValue::String(value)))
            }
            Token::Ident(name) if name == "null" => {
                self.bump();
                Ok(Stage::Literal(ZqValue::Null))
            }
            Token::Ident(name) if name == "true" => {
                self.bump();
                Ok(Stage::Literal(ZqValue::Bool(true)))
            }
            Token::Ident(name) if name == "false" => {
                self.bump();
                Ok(Stage::Literal(ZqValue::Bool(false)))
            }
            Token::Ident(name) => {
                if let Some(param_id) = self.resolve_param_call(name, 0) {
                    let name = name.clone();
                    self.bump();
                    Ok(Stage::Call {
                        function_id: None,
                        param_id: Some(param_id),
                        name,
                        args: Vec::new(),
                    })
                } else if let Some(function_id) = self.resolve_user_function(name, 0) {
                    let name = name.clone();
                    self.bump();
                    Ok(Stage::Call {
                        function_id: Some(function_id),
                        param_id: None,
                        name,
                        args: Vec::new(),
                    })
                } else if name == "nan" {
                    self.bump();
                    Ok(Stage::Literal(special_number_literal("nan")))
                } else if name == "infinite" {
                    self.bump();
                    Ok(Stage::Literal(special_number_literal("inf")))
                } else if name == "_negate" {
                    self.bump();
                    Ok(Stage::UnaryMinus(Box::new(Stage::Identity)))
                } else if name == "not" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Not))
                } else if name == "length" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Length))
                } else if name == "fabs" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::FAbs))
                } else if name == "floor" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Floor))
                } else if name == "ceil" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Ceil))
                } else if name == "sqrt" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Sqrt))
                } else if name == "cbrt" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Cbrt))
                } else if name == "round" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Round))
                } else if name == "acos" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Acos))
                } else if name == "acosh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Acosh))
                } else if name == "asin" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Asin))
                } else if name == "asinh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Asinh))
                } else if name == "atan" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Atan))
                } else if name == "atanh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Atanh))
                } else if name == "sin" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Sin))
                } else if name == "sinh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Sinh))
                } else if name == "tan" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Tan))
                } else if name == "tanh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Tanh))
                } else if name == "cos" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Cos))
                } else if name == "cosh" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Cosh))
                } else if name == "exp" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Exp))
                } else if name == "exp2" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Exp2))
                } else if name == "log" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Log))
                } else if name == "log10" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Log10))
                } else if name == "log1p" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Log1p))
                } else if name == "expm1" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Expm1))
                } else if name == "log2" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Log2))
                } else if name == "isinfinite" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::IsInfinite))
                } else if name == "isnan" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::IsNan))
                } else if name == "isnormal" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::IsNormal))
                } else if name == "abs" {
                    self.bump();
                    Ok(abs_stage())
                } else if name == "isfinite" {
                    self.bump();
                    Ok(isfinite_stage())
                } else if name == "finites" {
                    self.bump();
                    Ok(select_stage(isfinite_stage()))
                } else if name == "normals" {
                    self.bump();
                    Ok(select_stage(Stage::Builtin(Builtin::IsNormal)))
                } else if name == "type" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Type))
                } else if name == "add" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Add))
                } else if name == "keys" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Keys))
                } else if name == "keys_unsorted" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::KeysUnsorted))
                } else if name == "to_entries" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToEntries))
                } else if name == "from_entries" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::FromEntries))
                } else if name == "tonumber" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToNumber))
                } else if name == "tostring" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToString))
                } else if name == "toboolean" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToBoolean))
                } else if name == "tojson" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToJson))
                } else if name == "fromjson" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::FromJson))
                } else if name == "utf8bytelength" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Utf8ByteLength))
                } else if name == "explode" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Explode))
                } else if name == "implode" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Implode))
                } else if name == "trim" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Trim))
                } else if name == "ltrim" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::LTrim))
                } else if name == "rtrim" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::RTrim))
                } else if name == "reverse" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Reverse))
                } else if name == "ascii_upcase" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::AsciiUpcase))
                } else if name == "ascii_downcase" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::AsciiDowncase))
                } else if name == "flatten" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Flatten))
                } else if name == "transpose" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Transpose))
                } else if name == "first" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::First))
                } else if name == "last" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Last))
                } else if name == "sort" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Sort))
                } else if name == "unique" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Unique))
                } else if name == "min" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Min))
                } else if name == "max" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Max))
                } else if name == "gmtime" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Gmtime))
                } else if name == "localtime" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Localtime))
                } else if name == "mktime" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Mktime))
                } else if name == "fromdateiso8601" || name == "fromdate" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::FromDateIso8601))
                } else if name == "todateiso8601" || name == "todate" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ToDateIso8601))
                } else if name == "any" {
                    self.bump();
                    Ok(Stage::Any(
                        Box::new(Stage::Iterate { optional: false }),
                        Box::new(Stage::Identity),
                    ))
                } else if name == "all" {
                    self.bump();
                    Ok(Stage::All(
                        Box::new(Stage::Iterate { optional: false }),
                        Box::new(Stage::Identity),
                    ))
                } else if name == "recurse" {
                    self.bump();
                    Ok(Stage::RecurseBy(Box::new(Stage::Iterate {
                        optional: true,
                    })))
                } else if name == "debug" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Debug))
                } else if name == "stderr" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Stderr))
                } else if name == "env" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Env))
                } else if name == "halt" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Halt))
                } else if name == "get_search_list" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::GetSearchList))
                } else if name == "get_prog_origin" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::GetProgOrigin))
                } else if name == "get_jq_origin" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::GetJqOrigin))
                } else if name == "now" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::Now))
                } else if name == "input_filename" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::InputFilename))
                } else if name == "input_line_number" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::InputLineNumber))
                } else if name == "have_decnum" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::HaveDecnum))
                } else if name == "have_literal_numbers" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::HaveLiteralNumbers))
                } else if name == "builtins" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::BuiltinsList))
                } else if name == "input" {
                    self.bump();
                    Ok(Stage::Input)
                } else if name == "modulemeta" {
                    self.bump();
                    Ok(Stage::Builtin(Builtin::ModuleMeta))
                } else if name == "inputs" {
                    self.bump();
                    Ok(self.expand_inputs())
                } else if name == "combinations" {
                    self.bump();
                    Ok(Stage::Combinations)
                } else if name == "arrays" {
                    self.bump();
                    Ok(select_stage(type_eq_stage("array")))
                } else if name == "objects" {
                    self.bump();
                    Ok(select_stage(type_eq_stage("object")))
                } else if name == "iterables" {
                    self.bump();
                    Ok(select_stage(Stage::Binary {
                        op: BinaryOp::Or,
                        lhs: Box::new(type_eq_stage("array")),
                        rhs: Box::new(type_eq_stage("object")),
                    }))
                } else if name == "booleans" {
                    self.bump();
                    Ok(select_stage(type_eq_stage("boolean")))
                } else if name == "numbers" {
                    self.bump();
                    Ok(select_stage(type_eq_stage("number")))
                } else if name == "strings" {
                    self.bump();
                    Ok(select_stage(type_eq_stage("string")))
                } else if name == "nulls" {
                    self.bump();
                    Ok(select_stage(Stage::Binary {
                        op: BinaryOp::Eq,
                        lhs: Box::new(Stage::Identity),
                        rhs: Box::new(Stage::Literal(ZqValue::Null)),
                    }))
                } else if name == "values" {
                    self.bump();
                    Ok(select_stage(Stage::Binary {
                        op: BinaryOp::Ne,
                        lhs: Box::new(Stage::Identity),
                        rhs: Box::new(Stage::Literal(ZqValue::Null)),
                    }))
                } else if name == "scalars" {
                    self.bump();
                    Ok(select_stage(Stage::Binary {
                        op: BinaryOp::And,
                        lhs: Box::new(type_ne_stage("array")),
                        rhs: Box::new(type_ne_stage("object")),
                    }))
                } else if name == "paths" {
                    self.bump();
                    Ok(Stage::Paths)
                } else if name == "tostream" {
                    self.bump();
                    Ok(Stage::ToStream)
                } else if name == "empty" {
                    self.bump();
                    Ok(Stage::Empty)
                } else if name == "halt_error" {
                    self.bump();
                    Ok(Stage::HaltError(Box::new(Stage::Literal(ZqValue::from(5)))))
                } else if name == "error" {
                    self.parse_error_expr()
                } else if self.allow_unresolved_calls {
                    let name = name.clone();
                    self.bump();
                    Ok(Stage::Call {
                        function_id: None,
                        param_id: None,
                        name,
                        args: Vec::new(),
                    })
                } else {
                    Err(format!("{name}/0 is not defined"))
                }
            }
            Token::LParen => {
                self.bump();
                let expr = self.parse_pipe_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Token::ReduceKw => self.parse_reduce_expr(),
            Token::ForeachKw => self.parse_foreach_expr(),
            Token::If => self.parse_if_expr(),
            Token::Try => self.parse_try_expr(),
            Token::LBracket => self.parse_array_literal(),
            Token::LBrace => self.parse_object_literal(),
            Token::End => Err("syntax error, unexpected end of file".to_string()),
            Token::Percent => {
                Err("syntax error, unexpected '%', expecting end of file".to_string())
            }
            Token::RBrace => {
                Err("syntax error, unexpected INVALID_CHARACTER, expecting end of file".to_string())
            }
            other => Err(format!("parse error: unsupported stage start {:?}", other)),
        }
    }

    fn parse_qq_string(&mut self, fmt: String) -> Result<Stage, String> {
        self.expect(Token::QQStringStart)?;
        let mut out = Stage::Literal(ZqValue::String(String::new()));
        loop {
            match self.peek() {
                Token::QQStringText(text) => {
                    let text = text.clone();
                    self.bump();
                    out = Stage::Binary {
                        op: BinaryOp::Add,
                        lhs: Box::new(out),
                        rhs: Box::new(Stage::Literal(ZqValue::String(text))),
                    };
                }
                Token::QQInterpStart => {
                    self.bump();
                    let expr = self.parse_pipe_expr()?;
                    self.expect(Token::QQInterpEnd)?;
                    out = Stage::Binary {
                        op: BinaryOp::Add,
                        lhs: Box::new(out),
                        rhs: Box::new(Stage::Format {
                            fmt: fmt.clone(),
                            expr: Box::new(expr),
                        }),
                    };
                }
                Token::QQStringEnd => {
                    self.bump();
                    return Ok(out);
                }
                other => {
                    return Err(format!(
                        "parse error: unexpected token in interpolated string: {:?}",
                        other
                    ));
                }
            }
        }
    }

    fn parse_error_expr(&mut self) -> Result<Stage, String> {
        // Consume "error"
        self.bump();
        if self.peek() == &Token::LParen {
            self.bump();
            let arg = self.parse_pipe_expr()?;
            self.expect(Token::RParen)?;
            return Ok(Stage::Error(Box::new(arg)));
        }
        Ok(Stage::Error(Box::new(Stage::Identity)))
    }

    fn parse_break_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::BreakKw)?;
        let name = match self.peek() {
            Token::Binding(name) => {
                let name = name.clone();
                self.bump();
                name
            }
            _ => return Err("parse error: break requires a label to break to".to_string()),
        };
        if !self.label_stack.iter().any(|label| label == &name) {
            return Err(format!("$*label-{name} is not defined"));
        }
        Ok(Stage::Break(name))
    }

    fn parse_call(&mut self, name: &str) -> Result<Stage, String> {
        // consume function name
        self.bump();
        self.expect(Token::LParen)?;
        let mut args = vec![self.parse_pipe_expr()?];
        while self.peek() == &Token::Semi {
            self.bump();
            args.push(self.parse_pipe_expr()?);
        }
        self.expect(Token::RParen)?;
        let arity = args.len();
        if let Some(function_id) = self.resolve_user_function(name, arity) {
            if name == "while" && arity == 2 && self.is_canonical_while_def(function_id) {
                let source = args.pop().expect("arity checked by match");
                let cond = args.pop().expect("arity checked by match");
                return Ok(Stage::While(Box::new(cond), Box::new(source)));
            }
            if name == "until" && arity == 2 && self.is_canonical_until_def(function_id) {
                let source = args.pop().expect("arity checked by match");
                let cond = args.pop().expect("arity checked by match");
                return Ok(Stage::Until(Box::new(cond), Box::new(source)));
            }
            return Ok(Stage::Call {
                function_id: Some(function_id),
                param_id: None,
                name: name.to_string(),
                args,
            });
        }
        if name == "while" && arity == 1 {
            return Err("parse error: expected `;` in while/2 call".to_string());
        }
        if name == "until" && arity == 1 {
            return Err("parse error: expected `;` in until/2 call".to_string());
        }
        let mut args = args;
        match (name, arity) {
            ("_plus", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Add,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_minus", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Sub,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_multiply", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Mul,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_divide", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Div,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_mod", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Mod,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_equal", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Eq,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_notequal", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Ne,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_less", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Lt,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_lesseq", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Le,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_greater", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Gt,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_greatereq", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Ge,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("_negate", 0) => Ok(Stage::UnaryMinus(Box::new(Stage::Identity))),
            ("pow", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::Binary {
                    op: BinaryOp::Pow,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("atan2", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Atan2,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("hypot", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Hypot,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("copysign", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::CopySign,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("drem", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Drem,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("fdim", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Fdim,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("fmax", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Fmax,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("fmin", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Fmin,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("fmod", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Fmod,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("jn", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Jn,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("ldexp", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Ldexp,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("nextafter", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::NextAfter,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("nexttoward", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::NextToward,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("remainder", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Remainder,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("scalb", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Scalb,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("scalbln", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Scalbln,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("yn", 2) => {
                let rhs = args.pop().expect("arity checked by match");
                let lhs = args.pop().expect("arity checked by match");
                Ok(Stage::MathBinary {
                    op: MathBinaryOp::Yn,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            ("fma", 3) => {
                let c = args.pop().expect("arity checked by match");
                let b = args.pop().expect("arity checked by match");
                let a = args.pop().expect("arity checked by match");
                Ok(Stage::MathTernary {
                    op: MathTernaryOp::Fma,
                    a: Box::new(a),
                    b: Box::new(b),
                    c: Box::new(c),
                })
            }
            ("error", 1) => Ok(Stage::Error(Box::new(args.remove(0)))),
            ("halt_error", 1) => Ok(Stage::HaltError(Box::new(args.remove(0)))),
            ("del", 1) => Ok(self.expand_del(args.remove(0))),
            ("pick", 1) => Ok(self.expand_pick(args.remove(0))),
            ("has", 1) => Ok(Stage::Has(Box::new(args.remove(0)))),
            ("in", 1) => Ok(Stage::In(Box::new(args.remove(0)))),
            ("IN", 1) => Ok(self.expand_in_unary(args.remove(0))),
            // jq/src/builtin.jq
            // def INDEX(idx_expr): INDEX(.[]; idx_expr);
            ("INDEX", 1) => {
                Ok(self.expand_index(Stage::Iterate { optional: false }, args.remove(0)))
            }
            ("startswith", 1) => Ok(Stage::StartsWith(Box::new(args.remove(0)))),
            ("endswith", 1) => Ok(Stage::EndsWith(Box::new(args.remove(0)))),
            ("split", 1) => Ok(Stage::Split(Box::new(args.remove(0)))),
            ("join", 1) => Ok(Stage::Join(Box::new(args.remove(0)))),
            ("ltrimstr", 1) => Ok(Stage::LTrimStr(Box::new(args.remove(0)))),
            ("rtrimstr", 1) => Ok(Stage::RTrimStr(Box::new(args.remove(0)))),
            ("trimstr", 1) => Ok(Stage::TrimStr(Box::new(args.remove(0)))),
            ("indices", 1) => Ok(Stage::Indices(Box::new(args.remove(0)))),
            ("index", 1) => Ok(Stage::IndexOf(Box::new(args.remove(0)))),
            ("rindex", 1) => Ok(Stage::RIndexOf(Box::new(args.remove(0)))),
            ("contains", 1) => Ok(Stage::Contains(Box::new(args.remove(0)))),
            ("inside", 1) => Ok(Stage::Inside(Box::new(args.remove(0)))),
            ("bsearch", 1) => Ok(Stage::BSearch(Box::new(args.remove(0)))),
            ("debug", 1) => Ok(self.expand_debug(args.remove(0))),
            ("match", 1) => Ok(Stage::RegexMatch {
                spec: Box::new(args.remove(0)),
                flags: None,
                test: false,
                tuple_mode: true,
            }),
            ("test", 1) => Ok(Stage::RegexMatch {
                spec: Box::new(args.remove(0)),
                flags: None,
                test: true,
                tuple_mode: true,
            }),
            ("capture", 1) => Ok(Stage::RegexCapture {
                spec: Box::new(args.remove(0)),
                flags: None,
                tuple_mode: true,
            }),
            ("scan", 1) => Ok(Stage::RegexScan {
                regex: Box::new(args.remove(0)),
                flags: None,
            }),
            // jq/src/builtin.jq
            // def splits($re): splits($re; null);
            ("splits", 1) => Ok(Stage::RegexSplits {
                regex: Box::new(args.remove(0)),
                flags: None,
            }),
            // jq/src/builtin.jq
            // def sub($re; s): sub($re; s; "");
            ("sub", 2) => {
                let replacement = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexSub {
                    regex: Box::new(regex),
                    replacement: Box::new(replacement),
                    flags: Box::new(Stage::Literal(ZqValue::String(String::new()))),
                    global: false,
                })
            }
            // jq/src/builtin.jq
            // def gsub($re; s): sub($re; s; "g");
            ("gsub", 2) => {
                let replacement = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexSub {
                    regex: Box::new(regex),
                    replacement: Box::new(replacement),
                    flags: Box::new(Stage::Literal(ZqValue::String(String::new()))),
                    global: true,
                })
            }
            // jq/src/builtin.jq
            // def sort_by(f): _sort_by_impl(map([f]));
            ("sort_by", 1) => Ok(Stage::SortByImpl(Box::new(by_impl_keys_stage(
                args.remove(0),
            )))),
            // jq/src/builtin.jq
            // def group_by(f): _group_by_impl(map([f]));
            ("group_by", 1) => Ok(Stage::GroupByImpl(Box::new(by_impl_keys_stage(
                args.remove(0),
            )))),
            // jq/src/builtin.jq
            // def unique_by(f): _unique_by_impl(map([f]));
            ("unique_by", 1) => Ok(Stage::UniqueByImpl(Box::new(by_impl_keys_stage(
                args.remove(0),
            )))),
            // jq/src/builtin.jq
            // def min_by(f): _min_by_impl(map([f]));
            ("min_by", 1) => Ok(Stage::MinByImpl(Box::new(by_impl_keys_stage(
                args.remove(0),
            )))),
            // jq/src/builtin.jq
            // def max_by(f): _max_by_impl(map([f]));
            ("max_by", 1) => Ok(Stage::MaxByImpl(Box::new(by_impl_keys_stage(
                args.remove(0),
            )))),
            // jq internal builtins exposed from C runtime.
            ("_sort_by_impl", 1) => Ok(Stage::SortByImpl(Box::new(args.remove(0)))),
            ("_group_by_impl", 1) => Ok(Stage::GroupByImpl(Box::new(args.remove(0)))),
            ("_unique_by_impl", 1) => Ok(Stage::UniqueByImpl(Box::new(args.remove(0)))),
            ("_min_by_impl", 1) => Ok(Stage::MinByImpl(Box::new(args.remove(0)))),
            ("_max_by_impl", 1) => Ok(Stage::MaxByImpl(Box::new(args.remove(0)))),
            ("path", 1) => Ok(Stage::Path(Box::new(args.remove(0)))),
            ("paths", 1) => Ok(self.expand_paths_with_filter(args.remove(0))),
            ("getpath", 1) => Ok(Stage::GetPath(Box::new(args.remove(0)))),
            ("delpaths", 1) => Ok(Stage::DelPaths(Box::new(args.remove(0)))),
            ("truncate_stream", 1) => Ok(Stage::TruncateStream(Box::new(args.remove(0)))),
            ("fromstream", 1) => Ok(Stage::FromStream(Box::new(args.remove(0)))),
            // jq/src/builtin.c
            // CFUNC(f_strptime, "strptime", 2)
            ("strptime", 1) => Ok(Stage::Strptime(Box::new(args.remove(0)))),
            // jq/src/builtin.c
            // CFUNC(f_strftime, "strftime", 2)
            ("strftime", 1) => Ok(Stage::Strftime {
                format: Box::new(args.remove(0)),
                local: false,
            }),
            // jq/src/builtin.c
            // CFUNC(f_strflocaltime, "strflocaltime", 2)
            ("strflocaltime", 1) => Ok(Stage::Strftime {
                format: Box::new(args.remove(0)),
                local: true,
            }),
            ("flatten", 1) => Ok(Stage::Flatten(Box::new(args.remove(0)))),
            // jq/src/builtin.jq
            // def _flatten($x): ...
            ("_flatten", 1) => Ok(Stage::FlattenRaw(Box::new(args.remove(0)))),
            ("nth", 1) => Ok(Stage::Nth(Box::new(args.remove(0)))),
            ("first", 1) => Ok(Stage::FirstBy(Box::new(args.remove(0)))),
            ("last", 1) => Ok(Stage::LastBy(Box::new(args.remove(0)))),
            ("isempty", 1) => Ok(Stage::IsEmpty(Box::new(args.remove(0)))),
            ("add", 1) => Ok(Stage::AddBy(Box::new(args.remove(0)))),
            ("select", 1) => Ok(Stage::Select(Box::new(args.remove(0)))),
            ("map", 1) => Ok(Stage::Map(Box::new(args.remove(0)))),
            ("map_values", 1) => Ok(Stage::MapValues(Box::new(args.remove(0)))),
            ("with_entries", 1) => Ok(Stage::WithEntries(Box::new(args.remove(0)))),
            ("recurse", 1) => Ok(Stage::RecurseBy(Box::new(args.remove(0)))),
            ("walk", 1) => Ok(Stage::Walk(Box::new(args.remove(0)))),
            ("any", 1) => Ok(Stage::Any(
                Box::new(Stage::Iterate { optional: false }),
                Box::new(args.remove(0)),
            )),
            ("all", 1) => Ok(Stage::All(
                Box::new(Stage::Iterate { optional: false }),
                Box::new(args.remove(0)),
            )),
            ("range", 1) => Ok(Stage::Range(
                Box::new(Stage::Literal(ZqValue::from(0))),
                Box::new(args.remove(0)),
                Box::new(Stage::Literal(ZqValue::from(1))),
            )),
            // jq/src/builtin.jq
            // def repeat(exp):
            //   def _repeat: exp, _repeat;
            //   _repeat;
            ("repeat", 1) => Ok(Stage::Repeat(Box::new(args.remove(0)))),
            // jq/src/builtin.jq
            // def combinations(n):
            //   . as $dot | [range(n) | $dot] | combinations;
            ("combinations", 1) => Ok(self.expand_combinations_n(args.remove(0))),
            // jq/src/builtin.jq
            // def INDEX(stream; idx_expr):
            //   reduce stream as $row ({}; .[$row|idx_expr|tostring] = $row);
            ("INDEX", 2) => {
                let idx_expr = args.pop().expect("arity checked by match");
                let stream = args.pop().expect("arity checked by match");
                Ok(self.expand_index(stream, idx_expr))
            }
            // jq/src/builtin.jq
            // def JOIN($idx; idx_expr):
            //   [.[] | [., $idx[idx_expr]]];
            ("JOIN", 2) => {
                let idx_expr = args.pop().expect("arity checked by match");
                let idx_value = args.pop().expect("arity checked by match");
                Ok(Stage::ArrayLiteral(vec![self.expand_join(
                    idx_value,
                    Stage::Iterate { optional: false },
                    idx_expr,
                    None,
                )]))
            }
            ("nth", 2) => {
                let source = args.pop().expect("arity checked by match");
                let index = args.pop().expect("arity checked by match");
                Ok(Stage::NthBy(Box::new(index), Box::new(source)))
            }
            ("limit", 2) => {
                let source = args.pop().expect("arity checked by match");
                let count = args.pop().expect("arity checked by match");
                Ok(Stage::LimitBy(Box::new(count), Box::new(source)))
            }
            ("skip", 2) => {
                let source = args.pop().expect("arity checked by match");
                let count = args.pop().expect("arity checked by match");
                Ok(Stage::SkipBy(Box::new(count), Box::new(source)))
            }
            ("while", 2) => {
                let source = args.pop().expect("arity checked by match");
                let cond = args.pop().expect("arity checked by match");
                Ok(Stage::While(Box::new(cond), Box::new(source)))
            }
            ("until", 2) => {
                let source = args.pop().expect("arity checked by match");
                let cond = args.pop().expect("arity checked by match");
                Ok(Stage::Until(Box::new(cond), Box::new(source)))
            }
            ("any", 2) => {
                let condition = args.pop().expect("arity checked by match");
                let generator = args.pop().expect("arity checked by match");
                Ok(Stage::Any(Box::new(generator), Box::new(condition)))
            }
            ("all", 2) => {
                let condition = args.pop().expect("arity checked by match");
                let generator = args.pop().expect("arity checked by match");
                Ok(Stage::All(Box::new(generator), Box::new(condition)))
            }
            ("match", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let spec = args.pop().expect("arity checked by match");
                Ok(Stage::RegexMatch {
                    spec: Box::new(spec),
                    flags: Some(Box::new(flags)),
                    test: false,
                    tuple_mode: false,
                })
            }
            ("test", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let spec = args.pop().expect("arity checked by match");
                Ok(Stage::RegexMatch {
                    spec: Box::new(spec),
                    flags: Some(Box::new(flags)),
                    test: true,
                    tuple_mode: false,
                })
            }
            ("capture", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let spec = args.pop().expect("arity checked by match");
                Ok(Stage::RegexCapture {
                    spec: Box::new(spec),
                    flags: Some(Box::new(flags)),
                    tuple_mode: false,
                })
            }
            ("scan", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexScan {
                    regex: Box::new(regex),
                    flags: Some(Box::new(flags)),
                })
            }
            // jq/src/builtin.jq
            // def splits($re; $flags):
            //   .[foreach (match($re; $flags+"g"), null) as {$offset, $length}
            //       (null; {start: .next, end: $offset, next: ($offset+$length)})];
            ("splits", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexSplits {
                    regex: Box::new(regex),
                    flags: Some(Box::new(flags)),
                })
            }
            // jq/src/builtin.jq
            // def split($re; $flags): [ splits($re; $flags) ];
            ("split", 2) => {
                let flags = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::ArrayLiteral(vec![Stage::RegexSplits {
                    regex: Box::new(regex),
                    flags: Some(Box::new(flags)),
                }]))
            }
            // jq/src/builtin.jq
            // def sub($re; s; $flags): ...
            ("sub", 3) => {
                let flags = args.pop().expect("arity checked by match");
                let replacement = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexSub {
                    regex: Box::new(regex),
                    replacement: Box::new(replacement),
                    flags: Box::new(flags),
                    global: false,
                })
            }
            // jq/src/builtin.jq
            // def gsub($re; s; flags): sub($re; s; flags + "g");
            ("gsub", 3) => {
                let flags = args.pop().expect("arity checked by match");
                let replacement = args.pop().expect("arity checked by match");
                let regex = args.pop().expect("arity checked by match");
                Ok(Stage::RegexSub {
                    regex: Box::new(regex),
                    replacement: Box::new(replacement),
                    flags: Box::new(flags),
                    global: true,
                })
            }
            ("recurse", 2) => {
                let cond = args.pop().expect("arity checked by match");
                let step = args.pop().expect("arity checked by match");
                Ok(Stage::RecurseByCond(Box::new(step), Box::new(cond)))
            }
            // jq/src/builtin.jq
            // def JOIN($idx; stream; idx_expr):
            //   stream | [., $idx[idx_expr]];
            ("JOIN", 3) => {
                let idx_expr = args.pop().expect("arity checked by match");
                let stream = args.pop().expect("arity checked by match");
                let idx_value = args.pop().expect("arity checked by match");
                Ok(self.expand_join(idx_value, stream, idx_expr, None))
            }
            // jq/src/builtin.jq
            // def IN(src; s): any(src == s; .);
            ("IN", 2) => {
                let s = args.pop().expect("arity checked by match");
                let src = args.pop().expect("arity checked by match");
                Ok(self.expand_in_binary(src, s))
            }
            // jq/src/builtin.jq
            // def _assign(paths; $value): reduce path(paths) as $p (.; setpath($p; $value));
            ("_assign", 2) => {
                let value = args.pop().expect("arity checked by match");
                let paths = args.pop().expect("arity checked by match");
                Ok(self.expand_assign(paths, value))
            }
            // jq/src/builtin.jq
            // def _modify(paths; update): ...
            ("_modify", 2) => {
                let update = args.pop().expect("arity checked by match");
                let paths = args.pop().expect("arity checked by match");
                Ok(self.expand_modify(paths, update))
            }
            ("setpath", 2) => {
                let value = args.pop().expect("arity checked by match");
                let path = args.pop().expect("arity checked by match");
                Ok(Stage::SetPath(Box::new(path), Box::new(value)))
            }
            ("range", 2) => {
                let upto = args.pop().expect("arity checked by match");
                let init = args.pop().expect("arity checked by match");
                Ok(Stage::Range(
                    Box::new(init),
                    Box::new(upto),
                    Box::new(Stage::Literal(ZqValue::from(1))),
                ))
            }
            ("range", 3) => {
                let by = args.pop().expect("arity checked by match");
                let upto = args.pop().expect("arity checked by match");
                let init = args.pop().expect("arity checked by match");
                Ok(Stage::Range(Box::new(init), Box::new(upto), Box::new(by)))
            }
            // jq/src/builtin.jq
            // def JOIN($idx; stream; idx_expr; join_expr):
            //   stream | [., $idx[idx_expr]] | join_expr;
            ("JOIN", 4) => {
                let join_expr = args.pop().expect("arity checked by match");
                let idx_expr = args.pop().expect("arity checked by match");
                let stream = args.pop().expect("arity checked by match");
                let idx_value = args.pop().expect("arity checked by match");
                Ok(self.expand_join(idx_value, stream, idx_expr, Some(join_expr)))
            }
            _ => {
                if self.allow_unresolved_calls {
                    Ok(Stage::Call {
                        function_id: None,
                        param_id: None,
                        name: name.to_string(),
                        args,
                    })
                } else {
                    Err(format!("{name}/{arity} is not defined"))
                }
            }
        }
    }

    // jq/src/builtin.jq
    // def del(f): delpaths([path(f)]);
    fn expand_del(&self, path_filter: Stage) -> Stage {
        Stage::DelPaths(Box::new(Stage::ArrayLiteral(vec![Stage::Path(Box::new(
            path_filter,
        ))])))
    }

    // jq/src/builtin.jq
    // def pick(pathexps):
    //   . as $in
    //   | reduce path(pathexps) as $a (null;
    //       setpath($a; $in|getpath($a)) );
    fn expand_pick(&self, path_expressions: Stage) -> Stage {
        Stage::Bind {
            source: Box::new(Stage::Identity),
            pattern: BindingPattern::Var("in".to_string()),
            body: Box::new(Stage::Reduce {
                source: Box::new(Stage::Path(Box::new(path_expressions))),
                pattern: BindingPattern::Var("a".to_string()),
                init: Box::new(Stage::Literal(ZqValue::Null)),
                update: Box::new(Stage::SetPath(
                    Box::new(Stage::Var("a".to_string())),
                    Box::new(Stage::Pipe(vec![
                        Stage::Var("in".to_string()),
                        Stage::GetPath(Box::new(Stage::Var("a".to_string()))),
                    ])),
                )),
            }),
        }
    }

    // jq/src/builtin.jq
    // def paths(node_filter): path(recurse|select(node_filter))|select(length > 0);
    fn expand_paths_with_filter(&self, node_filter: Stage) -> Stage {
        let recurse_then_filter = Stage::Pipe(vec![
            Stage::RecurseBy(Box::new(Stage::Iterate { optional: true })),
            Stage::Select(Box::new(node_filter)),
        ]);
        let non_empty = Stage::Binary {
            op: BinaryOp::Gt,
            lhs: Box::new(Stage::Builtin(Builtin::Length)),
            rhs: Box::new(Stage::Literal(ZqValue::from(0))),
        };
        Stage::Pipe(vec![
            Stage::Path(Box::new(recurse_then_filter)),
            Stage::Select(Box::new(non_empty)),
        ])
    }

    // jq/src/builtin.jq
    // def _assign(paths; $value): reduce path(paths) as $p (.; setpath($p; $value));
    fn expand_assign(&mut self, path_filter: Stage, value_filter: Stage) -> Stage {
        let value_var = self.fresh_internal_name("assign-value");
        let path_var = self.fresh_internal_name("assign-path");
        Stage::Bind {
            source: Box::new(value_filter),
            pattern: BindingPattern::Var(value_var.clone()),
            body: Box::new(Stage::Reduce {
                source: Box::new(Stage::Path(Box::new(path_filter))),
                pattern: BindingPattern::Var(path_var.clone()),
                init: Box::new(Stage::Identity),
                update: Box::new(Stage::SetPath(
                    Box::new(Stage::Var(path_var)),
                    Box::new(Stage::Var(value_var)),
                )),
            }),
        }
    }

    // jq/src/builtin.jq
    // def _modify(paths; update): ...
    fn expand_modify(&mut self, path_filter: Stage, update_filter: Stage) -> Stage {
        Stage::Modify(Box::new(path_filter), Box::new(update_filter))
    }

    // jq/src/parser.y gen_update():
    // a op= b == _modify(a; . op tmp(rhs(input)))
    fn expand_update_assign(
        &mut self,
        path_filter: Stage,
        rhs_filter: Stage,
        op: BinaryOp,
    ) -> Stage {
        let root_var = self.fresh_internal_name("update-root");
        let rhs_from_root = Stage::Pipe(vec![Stage::Var(root_var.clone()), rhs_filter]);
        let update = Stage::Binary {
            op,
            lhs: Box::new(Stage::Identity),
            rhs: Box::new(rhs_from_root),
        };
        Stage::Bind {
            source: Box::new(Stage::Identity),
            pattern: BindingPattern::Var(root_var),
            body: Box::new(self.expand_modify(path_filter, update)),
        }
    }

    // jq/src/parser.y gen_definedor_assign():
    // a //= b == _modify(a; . // tmp(rhs(input)))
    fn expand_defined_or_assign(&mut self, path_filter: Stage, rhs_filter: Stage) -> Stage {
        let root_var = self.fresh_internal_name("definedor-root");
        let rhs_from_root = Stage::Pipe(vec![Stage::Var(root_var.clone()), rhs_filter]);
        let update = Stage::Binary {
            op: BinaryOp::DefinedOr,
            lhs: Box::new(Stage::Identity),
            rhs: Box::new(rhs_from_root),
        };
        Stage::Bind {
            source: Box::new(Stage::Identity),
            pattern: BindingPattern::Var(root_var),
            body: Box::new(self.expand_modify(path_filter, update)),
        }
    }

    // jq/src/builtin.jq
    // def IN(s): any(s == .; .);
    fn expand_in_unary(&self, set_filter: Stage) -> Stage {
        let generator = Stage::Binary {
            op: BinaryOp::Eq,
            lhs: Box::new(set_filter),
            rhs: Box::new(Stage::Identity),
        };
        Stage::Any(Box::new(generator), Box::new(Stage::Identity))
    }

    // jq/src/builtin.jq
    // def IN(src; s): any(src == s; .);
    fn expand_in_binary(&self, src_filter: Stage, set_filter: Stage) -> Stage {
        let generator = Stage::Binary {
            op: BinaryOp::Eq,
            lhs: Box::new(src_filter),
            rhs: Box::new(set_filter),
        };
        Stage::Any(Box::new(generator), Box::new(Stage::Identity))
    }

    // jq/src/builtin.jq
    // def INDEX(stream; idx_expr):
    //   reduce stream as $row ({}; .[$row|idx_expr|tostring] = $row);
    fn expand_index(&mut self, stream: Stage, idx_expr: Stage) -> Stage {
        let row_var = self.fresh_internal_name("index-row");
        let key_expr = Stage::Pipe(vec![
            Stage::Var(row_var.clone()),
            idx_expr,
            Stage::Builtin(Builtin::ToString),
        ]);
        let path_filter = Stage::Chain(vec![
            Stage::Identity,
            Stage::DynamicIndex {
                key: Box::new(key_expr),
                optional: false,
            },
        ]);
        Stage::Reduce {
            source: Box::new(stream),
            pattern: BindingPattern::Var(row_var.clone()),
            init: Box::new(Stage::ObjectLiteral(Vec::new())),
            update: Box::new(self.expand_assign(path_filter, Stage::Var(row_var))),
        }
    }

    // jq/src/builtin.jq
    // def JOIN($idx; stream; idx_expr; join_expr):
    //   stream | [., $idx[idx_expr]] | join_expr;
    fn expand_join(
        &mut self,
        idx_value: Stage,
        stream: Stage,
        idx_expr: Stage,
        join_expr: Option<Stage>,
    ) -> Stage {
        let idx_var = self.fresh_internal_name("join-idx");
        let pair = Stage::ArrayLiteral(vec![
            Stage::Identity,
            Stage::Chain(vec![
                Stage::Var(idx_var.clone()),
                Stage::DynamicIndex {
                    key: Box::new(idx_expr),
                    optional: false,
                },
            ]),
        ]);
        let mut stages = vec![stream, pair];
        if let Some(join_expr) = join_expr {
            stages.push(join_expr);
        }
        Stage::Bind {
            source: Box::new(idx_value),
            pattern: BindingPattern::Var(idx_var),
            body: Box::new(Stage::Pipe(stages)),
        }
    }

    // jq/src/builtin.jq
    // def debug(msgs): (msgs | debug | empty), .;
    //
    // We intentionally model only jq output semantics here; stderr side-effects
    // from the internal `debug` builtin are out-of-scope for vm_core execution.
    fn expand_debug(&self, messages: Stage) -> Stage {
        Stage::Comma(vec![
            Stage::Pipe(vec![messages, Stage::Empty]),
            Stage::Identity,
        ])
    }

    // jq/src/builtin.jq
    // def inputs: try repeat(input) catch if .=="break" then empty else error end;
    fn expand_inputs(&self) -> Stage {
        let is_break = Stage::Binary {
            op: BinaryOp::Eq,
            lhs: Box::new(Stage::Identity),
            rhs: Box::new(Stage::Literal(ZqValue::String("break".to_string()))),
        };
        Stage::TryCatch {
            inner: Box::new(Stage::Repeat(Box::new(Stage::Input))),
            catcher: Box::new(Stage::IfElse {
                cond: Box::new(is_break),
                then_expr: Box::new(Stage::Empty),
                else_expr: Box::new(Stage::Error(Box::new(Stage::Identity))),
            }),
        }
    }

    // jq/src/builtin.jq
    // def combinations(n):
    //   . as $dot
    //   | [range(n) | $dot]
    //   | combinations;
    fn expand_combinations_n(&mut self, n_filter: Stage) -> Stage {
        let dot_var = self.fresh_internal_name("combinations-dot");
        Stage::Bind {
            source: Box::new(Stage::Identity),
            pattern: BindingPattern::Var(dot_var.clone()),
            body: Box::new(Stage::Pipe(vec![
                Stage::ArrayLiteral(vec![Stage::Pipe(vec![
                    Stage::Range(
                        Box::new(Stage::Literal(ZqValue::from(0))),
                        Box::new(n_filter),
                        Box::new(Stage::Literal(ZqValue::from(1))),
                    ),
                    Stage::Var(dot_var),
                ])]),
                Stage::Combinations,
            ])),
        }
    }

    fn fresh_internal_name(&mut self, stem: &str) -> String {
        let id = self.next_param_id;
        self.next_param_id += 1;
        format!("*{stem}-{id}")
    }

    fn parse_pipe_expr(&mut self) -> Result<Stage, String> {
        let mut stages = vec![self.parse_comma_expr()?];
        while self.peek() == &Token::Pipe {
            self.bump();
            stages.push(self.parse_comma_expr()?);
        }
        if stages.len() == 1 {
            Ok(stages.remove(0))
        } else {
            Ok(Stage::Pipe(stages))
        }
    }

    fn parse_pipe_no_comma(&mut self) -> Result<Stage, String> {
        let mut stages = vec![self.parse_as_expr()?];
        while self.peek() == &Token::Pipe {
            self.bump();
            stages.push(self.parse_as_expr()?);
        }
        if stages.len() == 1 {
            Ok(stages.remove(0))
        } else {
            Ok(Stage::Pipe(stages))
        }
    }

    fn parse_comma_expr(&mut self) -> Result<Stage, String> {
        let mut args = vec![self.parse_as_expr()?];
        while self.peek() == &Token::Comma {
            self.bump();
            args.push(self.parse_as_expr()?);
        }
        if args.len() == 1 {
            Ok(args.remove(0))
        } else {
            Ok(Stage::Comma(args))
        }
    }

    fn parse_as_expr(&mut self) -> Result<Stage, String> {
        if self.peek() == &Token::DefKw {
            return self.parse_query_with_defs();
        }
        if self.peek() == &Token::LabelKw {
            return self.parse_label_expr();
        }
        let source = self.parse_defined_or_expr()?;
        if self.peek() != &Token::AsKw {
            return Ok(source);
        }
        self.bump();
        let pattern = self.parse_patterns()?;
        self.expect(Token::Pipe)?;
        let mut bindings = Vec::new();
        collect_pattern_bindings(&pattern, &mut bindings);
        self.binding_scopes.push(bindings);
        let body = self.parse_pipe_expr();
        let _ = self.binding_scopes.pop();
        let body = body?;
        Ok(Stage::Bind {
            source: Box::new(source),
            pattern,
            body: Box::new(body),
        })
    }

    fn parse_label_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::LabelKw)?;
        let name = match self.peek() {
            Token::Binding(name) => {
                let name = name.clone();
                self.bump();
                name
            }
            other => {
                return Err(format!(
                    "parse error: expected label variable after `label`, found {:?}",
                    other
                ))
            }
        };
        self.expect(Token::Pipe)?;
        self.label_stack.push(name.clone());
        let body = self.parse_pipe_expr();
        let _ = self.label_stack.pop();
        let body = body?;
        Ok(Stage::Label {
            name,
            body: Box::new(body),
        })
    }

    fn parse_patterns(&mut self) -> Result<BindingPattern, String> {
        let mut patterns = vec![self.parse_pattern()?];
        while self.peek() == &Token::Question && self.peek_n(1) == &Token::DefinedOr {
            self.bump();
            self.bump();
            patterns.push(self.parse_pattern()?);
        }
        if patterns.len() == 1 {
            Ok(patterns.remove(0))
        } else {
            Ok(BindingPattern::Alternatives(patterns))
        }
    }

    fn parse_pattern(&mut self) -> Result<BindingPattern, String> {
        match self.peek() {
            Token::Binding(name) => {
                let name = name.clone();
                self.bump();
                Ok(BindingPattern::Var(name))
            }
            Token::LBracket => {
                self.bump();
                if self.peek() == &Token::RBracket {
                    return Err("syntax error, unexpected ']'".to_string());
                }
                let mut items = Vec::new();
                loop {
                    items.push(self.parse_pattern()?);
                    if self.peek() == &Token::Comma {
                        self.bump();
                        continue;
                    }
                    break;
                }
                self.expect(Token::RBracket)?;
                Ok(BindingPattern::Array(items))
            }
            Token::LBrace => {
                self.bump();
                if self.peek() == &Token::RBrace {
                    return Err("syntax error, unexpected '}'".to_string());
                }
                let mut entries = Vec::new();
                loop {
                    entries.push(self.parse_object_pattern_entry()?);
                    if self.peek() == &Token::Comma {
                        self.bump();
                        continue;
                    }
                    break;
                }
                self.expect(Token::RBrace)?;
                Ok(BindingPattern::Object(entries))
            }
            other => Err(format!(
                "parse error: expected variable, array, or object pattern, found {:?}",
                other
            )),
        }
    }

    fn parse_object_pattern_entry(&mut self) -> Result<ObjectBindingEntry, String> {
        if let Token::Binding(name) = self.peek() {
            let name = name.clone();
            self.bump();
            if self.peek() == &Token::Colon {
                self.bump();
                let pattern = self.parse_pattern()?;
                return Ok(ObjectBindingEntry {
                    key: BindingKeySpec::Literal(name.clone()),
                    store_var: Some(name),
                    pattern,
                });
            }
            return Ok(ObjectBindingEntry {
                key: BindingKeySpec::Literal(name.clone()),
                store_var: None,
                pattern: BindingPattern::Var(name),
            });
        }

        if let Some(key_expr) = self.parse_string_expr_for_object_key()? {
            let key = match key_expr {
                Stage::Literal(ZqValue::String(name)) => BindingKeySpec::Literal(name),
                other => BindingKeySpec::Expr(Box::new(other)),
            };
            self.expect(Token::Colon)?;
            let pattern = self.parse_pattern()?;
            return Ok(ObjectBindingEntry {
                key,
                store_var: None,
                pattern,
            });
        }

        let key = if self.peek() == &Token::LParen {
            self.bump();
            let key_expr = self.parse_pipe_expr()?;
            self.expect(Token::RParen)?;
            if let Stage::Literal(value) = &key_expr {
                if let Some(message) = const_object_key_error(value) {
                    return Err(message);
                }
            }
            BindingKeySpec::Expr(Box::new(key_expr))
        } else if let Some(name) = self.parse_object_key_name() {
            BindingKeySpec::Literal(name)
        } else {
            if self.peek() == &Token::End {
                return Err("syntax error, unexpected end of file".to_string());
            }
            if self.has_unparenthesized_object_key_expr() {
                return Err("May need parentheses around object key expression".to_string());
            }
            return Err(format!(
                "parse error: unsupported object pattern key token {:?}",
                self.peek()
            ));
        };

        self.expect(Token::Colon)?;
        let pattern = self.parse_pattern()?;
        Ok(ObjectBindingEntry {
            key,
            store_var: None,
            pattern,
        })
    }

    fn parse_if_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::If)?;
        let cond = self.parse_pipe_expr()?;
        self.expect(Token::Then)?;
        let then_expr = self.parse_pipe_expr()?;
        let else_expr = self.parse_else_body()?;
        Ok(Stage::IfElse {
            cond: Box::new(cond),
            then_expr: Box::new(then_expr),
            else_expr: Box::new(else_expr),
        })
    }

    fn parse_else_body(&mut self) -> Result<Stage, String> {
        match self.peek() {
            Token::Elif => {
                self.bump();
                let cond = self.parse_pipe_expr()?;
                self.expect(Token::Then)?;
                let then_expr = self.parse_pipe_expr()?;
                let else_expr = self.parse_else_body()?;
                Ok(Stage::IfElse {
                    cond: Box::new(cond),
                    then_expr: Box::new(then_expr),
                    else_expr: Box::new(else_expr),
                })
            }
            Token::Else => {
                self.bump();
                let else_expr = self.parse_pipe_expr()?;
                self.expect(Token::EndKw)?;
                Ok(else_expr)
            }
            Token::EndKw => {
                self.bump();
                // jq parser.y ElseBody:
                // "end" { $$ = gen_noop(); }
                Ok(Stage::Identity)
            }
            other => Err(format!(
                "parse error: expected elif, else, or end after if-then, found {:?}",
                other
            )),
        }
    }

    fn parse_try_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::Try)?;
        // jq parser precedence makes `try/catch` bind tighter than binary operators.
        // So unparenthesized inner/catcher parts are parsed as terms/unary expressions.
        let inner = self.parse_unary_expr()?;
        let catcher = if self.peek() == &Token::Catch {
            self.bump();
            self.parse_unary_expr()?
        } else {
            Stage::Empty
        };
        Ok(Stage::TryCatch {
            inner: Box::new(inner),
            catcher: Box::new(catcher),
        })
    }

    fn parse_reduce_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::ReduceKw)?;
        let source = self.parse_defined_or_expr()?;
        self.expect(Token::AsKw)?;
        let pattern = self.parse_patterns()?;
        self.expect(Token::LParen)?;
        let init = self.parse_pipe_expr()?;
        self.expect(Token::Semi)?;
        let mut bindings = Vec::new();
        collect_pattern_bindings(&pattern, &mut bindings);
        self.binding_scopes.push(bindings);
        let update = self.parse_pipe_expr();
        let _ = self.binding_scopes.pop();
        let update = update?;
        self.expect(Token::RParen)?;
        Ok(Stage::Reduce {
            source: Box::new(source),
            pattern,
            init: Box::new(init),
            update: Box::new(update),
        })
    }

    fn parse_foreach_expr(&mut self) -> Result<Stage, String> {
        self.expect(Token::ForeachKw)?;
        let source = self.parse_defined_or_expr()?;
        self.expect(Token::AsKw)?;
        let pattern = self.parse_patterns()?;
        self.expect(Token::LParen)?;
        let init = self.parse_pipe_expr()?;
        self.expect(Token::Semi)?;
        let mut bindings = Vec::new();
        collect_pattern_bindings(&pattern, &mut bindings);
        self.binding_scopes.push(bindings);
        let update = self.parse_pipe_expr();
        let extract = if self.peek() == &Token::Semi {
            self.bump();
            self.parse_pipe_expr()
        } else {
            Ok(Stage::Identity)
        };
        let _ = self.binding_scopes.pop();
        let update = update?;
        let extract = extract?;
        self.expect(Token::RParen)?;
        Ok(Stage::Foreach {
            source: Box::new(source),
            pattern,
            init: Box::new(init),
            update: Box::new(update),
            extract: Box::new(extract),
        })
    }

    fn parse_array_literal(&mut self) -> Result<Stage, String> {
        self.expect(Token::LBracket)?;
        if self.peek() == &Token::RBracket {
            self.bump();
            return Ok(Stage::ArrayLiteral(Vec::new()));
        }
        let body = self.parse_pipe_expr()?;
        self.expect(Token::RBracket)?;
        Ok(Stage::ArrayLiteral(vec![body]))
    }

    fn parse_object_literal(&mut self) -> Result<Stage, String> {
        self.expect(Token::LBrace)?;
        let mut entries = Vec::new();
        if self.peek() == &Token::RBrace {
            self.bump();
            return Ok(Stage::ObjectLiteral(entries));
        }
        loop {
            if self.peek() == &Token::End {
                return Err("syntax error, unexpected end of file".to_string());
            }
            let (key, shorthand_value, allow_shorthand) = self.parse_object_key()?;
            let value = if self.peek() == &Token::Colon {
                self.bump();
                self.parse_pipe_no_comma()?
            } else if allow_shorthand {
                shorthand_value
            } else {
                return Err(
                    "parse error: expected `:` after parenthesized object key expression"
                        .to_string(),
                );
            };
            entries.push((key, value));
            if self.peek() == &Token::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(Token::RBrace)?;
        Ok(Stage::ObjectLiteral(entries))
    }

    fn parse_object_key(&mut self) -> Result<(ObjectKey, Stage, bool), String> {
        if let Token::Loc(line) = self.peek() {
            let line = *line;
            self.bump();
            return Ok((
                ObjectKey::Static("__loc__".to_string()),
                loc_stage(line),
                true,
            ));
        }
        if let Token::Binding(name) = self.peek() {
            let name = name.clone();
            self.bump();
            if self.peek() == &Token::Colon {
                return Ok((
                    ObjectKey::Expr(Box::new(Stage::Var(name))),
                    Stage::Identity,
                    false,
                ));
            }
            return Ok((ObjectKey::Static(name.clone()), Stage::Var(name), true));
        }
        if let Some(key_expr) = self.parse_string_expr_for_object_key()? {
            if let Stage::Literal(ZqValue::String(name)) = key_expr {
                return Ok((
                    ObjectKey::Static(name.clone()),
                    Stage::Field {
                        name,
                        optional: false,
                    },
                    true,
                ));
            }
            return Ok((
                ObjectKey::Expr(Box::new(key_expr.clone())),
                Stage::DynamicIndex {
                    key: Box::new(key_expr),
                    optional: false,
                },
                true,
            ));
        }
        if self.peek() == &Token::LParen {
            self.bump();
            let key_expr = self.parse_pipe_expr()?;
            self.expect(Token::RParen)?;
            if let Stage::Literal(value) = &key_expr {
                if let Some(message) = const_object_key_error(value) {
                    return Err(message);
                }
            }
            return Ok((ObjectKey::Expr(Box::new(key_expr)), Stage::Identity, false));
        }
        if let Some(name) = self.parse_object_key_name() {
            return Ok((
                ObjectKey::Static(name.clone()),
                Stage::Field {
                    name,
                    optional: false,
                },
                true,
            ));
        }
        if self.peek() == &Token::End {
            return Err("syntax error, unexpected end of file".to_string());
        }
        if self.has_unparenthesized_object_key_expr() {
            return Err("May need parentheses around object key expression".to_string());
        }
        Err(format!(
            "parse error: unsupported object key token {:?}",
            self.peek()
        ))
    }

    fn has_unparenthesized_object_key_expr(&self) -> bool {
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut brace_depth = 0usize;
        let mut idx = self.pos;
        loop {
            let token = self.tokens.get(idx).unwrap_or(&Token::End);
            match token {
                Token::LParen => paren_depth += 1,
                Token::RParen => {
                    if paren_depth == 0 {
                        return false;
                    }
                    paren_depth -= 1;
                }
                Token::LBracket => bracket_depth += 1,
                Token::RBracket => {
                    if bracket_depth == 0 {
                        return false;
                    }
                    bracket_depth -= 1;
                }
                Token::LBrace => brace_depth += 1,
                Token::RBrace => {
                    if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 {
                        return false;
                    }
                    brace_depth = brace_depth.saturating_sub(1);
                }
                Token::Colon if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    return true;
                }
                Token::Comma if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    return false;
                }
                Token::End => return false,
                _ => {}
            }
            idx += 1;
        }
    }

    fn parse_dot_stage(&mut self) -> Result<Stage, String> {
        self.expect(Token::Dot)?;
        let mut stages = vec![self.parse_after_dot_token()?];
        loop {
            match self.peek() {
                Token::Dot => {
                    self.bump();
                    stages.push(self.parse_after_dot_token()?);
                }
                Token::LBracket => stages.push(self.parse_bracket_stage()?),
                _ => break,
            }
        }
        if stages.len() == 1 {
            Ok(stages.remove(0))
        } else {
            Ok(Stage::Chain(stages))
        }
    }

    fn parse_after_dot_token(&mut self) -> Result<Stage, String> {
        if let Token::Str(name) = self.peek() {
            let name = name.clone();
            self.bump();
            let optional = self.consume_optional();
            return Ok(Stage::Field { name, optional });
        }
        if self.peek() == &Token::LBracket {
            return self.parse_bracket_stage();
        }
        if matches!(self.peek(), Token::Ident(_)) {
            return Err(
                "parse error: try .[\"field\"] instead of .field for unusually named fields"
                    .to_string(),
            );
        }
        Ok(Stage::Identity)
    }

    fn parse_postfix_dot_suffix(&mut self) -> Result<Stage, String> {
        let stage = self.parse_after_dot_token()?;
        if matches!(stage, Stage::Identity) {
            return Err("parse error: unexpected `.` after expression".to_string());
        }
        Ok(stage)
    }

    fn parse_object_key_name(&mut self) -> Option<String> {
        let name = match self.peek() {
            Token::Ident(name) => name.clone(),
            Token::Str(name) => name.clone(),
            Token::And => "and".to_string(),
            Token::Or => "or".to_string(),
            Token::If => "if".to_string(),
            Token::Then => "then".to_string(),
            Token::Else => "else".to_string(),
            Token::Elif => "elif".to_string(),
            Token::EndKw => "end".to_string(),
            Token::Try => "try".to_string(),
            Token::Catch => "catch".to_string(),
            Token::AsKw => "as".to_string(),
            Token::DefKw => "def".to_string(),
            Token::ModuleKw => "module".to_string(),
            Token::ImportKw => "import".to_string(),
            Token::IncludeKw => "include".to_string(),
            Token::ReduceKw => "reduce".to_string(),
            Token::ForeachKw => "foreach".to_string(),
            Token::LabelKw => "label".to_string(),
            Token::BreakKw => "break".to_string(),
            Token::Loc(_) => "__loc__".to_string(),
            _ => return None,
        };
        self.bump();
        Some(name)
    }

    fn parse_string_expr_for_object_key(&mut self) -> Result<Option<Stage>, String> {
        if let Token::Str(value) = self.peek() {
            let value = value.clone();
            self.bump();
            return Ok(Some(Stage::Literal(ZqValue::String(value))));
        }
        if self.peek() == &Token::QQStringStart {
            return self.parse_qq_string("text".to_string()).map(Some);
        }
        if let Token::Format(fmt) = self.peek() {
            if self.peek_n(1) == &Token::QQStringStart {
                let fmt = fmt.clone();
                self.bump();
                return self.parse_qq_string(fmt).map(Some);
            }
        }
        Ok(None)
    }

    fn parse_bracket_stage(&mut self) -> Result<Stage, String> {
        self.expect(Token::LBracket)?;
        match self.peek() {
            Token::RBracket => {
                self.bump();
                Ok(Stage::Iterate {
                    optional: self.consume_optional(),
                })
            }
            Token::Str(name) => {
                let start_pos = self.pos;
                let name = name.clone();
                self.bump();
                if self.peek() == &Token::RBracket {
                    self.expect(Token::RBracket)?;
                    Ok(Stage::Field {
                        name,
                        optional: self.consume_optional(),
                    })
                } else {
                    self.pos = start_pos;
                    let first = self.parse_pipe_expr()?;
                    if self.peek() == &Token::Colon {
                        self.bump();
                        let end = if self.peek() == &Token::RBracket {
                            None
                        } else {
                            Some(self.parse_bracket_bound_expr()?)
                        };
                        self.expect(Token::RBracket)?;
                        let optional = self.consume_optional();
                        Ok(self.finish_bracket_slice(
                            Some(BracketBound::Dynamic(first)),
                            end,
                            optional,
                        ))
                    } else {
                        self.expect(Token::RBracket)?;
                        Ok(Stage::DynamicIndex {
                            key: Box::new(first),
                            optional: self.consume_optional(),
                        })
                    }
                }
            }
            Token::Colon => {
                self.bump();
                let end = if self.peek() == &Token::RBracket {
                    None
                } else {
                    Some(self.parse_bracket_bound_expr()?)
                };
                self.expect(Token::RBracket)?;
                let optional = self.consume_optional();
                Ok(self.finish_bracket_slice(None, end, optional))
            }
            Token::Int(_) | Token::Minus => {
                let start_pos = self.pos;
                let first = self.parse_bracket_bound_expr()?;
                if self.peek() == &Token::Colon {
                    self.bump();
                    let end = if self.peek() == &Token::RBracket {
                        None
                    } else {
                        Some(self.parse_bracket_bound_expr()?)
                    };
                    self.expect(Token::RBracket)?;
                    let optional = self.consume_optional();
                    Ok(self.finish_bracket_slice(Some(first), end, optional))
                } else if self.peek() == &Token::RBracket {
                    self.expect(Token::RBracket)?;
                    match first {
                        BracketBound::Static(index) => Ok(Stage::Index {
                            index,
                            optional: self.consume_optional(),
                        }),
                        BracketBound::Dynamic(key) => Ok(Stage::DynamicIndex {
                            key: Box::new(key),
                            optional: self.consume_optional(),
                        }),
                    }
                } else {
                    // jq allows general expressions in index form:
                    // .[1+2], .[4,2], .[$i]
                    // If tokens after the first integer are not closing bracket,
                    // reparse as dynamic index expression.
                    self.pos = start_pos;
                    let key = self.parse_pipe_expr()?;
                    self.expect(Token::RBracket)?;
                    Ok(Stage::DynamicIndex {
                        key: Box::new(key),
                        optional: self.consume_optional(),
                    })
                }
            }
            _ => {
                let first = self.parse_pipe_expr()?;
                if self.peek() == &Token::Colon {
                    self.bump();
                    let end = if self.peek() == &Token::RBracket {
                        None
                    } else {
                        Some(self.parse_bracket_bound_expr()?)
                    };
                    self.expect(Token::RBracket)?;
                    let optional = self.consume_optional();
                    Ok(
                        self.finish_bracket_slice(
                            Some(BracketBound::Dynamic(first)),
                            end,
                            optional,
                        ),
                    )
                } else {
                    self.expect(Token::RBracket)?;
                    Ok(Stage::DynamicIndex {
                        key: Box::new(first),
                        optional: self.consume_optional(),
                    })
                }
            }
        }
    }

    fn parse_bracket_bound_expr(&mut self) -> Result<BracketBound, String> {
        let mut sign = 1i64;
        if self.peek() == &Token::Minus {
            self.bump();
            sign = -1;
        }
        match self.peek() {
            Token::Int(index) => {
                let index = *index;
                self.bump();
                Ok(BracketBound::Static(sign.saturating_mul(index)))
            }
            _ if sign == -1 => {
                let expr = self.parse_pipe_expr()?;
                Ok(BracketBound::Dynamic(Stage::UnaryMinus(Box::new(expr))))
            }
            _ => Ok(BracketBound::Dynamic(self.parse_pipe_expr()?)),
        }
    }

    // jq-port: jq/src/jv_aux.c:parse_slice() + jq/src/jv_aux.c:jv_get()
    // Dynamic bounds are encoded as object-key slice access: .[{start:...,end:...}]
    fn finish_bracket_slice(
        &self,
        start: Option<BracketBound>,
        end: Option<BracketBound>,
        optional: bool,
    ) -> Stage {
        let start_is_static = match start.as_ref() {
            None | Some(BracketBound::Static(_)) => true,
            Some(BracketBound::Dynamic(_)) => false,
        };
        let end_is_static = match end.as_ref() {
            None | Some(BracketBound::Static(_)) => true,
            Some(BracketBound::Dynamic(_)) => false,
        };
        if start_is_static && end_is_static {
            let start = start.and_then(|bound| match bound {
                BracketBound::Static(v) => Some(v),
                BracketBound::Dynamic(_) => None,
            });
            let end = end.and_then(|bound| match bound {
                BracketBound::Static(v) => Some(v),
                BracketBound::Dynamic(_) => None,
            });
            return Stage::Slice {
                start,
                end,
                optional,
            };
        }
        let start_expr = start
            .map(bracket_bound_to_stage)
            .unwrap_or(Stage::Literal(ZqValue::Null));
        let end_expr = end
            .map(bracket_bound_to_stage)
            .unwrap_or(Stage::Literal(ZqValue::Null));
        Stage::DynamicIndex {
            key: Box::new(Stage::ObjectLiteral(vec![
                (ObjectKey::Static("start".to_string()), start_expr),
                (ObjectKey::Static("end".to_string()), end_expr),
            ])),
            optional,
        }
    }

    fn consume_optional(&mut self) -> bool {
        if self.peek() == &Token::Question {
            self.bump();
            true
        } else {
            false
        }
    }

    fn is_binding_defined(&self, name: &str) -> bool {
        if matches!(name, "__loc__" | "ENV") {
            return true;
        }
        self.binding_scopes
            .iter()
            .rev()
            .any(|scope| scope.iter().any(|candidate| candidate == name))
    }

    fn expect(&mut self, wanted: Token) -> Result<(), String> {
        if *self.peek() == wanted {
            self.bump();
            Ok(())
        } else {
            Err(format!(
                "parse error: expected {:?}, found {:?}",
                wanted,
                self.peek()
            ))
        }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::End)
    }

    fn peek_n(&self, n: usize) -> &Token {
        self.tokens.get(self.pos + n).unwrap_or(&Token::End)
    }

    fn bump(&mut self) {
        self.pos += 1;
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn import_metadata_search_expands_origin_and_module_relative_paths_like_jq() {
        let mut object = IndexMap::new();
        object.insert(
            "search".to_string(),
            ZqValue::Array(vec![
                ZqValue::String("./".to_string()),
                ZqValue::String("sub".to_string()),
                ZqValue::String("$ORIGIN/lib/jq".to_string()),
                ZqValue::String("sub".to_string()),
            ]),
        );

        let module_dir = tempfile::tempdir().expect("module dir");
        let jq_origin = tempfile::tempdir().expect("jq origin");
        let metadata =
            import_metadata_from_object(&object, Some(module_dir.path()), Some(jq_origin.path()))
                .expect("metadata");
        let search_dirs = metadata.search_dirs.expect("search dirs");
        assert_eq!(
            search_dirs,
            vec![
                module_dir.path().join("./"),
                module_dir.path().join("sub"),
                jq_origin.path().join("lib/jq"),
            ]
        );
    }

    #[test]
    fn import_metadata_search_keeps_origin_literal_when_origin_is_unknown() {
        let mut object = IndexMap::new();
        object.insert(
            "search".to_string(),
            ZqValue::Array(vec![ZqValue::String("$ORIGIN/lib/jq".to_string())]),
        );

        let metadata = import_metadata_from_object(&object, None, None).expect("metadata");
        let search_dirs = metadata.search_dirs.expect("search dirs");
        assert_eq!(search_dirs, vec![PathBuf::from("$ORIGIN/lib/jq")]);
    }

    #[test]
    fn absolute_module_names_are_rejected_like_jq_linker() {
        let temp = tempfile::tempdir().expect("tempdir");
        let abs_stem = temp.path().join("abs_mod");
        let abs_jq = abs_stem.with_extension("jq");
        fs::write(&abs_jq, "def value: 1;").expect("write");

        let parser = Parser::new_with_context(Vec::new(), vec![temp.path().to_path_buf()], None, 0);
        let abs_name = abs_stem.to_string_lossy().replace('\\', "/");
        let err = parser
            .resolve_module_code_path(&abs_name, None)
            .expect_err("absolute import path must fail");
        assert!(
            err.starts_with("module not found:"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn module_resolution_returns_canonical_paths_for_cache_stability() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mods_dir = temp.path().join("mods");
        fs::create_dir_all(&mods_dir).expect("mkdir");
        fs::write(mods_dir.join("one.jq"), "def one: 1;").expect("write jq");
        fs::write(mods_dir.join("one.json"), "{\"v\":1}").expect("write json");

        let non_canonical_root = temp.path().join("mods/../mods");
        let parser =
            Parser::new_with_context(Vec::new(), vec![non_canonical_root.clone()], None, 0);

        let resolved_code = parser
            .resolve_module_code_path("one", None)
            .expect("resolve code");
        let expected_code = fs::canonicalize(mods_dir.join("one.jq")).expect("canonical jq");
        assert_eq!(resolved_code, expected_code);

        let resolved_data = parser
            .resolve_module_data_path("one", None)
            .expect("resolve data");
        let expected_data = fs::canonicalize(mods_dir.join("one.json")).expect("canonical json");
        assert_eq!(resolved_data, expected_data);
    }

    #[test]
    fn load_module_query_rejects_depth_above_limit() {
        let parser = Parser::new_with_context(Vec::new(), Vec::new(), None, 32);
        let err = parser
            .load_module_query("any", None)
            .expect_err("must reject depth");
        assert_eq!(err, "module import depth exceeded");
    }

    #[test]
    fn load_module_data_parses_multiple_json_documents_like_jq() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::write(temp.path().join("stream.json"), "{\"v\":1}\n2\n").expect("write stream");

        let parser = Parser::new_with_context(
            Vec::new(),
            vec![temp.path().to_path_buf()],
            Some(temp.path().to_path_buf()),
            0,
        );

        let value = parser
            .load_module_data("stream", None, false)
            .expect("load module data");
        assert_eq!(
            value,
            ZqValue::Array(vec![
                ZqValue::Object(IndexMap::from([("v".to_string(), ZqValue::from(1))])),
                ZqValue::from(2),
            ])
        );
    }

    #[test]
    fn module_search_roots_follow_jq_default_chain_rules() {
        let module_origin = tempfile::tempdir().expect("module origin");
        let parser = Parser::new_with_context(
            Vec::new(),
            vec![PathBuf::from("librel"), PathBuf::from(".")],
            Some(module_origin.path().to_path_buf()),
            0,
        );

        let roots = parser.module_search_roots(None);
        assert_eq!(
            roots,
            vec![PathBuf::from("."), module_origin.path().join("librel")]
        );
    }

    #[test]
    fn default_module_search_dirs_match_jq_default_chain() {
        let dirs = default_module_search_dirs();
        assert!(
            dirs.starts_with(&[
                PathBuf::from("~/.jq"),
                PathBuf::from("$ORIGIN/../lib/jq"),
                PathBuf::from("$ORIGIN/../lib"),
            ]),
            "dirs={dirs:?}"
        );
    }

    #[test]
    fn explicit_module_search_roots_resolve_relative_to_module_origin() {
        let module_origin = tempfile::tempdir().expect("module origin");
        let parser = Parser::new_with_context(
            Vec::new(),
            vec![PathBuf::from(".")],
            Some(module_origin.path().to_path_buf()),
            0,
        );
        let explicit = vec![PathBuf::from("custom-lib")];
        let roots = parser.module_search_roots(Some(&explicit));
        assert_eq!(roots, vec![module_origin.path().join("custom-lib")]);
    }

    #[test]
    fn import_directive_requires_identifier_or_binding_alias() {
        let err = parse_query(r#"import "mod" as 1; ."#).expect_err("must fail");
        assert!(
            err.contains("expected alias after `as` in import"),
            "err={err}"
        );
    }

    #[test]
    fn module_directive_requires_object_metadata_value() {
        let err = parse_query("module 1; .").expect_err("must fail");
        assert_eq!(err, "Module metadata must be an object");
    }

    #[test]
    fn import_directive_requires_object_metadata_value() {
        let err = parse_query(r#"import "mod" as m 1; ."#).expect_err("must fail");
        assert_eq!(err, "Module metadata must be an object");
    }

    #[test]
    fn canonical_user_defined_while_is_lowered_to_builtin_stage() {
        let parsed = parse_query(
            "def while(cond; update): def _while: if cond then ., (update | _while) else empty end; _while; [while(.<100; .*2)]",
        )
        .expect("parse");
        let root = &parsed.branches[0].stages[0];
        let Stage::ArrayLiteral(items) = root else {
            panic!("expected array literal root, got {root:?}");
        };
        assert!(
            matches!(items.as_slice(), [Stage::While(_, _)]),
            "expected builtin while lowering, got {root:?}"
        );
    }

    #[test]
    fn noncanonical_user_defined_while_stays_function_call() {
        let parsed = parse_query("def while(cond; update): [cond, update]; while(.<100; .*2)")
            .expect("parse");
        let root = &parsed.branches[0].stages[0];
        assert!(
            matches!(
                root,
                Stage::Call {
                    function_id: Some(_),
                    name,
                    args,
                    ..
                } if name == "while" && args.len() == 2
            ),
            "expected while call to remain user-defined, got {root:?}"
        );
    }

    #[test]
    fn canonical_user_defined_until_is_lowered_to_builtin_stage() {
        let parsed = parse_query(
            "def until(cond; next): def _until: if cond then . else (next | _until) end; _until; [until(.>5; .+2)]",
        )
        .expect("parse");
        let root = &parsed.branches[0].stages[0];
        let Stage::ArrayLiteral(items) = root else {
            panic!("expected array literal root, got {root:?}");
        };
        assert!(
            matches!(items.as_slice(), [Stage::Until(_, _)]),
            "expected builtin until lowering, got {root:?}"
        );
    }

    #[test]
    fn noncanonical_user_defined_until_stays_function_call() {
        let parsed =
            parse_query("def until(cond; next): [cond, next]; until(.>5; .+2)").expect("parse");
        let root = &parsed.branches[0].stages[0];
        assert!(
            matches!(
                root,
                Stage::Call {
                    function_id: Some(_),
                    name,
                    args,
                    ..
                } if name == "until" && args.len() == 2
            ),
            "expected until call to remain user-defined, got {root:?}"
        );
    }

    #[test]
    fn normalize_jq_float_text_covers_jq_compat_forms() {
        assert_eq!(normalize_jq_float_text("1."), Some("1.0".to_string()));
        assert_eq!(normalize_jq_float_text("1.e2"), Some("1.0e2".to_string()));
        assert_eq!(normalize_jq_float_text("1.E-2"), Some("1.0E-2".to_string()));
        assert_eq!(normalize_jq_float_text("1.2"), None);
    }

    #[test]
    fn parse_number_literal_accepts_supported_non_canonical_forms() {
        let integer = parse_number_literal("12").expect("integer");
        assert_eq!(integer.as_f64(), Some(12.0));

        let leading_dot = parse_number_literal(".5").expect("leading dot float");
        assert_eq!(leading_dot.as_f64(), Some(0.5));

        let dot_exp = parse_number_literal("1.e2").expect("dot exponent float");
        assert_eq!(dot_exp.as_f64(), Some(100.0));

        let huge_exp = parse_number_literal("1E+1000").expect("huge exponent");
        let ZqValue::Number(raw) = huge_exp else {
            panic!("expected numeric literal");
        };
        assert_eq!(raw.to_string(), "1E+1000");
    }

    #[test]
    fn parse_number_literal_rejects_invalid_literals() {
        let err = parse_number_literal(".e1").expect_err("invalid float");
        assert!(err.contains("invalid number literal"), "err={err}");

        let err = parse_number_literal("nan").expect_err("nan must be rejected");
        assert!(err.contains("invalid number literal"), "err={err}");
    }

    #[test]
    fn fold_large_integer_literal_equality_handles_eq_and_ne() {
        let mk_lit = |raw: &str| {
            Stage::Literal(ZqValue::Number(serde_json::Number::from_string_unchecked(
                raw.to_string(),
            )))
        };

        let lhs = mk_lit("123456789012345678901");
        let rhs_same = mk_lit("123456789012345678901");
        let rhs_diff = mk_lit("123456789012345678902");

        assert_eq!(
            fold_large_integer_literal_equality(&lhs, &rhs_same, BinaryOp::Eq),
            Some(Stage::Literal(ZqValue::Bool(true)))
        );
        assert_eq!(
            fold_large_integer_literal_equality(&lhs, &rhs_diff, BinaryOp::Eq),
            Some(Stage::Literal(ZqValue::Bool(false)))
        );
        assert_eq!(
            fold_large_integer_literal_equality(&lhs, &rhs_diff, BinaryOp::Ne),
            Some(Stage::Literal(ZqValue::Bool(true)))
        );
        assert_eq!(
            fold_large_integer_literal_equality(&lhs, &rhs_diff, BinaryOp::Gt),
            None
        );
    }

    #[test]
    fn const_object_key_error_reports_non_string_types() {
        assert!(const_object_key_error(&ZqValue::String("k".to_string())).is_none());
        let err = const_object_key_error(&ZqValue::from(7)).expect("error");
        assert_eq!(err, "Cannot use number (7) as object key");
        assert_eq!(zq_type_name(&ZqValue::Bool(true)), "boolean");
        assert_eq!(zq_type_name(&ZqValue::Array(Vec::new())), "array");
    }

    #[test]
    fn stage_helper_construction_contract() {
        let pred = Stage::Literal(ZqValue::Bool(true));
        assert_eq!(
            select_stage(pred.clone()),
            Stage::Select(Box::new(pred.clone()))
        );

        let eq = type_eq_stage("number");
        match eq {
            Stage::Binary { op, lhs, rhs } => {
                assert!(matches!(op, BinaryOp::Eq));
                assert!(matches!(*lhs, Stage::Builtin(Builtin::Type)));
                assert_eq!(*rhs, Stage::Literal(ZqValue::String("number".to_string())));
            }
            other => panic!("unexpected type_eq shape: {other:?}"),
        }

        let ne = type_ne_stage("array");
        match ne {
            Stage::Binary { op, lhs, rhs } => {
                assert!(matches!(op, BinaryOp::Ne));
                assert!(matches!(*lhs, Stage::Builtin(Builtin::Type)));
                assert_eq!(*rhs, Stage::Literal(ZqValue::String("array".to_string())));
            }
            other => panic!("unexpected type_ne shape: {other:?}"),
        }

        let by_impl = by_impl_keys_stage(Stage::Identity);
        assert!(matches!(
            by_impl,
            Stage::Map(inner) if matches!(inner.as_ref(), Stage::ArrayLiteral(values) if values == &vec![Stage::Identity])
        ));
    }

    #[test]
    fn stage_helper_abs_loc_bracket_and_chain_contract() {
        let abs = abs_stage();
        assert!(
            matches!(abs, Stage::IfElse { .. }),
            "abs stage shape: {abs:?}"
        );

        let loc = loc_stage(7);
        assert_eq!(
            loc,
            Stage::Literal(ZqValue::Object(IndexMap::from([
                (
                    "file".to_string(),
                    ZqValue::String("<top-level>".to_string())
                ),
                ("line".to_string(), ZqValue::from(7i64)),
            ])))
        );

        assert_eq!(
            bracket_bound_to_stage(BracketBound::Static(3)),
            Stage::Literal(ZqValue::from(3))
        );
        assert_eq!(
            bracket_bound_to_stage(BracketBound::Dynamic(Stage::Identity)),
            Stage::Identity
        );

        let chain = append_chain_stage(Stage::Identity, Stage::Literal(ZqValue::from(1)));
        assert!(matches!(chain, Stage::Chain(ref values) if values.len() == 2));
        let chained = append_chain_stage(chain, Stage::Literal(ZqValue::from(2)));
        assert!(matches!(chained, Stage::Chain(values) if values.len() == 3));
    }

    #[test]
    fn isfinite_stage_matches_expected_shape() {
        let stage = isfinite_stage();
        assert!(
            matches!(
                stage,
                Stage::Binary {
                    op: BinaryOp::And,
                    ..
                }
            ),
            "isfinite stage shape: {stage:?}"
        );
    }

    #[test]
    fn wrap_with_import_bindings_preserves_binding_order() {
        let wrapped = wrap_with_import_bindings(
            Stage::Identity,
            &[
                ("a".to_string(), ZqValue::from(1)),
                ("b".to_string(), ZqValue::from(2)),
            ],
        );
        assert_eq!(
            wrapped,
            Stage::Bind {
                source: Box::new(Stage::Literal(ZqValue::from(1))),
                pattern: BindingPattern::Var("a".to_string()),
                body: Box::new(Stage::Bind {
                    source: Box::new(Stage::Literal(ZqValue::from(2))),
                    pattern: BindingPattern::Var("b".to_string()),
                    body: Box::new(Stage::Identity),
                }),
            }
        );
    }

    #[test]
    fn rewrite_stage_symbol_ids_rewrites_function_and_param_ids() {
        let mut stage = Stage::Call {
            function_id: Some(1),
            param_id: Some(10),
            name: "old".to_string(),
            args: vec![Stage::Call {
                function_id: Some(2),
                param_id: None,
                name: "keep".to_string(),
                args: Vec::new(),
            }],
        };
        let function_id_map = BTreeMap::from([(1usize, 11usize)]);
        let function_name_map = BTreeMap::from([(1usize, "ns::new".to_string())]);
        let param_id_map = BTreeMap::from([(10usize, 20usize)]);

        rewrite_stage_symbol_ids(
            &mut stage,
            &function_id_map,
            &function_name_map,
            &param_id_map,
        );

        let Stage::Call {
            function_id,
            param_id,
            name,
            args,
        } = stage
        else {
            panic!("expected call stage");
        };
        assert_eq!(function_id, Some(11));
        assert_eq!(param_id, Some(20));
        assert_eq!(name, "ns::new");
        assert!(
            matches!(args.as_slice(), [Stage::Call { function_id: Some(2), name, .. }] if name == "keep")
        );
    }

    #[test]
    fn rewrite_binding_pattern_and_collect_bindings_contract() {
        let mut pattern = BindingPattern::Object(vec![ObjectBindingEntry {
            key: BindingKeySpec::Expr(Box::new(Stage::Call {
                function_id: Some(1),
                param_id: Some(7),
                name: "f".to_string(),
                args: Vec::new(),
            })),
            store_var: Some("captured".to_string()),
            pattern: BindingPattern::Alternatives(vec![
                BindingPattern::Var("x".to_string()),
                BindingPattern::Var("x".to_string()),
            ]),
        }]);

        let function_id_map = BTreeMap::from([(1usize, 42usize)]);
        let function_name_map = BTreeMap::from([(1usize, "ns::f".to_string())]);
        let param_id_map = BTreeMap::from([(7usize, 70usize)]);
        rewrite_binding_pattern_symbol_ids(
            &mut pattern,
            &function_id_map,
            &function_name_map,
            &param_id_map,
        );

        let BindingPattern::Object(entries) = &pattern else {
            panic!("expected object pattern");
        };
        let entry = entries.first().expect("entry");
        let BindingKeySpec::Expr(key_expr) = &entry.key else {
            panic!("expected expr key");
        };
        assert!(
            matches!(&**key_expr, Stage::Call { function_id: Some(42), param_id: Some(70), name, .. } if name == "ns::f")
        );

        let mut bindings = Vec::new();
        collect_pattern_bindings(&pattern, &mut bindings);
        assert_eq!(bindings, vec!["captured".to_string(), "x".to_string(),]);
    }

    #[test]
    fn function_binding_scope_pop_restores_previous_definition() {
        let mut parser = Parser::new(vec![Token::End]);
        parser.def_scope_stack.push(Vec::new());
        parser.push_function_binding(("f".to_string(), 0), 10);
        assert_eq!(parser.resolve_user_function("f", 0), Some(10));

        parser.def_scope_stack.push(Vec::new());
        parser.push_function_binding(("f".to_string(), 0), 11);
        assert_eq!(parser.resolve_user_function("f", 0), Some(11));

        parser.pop_def_scope();
        assert_eq!(parser.resolve_user_function("f", 0), Some(10));

        parser.pop_def_scope();
        assert_eq!(parser.resolve_user_function("f", 0), None);
    }

    #[test]
    fn resolve_param_call_prefers_innermost_scope() {
        let mut parser = Parser::new(vec![Token::End]);
        parser
            .local_param_scopes
            .push(BTreeMap::from([(("x".to_string(), 0), 1)]));
        parser
            .local_param_scopes
            .push(BTreeMap::from([(("x".to_string(), 0), 2)]));

        assert_eq!(parser.resolve_param_call("x", 0), Some(2));
        assert_eq!(parser.resolve_param_call("x", 1), None);
        assert_eq!(parser.resolve_param_call("missing", 0), None);
    }
}
