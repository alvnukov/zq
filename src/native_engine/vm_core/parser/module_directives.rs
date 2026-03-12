use super::*;

#[derive(Debug, Clone)]
enum ImportAlias {
    Namespace(String),
    Binding(String),
}

#[derive(Debug, Clone, Default)]
pub(super) struct ImportMetadata {
    pub(super) raw_object: Option<IndexMap<String, ZqValue>>,
    pub(super) search_dirs: Option<Vec<PathBuf>>,
    pub(super) optional: bool,
    pub(super) raw: bool,
}

impl Parser {
    // jq-port: jq parser top-level declarations:
    //   module <metadata>;
    //   import <path> as <alias> [<metadata>];
    //   include <path> [<metadata>];
    pub(super) fn parse_module_directive(&mut self) -> Result<(), String> {
        match self.peek() {
            Token::ModuleKw => self.parse_module_metadata_declaration(),
            Token::ImportKw => self.parse_import_directive(),
            Token::IncludeKw => self.parse_include_directive(),
            _ => Err("parse error: expected module/import/include directive".to_string()),
        }
    }

    fn parse_module_metadata_declaration(&mut self) -> Result<(), String> {
        self.bump();
        let metadata = self.parse_pipe_expr()?;
        self.expect(Token::Semi)?;
        ensure_const_object_metadata(&metadata)?;
        if let Some(value) = const_stage_value(&metadata) {
            self.module_decl_meta = Some(value);
        }
        Ok(())
    }

    fn parse_import_directive(&mut self) -> Result<(), String> {
        self.bump();
        let module_expr = self.parse_import_path_expr()?;
        self.expect(Token::AsKw)?;
        let alias = self.parse_import_alias()?;
        let metadata = self.parse_import_metadata()?;
        self.expect(Token::Semi)?;

        let module = module_path_literal(&module_expr)?;
        match alias {
            ImportAlias::Namespace(namespace) => {
                self.push_module_dependency(&module, Some(&namespace), false, &metadata);
                self.try_import_code_module(
                    &module,
                    metadata.search_dirs.as_deref(),
                    Some(&namespace),
                    metadata.optional,
                )
            }
            ImportAlias::Binding(binding) => {
                self.push_module_dependency(&module, Some(&binding), true, &metadata);
                self.try_import_data_module(
                    &module,
                    metadata.search_dirs.as_deref(),
                    metadata.raw,
                    metadata.optional,
                    &binding,
                )
            }
        }
    }

    fn parse_include_directive(&mut self) -> Result<(), String> {
        self.bump();
        let module_expr = self.parse_import_path_expr()?;
        let metadata = self.parse_import_metadata()?;
        self.expect(Token::Semi)?;
        let module = module_path_literal(&module_expr)?;
        self.push_module_dependency(&module, None, false, &metadata);
        self.try_import_code_module(
            &module,
            metadata.search_dirs.as_deref(),
            None,
            metadata.optional,
        )
    }

    fn parse_import_alias(&mut self) -> Result<ImportAlias, String> {
        match self.peek() {
            Token::Ident(name) => {
                let name = name.clone();
                self.bump();
                Ok(ImportAlias::Namespace(name))
            }
            Token::Binding(name) => {
                let name = name.clone();
                self.bump();
                Ok(ImportAlias::Binding(name))
            }
            other => {
                Err(format!("parse error: expected alias after `as` in import, found {:?}", other))
            }
        }
    }

    fn try_import_code_module(
        &mut self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
        namespace: Option<&str>,
        optional: bool,
    ) -> Result<(), String> {
        if !self.resolve_imports {
            return Ok(());
        }
        match self.load_module_query(module, search_dirs) {
            Ok(query) => self.import_module_functions(query.functions, namespace),
            Err(err) if optional && err.starts_with("module not found:") => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn try_import_data_module(
        &mut self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
        raw: bool,
        optional: bool,
        binding: &str,
    ) -> Result<(), String> {
        if !self.resolve_imports {
            return Ok(());
        }
        match self.load_module_data(module, search_dirs, raw) {
            Ok(value) => {
                self.register_imported_binding(binding.to_string(), value.clone());
                self.register_imported_binding(format!("{binding}::{binding}"), value);
                Ok(())
            }
            Err(_err) if optional => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn parse_import_path_expr(&mut self) -> Result<Stage, String> {
        // jq parser accepts interpolated strings in ImportFrom grammar then
        // rejects non-constant paths with "Import path must be constant".
        // Keep unresolved calls while parsing the interpolation to preserve that order.
        let prev_allow_unresolved = self.allow_unresolved_calls;
        self.allow_unresolved_calls = true;
        let parsed = self.parse_string_expr_for_object_key();
        self.allow_unresolved_calls = prev_allow_unresolved;
        if let Some(expr) = parsed? {
            return Ok(expr);
        }
        Err(format!("parse error: expected import/include path string, found {:?}", self.peek()))
    }

    fn parse_import_metadata(&mut self) -> Result<ImportMetadata, String> {
        if self.peek() == &Token::Semi {
            return Ok(ImportMetadata::default());
        }
        let metadata_expr = self.parse_pipe_expr()?;
        ensure_const_object_metadata(&metadata_expr)?;
        let Some(ZqValue::Object(object)) = const_stage_value(&metadata_expr) else {
            return Err("Module metadata must be an object".to_string());
        };
        let jq_origin = jq_origin_dir();
        import_metadata_from_object(
            &object,
            self.current_module_dir.as_deref(),
            jq_origin.as_deref(),
        )
    }

    fn push_module_dependency(
        &mut self,
        module: &str,
        alias: Option<&str>,
        is_data: bool,
        metadata: &ImportMetadata,
    ) {
        // jq/src/compile.c: gen_import_meta() uses jv_object_merge(metadata, import_meta),
        // so metadata fields keep their original order and import fields are appended.
        let mut dep = metadata.raw_object.clone().unwrap_or_default();
        if let Some(alias) = alias {
            dep.insert("as".to_string(), ZqValue::String(alias.to_string()));
        }
        dep.insert("is_data".to_string(), ZqValue::Bool(is_data));
        dep.insert("relpath".to_string(), ZqValue::String(module.to_string()));
        self.module_decl_deps.push(ZqValue::Object(dep));
    }

    pub(super) fn module_descriptor_value(&self) -> ZqValue {
        let mut desc = match self.module_decl_meta.clone() {
            Some(ZqValue::Object(object)) => object,
            _ => IndexMap::new(),
        };
        desc.insert("deps".to_string(), ZqValue::Array(self.module_decl_deps.clone()));
        desc.insert(
            "defs".to_string(),
            ZqValue::Array(self.local_function_defs.iter().cloned().map(ZqValue::String).collect()),
        );
        ZqValue::Object(desc)
    }

    fn register_imported_binding(&mut self, name: String, value: ZqValue) {
        if let Some(scope) = self.binding_scopes.last_mut() {
            push_unique_binding(scope, &name);
        }
        self.imported_bindings.push((name, value));
    }
}
