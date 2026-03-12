use super::*;
use std::fs;
use std::path::{Path, PathBuf};

impl Parser {
    pub(super) fn load_module_query(
        &self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
    ) -> Result<Query, String> {
        if self.module_load_depth >= 32 {
            return Err("module import depth exceeded".to_string());
        }
        let module_path = self.resolve_module_code_path(module, search_dirs)?;
        // jq-port: jq/src/linker.c process_dependencies()/load_library() caches
        // code modules by resolved path and rebinds definitions on reuse.
        if let Some(cached) = self.module_query_cache.borrow().get(&module_path) {
            return Ok(cached.clone());
        }
        let source = fs::read_to_string(&module_path)
            .map_err(|e| format!("module not found: {module} ({e})"))?;
        let tokens =
            lex(&source).map_err(|e| format!("lex error at {}: {}", e.position, e.message))?;
        let current_module_dir = module_path.parent().map(Path::to_path_buf);
        let mut parser = Parser::new_with_context_and_cache(
            tokens,
            self.module_search_dirs.clone(),
            current_module_dir,
            self.module_load_depth + 1,
            self.module_query_cache.clone(),
        );
        let parsed = parser.parse_query()?;
        self.module_query_cache.borrow_mut().insert(module_path, parsed.clone());
        Ok(parsed)
    }

    pub(super) fn load_module_data(
        &self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
        raw: bool,
    ) -> Result<ZqValue, String> {
        let module_path = self.resolve_module_data_path(module, search_dirs)?;
        let source = fs::read_to_string(&module_path)
            .map_err(|e| format!("module not found: {module} ({e})"))?;
        if raw {
            return Ok(ZqValue::String(source));
        }
        parse_module_json_documents(module, &source)
    }

    pub(super) fn resolve_module_code_path(
        &self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
    ) -> Result<PathBuf, String> {
        self.resolve_module_path_with_candidates(module, search_dirs, module_code_candidates)
    }

    pub(super) fn resolve_module_data_path(
        &self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
    ) -> Result<PathBuf, String> {
        self.resolve_module_path_with_candidates(module, search_dirs, module_data_candidates)
    }

    fn resolve_module_path_with_candidates(
        &self,
        module: &str,
        search_dirs: Option<&[PathBuf]>,
        candidates_for_root: fn(&Path, &str) -> Vec<PathBuf>,
    ) -> Result<PathBuf, String> {
        validate_module_relpath(module)?;
        // jq/src/linker.c: find_lib() always treats module names as relative.
        if Path::new(module).is_absolute() {
            return Err(module_not_found_err(module));
        }
        for root in self.module_search_roots(search_dirs) {
            for candidate in candidates_for_root(&root, module) {
                if candidate.exists() && candidate.is_file() {
                    return Ok(canonicalize_module_candidate(candidate));
                }
            }
        }
        Err(module_not_found_err(module))
    }

    pub(super) fn module_search_roots(&self, search_dirs: Option<&[PathBuf]>) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let jq_origin = jq_origin_dir();
        let candidate_dirs: Vec<PathBuf> = match search_dirs {
            Some(explicit) => explicit.to_vec(),
            None => {
                // jq/src/linker.c:default_search() prepends "." to jq library paths.
                let mut defaults = vec![PathBuf::from(".")];
                defaults.extend(self.module_search_dirs.clone());
                defaults
            }
        };
        for dir in candidate_dirs {
            let normalized = normalize_search_root_like_jq(
                &dir,
                self.current_module_dir.as_deref(),
                jq_origin.as_deref(),
            );
            if !out.iter().any(|seen| seen == &normalized) {
                out.push(normalized);
            }
        }
        out
    }
}

fn module_not_found_err(module: &str) -> String {
    format!("module not found: {module}")
}

fn parse_module_json_documents(module: &str, source: &str) -> Result<ZqValue, String> {
    // jq/src/jv_file.c:jv_load_file(raw=0):
    // parse all JSON texts from the file and return them as an array.
    let mut values = Vec::new();
    let stream = serde_json::Deserializer::from_str(source).into_iter::<serde_json::Value>();
    for item in stream {
        let value = item.map_err(|e| format!("module not found: {module} ({e})"))?;
        values.push(ZqValue::from_json(value));
    }
    Ok(ZqValue::Array(values))
}
