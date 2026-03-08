use super::*;

pub(crate) fn default_module_search_dirs() -> Vec<PathBuf> {
    // jq/src/main.c default lib search list
    let mut dirs = vec![
        PathBuf::from("~/.jq"),
        PathBuf::from("$ORIGIN/../lib/jq"),
        PathBuf::from("$ORIGIN/../lib"),
    ];
    for env_name in ["JQ_LIBRARY_PATH", "ZQ_LIBRARY_PATH"] {
        let Ok(raw) = std::env::var(env_name) else {
            continue;
        };
        for dir in std::env::split_paths(&raw) {
            if dir.as_os_str().is_empty() || dirs.iter().any(|seen| seen == &dir) {
                continue;
            }
            dirs.push(dir);
        }
    }
    dirs
}

pub(super) fn module_path_literal(stage: &Stage) -> Result<String, String> {
    match const_stage_value(stage) {
        Some(ZqValue::String(path)) => Ok(path),
        _ => Err("Import path must be constant".to_string()),
    }
}

pub(super) fn ensure_const_object_metadata(stage: &Stage) -> Result<(), String> {
    match const_stage_value(stage) {
        None => Err("Module metadata must be constant".to_string()),
        Some(ZqValue::Object(_)) => Ok(()),
        Some(_) => Err("Module metadata must be an object".to_string()),
    }
}

// jq-port: jq/src/linker.c process_dependencies() metadata fields:
// `search`, `optional`, `raw`.
pub(super) fn import_metadata_from_object(
    object: &IndexMap<String, ZqValue>,
    current_module_dir: Option<&Path>,
    jq_origin: Option<&Path>,
) -> Result<ImportMetadata, String> {
    let optional = matches!(object.get("optional"), Some(ZqValue::Bool(true)));
    let raw = matches!(object.get("raw"), Some(ZqValue::Bool(true)));
    let search_raw = object.get("search").cloned();

    let search_dirs = if let Some(search_value) = &search_raw {
        let mut values = Vec::new();
        match search_value {
            ZqValue::Array(items) => values.extend(items.iter().cloned()),
            other => values.push(other.clone()),
        }

        let mut dirs = Vec::new();
        for value in values {
            let ZqValue::String(path_text) = value else {
                continue;
            };
            let path = resolve_import_search_path_like_jq(
                path_text.as_str(),
                current_module_dir,
                jq_origin,
            );
            if !dirs.iter().any(|seen| seen == &path) {
                dirs.push(path);
            }
        }
        Some(dirs)
    } else {
        None
    };

    Ok(ImportMetadata {
        raw_object: Some(object.clone()),
        search_dirs,
        optional,
        raw,
    })
}

fn resolve_import_search_path_like_jq(
    path_text: &str,
    current_module_dir: Option<&Path>,
    jq_origin: Option<&Path>,
) -> PathBuf {
    if let Some(home_expanded) = expand_home_prefix_like_jq(path_text) {
        return home_expanded;
    }
    // jq/src/linker.c:build_lib_search_chain() treats "$ORIGIN/" specially.
    if let Some(suffix) = path_text.strip_prefix("$ORIGIN/") {
        if let Some(origin) = jq_origin {
            return origin.join(suffix);
        }
        return PathBuf::from(path_text);
    }

    let mut path = PathBuf::from(path_text);
    if path_text != "." && !path.is_absolute() {
        if let Some(base) = current_module_dir {
            path = base.join(path);
        }
    }
    path
}

pub(super) fn jq_origin_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
}

pub(super) fn home_dir_like_jq() -> Option<PathBuf> {
    for env_name in ["HOME", "USERPROFILE"] {
        let Ok(value) = std::env::var(env_name) else {
            continue;
        };
        if !value.is_empty() {
            return Some(PathBuf::from(value));
        }
    }
    None
}

fn expand_home_prefix_like_jq(path_text: &str) -> Option<PathBuf> {
    let home = home_dir_like_jq()?;
    if path_text == "~" {
        return Some(home);
    }
    path_text.strip_prefix("~/").map(|suffix| home.join(suffix))
}

pub(super) fn canonicalize_module_candidate(path: PathBuf) -> PathBuf {
    fs::canonicalize(&path).unwrap_or(path)
}

pub(super) fn normalize_search_root_like_jq(
    root: &Path,
    current_module_dir: Option<&Path>,
    jq_origin: Option<&Path>,
) -> PathBuf {
    let raw = root.to_string_lossy();
    if raw == "." {
        return PathBuf::from(".");
    }
    if let Some(home_expanded) = expand_home_prefix_like_jq(&raw) {
        return home_expanded;
    }
    if let Some(suffix) = raw.strip_prefix("$ORIGIN/") {
        if let Some(origin) = jq_origin {
            return origin.join(suffix);
        }
        return root.to_path_buf();
    }

    if root.is_relative() {
        if let Some(base) = current_module_dir {
            return base.join(root);
        }
    }
    root.to_path_buf()
}

pub(super) fn const_stage_value(stage: &Stage) -> Option<ZqValue> {
    match stage {
        Stage::Literal(value) => Some(value.clone()),
        Stage::ArrayLiteral(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(const_stage_value(item)?);
            }
            Some(ZqValue::Array(out))
        }
        Stage::ObjectLiteral(entries) => {
            let mut out = IndexMap::with_capacity(entries.len());
            for (key, value) in entries {
                let key = match key {
                    ObjectKey::Static(key) => key.clone(),
                    ObjectKey::Expr(expr) => match const_stage_value(expr)? {
                        ZqValue::String(key) => key,
                        _ => return None,
                    },
                };
                out.insert(key, const_stage_value(value)?);
            }
            Some(ZqValue::Object(out))
        }
        Stage::UnaryMinus(inner) => {
            let value = const_stage_value(inner)?;
            let as_f64 = value.clone().into_json().as_f64()?;
            let number = serde_json::Number::from_f64(-as_f64)?;
            Some(ZqValue::Number(number))
        }
        _ => None,
    }
}

// jq-port: jq/src/linker.c:validate_relpath()
pub(super) fn validate_module_relpath(path: &str) -> Result<(), String> {
    if path.contains('\\') {
        return Err(format!(
            "Modules must be named by relative paths using '/', not '\\\\' ({path})"
        ));
    }
    let components = path.split('/').collect::<Vec<_>>();
    for (idx, component) in components.iter().enumerate() {
        if *component == ".." {
            return Err(format!(
                "Relative paths to modules may not traverse to parent directories ({path})"
            ));
        }
        if idx > 0 && components[idx - 1] == *component {
            return Err(format!(
                "module names must not have equal consecutive components: {path}"
            ));
        }
    }
    Ok(())
}

// jq-port: jq/src/linker.c:find_lib() candidate order for code libs.
pub(super) fn module_code_candidates(root: &Path, module: &str) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(3);
    out.push(root.join(format!("{module}.jq")));
    out.push(root.join(module).join("jq").join("main.jq"));
    if let Some(name) = module.rsplit('/').next() {
        if !name.is_empty() {
            out.push(root.join(module).join(format!("{name}.jq")));
        }
    }
    out
}

// jq-port: jq/src/linker.c:find_lib() candidate order for data libs.
pub(super) fn module_data_candidates(root: &Path, module: &str) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(3);
    out.push(root.join(format!("{module}.json")));
    out.push(root.join(module).join("jq").join("main.json"));
    if let Some(name) = module.rsplit('/').next() {
        if !name.is_empty() {
            out.push(root.join(module).join(format!("{name}.json")));
        }
    }
    out
}
