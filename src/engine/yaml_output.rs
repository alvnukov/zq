use crate::value::ZqValue;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::OnceLock;

use super::{Error, YamlAnchorNameMode, YamlFormatOptions};

pub(super) fn format_output_yaml_documents(values: &[JsonValue]) -> Result<String, Error> {
    format_output_yaml_documents_with_options(values, YamlFormatOptions::default())
}

pub(super) fn format_output_yaml_documents_with_options(
    values: &[JsonValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(json_to_yaml_value(value)?);
    }
    if options.use_anchors {
        format_yaml_documents_with_anchors(
            &docs,
            options.anchor_name_mode,
            options.anchor_enrich_single_token,
        )
    } else {
        format_yaml_documents_plain(&docs)
    }
}

pub(super) fn format_output_yaml_documents_native(values: &[ZqValue]) -> Result<String, Error> {
    format_output_yaml_documents_native_with_options(values, YamlFormatOptions::default())
}

pub(super) fn format_output_yaml_documents_native_with_options(
    values: &[ZqValue],
    options: YamlFormatOptions,
) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(native_to_yaml_value(value)?);
    }
    if options.use_anchors {
        format_yaml_documents_with_anchors(
            &docs,
            options.anchor_name_mode,
            options.anchor_enrich_single_token,
        )
    } else {
        format_yaml_documents_plain(&docs)
    }
}

fn format_yaml_documents_plain(values: &[serde_yaml::Value]) -> Result<String, Error> {
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        let mut rendered =
            serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
        while rendered.ends_with('\n') {
            rendered.pop();
        }
        if rendered.is_empty() {
            rendered.push_str("null");
        }
        docs.push(rendered);
    }

    if docs.is_empty() {
        return Ok(String::new());
    }
    if docs.len() == 1 {
        return Ok(docs.remove(0));
    }
    Ok(docs.join("\n---\n"))
}

fn format_yaml_documents_with_anchors(
    values: &[serde_yaml::Value],
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Result<String, Error> {
    if values.is_empty() {
        return Ok(String::new());
    }

    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(render_yaml_document_with_anchors(value, name_mode, enrich_single_token)?);
    }

    if docs.len() == 1 {
        return Ok(docs.remove(0));
    }
    Ok(docs.join("\n---\n"))
}

fn render_yaml_document_with_anchors(
    value: &serde_yaml::Value,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Result<String, Error> {
    let mut plan = YamlAnchorPlan::new(value, name_mode, enrich_single_token);
    let mut out = String::new();
    emit_yaml_value_standalone(value, 0, true, &mut out, &mut plan)?;
    if out.is_empty() {
        out.push_str("null");
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum YamlFingerprint {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Sequence(Vec<u32>),
    Mapping(Vec<(u32, u32)>),
    Tagged(String, u32),
}

#[derive(Default)]
struct YamlAnchorAnalyzer {
    interner: HashMap<YamlFingerprint, u32>,
    fingerprints: Vec<YamlFingerprint>,
    node_ids: HashMap<usize, u32>,
    value_counts: Vec<usize>,
}

impl YamlAnchorAnalyzer {
    fn analyze_document(
        value: &serde_yaml::Value,
        name_mode: YamlAnchorNameMode,
        enrich_single_token: bool,
    ) -> YamlAnchorPlan {
        let mut analyzer = Self::default();
        analyzer.analyze_node(value, true);

        let mut first_seen = vec![usize::MAX; analyzer.fingerprints.len()];
        let mut cursor = 0usize;
        fill_first_seen(value, true, &analyzer.node_ids, &mut first_seen, &mut cursor);
        let mut first_hints = vec![None; analyzer.fingerprints.len()];
        let mut path = Vec::new();
        fill_first_hints(
            value,
            true,
            &analyzer.node_ids,
            &mut first_hints,
            &mut path,
            name_mode,
            enrich_single_token,
        );

        let mut repeated_ids = analyzer
            .value_counts
            .iter()
            .enumerate()
            .filter_map(|(id, count)| {
                if should_emit_anchor(&analyzer.fingerprints[id], *count) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        repeated_ids.sort_by_key(|id| (first_seen[*id], *id));

        let mut selected_ids = HashSet::<usize>::new();
        let mut selected_order = Vec::<usize>::new();
        for id in repeated_ids {
            let exposed = count_exposed_occurrences(
                value,
                &analyzer.node_ids,
                &selected_ids,
                analyzer.fingerprints.len(),
                id,
            );
            if exposed >= 2 {
                selected_ids.insert(id);
                selected_order.push(id);
            }
        }

        let merge_sources = select_mapping_merge_sources(&analyzer.fingerprints, &first_seen);
        let mut merge_base_ids = merge_sources.values().copied().collect::<Vec<_>>();
        merge_base_ids.sort_by_key(|id| (first_seen[*id], *id));
        merge_base_ids.dedup();
        for id in merge_base_ids {
            if selected_ids.insert(id) {
                selected_order.push(id);
            }
        }
        selected_order.sort_by_key(|id| (first_seen[*id], *id));

        let mut anchor_names = vec![None; analyzer.fingerprints.len()];
        let mut used_names = HashSet::<String>::new();
        for id in selected_order {
            let hint = first_hints[id].as_deref();
            let base = build_anchor_base_name(
                hint,
                &analyzer.fingerprints[id],
                name_mode,
                enrich_single_token,
            );
            let name = unique_anchor_name(base, &mut used_names);
            anchor_names[id] = Some(name);
        }

        let fingerprint_len = analyzer.fingerprints.len();
        YamlAnchorPlan {
            node_ids: analyzer.node_ids,
            fingerprints: analyzer.fingerprints,
            anchor_names,
            emitted: vec![false; fingerprint_len],
            merge_sources: merge_sources
                .into_iter()
                .map(|(target, base)| (target as u32, base as u32))
                .collect(),
        }
    }

    fn analyze_node(&mut self, value: &serde_yaml::Value, value_position: bool) -> u32 {
        use serde_yaml::Value as YamlValue;

        let fingerprint = match value {
            YamlValue::Null => YamlFingerprint::Null,
            YamlValue::Bool(v) => YamlFingerprint::Bool(*v),
            YamlValue::Number(v) => YamlFingerprint::Number(v.to_string()),
            YamlValue::String(v) => YamlFingerprint::String(v.clone()),
            YamlValue::Sequence(items) => {
                let mut child_ids = Vec::with_capacity(items.len());
                for item in items {
                    child_ids.push(self.analyze_node(item, value_position));
                }
                YamlFingerprint::Sequence(child_ids)
            }
            YamlValue::Mapping(map) => {
                let mut pairs = Vec::with_capacity(map.len());
                for (key, val) in map {
                    let key_id = self.analyze_node(key, false);
                    let val_id = self.analyze_node(val, value_position);
                    pairs.push((key_id, val_id));
                }
                YamlFingerprint::Mapping(pairs)
            }
            YamlValue::Tagged(tagged) => {
                let inner_id = self.analyze_node(&tagged.value, value_position);
                YamlFingerprint::Tagged(tagged.tag.to_string(), inner_id)
            }
        };

        let id = if let Some(id) = self.interner.get(&fingerprint) {
            *id
        } else {
            let next = self.fingerprints.len() as u32;
            self.interner.insert(fingerprint.clone(), next);
            self.fingerprints.push(fingerprint);
            self.value_counts.push(0);
            next
        };

        self.node_ids.insert(value as *const _ as usize, id);
        if value_position {
            self.value_counts[id as usize] += 1;
        }
        id
    }
}

fn should_emit_anchor(fingerprint: &YamlFingerprint, count: usize) -> bool {
    if count < 2 {
        return false;
    }

    match fingerprint {
        // Anchoring tiny scalars hurts readability more than it helps.
        YamlFingerprint::Null | YamlFingerprint::Bool(_) | YamlFingerprint::Number(_) => false,
        // Require repeated long strings to avoid noisy aliases for values like "dev".
        YamlFingerprint::String(s) => count >= 3 && s.len() >= 8,
        // Structured repeated values usually yield meaningful size savings.
        YamlFingerprint::Sequence(_)
        | YamlFingerprint::Mapping(_)
        | YamlFingerprint::Tagged(_, _) => true,
    }
}

const MIN_MERGE_BASE_KEYS: usize = 2;

fn mapping_pairs(fingerprint: &YamlFingerprint) -> Option<&[(u32, u32)]> {
    match fingerprint {
        YamlFingerprint::Mapping(pairs) => Some(pairs),
        YamlFingerprint::Null
        | YamlFingerprint::Bool(_)
        | YamlFingerprint::Number(_)
        | YamlFingerprint::String(_)
        | YamlFingerprint::Sequence(_)
        | YamlFingerprint::Tagged(_, _) => None,
    }
}

fn mapping_is_strict_subset(base: &[(u32, u32)], target: &[(u32, u32)]) -> bool {
    if base.len() < MIN_MERGE_BASE_KEYS || base.len() >= target.len() {
        return false;
    }
    base.iter().all(|pair| target.iter().any(|candidate| candidate == pair))
}

fn select_mapping_merge_sources(
    fingerprints: &[YamlFingerprint],
    first_seen: &[usize],
) -> HashMap<usize, usize> {
    let mut out = HashMap::<usize, usize>::new();
    for (target_id, target_fp) in fingerprints.iter().enumerate() {
        let Some(target_pairs) = mapping_pairs(target_fp) else {
            continue;
        };
        if target_pairs.len() <= MIN_MERGE_BASE_KEYS {
            continue;
        }

        let mut best: Option<(usize, usize, usize)> = None;
        for (base_id, base_fp) in fingerprints.iter().enumerate() {
            if base_id == target_id || first_seen[base_id] >= first_seen[target_id] {
                continue;
            }
            let Some(base_pairs) = mapping_pairs(base_fp) else {
                continue;
            };
            if !mapping_is_strict_subset(base_pairs, target_pairs) {
                continue;
            }

            let rank = (base_id, base_pairs.len(), first_seen[base_id]);
            let better = match best {
                None => true,
                Some((best_id, best_len, best_seen)) => {
                    rank.1 > best_len
                        || (rank.1 == best_len && rank.2 < best_seen)
                        || (rank.1 == best_len && rank.2 == best_seen && rank.0 < best_id)
                }
            };
            if better {
                best = Some(rank);
            }
        }

        if let Some((base_id, _, _)) = best {
            out.insert(target_id, base_id);
        }
    }
    out
}

fn count_exposed_occurrences(
    value: &serde_yaml::Value,
    node_ids: &HashMap<usize, u32>,
    selected_ids: &HashSet<usize>,
    id_count: usize,
    target_id: usize,
) -> usize {
    let mut emitted_selected = vec![false; id_count];
    let mut count = 0usize;
    count_exposed_occurrences_rec(
        value,
        node_ids,
        selected_ids,
        &mut emitted_selected,
        target_id,
        &mut count,
    );
    count
}

fn count_exposed_occurrences_rec(
    value: &serde_yaml::Value,
    node_ids: &HashMap<usize, u32>,
    selected_ids: &HashSet<usize>,
    emitted_selected: &mut [bool],
    target_id: usize,
    count: &mut usize,
) {
    use serde_yaml::Value as YamlValue;

    let Some(id) = node_ids.get(&(value as *const _ as usize)).copied() else {
        return;
    };
    let idx = id as usize;
    if selected_ids.contains(&idx) {
        if emitted_selected[idx] {
            return;
        }
        emitted_selected[idx] = true;
    }

    if idx == target_id {
        *count += 1;
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                count_exposed_occurrences_rec(
                    item,
                    node_ids,
                    selected_ids,
                    emitted_selected,
                    target_id,
                    count,
                );
            }
        }
        YamlValue::Mapping(map) => {
            for (_, val) in map {
                count_exposed_occurrences_rec(
                    val,
                    node_ids,
                    selected_ids,
                    emitted_selected,
                    target_id,
                    count,
                );
            }
        }
        YamlValue::Tagged(tagged) => {
            count_exposed_occurrences_rec(
                &tagged.value,
                node_ids,
                selected_ids,
                emitted_selected,
                target_id,
                count,
            );
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

fn fill_first_hints(
    value: &serde_yaml::Value,
    value_position: bool,
    node_ids: &HashMap<usize, u32>,
    first_hints: &mut [Option<String>],
    path: &mut Vec<String>,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) {
    use serde_yaml::Value as YamlValue;

    if value_position {
        if let Some(id) = node_ids.get(&(value as *const _ as usize)) {
            let idx = *id as usize;
            if first_hints[idx].is_none() {
                first_hints[idx] = Some(anchor_hint_from_path(path));
            }
        }
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                fill_first_hints(
                    item,
                    value_position,
                    node_ids,
                    first_hints,
                    path,
                    name_mode,
                    enrich_single_token,
                );
            }
        }
        YamlValue::Mapping(map) => {
            for (key, val) in map {
                if let Some(segment) = yaml_key_hint_segment(key, name_mode, enrich_single_token) {
                    path.push(segment);
                    fill_first_hints(
                        val,
                        value_position,
                        node_ids,
                        first_hints,
                        path,
                        name_mode,
                        enrich_single_token,
                    );
                    path.pop();
                } else {
                    fill_first_hints(
                        val,
                        value_position,
                        node_ids,
                        first_hints,
                        path,
                        name_mode,
                        enrich_single_token,
                    );
                }
            }
        }
        YamlValue::Tagged(tagged) => {
            fill_first_hints(
                &tagged.value,
                value_position,
                node_ids,
                first_hints,
                path,
                name_mode,
                enrich_single_token,
            );
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

fn anchor_hint_from_path(path: &[String]) -> String {
    if path.is_empty() {
        return "root".to_string();
    }
    let take = path.len().min(2);
    let mut parts = path[path.len() - take..].to_vec();
    if parts.len() == 2 {
        if parts[1].starts_with(&parts[0]) {
            parts.remove(0);
        } else if parts[0].starts_with(&parts[1]) {
            parts.remove(1);
        }
    }
    parts.join("_")
}

fn yaml_key_hint_segment(
    key: &serde_yaml::Value,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> Option<String> {
    use serde_yaml::Value as YamlValue;
    let raw = match key {
        YamlValue::String(s) => s.as_str().to_string(),
        YamlValue::Number(n) => n.to_string(),
        YamlValue::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        YamlValue::Null => "null".to_string(),
        YamlValue::Sequence(_) | YamlValue::Mapping(_) | YamlValue::Tagged(_) => return None,
    };
    let normalized =
        normalize_anchor_component_with_enrichment(raw.as_str(), name_mode, enrich_single_token);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn build_anchor_base_name(
    path_hint: Option<&str>,
    fingerprint: &YamlFingerprint,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let kind = match fingerprint {
        YamlFingerprint::Null
        | YamlFingerprint::Bool(_)
        | YamlFingerprint::Number(_)
        | YamlFingerprint::String(_) => None,
        YamlFingerprint::Sequence(_) => Some("list"),
        YamlFingerprint::Mapping(_) => Some("map"),
        YamlFingerprint::Tagged(_, _) => Some("tag"),
    };
    let mut base = path_hint
        .map(|raw| normalize_anchor_component_with_enrichment(raw, name_mode, enrich_single_token))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "root".to_string());
    if let Some(kind_suffix) = kind {
        if !base.ends_with(kind_suffix) {
            base.push('_');
            base.push_str(kind_suffix);
        }
    }
    normalize_anchor_name_with_enrichment(base.as_str(), name_mode, enrich_single_token)
}

fn unique_anchor_name(base: String, used_names: &mut HashSet<String>) -> String {
    if used_names.insert(base.clone()) {
        return base;
    }
    for idx in 2.. {
        let candidate = format!("{base}_{idx}");
        if used_names.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("infinite suffix loop")
}

fn normalize_anchor_name_with_enrichment(
    raw: &str,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let mut out = normalize_anchor_component_with_enrichment(raw, name_mode, enrich_single_token);
    if out.is_empty() {
        out.push_str("anchor");
    }
    if !out.chars().next().map(|ch| ch.is_ascii_alphabetic() || ch == '_').unwrap_or(false) {
        out.insert_str(0, "key_");
    }
    let max_len = match name_mode {
        YamlAnchorNameMode::Friendly => 40,
        YamlAnchorNameMode::StrictFriendly => 28,
    };
    if out.len() > max_len {
        out.truncate(max_len);
        while out.ends_with('_') {
            out.pop();
        }
        if out.is_empty() {
            out.push_str("anchor");
        }
    }
    out
}

#[cfg(test)]
pub(super) fn normalize_anchor_component(raw: &str, name_mode: YamlAnchorNameMode) -> String {
    normalize_anchor_component_with_enrichment(raw, name_mode, true)
}

fn normalize_anchor_component_with_enrichment(
    raw: &str,
    name_mode: YamlAnchorNameMode,
    enrich_single_token: bool,
) -> String {
    let tokens = split_anchor_tokens(raw);
    if tokens.is_empty() {
        return String::new();
    }

    let mut filtered = tokens
        .iter()
        .enumerate()
        .filter(|(idx, token)| *idx == 0 || !is_anchor_stopword(token, name_mode))
        .map(|(_, token)| token.clone())
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        filtered = tokens.to_vec();
    }

    let mut normalized = filtered
        .into_iter()
        .map(|token| canonicalize_anchor_token_readable(token, name_mode))
        .collect::<Vec<_>>();
    normalized = squash_anchor_tokens(normalized);
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) && normalized.len() > 1 {
        let without_single_chars =
            normalized.iter().filter(|token| token.len() > 1).cloned().collect::<Vec<_>>();
        if !without_single_chars.is_empty() {
            normalized = without_single_chars;
        }
    }
    if enrich_single_token
        && matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && normalized.len() == 1
        && should_enrich_strict_single_token(normalized[0].as_str())
    {
        for raw_token in tokens.iter().skip(1) {
            let candidate = canonicalize_anchor_token_readable(raw_token.clone(), name_mode);
            if candidate.is_empty()
                || candidate == normalized[0]
                || candidate.chars().all(|c| c.is_ascii_digit())
                || matches!(
                    candidate.as_str(),
                    "root" | "map" | "list" | "tag" | "item" | "value" | "object"
                )
            {
                continue;
            }
            normalized.push(candidate);
            break;
        }
    }
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) && normalized.len() > 1 {
        let without_single_chars =
            normalized.iter().filter(|token| token.len() > 1).cloned().collect::<Vec<_>>();
        if !without_single_chars.is_empty() {
            normalized = without_single_chars;
        }
    }
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && normalized.len() == 1
        && normalized[0].len() == 1
    {
        normalized[0] = "field".to_string();
    }

    // Keep names compact and readable: first token + most specific tail tokens.
    let max_parts = match name_mode {
        YamlAnchorNameMode::Friendly => 3,
        YamlAnchorNameMode::StrictFriendly => 2,
    };
    let compact = if normalized.len() <= max_parts {
        normalized
    } else {
        match max_parts {
            2 => vec![normalized[0].clone(), normalized[normalized.len() - 1].clone()],
            _ => vec![
                normalized[0].clone(),
                normalized[normalized.len() - 2].clone(),
                normalized[normalized.len() - 1].clone(),
            ],
        }
    };

    compact
        .into_iter()
        .map(|mut token| {
            let max_part_len = match name_mode {
                YamlAnchorNameMode::Friendly => 14,
                YamlAnchorNameMode::StrictFriendly => 12,
            };
            if token.len() > max_part_len {
                token.truncate(max_part_len);
            }
            token
        })
        .collect::<Vec<_>>()
        .join("_")
}

pub(super) fn split_anchor_tokens(raw: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let chars = raw.chars().collect::<Vec<_>>();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }

        // Split CamelCase, digit/alpha boundaries, and acronym tails:
        // `apiVersion` -> `api version`, `HTTPRoute` -> `http route`, `ipv6Addr` -> `ipv 6 addr`.
        let should_split = if idx == 0 {
            false
        } else {
            let prev = chars[idx - 1];
            let next = chars.get(idx + 1).copied();
            prev.is_ascii_alphanumeric()
                && ((prev.is_ascii_lowercase() && ch.is_ascii_uppercase())
                    || (prev.is_ascii_digit() && ch.is_ascii_alphabetic())
                    || (prev.is_ascii_alphabetic() && ch.is_ascii_digit())
                    || (prev.is_ascii_uppercase()
                        && ch.is_ascii_uppercase()
                        && next.map(|n| n.is_ascii_lowercase()).unwrap_or(false)))
        };
        if should_split && !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }

        current.push(ch.to_ascii_lowercase());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn is_anchor_stopword(token: &str, name_mode: YamlAnchorNameMode) -> bool {
    let dicts = anchor_name_dictionaries();
    dicts.stopwords_common.contains(token)
        || (matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
            && dicts.stopwords_strict.contains(token))
}

pub(super) fn canonicalize_anchor_token(token: String, name_mode: YamlAnchorNameMode) -> String {
    let dicts = anchor_name_dictionaries();
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly) {
        if let Some(mapped) = dicts.canonical_strict.get(token.as_str()) {
            return mapped.clone();
        }
    }
    if let Some(mapped) = dicts.canonical_common.get(token.as_str()) {
        return mapped.clone();
    }
    token
}

fn canonicalize_anchor_token_readable(token: String, name_mode: YamlAnchorNameMode) -> String {
    let canonical = canonicalize_anchor_token(token.clone(), name_mode);
    if matches!(name_mode, YamlAnchorNameMode::StrictFriendly)
        && canonical.len() == 1
        && token.len() >= 4
    {
        return token;
    }
    canonical
}

fn should_enrich_strict_single_token(token: &str) -> bool {
    matches!(token, "meta" | "kind" | "spec" | "status" | "data" | "api" | "cfg" | "config")
}

#[derive(Default)]
pub(super) struct AnchorNameDictionaries {
    pub(super) stopwords_common: HashSet<String>,
    pub(super) stopwords_strict: HashSet<String>,
    pub(super) canonical_common: HashMap<String, String>,
    pub(super) canonical_strict: HashMap<String, String>,
}

pub(super) fn anchor_name_dictionaries() -> &'static AnchorNameDictionaries {
    static DICTS: OnceLock<AnchorNameDictionaries> = OnceLock::new();
    DICTS.get_or_init(load_anchor_name_dictionaries)
}

fn load_anchor_name_dictionaries() -> AnchorNameDictionaries {
    let stopwords_common = parse_stopword_dict_zstd(
        include_bytes!("../../assets/yaml_anchor/stopwords_common.txt.zst"),
        "stopwords_common",
    );
    let stopwords_strict = parse_stopword_dict_zstd(
        include_bytes!("../../assets/yaml_anchor/stopwords_strict.txt.zst"),
        "stopwords_strict",
    );
    let canonical_common = parse_canonical_dict_zstd(
        include_bytes!("../../assets/yaml_anchor/canonical_common.tsv.zst"),
        "canonical_common",
    );
    let canonical_strict = parse_canonical_dict_zstd(
        include_bytes!("../../assets/yaml_anchor/canonical_strict.tsv.zst"),
        "canonical_strict",
    );
    AnchorNameDictionaries {
        stopwords_common,
        stopwords_strict,
        canonical_common,
        canonical_strict,
    }
}

fn parse_stopword_dict_zstd(bytes: &[u8], label: &str) -> HashSet<String> {
    decode_zstd_utf8(bytes, label)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.to_ascii_lowercase())
        .collect()
}

fn parse_canonical_dict_zstd(bytes: &[u8], label: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let text = decode_zstd_utf8(bytes, label);
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let Some(from) = parts.next() else {
            continue;
        };
        let Some(to) = parts.next() else {
            continue;
        };
        let from = from.trim().to_ascii_lowercase();
        let to = to.trim().to_ascii_lowercase();
        if !from.is_empty() && !to.is_empty() {
            out.insert(from, to);
        }
    }
    out
}

fn decode_zstd_utf8(bytes: &[u8], label: &str) -> String {
    let decoded = zstd::stream::decode_all(bytes)
        .unwrap_or_else(|e| panic!("decode yaml anchor dictionary `{label}`: {e}"));
    String::from_utf8(decoded)
        .unwrap_or_else(|e| panic!("yaml anchor dictionary `{label}` is not utf-8: {e}"))
}

fn squash_anchor_tokens(tokens: Vec<String>) -> Vec<String> {
    let mut out = Vec::<String>::new();
    for token in tokens {
        if token.is_empty() {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if *last == token {
                continue;
            }
            if token.starts_with(last.as_str()) && token.len() > last.len() + 1 {
                *last = token;
                continue;
            }
        }
        out.push(token);
    }
    out
}

fn fill_first_seen(
    value: &serde_yaml::Value,
    value_position: bool,
    node_ids: &HashMap<usize, u32>,
    first_seen: &mut [usize],
    cursor: &mut usize,
) {
    use serde_yaml::Value as YamlValue;

    if value_position {
        if let Some(id) = node_ids.get(&(value as *const _ as usize)) {
            let idx = *id as usize;
            if first_seen[idx] == usize::MAX {
                first_seen[idx] = *cursor;
            }
            *cursor += 1;
        }
    }

    match value {
        YamlValue::Sequence(items) => {
            for item in items {
                fill_first_seen(item, value_position, node_ids, first_seen, cursor);
            }
        }
        YamlValue::Mapping(map) => {
            for (key, val) in map {
                fill_first_seen(key, false, node_ids, first_seen, cursor);
                fill_first_seen(val, value_position, node_ids, first_seen, cursor);
            }
        }
        YamlValue::Tagged(tagged) => {
            fill_first_seen(&tagged.value, value_position, node_ids, first_seen, cursor);
        }
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {}
    }
}

struct YamlAnchorPlan {
    node_ids: HashMap<usize, u32>,
    fingerprints: Vec<YamlFingerprint>,
    anchor_names: Vec<Option<String>>,
    emitted: Vec<bool>,
    merge_sources: HashMap<u32, u32>,
}

impl YamlAnchorPlan {
    fn new(
        value: &serde_yaml::Value,
        name_mode: YamlAnchorNameMode,
        enrich_single_token: bool,
    ) -> Self {
        YamlAnchorAnalyzer::analyze_document(value, name_mode, enrich_single_token)
    }

    fn anchor_action(&mut self, value: &serde_yaml::Value, value_position: bool) -> AnchorAction {
        if !value_position {
            return AnchorAction::None;
        }

        let Some(id) = self.node_ids.get(&(value as *const _ as usize)).copied() else {
            return AnchorAction::None;
        };
        let idx = id as usize;
        let Some(name) = self.anchor_names.get(idx).and_then(|name| name.as_ref()).cloned() else {
            return AnchorAction::None;
        };
        if self.emitted[idx] {
            AnchorAction::Alias(name)
        } else {
            self.emitted[idx] = true;
            AnchorAction::Define(name)
        }
    }

    fn node_id(&self, value: &serde_yaml::Value) -> Option<u32> {
        self.node_ids.get(&(value as *const _ as usize)).copied()
    }

    fn mapping_merge_base(
        &self,
        map_value: &serde_yaml::Value,
        map: &serde_yaml::Mapping,
        value_position: bool,
    ) -> Option<(u32, String)> {
        if !value_position || mapping_has_plain_merge_key(map) {
            return None;
        }
        let target_id = self.node_id(map_value)?;
        let base_id = *self.merge_sources.get(&target_id)?;
        let base_idx = base_id as usize;
        if !self.emitted.get(base_idx).copied().unwrap_or(false) {
            return None;
        }
        let name = self.anchor_names.get(base_idx).and_then(|name| name.as_ref()).cloned()?;
        Some((base_id, name))
    }

    fn mapping_pair_covered_by_base(
        &self,
        base_id: u32,
        key: &serde_yaml::Value,
        value: &serde_yaml::Value,
    ) -> bool {
        let Some(key_id) = self.node_id(key) else {
            return false;
        };
        let Some(value_id) = self.node_id(value) else {
            return false;
        };
        let Some(base_pairs) = self.fingerprints.get(base_id as usize).and_then(mapping_pairs)
        else {
            return false;
        };
        base_pairs.contains(&(key_id, value_id))
    }
}

enum AnchorAction {
    None,
    Define(String),
    Alias(String),
}

const YAML_INDENT_STEP: usize = 2;

fn emit_yaml_value_standalone(
    value: &serde_yaml::Value,
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    use serde_yaml::Value as YamlValue;

    let action = plan.anchor_action(value, value_position);
    if let AnchorAction::Alias(name) = &action {
        write_indent(out, indent);
        out.push('*');
        out.push_str(name);
        return Ok(());
    }

    match value {
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {
            write_indent(out, indent);
            emit_scalar_with_anchor(value, action, out)?;
        }
        YamlValue::Sequence(items) => {
            if items.is_empty() {
                write_indent(out, indent);
                emit_empty_with_anchor("[]", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                write_indent(out, indent);
                out.push('&');
                out.push_str(&name);
                out.push('\n');
            }
            emit_yaml_sequence_body(items, indent, value_position, out, plan)?;
        }
        YamlValue::Mapping(map) => {
            if map.is_empty() {
                write_indent(out, indent);
                emit_empty_with_anchor("{}", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                write_indent(out, indent);
                out.push('&');
                out.push_str(&name);
                out.push('\n');
            }
            emit_yaml_mapping_body(value, map, indent, value_position, out, plan)?;
        }
        YamlValue::Tagged(_) => {
            write_indent(out, indent);
            emit_scalar_with_anchor(value, action, out)?;
        }
    }
    Ok(())
}

fn emit_yaml_sequence_body(
    items: &[serde_yaml::Value],
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        write_indent(out, indent);
        out.push('-');
        emit_yaml_value_inline(item, indent, value_position, out, plan)?;
    }
    Ok(())
}

fn emit_yaml_mapping_body(
    map_value: &serde_yaml::Value,
    map: &serde_yaml::Mapping,
    indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    let merge_base = plan.mapping_merge_base(map_value, map, value_position);
    let mut wrote_line = false;

    if let Some((_, merge_name)) = &merge_base {
        write_indent(out, indent);
        out.push_str("<<: *");
        out.push_str(merge_name);
        wrote_line = true;
    }

    for (key, value) in map {
        if let Some((base_id, _)) = &merge_base {
            if plan.mapping_pair_covered_by_base(*base_id, key, value) {
                continue;
            }
        }
        if wrote_line {
            out.push('\n');
        }
        write_indent(out, indent);
        out.push_str(&render_yaml_key(key)?);
        out.push(':');
        emit_yaml_value_inline(value, indent, value_position, out, plan)?;
        wrote_line = true;
    }
    Ok(())
}

fn mapping_has_plain_merge_key(map: &serde_yaml::Mapping) -> bool {
    map.keys().any(|key| matches!(key, serde_yaml::Value::String(s) if s == "<<"))
}

fn emit_yaml_value_inline(
    value: &serde_yaml::Value,
    parent_indent: usize,
    value_position: bool,
    out: &mut String,
    plan: &mut YamlAnchorPlan,
) -> Result<(), Error> {
    use serde_yaml::Value as YamlValue;

    let action = plan.anchor_action(value, value_position);
    if let AnchorAction::Alias(name) = &action {
        out.push(' ');
        out.push('*');
        out.push_str(name);
        return Ok(());
    }

    match value {
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) | YamlValue::String(_) => {
            out.push(' ');
            emit_scalar_with_anchor(value, action, out)?;
        }
        YamlValue::Sequence(items) => {
            if items.is_empty() {
                out.push(' ');
                emit_empty_with_anchor("[]", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                out.push(' ');
                out.push('&');
                out.push_str(&name);
            }
            out.push('\n');
            emit_yaml_sequence_body(
                items,
                parent_indent + YAML_INDENT_STEP,
                value_position,
                out,
                plan,
            )?;
        }
        YamlValue::Mapping(map) => {
            if map.is_empty() {
                out.push(' ');
                emit_empty_with_anchor("{}", action, out);
                return Ok(());
            }
            if let AnchorAction::Define(name) = action {
                out.push(' ');
                out.push('&');
                out.push_str(&name);
            }
            out.push('\n');
            emit_yaml_mapping_body(
                value,
                map,
                parent_indent + YAML_INDENT_STEP,
                value_position,
                out,
                plan,
            )?;
        }
        YamlValue::Tagged(_) => {
            out.push(' ');
            emit_scalar_with_anchor(value, action, out)?;
        }
    }
    Ok(())
}

fn emit_empty_with_anchor(token: &str, action: AnchorAction, out: &mut String) {
    match action {
        AnchorAction::None => out.push_str(token),
        AnchorAction::Define(name) => {
            out.push('&');
            out.push_str(&name);
            out.push(' ');
            out.push_str(token);
        }
        AnchorAction::Alias(name) => {
            out.push('*');
            out.push_str(&name);
        }
    }
}

fn emit_scalar_with_anchor(
    value: &serde_yaml::Value,
    action: AnchorAction,
    out: &mut String,
) -> Result<(), Error> {
    if let AnchorAction::Define(name) = action {
        out.push('&');
        out.push_str(&name);
        out.push(' ');
    }
    out.push_str(&render_yaml_scalar(value)?);
    Ok(())
}

fn render_yaml_key(value: &serde_yaml::Value) -> Result<String, Error> {
    use serde_yaml::Value as YamlValue;
    match value {
        YamlValue::String(s) => Ok(render_yaml_string(s)),
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) => render_yaml_scalar(value),
        _ => {
            let mut rendered =
                serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
            while rendered.ends_with('\n') {
                rendered.pop();
            }
            if rendered.contains('\n') {
                return Ok(render_yaml_quoted_string(&rendered.replace('\n', " ")));
            }
            Ok(rendered)
        }
    }
}

fn render_yaml_scalar(value: &serde_yaml::Value) -> Result<String, Error> {
    use serde_yaml::Value as YamlValue;
    match value {
        YamlValue::Null => Ok("null".to_string()),
        YamlValue::Bool(v) => Ok(if *v { "true" } else { "false" }.to_string()),
        YamlValue::Number(v) => Ok(v.to_string()),
        YamlValue::String(s) => Ok(render_yaml_string(s)),
        YamlValue::Tagged(_) => {
            let mut rendered =
                serde_yaml::to_string(value).map_err(|e| Error::OutputYamlEncode(e.to_string()))?;
            while rendered.ends_with('\n') {
                rendered.pop();
            }
            if rendered.is_empty() {
                rendered.push_str("null");
            }
            Ok(rendered)
        }
        YamlValue::Sequence(_) | YamlValue::Mapping(_) => Err(Error::OutputYamlEncode(
            "internal yaml emitter received non-scalar value where scalar was expected".to_string(),
        )),
    }
}

fn render_yaml_string(value: &str) -> String {
    if can_render_plain_yaml_string(value) {
        value.to_string()
    } else {
        render_yaml_quoted_string(value)
    }
}

fn can_render_plain_yaml_string(value: &str) -> bool {
    if value.is_empty() || value.trim() != value {
        return false;
    }
    if value.chars().any(|ch| ch.is_control()) {
        return false;
    }

    let lower = value.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "null"
            | "~"
            | "true"
            | "false"
            | "yes"
            | "no"
            | "on"
            | "off"
            | "y"
            | "n"
            | ".nan"
            | ".inf"
            | "-.inf"
    ) {
        return false;
    }

    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if first.is_ascii_digit() || matches!(first, '-' | '+' | '?' | ':' | '!' | '&' | '*') {
        return false;
    }
    if !is_plain_yaml_char(first) {
        return false;
    }
    chars.all(is_plain_yaml_char)
}

fn is_plain_yaml_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/')
}

fn render_yaml_quoted_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            other if other.is_control() => {
                let _ = write!(out, "\\u{:04X}", other as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

fn write_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push(' ');
    }
}

fn json_to_yaml_value(v: &JsonValue) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Mapping, Value as YamlValue};
    match v {
        JsonValue::Null => Ok(YamlValue::Null),
        JsonValue::Bool(b) => Ok(YamlValue::Bool(*b)),
        JsonValue::Number(n) => number_token_to_yaml_value(&n.to_string()),
        JsonValue::String(s) => Ok(YamlValue::String(s.clone())),
        JsonValue::Array(arr) => {
            let mut seq = Vec::with_capacity(arr.len());
            for item in arr {
                seq.push(json_to_yaml_value(item)?);
            }
            Ok(YamlValue::Sequence(seq))
        }
        JsonValue::Object(obj) => {
            let mut map = Mapping::new();
            for (k, val) in obj {
                map.insert(YamlValue::String(k.clone()), json_to_yaml_value(val)?);
            }
            Ok(YamlValue::Mapping(map))
        }
    }
}

fn native_to_yaml_value(v: &ZqValue) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Mapping as YamlMap, Value as YamlValue};
    match v {
        ZqValue::Null => Ok(YamlValue::Null),
        ZqValue::Bool(b) => Ok(YamlValue::Bool(*b)),
        ZqValue::Number(n) => number_token_to_yaml_value(&n.to_string()),
        ZqValue::String(s) => Ok(YamlValue::String(s.clone())),
        ZqValue::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(native_to_yaml_value(item)?);
            }
            Ok(YamlValue::Sequence(out))
        }
        ZqValue::Object(obj) => {
            let mut map = YamlMap::new();
            for (k, val) in obj {
                map.insert(YamlValue::String(k.clone()), native_to_yaml_value(val)?);
            }
            Ok(YamlValue::Mapping(map))
        }
    }
}

fn number_token_to_yaml_value(token: &str) -> Result<serde_yaml::Value, Error> {
    use serde_yaml::{Number, Value as YamlValue};

    if let Ok(i) = token.parse::<i64>() {
        return Ok(YamlValue::Number(Number::from(i)));
    }
    if let Ok(u) = token.parse::<u64>() {
        return Ok(YamlValue::Number(Number::from(u)));
    }
    if let Ok(f) = token.parse::<f64>() {
        if let Ok(yv) = serde_yaml::to_value(f) {
            return Ok(yv);
        }
    }
    serde_yaml::from_str::<YamlValue>(token).map_err(|e| Error::OutputYamlEncode(e.to_string()))
}
