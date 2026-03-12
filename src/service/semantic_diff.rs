use serde::Serialize;
use std::collections::BTreeSet;
use std::io::{self, Write};

use crate::cli::DiffOutputFormat;

use super::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SemanticDiffKind {
    Added,
    Removed,
    Changed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct SemanticDiff {
    pub(super) kind: SemanticDiffKind,
    pub(super) path: String,
    pub(super) left: Option<zq::NativeValue>,
    pub(super) right: Option<zq::NativeValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(super) struct SemanticDiffSummary {
    pub(super) total: usize,
    pub(super) changed: usize,
    pub(super) added: usize,
    pub(super) removed: usize,
}

impl SemanticDiffSummary {
    pub(super) fn from_diffs(diffs: &[SemanticDiff]) -> Self {
        let mut summary = Self { total: diffs.len(), changed: 0, added: 0, removed: 0 };
        for diff in diffs {
            match diff.kind {
                SemanticDiffKind::Added => summary.added += 1,
                SemanticDiffKind::Removed => summary.removed += 1,
                SemanticDiffKind::Changed => summary.changed += 1,
            }
        }
        summary
    }

    pub(super) fn equal(self) -> bool {
        self.total == 0
    }
}

pub(super) fn collect_semantic_doc_diffs(
    left_docs: &[zq::NativeValue],
    right_docs: &[zq::NativeValue],
) -> Vec<SemanticDiff> {
    let mut diffs = Vec::new();
    if left_docs.len() == 1 && right_docs.len() == 1 {
        collect_semantic_diffs("$", &left_docs[0], &right_docs[0], &mut diffs);
        return diffs;
    }

    let max_len = left_docs.len().max(right_docs.len());
    for idx in 0..max_len {
        let path = format!("$[{idx}]");
        match (left_docs.get(idx), right_docs.get(idx)) {
            (Some(left), Some(right)) => {
                collect_semantic_diffs(path.as_str(), left, right, &mut diffs)
            }
            (Some(left), None) => diffs.push(SemanticDiff {
                kind: SemanticDiffKind::Removed,
                path,
                left: Some(left.clone()),
                right: None,
            }),
            (None, Some(right)) => diffs.push(SemanticDiff {
                kind: SemanticDiffKind::Added,
                path,
                left: None,
                right: Some(right.clone()),
            }),
            (None, None) => {}
        }
    }
    diffs
}

fn collect_semantic_diffs(
    path: &str,
    left: &zq::NativeValue,
    right: &zq::NativeValue,
    out: &mut Vec<SemanticDiff>,
) {
    if left == right {
        return;
    }

    match (left, right) {
        (zq::NativeValue::Object(left_map), zq::NativeValue::Object(right_map)) => {
            let mut keys = BTreeSet::new();
            keys.extend(left_map.keys().cloned());
            keys.extend(right_map.keys().cloned());

            for key in keys {
                let key_path = join_object_path(path, &key);
                match (left_map.get(&key), right_map.get(&key)) {
                    (Some(left_value), Some(right_value)) => {
                        collect_semantic_diffs(key_path.as_str(), left_value, right_value, out)
                    }
                    (Some(left_value), None) => out.push(SemanticDiff {
                        kind: SemanticDiffKind::Removed,
                        path: key_path,
                        left: Some(left_value.clone()),
                        right: None,
                    }),
                    (None, Some(right_value)) => out.push(SemanticDiff {
                        kind: SemanticDiffKind::Added,
                        path: key_path,
                        left: None,
                        right: Some(right_value.clone()),
                    }),
                    (None, None) => {}
                }
            }
        }
        (zq::NativeValue::Array(left_items), zq::NativeValue::Array(right_items)) => {
            let max_len = left_items.len().max(right_items.len());
            for idx in 0..max_len {
                let item_path = format!("{path}[{idx}]");
                match (left_items.get(idx), right_items.get(idx)) {
                    (Some(left_value), Some(right_value)) => {
                        collect_semantic_diffs(item_path.as_str(), left_value, right_value, out)
                    }
                    (Some(left_value), None) => out.push(SemanticDiff {
                        kind: SemanticDiffKind::Removed,
                        path: item_path,
                        left: Some(left_value.clone()),
                        right: None,
                    }),
                    (None, Some(right_value)) => out.push(SemanticDiff {
                        kind: SemanticDiffKind::Added,
                        path: item_path,
                        left: None,
                        right: Some(right_value.clone()),
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => out.push(SemanticDiff {
            kind: SemanticDiffKind::Changed,
            path: path.to_string(),
            left: Some(left.clone()),
            right: Some(right.clone()),
        }),
    }
}

fn join_object_path(base: &str, key: &str) -> String {
    if is_simple_path_key(key) {
        return format!("{base}.{key}");
    }
    let escaped = serde_json::to_string(key).unwrap_or_else(|_| "\"<invalid-key>\"".to_string());
    format!("{base}[{escaped}]")
}

fn is_simple_path_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

pub(super) fn print_semantic_diff_report(
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
    format: DiffOutputFormat,
    compact_json: bool,
    color: bool,
) -> Result<(), Error> {
    let stdout = io::stdout();
    let mut writer = io::BufWriter::new(stdout.lock());
    write_semantic_diff_report(&mut writer, diffs, summary, format, compact_json, color)?;
    writer.flush()?;
    Ok(())
}

pub(super) fn write_semantic_diff_report<W: Write>(
    writer: &mut W,
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
    format: DiffOutputFormat,
    compact_json: bool,
    color: bool,
) -> Result<(), Error> {
    match format {
        DiffOutputFormat::Diff => write_semantic_diff_report_diff(writer, diffs, summary, color),
        DiffOutputFormat::Patch => write_semantic_diff_report_patch(writer, diffs, summary, color),
        DiffOutputFormat::Json => {
            write_semantic_diff_report_json(writer, diffs, summary, compact_json)
        }
        DiffOutputFormat::Jsonl => write_semantic_diff_report_jsonl(writer, diffs, summary),
        DiffOutputFormat::Summary => write_semantic_diff_report_summary(writer, summary),
    }
}

fn write_semantic_diff_report_diff<W: Write>(
    writer: &mut W,
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
    color: bool,
) -> Result<(), Error> {
    if summary.equal() {
        writeln!(writer, "No semantic differences.")?;
        return Ok(());
    }

    writeln!(writer, "Found {} semantic differences:", summary.total)?;
    for diff in diffs {
        match diff.kind {
            SemanticDiffKind::Added => {
                let value = diff.right.as_ref().expect("added diff always has right value");
                write_semantic_diff_line(
                    writer,
                    '+',
                    &diff.path,
                    &render_semantic_diff_value(value)?,
                    None,
                    color,
                )?;
            }
            SemanticDiffKind::Removed => {
                let value = diff.left.as_ref().expect("removed diff always has left value");
                write_semantic_diff_line(
                    writer,
                    '-',
                    &diff.path,
                    &render_semantic_diff_value(value)?,
                    None,
                    color,
                )?;
            }
            SemanticDiffKind::Changed => {
                let left = diff.left.as_ref().expect("changed diff always has left value");
                let right = diff.right.as_ref().expect("changed diff always has right value");
                let left_rendered = render_semantic_diff_value(left)?;
                let right_rendered = render_semantic_diff_value(right)?;
                write_semantic_diff_line(
                    writer,
                    '~',
                    &diff.path,
                    &left_rendered,
                    Some(&right_rendered),
                    color,
                )?;
            }
        }
    }
    writeln!(
        writer,
        "Summary: changed={}, added={}, removed={}",
        summary.changed, summary.added, summary.removed
    )?;
    Ok(())
}

fn write_semantic_diff_report_patch<W: Write>(
    writer: &mut W,
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
    color: bool,
) -> Result<(), Error> {
    if summary.equal() {
        writeln!(writer, "No semantic differences.")?;
        return Ok(());
    }

    write_patch_file_headers(writer, color)?;

    for (idx, diff) in diffs.iter().enumerate() {
        if idx > 0 {
            writeln!(writer)?;
        }
        write_patch_hunk_header(writer, &diff.path, color)?;
        match diff.kind {
            SemanticDiffKind::Added => {
                let right = diff.right.as_ref().expect("added diff always has right value");
                write_patch_value_line(writer, '+', &render_semantic_diff_value(right)?, color)?;
            }
            SemanticDiffKind::Removed => {
                let left = diff.left.as_ref().expect("removed diff always has left value");
                write_patch_value_line(writer, '-', &render_semantic_diff_value(left)?, color)?;
            }
            SemanticDiffKind::Changed => {
                let left = diff.left.as_ref().expect("changed diff always has left value");
                let right = diff.right.as_ref().expect("changed diff always has right value");
                write_patch_value_line(writer, '-', &render_semantic_diff_value(left)?, color)?;
                write_patch_value_line(writer, '+', &render_semantic_diff_value(right)?, color)?;
            }
        }
    }

    writeln!(
        writer,
        "\nSummary: changed={}, added={}, removed={}",
        summary.changed, summary.added, summary.removed
    )?;
    Ok(())
}

fn write_patch_file_headers<W: Write>(writer: &mut W, color: bool) -> Result<(), Error> {
    if !color {
        writeln!(writer, "--- left")?;
        writeln!(writer, "+++ right")?;
        return Ok(());
    }
    let reset = "\x1b[0m";
    writeln!(writer, "\x1b[31m--- left{reset}")?;
    writeln!(writer, "\x1b[32m+++ right{reset}")?;
    Ok(())
}

fn write_patch_hunk_header<W: Write>(writer: &mut W, path: &str, color: bool) -> Result<(), Error> {
    if !color {
        writeln!(writer, "@@ {path} @@")?;
        return Ok(());
    }
    writeln!(writer, "\x1b[36m@@ {path} @@\x1b[0m")?;
    Ok(())
}

fn write_patch_value_line<W: Write>(
    writer: &mut W,
    marker: char,
    value: &str,
    color: bool,
) -> Result<(), Error> {
    if !color {
        writeln!(writer, "{marker}{value}")?;
        return Ok(());
    }
    let style = match marker {
        '+' => "\x1b[32m",
        '-' => "\x1b[31m",
        _ => "\x1b[0m",
    };
    writeln!(writer, "{style}{marker}{value}\x1b[0m")?;
    Ok(())
}

fn render_semantic_diff_value(value: &zq::NativeValue) -> Result<String, Error> {
    serde_json::to_string(value).map_err(|e| Error::Query(format!("encode json: {e}")))
}

fn write_semantic_diff_line<W: Write>(
    writer: &mut W,
    marker: char,
    path: &str,
    left: &str,
    right: Option<&str>,
    color: bool,
) -> Result<(), Error> {
    if !color {
        if let Some(right) = right {
            writeln!(writer, "{marker} {path}: {left} -> {right}")?;
        } else {
            writeln!(writer, "{marker} {path}: {left}")?;
        }
        return Ok(());
    }

    let marker_style = match marker {
        '+' => "\x1b[32m",
        '-' => "\x1b[31m",
        '~' => "\x1b[33m",
        _ => "\x1b[0m",
    };
    let reset = "\x1b[0m";
    let path_style = "\x1b[36m";
    if let Some(right) = right {
        writeln!(
            writer,
            "{marker_style}{marker}{reset} {path_style}{path}{reset}: {left} -> {right}"
        )?;
    } else {
        writeln!(writer, "{marker_style}{marker}{reset} {path_style}{path}{reset}: {left}")?;
    }
    Ok(())
}

fn write_semantic_diff_report_json<W: Write>(
    writer: &mut W,
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
    compact_json: bool,
) -> Result<(), Error> {
    let payload = serde_json::json!({
        "equal": summary.equal(),
        "summary": summary,
        "differences": diffs,
    });
    if compact_json {
        serde_json::to_writer(&mut *writer, &payload)
            .map_err(|e| Error::Query(format!("encode diff json: {e}")))?;
    } else {
        serde_json::to_writer_pretty(&mut *writer, &payload)
            .map_err(|e| Error::Query(format!("encode diff json: {e}")))?;
    }
    writeln!(writer)?;
    Ok(())
}

fn write_semantic_diff_report_jsonl<W: Write>(
    writer: &mut W,
    diffs: &[SemanticDiff],
    summary: SemanticDiffSummary,
) -> Result<(), Error> {
    for diff in diffs {
        let payload = serde_json::json!({
            "type": "diff",
            "kind": diff.kind,
            "path": &diff.path,
            "left": &diff.left,
            "right": &diff.right,
        });
        serde_json::to_writer(&mut *writer, &payload)
            .map_err(|e| Error::Query(format!("encode diff jsonl: {e}")))?;
        writeln!(writer)?;
    }
    let summary_payload = serde_json::json!({
        "type": "summary",
        "equal": summary.equal(),
        "total": summary.total,
        "changed": summary.changed,
        "added": summary.added,
        "removed": summary.removed,
    });
    serde_json::to_writer(&mut *writer, &summary_payload)
        .map_err(|e| Error::Query(format!("encode diff jsonl: {e}")))?;
    writeln!(writer)?;
    Ok(())
}

fn write_semantic_diff_report_summary<W: Write>(
    writer: &mut W,
    summary: SemanticDiffSummary,
) -> Result<(), Error> {
    writeln!(
        writer,
        "equal={} total={} changed={} added={} removed={}",
        summary.equal(),
        summary.total,
        summary.changed,
        summary.added,
        summary.removed
    )?;
    Ok(())
}
