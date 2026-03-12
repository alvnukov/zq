// c-ref: jq container access helpers (jv_aux-style semantics).

use crate::c_compat::{json as c_json, math as c_math, value as c_value};
use crate::value::ZqValue;
use indexmap::IndexMap;
use std::cmp::Ordering;

// moved-from: src/native_engine/vm_core/vm.rs::run_has
pub(crate) fn has_jq(container: ZqValue, key: ZqValue) -> Result<ZqValue, String> {
    let out = match (&container, &key) {
        (ZqValue::Null, _) => ZqValue::Bool(false),
        (ZqValue::Object(map), ZqValue::String(k)) => ZqValue::Bool(map.contains_key(k)),
        (ZqValue::Array(values), ZqValue::Number(n)) => {
            let exists = if let Some(k) = n.as_f64() {
                if k.is_nan() {
                    false
                } else {
                    let idx = c_math::dtoi_compat(k);
                    idx >= 0 && (idx as usize) < values.len()
                }
            } else {
                false
            };
            ZqValue::Bool(exists)
        }
        _ => {
            return Err(format!(
                "Cannot check whether {} has a {} key",
                c_value::type_name_jq(&container),
                c_value::type_name_jq(&key)
            ));
        }
    };
    Ok(out)
}

// moved-from: src/native_engine/vm_core/vm.rs::run_keys
pub(crate) fn keys_jq(input: ZqValue, sorted: bool) -> Result<ZqValue, String> {
    match input {
        ZqValue::Array(arr) => {
            Ok(ZqValue::Array((0..arr.len()).map(|i| ZqValue::from(i as i64)).collect()))
        }
        ZqValue::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            if sorted {
                keys.sort();
            }
            Ok(ZqValue::Array(keys.into_iter().map(ZqValue::String).collect()))
        }
        other => Err(format!(
            "{} ({}) has no keys",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_to_entries
pub(crate) fn to_entries_jq(input: ZqValue) -> Result<ZqValue, String> {
    let out = match input {
        ZqValue::Array(arr) => ZqValue::Array(
            arr.into_iter()
                .enumerate()
                .map(|(idx, value)| {
                    ZqValue::Object(IndexMap::from([
                        ("key".to_string(), ZqValue::from(idx as i64)),
                        ("value".to_string(), value),
                    ]))
                })
                .collect(),
        ),
        ZqValue::Object(map) => ZqValue::Array(
            map.into_iter()
                .map(|(key, value)| {
                    ZqValue::Object(IndexMap::from([
                        ("key".to_string(), ZqValue::String(key)),
                        ("value".to_string(), value),
                    ]))
                })
                .collect(),
        ),
        other => {
            return Err(format!(
                "{} ({}) has no keys",
                c_value::type_name_jq(&other),
                c_value::value_for_error_jq(&other)
            ));
        }
    };
    Ok(out)
}

// moved-from: src/native_engine/vm_core/vm.rs::jq_alt
pub(crate) fn alt_jq(lhs: ZqValue, rhs: ZqValue) -> ZqValue {
    if matches!(lhs, ZqValue::Null | ZqValue::Bool(false)) {
        rhs
    } else {
        lhs
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::object_key_to_string
pub(crate) fn object_key_to_string_jq(value: ZqValue) -> Result<String, String> {
    match value {
        ZqValue::String(s) => Ok(s),
        ZqValue::Number(_) | ZqValue::Bool(_) | ZqValue::Null => c_json::tostring_value_jq(&value),
        other => Err(format!(
            "Cannot use {} ({}) as object key",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::iter_values_like_jq
pub(crate) fn iter_values_like_jq(input: ZqValue) -> Result<Vec<ZqValue>, String> {
    match input {
        ZqValue::Array(values) => Ok(values),
        ZqValue::Object(map) => Ok(map.into_iter().map(|(_, value)| value).collect()),
        other => Err(format!(
            "Cannot iterate over {} ({})",
            c_value::type_name_jq(&other),
            c_value::value_for_error_jq(&other)
        )),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::run_from_entries
pub(crate) fn from_entries_jq(input: ZqValue) -> Result<ZqValue, String> {
    let entries = iter_values_like_jq(input)?;
    let mut out = IndexMap::new();
    for entry in entries {
        let ZqValue::Object(map) = entry else {
            return Err(format!(
                "Cannot index {} with string \"key\"",
                c_value::type_name_jq(&entry)
            ));
        };

        let key_value = alt_jq(
            map.get("key").cloned().unwrap_or(ZqValue::Null),
            alt_jq(
                map.get("Key").cloned().unwrap_or(ZqValue::Null),
                alt_jq(
                    map.get("name").cloned().unwrap_or(ZqValue::Null),
                    map.get("Name").cloned().unwrap_or(ZqValue::Null),
                ),
            ),
        );
        let key = object_key_to_string_jq(key_value)?;
        let value = if map.contains_key("value") {
            map.get("value").cloned().unwrap_or(ZqValue::Null)
        } else {
            map.get("Value").cloned().unwrap_or(ZqValue::Null)
        };
        out.insert(key, value);
    }
    Ok(ZqValue::Object(out))
}

// moved-from: src/native_engine/vm_core/vm.rs::object_merge_recursive
pub(crate) fn object_merge_recursive_jq(
    mut lhs: IndexMap<String, ZqValue>,
    rhs: IndexMap<String, ZqValue>,
) -> IndexMap<String, ZqValue> {
    for (key, rhs_value) in rhs {
        match (lhs.swap_remove(&key), rhs_value) {
            (Some(ZqValue::Object(left_obj)), ZqValue::Object(right_obj)) => {
                lhs.insert(key, ZqValue::Object(object_merge_recursive_jq(left_obj, right_obj)));
            }
            (Some(_), value) | (None, value) => {
                lhs.insert(key, value);
            }
        }
    }
    lhs
}

// moved-from: src/native_engine/vm_core/vm.rs::run_sort
pub(crate) fn sort_jq(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Array(values) = input else {
        return Err(format!(
            "{} ({}) cannot be sorted, as it is not an array",
            c_value::type_name_jq(&input),
            c_value::value_for_error_jq(&input)
        ));
    };

    let mut indexed = values.into_iter().enumerate().collect::<Vec<_>>();
    indexed.sort_by(|(li, lv), (ri, rv)| {
        let ord = c_value::compare_jq(lv, rv);
        if ord == std::cmp::Ordering::Equal {
            li.cmp(ri)
        } else {
            ord
        }
    });
    Ok(ZqValue::Array(indexed.into_iter().map(|(_, value)| value).collect()))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_unique
pub(crate) fn unique_jq(input: ZqValue) -> Result<ZqValue, String> {
    let sorted = sort_jq(input)?;
    let ZqValue::Array(values) = sorted else {
        return Err("internal: sort returned non-array".to_string());
    };

    let mut out = Vec::new();
    for value in values {
        let keep = out.last().is_none_or(|last: &ZqValue| {
            c_value::compare_jq(last, &value) != std::cmp::Ordering::Equal
        });
        if keep {
            out.push(value);
        }
    }
    Ok(ZqValue::Array(out))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_minmax
pub(crate) fn minmax_jq(input: ZqValue, is_min: bool) -> Result<ZqValue, String> {
    let ZqValue::Array(values) = input.clone() else {
        return Err(format!(
            "{} ({}) and {} ({}) cannot be iterated over",
            c_value::type_name_jq(&input),
            c_value::value_for_error_jq(&input),
            c_value::type_name_jq(&input),
            c_value::value_for_error_jq(&input)
        ));
    };

    if values.is_empty() {
        return Ok(ZqValue::Null);
    }

    let mut best = values[0].clone();
    for item in values.into_iter().skip(1) {
        let ord = c_value::compare_jq(&item, &best);
        // jq/src/builtin.c minmax_by():
        // if ((cmp < 0) == (is_min == 1)) select current item.
        let choose_item = (ord == std::cmp::Ordering::Less) == is_min;
        if choose_item {
            best = item;
        }
    }
    Ok(best)
}

#[derive(Debug)]
struct BySortEntry {
    object: ZqValue,
    key: ZqValue,
    index: usize,
}

fn type_error2_jq(lhs: &ZqValue, rhs: &ZqValue, msg: &str) -> String {
    format!(
        "{} ({}) and {} ({}) {}",
        c_value::type_name_jq(lhs),
        c_value::value_for_error_jq(lhs),
        c_value::type_name_jq(rhs),
        c_value::value_for_error_jq(rhs),
        msg
    )
}

fn collect_by_entries_jq(values: ZqValue, keys: ZqValue) -> Result<Vec<BySortEntry>, String> {
    let values_len = match &values {
        ZqValue::Array(values) => Some(values.len()),
        _ => None,
    };
    let keys_len = match &keys {
        ZqValue::Array(keys) => Some(keys.len()),
        _ => None,
    };
    if values_len.is_none() || keys_len.is_none() || values_len != keys_len {
        return Err(type_error2_jq(
            &values,
            &keys,
            "cannot be sorted, as they are not both arrays",
        ));
    }
    let (ZqValue::Array(values), ZqValue::Array(keys)) = (values, keys) else {
        unreachable!("checked above");
    };
    let mut entries = values
        .into_iter()
        .zip(keys)
        .enumerate()
        .map(|(index, (object, key))| BySortEntry { object, key, index })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        let ord = c_value::compare_jq(&left.key, &right.key);
        if ord == Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ord
        }
    });
    Ok(entries)
}

// moved-from: src/native_engine/vm_core/vm.rs::run_sort_by_impl
pub(crate) fn sort_by_jq(values: ZqValue, keys: ZqValue) -> Result<ZqValue, String> {
    let entries = collect_by_entries_jq(values, keys)?;
    Ok(ZqValue::Array(entries.into_iter().map(|entry| entry.object).collect()))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_group_by_impl
pub(crate) fn group_by_jq(values: ZqValue, keys: ZqValue) -> Result<ZqValue, String> {
    let mut entries = collect_by_entries_jq(values, keys)?.into_iter();
    let Some(first) = entries.next() else {
        return Ok(ZqValue::Array(Vec::new()));
    };

    let mut out_groups = Vec::new();
    let mut current_key = first.key;
    let mut current_group = vec![first.object];
    for entry in entries {
        if c_value::compare_jq(&current_key, &entry.key) == Ordering::Equal {
            current_group.push(entry.object);
        } else {
            out_groups.push(ZqValue::Array(current_group));
            current_key = entry.key;
            current_group = vec![entry.object];
        }
    }
    out_groups.push(ZqValue::Array(current_group));
    Ok(ZqValue::Array(out_groups))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_unique_by_impl
pub(crate) fn unique_by_jq(values: ZqValue, keys: ZqValue) -> Result<ZqValue, String> {
    let entries = collect_by_entries_jq(values, keys)?;
    let mut out = Vec::new();
    let mut current_key: Option<ZqValue> = None;
    for entry in entries {
        if current_key
            .as_ref()
            .is_some_and(|key| c_value::compare_jq(key, &entry.key) == Ordering::Equal)
        {
            continue;
        }
        current_key = Some(entry.key);
        out.push(entry.object);
    }
    Ok(ZqValue::Array(out))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_minmax_by_impl
pub(crate) fn minmax_by_jq(
    values: ZqValue,
    keys: ZqValue,
    is_min: bool,
) -> Result<ZqValue, String> {
    let values = match values {
        ZqValue::Array(values) => values,
        other => return Err(type_error2_jq(&other, &keys, "cannot be iterated over")),
    };
    let keys = match keys {
        ZqValue::Array(keys) => keys,
        other => {
            return Err(type_error2_jq(&ZqValue::Array(values), &other, "cannot be iterated over"));
        }
    };
    if values.len() != keys.len() {
        return Err(type_error2_jq(
            &ZqValue::Array(values),
            &ZqValue::Array(keys),
            "have wrong length",
        ));
    }
    if values.is_empty() {
        return Ok(ZqValue::Null);
    }

    let mut values_iter = values.into_iter();
    let mut keys_iter = keys.into_iter();
    let mut best_value = values_iter.next().expect("non-empty checked above");
    let mut best_key = keys_iter.next().expect("non-empty checked above");
    for (value, key) in values_iter.zip(keys_iter) {
        let cmp = c_value::compare_jq(&key, &best_key);
        if (cmp == Ordering::Less) == is_min {
            best_key = key;
            best_value = value;
        }
    }
    Ok(best_value)
}

// moved-from: src/native_engine/vm_core/vm.rs::run_bsearch
pub(crate) fn bsearch_jq(input: ZqValue, target: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Array(values) = input else {
        return Err(format!(
            "{} ({}) cannot be searched from",
            c_value::type_name_jq(&input),
            c_value::value_for_error_jq(&input)
        ));
    };

    let mut start = 0usize;
    let mut end = values.len();
    while start < end {
        let mid = start + (end - start) / 2;
        let cmp = c_value::compare_jq(&target, &values[mid]);
        if cmp == Ordering::Equal {
            return Ok(ZqValue::from(mid as i64));
        }
        if cmp == Ordering::Less {
            end = mid;
        } else {
            start = mid + 1;
        }
    }

    Ok(ZqValue::from((-1isize - start as isize) as i64))
}

// moved-from: src/native_engine/vm_core/vm.rs::run_contains
pub(crate) fn contains_jq(haystack: ZqValue, needle: ZqValue) -> Result<ZqValue, String> {
    if c_value::type_name_jq(&haystack) != c_value::type_name_jq(&needle) {
        return Err(format!(
            "{} ({}) and {} ({}) cannot have their containment checked",
            c_value::type_name_jq(&haystack),
            c_value::value_for_error_jq(&haystack),
            c_value::type_name_jq(&needle),
            c_value::value_for_error_jq(&needle)
        ));
    }
    Ok(ZqValue::Bool(value_contains_jq(&haystack, &needle)))
}

// moved-from: src/native_engine/vm_core/vm.rs::value_contains
pub(crate) fn value_contains_jq(haystack: &ZqValue, needle: &ZqValue) -> bool {
    match (haystack, needle) {
        (ZqValue::Object(h), ZqValue::Object(n)) => {
            n.iter().all(|(k, nv)| h.get(k).is_some_and(|hv| value_contains_jq(hv, nv)))
        }
        (ZqValue::Array(h), ZqValue::Array(n)) => {
            n.iter().all(|nv| h.iter().any(|hv| value_contains_jq(hv, nv)))
        }
        (ZqValue::String(h), ZqValue::String(n)) => n.is_empty() || h.contains(n),
        _ => c_value::compare_jq(haystack, needle) == Ordering::Equal,
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::indices_array
pub(crate) fn indices_array_jq(values: Vec<ZqValue>, pattern: Vec<ZqValue>) -> ZqValue {
    let mut result = Vec::new();
    for ai in 0..values.len() {
        let mut idx: Option<usize> = None;
        for (bi, belem) in pattern.iter().enumerate() {
            let Some(candidate) = values.get(ai + bi) else {
                idx = None;
                continue;
            };
            if candidate != belem {
                idx = None;
            } else if bi == 0 && idx.is_none() {
                idx = Some(ai);
            }
        }
        if let Some(i) = idx {
            result.push(ZqValue::from(i as i64));
        }
    }
    ZqValue::Array(result)
}

// moved-from: src/native_engine/vm_core/vm.rs::indices_string
pub(crate) fn indices_string_jq(text: String, pattern: String) -> ZqValue {
    if pattern.is_empty() {
        return ZqValue::Array(Vec::new());
    }

    let haystack = text.as_bytes();
    let needle = pattern.as_bytes();
    let mut out = Vec::new();

    let mut p = 0usize;
    let mut lp = 0usize;
    let mut codepoint_index = 0i64;

    while let Some(found) = find_subslice_jq(&haystack[p..], needle) {
        let abs = p + found;
        while lp < abs {
            let Some(ch) = text[lp..].chars().next() else {
                break;
            };
            lp += ch.len_utf8();
            codepoint_index += 1;
        }
        out.push(ZqValue::from(codepoint_index));
        p = abs + 1;
    }

    ZqValue::Array(out)
}

fn find_subslice_jq(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_jq_handles_object_array_and_null() {
        let mut map = indexmap::IndexMap::new();
        map.insert("k".to_string(), ZqValue::from(1));
        let obj = ZqValue::Object(map);
        assert_eq!(
            has_jq(obj, ZqValue::String("k".to_string())).expect("object has").into_json(),
            serde_json::json!(true)
        );

        let arr = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)]);
        assert_eq!(
            has_jq(arr, ZqValue::from(1)).expect("array has").into_json(),
            serde_json::json!(true)
        );

        assert_eq!(
            has_jq(ZqValue::Null, ZqValue::String("x".to_string())).expect("null has").into_json(),
            serde_json::json!(false)
        );
    }

    #[test]
    fn has_jq_rejects_invalid_type_pairs() {
        let err = has_jq(ZqValue::from(1), ZqValue::from(0)).expect_err("must fail");
        assert_eq!(err, "Cannot check whether number has a number key");
    }

    #[test]
    fn keys_and_to_entries_follow_jq_shapes() {
        let keys = keys_jq(
            ZqValue::Object(IndexMap::from([
                ("b".to_string(), ZqValue::from(1)),
                ("a".to_string(), ZqValue::from(2)),
            ])),
            true,
        )
        .expect("keys");
        assert_eq!(keys.into_json(), serde_json::json!(["a", "b"]));

        let entries = to_entries_jq(ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(20)]))
            .expect("entries");
        assert_eq!(
            entries.into_json(),
            serde_json::json!([{"key": 0, "value": 10}, {"key": 1, "value": 20}])
        );
    }

    #[test]
    fn object_key_and_from_entries_follow_jq_shapes() {
        let key = object_key_to_string_jq(ZqValue::from(1)).expect("numeric key");
        assert_eq!(key, "1".to_string());

        let out = from_entries_jq(ZqValue::Array(vec![ZqValue::Object(IndexMap::from([
            ("key".to_string(), ZqValue::String("a".to_string())),
            ("value".to_string(), ZqValue::from(42)),
        ]))]))
        .expect("from entries");
        assert_eq!(out.into_json(), serde_json::json!({"a": 42}));
    }

    #[test]
    fn object_merge_recursive_jq_merges_nested_objects() {
        let left = IndexMap::from([(
            "a".to_string(),
            ZqValue::Object(IndexMap::from([("x".to_string(), ZqValue::from(1))])),
        )]);
        let right = IndexMap::from([(
            "a".to_string(),
            ZqValue::Object(IndexMap::from([("y".to_string(), ZqValue::from(2))])),
        )]);
        let merged = object_merge_recursive_jq(left, right);
        assert_eq!(ZqValue::Object(merged).into_json(), serde_json::json!({"a": {"x": 1, "y": 2}}));
    }

    #[test]
    fn sort_unique_minmax_follow_jq_shapes() {
        let sorted =
            sort_jq(ZqValue::Array(vec![ZqValue::from(2), ZqValue::from(1)])).expect("sort");
        assert_eq!(sorted.into_json(), serde_json::json!([1, 2]));

        let unique =
            unique_jq(ZqValue::Array(vec![ZqValue::from(2), ZqValue::from(1), ZqValue::from(2)]))
                .expect("unique");
        assert_eq!(unique.into_json(), serde_json::json!([1, 2]));

        let min = minmax_jq(
            ZqValue::Array(vec![ZqValue::from(3), ZqValue::from(1), ZqValue::from(2)]),
            true,
        )
        .expect("min");
        assert_eq!(min.into_json(), serde_json::json!(1));
    }

    #[test]
    fn by_family_helpers_follow_jq_shapes() {
        let values = ZqValue::Array(vec![
            ZqValue::String("b".to_string()),
            ZqValue::String("a".to_string()),
            ZqValue::String("c".to_string()),
        ]);
        let keys = ZqValue::Array(vec![ZqValue::from(2), ZqValue::from(1), ZqValue::from(3)]);

        let sorted = sort_by_jq(values.clone(), keys.clone()).expect("sort_by");
        assert_eq!(sorted.into_json(), serde_json::json!(["a", "b", "c"]));

        let grouped = group_by_jq(values.clone(), keys.clone()).expect("group_by");
        assert_eq!(grouped.into_json(), serde_json::json!([["a"], ["b"], ["c"]]));

        let unique = unique_by_jq(
            ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(11), ZqValue::from(20)]),
            ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(1), ZqValue::from(2)]),
        )
        .expect("unique_by");
        assert_eq!(unique.into_json(), serde_json::json!([10, 20]));

        let min = minmax_by_jq(
            ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(20)]),
            ZqValue::Array(vec![ZqValue::from(2), ZqValue::from(1)]),
            true,
        )
        .expect("min_by");
        assert_eq!(min.into_json(), serde_json::json!(20));
    }

    #[test]
    fn bsearch_jq_matches_found_and_insertion_results() {
        let found = bsearch_jq(
            ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(3), ZqValue::from(5)]),
            ZqValue::from(3),
        )
        .expect("found");
        assert_eq!(found.into_json(), serde_json::json!(1));

        let missing = bsearch_jq(
            ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(3), ZqValue::from(5)]),
            ZqValue::from(4),
        )
        .expect("missing");
        assert_eq!(missing.into_json(), serde_json::json!(-3));
    }

    #[test]
    fn contains_jq_matches_object_array_and_string_cases() {
        let obj_contains = contains_jq(
            ZqValue::Object(IndexMap::from([
                ("a".to_string(), ZqValue::from(1)),
                ("b".to_string(), ZqValue::from(2)),
            ])),
            ZqValue::Object(IndexMap::from([("a".to_string(), ZqValue::from(1))])),
        )
        .expect("contains object");
        assert_eq!(obj_contains.into_json(), serde_json::json!(true));

        let str_contains =
            contains_jq(ZqValue::String("abc".to_string()), ZqValue::String("b".to_string()))
                .expect("contains string");
        assert_eq!(str_contains.into_json(), serde_json::json!(true));
    }

    #[test]
    fn indices_helpers_match_array_and_string_shapes() {
        let idx = indices_array_jq(
            vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(1)],
            vec![ZqValue::from(1)],
        );
        assert_eq!(idx.into_json(), serde_json::json!([0, 2]));

        let sidx = indices_string_jq("ababa".to_string(), "ba".to_string());
        assert_eq!(sidx.into_json(), serde_json::json!([1, 3]));
    }
}
