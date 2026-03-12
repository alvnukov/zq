// c-ref: jq value type/introspection helpers used in error text formatting.

use crate::c_compat::math as c_math;
use crate::c_compat::string as c_string;
use crate::value::ZqValue;
use std::cmp::Ordering;

// moved-from: src/native_engine/vm_core/vm.rs::type_name
pub(crate) fn type_name_jq(value: &ZqValue) -> &'static str {
    value.jq_type_name()
}

// moved-from: src/native_engine/vm_core/vm.rs::value_for_error
pub(crate) fn value_for_error_jq(value: &ZqValue) -> String {
    match value {
        ZqValue::String(_) => {
            let dumped = serde_json::to_string(&value.clone().into_json())
                .unwrap_or_else(|_| "<invalid>".to_string());
            if dumped.len() > 24 {
                c_string::dump_value_string_trunc_modern(value, 30)
            } else {
                c_string::dump_value_string_trunc_legacy(value, 15)
            }
        }
        _ => c_string::dump_value_string_trunc_modern(value, 30),
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::jq_cmp
pub(crate) fn compare_jq(lhs: &ZqValue, rhs: &ZqValue) -> Ordering {
    let lrank = kind_rank_jq(lhs);
    let rrank = kind_rank_jq(rhs);
    if lrank != rrank {
        return lrank.cmp(&rrank);
    }

    match (lhs, rhs) {
        (ZqValue::Null, ZqValue::Null) => Ordering::Equal,
        (ZqValue::Bool(_), ZqValue::Bool(_)) => Ordering::Equal,
        (ZqValue::Number(a), ZqValue::Number(b)) => c_math::compare_json_numbers_like_jq(a, b),
        (ZqValue::String(a), ZqValue::String(b)) => a.cmp(b),
        (ZqValue::Array(a), ZqValue::Array(b)) => {
            for (la, lb) in a.iter().zip(b.iter()) {
                let ord = compare_jq(la, lb);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.len().cmp(&b.len())
        }
        (ZqValue::Object(a), ZqValue::Object(b)) => {
            let mut akeys = a.keys().cloned().collect::<Vec<_>>();
            let mut bkeys = b.keys().cloned().collect::<Vec<_>>();
            akeys.sort();
            bkeys.sort();
            let key_ord = akeys.cmp(&bkeys);
            if key_ord != Ordering::Equal {
                return key_ord;
            }
            for k in akeys {
                let ord = compare_jq(
                    a.get(&k).expect("key from object A"),
                    b.get(&k).expect("key from object B"),
                );
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        }
        _ => Ordering::Equal,
    }
}

// moved-from: src/native_engine/vm_core/vm.rs::jq_kind_rank
pub(crate) fn kind_rank_jq(value: &ZqValue) -> i32 {
    match value {
        ZqValue::Null => 1,
        ZqValue::Bool(false) => 2,
        ZqValue::Bool(true) => 3,
        ZqValue::Number(_) => 4,
        ZqValue::String(_) => 5,
        ZqValue::Array(_) => 6,
        ZqValue::Object(_) => 7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_name_jq_matches_value_kinds() {
        assert_eq!(type_name_jq(&ZqValue::Null), "null");
        assert_eq!(type_name_jq(&ZqValue::from(1)), "number");
        assert_eq!(type_name_jq(&ZqValue::String("x".to_string())), "string");
    }

    #[test]
    fn value_for_error_jq_uses_string_truncation_shapes() {
        let short = ZqValue::String("abc".to_string());
        assert_eq!(value_for_error_jq(&short), "\"abc\"".to_string());

        let long = ZqValue::String("abcdefghijklmnopqrstuvwxyz0123456789".to_string());
        let printed = value_for_error_jq(&long);
        assert!(printed.contains("..."));
    }

    #[test]
    fn compare_jq_orders_scalars_and_arrays() {
        assert_eq!(compare_jq(&ZqValue::Null, &ZqValue::Bool(false)), Ordering::Less);
        assert_eq!(compare_jq(&ZqValue::from(1), &ZqValue::from(2)), Ordering::Less);
        assert_eq!(
            compare_jq(
                &ZqValue::Array(vec![ZqValue::from(1)]),
                &ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2)])
            ),
            Ordering::Less
        );
    }
}
