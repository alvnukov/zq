// c-ref: shared C-style numeric semantics for integration tests.
// moved-from: tests/hardcode_guard.rs::jq_dtoi_compat/jq_mod_compat
// moved-from: tests/hardcode_guard_clusters.rs::jq_dtoi_compat/jq_mod_compat

pub(crate) fn dtoi_compat(value: f64) -> i64 {
    if value < i64::MIN as f64 {
        i64::MIN
    } else if -value < i64::MIN as f64 {
        i64::MAX
    } else {
        value as i64
    }
}

pub(crate) fn mod_compat(lhs: f64, rhs: f64) -> Result<f64, &'static str> {
    if lhs.is_nan() || rhs.is_nan() {
        return Ok(f64::NAN);
    }
    let rhs_int = dtoi_compat(rhs);
    if rhs_int == 0 {
        return Err("cannot be divided (remainder) because the divisor is zero");
    }
    if rhs_int == -1 {
        return Ok(0.0);
    }
    Ok((dtoi_compat(lhs) % rhs_int) as f64)
}
