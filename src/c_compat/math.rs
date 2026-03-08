// c-ref: C numeric compatibility helpers (math.h + libm/libc family).
use crate::value::ZqValue;
use std::cmp::Ordering;

fn dtoi_saturating(value: f64) -> i64 {
    if value >= i64::MAX as f64 {
        i64::MAX
    } else if value <= i64::MIN as f64 {
        i64::MIN
    } else {
        value as i64
    }
}

fn i32_from_f64_saturating(value: f64) -> i32 {
    dtoi_saturating(value).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

// c-ref: C cast-style truncation with saturation to signed 64-bit range.
pub(crate) fn dtoi_compat(value: f64) -> i64 {
    dtoi_saturating(value)
}

// c-ref: remainder semantics for integer modulo after C-style truncation.
pub(crate) fn mod_compat(lhs: f64, rhs: f64) -> Result<f64, &'static str> {
    if lhs.is_nan() || rhs.is_nan() {
        return Ok(f64::NAN);
    }
    let rhs_i = dtoi_saturating(rhs);
    if rhs_i == 0 {
        return Err("cannot be divided (remainder) because the divisor is zero");
    }
    if rhs_i == -1 {
        return Ok(0.0);
    }
    Ok((dtoi_saturating(lhs) % rhs_i) as f64)
}

// c-ref: math.h ldexp(double, int)
// moved-from: src/native_engine/vm_core/vm.rs::ldexp_compat
pub(crate) fn ldexp_compat(a: f64, b: f64) -> f64 {
    a * 2.0_f64.powi(i32_from_f64_saturating(b))
}

// c-ref: math.h fdim(double, double)
// moved-from: src/native_engine/vm_core/vm.rs::libm_fdim
pub(crate) fn fdim_compat(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    if a > b {
        a - b
    } else {
        0.0
    }
}

// c-ref: math.h fmax(double, double)
// moved-from: src/native_engine/vm_core/vm.rs::libm_fmax
pub(crate) fn fmax_compat(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    a.max(b)
}

// c-ref: math.h fmin(double, double)
// moved-from: src/native_engine/vm_core/vm.rs::libm_fmin
pub(crate) fn fmin_compat(a: f64, b: f64) -> f64 {
    if a.is_nan() {
        return b;
    }
    if b.is_nan() {
        return a;
    }
    a.min(b)
}

// c-ref: IEEE remainder (jq uses round-ties-even quotient reduction).
// moved-from: src/native_engine/vm_core/vm.rs::ieee_remainder
pub(crate) fn remainder_compat(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() || b == 0.0 || a.is_infinite() {
        return f64::NAN;
    }
    if b.is_infinite() {
        return a;
    }
    a - (a / b).round_ties_even() * b
}

// c-ref: math.h scalb/scalbln family.
// moved-from: src/native_engine/vm_core/vm.rs::run_math_binary
pub(crate) fn scalb_compat(a: f64, b: f64) -> f64 {
    a * 2.0_f64.powf(b)
}

// c-ref: scalbln exponent cast follows jq C-style dtoi truncation.
// moved-from: src/native_engine/vm_core/vm.rs::run_math_binary
pub(crate) fn scalbln_compat(a: f64, b: f64) -> f64 {
    a * 2.0_f64.powf(dtoi_compat(b) as f64)
}

// c-ref: math.h nextafter(double, double)
// moved-from: src/native_engine/vm_core/vm.rs::nextafter_compat
pub(crate) fn nextafter_compat(from: f64, toward: f64) -> f64 {
    if from.is_nan() || toward.is_nan() {
        return f64::NAN;
    }
    if from == toward {
        return toward;
    }
    if from == 0.0 {
        let min = f64::from_bits(1);
        return if toward.is_sign_negative() { -min } else { min };
    }

    let mut bits = from.to_bits();
    if (toward > from) == (from > 0.0) {
        bits = bits.wrapping_add(1);
    } else {
        bits = bits.wrapping_sub(1);
    }
    f64::from_bits(bits)
}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "jn"]
    fn c_jn(n: libc::c_int, x: libc::c_double) -> libc::c_double;
    #[link_name = "yn"]
    fn c_yn(n: libc::c_int, x: libc::c_double) -> libc::c_double;
}

// c-ref: libm jn(int, double)
// moved-from: src/native_engine/vm_core/vm.rs::run_jn
#[cfg(unix)]
pub(crate) fn jn_compat(order: f64, x: f64) -> Result<f64, String> {
    Ok(unsafe { c_jn(i32_from_f64_saturating(order), x) })
}

#[cfg(not(unix))]
pub(crate) fn jn_compat(_order: f64, _x: f64) -> Result<f64, String> {
    Err("Error: jn/2 not found at build time".to_string())
}

// c-ref: libm yn(int, double)
// moved-from: src/native_engine/vm_core/vm.rs::run_yn
#[cfg(unix)]
pub(crate) fn yn_compat(order: f64, x: f64) -> Result<f64, String> {
    Ok(unsafe { c_yn(i32_from_f64_saturating(order), x) })
}

#[cfg(not(unix))]
pub(crate) fn yn_compat(_order: f64, _x: f64) -> Result<f64, String> {
    Err("Error: yn/2 not found at build time".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedNumberText {
    pub(crate) sign: i8,
    pub(crate) digits: String,
    pub(crate) exponent: i64,
}

impl ParsedNumberText {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.sign != other.sign {
            return self.sign.cmp(&other.sign);
        }
        if self.sign == 0 {
            return Ordering::Equal;
        }
        let magnitude = compare_number_magnitude(self, other);
        if self.sign < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

pub(crate) fn plain_integer_digit_count(raw: &str) -> Option<usize> {
    let trimmed = raw.trim();
    let unsigned = trimmed
        .strip_prefix('-')
        .or_else(|| trimmed.strip_prefix('+'))
        .unwrap_or(trimmed);
    (!unsigned.is_empty() && unsigned.chars().all(|ch| ch.is_ascii_digit()))
        .then_some(unsigned.len())
}

// Mirrors jq decnum ordering for large exponents that do not fit in f64.
pub(crate) fn compare_number_texts(left: &str, right: &str) -> Ordering {
    let left = parse_number_text(left);
    let right = parse_number_text(right);
    match (left, right) {
        (Some(lhs), Some(rhs)) => lhs.cmp(&rhs),
        _ => Ordering::Equal,
    }
}

// jq-port: runtime numeric comparison with f64-first semantics and decnum
// text fallback for very large plain integers.
pub(crate) fn compare_json_numbers_like_jq(
    a: &serde_json::Number,
    b: &serde_json::Number,
) -> Ordering {
    let left_raw = a.to_string();
    let right_raw = b.to_string();
    if let (Some(left_digits), Some(right_digits)) = (
        plain_integer_digit_count(&left_raw),
        plain_integer_digit_count(&right_raw),
    ) {
        if left_digits > 20 || right_digits > 20 {
            return compare_number_texts(&left_raw, &right_raw);
        }
    }

    match (jq_number_to_f64_lossy(a), jq_number_to_f64_lossy(b)) {
        (Some(af), Some(bf)) => af.partial_cmp(&bf).unwrap_or(Ordering::Equal),
        _ => compare_number_texts(&left_raw, &right_raw),
    }
}

pub(crate) fn parse_number_text(source: &str) -> Option<ParsedNumberText> {
    let mut text = source.trim();
    let mut sign = 1i8;
    if let Some(rest) = text.strip_prefix('-') {
        sign = -1;
        text = rest;
    } else if let Some(rest) = text.strip_prefix('+') {
        text = rest;
    }

    let (mantissa, exp_part) = if let Some((m, e)) = text.split_once(['e', 'E']) {
        (m, e)
    } else {
        (text, "0")
    };
    let mut exponent = exp_part.parse::<i64>().ok()?;

    let mut digits = String::with_capacity(mantissa.len());
    let mut frac_len = 0i64;
    let mut seen_dot = false;
    for ch in mantissa.chars() {
        if ch == '.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !ch.is_ascii_digit() {
            return None;
        }
        if seen_dot {
            frac_len += 1;
        }
        digits.push(ch);
    }
    if digits.is_empty() {
        return None;
    }
    exponent -= frac_len;
    let trimmed = digits.trim_start_matches('0').to_string();
    if trimmed.is_empty() {
        return Some(ParsedNumberText {
            sign: 0,
            digits: "0".to_string(),
            exponent: 0,
        });
    }
    Some(ParsedNumberText {
        sign,
        digits: trimmed,
        exponent,
    })
}

fn compare_number_magnitude(left: &ParsedNumberText, right: &ParsedNumberText) -> Ordering {
    let left_mag = left.digits.len() as i64 + left.exponent;
    let right_mag = right.digits.len() as i64 + right.exponent;
    if left_mag != right_mag {
        return left_mag.cmp(&right_mag);
    }

    let mut idx = 0usize;
    let limit = left
        .digits
        .len()
        .max(right.digits.len())
        .saturating_add((left.exponent.max(right.exponent)).max(0) as usize);
    while idx < limit {
        let ld = digit_at(left, idx);
        let rd = digit_at(right, idx);
        if ld != rd {
            return ld.cmp(&rd);
        }
        idx += 1;
    }
    Ordering::Equal
}

fn digit_at(value: &ParsedNumberText, idx: usize) -> u8 {
    let int_len = (value.digits.len() as i64 + value.exponent).max(0) as usize;
    if idx >= int_len {
        return 0;
    }
    if idx < value.digits.len() {
        return value.digits.as_bytes()[idx] - b'0';
    }
    0
}

// jq-port: jq/src/jv_aux.c:parse_slice()
pub(crate) fn slice_bounds_from_f64_like_jq(
    len: usize,
    start: Option<f64>,
    end: Option<f64>,
) -> (usize, usize) {
    let lenf = len as f64;

    let mut dstart = start.unwrap_or(0.0);
    if dstart.is_nan() {
        dstart = 0.0;
    }
    if dstart < 0.0 {
        dstart += lenf;
    }
    if dstart < 0.0 {
        dstart = 0.0;
    }
    if dstart > lenf {
        dstart = lenf;
    }
    let mut start_i = if dstart > i32::MAX as f64 {
        i32::MAX as i64
    } else {
        dstart as i64
    };
    if start_i < 0 {
        start_i = 0;
    }
    if start_i as usize > len {
        start_i = len as i64;
    }

    let mut dend = end.unwrap_or(lenf);
    if dend.is_nan() {
        dend = lenf;
    }
    if dend < 0.0 {
        dend += lenf;
    }
    if dend < 0.0 {
        dend = start_i as f64;
    }
    let mut end_i = if dend > i32::MAX as f64 {
        i32::MAX as i64
    } else {
        dend as i64
    };
    if end_i > len as i64 {
        end_i = len as i64;
    }
    if end_i < len as i64 && (end_i as f64) < dend {
        end_i += 1;
    }
    if end_i < start_i {
        end_i = start_i;
    }

    (start_i as usize, end_i as usize)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpecialNumber {
    Nan,
    PosInf,
    NegInf,
}

pub(crate) fn classify_special_number(n: &serde_json::Number) -> Option<SpecialNumber> {
    let raw = n.to_string();
    let trimmed = raw.trim();
    let (sign, unsigned) = if let Some(rest) = trimmed.strip_prefix('-') {
        (-1, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (1, rest)
    } else {
        (1, trimmed)
    };
    let lower = unsigned.to_ascii_lowercase();
    if lower == "nan" {
        return Some(SpecialNumber::Nan);
    }
    if lower == "inf" || lower == "infinity" {
        return Some(if sign < 0 {
            SpecialNumber::NegInf
        } else {
            SpecialNumber::PosInf
        });
    }
    None
}

pub(crate) fn parse_jq_non_finite_number(text: &str) -> Result<Option<serde_json::Number>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let (sign, unsigned) = if let Some(rest) = trimmed.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        ("", rest)
    } else {
        ("", trimmed)
    };
    let lower = unsigned.to_ascii_lowercase();
    let normalized = if lower == "nan" {
        "nan".to_string()
    } else if let Some(suffix) = unsigned.strip_prefix("nan") {
        // jq 1.7 accepts lowercase nan payload suffixes made of decimal digits
        // (e.g. nan1234), while preserving NaN1 as invalid.
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            "nan".to_string()
        } else {
            return Err(format!(
                "Invalid numeric literal at EOF at line 1, column {} (while parsing '{}')",
                trimmed.chars().count(),
                trimmed
            ));
        }
    } else if lower == "inf" || lower == "infinity" {
        format!("{sign}inf")
    } else if lower.starts_with("nan")
        || lower.starts_with("inf")
        || lower.starts_with("infinity")
        || lower.starts_with("infinite")
    {
        return Err(format!(
            "Invalid numeric literal at EOF at line 1, column {} (while parsing '{}')",
            trimmed.chars().count(),
            trimmed
        ));
    } else {
        return Ok(None);
    };
    Ok(Some(serde_json::Number::from_string_unchecked(normalized)))
}

// moved-from: src/native_engine/vm_core/vm.rs::parse_jq_number
pub(crate) fn parse_finite_number_literal_jq(text: &str) -> Option<f64> {
    let parsed = text.parse::<f64>().ok()?;
    parsed.is_finite().then_some(parsed)
}

pub(crate) fn normalize_number_for_tojson(number: serde_json::Number) -> serde_json::Number {
    let raw = number.to_string();
    if let Some(expanded) = expand_negative_exponent_literal(&raw) {
        serde_json::Number::from_string_unchecked(expanded)
    } else {
        number
    }
}

fn expand_negative_exponent_literal(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let (sign, unsigned) = if let Some(rest) = raw.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        ("", rest)
    } else {
        ("", raw)
    };
    let (mantissa, exp_raw) = unsigned.split_once(['e', 'E'])?;
    let exponent = exp_raw.parse::<i64>().ok()?;
    if exponent >= 0 {
        return None;
    }

    let (int_part, frac_part) = if let Some((lhs, rhs)) = mantissa.split_once('.') {
        (lhs, rhs)
    } else {
        (mantissa, "")
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    if digits.is_empty() {
        return None;
    }

    let new_pos = int_part.len() as i64 + exponent;
    let mut out = String::new();
    out.push_str(sign);
    if new_pos <= 0 {
        out.push_str("0.");
        for _ in 0..(-new_pos) {
            out.push('0');
        }
        out.push_str(&digits);
        return Some(out);
    }

    let new_pos = new_pos as usize;
    if new_pos >= digits.len() {
        out.push_str(&digits);
        for _ in 0..(new_pos - digits.len()) {
            out.push('0');
        }
        return Some(out);
    }

    out.push_str(&digits[..new_pos]);
    out.push('.');
    out.push_str(&digits[new_pos..]);
    Some(out)
}

pub(crate) fn negate_special_number_literal(
    number: &serde_json::Number,
) -> Option<serde_json::Number> {
    let raw = number.to_string();
    let lowered = raw.to_ascii_lowercase();
    if lowered.starts_with("nan") || lowered.starts_with("-nan") {
        let negated = if lowered.starts_with('-') {
            "nan"
        } else {
            "-nan"
        };
        return Some(serde_json::Number::from_string_unchecked(
            negated.to_string(),
        ));
    }
    match classify_special_number(number)? {
        SpecialNumber::Nan => Some(serde_json::Number::from_string_unchecked(
            "-nan".to_string(),
        )),
        SpecialNumber::PosInf => Some(serde_json::Number::from_string_unchecked(
            "-inf".to_string(),
        )),
        SpecialNumber::NegInf => Some(serde_json::Number::from_string_unchecked("inf".to_string())),
    }
}

// jq-port: jq numbers are evaluated as IEEE-754 doubles when decnum is absent.
// For JSON literal numbers preserved beyond f64 range, clamp infinities to finite
// bounds to mirror jq's non-decnum behavior.
pub(crate) fn jq_number_to_f64_lossy(number: &serde_json::Number) -> Option<f64> {
    if let Some(special) = classify_special_number(number) {
        return Some(match special {
            SpecialNumber::Nan => f64::NAN,
            SpecialNumber::PosInf => f64::INFINITY,
            SpecialNumber::NegInf => f64::NEG_INFINITY,
        });
    }
    if let Some(value) = number.as_f64() {
        return Some(value);
    }

    let raw = number.to_string();
    if let Ok(parsed) = raw.parse::<f64>() {
        if parsed.is_finite() {
            return Some(parsed);
        }
        if parsed.is_sign_positive() {
            return Some(f64::MAX);
        }
        if parsed.is_sign_negative() {
            return Some(-f64::MAX);
        }
    }

    let parsed = parse_number_text(&raw)?;
    if parsed.sign == 0 {
        return Some(0.0);
    }
    // Approximate underflow behavior for extreme negative exponents.
    let magnitude = parsed.digits.len() as i64 + parsed.exponent;
    if magnitude < -324 {
        return Some(0.0f64.copysign(parsed.sign as f64));
    }
    Some(f64::MAX.copysign(parsed.sign as f64))
}

pub(crate) fn number_to_value(value: f64) -> ZqValue {
    number_to_value_with_hint(value, false)
}

// jq-port: keep integer representation unless explicit floating context exists.
pub(crate) fn number_to_value_with_hint(value: f64, force_float: bool) -> ZqValue {
    if !value.is_finite() {
        let raw = if value.is_nan() {
            "nan".to_string()
        } else if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
        return ZqValue::Number(serde_json::Number::from_string_unchecked(raw));
    }
    if !force_float && value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64
    {
        return ZqValue::from(value as i64);
    }
    let number = serde_json::Number::from_f64(value).expect("finite number");
    ZqValue::Number(number)
}

pub(crate) fn number_to_f64_lossy_for_index(
    number: &serde_json::Number,
    err_msg: &str,
) -> Result<f64, String> {
    if let Some(special) = classify_special_number(number) {
        return Ok(match special {
            SpecialNumber::Nan => f64::NAN,
            SpecialNumber::PosInf => f64::INFINITY,
            SpecialNumber::NegInf => f64::NEG_INFINITY,
        });
    }
    if let Some(v) = number.as_f64() {
        return Ok(v);
    }
    if let Some(v) = number.as_i64() {
        return Ok(v as f64);
    }
    if let Some(v) = number.as_u64() {
        return Ok(v as f64);
    }
    Err(err_msg.to_string())
}

// jq-port: jv_set path index coercion.
// moved-from: src/native_engine/vm_core/vm.rs::path_number_for_set
pub(crate) fn path_number_for_set(number: &serde_json::Number) -> Result<Option<i64>, String> {
    if let Some(raw) = number.as_f64() {
        if raw.is_nan() {
            return Ok(None);
        }
        return Ok(Some(dtoi_compat(raw)));
    }
    if let Some(raw) = number.as_i64() {
        return Ok(Some(raw));
    }
    if let Some(raw) = number.as_u64() {
        if raw > i64::MAX as u64 {
            return Err("Array index too large".to_string());
        }
        return Ok(Some(raw as i64));
    }
    Ok(None)
}

// jq-port: jv_del path index coercion.
// moved-from: src/native_engine/vm_core/vm.rs::path_number_for_delete
pub(crate) fn path_number_for_delete(number: &serde_json::Number) -> Option<i64> {
    if let Some(raw) = number.as_f64() {
        if raw.is_nan() {
            return None;
        }
        return Some(dtoi_compat(raw));
    }
    if let Some(raw) = number.as_i64() {
        return Some(raw);
    }
    number.as_u64().map(|raw| {
        if raw > i64::MAX as u64 {
            i64::MAX
        } else {
            raw as i64
        }
    })
}

pub(crate) type SliceComponentBounds = (Option<f64>, Option<f64>);

// jq-port: parse slice descriptor object with optional numeric bounds.
// moved-from: src/native_engine/vm_core/vm.rs::parse_slice_component_for_delete
pub(crate) fn parse_slice_component_for_delete(
    value: &ZqValue,
    err_msg: &str,
) -> Result<Option<SliceComponentBounds>, String> {
    let ZqValue::Object(map) = value else {
        return Ok(None);
    };
    if !map.contains_key("start") && !map.contains_key("end") {
        return Ok(None);
    }
    let start = match map.get("start") {
        None | Some(ZqValue::Null) => None,
        Some(ZqValue::Number(number)) => Some(number_to_f64_lossy_for_index(number, err_msg)?),
        _ => return Err(err_msg.to_string()),
    };
    let end = match map.get("end") {
        None | Some(ZqValue::Null) => None,
        Some(ZqValue::Number(number)) => Some(number_to_f64_lossy_for_index(number, err_msg)?),
        _ => return Err(err_msg.to_string()),
    };
    Ok(Some((start, end)))
}

// jq-port: canonicalize slice bounds using container length when known.
// moved-from: src/native_engine/vm_core/vm.rs::canonicalize_slice_bounds
pub(crate) fn canonicalize_slice_bounds_for_container(
    current: &ZqValue,
    start: Option<f64>,
    end: Option<f64>,
) -> (Option<i64>, Option<i64>) {
    let len = match current {
        ZqValue::Array(items) => Some(items.len()),
        ZqValue::String(text) => Some(text.chars().count()),
        _ => None,
    };
    let Some(len) = len else {
        return (start.map(dtoi_compat), end.map(dtoi_compat));
    };
    let (start_idx, end_idx) = slice_bounds_from_f64_like_jq(len, start, end);
    (Some(start_idx as i64), Some(end_idx as i64))
}

// jq/src/jv.c:jv_array_set()
// if (idx > (INT_MAX >> 2) - jvp_array_offset(j)) -> "Array index too large"
// zq arrays do not carry jq's internal offset, so offset is effectively 0.
// moved-from: src/native_engine/vm_core/vm.rs::ensure_array_set_index_not_too_large
pub(crate) fn ensure_array_set_index_not_too_large(index: usize) -> Result<(), String> {
    const JQ_ARRAY_SET_INDEX_MAX: usize = (i32::MAX as usize) >> 2;
    if index > JQ_ARRAY_SET_INDEX_MAX {
        Err("Array index too large".to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remainder_compat_matches_ieee_behavior() {
        assert_eq!(remainder_compat(5.5, 2.0), -0.5);
        assert!(remainder_compat(1.0, 0.0).is_nan());
    }

    #[test]
    fn fmax_fmin_compat_ignore_nan_operand_like_libm() {
        assert_eq!(fmax_compat(f64::NAN, 4.0), 4.0);
        assert_eq!(fmin_compat(f64::NAN, 4.0), 4.0);
    }

    #[test]
    fn scalbln_compat_uses_c_style_truncation() {
        assert_eq!(scalbln_compat(3.0, 2.9), 12.0);
        assert_eq!(scalbln_compat(3.0, -1.9), 1.5);
    }

    #[test]
    fn parse_non_finite_numbers_matches_jq_forms() {
        let inf = parse_jq_non_finite_number("Infinity")
            .expect("must parse")
            .expect("must return number");
        assert_eq!(inf.to_string(), "inf");

        let ninf = parse_jq_non_finite_number("-inf")
            .expect("must parse")
            .expect("must return number");
        assert_eq!(ninf.to_string(), "-inf");

        let nan = parse_jq_non_finite_number("nan1234")
            .expect("must parse")
            .expect("must return number");
        assert_eq!(nan.to_string(), "nan");

        assert!(parse_jq_non_finite_number("NaN1").is_err());
        assert!(parse_jq_non_finite_number("42")
            .expect("finite should be ignored")
            .is_none());
    }

    #[test]
    fn parse_finite_number_literal_jq_rejects_non_finite_forms() {
        assert_eq!(parse_finite_number_literal_jq("1.25"), Some(1.25));
        assert_eq!(parse_finite_number_literal_jq("1e10000"), None);
        assert_eq!(parse_finite_number_literal_jq("nan"), None);
    }

    #[test]
    fn classify_and_negate_special_numbers_are_consistent() {
        let pos_inf = serde_json::Number::from_string_unchecked("inf".to_string());
        assert_eq!(
            classify_special_number(&pos_inf),
            Some(SpecialNumber::PosInf)
        );
        let neg = negate_special_number_literal(&pos_inf).expect("negated");
        assert_eq!(neg.to_string(), "-inf");
    }

    #[test]
    fn normalize_number_for_tojson_expands_negative_exponents() {
        let n = serde_json::Number::from_string_unchecked("1.25e-2".to_string());
        let normalized = normalize_number_for_tojson(n);
        assert_eq!(normalized.to_string(), "0.0125");
    }

    #[test]
    fn jq_number_to_f64_lossy_handles_large_exponents() {
        let n = serde_json::Number::from_string_unchecked("1e10000".to_string());
        let v = jq_number_to_f64_lossy(&n).expect("coerced");
        assert_eq!(v, f64::MAX);
    }

    #[test]
    fn number_to_value_preserves_integer_shape_without_float_hint() {
        let out = number_to_value_with_hint(2.0, false);
        assert_eq!(out.into_json(), serde_json::json!(2));
    }

    #[test]
    fn number_to_value_preserves_float_shape_with_hint() {
        let out = number_to_value_with_hint(2.0, true);
        assert_eq!(out.into_json(), serde_json::json!(2.0));
    }

    #[test]
    fn number_to_f64_lossy_for_index_handles_special_and_error_cases() {
        let inf = serde_json::Number::from_string_unchecked("inf".to_string());
        let out = number_to_f64_lossy_for_index(&inf, "bad index").expect("special number");
        assert!(out.is_infinite() && out.is_sign_positive());

        let huge =
            serde_json::Number::from_string_unchecked("123456789012345678901234567890".to_string());
        let out = number_to_f64_lossy_for_index(&huge, "bad index").expect("lossy conversion");
        assert!(out.is_finite() && out > 1.0e20);
    }

    #[test]
    fn path_number_for_set_and_delete_follow_jq_rules() {
        let big_unsigned = serde_json::Number::from(u64::MAX);
        let set_idx = path_number_for_set(&big_unsigned).expect("set index");
        assert_eq!(set_idx, Some(i64::MAX));

        let del_idx = path_number_for_delete(&big_unsigned).expect("delete index");
        assert_eq!(del_idx, i64::MAX);
    }

    #[test]
    fn parse_slice_component_for_delete_reads_start_end_numbers() {
        let mut map = indexmap::IndexMap::new();
        map.insert(
            "start".to_string(),
            ZqValue::Number(serde_json::Number::from_string_unchecked("1".to_string())),
        );
        map.insert(
            "end".to_string(),
            ZqValue::Number(serde_json::Number::from_string_unchecked("4".to_string())),
        );
        let parsed = parse_slice_component_for_delete(&ZqValue::Object(map), "bad slice")
            .expect("must parse")
            .expect("must return bounds");
        assert_eq!(parsed, (Some(1.0), Some(4.0)));
    }

    #[test]
    fn canonicalize_slice_bounds_for_container_uses_lengths() {
        let arr = ZqValue::Array(vec![ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]);
        let bounds = canonicalize_slice_bounds_for_container(&arr, Some(-2.0), None);
        assert_eq!(bounds, (Some(1), Some(3)));

        let scalar = ZqValue::Null;
        let bounds = canonicalize_slice_bounds_for_container(&scalar, Some(-2.8), Some(4.1));
        assert_eq!(bounds, (Some(-2), Some(4)));
    }

    #[test]
    fn compare_json_numbers_like_jq_uses_text_fallback_for_huge_plain_ints() {
        let a = serde_json::Number::from_string_unchecked("123456789012345678901".to_string());
        let b = serde_json::Number::from_string_unchecked("123456789012345678900".to_string());
        assert_eq!(compare_json_numbers_like_jq(&a, &b), Ordering::Greater);
    }

    #[test]
    fn compare_json_numbers_like_jq_uses_f64_for_regular_numbers() {
        let a = serde_json::Number::from_f64(1.5).expect("finite");
        let b = serde_json::Number::from_f64(2.0).expect("finite");
        assert_eq!(compare_json_numbers_like_jq(&a, &b), Ordering::Less);
    }

    #[test]
    fn ensure_array_set_index_not_too_large_matches_jq_threshold() {
        let limit = (i32::MAX as usize) >> 2;
        ensure_array_set_index_not_too_large(limit).expect("limit is accepted");
        let err = ensure_array_set_index_not_too_large(limit + 1).expect_err("overflow");
        assert_eq!(err, "Array index too large");
    }
}
