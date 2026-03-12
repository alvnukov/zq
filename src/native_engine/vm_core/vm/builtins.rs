use super::*;

pub(super) fn run_builtin(filter: Builtin, input: ZqValue) -> Result<ZqValue, String> {
    match filter {
        Builtin::Not => Ok(ZqValue::Bool(!jq_truthy(&input))),
        Builtin::Length => run_length(input),
        Builtin::FAbs => run_fabs(input),
        Builtin::Floor => run_floor(input),
        Builtin::Ceil => run_ceil(input),
        Builtin::Sqrt => run_sqrt(input),
        Builtin::Cbrt => run_cbrt(input),
        Builtin::Round => run_round(input),
        Builtin::Acos => run_acos(input),
        Builtin::Acosh => run_acosh(input),
        Builtin::Asin => run_asin(input),
        Builtin::Asinh => run_asinh(input),
        Builtin::Atan => run_atan(input),
        Builtin::Atanh => run_atanh(input),
        Builtin::Sin => run_sin(input),
        Builtin::Sinh => run_sinh(input),
        Builtin::Tan => run_tan(input),
        Builtin::Tanh => run_tanh(input),
        Builtin::Cos => run_cos(input),
        Builtin::Cosh => run_cosh(input),
        Builtin::Exp => run_exp(input),
        Builtin::Exp2 => run_exp2(input),
        Builtin::Log => run_log(input),
        Builtin::Log10 => run_log10(input),
        Builtin::Log1p => run_log1p(input),
        Builtin::Expm1 => run_expm1(input),
        Builtin::Log2 => run_log2(input),
        Builtin::IsInfinite => run_isinfinite(input),
        Builtin::IsNan => run_isnan(input),
        Builtin::IsNormal => run_isnormal(input),
        Builtin::Type => Ok(ZqValue::String(type_name(&input).to_string())),
        Builtin::Add => run_add(input),
        Builtin::Keys => c_container::keys_jq(input, true),
        Builtin::KeysUnsorted => c_container::keys_jq(input, false),
        Builtin::ToEntries => c_container::to_entries_jq(input),
        Builtin::FromEntries => c_container::from_entries_jq(input),
        Builtin::ToNumber => c_json::tonumber_filter_jq(input),
        Builtin::ToString => c_json::tostring_filter_jq(input),
        Builtin::ToBoolean => c_json::toboolean_filter_jq(input),
        Builtin::ToJson => c_json::tojson_filter_jq(input),
        Builtin::FromJson => c_json::fromjson_filter_jq(input),
        Builtin::Utf8ByteLength => c_string::utf8_byte_length_jq(input),
        Builtin::Explode => c_string::explode_jq(input),
        Builtin::Implode => c_string::implode_jq(input),
        Builtin::Trim => c_string::trim_whitespace_jq(input, c_string::TrimMode::Both),
        Builtin::LTrim => c_string::trim_whitespace_jq(input, c_string::TrimMode::Left),
        Builtin::RTrim => c_string::trim_whitespace_jq(input, c_string::TrimMode::Right),
        Builtin::Reverse => run_reverse(input),
        Builtin::AsciiUpcase => c_string::ascii_case_jq(input, true),
        Builtin::AsciiDowncase => c_string::ascii_case_jq(input, false),
        Builtin::Flatten => run_flatten(input, None),
        Builtin::Transpose => run_transpose(input),
        Builtin::First => run_nth(input, ZqValue::from(0)),
        Builtin::Last => run_nth(input, ZqValue::from(-1)),
        Builtin::Sort => c_container::sort_jq(input),
        Builtin::Unique => c_container::unique_jq(input),
        Builtin::Min => c_container::minmax_jq(input, true),
        Builtin::Max => c_container::minmax_jq(input, false),
        Builtin::Gmtime => run_gmtime(input),
        Builtin::Localtime => run_localtime(input),
        Builtin::Mktime => run_mktime(input),
        Builtin::FromDateIso8601 => run_fromdateiso8601(input),
        Builtin::ToDateIso8601 => run_todateiso8601(input),
        Builtin::Debug => Ok(input),
        Builtin::Stderr => Ok(input),
        Builtin::ModuleMeta => run_modulemeta(input),
        Builtin::Env => run_env(input),
        Builtin::Halt => run_halt(),
        Builtin::GetSearchList => run_get_search_list(input),
        Builtin::GetProgOrigin => run_get_prog_origin(input),
        Builtin::GetJqOrigin => run_get_jq_origin(input),
        Builtin::Now => run_now(input),
        Builtin::InputFilename => run_input_filename(input),
        Builtin::InputLineNumber => run_input_line_number(input),
        Builtin::HaveDecnum => run_have_decnum(input),
        Builtin::HaveLiteralNumbers => run_have_literal_numbers(input),
        Builtin::BuiltinsList => run_builtins_list(input),
    }
}

fn run_modulemeta(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::String(module_name) = input else {
        return Err("modulemeta input module name must be a string".to_string());
    };
    parser::load_module_meta(&module_name, current_module_search_dirs())
}

pub(super) fn run_env(_input: ZqValue) -> Result<ZqValue, String> {
    let mut out = IndexMap::new();
    for (key, value) in std::env::vars() {
        out.insert(key, ZqValue::String(value));
    }
    Ok(ZqValue::Object(out))
}

fn run_halt() -> Result<ZqValue, String> {
    Err(encode_halt_error(ZqValue::from(0), ZqValue::Null)?)
}

fn run_get_search_list(_input: ZqValue) -> Result<ZqValue, String> {
    let values = current_module_search_dirs()
        .into_iter()
        .map(|path| ZqValue::String(path.to_string_lossy().to_string()))
        .collect();
    Ok(ZqValue::Array(values))
}

fn run_get_prog_origin(_input: ZqValue) -> Result<ZqValue, String> {
    // jq exposes program origin (directory of -f file). For string queries,
    // jq reports null, which matches zq's current execution model.
    Ok(ZqValue::Null)
}

fn run_get_jq_origin(_input: ZqValue) -> Result<ZqValue, String> {
    let origin = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .map(|p| ZqValue::String(p.to_string_lossy().to_string()))
        .unwrap_or(ZqValue::Null);
    Ok(origin)
}

fn run_now(_input: ZqValue) -> Result<ZqValue, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("now failed: {e}"))?
        .as_secs_f64();
    let number = serde_json::Number::from_f64(now).ok_or("number is out of range".to_string())?;
    Ok(ZqValue::Number(number))
}

fn run_input_filename(_input: ZqValue) -> Result<ZqValue, String> {
    Ok(ZqValue::Null)
}

fn run_input_line_number(_input: ZqValue) -> Result<ZqValue, String> {
    Ok(match input_line_number_value() {
        Some(line) => ZqValue::from(line),
        None => ZqValue::Null,
    })
}

fn run_have_decnum(_input: ZqValue) -> Result<ZqValue, String> {
    Ok(ZqValue::Bool(true))
}

fn run_have_literal_numbers(_input: ZqValue) -> Result<ZqValue, String> {
    Ok(ZqValue::Bool(true))
}

fn run_builtins_list(_input: ZqValue) -> Result<ZqValue, String> {
    // jq/src/builtin.c gen_builtin_list():
    // expose builtins as "name/arity", excluding names that start with "_".
    use std::collections::BTreeSet;

    const BUILTINS: &[(&str, usize)] = &[
        ("abs", 0),
        ("add", 0),
        ("add", 1),
        ("all", 0),
        ("all", 1),
        ("all", 2),
        ("any", 0),
        ("any", 1),
        ("any", 2),
        ("arrays", 0),
        ("ascii_downcase", 0),
        ("ascii_upcase", 0),
        ("acos", 0),
        ("acosh", 0),
        ("asin", 0),
        ("asinh", 0),
        ("atan", 0),
        ("atan2", 2),
        ("atanh", 0),
        ("booleans", 0),
        ("bsearch", 1),
        ("builtins", 0),
        ("capture", 1),
        ("capture", 2),
        ("combinations", 0),
        ("combinations", 1),
        ("contains", 1),
        ("copysign", 2),
        ("cos", 0),
        ("cosh", 0),
        ("debug", 0),
        ("debug", 1),
        ("del", 1),
        ("delpaths", 1),
        ("drem", 2),
        ("endswith", 1),
        ("env", 0),
        ("error", 0),
        ("error", 1),
        ("explode", 0),
        ("exp", 0),
        ("exp2", 0),
        ("expm1", 0),
        ("fabs", 0),
        ("fdim", 2),
        ("fma", 3),
        ("fmax", 2),
        ("fmin", 2),
        ("fmod", 2),
        ("finites", 0),
        ("first", 0),
        ("first", 1),
        ("flatten", 0),
        ("flatten", 1),
        ("floor", 0),
        ("ceil", 0),
        ("from_entries", 0),
        ("fromdate", 0),
        ("fromdateiso8601", 0),
        ("fromjson", 0),
        ("fromstream", 1),
        ("get_jq_origin", 0),
        ("get_prog_origin", 0),
        ("get_search_list", 0),
        ("getpath", 1),
        ("group_by", 1),
        ("gmtime", 0),
        ("gsub", 2),
        ("gsub", 3),
        ("halt", 0),
        ("halt_error", 0),
        ("halt_error", 1),
        ("has", 1),
        ("have_decnum", 0),
        ("have_literal_numbers", 0),
        ("hypot", 2),
        ("implode", 0),
        ("in", 1),
        ("INDEX", 1),
        ("INDEX", 2),
        ("indices", 1),
        ("input", 0),
        ("input_filename", 0),
        ("input_line_number", 0),
        ("inputs", 0),
        ("inside", 1),
        ("isempty", 1),
        ("isfinite", 0),
        ("isinfinite", 0),
        ("isnan", 0),
        ("isnormal", 0),
        ("iterables", 0),
        ("jn", 2),
        ("join", 1),
        ("JOIN", 2),
        ("JOIN", 3),
        ("JOIN", 4),
        ("keys", 0),
        ("keys_unsorted", 0),
        ("last", 0),
        ("last", 1),
        ("length", 0),
        ("limit", 2),
        ("localtime", 0),
        ("log", 0),
        ("log10", 0),
        ("log1p", 0),
        ("log2", 0),
        ("ldexp", 2),
        ("ltrim", 0),
        ("ltrimstr", 1),
        ("map", 1),
        ("map_values", 1),
        ("match", 1),
        ("match", 2),
        ("max", 0),
        ("max_by", 1),
        ("min", 0),
        ("min_by", 1),
        ("mktime", 0),
        ("modulemeta", 0),
        ("normals", 0),
        ("not", 0),
        ("now", 0),
        ("nextafter", 2),
        ("nexttoward", 2),
        ("nth", 1),
        ("nth", 2),
        ("nulls", 0),
        ("numbers", 0),
        ("objects", 0),
        ("path", 1),
        ("paths", 0),
        ("paths", 1),
        ("pick", 1),
        ("pow", 2),
        ("range", 1),
        ("range", 2),
        ("range", 3),
        ("rindex", 1),
        ("remainder", 2),
        ("repeat", 1),
        ("recurse", 0),
        ("recurse", 1),
        ("recurse", 2),
        ("reverse", 0),
        ("round", 0),
        ("rtrim", 0),
        ("rtrimstr", 1),
        ("scan", 1),
        ("scan", 2),
        ("scalb", 2),
        ("scalbln", 2),
        ("scalars", 0),
        ("select", 1),
        ("sin", 0),
        ("sinh", 0),
        ("skip", 2),
        ("sort", 0),
        ("sort_by", 1),
        ("split", 1),
        ("split", 2),
        ("splits", 1),
        ("splits", 2),
        ("sqrt", 0),
        ("cbrt", 0),
        ("startswith", 1),
        ("stderr", 0),
        ("strflocaltime", 1),
        ("strftime", 1),
        ("strings", 0),
        ("strptime", 1),
        ("sub", 2),
        ("sub", 3),
        ("todate", 0),
        ("todateiso8601", 0),
        ("to_entries", 0),
        ("toboolean", 0),
        ("tonumber", 0),
        ("tojson", 0),
        ("tostream", 0),
        ("tostring", 0),
        ("transpose", 0),
        ("tan", 0),
        ("tanh", 0),
        ("trim", 0),
        ("trimstr", 1),
        ("truncate_stream", 1),
        ("type", 0),
        ("unique", 0),
        ("unique_by", 1),
        ("until", 2),
        ("utf8bytelength", 0),
        ("values", 0),
        ("walk", 1),
        ("while", 2),
        ("with_entries", 1),
        ("yn", 2),
        ("IN", 1),
        ("IN", 2),
    ];

    let mut entries = BTreeSet::new();
    for (name, arity) in BUILTINS {
        if name.starts_with('_') {
            continue;
        }
        entries.insert(format!("{name}/{arity}"));
    }
    Ok(ZqValue::Array(entries.into_iter().map(ZqValue::String).collect()))
}

pub(super) fn run_length(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Null => Ok(ZqValue::from(0)),
        ZqValue::Array(arr) => Ok(ZqValue::from(arr.len() as i64)),
        ZqValue::Object(map) => Ok(ZqValue::from(map.len() as i64)),
        ZqValue::String(s) => Ok(ZqValue::from(s.chars().count() as i64)),
        ZqValue::Number(n) => {
            if let Some(value) = n.as_f64() {
                return Ok(c_math::number_to_value(value.abs()));
            }
            let raw = n.to_string();
            let abs_raw = raw.strip_prefix('-').unwrap_or(raw.as_str()).to_string();
            Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(abs_raw)))
        }
        ZqValue::Bool(b) => Err(format!("boolean ({b}) has no length")),
    }
}

fn run_fabs(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => {
            let Some(value) = c_math::jq_number_to_f64_lossy(&n) else {
                return Err("number is out of range".to_string());
            };
            Ok(c_math::number_to_value(value.abs()))
        }
        other => {
            Err(format!("{} ({}) number required", type_name(&other), value_for_error(&other)))
        }
    }
}

fn run_floor(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::floor)
}

fn run_ceil(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::ceil)
}

fn run_sqrt(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::sqrt)
}

fn run_cbrt(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::cbrt)
}

fn run_round(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::round)
}

fn run_acos(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::acos)
}

fn run_acosh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::acosh)
}

fn run_asin(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::asin)
}

fn run_asinh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::asinh)
}

fn run_atan(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::atan)
}

fn run_atanh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::atanh)
}

fn run_sin(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::sin)
}

fn run_sinh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::sinh)
}

fn run_tan(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::tan)
}

fn run_tanh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::tanh)
}

fn run_cos(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::cos)
}

fn run_cosh(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::cosh)
}

fn run_exp(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::exp)
}

fn run_exp2(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::exp2)
}

fn run_log(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::ln)
}

fn run_log10(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::log10)
}

fn run_log1p(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::ln_1p)
}

fn run_expm1(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::exp_m1)
}

fn run_log2(input: ZqValue) -> Result<ZqValue, String> {
    run_unary_math(input, f64::log2)
}

fn run_unary_math(input: ZqValue, op: fn(f64) -> f64) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => {
            let Some(value) = c_math::jq_number_to_f64_lossy(&n) else {
                return Err("number is out of range".to_string());
            };
            Ok(c_math::number_to_value(op(value)))
        }
        other => {
            Err(format!("{} ({}) number required", type_name(&other), value_for_error(&other)))
        }
    }
}

pub(super) fn run_math_binary(
    op: MathBinaryOp,
    lhs: ZqValue,
    rhs: ZqValue,
) -> Result<ZqValue, String> {
    let af = as_f64_math_arg(&lhs)?;
    let bf = as_f64_math_arg(&rhs)?;
    let value = match op {
        MathBinaryOp::Atan2 => Ok(af.atan2(bf)),
        MathBinaryOp::Hypot => Ok(af.hypot(bf)),
        MathBinaryOp::CopySign => Ok(af.copysign(bf)),
        MathBinaryOp::Drem => Ok(c_math::remainder_compat(af, bf)),
        MathBinaryOp::Fdim => Ok(c_math::fdim_compat(af, bf)),
        MathBinaryOp::Fmax => Ok(c_math::fmax_compat(af, bf)),
        MathBinaryOp::Fmin => Ok(c_math::fmin_compat(af, bf)),
        MathBinaryOp::Fmod => Ok(af % bf),
        MathBinaryOp::Jn => c_math::jn_compat(af, bf),
        MathBinaryOp::Ldexp => Ok(c_math::ldexp_compat(af, bf)),
        MathBinaryOp::NextAfter => Ok(c_math::nextafter_compat(af, bf)),
        MathBinaryOp::NextToward => Ok(c_math::nextafter_compat(af, bf)),
        MathBinaryOp::Remainder => Ok(c_math::remainder_compat(af, bf)),
        MathBinaryOp::Scalb => Ok(c_math::scalb_compat(af, bf)),
        MathBinaryOp::Scalbln => Ok(c_math::scalbln_compat(af, bf)),
        MathBinaryOp::Yn => c_math::yn_compat(af, bf),
    }?;
    Ok(c_math::number_to_value(value))
}

pub(super) fn run_math_ternary(
    op: MathTernaryOp,
    a: ZqValue,
    b: ZqValue,
    c: ZqValue,
) -> Result<ZqValue, String> {
    let af = as_f64_math_arg(&a)?;
    let bf = as_f64_math_arg(&b)?;
    let cf = as_f64_math_arg(&c)?;
    let value = match op {
        MathTernaryOp::Fma => af.mul_add(bf, cf),
    };
    Ok(c_math::number_to_value(value))
}

fn as_f64_math_arg(value: &ZqValue) -> Result<f64, String> {
    let ZqValue::Number(number) = value else {
        return Err(number_required_error(value));
    };
    c_math::jq_number_to_f64_lossy(number).ok_or_else(|| "number is out of range".to_string())
}

fn number_required_error(value: &ZqValue) -> String {
    format!("{} ({}) number required", type_name(value), value_for_error(value))
}

fn run_isinfinite(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => Ok(ZqValue::Bool(matches!(
            c_math::classify_special_number(&n),
            Some(c_math::SpecialNumber::PosInf | c_math::SpecialNumber::NegInf)
        ))),
        _ => Ok(ZqValue::Bool(false)),
    }
}

fn run_isnan(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => Ok(ZqValue::Bool(matches!(
            c_math::classify_special_number(&n),
            Some(c_math::SpecialNumber::Nan)
        ))),
        _ => Ok(ZqValue::Bool(false)),
    }
}

fn run_isnormal(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Number(n) => Ok(ZqValue::Bool(
            c_math::classify_special_number(&n).is_none()
                && n.as_f64().is_some_and(|value| value.is_normal()),
        )),
        _ => Ok(ZqValue::Bool(false)),
    }
}

pub(super) fn run_join(input: ZqValue, separator: ZqValue) -> Result<ZqValue, String> {
    let values = iter_values_like_jq(input)?;
    let mut acc = ZqValue::Null;
    for value in values {
        let prefix = if matches!(acc, ZqValue::Null) {
            ZqValue::String(String::new())
        } else {
            binop_add(acc, separator.clone(), false)?
        };

        let joined = match value {
            ZqValue::Bool(_) | ZqValue::Number(_) => {
                ZqValue::String(c_json::tostring_value_jq(&value)?)
            }
            ZqValue::Null => ZqValue::String(String::new()),
            other => other,
        };
        acc = binop_add(prefix, joined, false)?;
    }

    if matches!(acc, ZqValue::Null | ZqValue::Bool(false)) {
        Ok(ZqValue::String(String::new()))
    } else {
        Ok(acc)
    }
}

fn run_reverse(input: ZqValue) -> Result<ZqValue, String> {
    match input {
        ZqValue::Array(mut values) => {
            values.reverse();
            Ok(ZqValue::Array(values))
        }
        ZqValue::String(s) => {
            Ok(ZqValue::Array(s.chars().rev().map(|ch| ZqValue::String(ch.to_string())).collect()))
        }
        ZqValue::Null => Ok(ZqValue::Array(Vec::new())),
        ZqValue::Bool(b) => Err(format!("boolean ({b}) has no length")),
        ZqValue::Object(map) => {
            if map.is_empty() {
                Ok(ZqValue::Array(Vec::new()))
            } else {
                Err("Cannot index object with number".to_string())
            }
        }
        ZqValue::Number(n) => {
            let Some(value) = n.as_f64() else {
                return Err("number is out of range".to_string());
            };
            if value.abs() == 0.0 {
                Ok(ZqValue::Array(Vec::new()))
            } else {
                Err("Cannot index number with number".to_string())
            }
        }
    }
}

// Ported from jq/src/builtin.c:f_gmtime().
#[cfg(unix)]
fn run_gmtime(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Number(number) = input else {
        return Err("gmtime() requires numeric inputs".to_string());
    };
    let Some(fsecs) = number.as_f64() else {
        return Err("gmtime() requires numeric inputs".to_string());
    };
    let secs =
        c_time::cast_time_t_trunc(fsecs).map_err(|_| "number is out of range".to_string())?;
    let Some(tm) = c_time::utc_tm_from_seconds(secs) else {
        return Err("error converting number of seconds since epoch to datetime".to_string());
    };
    tm_to_jq_array(&tm, fsecs)
}

// Ported from jq/src/builtin.c:f_localtime().
#[cfg(unix)]
fn run_localtime(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Number(number) = input else {
        return Err("localtime() requires numeric inputs".to_string());
    };
    let Some(fsecs) = number.as_f64() else {
        return Err("localtime() requires numeric inputs".to_string());
    };
    let secs =
        c_time::cast_time_t_trunc(fsecs).map_err(|_| "number is out of range".to_string())?;
    let Some(tm) = c_time::local_tm_from_seconds(secs) else {
        return Err("error converting number of seconds since epoch to datetime".to_string());
    };
    tm_to_jq_array(&tm, fsecs)
}

#[cfg(not(unix))]
fn run_gmtime(_input: ZqValue) -> Result<ZqValue, String> {
    Err("gmtime not implemented on this platform".to_string())
}

#[cfg(not(unix))]
fn run_localtime(_input: ZqValue) -> Result<ZqValue, String> {
    Err("localtime not implemented on this platform".to_string())
}

// Ported from jq/src/builtin.c:f_mktime().
fn run_mktime(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Array(_) = input else {
        return Err("mktime requires array inputs".to_string());
    };
    let mut tm = match jq_array_to_tm(input, false) {
        Some(tm) => tm,
        None => return Err("mktime requires parsed datetime inputs".to_string()),
    };
    let Some(timestamp) = c_time::timegm_utc(&mut tm) else {
        return Err("mktime not supported on this platform".to_string());
    };
    if timestamp == -1 {
        return Err("invalid gmtime representation".to_string());
    }
    Ok(ZqValue::from(timestamp))
}

// Ported from jq/src/builtin.c:f_strptime().
#[cfg(unix)]
pub(super) fn run_strptime(input: ZqValue, format: ZqValue) -> Result<ZqValue, String> {
    let (ZqValue::String(input), ZqValue::String(format)) = (input, format) else {
        return Err("strptime/1 requires string inputs and arguments".to_string());
    };
    let (mut tm, remainder_bytes) = c_time::parse_strptime(&input, &format)
        .map_err(|_| format!("date \"{input}\" does not match format \"{format}\""))?;
    if !remainder_bytes.is_empty() && !remainder_bytes[0].is_ascii_whitespace() {
        return Err(format!("date \"{input}\" does not match format \"{format}\""));
    }
    c_time::fill_tm_wday_yday(&mut tm);
    let mut out = match tm_to_jq_array(&tm, 0.0)? {
        ZqValue::Array(values) => values,
        _ => unreachable!("tm_to_jq_array always returns array"),
    };
    if !remainder_bytes.is_empty() {
        let remainder = String::from_utf8_lossy(&remainder_bytes).to_string();
        out.push(ZqValue::String(remainder));
    }
    Ok(ZqValue::Array(out))
}

#[cfg(not(unix))]
pub(super) fn run_strptime(_input: ZqValue, _format: ZqValue) -> Result<ZqValue, String> {
    Err("strptime/1 not implemented on this platform".to_string())
}

// Ported from jq/src/builtin.c:f_strftime() and f_strflocaltime().
pub(super) fn run_strftime(
    input: ZqValue,
    format: ZqValue,
    local: bool,
) -> Result<ZqValue, String> {
    let op_name = if local { "strflocaltime" } else { "strftime" };
    let input = match input {
        ZqValue::Number(_) => {
            if local {
                run_localtime(input)?
            } else {
                run_gmtime(input)?
            }
        }
        ZqValue::Array(_) => input,
        _ => return Err(format!("{op_name}/1 requires parsed datetime inputs")),
    };
    let ZqValue::String(format) = format else {
        return Err(format!("{op_name}/1 requires a string format"));
    };
    if format.is_empty() {
        return Ok(ZqValue::String(String::new()));
    }
    let mut tm = jq_array_to_tm(input, local)
        .ok_or_else(|| format!("{op_name}/1 requires parsed datetime inputs"))?;
    let rendered = c_time::format_tm_with_strftime(&mut tm, &format, local).map_err(|err| {
        let _ = err;
        format!("{op_name}/1: unknown system failure")
    })?;
    Ok(ZqValue::String(rendered))
}

fn tm_to_jq_array(tm: &libc::tm, fsecs: f64) -> Result<ZqValue, String> {
    let fields = c_time::tm_to_numeric_fields_like_jq(tm, fsecs);
    Ok(ZqValue::Array(fields.into_iter().map(c_math::number_to_value).collect()))
}

fn jq_array_to_tm(input: ZqValue, local: bool) -> Option<libc::tm> {
    let ZqValue::Array(values) = input else {
        return None;
    };
    let mut fields = Vec::with_capacity(values.len().min(8));
    for value in values.into_iter().take(8) {
        let ZqValue::Number(number) = value else {
            return None;
        };
        fields.push(number.as_f64()?);
    }
    c_time::tm_from_numeric_fields_like_jq(&fields, local)
}

fn run_fromdateiso8601(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::String(text) = input else {
        return Err(format!(
            "{} ({}) cannot be parsed as ISO 8601 datetime",
            type_name(&input),
            value_for_error(&input)
        ));
    };
    let seconds = c_time::parse_iso8601_utc_seconds_strict(&text)
        .ok_or_else(|| format!("date \"{text}\" does not match format \"%Y-%m-%dT%H:%M:%SZ\""))?;
    Ok(ZqValue::from(seconds))
}

fn run_todateiso8601(input: ZqValue) -> Result<ZqValue, String> {
    let ZqValue::Number(number) = input else {
        // jq/src/builtin.jq:
        // def todateiso8601: strftime("%Y-%m-%dT%H:%M:%SZ");
        // Keep error shape aligned with strftime input validation.
        return Err("strftime/1 requires parsed datetime inputs".to_string());
    };
    let Some(raw) = number.as_f64() else {
        return Err("number is out of range".to_string());
    };
    if !raw.is_finite() {
        return Err("number is out of range".to_string());
    }
    let seconds = raw.trunc() as i64;
    Ok(ZqValue::String(c_time::format_iso8601_utc_seconds(seconds)))
}
