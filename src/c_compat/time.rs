// c-ref: C time compatibility helpers (time.h + strftime/strptime family).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeCastError {
    NonFinite,
    OutOfRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TimeFormatError {
    #[cfg(test)]
    NonFiniteTimestamp,
    #[cfg(test)]
    TimestampOutOfRange,
    #[cfg(test)]
    ConvertTimestampFailed,
    FormatContainsNul,
    FormatFailed,
    UnsupportedPlatform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrptimeError {
    InputContainsNul,
    FormatContainsNul,
    ParseFailed,
}

// c-ref: zero-initialized `struct tm` constructor for libc APIs.
// moved-from: src/native_engine/vm_core/vm.rs::jq_array_to_tm
pub(crate) fn zeroed_tm() -> libc::tm {
    // FFI POD from C; zero-init matches libc API expectations for tm seeds.
    unsafe { std::mem::MaybeUninit::<libc::tm>::zeroed().assume_init() }
}

// c-ref: conversion to time_t with truncation to integer seconds.
// moved-from: src/native_engine/vm_core/vm.rs::cast_time_t_trunc
pub(crate) fn cast_time_t_trunc(value: f64) -> Result<libc::time_t, TimeCastError> {
    if !value.is_finite() {
        return Err(TimeCastError::NonFinite);
    }
    let whole = value.trunc();
    if whole < libc::time_t::MIN as f64 || whole > libc::time_t::MAX as f64 {
        return Err(TimeCastError::OutOfRange);
    }
    Ok(whole as libc::time_t)
}

// c-ref: gmtime_r(3)
// moved-from: src/native_engine/vm_core/vm.rs::run_gmtime
#[cfg(unix)]
pub(crate) fn utc_tm_from_seconds(seconds: libc::time_t) -> Option<libc::tm> {
    let mut tm: libc::tm = zeroed_tm();
    let tm_ptr = unsafe { libc::gmtime_r(&seconds, &mut tm) };
    if tm_ptr.is_null() {
        None
    } else {
        Some(tm)
    }
}

#[cfg(not(unix))]
pub(crate) fn utc_tm_from_seconds(_seconds: libc::time_t) -> Option<libc::tm> {
    None
}

// c-ref: localtime_r(3)
// moved-from: src/native_engine/vm_core/vm.rs::run_localtime
#[cfg(unix)]
pub(crate) fn local_tm_from_seconds(seconds: libc::time_t) -> Option<libc::tm> {
    let mut tm: libc::tm = zeroed_tm();
    let tm_ptr = unsafe { libc::localtime_r(&seconds, &mut tm) };
    if tm_ptr.is_null() {
        None
    } else {
        Some(tm)
    }
}

#[cfg(not(unix))]
pub(crate) fn local_tm_from_seconds(_seconds: libc::time_t) -> Option<libc::tm> {
    None
}

// c-ref: timegm(3) equivalent path for UTC.
// moved-from: src/native_engine/vm_core/vm.rs::my_mktime_utc
pub(crate) fn timegm_utc(tm: &mut libc::tm) -> Option<libc::time_t> {
    #[cfg(not(target_os = "windows"))]
    {
        Some(unsafe { libc::timegm(tm) })
    }
    #[cfg(target_os = "windows")]
    {
        Some(unsafe { windows_crt_timegm_utc(tm) })
    }
}

// c-ref: mktime(3) normalization for local time structs.
// moved-from: src/native_engine/vm_core/vm.rs::jq_array_to_tm
pub(crate) fn normalize_local_tm(tm: &mut libc::tm) {
    tm.tm_isdst = -1;
    #[cfg(not(target_os = "windows"))]
    unsafe {
        libc::mktime(tm);
    }
    #[cfg(target_os = "windows")]
    unsafe {
        let _ = windows_crt_mktime_local(tm);
    }
}

#[cfg(target_os = "windows")]
unsafe fn windows_crt_timegm_utc(tm: *mut libc::tm) -> libc::time_t {
    unsafe extern "C" {
        fn _mkgmtime64(tm: *mut libc::tm) -> i64;
    }
    _mkgmtime64(tm) as libc::time_t
}

#[cfg(target_os = "windows")]
unsafe fn windows_crt_mktime_local(tm: *mut libc::tm) -> libc::time_t {
    unsafe extern "C" {
        fn _mktime64(tm: *mut libc::tm) -> i64;
    }
    _mktime64(tm) as libc::time_t
}

// c-ref: strftime(3) with Apple UTC workaround.
// moved-from: src/native_engine/vm_core/vm.rs::format_tm_with_strftime
#[cfg(unix)]
pub(crate) fn format_tm_with_strftime(
    tm: &mut libc::tm,
    format: &str,
    local: bool,
) -> Result<String, TimeFormatError> {
    ensure_process_locale_initialized();
    let format_c =
        std::ffi::CString::new(format).map_err(|_| TimeFormatError::FormatContainsNul)?;
    #[cfg(not(target_vendor = "apple"))]
    let _ = local;

    #[cfg(target_vendor = "apple")]
    let _tz_guard = if local {
        None
    } else {
        Some(ScopedUtcTz::enter())
    };

    let mut capacity = format.len().saturating_add(100).max(64);
    for _ in 0..6 {
        let mut buffer = vec![0u8; capacity];
        let written = unsafe {
            libc::strftime(
                buffer.as_mut_ptr().cast::<libc::c_char>(),
                buffer.len(),
                format_c.as_ptr(),
                tm,
            )
        };
        if written > 0 {
            let rendered =
                unsafe { std::ffi::CStr::from_ptr(buffer.as_ptr().cast::<libc::c_char>()) };
            return Ok(rendered.to_string_lossy().into_owned());
        }
        capacity = capacity.saturating_mul(2);
    }
    Err(TimeFormatError::FormatFailed)
}

#[cfg(not(unix))]
pub(crate) fn format_tm_with_strftime(
    _tm: &mut libc::tm,
    _format: &str,
    _local: bool,
) -> Result<String, TimeFormatError> {
    Err(TimeFormatError::UnsupportedPlatform)
}

// c-ref: strptime(3) parser wrapper.
// moved-from: src/native_engine/vm_core/vm.rs::run_strptime
#[cfg(unix)]
pub(crate) fn parse_strptime(
    input: &str,
    format: &str,
) -> Result<(libc::tm, Vec<u8>), StrptimeError> {
    ensure_process_locale_initialized();
    let input_c = std::ffi::CString::new(input).map_err(|_| StrptimeError::InputContainsNul)?;
    let format_c = std::ffi::CString::new(format).map_err(|_| StrptimeError::FormatContainsNul)?;
    let mut tm: libc::tm = zeroed_tm();
    tm.tm_wday = 8;
    tm.tm_yday = 367;
    let end = unsafe { libc::strptime(input_c.as_ptr(), format_c.as_ptr(), &mut tm) };
    if end.is_null() {
        return Err(StrptimeError::ParseFailed);
    }
    let remainder_bytes = unsafe { std::ffi::CStr::from_ptr(end).to_bytes().to_vec() };
    Ok((tm, remainder_bytes))
}

#[cfg(unix)]
fn ensure_process_locale_initialized() {
    // Keep locale mutation disabled in library/test contexts: calling setlocale()
    // is process-global and thread-unsafe and has caused sporadic CI crashes under
    // parallel test execution. We intentionally rely on the current process locale.
}

// c-ref: jq datetime arrays map to `struct tm` slots and normalize via mktime/timegm.
// moved-from: src/native_engine/vm_core/vm.rs::jq_array_to_tm
pub(crate) fn tm_from_numeric_fields_like_jq(fields: &[f64], local: bool) -> Option<libc::tm> {
    // Guard libc mktime/timegm calls from extreme user-provided values that can
    // trigger undefined behavior or crashes on some libc implementations.
    const TM_FIELD_ABS_LIMIT: f64 = 1_000_000.0;

    let mut tm: libc::tm = zeroed_tm();
    for (idx, mut raw) in fields.iter().copied().take(8).enumerate() {
        if !raw.is_finite() {
            return None;
        }
        if raw.abs() > TM_FIELD_ABS_LIMIT {
            return None;
        }
        if idx == 0 {
            raw -= 1900.0;
        }
        let clamped = raw as i32;
        match idx {
            0 => tm.tm_year = clamped,
            1 => tm.tm_mon = clamped,
            2 => tm.tm_mday = clamped,
            3 => tm.tm_hour = clamped,
            4 => tm.tm_min = clamped,
            5 => tm.tm_sec = clamped,
            6 => tm.tm_wday = clamped,
            7 => tm.tm_yday = clamped,
            _ => unreachable!(),
        }
    }
    if local {
        normalize_local_tm(&mut tm);
    } else {
        let _ = timegm_utc(&mut tm);
    }
    Some(tm)
}

// c-ref: jq emits datetime arrays as `[Y,m,d,H,M,S,wday,yday]`.
// moved-from: src/native_engine/vm_core/vm.rs::tm_to_jq_array
pub(crate) fn tm_to_numeric_fields_like_jq(tm: &libc::tm, fsecs: f64) -> [f64; 8] {
    let sec_value = (tm.tm_sec as f64) + (fsecs - fsecs.floor());
    [
        (tm.tm_year + 1900) as f64,
        tm.tm_mon as f64,
        tm.tm_mday as f64,
        tm.tm_hour as f64,
        tm.tm_min as f64,
        sec_value,
        tm.tm_wday as f64,
        tm.tm_yday as f64,
    ]
}

// c-ref: jq/src/builtin.c strptime handling computes wday/yday from y-m-d.
// moved-from: src/native_engine/vm_core/vm.rs::maybe_fill_tm_wday_yday
pub(crate) fn fill_tm_wday_yday(tm: &mut libc::tm) {
    if tm.tm_mday <= 0 || !(0..=11).contains(&tm.tm_mon) {
        return;
    }
    let year = tm.tm_year + 1900;
    let month = tm.tm_mon + 1;
    let day = tm.tm_mday;
    let days = days_from_civil(year, month, day);
    tm.tm_wday = ((days + 4).rem_euclid(7)) as i32;
    let yday = days - days_from_civil(year, 1, 1);
    tm.tm_yday = yday as i32;
}

// c-ref: jq man-tests use strict "%Y-%m-%dT%H:%M:%SZ" parsing path.
// moved-from: src/native_engine/vm_core/vm.rs::parse_iso8601_utc_seconds
pub(crate) fn parse_iso8601_utc_seconds_strict(input: &str) -> Option<i64> {
    let bytes = input.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return None;
    }

    let year = parse_u32_ascii(&input[0..4])? as i32;
    let month = parse_u32_ascii(&input[5..7])?;
    let day = parse_u32_ascii(&input[8..10])?;
    let hour = parse_u32_ascii(&input[11..13])?;
    let minute = parse_u32_ascii(&input[14..16])?;
    let second = parse_u32_ascii(&input[17..19])?;

    if !(1..=12).contains(&month) {
        return None;
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let days = days_from_civil(year, month as i32, day as i32);
    Some(
        days.saturating_mul(86_400)
            .saturating_add((hour as i64) * 3_600)
            .saturating_add((minute as i64) * 60)
            .saturating_add(second as i64),
    )
}

// c-ref: jq todateiso8601 uses UTC civil decomposition.
// moved-from: src/native_engine/vm_core/vm.rs::format_iso8601_utc_seconds
pub(crate) fn format_iso8601_utc_seconds(seconds: i64) -> String {
    let mut days = seconds.div_euclid(86_400);
    let mut rem = seconds.rem_euclid(86_400);
    if rem < 0 {
        rem += 86_400;
        days -= 1;
    }

    let (year, month, day) = civil_from_days(days);
    let hour = (rem / 3_600) as i32;
    rem %= 3_600;
    let minute = (rem / 60) as i32;
    let second = (rem % 60) as i32;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn parse_u32_ascii(text: &str) -> Option<u32> {
    if !text.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<u32>().ok()
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

// Howard Hinnant's civil date conversion algorithms (public domain).
fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let y = year - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era as i64) * 146_097 + (doe as i64) - 719_468
}

fn civil_from_days(days: i64) -> (i32, i32, i32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = (yoe as i32) + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32;
    let m = (mp + if mp < 10 { 3 } else { -9 }) as i32;
    y += if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

// c-ref: timestamp -> tm -> strftime pipeline
// moved-from: src/query_native.rs::format_timestamp_native
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn format_timestamp(
    timestamp: f64,
    format: &str,
    local: bool,
) -> Result<String, TimeFormatError> {
    let secs = match cast_time_t_trunc(timestamp) {
        Ok(value) => value,
        Err(TimeCastError::NonFinite) => return Err(TimeFormatError::NonFiniteTimestamp),
        Err(TimeCastError::OutOfRange) => return Err(TimeFormatError::TimestampOutOfRange),
    };
    let mut tm = if local {
        local_tm_from_seconds(secs).ok_or(TimeFormatError::ConvertTimestampFailed)?
    } else {
        utc_tm_from_seconds(secs).ok_or(TimeFormatError::ConvertTimestampFailed)?
    };
    format_tm_with_strftime(&mut tm, format, local)
}

#[cfg(target_vendor = "apple")]
struct ScopedUtcTz {
    previous: Option<std::ffi::OsString>,
}

#[cfg(target_vendor = "apple")]
impl ScopedUtcTz {
    fn enter() -> Self {
        let previous = std::env::var_os("TZ");
        std::env::set_var("TZ", "UTC");
        Self { previous }
    }
}

#[cfg(target_vendor = "apple")]
impl Drop for ScopedUtcTz {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            std::env::set_var("TZ", previous);
        } else {
            std::env::remove_var("TZ");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_tm_wday_yday_matches_unix_epoch_day() {
        let mut tm = zeroed_tm();
        tm.tm_year = 70;
        tm.tm_mon = 0;
        tm.tm_mday = 1;
        fill_tm_wday_yday(&mut tm);
        assert_eq!(tm.tm_wday, 4);
        assert_eq!(tm.tm_yday, 0);
    }

    #[test]
    fn strict_iso8601_parser_accepts_jq_shape() {
        let ts =
            parse_iso8601_utc_seconds_strict("1970-01-01T00:00:00Z").expect("must parse epoch");
        assert_eq!(ts, 0);
    }

    #[test]
    fn strict_iso8601_parser_rejects_non_jq_shape() {
        assert!(parse_iso8601_utc_seconds_strict("1970-01-01 00:00:00Z").is_none());
        assert!(parse_iso8601_utc_seconds_strict("1970-01-01T00:00:00+00:00").is_none());
    }

    #[test]
    fn strict_iso8601_formatter_matches_expected_shape() {
        assert_eq!(
            format_iso8601_utc_seconds(0),
            "1970-01-01T00:00:00Z".to_string()
        );
    }

    #[test]
    fn tm_numeric_fields_follow_jq_layout() {
        let tm = tm_from_numeric_fields_like_jq(&[1970.0, 0.0, 1.0, 0.0, 0.0, 0.0], false)
            .expect("tm from epoch fields");
        let fields = tm_to_numeric_fields_like_jq(&tm, 0.0);
        assert_eq!(fields[0], 1970.0);
        assert_eq!(fields[1], 0.0);
        assert_eq!(fields[2], 1.0);
        assert_eq!(fields[5], 0.0);
        assert_eq!(fields[6], 4.0);
        assert_eq!(fields[7], 0.0);
    }

    #[test]
    fn tm_numeric_fields_reject_nan_entries() {
        assert!(tm_from_numeric_fields_like_jq(&[f64::NAN], false).is_none());
    }

    #[test]
    fn tm_numeric_fields_reject_non_finite_and_extreme_entries() {
        assert!(tm_from_numeric_fields_like_jq(&[f64::INFINITY], false).is_none());
        assert!(tm_from_numeric_fields_like_jq(&[-f64::INFINITY], false).is_none());
        assert!(tm_from_numeric_fields_like_jq(&[2_000_000.0], false).is_none());
    }
}
