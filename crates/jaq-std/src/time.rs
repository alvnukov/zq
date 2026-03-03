use crate::{Error, ValR, ValT, ValTx};
use alloc::string::{String, ToString};
use jiff::{civil::DateTime, fmt::strtime, tz, Timestamp};
#[cfg(feature = "std")]
use std::process::Command;

/// Convert a UNIX epoch timestamp with optional fractions.
fn epoch_to_timestamp<V: ValT>(v: &V) -> Result<Timestamp, Error<V>> {
    let val = match v.as_isize() {
        Some(i) => i as i64 * 1000000,
        None => (v.try_as_f64()? * 1000000.0) as i64,
    };
    Timestamp::from_microsecond(val).map_err(Error::str)
}

/// Convert a date-time pair to a UNIX epoch timestamp.
fn timestamp_to_epoch<V: ValT>(ts: Timestamp, frac: bool) -> ValR<V> {
    if frac {
        Ok((ts.as_microsecond() as f64 / 1e6).into())
    } else {
        let seconds = ts.as_second();
        isize::try_from(seconds)
            .map(V::from)
            .or_else(|_| V::from_num(&seconds.to_string()))
    }
}

fn tm_num<V: ValT>(arr: &[V], idx: usize, default: f64) -> Option<f64> {
    let n = if let Some(v) = arr.get(idx) {
        v.as_f64()?
    } else {
        default
    };
    (!n.is_nan()).then_some(n)
}

fn array_to_datetime<V: ValT>(v: &[V]) -> Option<Result<DateTime, jiff::Error>> {
    // jq accepts partial "broken-down time" arrays:
    // [year, month, day, hour, min, sec, wday, yday]
    // with missing fields defaulting to 0.
    let year = tm_num(v, 0, 0.0)? as isize;
    let month0 = tm_num(v, 1, 0.0)? as isize;
    let day = tm_num(v, 2, 0.0)? as isize;
    let hour = tm_num(v, 3, 0.0)? as isize;
    let min = tm_num(v, 4, 0.0)? as isize;
    let sec = tm_num(v, 5, 0.0)?;

    let nanos = (sec.fract() * 1e9) as i32;
    Some(DateTime::new(
        year.try_into().ok()?,
        (month0 as i8) + 1,
        day.try_into().ok()?,
        hour.try_into().ok()?,
        min.try_into().ok()?,
        sec as i8,
        nanos,
    ))
}

/// Convert a `DateTime` to a "broken down time" array
fn datetime_to_array<V: ValT>(dt: DateTime) -> [V; 8] {
    [
        V::from(dt.year() as isize),
        V::from(dt.month() as isize - 1),
        V::from(dt.day() as isize),
        V::from(dt.hour() as isize),
        V::from(dt.minute() as isize),
        if dt.subsec_nanosecond() > 0 {
            V::from(dt.second() as f64 + dt.subsec_nanosecond() as f64 / 1e9)
        } else {
            V::from(dt.second() as isize)
        },
        V::from(dt.weekday().to_sunday_zero_offset() as isize),
        V::from(dt.day_of_year() as isize - 1),
    ]
}

/// Parse an ISO 8601 timestamp string to a number holding the equivalent UNIX timestamp
/// (seconds elapsed since 1970/01/01).
///
/// Actually, this parses RFC 3339; see
/// <https://ijmacd.github.io/rfc3339-iso8601/> for differences.
/// jq also only parses a very restricted subset of ISO 8601.
pub fn from_iso8601<V: ValT>(s: &str) -> ValR<V> {
    timestamp_to_epoch(s.parse().map_err(Error::str)?, s.contains('.'))
}

/// Format a number as an ISO 8601 timestamp string.
pub fn to_iso8601<V: ValT>(v: &V) -> Result<String, Error<V>> {
    let ts = if let Some(i) = v.as_isize() {
        Timestamp::from_second(i as i64)
    } else {
        Timestamp::from_microsecond((v.try_as_f64()? * 1e6) as i64)
    };
    Ok(ts.map_err(Error::str)?.to_string())
}

/// Format a date (either number or array) in a given timezone.
///
/// When the input is a "broken down time" array,
/// then it is assumed to be in the given timezone.
/// When the input is an integer, i.e. a Unix epoch,
/// then it is *converted* to the given timezone.
pub fn strftime<V: ValT>(name: &str, v: &V, fmt: &str, tz: tz::TimeZone) -> ValR<V> {
    let zoned = match v.clone().into_vec() {
        Ok(v) => array_to_datetime(&v)
            .ok_or_else(|| Error::str(format_args!("{name} requires parsed datetime inputs")))?
            .and_then(|dt| dt.to_zoned(tz))
            .map_err(Error::str)?,
        Err(_) => epoch_to_timestamp(v)?.to_zoned(tz),
    };
    strtime::format(fmt, &zoned)
        .map(V::from)
        .map_err(Error::str)
}

/// Convert an epoch timestamp to a "broken down time" array.
pub fn gmtime<V: ValT>(v: &V, tz: tz::TimeZone) -> ValR<V> {
    let dt = epoch_to_timestamp(v)?.to_zoned(tz).into();
    datetime_to_array(dt).into_iter().map(Ok).collect()
}

/// Parse a string into a "broken down time" array.
pub fn strptime<V: ValT>(s: &str, fmt: &str) -> ValR<V> {
    let mut bdt = match strtime::BrokenDownTime::parse(fmt, s) {
        Ok(v) => v,
        Err(primary_err) => {
            #[cfg(feature = "std")]
            if let Some(dt) = strptime_with_system_date(s, fmt) {
                return datetime_to_array(dt).into_iter().map(Ok).collect();
            }
            return Err(Error::str(primary_err));
        }
    };
    if (bdt.offset(), bdt.iana_time_zone()) == (None, None) {
        bdt.set_offset(Some(tz::Offset::UTC));
    }
    let dt = bdt.to_zoned().map_err(Error::str)?.into();
    datetime_to_array(dt).into_iter().map(Ok).collect()
}

#[cfg(feature = "std")]
fn strptime_with_system_date(s: &str, fmt: &str) -> Option<DateTime> {
    fn parse_out(out: &[u8]) -> Option<DateTime> {
        let text = String::from_utf8_lossy(out);
        let trimmed = text.trim();
        let (date, time) = trimmed.split_once(' ')?;
        let mut d = date.split('-');
        let year: i16 = d.next()?.parse().ok()?;
        let month: i8 = d.next()?.parse().ok()?;
        let day: i8 = d.next()?.parse().ok()?;
        let mut t = time.split(':');
        let hour: i8 = t.next()?.parse().ok()?;
        let min: i8 = t.next()?.parse().ok()?;
        let sec: i8 = t.next()?.parse().ok()?;
        DateTime::new(year, month, day, hour, min, sec, 0).ok()
    }

    // BSD/macOS date.
    if let Ok(out) = Command::new("date")
        .args(["-j", "-f", fmt, s, "+%Y-%m-%d %H:%M:%S"])
        .output()
    {
        if out.status.success() {
            if let Some(dt) = parse_out(&out.stdout) {
                return Some(dt);
            }
        }
    }

    // GNU date fallback.
    if let Ok(out) = Command::new("date")
        .args(["-d", s, "+%Y-%m-%d %H:%M:%S"])
        .output()
    {
        if out.status.success() {
            return parse_out(&out.stdout);
        }
    }

    None
}

/// Parse an array into a UNIX epoch timestamp.
pub fn mktime<V: ValT>(v: &V) -> ValR<V> {
    let input = v
        .clone()
        .into_vec()
        .map_err(|_| Error::str("mktime requires array inputs"))?;
    let ts = array_to_datetime(&input)
        .ok_or_else(|| Error::str("mktime requires parsed datetime inputs"))?
        .and_then(|dt| dt.to_zoned(tz::TimeZone::UTC))
        .map_err(Error::str)?
        .timestamp();
    timestamp_to_epoch(ts, ts.subsec_nanosecond() > 0)
}
