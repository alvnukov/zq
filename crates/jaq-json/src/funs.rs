use crate::{read, Error, Num, Tag, Val, ValR, ValX};
use alloc::{boxed::Box, vec::Vec};
use bstr::ByteSlice;
use bytes::{BufMut, Bytes, BytesMut};
use core::fmt;
use jaq_core::box_iter::{box_once, then, BoxIter};
use jaq_core::{DataT, Exn, Native, RunPtr};
use jaq_std::{bome, run, unary, v, Filter, ValT as _};
use num_traits::cast::ToPrimitive;

impl Val {
    /// Return 0 for null, the absolute value for numbers, and
    /// the length for strings, arrays, and objects.
    ///
    /// Fail on booleans.
    fn length(&self) -> ValR {
        match self {
            Val::Null => Ok(Val::from(0usize)),
            Val::Num(n) => Ok(Val::Num(n.length())),
            Val::Str(s, Tag::Utf8) => Ok(Val::from(s.chars().count() as isize)),
            Val::Str(b, Tag::Bytes) => Ok(Val::from(b.len() as isize)),
            Val::Arr(a) => Ok(Val::from(a.len() as isize)),
            Val::Obj(o) => Ok(Val::from(o.len() as isize)),
            Val::Bool(_) => Err(Error::str(format_args!("{self} has no length"))),
        }
    }

    /// Return the indices of `y` in `self`.
    fn indices<'a>(&'a self, y: &'a Val) -> Result<Box<dyn Iterator<Item = usize> + 'a>, Error> {
        match (self, y) {
            (Val::Str(_, tag @ (Tag::Bytes | Tag::Utf8)), Val::Str(y, tag_))
                if tag == tag_ && y.is_empty() =>
            {
                Ok(Box::new(core::iter::empty()))
            }
            (Val::Arr(_), Val::Arr(y)) if y.is_empty() => Ok(Box::new(core::iter::empty())),
            (Val::Str(x, Tag::Utf8), Val::Str(y, Tag::Utf8)) => {
                let index = |(i, _, _)| x.get(i..i + y.len());
                let iw = x.char_indices().map_while(index).enumerate();
                Ok(Box::new(iw.filter_map(|(i, w)| (w == *y).then_some(i))))
            }
            (Val::Str(x, tag @ Tag::Bytes), Val::Str(y, tag_)) if tag == tag_ => {
                let iw = x.windows(y.len()).enumerate();
                Ok(Box::new(iw.filter_map(|(i, w)| (w == *y).then_some(i))))
            }
            (Val::Arr(x), Val::Arr(y)) => {
                let iw = x.windows(y.len()).enumerate();
                Ok(Box::new(iw.filter_map(|(i, w)| (w == y.as_slice()).then_some(i))))
            }
            (Val::Arr(x), y) => {
                let ix = x.iter().enumerate();
                Ok(Box::new(ix.filter_map(move |(i, x)| (x == y).then_some(i))))
            }
            (x, y) => Err(Val::err_index(x, y)),
        }
    }

    /// Return true if `value | .[key]` is defined.
    ///
    /// Fail on values that are neither binaries, arrays nor objects.
    fn has(&self, key: &Self) -> Result<bool, Error> {
        match (self, key) {
            (Self::Str(a, Tag::Bytes), Self::Num(n)) => {
                let idx = match n {
                    Num::Float(f) if f.is_nan() => return Ok(false),
                    Num::Dec(s) if s.parse::<f64>().map_or(false, f64::is_nan) => return Ok(false),
                    _ => {
                        let mut idx = match n {
                            Num::Int(i) => *i as i64,
                            Num::BigInt(i) => i
                                .to_i64()
                                .unwrap_or_else(|| if i.sign() == num_bigint::Sign::Minus { i64::MIN } else { i64::MAX }),
                            Num::Float(f) => {
                                if *f < i64::MIN as f64 {
                                    i64::MIN
                                } else if *f > i64::MAX as f64 {
                                    i64::MAX
                                } else {
                                    f.trunc() as i64
                                }
                            }
                            Num::Dec(s) => {
                                let f = s.parse::<f64>().unwrap_or(f64::NAN);
                                if f.is_nan() {
                                    return Ok(false);
                                }
                                if f < i64::MIN as f64 {
                                    i64::MIN
                                } else if f > i64::MAX as f64 {
                                    i64::MAX
                                } else {
                                    f.trunc() as i64
                                }
                            }
                        };
                        if idx < 0 {
                            idx += a.len() as i64;
                        }
                        idx
                    }
                };
                Ok(idx >= 0 && (idx as usize) < a.len())
            }
            (Self::Arr(a), Self::Num(n)) => {
                let idx = match n {
                    Num::Float(f) if f.is_nan() => return Ok(false),
                    Num::Dec(s) if s.parse::<f64>().map_or(false, f64::is_nan) => return Ok(false),
                    _ => {
                        let mut idx = match n {
                            Num::Int(i) => *i as i64,
                            Num::BigInt(i) => i
                                .to_i64()
                                .unwrap_or_else(|| if i.sign() == num_bigint::Sign::Minus { i64::MIN } else { i64::MAX }),
                            Num::Float(f) => {
                                if *f < i64::MIN as f64 {
                                    i64::MIN
                                } else if *f > i64::MAX as f64 {
                                    i64::MAX
                                } else {
                                    f.trunc() as i64
                                }
                            }
                            Num::Dec(s) => {
                                let f = s.parse::<f64>().unwrap_or(f64::NAN);
                                if f.is_nan() {
                                    return Ok(false);
                                }
                                if f < i64::MIN as f64 {
                                    i64::MIN
                                } else if f > i64::MAX as f64 {
                                    i64::MAX
                                } else {
                                    f.trunc() as i64
                                }
                            }
                        };
                        if idx < 0 {
                            idx += a.len() as i64;
                        }
                        idx
                    }
                };
                Ok(idx >= 0 && (idx as usize) < a.len())
            }
            (Self::Obj(o), k) => Ok(o.contains_key(k)),
            _ => Err(Val::err_index(self, key)),
        }
    }

    /// `a` contains `b` iff either
    /// * the string `b` is a substring of `a`,
    /// * every element in the array `b` is contained in some element of the array `a`,
    /// * for every key-value pair `k, v` in `b`,
    ///   there is a key-value pair `k, v'` in `a` such that `v'` contains `v`, or
    /// * `a` equals `b`.
    fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Str(l, tag), Self::Str(r, tag_)) if tag == tag_ => l.contains_str(r),
            (Self::Arr(l), Self::Arr(r)) => r.iter().all(|r| l.iter().any(|l| l.contains(r))),
            (Self::Obj(l), Self::Obj(r)) => r
                .iter()
                .all(|(k, r)| l.get(k).is_some_and(|l| l.contains(r))),
            _ => self == other,
        }
    }

    fn flatten_native(self, depth: Option<&Self>) -> ValR {
        let depth = match depth {
            None => None,
            Some(v) => {
                let d = v
                    .as_f64()
                    .ok_or_else(|| Error::typ(v.clone(), "number"))?;
                if d < 0.0 {
                    return Err(Error::str("flatten depth must not be negative"));
                }
                Some(d)
            }
        };

        let arr = match self {
            Self::Arr(a) => a,
            v => return Err(Val::err_iter(&v)),
        };

        let mut out = Vec::new();
        let mut stack = Vec::new();
        for v in arr.iter().rev() {
            stack.push((v.clone(), depth));
        }

        while let Some((v, d)) = stack.pop() {
            if let Self::Arr(a) = &v {
                let should_flatten = d.map_or(true, |d| d != 0.0);
                if should_flatten {
                    let next = d.map(|d| d - 1.0);
                    for child in a.iter().rev() {
                        stack.push((child.clone(), next));
                    }
                    continue;
                }
            }
            out.push(v);
        }

        Ok(Val::from_iter(out))
    }

    fn to_bytes(&self) -> Result<Bytes, Self> {
        match self {
            Val::Num(n) => n
                .as_isize()
                .and_then(|i| u8::try_from(i).ok())
                .map(|u| Bytes::from(Vec::from([u])))
                .ok_or_else(|| self.clone()),
            Val::Str(b, _) => Ok(b.clone()),
            Val::Arr(a) => {
                let mut buf = BytesMut::new();
                for x in a.iter() {
                    buf.put(Val::to_bytes(x)?);
                }
                Ok(buf.into())
            }
            _ => Err(self.clone()),
        }
    }

    fn as_bytes_owned(&self) -> Option<Bytes> {
        if let Self::Str(b, _) = self {
            Some(b.clone())
        } else {
            None
        }
    }

    fn as_utf8_bytes_owned(&self) -> Option<Bytes> {
        self.is_utf8_str().then(|| self.as_bytes_owned()).flatten()
    }

    /// Return bytes if the value is a (byte or text) string.
    pub fn try_as_bytes_owned(&self) -> Result<Bytes, Error> {
        self.as_bytes_owned()
            .ok_or_else(|| Error::typ(self.clone(), "string"))
    }

    /// Return bytes if the value is a text string.
    pub fn try_as_utf8_bytes_owned(&self) -> Result<Bytes, Error> {
        self.as_utf8_bytes_owned()
            .ok_or_else(|| Error::typ(self.clone(), "string"))
    }
}

/// Box Map, Map Error.
fn bmme<'a>(iter: BoxIter<'a, ValR>) -> BoxIter<'a, ValX> {
    Box::new(iter.map(|r| r.map_err(Exn::from)))
}

fn parse_fail(i: &impl fmt::Display, fmt: &str, e: impl fmt::Display) -> Error {
    Error::str(format_args!("cannot parse {i} as {fmt}: {e}"))
}

fn parse_json_fail(input: &Val, e: read::Error) -> Error {
    if matches!(e.kind(), hifijson::Error::Depth) {
        return Error::str("Exceeds depth limit for parsing");
    }

    // jq reports a dedicated message for single-quoted object keys.
    if let Ok(bytes) = input.try_as_utf8_bytes_owned() {
        if let Ok(s) = core::str::from_utf8(&bytes) {
            if s.starts_with("{'") && s.contains("':") {
                let col = s.find(':').map_or(1, |i| i + 1);
                return Error::str(format_args!(
                    "Invalid string literal; expected \", but got ' at line 1, column {col} (while parsing '{s}')"
                ));
            }
        }
    }

    parse_fail(input, "JSON", e)
}

fn parse_jq_nan_json(input: &[u8]) -> Option<ValR> {
    let s = core::str::from_utf8(input).ok()?.trim();
    if matches!(s, "nan" | "NaN" | "-NaN" | "-nan") {
        return Some(Ok(Val::Num(Num::Float(f64::NAN))));
    }

    let payload = s
        .strip_prefix("NaN")
        .or_else(|| s.strip_prefix("-NaN"))
        .or_else(|| s.strip_prefix("nan"))
        .or_else(|| s.strip_prefix("-nan"))?;
    if payload.is_empty() {
        return None;
    }

    let col = s.len();
    Some(Err(Error::str(format_args!(
        "Invalid numeric literal at EOF at line 1, column {col} (while parsing '{s}')"
    ))))
}

fn is_delim(b: Option<u8>) -> bool {
    matches!(
        b,
        None
            | Some(b' ')
            | Some(b'\n')
            | Some(b'\r')
            | Some(b'\t')
            | Some(b',')
            | Some(b':')
            | Some(b'[')
            | Some(b']')
            | Some(b'{')
            | Some(b'}')
    )
}

fn normalize_non_finite_json_bytes(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0usize;
    let mut in_str = false;
    let mut escaped = false;

    while i < input.len() {
        let b = input[i];
        if in_str {
            out.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_str = false;
            }
            i += 1;
            continue;
        }

        if b == b'"' {
            in_str = true;
            out.push(b);
            i += 1;
            continue;
        }

        let rest = &input[i..];
        let prev = i.checked_sub(1).map(|p| input[p]);
        if rest.starts_with(b"NaN") && is_delim(prev) && is_delim(input.get(i + 3).copied()) {
            out.extend_from_slice(b"null");
            i += 3;
            continue;
        }
        if rest.starts_with(b"Infinity") && is_delim(prev) && is_delim(input.get(i + 8).copied())
        {
            out.extend_from_slice(b"null");
            i += 8;
            continue;
        }
        if rest.starts_with(b"-Infinity")
            && is_delim(prev)
            && is_delim(input.get(i + 9).copied())
        {
            out.extend_from_slice(b"null");
            i += 9;
            continue;
        }

        out.push(b);
        i += 1;
    }

    out
}

fn parse_tonumber_like_jq(input: &Val) -> ValR {
    fn is_int_literal(s: &str) -> bool {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return false;
        }
        let mut i = 0usize;
        if matches!(bytes[0], b'+' | b'-') {
            i = 1;
        }
        if i >= bytes.len() {
            return false;
        }
        if !bytes[i].is_ascii_digit() {
            return false;
        }
        bytes[i + 1..].iter().all(u8::is_ascii_digit)
    }

    match input {
        Val::Num(_) => Ok(input.clone()),
        Val::Str(s, _) => {
            let raw = core::str::from_utf8(s).map_err(Error::str)?;
            if raw.trim() != raw || raw.is_empty() {
                return Err(Error::str("cannot parse as number"));
            }

            if is_int_literal(raw) {
                return Ok(Val::Num(Num::from_str(raw)));
            }

            let f = raw.parse::<f64>().map_err(|_| Error::str("cannot parse as number"))?;
            Ok(Val::Num(Num::Float(f)))
        }
        _ => Err(Error::str("cannot parse as number")),
    }
}

self_cell::self_cell!(
    struct BytesValRs {
        owner: Bytes,

        #[not_covariant]
        dependent: ValRs,
    }
);

impl Iterator for BytesValRs {
    type Item = ValR;
    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_owner, iter| iter.next())
    }
}

type ValRs<'a> = BoxIter<'a, ValR>;

/// Apply a function to bytes and yield the resulting value results.
pub fn bytes_valrs(b: Bytes, f: impl FnOnce(&[u8]) -> ValRs) -> ValRs<'static> {
    Box::new(BytesValRs::new(b, |b| f(b)))
}

/// Functions of the standard library.
pub fn funs<D: for<'a> DataT<V<'a> = Val>>() -> impl Iterator<Item = Filter<Native<D>>> {
    base().into_vec().into_iter().map(run)
}

fn base<D: for<'a> DataT<V<'a> = Val>>() -> Box<[Filter<RunPtr<D>>]> {
    Box::new([
        ("__zq_tonumber_native", v(0), |cv| bome(parse_tonumber_like_jq(&cv.1))),
        ("__zq_getpath_native", v(1), |cv| {
            unary(cv, |root, path| root.getpath_native(&path))
        }),
        ("__zq_setpath_native", v(2), |mut cv| {
            let value = cv.0.pop_var();
            let path = cv.0.pop_var();
            bome(cv.1.setpath_native(&path, value))
        }),
        ("__zq_delpaths_native", v(1), |cv| {
            unary(cv, |root, paths| root.delpaths_native(&paths))
        }),
        ("fromjson", v(0), |cv| {
            let input = cv.1.clone();
            bmme(then(cv.1.try_as_utf8_bytes_owned(), |s| {
                let input = input.clone();
                bytes_valrs(s, move |s| {
                    if let Some(single) = parse_jq_nan_json(s) {
                        return box_once(single);
                    }
                    let fail = {
                        let input = input.clone();
                        move |r: Result<_, _>| r.map_err(|e| parse_json_fail(&input, e))
                    };
                    Box::new(read::parse_many(s).map(fail))
                })
            }))
        }),
        ("tojson", v(0), |cv| {
            let raw = cv.1.to_json();
            let normalized = normalize_non_finite_json_bytes(&raw);
            bome(Ok(Val::utf8_str(normalized)))
        }),
        ("tobytes", v(0), |cv| {
            let pass = |b| Val::Str(b, Tag::Bytes);
            let fail = |v| Error::str(format_args!("cannot convert {v} to bytes"));
            bome(cv.1.to_bytes().map(pass).map_err(fail))
        }),
        ("length", v(0), |cv| bome(cv.1.length())),
        ("contains", v(1), |cv| {
            unary(cv, |x, y| Ok(Val::from(x.contains(&y))))
        }),
        ("__zq_flatten_native", v(0), |cv| bome(cv.1.flatten_native(None))),
        ("__zq_flatten_native", v(1), |cv| {
            unary(cv, |x, d| x.flatten_native(Some(&d)))
        }),
        ("has", v(1), |cv| unary(cv, |v, k| v.has(&k).map(Val::from))),
        ("indices", v(1), |cv| {
            let to_int = |i: usize| Val::from(i as isize);
            unary(cv, move |x, v| {
                x.indices(&v).map(|idxs| idxs.map(to_int).collect())
            })
        }),
        ("bsearch", v(1), |cv| {
            let to_idx = |r: Result<_, _>| r.map_or_else(|i| -1 - i as isize, |i| i as isize);
            unary(cv, move |a, x| match a {
                Val::Arr(a) => Ok(Val::from(to_idx(a.binary_search(&x)))),
                v => Err(Val::err_search_from(&v)),
            })
        }),
    ])
}
