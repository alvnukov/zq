//! JSON superset with binary data and non-string object keys.
//!
//! This crate provides a few macros for formatting / writing;
//! this is done in order to function with both
//! [`core::fmt::Write`] and [`std::io::Write`].
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;
// even if this crate is `no_std`, it currently relies on `std`, because
// `to_json` has to go through the JSON writer to preserve invalid UTF-8
extern crate std;

mod funs;
mod num;
#[macro_use]
pub mod write;
pub mod read;

use alloc::{borrow::ToOwned, boxed::Box, string::String, vec, vec::Vec};
use bstr::{BStr, ByteSlice};
use bytes::{BufMut, Bytes, BytesMut};
use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::ops::{Deref, DerefMut};
use jaq_core::box_iter::box_once;
use jaq_core::{load, ops, path, val, Exn};
use jaq_core::ValT as _;
use num_bigint::Sign;
use num_traits::{cast::ToPrimitive, Signed};
use std::io::Write as _;

pub use funs::{bytes_valrs, funs};
pub use num::Num;

#[cfg(not(feature = "sync"))]
pub use alloc::rc::Rc;
#[cfg(feature = "sync")]
pub use alloc::sync::Arc as Rc;

#[cfg(feature = "serde")]
mod serde;

/// JSON superset with binary data and non-string object keys.
///
/// This is the default value type for jaq.
#[derive(Clone, Debug, Default)]
pub enum Val {
    #[default]
    /// Null
    Null,
    /// Boolean
    Bool(bool),
    /// Number
    Num(Num),
    /// String
    Str(Bytes, Tag),
    /// Array
    Arr(Rc<ArrayVals>),
    /// Object
    Obj(Rc<ObjectVals>),
}

/// Array payload used by [`Val::Arr`].
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArrayVals(Vec<Val>);

impl ArrayVals {
    fn take_inner(&mut self) -> Vec<Val> {
        core::mem::take(&mut self.0)
    }
}

impl Drop for ArrayVals {
    fn drop(&mut self) {
        drop_deep_vals(core::mem::take(&mut self.0));
    }
}

impl Deref for ArrayVals {
    type Target = Vec<Val>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArrayVals {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<Val>> for ArrayVals {
    fn from(v: Vec<Val>) -> Self {
        Self(v)
    }
}

impl IntoIterator for ArrayVals {
    type Item = Val;
    type IntoIter = alloc::vec::IntoIter<Val>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.take_inner().into_iter()
    }
}

/// Object payload used by [`Val::Obj`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectVals(Map<Val, Val>);

impl ObjectVals {
    fn take_inner(&mut self) -> Map<Val, Val> {
        core::mem::take(&mut self.0)
    }
}

impl Drop for ObjectVals {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        for (k, v) in core::mem::take(&mut self.0) {
            pending.push(k);
            pending.push(v);
        }
        drop_deep_vals(pending);
    }
}

impl Deref for ObjectVals {
    type Target = Map<Val, Val>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ObjectVals {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Map<Val, Val>> for ObjectVals {
    fn from(v: Map<Val, Val>) -> Self {
        Self(v)
    }
}

impl IntoIterator for ObjectVals {
    type Item = (Val, Val);
    type IntoIter = indexmap::map::IntoIter<Val, Val>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.take_inner().into_iter()
    }
}

fn drop_deep_vals(mut pending: Vec<Val>) {
    while let Some(v) = pending.pop() {
        match v {
            Val::Arr(a) => {
                if let Ok(mut a) = Rc::try_unwrap(a) {
                    pending.extend(core::mem::take(&mut a.0));
                }
            }
            Val::Obj(o) => {
                if let Ok(mut o) = Rc::try_unwrap(o) {
                    for (k, v) in core::mem::take(&mut o.0) {
                        pending.push(k);
                        pending.push(v);
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(feature = "sync")]
#[test]
fn val_send_sync() {
    fn send_sync<T: Send + Sync>(_: T) {}
    send_sync(Val::default())
}

/// Interpretation of a string.
///
/// This influences the outcome of a few operations (e.g. slicing)
/// as well as how a string is printed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Tag {
    /// Sequence of bytes, to be escaped
    Bytes,
    /// Sequence of UTF-8 code points
    ///
    /// Note that this does not require the actual bytes to be all valid UTF-8;
    /// this just means that the bytes are interpreted as UTF-8.
    /// An effort is made to preserve invalid UTF-8 as is, else
    /// replace invalid UTF-8 by the Unicode replacement character.
    Utf8,
}

/// Types and sets of types.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Type {
    /// `[] | .["a"]` or `limit("a"; 0)` or `range(0; "a")`
    Int,
    /*
    /// `"1" | sin` or `pow(2; "3")` or `fma(2; 3; "4")`
    Float,
    */
    /// `-"a"`, `"a" | round`
    Num,
    /// `0 | sort` or `0 | implode` or `[] | .[0:] = 0`
    Arr,
    /// `0 | .[]` or `0 | .[0]` or `0 | keys` (array or object)
    Iter,
    /// `{}[0:1]` (string or array)
    Range,
}

impl Type {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Int => "integer",
            //Self::Float => "floating-point number",
            Self::Num => "number",
            Self::Arr => "array",
            Self::Iter => "iterable (array or object)",
            Self::Range => "rangeable (array or string)",
        }
    }
}

/// Order-preserving map
pub type Map<K = Val, V = K> = indexmap::IndexMap<K, V, foldhash::fast::RandomState>;

/// Error that can occur during filter execution.
pub type Error = jaq_core::Error<Val>;
/// A value or an eRror.
pub type ValR = jaq_core::ValR<Val>;
/// A value or an eXception.
pub type ValX = jaq_core::ValX<Val>;

// This is part of the Rust standard library since 1.76:
// <https://doc.rust-lang.org/std/rc/struct.Rc.html#method.unwrap_or_clone>.
// However, to keep MSRV low, we reimplement it here.
fn rc_unwrap_or_clone<T: Clone>(a: Rc<T>) -> T {
    Rc::try_unwrap(a).unwrap_or_else(|a| (*a).clone())
}

impl jaq_core::ValT for Val {
    fn from_num(n: &str) -> ValR {
        Ok(Self::Num(Num::from_str(n)))
    }

    fn from_map<I: IntoIterator<Item = (Self, Self)>>(iter: I) -> ValR {
        Ok(Self::obj(iter.into_iter().collect()))
    }

    fn key_values(self) -> Box<dyn Iterator<Item = Result<(Val, Val), Error>>> {
        let arr_idx = |(i, x)| Ok((Self::from(i as isize), x));
        match self {
            Self::Arr(a) => Box::new(rc_unwrap_or_clone(a).into_iter().enumerate().map(arr_idx)),
            Self::Obj(o) => Box::new(rc_unwrap_or_clone(o).into_iter().map(Ok)),
            _ => box_once(Err(Self::err_iter(&self))),
        }
    }

    fn values(self) -> Box<dyn Iterator<Item = ValR>> {
        match self {
            Self::Arr(a) => Box::new(rc_unwrap_or_clone(a).into_iter().map(Ok)),
            Self::Obj(o) => Box::new(rc_unwrap_or_clone(o).into_iter().map(|(_k, v)| Ok(v))),
            _ => box_once(Err(Self::err_iter(&self))),
        }
    }

    fn index(self, index: &Self) -> ValR {
        match (self, index) {
            (Val::Null, _) => Ok(Val::Null),
            (Val::Str(a, Tag::Bytes), Val::Num(i @ (Num::Int(_) | Num::BigInt(_)))) => Ok(i
                .as_pos_usize()
                .and_then(|i| abs_index(i, a.len()))
                .map_or(Val::Null, |i| Val::from(a[i] as usize))),
            (Val::Arr(a), i @ Val::Num(_)) => match i.as_array_index()? {
                ArrayIndex::NaN => Ok(Val::Null),
                ArrayIndex::Index(i) => Ok(abs_index(i, a.len()).map_or(Val::Null, |i| a[i].clone())),
            },
            (v @ (Val::Str(..) | Val::Arr(_)), Val::Obj(o)) => {
                v.range(o.get(&Val::utf8_str("start"))..o.get(&Val::utf8_str("end")))
            }
            (Val::Obj(o), i @ Val::Str(..)) => Ok(o.get(i).cloned().unwrap_or(Val::Null)),
            (s @ Val::Obj(_), i) => Err(Self::err_index(&s, i)),
            (s @ (Val::Str(..) | Val::Arr(_)), _) => Err(Self::err_index(&s, index)),
            (s, _) => Err(Self::err_index(&s, index)),
        }
    }

    fn range(self, range: val::Range<&Self>) -> ValR {
        match self {
            Val::Null => Ok(Val::Null),
            Val::Str(s, t) => Self::range_int(range)
                .map(|range_char| skip_take_str(t, range_char, &s))
                .map(|(skip, take)| Val::Str(s.slice(skip..skip + take), t)),
            Val::Arr(a) => Self::range_int(range)
                .map(|range| skip_take(range, a.len()))
                .map(|(skip, take)| a.iter().skip(skip).take(take).cloned().collect()),
            _ => Err(Error::typ(self, Type::Range.as_str())),
        }
    }

    fn map_values<I: Iterator<Item = ValX>>(self, opt: path::Opt, f: impl Fn(Self) -> I) -> ValX {
        match self {
            Self::Arr(a) => {
                let iter = rc_unwrap_or_clone(a).into_iter().flat_map(f);
                Ok(iter.collect::<Result<_, _>>()?)
            }
            Self::Obj(o) => {
                let iter = rc_unwrap_or_clone(o).into_iter();
                let iter = iter.filter_map(|(k, v)| f(v).next().map(|v| Ok((k, v?))));
                Ok(Self::obj(iter.collect::<Result<_, Exn<_>>>()?))
            }
            v => opt.fail(v, |v| Exn::from(Self::err_iter(&v))),
        }
    }

    fn map_index<I: Iterator<Item = ValX>>(
        mut self,
        index: &Self,
        opt: path::Opt,
        f: impl Fn(Self) -> I,
    ) -> ValX {
        if let (Val::Str(..) | Val::Arr(_), Val::Obj(o)) = (&self, index) {
            let range = o.get(&Val::utf8_str("start"))..o.get(&Val::utf8_str("end"));
            return self.map_range(range, opt, f);
        };
        match self {
            Val::Null => match index {
                Val::Str(..) => Val::obj(Map::default()).map_index(index, opt, f),
                Val::Num(_) => Val::Arr(Rc::new(ArrayVals::default())).map_index(index, opt, f),
                _ => opt.fail(self, |v| Exn::from(Self::err_index(&v, index))),
            },
            Val::Obj(ref mut o) => {
                if !matches!(index, Val::Str(..)) {
                    return opt.fail(self, |v| Exn::from(Self::err_index(&v, index)));
                }
                use indexmap::map::Entry::{Occupied, Vacant};
                match Rc::make_mut(o).entry(index.clone()) {
                    Occupied(mut e) => {
                        let v = core::mem::take(e.get_mut());
                        match f(v).next().transpose()? {
                            Some(y) => e.insert(y),
                            // this runs in constant time, at the price of
                            // changing the order of the elements
                            None => e.swap_remove(),
                        };
                    }
                    Vacant(e) => {
                        if let Some(y) = f(Val::Null).next().transpose()? {
                            e.insert(y);
                        }
                    }
                }
                Ok(self)
            }
            Val::Arr(ref mut a) => {
                let idx = match index.as_array_index() {
                    Ok(ArrayIndex::Index(i)) => i,
                    Ok(ArrayIndex::NaN) => {
                        if f(Val::Null).next().transpose()?.is_none() {
                            return Ok(self);
                        }
                        return opt.fail(self, |_| Exn::from(Error::str("Cannot set array element at NaN index")));
                    }
                    Err(e) => return opt.fail(self, |_| Exn::from(e)),
                };

                let len = a.len();
                let i = if idx.0 {
                    idx.1
                } else {
                    match len.checked_sub(idx.1) {
                        Some(i) if i < len => i,
                        _ => {
                            if f(Val::Null).next().transpose()?.is_none() {
                                return Ok(self);
                            }
                            return opt.fail(self, |_| Exn::from(Error::str("Out of bounds negative array index")));
                        }
                    }
                };

                if idx.0 && i >= len {
                    const MAX_AUTOGROW_INDEX: usize = 1 << 20;
                    if i > MAX_AUTOGROW_INDEX {
                        if f(Val::Null).next().transpose()?.is_none() {
                            return Ok(self);
                        }
                        return opt.fail(self, |_| Exn::from(Error::str("Array index too large")));
                    }

                    let a = Rc::make_mut(a);
                    let Some(y) = f(Val::Null).next().transpose()? else {
                        return Ok(self);
                    };
                    if i > a.len() {
                        a.resize(i, Val::Null);
                    }
                    if i == a.len() {
                        a.push(y);
                    } else {
                        a[i] = y;
                    }
                    return Ok(self);
                }

                let a = Rc::make_mut(a);
                let x = core::mem::take(&mut a[i]);
                if let Some(y) = f(x).next().transpose()? {
                    a[i] = y;
                } else {
                    a.remove(i);
                }
                Ok(self)
            }
            _ => opt.fail(self, |v| Exn::from(Self::err_index(&v, index))),
        }
    }

    fn map_range<I: Iterator<Item = ValX>>(
        mut self,
        range: val::Range<&Self>,
        opt: path::Opt,
        f: impl Fn(Self) -> I,
    ) -> ValX {
        match self {
            Val::Arr(ref mut a) => {
                let (skip, take) = match Self::range_int(range) {
                    Ok(range) => skip_take(range, a.len()),
                    Err(e) => return opt.fail(self, |_| Exn::from(e)),
                };
                let arr = a.iter().skip(skip).take(take).cloned().collect();
                let y = f(arr).map(|y| y?.into_arr().map_err(Exn::from)).next();
                let y = y.transpose()?.unwrap_or_default();
                Rc::make_mut(a).splice(skip..skip + take, y.iter().cloned());
                Ok(self)
            }
            Val::Str(_, _) => opt.fail(self, |_| Exn::from(Error::str("Cannot update string slices"))),
            _ => opt.fail(self, |v| Exn::from(Error::typ(v, Type::Arr.as_str()))),
        }
    }

    fn recurse_update<'a, F>(self, f: F) -> jaq_core::ValXs<'a, Self>
    where
        Self: 'a,
        F: Fn(Self) -> jaq_core::ValXs<'a, Self> + Clone + 'a,
    {
        enum Frame {
            Eval(Val),
            Arr {
                rest: alloc::vec::IntoIter<Val>,
                acc: Vec<Val>,
            },
            Obj {
                rest: indexmap::map::IntoIter<Val, Val>,
                acc: Map<Val, Val>,
                key: Val,
            },
            Apply(Val),
        }

        let mut stack = Vec::from([Frame::Eval(self)]);
        let mut last: Option<Vec<ValX>> = None;

        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Eval(v) => match v {
                    Val::Arr(a) => {
                        let mut rest = rc_unwrap_or_clone(a).into_iter();
                        if let Some(child) = rest.next() {
                            stack.push(Frame::Arr {
                                rest,
                                acc: Vec::new(),
                            });
                            stack.push(Frame::Eval(child));
                        } else {
                            stack.push(Frame::Apply(Val::Arr(Rc::new(ArrayVals::default()))));
                        }
                    }
                    Val::Obj(o) => {
                        let mut rest = rc_unwrap_or_clone(o).into_iter();
                        if let Some((key, child)) = rest.next() {
                            stack.push(Frame::Obj {
                                rest,
                                acc: Map::default(),
                                key,
                            });
                            stack.push(Frame::Eval(child));
                        } else {
                            stack.push(Frame::Apply(Val::Obj(Rc::new(ObjectVals::default()))));
                        }
                    }
                    scalar => stack.push(Frame::Apply(scalar)),
                },
                Frame::Arr { mut rest, mut acc } => {
                    let child_outputs = last.take().unwrap_or_default();
                    let mut failed = false;
                    for out in child_outputs {
                        match out {
                            Ok(v) => acc.push(v),
                            Err(e) => {
                                last = Some(Vec::from([Err(e)]));
                                failed = true;
                                break;
                            }
                        }
                    }
                    if failed {
                        continue;
                    }

                    if let Some(child) = rest.next() {
                        stack.push(Frame::Arr { rest, acc });
                        stack.push(Frame::Eval(child));
                    } else {
                        stack.push(Frame::Apply(Val::Arr(Rc::new(ArrayVals::from(acc)))));
                    }
                }
                Frame::Obj { mut rest, mut acc, key } => {
                    let child_outputs = last.take().unwrap_or_default();
                    match child_outputs.into_iter().next() {
                        Some(Err(e)) => {
                            last = Some(Vec::from([Err(e)]));
                            continue;
                        }
                        Some(Ok(v)) => {
                            acc.insert(key, v);
                        }
                        None => {}
                    }

                    if let Some((next_key, next_child)) = rest.next() {
                        stack.push(Frame::Obj {
                            rest,
                            acc,
                            key: next_key,
                        });
                        stack.push(Frame::Eval(next_child));
                    } else {
                        stack.push(Frame::Apply(Val::Obj(Rc::new(ObjectVals::from(acc)))));
                    }
                }
                Frame::Apply(v) => {
                    last = Some(f.clone()(v).collect());
                }
            }
        }

        Box::new(last.unwrap_or_default().into_iter())
    }

    /// True if the value is neither null nor false.
    fn as_bool(&self) -> bool {
        !matches!(self, Self::Null | Self::Bool(false))
    }

    fn into_string(self) -> Self {
        if let Self::Str(b, _tag) = self {
            Self::utf8_str(b)
        } else {
            Self::utf8_str(self.to_json())
        }
    }
}

impl jaq_std::ValT for Val {
    fn into_seq<S: FromIterator<Self>>(self) -> Result<S, Self> {
        match self {
            Self::Arr(a) => match Rc::try_unwrap(a) {
                Ok(a) => Ok(a.into_iter().collect()),
                Err(a) => Ok(a.iter().cloned().collect()),
            },
            _ => Err(self),
        }
    }

    fn is_int(&self) -> bool {
        self.as_num().is_some_and(Num::is_int)
    }

    fn as_isize(&self) -> Option<isize> {
        self.as_num().and_then(Num::as_isize)
    }

    fn as_f64(&self) -> Option<f64> {
        self.as_num().map(Num::as_f64)
    }

    fn is_utf8_str(&self) -> bool {
        matches!(self, Self::Str(_, Tag::Utf8))
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Str(b, _) = self {
            Some(b)
        } else {
            None
        }
    }

    fn as_sub_str(&self, sub: &[u8]) -> Self {
        match self {
            Self::Str(b, tag) => Self::Str(b.slice_ref(sub), *tag),
            _ => panic!(),
        }
    }

    fn from_utf8_bytes(b: impl AsRef<[u8]> + Send + 'static) -> Self {
        Self::Str(Bytes::from_owner(b), Tag::Utf8)
    }
}

/// Definitions of the standard library.
pub fn defs() -> impl Iterator<Item = load::parse::Def<&'static str>> {
    load::parse(include_str!("defs.jq"), |p| p.defs())
        .unwrap()
        .into_iter()
}

fn skip_take(range: val::Range<num::PosUsize>, len: usize) -> (usize, usize) {
    let from = abs_bound(range.start, len, 0);
    let upto = abs_bound(range.end, len, len);
    (from, upto.saturating_sub(from))
}

fn skip_take_str(tag: Tag, range: val::Range<num::PosUsize>, b: &[u8]) -> (usize, usize) {
    let byte_index = |num::PosUsize(pos, c)| {
        let mut chars = b.char_indices().map(|(start, ..)| start);
        if pos {
            chars.nth(c).unwrap_or(b.len())
        } else {
            chars.nth_back(c - 1).unwrap_or(0)
        }
    };
    match tag {
        Tag::Bytes => skip_take(range, b.len()),
        Tag::Utf8 => {
            let from_byte = range.start.map_or(0, byte_index);
            let upto_byte = range.end.map_or(b.len(), byte_index);
            (from_byte, upto_byte.saturating_sub(from_byte))
        }
    }
}

/// If a range bound is given, absolutise and clip it between 0 and `len`,
/// else return `default`.
fn abs_bound(i: Option<num::PosUsize>, len: usize, default: usize) -> usize {
    i.map_or(default, |i| core::cmp::min(i.wrap(len).unwrap_or(0), len))
}

/// Absolutise an index and return result if it is inside [0, len).
fn abs_index(i: num::PosUsize, len: usize) -> Option<usize> {
    i.wrap(len).filter(|i| *i < len)
}

impl Val {
    /// Construct an object value.
    pub fn obj(m: Map) -> Self {
        Self::Obj(Rc::new(ObjectVals::from(m)))
    }

    /// Construct a string that is interpreted as UTF-8.
    pub fn utf8_str(s: impl Into<Bytes>) -> Self {
        Self::Str(s.into(), Tag::Utf8)
    }

    /// Construct a string that is interpreted as bytes.
    pub fn byte_str(s: impl Into<Bytes>) -> Self {
        Self::Str(s.into(), Tag::Bytes)
    }

    fn as_num(&self) -> Option<&Num> {
        match self {
            Self::Num(n) => Some(n),
            _ => None,
        }
    }

    /// Coerce a value to an array index similarly to jq:
    /// finite floats are truncated, NaN is handled separately.
    fn as_array_index(&self) -> Result<ArrayIndex, Error> {
        let fail = || Error::typ(self.clone(), Type::Int.as_str());
        let Some(num) = self.as_num() else {
            return Err(fail());
        };
        if num_is_nan(num) {
            return Ok(ArrayIndex::NaN);
        }
        num_to_pos_usize(num, BoundRound::Trunc)
            .map(ArrayIndex::Index)
            .ok_or_else(fail)
    }

    /// If the value is an array, return it, else fail.
    fn into_arr(self) -> Result<Rc<ArrayVals>, Error> {
        match self {
            Self::Arr(a) => Ok(a),
            _ => Err(Error::typ(self, Type::Arr.as_str())),
        }
    }

    fn as_arr(&self) -> Result<&Rc<ArrayVals>, Error> {
        match self {
            Self::Arr(a) => Ok(a),
            _ => Err(Error::typ(self.clone(), Type::Arr.as_str())),
        }
    }

    fn range_int(range: val::Range<&Self>) -> Result<val::Range<num::PosUsize>, Error> {
        let f = |i: Option<&Self>, round| {
            let Some(i) = i else {
                return Ok(None);
            };
            if matches!(i, Val::Null) {
                return Ok(None);
            }
            let Some(num) = i.as_num() else {
                return Err(Error::typ(i.clone(), Type::Int.as_str()));
            };
            if num_is_nan(num) {
                return Ok(None);
            }
            num_to_pos_usize(num, round)
                .ok_or_else(|| Error::typ(i.clone(), Type::Int.as_str()))
                .map(Some)
        };
        Ok(f(range.start, BoundRound::Floor)?..f(range.end, BoundRound::Ceil)?)
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "boolean",
            Self::Num(_) => "number",
            Self::Str(..) => "string",
            Self::Arr(_) => "array",
            Self::Obj(_) => "object",
        }
    }

    fn jq_dump_string_trunc(&self, bufsize: usize) -> String {
        debug_assert!(bufsize > 0);
        let dumped = self.to_json();
        let dumped_len = dumped.len();
        let max = bufsize.saturating_sub(1);
        let mut out = if dumped.len() > max {
            dumped[..max].to_vec()
        } else {
            dumped
        };
        if dumped_len > max && bufsize >= 4 && out.len() >= 3 {
            let n = out.len();
            out[n - 1] = b'.';
            out[n - 2] = b'.';
            out[n - 3] = b'.';
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn jq_value_repr(&self) -> String {
        // jq's type_error paths use jv_dump_string_trunc() with a 15-byte buffer.
        // Keep this width to preserve jq-compatible error messages.
        self.jq_dump_string_trunc(15)
    }

    fn jq_typed_value(&self) -> String {
        alloc::format!("{} ({})", self.kind_name(), self.jq_value_repr())
    }

    fn math_verb(op: ops::Math) -> &'static str {
        match op {
            ops::Math::Add => "added",
            ops::Math::Sub => "subtracted",
            ops::Math::Mul => "multiplied",
            ops::Math::Div => "divided",
            ops::Math::Rem => "divided (remainder)",
        }
    }

    pub(crate) fn err_index(left: &Self, right: &Self) -> Error {
        Error::str(alloc::format!(
            "Cannot index {} with {} ({})",
            left.kind_name(),
            right.kind_name(),
            right.jq_value_repr()
        ))
    }

    pub(crate) fn err_iter(v: &Self) -> Error {
        Error::str(alloc::format!("Cannot iterate over {}", v.jq_typed_value()))
    }

    pub(crate) fn err_neg(v: &Self) -> Error {
        Error::str(alloc::format!("{} cannot be negated", v.jq_typed_value()))
    }

    pub(crate) fn err_math(left: &Self, op: ops::Math, right: &Self) -> Error {
        Error::str(alloc::format!(
            "{} and {} cannot be {}",
            left.jq_typed_value(),
            right.jq_typed_value(),
            Self::math_verb(op)
        ))
    }

    pub(crate) fn err_search_from(v: &Self) -> Error {
        Error::str(alloc::format!("{} cannot be searched from", v.jq_typed_value()))
    }

    fn set_direct(self, key: &Self, value: Self) -> ValR {
        let out = self.map_index(key, path::Opt::Essential, move |_| {
            core::iter::once(Ok(value.clone()))
        });
        val::unwrap_valr(out)
    }

    fn parse_slice_indices_for_len(len: usize, slice: &Self) -> Result<(usize, usize), Error> {
        let Some(obj) = (match slice {
            Self::Obj(o) => Some(o),
            _ => None,
        }) else {
            return Err(Error::str("Array/string slice indices must be integers"));
        };

        let start = obj
            .get(&Self::utf8_str("start"))
            .cloned()
            .unwrap_or(Self::Null);
        let end = obj
            .get(&Self::utf8_str("end"))
            .cloned()
            .unwrap_or(Self::Null);

        let to_f64 = |v: Self| -> Result<Option<f64>, Error> {
            match v {
                Self::Null => Ok(None),
                Self::Num(n) => Ok(Some(n.as_f64())),
                _ => Err(Error::str("Array/string slice indices must be integers")),
            }
        };

        let len_f = len as f64;
        let mut dstart = to_f64(start)?.unwrap_or(0.0);
        let mut dend = to_f64(end)?.unwrap_or(len_f);

        if dstart.is_nan() {
            dstart = 0.0;
        }
        if dstart < 0.0 {
            dstart += len_f;
        }
        if dstart < 0.0 {
            dstart = 0.0;
        }
        if dstart > len_f {
            dstart = len_f;
        }
        let mut start_idx = dstart.floor();
        if start_idx > len_f {
            start_idx = len_f;
        }
        let start = start_idx as usize;

        if dend.is_nan() {
            dend = len_f;
        }
        if dend < 0.0 {
            dend += len_f;
        }
        if dend < 0.0 {
            dend = start as f64;
        }
        let mut end = if dend > usize::MAX as f64 {
            usize::MAX
        } else {
            dend.trunc() as usize
        };
        if end > len {
            end = len;
        }
        if end < len && (end as f64) < dend {
            end += 1;
        }
        if end < start {
            end = start;
        }
        Ok((start, end))
    }

    pub(crate) fn getpath_native(self, path: &Self) -> ValR {
        let path = match path {
            Self::Arr(a) => a.clone(),
            _ => return Err(Error::str("Path must be specified as an array")),
        };
        let mut cur = self;
        for p in path.iter() {
            cur = cur.index(p)?;
        }
        Ok(cur)
    }

    pub(crate) fn setpath_native(self, path: &Self, value: Self) -> ValR {
        let path = match path {
            Self::Arr(a) => a.clone(),
            _ => return Err(Error::str("Path must be specified as an array")),
        };

        fn rec(root: Val, path: &[Val], value: Val) -> ValR {
            if path.is_empty() {
                return Ok(value);
            }
            let head = path[0].clone();
            if matches!((&root, &head), (Val::Arr(_), Val::Arr(_))) {
                return Err(Error::str("Cannot update field at array index of array"));
            }
            let sub = root.clone().index(&head)?;
            let sub = rec(sub, &path[1..], value)?;
            root.set_direct(&head, sub)
        }

        rec(self, path.as_slice(), value)
    }

    fn delpaths_sorted(mut object: Self, paths: &[Vec<Self>], start: usize) -> ValR {
        let mut delkeys = Vec::new();
        let mut i = 0usize;

        while i < paths.len() {
            if paths[i].len() <= start {
                i += 1;
                continue;
            }

            let delkey = paths[i].len() == start + 1;
            let key = paths[i][start].clone();

            let mut j = i + 1;
            while j < paths.len() && paths[j].len() > start && paths[j][start] == key {
                j += 1;
            }

            if delkey {
                delkeys.push(key);
            } else {
                let sub = object.clone().index(&key)?;
                if !matches!(sub, Self::Null) {
                    let new_sub = Self::delpaths_sorted(sub, &paths[i..j], start + 1)?;
                    object = object.set_direct(&key, new_sub)?;
                }
            }

            i = j;
        }

        object.dels(delkeys)
    }

    fn dels(self, keys: Vec<Self>) -> ValR {
        if keys.is_empty() || matches!(self, Self::Null) {
            return Ok(self);
        }

        match self {
            Self::Arr(a) => {
                let len = a.len();
                let mut deleted = vec![false; len];

                for key in keys {
                    match key {
                        Self::Num(n) => {
                            if num_is_nan(&n) {
                                continue;
                            }
                            let mut idx = num_to_i64_trunc_saturated(&n);
                            if idx < 0 {
                                idx += len as i64;
                            }
                            if idx >= 0 && (idx as usize) < len {
                                deleted[idx as usize] = true;
                            }
                        }
                        k @ Self::Obj(_) => {
                            let (start, end) = Self::parse_slice_indices_for_len(len, &k)?;
                            for slot in deleted.iter_mut().take(end).skip(start) {
                                *slot = true;
                            }
                        }
                        k => {
                            return Err(Error::str(format_args!(
                                "Cannot delete {} element of array",
                                k.kind_name()
                            )));
                        }
                    }
                }

                let mut out = Vec::with_capacity(len);
                for (i, v) in a.iter().cloned().enumerate() {
                    if !deleted[i] {
                        out.push(v);
                    }
                }
                Ok(Self::Arr(Rc::new(ArrayVals::from(out))))
            }
            Self::Obj(mut o) => {
                for key in keys {
                    if !matches!(key, Self::Str(..)) {
                        return Err(Error::str(format_args!(
                            "Cannot delete {} field of object",
                            key.kind_name()
                        )));
                    }
                    Rc::make_mut(&mut o).shift_remove(&key);
                }
                Ok(Self::Obj(o))
            }
            v => Err(Error::str(format_args!(
                "Cannot delete fields from {}",
                v.kind_name()
            ))),
        }
    }

    pub(crate) fn delpaths_native(self, paths: &Self) -> ValR {
        let paths = match paths {
            Self::Arr(a) => a.clone(),
            _ => return Err(Error::str("Paths must be specified as an array")),
        };

        let mut ps = Vec::<Vec<Self>>::with_capacity(paths.len());
        for p in paths.iter() {
            let Self::Arr(a) = p else {
                return Err(Error::str(format_args!(
                    "Path must be specified as array, not {}",
                    p.kind_name()
                )));
            };
            ps.push(a.iter().cloned().collect());
        }

        if ps.is_empty() {
            return Ok(self);
        }

        ps.sort();
        if ps.first().is_some_and(|p| p.is_empty()) {
            return Ok(Self::Null);
        }

        Self::delpaths_sorted(self, &ps, 0)
    }

    /// Serialize the value into compact JSON-superset text.
    ///
    /// This writer is stack-safe for deeply nested arrays/objects.
    pub fn to_json(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_json_stack_safe(&mut buf, self).unwrap();
        buf
    }
}

enum ArrayIndex {
    Index(num::PosUsize),
    NaN,
}

#[derive(Copy, Clone)]
enum BoundRound {
    Trunc,
    Floor,
    Ceil,
}

fn num_is_nan(n: &Num) -> bool {
    match n {
        Num::Float(f) => f.is_nan(),
        Num::Dec(s) => s.parse::<f64>().map_or(false, f64::is_nan),
        _ => false,
    }
}

fn num_to_pos_usize(n: &Num, round: BoundRound) -> Option<num::PosUsize> {
    let round_f64 = |f: f64, round: BoundRound| -> Option<num::PosUsize> {
        if f.is_nan() {
            return None;
        }
        if !f.is_finite() {
            return Some(num::PosUsize(f.is_sign_positive(), usize::MAX));
        }
        let t = match round {
            BoundRound::Trunc => f.trunc(),
            BoundRound::Floor => f.floor(),
            BoundRound::Ceil => f.ceil(),
        };
        let pos = t.is_sign_positive() || t == 0.0;
        let abs = t.abs();
        let mag = if abs > usize::MAX as f64 {
            usize::MAX
        } else {
            abs as usize
        };
        Some(num::PosUsize(pos, mag))
    };

    match n {
        Num::Int(i) => Some(num::PosUsize(*i >= 0, i.unsigned_abs())),
        Num::BigInt(i) => Some(num::PosUsize(
            i.sign() != Sign::Minus,
            i.magnitude().to_usize().unwrap_or(usize::MAX),
        )),
        Num::Float(f) => round_f64(*f, round),
        Num::Dec(s) => round_f64(s.parse::<f64>().ok()?, round),
    }
}

fn num_to_i64_trunc_saturated(n: &Num) -> i64 {
    match n {
        Num::Int(i) => *i as i64,
        Num::BigInt(i) => i
            .to_i64()
            .unwrap_or_else(|| if i.is_negative() { i64::MIN } else { i64::MAX }),
        Num::Float(f) => {
            if f.is_nan() {
                0
            } else if *f < i64::MIN as f64 {
                i64::MIN
            } else if *f > i64::MAX as f64 {
                i64::MAX
            } else {
                f.trunc() as i64
            }
        }
        Num::Dec(s) => s
            .parse::<f64>()
            .ok()
            .map(|f| num_to_i64_trunc_saturated(&Num::Float(f)))
            .unwrap_or(0),
    }
}

fn write_json_scalar(out: &mut Vec<u8>, v: &Val) -> std::io::Result<()> {
    match v {
        Val::Null => out.write_all(b"null"),
        Val::Bool(true) => out.write_all(b"true"),
        Val::Bool(false) => out.write_all(b"false"),
        Val::Num(n) => write!(out, "{n}"),
        Val::Str(..) => write::write(out, &write::Pp::default(), 0, v),
        Val::Arr(_) | Val::Obj(_) => unreachable!("container handled by stack machine"),
    }
}

enum JsonWriteFrame<'a> {
    Value {
        v: &'a Val,
        depth: usize,
    },
    Array {
        vals: &'a [Val],
        idx: usize,
        depth: usize,
    },
    Object {
        entries: Vec<(&'a Val, &'a Val)>,
        idx: usize,
        depth: usize,
    },
    Byte(u8),
}

const MAX_PRINT_DEPTH: usize = 10000;

fn write_json_stack_safe(out: &mut Vec<u8>, root: &Val) -> std::io::Result<()> {
    let mut stack = Vec::from([JsonWriteFrame::Value { v: root, depth: 0 }]);

    while let Some(frame) = stack.pop() {
        match frame {
            JsonWriteFrame::Byte(b) => out.write_all(&[b])?,
            JsonWriteFrame::Value { v, depth } => {
                if depth > MAX_PRINT_DEPTH {
                    out.write_all(b"<skipped: too deep>")?;
                    continue;
                }
                match v {
                    Val::Arr(a) => {
                        out.write_all(b"[")?;
                        if a.is_empty() {
                            out.write_all(b"]")?;
                        } else {
                            let vals: &[Val] = a.as_slice();
                            stack.push(JsonWriteFrame::Array {
                                vals,
                                idx: 1,
                                depth,
                            });
                            stack.push(JsonWriteFrame::Value {
                                v: &vals[0],
                                depth: depth + 1,
                            });
                        }
                    }
                    Val::Obj(o) => {
                        out.write_all(b"{")?;
                        if o.is_empty() {
                            out.write_all(b"}")?;
                        } else {
                            let entries: Vec<_> = o.iter().collect();
                            let (k, v) = entries[0];
                            stack.push(JsonWriteFrame::Object {
                                entries,
                                idx: 1,
                                depth,
                            });
                            stack.push(JsonWriteFrame::Value {
                                v,
                                depth: depth + 1,
                            });
                            stack.push(JsonWriteFrame::Byte(b':'));
                            stack.push(JsonWriteFrame::Value {
                                v: k,
                                depth: depth + 1,
                            });
                        }
                    }
                    scalar => write_json_scalar(out, scalar)?,
                }
            }
            JsonWriteFrame::Array { vals, idx, depth } => {
                if idx < vals.len() {
                    out.write_all(b",")?;
                    stack.push(JsonWriteFrame::Array {
                        vals,
                        idx: idx + 1,
                        depth,
                    });
                    stack.push(JsonWriteFrame::Value {
                        v: &vals[idx],
                        depth: depth + 1,
                    });
                } else {
                    out.write_all(b"]")?;
                }
            }
            JsonWriteFrame::Object {
                entries,
                idx,
                depth,
            } => {
                if idx < entries.len() {
                    out.write_all(b",")?;
                    let (k, v) = entries[idx];
                    stack.push(JsonWriteFrame::Object {
                        entries,
                        idx: idx + 1,
                        depth,
                    });
                    stack.push(JsonWriteFrame::Value {
                        v,
                        depth: depth + 1,
                    });
                    stack.push(JsonWriteFrame::Byte(b':'));
                    stack.push(JsonWriteFrame::Value {
                        v: k,
                        depth: depth + 1,
                    });
                } else {
                    out.write_all(b"}")?;
                }
            }
        }
    }

    Ok(())
}

impl From<bool> for Val {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<isize> for Val {
    fn from(i: isize) -> Self {
        Self::Num(Num::Int(i))
    }
}

impl From<usize> for Val {
    fn from(i: usize) -> Self {
        Self::Num(Num::from_integral(i))
    }
}

impl From<f64> for Val {
    fn from(f: f64) -> Self {
        Self::Num(Num::Float(f))
    }
}

impl From<String> for Val {
    fn from(s: String) -> Self {
        Self::Str(Bytes::from_owner(s), Tag::Utf8)
    }
}

impl From<val::Range<Val>> for Val {
    fn from(r: val::Range<Val>) -> Self {
        let kv = |(k, v): (&str, Option<_>)| v.map(|v| (k.to_owned().into(), v));
        let kvs = [("start", r.start), ("end", r.end)];
        Val::obj(kvs.into_iter().flat_map(kv).collect())
    }
}

impl FromIterator<Self> for Val {
    fn from_iter<T: IntoIterator<Item = Self>>(iter: T) -> Self {
        Self::Arr(Rc::new(ArrayVals::from(iter.into_iter().collect::<Vec<_>>())))
    }
}

impl core::ops::Add for Val {
    type Output = ValR;
    fn add(self, rhs: Self) -> Self::Output {
        use Val::*;
        match (self, rhs) {
            // `null` is a neutral element for addition
            (Null, x) | (x, Null) => Ok(x),
            (Num(x), Num(y)) => Ok(Num(x + y)),
            (Str(l, tag), Str(r, tag_)) if tag == tag_ => {
                let mut buf = BytesMut::from(l);
                buf.put(r);
                Ok(Str(buf.into(), tag))
            }
            (Arr(mut l), Arr(r)) => {
                //std::dbg!(Rc::strong_count(&l));
                Rc::make_mut(&mut l).extend(r.iter().cloned());
                Ok(Arr(l))
            }
            (Obj(mut l), Obj(r)) => {
                Rc::make_mut(&mut l).extend(r.iter().map(|(k, v)| (k.clone(), v.clone())));
                Ok(Obj(l))
            }
            (l, r) => Err(Val::err_math(&l, ops::Math::Add, &r)),
        }
    }
}

impl core::ops::Sub for Val {
    type Output = ValR;
    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Num(x), Self::Num(y)) => Ok(Self::Num(x - y)),
            (Self::Arr(mut l), Self::Arr(r)) => {
                let r = r.iter().collect::<alloc::collections::BTreeSet<_>>();
                Rc::make_mut(&mut l).retain(|x| !r.contains(x));
                Ok(Self::Arr(l))
            }
            (l, r) => Err(Val::err_math(&l, ops::Math::Sub, &r)),
        }
    }
}

fn obj_merge(l: &mut Rc<ObjectVals>, r: Rc<ObjectVals>) {
    let l = Rc::make_mut(l);
    let r = rc_unwrap_or_clone(r).into_iter();
    r.for_each(|(k, v)| match (l.get_mut(&k), v) {
        (Some(Val::Obj(l)), Val::Obj(r)) => obj_merge(l, r),
        (Some(l), r) => *l = r,
        (None, r) => {
            l.insert(k, r);
        }
    });
}

impl core::ops::Mul for Val {
    type Output = ValR;
    fn mul(self, rhs: Self) -> Self::Output {
        use crate::Num::{BigInt, Dec, Float, Int};
        use Val::*;

        fn repeat_count(n: &crate::Num) -> Result<Option<usize>, Error> {
            let from_f64 = |f: f64| -> Result<Option<usize>, Error> {
                if f.is_nan() {
                    return Ok(None);
                }
                if !f.is_finite() {
                    return if f.is_sign_positive() {
                        Err(Error::str("Repeat string result too long"))
                    } else {
                        Ok(None)
                    };
                }
                if f < 0.0 {
                    return Ok(None);
                }
                if f == 0.0 {
                    return Ok(Some(0));
                }
                // jq truncates positive non-integers towards zero for repeats.
                let reps = f.floor();
                if reps > usize::MAX as f64 {
                    return Err(Error::str("Repeat string result too long"));
                }
                Ok(Some(reps as usize))
            };

            match n {
                Int(i) => {
                    if *i < 0 {
                        Ok(None)
                    } else {
                        Ok(Some(*i as usize))
                    }
                }
                BigInt(i) => {
                    if i.sign() == Sign::Minus {
                        Ok(None)
                    } else if i.sign() == Sign::NoSign {
                        Ok(Some(0))
                    } else {
                        i.to_usize()
                            .map(Some)
                            .ok_or_else(|| Error::str("Repeat string result too long"))
                    }
                }
                Float(f) => from_f64(*f),
                Dec(s) => from_f64(s.parse::<f64>().unwrap_or(f64::NAN)),
            }
        }

        match (self, rhs) {
            (Num(x), Num(y)) => Ok(Num(x * y)),
            (Str(s, tag), Num(n)) | (Num(n), Str(s, tag)) => {
                // jq rejects pathological repeats instead of trying to allocate huge strings.
                const MAX_REPEAT_BYTES: usize = 1 << 30;
                let Some(repeat) = repeat_count(&n)? else {
                    return Ok(Null);
                };
                if repeat == 0 {
                    return Ok(Self::Str(Bytes::new(), tag));
                }
                let Some(total_len) = s.len().checked_mul(repeat) else {
                    return Err(Error::str("Repeat string result too long"));
                };
                if total_len > MAX_REPEAT_BYTES {
                    return Err(Error::str("Repeat string result too long"));
                }
                Ok(Self::Str(s.repeat(repeat).into(), tag))
            }
            (Obj(mut l), Obj(r)) => {
                obj_merge(&mut l, r);
                Ok(Obj(l))
            }
            (l, r) => Err(Error::math(l, ops::Math::Mul, r)),
        }
    }
}

/// Split a string by a given separator string.
fn split<'a>(s: &'a [u8], sep: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
    if s.is_empty() {
        Box::new(core::iter::empty())
    } else if sep.is_empty() {
        // Rust's `split` function with an empty separator ("")
        // yields an empty string as first and last result
        // to prevent this, we are using `chars` instead
        Box::new(s.char_indices().map(|(start, end, _)| &s[start..end]))
    } else {
        Box::new(s.split_str(sep))
    }
}

impl core::ops::Div for Val {
    type Output = ValR;
    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Num(x), Self::Num(y)) => {
                if y.as_f64() == 0.0 {
                    return Err(Error::str(format_args!(
                        "number ({x}) and number ({y}) cannot be divided because the divisor is zero"
                    )));
                }
                Ok(Self::Num(x / y))
            }
            (Self::Str(x, tag), Self::Str(y, tag_)) if tag == tag_ => Ok(split(&x, &y)
                .map(|s| Val::Str(x.slice_ref(s), tag))
                .collect()),
            (l, r) => Err(Error::math(l, ops::Math::Div, r)),
        }
    }
}

impl core::ops::Rem for Val {
    type Output = ValR;
    fn rem(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Num(x), Self::Num(y)) => {
                let (na, nb) = (x.as_f64(), y.as_f64());
                if na.is_nan() || nb.is_nan() {
                    return Ok(Self::Num(Num::Float(f64::NAN)));
                }

                let bi = num_to_i64_trunc_saturated(&y);
                if bi == 0 {
                    return Err(Error::str(format_args!(
                        "number ({x}) and number ({y}) cannot be divided (remainder) because the divisor is zero"
                    )));
                }

                let ai = num_to_i64_trunc_saturated(&x);
                let r = if bi == -1 { 0 } else { ai % bi };
                Ok(Self::Num(Num::from_integral(r)))
            }
            (l, r) => Err(Error::math(l, ops::Math::Rem, r)),
        }
    }
}

impl core::ops::Neg for Val {
    type Output = ValR;
    fn neg(self) -> Self::Output {
        match self {
            Self::Num(n) => Ok(Self::Num(-n)),
            x => Err(Val::err_neg(&x)),
        }
    }
}

impl PartialOrd for Val {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Val {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::{Equal, Greater, Less};
        match (self, other) {
            (Self::Null, Self::Null) => Equal,
            (Self::Bool(x), Self::Bool(y)) => x.cmp(y),
            (Self::Num(x), Self::Num(y)) => x.cmp(y),
            (Self::Str(x, _), Self::Str(y, _)) => x.cmp(y),
            (Self::Arr(x), Self::Arr(y)) => x.cmp(y),
            (Self::Obj(x), Self::Obj(y)) => match (x.len(), y.len()) {
                (0, 0) => Equal,
                (0, _) => Less,
                (_, 0) => Greater,
                _ => {
                    let mut l: Vec<_> = x.iter().collect();
                    let mut r: Vec<_> = y.iter().collect();
                    l.sort_by_key(|(k, _v)| *k);
                    r.sort_by_key(|(k, _v)| *k);
                    // TODO: make this nicer
                    let kl = l.iter().map(|(k, _v)| k);
                    let kr = r.iter().map(|(k, _v)| k);
                    let vl = l.iter().map(|(_k, v)| v);
                    let vr = r.iter().map(|(_k, v)| v);
                    kl.cmp(kr).then_with(|| vl.cmp(vr))
                }
            },

            // nulls are smaller than anything else
            (Self::Null, _) => Less,
            (_, Self::Null) => Greater,
            // bools are smaller than anything else, except for nulls
            (Self::Bool(_), _) => Less,
            (_, Self::Bool(_)) => Greater,
            // numbers are smaller than anything else, except for nulls and bools
            (Self::Num(_), _) => Less,
            (_, Self::Num(_)) => Greater,
            // etc.
            (Self::Str(..), _) => Less,
            (_, Self::Str(..)) => Greater,
            (Self::Arr(_), _) => Less,
            (_, Self::Arr(_)) => Greater,
        }
    }
}

impl PartialEq for Val {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(x), Self::Bool(y)) => x == y,
            (Self::Num(x), Self::Num(y)) => x == y,
            (Self::Str(x, _tag), Self::Str(y, _)) => x == y,
            (Self::Arr(x), Self::Arr(y)) => x == y,
            (Self::Obj(x), Self::Obj(y)) => x == y,
            _ => false,
        }
    }
}

impl Eq for Val {}

impl Hash for Val {
    fn hash<H: Hasher>(&self, state: &mut H) {
        fn hash_with(u: u8, x: impl Hash, state: &mut impl Hasher) {
            state.write_u8(u);
            x.hash(state)
        }
        match self {
            Self::Num(n) => n.hash(state),
            // Num::hash() starts its hash with a 0 or 1, so we start with 2 here
            Self::Null => state.write_u8(2),
            Self::Bool(b) => state.write_u8(if *b { 3 } else { 4 }),
            Self::Str(b, _) => hash_with(5, b, state),
            Self::Arr(a) => hash_with(6, a, state),
            Self::Obj(o) => {
                state.write_u8(7);
                // this is similar to what happens in `Val::cmp`
                let mut kvs: Vec<_> = o.iter().collect();
                kvs.sort_by_key(|(k, _v)| *k);
                kvs.iter().for_each(|(k, v)| (k, v).hash(state));
            }
        }
    }
}

/// Display bytes as valid UTF-8 string.
///
/// This maps invalid UTF-8 to the Unicode replacement character.
pub fn bstr(s: &(impl core::convert::AsRef<[u8]> + ?Sized)) -> impl fmt::Display + '_ {
    BStr::new(s)
}

impl fmt::Display for Val {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write::format(f, &write::Pp::default(), 0, self)
    }
}
