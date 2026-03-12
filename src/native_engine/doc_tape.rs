use crate::value::{
    take_pooled_object_map_with_capacity, take_pooled_value_vec_with_capacity, ZqValue,
};
use indexmap::IndexMap;
use serde::de::{self, DeserializeSeed};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use serde::de::{Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Number as JsonNumber;
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt;
use std::io::{self, Write};

thread_local! {
    static ACTIVE_DOC_BUILDER: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeId(u32);

#[derive(Clone, Copy, Debug)]
struct StringRange {
    start: u32,
    len: u32,
}

#[derive(Debug, Clone)]
struct DocNode {
    kind: DocNodeKind,
}

#[derive(Debug, Clone)]
enum DocNodeKind {
    Null,
    Bool(bool),
    Number(DocNumber),
    String(StringRange),
    Array { start: u32, len: u32 },
    Object { start: u32, len: u32 },
}

#[derive(Debug, Clone, Copy)]
enum DocNumber {
    I64(i64),
    U64(u64),
    F64(u64),
    Raw(StringRange),
}

#[derive(Debug, Clone)]
struct ObjectEntry {
    key: StringRange,
    value: NodeId,
}

struct ActiveDocBuilderGuard {
    prev: usize,
}

impl Drop for ActiveDocBuilderGuard {
    fn drop(&mut self) {
        ACTIVE_DOC_BUILDER.with(|slot| slot.set(self.prev));
    }
}

#[derive(Default)]
pub(crate) struct JsonDocScratch {
    nodes: Vec<DocNode>,
    array_items: Vec<NodeId>,
    object_entries: Vec<ObjectEntry>,
    strings: Vec<u8>,
}

struct DocBuilder {
    nodes: Vec<DocNode>,
    array_items: Vec<NodeId>,
    object_entries: Vec<ObjectEntry>,
    strings: Vec<u8>,
    root_field_filter: Option<*const RootFieldFilter>,
    container_depth: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DocTape {
    root: NodeId,
    nodes: Vec<DocNode>,
    array_items: Vec<NodeId>,
    object_entries: Vec<ObjectEntry>,
    strings: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) struct NodeRef<'a> {
    doc: &'a DocTape,
    id: NodeId,
}

#[derive(Clone, Copy)]
struct DocStringRef(StringRange);

#[derive(Clone, Copy)]
struct DocNodeRef(NodeId);

#[derive(Clone, Copy)]
enum KeyClass {
    Map(StringRange),
    Number,
}

struct KeyClassifier;

#[derive(Clone, Copy)]
pub(crate) enum DocNumberRef<'a> {
    I64(i64),
    U64(u64),
    F64(f64),
    Raw(&'a str),
}

#[derive(Debug, Clone)]
pub(crate) struct RootFieldFilter {
    names: Box<[String]>,
}

fn install_active_doc_builder(builder: &mut DocBuilder) -> ActiveDocBuilderGuard {
    let ptr = (builder as *mut DocBuilder).cast::<()>() as usize;
    let prev = ACTIVE_DOC_BUILDER.with(|slot| {
        let prev = slot.get();
        slot.set(ptr);
        prev
    });
    ActiveDocBuilderGuard { prev }
}

fn with_active_doc_builder<R>(f: impl FnOnce(&mut DocBuilder) -> R) -> Option<R> {
    ACTIVE_DOC_BUILDER.with(|slot| {
        let ptr = slot.get();
        if ptr == 0 {
            return None;
        }
        let builder_ptr = ptr as *mut DocBuilder;
        // SAFETY: pointer is installed from a live stack builder and restored
        // by ActiveDocBuilderGuard; all access stays on the same thread.
        Some(unsafe { f(&mut *builder_ptr) })
    })
}

impl JsonDocScratch {
    pub(crate) fn parse_json_with_root_filter<'de, D>(
        &mut self,
        deserializer: D,
        root_field_filter: Option<&RootFieldFilter>,
    ) -> Result<DocTape, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut builder = DocBuilder {
            nodes: std::mem::take(&mut self.nodes),
            array_items: std::mem::take(&mut self.array_items),
            object_entries: std::mem::take(&mut self.object_entries),
            strings: std::mem::take(&mut self.strings),
            root_field_filter: root_field_filter.map(|filter| filter as *const RootFieldFilter),
            container_depth: 0,
        };
        builder.clear();
        let _guard = install_active_doc_builder(&mut builder);
        let root = DocNodeRef::deserialize(deserializer)?.0;
        Ok(builder.finish(root))
    }

    pub(crate) fn recycle(&mut self, mut doc: DocTape) {
        doc.nodes.clear();
        doc.array_items.clear();
        doc.object_entries.clear();
        doc.strings.clear();
        self.nodes = doc.nodes;
        self.array_items = doc.array_items;
        self.object_entries = doc.object_entries;
        self.strings = doc.strings;
    }
}

impl DocBuilder {
    fn clear(&mut self) {
        self.nodes.clear();
        self.array_items.clear();
        self.object_entries.clear();
        self.strings.clear();
    }

    fn encode_len<E: DeError>(len: usize, what: &str) -> Result<u32, E> {
        u32::try_from(len).map_err(|_| E::custom(format!("{what} exceeds supported size")))
    }

    fn finish(self, root: NodeId) -> DocTape {
        DocTape {
            root,
            nodes: self.nodes,
            array_items: self.array_items,
            object_entries: self.object_entries,
            strings: self.strings,
        }
    }

    fn push_string<E: DeError>(&mut self, value: &str) -> Result<StringRange, E> {
        let start = self.strings.len();
        self.strings.extend_from_slice(value.as_bytes());
        Ok(StringRange {
            start: Self::encode_len(start, "json string storage")?,
            len: Self::encode_len(value.len(), "json string length")?,
        })
    }

    fn push_node<E: DeError>(&mut self, kind: DocNodeKind) -> Result<NodeId, E> {
        let id = NodeId(Self::encode_len(self.nodes.len(), "json node count")?);
        self.nodes.push(DocNode { kind });
        Ok(id)
    }

    fn root_field_filter(&self) -> Option<&RootFieldFilter> {
        let ptr = self.root_field_filter?;
        // SAFETY: the filter outlives the parse call that owns this builder.
        Some(unsafe { &*ptr })
    }

    fn string(&self, range: StringRange) -> &str {
        let start = range.start as usize;
        let end = start + range.len as usize;
        std::str::from_utf8(&self.strings[start..end])
            .expect("document builder stores valid UTF-8 strings")
    }
}

impl<'de> Deserialize<'de> for DocStringRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StringVisitor;

        impl<'de> Visitor<'de> for StringVisitor {
            type Value = DocStringRef;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| builder.push_string(value).map(DocStringRef))
                    .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_str(value)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_string(StringVisitor)
    }
}

impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeyClassifier {
    type Value = KeyClass;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value == "$serde_json::private::Number" {
            return Ok(KeyClass::Number);
        }
        with_active_doc_builder(|builder| builder.push_string(value).map(KeyClass::Map))
            .ok_or_else(|| E::custom("document builder is unavailable"))?
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }
}

impl<'de> Deserialize<'de> for DocNodeRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeVisitor;

        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = DocNodeRef;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON-compatible value")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Bool(value)).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Number(DocNumber::I64(value))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Number(DocNumber::U64(value))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                if let Ok(value) = i64::try_from(value) {
                    return self.visit_i64(value);
                }
                with_active_doc_builder(|builder| {
                    let raw = builder.push_string(&value.to_string())?;
                    builder.push_node(DocNodeKind::Number(DocNumber::Raw(raw))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                if let Ok(value) = u64::try_from(value) {
                    return self.visit_u64(value);
                }
                with_active_doc_builder(|builder| {
                    let raw = builder.push_string(&value.to_string())?;
                    builder.push_node(DocNodeKind::Number(DocNumber::Raw(raw))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                if !value.is_finite() {
                    return Err(E::custom("non-finite number is not supported"));
                }
                with_active_doc_builder(|builder| {
                    builder
                        .push_node(DocNodeKind::Number(DocNumber::F64(value.to_bits())))
                        .map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    let slot = builder.push_string(value)?;
                    builder.push_node(DocNodeKind::String(slot)).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_str(value)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_str(&value)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Null).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_none()
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                DocNodeRef::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                with_active_doc_builder(|builder| {
                    let prev_depth = builder.container_depth;
                    builder.container_depth += 1;
                    let result = (|| {
                        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                        if let Some(size_hint) = seq.size_hint() {
                            items.reserve(size_hint);
                        }
                        while let Some(item) = seq.next_element::<DocNodeRef>()? {
                            items.push(item.0);
                        }
                        let start = builder.array_items.len();
                        let len = items.len();
                        builder.array_items.extend(items);
                        Ok(DocNodeRef(builder.push_node(DocNodeKind::Array {
                            start: DocBuilder::encode_len(start, "json array item storage")?,
                            len: DocBuilder::encode_len(len, "json array length")?,
                        })?))
                    })();
                    builder.container_depth = prev_depth;
                    result
                })
                .ok_or_else(|| A::Error::custom("document builder is unavailable"))?
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                match map.next_key_seed(KeyClassifier)? {
                    Some(KeyClass::Number) => {
                        let raw = map.next_value::<String>()?;
                        with_active_doc_builder(|builder| {
                            let slot = builder.push_string(&raw)?;
                            builder
                                .push_node(DocNodeKind::Number(DocNumber::Raw(slot)))
                                .map(DocNodeRef)
                        })
                        .ok_or_else(|| A::Error::custom("document builder is unavailable"))?
                    }
                    Some(KeyClass::Map(first_key)) => with_active_doc_builder(|builder| {
                        let prev_depth = builder.container_depth;
                        let is_root_object = prev_depth == 0;
                        builder.container_depth += 1;
                        let result = (|| {
                            let mut entries =
                                Vec::with_capacity(map.size_hint().unwrap_or(0).saturating_add(1));
                            let keep_first = !is_root_object
                                || builder
                                    .root_field_filter()
                                    .is_none_or(|filter| filter.contains(builder.string(first_key)));
                            if keep_first {
                                let first_value = map.next_value::<DocNodeRef>()?;
                                entries.push(ObjectEntry {
                                    key: first_key,
                                    value: first_value.0,
                                });
                            } else {
                                map.next_value::<serde::de::IgnoredAny>()?;
                            }
                            while let Some(key) = map.next_key::<DocStringRef>()? {
                                let keep = !is_root_object
                                    || builder.root_field_filter().is_none_or(|filter| {
                                        filter.contains(builder.string(key.0))
                                    });
                                if keep {
                                    let value = map.next_value::<DocNodeRef>()?;
                                    entries.push(ObjectEntry {
                                        key: key.0,
                                        value: value.0,
                                    });
                                } else {
                                    map.next_value::<serde::de::IgnoredAny>()?;
                                }
                            }
                            let start = builder.object_entries.len();
                            let len = entries.len();
                            builder.object_entries.extend(entries);
                            Ok(DocNodeRef(builder.push_node(DocNodeKind::Object {
                                start: DocBuilder::encode_len(start, "json object entry storage")?,
                                len: DocBuilder::encode_len(len, "json object length")?,
                            })?))
                        })();
                        builder.container_depth = prev_depth;
                        result
                    })
                    .ok_or_else(|| A::Error::custom("document builder is unavailable"))?,
                    None => with_active_doc_builder(|builder| {
                        let prev_depth = builder.container_depth;
                        builder.container_depth += 1;
                        let start = builder.object_entries.len();
                        let result = Ok(DocNodeRef(builder.push_node(DocNodeKind::Object {
                            start: DocBuilder::encode_len(start, "json object entry storage")?,
                            len: 0,
                        })?));
                        builder.container_depth = prev_depth;
                        result
                    })
                    .ok_or_else(|| A::Error::custom("document builder is unavailable"))?,
                }
            }
        }

        deserializer.deserialize_any(NodeVisitor)
    }
}

impl DocTape {
    pub(crate) fn root(&self) -> NodeRef<'_> {
        NodeRef { doc: self, id: self.root }
    }

    pub(crate) fn materialize_node(&self, node: NodeId) -> ZqValue {
        match &self.nodes[node.0 as usize].kind {
            DocNodeKind::Null => ZqValue::Null,
            DocNodeKind::Bool(value) => ZqValue::Bool(*value),
            DocNodeKind::Number(value) => ZqValue::Number(value.to_json_number(self)),
            DocNodeKind::String(range) => ZqValue::String(self.string(*range).to_owned()),
            DocNodeKind::Array { start, len } => {
                let start = *start as usize;
                let len = *len as usize;
                let mut items = take_pooled_value_vec_with_capacity(len);
                for child in &self.array_items[start..(start + len)] {
                    items.push(self.materialize_node(*child));
                }
                ZqValue::Array(items)
            }
            DocNodeKind::Object { start, len } => {
                let start = *start as usize;
                let len = *len as usize;
                let mut out: IndexMap<String, ZqValue> = take_pooled_object_map_with_capacity(len);
                for entry in &self.object_entries[start..(start + len)] {
                    out.insert(
                        self.string(entry.key).to_owned(),
                        self.materialize_node(entry.value),
                    );
                }
                ZqValue::Object(out)
            }
        }
    }

    fn string(&self, range: StringRange) -> &str {
        let start = range.start as usize;
        let end = start + range.len as usize;
        std::str::from_utf8(&self.strings[start..end])
            .expect("document tape stores valid UTF-8 strings")
    }
}

impl<'a> NodeRef<'a> {
    pub(crate) fn type_name(self) -> &'static str {
        match &self.doc.nodes[self.id.0 as usize].kind {
            DocNodeKind::Null => "null",
            DocNodeKind::Bool(_) => "boolean",
            DocNodeKind::Number(_) => "number",
            DocNodeKind::String(_) => "string",
            DocNodeKind::Array { .. } => "array",
            DocNodeKind::Object { .. } => "object",
        }
    }

    pub(crate) fn as_str(self) -> Option<&'a str> {
        let DocNodeKind::String(range) = &self.doc.nodes[self.id.0 as usize].kind else {
            return None;
        };
        Some(self.doc.string(*range))
    }

    pub(crate) fn as_number(self) -> Option<DocNumberRef<'a>> {
        let DocNodeKind::Number(number) = &self.doc.nodes[self.id.0 as usize].kind else {
            return None;
        };
        Some(number.as_ref(self.doc))
    }

    pub(crate) fn as_bool(self) -> Option<bool> {
        let DocNodeKind::Bool(value) = &self.doc.nodes[self.id.0 as usize].kind else {
            return None;
        };
        Some(*value)
    }

    pub(crate) fn is_null(self) -> bool {
        matches!(&self.doc.nodes[self.id.0 as usize].kind, DocNodeKind::Null)
    }

    pub(crate) fn lookup_field(self, name: &str) -> Option<NodeRef<'a>> {
        let DocNodeKind::Object { start, len } = &self.doc.nodes[self.id.0 as usize].kind else {
            return None;
        };
        let start = *start as usize;
        let len = *len as usize;
        for entry in self.doc.object_entries[start..(start + len)].iter().rev() {
            if self.doc.string(entry.key) == name {
                return Some(NodeRef { doc: self.doc, id: entry.value });
            }
        }
        None
    }

    pub(crate) fn lookup_index(self, index: i64) -> Option<EvaluatedNode<'a>> {
        match &self.doc.nodes[self.id.0 as usize].kind {
            DocNodeKind::Array { start, len } => {
                let start = *start as usize;
                let len = *len as usize;
                let idx = crate::c_compat::string::normalize_index_jq(len, index)?;
                let id = self.doc.array_items[start + idx];
                Some(EvaluatedNode::Node(NodeRef { doc: self.doc, id }))
            }
            DocNodeKind::String(range) => {
                crate::c_compat::string::string_index_like_jq(self.doc.string(*range), index)
                    .map(EvaluatedNode::Owned)
            }
            _ => None,
        }
    }

    pub(crate) fn materialize(self) -> ZqValue {
        self.doc.materialize_node(self.id)
    }

    pub(crate) fn jq_truthy(self) -> bool {
        !matches!(
            &self.doc.nodes[self.id.0 as usize].kind,
            DocNodeKind::Null | DocNodeKind::Bool(false)
        )
    }

    pub(crate) fn jq_kind_rank(self) -> i32 {
        match &self.doc.nodes[self.id.0 as usize].kind {
            DocNodeKind::Null => 1,
            DocNodeKind::Bool(false) => 2,
            DocNodeKind::Bool(true) => 3,
            DocNodeKind::Number(_) => 4,
            DocNodeKind::String(_) => 5,
            DocNodeKind::Array { .. } => 6,
            DocNodeKind::Object { .. } => 7,
        }
    }

    pub(crate) fn jq_length(self) -> Result<ZqValue, String> {
        match &self.doc.nodes[self.id.0 as usize].kind {
            DocNodeKind::Null => Ok(ZqValue::from(0)),
            DocNodeKind::Bool(value) => Err(format!("boolean ({value}) has no length")),
            DocNodeKind::Number(number) => {
                if let Some(value) = number.as_ref(self.doc).to_f64_lossy() {
                    return Ok(crate::c_compat::math::number_to_value(value.abs()));
                }
                let raw = number.as_ref(self.doc).to_text();
                let abs_raw = raw.strip_prefix('-').unwrap_or(raw.as_ref()).to_string();
                Ok(ZqValue::Number(serde_json::Number::from_string_unchecked(abs_raw)))
            }
            DocNodeKind::String(range) => {
                Ok(ZqValue::from(self.doc.string(*range).chars().count() as i64))
            }
            DocNodeKind::Array { len, .. } | DocNodeKind::Object { len, .. } => {
                Ok(ZqValue::from(i64::from(*len)))
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum EvaluatedNode<'a> {
    Node(NodeRef<'a>),
    ProjectedObject(Vec<(String, EvaluatedNode<'a>)>),
    Owned(ZqValue),
}

impl<'a> EvaluatedNode<'a> {
    pub(crate) fn into_owned(self) -> ZqValue {
        match self {
            EvaluatedNode::Node(node) => node.materialize(),
            EvaluatedNode::ProjectedObject(entries) => {
                let mut out = take_pooled_object_map_with_capacity(entries.len());
                for (key, value) in entries {
                    out.insert(key, value.into_owned());
                }
                ZqValue::Object(out)
            }
            EvaluatedNode::Owned(value) => value,
        }
    }
}

pub(crate) fn write_json_evaluated_line<W: Write>(
    writer: &mut W,
    value: &EvaluatedNode<'_>,
    compact: bool,
    raw_output: bool,
    scratch: &mut Vec<u8>,
    pretty_indent: Option<&[u8]>,
) -> Result<(), String> {
    if raw_output {
        if let Some(text) = evaluated_as_str(value) {
            writer.write_all(text.as_bytes()).map_err(|e| e.to_string())?;
            return Ok(());
        }
    }

    scratch.clear();
    if compact {
        let mut serializer = serde_json::Serializer::new(&mut *scratch);
        EvaluatedCliJsonCompat(value)
            .serialize(&mut serializer)
            .map_err(|e| format!("encode output: {e}"))?;
    } else {
        let indent = pretty_indent.unwrap_or(&[]);
        let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
        let mut serializer = serde_json::Serializer::with_formatter(&mut *scratch, formatter);
        EvaluatedCliJsonCompat(value)
            .serialize(&mut serializer)
            .map_err(|e| format!("encode output: {e}"))?;
    }
    write_jq_style_escaped_del(writer, scratch).map_err(|e| e.to_string())
}

fn evaluated_as_str<'a>(value: &'a EvaluatedNode<'a>) -> Option<&'a str> {
    match value {
        EvaluatedNode::Node(node) => node.as_str(),
        EvaluatedNode::ProjectedObject(_) => None,
        EvaluatedNode::Owned(ZqValue::String(text)) => Some(text.as_str()),
        EvaluatedNode::Owned(_) => None,
    }
}

fn write_jq_style_escaped_del<W: Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    for &byte in bytes {
        if byte == 0x7f {
            writer.write_all(b"\\u007f")?;
        } else {
            writer.write_all(&[byte])?;
        }
    }
    Ok(())
}

struct EvaluatedCliJsonCompat<'a>(&'a EvaluatedNode<'a>);

impl Serialize for EvaluatedCliJsonCompat<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            EvaluatedNode::Node(node) => NodeCliJsonCompat(*node).serialize(serializer),
            EvaluatedNode::ProjectedObject(entries) => {
                let mut object = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    object.serialize_entry(key, &EvaluatedCliJsonCompat(value))?;
                }
                object.end()
            }
            EvaluatedNode::Owned(value) => OwnedCliJsonCompat(value).serialize(serializer),
        }
    }
}

struct NodeCliJsonCompat<'a>(NodeRef<'a>);

impl Serialize for NodeCliJsonCompat<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0.doc.nodes[self.0.id.0 as usize].kind {
            DocNodeKind::Null => serializer.serialize_unit(),
            DocNodeKind::Bool(value) => serializer.serialize_bool(*value),
            DocNodeKind::Number(number) => {
                serialize_doc_number_cli_compat(number.as_ref(self.0.doc), serializer)
            }
            DocNodeKind::String(range) => serializer.serialize_str(self.0.doc.string(*range)),
            DocNodeKind::Array { start, len } => {
                let start = *start as usize;
                let len = *len as usize;
                let mut seq = serializer.serialize_seq(Some(len))?;
                for child in &self.0.doc.array_items[start..(start + len)] {
                    seq.serialize_element(&NodeCliJsonCompat(NodeRef {
                        doc: self.0.doc,
                        id: *child,
                    }))?;
                }
                seq.end()
            }
            DocNodeKind::Object { start, len } => {
                let start = *start as usize;
                let len = *len as usize;
                let mut map = serializer.serialize_map(Some(len))?;
                for entry in &self.0.doc.object_entries[start..(start + len)] {
                    map.serialize_entry(
                        self.0.doc.string(entry.key),
                        &NodeCliJsonCompat(NodeRef {
                            doc: self.0.doc,
                            id: entry.value,
                        }),
                    )?;
                }
                map.end()
            }
        }
    }
}

struct OwnedCliJsonCompat<'a>(&'a ZqValue);

impl Serialize for OwnedCliJsonCompat<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            ZqValue::Null => serializer.serialize_unit(),
            ZqValue::Bool(value) => serializer.serialize_bool(*value),
            ZqValue::Number(number) => serialize_json_number_cli_compat(number, serializer),
            ZqValue::String(text) => serializer.serialize_str(text),
            ZqValue::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&OwnedCliJsonCompat(item))?;
                }
                seq.end()
            }
            ZqValue::Object(map) => {
                let mut object = serializer.serialize_map(Some(map.len()))?;
                for (key, value) in map {
                    object.serialize_entry(key, &OwnedCliJsonCompat(value))?;
                }
                object.end()
            }
        }
    }
}

fn serialize_json_number_cli_compat<S>(
    number: &serde_json::Number,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if number.is_i64() || number.is_u64() || number.is_f64() {
        return number.serialize(serializer);
    }

    let raw = number.to_string();
    let unsigned = raw.strip_prefix('-').or_else(|| raw.strip_prefix('+')).unwrap_or(&raw);
    let lower = unsigned.to_ascii_lowercase();

    if lower.starts_with("nan") {
        return serializer.serialize_unit();
    }

    if lower == "inf" || lower == "infinity" {
        let finite = if raw.starts_with('-') {
            "-1.7976931348623157e+308"
        } else {
            "1.7976931348623157e+308"
        };
        let finite_number = serde_json::Number::from_string_unchecked(finite.to_string());
        return finite_number.serialize(serializer);
    }

    number.serialize(serializer)
}

fn serialize_doc_number_cli_compat<S>(
    number: DocNumberRef<'_>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match number {
        DocNumberRef::I64(value) => serializer.serialize_i64(value),
        DocNumberRef::U64(value) => serializer.serialize_u64(value),
        DocNumberRef::F64(value) => serde_json::Number::from_f64(value)
            .expect("document tape stores only finite f64 numbers")
            .serialize(serializer),
        DocNumberRef::Raw(raw) => {
            let number = serde_json::Number::from_string_unchecked(raw.to_owned());
            serialize_json_number_cli_compat(&number, serializer)
        }
    }
}

impl DocNumber {
    fn as_ref<'a>(self, doc: &'a DocTape) -> DocNumberRef<'a> {
        match self {
            DocNumber::I64(value) => DocNumberRef::I64(value),
            DocNumber::U64(value) => DocNumberRef::U64(value),
            DocNumber::F64(bits) => DocNumberRef::F64(f64::from_bits(bits)),
            DocNumber::Raw(range) => DocNumberRef::Raw(doc.string(range)),
        }
    }

    fn to_json_number(self, doc: &DocTape) -> JsonNumber {
        self.as_ref(doc).to_json_number()
    }
}

impl<'a> DocNumberRef<'a> {
    pub(crate) fn to_f64_lossy(self) -> Option<f64> {
        match self {
            DocNumberRef::I64(value) => Some(value as f64),
            DocNumberRef::U64(value) => Some(value as f64),
            DocNumberRef::F64(value) => Some(value),
            DocNumberRef::Raw(raw) => {
                let number = serde_json::Number::from_string_unchecked(raw.to_owned());
                crate::c_compat::math::jq_number_to_f64_lossy(&number)
            }
        }
    }

    pub(crate) fn to_json_number(self) -> JsonNumber {
        match self {
            DocNumberRef::I64(value) => JsonNumber::from(value),
            DocNumberRef::U64(value) => JsonNumber::from(value),
            DocNumberRef::F64(value) => {
                JsonNumber::from_f64(value).expect("document tape stores only finite f64 numbers")
            }
            DocNumberRef::Raw(raw) => serde_json::Number::from_string_unchecked(raw.to_owned()),
        }
    }

    pub(crate) fn to_text(self) -> std::borrow::Cow<'a, str> {
        match self {
            DocNumberRef::I64(value) => std::borrow::Cow::Owned(value.to_string()),
            DocNumberRef::U64(value) => std::borrow::Cow::Owned(value.to_string()),
            DocNumberRef::F64(value) => std::borrow::Cow::Owned(
                JsonNumber::from_f64(value)
                    .expect("document tape stores only finite f64 numbers")
                    .to_string(),
            ),
            DocNumberRef::Raw(raw) => std::borrow::Cow::Borrowed(raw),
        }
    }

    pub(crate) fn compare_jq(self, other: Self) -> Ordering {
        match (self, other) {
            (DocNumberRef::I64(left), DocNumberRef::I64(right)) => left.cmp(&right),
            (DocNumberRef::U64(left), DocNumberRef::U64(right)) => left.cmp(&right),
            (DocNumberRef::I64(left), DocNumberRef::U64(right)) => {
                if left < 0 { Ordering::Less } else { (left as u64).cmp(&right) }
            }
            (DocNumberRef::U64(left), DocNumberRef::I64(right)) => {
                if right < 0 { Ordering::Greater } else { left.cmp(&(right as u64)) }
            }
            _ => crate::c_compat::math::compare_json_numbers_like_jq(
                &self.to_json_number(),
                &other.to_json_number(),
            ),
        }
    }
}

impl RootFieldFilter {
    pub(crate) fn from_names(mut names: Vec<String>) -> Self {
        names.sort();
        names.dedup();
        Self { names: names.into_boxed_slice() }
    }

    fn contains(&self, name: &str) -> bool {
        self.names.binary_search_by(|candidate| candidate.as_str().cmp(name)).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_json_with_root_filter_keeps_only_requested_root_fields() {
        let mut scratch = JsonDocScratch::default();
        let mut parser =
            serde_json::Deserializer::from_str(r#"{"id":7,"skip":9,"text":"alpha"}"#);
        let doc = scratch
            .parse_json_with_root_filter(
                &mut parser,
                Some(&RootFieldFilter::from_names(vec!["id".to_string(), "text".to_string()])),
            )
            .expect("parse");
        assert_eq!(doc.root().materialize().into_json(), json!({"id": 7, "text": "alpha"}));
    }

    #[test]
    fn parse_json_with_root_filter_preserves_nested_subtree_of_kept_field() {
        let mut scratch = JsonDocScratch::default();
        let mut parser =
            serde_json::Deserializer::from_str(r#"{"keep":{"a":1,"b":2},"drop":{"x":9}}"#);
        let doc = scratch
            .parse_json_with_root_filter(
                &mut parser,
                Some(&RootFieldFilter::from_names(vec!["keep".to_string()])),
            )
            .expect("parse");
        assert_eq!(doc.root().materialize().into_json(), json!({"keep": {"a": 1, "b": 2}}));
    }

    #[test]
    fn parse_json_preserves_nested_arrays_and_objects() {
        let mut scratch = JsonDocScratch::default();
        let mut parser =
            serde_json::Deserializer::from_str(r#"{"nested":[[1,2],{"a":[3,4]}],"tail":5}"#);
        let doc = scratch.parse_json_with_root_filter(&mut parser, None).expect("parse");
        assert_eq!(
            doc.root().materialize().into_json(),
            json!({"nested": [[1, 2], {"a": [3, 4]}], "tail": 5})
        );
    }
}
