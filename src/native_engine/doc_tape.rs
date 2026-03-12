use crate::value::{
    take_pooled_object_map_with_capacity, take_pooled_value_vec_with_capacity, ZqValue,
};
use indexmap::IndexMap;
use serde::de::{Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Number as JsonNumber;
use std::cell::Cell;
use std::fmt;

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
    Number(JsonNumber),
    String(StringRange),
    Array { start: u32, len: u32 },
    Object { start: u32, len: u32 },
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
    pub(crate) fn parse_json<'de, D>(&mut self, deserializer: D) -> Result<DocTape, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut builder = DocBuilder {
            nodes: std::mem::take(&mut self.nodes),
            array_items: std::mem::take(&mut self.array_items),
            object_entries: std::mem::take(&mut self.object_entries),
            strings: std::mem::take(&mut self.strings),
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
                    builder.push_node(DocNodeKind::Number(JsonNumber::from(value))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Number(JsonNumber::from(value))).map(DocNodeRef)
                })
                .ok_or_else(|| E::custom("document builder is unavailable"))?
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                let Some(number) = JsonNumber::from_f64(value) else {
                    return Err(E::custom("non-finite number is not supported"));
                };
                with_active_doc_builder(|builder| {
                    builder.push_node(DocNodeKind::Number(number)).map(DocNodeRef)
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
                    let start = builder.array_items.len();
                    if let Some(size_hint) = seq.size_hint() {
                        builder.array_items.reserve(size_hint);
                    }
                    while let Some(item) = seq.next_element::<DocNodeRef>()? {
                        builder.array_items.push(item.0);
                    }
                    let len = builder.array_items.len() - start;
                    Ok(DocNodeRef(builder.push_node(DocNodeKind::Array {
                        start: DocBuilder::encode_len(start, "json array item storage")?,
                        len: DocBuilder::encode_len(len, "json array length")?,
                    })?))
                })
                .ok_or_else(|| A::Error::custom("document builder is unavailable"))?
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                with_active_doc_builder(|builder| {
                    let start = builder.object_entries.len();
                    if let Some(size_hint) = map.size_hint() {
                        builder.object_entries.reserve(size_hint);
                    }
                    while let Some((key, value)) = map.next_entry::<DocStringRef, DocNodeRef>()? {
                        builder.object_entries.push(ObjectEntry { key: key.0, value: value.0 });
                    }
                    let len = builder.object_entries.len() - start;
                    Ok(DocNodeRef(builder.push_node(DocNodeKind::Object {
                        start: DocBuilder::encode_len(start, "json object entry storage")?,
                        len: DocBuilder::encode_len(len, "json object length")?,
                    })?))
                })
                .ok_or_else(|| A::Error::custom("document builder is unavailable"))?
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
            DocNodeKind::Number(value) => ZqValue::Number(value.clone()),
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
}

#[derive(Clone)]
pub(crate) enum EvaluatedNode<'a> {
    Node(NodeRef<'a>),
    Owned(ZqValue),
}

impl<'a> EvaluatedNode<'a> {
    pub(crate) fn into_owned(self) -> ZqValue {
        match self {
            EvaluatedNode::Node(node) => node.materialize(),
            EvaluatedNode::Owned(value) => value,
        }
    }
}
