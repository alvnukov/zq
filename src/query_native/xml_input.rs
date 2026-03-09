use super::{Error, JsonValue, ZqValue};

pub(super) fn parse_xml_native_doc(input: &str) -> Result<ZqValue, Error> {
    let document =
        roxmltree::Document::parse(input).map_err(|e| Error::Runtime(format!("xml: {e}")))?;
    let root = document.root_element();
    let mut out = serde_json::Map::new();
    out.insert(
        root.tag_name().name().to_string(),
        xml_element_to_json_value(root),
    );
    Ok(ZqValue::from_json(JsonValue::Object(out)))
}

fn xml_element_to_json_value(node: roxmltree::Node<'_, '_>) -> JsonValue {
    let mut object = serde_json::Map::new();

    for attr in node.attributes() {
        object.insert(
            format!("@{}", attr.name()),
            JsonValue::String(attr.value().to_string()),
        );
    }

    for child in node.children().filter(|child| child.is_element()) {
        let key = child.tag_name().name().to_string();
        let child_value = xml_element_to_json_value(child);
        if let Some(existing) = object.get_mut(&key) {
            if let JsonValue::Array(items) = existing {
                items.push(child_value);
            } else {
                let previous = std::mem::replace(existing, JsonValue::Null);
                *existing = JsonValue::Array(vec![previous, child_value]);
            }
        } else {
            object.insert(key, child_value);
        }
    }

    if let Some(text) = collect_xml_text_content(node) {
        if object.is_empty() {
            return JsonValue::String(text);
        }
        object.insert("#text".to_string(), JsonValue::String(text));
    }

    if object.is_empty() {
        JsonValue::String(String::new())
    } else {
        JsonValue::Object(object)
    }
}

fn collect_xml_text_content(node: roxmltree::Node<'_, '_>) -> Option<String> {
    let parts = node
        .children()
        .filter(|child| child.is_text())
        .filter_map(|child| child.text())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}
