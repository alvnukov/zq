use super::Error;

pub(super) fn render_xml_output_native(values: &[zq::NativeValue]) -> Result<String, Error> {
    if values.is_empty() {
        return Ok(String::new());
    }
    let mut docs = Vec::with_capacity(values.len());
    for value in values {
        docs.push(render_xml_doc_native(value)?);
    }
    Ok(docs.join("\n"))
}

fn render_xml_doc_native(value: &zq::NativeValue) -> Result<String, Error> {
    let mut out = String::new();
    match value {
        zq::NativeValue::Object(map) if map.len() == 1 => {
            let (root, content) = map.iter().next().expect("single-key object must have one entry");
            if root != "#text" && !root.starts_with('@') && is_valid_xml_name(root) {
                write_xml_field_native(&mut out, root, content)?;
            } else {
                write_xml_field_native(&mut out, "root", value)?;
            }
        }
        _ => write_xml_field_native(&mut out, "root", value)?,
    }
    Ok(out)
}

fn write_xml_field_native(
    out: &mut String,
    name: &str,
    value: &zq::NativeValue,
) -> Result<(), Error> {
    match value {
        zq::NativeValue::Array(items) => {
            for item in items {
                write_xml_element_native(out, name, item)?;
            }
        }
        _ => write_xml_element_native(out, name, value)?,
    }
    Ok(())
}

fn write_xml_element_native(
    out: &mut String,
    name: &str,
    value: &zq::NativeValue,
) -> Result<(), Error> {
    if !is_valid_xml_name(name) {
        return Err(Error::Query(format!("encode xml: invalid element name `{name}`")));
    }

    match value {
        zq::NativeValue::Null => {
            out.push('<');
            out.push_str(name);
            out.push_str("/>");
            Ok(())
        }
        zq::NativeValue::Bool(_) | zq::NativeValue::Number(_) | zq::NativeValue::String(_) => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            out.push_str(&escape_xml_text(&xml_scalar_text(value)?));
            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
        zq::NativeValue::Array(items) => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            for item in items {
                write_xml_field_native(out, "item", item)?;
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
        zq::NativeValue::Object(fields) => {
            out.push('<');
            out.push_str(name);

            for (key, attr_value) in fields.iter().filter(|(k, _)| k.starts_with('@')) {
                let attr_name = &key[1..];
                if attr_name.is_empty() || !is_valid_xml_name(attr_name) {
                    return Err(Error::Query(format!(
                        "encode xml: invalid attribute name `{key}`"
                    )));
                }
                out.push(' ');
                out.push_str(attr_name);
                out.push_str("=\"");
                out.push_str(&escape_xml_attribute(&xml_scalar_text(attr_value)?));
                out.push('"');
            }

            let children_count = fields
                .keys()
                .filter(|key| !key.starts_with('@') && key.as_str() != "#text")
                .count();
            let text_value = fields.get("#text");

            if children_count == 0 && text_value.is_none() {
                out.push_str("/>");
                return Ok(());
            }

            out.push('>');
            if let Some(text_value) = text_value {
                out.push_str(&escape_xml_text(&xml_scalar_text(text_value)?));
            }
            for (child_name, child_value) in fields {
                if child_name.starts_with('@') || child_name == "#text" {
                    continue;
                }
                write_xml_field_native(out, child_name, child_value)?;
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
            Ok(())
        }
    }
}

fn xml_scalar_text(value: &zq::NativeValue) -> Result<String, Error> {
    match value {
        zq::NativeValue::Null => Ok(String::new()),
        zq::NativeValue::Bool(v) => Ok(v.to_string()),
        zq::NativeValue::Number(v) => Ok(v.to_string()),
        zq::NativeValue::String(v) => Ok(v.clone()),
        zq::NativeValue::Array(_) | zq::NativeValue::Object(_) => {
            Err(Error::Query("encode xml: scalar value expected".to_string()))
        }
    }
}

fn is_valid_xml_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().expect("name is not empty");
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

fn escape_xml_text(value: &str) -> String {
    value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn escape_xml_attribute(value: &str) -> String {
    escape_xml_text(value).replace('"', "&quot;").replace('\'', "&apos;")
}
