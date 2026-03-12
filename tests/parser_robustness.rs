use std::panic::{catch_unwind, AssertUnwindSafe};

use zq::{parse_native_input_values_with_format, NativeInputFormat};

fn parse_no_panic(input: &str, format: NativeInputFormat) -> Result<usize, String> {
    let result =
        catch_unwind(AssertUnwindSafe(|| parse_native_input_values_with_format(input, format)));
    match result {
        Ok(Ok(parsed)) => Ok(parsed.values.len()),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => panic!("parser panicked for format {format:?}"),
    }
}

#[test]
fn parses_valid_samples_for_each_format() {
    let cases = [
        (NativeInputFormat::Json, "{\"a\":1}\n{\"b\":2}"),
        (NativeInputFormat::Yaml, "a: 1\n---\nb: [1, 2, 3]\n"),
        (NativeInputFormat::Toml, "[app]\nname = \"zq\"\nport = 8080\n"),
        (NativeInputFormat::Csv, "name,age\nalice,10\nbob,20\n"),
        (NativeInputFormat::Xml, "<root><item id=\"1\">x</item></root>"),
    ];

    for (format, input) in cases {
        let count = parse_no_panic(input, format)
            .unwrap_or_else(|e| panic!("expected valid parse for {format:?}, got error: {e}"));
        assert!(count > 0, "expected at least one value for {format:?}");
    }
}

#[test]
fn rejects_malformed_inputs_without_panicking() {
    let malformed = [
        (NativeInputFormat::Json, "{\"a\":\n"),
        (NativeInputFormat::Yaml, "a: [1, 2"),
        (NativeInputFormat::Toml, "a = [1, 2"),
        (NativeInputFormat::Csv, "\"unterminated,cell\n1,2\n"),
        (NativeInputFormat::Xml, "<root><x></root>"),
    ];

    for (format, input) in malformed {
        let _ = parse_no_panic(input, format);
    }
}

#[test]
fn utf8_edge_inputs_do_not_panic() {
    let utf8_cases = [
        "{{- fail (printf \"Необходимо 🌍\" $.CurrentApp.name) }}",
        "ключ: значение\nemoji: \"😀 😃 😄\"\n",
        "<root><msg>Привет 👋 XML</msg></root>",
    ];

    let formats = [
        NativeInputFormat::Json,
        NativeInputFormat::Yaml,
        NativeInputFormat::Toml,
        NativeInputFormat::Csv,
        NativeInputFormat::Xml,
        NativeInputFormat::Auto,
    ];

    for input in utf8_cases {
        for format in formats {
            let _ = parse_no_panic(input, format);
        }
    }
}
