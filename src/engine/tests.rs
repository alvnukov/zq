use super::*;

#[test]
fn parse_doc_mode_contract() {
    assert_eq!(parse_doc_mode("first", None).expect("first"), DocMode::First);
    assert_eq!(parse_doc_mode("all", None).expect("all"), DocMode::All);
    assert_eq!(parse_doc_mode("index", Some(3)).expect("index"), DocMode::Index(3));
    assert!(matches!(parse_doc_mode("index", None), Err(Error::MissingDocIndex)));
    assert!(matches!(parse_doc_mode("x", None), Err(Error::InvalidDocMode(_))));
}

#[test]
fn run_jq_api_works_on_yaml_input() {
    let input = "a: 1\n";
    let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!(1)]);
    let native = run_jq_native(".a", input, QueryOptions::default()).expect("run jq native");
    assert_eq!(
        native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![serde_json::json!(1)]
    );
}

#[test]
fn run_jq_type_on_yaml_repos_uses_jq_type_names() {
    let input = r#"
repos:
  - !!str https://example.com/charts
  - !!map
    name: stable
    url: https://charts.example.com
"#;
    let out = run_jq(".repos[] | type", input, QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!("string"), serde_json::json!("object")]);
    let native =
        run_jq_native(".repos[] | type", input, QueryOptions::default()).expect("run jq native");
    assert_eq!(
        native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![serde_json::json!("string"), serde_json::json!("object")]
    );
}

#[test]
fn run_jq_api_reads_json_stream_even_with_default_doc_mode() {
    let input = "{\"a\":1}\n{\"a\":2}\n";
    let out = run_jq(".a", input, QueryOptions::default()).expect("run jq");
    assert_eq!(out, vec![serde_json::json!(1), serde_json::json!(2)]);
    let native = run_jq_native(".a", input, QueryOptions::default()).expect("run jq native stream");
    assert_eq!(
        native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![serde_json::json!(1), serde_json::json!(2)]
    );
}

#[test]
fn native_stream_direct_writer_support_matches_fast_subset() {
    assert!(supports_native_stream_json_direct_write("."));
    assert!(supports_native_stream_json_direct_write(".a"));
    assert!(supports_native_stream_json_direct_write("select(.id > 2) | .id"));
    assert!(!supports_native_stream_json_direct_write(".text | test(\"a.*\")"));
    assert!(!supports_native_stream_json_direct_write("inputs"));
}

#[test]
fn native_stream_direct_writer_emits_json_and_raw_string_output() {
    let mut json_out = Vec::new();
    let json_input = std::io::Cursor::new(br#"{"id":7,"group":2,"skip":9}"#.to_vec());
    let status = try_run_jq_native_stream_json_reader_write_options_native(
        "{id,group}",
        json_input,
        RunOptions::default(),
        &mut json_out,
        NativeJsonWriteOptions { compact: false, raw_output: false, join_output: false, indent: 2 },
    )
    .expect("direct writer");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(String::from_utf8(json_out).expect("utf8"), "{\n  \"id\": 7,\n  \"group\": 2\n}\n");

    let mut raw_out = Vec::new();
    let raw_input = std::io::Cursor::new(br#"{"text":"svc"}"#.to_vec());
    try_run_jq_native_stream_json_reader_write_options_native(
        ".text",
        raw_input,
        RunOptions::default(),
        &mut raw_out,
        NativeJsonWriteOptions { compact: true, raw_output: true, join_output: false, indent: 2 },
    )
    .expect("direct raw writer");
    assert_eq!(String::from_utf8(raw_out).expect("utf8"), "svc\n");
}

#[test]
fn native_stream_direct_writer_preserves_large_raw_integer() {
    let mut out = Vec::new();
    let input = std::io::Cursor::new(br#"{"n":123456789012345678901234567890}"#.to_vec());
    try_run_jq_native_stream_json_reader_write_options_native(
        ".n",
        input,
        RunOptions::default(),
        &mut out,
        NativeJsonWriteOptions { compact: true, raw_output: false, join_output: false, indent: 2 },
    )
    .expect("direct writer");
    assert_eq!(String::from_utf8(out).expect("utf8"), "123456789012345678901234567890\n");
}

#[test]
fn yaml_output_for_multiple_values_is_multidoc() {
    let out =
        format_output_yaml_documents(&[serde_json::json!({"a":1}), serde_json::json!({"b":2})])
            .expect("yaml output");
    assert!(out.contains("a: 1"));
    assert!(out.contains("---"));
    assert!(out.contains("b: 2"));
}

#[test]
fn yaml_output_uses_anchors_for_repeated_subtrees() {
    let out = format_output_yaml_documents_with_options(
        &[serde_json::json!({
            "left": {"x": [1, 2]},
            "right": {"x": [1, 2]}
        })],
        YamlFormatOptions { use_anchors: true, ..YamlFormatOptions::default() },
    )
    .expect("yaml output");
    assert!(out.contains("&left"), "yaml output must define a readable anchor name");
    assert!(out.contains("*left"), "yaml output must emit alias with the same readable name");

    let decoded: serde_yaml::Value = serde_yaml::from_str(&out).expect("decode anchored yaml");
    let expected: serde_yaml::Value =
        serde_yaml::from_str("left:\n  x:\n    - 1\n    - 2\nright:\n  x:\n    - 1\n    - 2\n")
            .expect("decode expected yaml");
    assert_eq!(decoded, expected);
}

#[test]
fn yaml_output_uses_merge_for_mapping_extensions() {
    let out = format_output_yaml_documents_with_options(
        &[serde_json::json!({
            "base": {"a": 1, "b": 2},
            "derived": {"a": 1, "b": 2, "c": 3}
        })],
        YamlFormatOptions { use_anchors: true, ..YamlFormatOptions::default() },
    )
    .expect("yaml output");

    let anchor_name = out
        .lines()
        .find_map(|line| {
            let (_, tail) = line.split_once('&')?;
            Some(tail.split(|c: char| c.is_whitespace()).next().unwrap_or_default().to_string())
        })
        .filter(|name| !name.is_empty())
        .expect("merge source anchor name");

    assert!(
        out.contains(&format!("&{anchor_name}")),
        "merge source should be anchored with readable name"
    );
    assert!(
        out.contains(&format!("<<: *{anchor_name}")),
        "derived mapping should use YAML merge against base anchor"
    );

    let decoded: serde_yaml::Value = serde_yaml::from_str(&out).expect("decode anchored yaml");
    let normalized = crate::yamlmerge::normalize_value_from_source(&out, decoded);
    let expected: serde_yaml::Value =
        serde_yaml::from_str("base:\n  a: 1\n  b: 2\nderived:\n  a: 1\n  b: 2\n  c: 3\n")
            .expect("decode expected yaml");
    assert_eq!(normalized, expected);
}

#[test]
fn yaml_output_uses_anchors_for_repeated_large_scalars() {
    let out = format_output_yaml_documents_with_options(
        &[serde_json::json!({
            "title": "global-shared-configuration",
            "backup_title": "global-shared-configuration",
            "fallback_title": "global-shared-configuration"
        })],
        YamlFormatOptions { use_anchors: true, ..YamlFormatOptions::default() },
    )
    .expect("yaml output");
    assert!(out.contains("&title"), "yaml output must define readable scalar anchor");
    assert!(out.contains("*title"), "yaml output must reuse readable scalar anchor");
}

#[test]
fn yaml_output_does_not_anchor_short_scalars_even_when_enabled() {
    let out = format_output_yaml_documents_with_options(
        &[serde_json::json!({
            "env": "dev",
            "default_env": "dev",
            "current_env": "dev"
        })],
        YamlFormatOptions { use_anchors: true, ..YamlFormatOptions::default() },
    )
    .expect("yaml output");
    assert!(!out.contains('&'), "short scalar aliases should stay disabled for readability");
    assert!(!out.contains('*'), "short scalar aliases should stay disabled for readability");
}

#[test]
fn yaml_output_without_anchor_option_preserves_plain_output() {
    let out = format_output_yaml_documents(&[serde_json::json!({
        "env": "dev",
        "default_env": "dev"
    })])
    .expect("yaml output");
    assert!(!out.contains('&'), "anchors must be opt-in");
    assert!(!out.contains('*'), "aliases must be opt-in");
}

#[test]
fn yaml_anchor_names_are_sanitized_and_key_based() {
    let out = format_output_yaml_documents_with_options(
        &[serde_json::json!({
            "service-config": {"cfg": {"ports": [80, 443]}},
            "backup-config": {"cfg": {"ports": [80, 443]}}
        })],
        YamlFormatOptions { use_anchors: true, ..YamlFormatOptions::default() },
    )
    .expect("yaml output");
    assert!(
        out.contains("&service_config"),
        "anchor name should be based on first key path and sanitized"
    );
    assert!(
        out.contains("*service_config"),
        "alias should use the same sanitized human-readable name"
    );
}

#[test]
fn yaml_strict_friendly_mode_makes_anchor_names_shorter() {
    let input = serde_json::json!({
        "cluster-metrics-apiversion": {"x": [1, 2]},
        "other-node": {"x": [1, 2]},
    });
    let friendly = format_output_yaml_documents_with_options(
        std::slice::from_ref(&input),
        YamlFormatOptions {
            use_anchors: true,
            anchor_name_mode: YamlAnchorNameMode::Friendly,
            ..YamlFormatOptions::default()
        },
    )
    .expect("friendly yaml output");
    let strict = format_output_yaml_documents_with_options(
        std::slice::from_ref(&input),
        YamlFormatOptions {
            use_anchors: true,
            anchor_name_mode: YamlAnchorNameMode::StrictFriendly,
            ..YamlFormatOptions::default()
        },
    )
    .expect("strict-friendly yaml output");

    let first_anchor_name = |text: &str| -> Option<String> {
        text.lines().find_map(|line| {
            let amp = line.find('&')?;
            let tail = &line[amp + 1..];
            Some(tail.split(|c: char| c.is_whitespace()).next().unwrap_or_default().to_string())
        })
    };

    let friendly_name = first_anchor_name(&friendly).expect("friendly anchor name");
    let strict_name = first_anchor_name(&strict).expect("strict anchor name");

    assert!(
        strict_name.len() <= friendly_name.len(),
        "strict-friendly mode should not generate longer names"
    );
}

#[test]
fn yaml_anchor_name_dictionaries_load_from_assets() {
    let dicts = anchor_name_dictionaries();
    assert!(
        dicts.stopwords_common.contains("default"),
        "common stopwords dictionary should be loaded from assets"
    );
    assert_eq!(
        dicts.canonical_common.get("configuration").map(String::as_str),
        Some("config"),
        "common canonical dictionary should be loaded from assets"
    );
    assert_eq!(
        dicts.canonical_strict.get("deployment").map(String::as_str),
        Some("deploy"),
        "strict canonical dictionary should be loaded from assets"
    );
}

#[test]
fn yaml_anchor_strict_mode_keeps_first_context_token() {
    assert_eq!(
        normalize_anchor_component(
            "default_serviceaccount_map",
            YamlAnchorNameMode::StrictFriendly
        ),
        "default_sa"
    );
    assert_eq!(
        normalize_anchor_component("common_serviceaccount_map", YamlAnchorNameMode::StrictFriendly),
        "common_sa"
    );
}

#[test]
fn yaml_anchor_strict_mode_keeps_default_and_common_distinct() {
    let default_name = normalize_anchor_component(
        "default_serviceaccount_map",
        YamlAnchorNameMode::StrictFriendly,
    );
    let common_name =
        normalize_anchor_component("common_serviceaccount_map", YamlAnchorNameMode::StrictFriendly);
    assert_eq!(default_name, "default_sa");
    assert_eq!(common_name, "common_sa");
    assert_ne!(
        default_name, common_name,
        "default_* and common_* should stay semantically distinct"
    );
}

#[test]
fn yaml_anchor_strict_dictionary_covers_k8s_openapi_and_ci_terms() {
    assert_eq!(
        canonicalize_anchor_token(
            "customresourcedefinition".to_string(),
            YamlAnchorNameMode::StrictFriendly
        ),
        "crd"
    );
    assert_eq!(
        canonicalize_anchor_token("requestbody".to_string(), YamlAnchorNameMode::StrictFriendly),
        "reqbody"
    );
    assert_eq!(
        canonicalize_anchor_token("workflow".to_string(), YamlAnchorNameMode::StrictFriendly),
        "wf"
    );
    assert_eq!(
        canonicalize_anchor_token("metadata".to_string(), YamlAnchorNameMode::StrictFriendly),
        "meta"
    );
}

#[test]
fn yaml_anchor_strict_mode_avoids_single_letter_tokens() {
    let meta = normalize_anchor_component("metadata_name_map", YamlAnchorNameMode::StrictFriendly);
    assert_eq!(meta, "meta_name");
    assert!(
        meta.split('_').all(|token| token.len() != 1),
        "strict mode should avoid single-letter tokens"
    );
    assert_eq!(
        normalize_anchor_component("kind_serviceaccount_map", YamlAnchorNameMode::StrictFriendly),
        "kind_sa"
    );
    assert_eq!(
        normalize_anchor_component("f_available_map", YamlAnchorNameMode::StrictFriendly),
        "available"
    );
    assert_eq!(normalize_anchor_component("f", YamlAnchorNameMode::StrictFriendly), "field");
    assert_eq!(
        normalize_anchor_component("source_repourl_map", YamlAnchorNameMode::StrictFriendly),
        "source_repo_url"
    );
}

#[test]
fn split_anchor_tokens_handles_camel_case_and_acronyms() {
    assert_eq!(split_anchor_tokens("apiVersion"), vec!["api", "version"]);
    assert_eq!(split_anchor_tokens("managedFieldsTime"), vec!["managed", "fields", "time"]);
    assert_eq!(split_anchor_tokens("HTTPRoute"), vec!["http", "route"]);
    assert_eq!(split_anchor_tokens("ipv6Address"), vec!["ipv", "6", "address"]);
}

#[test]
fn format_query_error_control_character_matches_jq_text() {
    let err = serde_json::from_str::<serde_json::Value>("\"\u{1}\"")
        .expect_err("must fail on unescaped control char");
    let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
    assert_eq!(
            msg,
            "jq: parse error: Invalid string: control characters from U+0000 through U+001F must be escaped at line 1, column 3"
        );
}

#[test]
fn format_query_error_colon_in_object_matches_jq_text() {
    let err = serde_json::from_str::<serde_json::Value>("{\"a\":1,\"b\",")
        .expect_err("must fail on malformed object");
    let msg = format_query_error("jq", "", &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Objects must consist of key:value pairs at line 1, column 11"
    );
}

#[test]
fn format_query_error_string_key_after_object_start_matches_jq_text() {
    let input = "{{\"a\":\"b\"}}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(msg, "jq: parse error: Expected string key after '{', not '{' at line 1, column 2");
}

#[test]
fn format_query_error_string_key_after_comma_matches_jq_text() {
    let input = "{\"x\":\"y\",{\"a\":\"b\"}}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after ',' in object, not '{' at line 1, column 10"
    );
}

#[test]
fn format_query_error_string_key_array_after_object_start_matches_jq_text() {
    let input = "{[\"a\",\"b\"]}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(msg, "jq: parse error: Expected string key after '{', not '[' at line 1, column 2");
}

#[test]
fn format_query_error_string_key_array_after_comma_matches_jq_text() {
    let input = "{\"x\":\"y\",[\"a\",\"b\"]}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert_eq!(
        msg,
        "jq: parse error: Expected string key after ',' in object, not '[' at line 1, column 10"
    );
}

#[test]
fn format_query_error_string_key_after_comma_handles_utf8_weird_sequences() {
    let input = "{\"🚀\":\"ok\",{\"a\":1}}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("must fail");
    let msg = format_query_error("jq", input, &crate::QueryError::Json(err));
    assert!(
        msg.contains("Expected string key after ',' in object, not '{'")
            || msg.contains("key must be a string")
    );
    assert!(msg.contains("line 1, column"));
}

#[test]
fn strip_serde_line_col_suffix_only_removes_valid_suffix() {
    assert_eq!(strip_serde_line_col_suffix("expected value at line 1 column 2"), "expected value");
    assert_eq!(
        strip_serde_line_col_suffix("hello at line nope column 2"),
        "hello at line nope column 2"
    );
}

#[test]
fn json_output_escapes_del_like_jq() {
    let out = format_output_json_lines(&[serde_json::json!(" ~\u{007f}")], true, false)
        .expect("json output");
    assert_eq!(out, "\" ~\\u007f\"");
}

#[test]
fn json_output_pretty_mode_matches_contract() {
    let out = format_output_json_lines(&[serde_json::json!({"a": 1})], false, false)
        .expect("pretty json output");
    assert_eq!(out, "{\n  \"a\": 1\n}");

    let out = format_output_json_lines(&[serde_json::json!(" ~\u{007f}")], false, false)
        .expect("pretty json string output");
    assert_eq!(out, "\" ~\\u007f\"");
}

#[test]
fn format_runtime_error_matches_jq_prefix() {
    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Runtime("Cannot index object with number".to_string()),
    );
    assert_eq!(msg, "jq: error (at <stdin>:1): Cannot index object with number");
}

#[test]
fn engine_wrapper_helpers_cover_contract() {
    assert!(validate_jq_query(".").is_ok());
    assert!(validate_jq_query_with_paths(".", &[]).is_ok());
    assert!(validate_jq_query("if").is_err());

    let prepared = prepare_jq_query_with_paths(".", &[]).expect("prepare query");
    assert_eq!(prepared.run_jsonish_lines("1").expect("prepared run"), vec!["1".to_string()]);
    assert_eq!(
        prepared.run_jsonish_lines_lenient("1").expect("prepared run lenient"),
        vec!["1".to_string()]
    );
    assert_eq!(
        run_jq_jsonish_lines(".", "1", &[]).expect("run jsonish lines"),
        vec!["1".to_string()]
    );
    assert_eq!(
        normalize_jsonish_line("{\"a\":1}").expect("normalize jsonish"),
        "{\"a\":1}".to_string()
    );
    assert!(jsonish_equal("1", "1").expect("jsonish compare"));
}

#[test]
fn parse_json_only_and_doc_selection_contract() {
    let values = parse_jq_json_values_only("1\n2\n").expect("parse json stream");
    assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)]);
    let values_native =
        parse_jq_json_values_only_native("1\n2\n").expect("parse json stream native");
    assert_eq!(
        values_native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![serde_json::json!(1), serde_json::json!(2)]
    );
    assert!(parse_jq_json_values_only("a: 1\n").is_err());

    let yaml_docs = "a: 1\n---\na: 2\n";
    let selected = parse_jq_input_values(yaml_docs, DocMode::Index(1), "jq")
        .expect("select yaml doc by index");
    assert_eq!(selected, vec![serde_json::json!({"a": 2})]);
    let selected_native = parse_jq_input_values_native(yaml_docs, DocMode::Index(1), "jq")
        .expect("select yaml doc by index native");
    assert_eq!(
        selected_native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![serde_json::json!({"a": 2})]
    );

    let out_of_range = parse_jq_input_values(yaml_docs, DocMode::Index(3), "jq")
        .expect_err("doc index out of range");
    assert!(matches!(out_of_range, Error::DocIndexOutOfRange { tool: "jq", index: 3, total: 2 }));
}

#[test]
fn stream_wrappers_and_output_formatters_contract() {
    let out = run_jq_stream_with_paths_options(
        ".",
        vec![serde_json::json!(1)],
        &[],
        RunOptions { null_input: true },
    )
    .expect("run jq stream with null-input");
    assert_eq!(out, vec![JsonValue::Null]);
    let out_native = run_jq_stream_with_paths_options_native(
        ".",
        vec![ZqValue::from_json(serde_json::json!(1))],
        &[],
        RunOptions { null_input: true },
    )
    .expect("run jq stream native with null-input");
    assert_eq!(
        out_native.into_iter().map(ZqValue::into_json).collect::<Vec<_>>(),
        vec![JsonValue::Null]
    );

    let mut emitted = Vec::new();
    let status = try_run_jq_native_stream_with_paths_options(
        ".a",
        &[serde_json::json!({"a": 1})],
        RunOptions::default(),
        |v| {
            emitted.push(v);
            Ok(())
        },
    )
    .expect("native stream executes");
    assert_eq!(status, NativeStreamStatus::Executed);
    assert_eq!(emitted, vec![serde_json::json!(1)]);

    let unsupported = try_run_jq_native_stream_with_paths_options(
        "label $out | .",
        &[serde_json::json!(1)],
        RunOptions::default(),
        |_| Ok(()),
    )
    .expect("unsupported native stream status");
    assert_eq!(unsupported, NativeStreamStatus::Executed);

    let sink_error = try_run_jq_native_stream_with_paths_options(
        ".",
        &[serde_json::json!(1)],
        RunOptions::default(),
        |_| Err("sink failed".to_string()),
    )
    .expect_err("sink error must surface");
    assert_eq!(
        format!("{sink_error}"),
        "sink failed",
        "native sink error must map to runtime error"
    );

    let json_out = format_output_json_lines(
        &[serde_json::json!("x"), serde_json::json!(1), serde_json::json!(" ~\u{007f}")],
        true,
        true,
    )
    .expect("format json");
    assert_eq!(json_out, "x\n1\n ~\u{7f}");

    assert_eq!(format_output_yaml_documents(&[]).expect("yaml empty"), String::new());
}

#[test]
fn format_query_error_adds_context_and_compile_forms() {
    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Unsupported("Top-level program not given (try \".\")".to_string()),
    );
    assert!(msg.contains("jq: 1 compile error"));

    let msg = format_query_error(
        "jq",
        "",
        &crate::QueryError::Unsupported(
            "too many function parameters or local function definitions (max 4095)".to_string(),
        ),
    );
    assert!(msg.contains("jq: 1 compile error"));

    let input = "line1\nline2\nline3\n";
    let msg = format_query_error(
        "jq",
        input,
        &crate::QueryError::Unsupported("boom at line 2, column 3".to_string()),
    );
    assert!(msg.contains("--> <stdin>:2:3"));
    assert!(msg.contains("2 | line2"));
    assert!(msg.contains("^"));

    let query = "lineA\nlineB\nlineC\n";
    let msg = format_query_error_with_sources(
        "jq",
        query,
        input,
        &crate::QueryError::Unsupported("boom at line 2, column 3".to_string()),
    );
    assert!(msg.contains("--> <query>:2:3"));
    assert!(msg.contains("2 | lineB"));
    assert!(msg.contains("^"));

    let msg = format_query_error_with_sources(
        "jq",
        ".foo | if",
        input,
        &crate::QueryError::Unsupported(
            "query is not supported by native engine: .foo | if".to_string(),
        ),
    );
    assert!(!msg.contains("unsupported query"));
    assert!(!msg.contains("not supported"));
    assert!(msg.contains("jq: error:"));
    assert!(msg.contains("--> <query>:1:1"));
    assert!(msg.contains("jq: 1 compile error"));

    let unterminated = "[\n  try if .\n         then 1\n         else 2\n  catch ]";
    let msg = format_query_error_with_sources(
        "jq",
        unterminated,
        "",
        &crate::QueryError::Unsupported("parse error: expected EndKw, found Catch".to_string()),
    );
    assert!(msg.contains("unexpected catch"));
    assert!(msg.contains("Possibly unterminated 'if' statement"));
    assert!(msg.contains("Possibly unterminated 'try' statement"));
    assert!(msg.contains("jq: 3 compile errors"));

    let msg = format_query_error_with_sources(
        "jq",
        "if\n",
        "",
        &crate::QueryError::Unsupported("syntax error, unexpected end of file".to_string()),
    );
    assert!(msg.contains("unexpected end of file at <top-level>"));
    assert!(msg.contains("line 1, column 3"));
    assert!(msg.contains("jq: 1 compile error"));
}

#[test]
fn format_query_error_unterminated_try_if_with_utf8_weird_sequences() {
    let query = "😀\ntry if .message == \"👩‍💻\" then 1 else 2\ncatch ]";
    let msg = format_query_error_with_sources(
        "jq",
        query,
        "",
        &crate::QueryError::Unsupported("parse error: expected EndKw, found Catch".to_string()),
    );
    assert!(msg.contains("unexpected catch"));
    assert!(msg.contains("Possibly unterminated 'if' statement"));
    assert!(msg.contains("Possibly unterminated 'try' statement"));
    assert!(msg.contains("jq: 3 compile errors"));
}
