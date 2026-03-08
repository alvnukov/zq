use super::*;
use fs2::FileExt;
use std::sync::{Mutex, OnceLock};

fn parse_cli_for_test(args: &[&str]) -> Cli {
    let mut all = Vec::with_capacity(args.len() + 1);
    all.push("zq");
    all.extend_from_slice(args);
    Cli::parse_from(all)
}

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[test]
fn cursor_parses_compile_fail_case_mode() {
    let mut cursor = TestCursor::new("%%FAIL\n@\nplaceholder\n\n.\nnull\nnull\n");
    let fail_case = cursor.next_case_program().expect("first case");
    assert!(matches!(
        fail_case.mode,
        RunTestMode::CompileFail {
            check_message: true
        }
    ));
    assert_eq!(fail_case.program, "@");

    let payload = cursor
        .read_case_payload(fail_case.mode)
        .expect("fail payload");
    match payload {
        CasePayload::CompileFail(payload) => {
            assert_eq!(payload.expected_error_line, "placeholder");
        }
        CasePayload::Query(_) => panic!("unexpected payload kind"),
    }

    let next_case = cursor.next_case_program().expect("next case");
    assert!(matches!(next_case.mode, RunTestMode::Query));
    assert_eq!(next_case.program, ".");
}

#[test]
fn cursor_reads_query_payload_until_separator() {
    let mut cursor = TestCursor::new(".\n1\n1\n2\n\n");
    let case = cursor.next_case_program().expect("case");
    let payload = cursor.read_case_payload(case.mode).expect("payload");
    match payload {
        CasePayload::CompileFail(_) => panic!("unexpected payload kind"),
        CasePayload::Query(payload) => {
            assert_eq!(payload.input_line_no, 2);
            assert_eq!(payload.input_line, "1");
            assert_eq!(
                payload.expected_lines,
                vec![(3usize, "1".to_string()), (4usize, "2".to_string())]
            );
        }
    }
}

#[test]
fn cursor_skip_case_payload_moves_to_next_case() {
    let mut cursor = TestCursor::new(".\nnull\nnull\n\n.[0]\n[1,2]\n1\n\n");
    let first = cursor.next_case_program().expect("first");
    cursor.skip_case_payload(first.mode);

    let second = cursor.next_case_program().expect("second");
    assert_eq!(second.program, ".[0]");
    assert!(matches!(second.mode, RunTestMode::Query));
}

#[test]
fn run_tests_runtime_error_without_expected_outputs_is_treated_as_stream_end() {
    let case = TestCaseProgram {
        program_line_no: 1,
        program: ".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a".to_string(),
        mode: RunTestMode::Query,
    };
    let payload = QueryCasePayload {
        input_line_no: 2,
        input_line: "[[3],[4],[5],6]".to_string(),
        expected_lines: Vec::new(),
    };
    let mut compile_cache = std::collections::HashMap::new();
    let mut stats = RunTestsStats {
        tests: 1,
        passed: 0,
        invalid: 0,
    };

    run_query_case(&case, payload, &[], &mut compile_cache, &mut stats);
    assert_eq!(
        stats.passed, 1,
        "runtime error must terminate stream like jq"
    );
    assert_eq!(stats.invalid, 0);
}

#[test]
fn raw_input_lines_follow_jq_semantics() {
    assert_eq!(
        raw_input_lines("a\nb\nc\n"),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(
        raw_input_lines("a\r\nb\r\nc"),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert!(raw_input_lines("").is_empty());
}

#[test]
fn exit_status_contract_matches_jq() {
    assert_eq!(exit_status_from_outputs(&[]), 4);
    assert_eq!(exit_status_from_outputs(&[JsonValue::Null]), 1);
    assert_eq!(exit_status_from_outputs(&[JsonValue::Bool(false)]), 1);
    assert_eq!(exit_status_from_outputs(&[JsonValue::Bool(true)]), 0);
    assert_eq!(exit_status_from_outputs(&[JsonValue::Number(1.into())]), 0);
    assert_eq!(exit_status_from_outputs_native(&[zq::NativeValue::Null]), 1);
    assert_eq!(
        exit_status_from_outputs_native(&[zq::NativeValue::Bool(false)]),
        1
    );
    assert_eq!(
        exit_status_from_outputs_native(&[zq::NativeValue::Bool(true)]),
        0
    );
    assert_eq!(
        exit_status_from_outputs_native(&[zq::NativeValue::from(1i64)]),
        0
    );
}

#[test]
fn seq_parser_matches_jq_truncated_messages() {
    let rs = '\u{1e}';
    let input =
        format!("1{rs}2 3\n[0,1{rs}[4,5]true\"ab\"{{\"c\":4{rs}{{}}{{\"d\":5,\"e\":6\"{rs}false\n");
    let parsed = parse_json_seq_input(&input);
    assert_eq!(
        parsed.values,
        vec![
            serde_json::json!(2),
            serde_json::json!(3),
            serde_json::json!([4, 5]),
            serde_json::json!(true),
            serde_json::json!("ab"),
            serde_json::json!({}),
            serde_json::json!(false),
        ]
    );
    assert_eq!(
        parsed.errors,
        vec![
            "Truncated value at line 2, column 5".to_string(),
            "Truncated value at line 2, column 25".to_string(),
            "Truncated value at line 2, column 41".to_string(),
        ]
    );
}

#[test]
fn seq_parser_reports_unfinished_abandoned_text_at_eof() {
    let parsed = parse_json_seq_input("\"foo");
    assert_eq!(
        parsed.errors,
        vec!["Unfinished abandoned text at EOF at line 1, column 4".to_string()]
    );

    let parsed = parse_json_seq_input("1");
    assert_eq!(
        parsed.errors,
        vec!["Unfinished abandoned text at EOF at line 1, column 1".to_string()]
    );

    let parsed = parse_json_seq_input("1\n");
    assert_eq!(
        parsed.errors,
        vec!["Unfinished abandoned text at EOF at line 2, column 0".to_string()]
    );
}

#[test]
fn inputs_builtin_detection_ignores_strings() {
    assert!(query_uses_inputs_builtin("[inputs]"));
    assert!(query_uses_inputs_builtin("input | ."));
    assert!(!query_uses_inputs_builtin("\"inputs\""));
    assert!(!query_uses_inputs_builtin(".foo"));
}

#[test]
fn stderr_builtin_detection_ignores_strings() {
    assert!(query_uses_stderr_builtin("stderr"));
    assert!(query_uses_stderr_builtin(". | stderr"));
    assert!(query_uses_stderr_builtin("debug, stderr"));
    assert!(!query_uses_stderr_builtin("\"stderr\""));
    assert!(!query_uses_stderr_builtin(".foo"));
}

#[test]
fn builtin_detection_handles_escaped_strings() {
    assert!(query_uses_inputs_builtin(r#""a\"b" | inputs"#));
    assert!(query_uses_stderr_builtin(r#""a\"b" | stderr"#));
}

#[test]
fn compat_cli_parser_handles_named_and_positional_args() {
    let args = vec![
        "zq".to_string(),
        "-n".to_string(),
        "-c".to_string(),
        "--arg".to_string(),
        "foo".to_string(),
        "1".to_string(),
        "--argjson".to_string(),
        "bar".to_string(),
        "2".to_string(),
        "$ARGS.positional".to_string(),
        "--args".to_string(),
        "x".to_string(),
        "--jsonargs".to_string(),
        "3".to_string(),
        "{}".to_string(),
    ];

    let (filtered, compat) = extract_cli_compat_args(args).expect("parse");
    assert_eq!(
        filtered,
        vec![
            "zq".to_string(),
            "-n".to_string(),
            "-c".to_string(),
            "$ARGS.positional".to_string()
        ]
    );
    assert_eq!(
        compat.named_vars.get("foo"),
        Some(&zq::NativeValue::from_json(serde_json::json!("1")))
    );
    assert_eq!(
        compat.named_vars.get("bar"),
        Some(&zq::NativeValue::from_json(serde_json::json!(2)))
    );
    assert_eq!(
        compat.positional_args,
        vec![
            zq::NativeValue::from_json(serde_json::json!("x")),
            zq::NativeValue::from_json(serde_json::json!(3)),
            zq::NativeValue::from_json(serde_json::json!({}))
        ]
    );
}

#[test]
fn compat_cli_parser_accepts_args_before_query() {
    let args = vec![
        "zq".to_string(),
        "-n".to_string(),
        "--args".to_string(),
        "$ARGS.positional".to_string(),
        "foo".to_string(),
        "bar".to_string(),
    ];

    let (filtered, compat) = extract_cli_compat_args(args).expect("parse");
    assert_eq!(
        filtered,
        vec![
            "zq".to_string(),
            "-n".to_string(),
            "$ARGS.positional".to_string()
        ]
    );
    assert_eq!(
        compat.positional_args,
        vec![
            zq::NativeValue::from_json(serde_json::json!("foo")),
            zq::NativeValue::from_json(serde_json::json!("bar"))
        ]
    );
}

#[test]
fn compat_cli_parser_preserves_double_dash_before_query_in_args_mode() {
    let args = vec![
        "zq".to_string(),
        "--args".to_string(),
        "-rn".to_string(),
        "--".to_string(),
        "$ARGS.positional[0]".to_string(),
        "bar".to_string(),
    ];

    let (filtered, compat) = extract_cli_compat_args(args).expect("parse");
    assert_eq!(
        filtered,
        vec![
            "zq".to_string(),
            "-rn".to_string(),
            "--".to_string(),
            "$ARGS.positional[0]".to_string(),
        ]
    );
    assert_eq!(
        compat.positional_args,
        vec![zq::NativeValue::from_json(serde_json::json!("bar"))]
    );
}

#[test]
fn compat_cli_parser_rejects_invalid_jsonargs() {
    let args = vec![
        "zq".to_string(),
        "-n".to_string(),
        ".".to_string(),
        "--jsonargs".to_string(),
        "null".to_string(),
        "invalid".to_string(),
    ];

    let err = extract_cli_compat_args(args).expect_err("must fail");
    assert!(matches!(err, Error::Io(_)));
}

#[test]
fn compat_cli_parser_switches_modes_and_collects_tail_after_double_dash() {
    let args = vec![
        "zq".to_string(),
        "-n".to_string(),
        ".".to_string(),
        "--args".to_string(),
        "a".to_string(),
        "--jsonargs".to_string(),
        "1".to_string(),
        "--args".to_string(),
        "b".to_string(),
        "--".to_string(),
        "c".to_string(),
        "d".to_string(),
    ];

    let (filtered, compat) = extract_cli_compat_args(args).expect("parse");
    assert_eq!(
        filtered,
        vec!["zq".to_string(), "-n".to_string(), ".".to_string()]
    );
    assert_eq!(
        compat.positional_args,
        vec![
            zq::NativeValue::from_json(serde_json::json!("a")),
            zq::NativeValue::from_json(serde_json::json!(1)),
            zq::NativeValue::from_json(serde_json::json!("b")),
            zq::NativeValue::from_json(serde_json::json!("c")),
            zq::NativeValue::from_json(serde_json::json!("d")),
        ]
    );
}

#[test]
fn compat_cli_parser_reports_missing_flag_values() {
    let cases = [
        (
            vec!["zq".to_string(), "--arg".to_string(), "name".to_string()],
            "--arg requires two arguments",
        ),
        (
            vec![
                "zq".to_string(),
                "--argjson".to_string(),
                "name".to_string(),
            ],
            "--argjson requires two arguments",
        ),
        (
            vec![
                "zq".to_string(),
                "--slurpfile".to_string(),
                "name".to_string(),
            ],
            "--slurpfile requires two arguments",
        ),
        (
            vec![
                "zq".to_string(),
                "--rawfile".to_string(),
                "name".to_string(),
            ],
            "--rawfile requires two arguments",
        ),
    ];

    for (args, msg) in cases {
        let err = extract_cli_compat_args(args).expect_err("must fail");
        assert!(format!("{err}").contains(msg), "{err}");
    }
}

#[test]
fn compat_cli_parser_rejects_invalid_slurpfile_json() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let bad = td.path().join("bad.json");
    std::fs::write(&bad, "{").expect("write bad json");

    let args = vec![
        "zq".to_string(),
        "-n".to_string(),
        "--slurpfile".to_string(),
        "foo".to_string(),
        bad.to_string_lossy().into_owned(),
        ".".to_string(),
    ];

    let err = extract_cli_compat_args(args).expect_err("must fail");
    assert!(format!("{err}").contains("--slurpfile"));
    assert!(matches!(err, Error::Io(_)));
}

#[test]
fn build_query_injects_empty_args_object_when_query_uses_args() {
    let wrapped = build_query_with_cli_compat("$ARGS.positional", &CliCompatArgs::default())
        .expect("wrap query");
    assert!(wrapped.contains("as $ARGS"));
    assert!(wrapped.contains("\"positional\":[]"));
}

#[test]
fn build_query_rejects_invalid_variable_names() {
    let mut compat = CliCompatArgs::default();
    compat.named_vars.insert(
        "1bad".to_string(),
        zq::NativeValue::from_json(serde_json::json!("x")),
    );
    let err = build_query_with_cli_compat(".", &compat).expect_err("must fail");
    assert!(format!("{err}").contains("invalid variable name"));
}

#[test]
fn build_query_with_cli_compat_injects_named_and_args_bindings() {
    let mut compat = CliCompatArgs::default();
    compat.named_vars.insert(
        "date".to_string(),
        zq::NativeValue::from_json(serde_json::json!("xx 03 yy 2026 at 16:03:45")),
    );
    compat.named_args = compat.named_vars.clone();
    compat.positional_args = vec![
        zq::NativeValue::from_json(serde_json::json!("a")),
        zq::NativeValue::from_json(serde_json::json!("b")),
    ];
    let wrapped =
        build_query_with_cli_compat("$ARGS.positional[1], $date", &compat).expect("wrapped query");
    assert!(wrapped.contains("as $ARGS"));
    assert!(wrapped.contains("as $date"));
    assert!(wrapped.contains("$ARGS.positional[1], $date"));
}

#[test]
fn build_query_with_cli_compat_preserves_query_without_compat_args() {
    let wrapped = build_query_with_cli_compat("{$foo, $bar} | .", &CliCompatArgs::default())
        .expect("wrapped query");
    assert_eq!(wrapped, "{$foo, $bar} | .");
}

#[test]
fn build_query_with_cli_compat_rejects_invalid_var_name_before_wrapping() {
    let mut compat = CliCompatArgs::default();
    compat.named_vars.insert(
        "9bad".to_string(),
        zq::NativeValue::from_json(serde_json::json!("x")),
    );
    let err = build_query_with_cli_compat("$ARGS.positional", &compat).expect_err("must fail");
    assert!(format!("{err}").contains("invalid variable name"));
}

#[test]
fn stream_json_values_matches_jq_shape_for_arrays() {
    let events = stream_json_values(vec![serde_json::json!([1, 2])]);
    assert_eq!(
        events,
        vec![
            serde_json::json!([[0], 1]),
            serde_json::json!([[1], 2]),
            serde_json::json!([[1]]),
        ]
    );
}

#[test]
fn stream_json_values_handles_empty_containers() {
    let events = stream_json_values(vec![serde_json::json!([]), serde_json::json!({})]);
    assert_eq!(
        events,
        vec![serde_json::json!([[], []]), serde_json::json!([[], {}])]
    );
}

#[test]
fn stream_error_value_matches_jq_contract() {
    let input = "[";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("invalid json");
    let event = stream_error_value_from_json_error(input, &err);
    assert_eq!(
        event,
        serde_json::json!(["Unfinished JSON term at EOF at line 1, column 1", [0]])
    );
}

#[test]
fn stream_error_value_tracks_object_key_path() {
    let input = "{\"a\":1";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("invalid json");
    let event = stream_error_value_from_json_error(input, &err);
    assert_eq!(
        event,
        serde_json::json!(["Unfinished JSON term at EOF at line 1, column 6", ["a"]])
    );
}

#[test]
fn stream_error_value_tracks_nested_array_index_path() {
    let input = "{\"a\":[1,2";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("invalid json");
    let event = stream_error_value_from_json_error(input, &err);
    assert_eq!(
        event,
        serde_json::json!(["Unfinished JSON term at EOF at line 1, column 9", ["a", 1]])
    );
}

#[test]
fn stream_error_value_uses_null_path_for_missing_colon() {
    let input = "{\"a\" 1}";
    let err = serde_json::from_str::<serde_json::Value>(input).expect_err("invalid json");
    let event = stream_error_value_from_json_error(input, &err);
    assert_eq!(
        event,
        serde_json::json!([
            "Objects must consist of key:value pairs at line 1, column 6",
            [null]
        ])
    );
}

#[test]
fn json_scanner_helper_contract() {
    assert_eq!(line_col_to_byte_index("a", 0, 1), None);
    assert_eq!(line_col_to_byte_index("a", 1, 0), None);
    assert_eq!(line_col_to_byte_index("a\nb", 2, 1), Some(2));
    assert_eq!(line_col_to_byte_index("a\nb", 3, 1), Some(3));

    let mut i = 0usize;
    assert_eq!(scan_json_string(b"x", &mut i, 1), None);

    let raw = r#""a\"b""#;
    let mut i = 0usize;
    assert_eq!(
        scan_json_string(raw.as_bytes(), &mut i, raw.len()),
        Some("a\"b".to_string())
    );
    assert_eq!(i, raw.len());

    let num = "-12.34e+5";
    let mut i = 0usize;
    assert!(scan_json_number(num.as_bytes(), &mut i, num.len()));
    assert_eq!(i, num.len());

    let mut i = 0usize;
    assert!(!scan_json_number(b"-.1", &mut i, 3));
    let mut i = 0usize;
    assert!(!scan_json_number(b"1e+", &mut i, 3));

    let mut i = 0usize;
    assert!(scan_json_literal(b"true", &mut i, 4, b"true"));
    assert_eq!(i, 4);
    let mut i = 0usize;
    assert!(!scan_json_literal(b"tru", &mut i, 3, b"true"));
    let mut i = 0usize;
    assert!(!scan_json_literal(b"tree", &mut i, 4, b"true"));
}

#[test]
fn json_scan_state_machine_contract() {
    let mut frames = vec![JsonScanFrame::Array {
        index: 0,
        state: JsonArrayState::ValueOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b"]",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut frames = vec![JsonScanFrame::Array {
        index: 0,
        state: JsonArrayState::CommaOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b",",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(matches!(
        frames[0],
        JsonScanFrame::Array {
            index: 1,
            state: JsonArrayState::ValueOrEnd
        }
    ));

    let mut frames = vec![JsonScanFrame::Array {
        index: 0,
        state: JsonArrayState::CommaOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b"]",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut frames = vec![JsonScanFrame::Array {
        index: 0,
        state: JsonArrayState::CommaOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(!advance_json_scan(
        b"x",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));

    let mut frames = vec![JsonScanFrame::Object {
        key: None,
        state: JsonObjectState::KeyOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b"}",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let raw = r#""a""#;
    let mut frames = vec![JsonScanFrame::Object {
        key: None,
        state: JsonObjectState::KeyOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        raw.as_bytes(),
        &mut i,
        raw.len(),
        &mut frames,
        &mut root_done
    ));
    assert!(matches!(
        frames[0],
        JsonScanFrame::Object {
            key: Some(_),
            state: JsonObjectState::Colon
        }
    ));

    let mut frames = vec![JsonScanFrame::Object {
        key: Some("a".to_string()),
        state: JsonObjectState::Colon,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b":",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(matches!(
        frames[0],
        JsonScanFrame::Object {
            key: Some(_),
            state: JsonObjectState::Value
        }
    ));

    let mut frames = vec![JsonScanFrame::Object {
        key: Some("a".to_string()),
        state: JsonObjectState::CommaOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b",",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(matches!(
        frames[0],
        JsonScanFrame::Object {
            key: None,
            state: JsonObjectState::KeyOrEnd
        }
    ));

    let mut frames = vec![JsonScanFrame::Object {
        key: Some("a".to_string()),
        state: JsonObjectState::CommaOrEnd,
    }];
    let mut i = 0usize;
    let mut root_done = false;
    assert!(advance_json_scan(
        b"}",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_done = false;
    assert!(scan_json_value(
        b"true",
        &mut i,
        4,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_done = false;
    assert!(scan_json_value(
        b"false",
        &mut i,
        5,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_done = false;
    assert!(scan_json_value(
        b"null",
        &mut i,
        4,
        &mut frames,
        &mut root_done
    ));
    assert!(root_done);

    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_done = false;
    assert!(!scan_json_value(
        b"\"",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));

    let mut i = 0usize;
    let mut frames = Vec::new();
    let mut root_done = false;
    assert!(!scan_json_value(
        b"x",
        &mut i,
        1,
        &mut frames,
        &mut root_done
    ));

    let mut frames = vec![JsonScanFrame::Object {
        key: Some("a".to_string()),
        state: JsonObjectState::Value,
    }];
    let mut root_done = false;
    complete_json_value(&mut frames, &mut root_done);
    assert!(matches!(
        frames[0],
        JsonScanFrame::Object {
            state: JsonObjectState::CommaOrEnd,
            ..
        }
    ));

    let mut frames = Vec::new();
    let mut root_done = false;
    complete_json_value(&mut frames, &mut root_done);
    assert!(root_done);
}

#[test]
fn json_error_message_and_suffix_stripping_contract() {
    let control_err =
        serde_json::from_str::<serde_json::Value>("\"a\u{0001}\"").expect_err("must fail");
    let msg = json_parse_error_message(&control_err);
    assert!(msg.contains("Invalid string: control characters"));

    let string_eof = serde_json::from_str::<serde_json::Value>("\"abc").expect_err("must fail");
    let msg = json_parse_error_message(&string_eof);
    assert!(msg.contains("Unfinished string at EOF"));

    let expected_value = serde_json::from_str::<serde_json::Value>("x").expect_err("must fail");
    let msg = json_parse_error_message(&expected_value);
    assert!(msg.contains("expected value"));

    assert_eq!(
        strip_serde_line_col_suffix("expected value at line 1 column 2"),
        "expected value"
    );
    assert_eq!(
        strip_serde_line_col_suffix("expected value at line x column 2"),
        "expected value at line x column 2"
    );
}

#[test]
fn raw_output0_renders_nul_delimited_outputs() {
    let (out, err) = render_raw_output0(
        &[
            serde_json::json!("a"),
            serde_json::json!(1),
            serde_json::json!({"b": 2}),
        ],
        true,
    )
    .expect("raw output0");
    assert!(err.is_none());
    assert_eq!(
        out,
        vec![b'a', 0, b'1', 0, b'{', b'"', b'b', b'"', b':', b'2', b'}', 0]
    );

    let native_values = vec![
        zq::NativeValue::from_json(serde_json::json!("a")),
        zq::NativeValue::from_json(serde_json::json!(1)),
        zq::NativeValue::from_json(serde_json::json!({"b": 2})),
    ];
    let (native_out, native_err) =
        render_raw_output0_native(&native_values, true).expect("native raw output0");
    assert_eq!(native_out, out);
    assert_eq!(native_err.is_some(), err.is_some());
}

#[test]
fn raw_output0_rejects_strings_with_nul() {
    let (out, err) = render_raw_output0(
        &[serde_json::json!("a"), serde_json::json!("a\u{0000}b")],
        false,
    )
    .expect("render");
    assert_eq!(out, vec![b'a', 0]);
    let err = err.expect("must fail");
    assert!(format!("{err}").contains("Cannot dump a string containing NUL"));

    let native_values = vec![
        zq::NativeValue::from_json(serde_json::json!("a")),
        zq::NativeValue::from_json(serde_json::json!("a\u{0000}b")),
    ];
    let (native_out, native_err) =
        render_raw_output0_native(&native_values, false).expect("native render");
    assert_eq!(native_out, out);
    let native_err = native_err.expect("native must fail");
    assert!(format!("{native_err}").contains("Cannot dump a string containing NUL"));
}

#[test]
fn render_json_line_supports_raw_and_compact_modes() {
    let raw = render_json_line(&serde_json::json!("abc"), true, true).expect("raw");
    assert_eq!(raw, "abc");

    let compact = render_json_line(&serde_json::json!({"a":1}), true, false).expect("compact");
    assert_eq!(compact, "{\"a\":1}");
}

#[test]
fn render_json_output_join_output_omits_newlines() {
    let out = render_json_output(
        &[
            serde_json::json!("hello"),
            serde_json::json!(1),
            serde_json::json!({"a": true}),
        ],
        true,
        true,
        true,
    )
    .expect("join output");
    assert_eq!(out, "hello1{\"a\":true}");
}

#[test]
fn render_json_output_default_mode_keeps_line_breaks() {
    let out = render_json_output(
        &[serde_json::json!("a"), serde_json::json!("b")],
        true,
        true,
        false,
    )
    .expect("line output");
    assert_eq!(out, "a\nb\n");
}

#[test]
fn render_json_line_escapes_del_like_jq() {
    let line = render_json_line(&serde_json::json!("~\u{007f}"), true, false).expect("render");
    assert_eq!(line, "\"~\\u007f\"");
}

#[test]
fn run_tests_error_normalization_strips_location_and_number_payload() {
    let got = "jq: error: Cannot index object with number (1) at <top-level>, line 1, column 7:";
    let expected = "jq: error: Cannot index object with number";
    assert_eq!(
        normalize_run_tests_error_line(got),
        normalize_run_tests_error_line(expected)
    );
}

#[test]
fn run_tests_error_normalization_strips_line_only_location_suffix() {
    let got = "jq: error: Module metadata must be constant at <top-level>, line 1:";
    let expected = "jq: error: Module metadata must be constant";
    assert_eq!(
        normalize_run_tests_error_line(got),
        normalize_run_tests_error_line(expected)
    );
}

#[test]
fn run_tests_values_equal_normalizes_string_error_variants() {
    let expected = serde_json::json!("Cannot index number with string \"a\"").to_string();
    let actual = serde_json::json!("Cannot index number with string (\"a\")").to_string();
    assert!(run_tests_values_equal(&expected, &actual));
}

#[test]
fn run_tests_values_equal_normalizes_nested_error_strings() {
    let expected = serde_json::json!(["ko", "Cannot index object with number"]).to_string();
    let actual = serde_json::json!(["ko", "Cannot index object with number (1)"]).to_string();
    assert!(run_tests_values_equal(&expected, &actual));
}

#[test]
fn run_tests_values_equal_accepts_equivalent_number_lexemes() {
    assert!(run_tests_values_equal("20e-1", "2.0"));
    assert!(run_tests_values_equal("[20e-1, 100e-2]", "[2.0, 1.0]"));
    assert!(run_tests_values_equal(
        "9.999999999e+999999999",
        "9999999999e+999999990"
    ));
}

#[test]
fn run_tests_values_equal_accepts_zero_length_unnamed_capture_variants() {
    let expected = r#"[{"offset":0,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]}]"#;
    let actual = r#"[{"offset":0,"length":0,"string":"","captures":[{"offset":0,"string":"","length":0,"name":null}]}]"#;
    assert!(run_tests_values_equal(expected, actual));
}

#[test]
fn run_tests_error_normalization_base64_message_variants() {
    let got = "string (\"Not base64 data\") is not valid base64 data";
    let expected = "string (\"Not base64...\") is not valid base64 data";
    assert_eq!(
        normalize_run_tests_error_line(got),
        normalize_run_tests_error_line(expected)
    );
}

#[test]
fn run_tests_error_normalization_added_object_payload_variants() {
    let got = "string (\"1,2,\") and object ({\"a\":{\"b\":{\"c\":33}}}) cannot be added";
    let expected = "string (\"1,2,\") and object ({\"a\":{\"b\":{...) cannot be added";
    assert_eq!(
        normalize_run_tests_error_line(got),
        normalize_run_tests_error_line(expected)
    );
}

#[test]
fn run_with_rejects_yaml_raw_output0_combination() {
    let cli = parse_cli_for_test(&["--output-format", "yaml", "--raw-output0", "."]);
    let err = run_with(cli, CliCompatArgs::default()).expect_err("must fail");
    assert!(format!("{err}").contains("--raw-output0 is supported only"));
}

#[test]
fn run_with_rejects_yaml_anchors_for_non_yaml_output() {
    let cli = parse_cli_for_test(&["--output-format", "json", "--yaml-anchors", "."]);
    let err = run_with(cli, CliCompatArgs::default()).expect_err("must fail");
    assert!(format!("{err}").contains("--yaml-anchors is supported only"));
}

#[test]
fn run_with_rejects_yaml_anchor_name_mode_without_yaml_output() {
    let cli = parse_cli_for_test(&[
        "--output-format",
        "json",
        "--yaml-anchor-name-mode",
        "strict-friendly",
        ".",
    ]);
    let err = run_with(cli, CliCompatArgs::default()).expect_err("must fail");
    assert!(format!("{err}").contains("--yaml-anchor-name-mode is supported only"));
}

#[test]
fn run_with_rejects_yaml_anchor_name_mode_without_yaml_anchors() {
    let cli = parse_cli_for_test(&[
        "--output-format",
        "yaml",
        "--yaml-anchor-name-mode",
        "strict-friendly",
        ".",
    ]);
    let err = run_with(cli, CliCompatArgs::default()).expect_err("must fail");
    assert!(format!("{err}").contains("--yaml-anchor-name-mode requires --yaml-anchors"));
}

#[test]
fn run_with_accepts_debug_dump_disasm_compat_flag() {
    let cli = parse_cli_for_test(&["-n", "--debug-dump-disasm", "1+1"]);
    let status = run_with(cli, CliCompatArgs::default()).expect("must run");
    assert_eq!(status, 0);
}

#[test]
fn resolve_diff_paths_contract() {
    let cli = parse_cli_for_test(&["--diff", "left.yaml", "right.json"]);
    assert_eq!(
        resolve_diff_paths(&cli).expect("two-file diff"),
        ("left.yaml".to_string(), "right.json".to_string())
    );

    let cli = parse_cli_for_test(&["--diff", "right.yaml"]);
    assert_eq!(
        resolve_diff_paths(&cli).expect("stdin vs file diff"),
        ("-".to_string(), "right.yaml".to_string())
    );

    let cli = parse_cli_for_test(&["--diff"]);
    let err = resolve_diff_paths(&cli).expect_err("missing paths");
    assert!(format!("{err}").contains("expects LEFT RIGHT"));

    let cli = parse_cli_for_test(&["--diff", "-", "-"]);
    let err = resolve_diff_paths(&cli).expect_err("double stdin must fail");
    assert!(format!("{err}").contains("both sides from stdin"));
}

#[test]
fn semantic_diff_collector_reports_changed_added_removed_paths() {
    let left_docs = vec![zq::NativeValue::from_json(serde_json::json!({
        "a": 1,
        "b": [1, 2],
        "drop": true,
        "keep": {"x": 1}
    }))];
    let right_docs = vec![zq::NativeValue::from_json(serde_json::json!({
        "a": 2,
        "add": "x",
        "b": [1, 3, 4],
        "keep": {"x": 1}
    }))];

    let diffs = collect_semantic_doc_diffs(&left_docs, &right_docs);
    assert_eq!(diffs.len(), 5);
    assert_eq!(
        diffs[0],
        SemanticDiff {
            kind: SemanticDiffKind::Changed,
            path: "$.a".to_string(),
            left: Some(zq::NativeValue::from_json(serde_json::json!(1))),
            right: Some(zq::NativeValue::from_json(serde_json::json!(2))),
        }
    );
    assert_eq!(
        diffs[1],
        SemanticDiff {
            kind: SemanticDiffKind::Added,
            path: "$.add".to_string(),
            left: None,
            right: Some(zq::NativeValue::from_json(serde_json::json!("x"))),
        }
    );
    assert_eq!(
        diffs[2],
        SemanticDiff {
            kind: SemanticDiffKind::Changed,
            path: "$.b[1]".to_string(),
            left: Some(zq::NativeValue::from_json(serde_json::json!(2))),
            right: Some(zq::NativeValue::from_json(serde_json::json!(3))),
        }
    );
    assert_eq!(
        diffs[3],
        SemanticDiff {
            kind: SemanticDiffKind::Added,
            path: "$.b[2]".to_string(),
            left: None,
            right: Some(zq::NativeValue::from_json(serde_json::json!(4))),
        }
    );
    assert_eq!(
        diffs[4],
        SemanticDiff {
            kind: SemanticDiffKind::Removed,
            path: "$.drop".to_string(),
            left: Some(zq::NativeValue::from_json(serde_json::json!(true))),
            right: None,
        }
    );
}

#[test]
fn semantic_diff_summary_counts_by_kind() {
    let diffs = vec![
        SemanticDiff {
            kind: SemanticDiffKind::Changed,
            path: "$.a".to_string(),
            left: Some(zq::NativeValue::from(1)),
            right: Some(zq::NativeValue::from(2)),
        },
        SemanticDiff {
            kind: SemanticDiffKind::Added,
            path: "$.b".to_string(),
            left: None,
            right: Some(zq::NativeValue::from(1)),
        },
        SemanticDiff {
            kind: SemanticDiffKind::Removed,
            path: "$.c".to_string(),
            left: Some(zq::NativeValue::from(1)),
            right: None,
        },
    ];
    let summary = SemanticDiffSummary::from_diffs(&diffs);
    assert_eq!(summary.total, 3);
    assert_eq!(summary.changed, 1);
    assert_eq!(summary.added, 1);
    assert_eq!(summary.removed, 1);
    assert!(!summary.equal());
}

#[test]
fn semantic_diff_report_jsonl_emits_only_summary_for_equal_inputs() {
    let mut out = Vec::new();
    let summary = SemanticDiffSummary::from_diffs(&[]);
    write_semantic_diff_report(
        &mut out,
        &[],
        summary,
        DiffOutputFormat::Jsonl,
        false,
        false,
    )
    .expect("jsonl report");
    let text = String::from_utf8(out).expect("utf8 report");
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "report:\n{text}");
    let payload: serde_json::Value = serde_json::from_str(lines[0]).expect("valid summary json");
    assert_eq!(
        payload["type"],
        serde_json::Value::String("summary".to_string())
    );
    assert_eq!(payload["equal"], serde_json::Value::Bool(true));
    assert_eq!(payload["total"], serde_json::Value::from(0u64));
}

#[test]
fn semantic_diff_report_diff_uses_color_codes_when_enabled() {
    let diffs = vec![SemanticDiff {
        kind: SemanticDiffKind::Changed,
        path: "$.a".to_string(),
        left: Some(zq::NativeValue::from(1)),
        right: Some(zq::NativeValue::from(2)),
    }];
    let summary = SemanticDiffSummary::from_diffs(&diffs);
    let mut out = Vec::new();
    write_semantic_diff_report(
        &mut out,
        &diffs,
        summary,
        DiffOutputFormat::Diff,
        false,
        true,
    )
    .expect("diff report");
    let text = String::from_utf8(out).expect("utf8 report");
    assert!(text.contains("\u{1b}[33m~\u{1b}[0m"), "report:\n{text}");
    assert!(text.contains("\u{1b}[36m$.a\u{1b}[0m"), "report:\n{text}");
}

#[test]
fn run_with_diff_mode_returns_expected_statuses() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let left = td.path().join("left.yaml");
    let right_equal = td.path().join("right-equal.json");
    let right_diff = td.path().join("right-diff.json");
    std::fs::write(&left, "a: 1\nb:\n  - 2\n").expect("write left");
    std::fs::write(&right_equal, "{\"b\":[2],\"a\":1}\n").expect("write right equal");
    std::fs::write(&right_diff, "{\"a\":2,\"b\":[2]}\n").expect("write right diff");

    let equal_cli = parse_cli_for_test(&[
        "--diff",
        left.to_str().expect("utf8 path"),
        right_equal.to_str().expect("utf8 path"),
    ]);
    let equal_status = run_with(equal_cli, CliCompatArgs::default()).expect("equal diff");
    assert_eq!(equal_status, 0);

    let diff_cli = parse_cli_for_test(&[
        "--diff",
        left.to_str().expect("utf8 path"),
        right_diff.to_str().expect("utf8 path"),
    ]);
    let diff_status = run_with(diff_cli, CliCompatArgs::default()).expect("different diff");
    assert_eq!(diff_status, 1);
}

#[test]
fn resolve_query_input_and_library_paths_contract() {
    let cli = parse_cli_for_test(&["."]);
    assert_eq!(resolve_base_query(&cli).expect("base query"), ".");
    assert_eq!(resolve_positional_input(&cli).expect("positional"), None);
    assert_eq!(resolve_input_path(&cli, None).expect("stdin path"), "-");

    let cli = parse_cli_for_test(&[".foo", "in.json"]);
    let positional = resolve_positional_input(&cli).expect("positional with file");
    assert_eq!(positional, Some("in.json".to_string()));
    assert_eq!(
        resolve_input_path(&cli, positional.as_deref()).expect("input file path"),
        "in.json"
    );

    let cli = parse_cli_for_test(&[".foo", "--input", "legacy.json"]);
    assert_eq!(
        resolve_input_path(&cli, None).expect("legacy input"),
        "legacy.json"
    );

    let mut cli = parse_cli_for_test(&[".foo", "file.json"]);
    cli.input_legacy = Some("legacy.json".to_string());
    let err = resolve_input_path(&cli, None).expect_err("duplicate input path");
    assert!(format!("{err}").contains("input path is specified twice"));

    let td = tempfile::TempDir::new().expect("tempdir");
    let query_file = td.path().join("q.jq");
    std::fs::write(&query_file, ".a").expect("write query file");
    let cli = parse_cli_for_test(&[
        "-f",
        query_file.to_str().expect("utf8 query file"),
        "input.json",
    ]);
    assert_eq!(
        resolve_base_query(&cli).expect("query from file"),
        ".a".to_string()
    );
    assert_eq!(
        resolve_positional_input(&cli).expect("file positional with -f"),
        Some("input.json".to_string())
    );

    let cli = parse_cli_for_test(&[
        "-f",
        query_file.to_str().expect("utf8 query file"),
        "query.jq",
        "input.json",
    ]);
    let err = resolve_positional_input(&cli).expect_err("too many positional args");
    assert!(format!("{err}").contains("too many positional arguments"));

    let test_dir = td.path().join("suite");
    std::fs::create_dir_all(test_dir.join("modules")).expect("create modules dir");
    let discovered = resolve_run_tests_library_paths(
        &parse_cli_for_test(&["--run-tests", "x.test"]),
        test_dir
            .join("cases.test")
            .to_str()
            .expect("utf8 test path"),
    );
    assert_eq!(discovered.len(), 1);
    assert!(discovered[0].ends_with("/modules"));
}

#[test]
fn requires_filter_for_interactive_stdin_contract() {
    let cli = parse_cli_for_test(&[]);
    assert!(requires_filter_for_interactive_stdin(&cli, true));
    assert!(!requires_filter_for_interactive_stdin(&cli, false));

    let with_query = parse_cli_for_test(&["."]);
    assert!(!requires_filter_for_interactive_stdin(&with_query, true));

    let null_input = parse_cli_for_test(&["-n"]);
    assert!(!requires_filter_for_interactive_stdin(&null_input, true));

    let with_input_file = parse_cli_for_test(&[".", "input.json"]);
    assert!(!requires_filter_for_interactive_stdin(
        &with_input_file,
        true
    ));

    let with_input_legacy = parse_cli_for_test(&[".", "--input", "input.json"]);
    assert!(!requires_filter_for_interactive_stdin(
        &with_input_legacy,
        true
    ));
}

#[test]
fn custom_input_and_json_color_options_contract() {
    let raw_cli = parse_cli_for_test(&["-R", "."]);
    let raw = build_custom_input_stream(&raw_cli, "a\nb\n", zq::DocMode::First)
        .expect("raw input stream");
    assert_eq!(raw, vec![serde_json::json!("a"), serde_json::json!("b")]);

    let raw_slurp_cli = parse_cli_for_test(&["-Rs", "."]);
    let raw_slurp = build_custom_input_stream(&raw_slurp_cli, "a\nb\n", zq::DocMode::First)
        .expect("raw slurp stream");
    assert_eq!(raw_slurp, vec![serde_json::json!("a\nb\n")]);

    let _guard = env_lock();
    let prev = std::env::var_os("JQ_COLORS");
    std::env::set_var("JQ_COLORS", "invalid");
    let color_cli = parse_cli_for_test(&["-C", "."]);
    let opts = resolve_json_color_options(&color_cli);
    if let Some(v) = prev {
        std::env::set_var("JQ_COLORS", v);
    } else {
        std::env::remove_var("JQ_COLORS");
    }
    assert!(opts.enabled);
    assert!(opts.warn_invalid);
    assert!(opts.jq_colors.is_none());

    let mono_cli = parse_cli_for_test(&["-C", "-M", "."]);
    let opts = resolve_json_color_options(&mono_cli);
    assert!(!opts.enabled);
}

#[test]
fn custom_input_format_contract() {
    let toml_cli = parse_cli_for_test(&["--input-format", "toml", "."]);
    let toml =
        build_custom_input_stream(&toml_cli, "a = 1\n", zq::DocMode::First).expect("toml parse");
    assert_eq!(toml, vec![serde_json::json!({"a": 1})]);

    let csv_cli = parse_cli_for_test(&["--input-format", "csv", "."]);
    let csv =
        build_custom_input_stream(&csv_cli, "k,v\nx,1\n", zq::DocMode::First).expect("csv parse");
    assert_eq!(csv, vec![serde_json::json!({"k": "x", "v": "1"})]);

    let csv_json_cells_cli =
        parse_cli_for_test(&["--input-format", "csv", "--csv-parse-json-cells", "."]);
    let csv_json_cells = build_custom_input_stream(
        &csv_json_cells_cli,
        "cases\n\"[{\"\"id\"\":\"\"jq_identity\"\"}]\"\n",
        zq::DocMode::First,
    )
    .expect("csv json-cells parse");
    assert_eq!(
        csv_json_cells,
        vec![serde_json::json!({"cases": [{"id": "jq_identity"}]})]
    );

    let xml_cli = parse_cli_for_test(&["--input-format", "xml", ".catalog.book.title"]);
    let xml = build_custom_input_stream(
        &xml_cli,
        "<catalog><book><title>Rust</title></book></catalog>",
        zq::DocMode::First,
    )
    .expect("xml parse");
    assert_eq!(
        xml,
        vec![serde_json::json!({"catalog":{"book":{"title":"Rust"}}})]
    );
}

#[test]
fn input_format_resolution_contract() {
    assert_eq!(
        resolve_effective_input_format(InputFormat::Auto, "a.yaml"),
        zq::NativeInputFormat::Yaml
    );
    assert_eq!(
        resolve_effective_input_format(InputFormat::Auto, "a.toml"),
        zq::NativeInputFormat::Toml
    );
    assert_eq!(
        resolve_effective_input_format(InputFormat::Auto, "a.csv"),
        zq::NativeInputFormat::Csv
    );
    assert_eq!(
        resolve_effective_input_format(InputFormat::Auto, "a.xml"),
        zq::NativeInputFormat::Xml
    );
    assert_eq!(
        resolve_effective_input_format(InputFormat::Auto, "-"),
        zq::NativeInputFormat::Auto
    );
    assert_eq!(
        resolve_effective_input_format(InputFormat::Json, "a.csv"),
        zq::NativeInputFormat::Json
    );
}

#[test]
fn extra_output_formats_contract() {
    let toml = render_toml_output_native(&[zq::NativeValue::from_json(serde_json::json!({
        "svc": "api",
        "port": 8080
    }))])
    .expect("toml render");
    assert!(toml.contains("svc = \"api\""));
    assert!(toml.contains("port = 8080"));

    let csv = render_csv_output_native(&[
        zq::NativeValue::from_json(serde_json::json!({"a": "x", "b": "1"})),
        zq::NativeValue::from_json(serde_json::json!({"a": "y", "b": "2"})),
    ])
    .expect("csv render");
    assert_eq!(csv, "a,b\nx,1\ny,2\n");

    let ragged_arrays = render_csv_output_native(&[
        zq::NativeValue::from_json(serde_json::json!([1, 2])),
        zq::NativeValue::from_json(serde_json::json!([3])),
    ])
    .expect("csv render ragged");
    assert_eq!(ragged_arrays, "1,2\n3,\n");

    let xml = render_xml_output_native(&[zq::NativeValue::from_json(serde_json::json!({
        "catalog": {"book": {"title": "Rust", "price": 10}}
    }))])
    .expect("xml render");
    assert_eq!(
        xml,
        "<catalog><book><title>Rust</title><price>10</price></book></catalog>"
    );
}

#[test]
fn structured_color_output_contract() {
    let yaml_colored =
        colorize_structured_output(OutputFormat::Yaml, "name: \"svc\" # note\n", true, None);
    assert!(
        yaml_colored.contains("\u{1b}[1;34mname"),
        "yaml:\n{yaml_colored}"
    );
    assert!(
        yaml_colored.contains("\u{1b}[0;32m\"svc\""),
        "yaml:\n{yaml_colored}"
    );
    assert!(
        yaml_colored.contains("\u{1b}[0;90m# note"),
        "yaml:\n{yaml_colored}"
    );

    let toml_colored =
        colorize_structured_output(OutputFormat::Toml, "[svc.api]\nport = 8080\n", true, None);
    assert!(
        toml_colored.contains("\u{1b}[1;34msvc.api"),
        "toml:\n{toml_colored}"
    );

    let csv_colored = colorize_structured_output(OutputFormat::Csv, "a,b\nx,2\n", true, None);
    assert_eq!(csv_colored, "a,b\nx,2\n");

    let xml_colored =
        colorize_structured_output(OutputFormat::Xml, "<root><a>1</a></root>\n", true, None);
    assert_eq!(xml_colored, "<root><a>1</a></root>\n");
}

#[test]
fn json_color_defaults_and_pretty_output_contract() {
    let default_cli = parse_cli_for_test(&["."]);
    let default_opts = resolve_json_color_options(&default_cli);
    assert!(!default_opts.warn_invalid);

    let palette = "0;90:0;39:0;39:0;39:0;32:1;39:1;39:1;31";
    let _guard = env_lock();
    let prev = std::env::var_os("JQ_COLORS");
    std::env::set_var("JQ_COLORS", palette);
    let color_cli = parse_cli_for_test(&["-C", "."]);
    let opts = resolve_json_color_options(&color_cli);
    if let Some(v) = prev {
        std::env::set_var("JQ_COLORS", v);
    } else {
        std::env::remove_var("JQ_COLORS");
    }
    assert_eq!(opts.jq_colors.as_deref(), Some(palette));

    let pretty = render_json_output(&[serde_json::json!({"a":1})], false, false, false)
        .expect("pretty output");
    assert_eq!(pretty, "{\n  \"a\": 1\n}\n");

    let mut buf = Vec::new();
    write_json_output(
        &mut buf,
        &[serde_json::json!(1), serde_json::json!(2)],
        true,
        false,
        false,
        &JsonColorOptions::default(),
    )
    .expect("write json output");
    assert_eq!(String::from_utf8(buf).expect("utf8"), "1\n2\n");
}

#[test]
fn native_json_output_escapes_del_like_jq() {
    let mut buf = Vec::new();
    write_json_output_native(
        &mut buf,
        &[zq::NativeValue::String("\u{007f}".to_string())],
        true,
        false,
        false,
        &JsonColorOptions::default(),
    )
    .expect("write native output");
    assert_eq!(String::from_utf8(buf).expect("utf8"), "\"\\u007f\"\n");
}

#[test]
fn native_json_output_normalizes_non_finite_numbers_like_jq() {
    let nan = zq::NativeValue::Number(serde_json::Number::from_string_unchecked("nan".to_string()));
    let inf = zq::NativeValue::Number(serde_json::Number::from_string_unchecked("inf".to_string()));
    let ninf = zq::NativeValue::Number(serde_json::Number::from_string_unchecked(
        "-inf".to_string(),
    ));
    let values = vec![
        nan.clone(),
        inf.clone(),
        ninf.clone(),
        zq::NativeValue::Array(vec![nan, inf, ninf]),
    ];

    let mut buf = Vec::new();
    write_json_output_native(
        &mut buf,
        &values,
        true,
        false,
        false,
        &JsonColorOptions::default(),
    )
    .expect("write native output");
    assert_eq!(
            String::from_utf8(buf).expect("utf8"),
            "null\n1.7976931348623157e+308\n-1.7976931348623157e+308\n[null,1.7976931348623157e+308,-1.7976931348623157e+308]\n"
        );

    let (raw0, err) = render_raw_output0_native(&values, true).expect("raw output0");
    assert!(err.is_none());
    assert_eq!(
            raw0,
            b"null\x001.7976931348623157e+308\0-1.7976931348623157e+308\0[null,1.7976931348623157e+308,-1.7976931348623157e+308]\0"
        );
}

#[test]
fn colored_rendering_and_engine_error_contract() {
    let compact = render_json_value_colored(
        &serde_json::json!([{"a": true, "b": false}, 123, null]),
        true,
        None,
        2,
        false,
    )
    .expect("compact colored render");
    let compact_text = String::from_utf8(compact).expect("utf8 compact");
    assert!(compact_text.contains("\x1b["));
    assert!(compact_text.contains("\"a\""));
    assert!(compact_text.contains("123"));
    let native_compact = render_native_value_colored(
        &zq::NativeValue::from_json(serde_json::json!([{"a": true, "b": false}, 123, null])),
        true,
        None,
        2,
    )
    .expect("native compact render");
    assert_eq!(native_compact, compact_text.into_bytes());

    let pretty = render_json_value_colored(&serde_json::json!({"k":[1]}), false, None, 2, false)
        .expect("pretty colored render");
    let native = zq::NativeValue::from_json(serde_json::json!({"k":[1]}));
    let native_pretty =
        render_native_value_colored(&native, false, None, 2).expect("native pretty render");
    assert_eq!(native_pretty, pretty);
    let pretty_text = String::from_utf8(pretty).expect("utf8 pretty");
    assert!(pretty_text.contains("\n"));
    assert!(pretty_text.contains("\"k\""));

    let msg = render_engine_error(
        "jq",
        "",
        "",
        zq::EngineError::Query(zq::QueryError::Runtime("boom".to_string())),
    );
    assert_eq!(msg, "jq: error (at <stdin>:1): boom");

    let trimmed = render_validation_error_without_engine_prefix(&zq::EngineError::Query(
        zq::QueryError::Unsupported("not supported".to_string()),
    ));
    assert_eq!(trimmed, "not supported");
}

#[test]
fn validate_jq_colors_accepts_valid_palette() {
    assert!(validate_jq_colors(
        "0;90:0;39:0;39:0;39:0;32:1;39:1;39:1;31"
    ));
    assert!(validate_jq_colors("4;31"));
    assert!(validate_jq_colors(":"));
    assert!(validate_jq_colors("::::::::"));
    assert!(validate_jq_colors(
            "38;2;160;196;255:38;2;220;220;170:38;2;205;168;105:38;2;255;173;173:38;2;160;196;255:38;2;150;205;251:38;2;255;214;165:38;2;138;43;226"
        ));
}

#[test]
fn validate_jq_colors_rejects_invalid_palette() {
    assert!(!validate_jq_colors(
        "garbage;30:*;31:,;3^:0;$%:0;34:1;35:1;36"
    ));
    assert!(!validate_jq_colors(
        "1234567890123456789;30:0;31:0;32:0;33:0;34:1;35:1;36"
    ));
    assert!(!validate_jq_colors(
        "1234567890123456;1234567890123456:0;39:0;39:0;39:0;32:1;39:1;39"
    ));
    assert!(!validate_jq_colors(
            "0123456789123:0123456789123:0123456789123:0123456789123:0123456789123:0123456789123:0123456789123:0123456789123:"
        ));
}

#[test]
fn spool_cleanup_removes_stale_run_dirs_and_keeps_locked_ones() {
    let td = tempfile::TempDir::new().expect("tempdir");
    let root = td.path().join("spool").join("v1");
    fs::create_dir_all(&root).expect("create spool root");

    let stale_dir = root.join("run-stale");
    fs::create_dir(&stale_dir).expect("create stale run dir");
    fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(stale_dir.join("run.lock"))
        .expect("create stale lock");

    let locked_dir = root.join("run-live");
    fs::create_dir(&locked_dir).expect("create live run dir");
    let live_lock = fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(locked_dir.join("run.lock"))
        .expect("create live lock");
    live_lock
        .try_lock_exclusive()
        .expect("lock live run as active");

    SpoolManager::sweep_stale_runs(&root).expect("sweep stale runs");
    assert!(
        !stale_dir.exists(),
        "stale run dir should be removed by startup cleanup"
    );
    assert!(
        locked_dir.exists(),
        "locked run dir must not be removed while process is active"
    );
    live_lock.unlock().expect("unlock live run");
}

#[test]
fn spool_manager_drop_cleans_its_run_dir() {
    let _guard = env_lock();
    let td = tempfile::TempDir::new().expect("tempdir");
    let custom_root = td.path().join("custom-spool-root");

    let prev = std::env::var_os("ZQ_SPOOL_DIR");
    std::env::set_var("ZQ_SPOOL_DIR", &custom_root);

    let run_dir = {
        let manager = SpoolManager::new().expect("create spool manager");
        assert!(manager.run_dir.exists(), "run dir must exist while active");
        assert!(
            manager.run_dir.join("run.lock").exists(),
            "run lock must exist"
        );
        manager.run_dir.clone()
    };

    if let Some(v) = prev {
        std::env::set_var("ZQ_SPOOL_DIR", v);
    } else {
        std::env::remove_var("ZQ_SPOOL_DIR");
    }

    assert!(
        !run_dir.exists(),
        "run dir should be removed when manager is dropped"
    );
}

#[test]
fn run_tests_mode_many_aggregates_statuses_for_multiple_files() {
    let _guard = env_lock();
    let td = tempfile::TempDir::new().expect("tempdir");
    let spool_root = td.path().join("spool-root");

    let prev = std::env::var_os("ZQ_SPOOL_DIR");
    std::env::set_var("ZQ_SPOOL_DIR", &spool_root);

    let spool = SpoolManager::new().expect("spool manager");
    let pass = td.path().join("pass.test");
    let fail = td.path().join("fail.test");
    std::fs::write(&pass, ".\n1\n1\n\n").expect("write pass suite");
    std::fs::write(&fail, ".\n1\n2\n\n").expect("write fail suite");

    let cli = parse_cli_for_test(&[
        "--run-tests",
        pass.to_str().expect("utf8 path"),
        "--monochrome-output",
    ]);
    let status = run_tests_mode_many(
        &cli,
        &[
            pass.to_string_lossy().to_string(),
            fail.to_string_lossy().to_string(),
        ],
        &spool,
    )
    .expect("run-tests mode");
    assert_eq!(status, 1, "failed suite should dominate final status");

    if let Some(v) = prev {
        std::env::set_var("ZQ_SPOOL_DIR", v);
    } else {
        std::env::remove_var("ZQ_SPOOL_DIR");
    }
}

#[test]
fn run_tests_mode_many_single_file_can_return_skip_overflow_status() {
    let _guard = env_lock();
    let td = tempfile::TempDir::new().expect("tempdir");
    let spool_root = td.path().join("spool-root");

    let prev = std::env::var_os("ZQ_SPOOL_DIR");
    std::env::set_var("ZQ_SPOOL_DIR", &spool_root);

    let spool = SpoolManager::new().expect("spool manager");
    let pass = td.path().join("pass.test");
    std::fs::write(&pass, ".\n1\n1\n\n").expect("write pass suite");

    let cli = parse_cli_for_test(&[
        "--run-tests",
        pass.to_str().expect("utf8 path"),
        "--skip",
        "10",
    ]);
    let status = run_tests_mode_many(&cli, &[pass.to_string_lossy().to_string()], &spool)
        .expect("run-tests mode");
    assert_eq!(status, 2, "skip past EOF must return status 2");

    if let Some(v) = prev {
        std::env::set_var("ZQ_SPOOL_DIR", v);
    } else {
        std::env::remove_var("ZQ_SPOOL_DIR");
    }
}

#[test]
fn run_tests_mode_rejects_incompatible_filter_and_file_flags() {
    let _guard = env_lock();
    let td = tempfile::TempDir::new().expect("tempdir");
    let spool_root = td.path().join("spool-root");
    let prev = std::env::var_os("ZQ_SPOOL_DIR");
    std::env::set_var("ZQ_SPOOL_DIR", &spool_root);

    let spool = SpoolManager::new().expect("spool manager");
    let mut cli = parse_cli_for_test(&["--run-tests", "suite.test"]);
    cli.query = Some(".".to_string());
    let err = run_tests_mode(&cli, "suite.test", &spool).expect_err("must reject query");
    assert!(format!("{err}").contains("cannot be combined"));

    if let Some(v) = prev {
        std::env::set_var("ZQ_SPOOL_DIR", v);
    } else {
        std::env::remove_var("ZQ_SPOOL_DIR");
    }
}

#[test]
fn run_tests_helpers_cover_parsing_and_formatting_edges() {
    assert!(is_skipline("  # comment"));
    assert!(is_skipline(" \t "));
    assert!(!is_skipline("x"));
    assert!(is_fail_marker("%%FAIL"));
    assert!(is_fail_marker("%%FAIL IGNORE MSG"));
    assert!(!is_fail_marker("%%FAIL OOPS"));
    assert!(is_fail_with_message("%%FAIL"));
    assert!(!is_fail_with_message("%%FAIL IGNORE MSG"));
    assert!(is_blank("  \t"));
    assert!(!is_blank("no"));

    let trimmed = strip_bom_prefix("\u{feff}abc");
    assert_eq!(trimmed, "abc");
    assert_eq!(strip_bom_prefix("abc"), "abc");

    assert_eq!(
        format_duration(std::time::Duration::from_millis(15)),
        "15ms"
    );
    assert_eq!(
        format_duration(std::time::Duration::from_millis(1200)),
        "1.200s"
    );

    let long = "x".repeat(500);
    let short = shorten_for_report(&long);
    assert!(short.contains("[300 chars omitted]"));
    assert!(short.starts_with(&"x".repeat(120)));
}

#[test]
fn run_tests_number_lexeme_canonicalization_rejects_invalid_forms() {
    assert_eq!(
        canonicalize_run_tests_number_lexeme("20e-1"),
        Some((false, "20".to_string(), -1))
    );
    assert_eq!(
        canonicalize_run_tests_number_lexeme("-001.2300"),
        Some((true, "12300".to_string(), -4))
    );
    assert_eq!(
        canonicalize_run_tests_number_lexeme("0.000"),
        Some((false, "0".to_string(), 0))
    );

    assert_eq!(canonicalize_run_tests_number_lexeme(""), None);
    assert_eq!(canonicalize_run_tests_number_lexeme("-"), None);
    assert_eq!(canonicalize_run_tests_number_lexeme("1e"), None);
    assert_eq!(canonicalize_run_tests_number_lexeme("1e+"), None);
    assert_eq!(canonicalize_run_tests_number_lexeme("1.2.3"), None);
    assert_eq!(canonicalize_run_tests_number_lexeme("abc"), None);
}
