use super::*;

fn program_with_ops(ops: Vec<Op>) -> Program {
    Program {
        branches: vec![crate::native_engine::vm_core::ir::Branch { ops }],
        functions: Vec::new(),
        module_search_dirs: Vec::new(),
    }
}

fn execute_single(op: Op, input: ZqValue) -> Result<Vec<ZqValue>, String> {
    execute(&program_with_ops(vec![op]), &input)
}

fn object(fields: &[(&str, ZqValue)]) -> ZqValue {
    let mut out = IndexMap::with_capacity(fields.len());
    for (key, value) in fields {
        out.insert((*key).to_string(), value.clone());
    }
    ZqValue::Object(out)
}

fn as_f64(value: &ZqValue) -> f64 {
    match value {
        ZqValue::Number(n) => n.as_f64().expect("finite number"),
        other => panic!("expected number, got {other:?}"),
    }
}

#[test]
fn execute_softens_terminal_stage_error_when_values_were_emitted() {
    let op = Op::Comma(vec![
        Op::Literal(ZqValue::from(1)),
        Op::Error(Box::new(Op::Literal(ZqValue::String("boom".to_string())))),
    ]);
    let out = execute_single(op, ZqValue::Null).expect("terminal stage must be softened");
    assert_eq!(out, vec![ZqValue::from(1)]);
}

#[test]
fn execute_keeps_non_terminal_stage_errors_hard() {
    let op = Op::Comma(vec![
        Op::Literal(ZqValue::from(1)),
        Op::Error(Box::new(Op::Literal(ZqValue::String("boom".to_string())))),
    ]);
    let err = execute(&program_with_ops(vec![op, Op::Identity]), &ZqValue::Null)
        .expect_err("non-terminal stage error must remain hard");
    assert!(err.contains("boom"), "unexpected error text: {err}");
}

#[test]
fn execute_terminal_repeat_softens_break_after_emitting_values() {
    let program = program_with_ops(vec![Op::Repeat(Box::new(Op::Input))]);
    let _input_guard =
        install_input_stream(&[ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]);
    set_input_cursor(1);

    let out = execute(&program, &ZqValue::from(1)).expect("terminal repeat must soften break");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(3)]);
}

#[test]
fn execute_terminal_repeat_keeps_break_hard_without_outputs() {
    let program = program_with_ops(vec![Op::Repeat(Box::new(Op::Input))]);
    let _input_guard = install_input_stream(&[ZqValue::from(1)]);
    set_input_cursor(1);

    let err = execute(&program, &ZqValue::from(1))
        .expect_err("terminal repeat without outputs must return hard error");
    assert!(
        err.contains("break"),
        "unexpected terminal repeat hard error: {err}"
    );
}

#[test]
fn execute_terminal_trycatch_repeat_input_matches_inputs_contract() {
    let op = Op::TryCatch {
        inner: Box::new(Op::Repeat(Box::new(Op::Input))),
        catcher: Box::new(Op::IfElse {
            cond: Box::new(Op::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(Op::Identity),
                rhs: Box::new(Op::Literal(ZqValue::String("break".to_string()))),
            }),
            then_expr: Box::new(Op::Empty),
            else_expr: Box::new(Op::Error(Box::new(Op::Identity))),
        }),
    };
    let program = program_with_ops(vec![op]);
    let _input_guard =
        install_input_stream(&[ZqValue::from(1), ZqValue::from(2), ZqValue::from(3)]);
    set_input_cursor(1);

    let out = execute(&program, &ZqValue::from(1)).expect("inputs contract must hold");
    assert_eq!(out, vec![ZqValue::from(2), ZqValue::from(3)]);
}

#[test]
fn execute_terminal_trycatch_reraises_non_break_errors() {
    let op = Op::TryCatch {
        inner: Box::new(Op::Error(Box::new(Op::Literal(ZqValue::String(
            "boom".to_string(),
        ))))),
        catcher: Box::new(Op::IfElse {
            cond: Box::new(Op::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(Op::Identity),
                rhs: Box::new(Op::Literal(ZqValue::String("break".to_string()))),
            }),
            then_expr: Box::new(Op::Empty),
            else_expr: Box::new(Op::Error(Box::new(Op::Identity))),
        }),
    };

    let err = execute(&program_with_ops(vec![op]), &ZqValue::Null)
        .expect_err("non-break errors must be re-raised");
    assert!(
        err.contains("boom"),
        "unexpected try/catch re-raise text: {err}"
    );
}

#[test]
fn binary_cartesian_order_is_rhs_major() {
    let op = Op::Binary {
        op: BinaryOp::Add,
        lhs: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(1)),
            Op::Literal(ZqValue::from(2)),
        ])),
        rhs: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(10)),
            Op::Literal(ZqValue::from(20)),
        ])),
    };
    let out = execute_single(op, ZqValue::Null).expect("binary execute");
    assert_eq!(
        out,
        vec![
            ZqValue::from(11),
            ZqValue::from(12),
            ZqValue::from(21),
            ZqValue::from(22)
        ]
    );
}

#[test]
fn object_literal_single_output_fast_path_matches_shape() {
    let op = Op::ObjectLiteral(vec![
        (
            OpObjectKey::Static("id".to_string()),
            Op::GetField {
                name: "id".to_string(),
                optional: false,
            },
        ),
        (
            OpObjectKey::Static("group".to_string()),
            Op::GetField {
                name: "group".to_string(),
                optional: false,
            },
        ),
        (
            OpObjectKey::Static("value".to_string()),
            Op::GetField {
                name: "value".to_string(),
                optional: false,
            },
        ),
    ]);

    let input = object(&[
        ("id", ZqValue::from(7)),
        ("group", ZqValue::from(2)),
        ("value", ZqValue::from(42)),
    ]);
    let out = execute_single(op, input).expect("object literal execute");
    assert_eq!(
        out,
        vec![object(&[
            ("id", ZqValue::from(7)),
            ("group", ZqValue::from(2)),
            ("value", ZqValue::from(42))
        ])]
    );
}

#[test]
fn object_literal_cartesian_shape_is_preserved() {
    let op = Op::ObjectLiteral(vec![
        (
            OpObjectKey::Static("a".to_string()),
            Op::Comma(vec![
                Op::Literal(ZqValue::from(1)),
                Op::Literal(ZqValue::from(2)),
            ]),
        ),
        (
            OpObjectKey::Static("b".to_string()),
            Op::Comma(vec![
                Op::Literal(ZqValue::from(3)),
                Op::Literal(ZqValue::from(4)),
            ]),
        ),
    ]);

    let out = execute_single(op, ZqValue::Null).expect("object cartesian execute");
    assert_eq!(
        out,
        vec![
            object(&[("a", ZqValue::from(1)), ("b", ZqValue::from(3))]),
            object(&[("a", ZqValue::from(1)), ("b", ZqValue::from(4))]),
            object(&[("a", ZqValue::from(2)), ("b", ZqValue::from(3))]),
            object(&[("a", ZqValue::from(2)), ("b", ZqValue::from(4))]),
        ]
    );
}

#[test]
fn run_dynamic_index_keeps_key_order() {
    let indexed = ZqValue::Array(vec![ZqValue::from(10), ZqValue::from(20)]);
    let key_op = Op::Comma(vec![
        Op::Literal(ZqValue::from(0)),
        Op::Literal(ZqValue::from(1)),
    ]);
    let mut out = Vec::new();
    run_dynamic_index(indexed, &key_op, &ZqValue::Null, false, &mut out).expect("dynamic index");
    assert_eq!(out, vec![ZqValue::from(10), ZqValue::from(20)]);
}

#[test]
fn select_gt_pipe_keeps_only_matching_inputs() {
    let select = Op::Select(Box::new(Op::Binary {
        op: BinaryOp::Gt,
        lhs: Box::new(Op::GetField {
            name: "id".to_string(),
            optional: false,
        }),
        rhs: Box::new(Op::Literal(ZqValue::from(2))),
    }));
    let extract_id = Op::GetField {
        name: "id".to_string(),
        optional: false,
    };
    let program = Program {
        branches: vec![crate::native_engine::vm_core::ir::Branch {
            ops: vec![select, extract_id],
        }],
        functions: Vec::new(),
        module_search_dirs: Vec::new(),
    };

    let low = object(&[("id", ZqValue::from(2)), ("payload", ZqValue::from(1))]);
    let high = object(&[("id", ZqValue::from(3)), ("payload", ZqValue::from(1))]);

    assert_eq!(
        execute(&program, &low).expect("execute low"),
        Vec::<ZqValue>::new()
    );
    assert_eq!(
        execute(&program, &high).expect("execute high"),
        vec![ZqValue::from(3)]
    );
}

#[test]
fn math_binary_cartesian_order_is_rhs_major() {
    let op = Op::MathBinary {
        op: MathBinaryOp::Hypot,
        lhs: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(3)),
            Op::Literal(ZqValue::from(6)),
        ])),
        rhs: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(4)),
            Op::Literal(ZqValue::from(8)),
        ])),
    };
    let out = execute_single(op, ZqValue::Null).expect("math binary execute");
    let got = out.iter().map(as_f64).collect::<Vec<_>>();
    let want = [5.0, 7.211102550927978, 8.54400374531753, 10.0];
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(want.iter()) {
        assert!((g - w).abs() < 1e-12, "got={g}, want={w}");
    }
}

#[test]
fn math_ternary_cartesian_order_has_last_arg_outermost() {
    let op = Op::MathTernary {
        op: MathTernaryOp::Fma,
        a: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(1)),
            Op::Literal(ZqValue::from(2)),
        ])),
        b: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(10)),
            Op::Literal(ZqValue::from(20)),
        ])),
        c: Box::new(Op::Comma(vec![
            Op::Literal(ZqValue::from(100)),
            Op::Literal(ZqValue::from(200)),
        ])),
    };
    let out = execute_single(op, ZqValue::Null).expect("math ternary execute");
    let got = out.iter().map(as_f64).collect::<Vec<_>>();
    let want = vec![110.0, 120.0, 120.0, 140.0, 210.0, 220.0, 220.0, 240.0];
    assert_eq!(got, want);
}

#[test]
fn regex_has_match_respects_no_empty_flag() {
    let cfg_no_empty = RegexModeConfig {
        global: true,
        no_empty: true,
        case_insensitive: false,
        multi_line: false,
        dot_matches_new_line: false,
        ignore_whitespace: false,
    };
    let cfg_with_empty = RegexModeConfig {
        no_empty: false,
        ..cfg_no_empty.clone()
    };

    assert!(!regex_has_match("abc", "", &cfg_no_empty).expect("regex no-empty"));
    assert!(regex_has_match("abc", "", &cfg_with_empty).expect("regex with-empty"));
}

#[test]
fn regex_cache_reuses_compiled_pattern() {
    REGEX_CACHE.with(|cache| cache.borrow_mut().clear());
    let cfg = RegexModeConfig {
        global: false,
        no_empty: false,
        case_insensitive: false,
        multi_line: false,
        dot_matches_new_line: false,
        ignore_whitespace: false,
    };

    assert!(regex_has_match("alpha", "a", &cfg).expect("first regex run"));
    assert!(regex_has_match("beta", "a", &cfg).expect("second regex run"));
    let cache_len = REGEX_CACHE.with(|cache| cache.borrow().len());
    assert_eq!(cache_len, 1, "compiled regex should be reused");
}

#[test]
fn normalize_named_capture_syntax_uses_borrowed_when_possible() {
    let unchanged = normalize_named_capture_syntax("a(b)c");
    assert!(matches!(unchanged, Cow::Borrowed(_)));

    let normalized = normalize_named_capture_syntax("(?<name>a)");
    assert_eq!(normalized.as_ref(), "(?P<name>a)");
}
