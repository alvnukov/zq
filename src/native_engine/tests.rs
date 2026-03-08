use super::*;

#[allow(dead_code)]
struct RegexFixtureCase {
    query: &'static str,
    input: &'static str,
    outputs: &'static [&'static str],
}

static ONIG_FIXTURE_CASES_ALL: &[RegexFixtureCase] = include!("../fixtures_onig_all.inc");
static MANONIG_FIXTURE_CASES_ALL: &[RegexFixtureCase] = include!("../fixtures_manonig_all.inc");

#[test]
fn parses_and_runs_simple_paths() {
    let input = vec![serde_json::json!({"a":[{"b":1},{"b":2}]})];
    let out = try_execute(".a[1].b", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn all_upstream_onig_fixture_queries_compile_in_vm_core() {
    for case in ONIG_FIXTURE_CASES_ALL.iter().chain(MANONIG_FIXTURE_CASES_ALL) {
        assert!(
            is_supported(case.query),
            "query from upstream onig fixtures must compile: {}",
            case.query
        );
    }
}

#[test]
fn runs_pipeline_with_literal() {
    let input = vec![serde_json::json!({"a": 7})];
    let out = try_execute(".a | 10", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(10)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn uses_null_input_mode() {
    let input = vec![serde_json::json!({"a": 7})];
    let out = try_execute(".", &input, RunOptions { null_input: true });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn as_binding_and_var_lookup_follow_jq_forms() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "1 as $x | 2 as $y | [$x,$y,$x]",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 2, 1])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[1,2,3][] as $x | [[4,5,6,7][$x]]",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!([5]),
                    serde_json::json!([6]),
                    serde_json::json!([7]),
                ]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "42 as $x | . | . | . + 432 | $x + 1",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(43)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("1 + 2 as $x | -$x", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(-3)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn destructure_object_patterns_follow_jq_forms() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]",
        &[serde_json::json!({"a":1,"b":[2,{"d":3}]})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 2, 3])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]",
        &[serde_json::json!({"a":1,"b":[2,{"d":3}]})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([1, [2, {"d": 3}], 2, {"d": 3}])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#,
        &[serde_json::json!({"as":1,"str":2,"exp":3})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 2, 3])]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn bound_variable_supports_dot_postfix_access() {
    let out = try_execute(
        ". as $o | $o.n",
        &[serde_json::json!({"n": 7})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn destructure_alternatives_follow_jq_forms() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
        &[serde_json::json!([{"a":1, "b":[2,{"d":3}]}, [4, {"b":5, "c":6}, 7, 8, 9], "foo"])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([1, null, 2, 3, null, null]),
                serde_json::json!([4, 5, null, null, 7, null]),
                serde_json::json!([null, null, null, null, null, "foo"]),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
        &[serde_json::json!([[3], [4], [5], 6])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([3]),
                serde_json::json!([4]),
                serde_json::json!([5]),
                serde_json::json!(6),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn generator_and_reduce_builtins_follow_jq_contracts() {
    let run = RunOptions { null_input: false };

    // jq: def map(f): [.[] | f];
    let out = try_execute("map(.+1)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([2, 3, 4])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("map(.)", &[serde_json::json!({"a":1,"b":2})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 2])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def select(f): if f then . else empty end;
    let out = try_execute("select(.ok)", &[serde_json::json!({"ok":true})], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"ok":true})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("select(.ok)", &[serde_json::json!({"ok":false})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def first(g): label $out | g | ., break $out;
    let out = try_execute("first(.[])", &[serde_json::json!([10, 20, 30])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(10)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("first(empty)", &[serde_json::json!([10, 20, 30])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq 1.7.1: def last(g): reduce g as $item (null; $item);
    let out = try_execute("last(.[])", &[serde_json::json!([10, 20, 30])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(30)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("last(empty)", &[serde_json::json!([10, 20, 30])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def isempty(g): first((g|false), true);
    let out = try_execute("isempty(.[])", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("isempty(.[])", &[serde_json::json!([1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def add: reduce .[] as $x (null; . + $x);
    let out = try_execute("add", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(6)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("add", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("add", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Err(err)) => assert_eq!(err, "Cannot iterate over null (null)"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn limit_skip_nth_follow_jq_generator_defs() {
    let run = RunOptions { null_input: false };

    let out = try_execute("[limit(5,7; range(9))]", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[skip(5,7; range(9))]", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([5, 6, 7, 8, 7, 8])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[nth(5,7; range(9;0;-1))]", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([4, 2])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try limit(-1; range(3)) catch .",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!("limit doesn't support negative count")]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try nth(-1; range(3)) catch .",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("nth doesn't support negative indices")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn nth_value_alias_matches_jq_def() {
    let run = RunOptions { null_input: false };

    // jq: def nth($n): .[$n];
    let out = try_execute("nth(0,2,-1)", &[serde_json::json!([10, 20, 30])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(10),
                    serde_json::json!(30),
                    serde_json::json!(30)
                ]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("nth(\"a\")", &[serde_json::json!({"a": 1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("nth(\"missing\")", &[serde_json::json!({"a": 1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try nth(\"a\") catch .", &[serde_json::json!([1, 2])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot index array with string \"a\"")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("nth(1.9)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try nth(1.9) catch .", &[serde_json::json!({"a": 1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot index object with number")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn while_until_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def while(cond; update): if cond then ., (update | _while) else empty end;
    let out = try_execute("[while(. < 100; .*2)]", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 2, 4, 8, 16, 32, 64])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[while(. < 0; .+1)]", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def until(cond; next): if cond then . else (next|_until) end;
    let out = try_execute("until(. >= 100; .*2)", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(128)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("until(. >= 0; .*2)", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn any_all_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    let out = try_execute("any", &[serde_json::json!([false, null, 1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all", &[serde_json::json!([true, 1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all", &[serde_json::json!([true, false])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("any", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("any(. > 2)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all(. > 0)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all(. > 2)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("any(.[]; . > 2)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("all(.[]; . > 0)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try any catch .", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot iterate over number (1)")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn in_uppercase_follows_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def IN(s): any(s == .; .);
    let out = try_execute("IN(1,2,3)", &[serde_json::json!(2)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("IN(1,2,3)", &[serde_json::json!(5)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def IN(src; s): any(src == s; .);
    let out = try_execute("IN(1,2,3; 2)", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("IN(1,2,3; 7)", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn in_uppercase_accepts_literal_postfix_stream() {
    let run = RunOptions { null_input: false };

    let out = try_execute("IN([1,2,3][])", &[serde_json::json!(2)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("IN([1,2,3][]; 7)", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported("IN([1,2,3][])"));
    assert!(is_supported("IN([1,2,3][]; 2)"));
}

#[test]
fn index_uppercase_follows_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def INDEX(idx_expr): INDEX(.[]; idx_expr);
    let out = try_execute(
        "INDEX(.id)",
        &[serde_json::json!([
            {"id": 1, "v": "a"},
            {"id": 2, "v": "b"},
            {"id": 1, "v": "c"}
        ])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!({
                "1": {"id": 1, "v": "c"},
                "2": {"id": 2, "v": "b"}
            })]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def INDEX(stream; idx_expr): reduce stream as $row ({}; .[$row|idx_expr|tostring] = $row);
    let out = try_execute(
        "INDEX(.items[]; .id, .alt)",
        &[serde_json::json!({
            "items": [
                {"id": "x", "alt": 1, "v": 10},
                {"id": "y", "alt": 2, "v": 20}
            ]
        })],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!({
                "x": {"id": "x", "alt": 1, "v": 10},
                "1": {"id": "x", "alt": 1, "v": 10},
                "y": {"id": "y", "alt": 2, "v": 20},
                "2": {"id": "y", "alt": 2, "v": 20}
            })]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try INDEX(.id) catch .", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot iterate over number (1)")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn walk_add_error_with_msg_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def walk(f): ...
    let out = try_execute("walk(.)", &[serde_json::json!({"x":0})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!({"x":0})]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("walk(1)", &[serde_json::json!({"x":0})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[walk(.,1)]", &[serde_json::json!({"x":0})], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([{"x":0}, 1])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def add(f): reduce f as $x (null; . + $x);
    let out = try_execute("add(.[])", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(6)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("add(.a, .b)", &[serde_json::json!({"a": 1, "b": 2})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(3)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def error(msg): msg|error;
    let out = try_execute(
        "try error(\"boom\") catch .",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("boom")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("error(empty)", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn setpath_error_cases_follow_jq_regressions() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "try setpath([1]; 1) catch .",
        &[serde_json::json!({"hi":"hello"})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!("Cannot index object with number (1)")]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try setpath([[1]]; 1) catch .",
        &[serde_json::json!([])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "Cannot update field at array index of array"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("setpath([\"a\"]; 1)", &[serde_json::json!({})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!({"a":1})]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported("setpath([0]; 1)"));
}

#[test]
fn setpath_negative_index_and_no_leak_follow_jq() {
    let run = RunOptions { null_input: false };

    // jq/tests/jq.test: setpath([-1]; 1) on [0] updates the last element.
    let out = try_execute("setpath([-1]; 1)", &[serde_json::json!([0])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/jq.test: negative index on null is out-of-bounds.
    let out = try_execute(
        "try setpath([-1]; 1) catch .",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Out of bounds negative array index")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/jq.test: negative index below array bounds is an error.
    let out = try_execute(
        "try setpath([-2]; 1) catch .",
        &[serde_json::json!([0])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Out of bounds negative array index")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/jq.test #2970: invalid setpath does not leak the original input.
    let out = try_execute(
        "try [\"ok\", setpath([1]; 1)] catch .",
        &[serde_json::json!({"hi":"hello"})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot index object with number (1)")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try [\"ok\", setpath([1]; 1)] catch [\"ko\", .]",
        &[serde_json::json!({"hi":"hello"})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                "ko",
                "Cannot index object with number (1)"
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn input_debug_and_builtins_follow_jq_cases() {
    let run = RunOptions { null_input: false };

    let out = try_execute("try input catch .", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("break")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("debug", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("builtins|length > 10", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "\"-1\"|IN(builtins[] / \"/\"|.[1])",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "all(builtins[] / \"/\"; .[1]|tonumber >= 0)",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "builtins|any(.[:1] == \"_\")",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn dynamic_bracket_index_filter_follows_jq() {
    let run = RunOptions { null_input: false };

    // jq/tests/jq.test: try 0[implode] catch .
    let out = try_execute("try 0[implode] catch .", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot index number with string \"\"")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // Optional dynamic indexing suppresses runtime indexing errors.
    let out = try_execute("0[implode]?", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // Dynamic index expression is evaluated from the current input, not from the target literal.
    let out = try_execute(
        "{\"a\":1}[keys_unsorted[]]",
        &[serde_json::json!({"a":0})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported("try 0[implode] catch ."));
}

#[test]
fn time_builtins_follow_jq_cases() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "try strftime(\"%Y-%m-%dT%H:%M:%SZ\") catch .",
        &[serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "strftime/1 requires parsed datetime inputs"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try strflocaltime(\"%Y-%m-%dT%H:%M:%SZ\") catch .",
        &[serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "strflocaltime/1 requires parsed datetime inputs"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try mktime catch .",
        &[serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("mktime requires parsed datetime inputs")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try [\"OK\", strftime([])] catch [\"KO\", .]",
        &[serde_json::json!(0)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                "KO",
                "strftime/1 requires a string format"
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try [\"OK\", strflocaltime({})] catch [\"KO\", .]",
        &[serde_json::json!(0)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                "KO",
                "strflocaltime/1 requires a string format"
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[strptime(\"%Y-%m-%dT%H:%M:%SZ\")|(.,mktime)]",
        &[serde_json::json!("2015-03-05T23:51:47Z")],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                [2015, 2, 5, 23, 51, 47, 4, 63],
                1425599507
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "last(range(2)|(\"1970-03-01T01:02:03Z\"|strptime(\"%Y-%m-%dT%H:%M:%SZ\")|mktime) + (86400 * .))",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(5187723)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported("try strftime(\"%Y-%m-%dT%H:%M:%SZ\") catch ."));
    assert!(is_supported(
        "try strflocaltime(\"%Y-%m-%dT%H:%M:%SZ\") catch ."
    ));
    assert!(is_supported("try mktime catch ."));
    assert!(is_supported("strptime(\"%Y-%m-%dT%H:%M:%SZ\")"));
    assert!(is_supported(
        "last(range(2)|(\"1970-03-01T01:02:03Z\"|strptime(\"%Y-%m-%dT%H:%M:%SZ\")|mktime) + (86400 * .))"
    ));
}

#[test]
fn if_expr_numeric_eq_and_isempty_follow_jq() {
    let run = RunOptions { null_input: false };

    let out = try_execute("isempty(1,error(\"foo\"))", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "map(. == 1)",
        &[serde_json::json!([1, 1.0, 1.000, 100e-2, 1e+0, 0.0001e4])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([true, true, true, true, true, true])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[0] | tostring | . == if have_decnum then \"13911860366432393\" else \"13911860366432392\" end",
        &[serde_json::json!([13911860366432393_i64])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "(13911860366432393 == 13911860366432392) | . == if have_decnum then false else true end",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "-. | tojson == if have_decnum then \"-13911860366432393\" else \"-13911860366432392\" end",
        &[serde_json::json!(13911860366432393_i64)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[1E+1000,-1E+1000 | tojson] == if have_decnum then [\"1E+1000\",\"-1E+1000\"] else [\"1.7976931348623157e+308\",\"-1.7976931348623157e+308\"] end",
        &[serde_json::json!(null)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported(
        ". == if have_decnum then \"13911860366432393\" else \"13911860366432392\" end"
    ));
    assert!(is_supported(
        "[1E+1000,-1E+1000 | tojson] == if have_decnum then [\"1E+1000\",\"-1E+1000\"] else [\"1.7976931348623157e+308\",\"-1.7976931348623157e+308\"] end"
    ));
}

#[test]
fn array_and_object_literal_comparisons_follow_jq() {
    let run = RunOptions { null_input: false };
    let input = [serde_json::json!({})];

    let out = try_execute(
        "[[1,2,3] == [1,2,3], [1,2,3] != [1,2,3], [1,2,3] == [4,5,6], [1,2,3] != [4,5,6]]",
        &input,
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([true, false, false, true])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[{\"foo\":42} == {\"foo\":42},{\"foo\":42} != {\"foo\":42}, {\"foo\":42} != {\"bar\":42}, {\"foo\":42} == {\"bar\":42}]",
        &input,
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([true, false, true, false])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[{\"foo\":[1,2,{\"bar\":18},\"world\"]} == {\"foo\":[1,2,{\"bar\":18},\"world\"]},{\"foo\":[1,2,{\"bar\":18},\"world\"]} == {\"foo\":[1,2,{\"bar\":19},\"world\"]}]",
        &input,
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([true, false])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(is_supported("[1,2,3] == [1,2,3]"));
    assert!(is_supported("{\"foo\":42} == {\"foo\":42}"));
}

#[test]
fn recurse_variants_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def recurse: recurse(.[]?);
    let out = try_execute(
        "[recurse | numbers]",
        &[serde_json::json!([1, [2, [3]], {"a": [4]}])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 2, 3, 4])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def recurse(f; cond): def r: ., (f | select(cond) | r); r;
    let out = try_execute(
        "[recurse(.next?; . != null) | .v]",
        &[serde_json::json!({
            "v": 0,
            "next": {"v": 1, "next": {"v": 2}}
        })],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([0, 1, 2])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[recurse(.children[]?) | .name?]",
        &[serde_json::json!({
            "name": "root",
            "children": [
                {"name": "a"},
                {"name": "b", "children": [{"name": "c"}]}
            ]
        })],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!(["root", "a", "b", "c"])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn paths_variants_follow_jq_defs() {
    let run = RunOptions { null_input: false };
    let input = serde_json::json!({
        "a": [10, {"b": 20}],
        "c": {"d": 30}
    });

    let out = try_execute("[paths]", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                ["a"],
                ["a", 0],
                ["a", 1],
                ["a", 1, "b"],
                ["c"],
                ["c", "d"]
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "[paths(type == \"number\")]",
        std::slice::from_ref(&input),
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([["a", 0], ["a", 1, "b"], ["c", "d"]])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn combinations_variants_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "[combinations]",
        &[serde_json::json!([[1, 2], [3, 4]])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!([[1, 3], [1, 4], [2, 3], [2, 4]])]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[combinations(2)]", &[serde_json::json!([1, 2])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!([[1, 1], [1, 2], [2, 1], [2, 2]])]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[combinations]", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([[]])]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn in_and_inside_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    // jq: def in(xs): . as $x | xs | has($x);
    let out = try_execute("in([1,2,3])", &[serde_json::json!(2)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("in({a:1})", &[serde_json::json!("b")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def inside(xs): . as $x | xs | contains($x);
    let out = try_execute("inside([1,2,3])", &[serde_json::json!([2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "inside({a:1,b:2})",
        &[serde_json::json!({"a":1,"c":3})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn entries_builtins_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    let out = try_execute("to_entries", &[serde_json::json!({"a":1,"b":2})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                {"key":"a","value":1},
                {"key":"b","value":2}
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("to_entries", &[serde_json::json!([10, 20])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                {"key":0,"value":10},
                {"key":1,"value":20}
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "from_entries",
        &[serde_json::json!([
            {"key":"a","value":1},
            {"Key":"b","Value":2}
        ])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"a":1,"b":2})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("from_entries", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!({})]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "with_entries({key:.key, value:(.value+1)})",
        &[serde_json::json!({"a":1,"b":2})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"a":2,"b":3})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn indices_index_rindex_follow_jq_cases() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "[(index(\",\"), rindex(\",\")), indices(\",\")]",
        &[serde_json::json!("a,bc,def,ghij,klmno")],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 13, [1, 4, 8, 13]])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "indices(1)",
        &[serde_json::json!([0, 1, 1, 2, 3, 4, 1, 5])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 2, 6])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "indices([1,2])",
        &[serde_json::json!([0, 1, 2, 3, 1, 4, 2, 5, 1, 2, 6, 7])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 8])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("index(\"!\")", &[serde_json::json!("здравствуй мир!")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(14)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("indices(\"o\")", &[serde_json::json!("🇬🇧oo")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([2, 3])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("index(\"\")", &[serde_json::json!("")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn flatten_follows_jq_definitions() {
    let run = RunOptions { null_input: false };
    let nested = serde_json::json!([0, [1], [[2]], [[[3]]]]);

    let out = try_execute("flatten", std::slice::from_ref(&nested), run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([0, 1, 2, 3])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("flatten(3,2,1)", std::slice::from_ref(&nested), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([0, 1, 2, 3]),
                serde_json::json!([0, 1, 2, [3]]),
                serde_json::json!([0, 1, [2], [[3]]]),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("flatten(-1)", std::slice::from_ref(&nested), run);
    match out {
        TryExecute::Executed(Err(err)) => assert_eq!(err, "flatten depth must not be negative"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn trim_family_follows_jq_behavior() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "map(trim), map(ltrim), map(rtrim)",
        &[serde_json::json!([
            " \n\t\r\u{000c}\u{000b}",
            "",
            "  ",
            "a",
            " a ",
            "abc",
            "  abc  ",
            "  abc",
            "abc  "
        ])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!(["", "", "", "a", "a", "abc", "abc", "abc", "abc"]),
                serde_json::json!(["", "", "", "a", "a ", "abc", "abc  ", "abc", "abc  "]),
                serde_json::json!(["", "", "", "a", " a", "abc", "  abc", "  abc", "abc"]),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let all_ws = "\u{0009}\u{000A}\u{000B}\u{000C}\u{000D}\u{0020}\u{0085}\u{00A0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}";
    let sample = format!("{all_ws}abc{all_ws}");
    let out = try_execute("trim, ltrim, rtrim", &[serde_json::json!(sample)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!("abc"),
                serde_json::json!(format!("abc{all_ws}")),
                serde_json::json!(format!("{all_ws}abc")),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "try trim catch ., try ltrim catch ., try rtrim catch .",
        &[serde_json::json!(123)],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!("trim input must be a string"),
                serde_json::json!("trim input must be a string"),
                serde_json::json!("trim input must be a string"),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn map_values_follows_jq_assignment_style() {
    let run = RunOptions { null_input: false };

    let out = try_execute("map_values(.+1)", &[serde_json::json!({"a":1,"b":2})], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"a":2,"b":3})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "map_values(select(. > 1))",
        &[serde_json::json!([1, 2, 3])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([2, 3])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("map_values(empty)", &[serde_json::json!({"a":1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!({})]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn split_matches_jq_examples_for_plain_separators() {
    let run = RunOptions { null_input: false };
    let input = serde_json::json!(["a, bc, def, ghij, jklmn, a,b, c,d, e,f", "a,b,c,d, e,f,g,h"]);

    let out = try_execute("[.[]|split(\",\")]", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                ["a", " bc", " def", " ghij", " jklmn", " a", "b", " c", "d", " e", "f"],
                ["a", "b", "c", "d", " e", "f", "g", "h"]
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[.[]|split(\", \")]", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                ["a", "bc", "def", "ghij", "jklmn", "a,b", "c,d", "e,f"],
                ["a,b,c,d", "e,f,g,h"]
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn sort_unique_min_max_reverse_follow_jq_cases() {
    let run = RunOptions { null_input: false };

    let out = try_execute("sort", &[serde_json::json!([8, 3, null, 6])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([null, 3, 6, 8])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "unique",
        &[serde_json::json!([1, 2, 5, 3, 5, 3, 1, 3])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 2, 3, 5])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("min", &[serde_json::json!([5, 4, 2, 7])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("max", &[serde_json::json!([5, 4, 2, 7])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("reverse", &[serde_json::json!([1, 2, 3, 4])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([4, 3, 2, 1])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn transpose_follows_jq_builtin_def() {
    let run = RunOptions { null_input: false };

    let out = try_execute("transpose", &[serde_json::json!([[1, 2], [3, 4]])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([[1, 3], [2, 4]])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("transpose", &[serde_json::json!([[1], [2, 3], []])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([[1, 2, null], [null, 3, null]])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "transpose",
        &[serde_json::json!({"a": [1, 2], "b": [3]})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([[1, 3], [2, null]])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try transpose catch .", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("Cannot iterate over number (1)")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn sort_group_unique_min_max_by_follow_jq_defs() {
    let run = RunOptions { null_input: false };
    let input = serde_json::json!([
        {"foo": 3, "bar": 1, "id": "a"},
        {"foo": 2, "bar": 9, "id": "b"},
        {"foo": 2, "bar": 5, "id": "c"},
        {"foo": 1, "bar": 7, "id": "d"}
    ]);

    let out = try_execute("sort_by(.foo)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([{"foo":1,"bar":7,"id":"d"},{"foo":2,"bar":9,"id":"b"},{"foo":2,"bar":5,"id":"c"},{"foo":3,"bar":1,"id":"a"}])
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("sort_by(.foo, .bar)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([{"foo":1,"bar":7,"id":"d"},{"foo":2,"bar":5,"id":"c"},{"foo":2,"bar":9,"id":"b"},{"foo":3,"bar":1,"id":"a"}])
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("group_by(.foo)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                [{"foo":1,"bar":7,"id":"d"}],
                [{"foo":2,"bar":9,"id":"b"},{"foo":2,"bar":5,"id":"c"}],
                [{"foo":3,"bar":1,"id":"a"}]
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("unique_by(.foo)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!([{"foo":1,"bar":7,"id":"d"},{"foo":2,"bar":9,"id":"b"},{"foo":3,"bar":1,"id":"a"}])
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("min_by(.foo)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"foo":1,"bar":7,"id":"d"})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("max_by(.foo)", std::slice::from_ref(&input), run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"foo":3,"bar":1,"id":"a"})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try sort_by(.foo) catch .", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "null (null) cannot be sorted, as it is not an array"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn bsearch_matches_jq_examples() {
    let run = RunOptions { null_input: false };

    let out = try_execute("bsearch(0)", &[serde_json::json!([0, 1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(0)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("bsearch(0)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(-1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("bsearch(0,1,2,3,4)", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(-1),
                    serde_json::json!(0),
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(-4),
                ]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "bsearch({x:1})",
        &[serde_json::json!([{"x":0},{"x":1},{"x":2}])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(1)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try bsearch(0) catch .", &[serde_json::json!("aa")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!("string (\"aa\") cannot be searched from")]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_iteration_and_comma_outputs() {
    let input = vec![serde_json::json!([1, 2, 3])];
    let out = try_execute(".[]", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(3)
                ]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "1, 2",
        &[RawJsonValue::Null],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn array_collect_constructor_follows_jq_stream_collection() {
    let run = RunOptions { null_input: false };

    let out = try_execute("[.]", &[serde_json::json!({"a":1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([{"a":1}])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[.[]]", &[serde_json::json!([1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([1, 2, 3])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[1,.]", &[serde_json::json!(7)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 7])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("[]", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([])]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn parenthesized_subprogram_stage_works_in_pipeline() {
    let run = RunOptions { null_input: false };

    let out = try_execute("(.)", &[serde_json::json!({"a": 1})], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"a":1})])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(". | (.a)", &[serde_json::json!({"a": 7})], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try (.[1:3]) catch .", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!("Cannot index number with object")]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn path_index_list_and_slice_accessors_follow_jq_shape() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        ".[-4,-3,-2,-1,0,1,2,3]",
        &[serde_json::json!([10, 20, 30])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!(null),
                serde_json::json!(10),
                serde_json::json!(20),
                serde_json::json!(30),
                serde_json::json!(10),
                serde_json::json!(20),
                serde_json::json!(30),
                serde_json::json!(null),
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[1:3]", &[serde_json::json!([0, 1, 2, 3])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([1, 2])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[1:3]", &[serde_json::json!("abcd")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("bc")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[1:3]?", &[serde_json::json!(1)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[0]", &[serde_json::json!("аб")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("а")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[-1]", &[serde_json::json!("аб")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("б")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[5]", &[serde_json::json!("ab")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".[0,2,-1]", &[serde_json::json!("abcd")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![
                serde_json::json!("a"),
                serde_json::json!("c"),
                serde_json::json!("d")
            ]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_optional_path_access() {
    let out = try_execute(
        ".a?",
        &[serde_json::json!(1)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[]?",
        &[serde_json::json!(1)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".missing?",
        &[serde_json::json!({"a": 1})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_select_test_regex_filter() {
    let input = vec![serde_json::json!([
        {"id": 1, "name": "user0999"},
        {"id": 2, "name": "user1000"},
        {"id": 3, "name": "user1999"},
        {"id": 4, "name": "user2000"}
    ])];
    let out = try_execute(
        ".[] | select(.name | test(\"user1[0-9]{3}$\")) | .id",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!(2), serde_json::json!(3)])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_select_compare_eq_filter() {
    let input = vec![
        serde_json::json!({"i": 1}),
        serde_json::json!({"i": 2}),
        serde_json::json!({"i": 3}),
    ];
    let out = try_execute("select(.i==2)", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"i": 2})]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_select_compare_ne_filter() {
    let input = vec![
        serde_json::json!({"i": 1}),
        serde_json::json!({"i": 2}),
        serde_json::json!({"i": 3}),
    ];
    let out = try_execute("select(.i!=2)", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!({"i": 1}), serde_json::json!({"i": 3})]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_select_compare_ordering_filter() {
    let input = vec![
        serde_json::json!({"i": 1}),
        serde_json::json!({"i": 2}),
        serde_json::json!({"i": 3}),
    ];
    let out = try_execute("select(.i>2)", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"i": 3})]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supports_path_mod_literal_length_and_gsub() {
    let input = vec![serde_json::json!({
        "value": 11,
        "tags": [1, 2, 3],
        "text": "alpha-beta"
    })];

    let out = try_execute(".value % 7", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(4)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".tags | length", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(3)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".text | gsub(\"[aeiou]\";\"\")",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!("lph-bt")])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn select_test_errors_on_non_string_like_jq() {
    let input = vec![serde_json::json!({"name": null})];
    let out = try_execute(
        "select(.name | test(\"x\"))",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "null (null) cannot be matched, as it is not a string")
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn unsupported_regex_syntax_falls_back() {
    let out = try_execute(
        "select(.name | test(\"(?=x)\"))",
        &[serde_json::json!({"name":"x"})],
        RunOptions { null_input: false },
    );
    assert!(matches!(out, TryExecute::Unsupported));
}

#[test]
fn native_runtime_errors_match_jq_wording() {
    let out = try_execute(
        ".[1]",
        &[serde_json::json!({})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "Cannot index object with number");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[]",
        &[serde_json::json!(1)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "Cannot iterate over number (1)");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn try_execute_stream_matches_collect_mode() {
    let input = vec![serde_json::json!([{"id":1},{"id":2},{"id":3}])];
    let collected = try_execute(".[] | .id", &input, RunOptions { null_input: false });
    let mut streamed = Vec::new();
    let streamed_out =
        try_execute_stream(".[] | .id", &input, RunOptions { null_input: false }, |v| {
            streamed.push(v);
            Ok(())
        });

    match (collected, streamed_out) {
        (TryExecute::Executed(Ok(a)), TryExecuteStream::Executed(Ok(()))) => {
            assert_eq!(a, streamed);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn parallel_execute_slice_preserves_input_order() {
    let input = (0..(PAR_EXEC_MIN_INPUTS + 37))
        .map(|i| serde_json::json!({ "id": i as i64 }))
        .collect::<Vec<_>>();
    let out = try_execute(".id", &input, RunOptions { null_input: false });
    let expected = (0..(PAR_EXEC_MIN_INPUTS + 37))
        .map(|i| serde_json::json!(i as i64))
        .collect::<Vec<_>>();
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, expected),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn parallel_execute_slice_keeps_first_error_in_input_order() {
    let mut input = vec![serde_json::json!([1, 2, 3])];
    input.extend((0..(PAR_EXEC_MIN_INPUTS + 16)).map(|i| serde_json::json!({ "a": i as i64 })));

    let out = try_execute(".a", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "Cannot index array with string \"a\"");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn try_execute_stream_propagates_sink_error() {
    let input = vec![serde_json::json!([1, 2, 3])];
    let out = try_execute_stream(".[]", &input, RunOptions { null_input: false }, |_v| {
        Err("sink failed".to_string())
    });
    match out {
        TryExecuteStream::Executed(Err(err)) => assert_eq!(err, "sink failed"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn select_reemits_input_for_each_match() {
    let row = serde_json::json!({"tags":[1,1,2], "name":"aaxx"});
    let input = vec![row.clone()];

    let out = try_execute(
        "select(.tags[]==1)",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row.clone(), row.clone()]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "select(.name | test(\"a\"))",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![row]),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn object_iteration_and_optional_accessors_follow_contract() {
    let out = try_execute(
        ".[]",
        &[serde_json::json!({"a": 1, "b": 2})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!(1), serde_json::json!(2)]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[]?",
        &[serde_json::json!(true)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[2]",
        &[serde_json::json!([0, 1])],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![JsonValue::Null]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".[1]?",
        &[serde_json::json!({"a": 1})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn parser_supports_quoted_paths_and_rejects_broken_forms() {
    let out = try_execute(
        ".\"a b\"",
        &[serde_json::json!({"a b": 7})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(7)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    assert!(matches!(
        try_execute(
            ".[",
            &[RawJsonValue::Null],
            RunOptions { null_input: false }
        ),
        TryExecute::Unsupported
    ));
    assert!(matches!(
        try_execute(
            "select(.a | test(\"x\")",
            &[RawJsonValue::Null],
            RunOptions { null_input: false }
        ),
        TryExecute::Unsupported
    ));
    assert!(matches!(
        try_execute(
            "select(.a=1)",
            &[serde_json::json!({"a": 1})],
            RunOptions { null_input: false }
        ),
        TryExecute::Unsupported
    ));
}

#[test]
fn comma_terms_in_intermediate_pipeline_clone_original_input() {
    let input = vec![serde_json::json!({"a": 1})];
    let out = try_execute("., .a | .", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!({"a": 1}), serde_json::json!(1)]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn length_semantics_follow_jq_builtin_c() {
    let input = vec![
        serde_json::json!(null),
        serde_json::json!([1, 2]),
        serde_json::json!({"a": 1}),
        serde_json::json!("abμ"),
        serde_json::json!(-3.5),
    ];
    let out = try_execute("length", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(0),
                    serde_json::json!(2),
                    serde_json::json!(1),
                    serde_json::json!(3),
                    serde_json::json!(3.5),
                ]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "length",
        &[serde_json::json!(true)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "boolean (true) has no length");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn numeric_selector_and_ascii_builtins_follow_jq_defs() {
    let run = RunOptions { null_input: false };

    let out = try_execute("isfinite", &[serde_json::json!(1.5)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(true)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("isfinite", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(false)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("normals", &[serde_json::json!(0)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("normals", &[serde_json::json!(3.5)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(3.5)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("finites", &[serde_json::json!(2)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(2)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("finites", &[serde_json::json!(null)], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("ascii_downcase", &[serde_json::json!("AbC-Я")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("abc-Я")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("ascii_upcase", &[serde_json::json!("AbC-Я")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("ABC-Я")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("ascii_downcase", &[serde_json::json!(7)], run);
    match out {
        TryExecute::Executed(Err(err)) => assert_eq!(err, "explode input must be a string"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn abs_implode_explode_and_trimstr_follow_jq_logic() {
    let run = RunOptions { null_input: false };

    // jq/src/builtin.jq: def abs: if . < 0 then - . else . end;
    let out = try_execute("abs", &[serde_json::json!("abc")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("abc")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("map(abs)", &[serde_json::json!([-0, 0, -10, -1.1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([0, 0, 10, 1.1])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("map(fabs)", &[serde_json::json!([-0, 0, -10, -1.1])], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([0, 0, 10, 1.1])])
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "map(abs == length) | unique",
        &[serde_json::json!([
            -10,
            -1.1,
            -1e-1,
            1000000000000000002i64
        ])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!([true])]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/src/builtin.jq:
    // def ltrimstr($left): if startswith($left) then .[$left | length:] end;
    // def rtrimstr($right): if endswith($right) then .[:length - ($right | length)] end;
    let out = try_execute("ltrimstr(\"x\")", &[serde_json::json!("abc")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("rtrimstr(\"x\")", &[serde_json::json!("abc")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("trimstr(\"x\")", &[serde_json::json!("xabc")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert!(values.is_empty()),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("ltrimstr(\"hi\")", &[serde_json::json!("hi")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "implode|explode",
        &[serde_json::json!([
            -1, 0, 1, 2, 3, 1114111, 1114112, 55295, 55296, 57343, 57344, 1.1, 1.9
        ])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!([
                65533, 0, 1, 2, 3, 1114111, 65533, 55295, 65533, 65533, 57344, 1, 1
            ])]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try implode catch .", &[serde_json::json!(123)], run);
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!("implode input must be an array")]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try implode catch .", &[serde_json::json!(["a"])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "string (\"a\") can't be imploded, unicode codepoint needs to be numeric"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn first_last_builtin_aliases_match_indexing_defs() {
    let run = RunOptions { null_input: false };

    // jq: def first: .[0];
    let out = try_execute("first", &[serde_json::json!([10, 20])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(10)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("first", &[serde_json::json!("xy")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("x")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("first", &[serde_json::json!([])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(null)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq: def last: .[-1];
    let out = try_execute("last", &[serde_json::json!([10, 20])], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(20)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("last", &[serde_json::json!("xy")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("y")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("last", &[serde_json::json!({})], run);
    match out {
        TryExecute::Executed(Err(err)) => assert_eq!(err, "Cannot index object with number"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn modulo_semantics_follow_jq_builtin_c() {
    let input = vec![
        serde_json::json!({"v": -5}),
        serde_json::json!({"v": 0}),
        serde_json::json!({"v": 7}),
    ];
    let out = try_execute(".v % 2", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(-1),
                    serde_json::json!(0),
                    serde_json::json!(1)
                ]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".v % -1", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(0),
                    serde_json::json!(0),
                    serde_json::json!(0)
                ]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(".v % 0", &input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(
                err,
                "cannot be divided (remainder) because the divisor is zero"
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ".v % 2",
        &[serde_json::json!({"v":"x"})],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(
                err,
                "string (\"x\") and number (2) cannot be divided (remainder)"
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn select_ordering_semantics_follow_jv_cmp() {
    let input = vec![
        serde_json::json!({"v": null}),
        serde_json::json!({"v": false}),
        serde_json::json!({"v": true}),
        serde_json::json!({"v": 0}),
        serde_json::json!({"v": "0"}),
        serde_json::json!({"v": []}),
        serde_json::json!({"v": {}}),
    ];
    let out = try_execute(
        "select(.v > false) | .v",
        &input,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![
                    serde_json::json!(true),
                    serde_json::json!(0),
                    serde_json::json!("0"),
                    serde_json::json!([]),
                    serde_json::json!({}),
                ]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let arrs = vec![
        serde_json::json!({"v":[1,2]}),
        serde_json::json!({"v":[1,2,0]}),
        serde_json::json!({"v":[1,3]}),
    ];
    let out = try_execute(
        "select(.v > [1,2]) | .v",
        &arrs,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!([1, 2, 0]), serde_json::json!([1, 3])]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let objs = vec![
        serde_json::json!({"v":{"a":1}}),
        serde_json::json!({"v":{"a":1,"b":0}}),
        serde_json::json!({"v":{"a":2}}),
    ];
    let out = try_execute(
        r#"select(.v > {"a":1}) | .v"#,
        &objs,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!({"a":1,"b":0}), serde_json::json!({"a":2})]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn gsub_basic_and_error_semantics_follow_jq() {
    let out = try_execute(
        r#"gsub("a";"x")"#,
        &[serde_json::json!("banana")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!("bxnxnx")]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        r#"gsub("a";"x")"#,
        &[serde_json::json!(1)],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "number (1) cannot be matched, as it is not a string");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn regex_zero_width_global_semantics_follow_jq_builtin_sources() {
    // jq/src/builtin.jq: gsub("[^a-z]*(?<x>[a-z]*)"; "Z\(.x)") => trailing empty match emits final "Z".
    let out = try_execute(
        r#"gsub("[^a-z]*(?<x>[a-z]*)"; "Z\(.x)")"#,
        &[serde_json::json!("123foo456bar")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!("ZfooZbarZ")]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/src/builtin.c advances by one byte for zero-width global matches to avoid infinite loops.
    let out = try_execute(
        r#"[match("(?=u)"; "g")]"#,
        &[serde_json::json!("qux")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values.len(), 1);
            let matches = values[0]
                .as_array()
                .expect("match output wrapped by [] must be an array");
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0]["offset"], serde_json::json!(1));
            assert_eq!(matches[0]["length"], serde_json::json!(0));
            assert_eq!(matches[0]["string"], serde_json::json!(""));
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/onig.test
    let out = try_execute(
        r#"gsub(""; "a"; "g")"#,
        &[serde_json::json!("a")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!("aaa")]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/onig.test: `n` drops empty matches.
    let out = try_execute(
        r#"[match("( )*"; "gn")]"#,
        &[serde_json::json!("abc")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([])]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn regex_queries_in_nested_jq_forms_follow_upstream_manonig_cases() {
    let run = RunOptions { null_input: false };

    // jq/tests/manonig.test
    let out = try_execute(
        r#"walk( if type == "object" then with_entries( .key |= sub( "^_+"; "") ) else . end )"#,
        &[serde_json::json!([{ "_a": { "__b": 2 } }])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!([{"a":{"b":2}}])]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/tests/onig.test
    let out = try_execute(
        r#"[sub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)", "c")]"#,
        &[serde_json::json!("aB")],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!(["AB", "aB", "cB"])]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn regex_single_quote_named_capture_syntax_matches_oniguruma_forms() {
    let out = try_execute(
        r#"capture("(?'a'[a-z]+)-(?'n'[0-9]+)")"#,
        &[serde_json::json!("xyzzy-14")],
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(values, vec![serde_json::json!({"a":"xyzzy","n":"14"})]);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn regex_flag_concatenation_errors_follow_jq_builtin_definitions() {
    let run = RunOptions { null_input: false };

    // jq/src/builtin.jq: scan($re; $flags): match($re; "g" + $flags)
    let out = try_execute(r#"scan("a"; 1)"#, &[serde_json::json!("a")], run);
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, r#"string ("g") and number (1) cannot be added"#);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/src/builtin.jq: splits($re; $flags): match($re; $flags + "g")
    let out = try_execute(r#"splits("a"; 1)"#, &[serde_json::json!("a")], run);
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, r#"number (1) and string ("g") cannot be added"#);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq/src/builtin.jq: gsub($re; s; flags): sub($re; s; flags + "g")
    let out = try_execute(r#"gsub("a"; "b"; 1)"#, &[serde_json::json!("a")], run);
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, r#"number (1) and string ("g") cannot be added"#);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn format_filters_and_try_catch_follow_jq_builtin_c() {
    let run = RunOptions { null_input: false };

    let payload = serde_json::json!("<>&'\"\tПривет");
    let out = try_execute("(@base64 | @base64d)", std::slice::from_ref(&payload), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![payload.clone()]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("(@uri | @urid)", std::slice::from_ref(&payload), run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![payload.clone()]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("@base64d", &[serde_json::json!("=")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // jq stops decoding at the first '=' and ignores trailing bytes.
    let out = try_execute("@base64d", &[serde_json::json!("QQ==tail")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!("A")]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("@base64d", &[serde_json::json!("QUJDa")], run);
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "string (\"QUJDa\") trailing base64 byte found")
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("@urid", &[serde_json::json!("%F0%93%81")], run);
    match out {
        TryExecute::Executed(Err(err)) => {
            assert_eq!(err, "string (\"%F0%93%81\") is not a valid uri encoding")
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute("try @base64d catch .", &[serde_json::json!("QUJDa")], run);
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "string (\"QUJDa\") trailing base64 byte found"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        ". | try @urid catch .",
        &[serde_json::json!("%F0%93%81")],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!(
                "string (\"%F0%93%81\") is not a valid uri encoding"
            )]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn bench_regression_queries_are_covered() {
    let single = vec![serde_json::json!({
        "id": 8,
        "active": true,
        "group": 3,
        "value": 77,
        "a": 8,
        "b": 4,
        "text": "alpha-beta",
        "tags": [1, 2, 3]
    })];
    let many = vec![
        serde_json::json!({"id": 1, "active": true}),
        serde_json::json!({"id": 3, "active": true}),
        serde_json::json!({"id": 8, "active": true}),
    ];

    let out = try_execute(".value % 7", &single, RunOptions { null_input: false });
    assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

    let out = try_execute(
        r#".text | gsub("[aeiou]";"")"#,
        &single,
        RunOptions { null_input: false },
    );
    assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

    let out = try_execute(".tags | length", &single, RunOptions { null_input: false });
    assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

    let out = try_execute("select(.id > 2)", &many, RunOptions { null_input: false });
    assert!(matches!(out, TryExecute::Executed(Ok(_))), "{out:?}");

    let out = try_execute(".a + .b", &single, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(values, vec![serde_json::json!(12)]),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        "{id,group,value}",
        &single,
        RunOptions { null_input: false },
    );
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!({"id":8,"group":3,"value":77})]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }

    // Exact complex filter from stdin benchmark that previously failed as unsupported.
    let complex = r#"select(.active and (.id % 7 == 0)) | {id,group,score:(.a*3 + .b - (.value/10)),txt:(.text|ascii_downcase),ok:(.tags|length>2)}"#;
    let complex_input = vec![serde_json::json!({
        "id": 14,
        "active": true,
        "group": 3,
        "value": 77,
        "a": 8,
        "b": 4,
        "text": "alpha-beta",
        "tags": [1, 2, 3]
    })];
    let out = try_execute(complex, &complex_input, RunOptions { null_input: false });
    match out {
        TryExecute::Executed(Ok(values)) => assert_eq!(
            values,
            vec![serde_json::json!({
                "id": 14,
                "group": 3,
                "score": 20.3,
                "txt": "alpha-beta",
                "ok": true
            })]
        ),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn expr_filter_operands_follow_jq_semantics() {
    let run = RunOptions { null_input: false };

    let out = try_execute(
        "length > 2",
        &[serde_json::json!([1, 2, 3]), serde_json::json!([1])],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!(true), serde_json::json!(false)]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let out = try_execute(
        r#"type == "array" and length > 1"#,
        &[serde_json::json!([1, 2]), serde_json::json!({"a": 1})],
        run,
    );
    match out {
        TryExecute::Executed(Ok(values)) => {
            assert_eq!(
                values,
                vec![serde_json::json!(true), serde_json::json!(false)]
            )
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn support_probe_matches_parser() {
    assert!(is_supported(".a | .b"));
    assert!(is_supported("@base64 | @base64d"));
    assert!(is_supported("try @base64d catch ."));
    assert!(is_supported("try @base64d catch [\"ko\", .]"));
    assert!(is_supported("try 0[implode] catch ."));
    assert!(is_supported("map(.)"));
    assert!(is_supported("add(.[])"));
    assert!(is_supported("ascii_downcase"));
    assert!(is_supported("explode"));
    assert!(is_supported("implode"));
    assert!(is_supported("abs"));
    assert!(is_supported("fabs"));
    assert!(is_supported("first"));
    assert!(is_supported("last"));
    assert!(is_supported("in([1])"));
    assert!(is_supported("inside([1])"));
    assert!(is_supported("to_entries"));
    assert!(is_supported("from_entries"));
    assert!(is_supported("with_entries(.)"));
    assert!(is_supported("indices(1)"));
    assert!(is_supported("index(\",\")"));
    assert!(is_supported("rindex(\",\")"));
    assert!(is_supported("flatten"));
    assert!(is_supported("flatten(2)"));
    assert!(is_supported("trim"));
    assert!(is_supported("ltrim"));
    assert!(is_supported("rtrim"));
    assert!(is_supported("map_values(.)"));
    assert!(is_supported("error(\"x\")"));
    assert!(is_supported("walk(.)"));
    assert!(is_supported("setpath([0]; 1)"));
    assert!(is_supported("split(\",\")"));
    assert!(is_supported("strptime(\"%Y-%m-%dT%H:%M:%SZ\")"));
    assert!(is_supported("strftime(\"%Y-%m-%dT%H:%M:%SZ\")"));
    assert!(is_supported("strflocaltime(\"%Y-%m-%dT%H:%M:%SZ\")"));
    assert!(is_supported("mktime"));
    assert!(is_supported("limit(2; range(5))"));
    assert!(is_supported("skip(2; range(5))"));
    assert!(is_supported("nth(2; range(5))"));
    assert!(is_supported("nth(0)"));
    assert!(is_supported("while(. < 3; . + 1)"));
    assert!(is_supported("until(. > 3; . + 1)"));
    assert!(is_supported("any"));
    assert!(is_supported("all"));
    assert!(is_supported("any(. > 1)"));
    assert!(is_supported("all(. > 1)"));
    assert!(is_supported("any(.[]; . > 1)"));
    assert!(is_supported("all(.[]; . > 1)"));
    assert!(is_supported("IN(1,2,3)"));
    assert!(is_supported("IN(1,2,3; 2)"));
    assert!(is_supported("INDEX(.)"));
    assert!(is_supported("INDEX(.[]; .)"));
    assert!(is_supported("recurse"));
    assert!(is_supported("recurse(.a?)"));
    assert!(is_supported("recurse(.a?; . != null)"));
    assert!(is_supported("paths"));
    assert!(is_supported("paths(type == \"number\")"));
    assert!(is_supported("combinations"));
    assert!(is_supported("combinations(2)"));
    assert!(is_supported("sort"));
    assert!(is_supported("sort_by(.)"));
    assert!(is_supported("group_by(.)"));
    assert!(is_supported("unique_by(.)"));
    assert!(is_supported("min_by(.)"));
    assert!(is_supported("max_by(.)"));
    assert!(is_supported("unique"));
    assert!(is_supported("min"));
    assert!(is_supported("max"));
    assert!(is_supported("reverse"));
    assert!(is_supported("transpose"));
    assert!(is_supported("bsearch(1)"));
    assert!(is_supported("length > 2"));
    assert!(is_supported("if . then 1 else 0 end"));
    assert!(is_supported(r#"type == "array" and length > 1"#));
    assert!(is_supported(
        r#"select(.active and (.id % 7 == 0)) | {id,group,score:(.a*3 + .b - (.value/10)),txt:(.text|ascii_downcase),ok:(.tags|length>2)}"#
    ));
}

#[test]
fn label_break_queries_are_rejected_without_runtime_crash() {
    let query = "[ label $if | range(10) | ., (select(. == 5) | break $if) ]";
    assert!(!is_supported(query));
    assert!(matches!(
        try_execute(
            query,
            &[serde_json::json!(null)],
            RunOptions { null_input: false }
        ),
        TryExecute::Unsupported
    ));
}

#[test]
fn execution_plan_prefers_sequential_for_regressing_stage_clusters() {
    let add = try_compile(".a + .b").expect("compile add");
    assert_eq!(add.preferred_execution_plan(), ExecutionPlan::Sequential);

    let modulo = try_compile(".value % 7").expect("compile mod");
    assert_eq!(modulo.preferred_execution_plan(), ExecutionPlan::Sequential);

    let length = try_compile(".tags | length").expect("compile length");
    assert_eq!(length.preferred_execution_plan(), ExecutionPlan::Sequential);

    let filter_pick =
        try_compile("select(.id > 2) | {id,group,value}").expect("compile select+pick");
    assert_eq!(
        filter_pick.preferred_execution_plan(),
        ExecutionPlan::Sequential
    );
}

#[test]
fn execution_plan_prefers_parallel_for_path_select_and_regex_clusters() {
    let path = try_compile(".id").expect("compile path");
    assert_eq!(path.preferred_execution_plan(), ExecutionPlan::Parallel);

    let select = try_compile("select(.id > 2) | .id").expect("compile select");
    assert_eq!(select.preferred_execution_plan(), ExecutionPlan::Parallel);

    let gsub = try_compile(r#".text | gsub("[aeiou]";"")"#).expect("compile gsub");
    assert_eq!(gsub.preferred_execution_plan(), ExecutionPlan::Parallel);

    let pick = try_compile("{id,group,value}").expect("compile pick");
    assert_eq!(pick.preferred_execution_plan(), ExecutionPlan::Parallel);
}

#[test]
fn parallel_override_mode_parsing_contract() {
    fn check(v: &str, expected: Option<bool>) {
        std::env::set_var("ZQ_NATIVE_PAR", v);
        assert_eq!(parse_parallel_override_mode(), expected, "value={v}");
        std::env::remove_var("ZQ_NATIVE_PAR");
    }

    check("1", Some(true));
    check("on", Some(true));
    check("true", Some(true));
    check("0", Some(false));
    check("off", Some(false));
    check("false", Some(false));
    check("auto", None);
    check("weird", None);
}
