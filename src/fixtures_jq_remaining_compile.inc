&[
    FixtureCase {
        query: r###"[.[] | not]"###,
        input: r###"[1,0,false,null,true,"hello"]"###,
        outputs: &[
            r###"[false,false,true,true,false,false]"###,
        ],
    },
    FixtureCase {
        query: r###"[10 > 0, 10 > 10, 10 > 20, 10 < 0, 10 < 10, 10 < 20]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false,false,false,false,true]"###,
        ],
    },
    FixtureCase {
        query: r###"[10 >= 0, 10 >= 10, 10 >= 20, 10 <= 0, 10 <= 10, 10 <= 20]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,true,false,false,true,true]"###,
        ],
    },
    FixtureCase {
        query: r###"[ 10 == 10, 10 != 10, 10 != 11, 10 == 11]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false,true,false]"###,
        ],
    },
    FixtureCase {
        query: r###"["hello" == "hello", "hello" != "hello", "hello" == "world", "hello" != "world" ]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false,false,true]"###,
        ],
    },
    FixtureCase {
        query: r###"[[1,2,3] == [1,2,3], [1,2,3] != [1,2,3], [1,2,3] == [4,5,6], [1,2,3] != [4,5,6]]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false,false,true]"###,
        ],
    },
    FixtureCase {
        query: r###"[{"foo":42} == {"foo":42},{"foo":42} != {"foo":42}, {"foo":42} != {"bar":42}, {"foo":42} == {"bar":42}]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false,true,false]"###,
        ],
    },
    FixtureCase {
        query: r###"[{"foo":[1,2,{"bar":18},"world"]} == {"foo":[1,2,{"bar":18},"world"]},{"foo":[1,2,{"bar":18},"world"]} == {"foo":[1,2,{"bar":19},"world"]}]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true,false]"###,
        ],
    },
    FixtureCase {
        query: r###"[("foo" | contains("foo")), ("foobar" | contains("foo")), ("foo" | contains("foobar"))]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true, true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"try strftime("%Y-%m-%dT%H:%M:%SZ") catch ."###,
        input: r###"["a",1,2,3,4,5,6,7]"###,
        outputs: &[
            r###""strftime/1 requires parsed datetime inputs""###,
        ],
    },
    FixtureCase {
        query: r###"try strflocaltime("%Y-%m-%dT%H:%M:%SZ") catch ."###,
        input: r###"["a",1,2,3,4,5,6,7]"###,
        outputs: &[
            r###""strflocaltime/1 requires parsed datetime inputs""###,
        ],
    },
    FixtureCase {
        query: r###"try mktime catch ."###,
        input: r###"["a",1,2,3,4,5,6,7]"###,
        outputs: &[
            r###""mktime requires parsed datetime inputs""###,
        ],
    },
    FixtureCase {
        query: r###"try ["OK", strftime([])] catch ["KO", .]"###,
        input: r###"0"###,
        outputs: &[
            r###"["KO","strftime/1 requires a string format"]"###,
        ],
    },
    FixtureCase {
        query: r###"try ["OK", strflocaltime({})] catch ["KO", .]"###,
        input: r###"0"###,
        outputs: &[
            r###"["KO","strflocaltime/1 requires a string format"]"###,
        ],
    },
    FixtureCase {
        query: r###"[strptime("%Y-%m-%dT%H:%M:%SZ")|(.,mktime)]"###,
        input: r###""2015-03-05T23:51:47Z""###,
        outputs: &[
            r###"[[2015,2,5,23,51,47,4,63],1425599507]"###,
        ],
    },
    FixtureCase {
        query: r###"last(range(365 * 67)|("1970-03-01T01:02:03Z"|strptime("%Y-%m-%dT%H:%M:%SZ")|mktime) + (86400 * .)|strftime("%Y-%m-%dT%H:%M:%SZ")|strptime("%Y-%m-%dT%H:%M:%SZ"))"###,
        input: r###"null"###,
        outputs: &[
            r###"[2037,1,11,1,2,3,3,41]"###,
        ],
    },
    FixtureCase {
        query: r###"import "a" as foo; import "b" as bar; def fooa: foo::a; [fooa, bar::a, bar::b, foo::a]"###,
        input: r###"null"###,
        outputs: &[
            r###"["a","b","c","a"]"###,
        ],
    },
    FixtureCase {
        query: r###"import "c" as foo; [foo::a, foo::c]"###,
        input: r###"null"###,
        outputs: &[
            r###"[0,"acmehbah"]"###,
        ],
    },
    FixtureCase {
        query: r###"include "c"; [a, c]"###,
        input: r###"null"###,
        outputs: &[
            r###"[0,"acmehbah"]"###,
        ],
    },
    FixtureCase {
        query: r###"import "data" as $e; import "data" as $d; [$d[].this,$e[].that,$d::d[].this,$e::e[].that]|join(";")"###,
        input: r###"null"###,
        outputs: &[
            r###""is a test;is too;is a test;is too""###,
        ],
    },
    FixtureCase {
        query: r###"(.a as $x | .b) = "b""###,
        input: r###"{"a":null,"b":null}"###,
        outputs: &[
            r###"{"a":null,"b":"b"}"###,
        ],
    },
    FixtureCase {
        query: r###"(.. | select(type == "object" and has("b") and (.b | type) == "array")|.b) |= .[0]"###,
        input: r###"{"a": {"b": [1, {"b": 3}]}}"###,
        outputs: &[
            r###"{"a": {"b": 1}}"###,
        ],
    },
    FixtureCase {
        query: r###"isempty(empty)"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"isempty(range(3))"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"isempty(1,error("foo"))"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"index("")"###,
        input: r###""""###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"builtins|length > 10"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###""-1"|IN(builtins[] / "/"|.[1])"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"all(builtins[] / "/"; .[1]|tonumber >= 0)"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"builtins|any(.[:1] == "_")"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"map(. == 1)"###,
        input: r###"[1, 1.0, 1.000, 100e-2, 1e+0, 0.0001e4]"###,
        outputs: &[
            r###"[true, true, true, true, true, true]"###,
        ],
    },
    FixtureCase {
        query: r###".[0] | tostring | . == if have_decnum then "13911860366432393" else "13911860366432392" end"###,
        input: r###"[13911860366432393]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###".x | tojson | . == if have_decnum then "13911860366432393" else "13911860366432392" end"###,
        input: r###"{"x":13911860366432393}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"(13911860366432393 == 13911860366432392) | . == if have_decnum then false else true end"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###". - 10"###,
        input: r###"13911860366432393"###,
        outputs: &[
            r###"13911860366432382"###,
        ],
    },
    FixtureCase {
        query: r###".[0] - 10"###,
        input: r###"[13911860366432393]"###,
        outputs: &[
            r###"13911860366432382"###,
        ],
    },
    FixtureCase {
        query: r###".x - 10"###,
        input: r###"{"x":13911860366432393}"###,
        outputs: &[
            r###"13911860366432382"###,
        ],
    },
    FixtureCase {
        query: r###"-. | tojson == if have_decnum then "-13911860366432393" else "-13911860366432392" end"###,
        input: r###"13911860366432393"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"-. | tojson == if have_decnum then "0.12345678901234567890123456789" else "0.12345678901234568" end"###,
        input: r###"-0.12345678901234567890123456789"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"[1E+1000,-1E+1000 | tojson] == if have_decnum then ["1E+1000","-1E+1000"] else ["1.7976931348623157e+308","-1.7976931348623157e+308"] end"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###". |= try . catch ."###,
        input: r###"1"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###".[] as $n | $n+0 | [., tostring, . == $n]"###,
        input: r###"[-9007199254740993, -9007199254740992, 9007199254740992, 9007199254740993, 13911860366432393]"###,
        outputs: &[
            r###"[-9007199254740992,"-9007199254740992",true]"###,
            r###"[-9007199254740992,"-9007199254740992",true]"###,
            r###"[9007199254740992,"9007199254740992",true]"###,
            r###"[9007199254740992,"9007199254740992",true]"###,
            r###"[13911860366432392,"13911860366432392",true]"###,
        ],
    },
    FixtureCase {
        query: r###"abs"###,
        input: r###""abc""###,
        outputs: &[
            r###""abc""###,
        ],
    },
    FixtureCase {
        query: r###"map(abs)"###,
        input: r###"[-0, 0, -10, -1.1]"###,
        outputs: &[
            r###"[0,0,10,1.1]"###,
        ],
    },
    FixtureCase {
        query: r###"map(fabs)"###,
        input: r###"[-0, 0, -10, -1.1]"###,
        outputs: &[
            r###"[0,0,10,1.1]"###,
        ],
    },
    FixtureCase {
        query: r###"map(abs == length) | unique"###,
        input: r###"[-10, -1.1, -1e-1, 1000000000000000002]"###,
        outputs: &[
            r###"[true]"###,
        ],
    },
    FixtureCase {
        query: r###"map(abs)"###,
        input: r###"[0.1,1000000000000000002]"###,
        outputs: &[
            r###"[1e-1, 1000000000000000002]"###,
        ],
    },
    FixtureCase {
        query: r###"[1E+1000,-1E+1000 | abs | tojson] | unique == if have_decnum then ["1E+1000"] else ["1.7976931348623157e+308"] end"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"[1E+1000,-1E+1000 | length | tojson] | unique == if have_decnum then ["1E+1000"] else ["1.7976931348623157e+308"] end"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"{ a, $__loc__, c }"###,
        input: r###"{"a":[1,2,3],"b":"foo","c":{"hi":"hey"}}"###,
        outputs: &[
            r###"{"a":[1,2,3],"__loc__":{"file":"<top-level>","line":1},"c":{"hi":"hey"}}"###,
        ],
    },
    FixtureCase {
        query: r###"1 as $x | "2" as $y | "3" as $z | { $x, as, $y: 4, ($z): 5, if: 6, foo: 7 }"###,
        input: r###"{"as":8}"###,
        outputs: &[
            r###"{"x":1,"as":8,"2":4,"3":5,"if":6,"foo":7}"###,
        ],
    },
    FixtureCase {
        query: r###"fromjson | isnan"###,
        input: r###""nan""###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"tojson | fromjson"###,
        input: r###"{"a":nan}"###,
        outputs: &[
            r###"{"a":null}"###,
        ],
    },
    FixtureCase {
        query: r###".[] | try (fromjson | isnan) catch ."###,
        input: r###"["NaN","-NaN","NaN1","NaN10","NaN100","NaN1000","NaN10000","NaN100000"]"###,
        outputs: &[
            r###"true"###,
            r###"true"###,
            r###""Invalid numeric literal at EOF at line 1, column 4 (while parsing 'NaN1')""###,
            r###""Invalid numeric literal at EOF at line 1, column 5 (while parsing 'NaN10')""###,
            r###""Invalid numeric literal at EOF at line 1, column 6 (while parsing 'NaN100')""###,
            r###""Invalid numeric literal at EOF at line 1, column 7 (while parsing 'NaN1000')""###,
            r###""Invalid numeric literal at EOF at line 1, column 8 (while parsing 'NaN10000')""###,
            r###""Invalid numeric literal at EOF at line 1, column 9 (while parsing 'NaN100000')""###,
        ],
    },
    FixtureCase {
        query: r###"try input catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""break""###,
        ],
    },
    FixtureCase {
        query: r###"debug"###,
        input: r###"1"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"first(.?,.?)"###,
        input: r###"null"###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"{foo: "bar"} | .foo |= .?"###,
        input: r###"null"###,
        outputs: &[
            r###"{"foo": "bar"}"###,
        ],
    },
    FixtureCase {
        query: r###". |= try 2"###,
        input: r###"1"###,
        outputs: &[
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###". |= try 2 catch 3"###,
        input: r###"1"###,
        outputs: &[
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###".[] |= try tonumber"###,
        input: r###"["1", "2a", "3", " 4", "5 ", "6.7", ".89", "-876", "+5.43", 21]"###,
        outputs: &[
            r###"[1, 3, 6.7, 0.89, -876, 5.43, 21]"###,
        ],
    },
    FixtureCase {
        query: r###"any(keys[]|tostring?;true)"###,
        input: r###"{"a":"1","b":"2","c":"3"}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"implode|explode"###,
        input: r###"[-1,0,1,2,3,1114111,1114112,55295,55296,57343,57344,1.1,1.9]"###,
        outputs: &[
            r###"[65533,0,1,2,3,1114111,65533,55295,65533,65533,57344,1,1]"###,
        ],
    },
    FixtureCase {
        query: r###"map(try implode catch .)"###,
        input: r###"[123,["a"],[nan]]"###,
        outputs: &[
            r###"["implode input must be an array","string (\"a\") can't be imploded, unicode codepoint needs to be numeric","number (null) can't be imploded, unicode codepoint needs to be numeric"]"###,
        ],
    },
    FixtureCase {
        query: r###"try 0[implode] catch ."###,
        input: r###"[]"###,
        outputs: &[
            r###""Cannot index number with string \"\"""###,
        ],
    },
    FixtureCase {
        query: r###"walk(.)"###,
        input: r###"{"x":0}"###,
        outputs: &[
            r###"{"x":0}"###,
        ],
    },
    FixtureCase {
        query: r###"walk(1)"###,
        input: r###"{"x":0}"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"[walk(.,1)]"###,
        input: r###"{"x":0}"###,
        outputs: &[
            r###"[{"x":0},1]"###,
        ],
    },
    FixtureCase {
        query: r###"walk(select(IN({}, []) | not))"###,
        input: r###"{"a":1,"b":[]}"###,
        outputs: &[
            r###"{"a":1}"###,
        ],
    },
    FixtureCase {
        query: r###"try ["ok", setpath([1]; 1)] catch ["ko", .]"###,
        input: r###"{"hi":"hello"}"###,
        outputs: &[
            r###"["ko","Cannot index object with number (1)"]"###,
        ],
    },
    FixtureCase {
        query: r###"try fromjson catch ."###,
        input: r###""{'a': 123}""###,
        outputs: &[
            r###""Invalid string literal; expected \", but got ' at line 1, column 5 (while parsing '{'a': 123}')""###,
        ],
    },
    FixtureCase {
        query: r###"try ltrimstr(1) catch "x", try rtrimstr(1) catch "x" | "ok""###,
        input: r###""hi""###,
        outputs: &[
            r###""ok""###,
            r###""ok""###,
        ],
    },
    FixtureCase {
        query: r###"try ltrimstr("x") catch "x", try rtrimstr("x") catch "x" | "ok""###,
        input: r###"{"hey":[]}"###,
        outputs: &[
            r###""ok""###,
            r###""ok""###,
        ],
    },
    FixtureCase {
        query: r###".[] as [$x, $y] | try ["ok", ($x | ltrimstr($y))] catch ["ko", .]"###,
        input: r###"[["hi",1],[1,"hi"],["hi","hi"],[1,1]]"###,
        outputs: &[
            r###"["ko","startswith() requires string inputs"]"###,
            r###"["ko","startswith() requires string inputs"]"###,
            r###"["ok",""]"###,
            r###"["ko","startswith() requires string inputs"]"###,
        ],
    },
    FixtureCase {
        query: r###".[] as [$x, $y] | try ["ok", ($x | rtrimstr($y))] catch ["ko", .]"###,
        input: r###"[["hi",1],[1,"hi"],["hi","hi"],[1,1]]"###,
        outputs: &[
            r###"["ko","endswith() requires string inputs"]"###,
            r###"["ko","endswith() requires string inputs"]"###,
            r###"["ok",""]"###,
            r###"["ko","endswith() requires string inputs"]"###,
        ],
    },
    FixtureCase {
        query: r###"try ["OK", setpath([[1]]; 1)] catch ["KO", .]"###,
        input: r###"[]"###,
        outputs: &[
            r###"["KO","Cannot update field at array index of array"]"###,
        ],
    },
    FixtureCase {
        query: r###"foreach .[] as $x (0, 1; . + $x)"###,
        input: r###"[1, 2]"###,
        outputs: &[
            r###"1"###,
            r###"3"###,
            r###"2"###,
            r###"4"###,
        ],
    },
    FixtureCase {
        query: r###"strflocaltime("" | ., @uri)"###,
        input: r###"0"###,
        outputs: &[
            r###""""###,
            r###""""###,
        ],
    },
    FixtureCase {
        query: r###"reduce range(9999) as $_ ([];[.]) | tojson | fromjson | flatten"###,
        input: r###"null"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"reduce range(10000) as $_ ([];[.]) | tojson | try (fromjson) catch . | (contains("<skipped: too deep>") | not) and contains("Exceeds depth limit for parsing")"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"reduce range(10001) as $_ ([];[.]) | tojson | contains("<skipped: too deep>")"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
]
