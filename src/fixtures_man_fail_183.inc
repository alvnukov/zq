&[
    FixtureCase {
        query: r###"[., tojson] == if have_decnum then [12345678909876543212345,"12345678909876543212345"] else [12345678909876543000000,"12345678909876543000000"] end"###,
        input: r###"12345678909876543212345"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"[1234567890987654321,-1234567890987654321 | tojson] == if have_decnum then ["1234567890987654321","-1234567890987654321"] else ["1234567890987654400","-1234567890987654400"] end"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###". < 0.12345678901234567890123456788"###,
        input: r###"0.12345678901234567890123456789"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"map([., . == 1]) | tojson == if have_decnum then "[[1,true],[1.000,true],[1.0,true],[1.00,true]]" else "[[1,true],[1,true],[1,true],[1,true]]" end"###,
        input: r###"[1, 1.000, 1.0, 100e-2]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###". as $big | [$big, $big + 1] | map(. > 10000000000000000000000000000000) | . == if have_decnum then [true, false] else [false, false] end"###,
        input: r###"10000000000000000000000000000001"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"[.foo?]"###,
        input: r###"[1,2]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###".[2:4]"###,
        input: r###"["a","b","c","d","e"]"###,
        outputs: &[
            r###"["c", "d"]"###,
        ],
    },
    FixtureCase {
        query: r###".[2:4]"###,
        input: r###""abcdefghi""###,
        outputs: &[
            r###""cd""###,
        ],
    },
    FixtureCase {
        query: r###".[:3]"###,
        input: r###"["a","b","c","d","e"]"###,
        outputs: &[
            r###"["a", "b", "c"]"###,
        ],
    },
    FixtureCase {
        query: r###".[-2:]"###,
        input: r###"["a","b","c","d","e"]"###,
        outputs: &[
            r###"["d", "e"]"###,
        ],
    },
    FixtureCase {
        query: r###".[4,2]"###,
        input: r###"["a","b","c","d","e"]"###,
        outputs: &[
            r###""e""###,
            r###""c""###,
        ],
    },
    FixtureCase {
        query: r###"(. + 2) * 5"###,
        input: r###"1"###,
        outputs: &[
            r###"15"###,
        ],
    },
    FixtureCase {
        query: r###"[.user, .projects[]]"###,
        input: r###"{"user":"stedolan", "projects": ["jq", "wikiflow"]}"###,
        outputs: &[
            r###"["stedolan", "jq", "wikiflow"]"###,
        ],
    },
    FixtureCase {
        query: r###"[ .[] | . * 2]"###,
        input: r###"[1, 2, 3]"###,
        outputs: &[
            r###"[2, 4, 6]"###,
        ],
    },
    FixtureCase {
        query: r###"{user, title: .titles[]}"###,
        input: r###"{"user":"stedolan","titles":["JQ Primer", "More JQ"]}"###,
        outputs: &[
            r###"{"user":"stedolan", "title": "JQ Primer"}"###,
            r###"{"user":"stedolan", "title": "More JQ"}"###,
        ],
    },
    FixtureCase {
        query: r###"{(.user): .titles}"###,
        input: r###"{"user":"stedolan","titles":["JQ Primer", "More JQ"]}"###,
        outputs: &[
            r###"{"stedolan": ["JQ Primer", "More JQ"]}"###,
        ],
    },
    FixtureCase {
        query: r###".. | .a?"###,
        input: r###"[[{"a":1}]]"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###".a + 1"###,
        input: r###"{"a": 7}"###,
        outputs: &[
            r###"8"###,
        ],
    },
    FixtureCase {
        query: r###".a + .b"###,
        input: r###"{"a": [1,2], "b": [3,4]}"###,
        outputs: &[
            r###"[1,2,3,4]"###,
        ],
    },
    FixtureCase {
        query: r###".a + null"###,
        input: r###"{"a": 1}"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###".a + 1"###,
        input: r###"{}"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"{a: 1} + {b: 2} + {c: 3} + {a: 42}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a": 42, "b": 2, "c": 3}"###,
        ],
    },
    FixtureCase {
        query: r###"4 - .a"###,
        input: r###"{"a":3}"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###". - ["xml", "yaml"]"###,
        input: r###"["xml", "yaml", "json"]"###,
        outputs: &[
            r###"["json"]"###,
        ],
    },
    FixtureCase {
        query: r###"10 / . * 3"###,
        input: r###"5"###,
        outputs: &[
            r###"6"###,
        ],
    },
    FixtureCase {
        query: r###". / ", ""###,
        input: r###""a, b,c,d, e""###,
        outputs: &[
            r###"["a","b,c,d","e"]"###,
        ],
    },
    FixtureCase {
        query: r###"{"k": {"a": 1, "b": 2}} * {"k": {"a": 0,"c": 3}}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"k": {"a": 0, "b": 2, "c": 3}}"###,
        ],
    },
    FixtureCase {
        query: r###".[] | (1 / .)?"###,
        input: r###"[1,0,-1]"###,
        outputs: &[
            r###"1"###,
            r###"-1"###,
        ],
    },
    FixtureCase {
        query: r###"map(abs)"###,
        input: r###"[-10, -1.1, -1e-1]"###,
        outputs: &[
            r###"[10,1.1,1e-1]"###,
        ],
    },
    FixtureCase {
        query: r###".[] | length"###,
        input: r###"[[1,2], "string", {"a":2}, null, -5]"###,
        outputs: &[
            r###"2"###,
            r###"6"###,
            r###"1"###,
            r###"0"###,
            r###"5"###,
        ],
    },
    FixtureCase {
        query: r###"keys"###,
        input: r###"{"abc": 1, "abcd": 2, "Foo": 3}"###,
        outputs: &[
            r###"["Foo", "abc", "abcd"]"###,
        ],
    },
    FixtureCase {
        query: r###".[] | in({"foo": 42})"###,
        input: r###"["foo", "bar"]"###,
        outputs: &[
            r###"true"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"map(in([0,1]))"###,
        input: r###"[2, 0]"###,
        outputs: &[
            r###"[false, true]"###,
        ],
    },
    FixtureCase {
        query: r###"map(.+1)"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"[2,3,4]"###,
        ],
    },
    FixtureCase {
        query: r###"map_values(.+1)"###,
        input: r###"{"a": 1, "b": 2, "c": 3}"###,
        outputs: &[
            r###"{"a": 2, "b": 3, "c": 4}"###,
        ],
    },
    FixtureCase {
        query: r###"map(., .)"###,
        input: r###"[1,2]"###,
        outputs: &[
            r###"[1,1,2,2]"###,
        ],
    },
    FixtureCase {
        query: r###"map_values(. // empty)"###,
        input: r###"{"a": null, "b": true, "c": false}"###,
        outputs: &[
            r###"{"b":true}"###,
        ],
    },
    FixtureCase {
        query: r###"pick(.a, .b.c, .x)"###,
        input: r###"{"a": 1, "b": {"c": 2, "d": 3}, "e": 4}"###,
        outputs: &[
            r###"{"a":1,"b":{"c":2},"x":null}"###,
        ],
    },
    FixtureCase {
        query: r###"pick(.[2], .[0], .[0])"###,
        input: r###"[1,2,3,4]"###,
        outputs: &[
            r###"[1,null,3]"###,
        ],
    },
    FixtureCase {
        query: r###"path(.a[0].b)"###,
        input: r###"null"###,
        outputs: &[
            r###"["a",0,"b"]"###,
        ],
    },
    FixtureCase {
        query: r###"[path(..)]"###,
        input: r###"{"a":[{"b":1}]}"###,
        outputs: &[
            r###"[[],["a"],["a",0],["a",0,"b"]]"###,
        ],
    },
    FixtureCase {
        query: r###"del(.foo)"###,
        input: r###"{"foo": 42, "bar": 9001, "baz": 42}"###,
        outputs: &[
            r###"{"bar": 9001, "baz": 42}"###,
        ],
    },
    FixtureCase {
        query: r###"del(.[1, 2])"###,
        input: r###"["foo", "bar", "baz"]"###,
        outputs: &[
            r###"["foo"]"###,
        ],
    },
    FixtureCase {
        query: r###"getpath(["a","b"])"###,
        input: r###"null"###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"[getpath(["a","b"], ["a","c"])]"###,
        input: r###"{"a":{"b":0, "c":1}}"###,
        outputs: &[
            r###"[0, 1]"###,
        ],
    },
    FixtureCase {
        query: r###"setpath(["a","b"]; 1)"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a": {"b": 1}}"###,
        ],
    },
    FixtureCase {
        query: r###"setpath(["a","b"]; 1)"###,
        input: r###"{"a":{"b":0}}"###,
        outputs: &[
            r###"{"a": {"b": 1}}"###,
        ],
    },
    FixtureCase {
        query: r###"setpath([0,"a"]; 1)"###,
        input: r###"null"###,
        outputs: &[
            r###"[{"a":1}]"###,
        ],
    },
    FixtureCase {
        query: r###"delpaths([["a","b"]])"###,
        input: r###"{"a":{"b":1},"x":{"y":2}}"###,
        outputs: &[
            r###"{"a":{},"x":{"y":2}}"###,
        ],
    },
    FixtureCase {
        query: r###"from_entries"###,
        input: r###"[{"key":"a", "value":1}, {"key":"b", "value":2}]"###,
        outputs: &[
            r###"{"a": 1, "b": 2}"###,
        ],
    },
    FixtureCase {
        query: r###"map(select(. >= 2))"###,
        input: r###"[1,5,3,0,7]"###,
        outputs: &[
            r###"[5,3,7]"###,
        ],
    },
    FixtureCase {
        query: r###".[] | select(.id == "second")"###,
        input: r###"[{"id": "first", "val": 1}, {"id": "second", "val": 2}]"###,
        outputs: &[
            r###"{"id": "second", "val": 2}"###,
        ],
    },
    FixtureCase {
        query: r###".[]|numbers"###,
        input: r###"[[],{},1,"foo",null,true,false]"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"1, empty, 2"###,
        input: r###"null"###,
        outputs: &[
            r###"1"###,
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###"[1,2,empty,3]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"try error catch ."###,
        input: r###""error message""###,
        outputs: &[
            r###""error message""###,
        ],
    },
    FixtureCase {
        query: r###"try error("invalid value: \(.)") catch ."###,
        input: r###"42"###,
        outputs: &[
            r###""invalid value: 42""###,
        ],
    },
    FixtureCase {
        query: r###"[paths(type == "number")]"###,
        input: r###"[1,[[],{"a":2}]]"###,
        outputs: &[
            r###"[[0],[1,1,"a"]]"###,
        ],
    },
    FixtureCase {
        query: r###"add"###,
        input: r###"["a","b","c"]"###,
        outputs: &[
            r###""abc""###,
        ],
    },
    FixtureCase {
        query: r###"add"###,
        input: r###"[1, 2, 3]"###,
        outputs: &[
            r###"6"###,
        ],
    },
    FixtureCase {
        query: r###"add"###,
        input: r###"[]"###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"add(.[].a)"###,
        input: r###"[{"a":3}, {"a":5}, {"b":6}]"###,
        outputs: &[
            r###"8"###,
        ],
    },
    FixtureCase {
        query: r###"any"###,
        input: r###"[true, false]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"any"###,
        input: r###"[false, false]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"any"###,
        input: r###"[]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"all"###,
        input: r###"[true, false]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"all"###,
        input: r###"[true, true]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"all"###,
        input: r###"[]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"flatten"###,
        input: r###"[1, [2], [[3]]]"###,
        outputs: &[
            r###"[1, 2, 3]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten(1)"###,
        input: r###"[1, [2], [[3]]]"###,
        outputs: &[
            r###"[1, 2, [3]]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten"###,
        input: r###"[[]]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten"###,
        input: r###"[{"foo": "bar"}, [{"foo": "baz"}]]"###,
        outputs: &[
            r###"[{"foo": "bar"}, {"foo": "baz"}]"###,
        ],
    },
    FixtureCase {
        query: r###"range(2; 4)"###,
        input: r###"null"###,
        outputs: &[
            r###"2"###,
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"floor"###,
        input: r###"3.14159"###,
        outputs: &[
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"sqrt"###,
        input: r###"9"###,
        outputs: &[
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###".[] | tonumber"###,
        input: r###"[1, "1"]"###,
        outputs: &[
            r###"1"###,
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###".[] | toboolean"###,
        input: r###"["true", "false", true, false]"###,
        outputs: &[
            r###"true"###,
            r###"false"###,
            r###"true"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###".[] | tostring"###,
        input: r###"[1, "1", [1]]"###,
        outputs: &[
            r###""1""###,
            r###""1""###,
            r###""[1]""###,
        ],
    },
    FixtureCase {
        query: r###"map(type)"###,
        input: r###"[0, false, [], {}, null, "hello"]"###,
        outputs: &[
            r###"["number", "boolean", "array", "object", "null", "string"]"###,
        ],
    },
    FixtureCase {
        query: r###".[] | (infinite * .) < 0"###,
        input: r###"[-1, 1]"###,
        outputs: &[
            r###"true"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"infinite, nan | type"###,
        input: r###"null"###,
        outputs: &[
            r###""number""###,
            r###""number""###,
        ],
    },
    FixtureCase {
        query: r###"sort"###,
        input: r###"[8,3,null,6]"###,
        outputs: &[
            r###"[null,3,6,8]"###,
        ],
    },
    FixtureCase {
        query: r###"sort_by(.foo)"###,
        input: r###"[{"foo":4, "bar":10}, {"foo":3, "bar":10}, {"foo":2, "bar":1}]"###,
        outputs: &[
            r###"[{"foo":2, "bar":1}, {"foo":3, "bar":10}, {"foo":4, "bar":10}]"###,
        ],
    },
    FixtureCase {
        query: r###"sort_by(.foo, .bar)"###,
        input: r###"[{"foo":4, "bar":10}, {"foo":3, "bar":20}, {"foo":2, "bar":1}, {"foo":3, "bar":10}]"###,
        outputs: &[
            r###"[{"foo":2, "bar":1}, {"foo":3, "bar":10}, {"foo":3, "bar":20}, {"foo":4, "bar":10}]"###,
        ],
    },
    FixtureCase {
        query: r###"group_by(.foo)"###,
        input: r###"[{"foo":1, "bar":10}, {"foo":3, "bar":100}, {"foo":1, "bar":1}]"###,
        outputs: &[
            r###"[[{"foo":1, "bar":10}, {"foo":1, "bar":1}], [{"foo":3, "bar":100}]]"###,
        ],
    },
    FixtureCase {
        query: r###"min"###,
        input: r###"[5,4,2,7]"###,
        outputs: &[
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###"max_by(.foo)"###,
        input: r###"[{"foo":1, "bar":14}, {"foo":2, "bar":3}]"###,
        outputs: &[
            r###"{"foo":2, "bar":3}"###,
        ],
    },
    FixtureCase {
        query: r###"unique_by(.foo)"###,
        input: r###"[{"foo": 1, "bar": 2}, {"foo": 1, "bar": 3}, {"foo": 4, "bar": 5}]"###,
        outputs: &[
            r###"[{"foo": 1, "bar": 2}, {"foo": 4, "bar": 5}]"###,
        ],
    },
    FixtureCase {
        query: r###"unique_by(length)"###,
        input: r###"["chunky", "bacon", "kitten", "cicada", "asparagus"]"###,
        outputs: &[
            r###"["bacon", "chunky", "asparagus"]"###,
        ],
    },
    FixtureCase {
        query: r###"reverse"###,
        input: r###"[1,2,3,4]"###,
        outputs: &[
            r###"[4,3,2,1]"###,
        ],
    },
    FixtureCase {
        query: r###"contains("bar")"###,
        input: r###""foobar""###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"contains(["baz", "bar"])"###,
        input: r###"["foobar", "foobaz", "blarp"]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"contains(["bazzzzz", "bar"])"###,
        input: r###"["foobar", "foobaz", "blarp"]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"contains({foo: 12, bar: [{barp: 12}]})"###,
        input: r###"{"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"contains({foo: 12, bar: [{barp: 15}]})"###,
        input: r###"{"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]}"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"indices(", ")"###,
        input: r###""a,b, cd, efg, hijk""###,
        outputs: &[
            r###"[3,7,12]"###,
        ],
    },
    FixtureCase {
        query: r###"indices(1)"###,
        input: r###"[0,1,2,1,3,1,4]"###,
        outputs: &[
            r###"[1,3,5]"###,
        ],
    },
    FixtureCase {
        query: r###"index(", ")"###,
        input: r###""a,b, cd, efg, hijk""###,
        outputs: &[
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"index(1)"###,
        input: r###"[0,1,2,1,3,1,4]"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"index([1,2])"###,
        input: r###"[0,1,2,3,1,4,2,5,1,2,6,7]"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"rindex(", ")"###,
        input: r###""a,b, cd, efg, hijk""###,
        outputs: &[
            r###"12"###,
        ],
    },
    FixtureCase {
        query: r###"rindex(1)"###,
        input: r###"[0,1,2,1,3,1,4]"###,
        outputs: &[
            r###"5"###,
        ],
    },
    FixtureCase {
        query: r###"rindex([1,2])"###,
        input: r###"[0,1,2,3,1,4,2,5,1,2,6,7]"###,
        outputs: &[
            r###"8"###,
        ],
    },
    FixtureCase {
        query: r###"inside("foobar")"###,
        input: r###""bar""###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"inside(["foobar", "foobaz", "blarp"])"###,
        input: r###"["baz", "bar"]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"inside(["foobar", "foobaz", "blarp"])"###,
        input: r###"["bazzzzz", "bar"]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"inside({"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]})"###,
        input: r###"{"foo": 12, "bar": [{"barp": 12}]}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"inside({"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]})"###,
        input: r###"{"foo": 12, "bar": [{"barp": 15}]}"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|endswith("foo")]"###,
        input: r###"["foobar", "barfoo"]"###,
        outputs: &[
            r###"[false, true]"###,
        ],
    },
    FixtureCase {
        query: r###"combinations"###,
        input: r###"[[1,2], [3, 4]]"###,
        outputs: &[
            r###"[1, 3]"###,
            r###"[1, 4]"###,
            r###"[2, 3]"###,
            r###"[2, 4]"###,
        ],
    },
    FixtureCase {
        query: r###"combinations(2)"###,
        input: r###"[0, 1]"###,
        outputs: &[
            r###"[0, 0]"###,
            r###"[0, 1]"###,
            r###"[1, 0]"###,
            r###"[1, 1]"###,
        ],
    },
    FixtureCase {
        query: r###"trim, ltrim, rtrim"###,
        input: r###"" abc ""###,
        outputs: &[
            r###""abc""###,
            r###""abc ""###,
            r###"" abc""###,
        ],
    },
    FixtureCase {
        query: r###"explode"###,
        input: r###""foobar""###,
        outputs: &[
            r###"[102,111,111,98,97,114]"###,
        ],
    },
    FixtureCase {
        query: r###"implode"###,
        input: r###"[65, 66, 67]"###,
        outputs: &[
            r###""ABC""###,
        ],
    },
    FixtureCase {
        query: r###"join(", ")"###,
        input: r###"["a","b,c,d","e"]"###,
        outputs: &[
            r###""a, b,c,d, e""###,
        ],
    },
    FixtureCase {
        query: r###"join(" ")"###,
        input: r###"["a",1,2.3,true,null,false]"###,
        outputs: &[
            r###""a 1 2.3 true  false""###,
        ],
    },
    FixtureCase {
        query: r###"[repeat(.*2, error)?]"###,
        input: r###"1"###,
        outputs: &[
            r###"[2]"###,
        ],
    },
    FixtureCase {
        query: r###"[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]"###,
        input: r###"4"###,
        outputs: &[
            r###"24"###,
        ],
    },
    FixtureCase {
        query: r###"recurse(.foo[])"###,
        input: r###"{"foo":[{"foo": []}, {"foo":[{"foo":[]}]}]}"###,
        outputs: &[
            r###"{"foo":[{"foo":[]},{"foo":[{"foo":[]}]}]}"###,
            r###"{"foo":[]}"###,
            r###"{"foo":[{"foo":[]}]}"###,
            r###"{"foo":[]}"###,
        ],
    },
    FixtureCase {
        query: r###"recurse"###,
        input: r###"{"a":0,"b":[1]}"###,
        outputs: &[
            r###"{"a":0,"b":[1]}"###,
            r###"0"###,
            r###"[1]"###,
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"recurse(. * .; . < 20)"###,
        input: r###"2"###,
        outputs: &[
            r###"2"###,
            r###"4"###,
            r###"16"###,
        ],
    },
    FixtureCase {
        query: r###"walk(if type == "array" then sort else . end)"###,
        input: r###"[[4, 1, 7], [8, 5, 2], [3, 6, 9]]"###,
        outputs: &[
            r###"[[1,4,7],[2,5,8],[3,6,9]]"###,
        ],
    },
    FixtureCase {
        query: r###"$ENV.PAGER"###,
        input: r###"null"###,
        outputs: &[
            r###""less""###,
        ],
    },
    FixtureCase {
        query: r###"env.PAGER"###,
        input: r###"null"###,
        outputs: &[
            r###""less""###,
        ],
    },
    FixtureCase {
        query: r###"bsearch(0)"###,
        input: r###"[0,1]"###,
        outputs: &[
            r###"0"###,
        ],
    },
    FixtureCase {
        query: r###"bsearch(0)"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"-1"###,
        ],
    },
    FixtureCase {
        query: r###"bsearch(4) as $ix | if $ix < 0 then .[-(1+$ix)] = 4 else . end"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"[1,2,3,4]"###,
        ],
    },
    FixtureCase {
        query: r###""The input was \(.), which is one less than \(.+1)""###,
        input: r###"42"###,
        outputs: &[
            r###""The input was 42, which is one less than 43""###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|tostring]"###,
        input: r###"[1, "foo", ["foo"]]"###,
        outputs: &[
            r###"["1","foo","[\"foo\"]"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|tojson]"###,
        input: r###"[1, "foo", ["foo"]]"###,
        outputs: &[
            r###"["1","\"foo\"","[\"foo\"]"]"###,
        ],
    },
    FixtureCase {
        query: r###"@sh "echo \(.)""###,
        input: r###""O'Hara's Ale""###,
        outputs: &[
            r###""echo 'O'\\''Hara'\\''s Ale'""###,
        ],
    },
    FixtureCase {
        query: r###"fromdate"###,
        input: r###""2015-03-05T23:51:47Z""###,
        outputs: &[
            r###"1425599507"###,
        ],
    },
    FixtureCase {
        query: r###"strptime("%Y-%m-%dT%H:%M:%SZ")"###,
        input: r###""2015-03-05T23:51:47Z""###,
        outputs: &[
            r###"[2015,2,5,23,51,47,4,63]"###,
        ],
    },
    FixtureCase {
        query: r###"strptime("%Y-%m-%dT%H:%M:%SZ")|mktime"###,
        input: r###""2015-03-05T23:51:47Z""###,
        outputs: &[
            r###"1425599507"###,
        ],
    },
    FixtureCase {
        query: r###". == {"b": {"d": (4 + 1e-20), "c": 3}, "a":1}"###,
        input: r###"{"a":1, "b": {"c": 3, "d": 4}}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###".[] == 1"###,
        input: r###"[1, 1.0, "1", "banana"]"###,
        outputs: &[
            r###"true"###,
            r###"true"###,
            r###"false"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"if . == 0 then   "zero" elif . == 1 then   "one" else   "many" end"###,
        input: r###"2"###,
        outputs: &[
            r###""many""###,
        ],
    },
    FixtureCase {
        query: r###". < 5"###,
        input: r###"2"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"42 and "a string""###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"(true, false) or false"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"(true, true) and (true, false)"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
            r###"false"###,
            r###"true"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"[true, false | not]"###,
        input: r###"null"###,
        outputs: &[
            r###"[false, true]"###,
        ],
    },
    FixtureCase {
        query: r###"empty // 42"###,
        input: r###"null"###,
        outputs: &[
            r###"42"###,
        ],
    },
    FixtureCase {
        query: r###".foo // 42"###,
        input: r###"{"foo": 19}"###,
        outputs: &[
            r###"19"###,
        ],
    },
    FixtureCase {
        query: r###".foo // 42"###,
        input: r###"{}"###,
        outputs: &[
            r###"42"###,
        ],
    },
    FixtureCase {
        query: r###"(false, null, 1) // 42"###,
        input: r###"null"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"(false, null, 1) | . // 42"###,
        input: r###"null"###,
        outputs: &[
            r###"42"###,
            r###"42"###,
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"try .a catch ". is not an object""###,
        input: r###"true"###,
        outputs: &[
            r###"". is not an object""###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|try .a]"###,
        input: r###"[{}, true, {"a":1}]"###,
        outputs: &[
            r###"[null, 1]"###,
        ],
    },
    FixtureCase {
        query: r###"try error("some exception") catch ."###,
        input: r###"true"###,
        outputs: &[
            r###""some exception""###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | .a?]"###,
        input: r###"[{}, true, {"a":1}]"###,
        outputs: &[
            r###"[null, 1]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | tonumber?]"###,
        input: r###"["1", "invalid", "3", 4]"###,
        outputs: &[
            r###"[1, 3, 4]"###,
        ],
    },
    FixtureCase {
        query: r###".bar as $x | .foo | . + $x"###,
        input: r###"{"foo":10, "bar":200}"###,
        outputs: &[
            r###"210"###,
        ],
    },
    FixtureCase {
        query: r###". as $i|[(.*2|. as $i| $i), $i]"###,
        input: r###"5"###,
        outputs: &[
            r###"[10,5]"###,
        ],
    },
    FixtureCase {
        query: r###". as [$a, $b, {c: $c}] | $a + $b + $c"###,
        input: r###"[2, 3, {"c": 4, "d": 5}]"###,
        outputs: &[
            r###"9"###,
        ],
    },
    FixtureCase {
        query: r###".[] as [$a, $b] | {a: $a, b: $b}"###,
        input: r###"[[0], [0, 1], [2, 1, 0]]"###,
        outputs: &[
            r###"{"a":0,"b":null}"###,
            r###"{"a":0,"b":1}"###,
            r###"{"a":2,"b":1}"###,
        ],
    },
    FixtureCase {
        query: r###".[] as {$a, $b, c: {$d, $e}} ?// {$a, $b, c: [{$d, $e}]} | {$a, $b, $d, $e}"###,
        input: r###"[{"a": 1, "b": 2, "c": {"d": 3, "e": 4}}, {"a": 1, "b": 2, "c": [{"d": 3, "e": 4}]}]"###,
        outputs: &[
            r###"{"a":1,"b":2,"d":3,"e":4}"###,
            r###"{"a":1,"b":2,"d":3,"e":4}"###,
        ],
    },
    FixtureCase {
        query: r###".[] as {$a, $b, c: {$d}} ?// {$a, $b, c: [{$e}]} | {$a, $b, $d, $e}"###,
        input: r###"[{"a": 1, "b": 2, "c": {"d": 3, "e": 4}}, {"a": 1, "b": 2, "c": [{"d": 3, "e": 4}]}]"###,
        outputs: &[
            r###"{"a":1,"b":2,"d":3,"e":null}"###,
            r###"{"a":1,"b":2,"d":null,"e":4}"###,
        ],
    },
    FixtureCase {
        query: r###".[] as [$a] ?// [$b] | if $a != null then error("err: \($a)") else {$a,$b} end"###,
        input: r###"[[3]]"###,
        outputs: &[
            r###"{"a":null,"b":3}"###,
        ],
    },
    FixtureCase {
        query: r###"def addvalue(f): . + [f]; map(addvalue(.[0]))"###,
        input: r###"[[1,2],[10,20]]"###,
        outputs: &[
            r###"[[1,2,1], [10,20,10]]"###,
        ],
    },
    FixtureCase {
        query: r###"def addvalue(f): f as $x | map(. + $x); addvalue(.[0])"###,
        input: r###"[[1,2],[10,20]]"###,
        outputs: &[
            r###"[[1,2,1,2], [10,20,1,2]]"###,
        ],
    },
    FixtureCase {
        query: r###"isempty(.[])"###,
        input: r###"[]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"isempty(.[])"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"[first(range(.)), last(range(.)), nth(5; range(.))]"###,
        input: r###"10"###,
        outputs: &[
            r###"[0,9,5]"###,
        ],
    },
    FixtureCase {
        query: r###"[first(empty), last(empty), nth(5; empty)]"###,
        input: r###"null"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(.)]|[first, last, nth(5)]"###,
        input: r###"10"###,
        outputs: &[
            r###"[0,9,5]"###,
        ],
    },
    FixtureCase {
        query: r###"reduce .[] as $item (0; . + $item)"###,
        input: r###"[1,2,3,4,5]"###,
        outputs: &[
            r###"15"###,
        ],
    },
    FixtureCase {
        query: r###"reduce .[] as [$i,$j] (0; . + $i * $j)"###,
        input: r###"[[1,2],[3,4],[5,6]]"###,
        outputs: &[
            r###"44"###,
        ],
    },
    FixtureCase {
        query: r###"reduce .[] as {$x,$y} (null; .x += $x | .y += [$y])"###,
        input: r###"[{"x":"a","y":1},{"x":"b","y":2},{"x":"c","y":3}]"###,
        outputs: &[
            r###"{"x":"abc","y":[1,2,3]}"###,
        ],
    },
    FixtureCase {
        query: r###"foreach .[] as $item (0; . + $item)"###,
        input: r###"[1,2,3,4,5]"###,
        outputs: &[
            r###"1"###,
            r###"3"###,
            r###"6"###,
            r###"10"###,
            r###"15"###,
        ],
    },
    FixtureCase {
        query: r###"foreach .[] as $item (0; . + $item; [$item, . * 2])"###,
        input: r###"[1,2,3,4,5]"###,
        outputs: &[
            r###"[1,2]"###,
            r###"[2,6]"###,
            r###"[3,12]"###,
            r###"[4,20]"###,
            r###"[5,30]"###,
        ],
    },
    FixtureCase {
        query: r###"foreach .[] as $item (0; . + 1; {index: ., $item})"###,
        input: r###"["foo", "bar", "baz"]"###,
        outputs: &[
            r###"{"index":1,"item":"foo"}"###,
            r###"{"index":2,"item":"bar"}"###,
            r###"{"index":3,"item":"baz"}"###,
        ],
    },
    FixtureCase {
        query: r###"def range(init; upto; by): def _range: if (by > 0 and . < upto) or (by < 0 and . > upto) then ., ((.+by)|_range) else empty end; if init == upto then empty elif by == 0 then init else init|_range end; range(0; 10; 3)"###,
        input: r###"null"###,
        outputs: &[
            r###"0"###,
            r###"3"###,
            r###"6"###,
            r###"9"###,
        ],
    },
    FixtureCase {
        query: r###"def while(cond; update): def _while: if cond then ., (update | _while) else empty end; _while; [while(.<100; .*2)]"###,
        input: r###"1"###,
        outputs: &[
            r###"[1,2,4,8,16,32,64]"###,
        ],
    },
    FixtureCase {
        query: r###"truncate_stream([[0],"a"],[[1,0],"b"],[[1,0]],[[1]])"###,
        input: r###"1"###,
        outputs: &[
            r###"[[0],"b"]"###,
            r###"[[0]]"###,
        ],
    },
    FixtureCase {
        query: r###"fromstream(1|truncate_stream([[0],"a"],[[1,0],"b"],[[1,0]],[[1]]))"###,
        input: r###"null"###,
        outputs: &[
            r###"["b"]"###,
        ],
    },
    FixtureCase {
        query: r###". as $dot|fromstream($dot|tostream)|.==$dot"###,
        input: r###"[0,[1,{"a":1},{"b":2}]]"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"(..|select(type=="boolean")) |= if . then 1 else 0 end"###,
        input: r###"[true,false,[5,true,[true,[false]],false]]"###,
        outputs: &[
            r###"[1,0,[5,1,[1,[0]],0]]"###,
        ],
    },
    FixtureCase {
        query: r###".foo += 1"###,
        input: r###"{"foo": 42}"###,
        outputs: &[
            r###"{"foo": 43}"###,
        ],
    },
    FixtureCase {
        query: r###".a = .b"###,
        input: r###"{"a": {"b": 10}, "b": 20}"###,
        outputs: &[
            r###"{"a":20,"b":20}"###,
        ],
    },
    FixtureCase {
        query: r###".a |= .b"###,
        input: r###"{"a": {"b": 10}, "b": 20}"###,
        outputs: &[
            r###"{"a":10,"b":20}"###,
        ],
    },
    FixtureCase {
        query: r###"(.a, .b) = range(3)"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a":0,"b":0}"###,
            r###"{"a":1,"b":1}"###,
            r###"{"a":2,"b":2}"###,
        ],
    },
    FixtureCase {
        query: r###"(.a, .b) |= range(3)"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a":0,"b":0}"###,
        ],
    },
]
