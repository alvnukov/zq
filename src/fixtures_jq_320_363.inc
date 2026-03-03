&[
    FixtureCase {
        query: r###"map(trim), map(ltrim), map(rtrim)"###,
        input: r###"[" \n\t\r\f\u000b", "","  ", "a", " a ", "abc", "  abc  ", "  abc", "abc  "]"###,
        outputs: &[
            r###"["", "", "", "a", "a", "abc", "abc", "abc", "abc"]"###,
            r###"["", "", "", "a", "a ", "abc", "abc  ", "abc", "abc  "]"###,
            r###"["", "", "", "a", " a", "abc", "  abc", "  abc", "abc"]"###,
        ],
    },
    FixtureCase {
        query: r###"trim, ltrim, rtrim"###,
        input: r###""\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F\u3000abc\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F\u3000""###,
        outputs: &[
            r###""abc""###,
            r###""abc\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F\u3000""###,
            r###""\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F\u3000abc""###,
        ],
    },
    FixtureCase {
        query: r###"try trim catch ., try ltrim catch ., try rtrim catch ."###,
        input: r###"123"###,
        outputs: &[
            r###""trim input must be a string""###,
            r###""trim input must be a string""###,
            r###""trim input must be a string""###,
        ],
    },
    FixtureCase {
        query: r###"indices(1)"###,
        input: r###"[0,1,1,2,3,4,1,5]"###,
        outputs: &[
            r###"[1,2,6]"###,
        ],
    },
    FixtureCase {
        query: r###"indices([1,2])"###,
        input: r###"[0,1,2,3,1,4,2,5,1,2,6,7]"###,
        outputs: &[
            r###"[1,8]"###,
        ],
    },
    FixtureCase {
        query: r###"indices([1,2])"###,
        input: r###"[1]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"indices(", ")"###,
        input: r###""a,b, cd,e, fgh, ijkl""###,
        outputs: &[
            r###"[3,9,14]"###,
        ],
    },
    FixtureCase {
        query: r###"index("!")"###,
        input: r###""здравствуй мир!""###,
        outputs: &[
            r###"14"###,
        ],
    },
    FixtureCase {
        query: r###".[:rindex("x")]"###,
        input: r###""正xyz""###,
        outputs: &[
            r###""正""###,
        ],
    },
    FixtureCase {
        query: r###"indices("o")"###,
        input: r###""🇬🇧oo""###,
        outputs: &[
            r###"[2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"indices("o")"###,
        input: r###""ƒoo""###,
        outputs: &[
            r###"[1,2]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|split(",")]"###,
        input: r###"["a, bc, def, ghij, jklmn, a,b, c,d, e,f", "a,b,c,d, e,f,g,h"]"###,
        outputs: &[
            r###"[["a"," bc"," def"," ghij"," jklmn"," a","b"," c","d"," e","f"],["a","b","c","d"," e","f","g","h"]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|split(", ")]"###,
        input: r###"["a, bc, def, ghij, jklmn, a,b, c,d, e,f", "a,b,c,d, e,f,g,h"]"###,
        outputs: &[
            r###"[["a","bc","def","ghij","jklmn","a,b","c,d","e,f"],["a,b,c,d","e,f,g,h"]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] * 3]"###,
        input: r###"["a", "ab", "abc"]"###,
        outputs: &[
            r###"["aaa", "ababab", "abcabcabc"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] * "abc"]"###,
        input: r###"[-1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 3.7, 10.0]"###,
        outputs: &[
            r###"[null,null,"","","abc","abc","abcabcabc","abcabcabcabcabcabcabcabcabcabc"]"###,
        ],
    },
    FixtureCase {
        query: r###"[. * (nan,-nan)]"###,
        input: r###""abc""###,
        outputs: &[
            r###"[null,null]"###,
        ],
    },
    FixtureCase {
        query: r###". * 100000 | [.[:10],.[-10:]]"###,
        input: r###""abc""###,
        outputs: &[
            r###"["abcabcabca","cabcabcabc"]"###,
        ],
    },
    FixtureCase {
        query: r###". * 1000000000"###,
        input: r###""""###,
        outputs: &[
            r###""""###,
        ],
    },
    FixtureCase {
        query: r###"try (. * 1000000000) catch ."###,
        input: r###""abc""###,
        outputs: &[
            r###""Repeat string result too long""###,
        ],
    },
    FixtureCase {
        query: r###"[.[] / ","]"###,
        input: r###"["a, bc, def, ghij, jklmn, a,b, c,d, e,f", "a,b,c,d, e,f,g,h"]"###,
        outputs: &[
            r###"[["a"," bc"," def"," ghij"," jklmn"," a","b"," c","d"," e","f"],["a","b","c","d"," e","f","g","h"]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] / ", "]"###,
        input: r###"["a, bc, def, ghij, jklmn, a,b, c,d, e,f", "a,b,c,d, e,f,g,h"]"###,
        outputs: &[
            r###"[["a","bc","def","ghij","jklmn","a,b","c,d","e,f"],["a,b,c,d","e,f,g,h"]]"###,
        ],
    },
    FixtureCase {
        query: r###"map(.[1] as $needle | .[0] | contains($needle))"###,
        input: r###"[[[],[]], [[1,2,3], [1,2]], [[1,2,3], [3,1]], [[1,2,3], [4]], [[1,2,3], [1,4]]]"###,
        outputs: &[
            r###"[true, true, true, false, false]"###,
        ],
    },
    FixtureCase {
        query: r###"map(.[1] as $needle | .[0] | contains($needle))"###,
        input: r###"[[["foobar", "foobaz"], ["baz", "bar"]], [["foobar", "foobaz"], ["foo"]], [["foobar", "foobaz"], ["blap"]]]"###,
        outputs: &[
            r###"[true, true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"[({foo: 12, bar:13} | contains({foo: 12})), ({foo: 12} | contains({})), ({foo: 12, bar:13} | contains({baz:14}))]"###,
        input: r###"{}"###,
        outputs: &[
            r###"[true, true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"{foo: {baz: 12, blap: {bar: 13}}, bar: 14} | contains({bar: 14, foo: {blap: {}}})"###,
        input: r###"{}"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"{foo: {baz: 12, blap: {bar: 13}}, bar: 14} | contains({bar: 14, foo: {blap: {bar: 14}}})"###,
        input: r###"{}"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"sort"###,
        input: r###"[42,[2,5,3,11],10,{"a":42,"b":2},{"a":42},true,2,[2,6],"hello",null,[2,5,6],{"a":[],"b":1},"abc","ab",[3,10],{},false,"abcd",null]"###,
        outputs: &[
            r###"[null,null,false,true,2,10,42,"ab","abc","abcd","hello",[2,5,3,11],[2,5,6],[2,6],[3,10],{},{"a":42},{"a":42,"b":2},{"a":[],"b":1}]"###,
        ],
    },
    FixtureCase {
        query: r###"(sort_by(.b) | sort_by(.a)), sort_by(.a, .b), sort_by(.b, .c), group_by(.b), group_by(.a + .b - .c == 2)"###,
        input: r###"[{"a": 1, "b": 4, "c": 14}, {"a": 4, "b": 1, "c": 3}, {"a": 1, "b": 4, "c": 3}, {"a": 0, "b": 2, "c": 43}]"###,
        outputs: &[
            r###"[{"a": 0, "b": 2, "c": 43}, {"a": 1, "b": 4, "c": 14}, {"a": 1, "b": 4, "c": 3}, {"a": 4, "b": 1, "c": 3}]"###,
            r###"[{"a": 0, "b": 2, "c": 43}, {"a": 1, "b": 4, "c": 14}, {"a": 1, "b": 4, "c": 3}, {"a": 4, "b": 1, "c": 3}]"###,
            r###"[{"a": 4, "b": 1, "c": 3}, {"a": 0, "b": 2, "c": 43}, {"a": 1, "b": 4, "c": 3}, {"a": 1, "b": 4, "c": 14}]"###,
            r###"[[{"a": 4, "b": 1, "c": 3}], [{"a": 0, "b": 2, "c": 43}], [{"a": 1, "b": 4, "c": 14}, {"a": 1, "b": 4, "c": 3}]]"###,
            r###"[[{"a": 1, "b": 4, "c": 14}, {"a": 0, "b": 2, "c": 43}], [{"a": 4, "b": 1, "c": 3}, {"a": 1, "b": 4, "c": 3}]]"###,
        ],
    },
    FixtureCase {
        query: r###"unique"###,
        input: r###"[1,2,5,3,5,3,1,3]"###,
        outputs: &[
            r###"[1,2,3,5]"###,
        ],
    },
    FixtureCase {
        query: r###"unique"###,
        input: r###"[]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[min, max, min_by(.[1]), max_by(.[1]), min_by(.[2]), max_by(.[2])]"###,
        input: r###"[[4,2,"a"],[3,1,"a"],[2,4,"a"],[1,3,"a"]]"###,
        outputs: &[
            r###"[[1,3,"a"],[4,2,"a"],[3,1,"a"],[2,4,"a"],[4,2,"a"],[1,3,"a"]]"###,
        ],
    },
    FixtureCase {
        query: r###"[min,max,min_by(.),max_by(.)]"###,
        input: r###"[]"###,
        outputs: &[
            r###"[null,null,null,null]"###,
        ],
    },
    FixtureCase {
        query: r###".foo[.baz]"###,
        input: r###"{"foo":{"bar":4},"baz":"bar"}"###,
        outputs: &[
            r###"4"###,
        ],
    },
    FixtureCase {
        query: r###".[] | .error = "no, it's OK""###,
        input: r###"[{"error":true}]"###,
        outputs: &[
            r###"{"error": "no, it's OK"}"###,
        ],
    },
    FixtureCase {
        query: r###"[{a:1}] | .[] | .a=999"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a": 999}"###,
        ],
    },
    FixtureCase {
        query: r###"to_entries"###,
        input: r###"{"a": 1, "b": 2}"###,
        outputs: &[
            r###"[{"key":"a", "value":1}, {"key":"b", "value":2}]"###,
        ],
    },
    FixtureCase {
        query: r###"from_entries"###,
        input: r###"[{"key":"a", "value":1}, {"Key":"b", "Value":2}, {"name":"c", "value":3}, {"Name":"d", "Value":4}]"###,
        outputs: &[
            r###"{"a": 1, "b": 2, "c": 3, "d": 4}"###,
        ],
    },
    FixtureCase {
        query: r###"with_entries(.key |= "KEY_" + .)"###,
        input: r###"{"a": 1, "b": 2}"###,
        outputs: &[
            r###"{"KEY_a": 1, "KEY_b": 2}"###,
        ],
    },
    FixtureCase {
        query: r###"map(has("foo"))"###,
        input: r###"[{"foo": 42}, {}]"###,
        outputs: &[
            r###"[true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"map(has(2))"###,
        input: r###"[[0,1], ["a","b","c"]]"###,
        outputs: &[
            r###"[false, true]"###,
        ],
    },
    FixtureCase {
        query: r###"has(nan)"###,
        input: r###"[0,1,2]"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"keys"###,
        input: r###"[42,3,35]"###,
        outputs: &[
            r###"[0,1,2]"###,
        ],
    },
    FixtureCase {
        query: r###"[][.]"###,
        input: r###"1000000000000000000"###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"map([1,2][0:.])"###,
        input: r###"[-1, 1, 2, 3, 1000000000000000000]"###,
        outputs: &[
            r###"[[1], [1], [1,2], [1,2], [1,2]]"###,
        ],
    },
]
