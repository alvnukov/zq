&[
    FixtureCase {
        query: r###"[match("( )*"; "g")]"###,
        input: r###""abc""###,
        outputs: &[
            r###"[{"offset":0,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},{"offset":1,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},{"offset":2,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]},{"offset":3,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":null}]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match("( )*"; "gn")]"###,
        input: r###""abc""###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[match(""; "g")]"###,
        input: r###""ab""###,
        outputs: &[
            r###"[{"offset":0,"length":0,"string":"","captures":[]},{"offset":1,"length":0,"string":"","captures":[]},{"offset":2,"length":0,"string":"","captures":[]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match("a"; "gi")]"###,
        input: r###""āáàä""###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[match(["(bar)"])]"###,
        input: r###""foo bar""###,
        outputs: &[
            r###"[{"offset": 4, "length": 3, "string": "bar", "captures":[{"offset": 4, "length": 3, "string": "bar", "name": null}]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match("bar")]"###,
        input: r###""ā bar with a combining codepoint U+0304""###,
        outputs: &[
            r###"[{"offset": 3, "length": 3, "string": "bar", "captures":[]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match("bār")]"###,
        input: r###""a bār""###,
        outputs: &[
            r###"[{"offset": 2, "length": 4, "string": "bār", "captures":[]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match(".+?\\b")]"###,
        input: r###""ā two-codepoint grapheme""###,
        outputs: &[
            r###"[{"offset": 0, "length": 2, "string": "ā", "captures":[]}]"###,
        ],
    },
    FixtureCase {
        query: r###"[match(["foo (?<bar123>bar)? foo", "ig"])]"###,
        input: r###""foo bar foo foo  foo""###,
        outputs: &[
            r###"[{"offset": 0, "length": 11, "string": "foo bar foo", "captures":[{"offset": 4, "length": 3, "string": "bar", "name": "bar123"}]},{"offset":12, "length": 8, "string": "foo  foo", "captures":[{"offset": -1, "length": 0, "string": null, "name": "bar123"}]}]"###,
        ],
    },
    FixtureCase {
        query: r###""a","b","c" | capture("(?<x>a)?b?")"###,
        input: r###"null"###,
        outputs: &[
            r###"{"x":"a"}"###,
            r###"{"x":null}"###,
            r###"{"x":null}"###,
        ],
    },
    FixtureCase {
        query: r###""a","b","c" | match("(?<x>a)?b?")"###,
        input: r###"null"###,
        outputs: &[
            r###"{"offset":0,"length":1,"string":"a","captures":[{"offset":0,"length":1,"string":"a","name":"x"}]}"###,
            r###"{"offset":0,"length":1,"string":"b","captures":[{"offset":-1,"string":null,"length":0,"name":"x"}]}"###,
            r###"{"offset":0,"length":0,"string":"","captures":[{"offset":-1,"string":null,"length":0,"name":"x"}]}"###,
        ],
    },
    FixtureCase {
        query: r###""a","b","c" | capture("(?<x>a?)?b?")"###,
        input: r###"null"###,
        outputs: &[
            r###"{"x":"a"}"###,
            r###"{"x":""}"###,
            r###"{"x":""}"###,
        ],
    },
    FixtureCase {
        query: r###""a","b","c" | match("(?<x>a?)?b?")"###,
        input: r###"null"###,
        outputs: &[
            r###"{"offset":0,"length":1,"string":"a","captures":[{"offset":0,"length":1,"string":"a","name":"x"}]}"###,
            r###"{"offset":0,"length":1,"string":"b","captures":[{"offset":0,"string":"","length":0,"name":"x"}]}"###,
            r###"{"offset":0,"length":0,"string":"","captures":[{"offset":0,"string":"","length":0,"name":"x"}]}"###,
        ],
    },
    FixtureCase {
        query: r###"[test("( )*"; "gn")]"###,
        input: r###""abc""###,
        outputs: &[
            r###"[false]"###,
        ],
    },
    FixtureCase {
        query: r###"[test("ā")]"###,
        input: r###""ā""###,
        outputs: &[
            r###"[true]"###,
        ],
    },
    FixtureCase {
        query: r###"capture("(?<a>[a-z]+)-(?<n>[0-9]+)")"###,
        input: r###""xyzzy-14""###,
        outputs: &[
            r###"{"a":"xyzzy","n":"14"}"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | sub(", "; ":")]"###,
        input: r###"["a,b, c, d, e,f", ", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"["a,b:c, d, e,f",":a,b, c, d, e,f, "]"###,
        ],
    },
    FixtureCase {
        query: r###"sub("^(?<head>.)"; "Head=\(.head) Tail=")"###,
        input: r###""abcdef""###,
        outputs: &[
            r###""Head=a Tail=bcdef""###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | gsub(", "; ":")]"###,
        input: r###"["a,b, c, d, e,f",", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"["a,b:c:d:e,f",":a,b:c:d:e,f:"]"###,
        ],
    },
    FixtureCase {
        query: r###"gsub("(?<d>\\d)"; ":\(.d);")"###,
        input: r###""a1b2""###,
        outputs: &[
            r###""a:1;b:2;""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("a";"b")"###,
        input: r###""aaaaa""###,
        outputs: &[
            r###""bbbbb""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("(.*)"; ""; "x")"###,
        input: r###""""###,
        outputs: &[
            r###""""###,
        ],
    },
    FixtureCase {
        query: r###"gsub(""; "a"; "g")"###,
        input: r###""""###,
        outputs: &[
            r###""a""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("^"; ""; "g")"###,
        input: r###""a""###,
        outputs: &[
            r###""a""###,
        ],
    },
    FixtureCase {
        query: r###"gsub(""; "a"; "g")"###,
        input: r###""a""###,
        outputs: &[
            r###""aaa""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("$"; "a"; "g")"###,
        input: r###""a""###,
        outputs: &[
            r###""aa""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("^"; "a")"###,
        input: r###""""###,
        outputs: &[
            r###""a""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("(?=u)"; "u")"###,
        input: r###""qux""###,
        outputs: &[
            r###""quux""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("^.*a"; "b")"###,
        input: r###""aaa""###,
        outputs: &[
            r###""b""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("^.*?a"; "b")"###,
        input: r###""aaa""###,
        outputs: &[
            r###""baa""###,
        ],
    },
    FixtureCase {
        query: r###"[gsub("a"; "b", "c")]"###,
        input: r###""a""###,
        outputs: &[
            r###"["b","c"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | scan(", ")]"###,
        input: r###"["a,b, c, d, e,f",", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"[", ",", ",", ",", ",", ",", ",", ",", "]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|[[sub(", *";":")], [gsub(", *";":")], [scan(", *")]]]"###,
        input: r###"["a,b, c, d, e,f",", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"[[["a:b, c, d, e,f"],["a:b:c:d:e:f"],[",",", ",", ",", ",","]],[[":a,b, c, d, e,f, "],[":a:b:c:d:e:f:"],[", ",",",", ",", ",", ",",",", "]]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|[[sub(", +";":")], [gsub(", +";":")], [scan(", +")]]]"###,
        input: r###"["a,b, c, d, e,f",", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"[[["a,b:c, d, e,f"],["a,b:c:d:e,f"],[", ",", ",", "]],[[":a,b, c, d, e,f, "],[":a,b:c:d:e,f:"],[", ",", ",", ",", ",", "]]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | scan("b+"; "i")]"###,
        input: r###"["","bBb","abcABBBCabbbc"]"###,
        outputs: &[
            r###"["bBb","b","BBB","bbb"]"###,
        ],
    },
    FixtureCase {
        query: r###"gsub("(?<x>.)[^a]*"; "+\(.x)-")"###,
        input: r###""Abcabc""###,
        outputs: &[
            r###""+A-+a-""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("(?<x>.)(?<y>[0-9])"; "\(.x|ascii_downcase)\(.y)")"###,
        input: r###""A1 B2 CD""###,
        outputs: &[
            r###""a1 b2 CD""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("\\b(?<x>.)"; "\(.x|ascii_downcase)")"###,
        input: r###""ABC DEF""###,
        outputs: &[
            r###""aBC dEF""###,
        ],
    },
    FixtureCase {
        query: r###"gsub("[^a-z]*(?<x>[a-z]*)"; "Z\(.x)")"###,
        input: r###""123foo456bar""###,
        outputs: &[
            r###""ZfooZbarZ""###,
        ],
    },
    FixtureCase {
        query: r###"sub("(?<x>.)"; "\(.x)!")"###,
        input: r###""’""###,
        outputs: &[
            r###""’!""###,
        ],
    },
    FixtureCase {
        query: r###"[sub("a"; "b", "c")]"###,
        input: r###""a""###,
        outputs: &[
            r###"["b","c"]"###,
        ],
    },
    FixtureCase {
        query: r###"[sub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)", "c")]"###,
        input: r###""aB""###,
        outputs: &[
            r###"["AB","aB","cB"]"###,
        ],
    },
    FixtureCase {
        query: r###"[gsub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)", "c")]"###,
        input: r###""aB""###,
        outputs: &[
            r###"["AB","ab","cc"]"###,
        ],
    },
    FixtureCase {
        query: r###"[splits("")]"###,
        input: r###""ab""###,
        outputs: &[
            r###"["","a","b",""]"###,
        ],
    },
    FixtureCase {
        query: r###"[splits("c")]"###,
        input: r###""ab""###,
        outputs: &[
            r###"["ab"]"###,
        ],
    },
    FixtureCase {
        query: r###"[splits("a+"; "i")]"###,
        input: r###""abAABBabA""###,
        outputs: &[
            r###"["","b","BB","b",""]"###,
        ],
    },
    FixtureCase {
        query: r###"[splits("b+"; "i")]"###,
        input: r###""abAABBabA""###,
        outputs: &[
            r###"["a","AA","a","A"]"###,
        ],
    },
]
