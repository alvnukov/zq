&[
    FixtureCase {
        query: r###"split(", ")"###,
        input: r###""a, b,c,d, e, ""###,
        outputs: &[
            r###"["a","b,c,d","e",""]"###,
        ],
    },
    FixtureCase {
        query: r###"walk( if type == "object" then with_entries( .key |= sub( "^_+"; "") ) else . end )"###,
        input: r###"[ { "_a": { "__b": 2 } } ]"###,
        outputs: &[
            r###"[{"a":{"b":2}}]"###,
        ],
    },
    FixtureCase {
        query: r###"test("foo")"###,
        input: r###""foo""###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###".[] | test("a b c # spaces are ignored"; "ix")"###,
        input: r###"["xabcd", "ABC"]"###,
        outputs: &[
            r###"true"###,
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"match("(abc)+"; "g")"###,
        input: r###""abc abc""###,
        outputs: &[
            r###"{"offset": 0, "length": 3, "string": "abc", "captures": [{"offset": 0, "length": 3, "string": "abc", "name": null}]}"###,
            r###"{"offset": 4, "length": 3, "string": "abc", "captures": [{"offset": 4, "length": 3, "string": "abc", "name": null}]}"###,
        ],
    },
    FixtureCase {
        query: r###"match("foo")"###,
        input: r###""foo bar foo""###,
        outputs: &[
            r###"{"offset": 0, "length": 3, "string": "foo", "captures": []}"###,
        ],
    },
    FixtureCase {
        query: r###"match(["foo", "ig"])"###,
        input: r###""foo bar FOO""###,
        outputs: &[
            r###"{"offset": 0, "length": 3, "string": "foo", "captures": []}"###,
            r###"{"offset": 8, "length": 3, "string": "FOO", "captures": []}"###,
        ],
    },
    FixtureCase {
        query: r###"match("foo (?<bar123>bar)? foo"; "ig")"###,
        input: r###""foo bar foo foo  foo""###,
        outputs: &[
            r###"{"offset": 0, "length": 11, "string": "foo bar foo", "captures": [{"offset": 4, "length": 3, "string": "bar", "name": "bar123"}]}"###,
            r###"{"offset": 12, "length": 8, "string": "foo  foo", "captures": [{"offset": -1, "length": 0, "string": null, "name": "bar123"}]}"###,
        ],
    },
    FixtureCase {
        query: r###"[ match("."; "g")] | length"###,
        input: r###""abc""###,
        outputs: &[
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"capture("(?<a>[a-z]+)-(?<n>[0-9]+)")"###,
        input: r###""xyzzy-14""###,
        outputs: &[
            r###"{ "a": "xyzzy", "n": "14" }"###,
        ],
    },
    FixtureCase {
        query: r###"scan("c")"###,
        input: r###""abcdefabc""###,
        outputs: &[
            r###""c""###,
            r###""c""###,
        ],
    },
    FixtureCase {
        query: r###"scan("(a+)(b+)")"###,
        input: r###""abaabbaaabbb""###,
        outputs: &[
            r###"["a","b"]"###,
            r###"["aa","bb"]"###,
            r###"["aaa","bbb"]"###,
        ],
    },
    FixtureCase {
        query: r###"split(", *"; null)"###,
        input: r###""ab,cd, ef""###,
        outputs: &[
            r###"["ab","cd","ef"]"###,
        ],
    },
    FixtureCase {
        query: r###"splits(", *")"###,
        input: r###""ab,cd,   ef, gh""###,
        outputs: &[
            r###""ab""###,
            r###""cd""###,
            r###""ef""###,
            r###""gh""###,
        ],
    },
    FixtureCase {
        query: r###"splits(",? *"; "n")"###,
        input: r###""ab,cd ef,  gh""###,
        outputs: &[
            r###""ab""###,
            r###""cd""###,
            r###""ef""###,
            r###""gh""###,
        ],
    },
    FixtureCase {
        query: r###"sub("[^a-z]*(?<x>[a-z]+)"; "Z\(.x)"; "g")"###,
        input: r###""123abc456def""###,
        outputs: &[
            r###""ZabcZdef""###,
        ],
    },
    FixtureCase {
        query: r###"[sub("(?<a>.)"; "\(.a|ascii_upcase)", "\(.a|ascii_downcase)")]"###,
        input: r###""aB""###,
        outputs: &[
            r###"["AB","aB"]"###,
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
        query: r###"[gsub("p"; "a", "b")]"###,
        input: r###""p""###,
        outputs: &[
            r###"["a","b"]"###,
        ],
    },
]
