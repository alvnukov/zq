&[
    FixtureCase {
        query: r###"[.[]|startswith("foo")]"###,
        input: r###"["fo", "foo", "barfoo", "foobar", "barfoob"]"###,
        outputs: &[
            r###"[false, true, false, true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|endswith("foo")]"###,
        input: r###"["fo", "foo", "barfoo", "foobar", "barfoob"]"###,
        outputs: &[
            r###"[false, true, true, false, false]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[] | split(", ")]"###,
        input: r###"["a,b, c, d, e,f",", a,b, c, d, e,f, "]"###,
        outputs: &[
            r###"[["a,b","c","d","e,f"],["","a,b","c","d","e,f",""]]"###,
        ],
    },
    FixtureCase {
        query: r###"split("")"###,
        input: r###""abc""###,
        outputs: &[
            r###"["a","b","c"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|ltrimstr("foo")]"###,
        input: r###"["fo", "foo", "barfoo", "foobar", "afoo"]"###,
        outputs: &[
            r###"["fo","","barfoo","bar","afoo"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|rtrimstr("foo")]"###,
        input: r###"["fo", "foo", "barfoo", "foobar", "foob"]"###,
        outputs: &[
            r###"["fo","","bar","foobar","foob"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|trimstr("foo")]"###,
        input: r###"["fo", "foo", "barfoo", "foobarfoo", "foob"]"###,
        outputs: &[
            r###"["fo","","bar","bar","b"]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|ltrimstr("")]"###,
        input: r###"["a", "xx", ""]"###,
        outputs: &[
            r###"["a", "xx", ""]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|rtrimstr("")]"###,
        input: r###"["a", "xx", ""]"###,
        outputs: &[
            r###"["a", "xx", ""]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|trimstr("")]"###,
        input: r###"["a", "xx", ""]"###,
        outputs: &[
            r###"["a", "xx", ""]"###,
        ],
    },
    FixtureCase {
        query: r###"[(index(","), rindex(",")), indices(",")]"###,
        input: r###""a,bc,def,ghij,klmno""###,
        outputs: &[
            r###"[1,13,[1,4,8,13]]"###,
        ],
    },
    FixtureCase {
        query: r###"[ index("aba"), rindex("aba"), indices("aba") ]"###,
        input: r###""xababababax""###,
        outputs: &[
            r###"[1,7,[1,3,5,7]]"###,
        ],
    },
]
