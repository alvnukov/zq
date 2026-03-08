&[
    FixtureCase {
        query: r###"[.[]|try if . == 0 then error("foo") elif . == 1 then .a elif . == 2 then empty else . end catch .]"###,
        input: r###"[0,1,2,3]"###,
        outputs: &[
            r###"["foo","Cannot index number with string \"a\"",3]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|(.a, .a)?]"###,
        input: r###"[null,true,{"a":1}]"###,
        outputs: &[
            r###"[null,null,1,1]"###,
        ],
    },
    FixtureCase {
        query: r###"[[.[]|[.a,.a]]?]"###,
        input: r###"[null,true,{"a":1}]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[if error then 1 else 2 end?]"###,
        input: r###""foo""###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"try error(0) // 1"###,
        input: r###"null"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"1, try error(2), 3"###,
        input: r###"null"###,
        outputs: &[
            r###"1"###,
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"1 + try 2 catch 3 + 4"###,
        input: r###"null"###,
        outputs: &[
            r###"7"###,
        ],
    },
    FixtureCase {
        query: r###"[-try .]"###,
        input: r###"1"###,
        outputs: &[
            r###"[-1]"###,
        ],
    },
    FixtureCase {
        query: r###"try -.? catch ."###,
        input: r###""foo""###,
        outputs: &[
            r###""string (\"foo\") cannot be negated""###,
        ],
    },
    FixtureCase {
        query: r###"{x: try 1, y: try error catch 2, z: if true then 3 end}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"x":1,"y":2,"z":3}"###,
        ],
    },
    FixtureCase {
        query: r###"{x: 1 + 2, y: false or true, z: null // 3}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"x":3,"y":true,"z":3}"###,
        ],
    },
    FixtureCase {
        query: r###".[] | try error catch ."###,
        input: r###"[1,null,2]"###,
        outputs: &[
            r###"1"###,
            r###"null"###,
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###"try error("\($__loc__)") catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""{\"file\":\"<top-level>\",\"line\":1}""###,
        ],
    },
]
