&[
    FixtureCase {
        query: r###"[contains(""), contains("\u0000")]"###,
        input: r###""\u0000""###,
        outputs: &[
            r###"[true, true]"###,
        ],
    },
    FixtureCase {
        query: r###"[contains(""), contains("a"), contains("ab"), contains("c"), contains("d")]"###,
        input: r###""ab\u0000cd""###,
        outputs: &[
            r###"[true, true, true, true, true]"###,
        ],
    },
    FixtureCase {
        query: r###"[contains("cd"), contains("b\u0000"), contains("ab\u0000")]"###,
        input: r###""ab\u0000cd""###,
        outputs: &[
            r###"[true, true, true]"###,
        ],
    },
    FixtureCase {
        query: r###"[contains("b\u0000c"), contains("b\u0000cd"), contains("b\u0000cd")]"###,
        input: r###""ab\u0000cd""###,
        outputs: &[
            r###"[true, true, true]"###,
        ],
    },
    FixtureCase {
        query: r###"[contains("@"), contains("\u0000@"), contains("\u0000what")]"###,
        input: r###""ab\u0000cd""###,
        outputs: &[
            r###"[false, false, false]"###,
        ],
    },
]
