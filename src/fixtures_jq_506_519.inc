&[
    FixtureCase {
        query: r###"[range(10)] | .[1.2:3.5]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(10)] | .[1.5:3.5]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(10)] | .[1.7:3.5]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(10)] | .[1.7:4294967295]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2,3,4,5,6,7,8,9]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(10)] | .[1.7:-4294967296]"###,
        input: r###"null"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"[[range(10)] | .[1.1,1.5,1.7]]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,1,1]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(5)] | .[1.1] = 5"###,
        input: r###"null"###,
        outputs: &[
            r###"[0,5,2,3,4]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(3)] | .[nan:1]"###,
        input: r###"null"###,
        outputs: &[
            r###"[0]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(3)] | .[1:nan]"###,
        input: r###"null"###,
        outputs: &[
            r###"[1,2]"###,
        ],
    },
    FixtureCase {
        query: r###"[range(3)] | .[nan]"###,
        input: r###"null"###,
        outputs: &[
            r###"null"###,
        ],
    },
    FixtureCase {
        query: r###"try ([range(3)] | .[nan] = 9) catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""Cannot set array element at NaN index""###,
        ],
    },
    FixtureCase {
        query: r###"try ("foobar" | .[1.5:3.5] = "xyz") catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""Cannot update string slices""###,
        ],
    },
    FixtureCase {
        query: r###"try ([range(10)] | .[1.5:3.5] = ["xyz"]) catch ."###,
        input: r###"null"###,
        outputs: &[
            r###"[0,"xyz",4,5,6,7,8,9]"###,
        ],
    },
    FixtureCase {
        query: r###"try ("foobar" | .[1.5]) catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""Cannot index string with number (1.5)""###,
        ],
    },
]
