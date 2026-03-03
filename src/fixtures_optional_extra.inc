&[
    FixtureCase {
        query: r###"fromdate"###,
        input: r###""2038-01-19T03:14:08Z""###,
        outputs: &[
            r###"2147483648"###,
        ],
    },
    FixtureCase {
        query: r###"strftime("%A, %B %e, %Y")"###,
        input: r###"1435677542.822351"###,
        outputs: &[
            r###""Tuesday, June 30, 2015""###,
        ],
    },
]
