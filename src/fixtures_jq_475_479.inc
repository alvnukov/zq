&[
    FixtureCase {
        query: r###"123 as $label | $label"###,
        input: r###"null"###,
        outputs: &[
            r###"123"###,
        ],
    },
    FixtureCase {
        query: r###"[ label $if | range(10) | ., (select(. == 5) | break $if) ]"###,
        input: r###"null"###,
        outputs: &[
            r###"[0,1,2,3,4,5]"###,
        ],
    },
    FixtureCase {
        query: r###"reduce .[] as $then (4 as $else | $else; . as $elif | . + $then * $elif)"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"96"###,
        ],
    },
    FixtureCase {
        query: r###"1 as $foreach | 2 as $and | 3 as $or | { $foreach, $and, $or, a }"###,
        input: r###"{"a":4,"b":5}"###,
        outputs: &[
            r###"{"foreach":1,"and":2,"or":3,"a":4}"###,
        ],
    },
    FixtureCase {
        query: r###"[ foreach .[] as $try (1 as $catch | $catch - 1; . + $try; .) ]"###,
        input: r###"[10,9,8,7]"###,
        outputs: &[
            r###"[10,19,27,34]"###,
        ],
    },
]
