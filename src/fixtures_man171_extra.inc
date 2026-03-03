&[
    FixtureCase {
        query: r###"[., tojson]"###,
        input: r###"12345678909876543212345"###,
        outputs: &[
            r###"[12345678909876543212345,"12345678909876543212345"]"###,
        ],
    },
    FixtureCase {
        query: r###"map([., . == 1]) | tojson"###,
        input: r###"[1, 1.000, 1.0, 100e-2]"###,
        outputs: &[
            r###""[[1,true],[1.000,true],[1.0,true],[1.00,true]]""###,
        ],
    },
    FixtureCase {
        query: r###". as $big | [$big, $big + 1] | map(. > 10000000000000000000000000000000)"###,
        input: r###"10000000000000000000000000000001"###,
        outputs: &[
            r###"[true, false]"###,
        ],
    },
    FixtureCase {
        query: r###"[limit(3;.[])]"###,
        input: r###"[0,1,2,3,4,5,6,7,8,9]"###,
        outputs: &[
            r###"[0,1,2]"###,
        ],
    },
    FixtureCase {
        query: r###"[first(range(.)), last(range(.)), nth(./2; range(.))]"###,
        input: r###"10"###,
        outputs: &[
            r###"[0,9,5]"###,
        ],
    },
    FixtureCase {
        query: r###"def range(init; upto; by): def _range: if (by > 0 and . < upto) or (by < 0 and . > upto) then ., ((.+by)|_range) else . end; if by == 0 then init else init|_range end | select((by > 0 and . < upto) or (by < 0 and . > upto)); range(0; 10; 3)"###,
        input: r###"null"###,
        outputs: &[
            r###"0"###,
            r###"3"###,
            r###"6"###,
            r###"9"###,
        ],
    },
    FixtureCase {
        query: r###"truncate_stream([[0],1],[[1,0],2],[[1,0]],[[1]])"###,
        input: r###"1"###,
        outputs: &[
            r###"[[0],2]"###,
            r###"[[0]]"###,
        ],
    },
    FixtureCase {
        query: r###"fromstream(1|truncate_stream([[0],1],[[1,0],2],[[1,0]],[[1]]))"###,
        input: r###"null"###,
        outputs: &[
            r###"[2]"###,
        ],
    },
]
