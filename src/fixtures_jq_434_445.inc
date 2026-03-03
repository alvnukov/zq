&[
    FixtureCase {
        query: r###"[range(-52;52;1)] as $powers | [$powers[]|pow(2;.)|log2|round] == $powers"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"[range(-99/2;99/2;1)] as $orig | [$orig[]|pow(2;.)|log2] as $back | ($orig|keys)[]|. as $k | (($orig|.[$k])-($back|.[$k]))|if . < 0 then . * -1 else . end|select(.>.00005)"###,
        input: r###"null"###,
        outputs: &[
        ],
    },
    FixtureCase {
        query: r###"(.[{}] = 0)?"###,
        input: r###"null"###,
        outputs: &[
        ],
    },
    FixtureCase {
        query: r###"INDEX(range(5)|[., "foo\(.)"]; .[0])"###,
        input: r###"null"###,
        outputs: &[
            r###"{"0":[0,"foo0"],"1":[1,"foo1"],"2":[2,"foo2"],"3":[3,"foo3"],"4":[4,"foo4"]}"###,
        ],
    },
    FixtureCase {
        query: r###"JOIN({"0":[0,"abc"],"1":[1,"bcd"],"2":[2,"def"],"3":[3,"efg"],"4":[4,"fgh"]}; .[0]|tostring)"###,
        input: r###"[[5,"foo"],[3,"bar"],[1,"foobar"]]"###,
        outputs: &[
            r###"[[[5,"foo"],null],[[3,"bar"],[3,"efg"]],[[1,"foobar"],[1,"bcd"]]]"###,
        ],
    },
    FixtureCase {
        query: r###"range(5;10)|IN(range(10))"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
            r###"true"###,
            r###"true"###,
            r###"true"###,
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"range(5;13)|IN(range(0;10;3))"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
            r###"true"###,
            r###"false"###,
            r###"false"###,
            r###"true"###,
            r###"false"###,
            r###"false"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"range(10;12)|IN(range(10))"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"IN(range(10;20); range(10))"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"IN(range(5;20); range(10))"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
]
