&[
    FixtureCase {
        query: r###"{"k": {"a": 1, "b": 2}} * ."###,
        input: r###"{"k": {"a": 0,"c": 3}}"###,
        outputs: &[
            r###"{"k": {"a": 0, "b": 2, "c": 3}}"###,
        ],
    },
    FixtureCase {
        query: r###"{"k": {"a": 1, "b": 2}, "hello": {"x": 1}} * ."###,
        input: r###"{"k": {"a": 0,"c": 3}, "hello": 1}"###,
        outputs: &[
            r###"{"k": {"a": 0, "b": 2, "c": 3}, "hello": 1}"###,
        ],
    },
    FixtureCase {
        query: r###"{"k": {"a": 1, "b": 2}, "hello": 1} * ."###,
        input: r###"{"k": {"a": 0,"c": 3}, "hello": {"x": 1}}"###,
        outputs: &[
            r###"{"k": {"a": 0, "b": 2, "c": 3}, "hello": {"x": 1}}"###,
        ],
    },
    FixtureCase {
        query: r###"{"a": {"b": 1}, "c": {"d": 2}, "e": 5} * ."###,
        input: r###"{"a": {"b": 2}, "c": {"d": 3, "f": 9}}"###,
        outputs: &[
            r###"{"a": {"b": 2}, "c": {"d": 3, "f": 9}, "e": 5}"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|arrays]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[[],[3,[]]]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|objects]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[{}]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|iterables]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[[],[3,[]],{}]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|scalars]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[1,2,"foo",true,false,null]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|values]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[1,2,"foo",[],[3,[]],{},true,false]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|booleans]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[true,false]"###,
        ],
    },
    FixtureCase {
        query: r###"[.[]|nulls]"###,
        input: r###"[1,2,"foo",[],[3,[]],{},true,false,null]"###,
        outputs: &[
            r###"[null]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten"###,
        input: r###"[0, [1], [[2]], [[[3]]]]"###,
        outputs: &[
            r###"[0, 1, 2, 3]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten(0)"###,
        input: r###"[0, [1], [[2]], [[[3]]]]"###,
        outputs: &[
            r###"[0, [1], [[2]], [[[3]]]]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten(2)"###,
        input: r###"[0, [1], [[2]], [[[3]]]]"###,
        outputs: &[
            r###"[0, 1, 2, [3]]"###,
        ],
    },
    FixtureCase {
        query: r###"flatten(2)"###,
        input: r###"[0, [1, [2]], [1, [[3], 2]]]"###,
        outputs: &[
            r###"[0, 1, 2, 1, [3], 2]"###,
        ],
    },
    FixtureCase {
        query: r###"try flatten(-1) catch ."###,
        input: r###"[0, [1], [[2]], [[[3]]]]"###,
        outputs: &[
            r###""flatten depth must not be negative""###,
        ],
    },
    FixtureCase {
        query: r###"transpose"###,
        input: r###"[[1], [2,3]]"###,
        outputs: &[
            r###"[[1,2],[null,3]]"###,
        ],
    },
    FixtureCase {
        query: r###"transpose"###,
        input: r###"[]"###,
        outputs: &[
            r###"[]"###,
        ],
    },
    FixtureCase {
        query: r###"ascii_upcase"###,
        input: r###""useful but not for é""###,
        outputs: &[
            r###""USEFUL BUT NOT FOR é""###,
        ],
    },
    FixtureCase {
        query: r###"bsearch(0,1,2,3,4)"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"-1"###,
            r###"0"###,
            r###"1"###,
            r###"2"###,
            r###"-4"###,
        ],
    },
    FixtureCase {
        query: r###"bsearch({x:1})"###,
        input: r###"[{ "x": 0 },{ "x": 1 },{ "x": 2 }]"###,
        outputs: &[
            r###"1"###,
        ],
    },
    FixtureCase {
        query: r###"try ["OK", bsearch(0)] catch ["KO",.]"###,
        input: r###""aa""###,
        outputs: &[
            r###"["KO","string (\"aa\") cannot be searched from"]"###,
        ],
    },
    FixtureCase {
        query: r###"strftime("%Y-%m-%dT%H:%M:%SZ")"###,
        input: r###"[2015,2,5,23,51,47,4,63]"###,
        outputs: &[
            r###""2015-03-05T23:51:47Z""###,
        ],
    },
    FixtureCase {
        query: r###"strftime("%A, %B %d, %Y")"###,
        input: r###"1435677542.822351"###,
        outputs: &[
            r###""Tuesday, June 30, 2015""###,
        ],
    },
    FixtureCase {
        query: r###"strftime("%Y-%m-%dT%H:%M:%SZ")"###,
        input: r###"[2024,2,15]"###,
        outputs: &[
            r###""2024-03-15T00:00:00Z""###,
        ],
    },
    FixtureCase {
        query: r###"mktime"###,
        input: r###"[2024,8,21]"###,
        outputs: &[
            r###"1726876800"###,
        ],
    },
    FixtureCase {
        query: r###"gmtime"###,
        input: r###"1425599507"###,
        outputs: &[
            r###"[2015,2,5,23,51,47,4,63]"###,
        ],
    },
    FixtureCase {
        query: r###"gmtime[5]"###,
        input: r###"1425599507.25"###,
        outputs: &[
            r###"47.25"###,
        ],
    },
]
