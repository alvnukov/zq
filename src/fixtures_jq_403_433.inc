&[
    FixtureCase {
        query: r###"import "data" as $a; import "data" as $b; def f: {$a, $b}; f"###,
        input: r###"null"###,
        outputs: &[
            r###"{"a":[{"this":"is a test","that":"is too"}],"b":[{"this":"is a test","that":"is too"}]}"###,
        ],
    },
    FixtureCase {
        query: r###"include "shadow1"; e"###,
        input: r###"null"###,
        outputs: &[
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###"include "shadow1"; include "shadow2"; e"###,
        input: r###"null"###,
        outputs: &[
            r###"3"###,
        ],
    },
    FixtureCase {
        query: r###"import "shadow1" as f; import "shadow2" as f; import "shadow1" as e; [e::e, f::e]"###,
        input: r###"null"###,
        outputs: &[
            r###"[2,3]"###,
        ],
    },
    FixtureCase {
        query: r###"modulemeta"###,
        input: r###""c""###,
        outputs: &[
            r###"{"whatever":null,"deps":[{"as":"foo","is_data":false,"relpath":"a"},{"search":"./","as":"d","is_data":false,"relpath":"d"},{"search":"./","as":"d2","is_data":false,"relpath":"d"},{"search":"./../lib/jq","as":"e","is_data":false,"relpath":"e"},{"search":"./../lib/jq","as":"f","is_data":false,"relpath":"f"},{"as":"d","is_data":true,"relpath":"data"}],"defs":["a/0","c/0"]}"###,
        ],
    },
    FixtureCase {
        query: r###"modulemeta | .deps | length"###,
        input: r###""c""###,
        outputs: &[
            r###"6"###,
        ],
    },
    FixtureCase {
        query: r###"modulemeta | .defs | length"###,
        input: r###""c""###,
        outputs: &[
            r###"2"###,
        ],
    },
    FixtureCase {
        query: r###"import "test_bind_order" as check; check::check"###,
        input: r###"null"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"try -. catch ."###,
        input: r###""very-long-long-long-long-string""###,
        outputs: &[
            r###""string (\"very-long-long-long-long...\") cannot be negated""###,
        ],
    },
    FixtureCase {
        query: r###"try (.-.) catch ."###,
        input: r###""very-long-long-long-long-string""###,
        outputs: &[
            r###""string (\"very-long-long-long-long...\") and string (\"very-long-long-long-long...\") cannot be subtracted""###,
        ],
    },
    FixtureCase {
        query: r###""x" * range(0; 12; 2) + "☆" * 8 | try -. catch ."###,
        input: r###"null"###,
        outputs: &[
            r###""string (\"☆☆☆☆☆☆☆☆\") cannot be negated""###,
            r###""string (\"xx☆☆☆☆☆☆☆☆\") cannot be negated""###,
            r###""string (\"xxxx☆☆☆☆☆☆...\") cannot be negated""###,
            r###""string (\"xxxxxx☆☆☆☆☆☆...\") cannot be negated""###,
            r###""string (\"xxxxxxxx☆☆☆☆☆...\") cannot be negated""###,
            r###""string (\"xxxxxxxxxx☆☆☆☆...\") cannot be negated""###,
        ],
    },
    FixtureCase {
        query: r###"try (. + "x") catch . == if have_decnum then "number (12345678901234567890123456...) and string (\"x\") cannot be added" else "number (12345678901234568000000000...) and string (\"x\") cannot be added" end"###,
        input: r###"123456789012345678901234567890"###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###"join(",")"###,
        input: r###"["1",2,true,false,3.4]"###,
        outputs: &[
            r###""1,2,true,false,3.4""###,
        ],
    },
    FixtureCase {
        query: r###".[] | join(",")"###,
        input: r###"[[], [null], [null,null], [null,null,null]]"###,
        outputs: &[
            r###""""###,
            r###""""###,
            r###"",""###,
            r###"",,""###,
        ],
    },
    FixtureCase {
        query: r###".[] | join(",")"###,
        input: r###"[["a",null], [null,"a"]]"###,
        outputs: &[
            r###""a,""###,
            r###"",a""###,
        ],
    },
    FixtureCase {
        query: r###"try join(",") catch ."###,
        input: r###"["1","2",{"a":{"b":{"c":33}}}]"###,
        outputs: &[
            r###""string (\"1,2,\") and object ({\"a\":{\"b\":{\"c\":33}}}) cannot be added""###,
        ],
    },
    FixtureCase {
        query: r###"try join(",") catch ."###,
        input: r###"["1","2",[3,4,5]]"###,
        outputs: &[
            r###""string (\"1,2,\") and array ([3,4,5]) cannot be added""###,
        ],
    },
    FixtureCase {
        query: r###"{if:0,and:1,or:2,then:3,else:4,elif:5,end:6,as:7,def:8,reduce:9,foreach:10,try:11,catch:12,label:13,import:14,include:15,module:16}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"if":0,"and":1,"or":2,"then":3,"else":4,"elif":5,"end":6,"as":7,"def":8,"reduce":9,"foreach":10,"try":11,"catch":12,"label":13,"import":14,"include":15,"module":16}"###,
        ],
    },
    FixtureCase {
        query: r###"try (1/.) catch ."###,
        input: r###"0"###,
        outputs: &[
            r###""number (1) and number (0) cannot be divided because the divisor is zero""###,
        ],
    },
    FixtureCase {
        query: r###"try (1/0) catch ."###,
        input: r###"0"###,
        outputs: &[
            r###""number (1) and number (0) cannot be divided because the divisor is zero""###,
        ],
    },
    FixtureCase {
        query: r###"try (0/0) catch ."###,
        input: r###"0"###,
        outputs: &[
            r###""number (0) and number (0) cannot be divided because the divisor is zero""###,
        ],
    },
    FixtureCase {
        query: r###"try (1%.) catch ."###,
        input: r###"0"###,
        outputs: &[
            r###""number (1) and number (0) cannot be divided (remainder) because the divisor is zero""###,
        ],
    },
    FixtureCase {
        query: r###"try (1%0) catch ."###,
        input: r###"0"###,
        outputs: &[
            r###""number (1) and number (0) cannot be divided (remainder) because the divisor is zero""###,
        ],
    },
]
