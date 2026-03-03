&[
    FixtureCase {
        query: r###"{x: -1}"###,
        input: r###"null"###,
        outputs: &[
            r###"{"x": -1}"###,
        ],
    },
    FixtureCase {
        query: r###"@text,@json,([1,.]|@csv,@tsv),@html,@uri,@sh,(@base64|.,@base64d)"###,
        input: r###""!()<>&'\"\t""###,
        outputs: &[
            r###""!()<>&'\"\t""###,
            r###""\"!()<>&'\\\"\\t\"""###,
            r###""1,\"!()<>&'\"\"\t\"""###,
            r###""1\t!()<>&'\"\\t""###,
            r###""!()&lt;&gt;&amp;&apos;&quot;\t""###,
            r###""%21%28%29%3C%3E%26%27%22%09""###,
            r###""'!()<>&'\\''\"\t'""###,
            r###""ISgpPD4mJyIJ""###,
            r###""!()<>&'\"\t""###,
        ],
    },
    FixtureCase {
        query: r###"bsearch(0,2,4)"###,
        input: r###"[1,2,3]"###,
        outputs: &[
            r###"-1"###,
            r###"1"###,
            r###"-4"###,
        ],
    },
    FixtureCase {
        query: r###"try -. catch ."###,
        input: r###""very-long-string""###,
        outputs: &[
            r###""string (\"very-long-...) cannot be negated""###,
        ],
    },
    FixtureCase {
        query: r###".[0] | tostring"###,
        input: r###"[13911860366432393]"###,
        outputs: &[
            r###""13911860366432393""###,
        ],
    },
    FixtureCase {
        query: r###".x | tojson"###,
        input: r###"{"x":13911860366432393}"###,
        outputs: &[
            r###""13911860366432393""###,
        ],
    },
    FixtureCase {
        query: r###"13911860366432393 == 13911860366432392"###,
        input: r###"null"###,
        outputs: &[
            r###"false"###,
        ],
    },
    FixtureCase {
        query: r###"map(fabs == length) | unique"###,
        input: r###"[-10, -1.1, -1e-1, 1000000000000000002]"###,
        outputs: &[
            r###"[true]"###,
        ],
    },
    FixtureCase {
        query: r###"fromjson | isnan"###,
        input: r###""nan1234""###,
        outputs: &[
            r###"true"###,
        ],
    },
    FixtureCase {
        query: r###".[] |= try tonumber"###,
        input: r###"["1", "2a", "3", 4]"###,
        outputs: &[
            r###"[1, 3, 4]"###,
        ],
    },
]
