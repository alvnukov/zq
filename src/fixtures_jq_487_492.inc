&[
    FixtureCase {
        query: r###""foo" | try ((try . catch "caught too much") | error) catch "caught just right""###,
        input: r###"null"###,
        outputs: &[
            r###""caught just right""###,
        ],
    },
    FixtureCase {
        query: r###".[]|(try (if .=="hi" then . else error end) catch empty) | "\(.) there!""###,
        input: r###"["hi","ho"]"###,
        outputs: &[
            r###""hi there!""###,
        ],
    },
    FixtureCase {
        query: r###"try (["hi","ho"]|.[]|(try . catch (if .=="ho" then "BROKEN"|error else empty end)) | if .=="ho" then error else "\(.) there!" end) catch "caught outside \(.)""###,
        input: r###"null"###,
        outputs: &[
            r###""hi there!""###,
            r###""caught outside ho""###,
        ],
    },
    FixtureCase {
        query: r###".[]|(try . catch (if .=="ho" then "BROKEN"|error else empty end)) | if .=="ho" then error else "\(.) there!" end"###,
        input: r###"["hi","ho"]"###,
        outputs: &[
            r###""hi there!""###,
        ],
    },
    FixtureCase {
        query: r###"try (try error catch "inner catch \(.)") catch "outer catch \(.)""###,
        input: r###""foo""###,
        outputs: &[
            r###""inner catch foo""###,
        ],
    },
    FixtureCase {
        query: r###"try ((try error catch "inner catch \(.)")|error) catch "outer catch \(.)""###,
        input: r###""foo""###,
        outputs: &[
            r###""outer catch inner catch foo""###,
        ],
    },
]
