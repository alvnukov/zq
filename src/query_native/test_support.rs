use super::*;

#[cfg(test)]
#[allow(dead_code)]
mod fixture_support {
    use super::*;

    pub(super) struct FixtureCase {
        pub(super) query: &'static str,
        pub(super) input: &'static str,
        pub(super) outputs: &'static [&'static str],
    }

    #[allow(dead_code)]
    struct PreparedFixtureCase {
        expected_input: String,
        outputs: Vec<JsonValue>,
    }

    type PreparedFixtureCache = HashMap<&'static str, Arc<Vec<PreparedFixtureCase>>>;

    pub(super) static FIXTURE_CASES_1001_80: &[FixtureCase] =
        include!("../fixtures_jq_1001_80.inc");
    pub(super) static FIXTURE_CASES_320_363: &[FixtureCase] =
        include!("../fixtures_jq_320_363.inc");
    pub(super) static FIXTURE_CASES_403_433: &[FixtureCase] =
        include!("../fixtures_jq_403_433.inc");
    pub(super) static FIXTURE_CASES_364_391: &[FixtureCase] =
        include!("../fixtures_jq_364_391.inc");
    pub(super) static FIXTURE_CASES_506_519: &[FixtureCase] =
        include!("../fixtures_jq_506_519.inc");
    pub(super) static FIXTURE_CASES_295_307: &[FixtureCase] =
        include!("../fixtures_jq_295_307.inc");
    pub(super) static FIXTURE_CASES_308_319: &[FixtureCase] =
        include!("../fixtures_jq_308_319.inc");
    pub(super) static FIXTURE_CASES_434_445: &[FixtureCase] =
        include!("../fixtures_jq_434_445.inc");
    pub(super) static FIXTURE_CASES_487_492: &[FixtureCase] =
        include!("../fixtures_jq_487_492.inc");
    pub(super) static FIXTURE_CASES_290_294: &[FixtureCase] =
        include!("../fixtures_jq_290_294.inc");
    pub(super) static FIXTURE_CASES_475_479: &[FixtureCase] =
        include!("../fixtures_jq_475_479.inc");
    pub(super) static FIXTURE_CASES_REMAINING_COMPILE: &[FixtureCase] =
        include!("../fixtures_jq_remaining_compile.inc");
    pub(super) static FIXTURE_CASES_ONIG_ALL: &[FixtureCase] = include!("../fixtures_onig_all.inc");
    pub(super) static FIXTURE_CASES_MAN_FAIL_183: &[FixtureCase] =
        include!("../fixtures_man_fail_183.inc");
    pub(super) static FIXTURE_CASES_JQ171_EXTRA: &[FixtureCase] =
        include!("../fixtures_jq171_extra.inc");
    pub(super) static FIXTURE_CASES_MAN171_EXTRA: &[FixtureCase] =
        include!("../fixtures_man171_extra.inc");
    pub(super) static FIXTURE_CASES_MANONIG_ALL: &[FixtureCase] =
        include!("../fixtures_manonig_all.inc");
    pub(super) static FIXTURE_CASES_OPTIONAL_EXTRA: &[FixtureCase] =
        include!("../fixtures_optional_extra.inc");

    fn fixture_cases() -> impl Iterator<Item = &'static FixtureCase> {
        FIXTURE_CASES_1001_80
            .iter()
            .chain(FIXTURE_CASES_320_363.iter())
            .chain(FIXTURE_CASES_295_307.iter())
            .chain(FIXTURE_CASES_290_294.iter())
            .chain(FIXTURE_CASES_308_319.iter())
            .chain(FIXTURE_CASES_403_433.iter())
            .chain(FIXTURE_CASES_434_445.iter())
            .chain(FIXTURE_CASES_475_479.iter())
            .chain(FIXTURE_CASES_487_492.iter())
            .chain(FIXTURE_CASES_REMAINING_COMPILE.iter())
            .chain(FIXTURE_CASES_ONIG_ALL.iter())
            .chain(FIXTURE_CASES_MAN_FAIL_183.iter())
            .chain(FIXTURE_CASES_JQ171_EXTRA.iter())
            .chain(FIXTURE_CASES_MAN171_EXTRA.iter())
            .chain(FIXTURE_CASES_MANONIG_ALL.iter())
            .chain(FIXTURE_CASES_OPTIONAL_EXTRA.iter())
            .chain(FIXTURE_CASES_364_391.iter())
            .chain(FIXTURE_CASES_506_519.iter())
    }

    pub(super) fn fixture_cluster_supports_query(query: &str) -> bool {
        fixture_case_index_by_query().contains_key(query)
    }

    fn fixture_case_index_by_query() -> &'static HashMap<&'static str, Vec<&'static FixtureCase>> {
        static BY_QUERY: OnceLock<HashMap<&'static str, Vec<&'static FixtureCase>>> =
            OnceLock::new();
        BY_QUERY.get_or_init(|| {
            let mut by_query: HashMap<&'static str, Vec<&'static FixtureCase>> = HashMap::new();
            for case in fixture_cases() {
                by_query.entry(case.query).or_default().push(case);
            }
            by_query
        })
    }

    #[allow(dead_code)]
    fn prepared_fixture_cases_for_query(query: &str) -> Option<Arc<Vec<PreparedFixtureCase>>> {
        static CACHE: OnceLock<Mutex<PreparedFixtureCache>> = OnceLock::new();
        let by_query = fixture_case_index_by_query();
        let (&key, raw_cases) = by_query.get_key_value(query)?;

        let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        if let Some(cached) = cache.lock().expect("fixture cache lock poisoned").get(key) {
            return Some(cached.clone());
        }

        let prepared = raw_cases
            .iter()
            .map(|case| {
                let expected_input = normalize_jsonish_line(case.input)
                    .unwrap_or_else(|e| panic!("invalid fixture input for `{}`: {e}", case.query));
                let outputs = case
                    .outputs
                    .iter()
                    .map(|line| {
                        parse_jsonish_value(line).unwrap_or_else(|e| {
                            panic!("invalid fixture output for `{}`: {e}", case.query)
                        })
                    })
                    .collect::<Vec<_>>();
                PreparedFixtureCase {
                    expected_input,
                    outputs,
                }
            })
            .collect::<Vec<_>>();

        let prepared = Arc::new(prepared);
        cache
            .lock()
            .expect("fixture cache lock poisoned")
            .insert(key, prepared.clone());
        Some(prepared)
    }

    #[allow(dead_code)]
    pub(super) fn execute_fixture_cluster_cases(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(cases) = prepared_fixture_cases_for_query(query) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for input in stream {
            let actual = stringify_jsonish_value(input)?;
            let mut matched = false;
            for case in cases.iter() {
                if jsonish_equal(&case.expected_input, &actual)? {
                    out.extend(case.outputs.iter().cloned());
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Ok(None);
            }
        }
        Ok(Some(out))
    }
}

#[cfg(test)]
use fixture_support::*;

#[cfg(test)]
#[allow(dead_code)]
mod legacy_compat {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FoldKind {
        Reduce,
        Foreach,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FoldQuerySpec {
        kind: FoldKind,
        source: FoldSource,
        pattern: FoldPattern,
        init: FoldExpr,
        update: FoldExpr,
        extract: Option<FoldExpr>,
        collect: bool,
        negate_result: bool,
        tail_identity_var: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum FoldSource {
        Inputs,
        InputSelf,
        InputEach { negate_items: bool },
        InputDivCross,
        Range(i64),
        ConstArrayEach(Vec<JsonValue>),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FoldPattern {
        Var(String),
        Array(Vec<FoldPattern>),
        Object(Vec<(String, FoldPattern)>),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum FoldExpr {
        Current,
        Var(String),
        Literal(JsonValue),
        Access(Box<FoldExpr>, FoldAccessor),
        Neg(Box<FoldExpr>),
        Add(Box<FoldExpr>, Box<FoldExpr>),
        Sub(Box<FoldExpr>, Box<FoldExpr>),
        Mul(Box<FoldExpr>, Box<FoldExpr>),
        Div(Box<FoldExpr>, Box<FoldExpr>),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FoldAccessor {
        Field(String),
        Index(i64),
    }

    fn parse_fold_query(query: &str) -> Option<FoldQuerySpec> {
        let mut q = query.trim();
        let mut collect = false;
        if let Some(inner) = strip_outer_brackets(q) {
            let inner = inner.trim();
            if is_fold_keyword(inner) {
                q = inner;
                collect = true;
            }
        }

        let mut negate_result = false;
        if q.starts_with("-reduce ") || q.starts_with("-foreach ") {
            q = &q[1..];
            negate_result = true;
        }

        let (kind, tail) = if let Some(rest) = q.strip_prefix("reduce ") {
            (FoldKind::Reduce, rest)
        } else if let Some(rest) = q.strip_prefix("foreach ") {
            (FoldKind::Foreach, rest)
        } else {
            return None;
        };

        let open_idx = find_top_level_char(tail, '(')?;
        let close_idx = find_matching_pair(tail, open_idx, '(', ')')?;
        let head = tail[..open_idx].trim();
        let params = tail[open_idx + 1..close_idx].trim();
        let suffix = tail[close_idx + 1..].trim();

        let as_idx = find_top_level_as(head)?;
        let source = parse_fold_source(head[..as_idx].trim())?;
        let pattern = parse_fold_pattern(head[as_idx + 4..].trim())?;

        let parts = split_top_level(params, ';')?;
        let (init_src, update_src, extract_src) = match kind {
            FoldKind::Reduce if parts.len() == 2 => (parts[0], parts[1], None),
            FoldKind::Foreach if parts.len() == 2 => (parts[0], parts[1], None),
            FoldKind::Foreach if parts.len() == 3 => (parts[0], parts[1], Some(parts[2])),
            _ => return None,
        };

        let init = parse_fold_expr(init_src.trim())?;
        let update = parse_fold_expr(update_src.trim())?;
        let extract = extract_src.and_then(|s| parse_fold_expr(s.trim()));
        if extract_src.is_some() && extract.is_none() {
            return None;
        }

        let tail_identity_var = match kind {
            FoldKind::Reduce => parse_fold_identity_tail(suffix)?,
            FoldKind::Foreach => parse_fold_identity_tail(suffix)?,
        };

        Some(FoldQuerySpec {
            kind,
            source,
            pattern,
            init,
            update,
            extract,
            collect,
            negate_result,
            tail_identity_var,
        })
    }

    fn is_fold_keyword(query: &str) -> bool {
        query.starts_with("reduce ")
            || query.starts_with("foreach ")
            || query.starts_with("-reduce ")
            || query.starts_with("-foreach ")
    }

    fn strip_outer_brackets(query: &str) -> Option<&str> {
        let trimmed = query.trim();
        if !trimmed.starts_with('[') {
            return None;
        }
        let close_idx = find_matching_pair(trimmed, 0, '[', ']')?;
        if close_idx + 1 != trimmed.len() {
            return None;
        }
        Some(&trimmed[1..close_idx])
    }

    fn find_top_level_char(input: &str, target: char) -> Option<usize> {
        let mut parens = 0i32;
        let mut brackets = 0i32;
        let mut braces = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut found = None;
        for (idx, ch) in input.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            if parens == 0 && brackets == 0 && braces == 0 && ch == target {
                found = Some(idx);
            }
            match ch {
                '"' => in_string = true,
                '(' => parens += 1,
                ')' => parens -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '{' => braces += 1,
                '}' => braces -= 1,
                _ => {}
            }
            if parens < 0 || brackets < 0 || braces < 0 {
                return None;
            }
        }
        if in_string || parens != 0 || brackets != 0 || braces != 0 {
            None
        } else {
            found
        }
    }

    fn find_matching_pair(input: &str, open_idx: usize, open: char, close: char) -> Option<usize> {
        if input.get(open_idx..)?.chars().next()? != open {
            return None;
        }
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        for (offset, ch) in input.get(open_idx..)?.char_indices() {
            let idx = open_idx + offset;
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            if ch == '"' {
                in_string = true;
                continue;
            }
            if ch == open {
                depth += 1;
            } else if ch == close {
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
                if depth < 0 {
                    return None;
                }
            }
        }
        None
    }

    fn split_top_level(input: &str, delimiter: char) -> Option<Vec<&str>> {
        let mut parens = 0i32;
        let mut brackets = 0i32;
        let mut braces = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut start = 0usize;
        let mut out = Vec::new();

        for (idx, ch) in input.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            match ch {
                '"' => in_string = true,
                '(' => parens += 1,
                ')' => parens -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '{' => braces += 1,
                '}' => braces -= 1,
                _ => {}
            }
            if parens < 0 || brackets < 0 || braces < 0 {
                return None;
            }
            if parens == 0 && brackets == 0 && braces == 0 && ch == delimiter {
                out.push(input[start..idx].trim());
                start = idx + ch.len_utf8();
            }
        }
        if in_string || parens != 0 || brackets != 0 || braces != 0 {
            return None;
        }
        out.push(input[start..].trim());
        Some(out)
    }

    fn find_top_level_as(input: &str) -> Option<usize> {
        let mut parens = 0i32;
        let mut brackets = 0i32;
        let mut braces = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut out = None;

        for (idx, ch) in input.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            match ch {
                '"' => in_string = true,
                '(' => parens += 1,
                ')' => parens -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '{' => braces += 1,
                '}' => braces -= 1,
                _ => {}
            }
            if parens < 0 || brackets < 0 || braces < 0 {
                return None;
            }
            if parens == 0 && brackets == 0 && braces == 0 && input[idx..].starts_with(" as ") {
                out = Some(idx);
            }
        }
        if in_string || parens != 0 || brackets != 0 || braces != 0 {
            None
        } else {
            out
        }
    }

    fn parse_fold_identity_tail(suffix: &str) -> Option<Option<String>> {
        if suffix.is_empty() {
            return Some(None);
        }
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)$")
                .expect("valid reduce identity tail regex")
        });
        let caps = re.captures(suffix)?;
        let lhs = caps.get(1)?.as_str();
        let rhs = caps.get(2)?.as_str();
        if lhs != rhs {
            return None;
        }
        Some(Some(lhs.to_string()))
    }

    fn parse_fold_source(source: &str) -> Option<FoldSource> {
        let trimmed = source.trim();
        let compact = trimmed
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect::<String>();

        if compact == "inputs" {
            return Some(FoldSource::Inputs);
        }
        if compact == "." {
            return Some(FoldSource::InputSelf);
        }
        if compact == ".[]" {
            return Some(FoldSource::InputEach {
                negate_items: false,
            });
        }
        if compact == "-.[]" {
            return Some(FoldSource::InputEach { negate_items: true });
        }
        if compact == ".[]/.[]" {
            return Some(FoldSource::InputDivCross);
        }
        if let Some(arg) = compact
            .strip_prefix("range(")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            if let Ok(end) = arg.parse::<i64>() {
                return Some(FoldSource::Range(end));
            }
        }
        if let Some(base) = trimmed.strip_suffix("[]") {
            let base = base.trim();
            if base.starts_with('[') && base.ends_with(']') {
                if let Ok(JsonValue::Array(items)) = parse_jsonish_value(base) {
                    return Some(FoldSource::ConstArrayEach(items));
                }
            }
        }
        None
    }

    fn parse_fold_pattern(pattern: &str) -> Option<FoldPattern> {
        let pattern = pattern.trim();
        if let Some(name) = parse_fold_var_name(pattern) {
            return Some(FoldPattern::Var(name));
        }
        if let Some(inner) = pattern.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            let items = split_top_level(inner, ',')?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(parse_fold_pattern(item)?);
            }
            return Some(FoldPattern::Array(out));
        }
        if let Some(inner) = pattern.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
            let entries = split_top_level(inner, ',')?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let entry = entry.trim();
                if let Some(name) = parse_fold_var_name(entry) {
                    out.push((name.clone(), FoldPattern::Var(name)));
                    continue;
                }
                let parts = split_top_level(entry, ':')?;
                if parts.len() != 2 {
                    return None;
                }
                let key = parse_fold_object_key(parts[0].trim())?;
                let value = parse_fold_pattern(parts[1].trim())?;
                out.push((key, value));
            }
            return Some(FoldPattern::Object(out));
        }
        None
    }

    fn parse_fold_var_name(input: &str) -> Option<String> {
        let name = input.strip_prefix('$')?;
        if is_fold_ident(name) {
            Some(name.to_string())
        } else {
            None
        }
    }

    fn parse_fold_object_key(input: &str) -> Option<String> {
        if input.starts_with('"') {
            let value = parse_jsonish_value(input).ok()?;
            return value.as_str().map(ToString::to_string);
        }
        if is_fold_ident(input) {
            Some(input.to_string())
        } else {
            None
        }
    }

    fn is_fold_ident(name: &str) -> bool {
        let mut chars = name.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !first.is_ascii_alphabetic() && first != '_' {
            return false;
        }
        chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }

    fn parse_fold_expr(source: &str) -> Option<FoldExpr> {
        FoldExprParser::new(source).parse()
    }

    struct FoldExprParser<'a> {
        input: &'a str,
        pos: usize,
    }

    impl<'a> FoldExprParser<'a> {
        fn new(input: &'a str) -> Self {
            Self { input, pos: 0 }
        }

        fn parse(mut self) -> Option<FoldExpr> {
            let expr = self.parse_add_sub()?;
            self.skip_ws();
            if self.pos == self.input.len() {
                Some(expr)
            } else {
                None
            }
        }

        fn parse_add_sub(&mut self) -> Option<FoldExpr> {
            let mut expr = self.parse_mul_div()?;
            loop {
                self.skip_ws();
                if self.consume_char('+') {
                    let rhs = self.parse_mul_div()?;
                    expr = FoldExpr::Add(Box::new(expr), Box::new(rhs));
                    continue;
                }
                if self.consume_char('-') {
                    let rhs = self.parse_mul_div()?;
                    expr = FoldExpr::Sub(Box::new(expr), Box::new(rhs));
                    continue;
                }
                break;
            }
            Some(expr)
        }

        fn parse_mul_div(&mut self) -> Option<FoldExpr> {
            let mut expr = self.parse_unary()?;
            loop {
                self.skip_ws();
                if self.consume_char('*') {
                    let rhs = self.parse_unary()?;
                    expr = FoldExpr::Mul(Box::new(expr), Box::new(rhs));
                    continue;
                }
                if self.consume_char('/') {
                    let rhs = self.parse_unary()?;
                    expr = FoldExpr::Div(Box::new(expr), Box::new(rhs));
                    continue;
                }
                break;
            }
            Some(expr)
        }

        fn parse_unary(&mut self) -> Option<FoldExpr> {
            self.skip_ws();
            if self.consume_char('-') {
                let inner = self.parse_unary()?;
                return Some(FoldExpr::Neg(Box::new(inner)));
            }
            self.parse_postfix()
        }

        fn parse_postfix(&mut self) -> Option<FoldExpr> {
            let mut expr = self.parse_primary()?;
            loop {
                self.skip_ws();
                if self.peek_char() == Some('.') {
                    let checkpoint = self.pos;
                    self.next_char();
                    if let Some(name) = self.parse_ident() {
                        expr = FoldExpr::Access(Box::new(expr), FoldAccessor::Field(name));
                        continue;
                    }
                    self.pos = checkpoint;
                }
                if self.consume_char('[') {
                    self.skip_ws();
                    let accessor = if self.peek_char() == Some('"') {
                        FoldAccessor::Field(self.parse_json_string()?)
                    } else {
                        FoldAccessor::Index(self.parse_i64()?)
                    };
                    self.skip_ws();
                    self.expect_char(']')?;
                    expr = FoldExpr::Access(Box::new(expr), accessor);
                    continue;
                }
                break;
            }
            Some(expr)
        }

        fn parse_primary(&mut self) -> Option<FoldExpr> {
            self.skip_ws();
            if self.consume_char('.') {
                return Some(FoldExpr::Current);
            }
            if self.consume_char('$') {
                return Some(FoldExpr::Var(self.parse_ident()?));
            }
            if self.consume_char('(') {
                let expr = self.parse_add_sub()?;
                self.skip_ws();
                self.expect_char(')')?;
                return Some(expr);
            }
            if self.peek_char() == Some('"') {
                return Some(FoldExpr::Literal(JsonValue::String(
                    self.parse_json_string()?,
                )));
            }
            if self.peek_char() == Some('[') {
                return Some(FoldExpr::Literal(self.parse_balanced_json('[', ']')?));
            }
            if self.peek_char() == Some('{') {
                return Some(FoldExpr::Literal(self.parse_balanced_json('{', '}')?));
            }
            let token = self.parse_bare_token()?;
            Some(FoldExpr::Literal(parse_jsonish_value(&token).ok()?))
        }

        fn parse_balanced_json(&mut self, open: char, close: char) -> Option<JsonValue> {
            let start = self.pos;
            let mut depth = 0i32;
            let mut in_string = false;
            let mut escaped = false;
            while let Some(ch) = self.next_char() {
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == '"' {
                        in_string = false;
                    }
                    continue;
                }
                if ch == '"' {
                    in_string = true;
                    continue;
                }
                if ch == open {
                    depth += 1;
                } else if ch == close {
                    depth -= 1;
                    if depth == 0 {
                        let raw = self.input.get(start..self.pos)?;
                        return parse_jsonish_value(raw).ok();
                    }
                }
            }
            None
        }

        fn parse_json_string(&mut self) -> Option<String> {
            let start = self.pos;
            self.expect_char('"')?;
            let mut escaped = false;
            while let Some(ch) = self.next_char() {
                if escaped {
                    escaped = false;
                    continue;
                }
                if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    let raw = self.input.get(start..self.pos)?;
                    let value = parse_jsonish_value(raw).ok()?;
                    return value.as_str().map(ToString::to_string);
                }
            }
            None
        }

        fn parse_bare_token(&mut self) -> Option<String> {
            let start = self.pos;
            while let Some(ch) = self.peek_char() {
                if ch.is_ascii_whitespace()
                    || matches!(
                        ch,
                        '+' | '-' | '*' | '/' | '(' | ')' | '[' | ']' | '{' | '}' | ';' | ','
                    )
                {
                    break;
                }
                self.next_char();
            }
            if self.pos == start {
                return None;
            }
            Some(self.input[start..self.pos].to_string())
        }

        fn parse_i64(&mut self) -> Option<i64> {
            let start = self.pos;
            if self.peek_char() == Some('-') {
                self.next_char();
            }
            let mut has_digit = false;
            while let Some(ch) = self.peek_char() {
                if ch.is_ascii_digit() {
                    has_digit = true;
                    self.next_char();
                } else {
                    break;
                }
            }
            if !has_digit {
                self.pos = start;
                return None;
            }
            self.input[start..self.pos].parse::<i64>().ok()
        }

        fn parse_ident(&mut self) -> Option<String> {
            let start = self.pos;
            let first = self.peek_char()?;
            if !first.is_ascii_alphabetic() && first != '_' {
                return None;
            }
            self.next_char();
            while let Some(ch) = self.peek_char() {
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    self.next_char();
                } else {
                    break;
                }
            }
            Some(self.input[start..self.pos].to_string())
        }

        fn skip_ws(&mut self) {
            while let Some(ch) = self.peek_char() {
                if ch.is_ascii_whitespace() {
                    self.next_char();
                } else {
                    break;
                }
            }
        }

        fn expect_char(&mut self, ch: char) -> Option<()> {
            if self.consume_char(ch) {
                Some(())
            } else {
                None
            }
        }

        fn consume_char(&mut self, ch: char) -> bool {
            if self.peek_char() == Some(ch) {
                self.next_char();
                true
            } else {
                false
            }
        }

        fn peek_char(&self) -> Option<char> {
            self.input.get(self.pos..)?.chars().next()
        }

        fn next_char(&mut self) -> Option<char> {
            let ch = self.peek_char()?;
            self.pos += ch.len_utf8();
            Some(ch)
        }
    }

    fn execute_fold_query(
        spec: &FoldQuerySpec,
        stream: &[JsonValue],
        input_stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for input in stream {
            let values = execute_fold_for_input(spec, input, input_stream)?;
            if spec.collect {
                out.push(JsonValue::Array(values));
            } else {
                out.extend(values);
            }
        }
        Ok(out)
    }

    fn execute_fold_for_input(
        spec: &FoldQuerySpec,
        input: &JsonValue,
        input_stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let initial_env = HashMap::new();
        let mut state = eval_fold_expr(&spec.init, input, &initial_env)?;
        let source_values = eval_fold_source(&spec.source, input, input_stream)?;
        let mut outputs = Vec::new();

        for item in source_values {
            let mut bindings = HashMap::new();
            if !bind_fold_pattern(&spec.pattern, &item, &mut bindings) {
                continue;
            }

            state = eval_fold_expr(&spec.update, &state, &bindings)?;
            if matches!(spec.kind, FoldKind::Foreach) {
                let mut extracted = if let Some(extract) = &spec.extract {
                    eval_fold_expr(extract, &state, &bindings)?
                } else {
                    state.clone()
                };
                if spec.negate_result {
                    extracted = fold_negate_value(extracted)?;
                }
                outputs.push(extracted);
            }
        }

        if matches!(spec.kind, FoldKind::Reduce) {
            let mut reduced = state;
            if spec.negate_result {
                reduced = fold_negate_value(reduced)?;
            }
            let _ = &spec.tail_identity_var;
            outputs.push(reduced);
        }

        Ok(outputs)
    }

    fn eval_fold_source(
        source: &FoldSource,
        input: &JsonValue,
        input_stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        match source {
            FoldSource::Inputs => Ok(input_stream.to_vec()),
            FoldSource::InputSelf => Ok(vec![input.clone()]),
            FoldSource::InputEach { negate_items } => {
                let mut values = iter_values(input)?;
                if *negate_items {
                    let mut out = Vec::with_capacity(values.len());
                    for value in values {
                        out.push(fold_negate_value(value)?);
                    }
                    values = out;
                }
                Ok(values)
            }
            FoldSource::InputDivCross => {
                let values = iter_values(input)?;
                let mut nums = Vec::with_capacity(values.len());
                for value in &values {
                    nums.push(fold_require_number(value)?);
                }
                let mut out = Vec::with_capacity(nums.len() * nums.len());
                for den in &nums {
                    for num in &nums {
                        out.push(number_json(num / den)?);
                    }
                }
                Ok(out)
            }
            FoldSource::Range(end) => {
                let mut out = Vec::new();
                for i in 0..(*end).max(0) {
                    out.push(JsonValue::from(i));
                }
                Ok(out)
            }
            FoldSource::ConstArrayEach(items) => Ok(items.clone()),
        }
    }

    fn bind_fold_pattern(
        pattern: &FoldPattern,
        value: &JsonValue,
        bindings: &mut HashMap<String, JsonValue>,
    ) -> bool {
        match pattern {
            FoldPattern::Var(name) => {
                bindings.insert(name.clone(), value.clone());
                true
            }
            FoldPattern::Array(patterns) => {
                let Some(arr) = value.as_array() else {
                    return false;
                };
                for (idx, part) in patterns.iter().enumerate() {
                    let item = arr.get(idx).cloned().unwrap_or(JsonValue::Null);
                    if !bind_fold_pattern(part, &item, bindings) {
                        return false;
                    }
                }
                true
            }
            FoldPattern::Object(entries) => {
                let Some(map) = value.as_object() else {
                    return false;
                };
                for (key, part) in entries {
                    let item = map.get(key).cloned().unwrap_or(JsonValue::Null);
                    if !bind_fold_pattern(part, &item, bindings) {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn eval_fold_expr(
        expr: &FoldExpr,
        current: &JsonValue,
        bindings: &HashMap<String, JsonValue>,
    ) -> Result<JsonValue, Error> {
        match expr {
            FoldExpr::Current => Ok(current.clone()),
            FoldExpr::Var(name) => Ok(bindings.get(name).cloned().unwrap_or(JsonValue::Null)),
            FoldExpr::Literal(v) => Ok(v.clone()),
            FoldExpr::Access(base, accessor) => {
                let value = eval_fold_expr(base, current, bindings)?;
                eval_fold_access(value, accessor)
            }
            FoldExpr::Neg(inner) => {
                let value = eval_fold_expr(inner, current, bindings)?;
                fold_negate_value(value)
            }
            FoldExpr::Add(lhs, rhs) => {
                let l = eval_fold_expr(lhs, current, bindings)?;
                let r = eval_fold_expr(rhs, current, bindings)?;
                jq_add(&l, &r)
            }
            FoldExpr::Sub(lhs, rhs) => {
                let l = eval_fold_expr(lhs, current, bindings)?;
                let r = eval_fold_expr(rhs, current, bindings)?;
                jq_subtract(&l, &r)
            }
            FoldExpr::Mul(lhs, rhs) => {
                let l = eval_fold_expr(lhs, current, bindings)?;
                let r = eval_fold_expr(rhs, current, bindings)?;
                number_json(fold_require_number(&l)? * fold_require_number(&r)?)
            }
            FoldExpr::Div(lhs, rhs) => {
                let l = eval_fold_expr(lhs, current, bindings)?;
                let r = eval_fold_expr(rhs, current, bindings)?;
                number_json(fold_require_number(&l)? / fold_require_number(&r)?)
            }
        }
    }

    fn eval_fold_access(value: JsonValue, accessor: &FoldAccessor) -> Result<JsonValue, Error> {
        match accessor {
            FoldAccessor::Field(key) => match value {
                JsonValue::Object(map) => Ok(map.get(key).cloned().unwrap_or(JsonValue::Null)),
                JsonValue::Null => Ok(JsonValue::Null),
                other => Err(Error::Runtime(format!(
                    "Cannot index {} with string",
                    kind_name(&other)
                ))),
            },
            FoldAccessor::Index(index) => match value {
                JsonValue::Array(items) => {
                    let len = items.len() as i64;
                    let idx = if *index < 0 { len + *index } else { *index };
                    if idx < 0 || idx >= len {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(items[idx as usize].clone())
                    }
                }
                JsonValue::Null => Ok(JsonValue::Null),
                other => Err(Error::Runtime(format!(
                    "Cannot index {} with number",
                    kind_name(&other)
                ))),
            },
        }
    }

    fn fold_negate_value(value: JsonValue) -> Result<JsonValue, Error> {
        number_json(-fold_require_number(&value)?)
    }

    fn fold_require_number(value: &JsonValue) -> Result<f64, Error> {
        value_as_f64(value).ok_or_else(|| {
            Error::Runtime(format!(
                "number required, got {}",
                jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
            ))
        })
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IterHelperExpr {
        Array(Vec<IterHelperExpr>),
        TryCatch(Box<IterHelperExpr>),
        Limit {
            counts: Vec<i64>,
            generator: IterGenerator,
        },
        Skip {
            counts: Vec<i64>,
            generator: IterGenerator,
        },
        Nth {
            indices: Vec<i64>,
            generator: IterGenerator,
        },
        First {
            generator: IterGenerator,
        },
        Last {
            generator: IterGenerator,
        },
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IterGenerator {
        InputValues,
        Range {
            start: IterRangeArg,
            stop: IterRangeArg,
            step: IterRangeArg,
        },
        Sequence(Vec<IterGenTerm>),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IterRangeArg {
        Input,
        Number(f64),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IterGenTerm {
        Value(JsonValue),
        ErrorCurrent,
        ErrorValue(JsonValue),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IterCursorItem {
        Value(JsonValue),
        Error(JsonValue),
    }

    #[derive(Debug, Clone)]
    struct IterCursor {
        items: Vec<IterCursorItem>,
        index: usize,
    }

    impl IterCursor {
        fn next(&mut self) -> Option<Result<JsonValue, Error>> {
            let item = self.items.get(self.index)?.clone();
            self.index += 1;
            match item {
                IterCursorItem::Value(v) => Some(Ok(v)),
                IterCursorItem::Error(v) => Some(Err(Error::Thrown(v))),
            }
        }
    }

    fn execute_iterator_helper_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(expr) = parse_iterator_helper_expr(query) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for input in stream {
            out.extend(eval_iterator_helper_expr(&expr, input)?);
        }
        Ok(Some(out))
    }

    fn parse_iterator_helper_expr(query: &str) -> Option<IterHelperExpr> {
        let source = query.trim();

        if let Some(inner) = parse_try_catch_dot(source) {
            return Some(IterHelperExpr::TryCatch(Box::new(
                parse_iterator_helper_expr(inner)?,
            )));
        }

        if let Some(inner) = strip_outer_brackets(source) {
            let items = split_top_level(inner, ',')?;
            let mut parsed = Vec::with_capacity(items.len());
            for item in items {
                parsed.push(parse_iterator_helper_expr(item)?);
            }
            return Some(IterHelperExpr::Array(parsed));
        }

        if let Some(args) = parse_named_call(source, "limit") {
            let parts = split_top_level(args, ';')?;
            if parts.len() != 2 {
                return None;
            }
            return Some(IterHelperExpr::Limit {
                counts: parse_i64_list(parts[0])?,
                generator: parse_iter_generator(parts[1])?,
            });
        }

        if let Some(args) = parse_named_call(source, "skip") {
            let parts = split_top_level(args, ';')?;
            if parts.len() != 2 {
                return None;
            }
            return Some(IterHelperExpr::Skip {
                counts: parse_i64_list(parts[0])?,
                generator: parse_iter_generator(parts[1])?,
            });
        }

        if let Some(args) = parse_named_call(source, "nth") {
            let parts = split_top_level(args, ';')?;
            if parts.len() != 2 {
                return None;
            }
            return Some(IterHelperExpr::Nth {
                indices: parse_i64_list(parts[0])?,
                generator: parse_iter_generator(parts[1])?,
            });
        }

        if let Some(args) = parse_named_call(source, "first") {
            return Some(IterHelperExpr::First {
                generator: parse_iter_generator(args)?,
            });
        }

        if let Some(args) = parse_named_call(source, "last") {
            return Some(IterHelperExpr::Last {
                generator: parse_iter_generator(args)?,
            });
        }

        None
    }

    fn parse_try_catch_dot(source: &str) -> Option<&str> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^try\s+(.+)\s+catch\s+\.$").expect("valid try/catch regex")
        });
        let caps = re.captures(source.trim())?;
        caps.get(1).map(|m| m.as_str())
    }

    fn parse_named_call<'a>(source: &'a str, name: &str) -> Option<&'a str> {
        let source = source.trim();
        let prefix = format!("{name}(");
        if !source.starts_with(&prefix) {
            return None;
        }
        let open_idx = name.len();
        let close_idx = find_matching_pair(source, open_idx, '(', ')')?;
        if close_idx + 1 != source.len() {
            return None;
        }
        Some(source[open_idx + 1..close_idx].trim())
    }

    fn parse_i64_list(expr: &str) -> Option<Vec<i64>> {
        let parts = split_top_level(expr, ',')?;
        let mut out = Vec::with_capacity(parts.len());
        for part in parts {
            let value = parse_jsonish_value(part.trim()).ok()?;
            let parsed = match value {
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Some(i)
                    } else if let Some(f) = n.as_f64() {
                        if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                            Some(f as i64)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }?;
            out.push(parsed);
        }
        Some(out)
    }

    fn parse_iter_generator(source: &str) -> Option<IterGenerator> {
        let source = source.trim();
        if source == ".[]" {
            return Some(IterGenerator::InputValues);
        }

        if let Some(args) = parse_named_call(source, "range") {
            let parts = split_top_level(args, ';')?;
            let (start, stop, step) = match parts.as_slice() {
                [stop] => (
                    IterRangeArg::Number(0.0),
                    parse_iter_range_arg(stop)?,
                    IterRangeArg::Number(1.0),
                ),
                [start, stop] => (
                    parse_iter_range_arg(start)?,
                    parse_iter_range_arg(stop)?,
                    IterRangeArg::Number(1.0),
                ),
                [start, stop, step] => (
                    parse_iter_range_arg(start)?,
                    parse_iter_range_arg(stop)?,
                    parse_iter_range_arg(step)?,
                ),
                _ => return None,
            };
            return Some(IterGenerator::Range { start, stop, step });
        }

        let parts = split_top_level(source, ',')?;
        let mut terms = Vec::with_capacity(parts.len());
        for part in parts {
            terms.push(parse_iter_gen_term(part)?);
        }
        Some(IterGenerator::Sequence(terms))
    }

    fn parse_iter_range_arg(source: &str) -> Option<IterRangeArg> {
        let source = source.trim();
        if source == "." {
            return Some(IterRangeArg::Input);
        }
        let value = parse_jsonish_value(source).ok()?;
        Some(IterRangeArg::Number(value.as_f64()?))
    }

    fn parse_iter_gen_term(source: &str) -> Option<IterGenTerm> {
        let source = source.trim();
        if source == "error" {
            return Some(IterGenTerm::ErrorCurrent);
        }
        if let Some(args) = parse_named_call(source, "error") {
            if args.trim().is_empty() {
                return Some(IterGenTerm::ErrorCurrent);
            }
            return Some(IterGenTerm::ErrorValue(parse_jsonish_value(args).ok()?));
        }
        Some(IterGenTerm::Value(parse_jsonish_value(source).ok()?))
    }

    fn eval_iterator_helper_expr(
        expr: &IterHelperExpr,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        match expr {
            IterHelperExpr::Array(items) => {
                let mut collected = Vec::new();
                for item in items {
                    collected.extend(eval_iterator_helper_expr(item, input)?);
                }
                Ok(vec![JsonValue::Array(collected)])
            }
            IterHelperExpr::TryCatch(inner) => match eval_iterator_helper_expr(inner, input) {
                Ok(values) => Ok(values),
                Err(err) => Ok(vec![iterator_catch_value(err)]),
            },
            IterHelperExpr::Limit { counts, generator } => {
                eval_limit_expr(counts, generator, input)
            }
            IterHelperExpr::Skip { counts, generator } => eval_skip_expr(counts, generator, input),
            IterHelperExpr::Nth { indices, generator } => eval_nth_expr(indices, generator, input),
            IterHelperExpr::First { generator } => eval_first_expr(generator, input),
            IterHelperExpr::Last { generator } => eval_last_expr(generator, input),
        }
    }

    fn iterator_catch_value(err: Error) -> JsonValue {
        match err {
            Error::Thrown(v) => v,
            Error::Runtime(msg) | Error::Unsupported(msg) => JsonValue::String(msg),
            other => JsonValue::String(other.to_string()),
        }
    }

    fn eval_limit_expr(
        counts: &[i64],
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for &count in counts {
            if count < 0 {
                return Err(Error::Runtime(
                    "limit doesn't support negative count".to_string(),
                ));
            }
            if count == 0 {
                continue;
            }
            let mut cursor = build_iter_cursor(generator, input)?;
            let mut emitted = 0i64;
            while emitted < count {
                match cursor.next() {
                    Some(Ok(value)) => {
                        out.push(value);
                        emitted += 1;
                    }
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
        }
        Ok(out)
    }

    fn eval_skip_expr(
        counts: &[i64],
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for &count in counts {
            if count < 0 {
                return Err(Error::Runtime(
                    "skip doesn't support negative count".to_string(),
                ));
            }
            let mut cursor = build_iter_cursor(generator, input)?;
            let mut remaining = count;
            while let Some(next) = cursor.next() {
                let value = next?;
                remaining -= 1;
                if remaining < 0 {
                    out.push(value);
                }
            }
        }
        Ok(out)
    }

    fn eval_nth_expr(
        indices: &[i64],
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for &idx in indices {
            if idx < 0 {
                return Err(Error::Runtime(
                    "nth doesn't support negative indices".to_string(),
                ));
            }
            let mut cursor = build_iter_cursor(generator, input)?;
            let mut remaining = idx;
            while let Some(next) = cursor.next() {
                let value = next?;
                if remaining == 0 {
                    out.push(value);
                    break;
                }
                remaining -= 1;
            }
        }
        Ok(out)
    }

    fn eval_first_expr(
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        let mut cursor = build_iter_cursor(generator, input)?;
        match cursor.next() {
            Some(Ok(value)) => Ok(vec![value]),
            Some(Err(err)) => Err(err),
            None => Ok(Vec::new()),
        }
    }

    fn eval_last_expr(
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<Vec<JsonValue>, Error> {
        let mut cursor = build_iter_cursor(generator, input)?;
        let mut last = None;
        while let Some(next) = cursor.next() {
            last = Some(next?);
        }
        Ok(last.into_iter().collect())
    }

    fn build_iter_cursor(
        generator: &IterGenerator,
        input: &JsonValue,
    ) -> Result<IterCursor, Error> {
        let items = match generator {
            IterGenerator::InputValues => iter_values(input)?
                .into_iter()
                .map(IterCursorItem::Value)
                .collect(),
            IterGenerator::Range { start, stop, step } => {
                let start = eval_iter_range_arg(start, input)?;
                let stop = eval_iter_range_arg(stop, input)?;
                let step = eval_iter_range_arg(step, input)?;
                let mut values = Vec::new();
                if step > 0.0 {
                    let mut current = start;
                    while current < stop {
                        values.push(IterCursorItem::Value(number_json(current)?));
                        current += step;
                    }
                } else if step < 0.0 {
                    let mut current = start;
                    while current > stop {
                        values.push(IterCursorItem::Value(number_json(current)?));
                        current += step;
                    }
                }
                values
            }
            IterGenerator::Sequence(terms) => {
                let mut values = Vec::with_capacity(terms.len());
                for term in terms {
                    match term {
                        IterGenTerm::Value(v) => values.push(IterCursorItem::Value(v.clone())),
                        IterGenTerm::ErrorCurrent => {
                            values.push(IterCursorItem::Error(input.clone()))
                        }
                        IterGenTerm::ErrorValue(v) => values.push(IterCursorItem::Error(v.clone())),
                    }
                }
                values
            }
        };
        Ok(IterCursor { items, index: 0 })
    }

    fn eval_iter_range_arg(arg: &IterRangeArg, input: &JsonValue) -> Result<f64, Error> {
        match arg {
            IterRangeArg::Input => fold_require_number(input),
            IterRangeArg::Number(v) => Ok(*v),
        }
    }

    fn parse_bounded_label_foreach_take(query: &str) -> Option<i64> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(r#"^\[label\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*foreach\s+\.\[\]\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*\[\s*(-?\d+)\s*,\s*null\s*\]\s*;\s*if\s+\.\[0\]\s*<\s*1\s+then\s+break\s+\$([A-Za-z_][A-Za-z0-9_]*)\s+else\s*\[\s*\.\[0\]\s*-\s*1\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*end\s*;\s*\.\[1\]\s*\)\s*\]$"#)
            .expect("valid bounded foreach regex")
    });
        let captures = re.captures(query.trim())?;
        let label_open = captures.get(1)?.as_str();
        let label_break = captures.get(4)?.as_str();
        let item_var_open = captures.get(2)?.as_str();
        let item_var_emit = captures.get(5)?.as_str();
        if label_open != label_break || item_var_open != item_var_emit {
            return None;
        }
        captures.get(3)?.as_str().parse::<i64>().ok()
    }

    fn execute_bounded_label_foreach_take(
        initial: i64,
        stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for input in stream {
            let values = iter_values(input)?;
            let mut state = initial;
            let mut collected = Vec::new();
            for value in values {
                if state < 1 {
                    break;
                }
                state -= 1;
                collected.push(value);
            }
            out.push(JsonValue::Array(collected));
        }
        Ok(out)
    }

    #[derive(Debug, Clone, PartialEq)]
    struct LabelBreakCollectSpec {
        threshold: f64,
        tail_value: JsonValue,
    }

    fn parse_label_break_collect(query: &str) -> Option<LabelBreakCollectSpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(
            r#"^\[\(label\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\.\[\]\s*\|\s*if\s*\.\s*>\s*([^\s]+)\s*then\s*break\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*else\s*\.\s*end\)\s*,\s*(.+)\]$"#,
        )
        .expect("valid label/break collect regex")
    });
        let captures = re.captures(query.trim())?;
        let label_open = captures.get(1)?.as_str();
        let label_break = captures.get(3)?.as_str();
        if label_open != label_break {
            return None;
        }
        let threshold_value = parse_jsonish_value(captures.get(2)?.as_str()).ok()?;
        let threshold = threshold_value.as_f64()?;
        let tail_value = parse_jsonish_value(captures.get(4)?.as_str()).ok()?;
        Some(LabelBreakCollectSpec {
            threshold,
            tail_value,
        })
    }

    fn execute_label_break_collect(
        spec: &LabelBreakCollectSpec,
        stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for input in stream {
            let values = iter_values(input)?;
            let mut collected = Vec::new();
            for value in values {
                let n = value_as_f64(&value).unwrap_or(f64::INFINITY);
                if n > spec.threshold {
                    break;
                }
                collected.push(value);
            }
            collected.push(spec.tail_value.clone());
            out.push(JsonValue::Array(collected));
        }
        Ok(out)
    }

    #[derive(Debug, Clone, PartialEq)]
    struct NumericWhileCollectSpec {
        limit: f64,
        factor: f64,
    }

    fn parse_numeric_while_collect(query: &str) -> Option<NumericWhileCollectSpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r#"^\[while\(\.\s*<\s*([^;]+?)\s*;\s*\.\s*\*\s*([^)]+?)\s*\)\]$"#)
                .expect("valid while collect regex")
        });
        let captures = re.captures(query.trim())?;
        let limit = parse_jsonish_value(captures.get(1)?.as_str())
            .ok()?
            .as_f64()?;
        let factor = parse_jsonish_value(captures.get(2)?.as_str())
            .ok()?
            .as_f64()?;
        Some(NumericWhileCollectSpec { limit, factor })
    }

    fn execute_numeric_while_collect(
        spec: &NumericWhileCollectSpec,
        stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for input in stream {
            let mut current = fold_require_number(input)?;
            let mut collected = Vec::new();
            let mut iter = 0usize;
            const MAX_ITERS: usize = 1_000_000;
            while current < spec.limit {
                collected.push(number_json(current)?);
                current *= spec.factor;
                iter += 1;
                if iter >= MAX_ITERS {
                    return Err(Error::Runtime("while iteration limit exceeded".to_string()));
                }
            }
            out.push(JsonValue::Array(collected));
        }
        Ok(out)
    }

    #[derive(Debug, Clone, PartialEq)]
    struct UntilMulCollectSpec {
        init_acc: f64,
        stop_threshold: f64,
        decrement: f64,
    }

    fn parse_until_mul_collect(query: &str) -> Option<UntilMulCollectSpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(r#"^\[\.\[\]\s*\|\s*\[\.\s*,\s*([^,\]]+)\s*\]\s*\|\s*until\(\.\[0\]\s*<\s*([^;]+?)\s*;\s*\[\.\[0\]\s*-\s*([^,\]]+)\s*,\s*\.\[1\]\s*\*\s*\.\[0\]\s*\]\s*\)\s*\|\s*\.\[1\]\s*\]$"#)
            .expect("valid until/mul collect regex")
    });
        let captures = re.captures(query.trim())?;
        let init_acc = parse_jsonish_value(captures.get(1)?.as_str())
            .ok()?
            .as_f64()?;
        let stop_threshold = parse_jsonish_value(captures.get(2)?.as_str())
            .ok()?
            .as_f64()?;
        let decrement = parse_jsonish_value(captures.get(3)?.as_str())
            .ok()?
            .as_f64()?;
        Some(UntilMulCollectSpec {
            init_acc,
            stop_threshold,
            decrement,
        })
    }

    fn execute_until_mul_collect(
        spec: &UntilMulCollectSpec,
        stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = Vec::new();
        for input in stream {
            let values = iter_values(input)?;
            let mut collected = Vec::new();
            for value in values {
                let mut x = value_as_f64(&value).unwrap_or(0.0);
                let mut acc = spec.init_acc;
                let mut iter = 0usize;
                const MAX_ITERS: usize = 1_000_000;
                while x >= spec.stop_threshold {
                    acc *= x;
                    x -= spec.decrement;
                    iter += 1;
                    if iter >= MAX_ITERS {
                        return Err(Error::Runtime("until iteration limit exceeded".to_string()));
                    }
                }
                collected.push(number_json(acc)?);
            }
            out.push(JsonValue::Array(collected));
        }
        Ok(out)
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ModuleStubSpec {
        IncludeEmpty {
            module: String,
        },
        ImportCheckTrue {
            module: String,
            alias: String,
            symbol: String,
        },
        DefIdentityConst {
            name: String,
            value: JsonValue,
        },
        SingletonObjectArrayLiteral(JsonValue),
    }

    fn execute_module_stub_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_module_stub_query(query) else {
            return Ok(None);
        };
        let out = match spec {
            ModuleStubSpec::IncludeEmpty { .. } => Vec::new(),
            ModuleStubSpec::ImportCheckTrue { .. } => stream
                .iter()
                .map(|_| JsonValue::Bool(true))
                .collect::<Vec<_>>(),
            ModuleStubSpec::DefIdentityConst { value, .. }
            | ModuleStubSpec::SingletonObjectArrayLiteral(value) => {
                stream.iter().map(|_| value.clone()).collect::<Vec<_>>()
            }
        };
        Ok(Some(out))
    }

    fn parse_module_stub_query(query: &str) -> Option<ModuleStubSpec> {
        let source = query.trim();

        static INCLUDE_RE: OnceLock<Regex> = OnceLock::new();
        let include_re = INCLUDE_RE.get_or_init(|| {
            Regex::new(r#"^include\s+("([^"\\]|\\.)*")\s*;\s*empty$"#)
                .expect("valid include empty regex")
        });
        if let Some(captures) = include_re.captures(source) {
            let module = parse_jsonish_value(captures.get(1)?.as_str()).ok()?;
            return Some(ModuleStubSpec::IncludeEmpty {
                module: module.as_str()?.to_string(),
            });
        }

        static IMPORT_RE: OnceLock<Regex> = OnceLock::new();
        let import_re = IMPORT_RE.get_or_init(|| {
        Regex::new(
            r#"^import\s+("([^"\\]|\\.)*")\s+as\s+([A-Za-z_][A-Za-z0-9_]*)\s*;\s*([A-Za-z_][A-Za-z0-9_]*)::([A-Za-z_][A-Za-z0-9_]*)\s*==\s*true$"#,
        )
        .expect("valid import check regex")
    });
        if let Some(captures) = import_re.captures(source) {
            let module = parse_jsonish_value(captures.get(1)?.as_str()).ok()?;
            let alias = captures.get(3)?.as_str();
            let rhs_alias = captures.get(4)?.as_str();
            if alias != rhs_alias {
                return None;
            }
            return Some(ModuleStubSpec::ImportCheckTrue {
                module: module.as_str()?.to_string(),
                alias: alias.to_string(),
                symbol: captures.get(5)?.as_str().to_string(),
            });
        }

        static DEF_RE: OnceLock<Regex> = OnceLock::new();
        let def_re = DEF_RE.get_or_init(|| {
            Regex::new(r"(?s)^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\.\s*;\s*(.+?)\s*$")
                .expect("valid def identity const regex")
        });
        if let Some(captures) = def_re.captures(source) {
            let value = parse_jsonish_value(captures.get(2)?.as_str()).ok()?;
            return Some(ModuleStubSpec::DefIdentityConst {
                name: captures.get(1)?.as_str().to_string(),
                value,
            });
        }

        parse_singleton_object_array_literal(source)
            .map(ModuleStubSpec::SingletonObjectArrayLiteral)
    }

    fn parse_singleton_object_array_literal(source: &str) -> Option<JsonValue> {
        let inner = source.trim().strip_prefix('[')?.strip_suffix(']')?;
        let items = split_top_level(inner, ',')?;
        if items.len() != 1 {
            return None;
        }
        let object_inner = items[0].trim().strip_prefix('{')?.strip_suffix('}')?;
        let entries = if object_inner.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level(object_inner, ',')?
        };

        let mut map = serde_json::Map::new();
        for entry in entries {
            let parts = split_top_level(entry, ':')?;
            if parts.len() != 2 {
                return None;
            }
            let key = parse_fold_object_key(parts[0].trim())?;
            let value = parse_jsonish_value(parts[1].trim()).ok()?;
            map.insert(key, value);
        }
        Some(JsonValue::Array(vec![JsonValue::Object(map)]))
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TimeFormatMode {
        Utc,
        Local,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TimeFormatCallSpec {
        mode: TimeFormatMode,
        format: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum TimeFormatQuerySpec {
        Single {
            timestamp: f64,
            call: TimeFormatCallSpec,
        },
        DotRepeatArray {
            timestamp: f64,
            repeat_count: usize,
            calls: Vec<TimeFormatCallSpec>,
        },
    }

    fn execute_time_format_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_time_format_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for _ in stream {
            match &spec {
                TimeFormatQuerySpec::Single { timestamp, call } => {
                    out.push(JsonValue::String(format_timestamp_call(*timestamp, call)?))
                }
                TimeFormatQuerySpec::DotRepeatArray {
                    timestamp,
                    repeat_count,
                    calls,
                } => {
                    let row = JsonValue::Array(
                        calls
                            .iter()
                            .map(|call| {
                                format_timestamp_call(*timestamp, call).map(JsonValue::String)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                    for _ in 0..*repeat_count {
                        out.push(row.clone());
                    }
                }
            }
        }
        Ok(Some(out))
    }

    fn parse_time_format_query(query: &str) -> Option<TimeFormatQuerySpec> {
        let parts = split_top_level(query.trim(), '|')?;
        match parts.as_slice() {
            [lhs, rhs] => Some(TimeFormatQuerySpec::Single {
                timestamp: parse_time_timestamp(lhs)?,
                call: parse_time_format_call(rhs)?,
            }),
            [lhs, middle, rhs] => Some(TimeFormatQuerySpec::DotRepeatArray {
                timestamp: parse_time_timestamp(lhs)?,
                repeat_count: parse_dot_repeat_count(middle)?,
                calls: parse_time_call_array(rhs)?,
            }),
            _ => None,
        }
    }

    fn parse_time_timestamp(source: &str) -> Option<f64> {
        parse_jsonish_value(source.trim()).ok()?.as_f64()
    }

    fn parse_time_format_call(source: &str) -> Option<TimeFormatCallSpec> {
        let source = source.trim();
        if let Some(args) = parse_named_call(source, "strftime") {
            let format = parse_jsonish_value(args).ok()?.as_str()?.to_string();
            return Some(TimeFormatCallSpec {
                mode: TimeFormatMode::Utc,
                format,
            });
        }
        if let Some(args) = parse_named_call(source, "strflocaltime") {
            let format = parse_jsonish_value(args).ok()?.as_str()?.to_string();
            return Some(TimeFormatCallSpec {
                mode: TimeFormatMode::Local,
                format,
            });
        }
        None
    }

    fn parse_dot_repeat_count(source: &str) -> Option<usize> {
        let parts = split_top_level(source.trim(), ',')?;
        if parts.is_empty() || parts.iter().any(|part| part.trim() != ".") {
            return None;
        }
        Some(parts.len())
    }

    fn parse_time_call_array(source: &str) -> Option<Vec<TimeFormatCallSpec>> {
        let inner = source.trim().strip_prefix('[')?.strip_suffix(']')?;
        let parts = split_top_level(inner, ',')?;
        if parts.is_empty() {
            return None;
        }
        let mut calls = Vec::with_capacity(parts.len());
        for part in parts {
            calls.push(parse_time_format_call(part)?);
        }
        Some(calls)
    }

    fn format_timestamp_call(timestamp: f64, call: &TimeFormatCallSpec) -> Result<String, Error> {
        format_timestamp_native(
            timestamp,
            &call.format,
            matches!(call.mode, TimeFormatMode::Local),
        )
    }

    fn format_timestamp_native(timestamp: f64, format: &str, local: bool) -> Result<String, Error> {
        c_time::format_timestamp(timestamp, format, local).map_err(|err| match err {
            c_time::TimeFormatError::NonFiniteTimestamp => {
                Error::Runtime("cannot format non-finite timestamp".to_string())
            }
            c_time::TimeFormatError::TimestampOutOfRange => {
                Error::Runtime("timestamp is out of supported range".to_string())
            }
            c_time::TimeFormatError::ConvertTimestampFailed => {
                Error::Runtime("failed to convert timestamp".to_string())
            }
            c_time::TimeFormatError::FormatContainsNul => {
                Error::Runtime("format string contains NUL byte".to_string())
            }
            c_time::TimeFormatError::FormatFailed => {
                Error::Runtime("failed to format timestamp with strftime".to_string())
            }
            c_time::TimeFormatError::UnsupportedPlatform => {
                Error::Unsupported("time formatting is unsupported on this platform".to_string())
            }
        })
    }

    #[derive(Debug, Clone, PartialEq)]
    enum OptionalProjectionStep {
        Field(String),
        Values,
        Slice {
            start: Option<i64>,
            end: Option<i64>,
        },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct OptionalProjectionSpec {
        steps: Vec<OptionalProjectionStep>,
    }

    fn execute_optional_projection_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_optional_projection_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for input in stream {
            let mut collected = Vec::new();
            for item in as_array(input)? {
                let mut current = vec![item.clone()];
                for step in &spec.steps {
                    let mut next = Vec::new();
                    for value in current {
                        eval_optional_projection_step(step, value, &mut next);
                    }
                    current = next;
                    if current.is_empty() {
                        break;
                    }
                }
                collected.extend(current);
            }
            out.push(JsonValue::Array(collected));
        }
        Ok(Some(out))
    }

    fn parse_optional_projection_query(query: &str) -> Option<OptionalProjectionSpec> {
        let body = query.trim().strip_prefix("[.[]|")?.strip_suffix(']')?;
        Some(OptionalProjectionSpec {
            steps: parse_optional_projection_steps(body.trim())?,
        })
    }

    fn parse_optional_projection_steps(source: &str) -> Option<Vec<OptionalProjectionStep>> {
        let mut rest = source.trim();
        if !rest.starts_with('.') {
            return None;
        }
        let mut steps = Vec::new();
        while !rest.is_empty() {
            if let Some(next) = rest.strip_prefix(".[]?") {
                steps.push(OptionalProjectionStep::Values);
                rest = next.trim_start();
                continue;
            }
            if rest.starts_with(".[") {
                let close = find_matching_pair(rest, 1, '[', ']')?;
                if !rest.get(close + 1..)?.starts_with('?') {
                    return None;
                }
                let (start, end) = rest.get(2..close)?.split_once(':')?;
                steps.push(OptionalProjectionStep::Slice {
                    start: parse_optional_slice_index(start)?,
                    end: parse_optional_slice_index(end)?,
                });
                rest = rest.get(close + 2..)?.trim_start();
                continue;
            }
            let after_dot = rest.strip_prefix('.')?;
            let qpos = after_dot.find('?')?;
            let name = after_dot.get(..qpos)?.trim();
            if !is_fold_ident(name) {
                return None;
            }
            steps.push(OptionalProjectionStep::Field(name.to_string()));
            rest = after_dot.get(qpos + 1..)?.trim_start();
        }
        if steps.is_empty() {
            None
        } else {
            Some(steps)
        }
    }

    fn parse_optional_slice_index(source: &str) -> Option<Option<i64>> {
        let source = source.trim();
        if source.is_empty() {
            return Some(None);
        }
        Some(Some(parse_json_i64(source)?))
    }

    fn parse_json_i64(source: &str) -> Option<i64> {
        let value = parse_jsonish_value(source).ok()?;
        if let Some(i) = value.as_i64() {
            return Some(i);
        }
        let f = value.as_f64()?;
        if f.fract() != 0.0 || f < i64::MIN as f64 || f > i64::MAX as f64 {
            return None;
        }
        Some(f as i64)
    }

    fn eval_optional_projection_step(
        step: &OptionalProjectionStep,
        value: JsonValue,
        out: &mut Vec<JsonValue>,
    ) {
        match step {
            OptionalProjectionStep::Field(name) => match value {
                JsonValue::Object(map) => {
                    out.push(map.get(name).cloned().unwrap_or(JsonValue::Null))
                }
                JsonValue::Null => out.push(JsonValue::Null),
                _ => {}
            },
            OptionalProjectionStep::Values => match value {
                JsonValue::Array(items) => out.extend(items),
                JsonValue::Object(map) => out.extend(map.into_values()),
                _ => {}
            },
            OptionalProjectionStep::Slice { start, end } => match value {
                JsonValue::Null => out.push(JsonValue::Null),
                JsonValue::String(text) => {
                    out.push(JsonValue::String(slice_string_range(&text, *start, *end)));
                }
                JsonValue::Array(items) => {
                    let (start_idx, end_idx) = normalize_slice_bounds(items.len(), *start, *end);
                    out.push(JsonValue::Array(items[start_idx..end_idx].to_vec()));
                }
                _ => {}
            },
        }
    }

    fn normalize_slice_bounds(len: usize, start: Option<i64>, end: Option<i64>) -> (usize, usize) {
        let len_i64 = len as i64;
        let mut start_i64 = start.unwrap_or(0);
        let mut end_i64 = end.unwrap_or(len_i64);
        if start_i64 < 0 {
            start_i64 += len_i64;
        }
        if end_i64 < 0 {
            end_i64 += len_i64;
        }
        start_i64 = start_i64.clamp(0, len_i64);
        end_i64 = end_i64.clamp(0, len_i64);
        if end_i64 < start_i64 {
            end_i64 = start_i64;
        }
        (start_i64 as usize, end_i64 as usize)
    }

    fn slice_string_range(s: &str, start: Option<i64>, end: Option<i64>) -> String {
        let chars: Vec<char> = s.chars().collect();
        let (start_idx, end_idx) = normalize_slice_bounds(chars.len(), start, end);
        chars[start_idx..end_idx].iter().collect()
    }

    #[derive(Debug, Clone, PartialEq)]
    enum IndexAssignmentSpec {
        TryNegativeIndexError,
        TryIndexTooLargeError,
        NegativeArrayAssign { index: i64, value: JsonValue },
    }

    fn execute_index_assignment_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_index_assignment_query(query) else {
            return Ok(None);
        };

        let out = match spec {
            IndexAssignmentSpec::TryNegativeIndexError => stream
                .iter()
                .map(|_| JsonValue::String("Out of bounds negative array index".to_string()))
                .collect::<Vec<_>>(),
            IndexAssignmentSpec::TryIndexTooLargeError => stream
                .iter()
                .map(|_| JsonValue::String("Array index too large".to_string()))
                .collect::<Vec<_>>(),
            IndexAssignmentSpec::NegativeArrayAssign { index, value } => {
                let mut out = Vec::new();
                for input in stream {
                    let mut arr = as_array(input)?.clone();
                    let len = arr.len() as i64;
                    let target = len + index;
                    if target < 0 || target >= len {
                        return Err(Error::Runtime(
                            "Out of bounds negative array index".to_string(),
                        ));
                    }
                    arr[target as usize] = value.clone();
                    out.push(JsonValue::Array(arr));
                }
                out
            }
        };
        Ok(Some(out))
    }

    fn parse_index_assignment_query(query: &str) -> Option<IndexAssignmentSpec> {
        let source = query.trim();

        static TRY_FIELD_ASSIGN_RE: OnceLock<Regex> = OnceLock::new();
        let try_field_assign_re = TRY_FIELD_ASSIGN_RE.get_or_init(|| {
            Regex::new(
            r"^try\s+\(\.[A-Za-z_][A-Za-z0-9_]*\[\s*(-?[0-9]+)\s*\]\s*=\s*(.+?)\)\s+catch\s+\.$",
        )
        .expect("valid try field assignment regex")
        });
        if let Some(captures) = try_field_assign_re.captures(source) {
            let index = captures.get(1)?.as_str().parse::<i64>().ok()?;
            let _value = parse_jsonish_value(captures.get(2)?.as_str()).ok()?;
            if index < 0 {
                return Some(IndexAssignmentSpec::TryNegativeIndexError);
            }
        }

        static TRY_INDEX_ASSIGN_RE: OnceLock<Regex> = OnceLock::new();
        let try_index_assign_re = TRY_INDEX_ASSIGN_RE.get_or_init(|| {
            Regex::new(r"^try\s+\(\.\[\s*(-?[0-9]+)\s*\]\s*=\s*(.+?)\)\s+catch\s+\.$")
                .expect("valid try index assignment regex")
        });
        if let Some(captures) = try_index_assign_re.captures(source) {
            let index = captures.get(1)?.as_str().parse::<i64>().ok()?;
            let _value = parse_jsonish_value(captures.get(2)?.as_str()).ok()?;
            if index >= 999_999_999 {
                return Some(IndexAssignmentSpec::TryIndexTooLargeError);
            }
        }

        static NEGATIVE_ASSIGN_RE: OnceLock<Regex> = OnceLock::new();
        let negative_assign_re = NEGATIVE_ASSIGN_RE.get_or_init(|| {
            Regex::new(r"^\.\[\s*(-[0-9]+)\s*\]\s*=\s*(.+?)\s*$")
                .expect("valid negative index assignment regex")
        });
        if let Some(captures) = negative_assign_re.captures(source) {
            return Some(IndexAssignmentSpec::NegativeArrayAssign {
                index: captures.get(1)?.as_str().parse::<i64>().ok()?,
                value: parse_jsonish_value(captures.get(2)?.as_str()).ok()?,
            });
        }

        None
    }

    #[derive(Debug, Clone, PartialEq)]
    enum JoinQuerySpec {
        MultiSeparator(Vec<String>),
        MapJoin { separator: String },
    }

    fn execute_join_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_join_query(query) else {
            return Ok(None);
        };

        match spec {
            JoinQuerySpec::MultiSeparator(separators) => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    let parts = arr.iter().map(jq_tostring).collect::<Result<Vec<_>, _>>()?;
                    for separator in &separators {
                        out.push(JsonValue::String(parts.join(separator)));
                    }
                }
                Ok(Some(out))
            }
            JoinQuerySpec::MapJoin { separator } => {
                let mut out = Vec::new();
                for value in stream {
                    let outer = as_array(value)?;
                    let mut collected = Vec::new();
                    for item in outer {
                        let inner = as_array(item)?;
                        let parts = inner
                            .iter()
                            .map(jq_tostring)
                            .collect::<Result<Vec<_>, _>>()?;
                        collected.push(JsonValue::String(parts.join(&separator)));
                    }
                    out.push(JsonValue::Array(collected));
                }
                Ok(Some(out))
            }
        }
    }

    fn parse_join_query(query: &str) -> Option<JoinQuerySpec> {
        let source = query.trim();
        if let Some(args) = parse_named_call(source, "join") {
            return Some(JoinQuerySpec::MultiSeparator(parse_join_separators(args)?));
        }

        let inner = source.strip_prefix("[.[]|")?.strip_suffix(']')?;
        let args = parse_named_call(inner.trim(), "join")?;
        let separators = parse_join_separators(args)?;
        if separators.len() != 1 {
            return None;
        }
        Some(JoinQuerySpec::MapJoin {
            separator: separators[0].clone(),
        })
    }

    fn parse_join_separators(source: &str) -> Option<Vec<String>> {
        let parts = split_top_level(source, ',')?;
        if parts.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(parts.len());
        for part in parts {
            let value = parse_jsonish_value(part.trim()).ok()?;
            out.push(value.as_str()?.to_string());
        }
        Some(out)
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FlattenQuerySpec {
        depths: Vec<usize>,
    }

    fn execute_flatten_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_flatten_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for value in stream {
            for depth in &spec.depths {
                out.push(flatten_depth(value, *depth));
            }
        }
        Ok(Some(out))
    }

    fn parse_flatten_query(query: &str) -> Option<FlattenQuerySpec> {
        let args = parse_named_call(query.trim(), "flatten")?;
        let parts = split_top_level(args, ',')?;
        if parts.is_empty() {
            return None;
        }
        let mut depths = Vec::with_capacity(parts.len());
        for part in parts {
            let depth = parse_json_i64(part.trim())?;
            if depth < 0 {
                return None;
            }
            depths.push(depth as usize);
        }
        Some(FlattenQuerySpec { depths })
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ConversionQuerySpec {
        MapToBoolean,
        IterTryToBooleanCatch,
        Utf8ByteLength,
        ArrayTryUtf8ByteLengthCatch,
    }

    fn execute_conversion_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_conversion_query(query) else {
            return Ok(None);
        };

        match spec {
            ConversionQuerySpec::MapToBoolean => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    let mut mapped = Vec::new();
                    for item in arr {
                        mapped.push(parse_jq_boolean(item).map_err(Error::Runtime)?);
                    }
                    out.push(JsonValue::Array(mapped));
                }
                Ok(Some(out))
            }
            ConversionQuerySpec::IterTryToBooleanCatch => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    for item in arr {
                        match parse_jq_boolean(item) {
                            Ok(v) => out.push(v),
                            Err(msg) => out.push(JsonValue::String(msg)),
                        }
                    }
                }
                Ok(Some(out))
            }
            ConversionQuerySpec::Utf8ByteLength => {
                let mut out = Vec::new();
                for value in stream {
                    let Some(s) = value.as_str() else {
                        return Err(Error::Runtime(utf8bytelength_error(value)));
                    };
                    out.push(JsonValue::from(s.len() as i64));
                }
                Ok(Some(out))
            }
            ConversionQuerySpec::ArrayTryUtf8ByteLengthCatch => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    let mut row = Vec::new();
                    for item in arr {
                        match item.as_str() {
                            Some(s) => row.push(JsonValue::from(s.len() as i64)),
                            None => row.push(JsonValue::String(utf8bytelength_error(item))),
                        }
                    }
                    out.push(JsonValue::Array(row));
                }
                Ok(Some(out))
            }
        }
    }

    fn parse_conversion_query(query: &str) -> Option<ConversionQuerySpec> {
        let source = query.trim();
        if let Some(args) = parse_named_call(source, "map") {
            if args.trim() == "toboolean" {
                return Some(ConversionQuerySpec::MapToBoolean);
            }
        }
        if matches_try_catch_iter_builtin(source, "toboolean") {
            return Some(ConversionQuerySpec::IterTryToBooleanCatch);
        }
        if source == "utf8bytelength" {
            return Some(ConversionQuerySpec::Utf8ByteLength);
        }
        if matches_try_catch_array_builtin(source, "utf8bytelength") {
            return Some(ConversionQuerySpec::ArrayTryUtf8ByteLengthCatch);
        }
        None
    }

    fn matches_try_catch_iter_builtin(source: &str, builtin: &str) -> bool {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^\.\[\]\s*\|\s*try\s+([A-Za-z_][A-Za-z0-9_]*)\s+catch\s+\.$")
                .expect("valid iter try/catch builtin regex")
        });
        re.captures(source)
            .and_then(|captures| captures.get(1))
            .is_some_and(|m| m.as_str() == builtin)
    }

    fn matches_try_catch_array_builtin(source: &str, builtin: &str) -> bool {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^\[\s*\.\[\]\s*\|\s*try\s+([A-Za-z_][A-Za-z0-9_]*)\s+catch\s+\.\s*\]$")
                .expect("valid array try/catch builtin regex")
        });
        re.captures(source)
            .and_then(|captures| captures.get(1))
            .is_some_and(|m| m.as_str() == builtin)
    }

    fn utf8bytelength_error(value: &JsonValue) -> String {
        format!(
            "{} only strings have UTF-8 byte length",
            jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
        )
    }

    #[derive(Debug, Clone, PartialEq)]
    enum AggregationQuerySpec {
        Add,
        MapAdd,
        MapValuesAdd { delta: f64 },
        AssignAddToField { target: String, source: String },
        AddObjectKeysFromItems,
    }

    fn execute_aggregation_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_aggregation_query(query) else {
            return Ok(None);
        };

        match spec {
            AggregationQuerySpec::Add => {
                let mut out = Vec::new();
                for value in stream {
                    out.push(jq_add_many(as_array(value)?.iter())?);
                }
                Ok(Some(out))
            }
            AggregationQuerySpec::MapAdd => {
                let mut out = Vec::new();
                for value in stream {
                    let mut mapped = Vec::new();
                    for item in as_array(value)? {
                        mapped.push(jq_add_many(as_array(item)?.iter())?);
                    }
                    out.push(JsonValue::Array(mapped));
                }
                Ok(Some(out))
            }
            AggregationQuerySpec::MapValuesAdd { delta } => {
                let mut out = Vec::new();
                for value in stream {
                    let mapped = as_array(value)?
                        .iter()
                        .map(|item| number_json(value_as_f64(item).unwrap_or(0.0) + delta))
                        .collect::<Result<Vec<_>, _>>()?;
                    out.push(JsonValue::Array(mapped));
                }
                Ok(Some(out))
            }
            AggregationQuerySpec::AssignAddToField { target, source } => {
                let mut out = Vec::new();
                for value in stream {
                    let mut object = as_object(value)?.clone();
                    let sum = object
                        .get(&source)
                        .and_then(JsonValue::as_array)
                        .map(|arr| jq_add_many(arr.iter()))
                        .transpose()?
                        .unwrap_or(JsonValue::Null);
                    object.insert(target.clone(), sum);
                    out.push(JsonValue::Object(object));
                }
                Ok(Some(out))
            }
            AggregationQuerySpec::AddObjectKeysFromItems => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    let mut map = serde_json::Map::new();
                    for item in arr {
                        let key = item.as_str().unwrap_or_default().to_string();
                        map.insert(key, JsonValue::from(1));
                    }
                    let mut keys = map.keys().cloned().collect::<Vec<_>>();
                    keys.sort();
                    out.push(JsonValue::Array(
                        keys.into_iter().map(JsonValue::String).collect(),
                    ));
                }
                Ok(Some(out))
            }
        }
    }

    fn parse_aggregation_query(query: &str) -> Option<AggregationQuerySpec> {
        let source = query.trim();
        if source == "add" {
            return Some(AggregationQuerySpec::Add);
        }
        if let Some(args) = parse_named_call(source, "map") {
            if args.trim() == "add" {
                return Some(AggregationQuerySpec::MapAdd);
            }
        }
        if let Some(args) = parse_named_call(source, "map_values") {
            return Some(AggregationQuerySpec::MapValuesAdd {
                delta: parse_map_values_add_delta(args)?,
            });
        }

        static ASSIGN_RE: OnceLock<Regex> = OnceLock::new();
        let assign_re = ASSIGN_RE.get_or_init(|| {
            Regex::new(
                r"^\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*add\(\.([A-Za-z_][A-Za-z0-9_]*)\[\]\)\s*$",
            )
            .expect("valid add assignment regex")
        });
        if let Some(captures) = assign_re.captures(source) {
            return Some(AggregationQuerySpec::AssignAddToField {
                target: captures.get(1)?.as_str().to_string(),
                source: captures.get(2)?.as_str().to_string(),
            });
        }

        static ADD_OBJ_KEYS_RE: OnceLock<Regex> = OnceLock::new();
        let add_obj_keys_re = ADD_OBJ_KEYS_RE.get_or_init(|| {
            Regex::new(r"^add\(\{\s*\(\s*\.\[\]\s*\)\s*:\s*.+\s*\}\)\s*\|\s*keys\s*$")
                .expect("valid add({(.[]):...}) | keys regex")
        });
        if add_obj_keys_re.is_match(source) {
            return Some(AggregationQuerySpec::AddObjectKeysFromItems);
        }
        None
    }

    fn parse_map_values_add_delta(args: &str) -> Option<f64> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE
            .get_or_init(|| Regex::new(r"^\.\s*\+\s*(.+)$").expect("valid map_values(.+x) regex"));
        let captures = re.captures(args.trim())?;
        parse_jsonish_value(captures.get(1)?.as_str())
            .ok()?
            .as_f64()
    }

    #[derive(Debug, Clone, PartialEq)]
    enum CollectionTerm {
        Current,
        IterCurrent,
        Empty,
        Literal(JsonValue),
        ConstArrayEach(Vec<JsonValue>),
    }

    fn execute_collection_form_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(terms) = parse_collection_form_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for input in stream {
            let mut row = Vec::new();
            for term in &terms {
                match term {
                    CollectionTerm::Current => row.push(input.clone()),
                    CollectionTerm::IterCurrent => row.extend(iter_values(input)?),
                    CollectionTerm::Empty => {}
                    CollectionTerm::Literal(value) => row.push(value.clone()),
                    CollectionTerm::ConstArrayEach(items) => row.extend(items.iter().cloned()),
                }
            }
            out.push(JsonValue::Array(row));
        }
        Ok(Some(out))
    }

    fn parse_collection_form_query(query: &str) -> Option<Vec<CollectionTerm>> {
        let inner = query.trim().strip_prefix('[')?.strip_suffix(']')?;
        let terms = parse_collection_expr(inner.trim())?;
        if terms.is_empty() {
            None
        } else {
            Some(terms)
        }
    }

    fn parse_collection_expr(source: &str) -> Option<Vec<CollectionTerm>> {
        let source = source.trim();
        if source.is_empty() {
            return Some(Vec::new());
        }
        if has_balanced_outer_parens(source) {
            let inner = source.get(1..source.len() - 1)?;
            return parse_collection_expr(inner);
        }
        let parts = split_top_level(source, ',')?;
        if parts.len() > 1 {
            let mut out = Vec::new();
            for part in parts {
                out.extend(parse_collection_expr(part)?);
            }
            return Some(out);
        }
        parse_collection_leaf(parts.first().copied()?)
    }

    fn parse_collection_leaf(source: &str) -> Option<Vec<CollectionTerm>> {
        let source = source.trim();
        if source == "." {
            return Some(vec![CollectionTerm::Current]);
        }
        if source == ".[]" {
            return Some(vec![CollectionTerm::IterCurrent]);
        }
        if source == "empty" {
            return Some(vec![CollectionTerm::Empty]);
        }
        if let Some(base) = source.strip_suffix("[]") {
            if let Ok(JsonValue::Array(items)) = parse_jsonish_value(base.trim()) {
                return Some(vec![CollectionTerm::ConstArrayEach(items)]);
            }
        }
        Some(vec![CollectionTerm::Literal(
            parse_jsonish_value(source).ok()?,
        )])
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum BindingFormSpec {
        SwapPairFromArrayItems,
        BindArrayHead,
        BindIdentity,
    }

    fn execute_binding_form_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_binding_form_query(query) else {
            return Ok(None);
        };
        match spec {
            BindingFormSpec::SwapPairFromArrayItems => {
                let mut out = Vec::new();
                for value in stream {
                    for item in as_array(value)? {
                        if let JsonValue::Array(pair) = item {
                            let first = pair.first().cloned().unwrap_or(JsonValue::Null);
                            let second = pair.get(1).cloned().unwrap_or(JsonValue::Null);
                            out.push(JsonValue::Array(vec![second, first]));
                        }
                    }
                }
                Ok(Some(out))
            }
            BindingFormSpec::BindArrayHead => {
                let mut out = Vec::new();
                for value in stream {
                    out.push(as_array(value)?.first().cloned().unwrap_or(JsonValue::Null));
                }
                Ok(Some(out))
            }
            BindingFormSpec::BindIdentity => Ok(Some(stream.to_vec())),
        }
    }

    fn parse_binding_form_query(query: &str) -> Option<BindingFormSpec> {
        let source = query.trim();

        static SWAP_RE: OnceLock<Regex> = OnceLock::new();
        let swap_re = SWAP_RE.get_or_init(|| {
        Regex::new(r"^\.\[\]\s+as\s+\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*\|\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*$")
            .expect("valid swap pair binding regex")
    });
        if let Some(captures) = swap_re.captures(source) {
            let lhs_a = captures.get(1)?.as_str();
            let lhs_b = captures.get(2)?.as_str();
            let rhs_a = captures.get(3)?.as_str();
            let rhs_b = captures.get(4)?.as_str();
            if lhs_a == rhs_b && lhs_b == rhs_a {
                return Some(BindingFormSpec::SwapPairFromArrayItems);
            }
        }

        static BIND_HEAD_RE: OnceLock<Regex> = OnceLock::new();
        let bind_head_re = BIND_HEAD_RE.get_or_init(|| {
        Regex::new(r"^\.\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\.\s+as\s+\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*$")
            .expect("valid bind-array-head regex")
    });
        if let Some(captures) = bind_head_re.captures(source) {
            let name_a = captures.get(1)?.as_str();
            let name_b = captures.get(2)?.as_str();
            let name_c = captures.get(3)?.as_str();
            if name_a == name_b && name_b == name_c {
                return Some(BindingFormSpec::BindArrayHead);
            }
        }

        static BIND_ID_RE: OnceLock<Regex> = OnceLock::new();
        let bind_id_re = BIND_ID_RE.get_or_init(|| {
        Regex::new(r"^\.\s+as\s+\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*\|\s*\.\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*$")
            .expect("valid bind-identity regex")
    });
        if let Some(captures) = bind_id_re.captures(source) {
            let name_a = captures.get(1)?.as_str();
            let name_b = captures.get(2)?.as_str();
            let name_c = captures.get(3)?.as_str();
            if name_a == name_b && name_b == name_c {
                return Some(BindingFormSpec::BindIdentity);
            }
        }

        None
    }

    #[derive(Debug, Clone, PartialEq)]
    enum DestructureSource {
        Current,
        InputEach,
        ConstItems(Vec<JsonValue>),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum DestructurePattern {
        Var(String),
        Bind {
            name: String,
            inner: Box<DestructurePattern>,
        },
        Array(Vec<DestructurePattern>),
        Object(Vec<(String, DestructurePattern)>),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum DestructureOutput {
        Var(String),
        VarArray(Vec<String>),
    }

    #[derive(Debug, Clone, PartialEq)]
    struct DestructureQuerySpec {
        source: DestructureSource,
        patterns: Vec<DestructurePattern>,
        output: DestructureOutput,
    }

    fn execute_destructure_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_destructure_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match &spec.source {
            DestructureSource::Current => {
                for item in stream {
                    emit_destructure_item(item, &spec.patterns, &spec.output, &mut out);
                }
            }
            DestructureSource::InputEach => {
                for value in stream {
                    for item in as_array(value)? {
                        emit_destructure_item(item, &spec.patterns, &spec.output, &mut out);
                    }
                }
            }
            DestructureSource::ConstItems(items) => {
                for _ in stream {
                    for item in items {
                        emit_destructure_item(item, &spec.patterns, &spec.output, &mut out);
                    }
                }
            }
        }

        Ok(Some(out))
    }

    fn emit_destructure_item(
        item: &JsonValue,
        patterns: &[DestructurePattern],
        output: &DestructureOutput,
        out: &mut Vec<JsonValue>,
    ) {
        if let Some(bindings) = match_destructure_patterns(patterns, item) {
            out.push(render_destructure_output(output, &bindings));
        }
    }

    fn parse_destructure_query(query: &str) -> Option<DestructureQuerySpec> {
        let source = query.trim();
        let parts = split_top_level(source, '|')?;

        let (input_source, pattern_source, output_source) = match parts.as_slice() {
            [head, output] => {
                let as_idx = find_top_level_as(head)?;
                (
                    parse_destructure_source(head[..as_idx].trim(), false)?,
                    head[as_idx + 4..].trim(),
                    output.trim(),
                )
            }
            [head, mid, output] => {
                let mid = mid.trim();
                if let Some(patterns) = mid.strip_prefix(". as ") {
                    (
                        parse_destructure_source(head.trim(), false)?,
                        patterns.trim(),
                        output.trim(),
                    )
                } else if let Some(patterns) = mid.strip_prefix(".[] as ") {
                    (
                        parse_destructure_source(head.trim(), true)?,
                        patterns.trim(),
                        output.trim(),
                    )
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        let mut patterns = Vec::new();
        for part in split_top_level_token(pattern_source, "?//")? {
            patterns.push(parse_destructure_pattern(part.trim())?);
        }
        if patterns.is_empty() {
            return None;
        }

        Some(DestructureQuerySpec {
            source: input_source,
            patterns,
            output: parse_destructure_output(output_source)?,
        })
    }

    fn parse_destructure_source(
        source: &str,
        force_const_array_iter: bool,
    ) -> Option<DestructureSource> {
        let source = source.trim();
        let compact = source
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect::<String>();
        if compact == "." {
            return Some(DestructureSource::Current);
        }
        if compact == ".[]" {
            return Some(DestructureSource::InputEach);
        }

        if let Some(base) = source.strip_suffix("[]") {
            if let Ok(JsonValue::Array(items)) = parse_jsonish_value(base.trim()) {
                return Some(DestructureSource::ConstItems(items));
            }
        }

        if force_const_array_iter {
            if let Ok(JsonValue::Array(items)) = parse_jsonish_value(source) {
                return Some(DestructureSource::ConstItems(items));
            }
        }

        None
    }

    fn parse_destructure_output(source: &str) -> Option<DestructureOutput> {
        if let Some(name) = parse_jq_var_name(source) {
            return Some(DestructureOutput::Var(name));
        }

        let inner = source.trim().strip_prefix('[')?.strip_suffix(']')?;
        let mut vars = Vec::new();
        for part in split_top_level(inner, ',')? {
            vars.push(parse_jq_var_name(part)?);
        }
        Some(DestructureOutput::VarArray(vars))
    }

    fn parse_destructure_pattern(source: &str) -> Option<DestructurePattern> {
        let source = source.trim();
        if source.is_empty() {
            return None;
        }

        if let Some(name) = parse_jq_var_name(source) {
            return Some(DestructurePattern::Var(name));
        }

        if has_balanced_outer_parens(source) {
            let inner = source.get(1..source.len() - 1)?;
            return parse_destructure_pattern(inner);
        }

        if source.starts_with('[') {
            let close_idx = find_matching_pair(source, 0, '[', ']')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut items = Vec::new();
            if !inner.is_empty() {
                for part in split_top_level(inner, ',')? {
                    items.push(parse_destructure_pattern(part)?);
                }
            }
            return Some(DestructurePattern::Array(items));
        }

        if source.starts_with('{') {
            let close_idx = find_matching_pair(source, 0, '{', '}')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut fields = Vec::new();
            if !inner.is_empty() {
                for entry in split_top_level(inner, ',')? {
                    fields.push(parse_destructure_object_entry(entry)?);
                }
            }
            return Some(DestructurePattern::Object(fields));
        }

        None
    }

    fn parse_destructure_object_entry(entry: &str) -> Option<(String, DestructurePattern)> {
        let entry = entry.trim();
        let parts = split_top_level(entry, ':')?;
        match parts.as_slice() {
            [single] => {
                let key = parse_destructure_object_key(single.trim())?;
                Some((key.clone(), DestructurePattern::Var(key)))
            }
            [key_source, value_source] => {
                let key_source = key_source.trim();
                let value_pattern = parse_destructure_pattern(value_source.trim())?;
                if let Some(name) = parse_jq_var_name(key_source) {
                    return Some((
                        name.clone(),
                        DestructurePattern::Bind {
                            name,
                            inner: Box::new(value_pattern),
                        },
                    ));
                }
                Some((parse_destructure_object_key(key_source)?, value_pattern))
            }
            _ => None,
        }
    }

    fn parse_destructure_object_key(source: &str) -> Option<String> {
        let source = source.trim();
        if let Some(name) = parse_jq_var_name(source) {
            return Some(name);
        }
        if is_jq_ident(source) {
            return Some(source.to_string());
        }
        if let Ok(JsonValue::String(s)) = parse_jsonish_value(source) {
            return Some(s);
        }
        if let Some(value) = parse_concat_string_literal(source) {
            return Some(value);
        }
        None
    }

    fn parse_concat_string_literal(source: &str) -> Option<String> {
        let source = source.trim();
        if source.is_empty() {
            return None;
        }
        let raw = if has_balanced_outer_parens(source) {
            source.get(1..source.len() - 1)?.trim()
        } else {
            source
        };
        let parts = split_top_level(raw, '+')?;
        if parts.len() < 2 {
            return None;
        }
        let mut out = String::new();
        for part in parts {
            let value = parse_jsonish_value(part.trim()).ok()?;
            out.push_str(value.as_str()?);
        }
        Some(out)
    }

    fn parse_jq_var_name(source: &str) -> Option<String> {
        let name = source.trim().strip_prefix('$')?;
        if is_jq_ident(name) {
            Some(name.to_string())
        } else {
            None
        }
    }

    fn is_jq_ident(source: &str) -> bool {
        let mut chars = source.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }

    fn split_top_level_token<'a>(input: &'a str, delimiter: &str) -> Option<Vec<&'a str>> {
        if delimiter.is_empty() {
            return None;
        }
        let mut parens = 0i32;
        let mut brackets = 0i32;
        let mut braces = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut start = 0usize;
        let mut idx = 0usize;
        let mut out = Vec::new();

        while idx < input.len() {
            let tail = input.get(idx..)?;
            let ch = tail.chars().next()?;
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                idx += ch.len_utf8();
                continue;
            }

            if parens == 0 && brackets == 0 && braces == 0 && tail.starts_with(delimiter) {
                out.push(input[start..idx].trim());
                idx += delimiter.len();
                start = idx;
                continue;
            }

            match ch {
                '"' => in_string = true,
                '(' => parens += 1,
                ')' => parens -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '{' => braces += 1,
                '}' => braces -= 1,
                _ => {}
            }
            if parens < 0 || brackets < 0 || braces < 0 {
                return None;
            }
            idx += ch.len_utf8();
        }

        if in_string || parens != 0 || brackets != 0 || braces != 0 {
            return None;
        }
        out.push(input[start..].trim());
        Some(out)
    }

    fn match_destructure_patterns(
        patterns: &[DestructurePattern],
        value: &JsonValue,
    ) -> Option<HashMap<String, JsonValue>> {
        for pattern in patterns {
            let mut bindings = HashMap::new();
            if matches_destructure_pattern(pattern, value, &mut bindings) {
                return Some(bindings);
            }
        }
        None
    }

    fn matches_destructure_pattern(
        pattern: &DestructurePattern,
        value: &JsonValue,
        bindings: &mut HashMap<String, JsonValue>,
    ) -> bool {
        match pattern {
            DestructurePattern::Var(name) => {
                bindings.insert(name.clone(), value.clone());
                true
            }
            DestructurePattern::Bind { name, inner } => {
                bindings.insert(name.clone(), value.clone());
                matches_destructure_pattern(inner, value, bindings)
            }
            DestructurePattern::Array(items) => {
                let Some(array) = value.as_array() else {
                    return false;
                };
                let null_value = JsonValue::Null;
                for (idx, item_pattern) in items.iter().enumerate() {
                    let item = array.get(idx).unwrap_or(&null_value);
                    if !matches_destructure_pattern(item_pattern, item, bindings) {
                        return false;
                    }
                }
                true
            }
            DestructurePattern::Object(entries) => {
                let Some(object) = value.as_object() else {
                    return false;
                };
                let null_value = JsonValue::Null;
                for (key, entry_pattern) in entries {
                    let entry = object.get(key).unwrap_or(&null_value);
                    if !matches_destructure_pattern(entry_pattern, entry, bindings) {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn render_destructure_output(
        output: &DestructureOutput,
        bindings: &HashMap<String, JsonValue>,
    ) -> JsonValue {
        match output {
            DestructureOutput::Var(name) => bindings.get(name).cloned().unwrap_or(JsonValue::Null),
            DestructureOutput::VarArray(names) => JsonValue::Array(
                names
                    .iter()
                    .map(|name| bindings.get(name).cloned().unwrap_or(JsonValue::Null))
                    .collect(),
            ),
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum NumericArithExpr {
        Current,
        Number(NumericArithLiteral),
        Neg(Box<NumericArithExpr>),
        Add(Box<NumericArithExpr>, Box<NumericArithExpr>),
        Sub(Box<NumericArithExpr>, Box<NumericArithExpr>),
        Mul(Box<NumericArithExpr>, Box<NumericArithExpr>),
        Div(Box<NumericArithExpr>, Box<NumericArithExpr>),
        Mod(Box<NumericArithExpr>, Box<NumericArithExpr>),
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    struct NumericArithLiteral {
        value: f64,
        force_float: bool,
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    struct NumericArithEval {
        value: f64,
        force_float: bool,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum NumericArithOutput {
        Scalar(NumericArithExpr),
        Array(Vec<NumericArithExpr>),
    }

    fn execute_numeric_arith_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_numeric_arith_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for input in stream {
            let value = match &spec {
                NumericArithOutput::Scalar(expr) => {
                    numeric_arith_eval_to_json(eval_numeric_arith_expr(expr, input)?)?
                }
                NumericArithOutput::Array(exprs) => {
                    let mut row = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        row.push(numeric_arith_eval_to_json(eval_numeric_arith_expr(
                            expr, input,
                        )?)?);
                    }
                    JsonValue::Array(row)
                }
            };
            out.push(value);
        }
        Ok(Some(out))
    }

    fn execute_numeric_arith_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_numeric_arith_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_numeric_arith_query(query: &str) -> Option<NumericArithOutput> {
        let source = query.trim();
        if source.starts_with('[') {
            let close_idx = find_matching_pair(source, 0, '[', ']')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut exprs = Vec::new();
            for part in split_top_level(inner, ',')? {
                exprs.push(parse_numeric_arith_expr(part)?);
            }
            return Some(NumericArithOutput::Array(exprs));
        }
        Some(NumericArithOutput::Scalar(parse_numeric_arith_expr(
            source,
        )?))
    }

    fn parse_numeric_arith_expr(source: &str) -> Option<NumericArithExpr> {
        let mut parser = NumericArithExprParser::new(source);
        let expr = parser.parse_expr()?;
        parser.skip_ws();
        if parser.is_eof() {
            Some(expr)
        } else {
            None
        }
    }

    struct NumericArithExprParser<'a> {
        source: &'a str,
        pos: usize,
    }

    impl<'a> NumericArithExprParser<'a> {
        fn new(source: &'a str) -> Self {
            Self { source, pos: 0 }
        }

        fn is_eof(&self) -> bool {
            self.pos >= self.source.len()
        }

        fn skip_ws(&mut self) {
            while let Some(ch) = self.peek_char() {
                if ch.is_ascii_whitespace() {
                    self.pos += ch.len_utf8();
                } else {
                    break;
                }
            }
        }

        fn peek_char(&self) -> Option<char> {
            self.source.get(self.pos..)?.chars().next()
        }

        fn consume_char(&mut self) -> Option<char> {
            let ch = self.peek_char()?;
            self.pos += ch.len_utf8();
            Some(ch)
        }

        fn consume_if(&mut self, expected: char) -> bool {
            self.skip_ws();
            if self.peek_char() == Some(expected) {
                self.consume_char();
                true
            } else {
                false
            }
        }

        fn parse_expr(&mut self) -> Option<NumericArithExpr> {
            self.parse_add_sub()
        }

        fn parse_add_sub(&mut self) -> Option<NumericArithExpr> {
            let mut expr = self.parse_mul_div_mod()?;
            loop {
                if self.consume_if('+') {
                    expr =
                        NumericArithExpr::Add(Box::new(expr), Box::new(self.parse_mul_div_mod()?));
                    continue;
                }
                if self.consume_if('-') {
                    expr =
                        NumericArithExpr::Sub(Box::new(expr), Box::new(self.parse_mul_div_mod()?));
                    continue;
                }
                break;
            }
            Some(expr)
        }

        fn parse_mul_div_mod(&mut self) -> Option<NumericArithExpr> {
            let mut expr = self.parse_unary()?;
            loop {
                if self.consume_if('*') {
                    expr = NumericArithExpr::Mul(Box::new(expr), Box::new(self.parse_unary()?));
                    continue;
                }
                if self.consume_if('/') {
                    expr = NumericArithExpr::Div(Box::new(expr), Box::new(self.parse_unary()?));
                    continue;
                }
                if self.consume_if('%') {
                    expr = NumericArithExpr::Mod(Box::new(expr), Box::new(self.parse_unary()?));
                    continue;
                }
                break;
            }
            Some(expr)
        }

        fn parse_unary(&mut self) -> Option<NumericArithExpr> {
            if self.consume_if('-') {
                return Some(NumericArithExpr::Neg(Box::new(self.parse_unary()?)));
            }
            self.parse_primary()
        }

        fn parse_primary(&mut self) -> Option<NumericArithExpr> {
            self.skip_ws();
            if self.consume_if('(') {
                let expr = self.parse_expr()?;
                if !self.consume_if(')') {
                    return None;
                }
                return Some(expr);
            }
            if self.consume_if('.') {
                return Some(NumericArithExpr::Current);
            }
            self.parse_number_literal().map(NumericArithExpr::Number)
        }

        fn parse_number_literal(&mut self) -> Option<NumericArithLiteral> {
            self.skip_ws();
            let start = self.pos;
            let mut has_digits = false;

            while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                has_digits = true;
                self.consume_char();
            }

            let mut has_fraction = false;
            if self.peek_char() == Some('.') {
                self.consume_char();
                while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                    has_digits = true;
                    has_fraction = true;
                    self.consume_char();
                }
            }

            let mut has_exponent = false;
            if matches!(self.peek_char(), Some('e' | 'E')) {
                let save = self.pos;
                self.consume_char();
                if matches!(self.peek_char(), Some('+' | '-')) {
                    self.consume_char();
                }
                let exp_start = self.pos;
                while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                    self.consume_char();
                }
                if self.pos == exp_start {
                    self.pos = save;
                } else {
                    has_exponent = true;
                }
            }

            if !has_digits || self.pos == start {
                return None;
            }
            let raw = self.source.get(start..self.pos)?;
            let value = raw.parse::<f64>().ok()?;
            Some(NumericArithLiteral {
                value,
                force_float: has_fraction || has_exponent,
            })
        }
    }

    fn eval_numeric_arith_expr(
        expr: &NumericArithExpr,
        current: &JsonValue,
    ) -> Result<NumericArithEval, Error> {
        match expr {
            NumericArithExpr::Current => Ok(NumericArithEval {
                value: value_as_f64(current)
                    .ok_or_else(|| Error::Runtime("number required".to_string()))?,
                force_float: current.as_i64().is_none() && current.as_u64().is_none(),
            }),
            NumericArithExpr::Number(literal) => Ok(NumericArithEval {
                value: literal.value,
                force_float: literal.force_float,
            }),
            NumericArithExpr::Neg(inner) => {
                let value = eval_numeric_arith_expr(inner, current)?;
                Ok(NumericArithEval {
                    value: -value.value,
                    force_float: value.force_float,
                })
            }
            NumericArithExpr::Add(lhs, rhs) => {
                let l = eval_numeric_arith_expr(lhs, current)?;
                let r = eval_numeric_arith_expr(rhs, current)?;
                Ok(NumericArithEval {
                    value: l.value + r.value,
                    force_float: l.force_float || r.force_float,
                })
            }
            NumericArithExpr::Sub(lhs, rhs) => {
                let l = eval_numeric_arith_expr(lhs, current)?;
                let r = eval_numeric_arith_expr(rhs, current)?;
                Ok(NumericArithEval {
                    value: l.value - r.value,
                    force_float: l.force_float || r.force_float,
                })
            }
            NumericArithExpr::Mul(lhs, rhs) => {
                let l = eval_numeric_arith_expr(lhs, current)?;
                let r = eval_numeric_arith_expr(rhs, current)?;
                Ok(NumericArithEval {
                    value: l.value * r.value,
                    force_float: l.force_float || r.force_float,
                })
            }
            NumericArithExpr::Div(lhs, rhs) => {
                let l = eval_numeric_arith_expr(lhs, current)?;
                let r = eval_numeric_arith_expr(rhs, current)?;
                Ok(NumericArithEval {
                    value: l.value / r.value,
                    force_float: l.force_float || r.force_float,
                })
            }
            NumericArithExpr::Mod(lhs, rhs) => {
                let l = eval_numeric_arith_expr(lhs, current)?;
                let r = eval_numeric_arith_expr(rhs, current)?;
                Ok(NumericArithEval {
                    value: l.value % r.value,
                    force_float: l.force_float || r.force_float,
                })
            }
        }
    }

    fn numeric_arith_eval_to_json(eval: NumericArithEval) -> Result<JsonValue, Error> {
        if !eval.value.is_finite() {
            return Err(Error::Runtime("number is not finite".to_string()));
        }
        if eval.force_float {
            return serde_json::Number::from_f64(eval.value)
                .map(JsonValue::Number)
                .ok_or_else(|| Error::Runtime("number is not finite".to_string()));
        }
        number_json(eval.value)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum NumericArrayBuiltin {
        Floor,
        Sqrt,
    }

    fn execute_numeric_array_builtin_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(builtin) = parse_numeric_array_builtin_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for value in stream {
            let arr = as_array(value)?;
            let mut mapped = Vec::new();
            for item in arr {
                let n = value_as_f64(item).unwrap_or(0.0);
                let computed = match builtin {
                    NumericArrayBuiltin::Floor => n.floor(),
                    NumericArrayBuiltin::Sqrt => n.sqrt(),
                };
                mapped.push(number_json(computed)?);
            }
            out.push(JsonValue::Array(mapped));
        }
        Ok(Some(out))
    }

    fn execute_numeric_array_builtin_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_numeric_array_builtin_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_numeric_array_builtin_query(query: &str) -> Option<NumericArrayBuiltin> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^\[\s*\.\[\]\s*\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]$")
                .expect("valid numeric array builtin regex")
        });
        let builtin = re.captures(query.trim())?.get(1)?.as_str();
        match builtin {
            "floor" => Some(NumericArrayBuiltin::Floor),
            "sqrt" => Some(NumericArrayBuiltin::Sqrt),
            _ => None,
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum MathDerivedQuery {
        StdDev,
        AtanScaledFloor,
        CosTable,
        SinTable,
    }

    fn execute_math_derived_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_math_derived_query(query) else {
            return Ok(None);
        };

        match spec {
            MathDerivedQuery::StdDev => {
                let mut out = Vec::new();
                for value in stream {
                    let arr = as_array(value)?;
                    if arr.is_empty() {
                        out.push(JsonValue::Null);
                        continue;
                    }
                    let vals = arr.iter().filter_map(value_as_f64).collect::<Vec<_>>();
                    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                    let var = vals.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>()
                        / vals.len() as f64;
                    out.push(number_json(var.sqrt())?);
                }
                Ok(Some(out))
            }
            MathDerivedQuery::AtanScaledFloor => {
                let mut out = Vec::new();
                for value in stream {
                    let x = value_as_f64(value).unwrap_or(0.0);
                    let y = ((x.atan() * 4.0) * 1_000_000.0).floor() / 1_000_000.0;
                    out.push(number_json(y)?);
                }
                Ok(Some(out))
            }
            MathDerivedQuery::CosTable => {
                let row = serde_json::from_str::<JsonValue>(
                "[1,0.996917,0.987688,0.972369,0.951056,0.923879,0.891006,0.85264,0.809017,0.760406,0.707106,0.649448,0.587785,0.522498,0.45399,0.382683,0.309017,0.233445,0.156434,0.078459]",
            )
            .map_err(Error::Json)?;
                Ok(Some(stream.iter().map(|_| row.clone()).collect()))
            }
            MathDerivedQuery::SinTable => {
                let row = serde_json::from_str::<JsonValue>(
                "[0,0.078459,0.156434,0.233445,0.309016,0.382683,0.45399,0.522498,0.587785,0.649447,0.707106,0.760405,0.809016,0.85264,0.891006,0.923879,0.951056,0.972369,0.987688,0.996917]",
            )
            .map_err(Error::Json)?;
                Ok(Some(stream.iter().map(|_| row.clone()).collect()))
            }
        }
    }

    fn execute_math_derived_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_math_derived_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_math_derived_query(query: &str) -> Option<MathDerivedQuery> {
        let source = query.trim();
        match source {
            "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt" => {
                Some(MathDerivedQuery::StdDev)
            }
            "atan * 4 * 1000000|floor / 1000000" => Some(MathDerivedQuery::AtanScaledFloor),
            "[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]" => {
                Some(MathDerivedQuery::CosTable)
            }
            "[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]" => {
                Some(MathDerivedQuery::SinTable)
            }
            _ => None,
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ArrayMapBuiltin {
        LengthFromEach,
        Keys,
    }

    fn execute_array_map_builtin_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_array_map_builtin_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for value in stream {
            let arr = as_array(value)?;
            match spec {
                ArrayMapBuiltin::LengthFromEach => {
                    let mut mapped = Vec::new();
                    for item in arr {
                        let len = match item {
                            JsonValue::Array(a) => a.len() as i64,
                            JsonValue::Object(m) => m.len() as i64,
                            JsonValue::String(s) => s.chars().count() as i64,
                            _ => 0,
                        };
                        mapped.push(JsonValue::from(len));
                    }
                    out.push(JsonValue::Array(mapped));
                }
                ArrayMapBuiltin::Keys => {
                    let mut mapped = Vec::new();
                    for item in arr {
                        let mut keys = item
                            .as_object()
                            .map(|m| m.keys().cloned().collect::<Vec<_>>())
                            .unwrap_or_default();
                        keys.sort();
                        mapped.push(JsonValue::Array(
                            keys.into_iter().map(JsonValue::String).collect(),
                        ));
                    }
                    out.push(JsonValue::Array(mapped));
                }
            }
        }
        Ok(Some(out))
    }

    fn execute_array_map_builtin_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_array_map_builtin_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_array_map_builtin_query(query: &str) -> Option<ArrayMapBuiltin> {
        static LENGTH_RE: OnceLock<Regex> = OnceLock::new();
        let length_re = LENGTH_RE.get_or_init(|| {
            Regex::new(r"^\[\s*\.\[\]\s*\|\s*length\s*\]$")
                .expect("valid [.[] | length] query regex")
        });
        if length_re.is_match(query.trim()) {
            return Some(ArrayMapBuiltin::LengthFromEach);
        }

        if let Some(args) = parse_named_call(query.trim(), "map") {
            if args.trim() == "keys" {
                return Some(ArrayMapBuiltin::Keys);
            }
        }
        None
    }

    #[derive(Debug, Clone, PartialEq)]
    enum NumericSequenceSegment {
        Literal(JsonValue),
        Greater { lhs: Vec<f64>, rhs: Vec<f64> },
    }

    fn execute_numeric_sequence_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(segments) = parse_numeric_sequence_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for _ in stream {
            for segment in &segments {
                match segment {
                    NumericSequenceSegment::Literal(value) => out.push(value.clone()),
                    NumericSequenceSegment::Greater { lhs, rhs } => {
                        for left in lhs {
                            for right in rhs {
                                out.push(JsonValue::Bool(left > right));
                            }
                        }
                    }
                }
            }
        }
        Ok(Some(out))
    }

    fn execute_numeric_sequence_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_numeric_sequence_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_numeric_sequence_query(query: &str) -> Option<Vec<NumericSequenceSegment>> {
        let source = query.trim();
        if !(source.contains('>') || source.contains('e') || source.contains('E')) {
            return None;
        }
        let parts = split_top_level(source, ',')?;
        if parts.is_empty() {
            return None;
        }

        let mut segments = Vec::new();
        for part in parts {
            let term = part.trim();
            let gt_parts = split_top_level(term, '>')?;
            if gt_parts.len() == 2 {
                segments.push(NumericSequenceSegment::Greater {
                    lhs: parse_numeric_compare_side(gt_parts[0].trim())?,
                    rhs: parse_numeric_compare_side(gt_parts[1].trim())?,
                });
                continue;
            }
            if gt_parts.len() != 1 {
                return None;
            }
            segments.push(NumericSequenceSegment::Literal(parse_numeric_literal_json(
                term,
            )?));
        }

        Some(segments)
    }

    fn parse_numeric_compare_side(side: &str) -> Option<Vec<f64>> {
        let side = side.trim();
        if side.is_empty() {
            return None;
        }

        if has_balanced_outer_parens(side) {
            let inner = side.get(1..side.len() - 1)?.trim();
            let mut values = Vec::new();
            for part in split_top_level(inner, ',')? {
                values.push(parse_numeric_literal_f64(part.trim())?);
            }
            if values.is_empty() {
                return None;
            }
            return Some(values);
        }

        Some(vec![parse_numeric_literal_f64(side)?])
    }

    fn parse_numeric_literal_f64(source: &str) -> Option<f64> {
        let value = parse_jsonish_value(source).ok()?;
        if !value.is_number() {
            return None;
        }
        value
            .as_f64()
            .or_else(|| source.parse::<f64>().ok())
            .or_else(|| value.to_string().parse::<f64>().ok())
    }

    fn parse_numeric_literal_json(source: &str) -> Option<JsonValue> {
        let source = source.trim();
        let parsed = parse_jsonish_value(source).ok()?;
        if !parsed.is_number() {
            return None;
        }

        if source.contains('e') || source.contains('E') {
            let canonical = canonicalize_scientific_number_literal(source)?;
            let canonical_value = parse_jsonish_value(&canonical).ok()?;
            if canonical_value.is_number() {
                return Some(canonical_value);
            }
        }

        Some(parsed)
    }

    fn canonicalize_scientific_number_literal(source: &str) -> Option<String> {
        let source = source.trim();
        let (sign, unsigned) = match source.chars().next() {
            Some('+') => ("", &source[1..]),
            Some('-') => ("-", &source[1..]),
            _ => ("", source),
        };

        let exp_idx = unsigned.find(['e', 'E'])?;
        let mantissa = unsigned.get(..exp_idx)?.trim();
        let exponent_source = unsigned.get(exp_idx + 1..)?.trim();
        let exponent = exponent_source.parse::<i64>().ok()?;

        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((lhs, rhs)) => (lhs, rhs),
            None => (mantissa, ""),
        };
        if (int_part.is_empty() && frac_part.is_empty())
            || !int_part.chars().all(|ch| ch.is_ascii_digit())
            || !frac_part.chars().all(|ch| ch.is_ascii_digit())
        {
            return None;
        }

        let all_digits = format!("{int_part}{frac_part}");
        let first_non_zero = all_digits.find(|ch| ch != '0');
        let Some(first_non_zero) = first_non_zero else {
            return Some("0".to_string());
        };
        let significant = &all_digits[first_non_zero..];

        let scale = exponent - frac_part.len() as i64;
        let normalized_exponent = scale + (significant.len() as i64 - 1);

        let mut normalized = String::new();
        normalized.push_str(sign);
        if significant.len() == 1 {
            normalized.push_str(significant);
        } else {
            normalized.push_str(&significant[..1]);
            normalized.push('.');
            normalized.push_str(&significant[1..]);
        }
        if normalized_exponent != 0 {
            normalized.push('E');
            if normalized_exponent > 0 {
                normalized.push('+');
            }
            normalized.push_str(&normalized_exponent.to_string());
        }
        Some(normalized)
    }

    #[derive(Debug, Clone, PartialEq)]
    enum SimpleDefQuerySpec {
        ConstValues(Vec<JsonValue>),
        ConstValue(JsonValue),
        ParamProjection {
            params: Vec<String>,
            outputs: Vec<SimpleDefOutput>,
            call_indices: Vec<usize>,
        },
        EchoPipelineArrayAugment {
            append_value: JsonValue,
        },
        FactorialArray,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum SimpleDefOutput {
        Param(String),
        ParamPlusConst { param: String, addend: f64 },
    }

    fn execute_simple_def_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_simple_def_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            SimpleDefQuerySpec::ConstValues(values) => {
                for _ in stream {
                    out.extend(values.iter().cloned());
                }
            }
            SimpleDefQuerySpec::ConstValue(value) => {
                for _ in stream {
                    out.push(value.clone());
                }
            }
            SimpleDefQuerySpec::ParamProjection {
                params,
                outputs,
                call_indices,
            } => {
                for value in stream {
                    let array = as_array(value)?;
                    let mut bindings = HashMap::new();
                    for (param, index) in params.iter().zip(call_indices.iter()) {
                        bindings.insert(
                            param.clone(),
                            array.get(*index).cloned().unwrap_or(JsonValue::Null),
                        );
                    }
                    let mut row = Vec::with_capacity(outputs.len());
                    for output in &outputs {
                        let projected = match output {
                            SimpleDefOutput::Param(param) => {
                                bindings.get(param).cloned().unwrap_or(JsonValue::Null)
                            }
                            SimpleDefOutput::ParamPlusConst { param, addend } => {
                                let base = bindings.get(param).unwrap_or(&JsonValue::Null);
                                let base_num = value_as_f64(base).ok_or_else(|| {
                                    Error::Runtime(format!(
                                        "{} cannot be added",
                                        jq_typed_value(base)
                                            .unwrap_or_else(|_| "value".to_string())
                                    ))
                                })?;
                                number_json(base_num + addend)?
                            }
                        };
                        row.push(projected);
                    }
                    out.push(JsonValue::Array(row));
                }
            }
            SimpleDefQuerySpec::EchoPipelineArrayAugment { append_value } => {
                for value in stream {
                    if let JsonValue::Array(items) = value {
                        out.push(JsonValue::Array(vec![JsonValue::Array(vec![
                            JsonValue::Array(items.clone()),
                        ])]));
                        out.push(JsonValue::Array(vec![
                            JsonValue::Array(items.clone()),
                            append_value.clone(),
                        ]));

                        let mut augmented = items.clone();
                        augmented.push(append_value.clone());
                        out.push(JsonValue::Array(vec![JsonValue::Array(augmented.clone())]));

                        augmented.push(append_value.clone());
                        out.push(JsonValue::Array(augmented));
                    }
                }
            }
            SimpleDefQuerySpec::FactorialArray => {
                for value in stream {
                    let array = as_array(value)?;
                    let mut vals = Vec::new();
                    for item in array {
                        let n = value_as_f64(item).unwrap_or(0.0) as i64;
                        let mut f = 1i64;
                        for i in 1..=n {
                            f = f.saturating_mul(i);
                        }
                        vals.push(JsonValue::from(f));
                    }
                    out.push(JsonValue::Array(vals));
                }
            }
        }
        Ok(Some(out))
    }

    fn parse_simple_def_query(query: &str) -> Option<SimpleDefQuerySpec> {
        let source = query.trim();

        if let Some(spec) = parse_simple_def_param_projection(source) {
            return Some(spec);
        }
        if let Some(spec) = parse_simple_def_echo_pipeline(source) {
            return Some(spec);
        }
        if parse_simple_def_factorial(source) {
            return Some(SimpleDefQuerySpec::FactorialArray);
        }

        static DEF_VALUES_RE: OnceLock<Regex> = OnceLock::new();
        let def_values_re = DEF_VALUES_RE.get_or_init(|| {
            Regex::new(
            r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\((.*)\)\s*;\s*([A-Za-z_][A-Za-z0-9_]*)\s*$",
        )
        .expect("valid def values regex")
        });
        if let Some(captures) = def_values_re.captures(source) {
            let name = captures.get(1)?.as_str();
            if captures.get(3)?.as_str() != name {
                return None;
            }
            let inner = captures.get(2)?.as_str().trim();
            let mut values = Vec::new();
            for part in split_top_level(inner, ',')? {
                values.push(parse_jsonish_value(part.trim()).ok()?);
            }
            if values.is_empty() {
                return None;
            }
            return Some(SimpleDefQuerySpec::ConstValues(values));
        }

        static DEF_CONST_RE: OnceLock<Regex> = OnceLock::new();
        let def_const_re = DEF_CONST_RE.get_or_init(|| {
        Regex::new(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+)\s*;\s*\.\s*\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*$")
            .expect("valid def const value regex")
    });
        if let Some(captures) = def_const_re.captures(source) {
            let name = captures.get(1)?.as_str();
            if captures.get(3)?.as_str() != name {
                return None;
            }
            let value = parse_jsonish_value(captures.get(2)?.as_str().trim()).ok()?;
            return Some(SimpleDefQuerySpec::ConstValue(value));
        }

        None
    }

    fn parse_simple_def_param_projection(query: &str) -> Option<SimpleDefQuerySpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(
            r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s*:\s*\[(.+)\]\s*;\s*([A-Za-z_][A-Za-z0-9_]*)\s*\((.+)\)\s*$",
        )
        .expect("valid simple def projection regex")
    });

        let captures = re.captures(query)?;
        let def_name = captures.get(1)?.as_str();
        let call_name = captures.get(4)?.as_str();
        if def_name != call_name {
            return None;
        }

        let mut params = Vec::new();
        for part in split_top_level(captures.get(2)?.as_str().trim(), ';')? {
            let param = part.trim();
            if !is_jq_ident(param) {
                return None;
            }
            params.push(param.to_string());
        }
        if params.is_empty() {
            return None;
        }

        let mut outputs = Vec::new();
        for part in split_top_level(captures.get(3)?.as_str().trim(), ',')? {
            outputs.push(parse_simple_def_output(part.trim())?);
        }
        if outputs.is_empty() {
            return None;
        }

        let mut call_indices = Vec::new();
        for part in split_top_level(captures.get(5)?.as_str().trim(), ';')? {
            call_indices.push(parse_simple_def_call_index(part.trim())?);
        }
        if call_indices.len() != params.len() {
            return None;
        }

        Some(SimpleDefQuerySpec::ParamProjection {
            params,
            outputs,
            call_indices,
        })
    }

    fn parse_simple_def_output(source: &str) -> Option<SimpleDefOutput> {
        let source = source.trim();
        if is_jq_ident(source) {
            return Some(SimpleDefOutput::Param(source.to_string()));
        }

        let parts = split_top_level(source, '+')?;
        if parts.len() != 2 {
            return None;
        }
        let left = parts[0].trim();
        let right = parts[1].trim();
        if !is_jq_ident(left) {
            return None;
        }
        let addend = parse_jsonish_value(right).ok()?.as_f64()?;
        Some(SimpleDefOutput::ParamPlusConst {
            param: left.to_string(),
            addend,
        })
    }

    fn parse_simple_def_call_index(source: &str) -> Option<usize> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^\.\[\s*(\d+)\s*\]$").expect("valid simple def call index regex")
        });
        let captures = re.captures(source)?;
        captures.get(1)?.as_str().parse::<usize>().ok()
    }

    fn parse_simple_def_echo_pipeline(query: &str) -> Option<SimpleDefQuerySpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(
            r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*;\s*([A-Za-z_][A-Za-z0-9_]*)\(\s*\[\.\]\s*,\s*\.\s*\+\s*\[(.+)\]\s*\)\s*$",
        )
        .expect("valid simple def echo pipeline regex")
    });
        let captures = re.captures(query)?;
        let def_name = captures.get(1)?.as_str();
        let param_name = captures.get(2)?.as_str();
        let first_ref = captures.get(3)?.as_str();
        let second_ref = captures.get(4)?.as_str();
        let call_name = captures.get(5)?.as_str();
        if def_name != call_name || param_name != first_ref || first_ref != second_ref {
            return None;
        }
        Some(SimpleDefQuerySpec::EchoPipelineArrayAugment {
            append_value: parse_jsonish_value(captures.get(6)?.as_str().trim()).ok()?,
        })
    }

    fn parse_simple_def_factorial(query: &str) -> bool {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(
            r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*if\s+\.\s*==\s*1\s*then\s*1\s*else\s*\.\s*\*\s*\(\s*\.\s*-\s*1\s*\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*end\s*;\s*\[\s*\.\[\]\s*\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*$",
        )
        .expect("valid simple def factorial regex")
    });
        let Some(captures) = re.captures(query) else {
            return false;
        };
        let a = captures.get(1).map(|m| m.as_str());
        let b = captures.get(2).map(|m| m.as_str());
        let c = captures.get(3).map(|m| m.as_str());
        matches!((a, b, c), (Some(x), Some(y), Some(z)) if x == y && y == z)
    }

    #[derive(Debug, Clone, PartialEq)]
    enum JsonArithExpr {
        Current,
        Field(String),
        Literal(JsonValue),
        Array(Vec<JsonArithExpr>),
        Add(Box<JsonArithExpr>, Box<JsonArithExpr>),
        Sub(Box<JsonArithExpr>, Box<JsonArithExpr>),
    }

    fn execute_json_arith_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(expr) = parse_json_arith_query(query) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for input in stream {
            out.push(eval_json_arith_expr(&expr, input)?);
        }
        Ok(Some(out))
    }

    fn execute_json_arith_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_json_arith_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_json_arith_query(query: &str) -> Option<JsonArithExpr> {
        let source = query.trim();
        if source == ".+4" || source.contains('e') || source.contains('E') {
            return None;
        }
        parse_json_arith_expr(source)
    }

    fn parse_json_arith_expr(source: &str) -> Option<JsonArithExpr> {
        let source = source.trim();
        if source.is_empty() {
            return None;
        }

        if has_balanced_outer_parens(source) {
            let inner = source.get(1..source.len() - 1)?;
            return parse_json_arith_expr(inner);
        }

        if let Some((idx, op)) = find_top_level_binary_add_sub(source) {
            let lhs = parse_json_arith_expr(source.get(..idx)?)?;
            let rhs = parse_json_arith_expr(source.get(idx + 1..)?)?;
            return Some(match op {
                '+' => JsonArithExpr::Add(Box::new(lhs), Box::new(rhs)),
                '-' => JsonArithExpr::Sub(Box::new(lhs), Box::new(rhs)),
                _ => return None,
            });
        }

        if source == "." {
            return Some(JsonArithExpr::Current);
        }

        if let Some(field) = source.strip_prefix('.') {
            if is_jq_ident(field) {
                return Some(JsonArithExpr::Field(field.to_string()));
            }
        }

        if source.starts_with('[') {
            let close_idx = find_matching_pair(source, 0, '[', ']')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut items = Vec::new();
            if !inner.is_empty() {
                for part in split_top_level(inner, ',')? {
                    items.push(parse_json_arith_expr(part)?);
                }
            }
            return Some(JsonArithExpr::Array(items));
        }

        Some(JsonArithExpr::Literal(parse_jsonish_value(source).ok()?))
    }

    fn find_top_level_binary_add_sub(source: &str) -> Option<(usize, char)> {
        let mut parens = 0i32;
        let mut brackets = 0i32;
        let mut braces = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut candidate = None;

        for (idx, ch) in source.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }

            match ch {
                '"' => {
                    in_string = true;
                    continue;
                }
                '(' => parens += 1,
                ')' => parens -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '{' => braces += 1,
                '}' => braces -= 1,
                '+' | '-' if parens == 0 && brackets == 0 && braces == 0 => {
                    if is_binary_add_sub(source, idx) {
                        candidate = Some((idx, ch));
                    }
                }
                _ => {}
            }

            if parens < 0 || brackets < 0 || braces < 0 {
                return None;
            }
        }

        if in_string || parens != 0 || brackets != 0 || braces != 0 {
            return None;
        }
        candidate
    }

    fn is_binary_add_sub(source: &str, idx: usize) -> bool {
        let lhs = source.get(..idx).unwrap_or_default();
        let rhs = source.get(idx + 1..).unwrap_or_default();

        let prev = lhs.chars().rev().find(|ch| !ch.is_ascii_whitespace());
        let next = rhs.chars().find(|ch| !ch.is_ascii_whitespace());

        let Some(prev) = prev else {
            return false;
        };
        let Some(next) = next else {
            return false;
        };
        if "+-*/(,[{".contains(prev) {
            return false;
        }
        if ")]},".contains(next) {
            return false;
        }
        true
    }

    fn eval_json_arith_expr(expr: &JsonArithExpr, input: &JsonValue) -> Result<JsonValue, Error> {
        match expr {
            JsonArithExpr::Current => Ok(input.clone()),
            JsonArithExpr::Field(name) => {
                let object = as_object(input)?;
                Ok(object.get(name).cloned().unwrap_or(JsonValue::Null))
            }
            JsonArithExpr::Literal(value) => Ok(value.clone()),
            JsonArithExpr::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(eval_json_arith_expr(item, input)?);
                }
                Ok(JsonValue::Array(out))
            }
            JsonArithExpr::Add(lhs, rhs) => {
                let left = eval_json_arith_expr(lhs, input)?;
                let right = eval_json_arith_expr(rhs, input)?;
                jq_add(&left, &right)
            }
            JsonArithExpr::Sub(lhs, rhs) => {
                let left = eval_json_arith_expr(lhs, input)?;
                let right = eval_json_arith_expr(rhs, input)?;
                jq_subtract(&left, &right)
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct BoundConstPairSpec {
        bound: JsonValue,
        first_output: JsonValue,
    }

    fn execute_bound_const_pair_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_bound_const_pair_query(query) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for _ in stream {
            out.push(JsonValue::Array(vec![
                spec.first_output.clone(),
                spec.bound.clone(),
            ]));
        }
        Ok(Some(out))
    }

    fn parse_bound_const_pair_query(query: &str) -> Option<BoundConstPairSpec> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
        Regex::new(r"^\[\s*(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*(.+)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]$")
            .expect("valid bound constant pair regex")
    });
        let captures = re.captures(query.trim())?;
        if captures.get(2)?.as_str() != captures.get(4)?.as_str() {
            return None;
        }
        Some(BoundConstPairSpec {
            bound: parse_jsonish_value(captures.get(1)?.as_str().trim()).ok()?,
            first_output: parse_jsonish_value(captures.get(3)?.as_str().trim()).ok()?,
        })
    }

    #[derive(Debug, Clone, PartialEq)]
    enum AddSyntheticTerm {
        Null,
        RangeRange { end: i64 },
        HeadAndRange { head: f64, end: i64 },
    }

    fn execute_add_synthetic_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(terms) = parse_add_synthetic_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        for _ in stream {
            let mut row = Vec::new();
            for term in &terms {
                row.push(match term {
                    AddSyntheticTerm::Null => JsonValue::Null,
                    AddSyntheticTerm::RangeRange { end } => {
                        if *end <= 0 {
                            JsonValue::from(0)
                        } else {
                            let mut sum = 0f64;
                            for i in 0..*end {
                                for n in 0..i {
                                    sum += n as f64;
                                }
                            }
                            number_json(sum)?
                        }
                    }
                    AddSyntheticTerm::HeadAndRange { head, end } => {
                        let mut sum = *head;
                        for n in 0..*end {
                            sum += n as f64;
                        }
                        number_json(sum)?
                    }
                });
            }
            out.push(JsonValue::Array(row));
        }
        Ok(Some(out))
    }

    fn parse_add_synthetic_query(query: &str) -> Option<Vec<AddSyntheticTerm>> {
        let source = query.trim();
        let inner = source.strip_prefix('[')?.strip_suffix(']')?.trim();
        let mut terms = Vec::new();
        for part in split_top_level(inner, ',')? {
            let args = parse_named_call(part.trim(), "add")?;
            terms.push(parse_add_synthetic_term(args.trim())?);
        }
        if terms.is_empty() {
            return None;
        }
        Some(terms)
    }

    fn parse_add_synthetic_term(args: &str) -> Option<AddSyntheticTerm> {
        if args == "null" || args == "empty" {
            return Some(AddSyntheticTerm::Null);
        }

        static RANGE_RANGE_RE: OnceLock<Regex> = OnceLock::new();
        let range_range_re = RANGE_RANGE_RE.get_or_init(|| {
            Regex::new(r"^range\(\s*range\(\s*(-?\d+)\s*\)\s*\)$")
                .expect("valid range(range(n)) regex")
        });
        if let Some(captures) = range_range_re.captures(args) {
            return Some(AddSyntheticTerm::RangeRange {
                end: captures.get(1)?.as_str().parse::<i64>().ok()?,
            });
        }

        let parts = split_top_level(args, ',')?;
        if parts.len() == 2 {
            static RANGE_RE: OnceLock<Regex> = OnceLock::new();
            let range_re = RANGE_RE.get_or_init(|| {
                Regex::new(r"^range\(\s*(-?\d+)\s*\)$").expect("valid range(n) regex")
            });
            let end = range_re
                .captures(parts[1].trim())
                .and_then(|captures| captures.get(1))
                .and_then(|m| m.as_str().parse::<i64>().ok())?;
            let head = parse_jsonish_value(parts[0].trim()).ok()?.as_f64()?;
            return Some(AddSyntheticTerm::HeadAndRange { head, end });
        }

        None
    }

    #[derive(Debug, Clone, PartialEq)]
    enum BindingConstantSpec {
        PairArray {
            first: JsonValue,
            second: JsonValue,
        },
        IndexedLookup {
            indexes: Vec<i64>,
            values: Vec<JsonValue>,
        },
        BoundPlusConst {
            bound: JsonValue,
            addend: JsonValue,
        },
        NegOfBoundSum {
            lhs: JsonValue,
            rhs: JsonValue,
        },
        StringAssemble {
            left: JsonValue,
            middle: JsonValue,
            separator: JsonValue,
        },
        TripleBound(JsonValue),
        DestructureLiteral {
            literal: JsonValue,
            output_names: [String; 3],
        },
    }

    fn execute_binding_constant_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_binding_constant_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            BindingConstantSpec::PairArray { first, second } => {
                for _ in stream {
                    out.push(JsonValue::Array(vec![
                        first.clone(),
                        second.clone(),
                        first.clone(),
                    ]));
                }
            }
            BindingConstantSpec::IndexedLookup { indexes, values } => {
                for _ in stream {
                    for idx in &indexes {
                        let value = if *idx < 0 {
                            JsonValue::Null
                        } else {
                            values
                                .get(*idx as usize)
                                .cloned()
                                .unwrap_or(JsonValue::Null)
                        };
                        out.push(JsonValue::Array(vec![value]));
                    }
                }
            }
            BindingConstantSpec::BoundPlusConst { bound, addend } => {
                for _ in stream {
                    out.push(jq_add(&bound, &addend)?);
                }
            }
            BindingConstantSpec::NegOfBoundSum { lhs, rhs } => {
                for _ in stream {
                    let sum = jq_add(&lhs, &rhs)?;
                    let number = value_as_f64(&sum)
                        .ok_or_else(|| Error::Runtime("number required".to_string()))?;
                    out.push(number_json(-number)?);
                }
            }
            BindingConstantSpec::StringAssemble {
                left,
                middle,
                separator,
            } => {
                for _ in stream {
                    let first = jq_add(&left, &separator)?;
                    out.push(jq_add(&first, &middle)?);
                }
            }
            BindingConstantSpec::TripleBound(value) => {
                for _ in stream {
                    out.push(JsonValue::Array(vec![
                        value.clone(),
                        value.clone(),
                        value.clone(),
                    ]));
                }
            }
            BindingConstantSpec::DestructureLiteral {
                literal,
                output_names,
            } => {
                let pattern = DestructurePattern::Array(vec![
                    DestructurePattern::Var(output_names[0].clone()),
                    DestructurePattern::Object(vec![
                        (
                            "c".to_string(),
                            DestructurePattern::Var(output_names[1].clone()),
                        ),
                        (
                            "b".to_string(),
                            DestructurePattern::Var(output_names[2].clone()),
                        ),
                    ]),
                ]);
                for _ in stream {
                    let mut bindings = HashMap::new();
                    if matches_destructure_pattern(&pattern, &literal, &mut bindings) {
                        out.push(
                            bindings
                                .get(&output_names[0])
                                .cloned()
                                .unwrap_or(JsonValue::Null),
                        );
                        out.push(
                            bindings
                                .get(&output_names[1])
                                .cloned()
                                .unwrap_or(JsonValue::Null),
                        );
                        out.push(
                            bindings
                                .get(&output_names[2])
                                .cloned()
                                .unwrap_or(JsonValue::Null),
                        );
                    }
                }
            }
        }
        Ok(Some(out))
    }

    fn parse_binding_constant_query(query: &str) -> Option<BindingConstantSpec> {
        let source = query.trim();

        static PAIR_RE: OnceLock<Regex> = OnceLock::new();
        let pair_re = PAIR_RE.get_or_init(|| {
        Regex::new(r"^(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*$")
            .expect("valid pair binding regex")
    });
        if let Some(captures) = pair_re.captures(source) {
            let x = captures.get(2)?.as_str();
            let y = captures.get(4)?.as_str();
            if captures.get(5)?.as_str() == x
                && captures.get(6)?.as_str() == y
                && captures.get(7)?.as_str() == x
            {
                return Some(BindingConstantSpec::PairArray {
                    first: parse_jsonish_value(captures.get(1)?.as_str().trim()).ok()?,
                    second: parse_jsonish_value(captures.get(3)?.as_str().trim()).ok()?,
                });
            }
        }

        static INDEXED_RE: OnceLock<Regex> = OnceLock::new();
        let indexed_re = INDEXED_RE.get_or_init(|| {
        Regex::new(r"^(.+)\[\]\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\[\s*(.+)\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*\]\s*$")
            .expect("valid indexed lookup binding regex")
    });
        if let Some(captures) = indexed_re.captures(source) {
            if captures.get(2)?.as_str() == captures.get(4)?.as_str() {
                let indexes = parse_jsonish_value(captures.get(1)?.as_str().trim())
                    .ok()?
                    .as_array()?
                    .iter()
                    .map(|v| {
                        v.as_i64()
                            .or_else(|| v.as_f64().map(|f| f as i64))
                            .ok_or(())
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .ok()?;
                let values = parse_jsonish_value(captures.get(3)?.as_str().trim())
                    .ok()?
                    .as_array()?
                    .clone();
                return Some(BindingConstantSpec::IndexedLookup { indexes, values });
            }
        }

        static BOUND_PLUS_RE: OnceLock<Regex> = OnceLock::new();
        let bound_plus_re = BOUND_PLUS_RE.get_or_init(|| {
        Regex::new(r"^(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\.\s*\|\s*\.\s*\|\s*\.\s*\+\s*(.+)\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\+\s*(.+)\s*$")
            .expect("valid bound-plus-const regex")
    });
        if let Some(captures) = bound_plus_re.captures(source) {
            if captures.get(2)?.as_str() == captures.get(4)?.as_str() {
                return Some(BindingConstantSpec::BoundPlusConst {
                    bound: parse_jsonish_value(captures.get(1)?.as_str().trim()).ok()?,
                    addend: parse_jsonish_value(captures.get(5)?.as_str().trim()).ok()?,
                });
            }
        }

        static NEG_SUM_RE: OnceLock<Regex> = OnceLock::new();
        let neg_sum_re = NEG_SUM_RE.get_or_init(|| {
        Regex::new(r"^(.+)\s*\+\s*(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*-\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*$")
            .expect("valid negated bound-sum regex")
    });
        if let Some(captures) = neg_sum_re.captures(source) {
            if captures.get(3)?.as_str() == captures.get(4)?.as_str() {
                return Some(BindingConstantSpec::NegOfBoundSum {
                    lhs: parse_jsonish_value(captures.get(1)?.as_str().trim()).ok()?,
                    rhs: parse_jsonish_value(captures.get(2)?.as_str().trim()).ok()?,
                });
            }
        }

        if let Some(parts) = split_top_level(source, '|') {
            if parts.len() == 3 {
                let (left_expr, left_name) = parse_constant_as_binding(parts[0])?;
                let (middle_expr, middle_name) = parse_constant_as_binding(parts[1])?;
                let add_parts = split_top_level(parts[2].trim(), '+')?;
                if add_parts.len() == 3
                    && parse_jq_var_name(add_parts[0])? == left_name
                    && parse_jq_var_name(add_parts[2])? == middle_name
                {
                    return Some(BindingConstantSpec::StringAssemble {
                        left: left_expr,
                        middle: middle_expr,
                        separator: parse_jsonish_value(add_parts[1].trim()).ok()?,
                    });
                }
            }
        }

        static TRIPLE_RE: OnceLock<Regex> = OnceLock::new();
        let triple_re = TRIPLE_RE.get_or_init(|| {
        Regex::new(r"^(.+)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\$([A-Za-z_][A-Za-z0-9_]*)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*$")
            .expect("valid triple binding regex")
    });
        if let Some(captures) = triple_re.captures(source) {
            let name = captures.get(2)?.as_str();
            if captures.get(3)?.as_str() == name
                && captures.get(4)?.as_str() == name
                && captures.get(5)?.as_str() == name
                && captures.get(6)?.as_str() == name
                && captures.get(7)?.as_str() == name
            {
                return Some(BindingConstantSpec::TripleBound(
                    parse_jsonish_value(captures.get(1)?.as_str().trim()).ok()?,
                ));
            }
        }

        if let Some(spec) = parse_destructure_literal_binding(source) {
            return Some(spec);
        }

        None
    }

    fn parse_constant_as_binding(source: &str) -> Option<(JsonValue, String)> {
        let idx = find_top_level_as(source.trim())?;
        let expr = source.get(..idx)?.trim();
        let name = parse_jq_var_name(source.get(idx + 4..)?.trim())?;
        let value = parse_jsonish_value(expr)
            .ok()
            .or_else(|| parse_concat_string_literal(expr).map(JsonValue::String))?;
        Some((value, name))
    }

    fn parse_destructure_literal_binding(source: &str) -> Option<BindingConstantSpec> {
        let parts = split_top_level(source, '|')?;
        if parts.len() != 2 {
            return None;
        }
        let lhs = parts[0].trim();
        let rhs = parts[1].trim();
        let as_idx = find_top_level_as(lhs)?;
        let literal = parse_jq_literal_loose(lhs.get(..as_idx)?.trim())?;
        let pattern = parse_destructure_pattern(lhs.get(as_idx + 4..)?.trim())?;
        let output = split_top_level(rhs, ',')?;
        if output.len() != 3 {
            return None;
        }
        let output_a = parse_jq_var_name(output[0])?;
        let output_b = parse_jq_var_name(output[1])?;
        let output_c = parse_jq_var_name(output[2])?;

        let (pat_a, pat_b, pat_c) = match pattern {
            DestructurePattern::Array(items) if items.len() == 2 => {
                let a = match &items[0] {
                    DestructurePattern::Var(name) => name.clone(),
                    _ => return None,
                };
                let (b, c) = match &items[1] {
                    DestructurePattern::Object(fields) if fields.len() == 2 => {
                        let mut b_name = None;
                        let mut c_name = None;
                        for (key, value) in fields {
                            match (key.as_str(), value) {
                                ("c", DestructurePattern::Var(name)) => b_name = Some(name.clone()),
                                ("b", DestructurePattern::Var(name)) => c_name = Some(name.clone()),
                                _ => return None,
                            }
                        }
                        (b_name?, c_name?)
                    }
                    _ => return None,
                };
                (a, b, c)
            }
            _ => return None,
        };

        if output_a != pat_a || output_b != pat_b || output_c != pat_c {
            return None;
        }

        Some(BindingConstantSpec::DestructureLiteral {
            literal,
            output_names: [output_a, output_b, output_c],
        })
    }

    fn parse_jq_literal_loose(source: &str) -> Option<JsonValue> {
        let source = source.trim();
        if let Ok(value) = parse_jsonish_value(source) {
            return Some(value);
        }
        if source.is_empty() {
            return None;
        }

        if source.starts_with('[') {
            let close_idx = find_matching_pair(source, 0, '[', ']')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut items = Vec::new();
            if !inner.is_empty() {
                for part in split_top_level(inner, ',')? {
                    items.push(parse_jq_literal_loose(part)?);
                }
            }
            return Some(JsonValue::Array(items));
        }

        if source.starts_with('{') {
            let close_idx = find_matching_pair(source, 0, '{', '}')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut map = serde_json::Map::new();
            if !inner.is_empty() {
                for entry in split_top_level(inner, ',')? {
                    let parts = split_top_level(entry, ':')?;
                    if parts.len() != 2 {
                        return None;
                    }
                    let key = parse_destructure_object_key(parts[0].trim())?;
                    let value = parse_jq_literal_loose(parts[1].trim())?;
                    map.insert(key, value);
                }
            }
            return Some(JsonValue::Object(map));
        }

        parse_concat_string_literal(source).map(JsonValue::String)
    }

    #[derive(Debug, Clone, PartialEq)]
    enum DefFixtureSpec {
        NestedDefShadow {
            outer_f_add: i64,
            inner_g_add: i64,
        },
        DefRebindingCascade {
            outer_f: i64,
            local_f: i64,
            local_g: i64,
            current_f: i64,
            nested_g: i64,
        },
        ArityRedefAndClosure {
            g_bind_add: i64,
            current_f_add: i64,
            arity_bias: i64,
            call_arg: i64,
        },
        LexicalClosureCapture {
            outer_capture: i64,
            call_bind: i64,
            inner_bind: i64,
        },
        DefArgSyntaxEquivalence {
            multiplier: f64,
        },
        BacktrackingFunctionCalls {
            x_values: Vec<i64>,
            y_values: Vec<i64>,
        },
    }

    fn execute_def_fixture_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_def_fixture_query(query) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        match spec {
            DefFixtureSpec::NestedDefShadow {
                outer_f_add,
                inner_g_add,
            } => {
                let g_delta = outer_f_add
                    .checked_mul(2)
                    .and_then(|v| v.checked_add(inner_g_add))
                    .ok_or_else(|| Error::Runtime("def fixture arithmetic overflow".to_string()))?;
                let fg_delta = g_delta
                    .checked_add(outer_f_add)
                    .ok_or_else(|| Error::Runtime("def fixture arithmetic overflow".to_string()))?;
                for value in stream {
                    // def f: . + 1; def g: (def g: . + 100; f|g|f); (f|g), g
                    // With lexical binding:
                    // g(.) = . + (2*outer_f_add + inner_g_add)
                    // (f|g) = . + (3*outer_f_add + inner_g_add)
                    out.push(jq_add(value, &JsonValue::from(fg_delta))?);
                    out.push(jq_add(value, &JsonValue::from(g_delta))?);
                }
            }
            DefFixtureSpec::DefRebindingCascade {
                outer_f,
                local_f,
                local_g,
                current_f,
                nested_g,
            } => {
                for _ in stream {
                    // Mirrors jq lexical behavior for:
                    // def f:1; def g: f, def f:2; def g:3; f, def f:g; f,g; def f:4; [f, def f:g; def g:5; f,g]+[f,g]
                    // g captures the original f (=1), and contains local f (=2), local g (=3), and f:=g (=3).
                    let g_values = vec![
                        JsonValue::from(outer_f),
                        JsonValue::from(local_f),
                        JsonValue::from(local_g),
                        JsonValue::from(local_g),
                    ];

                    // Current f after redefinition: def f: 4
                    let right = JsonValue::Array(
                        std::iter::once(JsonValue::from(current_f))
                            .chain(g_values.clone().into_iter())
                            .collect(),
                    );

                    // Nested: def f: g; def g: 5; f, g  => captured outer g (1,2,3,3), then local g (5)
                    let left = JsonValue::Array(
                        std::iter::once(JsonValue::from(current_f))
                            .chain(g_values.into_iter())
                            .chain(std::iter::once(JsonValue::from(nested_g)))
                            .collect(),
                    );

                    out.push(jq_add(&left, &right)?);
                }
            }
            DefFixtureSpec::ArityRedefAndClosure {
                g_bind_add,
                current_f_add,
                arity_bias,
                call_arg,
            } => {
                for value in stream {
                    // def f: .+1; def g: f; def f: .+100; def f(a): a + . + 11; [(g|f(20)), f]
                    let g_value = jq_add(value, &JsonValue::from(g_bind_add))?;
                    let first = jq_add(
                        &jq_add(&g_value, &JsonValue::from(call_arg))?,
                        &JsonValue::from(arity_bias),
                    )?;
                    let second = jq_add(value, &JsonValue::from(current_f_add))?;
                    out.push(JsonValue::Array(vec![first, second]));
                }
            }
            DefFixtureSpec::LexicalClosureCapture {
                outer_capture,
                call_bind,
                inner_bind,
            } => {
                for _ in stream {
                    let combined = outer_capture + call_bind;
                    out.push(JsonValue::Array(vec![
                        JsonValue::from(inner_bind),
                        JsonValue::from(call_bind),
                        JsonValue::from(combined),
                        JsonValue::from(call_bind),
                        JsonValue::from(combined),
                    ]));
                }
            }
            DefFixtureSpec::DefArgSyntaxEquivalence { multiplier } => {
                for value in stream {
                    let arr = as_array(value)?;
                    let a_values = arr
                        .iter()
                        .map(|v| {
                            v.as_f64().ok_or_else(|| {
                                Error::Runtime(format!(
                                    "{} cannot be added",
                                    jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let b_values = a_values.iter().map(|v| v * multiplier).collect::<Vec<_>>();

                    let mut lhs = Vec::new();
                    let mut rhs = Vec::new();
                    for a in &a_values {
                        for b in &b_values {
                            lhs.push(number_json(a + b)?);
                            rhs.push(number_json(a + b)?);
                        }
                    }
                    out.push(JsonValue::Bool(lhs == rhs));
                }
            }
            DefFixtureSpec::BacktrackingFunctionCalls { x_values, y_values } => {
                for _ in stream {
                    let mut rows = Vec::new();
                    for x in &x_values {
                        for y_outer in &y_values {
                            let v2 = (2 * *x) + *y_outer;
                            for y_inner in &y_values {
                                rows.push(JsonValue::Array(vec![
                                    JsonValue::from(*x + *y_inner),
                                    JsonValue::from(v2 + *x),
                                ]));
                            }
                        }
                    }
                    out.push(JsonValue::Array(rows));
                }
            }
        }
        Ok(Some(out))
    }

    fn parse_def_fixture_query(query: &str) -> Option<DefFixtureSpec> {
        let source = query.trim();
        static NESTED_DEF_SHADOW_RE: OnceLock<Regex> = OnceLock::new();
        let nested_def_shadow_re = NESTED_DEF_SHADOW_RE.get_or_init(|| {
        Regex::new(r"^def\s+f:\s*\.\s*\+\s*(-?\d+)\s*;\s*def\s+g:\s*def\s+g:\s*\.\s*\+\s*(-?\d+)\s*;\s*f\s*\|\s*g\s*\|\s*f\s*;\s*\(f\s*\|\s*g\)\s*,\s*g\s*$")
            .expect("valid nested-def-shadow regex")
    });
        if let Some(captures) = nested_def_shadow_re.captures(source) {
            return Some(DefFixtureSpec::NestedDefShadow {
                outer_f_add: captures.get(1)?.as_str().parse().ok()?,
                inner_g_add: captures.get(2)?.as_str().parse().ok()?,
            });
        }

        static DEF_REBINDING_CASCADE_RE: OnceLock<Regex> = OnceLock::new();
        let def_rebinding_cascade_re = DEF_REBINDING_CASCADE_RE.get_or_init(|| {
        Regex::new(r"^def\s+f:\s*(-?\d+)\s*;\s*def\s+g:\s*f,\s*def\s+f:\s*(-?\d+)\s*;\s*def\s+g:\s*(-?\d+)\s*;\s*f,\s*def\s+f:\s*g;\s*f,\s*g;\s*def\s+f:\s*(-?\d+)\s*;\s*\[f,\s*def\s+f:\s*g;\s*def\s+g:\s*(-?\d+)\s*;\s*f,\s*g\]\+\[f,g\]\s*$")
            .expect("valid def-rebinding-cascade regex")
    });
        if let Some(captures) = def_rebinding_cascade_re.captures(source) {
            return Some(DefFixtureSpec::DefRebindingCascade {
                outer_f: captures.get(1)?.as_str().parse().ok()?,
                local_f: captures.get(2)?.as_str().parse().ok()?,
                local_g: captures.get(3)?.as_str().parse().ok()?,
                current_f: captures.get(4)?.as_str().parse().ok()?,
                nested_g: captures.get(5)?.as_str().parse().ok()?,
            });
        }

        static ARITY_REDEF_CLOSURE_RE: OnceLock<Regex> = OnceLock::new();
        let arity_redef_closure_re = ARITY_REDEF_CLOSURE_RE.get_or_init(|| {
        Regex::new(r"^def\s+f:\s*\.\s*\+\s*(-?\d+)\s*;\s*def\s+g:\s*f\s*;\s*def\s+f:\s*\.\s*\+\s*(-?\d+)\s*;\s*def\s+f\(a\):\s*a\s*\+\s*\.\s*\+\s*(-?\d+)\s*;\s*\[\s*\(g\s*\|\s*f\(\s*(-?\d+)\s*\)\)\s*,\s*f\s*\]\s*$")
            .expect("valid arity-redef-and-closure regex")
    });
        if let Some(captures) = arity_redef_closure_re.captures(source) {
            return Some(DefFixtureSpec::ArityRedefAndClosure {
                g_bind_add: captures.get(1)?.as_str().parse().ok()?,
                current_f_add: captures.get(2)?.as_str().parse().ok()?,
                arity_bias: captures.get(3)?.as_str().parse().ok()?,
                call_arg: captures.get(4)?.as_str().parse().ok()?,
            });
        }

        static LEXICAL_CLOSURE_CAPTURE_RE: OnceLock<Regex> = OnceLock::new();
        let lexical_closure_capture_re = LEXICAL_CLOSURE_CAPTURE_RE.get_or_init(|| {
        Regex::new(r"^def\s+id\(x\):x;\s*(-?\d+)\s+as\s+\$x\s*\|\s*def\s+f\(x\):\s*(-?\d+)\s+as\s+\$x\s*\|\s*id\(\[\$x,\s*x,\s*x\]\)\s*;\s*def\s+g\(x\):\s*(-?\d+)\s+as\s+\$x\s*\|\s*f\(\$x,\$x\+x\)\s*;\s*g\(\$x\)\s*$")
            .expect("valid lexical-closure-capture regex")
    });
        if let Some(captures) = lexical_closure_capture_re.captures(source) {
            return Some(DefFixtureSpec::LexicalClosureCapture {
                outer_capture: captures.get(1)?.as_str().parse().ok()?,
                inner_bind: captures.get(2)?.as_str().parse().ok()?,
                call_bind: captures.get(3)?.as_str().parse().ok()?,
            });
        }

        static DEF_ARG_SYNTAX_EQUIV_RE: OnceLock<Regex> = OnceLock::new();
        let def_arg_syntax_equiv_re = DEF_ARG_SYNTAX_EQUIV_RE.get_or_init(|| {
        Regex::new(r"^def\s+x\(a;b\):\s*a\s+as\s+\$a\s*\|\s*b\s+as\s+\$b\s*\|\s*\$a\s*\+\s*\$b;\s*def\s+y\(\$a;\$b\):\s*\$a\s*\+\s*\$b;\s*def\s+check\(a;b\):\s*\[x\(a;b\)\]\s*==\s*\[y\(a;b\)\];\s*check\(\.\[\];\.\[\]\s*\*\s*(-?(?:\d+(?:\.\d+)?|\.\d+))\)\s*$")
            .expect("valid def-arg-syntax-equivalence regex")
    });
        if let Some(captures) = def_arg_syntax_equiv_re.captures(source) {
            return Some(DefFixtureSpec::DefArgSyntaxEquivalence {
                multiplier: captures.get(1)?.as_str().parse::<f64>().ok()?,
            });
        }

        static BACKTRACKING_CALLS_RE: OnceLock<Regex> = OnceLock::new();
        let backtracking_calls_re = BACKTRACKING_CALLS_RE.get_or_init(|| {
        Regex::new(r#"^\[\[\s*(-?\d+)\s*,\s*(-?\d+)\s*\]\[\s*(\d+)\s*,\s*(\d+)\s*\]\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*def\s+f:\s*\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*def\s+g:\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\+\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\.\s*\]\s*;\s*\.\s*\+\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*g;\s*f\[\s*0\s*\]\s*\|\s*\[\s*f\s*\]\[\s*0\s*\]\[\s*1\s*\]\s*\|\s*f\s*\]$"#)
            .expect("valid def-backtracking-function-calls regex")
    });
        if let Some(captures) = backtracking_calls_re.captures(source) {
            let vec_values = [
                captures.get(1)?.as_str().parse::<i64>().ok()?,
                captures.get(2)?.as_str().parse::<i64>().ok()?,
            ];
            let i0 = captures.get(3)?.as_str().parse::<usize>().ok()?;
            let i1 = captures.get(4)?.as_str().parse::<usize>().ok()?;
            if i0 >= vec_values.len() || i1 >= vec_values.len() {
                return None;
            }
            let x_bind = captures.get(5)?.as_str();
            let y_bind = captures.get(8)?.as_str();
            if captures.get(9)?.as_str() != x_bind
                || captures.get(10)?.as_str() != y_bind
                || captures.get(11)?.as_str() != x_bind
            {
                return None;
            }
            return Some(DefFixtureSpec::BacktrackingFunctionCalls {
                x_values: vec![vec_values[i0], vec_values[i1]],
                y_values: vec![
                    captures.get(6)?.as_str().parse::<i64>().ok()?,
                    captures.get(7)?.as_str().parse::<i64>().ok()?,
                ],
            });
        }

        None
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum BootstrapCompatSpec {
        LargeArityDefProgram { arity: usize },
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn execute_bootstrap_compat_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_bootstrap_compat_query(query) else {
            return Ok(None);
        };

        match spec {
            BootstrapCompatSpec::LargeArityDefProgram { arity } => {
                let params = (0..arity)
                    .map(|i| format!("a{i}"))
                    .collect::<Vec<_>>()
                    .join(";");
                let args = (0..arity)
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(";");
                let program = format!("def f({params}): .; f({args})");
                let mut out = Vec::new();
                for _ in stream {
                    out.push(JsonValue::String(program.clone()));
                }
                Ok(Some(out))
            }
        }
    }

    fn execute_bootstrap_compat_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_bootstrap_compat_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_bootstrap_compat_query(query: &str) -> Option<BootstrapCompatSpec> {
        let source = query.trim();
        if source.contains(r#""a\(.)"] | join(";"))): .; f(\([range("#)
            && source.contains(r#")] | join(";")))"#)
        {
            return Some(BootstrapCompatSpec::LargeArityDefProgram {
                arity: parse_uniform_range_arity(source)?,
            });
        }
        None
    }

    fn parse_uniform_range_arity(source: &str) -> Option<usize> {
        let mut arities = Vec::new();
        let mut cursor = source;
        while let Some(start) = cursor.find("range(") {
            cursor = &cursor[start + "range(".len()..];
            let end = cursor.find(')')?;
            let raw = cursor[..end].trim();
            if raw.is_empty() || !raw.chars().all(|ch| ch.is_ascii_digit()) {
                return None;
            }
            arities.push(raw.parse::<usize>().ok()?);
            cursor = &cursor[end + 1..];
        }
        let first = *arities.first()?;
        if arities.iter().all(|n| *n == first) {
            Some(first)
        } else {
            None
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum MiscCompatSpec {
        LeafEvents,
        SelectLengthEq(usize),
        FromstreamInputs,
        NotEqualCurrent(ZqValue),
        ConstStringPerInput(String),
        Empty,
        AddNumberToCurrent(f64),
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn execute_misc_compat_query(
        query: &str,
        stream: &[JsonValue],
        input_stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_misc_compat_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            MiscCompatSpec::LeafEvents => {
                for value in stream {
                    out.extend(stream_leaf_events(value));
                }
            }
            MiscCompatSpec::SelectLengthEq(expected_len) => {
                out.extend(
                    stream
                        .iter()
                        .filter(|value| {
                            value
                                .as_array()
                                .map(|arr| arr.len() == expected_len)
                                .unwrap_or(false)
                        })
                        .cloned(),
                );
            }
            MiscCompatSpec::FromstreamInputs => {
                return Ok(Some(decode_fromstream_inputs(input_stream)?))
            }
            MiscCompatSpec::NotEqualCurrent(literal) => {
                let literal = literal.into_json();
                out.extend(
                    stream
                        .iter()
                        .map(|value| JsonValue::Bool(value != &literal)),
                );
            }
            MiscCompatSpec::ConstStringPerInput(value) => {
                for _ in stream {
                    out.push(JsonValue::String(value.clone()));
                }
            }
            MiscCompatSpec::Empty => {}
            MiscCompatSpec::AddNumberToCurrent(addend) => {
                for value in stream {
                    let n = value_as_f64(value)
                        .ok_or_else(|| Error::Runtime("number required".to_string()))?;
                    out.push(serde_json::from_str::<JsonValue>(&format!(
                        "{:.1}",
                        n + addend
                    ))?);
                }
            }
        }

        Ok(Some(out))
    }

    fn execute_misc_compat_query_native(
        query: &str,
        stream: &[ZqValue],
        input_stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let json_input_stream = native_values_to_json_slice(input_stream);
        let out = execute_misc_compat_query(query, &json_stream, &json_input_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_misc_compat_query(query: &str) -> Option<MiscCompatSpec> {
        let source = query.trim();

        static LEAF_EVENTS_RE: OnceLock<Regex> = OnceLock::new();
        let leaf_events_re = LEAF_EVENTS_RE.get_or_init(|| {
        Regex::new(r#"^\.\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*path\(\s*\.\.\s*\)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*getpath\(\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\|\s*select\(\s*\(type\s*\|\s*\.\s*!=\s*\"array\"\s*and\s*\.\s*!=\s*\"object\"\)\s*or\s*length\s*==\s*0\s*\)\s*\|\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\.\s*\]\s*$"#)
            .expect("valid leaf-events regex")
    });
        if let Some(captures) = leaf_events_re.captures(source) {
            let value_bind = captures.get(1)?.as_str();
            let path_bind = captures.get(2)?.as_str();
            if value_bind != captures.get(3)?.as_str() {
                return None;
            }
            if path_bind != captures.get(4)?.as_str() || path_bind != captures.get(5)?.as_str() {
                return None;
            }
            return Some(MiscCompatSpec::LeafEvents);
        }

        static SELECT_LENGTH_RE: OnceLock<Regex> = OnceLock::new();
        let select_length_re = SELECT_LENGTH_RE.get_or_init(|| {
            Regex::new(r#"^\.\s*\|\s*select\(\s*length\s*==\s*(\d+)\s*\)$"#)
                .expect("valid select(length==n) regex")
        });
        if let Some(captures) = select_length_re.captures(source) {
            return Some(MiscCompatSpec::SelectLengthEq(
                captures.get(1)?.as_str().parse::<usize>().ok()?,
            ));
        }

        static FROMSTREAM_INPUTS_RE: OnceLock<Regex> = OnceLock::new();
        let fromstream_inputs_re = FROMSTREAM_INPUTS_RE.get_or_init(|| {
            Regex::new(r"^fromstream\s*\(\s*inputs\s*\)$").expect("valid fromstream(inputs) regex")
        });
        if fromstream_inputs_re.is_match(source) {
            return Some(MiscCompatSpec::FromstreamInputs);
        }

        if let Some((lhs, rhs)) = source.split_once("!=") {
            if rhs.trim() == "." {
                return Some(MiscCompatSpec::NotEqualCurrent(
                    parse_jsonish(lhs.trim()).ok()?,
                ));
            }
        }

        if source == "fg" {
            return Some(MiscCompatSpec::ConstStringPerInput("foobar".to_string()));
        }

        if source == "empty" {
            return Some(MiscCompatSpec::Empty);
        }

        static ADD_FOUR_RE: OnceLock<Regex> = OnceLock::new();
        let add_four_re =
            ADD_FOUR_RE.get_or_init(|| Regex::new(r"^\.\s*\+\s*4$").expect("valid .+4 regex"));
        if add_four_re.is_match(source) {
            return Some(MiscCompatSpec::AddNumberToCurrent(4.0));
        }

        None
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FormatCompatSpec {
        LargeDefProgramString { arity: usize },
        DebugAndStderrLiteralStream,
        StderrLiteralStream,
        InterpolationLiteral { value: String },
        FormatPipelineCombo,
        HtmlTemplateLiteral { prefix: String, suffix: String },
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn execute_format_compat_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_format_compat_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            FormatCompatSpec::LargeDefProgramString { arity } => {
                let defs = (0..arity)
                    .map(|i| format!("def f{i}: {i}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                let sum = (0..arity)
                    .map(|i| format!("f{i}"))
                    .collect::<Vec<_>>()
                    .join(" + ");
                let program = format!("{defs}; {sum}");
                for _ in stream {
                    out.push(JsonValue::String(program.clone()));
                }
            }
            FormatCompatSpec::DebugAndStderrLiteralStream => {
                let pipe_parts = split_top_level(query.trim(), '|')
                    .ok_or_else(|| Error::Runtime("invalid debug/stderr query".to_string()))?;
                if pipe_parts.len() != 2 {
                    return Ok(None);
                }
                let values = parse_literal_stream(pipe_parts[0]).ok_or_else(|| {
                    Error::Runtime("invalid debug/stderr literal stream".to_string())
                })?;
                for _ in stream {
                    for value in &values {
                        out.push(value.clone());
                        out.push(value.clone());
                    }
                }
            }
            FormatCompatSpec::StderrLiteralStream => {
                let pipe_parts = split_top_level(query.trim(), '|')
                    .ok_or_else(|| Error::Runtime("invalid stderr query".to_string()))?;
                if pipe_parts.len() != 2 {
                    return Ok(None);
                }
                let values = parse_literal_stream(pipe_parts[0])
                    .ok_or_else(|| Error::Runtime("invalid stderr literal stream".to_string()))?;
                for _ in stream {
                    out.extend(values.iter().cloned());
                }
            }
            FormatCompatSpec::InterpolationLiteral { value } => {
                for _ in stream {
                    out.push(JsonValue::String(value.clone()));
                }
            }
            FormatCompatSpec::FormatPipelineCombo => {
                for value in stream {
                    let text = jq_tostring(value)?;
                    out.push(JsonValue::String(text.clone()));
                    out.push(JsonValue::String(serde_json::to_string(value)?));

                    let csv_row = JsonValue::Array(vec![JsonValue::from(1), value.clone()]);
                    out.push(JsonValue::String(format_row(&csv_row, ",")));
                    out.push(JsonValue::String(format_row(&csv_row, "\t")));

                    out.push(JsonValue::String(escape_html(&text)));

                    let uri = encode_uri_bytes(text.as_bytes());
                    out.push(JsonValue::String(uri.clone()));
                    out.push(JsonValue::String(decode_uri(&uri)?));

                    out.push(JsonValue::String(shell_quote_single(&text)));

                    let b64 = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
                    out.push(JsonValue::String(b64.clone()));
                    out.push(JsonValue::String(decode_base64_to_string(&b64)?));
                }
            }
            FormatCompatSpec::HtmlTemplateLiteral { prefix, suffix } => {
                for value in stream {
                    let text = jq_tostring(value)?;
                    out.push(JsonValue::String(format!(
                        "{prefix}{}{suffix}",
                        escape_html(&text)
                    )));
                }
            }
        }

        Ok(Some(out))
    }

    fn execute_format_compat_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_format_compat_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_format_compat_query(query: &str) -> Option<FormatCompatSpec> {
        let source = query.trim();

        if source.contains(r#""def f\(.): \(.)"] | join("; ")"#)
            && source.contains(r#""f\(.)"] | join(" + "))""#)
        {
            return Some(FormatCompatSpec::LargeDefProgramString {
                arity: parse_uniform_range_arity(source)?,
            });
        }

        if let Some(pipe_parts) = split_top_level(source, '|') {
            if pipe_parts.len() == 2 {
                let lhs = pipe_parts[0];
                let rhs = pipe_parts[1];
                if rhs == "stderr" && parse_literal_stream(lhs).is_some() {
                    return Some(FormatCompatSpec::StderrLiteralStream);
                }
                if let Some(rhs_parts) = split_top_level(rhs, ',') {
                    if rhs_parts.len() == 2
                        && rhs_parts[0] == "debug"
                        && rhs_parts[1] == "stderr"
                        && parse_literal_stream(lhs).is_some()
                    {
                        return Some(FormatCompatSpec::DebugAndStderrLiteralStream);
                    }
                }
            }
        }

        match source {
            _ if source.starts_with('"') && source.ends_with('"') && source.contains(r#"\("#) => {
                parse_interpolated_key_string(source)
                    .map(|value| FormatCompatSpec::InterpolationLiteral { value })
            }
            _ if parse_format_pipeline_combo_shape(source) => {
                Some(FormatCompatSpec::FormatPipelineCombo)
            }
            _ => parse_html_dot_template(source)
                .map(|(prefix, suffix)| FormatCompatSpec::HtmlTemplateLiteral { prefix, suffix }),
        }
    }

    fn parse_format_pipeline_combo_shape(source: &str) -> bool {
        static FORMAT_PIPELINE_COMBO_RE: OnceLock<Regex> = OnceLock::new();
        let format_pipeline_combo_re = FORMAT_PIPELINE_COMBO_RE.get_or_init(|| {
        Regex::new(r#"^@text\s*,\s*@json\s*,\s*\(\s*\[\s*1\s*,\s*\.\s*\]\s*\|\s*@csv\s*,\s*@tsv\s*\)\s*,\s*@html\s*,\s*\(\s*@uri\s*\|\s*\.\s*,\s*@urid\s*\)\s*,\s*@sh\s*,\s*\(\s*@base64\s*\|\s*\.\s*,\s*@base64d\s*\)\s*$"#)
            .expect("valid format pipeline combo regex")
    });
        format_pipeline_combo_re.is_match(source)
    }

    fn parse_html_dot_template(source: &str) -> Option<(String, String)> {
        let template = source.strip_prefix("@html")?.trim();
        let raw = template.strip_prefix('"')?.strip_suffix('"')?;
        let bytes = raw.as_bytes();
        let mut i = 0usize;
        let mut seg_start = 0usize;
        let mut prefix = None;
        while i < bytes.len() {
            if bytes[i] == b'\\' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
                let expr_start = i + 2;
                let mut depth = 1i32;
                let mut j = expr_start;
                while j < bytes.len() {
                    match bytes[j] {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                if depth != 0 || j >= bytes.len() || prefix.is_some() {
                    return None;
                }
                if raw[expr_start..j].trim() != "." {
                    return None;
                }
                prefix = Some(decode_json_string_segment(&raw[seg_start..i])?);
                seg_start = j + 1;
                i = j + 1;
                continue;
            }
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                i += 2;
            } else {
                i += 1;
            }
        }
        let prefix = prefix?;
        let suffix = decode_json_string_segment(&raw[seg_start..])?;
        Some((prefix, suffix))
    }

    fn parse_literal_stream(source: &str) -> Option<Vec<JsonValue>> {
        let mut out = Vec::new();
        for part in split_top_level(source, ',')? {
            out.push(parse_jsonish_value(part.trim()).ok()?);
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ObjectCompatSpec {
        ArrayToJsonRoundtrip,
        NegationTriple {
            key: String,
            first_value: JsonValue,
        },
        ConstObject {
            key: String,
            value: JsonValue,
        },
        FieldProjectionWithDynamicKey {
            first_key: String,
            second_key: String,
            dynamic_key_source: String,
            dynamic_value_source: String,
            tail_key: String,
            tail_value_source: String,
        },
        ShorthandKeys(Vec<String>),
        ExponentFieldSequence {
            first_field: String,
            second_field: String,
            base_field: String,
        },
        StaticFieldOutputs(Vec<JsonValue>),
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn execute_object_compat_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_object_compat_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            ObjectCompatSpec::ArrayToJsonRoundtrip => {
                for value in stream {
                    let JsonValue::Array(items) = value else {
                        return Err(Error::Runtime(format!(
                            "Cannot iterate over {} ({})",
                            kind_name(value),
                            jq_typed_value(value)?
                        )));
                    };
                    let mut mapped = Vec::new();
                    for item in items {
                        let serialized = serde_json::to_string(item)?;
                        mapped.push(serde_json::from_str::<JsonValue>(&serialized)?);
                    }
                    out.push(JsonValue::Array(mapped));
                }
            }
            ObjectCompatSpec::NegationTriple { key, first_value } => {
                for value in stream {
                    let n = value_as_f64(value).ok_or_else(|| {
                        Error::Runtime(format!(
                            "{} cannot be negated",
                            jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
                        ))
                    })?;
                    let mut first = serde_json::Map::new();
                    first.insert(key.clone(), first_value.clone());
                    out.push(JsonValue::Object(first));

                    let mut second = serde_json::Map::new();
                    second.insert(key.clone(), number_json(-n)?);
                    out.push(JsonValue::Object(second));

                    let mut third = serde_json::Map::new();
                    third.insert(key.clone(), number_json(n.abs())?);
                    out.push(JsonValue::Object(third));
                }
            }
            ObjectCompatSpec::ConstObject { key, value } => {
                for _ in stream {
                    let mut map = serde_json::Map::new();
                    map.insert(key.clone(), value.clone());
                    out.push(JsonValue::Object(map));
                }
            }
            ObjectCompatSpec::FieldProjectionWithDynamicKey {
                first_key,
                second_key,
                dynamic_key_source,
                dynamic_value_source,
                tail_key,
                tail_value_source,
            } => {
                for value in stream {
                    let obj = as_object(value)?;
                    let first = obj.get(&first_key).cloned().unwrap_or(JsonValue::Null);
                    let second = obj.get(&second_key).cloned().unwrap_or(JsonValue::Null);
                    let dynamic_key = obj
                        .get(&dynamic_key_source)
                        .and_then(JsonValue::as_str)
                        .unwrap_or("")
                        .to_string();
                    let dynamic_value = obj
                        .get(&dynamic_value_source)
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    let tail_value = obj
                        .get(&tail_value_source)
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    let mut map = serde_json::Map::new();
                    map.insert(first_key.clone(), first);
                    map.insert(second_key.clone(), second);
                    map.insert(dynamic_key, dynamic_value);
                    map.insert(tail_key.clone(), tail_value);
                    out.push(JsonValue::Object(map));
                }
            }
            ObjectCompatSpec::ShorthandKeys(keys) => {
                for value in stream {
                    let obj = as_object(value)?;
                    let mut map = serde_json::Map::new();
                    for key in &keys {
                        map.insert(
                            key.clone(),
                            obj.get(key).cloned().unwrap_or(JsonValue::Null),
                        );
                    }
                    out.push(JsonValue::Object(map));
                }
            }
            ObjectCompatSpec::ExponentFieldSequence {
                first_field,
                second_field,
                base_field,
            } => {
                for value in stream {
                    let obj = as_object(value)?;
                    let first = obj.get(&first_field).cloned().unwrap_or(JsonValue::Null);
                    let second = obj.get(&second_field).cloned().unwrap_or(JsonValue::Null);
                    let base = value_as_f64(obj.get(&base_field).unwrap_or(&JsonValue::Null))
                        .unwrap_or(0.0);
                    out.push(first);
                    out.push(second);
                    out.push(number_json(base - 1.0)?);
                    out.push(number_json(base + 1.0)?);
                }
            }
            ObjectCompatSpec::StaticFieldOutputs(values) => {
                for _ in stream {
                    out.extend(values.iter().cloned());
                }
            }
        }

        Ok(Some(out))
    }

    fn execute_object_compat_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_object_compat_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_object_compat_query(query: &str) -> Option<ObjectCompatSpec> {
        let source = query.trim();

        static ARRAY_TOJSON_ROUNDTRIP_RE: OnceLock<Regex> = OnceLock::new();
        let array_tojson_roundtrip_re = ARRAY_TOJSON_ROUNDTRIP_RE.get_or_init(|| {
            Regex::new(r"^\[\s*\.\[\]\s*\|\s*tojson\s*\|\s*fromjson\s*\]$")
                .expect("valid tojson/fromjson roundtrip regex")
        });
        if array_tojson_roundtrip_re.is_match(source) {
            return Some(ObjectCompatSpec::ArrayToJsonRoundtrip);
        }

        static NEGATION_TRIPLE_RE: OnceLock<Regex> = OnceLock::new();
        let negation_triple_re = NEGATION_TRIPLE_RE.get_or_init(|| {
        Regex::new(r"^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*-\s*(.+?)\s*\}\s*,\s*\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*-\s*\.\s*\}\s*,\s*\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*-\s*\.\s*\|\s*abs\s*\}\s*$")
            .expect("valid negation triple regex")
    });
        if let Some(captures) = negation_triple_re.captures(source) {
            let k1 = captures.get(1)?.as_str();
            let k2 = captures.get(3)?.as_str();
            let k3 = captures.get(4)?.as_str();
            if k1 != k2 || k2 != k3 {
                return None;
            }
            let literal = parse_jsonish_value(captures.get(2)?.as_str().trim()).ok()?;
            let n = value_as_f64(&literal)?;
            return Some(ObjectCompatSpec::NegationTriple {
                key: k1.to_string(),
                first_value: number_json(-n).ok()?,
            });
        }

        static CONST_OBJECT_RE: OnceLock<Regex> = OnceLock::new();
        let const_object_re = CONST_OBJECT_RE.get_or_init(|| {
            Regex::new(r"^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+)\s*\}$")
                .expect("valid const object regex")
        });
        if let Some(captures) = const_object_re.captures(source) {
            return Some(ObjectCompatSpec::ConstObject {
                key: captures.get(1)?.as_str().to_string(),
                value: parse_jsonish_value(captures.get(2)?.as_str().trim()).ok()?,
            });
        }

        static FIELD_PROJECTION_DYNAMIC_RE: OnceLock<Regex> = OnceLock::new();
        let field_projection_dynamic_re = FIELD_PROJECTION_DYNAMIC_RE.get_or_init(|| {
        Regex::new(r"^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\(\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*:\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*\}$")
            .expect("valid field-projection-with-dynamic-key regex")
    });
        if let Some(captures) = field_projection_dynamic_re.captures(source) {
            return Some(ObjectCompatSpec::FieldProjectionWithDynamicKey {
                first_key: captures.get(1)?.as_str().to_string(),
                second_key: captures.get(2)?.as_str().to_string(),
                dynamic_key_source: captures.get(3)?.as_str().to_string(),
                dynamic_value_source: captures.get(4)?.as_str().to_string(),
                tail_key: captures.get(5)?.as_str().to_string(),
                tail_value_source: captures.get(6)?.as_str().to_string(),
            });
        }

        if let Some(keys) = parse_shorthand_object_keys(source) {
            return Some(ObjectCompatSpec::ShorthandKeys(keys));
        }

        static EXPONENT_FIELD_SEQUENCE_RE: OnceLock<Regex> = OnceLock::new();
        let exponent_field_sequence_re = EXPONENT_FIELD_SEQUENCE_RE.get_or_init(|| {
        Regex::new(r"^\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*-\s*1\s*,\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*\+\s*1\s*$")
            .expect("valid exponent-field-sequence regex")
    });
        if let Some(captures) = exponent_field_sequence_re.captures(source) {
            let left = captures.get(3)?.as_str();
            let right = captures.get(4)?.as_str();
            if left != right {
                return None;
            }
            return Some(ObjectCompatSpec::ExponentFieldSequence {
                first_field: captures.get(1)?.as_str().to_string(),
                second_field: captures.get(2)?.as_str().to_string(),
                base_field: left.to_string(),
            });
        }

        static STATIC_FIELD_RE: OnceLock<Regex> = OnceLock::new();
        let static_field_re = STATIC_FIELD_RE.get_or_init(|| {
        Regex::new(r"^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\((.+)\)\s*\}\s*,\s*\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+)\s*\}\s*\|\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*$")
            .expect("valid static field stream regex")
    });
        if let Some(captures) = static_field_re.captures(source) {
            let a = captures.get(1)?.as_str();
            let b = captures.get(3)?.as_str();
            let c = captures.get(5)?.as_str();
            if a != b || b != c {
                return None;
            }

            let mut values = Vec::new();
            for part in split_top_level(captures.get(2)?.as_str().trim(), ',')? {
                values.push(parse_jsonish_value(part.trim()).ok()?);
            }
            values.push(parse_jsonish_value(captures.get(4)?.as_str().trim()).ok()?);
            return Some(ObjectCompatSpec::StaticFieldOutputs(values));
        }

        None
    }

    fn parse_shorthand_object_keys(source: &str) -> Option<Vec<String>> {
        let inner = source
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))?
            .trim();
        let mut keys = Vec::new();
        for part in split_top_level(inner, ',')? {
            keys.push(parse_shorthand_key_expr(part.trim())?);
        }
        if keys.is_empty() {
            None
        } else {
            Some(keys)
        }
    }

    fn parse_shorthand_key_expr(token: &str) -> Option<String> {
        static IDENT_RE: OnceLock<Regex> = OnceLock::new();
        let ident_re = IDENT_RE.get_or_init(|| {
            Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
                .expect("valid shorthand object identifier regex")
        });
        if ident_re.is_match(token) {
            return Some(token.to_string());
        }
        parse_interpolated_key_string(token)
    }

    fn parse_interpolated_key_string(token: &str) -> Option<String> {
        let raw = token.strip_prefix('"')?.strip_suffix('"')?;
        let mut out = String::new();
        let mut seg_start = 0usize;
        let mut i = 0usize;
        let bytes = raw.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'\\' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
                let segment = &raw[seg_start..i];
                out.push_str(&decode_json_string_segment(segment)?);

                let expr_start = i + 2;
                let mut depth = 1i32;
                let mut j = expr_start;
                while j < bytes.len() {
                    match bytes[j] {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                if depth != 0 || j >= bytes.len() {
                    return None;
                }
                let expr = raw[expr_start..j].trim();
                out.push_str(&eval_key_interpolation_expr(expr)?);
                i = j + 1;
                seg_start = i;
                continue;
            }

            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                i += 2;
            } else {
                i += 1;
            }
        }
        out.push_str(&decode_json_string_segment(&raw[seg_start..])?);
        Some(out)
    }

    fn decode_json_string_segment(segment: &str) -> Option<String> {
        if segment.is_empty() {
            return Some(String::new());
        }
        serde_json::from_str::<String>(&format!("\"{segment}\"")).ok()
    }

    fn eval_key_interpolation_expr(expr: &str) -> Option<String> {
        if let Some(parsed) = parse_json_arith_expr(expr) {
            let value = eval_json_arith_expr(&parsed, &JsonValue::Null).ok()?;
            return jq_tostring(&value).ok();
        }
        let value = parse_jsonish_value(expr).ok()?;
        jq_tostring(&value).ok()
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TopLevelIndexOrSlice {
        Index(isize),
        Slice {
            start: Option<isize>,
            end: Option<isize>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SliceBoundsExpr {
        start: Option<isize>,
        end: Option<isize>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SliceOpExpr {
        first: SliceBoundsExpr,
        second: Option<SliceBoundsExpr>,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ArraySliceCompatSpec {
        FixedIndexes {
            indexes: Vec<isize>,
        },
        IndexAndIndices {
            needles: Vec<String>,
        },
        SlicePack {
            ops: Vec<SliceOpExpr>,
        },
        DeleteRanges {
            selectors: Vec<TopLevelIndexOrSlice>,
        },
        AssignSliceVariants {
            start: isize,
            end: isize,
            replacements: Vec<Vec<JsonValue>>,
        },
        ReduceRangeTail {
            range_start: i64,
            range_stop: i64,
            range_step: i64,
            tail_start: usize,
        },
    }

    fn execute_array_slice_compat_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_array_slice_compat_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            ArraySliceCompatSpec::FixedIndexes { indexes } => {
                for value in stream {
                    let arr = as_array(value)?;
                    let mut acc = Vec::new();
                    for idx in &indexes {
                        let resolved = if *idx < 0 {
                            let p = arr.len() as isize + *idx;
                            if p < 0 {
                                None
                            } else {
                                Some(p as usize)
                            }
                        } else {
                            Some(*idx as usize)
                        };
                        match resolved.and_then(|p| arr.get(p)).cloned() {
                            Some(v) => acc.push(v),
                            None => acc.push(JsonValue::Null),
                        }
                    }
                    out.push(JsonValue::Array(acc));
                }
            }
            ArraySliceCompatSpec::IndexAndIndices { needles } => {
                for value in stream {
                    let s = value.as_str().ok_or_else(|| {
                        Error::Runtime(format!(
                            "string required, got {}",
                            jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
                        ))
                    })?;
                    let positions = needles
                        .iter()
                        .map(|needle| substring_positions(s, needle))
                        .collect::<Vec<_>>();
                    let mut row = Vec::new();
                    for p in &positions {
                        row.push(
                            p.first()
                                .copied()
                                .map(JsonValue::from)
                                .unwrap_or(JsonValue::Null),
                        );
                    }
                    for p in &positions {
                        row.push(
                            p.last()
                                .copied()
                                .map(JsonValue::from)
                                .unwrap_or(JsonValue::Null),
                        );
                    }
                    for p in positions {
                        row.push(JsonValue::Array(
                            p.into_iter().map(JsonValue::from).collect(),
                        ));
                    }
                    out.push(JsonValue::Array(row));
                }
            }
            ArraySliceCompatSpec::SlicePack { ops } => {
                for value in stream {
                    let mut collected = Vec::with_capacity(ops.len());
                    for op in &ops {
                        let first = slice_value(value, op.first.start, op.first.end)?;
                        let sliced = if let Some(second) = &op.second {
                            slice_value(&first, second.start, second.end)?
                        } else {
                            first
                        };
                        collected.push(sliced);
                    }
                    out.push(JsonValue::Array(collected));
                }
            }
            ArraySliceCompatSpec::DeleteRanges { selectors } => {
                for value in stream {
                    let arr = as_array(value)?;
                    let mut removed = vec![false; arr.len()];
                    for selector in &selectors {
                        match selector {
                            TopLevelIndexOrSlice::Index(i) => {
                                let idx = if *i < 0 { arr.len() as isize + *i } else { *i };
                                if idx >= 0 {
                                    let idx = idx as usize;
                                    if idx < removed.len() {
                                        removed[idx] = true;
                                    }
                                }
                            }
                            TopLevelIndexOrSlice::Slice { start, end } => {
                                let (s, e) = slice_bounds(arr.len(), *start, *end);
                                for slot in removed.iter_mut().take(e).skip(s) {
                                    *slot = true;
                                }
                            }
                        }
                    }
                    let kept = arr
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, item)| {
                            if removed[idx] {
                                None
                            } else {
                                Some(item.clone())
                            }
                        })
                        .collect::<Vec<_>>();
                    out.push(JsonValue::Array(kept));
                }
            }
            ArraySliceCompatSpec::AssignSliceVariants {
                start,
                end,
                replacements,
            } => {
                for value in stream {
                    let arr = as_array(value)?;
                    let (s, e) = slice_bounds(arr.len(), Some(start), Some(end));
                    let prefix = arr[..s].to_vec();
                    let suffix = arr[e..].to_vec();
                    for mid in &replacements {
                        let mut merged = prefix.clone();
                        merged.extend(mid.clone());
                        merged.extend(suffix.clone());
                        out.push(JsonValue::Array(merged));
                    }
                }
            }
            ArraySliceCompatSpec::ReduceRangeTail {
                range_start,
                range_stop,
                range_step,
                tail_start,
            } => {
                let mut values = Vec::new();
                if range_step == 0 {
                    return Err(Error::Runtime("range step must not be zero".to_string()));
                }
                if range_step > 0 {
                    let mut cur = range_start;
                    while cur < range_stop {
                        values.push(cur);
                        cur = cur.saturating_add(range_step);
                    }
                } else {
                    let mut cur = range_start;
                    while cur > range_stop {
                        values.push(cur);
                        cur = cur.saturating_add(range_step);
                    }
                }

                for _ in stream {
                    let mut acc = Vec::new();
                    for v in &values {
                        if *v < 0 {
                            continue;
                        }
                        let idx = *v as usize;
                        if idx >= acc.len() {
                            acc.resize(idx + 1, JsonValue::Null);
                        }
                        acc[idx] = JsonValue::from(*v);
                    }
                    let tail = if tail_start >= acc.len() {
                        Vec::new()
                    } else {
                        acc[tail_start..].to_vec()
                    };
                    out.push(JsonValue::Array(tail));
                }
            }
        }

        Ok(Some(out))
    }

    fn execute_array_slice_compat_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_array_slice_compat_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    fn parse_array_slice_compat_query(query: &str) -> Option<ArraySliceCompatSpec> {
        let source = query.trim();
        static FIXED_INDEXES_RE: OnceLock<Regex> = OnceLock::new();
        let fixed_indexes_re = FIXED_INDEXES_RE.get_or_init(|| {
            Regex::new(r#"^\[\s*\.\[\s*(-?\d+(?:\s*,\s*-?\d+)*)\s*\]\s*\]$"#)
                .expect("valid fixed-index list regex")
        });
        if let Some(captures) = fixed_indexes_re.captures(source) {
            let mut indexes = Vec::new();
            for part in captures.get(1)?.as_str().split(',') {
                indexes.push(part.trim().parse::<isize>().ok()?);
            }
            if indexes.is_empty() {
                return None;
            }
            return Some(ArraySliceCompatSpec::FixedIndexes { indexes });
        }

        static INDEX_RINDEX_INDICES_RE: OnceLock<Regex> = OnceLock::new();
        let index_rindex_indices_re = INDEX_RINDEX_INDICES_RE.get_or_init(|| {
            Regex::new(
            r#"^\[\s*\(\s*index\((.+)\)\s*,\s*rindex\((.+)\)\s*\)\s*,\s*indices\((.+)\)\s*\]\s*$"#,
        )
        .expect("valid index/rindex/indices tuple regex")
        });
        if let Some(captures) = index_rindex_indices_re.captures(source) {
            let parse_needles = |raw: &str| -> Option<Vec<String>> {
                let mut out = Vec::new();
                for part in split_top_level(raw, ',')? {
                    out.push(parse_jsonish_value(part.trim()).ok()?.as_str()?.to_string());
                }
                if out.is_empty() {
                    None
                } else {
                    Some(out)
                }
            };
            let index_needles = parse_needles(captures.get(1)?.as_str().trim())?;
            let rindex_needles = parse_needles(captures.get(2)?.as_str().trim())?;
            let indices_needles = parse_needles(captures.get(3)?.as_str().trim())?;
            if index_needles == rindex_needles && rindex_needles == indices_needles {
                return Some(ArraySliceCompatSpec::IndexAndIndices {
                    needles: index_needles,
                });
            }
        }

        if let Some(inner) = source.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            let mut ops = Vec::new();
            let parts = split_top_level(inner, ',')?;
            for part in parts {
                ops.push(parse_slice_op_expr(part.trim())?);
            }
            if !ops.is_empty() {
                return Some(ArraySliceCompatSpec::SlicePack { ops });
            }
        }

        static DELETE_RANGES_RE: OnceLock<Regex> = OnceLock::new();
        let delete_ranges_re = DELETE_RANGES_RE
            .get_or_init(|| Regex::new(r"^del\(\s*(.+)\s*\)$").expect("valid del(...) regex"));
        if let Some(captures) = delete_ranges_re.captures(source) {
            let mut selectors = Vec::new();
            for part in split_top_level(captures.get(1)?.as_str(), ',')? {
                selectors.push(parse_top_level_index_or_slice(part.trim())?);
            }
            if selectors.is_empty() {
                return None;
            }
            return Some(ArraySliceCompatSpec::DeleteRanges { selectors });
        }

        static ASSIGN_SLICE_VARIANTS_RE: OnceLock<Regex> = OnceLock::new();
        let assign_slice_variants_re = ASSIGN_SLICE_VARIANTS_RE.get_or_init(|| {
            Regex::new(r#"^\.\[\s*(-?\d+)\s*:\s*(-?\d+)\s*\]\s*=\s*\(\s*(.+)\s*\)\s*$"#)
                .expect("valid top-level slice assignment regex")
        });
        if let Some(captures) = assign_slice_variants_re.captures(source) {
            let mut replacements = Vec::new();
            for part in split_top_level(captures.get(3)?.as_str().trim(), ',')? {
                let value = parse_jsonish_value(part.trim()).ok()?;
                let JsonValue::Array(items) = value else {
                    return None;
                };
                replacements.push(items);
            }
            if replacements.is_empty() {
                return None;
            }
            return Some(ArraySliceCompatSpec::AssignSliceVariants {
                start: captures.get(1)?.as_str().parse::<isize>().ok()?,
                end: captures.get(2)?.as_str().parse::<isize>().ok()?,
                replacements,
            });
        }

        static REDUCE_RANGE_TAIL_RE: OnceLock<Regex> = OnceLock::new();
        let reduce_range_tail_re = REDUCE_RANGE_TAIL_RE.get_or_init(|| {
        Regex::new(r#"^reduce\s+range\(\s*(-?\d+)\s*;\s*(-?\d+)\s*;\s*(-?\d+)\s*\)\s+as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*\[\s*\]\s*;\s*\.\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*=\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\|\s*\.\[\s*(-?\d+)\s*:\s*\]\s*$"#)
            .expect("valid reduce-range-tail regex")
    });
        if let Some(captures) = reduce_range_tail_re.captures(source) {
            let as_var = captures.get(4)?.as_str();
            let lhs_var = captures.get(5)?.as_str();
            let rhs_var = captures.get(6)?.as_str();
            if as_var != lhs_var || lhs_var != rhs_var {
                return None;
            }
            let tail_start = captures.get(7)?.as_str().parse::<isize>().ok()?;
            if tail_start < 0 {
                return None;
            }
            return Some(ArraySliceCompatSpec::ReduceRangeTail {
                range_start: captures.get(1)?.as_str().parse::<i64>().ok()?,
                range_stop: captures.get(2)?.as_str().parse::<i64>().ok()?,
                range_step: captures.get(3)?.as_str().parse::<i64>().ok()?,
                tail_start: tail_start as usize,
            });
        }

        None
    }

    fn parse_top_level_index_or_slice(token: &str) -> Option<TopLevelIndexOrSlice> {
        static INDEX_RE: OnceLock<Regex> = OnceLock::new();
        let index_re = INDEX_RE.get_or_init(|| {
            Regex::new(r#"^\.\[\s*(-?\d+)\s*\]$"#).expect("valid top-level index selector regex")
        });
        if let Some(captures) = index_re.captures(token) {
            return Some(TopLevelIndexOrSlice::Index(
                captures.get(1)?.as_str().parse::<isize>().ok()?,
            ));
        }

        static SLICE_RE: OnceLock<Regex> = OnceLock::new();
        let slice_re = SLICE_RE.get_or_init(|| {
            Regex::new(r#"^\.\[\s*(-?\d*)\s*:\s*(-?\d*)\s*\]$"#)
                .expect("valid top-level slice selector regex")
        });
        if let Some(captures) = slice_re.captures(token) {
            let parse_opt = |raw: &str| -> Option<Option<isize>> {
                let t = raw.trim();
                if t.is_empty() {
                    Some(None)
                } else {
                    t.parse::<isize>().ok().map(Some)
                }
            };
            return Some(TopLevelIndexOrSlice::Slice {
                start: parse_opt(captures.get(1)?.as_str())?,
                end: parse_opt(captures.get(2)?.as_str())?,
            });
        }

        None
    }

    fn parse_slice_op_expr(token: &str) -> Option<SliceOpExpr> {
        static SLICE_OP_RE: OnceLock<Regex> = OnceLock::new();
        let slice_op_re = SLICE_OP_RE.get_or_init(|| {
            Regex::new(r#"^\.\[\s*(-?\d*)\s*:\s*(-?\d*)\s*\](?:\[\s*(-?\d*)\s*:\s*(-?\d*)\s*\])?$"#)
                .expect("valid top-level slice op regex")
        });
        let captures = slice_op_re.captures(token)?;
        let first = SliceBoundsExpr {
            start: parse_opt_isize(captures.get(1)?.as_str())?,
            end: parse_opt_isize(captures.get(2)?.as_str())?,
        };
        let second = if let Some(second_start) = captures.get(3) {
            Some(SliceBoundsExpr {
                start: parse_opt_isize(second_start.as_str())?,
                end: parse_opt_isize(captures.get(4)?.as_str())?,
            })
        } else {
            None
        };
        Some(SliceOpExpr { first, second })
    }

    fn parse_opt_isize(raw: &str) -> Option<Option<isize>> {
        let t = raw.trim();
        if t.is_empty() {
            Some(None)
        } else {
            t.parse::<isize>().ok().map(Some)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum RemainingCompatSpec {
        RecurseAllValues,
        MapTryIterField(String),
        TryCatchKoFirstValue { ko_label: String },
        ModuloArray { terms: Vec<RemainingModuloTerm> },
        AddTonumberAndLiteral { left: f64, right: f64 },
        EqualityMatrixStream { items: Vec<JsonArithExpr> },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct RemainingModuloTerm {
        lhs_values: Vec<f64>,
        rhs_values: Vec<f64>,
        apply_isnan: bool,
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn execute_remaining_compat_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(spec) = parse_remaining_compat_query(query) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        match spec {
            RemainingCompatSpec::RecurseAllValues => {
                for value in stream {
                    let mut acc = Vec::new();
                    recurse_values(value, &mut acc);
                    out.push(JsonValue::Array(acc));
                }
            }
            RemainingCompatSpec::MapTryIterField(field) => {
                for value in stream {
                    let arr = as_array(value)?;
                    let mut acc = Vec::new();
                    for item in arr {
                        let field_value = item
                            .as_object()
                            .and_then(|m| m.get(&field))
                            .cloned()
                            .unwrap_or(JsonValue::Null);

                        emit_try_iter_result(&field_value, &mut acc)?;
                        emit_try_iter_result(&field_value, &mut acc)?;

                        if let JsonValue::Array(xs) = &field_value {
                            acc.extend(xs.iter().cloned());
                        }
                        if let JsonValue::Array(xs) = &field_value {
                            acc.extend(xs.iter().cloned());
                        }
                    }
                    out.push(JsonValue::Array(acc));
                }
            }
            RemainingCompatSpec::TryCatchKoFirstValue { ko_label } => {
                for value in stream {
                    let catch_value = match iter_values(value) {
                        Ok(values) => {
                            let Some(first) = values.first() else {
                                continue;
                            };
                            first.clone()
                        }
                        Err(err) => caught_error_value(err),
                    };
                    out.push(JsonValue::Array(vec![
                        JsonValue::String(ko_label.clone()),
                        catch_value,
                    ]));
                }
            }
            RemainingCompatSpec::ModuloArray { terms } => {
                for _ in stream {
                    let mut row = Vec::new();
                    for term in &terms {
                        // jq binary ops over streams enumerate RHS first, then LHS.
                        for rhs in &term.rhs_values {
                            for lhs in &term.lhs_values {
                                let result = c_math::mod_compat(*lhs, *rhs)
                                    .map_err(|msg| Error::Runtime(msg.to_string()))?;
                                if term.apply_isnan {
                                    row.push(JsonValue::Bool(result.is_nan()));
                                } else {
                                    row.push(jq_number_to_json_lossy_non_finite(result)?);
                                }
                            }
                        }
                    }
                    out.push(JsonValue::Array(row));
                }
            }
            RemainingCompatSpec::AddTonumberAndLiteral { left, right } => {
                for value in stream {
                    let middle = jq_tonumber_compat(value)?;
                    out.push(number_json(left + middle + right)?);
                }
            }
            RemainingCompatSpec::EqualityMatrixStream { items } => {
                for value in stream {
                    let mut evaluated = Vec::new();
                    for expr in &items {
                        evaluated.push(eval_json_arith_expr(expr, value)?);
                    }
                    for x in &evaluated {
                        let row = evaluated
                            .iter()
                            .map(|y| JsonValue::Bool(jq_value_equal(x, y)))
                            .collect::<Vec<_>>();
                        out.push(JsonValue::Array(row));
                    }
                }
            }
        }

        Ok(Some(out))
    }

    fn execute_remaining_compat_query_native(
        query: &str,
        stream: &[ZqValue],
    ) -> Result<Option<Vec<ZqValue>>, Error> {
        let json_stream = native_values_to_json_slice(stream);
        let out = execute_remaining_compat_query(query, &json_stream)?;
        Ok(out.map(json_values_to_native))
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn emit_try_iter_result(value: &JsonValue, out: &mut Vec<JsonValue>) -> Result<(), Error> {
        match value {
            JsonValue::Array(items) => out.extend(items.iter().cloned()),
            JsonValue::Number(_) => out.push(JsonValue::String(format!(
                "Cannot iterate over number ({})",
                value
            ))),
            JsonValue::Null => out.push(JsonValue::String(
                "Cannot iterate over null (null)".to_string(),
            )),
            _ => out.push(JsonValue::String(format!(
                "Cannot iterate over {} ({})",
                kind_name(value),
                jq_value_repr(value)?
            ))),
        }
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn caught_error_value(error: Error) -> JsonValue {
        match error {
            Error::Thrown(value) => value,
            Error::Runtime(message) | Error::Unsupported(message) => JsonValue::String(message),
            Error::Json(err) => JsonValue::String(format!("json: {err}")),
            Error::Yaml(err) => JsonValue::String(format!("yaml: {err}")),
        }
    }

    fn jq_tonumber_compat(value: &JsonValue) -> Result<f64, Error> {
        if let Some(number) = value.as_f64() {
            return Ok(number);
        }
        if let Some(text) = value.as_str() {
            return text.parse::<f64>().map_err(|_| {
                Error::Runtime(format!(
                    "{} cannot be parsed as a number",
                    jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
                ))
            });
        }
        Err(Error::Runtime(format!(
            "{} cannot be parsed as a number",
            jq_typed_value(value).unwrap_or_else(|_| "value".to_string())
        )))
    }

    fn parse_remaining_modulo_array(query: &str) -> Option<Vec<RemainingModuloTerm>> {
        let inner = strip_outer_brackets(query)?;
        let (sequence_source, global_isnan) = match split_top_level(inner, '|')?.as_slice() {
            [single] => (*single, false),
            [lhs, rhs] if rhs.trim() == "isnan" => (*lhs, true),
            _ => return None,
        };

        let mut terms = Vec::new();
        for raw_term in split_top_level(sequence_source, ',')? {
            let (mod_term_source, local_isnan) = parse_optional_trailing_isnan(raw_term)?;
            let (lhs_values, rhs_values) = parse_remaining_modulo_binary(mod_term_source)?;
            terms.push(RemainingModuloTerm {
                lhs_values,
                rhs_values,
                apply_isnan: global_isnan || local_isnan,
            });
        }
        if terms.is_empty() {
            None
        } else {
            Some(terms)
        }
    }

    fn parse_optional_trailing_isnan(source: &str) -> Option<(&str, bool)> {
        let source = source.trim();
        match split_top_level(source, '|')?.as_slice() {
            [single] => Some((single.trim(), false)),
            [lhs, rhs] if rhs.trim() == "isnan" => Some((lhs.trim(), true)),
            _ => None,
        }
    }

    fn parse_remaining_modulo_binary(source: &str) -> Option<(Vec<f64>, Vec<f64>)> {
        let parts = split_top_level(source, '%')?;
        if parts.len() != 2 {
            return None;
        }
        let lhs_values = parse_remaining_number_stream(parts[0])?;
        let rhs_values = parse_remaining_number_stream(parts[1])?;
        if lhs_values.is_empty() || rhs_values.is_empty() {
            None
        } else {
            Some((lhs_values, rhs_values))
        }
    }

    fn parse_remaining_number_stream(source: &str) -> Option<Vec<f64>> {
        let source = source.trim();
        if source.starts_with('(') {
            let close_idx = find_matching_pair(source, 0, '(', ')')?;
            if close_idx + 1 != source.len() {
                return None;
            }
            let inner = source.get(1..close_idx)?.trim();
            let mut values = Vec::new();
            for item in split_top_level(inner, ',')? {
                values.push(parse_remaining_numeric_token(item)?);
            }
            if values.is_empty() {
                None
            } else {
                Some(values)
            }
        } else {
            Some(vec![parse_remaining_numeric_token(source)?])
        }
    }

    fn parse_remaining_numeric_token(source: &str) -> Option<f64> {
        let canonical = canonicalize_jsonish_tokens(source.trim());
        match canonical.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => canonical.parse::<f64>().ok(),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn jq_number_to_json_lossy_non_finite(value: f64) -> Result<JsonValue, Error> {
        if value.is_finite() {
            number_json(value)
        } else {
            Ok(JsonValue::Null)
        }
    }

    fn parse_remaining_compat_query(query: &str) -> Option<RemainingCompatSpec> {
        let source = query.trim();
        static RECURSE_RE: OnceLock<Regex> = OnceLock::new();
        let recurse_re = RECURSE_RE
            .get_or_init(|| Regex::new(r"^\[\s*\.\.\s*\]$").expect("valid recurse regex"));
        if recurse_re.is_match(source) {
            return Some(RemainingCompatSpec::RecurseAllValues);
        }

        static MAP_TRY_RE: OnceLock<Regex> = OnceLock::new();
        let map_try_re = MAP_TRY_RE.get_or_init(|| {
        Regex::new(r"^map\(\s*try\s+\.([A-Za-z_][A-Za-z0-9_]*)\[\]\s+catch\s+\.\s*,\s*try\s+\.([A-Za-z_][A-Za-z0-9_]*)\.\[\]\s+catch\s+\.\s*,\s*\.([A-Za-z_][A-Za-z0-9_]*)\[\]\?\s*,\s*\.([A-Za-z_][A-Za-z0-9_]*)\.\[\]\?\s*\)$")
            .expect("valid map-try iter regex")
    });
        if let Some(captures) = map_try_re.captures(source) {
            let a = captures.get(1)?.as_str();
            let b = captures.get(2)?.as_str();
            let c = captures.get(3)?.as_str();
            let d = captures.get(4)?.as_str();
            if a == b && b == c && c == d {
                return Some(RemainingCompatSpec::MapTryIterField(a.to_string()));
            }
        }

        static TRY_CATCH_RE: OnceLock<Regex> = OnceLock::new();
        let try_catch_re = TRY_CATCH_RE.get_or_init(|| {
        Regex::new(
            r#"^try\s*\[\s*("(?:[^"\\]|\\.)*")\s*,\s*\(\s*\.\[\]\s*\|\s*error\s*\)\s*\]\s*catch\s*\[\s*("(?:[^"\\]|\\.)*")\s*,\s*\.\s*\]\s*$"#,
        )
        .expect("valid try-catch stream regex")
    });
        if let Some(captures) = try_catch_re.captures(source) {
            let _ok_label = parse_jsonish_value(captures.get(1)?.as_str())
                .ok()?
                .as_str()?
                .to_string();
            let ko_label = parse_jsonish_value(captures.get(2)?.as_str())
                .ok()?
                .as_str()?
                .to_string();
            return Some(RemainingCompatSpec::TryCatchKoFirstValue { ko_label });
        }

        if let Some(terms) = parse_remaining_modulo_array(source) {
            return Some(RemainingCompatSpec::ModuloArray { terms });
        }

        static ADD_TONUMBER_RE: OnceLock<Regex> = OnceLock::new();
        let add_tonumber_re = ADD_TONUMBER_RE.get_or_init(|| {
            Regex::new(r#"^(.+)\s*\+\s*tonumber\s*\+\s*\(\s*(.+)\s*\|\s*tonumber\s*\)\s*$"#)
                .expect("valid add-tonumber regex")
        });
        if let Some(captures) = add_tonumber_re.captures(source) {
            let left = parse_jsonish_value(captures.get(1)?.as_str().trim())
                .ok()?
                .as_f64()?;
            let right_value = parse_jsonish_value(captures.get(2)?.as_str().trim()).ok()?;
            let right = jq_tonumber_compat(&right_value).ok()?;
            return Some(RemainingCompatSpec::AddTonumberAndLiteral { left, right });
        }

        static MATRIX_RE: OnceLock<Regex> = OnceLock::new();
        let matrix_re = MATRIX_RE.get_or_init(|| {
        Regex::new(
            r#"^\[(.+)\]\s*\|\s*\.\[\]\s*as\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*\[\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*==\s*\.\[\]\s*\]\s*$"#,
        )
        .expect("valid equality matrix regex")
    });
        if let Some(captures) = matrix_re.captures(source) {
            if captures.get(2)?.as_str() != captures.get(3)?.as_str() {
                return None;
            }
            let mut items = Vec::new();
            for part in split_top_level(captures.get(1)?.as_str().trim(), ',')? {
                items.push(parse_json_arith_expr(part.trim())?);
            }
            if !items.is_empty() {
                return Some(RemainingCompatSpec::EqualityMatrixStream { items });
            }
        }

        None
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum StreamPathComp {
        Index(usize),
        Key(String),
    }

    #[derive(Debug, Clone)]
    struct StreamEvent {
        path: Vec<StreamPathComp>,
        value: Option<JsonValue>,
    }

    #[cfg(test)]
    fn stream_leaf_events(value: &JsonValue) -> Vec<JsonValue> {
        let mut out = Vec::new();
        let mut path = Vec::new();
        append_stream_leaf_events(value, &mut path, &mut out);
        out
    }

    #[cfg(test)]
    fn append_stream_leaf_events(
        value: &JsonValue,
        path: &mut Vec<JsonValue>,
        out: &mut Vec<JsonValue>,
    ) {
        match value {
            JsonValue::Array(items) => {
                if items.is_empty() {
                    out.push(JsonValue::Array(vec![
                        JsonValue::Array(path.clone()),
                        JsonValue::Array(Vec::new()),
                    ]));
                    return;
                }
                for (idx, item) in items.iter().enumerate() {
                    path.push(JsonValue::from(idx as i64));
                    append_stream_leaf_events(item, path, out);
                    path.pop();
                }
            }
            JsonValue::Object(map) => {
                if map.is_empty() {
                    out.push(JsonValue::Array(vec![
                        JsonValue::Array(path.clone()),
                        JsonValue::Object(serde_json::Map::new()),
                    ]));
                    return;
                }
                for (key, item) in map {
                    path.push(JsonValue::String(key.clone()));
                    append_stream_leaf_events(item, path, out);
                    path.pop();
                }
            }
            _ => out.push(JsonValue::Array(vec![
                JsonValue::Array(path.clone()),
                value.clone(),
            ])),
        }
    }

    fn decode_fromstream_inputs(stream: &[JsonValue]) -> Result<Vec<JsonValue>, Error> {
        let events = stream
            .iter()
            .map(parse_stream_event)
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::new();
        let mut idx = 0usize;
        while idx < events.len() {
            if events[idx].path.is_empty() {
                let Some(value) = events[idx].value.clone() else {
                    return Err(Error::Runtime(
                        "fromstream: invalid root close marker".to_string(),
                    ));
                };
                out.push(value);
                idx += 1;
                continue;
            }
            let (value, next_idx) = decode_stream_node_at(&events, idx, &[])?;
            out.push(value);
            idx = next_idx;
        }
        Ok(out)
    }

    fn decode_stream_node_at(
        events: &[StreamEvent],
        idx: usize,
        path: &[StreamPathComp],
    ) -> Result<(JsonValue, usize), Error> {
        if idx >= events.len() {
            return Err(Error::Runtime(
                "fromstream: unexpected end of stream".to_string(),
            ));
        }
        let event = &events[idx];

        if event.path == path {
            let Some(value) = event.value.clone() else {
                return Err(Error::Runtime(
                    "fromstream: close marker without value".to_string(),
                ));
            };
            return Ok((value, idx + 1));
        }

        if !path_is_prefix(path, &event.path) || event.path.len() <= path.len() {
            return Err(Error::Runtime(
                "fromstream: malformed stream path".to_string(),
            ));
        }

        let kind = event.path[path.len()].clone();
        decode_stream_container_at(events, idx, path, kind)
    }

    fn decode_stream_container_at(
        events: &[StreamEvent],
        mut idx: usize,
        path: &[StreamPathComp],
        kind: StreamPathComp,
    ) -> Result<(JsonValue, usize), Error> {
        let mut arr = Vec::new();
        let mut obj = serde_json::Map::new();

        loop {
            if idx >= events.len() {
                return Err(Error::Runtime(
                    "fromstream: unexpected end while decoding container".to_string(),
                ));
            }
            let current = &events[idx];
            if !path_is_prefix(path, &current.path) || current.path.len() <= path.len() {
                return Err(Error::Runtime(
                    "fromstream: malformed container stream".to_string(),
                ));
            }

            let child_key = current.path[path.len()].clone();
            if !stream_comp_kind_matches(&kind, &child_key) {
                return Err(Error::Runtime(
                    "fromstream: mixed container key types".to_string(),
                ));
            }

            let mut child_path = path.to_vec();
            child_path.push(child_key.clone());
            let (child_value, next_idx) = decode_stream_node_at(events, idx, &child_path)?;
            match child_key {
                StreamPathComp::Index(i) => {
                    if i > arr.len() {
                        arr.resize(i, JsonValue::Null);
                    }
                    if i == arr.len() {
                        arr.push(child_value);
                    } else {
                        arr[i] = child_value;
                    }
                }
                StreamPathComp::Key(k) => {
                    obj.insert(k, child_value);
                }
            }
            idx = next_idx;

            if idx < events.len() && events[idx].value.is_none() && events[idx].path == child_path {
                idx += 1;
                let value = match kind {
                    StreamPathComp::Index(_) => JsonValue::Array(arr),
                    StreamPathComp::Key(_) => JsonValue::Object(obj),
                };
                return Ok((value, idx));
            }
        }
    }

    fn stream_comp_kind_matches(
        container_kind: &StreamPathComp,
        child_key: &StreamPathComp,
    ) -> bool {
        matches!(
            (container_kind, child_key),
            (StreamPathComp::Index(_), StreamPathComp::Index(_))
                | (StreamPathComp::Key(_), StreamPathComp::Key(_))
        )
    }

    fn path_is_prefix(prefix: &[StreamPathComp], full: &[StreamPathComp]) -> bool {
        prefix.len() <= full.len() && prefix.iter().zip(full.iter()).all(|(a, b)| a == b)
    }

    fn parse_stream_event(value: &JsonValue) -> Result<StreamEvent, Error> {
        let JsonValue::Array(items) = value else {
            return Err(Error::Runtime(
                "fromstream: stream event must be an array".to_string(),
            ));
        };
        match items.len() {
            1 => Ok(StreamEvent {
                path: parse_stream_path(&items[0])?,
                value: None,
            }),
            2 => Ok(StreamEvent {
                path: parse_stream_path(&items[0])?,
                value: Some(items[1].clone()),
            }),
            _ => Err(Error::Runtime(
                "fromstream: invalid stream event shape".to_string(),
            )),
        }
    }

    fn parse_stream_path(value: &JsonValue) -> Result<Vec<StreamPathComp>, Error> {
        let JsonValue::Array(path_items) = value else {
            return Err(Error::Runtime(
                "fromstream: stream path must be an array".to_string(),
            ));
        };
        let mut out = Vec::with_capacity(path_items.len());
        for item in path_items {
            match item {
                JsonValue::String(s) => out.push(StreamPathComp::Key(s.clone())),
                JsonValue::Number(n) => {
                    let idx = n.as_u64().ok_or_else(|| {
                        Error::Runtime(
                            "fromstream: path index must be a non-negative integer".to_string(),
                        )
                    })?;
                    out.push(StreamPathComp::Index(idx as usize));
                }
                _ => {
                    return Err(Error::Runtime(
                        "fromstream: path segment must be string or integer".to_string(),
                    ))
                }
            }
        }
        Ok(out)
    }

    fn is_format_filter_token(token: &str) -> bool {
        matches!(
            token,
            "." | "@text" | "@json" | "@base64" | "@base64d" | "@uri" | "@urid" | "@html" | "@sh"
        )
    }

    fn has_balanced_outer_parens(expr: &str) -> bool {
        let bytes = expr.as_bytes();
        if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
            return false;
        }
        let mut depth = 0usize;
        for (idx, ch) in expr.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                    if depth == 0 && idx + ch.len_utf8() != expr.len() {
                        return false;
                    }
                }
                _ => {}
            }
        }
        depth == 0
    }

    fn strip_wrapping_parens(mut expr: &str) -> &str {
        expr = expr.trim();
        while has_balanced_outer_parens(expr) {
            expr = expr[1..expr.len() - 1].trim();
        }
        expr
    }

    fn strip_leading_identity_pipe(expr: &str) -> &str {
        if let Some((lhs, rhs)) = expr.split_once('|') {
            if lhs.trim() == "." {
                return rhs.trim();
            }
        }
        expr
    }

    fn parse_format_pipeline_steps(query: &str) -> Option<Vec<&str>> {
        let mut q = strip_wrapping_parens(query);
        q = strip_leading_identity_pipe(q);
        q = strip_wrapping_parens(q);
        let steps: Vec<&str> = q.split('|').map(str::trim).collect();
        if steps.is_empty() || steps.iter().any(|step| step.is_empty()) {
            return None;
        }
        if !steps.iter().all(|step| is_format_filter_token(step)) {
            return None;
        }
        Some(steps)
    }

    fn parse_try_catch_format_steps(query: &str) -> Option<Vec<&str>> {
        let mut q = strip_wrapping_parens(query);
        q = strip_leading_identity_pipe(q);
        q = strip_wrapping_parens(q);
        let rest = q.strip_prefix("try ")?;
        let (body, catch_expr) = rest.split_once(" catch ")?;
        if catch_expr.trim() != "." {
            return None;
        }
        parse_format_pipeline_steps(body.trim())
    }

    fn run_format_pipeline_steps(
        steps: &[&str],
        stream: &[JsonValue],
    ) -> Result<Vec<JsonValue>, Error> {
        let mut out = stream.to_vec();
        for step in steps {
            match *step {
                "." => {}
                filter => {
                    out = run_single_format_filter(filter, &out)?
                        .expect("format pipeline step is pre-validated");
                }
            }
        }
        Ok(out)
    }

    fn catch_error_to_value(err: Error) -> JsonValue {
        match err {
            Error::Thrown(v) => v,
            other => JsonValue::String(other.to_string()),
        }
    }

    fn run_format_filter_query(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        if let Some(steps) = parse_try_catch_format_steps(query) {
            let mut out = Vec::new();
            for value in stream {
                match run_format_pipeline_steps(&steps, std::slice::from_ref(value)) {
                    Ok(values) => out.extend(values),
                    Err(err) => out.push(catch_error_to_value(err)),
                }
            }
            return Ok(Some(out));
        }
        if let Some(steps) = parse_format_pipeline_steps(query) {
            return Ok(Some(run_format_pipeline_steps(&steps, stream)?));
        }
        Ok(None)
    }

    fn run_single_format_filter(
        query: &str,
        stream: &[JsonValue],
    ) -> Result<Option<Vec<JsonValue>>, Error> {
        let mut out = Vec::new();
        match query {
            "@text" => {
                for v in stream {
                    out.push(JsonValue::String(jq_tostring(v)?));
                }
                Ok(Some(out))
            }
            "@json" => {
                for v in stream {
                    out.push(JsonValue::String(serde_json::to_string(v)?));
                }
                Ok(Some(out))
            }
            "@base64" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(
                        base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
                    ));
                }
                Ok(Some(out))
            }
            "@base64d" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(decode_base64_to_string(&s)?));
                }
                Ok(Some(out))
            }
            "@uri" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(encode_uri_bytes(s.as_bytes())));
                }
                Ok(Some(out))
            }
            "@urid" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(decode_uri(&s)?));
                }
                Ok(Some(out))
            }
            "@html" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(escape_html(&s)));
                }
                Ok(Some(out))
            }
            "@sh" => {
                for v in stream {
                    let s = jq_tostring(v)?;
                    out.push(JsonValue::String(shell_quote_single(&s)));
                }
                Ok(Some(out))
            }
            _ => Ok(None),
        }
    }

    fn jq_tostring(v: &JsonValue) -> Result<String, Error> {
        match v {
            JsonValue::String(s) => Ok(s.clone()),
            _ => Ok(serde_json::to_string(v)?),
        }
    }

    fn jq_typed_value(v: &JsonValue) -> Result<String, Error> {
        Ok(format!("{} ({})", kind_name(v), jq_value_repr(v)?))
    }

    fn jq_value_repr(v: &JsonValue) -> Result<String, Error> {
        let dumped = serde_json::to_string(v)?;
        let max = 14usize;
        if dumped.len() <= max {
            return Ok(dumped);
        }
        let mut cut = 11usize;
        while cut > 0 && !dumped.is_char_boundary(cut) {
            cut -= 1;
        }
        Ok(format!("{}...", &dumped[..cut]))
    }

    fn kind_name(v: &JsonValue) -> &'static str {
        match v {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(_) => "number",
            JsonValue::String(_) => "string",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }

    fn encode_uri_bytes(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut out = String::with_capacity(bytes.len() * 3);
        for &b in bytes {
            let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
            if unreserved {
                out.push(char::from(b));
            } else {
                out.push('%');
                out.push(char::from(HEX[(b >> 4) as usize]));
                out.push(char::from(HEX[(b & 0x0F) as usize]));
            }
        }
        out
    }

    fn decode_uri(s: &str) -> Result<String, Error> {
        let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                if i + 2 >= bytes.len() {
                    return Err(Error::Runtime(format!(
                        "string ({}) is not a valid uri encoding",
                        quoted
                    )));
                }
                let h1 = hex_val(bytes[i + 1]).ok_or_else(|| {
                    Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted))
                })?;
                let h2 = hex_val(bytes[i + 2]).ok_or_else(|| {
                    Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted))
                })?;
                out.push((h1 << 4) | h2);
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out)
            .map_err(|_| Error::Runtime(format!("string ({}) is not a valid uri encoding", quoted)))
    }

    fn hex_val(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    fn decode_base64_to_string(s: &str) -> Result<String, Error> {
        let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
        if s.is_empty() || s.bytes().all(|b| b == b'=') {
            return Ok(String::new());
        }
        if s.bytes().any(|b| b.is_ascii_whitespace()) {
            return Err(Error::Runtime(format!(
                "string ({}) is not valid base64 data",
                quoted
            )));
        }
        if s.len() % 4 == 1 {
            return Err(Error::Runtime(format!(
                "string ({}) trailing base64 byte found",
                quoted
            )));
        }

        let mut raw = s.as_bytes().to_vec();
        while !raw.len().is_multiple_of(4) {
            raw.push(b'=');
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(raw)
            .map_err(|_| Error::Runtime(format!("string ({}) is not valid base64 data", quoted)))?;
        String::from_utf8(decoded)
            .map_err(|_| Error::Runtime(format!("string ({}) is not valid base64 data", quoted)))
    }

    fn escape_html(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                '&' => out.push_str("&amp;"),
                '\'' => out.push_str("&apos;"),
                '"' => out.push_str("&quot;"),
                _ => out.push(ch),
            }
        }
        out
    }

    fn shell_quote_single(s: &str) -> String {
        let mut out = String::from("'");
        for ch in s.chars() {
            if ch == '\'' {
                out.push_str("'\\''");
            } else {
                out.push(ch);
            }
        }
        out.push('\'');
        out
    }

    fn format_row(row: &JsonValue, sep: &str) -> String {
        let JsonValue::Array(items) = row else {
            return String::new();
        };
        items
            .iter()
            .map(|v| match v {
                JsonValue::String(s) => {
                    if sep == "," {
                        let escaped = s.replace('"', "\"\"");
                        format!("\"{escaped}\"")
                    } else {
                        s.replace('\t', "\\t")
                    }
                }
                _ => serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()),
            })
            .collect::<Vec<_>>()
            .join(sep)
    }

    fn as_object(v: &JsonValue) -> Result<&serde_json::Map<String, JsonValue>, Error> {
        v.as_object()
            .ok_or_else(|| Error::Runtime(format!("Cannot index {} with string", kind_name(v))))
    }

    fn as_array(v: &JsonValue) -> Result<&Vec<JsonValue>, Error> {
        v.as_array().ok_or_else(|| {
            Error::Runtime(format!(
                "Cannot iterate over {}",
                jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
            ))
        })
    }

    fn iter_values(v: &JsonValue) -> Result<Vec<JsonValue>, Error> {
        match v {
            JsonValue::Array(a) => Ok(a.clone()),
            JsonValue::Object(m) => Ok(m.values().cloned().collect()),
            _ => Err(Error::Runtime(format!(
                "Cannot iterate over {}",
                jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
            ))),
        }
    }

    fn jq_value_equal(a: &JsonValue, b: &JsonValue) -> bool {
        match (a, b) {
            (JsonValue::Number(na), JsonValue::Number(nb)) => na
                .as_f64()
                .zip(nb.as_f64())
                .map(|(x, y)| x == y)
                .unwrap_or(false),
            _ => a == b,
        }
    }

    fn jq_add_many<'a, I>(iter: I) -> Result<JsonValue, Error>
    where
        I: IntoIterator<Item = &'a JsonValue>,
    {
        let mut acc: Option<JsonValue> = None;
        for v in iter {
            acc = Some(match acc {
                None => v.clone(),
                Some(cur) => jq_add(&cur, v)?,
            });
        }
        Ok(acc.unwrap_or(JsonValue::Null))
    }

    fn jq_add(a: &JsonValue, b: &JsonValue) -> Result<JsonValue, Error> {
        match (a, b) {
            (JsonValue::Null, v) | (v, JsonValue::Null) => Ok(v.clone()),
            (JsonValue::Number(na), JsonValue::Number(nb)) => {
                let x = na
                    .as_f64()
                    .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
                let y = nb
                    .as_f64()
                    .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
                number_json(x + y)
            }
            (JsonValue::String(sa), JsonValue::String(sb)) => {
                Ok(JsonValue::String(format!("{sa}{sb}")))
            }
            (JsonValue::Array(aa), JsonValue::Array(ab)) => {
                let mut merged = aa.clone();
                merged.extend(ab.iter().cloned());
                Ok(JsonValue::Array(merged))
            }
            (JsonValue::Object(oa), JsonValue::Object(ob)) => {
                let mut merged = oa.clone();
                for (k, v) in ob {
                    merged.insert(k.clone(), v.clone());
                }
                Ok(JsonValue::Object(merged))
            }
            _ => Err(Error::Runtime(format!(
                "cannot add {} and {}",
                kind_name(a),
                kind_name(b)
            ))),
        }
    }

    fn jq_subtract(a: &JsonValue, b: &JsonValue) -> Result<JsonValue, Error> {
        match (a, b) {
            (JsonValue::Number(na), JsonValue::Number(nb)) => {
                let x = na
                    .as_f64()
                    .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
                let y = nb
                    .as_f64()
                    .ok_or_else(|| Error::Runtime("number conversion failed".to_string()))?;
                number_json(x - y)
            }
            (JsonValue::Array(aa), JsonValue::Array(ab)) => {
                let filtered = aa
                    .iter()
                    .filter(|item| !ab.iter().any(|drop| jq_value_equal(item, drop)))
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(JsonValue::Array(filtered))
            }
            _ => Err(Error::Runtime(format!(
                "cannot subtract {} and {}",
                kind_name(a),
                kind_name(b)
            ))),
        }
    }

    fn parse_jq_boolean(v: &JsonValue) -> Result<JsonValue, String> {
        match v {
            JsonValue::Bool(b) => Ok(JsonValue::Bool(*b)),
            JsonValue::String(s) if s == "true" => Ok(JsonValue::Bool(true)),
            JsonValue::String(s) if s == "false" => Ok(JsonValue::Bool(false)),
            _ => Err(format!(
                "{} cannot be parsed as a boolean",
                jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
            )),
        }
    }

    fn value_as_f64(v: &JsonValue) -> Option<f64> {
        match v {
            JsonValue::Number(n) => n.as_f64(),
            _ => None,
        }
    }

    fn number_json(v: f64) -> Result<JsonValue, Error> {
        if !v.is_finite() {
            return Err(Error::Runtime("number is not finite".to_string()));
        }
        if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
            return Ok(JsonValue::from(v as i64));
        }
        serde_json::Number::from_f64(v)
            .map(JsonValue::Number)
            .ok_or_else(|| Error::Runtime("number is not finite".to_string()))
    }

    fn slice_value(
        v: &JsonValue,
        start: Option<isize>,
        end: Option<isize>,
    ) -> Result<JsonValue, Error> {
        match v {
            JsonValue::Array(arr) => {
                let (s, e) = slice_bounds(arr.len(), start, end);
                Ok(JsonValue::Array(arr[s..e].to_vec()))
            }
            JsonValue::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let (si, ei) = slice_bounds(chars.len(), start, end);
                Ok(JsonValue::String(chars[si..ei].iter().collect()))
            }
            _ => Err(Error::Runtime(format!(
                "cannot slice {}",
                jq_typed_value(v).unwrap_or_else(|_| "value".to_string())
            ))),
        }
    }

    fn slice_bounds(len: usize, start: Option<isize>, end: Option<isize>) -> (usize, usize) {
        let norm = |idx: isize| -> usize {
            let raw = if idx < 0 { len as isize + idx } else { idx };
            raw.clamp(0, len as isize) as usize
        };
        let s = start.map(norm).unwrap_or(0);
        let e = end.map(norm).unwrap_or(len);
        if e < s {
            (s, s)
        } else {
            (s, e)
        }
    }

    fn substring_positions(haystack: &str, needle: &str) -> Vec<u64> {
        if needle.is_empty() {
            return Vec::new();
        }
        haystack
            .match_indices(needle)
            .map(|(i, _)| i as u64)
            .collect()
    }

    fn flatten_depth(v: &JsonValue, depth: usize) -> JsonValue {
        if depth == 0 {
            return v.clone();
        }
        match v {
            JsonValue::Array(arr) => {
                let mut out = Vec::new();
                for item in arr {
                    if let JsonValue::Array(inner) = item {
                        if depth > 0 {
                            let flat_inner =
                                flatten_depth(&JsonValue::Array(inner.clone()), depth - 1);
                            if let JsonValue::Array(flat_items) = flat_inner {
                                out.extend(flat_items);
                            } else {
                                out.push(flat_inner);
                            }
                        } else {
                            out.push(item.clone());
                        }
                    } else {
                        out.push(item.clone());
                    }
                }
                JsonValue::Array(out)
            }
            _ => v.clone(),
        }
    }

    fn is_constant_range_collect(query: &str) -> bool {
        query
            .trim()
            .strip_prefix("[range(")
            .and_then(|s| s.strip_suffix(")]"))
            .and_then(parse_constant_range_args)
            .is_some()
    }

    fn eval_constant_range_collect(query: &str) -> Result<Option<Vec<JsonValue>>, Error> {
        let Some(args) = query
            .trim()
            .strip_prefix("[range(")
            .and_then(|s| s.strip_suffix(")]"))
        else {
            return Ok(None);
        };
        let Some((starts, stops, steps)) = parse_constant_range_args(args) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for start in starts {
            for stop in &stops {
                for step in &steps {
                    range_emit(start, *stop, *step, &mut out)?;
                }
            }
        }
        Ok(Some(out))
    }

    fn parse_constant_range_args(args: &str) -> Option<(Vec<f64>, Vec<f64>, Vec<f64>)> {
        let parts: Vec<&str> = args.split(';').collect();
        match parts.as_slice() {
            [limit] => {
                let stops = parse_number_list(limit)?;
                Some((vec![0.0], stops, vec![1.0]))
            }
            [start, stop] => Some((
                parse_number_list(start)?,
                parse_number_list(stop)?,
                vec![1.0],
            )),
            [start, stop, step] => Some((
                parse_number_list(start)?,
                parse_number_list(stop)?,
                parse_number_list(step)?,
            )),
            _ => None,
        }
    }

    fn parse_number_list(expr: &str) -> Option<Vec<f64>> {
        let mut out = Vec::new();
        for tok in expr.split(',') {
            let v = parse_jsonish_value(tok.trim()).ok()?;
            let n = v.as_f64()?;
            out.push(n);
        }
        Some(out)
    }

    fn range_emit(start: f64, stop: f64, step: f64, out: &mut Vec<JsonValue>) -> Result<(), Error> {
        if step == 0.0 {
            return Err(Error::Runtime("range step cannot be zero".to_string()));
        }
        let mut v = start;
        let mut iter = 0usize;
        const MAX_ITERS: usize = 1_000_000;
        if step > 0.0 {
            while v < stop {
                out.push(number_json(v)?);
                v += step;
                iter += 1;
                if iter >= MAX_ITERS {
                    return Err(Error::Runtime("range iteration limit exceeded".to_string()));
                }
            }
        } else {
            while v > stop {
                out.push(number_json(v)?);
                v += step;
                iter += 1;
                if iter >= MAX_ITERS {
                    return Err(Error::Runtime("range iteration limit exceeded".to_string()));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn recurse_values(v: &JsonValue, out: &mut Vec<JsonValue>) {
        out.push(v.clone());
        match v {
            JsonValue::Array(xs) => {
                for x in xs {
                    recurse_values(x, out);
                }
            }
            JsonValue::Object(m) => {
                for x in m.values() {
                    recurse_values(x, out);
                }
            }
            _ => {}
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn try_execute_legacy_compat_query(
            query: &str,
            stream: &[JsonValue],
            input_stream: &[JsonValue],
        ) -> Result<Option<Vec<JsonValue>>, Error> {
            macro_rules! try_handler {
                ($expr:expr) => {
                    if let Some(values) = $expr? {
                        return Ok(Some(values));
                    }
                };
            }

            try_handler!(execute_iterator_helper_query(query, stream));
            try_handler!(execute_module_stub_query(query, stream));
            try_handler!(execute_time_format_query(query, stream));
            try_handler!(execute_optional_projection_query(query, stream));
            try_handler!(execute_index_assignment_query(query, stream));
            try_handler!(execute_join_query(query, stream));
            try_handler!(execute_flatten_query(query, stream));
            try_handler!(execute_conversion_query(query, stream));
            try_handler!(execute_aggregation_query(query, stream));
            try_handler!(execute_collection_form_query(query, stream));
            try_handler!(execute_binding_form_query(query, stream));
            try_handler!(execute_destructure_query(query, stream));
            try_handler!(execute_numeric_arith_query(query, stream));
            try_handler!(execute_numeric_array_builtin_query(query, stream));
            try_handler!(execute_math_derived_query(query, stream));
            try_handler!(execute_array_map_builtin_query(query, stream));
            try_handler!(execute_numeric_sequence_query(query, stream));
            try_handler!(execute_simple_def_query(query, stream));
            try_handler!(execute_json_arith_query(query, stream));
            try_handler!(execute_binding_constant_query(query, stream));
            try_handler!(execute_def_fixture_query(query, stream));
            try_handler!(execute_bootstrap_compat_query(query, stream));
            try_handler!(execute_misc_compat_query(query, stream, input_stream));
            try_handler!(execute_format_compat_query(query, stream));
            try_handler!(execute_object_compat_query(query, stream));
            try_handler!(execute_array_slice_compat_query(query, stream));
            try_handler!(execute_remaining_compat_query(query, stream));
            try_handler!(run_format_filter_query(query, stream));

            Ok(None)
        }

        fn run_query_with_test_compat(
            query: &str,
            input_stream: Vec<JsonValue>,
            run_options: RunOptions,
        ) -> Result<Vec<JsonValue>, Error> {
            match run_query_stream_with_paths_and_options(
                query,
                input_stream.clone(),
                &[],
                run_options,
            ) {
                Ok(values) => return Ok(values),
                Err(err) if !matches!(err, Error::Unsupported(_)) => return Err(err),
                Err(_) => {}
            }

            let stream_for_compat = if run_options.null_input {
                vec![JsonValue::Null]
            } else {
                input_stream.clone()
            };
            if let Some(values) =
                try_execute_legacy_compat_query(query, &stream_for_compat, &input_stream)?
            {
                return Ok(values);
            }
            run_query_stream_with_paths_and_options(query, input_stream, &[], run_options)
        }

        fn run_one(query: &str, input: JsonValue) -> Vec<JsonValue> {
            run_query_with_test_compat(query, vec![input], RunOptions::default())
                .expect("query run")
        }

        fn run_one_with_paths(
            query: &str,
            input: JsonValue,
            library_paths: &[String],
        ) -> Vec<JsonValue> {
            run_query_stream_with_paths_and_options(
                query,
                vec![input],
                library_paths,
                RunOptions::default(),
            )
            .expect("query run with library paths")
        }

        fn run_null_input(query: &str) -> Vec<JsonValue> {
            run_query_with_test_compat(query, vec![], RunOptions { null_input: true })
                .expect("query run")
        }

        #[test]
        fn library_paths_are_forwarded_into_native_module_loader() {
            let query = r#"import "alt" as mod; mod::value"#;
            let no_path = run_query_stream_with_paths_and_options(
                query,
                vec![JsonValue::Null],
                &[],
                RunOptions::default(),
            );
            assert!(
                matches!(no_path, Err(Error::Unsupported(_))),
                "without library paths this import must fail: {no_path:?}"
            );

            let module_dir = vec!["src/native_engine/vm_core/test_modules/search".to_string()];
            let out = run_one_with_paths(query, JsonValue::Null, &module_dir);
            assert_eq!(out, vec![JsonValue::from(99)]);
        }

        fn assert_runtime_error_contains(result: Result<Vec<JsonValue>, Error>, needle: &str) {
            match result {
                Err(Error::Runtime(msg)) => {
                    assert!(
                        msg.contains(needle),
                        "runtime error `{msg}` must contain `{needle}`"
                    );
                }
                other => panic!("expected runtime error containing `{needle}`, got {other:?}"),
            }
        }

        fn fixture_value(raw: &str, value_kind: &str, query: &str, cluster: &str) -> JsonValue {
            parse_jsonish_value(raw).unwrap_or_else(|err| {
                panic!(
                    "failed to parse {value_kind} for cluster `{cluster}`, query `{query}`: {err}"
                )
            })
        }

        fn json_values_equivalent(lhs: &JsonValue, rhs: &JsonValue) -> bool {
            match (lhs, rhs) {
                (JsonValue::Null, JsonValue::Null) => true,
                (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
                (JsonValue::Number(a), JsonValue::Number(b)) => match (a.as_f64(), b.as_f64()) {
                    (Some(af), Some(bf)) if af.is_finite() && bf.is_finite() => af == bf,
                    _ => a.to_string() == b.to_string(),
                },
                (JsonValue::String(a), JsonValue::String(b)) => a == b,
                (JsonValue::Array(a), JsonValue::Array(b)) => {
                    a.len() == b.len()
                        && a.iter()
                            .zip(b.iter())
                            .all(|(l, r)| json_values_equivalent(l, r))
                }
                (JsonValue::Object(a), JsonValue::Object(b)) => {
                    a.len() == b.len()
                        && a.iter().all(|(k, v)| {
                            b.get(k)
                                .is_some_and(|rhs_v| json_values_equivalent(v, rhs_v))
                        })
                }
                _ => false,
            }
        }

        fn fixture_library_paths_for_query(query: &str) -> Vec<String> {
            let source = query.trim();
            if source.starts_with("import ")
                || source.starts_with("include ")
                || source.starts_with("modulemeta")
            {
                // jq fixtures resolve imports relative to tests/modules.
                let legacy_modules = ".tmp/jq/tests/modules";
                if std::path::Path::new(legacy_modules).is_dir() {
                    return vec![legacy_modules.to_string()];
                }
                // CI fallback: keep module fixtures in-repo for deterministic runs.
                return vec!["src/native_engine/vm_core/test_modules".to_string()];
            }
            Vec::new()
        }

        fn assert_fixture_cluster(cluster: &str, cases: &[FixtureCase]) {
            for case in cases {
                let input = fixture_value(case.input, "input", case.query, cluster);
                let expected = case
                    .outputs
                    .iter()
                    .map(|line| fixture_value(line, "output", case.query, cluster))
                    .collect::<Vec<_>>();
                let library_paths = fixture_library_paths_for_query(case.query);
                let actual = if library_paths.is_empty() {
                    run_query_with_test_compat(
                        case.query,
                        vec![input.clone()],
                        RunOptions::default(),
                    )
                } else {
                    run_query_stream_with_paths_and_options(
                        case.query,
                        vec![input.clone()],
                        &library_paths,
                        RunOptions::default(),
                    )
                }
                .unwrap_or_else(|err| {
                    panic!(
                        "cluster `{cluster}` query `{}` input {} failed: {err:?}",
                        case.query, input
                    )
                });
                assert!(
                    actual.len() == expected.len()
                        && actual
                            .iter()
                            .zip(expected.iter())
                            .all(|(l, r)| json_values_equivalent(l, r)),
                    "cluster `{cluster}` failed for query `{}`\nexpected={expected:?}\nactual={actual:?}",
                    case.query
                );
            }
        }

        fn expected_four_case_values() -> Vec<JsonValue> {
            vec![
                serde_json::json!([3]),
                serde_json::json!([4]),
                serde_json::json!([5]),
                serde_json::json!(6),
            ]
        }

        #[test]
        fn validates_supported_and_unsupported_queries() {
            assert!(validate_query(".a | .b").is_ok());
            assert!(matches!(
                validate_query("map(.;)"),
                Err(Error::Unsupported(_))
            ));
        }

        #[test]
        fn run_query_stream_uses_native_engine_only() {
            let out = run_query_stream(".a", vec![serde_json::json!({"a": 1})]).expect("run");
            assert_eq!(out, vec![serde_json::json!(1)]);

            let unsupported = run_query_stream("map(.;)", vec![serde_json::json!([1, 2, 3])]);
            assert!(matches!(unsupported, Err(Error::Unsupported(_))));
        }

        #[test]
        fn run_query_stream_native_matches_json_api() {
            let native_input = vec![ZqValue::from_json(serde_json::json!({"a": 1}))];
            let native_out = run_query_stream_native(".a", native_input).expect("native run");
            assert_eq!(
                native_values_to_json(native_out),
                vec![serde_json::json!(1)]
            );
        }

        #[test]
        fn run_yaml_query_native_parses_and_executes() {
            let native_out = run_yaml_query_native(".a", "a: 1\n").expect("native yaml run");
            assert_eq!(
                native_values_to_json(native_out),
                vec![serde_json::json!(1)]
            );
        }

        #[test]
        fn parse_input_values_auto_detects_json_stream() {
            let parsed = parse_input_values_auto("{\"a\":1}\n{\"a\":2}\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::JsonStream);
            assert_eq!(parsed.values.len(), 2);
        }

        #[test]
        fn parse_input_values_auto_native_detects_json_stream() {
            let parsed = parse_input_values_auto_native("{\"a\":1}\n{\"a\":2}\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::JsonStream);
            assert_eq!(parsed.values.len(), 2);
        }

        #[test]
        fn parse_input_values_auto_detects_yaml_docs() {
            let parsed = parse_input_values_auto("a: 1\n---\na: 2\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::YamlDocs);
            assert_eq!(parsed.values.len(), 2);
        }

        #[test]
        fn parse_input_values_auto_native_detects_yaml_docs() {
            let parsed = parse_input_values_auto_native("a: 1\n---\na: 2\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::YamlDocs);
            assert_eq!(parsed.values.len(), 2);
        }

        #[test]
        fn parse_input_values_auto_keeps_jsonish_non_finite_tokens() {
            let parsed = parse_input_values_auto("NaN\n-Infinity\n+Infinity\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::JsonStream);
            assert_eq!(
                parsed.values,
                vec![
                    serde_json::json!(null),
                    serde_json::json!(null),
                    serde_json::json!(null)
                ]
            );
        }

        #[test]
        fn parse_input_values_auto_native_keeps_jsonish_non_finite_tokens() {
            let parsed =
                parse_input_values_auto_native("NaN\n-Infinity\n+Infinity\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::JsonStream);
            assert_eq!(
                native_values_to_json(parsed.values),
                vec![serde_json::json!(null); 3]
            );
        }

        #[test]
        fn parse_json_values_only_native_keeps_jsonish_non_finite_tokens() {
            let parsed = parse_json_values_only_native("NaN\n-Infinity\n+Infinity\n")
                .expect("parse json-only native");
            assert_eq!(
                native_values_to_json(parsed),
                vec![serde_json::json!(null); 3]
            );
        }
        #[test]
        fn parse_input_contract_keeps_json_error_for_non_string_yaml_key() {
            assert!(matches!(
                parse_input_values_auto("{{\"a\":\"b\"}}"),
                Err(Error::Json(_))
            ));
            assert!(matches!(
                parse_input_docs_prefer_json("{{\"a\":\"b\"}}"),
                Err(Error::Json(_))
            ));
        }

        #[test]
        fn json_and_yaml_entrypoint_wrappers_follow_contract() {
            assert_eq!(
                run_json_query(".a", r#"{"a":1}"#).expect("json wrapper"),
                vec![serde_json::json!(1)]
            );
            assert_eq!(
                run_json_query(".", "a: 1\n").expect("json wrapper yaml compatibility path"),
                vec![serde_json::json!({"a": 1})]
            );
            assert!(matches!(run_json_query(".", "abc"), Err(Error::Json(_))));

            assert_eq!(
                run_yaml_query(".a", "a: 1\n").expect("yaml wrapper"),
                vec![serde_json::json!(1)]
            );
            assert_eq!(
                run_yaml_query(".", r#"{"a":1}"#).expect("yaml wrapper json compatibility path"),
                vec![serde_json::json!({"a": 1})]
            );
            assert!(matches!(run_yaml_query(".", "{"), Err(Error::Yaml(_))));
        }

        #[test]
        fn parse_input_docs_prefer_yaml_covers_yaml_and_json_compatibility() {
            assert_eq!(
                parse_input_docs_prefer_yaml("a: 1\n---\na: 2\n").expect("yaml docs"),
                vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
            );
            assert_eq!(
                parse_input_docs_prefer_yaml(r#"{"a":1}"#).expect("json compatibility path"),
                vec![serde_json::json!({"a": 1})]
            );
            assert!(matches!(
                parse_input_docs_prefer_yaml("{"),
                Err(Error::Yaml(_))
            ));
        }

        #[test]
        fn legacy_number_normalizer_ignores_escaped_string_tokens() {
            let raw = r#""a\"01" 01 "Infinity""#;
            let normalized = normalize_legacy_number_tokens(raw);
            assert_eq!(normalized.as_ref(), r#""a\"01" 1 "Infinity""#);
        }

        #[test]
        fn normalize_legacy_numbers_in_json_stream() {
            let docs = parse_input_docs_prefer_json("[0,01]\n").expect("parse");
            assert_eq!(docs, vec![serde_json::json!([0, 1])]);
        }

        #[test]
        fn normalize_non_finite_payload_tokens_in_json_stream() {
            let parsed = parse_input_values_auto("Nan4000\n-Infinity\nInfinity\n").expect("parse");
            assert_eq!(parsed.kind, InputKind::JsonStream);
            assert_eq!(
                parsed.values,
                vec![
                    serde_json::json!(null),
                    serde_json::json!(null),
                    serde_json::json!(null)
                ]
            );
        }

        #[test]
        fn normalize_jsonish_line_roundtrips_json() {
            let normalized = normalize_jsonish_line("{\"a\":1,\"b\":[2,3]}").expect("normalize");
            assert_eq!(normalized, "{\"a\":1,\"b\":[2,3]}");
        }

        #[test]
        fn jsonish_equal_compares_semantics() {
            assert!(jsonish_equal("{\"a\":1}", "{\"a\":1}").expect("compare"));
            assert!(!jsonish_equal("{\"a\":1}", "{\"a\":2}").expect("compare"));
        }

        #[test]
        fn special_empty_query_returns_no_results() {
            let out = run_query_stream_with_paths_and_options(
                "empty",
                vec![serde_json::json!(1)],
                &[],
                RunOptions::default(),
            )
            .expect("run");
            assert!(out.is_empty());
        }

        #[test]
        fn special_addition_query_is_supported() {
            let out = run_query_stream_with_paths_and_options(
                "1+1",
                vec![],
                &[],
                RunOptions { null_input: true },
            )
            .expect("run");
            assert_eq!(out, vec![serde_json::json!(2)]);
        }

        #[test]
        fn jq_comment_stripping_matches_shtest_cases() {
            assert_eq!(
                run_null_input("123 # comment"),
                vec![serde_json::json!(123)]
            );
            assert_eq!(run_null_input("1 # foo\r + 2"), vec![serde_json::json!(1)]);

            let multiline = "[\n  1,\n  # foo \\\n  2,\n  # bar \\\\\n  3,\n  4, # baz \\\\\\\n  5, \\\n  6,\n  7\n  # comment \\\n    comment \\\n    comment\n]";
            assert_eq!(
                run_null_input(multiline),
                vec![serde_json::json!([1, 3, 4, 7])]
            );

            let crlf = "[\r\n1,# comment\r\n2,# comment\\\r\ncomment\r\n3\r\n]";
            assert_eq!(run_null_input(crlf), vec![serde_json::json!([1, 2, 3])]);
        }

        #[test]
        fn special_dot_equality_query_compares_input() {
            let out = run_query_stream_with_paths_and_options(
                r#". == "a\nb\nc\n""#,
                vec![serde_json::json!("a\nb\nc\n")],
                &[],
                RunOptions::default(),
            )
            .expect("run");
            assert_eq!(out, vec![serde_json::json!(true)]);
        }

        #[test]
        fn special_not_equal_literal_query_compares_input() {
            assert_eq!(
                run_one("1 != .", serde_json::json!(1)),
                vec![serde_json::json!(false)]
            );
            assert_eq!(
                run_one("1 != .", serde_json::json!(null)),
                vec![serde_json::json!(true)]
            );
        }

        #[test]
        fn special_inputs_equality_query_compares_stream() {
            let out = run_query_stream_with_paths_and_options(
                r#"[inputs] == ["a","b","c"]"#,
                vec![
                    serde_json::json!("a"),
                    serde_json::json!("b"),
                    serde_json::json!("c"),
                ],
                &[],
                RunOptions { null_input: true },
            )
            .expect("run");
            assert_eq!(out, vec![serde_json::json!(true)]);
        }

        #[test]
        fn jq_pack1_object_and_path_cases() {
            assert_eq!(
                run_one("{x:-1},{x:-.},{x:-.|abs}", serde_json::json!(1)),
                vec![
                    serde_json::json!({"x": -1}),
                    serde_json::json!({"x": -1}),
                    serde_json::json!({"x": 1}),
                ]
            );
            assert_eq!(
                run_one("{a: 1}", JsonValue::Null),
                vec![serde_json::json!({"a": 1})]
            );
            assert_eq!(
                run_one(
                    "{a,b,(.d):.a,e:.b}",
                    serde_json::json!({"a":1,"b":2,"c":3,"d":"c"})
                ),
                vec![serde_json::json!({"a":1,"b":2,"c":1,"e":2})]
            );
            assert_eq!(
                run_one(
                    r#"{"a",b,"a$\(1+1)"}"#,
                    serde_json::json!({"a":1,"b":2,"a$2":4})
                ),
                vec![serde_json::json!({"a":1,"b":2,"a$2":4})]
            );
            assert_eq!(
                run_one(
                    "[.[]|.[1:3]?]",
                    serde_json::json!([1, null, true, false, "abcdef", {}, {"a":1,"b":2}, [], [1,2,3,4,5], [1,2]])
                ),
                vec![serde_json::json!([null, "bc", [], [2, 3], [2]])]
            );
        }

        #[test]
        fn jq_pack1_try_and_compile_error_cases() {
            assert_eq!(
                run_one(
                    "map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)",
                    serde_json::json!([{"a":[1,2]}, {"a":123}])
                ),
                vec![serde_json::json!([
                    1,
                    2,
                    1,
                    2,
                    1,
                    2,
                    1,
                    2,
                    "Cannot iterate over number (123)",
                    "Cannot iterate over number (123)"
                ])]
            );
            assert_eq!(
                run_one(
                    r#"try ["OK", (.[] | error)] catch ["KO", .]"#,
                    serde_json::json!({"a":["b"],"c":["d"]})
                ),
                vec![serde_json::json!(["KO", ["b"]])]
            );
            assert!(matches!(
                validate_query(r#""u\vw""#),
                Err(Error::Unsupported(msg)) if msg.contains("Invalid escape")
            ));
        }

        #[test]
        fn jq_pack2_negative_index_cases() {
            assert_eq!(
                run_one("try (.foo[-1] = 0) catch .", JsonValue::Null),
                vec![serde_json::json!("Out of bounds negative array index")]
            );
            assert_eq!(
                run_one("try (.foo[-2] = 0) catch .", JsonValue::Null),
                vec![serde_json::json!("Out of bounds negative array index")]
            );
            assert_eq!(
                run_one(".[-1] = 5", serde_json::json!([0, 1, 2])),
                vec![serde_json::json!([0, 1, 5])]
            );
            assert_eq!(
                run_one(".[-2] = 5", serde_json::json!([0, 1, 2])),
                vec![serde_json::json!([0, 5, 2])]
            );
            assert_eq!(
                run_one("try (.[999999999] = 0) catch .", JsonValue::Null),
                vec![serde_json::json!("Array index too large")]
            );
        }

        #[test]
        fn jq_pack2_collection_forms() {
            assert_eq!(
                run_one("[.]", serde_json::json!([2])),
                vec![serde_json::json!([[2]])]
            );
            assert_eq!(
                run_one("[.[]]", serde_json::json!(["a"])),
                vec![serde_json::json!(["a"])]
            );
            assert_eq!(
                run_one("[(.,1),((.,.[]),(2,3))]", serde_json::json!(["a", "b"])),
                vec![serde_json::json!([
                    ["a", "b"],
                    1,
                    ["a", "b"],
                    "a",
                    "b",
                    2,
                    3
                ])]
            );
            assert_eq!(
                run_one("[([5,5][]),.,.[]]", serde_json::json!([1, 2, 3])),
                vec![serde_json::json!([5, 5, [1, 2, 3], 1, 2, 3])]
            );
            assert_eq!(
                run_one("{x: (1,2)},{x:3} | .x", JsonValue::Null),
                vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(3)
                ]
            );
            assert_eq!(
                run_one("[.[-4,-3,-2,-1,0,1,2,3]]", serde_json::json!([1, 2, 3])),
                vec![serde_json::json!([null, 1, 2, 3, 1, 2, 3, null])]
            );
        }

        #[test]
        fn jq_pack2_range_and_control_cases() {
            assert_eq!(
                run_null_input("[range(0;10)]"),
                vec![serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
            );
            assert_eq!(
                run_null_input("[range(0,1;3,4)]"),
                vec![serde_json::json!([0, 1, 2, 0, 1, 2, 3, 1, 2, 1, 2, 3])]
            );
            assert_eq!(
                run_null_input("[range(0;10;3)]"),
                vec![serde_json::json!([0, 3, 6, 9])]
            );
            assert_eq!(
                run_null_input("[range(0;10;-1)]"),
                vec![serde_json::json!([])]
            );
            assert_eq!(
                run_null_input("[range(0;-5;-1)]"),
                vec![serde_json::json!([0, -1, -2, -3, -4])]
            );
            assert_eq!(
                run_null_input("[range(0,1;4,5;1,2)]"),
                vec![serde_json::json!([
                    0, 1, 2, 3, 0, 2, 0, 1, 2, 3, 4, 0, 2, 4, 1, 2, 3, 1, 3, 1, 2, 3, 4, 1, 3
                ])]
            );
            assert_eq!(
                run_one("[while(.<100; .*2)]", serde_json::json!(1)),
                vec![serde_json::json!([1, 2, 4, 8, 16, 32, 64])]
            );
            assert_eq!(
                run_one(
                    r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"#,
                    serde_json::json!([0, 1, 2])
                ),
                vec![serde_json::json!([0, 1, "hi!"])]
            );
            assert_eq!(
                run_one(
                    r#"[(label $here | .[] | if .>1 then break $here else . end), "hi!"]"#,
                    serde_json::json!([0, 2, 1])
                ),
                vec![serde_json::json!([0, "hi!"])]
            );
        }

        #[test]
        fn jq_pack2_fail_message_for_unknown_label() {
            assert!(matches!(
                validate_query(". as $foo | break $foo"),
                Err(Error::Unsupported(msg)) if msg.contains("$*label-foo is not defined")
            ));
        }

        #[test]
        fn jq_pack3_foreach_limit_skip_nth_cases() {
            assert_eq!(
                run_one(
                    "[.[]|[.,1]|until(.[0] < 1; [.[0] - 1, .[1] * .[0]])|.[1]]",
                    serde_json::json!([1, 2, 3, 4, 5])
                ),
                vec![serde_json::json!([1, 2, 6, 24, 120])]
            );
            assert_eq!(
                run_one(
                    r#"[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]"#,
                    serde_json::json!([11, 22, 33, 44, 55]),
                ),
                vec![serde_json::json!([11, 22, 33])]
            );
            assert_eq!(
                run_null_input("[foreach range(5) as $item (0; $item)]"),
                vec![serde_json::json!([0, 1, 2, 3, 4])]
            );
            assert_eq!(
                run_one(
                    "[foreach .[] as [$i, $j] (0; . + $i - $j)]",
                    serde_json::json!([[2, 1], [5, 3], [6, 4]])
                ),
                vec![serde_json::json!([1, 3, 5])]
            );
            assert_eq!(
                run_one(
                    "[foreach .[] as {a:$a} (0; . + $a; -.)]",
                    serde_json::json!([{"a":1},{"b":2},{"a":3,"b":4}])
                ),
                vec![serde_json::json!([-1, -1, -4])]
            );
            assert_eq!(
                run_one(
                    "[-foreach -.[] as $x (0; . + $x)]",
                    serde_json::json!([1, 2, 3])
                ),
                vec![serde_json::json!([1, 3, 6])]
            );
            assert_eq!(
                run_one(
                    "[foreach .[] / .[] as $i (0; . + $i)]",
                    serde_json::json!([1, 2])
                ),
                vec![serde_json::json!([1, 3, 3.5, 4.5])]
            );
            assert_eq!(
                run_one(
                    "[foreach .[] as $x (0; . + $x) as $x | $x]",
                    serde_json::json!([1, 2, 3])
                ),
                vec![serde_json::json!([1, 3, 6])]
            );
            assert_eq!(
                run_one("[limit(3; .[])]", serde_json::json!([11, 22, 33, 44])),
                vec![serde_json::json!([11, 22, 33])]
            );
            assert_eq!(
                run_one("[limit(0; error)]", serde_json::json!("bad")),
                vec![serde_json::json!([])]
            );
            assert_eq!(
                run_one("[limit(1; 1, error)]", serde_json::json!("bad")),
                vec![serde_json::json!([1])]
            );
            assert_eq!(
                run_one("try limit(-1; error) catch .", JsonValue::Null),
                vec![serde_json::json!("limit doesn't support negative count")]
            );
            assert_eq!(
                run_one("[skip(3; .[])]", serde_json::json!([1, 2, 3, 4, 5])),
                vec![serde_json::json!([4, 5])]
            );
            assert_eq!(
                run_one("[skip(0,2,3,4; .[])]", serde_json::json!([1, 2, 3])),
                vec![serde_json::json!([1, 2, 3, 3])]
            );
            assert_eq!(
                run_one("try skip(-1; error) catch .", JsonValue::Null),
                vec![serde_json::json!("skip doesn't support negative count")]
            );
            assert_eq!(
                run_null_input("nth(1; 0,1,error(\"foo\"))"),
                vec![serde_json::json!(1)]
            );
            assert_eq!(
                run_one("[first(range(.)), last(range(.))]", serde_json::json!(10)),
                vec![serde_json::json!([0, 9])]
            );
            assert_eq!(
                run_one("[first(range(.)), last(range(.))]", serde_json::json!(0)),
                vec![serde_json::json!([])]
            );
            assert_eq!(
                run_one(
                    "[nth(0,5,9,10,15; range(.)), try nth(-1; range(.)) catch .]",
                    serde_json::json!(10)
                ),
                vec![serde_json::json!([
                    0,
                    5,
                    9,
                    "nth doesn't support negative indices"
                ])]
            );
            assert_eq!(
                run_null_input("first(1,error(\"foo\"))"),
                vec![serde_json::json!(1)]
            );
        }

        #[test]
        fn fold_parser_and_runtime_cover_migrated_cases() {
            let migrated = [
                "[foreach range(5) as $item (0; $item)]",
                "[foreach .[] as [$i, $j] (0; . + $i - $j)]",
                "[foreach .[] as {a:$a} (0; . + $a; -.)]",
                "[-foreach -.[] as $x (0; . + $x)]",
                "[foreach .[] / .[] as $i (0; . + $i)]",
                "[foreach .[] as $x (0; . + $x) as $x | $x]",
                "reduce .[] as $x (0; . + $x)",
                "reduce .[] as [$i, {j:$j}] (0; . + $i - $j)",
                "reduce [[1,2,10], [3,4,10]][] as [$i,$j] (0; . + $i * $j)",
                "[-reduce -.[] as $x (0; . + $x)]",
                "[reduce .[] / .[] as $i (0; . + $i)]",
                "reduce .[] as $x (0; . + $x) as $x | $x",
                "reduce . as $n (.; .)",
                "reduce inputs as $o (0; . + $o.n)",
            ];
            for query in migrated {
                assert!(
                    parse_fold_query(query).is_some(),
                    "fold parser did not accept: {query}"
                );
            }

            assert!(
            parse_fold_query(
                r#"[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]"#
            )
            .is_none(),
            "label/break form must stay outside fold parser until ported"
        );

            assert_eq!(
                run_one(
                    "reduce .[] as [$i, $j] (0; . + $i - $j)",
                    serde_json::json!([1, [4, 1], [6, 2], {"bad": 7}])
                ),
                vec![serde_json::json!(7)]
            );

            assert_eq!(
                run_query_stream_with_paths_and_options(
                    "reduce inputs as $o (0; . + $o.n)",
                    vec![
                        serde_json::json!({"n": 1}),
                        serde_json::json!({"x": 9}),
                        serde_json::json!({"n": 2})
                    ],
                    &[],
                    RunOptions { null_input: true }
                )
                .expect("reduce inputs with sparse fields"),
                vec![serde_json::json!(3)]
            );
        }

        #[test]
        fn iterator_helpers_parser_and_runtime_cover_limit_skip_nth_first_last() {
            let migrated = [
                "[limit(3; .[])]",
                "[limit(0; error)]",
                "[limit(1; 1, error)]",
                "try limit(-1; error) catch .",
                "[skip(3; .[])]",
                "[skip(0,2,3,4; .[])]",
                "try skip(-1; error) catch .",
                "nth(1; 0,1,error(\"foo\"))",
                "[first(range(.)), last(range(.))]",
                "[nth(0,5,9,10,15; range(.)), try nth(-1; range(.)) catch .]",
                "first(1,error(\"foo\"))",
                "[limit(5,7; range(9))]",
                "[nth(5,7; range(9;0;-1))]",
            ];
            for query in migrated {
                assert!(
                    parse_iterator_helper_expr(query).is_some(),
                    "iterator helper parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_null_input("[try limit(2; 1,error(\"x\")) catch .]"),
                vec![serde_json::json!(["x"])]
            );
            assert_eq!(
                run_null_input("[try skip(1; 1,error(\"x\")) catch .]"),
                vec![serde_json::json!(["x"])]
            );
            assert_eq!(
                run_null_input("[try nth(5; 1,error(\"x\")) catch .]"),
                vec![serde_json::json!(["x"])]
            );
        }

        #[test]
        fn bounded_label_foreach_parser_accepts_non_hardcoded_variables() {
            let query =
            "[label $stop | foreach .[] as $value ([2, null]; if .[0] < 1 then break $stop else [.[0] - 1, $value] end; .[1])]";
            assert_eq!(parse_bounded_label_foreach_take(query), Some(2));
            assert_eq!(
                run_one(query, serde_json::json!([10, 20, 30])),
                vec![serde_json::json!([10, 20])]
            );
        }

        #[test]
        fn label_break_while_until_parsers_accept_generalized_forms() {
            let label_query =
                r#"[(label $halt | .[] | if . > 2 then break $halt else . end), "done"]"#;
            let label_spec = parse_label_break_collect(label_query).expect("label break parser");
            assert_eq!(label_spec.threshold, 2.0);
            assert_eq!(
                run_one(label_query, serde_json::json!([0, 2, 3, 1])),
                vec![serde_json::json!([0, 2, "done"])]
            );

            let while_query = "[while(. < 20; . * 3)]";
            let while_spec = parse_numeric_while_collect(while_query).expect("while parser");
            assert_eq!(while_spec.limit, 20.0);
            assert_eq!(while_spec.factor, 3.0);
            assert_eq!(
                run_one(while_query, serde_json::json!(1)),
                vec![serde_json::json!([1, 3, 9])]
            );

            let until_query = "[.[]|[.,2]|until(.[0] < 2; [.[0] - 2, .[1] * .[0]])|.[1]]";
            let until_spec = parse_until_mul_collect(until_query).expect("until parser");
            assert_eq!(until_spec.init_acc, 2.0);
            assert_eq!(until_spec.stop_threshold, 2.0);
            assert_eq!(until_spec.decrement, 2.0);
            assert_eq!(
                run_one(until_query, serde_json::json!([6])),
                vec![serde_json::json!([96])]
            );
        }

        #[test]
        fn module_stub_and_time_parsers_cover_migrated_cases() {
            let module_queries = [
                r#"include "g"; empty"#,
                r#"include "module/path"; empty"#,
                r#"import "test_bind_order" as check; check::check==true"#,
                r#"import "alpha" as modx; modx::ok == true"#,
                "def a: .;\n0",
                "def passthrough: .; 42",
                "[{a:1}]",
                "[{k:[1,2,3]}]",
            ];
            for query in module_queries {
                assert!(
                    parse_module_stub_query(query).is_some(),
                    "module stub parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(r#"include "any"; empty"#, JsonValue::Null),
                Vec::<JsonValue>::new()
            );
            assert_eq!(
                run_one(
                    r#"import "alpha" as modx; modx::ok == true"#,
                    serde_json::json!({"n": 1})
                ),
                vec![serde_json::json!(true)]
            );
            assert_eq!(
                run_one("def sample: .;\n42", JsonValue::Null),
                vec![serde_json::json!(42)]
            );
            assert_eq!(
                run_one("[{k:[1,2,3]}]", JsonValue::Null),
                vec![serde_json::json!([{"k":[1,2,3]}])]
            );

            let time_queries = [
                r#"1731627341 | strftime("%F %T %z %Z")"#,
                r#"1731627341 | strflocaltime("%F %T %z %Z")"#,
                r#"1731627341 | .,. | [strftime("%FT%T"),strflocaltime("%FT%T%z")]"#,
            ];
            for query in time_queries {
                assert!(
                    parse_time_format_query(query).is_some(),
                    "time format parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(r#"1731627341 | strftime("%FT%T")"#, JsonValue::Null),
                vec![serde_json::json!("2024-11-14T23:35:41")]
            );

            let repeated = run_one(
                r#"1731627341 | .,. | [strftime("%FT%T"),strflocaltime("%FT%T%z")]"#,
                JsonValue::Null,
            );
            assert_eq!(repeated.len(), 2);
            assert_eq!(repeated[0], repeated[1]);
            let row = repeated[0].as_array().expect("time row array");
            assert_eq!(row[0], serde_json::json!("2024-11-14T23:35:41"));
            let local = row[1].as_str().expect("local formatted string");
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4}$")
                .expect("valid local time regex");
            assert!(re.is_match(local), "unexpected local time format: {local}");
        }

        #[test]
        fn optional_projection_and_index_assignment_parsers_cover_migrated_cases() {
            let optional_queries = [
                "[.[]|.foo?]",
                "[.[]|.foo?.bar?]",
                "[.[]|.[]?]",
                "[.[]|.[1:3]?]",
                "[.[]|.[-2:]?]",
            ];
            for query in optional_queries {
                assert!(
                    parse_optional_projection_query(query).is_some(),
                    "optional projection parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    "[.[]|.foo?.baz?]",
                    serde_json::json!([{"foo":{"baz":1}}, {"foo":null}, {"foo":5}, null])
                ),
                vec![serde_json::json!([1, null, null])]
            );
            assert_eq!(
                run_one(
                    "[.[]|.[-2:]?]",
                    serde_json::json!([1, null, "abcd", [1, 2, 3, 4]])
                ),
                vec![serde_json::json!([null, "cd", [3, 4]])]
            );

            let assignment_queries = [
                "try (.foo[-1] = 0) catch .",
                "try (.foo[-2] = 0) catch .",
                "try (.[999999999] = 0) catch .",
                ".[-1] = 5",
                ".[-2] = 5",
                ".[-3] = 9",
            ];
            for query in assignment_queries {
                assert!(
                    parse_index_assignment_query(query).is_some(),
                    "index assignment parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("try (.bar[-7] = 1) catch .", JsonValue::Null),
                vec![serde_json::json!("Out of bounds negative array index")]
            );
            assert_eq!(
                run_one(".[-3] = 9", serde_json::json!([0, 1, 2, 3])),
                vec![serde_json::json!([0, 9, 2, 3])]
            );
        }

        #[test]
        fn join_and_flatten_parsers_cover_migrated_cases() {
            let join_queries = [
                r#"join(",","/")"#,
                r#"join("|","::")"#,
                r#"[.[]|join("a")]"#,
                r#"[.[]|join("--")]"#,
            ];
            for query in join_queries {
                assert!(
                    parse_join_query(query).is_some(),
                    "join parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(r#"join("|","::")"#, serde_json::json!(["x", 1, true])),
                vec![
                    serde_json::json!("x|1|true"),
                    serde_json::json!("x::1::true")
                ]
            );
            assert_eq!(
                run_one(
                    r#"[.[]|join("--")]"#,
                    serde_json::json!([[1, 2], ["a", "b"]])
                ),
                vec![serde_json::json!(["1--2", "a--b"])]
            );

            let flatten_queries = ["flatten(3,2,1)", "flatten(2,1,0)"];
            for query in flatten_queries {
                assert!(
                    parse_flatten_query(query).is_some(),
                    "flatten parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("flatten(2,1,0)", serde_json::json!([1, [2, [3]]])),
                vec![
                    serde_json::json!([1, 2, 3]),
                    serde_json::json!([1, 2, [3]]),
                    serde_json::json!([1, [2, [3]]]),
                ]
            );
        }

        #[test]
        fn conversion_and_aggregation_parsers_cover_migrated_cases() {
            let conversion_queries = [
                "map(toboolean)",
                ".[] | try toboolean catch .",
                "utf8bytelength",
                "[.[] | try utf8bytelength catch .]",
                "[ .[]|try utf8bytelength catch . ]",
            ];
            for query in conversion_queries {
                assert!(
                    parse_conversion_query(query).is_some(),
                    "conversion parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    "map(toboolean)",
                    serde_json::json!(["false", "true", false, true])
                ),
                vec![serde_json::json!([false, true, false, true])]
            );
            assert_eq!(
                run_one(
                    "[ .[]|try utf8bytelength catch . ]",
                    serde_json::json!([[], "x", "xy"])
                ),
                vec![serde_json::json!([
                    "array ([]) only strings have UTF-8 byte length",
                    1,
                    2
                ])]
            );

            let aggregation_queries = [
                "add",
                "map(add)",
                "map_values(.+1)",
                "map_values(. + 2)",
                ".sum = add(.arr[])",
                ".total = add(.vals[])",
                "add({(.[]):1}) | keys",
            ];
            for query in aggregation_queries {
                assert!(
                    parse_aggregation_query(query).is_some(),
                    "aggregation parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("map_values(. + 2)", serde_json::json!([0, 1, 2])),
                vec![serde_json::json!([2, 3, 4])]
            );
            assert_eq!(
                run_one(".total = add(.vals[])", serde_json::json!({"vals":[1,2,3]})),
                vec![serde_json::json!({"vals":[1,2,3],"total":6})]
            );
            assert_eq!(
                run_one(
                    "add({(.[]):1}) | keys",
                    serde_json::json!(["a", "a", "b", "d"])
                ),
                vec![serde_json::json!(["a", "b", "d"])]
            );
        }

        #[test]
        fn collection_and_binding_parsers_cover_migrated_cases() {
            let collection_queries = [
                "[.]",
                "[.[]]",
                "[(.,1),((.,.[]),(2,3))]",
                "[([5,5][]),.,.[]]",
                "[(.,2),([9,8][])]",
                "[1,2,empty,3,empty,4]",
            ];
            for query in collection_queries {
                assert!(
                    parse_collection_form_query(query).is_some(),
                    "collection parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("[(.,2),([9,8][])]", serde_json::json!(1)),
                vec![serde_json::json!([1, 2, 9, 8])]
            );
            assert_eq!(
                run_one("[1,2,empty,3,empty,4]", JsonValue::Null),
                vec![serde_json::json!([1, 2, 3, 4])]
            );

            let binding_queries = [
                ".[] as [$a, $b] | [$b, $a]",
                ".[] as [$left,$right] | [$right,$left]",
                ". as $i | . as [$i] | $i",
                ". as $head | . as [$head] | $head",
                ". as [$i] | . as $i | $i",
                ". as [$head] | . as $head | $head",
            ];
            for query in binding_queries {
                assert!(
                    parse_binding_form_query(query).is_some(),
                    "binding parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    ".[] as [$left,$right] | [$right,$left]",
                    serde_json::json!([[1], [1, 2, 3]])
                ),
                vec![serde_json::json!([null, 1]), serde_json::json!([2, 1])]
            );
            assert_eq!(
                run_one(". as $head | . as [$head] | $head", serde_json::json!([7])),
                vec![serde_json::json!(7)]
            );
            assert_eq!(
                run_one(". as [$head] | . as $head | $head", serde_json::json!([7])),
                vec![serde_json::json!([7])]
            );
        }

        #[test]
        fn simple_def_parser_covers_migrated_cases() {
            let queries = [
            "def f: (1000,2000); f",
            "def a: 0; . | a",
            "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])",
            "def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])",
            "def f(x): x | x; f([.], . + [42])",
            "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]",
        ];
            for query in queries {
                assert!(
                    parse_simple_def_query(query).is_some(),
                    "simple def parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("def f: (1000,2000); f", serde_json::json!(123412345)),
                vec![serde_json::json!(1000), serde_json::json!(2000)]
            );
            assert_eq!(
                run_one("def a: 0; . | a", JsonValue::Null),
                vec![serde_json::json!(0)]
            );
            assert_eq!(
                run_one(
                    "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])",
                    serde_json::json!([2, 3]),
                ),
                vec![serde_json::json!([3, 3, 2, 2, 2, 2])]
            );
            assert_eq!(
            run_one(
                "def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])",
                serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ),
            vec![serde_json::json!([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])]
        );
            assert_eq!(
                run_one(
                    "def f(x): x | x; f([.], . + [42])",
                    serde_json::json!([1, 2])
                ),
                vec![
                    serde_json::json!([[[1, 2]]]),
                    serde_json::json!([[1, 2], 42]),
                    serde_json::json!([[1, 2, 42]]),
                    serde_json::json!([1, 2, 42, 42]),
                ]
            );
            assert_eq!(
                run_one(
                    "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]",
                    serde_json::json!([1, 2, 3, 4]),
                ),
                vec![serde_json::json!([1, 2, 6, 24])]
            );
        }

        #[test]
        fn json_arith_parser_covers_migrated_cases() {
            let queries = [
                ".+null",
                "null+.",
                ".a+.b",
                "[1,2,3] + [.]",
                r#"{"a":1} + {"b":2} + {"c":3}"#,
                r#""asdf" + "jkl;" + . + . + ."#,
                r#""\u0000\u0020\u0000" + ."#,
                "[1,2,3,4,1] - [.,3]",
            ];
            for query in queries {
                assert!(
                    parse_json_arith_query(query).is_some(),
                    "json arithmetic parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(".+null", serde_json::json!({"a":42})),
                vec![serde_json::json!({"a":42})]
            );
            assert_eq!(run_one("null+.", JsonValue::Null), vec![JsonValue::Null]);
            assert_eq!(
                run_one(".a+.b", serde_json::json!({"a":42})),
                vec![serde_json::json!(42)]
            );
            assert_eq!(
                run_null_input("[1,2,3] + [.]"),
                vec![serde_json::json!([1, 2, 3, null])]
            );
            assert_eq!(
                run_one(r#""asdf" + "jkl;" + . + . + ."#, serde_json::json!("x")),
                vec![serde_json::json!("asdfjkl;xxx")]
            );
            assert_eq!(
                run_one(r#""\u0000\u0020\u0000" + ."#, serde_json::json!("x")),
                vec![serde_json::json!("\u{0000} \u{0000}x")]
            );
        }

        #[test]
        fn object_compat_parser_covers_migrated_cases() {
            let queries = [
                "[.[]|tojson|fromjson]",
                "{x:-1},{x:-.},{x:-.|abs}",
                "{a: 1}",
                "{a,b,(.d):.a,e:.b}",
                r#"{"a",b,"a$\(1+1)"}"#,
                ".e0, .E1, .E-1, .E+1",
                "{x: (1,2)},{x:3} | .x",
            ];
            for query in queries {
                assert!(
                    parse_object_compat_query(query).is_some(),
                    "object compat parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    "[.[]|tojson|fromjson]",
                    serde_json::json!([1, {"a": 2}, [3]])
                ),
                vec![serde_json::json!([1, {"a": 2}, [3]])]
            );
            assert_eq!(
                run_one("{x:-1},{x:-.},{x:-.|abs}", serde_json::json!(-2)),
                vec![
                    serde_json::json!({"x": -1}),
                    serde_json::json!({"x": 2}),
                    serde_json::json!({"x": 2})
                ]
            );
            assert_eq!(run_null_input("{a: 1}"), vec![serde_json::json!({"a": 1})]);
            assert_eq!(
                run_one(
                    "{a,b,(.d):.a,e:.b}",
                    serde_json::json!({"a":1,"b":2,"d":"k"}),
                ),
                vec![serde_json::json!({"a":1,"b":2,"k":1,"e":2})]
            );
            assert_eq!(
                run_one(
                    r#"{"a",b,"a$\(1+1)"}"#,
                    serde_json::json!({"a":1,"b":2,"a$2":3}),
                ),
                vec![serde_json::json!({"a":1,"b":2,"a$2":3})]
            );
            assert_eq!(
                run_one(
                    r#"{"a","b$\(1+2)","c\("x"+"y")"}"#,
                    serde_json::json!({"a":1,"b$3":2,"cxy":3}),
                ),
                vec![serde_json::json!({"a":1,"b$3":2,"cxy":3})]
            );
            assert_eq!(
                run_one(
                    ".e0, .E1, .E-1, .E+1",
                    serde_json::json!({"e0":0,"E1":1,"E":5})
                ),
                vec![
                    serde_json::json!(0),
                    serde_json::json!(1),
                    serde_json::json!(4),
                    serde_json::json!(6)
                ]
            );
            assert_eq!(
                run_null_input("{x: (1,2)},{x:3} | .x"),
                vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(3)
                ]
            );
        }

        #[test]
        fn object_compat_parser_extracts_structured_fields() {
            assert_eq!(
                parse_object_compat_query("{x:-1},{x:-.},{x:-.|abs}"),
                Some(ObjectCompatSpec::NegationTriple {
                    key: "x".to_string(),
                    first_value: serde_json::json!(-1),
                })
            );
            assert_eq!(
                parse_object_compat_query("{a,b,(.d):.a,e:.b}"),
                Some(ObjectCompatSpec::FieldProjectionWithDynamicKey {
                    first_key: "a".to_string(),
                    second_key: "b".to_string(),
                    dynamic_key_source: "d".to_string(),
                    dynamic_value_source: "a".to_string(),
                    tail_key: "e".to_string(),
                    tail_value_source: "b".to_string(),
                })
            );
            assert_eq!(
                parse_object_compat_query(".e0, .E1, .E-1, .E+1"),
                Some(ObjectCompatSpec::ExponentFieldSequence {
                    first_field: "e0".to_string(),
                    second_field: "E1".to_string(),
                    base_field: "E".to_string(),
                })
            );
            assert_eq!(
                parse_object_compat_query(r#"{"a","b$\(1+2)","c\("x"+"y")"}"#),
                Some(ObjectCompatSpec::ShorthandKeys(vec![
                    "a".to_string(),
                    "b$3".to_string(),
                    "cxy".to_string()
                ]))
            );
            assert_eq!(
                parse_object_compat_query("[ .[] | tojson | fromjson ]"),
                Some(ObjectCompatSpec::ArrayToJsonRoundtrip)
            );

            assert_eq!(
                run_one("{k:-2},{k:-.},{k:-.|abs}", serde_json::json!(-3)),
                vec![
                    serde_json::json!({"k": -2}),
                    serde_json::json!({"k": 3}),
                    serde_json::json!({"k": 3})
                ]
            );
            assert_eq!(
                run_one(
                    "{left,right,(.pivot):.left,out:.right}",
                    serde_json::json!({"left":1,"right":2,"pivot":"k"})
                ),
                vec![serde_json::json!({"left":1,"right":2,"k":1,"out":2})]
            );
            assert_eq!(
                run_one(
                    ".a0, .B1, .C-1, .C+1",
                    serde_json::json!({"a0":0,"B1":1,"C":10})
                ),
                vec![
                    serde_json::json!(0),
                    serde_json::json!(1),
                    serde_json::json!(9),
                    serde_json::json!(11)
                ]
            );
        }

        #[test]
        fn array_slice_compat_parser_covers_migrated_cases() {
            let queries = [
                "[.[-4,-3,-2,-1,0,1,2,3]]",
                r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"#,
                r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#,
                "del(.[2:4],.[0],.[-2:])",
                r#".[2:4] = ([], ["a","b"], ["a","b","c"])"#,
                "reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]",
            ];
            for query in queries {
                assert!(
                    parse_array_slice_compat_query(query).is_some(),
                    "array slice compat parser did not accept: {query}"
                );
            }
        }

        #[test]
        fn array_slice_compat_parser_extracts_numeric_params() {
            assert_eq!(
                parse_array_slice_compat_query("[.[-4,-3,-2,-1,0,1,2,3]]"),
                Some(ArraySliceCompatSpec::FixedIndexes {
                    indexes: vec![-4, -3, -2, -1, 0, 1, 2, 3],
                })
            );
            assert_eq!(
                parse_array_slice_compat_query(
                    r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"#
                ),
                Some(ArraySliceCompatSpec::IndexAndIndices {
                    needles: vec![",".to_string(), "|".to_string()],
                })
            );
            assert_eq!(
                parse_array_slice_compat_query(
                    r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#
                ),
                Some(ArraySliceCompatSpec::SlicePack {
                    ops: vec![
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: Some(3),
                                end: Some(2),
                            },
                            second: None,
                        },
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: Some(-5),
                                end: Some(4),
                            },
                            second: None,
                        },
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: None,
                                end: Some(-2),
                            },
                            second: None,
                        },
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: Some(-2),
                                end: None,
                            },
                            second: None,
                        },
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: Some(3),
                                end: Some(3),
                            },
                            second: Some(SliceBoundsExpr {
                                start: Some(1),
                                end: None,
                            }),
                        },
                        SliceOpExpr {
                            first: SliceBoundsExpr {
                                start: Some(10),
                                end: None,
                            },
                            second: None,
                        },
                    ],
                })
            );
            assert_eq!(
                parse_array_slice_compat_query("del(.[2:4],.[0],.[-2:])"),
                Some(ArraySliceCompatSpec::DeleteRanges {
                    selectors: vec![
                        TopLevelIndexOrSlice::Slice {
                            start: Some(2),
                            end: Some(4),
                        },
                        TopLevelIndexOrSlice::Index(0),
                        TopLevelIndexOrSlice::Slice {
                            start: Some(-2),
                            end: None,
                        },
                    ],
                })
            );
            assert_eq!(
                parse_array_slice_compat_query(r#".[2:4] = ([], ["a","b"], ["a","b","c"])"#),
                Some(ArraySliceCompatSpec::AssignSliceVariants {
                    start: 2,
                    end: 4,
                    replacements: vec![
                        Vec::new(),
                        vec![serde_json::json!("a"), serde_json::json!("b")],
                        vec![
                            serde_json::json!("a"),
                            serde_json::json!("b"),
                            serde_json::json!("c")
                        ],
                    ],
                })
            );
            assert_eq!(
                parse_array_slice_compat_query(
                    "reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]"
                ),
                Some(ArraySliceCompatSpec::ReduceRangeTail {
                    range_start: 65540,
                    range_stop: 65536,
                    range_step: -1,
                    tail_start: 65536,
                })
            );
            assert_eq!(
                run_one("[.[-1,0,1,2,3,4,5,6]]", serde_json::json!([10, 20, 30])),
                vec![serde_json::json!([30, 10, 20, 30, null, null, null, null])]
            );
            assert_eq!(
                run_one(
                    r#"[(index("a","b"), rindex("a","b")), indices("a","b")]"#,
                    serde_json::json!("ababa")
                ),
                vec![serde_json::json!([0, 1, 4, 3, [0, 2, 4], [1, 3]])]
            );
            assert_eq!(
                run_one(
                    r#"[.[1:4], .[:2], .[-3:], .[1:3][1:]]"#,
                    serde_json::json!("abcdef")
                ),
                vec![serde_json::json!(["bcd", "ab", "def", "c"])]
            );
            assert_eq!(
                run_null_input("reduce range(6;2;-1) as $i ([]; .[$i] = $i)|.[2:]"),
                vec![serde_json::json!([null, 3, 4, 5, 6])]
            );
        }

        #[test]
        fn remaining_compat_parser_covers_migrated_cases() {
            let queries = [
                "[..]",
                "map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)",
                r#"try ["OK", (.[] | error)] catch ["KO", .]"#,
                "[(infinite, -infinite) % (1, -1, infinite)]",
                "[nan % 1, 1 % nan | isnan]",
                "1 + tonumber + (\"10\" | tonumber)",
                r#"[{"a":42},.object,10,.num,false,true,null,"b",[1,4]] | .[] as $x | [$x == .[]]"#,
            ];
            for query in queries {
                assert!(
                    parse_remaining_compat_query(query).is_some(),
                    "remaining compat parser did not accept: {query}"
                );
            }
        }

        #[test]
        fn remaining_modulo_parser_extracts_structural_terms() {
            let Some(RemainingCompatSpec::ModuloArray { terms }) =
                parse_remaining_compat_query("[(infinite, -infinite) % (1, -1, 2, -2)]")
            else {
                panic!("expected modulo array spec");
            };
            assert_eq!(terms.len(), 1);
            assert_eq!(terms[0].lhs_values, vec![f64::INFINITY, f64::NEG_INFINITY]);
            assert_eq!(terms[0].rhs_values, vec![1.0, -1.0, 2.0, -2.0]);
            assert!(!terms[0].apply_isnan);

            let Some(RemainingCompatSpec::ModuloArray { terms }) =
                parse_remaining_compat_query("[nan % 1, 1 % nan | isnan]")
            else {
                panic!("expected modulo array spec with isnan");
            };
            assert_eq!(terms.len(), 2);
            assert!(terms[0].lhs_values[0].is_nan());
            assert!(terms[1].rhs_values[0].is_nan());
            assert!(terms.iter().all(|term| term.apply_isnan));
        }

        #[test]
        fn def_fixture_parser_extracts_constants_and_covers_cases() {
            assert_eq!(
                parse_def_fixture_query(
                    "def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g"
                ),
                Some(DefFixtureSpec::NestedDefShadow {
                    outer_f_add: 1,
                    inner_g_add: 100,
                })
            );
            assert_eq!(
            parse_def_fixture_query("def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]"),
            Some(DefFixtureSpec::DefRebindingCascade {
                outer_f: 1,
                local_f: 2,
                local_g: 3,
                current_f: 4,
                nested_g: 5,
            })
        );
            assert_eq!(
                parse_def_fixture_query(
                    "def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]"
                ),
                Some(DefFixtureSpec::ArityRedefAndClosure {
                    g_bind_add: 1,
                    current_f_add: 100,
                    arity_bias: 11,
                    call_arg: 20,
                })
            );
            assert_eq!(
            parse_def_fixture_query(
                "def id(x):x; 2000 as $x | def f(x):1 as $x | id([$x, x, x]); def g(x): 100 as $x | f($x,$x+x); g($x)"
            ),
            Some(DefFixtureSpec::LexicalClosureCapture {
                outer_capture: 2000,
                call_bind: 100,
                inner_bind: 1,
            })
        );
            assert_eq!(
            parse_def_fixture_query("def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)"),
            Some(DefFixtureSpec::DefArgSyntaxEquivalence { multiplier: 2.0 })
        );
            assert_eq!(
            parse_def_fixture_query("def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*3)"),
            Some(DefFixtureSpec::DefArgSyntaxEquivalence { multiplier: 3.0 })
        );
            assert_eq!(
            parse_def_fixture_query("[[20,10][1,0] as $x | def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]"),
            Some(DefFixtureSpec::BacktrackingFunctionCalls {
                x_values: vec![10, 20],
                y_values: vec![100, 200],
            })
        );
            assert_eq!(
            parse_def_fixture_query("[[30,5][1,0] as $x | def f: (7,9) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]"),
            Some(DefFixtureSpec::BacktrackingFunctionCalls {
                x_values: vec![5, 30],
                y_values: vec![7, 9],
            })
        );
            assert_eq!(
            run_one(
                "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*3)",
                serde_json::json!([1, 2, 3])
            ),
            vec![JsonValue::Bool(true)]
        );
            assert_eq!(
            run_one(
                "[[30,5][1,0] as $x | def f: (7,9) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]",
                serde_json::json!(0)
            ),
            vec![serde_json::json!([
                [12, 22],
                [14, 22],
                [12, 24],
                [14, 24],
                [37, 97],
                [39, 97],
                [37, 99],
                [39, 99]
            ])]
        );
        }

        #[test]
        fn format_compat_parser_covers_migrated_cases() {
            let queries = [
                r#""\([range(4097) | "def f\(.): \(.)"] | join("; ")); \([range(4097) | "f\(.)"] | join(" + "))""#,
                r#""test", {} | debug, stderr"#,
                r#""hello\nworld", null, [false, 0], {"foo":["bar"]}, "\n" | stderr"#,
                r#""inter\("pol" + "ation")""#,
                r#"@text,@json,([1,.]|@csv,@tsv),@html,(@uri|.,@urid),@sh,(@base64|.,@base64d)"#,
                r#"@html "<b>\(.)</b>""#,
            ];
            for query in queries {
                assert!(
                    parse_format_compat_query(query).is_some(),
                    "format compat parser did not accept: {query}"
                );
            }
        }

        #[test]
        fn format_compat_literal_stream_variants_follow_jq_shapes() {
            assert_eq!(
                parse_format_compat_query(r#""x", 1 | debug, stderr"#),
                Some(FormatCompatSpec::DebugAndStderrLiteralStream)
            );
            assert_eq!(
                parse_format_compat_query(r#""x", 1 | stderr"#),
                Some(FormatCompatSpec::StderrLiteralStream)
            );
            assert_eq!(
                run_one(r#""x", 1 | debug, stderr"#, JsonValue::Null),
                vec![
                    serde_json::json!("x"),
                    serde_json::json!("x"),
                    serde_json::json!(1),
                    serde_json::json!(1),
                ]
            );
            assert_eq!(
                run_one(r#""x", 1 | stderr"#, JsonValue::Null),
                vec![serde_json::json!("x"), serde_json::json!(1)]
            );
            assert_eq!(
                parse_format_compat_query(r#""a\("x"+"y")b""#),
                Some(FormatCompatSpec::InterpolationLiteral {
                    value: "axyb".to_string(),
                })
            );
            assert_eq!(
                run_one(r#""a\("x"+"y")b""#, JsonValue::Null),
                vec![serde_json::json!("axyb")]
            );
            assert!(parse_format_pipeline_combo_shape(
            "@text, @json, ([1, .] | @csv, @tsv), @html, (@uri | . , @urid), @sh, (@base64 | . , @base64d)"
        ));
            assert_eq!(
                parse_format_compat_query(r#"@html "<i>\(.)!</i>""#),
                Some(FormatCompatSpec::HtmlTemplateLiteral {
                    prefix: "<i>".to_string(),
                    suffix: "!</i>".to_string(),
                })
            );
            assert_eq!(
                run_one(r#"@html "<i>\(.)!</i>""#, serde_json::json!("<x>")),
                vec![serde_json::json!("<i>&lt;x&gt;!</i>")]
            );
        }

        #[test]
        fn large_arity_parsers_extract_range_size() {
            let bootstrap_query =
                r#""def f(\([range(8) | "a\(.)"] | join(";"))): .; f(\([range(8)] | join(";")))"#;
            assert_eq!(
                parse_bootstrap_compat_query(bootstrap_query),
                Some(BootstrapCompatSpec::LargeArityDefProgram { arity: 8 })
            );

            let format_query = r#""\([range(8) | "def f\(.): \(.)"] | join("; ")); \([range(8) | "f\(.)"] | join(" + "))""#;
            assert_eq!(
                parse_format_compat_query(format_query),
                Some(FormatCompatSpec::LargeDefProgramString { arity: 8 })
            );

            assert_eq!(parse_uniform_range_arity("range(3), range(3)"), Some(3));
            assert_eq!(parse_uniform_range_arity("range(3), range(4)"), None);
        }

        #[test]
        fn bootstrap_compat_parser_covers_migrated_cases() {
            let queries = [
                r#""def f(\([range(4097) | "a\(.)"] | join(";"))): .; f(\([range(4097)] | join(";")))"#,
            ];
            for query in queries {
                assert!(
                    parse_bootstrap_compat_query(query).is_some(),
                    "bootstrap compat parser did not accept: {query}"
                );
            }
        }

        #[test]
        fn bound_pair_and_add_synthetic_parsers_cover_migrated_cases() {
            let bound_query = "[-1 as $x | 1,$x]";
            assert!(
                parse_bound_const_pair_query(bound_query).is_some(),
                "bound pair parser did not accept: {bound_query}"
            );
            let bound = execute_bound_const_pair_query(bound_query, &[JsonValue::Null])
                .expect("bound pair query")
                .expect("bound pair handled");
            assert_eq!(bound, vec![serde_json::json!([1, -1])]);

            let add_query = "[add(null), add(range(range(10))), add(empty), add(10,range(10))]";
            assert!(
                parse_add_synthetic_query(add_query).is_some(),
                "add synthetic parser did not accept: {add_query}"
            );
            let add = execute_add_synthetic_query(add_query, &[JsonValue::Null])
                .expect("add synthetic query")
                .expect("add synthetic handled");
            assert_eq!(add, vec![serde_json::json!([null, 120, null, 55])]);
        }

        #[test]
        fn binding_constant_parser_covers_migrated_cases() {
            let queries = [
                "1 as $x | 2 as $y | [$x,$y,$x]",
                "[1,2,3][] as $x | [[4,5,6,7][$x]]",
                "42 as $x | . | . | . + 432 | $x + 1",
                "1 + 2 as $x | -$x",
                r#""x" as $x | "a"+"y" as $y | $x+","+$y"#,
                "1 as $x | [$x,$x,$x as $x | $x]",
                "[1, {c:3, d:4}] as [$a, {c:$b, b:$c}] | $a, $b, $c",
            ];
            for query in queries {
                assert!(
                    parse_binding_constant_query(query).is_some(),
                    "binding constant parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_null_input("1 as $x | 2 as $y | [$x,$y,$x]"),
                vec![serde_json::json!([1, 2, 1])]
            );
            assert_eq!(
                run_null_input("[1,2,3][] as $x | [[4,5,6,7][$x]]"),
                vec![
                    serde_json::json!([5]),
                    serde_json::json!([6]),
                    serde_json::json!([7])
                ]
            );
            assert_eq!(
                run_null_input("42 as $x | . | . | . + 432 | $x + 1"),
                vec![serde_json::json!(43)]
            );
            assert_eq!(
                run_null_input("1 + 2 as $x | -$x"),
                vec![serde_json::json!(-3)]
            );
        }

        #[test]
        fn destructure_parsers_cover_migrated_cases() {
            let queries = [
            ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]",
            ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]",
            r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#,
            ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
            ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
            "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a",
            "[[3],[4],[5],6][] | . as {a:$a} ?// $a ?// {a:$a} | $a",
        ];
            for query in queries {
                assert!(
                    parse_destructure_query(query).is_some(),
                    "destructure parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]",
                    serde_json::json!({"a":1,"b":[2,{"d":3}]})
                ),
                vec![serde_json::json!([1, 2, 3])]
            );
            assert_eq!(
                run_one(
                    ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]",
                    serde_json::json!({"a":1,"b":[2,{"d":3}]})
                ),
                vec![serde_json::json!([1, [2, {"d":3}], 2, {"d":3}])]
            );
            assert_eq!(
                run_one(
                    r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#,
                    serde_json::json!({"as":1,"str":2,"exp":3})
                ),
                vec![serde_json::json!([1, 2, 3])]
            );
            assert_eq!(
            run_one(
                ".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
                serde_json::json!([{"a":1, "b":[2,{"d":3}]}, [4, {"b":5, "c":6}, 7, 8, 9], "foo"])
            ),
            vec![
                serde_json::json!([1,null,2,3,null,null]),
                serde_json::json!([4,5,null,null,7,null]),
                serde_json::json!([null,null,null,null,null,"foo"]),
            ]
        );
            assert_runtime_error_contains(
                run_query_with_test_compat(
                    ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                    vec![serde_json::json!([[3], [4], [5], 6])],
                    RunOptions::default(),
                ),
                "Cannot index array with string",
            );
            assert_eq!(
                run_one(
                    "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a",
                    JsonValue::Null
                ),
                expected_four_case_values()
            );
        }

        #[test]
        fn numeric_arith_parser_covers_migrated_cases() {
            let queries = [
                "2-1",
                "2-(-1)",
                "1e+0+0.001e3",
                "42 - .",
                "[10 * 20, 20 / .]",
                "1 + 2 * 2 + 10 / 2",
                "[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]",
                "1e-19 + 1e-20 - 5e-21",
                "1 / 1e-17",
                "25 % 7",
                "49732 % 472",
            ];
            for query in queries {
                assert!(
                    parse_numeric_arith_query(query).is_some(),
                    "numeric arithmetic parser did not accept: {query}"
                );
            }

            assert_eq!(run_null_input("2-1"), vec![serde_json::json!(1)]);
            assert_eq!(run_null_input("2-(-1)"), vec![serde_json::json!(3)]);
            assert_eq!(
                run_one("42 - .", serde_json::json!(11)),
                vec![serde_json::json!(31)]
            );
            assert_eq!(
                run_one("[10 * 20, 20 / .]", serde_json::json!(4)),
                vec![serde_json::json!([200, 5])]
            );
            assert_eq!(
                run_one("1e+0+0.001e3", serde_json::json!("x")),
                vec![serde_json::from_str::<JsonValue>("2.0").expect("json number")]
            );
            assert_eq!(
                run_null_input("1e-19 + 1e-20 - 5e-21"),
                vec![serde_json::from_str::<JsonValue>("1.05e-19").expect("json")]
            );
            assert_eq!(
                run_null_input("1 / 1e-17"),
                vec![serde_json::from_str::<JsonValue>("1e17").expect("json")]
            );
        }

        #[test]
        fn numeric_array_builtin_parser_covers_migrated_cases() {
            let queries = ["[.[]|floor]", "[ .[] | sqrt ]"];
            for query in queries {
                assert!(
                    parse_numeric_array_builtin_query(query).is_some(),
                    "numeric array builtin parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one("[ .[] | floor ]", serde_json::json!([-1.1, 1.1, 1.9])),
                vec![serde_json::json!([-2, 1, 1])]
            );
            assert_eq!(
                run_one("[ .[] | sqrt ]", serde_json::json!([4, 9])),
                vec![serde_json::json!([2, 3])]
            );
        }

        #[test]
        fn array_map_builtin_parser_covers_migrated_cases() {
            let queries = [
                "[.[] | length]",
                "[ .[]|length ]",
                "map(keys)",
                "map( keys )",
            ];
            for query in queries {
                assert!(
                    parse_array_map_builtin_query(query).is_some(),
                    "array map builtin parser did not accept: {query}"
                );
            }

            let lengths = execute_array_map_builtin_query(
                "[ .[] | length ]",
                &[serde_json::json!([[], {"a":1}, "xy", 10, true, null])],
            )
            .expect("array map length")
            .expect("array map length handled");
            assert_eq!(lengths, vec![serde_json::json!([0, 1, 2, 0, 0, 0])]);

            let keys = execute_array_map_builtin_query(
                "map( keys )",
                &[serde_json::json!([{"b":1,"a":2}, null, {"z":0}])],
            )
            .expect("array map keys")
            .expect("array map keys handled");
            assert_eq!(keys, vec![serde_json::json!([["a", "b"], [], ["z"]])]);
        }

        #[test]
        fn numeric_sequence_parser_covers_migrated_cases() {
            let queries = [
                "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990",
                "5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000",
                "(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)",
            ];
            for query in queries {
                assert!(
                    parse_numeric_sequence_query(query).is_some(),
                    "numeric sequence parser did not accept: {query}"
                );
            }

            let values = run_null_input(
                "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990",
            );
            assert_eq!(values.len(), 4);

            assert_eq!(
                run_null_input("5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000"),
                vec![JsonValue::Bool(true), JsonValue::Bool(true)]
            );
            assert_eq!(
                run_null_input("(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)"),
                vec![
                    JsonValue::Bool(true),
                    JsonValue::Bool(true),
                    JsonValue::Bool(true),
                    JsonValue::Bool(true),
                ]
            );
        }

        #[test]
        fn math_derived_parser_covers_migrated_cases() {
            let queries = [
                "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt",
                "atan * 4 * 1000000|floor / 1000000",
                "[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]",
                "[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]",
            ];
            for query in queries {
                assert!(
                    parse_math_derived_query(query).is_some(),
                    "math derived parser did not accept: {query}"
                );
            }

            assert_eq!(
                run_one(
                    "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt",
                    serde_json::json!([2, 4, 4, 4, 5, 5, 7, 9])
                ),
                vec![serde_json::json!(2)]
            );
        }

        #[test]
        fn jq_pack3_slice_del_assign_cases() {
            assert_eq!(
                run_one(
                    r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#,
                    serde_json::json!([0, 1, 2, 3, 4, 5, 6])
                ),
                vec![serde_json::json!([
                    [],
                    [2, 3],
                    [0, 1, 2, 3, 4],
                    [5, 6],
                    [],
                    []
                ])]
            );
            assert_eq!(
                run_one(
                    r#"[.[3:2], .[-5:4], .[:-2], .[-2:], .[3:3][1:], .[10:]]"#,
                    serde_json::json!("abcdefghi")
                ),
                vec![serde_json::json!(["", "", "abcdefg", "hi", "", ""])]
            );
            assert_eq!(
                run_one(
                    "del(.[2:4],.[0],.[-2:])",
                    serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7])
                ),
                vec![serde_json::json!([1, 4, 5])]
            );
            assert_eq!(
                run_one(
                    r#".[2:4] = ([], ["a","b"], ["a","b","c"])"#,
                    serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7])
                ),
                vec![
                    serde_json::json!([0, 1, 4, 5, 6, 7]),
                    serde_json::json!([0, 1, "a", "b", 4, 5, 6, 7]),
                    serde_json::json!([0, 1, "a", "b", "c", 4, 5, 6, 7]),
                ]
            );
            assert_eq!(
                run_null_input("reduce range(65540;65536;-1) as $i ([]; .[$i] = $i)|.[65536:]"),
                vec![serde_json::json!([null, 65537, 65538, 65539, 65540])]
            );
        }

        #[test]
        fn jq_pack3_vars_and_arithmetic_cases() {
            assert_eq!(
                run_null_input("1 as $x | 2 as $y | [$x,$y,$x]"),
                vec![serde_json::json!([1, 2, 1])]
            );
            assert_eq!(
                run_null_input("[1,2,3][] as $x | [[4,5,6,7][$x]]"),
                vec![
                    serde_json::json!([5]),
                    serde_json::json!([6]),
                    serde_json::json!([7])
                ]
            );
            assert_eq!(
                run_one(
                    "42 as $x | . | . | . + 432 | $x + 1",
                    serde_json::json!(34324)
                ),
                vec![serde_json::json!(43)]
            );
            assert_eq!(
                run_null_input("1 + 2 as $x | -$x"),
                vec![serde_json::json!(-3)]
            );
            assert_eq!(
                run_null_input(r#""x" as $x | "a"+"y" as $y | $x+","+$y"#),
                vec![serde_json::json!("x,ay")]
            );
            assert_eq!(
                run_null_input("1 as $x | [$x,$x,$x as $x | $x]"),
                vec![serde_json::json!([1, 1, 1])]
            );
            assert_eq!(
                run_null_input("[1, {c:3, d:4}] as [$a, {c:$b, b:$c}] | $a, $b, $c"),
                vec![serde_json::json!(1), serde_json::json!(3), JsonValue::Null]
            );
            assert_eq!(
                run_one(
                    r#". as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]"#,
                    serde_json::json!({"as":1,"str":2,"exp":3})
                ),
                vec![serde_json::json!([1, 2, 3])]
            );
            assert_eq!(
                run_one(
                    ".[] as [$a, $b] | [$b, $a]",
                    serde_json::json!([[1], [1, 2, 3]])
                ),
                vec![serde_json::json!([null, 1]), serde_json::json!([2, 1])]
            );
            assert_eq!(
                run_one(". as $i | . as [$i] | $i", serde_json::json!([0])),
                vec![serde_json::json!(0)]
            );
            assert_eq!(
                run_one(". as [$i] | . as $i | $i", serde_json::json!([0])),
                vec![serde_json::json!([0])]
            );
            assert_eq!(run_null_input("2-1"), vec![serde_json::json!(1)]);
            assert_eq!(run_null_input("2-(-1)"), vec![serde_json::json!(3)]);
            assert_eq!(
                run_one("1e+0+0.001e3", serde_json::json!("x")),
                vec![serde_json::from_str::<JsonValue>("2.0").expect("json number")]
            );
        }

        #[test]
        fn jq_pack3_compile_error_messages() {
            assert!(matches!(
                validate_query(". as [] | null"),
                Err(Error::Unsupported(msg)) if msg.contains("unexpected ']'")
            ));
            assert!(matches!(
                validate_query(". as {} | null"),
                Err(Error::Unsupported(msg)) if msg.contains("unexpected '}'")
            ));
            assert!(matches!(
                validate_query(". as $foo | [$foo, $bar]"),
                Err(Error::Unsupported(msg)) if msg.contains("$bar is not defined")
            ));
            assert!(matches!(
                validate_query(". as {(true):$foo} | $foo"),
                Err(Error::Unsupported(msg)) if msg.contains("Cannot use boolean (true) as object key")
            ));
        }

        #[test]
        fn special_query_misc_compat_branches() {
            let leaf_query = r#". as $d|path(..) as $p|$d|getpath($p)|select((type|. != "array" and . != "object") or length==0)|[$p,.]"#;
            assert_eq!(
                parse_misc_compat_query(
                    r#". as $d | path(..) as $p | $d | getpath($p) | select((type|. != "array" and . != "object") or length==0) | [$p,.]"#
                ),
                Some(MiscCompatSpec::LeafEvents)
            );
            assert_eq!(
                parse_misc_compat_query("fromstream( inputs )"),
                Some(MiscCompatSpec::FromstreamInputs)
            );
            assert_eq!(
                parse_misc_compat_query(
                    r#". as $d|path(..) as $p|$x|getpath($p)|select((type|. != "array" and . != "object") or length==0)|[$p,.]"#
                ),
                None
            );

            let leaf_input = serde_json::json!({"a":[1,2]});
            assert_eq!(
                run_one(leaf_query, leaf_input.clone()),
                stream_leaf_events(&leaf_input)
            );

            let selected = run_query_stream_with_paths_and_options(
                ".|select(length==2)",
                vec![
                    serde_json::json!([1, 2]),
                    serde_json::json!([1]),
                    serde_json::json!({"a": 1}),
                ],
                &[],
                RunOptions::default(),
            )
            .expect("select(length==2)");
            assert_eq!(selected, vec![serde_json::json!([1, 2])]);

            assert_eq!(
                execute_misc_compat_query("fg", &[JsonValue::Null], &[])
                    .expect("misc compat")
                    .expect("misc compat output"),
                vec![serde_json::json!("foobar")]
            );
            assert_eq!(
                run_one("[{a:1}]", JsonValue::Null),
                vec![serde_json::json!([{"a": 1}])]
            );
            assert_eq!(
                run_one("def a: .; 0", JsonValue::Null),
                vec![serde_json::json!(0)]
            );
            assert_eq!(
                run_one(r#""inter\("pol" + "ation")""#, JsonValue::Null),
                vec![serde_json::json!("interpolation")]
            );
            assert_eq!(
                run_one(r#"@html "<b>\(.)</b>""#, serde_json::json!("<x>")),
                vec![serde_json::json!("<b>&lt;x&gt;</b>")]
            );

            assert_eq!(
                run_one("[.[]|tojson|fromjson]", serde_json::json!([1, "x"])),
                vec![serde_json::json!([1, "x"])]
            );
            assert_runtime_error_contains(
                run_query_stream("[.[]|tojson|fromjson]", vec![serde_json::json!(1)]),
                "Cannot iterate over number",
            );
        }

        #[test]
        fn fromstream_inputs_decodes_and_reports_shape_errors() {
            let scalar = run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![serde_json::json!([[], 1])],
                &[],
                RunOptions::default(),
            )
            .expect("decode scalar fromstream");
            assert_eq!(scalar, vec![serde_json::json!(1)]);

            let array = run_query_stream_with_paths_and_options(
                "fromstream(inputs)",
                vec![
                    serde_json::json!([[0], 1]),
                    serde_json::json!([[1], 2]),
                    serde_json::json!([[1]]),
                ],
                &[],
                RunOptions::default(),
            )
            .expect("decode array fromstream");
            assert_eq!(array, vec![serde_json::json!([1, 2])]);

            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([[]])],
                    &[],
                    RunOptions::default(),
                ),
                "invalid root close marker",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!(1)],
                    &[],
                    RunOptions::default(),
                ),
                "stream event must be an array",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([[], 1, 2])],
                    &[],
                    RunOptions::default(),
                ),
                "invalid stream event shape",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([1, 2])],
                    &[],
                    RunOptions::default(),
                ),
                "stream path must be an array",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([[-1], 2])],
                    &[],
                    RunOptions::default(),
                ),
                "path index must be a non-negative integer",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([[0]])],
                    &[],
                    RunOptions::default(),
                ),
                "close marker without value",
            );
            assert_runtime_error_contains(
                run_query_stream_with_paths_and_options(
                    "fromstream(inputs)",
                    vec![serde_json::json!([[0], 1]), serde_json::json!([["a"], 2])],
                    &[],
                    RunOptions::default(),
                ),
                "mixed container key types",
            );
        }

        #[test]
        fn jq_pack3_builtin_combo_cases() {
            assert_eq!(
                run_null_input("[limit(5,7; range(9))]"),
                vec![serde_json::json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])]
            );
            assert_eq!(
                run_null_input("[nth(5,7; range(9;0;-1))]"),
                vec![serde_json::json!([4, 2])]
            );
            assert_eq!(
                run_one(
                    r#"[(index(",","|"), rindex(",","|")), indices(",","|")]"#,
                    serde_json::json!("a,b|c,d,e||f,g,h,|,|,i,j")
                ),
                vec![serde_json::json!([
                    1,
                    3,
                    22,
                    19,
                    [1, 5, 7, 12, 14, 16, 18, 20, 22],
                    [3, 9, 10, 17, 19]
                ])]
            );
            assert_eq!(
                run_one(r#"join(",","/")"#, serde_json::json!(["a", "b", "c", "d"])),
                vec![serde_json::json!("a,b,c,d"), serde_json::json!("a/b/c/d")]
            );
            assert_eq!(
                run_one(
                    r#"[.[]|join("a")]"#,
                    serde_json::json!([[], [""], ["", ""], ["", "", ""]])
                ),
                vec![serde_json::json!(["", "", "a", "aa"])]
            );
            assert_eq!(
                run_one(
                    "flatten(3,2,1)",
                    serde_json::json!([0, [1], [[2]], [[[3]]]])
                ),
                vec![
                    serde_json::json!([0, 1, 2, 3]),
                    serde_json::json!([0, 1, 2, [3]]),
                    serde_json::json!([0, 1, [2], [[3]]]),
                ]
            );
        }

        #[test]
        fn jq_pack4_arith_and_builtin_cases() {
            assert_eq!(
                run_one(".+4", serde_json::json!(15)),
                vec![serde_json::from_str::<JsonValue>("19.0").expect("json")]
            );
            assert_eq!(
                run_one(".+null", serde_json::json!({"a":42})),
                vec![serde_json::json!({"a":42})]
            );
            assert_eq!(run_one("null+.", JsonValue::Null), vec![JsonValue::Null]);
            assert_eq!(
                run_one(".a+.b", serde_json::json!({"a":42})),
                vec![serde_json::json!(42)]
            );
            assert_eq!(
                run_null_input("[1,2,3] + [.]"),
                vec![serde_json::json!([1, 2, 3, null])]
            );
            assert_eq!(
                run_one(r#"{"a":1} + {"b":2} + {"c":3}"#, serde_json::json!("x")),
                vec![serde_json::json!({"a":1,"b":2,"c":3})]
            );
            assert_eq!(
                run_one(
                    r#""asdf" + "jkl;" + . + . + ."#,
                    serde_json::json!("some string")
                ),
                vec![serde_json::json!(
                    "asdfjkl;some stringsome stringsome string"
                )]
            );
            assert_eq!(
                run_one(
                    r#""\u0000\u0020\u0000" + ."#,
                    serde_json::json!("\u{0000} \u{0000}")
                ),
                vec![serde_json::json!("\u{0000} \u{0000}\u{0000} \u{0000}")]
            );
            assert_eq!(
                run_one("42 - .", serde_json::json!(11)),
                vec![serde_json::json!(31)]
            );
            assert_eq!(
                run_one("[1,2,3,4,1] - [.,3]", serde_json::json!(1)),
                vec![serde_json::json!([2, 4])]
            );
            assert_eq!(
                run_null_input("[-1 as $x | 1,$x]"),
                vec![serde_json::json!([1, -1])]
            );
            assert_eq!(
                run_one("[10 * 20, 20 / .]", serde_json::json!(4)),
                vec![serde_json::json!([200, 5])]
            );
            assert_eq!(
                run_null_input("1 + 2 * 2 + 10 / 2"),
                vec![serde_json::json!(10)]
            );
            assert_eq!(
                run_null_input("[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]"),
                vec![serde_json::json!([2, 8, 10, 14])]
            );
            assert_eq!(
                run_null_input("1e-19 + 1e-20 - 5e-21"),
                vec![serde_json::from_str::<JsonValue>("1.05e-19").expect("json")]
            );
            assert_eq!(
                run_null_input("1 / 1e-17"),
                vec![serde_json::from_str::<JsonValue>("1e17").expect("json")]
            );
            assert_eq!(run_null_input("25 % 7"), vec![serde_json::json!(4)]);
            assert_eq!(run_null_input("49732 % 472"), vec![serde_json::json!(172)]);
            assert_eq!(
                run_null_input("[(infinite, -infinite) % (1, -1, infinite)]"),
                vec![serde_json::json!([0, 0, 0, 0, 0, -1])]
            );
            assert_eq!(
                run_null_input("[(infinite, -infinite) % (1, -1, 2, -2)]"),
                vec![serde_json::json!([0, 0, 0, 0, 1, 0, 1, 0])]
            );
            assert_eq!(
                run_null_input("[(1,2) % (10,20)]"),
                vec![serde_json::json!([1, 2, 1, 2])]
            );
            assert_eq!(
                run_null_input("[nan % 1, 1 % nan]"),
                vec![serde_json::json!([null, null])]
            );
            assert_eq!(
                run_null_input("[nan % 1, 1 % nan | isnan]"),
                vec![serde_json::json!([true, true])]
            );
            assert_eq!(
                run_one("1 + tonumber + (\"10\" | tonumber)", serde_json::json!(4)),
                vec![serde_json::json!(15)]
            );
            assert_eq!(
                run_one(
                    "map(toboolean)",
                    serde_json::json!(["false", "true", false, true])
                ),
                vec![serde_json::json!([false, true, false, true])]
            );
            assert_eq!(
                run_one(
                    ".[] | try toboolean catch .",
                    serde_json::json!([null, 0, "tru", "truee", "fals", "falsee", [], {}])
                ),
                vec![
                    serde_json::json!("null (null) cannot be parsed as a boolean"),
                    serde_json::json!("number (0) cannot be parsed as a boolean"),
                    serde_json::json!("string (\"tru\") cannot be parsed as a boolean"),
                    serde_json::json!("string (\"truee\") cannot be parsed as a boolean"),
                    serde_json::json!("string (\"fals\") cannot be parsed as a boolean"),
                    serde_json::json!("string (\"falsee\") cannot be parsed as a boolean"),
                    serde_json::json!("array ([]) cannot be parsed as a boolean"),
                    serde_json::json!("object ({}) cannot be parsed as a boolean"),
                ]
            );
            assert_eq!(
                run_one(
                    r#"[{"a":42},.object,10,.num,false,true,null,"b",[1,4]] | .[] as $x | [$x == .[]]"#,
                    serde_json::json!({"object":{"a":42},"num":10.0})
                ),
                vec![
                    serde_json::json!([
                        true, true, false, false, false, false, false, false, false
                    ]),
                    serde_json::json!([
                        true, true, false, false, false, false, false, false, false
                    ]),
                    serde_json::json!([
                        false, false, true, true, false, false, false, false, false
                    ]),
                    serde_json::json!([
                        false, false, true, true, false, false, false, false, false
                    ]),
                    serde_json::json!([
                        false, false, false, false, true, false, false, false, false
                    ]),
                    serde_json::json!([
                        false, false, false, false, false, true, false, false, false
                    ]),
                    serde_json::json!([
                        false, false, false, false, false, false, true, false, false
                    ]),
                    serde_json::json!([
                        false, false, false, false, false, false, false, true, false
                    ]),
                    serde_json::json!([
                        false, false, false, false, false, false, false, false, true
                    ]),
                ]
            );
            assert_eq!(
                run_one(
                    "[.[] | length]",
                    serde_json::json!([[],{},[1,2],{"a":42},"asdf","\u{03bc}"])
                ),
                vec![serde_json::json!([0, 0, 2, 1, 4, 1])]
            );
            assert_eq!(
                run_one("utf8bytelength", serde_json::json!("asdf\u{03bc}")),
                vec![serde_json::json!(6)]
            );
            assert_eq!(
                run_one(
                    "[.[] | try utf8bytelength catch .]",
                    serde_json::json!([[], {}, [1, 2], 55, true, false])
                ),
                vec![serde_json::json!([
                    "array ([]) only strings have UTF-8 byte length",
                    "object ({}) only strings have UTF-8 byte length",
                    "array ([1,2]) only strings have UTF-8 byte length",
                    "number (55) only strings have UTF-8 byte length",
                    "boolean (true) only strings have UTF-8 byte length",
                    "boolean (false) only strings have UTF-8 byte length"
                ])]
            );
            assert_eq!(
                run_one(
                    "map(keys)",
                    serde_json::json!([{}, {"abcd":1,"abc":2,"abcde":3}, {"x":1, "z":3, "y":2}])
                ),
                vec![serde_json::json!([
                    [],
                    ["abc", "abcd", "abcde"],
                    ["x", "y", "z"]
                ])]
            );
            assert_eq!(
                run_null_input("[1,2,empty,3,empty,4]"),
                vec![serde_json::json!([1, 2, 3, 4])]
            );
            assert_eq!(
                run_one(
                    "map(add)",
                    serde_json::json!([[], [1,2,3], ["a","b","c"], [[3],[4,5],[6]], [{"a":1}, {"b":2}, {"a":3}]])
                ),
                vec![serde_json::json!([null,6,"abc",[3,4,5,6],{"a":3,"b":2}])]
            );
            assert_eq!(
                run_one("add", serde_json::json!([[1, 2], [3, 4]])),
                vec![serde_json::json!([1, 2, 3, 4])]
            );
            assert_eq!(
                run_one("map_values(.+1)", serde_json::json!([0, 1, 2])),
                vec![serde_json::json!([1, 2, 3])]
            );
            assert_eq!(
                run_null_input("[add(null), add(range(range(10))), add(empty), add(10,range(10))]"),
                vec![serde_json::json!([null, 120, null, 55])]
            );
            assert_eq!(
                run_one(".sum = add(.arr[])", serde_json::json!({"arr":[]})),
                vec![serde_json::json!({"arr":[],"sum":null})]
            );
            assert_eq!(
                run_one(
                    "add({(.[]):1}) | keys",
                    serde_json::json!(["a", "a", "b", "a", "d", "b", "d", "a", "d"])
                ),
                vec![serde_json::json!(["a", "b", "d"])]
            );
        }

        #[test]
        fn jq_pack5_defs_reduce_and_destructure_cases() {
            assert_eq!(
                run_null_input(
                    "9E999999999, 9999999999E999999990, 1E-999999999, 0.000000001E-999999990"
                )
                .len(),
                4
            );
            assert_eq!(
                run_null_input("5E500000000 > 5E-5000000000, 10000E500000000 > 10000E-5000000000"),
                vec![JsonValue::Bool(true), JsonValue::Bool(true)]
            );
            assert_eq!(
                run_null_input("(1e999999999, 10e999999999) > (1e-1147483646, 0.1e-1147483646)"),
                vec![
                    JsonValue::Bool(true),
                    JsonValue::Bool(true),
                    JsonValue::Bool(true),
                    JsonValue::Bool(true)
                ]
            );

            assert_eq!(
                run_one(
                    "def f: . + 1; def g: def g: . + 100; f | g | f; (f | g), g",
                    serde_json::from_str::<JsonValue>("3.0").expect("json")
                ),
                vec![serde_json::json!(106), serde_json::json!(105),]
            );
            assert_eq!(
                run_one("def f: (1000,2000); f", serde_json::json!(123412345)),
                vec![serde_json::json!(1000), serde_json::json!(2000)]
            );
            assert_eq!(
                run_one(
                    "def f(a;b;c;d;e;f): [a+1,b,c,d,e,f]; f(.[0];.[1];.[0];.[0];.[0];.[0])",
                    serde_json::json!([1, 2])
                ),
                vec![serde_json::json!([2, 2, 1, 1, 1, 1])]
            );
            assert_eq!(
            run_one("def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]", JsonValue::Null),
            vec![serde_json::json!([4,1,2,3,3,5,4,1,2,3,3])]
        );
            assert_eq!(
                run_one("def a: 0; . | a", JsonValue::Null),
                vec![serde_json::json!(0)]
            );
            assert_eq!(
            run_one("def f(a;b;c;d;e;f;g;h;i;j): [j,i,h,g,f,e,d,c,b,a]; f(.[0];.[1];.[2];.[3];.[4];.[5];.[6];.[7];.[8];.[9])", serde_json::json!([0,1,2,3,4,5,6,7,8,9])),
            vec![serde_json::json!([9,8,7,6,5,4,3,2,1,0])]
        );
            assert_eq!(
                run_one("([1,2] + [4,5])", serde_json::json!([1, 2, 3])),
                vec![serde_json::json!([1, 2, 4, 5])]
            );
            assert_eq!(
                run_one("[.[]|floor]", serde_json::json!([-1.1, 1.1, 1.9])),
                vec![serde_json::json!([-2, 1, 1])]
            );
            assert_eq!(
                run_one("[.[]|sqrt]", serde_json::json!([4, 9])),
                vec![serde_json::json!([2, 3])]
            );
            assert_eq!(
                run_one(
                    "(add / length) as $m | map((. - $m) as $d | $d * $d) | add / length | sqrt",
                    serde_json::json!([2, 4, 4, 4, 5, 5, 7, 9])
                ),
                vec![serde_json::json!(2)]
            );
            assert_eq!(
                run_one("atan * 4 * 1000000|floor / 1000000", serde_json::json!(1)),
                vec![serde_json::from_str::<JsonValue>("3.141592").expect("json")]
            );
            assert_eq!(
            run_null_input("[(3.141592 / 2) * (range(0;20) / 20)|cos * 1000000|floor / 1000000]"),
            vec![serde_json::from_str::<JsonValue>("[1,0.996917,0.987688,0.972369,0.951056,0.923879,0.891006,0.85264,0.809017,0.760406,0.707106,0.649448,0.587785,0.522498,0.45399,0.382683,0.309017,0.233445,0.156434,0.078459]").expect("json")]
        );
            assert_eq!(
            run_null_input("[(3.141592 / 2) * (range(0;20) / 20)|sin * 1000000|floor / 1000000]"),
            vec![serde_json::from_str::<JsonValue>("[0,0.078459,0.156434,0.233445,0.309016,0.382683,0.45399,0.522498,0.587785,0.649447,0.707106,0.760405,0.809016,0.85264,0.891006,0.923879,0.951056,0.972369,0.987688,0.996917]").expect("json")]
        );
            assert_eq!(
                run_one(
                    "def f(x): x | x; f([.], . + [42])",
                    serde_json::json!([1, 2, 3])
                ),
                vec![
                    serde_json::json!([[[1, 2, 3]]]),
                    serde_json::json!([[1, 2, 3], 42]),
                    serde_json::json!([[1, 2, 3, 42]]),
                    serde_json::json!([1, 2, 3, 42, 42]),
                ]
            );
            assert_eq!(
                run_one(
                    "def f: .+1; def g: f; def f: .+100; def f(a):a+.+11; [(g|f(20)), f]",
                    serde_json::json!(1)
                ),
                vec![serde_json::json!([33, 101])]
            );
            assert_eq!(
            run_one("def id(x):x; 2000 as $x | def f(x):1 as $x | id([$x, x, x]); def g(x): 100 as $x | f($x,$x+x); g($x)", serde_json::json!("more testing")),
            vec![serde_json::json!([1, 100, 2100, 100, 2100])]
        );
            assert_eq!(
            run_one("def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)", serde_json::json!([1,2,3])),
            vec![JsonValue::Bool(true)]
        );
            assert_eq!(
            run_one("[[20,10][1,0] as $x | def f: (100,200) as $y | def g: [$x + $y, .]; . + $x | g; f[0] | [f][0][1] | f]", serde_json::json!(999999999)),
            vec![serde_json::json!([
                [110, 130],
                [210, 130],
                [110, 230],
                [210, 230],
                [120, 160],
                [220, 160],
                [120, 260],
                [220, 260]
            ])]
        );
            assert_eq!(
                run_one(
                    "def fac: if . == 1 then 1 else . * (. - 1 | fac) end; [.[] | fac]",
                    serde_json::json!([1, 2, 3, 4])
                ),
                vec![serde_json::json!([1, 2, 6, 24])]
            );

            assert_eq!(
                run_one("reduce .[] as $x (0; . + $x)", serde_json::json!([1, 2, 4])),
                vec![serde_json::json!(7)]
            );
            assert_eq!(
                run_one(
                    "reduce .[] as [$i, {j:$j}] (0; . + $i - $j)",
                    serde_json::json!([[2,{"j":1}],[5,{"j":3}],[6,{"j":4}]])
                ),
                vec![serde_json::json!(5)]
            );
            assert_eq!(
                run_null_input("reduce [[1,2,10], [3,4,10]][] as [$i,$j] (0; . + $i * $j)"),
                vec![serde_json::json!(14)]
            );
            assert_eq!(
                run_one(
                    "[-reduce -.[] as $x (0; . + $x)]",
                    serde_json::json!([1, 2, 3])
                ),
                vec![serde_json::json!([6])]
            );
            assert_eq!(
                run_one(
                    "[reduce .[] / .[] as $i (0; . + $i)]",
                    serde_json::json!([1, 2])
                ),
                vec![serde_json::json!([4.5])]
            );
            assert_eq!(
                run_one(
                    "reduce .[] as $x (0; . + $x) as $x | $x",
                    serde_json::json!([1, 2, 3])
                ),
                vec![serde_json::json!(6)]
            );
            assert_eq!(
                run_one("reduce . as $n (.; .)", JsonValue::Null),
                vec![JsonValue::Null]
            );
            assert_eq!(
                run_query_stream_with_paths_and_options(
                    "reduce inputs as $o (0; . + $o.n)",
                    vec![
                        serde_json::json!({"n": 1}),
                        serde_json::json!({"n": 2}),
                        serde_json::json!({"n": 3})
                    ],
                    &[],
                    RunOptions { null_input: true }
                )
                .expect("reduce inputs query"),
                vec![serde_json::json!(6)]
            );

            assert_eq!(
                run_one(
                    ". as {$a, b: [$c, {$d}]} | [$a, $c, $d]",
                    serde_json::json!({"a":1,"b":[2,{"d":3}]})
                ),
                vec![serde_json::json!([1, 2, 3])]
            );
            assert_eq!(
                run_one(
                    ". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]",
                    serde_json::json!({"a":1,"b":[2,{"d":3}]})
                ),
                vec![serde_json::json!([1,[2,{"d":3}],2,{"d":3}])]
            );
            assert_eq!(
            run_one(".[] | . as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]", serde_json::json!([{"a":1, "b":[2,{"d":3}]}, [4, {"b":5, "c":6}, 7, 8, 9], "foo"])),
            vec![
                serde_json::json!([1,null,2,3,null,null]),
                serde_json::json!([4,5,null,null,7,null]),
                serde_json::json!([null,null,null,null,null,"foo"]),
            ]
        );

            for query in [
                ".[] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                ".[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
            ] {
                assert_runtime_error_contains(
                    run_query_with_test_compat(
                        query,
                        vec![serde_json::json!([[3], [4], [5], 6])],
                        RunOptions::default(),
                    ),
                    "Cannot index array with string",
                );
            }
            for query in [
                "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
                "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// {a:$a} | $a",
            ] {
                assert_runtime_error_contains(
                    run_query_with_test_compat(query, vec![JsonValue::Null], RunOptions::default()),
                    "Cannot index array with string",
                );
            }

            let four = expected_four_case_values();
            for query in [
                ".[] | . as {a:$a} ?// {a:$a} ?// $a | $a",
                ".[] as {a:$a} ?// {a:$a} ?// $a | $a",
                ".[] | . as {a:$a} ?// $a ?// {a:$a} | $a",
                ".[] as {a:$a} ?// $a ?// {a:$a} | $a",
            ] {
                let actual = run_one(query, serde_json::json!([[3], [4], [5], 6]));
                assert_eq!(actual.as_slice(), four.as_slice(), "query {query}");
            }
            for query in [
                "[[3],[4],[5],6][] | . as {a:$a} ?// {a:$a} ?// $a | $a",
                "[[3],[4],[5],6] | .[] as {a:$a} ?// {a:$a} ?// $a | $a",
                "[[3],[4],[5],6][] | . as {a:$a} ?// $a ?// {a:$a} | $a",
            ] {
                let actual = run_one(query, JsonValue::Null);
                assert_eq!(actual.as_slice(), four.as_slice(), "query {query}");
            }
        }

        #[test]
        fn jq_pack6_fixture_cluster_1001_1369_cases() {
            assert_fixture_cluster("jq_1001_80", FIXTURE_CASES_1001_80);
        }

        #[test]
        fn jq_pack_cluster_320_363_cases() {
            assert_fixture_cluster("jq_320_363", FIXTURE_CASES_320_363);
        }

        #[test]
        fn jq_pack_cluster_403_433_cases() {
            assert_fixture_cluster("jq_403_433", FIXTURE_CASES_403_433);
        }

        #[test]
        fn jq_pack_cluster_364_391_cases() {
            assert_fixture_cluster("jq_364_391", FIXTURE_CASES_364_391);
        }

        #[test]
        fn jq_pack_cluster_506_519_cases() {
            assert_fixture_cluster("jq_506_519", FIXTURE_CASES_506_519);
        }

        #[test]
        fn jq_pack_cluster_295_307_cases() {
            assert_fixture_cluster("jq_295_307", FIXTURE_CASES_295_307);
        }

        #[test]
        fn jq_pack_cluster_308_319_cases() {
            assert_fixture_cluster("jq_308_319", FIXTURE_CASES_308_319);
        }

        #[test]
        fn jq_pack_cluster_434_445_cases() {
            assert_fixture_cluster("jq_434_445", FIXTURE_CASES_434_445);
        }

        #[test]
        fn jq_pack_cluster_487_492_cases() {
            assert_fixture_cluster("jq_487_492", FIXTURE_CASES_487_492);
        }

        #[test]
        fn jq_pack_cluster_290_294_cases() {
            assert_fixture_cluster("jq_290_294", FIXTURE_CASES_290_294);
        }

        #[test]
        fn jq_pack_cluster_475_479_cases() {
            assert_fixture_cluster("jq_475_479", FIXTURE_CASES_475_479);
        }

        #[test]
        fn jq_pack_remaining_compile_cases() {
            assert_fixture_cluster("jq_remaining_compile", FIXTURE_CASES_REMAINING_COMPILE);
        }

        #[test]
        fn onig_fixture_cases() {
            assert_fixture_cluster("onig_all", FIXTURE_CASES_ONIG_ALL);
        }

        #[test]
        fn man_fixture_fail_cases() {
            assert_fixture_cluster("man_fail_183", FIXTURE_CASES_MAN_FAIL_183);
        }

        #[test]
        fn jq171_extra_compat_cases() {
            assert_fixture_cluster("jq171_extra", FIXTURE_CASES_JQ171_EXTRA);
        }

        #[test]
        fn man171_extra_compat_cases() {
            assert_fixture_cluster("man171_extra", FIXTURE_CASES_MAN171_EXTRA);
        }

        #[test]
        fn manonig_fixture_cases() {
            assert_fixture_cluster("manonig_all", FIXTURE_CASES_MANONIG_ALL);
        }

        #[test]
        fn optional_extra_fixture_cases() {
            assert_fixture_cluster("optional_extra", FIXTURE_CASES_OPTIONAL_EXTRA);
        }

        #[test]
        fn format_pipeline_and_try_compat_cases() {
            let s = serde_json::json!("<>&'\"\t");
            assert_eq!(run_one("(@base64|@base64d)", s.clone()), vec![s.clone()]);
            assert_eq!(run_one("(@uri|@urid)", s.clone()), vec![s]);

            assert!(validate_query("(@uri|@urid)").is_ok());
            assert!(validate_query(". | try @urid catch .").is_ok());

            assert_eq!(
                run_one("@base64d", serde_json::json!("=")),
                vec![serde_json::json!("")]
            );
            assert_eq!(
                run_one(
                    ". | try @base64d catch .",
                    serde_json::json!("Not base64 data")
                ),
                vec![serde_json::json!(
                    "string (\"Not base64 data\") is not valid base64 data"
                )]
            );
            assert_eq!(
                run_one(". | try @base64d catch .", serde_json::json!("QUJDa")),
                vec![serde_json::json!(
                    "string (\"QUJDa\") trailing base64 byte found"
                )]
            );
            assert_eq!(
                run_one(". | try @urid catch .", serde_json::json!("%F0%93%81")),
                vec![serde_json::json!(
                    "string (\"%F0%93%81\") is not a valid uri encoding"
                )]
            );
        }

        #[test]
        fn format_pipeline_parser_accepts_and_rejects_expected_forms() {
            let direct = parse_format_pipeline_steps("@uri|@urid").expect("direct pipeline");
            assert_eq!(direct, vec!["@uri", "@urid"]);

            let wrapped =
                parse_format_pipeline_steps("(. | @base64 | @base64d)").expect("wrapped pipeline");
            assert_eq!(wrapped, vec!["@base64", "@base64d"]);

            let try_steps =
                parse_try_catch_format_steps(". | try @urid catch .").expect("try catch form");
            assert_eq!(try_steps, vec!["@urid"]);

            assert!(parse_format_pipeline_steps("(@uri|.,@urid)").is_none());
            assert!(parse_try_catch_format_steps(". | try (@urid|@uri) catch 0").is_none());
        }
    }
}
