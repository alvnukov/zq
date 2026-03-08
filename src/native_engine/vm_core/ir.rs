use super::ast::{
    BinaryOp, BindingKeySpec, BindingPattern, Builtin, FunctionDef, MathBinaryOp, MathTernaryOp,
    ObjectKey, Pipeline, Query, Stage,
};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program {
    pub(crate) branches: Vec<Branch>,
    pub(crate) functions: Vec<ProgramFunction>,
    pub(crate) module_search_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Branch {
    pub(crate) ops: Vec<Op>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProgramFunction {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) arity: usize,
    pub(crate) params: Vec<String>,
    pub(crate) param_ids: Vec<usize>,
    pub(crate) body: Op,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OpObjectKey {
    Static(String),
    Expr(Box<Op>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OpBindingPattern {
    Var(String),
    Array(Vec<OpBindingPattern>),
    Object(Vec<OpObjectBindingEntry>),
    Alternatives(Vec<OpBindingPattern>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpObjectBindingEntry {
    pub(crate) key: OpBindingKeySpec,
    pub(crate) store_var: Option<String>,
    pub(crate) pattern: OpBindingPattern,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OpBindingKeySpec {
    Literal(String),
    Expr(Box<Op>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Op {
    Identity,
    Chain(Vec<Op>),
    Pipe(Vec<Op>),
    Call {
        function_id: Option<usize>,
        param_id: Option<usize>,
        name: String,
        args: Vec<Op>,
    },
    Var(String),
    Label {
        name: String,
        body: Box<Op>,
    },
    Break(String),
    Literal(crate::value::ZqValue),
    Comma(Vec<Op>),
    ArrayLiteral(Vec<Op>),
    ObjectLiteral(Vec<(OpObjectKey, Op)>),
    Builtin(Builtin),
    Has(Box<Op>),
    In(Box<Op>),
    StartsWith(Box<Op>),
    EndsWith(Box<Op>),
    Split(Box<Op>),
    Join(Box<Op>),
    LTrimStr(Box<Op>),
    RTrimStr(Box<Op>),
    TrimStr(Box<Op>),
    Indices(Box<Op>),
    IndexOf(Box<Op>),
    RIndexOf(Box<Op>),
    Contains(Box<Op>),
    Inside(Box<Op>),
    BSearch(Box<Op>),
    SortByImpl(Box<Op>),
    GroupByImpl(Box<Op>),
    UniqueByImpl(Box<Op>),
    MinByImpl(Box<Op>),
    MaxByImpl(Box<Op>),
    RegexMatch {
        spec: Box<Op>,
        flags: Option<Box<Op>>,
        test: bool,
        tuple_mode: bool,
    },
    RegexCapture {
        spec: Box<Op>,
        flags: Option<Box<Op>>,
        tuple_mode: bool,
    },
    RegexScan {
        regex: Box<Op>,
        flags: Option<Box<Op>>,
    },
    RegexSplits {
        regex: Box<Op>,
        flags: Option<Box<Op>>,
    },
    RegexSub {
        regex: Box<Op>,
        replacement: Box<Op>,
        flags: Box<Op>,
        global: bool,
    },
    Path(Box<Op>),
    Paths,
    GetPath(Box<Op>),
    SetPath(Box<Op>, Box<Op>),
    Modify(Box<Op>, Box<Op>),
    DelPaths(Box<Op>),
    TruncateStream(Box<Op>),
    FromStream(Box<Op>),
    ToStream,
    Flatten(Box<Op>),
    FlattenRaw(Box<Op>),
    Nth(Box<Op>),
    NthBy(Box<Op>, Box<Op>),
    LimitBy(Box<Op>, Box<Op>),
    SkipBy(Box<Op>, Box<Op>),
    Range(Box<Op>, Box<Op>, Box<Op>),
    While(Box<Op>, Box<Op>),
    Until(Box<Op>, Box<Op>),
    Reduce {
        source: Box<Op>,
        pattern: OpBindingPattern,
        init: Box<Op>,
        update: Box<Op>,
    },
    Foreach {
        source: Box<Op>,
        pattern: OpBindingPattern,
        init: Box<Op>,
        update: Box<Op>,
        extract: Box<Op>,
    },
    Any(Box<Op>, Box<Op>),
    All(Box<Op>, Box<Op>),
    FirstBy(Box<Op>),
    LastBy(Box<Op>),
    IsEmpty(Box<Op>),
    AddBy(Box<Op>),
    Select(Box<Op>),
    Map(Box<Op>),
    MapValues(Box<Op>),
    WithEntries(Box<Op>),
    RecurseBy(Box<Op>),
    RecurseByCond(Box<Op>, Box<Op>),
    Walk(Box<Op>),
    Combinations,
    Repeat(Box<Op>),
    Input,
    Format {
        fmt: String,
        expr: Box<Op>,
    },
    Strptime(Box<Op>),
    Strftime {
        format: Box<Op>,
        local: bool,
    },
    Empty,
    Error(Box<Op>),
    HaltError(Box<Op>),
    UnaryMinus(Box<Op>),
    UnaryNot(Box<Op>),
    TryCatch {
        inner: Box<Op>,
        catcher: Box<Op>,
    },
    IfElse {
        cond: Box<Op>,
        then_expr: Box<Op>,
        else_expr: Box<Op>,
    },
    Binary {
        op: BinaryOp,
        lhs: Box<Op>,
        rhs: Box<Op>,
    },
    MathBinary {
        op: MathBinaryOp,
        lhs: Box<Op>,
        rhs: Box<Op>,
    },
    MathTernary {
        op: MathTernaryOp,
        a: Box<Op>,
        b: Box<Op>,
        c: Box<Op>,
    },
    GetField {
        name: String,
        optional: bool,
    },
    GetIndex {
        index: i64,
        optional: bool,
    },
    DynamicIndex {
        key: Box<Op>,
        optional: bool,
    },
    Slice {
        start: Option<i64>,
        end: Option<i64>,
        optional: bool,
    },
    Bind {
        source: Box<Op>,
        pattern: OpBindingPattern,
        body: Box<Op>,
    },
    Iterate {
        optional: bool,
    },
}

pub(crate) fn compile(query: &Query) -> Program {
    Program {
        branches: query.branches.iter().map(compile_branch).collect(),
        functions: query.functions.iter().map(compile_function).collect(),
        module_search_dirs: Vec::new(),
    }
}

fn compile_function(function: &FunctionDef) -> ProgramFunction {
    ProgramFunction {
        id: function.id,
        name: function.name.clone(),
        arity: function.arity,
        params: function.params.clone(),
        param_ids: function.param_ids.clone(),
        body: compile_stage(&function.body),
    }
}

fn compile_branch(branch: &Pipeline) -> Branch {
    let mut ops = Vec::with_capacity(branch.stages.len());
    for stage in &branch.stages {
        ops.push(compile_stage(stage));
    }
    Branch { ops }
}

fn compile_stage(stage: &Stage) -> Op {
    match stage {
        Stage::Identity => Op::Identity,
        Stage::Chain(items) => Op::Chain(items.iter().map(compile_stage).collect()),
        Stage::Pipe(items) => Op::Pipe(items.iter().map(compile_stage).collect()),
        Stage::Call {
            function_id,
            param_id,
            name,
            args,
        } => Op::Call {
            function_id: *function_id,
            param_id: *param_id,
            name: name.clone(),
            args: args.iter().map(compile_stage).collect(),
        },
        Stage::Var(name) => Op::Var(name.clone()),
        Stage::Label { name, body } => Op::Label {
            name: name.clone(),
            body: Box::new(compile_stage(body)),
        },
        Stage::Break(name) => Op::Break(name.clone()),
        Stage::Literal(value) => Op::Literal(value.clone()),
        Stage::Comma(items) => Op::Comma(items.iter().map(compile_stage).collect()),
        Stage::ArrayLiteral(items) => Op::ArrayLiteral(items.iter().map(compile_stage).collect()),
        Stage::ObjectLiteral(entries) => Op::ObjectLiteral(
            entries
                .iter()
                .map(|(key, value)| {
                    let key = match key {
                        ObjectKey::Static(name) => OpObjectKey::Static(name.clone()),
                        ObjectKey::Expr(expr) => OpObjectKey::Expr(Box::new(compile_stage(expr))),
                    };
                    (key, compile_stage(value))
                })
                .collect(),
        ),
        Stage::Builtin(filter) => Op::Builtin(*filter),
        Stage::Has(arg) => Op::Has(Box::new(compile_stage(arg))),
        Stage::In(arg) => Op::In(Box::new(compile_stage(arg))),
        Stage::StartsWith(arg) => Op::StartsWith(Box::new(compile_stage(arg))),
        Stage::EndsWith(arg) => Op::EndsWith(Box::new(compile_stage(arg))),
        Stage::Split(arg) => Op::Split(Box::new(compile_stage(arg))),
        Stage::Join(arg) => Op::Join(Box::new(compile_stage(arg))),
        Stage::LTrimStr(arg) => Op::LTrimStr(Box::new(compile_stage(arg))),
        Stage::RTrimStr(arg) => Op::RTrimStr(Box::new(compile_stage(arg))),
        Stage::TrimStr(arg) => Op::TrimStr(Box::new(compile_stage(arg))),
        Stage::Indices(arg) => Op::Indices(Box::new(compile_stage(arg))),
        Stage::IndexOf(arg) => Op::IndexOf(Box::new(compile_stage(arg))),
        Stage::RIndexOf(arg) => Op::RIndexOf(Box::new(compile_stage(arg))),
        Stage::Contains(arg) => Op::Contains(Box::new(compile_stage(arg))),
        Stage::Inside(arg) => Op::Inside(Box::new(compile_stage(arg))),
        Stage::BSearch(arg) => Op::BSearch(Box::new(compile_stage(arg))),
        Stage::SortByImpl(arg) => Op::SortByImpl(Box::new(compile_stage(arg))),
        Stage::GroupByImpl(arg) => Op::GroupByImpl(Box::new(compile_stage(arg))),
        Stage::UniqueByImpl(arg) => Op::UniqueByImpl(Box::new(compile_stage(arg))),
        Stage::MinByImpl(arg) => Op::MinByImpl(Box::new(compile_stage(arg))),
        Stage::MaxByImpl(arg) => Op::MaxByImpl(Box::new(compile_stage(arg))),
        Stage::RegexMatch {
            spec,
            flags,
            test,
            tuple_mode,
        } => Op::RegexMatch {
            spec: Box::new(compile_stage(spec)),
            flags: flags.as_ref().map(|flags| Box::new(compile_stage(flags))),
            test: *test,
            tuple_mode: *tuple_mode,
        },
        Stage::RegexCapture {
            spec,
            flags,
            tuple_mode,
        } => Op::RegexCapture {
            spec: Box::new(compile_stage(spec)),
            flags: flags.as_ref().map(|flags| Box::new(compile_stage(flags))),
            tuple_mode: *tuple_mode,
        },
        Stage::RegexScan { regex, flags } => Op::RegexScan {
            regex: Box::new(compile_stage(regex)),
            flags: flags.as_ref().map(|flags| Box::new(compile_stage(flags))),
        },
        Stage::RegexSplits { regex, flags } => Op::RegexSplits {
            regex: Box::new(compile_stage(regex)),
            flags: flags.as_ref().map(|flags| Box::new(compile_stage(flags))),
        },
        Stage::RegexSub {
            regex,
            replacement,
            flags,
            global,
        } => Op::RegexSub {
            regex: Box::new(compile_stage(regex)),
            replacement: Box::new(compile_stage(replacement)),
            flags: Box::new(compile_stage(flags)),
            global: *global,
        },
        Stage::Path(arg) => Op::Path(Box::new(compile_stage(arg))),
        Stage::Paths => Op::Paths,
        Stage::GetPath(arg) => Op::GetPath(Box::new(compile_stage(arg))),
        Stage::SetPath(path, value) => Op::SetPath(
            Box::new(compile_stage(path)),
            Box::new(compile_stage(value)),
        ),
        Stage::Modify(path, update) => Op::Modify(
            Box::new(compile_stage(path)),
            Box::new(compile_stage(update)),
        ),
        Stage::DelPaths(arg) => Op::DelPaths(Box::new(compile_stage(arg))),
        Stage::TruncateStream(arg) => Op::TruncateStream(Box::new(compile_stage(arg))),
        Stage::FromStream(arg) => Op::FromStream(Box::new(compile_stage(arg))),
        Stage::ToStream => Op::ToStream,
        Stage::Flatten(arg) => Op::Flatten(Box::new(compile_stage(arg))),
        Stage::FlattenRaw(arg) => Op::FlattenRaw(Box::new(compile_stage(arg))),
        Stage::Nth(arg) => Op::Nth(Box::new(compile_stage(arg))),
        Stage::NthBy(index, source) => Op::NthBy(
            Box::new(compile_stage(index)),
            Box::new(compile_stage(source)),
        ),
        Stage::LimitBy(count, source) => Op::LimitBy(
            Box::new(compile_stage(count)),
            Box::new(compile_stage(source)),
        ),
        Stage::SkipBy(count, source) => Op::SkipBy(
            Box::new(compile_stage(count)),
            Box::new(compile_stage(source)),
        ),
        Stage::Range(init, upto, by) => Op::Range(
            Box::new(compile_stage(init)),
            Box::new(compile_stage(upto)),
            Box::new(compile_stage(by)),
        ),
        Stage::While(cond, update) => Op::While(
            Box::new(compile_stage(cond)),
            Box::new(compile_stage(update)),
        ),
        Stage::Until(cond, next) => {
            Op::Until(Box::new(compile_stage(cond)), Box::new(compile_stage(next)))
        }
        Stage::Reduce {
            source,
            pattern,
            init,
            update,
        } => Op::Reduce {
            source: Box::new(compile_stage(source)),
            pattern: compile_binding_pattern(pattern),
            init: Box::new(compile_stage(init)),
            update: Box::new(compile_stage(update)),
        },
        Stage::Foreach {
            source,
            pattern,
            init,
            update,
            extract,
        } => Op::Foreach {
            source: Box::new(compile_stage(source)),
            pattern: compile_binding_pattern(pattern),
            init: Box::new(compile_stage(init)),
            update: Box::new(compile_stage(update)),
            extract: Box::new(compile_stage(extract)),
        },
        Stage::Any(generator, condition) => Op::Any(
            Box::new(compile_stage(generator)),
            Box::new(compile_stage(condition)),
        ),
        Stage::All(generator, condition) => Op::All(
            Box::new(compile_stage(generator)),
            Box::new(compile_stage(condition)),
        ),
        Stage::FirstBy(source) => Op::FirstBy(Box::new(compile_stage(source))),
        Stage::LastBy(source) => Op::LastBy(Box::new(compile_stage(source))),
        Stage::IsEmpty(arg) => Op::IsEmpty(Box::new(compile_stage(arg))),
        Stage::AddBy(arg) => Op::AddBy(Box::new(compile_stage(arg))),
        Stage::Select(arg) => Op::Select(Box::new(compile_stage(arg))),
        Stage::Map(arg) => Op::Map(Box::new(compile_stage(arg))),
        Stage::MapValues(arg) => Op::MapValues(Box::new(compile_stage(arg))),
        Stage::WithEntries(arg) => Op::WithEntries(Box::new(compile_stage(arg))),
        Stage::RecurseBy(arg) => Op::RecurseBy(Box::new(compile_stage(arg))),
        Stage::RecurseByCond(arg, cond) => {
            Op::RecurseByCond(Box::new(compile_stage(arg)), Box::new(compile_stage(cond)))
        }
        Stage::Walk(arg) => Op::Walk(Box::new(compile_stage(arg))),
        Stage::Combinations => Op::Combinations,
        Stage::Repeat(arg) => Op::Repeat(Box::new(compile_stage(arg))),
        Stage::Input => Op::Input,
        Stage::Format { fmt, expr } => Op::Format {
            fmt: fmt.clone(),
            expr: Box::new(compile_stage(expr)),
        },
        Stage::Strptime(format) => Op::Strptime(Box::new(compile_stage(format))),
        Stage::Strftime { format, local } => Op::Strftime {
            format: Box::new(compile_stage(format)),
            local: *local,
        },
        Stage::Empty => Op::Empty,
        Stage::Error(inner) => Op::Error(Box::new(compile_stage(inner))),
        Stage::HaltError(inner) => Op::HaltError(Box::new(compile_stage(inner))),
        Stage::UnaryMinus(inner) => Op::UnaryMinus(Box::new(compile_stage(inner))),
        Stage::UnaryNot(inner) => Op::UnaryNot(Box::new(compile_stage(inner))),
        Stage::TryCatch { inner, catcher } => Op::TryCatch {
            inner: Box::new(compile_stage(inner)),
            catcher: Box::new(compile_stage(catcher)),
        },
        Stage::IfElse {
            cond,
            then_expr,
            else_expr,
        } => Op::IfElse {
            cond: Box::new(compile_stage(cond)),
            then_expr: Box::new(compile_stage(then_expr)),
            else_expr: Box::new(compile_stage(else_expr)),
        },
        Stage::Binary { op, lhs, rhs } => Op::Binary {
            op: *op,
            lhs: Box::new(compile_stage(lhs)),
            rhs: Box::new(compile_stage(rhs)),
        },
        Stage::MathBinary { op, lhs, rhs } => Op::MathBinary {
            op: *op,
            lhs: Box::new(compile_stage(lhs)),
            rhs: Box::new(compile_stage(rhs)),
        },
        Stage::MathTernary { op, a, b, c } => Op::MathTernary {
            op: *op,
            a: Box::new(compile_stage(a)),
            b: Box::new(compile_stage(b)),
            c: Box::new(compile_stage(c)),
        },
        Stage::Field { name, optional } => Op::GetField {
            name: name.clone(),
            optional: *optional,
        },
        Stage::Index { index, optional } => Op::GetIndex {
            index: *index,
            optional: *optional,
        },
        Stage::DynamicIndex { key, optional } => Op::DynamicIndex {
            key: Box::new(compile_stage(key)),
            optional: *optional,
        },
        Stage::Slice {
            start,
            end,
            optional,
        } => Op::Slice {
            start: *start,
            end: *end,
            optional: *optional,
        },
        Stage::Bind {
            source,
            pattern,
            body,
        } => Op::Bind {
            source: Box::new(compile_stage(source)),
            pattern: compile_binding_pattern(pattern),
            body: Box::new(compile_stage(body)),
        },
        Stage::Iterate { optional } => Op::Iterate {
            optional: *optional,
        },
    }
}

fn compile_binding_pattern(pattern: &BindingPattern) -> OpBindingPattern {
    match pattern {
        BindingPattern::Var(name) => OpBindingPattern::Var(name.clone()),
        BindingPattern::Array(items) => {
            OpBindingPattern::Array(items.iter().map(compile_binding_pattern).collect())
        }
        BindingPattern::Object(entries) => OpBindingPattern::Object(
            entries
                .iter()
                .map(|entry| OpObjectBindingEntry {
                    key: compile_binding_key(&entry.key),
                    store_var: entry.store_var.clone(),
                    pattern: compile_binding_pattern(&entry.pattern),
                })
                .collect(),
        ),
        BindingPattern::Alternatives(items) => {
            OpBindingPattern::Alternatives(items.iter().map(compile_binding_pattern).collect())
        }
    }
}

fn compile_binding_key(key: &BindingKeySpec) -> OpBindingKeySpec {
    match key {
        BindingKeySpec::Literal(value) => OpBindingKeySpec::Literal(value.clone()),
        BindingKeySpec::Expr(expr) => OpBindingKeySpec::Expr(Box::new(compile_stage(expr))),
    }
}
