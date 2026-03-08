use crate::value::ZqValue;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Query {
    pub(crate) branches: Vec<Pipeline>,
    pub(crate) functions: Vec<FunctionDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pipeline {
    pub(crate) stages: Vec<Stage>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FunctionDef {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) arity: usize,
    pub(crate) params: Vec<String>,
    pub(crate) param_ids: Vec<usize>,
    pub(crate) body: Stage,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ObjectKey {
    Static(String),
    Expr(Box<Stage>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BindingPattern {
    Var(String),
    Array(Vec<BindingPattern>),
    Object(Vec<ObjectBindingEntry>),
    Alternatives(Vec<BindingPattern>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjectBindingEntry {
    pub(crate) key: BindingKeySpec,
    pub(crate) store_var: Option<String>,
    pub(crate) pattern: BindingPattern,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BindingKeySpec {
    Literal(String),
    Expr(Box<Stage>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stage {
    Identity,
    Chain(Vec<Stage>),
    Pipe(Vec<Stage>),
    Call {
        function_id: Option<usize>,
        param_id: Option<usize>,
        name: String,
        args: Vec<Stage>,
    },
    Var(String),
    Label {
        name: String,
        body: Box<Stage>,
    },
    Break(String),
    Literal(ZqValue),
    Comma(Vec<Stage>),
    ArrayLiteral(Vec<Stage>),
    ObjectLiteral(Vec<(ObjectKey, Stage)>),
    Builtin(Builtin),
    Has(Box<Stage>),
    In(Box<Stage>),
    StartsWith(Box<Stage>),
    EndsWith(Box<Stage>),
    Split(Box<Stage>),
    Join(Box<Stage>),
    LTrimStr(Box<Stage>),
    RTrimStr(Box<Stage>),
    TrimStr(Box<Stage>),
    Indices(Box<Stage>),
    IndexOf(Box<Stage>),
    RIndexOf(Box<Stage>),
    Contains(Box<Stage>),
    Inside(Box<Stage>),
    BSearch(Box<Stage>),
    SortByImpl(Box<Stage>),
    GroupByImpl(Box<Stage>),
    UniqueByImpl(Box<Stage>),
    MinByImpl(Box<Stage>),
    MaxByImpl(Box<Stage>),
    RegexMatch {
        spec: Box<Stage>,
        flags: Option<Box<Stage>>,
        test: bool,
        tuple_mode: bool,
    },
    RegexCapture {
        spec: Box<Stage>,
        flags: Option<Box<Stage>>,
        tuple_mode: bool,
    },
    RegexScan {
        regex: Box<Stage>,
        flags: Option<Box<Stage>>,
    },
    RegexSplits {
        regex: Box<Stage>,
        flags: Option<Box<Stage>>,
    },
    RegexSub {
        regex: Box<Stage>,
        replacement: Box<Stage>,
        flags: Box<Stage>,
        global: bool,
    },
    Path(Box<Stage>),
    Paths,
    GetPath(Box<Stage>),
    SetPath(Box<Stage>, Box<Stage>),
    Modify(Box<Stage>, Box<Stage>),
    DelPaths(Box<Stage>),
    TruncateStream(Box<Stage>),
    FromStream(Box<Stage>),
    ToStream,
    Flatten(Box<Stage>),
    FlattenRaw(Box<Stage>),
    Nth(Box<Stage>),
    NthBy(Box<Stage>, Box<Stage>),
    LimitBy(Box<Stage>, Box<Stage>),
    SkipBy(Box<Stage>, Box<Stage>),
    Range(Box<Stage>, Box<Stage>, Box<Stage>),
    While(Box<Stage>, Box<Stage>),
    Until(Box<Stage>, Box<Stage>),
    Reduce {
        source: Box<Stage>,
        pattern: BindingPattern,
        init: Box<Stage>,
        update: Box<Stage>,
    },
    Foreach {
        source: Box<Stage>,
        pattern: BindingPattern,
        init: Box<Stage>,
        update: Box<Stage>,
        extract: Box<Stage>,
    },
    Any(Box<Stage>, Box<Stage>),
    All(Box<Stage>, Box<Stage>),
    FirstBy(Box<Stage>),
    LastBy(Box<Stage>),
    IsEmpty(Box<Stage>),
    AddBy(Box<Stage>),
    Select(Box<Stage>),
    Map(Box<Stage>),
    MapValues(Box<Stage>),
    WithEntries(Box<Stage>),
    RecurseBy(Box<Stage>),
    RecurseByCond(Box<Stage>, Box<Stage>),
    Walk(Box<Stage>),
    Combinations,
    Repeat(Box<Stage>),
    Input,
    Format {
        fmt: String,
        expr: Box<Stage>,
    },
    Strptime(Box<Stage>),
    Strftime {
        format: Box<Stage>,
        local: bool,
    },
    Empty,
    Error(Box<Stage>),
    HaltError(Box<Stage>),
    UnaryMinus(Box<Stage>),
    UnaryNot(Box<Stage>),
    TryCatch {
        inner: Box<Stage>,
        catcher: Box<Stage>,
    },
    IfElse {
        cond: Box<Stage>,
        then_expr: Box<Stage>,
        else_expr: Box<Stage>,
    },
    Binary {
        op: BinaryOp,
        lhs: Box<Stage>,
        rhs: Box<Stage>,
    },
    MathBinary {
        op: MathBinaryOp,
        lhs: Box<Stage>,
        rhs: Box<Stage>,
    },
    MathTernary {
        op: MathTernaryOp,
        a: Box<Stage>,
        b: Box<Stage>,
        c: Box<Stage>,
    },
    Field {
        name: String,
        optional: bool,
    },
    Index {
        index: i64,
        optional: bool,
    },
    DynamicIndex {
        key: Box<Stage>,
        optional: bool,
    },
    Slice {
        start: Option<i64>,
        end: Option<i64>,
        optional: bool,
    },
    Bind {
        source: Box<Stage>,
        pattern: BindingPattern,
        body: Box<Stage>,
    },
    Iterate {
        optional: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    DefinedOr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MathBinaryOp {
    Atan2,
    Hypot,
    CopySign,
    Drem,
    Fdim,
    Fmax,
    Fmin,
    Fmod,
    Jn,
    Ldexp,
    NextAfter,
    NextToward,
    Remainder,
    Scalb,
    Scalbln,
    Yn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MathTernaryOp {
    Fma,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Builtin {
    Not,
    Length,
    FAbs,
    Floor,
    Ceil,
    Sqrt,
    Cbrt,
    Round,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Sin,
    Sinh,
    Tan,
    Tanh,
    Cos,
    Cosh,
    Exp,
    Exp2,
    Log,
    Log10,
    Log1p,
    Expm1,
    Log2,
    IsInfinite,
    IsNan,
    IsNormal,
    Type,
    Add,
    Keys,
    KeysUnsorted,
    ToEntries,
    FromEntries,
    ToNumber,
    ToString,
    ToBoolean,
    ToJson,
    FromJson,
    Utf8ByteLength,
    Explode,
    Implode,
    Trim,
    LTrim,
    RTrim,
    Reverse,
    AsciiUpcase,
    AsciiDowncase,
    Flatten,
    Transpose,
    First,
    Last,
    Sort,
    Unique,
    Min,
    Max,
    Gmtime,
    Localtime,
    Mktime,
    FromDateIso8601,
    ToDateIso8601,
    Debug,
    Stderr,
    ModuleMeta,
    Env,
    Halt,
    GetSearchList,
    GetProgOrigin,
    GetJqOrigin,
    Now,
    InputFilename,
    InputLineNumber,
    HaveDecnum,
    HaveLiteralNumbers,
    BuiltinsList,
}
