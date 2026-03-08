# New Core Migration (jq Architecture)

Date: 2026-03-06

## Freeze Contract

- Feature development is frozen.
- Migration target is a jq-equivalent architecture implemented natively in Rust.
- No external runtime dependencies for query execution.
- Source of truth for semantics is jq upstream sources and jq upstream tests.

## Target Runtime Model

`zq` migrates to the same execution model shape as jq:

1. Lexer/Parser for jq grammar.
2. Compiler from syntax to internal IR/bytecode.
3. VM runtime with:
   - explicit stack and frames,
   - closures/environments,
   - fork/backtrack semantics for generators and control flow,
   - jq-compatible error propagation.

Primary jq references:

- `.tmp/jq/src/lexer.l`
- `.tmp/jq/src/parser.y`
- `.tmp/jq/src/compile.c`
- `.tmp/jq/src/bytecode.c`
- `.tmp/jq/src/execute.c`

## Migration Strategy (Clustered)

1. Core syntax and pipeline:
   - `.`, `.foo`, `.[]`, `|`, `,`
2. Core expressions:
   - literals, arithmetic, comparisons, boolean ops
3. Control flow:
   - `if/then/else`, `try/catch`, `empty`, `error`
4. Iteration/generators:
   - `map`, `select`, `reduce`, `foreach`, `recurse`
5. Builtins and formatting:
   - string/time/path/math/regex clusters
6. Module/import and function semantics

For each cluster:

- Port semantics from jq source first.
- Add/port unit tests from jq where available.
- Add additional hardcode-guard tests for edge behavior.

## Current Status

- New `vm_core` skeleton is added under `src/native_engine/vm_core`.
- First covered cluster in the new core:
  - `.`, `..`, `.foo`, `.foo?`, `."foo"`, `."foo"?`, `.["foo"]`, `.["foo"]?`, `.[n]`, `.[n]?`, `.[expr]`, `.[expr]?`, `.[a:b]`, `.[a:b]?`, `.[]`, `.[]?`, `|`, `,`
  - literals: integers, strings, booleans, null, arrays, objects (including jq shorthand object fields, dynamic `({(expr): value})` keys, and cartesian stream object construction)
  - expressions: unary minus/not, `+ - * / %`, `== != < <= > >=`, `and`, `or`, `//`, parentheses
  - control: `if ... then ... else ... end`, `if ... then ... elif ... then ... else ... end`, `if ... then ... end` (jq-compatible implicit `else .`)
  - subqueries in parentheses/if branches/function args/array literals support jq-style `|` and `,` composition
  - error flow: `empty`, `error(...)`, `try ... catch ...`, `try ...`, `expr?`
  - core builtins: `length`, `abs`, `fabs`, `isinfinite`, `isnan`, `isnormal`, `isfinite`, `finites`, `normals`, `type`, `add`, `add(f)`, `keys`, `keys_unsorted`, `to_entries`, `from_entries`, `with_entries(f)`, `tonumber`, `tostring`, `toboolean`, `tojson`, `fromjson`, `utf8bytelength`, `explode`, `implode`, `trim`, `ltrim`, `rtrim`, `reverse`, `ascii_upcase`, `ascii_downcase`, `transpose`, `flatten`, `range(n)`, `range(init; upto)`, `range(init; upto; by)`, `while(cond; update)`, `until(cond; next)`, `recurse`, `recurse(f)`, `recurse(f;cond)`, `any(gen; cond)`, `all(gen; cond)`, `any(cond)`, `all(cond)`, `any`, `all`, `arrays`, `objects`, `iterables`, `booleans`, `numbers`, `strings`, `nulls`, `values`, `scalars`, `first`, `last`, `first(f)`, `last(f)`, `nth(f)`, `nth(i; f)`, `limit(n; f)`, `skip(n; f)`, `isempty(f)`, `sort`, `unique`, `min`, `max`, `has(f)`, `in(f)`, `contains(f)`, `inside(f)`, `bsearch(f)`, `startswith(f)`, `endswith(f)`, `split(f)`, `join(f)`, `ltrimstr(f)`, `rtrimstr(f)`, `trimstr(f)`, `indices(f)`, `index(f)`, `rindex(f)`, `select(f)`, `map(f)`, `map_values(f)`, `walk(f)`
- It is intentionally partial and not the final full jq port.
- Existing engine remains in place while clusters are migrated into the VM model.
