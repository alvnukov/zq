# jq-compat prelude for the built-in native zq runtime.

# zq keeps exact big integers/decimals, so jq compatibility checks should
# follow decimal-capable expectations.
def have_decnum: true;
def modulemeta:
  if type == "string" then
    $__zq_modulemeta[.] // {deps: [], defs: []}
  else
    error("modulemeta input module name must be a string")
  end;

def builtins: [
  "abs/0","add/0","all/0","any/0","contains/1","endswith/1","explode/0","fromjson/0",
  "implode/0","index/1","indices/1","join/1","keys/0","length/0","map/1","map_values/1",
  "max/0","min/0","path/1","paths/0","range/1","reverse/0","select/1","sort/0",
  "split/1","startswith/1","tojson/0","tonumber/0","tostring/0","type/0","values/0","walk/1"
];

# Keep jq NaN/Infinity primitives independent from division-by-zero semantics.
def nan: "nan" | __zq_tonumber_native;
def infinite: "1e9999" | __zq_tonumber_native;
def isnan: type == "number" and (. != .);
def isinfinite: type == "number" and (isnan | not) and (. == infinite or . == -infinite);
def isfinite: type == "number" and (isnan | not) and (isinfinite | not);

# Keep jq path-update behavior for getpath (from std defs),
# but use stricter native set/del path helpers.
def setpath($path; $x): __zq_setpath_native($path; $x);
def delpaths($paths): __zq_delpaths_native($paths);
def del(f): delpaths([path(f)]);

def tonumber: __zq_tonumber_native;

# jq expects strict boolean parsing for toboolean.
def toboolean:
  if type == "boolean" then .
  elif type == "string" then
    if . == "true" then true
    elif . == "false" then false
    else error("string (\(tojson)) cannot be parsed as a boolean") end
  else error("\(type) (\(tojson)) cannot be parsed as a boolean")
  end;

# Derive UTF-8 byte length via base64 encoding to avoid codepoint-count semantics.
def utf8bytelength:
  if type == "string" then
    @base64 as $b
    | ($b | length) as $n
    | ($n / 4 | floor) * 3
      - (if ($b | endswith("==")) then 2 elif ($b | endswith("=")) then 1 else 0 end)
  else error("\(type) (\(tojson)) only strings have UTF-8 byte length")
  end;

# jq-compatible join/from_entries semantics.
def join($x): reduce .[] as $i (null;
            (if .==null then "" else .+$x end) +
            ($i | if type=="boolean" or type=="number" then tostring else .//"" end)
        ) // "";

def from_entries:
  map({(.key // .Key // .name // .Name): (if has("value") then .value else .Value end)})
  | add
  | .//={};

# jq-compatible flatten and trimstr variants.
def flatten($x): __zq_flatten_native($x);
def flatten: __zq_flatten_native;

def ltrimstr($left):
  if type != "string" or ($left|type) != "string" then
    error("startswith() requires string inputs")
  elif startswith($left) then .[$left | length:]
  else .
  end;
def rtrimstr($right):
  if type != "string" or ($right|type) != "string" then
    error("endswith() requires string inputs")
  elif endswith($right) then .[:length - ($right | length)]
  else .
  end;
def trimstr($val): ltrimstr($val) | rtrimstr($val);

# Match jq's Unicode whitespace class used by trim/ltrim/rtrim.
def _is_trim_space($cp):
  ($cp == 9) or ($cp == 10) or ($cp == 11) or ($cp == 12) or ($cp == 13) or ($cp == 32) or
  ($cp == 133) or ($cp == 160) or ($cp == 5760) or
  ($cp >= 8192 and $cp <= 8202) or
  ($cp == 8232) or ($cp == 8233) or ($cp == 8239) or ($cp == 8287) or ($cp == 12288);

def ltrim:
  if type != "string" then error("trim input must be a string")
  else
    explode as $a
    | ([range(0; $a|length) | select((_is_trim_space($a[.])) | not)][0] // ($a|length)) as $i
    | $a[$i:] | implode
  end;

def rtrim:
  if type != "string" then error("trim input must be a string")
  else
    explode as $a
    | ([range(($a|length)-1; -1; -1) | select((_is_trim_space($a[.])) | not)][0] // -1) as $j
    | $a[:($j + 1)] | implode
  end;

def trim:
  if type != "string" then error("trim input must be a string")
  else ltrim | rtrim
  end;

# jq-style regex combinators, including non-cartesian sub/gsub behavior.
def scan($re; $flags):
  match($re; "g" + ($flags // ""))
    | if (.captures | length) > 0
      then [.captures[] | .string]
      else .string
      end;
def scan($re): scan($re; null);

def splits($re; $flags):
  .[foreach (match($re; ($flags // "") + "g"), null) as {$offset, $length}
      (null; {start: .next, end: $offset, next: ($offset + $length)})];
def splits($re): splits($re; null);

def split($re; $flags):
  if ($re == "") and (($flags // "") == "") then
    explode | map([.] | implode)
  else
    [splits($re; $flags)]
  end;
def split($re): split($re; null);

# If s contains capture variables, then create a capture object and pipe it to s.
def sub($re; s; $flags):
   . as $in
   | (reduce match($re; ($flags // "")) as $edit
        ({result: [], previous: 0};
            $in[ .previous: ($edit | .offset) ] as $gap
            | [reduce ($edit | .captures | .[] | select(.name != null) | { (.name): .string }) as $pair
                 ({}; . + $pair) | s ] as $inserts
            | reduce range(0; $inserts|length) as $ix (.; .result[$ix] += $gap + $inserts[$ix])
            | .previous = ($edit | .offset + .length))
          | .result[] + $in[.previous:] )
      // $in;
def sub($re; s): sub($re; s; "");
def gsub($re; s; flags): sub($re; s; (flags // "") + "g");
def gsub($re; s): sub($re; s; "g");

# jq generic iterator/generator helpers.
def while(cond; update):
     def _while:
         if cond then ., (update | _while) else empty end;
     _while;
def until(cond; next):
     def _until:
         if cond then . else (next|_until) end;
     _until;
def limit($n; expr):
  if $n > 0 then label $out | foreach expr as $item ($n; . - 1; $item, if . <= 0 then break $out else empty end)
  elif $n == 0 then empty
  else error("limit doesn't support negative count") end;
def skip($n; expr):
  if $n > 0 then foreach expr as $item ($n; . - 1; if . < 0 then $item else empty end)
  elif $n == 0 then expr
  else error("skip doesn't support negative count") end;
def first(g): label $out | g | ., break $out;
def isempty(g): first((g|false), true);
def all(generator; condition): isempty(generator|condition and empty);
def any(generator; condition): isempty(generator|condition or empty)|not;
def all(condition): all(.[]; condition);
def any(condition): any(.[]; condition);
def all: all(.[]; .);
def any: any(.[]; .);
def nth($n; g):
  if $n < 0 then error("nth doesn't support negative indices")
  else first(skip($n; g)) end;
def first: .[0];
def last: .[-1];
def nth($n): .[$n];

# Helpers needed by upstream jq tests.
def combinations:
    if length == 0 then [] else
        .[0][] as $x
          | (.[1:] | combinations) as $y
          | [$x] + $y
    end;
def combinations(n):
    . as $dot
      | [range(n) | $dot]
      | combinations;
def repeat(exp):
     def _repeat:
         exp, _repeat;
     _repeat;
def inputs: try repeat(input) catch if .=="break" then empty else error end;

def truncate_stream(stream):
  . as $n | null | stream | . as $input | if (.[0]|length) > $n then setpath([0];$input[0][$n:]) else empty end;
def fromstream(i): {x: null, e: false} as $init |
  foreach i as $i ($init
  ; if .e then $init else . end
  | if $i|length == 2
    then setpath(["e"]; $i[0]|length==0) | setpath(["x"]+$i[0]; $i[1])
    else setpath(["e"]; $i[0]|length==1) end
  ; if .e then .x else empty end);
def tostream:
  path(def r: (.[]?|r), .; r) as $p |
  getpath($p) |
  reduce path(.[]?) as $q ([$p, .]; [$p+$q]);

def INDEX(stream; idx_expr):
  reduce stream as $row ({}; .[$row|idx_expr|tostring] = $row);
def INDEX(idx_expr): INDEX(.[]; idx_expr);

def JOIN($idx; idx_expr):
  [.[] | [., $idx[idx_expr]]];
def JOIN($idx; stream; idx_expr):
  stream | [., $idx[idx_expr]];
def JOIN($idx; stream; idx_expr; join_expr):
  stream | [., $idx[idx_expr]] | join_expr;

def IN(s): any(s == .; .);
def IN(src; s): any(src == s; .);

# jq-compatible pick implementation over paths.
def pick(pathexps):
  . as $in
  | reduce path(pathexps) as $a (null; setpath($a; $in|getpath($a)));
