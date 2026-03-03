#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/.tmp/bench/data.ndjson}"
ROWS="${ROWS:-200000}"

mkdir -p "$(dirname "$OUT")"

awk -v rows="$ROWS" '
BEGIN {
  split("alpha beta gamma delta omega theta", words, " ");
  for (i = 1; i <= rows; i++) {
    id = i;
    grp = i % 17;
    a = (i * 17) % 1000;
    b = (i * 31) % 1000;
    value = ((i * 97) % 10000) - 5000;
    active = (i % 3 == 0) ? "true" : "false";
    text_prefix = (i % 2 == 0) ? "alpha" : "beta";
    text_mid = (i % 5 == 0) ? "omega" : "delta";
    text = text_prefix "-" text_mid "-" i;

    tagc = (i % 4) + 1;
    tags = "[";
    for (t = 1; t <= tagc; t++) {
      idx = ((i + t) % 6) + 1;
      if (t > 1) tags = tags ",";
      tags = tags "\"" words[idx] "\"";
    }
    tags = tags "]";

    printf("{\"id\":%d,\"group\":%d,\"a\":%d,\"b\":%d,\"value\":%d,\"active\":%s,\"text\":\"%s\",\"tags\":%s}\n",
      id, grp, a, b, value, active, text, tags);
  }
}
' > "$OUT"

bytes=$(wc -c < "$OUT" | tr -d " ")
lines=$(wc -l < "$OUT" | tr -d " ")
echo "wrote $OUT ($lines lines, $bytes bytes)"
