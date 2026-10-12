#!/usr/bin/env bash
# Regenerate the committed results from local captures: for each run, the req-check and
# pp-analyze verdicts with that scenario's flags, plus its compact summary and timeline. Raw
# captures (controller and driver logs, watch streams) stay local; they are large and carry
# cluster addresses.
#   archive-results.sh RUNS_DIR OUT_DIR
set -euo pipefail
RUNS=$1; OUT=$2; HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$OUT"
# run-dir glob | output name | req-check flags | pp-analyze flags ("-" = none)
while IFS='|' read -r glob name rc ppa; do
  [ -z "$glob" ] && continue
  for d in $RUNS/$glob; do
    [ -d "$d" ] || continue
    f="$OUT/$name.txt"
    { echo "# $name  (capture: $(basename "$d"))"
      grep -m1 'kueue-controller-manager' "$d/context.txt" 2>/dev/null | sed 's/^/# /' || true
      echo; echo "## req-check $rc"
      python3 -I "$HERE/req-check.py" "$d" $rc
      if [ "$ppa" != "-" ]; then echo; echo "## pp-analyze $ppa"; python3 -I "$HERE/pp-analyze.py" $ppa "$d"; fi
      echo; echo "## summary.txt"; cat "$d/summary.txt" 2>/dev/null || true; } > "$f" 2>&1
    cp "$d/timeline.csv" "$OUT/$name.timeline.csv" 2>/dev/null || true
  done
done <<'MAP'
REQ-1011-1054-S4-preempt-partial-cap2|baseline-S4-gate-off|--cap 2 --partial --preemption|--expect-whole 14
REQ-1011-1054-S5-preempt-atomic|baseline-S5-gate-off|--preemption|--expect-whole 11
PP-1012-0329-P1-S4-floor-min|r0-ec011894f-P1-S4-floor-min|--cap 2 --partial --preemption|--floor 3 --expect-whole 0
PP-1012-0625-P1-S4-floor-min|r1-505348177-P1-S4-floor-min|--cap 2 --partial --preemption|--floor 3 --expect-whole 0
PP-1012-0625-P2-S4-floor1|r1-505348177-P2-S4-floor1|--cap 2 --partial --preemption|--floor 1 --expect-whole 0
PP-1012-0625-P3-S5-floor1|r1-505348177-P3-S5-floor1|--preemption|--floor 1 --expect-whole 0
PP-1012-0625-P4-S4-floor1-nodecom|r1-505348177-P4-S4-floor1-nodecom|--cap 2 --partial --preemption|--floor 1 --expect-whole 0
PP-1012-0625-P5-static-victims|r1-505348177-P5-static-victims|--slots 24 --preemption|--floor 2 --expect-whole 0
PP-1012-0731-P1-S4-floor-min|r2-a901d16b3-P1-S4-floor-min|--cap 2 --partial --preemption|--floor 3 --expect-whole 0
PP-1012-0731-P2-S4-floor1|r2-a901d16b3-P2-S4-floor1|--cap 2 --partial --preemption|--floor 1 --expect-whole 0
PP-1012-0731-P3-S5-floor1|r2-a901d16b3-P3-S5-floor1|--preemption|--floor 1 --expect-whole 0
PP-1012-0731-P4-S4-floor1-nodecom|r2-a901d16b3-P4-S4-floor1-nodecom|--cap 2 --partial --preemption|--floor 1 --expect-whole 0
PP-1012-0731-P5-static-victims|r2-a901d16b3-P5-static-victims|--slots 24 --preemption|--floor 2 --expect-whole 0
PPT-*-P3R-S5-floor1|r3-18abe488b-P3R-S5-floor1|--preemption|--floor 1 --expect-whole 0
PPT-*-P5B-static-contended|r3-18abe488b-P5B-static-contended|--slots 18 --preemption|--floor 2 --expect-whole 0
PPT-*-P6-not-opted-in|r3-18abe488b-P6-not-opted-in|--preemption|--expect-unchanged
PPT-*-P7a-fallback-wait|r3-18abe488b-P7a-fallback-wait|--preemption|--expect-whole 0 --floor 5
PPT-*-P7b-fallback-evict|r3-18abe488b-P7b-fallback-evict|--preemption|--min-whole 1
PPT-*-P8-spread-4-victims|r3-18abe488b-P8-spread-4-victims|--slots 24 --cap 2 --partial --preemption|--expect-whole 0 --floor 1
PPT-*-P9-timeout-1s|r3-18abe488b-P9-timeout-1s|--cap 2 --partial --preemption|--expect-whole 0 --floor 1 --expect-timeout
REQ-1012-*-S1-equal-partial-cap2|r3-18abe488b-REQ-S1-equal-partial-cap2|--cap 2 --partial|-
REQ-1012-*-S2-equal-control|r3-18abe488b-REQ-S2-equal-control||-
REQ-1012-*-S3-aggressive-partial|r3-18abe488b-REQ-S3-aggressive-partial|--cap 2 --partial|-
REQ-1012-*-S4-preempt-partial-cap2|r3-18abe488b-REQ-S4-preempt-partial-cap2|--cap 2 --partial --preemption|--floor 3 --expect-whole 0
REQ-1012-*-S5-preempt-atomic|r3-18abe488b-REQ-S5-preempt-atomic|--preemption|--floor 3 --expect-whole 0
MAP
echo "archived into $OUT"
