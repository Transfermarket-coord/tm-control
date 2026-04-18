#!/usr/bin/env bash
# worker_scrape_daily.sh v2
# Uses competitions_raw.jsonl (cached in tm-control) to feed dcaribou
# with correct parent objects. Keeps stderr visible for debugging.
set -euo pipefail

START_EPOCH=$(date -u +%s)
MAX_SECS=$((${MAX_MINUTES:-320} * 60))
COOLDOWN=${COMP_COOLDOWN_S:-30}
COMPS_JSONL=".tm-control/competitions_raw.jsonl"

echo "=== daily scrape: account=${ACCOUNT_ID} season=${SEASON} ==="

if [[ ! -s "$COMPS_JSONL" ]]; then
    echo "FATAL: $COMPS_JSONL missing. Re-run discovery on tm-control."
    exit 1
fi
echo "competitions_raw: $(wc -l < "$COMPS_JSONL") entries"

COMPS=$(python3 -c "
import json, os
with open('.tm-control/assignments.json') as f:
    a = json.load(f)
print(' '.join(a['by_account'].get(os.environ['ACCOUNT_ID'], [])))
")

if [[ -z "$COMPS" ]]; then
    echo "No competitions assigned to account ${ACCOUNT_ID}."
    exit 0
fi

COUNT=$(echo "$COMPS" | wc -w)
echo "Assigned: ${COUNT} competitions"

IDX=0
FAILS=0
for COMP in $COMPS; do
    IDX=$((IDX+1))
    ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
    if (( ELAPSED > MAX_SECS )); then
        echo "⏰ Budget exhausted at ${IDX}/${COUNT}."
        break
    fi

    OUT_DIR="data/${COMP}/${SEASON}"
    mkdir -p "$OUT_DIR"

    echo ""
    echo "━━ [$IDX/$COUNT] ${COMP} ━━━━━━━━━━━━━━━━━━━━━━━━"

    # Extract the ONE matching competition line from cached jsonl
    COMP_JSON=$(python3 -c "
import json, sys
with open('${COMPS_JSONL}') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        last = obj.get('href','').strip('/').split('/')[-1] if obj.get('href') else ''
        if last.upper() == '${COMP}'.upper():
            print(line)
            sys.exit(0)
sys.exit(1)
")

    if [[ -z "$COMP_JSON" ]]; then
        echo "  SKIP: ${COMP} not in competitions_raw.jsonl"
        continue
    fi

    # ── Games spider (stderr NOT suppressed) ──
    if echo "$COMP_JSON" | docker run --rm -i "$DCARIBOU_IMAGE" \
         python -m tfmkt games -s "$SEASON" \
         > "${OUT_DIR}/games.jsonl.tmp" 2> "${OUT_DIR}/games.stderr.log"; then
        if [[ -s "${OUT_DIR}/games.jsonl.tmp" ]]; then
            mv "${OUT_DIR}/games.jsonl.tmp" "${OUT_DIR}/games.jsonl"
            rm -f "${OUT_DIR}/games.stderr.log"
            GAMES_COUNT=$(wc -l < "${OUT_DIR}/games.jsonl")
            echo "  games: ${GAMES_COUNT} rows"
        else
            rm -f "${OUT_DIR}/games.jsonl.tmp"
            echo "  games: EMPTY (see ${OUT_DIR}/games.stderr.log)"
            tail -5 "${OUT_DIR}/games.stderr.log" 2>/dev/null | sed 's/^/    /'
            FAILS=$((FAILS+1))
            sleep "$COOLDOWN"
            continue
        fi
    else
        echo "  games: FAILED (exit non-zero)"
        tail -10 "${OUT_DIR}/games.stderr.log" 2>/dev/null | sed 's/^/    /'
        rm -f "${OUT_DIR}/games.jsonl.tmp"
        FAILS=$((FAILS+1))
        sleep "$COOLDOWN"
        continue
    fi

    # Filter to last 7 days for lineups
    python3 - "${OUT_DIR}/games.jsonl" "${OUT_DIR}/games_recent.jsonl" <<'PY'
import json, sys, datetime as dt
src, dst = sys.argv[1], sys.argv[2]
cutoff  = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).date()
ceiling =  dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)
recent = []
with open(src) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            g = json.loads(line)
        except json.JSONDecodeError:
            continue
        d = g.get("date", "")
        try:
            gd = dt.date.fromisoformat(d[:10])
        except (ValueError, TypeError):
            continue
        if cutoff <= gd <= ceiling:
            recent.append(line)
with open(dst, "w") as f:
    f.write("\n".join(recent))
    if recent: f.write("\n")
print(f"  recent: {len(recent)} of {sum(1 for _ in open(src))}")
PY

    # Lineups
    if [[ -s "${OUT_DIR}/games_recent.jsonl" ]]; then
        if cat "${OUT_DIR}/games_recent.jsonl" \
           | docker run --rm -i "$DCARIBOU_IMAGE" \
               python -m tfmkt game_lineups \
               > "${OUT_DIR}/game_lineups.jsonl.tmp" 2> "${OUT_DIR}/lineups.stderr.log"; then
            if [[ -s "${OUT_DIR}/game_lineups.jsonl.tmp" ]]; then
                mv "${OUT_DIR}/game_lineups.jsonl.tmp" "${OUT_DIR}/game_lineups.jsonl"
                rm -f "${OUT_DIR}/lineups.stderr.log"
                echo "  lineups: $(wc -l < "${OUT_DIR}/game_lineups.jsonl") rows"
            else
                rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
                echo "  lineups: empty"
            fi
        fi
    fi
    rm -f "${OUT_DIR}/games_recent.jsonl"
    sleep "$COOLDOWN"
done

FINAL=$(( $(date -u +%s) - START_EPOCH ))
echo ""
echo "=== done: ${IDX}/${COUNT} comps, ${FAILS} failed, ${FINAL}s ==="
