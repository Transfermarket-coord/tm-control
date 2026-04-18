#!/usr/bin/env bash
# worker_scrape_daily.sh v4
# v4 changes (over v3):
#   - MAX_SECS now has 10-min buffer before GHA 360min hard kill
#   - break 2 equivalent not needed here (single loop)
#   - Everything else carried from v3 (set -e removed, date parser fixed,
#     elapsed_min computed in bash, rate-limit cascade abort)

set -u

START_EPOCH=$(date -u +%s)
# 10-min buffer: stop scraping before GHA SIGKILLs us at 360min
MAX_SECS=$(( ( ${MAX_MINUTES:-320} - 10 ) * 60 ))
COOLDOWN=${COMP_COOLDOWN_S:-30}
COMPS_JSONL=".tm-control/competitions_raw.jsonl"
CONSECUTIVE_FAIL_LIMIT=${CONSECUTIVE_FAIL_LIMIT:-3}

echo "=== daily scrape: account=${ACCOUNT_ID} season=${SEASON} ==="
echo "  budget: $((MAX_SECS / 60))min (MAX_MINUTES=${MAX_MINUTES:-320}, -10min buffer)"

if [[ ! -s "$COMPS_JSONL" ]]; then
    echo "FATAL: $COMPS_JSONL missing. Re-run discovery on tm-control."
    exit 0
fi
echo "competitions_raw: $(wc -l < "$COMPS_JSONL") entries"

COMPS=$(python3 -c "
import json, os
with open('.tm-control/assignments.json') as f:
    a = json.load(f)
print(' '.join(a['by_account'].get(os.environ['ACCOUNT_ID'], [])))
" 2>/dev/null)

if [[ -z "$COMPS" ]]; then
    echo "No competitions assigned to account ${ACCOUNT_ID}."
    exit 0
fi

COUNT=$(echo "$COMPS" | wc -w)
echo "Assigned: ${COUNT} competitions"

IDX=0
FAILS=0
CONSECUTIVE_FAILS=0

for COMP in $COMPS; do
    IDX=$((IDX+1))
    ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
    if (( ELAPSED > MAX_SECS )); then
        echo "⏰ Budget exhausted at ${IDX}/${COUNT} (${ELAPSED}s)."
        break
    fi

    if (( CONSECUTIVE_FAILS >= CONSECUTIVE_FAIL_LIMIT )); then
        REMAINING=$((COUNT - IDX + 1))
        echo "⚠ ${CONSECUTIVE_FAILS} consecutive failures → TM throttling suspected."
        echo "  Aborting at ${IDX}/${COUNT}, skipping ${REMAINING} remaining."
        break
    fi

    OUT_DIR="data/${COMP}/${SEASON}"
    mkdir -p "$OUT_DIR"

    echo ""
    echo "━━ [$IDX/$COUNT] ${COMP} ━━━━━━━━━━━━━━━━━━━━━━━━"

    COMP_JSON=$(python3 -c "
import json, sys
with open('${COMPS_JSONL}') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try: obj = json.loads(line)
        except json.JSONDecodeError: continue
        href = obj.get('href','') or ''
        last = href.strip('/').split('/')[-1] if href else ''
        if last.upper() == '${COMP}'.upper():
            print(line); sys.exit(0)
sys.exit(1)
" 2>/dev/null)

    if [[ -z "$COMP_JSON" ]]; then
        echo "  SKIP: ${COMP} not in competitions_raw.jsonl"
        continue
    fi

    GAMES_OK=0
    if echo "$COMP_JSON" | docker run --rm -i "$DCARIBOU_IMAGE" \
         python -m tfmkt games -s "$SEASON" \
         > "${OUT_DIR}/games.jsonl.tmp" 2> "${OUT_DIR}/games.stderr.log"; then
        if [[ -s "${OUT_DIR}/games.jsonl.tmp" ]]; then
            mv "${OUT_DIR}/games.jsonl.tmp" "${OUT_DIR}/games.jsonl"
            rm -f "${OUT_DIR}/games.stderr.log"
            echo "  games: $(wc -l < "${OUT_DIR}/games.jsonl") rows"
            GAMES_OK=1
            CONSECUTIVE_FAILS=0
        else
            rm -f "${OUT_DIR}/games.jsonl.tmp"
            echo "  games: EMPTY"
            tail -3 "${OUT_DIR}/games.stderr.log" 2>/dev/null | sed 's/^/    /'
            FAILS=$((FAILS+1))
            CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS+1))
        fi
    else
        echo "  games: FAILED (exit non-zero)"
        tail -3 "${OUT_DIR}/games.stderr.log" 2>/dev/null | sed 's/^/    /'
        rm -f "${OUT_DIR}/games.jsonl.tmp"
        FAILS=$((FAILS+1))
        CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS+1))
    fi

    if (( GAMES_OK == 0 )); then
        sleep "$COOLDOWN"
        continue
    fi

    # Recent games filter (last 7 days, 'Day, DD/MM/YY' format)
    python3 - <<'PY' "${OUT_DIR}/games.jsonl" "${OUT_DIR}/games_recent.jsonl"
import json, sys, datetime as dt

src, dst = sys.argv[1], sys.argv[2]
today   = dt.datetime.now(dt.timezone.utc).date()
cutoff  = today - dt.timedelta(days=7)
ceiling = today + dt.timedelta(days=1)

def parse_tm(s):
    if not s: return None
    s = s.strip()
    if "," in s:
        s = s.split(",", 1)[1].strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

recent = []
total = 0
with open(src) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        total += 1
        try:
            g = json.loads(line)
        except json.JSONDecodeError:
            continue
        gd = parse_tm(g.get("date", ""))
        if gd and cutoff <= gd <= ceiling:
            recent.append(line)

with open(dst, "w") as f:
    f.write("\n".join(recent))
    if recent: f.write("\n")

print(f"  recent: {len(recent)} of {total}")
PY

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
                echo "  lineups: EMPTY"
                tail -3 "${OUT_DIR}/lineups.stderr.log" 2>/dev/null | sed 's/^/    /'
            fi
        else
            echo "  lineups: failed"
            rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
        fi
    fi
    rm -f "${OUT_DIR}/games_recent.jsonl"

    sleep "$COOLDOWN"
done

FINAL_ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
ELAPSED_MIN=$(( (FINAL_ELAPSED + 59) / 60 ))

echo ""
echo "=== done: ${IDX}/${COUNT} comps, ${FAILS} failed, ${FINAL_ELAPSED}s (${ELAPSED_MIN}min) ==="
echo "elapsed_min=${ELAPSED_MIN}" >> "${GITHUB_OUTPUT:-/dev/null}"

exit 0
