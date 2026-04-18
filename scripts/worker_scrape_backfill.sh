#!/usr/bin/env bash
# worker_scrape_backfill.sh v4
# v4 changes:
#   - MAX_SECS buffer (10min before GHA SIGKILL)
#   - THROTTLE ABORT uses `break 2` — prevents re-attacking next season
#     when TM is already blocking us

set -u

if [[ -z "${SEASONS_TO_SCRAPE:-}" ]]; then
    echo "ERROR: SEASONS_TO_SCRAPE not set"
    exit 0
fi

START_EPOCH=$(date -u +%s)
# 10-min buffer before GHA SIGKILL at 360min
MAX_SECS=$(( ( ${MAX_MINUTES:-320} - 10 ) * 60 ))
COOLDOWN=${COMP_COOLDOWN_S:-30}
COMPS_JSONL=".tm-control/competitions_raw.jsonl"
CONSECUTIVE_FAIL_LIMIT=${CONSECUTIVE_FAIL_LIMIT:-3}

echo "=== backfill: account=${ACCOUNT_ID} seasons=[${SEASONS_TO_SCRAPE}] ==="
echo "  budget: $((MAX_SECS / 60))min (MAX_MINUTES=${MAX_MINUTES:-320}, -10min buffer)"

if [[ ! -s "$COMPS_JSONL" ]]; then
    echo "FATAL: $COMPS_JSONL missing."
    exit 0
fi

COMPS=$(python3 -c "
import json, os
with open('.tm-control/assignments.json') as f:
    a = json.load(f)
print(' '.join(a['by_account'].get(os.environ['ACCOUNT_ID'], [])))
" 2>/dev/null)

if [[ -z "$COMPS" ]]; then
    echo "No comps assigned."
    exit 0
fi
COMP_COUNT=$(echo "$COMPS" | wc -w)

TOTAL_FAILS=0

for SEASON in $SEASONS_TO_SCRAPE; do
    echo ""
    echo "═══ SEASON ${SEASON} (${COMP_COUNT} comps) ═══"

    IDX=0
    CONSECUTIVE_FAILS=0

    for COMP in $COMPS; do
        IDX=$((IDX+1))
        ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
        if (( ELAPSED > MAX_SECS )); then
            echo "⏰ Global budget out at season ${SEASON}, comp ${IDX}/${COMP_COUNT}."
            break 2
        fi

        if (( CONSECUTIVE_FAILS >= CONSECUTIVE_FAIL_LIMIT )); then
            echo "⚠ ${CONSECUTIVE_FAILS} consecutive fails → TM throttling."
            echo "  Aborting entirely (not re-attacking next season)."
            break 2    # exit BOTH loops — fixes v3 leak
        fi

        OUT_DIR="data/${COMP}/${SEASON}"
        mkdir -p "$OUT_DIR"

        if [[ -s "${OUT_DIR}/games.jsonl" && -s "${OUT_DIR}/game_lineups.jsonl" ]]; then
            echo "  [$IDX/$COMP_COUNT] ${COMP}/${SEASON}: cached ✓"
            CONSECUTIVE_FAILS=0
            continue
        fi

        echo "━━ [$IDX/$COMP_COUNT] ${COMP}/${SEASON} ━━"

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
            echo "  SKIP: not in competitions_raw"
            continue
        fi

        GAMES_OK=0
        if [[ ! -s "${OUT_DIR}/games.jsonl" ]]; then
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
                    rm -f "${OUT_DIR}/games.stderr.log"
                    echo "    empty (dcaribou OK but 0 rows — no season data)"
                    CONSECUTIVE_FAILS=0
                fi
            else
                tail -3 "${OUT_DIR}/games.stderr.log" 2>/dev/null | sed 's/^/    /'
                rm -f "${OUT_DIR}/games.jsonl.tmp"
                CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS+1))
                TOTAL_FAILS=$((TOTAL_FAILS+1))
            fi
        else
            GAMES_OK=1
        fi

        if (( GAMES_OK )) && [[ ! -s "${OUT_DIR}/game_lineups.jsonl" ]]; then
            if cat "${OUT_DIR}/games.jsonl" \
               | docker run --rm -i "$DCARIBOU_IMAGE" \
                   python -m tfmkt game_lineups \
                   > "${OUT_DIR}/game_lineups.jsonl.tmp" 2> "${OUT_DIR}/lineups.stderr.log"; then
                if [[ -s "${OUT_DIR}/game_lineups.jsonl.tmp" ]]; then
                    mv "${OUT_DIR}/game_lineups.jsonl.tmp" "${OUT_DIR}/game_lineups.jsonl"
                    rm -f "${OUT_DIR}/lineups.stderr.log"
                    echo "  lineups: $(wc -l < "${OUT_DIR}/game_lineups.jsonl") rows"
                else
                    rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
                fi
            fi
        fi
        sleep "$COOLDOWN"
    done
done

FINAL_ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
ELAPSED_MIN=$(( (FINAL_ELAPSED + 59) / 60 ))

echo ""
echo "=== done in ${FINAL_ELAPSED}s (${ELAPSED_MIN}min), ${TOTAL_FAILS} failed ==="
echo "elapsed_min=${ELAPSED_MIN}" >> "${GITHUB_OUTPUT:-/dev/null}"
exit 0
