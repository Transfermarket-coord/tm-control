#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SEASONS_TO_SCRAPE:-}" ]]; then
    echo "ERROR: SEASONS_TO_SCRAPE not set"
    exit 1
fi

START_EPOCH=$(date -u +%s)
MAX_SECS=$((${MAX_MINUTES:-320} * 60))
COOLDOWN=${COMP_COOLDOWN_S:-30}
COMPS_JSONL=".tm-control/competitions_raw.jsonl"

echo "=== backfill: account=${ACCOUNT_ID} seasons=[${SEASONS_TO_SCRAPE}] ==="

if [[ ! -s "$COMPS_JSONL" ]]; then
    echo "FATAL: $COMPS_JSONL missing. Re-run discovery on tm-control."
    exit 1
fi

COMPS=$(python3 -c "
import json, os
with open('.tm-control/assignments.json') as f:
    a = json.load(f)
print(' '.join(a['by_account'].get(os.environ['ACCOUNT_ID'], [])))
")
[[ -z "$COMPS" ]] && { echo "No comps assigned."; exit 0; }
COMP_COUNT=$(echo "$COMPS" | wc -w)

for SEASON in $SEASONS_TO_SCRAPE; do
    echo ""
    echo "═══ SEASON ${SEASON} (${COMP_COUNT} comps) ═══"

    IDX=0
    for COMP in $COMPS; do
        IDX=$((IDX+1))
        ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
        (( ELAPSED > MAX_SECS )) && { echo "⏰ budget out"; break 2; }

        OUT_DIR="data/${COMP}/${SEASON}"
        mkdir -p "$OUT_DIR"

        if [[ -s "${OUT_DIR}/games.jsonl" && -s "${OUT_DIR}/game_lineups.jsonl" ]]; then
            echo "  [$IDX/$COMP_COUNT] ${COMP}/${SEASON}: cached ✓"
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
        except: continue
        last = obj.get('href','').strip('/').split('/')[-1] if obj.get('href') else ''
        if last.upper() == '${COMP}'.upper():
            print(line); sys.exit(0)
sys.exit(1)
")
        [[ -z "$COMP_JSON" ]] && { echo "  SKIP: not in competitions_raw"; continue; }

        if [[ ! -s "${OUT_DIR}/games.jsonl" ]]; then
            if echo "$COMP_JSON" | docker run --rm -i "$DCARIBOU_IMAGE" \
                 python -m tfmkt games -s "$SEASON" \
                 > "${OUT_DIR}/games.jsonl.tmp" 2> "${OUT_DIR}/games.stderr.log"; then
                if [[ -s "${OUT_DIR}/games.jsonl.tmp" ]]; then
                    mv "${OUT_DIR}/games.jsonl.tmp" "${OUT_DIR}/games.jsonl"
                    rm -f "${OUT_DIR}/games.stderr.log"
                    echo "  games: $(wc -l < "${OUT_DIR}/games.jsonl") rows"
                else
                    rm -f "${OUT_DIR}/games.jsonl.tmp"
                    tail -5 "${OUT_DIR}/games.stderr.log" | sed 's/^/    /'
                    continue
                fi
            else
                tail -10 "${OUT_DIR}/games.stderr.log" | sed 's/^/    /'
                rm -f "${OUT_DIR}/games.jsonl.tmp"
                continue
            fi
        fi

        if [[ ! -s "${OUT_DIR}/game_lineups.jsonl" && -s "${OUT_DIR}/games.jsonl" ]]; then
            cat "${OUT_DIR}/games.jsonl" \
              | docker run --rm -i "$DCARIBOU_IMAGE" \
                  python -m tfmkt game_lineups \
                  > "${OUT_DIR}/game_lineups.jsonl.tmp" 2>/dev/null || true
            if [[ -s "${OUT_DIR}/game_lineups.jsonl.tmp" ]]; then
                mv "${OUT_DIR}/game_lineups.jsonl.tmp" "${OUT_DIR}/game_lineups.jsonl"
                echo "  lineups: $(wc -l < "${OUT_DIR}/game_lineups.jsonl") rows"
            else
                rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
            fi
        fi
        sleep "$COOLDOWN"
    done
done
echo ""
echo "=== done in $(( $(date -u +%s) - START_EPOCH ))s ==="
