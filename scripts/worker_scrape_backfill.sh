#!/usr/bin/env bash
# worker_scrape_backfill.sh
# =====================================================================
# Backfill variant of worker_scrape_daily.sh. Invoked from scrape-backfill.yml.
#
# Key differences from daily:
#   - Scrapes one or more HISTORICAL seasons (from $SEASONS_TO_SCRAPE)
#   - Full-season lineups (not last-7-days), since everything is historical
#   - Writes to data/{comp_id}/{season}/*.jsonl — one dir per season
#   - Skips comps that already have games.jsonl with >0 rows (resume-safe)
#
# Required env vars (set by workflow):
#   ACCOUNT_ID            — 1..10
#   SEASONS_TO_SCRAPE     — space-separated seasons, e.g. "2024 2023"
#   DCARIBOU_IMAGE        — docker image name
#   COMP_COOLDOWN_S       — default 30
#   MAX_MINUTES           — default 320
# =====================================================================
set -euo pipefail

if [[ -z "${SEASONS_TO_SCRAPE:-}" ]]; then
    echo "ERROR: SEASONS_TO_SCRAPE not set"
    exit 1
fi

START_EPOCH=$(date -u +%s)
MAX_SECS=$((${MAX_MINUTES:-320} * 60))
COOLDOWN=${COMP_COOLDOWN_S:-30}

echo "=== backfill: account=${ACCOUNT_ID} seasons=[${SEASONS_TO_SCRAPE}] ==="

# Load this account's assigned competitions once
COMPS=$(python3 -c "
import json, os
with open('.tm-control/assignments.json') as f:
    a = json.load(f)
print(' '.join(a['by_account'].get(os.environ['ACCOUNT_ID'], [])))
")

if [[ -z "$COMPS" ]]; then
    echo "No competitions assigned to account ${ACCOUNT_ID}. Exiting cleanly."
    exit 0
fi

COMP_COUNT=$(echo "$COMPS" | wc -w)

for SEASON in $SEASONS_TO_SCRAPE; do
    echo ""
    echo "═══ SEASON ${SEASON} ═══ (${COMP_COUNT} competitions)"

    IDX=0
    for COMP in $COMPS; do
        IDX=$((IDX+1))
        ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
        if (( ELAPSED > MAX_SECS )); then
            echo "⏰ Budget exhausted at ${ELAPSED}s. Stopping mid-backfill."
            echo "   Will resume tomorrow from the same (comp, season) pair."
            break 2
        fi

        OUT_DIR="data/${COMP}/${SEASON}"
        mkdir -p "$OUT_DIR"

        # Resume-safe: skip if both games + lineups already present and non-empty
        if [[ -s "${OUT_DIR}/games.jsonl" && -s "${OUT_DIR}/game_lineups.jsonl" ]]; then
            G=$(wc -l < "${OUT_DIR}/games.jsonl")
            L=$(wc -l < "${OUT_DIR}/game_lineups.jsonl")
            echo "  [$IDX/$COMP_COUNT] ${COMP}/${SEASON}: cached (${G} games, ${L} lineups) ✓"
            continue
        fi

        echo "━━ [$IDX/$COMP_COUNT] ${COMP}/${SEASON} ━━━━━━━━━━━━━━━━━━━━"

        # ── Games ──
        if [[ ! -s "${OUT_DIR}/games.jsonl" ]]; then
            echo '{"comp_id":"'"$COMP"'","href":"/wettbewerb/'"$COMP"'"}' \
              | docker run --rm -i "$DCARIBOU_IMAGE" \
                  python -m tfmkt games -s "$SEASON" 2>/dev/null \
              > "${OUT_DIR}/games.jsonl.tmp" || {
                  echo "  WARN: games failed for ${COMP}/${SEASON}"
                  rm -f "${OUT_DIR}/games.jsonl.tmp"
                  continue
              }

            if [[ -s "${OUT_DIR}/games.jsonl.tmp" ]]; then
                mv "${OUT_DIR}/games.jsonl.tmp" "${OUT_DIR}/games.jsonl"
                GC=$(wc -l < "${OUT_DIR}/games.jsonl")
                echo "  games: ${GC} rows"
            else
                rm -f "${OUT_DIR}/games.jsonl.tmp"
                echo "  games: EMPTY → skipping lineups for ${COMP}/${SEASON}"
                continue
            fi
        fi

        # ── Lineups (full season — all games, not just last 7 days) ──
        if [[ ! -s "${OUT_DIR}/game_lineups.jsonl" && -s "${OUT_DIR}/games.jsonl" ]]; then
            cat "${OUT_DIR}/games.jsonl" \
              | docker run --rm -i "$DCARIBOU_IMAGE" \
                  python -m tfmkt game_lineups 2>/dev/null \
              > "${OUT_DIR}/game_lineups.jsonl.tmp" || {
                  echo "  WARN: lineups failed for ${COMP}/${SEASON}"
                  rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
              }

            if [[ -s "${OUT_DIR}/game_lineups.jsonl.tmp" ]]; then
                mv "${OUT_DIR}/game_lineups.jsonl.tmp" "${OUT_DIR}/game_lineups.jsonl"
                LC=$(wc -l < "${OUT_DIR}/game_lineups.jsonl")
                echo "  lineups: ${LC} rows"
            else
                rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
                echo "  lineups: EMPTY (older seasons often lack this)"
            fi
        fi

        sleep "$COOLDOWN"
    done
done

FINAL_ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
echo ""
echo "=== done in ${FINAL_ELAPSED}s ==="
