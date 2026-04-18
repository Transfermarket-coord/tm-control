#!/usr/bin/env bash
# worker_scrape_daily.sh
# =====================================================================
# Invoked by each worker (TM1..TM10) from its scrape-daily.yml workflow.
# This file lives in arifin971/tm-control/scripts/ — single source of
# truth for daily scrape logic. Update this file, all 10 workers get
# the new behavior on their next run.
#
# Required env vars (set by workflow):
#   ACCOUNT_ID       — 1..10
#   SEASON           — current season (e.g. 2025)
#   DCARIBOU_IMAGE   — docker image name
#   COMP_COOLDOWN_S  — sleep between competition scrapes (default 30)
#   MAX_MINUTES      — soft wallclock budget (default 320)
#
# Expects .tm-control/ to be a git clone of tm-control in the worker's
# checkout, so assignments.json is at .tm-control/assignments.json.
# =====================================================================
set -euo pipefail

START_EPOCH=$(date -u +%s)
MAX_SECS=$((${MAX_MINUTES:-320} * 60))
COOLDOWN=${COMP_COOLDOWN_S:-30}

echo "=== daily scrape: account=${ACCOUNT_ID} season=${SEASON} ==="

# Extract this account's assigned competitions
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

COUNT=$(echo "$COMPS" | wc -w)
echo "Assigned: ${COUNT} competitions"

IDX=0
for COMP in $COMPS; do
    IDX=$((IDX+1))
    ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
    if (( ELAPSED > MAX_SECS )); then
        echo "⏰ Budget exhausted (${ELAPSED}s > ${MAX_SECS}s). Stopping at ${IDX}/${COUNT}."
        break
    fi

    OUT_DIR="data/${COMP}/${SEASON}"
    mkdir -p "$OUT_DIR"

    echo ""
    echo "━━ [$IDX/$COUNT] ${COMP} ━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── Games spider ──
    # dcaribou pipe: competitions → games
    echo '{"comp_id":"'"$COMP"'","href":"/wettbewerb/'"$COMP"'"}' \
      | docker run --rm -i "$DCARIBOU_IMAGE" \
          python -m tfmkt games -s "$SEASON" 2>/dev/null \
      > "${OUT_DIR}/games.jsonl.tmp" || {
          echo "  WARN: games scrape failed for ${COMP}"
          rm -f "${OUT_DIR}/games.jsonl.tmp"
          continue
      }

    # Atomic move only if non-empty
    if [[ -s "${OUT_DIR}/games.jsonl.tmp" ]]; then
        mv "${OUT_DIR}/games.jsonl.tmp" "${OUT_DIR}/games.jsonl"
        GAMES_COUNT=$(wc -l < "${OUT_DIR}/games.jsonl")
        echo "  games: ${GAMES_COUNT} rows"
    else
        rm -f "${OUT_DIR}/games.jsonl.tmp"
        echo "  games: EMPTY (skipping lineups)"
        sleep "$COOLDOWN"
        continue
    fi

    # ── Filter to last 7 days (recent games only for lineups) ──
    python3 - "${OUT_DIR}/games.jsonl" "${OUT_DIR}/games_recent.jsonl" <<'PY'
import json, sys, datetime as dt
src, dst = sys.argv[1], sys.argv[2]
cutoff  = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).date()
ceiling =  dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)
recent = []
with open(src) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
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
    if recent:
        f.write("\n")
print(f"  recent_games: {len(recent)} of {sum(1 for _ in open(src))}")
PY

    # ── Lineups spider (recent games only) ──
    if [[ -s "${OUT_DIR}/games_recent.jsonl" ]]; then
        cat "${OUT_DIR}/games_recent.jsonl" \
          | docker run --rm -i "$DCARIBOU_IMAGE" \
              python -m tfmkt game_lineups 2>/dev/null \
          > "${OUT_DIR}/game_lineups.jsonl.tmp" || {
              echo "  WARN: lineups scrape failed for ${COMP}"
              rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
          }
        if [[ -s "${OUT_DIR}/game_lineups.jsonl.tmp" ]]; then
            mv "${OUT_DIR}/game_lineups.jsonl.tmp" "${OUT_DIR}/game_lineups.jsonl"
            LINEUPS_COUNT=$(wc -l < "${OUT_DIR}/game_lineups.jsonl")
            echo "  lineups: ${LINEUPS_COUNT} rows"
        else
            rm -f "${OUT_DIR}/game_lineups.jsonl.tmp"
        fi
    fi

    rm -f "${OUT_DIR}/games_recent.jsonl"

    sleep "$COOLDOWN"
done

FINAL_ELAPSED=$(( $(date -u +%s) - START_EPOCH ))
echo ""
echo "=== done: ${IDX}/${COUNT} comps processed in ${FINAL_ELAPSED}s ==="
