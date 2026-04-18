#!/usr/bin/env python3
"""
generate_backfill_schedule.py — Build seasons/backfill_schedule.json
=====================================================================
Reads config/workers.yml → seasons.{current, backfill_from, backfill_to,
recent_tier_count, mid_tier_count, old_tier_count}, then emits a JSON
schedule mapping ISO date → list-of-seasons to scrape that day.

SCHEDULE SHAPE (given defaults: 2000–2024 backfill + 5/10/10 tiers):
  Days 1–5:   1 season each (2024, 2023, 2022, 2021, 2020)   — recent tier
  Days 6–10:  2 seasons each (2019+2018, 2017+2016, ...)      — mid tier
  Days 11–12: 5 seasons each (2009..2005, 2004..2000)         — old tier

Total wallclock: 12 days from start date.

Workers read this file and pick up the seasons listed for TODAY. Deterministic
so multiple workers compute the same schedule.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed", file=sys.stderr)
    sys.exit(5)


def build_schedule(
    backfill_from: int,
    backfill_to: int,
    recent_n: int,
    mid_n: int,
    old_n: int,
    start_date: date,
) -> Dict[str, Any]:
    # Build season list ordered newest-first
    seasons_desc = list(range(backfill_to, backfill_from - 1, -1))

    # Split into tiers
    recent = seasons_desc[:recent_n]
    mid    = seasons_desc[recent_n:recent_n + mid_n]
    old    = seasons_desc[recent_n + mid_n:recent_n + mid_n + old_n]

    schedule: Dict[str, List[int]] = {}
    day_offset = 0

    # Tier 1: recent — 1 season per day
    for season in recent:
        d = start_date + timedelta(days=day_offset)
        schedule[d.isoformat()] = [season]
        day_offset += 1

    # Tier 2: mid — 2 seasons per day
    for i in range(0, len(mid), 2):
        d = start_date + timedelta(days=day_offset)
        schedule[d.isoformat()] = mid[i:i + 2]
        day_offset += 1

    # Tier 3: old — 5 seasons per day
    for i in range(0, len(old), 5):
        d = start_date + timedelta(days=day_offset)
        schedule[d.isoformat()] = old[i:i + 5]
        day_offset += 1

    end_date = start_date + timedelta(days=day_offset - 1)

    return {
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "start_date":      start_date.isoformat(),
        "end_date":        end_date.isoformat(),
        "total_days":      day_offset,
        "total_seasons":   len(recent) + len(mid) + len(old),
        "tier_breakdown": {
            "recent": {"count": len(recent), "seasons_per_day": 1, "seasons": recent},
            "mid":    {"count": len(mid),    "seasons_per_day": 2, "seasons": mid},
            "old":    {"count": len(old),    "seasons_per_day": 5, "seasons": old},
        },
        "schedule":        schedule,
    }


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/workers.yml")
    ap.add_argument("--output", default="seasons/backfill_schedule.json")
    ap.add_argument("--start-date", default=None,
                    help="ISO date to start backfill (default: tomorrow UTC)")
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    seasons = cfg.get("seasons", {}) or {}
    backfill_from    = int(seasons.get("backfill_from", 2000))
    backfill_to      = int(seasons.get("backfill_to", 2024))
    recent_n         = int(seasons.get("recent_tier_count", 5))
    mid_n            = int(seasons.get("mid_tier_count", 10))
    old_n            = int(seasons.get("old_tier_count", 10))

    start_date = (
        date.fromisoformat(args.start_date) if args.start_date
        else datetime.now(timezone.utc).date() + timedelta(days=1)
    )

    schedule = build_schedule(
        backfill_from, backfill_to,
        recent_n, mid_n, old_n,
        start_date,
    )

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(schedule, f, indent=2)

    print(f"✓ {args.output} written", file=sys.stderr)
    print(f"  start:    {schedule['start_date']}", file=sys.stderr)
    print(f"  end:      {schedule['end_date']}", file=sys.stderr)
    print(f"  days:     {schedule['total_days']}", file=sys.stderr)
    print(f"  seasons:  {schedule['total_seasons']} "
          f"(recent={recent_n}, mid={mid_n}, old={old_n})",
          file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
