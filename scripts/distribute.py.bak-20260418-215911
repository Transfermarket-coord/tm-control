#!/usr/bin/env python3
"""
distribute.py v2 — Control plane competition distributor
=========================================================
Runs inside arifin971/tm-control/.github/workflows/discovery.yml.

INPUT:
  - config/workers.yml      (public-safe subset of tm_accounts.yml, synced from Servarica)
  - stdin                   (dcaribou competitions output, JSONL)

OUTPUT:
  - assignments.json        (public file workers curl)

LOGIC:
  1. Read workers.yml, filter to status=active accounts (typically 8 of 10)
  2. Read tier_1_pins — GB1 always goes to specific account regardless of hash
  3. For every other competition: md5(comp_id) % len(active) → active_ids[idx]
  4. Write assignments.json

CHANGES FROM v1:
  - v1: hardcoded 10-account distribution with md5 % 10
  - v2: reads YAML, filters by status, dynamic pool size
  - v2: backup accounts EXCLUDED from assignment (but listed in output for audit)
  - v2: quota_paused accounts also excluded (treated like backup until quota resets)

FAIL MODES:
  - YAML missing → exit 2
  - No active accounts → exit 3
  - GB1 pinned to non-active id → exit 4
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(5)


# ─── Classification (from v1, unchanged) ──────────────────────────────────────

TIER_1_COMP_IDS = {"GB1"}

TIER_2_COUNTRIES = {
    # 55 top football nations — first_tier competitions get daily scraping
    "ES", "IT", "DE", "FR", "PT", "NL", "BE", "TR", "RU", "UA",
    "GR", "AT", "CH", "SCO", "DK", "SE", "NO", "FI", "PL", "CZ",
    "HR", "RS", "RO", "HU", "BG", "CY", "IS", "IE", "SK", "SI",
    "BY", "AZ", "KZ", "IL", "LV", "LT", "EE", "LU",
    "BR", "AR", "MX", "CO", "CL", "UY", "PE", "EC", "PY", "US",
    "JP", "KR", "CN", "AU", "SA", "AE", "QA",
}

TIER_2_CUP_COMP_IDS = {"CL", "EL", "UECL"}


def comp_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    parts = [p for p in href.strip("/").split("/") if p]
    if parts and parts[-1] and len(parts[-1]) <= 10 and parts[-1].replace("_", "").isalnum():
        return parts[-1].upper()
    return None


def classify_tier(comp: Dict[str, Any]) -> int:
    comp_id = comp.get("comp_id", "") or ""
    comp_id_up = comp_id.upper()
    country = (comp.get("country_id", "") or comp.get("country_code", "") or "").upper()
    is_first_tier = comp.get("is_first_tier", False)

    if comp_id_up in TIER_1_COMP_IDS:
        return 1
    if comp_id_up in TIER_2_CUP_COMP_IDS:
        return 2
    if country in TIER_2_COUNTRIES and is_first_tier:
        return 2
    return 3


# ─── Active-pool assignment ───────────────────────────────────────────────────

def hash_to_active(comp_id: str, active_ids: List[int]) -> int:
    """Deterministic: same comp_id + same active_ids → same assignment."""
    if not active_ids:
        raise ValueError("active_ids is empty")
    h = int(hashlib.md5(comp_id.encode()).hexdigest(), 16)
    return active_ids[h % len(active_ids)]


def load_workers(config_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Returns (accounts, tier_1_pins). Exits on bad config."""
    if not config_path.exists():
        print(f"ERROR: {config_path} not found", file=sys.stderr)
        sys.exit(2)
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    accounts = cfg.get("accounts") or []
    tier_1_pins = cfg.get("tier_1_pins") or {}
    if not accounts:
        print("ERROR: no accounts in config", file=sys.stderr)
        sys.exit(2)
    return accounts, tier_1_pins


def get_active_ids(accounts: List[Dict[str, Any]]) -> List[int]:
    active = sorted([a["id"] for a in accounts if a.get("status") == "active"])
    if not active:
        print("ERROR: no active accounts; nothing to assign", file=sys.stderr)
        sys.exit(3)
    return active


def validate_tier_1_pins(tier_1_pins: Dict[str, int], active_ids: List[int]) -> None:
    for comp_id, pinned_id in tier_1_pins.items():
        if pinned_id not in active_ids:
            print(
                f"ERROR: tier_1_pin {comp_id} → id:{pinned_id} is not active "
                f"(active = {active_ids})",
                file=sys.stderr,
            )
            sys.exit(4)


def normalize_competition(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    comp_id = raw.get("comp_id") or raw.get("competition_id") or comp_id_from_href(
        raw.get("href", "")
    )
    if not comp_id:
        return None
    return {
        "comp_id":       str(comp_id).upper(),
        "name":          raw.get("name") or raw.get("competition_name") or "",
        "country_id":    (raw.get("country_id") or raw.get("country_code") or "").upper(),
        "is_first_tier": bool(raw.get("is_first_tier", False)),
        "href":          raw.get("href", ""),
    }


def distribute(
    competitions: List[Dict[str, Any]],
    active_ids: List[int],
    tier_1_pins: Dict[str, int],
) -> Dict[str, List[str]]:
    """Returns {account_id_str: [comp_id, ...]} assignments."""
    assignments: Dict[str, List[str]] = {str(aid): [] for aid in active_ids}
    seen_comps: set = set()

    for comp in competitions:
        cid = comp["comp_id"]
        if cid in seen_comps:
            continue
        seen_comps.add(cid)

        # Tier-1 pin overrides hash
        if cid in tier_1_pins:
            target = tier_1_pins[cid]
        else:
            target = hash_to_active(cid, active_ids)

        assignments[str(target)].append(cid)

    # Sort each account's list for determinism
    for aid in assignments:
        assignments[aid].sort()

    return assignments


def build_meta(
    accounts: List[Dict[str, Any]],
    active_ids: List[int],
    tier_1_pins: Dict[str, int],
    competitions: List[Dict[str, Any]],
    assignments: Dict[str, List[str]],
) -> Dict[str, Any]:
    by_tier = {1: 0, 2: 0, 3: 0}
    for comp in competitions:
        by_tier[classify_tier(comp)] += 1

    return {
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "active_ids":       active_ids,
        "backup_ids":       sorted([a["id"] for a in accounts if a.get("status") == "backup"]),
        "disabled_ids":     sorted([a["id"] for a in accounts if a.get("status") == "disabled"]),
        "quota_paused_ids": sorted([a["id"] for a in accounts if a.get("status") == "quota_paused"]),
        "tier_1_pins":      tier_1_pins,
        "total_competitions": len(competitions),
        "by_tier":          by_tier,
        "competitions_per_account": {aid: len(comps) for aid, comps in assignments.items()},
        "by_account":       assignments,
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def read_competitions_from_stdin() -> List[Dict[str, Any]]:
    """Read dcaribou competitions JSONL from stdin, normalize, return list."""
    comps: List[Dict[str, Any]] = []
    for line_num, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"WARN: line {line_num} malformed, skipped: {e}", file=sys.stderr)
            continue
        normalized = normalize_competition(raw)
        if normalized:
            comps.append(normalized)
    return comps


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/workers.yml",
                    help="Path to workers.yml (public-safe subset)")
    ap.add_argument("--output", default="assignments.json",
                    help="Output assignments.json path")
    ap.add_argument("--from-file", default=None,
                    help="Read competitions from this JSONL file instead of stdin")
    args = ap.parse_args()

    accounts, tier_1_pins = load_workers(Path(args.config))
    active_ids = get_active_ids(accounts)
    validate_tier_1_pins(tier_1_pins, active_ids)

    if args.from_file:
        comps: List[Dict[str, Any]] = []
        with open(args.from_file) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"WARN: {args.from_file}:{line_num} malformed, skipped: {e}",
                          file=sys.stderr)
                    continue
                n = normalize_competition(raw)
                if n:
                    comps.append(n)
    else:
        comps = read_competitions_from_stdin()

    if not comps:
        print("ERROR: no competitions received", file=sys.stderr)
        sys.exit(6)

    assignments = distribute(comps, active_ids, tier_1_pins)
    meta = build_meta(accounts, active_ids, tier_1_pins, comps, assignments)

    with open(args.output, "w") as f:
        json.dump(meta, f, indent=2, sort_keys=False)

    # Stderr summary (stdout reserved for JSON if ever piped)
    print(f"✓ Wrote {args.output}", file=sys.stderr)
    print(f"  active:  {active_ids}", file=sys.stderr)
    print(f"  comps:   {len(comps)} across {len(active_ids)} accounts", file=sys.stderr)
    print(f"  by tier: T1={meta['by_tier'][1]}  T2={meta['by_tier'][2]}  T3={meta['by_tier'][3]}",
          file=sys.stderr)
    load_dist = meta["competitions_per_account"]
    print(f"  load:    min={min(load_dist.values())}  max={max(load_dist.values())}  "
          f"spread={max(load_dist.values()) / max(1, min(load_dist.values())):.2f}x",
          file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
