#!/usr/bin/env python3
"""
check_quotas.py — Control-plane quota monitor
==============================================
Runs inside arifin971/tm-control/.github/workflows/discovery.yml, BEFORE
distribute.py. For every account listed in config/workers.yml:

  1. curl https://raw.githubusercontent.com/{owner}/{repo_name}/main/status.json
  2. Parse minutes_used_this_month
  3. If used / limit >= pause_threshold (default 0.90) → mark account as
     `quota_paused` in the in-memory config passed to distribute.py
  4. Promote N backup accounts to active to compensate

OUTPUT:
  - Writes updated workers-effective.yml (transient, consumed by distribute.py)
  - Writes quota_report.json (committed to tm-control for audit)

NOTE: 404 on status.json is EXPECTED for fresh worker repos (haven't run yet).
Treated as "0 minutes used, healthy". Only real values trigger pause.

EXIT CODES:
  0 = success, possibly with paused/promoted accounts
  1 = config file missing
  2 = all active accounts would be paused (catastrophic, needs manual intervention)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed", file=sys.stderr)
    sys.exit(5)


STATUS_URL_TEMPLATE = "https://raw.githubusercontent.com/{owner}/{repo}/main/status.json"
TIMEOUT_SECONDS = 10


def fetch_status(owner: str, repo: str) -> Optional[Dict[str, Any]]:
    """Returns status dict, or None if 404 / unreachable / malformed."""
    url = STATUS_URL_TEMPLATE.format(owner=owner, repo=repo)
    req = urllib.request.Request(url, headers={"User-Agent": "tm-control-quota-check/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            data = resp.read().decode("utf-8")
        return json.loads(data)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None  # expected for fresh repos
        print(f"  WARN: {owner}/{repo}: HTTP {e.code}", file=sys.stderr)
        return None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"  WARN: {owner}/{repo}: {type(e).__name__}: {e}", file=sys.stderr)
        return None


def check_account_quota(
    account: Dict[str, Any],
    limit_minutes: int,
    pause_threshold: float,
) -> Dict[str, Any]:
    """Returns a quota-state record for this account."""
    owner = account["github_user"]
    repo = account["repo_name"]
    status = fetch_status(owner, repo)

    record = {
        "id":                account["id"],
        "github_user":       owner,
        "repo_name":         repo,
        "original_status":   account.get("status", "active"),
        "minutes_used":      0,
        "usage_pct":         0.0,
        "status_reachable":  status is not None,
        "quota_paused":      False,
        "checked_at":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    if status is None:
        # Fresh repo or unreachable — treat as 0 used, healthy
        return record

    used = status.get("minutes_used_this_month", 0)
    try:
        used = int(used)
    except (TypeError, ValueError):
        used = 0

    record["minutes_used"] = used
    record["usage_pct"] = round(used / limit_minutes, 4) if limit_minutes > 0 else 0.0

    # Only active accounts can be quota_paused (backups are already out of rotation)
    if account.get("status") == "active" and record["usage_pct"] >= pause_threshold:
        record["quota_paused"] = True

    return record


def apply_promotions(
    accounts: List[Dict[str, Any]],
    quota_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return mutated account list with quota_paused statuses applied and
    backup accounts promoted to fill the gap.
    
    Backup promotion order: lowest id first (sportinerd-byte id:9 before arifin971 id:10).
    """
    # Map id → record for fast lookup
    quota_by_id = {r["id"]: r for r in quota_records}

    # Build a list that we'll mutate
    result: List[Dict[str, Any]] = []
    paused_count = 0

    for acc in accounts:
        copy = dict(acc)
        qr = quota_by_id.get(copy["id"])
        if qr and qr["quota_paused"]:
            copy["status"] = "quota_paused"
            paused_count += 1
        result.append(copy)

    # Promote that many backups (lowest id first)
    backups_sorted = sorted(
        [a for a in result if a.get("status") == "backup"],
        key=lambda x: x["id"],
    )
    to_promote = backups_sorted[:paused_count]
    promoted_ids = {a["id"] for a in to_promote}

    for a in result:
        if a["id"] in promoted_ids:
            a["status"] = "active"
            a["_promoted_from_backup"] = True

    # Safety: if after promotion we STILL have zero active, this is catastrophic
    final_active = [a for a in result if a.get("status") == "active"]
    if not final_active:
        print("CATASTROPHIC: no active accounts after quota checks + promotions",
              file=sys.stderr)
        print("All 8 primary workers are quota-paused and 2 backups already promoted.",
              file=sys.stderr)
        print("Manual intervention required: add new accounts or wait for quota reset.",
              file=sys.stderr)
        sys.exit(2)

    return result


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  default="config/workers.yml",
                    help="Input workers.yml (source of truth)")
    ap.add_argument("--output", default="config/workers-effective.yml",
                    help="Output workers-effective.yml (consumed by distribute.py)")
    ap.add_argument("--report", default="quota_report.json",
                    help="Human-readable quota report (committed for audit)")
    args = ap.parse_args()

    cfg_path = Path(args.input)
    if not cfg_path.exists():
        print(f"ERROR: {cfg_path} not found", file=sys.stderr)
        return 1

    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)

    accounts = cfg.get("accounts") or []
    quota_cfg = cfg.get("quota", {}) or {}
    limit_minutes   = int(quota_cfg.get("limit_minutes", 2000))
    pause_threshold = float(quota_cfg.get("pause_threshold", 0.90))

    print(f"Quota check: limit={limit_minutes}min  threshold={pause_threshold*100:.0f}%",
          file=sys.stderr)

    # Check each account
    quota_records: List[Dict[str, Any]] = []
    for acc in accounts:
        rec = check_account_quota(acc, limit_minutes, pause_threshold)
        quota_records.append(rec)
        flag = "PAUSED" if rec["quota_paused"] else f"{rec['usage_pct']*100:.1f}%"
        print(f"  id:{rec['id']:2d} {rec['github_user']:30s} "
              f"{rec['minutes_used']:>4d}min  {flag}",
              file=sys.stderr)

    # Apply promotions
    effective_accounts = apply_promotions(accounts, quota_records)

    # Write effective config
    effective_cfg = dict(cfg)
    effective_cfg["accounts"] = effective_accounts
    with open(args.output, "w") as f:
        yaml.safe_dump(effective_cfg, f, sort_keys=False)

    # Write quota report
    report = {
        "generated_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "limit_minutes": limit_minutes,
        "pause_threshold": pause_threshold,
        "accounts":      quota_records,
        "paused_count":  sum(1 for r in quota_records if r["quota_paused"]),
        "promoted_count": sum(1 for a in effective_accounts if a.get("_promoted_from_backup")),
    }
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    print(f"✓ {args.output} written", file=sys.stderr)
    print(f"  paused:   {report['paused_count']}", file=sys.stderr)
    print(f"  promoted: {report['promoted_count']} (backup → active)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
