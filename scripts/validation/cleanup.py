"""Cleanup rotation for validation reports.

Keep the 5 most recent reports per scenario AND drop any report older
than 30 days. No effect on `golden_logs/` (versioned reference).
"""
from __future__ import annotations

import argparse
import os
import time
from collections import defaultdict
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPORTS = _HERE / "reports"


def _scenario_id_from_filename(path: Path) -> str:
    """Strip the trailing _YYYYMMDD_HHMMSS suffix to recover scenario id."""
    stem = path.stem
    parts = stem.rsplit("_", 2)
    if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
        return parts[0]
    return stem


def purge_reports(
    reports_dir: Path = _REPORTS,
    keep_per_scenario: int = 5,
    max_age_days: int = 30,
    dry_run: bool = False,
) -> dict:
    """Apply rotation rules. Returns a dict with counters."""
    if not reports_dir.is_dir():
        return {"deleted": 0, "kept": 0, "reason": "no_reports_dir"}

    now = time.time()
    cutoff = now - max_age_days * 86400

    by_scenario: dict[str, list[Path]] = defaultdict(list)
    for path in reports_dir.glob("*.json"):
        by_scenario[_scenario_id_from_filename(path)].append(path)

    deleted = 0
    kept = 0
    for scenario, paths in by_scenario.items():
        paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for i, path in enumerate(paths):
            too_old = path.stat().st_mtime < cutoff
            beyond_keep = i >= keep_per_scenario
            if too_old or beyond_keep:
                if dry_run:
                    print(f"[cleanup] would delete {path}")
                else:
                    try:
                        path.unlink()
                        deleted += 1
                    except OSError as exc:
                        print(f"[cleanup] failed to delete {path}: {exc}")
            else:
                kept += 1
    return {"deleted": deleted, "kept": kept, "scenarios": len(by_scenario)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", type=int, default=5,
                        help="reports to keep per scenario (default 5)")
    parser.add_argument("--max-age-days", type=int, default=30,
                        help="hard age cap (default 30 days)")
    parser.add_argument("--dry-run", action="store_true",
                        help="only print what would be deleted")
    args = parser.parse_args()

    stats = purge_reports(
        keep_per_scenario=args.keep,
        max_age_days=args.max_age_days,
        dry_run=args.dry_run,
    )
    print(f"[cleanup] deleted={stats['deleted']} kept={stats['kept']} "
          f"scenarios={stats.get('scenarios', 0)} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
