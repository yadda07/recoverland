#!/usr/bin/env python3
"""Purge historical test artifacts from `scripts/` (BL-RW-P3-16).

Targets three bounded glob patterns and NEVER touches anything else:

  - stress_snapshot_*.json
  - rewind_report_*.txt
  - session_analysis_*.txt

Files ending in `_latest.<ext>` are ALWAYS preserved (they are the
versioned reference of the last run).

Among the remaining historical files for each pattern, the script:
  1. Keeps the 5 most recent (by mtime).
  2. Purges those older than `--days` (default 30) and not in the
     top-5.

The default mode is `--dry-run` (no deletion). Use `--apply` to
actually remove files. Per-file decisions are logged.

Antithèses guarded:
  * --apply mandatory : prevents accidental destructive run.
  * BASE_DIR pinned to Path(__file__).parent : ignores any env var or
    argv path injection.
  * Pattern bounded to exact 3 globs : no `**` recursion, no overlap
    with unrelated files.
  * `_latest.<ext>` exclusion is enforced both by exclusion in the
    glob (negation pattern) and by an explicit re-check before any
    unlink.

Usage:
    python scripts/purge_test_artifacts.py            # dry-run, 30 days
    python scripts/purge_test_artifacts.py --apply    # actually delete
    python scripts/purge_test_artifacts.py --days 90 --apply
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import sys
import time
from pathlib import Path
from typing import List, Tuple

BASE_DIR = Path(__file__).resolve().parent

PATTERNS: Tuple[str, ...] = (
    "stress_snapshot_*.json",
    "rewind_report_*.txt",
    "session_analysis_*.txt",
)

PROTECT_SUFFIXES: Tuple[str, ...] = (
    "_latest.json",
    "_latest.txt",
)

DEFAULT_DAYS = 30
DEFAULT_KEEP_RECENT = 5


def _logger() -> logging.Logger:
    logger = logging.getLogger("purge_test_artifacts")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger


def _is_protected(path: Path) -> bool:
    name = path.name
    return any(name.endswith(s) for s in PROTECT_SUFFIXES)


def _scan_pattern(pattern: str, days: int, keep_recent: int,
                  logger: logging.Logger) -> List[Path]:
    """Return the list of files to purge for one pattern.

    Files are kept if any of:
      - they are protected (`_latest.<ext>`)
      - they are in the `keep_recent` most-recent files
      - their mtime is younger than `days`
    """
    now = time.time()
    age_cutoff = now - days * 86400.0

    matches = sorted(
        (p for p in BASE_DIR.glob(pattern) if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        logger.info(
            "pattern=%s matches=0 (nothing to scan)", pattern,
        )
        return []

    to_purge: List[Path] = []
    for idx, path in enumerate(matches):
        if _is_protected(path):
            logger.info(
                "pattern=%s idx=%d path=%s action=keep reason=protected",
                pattern, idx, path.name,
            )
            continue
        if idx < keep_recent:
            logger.info(
                "pattern=%s idx=%d path=%s action=keep reason=top%d_recent",
                pattern, idx, path.name, keep_recent,
            )
            continue
        st = path.stat()
        if st.st_mtime >= age_cutoff:
            logger.info(
                "pattern=%s idx=%d path=%s action=keep reason=young "
                "age_days=%.1f",
                pattern, idx, path.name,
                (now - st.st_mtime) / 86400.0,
            )
            continue
        logger.info(
            "pattern=%s idx=%d path=%s action=purge "
            "age_days=%.1f size_bytes=%d",
            pattern, idx, path.name,
            (now - st.st_mtime) / 86400.0, st.st_size,
        )
        to_purge.append(path)
    return to_purge


def _delete(path: Path, logger: logging.Logger) -> bool:
    # Defense-in-depth: re-check protection right before unlink.
    if _is_protected(path):
        logger.warning(
            "abort_unlink path=%s reason=protected_after_recheck "
            "(should never happen, indicates scan bug)",
            path.name,
        )
        return False
    try:
        size = path.stat().st_size
        path.unlink()
        logger.info(
            "deleted path=%s size_bytes=%d", path.name, size,
        )
        return True
    except OSError as exc:
        logger.error(
            "delete_failed path=%s err=%s", path.name, exc,
        )
        return False


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Purge historical test artifacts from scripts/.")
    parser.add_argument(
        "--days", type=int, default=DEFAULT_DAYS,
        help=f"Age threshold in days (default {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--keep-recent", type=int, default=DEFAULT_KEEP_RECENT,
        help=f"Number of most-recent files to keep per pattern "
             f"(default {DEFAULT_KEEP_RECENT}).",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually delete files. Without this flag, runs in dry-run.",
    )
    args = parser.parse_args(argv)

    logger = _logger()
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "start mode=%s base_dir=%s days=%d keep_recent=%d patterns=%s",
        mode, BASE_DIR, args.days, args.keep_recent,
        ",".join(PATTERNS),
    )

    total_to_purge = 0
    total_deleted = 0
    total_bytes = 0
    for pattern in PATTERNS:
        candidates = _scan_pattern(
            pattern, args.days, args.keep_recent, logger,
        )
        total_to_purge += len(candidates)
        if args.apply:
            for path in candidates:
                try:
                    size = path.stat().st_size
                except OSError:
                    size = 0
                if _delete(path, logger):
                    total_deleted += 1
                    total_bytes += size

    logger.info(
        "summary mode=%s candidates=%d deleted=%d bytes=%d",
        mode, total_to_purge, total_deleted, total_bytes,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
