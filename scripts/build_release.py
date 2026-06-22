"""Build a release ZIP for plugins.qgis.org submission.

Produces ``dist/RecoverLand.zip`` whose single top-level directory is
``RecoverLand/`` (exact case matches the slug
``https://plugins.qgis.org/plugins/RecoverLand/``). The version is read
from ``metadata.txt`` and reported in the logs only; the ZIP file name
itself is stable across releases.

The release content is derived from ``git ls-files`` (so anything
gitignored never leaks) plus a defensive blacklist (so anything
accidentally tracked but internal is still rejected).

Usage::

    python scripts/build_release.py
    python scripts/build_release.py --clean      # wipe build/ and dist/ first
    python scripts/build_release.py --verbose    # log every file copied/skipped

Exit codes::

    0  success
    2  metadata.txt missing or unparsable
    3  required runtime file missing in stage (sanity check)
    4  git not available or repo error
"""
from __future__ import annotations

import argparse
import fnmatch
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

# Canonical name on plugins.qgis.org (URL slug). The ZIP top-level
# directory MUST match this exactly or the upload is rejected.
PLUGIN_NAME = "RecoverLand"

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = PLUGIN_ROOT / "build"
DIST_DIR = PLUGIN_ROOT / "dist"

# Directories whose presence anywhere in the path forbids inclusion.
# Defense-in-depth on top of git ls-files (which already filters these).
EXCLUDE_DIR_PARTS = frozenset({
    ".git", ".github", ".windsurf",
    "scripts", "tests",
    "build", "dist",
    "__pycache__", ".pytest_cache",
})

# Exact relative paths (POSIX) to drop unconditionally.
EXCLUDE_FILES = frozenset({
    ".flake8",
    ".gitignore",
    "conftest.py",
    "COMMIT_MSG",
    "COMMIT_EDITMSG",
    # Internal team charter (gates, orchestrator, profiles): dev-only,
    # not part of the user-facing plugin. Root-level .md, so not caught
    # by the docs/*.md glob below.
    "AGENTS.md",
})

# Glob patterns matched against the full relative POSIX path.
EXCLUDE_GLOBS = (
    "*.pyc",
    "*.pyo",
    "*.log",
    "flake8_report.*",
    # All .md inside docs/ are internal working documents; only HTML/CSS/JS
    # assets are user-facing documentation.
    "docs/*.md",
    "docs/orchestrator*.json",
)

# Dotfile basenames explicitly allowed despite the global "no hidden file"
# rule below. Anything else starting with "." is rejected to avoid leaking
# editor / tooling configs (.flake8, .editorconfig, .envrc, .DS_Store, ...).
# Empty by design: even .nojekyll is filtered out because it only matters
# to GitHub Pages on the repo, never to the QGIS plugin runtime; some
# external scanners flag every hidden file as "suspicious".
ALLOWED_DOTFILES: frozenset = frozenset()

# Files that MUST exist in the stage after copy; abort if any is missing.
REQUIRED_STAGE_FILES = (
    "__init__.py",
    "metadata.txt",
    "recover.py",
    "recover_dialog.py",
    "icon.svg",
    "LICENSE",
    # Imported lazily by __init__.py:22 to compile .ts -> .qm on first run.
    "i18n/compile_translations.py",
)

VERSION_RE = re.compile(r"^version\s*=\s*(\S+)\s*$", re.MULTILINE)


def log(level: str, event: str, **fields: object) -> None:
    """Structured key=value log line."""
    parts = [f"level={level}", "module=build_release", f"event={event}"]
    for key, value in fields.items():
        text = str(value)
        if any(ch.isspace() for ch in text) or "=" in text:
            text = '"' + text.replace('"', r"\"") + '"'
        parts.append(f"{key}={text}")
    print(" ".join(parts))


def read_version(metadata_path: Path) -> str:
    """Extract the ``version=`` line from metadata.txt."""
    content = metadata_path.read_text(encoding="utf-8")
    match = VERSION_RE.search(content)
    if not match:
        log("CRITICAL", "version_not_found", path=str(metadata_path))
        raise SystemExit(2)
    return match.group(1).strip()


def list_tracked_files(repo: Path) -> list[str]:
    """Return git-tracked files relative to ``repo``, with POSIX separators."""
    try:
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=str(repo),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        log("CRITICAL", "git_not_found")
        raise SystemExit(4)
    except subprocess.TimeoutExpired:
        log("CRITICAL", "git_ls_files_timeout")
        raise SystemExit(4)
    except subprocess.CalledProcessError as exc:
        log(
            "CRITICAL",
            "git_ls_files_failed",
            returncode=exc.returncode,
            stderr=(exc.stderr or "").strip(),
        )
        raise SystemExit(4)
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def is_excluded(rel_posix: str) -> bool:
    """Return True if the file must NOT ship in the release."""
    parts = rel_posix.split("/")
    if any(part in EXCLUDE_DIR_PARTS for part in parts):
        return True
    if rel_posix in EXCLUDE_FILES:
        return True
    # Defense-in-depth: reject any dotfile (basename starting with ".")
    # unless explicitly whitelisted. Catches .flake8, .editorconfig,
    # .envrc, .DS_Store, etc. without needing to enumerate them.
    basename = parts[-1]
    if basename.startswith(".") and basename not in ALLOWED_DOTFILES:
        return True
    return any(fnmatch.fnmatchcase(rel_posix, pat) for pat in EXCLUDE_GLOBS)


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def copy_tracked_files(
    tracked: list[str],
    stage_root: Path,
    verbose: bool,
) -> tuple[int, int, list[str]]:
    """Copy non-excluded tracked files into ``stage_root``.

    Returns ``(n_copied, n_skipped, skipped_sample)``.
    """
    copied = 0
    skipped = 0
    skipped_sample: list[str] = []

    for rel in tracked:
        rel_posix = rel.replace("\\", "/")
        if is_excluded(rel_posix):
            skipped += 1
            if len(skipped_sample) < 8:
                skipped_sample.append(rel_posix)
            if verbose:
                log("DEBUG", "file_skipped", path=rel_posix)
            continue

        src = PLUGIN_ROOT / rel_posix
        if not src.is_file():
            log("WARNING", "tracked_file_missing_on_disk", path=rel_posix)
            continue

        dst = stage_root / rel_posix
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1
        if verbose:
            log("DEBUG", "file_copied", path=rel_posix)

    return copied, skipped, skipped_sample


def verify_required(stage_root: Path) -> None:
    """Abort if a required runtime file is missing in the stage."""
    missing = [
        name for name in REQUIRED_STAGE_FILES
        if not (stage_root / name).is_file()
    ]
    if missing:
        log("CRITICAL", "required_files_missing", missing=",".join(missing))
        raise SystemExit(3)


def build_zip(stage_root: Path, zip_path: Path) -> tuple[int, int]:
    """Zip ``stage_root`` into ``zip_path``. Returns ``(n_entries, size_bytes)``."""
    if zip_path.exists():
        zip_path.unlink()
    n_entries = 0
    with zipfile.ZipFile(
        zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9,
    ) as zf:
        for path in sorted(stage_root.rglob("*")):
            if not path.is_file():
                continue
            # Archive name = relative to BUILD_DIR so RecoverLand/ is the
            # single top-level directory in the ZIP.
            arc = path.relative_to(BUILD_DIR).as_posix()
            zf.write(path, arc)
            n_entries += 1
    return n_entries, zip_path.stat().st_size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a plugins.qgis.org release ZIP for RecoverLand.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="wipe build/ and dist/ before building",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="log every copied/skipped file at DEBUG level",
    )
    args = parser.parse_args(argv)

    t0 = time.time()

    metadata = PLUGIN_ROOT / "metadata.txt"
    if not metadata.is_file():
        log("CRITICAL", "metadata_missing", path=str(metadata))
        return 2

    version = read_version(metadata)
    log(
        "INFO",
        "release_start",
        plugin=PLUGIN_NAME,
        version=version,
        root=str(PLUGIN_ROOT),
    )

    if args.clean:
        for directory in (BUILD_DIR, DIST_DIR):
            if directory.exists():
                shutil.rmtree(directory)
                log("INFO", "dir_cleaned", path=str(directory))

    stage_root = BUILD_DIR / PLUGIN_NAME
    reset_dir(stage_root)
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    tracked = list_tracked_files(PLUGIN_ROOT)
    log("INFO", "tracked_files_listed", n=len(tracked))

    copied, skipped, sample = copy_tracked_files(
        tracked, stage_root, args.verbose,
    )
    log(
        "INFO",
        "stage_complete",
        copied=copied,
        skipped=skipped,
        skipped_sample=",".join(sample) or "none",
    )

    verify_required(stage_root)

    zip_path = DIST_DIR / f"{PLUGIN_NAME}.zip"
    n_entries, size_bytes = build_zip(stage_root, zip_path)

    elapsed_ms = int((time.time() - t0) * 1000)
    log(
        "INFO",
        "release_done",
        plugin=PLUGIN_NAME,
        version=version,
        zip=str(zip_path),
        n_entries=n_entries,
        size_kb=size_bytes // 1024,
        elapsed_ms=elapsed_ms,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
