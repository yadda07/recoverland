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
    5  staged .qm is not a usable catalogue, or Qt is unavailable to prove it
"""
from __future__ import annotations

import argparse
import fnmatch
import glob
import importlib
import os
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
    # Validation/QA helper script moved to scripts/validation/; excluded
    # here as defense-in-depth in case it was ever tracked at root.
    "validate_zip.py",
    # The .ts -> .qm compiler is a DEV tool and must not ship. It stays in the
    # repo as the maintainer's way to regenerate a catalogue -- and only that:
    # measured, no scenario and no other module imports it. Shipping it was what
    # let classFactory recompile at runtime and overwrite a correct .qm with a
    # dead one (339/339 messages resolving before, 18/339 after). It also carries
    # every bandit finding of the package (B404/B405/B603), 3 of 3.
    "i18n/compile_translations.py",
    # The source catalogue is a build input, not a runtime asset: nothing in the
    # plugin reads a .ts. It was also the second half of the removed branch's
    # trigger, which fired when a .ts sat next to a missing .qm.
    "i18n/recoverland_en.ts",
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
    # The compiled English catalogue is the ONLY translation artefact the plugin
    # loads at runtime, and it is never regenerated on the user's machine.
    "i18n/recoverland_en.qm",
)

# Sample of (context, source, expected_english) taken verbatim from
# scripts/validation/scenarios/i18n_runtime_qgis.py, which proves these pairs
# against a live QGIS. Every entry has expected != source, so a catalogue that
# fails to resolve cannot pass by accident: QTranslator returns the source
# string on a miss, and the old dead .qm scored exactly 0 here. All four
# translated contexts are represented.
QM_SAMPLE = (
    ("ReviewStatusWidget", "Desactiver Review", "Disable Review"),
    ("ReviewStatusWidget", "Inactif", "Inactive"),
    ("ReviewStatusWidget", "Review — Recherche des modifications",
     "Review — Searching for modifications"),
    ("ReviewSegmentedSwitch", "Présent", "Present"),
    ("AppleToggleSwitch", "Enregistrement actif", "Recording active"),
    ("CanvasDateBar", "Aujourd'hui", "Today"),
    ("CanvasDateBar", "Exporter le snapshot vers GeoPackage",
     "Export snapshot to GeoPackage"),
)

QM_STAGE_REL = "i18n/recoverland_en.qm"

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


def _import_qtcore():
    """Return a Qt QtCore module, or None if this interpreter has no Qt."""
    for name in ("qgis.PyQt.QtCore", "PyQt6.QtCore", "PyQt5.QtCore"):
        try:
            return importlib.import_module(name)
        except Exception:
            continue
    return None


def check_qm_content(qm_path: Path) -> tuple[bool, str]:
    """Load ``qm_path`` in a QTranslator and resolve a sample of known strings.

    The check is on CONTENT only: never on a timestamp, never on mere presence.
    A file can exist, load, and report isEmpty() False while translating
    nothing at all -- that is precisely the dead catalogue the pure-Python
    fallback used to write -- so the verdict is the sample resolution.

    Returns ``(ok, detail)``. Requires Qt in THIS interpreter; callers handle
    the Qt-less case.
    """
    qtcore = _import_qtcore()
    if qtcore is None:
        return False, "qt_unavailable"
    if not qm_path.is_file():
        return False, f"missing:{qm_path}"

    # QCoreApplication.translate() is the lookup the plugin's self.tr() actually
    # goes through, and unlike QTranslator.translate() it handles non-ASCII
    # sources on this PyQt build. It needs a live application object.
    app = qtcore.QCoreApplication.instance() or qtcore.QCoreApplication([])
    translator = qtcore.QTranslator()
    if not translator.load(str(qm_path)):
        return False, "load_failed"
    if translator.isEmpty():
        return False, "catalogue_empty"

    app.installTranslator(translator)
    try:
        failures = []
        for context, source, expected in QM_SAMPLE:
            got = qtcore.QCoreApplication.translate(context, source)
            if got != expected or got == source:
                failures.append(f"{context}/{ascii(source)}->{ascii(got)}")
    finally:
        app.removeTranslator(translator)

    n = len(QM_SAMPLE)
    if failures:
        return False, f"resolved={n - len(failures)}/{n} failed={';'.join(failures[:3])}"
    return True, f"resolved={n}/{n}"


def _qt_capable_interpreter() -> str | None:
    """Find an interpreter that can import Qt, for the .qm content check.

    Deliberate design: this script is normally run by a bare `python` that has
    no Qt at all (measured: C:\\Python314 imports none of qgis.PyQt, PyQt6,
    PyQt5). A check that quietly disables itself there would be the same class
    of lie we are removing from classFactory, so the build instead borrows a
    Qt-capable interpreter, and FAILS (exit 5) when it cannot find one. Set
    RECOVERLAND_QT_PYTHON to point at one explicitly.

    Unrelated to lrelease: nothing here compiles anything, so the absence of
    lrelease -- which is not shipped with QGIS 4.0.3 or OSGeo4W -- can never
    weaken or skip the check.
    """
    explicit = os.environ.get("RECOVERLAND_QT_PYTHON")
    if explicit and Path(explicit).exists():
        return explicit
    found = shutil.which("python-qgis.bat") or shutil.which("python-qgis")
    if found:
        return found
    # Linux: a distro QGIS puts qgis.PyQt in the system python3, which a venv
    # building the release would not see. If that python3 has no Qt either, the
    # child reports qt_unavailable and the build still fails loudly.
    if not sys.platform.startswith("win"):
        system_python = shutil.which("python3")
        if system_python and Path(system_python).resolve() != Path(sys.executable).resolve():
            return system_python
    for pattern in (
        "C:/Program Files/QGIS */bin/python-qgis.bat",
        "C:/OSGeo4W/bin/python-qgis.bat",
    ):
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            return matches[0]
    return None


def verify_qm(stage_root: Path) -> None:
    """Abort the build unless the STAGED .qm is a usable catalogue."""
    qm_path = stage_root / QM_STAGE_REL

    if _import_qtcore() is not None:
        ok, detail = check_qm_content(qm_path)
        log(
            "INFO" if ok else "CRITICAL",
            "qm_content_checked" if ok else "qm_content_rejected",
            path=str(qm_path), mode="in_process", detail=detail,
        )
        if not ok:
            raise SystemExit(5)
        return

    interpreter = _qt_capable_interpreter()
    if interpreter is None:
        log(
            "CRITICAL",
            "qm_content_unverifiable",
            reason="no Qt in this interpreter and no Qt-capable interpreter found",
            hint="set RECOVERLAND_QT_PYTHON to a python that can import PyQt",
        )
        raise SystemExit(5)

    result = subprocess.run(  # nosec B603 - fixed argv, interpreter path from env/known locations
        [interpreter, str(Path(__file__).resolve()), "--check-qm", str(qm_path)],
        capture_output=True,
        text=True,
        timeout=300,
    )
    tail = (result.stdout or "").strip().splitlines()
    if result.returncode == 0:
        event = "qm_content_checked"
    elif "qt_unavailable" in (result.stdout or ""):
        # The borrowed interpreter had no Qt either: nothing was proven about the
        # catalogue. Report that, do not dress it up as a verdict on the file.
        event = "qm_content_unverifiable"
    else:
        event = "qm_content_rejected"
    log(
        "INFO" if result.returncode == 0 else "CRITICAL",
        event,
        path=str(qm_path), mode="subprocess", interpreter=interpreter,
        returncode=result.returncode,
        detail=(tail[-1] if tail else (result.stderr or "").strip()[-300:]),
    )
    if result.returncode != 0:
        raise SystemExit(5)


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
    # Content gate BEFORE the ZIP is written: a rejected catalogue must leave no
    # artefact behind that anyone could mistake for a release.
    verify_qm(stage_root)

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


def check_qm_cli(qm_path: str) -> int:
    """`--check-qm <path>` entry point, run in a Qt-capable interpreter."""
    ok, detail = check_qm_content(Path(qm_path))
    if ok:
        event = "qm_content_checked"
    elif detail == "qt_unavailable":
        # Distinct from a rejected catalogue: we learned nothing about the file.
        # Still a build failure -- an unverifiable guarantee is not a guarantee.
        event = "qm_content_unverifiable"
    else:
        event = "qm_content_rejected"
    log("INFO" if ok else "CRITICAL", event, path=qm_path, mode="child", detail=detail)
    return 0 if ok else 5


if __name__ == "__main__":
    # Hidden sub-invocation used by verify_qm() when the building interpreter has
    # no Qt. Kept out of argparse so the normal CLI stays unchanged.
    if len(sys.argv) == 3 and sys.argv[1] == "--check-qm":
        sys.exit(check_qm_cli(sys.argv[2]))
    sys.exit(main())
