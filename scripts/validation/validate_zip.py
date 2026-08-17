"""Oracle on the built package: dist/RecoverLand.zip.

Exit code is the verdict: 0 when every check passes, 1 otherwise. The previous
version printed FORBIDDEN_FOUND / REQUIRED_MISSING and then still fell off the
end of the file with status 0, so a CI step running it could never fail. That is
fixed here: failures are collected and the process exits 1.
"""
import sys
import zipfile
from pathlib import Path

# Path relative to repo root (script is at scripts/validation/validate_zip.py)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
zip_path = REPO_ROOT / "dist" / "RecoverLand.zip"
if not zip_path.exists():
    print("ERROR: ZIP not found")
    sys.exit(1)

failures: list[str] = []

with zipfile.ZipFile(str(zip_path), 'r') as z:
    entries = z.namelist()
    roots = set(p.split('/')[0] for p in entries)
    print(f"TOP_ROOTS: {sorted(roots)}")
    print(f"TOTAL_ENTRIES: {len(entries)}")
    size_bytes = sum(z.getinfo(p).file_size for p in entries)
    print(f"SIZE_BYTES: {size_bytes}")
    print(f"SIZE_KB: {size_bytes // 1024}")
    print("SAMPLE_ENTRIES:", entries[:10])

    # Check for forbidden files
    _FORBIDDEN = ['.git', '.github', 'scripts/', 'tests/', 'build/', 'dist/',
                  '__pycache__', '.pyc', '.log', 'AGENTS.md', 'orchestrator',
                  # The runtime never compiles a catalogue; shipping the
                  # compiler is what allowed classFactory to overwrite a
                  # correct .qm with a dead one, and it carries every bandit
                  # finding of the package. Build input, not runtime asset:
                  # the .ts goes with it.
                  'compile_translations.py', 'recoverland_en.ts']
    forbidden = [p for p in entries if any(x in p for x in _FORBIDDEN)]
    if forbidden:
        print("FORBIDDEN_FOUND:", forbidden[:10])
        failures.append(f"forbidden_entries={forbidden[:10]}")
    else:
        print("FORBIDDEN_CHECK: PASS")

    # Check required files
    required = ['__init__.py', 'metadata.txt', 'recover.py', 'recover_dialog.py',
                'icon.svg', 'LICENSE',
                # The only translation artefact the plugin loads at runtime.
                'i18n/recoverland_en.qm']
    missing = [r for r in required if not any(p.endswith(r) for p in entries)]
    if missing:
        print("REQUIRED_MISSING:", missing)
        failures.append(f"required_missing={missing}")
    else:
        print("REQUIRED_CHECK: PASS")

if failures:
    print("VERDICT: FAIL", " | ".join(failures))
    sys.exit(1)
print("VERDICT: PASS")
sys.exit(0)
