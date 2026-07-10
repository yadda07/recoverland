import zipfile
import os
from pathlib import Path

# Path relative to repo root (script is at scripts/validation/validate_zip.py)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
zip_path = REPO_ROOT / "dist" / "RecoverLand.zip"
if not zip_path.exists():
    print("ERROR: ZIP not found")
    exit(1)

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
    forbidden = [p for p in entries if any(x in p for x in ['.git', '.github', 'scripts/', 'tests/', 'build/', 'dist/', '__pycache__', '.pyc', '.log', 'AGENTS.md', 'orchestrator'])]
    if forbidden:
        print("FORBIDDEN_FOUND:", forbidden[:10])
    else:
        print("FORBIDDEN_CHECK: PASS")
    
    # Check required files
    required = ['__init__.py', 'metadata.txt', 'recover.py', 'recover_dialog.py', 'icon.svg', 'LICENSE', 'i18n/compile_translations.py']
    missing = [r for r in required if not any(p.endswith(r) for p in entries)]
    if missing:
        print("REQUIRED_MISSING:", missing)
    else:
        print("REQUIRED_CHECK: PASS")
