# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Windows long paths (MAX_PATH) for the journal and its companion files.

Win32 calls refuse a plain path of 260 characters or more (measured on
this code base: `sqlite3.connect` answers "unable to open database file",
`os.open` of the `.rlwriter` lock and `os.makedirs` raise WinError 206, and
`os.path.isfile` answers False for a file that exists, so a project saved
in a deep folder was taken for an unsaved one). The extended-length form,
two backslashes, a question mark and a backslash before the absolute path
(`UNC\\` after it for a share), lifts that limit for every Win32 file call
without the LongPathsEnabled registry setting; SQLite, `os` and `shutil`
accept it (measured).

Two spellings, two uses:

* `extended(path)`: always the extended form (Windows). For a directory
  whose deepest file name is not known in advance (the capture outbox).
* `when_needed(path)`: the extended form only for a path long enough to
  put one of its companion files past MAX_PATH; any shorter path comes
  back EXACTLY as given, so the journals of every normal project keep the
  spelling they always had (logs, keys, comparisons).

`plain(path)` undoes the prefix for display and comparison: both forms
name the same file.
"""
import os

EXTENDED_PREFIX = "\\\\?\\"          # the 4 characters  \\?\
_UNC_PREFIX = EXTENDED_PREFIX + "UNC\\"

# A path longer than this gets the extended form. Measured with plain
# paths: SQLite opens a journal of up to 251 characters and refuses 252
# (its -journal / -wal companions need the room), and the '.rlwriter' lock
# of a 250-character journal is the longest that opens (259). Every path
# the journal code derives (lock, pending file, owed carries, their
# temporary files) goes through when_needed itself, so a companion never
# inherits a plain spelling it cannot use. Anything shorter is left as it
# always was: journals of normal projects keep their exact spelling.
LONG_PATH_THRESHOLD = 250


def extended(path: str) -> str:
    """Windows extended-length form of *path* (unchanged elsewhere)."""
    if os.name != "nt":
        return path
    p = os.path.abspath(path)
    if p.startswith(EXTENDED_PREFIX):
        return p
    if p.startswith("\\\\"):       # UNC share \\server\share
        return _UNC_PREFIX + p[2:]
    return EXTENDED_PREFIX + p


def when_needed(path: str) -> str:
    """*path* in the extended form only when it is long enough to need it."""
    if os.name != "nt" or not path:
        return path
    if path.startswith(EXTENDED_PREFIX):
        return path
    if len(os.path.abspath(path)) <= LONG_PATH_THRESHOLD:
        return path
    return extended(path)


def plain(path: str) -> str:
    """*path* without the extended-length prefix (display, comparison)."""
    if not path or os.name != "nt":
        return path
    if path.upper().startswith(_UNC_PREFIX.upper()):
        return "\\\\" + path[len(_UNC_PREFIX):]
    if path.startswith(EXTENDED_PREFIX):
        return path[len(EXTENDED_PREFIX):]
    return path


def path_key(path: str) -> str:
    """One comparison key per file, whatever its spelling."""
    return os.path.normcase(os.path.abspath(plain(path)))
