"""Parser for RecoverLand debug log file.

Reads `recoverland_debug.log` line-by-line and yields structured records.

Each line in the log follows the formatter:
    %(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] %(message)s

Example:
    2026-05-14T07:42:13.421 [INFO   ] [MainThread     ] BUF_DEL eid=12 fp=fid:42 status=APPLIED

This parser is provider-agnostic and has no QGIS dependency, so it can
be reused from CI or external tooling.

The log ROTATES (5 MB, 5 backups): a byte offset into it only means
something as long as the file does not turn over. `log_anchor` /
`read_window` read "everything written since" across rotations, by file
identity rather than by name. Every file is opened so that the plugin can
still rename or delete it while it is being read (`_open_binary`): a
reader must never be the reason a rotation fails.
"""
from __future__ import annotations

import io
import re
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import FrozenSet, Iterator, List, Optional, Tuple


_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})\s+"
    r"\[(?P<level>[A-Z]+)\s*\]\s+"
    r"\[(?P<thread>[^\]]+)\]\s+"
    r"(?P<message>.*)$"
)

# Extracts key=value pairs, supporting bare tokens, single-quoted and
# double-quoted values.  Used opportunistically: a message can contain
# both free text and structured fields.
_KV_RE = re.compile(
    r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)="
    r"(?P<value>\"(?:[^\"\\]|\\.)*\"|'[^']*'|[^\s]+)"
)


@dataclass
class LogRecord:
    """One parsed log line."""
    ts: datetime
    level: str
    thread: str
    message: str
    fields: dict = field(default_factory=dict)
    raw: str = ""

    @property
    def event(self) -> str:
        """First token of the message, used as event identifier."""
        return self.message.split(maxsplit=1)[0] if self.message else ""


def _strip_quotes(value: str) -> str:
    """Decode a key=value value: JSON string, legacy quotes, or bare token."""
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        import json
        try:
            decoded = json.loads(value)
            if isinstance(decoded, str):
                return decoded
        except ValueError:
            pass   # legacy line written before the JSON-string format
        return value[1:-1]
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1]
    return value


def _extract_fields(message: str) -> dict:
    out = {}
    for match in _KV_RE.finditer(message):
        out[match.group("key")] = _strip_quotes(match.group("value"))
    return out


def parse_line(line: str) -> Optional[LogRecord]:
    """Parse a single log line. Returns None if the format does not match."""
    line = line.rstrip("\r\n")
    if not line:
        return None
    match = _LINE_RE.match(line)
    if not match:
        return None
    ts = datetime.strptime(match.group("ts"), "%Y-%m-%dT%H:%M:%S.%f")
    return LogRecord(
        ts=ts,
        level=match.group("level").strip(),
        thread=match.group("thread").strip(),
        message=match.group("message"),
        fields=_extract_fields(match.group("message")),
        raw=line,
    )


# --------------------------------------------------------------------- #
# Reading a log file without holding up its rotation
# --------------------------------------------------------------------- #

_GENERIC_READ = 0x80000000
_FILE_SHARE_ALL = 0x7                   # READ | WRITE | DELETE
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_NORMAL = 0x80
_win32 = None


def _win32_api():
    """``(CreateFileW, CloseHandle, INVALID_HANDLE_VALUE)``, typed once.

    On a private WinDLL instance: the prototypes of the shared
    ctypes.windll.kernel32 are left as other code set them.
    """
    global _win32
    if _win32 is None:
        import ctypes
        from ctypes import wintypes

        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create = k32.CreateFileW
        create.restype = wintypes.HANDLE
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                           ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                           wintypes.HANDLE]
        close = k32.CloseHandle
        close.restype = wintypes.BOOL
        close.argtypes = [wintypes.HANDLE]
        _win32 = (create, close, wintypes.HANDLE(-1).value)
    return _win32


def _open_binary(path):
    """Open a log file for reading WITHOUT ever blocking its rotation.

    On Windows, open() shares a file for reading and writing but not for
    deletion: while it is open, nobody can rename or delete it. The
    plugin's handler renames when the log rotates (live -> .rolling,
    .1 -> .2, ..., .rolling -> .1). A shift refused because this module
    was reading a backup at that instant left the generation that had just
    rotated in ``.rolling``, which the NEXT rotation deletes: 5 MB of
    forensic log gone, without a line to say so. Holding the live file
    made the rotation itself fail and be postponed, with a "rollover
    postponed" line blaming a sibling QGIS for what a reader did.

    Opened with FILE_SHARE_DELETE, the file can be renamed or deleted
    under the reader, which keeps reading the same file through its
    handle (`read_window` takes identities from that handle, and starts
    over when the chain moved meanwhile). POSIX never blocks a rename on
    an open file: plain open() there.
    """
    if os.name != "nt":
        return open(path, "rb")
    import ctypes
    import msvcrt

    create, close, invalid = _win32_api()
    handle = create(str(path), _GENERIC_READ, _FILE_SHARE_ALL, None,
                    _OPEN_EXISTING, _FILE_ATTRIBUTE_NORMAL, None)
    if handle is None or handle == invalid:
        code = ctypes.get_last_error()
        raise OSError(None, ctypes.FormatError(code).strip(), str(path), code)
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except OSError:
        close(handle)
        raise
    try:
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise


def iter_records(
    log_path: Path | str,
    start_offset: int = 0,
    since: Optional[datetime] = None,
) -> Iterator[LogRecord]:
    """Yield parsed records from log file.

    The file stays open while the caller iterates; it is opened so that
    the log can still rotate meanwhile (`_open_binary`), and the records
    then keep coming from the file that was opened, to its end.

    Args:
        log_path: absolute path to the log file.
        start_offset: byte offset to seek to before reading (used by
            runner to skip lines emitted before scenario start).
        since: if provided, skip records with ts < since.
    """
    path = Path(log_path)
    if not path.exists():
        return
    raw = _open_binary(path)
    try:
        if start_offset:
            raw.seek(start_offset)
        # Universal newlines, as the text-mode open() used before.
        fh = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
    except BaseException:
        raw.close()
        raise
    with fh:
        for line in fh:
            record = parse_line(line)
            if record is None:
                continue
            if since is not None and record.ts < since:
                continue
            yield record


def read_records(
    log_path: Path | str,
    start_offset: int = 0,
    since: Optional[datetime] = None,
) -> List[LogRecord]:
    """Eager version of iter_records. Use for small log windows only."""
    return list(iter_records(log_path, start_offset=start_offset, since=since))


def log_file_size(log_path: Path | str) -> int:
    """Return current size of the log file, or 0 if missing."""
    try:
        return os.path.getsize(log_path)
    except OSError:
        return 0


# --------------------------------------------------------------------- #
# A window that survives rotation
# --------------------------------------------------------------------- #

# Bytes kept from just before the anchor: the file the window started in
# is recognised by its content as well as by its identity, so a recycled
# inode (POSIX reuses them) never passes for it.
_ANCHOR_TAIL = 256
# Highest backup suffix looked for (the plugin keeps 5; gaps are allowed).
_MAX_BACKUP = 99
_LINE_BREAK_RE = re.compile(r"\r\n|\r|\n")

FileIdent = Tuple[int, int]


@dataclass(frozen=True)
class LogAnchor:
    """Where a log window starts: a position in ONE file, known by identity.

    ``offset`` alone is what the runner used to keep. After a rotation the
    live file is a new, shorter one and that offset points past its end:
    reading from it gave zero records, and every log assertion of the
    scenario failed with n=0 on code that behaved perfectly.
    """
    path: str
    offset: int
    ident: Optional[FileIdent]          # None: no file there at the anchor
    tail: bytes                         # the bytes just before ``offset``
    known: FrozenSet[FileIdent]         # every file of the chain at the anchor


@dataclass
class LogWindow:
    """What `read_window` found: the records and how far it could follow."""
    records: List[LogRecord]
    files: List[str]                    # read, oldest first
    bytes_read: int
    rotations: int                      # files opened after the anchor file
    complete: bool
    note: str = ""


def _ident(st) -> Optional[FileIdent]:
    return (st.st_dev, st.st_ino) if st.st_ino else None


def _chain_paths(path: str) -> List[str]:
    """The rotation chain, NEWEST first: live file, move-aside, .1, .2, ...

    ``.rolling`` is the name the plugin's handler moves the live file to
    before shifting the backups; it only outlives a rollover whose shift
    failed, and is then newer than ``.1``.
    """
    out = [path, path + ".rolling"]
    out += [f"{path}.{i}" for i in range(1, _MAX_BACKUP + 1)]
    return [p for p in out if os.path.isfile(p)]


def _chain_idents(path: str) -> dict:
    """{path: identity or None} of the chain, newest first.

    A file that vanishes meanwhile is skipped. The identity is None on a
    filesystem that has none (st_ino 0).
    """
    found = {}
    for p in _chain_paths(path):
        try:
            found[p] = _ident(os.stat(p))
        except OSError:
            continue
    return found


def log_anchor(log_path: Path | str) -> LogAnchor:
    """Mark "now" in the log, for a later `read_window`."""
    path = str(log_path)
    known = frozenset(i for i in _chain_idents(path).values() if i is not None)
    try:
        with _open_binary(path) as fh:
            st = os.fstat(fh.fileno())
            size = st.st_size
            start = max(0, size - _ANCHOR_TAIL)
            fh.seek(start)
            tail = fh.read(size - start)
    except OSError:
        return LogAnchor(path, 0, None, b"", known)
    return LogAnchor(path, size, _ident(st), tail, known)


def _read_from(path: str, offset: int, anchor_tail: Optional[bytes] = None):
    """``(identity, text, size)`` of ``path`` from ``offset``, or None.

    With ``anchor_tail``, None unless the bytes just before ``offset``
    are exactly those seen at the anchor: this is the anchor file, still
    holding what it held then.
    """
    try:
        with _open_binary(path) as fh:
            st = os.fstat(fh.fileno())
            if anchor_tail is not None:
                if st.st_size < offset:
                    return None
                fh.seek(offset - len(anchor_tail))
                if fh.read(len(anchor_tail)) != anchor_tail:
                    return None
            fh.seek(offset)
            data = fh.read()
    except OSError:
        return None
    return (_ident(st), data.decode("utf-8", errors="replace"), len(data))


def _parse_text(text: str) -> List[LogRecord]:
    # The universal newlines of a text-mode read (what iter_records sees).
    out = []
    for line in _LINE_BREAK_RE.split(text):
        record = parse_line(line)
        if record is not None:
            out.append(record)
    return out


def _read_window_once(anchor: LogAnchor) -> Optional[LogWindow]:
    """One attempt; None when the chain moved while it was being read."""
    before = _chain_idents(anchor.path)
    chain = list(before)                        # newest first
    start_at = None
    head = None
    if anchor.ident is not None:
        for i, p in enumerate(chain):
            if before[p] != anchor.ident:
                continue
            head = _read_from(p, anchor.offset, anchor.tail)
            if head is not None and head[0] == anchor.ident:
                start_at = i
            break
    elif anchor.offset == 0:
        # No file at the anchor: everything created since is the window.
        pass
    else:
        # A filesystem without file identity: recognise the file by the
        # bytes it held just before the anchor.
        for i, p in enumerate(chain):
            head = _read_from(p, anchor.offset, anchor.tail)
            if head is not None:
                start_at = i
                break

    parts = []                                  # (path, text, bytes), oldest first
    complete = True
    note = ""
    if start_at is not None:
        parts.append((chain[start_at], head[1], head[2]))
        newer = chain[:start_at]
    else:
        # Every file of the chain that did not exist at the anchor was
        # opened since: all of it belongs to the window. Without file
        # identities that cannot be told, and only the live file is read.
        if any(before[p] is not None for p in chain):
            newer = [p for p in chain
                     if before[p] is not None and before[p] not in anchor.known]
        else:
            newer = chain[:1]
        if anchor.ident is not None or anchor.offset:
            complete = False
            note = (f"the file this window started in ({anchor.path} at "
                    f"byte {anchor.offset}) is no longer in the rotation "
                    f"chain: rotated past the kept backups, deleted or "
                    f"rewritten; only the {len(newer)} file(s) opened since "
                    f"are read, and what was written before them is lost")
    for p in reversed(newer):
        got = _read_from(p, 0)
        if got is None or got[0] != before[p]:
            return None
        parts.append((p, got[1], got[2]))
    if _chain_idents(anchor.path) != before:
        return None
    records = []
    for _p, text, _n in parts:
        records.extend(_parse_text(text))
    return LogWindow(records=records, files=[p for p, _t, _n in parts],
                     bytes_read=sum(n for _p, _t, n in parts),
                     rotations=len(newer), complete=complete, note=note)


def read_window(anchor: LogAnchor, attempts: int = 3) -> LogWindow:
    """Every record written to the log since ``anchor``, across rotations.

    The file the window started in is found again by identity (device and
    inode, which a rename keeps) and by the bytes it held just before the
    anchor; it is read from the anchor offset, then every file opened
    after it, oldest first. When that file is gone (more rotations than
    backups kept, or the log deleted), the files created since are still
    read and the window says it is incomplete (``complete=False`` and a
    ``note``) instead of passing for whole. A rotation that happens while
    the chain is being read starts the read over.
    """
    for _ in range(max(1, attempts)):
        window = _read_window_once(anchor)
        if window is not None:
            return window
        time.sleep(0.05)
    return LogWindow(records=[], files=[], bytes_read=0, rotations=0,
                     complete=False,
                     note=f"the rotation chain of {anchor.path} kept moving "
                          f"during {max(1, attempts)} read attempts")


__all__ = [
    "LogRecord",
    "parse_line",
    "iter_records",
    "read_records",
    "log_file_size",
    "LogAnchor",
    "LogWindow",
    "log_anchor",
    "read_window",
]
