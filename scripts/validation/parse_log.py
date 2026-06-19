"""Parser for RecoverLand debug log file.

Reads `recoverland_debug.log` line-by-line and yields structured records.

Each line in the log follows the formatter:
    %(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] %(message)s

Example:
    2026-05-14T07:42:13.421 [INFO   ] [MainThread     ] BUF_DEL eid=12 fp=fid:42 status=APPLIED

This parser is provider-agnostic and has no QGIS dependency, so it can
be reused from CI or external tooling.
"""
from __future__ import annotations

import re
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional


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
    r"(?P<value>'[^']*'|\"[^\"]*\"|[^\s]+)"
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
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
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


def iter_records(
    log_path: Path | str,
    start_offset: int = 0,
    since: Optional[datetime] = None,
) -> Iterator[LogRecord]:
    """Yield parsed records from log file.

    Args:
        log_path: absolute path to the log file.
        start_offset: byte offset to seek to before reading (used by
            runner to skip lines emitted before scenario start).
        since: if provided, skip records with ts < since.
    """
    path = Path(log_path)
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        if start_offset:
            fh.seek(start_offset)
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


__all__ = [
    "LogRecord",
    "parse_line",
    "iter_records",
    "read_records",
    "log_file_size",
]
