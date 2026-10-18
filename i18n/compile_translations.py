""".ts -> .qm compiler for RecoverLand.

Prefers the Qt lrelease tool when available (produces a standard .qm
that QTranslator reads reliably). Falls back to a pure-Python compiler
for environments without lrelease.

Usage (QGIS Python console or standalone):
    from recoverland.i18n.compile_translations import compile_ts_files
    compile_ts_files()
"""
# Required: this module annotates with PEP 604 unions (`str | None`), which are
# evaluated at def time. QGIS 3.40 is packaged against Python 3.9 on several
# Linux distributions, where that raises TypeError at import. The future import
# defers annotation evaluation to strings, so the module loads on 3.9 too.
from __future__ import annotations

import os
import shutil
import struct
import subprocess
import xml.etree.ElementTree as ET

try:
    import defusedxml.ElementTree as _safe_ET
    _HAS_DEFUSEDXML = True
except ImportError:
    _HAS_DEFUSEDXML = False

_MAX_TS_FILE_SIZE = 50 * 1024 * 1024  # 50 MB hard limit


def _safe_parse(path: str):
    """Parse XML with XXE and entity expansion protections."""
    size = os.path.getsize(path)
    if size > _MAX_TS_FILE_SIZE:
        raise ValueError(
            f"TS file too large ({size} bytes, max {_MAX_TS_FILE_SIZE})"
        )
    if _HAS_DEFUSEDXML:
        return _safe_ET.parse(path)
    parser = ET.XMLParser()  # nosec B314 - fallback with entity expansion disabled
    parser.entity.clear()  # disable custom entity expansion (XXE defense)
    return ET.parse(path, parser=parser)  # nosec B320 - size-limited, entities stripped


_QM_MAGIC = (
    b"\x3c\xb8\x64\x18\xca\xef\x9c\x95"
    b"\xcd\x21\x1c\xbf\x60\xa1\xbd\xdd"
)

# Message tags, as QTranslator reads them (qtranslator.cpp, getMessage).
# Its reader knows only End, Translation, Obsolete1, SourceText, Context and
# Comment: a message carrying any other tag (the Qt 3 SourceText16 = 2 and
# Context16 = 4 this compiler used to write) is refused, so every lookup
# missed while the file still loaded and reported isEmpty() False.
_TAG_END = 1
_TAG_TRANSLATION = 3
_TAG_SOURCE_TEXT = 6
_TAG_CONTEXT = 7
_TAG_COMMENT = 8

_BLOCK_HASHES = 0x42
_BLOCK_MESSAGES = 0x69
_BLOCK_LANGUAGE = 0xA7


def _elf_hash(ba: bytes) -> int:
    """Qt elfHash used by QTranslator for message lookup."""
    h = 0
    for byte in ba:
        h = ((h << 4) + byte) & 0xFFFFFFFF
        g = h & 0xF0000000
        if g:
            h ^= g >> 24
        h &= ~g & 0xFFFFFFFF
    return h if h else 1


def _encode_utf16be(text: str) -> bytes:
    return text.encode("utf-16-be")


def _pack_field(tag: int, data: bytes) -> bytes:
    return struct.pack(">BI", tag, len(data)) + data


def _build_message_entry(context: str, source: str, comment: str,
                         translation: str):
    """Build one Qt .qm message entry and its lookup hash.

    Same layout as lrelease's default (SaveEverything) output:
    [Translation (UTF-16BE)][Comment][SourceText][Context][End], the last
    three as UTF-8 bytes, which is what QTranslator compares them with.
    The hash is elfHash(source + comment) in UTF-8: QTranslator computes
    exactly that for the lookup, the context is only compared once an entry
    of that hash is found. It lives in the hash block, never in the entry.
    """
    buf = bytearray()
    h = _elf_hash((source + comment).encode("utf-8"))
    buf += _pack_field(_TAG_TRANSLATION, _encode_utf16be(translation))
    buf += _pack_field(_TAG_COMMENT, comment.encode("utf-8"))
    buf += _pack_field(_TAG_SOURCE_TEXT, source.encode("utf-8"))
    buf += _pack_field(_TAG_CONTEXT, context.encode("utf-8"))
    buf += struct.pack(">B", _TAG_END)
    return bytes(buf), h


def _parse_ts(ts_path: str):
    """Yield (context, source, comment, translation) from a .ts file.

    Plural (numerus) messages are refused rather than written wrong: they
    need the language's plural rules block, which only lrelease produces.
    """
    tree = _safe_parse(ts_path)
    root = tree.getroot()
    for ctx_el in root.iter("context"):
        name_el = ctx_el.find("name")
        context = name_el.text if name_el is not None and name_el.text else ""
        for msg_el in ctx_el.iter("message"):
            src_el = msg_el.find("source")
            tr_el = msg_el.find("translation")
            source = src_el.text if src_el is not None and src_el.text else ""
            translation = tr_el.text if tr_el is not None and tr_el.text else ""
            if not source:
                continue
            tr_type = tr_el.get("type", "") if tr_el is not None else ""
            if tr_type in ("vanished", "obsolete"):
                continue
            if msg_el.get("numerus") == "yes":
                raise ValueError(
                    f"plural message {context}/{source[:40]!r} needs lrelease; "
                    "the fallback compiler does not write plural rules")
            comment = msg_el.findtext("comment") or ""
            yield context, source, comment, translation


def _find_lrelease() -> str | None:
    """Return the path to the Qt lrelease tool, or None if not found."""
    return shutil.which("lrelease")


def _compile_with_lrelease(ts_path: str, qm_path: str) -> int | None:
    """Try to compile with Qt lrelease. Return message count or None on failure."""
    lrelease = _find_lrelease()
    if lrelease is None:
        return None
    try:
        result = subprocess.run(
            [lrelease, ts_path, "-qm", qm_path],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        # lrelease prints summary like "Generated 337 translation(s) ..."
        for line in result.stdout.splitlines() + result.stderr.splitlines():
            if "Generated" in line and "translation" in line:
                try:
                    return int(line.strip().split()[1])
                except (ValueError, IndexError):
                    pass
        if result.returncode == 0 and os.path.isfile(qm_path):
            return 0
        return None
    except Exception:
        return None


def _compile_with_custom(ts_path: str, qm_path: str) -> int:
    """Compile a single .ts file to .qm using the pure-Python compiler.

    Returns the number of messages written. Untranslated messages are left
    out, as lrelease does: QTranslator then returns the source.
    """
    messages_buf = bytearray()
    hash_entries = []

    for context, source, comment, translation in _parse_ts(ts_path):
        if not translation:
            continue
        offset = len(messages_buf)
        entry_bytes, h = _build_message_entry(context, source, comment, translation)
        messages_buf += entry_bytes
        hash_entries.append((h, offset))

    # QTranslator binary-searches this table by hash, then walks the
    # entries of equal hash (same source in several contexts) in order.
    hash_entries.sort()
    hashes_buf = b"".join(
        struct.pack(">II", h, off) for h, off in hash_entries
    )
    language = (_safe_parse(ts_path).getroot().get("language") or "").encode("utf-8")

    with open(qm_path, "wb") as f:
        f.write(_QM_MAGIC)
        if language:
            f.write(struct.pack(">BI", _BLOCK_LANGUAGE, len(language)))
            f.write(language)
        f.write(struct.pack(">BI", _BLOCK_HASHES, len(hashes_buf)))
        f.write(hashes_buf)
        f.write(struct.pack(">BI", _BLOCK_MESSAGES, len(messages_buf)))
        f.write(messages_buf)

    return len(hash_entries)


def compile_ts_to_qm(ts_path: str, qm_path: str) -> int:
    """Compile a single .ts file to .qm.

    Prefers Qt lrelease for a standard .qm; falls back to the custom compiler.
    """
    count = _compile_with_lrelease(ts_path, qm_path)
    if count is not None:
        return count
    return _compile_with_custom(ts_path, qm_path)


def compile_ts_files(directory=None):
    """Compile all .ts files in directory to .qm."""
    if directory is None:
        directory = os.path.dirname(os.path.abspath(__file__))
    lrelease = _find_lrelease()
    if lrelease is None:
        print("lrelease not found; using pure-Python fallback compiler")
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".ts"):
            continue
        ts_path = os.path.join(directory, filename)
        qm_path = ts_path.replace(".ts", ".qm")
        count = compile_ts_to_qm(ts_path, qm_path)
        print(f"{filename} -> {os.path.basename(qm_path)}  ({count} messages)")


if __name__ == "__main__":
    compile_ts_files()
