"""Pure-Python .ts -> .qm compiler for RecoverLand.

No external tool required.  Produces a binary .qm file that
QTranslator.load() can read.

Usage (QGIS Python console or standalone):
    from recoverland.i18n.compile_translations import compile_ts_files
    compile_ts_files()
"""
import os
import struct
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
    parser = ET.XMLParser()
    parser.entity = {}
    return ET.parse(path, parser=parser)


_QM_MAGIC = (
    b"\x3c\xb8\x64\x18\xca\xef\x9c\x95"
    b"\xcd\x21\x1c\xbf\x60\xa1\xbd\xdd"
)

_TAG_END = 1
_TAG_SOURCE16 = 2
_TAG_TRANSLATION = 3
_TAG_CONTEXT16 = 4
_TAG_HASH = 6

_BLOCK_HASHES = 0x42
_BLOCK_MESSAGES = 0x69


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


def _build_message_entry(context: str, source: str, translation: str) -> bytes:
    buf = bytearray()
    key = (context + "\n" + source + "\n").encode("utf-8")
    h = _elf_hash(key)
    buf += struct.pack(">BI", _TAG_HASH, h)
    buf += _pack_field(_TAG_CONTEXT16, _encode_utf16be(context))
    buf += _pack_field(_TAG_SOURCE16, _encode_utf16be(source))
    buf += _pack_field(_TAG_TRANSLATION, _encode_utf16be(translation))
    buf += struct.pack(">B", _TAG_END)
    return bytes(buf), h


def _parse_ts(ts_path: str):
    """Yield (context, source, translation) from a .ts file."""
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
            yield context, source, translation


def compile_ts_to_qm(ts_path: str, qm_path: str) -> int:
    """Compile a single .ts file to .qm.  Returns message count."""
    messages_buf = bytearray()
    hash_entries = []

    for context, source, translation in _parse_ts(ts_path):
        if not translation:
            continue
        offset = len(messages_buf)
        entry_bytes, h = _build_message_entry(context, source, translation)
        messages_buf += entry_bytes
        hash_entries.append((h, offset))

    hash_entries.sort(key=lambda e: e[0])
    hashes_buf = b"".join(
        struct.pack(">II", h, off) for h, off in hash_entries
    )

    with open(qm_path, "wb") as f:
        f.write(_QM_MAGIC)
        f.write(struct.pack(">BI", _BLOCK_HASHES, len(hashes_buf)))
        f.write(hashes_buf)
        f.write(struct.pack(">BI", _BLOCK_MESSAGES, len(messages_buf)))
        f.write(messages_buf)

    return len(hash_entries)


def compile_ts_files(directory=None):
    """Compile all .ts files in directory to .qm."""
    if directory is None:
        directory = os.path.dirname(os.path.abspath(__file__))
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".ts"):
            continue
        ts_path = os.path.join(directory, filename)
        qm_path = ts_path.replace(".ts", ".qm")
        count = compile_ts_to_qm(ts_path, qm_path)
        print(f"{filename} -> {os.path.basename(qm_path)}  ({count} messages)")


if __name__ == "__main__":
    compile_ts_files()
