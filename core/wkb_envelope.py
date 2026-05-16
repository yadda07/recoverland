"""Pure-Python WKB envelope (BBOX) parser for RecoverLand.

Reads the bounding box of a Well-Known Binary geometry without QGIS.
Used by `event_stream_repository.fetch_events_in_zone` to filter audit
events by spatial extent without a SQL spatial index (Time Lens P0,
cause racine CR-IL-1).

Supports:
    POINT, LINESTRING, POLYGON,
    MULTIPOINT, MULTILINESTRING, MULTIPOLYGON
in both byte orders (big- and little-endian).

Variants accepted:
    - ISO WKB (type codes 1, 2, 3, 4, 5, 6)
    - Z variant (type bit 0x80000000 OR type >= 1000)
    - M variant (type bit 0x40000000 OR type >= 2000)
    - ZM variant (both bits, or type >= 3000)
    - EWKB SRID variant (type bit 0x20000000), SRID consumed silently.

For unsupported types (e.g. GEOMETRYCOLLECTION, custom extensions),
returns None and the caller MUST treat the geometry as "envelope
unknown" - usually keeping it (conservative).

No QGIS, no Qt, no shapely dependency. Pure stdlib (struct).
"""
from __future__ import annotations

import struct
from typing import Optional, Tuple

Envelope = Tuple[float, float, float, float]  # (xmin, ymin, xmax, ymax)

# Type-bit masks used by EWKB.
_EWKB_Z_MASK = 0x80000000
_EWKB_M_MASK = 0x40000000
_EWKB_SRID_MASK = 0x20000000

# ISO WKB code conventions (1000/2000/3000 offsets for Z/M/ZM).
_ISO_Z_OFFSET = 1000
_ISO_M_OFFSET = 2000
_ISO_ZM_OFFSET = 3000


def _resolve_dims(type_code: int) -> Tuple[int, int]:
    """Return (base_type, n_dims) for a possibly-extended WKB type code.

    Handles both EWKB bit-flag style and ISO-style offsets. SRID flag
    is consumed by the caller (reads 4 extra bytes after the type).
    """
    has_z = False
    has_m = False
    cleaned = type_code

    # EWKB style.
    if cleaned & _EWKB_Z_MASK:
        has_z = True
        cleaned &= ~_EWKB_Z_MASK
    if cleaned & _EWKB_M_MASK:
        has_m = True
        cleaned &= ~_EWKB_M_MASK
    if cleaned & _EWKB_SRID_MASK:
        cleaned &= ~_EWKB_SRID_MASK

    # ISO style.
    if cleaned >= _ISO_ZM_OFFSET:
        cleaned -= _ISO_ZM_OFFSET
        has_z = True
        has_m = True
    elif cleaned >= _ISO_M_OFFSET:
        cleaned -= _ISO_M_OFFSET
        has_m = True
    elif cleaned >= _ISO_Z_OFFSET:
        cleaned -= _ISO_Z_OFFSET
        has_z = True

    n_dims = 2 + (1 if has_z else 0) + (1 if has_m else 0)
    return cleaned, n_dims


class _Reader:
    """Stream reader over a bytes buffer with explicit byteorder."""

    def __init__(self, data: bytes, endian: str):
        self._data = data
        self._pos = 0
        self._endian = endian  # "<" or ">"

    @property
    def endian(self) -> str:
        return self._endian

    def set_endian(self, endian: str) -> None:
        self._endian = endian

    def remaining(self) -> int:
        return len(self._data) - self._pos

    def read_byte(self) -> int:
        v = self._data[self._pos]
        self._pos += 1
        return v

    def read_uint32(self) -> int:
        (v,) = struct.unpack_from(self._endian + "I", self._data, self._pos)
        self._pos += 4
        return v

    def read_double(self) -> float:
        (v,) = struct.unpack_from(self._endian + "d", self._data, self._pos)
        self._pos += 8
        return v

    def skip(self, n: int) -> None:
        self._pos += n


def _expand(env: Optional[Envelope], x: float, y: float) -> Envelope:
    if env is None:
        return (x, y, x, y)
    xmin, ymin, xmax, ymax = env
    return (min(xmin, x), min(ymin, y), max(xmax, x), max(ymax, y))


def _merge(a: Optional[Envelope], b: Optional[Envelope]) -> Optional[Envelope]:
    if a is None:
        return b
    if b is None:
        return a
    return (
        min(a[0], b[0]), min(a[1], b[1]),
        max(a[2], b[2]), max(a[3], b[3]),
    )


def _read_header(r: _Reader) -> Tuple[int, int]:
    """Read byteorder + type. Set reader endian. Return (base_type, n_dims)."""
    endian_byte = r.read_byte()
    r.set_endian("<" if endian_byte == 1 else ">")
    raw_type = r.read_uint32()
    base, n_dims = _resolve_dims(raw_type)
    if raw_type & _EWKB_SRID_MASK:
        r.skip(4)  # SRID
    return base, n_dims


def _read_point(r: _Reader, n_dims: int) -> Tuple[float, float]:
    x = r.read_double()
    y = r.read_double()
    # Skip Z and/or M if present.
    if n_dims > 2:
        r.skip(8 * (n_dims - 2))
    return x, y


def _read_linestring_env(r: _Reader, n_dims: int) -> Optional[Envelope]:
    n = r.read_uint32()
    env: Optional[Envelope] = None
    for _ in range(n):
        x, y = _read_point(r, n_dims)
        env = _expand(env, x, y)
    return env


def _read_polygon_env(r: _Reader, n_dims: int) -> Optional[Envelope]:
    n_rings = r.read_uint32()
    env: Optional[Envelope] = None
    for _ in range(n_rings):
        env = _merge(env, _read_linestring_env(r, n_dims))
    return env


def _read_multi_env(r: _Reader, sub_reader) -> Optional[Envelope]:
    n_parts = r.read_uint32()
    env: Optional[Envelope] = None
    for _ in range(n_parts):
        # Each child has its own header (byteorder + type).
        child_env = sub_reader(r)
        env = _merge(env, child_env)
    return env


def _read_geometry_env(r: _Reader) -> Optional[Envelope]:
    base, n_dims = _read_header(r)
    if base == 1:  # POINT
        x, y = _read_point(r, n_dims)
        return (x, y, x, y)
    if base == 2:  # LINESTRING
        return _read_linestring_env(r, n_dims)
    if base == 3:  # POLYGON
        return _read_polygon_env(r, n_dims)
    if base == 4:  # MULTIPOINT
        return _read_multi_env(r, _read_geometry_env)
    if base == 5:  # MULTILINESTRING
        return _read_multi_env(r, _read_geometry_env)
    if base == 6:  # MULTIPOLYGON
        return _read_multi_env(r, _read_geometry_env)
    # 7 = GEOMETRYCOLLECTION (each child is a full WKB geometry).
    if base == 7:
        return _read_multi_env(r, _read_geometry_env)
    # Unknown / custom type.
    return None


def parse_envelope(wkb_data: Optional[bytes]) -> Optional[Envelope]:
    """Return (xmin, ymin, xmax, ymax) of *wkb_data*, or None if unparsable.

    Returns None when:
        - wkb_data is None or empty.
        - WKB type is not in the supported set (1..7).
        - Buffer truncated / corrupted.

    Returning None is a SIGNAL to the caller; it does not mean "empty
    envelope". Conservative callers should treat None as "extent
    unknown" and keep the event (rather than dropping it).
    """
    if wkb_data is None or len(wkb_data) < 5:
        return None
    try:
        r = _Reader(wkb_data, "<")  # endian will be reset by _read_header
        return _read_geometry_env(r)
    except (struct.error, IndexError):
        return None


def envelope_intersects(
    env: Optional[Envelope],
    bbox: Tuple[float, float, float, float],
) -> bool:
    """Strict axis-aligned BBOX intersection test.

    If *env* is None (parser could not extract bbox), returns True so
    the event is kept conservatively (the caller may filter further
    downstream).
    """
    if env is None:
        return True
    xmin, ymin, xmax, ymax = env
    bxmin, bymin, bxmax, bymax = bbox
    if xmax < bxmin or xmin > bxmax:
        return False
    if ymax < bymin or ymin > bymax:
        return False
    return True


__all__ = [
    "Envelope",
    "parse_envelope",
    "envelope_intersects",
]
