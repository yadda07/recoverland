"""Read-back verification and compensation around a restore commit.

``QgsVectorLayer.commitChanges()`` returning True proves that the provider
did not report an error. It does not prove that the file holds what was
asked: OGR drops the extra parts of a multi-part geometry written into a
single-part layer, truncates text to the column width, reorients rings.
And ``commitChanges()`` returning False does not prove that nothing was
written: the edit buffer commits stage by stage, and on GeoPackage the
geometry and attribute stages are ALREADY in the file when the additions
stage hits a UNIQUE index (measured on QGIS 4.2.1).

The provider is the authority. :class:`CommitGuard` wraps a commit so that
every restore write ends in a verified, explicit state:

``COMMITTED_VERIFIED``         every touched feature reads back as asked
``COMMITTED_MISMATCH``         the commit succeeded, some features read back
                               differently (per event: FAILED_POSTCOMMIT_VERIFICATION)
``REFUSED_NOTHING_WRITTEN``    the commit failed and the provider holds its
                               pre-commit state (verified)
``REFUSED_COMPENSATED``        the commit failed after writing some stages;
                               those were reverted and the revert verified
``REFUSED_COMPENSATION_FAILED``the commit failed after writing some stages and
                               the revert did not fully land: the report
                               names every feature left modified

How it works: the guard records which feature ids each action touched
(edit-buffer deltas around the action), reads the provider state of those
features before the commit (the pre-image) and the edit-buffer projection
(what the provider is asked to store), listens to the ``committed*``
signals during the commit (what the provider says it applied), then reads
the provider again through a FRESH layer on the same source.

GUI thread only (QGIS layers).
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Set

from .logger import flog

COMMITTED_VERIFIED = "COMMITTED_VERIFIED"
COMMITTED_MISMATCH = "COMMITTED_MISMATCH"
REFUSED_NOTHING_WRITTEN = "REFUSED_NOTHING_WRITTEN"
REFUSED_COMPENSATED = "REFUSED_COMPENSATED"
REFUSED_COMPENSATION_FAILED = "REFUSED_COMPENSATION_FAILED"

# Per-event outcomes (restore status taxonomy).
EV_APPLIED_VERIFIED = "APPLIED_VERIFIED"
EV_FAILED_POSTCOMMIT_VERIFICATION = "FAILED_POSTCOMMIT_VERIFICATION"
EV_NOT_APPLIED = "NOT_APPLIED"
EV_LANDED_THEN_COMPENSATED = "LANDED_THEN_COMPENSATED"
EV_LANDED_NOT_COMPENSATED = "LANDED_NOT_COMPENSATED"
EV_FAILED_COMMIT = "FAILED_COMMIT"

_FLOAT_REL_TOL = 1e-9


class _GeomHolder:
    def __init__(self, geom):
        self._g = geom

    def geometry(self):
        return self._g


def _norm_value(value):
    """QGIS attribute value -> comparable Python value."""
    from .serialization import serialize_value
    try:
        out = serialize_value(value)
    except Exception:  # noqa: BLE001 - comparison must not raise
        out = repr(value)
    return out


def _instant_ms(value):
    """Epoch milliseconds of a live date-time value, else None."""
    from .serialization import datetime_instant_ms
    try:
        return datetime_instant_ms(value)
    except Exception:  # noqa: BLE001 - comparison must not raise
        return None


def _datetimes_equal(a, b):
    """True / False when *a* and *b* compare as date-times, else None.

    A date-time is an INSTANT. Its journal text keeps its time spec since
    v7 (``14:30:00.000`` local, ``13:30:00.000Z``), and a GeoPackage hands
    back in UTC what QGIS wrote in local time: comparing texts judged the
    same instant different, so a restore that wrote the exact value was
    reported failed, the strict guard classed an exact compensation
    FAILED_POSTCOMMIT_VERIFICATION, and crash reconciliation lost an
    INSERT that had landed. Compared here as milliseconds since the epoch;
    a text on one side is read the way Qt reads it when it is handed to a
    date-time field (no suffix: local wall clock). None when neither side
    is a date-time value, or when the other side is no date-time at all.

    *a* is the value the provider holds, *b* the one expected of it (every
    caller passes them in that order): compared at the resolution the
    stored side carries (`serialization.instants_match`). A provider that
    stores to the second (SpatiaLite, PostgreSQL ``timestamp(0)``) holds
    ``14:30:12.000`` for a journaled ``14:30:12.517``; compared to the
    millisecond, a crash reconciliation lost the INSERT that had landed
    and the undo of that INSERT was refused for good.
    """
    ia, ib = _instant_ms(a), _instant_ms(b)
    if ia is None and ib is None:
        return None
    if ia is None or ib is None:
        from .serialization import journal_text_instant_ms
        other = a if ia is None else b
        if not isinstance(other, str):
            return None
        try:
            parsed = journal_text_instant_ms(other)
        except Exception:  # noqa: BLE001 - comparison must not raise
            parsed = None
        if parsed is None:
            return None
        ia = parsed if ia is None else ia
        ib = parsed if ib is None else ib
    from .serialization import instants_match
    return instants_match(ia, ib)


def values_equal(a, b) -> bool:
    """True when the stored value *a* holds the expected value *b*.

    The order matters for date-times only (`_datetimes_equal`): a stored
    value without a fraction of a second matches an expected one with a
    fraction when they are less than a second apart, never the reverse.
    """
    va, vb = _norm_value(a), _norm_value(b)
    if va is None or vb is None:
        return va is None and vb is None
    as_datetimes = _datetimes_equal(a, b)
    if as_datetimes is not None:
        return as_datetimes
    if isinstance(va, bool) or isinstance(vb, bool):
        return va == vb
    if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
        if isinstance(va, float) or isinstance(vb, float):
            fa, fb = float(va), float(vb)
            if math.isnan(fa) and math.isnan(fb):
                return True
            return math.isclose(fa, fb, rel_tol=_FLOAT_REL_TOL, abs_tol=1e-12)
        return va == vb
    return va == vb


def geometry_equal(actual, expected) -> bool:
    """Same shape, tolerant to ring start/orientation and rounding re-encoding."""
    from .geometry_utils import is_geometry_present, feature_matches_geometry
    a_present = is_geometry_present(actual)
    e_present = is_geometry_present(expected)
    if not a_present or not e_present:
        return a_present == e_present
    try:
        if bytes(actual.asWkb()) == bytes(expected.asWkb()):
            return True
    except (AttributeError, RuntimeError, TypeError):
        pass
    if feature_matches_geometry(_GeomHolder(actual), expected):
        return True
    ka, ke = _vertex_key(actual), _vertex_key(expected)
    if ka is not None and ka == ke and _part_count(actual) == _part_count(expected):
        return True
    # Single-part stored as a one-part multi (or the reverse) is the same
    # shape: shapefile and GPKG layers promote on write.
    try:
        a_parts = actual.asGeometryCollection()
        e_parts = expected.asGeometryCollection()
        if len(a_parts) == 1 and len(e_parts) == 1:
            return feature_matches_geometry(_GeomHolder(a_parts[0]), e_parts[0])
    except (AttributeError, RuntimeError, TypeError):
        pass
    return False


_KEY_DIGITS = 7
_ISO_MIN_LEN = 16
_AREA_MIN_SPAN = 1.0
_AREA_PAD_REL = 1e-6
_AREA_PAD_ABS = 1e-9
_CANON_TAG = "canon"


def _vertex_key(geom):
    """Rounded canonical shape key: equal for two encodings of one shape.

    Invariant to ring start, ring orientation, part order and to the
    single/multi wrapping providers apply on write; tolerant to the
    coordinate rounding of text formats (GeoJSON, CSV WKT) at 1e-7,
    while preserving vertex order on linear geometries.
    """
    from .geometry_utils import is_geometry_present, _canonical_ring_cycle
    if not is_geometry_present(geom):
        return None
    try:
        polys = geom.asMultiPolygon() if geom.isMultipart() else (
            [geom.asPolygon()] if geom.asPolygon() else [])
    except (AttributeError, RuntimeError, TypeError):
        polys = []
    if polys:
        out_polys = []
        for rings in polys:
            canon = []
            for ring in rings:
                pts = [(round(p.x(), _KEY_DIGITS), round(p.y(), _KEY_DIGITS))
                       for p in ring]
                if len(pts) > 1 and pts[0] == pts[-1]:
                    pts = pts[:-1]
                canon.append(_canonical_ring_cycle(pts))
            out_polys.append(tuple(sorted(canon)))
        return tuple(sorted(out_polys)) if out_polys else None
    try:
        lines = geom.asMultiPolyline() if geom.isMultipart() else (
            [geom.asPolyline()] if geom.asPolyline() else [])
    except (AttributeError, RuntimeError, TypeError):
        lines = []
    if lines:
        out_lines = [tuple((round(p.x(), _KEY_DIGITS), round(p.y(), _KEY_DIGITS))
                           for p in line) for line in lines if line]
        return tuple(sorted(out_lines)) if out_lines else None
    try:
        pts = sorted((round(v.x(), _KEY_DIGITS), round(v.y(), _KEY_DIGITS))
                     for v in geom.vertices())
    except (AttributeError, RuntimeError, TypeError):
        return None
    return tuple(pts) if pts else None


def _text_instant_ms(value):
    if not isinstance(value, str) or len(value) < _ISO_MIN_LEN:
        return None
    from .serialization import journal_text_instant_ms
    try:
        return journal_text_instant_ms(value)
    except Exception:  # noqa: BLE001 - comparison must not raise
        return None


def _canon_value(value):
    # Same notion of equality as `values_equal`: one instant, one key,
    # whatever the time spec the value was written or read back with. A
    # hash key cannot carry the tolerance `values_equal` grants a provider
    # that stores to the second, so the key is the whole second the
    # instant falls in: what a truncating store keeps (the positional-FID
    # formats whose content these signatures compare). A rounding store is
    # a database provider, with stable FIDs, never compared by signature.
    instant = _instant_ms(value)
    if instant is not None:
        return ("datetime_s", instant // 1000)
    v = _norm_value(value)
    if isinstance(v, str):
        text_ms = _text_instant_ms(v)
        if text_ms is not None:
            return ("datetime_s", text_ms // 1000)
    if isinstance(v, float):
        if math.isnan(v):
            return "nan"
        return float(f"{v:.12g}")
    return v


def fid_is_stable(layer) -> bool:
    """False when the provider renumbers features (positional FIDs).

    Shapefile, GeoJSON, CSV and most flat OGR formats derive the FID from
    the position of the record: a deletion followed by a repack moves every
    later feature to another FID. Reading back "fid 12" after such a commit
    reads a different feature. Database-backed providers keep their key.
    """
    try:
        provider = layer.dataProvider()
        name = provider.name()
    except (RuntimeError, AttributeError):
        return False
    if name in ("postgres", "spatialite", "mssql", "oracle", "memory", "hana"):
        return True
    if name == "ogr":
        try:
            storage = (provider.storageType() or "").upper()
        except (RuntimeError, AttributeError):
            storage = ""
        return storage in ("GPKG", "SQLITE", "FILEGDB", "OPENFILEGDB",
                           "POSTGRESQL", "MSSQLSPATIAL", "OCI")
    return False


def _part_count(geom) -> int:
    try:
        return max(1, len(geom.asGeometryCollection()))
    except (AttributeError, RuntimeError, TypeError):
        return 1


class _FeatureState:
    __slots__ = ("attrs", "geom", "exists")

    def __init__(self, attrs=None, geom=None, exists=True):
        self.attrs = attrs or {}
        self.geom = geom
        self.exists = exists


def _read_states(source, fids, field_names) -> Dict[int, _FeatureState]:
    from qgis.core import QgsFeatureRequest, QgsGeometry
    out: Dict[int, _FeatureState] = {}
    fids = [int(f) for f in fids if f is not None and int(f) >= 0]
    if not fids:
        return out
    request = QgsFeatureRequest().setFilterFids(fids)
    for feat in source.getFeatures(request):
        attrs = {}
        for name in field_names:
            try:
                attrs[name] = feat[name]
            except (KeyError, IndexError):
                attrs[name] = None
        geom = feat.geometry()
        out[int(feat.id())] = _FeatureState(
            attrs, QgsGeometry(geom) if geom is not None else None, True)
    for fid in fids:
        out.setdefault(fid, _FeatureState(exists=False))
    return out


def _sync_layer_encoding(src_layer, dst_layer) -> None:
    try:
        src_dp = src_layer.dataProvider()
        dst_dp = dst_layer.dataProvider()
        enc = src_dp.encoding() if src_dp is not None else ""
        if enc and dst_dp is not None and hasattr(dst_dp, "setEncoding"):
            dst_dp.setEncoding(enc)
    except (RuntimeError, AttributeError, TypeError):
        pass


def _fresh_layer(layer):
    """A new layer on the same source: reads the file, not a provider cache."""
    from qgis.core import QgsVectorLayer
    try:
        fresh = QgsVectorLayer(layer.source(), "rl_readback", layer.providerType())
        if fresh.isValid():
            _sync_layer_encoding(layer, fresh)
            return fresh
    except (RuntimeError, AttributeError):
        pass
    return None


def _diff(actual: _FeatureState, expected: _FeatureState, field_names,
          auto_fields=()) -> List[str]:
    if actual.exists != expected.exists:
        return [f"exists={actual.exists} expected={expected.exists}"]
    if not actual.exists:
        return []
    out = []
    for name in field_names:
        if name not in expected.attrs:
            continue
        if name in auto_fields and _norm_value(expected.attrs.get(name)) is None:
            continue
        if not values_equal(actual.attrs.get(name), expected.attrs.get(name)):
            out.append(f"{name}: {actual.attrs.get(name)!r} != {expected.attrs.get(name)!r}")
    if not geometry_equal(actual.geom, expected.geom):
        out.append("geometry differs")
    return out


class CommitVerdict:
    def __init__(self):
        self.state = ""
        self.commit_errors: List[str] = []
        self.per_event: Dict[int, str] = {}
        self.per_event_detail: Dict[int, str] = {}
        self.landed_fids: Set[int] = set()
        self.added_fids: List[int] = []
        self.left_modified: List[str] = []

    @property
    def committed(self) -> bool:
        return self.state in (COMMITTED_VERIFIED, COMMITTED_MISMATCH)

    def events_with(self, status: str) -> List[int]:
        return [eid for eid, st in self.per_event.items() if st == status]

    def as_dict(self) -> dict:
        return {"state": self.state, "commit_errors": self.commit_errors[:6],
                "per_event": dict(self.per_event),
                "left_modified": self.left_modified[:20]}


class CommitGuard:
    """Track, commit, read back, compensate. One instance per restore commit."""

    def __init__(self, layer, trace_id: str = ""):
        self.layer = layer
        self.prefix = f"[{trace_id}] " if trace_id else ""
        provider = layer.dataProvider()
        self.field_names = [f.name() for f in provider.fields()] if provider else []
        # Columns the provider fills itself (primary key, default clause):
        # a projection that leaves them NULL is not contradicted by the
        # value the provider assigned.
        self.auto_fields = set()
        try:
            fields = provider.fields()
            for idx in provider.pkAttributeIndexes():
                if 0 <= idx < fields.count():
                    self.auto_fields.add(fields.at(idx).name())
            for idx in range(fields.count()):
                if provider.defaultValueClause(idx):
                    self.auto_fields.add(fields.at(idx).name())
        except (RuntimeError, AttributeError):
            pass
        self.fid_stable = fid_is_stable(layer)
        self._event_fids: Dict[int, Set[int]] = {}
        self._event_added: Dict[int, Set[int]] = {}
        self._event_deleted: Dict[int, Set[int]] = {}
        self._cur: Optional[int] = None
        self._before_keys = None

    # ------------------------------------------------------------------
    def _buffer_keys(self):
        """(changed, added, deleted) feature ids of the edit buffer, ids only.

        Read around EVERY action of a restore (begin_action, end_action):
        copying changedAttributeValues(), changedGeometries() and
        addedFeatures() converted every value, geometry and added feature
        to Python each time, only to keep the keys (4.6 s of 21.8 s for
        4 500 compensations, rw_rewind_mass_exec_bench).
        allAddedOrEditedFeatures() (QGIS >= 3.20) returns the union of the
        three key sets without the values; each id is then asked which
        table holds it (isFeatureAdded, isFeatureAttributesChanged,
        isFeatureGeometryChanged: lookups, nothing copied). About a third
        of the cost of the copies: 0.27 to 0.28 ms against 0.90 to 1.0 ms
        per read, buffer of 999 changed, 203 added and 143 deleted
        features, QGIS 4.2.1.
        The SIGN of an id says nothing here. The buffered edit buffer gives
        an added feature a temporary negative id, but the passthrough
        buffer of a transaction group (QgsVectorLayerEditPassthrough,
        Project > Transaction mode) inserts at once and keeps the added
        feature under the positive fid the provider gave it. Classified by
        sign, a restored insert was taken for a change and left out of
        _event_added; _verify_commit then never checked it, and the event
        came out APPLIED_VERIFIED whatever the provider stored
        (rw_guard_buffer_keys_ids_only).
        """
        eb = self.layer.editBuffer()
        if eb is None:
            return set(), set(), set()
        deleted = set(eb.deletedFeatureIds())
        ids_of = getattr(eb, "allAddedOrEditedFeatures", None)
        is_added = getattr(eb, "isFeatureAdded", None)
        is_attr = getattr(eb, "isFeatureAttributesChanged", None)
        is_geom = getattr(eb, "isFeatureGeometryChanged", None)
        if not (ids_of and is_added and is_attr and is_geom):
            changed = (set(eb.changedAttributeValues().keys())
                       | set(eb.changedGeometries().keys()))
            return changed, set(eb.addedFeatures().keys()), deleted
        ids = set(ids_of())
        added = set(filter(is_added, ids))
        # ids is the union of the three tables: an id that is not an added
        # feature is a changed one. An added id is a changed one too only
        # when a changed table holds it.
        changed = ids - added
        if added:
            changed |= set(filter(is_attr, added))
            changed |= set(filter(is_geom, added))
        return changed, added, deleted

    def begin_action(self, event_id: int) -> None:
        self._cur = int(event_id)
        self._before_keys = self._buffer_keys()

    def end_action(self, event_id: int, extra_fids=()) -> None:
        if self._before_keys is None:
            return
        changed0, added0, deleted0 = self._before_keys
        changed1, added1, deleted1 = self._buffer_keys()
        eid = int(event_id)
        fids = (changed1 - changed0) | set(int(f) for f in extra_fids if f is not None)
        # A feature already changed by an earlier action of the same batch
        # does not show up as a NEW key: fall back on the target the
        # action reported, when it did.
        self._event_fids.setdefault(eid, set()).update(
            f for f in fids if f >= 0)
        self._event_added.setdefault(eid, set()).update(added1 - added0)
        self._event_deleted.setdefault(eid, set()).update(deleted1 - deleted0)
        self._before_keys = None
        self._cur = None

    def foreign_changes(self) -> List[int]:
        """Edit-buffer entries no action of this run produced.

        The run holds the layer in edit mode across QTimer ticks; anything
        the user digitises there meanwhile lands in the same buffer and
        would be committed as part of the restore, suppressed from the
        journal. Fids changed or deleted, and fids added (temporary in a
        buffered session, the provider's own in a transaction group), that
        no action recorded, are returned.
        """
        eb = self.layer.editBuffer()
        if eb is None:
            return []
        changed, added, deleted = self._buffer_keys()
        known = set()
        for fids in self._event_fids.values():
            known |= fids
        for fids in self._event_deleted.values():
            known |= fids
        known_added = set()
        for fids in self._event_added.values():
            known_added |= fids
        foreign = sorted((changed | deleted) - known) + sorted(added - known_added)
        return foreign

    # ------------------------------------------------------------------
    def commit(self, stop_editing: bool = True) -> CommitVerdict:
        """prepare() + layer.commitChanges() + finish(): the whole guarded commit."""
        self.prepare()
        try:
            ok = self.layer.commitChanges(stop_editing)
        except Exception:
            self._disconnect()
            raise
        return self.finish(ok)

    def prepare(self) -> None:
        """Read pre-images and the projection, start listening to committed*."""
        layer = self.layer
        eb = layer.editBuffer()
        changed = (set(eb.changedAttributeValues().keys())
                   | set(eb.changedGeometries().keys())) if eb else set()
        deleted = set(eb.deletedFeatureIds()) if eb else set()
        added_tmp = dict(eb.addedFeatures()) if eb else {}
        self._touched = sorted(f for f in (changed | deleted) if f >= 0)
        pre_source = layer.dataProvider()
        try:
            from qgis.core import QgsVectorLayerEditPassthrough
            if isinstance(eb, QgsVectorLayerEditPassthrough):
                fresh = _fresh_layer(layer)
                if fresh is not None and fresh.dataProvider() is not None:
                    pre_source = fresh.dataProvider()
        except (ImportError, RuntimeError, AttributeError):
            pass
        self._pre = _read_states(pre_source, self._touched, self.field_names)
        self._projection = _read_states(layer, sorted(changed), self.field_names)
        for fid in deleted:
            self._projection[int(fid)] = _FeatureState(exists=False)
        self._added_proj = []
        for tmp_fid, feat in added_tmp.items():
            attrs = {}
            for name in self.field_names:
                try:
                    attrs[name] = feat[name]
                except (KeyError, IndexError):
                    attrs[name] = None
            self._added_proj.append((tmp_fid, _FeatureState(attrs, feat.geometry(), True)))
        applied = {"attr": set(), "geom": set(), "removed": set(), "added": []}
        self._applied = applied
        self._area = None
        self._before_sigs = None
        if not self.fid_stable:
            self._area = self._affected_area()
            self._before_sigs = self._signatures(layer.dataProvider(), self._area)

        def _on_attr(_lid, changes):
            applied["attr"].update(int(f) for f in changes.keys())

        def _on_geom(_lid, changes):
            applied["geom"].update(int(f) for f in changes.keys())

        def _on_removed(_lid, fids):
            applied["removed"].update(int(f) for f in fids)

        def _on_added(_lid, feats):
            applied["added"].extend(int(f.id()) for f in feats)

        self._conns = [(layer.committedAttributeValuesChanges, _on_attr),
                       (layer.committedGeometriesChanges, _on_geom),
                       (layer.committedFeaturesRemoved, _on_removed),
                       (layer.committedFeaturesAdded, _on_added)]
        for sig, slot in self._conns:
            sig.connect(slot)

    def _disconnect(self) -> None:
        for sig, slot in getattr(self, "_conns", []):
            try:
                sig.disconnect(slot)
            except (TypeError, RuntimeError):
                pass
        self._conns = []

    def finish(self, ok: bool) -> CommitVerdict:
        """Establish what the provider really holds after commitChanges() == ok."""
        verdict = self._finish(ok)
        try:
            from .logger import jlog
            counts = {}
            for status in verdict.per_event.values():
                counts[status] = counts.get(status, 0) + 1
            jlog("restore_commit_verdict",
                 "INFO" if verdict.state == COMMITTED_VERIFIED else "ERROR",
                 component="commit_guard", trace_id=self.prefix.strip("[] "),
                 state=verdict.state, per_event=counts,
                 commit_errors=verdict.commit_errors[:4],
                 left_modified=verdict.left_modified[:10],
                 fid_stable=self.fid_stable)
        except Exception:  # noqa: BLE001 # nosec B110 - forensic log never breaks verdict
            pass
        return verdict

    def _finish(self, ok: bool) -> CommitVerdict:
        self._disconnect()
        layer = self.layer
        provider = layer.dataProvider()
        verdict = CommitVerdict()
        applied = self._applied
        touched, pre = self._touched, self._pre
        projection, added_proj = self._projection, self._added_proj
        verdict.commit_errors = list(layer.commitErrors()) if not ok else []
        landed = applied["attr"] | applied["geom"] | applied["removed"]
        verdict.landed_fids = set(landed)
        verdict.added_fids = list(applied["added"])

        if ok and not self.fid_stable:
            self._verify_by_content(verdict)
            return verdict
        if ok:
            self._verify_commit(verdict, projection, added_proj)
            return verdict
        if not self.fid_stable:
            if layer.isEditable():
                layer.rollBack()
            self._refused_by_content(verdict)
            return verdict

        # ---- the commit was refused: what is on disk now? -------------
        if layer.isEditable():
            layer.rollBack()
        fresh = _fresh_layer(layer) or provider
        now = _read_states(fresh, touched, self.field_names)
        really_changed = [fid for fid in touched
                          if _diff(now[fid], pre[fid], self.field_names, self.auto_fields)]
        landed_added = list(applied["added"])
        if not really_changed and not landed_added:
            verdict.state = REFUSED_NOTHING_WRITTEN
            self._attribute_refusal(verdict, set(), set())
            flog(f"{self.prefix}commit_guard: event=commit_refused_nothing_written "
                 f"errors={verdict.commit_errors[:4]}", "WARNING")
            return verdict
        flog(f"{self.prefix}commit_guard: event=commit_refused_partial_write "
             f"changed={really_changed[:20]} added={landed_added[:20]} "
             f"errors={verdict.commit_errors[:4]}: compensating", "ERROR")
        left = self._compensate(pre, really_changed, landed_added)
        if not left:
            verdict.state = REFUSED_COMPENSATED
        else:
            verdict.state = REFUSED_COMPENSATION_FAILED
            verdict.left_modified = left
        self._attribute_refusal(verdict, set(really_changed), set(landed_added),
                                compensated=not left)
        flog(f"{self.prefix}commit_guard: event=compensation_result "
             f"state={verdict.state} left_modified={left[:20]}",
             "ERROR" if left else "WARNING")
        return verdict

    # ------------------------------------------------------------------
    def _verify_commit(self, verdict: CommitVerdict, projection, added_proj) -> None:
        layer = self.layer
        fresh = _fresh_layer(layer) or layer.dataProvider()
        fids = sorted(projection.keys())
        now = _read_states(fresh, fids, self.field_names)
        mismatched: Dict[int, List[str]] = {}
        for fid in fids:
            d = _diff(now[fid], projection[fid], self.field_names, self.auto_fields)
            if d:
                mismatched[fid] = d
        added_now = _read_states(fresh, verdict.added_fids, self.field_names)
        added_bad: Dict[int, List[str]] = {}
        # Pair each requested addition with a reported one BY CONTENT: the
        # order of committedFeaturesAdded is not a documented guarantee.
        unmatched = [f for f in verdict.added_fids]
        for tmp_fid, proj in added_proj:
            match = None
            for real in unmatched:
                if not _diff(added_now.get(real, _FeatureState(exists=False)),
                             proj, self.field_names, self.auto_fields):
                    match = real
                    break
            if match is not None:
                unmatched.remove(match)
            else:
                added_bad[tmp_fid] = [
                    "no feature of the provider holds the values asked for"]
        verdict.state = COMMITTED_MISMATCH if (mismatched or added_bad) else COMMITTED_VERIFIED
        for eid in self._all_events():
            problems = []
            for fid in self._event_fids.get(eid, ()) | self._event_deleted.get(eid, ()):
                problems.extend(f"fid {fid}: {p}" for p in mismatched.get(fid, []))
            for tmp in self._event_added.get(eid, ()):
                problems.extend(f"new feature: {p}" for p in added_bad.get(tmp, []))
            if problems:
                verdict.per_event[eid] = EV_FAILED_POSTCOMMIT_VERIFICATION
                verdict.per_event_detail[eid] = "; ".join(problems)[:500]
            else:
                verdict.per_event[eid] = EV_APPLIED_VERIFIED
        if mismatched or added_bad:
            flog(f"{self.prefix}commit_guard: event=postcommit_mismatch "
                 f"features={sorted(mismatched)[:20]} added={sorted(added_bad)[:20]} "
                 f"detail={list(mismatched.items())[:3]}", "ERROR")
        else:
            flog(f"{self.prefix}commit_guard: event=postcommit_verified "
                 f"features={len(fids)} added={len(verdict.added_fids)}")

    # ---------------------------------------------------- unstable FIDs
    def _attr_key(self, attrs: dict) -> tuple:
        return tuple((n, _canon_value(attrs.get(n)))
                     for n in self.field_names if n not in self.auto_fields)

    def _signature(self, state: "_FeatureState"):
        return self._attr_key(state.attrs), _vertex_key(state.geom), (
            _part_count(state.geom) if state.geom is not None else 0)

    def _fast_signature(self, state: "_FeatureState") -> tuple:
        from .geometry_utils import is_geometry_present
        attrs = self._attr_key(state.attrs)
        g = state.geom
        if not is_geometry_present(g):
            return attrs, None
        try:
            wkb = bytes(g.asWkb())
            if wkb:
                return attrs, wkb
        except (AttributeError, RuntimeError, TypeError):
            pass
        return attrs, (_CANON_TAG, _vertex_key(g), _part_count(g))

    def _canon_from_fast(self, fast_sig: tuple) -> tuple:
        from .geometry_utils import rebuild_geometry
        attrs, geom_tok = fast_sig
        if geom_tok is None:
            return attrs, None, 0
        if isinstance(geom_tok, tuple) and len(geom_tok) == 3:
            return attrs, geom_tok[1], geom_tok[2]
        cache = self.__dict__.setdefault("_wkb_canon_cache", {})
        cached = cache.get(geom_tok)
        if cached is None:
            geom = rebuild_geometry(geom_tok)
            cached = (_vertex_key(geom),
                      _part_count(geom) if geom is not None else 0)
            cache[geom_tok] = cached
        return attrs, cached[0], cached[1]

    def _project_canon(self, fast_counts):
        from collections import Counter
        out = Counter()
        for fast_sig, count in fast_counts.items():
            if count > 0:
                out[self._canon_from_fast(fast_sig)] += count
        return out

    def _resolve_canon_diff(self, expected_fast, after_fast, unmatched_pre):
        from collections import Counter
        missing_fast = expected_fast - after_fast
        extra_fast = (after_fast - expected_fast) + unmatched_pre
        if not missing_fast and not extra_fast:
            return Counter(), Counter(), 0
        n_res = sum(missing_fast.values()) + sum(extra_fast.values())
        missing_canon = self._project_canon(missing_fast)
        extra_canon = self._project_canon(extra_fast)
        return missing_canon - extra_canon, extra_canon - missing_canon, n_res

    def _affected_area(self):
        from qgis.core import QgsRectangle
        from .geometry_utils import _finite_box, is_geometry_present
        states = list(self._pre.values()) + list(self._projection.values())
        states += [st for _t, st in self._added_proj]
        boxes = []
        for st in states:
            if not st.exists:
                continue
            if not is_geometry_present(st.geom):
                return None
            box = _finite_box(st.geom)
            if box is None:
                return None
            boxes.append(box)
        if not boxes:
            return None
        xmin = min(b[0] for b in boxes)
        ymin = min(b[1] for b in boxes)
        xmax = max(b[2] for b in boxes)
        ymax = max(b[3] for b in boxes)
        span = max(xmax - xmin, ymax - ymin, _AREA_MIN_SPAN)
        grow = span * _AREA_PAD_REL + _AREA_PAD_ABS
        return QgsRectangle(xmin - grow, ymin - grow, xmax + grow, ymax + grow)

    def _signatures(self, source, area):
        from collections import Counter
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest()
        if area is not None:
            request.setFilterRect(area)
        sigs = Counter()
        for feat in source.getFeatures(request):
            attrs = {}
            for name in self.field_names:
                try:
                    attrs[name] = feat[name]
                except (KeyError, IndexError):
                    attrs[name] = None
            sigs[self._fast_signature(_FeatureState(attrs, feat.geometry(), True))] += 1
        return sigs

    def _expected_after(self):
        from collections import Counter
        expected = Counter(self._before_sigs)
        unmatched_pre = Counter()
        per_event_expect = {}
        for fid, st in self._pre.items():
            if st.exists:
                sig = self._fast_signature(st)
                if expected[sig] > 0:
                    expected[sig] -= 1
                else:
                    unmatched_pre[sig] += 1
        for fid, st in self._projection.items():
            if st.exists:
                expected[self._fast_signature(st)] += 1
        for _tmp, st in self._added_proj:
            expected[self._fast_signature(st)] += 1
        for eid in self._all_events():
            want, gone = [], []
            for fid in self._event_fids.get(eid, ()):
                proj = self._projection.get(fid)
                if proj is not None and proj.exists:
                    want.append(self._fast_signature(proj))
            for fid in self._event_deleted.get(eid, ()):
                pre = self._pre.get(fid)
                if pre is not None and pre.exists:
                    gone.append(self._fast_signature(pre))
            added = dict(self._added_proj)
            for tmp in self._event_added.get(eid, ()):
                if tmp in added:
                    want.append(self._fast_signature(added[tmp]))
            per_event_expect[eid] = (want, gone)
        return +expected, unmatched_pre, per_event_expect

    def _verify_by_content(self, verdict: "CommitVerdict") -> None:
        import time
        t0 = time.monotonic()
        fresh = _fresh_layer(self.layer) or self.layer.dataProvider()
        after = self._signatures(fresh, self._area)
        expected, unmatched_pre, per_event = self._expected_after()
        missing, extra, n_res = self._resolve_canon_diff(
            expected, after, unmatched_pre)
        verdict.state = COMMITTED_VERIFIED if not (missing or extra) else COMMITTED_MISMATCH
        has_diff = bool(missing or extra)
        for eid, (want, gone) in per_event.items():
            if not has_diff:
                verdict.per_event[eid] = EV_APPLIED_VERIFIED
                continue
            problems = []
            for sig in want:
                if missing.get(self._canon_from_fast(sig), 0) > 0:
                    problems.append("a feature does not hold the values asked for")
            for sig in gone:
                if extra.get(self._canon_from_fast(sig), 0) > 0:
                    problems.append("a feature that should be gone is still present")
            if problems:
                verdict.per_event[eid] = EV_FAILED_POSTCOMMIT_VERIFICATION
                verdict.per_event_detail[eid] = "; ".join(problems)[:500]
            else:
                verdict.per_event[eid] = EV_APPLIED_VERIFIED
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        tag = "verified" if verdict.state == COMMITTED_VERIFIED else "mismatch"
        flog(f"{self.prefix}commit_guard: event=postcommit_{tag} "
             f"mode=content missing={sum(missing.values())} "
             f"extra={sum(extra.values())} canon_resolved={n_res} "
             f"elapsed_ms={elapsed_ms}",
             "INFO" if verdict.state == COMMITTED_VERIFIED else "ERROR")

    def _refused_by_content(self, verdict: "CommitVerdict") -> None:
        from collections import Counter
        fresh = _fresh_layer(self.layer) or self.layer.dataProvider()
        after = self._signatures(fresh, self._area)
        before = self._before_sigs
        lost, gained, _n_res = self._resolve_canon_diff(
            before, after, Counter())
        if not gained and not lost:
            verdict.state = REFUSED_NOTHING_WRITTEN
            self._attribute_refusal(verdict, set(), set())
            flog(f"{self.prefix}commit_guard: event=commit_refused_nothing_written "
                 f"mode=content errors={verdict.commit_errors[:4]}", "WARNING")
            return
        # Positional FIDs: what the refused commit wrote cannot be mapped
        # back to features reliably enough to revert it blindly. Report it
        # precisely instead of pretending the batch left nothing behind.
        verdict.state = REFUSED_COMPENSATION_FAILED
        verdict.left_modified = (
            [f"{n} feature(s) now in the file that were not before" for n in
             [sum(gained.values())] if n]
            + [f"{n} feature(s) of before no longer in the file" for n in
               [sum(lost.values())] if n])
        # The committed* signals say which features each stage applied,
        # whatever the provider does with its FIDs afterwards.
        self._attribute_refusal(verdict, set(verdict.landed_fids),
                                set(verdict.added_fids), compensated=False)
        flog(f"{self.prefix}commit_guard: event=commit_refused_partial_write "
             f"mode=content gained={sum(gained.values())} lost={sum(lost.values())} "
             f"errors={verdict.commit_errors[:4]}: NOT reverted (positional FIDs)",
             "CRITICAL")

    def _all_events(self):
        return sorted(set(self._event_fids) | set(self._event_added) | set(self._event_deleted))

    def _attribute_refusal(self, verdict: CommitVerdict, changed: set,
                           added: set, compensated: bool = True) -> None:
        errors = "; ".join(verdict.commit_errors[:4]) or "commit refused"
        for eid in self._all_events():
            touched = self._event_fids.get(eid, set()) | self._event_deleted.get(eid, set())
            wrote = bool(touched & changed) or bool(self._event_added.get(eid) and added)
            if wrote:
                verdict.per_event[eid] = (EV_LANDED_THEN_COMPENSATED if compensated
                                          else EV_LANDED_NOT_COMPENSATED)
                verdict.per_event_detail[eid] = (
                    f"written by the refused commit then "
                    f"{'reverted (verified)' if compensated else 'NOT fully reverted'}; "
                    f"provider: {errors}")[:500]
            else:
                verdict.per_event[eid] = EV_FAILED_COMMIT
                verdict.per_event_detail[eid] = f"not applied; provider: {errors}"[:500]

    def _compensate(self, pre: Dict[int, _FeatureState], changed: List[int],
                    added: List[int]) -> List[str]:
        """Revert what a refused commit wrote. Returns what could not be reverted."""
        from qgis.core import QgsFeature
        layer = self.layer
        left: List[str] = []
        if not layer.startEditing():
            return [f"fid {fid}" for fid in changed] + [f"added fid {f}" for f in added]
        fields = layer.fields()
        for fid in changed:
            state = pre[fid]
            if not state.exists:
                continue
            if layer.getFeature(fid).isValid():
                for name, value in state.attrs.items():
                    idx = fields.indexFromName(name)
                    if idx >= 0:
                        layer.changeAttributeValue(fid, idx, value)
                if state.geom is not None:
                    layer.changeGeometry(fid, state.geom)
            else:
                feat = QgsFeature(fields)
                for name, value in state.attrs.items():
                    idx = fields.indexFromName(name)
                    if idx >= 0:
                        feat.setAttribute(idx, value)
                if state.geom is not None:
                    feat.setGeometry(state.geom)
                layer.addFeature(feat)
        for fid in added:
            layer.deleteFeature(fid)
        if not layer.commitChanges():
            errs = list(layer.commitErrors())
            layer.rollBack()
            flog(f"{self.prefix}commit_guard: compensation commit refused {errs[:4]}",
                 "CRITICAL")
        fresh = _fresh_layer(layer) or layer.dataProvider()
        now = _read_states(fresh, sorted(set(changed) | set(added)), self.field_names)
        for fid in changed:
            state = pre[fid]
            cur = now.get(fid)
            if state.exists and (cur is None or not cur.exists):
                # Re-inserted at another fid: accept it if a feature with
                # the pre-image attributes exists.
                if not self._pre_image_present(fresh, state):
                    left.append(f"fid {fid}: deleted, not restored")
                continue
            d = _diff(cur, state, self.field_names, self.auto_fields)
            if d:
                left.append(f"fid {fid}: {'; '.join(d)}")
        for fid in added:
            cur = now.get(fid)
            if cur is not None and cur.exists:
                left.append(f"added fid {fid} still present")
        return left

    def _pre_image_present(self, source, state: _FeatureState) -> bool:
        for feat in source.getFeatures():
            if all(values_equal(feat[n], v) for n, v in state.attrs.items()
                   if n in self.field_names):
                return True
        return False
