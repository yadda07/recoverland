# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Reconcile commits a crash interrupted, against the provider itself.

When QGIS dies between beforeCommitChanges and the end of the commit, the
outbox still holds the pre-commit capture (``prepare``) and the next start
records the commit INTERRUPTED in ``capture_commit`` with those snapshots.
Whether the provider applied the edit is unknown at that point; the
provider is the only one that can say. This module asks it, feature by
feature, the next time the layer is connected:

* DELETE snapshot, feature gone          -> the delete landed: DELETE event
* DELETE snapshot, feature == pre-image  -> not applied: nothing
* UPDATE snapshot, feature == pre-image  -> not applied: nothing
* UPDATE snapshot, feature differs       -> it changed after the capture:
                                            UPDATE event pre-image -> current
* ADDED content found in the provider
  once, and not on a feature that held
  it before the commit (``twins``)       -> the insert landed: INSERT event
* ADDED content found nowhere, or only
  on those recorded twins                -> not applied: nothing
* anything else                          -> unresolved, counted and kept

Every verdict reads the source without the layer's filter (`_Source`).
ADDED content is compared as the source stores it, not as it was entered
(`storage_decimals`, `_addition_match`): a rounded number, a text without
its trailing spaces is the addition; a text the source may have cut is
never "not applied".

An UPDATE event follows the schema 7 rule of the direct capture: a NULL
side of a changed geometry is ``GEOMETRY_NULL_MARKER`` (None reads
"geometry unchanged"), and an UPDATE whose pre-commit state was read
without its geometry (``geometry_captured`` False) is compared on its
attributes only and carries no geometry.

The commit then ends RECONCILED with a detail naming the four counts, and
the events carry ``restored_from_event_id = NULL`` like any user edit: a
rewind can undo them. Positional-FID providers are reconciled by content
only for additions; a DELETE/UPDATE on them stays unresolved rather than
guessed (FIDs are recycled after a repack).

A commit is reconciled once. In this process, a commit already handed
over is not rebuilt by the next layer connected on the same source
(`_CLAIMED`); across processes, a rebuilt event has the same ``event_uid``
every time (`_reconciled_uid`), so a second arrival is a no-op.

GUI thread only (reads the layer).
"""
from __future__ import annotations

import base64
import json
import math
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .logger import flog


# Commits a reconciliation of THIS process has rebuilt and handed over,
# keyed by (journal, commit_id). The ledger says RECONCILED only once the
# writer thread has committed the batch: until then every other
# connect_layer on the same source (a second layer on the same table, a
# layer reconnected after a repair) read the commit still INTERRUPTED and
# journalled each of its changes again, with an alert each time (measured,
# scripts/validation/scenarios/tx_capture_reconcile_once.py).
_CLAIMED: set = set()
_CLAIM_LOCK = threading.Lock()

# Namespace of the event_uid of a reconciled event (`_reconciled_uid`).
_RECONCILED_UID_NS = uuid.UUID("5f0f3c2e-8a51-4d0e-9a57-2f6f1b1d7c41")


def _reconciled_uid(commit_id: str, index: int) -> str:
    """event_uid of the event rebuilt from snapshot *index* of *commit_id*.

    A snapshot gives at most one event, and the ledger keeps the snapshots
    in one order: the uid is the same whichever process rebuilds it. A
    random uid let a second reconciliation of the same commit land beside
    the first one: a first batch still waiting in the outbox when QGIS
    stopped, a restart whose replay of that outbox failed (journal held by
    another process) and which reconciled again, then the replay that
    finally integrated the first batch: every change twice.
    ON CONFLICT(event_uid) now makes that arrival a no-op.
    """
    return uuid.uuid5(_RECONCILED_UID_NS, f"{commit_id}:{index}").hex


def _claim_key(journal_path: str, commit_id: str) -> tuple:
    return (os.path.normcase(os.path.abspath(journal_path)), commit_id)


def _unb64(value):
    if isinstance(value, str) and value.startswith("b64:"):
        return base64.b64decode(value[4:])
    return value


def _geom(wkb):
    from qgis.core import QgsGeometry
    wkb = _unb64(wkb)
    if not wkb:
        return None
    g = QgsGeometry()
    g.fromWkb(bytes(wkb))
    return None if g.isNull() else g


def _same(feature, attrs: dict, wkb, geometry: bool = True) -> bool:
    """*feature* holds every value of *attrs* and, when *geometry*, *wkb*.

    ``geometry=False``: the recorded state never read the geometry (an
    attribute-only UPDATE is read with NO_GEOMETRY, ``geometry_captured``
    False). Its None means "not read": compared with the feature's
    geometry it made every such feature differ, and an edit that never
    reached the provider was counted as landed.
    """
    from .commit_guard import values_equal
    for name, value in (attrs or {}).items():
        try:
            current = feature[name]
        except (KeyError, IndexError):
            continue
        if not values_equal(current, _decode_journal_value(value)):
            return False
    return not geometry or _geometry_same(feature, wkb)


def _geometry_same(feature, wkb) -> bool:
    """*feature* holds the geometry *wkb* (None or the NULL marker: none)."""
    from .commit_guard import geometry_equal
    expected = _geom(wkb)
    current_geom = feature.geometry()
    if expected is None and (current_geom is None or current_geom.isNull()):
        return True
    return geometry_equal(current_geom, expected)


def _geometry_read(snap: dict, old_wkb, feature) -> Optional[bool]:
    """Whether the UPDATE record *snap* read the feature's geometry.

    The prepare record says it (``geometry_captured``). A record that does
    not say it (a build before the key) read a geometry when it holds one;
    when it holds None, "not read" and "no geometry before the commit" both
    fit. They agree while the feature has no geometry now (the commit did
    not change it either way): False. Otherwise None, undecidable: taking
    it for a geometry the commit added makes its undo erase the user's
    geometry.
    """
    captured = snap.get("geometry_captured")
    if captured is not None:
        return bool(captured)
    if old_wkb is not None:
        return True
    current = feature.geometry()
    if current is None or current.isNull():
        return False
    return None


def _decode_journal_value(value):
    return value


def _content_matches(layer, attrs: dict, wkb) -> list:
    """Features of *layer* holding exactly *attrs* (non-NULL ones) and *wkb*."""
    out = []
    for feat in _content_candidates(layer.dataProvider(), attrs, wkb):
        out.append(feat)
        if len(out) > 1:
            break
    return out


def _near(provider, wkb):
    """Features of *provider* in the box of *wkb* (all of them without one)."""
    from qgis.core import QgsFeatureRequest
    request = QgsFeatureRequest()
    geom = _geom(wkb)
    if geom is not None:
        bb = geom.boundingBox()
        bb.grow(max(bb.width(), bb.height(), 1e-6) * 1e-6 + 1e-9)
        request.setFilterRect(bb)
    return provider.getFeatures(request)


def _content_candidates(provider, attrs: dict, wkb):
    """Every feature of *provider* holding *attrs* (non-NULL ones) and *wkb*."""
    wanted = {k: v for k, v in (attrs or {}).items() if v is not None}
    for feat in _near(provider, wkb):
        if _same(feat, wanted, wkb):
            yield feat


# ----------------------------------------------------------------------
# What the source keeps of an addition
# ----------------------------------------------------------------------
# An addition is recorded as it was entered; the source does not always
# keep that (QGIS 4.2.1, measured, tx_capture_reconcile_coerced.py): a real
# field with a declared precision is rounded to it (Shapefile N(12,2),
# MapInfo: 10/3 -> 3.33, the field calculator, $area); an integer field
# gets the value rounded (3.7 -> 4 on Shapefile, GeoPackage, MapInfo and
# Spatialite, while the edit buffer keeps 3.7); a Shapefile drops the
# trailing spaces of a text and cuts it past 254 characters, MapInfo at
# the field width. Compared exactly, such an addition was found nowhere
# once it had landed: "not applied", no INSERT, and a rewind before the
# crash could never remove it.
#
# Matching it as stored is sound only if the twin search (the features
# that held the content BEFORE the commit) is at least as wide: an
# existing feature equal to the addition once rounded, missing from the
# twins, passed for the addition, and undoing that INSERT deleted it.
# The twin search compares with `twin_matcher`, a superset of
# `_addition_match` level 2; the prepare record says so (``rounding``).

_INT_TYPES = frozenset({2, 3, 4, 5})   # QMetaType Int, UInt, LongLong, ULongLong
_DOUBLE_TYPE = 6                        # QMetaType Double
_STRING_TYPE = 10                       # QMetaType QString


def text_widths(fields) -> Dict[str, int]:
    """Declared width of each text field of *fields* that has one."""
    out: Dict[str, int] = {}
    for i in range(fields.count()):
        field = fields.at(i)
        kind = field.type()
        try:
            kind = int(getattr(kind, "value", kind))
        except (TypeError, ValueError):
            continue
        if kind == _STRING_TYPE and field.length() > 0:
            out[field.name()] = int(field.length())
    return out


def storage_decimals(fields) -> Dict[str, int]:
    """Decimals each numeric field of *fields* may be rounded to on write.

    An integer field: 0. A real field with a declared precision: that
    precision. Other fields are not listed (stored as given).
    """
    out: Dict[str, int] = {}
    for i in range(fields.count()):
        field = fields.at(i)
        kind = field.type()
        try:
            kind = int(getattr(kind, "value", kind))
        except (TypeError, ValueError):
            continue
        if kind in _INT_TYPES:
            out[field.name()] = 0
        elif kind == _DOUBLE_TYPE and field.precision() > 0:
            out[field.name()] = int(field.precision())
    return out


def _number(value) -> Optional[float]:
    from .serialization import serialize_value
    try:
        out = serialize_value(value)
    except Exception:  # noqa: BLE001 - comparison must not raise
        return None
    if isinstance(out, bool) or not isinstance(out, (int, float)):
        return None
    out = float(out)
    return None if math.isnan(out) or math.isinf(out) else out


def _within(held, expected, decimals: int) -> bool:
    """*held* is *expected* rounded to *decimals*: half a unit away at most.

    Half a unit: what rounding moves a value by, whatever the tie rule of
    the provider (0.125 -> 0.12 on Shapefile, measured).
    """
    a, b = _number(held), _number(expected)
    if a is None or b is None:
        return False
    half = 0.5 * 10.0 ** -decimals
    return abs(a - b) <= half * (1 + 1e-9) + abs(b) * 1e-15


def _may_round_alike(value, expected, decimals: int) -> bool:
    """*value* and *expected*, both rounded to *decimals* on write, may be
    held as the same number by the reconciliation.

    Each moves by up to half a unit, and `values_equal` itself grants the
    stored side a relative 1e-9 (a thousandth at a million): the two
    compose. A property check on random pairs up to 1e9 found the whole
    unit alone too narrow at large magnitudes.
    """
    a, b = _number(value), _number(expected)
    if a is None or b is None:
        return False
    unit = 10.0 ** -decimals
    return abs(a - b) <= unit * (1 + 2e-9) + 2e-9 * max(abs(a), abs(b)) + 1e-12


def rounding_rules(attributes, decimals: Dict[str, int]) -> Dict[str, int]:
    """Fields of an addition whose value the source rounds on write: {name: decimals}."""
    out: Dict[str, int] = {}
    for name, places in decimals.items():
        value = _number((attributes or {}).get(name))
        if value is not None and abs(round(value, places) - value) > abs(value) * 1e-15:
            out[name] = places
    return out


def _instant(value) -> Optional[int]:
    """Epoch milliseconds of a date-time (live value or journal text), else None."""
    if isinstance(value, str) and not (len(value) >= 16 and value[4:5] == "-"):
        return None
    from .serialization import datetime_instant_ms, journal_text_instant_ms
    try:
        ms = datetime_instant_ms(value)
        return ms if ms is not None else journal_text_instant_ms(value)
    except Exception:  # noqa: BLE001 - comparison must not raise
        return None


def _may_store_alike(value, expected, decimals: Optional[int]) -> bool:
    """The twin search's comparison of one value: a superset of what the
    reconciliation matches once both are stored.

    *value* is a stored value (a feature of the provider) or one the same
    commit is about to write (a feature it modifies), *expected* the
    addition's. A stored value matches the addition when it holds it as
    stored (`_held_as_stored`: rounded within half a unit, or without the
    trailing spaces); a value about to be written matches when their stored
    forms may coincide: numbers within a whole unit (`_may_round_alike`), a
    text of which the addition, trailing spaces aside, is a prefix (a cut
    or trimmed copy; the empty text included, a cut made only of spaces
    trims to it), date-times less than two seconds apart (stored to the
    second).
    """
    from .commit_guard import values_equal
    if values_equal(value, expected):
        return True
    if decimals is not None and _may_round_alike(value, expected, decimals):
        return True
    if isinstance(value, str) and isinstance(expected, str):
        if value.rstrip(" ").startswith(expected.rstrip(" ")):
            return True
    a, b = _instant(value), _instant(expected)
    return a is not None and b is not None and abs(a - b) < 2000


def twin_matcher(decimals: Dict[str, int]):
    """``same(feature, attrs, wkb)`` for the twin search of the additions.

    *decimals*: `storage_decimals` of the source's fields at the time of
    the search. Geometry compared as `_same` does.
    """
    def same(feature, attrs: dict, wkb) -> bool:
        for name, value in (attrs or {}).items():
            try:
                current = feature[name]
            except (KeyError, IndexError):
                continue
            if not _may_store_alike(current, value, decimals.get(name)):
                return False
        return _geometry_same(feature, wkb)
    return same


def _held_as_stored(held, expected, decimals: Optional[int], trims: bool) -> bool:
    """*held* (in the source) is *expected* (entered) as the source stores it."""
    if decimals is not None and _within(held, expected, decimals):
        return True
    return (trims and isinstance(held, str) and isinstance(expected, str)
            and held == expected.rstrip(" "))


def _held_loosely(held, expected, decimals: Optional[int],
                  width: Optional[int]) -> bool:
    """*held* may be *expected* transformed in a way no match can prove.

    A number rounded to the precision the field declares now; a text
    without its trailing spaces; a text cut at the width the field
    declares now (*width*, in bytes as a DBF or a MapInfo table counts
    it): a prefix of *expected* reaching that width, give or take the
    character the cut split (read back as U+FFFD, or dropped) and the
    trailing spaces a Shapefile trims. A shorter prefix is another text,
    not a cut one.
    """
    if decimals is not None and _within(held, expected, decimals):
        return True
    if not (isinstance(held, str) and isinstance(expected, str)):
        return False
    if held == expected.rstrip(" "):
        return True
    if not width:
        return False
    kept = held.rstrip("\ufffd").rstrip(" ")
    if not kept or len(kept) >= len(expected) or not expected.startswith(kept):
        return False
    end = len(kept)
    while end < len(expected) and expected[end] == " ":
        end += 1
    return len(expected[:end].encode("utf-8")) >= width - 3


def _addition_match(feature, attrs: dict, wkb, rounding: Dict[str, int],
                    trims: bool, source: "_Source") -> int:
    """How *feature* holds the addition *attrs* / *wkb*.

    2: exactly, or as the source stores it (*rounding*: the fields the
    record says the source rounds; *trims*: trailing spaces dropped, only
    for a record whose twin search compared the same way). 1: only
    loosely (`_held_loosely`, with the decimals and widths the fields of
    *source* declare now): it may be the addition, nothing proves it.
    0: it is not.
    """
    from .commit_guard import values_equal
    level = 2
    for name, value in attrs.items():
        try:
            current = feature[name]
        except (KeyError, IndexError):
            continue
        if values_equal(current, value):
            continue
        if level == 2 and _held_as_stored(current, value, rounding.get(name), trims):
            continue
        if _held_loosely(current, value, source.decimals.get(name),
                         source.widths.get(name)):
            level = 1
            continue
        return 0
    return level if _geometry_same(feature, wkb) else 0


def fid_is_stored_key(feature, pk: list, rowid_fids: bool) -> bool:
    """The FID of *feature* is the key the source stores, not a session number.

    True when the provider's single key field holds the FID itself
    (Spatialite INTEGER PRIMARY KEY, a GeoPackage fid, a PostgreSQL integer
    key), or, with no key field, on an SQLite rowid (``rowid_fids``: OGR,
    Spatialite). A FID a provider makes up per session (a PostgreSQL text
    or composite key) can name another row in another session.
    """
    if len(pk) == 1:
        try:
            return int(feature.attribute(pk[0])) == feature.id()
        except (TypeError, ValueError, KeyError):
            return False
    return not pk and rowid_fids


class _Source:
    """What the verdicts read: the source WITHOUT the layer's filter.

    A layer filter (Layer > Filter) is a view, not the data. Read through
    it, an addition the filter hides was "not applied" (it had landed and
    never got its INSERT), and on Spatialite and PostgreSQL, which apply
    the filter even to a read by FID, a feature it hides read as "gone": a
    deletion that never happened was journalled as landed (measured,
    scripts/validation/scenarios/tx_capture_reconcile_filtered.py). The
    verdicts read the provider without the filter that the twin search and
    the read-back of additions already use (`edit_tracker._unfiltered_provider`).
    A feature read there by FID is kept only when its FID is the stored
    key: another provider's session numbering can name another row.
    ``blind``: the layer is filtered and no provider without the filter
    could be built; an absence then proves nothing.
    """

    def __init__(self, layer):
        self.provider = layer.dataProvider()
        self.second = False
        self.blind = False
        if self.provider.subsetString() or "":
            from .edit_tracker import _unfiltered_provider
            other = _unfiltered_provider(layer)
            if other is None:
                self.blind = True
            else:
                self.provider, self.second = other, True
        self.pk = list(self.provider.pkAttributeIndexes())
        self.rowid_fids = layer.providerType() in ("ogr", "spatialite")
        self.decimals = storage_decimals(self.provider.fields())
        self.widths = text_widths(self.provider.fields())
        # Through a second provider a FID designates a row only as its key.
        self.fids_blind = self.blind or (self.second and not (
            len(self.pk) == 1 or (not self.pk and self.rowid_fids)))

    def read_fid(self, fid):
        """``(feature or None, why the answer proves nothing or "")``."""
        from qgis.core import QgsFeatureRequest
        feat = next(self.provider.getFeatures(QgsFeatureRequest(int(fid))), None)
        if feat is None or not feat.isValid():
            return None, ("hidden_by_layer_filter" if self.fids_blind else "")
        if self.second and not fid_is_stored_key(feat, self.pk, self.rowid_fids):
            return None, "fid_not_the_stored_key"
        return feat, ""


def _addition_verdict(source: _Source, snap: dict, stable: bool, deleted: set):
    """Did the addition *snap* land? ``(verdict, feature_or_reason)``.

    ("landed", feature), ("not_applied", None) or ("unresolved", why).

    The addition is recognised by its content: what the user entered and
    its geometry, never the fields left to the source (``unset_fields``,
    the key among them). Content alone cannot tell the addition from a
    feature that held the same content BEFORE the commit (a duplicated or
    pasted feature): before the provider wrote, the capture searched them
    and recorded their FIDs (``twins``, [] when it checked and found none,
    ``n_twins`` their number when the list is cut). A match is set aside
    as one of those only when its FID still designates the same row: FIDs
    stable, the FID the stored key, and no deletion of the same commit
    that could have freed it (the copy may have taken the number of the
    original it replaced). What remains decides: nothing, the addition did
    not land (unless the source is filtered and blind, see `_Source`);
    one feature, it did, provided the twin list is complete (otherwise
    that one may be an unlisted original); more, unresolved.

    The content is matched as the source stores it (`_addition_match`)
    when the record says its twins were searched the same way
    (``rounding``: the fields the source rounds, possibly empty); a record
    without it (an earlier build) is matched exactly, its twins having
    been searched exactly. A feature that holds the content only loosely
    (a text the source may have cut at the field width; for an earlier
    record, a rounded number or trimmed text too) turns "not applied" into
    unresolved ``content_coerced_by_source``: nothing proves it is the
    addition, nothing proves it is not.
    """
    if not isinstance(snap.get("attributes"), dict):
        return "unresolved", "content_not_recorded"
    unset = set(snap.get("unset_fields") or [])
    attrs = {k: v for k, v in snap["attributes"].items()
             if k not in unset and v is not None}
    wkb = _unb64(snap.get("geometry_wkb"))
    rounding = snap.get("rounding")
    trims = isinstance(rounding, dict)
    rounding = rounding if trims else {}
    twins = snap.get("twins")
    listed = set(twins) if isinstance(twins, list) else set()
    if not isinstance(twins, list):
        complete, why = False, "twins_not_checked"
    elif snap.get("n_twins") is not None:
        complete, why = int(snap["n_twins"]) <= len(twins), "twin_list_cut"
    else:
        from .edit_tracker import EditSessionTracker
        complete = len(twins) < EditSessionTracker._TWIN_FIDS_KEPT
        why = "twin_list_cut"
    remaining, coerced = [], 0
    for feat in _near(source.provider, wkb):
        level = _addition_match(feat, attrs, wkb, rounding, trims, source)
        if not level:
            continue
        if (feat.id() in listed and stable and feat.id() not in deleted
                and fid_is_stored_key(feat, source.pk, source.rowid_fids)):
            continue
        if level == 1:
            coerced += 1
            continue
        remaining.append(feat)
        if len(remaining) > 1:
            return "unresolved", "several_features_hold_the_content"
    if not remaining:
        if source.blind:
            return "unresolved", "hidden_by_layer_filter"
        if coerced:
            return "unresolved", "content_coerced_by_source"
        return "not_applied", None
    if remaining[0].id() in listed:
        return "unresolved", "twin_fid_not_provable"
    if not complete:
        return "unresolved", why
    return "landed", remaining[0]


def interrupted_commits(conn: sqlite3.Connection, fingerprints) -> List[dict]:
    fps = [f for f in fingerprints if f]
    if not fps:
        return []
    ph = ",".join("?" for _ in fps)
    try:
        rows = conn.execute(
            "SELECT commit_id, layer_id, layer_name, datasource_fingerprint, "  # nosec B608
            "session_id, prepared_at, snapshots_json FROM capture_commit "
            "WHERE state = 'INTERRUPTED' AND datasource_fingerprint IN (" + ph + ")",
            fps).fetchall()
    except sqlite3.Error:
        return []
    out = []
    for cid, lid, lname, ds, sess, prepared, snaps in rows:
        try:
            payload = json.loads(snaps or "[]")
        except ValueError:
            payload = []
        out.append({"commit_id": cid, "layer_id": lid, "layer_name": lname,
                    "ds": ds, "session_id": sess, "prepared_at": prepared,
                    "snapshots": payload})
    return out


def _count(counts: Dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def reconcile_layer(layer, write_queue, journal_path: str,
                    datasource_fp: str) -> Dict[str, int]:
    """Reconcile every INTERRUPTED commit of *layer*'s datasource."""
    from .audit_backend import AuditEvent
    from .identity import compute_entity_fingerprint, compute_project_fingerprint
    from .serialization import (
        serialize_attributes, build_full_snapshot, compute_update_delta,
        serialize_field_schema,
    )
    from .geometry_utils import geometry_to_wkb, extract_geometry_type, extract_crs_authid
    from .commit_guard import fid_is_stable
    from .user_identity import resolve_user_name
    from .sqlite_schema import CURRENT_SCHEMA_VERSION
    from .constants import GEOMETRY_NULL_MARKER
    from .edit_tracker import UNPROVEN_INSERT_KIND
    from .identity import compute_feature_identity
    from .write_queue import CommitInfo, EnqueueOutcome
    from .journal_manager import sqlite_readonly_uri

    totals = {"commits": 0, "landed": 0, "not_applied": 0, "unresolved": 0}
    if not journal_path:
        return totals
    try:
        from .datasource_alias import expand_fingerprints
        conn = sqlite3.connect(sqlite_readonly_uri(journal_path), uri=True)
        try:
            family = set(expand_fingerprints(conn, datasource_fp)) | {datasource_fp}
            commits = interrupted_commits(conn, family)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - a reconciliation never blocks capture
        flog(f"crash_reconcile: cannot read interrupted commits: {exc}", "ERROR")
        return totals
    if not commits:
        return totals
    provider = layer.dataProvider()
    field_names = [f.name() for f in provider.fields()]
    stable = fid_is_stable(layer)
    source = _Source(layer)
    now = datetime.now(timezone.utc).isoformat()
    geom_type, crs = extract_geometry_type(layer), extract_crs_authid(layer)
    field_schema = serialize_field_schema(layer.fields())
    proj_fp = compute_project_fingerprint()
    user = resolve_user_name()

    def _base(op, identity, attrs_json, old_wkb, new_wkb, session_id, commit_id,
              index):
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=datasource_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer.name(), provider_type=provider.name(),
            feature_identity_json=identity, operation_type=op,
            attributes_json=attrs_json, geometry_wkb=old_wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=new_wkb, commit_id=commit_id,
            event_uid=_reconciled_uid(commit_id, index))

    additions = ("INSERT", UNPROVEN_INSERT_KIND)
    for commit in commits:
        claim = _claim_key(journal_path, commit["commit_id"])
        with _CLAIM_LOCK:
            claimed = claim in _CLAIMED
        if claimed:
            flog(f"crash_reconcile: event=commit_already_reconciled "
                 f"commit={commit['commit_id']} layer={layer.name()!r}: rebuilt "
                 f"and handed over earlier in this session (another layer on "
                 f"the same source), not rebuilt again", "INFO")
            continue
        landed = not_applied = 0
        why_unresolved: Dict[str, int] = {}
        events = []
        # A twin FID freed by a deletion of this same commit may now be the
        # copy's (see _addition_verdict).
        deleted = {s.get("fid") for s in commit["snapshots"]
                   if isinstance(s, dict) and s.get("kind") == "DELETE"}
        for index, snap in enumerate(commit["snapshots"]):
            kind = snap.get("kind")
            fid = snap.get("fid")
            attrs = snap.get("attributes") or {}
            old_wkb = _unb64(snap.get("geometry_wkb"))
            identity = snap.get("identity") or json.dumps({"fid": fid})
            if kind not in additions and (not stable or fid is None):
                _count(why_unresolved, "positional_fid" if not stable else "no_fid")
                continue
            feat, exists = None, False
            if kind in ("DELETE", "UPDATE"):
                feat, unprovable = source.read_fid(fid)
                if unprovable:
                    _count(why_unresolved, unprovable)
                    continue
                exists = feat is not None
            if kind == "DELETE":
                if not exists:
                    events.append(_base("DELETE", identity, build_full_snapshot(attrs),
                                        old_wkb, None, commit["session_id"],
                                        commit["commit_id"], index))
                    landed += 1
                elif _same(feat, attrs, old_wkb):
                    not_applied += 1
                else:
                    _count(why_unresolved, "deleted_feature_changed")
            elif kind == "UPDATE":
                read = _geometry_read(snap, old_wkb, feat) if exists else None
                if not exists:
                    _count(why_unresolved, "updated_feature_gone")
                elif read is None:
                    _count(why_unresolved, "geometry_read_unknown")
                elif _same(feat, attrs, old_wkb, geometry=read):
                    not_applied += 1
                else:
                    current = serialize_attributes(feat, field_names)
                    delta = compute_update_delta(attrs, current, list(current.keys()))
                    geom_changed = read and not _geometry_same(feat, old_wkb)
                    if delta is None and not geom_changed:
                        # The comparison saw a difference no delta expresses:
                        # an event would say "changed" with nothing in it.
                        _count(why_unresolved, "difference_not_expressible")
                        continue
                    old_side = new_side = None
                    if geom_changed:
                        # Schema 7, as the direct capture: a NULL side is the
                        # explicit marker; None reads "geometry unchanged".
                        old_side = old_wkb or GEOMETRY_NULL_MARKER
                        new_side = (geometry_to_wkb(feat.geometry())
                                    or GEOMETRY_NULL_MARKER)
                    events.append(_base(
                        "UPDATE", identity, delta or '{"changed_only": {}}',
                        old_side, new_side, commit["session_id"],
                        commit["commit_id"], index))
                    landed += 1
            elif kind in additions:
                verdict, found = _addition_verdict(source, snap, stable, deleted)
                if verdict == "landed":
                    ident = compute_feature_identity(layer, found)
                    wkb = geometry_to_wkb(found.geometry())
                    events.append(_base(
                        "INSERT", ident,
                        build_full_snapshot(serialize_attributes(found, field_names)),
                        wkb, wkb, commit["session_id"], commit["commit_id"],
                        index))
                    landed += 1
                elif verdict == "not_applied":
                    not_applied += 1
                else:
                    _count(why_unresolved, found)
            else:
                _count(why_unresolved, f"unknown_kind:{kind}")
        unresolved = sum(why_unresolved.values())
        # Why each unresolved one is: the user is told a count, the ledger
        # and the log say what could not be proven.
        reasons = ", ".join(f"{k}={v}" for k, v in sorted(why_unresolved.items()))
        state = "RECONCILED"
        detail = (f"rebuilt from the provider after a crash: landed={landed} "
                  f"not_applied={not_applied} unresolved={unresolved}"
                  + (f" ({reasons})" if reasons else "")
                  + ("" if stable else " (positional FIDs: not guessed)"))
        info = CommitInfo(commit_id=commit["commit_id"], state=state,
                          layer_id=layer.id(), layer_name=layer.name(),
                          datasource_fingerprint=datasource_fp,
                          session_id=commit["session_id"],
                          prepared_at=commit["prepared_at"], detail=detail)
        # Claimed before the hand-over: from here on another layer on the
        # source must not rebuild it. Released only when nothing was written
        # anywhere (the ledger still says INTERRUPTED: the next connection
        # tries again).
        with _CLAIM_LOCK:
            _CLAIMED.add(claim)
        try:
            outcome = write_queue.enqueue(events, commit=info,
                                          journal_path=journal_path)
        except Exception:
            with _CLAIM_LOCK:
                _CLAIMED.discard(claim)
            raise
        if outcome is EnqueueOutcome.REFUSED_NO_DURABLE_PATH:
            with _CLAIM_LOCK:
                _CLAIMED.discard(claim)
        flog(f"crash_reconcile: event=commit_reconciled commit={commit['commit_id']} "
             f"layer={layer.name()!r} {detail} outcome={getattr(outcome, 'value', outcome)}",
             "WARNING")
        totals["commits"] += 1
        totals["landed"] += landed
        totals["not_applied"] += not_applied
        totals["unresolved"] += unresolved
    return totals
