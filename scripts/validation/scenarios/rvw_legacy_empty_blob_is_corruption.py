# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_legacy_empty_blob_is_corruption - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a T ou nomme ce qu'il ne sait
pas ; une donnee corrompue n'est jamais presentee comme un etat connu).

Le mecanisme
------------
Depuis le schema 7 la capture ecrit un BLOB de longueur nulle
(``GEOMETRY_NULL_MARKER``) pour dire "cette entite n'avait pas de
geometrie" sur un cote d'UPDATE. Aucune version anterieure n'a jamais ecrit
de BLOB vide : une geometrie absente y etait SQL NULL. Dans un evenement de
schema < 7, un BLOB vide ne peut donc etre qu'une corruption (fichier
abime, outil tiers, conversion ratee). ``core.constants.is_null_geometry_marker``
porte cette regle ; le moteur de Review l'ignorait et lisait tout b"" comme
le marqueur, quel que soit ``event_schema_version``.

L'echec concret pour l'utilisateur
----------------------------------
Un vieux journal (schema 5) dont une ligne a un BLOB de geometrie vide.
Review a une date anterieure a cette ligne : l'entite est affichee "sans
geometrie a cette date" (etat connu), la barre la compte parmi les entites
sans geometrie, rien ne la dit incomplete, et l'export l'ecrit sans
geometrie comme si c'etait son etat. Le point qu'elle avait reellement a T
est perdu sans alerte.

Cas
---
L1  entite preexistante, UPDATE de schema 5 apres T dont le cote "avant"
    (geometry_wkb) est un BLOB vide : a T, etat INCONNU (corruption nommee),
    pas "sans geometrie".
L2  INSERT de schema 5 avant T avec un BLOB vide, renommee apres T : a T,
    geometrie inconnue (corruption nommee).
L3  garde : le meme UPDATE en schema 7 est le marqueur, "sans geometrie a
    T" est un etat connu (ne doit pas regresser).
L4  la meme lecture par le vrai worker sur un journal SQLite (L1 et L3).
L5  meme famille, cas voisin trouve en chemin : entite CREEE sans geometrie
    avant T (INSERT de schema 7, geometry_wkb SQL NULL, que la capture
    ecrit ainsi : geometry_utils.geometry_to_wkb), renommee apres T. Le
    moteur rendait "geometrie inconnue" sans aucune raison : l'entite
    n'etait ni dessinee, ni listee, ni comptee. A T : aucune geometrie,
    etat connu.
L6  la meme histoire ecrite par une capture de schema 5 : geometrie
    inconnue, NOMMEE (geometry_unknown), jamais passee sous silence.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rvw_legacy_empty_blob_is_corruption"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FP = "ds_rvw_legacy_blob"
_T_ISO = "2026-05-01T12:00:00"
_T = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
_PT = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"


def _row(eid, fid, op, created, attrs, geom=None, new_geom=None, version=5):
    return {
        "event_id": eid, "project_fingerprint": "p",
        "datasource_fingerprint": _FP, "layer_id_snapshot": "l",
        "layer_name_snapshot": "points", "provider_type": "ogr",
        "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op, "attributes_json": json.dumps(attrs),
        "geometry_wkb": geom, "geometry_type": "Point",
        "crs_authid": "EPSG:2154", "field_schema_json": "[]",
        "user_name": "u", "session_id": "s", "created_at": created,
        "restored_from_event_id": None, "entity_fingerprint": f"fid:{fid}",
        "event_schema_version": version, "new_geometry_wkb": new_geom,
        "invalidated_at": None,
    }


def _rows():
    return [
        # L1: legacy UPDATE after T, empty old side.
        _row(1, 1, "UPDATE", "2026-06-01T10:00:00+00:00",
             {"changed_only": {}}, geom=b"", new_geom=_PT),
        # L2: legacy INSERT before T with an empty blob, renamed after T.
        _row(2, 2, "INSERT", "2026-04-01T10:00:00+00:00",
             {"all_attributes": {"nom": "Deux"}}, geom=b""),
        _row(3, 2, "UPDATE", "2026-06-01T10:00:00+00:00",
             {"changed_only": {"nom": {"old": "Deux", "new": "Deux2"}}}),
        # L3: the same UPDATE as L1 in schema 7: the null marker.
        _row(4, 3, "UPDATE", "2026-06-01T10:00:00+00:00",
             {"changed_only": {}}, geom=b"", new_geom=_PT, version=7),
        # L5: created WITHOUT geometry before T (schema 7 capture: SQL NULL),
        # renamed after T.
        _row(5, 5, "INSERT", "2026-04-01T10:00:00+00:00",
             {"all_attributes": {"nom": "Cinq"}}, geom=None, version=7),
        _row(6, 5, "UPDATE", "2026-06-01T10:00:00+00:00",
             {"changed_only": {"nom": {"old": "Cinq", "new": "Cinq2"}}}, version=7),
        # L6: the same history written by an older capture (schema 5).
        _row(7, 6, "INSERT", "2026-04-01T10:00:00+00:00",
             {"all_attributes": {"nom": "Six"}}, geom=None),
        _row(8, 6, "UPDATE", "2026-06-01T10:00:00+00:00",
             {"changed_only": {"nom": {"old": "Six", "new": "Six2"}}}),
    ]


def _events():
    from recoverland.core.audit_backend import AuditEvent
    return [AuditEvent(**r) for r in _rows()]


def _describe(feat):
    from recoverland.core.temporal_snapshot_engine import has_null_geometry
    if feat is None:
        return None
    return {
        "geom": None if feat.geom_wkb is None else len(feat.geom_wkb),
        "null_geometry": has_null_geometry(feat),
        "unresolved": feat.unresolved,
        "geom_from_live": feat.geom_from_live,
    }


class _Journal:
    def __init__(self, path):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_blob_")
    path = os.path.join(tmpdir, "j.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    cols = ", ".join(AUDIT_EVENT_COLUMNS)
    ph = ",".join("?" for _ in AUDIT_EVENT_COLUMNS)
    with conn:
        conn.executemany(
            f"INSERT INTO audit_event ({cols}) VALUES ({ph})",  # nosec B608
            [tuple(r[c] for c in AUDIT_EVENT_COLUMNS) for r in _rows()])
    conn.close()
    ctx.data["tmpdir"] = tmpdir
    ctx.data["journal"] = path
    flog(f"rvw_legacy_empty_blob_is_corruption setup: trace_id={ctx.trace_id} "
         f"journal={path}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.temporal_snapshot_engine import reconstruct_snapshot_at
    d = ctx.data
    try:
        res = reconstruct_snapshot_at({_FP: _events()}, _T, trace_id=ctx.trace_id)
        feats = res.features.get(_FP, {})
        d["engine"] = {k: _describe(feats.get(k)) for k in ("fid:1", "fid:2", "fid:3", "fid:5", "fid:6")}
        d["engine_n_unresolved"] = res.n_unresolved
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        d["crash"] = repr(exc)
    import recoverland.widgets.snapshot_rebuild_worker as wmod
    captured = {}
    worker = wmod.SnapshotRebuildWorker(
        _Journal(d["journal"]),
        [{"fingerprint": _FP, "layer_id": "", "layer_name": "points",
          "storage_crs": "EPSG:2154"}],
        _T_ISO, trace_id=uuid.uuid4().hex[:8])
    worker.result_ready.connect(lambda t, r: captured.__setitem__("result", r))
    worker.error.connect(lambda t, e: captured.__setitem__("error", e))
    worker.run()
    wres = captured.get("result")
    wfeats = wres.features.get(_FP, {}) if wres is not None else {}
    d["worker"] = {k: _describe(wfeats.get(k)) for k in ("fid:1", "fid:3")}
    d["worker_error"] = captured.get("error")
    flog(f"rvw_legacy_empty_blob_is_corruption run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'tmpdir'}, default=str)}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _corruption_named(desc) -> bool:
    return (desc is not None and desc["geom"] is None
            and not desc["null_geometry"] and not desc["geom_from_live"]
            and "geometry_corrupt" in (desc["unresolved"] or ""))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    eng = d.get("engine") or {}
    wrk = d.get("worker") or {}
    crash = f" crash={d.get('crash')}" if d.get("crash") else ""
    out = []
    out.append((
        "l1_legacy_empty_old_side_is_corruption_not_no_geometry",
        _corruption_named(eng.get("fid:1")),
        f"L1 a T={eng.get('fid:1')} : un BLOB vide dans un evenement de "
        f"schema 5 est une corruption, l'entite doit etre 'etat inconnu' "
        f"(geometry_corrupt), pas 'sans geometrie a cette date'{crash}",
    ))
    out.append((
        "l2_legacy_empty_insert_geometry_is_corruption",
        _corruption_named(eng.get("fid:2")),
        f"L2 a T={eng.get('fid:2')} : l'INSERT de schema 5 n'a jamais pu "
        f"ecrire un BLOB vide pour 'sans geometrie'{crash}",
    ))
    l3 = eng.get("fid:3") or {}
    out.append((
        "l3_schema7_marker_stays_a_known_no_geometry_state",
        l3.get("null_geometry") is True and not l3.get("unresolved"),
        f"L3 a T={eng.get('fid:3')} : en schema 7 le BLOB vide est le "
        f"marqueur, 'sans geometrie a T' est un etat connu{crash}",
    ))
    l5 = eng.get("fid:5") or {}
    out.append((
        "l5_schema7_insert_without_geometry_is_known_no_geometry",
        l5.get("null_geometry") is True and not l5.get("unresolved"),
        f"L5 a T={eng.get('fid:5')} : creee sans geometrie (capture v7), "
        f"elle n'avait pas de geometrie a T ; sinon elle disparait de Review "
        f"sans etre ni listee ni comptee{crash}",
    ))
    l6 = eng.get("fid:6") or {}
    out.append((
        "l6_legacy_insert_without_geometry_is_named_unknown",
        l6.get("geom") is None and "geometry_unknown" in (l6.get("unresolved") or ""),
        f"L6 a T={eng.get('fid:6')} : un INSERT de schema 5 sans geometrie "
        f"doit etre compte comme etat incomplet, pas passe sous silence{crash}",
    ))
    out.append((
        "l4_worker_names_the_corruption",
        _corruption_named(wrk.get("fid:1"))
        and (wrk.get("fid:3") or {}).get("null_geometry") is True,
        f"worker sur journal SQLite : L1={wrk.get('fid:1')} L3={wrk.get('fid:3')} "
        f"erreur={d.get('worker_error')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_legacy_empty_blob_is_corruption.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
