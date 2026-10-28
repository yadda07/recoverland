# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_undo_compensation_state_at_t - RecoverLand validation runtime.

Invariant: I-RVW-ASOF + I-3 (Review montre a la date T l'etat que le Rewind
reconstruirait pour la meme date : une compensation compte de son
created_at a son invalidated_at, et une trace de schema 7 qui reference une
trace l'annule).

Le mecanisme
------------
Un Rewind ecrit des traces (``restored_from_event_id``) ; une Annulation
(Undo) ne cree aucun evenement : elle remet la couche en place puis pose
``invalidated_at`` sur les traces qu'elle a defaites
(recover_dialog._invalidate_trace_events). La compensation a donc ete EN
VIGUEUR de son created_at a son invalidated_at. C'est la regle de
rewind_dedup.traces_in_effect(traces, happened), celle du Rewind.

Le worker de Review filtrait ``invalidated_at IS NULL`` dans toutes ses
requetes : une trace annulee disparaissait de toute l'histoire, comme si le
Rewind n'avait jamais eu lieu, et l'Annulation, qui n'a pas d'evenement,
n'etait pas non plus un changement "apres T".

L'echec concret pour l'utilisateur
----------------------------------
Lundi il renomme une parcelle A -> B. Mardi un Rewind la remet a A. Mercredi
il annule ce Rewind (B de nouveau). Review a mardi midi : la parcelle
s'appelait A ce jour-la (c'est ce qu'un Rewind vers mardi midi
reconstruirait), Review affiche B. De meme une entite supprimee par le
Rewind puis rendue par l'Annulation n'est pas dite absente, et une entite
re-inseree par le Rewind puis re-supprimee par l'Annulation n'est pas
montree.

Cas (un journal SQLite par cas, lignes formees comme la capture v7 et
build_restore_trace_event ; le vrai worker, lance en synchrone)
------------------------------------------------------------------------
U1  UPDATE A->B (t1), trace UPDATE (t2), annulee (t3). T entre t2 et t3 :
    nom=A ; T entre t1 et t2 : nom=B. Oracle : traces_in_effect ET la
    fenetre de Rewind (plan "recompensate" pour U1).
U2  INSERT (t1), trace DELETE (t2), annulee (t3). T entre t2 et t3 : absente
    a T et presente aujourd'hui (comptee n_born_after).
U3  DELETE (t1), trace INSERT de re-insertion (t2), annulee (t3). T entre t2
    et t3 : presente a T avec son etat.
U4  UPDATE A->B (t1), trace (t2), trace de schema 7 qui annule la trace (t3),
    elle-meme annulee (t4). T entre t3 et t4 : nom=B (la seconde trace
    annule la premiere) ; T entre t2 et t3 : nom=A.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

SCENARIO_ID = "rvw_undo_compensation_state_at_t"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PT2 = b"WKB_POINT_2"
_PT3 = b"WKB_POINT_3"

_T1 = "2026-04-01T10:00:00+00:00"
_T2 = "2026-04-02T10:00:00+00:00"
_T3 = "2026-04-03T10:00:00+00:00"
_T4 = "2026-04-04T10:00:00+00:00"
_AT_12 = "2026-04-01T12:00:00"
_AT_23 = "2026-04-02T12:00:00"
_AT_34 = "2026-04-03T12:00:00"


def _user(eid, fp, fid, op, created, attrs, geom=None):
    return {
        "event_id": eid, "project_fingerprint": "p",
        "datasource_fingerprint": fp, "layer_id_snapshot": "l",
        "layer_name_snapshot": "parcelles", "provider_type": "ogr",
        "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op, "attributes_json": json.dumps(attrs),
        "geometry_wkb": geom, "geometry_type": "Point",
        "crs_authid": "EPSG:2154", "field_schema_json": "[]",
        "user_name": "u", "session_id": "s", "created_at": created,
        "restored_from_event_id": None, "entity_fingerprint": f"fid:{fid}",
        "event_schema_version": 7, "new_geometry_wkb": None,
        "invalidated_at": None,
    }


def _trace(eid, fp, fid, op, created, src, invalidated=None):
    """Shaped like restore_service.build_restore_trace_event (schema 7)."""
    row = _user(eid, fp, fid, op, created, {"_restore_ref": src})
    row.update({"restored_from_event_id": src, "session_id": None,
                "field_schema_json": None, "invalidated_at": invalidated})
    return row


_CASES = {
    "U1": ("ds_u1", [
        _user(11, "ds_u1", 1, "UPDATE", _T1,
              {"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        _trace(12, "ds_u1", 1, "UPDATE", _T2, 11, invalidated=_T3),
    ]),
    "U2": ("ds_u2", [
        _user(21, "ds_u2", 2, "INSERT", _T1,
              {"all_attributes": {"nom": "Deux"}}, geom=_PT2),
        _trace(22, "ds_u2", 2, "DELETE", _T2, 21, invalidated=_T3),
    ]),
    "U3": ("ds_u3", [
        _user(31, "ds_u3", 3, "DELETE", _T1,
              {"all_attributes": {"nom": "Trois"}}, geom=_PT3),
        _trace(32, "ds_u3", 3, "INSERT", _T2, 31, invalidated=_T3),
    ]),
    "U4": ("ds_u4", [
        _user(41, "ds_u4", 4, "UPDATE", _T1,
              {"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        _trace(42, "ds_u4", 4, "UPDATE", _T2, 41),
        _trace(43, "ds_u4", 4, "UPDATE", _T3, 42, invalidated=_T4),
    ]),
}


class _Journal:
    def __init__(self, path):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def _write_journal(path, rows):
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    cols = ", ".join(AUDIT_EVENT_COLUMNS)
    ph = ",".join("?" for _ in AUDIT_EVENT_COLUMNS)
    with conn:
        conn.executemany(
            f"INSERT INTO audit_event ({cols}) VALUES ({ph})",  # nosec B608
            [tuple(r[c] for c in AUDIT_EVENT_COLUMNS) for r in rows])
    conn.close()


def _review(path, fp, iso):
    """The real worker, run synchronously, then its entity map."""
    import recoverland.widgets.snapshot_rebuild_worker as wmod
    captured = {}
    worker = wmod.SnapshotRebuildWorker(
        _Journal(path),
        [{"fingerprint": fp, "layer_id": "", "layer_name": "parcelles",
          "storage_crs": "EPSG:2154"}],
        iso, trace_id=uuid.uuid4().hex[:8])
    worker.result_ready.connect(lambda t, r: captured.__setitem__("result", r))
    worker.error.connect(lambda t, e: captured.__setitem__("error", e))
    worker.run()
    res = captured.get("result")
    out = {"error": captured.get("error"), "entities": {}}
    if res is None:
        return out
    for key, feat in res.features.get(fp, {}).items():
        attrs = json.loads(feat.attrs_json) if feat.attrs_json else {}
        out["entities"][key] = {"nom": attrs.get("nom"),
                                "geom": feat.geom_wkb.decode("latin-1")
                                if feat.geom_wkb else None,
                                "unresolved": feat.unresolved}
    out["n_born_after"] = res.n_born_after
    out["n_absent"] = res.n_absent
    return out


def _rewind_in_effect(rows, iso):
    """Rewind's reading: {trace event_id: in effect at iso}."""
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.rewind_dedup import traces_in_effect
    instant = datetime.fromisoformat(iso + "+00:00")
    traces = [AuditEvent(**r) for r in rows if r["restored_from_event_id"]]

    def happened(ts):
        return bool(ts) and datetime.fromisoformat(ts) <= instant
    return traces_in_effect(traces, happened)


def _rewind_plan_kinds(path, fp, iso):
    """Plan items the real Rewind window builds for the history before T."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import CutoffType, RestoreCutoff
    conn = sqlite3.connect(path)
    try:
        window = fetch_events_after_cutoff(
            conn, fp, RestoreCutoff(CutoffType.BY_DATE, iso, inclusive=True))
    finally:
        conn.close()
    return sorted(str(e.commit_id).split("@")[0] for e in window
                  if str(getattr(e, "commit_id", "") or "").startswith("rewind-plan:"))


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_undo_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["paths"] = {}
    for name, (_fp, rows) in _CASES.items():
        path = os.path.join(tmpdir, f"{name}.sqlite")
        _write_journal(path, rows)
        ctx.data["paths"][name] = path
    flog(f"rvw_undo_compensation_state_at_t setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    p = d["paths"]
    try:
        d["u1_23"] = _review(p["U1"], "ds_u1", _AT_23)
        d["u1_12"] = _review(p["U1"], "ds_u1", _AT_12)
        d["u1_rewind_23"] = _rewind_in_effect(_CASES["U1"][1], _AT_23)
        d["u1_plan_23"] = _rewind_plan_kinds(p["U1"], "ds_u1", _AT_23)
        d["u2_23"] = _review(p["U2"], "ds_u2", _AT_23)
        d["u3_23"] = _review(p["U3"], "ds_u3", _AT_23)
        d["u4_34"] = _review(p["U4"], "ds_u4", _AT_34)
        d["u4_23"] = _review(p["U4"], "ds_u4", _AT_23)
        d["u4_rewind_34"] = _rewind_in_effect(_CASES["U4"][1], _AT_34)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"rvw_undo_compensation_state_at_t run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k not in ('trace', 'paths')}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _nom(res, key):
    return ((res or {}).get("entities") or {}).get(key, {}).get("nom")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    out = []
    rw = d.get("u1_rewind_23") or {}
    out.append((
        "u1_oracle_rewind_counts_the_compensation_until_its_undo",
        rw.get(12) is True and "rewind-plan:recompensate" in (d.get("u1_plan_23") or []),
        f"traces_in_effect a T={rw} plan du Rewind={d.get('u1_plan_23')} : pour "
        f"le Rewind la compensation 12 est en vigueur a T (il la referait){crash}",
    ))
    out.append((
        "u1_review_between_compensation_and_undo_shows_compensated_value",
        _nom(d.get("u1_23"), "fid:1") == "A",
        f"Review a T (entre le Rewind et son annulation) : nom="
        f"{_nom(d.get('u1_23'), 'fid:1')!r} (attendu 'A', ce que le Rewind "
        f"reconstruirait ; 'B' ou absente = la compensation ignoree) "
        f"resultat={d.get('u1_23')}{crash}",
    ))
    out.append((
        "u1_review_before_compensation_unchanged",
        _nom(d.get("u1_12"), "fid:1") == "B",
        f"Review avant le Rewind : nom={_nom(d.get('u1_12'), 'fid:1')!r} "
        f"(attendu 'B'){crash}",
    ))
    u2 = d.get("u2_23") or {}
    out.append((
        "u2_entity_removed_by_rewind_then_restored_by_undo_is_absent_at_t",
        "fid:2" not in (u2.get("entities") or {}) and (u2.get("n_born_after") or 0) >= 1,
        f"Review a T : entites={sorted((u2.get('entities') or {}))} "
        f"n_born_after={u2.get('n_born_after')} : l'entite n'existait pas a T "
        f"(le Rewind l'avait retiree) mais la couche actuelle l'affiche, la "
        f"barre doit le dire{crash}",
    ))
    u3 = ((d.get("u3_23") or {}).get("entities") or {}).get("fid:3") or {}
    out.append((
        "u3_entity_reinserted_by_rewind_then_removed_by_undo_is_present_at_t",
        u3.get("nom") == "Trois" and u3.get("geom") == _PT3.decode("latin-1"),
        f"Review a T : fid:3={u3 or 'ABSENTE'} (attendu nom=Trois a sa "
        f"geometrie : le Rewind l'avait re-inseree, l'annulation l'a retiree "
        f"ensuite){crash}",
    ))
    rw4 = d.get("u4_rewind_34") or {}
    out.append((
        "u4_schema7_trace_on_a_trace_cancels_it_until_its_undo",
        rw4.get(42) is False and rw4.get(43) is True
        and _nom(d.get("u4_34"), "fid:4") == "B",
        f"traces_in_effect a T={rw4} ; Review a T : nom="
        f"{_nom(d.get('u4_34'), 'fid:4')!r} (attendu 'B' : la trace 43 annule "
        f"la trace 42, jusqu'a sa propre annulation){crash}",
    ))
    out.append((
        "u4_before_the_cancelling_trace",
        _nom(d.get("u4_23"), "fid:4") == "A",
        f"Review entre la trace 42 et la trace 43 : nom="
        f"{_nom(d.get('u4_23'), 'fid:4')!r} (attendu 'A'){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_undo_compensation_state_at_t.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
