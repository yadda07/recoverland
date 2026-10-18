"""rw_rewind_live_key_columns - le dialogue donne au rewind la cle de la couche vivante.

Invariant : I-3 (une cle d'entite designe UNE entite) et I-REWIND-01.

Le mecanisme
------------
Avant v7, une cle composite (zone_id, parcel_no) etait journalisee par sa
PREMIERE colonne : ``pk:zone_id=12`` designe toutes les parcelles de la
zone 12. Le coeur sait re-cles ces lignes depuis leur propre instantane
(`rewind_dedup.prove_legacy_keys`) des qu'il connait les colonnes de la
cle : celles d'une ligne v7 de la couche dans la fenetre, ou celles de la
couche VIVANTE que l'appelant lui passe (`key_columns=`). Le dialogue
appelait `collapse_rewind_events_with_stats(events)` sans elles : une
couche dont la fenetre ne contient que des lignes d'avant v7 gardait des
cles qui designent plusieurs parcelles, et le seau entier etait refuse
("unrebuildable_state"), ou, sans la garde, l'INSERT d'une parcelle
annulait le DELETE d'une autre.

Ce que l'utilisateur voyait
---------------------------
Un rewind avant la creation de la parcelle (12,9) et la suppression de la
parcelle (12,7) refuse la couche entiere, ou (12,9) reste et (12,7) ne
revient pas, alors que la couche chargee dans le projet dit clairement que
sa cle a deux colonnes. Idem pour "Annuler toute la session".

Ce qui est prouve
-----------------
R  `_on_version_fetch_done` (vraie fenetre lue du journal, vraie collapse)
   remet au runner l'INSERT de (12,9) et le DELETE de (12,7), chacun sous la
   cle composite relue de son instantane, sans refus.
S  `_session_undo_plan` fait de meme pour les evenements d'une session.
K  Controle : une couche sans cle vivante lisible laisse le journal decider
   (le resultat d'avant, pas d'invention).

Verdict avant correctif : FAIL (R, S). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_rewind_live_key_columns"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_DS = "postgres::service=prod host= port=5432 dbname= schema=cad table=parcelles"
_CUTOFF_ISO = "2026-05-01T12:00:00"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _row(op, identity, entity, ts, attrs, session="s_lkc"):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": _DS,
        "layer_id_snapshot": "lyr_parcelles", "layer_name_snapshot": "parcelles",
        "provider_type": "postgres",
        "feature_identity_json": json.dumps(identity),
        "operation_type": op, "attributes_json": json.dumps(attrs),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": None,
        "field_schema_json": None, "user_name": "u", "session_id": session,
        "created_at": ts, "restored_from_event_id": None,
        "entity_fingerprint": entity, "event_schema_version": 6,
        "new_geometry_wkb": None, "invalidated_at": None,
    }


def _insert(conn, row) -> int:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


class _Field:
    def __init__(self, name):
        self._name = name

    def name(self):
        return self._name


class _Fields:
    def __init__(self, names):
        self._names = list(names)

    def count(self):
        return len(self._names)

    def at(self, i):
        return _Field(self._names[i])


def _layer_class(h, pk_indexes):
    class _Provider(h.FakeProvider):
        def name(self):
            return "postgres"

        def pkAttributeIndexes(self):  # noqa: N802
            if pk_indexes is None:
                raise RuntimeError("provider cannot describe its key (scripted)")
            return list(pk_indexes)

    class _CpkLayer(h.FakeLayer):
        def fields(self):
            return _Fields(["zone_id", "parcel_no", "nom"])

        def dataProvider(self):  # noqa: N802
            return _Provider()

    return _CpkLayer


def _seed(tmp):
    h = _h()
    path = h.new_journal(tmp, "lkc.sqlite")
    conn = sqlite3.connect(path)
    ids = {}
    try:
        ids["ins_129"] = _insert(conn, _row(
            "INSERT", {"fid": 9, "pk_field": "zone_id", "pk_value": 12},
            "pk:zone_id=12", "2026-05-01T12:01:00+00:00",
            {"all_attributes": {"zone_id": 12, "parcel_no": 9, "nom": "neuf"}}))
        ids["del_127"] = _insert(conn, _row(
            "DELETE", {"fid": 7, "pk_field": "zone_id", "pk_value": 12},
            "pk:zone_id=12", "2026-05-01T12:02:00+00:00",
            {"all_attributes": {"zone_id": 12, "parcel_no": 7, "nom": "sept"}}))
        conn.commit()
    finally:
        conn.close()
    return path, ids


def _window(path):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    conn = sqlite3.connect(path)
    try:
        return fetch_events_after_cutoff(
            conn, _DS, RestoreCutoff(CutoffType.BY_DATE,
                                     _CUTOFF_ISO + "+00:00", inclusive=True))
    finally:
        conn.close()


def _plan_rows(events):
    from recoverland.core.rewind_dedup import plan_item_kind, refusal_reason
    return [(e.event_id, e.operation_type, e.entity_fingerprint,
             plan_item_kind(e) or "user", refusal_reason(e) or "")
            for e in events or []]


def _case_r(ctx, tmp, label, pk_indexes):
    import recoverland.recover_dialog as rd
    h = _h()
    path, ids = _seed(tmp)
    ctx.data[f"{label}_ids"] = ids
    window = _window(path)
    layer = _layer_class(h, pk_indexes)("lyr_parcelles", "parcelles")
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path), [_DS])
    stricts, asked, logs = [], [], []
    seams = h.Seams()
    try:
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
        dlg._version_restore_cutoff = RestoreCutoff(
            CutoffType.BY_DATE, _CUTOFF_ISO, inclusive=True)
        dlg._active_restore_trace_id = f"rw_lkc_{label}"
        dlg._is_recovering = True
        dlg._version_fetch_total = len(window)
        dlg._on_version_fetch_done(list(window))
        h.pump_until(lambda: dlg._restore_runner is None
                     and not dlg._restore_in_progress, 5.0)
    finally:
        seams.restore()
        h.close_dialog(dlg)
    ctx.data[f"{label}_window"] = [e.event_id for e in window]
    ctx.data[f"{label}_plan"] = _plan_rows(stricts[0].events) if stricts else None
    ctx.data[f"{label}_logs"] = [m[:300] for m, _l in logs][-3:]


def _case_s(ctx, tmp):
    import recoverland.recover_dialog as rd
    h = _h()
    path, ids = _seed(tmp)
    ctx.data["s_ids"] = ids
    layer = _layer_class(h, [0, 1])("lyr_parcelles", "parcelles")
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path), [_DS])
    seams = h.Seams()
    try:
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        conn = sqlite3.connect(path)
        try:
            events = h.fetch_events(path, [ids["ins_129"], ids["del_127"]])
            keep, refused, _stats = dlg._session_undo_plan(conn, events)
        finally:
            conn.close()
    finally:
        seams.restore()
        h.close_dialog(dlg)
    ctx.data["s_plan"] = _plan_rows(keep)
    ctx.data["s_refused"] = list(refused)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_lkc_")
    flog(f"rw_rewind_live_key_columns setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmpdir"]
    ctx.data["errors"] = {}
    for name, case in (("r", lambda c, t: _case_r(c, t, "r", [0, 1])),
                       ("k", lambda c, t: _case_r(c, t, "k", None)),
                       ("s", _case_s)):
        sub = Path(tmp) / name
        sub.mkdir(exist_ok=True)
        try:
            case(ctx, str(sub))
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-900:]}")
    flog(f"rw_rewind_live_key_columns run: trace_id={ctx.trace_id} "
         f"r={ctx.data.get('r_plan')} k={ctx.data.get('k_plan')} "
         f"s={ctx.data.get('s_plan')} errors={ctx.data['errors']}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _compensated(plan, ids):
    rows = {row[0]: row for row in plan or [] if row[3] == "user"}
    ins, dele = rows.get(ids.get("ins_129")), rows.get(ids.get("del_127"))
    return ins, dele


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"), f"crash={d.get('errors')}")]
    for label, what in (("r", "rewind avant les deux editions"),
                        ("s", "annulation de toute la session")):
        plan = d.get(f"{label}_plan")
        ids = d.get(f"{label}_ids") or {}
        ins, dele = _compensated(plan, ids)
        refusals = [row for row in plan or [] if row[3] != "user"]
        out.append((
            f"{label}_both_parcels_compensated_under_their_own_key",
            ins is not None and dele is not None
            and ins[2] == "pkc:zone_id=12|parcel_no=9"
            and dele[2] == "pkc:zone_id=12|parcel_no=7" and not refusals,
            f"{what} : plan remis au runner={plan}. La couche chargee a une "
            f"cle (zone_id, parcel_no) : la creation de (12,9) et la "
            f"suppression de (12,7) sont deux parcelles, chacune compensee ; "
            f"sans la cle vivante, le seau 'zone 12' est refuse en entier ou "
            f"l'une annule l'autre et (12,9) reste, (12,7) ne revient pas.",
        ))
    k_plan = d.get("k_plan")
    out.append((
        "k_unreadable_live_key_leaves_the_journal_to_decide",
        k_plan is not None and not any(
            row[2] and row[2].startswith("pkc:") for row in k_plan),
        f"couche sans cle lisible : plan={k_plan}. Rien n'est invente : sans "
        f"colonnes connues, les lignes gardent la cle du journal et la "
        f"collapse applique ses propres regles.",
    ))
    return out
