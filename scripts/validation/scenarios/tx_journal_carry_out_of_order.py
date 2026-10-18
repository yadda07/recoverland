"""tx_journal_carry_out_of_order - RecoverLand validation runtime.

Invariant: I-JRN (l'historique d'une entite se relit dans l'ordre ou il a
eu lieu ; ce qui ne peut pas l'etre est dit, pas fusionne en silence).

Le mecanisme
------------
Le rattachement d'un historique (`carry_journal_history`) ajoute les lignes
de la session a la FIN du journal du projet : elles recoivent des event_id
plus grands que tout ce qu'il contient. L'historique d'une entite se relit
par event_id croissant (fetch_events_for_entity, Rewind). Tant que la
session est la plus recente, l'ordre est juste. Mais quand le journal du
projet a ete ecrit ENTRE-TEMPS par une autre instance (journal tenu
ailleurs au moment de l'enregistrement, rattachement repris plus tard), une
modification ancienne de la session arrive APRES une modification plus
recente de la meme entite : relue dans l'ordre des event_id, l'ancienne
passe pour la derniere.

Le scenario d'echec concret
---------------------------
Deux postes modifient la parcelle C001 : le poste A a 10 h (sa session,
journal du projet tenu par le poste B), le poste B a 12 h dans le journal du
projet. Le rattachement de la session de A a lieu a 14 h : la valeur de 10 h
est rangee apres celle de 12 h. Un Rewind "a 13 h" reconstruit la valeur de
10 h, sans que personne n'ait ete prevenu que l'ordre etait douteux.

Ce que le scenario execute reellement
-------------------------------------
Le journal du projet contient deja l'entite E (12 h) et l'entite F (8 h).
Le journal de session contient E (10 h, plus ancienne), F (13 h, plus
recente) et G (nouvelle). Le vrai `carry_journal_history` : les trois
lignes sont rattachees, UNE entite (E) est signalee hors d'ordre, et le
message destine a l'utilisateur le dit.

Verdict attendu avant correctif : FAIL (rien n'est signale, le message
annonce un rattachement sans reserve).
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_carry_out_of_order"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _insert(conn, entity: str, created: str, uid: str) -> None:
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS) + ["event_uid", "commit_id"]
    row = {
        "project_fingerprint": "proj", "datasource_fingerprint": "ds_ooo",
        "layer_id_snapshot": "lyr", "layer_name_snapshot": "parcelles",
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": entity}),
        "operation_type": "UPDATE",
        "attributes_json": json.dumps({"marker": f"OOO_{entity}_{created[11:13]}h"}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "tester", "session_id": "s_ooo",
        "created_at": created, "restored_from_event_id": None,
        "entity_fingerprint": f"fid:{entity}", "event_schema_version": 2,
        "new_geometry_wkb": None, "invalidated_at": None,
        "event_uid": uid, "commit_id": None,
    }
    conn.execute(f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                 f"VALUES ({','.join(['?'] * len(cols))})", [row[c] for c in cols])
    conn.commit()


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_tx_jooo_")
    flog(f"tx_journal_carry_out_of_order setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.journal_manager import JournalManager, carry_journal_history
    from recoverland.core.logger import flog

    d = ctx.data
    tmp = d["tmp"]
    try:
        profile = os.path.join(tmp, "profil")
        os.makedirs(profile, exist_ok=True)
        src_jm = JournalManager()
        src = src_jm.open_for_project("", profile)
        conn = src_jm.get_connection()
        _insert(conn, "E", "2026-09-27T10:00:00+00:00", "u-src-e")
        _insert(conn, "F", "2026-09-27T13:00:00+00:00", "u-src-f")
        _insert(conn, "G", "2026-09-27T09:00:00+00:00", "u-src-g")
        src_jm.close()
        pdir = os.path.join(tmp, "projet")
        os.makedirs(pdir, exist_ok=True)
        project = os.path.join(pdir, "P.qgz")
        Path(project).write_text("<qgis/>", encoding="utf-8")
        dst_jm = JournalManager()
        try:
            dst = dst_jm.open_for_project(project, profile)
            dconn = dst_jm.get_connection()
            _insert(dconn, "E", "2026-09-27T12:00:00+00:00", "u-dst-e")
            _insert(dconn, "F", "2026-09-27T08:00:00+00:00", "u-dst-f")
            rep = carry_journal_history(src, dconn, dst, since_event_id=0)
            d["report"] = rep.as_dict()
            d["message"] = list(rep.user_message())
        finally:
            dst_jm.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"tx_journal_carry_out_of_order run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:3000]}", "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    rep = d.get("report") or {}
    level, text = (d.get("message") or ["", ""])[:2]
    out = [(
        "fixture_rattachement_effectue",
        rep.get("ok") is True and rep.get("events_inserted") == 3,
        f"ok={rep.get('ok')} inseres={rep.get('events_inserted')} (attendu 3) "
        f"erreur={rep.get('error')} crash={d.get('crash')} {d.get('trace', '')[-400:]}",
    ), (
        "entite_hors_d_ordre_signalee",
        rep.get("out_of_order_entities") == 1,
        f"entites hors d'ordre signalees={rep.get('out_of_order_entities')} "
        f"(attendu 1 : E, modifiee a 10 h dans la session et a 12 h dans le "
        f"journal du projet ; F et G sont dans l'ordre) : fusionnee sans un mot, "
        f"la valeur de 10 h passe pour la derniere",
    ), (
        "l_utilisateur_est_prevenu",
        level == "WARNING" and "ordre" in (text or ""),
        f"message=({level}, {text!r}) : un rattachement dont l'ordre est douteux "
        f"ne peut pas etre annonce comme un succes sans reserve",
    )]
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_carry_out_of_order.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
