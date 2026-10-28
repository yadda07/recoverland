# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_journal_follows_saved_project - RecoverLand validation runtime.

Invariant: I-JRN / I-CAPTURE (un evenement capture finit dans le journal
durable que le plugin lira pour ces donnees ; il reste joignable).

Le mecanisme
------------
`JournalManager.open_for_project` choisit le journal d'apres
`absoluteFilePath()` :

  * projet enregistre -> `<dossier du projet>/.recoverland/recoverland_audit.sqlite`
  * projet jamais enregistre -> `<profil>/recoverland/audit/audit_<cle>.sqlite`,
    ou `<cle>` est tiree au hasard a chaque ouverture et n'est persistee nulle
    part.

Rien n'ecoutait `QgsProject.projectSaved` : apres le premier "Enregistrer
sous", la session continuait d'ecrire dans le fichier du profil ; a la
reouverture, le projet lisait `.recoverland/` qui ne contenait rien de tout
cela, et `cleanup_orphan_journals` supprimait le fichier du profil 30 jours
apres sa derniere ecriture. "Enregistrer sous" dans un autre dossier
coupait l'historique de la meme facon.

Le scenario d'echec concret
---------------------------
L'utilisateur demarre QGIS, ajoute parcelles.gpkg, modifie des entites,
enregistre le projet sous P.qgz, modifie encore, ferme. Le lendemain il
ouvre P.qgz : Restaurer / Rewind n'affichent rien des modifications
capturees la veille. Trente jours plus tard, le fichier qui les contenait
est supprime au demarrage.

Ce que le scenario execute reellement
-------------------------------------
Partie A (bout en bout, VRAI RecoverPlugin, faux iface, vraie couche
GeoPackage, vrais `QgsProject.write` / `read`) :
  A1 projet vierge : deux commits ; `write(P.qgz)` ; un commit ; Ctrl+S
     (`write()` sans chemin) ; un commit ; fermeture ; `read(P.qgz)`.
     Chaque modification doit etre lue UNE fois par `create_read_connection`
     du journal rouvert.
  A2 dans la session rouverte : un commit ; "Enregistrer sous" P2.qgz dans
     un AUTRE dossier ; un commit ; fermeture ; `read(P2.qgz)`. Les deux
     modifications de la session doivent etre dans le journal de P2.
Partie B (API, idempotence) : un journal de session contenant des lignes
  ANCIENNES (sans event_uid), une ligne v7, une trace de restauration qui
  pointe vers une ligne ancienne, une trace deja orpheline, le registre des
  commits et un horizon de purge est rattache DEUX fois au journal d'un
  projet qui contient deja d'autres evenements. Aucun doublon ; la trace
  pointe vers la BONNE ligne dans le journal cible ; la trace orpheline
  reste orpheline ; le registre et l'horizon de purge suivent.

Verdict attendu avant correctif : FAIL (A1, A2, B).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_follows_saved_project"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_N_FEATURES = 12
_A1_MARKERS = ("AVANT_SAUV_1", "AVANT_SAUV_2", "APRES_SAUV", "APRES_CTRL_S")
_A2_MARKERS = ("SESSION_AVANT_SOUS", "APRES_SAUV_SOUS")


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(_N_FEATURES):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"C{i:03d}", f"NOM_{i:02d}"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


def _project_layer():
    from qgis.core import QgsProject
    layers = QgsProject.instance().mapLayersByName("parcelles")
    return layers[0] if layers else None


def _rename(layer, index: int, value: str) -> bool:
    """One commit that sets `nom` of the index-th feature (by code)."""
    if layer is None:
        return False
    idx = layer.fields().indexFromName("nom")
    target = None
    for feat in layer.getFeatures():
        if feat["code"] == f"C{index:03d}":
            target = feat.id()
    if target is None:
        return False
    layer.startEditing()
    layer.changeAttributeValue(target, idx, value)
    return bool(layer.commitChanges())


def _count(journal: str, value: str) -> int:
    if not journal or not os.path.isfile(journal):
        return -1
    try:
        from recoverland.core.journal_manager import sqlite_readonly_uri
        conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                (f"%{value}%",)).fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _wait_current(plugin, value: str, timeout: float = 6.0) -> int:
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = -1
    while time.monotonic() < deadline:
        journal = plugin._journal.path if plugin._journal.is_open else None
        got = _count(journal, value)
        if got >= 1:
            return got
        pump(0.1)
    return got


def _counts_through_reader(plugin, markers) -> dict:
    """What the reopened project reads, through the product's read path."""
    out = {}
    if not plugin._journal.is_open:
        return {m: None for m in markers}
    conn = plugin._journal.create_read_connection()
    try:
        for marker in markers:
            out[marker] = int(conn.execute(
                "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                (f"%{marker}%",)).fetchone()[0])
    finally:
        conn.close()
    return out


# ----------------------------------------------------------------------
# Part B: carry idempotence at the API level
# ----------------------------------------------------------------------

_B_DS = "tx_jfsp_ds"


def _insert(conn, marker: str, op: str, fid: int, restored_from=None,
            uid=None, commit_id=None, created="2026-09-01T10:00:00+00:00"):
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS)
    values = {
        "project_fingerprint": "carry_src", "datasource_fingerprint": _B_DS,
        "layer_id_snapshot": "lyr", "layer_name_snapshot": "parcelles",
        "provider_type": "ogr",
        "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op,
        "attributes_json": json.dumps({"marker": marker}),
        "geometry_wkb": None, "geometry_type": "NoGeometry",
        "crs_authid": "EPSG:2154", "field_schema_json": None,
        "user_name": "tester", "session_id": "sess_b", "created_at": created,
        "restored_from_event_id": restored_from,
        "entity_fingerprint": f"fid:{fid}", "event_schema_version": 2,
        "new_geometry_wkb": None, "invalidated_at": None,
    }
    ext_cols = []
    ext_vals = []
    if uid is not None or commit_id is not None:
        ext_cols = ["event_uid", "commit_id"]
        ext_vals = [uid, commit_id]
    sql = (f"INSERT INTO audit_event ({', '.join(cols + ext_cols)}) "  # nosec B608
           f"VALUES ({','.join(['?'] * (len(cols) + len(ext_cols)))})")
    cur = conn.execute(sql, [values[c] for c in cols] + ext_vals)
    return cur.lastrowid


def _fixture_schema(conn) -> None:
    """v7 tables for the fixture rows, whatever the checkout under test."""
    from recoverland.core.journal_writes import ensure_v7_schema
    with conn:
        ensure_v7_schema(conn)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS journal_coverage ("
            "datasource_fingerprint TEXT PRIMARY KEY, "
            "purged_through TEXT NOT NULL, "
            "purged_events INTEGER NOT NULL DEFAULT 0, "
            "updated_at TEXT NOT NULL)")


def _part_b(tmp: str) -> dict:
    from recoverland.core.journal_manager import JournalManager

    res = {}
    profile = os.path.join(tmp, "profil_b")
    os.makedirs(profile, exist_ok=True)
    src_jm = JournalManager()
    src = src_jm.open_for_project("", profile)
    conn = src_jm.get_connection()
    _fixture_schema(conn)
    with conn:
        l1 = _insert(conn, "B_LEGACY_UPDATE", "UPDATE", 1)
        l2 = _insert(conn, "B_LEGACY_DELETE", "DELETE", 2)
        _insert(conn, "B_V7_INSERT", "INSERT", 3, uid="uid-b-v7", commit_id="c-b-1")
        _insert(conn, "B_TRACE_OF_DELETE", "INSERT", 2, restored_from=l2)
        _insert(conn, "B_ORPHAN_TRACE", "INSERT", 9, restored_from=99999,
                uid="uid-b-orphan")
        conn.execute(
            "INSERT INTO capture_commit (commit_id, state, recorded_at, "
            "snapshots_json) VALUES ('c-b-1', 'COMMITTED', '2026-09-01', NULL)")
        conn.execute(
            "INSERT INTO capture_commit (commit_id, state, recorded_at, "
            "snapshots_json) VALUES ('c-b-int', 'INTERRUPTED', '2026-09-01', "
            "'[{\"kind\": \"UPDATE\", \"fid\": 5}]')")
        conn.execute(
            "INSERT INTO journal_coverage (datasource_fingerprint, "
            "purged_through, purged_events, updated_at) VALUES "
            "(?, '2026-06-01T00:00:00+00:00', 40, '2026-09-01')", (_B_DS,))
    res["src_legacy_ids"] = [l1, l2]
    src_jm.close()

    pdir = os.path.join(tmp, "projet_b")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    dst_jm = JournalManager()
    dst = dst_jm.open_for_project(project, profile)
    dconn = dst_jm.get_connection()
    _fixture_schema(dconn)
    with dconn:
        for i in range(7):
            _insert(dconn, f"B_PREEXISTING_{i}", "UPDATE", 100 + i,
                    uid=f"uid-pre-{i}")
    res["src"] = src
    res["dst"] = dst
    try:
        from recoverland.core.journal_manager import carry_journal_history
        reports = []
        for _ in range(2):
            rep = carry_journal_history(src, dconn, dst, since_event_id=0)
            reports.append(rep.as_dict())
        res["reports"] = reports
    except Exception as exc:  # noqa: BLE001 - pre-fix: the API is missing
        res["error"] = f"{type(exc).__name__}: {exc}"
    rows = dconn.execute(
        "SELECT event_id, attributes_json, restored_from_event_id, event_uid "
        "FROM audit_event WHERE project_fingerprint = 'carry_src' "
        "ORDER BY event_id").fetchall()
    by_marker = {}
    for eid, attrs, ref, uid in rows:
        marker = json.loads(attrs).get("marker")
        by_marker.setdefault(marker, []).append(
            {"id": eid, "ref": ref, "uid": uid})
    res["by_marker"] = by_marker
    res["commits"] = {r[0]: r[1] for r in dconn.execute(
        "SELECT commit_id, snapshots_json FROM capture_commit").fetchall()}
    row = dconn.execute(
        "SELECT purged_through FROM journal_coverage "
        "WHERE datasource_fingerprint = ?", (_B_DS,)).fetchone()
    res["coverage"] = row[0] if row else None
    ids = {r[0] for r in dconn.execute("SELECT event_id FROM audit_event")}
    trace = (by_marker.get("B_TRACE_OF_DELETE") or [{}])[0]
    target = (by_marker.get("B_LEGACY_DELETE") or [{}])[0]
    orphan = (by_marker.get("B_ORPHAN_TRACE") or [{}])[0]
    res["trace_points_to_its_delete"] = (
        trace.get("ref") is not None and trace.get("ref") == target.get("id"))
    res["orphan_stays_orphan"] = (
        orphan.get("ref") is not None and orphan.get("ref") not in ids)
    dst_jm.close()
    sconn = sqlite3.connect(src)
    try:
        res["src_uids"] = sorted(
            (r[0] or "") for r in sconn.execute(
                "SELECT event_uid FROM audit_event"))
    finally:
        sconn.close()
    return res


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jfsp_")
    ctx.data["tmp"] = tmp
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    _make_gpkg(gpkg)
    ctx.data["gpkg"] = gpkg
    flog(f"tx_journal_follows_saved_project setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    tmp = d["tmp"]
    p1 = os.path.join(tmp, "dossier_1", "P.qgz")
    p2 = os.path.join(tmp, "dossier_2", "P2.qgz")
    os.makedirs(os.path.dirname(p1), exist_ok=True)
    os.makedirs(os.path.dirname(p2), exist_ok=True)
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        plugin.initGui()
        pump(0.3)
        d["session_journal"] = plugin._journal.path if plugin._journal.is_open else None
        layer = QgsVectorLayer(f"{d['gpkg']}|layername=parcelles", "parcelles", "ogr")
        QgsProject.instance().addMapLayer(layer)
        # ---- A1 -------------------------------------------------------
        d["a1_commits"] = [
            _rename(layer, 0, "AVANT_SAUV_1"),
            _rename(layer, 1, "AVANT_SAUV_2"),
        ]
        d["a1_before_save_in_session"] = [
            _wait_current(plugin, m) for m in ("AVANT_SAUV_1", "AVANT_SAUV_2")]
        d["a1_write"] = bool(QgsProject.instance().write(p1))
        pump(0.5)
        d["a1_journal_after_save"] = plugin._journal.path if plugin._journal.is_open else None
        d["a1_commits"].append(_rename(_project_layer(), 2, "APRES_SAUV"))
        _wait_current(plugin, "APRES_SAUV")
        d["a1_write_again"] = bool(QgsProject.instance().write())
        pump(0.5)
        d["a1_commits"].append(_rename(_project_layer(), 3, "APRES_CTRL_S"))
        _wait_current(plugin, "APRES_CTRL_S")
        QgsProject.instance().clear()
        pump(0.3)
        d["a1_read"] = bool(QgsProject.instance().read(p1))
        pump(0.5)
        d["a1_reopened_journal"] = plugin._journal.path if plugin._journal.is_open else None
        d["a1_counts"] = _counts_through_reader(plugin, _A1_MARKERS)
        # ---- A2 -------------------------------------------------------
        d["a2_commits"] = [_rename(_project_layer(), 4, "SESSION_AVANT_SOUS")]
        _wait_current(plugin, "SESSION_AVANT_SOUS")
        d["a2_write"] = bool(QgsProject.instance().write(p2))
        pump(0.5)
        d["a2_journal_after_save_as"] = plugin._journal.path if plugin._journal.is_open else None
        d["a2_commits"].append(_rename(_project_layer(), 5, "APRES_SAUV_SOUS"))
        _wait_current(plugin, "APRES_SAUV_SOUS")
        QgsProject.instance().clear()
        pump(0.3)
        d["a2_read"] = bool(QgsProject.instance().read(p2))
        pump(0.5)
        d["a2_reopened_journal"] = plugin._journal.path if plugin._journal.is_open else None
        d["a2_counts"] = _counts_through_reader(plugin, _A2_MARKERS)
        d["a2_p1_keeps_its_session_edit"] = _count(
            d.get("a1_reopened_journal"), "SESSION_AVANT_SOUS")
        d["bar"] = iface.bar.texts()[-6:]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    d["session_journal_still_exists"] = bool(
        d.get("session_journal") and os.path.isfile(d["session_journal"]))
    try:
        d["b"] = _part_b(tmp)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["b"] = {"crash": f"{type(exc).__name__}: {exc}",
                  "trace": traceback.format_exc()[-1500:]}
    flog(f"tx_journal_follows_saved_project run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:5000]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    crash = f"{d.get('crash', '')} {d.get('trace', '')[-500:]}".strip()
    out.append((
        "fixture_commits_et_sauvegardes",
        all(d.get("a1_commits") or [False]) and d.get("a1_write") is True
        and d.get("a1_read") is True and not d.get("crash"),
        f"commits={d.get('a1_commits')} write={d.get('a1_write')} "
        f"read={d.get('a1_read')} crash={crash!r}",
    ))
    out.append((
        "fixture_capture_avant_sauvegarde",
        d.get("a1_before_save_in_session") == [1, 1],
        f"lignes dans le journal de session avant la sauvegarde="
        f"{d.get('a1_before_save_in_session')} (journal="
        f"{d.get('session_journal')}) : sans capture initiale le cas ne "
        f"prouve rien",
    ))
    counts = d.get("a1_counts") or {}
    out.append((
        "a1_historique_suit_le_premier_enregistrement",
        all(counts.get(m) == 1 for m in _A1_MARKERS),
        f"modifications lues dans le journal du projet rouvert "
        f"({d.get('a1_reopened_journal')}) : {counts} (attendu 1 chacune). "
        f"Journal apres 'Enregistrer sous'={d.get('a1_journal_after_save')}. "
        f"Sinon l'utilisateur rouvre P.qgz le lendemain et Restaurer/Rewind "
        f"ne montrent rien de ce qui a ete capture.",
    ))
    out.append((
        "a1_journal_bascule_a_l_enregistrement",
        bool(d.get("a1_journal_after_save"))
        and d.get("a1_journal_after_save") == d.get("a1_reopened_journal"),
        f"journal courant juste apres l'enregistrement="
        f"{d.get('a1_journal_after_save')} ; journal lu a la reouverture="
        f"{d.get('a1_reopened_journal')} : apres l'enregistrement les "
        f"modifications doivent aller la ou le projet les relira",
    ))
    out.append((
        "a1_journal_de_session_non_detruit",
        d.get("session_journal_still_exists") is True,
        f"journal de session {d.get('session_journal')} encore present="
        f"{d.get('session_journal_still_exists')} : il reste la copie de "
        f"secours tant que rien ne prouve qu'il est inutile",
    ))
    counts2 = d.get("a2_counts") or {}
    out.append((
        "a2_enregistrer_sous_ailleurs_emporte_la_session",
        d.get("a2_write") is True and d.get("a2_read") is True
        and all(counts2.get(m) == 1 for m in _A2_MARKERS),
        f"'Enregistrer sous' P2 dans un autre dossier : lu dans le journal de "
        f"P2 ({d.get('a2_reopened_journal')}) : {counts2} (attendu 1 chacune). "
        f"Journal juste apres = {d.get('a2_journal_after_save_as')}. Sinon la "
        f"session est coupee en deux et P2 ne montre qu'une partie.",
    ))
    out.append((
        "a2_p1_garde_son_historique",
        d.get("a2_p1_keeps_its_session_edit") == 1,
        f"modification faite dans P avant 'Enregistrer sous' encore lue dans "
        f"le journal de P : {d.get('a2_p1_keeps_its_session_edit')} (attendu "
        f"1) : rattacher a P2 ne doit rien retirer a P",
    ))
    b = d.get("b") or {}
    reports = b.get("reports") or []
    bym = b.get("by_marker") or {}
    b_markers = ("B_LEGACY_UPDATE", "B_LEGACY_DELETE", "B_V7_INSERT",
                 "B_TRACE_OF_DELETE", "B_ORPHAN_TRACE")
    out.append((
        "b_rattachement_sans_doublon_meme_repete",
        not b.get("error") and not b.get("crash")
        and all(len(bym.get(m) or []) == 1 for m in b_markers),
        f"lignes par evenement dans le journal cible apres DEUX rattachements="
        f"{ {m: len(bym.get(m) or []) for m in b_markers} } erreur="
        f"{b.get('error') or b.get('crash')!r} : une ligne ancienne (sans "
        f"event_uid) dupliquee est un evenement qu'un rewind appliquerait "
        f"deux fois",
    ))
    out.append((
        "b_second_rattachement_n_insere_rien",
        len(reports) == 2 and reports[1].get("events_inserted") == 0
        and reports[0].get("events_inserted") == len(b_markers),
        f"rapports={reports}",
    ))
    out.append((
        "b_trace_pointe_vers_la_bonne_ligne",
        b.get("trace_points_to_its_delete") is True,
        f"trace de restauration dans le journal cible : "
        f"{bym.get('B_TRACE_OF_DELETE')} ; suppression d'origine : "
        f"{bym.get('B_LEGACY_DELETE')}. Un restored_from_event_id recopie tel "
        f"quel pointe vers un evenement sans rapport du journal cible : le "
        f"prochain rewind recompense une suppression deja compensee et "
        f"l'entite revient une deuxieme fois.",
    ))
    out.append((
        "b_trace_orpheline_reste_orpheline",
        b.get("orphan_stays_orphan") is True,
        f"trace dont la source n'existait deja plus : "
        f"{bym.get('B_ORPHAN_TRACE')} : elle ne doit designer aucune ligne du "
        f"journal cible",
    ))
    out.append((
        "b_registre_des_commits_suit",
        "c-b-1" in (b.get("commits") or {})
        and "UPDATE" in str((b.get("commits") or {}).get("c-b-int")),
        f"registre des commits dans le journal cible="
        f"{sorted((b.get('commits') or {}))} : un commit INTERRUPTED garde "
        f"l'etat d'avant des entites, le perdre efface la seule trace d'un "
        f"plantage",
    ))
    out.append((
        "b_horizon_de_purge_suit",
        b.get("coverage") == "2026-06-01T00:00:00+00:00",
        f"horizon de purge du journal cible pour la source={b.get('coverage')!r} "
        f"(attendu 2026-06-01) : sans lui, un rewind a une date purgee dans le "
        f"journal de session se declarerait complet",
    ))
    out.append((
        "b_lignes_anciennes_identifiees_a_la_source",
        bool(b.get("src_uids")) and all(b.get("src_uids")),
        f"event_uid des lignes du journal source apres rattachement="
        f"{b.get('src_uids')} : l'identite donnee aux lignes anciennes doit "
        f"etre la meme des deux cotes, sinon un second rattachement les "
        f"dupliquerait",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_follows_saved_project.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
