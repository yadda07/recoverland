# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_review_date_gui_cost - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a une date) SANS figer QGIS :
le fil graphique n'est jamais tenu plus d'une fraction de seconde par la
reconstruction d'une date, quel que soit le nombre d'entites modifiees.

Le mecanisme
------------
Le resultat du worker (lecture du journal, fil de fond) est complete sur le
fil graphique par `snapshot_overlay_session.complete_from_live` : chaque
geometrie journalisee dans un autre SCR que celui de la couche passe par
le verdict de SCR du Rewind (`_align_to_layer_crs` -> display_verdict ->
geometry_utils.crs_verdict), qui lit le fournisseur pour chaque lecture
temoin (jusqu'a 4 requetes par entite), meme une fois l'histoire etablie.
Le tout tenait en UN appel sur le fil graphique, sans decoupage : le cout
croit avec le nombre d'entites modifiees, se repaye a chaque nouvelle date,
et sur un fournisseur distant une date d'une couche editee en masse fige
QGIS de longues minutes.

L'echec concret pour l'utilisateur
----------------------------------
Sa couche en degres a ete reecrite en Lambert-93 au meme chemin, puis 2000
entites ont ete modifiees. Il deplace le curseur de date de Review : QGIS
ne repond plus pendant plusieurs secondes (la carte ne se redessine pas,
les clics sont perdus), a chaque date ; sur une base distante, il tue QGIS
et perd ses editions non enregistrees.

Ce que le scenario pilote : le vrai plugin, le vrai dialogue en mode Review,
un vrai GeoPackage en Lambert-93 et, dans le vrai journal du plugin, 2000
UPDATE captures quand la couche se declarait en EPSG:4326 (ce que contient
le journal apres une reprojection en place). Un battement QTimer de 5 ms
mesure le plus long intervalle pendant lequel la boucle d'evenements n'a
pas tourne, de l'emission de la date a l'etat affiche. Puis le cycle de vie
d'une reconstruction en cours : une nouvelle date la remplace (l'ancienne
n'est jamais affichee), l'export est refuse tant qu'elle court, l'arret de
Review l'arrete, et une couche retiree du projet en cours de route donne un
refus nomme, pas une exception. Enfin, une date demandee une fois la couche
retiree : sans couche, aucun verdict de SCR n'est possible, et ses
geometries capturees en degres ne sont pas dessinees brutes dans le calque
Lambert-93 (pres de l'origine), elles sont refusees et comptees.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_review_date_gui_cost"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_N = 2000
# Longest stretch the GUI thread may be held while a date is rebuilt. The
# rebuild of a date of 2000 changed entities held it ~3 s in one piece.
_MAX_STALL_MS = 400


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _deg(i):
    return (2.0 + (i % 50) * 0.01, 48.0 + (i // 50) * 0.01)


def _l93(xy):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsPointXY,
        QgsProject,
    )
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"),
                                QgsCoordinateReferenceSystem("EPSG:2154"),
                                QgsProject.instance())
    p = tr.transform(QgsPointXY(*xy))
    return (p.x(), p.y())


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_cost_")
    ctx.data["tmpdir"] = tmpdir
    rows = []
    for i in range(_N):
        x, y = _deg(i)
        rows.append((f"C{i}", "new", float(i), *_l93((x + 0.0005, y + 0.0005))))
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "reproj", rows)
    flog(f"rvw_review_date_gui_cost setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def _journal_updates(jpath, layer) -> int:
    """2000 UPDATE rows captured while the layer declared EPSG:4326 (degrees).

    Dated now, after every date looked at: rows older than the journal
    itself would be taken for an import and bring the Review floor to them.
    """
    from datetime import datetime, timedelta, timezone
    from qgis.core import QgsGeometry, QgsPointXY
    from recoverland.core.identity import (
        compute_datasource_fingerprint, compute_entity_fingerprint,
        compute_feature_identity,
    )
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS
    fp = compute_datasource_fingerprint(layer)
    base = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    base.update({"project_fingerprint": "p", "datasource_fingerprint": fp,
                 "layer_id_snapshot": layer.id(), "layer_name_snapshot": layer.name(),
                 "provider_type": "ogr", "geometry_type": "Point",
                 "user_name": "u", "event_schema_version": 7,
                 "crs_authid": "EPSG:4326"})
    rows = []
    t_edit = datetime.now(timezone.utc)
    for f in layer.getFeatures():
        i = int(str(f["code"])[1:])
        x, y = _deg(i)
        ident = compute_feature_identity(layer, f)
        rows.append(dict(
            base, feature_identity_json=ident,
            entity_fingerprint=compute_entity_fingerprint(ident),
            operation_type="UPDATE",
            created_at=(t_edit + timedelta(milliseconds=i)).strftime(
                "%Y-%m-%dT%H:%M:%S.%f+00:00"),
            attributes_json=json.dumps({"changed_only": {"nom": {"old": "old", "new": "new"}}}),
            geometry_wkb=bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb()),
            new_geometry_wkb=bytes(QgsGeometry.fromPointXY(
                QgsPointXY(x + 0.0005, y + 0.0005)).asWkb())))
    cols = [c for c in AUDIT_EVENT_COLUMNS if c != "event_id"]
    conn = sqlite3.connect(jpath)
    try:
        with conn:
            conn.executemany(
                f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
                f"({', '.join('?' for _ in cols)})",
                [tuple(r[c] for c in cols) for r in rows])
    finally:
        conn.close()
    return len(rows)


def _in_flight(dlg, iso) -> bool:
    """The worker has answered but the date is not shown yet (nor refused)."""
    return (dlg._review_snap_worker is None and dlg._review_snap_raw_iso != iso
            and not getattr(dlg, "_review_snap_error", "")
            and dlg._review_snap_mode)


def _drive(dlg, iso, timeout=60.0, on_turn=None, emit=True) -> dict:
    """Emit *iso* and turn the event loop until it is shown; measure the stalls.

    *on_turn* runs after every turn and ends the wait when it returns True.
    """
    from qgis.PyQt.QtCore import QCoreApplication, QTimer
    h = _h()
    ticks = []
    beat = QTimer()
    beat.setInterval(5)
    beat.timeout.connect(lambda: ticks.append(time.monotonic()))
    beat.start()
    t0 = time.monotonic()
    if emit:
        dlg._on_snapshot_date_changed(iso)
    shown = False
    n_turns = 0
    refusals_while_building = set()
    tips = []
    while time.monotonic() - t0 < timeout:
        QCoreApplication.processEvents()
        n_turns += 1
        if not (h.review_idle(dlg) and dlg._review_snap_raw_iso == iso):
            refusals_while_building.add(bool(dlg._review_export_refusal()))
        bar = dlg._review_date_bar
        if bar is not None and _in_flight(dlg, iso):
            tip = bar._lbl_status.toolTip()
            if tip and (not tips or tips[-1] != tip):
                tips.append(tip)
        if on_turn is not None and on_turn():
            break
        if h.review_idle(dlg) and dlg._review_snap_raw_iso == iso:
            shown = True
            break
        time.sleep(0.001)
    t1 = time.monotonic()
    beat.stop()
    stamps = [t0] + ticks + [t1]
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    return {"shown": shown, "total_ms": int((t1 - t0) * 1000),
            "max_stall_ms": int(max(gaps) * 1000) if gaps else None,
            "n_beats": len(ticks), "n_turns": n_turns,
            "export_refused_while_building": sorted(refusals_while_building),
            "loading_tips": [tips[0], tips[-1]] if tips else [],
            "n_progress_tips": sum(1 for t in tips if f"/ {_N} " in t)}


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "reproj")
        layer_id = layer.id()
        h.pump(0.2)
        # Five dates, oldest first, after the journal was created and before
        # the edits (every one of them shows the 2000 entities as they were).
        dates = []
        for _ in range(5):
            time.sleep(1.1)
            dates.append(h.utc_now_iso())
        time.sleep(1.1)
        d5, d4, d2, d3, d1 = dates
        d["dates"] = dates
        d["n_rows"] = _journal_updates(h.journal_path(plugin), layer)
        h.set_view(iface, 600000.0, 6750000.0, 700000.0, 6850000.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        applied = []
        real_apply = dlg._apply_snapshot_to_extent

        def spy_apply(result, trace_id):
            applied.append(dlg._review_snap_raw_iso)
            return real_apply(result, trace_id)
        dlg._apply_snapshot_to_extent = spy_apply

        # (A) One date: how long is the GUI thread held in one piece?
        d["a"] = _drive(dlg, d1)
        rows = h.overlay_rows(dlg)
        off = []
        for i in range(0, _N, 97):
            want = _l93(_deg(i))
            got = rows.get(f"C{i}") or {}
            if got.get("x") is None or abs(got["x"] - want[0]) > 1.0 \
                    or abs(got["y"] - want[1]) > 1.0:
                off.append((i, got.get("x"), got.get("y"), want))
        d["a_n_drawn"] = len(rows)
        d["a_off"] = off[:5]
        d["a_unresolved"] = getattr(dlg._review_snap_raw_result, "n_unresolved", None)

        # (B) A new date while the previous one is being completed.
        seen = {}

        def switch_when_in_flight():
            if not seen and _in_flight(dlg, d2):
                seen["at"] = time.monotonic()
                dlg._on_snapshot_date_changed(d3)
                return True
            return False
        n_applied = len(applied)
        b1 = _drive(dlg, d2, on_turn=switch_when_in_flight, timeout=20.0)
        b2 = _drive(dlg, d3, emit=False) if seen else {}
        d["b"] = {"in_flight_seen": bool(seen), "first": b1, "then": b2,
                  "applied": applied[n_applied:],
                  "answered": dlg._review_snap_raw_iso}

        # (G) A commit lands while a date is being completed: the witness
        # reads would straddle it. The completion is dropped and the date
        # rebuilt from the journal after the commit.
        jpath = h.journal_path(plugin)
        rows_before = h.journal_rows(jpath)
        committed = {}

        def commit_when_in_flight():
            if not committed and _in_flight(dlg, d1):
                committed["job"] = getattr(dlg, "_review_snap_completion", None)
                committed["ok"] = h.user_commit(
                    layer, lambda lyr: h.rename(lyr, "C7", "commit_in_flight"))
                return True
            return False
        n_applied = len(applied)
        _drive(dlg, d1, on_turn=commit_when_in_flight, timeout=20.0)
        rows_after = h.wait_rows(jpath, rows_before + 1)
        g_run = _drive(dlg, d1, emit=False, timeout=30.0)
        job = committed.get("job")
        c7 = h.overlay_rows(dlg).get("C7") or {}
        d["g"] = {"in_flight_seen": bool(committed),
                  "commit": committed.get("ok"),
                  "journal_rows": rows_after - rows_before,
                  "job_state": getattr(job, "state", None),
                  "shown_again": g_run.get("shown"),
                  "applied": applied[n_applied:],
                  "c7": {"nom": c7.get("nom"), "x": c7.get("x")}}

        # (E) Review stopped while a date is being completed.
        stopped = {}

        def stop_when_in_flight():
            if not stopped and _in_flight(dlg, d4):
                stopped["at"] = time.monotonic()
                h.review_off(dlg)
                return True
            return False
        n_msgs = len(iface.bar.messages)
        _drive(dlg, d4, on_turn=stop_when_in_flight, timeout=20.0)
        h.pump(1.0)
        d["e"] = {"in_flight_seen": bool(stopped),
                  "session": dlg._review_snap_session is not None,
                  "snap_mode": dlg._review_snap_mode,
                  "messages": [f"{t} | {x}" for t, x, _l, _dd in iface.bar.messages[n_msgs:]]}

        # (D) The source layer is removed while a date is being completed.
        d["review_on_again"] = h.review_on(dlg, 1)
        removed = {}

        def remove_when_in_flight():
            if not removed and _in_flight(dlg, d5):
                removed["at"] = time.monotonic()
                QgsProject.instance().removeMapLayer(layer_id)
            return bool(removed) and (
                getattr(dlg, "_review_snap_error", "")
                or dlg._review_snap_raw_iso == d5)
        n_msgs = len(iface.bar.messages)
        _drive(dlg, d5, on_turn=remove_when_in_flight, timeout=20.0)
        h.pump(0.5)
        bar = dlg._review_date_bar
        d["d"] = {"in_flight_seen": bool(removed),
                  "error": getattr(dlg, "_review_snap_error", ""),
                  "answered": dlg._review_snap_raw_iso,
                  "bar_state": bar.status_state() if bar is not None else None,
                  "messages": [f"{t} | {x}" for t, x, _l, _dd in iface.bar.messages[n_msgs:]]}

        # (F) A date asked once the layer is gone (Review still on): its
        # geometries, captured in degrees, cannot be placed without it.
        if QgsProject.instance().mapLayer(layer_id) is not None:
            QgsProject.instance().removeMapLayer(layer_id)
        h.pump(0.2)
        n_msgs = len(iface.bar.messages)
        f_run = _drive(dlg, d1, timeout=30.0)
        rows = h.overlay_rows(dlg)
        raw = dlg._review_snap_raw_result
        # What the overlays (whatever the extent) and the export receive.
        placed = [f for ents in (getattr(raw, "features", None) or {}).values()
                  for f in ents.values() if f.geom_wkb]
        sample = []
        for f in placed[:2]:
            from recoverland.core.geometry_utils import rebuild_geometry
            g = rebuild_geometry(f.geom_wkb)
            if g is not None and not g.isNull():
                sample.append([g.asPoint().x(), g.asPoint().y()])
        d["f"] = {"shown": f_run.get("shown"),
                  "n_rows_in_view": len(rows),
                  "n_placed": len(placed),
                  "placed_sample": sample,
                  "n_unresolved": getattr(raw, "n_unresolved", None),
                  "messages": [f"{t} | {x}" for t, x, _l, _dd in iface.bar.messages[n_msgs:]]}
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_review_date_gui_cost run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_absent, assert_log_contains
    d = ctx.data
    d5, d4, d2, d3, d1 = (d.get("dates") or [None] * 5)
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    a = d.get("a") or {}
    out = []
    out.append((
        "setup_review_on_with_2000_updates_captured_in_degrees",
        d.get("n_rows") == _N and d.get("review_on") is True,
        f"lignes={d.get('n_rows')} review_on={d.get('review_on')}{crash}",
    ))
    out.append((
        "a_review_date_never_holds_the_gui_thread_in_one_piece",
        a.get("shown") is True and a.get("max_stall_ms") is not None
        and a["max_stall_ms"] <= _MAX_STALL_MS,
        f"date {d1} : {a} (attendu : plus long blocage <= {_MAX_STALL_MS} ms ; "
        f"en un seul morceau, QGIS ne repond plus pendant toute la "
        f"reconstruction){crash}",
    ))
    out.append((
        "the_date_is_drawn_reprojected_and_complete",
        d.get("a_n_drawn") == _N and not d.get("a_off") and d.get("a_unresolved") == 0,
        f"dessinees={d.get('a_n_drawn')}/{_N} ecarts={d.get('a_off')} "
        f"non resolues={d.get('a_unresolved')} (attendu : toutes, a leur position "
        f"de cette date reprojetee en Lambert-93){crash}",
    ))
    tips = a.get("loading_tips") or []
    out.append((
        "the_bar_says_how_far_the_date_is",
        (a.get("n_progress_tips") or 0) >= 3,
        f"infobulles de la barre pendant la completion (premiere, derniere)="
        f"{tips}, avancements distincts={a.get('n_progress_tips')} (attendu : "
        f"au moins 3, l'avancement, geometries "
        f"placees sur {_N}, qui progresse : sans lui, plusieurs secondes de "
        f"chargement ne disent rien){crash}",
    ))
    b = d.get("b") or {}
    out.append((
        "export_is_refused_while_a_date_is_being_built",
        a.get("export_refused_while_building") == [True]
        and (b.get("first") or {}).get("export_refused_while_building") in ([True], []),
        f"export refuse pendant la reconstruction : A={a.get('export_refused_while_building')} "
        f"B={(b.get('first') or {}).get('export_refused_while_building')} (attendu "
        f"toujours refuse : un export pendant la completion ecrirait l'etat d'une "
        f"autre date ou un etat partiel){crash}",
    ))
    out.append((
        "a_new_date_replaces_the_one_being_completed",
        b.get("in_flight_seen") is True and b.get("answered") == d3
        and d2 not in (b.get("applied") or []),
        f"{b} (attendu : la completion de {d2} observee en cours puis "
        f"remplacee ; seule {d3} est affichee){crash}",
    ))
    g = d.get("g") or {}
    out.append((
        "a_commit_during_the_completion_rebuilds_the_date",
        g.get("in_flight_seen") is True and g.get("commit") is True
        and g.get("journal_rows") == 1 and g.get("job_state") == "cancelled"
        and g.get("shown_again") is True and g.get("applied") == [d1]
        and (g.get("c7") or {}).get("nom") == "old",
        f"{g} (attendu : la completion en cours au commit est abandonnee "
        f"(ses lectures chevauchent le commit), la date est reconstruite puis "
        f"affichee une seule fois, C7 dans son etat a cette date){crash}",
    ))
    e = d.get("e") or {}
    out.append((
        "stopping_review_stops_the_completion",
        e.get("in_flight_seen") is True and e.get("session") is False
        and e.get("snap_mode") is False,
        f"{e} (attendu : completion observee en cours, arretee avec Review, "
        f"aucune suite apres l'arret){crash}",
    ))
    dd = d.get("d") or {}
    out.append((
        "a_layer_removed_during_the_completion_is_a_named_refusal",
        dd.get("in_flight_seen") is True and bool(dd.get("error"))
        and dd.get("answered") != d5 and dd.get("bar_state") == "error"
        and any("reproj" in m for m in dd.get("messages") or []),
        f"{dd} (attendu : couche retiree pendant la completion -> la date n'est "
        f"pas affichee, la barre est en erreur et le message nomme la couche){crash}",
    ))
    ff = d.get("f") or {}
    out.append((
        "a_layer_gone_draws_none_of_its_other_crs_geometries_raw",
        ff.get("shown") is True and ff.get("n_placed") == 0
        and ff.get("n_unresolved") == _N
        and any("SCR" in m for m in ff.get("messages") or []),
        f"{ff} (attendu : couche absente du projet -> aucune geometrie capturee "
        f"en degres dessinee brute dans le calque Lambert-93, les {_N} entites "
        f"comptees non resolues et le refus de SCR nomme ; brutes, elles "
        f"tombaient pres de l'origine du Lambert-93){crash}",
    ))
    out.append(assert_log_absent(
        ctx.records, r"Traceback|RuntimeError: wrapped C/C\+\+ object",
        name="no_exception_escapes_the_review_completion"))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_review_date_gui_cost.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
