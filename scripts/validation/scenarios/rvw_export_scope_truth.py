# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_export_scope_truth - RecoverLand validation runtime.

Invariant: I-NO-FALSE-SUCCESS (aucun resultat partiel presente comme
complet ; un fichier durable etiquete "etat a T" contient l'etat a T ou dit
explicitement quel sous-ensemble il contient).

Le mecanisme
------------
L'export GeoPackage serialisait les calques Review tels quels. Ils ne
contiennent que (1) les entites modifiees apres T, (2) celles qui coupent
l'emprise COURANTE de la carte, (3) pour une entite dont l'historique commence
par un UPDATE, les seuls champs modifies (les autres NULL). Les couches vides
etaient sautees, deux couches sources de meme nom s'ecrasaient dans le fichier
(CreateOrOverwriteLayer sur opts.layerName) tout en etant comptees deux fois,
l'export partait pendant qu'une reconstruction tournait encore, et le message
disait "Snapshot exporte : N couche(s), M entite(s)", sans la date ni le
perimetre ; le fichier ne portait pas non plus la date.

L'echec concret pour l'utilisateur
----------------------------------
Deux couches "parcelles" (deux fichiers). Apres T : une parcelle supprimee,
une renommee, une deplacee HORS de la vue courante. Export a la date T :
le fichier ne contient que ce qui est dans la vue, d'une seule des deux
couches, sans le nom d'origine des parcelles renommees... et le message
annonce un "Snapshot". Archive comme "les donnees a T", il perd des entites
et des valeurs sans que rien ne le dise.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_export_scope_truth"
INVARIANT = "I-NO-FALSE-SUCCESS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS_A = [
    ("A1", "Alpha", 10.0, 1.0, 1.0),
    ("A2", "Beta", 20.0, 2.0, 2.0),
    ("A3", "Gamma", 30.0, 3.0, 3.0),
]
_ROWS_B = [
    ("B1", "Delta", 40.0, 4.0, 4.0),
    ("B2", "Epsilon", 50.0, 5.0, 5.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _read_gpkg(path: str) -> dict:
    """{layer name: [{code, nom, surface, x, y}]} + info table rows."""
    from qgis.core import QgsVectorLayer
    out = {"layers": {}, "info": {}}
    if not os.path.isfile(path):
        return out
    probe = QgsVectorLayer(path, "probe", "ogr")
    names = [s.split("!!::!!")[1] for s in probe.dataProvider().subLayers()] \
        if probe.isValid() else []
    for name in names:
        lyr = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
        if not lyr.isValid():
            continue
        if name == "recoverland_etat_info":
            for f in lyr.getFeatures():
                out["info"][str(f["cle"])] = str(f["valeur"])
            continue
        rows = []
        for f in lyr.getFeatures():
            g = f.geometry()
            pt = g.asPoint() if g is not None and not g.isNull() else None
            rows.append({
                "code": str(f["code"]), "nom": str(f["nom"]),
                "surface": f["surface"],
                "x": pt.x() if pt else None, "y": pt.y() if pt else None,
            })
        out["layers"][name] = rows
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_export_")
    ctx.data["tmpdir"] = tmpdir
    os.makedirs(os.path.join(tmpdir, "a"))
    os.makedirs(os.path.join(tmpdir, "b"))
    ctx.data["gpkg_a"] = _h().make_gpkg(os.path.join(tmpdir, "a"), "cadastre", _ROWS_A)
    ctx.data["gpkg_b"] = _h().make_gpkg(os.path.join(tmpdir, "b"), "cadastre", _ROWS_B)
    flog(f"rvw_export_scope_truth setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def run(ctx):
    from qgis.PyQt import QtWidgets
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    saved_dialog = QtWidgets.QFileDialog.getSaveFileName
    try:
        la = h.load_layer(d["gpkg_a"], "parcelles")
        lb = h.load_layer(d["gpkg_b"], "parcelles")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        cutoff = h.utc_now_iso()
        d["cutoff"] = cutoff
        time.sleep(1.2)
        d["commits"] = [
            h.user_commit(la, lambda lyr: h.delete(lyr, "A1")),
            h.user_commit(la, lambda lyr: h.rename(lyr, "A2", "Beta2")),
            h.user_commit(lb, lambda lyr: h.move(lyr, "B1", 900.0, 900.0)),
        ]
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 3) - rows0

        # The view covers A1..A3 only: B1 changed OUTSIDE it, so the export
        # must reach beyond what the map shows.
        h.set_view(iface, 0.0, 0.0, 3.5, 3.5)
        dlg = h.open_dialog(plugin, [la, lb])
        d["review_on"] = h.review_on(dlg, 2)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay_codes"] = sorted(h.overlay_rows(dlg))

        # ---- export refused while a reconstruction is running ------------
        busy_path = os.path.join(d["tmpdir"], "busy.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (busy_path, "GeoPackage (*.gpkg)"))

        class _Busy:
            trace_id = "busy"

            def cancel(self):
                pass

            def isRunning(self):
                return False

            def wait(self, *_a):
                return True

        real_worker = dlg._review_snap_worker
        dlg._review_snap_worker = _Busy()
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        dlg._review_snap_worker = real_worker
        d["busy_file_written"] = os.path.isfile(busy_path)
        d["busy_messages"] = boxes.calls

        # ---- the export the user asks for --------------------------------
        out_path = os.path.join(d["tmpdir"], "export.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (out_path, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["messages"] = boxes.texts()
        d["file"] = _read_gpkg(out_path)
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        QtWidgets.QFileDialog.getSaveFileName = saved_dialog
        h.shutdown(plugin)
    flog(f"rvw_export_scope_truth run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    f = d.get("file") or {}
    layers = f.get("layers") or {}
    info = f.get("info") or {}
    exported = {r["code"]: r for rows in layers.values() for r in rows}
    msg = " ".join(d.get("messages") or [])
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_real_edits_and_review",
        d.get("commits") == [True, True, True] and d.get("journal_rows", 0) >= 3
        and d.get("review_on") is True,
        f"commits={d.get('commits')} lignes={d.get('journal_rows')} "
        f"review_on={d.get('review_on')} overlay={d.get('overlay_codes')}{crash}",
    ))
    b1 = exported.get("B1") or {}
    out.append((
        "export_reaches_beyond_the_current_view",
        b1.get("x") == 4.0 and b1.get("y") == 4.0,
        f"B1 (modifiee apres T, hors de la vue courante) dans le fichier={b1 or 'ABSENTE'} "
        f"(attendu (4,4)) : l'export ne se limite pas a ce que la carte affiche",
    ))
    out.append((
        "same_named_layers_are_not_overwritten",
        len(layers) == 2,
        f"couches ecrites={sorted(layers)} (attendu 2 : deux couches sources "
        f"nommees 'parcelles', la seconde ne doit pas ecraser la premiere)",
    ))
    a1 = exported.get("A1") or {}
    a2 = exported.get("A2") or {}
    out.append((
        "exported_state_is_the_state_at_t",
        a1.get("nom") == "Alpha" and a1.get("surface") == 10.0
        and a2.get("nom") == "Beta" and a2.get("surface") == 20.0,
        f"A1={a1 or 'ABSENTE'} A2={a2 or 'ABSENTE'} (attendu Alpha/10 et "
        f"Beta/20 : valeurs a T, aucun champ laisse a NULL)",
    ))
    out.append((
        "unchanged_entities_not_passed_off_as_exported",
        not ({"A3", "B2"} & set(exported)),
        f"entites du fichier={sorted(exported)} : A3/B2 inchangees ne sont pas "
        f"exportees, et le fichier doit le dire (voir la table d'information)",
    ))
    out.append((
        "file_records_date_and_scope",
        info.get("date_etat_utc") == d.get("cutoff")
        and "non modifiees" in info.get("perimetre", ""),
        f"table recoverland_etat_info={info or 'ABSENTE'} : sans elle, un "
        f"GeoPackage relu plus tard passe pour 'les donnees a T'",
    ))
    out.append((
        "message_states_date_and_scope",
        bool(d.get("cutoff")) and d.get("cutoff") in msg and "modifi" in msg
        and "non modifi" in msg,
        f"message={msg!r} : il doit donner la date et dire que seules les "
        f"entites modifiees depuis sont exportees",
    ))
    n_file = sum(len(rows) for rows in layers.values())
    out.append((
        "message_count_is_the_file_count",
        f"{n_file} entit" in msg,
        f"entites dans le fichier={n_file} message={msg!r}",
    ))
    out.append((
        "export_refused_while_reconstruction_runs",
        d.get("busy_file_written") is False
        and any(kind == "warning" for kind, _t in (d.get("busy_messages") or [])),
        f"fichier ecrit pendant une reconstruction={d.get('busy_file_written')} "
        f"messages={d.get('busy_messages')} : exporter un etat en cours de "
        f"calcul melange deux dates",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_export_scope_truth.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
