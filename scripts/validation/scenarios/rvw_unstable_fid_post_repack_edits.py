"""rvw_unstable_fid_post_repack_edits - RecoverLand validation runtime.

Invariant: I-RVW-ASOF + ne jamais attribuer a une entite l'etat d'une
autre (une identite non prouvee est nommee et comptee, jamais devinee).

Le mecanisme
------------
Sur un Shapefile ou un GeoJSON (core.commit_guard.fid_is_stable : False),
le fid est la POSITION de l'enregistrement. Une suppression recompacte le
fichier a l'enregistrement : chaque enregistrement situe apres celui qui
est supprime prend le fid precedent. La capture journalise chaque edition
sous le fid qu'elle voit a ce moment : apres un recompactage, les editions
d'un AUTRE enregistrement arrivent sous une cle ``fid:N`` que le journal
porte deja.

Le moteur Review (core/temporal_snapshot_engine.py) regroupait tous les
evenements d'une cle ``fid:N`` en UNE entite (_group_by_entity) et
parcourait son image-avant d'un enregistrement a l'autre :
_attrs_from_later et _geom_before_later prenaient les valeurs de
l'enregistrement suivant, et la reprise des evenements <= T melangeait
deux enregistrements. Seule la completion par la couche actuelle
(snapshot_overlay_session._fetch_live) avait ete refusee sur ces couches,
pas ce parcours.

L'echec concret pour l'utilisateur
----------------------------------
Couche X (Shapefile A, B, C, D). Apres T : C renommee (cle fid:2), A
supprimee (B, C, D passent aux fid 0, 1, 2), D supprimee (cle fid:2, etat
complet), G creee (ajoutee en fin de fichier : cle fid:2 encore), B
deplacee en (9,9) (cle fid:0). A la date T, Review dessinait
en (3,3) une entite "code D, nom Gamma, surface 40" : le nom de C sur
l'enregistrement de D, sans aucun signalement. B et C n'apparaissaient
nulle part et n'etaient comptees nulle part : le deplacement de B etait
fondu dans la vie de A, la carte montrait B en (9,9) comme si elle
n'avait pas change depuis T. L'export annoncait "Export termine", 2
entites, et affirmait que les entites absentes du fichier etaient
identiques aux couches actuelles.

Variantes, dans le meme projet :
  * M (Shapefile) : apres le recompactage, D deplacee au lieu d'etre
    supprimee ; l'entite etait dessinee a la place de D (3,3), ou C n'a
    jamais ete, avec le nom de C ;
  * P (Shapefile) : C renommee et A supprimee AVANT T, D deplacee apres
    T ; D a T etait dessinee a sa place avec le nom que C a recu avant T ;
  * R (Shapefile) : AVANT T, E puis F creees (cles fid:4, fid:5), A
    supprimee (E passe au fid 3, F au fid 4), F renommee Phi2 (cle fid:4) ;
    apres T, F deplacee (cle fid:4). La reprise des evenements <= T
    ajoutait le renommage de F a la creation de E : Review dessinait a la
    place de E une entite "code E, nom Phi2", etat COMPLET, sans aucun
    signalement, alors que E n'a jamais porte ce nom ;
  * G (GeoPackage, fid stables) : les memes editions que X. Rien n'y
    doit changer : les quatre entites y restent entierement connues.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_unstable_fid_post_repack_edits"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FLAG = "_rl_etat_incomplet"
_TAGS = ("X", "M", "P", "R", "G")
_SHP = ("X", "M", "P", "R")
_NAMES = {"X": "sx", "M": "sm", "P": "sp", "R": "sr", "G": "gx"}
# Enregistrements existant a T et modifies apres T, par couche : chacun
# doit etre montre dans son etat a T ou compte, jamais omis en silence.
_CHANGED_AFTER_T = {"X": 4, "M": 3, "P": 1, "R": 1, "G": 4}
# Ce que la capture journalise (operation, cle) : la precondition des fid
# renumerotes, verifiee avant tout.
_KEYS = {
    "sx": [("UPDATE", "fid:2"), ("DELETE", "fid:0"), ("DELETE", "fid:2"),
           ("INSERT", "fid:2"), ("UPDATE", "fid:0")],
    "sr": [("INSERT", "fid:4"), ("INSERT", "fid:5"), ("DELETE", "fid:0"),
           ("UPDATE", "fid:4"), ("UPDATE", "fid:4")],
}
_N_COMMITS = 20


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _fx():
    from scripts.validation.scenarios import _rvw_unstable_fid
    return _rvw_unstable_fid


def _truth() -> dict:
    """Etat a T de chaque enregistrement, par couche."""
    rows = {code: {"code": code, "nom": nom, "surface": float(s), "xy": (x, y)}
            for code, nom, s, x, y in _fx().ROWS}
    p = {k: dict(v) for k, v in rows.items() if k != "A"}
    p["C"]["nom"] = "Gamma2"
    r = {k: dict(v) for k, v in rows.items() if k != "A"}
    r["E"] = {"code": "E", "nom": "Epsilon", "surface": 60.0, "xy": (6.0, 6.0)}
    r["F"] = {"code": "F", "nom": "Phi2", "surface": 70.0, "xy": (7.0, 7.0)}
    return {"X": rows, "M": rows, "P": p, "R": r, "G": rows}


def _norm(code, nom, surface, xy) -> dict:
    h = _h()
    out = {}
    if code is not None and not h.is_null(code):
        out["code"] = str(code)
    if nom is not None and not h.is_null(nom):
        out["nom"] = str(nom)
    if surface is not None and not h.is_null(surface):
        try:
            out["surface"] = float(surface)
        except (TypeError, ValueError):
            out["surface"] = str(surface)
    if xy is not None:
        out["xy"] = (round(float(xy[0]), 3), round(float(xy[1]), 3))
    return out


def _state(feat):
    from recoverland.core.geometry_utils import rebuild_geometry
    attrs = json.loads(feat.attrs_json) if feat.attrs_json else {}
    pt = None
    if feat.geom_wkb:
        g = rebuild_geometry(feat.geom_wkb)
        if g is not None and not g.isNull():
            p = g.asPoint()
            pt = (p.x(), p.y())
    out = _norm(attrs.get("code"), attrs.get("nom"), attrs.get("surface"), pt)
    out["unresolved"] = getattr(feat, "unresolved", "")
    return out


def _foreign(state: dict, truth: dict):
    """None when every known value of *state* is the value of ONE record at T."""
    known = {k: v for k, v in state.items() if k in ("code", "nom", "surface", "xy")}
    if not known:
        return None
    for rec in truth.values():
        if all(rec.get(k) == v for k, v in known.items()):
            return None
    return known


def _read_gpkg(path: str) -> dict:
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
        fields = lyr.fields().names()
        rows = []
        for f in lyr.getFeatures():
            g = f.geometry()
            pt = None
            if g is not None and not g.isNull() and not g.isEmpty():
                p = g.asPoint()
                pt = (p.x(), p.y())
            row = _norm(f["code"], f["nom"], f["surface"], pt)
            flag = f[_FLAG] if _FLAG in fields else None
            row["flag"] = None if _h().is_null(flag) else str(flag)
            rows.append(row)
        out["layers"][name] = rows
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_rpk_")
    ctx.data["tmpdir"] = tmpdir
    paths = {}
    for tag in _SHP:
        sub = os.path.join(tmpdir, tag.lower())
        os.makedirs(sub)
        paths[tag] = _fx().make_shp(sub, _NAMES[tag])
    paths["G"] = _h().make_gpkg(tmpdir, _NAMES["G"], _fx().ROWS)
    ctx.data["paths"] = paths
    flog(f"rvw_unstable_fid_post_repack_edits setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _fids(layer) -> list:
    return sorted((f.id(), f["code"]) for f in layer.getFeatures())


def _edit(ctx, plugin, layers) -> str:
    """Les editions reelles des quatre couches ; renvoie T."""
    h = _h()
    d = ctx.data
    jpath = h.journal_path(plugin)
    rows0 = h.journal_rows(jpath)
    commits = []
    fids = {tag: [_fids(layers[tag])] for tag in _TAGS}
    lp = layers["P"]
    commits.append(h.user_commit(lp, lambda lyr: h.rename(lyr, "C", "Gamma2")))
    commits.append(h.user_commit(lp, lambda lyr: h.delete(lyr, "A")))
    lp.reload()
    fids["P"].append(_fids(lp))
    lr = layers["R"]
    commits.append(h.user_commit(
        lr, lambda lyr: h.add(lyr, "E", "Epsilon", 60.0, 6.0, 6.0)))
    lr.reload()
    commits.append(h.user_commit(
        lr, lambda lyr: h.add(lyr, "F", "Phi", 70.0, 7.0, 7.0)))
    lr.reload()
    commits.append(h.user_commit(lr, lambda lyr: h.delete(lyr, "A")))
    lr.reload()
    commits.append(h.user_commit(lr, lambda lyr: h.rename(lyr, "F", "Phi2")))
    lr.reload()
    fids["R"].append(_fids(lr))
    time.sleep(1.2)
    cutoff = h.utc_now_iso()
    time.sleep(1.2)
    for tag in ("X", "M", "G"):
        lyr = layers[tag]
        commits.append(h.user_commit(lyr, lambda v: h.rename(v, "C", "Gamma2")))
        commits.append(h.user_commit(lyr, lambda v: h.delete(v, "A")))
        lyr.reload()
        fids[tag].append(_fids(lyr))
    for tag in ("X", "G"):
        lyr = layers[tag]
        commits.append(h.user_commit(lyr, lambda v: h.delete(v, "D")))
        lyr.reload()
        if tag == "X":
            # Created after T, appended at the end: fid 2 again.
            commits.append(h.user_commit(
                lyr, lambda v: h.add(v, "G", "Gimel", 80.0, 12.0, 12.0)))
            lyr.reload()
        commits.append(h.user_commit(lyr, lambda v: h.move(v, "B", 9.0, 9.0)))
        lyr.reload()
    commits.append(h.user_commit(layers["R"], lambda v: h.move(v, "F", 8.0, 8.0)))
    layers["R"].reload()
    for tag in ("M", "P"):
        lyr = layers[tag]
        commits.append(h.user_commit(lyr, lambda v: h.move(v, "D", 5.0, 5.0)))
        lyr.reload()
    for tag in _TAGS:
        fids[tag].append(_fids(layers[tag]))
    d["commits"] = commits
    d["fids"] = fids
    d["journal_rows"] = h.wait_rows(jpath, rows0 + _N_COMMITS) - rows0
    import sqlite3
    conn = sqlite3.connect(jpath)
    try:
        d["journal"] = [tuple(r) for r in conn.execute(
            "SELECT event_id, layer_name_snapshot, operation_type, "
            "entity_fingerprint, created_at FROM audit_event ORDER BY event_id")]
    finally:
        conn.close()
    return cutoff


def run(ctx):
    from qgis.PyQt import QtWidgets
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    h = _h()
    fx = _fx()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    saved_dialog = QtWidgets.QFileDialog.getSaveFileName
    try:
        layers = {tag: fx.load_shp(d["paths"][tag], _NAMES[tag])
                  for tag in _SHP}
        layers["G"] = h.load_layer(d["paths"]["G"], _NAMES["G"])
        h.pump(0.2)
        cutoff = _edit(ctx, plugin, layers)
        d["cutoff"] = cutoff
        fps = {tag: compute_datasource_fingerprint(layers[tag]) for tag in _TAGS}

        h.set_view(iface, -5.0, -5.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layers[t] for t in _TAGS])
        d["review_on"] = h.review_on(dlg, len(_TAGS))
        n_bar0 = len(iface.bar.messages)
        d["review_date"] = h.review_date(dlg, cutoff)
        raw = dlg._review_snap_raw_result
        d["review"] = {
            tag: {k: _state(f) for k, f in
                  ((raw.features.get(fps[tag]) or {}).items() if raw else [])}
            for tag in _TAGS}
        d["n_unresolved"] = getattr(raw, "n_unresolved", None)
        d["n_identity_unproven"] = getattr(raw, "n_identity_unproven", None)
        d["n_born_after"] = getattr(raw, "n_born_after", None)
        d["overlay"] = {}
        for lyr in h.overlay_layers(dlg):
            rows = []
            for f in lyr.getFeatures():
                g = f.geometry()
                pt = None
                if g is not None and not g.isNull() and not g.isEmpty():
                    p = g.asPoint()
                    pt = (p.x(), p.y())
                row = _norm(f["code"], f["nom"], f["surface"], pt)
                row["flag"] = (str(f[_FLAG]) if _FLAG in f.fields().names()
                               and not h.is_null(f[_FLAG]) else None)
                rows.append(row)
            d["overlay"][lyr.name()] = rows
        bar = dlg._review_date_bar
        d["bar_tip"] = bar._lbl_status.toolTip() if bar is not None else None
        d["bar_text"] = bar._lbl_status.text() if bar is not None else None
        d["message_bar"] = [f"{t} | {x}" for t, x, _l, _d in iface.bar.messages[n_bar0:]]

        out_path = os.path.join(d["tmpdir"], "etat.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (out_path, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["export_calls"] = boxes.calls
        d["file"] = _read_gpkg(out_path)
        h.review_off(dlg)

        # The Shapefile X alone in Review, as the user who has only it: the
        # other layers' flagged entities no longer make the export partial.
        dlg._apply_journal_layers([{"name": layers["X"].name(),
                                    "fingerprint": fps["X"]}])
        d["x_review_on"] = h.review_on(dlg, 1)
        d["x_review_date"] = h.review_date(dlg, cutoff)
        out_x = os.path.join(d["tmpdir"], "etat_x.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (out_x, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["x_export_calls"] = boxes.calls
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        QtWidgets.QFileDialog.getSaveFileName = saved_dialog
        h.shutdown(plugin)
    flog(f"rvw_unstable_fid_post_repack_edits run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _overlay_of(d: dict, tag: str) -> list:
    name = _NAMES[tag]
    return [r for lname, rows in (d.get("overlay") or {}).items()
            if lname.split(" ")[0] == name for r in rows]


def _file_of(d: dict, tag: str) -> list:
    name = _NAMES[tag]
    return [r for lname, rows in ((d.get("file") or {}).get("layers") or {}).items()
            if lname == name for r in rows]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    truth = _truth()
    rev = d.get("review") or {}
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-600:]}" if d.get("crash") else ""
    fids = d.get("fids") or {}
    out = []
    keys = {}
    for _eid, lname, op, key, _at in d.get("journal") or []:
        keys.setdefault(lname, []).append((op, key))
    want_keys = {n: keys.get(n) for n in _KEYS}
    out.append((
        "setup_real_edits_and_repacks",
        d.get("commits") == [True] * _N_COMMITS
        and d.get("journal_rows") == _N_COMMITS
        and fids.get("P", [None, None])[1] == [(0, "B"), (1, "C"), (2, "D")]
        and (fids.get("X") or [None, None])[1] == [(0, "B"), (1, "C"), (2, "D")]
        and (fids.get("X") or [None])[-1] == [(0, "B"), (1, "C"), (2, "G")]
        and (fids.get("R") or [None, None])[1]
        == [(0, "B"), (1, "C"), (2, "D"), (3, "E"), (4, "F")]
        and (fids.get("G") or [None])[-1] == [(2, "B"), (3, "C")]
        and want_keys == _KEYS,
        f"commits={d.get('commits')} lignes={d.get('journal_rows')} fid={fids} "
        f"cles={want_keys} (Shapefile : fid renumerotes a chaque suppression ; "
        f"GeoPackage : fid stables){crash}",
    ))

    mixed = {tag: [(k, s) for k, s in (rev.get(tag) or {}).items()
                   if _foreign(s, truth[tag]) is not None]
             for tag in _TAGS}
    out.append((
        "no_reconstructed_entity_mixes_two_records",
        bool(rev) and not any(mixed.values()),
        f"entites melant deux enregistrements={mixed} : l'etat a T d'une "
        f"entite doit etre celui d'UN enregistrement (nom de C sur la place "
        f"ou la surface de D = une entite qui n'a jamais existe){crash}",
    ))

    drawn_mixed = {tag: [r for r in _overlay_of(d, tag)
                         if _foreign(r, truth[tag]) is not None]
                   for tag in _TAGS}
    out.append((
        "overlay_draws_no_mixed_record",
        bool(d.get("overlay")) and not any(drawn_mixed.values()),
        f"dessine sur la carte={drawn_mixed} (calques={d.get('overlay')}) : "
        f"l'utilisateur voit a la date T une entite qui n'a jamais existe{crash}",
    ))

    n_shown = sum(len(rev.get(tag) or {}) for tag in _TAGS)
    n_unproven = d.get("n_identity_unproven") or 0
    need = sum(_CHANGED_AFTER_T.values())
    out.append((
        "every_change_after_T_shown_or_counted",
        n_shown + n_unproven >= need,
        f"entites reconstituees={n_shown} + modifications sans identite "
        f"prouvee={d.get('n_identity_unproven')} < {need} enregistrements "
        f"modifies apres T : B, deplacee apres le recompactage, est omise "
        f"sans rien dire et la carte actuelle la montre comme inchangee "
        f"depuis T (Review X={rev.get('X')}){crash}",
    ))

    tip = d.get("bar_tip") or ""
    bar_msgs = " || ".join(d.get("message_bar") or [])
    out.append((
        "renumbered_stretches_classified",
        d.get("n_identity_unproven") == 3 and d.get("n_born_after") == 1,
        f"n_identity_unproven={d.get('n_identity_unproven')} (attendu 3 : X "
        f"suppression de D et deplacement de B, M deplacement de D, tous "
        f"apres un renumerotage) n_born_after={d.get('n_born_after')} "
        f"(attendu 1 : G, creee apres T sous un fid deja journalise, "
        f"n'existait pas a T){crash}",
    ))

    out.append((
        "unproven_changes_named_on_the_bar_and_message_bar",
        n_unproven >= 1 and "identité prouvée" in tip
        and "identité non prouvée" in bar_msgs,
        f"n_identity_unproven={d.get('n_identity_unproven')} infobulle={tip!r} "
        f"barre de messages={bar_msgs!r} : les modifications que Review ne "
        f"peut rattacher a aucune entite a T doivent etre annoncees{crash}",
    ))

    calls = d.get("export_calls") or []
    kinds = [k for k, _t in calls]
    texts = " || ".join(t for _k, t in calls)
    info = (d.get("file") or {}).get("info") or {}
    out.append((
        "export_is_partial_and_names_the_unproven_changes",
        "information" not in kinds and "Export partiel" in texts
        and "identité prouvée" in texts
        and info.get("entites_modifiees_identite_non_prouvee_non_ecrites")
        == str(n_unproven) and n_unproven >= 1,
        f"messages={calls} table info={info} : un fichier 'etat a T' qui ne "
        f"contient pas toutes les entites modifiees ne peut pas etre annonce "
        f"'Export termine' ni dire que les absentes sont identiques aux "
        f"couches actuelles{crash}",
    ))

    xcalls = d.get("x_export_calls") or []
    xkinds = [k for k, _t in xcalls]
    xtexts = " || ".join(t for _k, t in xcalls)
    out.append((
        "export_of_the_shapefile_alone_is_partial",
        d.get("x_review_on") is True and bool(xcalls)
        and "information" not in xkinds and "Export partiel" in xtexts
        and "identité prouvée" in xtexts,
        f"Review de X seule (review_on={d.get('x_review_on')}), export={xcalls} : "
        f"'Export termine' annoncait un fichier complet dont B, C et D sont "
        f"absents ou faux{crash}",
    ))

    written_mixed = {tag: [r for r in _file_of(d, tag)
                           if _foreign(r, truth[tag]) is not None]
                     for tag in _TAGS}
    out.append((
        "export_writes_no_mixed_record",
        bool((d.get("file") or {}).get("layers")) and not any(written_mixed.values()),
        f"ecrit dans le fichier={written_mixed} (fichier={(d.get('file') or {}).get('layers')})"
        f" : le fichier garde comme 'etat a T' une entite qui n'a jamais "
        f"existe{crash}",
    ))

    g = rev.get("G") or {}
    g_states = sorted((s.get("code"), s.get("nom"), s.get("surface"), s.get("xy"),
                       s.get("unresolved")) for s in g.values())
    expected_g = sorted((r["code"], r["nom"], r["surface"], r["xy"], "")
                        for r in truth["G"].values())
    out.append((
        "stable_fid_layer_fully_known",
        g_states == expected_g,
        f"GeoPackage (fid stables) a T={g_states}, attendu {expected_g} : "
        f"les memes editions sur une couche dont les fid ne bougent pas "
        f"doivent rester entierement connues{crash}",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"rvw_unstable_fid_post_repack_edits.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
