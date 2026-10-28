# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_journal_qgs_save_carry - RecoverLand validation runtime.

Invariant: I-JRN / I-CAPTURE (l'historique d'une session suit le projet la
ou il sera relu ; aucun message d'echec pour un enregistrement reussi).

Le mecanisme
------------
Pour decider si un enregistrement a vraiment ecrit le fichier du projet, le
plugin note, au moment de `projectSaved`, la "signature" du fichier (date,
taille, identifiant) ; si elle n'a pas change ensuite, il conclut a un
enregistrement rate et annule le rattachement de l'historique. Or l'ordre
entre l'ecriture et le signal depend du format (mesure, QGIS 4.2.1) :
  * .qgz (archive) : `projectSaved` est emis AVANT que l'archive soit
    ecrite, meme quand l'enregistrement echoue ; la signature notee est
    celle de l'ancien fichier (ou aucune) ;
  * .qgs : `projectSaved` n'est emis qu'APRES l'ecriture reussie du fichier
    (un echec n'emet rien) ; la signature notee est deja celle du fichier
    que l'enregistrement vient d'ecrire.
Pour un .qgs, la signature "avant" et la signature "apres" etaient donc
identiques, et CHAQUE enregistrement reussi etait classe comme rate.

Le scenario d'echec concret
---------------------------
L'utilisateur modifie des parcelles dans un projet jamais enregistre, fait
"Enregistrer sous" P.qgs. RecoverLand continue d'ecrire dans le journal de
session du profil, sans rien dire. Le lendemain il ouvre P.qgs :
Restaurer / Rewind ne montrent rien de ce qu'il a modifie. Pire, en
repondant "Enregistrer" a "Nouveau projet", il lit "Le projet n'a pas ete
enregistre" alors que QGIS vient de l'enregistrer ; apres un plantage,
l'obligation de rattachement est abandonnee avec un message faux.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin, faux iface, vraie couche GeoPackage, vrais
`QgsProject.write` / `clear` / `read`, fichiers .qgs :
  G1 "Enregistrer sous" d'un projet vierge vers P1.qgs : une modification
     avant, une apres ; le journal doit suivre P1 des l'enregistrement,
     aucun message d'echec ; P1 rouvert : les deux modifications sont dans
     SON journal.
  G2 "Nouveau projet" -> "Enregistrer" P2.qgs : `write()` puis `clear()`
     dans le meme appel ; aucun message "n'a pas ete enregistre" ; P2
     rouvert : la modification est dans son journal.
  G3 "Enregistrer sous" P3.qgs depuis un projet deja enregistre Q.qgz
     (autre dossier) : la modification faite sur Q suit P3.
  G4 "Plantage" : un VRAI processus QGIS fils capture une modification,
     enregistre P4.qgs et s'arrete brutalement ; le processus de test ouvre
     P4 : la modification est dans son journal, rien n'est abandonne.
  G5 (garde) : enregistrements .qgs RATES (fichier en lecture seule,
     dossier absent) : le journal reste celui de la session, aucun message
     de succes, le journal du dossier de l'ancien fichier ne recoit rien.

Verdict attendu avant correctif : FAIL (G1 a G4 ; G5 passe deja, c'est une
garde contre un correctif qui ferait suivre un enregistrement rate).
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess  # nosec B404: processus QGIS fils lance par le test
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_qgs_save_carry"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Suffixed with the run's trace_id (setup): the profile is shared by runs.
_MARKERS = {
    "g1_before": "QGS_G1_AVANT", "g1_after": "QGS_G1_APRES",
    "g2": "QGS_G2_AVANT_NOUVEAU", "g3": "QGS_G3_SUR_Q",
    "g4": "QGS_G4_AVANT_PLANTAGE",
    "g5_ro": "QGS_G5_ECHEC_LECT_SEULE", "g5_missing": "QGS_G5_ECHEC_DOSSIER",
}
# What the user must not read after a save that worked.
_FAILURE_WORDS = ("n'a pas ete enregistre", "n'a pas pu etre verifie",
                  "abandonne", "n'a pas ete ecrit")
_SUCCESS_WORDS = ("Projet enregistre", "rattachee(s)")


def _base():
    from scripts.validation.scenarios import tx_journal_follows_saved_project as base
    return base


def _norm(path) -> str:
    """One spelling per file (the extended-length prefix aside)."""
    if not path:
        return ""
    p = str(path)
    if p.startswith("\\\\?\\"):
        p = p[4:]
    return os.path.normcase(os.path.abspath(p))


def _journal_of(plugin):
    return plugin._journal.path if plugin._journal.is_open else None


def _journal_next_to(project: str) -> str:
    return os.path.join(os.path.dirname(project), ".recoverland",
                        "recoverland_audit.sqlite")


def _said(texts, words) -> list:
    return [t for t in texts or [] if any(w in t for w in words)]


def _blank_with_layer(gpkg: str):
    from qgis.core import QgsProject, QgsVectorLayer
    from scripts.validation.plugin_harness import pump
    QgsProject.instance().clear()
    pump(0.5)
    layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    pump(0.3)
    return layer


def _reopen(plugin, project: str, markers) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    QgsProject.instance().clear()
    pump(0.3)
    res = {"read": bool(QgsProject.instance().read(project))}
    pump(1.0)
    res["journal"] = _journal_of(plugin)
    res["counts_reader"] = _base()._counts_through_reader(plugin, markers)
    res["counts_file"] = {m: _base()._count(_journal_next_to(project), m)
                          for m in markers}
    return res


def _g1(plugin, gpkg: str, p1: str, m: dict) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    b = _base()
    res = {}
    layer = _blank_with_layer(gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 0, m["g1_before"])
    res["captured"] = b._wait_current(plugin, m["g1_before"])
    bar0 = len(plugin.iface.bar.texts())
    res["write"] = bool(QgsProject.instance().write(p1))
    pump(1.5)
    res["journal_after_save"] = _journal_of(plugin)
    res["bar"] = plugin.iface.bar.texts()[bar0:]
    res["commit2"] = b._rename(b._project_layer(), 1, m["g1_after"])
    res["captured2"] = b._wait_current(plugin, m["g1_after"])
    res["reopen"] = _reopen(plugin, p1, (m["g1_before"], m["g1_after"]))
    res["in_session_journal"] = {k: b._count(res["session_journal"], m[k])
                                 for k in ("g1_before", "g1_after")}
    return res


def _g2(plugin, gpkg: str, p2: str, m: dict) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    b = _base()
    res = {}
    layer = _blank_with_layer(gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 2, m["g2"])
    res["captured"] = b._wait_current(plugin, m["g2"])
    bar0 = len(plugin.iface.bar.texts())
    # The answer "Enregistrer" to "Nouveau projet": write, then clear, in
    # one call, no event-loop turn in between.
    res["write"] = bool(QgsProject.instance().write(p2))
    QgsProject.instance().clear()
    pump(1.5)
    res["bar"] = plugin.iface.bar.texts()[bar0:]
    res["reopen"] = _reopen(plugin, p2, (m["g2"],))
    return res


def _g3(plugin, q: str, p3: str, m: dict) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    b = _base()
    res = {}
    QgsProject.instance().clear()
    pump(0.3)
    res["read_q"] = bool(QgsProject.instance().read(q))
    pump(1.0)
    res["journal_q"] = _journal_of(plugin)
    res["commit"] = b._rename(b._project_layer(), 3, m["g3"])
    res["captured"] = b._wait_current(plugin, m["g3"])
    bar0 = len(plugin.iface.bar.texts())
    res["write"] = bool(QgsProject.instance().write(p3))
    pump(1.5)
    res["journal_after_save"] = _journal_of(plugin)
    res["bar"] = plugin.iface.bar.texts()[bar0:]
    res["reopen"] = _reopen(plugin, p3, (m["g3"],))
    return res


def _g4(plugin, base_dir: str, gpkg4: str, p4: str, m: dict) -> dict:
    from scripts.validation.scenarios import tx_journal_save_then_close as stc
    child_py = os.path.join(base_dir, "qgs_crash_after_save_child.py")
    Path(child_py).write_text(stc._CHILD_CODE.format(
        root=str(_PLUGIN_ROOT), parent=str(_PLUGIN_ROOT.parent),
        package=_PLUGIN_ROOT.name, gpkg=gpkg4, marker=m["g4"], project=p4),
        encoding="utf-8")
    res = {"captured": "", "wrote": ""}
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, child_py], stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        res["captured"] = (proc.stdout.readline() or "").strip()
        res["wrote"] = (proc.stdout.readline() or "").strip()
        res["exit_code"] = proc.wait(timeout=120)
    except Exception as exc:  # noqa: BLE001 - a hung child is a FAIL
        res["error"] = f"{type(exc).__name__}: {exc}"
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            pass
    res["project_written"] = os.path.isfile(p4)
    bar0 = len(plugin.iface.bar.texts())
    res["reopen"] = _reopen(plugin, p4, (m["g4"],))
    res["bar"] = plugin.iface.bar.texts()[bar0:]
    return res


def _g5(plugin, gpkg: str, p5_ro: str, p5_missing: str, profile: str,
        m: dict) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    from scripts.validation.scenarios import tx_journal_failed_save_carry as fsc
    b = _base()
    res = {}
    layer = _blank_with_layer(gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit_ro"] = b._rename(layer, 5, m["g5_ro"])
    res["captured_ro"] = b._wait_current(plugin, m["g5_ro"])
    os.chmod(p5_ro, stat.S_IREAD)
    try:
        bar0 = len(plugin.iface.bar.texts())
        res["write_ro"] = bool(QgsProject.instance().write(p5_ro))
        pump(1.5)
        res["journal_after_ro"] = _journal_of(plugin)
        res["bar_ro"] = plugin.iface.bar.texts()[bar0:]
        res["in_old_journal"] = b._count(_journal_next_to(p5_ro), m["g5_ro"])
    finally:
        os.chmod(p5_ro, stat.S_IREAD | stat.S_IWRITE)
    res["commit_missing"] = b._rename(b._project_layer(), 6, m["g5_missing"])
    res["captured_missing"] = b._wait_current(plugin, m["g5_missing"])
    bar0 = len(plugin.iface.bar.texts())
    res["write_missing"] = bool(QgsProject.instance().write(p5_missing))
    pump(1.5)
    res["folder_created"] = os.path.isdir(os.path.dirname(p5_missing))
    res["journal_after_missing"] = _journal_of(plugin)
    res["bar_missing"] = plugin.iface.bar.texts()[bar0:]
    res["elsewhere"] = fsc._journals_holding(
        profile, m["g5_missing"], but=res["session_journal"])
    return res


def setup(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jqgs_")
    ctx.data["tmp"] = tmp
    tag = str(ctx.trace_id)[:8]
    ctx.data["m"] = {k: f"{v}_{tag}" for k, v in _MARKERS.items()}
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    gpkg4 = os.path.join(tmp, "data", "parcelles_g4.gpkg")
    _base()._make_gpkg(gpkg)
    _base()._make_gpkg(gpkg4)
    ctx.data["gpkg"] = gpkg
    ctx.data["gpkg4"] = gpkg4
    paths = {}
    for key, folder, name in (("p1", "dossier_G1", "P1.qgs"),
                              ("p2", "dossier_G2", "P2.qgs"),
                              ("q", "dossier_Q", "Q.qgz"),
                              ("p3", "dossier_G3", "P3.qgs"),
                              ("p4", "dossier_G4", "P4.qgs"),
                              ("p5_ro", "dossier_G5", "P5_old.qgs")):
        os.makedirs(os.path.join(tmp, folder), exist_ok=True)
        paths[key] = os.path.join(tmp, folder, name)
    paths["p5_missing"] = os.path.join(tmp, "cle_usb_retiree", "P5.qgs")
    ctx.data["paths"] = paths
    # Q: a saved project holding the layer; P5_old: an existing .qgs.
    layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    ctx.data["q_written"] = bool(QgsProject.instance().write(paths["q"]))
    QgsProject.instance().clear()
    ctx.data["p5_written"] = bool(QgsProject.instance().write(paths["p5_ro"]))
    QgsProject.instance().clear()
    flog(f"tx_journal_qgs_save_carry setup: trace_id={ctx.trace_id} tmp={tmp}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsApplication
    from recoverland.core import journal_manager as jmod
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    paths = d["paths"]
    m = d["m"]
    profile = QgsApplication.qgisSettingsDirPath()
    told = []
    saved_qlog = getattr(jmod, "qlog", None)   # absent from older versions

    def _spy(msg, level="INFO", *args, **kwargs):
        told.append((str(msg), str(level)))
        if saved_qlog is not None:
            return saved_qlog(msg, level, *args, **kwargs)
        return None

    jmod.qlog = _spy
    plugin = RecoverPlugin(FakeIface())
    try:
        plugin.initGui()
        pump(0.3)
        steps = (
            ("g1", lambda: _g1(plugin, d["gpkg"], paths["p1"], m)),
            ("g2", lambda: _g2(plugin, d["gpkg"], paths["p2"], m)),
            ("g3", lambda: _g3(plugin, paths["q"], paths["p3"], m)),
            ("g4", lambda: _g4(plugin, d["tmp"], d["gpkg4"], paths["p4"], m)),
            ("g5", lambda: _g5(plugin, d["gpkg"], paths["p5_ro"],
                               paths["p5_missing"], profile, m)),
        )
        for key, fn in steps:
            mark = len(told)
            try:
                d[key] = fn()
            except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
                import traceback
                d[key] = {"crash": f"{type(exc).__name__}: {exc}",
                          "trace": traceback.format_exc()[-1500:]}
            d[key]["told"] = told[mark:]
    finally:
        if saved_qlog is not None:
            jmod.qlog = saved_qlog
        else:
            del jmod.qlog
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_journal_qgs_save_carry run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:7000]}", "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def _crash(part: dict) -> str:
    return f"{part.get('crash', '')} {part.get('trace', '')[-400:]}".strip()


def _all_texts(part: dict, *keys) -> list:
    texts = []
    for key in keys:
        texts.extend(part.get(key) or [])
    texts.extend(msg for msg, _lvl in part.get("told") or [])
    return texts


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    m = d.get("m") or {}
    paths = d.get("paths") or {}
    out = []

    g1 = d.get("g1") or {}
    r1 = g1.get("reopen") or {}
    out.append((
        "fixture_g1_enregistrement_qgs_reussi",
        g1.get("captured") == 1 and g1.get("write") is True
        and g1.get("captured2") == 1 and r1.get("read") is True,
        f"capture avant={g1.get('captured')} write(P1.qgs)={g1.get('write')} "
        f"capture apres={g1.get('captured2')} relu={r1.get('read')} "
        f"crash={_crash(g1)!r}",
    ))
    bad1 = _said(_all_texts(g1, "bar"), _FAILURE_WORDS)
    out.append((
        "g1_le_journal_suit_le_projet_qgs_des_l_enregistrement",
        _norm(g1.get("journal_after_save")) == _norm(_journal_next_to(paths.get("p1", "")))
        and not bad1,
        f"journal apres 'Enregistrer sous' P1.qgs={g1.get('journal_after_save')} "
        f"(attendu {_journal_next_to(paths.get('p1', ''))}) ; messages d'echec="
        f"{bad1} : la capture restait dans le journal de session du profil, "
        f"qu'aucun projet n'ouvre",
    ))
    c1 = r1.get("counts_file") or {}
    out.append((
        "g1_projet_qgs_rouvert_a_tout_son_historique",
        _norm(r1.get("journal")) == _norm(_journal_next_to(paths.get("p1", "")))
        and c1.get(m.get("g1_before")) == 1 and c1.get(m.get("g1_after")) == 1,
        f"P1.qgs rouvert, journal={r1.get('journal')} ; dans son journal : {c1} "
        f"(attendu 1 chacune) ; lecture produit={r1.get('counts_reader')} ; "
        f"journal de session={g1.get('in_session_journal')} : Restaurer/Rewind "
        f"ne montrent rien de ce qui a ete modifie avant et apres l'enregistrement",
    ))

    g2 = d.get("g2") or {}
    r2 = g2.get("reopen") or {}
    bad2 = _said(_all_texts(g2, "bar"), _FAILURE_WORDS)
    out.append((
        "g2_nouveau_apres_enregistrer_qgs_ni_faux_echec_ni_perte",
        g2.get("captured") == 1 and g2.get("write") is True
        and r2.get("read") is True and not bad2
        and _norm(r2.get("journal")) == _norm(_journal_next_to(paths.get("p2", "")))
        and (r2.get("counts_file") or {}).get(m.get("g2")) == 1,
        f"'Nouveau' -> 'Enregistrer' P2.qgs : write={g2.get('write')} ; messages "
        f"d'echec affiches={bad2} ; P2 rouvert, journal={r2.get('journal')} ; "
        f"modification dans son journal={(r2.get('counts_file') or {})} (attendu "
        f"1) crash={_crash(g2)!r} : QGIS a enregistre, RecoverLand dit le "
        f"contraire et l'historique reste hors du projet",
    ))

    g3 = d.get("g3") or {}
    r3 = g3.get("reopen") or {}
    bad3 = _said(_all_texts(g3, "bar"), _FAILURE_WORDS)
    out.append((
        "g3_enregistrer_sous_qgs_depuis_un_projet_enregistre",
        d.get("q_written") is True and g3.get("read_q") is True
        and g3.get("captured") == 1 and g3.get("write") is True and not bad3
        and _norm(g3.get("journal_after_save")) == _norm(_journal_next_to(paths.get("p3", "")))
        and (r3.get("counts_file") or {}).get(m.get("g3")) == 1,
        f"Q.qgz -> 'Enregistrer sous' P3.qgs : journal de Q={g3.get('journal_q')} "
        f"; journal apres={g3.get('journal_after_save')} ; messages d'echec="
        f"{bad3} ; dans le journal de P3={(r3.get('counts_file') or {})} (attendu "
        f"1) crash={_crash(g3)!r} : la modification faite sur Q ne suit pas P3",
    ))

    g4 = d.get("g4") or {}
    r4 = g4.get("reopen") or {}
    bad4 = _said(_all_texts(g4, "bar"), _FAILURE_WORDS)
    out.append((
        "fixture_g4_processus_fils",
        (g4.get("captured") or "").startswith("CAPTURED True 1")
        and g4.get("wrote") == "WROTE True" and g4.get("project_written") is True,
        f"processus QGIS fils : {g4.get('captured')!r} {g4.get('wrote')!r} code="
        f"{g4.get('exit_code')} P4.qgs ecrit={g4.get('project_written')} erreur="
        f"{g4.get('error')!r} crash={_crash(g4)!r}",
    ))
    out.append((
        "g4_plantage_apres_enregistrer_qgs_rien_d_abandonne",
        r4.get("read") is True and not bad4
        and _norm(r4.get("journal")) == _norm(_journal_next_to(paths.get("p4", "")))
        and (r4.get("counts_file") or {}).get(m.get("g4")) == 1,
        f"QGIS s'arrete juste apres avoir enregistre P4.qgs ; P4 rouvert, journal="
        f"{r4.get('journal')} ; modification dans son journal="
        f"{(r4.get('counts_file') or {})} (attendu 1) ; messages={bad4} : "
        f"l'obligation etait abandonnee avec 'n'a pas ete ecrit' pour un "
        f"fichier bel et bien ecrit",
    ))

    g5 = d.get("g5") or {}
    ok5 = _said(_all_texts(g5, "bar_ro", "bar_missing"), _SUCCESS_WORDS)
    out.append((
        "g5_garde_enregistrement_qgs_rate_ne_deplace_rien",
        d.get("p5_written") is True and g5.get("captured_ro") == 1
        and g5.get("captured_missing") == 1
        and g5.get("write_ro") is False and g5.get("write_missing") is False
        and g5.get("folder_created") is False
        and _norm(g5.get("journal_after_ro")) == _norm(g5.get("session_journal"))
        and _norm(g5.get("journal_after_missing")) == _norm(g5.get("session_journal"))
        and g5.get("in_old_journal") in (0, -1) and not g5.get("elsewhere")
        and not ok5,
        f"write(lecture seule)={g5.get('write_ro')} write(dossier absent)="
        f"{g5.get('write_missing')} ; journal apres={g5.get('journal_after_ro')} / "
        f"{g5.get('journal_after_missing')} (session={g5.get('session_journal')}) ; "
        f"dans le journal de l'ancien P5={g5.get('in_old_journal')} ; ailleurs="
        f"{g5.get('elsewhere')} ; messages de succes={ok5} crash={_crash(g5)!r} : "
        f"un enregistrement rate ne doit jamais emporter l'historique",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_qgs_save_carry.*trace_id={ctx.trace_id}",
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
