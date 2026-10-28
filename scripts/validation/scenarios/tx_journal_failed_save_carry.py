# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_journal_failed_save_carry - RecoverLand validation runtime.

Invariant: I-JRN / I-CAPTURE (l'historique d'une session suit le projet la
ou il sera relu ; aucun message de succes pour ce qui n'a pas eu lieu).

Le mecanisme
------------
`QgsProject.projectSaved` est emis DANS `QgsProject.write()`, et il l'est
aussi quand l'enregistrement ECHOUE (mesure, QGIS 4.2.1) :
  * vers un dossier qui n'existe pas (cle USB retiree) : `write()` renvoie
    False, le fichier n'existe pas, le dossier non plus ;
  * par-dessus un fichier en lecture seule : `write()` renvoie False, le
    fichier reste intact (meme date, meme taille, meme identifiant).
Le plugin planifiait pourtant le rattachement de l'historique :
  * dossier absent : vers un journal "indice" du PROFIL, calcule sur le
    chemin voulu, qu'aucun projet n'ouvre ; la capture y basculait, avec le
    message "Projet enregistre : N modification(s) rattachee(s)". Apres un
    enregistrement reussi ailleurs, le journal du projet n'avait pas ce
    qui avait ete capture avant l'echec ;
  * fichier en lecture seule : `isfile()` etant vrai, l'historique de la
    session partait dans le journal du dossier de l'ANCIEN projet, qui n'a
    pas ete remplace, avec le meme message de succes.

Le scenario d'echec concret
---------------------------
L'utilisateur modifie des parcelles dans un projet jamais enregistre, fait
"Enregistrer sous" sur sa cle USB qu'il vient de retirer : QGIS dit que
l'enregistrement a echoue, RecoverLand dit "Projet enregistre". Il
enregistre dans un dossier du disque. Le lendemain il ouvre ce projet :
Restaurer / Rewind ne montrent pas ce qu'il avait modifie avant l'echec.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin, faux iface, vraie couche GeoPackage, vrais
`QgsProject.write` / `clear` / `read` :
  A (dossier absent) : une modification ; `write(<dossier absent>/P.qgz)`
    (False) ; le journal reste celui de la session, aucun message de
    succes, aucun autre journal ne recoit la modification ; une seconde
    modification ; `write(<dossier existant>/P.qgz)` ; reouverture : les
    deux modifications sont lues dans le journal de P, et c'est bien lui
    qui est rouvert.
  B (fichier en lecture seule) : un projet existant P_old en lecture
    seule ; une modification dans un projet vierge ; `write(P_old)`
    (False) : le journal reste celui de la session, aucun message de
    succes, le journal du dossier de P_old ne recoit rien ; P_old redevient
    inscriptible, Ctrl+S (`write()`) : la modification est alors dans le
    journal de P_old.

Verdict attendu avant correctif : FAIL (A et B).
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_failed_save_carry"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Suffixed with the run's trace_id (setup): the search for journals that
# received a marker walks the whole profile, where earlier runs leave theirs.
_A_BEFORE = "FS_AVANT_ECHEC"
_A_BETWEEN = "FS_ENTRE_LES_DEUX"
_B_BEFORE = "FS_AVANT_ECHEC_LECT_SEULE"
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


def _saved_journal(project: str) -> str:
    return os.path.join(os.path.dirname(project), ".recoverland",
                        "recoverland_audit.sqlite")


def _journals_holding(root: str, marker: str, but: str = "") -> list:
    """Every journal under *root* that holds *marker*, except *but*."""
    hits = []
    for folder, _dirs, files in os.walk(root):
        for name in files:
            if not name.endswith(".sqlite"):
                continue
            path = os.path.join(folder, name)
            if but and _norm(path) == _norm(but):
                continue
            if _base()._count(path, marker) > 0:
                hits.append(path)
    return hits


def _success_told(texts) -> list:
    return [t for t in texts or [] if any(w in t for w in _SUCCESS_WORDS)]


def _part_a(plugin, tmp: str, gpkg: str, profile: str, m: dict) -> dict:
    from qgis.core import QgsProject, QgsVectorLayer
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {}
    bad = os.path.join(tmp, "cle_usb_retiree", "P.qgz")
    good = os.path.join(tmp, "dossier_ok", "P.qgz")
    os.makedirs(os.path.dirname(good), exist_ok=True)
    res["expected_journal"] = _saved_journal(good)
    QgsProject.instance().clear()
    pump(0.5)
    layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    pump(0.3)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(b._project_layer(), 0, m["a_before"])
    res["captured"] = b._wait_current(plugin, m["a_before"])
    bar0 = len(plugin.iface.bar.texts())
    res["write_bad"] = bool(QgsProject.instance().write(bad))
    pump(1.5)
    res["folder_created"] = os.path.isdir(os.path.dirname(bad))
    res["journal_after_failed_save"] = _journal_of(plugin)
    res["bar_after_failed_save"] = plugin.iface.bar.texts()[bar0:]
    res["elsewhere_after_failed_save"] = _journals_holding(
        profile, m["a_before"], but=res["session_journal"])
    res["commit2"] = b._rename(b._project_layer(), 1, m["a_between"])
    res["captured2"] = b._wait_current(plugin, m["a_between"])
    res["write_good"] = bool(QgsProject.instance().write(good))
    pump(1.5)
    res["journal_after_good_save"] = _journal_of(plugin)
    QgsProject.instance().clear()
    pump(0.5)
    res["read_good"] = bool(QgsProject.instance().read(good))
    pump(1.0)
    res["reopened_journal"] = _journal_of(plugin)
    counts = b._counts_through_reader(plugin, (m["a_before"], m["a_between"]))
    res["counts"] = {"before": counts.get(m["a_before"]),
                     "between": counts.get(m["a_between"])}
    return res


def _part_b(plugin, tmp: str, gpkg: str, p_old: str, m: dict) -> dict:
    from qgis.core import QgsProject, QgsVectorLayer
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {}
    old_journal = _saved_journal(p_old)
    res["old_journal"] = old_journal
    QgsProject.instance().clear()
    pump(0.5)
    layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    pump(0.3)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(b._project_layer(), 2, m["b_before"])
    res["captured"] = b._wait_current(plugin, m["b_before"])
    os.chmod(p_old, stat.S_IREAD)
    try:
        st = os.stat(p_old)
        res["stamp_before"] = [st.st_mtime_ns, st.st_size, st.st_ino]
        bar0 = len(plugin.iface.bar.texts())
        res["write_ro"] = bool(QgsProject.instance().write(p_old))
        pump(1.5)
        st = os.stat(p_old)
        res["stamp_after"] = [st.st_mtime_ns, st.st_size, st.st_ino]
        res["journal_after_failed_save"] = _journal_of(plugin)
        res["bar_after_failed_save"] = plugin.iface.bar.texts()[bar0:]
        res["in_old_journal_after_failed_save"] = b._count(old_journal, m["b_before"])
    finally:
        os.chmod(p_old, stat.S_IREAD | stat.S_IWRITE)
    res["ctrl_s"] = bool(QgsProject.instance().write())
    pump(1.5)
    res["journal_after_ctrl_s"] = _journal_of(plugin)
    res["in_old_journal_after_ctrl_s"] = b._count(old_journal, m["b_before"])
    return res


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jfs_")
    ctx.data["tmp"] = tmp
    tag = str(ctx.trace_id)[:8]
    ctx.data["m"] = {"a_before": f"{_A_BEFORE}_{tag}",
                     "a_between": f"{_A_BETWEEN}_{tag}",
                     "b_before": f"{_B_BEFORE}_{tag}"}
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    _base()._make_gpkg(gpkg)
    ctx.data["gpkg"] = gpkg
    p_old = os.path.join(tmp, "ancien", "P_old.qgz")
    os.makedirs(os.path.dirname(p_old), exist_ok=True)
    ctx.data["p_old"] = p_old
    ctx.data["p_old_written"] = bool(QgsProject.instance().write(p_old))
    QgsProject.instance().clear()
    flog(f"tx_journal_failed_save_carry setup: trace_id={ctx.trace_id} tmp={tmp}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsApplication
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    profile = QgsApplication.qgisSettingsDirPath()
    plugin = RecoverPlugin(FakeIface())
    try:
        plugin.initGui()
        pump(0.3)
        for key, fn in (("a", lambda: _part_a(plugin, d["tmp"], d["gpkg"], profile,
                                              d["m"])),
                        ("b", lambda: _part_b(plugin, d["tmp"], d["gpkg"], d["p_old"],
                                              d["m"]))):
            try:
                d[key] = fn()
            except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
                import traceback
                d[key] = {"crash": f"{type(exc).__name__}: {exc}",
                          "trace": traceback.format_exc()[-1500:]}
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_journal_failed_save_carry run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:6000]}", "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    a = d.get("a") or {}
    out.append((
        "fixture_a_echec_reel_de_l_enregistrement",
        a.get("captured") == 1 and a.get("write_bad") is False
        and a.get("folder_created") is False and a.get("captured2") == 1
        and a.get("write_good") is True,
        f"capture avant={a.get('captured')} write(dossier absent)="
        f"{a.get('write_bad')} dossier cree={a.get('folder_created')} capture "
        f"entre les deux={a.get('captured2')} write(dossier ok)="
        f"{a.get('write_good')} crash={a.get('crash')} {a.get('trace', '')[-400:]}",
    ))
    told = _success_told(a.get("bar_after_failed_save"))
    out.append((
        "a_echec_ni_bascule_ni_message_de_succes",
        _norm(a.get("journal_after_failed_save")) == _norm(a.get("session_journal"))
        and not told and not a.get("elsewhere_after_failed_save"),
        f"journal apres l'echec={a.get('journal_after_failed_save')} (session="
        f"{a.get('session_journal')}) ; messages de succes={told} ; autres "
        f"journaux qui ont recu la modification={a.get('elsewhere_after_failed_save')} "
        f": l'historique partait dans un journal du profil qu'aucun projet "
        f"n'ouvre, sous le message 'Projet enregistre'",
    ))
    counts = a.get("counts") or {}
    out.append((
        "a_enregistrement_suivant_emporte_tout_l_historique",
        a.get("read_good") is True
        and _norm(a.get("reopened_journal")) == _norm(a.get("expected_journal"))
        and counts.get("before") == 1 and counts.get("between") == 1,
        f"projet rouvert, journal={a.get('reopened_journal')} (attendu "
        f"{a.get('expected_journal')}) ; lues={counts} (attendu 1 chacune) : "
        f"ce qui a ete modifie avant l'echec manquait au projet enregistre",
    ))
    bpart = d.get("b") or {}
    out.append((
        "fixture_b_fichier_en_lecture_seule_intact",
        d.get("p_old_written") is True and bpart.get("captured") == 1
        and bpart.get("write_ro") is False
        and bpart.get("stamp_before") == bpart.get("stamp_after")
        and bpart.get("ctrl_s") is True,
        f"P_old ecrit={d.get('p_old_written')} capture={bpart.get('captured')} "
        f"write(lecture seule)={bpart.get('write_ro')} fichier intact="
        f"{bpart.get('stamp_before') == bpart.get('stamp_after')} Ctrl+S="
        f"{bpart.get('ctrl_s')} crash={bpart.get('crash')} "
        f"{bpart.get('trace', '')[-400:]}",
    ))
    told_b = _success_told(bpart.get("bar_after_failed_save"))
    out.append((
        "b_echec_par_dessus_un_fichier_ni_rattachement_ni_succes",
        _norm(bpart.get("journal_after_failed_save")) == _norm(bpart.get("session_journal"))
        and not told_b
        and bpart.get("in_old_journal_after_failed_save") in (0, -1),
        f"journal apres l'echec={bpart.get('journal_after_failed_save')} "
        f"(session={bpart.get('session_journal')}) ; messages de succes={told_b} "
        f"; modification dans le journal de l'ANCIEN projet="
        f"{bpart.get('in_old_journal_after_failed_save')} (attendu 0 ou absent) "
        f": l'historique partait chez un projet qui n'a pas ete remplace",
    ))
    out.append((
        "b_ctrl_s_reussi_emporte_l_historique",
        _norm(bpart.get("journal_after_ctrl_s")) == _norm(bpart.get("old_journal"))
        and bpart.get("in_old_journal_after_ctrl_s") == 1,
        f"apres Ctrl+S reussi : journal={bpart.get('journal_after_ctrl_s')} "
        f"(attendu {bpart.get('old_journal')}) ; modification presente="
        f"{bpart.get('in_old_journal_after_ctrl_s')} (attendu 1)",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_failed_save_carry.*trace_id={ctx.trace_id}",
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
