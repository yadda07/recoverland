"""tx_journal_save_then_close - RecoverLand validation runtime.

Invariant: I-JRN / I-CAPTURE (un evenement capture finit dans le journal
durable que le plugin lira pour ces donnees ; il reste joignable).

Le mecanisme
------------
QGIS enregistre tres souvent un projet JUSTE avant de le fermer : "Nouveau
projet", "Ouvrir un projet" et "Quitter" posent la question "Enregistrer
les modifications ?". Repondre "Enregistrer" appelle `QgsProject.write(P)`
puis `clear()` (ou la fermeture de QGIS) dans le MEME appel, sans aucun tour
de boucle d'evenements entre les deux.

Le rattachement de l'historique d'une session au journal du projet enregistre
(tx_journal_follows_saved_project) etait differe d'un tour de boucle, parce
que `projectSaved` est emis DANS `write()`, avant que le .qgz existe sur le
disque. Or ce tour n'arrive qu'apres `clear()` : le journal de session est
deja ferme, le projet deja vide ; le rattachement ne sait plus ou aller et
l'historique reste dans le fichier de session du profil, qu'aucun projet
n'ouvre. A la fermeture de QGIS, ce tour n'arrive jamais. Un plantage de
QGIS apres l'enregistrement a le meme effet : rien de durable ne disait que
cet historique appartenait desormais a P.

Le scenario d'echec concret
---------------------------
L'utilisateur demarre QGIS, modifie des parcelles dans un projet jamais
enregistre, clique "Nouveau projet", repond "Enregistrer" et nomme P.qgz.
Le lendemain il ouvre P.qgz : Restaurer / Rewind ne montrent rien de ce qu'il
a modifie la veille. Meme chose s'il a quitte QGIS par la question
"Enregistrer ?", ou si QGIS a plante apres l'enregistrement.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin, faux iface, vraie couche GeoPackage, vrais
`QgsProject.write` / `clear` / `read` :
  S1 "Nouveau"  : un commit ; `write(P1)` puis `clear()` dans le meme appel ;
     puis `read(P1)` : la modification doit etre lue dans le journal de P1,
     et le nouveau projet vierge ne doit PAS journaliser dans celui de P1.
  S2 "Ouvrir"   : un commit ; `write(P2)` puis `read(Q)` dans le meme appel ;
     puis `read(P2)`.
  S3 "Quitter"  : un commit ; `write(P3)` puis `unload()` du plugin dans le
     meme appel ; un NOUVEAU plugin (le demarrage suivant) ouvre P3.
  S4 "Plantage" : un VRAI processus QGIS fils (code produit, meme profil)
     capture un commit, enregistre P4 puis se termine brutalement
     (`os._exit`) sans aucun tour de boucle ; le processus de test ouvre P4.
  S5 "Journal du projet tenu ailleurs" (non-regression du repli) : une
     vraie seconde instance tient le journal de P5 ; "Enregistrer sous" P5 :
     la capture doit continuer dans le journal de session (rien de coupe,
     l'utilisateur est prevenu) ; l'autre instance se ferme ; Ctrl+S :
     tout l'historique doit etre dans le journal de P5.

Verdict attendu avant correctif : FAIL (S1 a S4, S5 sur le code d'origine).
Sur le code d'origine rien n'ecoute `projectSaved` ; sur le v7 avant ce
correctif, le rattachement differe ne trouve plus sa source.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess  # nosec B404: processus QGIS fils lance par le test
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_save_then_close"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_S1 = "S1_AVANT_NOUVEAU"
_S2 = "S2_AVANT_OUVRIR"
_S3 = "S3_AVANT_QUITTER"
_S4 = "S4_AVANT_PLANTAGE"
_S1_AFTER = "S1_PROJET_VIERGE"
_S5 = ("S5_AVANT_SAUV", "S5_PENDANT_VERROU")

# Another QGIS instance holding a project's journal (product code).
_HOLDER_CODE = r'''
import os, sys
sys.path.insert(0, {parent!r})
from qgis.core import QgsApplication
app = QgsApplication([], False)
app.initQgis()
from {package}.core.journal_manager import JournalManager
jm = JournalManager()
try:
    jm.open_for_project({project!r}, {profile!r})
    print("READY " + str(os.getpid()), flush=True)
except Exception as exc:
    print("FAILED " + type(exc).__name__ + ": " + str(exc), flush=True)
    sys.exit(3)
sys.stdin.readline()
jm.close()
print("CLOSED", flush=True)
'''

# A real QGIS process: the product captures one commit, saves the project,
# and dies on the spot, like a crash right after the save.
_CHILD_CODE = r'''
import os, sys, time, sqlite3
sys.path.insert(0, {root!r})
sys.path.insert(0, {parent!r})
prefix = os.environ.get("QGIS_PREFIX_PATH")
from qgis.core import QgsApplication
if prefix:
    QgsApplication.setPrefixPath(prefix, True)
app = QgsApplication([], False)
app.initQgis()
from qgis.core import QgsProject, QgsVectorLayer
from {package}.recover import RecoverPlugin
from {package}.core.journal_manager import sqlite_readonly_uri
from scripts.validation.plugin_harness import FakeIface, pump
plugin = RecoverPlugin(FakeIface())
plugin.initGui()
pump(0.3)
layer = QgsVectorLayer({gpkg!r} + "|layername=parcelles", "parcelles", "ogr")
QgsProject.instance().addMapLayer(layer)
idx = layer.fields().indexFromName("nom")
fid = [f.id() for f in layer.getFeatures() if f["code"] == "C000"][0]
layer.startEditing()
layer.changeAttributeValue(fid, idx, {marker!r})
ok = layer.commitChanges()
journal = plugin._journal.path if plugin._journal.is_open else ""
seen = 0
deadline = time.monotonic() + 8.0
while journal and not seen and time.monotonic() < deadline:
    pump(0.1)
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
        seen = conn.execute("SELECT COUNT(*) FROM audit_event WHERE "
                            "attributes_json LIKE ?", ("%" + {marker!r} + "%",)).fetchone()[0]
        conn.close()
    except sqlite3.Error:
        pass
print("CAPTURED " + str(ok) + " " + str(seen) + " " + journal, flush=True)
wrote = QgsProject.instance().write({project!r})
print("WROTE " + str(wrote), flush=True)
os._exit(9)
'''


def _base():
    from scripts.validation.scenarios import tx_journal_follows_saved_project as base
    return base


def _add_layer(gpkg: str):
    from qgis.core import QgsProject, QgsVectorLayer
    layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    return layer


def _journal_of(plugin):
    return plugin._journal.path if plugin._journal.is_open else None


def _reopen_and_count(plugin, project: str, marker: str) -> dict:
    """Close whatever is open, open *project*, read *marker* the product's way."""
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    QgsProject.instance().clear()
    pump(0.3)
    res = {"read": bool(QgsProject.instance().read(project))}
    pump(0.5)
    res["journal"] = _journal_of(plugin)
    res["count"] = _base()._counts_through_reader(plugin, (marker,)).get(marker)
    return res


def _new_blank(plugin, gpkg: str):
    """A blank project with the layer, its journal open (the session of S*)."""
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    QgsProject.instance().clear()
    pump(0.5)
    return _add_layer(gpkg)


def _s1_new(plugin, d: dict, gpkg: str, p1: str) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {}
    layer = _new_blank(plugin, gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 0, _S1)
    res["captured"] = b._wait_current(plugin, _S1)
    # The answer "Enregistrer" to "Nouveau projet": write, then clear, in
    # one call. No event-loop turn in between.
    res["write"] = bool(QgsProject.instance().write(p1))
    QgsProject.instance().clear()
    pump(1.0)
    res["journal_after_new"] = _journal_of(plugin)
    layer2 = _add_layer(gpkg)
    res["commit_blank"] = b._rename(layer2, 6, _S1_AFTER)
    res["captured_blank"] = b._wait_current(plugin, _S1_AFTER)
    res["reopen"] = _reopen_and_count(plugin, p1, _S1)
    res["blank_edit_in_p1"] = b._count(res["reopen"].get("journal"), _S1_AFTER)
    return res


def _s2_open_other(plugin, d: dict, gpkg: str, p2: str, q: str) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {}
    layer = _new_blank(plugin, gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 1, _S2)
    res["captured"] = b._wait_current(plugin, _S2)
    res["write"] = bool(QgsProject.instance().write(p2))
    res["read_q"] = bool(QgsProject.instance().read(q))
    pump(1.0)
    res["journal_on_q"] = _journal_of(plugin)
    res["reopen"] = _reopen_and_count(plugin, p2, _S2)
    return res


def _s3_quit(plugin, d: dict, gpkg: str, p3: str) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {}
    layer = _new_blank(plugin, gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 2, _S3)
    res["captured"] = b._wait_current(plugin, _S3)
    # The answer "Enregistrer" to "Quitter": write, then the plugins are
    # unloaded, in one call.
    res["write"] = bool(QgsProject.instance().write(p3))
    plugin.unload()
    pump(0.5)
    return res


def _s4_crash(base_dir: str, gpkg: str, p4: str) -> dict:
    child_py = os.path.join(base_dir, "crash_after_save_child.py")
    Path(child_py).write_text(_CHILD_CODE.format(
        root=str(_PLUGIN_ROOT), parent=str(_PLUGIN_ROOT.parent),
        package=_PLUGIN_ROOT.name, gpkg=gpkg, marker=_S4, project=p4),
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
    parts = res["captured"].split(" ", 3)
    res["child_session_journal"] = parts[3] if len(parts) == 4 else ""
    res["project_written"] = os.path.isfile(p4)
    return res


def _s5_locked_target(plugin, base_dir: str, gpkg: str, p5: str) -> dict:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    b = _base()
    res = {"holder_ready": "", "holder_closed": ""}
    layer = _new_blank(plugin, gpkg)
    res["session_journal"] = _journal_of(plugin)
    res["commit"] = b._rename(layer, 3, _S5[0])
    res["captured"] = b._wait_current(plugin, _S5[0])
    # The other instance has P5 open (a first version of it exists).
    Path(p5).write_text("<qgis/>", encoding="utf-8")
    holder_py = os.path.join(base_dir, "holder_child.py")
    Path(holder_py).write_text(_HOLDER_CODE.format(
        parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
        project=p5, profile=os.path.join(base_dir, "profil_autre")),
        encoding="utf-8")
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, holder_py], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        res["holder_ready"] = (proc.stdout.readline() or "").strip()
        bar_before = len(plugin.iface.bar.texts())
        res["write"] = bool(QgsProject.instance().write(p5))
        pump(1.0)
        res["journal_while_held"] = _journal_of(plugin)
        res["bar_while_held"] = plugin.iface.bar.texts()[bar_before:]
        res["commit_held"] = b._rename(b._project_layer(), 4, _S5[1])
        res["captured_held"] = b._wait_current(plugin, _S5[1])
        proc.stdin.write("\n")
        proc.stdin.flush()
        res["holder_closed"] = (proc.stdout.readline() or "").strip()
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    res["write_again"] = bool(QgsProject.instance().write())
    pump(1.0)
    res["journal_after_ctrl_s"] = _journal_of(plugin)
    QgsProject.instance().clear()
    pump(0.3)
    res["read"] = bool(QgsProject.instance().read(p5))
    pump(0.5)
    res["reopened_journal"] = _journal_of(plugin)
    res["counts"] = _base()._counts_through_reader(plugin, _S5)
    return res


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jstc_")
    ctx.data["tmp"] = tmp
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    _base()._make_gpkg(gpkg)
    gpkg4 = os.path.join(tmp, "data", "parcelles_s4.gpkg")
    _base()._make_gpkg(gpkg4)
    ctx.data["gpkg"] = gpkg
    ctx.data["gpkg4"] = gpkg4
    paths = {}
    for key, name in (("p1", "P1"), ("p2", "P2"), ("p3", "P3"), ("p4", "P4"),
                      ("p5", "P5"), ("q", "Q")):
        folder = os.path.join(tmp, f"dossier_{name}")
        os.makedirs(folder, exist_ok=True)
        paths[key] = os.path.join(folder, f"{name}.qgz")
    ctx.data["paths"] = paths
    # Q: an existing project the user opens after the save prompt.
    ctx.data["q_written"] = bool(QgsProject.instance().write(paths["q"]))
    QgsProject.instance().clear()
    flog(f"tx_journal_save_then_close setup: trace_id={ctx.trace_id} tmp={tmp}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    paths = d["paths"]
    first = RecoverPlugin(FakeIface())
    second = None
    first_alive = True
    try:
        first.initGui()
        pump(0.3)
        d["s1"] = _s1_new(first, d, d["gpkg"], paths["p1"])
        d["s2"] = _s2_open_other(first, d, d["gpkg"], paths["p2"], paths["q"])
        d["s3"] = _s3_quit(first, d, d["gpkg"], paths["p3"])
        first_alive = False
        # ---- the next QGIS start --------------------------------------
        QgsProject.instance().clear()
        second = RecoverPlugin(FakeIface())
        second.initGui()
        pump(0.3)
        d["s3"]["reopen"] = _reopen_and_count(second, paths["p3"], _S3)
        QgsProject.instance().clear()
        pump(0.3)
        d["s4"] = _s4_crash(d["tmp"], d["gpkg4"], paths["p4"])
        d["s4"]["reopen"] = _reopen_and_count(second, paths["p4"], _S4)
        d["s5"] = _s5_locked_target(second, d["tmp"], d["gpkg"], paths["p5"])
        d["bar"] = second.iface.bar.texts()[-6:]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        for plugin, alive in ((first, first_alive), (second, second is not None)):
            if not alive:
                continue
            try:
                plugin.unload()
            except Exception as exc:  # noqa: BLE001
                d.setdefault("unload_errors", []).append(repr(exc))
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_journal_save_then_close run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def _norm(path) -> str:
    """One spelling per file (the extended-length prefix aside)."""
    if not path:
        return ""
    p = str(path)
    if p.startswith("\\\\?\\"):
        p = p[4:]
    return os.path.normcase(os.path.abspath(p))


def _journal_next_to(project) -> str:
    if not project:
        return ""
    return os.path.join(os.path.dirname(project), ".recoverland",
                        "recoverland_audit.sqlite")


def _fixture(case: dict) -> bool:
    return (case.get("commit") is True and case.get("captured") == 1
            and case.get("write") is True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    crash = f"{d.get('crash', '')} {d.get('trace', '')[-600:]}".strip()
    out.append((
        "fixture_sans_plantage",
        not d.get("crash") and d.get("q_written") is True,
        f"crash={crash!r} projet Q ecrit={d.get('q_written')}",
    ))
    s1 = d.get("s1") or {}
    s2 = d.get("s2") or {}
    s3 = d.get("s3") or {}
    s4 = d.get("s4") or {}
    out.append((
        "fixture_capture_avant_chaque_enregistrement",
        _fixture(s1) and _fixture(s2) and _fixture(s3),
        f"S1 commit={s1.get('commit')} capture={s1.get('captured')} write="
        f"{s1.get('write')} ; S2 commit={s2.get('commit')} capture="
        f"{s2.get('captured')} write={s2.get('write')} ; S3 commit="
        f"{s3.get('commit')} capture={s3.get('captured')} write="
        f"{s3.get('write')} : sans capture initiale le cas ne prouve rien",
    ))
    r1 = s1.get("reopen") or {}
    out.append((
        "s1_nouveau_projet_apres_enregistrer_garde_l_historique",
        r1.get("read") is True and r1.get("count") == 1,
        f"'Nouveau projet' -> 'Enregistrer' P1 : modification lue dans le "
        f"journal de P1 rouvert ({r1.get('journal')}) : {r1.get('count')} "
        f"(attendu 1). Journal de session={s1.get('session_journal')}. Sinon "
        f"l'utilisateur rouvre P1 le lendemain et Restaurer/Rewind ne "
        f"montrent rien de ce qu'il a modifie avant d'enregistrer.",
    ))
    out.append((
        "s1_projet_vierge_ne_journalise_pas_dans_p1",
        s1.get("commit_blank") is True and s1.get("captured_blank") == 1
        and s1.get("blank_edit_in_p1") == 0,
        f"modification du NOUVEAU projet vierge : commit="
        f"{s1.get('commit_blank')} capturee={s1.get('captured_blank')} dans "
        f"{s1.get('journal_after_new')} ; presente dans le journal de P1="
        f"{s1.get('blank_edit_in_p1')} (attendu 0) : l'historique d'un autre "
        f"projet dans P1 serait propose a la restauration de P1",
    ))
    r2 = s2.get("reopen") or {}
    out.append((
        "s2_ouvrir_un_projet_apres_enregistrer_garde_l_historique",
        s2.get("read_q") is True and r2.get("read") is True
        and r2.get("count") == 1,
        f"'Ouvrir' Q -> 'Enregistrer' P2 : modification lue dans le journal "
        f"de P2 rouvert ({r2.get('journal')}) : {r2.get('count')} (attendu "
        f"1). Journal pendant Q={s2.get('journal_on_q')}.",
    ))
    r3 = s3.get("reopen") or {}
    out.append((
        "s3_quitter_apres_enregistrer_garde_l_historique",
        r3.get("read") is True and r3.get("count") == 1,
        f"'Quitter' -> 'Enregistrer' P3, puis demarrage suivant : "
        f"modification lue dans le journal de P3 ({r3.get('journal')}) : "
        f"{r3.get('count')} (attendu 1). Journal de session="
        f"{s3.get('session_journal')}.",
    ))
    out.append((
        "fixture_s4_processus_fils",
        (s4.get("captured") or "").startswith("CAPTURED True 1")
        and s4.get("wrote") == "WROTE True" and s4.get("project_written") is True,
        f"processus QGIS fils : {s4.get('captured')!r} {s4.get('wrote')!r} "
        f"code={s4.get('exit_code')} P4 ecrit={s4.get('project_written')} "
        f"erreur={s4.get('error')!r}",
    ))
    r4 = s4.get("reopen") or {}
    out.append((
        "s4_plantage_apres_enregistrer_garde_l_historique",
        r4.get("read") is True and r4.get("count") == 1,
        f"QGIS s'arrete brutalement juste apres avoir enregistre P4 : "
        f"modification lue a la reouverture de P4 ({r4.get('journal')}) : "
        f"{r4.get('count')} (attendu 1). Journal de session du processus "
        f"arrete={s4.get('child_session_journal')}. Rien de durable ne "
        f"disait que cet historique appartenait a P4.",
    ))
    s5 = d.get("s5") or {}
    out.append((
        "fixture_s5_autre_instance_tient_p5",
        (s5.get("holder_ready") or "").startswith("READY") and _fixture(s5)
        and s5.get("holder_closed") == "CLOSED",
        f"autre instance : {s5.get('holder_ready')!r} puis "
        f"{s5.get('holder_closed')!r} ; commit={s5.get('commit')} capture="
        f"{s5.get('captured')} write={s5.get('write')}",
    ))
    told = any("n'a pas pu recevoir" in t for t in (s5.get("bar_while_held") or []))
    out.append((
        "s5_journal_tenu_capture_continue_la_ou_est_l_historique",
        s5.get("journal_while_held") == s5.get("session_journal")
        and s5.get("commit_held") is True and s5.get("captured_held") == 1
        and told,
        f"journal de P5 tenu par une autre instance au moment de "
        f"l'enregistrement : journal courant={s5.get('journal_while_held')} "
        f"(session={s5.get('session_journal')}) ; modification suivante "
        f"capturee={s5.get('captured_held')} ; utilisateur prevenu={told} "
        f"({s5.get('bar_while_held')}) : l'historique ne doit etre ni coupe "
        f"en deux ni perdu en silence",
    ))
    counts5 = s5.get("counts") or {}
    out.append((
        "s5_historique_rattache_au_prochain_enregistrement",
        s5.get("write_again") is True and s5.get("read") is True
        and all(counts5.get(m) == 1 for m in _S5),
        f"apres fermeture de l'autre instance puis Ctrl+S : lu dans le journal "
        f"de P5 ({s5.get('reopened_journal')}) : {counts5} (attendu 1 "
        f"chacune). Journal juste apres Ctrl+S={s5.get('journal_after_ctrl_s')}.",
    ))
    # The counts above are read through whatever journal is open after
    # read(Pn): a fallback to the session journal at that point would pass
    # them while the history is still outside Pn's journal. The reopened
    # journal must BE the one next to Pn.
    paths = d.get("paths") or {}
    reopened = {
        "S1": (s1.get("reopen") or {}).get("journal"),
        "S2": (s2.get("reopen") or {}).get("journal"),
        "S3": (s3.get("reopen") or {}).get("journal"),
        "S4": (s4.get("reopen") or {}).get("journal"),
        "S5": s5.get("reopened_journal"),
    }
    wrong = {
        case: got for case, got in reopened.items()
        if _norm(got) != _norm(_journal_next_to(paths.get(f"p{case[1]}")))
    }
    out.append((
        "reouverture_lit_le_journal_du_projet",
        bool(paths) and not wrong,
        f"journal rouvert qui n'est pas celui du dossier du projet : {wrong} "
        f"(attendu <dossier_Pn>/.recoverland/recoverland_audit.sqlite pour S1 a "
        f"S5) : un comptage lu ailleurs prouverait un rattachement qui n'a pas "
        f"eu lieu",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_save_then_close.*trace_id={ctx.trace_id}",
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
