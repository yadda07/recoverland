# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_journal_owed_concurrent_writers - RecoverLand validation runtime.

Invariant: I-JRN (une obligation de rattachement d'historique n'est jamais
perdue en silence).

Le mecanisme
------------
Un enregistrement de projet note durablement, dans le profil, que
l'historique de la session reste a rattacher au journal du projet
(`carries_owed.json`, `_remember_owed`) : c'est ce qui survit a un
plantage entre la sauvegarde et la fermeture du journal de session. Le
fichier est lu, modifie puis reecrit en entier (remplacement atomique).
Le verrou qui protegeait cette lecture-ecriture etait un verrou de
THREAD : deux QGIS du meme profil (deux fenetres ouvertes par
l'utilisateur, ou un QGIS lance par un script) lisent la meme version,
chacun ajoute sa ligne, le second ecrase celle du premier. Sous Windows,
le remplacement echoue aussi tant qu'un autre processus tient le fichier
ouvert (l'autre QGIS qui le lit, un antivirus ou l'indexeur qui ouvrent un
fichier qui vient d'etre ecrit) : l'obligation est refusee, et le fichier
temporaire reste a cote. Enfin un registre illisible etait lu comme vide,
et le premier enregistrement suivant l'ecrasait avec sa seule ligne.

Le scenario d'echec concret
---------------------------
L'utilisateur enregistre deux projets en meme temps dans deux QGIS du meme
profil, puis l'un des deux plante avant d'avoir ferme son journal de
session. A la reouverture de son projet, rien n'est rattache : son
obligation a ete ecrasee par celle de l'autre QGIS, sans un mot ; les
modifications de la session restent dans un journal de profil qu'aucun
projet n'ouvre, supprime trente jours plus tard.

Ce que le scenario execute reellement
-------------------------------------
Quatre VRAIS processus Python (QGIS initialise, vrai module
`journal_manager`) sur le meme profil, lances ensemble : chacun enregistre
40 obligations distinctes (`_remember_owed`), oublie a mi-parcours
l'obligation qui lui avait ete preparee (`_forget_owed`) et en met une a
jour (`_update_owed`). Puis lecture du fichier final. Dans ce processus,
sur d'autres profils :
  H le registre tenu ouvert 0,3 s par un autre lecteur (comme un
    antivirus) pendant un enregistrement : l'obligation est enregistree ;
  L tenu 2,5 s : l'enregistrement est refuse (et le dit), le registre est
    intact, aucun fichier temporaire ne reste ;
  C registre illisible (JSON tronque) : il est conserve sous un autre nom,
    l'utilisateur en est informe, la nouvelle obligation est enregistree.

Verdict attendu avant correctif : FAIL (obligations annoncees enregistrees
et absentes du fichier, ou refusees par le remplacement concurrent ; H
refusee ; L laisse son fichier temporaire ; C ecrase le registre sans le
dire).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess  # nosec B404: processus fils locaux lances par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_owed_concurrent_writers"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_CHILDREN = 4
_PER_CHILD = 40

_CHILD_CODE = r'''
import json, os, sys, time
sys.path.insert(0, {parent!r})
from qgis.core import QgsApplication
app = QgsApplication([], False)
app.initQgis()
jm = __import__({package!r} + ".core.journal_manager", fromlist=["x"])
profile, tag, count, go = {profile!r}, {tag!r}, {count!r}, {go!r}

def carry(name):
    return jm.HistoryCarry(
        source=os.path.join(profile, "src_" + name + ".sqlite"),
        source_kind="session", source_session_key="",
        target=os.path.join(profile, "dst_" + name + ".sqlite"),
        target_kind="saved", since_event_id=0, opened_at="",
        mark_source=True, project_file="", profile_path=profile)

print("READY", flush=True)
while not os.path.exists(go):
    time.sleep(0.001)
result = {{"tag": tag, "remembered": [], "refused": [], "errors": []}}
for i in range(count):
    name = tag + "_" + str(i)
    try:
        ok = jm._remember_owed(carry(name))
    except Exception as exc:
        ok = False
        result["errors"].append(type(exc).__name__ + ": " + str(exc))
    (result["remembered"] if ok else result["refused"]).append(name)
    if i == count // 2:
        seed = carry("seed_" + tag)
        try:
            jm._forget_owed(profile, seed.source, seed.target)
            upd = carry("upd_" + tag)
            result["updated"] = jm._update_owed(
                profile, upd.source, upd.target, until_event_id=4242)
        except Exception as exc:
            result["errors"].append(type(exc).__name__ + ": " + str(exc))
print("RESULT " + json.dumps(result), flush=True)
'''


def _key(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def _carry(jm, profile: str, name: str):
    return jm.HistoryCarry(
        source=os.path.join(profile, f"src_{name}.sqlite"),
        source_kind="session", source_session_key="",
        target=os.path.join(profile, f"dst_{name}.sqlite"),
        target_kind="saved", since_event_id=0, opened_at="",
        mark_source=True, project_file="", profile_path=profile)


def _held_while(path: str, seconds: float, action):
    """Run *action* while another reader keeps *path* open *seconds*."""
    import threading
    fh = open(path, "r", encoding="utf-8")  # noqa: SIM115 - closed by the timer
    timer = threading.Timer(seconds, fh.close)
    timer.start()
    try:
        return action()
    finally:
        timer.join()
        fh.close()


def single_process_cases(tmp: str) -> dict:
    """H, L and C: one process, the registry of other profiles."""
    from recoverland.core import journal_manager as jm
    out = {}
    told = []
    saved_qlog = jm.qlog
    jm.qlog = lambda msg, level="INFO": told.append(str(msg))
    try:
        # H: a transient hold (antivirus, indexer, another reader).
        prof = os.path.join(tmp, "profil_h")
        audit = os.path.join(prof, "recoverland", "audit")
        first = jm._remember_owed(_carry(jm, prof, "h0"))
        owed = os.path.join(audit, "carries_owed.json")
        t0 = time.monotonic()
        out["h"] = {"first": first, "ok": _held_while(
            owed, 0.3, lambda: jm._remember_owed(_carry(jm, prof, "h1")))}
        out["h"]["elapsed"] = round(time.monotonic() - t0, 2)
        with open(owed, "r", encoding="utf-8") as fh:
            out["h"]["entries"] = len(json.load(fh))
        # L: a hold longer than any retry.
        prof = os.path.join(tmp, "profil_l")
        audit = os.path.join(prof, "recoverland", "audit")
        jm._remember_owed(_carry(jm, prof, "l0"))
        owed = os.path.join(audit, "carries_owed.json")
        with open(owed, "r", encoding="utf-8") as fh:
            before = fh.read()
        out["l"] = {"ok": _held_while(
            owed, 2.5, lambda: jm._remember_owed(_carry(jm, prof, "l1")))}
        with open(owed, "r", encoding="utf-8") as fh:
            out["l"]["unchanged"] = fh.read() == before
        out["l"]["leftovers"] = sorted(f for f in os.listdir(audit)
                                       if f.endswith(".tmp"))
        # C: an unreadable registry.
        prof = os.path.join(tmp, "profil_c")
        audit = os.path.join(prof, "recoverland", "audit")
        os.makedirs(audit, exist_ok=True)
        owed = os.path.join(audit, "carries_owed.json")
        broken = '[{"source": "C:/autre/src.sqlite", "target": "C:/autre/dst.sq'
        with open(owed, "w", encoding="utf-8") as fh:
            fh.write(broken)
        n = len(told)
        out["c"] = {"ok": jm._remember_owed(_carry(jm, prof, "c1"))}
        kept = [f for f in os.listdir(audit) if f.startswith("carries_owed.json.unreadable")]
        out["c"]["kept"] = kept
        out["c"]["kept_intact"] = bool(kept) and Path(
            os.path.join(audit, kept[0])).read_text(encoding="utf-8") == broken
        try:
            with open(owed, "r", encoding="utf-8") as fh:
                out["c"]["entries"] = [e.get("source") for e in json.load(fh)]
        except (OSError, ValueError) as exc:
            out["c"]["entries"] = f"{type(exc).__name__}"
        out["c"]["told"] = told[n:]
    finally:
        jm.qlog = saved_qlog
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.mkdtemp(prefix="rl_tx_jocw_")
    profile = os.path.join(tmp, "profil")
    os.makedirs(profile, exist_ok=True)
    ctx.data["tmp"] = tmp
    ctx.data["profile"] = profile
    flog(f"tx_journal_owed_concurrent_writers setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core import journal_manager as jm

    d = ctx.data
    tmp, profile = d["tmp"], d["profile"]
    tags = [f"p{i}" for i in range(_CHILDREN)]
    procs = []
    try:
        # What each child forgets and updates midway, recorded beforehand.
        def carry(name):
            return jm.HistoryCarry(
                source=os.path.join(profile, f"src_{name}.sqlite"),
                source_kind="session", source_session_key="",
                target=os.path.join(profile, f"dst_{name}.sqlite"),
                target_kind="saved", since_event_id=0, opened_at="",
                mark_source=True, project_file="", profile_path=profile)
        d["seeded"] = all(jm._remember_owed(carry(f"{kind}_{t}"))
                          for t in tags for kind in ("seed", "upd"))
        go = os.path.join(tmp, "go")
        for tag in tags:
            child_py = os.path.join(tmp, f"child_{tag}.py")
            Path(child_py).write_text(_CHILD_CODE.format(
                parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
                profile=profile, tag=tag, count=_PER_CHILD, go=go),
                encoding="utf-8")
            procs.append(subprocess.Popen(  # nosec B603: sys.executable, script du test
                [sys.executable, child_py], stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True))
        d["ready"] = [(p.stdout.readline() or "").strip() for p in procs]
        Path(go).write_text("go", encoding="utf-8")
        results, lines = [], []
        deadline = time.monotonic() + 120
        for p in procs:
            line = ""
            while time.monotonic() < deadline:
                line = p.stdout.readline()
                if not line or line.startswith("RESULT "):
                    break
            lines.append(line.strip()[:200])
            if line.startswith("RESULT "):
                results.append(json.loads(line[len("RESULT "):]))
            p.wait(timeout=max(1, deadline - time.monotonic()))
        d["results"] = results
        d["missing_results"] = len(procs) - len(results)
        owed_path = os.path.join(profile, "recoverland", "audit", "carries_owed.json")
        try:
            with open(owed_path, "r", encoding="utf-8") as fh:
                entries = json.load(fh)
            d["file_ok"] = isinstance(entries, list)
        except (OSError, ValueError) as exc:
            entries = []
            d["file_ok"] = False
            d["file_error"] = str(exc)
        present = {_key(e.get("source", "")): e for e in entries if isinstance(e, dict)}
        remembered = [n for r in results for n in r.get("remembered", [])]
        refused = [n for r in results for n in r.get("refused", [])]
        d["remembered"] = len(remembered)
        d["refused"] = refused[:20]
        d["n_refused"] = len(refused)
        d["errors"] = [e for r in results for e in r.get("errors", [])][:10]
        d["lost"] = [n for n in remembered
                     if _key(os.path.join(profile, f"src_{n}.sqlite")) not in present][:40]
        d["n_lost"] = sum(1 for n in remembered
                          if _key(os.path.join(profile, f"src_{n}.sqlite")) not in present)
        d["seeds_back"] = [t for t in tags
                           if _key(os.path.join(profile, f"src_seed_{t}.sqlite")) in present]
        d["updates_lost"] = [
            t for t in tags
            if (present.get(_key(os.path.join(profile, f"src_upd_{t}.sqlite"))) or {})
            .get("until_event_id") != 4242]
        d["updated_flags"] = [r.get("updated") for r in results]
        d["entries"] = len(entries)
        d["single"] = single_process_cases(tmp)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        for p in procs:
            try:
                p.kill()
                p.wait(timeout=10)
            except Exception:  # noqa: BLE001
                pass
            try:
                p.stdout.close()
            except Exception:  # noqa: BLE001
                pass
    flog(f"tx_journal_owed_concurrent_writers run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    expected = _CHILDREN * _PER_CHILD
    single = d.get("single") or {}
    h, lo, c = single.get("h") or {}, single.get("l") or {}, single.get("c") or {}
    return [
        ("h_transient_hold_does_not_refuse_the_obligation",
         h.get("first") is True and h.get("ok") is True and h.get("entries") == 2,
         f"registre tenu 0,3 s par un autre lecteur : enregistre={h.get('ok')} "
         f"en {h.get('elapsed')} s, entrees={h.get('entries')} (attendu 2). "
         f"Un antivirus qui ouvre le fichier juste ecrit ne doit pas couter "
         f"une obligation{crash}"),
        ("l_long_hold_refused_loudly_and_leaves_nothing",
         lo.get("ok") is False and lo.get("unchanged") is True
         and lo.get("leftovers") == [],
         f"registre tenu 2,5 s : enregistre={lo.get('ok')} (attendu False, "
         f"dit dans le log) registre intact={lo.get('unchanged')} fichiers "
         f"temporaires restes={lo.get('leftovers')}"),
        ("c_unreadable_registry_kept_and_told",
         c.get("ok") is True and c.get("kept_intact") is True
         and isinstance(c.get("entries"), list) and len(c["entries"]) == 1
         and any("illisible" in m for m in c.get("told") or []),
         f"registre illisible : nouvelle obligation={c.get('ok')} registre "
         f"conserve={c.get('kept')} intact={c.get('kept_intact')} entrees="
         f"{c.get('entries')} message={c.get('told')}. Lu comme vide, il "
         f"etait ecrase sans que personne le sache"),
        ("setup_children_ran_together",
         d.get("seeded") is True and d.get("ready") == ["READY"] * _CHILDREN
         and d.get("missing_results") == 0 and not d.get("crash"),
         f"obligations preparees={d.get('seeded')} processus prets={d.get('ready')} "
         f"resultats manquants={d.get('missing_results')}{crash}"),
        ("no_recorded_obligation_lost",
         d.get("missing_results") == 0 and d.get("n_lost") == 0
         and d.get("remembered", 0) > 0,
         f"{d.get('n_lost')} obligation(s) annoncee(s) enregistree(s) par "
         f"_remember_owed et absente(s) du fichier (ex. {d.get('lost', [])[:6]}) "
         f"sur {d.get('remembered')} : apres un plantage, ces projets "
         f"n'auraient jamais leur historique, sans que personne le sache"),
        ("every_obligation_recorded",
         d.get("n_refused") == 0 and d.get("remembered") == expected
         and not d.get("errors"),
         f"{d.get('n_refused')} obligation(s) refusee(s) (ex. {d.get('refused', [])[:6]}) "
         f"et erreurs {d.get('errors')} sur {expected} : un autre QGIS du meme "
         f"profil qui ecrit au meme moment ne doit pas faire echouer "
         f"l'enregistrement"),
        ("forgotten_and_updated_entries_hold",
         d.get("seeds_back") == [] and d.get("updates_lost") == []
         and d.get("updated_flags") == [True] * _CHILDREN,
         f"obligations oubliees revenues={d.get('seeds_back')} ; mises a jour "
         f"perdues={d.get('updates_lost')} (retours {d.get('updated_flags')}) : "
         f"une ecriture concurrente ne doit defaire ni un oubli ni une borne"),
        ("owed_file_is_a_valid_list",
         d.get("file_ok") is True
         and d.get("entries") == expected + _CHILDREN,
         f"fichier lisible={d.get('file_ok')} {d.get('file_error', '')} ; "
         f"entrees={d.get('entries')} (attendu {expected + _CHILDREN} : "
         f"{expected} nouvelles + {_CHILDREN} mises a jour, les "
         f"{_CHILDREN} oubliees en moins)"),
        assert_log_contains(
            ctx.records, rf"tx_journal_owed_concurrent_writers.*trace_id={ctx.trace_id}",
            name="trace_id_propagated", min_count=2),
    ]


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
