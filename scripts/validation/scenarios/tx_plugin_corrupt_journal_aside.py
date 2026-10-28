# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_corrupt_journal_aside - RecoverLand validation runtime.

Invariant: I-JRN (un journal et ses fichiers compagnons forment un tout :
ce qui est conserve "pour analyse" l'est en entier) et I-OBS (ce que le
plugin fait du journal est dit a l'utilisateur, echec compris).

Le mecanisme
------------
A l'ouverture, un journal corrompu fait poser une question ("Voulez-vous le
renommer et creer un nouveau journal ? Le fichier corrompu sera conserve
pour analyse."). Sur Oui, `RecoverPlugin._handle_corrupt_journal`
(recover.py) renommait le journal en ``<journal>.corrupt_<horodatage>``
puis ses ``-wal`` / ``-shm`` avec des chemins SIMPLES. Le suffixe ajoute 24
caracteres : pour un journal de 232 a 250 caracteres (laisse en chemin
simple par core.long_paths, qui ne passe en forme etendue qu'au-dela de 250),
la cible depasse MAX_PATH (mesure : WinError 3).
  * 236 a 250 : le renommage du journal echoue ; une ligne de log, puis le
    plugin rouvre le MEME journal corrompu. L'utilisateur, qui a dit Oui,
    croit travailler sur un journal neuf.
  * 232 a 235 : le journal part, son ``-wal`` reste. Le journal neuf est
    cree vide a cote de l'ancien ``-wal`` ; SQLite supprime un ``-wal``
    pose a cote d'une base vide (il le tient pour perime). Les ecritures
    que ce ``-wal`` portait disparaissent de la copie "conservee pour
    analyse".
Et quel que soit le chemin, un renommage impossible (fichier tenu par un
autre programme) n'etait dit qu'au journal technique, et pouvait laisser
le journal d'un cote et son ``-wal`` de l'autre.

Le scenario d'echec concret
---------------------------
Un projet range dans un dossier profond a un journal corrompu. L'utilisateur
accepte de le mettre de cote. Soit rien n'est mis de cote et il le decouvre
au prochain incident, soit la copie remise au support n'a pas les
dernieres ecritures : l'analyse porte sur un etat qui n'est pas celui qui
a casse.

Ce que le scenario execute reellement
-------------------------------------
Le VRAI `RecoverPlugin._handle_corrupt_journal` (question patchee : Oui),
le vrai `JournalManager.open_for_project`, un projet enregistre (fichier
.qgs present) dont le journal est un trio SQLite reel : une ligne
'AVANT_CHECKPOINT' dans la base, une ligne 'DANS_LE_WAL' seulement dans le
``-wal`` (copie d'une base ouverte, comme apres un arret brutal).
  C1  journal de 234 caracteres : la copie mise de cote contient les DEUX
      lignes, le journal rouvert est neuf, l'utilisateur lit ou est la copie.
  C2  journal de 245 caracteres : meme attendu.
  C3  journal court dont le ``-wal`` est tenu ouvert par un autre programme :
      rien n'est separe (le journal reste entier a sa place, aucune copie
      partielle), et l'echec est dit (alerte critique persistante qui nomme
      le journal).

Verdict attendu avant correctif : FAIL (C1, C2, C3).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_plugin_corrupt_journal_aside"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PFX = "\\\\?\\"
_TAIL = os.sep + os.path.join(".recoverland", "recoverland_audit.sqlite")
_BOTH = ["AVANT_CHECKPOINT", "DANS_LE_WAL"]


def _ext(path: str) -> str:
    p = os.path.abspath(path)
    return p if p.startswith(_PFX) else _PFX + p


def _dir_of_len(base: str, length: int) -> str:
    """A folder under *base* whose plain path is exactly *length* long."""
    path = base
    need = length - len(path)
    while need > 0:
        # A component costs its name plus one separator; never leave a
        # single character of room (a separator with no name after it).
        take = min(need - 1, 120)
        if need - 1 - take == 1:
            take -= 1
        path = os.path.join(path, "p" * take)
        need = length - len(path)
    return path


def _markers(journal: str) -> list:
    """The marker rows a SQLite trio holds (its -wal applied), [] if none.

    Read from a copy: opening the trio itself checkpoints it at close.
    """
    if not os.path.isfile(_ext(journal)):
        return []
    work = tempfile.mkdtemp(prefix="rl_q_cjrd_")
    try:
        copy = os.path.join(work, "j.sqlite")
        for suffix in ("", "-wal", "-shm"):
            if os.path.isfile(_ext(journal + suffix)):
                shutil.copyfile(_ext(journal + suffix), copy + suffix)
        conn = sqlite3.connect(copy)
        try:
            return sorted(r[0] for r in conn.execute("SELECT v FROM marqueur"))
        except sqlite3.Error:
            return []
        finally:
            conn.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _make_wal_journal(journal: str) -> None:
    """A trio whose last write lives only in its -wal (a hard stop)."""
    work = tempfile.mkdtemp(prefix="rl_q_cjsrc_")
    src = os.path.join(work, "j.sqlite")
    conn = sqlite3.connect(src, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE marqueur (v TEXT)")
        conn.execute("INSERT INTO marqueur VALUES ('AVANT_CHECKPOINT')")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("INSERT INTO marqueur VALUES ('DANS_LE_WAL')")
        for suffix in ("", "-wal", "-shm"):
            shutil.copyfile(src + suffix, _ext(journal + suffix))
    finally:
        conn.close()
        shutil.rmtree(work, ignore_errors=True)


def _asides(journal: str) -> list:
    folder = os.path.dirname(journal)
    name = os.path.basename(journal) + ".corrupt_"
    try:
        return sorted(n for n in os.listdir(_ext(folder)) if n.startswith(name))
    except OSError:
        return []


def _case(ctx, key: str, journal_len: int, hold_wal: bool = False) -> dict:
    from qgis.core import Qgis
    from qgis.PyQt.QtWidgets import QMessageBox
    from recoverland.compat import QtCompat
    from recoverland.core import remove_project_log_handler
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface

    base = os.path.join(ctx.data["tmp"], key)
    project_dir = _dir_of_len(base, journal_len - len(_TAIL))
    os.makedirs(_ext(os.path.join(project_dir, ".recoverland")), exist_ok=True)
    project = os.path.join(project_dir, "P.qgs")
    with open(_ext(project), "w", encoding="utf-8") as fh:
        fh.write("<qgis/>")
    profile = os.path.join(ctx.data["tmp"], "profil")
    os.makedirs(profile, exist_ok=True)
    journal = project_dir + _TAIL
    _make_wal_journal(journal)
    res = {"journal_len": len(journal), "journal": journal,
           "fixture_markers": _markers(journal)}
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    saved_q = QMessageBox.__dict__.get("question")
    QMessageBox.question = staticmethod(lambda *a, **k: QtCompat.MSG_YES)
    holder = open(_ext(journal + "-wal"), "rb") if hold_wal else None
    try:
        new_path = plugin._handle_corrupt_journal(journal, profile, project)
        res["new_path"] = new_path
        res["reopened_here"] = bool(plugin._journal.is_open) and (
            os.path.normcase(os.path.abspath(new_path).replace(_PFX, ""))
            == os.path.normcase(os.path.abspath(journal)))
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        res["crash"] = f"{type(exc).__name__}: {exc}"
        res["trace"] = traceback.format_exc()[-1200:]
    finally:
        if saved_q is not None:
            QMessageBox.question = saved_q
        try:
            plugin._journal.close()
        except Exception as exc:  # noqa: BLE001
            res["close_error"] = repr(exc)
        remove_project_log_handler()
        if holder is not None:
            holder.close()
    res["bar"] = [{"text": text, "critical": level == Qgis.MessageLevel.Critical,
                   "duration": duration}
                  for _t, text, level, duration in iface.bar.messages]
    asides = _asides(journal)
    res["asides"] = asides
    main = [n for n in asides if not n.endswith(("-wal", "-shm", "-journal"))]
    res["aside_markers"] = (_markers(os.path.join(os.path.dirname(journal), main[0]))
                            if main else None)
    res["journal_markers"] = _markers(journal)
    return res


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_q_cj_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmp={ctx.data['tmp']}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, length, hold in (("c1", 234, False), ("c2", 245, False),
                              ("c3", 0, True)):
        if not length:
            length = len(ctx.data["tmp"]) + 60
        try:
            ctx.data[key] = _case(ctx, key, length, hold)
        except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-1200:]}
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps(ctx.data, default=str)[:6000]}", "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(_ext(tmp), ignore_errors=True)


def _told_where(res: dict) -> bool:
    main = [n for n in res.get("asides") or [] if "-" not in n.rsplit(".corrupt_", 1)[-1]]
    return bool(main) and any(main[0] in m["text"] for m in res.get("bar") or [])


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    for key, length in (("c1", 234), ("c2", 245)):
        res = ctx.data.get(key) or {}
        out.append((
            f"{key}_fixture_long_journal_with_wal_writes",
            res.get("journal_len") == length and res.get("fixture_markers") == _BOTH,
            f"longueur={res.get('journal_len')} (attendu {length}) "
            f"lignes du trio={res.get('fixture_markers')} crash={res.get('crash')}",
        ))
        out.append((
            f"{key}_copy_kept_for_analysis_is_whole",
            res.get("aside_markers") == _BOTH and not res.get("crash"),
            f"copies mises de cote={res.get('asides')} lignes qu'elles "
            f"contiennent={res.get('aside_markers')} (attendu {_BOTH}) "
            f"crash={res.get('crash')} {res.get('trace', '')}. La copie "
            f"'conservee pour analyse' doit porter aussi ce que son -wal "
            f"contenait, sinon l'analyse porte sur un autre etat",
        ))
        out.append((
            f"{key}_fresh_journal_opened",
            res.get("reopened_here") is True and res.get("journal_markers") == [],
            f"journal rouvert={res.get('new_path')} a la meme place="
            f"{res.get('reopened_here')} lignes qu'il contient="
            f"{res.get('journal_markers')}. L'utilisateur a dit Oui : il ne doit "
            f"pas continuer sur le journal corrompu",
        ))
        out.append((
            f"{key}_user_told_where_the_copy_is",
            _told_where(res),
            f"barre de messages={[m['text'][:200] for m in res.get('bar') or []]}",
        ))
    c3 = ctx.data.get("c3") or {}
    journal3 = c3.get("journal") or "?"
    out.append((
        "c3_fixture",
        c3.get("fixture_markers") == _BOTH,
        f"lignes du trio={c3.get('fixture_markers')} crash={c3.get('crash')}",
    ))
    out.append((
        "c3_failed_aside_is_told",
        any(m["critical"] and m["duration"] == 0 and journal3 in m["text"]
            for m in c3.get("bar") or []),
        f"barre de messages={[m['text'][:240] for m in c3.get('bar') or []]} "
        f"crash={c3.get('crash')}. Un Oui qui n'a rien pu faire doit etre dit, "
        f"pas seulement ecrit au journal technique",
    ))
    out.append((
        "c3_journal_never_split_from_its_wal",
        not c3.get("asides") and c3.get("journal_markers") == _BOTH,
        f"copies partielles={c3.get('asides')} lignes restees au journal="
        f"{c3.get('journal_markers')} (attendu {_BOTH}) crash={c3.get('crash')}. "
        f"Un journal parti sans son -wal perd les ecritures que ce -wal porte",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
