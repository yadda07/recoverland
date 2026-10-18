"""tx_journal_n3_lock_kept - RecoverLand validation runtime.

Invariant: I-JRN (un seul ecrivain par journal ; la trace de l'ecrivain en
place n'est jamais effacee par celui qui renonce a ecrire).

Le mecanisme
------------
Un journal sur un partage reseau, encore tenu en WAL par un poste qui
utilise une version plus ancienne du plugin (cas N3 de
tx_journal_network_legacy_wal). Son verrou `.rlwriter` n'est jamais
rafraichi : il passe pour perime et `_acquire_lock` le reprend en y ECRIVANT
le contenu de cette instance. L'ouverture est ensuite refusee (le journal ne
peut pas quitter WAL tant que l'autre poste l'a ouvert), et la liberation
de notre verrou SUPPRIMAIT le fichier, puisqu'il portait desormais notre
nom.

Le scenario d'echec concret
---------------------------
Juste apres avoir refuse d'ecrire PARCE QUE l'autre poste est actif, le
plugin efface la seule trace de cet autre poste : le diagnostic ("qui tient
ce journal ?") ne peut plus le nommer, et les autres clients anciens, qui ne
regardent que ce fichier, ne voient plus personne et ecrivent a leur tour.

Ce que le scenario execute reellement
-------------------------------------
Le vrai partage administratif local (`\\\\localhost\\C$\\...`), un journal en
WAL laisse par l'ancienne version, une connexion WAL ouverte a cote (l'autre
poste) et son verrou date d'une heure. `JournalManager.open_for_project`
doit refuser (JournalLockError) ET laisser le verrou de l'autre poste en
place, contenu et date d'origine.

Verdict attendu avant correctif : FAIL (le fichier de verrou a disparu).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_n3_lock_kept"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_OLD_HOST = "RL-POSTE-ANCIENNE-VERSION"


def _legacy():
    from scripts.validation.scenarios import tx_journal_network_legacy_wal as legacy
    return legacy


def setup(ctx):
    from recoverland.core.logger import flog

    base = tempfile.mkdtemp(prefix="rl_tx_jn3_")
    ctx.data["base"] = base
    local_base = os.path.join(base, "partage")
    os.makedirs(local_base, exist_ok=True)
    ctx.data["local_base"] = local_base
    unc_base = _legacy()._unc_mirror(local_base)
    ctx.data["unc_base"] = unc_base
    ctx.data["unc_reachable"] = bool(unc_base) and os.path.isdir(unc_base)
    ctx.data["profile"] = os.path.join(base, "profil")
    os.makedirs(ctx.data["profile"], exist_ok=True)
    flog(f"tx_journal_n3_lock_kept setup: trace_id={ctx.trace_id} "
         f"unc={unc_base!r} reachable={ctx.data['unc_reachable']}", "INFO")


def run(ctx):
    from recoverland.core.journal_manager import JournalManager, JournalLockError
    from recoverland.core.logger import flog

    d = ctx.data
    if d["unc_reachable"]:
        other = None
        try:
            case = _legacy()._legacy_project(d["local_base"], d["unc_base"], "n3")
            lock_path = case["journal"] + ".rlwriter"
            ts = int(time.time() - 3600)
            content = f"4747|{_OLD_HOST}|{ts}\n"
            with open(lock_path, "w", encoding="utf-8") as fh:
                fh.write(content)
            os.utime(lock_path, (ts, ts))
            d["lock_before"] = content
            other = sqlite3.connect(case["journal"], check_same_thread=False)
            other.execute("PRAGMA journal_mode=WAL")
            other.execute("SELECT COUNT(*) FROM audit_event").fetchone()
            jm = JournalManager()
            try:
                jm.open_for_project(case["project"], d["profile"])
                d["opened"] = jm.is_open
            except JournalLockError as exc:
                d["refused"] = str(exc)[:300]
                d["holder_host"] = exc.holder_host
            finally:
                jm.close()
            d["lock_exists_after"] = os.path.isfile(lock_path)
            if d["lock_exists_after"]:
                with open(lock_path, "r", encoding="utf-8") as fh:
                    d["lock_after"] = fh.read()
                d["lock_mtime_delta_s"] = round(os.path.getmtime(lock_path) - ts, 1)
        except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
            import traceback
            d["crash"] = f"{type(exc).__name__}: {exc}"
            d["trace"] = traceback.format_exc()[-1500:]
        finally:
            if other is not None:
                other.close()
    flog(f"tx_journal_n3_lock_kept run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)}",
         "INFO")
    shutil.rmtree(d["base"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = [(
        "fixture_partage_joignable_et_refus",
        d.get("unc_reachable") is True and bool(d.get("refused"))
        and d.get("holder_host") == _OLD_HOST,
        f"partage joignable={d.get('unc_reachable')} refus={d.get('refused')!r} "
        f"poste nomme={d.get('holder_host')!r} ouvert={d.get('opened')} crash="
        f"{d.get('crash')} {d.get('trace', '')[-400:]}",
    ), (
        "verrou_de_l_autre_poste_laisse_en_place",
        d.get("lock_exists_after") is True
        and d.get("lock_after") == d.get("lock_before")
        and d.get("lock_mtime_delta_s") is not None
        and abs(d["lock_mtime_delta_s"]) <= 2,
        f"verrou apres le refus : present={d.get('lock_exists_after')} contenu="
        f"{d.get('lock_after')!r} (attendu {d.get('lock_before')!r}) ecart de "
        f"date={d.get('lock_mtime_delta_s')} s : le refus effacait la seule "
        f"trace du poste qui ecrit ce journal",
    )]
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_n3_lock_kept.*trace_id={ctx.trace_id}",
        name="trace_id_propage", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
