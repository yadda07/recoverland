"""obs_log_rollover_keeps_generations - RecoverLand validation runtime.

Invariant: I-LOG-ROTATION (une rotation du journal de debug ne fait
jamais disparaitre une generation encore dans la limite des copies
gardees, meme quand un autre programme lit une copie a ce moment-la).

Scenario ecrit par la zone "suite" (t) pour le proprietaire de
core/logger.py : il prouve un defaut du PRODUIT que la zone suite ne
peut pas corriger. Rouge tant que `doRollover` n'est pas corrige.

Le mecanisme
------------
`core.logger._SharedRotatingFileHandler.doRollover` (v7) deplace d'abord
le fichier courant en `.rolling`, puis fait glisser les copies (.4 -> .5,
..., .1 -> .2) et renomme `.rolling` en `.1`, le tout dans un
`try/except OSError: pass`. Sous Windows, un fichier ouvert par un autre
programme sans FILE_SHARE_DELETE (un antivirus, l'indexeur, un `tail`, un
autre Python, une autre instance QGIS qui lit ses copies) ne peut pas etre
renomme : le glissement echoue, en silence, et la generation qui vient de
tourner reste dans `.rolling`. La rotation suivante commence par
`os.remove(.rolling)` : cette generation est effacee, sans une ligne pour
le dire. Le checkout d'origine (rotation standard) n'a pas cette etape ;
c'est une regression v7.

Consequence concrete pour l'utilisateur : apres un incident, les 5 Mo de
journal forensique qui couvraient le moment de l'incident peuvent ne plus
exister, alors que le plugin promet de ne rien perdre en silence ; et
rien, nulle part, ne dit qu'ils ont ete effaces.

Ce que le scenario fait, avec le vrai gestionnaire du produit sur des
fichiers temporaires (chemin normal emit -> shouldRollover -> doRollover,
seuil abaisse le temps d'une ligne) :

    P1. un lecteur tient `.1` (open() ordinaire) pendant UNE rotation,
        le relache, puis le journal tourne encore une fois ;
    P2. un lecteur tient `.1` pendant DEUX rotations de suite, puis le
        relache, puis une rotation de plus ;
    P3. un lecteur tient le fichier COURANT pendant une rotation (chemin
        "rollover postponed" deja prevu par le produit, garde de
        non-regression).

Il verifie qu'apres chaque cas, chaque generation ecrite (moins de
generations que de copies gardees : aucune n'a le droit de vieillir hors
de la chaine) est encore sur le disque, et que les fichiers de la chaine,
lus du plus recent au plus ancien (courant, `.rolling`, `.1`, `.2`, ...),
donnent les generations dans l'ordre : c'est l'ordre que suit
`scripts/validation/parse_log.read_window`.
"""
from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "obs_log_rollover_keeps_generations"
INVARIANT = "I-LOG-ROTATION"

_BACKUPS = 5
_FMT = ("%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] "
        "%(message)s")
_GEN_RE = re.compile(r"\] (G\d+)_")
_ORDER = [""] + [".rolling"] + [f".{i}" for i in range(1, _BACKUPS + 2)]


def _handler(path: str):
    import logging

    from recoverland.core.logger import _SharedRotatingFileHandler

    handler = _SharedRotatingFileHandler(
        path, mode="a", maxBytes=10 ** 9, backupCount=_BACKUPS,
        encoding="utf-8")
    handler.setFormatter(logging.Formatter(_FMT, datefmt="%Y-%m-%dT%H:%M:%S"))
    return handler


def _emit(handler, text: str) -> None:
    import logging

    handler.handle(logging.LogRecord(
        "zz.logrollover", logging.INFO, __file__, 0, text, None, None))


def _rotate(handler, text: str) -> None:
    """Fait tourner par le chemin normal du gestionnaire.

    Le seuil est abaisse a 1 octet le temps d'UNE ligne : emit() voit le
    fichier plein, appelle doRollover() puis ecrit la ligne. Le delai de
    nouvel essai d'une rotation reportee est remis a zero avant : le
    scenario ne dure pas 60 s, et une rotation reportee doit pouvoir etre
    retentee comme elle le serait plus tard.
    """
    handler.acquire()
    try:
        if hasattr(handler, "_rollover_retry_at"):
            handler._rollover_retry_at = 0.0
        saved = handler.maxBytes
        handler.maxBytes = 1
        try:
            _emit(handler, text)
        finally:
            handler.maxBytes = saved
    finally:
        handler.release()


def _chain(path: str) -> list:
    """[(suffixe, texte)] des fichiers presents, du plus recent au plus ancien."""
    out = []
    for suffix in _ORDER:
        p = path + suffix
        if os.path.isfile(p):
            with open(p, encoding="utf-8", errors="replace") as fh:
                out.append((suffix or "live", fh.read()))
    return out


def _state(path: str) -> dict:
    chain = _chain(path)
    gens = set()
    order = []
    for _suffix, text in chain:
        found = _GEN_RE.findall(text)
        gens.update(found)
        order.extend(int(g[1:]) for g in dict.fromkeys(reversed(found)))
    return {
        "gens": sorted(gens, key=lambda g: int(g[1:])),
        "layout": {s: sorted(set(_GEN_RE.findall(t)),
                             key=lambda g: int(g[1:])) for s, t in chain},
        # Du plus recent au plus ancien : strictement decroissant.
        "ordered": all(a > b for a, b in zip(order, order[1:])),
    }


def _write_gens(handler, first: int, last: int) -> None:
    for g in range(first, last + 1):
        _rotate(handler, f"G{g}_FIRST")
        _emit(handler, f"G{g}_MORE")


def _case(base: str, name: str, hold_suffix: str, held_rotations: int) -> dict:
    """G0, G1, G2 ecrites ; un lecteur tient ``hold_suffix`` pendant
    ``held_rotations`` rotations ; puis une rotation de plus, lecteur
    relache."""
    path = os.path.join(base, f"{name}.log")
    handler = _handler(path)
    holder = None
    try:
        _emit(handler, "G0_FIRST")
        _emit(handler, "G0_MORE")
        _write_gens(handler, 1, 2)
        holder = open(path + hold_suffix, "rb")
        _write_gens(handler, 3, 2 + held_rotations)
        holder.close()
        holder = None
        last = 3 + held_rotations
        _write_gens(handler, last, last)
    finally:
        if holder is not None:
            holder.close()
        handler.close()
    res = _state(path)
    res["want"] = [f"G{g}" for g in range(0, last + 1)]
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["dir"] = tempfile.mkdtemp(prefix="rl_logrot_")
    flog(f"obs_log_rollover_keeps_generations setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    base = ctx.data["dir"]
    for key, hold, rotations in (("p1", ".1", 1), ("p2", ".1", 2),
                                 ("p3", "", 1)):
        try:
            ctx.data[key] = _case(base, key, hold, rotations)
        except Exception as exc:  # noqa: BLE001 - reported by the assertions
            ctx.data[key] = {"error": f"{type(exc).__name__}: {exc}"}
    flog(f"obs_log_rollover_keeps_generations run end trace_id={ctx.trace_id}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("dir") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    for key, label in (
            ("p1", "un lecteur tient .1 pendant une rotation"),
            ("p2", "un lecteur tient .1 pendant deux rotations"),
            ("p3", "un lecteur tient le fichier courant pendant une "
                   "rotation")):
        res = ctx.data.get(key) or {}
        err = res.get("error")
        missing = sorted(set(res.get("want") or []) - set(res.get("gens") or []),
                         key=lambda g: int(g[1:]))
        out.append((
            f"{key}_every_generation_still_on_disk",
            err is None and bool(res.get("want")) and not missing,
            f"{label}, puis une rotation de plus : erreur={err!r} "
            f"generations manquantes={missing} chaine={res.get('layout')}. "
            f"Chaque generation manquante, ce sont jusqu'a 5 Mo de journal "
            f"forensique effaces en silence (defaut de core/logger.py "
            f"doRollover : os.remove(.rolling) au debut de la rotation qui "
            f"suit un glissement echoue)",
        ))
        out.append((
            f"{key}_chain_newest_to_oldest_is_in_generation_order",
            err is None and res.get("ordered") is True,
            f"{label} : chaine={res.get('layout')} lue courant, .rolling, "
            f".1, .2... : ordre des generations respecte="
            f"{res.get('ordered')}. Hors d'ordre, un lecteur du journal "
            f"(parse_log.read_window, un humain apres incident) remet les "
            f"evenements dans le mauvais ordre",
        ))
    out.append(assert_log_contains(
        ctx.records,
        rf"obs_log_rollover_keeps_generations.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
