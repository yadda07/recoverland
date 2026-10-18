"""obs_log_rollover_refusal_named - RecoverLand validation runtime.

Invariant: I-LOG-ROTATION (une rotation du journal de debug ne fait
jamais disparaitre une generation que la retention garde, et un
glissement refuse est dit dans le journal, jamais avale).

Complement de obs_log_rollover_keeps_generations (zone suite), qui
couvre la chaine incomplete. Ici, la chaine est PLEINE (5 copies) et la
rotation qui suit un glissement refuse doit encore garder les 6
generations les plus recentes.

Le mecanisme
------------
`core.logger._SharedRotatingFileHandler.doRollover` deplace le fichier
courant en `.rolling`, puis fait glisser les copies et renomme `.rolling`
en `.1`. Sous Windows, un lecteur qui ne partage pas la suppression
(antivirus, indexeur, `tail`, une autre instance QGIS qui lit ses copies)
empeche le renommage du fichier qu'il tient : le glissement s'arrete a
mi-chemin et laisse un trou dans la chaine, la generation qui vient de
tourner reste dans `.rolling`.
  - v7 : le glissement etait dans `try/except OSError: pass` (aucune
    trace) et la rotation suivante commencait par `os.remove(.rolling)` :
    5 Mo de journal forensique effaces, sans une ligne pour le dire ;
  - finir le glissement en attente en repartant du haut de la chaine
    (.4 -> .5 en effacant .5, ...) ignore le trou : la chaine pleine perd
    une generation de plus que la retention, celle qui aurait tenu dans
    le trou.

Consequence concrete pour l'utilisateur : apres un incident, le journal
qui couvrait le moment de l'incident a disparu, et rien ne dit qu'il a
existe ni pourquoi il manque.

Ce que le scenario fait, avec le vrai gestionnaire du produit sur des
fichiers temporaires (chemin normal emit -> shouldRollover -> doRollover,
seuil abaisse le temps d'une ligne, delai de nouvel essai remis a zero) :

    Q1. G0 a G6 ecrites (chaine pleine : G6 courante, G5..G1 en copies) ;
        un lecteur tient `.1` pendant la rotation vers G7, le relache,
        puis la rotation vers G8. Les 6 generations les plus recentes
        (G3 a G8) doivent etre sur le disque, dans l'ordre de la chaine.
    Q2. Le refus du glissement de Q1 est dit : une ligne
        "# backup shift postponed (...)" nommant l'erreur est dans le
        journal.
    Q3. Un lecteur tient `.1` pendant DEUX rotations (chaine incomplete) :
        la seconde, qui ne peut pas finir le glissement en attente, est
        reportee avec une ligne "# rollover postponed (backup shift: ...)"
        et `.rolling` n'est pas efface ; tout revient en ordre une fois le
        lecteur parti.
    Q4. Les lignes "#" n'abiment pas le format : chaque autre ligne de la
        chaine se lit avec parse_log.parse_line.
"""
from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "obs_log_rollover_refusal_named"
INVARIANT = "I-LOG-ROTATION"

_BACKUPS = 5
_FMT = ("%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] "
        "%(message)s")
_GEN_RE = re.compile(r"\] (G\d+)_")
_ORDER = [""] + [".rolling"] + [f".{i}" for i in range(1, _BACKUPS + 2)]
_SHIFT_REFUSED = "# backup shift postponed ("
_ROLLOVER_REFUSED = "# rollover postponed (backup shift: "


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
        "zz.logrefusal", logging.INFO, __file__, 0, text, None, None))


def _rotate(handler, text: str) -> None:
    """Une rotation par le chemin normal (seuil a 1 octet le temps d'une
    ligne), delai de nouvel essai remis a zero : une rotation reportee est
    retentee comme elle le serait 60 s plus tard."""
    handler.acquire()
    try:
        handler._rollover_retry_at = 0.0
        saved = handler.maxBytes
        handler.maxBytes = 1
        try:
            _emit(handler, text)
        finally:
            handler.maxBytes = saved
    finally:
        handler.release()


def _write_gens(handler, first: int, last: int) -> None:
    for g in range(first, last + 1):
        _rotate(handler, f"G{g}_FIRST")
        _emit(handler, f"G{g}_MORE")


def _chain(path: str) -> list:
    out = []
    for suffix in _ORDER:
        p = path + suffix
        if os.path.isfile(p):
            with open(p, encoding="utf-8", errors="replace") as fh:
                out.append((suffix or "live", fh.read()))
    return out


def _blocks_rename(path: str) -> bool:
    """Le fichier tenu refuse-t-il vraiment d'etre renomme (Windows) ?"""
    probe = path + ".probe"
    try:
        os.rename(path, probe)
    except OSError:
        return True
    os.rename(probe, path)
    return False


def _state(path: str) -> dict:
    from scripts.validation.parse_log import parse_line

    chain = _chain(path)
    gens, order, bad, notes = set(), [], [], []
    for suffix, text in chain:
        found = _GEN_RE.findall(text)
        gens.update(found)
        order.extend(int(g[1:]) for g in dict.fromkeys(reversed(found)))
        for line in text.splitlines():
            if line.startswith("#"):
                notes.append(f"{suffix}: {line[:300]}")
            elif line.strip() and parse_line(line) is None:
                bad.append(f"{suffix}: {line[:200]}")
    return {
        "gens": sorted(gens, key=lambda g: int(g[1:])),
        "layout": {s: sorted(set(_GEN_RE.findall(t)), key=lambda g: int(g[1:]))
                   for s, t in chain},
        "ordered": all(a > b for a, b in zip(order, order[1:])),
        "notes": notes,
        "unparsed": bad,
    }


def _case_full_chain(base: str) -> dict:
    path = os.path.join(base, "q1.log")
    handler = _handler(path)
    holder = None
    res = {}
    try:
        _emit(handler, "G0_FIRST")
        _emit(handler, "G0_MORE")
        _write_gens(handler, 1, 6)
        holder = open(path + ".1", "rb")
        res["blocked"] = _blocks_rename(path + ".1")
        _write_gens(handler, 7, 7)
        res["after_refusal"] = _state(path)
        holder.close()
        holder = None
        _write_gens(handler, 8, 8)
    finally:
        if holder is not None:
            holder.close()
        handler.close()
    res.update(_state(path))
    res["want"] = [f"G{g}" for g in range(8 - _BACKUPS, 9)]
    return res


def _case_two_refusals(base: str) -> dict:
    path = os.path.join(base, "q3.log")
    handler = _handler(path)
    holder = None
    res = {}
    try:
        _emit(handler, "G0_FIRST")
        _emit(handler, "G0_MORE")
        _write_gens(handler, 1, 2)
        holder = open(path + ".1", "rb")
        res["blocked"] = _blocks_rename(path + ".1")
        _write_gens(handler, 3, 4)
        res["rolling_kept"] = os.path.isfile(path + ".rolling")
        res["during"] = _state(path)
        holder.close()
        holder = None
        _write_gens(handler, 5, 5)
    finally:
        if holder is not None:
            holder.close()
        handler.close()
    res.update(_state(path))
    res["rolling_left"] = os.path.isfile(path + ".rolling")
    res["want"] = [f"G{g}" for g in range(0, 6)]
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["dir"] = tempfile.mkdtemp(prefix="rl_logref_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    base = ctx.data["dir"]
    for key, case in (("q1", _case_full_chain), ("q3", _case_two_refusals)):
        try:
            ctx.data[key] = case(base)
        except Exception as exc:  # noqa: BLE001 - reported by the assertions
            ctx.data[key] = {"error": f"{type(exc).__name__}: {exc}"}
    flog(f"{SCENARIO_ID} run end trace_id={ctx.trace_id} "
         f"q1={ctx.data['q1'].get('layout')} q3={ctx.data['q3'].get('layout')}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("dir") or "", ignore_errors=True)


def _missing(res: dict) -> list:
    return sorted(set(res.get("want") or []) - set(res.get("gens") or []),
                  key=lambda g: int(g[1:]))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    q1 = ctx.data.get("q1") or {}
    q3 = ctx.data.get("q3") or {}
    out = []
    on_windows = os.name == "nt"
    out.append((
        "fixture_reader_blocks_the_rename",
        not on_windows or (q1.get("blocked") is True and q3.get("blocked") is True),
        f"renommage refuse sous le lecteur : Q1={q1.get('blocked')} "
        f"Q3={q3.get('blocked')} (Windows={on_windows}) : sans refus, ce "
        f"scenario ne prouve rien",
    ))
    miss = _missing(q1)
    out.append((
        "q1_full_chain_keeps_the_retained_generations",
        q1.get("error") is None and bool(q1.get("want")) and not miss
        and q1.get("ordered") is True,
        f"chaine pleine, un lecteur tient .1 pendant une rotation puis une "
        f"rotation de plus : erreur={q1.get('error')!r} generations "
        f"retenues manquantes={miss} (attendu {q1.get('want')}) chaine="
        f"{q1.get('layout')} ordre={q1.get('ordered')}. Une generation "
        f"manquante, ce sont 5 Mo de journal forensique effaces alors que "
        f"la retention les gardait",
    ))
    notes = (q1.get("after_refusal") or {}).get("notes") or []
    named = [n for n in notes if _SHIFT_REFUSED in n]
    out.append((
        "q2_refused_shift_is_named_in_the_log",
        q1.get("error") is None and (not q1.get("blocked") or bool(named)),
        f"lignes '#' apres le glissement refuse={notes} : attendu une ligne "
        f"'{_SHIFT_REFUSED}...)' qui nomme l'erreur. Avalee, la rotation "
        f"ratee ne laisse aucune trace, et la generation qu'elle expose n'a "
        f"personne pour la reclamer",
    ))
    during = (q3.get("during") or {}).get("notes") or []
    postponed = [n for n in during if _ROLLOVER_REFUSED in n]
    miss3 = _missing(q3)
    out.append((
        "q3_second_refusal_postpones_and_keeps_rolling",
        q3.get("error") is None
        and (not q3.get("blocked") or (bool(postponed)
                                       and q3.get("rolling_kept") is True))
        and not miss3 and q3.get("ordered") is True
        and q3.get("rolling_left") is False,
        f"lecteur sur .1 pendant deux rotations : lignes '#'={during} "
        f"'.rolling' garde pendant le refus={q3.get('rolling_kept')} ; apres "
        f"depart du lecteur : generations manquantes={miss3} chaine="
        f"{q3.get('layout')} ordre={q3.get('ordered')} '.rolling' restant="
        f"{q3.get('rolling_left')} (attendu False) : une rotation qui ne "
        f"peut pas finir le glissement en attente doit se reporter en le "
        f"disant, jamais effacer la generation en attente",
    ))
    unparsed = (q1.get("unparsed") or []) + (q3.get("unparsed") or [])
    out.append((
        "q4_chain_stays_readable_by_parse_log",
        q1.get("error") is None and q3.get("error") is None and not unparsed,
        f"lignes illisibles pour parse_log (hors lignes '#')={unparsed[:5]} : "
        f"un lecteur du journal (parse_log.read_window, runner) perdrait ces "
        f"enregistrements",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
