"""tx_harness_log_reader_never_blocks_rotation - RecoverLand validation runtime.

Invariant: I-SUITE-LOG-READER (lire le journal de debug pour la suite ne
doit jamais empecher le plugin de le faire tourner, ni lui faire perdre
une generation).

Le mecanisme
------------
Sous Windows, open() de Python partage le fichier en lecture et en
ecriture, mais PAS en suppression : tant qu'il est ouvert, personne ne
peut le renommer ni l'effacer. Or la rotation du journal de debug
(`core.logger._SharedRotatingFileHandler.doRollover`) renomme : le
fichier courant part en `.rolling`, les copies glissent (.1 -> .2, ...),
puis `.rolling` devient `.1`. La suite lit ces memes fichiers :

    - `parse_log.log_anchor` ouvre le fichier courant au debut de chaque
      scenario ;
    - `parse_log.read_window` ouvre le fichier de depart de la fenetre
      (souvent une copie, `.1`) et les plus recents, a la fin ;
    - `parse_log.iter_records` / `read_records` (golden, log_format_audit)
      est un generateur qui garde le fichier ouvert tant que l'appelant
      n'a pas fini de lire.

Qu'une ligne du plugin (un thread de fond, un processus fils QGIS qui
partage le journal du profil) fasse tourner le journal pendant qu'une de
ces lectures tient un fichier, et :

    - si c'est une COPIE : le glissement echoue en silence (le produit
      l'avale), la generation qui vient de tourner reste dans `.rolling`,
      et la rotation SUIVANTE commence par effacer `.rolling`. 5 Mo de
      journal forensique disparaissent sans une ligne pour le dire.
      (La perte elle-meme est un defaut du produit, signale au
      proprietaire de core/logger.py ; la lecture de la suite en etait un
      declencheur.)
    - si c'est le fichier COURANT : la rotation echoue, est reportee de
      60 s, et le journal recoit "# rollover postponed (...)", qui accuse
      une instance QGIS soeur alors que le lecteur etait la suite
      elle-meme.

Consequence concrete : apres un run de la suite, les lignes qu'on vient
rouvrir pour comprendre un rouge (ou celles d'un incident anterieur,
gardees dans les copies) ne sont plus sur le disque, et c'est l'outil de
validation qui les a fait effacer.

Ce que le scenario fait, sur des fichiers temporaires tenus par le vrai
gestionnaire du produit (chemin normal emit -> shouldRollover ->
doRollover, le seuil abaisse le temps d'une ligne, verrou tenu) :

    A. read_window tient la copie `.1` ou sa fenetre commence quand une
       rotation arrive ;
    B. log_anchor tient le fichier courant quand une rotation arrive ;
    C. iter_records est suspendu au milieu de `.1` quand une rotation
       arrive.

Pour A et B, la rotation est lancee a l'instant precis ou parse_log
tient le fichier : juste apres le os.fstat(fh.fileno()) par lequel il lit
l'identite du fichier qu'il vient d'ouvrir (le module os vu par parse_log
est remplace le temps d'un appel). Pour C, le generateur suspendu tient
le fichier. Une precondition verifie chaque fois que la rotation a bien
eu lieu PENDANT que le fichier etait tenu : sans elle, le scenario ne
prouverait rien.

Il verifie ensuite que la rotation a abouti (aucun `.rolling` restant,
aucun "rollover postponed", chaque generation a sa place), que la
lecture a rendu ce qu'elle devait (fenetre complete et dans l'ordre ;
toutes les lignes de la copie lue par iter_records), et qu'apres une
rotation de plus, chaque generation est encore sur le disque.
"""
from __future__ import annotations

import inspect
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_harness_log_reader_never_blocks_rotation"
INVARIANT = "I-SUITE-LOG-READER"

_BACKUPS = 5
_C_LINES = 200
_FMT = ("%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] "
        "%(message)s")
_GEN_RE = re.compile(r"\] (G\d+)_")
_POSTPONED = "rollover postponed"


# --------------------------------------------------------------------- #
# Le gestionnaire du produit, sur un fichier temporaire
# --------------------------------------------------------------------- #

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
        "zz.logreader", logging.INFO, __file__, 0, text, None, None))


def _rotate(handler, text: str) -> None:
    """Fait tourner par le chemin normal du gestionnaire.

    Le seuil est abaisse a 1 octet le temps d'UNE ligne : emit() voit le
    fichier plein, appelle doRollover() puis ecrit la ligne (premiere de
    la nouvelle generation) dans le fichier neuf.
    """
    handler.acquire()
    try:
        saved = handler.maxBytes
        handler.maxBytes = 1
        try:
            _emit(handler, text)
        finally:
            handler.maxBytes = saved
    finally:
        handler.release()


def _chain(path: str) -> dict:
    """{suffixe: texte} des fichiers de la chaine presents sur le disque."""
    out = {}
    suffixes = ["", ".rolling"] + [f".{i}" for i in range(1, _BACKUPS + 2)]
    for suffix in suffixes:
        p = path + suffix
        if os.path.isfile(p):
            with open(p, encoding="utf-8", errors="replace") as fh:
                out[suffix] = fh.read()
    return out


def _layout(path: str) -> dict:
    """{suffixe: generation de sa premiere ligne}, ex. {"": "G3", ".1": "G2"}."""
    out = {}
    for suffix, text in _chain(path).items():
        m = _GEN_RE.search(text)
        out[suffix or "live"] = m.group(1) if m else None
    return out


def _gens_on_disk(path: str) -> list:
    found = set()
    for text in _chain(path).values():
        found.update(_GEN_RE.findall(text))
    return sorted(found, key=lambda g: int(g[1:]))


def _postponed(path: str) -> bool:
    return any(_POSTPONED in text for text in _chain(path).values())


class _RotateWhileHeld:
    """Le module os vu par parse_log, le temps d'UN appel.

    parse_log lit l'identite de chaque fichier qu'il ouvre par
    os.fstat(fh.fileno()), juste apres l'avoir ouvert. La premiere fois
    que ce fichier est `target`, la rotation est lancee la : parse_log
    tient le fichier ouvert a cet instant precis. Tout le reste est le
    vrai module os.
    """

    def __init__(self, real_os, target, action):
        self._os = real_os
        self._target = target
        self._action = action
        self.fired = False

    def __getattr__(self, name):
        return getattr(self._os, name)

    def fstat(self, fd):
        st = self._os.fstat(fd)
        if not self.fired and (st.st_dev, st.st_ino) == self._target:
            self.fired = True
            self._action()
        return st


def _rotation_while_held(target_path: str, action, call):
    """``(fired, result)`` de ``call()``, ``action`` lancee pendant qu'il
    tient ``target_path``."""
    from scripts.validation import parse_log

    st = os.stat(target_path)
    real = parse_log.os
    hook = _RotateWhileHeld(real, (st.st_dev, st.st_ino), action)
    parse_log.os = hook
    try:
        result = call()
    finally:
        parse_log.os = real
    return hook.fired, result


# --------------------------------------------------------------------- #
# Les trois lectures
# --------------------------------------------------------------------- #

def _case_read_window(base: str) -> dict:
    """A. read_window tient la copie .1 ou sa fenetre commence."""
    from scripts.validation import parse_log

    res = {"api": all(hasattr(parse_log, n)
                      for n in ("log_anchor", "read_window"))}
    if not res["api"]:
        return res
    path = os.path.join(base, "a.log")
    handler = _handler(path)
    try:
        for i in range(3):
            _emit(handler, f"G0_OLD i={i}")
        _rotate(handler, "G1_FIRST")
        _emit(handler, "G1_BEFORE_ANCHOR")
        anchor = parse_log.log_anchor(path)
        _emit(handler, "G1_IN_A")
        _rotate(handler, "G2_FIRST")
        _emit(handler, "G2_IN_B")
        # Le fichier de depart de la fenetre est maintenant .1.
        res["held"], window = _rotation_while_held(
            path + ".1", lambda: _rotate(handler, "G3_DURING_READ"),
            lambda: parse_log.read_window(anchor))
        res["layout"] = _layout(path)
        res["postponed"] = _postponed(path)
        res["events"] = [r.event for r in window.records]
        res["complete"] = window.complete
        res["note"] = window.note
        _rotate(handler, "G4_FIRST")
    finally:
        handler.close()
    res["gens_after_next"] = _gens_on_disk(path)
    res["layout_after_next"] = _layout(path)
    return res


def _case_log_anchor(base: str) -> dict:
    """B. log_anchor tient le fichier courant."""
    from scripts.validation import parse_log

    res = {"api": all(hasattr(parse_log, n)
                      for n in ("log_anchor", "read_window"))}
    if not res["api"]:
        return res
    path = os.path.join(base, "b.log")
    handler = _handler(path)
    try:
        for i in range(3):
            _emit(handler, f"G0_OLD i={i}")
        res["held"], anchor = _rotation_while_held(
            path, lambda: _rotate(handler, "G1_DURING_ANCHOR"),
            lambda: parse_log.log_anchor(path))
        _emit(handler, "G1_AFTER_ANCHOR")
        res["layout"] = _layout(path)
        res["postponed"] = _postponed(path)
        window = parse_log.read_window(anchor)
        res["events"] = [r.event for r in window.records]
        res["complete"] = window.complete
        res["note"] = window.note
        _rotate(handler, "G2_FIRST")
    finally:
        handler.close()
    res["gens_after_next"] = _gens_on_disk(path)
    res["layout_after_next"] = _layout(path)
    return res


def _open_file_in_frame(gen) -> bool:
    """Le generateur suspendu tient-il un fichier ouvert ?"""
    frame = getattr(gen, "gi_frame", None)
    if frame is None:
        return False
    for value in frame.f_locals.values():
        if hasattr(value, "fileno") and getattr(value, "closed", True) is False:
            return True
    return False


def _case_iter_records(base: str) -> dict:
    """C. iter_records suspendu au milieu de .1."""
    from scripts.validation import parse_log

    path = os.path.join(base, "c.log")
    handler = _handler(path)
    res = {}
    gen = None
    try:
        for i in range(_C_LINES):
            _emit(handler, f"G0_LINE i={i}")
        _rotate(handler, "G1_FIRST")
        _emit(handler, "G1_MORE")
        gen = parse_log.iter_records(path + ".1")
        first = next(gen)
        res["held"] = (inspect.getgeneratorstate(gen)
                       == inspect.GEN_SUSPENDED and _open_file_in_frame(gen))
        _rotate(handler, "G2_WHILE_ITERATING")
        rest = list(gen)
        got = [first] + rest
        res["read_count"] = sum(1 for r in got if r.event == "G0_LINE")
        res["read_in_order"] = ([r.fields.get("i") for r in got]
                                == [str(i) for i in range(_C_LINES)])
        res["layout"] = _layout(path)
        res["postponed"] = _postponed(path)
        _rotate(handler, "G3_FIRST")
    finally:
        if gen is not None:
            gen.close()
        handler.close()
    res["gens_after_next"] = _gens_on_disk(path)
    res["layout_after_next"] = _layout(path)
    return res


# --------------------------------------------------------------------- #
# Contrat
# --------------------------------------------------------------------- #

def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["dir"] = tempfile.mkdtemp(prefix="rl_logrdr_")
    flog(f"tx_harness_log_reader_never_blocks_rotation setup: "
         f"trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    base = ctx.data["dir"]
    for key, case in (("a", _case_read_window), ("b", _case_log_anchor),
                      ("c", _case_iter_records)):
        sub = os.path.join(base, key)
        os.makedirs(sub, exist_ok=True)
        try:
            ctx.data[key] = case(sub)
        except Exception as exc:  # noqa: BLE001 - reported by the assertions
            ctx.data[key] = {"error": f"{type(exc).__name__}: {exc}"}
    flog(f"tx_harness_log_reader_never_blocks_rotation run end "
         f"trace_id={ctx.trace_id}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("dir") or "", ignore_errors=True)


def _rotation_checks(out, key, label, res, want_layout, want_gens,
                     want_after):
    err = res.get("error")
    out.append((
        f"{key}_fixture_rotation_ran_while_the_reader_held_the_file",
        err is None and res.get("api", True) is True
        and res.get("held") is True,
        f"{label} : erreur={err!r} api={res.get('api', True)} rotation "
        f"lancee pendant que la lecture tenait le fichier="
        f"{res.get('held')} (attendu True) : sinon le scenario ne prouve "
        f"rien",
    ))
    layout = res.get("layout")
    out.append((
        f"{key}_rotation_during_the_read_completes",
        err is None and layout == want_layout
        and res.get("postponed") is False,
        f"{label} : chaine apres la rotation={layout} (attendu "
        f"{want_layout}) ; ligne '{_POSTPONED}'={res.get('postponed')} "
        f"(attendu False). Un '.rolling' restant est une generation que la "
        f"rotation suivante efface ; un report accuse a tort une instance "
        f"QGIS soeur et laisse le journal depasser 5 Mo",
    ))
    gens = res.get("gens_after_next")
    out.append((
        f"{key}_every_generation_still_on_disk_after_the_next_rotation",
        err is None and gens == want_gens
        and res.get("layout_after_next") == want_after,
        f"{label} : generations sur le disque apres la rotation suivante="
        f"{gens} (attendu {want_gens}) ; chaine={res.get('layout_after_next')}"
        f" (attendu {want_after}). Une generation absente, ce sont 5 Mo de "
        f"journal forensique effaces sans une ligne pour le dire ; une "
        f"autre chaine, une rotation qui n'a pas eu lieu",
    ))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []

    a = d.get("a") or {}
    _rotation_checks(
        out, "a", "read_window tenant la copie .1", a,
        {"live": "G3", ".1": "G2", ".2": "G1", ".3": "G0"},
        ["G0", "G1", "G2", "G3", "G4"],
        {"live": "G4", ".1": "G3", ".2": "G2", ".3": "G1", ".4": "G0"})
    want_a = ["G1_IN_A", "G2_FIRST", "G2_IN_B", "G3_DURING_READ"]
    out.append((
        "a_read_window_still_returns_the_whole_window",
        a.get("events") == want_a and a.get("complete") is True,
        f"evenements={a.get('events')} (attendu {want_a}) complete="
        f"{a.get('complete')} note={a.get('note')!r} : une rotation pendant "
        f"la lecture ne doit ni perdre ni doubler une ligne de la fenetre",
    ))

    b = d.get("b") or {}
    _rotation_checks(
        out, "b", "log_anchor tenant le fichier courant", b,
        {"live": "G1", ".1": "G0"},
        ["G0", "G1", "G2"],
        {"live": "G2", ".1": "G1", ".2": "G0"})
    want_b = ["G1_DURING_ANCHOR", "G1_AFTER_ANCHOR"]
    out.append((
        "b_window_from_that_anchor_is_whole",
        b.get("events") == want_b and b.get("complete") is True,
        f"evenements={b.get('events')} (attendu {want_b}) complete="
        f"{b.get('complete')} note={b.get('note')!r}",
    ))

    c = d.get("c") or {}
    _rotation_checks(
        out, "c", "iter_records suspendu dans .1", c,
        {"live": "G2", ".1": "G1", ".2": "G0"},
        ["G0", "G1", "G2", "G3"],
        {"live": "G3", ".1": "G2", ".2": "G1", ".3": "G0"})
    out.append((
        "c_iter_records_reads_the_whole_file_it_started",
        c.get("read_count") == _C_LINES and c.get("read_in_order") is True,
        f"lignes G0 lues={c.get('read_count')} (attendu {_C_LINES}) dans "
        f"l'ordre={c.get('read_in_order')} : le fichier renomme sous le "
        f"lecteur doit etre lu jusqu'au bout, comme avant la rotation",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"tx_harness_log_reader_never_blocks_rotation.*trace_id="
        rf"{ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
