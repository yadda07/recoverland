# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_harness_log_window_rotation - RecoverLand validation runtime.

Invariant: I-SUITE-LOG-WINDOW (les lignes de journal qu'un scenario verifie
sont toutes celles ecrites pendant ce scenario, chacune une seule fois et
dans l'ordre, meme quand le journal de debug tourne pendant le scenario).

Le mecanisme
------------
Le journal de debug (`recoverland_debug.log`) est un fichier tournant de
5 Mo gardant 5 copies (`core.logger._SharedRotatingFileHandler`). Le
lanceur (`runner.py`) lisait la fenetre d'un scenario par position
d'octets dans CE fichier : taille avant setup(ctx), puis lecture a partir
de cette position apres run(ctx). Quand le fichier tourne pendant le
scenario, le debut de la fenetre part dans `recoverland_debug.log.1` et le
fichier courant, neuf, est plus court que la position memorisee : la
lecture commence apres la fin du fichier et rend zero ligne (le rapport
JSON affiche alors un `log_window_bytes` negatif).

Consequence concrete pour qui lit le verdict : toutes les assertions de
journal du scenario (`assert_log_contains`, `trace_id_propagated`)
echouent avec n=0 sur un code qui s'est parfaitement comporte, et
seulement quand le scenario tombe au moment ou le fichier atteint 5 Mo,
c'est-a-dire selon l'ordre de la suite : rw_rewind_shp_record_identity
dans l'ordre par defaut, rw_rewind_mass_exec_bench dans l'ordre melange,
rw_state_at_t_property plus tot. Un faux rouge intermittent apprend a
ignorer les rouges, et un vrai defaut passe ensuite inapercu.

Ce que le scenario fait, avec le vrai gestionnaire de rotation du
produit (chemin normal emit -> shouldRollover -> doRollover, le seuil
etant abaisse le temps d'une ligne, verrou du gestionnaire tenu) :

    - des marqueurs du scenario avant, entre et apres DEUX rotations, et
      un marqueur ecrit depuis un autre thread ;
    - des lignes ecrites directement dans le fichier par un autre
      ecrivain (comme un processus fils QGIS lance par un scenario, qui
      partage le journal du profil), avant et apres chaque rotation ;

puis verifie que ctx.records les contient toutes, chacune une seule
fois, dans l'ordre ou elles ont ete ecrites, au format de parse_log
(horodatage, niveau, thread, champs key=value). A l'inverse, une ligne
de ce processus que le fichier n'a jamais recue (filtre pose sur le
gestionnaire fichier, comme une ecriture qui echoue) ne doit PAS
apparaitre dans ctx.records, et le lanceur doit le dire : d'autres
scenarios verifient justement que le journal ne perd aucune ligne, et
une fenetre qui comblerait les trous depuis la memoire les ferait
passer sur un journal troue.

Il verifie aussi, sur un fichier temporaire tenu par une instance du
meme gestionnaire du produit, la lecture qui suit la rotation
(`parse_log.log_anchor` / `read_window`), y compris le cas ou le fichier
de depart a ete efface (plus de rotations que de copies gardees) : ce
cas doit etre dit, jamais rendu comme une fenetre complete. Dans ce
meme cas, la fenetre du lanceur doit encore rendre les lignes de CE
processus, gardees en memoire par un gestionnaire branche sur le logger
du plugin le temps du scenario.

Effet de bord assume : le journal de debug du profil tourne deux fois
(comme apres 10 Mo de journal) ; le lanceur headless utilise un profil
de test.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

SCENARIO_ID = "tx_harness_log_window_rotation"
INVARIANT = "I-SUITE-LOG-WINDOW"

_FILE_LOGGER = "RecoverLand.FileDebug"
_FOREIGN_THREAD = "ChildQGIS"
_WORKER_THREAD = "LogWinWorker"
# Ordre d'ecriture des marqueurs pendant run(ctx).
_ORDER = ("LOGWIN_MARK_A", "LOGWIN_FOREIGN_A", "LOGWIN_MARK_B",
          "LOGWIN_FOREIGN_B", "LOGWIN_MARK_T", "LOGWIN_MARK_C",
          "LOGWIN_FOREIGN_C", "LOGWIN_MARK_D")


def _live_handler(log_path):
    """Le gestionnaire tournant du produit qui ecrit `log_path`."""
    import logging

    target = os.path.normcase(os.path.abspath(str(log_path)))
    for handler in logging.getLogger(_FILE_LOGGER).handlers:
        base = getattr(handler, "baseFilename", None)
        if (base and hasattr(handler, "maxBytes")
                and os.path.normcase(os.path.abspath(base)) == target):
            return handler
    return None


def _ident(path):
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def _rotate_with(handler, write) -> None:
    """Fait tourner le fichier par le chemin normal du gestionnaire.

    Le seuil est abaisse a 1 octet le temps d'UNE ligne : emit() voit le
    fichier plein, appelle doRollover() puis ecrit la ligne dans le
    fichier neuf. Le verrou du gestionnaire est tenu pendant ce temps,
    pour qu'une ligne d'un autre thread ne declenche pas une rotation de
    plus.
    """
    handler.acquire()
    try:
        saved = handler.maxBytes
        handler.maxBytes = 1
        try:
            write()
        finally:
            handler.maxBytes = saved
    finally:
        handler.release()


def _foreign(path, text: str) -> None:
    """Une ligne ecrite par un autre ecrivain du meme fichier.

    C'est ce que fait un processus fils QGIS lance par un scenario : il
    importe le plugin avec le meme profil et ajoute ses lignes au meme
    journal. Le fichier est ferme aussitot (ouvert, il empecherait la
    rotation sous Windows).
    """
    stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"{stamp} [INFO   ] [{_FOREIGN_THREAD:<15}] {text}\n")


def _pause() -> None:
    # Deux ecritures successives ne partagent pas la meme milliseconde :
    # l'ordre attendu ne depend pas de la resolution de l'horodatage.
    time.sleep(0.005)


# --------------------------------------------------------------------- #
# Lecture qui suit la rotation, sur un fichier temporaire
# --------------------------------------------------------------------- #

def _offline_handler(path: str, backups: int):
    import logging

    from recoverland.core.logger import _SharedRotatingFileHandler

    handler = _SharedRotatingFileHandler(
        path, mode="a", maxBytes=10 ** 9, backupCount=backups,
        encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] "
        "%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
    return handler


def _offline_emit(handler, text: str) -> None:
    import logging

    handler.handle(logging.LogRecord(
        "zz.logwin", logging.INFO, __file__, 0, text, None, None))


def _offline_case(base: str, backups: int, rotations: int) -> dict:
    """Ancre, puis `rotations` rotations du gestionnaire du produit."""
    from scripts.validation import parse_log

    res = {"api": all(hasattr(parse_log, n)
                      for n in ("log_anchor", "read_window"))}
    if not res["api"]:
        return res
    path = os.path.join(base, f"offline_{backups}_{rotations}.log")
    handler = _offline_handler(path, backups)
    try:
        for i in range(20):
            _offline_emit(handler, f"OFFLINE_BEFORE i={i}")
        anchor = parse_log.log_anchor(path)
        _offline_emit(handler, "OFFLINE_IN_0")
        for k in range(1, rotations + 1):
            _rotate_with(handler, lambda k=k: _offline_emit(
                handler, f"OFFLINE_IN_{k}"))
        _offline_emit(handler, "OFFLINE_IN_END")
    finally:
        handler.close()
    window = parse_log.read_window(anchor)
    res.update(
        events=[r.event for r in window.records],
        complete=window.complete, rotations=window.rotations,
        note=window.note, files=[os.path.basename(f) for f in window.files])
    return res


def _offline_memory_case(base: str) -> dict:
    """La fenetre du lanceur quand son fichier de depart a ete efface.

    Un second fichier (gestionnaire du produit, 2 copies) est branche sur
    le logger du plugin ; la fenetre du lanceur est ouverte sur lui, puis
    le fichier tourne 4 fois : les lignes du debut ne sont plus sur le
    disque, seule la capture en memoire les a encore. La rotation passe
    par doRollover() sous le seul verrou de ce gestionnaire : ecrire une
    ligne en le tenant prendrait les verrous des autres gestionnaires du
    logger dans l'ordre inverse de celui d'un autre thread.
    """
    import logging

    from recoverland.core.logger import flog
    from scripts.validation import runner

    res = {"api": hasattr(runner, "_LogWindow")}
    if not res["api"]:
        return res
    path = os.path.join(base, "offline_memory.log")
    handler = _offline_handler(path, backups=2)
    logger = logging.getLogger(_FILE_LOGGER)
    logger.addHandler(handler)
    try:
        flog("OFFLINE_MEM_BEFORE line before the window", "INFO")
        window = runner._LogWindow(path)
        flog("OFFLINE_MEM_IN_0 first line of the window", "INFO")
        for k in range(1, 5):
            _pause()
            handler.acquire()
            try:
                handler.doRollover()
            finally:
                handler.release()
            flog(f"OFFLINE_MEM_IN_{k} after rotation {k}", "INFO")
        _pause()
        flog("OFFLINE_MEM_IN_END last line of the window", "INFO")
        records, info = window.close()
    finally:
        logger.removeHandler(handler)
        handler.close()
    res.update(events=[r.event for r in records
                       if r.event.startswith("OFFLINE_MEM")],
               complete=info.get("complete"),
               restored=info.get("restored_from_memory"),
               capture=info.get("memory_capture"))
    return res


class _RefuseMarker:
    """Filtre pose sur le gestionnaire FICHIER : la ligne n'atteint pas le
    journal, comme une ecriture qui echoue (disque plein, encodage)."""

    def __init__(self, token: str):
        self.token = token

    def filter(self, record) -> bool:
        return self.token not in record.getMessage()


# --------------------------------------------------------------------- #
# Contrat
# --------------------------------------------------------------------- #

def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["offline_dir"] = tempfile.mkdtemp(prefix="rl_logwin_")
    flog(f"tx_harness_log_window_rotation setup: trace_id={ctx.trace_id} "
         f"log={ctx.log_path}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    tid = ctx.trace_id
    path = str(ctx.log_path)
    d = ctx.data
    handler = _live_handler(path)
    d["handler_found"] = handler is not None
    d["ident_start"] = _ident(path)

    flog(f"LOGWIN_MARK_A before the first rotation trace_id={tid}", "INFO")
    _pause()
    _foreign(path, f"LOGWIN_FOREIGN_A child line trace_id={tid}")
    _pause()
    if handler is not None:
        _rotate_with(handler, lambda: flog(
            f"LOGWIN_MARK_B first line after rotation 1 trace_id={tid}",
            "WARNING"))
    d["ident_after_1"] = _ident(path)
    _pause()
    _foreign(path, f"LOGWIN_FOREIGN_B child line trace_id={tid}")
    _pause()

    def _worker():
        flog(f"LOGWIN_MARK_T from another thread trace_id={tid}", "INFO")

    worker = threading.Thread(target=_worker, name=_WORKER_THREAD)
    worker.start()
    worker.join(10)
    _pause()
    if handler is not None:
        _rotate_with(handler, lambda: flog(
            f"LOGWIN_MARK_C first line after rotation 2 trace_id={tid}",
            "INFO"))
    _pause()
    _foreign(path, f"LOGWIN_FOREIGN_C child line trace_id={tid}")
    _pause()
    # Une ligne de ce processus que le fichier n'a jamais recue : la
    # fenetre ne doit pas l'inventer (des scenarios verifient que le
    # journal ne perd pas de lignes), mais le lanceur doit le dire.
    if handler is not None:
        refuse = _RefuseMarker("LOGWIN_DROPPED")
        handler.addFilter(refuse)
        try:
            flog(f"LOGWIN_DROPPED refused by the file handler "
                 f"trace_id={tid}", "INFO")
        finally:
            handler.removeFilter(refuse)
    d["ident_end"] = _ident(path)
    d["ident_dot2"] = _ident(path + ".2")
    d["ident_dot1"] = _ident(path + ".1")

    # Hors du journal du profil : 2 rotations sur 5 copies (fenetre suivie
    # jusqu'au bout), puis 4 rotations sur 2 copies (fichier de depart
    # efface).
    base = d["offline_dir"]
    d["offline_kept"] = _offline_case(base, backups=5, rotations=2)
    d["offline_lost"] = _offline_case(base, backups=2, rotations=4)
    d["offline_memory"] = _offline_memory_case(base)

    flog(f"LOGWIN_MARK_D tx_harness_log_window_rotation run end "
         f"trace_id={tid}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("offline_dir") or "", ignore_errors=True)


def _count(records, token: str) -> int:
    return sum(1 for r in records if token in r.message)


def assertions(ctx):
    from scripts.validation.assert_log import (
        assert_field_value, assert_log_contains, assert_sequence_in_order)

    d = ctx.data
    recs = ctx.records
    out = []

    # ===== Precondition : le journal a bien tourne deux fois ============
    start = d.get("ident_start")
    rotated = (start is not None and d.get("ident_dot2") == start
               and d.get("ident_after_1") not in (None, start)
               and d.get("ident_end") not in (None, start,
                                              d.get("ident_after_1")))
    out.append((
        "fixture_log_rotated_twice_mid_scenario",
        d.get("handler_found") is True and rotated,
        f"gestionnaire tournant trouve={d.get('handler_found')} ; fichier "
        f"de depart maintenant en .2={d.get('ident_dot2') == start} ; "
        f"fichier courant neuf apres chaque rotation="
        f"{d.get('ident_after_1') != start}/"
        f"{d.get('ident_end') not in (start, d.get('ident_after_1'))} : "
        f"sans les deux rotations, ce scenario ne prouve rien",
    ))

    # ===== Chaque ligne, une seule fois =================================
    for token, where in (
            ("LOGWIN_MARK_A", "ecrite avant la premiere rotation, partie "
                              "dans .2"),
            ("LOGWIN_MARK_B", "premiere ligne du fichier ouvert par la "
                              "rotation 1"),
            ("LOGWIN_MARK_T", "ecrite par un autre thread entre les deux "
                              "rotations"),
            ("LOGWIN_MARK_C", "premiere ligne du fichier ouvert par la "
                              "rotation 2"),
            ("LOGWIN_MARK_D", "derniere ligne du scenario")):
        out.append(assert_log_contains(
            recs, rf"{token} .*trace_id={ctx.trace_id}",
            name=f"window_keeps_{token.lower()}", min_count=1, max_count=1))
        out[-1] = (out[-1][0], out[-1][1],
                   f"{out[-1][2]} ({where}) : une ligne du scenario absente "
                   f"de ctx.records fait echouer ses assertions de journal "
                   f"avec n=0 sur un code correct ; comptee deux fois, elle "
                   f"fausse les compteurs exacts")
    for token in ("LOGWIN_FOREIGN_A", "LOGWIN_FOREIGN_B", "LOGWIN_FOREIGN_C"):
        n = _count(recs, token)
        out.append((
            f"window_keeps_other_writer_{token[-1].lower()}",
            n == 1,
            f"{token} vue {n} fois (attendu 1) : les lignes d'un processus "
            f"fils qui partage le journal du profil faisaient partie de la "
            f"fenetre ; les perdre affaiblit en silence les assertions "
            f"d'absence (assert_log_absent) qui les couvraient",
        ))

    # ===== Ordre et format ==============================================
    out.append(assert_sequence_in_order(
        recs, [rf"{t} " for t in _ORDER], name="window_order_is_write_order"))
    out.append(assert_field_value(
        recs, r"LOGWIN_MARK_B ", "trace_id", ctx.trace_id,
        name="record_fields_parsed_like_parse_log"))
    b = next((r for r in recs if "LOGWIN_MARK_B " in r.message), None)
    t = next((r for r in recs if "LOGWIN_MARK_T " in r.message), None)
    out.append((
        "record_level_and_thread_parsed_like_parse_log",
        b is not None and b.level == "WARNING" and t is not None
        and t.thread == _WORKER_THREAD and b.ts is not None,
        f"niveau de B={getattr(b, 'level', None)!r} (attendu 'WARNING'), "
        f"thread de T={getattr(t, 'thread', None)!r} (attendu "
        f"{_WORKER_THREAD!r}) : ctx.records doit garder le format que "
        f"parse_log produit, sinon les assertions sur les champs lisent "
        f"autre chose que le journal",
    ))
    out.append(assert_log_contains(
        recs, rf"tx_harness_log_window_rotation.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    window = getattr(ctx, "log_window", None) or {}
    dropped = _count(recs, "LOGWIN_DROPPED")
    out.append((
        "window_never_adds_a_line_the_log_lacks_and_says_so",
        dropped == 0 and (window.get("not_in_log") or 0) >= 1,
        f"ligne emise par ce processus mais refusee par le fichier : vue "
        f"{dropped} fois dans ctx.records (attendu 0), signalee par le "
        f"lanceur={window.get('not_in_log')} (attendu >= 1). Glissee dans "
        f"la fenetre, elle ferait passer les scenarios qui verifient que le "
        f"journal de debug ne perd aucune ligne ; tue, la perte passerait "
        f"inapercue",
    ))

    # ===== Lecture qui suit la rotation, hors du profil =================
    kept = d.get("offline_kept") or {}
    want = ["OFFLINE_IN_0", "OFFLINE_IN_1", "OFFLINE_IN_2", "OFFLINE_IN_END"]
    out.append((
        "read_window_follows_rotation_chain",
        kept.get("api") is True and kept.get("events") == want
        and kept.get("complete") is True and kept.get("rotations") == 2,
        f"api={kept.get('api')} evenements={kept.get('events')} (attendu "
        f"{want}, ni une ligne d'avant l'ancre ni un doublon) complete="
        f"{kept.get('complete')} rotations={kept.get('rotations')} "
        f"fichiers={kept.get('files')}",
    ))
    lost = d.get("offline_lost") or {}
    out.append((
        "read_window_says_when_its_start_was_rotated_away",
        lost.get("api") is True and lost.get("complete") is False
        and bool(lost.get("note"))
        and "OFFLINE_IN_END" in (lost.get("events") or [])
        and not any(e.startswith("OFFLINE_BEFORE")
                    for e in (lost.get("events") or [])),
        f"4 rotations sur 2 copies gardees : complete={lost.get('complete')} "
        f"note={lost.get('note')!r} evenements={lost.get('events')} : le "
        f"fichier de depart est efface, la fenetre ne peut plus etre "
        f"complete et doit le dire (ce qui reste est rendu, sans ligne "
        f"d'avant le depart)",
    ))
    mem = d.get("offline_memory") or {}
    want_mem = ["OFFLINE_MEM_IN_0", "OFFLINE_MEM_IN_1", "OFFLINE_MEM_IN_2",
                "OFFLINE_MEM_IN_3", "OFFLINE_MEM_IN_4", "OFFLINE_MEM_IN_END"]
    out.append((
        "runner_window_keeps_own_records_whose_file_was_rotated_away",
        mem.get("api") is True and mem.get("events") == want_mem
        and mem.get("complete") is False
        and (mem.get("restored") or 0) >= 1,
        f"api={mem.get('api')} capture={mem.get('capture')!r} evenements="
        f"{mem.get('events')} (attendu {want_mem}) complete="
        f"{mem.get('complete')} lignes rendues par la seule memoire="
        f"{mem.get('restored')} : quand le fichier ou la fenetre a "
        f"commence est efface par les rotations, les lignes de CE processus "
        f"doivent encore etre la, a leur place, et la fenetre se dire "
        f"incomplete",
    ))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
