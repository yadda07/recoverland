"""rs_workflow_service_journal - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant toute ecriture) sans refus
injustifie, sur le point d'entree public core.workflow_service
(execute_grouped_restore, execute_grouped_undo), et fail-safe quand le
journal manque.

Le mecanisme
------------
Une cle GeoPackage n'est une identite que si le contenu la corrobore, ou
si le journal prouve qu'elle n'a jamais change de mains
(restore_service._key_continuity). Le journal porte aussi la preuve qu'un
evenement est DEJA compense par une trace en vigueur (refus
`already_compensated`) : sans lui, une deuxieme restauration du meme
evenement est ecrite par-dessus une edition posterieure. restore_batch et
undo_restore_batch acceptent le journal (journal=), les runners le leur
passent ; workflow_service les appelait SANS journal : 'flag: False ->
True' sur une couche ou presque toutes les lignes sont a True restait
refuse "key_continuity_unproven (no_journal)" par ce chemin, alors que la
cle n'a jamais change de mains, et le garde `already_compensated` etait
eteint.

Ce que l'utilisateur voit
-------------------------
Un outil (ou un script) qui restaure par workflow_service une case cochee
par erreur obtient "Target feature could not be verified" ; le meme
evenement passe par le dialogue (runner) revient.

Cas (GeoPackage, cle fid, vrai fichier journal SQLite ; C et D a FAUX)
----------------------------------------------------------------------
    W1  execute_grouped_restore(journal_path=) : FAUX revient sur C
        (FAUX partage par D : le contenu seul ne prouve rien)
    W2  execute_grouped_undo(journal_path=) : l'annulation remet VRAI
        (C porte FAUX, partage par D : seule la continuite prouve la cle)
    N1  temoin executeur : restore_batch sans journal refuse NOMME
        (key_continuity_unproven), couche intacte
    N2  execute_grouped_restore sans journal : refus nomme, rien d'ecrit
        (sans journal, le garde already_compensated est eteint)
    N3  execute_grouped_undo sans journal : l'executeur refuse NOMME
        (key_continuity_unproven), C reste a FAUX
    U1  journal donne mais illisible : chaque couche refusee nommee, rien
        d'ecrit, la couche n'est meme pas cherchee
    C1  UNE connexion en lecture seule par groupe (par source), fermee sur
        CHAQUE chemin : succes, couche introuvable, exception de la
        resolution de couche, annulation

Verdict avant correctif : FAIL (W1, W2 refuses ; C1 : aucune connexion ;
U1 : l'illisibilite n'est pas dite). Apres : PASS.
"""
from __future__ import annotations

import inspect
import json
import os
import shutil
import sqlite3
import tempfile

SCENARIO_ID = "rs_workflow_service_journal"
INVARIANT = "I-3"

# C (fid 3) and D (fid 4) both FALSE: after C goes back to FALSE its
# content is shared, before too (TRUE is everyone else's).
_ROWS = [("a", True, 1), ("b", True, 1), ("c", False, 1), ("d", False, 1),
         ("e", True, 1)]
_OTHER_DS = "rs_wsj_other_datasource"


def _kcj():
    from scripts.validation.scenarios import rs_key_continuity_journal as kcj
    return kcj


def _file_journal(tmp, name):
    from recoverland.core.sqlite_schema import initialize_schema
    path = os.path.join(tmp, f"{name}.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.commit()
    return path, conn


def _flag_case(tmp, name):
    """GPKG with C and D FALSE; 'flag' of C edited FALSE -> TRUE, journalled."""
    kcj = _kcj()
    path = kcj._gpkg(tmp, name, _ROWS)
    layer = kcj._open(path, name)
    delta, identity = kcj._edit_flag(layer, 3, True)
    jpath, conn = _file_journal(tmp, name)
    event = kcj._journalled(conn, kcj._event(layer, "UPDATE", identity,
                                             delta, 0))
    return path, layer, jpath, conn, event


def _flag_of_c(path, name):
    rows = _kcj()._flags(path, name)
    return next((flag for fid, _etat, flag in rows if fid == 3), "<absent>")


def _accepts(fn, kw) -> bool:
    try:
        return kw in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def _grouped(fn, *args, journal_path=None):
    """Call *fn* with journal_path when it takes it (before the fix, no)."""
    if journal_path is not None and _accepts(fn, "journal_path"):
        return fn(*args, journal_path=journal_path)
    return fn(*args)


def _result(res):
    return {"ok": int(res.total_ok), "fail": int(res.total_fail),
            "errors": [str(e)[:800] for e in res.errors]}


class _ConnRecorder:
    """Records the connections opened on the journal, read-only URIs."""

    def __init__(self):
        self.real = sqlite3.connect
        self.opened = []

    def __enter__(self):
        real = self.real

        def _connect(database, *args, **kwargs):
            conn = real(database, *args, **kwargs)
            if kwargs.get("uri") and "mode=ro" in str(database):
                self.opened.append(conn)
            return conn

        sqlite3.connect = _connect
        return self

    def __exit__(self, *exc):
        sqlite3.connect = self.real
        return False

    def open_ones(self) -> int:
        n = 0
        for conn in self.opened:
            try:
                conn.execute("SELECT 1").fetchall()
                n += 1
            except sqlite3.ProgrammingError:
                pass
        return n


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_wsj_")
    flog(f"rs_workflow_service_journal setup: trace_id={ctx.trace_id}")


def _case_w(tmp, out):
    """W1 then W2 on the same layer, through the service, journal given."""
    from recoverland.core import workflow_service as ws
    kcj = _kcj()
    path, layer, jpath, conn, event = _flag_case(tmp, "w")
    try:
        with _ConnRecorder() as rec:
            res = _grouped(ws.execute_grouped_restore, [event],
                           lambda _e: layer, journal_path=jpath)
        out["w1"] = _result(res)
        out["w1_flag"] = _flag_of_c(path, "w")
        out["w1_conns"] = [len(rec.opened), rec.open_ones()]
        # The caller journals the traces (the dialog's write queue does).
        for trace in res.trace_events:
            kcj._journalled(conn, trace)
        out["w1_traces"] = len(res.trace_events)
        # N3: the same undo without the journal, the executor's witness.
        res = ws.execute_grouped_undo({event.datasource_fingerprint: [event]},
                                      lambda _e: layer)
        out["n3"] = _result(res)
        out["n3_flag"] = _flag_of_c(path, "w")
        with _ConnRecorder() as rec:
            res = _grouped(ws.execute_grouped_undo,
                           {event.datasource_fingerprint: [event]},
                           lambda _e: layer, journal_path=jpath)
        out["w2"] = _result(res)
        out["w2_flag"] = _flag_of_c(path, "w")
        out["w2_conns"] = [len(rec.opened), rec.open_ones()]
    finally:
        layer = None  # noqa: F841 - the GeoPackage handle is released
        conn.close()


def _case_n(tmp, out):
    """N1: the executor without the journal. N2: the service without it."""
    from recoverland.core import restore_service as rs
    from recoverland.core import workflow_service as ws
    path, layer, _jpath, conn, event = _flag_case(tmp, "n1")
    try:
        rep = rs.restore_batch(layer, [event])
        out["n1"] = {"succeeded": sorted(rep.succeeded),
                     "failed": {int(k): str(v)[:300]
                                for k, v in rep.failed.items()}}
        out["n1_flag"] = _flag_of_c(path, "n1")
    finally:
        layer = None  # noqa: F841 - the GeoPackage handle is released
        conn.close()
    path, layer, _jpath, conn, event = _flag_case(tmp, "n2")
    try:
        out["n2"] = _result(ws.execute_grouped_restore([event],
                                                       lambda _e: layer))
        out["n2_flag"] = _flag_of_c(path, "n2")
    finally:
        layer = None  # noqa: F841 - the GeoPackage handle is released
        conn.close()


def _case_u(tmp, out):
    """U1: a journal path given that cannot be read."""
    from recoverland.core import workflow_service as ws
    path, layer, _jpath, conn, event = _flag_case(tmp, "u1")
    asked = []

    def _find(evt):
        asked.append(evt.event_id)
        return layer

    gone = os.path.join(tmp, "absent", "journal.sqlite")
    try:
        out["u1_restore"] = _result(_grouped(
            ws.execute_grouped_restore, [event], _find, journal_path=gone))
        out["u1_undo"] = _result(_grouped(
            ws.execute_grouped_undo, {event.datasource_fingerprint: [event]},
            _find, journal_path=gone))
        out["u1_flag"] = _flag_of_c(path, "u1")
        out["u1_layer_asked"] = len(asked)
    finally:
        layer = None  # noqa: F841 - the GeoPackage handle is released
        conn.close()


def _case_c(tmp, out):
    """C1: one connection per group, closed on every path."""
    from recoverland.core import workflow_service as ws
    kcj = _kcj()
    path, layer, jpath, conn, event = _flag_case(tmp, "c1")
    try:
        other = kcj._journalled(conn, kcj._event(
            layer, "UPDATE", event.feature_identity_json,
            json.dumps({"changed_only": {"nb": {"old": 1, "new": 2}}}), 5,
            datasource_fingerprint=_OTHER_DS))

        def _find(evt):
            # The second source is not loaded: that group is refused.
            return layer if evt.datasource_fingerprint != _OTHER_DS else None

        with _ConnRecorder() as rec:
            res = _grouped(ws.execute_grouped_restore, [event, other], _find,
                           journal_path=jpath)
        out["c1_two_groups"] = {"result": _result(res),
                                "opened": len(rec.opened),
                                "left_open": rec.open_ones()}

        def _boom(_evt):
            raise RuntimeError("layer resolution failed")

        with _ConnRecorder() as rec:
            try:
                _grouped(ws.execute_grouped_restore, [event], _boom,
                         journal_path=jpath)
                raised = None
            except RuntimeError as exc:
                raised = str(exc)
        out["c1_exception"] = {"raised": raised, "opened": len(rec.opened),
                               "left_open": rec.open_ones()}
    finally:
        layer = None  # noqa: F841 - the GeoPackage handle is released
        conn.close()


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmp"]
    out = {}
    for key, case in (("w", _case_w), ("n", _case_n), ("u", _case_u),
                      ("c", _case_c)):
        try:
            case(tmp, out)
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            out[f"{key}_crash"] = (f"{type(exc).__name__}: {exc} "
                                   f"{traceback.format_exc()[-1200:]}")
    ctx.data["out"] = out
    flog(f"rs_workflow_service_journal run: trace_id={ctx.trace_id} "
         f"{json.dumps(out, default=str)[:3500]}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    crashes = {k: v for k, v in out.items() if k.endswith("_crash")}
    res = [("no_crash", not crashes, str(crashes)[:1500])]

    w1 = out.get("w1") or {}
    res.append((
        "w1_grouped_restore_uses_the_journal",
        w1.get("ok") == 1 and w1.get("fail") == 0
        and out.get("w1_flag") is False,
        f"execute_grouped_restore(journal_path=) -> {w1} flag de C="
        f"{out.get('w1_flag')!r} : la cle de C n'a jamais change de mains, "
        f"le journal le prouve ; FAUX doit revenir par ce chemin comme par "
        f"le dialogue",
    ))
    w2 = out.get("w2") or {}
    res.append((
        "w2_grouped_undo_uses_the_journal",
        out.get("w1_traces") == 1 and w2.get("ok") == 1
        and w2.get("fail") == 0 and out.get("w2_flag") is True,
        f"execute_grouped_undo(journal_path=) -> {w2} flag de C="
        f"{out.get('w2_flag')!r} traces={out.get('w1_traces')} : C porte "
        f"FAUX comme D, seul le journal prouve la cle ; l'annulation doit "
        f"remettre VRAI",
    ))
    n1 = out.get("n1") or {}
    res.append((
        "n1_executor_refuses_without_the_journal",
        not n1.get("succeeded") and out.get("n1_flag") is True
        and "key_continuity_unproven" in json.dumps(n1),
        f"restore_batch sans journal -> {n1} flag de C={out.get('n1_flag')!r}"
        f" : sans journal rien ne distingue cette cle d'une cle redonnee ; "
        f"le refus nomme doit rester (temoin : W1 passe par le journal)",
    ))
    n3 = out.get("n3") or {}
    res.append((
        "n3_undo_without_journal_refused_by_name",
        n3.get("ok") == 0 and n3.get("fail") == 1
        and out.get("n3_flag") is False
        and "key_continuity_unproven" in json.dumps(n3),
        f"execute_grouped_undo sans journal_path -> {n3} flag de C="
        f"{out.get('n3_flag')!r} : C porte FAUX comme D ; sans journal "
        f"l'executeur refuse par son nom (temoin : W2 passe par le journal)",
    ))
    n2 = out.get("n2") or {}
    res.append((
        "n2_service_without_journal_refuses_by_name",
        n2.get("ok") == 0 and n2.get("fail") == 1
        and out.get("n2_flag") is True
        and any("journal" in e for e in n2.get("errors") or []),
        f"execute_grouped_restore sans journal_path -> {n2} flag de C="
        f"{out.get('n2_flag')!r} : sans journal, rien d'ecrit et la raison "
        f"nommee",
    ))
    ur = out.get("u1_restore") or {}
    uu = out.get("u1_undo") or {}
    unread = [e for e in (ur.get("errors") or []) + (uu.get("errors") or [])
              if "journal" in e and "rien d'ecrit" in e]
    res.append((
        "u1_unreadable_journal_refused_by_name",
        ur.get("ok") == 0 and ur.get("fail") == 1 and uu.get("ok") == 0
        and uu.get("fail") == 1 and len(unread) == 2
        and out.get("u1_flag") is True and out.get("u1_layer_asked") == 0,
        f"journal illisible -> restauration={ur} annulation={uu} flag de C="
        f"{out.get('u1_flag')!r} couche cherchee {out.get('u1_layer_asked')} "
        f"fois : le journal donne ne peut pas etre lu, chaque couche est "
        f"refusee nommee, rien d'ecrit, la couche n'est pas touchee",
    ))
    conns = {k: out.get(k) for k in ("w1_conns", "w2_conns")}
    two = out.get("c1_two_groups") or {}
    exc = out.get("c1_exception") or {}
    res.append((
        "c1_one_read_only_connection_per_group_closed_on_every_path",
        out.get("w1_conns") == [1, 0] and out.get("w2_conns") == [1, 0]
        and two.get("opened") == 2 and two.get("left_open") == 0
        and (two.get("result") or {}).get("ok") == 1
        and (two.get("result") or {}).get("fail") == 1
        and exc.get("raised") == "layer resolution failed"
        and exc.get("opened") == 1 and exc.get("left_open") == 0,
        f"connexions (ouvertes, restees ouvertes) : {conns} deux groupes="
        f"{two} exception={exc} : une connexion en lecture seule par groupe, "
        f"fermee quel que soit le chemin (une connexion oubliee tient le "
        f"fichier du journal ouvert)",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_workflow_service_journal run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[3]
    for extra in (str(root), str(root.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
