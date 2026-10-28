# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_state_at_t_property - RecoverLand validation runtime.

Invariant: I-REWIND-01 (apres un rewind a la date T, le fichier contient
EXACTEMENT l'etat qu'il avait a T ; deux rewinds a la meme date donnent le
meme etat ; T1 puis T2 puis T1 retombe sur l'etat de T1 ; un rewind ou une
restauration precedents ne sont jamais rejoues comme une action
utilisateur ; les compensations faites avant T font partie de l'etat a T).

Ce que fait le scenario
-----------------------
Test par proprietes, sur la VRAIE pile : un GeoPackage, la vraie capture
(EditSessionTracker + WriteQueue + JournalManager), la vraie restauration
unitaire (RestoreRunner, Mode A) et le vrai rewind (fenetre du journal
`fetch_events_after_cutoff`, dedup `collapse_rewind_events_with_stats`,
application par `StrictRestoreRunner` en un seul passage). A cote, un
modele de reference en Python pur : un dict code -> (nom, val, x, y), et
l'etat du modele apres CHAQUE action, date. L'etat attendu a la date T est
le dernier etat du modele date avant T : ce que l'utilisateur voyait a T,
restaurations et rewinds precedents compris.

Pour plusieurs graines (journalisees), une suite aleatoire d'actions :
INSERT, UPDATE (nom, val, geometrie, ou plusieurs), DELETE, RECREATE
(suppression de l'entite au plus grand fid puis creation d'une autre),
RESTORE (Mode A) d'un seul evenement, REWIND vers une date passee tiree au
hasard (parfois une date deja utilisee). Apres chaque rewind, le fichier
doit etre egal au modele a T (multiensemble de tuples : un doublon est un
echec, un identifiant qui change n'en est pas un).

Le rewind est pilote comme le moteur doit l'etre : la fenetre calculee du
journal seul, appliquee en UN passage chronologique. Le dialogue d'origine
defait d'abord le rewind precedent (auto-undo) puis recompense : c'est un
defaut du dialogue, rapporte a part, que ce scenario ne reproduit pas.

Echecs concrets pour l'utilisateur (avant correctif)
----------------------------------------------------
- une restauration unitaire d'UNE edition neutralise les autres editions
  de l'entite : le rewind suivant laisse des valeurs posterieures a T ;
- un rewind vers une date posterieure a un rewind precedent ne remet pas
  les entites dans l'etat du moment (le saut en avant n'est pas dans la
  fenetre, ou defait une compensation anterieure a T) ;
- une chaine UPDATE puis DELETE fusionnee laisse une trace qui ne dit pas
  ce qu'elle couvre.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import random
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_state_at_t_property"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
# Seeds are logged with every failure. RL_PROP_SEEDS="1,2,3" and
# RL_PROP_STEPS=40 explore further without editing the file (the budget
# still bounds the run). Seed 318 found the order of a re-applied edit
# (rw_state_at_t_engine_cases, case K): kept early so it always runs.
_SEEDS = tuple(int(s) for s in os.environ.get(
    "RL_PROP_SEEDS", "11,318,23,37,58,71,89").split(",") if s.strip())
_STEPS = int(os.environ.get("RL_PROP_STEPS", "30"))
_BUDGET_S = float(os.environ.get("RL_PROP_BUDGET_S", "80"))
# Rewinds checked against the model, at least, whatever the machine load.
_MIN_REWINDS_CHECKED = int(os.environ.get("RL_PROP_MIN_REWINDS", "25"))
_WORDS = ("PONT", "GARE", "ECOLE", "MAIRIE", "PARC", "QUAI", "HALLE", "TOUR")
_WEIGHTS = (("INSERT", 14), ("UPDATE", 28), ("DELETE", 11), ("RECREATE", 6),
            ("RESTORE", 13), ("REWIND", 28))


def _now():
    return datetime.now(timezone.utc).isoformat()


def _make_gpkg(tmpdir, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(12)&field=nom:string(40)"
        "&field=val:integer", "m", "memory")
    mem.startEditing()
    seed = {}
    for i in range(4):
        code = f"C{i:03d}"
        f = QgsFeature(mem.fields())
        f.setAttributes([code, _WORDS[i], i * 10])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(10.0 * i, 5.0 * i)))
        mem.addFeature(f)
        seed[code] = (_WORDS[i], i * 10, 10.0 * i, 5.0 * i)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    assert lyr.isValid(), path
    return path, lyr, seed


def _file_state(path):
    """Multiset of the file's features, identity-free."""
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = []
    for f in lyr.getFeatures():
        g = f.geometry()
        pt = g.asPoint() if g is not None and not g.isNull() else None
        xy = (round(pt.x(), 3), round(pt.y(), 3)) if pt is not None else (None, None)
        val = f["val"]
        out.append((f["code"], f["nom"], int(val) if val is not None else None) + xy)
    return sorted(out, key=repr)


def _model_rows(model):
    return sorted(((c,) + tuple(v[:2]) + (round(v[2], 3), round(v[3], 3))
                   for c, v in model.items()), key=repr)


class _Run:
    """One seed: the real stack, the reference model, the action log."""

    def __init__(self, ctx, seed, tmp):
        from qgis.core import QgsProject
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        from recoverland.core.identity import compute_datasource_fingerprint
        self.ctx = ctx
        self.seed = seed
        self.rng = random.Random(seed)
        self.dir = os.path.join(tmp, f"seed{seed}")
        os.makedirs(self.dir, exist_ok=True)
        self.path, self.layer, seed_state = _make_gpkg(self.dir, f"lyr{seed}")
        QgsProject.instance().addMapLayer(self.layer)
        project = os.path.join(self.dir, "p.qgz")
        open(project, "w").close()
        self.jm = JournalManager()
        self.jm.open_for_project(project, self.dir)
        self.wq = WriteQueue()
        self.wq.start(self.jm.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()
        self.tracker.connect_layer(self.layer)
        self.fp = compute_datasource_fingerprint(self.layer)
        self.model = dict(seed_state)
        self.history = []          # [(t_gap, model copy, label)]
        self.edits = []            # restore candidates
        self.log = []
        self.n_code = 0
        self.last_eid = 0
        self.checked = 0
        self.reused = 0
        self.restores = 0
        self.used_t = []
        self.failure = None
        self._gap("seed")

    # -- infrastructure -------------------------------------------------
    def close(self):
        from qgis.core import QgsProject
        try:
            self.tracker.deactivate()
            self.tracker.disconnect_all()
        finally:
            self.wq.stop()
            self.jm.close()
            QgsProject.instance().removeMapLayer(self.layer.id())

    def _drain(self):
        from scripts.validation.plugin_harness import pump
        pump(0.03)
        if hasattr(self.wq, "wait_until_drained"):
            self.wq.wait_until_drained(10.0)
        else:
            deadline = time.monotonic() + 10
            while not self.wq._queue.empty() and time.monotonic() < deadline:
                time.sleep(0.02)
            time.sleep(0.3)
        pump(0.01)

    def _gap(self, label):
        time.sleep(0.012)
        stamp = _now()
        time.sleep(0.012)
        self.history.append((stamp, dict(self.model), label))

    def _rows(self, sql, params=()):
        conn = sqlite3.connect(f"file:{Path(self.jm.path).as_posix()}?mode=ro",
                               uri=True)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def _new_user_eids(self):
        rows = self._rows(
            "SELECT event_id FROM audit_event WHERE event_id > ? "
            "AND restored_from_event_id IS NULL ORDER BY event_id",
            (self.last_eid,))
        top = self._rows("SELECT MAX(event_id) FROM audit_event")[0][0] or 0
        self.last_eid = max(self.last_eid, top)
        return [r[0] for r in rows]

    def _fid_of(self, code):
        for f in self.layer.getFeatures():
            if f["code"] == code:
                return f.id()
        return None

    def _commit(self, fn):
        lyr = self.layer
        lyr.startEditing()
        fn()
        ok = bool(lyr.commitChanges())
        if not ok:
            lyr.rollBack()
        self._drain()
        return ok

    def _fail(self, kind, **detail):
        if self.failure is None:
            self.failure = dict(kind=kind, seed=self.seed, step=len(self.log),
                                log=self.log[-12:], **detail)

    # -- user actions ---------------------------------------------------
    def _set_feature(self, fid, nom=None, val=None, xy=None):
        from qgis.core import QgsGeometry, QgsPointXY
        idx = self.layer.fields().indexFromName
        if nom is not None:
            self.layer.changeAttributeValue(fid, idx("nom"), nom)
        if val is not None:
            self.layer.changeAttributeValue(fid, idx("val"), val)
        if xy is not None:
            self.layer.changeGeometry(fid, QgsGeometry.fromPointXY(QgsPointXY(*xy)))

    def _record_edit(self, op, code, fields, before, after, label):
        eids = self._new_user_eids()
        self.log.append(f"{label} eids={eids}")
        if len(eids) != 1:
            self._fail("capture", op=op, code=code, eids=eids)
            return
        self.edits.append(dict(op=op, code=code, eid=eids[0], fields=fields,
                               before=before, after=after, ok=True))
        self._gap(label)

    def act_insert(self):
        from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
        self.n_code += 1
        code = f"N{self.seed % 100:02d}{self.n_code:03d}"
        nom = self.rng.choice(_WORDS)
        val = self.rng.randint(0, 99)
        xy = (float(self.rng.randint(0, 400)), float(self.rng.randint(0, 400)))

        def _add():
            f = QgsFeature(self.layer.fields())
            idx = self.layer.fields().indexFromName
            f.setAttribute(idx("code"), code)
            f.setAttribute(idx("nom"), nom)
            f.setAttribute(idx("val"), val)
            f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*xy)))
            self.layer.addFeature(f)
        if not self._commit(_add):
            return self._fail("commit", op="INSERT", code=code)
        self.model[code] = (nom, val) + xy
        self._record_edit("INSERT", code, None, None, self.model[code],
                          f"INSERT {code}")

    def act_update(self, code=None, kind=None):
        if not self.model:
            return self.act_insert()
        code = code or self.rng.choice(sorted(self.model))
        nom, val, x, y = self.model[code]
        kind = kind or self.rng.choice(("nom", "val", "geom", "nom+val", "all"))
        new = dict(nom=nom, val=val, xy=(x, y))
        fields = set()
        if "nom" in kind or kind == "all":
            new["nom"] = self.rng.choice(_WORDS) + str(self.rng.randint(0, 9))
            fields.add("nom")
        if "val" in kind or kind == "all":
            new["val"] = self.rng.randint(100, 999)
            fields.add("val")
        if kind in ("geom", "all"):
            new["xy"] = (float(self.rng.randint(0, 400)),
                         float(self.rng.randint(0, 400)))
            fields.add("geom")
        fid = self._fid_of(code)
        if not self._commit(lambda: self._set_feature(
                fid, nom=new["nom"] if "nom" in fields else None,
                val=new["val"] if "val" in fields else None,
                xy=new["xy"] if "geom" in fields else None)):
            return self._fail("commit", op="UPDATE", code=code)
        before = self.model[code]
        self.model[code] = (new["nom"], new["val"]) + tuple(new["xy"])
        self._record_edit("UPDATE", code, fields, before, self.model[code],
                          f"UPDATE {code} {sorted(fields)}")

    def act_delete(self, code=None):
        if not self.model:
            return self.act_insert()
        code = code or self.rng.choice(sorted(self.model))
        fid = self._fid_of(code)
        if not self._commit(lambda: self.layer.deleteFeature(fid)):
            return self._fail("commit", op="DELETE", code=code)
        before = self.model.pop(code)
        self._record_edit("DELETE", code, None, before, None,
                          f"DELETE {code} fid={fid}")

    def act_recreate(self):
        fids = {f.id(): f["code"] for f in self.layer.getFeatures()}
        if not fids:
            return self.act_insert()
        self.act_delete(fids[max(fids)])
        if self.failure is None:
            self.act_insert()

    # -- restore (Mode A) -----------------------------------------------
    def _restorable(self):
        out = []
        for i, e in enumerate(self.edits):
            if not e["ok"]:
                continue
            later = [x for x in self.edits[i + 1:] if x["code"] == e["code"]]
            if e["op"] == "UPDATE":
                if any(x["op"] != "UPDATE" or x["fields"] & e["fields"]
                       for x in later):
                    continue
            elif later:
                continue
            out.append(e)
        return out

    def _freeze_edits(self):
        for e in self.edits:
            e["ok"] = False

    def act_restore(self):
        import recoverland.restore_runner as rr
        from recoverland.core.event_stream_repository import fetch_events_by_ids
        from scripts.validation.plugin_harness import pump
        cands = self._restorable()
        if not cands:
            return self.act_update()
        e = self.rng.choice(cands)
        conn = self.jm.create_read_connection()
        try:
            events = fetch_events_by_ids(conn, [e["eid"]])
        finally:
            conn.close()
        done = {}
        runner = rr.RestoreRunner(
            events, lambda ev: self.layer if ev.datasource_fingerprint == self.fp else None,
            self.wq, tracker=self.tracker, trace_id=self.ctx.trace_id)
        runner.finished.connect(lambda res: done.setdefault("r", res))
        runner.start()
        deadline = time.monotonic() + 20
        while "r" not in done and time.monotonic() < deadline:
            pump(0.02)
        self._drain()
        res = done.get("r")
        code = e["code"]
        if e["op"] == "UPDATE":
            nom, val, x, y = self.model[code]
            bn, bv, bx, by = e["before"]
            if "nom" in e["fields"]:
                nom = bn
            if "val" in e["fields"]:
                val = bv
            if "geom" in e["fields"]:
                x, y = bx, by
            self.model[code] = (nom, val, x, y)
        elif e["op"] == "DELETE":
            self.model[code] = e["before"]
        else:
            self.model.pop(code, None)
        self._freeze_edits()
        self.restores += 1
        self.log.append(f"RESTORE eid={e['eid']} {e['op']} {code}")
        actual = _file_state(self.path)
        if (res is None or getattr(res, "total_fail", 1) != 0
                or actual != _model_rows(self.model)):
            self._fail("restore", eid=e["eid"], op=e["op"], code=code,
                       expected=_model_rows(self.model), actual=actual,
                       total_fail=getattr(res, "total_fail", None),
                       errors=list(getattr(res, "errors", []) or [])[:4])
            return
        self._gap(f"RESTORE {e['eid']}")

    # -- rewind -----------------------------------------------------------
    def act_rewind(self, idx=None):
        import recoverland.restore_runner as rr
        from recoverland.core.event_stream_repository import fetch_events_after_cutoff
        from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
        from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
        from scripts.validation.plugin_harness import pump
        if idx is not None:
            pass
        elif self.used_t and self.rng.random() < 0.3:
            idx = self.rng.choice(self.used_t)
            self.reused += 1
        else:
            idx = self.rng.randrange(len(self.history))
        self.used_t.append(idx)
        stamp, expected_model, label = self.history[idx]
        cutoff = RestoreCutoff(CutoffType.BY_DATE, stamp, inclusive=True)
        self._drain()
        conn = self.jm.create_read_connection()
        try:
            events = fetch_events_after_cutoff(conn, self.fp, cutoff,
                                               trace_id=self.ctx.trace_id)
        finally:
            conn.close()
        events.sort(key=lambda ev: (ev.created_at or "", ev.event_id or 0),
                    reverse=True)
        active, stats = collapse_rewind_events_with_stats(events)
        plan = [(ev.event_id, ev.operation_type,
                 (getattr(ev, "commit_id", None) or "user").split(":")[-1].split("@")[0])
                for ev in active]
        res = None
        if active:
            done = {}
            runner = rr.StrictRestoreRunner(
                active, lambda ev: self.layer if ev.datasource_fingerprint == self.fp else None,
                cutoff, write_queue=self.wq, tracker=self.tracker,
                trace_id=self.ctx.trace_id)
            runner.finished.connect(lambda r: done.setdefault("r", r))
            runner.start()
            deadline = time.monotonic() + 30
            while "r" not in done and time.monotonic() < deadline:
                pump(0.02)
            res = done.get("r")
            self._drain()
        self.model = dict(expected_model)
        self._freeze_edits()
        self.checked += 1
        self.log.append(f"REWIND to #{idx} ({label}) plan={plan}")
        actual = _file_state(self.path)
        expected = _model_rows(self.model)
        if actual != expected:
            self._fail("state", t_index=idx, t_label=label, expected=expected,
                       actual=actual, plan=plan, stats=stats,
                       errors=list(getattr(res, "errors", []) or [])[:4])
            return
        if res is not None and getattr(res, "total_fail", 0):
            self._fail("report", t_index=idx, plan=plan,
                       total_fail=res.total_fail,
                       errors=list(getattr(res, "errors", []) or [])[:4])
            return
        self._gap(f"REWIND #{idx}")

    def prologue(self):
        """Fixed shapes the random walk may miss, on the real file.

        An UPDATE then a DELETE of the same feature, and a chain of twelve
        UPDATEs of one feature, both rewound: schema 7 compensates every
        event on its own (no fused synthetic event), the re-insert first
        and the UPDATEs on the re-inserted feature next. Then the forward
        hop into the middle of each chain, and back.
        """
        base = len(self.history) - 1
        self.act_update("C001", "all")
        mid = len(self.history) - 1
        self.act_delete("C001")
        self.act_rewind(base)
        self.act_rewind(mid)
        self.act_rewind(base)
        start = len(self.history) - 1
        for i in range(12):
            if self.failure:
                return
            self.act_update("C002", ("nom", "val", "geom", "all")[i % 4])
        self.act_rewind(start)
        self.act_rewind(start + 6)
        self.act_rewind(len(self.history) - 1)

    def step(self):
        ops = [op for op, _w in _WEIGHTS]
        weights = [w for _op, w in _WEIGHTS]
        op = self.rng.choices(ops, weights=weights)[0]
        getattr(self, "act_" + op.lower())()


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_prop_")
    flog(f"rw_state_at_t_property setup: trace_id={ctx.trace_id} "
         f"seeds={_SEEDS} steps={_STEPS}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    started = time.monotonic()
    results = []
    for seed in _SEEDS:
        if time.monotonic() - started > _BUDGET_S:
            break
        r = None
        try:
            r = _Run(ctx, seed, ctx.data["tmpdir"])
            if seed == _SEEDS[0]:
                r.prologue()
            for _ in range(_STEPS):
                if r.failure or time.monotonic() - started > _BUDGET_S:
                    break
                r.step()
        except Exception as exc:  # noqa: BLE001 - reported by the assertions
            import traceback
            if r is not None:
                r._fail("crash", error=f"{type(exc).__name__}: {exc}",
                        trace=traceback.format_exc()[-1500:])
            else:
                results.append(dict(seed=seed, checked=0, reused=0, restores=0,
                                    failure=dict(kind="crash", error=repr(exc),
                                                 trace=traceback.format_exc()[-1500:])))
        finally:
            if r is not None:
                results.append(dict(seed=seed, checked=r.checked, reused=r.reused,
                                    restores=r.restores, failure=r.failure,
                                    steps=len(r.log)))
                flog(f"rw_state_at_t_property seed={seed} trace_id={ctx.trace_id} "
                     f"steps={len(r.log)} rewinds={r.checked} restores={r.restores} "
                     f"failure={json.dumps(r.failure, default=str)[:1500]} "
                     f"log={r.log}", "INFO" if r.failure is None else "ERROR")
                r.close()
    ctx.data["results"] = results
    ctx.data["elapsed_s"] = round(time.monotonic() - started, 1)
    QgsProject.instance().clear()
    shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)
    flog(f"rw_state_at_t_property run: trace_id={ctx.trace_id} "
         f"elapsed_s={ctx.data['elapsed_s']} "
         f"seeds={[r['seed'] for r in results]}", "INFO")


def _first(results, kinds):
    for r in results:
        f = r.get("failure")
        if f and f.get("kind") in kinds:
            return f
    return None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    results = ctx.data.get("results") or []
    checked = sum(r.get("checked", 0) for r in results)
    out = []
    crash = _first(results, ("crash", "commit", "capture"))
    # The time budget bounds the run (under ~90 s on a loaded machine): the
    # seeds it could not start are named, not hidden. A floor on what WAS
    # verified keeps a harness that does nothing from passing.
    skipped = [s for s in _SEEDS if s not in {r["seed"] for r in results}]
    out.append((
        "harness_ran_enough_seeds",
        crash is None and len(results) >= min(3, len(_SEEDS))
        and checked >= _MIN_REWINDS_CHECKED,
        f"graines={[(r['seed'], r.get('checked'), r.get('steps')) for r in results]} "
        f"non lancees (budget {_BUDGET_S:.0f} s)={skipped} rewinds verifies="
        f"{checked} (minimum {_MIN_REWINDS_CHECKED}) elapsed="
        f"{ctx.data.get('elapsed_s')}s incident={json.dumps(crash, default=str)[:1500]}",
    ))
    state = _first(results, ("state",))
    out.append((
        "file_equals_state_at_t_after_every_rewind",
        state is None,
        f"premier ecart : {json.dumps(state, default=str)[:3000]} -- apres ce "
        f"rewind, l'utilisateur n'a pas la couche qu'il avait a la date "
        f"choisie (valeurs posterieures restees, entites en double ou "
        f"supprimees a tort)",
    ))
    report = _first(results, ("report",))
    out.append((
        "rewind_reports_no_failure",
        report is None,
        f"{json.dumps(report, default=str)[:1500]} : un rewind qui atteint "
        f"l'etat doit le dire sans echec ; un echec annonce masque l'etat reel",
    ))
    restore = _first(results, ("restore",))
    out.append((
        "mode_a_restore_matches_the_model",
        restore is None and sum(r.get("restores", 0) for r in results) >= 1,
        f"restaurations={[r.get('restores') for r in results]} ecart="
        f"{json.dumps(restore, default=str)[:1500]}",
    ))
    out.append((
        "same_date_replayed_at_least_once",
        sum(r.get("reused", 0) for r in results) >= 1,
        f"dates rejouees={[r.get('reused') for r in results]} : rejouer une "
        f"date deja choisie doit rendre le meme etat",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_state_at_t_property.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
