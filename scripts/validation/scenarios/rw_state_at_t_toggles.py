# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_state_at_t_toggles - RecoverLand validation runtime.

Invariant: I-REWIND-01 (apres un rewind a la date T, le fichier contient
EXACTEMENT l'etat qu'il avait a T), sur un GeoPackage ET sur un shapefile,
quand une meme edition anterieure a T a ete compensee PLUSIEURS fois
apres T, ou quand une entite re-inseree par un rewind a ete re-editee.

Le mecanisme
------------
1. Saut en avant a travers une entite re-inseree (shapefile).
   Un rewind re-insere l'entite C001 supprimee par l'evenement D : sur un
   shapefile elle recoit un NOUVEAU fid (fid:3), alors que la trace du
   rewind garde l'identite de D (fid:1). L'utilisateur supprime a nouveau
   C001 (evenement utilisateur sur fid:3), puis demande un rewind a une
   date situee entre D et la re-insertion : C001 doit etre ABSENTE (elle
   l'est a T et maintenant). Le plan contient deux lignes pour une seule
   entite logique, sous deux cles : la re-insertion qui compense la
   suppression de fid:3, et la suppression (REAPPLY) de la re-insertion
   faite par le rewind, cle fid:1. Elles ne tombent pas dans le meme
   seau, ne s'annulent pas ; l'executeur re-insere C001 puis refuse de
   supprimer une entite qu'il vient de re-inserer ("deja absente").
   Resultat : C001 ressuscitee, rewind annonce reussi. Sur GeoPackage le
   fid est reinsere comme cle : les deux lignes partagent la cle et
   s'annulent, le defaut ne se voit pas.

2. Double bascule d'une edition anterieure a T.
   U (val 10 -> 872) avant T ; restauration Mode A de U ; l'utilisateur
   retape 872 ; nouvelle restauration Mode A de U ; rewind juste apres U.
   Toutes les bascules de U sont regroupees en UN seul REAPPLY place a la
   DERNIERE bascule : la premiere bascule n'est jamais defaite apres la
   compensation de l'edition intermediaire, et le champ finit a 10 au lieu
   de 872, toutes les preuves passant et le succes annonce. La variante
   DELETE (supprimer, restaurer, supprimer a nouveau, restaurer) finit
   avec l'entite ressuscitee.

   Depuis v7, avec le journal, la deuxieme restauration Mode A est
   refusee par son nom (already_compensated, rs_mode_a_double_restore) :
   rien d'ecrit. Mais les journaux a double bascule existent : les
   versions precedentes les ecrivaient (leur RestoreRunner ne donnait pas
   le journal a l'executeur), et le rewind doit toujours les lire juste.
   Le scenario verifie donc les deux, separement : la deuxieme
   restauration, journal fourni, est refusee par son nom, fichier intact
   (assertion a part) ; puis il construit le journal a double bascule
   expres, en rejouant cette restauration comme une version precedente,
   RestoreRunner prive de journal (file d'ecriture sans chemin, qui
   journalise sa trace dans le vrai journal). Les rewinds qui suivent
   lisent ce journal-la et doivent retomber sur l'etat a T.

Ce que le scenario execute
--------------------------
La vraie pile de rw_state_at_t_property (_Run : capture reelle,
RestoreRunner Mode A reel, fenetre du journal, dedup, StrictRestoreRunner
en un passage), sur un GeoPackage puis sur un shapefile :
  - formes fixes p1 (supprimer, rewind avant, supprimer encore, rewind
    entre les deux), p2 (idem avec une modification entre), p5 (double
    restauration d'un UPDATE avec re-saisie), p6 (double restauration d'un
    DELETE avec re-suppression), chacune sur une couche neuve ;
  - une marche aleatoire elargie : l'action RETOGGLE fait la double
    restauration (UPDATE ou DELETE) au milieu des autres actions, puis
    des rewinds a des dates tirees au hasard (avant, entre, apres).
La deuxieme restauration de p5, p6 et de chaque RETOGGLE : refus nomme
already_compensated, fichier intact, puis la meme restauration sans
journal, ecrite, et DEUX traces sur l'evenement dans le journal.
Apres chaque rewind le fichier doit egaler le modele a T (multiensemble,
identifiants ignores). Un refus NOMME (preflight bloque avec
"unrebuildable_state", fichier intact) est compte a part et affiche : il
n'est tolere que dans la marche aleatoire, jamais sur les formes fixes.

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
p1/p2 sur shapefile : l'entite supprimee revient apres le rewind, rapport
sans echec. p5 : la valeur du champ est celle d'avant l'edition, pas celle
de la date choisie, rapport sans echec. Deuxieme restauration ecrite alors
que le journal dit l'evenement deja restaure : la re-saisie de
l'utilisateur est defaite sous une trace qui nomme l'evenement d'avant.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rw_state_at_t_toggles"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_SEEDS = tuple(int(s) for s in os.environ.get(
    "RL_TOGGLE_SEEDS", "5,17,29").split(",") if s.strip())
_STEPS = int(os.environ.get("RL_TOGGLE_STEPS", "24"))
_BUDGET_S = float(os.environ.get("RL_TOGGLE_BUDGET_S", "90"))
_PROVIDERS = tuple(p for p in os.environ.get(
    "RL_TOGGLE_PROVIDERS", "gpkg,shp").split(",") if p in ("gpkg", "shp"))
# Exploration only: a LOUD failure (errors reported) ends a GeoPackage walk
# too instead of failing it, to keep looking for SILENT ones (the default
# run keeps GeoPackage strict).
_TOLERATE_LOUD = os.environ.get("RL_TOGGLE_TOLERATE_LOUD", "") == "1"
_WEIGHTS = (("INSERT", 10), ("UPDATE", 24), ("DELETE", 9), ("RECREATE", 4),
            ("RESTORE", 9), ("RETOGGLE", 12), ("REWIND", 32))


def _make_shp(tmpdir, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    from scripts.validation.scenarios import rw_state_at_t_property as prop
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(12)&field=nom:string(40)"
        "&field=val:integer", "m", "memory")
    mem.startEditing()
    seed = {}
    for i in range(4):
        code = f"C{i:03d}"
        f = QgsFeature(mem.fields())
        f.setAttributes([code, prop._WORDS[i], i * 10])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(10.0 * i, 5.0 * i)))
        mem.addFeature(f)
        seed[code] = (prop._WORDS[i], i * 10, 10.0 * i, 5.0 * i)
    mem.commitChanges()
    path = os.path.join(tmpdir, "pts.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(path, name, "ogr")
    assert lyr.isValid(), path
    return path, lyr, seed


def _new_run(ctx, seed, tmp, provider):
    """A _Run of the property harness on *provider*.

    The harness builds its layer through the module-level `_make_gpkg`:
    swapped for the shapefile writer only while the run is built, then put
    back, so no other scenario of the process inherits the swap.
    """
    from scripts.validation.scenarios import rw_state_at_t_property as prop
    original = prop._make_gpkg
    if provider == "shp":
        prop._make_gpkg = _make_shp
    try:
        return _ToggleRun(ctx, seed, os.path.join(tmp, provider))
    finally:
        prop._make_gpkg = original


class _LegacyQueue:
    """The write queue as a RestoreRunner of a version before v7 used it.

    No `journal_path`: the runner then hands its executor no journal, as
    those versions did, and a second restore of an event already
    compensated is written. Its traces still go to the journal of the run,
    through the real queue (the running writer's journal).
    """

    journal_path = None

    def __init__(self, wq):
        self._wq = wq

    def enqueue(self, events, commit=None, journal_path=None):
        return self._wq.enqueue(events, commit=commit)


def _toggle_run_class():
    from scripts.validation.scenarios.rw_state_at_t_property import _Run

    class ToggleRun(_Run):
        """_Run plus the re-toggle action and a tolerated NAMED refusal."""

        refused = 0
        refusals: list = []
        provider = "gpkg"
        loud: list = []
        stopped = False

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # One record per second restore (p5, p6, RETOGGLE): what the
            # restore did with the journal, then the legacy write.
            self.second_restores = []

        def _run_restore(self, events, legacy=False):
            """The real RestoreRunner on *events*; its result, queue drained."""
            import recoverland.restore_runner as rr
            from scripts.validation.plugin_harness import pump
            done = {}
            queue = _LegacyQueue(self.wq) if legacy else self.wq
            runner = rr.RestoreRunner(
                events,
                lambda ev: self.layer if ev.datasource_fingerprint == self.fp else None,
                queue, tracker=self.tracker, trace_id=self.ctx.trace_id)
            runner.finished.connect(lambda res: done.setdefault("r", res))
            runner.start()
            deadline = time.monotonic() + 20
            while "r" not in done and time.monotonic() < deadline:
                pump(0.02)
            self._drain()
            return done.get("r")

        def _restore_edit(self, e, label, again=False):
            """Mode A restore of the journal event of *e*, checked.

            *again*: *e* was restored already and the user has redone its
            edit since (p5, p6, RETOGGLE).
            """
            from recoverland.core.event_stream_repository import fetch_events_by_ids
            from scripts.validation.scenarios.rw_state_at_t_property import (
                _file_state, _model_rows)
            conn = self.jm.create_read_connection()
            try:
                events = fetch_events_by_ids(conn, [e["eid"]])
            finally:
                conn.close()
            res = self._run_restore(events)
            code = e["code"]
            record = None
            if again:
                # Hypothesis revised (v7): a second Mode A restore of an
                # event that a trace in effect already compensates is
                # refused by name by the executor (`already_compensated`,
                # rs_mode_a_double_restore) now that RestoreRunner hands it
                # the journal: it used to write, undoing the later edit
                # under a trace naming the first event. That refusal is
                # recorded and asserted on its own
                # (`*_second_restore_refused_by_name`, and the walk's). The
                # journal with two traces in effect on one event still
                # exists (versions before v7 wrote it), and the rewinds
                # must read it right: it is rebuilt on purpose, the same
                # restore replayed as those versions ran it (no journal,
                # `_LegacyQueue`), and every rewind that follows is checked
                # on it as before. Stronger than the previous revision,
                # which kept the refused journal (one toggle) and so no
                # longer tested the double toggle at all.
                errors = list(getattr(res, "errors", []) or [])
                by_name = (res is not None
                           and getattr(res, "total_fail", 0) == 1
                           and len(errors) == 1
                           and "(already_compensated)" in errors[0])
                actual = _file_state(self.path)
                unchanged = actual == _model_rows(self.model)
                if by_name:
                    outcome = "refused_by_name"
                elif not unchanged:
                    outcome = "written"
                elif res is not None and getattr(res, "total_fail", 1) == 0:
                    outcome = "skipped"
                else:
                    outcome = "refused_other"
                record = dict(label=label, eid=e["eid"], op=e["op"], code=code,
                              provider=self.provider, outcome=outcome,
                              file_unchanged=unchanged, errors=errors[:2])
                self.second_restores.append(record)
                if by_name and not unchanged:
                    self._fail("restore", eid=e["eid"], op=e["op"], code=code,
                               expected=_model_rows(self.model), actual=actual,
                               total_fail=1, errors=errors[:4],
                               refused_but_written=True)
                    return
                if by_name:
                    self.log.append(f"{label} eid={e['eid']} {e['op']} {code} "
                                    f"refused by name (already_compensated), "
                                    f"nothing written")
                    res = self._run_restore(events, legacy=True)
                    label = f"{label} (no journal, as before v7)"
                # Otherwise the checks below judge what it did: a write
                # lands in the model (and fails the refusal assertion), any
                # other failure fails the walk as before.
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
            self.restores += 1
            self.log.append(f"{label} eid={e['eid']} {e['op']} {code}")
            actual = _file_state(self.path)
            if (res is None or getattr(res, "total_fail", 1) != 0
                    or actual != _model_rows(self.model)):
                self._fail("restore", eid=e["eid"], op=e["op"], code=code,
                           expected=_model_rows(self.model), actual=actual,
                           total_fail=getattr(res, "total_fail", None),
                           errors=list(getattr(res, "errors", []) or [])[:4])
                return
            if record is not None and record["outcome"] == "refused_by_name":
                # The journal the rewinds now read: two traces on the event.
                record["traces_on_event"] = self._rows(
                    "SELECT COUNT(*) FROM audit_event WHERE "
                    "restored_from_event_id = ?", (e["eid"],))[0][0]
            self._gap(f"{label} {e['eid']}")

        def _record_edit(self, op, code, fields, before, after, label):
            # A random UPDATE may draw the values the feature already has:
            # QGIS commits nothing and no event exists. Not a capture loss.
            if op == "UPDATE" and before == after:
                self._new_user_eids()
                self.log.append(f"{label} (same values, nothing to capture)")
                self._gap(label)
                return
            super()._record_edit(op, code, fields, before, after, label)

        def _retype(self, u):
            """The user types the NEW values of *u* again (same fields)."""
            code = u["code"]
            fid = self._fid_of(code)
            an, av, ax, ay = u["after"]
            fields = u["fields"]
            if not self._commit(lambda: self._set_feature(
                    fid, nom=an if "nom" in fields else None,
                    val=av if "val" in fields else None,
                    xy=(ax, ay) if "geom" in fields else None)):
                return self._fail("commit", op="RETYPE", code=code)
            before = self.model[code]
            nom, val, x, y = before
            if "nom" in fields:
                nom = an
            if "val" in fields:
                val = av
            if "geom" in fields:
                x, y = ax, ay
            self.model[code] = (nom, val, x, y)
            self._record_edit("UPDATE", code, set(fields), before,
                              self.model[code], f"RETYPE {code} {sorted(fields)}")

        def retoggle_update(self, code=None, kind=None):
            n = len(self.edits)
            self.act_update(code, kind)
            if self.failure or len(self.edits) == n:
                return  # nothing recorded (same values drawn): no U
            u = self.edits[-1]
            self._restore_edit(u, "RESTORE U")
            if self.failure:
                return
            self._retype(u)
            if self.failure:
                return
            self._restore_edit(u, "RESTORE U again", again=True)
            self._freeze_edits()

        def retoggle_delete(self, code=None):
            if not self.model:
                return self.act_insert()
            code = code or self.rng.choice(sorted(self.model))
            n = len(self.edits)
            self.act_delete(code)
            if self.failure or len(self.edits) == n:
                return
            d = self.edits[-1]
            self._restore_edit(d, "RESTORE D")
            if self.failure:
                return
            self.act_delete(code)
            if self.failure:
                return
            self._restore_edit(d, "RESTORE D again", again=True)
            self._freeze_edits()

        def act_retoggle(self):
            if not self.model:
                return self.act_insert()
            if self.rng.random() < 0.6:
                self.retoggle_update(kind=self.rng.choice(
                    ("nom", "val", "geom", "all")))
            else:
                self.retoggle_delete()

        def act_rewind(self, idx=None):
            """The harness rewind; a NAMED refusal that wrote nothing is
            counted (``refused``) instead of failing the walk.

            On a shapefile a repack renumbers the records after every
            delete: the executor then refuses, out loud, an UPDATE whose
            FID now holds another feature (fail-safe, the executor's
            identity proof, not the rewind plan). Such a LOUD failure
            (errors reported) ends the walk and is listed (``loud``); a
            wrong state reported without error still fails."""
            from scripts.validation.scenarios.rw_state_at_t_property import (
                _file_state)
            before = _file_state(self.path)
            model_before = dict(self.model)
            super().act_rewind(idx)
            f = self.failure
            if not self.allow_refusal or f is None:
                return
            errors = " ".join(str(x) for x in (f.get("errors") or []))
            if (f.get("kind") == "state" and "unrebuildable_state" in errors
                    and f.get("actual") == before):
                self.failure = None
                self.model = model_before
                self.refused += 1
                self.refusals.append(errors[:300])
                self.log.append(f"REWIND refused by name: {errors[:200]}")
                self._gap("REWIND refused")
                return
            if ((self.provider == "shp" or _TOLERATE_LOUD)
                    and f.get("kind") in ("state", "report") and errors):
                self.failure = None
                self.loud.append(errors[:300])
                self.stopped = True
                self.log.append(f"REWIND failed out loud (shapefile): {errors[:200]}")

        def step(self):
            ops = [op for op, _w in _WEIGHTS]
            weights = [w for _op, w in _WEIGHTS]
            op = self.rng.choices(ops, weights=weights)[0]
            getattr(self, "act_" + op.lower())()
            f = self.failure
            if (f is not None and (self.provider == "shp" or _TOLERATE_LOUD)
                    and f.get("kind") == "restore" and f.get("errors")):
                # A Mode A restore refused out loud on a renumbered record:
                # same rule as the rewind above.
                self.failure = None
                self.loud.append(" ".join(map(str, f["errors"]))[:300])
                self.stopped = True

    return ToggleRun


_ToggleRun = None


def _p1(r):
    seed = len(r.history) - 1
    r.act_delete("C001")
    after_del = len(r.history) - 1
    r.act_rewind(seed)
    r.act_delete("C001")
    r.act_rewind(after_del)
    r.act_rewind(seed)
    r.act_rewind(len(r.history) - 1)


def _p2(r):
    seed = len(r.history) - 1
    r.act_delete("C001")
    after_del = len(r.history) - 1
    r.act_rewind(seed)
    r.act_update("C001", "val")
    r.act_delete("C001")
    r.act_rewind(after_del)
    r.act_rewind(seed)


def _p5(r):
    base = len(r.history) - 1
    r.act_update("C001", "val")
    t_idx = len(r.history) - 1
    u = r.edits[-1]
    r._restore_edit(u, "RESTORE U")
    r._retype(u)
    r._restore_edit(u, "RESTORE U again", again=True)
    r.act_rewind(t_idx)
    r.act_rewind(base)
    r.act_rewind(t_idx)


def _p6(r):
    base = len(r.history) - 1
    r.act_delete("C001")
    t_idx = len(r.history) - 1
    d = r.edits[-1]
    r._restore_edit(d, "RESTORE D")
    r.act_delete("C001")
    r._restore_edit(d, "RESTORE D again", again=True)
    r.act_rewind(t_idx)
    r.act_rewind(base)
    r.act_rewind(t_idx)


_SHAPES = (("p1_delete_again", _p1), ("p2_update_delete_again", _p2),
           ("p5_double_mode_a_update", _p5), ("p6_double_mode_a_delete", _p6))
# The shapes that restore one event twice (a second restore, see
# ToggleRun._restore_edit).
_SECOND_RESTORE_SHAPES = ("p5_double_mode_a_update", "p6_double_mode_a_delete")


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_tog_")
    flog(f"rw_state_at_t_toggles setup: trace_id={ctx.trace_id} "
         f"seeds={_SEEDS} steps={_STEPS}", "INFO")


def _run_one(ctx, seed, provider, fn, allow_refusal):
    r = None
    out = {}
    try:
        r = _new_run(ctx, seed, ctx.data["tmpdir"], provider)
        r.allow_refusal = allow_refusal
        r.refusals = []
        r.provider = provider
        r.loud = []
        fn(r)
    except Exception as exc:  # noqa: BLE001 - reported by the assertions
        import traceback
        if r is not None:
            r._fail("crash", error=f"{type(exc).__name__}: {exc}",
                    trace=traceback.format_exc()[-1500:])
        else:
            out = dict(failure=dict(kind="crash", error=repr(exc),
                                    trace=traceback.format_exc()[-1500:]))
    finally:
        if r is not None:
            out = dict(failure=r.failure, checked=r.checked, refused=r.refused,
                       refusals=list(r.refusals)[:3], loud=list(r.loud)[:3],
                       second_restores=list(r.second_restores),
                       steps=len(r.log), log=r.log[-14:])
            r.close()
    return out


def run(ctx):
    global _ToggleRun
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    _ToggleRun = _toggle_run_class()
    started = time.monotonic()
    shapes = {}
    for provider in _PROVIDERS:
        for n, (name, fn) in enumerate(_SHAPES):
            res = _run_one(ctx, 9300 + n, provider, fn, allow_refusal=False)
            shapes[f"{provider}:{name}"] = res
            flog(f"rw_state_at_t_toggles shape={provider}:{name} "
                 f"trace_id={ctx.trace_id} result="
                 f"{json.dumps(res, default=str)[:1500]}",
                 "INFO" if res.get("failure") is None else "ERROR")
    walks = []
    for seed in _SEEDS:
        for provider in _PROVIDERS:
            if time.monotonic() - started > _BUDGET_S:
                break

            def _walk(r):
                for _ in range(_STEPS):
                    if (r.failure or r.stopped
                            or time.monotonic() - started > _BUDGET_S):
                        break
                    r.step()
            res = _run_one(ctx, seed, provider, _walk, allow_refusal=True)
            res.update(seed=seed, provider=provider)
            walks.append(res)
            flog(f"rw_state_at_t_toggles walk seed={seed} provider={provider} "
                 f"trace_id={ctx.trace_id} result="
                 f"{json.dumps(res, default=str)[:1500]}",
                 "INFO" if res.get("failure") is None else "ERROR")
    ctx.data["shapes"] = shapes
    ctx.data["walks"] = walks
    ctx.data["elapsed_s"] = round(time.monotonic() - started, 1)
    QgsProject.instance().clear()
    shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    shapes = ctx.data.get("shapes") or {}
    walks = ctx.data.get("walks") or []
    out = []
    for provider in _PROVIDERS:
        for name, _fn in _SHAPES:
            key = f"{provider}:{name}"
            res = shapes.get(key) or {}
            f = res.get("failure")
            out.append((
                f"{provider}_{name}_lands_on_state_at_t",
                bool(res) and f is None and res.get("checked", 0) >= 2,
                f"{json.dumps(f, default=str)[:2500]} rewinds={res.get('checked')} "
                f": apres ce rewind la couche n'est pas celle de la date choisie "
                f"(entite ressuscitee ou valeur d'avant l'edition) et le rapport "
                f"ne dit aucun echec" if f else f"rewinds verifies={res.get('checked')}",
            ))
    for provider in _PROVIDERS:
        for name in _SECOND_RESTORE_SHAPES:
            recs = (shapes.get(f"{provider}:{name}") or {}).get(
                "second_restores") or []
            first = recs[0] if recs else {}
            out.append((
                f"{provider}_{name}_second_restore_refused_by_name",
                len(recs) == 1 and first.get("outcome") == "refused_by_name"
                and first.get("file_unchanged") is True
                and first.get("traces_on_event") == 2,
                f"deuxieme restauration={json.dumps(recs, default=str)[:1200]}"
                f" : journal fourni, elle doit etre refusee par son nom "
                f"(already_compensated), fichier intact, sinon elle defait la "
                f"re-saisie de l'utilisateur sous une trace qui nomme "
                f"l'evenement d'avant ; puis, rejouee sans journal comme avant "
                f"v7, elle doit laisser DEUX traces sur l'evenement (le journal "
                f"a double bascule que les rewinds suivants lisent).",
            ))
    walk_recs = [rec for w in walks for rec in w.get("second_restores") or []]
    by_name = [rec for rec in walk_recs
               if rec.get("outcome") == "refused_by_name"]
    # A write with the journal, a skip, or a legacy replay that did not
    # leave the double toggle. A refusal of another kind is judged by the
    # walk rules (it fails a GeoPackage walk, ends a shapefile one).
    wrong = [rec for rec in walk_recs
             if rec.get("outcome") in ("written", "skipped")
             or (rec.get("outcome") == "refused_by_name"
                 and rec.get("traces_on_event") not in (2, None))]
    outcomes = {}
    for rec in walk_recs:
        outcomes[rec.get("outcome")] = outcomes.get(rec.get("outcome"), 0) + 1
    out.append((
        "random_walk_second_restores_refused_by_name",
        not wrong and len(by_name) >= 1,
        f"deuxiemes restaurations des RETOGGLE={outcomes} "
        f"ecarts={json.dumps(wrong[:3], default=str)[:1500]} : journal "
        f"fourni, chacune doit etre refusee par son nom, et au moins une "
        f"doit avoir construit le journal a double bascule que la marche "
        f"relit ensuite.",
    ))
    bad = [w for w in walks if w.get("failure")]
    refused = sum(w.get("refused", 0) for w in walks)
    # Rewinds whose result was compared to the model (a refusal is counted
    # by the harness as a rewind, it is not a verified landing).
    checked = sum(w.get("checked", 0) for w in walks) - refused
    loud = [(w.get("seed"), w.get("loud")) for w in walks if w.get("loud")]
    out.append((
        "random_walk_with_retoggles_never_lands_elsewhere",
        not bad and len(walks) >= 2 and checked >= 12,
        f"marches={[(w.get('seed'), w.get('provider'), w.get('checked'), w.get('refused')) for w in walks]} "
        f"rewinds verifies={checked} refus nommes={refused} "
        f"echecs annonces (shapefile renumerote, marche arretee)={loud} "
        f"premier ecart={json.dumps(bad[0] if bad else None, default=str)[:2500]} "
        f"elapsed={ctx.data.get('elapsed_s')}s",
    ))
    out.append((
        "named_refusals_stay_the_exception",
        refused * 3 <= max(checked, 1),
        f"refus nommes={refused} sur {checked} rewinds : "
        f"{[w.get('refusals') for w in walks if w.get('refused')][:3]}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_state_at_t_toggles.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
