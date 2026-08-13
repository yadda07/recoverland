"""tx_capture_suppress_leak - RecoverLand validation runtime.

Invariant: I-2 (la suppression de capture est TEMPORAIRE : elle protege la
duree d'une restauration, jamais au-dela).

Le mecanisme
------------
Pendant une restauration, le plugin ecrit lui-meme dans les couches. Sans
precaution, ces ecritures seraient journalisees comme des modifications de
l'utilisateur. `EditSessionTracker.suppress()` incremente un compteur de
profondeur, `unsuppress()` le decremente ; tant qu'il est > 0 les huit
gestionnaires de signaux sortent immediatement et RIEN n'est capture.

Deux facons d'utiliser ce verrou coexistent dans le produit :

    with tracker.suppressed(): ...      # try/finally, libere meme sur erreur
    tracker.suppress()                  # brut, la liberation est ailleurs

Les trois runners de restauration (`RestoreRunner.start`,
`StrictRestoreRunner.start`, `UndoRunner.start` dans restore_runner.py)
utilisent la forme brute :

    if self._tracker is not None:
        self._tracker.suppress()
    self._advance_group()          # <-- aucune protection

`_advance_group` appelle `_resolve_runner_layer`, qui appelle sans filet la
fonction de resolution de couche fournie par la fenetre
(`find_target_layer`). Cette fonction touche QgsProject et la base SQLite du
journal : une couche retiree du projet entre l'ouverture de la fenetre et le
clic sur Restaurer, un objet C++ deja detruit, une erreur SQLite, et elle
leve. L'exception traverse `start()`, qui est appele nu depuis
recover_dialog (`runner.start()`, sans try). Le compteur reste a 1.

Scenario d'echec concret
------------------------
L'utilisateur lance une restauration, elle echoue avec un message d'erreur.
Il laisse la fenetre RecoverLand ouverte, retourne a sa carte et continue a
travailler : deplacements, corrections d'attributs, suppressions. AUCUNE de
ces modifications n'est journalisee. Il n'y a ni message ni indicateur. Le
seul appel a `force_unsuppress()` du produit se trouve dans
`recover_dialog.cleanup_resources`, c'est-a-dire a la FERMETURE de la
fenetre : tout le travail fait avant cette fermeture est definitivement
absent de l'historique. Pour un outil de recuperation, c'est le pire moment
possible pour devenir aveugle - juste apres un echec de restauration, quand
l'utilisateur repare a la main.

Montage (vrai GPKG dans un dossier temporaire, vrai RestoreRunner) :
    S1  exception levee DANS `with tracker.suppressed()`
    S2  imbrication suppress/unsuppress et unsuppress en trop
    S3  edition reelle : la capture fonctionne avant l'incident
    S4  RestoreRunner reel dont la resolution de couche leve
    S5  edition reelle apres l'incident : est-elle encore journalisee ?
    S6  force_unsuppress puis edition reelle : la sortie de secours existe
"""
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_capture_suppress_leak"
INVARIANT = "I-2"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = ["nom", "etat"]


# --------------------------------------------------------------------------
# Fixture
# --------------------------------------------------------------------------
def _make_gpkg(tmpdir: str) -> str:
    from osgeo import ogr

    path = os.path.join(tmpdir, "reseau.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("driver GPKG indisponible")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer("reseau", geom_type=ogr.wkbPoint)
    for name in _FIELDS:
        lyr.CreateField(ogr.FieldDefn(name, ogr.OFTString))
    ds.FlushCache()
    ds = None
    return path


def _load_layer(path: str):
    from qgis.core import QgsVectorLayer

    layer = QgsVectorLayer(f"{path}|layername=reseau", "reseau", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path!r}")
    return layer


def _seed(layer) -> list:
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    layer.startEditing()
    for k in range(3):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("nom", "poste_%d" % k)
        feat.setAttribute("etat", "initial")
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(k), float(k))))
        layer.addFeature(feat)
    layer.commitChanges()
    layer.reload()
    return [f.id() for f in layer.getFeatures()]


class _CapturingWQ:
    def __init__(self):
        self.events: list = []

    def enqueue(self, events) -> bool:
        self.events.extend(events)
        return True

    def set_flush_callback(self, callback) -> None:
        self._cb = callback


class _DummyJM:
    def get_connection(self):
        raise RuntimeError("tx_capture: pas de journal reel dans ce scenario")


def _make_event(ds_fp: str):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=1,
        project_fingerprint="tx_leak_project",
        datasource_fingerprint=ds_fp,
        layer_id_snapshot="lyr_leak",
        layer_name_snapshot="reseau",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 1}),
        operation_type="UPDATE",
        attributes_json=json.dumps(
            {"changed_only": {"etat": {"old": "initial", "new": "modifie"}}}),
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps([{"name": n, "type": "string"}
                                      for n in _FIELDS]),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 9, 0, 0,
                            tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:1",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _edit_once(layer, value: str) -> bool:
    """Une vraie modification d'attribut, commit compris."""
    idx = layer.fields().indexFromName("etat")
    fid = next(layer.getFeatures()).id()
    if not layer.startEditing():
        return False
    layer.changeAttributeValue(fid, idx, value)
    return bool(layer.commitChanges())


# --------------------------------------------------------------------------
# Scenario
# --------------------------------------------------------------------------
def setup(ctx):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_leak_")
    path = _make_gpkg(tmpdir)
    layer = _load_layer(path)
    fids = _seed(layer)

    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()
    tracker.connect_layer(layer)

    ctx.data["tmpdir"] = tmpdir
    ctx.data["layer"] = layer
    ctx.data["fids"] = fids
    ctx.data["wq"] = wq
    ctx.data["tracker"] = tracker
    ctx.data["ds_fp"] = compute_datasource_fingerprint(layer)
    ctx.data["connected"] = layer.id() in tracker._connected_layers

    flog(
        f"tx_capture_suppress_leak setup: trace_id={ctx.trace_id} "
        f"gpkg={path} fids={fids} connected={ctx.data['connected']} "
        f"ds_fp={ctx.data['ds_fp']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    tracker = ctx.data["tracker"]
    layer = ctx.data["layer"]
    wq = ctx.data["wq"]

    flog(f"tx_capture_suppress_leak run start: trace_id={ctx.trace_id}", "INFO")

    # ===== S1 : exception dans le gestionnaire de contexte ==============
    raised = None
    try:
        with tracker.suppressed():
            ctx.data["s1_depth_inside"] = tracker._suppress_depth
            raise ValueError("panne simulee pendant la restauration")
    except ValueError as exc:
        raised = str(exc)
    ctx.data["s1_exception"] = raised
    ctx.data["s1_suppressed_after"] = tracker.is_suppressed
    ctx.data["s1_depth_after"] = tracker._suppress_depth

    # ===== S2 : imbrication et unsuppress en trop =======================
    tracker.suppress()
    tracker.suppress()
    tracker.unsuppress()
    ctx.data["s2_still_suppressed_at_depth_1"] = tracker.is_suppressed
    tracker.unsuppress()
    ctx.data["s2_free_at_depth_0"] = not tracker.is_suppressed
    tracker.unsuppress()
    tracker.unsuppress()
    ctx.data["s2_depth_after_extra_unsuppress"] = tracker._suppress_depth
    tracker.suppress()
    ctx.data["s2_suppress_still_works"] = tracker.is_suppressed
    tracker.unsuppress()

    # ===== S3 : la capture fonctionne AVANT l'incident ==================
    mark = len(wq.events)
    ctx.data["s3_commit_ok"] = _edit_once(layer, "S3_AVANT")
    layer.reload()
    ctx.data["s3_events"] = len(wq.events) - mark

    # ===== S4 : un vrai RestoreRunner dont la resolution de couche leve ==
    from recoverland.restore_runner import RestoreRunner

    def _broken_resolver(_event):
        # Cas reel : la couche a ete retiree du projet depuis l'ouverture de
        # la fenetre, l'objet C++ sous-jacent est detruit et PyQt leve.
        raise RuntimeError(
            "wrapped C/C++ object of type QgsVectorLayer has been deleted")

    runner = RestoreRunner(
        [_make_event(ctx.data["ds_fp"])],
        _broken_resolver,
        write_queue=None,
        tracker=tracker,
        trace_id=ctx.trace_id,
    )
    runner_error = None
    try:
        runner.start()
    except Exception as exc:  # noqa: BLE001 - c'est exactement ce qu'on teste
        runner_error = f"{type(exc).__name__}: {exc}"
    ctx.data["s4_runner_error"] = runner_error
    ctx.data["s4_suppressed_after"] = tracker.is_suppressed
    ctx.data["s4_depth_after"] = tracker._suppress_depth

    # ===== S5 : l'utilisateur continue a travailler =====================
    mark = len(wq.events)
    ctx.data["s5_commit_ok"] = _edit_once(layer, "S5_APRES_ECHEC")
    layer.reload()
    ctx.data["s5_events"] = len(wq.events) - mark
    ctx.data["s5_disk_value"] = next(layer.getFeatures())["etat"]

    # ===== S6 : la sortie de secours (fermeture de la fenetre) ==========
    tracker.force_unsuppress()
    mark = len(wq.events)
    ctx.data["s6_commit_ok"] = _edit_once(layer, "S6_APRES_SECOURS")
    layer.reload()
    ctx.data["s6_events"] = len(wq.events) - mark

    flog(
        f"tx_capture_suppress_leak run end: trace_id={ctx.trace_id} "
        f"s1_after={ctx.data['s1_suppressed_after']} "
        f"s4_error={ctx.data['s4_runner_error']!r} "
        f"s4_suppressed={ctx.data['s4_suppressed_after']} "
        f"s3_events={ctx.data['s3_events']} s5_events={ctx.data['s5_events']} "
        f"s6_events={ctx.data['s6_events']}",
        "INFO",
    )

    ctx.data["layer"] = None
    tracker.disconnect_all()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


# --------------------------------------------------------------------------
# Assertions
# --------------------------------------------------------------------------
_START_RE = re.compile(
    r"^\s{4}def start\(self\)[^\n]*\n((?:\s{8}[^\n]*\n|\s*\n)+)", re.M)


def _unprotected_start_methods() -> list:
    """Retourne les `def start` de restore_runner.py qui suppriment la
    capture sans aucun try/finally autour de la suite."""
    src = (_PLUGIN_ROOT / "restore_runner.py").read_text(
        encoding="utf-8", errors="replace")
    bad = []
    for idx, match in enumerate(_START_RE.finditer(src)):
        body = match.group(1)
        if "suppress()" not in body:
            continue
        if "try:" in body or "suppressed()" in body:
            continue
        line = src[:match.start()].count("\n") + 1
        bad.append(f"restore_runner.py:{line}")
    return bad


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    out.append((
        "fixture_layer_tracked",
        ctx.data.get("connected") is True,
        f"connected={ctx.data.get('connected')} : sans couche suivie le "
        f"scenario ne teste rien",
    ))

    # ===== S1 : le gestionnaire de contexte tient ses promesses =========
    out.append((
        "S1_exception_really_propagated",
        ctx.data.get("s1_exception") is not None
        and ctx.data.get("s1_depth_inside") == 1,
        f"exception={ctx.data.get('s1_exception')!r} "
        f"profondeur_dans_le_bloc={ctx.data.get('s1_depth_inside')} : la "
        f"phase doit vraiment lever a l'interieur du bloc protege",
    ))
    out.append((
        "S1_context_manager_releases_on_exception",
        ctx.data.get("s1_suppressed_after") is False
        and ctx.data.get("s1_depth_after") == 0,
        f"suppressed={ctx.data.get('s1_suppressed_after')} "
        f"depth={ctx.data.get('s1_depth_after')} attendu=False/0 : "
        f"`with tracker.suppressed()` doit rendre la capture meme quand le "
        f"corps explose",
    ))

    # ===== S2 : semantique du compteur ==================================
    out.append((
        "S2_nested_suppress_needs_as_many_unsuppress",
        ctx.data.get("s2_still_suppressed_at_depth_1") is True
        and ctx.data.get("s2_free_at_depth_0") is True,
        f"apres 2 suppress + 1 unsuppress suppressed="
        f"{ctx.data.get('s2_still_suppressed_at_depth_1')} (attendu True), "
        f"apres le 2e unsuppress libre="
        f"{ctx.data.get('s2_free_at_depth_0')} (attendu True) : deux "
        f"restaurations imbriquees ne doivent pas se deverrouiller l'une "
        f"l'autre",
    ))
    out.append((
        "S2_extra_unsuppress_cannot_go_negative",
        ctx.data.get("s2_depth_after_extra_unsuppress") == 0
        and ctx.data.get("s2_suppress_still_works") is True,
        f"profondeur apres unsuppress en trop="
        f"{ctx.data.get('s2_depth_after_extra_unsuppress')} attendu=0, "
        f"suppress suivant efficace={ctx.data.get('s2_suppress_still_works')} "
        f"attendu=True : un compteur negatif rendrait la protection "
        f"inoperante et la restauration suivante se journaliserait elle-meme, "
        f"creant une boucle d'evenements parasites",
    ))

    # ===== S3 : etat de reference =======================================
    out.append((
        "S3_capture_works_before_the_incident",
        ctx.data.get("s3_commit_ok") is True and ctx.data.get("s3_events") == 1,
        f"commit_ok={ctx.data.get('s3_commit_ok')} "
        f"evenements={ctx.data.get('s3_events')} attendu=True/1 : etat de "
        f"reference, la capture fonctionne",
    ))

    # ===== S4 : le trou =================================================
    out.append((
        "S4_runner_really_failed",
        ctx.data.get("s4_runner_error") is not None,
        f"erreur={ctx.data.get('s4_runner_error')!r} : la phase doit vraiment "
        f"faire echouer la resolution de couche du runner",
    ))
    out.append((
        "S4_failed_restore_releases_the_capture_lock",
        ctx.data.get("s4_suppressed_after") is False,
        f"suppressed={ctx.data.get('s4_suppressed_after')} "
        f"depth={ctx.data.get('s4_depth_after')} attendu=False/0 : "
        f"`RestoreRunner.start` appelle `tracker.suppress()` puis "
        f"`_advance_group()` sans try/finally, et recover_dialog appelle "
        f"`runner.start()` nu. Une resolution de couche qui leve (couche "
        f"retiree du projet, objet C++ detruit, erreur SQLite) laisse le "
        f"verrou pose. Le plugin cesse alors d'enregistrer les modifications "
        f"de l'utilisateur.",
    ))
    out.append((
        "S5_user_edits_are_still_journalled",
        ctx.data.get("s5_commit_ok") is True and ctx.data.get("s5_events") == 1,
        f"commit_ok={ctx.data.get('s5_commit_ok')} "
        f"evenements_captures={ctx.data.get('s5_events')} attendu=1, valeur "
        f"ecrite sur le disque={ctx.data.get('s5_disk_value')!r} : c'est "
        f"l'impact utilisateur. Apres l'echec de restauration il repare sa "
        f"donnee a la main ; QGIS enregistre tout sur le disque et "
        f"RecoverLand n'en garde aucune trace, sans le moindre message. Ce "
        f"travail sera irrecuperable et les rewind ulterieurs partiront d'un "
        f"historique faux. Le seul retour a la normale du produit est la "
        f"fermeture de la fenetre RecoverLand "
        f"(recover_dialog.cleanup_resources -> force_unsuppress).",
    ))

    # ===== S6 : la sortie de secours existe bien ========================
    out.append((
        "S6_force_unsuppress_restores_capture",
        ctx.data.get("s6_commit_ok") is True and ctx.data.get("s6_events") == 1,
        f"commit_ok={ctx.data.get('s6_commit_ok')} "
        f"evenements={ctx.data.get('s6_events')} attendu=True/1 : "
        f"`force_unsuppress` doit bien remettre la capture en marche",
    ))

    # ===== Source : les points d'appel bruts ============================
    bad = _unprotected_start_methods()
    out.append((
        "source_runners_protect_their_suppress",
        bad == [],
        f"methodes start() qui posent le verrou sans try/finally: {bad} : "
        f"tant qu'un seul de ces chemins existe, n'importe quelle exception "
        f"pendant une restauration rend le plugin muet pour le reste de la "
        f"session de travail",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker: suppressed \(depth=1\)",
        name="suppress_is_logged_with_depth",
        min_count=2,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_capture_suppress_leak.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
