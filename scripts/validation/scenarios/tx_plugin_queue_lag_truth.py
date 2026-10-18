"""tx_plugin_queue_lag_truth - RecoverLand validation runtime.

Invariant: I-OBS (ce que RecoverLand dit a l'utilisateur est vrai : ni
risque invente, ni risque tu).

Le mecanisme
------------
`WriteQueue._warn_on_depth` (core/write_queue.py) appelle, une fois par
thread d'ecriture, le rappel d'alerte precoce quand plus de
`_QUEUE_EARLY_WARNING` evenements attendent le journal. Le plugin y branche
`RecoverPlugin._on_queue_early_warning` (recover.py), qui disait :

    "File d'ecriture a 80%% de capacite. Si le probleme persiste,
     l'enregistrement sera desactive. Verifiez les performances disque."

Deux choses fausses. Le ``%%`` s'affichait tel quel (rien ne formate cette
chaine). Et v7 ne desactive JAMAIS la capture sur le volume : la file ne
refuse plus rien, chaque evenement est deja durable dans l'outbox avant
d'y entrer. Le vrai cout du retard est ailleurs : un Restore, un Rewind ou
une annulation lance pendant ce retard lit le journal, donc attend qu'il
soit a jour (`RecoverDialog._journal_barrier`) et est refuse s'il ne l'est
pas a temps. Le message n'allait en outre qu'au panneau de log de QGIS.

Le scenario d'echec concret
---------------------------
Sur un gros collage d'entites, l'utilisateur lit que l'enregistrement
"sera desactive" : il arrete de travailler, ou coupe lui-meme la capture
pour "eviter pire", alors que rien n'etait en danger. Et quand son Rewind
est refuse deux minutes plus tard, rien ne lui a dit pourquoi.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous plugin_harness.FakeIface, VRAI WriteQueue et VRAI
suivi branches par `_init_local_backend`, une vraie couche GeoPackage. Le
seuil `_QUEUE_EARLY_WARNING` est abaisse a -1 pour la duree du cas : le
premier commit declenche le vrai `_warn_on_depth`, qui appelle le rappel
que le plugin a branche. Ce que l'utilisateur voit (barre de messages,
panneau de log RecoverLand, enregistre a l'appel de `QgsMessageLog`) est
note.
  Q1  l'alerte arrive a l'utilisateur, dans la barre de messages ;
  Q2  aucun ``%%`` litteral ;
  Q3  aucune promesse de desactivation de l'enregistrement ;
  Q4  elle dit que rien n'est perdu (outbox durable) et ce que le retard
      coute (Restore / Rewind qui attendent ou sont refuses).

Verdict attendu avant correctif : FAIL (Q1, Q2, Q3, Q4).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_plugin_queue_lag_truth"
INVARIANT = "I-OBS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)",
                         "seed", "memory")
    mem.startEditing()
    f = QgsFeature(mem.fields())
    f.setAttributes(["C000"])
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 1.0)))
    mem.addFeature(f)
    mem.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


class _PanelRecorder:
    """What reaches the RecoverLand tab of the QGIS message log panel.

    ``QgsMessageLog.messageReceived`` is not emitted in this headless
    runtime (measured, see tx_journal_lock_no_retry), so every user-facing
    ``qlog`` is recorded at the call, and forwarded unchanged.
    """

    def __init__(self, real):
        self.real = real
        self.messages = []

    def logMessage(self, message, tag="", *args, **kwargs):  # noqa: N802
        if tag == "RecoverLand":
            self.messages.append(str(message))
        return self.real.logMessage(message, tag, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.real, name)


def setup(ctx):
    from qgis.core import QgsProject, QgsSettings
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    QgsSettings().setValue("RecoverLand/tracking_enabled", True)
    QgsSettings().setValue("RecoverLand/tracking_disabled_by_disk", False)
    tmp = tempfile.mkdtemp(prefix="rl_q_lag_")
    ctx.data["tmp"] = tmp
    _make_gpkg(os.path.join(tmp, "lag.gpkg"))
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    import recoverland.core.logger as logger_mod
    import recoverland.core.write_queue as wq_mod
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    real_log = logger_mod.QgsMessageLog
    panel = _PanelRecorder(real_log)
    logger_mod.QgsMessageLog = panel
    logged = panel.messages
    saved = wq_mod._QUEUE_EARLY_WARNING
    try:
        plugin.initGui()
        pump(0.3)
        d["wired"] = getattr(plugin._write_queue, "_on_early_warning", None) is not None
        layer = QgsVectorLayer(f"{os.path.join(d['tmp'], 'lag.gpkg')}|layername=pts",
                               "couche_lag", "ogr")
        QgsProject.instance().addMapLayer(layer)
        pump(0.2)
        bar_start, log_start = len(iface.bar.messages), len(logged)
        wq_mod._QUEUE_EARLY_WARNING = -1
        fid = next(layer.getFeatures()).id()
        layer.startEditing()
        layer.changeAttributeValue(fid, layer.fields().indexFromName("code"), "LAG")
        d["commit"] = bool(layer.commitChanges())
        pump(0.3)
        d["bar"] = [str(m[1]) for m in iface.bar.messages[bar_start:]]
        d["log"] = list(logged[log_start:])
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        wq_mod._QUEUE_EARLY_WARNING = saved
        logger_mod.QgsMessageLog = real_log
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:5000]}",
         "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)


def _lag_texts(texts) -> list:
    """The queue-lag warning among what was shown (old or new wording)."""
    keys = ("file d'ecriture", "prend du retard")
    return [t for t in texts or [] if any(k in t.lower() for k in keys)]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    shown = _lag_texts((d.get("bar") or []) + (d.get("log") or []))
    out.append((
        "fixture_warning_fired",
        d.get("wired") is True and d.get("commit") is True and bool(shown),
        f"rappel branche={d.get('wired')} commit={d.get('commit')} textes="
        f"{shown} crash={d.get('crash')} {d.get('trace', '')}",
    ))
    bar = _lag_texts(d.get("bar"))
    out.append((
        "q1_warning_reaches_the_message_bar",
        bool(bar),
        f"barre de messages={d.get('bar')} (panneau de log : {d.get('log')}). "
        f"Un avertissement que seul le panneau de log montre n'est pas lu",
    ))
    out.append((
        "q2_no_literal_double_percent",
        bool(shown) and not any("%%" in t for t in shown),
        f"textes={shown} : '%%' s'affiche tel quel, rien ne formate la chaine",
    ))
    out.append((
        "q3_no_false_promise_to_disable_capture",
        bool(shown) and not any("desactiv" in t.lower() for t in shown),
        f"textes={shown} : v7 ne desactive jamais la capture sur le volume ; "
        f"l'annoncer pousse l'utilisateur a s'arreter pour rien",
    ))
    out.append((
        "q4_says_nothing_is_lost_and_what_the_lag_costs",
        bool(shown) and all("outbox" in t and "durable" in t
                            and ("Restore" in t or "Rewind" in t) for t in shown),
        f"textes={shown} : doit dire que chaque modification est deja durable "
        f"(outbox) et qu'un Restore / Rewind attend le journal ou est refuse",
    ))
    out.append((
        "unload_clean",
        not d.get("unload_error"),
        f"unload={d.get('unload_error')}",
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
