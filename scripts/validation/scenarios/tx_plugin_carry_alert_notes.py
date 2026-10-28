# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_carry_alert_notes - RecoverLand validation runtime.

Invariant: I-OBS (ce que le rattachement de l'historique n'a pas pu
garantir est dit a l'utilisateur, dans l'alerte de l'enregistrement).

Le mecanisme
------------
A l'enregistrement d'un projet, `RecoverPlugin._on_project_saved`
(recover.py) montre `HistoryCarryReport.user_message()` (core/
journal_manager.py) dans la barre de messages. Deux champs du rapport
pouvaient compter pour l'utilisateur :
  * `out_of_order_entities` : des entites que le journal du projet avait
    deja modifiees plus tard (une autre instance) ; leur historique est
    fusionne dans le desordre, un Rewind sur elles peut se tromper d'ordre ;
  * `source_read_only` : le journal d'origine n'a pu qu'etre lu.
La question etait de savoir si l'alerte les dit.

Constat (mesure ici) : `user_message()` porte deja `out_of_order_entities`
(par `HistoryCarryReport.notes()`, niveau WARNING), et le plugin montre ce
texte tel quel. `source_read_only`, apres un rattachement reussi, n'a pas
de consequence pour l'utilisateur : la source n'est que lue, rien n'y est
ecrit ni perdu, l'historique est dans le journal du projet ou la capture
continue ; seul un marquage "rattache" manque, que le nettoyage des
orphelins gere et signale lui-meme. Quand il se combine a des
modifications d'autres sessions laissees dans l'outbox de la source, le
texte le dit deja. Rien n'est ajoute : une phrase de plus sans consequence
noierait celle qui en a une.

Le scenario d'echec qu'il garde
-------------------------------
Un "Enregistrer sous" fusionne l'historique d'une entite que l'autre poste
a modifiee entre-temps. Si l'alerte ne le disait pas, l'utilisateur
lancerait un Rewind sur un ordre d'historique faux sans le savoir.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous FakeIface, VRAI `HistoryCarryReport` (rattachement
reussi, 5 modifications, 2 entites hors d'ordre, source en lecture seule)
pose comme dernier rapport du VRAI JournalManager, puis la VRAIE etape 2 de
`_on_project_saved` pour un projet ferme dans le meme appel (le chemin qui
ne montre que le rapport).

Verdict attendu avant correctif : PASS (deja dit ; aucun correctif).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCENARIO_ID = "tx_plugin_carry_alert_notes"
INVARIANT = "I-OBS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import Qgis, QgsProject
    from recoverland.core.journal_manager import HistoryCarryReport
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        plugin.initGui()
        pump(0.3)
        report = HistoryCarryReport("C:/archive/session.sqlite",
                                    "C:/projets/p/.recoverland/recoverland_audit.sqlite")
        report.ok = True
        report.events_inserted = 5
        report.out_of_order_entities = 2
        report.source_read_only = True
        report.capture_journal = report.target
        report.capture_write_ok = True
        plugin._journal.last_carry_report = report
        start = len(iface.bar.messages)
        # Step 2 of a save whose project was cleared in the same call: only
        # the carry's outcome is left to tell.
        plugin._on_project_saved(True, "__projet_ferme__", True)
        pump(0.1)
        d["bar"] = [{"text": m[1], "warning": m[2] == Qgis.MessageLevel.Warning}
                    for m in iface.bar.messages[start:]]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            plugin._journal.last_carry_report = None
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps(d, default=str)[:3000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    bar = d.get("bar") or []
    out = [(
        "out_of_order_entities_reach_the_carry_alert",
        any(m["warning"] and "2 entite(s)" in m["text"] and "ordre" in m["text"]
            and "Rewind" in m["text"] for m in bar),
        f"alerte du rattachement={bar} crash={d.get('crash')} "
        f"{d.get('trace', '')}. Sans elle, un Rewind part sur un historique "
        f"fusionne dans le desordre sans que l'utilisateur le sache",
    ), (
        "unload_clean",
        not d.get("unload_error"),
        f"unload={d.get('unload_error')}",
    )]
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
