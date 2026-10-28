# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_stop_keeps_run_guard - RecoverLand validation runtime.

Invariant: I-SINGLE-WRITER (une seule restauration ou un seul rewind ecrit a
la fois, et le rapport d'un run decrit les effets de CE run).

Le mecanisme
------------
`_on_period_mode_changed` appelle `_review_stop_session()` a CHAQUE passage en
mode Rewind ou Restore, que Review ait ete actif ou non. Cette methode faisait
``self._is_recovering = False`` puis ``enable_controls(True)`` sans condition.
Or le selecteur de mode n'est jamais desactive pendant un run, et le chemin
Rewind n'est garde que par ``_is_recovering``. Les runners de restauration
tournent sur le thread de l'interface par tranches (QTimer), donc le dialogue
reste cliquable pendant les 95 a 283 s d'un run.

L'echec concret pour l'utilisateur
----------------------------------
Un Rewind est en cours (groupe 3 sur 20). L'utilisateur clique sur le segment
"Restore" puis revient sur "Rewind" : le garde tombe, le bouton Rewind se
rallume, un second clic lance un second run sur un journal auquel il manque
les traces du premier. Deux runners ecrivent les memes groupes, les traces
sont dupliquees, et le rapport final ne decrit plus ce qui a ete ecrit.

Le scenario pilote le vrai dialogue : il pose l'etat d'un run en cours
(garde leve, restauration en cours, runner present), bascule les modes comme
l'utilisateur, puis relance un Rewind. Il verifie aussi que Review, lui,
relache bien le garde qu'il a leve pour son propre demarrage.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_stop_keeps_run_guard"
INVARIANT = "I-SINGLE-WRITER"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_guard_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", [
        ("P1", "Alpha", 1.0, 1.0, 1.0), ("P2", "Beta", 2.0, 2.0, 2.0)])
    flog(f"rvw_stop_keeps_run_guard setup: trace_id={ctx.trace_id}", "INFO")


class _RunnerInFlight:
    """Stands for the StrictRestoreRunner of the Rewind in progress."""

    def cancel(self):
        pass


def run(ctx):
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        dlg = h.open_dialog(plugin, [layer])

        # ---- Review releases the guard it raised for its own start ------
        d["review_on"] = h.review_on(dlg, 1)
        d["review_guard_after_start"] = bool(dlg._is_recovering)
        h.review_off(dlg)

        # ---- a Rewind is running -------------------------------------------
        dlg.restore_mode_selector.setMode("temporal")
        dlg._on_period_mode_changed("temporal")
        calls = []
        dlg._recover_version_mode = lambda: calls.append("second_rewind")
        dlg._is_recovering = True
        dlg.enable_controls(False)
        dlg._restore_in_progress = True
        dlg._restore_runner = _RunnerInFlight()
        # The user clicks "Restore", then "Rewind" again, mid-run.
        dlg.restore_mode_selector.setMode("event")
        dlg._on_period_mode_changed("event")
        dlg.restore_mode_selector.setMode("temporal")
        dlg._on_period_mode_changed("temporal")
        d["guard_after_switch"] = bool(dlg._is_recovering)
        d["rewind_button_enabled"] = bool(dlg.recover_button.isEnabled())
        # ...and clicks Rewind, on a valid date: only the guard may stop it.
        from qgis.PyQt.QtCore import QDateTime
        dlg.time_slider.cutoff_datetime = QDateTime.currentDateTimeUtc
        d["inputs_valid"] = bool(dlg.validate_inputs())
        dlg.recover_and_load()
        d["second_rewind_started"] = bool(calls)
        d["guard_after_click"] = bool(dlg._is_recovering)
        # The run ends normally.
        dlg._restore_runner = None
        dlg._restore_in_progress = False
        dlg._is_recovering = False
        dlg.enable_controls(True)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_stop_keeps_run_guard run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "review_start_releases_its_own_guard",
        d.get("review_on") is True and d.get("review_guard_after_start") is False,
        f"review_on={d.get('review_on')} garde apres demarrage Review="
        f"{d.get('review_guard_after_start')} : Review ne doit pas laisser le "
        f"dialogue bloque en 'recovering'{crash}",
    ))
    out.append((
        "mode_switch_keeps_the_running_rewind_guard",
        d.get("guard_after_switch") is True,
        f"garde apres Restore puis Rewind pendant le run="
        f"{d.get('guard_after_switch')} (attendu True) : sinon un second "
        f"Rewind peut demarrer pendant que le premier ecrit",
    ))
    out.append((
        "rewind_button_stays_disabled_during_the_run",
        d.get("rewind_button_enabled") is False,
        f"bouton Rewind actif pendant le run={d.get('rewind_button_enabled')}",
    ))
    out.append((
        "no_second_rewind_while_one_is_writing",
        d.get("inputs_valid") is True and d.get("second_rewind_started") is False
        and d.get("guard_after_click") is True,
        f"entrees valides={d.get('inputs_valid')} second rewind lance="
        f"{d.get('second_rewind_started')} garde apres le clic="
        f"{d.get('guard_after_click')} : deux runners ecriraient les memes "
        f"groupes et dupliqueraient les traces",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_stop_keeps_run_guard.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
