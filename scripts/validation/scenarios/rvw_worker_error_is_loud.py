# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_worker_error_is_loud - RecoverLand validation runtime.

Invariant: I-FAIL-LOUD (une erreur n'est jamais rendue comme un resultat
vide valide).

Le mecanisme
------------
Toute exception du SnapshotRebuildWorker (journal ferme pendant un changement
de projet, page corrompue, schema different, decodage) arrive dans
`_on_snapshot_error`, qui ne faisait que journaliser et appeler
``bar.set_stats(0)`` : la barre affiche alors le symbole vide et l'infobulle
"Aucune entite a cette date". Pas de message, pas d'etat d'erreur, et les
calques Review de la date PRECEDENTE restent peints.

L'echec concret pour l'utilisateur
----------------------------------
Le journal devient illisible (verrou, corruption, disque reseau coupe) et
l'utilisateur deplace la date vers T2. La barre dit "aucune entite a cette
date" pendant que la carte montre encore l'etat de T1. Il conclut que rien
ne differait a T2.

Le scenario pilote le vrai dialogue, avec un vrai journal ; la seule
intervention est un journal dont l'ouverture en lecture echoue au moment ou
l'utilisateur change de date.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_worker_error_is_loud"
INVARIANT = "I-FAIL-LOUD"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


class _UnreadableJournal:
    """The real journal, whose file can no longer be read."""

    def __init__(self, journal):
        self._journal = journal

    def __getattr__(self, name):
        return getattr(self._journal, name)

    def create_read_connection(self):
        raise sqlite3.OperationalError("disk I/O error")


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_error_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", [
        ("P1", "Alpha", 1.0, 1.0, 1.0)])
    flog(f"rvw_worker_error_is_loud setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        h.user_commit(layer, lambda lyr: h.add(lyr, "P7", "Eta", 7.0, 7.0, 7.0))
        rows0 = h.wait_rows(jpath, 1)
        time.sleep(1.2)
        cutoff = h.utc_now_iso()
        time.sleep(1.2)
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P7", "Eta2"))
        h.wait_rows(jpath, rows0 + 1)
        h.set_view(iface, 0.0, 0.0, 10.0, 10.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay_t1"] = h.overlay_rows(dlg)

        n_msgs = len(iface.bar.messages)
        dlg._journal = _UnreadableJournal(plugin._journal)
        h.review_date(dlg, "2020-01-01T00:00:00")
        bar = dlg._review_date_bar
        d["bar_text"] = bar._lbl_status.text() if bar is not None else None
        d["bar_tip"] = bar._lbl_status.toolTip() if bar is not None else None
        d["new_messages"] = [m[:2] for m in iface.bar.messages[n_msgs:]]
        d["overlay_after_error"] = h.overlay_rows(dlg)
        dlg._journal = plugin._journal
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_worker_error_is_loud run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    tip = d.get("bar_tip") or ""
    out = []
    out.append((
        "setup_overlay_shows_t1",
        d.get("review_on") is True and "P7" in (d.get("overlay_t1") or {}),
        f"review_on={d.get('review_on')} calque Review a T1={d.get('overlay_t1')}{crash}",
    ))
    out.append((
        "bar_does_not_render_the_error_as_empty",
        d.get("bar_text") not in (None, "∅") and "Aucune entit" not in tip,
        f"barre={d.get('bar_text')!r} infobulle={tip!r} : une lecture ratee "
        f"n'est pas 'aucune entite a cette date'",
    ))
    out.append((
        "bar_names_the_failure",
        "disk I/O error" in tip,
        f"infobulle={tip!r} : elle doit nommer l'erreur",
    ))
    out.append((
        "user_is_told_in_the_message_bar",
        bool(d.get("new_messages")),
        f"messages pousses={d.get('new_messages')} : l'erreur ne doit pas "
        f"rester dans le fichier de log",
    ))
    out.append((
        "previous_date_not_left_painted",
        not (d.get("overlay_after_error") or {}),
        f"calque Review apres l'erreur={d.get('overlay_after_error')} : l'etat "
        f"de T1 ne doit pas rester affiche comme la reponse pour T2",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_worker_error_is_loud.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
