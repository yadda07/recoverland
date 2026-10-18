"""maint_purge_dialog_defaults - RecoverLand validation runtime.

Invariant: "rien de cache a l'utilisateur" : une purge qui retire des
dates a l'historique de rewind le dit AVANT d'etre confirmee, et ne
s'applique pas par un clic par defaut.

Le mecanisme
------------
Le bouton "Purger" de la maintenance ouvre `_ask_purge_options`. Les
quatre cases (evenements anciens, traces invalidees, paires INSERT/DELETE,
traces orphelines) etaient toutes cochees par defaut, et rien ne disait ce
que chacune coute. Or chaque purge logique supprime des lignes que le
rewind lit encore : une paire INSERT/DELETE reconstruit les dates entre
ses deux evenements, une trace invalidee les dates ou elle etait en
vigueur, une trace orpheline dit que tout ce qui la precede exige un
evenement deja purge. Apres la purge, ces dates sont refusees au rewind
(horizon de purge) : un simple "OK" rendait des dates inaccessibles sans
que l'utilisateur l'ait su ni choisi.

Ce que le scenario execute
--------------------------
Un vrai journal avec une paire, une trace invalidee et une orpheline ; les
vrais compteurs et l'impact calcule par `retention` ; la vraie methode
`JournalMaintenanceDialog._ask_purge_options`, dont la fenetre modale est
remplacee par une lecture de ses cases et de ses textes suivie d'un "OK"
sans rien toucher (ce que fait l'utilisateur presse).

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
Un OK par defaut purge les paires, les traces invalidees et les
orphelines ; le lendemain, un rewind de la couche a une date de ces
intervalles est refuse "History purged" sans que la boite de purge l'ait
annonce.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "maint_purge_dialog_defaults"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _row(ds, op, fid, ts, session=None, restored_from=None, invalidated_at=None):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": ds,
        "layer_id_snapshot": ds, "layer_name_snapshot": ds,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op,
        "attributes_json": json.dumps({"all_attributes": {"nom": "x"}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": None,
        "field_schema_json": None, "user_name": "u", "session_id": session,
        "created_at": ts, "restored_from_event_id": restored_from,
        "entity_fingerprint": f"fid:{fid}", "event_schema_version": 7,
        "new_geometry_wkb": None, "invalidated_at": invalidated_at,
    }


def _insert(conn, row) -> int:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    _insert(conn, _row("pd_ds_p", "INSERT", 9, "2026-03-03T09:00:00+00:00", "s-p"))
    _insert(conn, _row("pd_ds_p", "DELETE", 9, "2026-03-03T09:30:00+00:00", "s-p"))
    src = _insert(conn, _row("pd_ds_i", "DELETE", 6, "2026-03-05T09:00:00+00:00", "s-i"))
    _insert(conn, _row("pd_ds_i", "INSERT", 6, "2026-03-05T10:00:00+00:00",
                       restored_from=src, invalidated_at="2026-03-05T11:00:00+00:00"))
    _insert(conn, _row("pd_ds_o", "INSERT", 4, "2026-03-04T10:00:00+00:00",
                       restored_from=999999))
    _insert(conn, _row("pd_ds_x", "UPDATE", 1, "2020-01-01T10:00:00+00:00", "s-x"))
    conn.commit()
    conn.close()
    ctx.data["path"] = path
    flog(f"maint_purge_dialog_defaults setup: trace_id={ctx.trace_id} path={path}")


def run(ctx):
    from qgis.PyQt.QtWidgets import QDialog, QCheckBox, QLabel
    from recoverland.compat import QtCompat
    from recoverland.core.logger import flog
    from recoverland.core import retention
    from recoverland.journal_maintenance import JournalMaintenanceDialog
    conn = sqlite3.connect(ctx.data["path"])
    policy = retention.RetentionPolicy(retention_days=365, max_events=1_000_000)
    try:
        old = retention.count_purgeable_events(conn, policy)
        garbage = retention.count_logical_garbage_events(conn)
        impact_fn = getattr(retention, "logical_purge_impact", None)
        impact = impact_fn(conn) if impact_fn else None
        cutoff_fn = getattr(retention, "retention_cutoff", None)
        cutoff = cutoff_fn(policy) if cutoff_fn else ""
    finally:
        conn.close()

    seen = {}

    def _fake_exec(dlg):
        seen["boxes"] = {c.text(): c.isChecked() for c in dlg.findChildren(QCheckBox)}
        seen["texts"] = [label.text() for label in dlg.findChildren(QLabel)]
        return QtCompat.DIALOG_ACCEPTED

    parent = QDialog()
    original = QDialog.exec
    QDialog.exec = _fake_exec
    try:
        try:
            options = JournalMaintenanceDialog._ask_purge_options(
                parent, old, garbage, impact, cutoff)
        except TypeError:  # older signature: counts only
            options = JournalMaintenanceDialog._ask_purge_options(parent, old, garbage)
    finally:
        QDialog.exec = original
        parent.deleteLater()
    ctx.data["old"] = old
    ctx.data["garbage"] = tuple(garbage)
    ctx.data["boxes"] = seen.get("boxes") or {}
    ctx.data["texts"] = seen.get("texts") or []
    ctx.data["options"] = options._asdict() if options is not None else None
    flog(f"maint_purge_dialog_defaults run: trace_id={ctx.trace_id} "
         f"boxes={ctx.data['boxes']} options={ctx.data['options']} "
         f"texts={ctx.data['texts']}")


def teardown(ctx):
    path = ctx.data.get("path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    boxes = ctx.data.get("boxes") or {}
    options = ctx.data.get("options") or {}
    texts = " | ".join(ctx.data.get("texts") or [])
    out = []
    logical = {k: v for k, v in boxes.items()
               if k.startswith(("Paires", "Traces"))}
    out.append((
        "logical_purges_unchecked_by_default",
        len(logical) == 3 and not any(logical.values()),
        f"cases={boxes} : une purge qui retire des dates au rewind ne doit "
        f"pas partir sur un OK par defaut.",
    ))
    out.append((
        "a_default_ok_keeps_the_rewind_history",
        bool(options) and not options.get("invalidated")
        and not options.get("insert_delete_pairs")
        and not options.get("orphan_traces"),
        f"options apres OK sans rien toucher={options}",
    ))
    out.append((
        "retention_purge_still_offered_as_configured",
        options.get("retention") is True,
        f"options={options} : la purge par anciennete applique la duree de "
        f"retention reglee par l'utilisateur.",
    ))
    out.append((
        "the_dialog_names_the_dates_each_purge_takes_away",
        "2026-03-03 09:00:00" in texts and "2026-03-03 09:30:00" in texts
        and "2026-03-05 11:00:00" in texts and "2026-03-04 10:00:00" in texts
        and "refuses" in texts,
        f"textes de la boite={ctx.data.get('texts')} : l'utilisateur doit lire "
        f"quelles dates ne pourront plus etre rembobinees (paire "
        f"09:00-09:30, trace annulee 10:00-11:00, orpheline 10:00) avant de "
        f"cocher.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"maint_purge_dialog_defaults.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
