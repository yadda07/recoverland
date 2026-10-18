"""rs_trace_fingerprint_scope - RecoverLand validation runtime.

Invariant: une trace de restauration est lisible dans la meme portee que
l'evenement qu'elle compense.
Constat resolu: trace-fingerprint-recomputed (HIGH).

Le mecanisme
------------
`build_restore_trace_event` remplissait ``datasource_fingerprint`` avec
`compute_datasource_fingerprint(layer)`, recalcule sur la couche vivante,
au lieu de reprendre l'empreinte de l'evenement source. Tous les lecteurs
de traces filtrent sur ``datasource_fingerprint IN resolve_scope(<empreinte
de l'evenement>)``. Des que la forme recalculee differe de celle de
l'evenement -- journal d'avant la v6 dont la reconciliation des alias a ete
refusee, mode d'empreinte relatif contre absolu, couche retrouvee par
equivalence plutot que par identifiant -- la trace est rangee la ou aucun
lecteur ne la cherche.

Ce que l'utilisateur voit
-------------------------
Il restaure une entite supprimee sur une couche dont le journal date
d'avant la v6 (l'avertissement "degrade" est affiche). La trace part sous
l'empreinte canonique, les evenements restent sous l'ancienne. Au rewind
suivant, la trace est invisible: le DELETE est compense une seconde fois
et l'entite existe en double; le saut vers l'avant ne trouve aucune trace.

Cas
---
    F1  build_restore_trace_event: empreinte de la trace == empreinte de
        l'evenement source (et non celle recalculee sur la couche)
    F2  restore_batch: la trace produite par une vraie restauration porte
        l'empreinte de l'evenement

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_trace_fingerprint_scope"
INVARIANT = "trace-scope"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# The form a pre-v6 journal recorded for this layer; the live layer
# computes another one.
_LEGACY_FP = "ogr::C:/anciens_projets/reseau.gpkg|layername=reseau"


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rs_tfs_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = fx.make_gpkg(
        tmpdir, "reseau", ogr.wkbPoint,
        [("nom", ogr.OFTString), ("code", ogr.OFTString)],
        [(1, ["RESTE", "K1"], "POINT (1 1)")])
    flog(f"rs_trace_fingerprint_scope setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_service import (
        build_restore_trace_event, restore_batch,
    )
    fx = _fx()
    tid = ctx.trace_id
    flog(f"rs_trace_fingerprint_scope run start: trace_id={tid}", "INFO")

    layer = fx.open_layer(ctx.data["gpkg"], "reseau")
    ctx.data["computed_fp"] = compute_datasource_fingerprint(layer)
    event = fx.audit_event(
        event_id=601, operation_type="DELETE",
        datasource_fingerprint=_LEGACY_FP,
        feature_identity_json=json.dumps(
            {"fid": 8, "pk_field": "fid", "pk_value": 8}),
        entity_fingerprint="pk:fid=8",
        attributes_json=json.dumps({"all_attributes": {
            "fid": 8, "nom": "BORNE", "code": "K8"}}),
        geometry_wkb=fx.wkb_wkt("POINT (8 8)"),
        field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                    ("code", "string")))
    trace = build_restore_trace_event(event, layer)
    ctx.data["f1"] = {
        "trace_fp": getattr(trace, "datasource_fingerprint", None),
        "ref": getattr(trace, "restored_from_event_id", None),
        "entity": getattr(trace, "entity_fingerprint", None),
    }
    report = restore_batch(layer, [event], trace_id=tid)
    ctx.data["f2"] = {
        "succeeded": list(report.succeeded),
        "failed": {int(k): str(v)[:160] for k, v in report.failed.items()},
        "trace_fps": [t.datasource_fingerprint
                      for t in report.trace_events or ()],
    }
    flog(f"rs_trace_fingerprint_scope run end: trace_id={tid} "
         f"computed={ctx.data['computed_fp']!r} f1={ctx.data['f1']} "
         f"f2={ctx.data['f2']}", "INFO")
    del layer
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    f1 = ctx.data.get("f1") or {}
    f2 = ctx.data.get("f2") or {}
    out.append((
        "fixture_forms_differ",
        ctx.data.get("computed_fp") not in (None, _LEGACY_FP),
        f"empreinte recalculee={ctx.data.get('computed_fp')!r}: elle doit "
        f"differer de celle de l'evenement, sinon le scenario ne prouve rien",
    ))
    out.append((
        "f1_trace_keeps_event_fingerprint",
        f1.get("trace_fp") == _LEGACY_FP,
        f"trace={f1.get('trace_fp')!r} evenement={_LEGACY_FP!r}: la trace est "
        f"rangee sous une empreinte qu'aucun lecteur de la portee de "
        f"l'evenement ne consulte; le rewind suivant recompense le DELETE",
    ))
    out.append((
        "f1_trace_still_references_the_event",
        f1.get("ref") == 601 and f1.get("entity") == "pk:fid=8",
        f"trace={f1}",
    ))
    out.append((
        "f2_restore_trace_in_event_scope",
        f2.get("succeeded") == [601] and f2.get("trace_fps") == [_LEGACY_FP],
        f"restore_batch={f2}: la restauration reussie doit produire une "
        f"trace sous l'empreinte de l'evenement",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_trace_fingerprint_scope.*trace_id={ctx.trace_id}",
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
