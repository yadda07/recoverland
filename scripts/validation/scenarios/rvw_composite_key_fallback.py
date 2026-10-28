# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_composite_key_fallback - RecoverLand validation runtime.

Invariant: I-3 (Review et Rewind designent la MEME entite par la meme cle,
et cette cle est celle que calcule identity.compute_entity_fingerprint).

Le mecanisme
------------
Une cle primaire composite est journalisee entiere depuis v7
(``{"fid": .., "pk_fields": [..], "pk_values": [..]}``) et son empreinte
canonique est ``pkc:zone=12|parcelle=7`` (identity.compute_entity_fingerprint).
Quand la colonne ``entity_fingerprint`` d'un evenement est vide (ligne
re-importee du fichier d'attente, calcul d'empreinte en echec, journal
rempli par un outil), le moteur de Review retombe sur
``temporal_snapshot_engine._canonical_fallback_key``, qui dit "reproduire
compute_entity_fingerprint" mais n'en connait que les formes ``pk:`` et
``fid:``. Il range donc cet evenement sous ``fid:5`` alors que les
evenements de la meme entite qui portent leur empreinte sont sous
``pkc:...``. Le Rewind (rewind_dedup._entity_key) delegue a la meme
fonction : la meme entite se retrouve dans deux seaux dans les deux
moteurs.

L'echec concret pour l'utilisateur
----------------------------------
Parcelle (zone 12, numero 7) creee avant T, modifiee apres T par une ligne
sans empreinte. Review a la date T : deux entites au lieu d'une ; celle qui
est gardee (modifiee apres T) est reconstruite a partir de la seule image
avant de la modification, l'autre (creee avant T, avec tous ses attributs)
est jetee comme "non modifiee depuis T". Le Rewind planifie de meme sur
deux chaines, et la regle "INSERT puis DELETE = rien a faire" ne voit plus
la paire.

Pur moteur (pas de couche) : evenements formes comme la capture v7.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rvw_composite_key_fallback"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FP = "ds_rvw_composite_key"
_IDENT = json.dumps({"fid": 5, "pk_fields": ["zone", "parcelle"],
                     "pk_values": [12, 7]})
_T = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def _event(eid, op, created, attrs, entity_fp, geom=None, new_geom=None):
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=eid, project_fingerprint="p", datasource_fingerprint=_FP,
        layer_id_snapshot="l", layer_name_snapshot="parcelles",
        provider_type="postgres", feature_identity_json=_IDENT,
        operation_type=op, attributes_json=json.dumps(attrs),
        geometry_wkb=geom, geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json="[]", user_name="u", session_id="s",
        created_at=created, restored_from_event_id=None,
        entity_fingerprint=entity_fp, event_schema_version=7,
        new_geometry_wkb=new_geom)


def setup(ctx):
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.logger import flog
    ctx.data["canonical"] = compute_entity_fingerprint(_IDENT)
    flog(f"rvw_composite_key_fallback setup: trace_id={ctx.trace_id} "
         f"canonical={ctx.data['canonical']}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.rewind_dedup import _entity_key
    from recoverland.core.temporal_snapshot_engine import (
        compute_entity_key, reconstruct_snapshot_at,
    )
    d = ctx.data
    canonical = d["canonical"]
    insert = _event(1, "INSERT", "2026-04-01T10:00:00+00:00",
                    {"all_attributes": {"zone": 12, "parcelle": 7,
                                        "nom": "Ancien", "surface": 70.0}},
                    canonical, geom=b"WKB_P7")
    # Written without its fingerprint (re-imported row): only the identity.
    update = _event(2, "UPDATE", "2026-06-01T10:00:00+00:00",
                    {"changed_only": {"nom": {"old": "Ancien", "new": "Nouveau"}}},
                    None)
    d["review_key_null_fp"] = compute_entity_key(None, _IDENT)
    d["rewind_keys"] = sorted({_entity_key(insert), _entity_key(update)})
    res = reconstruct_snapshot_at({_FP: [insert, update]}, _T,
                                  trace_id=ctx.trace_id)
    feats = res.features.get(_FP, {})
    d["keys_at_t"] = sorted(feats)
    one = feats.get(canonical)
    d["state_at_t"] = json.loads(one.attrs_json) if one and one.attrs_json else None
    d["geom_at_t"] = one.geom_wkb if one else None
    flog(f"rvw_composite_key_fallback run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    canonical = d.get("canonical")
    out = []
    out.append((
        "precondition_composite_identity_has_pkc_form",
        isinstance(canonical, str) and canonical.startswith("pkc:"),
        f"empreinte canonique={canonical!r} (attendu pkc:...)",
    ))
    out.append((
        "review_key_without_fingerprint_is_the_canonical_one",
        d.get("review_key_null_fp") == canonical,
        f"cle Review d'un evenement sans empreinte={d.get('review_key_null_fp')!r}, "
        f"cle canonique={canonical!r} : deux cles pour une parcelle, Review la "
        f"montre deux fois ou jette la moitie de son histoire",
    ))
    out.append((
        "rewind_sees_one_entity",
        d.get("rewind_keys") == [f"{_FP}::{canonical}"],
        f"seaux du Rewind={d.get('rewind_keys')} : un seul attendu, sinon la "
        f"chaine de la parcelle est compensee en deux morceaux",
    ))
    state = d.get("state_at_t") or {}
    out.append((
        "review_rebuilds_one_entity_with_its_state_at_t",
        d.get("keys_at_t") == [canonical] and state.get("nom") == "Ancien"
        and state.get("surface") == 70.0 and d.get("geom_at_t") == "WKB_P7".encode(),
        f"entites a T={d.get('keys_at_t')} etat={state} : attendu une seule "
        f"entite {canonical} avec nom=Ancien, surface=70 et sa geometrie",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_composite_key_fallback.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
