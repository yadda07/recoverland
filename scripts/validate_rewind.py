"""
validate_rewind.py  -  Validation ground-truth du Rewind RecoverLand
=====================================================================
Compare l'état courant des couches avec le snapshot JSON sauvegardé
par stress_edit.py AVANT ses éditions.

Si le Rewind a fonctionné correctement (cutoff < snapshot_time),
l'état des couches doit correspondre exactement au snapshot.

Usage dans la console Python QGIS :

    exec(open('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validate_rewind.py').read())

Options :
    SNAPSHOT_PATH   = chemin explicite vers un JSON (None = dernier connu)
    MAX_DETAIL      = nombre max de FIDs à afficher par catégorie d'écart
    CHECK_GEOM      = True pour comparer les géométries (plus lent)
    CHECK_ATTRS     = True pour comparer les attributs
    SKIP_FIELDS     = noms de champs à ignorer lors de la comparaison
"""

import json
import os
from datetime import datetime, timezone

from qgis.core import (
    QgsProject,
    QgsVectorLayer,
)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
SNAPSHOT_PATH = None        # None = charge stress_snapshot_latest.json auto
MAX_DETAIL    = 10          # nb max de FIDs listés par catégorie d'écart
CHECK_GEOM    = True        # comparer les géométries
CHECK_ATTRS   = True        # comparer les attributs
AUTO_REWIND   = True        # True = déclenche le rewind avec cutoff=snapshot_time
                            # avant la comparaison. False = compare l'état actuel
                            # tel quel (cas où le user a déjà fait son rewind UI).
SKIP_FIELDS   = [
    "date_modif",   # timestamp auto-géré par trigger DB à chaque écriture (faux positif certain)
    "gid",          # compteur auto-géré par trigger DB à chaque écriture (seq interne PostgreSQL)
    "modif_par",    # username auto-géré par trigger DB à chaque écriture (faux positif certain)
                    # NB: le FID QGIS (clé de suivi des entités) n'est JAMAIS exclu ici
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _scripts_dir():
    """Répertoire scripts/ du plugin — lisible directement par Cascade."""
    return os.path.dirname(os.path.abspath(__file__))


def _load_snapshot(path=None):
    if path is None:
        path = os.path.join(_scripts_dir(), "stress_snapshot_latest.json")
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Snapshot introuvable : {path}\n"
            "Lance d'abord stress_edit.py pour créer un snapshot."
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh), path


def _find_layer(name):
    for layer in QgsProject.instance().mapLayers().values():
        if isinstance(layer, QgsVectorLayer) and layer.name() == name:
            return layer
    return None


def _current_features(layer):
    """Retourne {str(fid): {'attrs': {...}, 'geom_wkt': ...}} pour la couche."""
    result = {}
    for feat in layer.getFeatures():
        fid = feat.id()
        geom = feat.geometry()
        attrs = {}
        for field in feat.fields():
            if field.name() in SKIP_FIELDS:
                continue
            val = feat[field.name()]
            if hasattr(val, 'isNull') and val.isNull():
                attrs[field.name()] = None
            elif isinstance(val, (int, float, str, bool, type(None))):
                attrs[field.name()] = val
            else:
                attrs[field.name()] = str(val)
        result[str(fid)] = {
            "attrs": attrs,
            "geom_wkt": geom.asWkt(6) if geom and not geom.isNull() else None,
        }
    return result


def _attrs_equal(snap_attrs, cur_attrs):
    """Compare deux dicts d'attributs. None et valeur manquante sont équivalents."""
    all_keys = set(snap_attrs) | set(cur_attrs)
    all_keys -= set(SKIP_FIELDS)
    for k in all_keys:
        sv = snap_attrs.get(k)
        cv = cur_attrs.get(k)
        if sv != cv:
            return False, k, sv, cv
    return True, None, None, None


def _geom_equal(snap_wkt, cur_wkt):
    if snap_wkt is None and cur_wkt is None:
        return True
    if snap_wkt is None or cur_wkt is None:
        return False
    return snap_wkt == cur_wkt


def _fmt_list(items, max_n):
    shown = [str(x) for x in items[:max_n]]
    suffix = f"  (+{len(items) - max_n} autres)" if len(items) > max_n else ""
    return ", ".join(shown) + suffix


# ---------------------------------------------------------------------------
# AUTO-REWIND (déclenche le rewind avec cutoff = snapshot_time)
# ---------------------------------------------------------------------------

# BL-RW-P1-09: latest auto-rewind breakdown, populated by
# trigger_rewind_to_snapshot() and consumed by validate_rewind() so the
# text report exposes per-category counters separately.
_LAST_REWIND_BREAKDOWN: dict = {}

def _get_recoverland_plugin():
    """Retourne l'instance du plugin RecoverLand, ou lève RuntimeError."""
    import qgis.utils
    for name, plugin in dict(qgis.utils.plugins).items():
        if name == 'recoverland' or type(plugin).__name__ == 'RecoverPlugin':
            return plugin
    raise RuntimeError(
        "Plugin RecoverLand introuvable dans qgis.utils.plugins. "
        "Vérifie que le plugin est activé."
    )


def _snapshot_time_to_utc_iso(snap_time_iso):
    """Convertit 'YYYY-MM-DDTHH:MM:SS' (heure locale) en UTC ISO compatible
    avec les events du journal RecoverLand (qui sont en UTC)."""
    local_dt = datetime.fromisoformat(snap_time_iso)
    # astimezone() sans argument attache la timezone locale puis convertit en UTC
    utc_dt = local_dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S")


def trigger_rewind_to_snapshot(snapshot_path=None):
    """Déclenche un rewind RecoverLand jusqu'au snapshot_time du JSON.

    Évite d'avoir à régler manuellement le slider du dialog: le cutoff
    est lu depuis stress_snapshot_latest.json (champ snapshot_time) puis
    converti en UTC. StrictRestoreRunner est lancé dans une QEventLoop
    pour bloquer jusqu'à la fin.
    """
    from qgis.PyQt.QtCore import QEventLoop
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.restore_runner import StrictRestoreRunner
    from recoverland.core.workflow_service import find_target_layer

    snap, _ = _load_snapshot(snapshot_path or SNAPSHOT_PATH)
    snap_time_iso = snap.get("snapshot_time")
    if not snap_time_iso:
        raise RuntimeError("snapshot.json: champ 'snapshot_time' absent")

    cutoff_iso = _snapshot_time_to_utc_iso(snap_time_iso)
    print(f"[auto-rewind] snapshot_time={snap_time_iso} (local) "
          f"→ cutoff={cutoff_iso} (UTC)")

    plugin = _get_recoverland_plugin()
    if not getattr(plugin, '_journal', None):
        raise RuntimeError("Plugin RecoverLand non-initialisé (pas de _journal)")
    if not getattr(plugin, '_tracker', None):
        raise RuntimeError("Plugin RecoverLand non-initialisé (pas de _tracker)")
    if not getattr(plugin, '_write_queue', None):
        raise RuntimeError("Plugin RecoverLand non-initialisé (pas de _write_queue)")

    # Inclusive=True: same-second events at the snapshot boundary are
    # compensated (cf. core.restore_contracts and SESSION_REWIND.md §17.1).
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)

    fingerprints = list(set(plugin._tracker._layer_fingerprints.values()))
    print(f"[auto-rewind] {len(fingerprints)} fingerprint(s) tracked par le plugin")

    conn = plugin._journal.create_read_connection()
    events = []
    try:
        for fp in fingerprints:
            events.extend(fetch_events_after_cutoff(
                conn, fp, cutoff, include_traces=True))
    finally:
        conn.close()

    events.sort(key=lambda e: (e.created_at or "", e.event_id or 0), reverse=True)
    print(f"[auto-rewind] {len(events)} event(s) à annuler après cutoff")

    if not events:
        print("[auto-rewind] rien à annuler — état déjà au snapshot")
        return {"total_ok": 0, "total_fail": 0}

    read_conn = plugin._journal.create_read_connection()

    def resolver(evt):
        return find_target_layer(evt, read_conn)

    runner = StrictRestoreRunner(
        events, resolver, cutoff,
        write_queue=plugin._write_queue,
        tracker=plugin._tracker,
        trace_id="validate_auto",
    )

    loop = QEventLoop()
    result_holder = {}

    def _on_finished(res):
        result_holder['result'] = res
        loop.quit()

    runner.finished.connect(_on_finished)
    runner.start()
    loop.exec_()

    try:
        read_conn.close()
    except Exception:
        pass

    result = result_holder.get('result')
    if result is None:
        print("[auto-rewind] runner terminé sans résultat (?)")
        return {"total_ok": 0, "total_fail": 0}

    # BL-RW-P1-09: propagate the 5-bucket breakdown so the validation
    # report distinguishes target_absent / geometry_drift from generic
    # failures. Defaults keep the result valid for legacy callers that
    # do not populate the breakdown.
    applied = int(getattr(result, "applied", 0) or 0)
    skipped_idempotent = int(getattr(result, "skipped_idempotent", 0) or 0)
    failed_other = int(getattr(result, "failed", 0) or 0)
    failed_target_absent = int(getattr(result, "failed_target_absent", 0) or 0)
    failed_geometry_drift = int(
        getattr(result, "failed_geometry_drift", 0) or 0
    )

    print(
        f"[auto-rewind] termine: ok={result.total_ok} "
        f"fail={result.total_fail} "
        f"applied={applied} skipped_idempotent={skipped_idempotent} "
        f"failed={failed_other} "
        f"failed_target_absent={failed_target_absent} "
        f"failed_geometry_drift={failed_geometry_drift} "
        f"errors={len(result.errors or [])}"
    )
    if result.errors:
        for err in result.errors[:5]:
            print(f"  err: {err}")

    # Force la mise a jour du canvas pour que la validation voie l'etat post-rewind
    try:
        from qgis.utils import iface
        iface.mapCanvas().refresh()
    except Exception:
        pass

    breakdown = {
        "total_ok": result.total_ok,
        "total_fail": result.total_fail,
        "errors": list(result.errors or []),
        "applied": applied,
        "skipped_idempotent": skipped_idempotent,
        "failed": failed_other,
        "failed_target_absent": failed_target_absent,
        "failed_geometry_drift": failed_geometry_drift,
    }
    # BL-RW-P1-09: expose the breakdown to validate_rewind() via module scope.
    global _LAST_REWIND_BREAKDOWN
    _LAST_REWIND_BREAKDOWN = dict(breakdown)
    return breakdown


# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------

def validate_rewind(snapshot_path=None):
    snap, loaded_path = _load_snapshot(snapshot_path or SNAPSHOT_PATH)
    snap_time = snap.get("snapshot_time", "?")

    lines = []
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  VALIDATION REWIND  vs  snapshot {snap_time}")
    lines.append(f"  Fichier : {os.path.basename(loaded_path)}")
    lines.append("=" * 70)

    total_expected = 0
    total_ok = 0
    total_missing = 0
    total_extra = 0
    total_changed = 0
    layers_ok = 0
    layers_ko = 0
    layers_absent = 0

    for layer_name, layer_snap in snap["layers"].items():
        snap_feats = layer_snap["features"]
        expected_n = layer_snap["feature_count"]
        total_expected += expected_n

        layer = _find_layer(layer_name)
        if layer is None:
            lines.append(f"[---] {layer_name:<30} : couche absente du projet QGIS")
            layers_absent += 1
            continue

        cur_feats = _current_features(layer)
        snap_fids = set(snap_feats.keys())
        cur_fids  = set(cur_feats.keys())

        missing = sorted(snap_fids - cur_fids, key=int)
        extra   = sorted(cur_fids  - snap_fids, key=int)
        common  = snap_fids & cur_fids

        changed_fids = []
        changed_details = {}
        for fid in common:
            sf = snap_feats[fid]
            cf = cur_feats[fid]
            issues = []
            if CHECK_ATTRS:
                ok, field, sv, cv = _attrs_equal(
                    {k: v for k, v in sf["attrs"].items() if k not in SKIP_FIELDS},
                    cf["attrs"],
                )
                if not ok:
                    issues.append(f"attr '{field}': snap={sv!r} now={cv!r}")
            if CHECK_GEOM:
                if not _geom_equal(sf.get("geom_wkt"), cf.get("geom_wkt")):
                    issues.append("geom diff")
            if issues:
                changed_fids.append(fid)
                changed_details[fid] = issues[0]

        n_missing = len(missing)
        n_extra   = len(extra)
        n_changed = len(changed_fids)
        n_ok_feat = expected_n - n_missing - n_changed

        total_missing += n_missing
        total_extra   += n_extra
        total_changed += n_changed
        total_ok      += max(n_ok_feat, 0)

        if n_missing == 0 and n_extra == 0 and n_changed == 0:
            lines.append(f"[OK ] {layer_name:<30} : {expected_n}/{expected_n} features OK")
            layers_ok += 1
        else:
            status = "PART" if n_ok_feat > 0 else "FAIL"
            lines.append(
                f"[{status}] {layer_name:<30} : "
                f"{n_ok_feat}/{expected_n} OK  "
                f"|  -{n_missing} manquants  "
                f"+{n_extra} surplus  "
                f"~{n_changed} modifiés"
            )
            if n_missing:
                lines.append(
                    f"       MANQUANTS (devaient exister) : "
                    f"fid={_fmt_list(missing, MAX_DETAIL)}"
                )
            if n_extra:
                lines.append(
                    f"       SURPLUS   (ne devaient pas)  : "
                    f"fid={_fmt_list(extra, MAX_DETAIL)}"
                )
            if n_changed:
                details = [
                    f"fid={fid} [{changed_details[fid]}]"
                    for fid in changed_fids[:MAX_DETAIL]
                ]
                suffix = f" (+{n_changed - MAX_DETAIL} autres)" if n_changed > MAX_DETAIL else ""
                lines.append(
                    "       MODIFIÉS  (valeur diff)      : "
                    + ", ".join(details) + suffix
                )
            layers_ko += 1

    lines.append("-" * 70)
    score_pct = (total_ok / total_expected * 100) if total_expected else 0.0
    lines.append(
        f"  COUCHES : {layers_ok} OK  {layers_ko} KO  {layers_absent} absentes"
    )
    lines.append(
        f"  FEATURES: {total_ok}/{total_expected} correctes ({score_pct:.1f}%)"
        f"  |  -{total_missing} manquants  +{total_extra} surplus  ~{total_changed} modifiés"
    )
    # BL-RW-P1-09: append the per-category breakdown of the latest rewind.
    if _LAST_REWIND_BREAKDOWN:
        bd = _LAST_REWIND_BREAKDOWN
        lines.append(
            f"  REWIND  : applied={bd.get('applied', 0)} "
            f"skipped_idempotent={bd.get('skipped_idempotent', 0)} "
            f"failed={bd.get('failed', 0)} "
            f"failed_target_absent={bd.get('failed_target_absent', 0)} "
            f"failed_geometry_drift={bd.get('failed_geometry_drift', 0)}"
        )
    if total_missing == 0 and total_extra == 0 and total_changed == 0 and layers_absent == 0:
        lines.append("  VERDICT : REWIND PARFAIT — état identique au snapshot")
    elif score_pct >= 95.0:
        lines.append(f"  VERDICT : REWIND QUASI-COMPLET — {total_missing + total_extra + total_changed} écart(s) mineurs")
    else:
        lines.append(f"  VERDICT : REWIND INCOMPLET — {total_missing + total_extra + total_changed} écart(s) significatifs")
    lines.append("=" * 70)
    lines.append("")

    report = "\n".join(lines)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    d = _scripts_dir()
    report_path = os.path.join(d, "rewind_report_latest.txt")
    ts_path = os.path.join(d, f"rewind_report_{ts}.txt")
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(report)
    with open(ts_path, "w", encoding="utf-8") as fh:
        fh.write(report)

    print(f"[validate_rewind] rapport sauvegardé : {report_path}")

    return {
        "layers_ok": layers_ok,
        "layers_ko": layers_ko,
        "layers_absent": layers_absent,
        "total_ok": total_ok,
        "total_expected": total_expected,
        "total_missing": total_missing,
        "total_extra": total_extra,
        "total_changed": total_changed,
        "score_pct": score_pct,
    }


# ---------------------------------------------------------------------------
# AUTO-RUN
# ---------------------------------------------------------------------------
def _auto_run():
    if AUTO_REWIND:
        try:
            trigger_rewind_to_snapshot()
        except Exception as exc:
            print(f"[auto-rewind] ABORT: {type(exc).__name__}: {exc}")
            print("[auto-rewind] validation faite sur l'état courant (sans rewind auto)")
    validate_rewind()


if __name__ == "__main__":
    _auto_run()
