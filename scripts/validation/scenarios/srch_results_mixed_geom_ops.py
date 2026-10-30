# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""srch_results_mixed_geom_ops - la recherche "Toutes" sur des evenements meles.

Constat du 29/10/2026 (7.1.4, QGIS 4.2.1, couche Canalisations, operation
"Toutes") : la recherche plantait sur KeyError: 'changed_only' des que les
resultats melaient une modification de geometrie et un INSERT ou un DELETE.

Cause : `_populate_results_table` calculait `is_geom_only` avec
`all((is_update, parsed_data is not None, "changed_only" in parsed_data,
all(... parsed_data["changed_only"])))`. Le tuple est construit avant que
`all` ne l'examine : `parsed_data["changed_only"]` etait lu pour chaque
ligne, y compris un INSERT ou un DELETE, qui portent `all_attributes` et
pas `changed_only`. La colonne "Geometrie" n'existe que si un evenement
de la page touche la geometrie ; la recherche legere y met le marqueur
b'\\x00' pour toute geometrie presente, donc tout INSERT ou DELETE
geometrique l'ouvre. Le meme tuple levait TypeError sur un UPDATE sans
attributes_json lisible (parsed_data None). Introduit le 22/06/2026 par
dfc64f9 (mise au propre flake8 W503/W504).

Prouve ici avec la vraie methode du dialogue, `_populate_results_table`
(et `_build_event_diff_tooltip`), sur un porteur qui n'a que ce qu'elle
lit : un vrai QTableWidget et les widgets du groupe Resultats.

  A  la page melee de la recherche "Toutes" : UPDATE geometrique seul
     (changed_only vide, puis audit seul), UPDATE attributaire, INSERT et
     DELETE geometriques ; aucune exception, une ligne par evenement, la
     colonne "Geometrie" juste ligne par ligne.
  B  une page sans aucun UPDATE (INSERT et DELETE geometriques seuls) :
     la colonne s'ouvre sur le seul marqueur de geometrie, rien ne plante.
  C  un UPDATE dont attributes_json est vide, sur une page qui a la
     colonne : la ligne s'affiche, sans TypeError.

Verdict avant correctif : FAIL (A, B, C). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCENARIO_ID = "srch_results_mixed_geom_ops"
INVARIANT = "I-SEARCH-RESULTS"

EXPECTED_SIGNATURE = r"srch_results_mixed_geom_ops run: trace_id="

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

# The light search hands this marker for any geometry present (BL-PERF-003).
_GEOM = b"\x00"
_GEOM_LABEL = "Modifiée"


def _event(event_id, op, attributes, geometry_wkb):
    from recoverland.core.audit_backend import AuditEvent
    if isinstance(attributes, str):
        attributes_json = attributes
    else:
        attributes_json = json.dumps(attributes)
    return AuditEvent(
        event_id=event_id, project_fingerprint="prj_srch",
        datasource_fingerprint="ds_srch_canalisations",
        layer_id_snapshot="canalisations_1", layer_name_snapshot="Canalisations",
        provider_type="ogr", feature_identity_json=json.dumps({"fid": event_id}),
        operation_type=op, attributes_json=attributes_json,
        geometry_wkb=geometry_wkb, geometry_type="LineString",
        crs_authid="EPSG:2154", field_schema_json="[]", user_name="scenario",
        session_id="s1", created_at=f"2026-10-29T08:{event_id:02d}:00Z",
        restored_from_event_id=None, event_schema_version=7)


def _page_mixed():
    """Newest first, as the search returns them."""
    return [
        _event(6, "DELETE", {"all_attributes": {
            "id": 6, "diametre": 160, "materiau": "Fonte"}}, _GEOM),
        _event(5, "INSERT", {"all_attributes": {
            "id": 5, "diametre": 110, "materiau": "PEHD"}}, _GEOM),
        _event(4, "UPDATE", {"changed_only": {
            "diametre": {"old": 90, "new": 110}}}, None),
        _event(3, "UPDATE", {"changed_only": {
            "date_modif": {"old": "2026-10-28", "new": "2026-10-29"}}}, _GEOM),
        _event(2, "UPDATE", {"changed_only": {}}, _GEOM),
    ]


def _page_no_update():
    return [
        _event(12, "DELETE", {"all_attributes": {"id": 12, "diametre": 63}}, _GEOM),
        _event(11, "INSERT", {"all_attributes": {"id": 11, "diametre": 63}}, _GEOM),
    ]


def _page_unreadable_update():
    return [
        _event(22, "UPDATE", "", None),
        _event(21, "UPDATE", {"changed_only": {}}, _GEOM),
    ]


def _carrier():
    """What `_populate_results_table` reads on the dialog, nothing more."""
    from qgis.PyQt.QtWidgets import (QCheckBox, QLabel, QLineEdit,
                                     QPushButton, QTableWidget, QWidget)
    from qgis.gui import QgsCollapsibleGroupBox
    from recoverland.recover_dialog import RecoverDialog

    class _Carrier:
        _build_event_diff_tooltip = RecoverDialog._build_event_diff_tooltip

        def __init__(self):
            self.table_widget = QTableWidget()
            self.results_info_label = QLabel()
            self.results_group = QgsCollapsibleGroupBox()
            self.select_all_button = QPushButton()
            self.select_none_button = QPushButton()
            self.search_filter = QLineEdit()
            self.change_legend = QWidget()
            self._modified_only_check = QCheckBox()
            self._modified_col_indices = set()
            self._all_attr_keys = []

        @staticmethod
        def tr(text):
            return text

    return _Carrier()


def _show(events):
    """Fill the table as the search does; return what the user would see."""
    from recoverland.recover_dialog import RecoverDialog
    carrier = _carrier()
    try:
        RecoverDialog._populate_results_table(carrier, events, len(events))
    except Exception as exc:  # noqa: BLE001 - the crash is the finding
        return {"crash": f"{type(exc).__name__}: {exc}"}
    table = carrier.table_widget
    headers = [table.horizontalHeaderItem(c).text()
               for c in range(table.columnCount())]
    out = {"crash": None, "rows": table.rowCount(), "headers": headers,
           "geom": {}, "tooltips": {}}
    if "Géométrie" in headers:
        g_col = headers.index("Géométrie")
        for r in range(table.rowCount()):
            eid = table.item(r, 0).text()
            out["geom"][eid] = table.item(r, g_col).text()
            out["tooltips"][eid] = table.item(r, 3).toolTip()
    return out


def setup(ctx):
    ctx.data.clear()


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    d["A"] = _show(_page_mixed())
    d["B"] = _show(_page_no_update())
    d["C"] = _show(_page_unreadable_update())
    flog(f"srch_results_mixed_geom_ops run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str, ensure_ascii=True)[:3500]}", "INFO")


def _no_crash(key, d):
    crash = d[key].get("crash")
    return (f"{key}_no_crash", crash is None,
            "results table filled" if crash is None else f"raised {crash}")


def _geom_column(key, d, expected):
    got = d[key].get("geom", {})
    return (f"{key}_geometry_column", got == expected,
            f"expected {expected}, got {got}")


def assertions(ctx):
    d = ctx.data
    out = [_no_crash("A", d), _no_crash("B", d), _no_crash("C", d)]
    if d["A"].get("crash") is None:
        out.append(("A_one_row_per_event", d["A"]["rows"] == 5,
                    f"rows={d['A']['rows']}"))
        out.append(_geom_column("A", d, {
            "6": "", "5": "", "4": "", "3": _GEOM_LABEL, "2": _GEOM_LABEL}))
        tip = d["A"]["tooltips"].get("5", "")
        out.append(("A_insert_tooltip_lists_values",
                    "Valeurs creees" in tip and "PEHD" in tip, tip[:200]))
    if d["B"].get("crash") is None:
        out.append(_geom_column("B", d, {"12": "", "11": ""}))
    if d["C"].get("crash") is None:
        out.append(_geom_column("C", d, {"22": "", "21": _GEOM_LABEL}))
    return out
