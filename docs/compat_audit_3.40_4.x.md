# Audit de compatibilite QGIS 3.40 LTR -> 4.x

Date : 2026-04-27
Plugin : RecoverLand 4.5.0
Baseline : `qgisMinimumVersion=3.40`, `qgisMaximumVersion=4.99`

## Etat

Aucun bug bloquant ou haute severite ouvert.

## Principes

1. Tous les imports Qt passent par `qgis.PyQt`. Aucun `from PyQt5.X` ou
   `from PyQt6.X` direct dans le code source.
2. Tous les acces aux enums QGIS/Qt versions-dependants passent par
   `compat.QtCompat` ou `compat.QgisCompat`.
3. Les resolveurs essaient la forme scopee (Qt6 / QGIS 4.x), puis la
   forme courte (Qt5 / QGIS 3.x), puis un fallback documente.

## Resolveurs ajoutes en 4.5.0

| Resolveur | Source moderne | Fallback | Constantes exposees |
|-----------|----------------|----------|---------------------|
| `_resolve_geometry_type(name)` | `Qgis.GeometryType.<name>` (3.30+) | `QgsWkbTypes.GeometryType.<name>` ou `QgsWkbTypes.<name>Geometry` | `GEOM_POINT`, `GEOM_LINE`, `GEOM_POLYGON`, `GEOM_UNKNOWN`, `GEOM_NULL` |
| `_resolve_wkb_no_geometry()` | `Qgis.WkbType.NoGeometry` (3.30+) | `QgsWkbTypes.NoGeometry` | `WKB_NO_GEOMETRY` |
| `_resolve_enum(parent, scoped, name)` | `parent.<scoped>.<name>` (Qt6 / QGIS scope) | `parent.<name>` (Qt5 / QGIS short) | utilise pour `MSG_*`, `CAP_*`, et tous les enums `Qt.*` |

## Helpers

- `qgis_version_info() -> QgisVersion(major, minor, patch)` : parse
  `Qgis.QGIS_VERSION` ; tolere les suffixes `-Bratislava`, `-mock`, `-dev`.
- `QgisVersion.at_least(major, minor)` : aiguillage runtime.
- `is_qt6() -> bool` : `hasattr(Qt, 'AlignmentFlag')`.

## Modifications applicatives

- `core/geometry_preview.py` : `QgsRubberBand` recoit `QgisCompat.GEOM_*`,
  plus de `int` brut. Corrige le `TypeError` constate sous PyQt6 strict.
- `core/geometry_utils.py` : `extract_geometry_type()` detecte
  `NoGeometry` via `Qgis.WkbType` puis `QgsWkbTypes`, plus de cas
  silencieux sur les bindings sans `Qgis.WkbType`.
- `metadata.txt` : `qgisMinimumVersion=3.40` (etait 3.44).
- `compat.py` : docstring met a jour la baseline et liste les divergences
  couvertes.

## Couverture actuelle

| Composant | Strategie |
|-----------|-----------|
| Imports Qt | 100 % via `qgis.PyQt` |
| Enums Qt | 100 % via `QtCompat` hors `compat.py` |
| Enums QGIS (`GeometryType`, `WkbType`, `MessageLevel`, `Capability`) | 100 % via `QgisCompat` ou guards `hasattr` documentes |
| `QgsFeatureRequest.NoGeometry` | `QgisCompat.NO_GEOMETRY` |
| `QgsRubberBand` constructeur | `QgisCompat.GEOM_*` (jamais `int`) |
| `QDialog.exec()` vs `exec_()` | guard `hasattr` dans `recover_dialog.py` |
| `QAction` import | try `QtWidgets`, fallback `QtGui` (PyQt6) |

## Tests

- `tests/test_compat.py` : 15 tests sentinelles. Verifie chaque
  attribut `QtCompat` et `QgisCompat`, distingue `GEOM_POINT/LINE/POLYGON`,
  combine `CAP_*` avec `|`, parse de version, fallback resolveurs sous
  stub.
- `tests/test_qgis_runtime_smoke.py` : import du plugin sous QGIS reel
  (xvfb + pytest-qgis).

## CI

Trois images Docker, tags floating uniquement :

- `qgis/qgis:ltr` : LTR active (3.40 a date, suit l'evolution).
- `qgis/qgis:latest` : stable courante.
- `qgis/qgis:4.0-trixie` : dev Qt6.

Aucun tag `final-X_Y_Z` ou `release-X_Y` enumere : la matrice n'est pas
liee a un patch ou un minor specifique.

## Faux positifs lint

`Unable to import 'qgis.core'` dans `geometry_utils.py` : pylint hors
environnement QGIS ne resout pas les bindings. Imports volontairement
lazy dans les fonctions pour eviter de casser l'analyse statique en
dehors d'une installation QGIS.
