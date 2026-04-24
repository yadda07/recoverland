# Audit de compatibilite QGIS 3.44 - 4.0

Date: 2026-04-24
Scope: tous les fichiers Python du plugin RecoverLand (hors tests/)

## Resume

| Severite | Compte |
|----------|--------|
| HAUTE    | 3      |
| MOYENNE  | 4      |
| BASSE    | 3      |
| OK       | ~30 fichiers sans probleme |

Le plugin est globalement bien prepare grace a `compat.py` (`QtCompat` + `QgisCompat`).
3 problemes bloquants et 4 points a renforcer restent a corriger.

---

## HAUTE (bloquant en QGIS 4.0)

### COMPAT-H01 : `QgsFeatureRequest.NoGeometry` non scope dans `restore_service.py`

- **Fichier**: `core/restore_service.py:220`
- **Code**: `request.setFlags(QgsFeatureRequest.NoGeometry)`
- **Probleme**: QGIS 4.0 scope les enums: `QgsFeatureRequest.Flag.NoGeometry`.
  L'acces court `QgsFeatureRequest.NoGeometry` leve `AttributeError` sous PyQt6/QGIS 4.0.
- **Fix**: Utiliser `QgisCompat.NO_GEOMETRY` deja defini dans `compat.py:150-152`.
- **Impact**: `build_fid_cache()` crashe a chaque restore batch.

### COMPAT-H02 : `_EDIT_CAPABILITIES_MASK` magic numbers dans `support_policy.py`

- **Fichier**: `core/support_policy.py:78`
- **Code**: `_EDIT_CAPABILITIES_MASK = 1 | 2 | 4 | 8`
- **Probleme**: En QGIS 4.0, `QgsVectorDataProvider.Capability` est un vrai flag enum scope.
  Les valeurs internes `1|2|4|8` ne sont plus garanties stables entre versions majeures.
  Le masque devrait etre construit a partir des constantes symboliques.
- **Fix**: Utiliser `QgisCompat.CAP_ADD_FEATURES | CAP_DELETE_FEATURES | CAP_CHANGE_ATTRIBUTE_VALUES | CAP_CHANGE_GEOMETRIES`.
- **Impact**: `_has_edit_capabilities()` peut refuser ou accepter des layers a tort.

### COMPAT-H03 : `Qgis.MessageLevel` non scope dans `compat.py`

- **Fichier**: `compat.py:167-170`
- **Code**: `MSG_INFO = getattr(Qgis, 'Info', 0)` etc.
- **Probleme**: En QGIS 4.0, les niveaux sont `Qgis.MessageLevel.Info`, `Qgis.MessageLevel.Warning`, etc.
  `Qgis.Info` n'existe plus comme attribut court.
  Le `getattr` avec fallback int (0, 1, 2, 3) fonctionne mais envoie un `int` a
  `QgsMessageLog.logMessage()` qui attend un `Qgis.MessageLevel` enum, pas un int.
  Selon la stricte du binding PyQt6, cela peut lever `TypeError`.
- **Fix**: Resoudre en scope d'abord: `getattr(getattr(Qgis, 'MessageLevel', Qgis), 'Info', 0)`.
- **Impact**: Tout le logging QGIS (qlog, LoggerMixin, messageBar().pushMessage).

---

## MOYENNE (risque fonctionnel, pas de crash immediat)

### COMPAT-M01 : `metadata.txt` ne declare pas `supportsQt6` (non requis mais note)

- **Fichier**: `metadata.txt`
- **Etat**: `qgisMinimumVersion=3.44`, `qgisMaximumVersion=4.99`.
  Pas de `supportsQt6=True`.
- **Verdict**: Depuis avril 2025, `supportsQt6` est obsolete et ne doit plus etre utilise.
  La config actuelle (`qgisMaximumVersion=4.99`) est **correcte et suffisante**.
- **Action**: Aucune. Confirme: OK.

### COMPAT-M02 : `QgsRubberBand` constructeur avec int au lieu de `Qgis.GeometryType`

- **Fichier**: `core/geometry_preview.py:65`
- **Code**: `self._band = QgsRubberBand(self._canvas, band_type)`
  ou `band_type` vaut un int (fallback `_POLYGON_TYPE = 2`) quand `Qgis.GeometryType` n'existe pas.
- **Probleme**: En QGIS 4.0, le constructeur `QgsRubberBand(canvas, geomType)` attend strictement
  un `Qgis.GeometryType` enum, pas un int. Passer un int leve `TypeError` sous PyQt6 strict.
- **Guard actuel**: Le try/except aux lignes 15-23 resout le `Qgis.GeometryType.Polygon` pour 4.0,
  et retombe sur int pour 3.x. C'est correct tant que QGIS 4.0 est la cible; le fallback int
  n'est active que sur 3.28-3.36 (hors scope du min 3.44). **Pas de fix requis si min=3.44**.
- **Risque residuel**: Si un import echoue pour une autre raison, le fallback int cause un crash.
- **Fix optionnel**: Ajouter dans `compat.py` les constantes `GEOM_POINT`, `GEOM_LINE`, `GEOM_POLYGON`.

### COMPAT-M03 : `QAction` import try/except

- **Fichier**: `recover.py:1-4`
- **Code**:
  ```python
  try:
      from qgis.PyQt.QtWidgets import QAction
  except ImportError:
      from qgis.PyQt.QtGui import QAction
  ```
- **Verdict**: Correct. En Qt6/QGIS 4.0, `QAction` est dans `QtGui`. Le shim `qgis.PyQt`
  re-exporte `QAction` dans `QtWidgets` pour compatibilite. Le try/except est une ceinture+bretelles.
- **Action**: Aucune correction necessaire. Le code est deja compatible.

### COMPAT-M04 : `QDialog.exec()` vs `exec_()`

- **Fichier**: `recover_dialog.py:1160-1163`
- **Code**:
  ```python
  if hasattr(dlg, 'exec'):
      dlg.exec()
  else:
      dlg.exec_()
  ```
- **Verdict**: Correct. `exec_()` est supprime en PyQt6; `exec()` est le seul nom.
  Le guard `hasattr` gere les deux cas.
- **Fichier**: `recover_dialog.py:2274` utilise `dlg.exec()` directement.
- **Risque**: `exec()` seul fonctionne sur QGIS 3.44+ (Qt 5.15 shim disponible).
  Aucun probleme avec min=3.44.
- **Action**: Aucune.

---

## BASSE (cosmetique ou couverture defensive)

### COMPAT-B01 : `QDateTime.toSecsSinceEpoch()` / `fromSecsSinceEpoch()`

- **Fichier**: `widgets/time_slider.py:106-107, 128, 175, 184`
- **Verdict**: Ces methodes existent depuis Qt 5.8. Aucun probleme 3.44-4.0.
- **Action**: Aucune.

### COMPAT-B02 : `QgsSettings` vs `QSettings`

- **Fichiers**: `recover.py`, `recover_dialog.py`, `journal_maintenance.py`
- **Verdict**: Le plugin utilise `QgsSettings` partout (pas `QSettings` directement sauf dans
  `__init__.py:14` pour la locale). `QgsSettings` est stable 3.x-4.0. `QSettings` dans
  `__init__.py` est utilise avant l'init QGIS complete: compatible.
- **Action**: Aucune.

### COMPAT-B03 : `Qgis.QGIS_VERSION`

- **Fichier**: `compat.py:177`
- **Verdict**: Stable, pas de changement prevu.
- **Action**: Aucune.

---

## Fichiers verifies sans probleme

Les fichiers suivants utilisent soit `QtCompat`/`QgisCompat`, soit uniquement des API stables :

| Fichier | Status |
|---------|--------|
| `compat.py` (sauf H03) | Couche de compatibilite, bien construite |
| `core/edit_tracker.py` | Imports `QgsFeatureRequest` locaux, usage correct |
| `core/identity.py` | `pkAttributeIndexes()` stable 3.x-4.x |
| `core/geometry_utils.py` | `Qgis.WkbType` avec guard, OK |
| `core/datasource_registry.py` | `QgsDataSourceUri` stable |
| `core/datasource_alias.py` | Pure SQLite, aucune dep Qt/QGIS |
| `core/workflow_service.py` | `QgsProject.addMapLayer()` stable |
| `core/serialization.py` | Pure Python |
| `core/logger.py` | Via `QgisCompat`, OK (depend de H03) |
| `journal_maintenance.py` | Via `QtCompat`, OK |
| `journal_info_bar.py` | Via `QtCompat`, OK |
| `status_bar_widget.py` | Via `QtCompat`, OK |
| `widgets/toggle_switch.py` | Via `QtCompat`, OK |
| `widgets/time_slider.py` | Via `QtCompat` + `QgsDateTimeEdit`, OK |
| `widgets/restore_mode_selector.py` | Via `QtCompat`, OK |
| `widgets/restore_preflight_dialog.py` | Imports stables |
| `widgets/themed_logo.py` | Via `QtCompat`, OK |
| `themed_action_icon.py` | Via `QtCompat`, OK |
| `restore_runner.py` | `QObject`, `QTimer`, `pyqtSignal` stables |
| `qgs_task_support.py` | `QThread`, `QgsTask` stables |
| `local_search_thread.py` | Via `pyqtSignal`, OK |
| `version_fetch_thread.py` | Via `pyqtSignal`, OK |
| `core/support_policy.py` (sauf H02) | Pure enum, OK |
| `core/geometry_preview.py` (sauf M02) | Guard `Qgis.GeometryType`, OK |
| `recover.py` (sauf M03) | Imports gardes, OK |
| `recover_dialog.py` (sauf M04) | Via `QtCompat`/`QgisCompat`, OK |

---

## Plan de correction (3 items HAUTE)

1. **COMPAT-H01**: `restore_service.py:220` remplacer `QgsFeatureRequest.NoGeometry` par `QgisCompat.NO_GEOMETRY`
2. **COMPAT-H02**: `support_policy.py:78` remplacer le masque magic number par les constantes symboliques de `QgisCompat`
3. **COMPAT-H03**: `compat.py:167-170` resoudre `Qgis.MessageLevel.Info` scope d'abord, fallback ensuite
