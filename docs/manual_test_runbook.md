# Runbook de test manuel

Plugin : RecoverLand 4.5.0
Cible : une installation Qt5 (QGIS 3.40+ LTR) et une installation Qt6
(QGIS 4.0+).

## Pre-requis

- Plugin installe dans le profil par defaut.
- Une couche test geopackage (~50 entites, melange points/lignes/polygones).
- Optionnel : une couche PostgreSQL accessible via une connexion
  enregistree dans QGIS.

## 1. Environnement

Console Python QGIS :

```python
from recoverland.compat import get_environment_info, qgis_version_info, is_qt6
print(get_environment_info())
print(qgis_version_info())
print(is_qt6())
```

Attendu : aucune exception. `qgis_version_info()` retourne un
`QgisVersion` non nul. `is_qt6()` retourne `True` ou `False` selon le
binding.

## 2. Activation

Plugin Manager : cocher RecoverLand. Verifier l'absence de message
`Critical` dans le panel Log Messages, onglet `RecoverLand`.

## 3. Capture

Activer le tracking, passer la couche en edition, faire :

- une modification d'attribut,
- une modification de geometrie,
- une suppression.

Sauvegarder. Le panel `RecoverLand` doit afficher trois captures.

## 4. Recherche

Ouvrir la dialog principale, onglet `Search`. Verifier la pagination,
le filtre par date, le filtre par type d'operation.

## 5. Restauration evenement

Selectionner l'evenement DELETE, cliquer `Restore`. La preflight doit
afficher le plan ; confirmer. L'entite supprimee reapparait sur la
couche. Un trace event est ecrit.

## 6. Restauration temporelle

Onglet `Temporal`, deplacer le slider sur une date anterieure, lancer
la restauration. La couche revient a l'etat anterieur.

## 7. Preview de geometrie sur le canvas

Selectionner un evenement UPDATE de geometrie, cliquer
`Preview on canvas`. Une rubber band rouge s'affiche.

Test critique sous Qt6 : aucune exception
`TypeError: argument 2 must be Qgis.GeometryType, not int`. Si elle
apparait, `compat.QgisCompat.GEOM_*` ne resout pas correctement.

## 8. Maintenance

Menu RecoverLand -> `Journal Maintenance`. Lancer integrity check,
vacuum, export, purge par age. Aucune exception non geree.

## 9. PostgreSQL (si dispo)

Tracker une couche PG, supprimer une entite, restaurer. Verifier que
les credentials viennent de QGIS saved connections (jamais persistes).
Aucun mot de passe dans les logs.

## 10. Theme

Bascule clair/sombre. Les widgets restent lisibles.

## 11. Volumetrie

Sur 10000 entites : modifier 100 entites en une transaction. Le
tracking absorbe la salve sans bloquer l'UI plus de 2 secondes. Le
journal contient 100 evenements UPDATE.

## 12. Shutdown

Fermer QGIS, rouvrir. Le plugin recharge sans erreur. Le journal du
projet precedent est accessible.

## 13. Scenarios non-regression hypotheses (BL-RW-P3-19)

Les trois scenarios runtime ci-dessous reproduisent les hypotheses
H-V1, H-V2, H-T1 documentees dans `SESSION_REWIND.md` chap. 17. Ils
sont localises dans `scripts/validation/scenarios/hypotheses/`
(toolkit interne, gitignored). Chacun emet exactement une ligne de
verdict :

```
hypothesis_h_v1: status=VALIDATED|FALSIFIED|UNREPRODUCED reason=...
hypothesis_h_v2: status=VALIDATED|FALSIFIED|UNREPRODUCED reason=...
hypothesis_h_t1: status=VALIDATED|FALSIFIED|UNREPRODUCED reason=...
```

Lancement console Python QGIS :

```python
from scripts.validation.runner import run_scenario
run_scenario('scripts/validation/scenarios/hypotheses/h_v1_trace_invalidated.py')
run_scenario('scripts/validation/scenarios/hypotheses/h_v2_fuse_long_chain.py')
run_scenario('scripts/validation/scenarios/hypotheses/h_t1_phase_order.py')
```

Verdicts attendus en l etat actuel du code :

- `h_v1` : sortie `FALSIFIED` ou `UNREPRODUCED` (le pipeline filtre les
  trace events avec ou sans `invalidated_at` ; la branche orpheline n est
  plus silencieusement laissee passer).
- `h_v2` : sortie `FALSIFIED` (le synthetic preserve `feature_identity_json`,
  `entity_fingerprint` et `new_geometry_wkb` du newest, `geometry_wkb`
  du oldest).
- `h_t1` : sortie `VALIDATED` (le risque structurel decrit par H-T1
  reste theorique ; la mitigation est runtime via `fid_remap`).

Tout autre triplet de verdicts doit etre traite comme une regression et
escalade en backlog.
