# Politique de support

Plugin : RecoverLand 4.5.0
Date : 2026-04-27

## Versions declarees

| Champ | Valeur |
|-------|--------|
| `qgisMinimumVersion` | `3.40` |
| `qgisMaximumVersion` | `4.99` |

## Verification CI

A chaque push sur `main` ou `develop` :

- Tests unitaires Python 3.10 et 3.12 (mock QGIS) : `tests/`.
- Smoke runtime sous QGIS reel via Docker : `tests/test_qgis_runtime_smoke.py`
  et `tests/test_compat.py`, sur trois images floating
  (`qgis/qgis:ltr`, `qgis/qgis:latest`, `qgis/qgis:4.0-trixie`).

Aucun tag patch ou minor n'est fixe dans la matrice. Quand QGIS
publie une nouvelle LTR ou une nouvelle stable, la CI suit
automatiquement.

## Versions hors metadata

`compat.py` reste defensif sur les versions anterieures a 3.40 (3.22 -
3.38) et sur les builds dev/nightly :

- Les resolveurs gerent l'absence de `Qgis.GeometryType` (introduit
  3.30) en retombant sur `QgsWkbTypes`.
- Les resolveurs gerent l'absence de `Qgis.WkbType` (introduit 3.30)
  en retombant sur `QgsWkbTypes.NoGeometry`.
- Le shim `qgis.PyQt` couvre les variations Qt5 / Qt6.

Une installation 3.30 - 3.38 chargera donc le plugin sans crash, mais
la baseline declaree reste 3.40 : le support officiel commence a 3.40.

## Procedure pour bumper la baseline

1. Mettre a jour `qgisMinimumVersion` dans `metadata.txt`.
2. Retirer dans `compat.py` les fallbacks devenus inutiles (avec un
   commentaire dans le commit indiquant la version retiree).
3. Verifier `tests/test_compat.py` : retirer les tests de fallback
   correspondants si ceux-ci ne sont plus declenchables.
4. Annoncer le changement dans `changelog`.

## Procedure pour declarer 5.0+

1. Lancer la CI matrix sur `qgis/qgis:5.X-trixie` (image dev) ou
   equivalent.
2. Auditer les ruptures d'API et les ajouter dans `compat.py`.
3. Bumper `qgisMaximumVersion`.

## Verification manuelle pre-release

Voir `manual_test_runbook.md`.
