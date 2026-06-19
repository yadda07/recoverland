# RecoverLand — Charte de l'équipe

Plugin QGIS qui enregistre chaque édition d'un projet (INSERT / UPDATE / DELETE)
dans un journal SQLite local, et permet de revivre cet historique via trois
modes : **Restore** (recherche + restauration d'événements), **Rewind**
(retour temporel par versions) et **ReView** (relecture non destructive de
l'état passé : sous-modes Diff, Snapshot, Time Lens).

Ce fichier est la charte commune. Tout agent qui travaille sur ce dépôt la
respecte sans exception. Les profils détaillés vivent dans `.devin/workflows/`.

## 1. Principes non négociables

1. **Architecture d'abord.** Aucun patch ne commence sans contrat explicite :
   entrées, sorties, invariants, call-sites amont/aval. Le `/architecte` cadre
   avant que le `/corrector` ne touche une ligne.
2. **Honnêteté totale, zéro complaisance.** On ne maquille pas un résultat. On
   ne réduit pas un scope pour atteindre `PASS`. On ne dit jamais « ça marche »
   sans preuve. Un doute énoncé vaut mieux qu'un faux verdict.
3. **Aucun `PASS` sans antithèse.** Toute thèse (« le bug est corrigé »,
   « l'invariant tient ») survit à ≥3 antithèses concrètes dont ≥1 implémentée
   comme assertion brutale. Voir `/antithese`.
4. **Preuve = logs runtime QGIS.** La validation se fait dans la console QGIS via
   `flog` / `flog_kv`, pas avec des mocks pytest hors-QGIS. Un run silencieux
   n'est pas une preuve.
5. **Pas d'emoji.** Nulle part : ni dans le code, ni dans les logs, ni dans
   l'UI, ni dans les messages de commit. Les icônes de l'interface sont
   **exclusivement en SVG**.
6. **Compatibilité par `compat.py`.** Tout accès `Qt.X`, `Qgis.X`, `QgsXxx.Y`
   sensible aux versions passe par `QtCompat` / `QgisCompat`. Baseline QGIS 3.40,
   cible QGIS 3.44 et 4.0, Qt5 et Qt6.
7. **L'orchestrateur possède la machine à états.** Tout changement transite par
   les gates et est tracé dans `docs/orchestrator_state.json`.

## 2. Roster

| Profil | Fichier | Rôle |
|---|---|---|
| `/orchestrator` | `.devin/workflows/orchestrator.md` | Pilote la machine à états, enforce les gates et l'honnêteté, dispatche les spécialistes, met à jour le ledger. |
| `/architecte` | `.devin/workflows/architecte.md` | Cadrage architecture-first : contrats, call-sites, risques MUST/SHOULD, verdict GO/STOP. |
| `/corrector` | `.devin/workflows/corrector.md` | Implémentation end-to-end gated, édition minimale, preuve runtime. |
| `/reviewer` | `.devin/workflows/reviewer.md` | Audit ciblé avec sévérités et antithèse par finding. |
| `/antithese` | `.devin/workflows/antithese.md` | Pensée adversariale : casse toute thèse avant tout `PASS`. |
| `/qgis-plugin` | `.devin/workflows/qgis-plugin.md` | Spécialiste API plugin QGIS : signaux, layer tree, couches mémoire, providers, rendu, `iface`. |
| `/qgis-compat` | `.devin/workflows/qgis-compat.md` | Spécialiste versions : QGIS 3.44 ↔ 4.0, Qt5 ↔ Qt6, enums déplacés, audit `compat.py`. |
| `/perf` | `.devin/workflows/perf.md` | Performance senior : threading, `WriteQueue`, overlays async, gros journaux/couches, caching, mémoire. |

Chaque profil est aussi un sous-agent dispatchable. L'orchestrateur répartit le
travail par gate et synthétise les verdicts.

## 3. Machine à états (gates)

```
OPEN ── /architecte ──▶ SCOPED ── /critique|/corrector ──▶ REPRODUCED
     ── /corrector ──▶ PATCHED ── /antithese (G3.5) ──▶ PROVEN
     ── /git ──▶ DONE
```

- **G1 SCOPE** (`/architecte`, + spécialistes selon domaine) : contrat + risques
  + verdict `GO` / `GO_WITH_NOTES` / `STOP`.
- **G2 REPRODUCED** : scénario qui FAIL avant patch (bug), ou invariants +
  antithèses (ajout).
- **G3 PATCHED** (`/corrector`) : diff minimal, imports en haut, logs structurés,
  `py_compile` OK, aucun TODO/placeholder.
- **G3.5 ANTITHÈSE** (`/antithese`, **obligatoire**) : ≥3 antithèses, ≥1
  implémentée et capable d'échouer.
- **G4 PROVEN** : run dans QGIS, verdict « PASS sur K antithèses tentées ; L
  exclues avec raison », ≥1 ligne de log citée par assertion clé.
- **G5 DONE** (`/git`) : commit ciblé référençant l'item, push,
  `orchestrator advance --to DONE` avec evidence (commit hash + extrait log).

Aucune gate ne se saute. Aucune ne se « suppose tenir ».

## 4. Ledger

`docs/orchestrator_state.json` (schema_version 1) est la source de vérité de
l'avancement : `session_active`, `session_history`, et `items[]` avec `state`,
`history[]` (transitions horodatées avec `by` et `evidence`), `scenario_path`,
`log_evidence_excerpt`, `commit_hash`. Tout nouvel item suit le même schéma que
les items `BL-RW-*` existants.

## 5. Anti-patterns interdits (rappel)

- Aligner des assertions positives sans une seule assertion négative.
- Ajuster une regex/typo pour faire passer une assertion sans questionner la thèse.
- Réduire le scope pour atteindre `PASS` sans justification externe tracée.
- Déclarer `PROVEN` sur un run sans log produit dans l'environnement cible.
- « Je ne vois pas comment ça casse » : tu n'as pas cherché. Reviens avec 3 antithèses.
