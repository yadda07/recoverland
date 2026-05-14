---
description: Workflow de correction ou implémentation end-to-end avec preuve, sobriété et zéro ambiguïté — antithèse obligatoire en G3.5 avant tout PASS.
---

## Gates imposés

Aucune gate ne peut être sautée. Aucune ne peut être "supposée tenir".

### G1 — SCOPE (profil `/architecte`)

- Lire le backlog item : statut, cause racine, acceptance, scénario attendu, effort.
- Lire tous les call sites du code à toucher (signatures, contrats amont/aval).
- Produire une /critique structurée :
  - Risques `MUST` / `SHOULD` / `OPTIONAL` numérotés.
  - Pour chaque risque : Constat / Options / Choix justifié.
  - Verdict explicite : `GO` / `GO_WITH_NOTES` / `STOP` + raison.
- Avancer SCOPED uniquement si verdict ≠ STOP.

### G2 — REPRODUCED (profil `/critique`)

- Si bug : produire un scénario qui FAIL avant patch. Pas optionnel.
- Si ajout (pas de bug) : produire les invariants attaqués + ≥3 antithèses (cf. `/antithese`) qui DOIVENT être implémentés dans le scénario.
- Si aucune antithèse possible → STOP : l'item n'a probablement pas de valeur de validation, le rejeter ou réduire scope.

### G3 — PATCHED (profil `/corrector`)

- Édition minimale, périmètre demandé, zéro refactor opportuniste.
- Imports en haut, logs structurés `flog_kv` sur chemins critiques.
- Pas de TODO, pas de placeholder, pas de "...".
- Pré-validation locale immédiate : `py_compile` + regex source check des patterns produits.

### G3.5 — ANTITHÈSE (profil `/antithese`) — **OBLIGATOIRE**

- Invoquer le workflow `/antithese` (cf. `.windsurf/workflows/antithese.md`).
- Énumérer ≥3 antithèses concrètes contre la thèse du patch.
- Implémenter ≥1 antithèse comme assertion brutale dans le scénario (pas en TODO).
- Pour chaque antithèse exclue : raison écrite (contrainte externe, hors périmètre item, etc.).
- Si une antithèse implémentée FAIL → revenir G3, corriger code, refaire G3.5.
- Si toutes les antithèses passent du premier coup et qu'aucune ne tente un cas limite réel → durcir, c'est qu'elles étaient tièdes.

### G4 — PROVEN (profil `/critique` + `/logger`)

- Run scénario en environnement cible (QGIS console, pas CLI hors-QGIS).
- Verdict = logs runtime + assertions.
- Synthèse en format `PASS sur K antithèses tentées ; L exclues avec raison`. Jamais "PASS N/N" sec.
- Citer ≥1 ligne de log qui prouve chaque assertion clé.

### G5 — DONE (profil `/git`)

- Commit ciblé, message en référencement de l'item backlog.
- Message contient : item ID, fichiers touchés, count assertions, antithèses attaquées, antithèses exclues.
- Push.
- `orchestrator advance --to DONE` avec evidence incluant commit hash + extrait log + résumé antithèses.

## Anti-patterns interdits

- Sauter G3.5. Pas d'antithèse = pas de DONE.
- "Je ne vois pas comment ça casse" → tu n'as pas cherché. Reviens avec 3 antithèses concrètes.
- Aligner assertions positives sans une seule assertion négative.
- Ajuster une regex pour faire passer une assertion sans questionner la thèse.
- Réduire scope pour faire passer ; admissible uniquement avec justification externe explicite et tracée.
- Déclarer PROVEN sur run silencieux sans log produit dans l'environnement cible.

## Sortie attendue par G

| Gate | Sortie visible obligatoire |
|---|---|
| G1 | /critique structurée + verdict GO/STOP |
| G2 | baseline FAIL log OR liste invariants+antithèses |
| G3 | diff minimal + py_compile=0 + regex source check OK |
| G3.5 | bloc THÈSE/ANTITHÈSES/ATTAQUE/SYNTHÈSE explicite |
| G4 | run runtime + verdict synthèse "PASS sur K ; L exclues" |
| G5 | commit hash + push + orchestrator advance DONE |
