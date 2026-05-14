---
description: Audit technique ciblé avec sévérités, références exactes, regroupement par cause racine — antithèse obligatoire sur chaque finding "non bloquant".
---

## Mission

Auditer un périmètre (commit, module, fichier, patch en cours) et produire une liste de findings avec :

- **sévérité** : `BLOCKER` / `HIGH` / `MEDIUM` / `LOW`.
- **localisation exacte** : `@/abs/path/file.py:lineA-lineB`.
- **cause racine** : groupe par CR-N si possible.
- **antithèse** : pour chaque finding noté `MEDIUM`/`LOW`, énoncer le cas qui rendrait la sévérité HIGH/BLOCKER (force la réflexion adversariale, évite les minorations complaisantes).

## Règles dures

- Pas d'audit "tout va bien". Si aucun finding émergent, le rendre comme `NOTE` avec liste explicite de 3 antithèses tentées qui n'ont rien révélé. Sinon = audit non fait.
- Pas de double minoration. Un finding ne peut pas être à la fois `LOW` et "à voir plus tard". Il est `LOW` *parce que* l'antithèse écrite est faible. Si l'antithèse est forte, le sévérité monte.
- Citer la ligne. Pas de référence vague.
- Regrouper par cause racine quand >2 findings convergent.

## Sortie

```
# Audit <périmètre>

## Findings

### F-1 [BLOCKER] <titre court>
- Localisation : @/abs/path/file.py:linA-linB
- Constat : <fait observé, 2 lignes max>
- Antithèse forte : <cas qui prouve la sévérité>
- Cause racine : CR-N (si applicable)
- Reco : <action minimale, périmètre exact>

### F-2 [MEDIUM] <titre court>
- ...
- Antithèse (cas qui le rendrait HIGH) : ...

## Synthèse
<K> BLOCKER, <L> HIGH, <M> MEDIUM, <N> LOW.
<Z> antithèses tentées dont <W> ont révélé un finding.
```

## Anti-patterns interdits

- "Code propre, RAS." Sans liste d'antithèses tentées, c'est un audit fictif.
- Tout noter `LOW` pour éviter le débat. La sévérité doit refléter une antithèse réelle.
- Énoncer un finding sans localisation ligne précise.
- Audit qui ne regroupe jamais par cause racine alors que des findings convergent.
