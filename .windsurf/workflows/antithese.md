---
description: Forçage de la pensée adversariale — toute thèse doit survivre à 3+ antithèses sincères avant d'être déclarée vraie.
---

## Quand l'invoquer

- Avant tout verdict `PASS` sur un scénario, un test ou un patch.
- Avant tout commit qui prétend "le bug est corrigé" ou "l'invariant tient".
- Avant toute transition `PROVEN` → `DONE` dans le workflow orchestrateur.

Si on n'invoque pas ce workflow, le verdict est suspect par défaut.

## Procédure stricte

### 1. Énoncer la THÈSE en une phrase

Format imposé : `THÈSE: <fait précis qui sera réputé prouvé>`. Pas "ça marche", pas "le code est OK". Quelque chose comme : `THÈSE: après refactor, EditSessionTracker refuse toute couche memory en émettant un log action=refused reason=no_stable_identity, et aucun event ne touche le journal.`

### 2. Lister ≥3 ANTITHÈSES concrètes

Pour chaque antithèse, écrire :

- **Hypothèse de rupture** : quel mécanisme exact pourrait rendre la thèse fausse.
- **Conditions de déclenchement** : valeurs, ordres, races, encoding, taille, droits, état partiel.
- **Couverture actuelle** : le scénario actuel l'attrape-t-il ? Si non, dire pourquoi (à attaquer ou à exclure avec raison).

Catégories à scanner systématiquement :

- **Données** : vide, taille massive, doublons, ordre inverse, NULL, types mixtes, Unicode, BLOB, géométrie invalide, FID recyclé.
- **Provider/IO** : driver alternatif (e.g. `OpenFileGDB` vs `ESRI Shapefile`), fichier locké, read-only, multi-process, chemin UNC/Unicode, disque plein.
- **Concurrence/Timing** : signal Qt déconnecté en plein vol, suppress reentré, rollback pendant after_commit, plugin déchargé mid-edit, write_queue saturée.
- **Identité** : fingerprint quasi-collision, recyclage FID dans la même seconde, projet sans home path.
- **Environnement** : RAM, disque, droits, multi-instance QGIS, version QGIS 3.40 vs 4.x.
- **Régression rétroactive** : ce qui marchait avant marche-t-il toujours ?

### 3. ATTAQUER avec ≥1 antithèse implémentée

Au minimum **un cas d'antithèse doit devenir une vraie assertion ou phase brutale dans le scénario**. Pas "TODO antithèse plus tard". Implémentée maintenant.

Cette assertion :

- doit pouvoir **échouer** réellement si la thèse est fausse (sinon elle est morte) ;
- doit produire un **log explicite** sur l'échec pour qu'on puisse diagnostiquer ;
- doit être **brutale** : valeurs limites, jamais des cas tièdes du milieu.

### 4. Verdict en SYNTHÈSE

Format imposé :

```
SYNTHÈSE: PASS sur <K> antithèses tentées ; <L> exclues avec raison.
Antithèses attaquées : [liste].
Antithèses exclues : [liste avec raison].
```

Si `K = 0` → verdict invalide. Si `L > 0` sans raison écrite → verdict invalide.

## Anti-patterns interdits

- Aligner des assertions "ok=True", "error=None", "n>=1" sans une seule assertion négative `under X, must be 0/None/empty`.
- "Le code production était déjà correct donc pas besoin d'antithèse" → faux ; le code peut être correct sur happy path et casser sur l'antithèse.
- Ajuster regex/typo pour faire passer une assertion ratée sans questionner la thèse sous-jacente.
- Réduire le scope du scénario pour atteindre PASS — admis uniquement avec justification externe explicite (contrainte d'accès, environnement indisponible).

## Sortie

Un bloc texte structuré rendu dans la conversation, lisible par le user, avec : THÈSE, 3+ antithèses, ce qui est attaqué dans le code, ce qui est exclu avec raison, SYNTHÈSE finale.

Pas d'antithèse rendue visible = pas de verdict PASS accepté.
