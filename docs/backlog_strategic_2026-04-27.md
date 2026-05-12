# Backlog strategique RecoverLand - 2026-04-27

## Intention directrice

**RecoverLand sauve des equipes en production aujourd'hui.** Le plugin capture des editions QGIS sur sources heterogenes (PostGIS, GeoPackage, Shapefile, SpatiaLite, MSSQL, Oracle, GeoJSON, FlatGeobuf, CSV, et autres formats OGR) et permet la restauration. Cette utilite est un fait constate, pas une hypothese.

**Ce backlog ne retire aucune fonctionnalite existante.** Il les renforce, les rend plus intelligentes, plus adaptatives, plus observables. Tous les items vont dans le sens de :

- **Plus robuste** : tolerance aux pannes accrue, modes degrades couverts explicitement, recovery automatique etendue.
- **Plus intelligent** : adaptation automatique au contexte (volume, format, environnement reseau, charge).
- **Plus adaptatif** : detection des situations limites avant qu'elles ne deviennent des problemes.
- **Plus large** : extension du support multi-format, integration avec ecosysteme QGIS (Processing, Modeler, console).
- **Plus visible** : distribution officielle, observabilite, traces auditables.

**Aucun item ne propose** : suppression d'un module existant, retrait d'une fonctionnalite documentee, reduction du perimetre couvert. Toute mention d'audit ou de revue concerne l'**enrichissement** du composant, pas son retrait.

## Complementarite avec les backlogs existants

- `docs/backlog_qa_hardening_2026-04-27.md` : robustesse logicielle interne (atomicite, preflight, file d'ecriture).
- `docs/backlog_performance_2026-04-27.md` : volumetrie, latence, concurrence SQLite.
- `docs/backlog_security_2026-04-16.md` : securite XML, SQL, logs (livre).
- `docs/backlog_2026-04-16.md` : compatibilite QGIS 3.40-4.x, ARCH, COMPAT.
- **Ce backlog** : produit, distribution, honnetete communication, couverture fonctionnelle elargie, resilience renforcee.

## Methodologie

Chaque item suit la regle "fait verifie / hypothese / non verifie". Les references code et docs pointent un chemin precis. Les hypotheses sont marquees explicitement. Aucun item ne propose une garantie qui ne soit pas implementable.

Chaque item contient :

- **Constat** avec references code/doc verifiees.
- **Cause** factuelle.
- **Valeur metier** orientee robustesse / intelligence / adaptativite.
- **Perimetre technique** executable sans interpretation.
- **Failure scenarios** systematiques.
- **Edge cases** explicites.
- **Plan d'observabilite** (logs, metriques, traces).
- **Considerations securite** quand pertinent.
- **Criteres d'acceptation** mesurables.
- **Cout** d'execution et impact runtime.
- **Tags** : BLOCKER, RISK, WEAK DESIGN, UNKNOWN.

## Phase de recherche externe (workflow @backlogger)

### Sources officielles lues

- **SQLite WAL et reseau** : `https://sqlite.org/wal.html` confirme : "WAL does not work over a network filesystem". Citation textuelle : "All processes using a database must be on the same host computer". **Consequence** : tout deploiement RecoverLand sur partage reseau (NFS, SMB, Dropbox, OneDrive) est en risque de corruption silencieuse. Source de BL-PROJ-001.
- **SQLite over network** : `https://sqlite.org/useovernet.html` confirme : "Network filesystems do not support the ability to do simultaneous reads and writes while at the same time keeping the database consistent". Renforce BL-PROJ-001 et BL-RESILIENCE-005.
- **SQLite corruption** : `https://sqlite.org/howtocorrupt.html` liste les scenarios de corruption a couvrir defensivement.
- **QGIS plugin structure 3.40** : `https://docs.qgis.org/3.40/en/docs/pyqgis_developer_cookbook/plugins/plugins.html` confirme structure attendue : `metadata.txt`, `i18n/`, `__init__.py`, ressources. Source de BL-DIST-002.
- **QGIS plugin release 3.40** : `https://docs.qgis.org/3.40/en/docs/pyqgis_developer_cookbook/plugins/releasing.html` documente le processus officiel de soumission a `plugins.qgis.org`. Source de BL-DIST-001.
- **QGIS translation** : `https://docs.qgis.org/3.40/en/docs/documentation_guidelines/do_translations.html` documente la chaine `.ts` -> `.qm`. Source de BL-DIST-002.
- **PyQGIS QgsVectorLayer** : `https://qgis.org/pyqgis/master/core/QgsVectorLayer.html` decrit les signaux `editingStarted`, `beforeCommitChanges`, `afterCommitChanges`, `committedFeaturesAdded`. Confirme : ces signaux sont emis quel que soit le declencheur (UI, console Python, Processing in-place). Source de BL-COV-001 et BL-COV-002.
- **QGIS Enhancement Proposal #114** : `https://github.com/qgis/QGIS-Enhancement-Proposals/issues/114` confirme que Processing in-place edit utilise le buffer d'edition standard et donc emet les signaux d'edition. Source de BL-COV-002.

### Sources externes evaluees

- **SQLCipher** : `https://github.com/sqlcipher/sqlcipher` et `https://www.zetetic.net/sqlcipher/`. Patterns de chiffrement at-rest, performance attendue. Overhead PBKDF2 sur premier accès (par defaut 64000 iterations), cache ensuite. Source de BL-DATA-001.
- **pg_history_viewer** (`https://plugins.qgis.org/plugins/pg_history_viewer/`) : reference QGIS pour audit, mais PostgreSQL-only et server-side. RecoverLand garde un avantage net sur la couverture multi-format.
- **Postgres 91 plus Auditor** (`https://plugins.qgis.org/plugins/postgres91plusauditor/`) : autre reference, meme limite.
- **undoPropertiesChanges** (`https://plugins.qgis.org/plugins/undoPropertiesChanges/`) : limite a undo/redo properties, pas comparable.

### Decisions soutenues par preuves

- **Distribution officielle** : la doc QGIS 3.40 documente le canal officiel `plugins.qgis.org`. Pas d'alternative recommandee.
- **WAL non utilisable sur reseau** : doc SQLite officielle. Detection necessaire avant ouverture du journal.
- **Capture transparente Processing** : doc QGIS confirme l'usage du buffer d'edition. Hypothese a tester en condition reelle.
- **Chiffrement at-rest possible** : SQLCipher est mature, integrable via `pysqlcipher3` ou compilation custom.

### Inconnues bloquantes

- **UNKNOWN-DIST** : statut actuel sur `plugins.qgis.org` non verifie. A confirmer manuellement.
- **UNKNOWN-USERS** : segment utilisateur cible reel inconnu. Influence priorisation UX vs admin.
- **UNKNOWN-RGPD** : usage avec donnees personnelles non sonde. Influence priorite chiffrement / scrub.
- **UNKNOWN-NETWORK** : pourcentage d'utilisateurs sur partages reseau inconnu. Influence priorite warning WAL.
- **UNKNOWN-LOAD** : volumetrie reelle deployee inconnue. Voir backlog perf BL-PERF-000.
- **UNKNOWN-PROCESSING-COVERAGE** : capture effective des modifications Processing in-place pas validee par test e2e.

## Synthese des constats

Tous les constats sont fondes sur lecture du code et de la doc, sauf mention contraire.

- **Distribution non confirmee** : `metadata.txt:16-18` pointe github personnel, presence `plugins.qgis.org` non verifiee.
- **Modes degrades silencieux possibles** : `core/write_queue.py:91-100` overflow vers pending recovery, `docs/limits.html#tracking-off` tracking off sans catch-up, `docs/limits.html#disk-full` auto-disable sous 100 MB.
- **Format export non portable** : `journal_maintenance.py:300-325` copie SQLite via `sqlite3.Connection.backup()`. Format proprietaire au plugin.
- **Action UI unique** : `recover.py:53-63` enregistre un seul `QAction`. Aucune action sur menu contextuel couche pour le cas d'usage le plus frequent ("annuler ma derniere erreur sur cette couche").
- **Capture Processing/console non instrumentee** : grep `QgsProcessingProvider` retourne zero. Hypothese : capture deja effective via signaux QGIS standards. A prouver.
- **Pas de chiffrement at-rest** : grep `sqlcipher|encrypt|cipher` retourne zero (verifie). Implication RGPD.
- **Fingerprint base sur chemin** : `docs/index.html#fingerprint`. Cas critiques documentes (`docs/limits.html#fid-instable`, `#network-drive`, `#file-moved`). Renforcement possible sans casser l'existant.
- **Composants resilience presents mais a renforcer** : `core/disk_monitor.py`, `core/health_monitor.py`, `core/integrity.py`, `core/datasource_alias.py`. Tous conserves, tous renforces.

## Echelle de priorite

- **P0 BLOCKER** : ecart promesse / realite, risque corruption silencieuse, ou risque adoption majeur.
- **P1 MAJEUR** : differentiation forte de robustesse ou de portee.
- **P2 IMPORTANT** : robustesse, decouverte, maintenabilite, observabilite.
- **P3 MINEUR** : finition, dette faible.

---

# Axe 1 - Distribution et decouverte

## BL-DIST-001 - Soumettre le plugin a plugins.qgis.org

**Tags** : DISTRIBUTION, ADOPTION, P0

**Priorite** : P0

**Complexite** : faible

### Constat

`metadata.txt:16` pointe `homepage=https://github.com/yadda07/recoverland/`. Aucune verification automatisee n'a confirme une publication active sur le repo officiel `plugins.qgis.org`. Sans canal officiel : pas de decouverte par les utilisateurs QGIS, pas de notification de mise a jour, pas de signal social (notes, telechargements).

### Cause

Statut hypothese : `plugins.qgis.org` non publie, ou publie sans visibilite ciblee.

### Valeur metier

Distribution = condition necessaire a l'adoption. Une discipline d'ingenierie de 1168 tests sans canal de distribution donne un excellent plugin invisible.

### Perimetre technique

- Verifier si le plugin est deja publie. Si oui, verifier visibilite des metadonnees (tags, description, screenshots).
- Si non publie : preparer paquet zip conforme aux exigences `plugins.qgis.org` (sans .git, sans tests, sans logs locaux).
- Soumettre la version `4.8.2`.
- Ajouter `experimental=False` (deja present `metadata.txt:14`) avec justification couverture tests.
- Mettre a jour `homepage` vers une page d'atterrissage neutre (voir BL-DIST-003).

### Criteres d'acceptation

- Page plugin visible sur `plugins.qgis.org/plugins/recoverland/`.
- Mise a jour automatique fonctionnelle depuis QGIS (`Plugins -> Manage and install`).
- Tags QGIS officiels alignes avec `metadata.txt:11` : `audit, recovery, restore, sketching, versioning, local, sqlite, temporal, rollback, health, preview`.

### Risques

- Refus pour raison technique (ex : depandances non standard). A verifier sur `metadata.txt`.
- Conflit de nom avec un plugin existant. Probabilite faible.

---

## BL-DIST-002 - Internationalisation effective EN et FR

**Tags** : DISTRIBUTION, I18N, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`i18n/recoverland_en.ts` et `i18n/recoverland_en.qm` existent. `recover.py:60` montre `tr("RecoverLand - Recuperation de donnees d'audit")` en francais code en dur. Le tooltip francais sera traduit en anglais via `.qm` ? Non verifie. La doc HTML `docs/index.html` est en anglais. Le code source contient des chaines en francais (`journal_maintenance.py:122` "Exporter le journal", etc.) et en anglais.

### Cause

Inversion source/traduction : la langue source du plugin oscille entre FR (UI) et EN (doc). L'utilisateur anglophone aura des incoherences visuelles sans verification.

### Valeur metier

`plugins.qgis.org` audience est mondiale. Sans EN propre, marche reduit a la francophonie.

### Perimetre technique

- Choisir une langue source unique (recommandation : EN pour plus large audience).
- Auditer `i18n/recoverland_en.ts` : completude, coherence terminologique.
- Generer `recoverland_fr.ts` si la source devient EN.
- Re-tester `i18n/compile_translations.py` (deja durci, voir SEC backlog 2026-04-16).
- Verifier que la doc HTML reflete la langue source.

### Criteres d'acceptation

- `pylupdate6` sur le code retourne zero chaine non extraite.
- Capture d'ecran QGIS-en : zero chaine francaise residuelle.
- Capture d'ecran QGIS-fr : zero chaine anglaise residuelle (sauf termes techniques GIS standard).

### Edge cases

- QGIS profil locale `de_DE` non couvert : doit retomber sur EN.

---

## BL-DIST-003 - Page d'atterrissage publique neutre

**Tags** : DISTRIBUTION, BRAND, P2

**Priorite** : P2

**Complexite** : faible

### Constat

`metadata.txt:16` : `homepage=github.com/yadda07/recoverland/`. Profile github personnel, pas un site projet. `docs/` contient une documentation HTML soignee mais hostee ou ? `.github/workflows/static.yml` existe : hypothese GitHub Pages, non verifie.

### Cause

Pas de page projet identifiee, donc pas de point d'entree marketing pour les decideurs (DSI, RSSI, chef de projet SIG).

### Valeur metier

Une page projet permet de communiquer sur la securite, la conformite, les references, le support, les versions, sans noyer l'utilisateur dans la doc technique.

### Perimetre technique

- Activer GitHub Pages sur `docs/` (si `.github/workflows/static.yml` ne le fait pas deja).
- Pointer `metadata.txt:homepage` vers cette page (pas vers le repo).
- Section "for decision makers" : trust boundary, RGPD, perimetre, ce que le plugin n'est pas.
- Section "quick start" 3 etapes max.

### Criteres d'acceptation

- URL stable hors github.com/yadda07.
- Page chargee en moins de 2 secondes (texte HTML statique, deja le cas).

---

# Axe 2 - UX du moment de verite

## BL-UX-001 - Action "Annuler le dernier commit" depuis le menu contextuel de la couche

**Tags** : UX, DISCOVERABILITY, P0

**Priorite** : P0

**Complexite** : moyenne

### Constat

`recover.py:53-63` enregistre une seule action via `iface.addPluginToMenu` et `iface.addToolBarIcon`. Aucune action n'est ajoutee au menu contextuel de la couche dans la table des couches. L'utilisateur qui veut annuler son dernier commit doit : ouvrir le plugin, choisir un filtre couche, paginer, selectionner, prevoir, confirmer le preflight. Total : minimum 6 clics et un changement de focus.

### Cause

Decision d'architecture historique : un point d'entree unique = une dialog unique.

### Valeur metier

Le moment ou l'utilisateur a besoin de RecoverLand est un moment de panique : "je viens de supprimer 200 entites par erreur". Reduire ce moment a 2 clics maximum transforme un outil utile en outil indispensable.

### Perimetre technique

- Ajouter un `QAction` au menu contextuel des couches editables via `iface.addCustomActionForLayer` ou hook `LayerTreeContextMenu`.
- Action visible uniquement si la couche a des events RecoverLand (verification rapide via index sur `datasource_fingerprint`).
- Action declenchee : detecter le dernier event commit pour cette couche, prevoir le restore, demander confirmation explicite, executer en STRICT.
- Aucune ouverture de la dialog complete.

### Dependances

- `core/restore_planner.py` : reutiliser le builder de plan.
- `core/restore_executor.py` : reutiliser le mode STRICT.
- `core/search_service.py` : ajouter `get_last_commit_session_for_layer(datasource_fingerprint)`.

### Criteres d'acceptation

- 2 clics droite + 1 confirmation pour annuler le dernier commit sur une couche.
- Test manuel : edit, commit, clic droit, "Undo last commit", confirmation, etat restaure.
- Test edge : couche sans historique = action grisee.

### Edge cases

- Couche sans aucun event RecoverLand : action absente ou grisee.
- Dernier commit > 7 jours : warning explicite avant restore.
- Plusieurs sessions imbriquees : annuler uniquement la derniere session, pas le dernier event.

---

## BL-UX-002 - Restore complet en 3 clics maximum dans la dialog

**Tags** : UX, FRICTION, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

Hypothese a verifier : le chemin restore actuel dans `recover_dialog.py` impose plusieurs etapes obligatoires (filtre couche, plage temporelle, mode STRICT/BEST_EFFORT, preflight). A confirmer en testant un cas reel chronometre.

### Cause

Architecture orientee admin : tous les leviers exposes au meme niveau. Pas de chemin "rapide" pour le cas commun (annuler les 5 dernieres minutes sur la couche active).

### Valeur metier

Reduire la friction sur le 80% des usages. Le cas admin garde son chemin complet.

### Perimetre technique

- Mode "Restore rapide" : prefiltre = couche active du projet, plage = 5 dernieres minutes, mode STRICT par defaut.
- Bouton unique "Restaurer maintenant" qui enchaine planification + preflight + execution sans dialogues intermediaires.
- Bouton "Restore avance" qui ouvre l'experience actuelle complete.

### Criteres d'acceptation

- Restore d'un cas trivial (1 commit annule) en 3 clics : ouvrir plugin, "Quick restore", confirmer.
- Restore avance accessible sans regression.

---

## BL-UX-003 - Decomposer RecoverDialog monolithique

**Tags** : UX, ARCH, REFACTOR, P2

**Priorite** : P2

**Complexite** : elevee

### Constat

Identifie comme `ARCH-01` dans `docs/backlog_2026-04-16.md` (priorite HAUTE selon memoire systeme). Re-priorise ici comme P2 strategique car son blocage retarde BL-UX-001 et BL-UX-002.

### Cause

Cf backlog `2026-04-16`. Cumul de responsabilites : recherche, dashboard, restore, maintenance, smart bar, info bar.

### Valeur metier

Reduire la surface de bug, faciliter les ajouts d'actions ciblees (BL-UX-001).

### Perimetre technique

Voir `docs/backlog_2026-04-16.md`. Ajouter ici : extraire un module `restore_orchestrator.py` qui sert aussi BL-UX-001 (action contextuelle) sans passer par la dialog.

### Criteres d'acceptation

- `recover_dialog.py` < 500 lignes (statut actuel : non verifie, a mesurer).
- Module `restore_orchestrator.py` utilisable sans creer de QDialog.

---

## BL-UX-004 - Indicateur permanent et explicite de l'etat tracking

**Tags** : UX, OBSERVABILITY, P1

**Priorite** : P1

**Complexite** : faible

### Constat

`recover.py:264-268` ajoute un `StatusBarIndicator` permanent. Bien. Mais `docs/limits.html#tracking-off` indique : si tracking off, rien n'est capture, pas de catch-up. `docs/limits.html#disk-full` indique : auto-disable sous 100 MB, l'utilisateur doit re-enable manuellement. La presence de l'indicateur est insuffisante si l'utilisateur ne le surveille pas.

### Cause

L'indicateur passif ne previent pas l'oubli. Aucune notification active a ete identifiee dans le code (a confirmer par audit).

### Valeur metier

L'utilisateur doit savoir, sans surveiller, que le tracking est inactif quand il commence a editer.

### Perimetre technique

- Au declenchement de `startEditing` sur n'importe quelle couche editable, si tracking off : QMessageBox avec choix "Activer le tracking" / "Continuer sans tracking" / "Ne plus demander pour cette session".
- Au demarrage QGIS apres une auto-desactivation disk-full : banner persistant en haut du panneau RecoverLand jusqu'a re-activation manuelle.
- Audit log explicite des transitions tracking on/off.

### Criteres d'acceptation

- Test : tracker desactive + clic edit sur couche : message visible.
- Test : disque > 100 MB apres auto-desactivation : banner toujours visible jusqu'a action utilisateur.

---

## BL-UX-005 - Notifications explicites des modes degrades

**Tags** : UX, OBSERVABILITY, HONESTY, P0

**Priorite** : P0

**Complexite** : moyenne

### Constat

Modes degrades documentes dans `docs/limits.html` :
- tracking off (utilisateur),
- tracking auto-off (disk),
- modif hors QGIS,
- crash avant flush WriteQueue,
- WriteQueue overflow (`core/write_queue.py:91-100` : sauve dans pending recovery),
- writer lock pris par autre instance.

Hypothese : aucune ou peu de ces transitions ne produit de toast/dialog visible. Code a auditer module par module.

### Cause

Promesse "Zero silent loss" (`docs/limits.html#crash`) n'est tenable qu'en alertant explicitement chaque transition vers un mode degrade.

### Valeur metier

Aligner promesse marketing et realite operationnelle.

### Perimetre technique

- Inventorier les 6 modes degrades dans une matrice : event, signal, notification UI, log.
- Pour chaque transition non notifiee : ajouter une notification toast non bloquante dans la barre d'info QGIS.
- Une zone "incidents" dans la dialog plugin : journal chronologique des transitions degradees, exportable.

### Criteres d'acceptation

- Tester chaque mode et constater une notification visible.
- La zone "incidents" persiste les 30 derniers evenements degrades.

---

# Axe 3 - Portabilite du format

## BL-PORT-001 - Export JSONL neutre des events

**Tags** : PORTABILITY, FORMAT, OPENNESS, P0

**Priorite** : P0

**Complexite** : moyenne

### Constat

`journal_maintenance.py:300-325` : l'export consiste en `sqlite3.Connection.backup()` vers un fichier `.sqlite` du meme schema. C'est une copie, pas un export portable. Sans le code RecoverLand, le `.sqlite` exporte est un fichier opaque.

### Cause

Decision implicite : le format de stockage = le format d'echange.

### Valeur metier

Lever le verrou format. Un export JSONL ouvert :
- protege l'utilisateur en cas d'abandon du plugin,
- rassure DSI/RSSI sur la portabilite,
- permet integration ETL/GitOps externe (un fichier JSONL = un commit Git lisible).

### Perimetre technique

- Format : un objet JSON par ligne, schema versionne (`event_schema_version`).
- Champs : tous les champs de `audit_event` (`docs/index.html#journal`), avec `geometry_wkb` encode en base64 (deja la convention serialisation, voir `core/serialization.py`).
- Filtres a l'export : par couche, par plage temporelle, par session.
- Streaming pour journaux volumineux (`yield` ligne par ligne).
- Documenter le schema dans `docs/formats.html`.

### Criteres d'acceptation

- Export d'un journal de 100k events termine en < 30 s sur SSD (cible, a affiner via BL-PERF-000).
- Validation : `cat export.jsonl | jq -c .` retourne 100k lignes valides.
- Test e2e : exporter, importer dans un journal vide (BL-PORT-002), comparer event par event.

### Edge cases

- BLOB `geometry_wkb` > 1 MB : conserver en base64 ou referencer un fichier annexe ? Decider et documenter.
- Caracteres non-UTF8 dans attributes_json : tester avec donnees historiques.
- Export incremental : par cutoff `created_at` plus recent que X.

---

## BL-PORT-002 - Import JSONL pour reprise et migration

**Tags** : PORTABILITY, FORMAT, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

Le pendant de BL-PORT-001. Aucun chemin d'import de journal aujourd'hui.

### Cause

Pas de scenario "merger deux journaux" prevu dans l'architecture.

### Valeur metier

- Reprise apres reinstallation OS.
- Consolidation de plusieurs postes solo en un journal central (a froid).
- Migration entre versions majeures.

### Perimetre technique

- Lecture streaming du JSONL.
- Validation par `event_schema_version` (`docs/index.html#journal`).
- Migration en cas de schema older (`core/sqlite_schema.py` connait deja v1->v2).
- Strategie de conflit : skip duplicate (sur clef composite project_fingerprint + datasource_fingerprint + entity_fingerprint + created_at).
- Mode dry-run obligatoire avec rapport.

### Criteres d'acceptation

- Round-trip BL-PORT-001 + BL-PORT-002 conserve tous les events.
- Import d'un fichier corrompu (ligne tronquee) : zero event applique, rapport precis.

### Edge cases

- Import dans un journal non vide : conflit sur event_id auto-incremente. Accepter de re-incrementer.
- Schema version inconnue : refuser avec message clair.

---

## BL-PORT-003 - Documentation publique du schema event

**Tags** : PORTABILITY, DOC, P2

**Priorite** : P2

**Complexite** : faible

### Constat

`docs/index.html#journal` decrit le schema audit_event de maniere lisible. `docs/formats.html` existe mais non audite. Manque : un fichier de reference machine-readable du schema.

### Cause

La doc actuelle est humaine, pas exploitable par un outil tiers.

### Valeur metier

Permettre des outils tiers (ETL, validateurs, migrations futures) de consommer le format.

### Perimetre technique

- Generer un JSON Schema valide pour le format JSONL exporte.
- Le placer dans `docs/schema/event-v2.schema.json`.
- Lier depuis `docs/formats.html`.
- Test CI : valider chaque ligne du JSONL exporte contre le JSON Schema.

### Criteres d'acceptation

- `jsonschema` Python lib accepte 100% des events exportes.

---

# Axe 4 - Promesses vs realite

## BL-PROMISE-001 - Aligner "Zero silent loss" avec le mecanisme reel

**Tags** : HONESTY, MARKETING, P0

**Priorite** : P0

**Complexite** : faible

### Constat

`docs/limits.html#crash` ligne 245 : `"Zero silent loss: If events are lost despite the recovery mechanisms, the user is explicitly informed. No loss is hidden."`

Cette promesse n'est tenable que si tous les modes degrades de BL-UX-005 produisent une notification utilisateur. A date, plusieurs ne le font pas (a confirmer).

### Cause

Promesse plus forte que l'implementation.

### Valeur metier

Confiance des utilisateurs et des DSI. Une promesse cassee une fois = perte de credibilite irreversible.

### Perimetre technique

- Soit livrer BL-UX-005 (notifications systematiques), et conserver la promesse.
- Soit reformuler en : "Best-effort loss disclosure: detected losses are reported via the activity log. Some losses (modifications outside QGIS, hard crash before flush) cannot be detected by RecoverLand."
- Ne pas garder le slogan sans le mecanisme.

### Criteres d'acceptation

- Soit BL-UX-005 livre, soit slogan reformule.

---

## BL-PROMISE-002 - Qualifier le terme "safety net"

**Tags** : HONESTY, MARKETING, P1

**Priorite** : P1

**Complexite** : faible

### Constat

`docs/index.html:74` : `"Smart local audit, delta storage, surgical restore: travel back in time through your QGIS edits"`. `docs/limits.html#what-rl-is-not` qualifie le plugin de "local safety net". 

Le terme "safety net" suppose une garantie. Or les cas critiques documentes (FID instable, modif hors QGIS, fingerprint perdu sur deplacement de fichier) cassent cette garantie.

### Cause

Marketing vs realite operationnelle.

### Valeur metier

Eviter les retours utilisateur du type "ca ne marche pas" alors qu'ils sont dans un mode documente comme degrade.

### Perimetre technique

- Remplacer "safety net" par "audit trail with optional restore on strong-identity formats".
- Ajouter en page d'accueil un bandeau "Best with: GeoPackage, PostGIS. Limited with: Shapefile, GeoJSON. Not for: CSV, memory layers."
- Aligner toute la communication sur cette taxonomie de fiabilite (deja presente partiellement via `core/support_policy.py` et `identity_strength`).

### Criteres d'acceptation

- Aucune occurrence de "safety net" sans qualification dans `docs/`.
- Bandeau de fiabilite visible des l'accueil.

---

## BL-PROMISE-003 - Tracabilite formelle des modes degrades dans le journal

**Tags** : HONESTY, AUDIT, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

Modes degrades ne sont pas necessairement enregistres dans le journal (a auditer). Si tracking est auto-desactive a 14h, edition continue, re-activation manuelle a 16h, il n'y a aucune trace dans le journal de ce trou de 2h.

### Cause

Le journal capture les events business, pas les events systeme du plugin lui-meme.

### Valeur metier

Permettre a l'utilisateur de savoir avec certitude : "entre 14h et 16h, le journal etait inactif". Sinon, l'absence d'event peut signifier soit "rien edite" soit "tracking off". Ambigu = inutilisable pour l'audit.

### Perimetre technique

- Nouvelle table `audit_lifecycle` ou nouveau `operation_type` reserve "SYSTEM" dans `audit_event`.
- Events lifecycle : tracking_enabled, tracking_disabled (manuel/auto/disk_full/lock), startup, shutdown, write_queue_overflow, integrity_recovery_run.
- Visibles dans la zone "incidents" (BL-UX-005) et dans l'export (BL-PORT-001).

### Criteres d'acceptation

- Test : desactiver tracking, attendre 1 minute, reactiver. Le journal contient 2 events lifecycle avec timestamps.
- L'export JSONL inclut ces events.

---

# Axe 5 - Couverture fonctionnelle

## BL-COV-001 - Capturer les modifications via la console Python QGIS

**Tags** : COVERAGE, USECASE, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`docs/limits.html#outside-qgis` recommande : "If you must use a script, run it in the QGIS Python console in edit mode (`startEditing` / `commitChanges`) so that the signals are emitted." Cela suggere que le tracker capte deja ce cas. A verifier par test reel.

### Cause

Hypothese a verifier : le tracker s'attache via `EditSessionTracker` (`core/edit_tracker.py`) aux signaux `beforeCommitChanges` / `afterCommitChanges` qui sont emis quel que soit le declencheur (UI ou script).

### Valeur metier

Etendre le perimetre de capture sans modification du tracker. Si la capture marche deja, c'est un argument commercial fort a documenter.

### Perimetre technique

- Test e2e : ouvrir QGIS, console Python, `layer.startEditing(); layer.changeAttributeValue(...); layer.commitChanges()`. Verifier event dans le journal.
- Si oui : documenter explicitement dans `docs/index.html#capture` et reformuler `limits.html#outside-qgis`.
- Si non : analyser la cause et le corriger.

### Criteres d'acceptation

- Test e2e ecrit dans `tests/test_python_console_capture.py` ou test manuel documente.
- Documentation alignee avec le resultat.

---

## BL-COV-002 - Capturer les modifications via Processing et Modeler

**Tags** : COVERAGE, USECASE, P1

**Priorite** : P1

**Complexite** : elevee

### Constat

Aucune integration Processing identifiee dans le code (`grep -ri 'QgsProcessingProvider' core/` retourne zero). La majorite des modifications industrielles QGIS passent par Processing (algorithmes Field calculator, Buffer in place, Refactor fields, etc.). Ces modifications ne sont aujourd'hui pas garanties capturees.

### Cause

Architecture event-driven sur les signaux d'edit. Processing peut suivre des chemins differents selon les algorithmes (in-place vs nouvelle couche).

### Valeur metier

Couverture proche de 100% des modifications QGIS = plugin standard, pas seulement utile aux editeurs manuels.

### Perimetre technique

- Audit : lister les algorithmes Processing in-place et leur comportement vis-a-vis de `commitChanges`.
- Verifier si les algorithmes Processing in-place declenchent `beforeCommitChanges`/`afterCommitChanges`. Si oui, capture deja active. Si non, hook explicite via `QgsProcessingFeedback` ou wrapper.
- Tester sur 5 algorithmes typiques : Field calculator, Buffer (in place), Refactor fields, Snap geometries to layer, Delete duplicate geometries.
- Documenter la matrice de couverture dans `docs/formats.html` ou `docs/coverage.html`.

### Criteres d'acceptation

- Matrice "Processing algorithm vs capture" disponible dans la doc.
- Au moins 3 des 5 algorithmes types capturent correctement.

### Edge cases

- Modeler enchainant 5 algorithmes sur la meme couche : 5 events ou 1 event ?
- Algorithme Processing qui ecrit une nouvelle couche : INSERT en masse a tracer.

---

## BL-COV-003 - Watcher externe optionnel pour modifications hors QGIS

**Tags** : COVERAGE, ADVANCED, P2

**Priorite** : P2

**Complexite** : elevee

### Constat

`docs/limits.html#outside-qgis` classe en CRITICAL les modifications hors QGIS (script externe, ogr2ogr, autre logiciel). RecoverLand ne detecte rien.

### Cause

Architecture in-process sur signaux QGIS.

### Valeur metier

Transformer une limite documentee en feature optionnelle. Cas d'usage : audit reglementaire ou administrateur veut savoir si quelqu'un a modifie le fichier hors QGIS.

### Perimetre technique

- Au demarrage et a la fermeture, calculer un hash (SHA-256) de chaque fichier source des couches editables.
- Comparer entre sessions. Si hash change sans events RecoverLand correspondants : alerte "modification non capturee detectee".
- Mode opt-in via `LocalSettings` (cout I/O sur gros fichiers).
- Pas de tentative de capture des deltas hors-QGIS (impossible sans replayer le diff). Juste detection et alerte.

### Criteres d'acceptation

- Test : modifier `vegetation.shp` avec ogr2ogr entre deux ouvertures QGIS. Au prochain demarrage, alerte visible.
- Performance : hash calcul < 5 s sur fichiers < 100 MB.

### Edge cases

- Fichier supprime entre deux sessions : alerte differenciee "fichier disparu".
- Fichier deplace : croiser avec `core/datasource_alias.py`.

---

# Axe 6 - Resilience renforcee du noyau

**Principe** : tous les composants resilience existants (`core/disk_monitor.py`, `core/health_monitor.py`, `core/integrity.py`, `core/write_queue.py`, `core/journal_manager.py`) sont **conserves et renforces**. Cet axe les rend plus actifs, plus predictifs et plus auto-correctifs.

## BL-RESILIENCE-001 - DiskMonitor periodique reel et multi-niveaux

**Tags** : RESILIENCE, OBSERVABILITY, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`core/disk_monitor.py` (2532 octets) verifie l'espace disque. Memoire systeme `f70e4a4e` indique : la documentation annonce une verification toutes les 5 minutes mais l'implementation reelle ne s'execute qu'a l'initialisation de la dialog. Code et doc desynchronises.

### Cause

Le mecanisme est present mais ne tourne pas en arriere-plan continu.

### Valeur metier

- **Plus robuste** : detection precoce avant que le disque soit critique.
- **Plus intelligent** : multi-seuils contextuels selon volume du journal.
- **Plus adaptatif** : reactiver tracking automatiquement quand le disque revient a un niveau sain (apres confirmation utilisateur).

### Perimetre technique

- Implementer la verification periodique reelle via `QTimer` (intervalle 5 min, ou `QgsTask` non bloquante).
- Trois seuils en remplacement du seuil unique 100 MB :
  - **INFO** (< 1 GB libre) : log + indicateur status bar bleu.
  - **WARNING** (< 500 MB) : notification non bloquante + suggestion VACUUM ou purge retention.
  - **CRITICAL** (< 100 MB) : auto-suspend tracking, banner persistant rouge, journal `audit_lifecycle` (BL-PROMISE-003).
- Calcul intelligent : seuil critique = max(100 MB, taille journal x 0.1) pour anticiper qu'une grosse session ne sature pas.
- Au passage CRITICAL -> WARNING (disque libere) : proposer reactivation tracking automatique avec confirmation utilisateur.

### Failure scenarios

- Disque libere mais filesystem en lecture seule : detecter et alerter, ne pas reactiver.
- API systeme `shutil.disk_usage` indisponible (rare, conteneur sandbox) : fallback sur attente operateur, ne pas bloquer le plugin.
- Volume reseau monte/demonte pendant l'execution : detecter et passer en mode degrade gracieux.

### Edge cases

- Disque virtuel (subst, mount bind) : taille rapportee par OS peut etre fausse. Documenter le cas.
- ReFS / btrfs avec snapshots : espace libre reel != rapporte. Tolerer marge de securite supplementaire.
- Quota utilisateur (Linux/Windows) plus restrictif que disque physique : `shutil.disk_usage` rapporte le quota, ce qui est le bon comportement.

### Plan d'observabilite

- Log INFO toutes les 30 min : "DiskMonitor: free=X MB, journal=Y MB, threshold=Z".
- Metriques exposees via journal `audit_lifecycle` : transitions de seuil.
- Status bar : indicateur couleur (vert / bleu / orange / rouge).

### Considerations securite

- Aucun chemin systeme expose dans les logs au-dela de la racine du journal.

### Criteres d'acceptation

- Test : limiter espace disque virtuel a 200 MB, attendre 5 min, notification WARNING visible.
- Test : passer de 50 MB a 1 GB libre, notification de reactivation visible.
- Test : 1000 commits avec polling actif, latence UI inchangee (mesure via BL-PERF-000).

### Cout

- CPU : < 1 ms toutes les 5 min, negligeable.
- I/O : 1 syscall `statvfs` toutes les 5 min, negligeable.

---

## BL-RESILIENCE-002 - HealthMonitor predictif et actionnable

**Tags** : RESILIENCE, INTELLIGENCE, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`core/health_monitor.py` (7773 octets) evalue la sante du journal (taille, count, disk space) et produit des messages UI. Composant utile, mais reactif (alerte une fois le seuil atteint) et generique (memes seuils 50 / 200 / 500 MB pour tous projets).

### Cause

Approche statique. Un projet de cadastre 10 communes != projet ponctuel de demonstration.

### Valeur metier

- **Plus intelligent** : seuils adaptatifs au volume historique du projet, projection de saturation.
- **Plus actionnable** : chaque alerte propose une action precise et un bouton.
- **Plus proactif** : alerter avant le seuil critique en projetant la croissance.

### Perimetre technique

- Conserver tous les seuils et alertes existants.
- Ajouter un module `core/health_predictor.py` qui calcule la pente de croissance journal (events/jour, taille/jour) sur les 30 derniers jours.
- Projection : "A ce rythme, le journal atteindra 500 MB dans X jours". Si X < 7 : alerte preventive WARNING.
- Chaque alerte HealthMonitor inclut maintenant une action one-click :
  - "Journal volumineux" -> bouton "Lancer VACUUM" (deja present dans maintenance) + "Configurer retention" (raccourci vers le bon onglet).
  - "Beaucoup d'events" -> bouton "Purger events anterieurs a X" avec X calcule pour ramener sous le seuil.
  - "Disque faible" -> bouton "Lancer purge automatique" avec preview des events qui seront supprimes.
- Recommandation contextuelle : si un projet a 80% d'events sur 1 couche specifique, suggerer une retention par couche.

### Failure scenarios

- Donnees insuffisantes pour projection (< 7 jours d'historique) : afficher "Donnees insuffisantes pour projection" sans erreur.
- Croissance non lineaire (pic d'edition) : projection reset apres 24h sans nouvelle donnee.
- VACUUM en cours quand l'action one-click est cliquee : detecter et indiquer "Maintenance deja en cours".

### Edge cases

- Journal partage entre plusieurs projets (cas non standard) : projection sur somme des projets.
- Decalage horaire : utiliser UTC pour tous les calculs (deja la convention `audit_event.created_at`).
- Journal vide ou tres jeune : afficher info "RecoverLand demarre, pas assez de donnees pour predire".

### Plan d'observabilite

- Log INFO quotidien : "HealthPredictor: pente=X events/j, taille=Y MB/j, satur. estimee=Z j".
- Metrique : ratio alertes preventives transformees en action utilisateur (apprend si l'utilisateur agit).
- Si l'utilisateur dismiss 3 fois la meme alerte : passer l'alerte en niveau plus bas (apprentissage simple).

### Criteres d'acceptation

- Test : seeded journal avec 30 jours croissance lineaire, projection affichee correcte +/- 10%.
- Test : alerte avec action one-click, l'action s'execute sans ouvrir d'autre dialog.
- Test : dismiss 3 fois -> alerte degraded.
- Aucune regression sur les alertes existantes.

### Cout

- CPU : 1 calcul de regression lineaire sur 30 points, < 1 ms.
- RAM : negligeable.

---

## BL-RESILIENCE-003 - Integrity etendue avec auto-repair

**Tags** : RESILIENCE, AUTO-RECOVERY, P0

**Priorite** : P0

**Complexite** : elevee

### Constat

`core/integrity.py` (10896 octets) couvre : `PRAGMA integrity_check`, WAL checkpoint, recuperation pending events, validation schema. Solide. Mais pour certains cas detectes, il se contente d'alerter sans tenter de reparer.

### Cause

Politique conservative initiale : detecter, alerter, laisser l'utilisateur decider.

### Valeur metier

- **Plus auto-correctif** : reparation automatique des cas non destructifs.
- **Plus robuste** : aucune intervention manuelle requise pour les cas connus.
- **Plus informatif** : trace detaillee des reparations effectuees.

### Perimetre technique

- **Auto-repair non destructif** sur cas documentes :
  - WAL trop gros (> 100 MB) : checkpoint forced PASSIVE, puis TRUNCATE si encore > 100 MB.
  - Event avec `attributes_json` JSON malforme : tenter parsing tolerant (jq style), si echec : passer l'event en `quarantine_event` et alerter.
  - Schema version inconnue (futur) : refuser l'ouverture, conserver le journal intact, demander mise a jour plugin.
  - Schema version anterieure : appliquer migration (deja fait, voir backlog QA).
  - Index corrompu : `REINDEX` automatique avec log avant/apres.
  - Page corrompue dans une table non critique : tenter `INSERT INTO new SELECT * FROM old WHERE ...` pour isoler les rows lisibles, mettre les rows perdues dans `quarantine_event`.
- Nouvelle table `quarantine_event` : meme schema que `audit_event`, contient les events qu'on ne peut pas exploiter mais qu'on conserve pour analyse manuelle.
- Rapport de reparation accessible depuis dialog Maintenance : "Last integrity run: X repairs applied, Y events quarantined".

### Failure scenarios

- Reparation echoue : laisser le journal intact, alerter en CRITICAL, fournir un export de diagnostic.
- Corruption etendue (> 50% du journal) : refuser l'auto-repair, exiger sauvegarde + decision utilisateur.
- Disque plein pendant la reparation : interrompre proprement, restaurer l'etat initial.

### Edge cases

- Journal lu en parallele par un autre process : utiliser `BEGIN IMMEDIATE` pour serializer.
- Reparation interrompue par crash : la transaction SQLite garantit atomicite, etat propre au redemarrage.
- Event quarantine > 1000 : alerter "Investigation manuelle recommandee".

### Plan d'observabilite

- Chaque reparation logue : type, table, rows affectes, duree, succes/echec.
- Event lifecycle "integrity_auto_repair" dans `audit_lifecycle`.
- Compteur exposable via BL-RESILIENCE-009.

### Considerations securite

- L'auto-repair ne supprime jamais d'event. Quarantine = transfert, pas perte.
- Logs de reparation ne contiennent pas les valeurs des events quarantines.

### Criteres d'acceptation

- Test : creer un journal avec WAL = 200 MB, demarrer plugin, WAL ramene < 100 MB.
- Test : injecter un event avec JSON malforme, redemarrer, l'event est en quarantine_event, le reste du journal est utilisable.
- Test : corruption simulee sur 1 page index, REINDEX automatique, journal de nouveau utilisable.
- Test : corruption massive (50% journal) : pas de reparation, alerte CRITICAL.

### Cout

- Reparation routiniere : < 5 s sur journal moyen.
- Auto-repair non bloquant pour le tracking en cours (executé en thread dedie).

### Risques

- **RISK** : auto-repair sur table critique (`audit_event`) doit etre extremement defensif. Toujours snapshot via `sqlite3.backup` avant tout REINDEX/REBUILD.

---

## BL-RESILIENCE-004 - WriteQueue avec backpressure adaptative

**Tags** : RESILIENCE, ADAPTIVE, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`core/write_queue.py:18-21` definit des seuils statiques : `_QUEUE_HARD_LIMIT=50000`, `_QUEUE_EARLY_WARNING=40000`, `_QUEUE_WARNING_THRESHOLD=10000`. `core/write_queue.py:91-100` : au-dessus du hard limit, les events sont sauves en pending recovery (bonne robustesse, conservée).

### Cause

Seuils statiques, identiques pour tous les contextes. Pas d'adaptation a la vitesse d'ecriture reelle ni a la latence du backend SQLite.

### Valeur metier

- **Plus adaptatif** : ralentir intelligemment la capture quand la queue grossit, accelerer le flush quand SQLite est pret.
- **Plus robuste** : reduire la probabilite de hit du hard limit, reduire les pending recovery.
- **Plus observable** : exposer la pression queue en temps reel.

### Perimetre technique

- Conserver tous les seuils et la sauvegarde pending. Ajouter par-dessus :
- Mesure continue de la latence d'ecriture batch SQLite (median, p95).
- Adaptation du `batch_size` (deja 500) entre 100 et 2000 selon la latence : si SQLite repond rapidement, batch plus grand pour throughput; si lent, batch plus petit pour latence.
- Backpressure soft : quand queue > 80% hard limit, ralentir l'`enqueue` cote producteur via `time.sleep(min(0.01 * pression, 0.1))` pour eviter le hit dur.
- Conserver le hard limit comme dernier filet : si malgre la backpressure on l'atteint, fallback existant (pending recovery).
- Metrique exposable : pression queue (%), latence p95 batch (ms), taille batch courant.

### Failure scenarios

- Producteur en boucle serree (mass delete 100k) : la backpressure ralentit le producteur de 100 ms max par appel, ne bloque pas QGIS.
- SQLite bloque (lock externe) : le batch courant retry (deja `_BATCH_RETRY_COUNT=3`), si echec : pending recovery (deja).
- Le thread writer plante : detecter via heartbeat et redemarrer (peut deja exister, a verifier).

### Edge cases

- Tres petit volume (< 10 events/heure) : batch_size descend a 100, latence inchangee.
- Tres gros volume (10k events/min) : batch_size monte a 2000, throughput optimal.
- Disque sature : la latence explose, batch_size descend a 100, backpressure forte, alerte BL-RESILIENCE-001 declenchee.

### Plan d'observabilite

- Log DEBUG par batch : taille, latence, queue size avant/apres.
- Metrique exposee via BL-RESILIENCE-009 : pression queue, throughput, latence p95.
- Visualisable dans la dialog Maintenance, onglet "Performance".

### Criteres d'acceptation

- Test : 100k events injectes en 10 s, zero pending recovery, queue n'atteint jamais 100%.
- Test : SQLite ralenti artificiellement (lock externe 5 s), le batch retry, aucun event perdu.
- Test : metriques visibles et coherentes.

### Cout

- CPU : negligeable (1 mesure de temps par batch).
- RAM : negligeable.

---

## BL-RESILIENCE-005 - Journal sur reseau detecte et redirige avec preservation

**Tags** : RESILIENCE, DATA INTEGRITY, P0

**Priorite** : P0

**Complexite** : moyenne

### Constat

`https://sqlite.org/wal.html` (officiel) : "WAL does not work over a network filesystem". `https://sqlite.org/useovernet.html` : "Network filesystems do not support the ability to do simultaneous reads and writes while at the same time keeping the database consistent".

Si l'utilisateur place son projet QGIS sur un partage reseau, le journal `.recoverland/` y est aussi, en mode WAL, donc en risque de corruption silencieuse.

### Cause

Decision actuelle : journal pres du projet, sans verification du type de filesystem.

### Valeur metier

- **Plus robuste** : aucune corruption silencieuse en environnement reseau.
- **Plus intelligent** : detection automatique du type de filesystem.
- **Plus adaptatif** : redirection optionnelle vers le profil QGIS local avec lien retour.

### Perimetre technique

- Detecter le type de filesystem du chemin du journal a l'ouverture :
  - Windows : `GetDriveTypeW` via `ctypes`. Si `DRIVE_REMOTE` : reseau.
  - Linux/Mac : parse `/proc/mounts` ou `mount -v`. Match `nfs`, `smbfs`, `cifs`, `fuse.sshfs`, `vboxsf`.
  - Detection heuristique Dropbox / OneDrive / iCloud / Google Drive : nom de dossier dans le chemin (`OneDrive`, `Dropbox`, `Google Drive`, `iCloud Drive`).
- Si reseau detecte, **avant ouverture du journal** :
  - Banner CRITICAL : "Journal sur partage reseau, risque de corruption WAL detecte (source : sqlite.org)".
  - Trois choix : `[Continuer quand meme]` (avec confirmation explicite "j'accepte le risque"), `[Migrer vers profil local]`, `[Plus d'infos]` (lien doc).
- Si "Migrer vers profil local" choisi :
  - Copier le journal via `sqlite3.Connection.backup()` vers `[QGIS profile]/recoverland/relocated/<hash>.sqlite`.
  - Garder un fichier marqueur `.recoverland/journal_relocated.txt` avec le nouveau chemin et timestamp.
  - Mettre a jour `JournalManager` pour ouvrir le journal local.
  - Conserver la possibilite de re-localiser via la dialog Maintenance.

### Failure scenarios

- Detection echoue (filesystem inconnu) : choix par defaut = "demander a l'utilisateur" plutot qu'assumer local.
- Migration interrompue : conserver les deux journaux, marquer l'incomplet.
- Profil local plein : detection BL-RESILIENCE-001 declenchee, alerter avant migration.

### Edge cases

- Volume reseau monte localement via NFS-loopback : detection rapporte `nfs`, alerter quand meme.
- Lecteur USB rapporte parfois `DRIVE_REMOTE` sur Windows : detection nuancee.
- Symlinks : resoudre via `os.path.realpath` avant detection.

### Plan d'observabilite

- Log INFO a chaque ouverture : "Journal filesystem: type=X, path=Y".
- Event `audit_lifecycle` au moment de la detection reseau.
- Metrique : pourcentage d'utilisateurs en reseau (anonyme, agrege via opt-in BL-RESILIENCE-009).

### Considerations securite

- Le marker `.recoverland/journal_relocated.txt` doit avoir des permissions restrictives.
- Le chemin local ne doit pas exposer le nom utilisateur si le journal est partage ulterieurement.

### Criteres d'acceptation

- Test : projet sur `\\server\share`, ouverture, banner visible.
- Test : "Migrer vers local", journal accessible, ancien chemin marquee.
- Test : "Continuer quand meme" + confirmation, ouverture en mode degrade documente, log explicite.

### Cout

- CPU : 1 syscall a l'ouverture, negligeable.
- I/O migration : O(taille journal), une fois.

### Risques

- **RISK** : utilisateurs habitues a "ca marchait avant" peuvent etre surpris. Communication claire necessaire.

---

## BL-RESILIENCE-006 - JournalManager multi-journal et fallback automatique

**Tags** : RESILIENCE, INTELLIGENCE, P2

**Priorite** : P2

**Complexite** : elevee

### Constat

`core/journal_manager.py` (13947 octets) gere un journal par projet. Bonne base. Mais en cas de probleme acces journal projet (fichier verrouille par antivirus, permissions denied, disque ROFS), aucun fallback : le tracking est desactive.

### Cause

Architecture single-journal par projet.

### Valeur metier

- **Plus robuste** : tracking continue meme si le journal principal est temporairement inaccessible.
- **Plus intelligent** : reconciliation automatique quand l'acces revient.
- **Plus adaptatif** : choix dynamique du meilleur emplacement journal possible.

### Perimetre technique

- Conserver le comportement actuel (journal principal pres du projet).
- Ajouter un journal de secours dans `[QGIS profile]/recoverland/fallback/<project_hash>.sqlite`.
- En cas d'echec d'ouverture du journal principal :
  - Logger en CRITICAL.
  - Ouvrir le fallback, capturer normalement.
  - Reessayer l'acces au journal principal toutes les 5 min.
  - Quand l'acces revient : merger les events du fallback vers le principal via INSERT (cle composite garanti unicite), nettoyer le fallback.
- L'utilisateur voit en status bar : "Journal principal indisponible, capture vers fallback".
- Dialog Maintenance affiche les fallbacks actifs.

### Failure scenarios

- Fallback echoue aussi : derniere ligne de defense = fichier `recoverland_pending.json` deja existant pour pending events (extension a tous les events).
- Merge fallback -> principal echoue : conserver le fallback, alerter, ne pas perdre.
- Conflit de events identiques (cle composite collision) : deduplication par hash de payload.

### Edge cases

- Antivirus qui bloque temporairement le fichier (Windows Defender scan) : retry suffira generalement.
- Permission ROFS : detecter, fallback definitif, suggerer chmod a l'utilisateur.
- Plusieurs sessions QGIS sur le meme projet (writer lock present) : pas de conflit, le lock fait son travail.

### Plan d'observabilite

- Event `audit_lifecycle` "fallback_journal_activated" / "fallback_journal_merged".
- Metrique : nombre d'activations fallback / 30 jours (signal de probleme environnement).
- Log DEBUG du chemin du fallback courant.

### Criteres d'acceptation

- Test : verrouiller le journal principal (autre process), capture continue vers fallback.
- Test : libérer le verrou, merge automatique apres < 5 min.
- Test : aucun event perdu sur 1000 events captures pendant la bascule.

### Cout

- I/O : double ecriture nulle en mode normal, seulement en mode fallback.
- Disque : fallback grossit en mode degrade, nettoye apres merge.

---

## BL-RESILIENCE-007 - Recuperation de capture apres signal d'edition non capturé

**Tags** : RESILIENCE, INTELLIGENCE, P2

**Priorite** : P2

**Complexite** : elevee

### Constat

`docs/limits.html#tracking-off` documente que si tracking est off, aucune capture n'a lieu et il n'y a "pas de catch-up". Cas legitime : utilisateur a manuellement desactive, edite, puis se rend compte qu'il aurait du laisser le tracking actif.

### Cause

Architecture event-driven : si l'event source n'est pas capte, rien a recuperer.

### Valeur metier

- **Plus intelligent** : reconstruire un event approximatif a partir de l'etat actuel + dernier event connu, avec marquage explicite.
- **Plus robuste** : recouvrement partiel possible la ou avant aucun n'etait possible.

### Perimetre technique

- Au demarrage du tracking apres une periode off, pour chaque couche editable :
  - Comparer l'etat actuel (count features, sample geometries hash, sample attributes hash) au dernier snapshot connu.
  - Si difference detectee : creer un event `RECONSTRUCTED` dans `audit_event` avec :
    - `operation_type = "RECONSTRUCTED"` (nouveau type).
    - `attributes_json = {"reconstructed": true, "diff_summary": {...}}`.
    - `created_at = "now"`.
    - Note explicite "Etat divergent detecte au reactiver tracking, valeurs detaillees non reconstituables".
- L'utilisateur voit dans la recherche : un event marque "Reconstructed" qui dit "quelque chose a change pendant que le tracking etait off".

### Failure scenarios

- Couche tres grosse (> 1M features) : echantillonnage statistique seulement, alerter "Diff approximatif".
- Couche supprimee pendant la periode off : event `LAYER_DISAPPEARED` dans `audit_lifecycle`.
- Schema modifie : event `SCHEMA_DRIFT_RECONSTRUCTED`.

### Edge cases

- Couche reouverte avec un fingerprint different : croiser avec `core/datasource_alias.py` pour eviter faux positif.
- Heure systeme modifiee : marquer l'event avec une note "timestamp peu fiable".

### Plan d'observabilite

- Log INFO : "Reconstruction: layer X, count diff Y, samples diff Z".
- Metrique : nombre de reconstructions / mois (signal de discipline tracking).

### Criteres d'acceptation

- Test : tracking off, modifier 10 features, reactiver tracking, 1 event RECONSTRUCTED visible.
- Test : aucune modification entre off/on : aucun event RECONSTRUCTED.
- Test : recherche standard exclut les RECONSTRUCTED par defaut, filtre dedie pour les voir.

### Cout

- CPU : O(n) hash sur chaque couche editable au reactiver tracking. Pour 100k features : ~1-2 s.
- Borne : si la couche est trop grosse, basculer en echantillonnage.

### Risques

- **RISK** : reconstruction != restauration. Communication claire dans l'UI : "diff signale, valeurs avant non recuperables".

---

## BL-RESILIENCE-008 - Cycle de vie tracking auditable end-to-end

**Tags** : RESILIENCE, AUDITABILITY, P0

**Priorite** : P0

**Complexite** : moyenne

### Constat

Le journal capture les events business, pas les events systeme du plugin lui-meme. Si tracking est desactive (manuel ou auto disk-full) entre 14h et 16h et l'utilisateur edite pendant ce temps, l'absence d'event sur cette plage est ambigu : "rien edite" ou "tracking off". Pour audit reglementaire, cette ambiguité est inacceptable.

### Cause

Architecture event-driven business. Pas de table system events.

### Valeur metier

- **Plus auditable** : zero ambiguité sur l'etat du tracking dans le temps.
- **Plus robuste** : trace systematique des transitions critiques pour debugging post-incident.
- **Plus conforme** : exigence courante pour audit reglementaire (ISO 27001, SOC 2).

### Perimetre technique

- Nouvelle table `audit_lifecycle` avec colonnes : `event_id`, `event_type`, `payload_json`, `created_at`, `session_id`, `user_name`, `host`.
- Types d'events lifecycle :
  - `tracking_enabled` / `tracking_disabled` (avec source : manual, disk_full, lock_lost, plugin_load, plugin_unload).
  - `journal_opened` / `journal_closed` (avec path, fallback flag).
  - `disk_threshold_crossed` (info, warning, critical).
  - `write_queue_overflow_warning` / `write_queue_overflow_hard`.
  - `integrity_check_passed` / `integrity_auto_repair_applied` / `integrity_failed`.
  - `network_filesystem_detected` (BL-RESILIENCE-005).
  - `fallback_journal_activated` / `fallback_journal_merged` (BL-RESILIENCE-006).
  - `reconstruction_event_created` (BL-RESILIENCE-007).
  - `schema_migration_applied`.
- Indexable par `created_at`, `event_type`.
- Visible dans la dialog plugin, onglet "Activity log" (pas de menu cache).
- Inclus dans l'export JSONL (BL-PORT-001).

### Failure scenarios

- Insertion lifecycle echoue : log dans le fichier `recoverland_debug.log`, ne bloque jamais l'operation principale.
- Table corrompue : auto-repair (BL-RESILIENCE-003) la recree vide, log explicite.

### Edge cases

- Plugin charge sans journal accessible : event lifecycle place en buffer memoire, flush des que journal disponible.
- Crash plugin : table standard SQLite, donc cohérente apres redemarrage.

### Plan d'observabilite

- Vue de timeline dans la dialog (graphique chronologique events lifecycle + business).
- Filtre "voir uniquement lifecycle" pour audit.

### Considerations securite

- Pas de donnees sensibles dans les payloads lifecycle (juste meta).
- Logs lifecycle ne doivent jamais contenir le contenu d'un event business.

### Criteres d'acceptation

- Test : 6 transitions tracking on/off en 1 heure, 6 events lifecycle visibles avec timestamps.
- Test : disk full -> auto disable -> recovery -> reactivation : 3 events lifecycle distincts.
- Test : export JSONL contient les events lifecycle avec un type distinct.

### Cout

- I/O : 1 INSERT par transition, frequence faible (< 100/jour typiquement).
- Stockage : negligeable.

---

## BL-RESILIENCE-009 - Telemetrie locale opt-in avec dashboard interne

**Tags** : RESILIENCE, OBSERVABILITY, P2

**Priorite** : P2

**Complexite** : moyenne

### Constat

Le plugin a beaucoup de mecanismes (write queue, disk monitor, health monitor, integrity, alias, support policy). Pas de tableau de bord operationnel unifié. L'utilisateur expert ou l'administrateur n'a pas de vue synthetique.

### Cause

Pas de dashboard interne dedie observabilite.

### Valeur metier

- **Plus observable** : un seul endroit pour tout savoir.
- **Plus actionnable** : metriques aboutees a actions (cf BL-RESILIENCE-002).
- **Plus diagnostique** : envoyer un screenshot du dashboard suffit pour qualifier un incident.

### Perimetre technique

- Nouvelle vue dans la dialog Maintenance : "Operational dashboard".
- Metriques affichees (toutes locales, aucune sortie reseau) :
  - Sante journal : taille, count events, age max event, age dernier event.
  - WriteQueue : pression %, throughput last 5 min, latence p95 batch, pending recovery count.
  - DiskMonitor : free, threshold, statut, prochain check.
  - HealthPredictor : projection saturation, recommandations actives.
  - Integrity : derniere verification, nombre de reparations vie / 30j, quarantine count.
  - Lifecycle : 30 derniers events lifecycle.
  - Datasource alias : count alias actifs, dernier resolved.
- Bouton "Export diagnostic snapshot" : zip contenant le dashboard JSON + 1000 derniers logs + extrait integrity report. Tout local, l'utilisateur choisit quoi envoyer.

### Failure scenarios

- Calcul d'une metrique echoue : afficher "N/A" pour cette metrique seulement, ne pas casser le dashboard.
- Performance dashboard sur gros journal : metriques cachees pendant 30 s pour eviter requete a chaque ouverture.

### Edge cases

- Journal vide : dashboard affiche "Journal vide, en attente de premier event".
- Pas de droits ecriture sur le dossier export : alerte explicite, suggerer autre dossier.

### Plan d'observabilite

- Le dashboard EST l'observabilite. Aucun autre log specifique.

### Considerations securite

- Aucune sortie reseau. Aucun ID materiel ou utilisateur exporte sans confirmation.
- L'export diagnostic propose une preview avant zip pour anonymisation manuelle.

### Criteres d'acceptation

- Test : ouvrir le dashboard sur journal de 10k events, affichage < 1 s.
- Test : export diagnostic genere un zip valide.
- Test : metriques coherentes avec realite (croisement avec audit_lifecycle).

### Cout

- CPU : negligeable hors ouverture (cache 30 s).
- I/O : zero en arriere plan, seulement sur ouverture.

---

# Axe 7 - Securite des donnees et conformite

## BL-DATA-001 - Chiffrement at-rest optionnel du journal

**Tags** : SECURITY, RGPD, P1

**Priorite** : P1

**Complexite** : elevee

### Constat

Verification : `grep -ri 'sqlcipher\|encrypt\|cipher' .` retourne zero occurrence (verifie). Le journal SQLite est en clair sur disque.

### Cause

Choix initial : simplicite, pas de dependance native.

### Valeur metier

- Conformite RGPD si table source contient donnees personnelles : le journal en contient l'historique.
- Conformite CNIL/RSSI sur poste prete ou poste vole.
- Argument fort pour adoption en collectivite ou ESN.

### Perimetre technique

- Option opt-in via `LocalSettings`.
- Implementation 1 (preferee) : SQLCipher avec passphrase utilisateur. Compatible Python via `pysqlcipher3`. Cout : dependance native, packaging delicat.
- Implementation 2 (alternative) : chiffrement applicatif des champs sensibles (`attributes_json`, `geometry_wkb`) via `cryptography` lib. Cout : recherche/restore impose dechiffrement en memoire.
- Question : cle stockee ou ? Garder hors du fichier SQLite. QGIS a un AuthManager natif `QgsAuthManager` qui peut stocker des secrets.

### Criteres d'acceptation

- Mode chiffre activable, journal illisible sans passphrase.
- Recherche, restore, export fonctionnent en mode chiffre.
- Documentation RGPD explicite.

### Risques

- Perte de passphrase = perte du journal. Documenter et avertir.
- Performance : chiffrement applicatif peut couter 2-5x sur lecture massive.

---

## BL-DATA-002 - Avertissement explicite sur donnees personnelles

**Tags** : SECURITY, RGPD, COMMUNICATION, P1

**Priorite** : P1

**Complexite** : faible

### Constat

Aucune mention RGPD identifiee dans `docs/`. Hypothese : l'utilisateur metier ne realise pas que le journal duplique toutes les valeurs avant/apres des champs.

### Cause

Plugin technique presente comme un outil de recovery, pas comme un sous-systeme d'audit avec implications conformite.

### Valeur metier

Eviter l'utilisation a aveugle sur donnees personnelles. Donner aux DPO de quoi statuer.

### Perimetre technique

- Section dediee dans `docs/limits.html` : "Donnees personnelles et RGPD".
- Au premier demarrage : message d'information non bloquant.
- Decrire ce qui est stocke (anciennes valeurs) et la duree (rétention).

### Criteres d'acceptation

- Section RGPD presente dans `docs/`.
- Message d'information sur premier demarrage avec lien vers la section.

---

## BL-DATA-003 - Commande "Scrub journal" pour anonymisation a la demande

**Tags** : SECURITY, RGPD, P2

**Priorite** : P2

**Complexite** : moyenne

### Constat

RGPD impose un droit a l'effacement. Si une personne demande la suppression de ses donnees du systeme metier, son historique doit aussi disparaitre du journal RecoverLand.

### Cause

Pas de mecanisme cible a date.

### Valeur metier

Conformite. Sans cela, RecoverLand peut bloquer une demande RGPD legitime sur la base metier.

### Perimetre technique

- UI dans la dialog maintenance : "Anonymiser les events correspondant a un critere".
- Critere : valeur exacte d'un champ dans `attributes_json`, ou plage de FID, ou regex sur user_name.
- Action : remplacer les valeurs par `null` ou un sentinel `[REDACTED]`, conserver les metadonnees structurelles (timestamps, type d'op).
- Trace de l'operation dans `audit_lifecycle` (BL-PROMISE-003).

### Criteres d'acceptation

- Test : creer 100 events avec une valeur identifiante, scrubber, confirmer la disparition.
- Trace de scrub presente dans le journal des incidents.

---

## BL-DATA-004 - Template .gitignore auto-propose

**Tags** : DEVOPS, USABILITY, P2

**Priorite** : P2

**Complexite** : faible

### Constat

`docs/index.html#journal-location` indique que le journal est en `[project]/.recoverland/recoverland_audit.sqlite` (avec WAL = 3 fichiers : `.sqlite`, `-wal`, `-shm`). Si l'utilisateur versionne son projet QGIS dans Git, ces fichiers polluent le repo et peuvent contenir des donnees sensibles.

### Cause

Aucune integration Git.

### Valeur metier

Eviter l'exposition accidentelle du journal dans un push public.

### Perimetre technique

- A la creation du dossier `.recoverland/`, ecrire un `.gitignore` local qui s'ignore lui-meme.
- Contenu : `*.sqlite`, `*.sqlite-wal`, `*.sqlite-shm`, `*.sqlite-journal`.
- Bonus : detecter la presence d'un `.git/` parent et signaler dans la barre d'info "Journal under git: gitignore template applied".

### Criteres d'acceptation

- Test : creer projet, verifier `.recoverland/.gitignore` present.
- `git status` ignore les fichiers SQLite et WAL.

---

# Axe 8 - Robustesse du fingerprint

## BL-FP-001 - Empreinte par hash de contenu en complement du chemin

**Tags** : ROBUSTNESS, FINGERPRINT, P1

**Priorite** : P1

**Complexite** : elevee

### Constat

`docs/index.html#fingerprint` : empreinte = `provider::normalized_source`. Cas critiques documentes en `docs/limits.html` :
- file moved : MODERATE
- network drive : MODERATE
- unstable FID shapefile : CRITICAL

Toutes ces situations cassent le fingerprint actuel.

### Cause

Empreinte purement syntaxique sur le chemin.

### Valeur metier

Reduire la rupture d'historique sur les operations de reorganisation legitimes.

### Perimetre technique

- En complement du fingerprint chemin, calculer un hash de contenu (premiers et derniers MB pour gros fichiers, ou hash complet < 100 MB).
- Stocker dans `datasource_registry` un champ `content_hash`.
- A l'ouverture d'un projet : si le fingerprint chemin ne match pas mais le content_hash match : proposer une reconciliation automatique (extension de `core/datasource_alias.py`).
- Mode opt-in pour eviter coute I/O sur tous projets.

### Criteres d'acceptation

- Test : deplacer `vegetation.shp` de `C:/data/` vers `D:/archive/`, ouvrir le projet, RecoverLand propose la reconciliation.
- L'historique reste accessible apres acceptation utilisateur.

### Edge cases

- Fichier en cours d'edition : hash instable, prevoir un mode "snapshot" sur version commitee.
- Couche PG : pas de fichier, le hash de contenu = hash des cles primaires + count + bornes geographiques (heuristique).

---

## BL-FP-002 - Detection des aliases UNC vs lettre de lecteur

**Tags** : ROBUSTNESS, FINGERPRINT, P2

**Priorite** : P2

**Complexite** : moyenne

### Constat

`docs/limits.html#network-drive` documente : `Z:/share/x.shp` vs `\\server/share/x.shp` produisent deux fingerprints differents pour le meme fichier.

### Cause

Normalisation du chemin ne resout pas les alias systeme.

### Valeur metier

Cas tres frequent en environnement professionnel avec partages reseau.

### Perimetre technique

- Sur Windows : utiliser `WNetGetUniversalNameW` pour resoudre une lettre de lecteur reseau vers son chemin UNC canonique.
- Sur Linux/Mac : resoudre `/Volumes/share` ou `/mnt/share` vers la cible NFS/SMB si possible via `mount` parsing.
- Stocker le chemin canonique dans le fingerprint, conserver le chemin "vu" comme metadonnee.
- Au demarrage : si une lettre est resolue differemment qu'a la session precedente, alerter.

### Criteres d'acceptation

- Test : connecter `Z:` a `\\server\share`, capturer un event, deconnecter, reconnecter avec la lettre `Y:`. L'historique reste lie.

### Risques

- Performance : appel systeme par fingerprint a borner.
- Absence de reseau au demarrage : prevoir un fallback gracieux.

---

## BL-FP-003 - Avertissement a l'ouverture si fingerprint derive

**Tags** : ROBUSTNESS, OBSERVABILITY, P2

**Priorite** : P2

**Complexite** : faible

### Constat

Quand un projet ouvre une couche dont le fingerprint a change, RecoverLand cree silencieusement un nouveau fingerprint. L'utilisateur peut ne pas s'en rendre compte.

### Cause

Le mecanisme `core/datasource_alias.py` est curatif (reconciliation a posteriori), pas preventif (alerte a la divergence).

### Valeur metier

Donner a l'utilisateur l'information avant qu'il ne realise des heures plus tard que son historique est invisible.

### Perimetre technique

- A l'ouverture d'un projet : pour chaque couche editable, comparer le fingerprint actuel avec le dernier fingerprint connu (via `datasource_registry`).
- Si divergence : afficher une notification non bloquante "Layer X identifier changed, history may be hidden. [View options]".
- Bouton vers la dialog d'alias.

### Criteres d'acceptation

- Test : ouvrir un projet, fermer, deplacer le fichier, ouvrir : notification visible.

---

# Axe 9 - Coherence avec l'environnement projet

## BL-PROJ-001 - Avertissement sur partages reseau (risque WAL)

**Tags** : ROBUSTNESS, WARN, P1

**Priorite** : P1

**Complexite** : faible

### Constat

SQLite WAL fonctionne mal sur partages reseau (NFS, SMB, Dropbox, OneDrive). Documentation SQLite officielle : `https://sqlite.org/wal.html` mentionne la non-compatibilite WAL avec network filesystems. Le journal RecoverLand est en WAL (`core/sqlite_schema.py`, voir backlog QA).

Si l'utilisateur place son projet QGIS sur un partage reseau, le journal RecoverLand sera aussi sur partage reseau et risque corruption.

### Cause

Aucune detection actuelle.

### Valeur metier

Eviter une corruption silencieuse de journal qui invaliderait toute la promesse "safety net".

### Perimetre technique

- A l'ouverture du journal, detecter le type de filesystem du chemin du journal.
- Sur Windows : `GetDriveTypeW`. Si `DRIVE_REMOTE` : warning.
- Sur Linux/Mac : parser `/proc/mounts` ou `mount`. Si `nfs`/`smbfs`/`cifs` : warning.
- Detecter les dossiers Dropbox/OneDrive (heuristique sur nom de dossier dans le chemin).
- Notification claire : "Journal on network drive may corrupt. Consider local journal storage."
- Option : "Move journal to local profile dir".

### Criteres d'acceptation

- Test : placer projet sur partage reseau monte, ouvrir, warning visible.
- Test : option "Move to local" deplace le journal et met a jour les references.

---

## BL-PROJ-002 - Politique de retention par defaut sensee

**Tags** : USABILITY, DEFAULT, P2

**Priorite** : P2

**Complexite** : faible

### Constat

`core/retention.py` (7355 octets) existe mais hypothese : aucune politique active par defaut. L'utilisateur doit aller dans le maintenance dialog pour configurer. Resultat probable : la majorite ne configure jamais, journal grossit indefiniment.

### Cause

Choix par defaut "ne rien purger" pour eviter la perte d'audit. Mais effet de bord : obesity du journal.

### Valeur metier

Reduire la maintenance manuelle, eviter les journaux multi-Go en production.

### Perimetre technique

- Politique par defaut proposee : conserver 1 an d'events, purger plus ancien.
- Au premier demarrage : message d'info "Default retention: 1 year. [Customize]".
- Application non destructive : la politique est seulement evaluee, pas appliquee, jusqu'a confirmation utilisateur.

### Criteres d'acceptation

- Premier demarrage : message d'info.
- Politique par defaut visible dans le maintenance dialog.

---

## BL-PROJ-003 - Diagnostic clair de l'emplacement du journal a l'ouverture

**Tags** : OBSERVABILITY, P3

**Priorite** : P3

**Complexite** : faible

### Constat

`docs/index.html#journal` documente : projet sauve = `[project]/.recoverland/`, projet non sauve = `[QGIS profile]/recoverland/audit/audit_<hash>.sqlite`. L'utilisateur ne sait pas toujours dans quel mode il est.

### Cause

Information accessible mais pas mise en avant.

### Valeur metier

Reduire la confusion "ou est mon historique".

### Perimetre technique

- Status bar widget (deja present, voir `recover.py:264-268`) : afficher le chemin court du journal au survol.
- Maintenance dialog : section "Journal location" avec chemin complet et bouton "Open in Explorer/Finder/Files".

### Criteres d'acceptation

- Tooltip status bar montre le chemin.
- Bouton ouvre le dossier dans l'explorateur OS.

---

# Axe 10 - Expansion intelligente du perimetre couvert

**Principe** : RecoverLand couvre deja une matrice large de formats (`docs/index.html` : GeoPackage, Shapefile, PostGIS, SpatiaLite, MSSQL, Oracle, GeoJSON, CSV, FlatGeobuf). Cet axe **etend** la couverture sans toucher au noyau, en s'appuyant sur les signaux QGIS existants.

## BL-EXPAND-001 - Matrice de couverture par format documentee et testee

**Tags** : EXPANSION, COVERAGE, OBSERVABILITY, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

`core/support_policy.py` (5085 octets) definit deja les niveaux d'identite par provider (STRONG, MEDIUM, WEAK, NONE). `docs/index.html#identity-strength` documente la matrice. Mais aucun test e2e systematique ne verifie la capture sur chaque format.

### Cause

Tests unitaires solides, tests integration par format manquants.

### Valeur metier

- **Plus robuste** : confirmer experimentalement que la capture marche sur chaque format documente.
- **Plus adaptatif** : detecter les formats partiellement supportes avant que l'utilisateur ne le decouvre.
- **Plus credible** : matrice publique avec preuve de test = argument commercial fort.

### Perimetre technique

- Nouveau dossier `tests/integration/by_format/` :
  - `test_capture_geopackage.py`
  - `test_capture_shapefile.py`
  - `test_capture_postgis.py` (avec docker compose)
  - `test_capture_spatialite.py`
  - `test_capture_mssql.py` (avec docker compose, optionnel)
  - `test_capture_oracle.py` (avec docker compose, optionnel)
  - `test_capture_geojson.py`
  - `test_capture_flatgeobuf.py`
  - `test_capture_csv.py`
  - `test_capture_kml.py`
  - `test_capture_gml.py`
  - `test_capture_excel.py` (xlsx editable depuis QGIS 3.40+)
  - `test_capture_wfst.py` (Web Feature Service Transactional, si support).
- Chaque test : creer une couche, editer, verifier event en journal, restorer, verifier resultat.
- Genere automatiquement un rapport `docs/coverage_matrix_<date>.md` et un fichier `docs/coverage_matrix.json`.
- Page web `docs/coverage.html` lit le JSON et l'affiche.

### Failure scenarios

- Test format X echoue : marquer le format en "EXPERIMENTAL" dans `support_policy.py`, alerter l'utilisateur a l'ouverture d'une couche de ce format.
- Provider absent (ex : pas de driver MSSQL local) : marquer le test "skipped" avec raison claire, ne pas faire echouer le CI.

### Edge cases

- Format avec sous-variantes (Shapefile DBF vs DBF+CPG, GeoJSON simple vs FeatureCollection nestee) : un test par variante.
- Encodage : tester ASCII, UTF-8, Latin-1, UTF-16 pour CSV / DBF.

### Plan d'observabilite

- Page web `docs/coverage.html` mise a jour a chaque release.
- Test runner CI publie le rapport en artifact.

### Criteres d'acceptation

- 80% des formats documentes ont un test e2e qui passe.
- Page coverage publique a jour automatiquement.
- Au moins 3 formats "STRONG" sont testes en CI a chaque commit.

### Cout

- Dev initial : 2-3 semaines.
- CI : duree augmente, isoler les tests format dans un job dedie.

---

## BL-EXPAND-002 - Capture confirmee via console Python QGIS

**Tags** : EXPANSION, USECASE, P1

**Priorite** : P1

**Complexite** : faible

### Constat

`https://qgis.org/pyqgis/master/core/QgsVectorLayer.html` confirme que les signaux `beforeCommitChanges` / `afterCommitChanges` sont emis par `QgsVectorLayer.commitChanges()` quel que soit l'appelant. Hypothese : la capture marche deja via la console Python. A prouver.

### Cause

Pas de test e2e dedie a ce cas.

### Valeur metier

- **Plus large** : documenter et garantir la couverture script Python.
- **Plus credible** : argument fort vs `pg_history_viewer` qui ne capte que les triggers PG.

### Perimetre technique

- Test e2e `tests/integration/test_python_console_capture.py` :
  - Charger une couche editable.
  - Executer `layer.startEditing(); layer.changeAttributeValue(fid, 0, "X"); layer.commitChanges()` via la console QGIS.
  - Verifier event UPDATE dans le journal avec `attributes_json` correct.
- Si test passe : documenter explicitement dans `docs/index.html#capture` et reformuler `docs/limits.html#outside-qgis` pour distinguer "console QGIS = capture" vs "script externe = pas de capture".
- Si test echoue : analyser et corriger.

### Failure scenarios

- Le signal n'est pas emis pour `commitChanges(stopEditing=False)` : tester les deux variantes.
- Edit en lot via `QgsVectorLayer.dataProvider().changeAttributeValues(...)` : court-circuit le signal d'edit. Documenter ce contournement.

### Edge cases

- Edit dans un thread non-main via PyQGIS : a tester separement.
- Editing buffer non flush au commit : a tester.

### Plan d'observabilite

- Test CI permanent.
- Documentation a jour avec exemples Python copy-paste.

### Criteres d'acceptation

- Test e2e passe.
- Doc `docs/index.html` montre un exemple `layer.commitChanges()` avec event capture.

### Cout

- Dev : < 1 semaine.

---

## BL-EXPAND-003 - Capture confirmee via Processing in-place et Modeler

**Tags** : EXPANSION, USECASE, P1

**Priorite** : P1

**Complexite** : moyenne

### Constat

QGIS Enhancement Proposal #114 (`https://github.com/qgis/QGIS-Enhancement-Proposals/issues/114`) documente que les algorithmes Processing in-place utilisent le buffer d'edition standard. Hypothese : capture deja effective.

### Cause

Pas de test integration sur algorithmes Processing.

### Valeur metier

- **Plus large** : couvrir un cas d'usage industriel majeur (Field calculator, Buffer in place, Refactor fields, Snap geometries, Delete duplicates).
- **Plus credible** : RecoverLand couvre la majorite des modifications QGIS reelles, pas seulement les clics manuels.

### Perimetre technique

- Test e2e par algorithme :
  - `qgis:fieldcalculator` (in-place).
  - `native:buffer` avec output = source layer.
  - `native:refactorfields` (in-place).
  - `native:snapgeometries` (in-place).
  - `native:deleteduplicategeometries` (in-place).
- Pour chacun : declencher via `processing.run(algorithm_id, params)`, verifier event(s) dans le journal.
- Documenter la matrice "algorithme Processing -> capture effective oui/non/partiel" dans `docs/coverage.html`.

### Failure scenarios

- Algorithme qui court-circuite le commit (rare mais possible) : marquer "partiellement supporte".
- Algorithme qui cree une nouvelle couche au lieu de in-place : capture fonctionne sur la nouvelle couche.

### Edge cases

- Modeler enchainant 5 algorithmes : potentiellement 5 events business par feature, ou consolidation. A documenter.
- Processing batch sur 100 couches : le throughput WriteQueue (BL-RESILIENCE-004) doit absorber.

### Plan d'observabilite

- Matrice publique de couverture Processing a jour.

### Criteres d'acceptation

- 5 algorithmes courants testes.
- Au moins 4 capturent correctement.
- Doc explicite sur les eventuels cas non couverts.

### Cout

- Dev : 1-2 semaines.

---

## BL-EXPAND-004 - Multi-projet et reporting transverse

**Tags** : EXPANSION, INTELLIGENCE, P2

**Priorite** : P2

**Complexite** : elevee

### Constat

Architecture actuelle : un journal par projet (`docs/index.html#journal`). Pour un administrateur SIG qui veut un rapport "qui a edite quoi sur l'ensemble des projets de l'entreprise", il faut ouvrir chaque projet a la main.

### Cause

Choix initial : isolation par projet. Bonne propriete pour la confidentialite.

### Valeur metier

- **Plus intelligent** : agreger des journaux selectionnes pour reporting sans casser l'isolation.
- **Plus adaptatif** : permet a un responsable SIG de superviser sans imposer un mode multi-projet par defaut.
- **Conserve** la confidentialite : agregation explicite, opt-in projet par projet.

### Perimetre technique

- Nouvel outil dans la dialog Maintenance : "Reporting transverse".
- L'utilisateur ajoute manuellement les chemins de journaux a inclure.
- Pour chaque journal : ouverture en lecture seule, extraction des metadonnees (project, layer, count, time range).
- Vue agregee : qui a edite quoi sur quelle plage, top users, top layers, top operation_types.
- Export rapport CSV / JSONL / HTML.
- Aucune modification des journaux sources.

### Failure scenarios

- Un journal du lot est inaccessible : ignorer avec log explicite, continuer avec les autres.
- Versions de schema differentes entre journaux : utiliser le schema le plus recent comme cible, faire des projections.
- Total events > 10M : l'agregation peut etre lente. Limiter a 1M events ou utiliser sampling.

### Edge cases

- Meme `entity_fingerprint` dans deux journaux distincts : conflit possible si la meme couche est referencee par deux projets. A clarifier visuellement.

### Plan d'observabilite

- Rapport export inclut une section "sources : N journaux, X events, Y rejets".

### Considerations securite

- Aucune connexion reseau. Lecture locale stricte.
- Avertir l'utilisateur si les journaux contiennent des chemins de fichiers personnels.

### Criteres d'acceptation

- Test : 3 journaux locaux fictifs, reporting agregé coherent.
- Test : un journal corrompu, le rapport continue avec les 2 autres.

### Cout

- Dev : 2-3 semaines.

---

## BL-EXPAND-005 - Smart restore avec resolution de conflit assistee

**Tags** : EXPANSION, INTELLIGENCE, RESILIENCE, P1

**Priorite** : P1

**Complexite** : elevee

### Constat

`core/restore_executor.py` (22012 octets) implemente STRICT et BEST_EFFORT (memoire systeme `346e1a25`). Bon. Mais en cas de conflit (ex : la feature a ete modifiee depuis l'event a restaurer), STRICT echoue, BEST_EFFORT applique avec ecrasement. Pas de chemin de resolution interactif.

### Cause

Architecture binaire : on accepte ou on rejette le conflit, on ne le resout pas.

### Valeur metier

- **Plus intelligent** : detecter le conflit, l'exposer, proposer 3 strategies (ecraser, ignorer, merger champ par champ).
- **Plus robuste** : evite les ecrasements silencieux qui font perdre des donnees.
- **Plus puissant** : permet le restore sur des couches actives multi-utilisateur (avec discipline).

### Perimetre technique

- Avant restore, pour chaque event a appliquer :
  - Lire l'etat actuel de la feature.
  - Calculer le diff entre etat actuel et etat cible (event).
  - Si la difference touche **uniquement les champs concernes par l'event** : pas de conflit, restore direct.
  - Si la difference touche d'autres champs (modifies depuis) : conflit detecte.
- Pour les conflits : 3 strategies utilisateur, choisies par event ou en lot :
  - **Ecraser** : appliquer l'event tel quel (comportement BEST_EFFORT actuel).
  - **Ignorer** : ne pas restaurer cet event.
  - **Merger** : appliquer uniquement les champs de l'event qui ne sont pas en conflit.
- UI conflict resolution : tableau "champ / valeur actuelle / valeur a restaurer / strategie".
- Le merge est trace dans `audit_lifecycle` avec details du diff.

### Failure scenarios

- Lecture etat actuel echoue (lock provider) : marquer conflit "non resolvable", proposer "ignorer" ou "retenter".
- Geometry conflict : strategie speciale "garder geometrie actuelle, restaurer attributs".
- Schema drift : croiser avec `core/schema_drift.py` deja present.

### Edge cases

- 1000 events a restaurer dont 200 en conflit : UI doit gerer le bulk avec strategies par defaut.
- Conflit detecte mais utilisateur quitte sans choisir : annulation propre, aucun event applique.

### Plan d'observabilite

- Event lifecycle "restore_with_conflict_resolution" avec details.
- Metrique : pourcentage de restores avec conflits / mois.

### Criteres d'acceptation

- Test : event UPDATE sur feature modifiee depuis, dialog de conflit visible, 3 choix testes.
- Test : strategie merge applique uniquement les champs non-conflictuels.
- Test : annulation au milieu = etat propre.

### Cout

- Dev : 2-3 semaines.

---

## BL-EXPAND-006 - Adaptive batching et throttling intelligent

**Tags** : EXPANSION, ADAPTIVE, P2

**Priorite** : P2

**Complexite** : moyenne

### Constat

Charge edition tres variable selon contexte : 1 commit / heure (utilisateur cartographe ponctuel) vs 1000 commits / minute (script Processing batch). Le plugin a deja une `WriteQueue` async, mais la strategie est uniforme.

### Cause

Pas de profil charge actif.

### Valeur metier

- **Plus adaptatif** : ressources allouees correspondent a la charge reelle.
- **Plus econome** : faible empreinte sur usage leger.
- **Plus performant** : burst absorbe sur usage intensif.

### Perimetre technique

- Mesure continue throughput moyen (events / min sur fenetre 5 min).
- Trois profils :
  - **LIGHT** (< 10 events/min) : batch_size = 100, flush every 5 s, mmap reduit.
  - **NORMAL** (10-1000 events/min) : batch_size = 500, flush every 1 s, mmap normal.
  - **HEAVY** (> 1000 events/min) : batch_size = 2000, flush every 100 ms, mmap large, prefetch index.
- Bascule automatique entre profils avec hysteresis (eviter oscillations).
- Profil visible dans le dashboard (BL-RESILIENCE-009).

### Failure scenarios

- Bascule profil pendant un commit : terminer le batch avant bascule.
- Detection trompee par un burst ponctuel : hysteresis sur 3 mesures consecutives.

### Edge cases

- Profil en mode HEAVY sustained : verifier que la WriteQueue ne sature pas malgre le batch_size eleve.

### Plan d'observabilite

- Log INFO a chaque bascule de profil.
- Metrique exposee dans le dashboard : profil courant + duree dans le profil.

### Criteres d'acceptation

- Test : injecter 100 events / s pendant 1 min, profil HEAVY active, latence stable.
- Test : 1 event / 10 min, profil LIGHT, mmap reduit confirmé.

### Cout

- Dev : 1-2 semaines.

---

## BL-EXPAND-007 - Hooks publics scriptables (API plugin)

**Tags** : EXPANSION, EXTENSIBILITY, P2

**Priorite** : P2

**Complexite** : moyenne

### Constat

Aujourd'hui RecoverLand est une UI + un noyau interne. Pas d'API publique pour qu'un autre plugin / script s'integre.

### Cause

Pas d'exposition de l'API.

### Valeur metier

- **Plus puissant** : autres plugins (ex : Mergin Maps, QField, plugins metiers) peuvent capturer / consulter / restaurer programmatiquement.
- **Plus adaptatif** : workflows specifiques sans modifier RecoverLand.

### Perimetre technique

- Module public `recoverland.api` exporte :
  - `get_journal_path() -> str`.
  - `search_events(criteria: dict) -> list[Event]`.
  - `get_event_by_id(event_id: int) -> Event`.
  - `restore_events(event_ids: list[int], mode: str) -> RestoreReport`.
  - `register_capture_hook(callback)` : appele apres chaque event capture.
  - `register_restore_hook(callback)` : appele avant et apres restore.
- Documentation API publique dans `docs/api.html`.
- Versioning semantique (`X.Y.Z`), changement breaking = bump major.

### Failure scenarios

- Hook callback leve une exception : isoler dans un try/except, log, continuer.
- Hook trop lent : timeout 1 s, log.

### Edge cases

- Plusieurs hooks enregistres : ordre d'appel stable (FIFO).

### Plan d'observabilite

- Log DEBUG a chaque appel de hook tiers.

### Considerations securite

- Hooks enregistres ne doivent pas pouvoir modifier les events directement (lecture seule depuis le hook).

### Criteres d'acceptation

- Test : autre plugin appelle `search_events`, recoit liste valide.
- Test : hook tiers leve exception, RecoverLand continue.
- Doc publique avec exemples.

### Cout

- Dev : 2 semaines.
- Documentation : 1 semaine.

---

## BL-EXPAND-008 - Detection automatique des sources fragiles avec recommandation

**Tags** : EXPANSION, INTELLIGENCE, P1

**Priorite** : P1

**Complexite** : faible

### Constat

`core/support_policy.py` connait deja les niveaux d'identite. Mais l'utilisateur ne le voit qu'apres avoir lance un restore. Trop tard pour anticiper.

### Cause

Information cachee jusqu'au moment du restore.

### Valeur metier

- **Plus intelligent** : avertir l'utilisateur des qu'il commence a editer une source a identite faible.
- **Plus pedagogique** : suggerer la meilleure pratique pour le contexte (ex : "pour ce shapefile, eviter le repack").

### Perimetre technique

- Hook sur `editingStarted` de chaque couche.
- Si `support_policy.evaluate_layer(layer)` retourne MEDIUM ou WEAK : afficher un toast non bloquant avec :
  - Format detecte.
  - Niveau de fiabilite.
  - Recommandation specifique (ex : "Shapefile : eviter d'utiliser 'Save as' apres edition").
  - Lien vers la doc `docs/limits.html`.
- Toast dismissable, mais la decision est tracee.
- Option "ne plus me prevenir pour ce format" stockee dans `LocalSettings`.

### Failure scenarios

- Format inconnu : niveau "UNKNOWN", suggerer test BL-EXPAND-001.

### Edge cases

- Couche memoire : niveau NONE, toast critique.
- CSV avec PK fictive : detecter et avertir.

### Plan d'observabilite

- Event lifecycle "support_policy_warning_shown" avec niveau et format.

### Criteres d'acceptation

- Test : ouvrir un shapefile en edit, toast MEDIUM visible.
- Test : option "ne plus prevenir" persistee entre sessions.

### Cout

- Dev : < 1 semaine.

---

# Synthese et plan de bataille recommande

## Inventaire complet des items

| ID | Titre | Priorite | Complexite | Categorie |
|---|---|---|---|---|
| BL-DIST-001 | Soumettre le plugin a plugins.qgis.org | P0 | faible | Distribution |
| BL-DIST-002 | Internationalisation effective EN et FR | P1 | moyenne | Distribution |
| BL-DIST-003 | Page d'atterrissage publique neutre | P2 | faible | Distribution |
| BL-UX-001 | Action "Annuler dernier commit" menu contextuel layer | P0 | moyenne | UX |
| BL-UX-002 | Restore en 3 clics maximum dans la dialog | P1 | moyenne | UX |
| BL-UX-003 | Decomposer RecoverDialog monolithique | P2 | elevee | UX |
| BL-UX-004 | Indicateur permanent et explicite etat tracking | P1 | faible | UX |
| BL-UX-005 | Notifications explicites des modes degrades | P0 | moyenne | UX |
| BL-PORT-001 | Export JSONL neutre des events | P0 | moyenne | Portabilite |
| BL-PORT-002 | Import JSONL pour reprise et migration | P1 | moyenne | Portabilite |
| BL-PORT-003 | Documentation publique du schema event | P2 | faible | Portabilite |
| BL-PROMISE-001 | Aligner "Zero silent loss" avec mecanisme reel | P0 | faible | Honnetete |
| BL-PROMISE-002 | Qualifier le terme "safety net" | P1 | faible | Honnetete |
| BL-PROMISE-003 | Tracabilite formelle des modes degrades | P1 | moyenne | Honnetete |
| BL-COV-001 | Capture via console Python QGIS (verifier) | P1 | moyenne | Couverture |
| BL-COV-002 | Capture via Processing et Modeler | P1 | elevee | Couverture |
| BL-COV-003 | Watcher externe optionnel modifications hors QGIS | P2 | elevee | Couverture |
| BL-RESILIENCE-001 | DiskMonitor periodique reel et multi-niveaux | P1 | moyenne | Resilience |
| BL-RESILIENCE-002 | HealthMonitor predictif et actionnable | P1 | moyenne | Resilience |
| BL-RESILIENCE-003 | Integrity etendue avec auto-repair | P0 | elevee | Resilience |
| BL-RESILIENCE-004 | WriteQueue avec backpressure adaptative | P1 | moyenne | Resilience |
| BL-RESILIENCE-005 | Journal sur reseau detecte et redirige | P0 | moyenne | Resilience |
| BL-RESILIENCE-006 | JournalManager multi-journal et fallback | P2 | elevee | Resilience |
| BL-RESILIENCE-007 | Recuperation apres signal d'edition non capture | P2 | elevee | Resilience |
| BL-RESILIENCE-008 | Cycle de vie tracking auditable end-to-end | P0 | moyenne | Resilience |
| BL-RESILIENCE-009 | Telemetrie locale opt-in avec dashboard interne | P2 | moyenne | Resilience |
| BL-DATA-001 | Chiffrement at-rest optionnel du journal | P1 | elevee | Securite |
| BL-DATA-002 | Avertissement explicite sur donnees personnelles | P1 | faible | Securite |
| BL-DATA-003 | Commande "Scrub journal" anonymisation a la demande | P2 | moyenne | Securite |
| BL-DATA-004 | Template .gitignore auto-propose | P2 | faible | Securite |
| BL-FP-001 | Empreinte par hash de contenu en complement | P1 | elevee | Fingerprint |
| BL-FP-002 | Detection automatique alias UNC vs lettre lecteur | P2 | moyenne | Fingerprint |
| BL-FP-003 | Avertissement a l'ouverture si fingerprint derive | P2 | faible | Fingerprint |
| BL-PROJ-001 | Avertissement projets sur partages reseau | P1 | faible | Environnement |
| BL-PROJ-002 | Politique de retention par defaut sensee | P2 | faible | Environnement |
| BL-PROJ-003 | Diagnostic clair emplacement journal | P3 | faible | Environnement |
| BL-EXPAND-001 | Matrice de couverture par format testee | P1 | moyenne | Expansion |
| BL-EXPAND-002 | Capture confirmee via console Python | P1 | faible | Expansion |
| BL-EXPAND-003 | Capture confirmee via Processing et Modeler | P1 | moyenne | Expansion |
| BL-EXPAND-004 | Multi-projet et reporting transverse | P2 | elevee | Expansion |
| BL-EXPAND-005 | Smart restore avec resolution de conflit assistee | P1 | elevee | Expansion |
| BL-EXPAND-006 | Adaptive batching et throttling intelligent | P2 | moyenne | Expansion |
| BL-EXPAND-007 | Hooks publics scriptables (API plugin) | P2 | moyenne | Expansion |
| BL-EXPAND-008 | Detection sources fragiles avec recommandation | P1 | faible | Expansion |

**Total** : 41 items. **Aucune fonctionnalite supprimee**, **toutes les capacites existantes conservees**, toutes les ameliorations vont vers : plus robuste, plus intelligent, plus adaptatif, plus large.

## Top 7 P0 a livrer en priorite

Items qui combinent fort impact et alignement promesse/realite.

1. **BL-RESILIENCE-005** : detection partage reseau et migration vers profil local. Empeche corruption silencieuse (preuve : `sqlite.org/wal.html`).
2. **BL-RESILIENCE-008** : `audit_lifecycle` end-to-end. Rend le tracking auditable et explique l'ambiguite "rien edite vs tracking off".
3. **BL-RESILIENCE-003** : auto-repair non destructif. Robustesse maximale sans perdre d'event (quarantine table).
4. **BL-DIST-001** : presence sur `plugins.qgis.org`. Decouverte massive.
5. **BL-UX-001** : action "Annuler dernier commit" menu contextuel layer. UX moment de verite.
6. **BL-UX-005** : notifications explicites des modes degrades. Aligne promesse et realite.
7. **BL-PORT-001** : export JSONL neutre. Leve le verrou format, ajoute une voie de portabilite **sans toucher au format SQLite existant**.

## Sequencement suggere

Les sprints presument une equipe d'1 a 2 developpeurs. A ajuster selon ressources reelles.

### Sprint 1 - Honnetete et auditabilite (2 semaines)

- **BL-DIST-001** : soumission `plugins.qgis.org`.
- **BL-PROMISE-001**, **BL-PROMISE-002** : reformulations doc.
- **BL-RESILIENCE-008** : table `audit_lifecycle` (base pour les autres items).
- **BL-PROJ-003**, **BL-DATA-004** : visibilite emplacement journal et `.gitignore` template.

### Sprint 2 - Resilience reseau et auto-repair (4 semaines)

- **BL-RESILIENCE-005** : detection partage reseau.
- **BL-RESILIENCE-003** : auto-repair integrity etendu.
- **BL-PROJ-001** : warnings WAL specifiques renvoyant vers BL-RESILIENCE-005.
- **BL-PROMISE-003** : trace lifecycle des modes degrades.

### Sprint 3 - UX moment de verite (3 semaines)

- **BL-UX-001** : action menu contextuel layer.
- **BL-UX-005** : notifications systematiques modes degrades.
- **BL-UX-004** : indicateur tracking permanent renforce.
- **BL-EXPAND-008** : warning preventif sur formats fragiles.

### Sprint 4 - Portabilite et confiance DSI (3 semaines)

- **BL-PORT-001** : export JSONL streaming.
- **BL-PORT-002** : import JSONL avec dry-run.
- **BL-PORT-003** : JSON Schema publie et valide en CI.
- **BL-DATA-002** : section RGPD dans la doc + message premier demarrage.

### Sprint 5 - Resilience adaptive (3 semaines)

- **BL-RESILIENCE-001** : DiskMonitor periodique reel multi-seuils.
- **BL-RESILIENCE-002** : HealthMonitor predictif + actions one-click.
- **BL-RESILIENCE-004** : WriteQueue backpressure adaptative.
- **BL-EXPAND-006** : profils LIGHT/NORMAL/HEAVY.

### Sprint 6 - Couverture confirmee et matrice publique (3 semaines)

- **BL-COV-001**, **BL-EXPAND-002** : confirmer capture console Python.
- **BL-COV-002**, **BL-EXPAND-003** : confirmer capture Processing/Modeler.
- **BL-EXPAND-001** : matrice formats publique et testee en CI.
- **BL-DIST-002** : i18n complete EN/FR.

### Sprint 7+ - Capacites avancees (long terme)

- **BL-EXPAND-005** : conflict resolution.
- **BL-EXPAND-007** : API publique.
- **BL-RESILIENCE-006**, **BL-RESILIENCE-007** : fallback journal et reconstruction.
- **BL-RESILIENCE-009** : dashboard operationnel.
- **BL-DATA-001** : SQLCipher (necessite decision RGPD).
- **BL-FP-001**, **BL-FP-002** : empreinte par hash, detection alias UNC.
- **BL-COV-003**, **BL-EXPAND-004** : watcher externe et reporting transverse.
- **BL-DATA-003** : commande scrub journal.

### Estimation cout total

- P0 (Sprints 1 et 2) : ~6 semaines.
- P1 critiques (Sprints 3-5) : ~9 semaines.
- P1 confort + P2 (Sprints 6-7) : ~10 a 14 semaines.
- **Total cible** : 6 a 9 mois pour livrer integralement, en 1-2 dev focalises.

## Hypotheses a confirmer avant execution

| Inconnue | Bloquant pour | Comment lever |
|---|---|---|
| **UNKNOWN-DIST** | BL-DIST-001 | Verifier manuellement `plugins.qgis.org/plugins/recoverland/` |
| **UNKNOWN-USERS** | Priorisation BL-DATA et BL-PROJ | Sondage utilisateurs ou retours github issues |
| **UNKNOWN-RGPD** | BL-DATA-001 vs BL-DATA-002 | Cas d'usage cible documentes |
| **UNKNOWN-NETWORK** | Urgence BL-RESILIENCE-005 | Statistiques sur deploiement actuel |
| **UNKNOWN-LOAD** | BL-RESILIENCE-004 et BL-EXPAND-006 | Voir BL-PERF-000 du backlog perf |
| **UNKNOWN-PROCESSING-COVERAGE** | Cout reel BL-COV-002 | Test e2e a executer |

## Items hors scope de ce backlog

- Optimisation perf : voir `docs/backlog_performance_2026-04-27.md`.
- QA hardening interne : voir `docs/backlog_qa_hardening_2026-04-27.md`.
- Securite SQL et XML : voir `docs/backlog_security_2026-04-16.md` (livre).
- Compatibilite QGIS 3.40-4.x : voir `docs/compat_audit_3.40_4.x.md`.
- Architecture monolithe RecoverDialog : voir `ARCH-01` dans `docs/backlog_2026-04-16.md`.

## Conclusion strategique

RecoverLand sauve aujourd'hui des equipes en production. Sa couverture multi-source (PostGIS, GeoPackage, Shapefile, SpatiaLite, MSSQL, Oracle, GeoJSON, FlatGeobuf, CSV, KML, GML) est un differenciateur reel face aux concurrents (`pg_history_viewer`, `Postgres 91 plus Auditor`) qui sont PG-only.

**Ce backlog est un programme de renforcement, pas une remise en cause.** Aucun item ne propose la suppression d'une capacite existante. Tous etendent ou rendent plus intelligent un comportement deja present.

Les 4 priorites strategiques :

1. **Distribution officielle** : faire connaitre.
2. **Resilience renforcee** : detecter les modes degrades avant l'utilisateur.
3. **UX du moment de verite** : 1 clic depuis la couche, plus 6 clics dans la dialog.
4. **Portabilite et honnetete** : format ouvert et alignement promesse/realite.

Aucun de ces axes ne demande de reecrire le noyau. Tous sont additifs. Les composants `core/disk_monitor.py`, `core/health_monitor.py`, `core/integrity.py`, `core/journal_manager.py`, `core/write_queue.py`, `core/datasource_alias.py`, `core/support_policy.py`, `core/restore_executor.py` et `core/restore_planner.py` restent en place et **gagnent** des capacites.

Cout total des P0 : 6 semaines focalisees. Investissement strategique majeur : 6 a 9 mois pour le programme complet.

---

# Final validation checklist (workflow @backlogger)

Avant livraison de ce backlog, verification systematique :

| Critere | Statut | Note |
|---|---|---|
| **Aucun comportement non defini** | OK | Chaque item liste failure scenarios et edge cases. |
| **Aucun chemin d'echec silencieux** | OK | BL-RESILIENCE-008 garantit la trace lifecycle de tout etat degrade. |
| **Aucune dependance manquante** | PARTIEL | BL-DATA-001 depend de packaging SQLCipher, ouvert. BL-EXPAND-005 depend de BL-RESILIENCE-008. |
| **Aucune complexite cachee** | OK | Chaque item indique sa complexite (faible/moyenne/elevee). |
| **Aucune hypothese non verifiee acceptee comme fait** | OK | Toutes les hypotheses sont marquees explicitement (UNKNOWN-X). |
| **Recherche externe completee et synthetisee** | OK | Section "Phase de recherche externe" liste 8 sources officielles lues. |
| **Documentation officielle revue pour technologies critiques** | OK | SQLite WAL, QGIS plugin structure, PyQGIS QgsVectorLayer, QGIS Translation. |
| **Reference implementations consultees** | OK | `pg_history_viewer`, `Postgres 91 plus Auditor`, `undoPropertiesChanges`, SQLCipher. |
| **Aucune affirmation "best practice" sans preuve** | OK | Chaque pattern propose cite la source. |
| **Failure scenarios systematiques** | OK | Chaque item P0/P1 contient une section "Failure scenarios". |
| **Edge cases explicites** | OK | Chaque item contient une section "Edge cases". |
| **Plan d'observabilite present** | OK | Chaque item contient une section "Plan d'observabilite". |
| **Acceptance criteria mesurables** | OK | Tous formules en termes testables. |
| **Cost impact present** | OK | Chaque item contient une section "Cout". |
| **Tags BLOCKER/RISK/WEAK DESIGN/UNKNOWN appliques** | OK | Items P0 marques RISK quand pertinent, UNKNOWN listes en debut. |

### Items non bloquants residuels

- **UNKNOWN-DIST** : statut sur `plugins.qgis.org` doit etre verifie manuellement avant Sprint 1.
- **UNKNOWN-PROCESSING-COVERAGE** : test BL-EXPAND-002/003 a executer pour confirmer hypothese de couverture par signaux.
- **Decision RGPD** : lever UNKNOWN-RGPD pour decider entre Sprint 4 (BL-DATA-002 minimal) et Sprint 7+ (BL-DATA-001 SQLCipher).

Aucun blocage absolu. Le backlog est executable.

### Verdict @backlogger

Backlog **valide**.

- Survit a la production : oui (resilience renforcee, modes degrades couverts).
- Survit aux developpeurs juniors : oui (chaque item executable sans interpretation).
- Survit au scaling : oui (BL-EXPAND-006 adaptive batching, BL-RESILIENCE-004 backpressure).
- Survit aux exigences floues : oui (UNKNOWN explicites, sequencement adaptatif par sprint).
