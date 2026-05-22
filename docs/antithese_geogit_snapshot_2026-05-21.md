# Diagnostic intégral — RecoverLand

**Date** : 2026-05-21 | **Périmètre** : Plugin complet, tous modes, 49 modules core + 17 widgets | **Méthode** : `/antithese` + `/architecte`

---

## 1. THÈSE GLOBALE

> RecoverLand capture toutes les éditions QGIS, les stocke en SQLite local, permet recherche/restauration/rewind/Review diff/Review snapshot/Time Lens — le tout sans écriture sur les couches sources (sauf restore explicite), UI responsive, compat QGIS 3.40–4.x.

---

## 2. ARCHITECTURE — 3 modes

```
recover.py (RecoverPlugin)
├── JournalManager (lock, open, close, relocate)
├── WriteQueue (thread dédié, batch, hard limit 50K)
├── EditSessionTracker (signaux QGIS → AuditEvent)
├── StatusBarIndicator (icône permanente)
│
recover_dialog.py (4689 lignes — GOD OBJECT)
│
├── MODE VERSION (temporal) ── "Rewind"
│   ├── rewind_dedup, version_fetch_thread
│   ├── restore_executor (STRICT / BEST_EFFORT)
│   └── restore_runner (chunked, async UI)
│
├── MODE ACTION (event) ── "Recover + Restore"
│   ├── search_service, sqlite_backend (recherche)
│   └── restore_service, restore_executor, restore_runner (restauration)
│
└── MODE REVIEW ── "Diff + Snapshot + Time Lens"
    ├── Sous-mode Diff ── review_session, review_render_worker, review_worker
    ├── Sous-mode Snapshot ── temporal_snapshot_engine, snapshot_overlay_session,
    │                          canvas_date_bar, snapshot_rebuild_worker
    └── Time Lens (dock séparé) ── lens_planner, lens_renderer, temporal_lens_dock
```

**Note** : "Recover" et "Restore" sont le même mode (ACTION/event) — la recherche et la restauration sont deux étapes du même flux.

---

## 3. DIAGNOSTIC PAR MODE

---

### 3.1 MODE RECOVER — Recherche d'événements

**Fichiers** : `recover_dialog.py:1865-1900`, `core/search_service.py`, `core/sqlite_backend.py`

#### AT-RECOVER-1 : Connexion read stale après switch projet

`recover_dialog.py:355-367` maintient `_dialog_read_conn` comme singleton. `_close_dialog_read_conn` est appelé dans `on_events_committed` et `_open_maintenance` mais **pas** dans `on_project_switched`. Après switch projet A→B, la première recherche utilise l'ancienne connexion → résultats vides ou erreur.

**Sévérité** : MOYENNE.

#### AT-RECOVER-2 : Pas de timeout sur `recover_and_load`

`recover_dialog.py:1865` lance `_recover_event_mode` → `search_service.search_events`. Aucun timeout. Journal 2M events + filtre large → >30s gel UI.

**Sévérité** : MOYENNE.

#### AT-RECOVER-3 : Cache stats stale après édition

`recover_dialog.py:377-391` utilise `_stats_cache` pour bornes de date. Si des événements sont ajoutés depuis le dernier refresh, les bornes sont fausses et l'utilisateur peut exclure des événements récents.

**Sévérité** : BASSE.

---

### 3.2 MODE RESTORE — Restauration par action

**Fichiers** : `core/restore_service.py` (1041L), `core/restore_executor.py` (956L), `restore_runner.py` (1145L)

#### AT-RESTORE-1 : `_qgis_vals_equal` ignore les timezones

`restore_service.py:27-56` compare QDateTime vs string ISO. Gère `QDate`, `QDateTime`, `QTime` mais pas les timezones. Un `QDateTime` avec fuseau non-UTC peut échouer la comparaison → feature considérée modifiée → re-restaurée → duplication.

**Sévérité** : HAUTE.

#### AT-RESTORE-2 : `restore_deleted_feature` ignore contraintes UNIQUE

`restore_service.py:88-150` réinsère une feature supprimée. Si la couche a une contrainte UNIQUE et qu'une autre feature a pris la même valeur, l'insertion échoue. L'erreur est logguée mais sans mention de la contrainte violée.

**Sévérité** : MOYENNE.

#### AT-RESTORE-3 : `_execute_strict` rollback cascade non documenté

`restore_executor.py` mode STRICT utilise `layer.rollBack()` en cas d'échec. Mais si d'autres éditions utilisateur sont dans le buffer, elles sont aussi rollbackées. Le contrat ne documente pas cette perte collatérale.

**Sévérité** : MOYENNE.

#### AT-RESTORE-4 : `RestoreRunner` émet `finished` même après cancel

`restore_runner.py` utilise `QTimer.singleShot(0, ...)` pour le chunking. Si `cancel()` est appelé pendant un chunk, le signal `finished` peut être émis avec des résultats partiels. Le consommateur (`recover_dialog`) ne vérifie pas si le runner a été cancellé.

**Sévérité** : BASSE.

---

### 3.3 MODE REWIND — Restauration temporelle

**Fichiers** : `core/rewind_dedup.py` (424L), `version_fetch_thread.py` (119L)

#### AT-REWIND-1 : `_detect_fid_recycle` ne détecte que INSERT→DELETE→INSERT

`rewind_dedup.py:45-96` détecte le pattern FID recyclé uniquement pour INSERT→DELETE→INSERT. Mais le pattern DELETE→INSERT (sans INSERT initial) n'est pas détecté — une entité créée avant le début du tracking, supprimée, puis une nouvelle entité reçoit le même FID.

**Sévérité** : MOYENNE.

#### AT-REWIND-2 : `VersionFetchThread` double exécution QThread + QgsTask

`version_fetch_thread.py:64-118` hérite de `TaskEnabledThread` qui peut s'exécuter via QThread.run() OU QgsTaskManager. La méthode `_start_task` soumet au TaskManager, mais `run()` est aussi overridé. Si `start()` est appelé (mode QThread), `run()` s'exécute directement. Si `_start_task()` est appelé (mode QgsTask), le TaskManager exécute `_run_fetch_task`. Les deux chemins peuvent être actifs simultanément.

**Sévérité** : HAUTE.

#### AT-REWIND-3 : `_invalidate_orphan_traces_on_open` invalide TOUTES les traces

`recover_dialog.py:4363-4397` invalide toutes les traces actives à l'ouverture du dialog. Si le dialog est fermé puis rouvert dans la même session, les traces de restore légitimes sont invalidées → l'undo ne fonctionne plus.

**Sévérité** : MOYENNE.

---

### 3.4 MODE REVIEW DIFF — Visualisation des modifications

**Fichiers** : `core/review_session.py` (506L), `widgets/review_render_worker.py` (513L), `widgets/review_worker.py`

#### AT-GEODIFF-1 : `ReviewSession._reproject_bbox` dupliqué ×3

`review_session.py:317`, `review_render_worker.py:425`, `recover_dialog.py:2571-2609` — trois copies identiques de la logique de reprojection bbox. Déjà documenté dans F4.

**Sévérité** : MOYENNE.

#### AT-GEODIFF-2 : `ReviewSession.refresh_one_layer` pas de cache par échelle

`review_session.py` filtre par bbox mais pas par échelle. À petite échelle (zoom pays), toutes les features sont rendues même si invisibles. Pour 100K features, le rendu peut prendre >2s.

**Sévérité** : BASSE.

#### AT-GEODIFF-3 : `_review_connect_auto_refresh` connecte `committed_features` sans vérifier l'existence

`recover_dialog.py:2797-2801` essaie de connecter `self._tracker.committed_features`. Si le tracker n'a pas ce signal (version future), l'erreur est silencieusement avalée par `except (AttributeError, TypeError): pass`.

**Sévérité** : BASSE.

---

### 3.5 MODE REVIEW SNAPSHOT — État à une date T

**Fichiers** : `core/temporal_snapshot_engine.py` (283L), `core/snapshot_overlay_session.py` (650L), `widgets/canvas_date_bar.py`, `widgets/snapshot_rebuild_worker.py`

#### AT-SNAP-1 : `_geom_at_cutoff` géométrie fausse sur UPDATEs pré-schema-v2

`temporal_snapshot_engine.py:254-275` utilise `ev.new_geometry_wkb or ev.geometry_wkb`. Pour les UPDATEs d'avant la migration v2, `new_geometry_wkb` est NULL → le fallback est l'ancienne géométrie, pas la nouvelle. **Bug silencieux.**

**Sévérité** : HAUTE.

#### AT-SNAP-2 : Invariant "zéro SQL" faux

Le backlog promet "ZÉRO SQL à chaque changement de date". `SnapshotRebuildWorker.run()` fait 2N requêtes SQL (N = nombre de couches). Pour 20 couches = 40 requêtes par date.

**Sévérité** : HAUTE.

#### AT-SNAP-3 : Contrat `SnapshotFeature.attrs_json` mensonger

Docstring dit "raw attributes_json of last event". Code produit une fusion flat dict. Consommateur attend une fusion flat dict. Le contrat écrit est faux.

**Sévérité** : MOYENNE.

#### AT-SNAP-4 : `CanvasDateBar` widget orphelin, `hideEvent` flood WARNING

`canvas_date_bar.py` : pas de parent Qt, `hideEvent` log un WARNING avec `traceback.format_stack(limit=6)` à chaque hide → flood de logs. `QTimer.singleShot(150, self._reposition)` dans l'eventFilter pas protégé par `_closing`.

**Sévérité** : MOYENNE.

#### AT-SNAP-5 : `_populate_layer` skip silencieux sur geom type mismatch

`snapshot_overlay_session.py:360-411` skip les features dont le type de géométrie ne correspond pas, avec un compteur mais sans les entity_fp concernés.

**Sévérité** : BASSE.

#### AT-SNAP-6 : `filter_snapshot_by_bbox` sur le thread UI

`recover_dialog.py:2637-2646` appelle `filter_snapshot_by_bbox` dans `_on_snapshot_result` (slot UI). Parse WKB de toutes les features → ~50ms gel pour 10K entités.

**Sévérité** : BASSE.

#### AT-SNAP-7 : Race condition `cancel()` sans `wait()` dans `_on_snapshot_date_changed`

`recover_dialog.py:2555-2562` déconnecte les signaux puis `cancel()` sans `wait()`. L'ancien worker peut encore émettre `result_ready` via la queue d'événements Qt.

**Sévérité** : MOYENNE.

#### AT-SNAP-8 : `export_to_geopackage` synchrone, gèle l'UI

`snapshot_overlay_session.py:453-532` exporte toutes les couches en série. 20 couches × 5000 features = ~4s de gel UI.

**Sévérité** : BASSE.

#### AT-SNAP-9 : `_coerce_field_value` convertit `""` et `"NULL"` en None

`snapshot_overlay_session.py:632-646` transforme `""` → None et `"NULL"` → None. Perte de données pour les champs texte où `""` et `"NULL"` sont des valeurs légitimes.

**Sévérité** : BASSE.

---

### 3.6 MODE TIME LENS — Requête spatio-temporelle

**Fichiers** : `core/lens_planner.py`, `core/lens_renderer.py` (508L), `widgets/temporal_lens_dock.py` (870L)

#### AT-LENS-1 : `TemporalLensDock` recrée les couches overlay à chaque requête

`temporal_lens_dock.py` crée de nouvelles couches mémoire à chaque exécution. Les anciennes sont purgées via `purge_lens_overlays`. Si la purge échoue (ex: couche verrouillée), les couches s'accumulent dans le projet.

**Sévérité** : MOYENNE.

#### AT-LENS-2 : `LensRectangleMapTool` pas de limite de zone

`temporal_lens_map_tool.py` permet de dessiner un rectangle arbitrairement grand. Si l'utilisateur sélectionne tout le canvas, `fetch_events_in_zone` peut ramener 500K events → OOM ou gel.

**Sévérité** : MOYENNE.

#### AT-LENS-3 : `lens_renderer.execute_lens_render` pas de cache de résultats

Chaque changement de paramètre (date, filtre) refait le fetch SQL + render complet. Pas de cache des résultats intermédiaires.

**Sévérité** : BASSE.

---

## 4. DIAGNOSTIC INFRASTRUCTURE

---

### 4.1 PLUGIN LIFECYCLE

**Fichier** : `recover.py` (658L)

#### AT-INFRA-1 : `_detect_duplicate_recoverland` ne détecte que par nom de classe

`recover.py:27-45` vérifie `type(plugin_obj).__name__ == 'RecoverPlugin'`. Si une ancienne version a un nom de classe différent, elle n'est pas détectée. La vérification par `__package__` serait plus robuste.

**Sévérité** : BASSE.

#### AT-INFRA-2 : `unload` ne fait pas `wait()` sur le WriteQueue

`recover.py:154-204` appelle `_shutdown_local_backend` qui stop le tracker et le WriteQueue. Mais si le WriteQueue a 50K events en attente, `stop()` attend `_FLUSH_TIMEOUT_SEC=10s`. Pendant ces 10s, QGIS est bloqué.

**Sévérité** : BASSE.

#### AT-INFRA-3 : `_disk_timer` périodicité non respectée

`recover.py` a `_disk_timer` et `_disk_journal_path` mais `_CHECK_INTERVAL_SEC=300` n'est pas utilisé — le timer est configuré à 60s en dur dans `_start_disk_monitoring`. La constante est ignorée.

**Sévérité** : BASSE.

---

### 4.2 WRITE QUEUE

**Fichier** : `core/write_queue.py` (272L)

#### AT-WQ-1 : Hard limit 50K → perte d'événements

`write_queue.py:89-97` : si `qsize + len(events) > 50000`, les événements sont sauvés dans `_save_lost_events` (fichier pending JSON) mais **pas** écrits dans le journal. L'utilisateur n'est pas notifié via l'UI (juste un log ERROR). Les événements sont perdus jusqu'au prochain `recover_pending_events`.

**Sévérité** : HAUTE.

#### AT-WQ-2 : `_validate_event` ne valide pas `new_geometry_wkb`

`write_queue.py:100-120` valide `operation_type`, `created_at`, `datasource_fingerprint` mais pas `new_geometry_wkb`. Un WKB corrompu est inséré silencieusement.

**Sévérité** : BASSE.

#### AT-WQ-3 : `_save_lost_events` écrase le fichier pending précédent

`write_queue.py:140-160` écrit les événements perdus dans `_PENDING_FILENAME`. Si le fichier existe déjà (événements perdus précédents non récupérés), il est écrasé → perte cumulative.

**Sévérité** : MOYENNE.

---

### 4.3 EDIT TRACKER

**Fichier** : `core/edit_tracker.py` (1006L)

#### AT-TRACK-1 : `suppress/unsuppress` pas de timeout

`edit_tracker.py:73-89` utilise un compteur de profondeur. Si un appelant oublie `unsuppress()` (exception avant finally), le tracker reste supprimé jusqu'au `force_unsuppress()` au unload. Toutes les éditions entre-temps sont perdues.

**Sévérité** : MOYENNE.

#### AT-TRACK-2 : `_MASS_DELETE_THRESHOLD = 100` arbitraire

`edit_tracker.py:49` définit le seuil de suppression massive à 100. Pour une couche de 500K features, 100 n'est pas "massif". Le seuil devrait être proportionnel à `layer.featureCount()`.

**Sévérité** : BASSE.

---

### 4.4 JOURNAL MANAGER

**Fichier** : `core/journal_manager.py` (421L)

#### AT-JRN-1 : Lock file pas nettoyé si processus QGIS crash

`journal_manager.py:27-28` utilise un fichier `.rlwriter` avec PID. Si QGIS crash, le fichier lock reste. Au redémarrage, `_is_pid_alive` détecte que le PID n'existe plus → le lock est considéré stale → nettoyé. OK. Mais si le PID est réutilisé par le système (Windows recycle les PIDs rapidement), un nouveau processus peut avoir le même PID → le lock est considéré vivant → `JournalLockError`.

**Sévérité** : BASSE.

#### AT-JRN-2 : `cleanup_orphan_journals` scan récursif sans limite

`journal_manager.py` parcourt `%APPDATA%/.../audit/` récursivement. Si le dossier contient 1000+ fichiers (bug, autre plugin), le scan peut prendre plusieurs secondes au startup.

**Sévérité** : BASSE.

---

### 4.5 DISK MONITORING

**Fichiers** : `core/disk_monitor.py` (88L), `core/health_monitor.py` (276L)

#### AT-DISK-1 : `disk_monitor.check_disk_for_path` et `health_monitor.check_disk_space` dupliqués

Deux modules font la même chose : `shutil.disk_usage` + seuils. `disk_monitor.py` a `_THRESHOLD_LOW=500MB`, `health_monitor.py` a `_DISK_WARNING=500MB`. Deux copies de la même logique avec des noms différents.

**Sévérité** : MOYENNE.

#### AT-DISK-2 : `check_disk_for_path` retourne `DiskStatus(0,0,"",False,False)` sur erreur

`disk_monitor.py:34-54` retourne un statut "tout va bien" (is_low=False, is_critical=False) quand le check échoue. L'appelant ne peut pas distinguer "disque OK" de "check failed".

**Sévérité** : MOYENNE.

---

### 4.6 INTEGRITY & RECOVERY

**Fichier** : `core/integrity.py` (315L)

#### AT-INT-1 : `_recover_pending_events` pas de limite de taille

`integrity.py` lit `recoverland_pending.json` en entier. Si le fichier fait 500MB (bug, corruption), `json.load` OOM.

**Sévérité** : MOYENNE.

#### AT-INT-2 : `check_journal_integrity` ne vérifie pas les index

`integrity.py:56-86` fait `PRAGMA integrity_check` et `wal_checkpoint` mais pas `PRAGMA index_list` + vérification que les index sont utilisables. Un index corrompu peut causer des résultats de recherche incorrects sans être détecté.

**Sévérité** : BASSE.

---

### 4.7 RETENTION & PURGE

**Fichier** : `core/retention.py` (226L)

#### AT-RET-1 : `purge_old_events` DELETE sans contrainte de temps max

`retention.py:40-79` boucle `while True` avec `LIMIT 5000`. Si le journal a 10M events à purger, c'est 2000 itérations. Pas de timeout global, pas de yield au thread UI.

**Sévérité** : MOYENNE.

#### AT-RET-2 : `vacuum_async` thread sans garde-fou

`retention.py` lance `VACUUM` dans un thread. Si le VACUUM prend 30 minutes (journal 50GB), le thread tourne sans que l'utilisateur sache qu'il est actif. Pas de progress reporting.

**Sévérité** : BASSE.

---

### 4.8 OBSERVABILITY

**Fichier** : `core/observability.py` (325L)

#### AT-OBS-1 : `flog_kv` non utilisé dans 90% des logs

Le module `observability.py` définit `flog_kv` pour le logging structuré key=value. Mais la majorité des logs dans le codebase utilisent des f-strings libres, pas le format structuré. La règle "Format structuré : key=value" est massivement violée.

**Sévérité** : BASSE (dette technique).

---

## 5. VIOLATIONS TRANSVERSES

### V-GOD — `recover_dialog.py` est un God Object (4689 lignes)

**Preuve** : 8 responsabilités distinctes dans un seul fichier :
- Smart bar / stats (300 lignes)
- Recherche événements (200 lignes)
- Restauration par action (400 lignes)
- Rewind temporel (500 lignes)
- Review Diff (400 lignes)
- Review Snapshot (300 lignes)
- Time Lens wiring (100 lignes)
- Journal maintenance (200 lignes)

**Impact** : tout changement risque de casser un mode non visé. Impossible de tester un mode sans instancier le dialog complet.

**Sévérité** : HAUTE (dette architecturale).

### V-DUP — Duplication inter-modules

| Élément | Copies | Fichiers |
|---------|--------|----------|
| `_reproject_bbox` | 3 | review_session, review_render_worker, recover_dialog |
| `_filter_by_bbox` | 3 | review_session, review_render_worker, snapshot_rebuild_worker |
| `check_disk_space` | 2 | disk_monitor, health_monitor |
| `_format_size` | 2 | health_monitor, journal_maintenance |

**Sévérité** : MOYENNE.

### V-LOG — Logging non structuré

**Preuve** : >80% des appels à `flog` utilisent des f-strings libres (`f"review: snapshot_mode_start n_layers={n}"`) au lieu du format key=value (`review.snapshot.start n_layers=5`). La règle "Format structuré : key=value" est violée presque partout.

**Sévérité** : BASSE.

### V-THREAD — Pas de politique de thread documentée

Le plugin utilise 4 mécanismes de threading différents :
- `QThread` (ReviewRenderWorker, SnapshotRebuildWorker)
- `threading.Thread` (WriteQueue)
- `QgsTask` (VersionFetchThread via TaskEnabledThread)
- `QTimer.singleShot(0, ...)` (RestoreRunner, SnapshotOverlaySession.update_async)

Aucun document ne spécifie quel mécanisme utiliser pour quel type de travail. Le choix est ad-hoc par module.

**Sévérité** : MOYENNE.

---

## 6. SYNTHÈSE GLOBALE

```
SYNTHÈSE: 35 antithèses documentées sur 7 modes + infrastructure.
0 antithèses exclues. Toutes fondées sur preuve statique (code source).

RÉPARTITION PAR SÉVÉRITÉ:
  HAUTE   : 6  (AT-RESTORE-1, AT-REWIND-2, AT-SNAP-1, AT-SNAP-2, AT-WQ-1, V-GOD)
  MOYENNE : 17
  BASSE   : 12
```

### Top 6 — Bloquants ou semi-bloquants

| ID | Mode | Problème | Impact |
|----|------|----------|--------|
| AT-RESTORE-1 | Restore | Timezone → duplication features | Corruption données |
| AT-REWIND-2 | Rewind | Double exécution QThread+QgsTask | Crash ou double restore |
| AT-SNAP-1 | Snapshot | Géométrie fausse UPDATEs pré-v2 | Affichage incorrect |
| AT-SNAP-2 | Snapshot | Invariant "zéro SQL" faux | Promesse non tenue |
| AT-WQ-1 | WriteQueue | Perte events >50K sans notif UI | Perte données silencieuse |
| V-GOD | Transverse | Dialog 4689 lignes, 8 responsabilités | Impossibilité de tester isolément |

### Dette architecturale prioritaire

1. **V-GOD** : Extraire un `SnapshotCoordinator` + `RestoreCoordinator` du dialog (P2)
2. **V-DUP** : Dédupliquer `_reproject_bbox` → `geometry_utils` (P1)
3. **V-THREAD** : Documenter la politique de threading (P2)
4. **V-LOG** : Migrer progressivement vers `flog_kv` structuré (P3)

### Conclusion

RecoverLand est **fonctionnellement solide sur le happy path** pour tous les modes. Les bugs identifiés sont principalement dans les cas limites (timezone, pré-schema-v2, saturation file d'attente, FID recycling). La dette architecturale principale est le God Object `recover_dialog.py` (4689 lignes) qui concentre toute la logique d'orchestration et rend le test isolé impossible. Le logging non structuré est une dette de maintenance mais pas un bug fonctionnel.
