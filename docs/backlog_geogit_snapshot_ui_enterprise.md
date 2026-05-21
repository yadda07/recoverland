# Spécification Technique & Backlog GeoGit Temporal Timeline Enterprise

Ce document définit les fondations architecturales, l'analyse approfondie du flux de données actuel, et le plan d'action d'ingénierie logicielle pour faire évoluer le composant de navigation temporelle (CanvasDateBar) vers un standard d'ingénierie de classe entreprise.

---

## 1. Analyse Profonde du Flux de Données Actuel (As-Is)

Pour concevoir des optimisations robustes, il est indispensable de cartographier précisément le cycle de vie d'un événement d'interaction (date ou zoom) dans le système existant :

```
[Utilisateur : Déplace le curseur T ou effectue un Zoom/Pan]
                       │
                       ▼
    1. CAPTURE & DEBOUNCE (800ms) - recover_dialog.py
       - QTimer filtre les signaux de micro-mouvements.
                       │
                       ▼
    2. REPROJECTION DE L'EMPRISE - recover_dialog.py
       - Transforme le QgsRectangle du canvas QGIS vers le CRS de stockage de chaque couche
         via QgsCoordinateTransform (ex. Lambert 93, UTM). Produit 'bbox_per_layer'.
                       │
                       ▼
    3. DISPATCH THREAD ASYNC - widgets/snapshot_rebuild_worker.py (QThread)
       - Ouvre une connexion SQLite locale en lecture seule.
       - Lance deux requêtes distinctes :
         A. _SQL_ALL_EVENTS_BEFORE : Récupère la totalité de l'historique jusqu'à T.
         B. _SQL_ALL_EVENT_DATES : Récupère toutes les dates de commits uniques (pour bookmarks).
                       │
                       ▼
    4. RECONSTRUCTION TEMPORELLE (CPU Bound) - core/temporal_snapshot_engine.py
       - Parcourt l'historique ASC par entité.
       - Reconstitue l'état à T (Forward Replay).
       - Résout la géométrie via _geom_at_cutoff (remonte dans le passé si UPDATE uniquement attributaire).
                       │
                       ▼
    5. FILTRAGE SPATIAL POST-RECONSTRUCTION - widgets/snapshot_rebuild_worker.py
       - Filtre en Python pur (wkb_envelope.py) pour éviter de bloquer le MainThread QGIS.
       - Élimine les géométries hors bbox. Conserve un comportement "fail-open" en cas d'erreur.
                       │
                       ▼
    6. RENDU ASYNC SUR LE CANVAS - core/snapshot_overlay_session.py
       - Retourne le SnapshotResult au MainThread.
       - Met à jour les couches mémoires (__rl_snap_{uid}_geom) via QTimer.singleShot(0)
         (1 couche par tick d'event-loop) pour garder QGIS réactif.
```

---

## 2. Diagnostics des Friction Points & Goulots d'Étranglement

Une analyse d'impact à grande échelle (volumétrie > 100 000 événements) révèle trois vulnérabilités architecturales majeures :

### A. La Vulnérabilité d'Échelle de la Requête Historique (Memory & CPU Bloat)
* **Problème** : Pour reconstruire l'état à l'instant `T`, le worker exécute `_SQL_ALL_EVENTS_BEFORE` qui récupère **tous les événements** de l'origine jusqu'à `T`. Si le projet a 3 ans d'historique et des centaines de milliers d'éditions, cette requête charge en RAM des mégaoctets de données et force Python à instancier des milliers d'objets `AuditEvent` à chaque mouvement de curseur.
* **Complexité Actuelle** : O(N) où N est le nombre total d'événements historiques de la couche.
* **Solution Cible** : Implémenter un cache de snapshot incrémental indexé.

### B. Le Paradoxe des Marqueurs Globaux (Le "Bookmark Cluttering")
* **Problème** : Les marqueurs (les triangles jaunes) sont actuellement calculés via `_SQL_ALL_EVENT_DATES` qui récupère **toutes les dates d'événements de la base de données entière**, indépendamment de ce que l'utilisateur regarde. 
* **Conséquence** : Si l'utilisateur zoome sur une parcelle précise qui n'a été modifiée que 2 fois, la timeline affiche tout de même les 50 marqueurs correspondant à des modifications ayant eu lieu à l'autre bout du territoire. Cela donne une fausse impression de stagnation historique (marqueurs "figés" visuellement) et surcharge l'affichage.
* **Solution Cible** : Marqueurs spatio-temporels indexés (filtrés par la Bounding Box courante).

### C. Le Verrouillage de l'Interface Graphique (MainThread Lock)
* **Problème** : Bien que la reconstruction soit asynchrone (QThread), la phase d'insertion des données géométriques dans les couches mémoires de QGIS (`_populate_layer` via `addFeatures()`) s'exécute obligatoirement dans le thread principal de QGIS (MainThread). Pour des datasets denses (> 10 000 entités visibles), cet appel fige l'écran pendant plusieurs vitesses de rafraîchissement.
* **Solution Cible** : Chunking et pagination dynamique des géométries d'overlay.

---

## 3. Spécifications de l'Architecture Cible

Pour résoudre ces contraintes, l'architecture doit s'articuler autour d'un pipeline découplé et optimisé.

### A. Structure du Modèle de Données Temporelles (`TemporalModel`)

```python
class TimelineEventMarker:
    event_id: int
    created_at: datetime
    operation_type: str  # INSERT, UPDATE, DELETE
    user_name: str
    affected_entities_count: int
    summary_text: str
```

### B. Moteur d'Échelle Non-Linéaire (`TimelineScaleEngine`)
Pour rendre la navigation fluide, l'échelle physique du widget doit s'adapter dynamiquement :

* **Mode Linéaire** : X = (T - T_min) / (T_max - T_min) (par défaut).
* **Mode Événementiel (Non-Linéaire)** : L'espace entre deux marqueurs sur l'axe X est constant, peu importe la durée réelle qui les sépare. Permet de naviguer facilement de modification en modification même si elles ont eu lieu à des intervalles de temps très irréguliers.

---

## 4. Backlog Technique Détaillé (Work Items)

### Phase 1 : Rénovation de la Chronologie Visuelle (Priorité 1)

#### TSUI-01 : Développement du `TemporalTimelineWidget` (Custom Painted QWidget)
* **Description** : Remplacer l'usage détourné du composant `QSlider` (qui n'offre pas la finesse de contrôle requise pour les applications spatiales de classe entreprise) par un composant écrit sur-mesure dérivant de `QWidget`.
* **Spécifications Techniques** :
  * Redéfinir entièrement `paintEvent(self, event)` avec un `QPainter` anti-aliasé.
  * Dessiner une règle chronologique avec graduations auto-adaptatives en fonction de la plage de temps (si plage < 1 jour : afficher les heures ; si > 1 an : afficher les mois/années).
  * Gérer le tracé des marqueurs (triangles de couleur verte pour `INSERT`, orange pour `UPDATE`, rouge pour `DELETE`) avec calcul précis des collisions d'affichage pour éviter l'empilement de jalons temporels trop proches.

#### TSUI-02 : Interaction Tactile, Tooltips Riches et Magnétisme
* **Description** : Fournir une expérience utilisateur fluide et informative directement au survol des jalons de la timeline.
* **Spécifications Techniques** :
  * **Hover Détecteur** : Capturer les événements `MouseMove` avec une précision géométrique (rayon de détection de 8px autour de chaque marqueur).
  * **Rich Tooltip Widget** : Instancier une fenêtre d'information élégante (avec ombres portées et coins arrondis) affichant les détails du commit (Heure, Auteur, Description, Entités modifiées).
  * **Aimentation Chronologique (Magnetic Snap)** : Lors du glisser-déposer du curseur, si la souris s'approche d'un jalon historique à moins de 10px, le curseur doit s'aimanter sur la date exacte de l'événement et déclencher instantanément la reconstruction à cette date.

#### TSUI-03 : Implémentation du Contrôle de Lecture Dynamique (Playback Controls)
* **Description** : Permettre à l'utilisateur de voir l'évolution historique du territoire comme un film, en lecture continue directement sur le canvas QGIS.
* **Spécifications Techniques** :
  * Ajouter les commandes : Play, Pause, Reculer d'une étape, Avancer d'une étape.
  * Mettre en place un régulateur de vitesse (FPS / commits par seconde).
  * Intégrer un mécanisme de préchargement (prefetching) du pas de temps suivant dans le worker d'arrière-plan pour garantir des transitions fluides sans saccades visuelles lors du défilement.

### Phase 2 : Optimisations Spatialisées & Performances (Priorité 2)

#### TSUI-04 : Requêtes SQL Spatialisées pour Marqueurs Dynamiques (BBOX-constrained Bookmarks)
* **Description** : Adapter l'affichage des bookmarks sur la timeline pour qu'ils ne reflètent **que** les événements ayant eu lieu dans la zone géographique actuellement affichée par l'utilisateur.
* **Spécifications Techniques** :
  * Modifier le worker pour exécuter une requête de géométrie spatiale optimisée :
    ```sql
    -- Exemple de requête de sélection des marqueurs par emprise géographique à T
    SELECT DISTINCT created_at 
    FROM audit_event 
    WHERE datasource_fingerprint = ?
      -- Filtre spatial approximatif via l'enveloppe
      AND (xmin <= :bbox_xmax AND xmax >= :bbox_xmin AND ymin <= :bbox_ymax AND ymax >= :bbox_ymin)
      AND invalidated_at IS NULL;
    ```
  * Mettre à jour l'affichage des marqueurs jaunes à chaque événement de pan/zoom de l'utilisateur de manière fluide.

#### TSUI-05 : Cache de Snapshot Temporel Incrémental
* **Description** : Supprimer la nécessité de re-calculer l'intégralité du forward-replay depuis l'origine de la base de données à chaque déplacement de curseur de quelques minutes.
* **Spécifications Techniques** :
  * Mettre en place une structure de cache de type "Interval Tree" ou "Segment Tree" en mémoire RAM.
  * Si l'utilisateur déplace le curseur de la date T1 à la date T2 (T2 > T1), le moteur doit uniquement appliquer les deltas d'événements compris dans l'intervalle [T1, T2], réduisant la complexité de calcul de O(N) à O(M) (où M est très petit devant N).

---

## 5. Guide d'Implémentation Visuelle (Design System)

Pour maintenir l'aspect moderne et s'intégrer harmonieusement avec QGIS :

```css
/* Palette et Variables de Style Appliquées au Widget Custom Painter */
@define-color bg_acrylic rgba(28, 28, 30, 220);      /* Fond translucide flouté */
@define-color border_accent rgba(100, 100, 105, 120); /* Bordures fines */
@define-color track_filled #3a91ff;                    /* Couleur du temps écoulé */
@define-color track_empty #424245;                     /* Couleur du temps futur */
@define-color marker_insert #2ecc71;                   /* Vert Émeraude */
@define-color marker_update #f39c12;                   /* Orange Solaire */
@define-color marker_delete #e74c3c;                   /* Rouge Alerte */
```

* **Typographie** : Utiliser la police système par défaut de QGIS (Segoe UI, Ubuntu ou Helvetica Neue) calibrée à 11px pour les indicateurs temporels de la règle de graduation.
* **Transitions et Animations** : Lors du snapping automatique sur un jalon historique, le curseur bleu doit se déplacer via une animation d'interpolation linéaire douce sur une durée de 150ms pour donner une sensation de fluidité et de haute interactivité.
