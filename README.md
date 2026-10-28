# RecoverLand

**Local GIS data audit, temporal snapshot review, and transactional feature recovery for QGIS (3.40 LTR to 4.99, Qt5 & Qt6).**

[![QGIS Plugin Repository](https://img.shields.io/badge/QGIS_Plugin-RecoverLand_(%234898)-2f7d32)](https://plugins.qgis.org/plugins/RecoverLand/)
[![Documentation](https://img.shields.io/badge/Documentation-GitHub_Pages-1f6feb)](https://yadda07.github.io/recoverland/)
[![QGIS Support](https://img.shields.io/badge/QGIS-3.40_LTR_.._4.99_(Qt5_%2F_Qt6)-589632)](https://plugins.qgis.org/plugins/RecoverLand/)
[![License: MIT](https://img.shields.io/badge/License-MIT-slate)](LICENSE)

---

## Why RecoverLand Exists

In QGIS, the native edit buffer (`Ctrl+Z`) is discarded the instant you click **Save Layer Edits** (`commitChanges()`). Once a session is committed to disk or to a database, accidental deletions, overwritten attributes, or broken geometries can no longer be undone from QGIS.

Traditional spatial versioning tools (such as Kart, GeoGig, or server-side PostGIS audit triggers) solve this only if you convert your files into dedicated repositories, install external binaries, or maintain database infrastructure.

**RecoverLand** takes a different approach: it acts as a zero-dependency local black box for your existing layers. Using only Python and the SQLite engine shipped with QGIS, it records every committed modification in a local journal next to your project and lets you audit, inspect at any past timestamp, and restore features directly on your original data sources.

---

## Core Capabilities

| Workflow | What It Does |
| :--- | :--- |
| **1. Automatic Capture** | Tracks committed `INSERT`, `UPDATE` (attributes and WKB geometry), and `DELETE` operations across tracked project layers without altering source schemas. |
| **2. Search & Compare** | Paginated, BLOB-free search across journal history by layer, operation type, date window, or operator, with live rubber-band geometry comparison on the map canvas. |
| **3. Review Snapshot (State at `t`)** | Non-destructive temporal lens that reconstructs a read-only memory overlay of the layer as it existed at a chosen timestamp `t`, with direct GeoPackage export. |
| **4. Selective Restore & Rewind** | Restores individual features or rewinds a layer to a cutoff timestamp, preceded by a preflight compatibility check and backed by one-click session Undo. |

### Supported Providers
* **GeoPackage** (`.gpkg`)
* **ESRI Shapefile** (`.shp`), including deterministic FID shift tracking across OGR repacks
* **PostgreSQL / PostGIS**, **MSSQL**, and **Oracle** (via saved QGIS connections)
* **SpatiaLite** and **Memory** layers

---

## Architectural & Data-Integrity Guarantees

Most QGIS plugins assume `dataProvider()` writes always succeed and that `featureId()` values never move. RecoverLand treats providers as fallible systems and enforces strict invariants:

* **Crash-Safe Pre-Commit Outbox (`capture_outbox`)**: Before a commit reaches the provider, `EditSessionTracker` writes a pre-commit snapshot to a per-journal JSONL outbox, `fsync`s the post-commit diff, and indexes events idempotently into SQLite (`event_uid`, `commit_id`). On startup, `crash_reconcile` inspects any interrupted commit against live provider state.
* **Post-Commit Verification (`CommitGuard`)**: Every restore, rewind, and undo reads back the affected features from the provider using a two-tier WKB signature and canonical vertex comparator (`48x` faster on dense layers). Partial provider failures trigger automatic compensation rather than false success reports.
* **Multi-Repack Shapefile Continuity**: Deleting a feature in a Shapefile causes OGR/GDAL to repack and renumber all higher FIDs at commit end. RecoverLand tracks end-of-session FID shifts deterministically (`shifted_state_at` and relocated slot propagation) across 100+ chained edits, and refuses to overwrite a target feature if its identity cannot be proven.
* **Explicit Coverage & Purge Boundaries**: Logical purges and `Save As` history floors are recorded in `journal_coverage` and `journal_coverage_gap` so a temporal rewind never silently crosses a purged interval.
* **Canonical Datasource Identity (`datasource_alias`)**: Reconciles moved files, display filter subsets, and database URIs through `QgsDataSourceUri` without rewriting historical audit rows.
* **Headless Runtime Validation Suite**: Verified in CI across QGIS `3.40` (Qt5), `4.2.0` (Qt6), and `latest` against 220+ runtime scenarios under `scripts/validation/scenarios/`.

---

## Documentation

Full technical documentation is published on GitHub Pages:

* **[Documentation Home](https://yadda07.github.io/recoverland/)**
* **[User Guide](https://yadda07.github.io/recoverland/guide.html)**: Installation, layer tracking, search, and restoration workflows
* **[System Architecture](https://yadda07.github.io/recoverland/architecture.html)**: Threading model, SQLite schema v7, durable outbox, and `CommitGuard`
* **[Review Snapshot & Temporal Lens](https://yadda07.github.io/recoverland/review.html)**: Non-destructive reconstruction at timestamp `t`
* **[RecoverLand vs. GeoGit / Kart](https://yadda07.github.io/recoverland/geogit.html)**: Architectural comparison between local audit journaling and distributed GIS versioning
* **[Supported Formats & Providers](https://yadda07.github.io/recoverland/formats.html)**: Provider matrix and primary-key handling
* **[Known Limits & Identity Proofs](https://yadda07.github.io/recoverland/limits.html)**: Unstable FIDs, Shapefile repacks, and recovery windows
* **[Journal Maintenance](https://yadda07.github.io/recoverland/maintenance.html)**: Retention policies, logical purge, `VACUUM`, and diagnostics

---

## Installation

1. In QGIS, open **Plugins -> Manage and Install Plugins...**
2. Search for **RecoverLand** and click **Install Plugin**.
3. Alternatively, download the latest release archive from the [QGIS Python Plugins Repository](https://plugins.qgis.org/plugins/RecoverLand/) or from [GitHub Releases](https://github.com/yadda07/recoverland/releases).

---

## Présentation en Français

**RecoverLand** est une extension QGIS d'audit local, d'inspection temporelle et de restauration transactionnelle pour couches vectorielles, compatible **QGIS 3.40 LTR à 4.99 (Qt5 et Qt6)** sans aucune dépendance externe.

### Le problème résolu
Dans QGIS, l'historique d'annulation natif (`Ctrl+Z`) est détruit dès que l'opérateur enregistre ses modifications (`commitChanges()`). À l'inverse, les outils de gestion de versions comme Kart ou les schémas d'audit PostGIS imposent de convertir les données ou d'administrer un serveur.

RecoverLand s'attache directement à vos couches existantes (**GeoPackage, Shapefile, PostgreSQL/PostGIS, SpatiaLite**) et consigne chaque modification validée (création, modification d'attribut ou de géométrie, suppression) dans un journal SQLite local au projet.

### Points forts techniques
* **Outbox durable pré-commit et réconciliation au démarrage** : aucune modification perdue ni dupliquée en cas d'arrêt brutal de QGIS pendant un enregistrement.
* **Vérification post-commit (`CommitGuard`)** : contrôle systématique de l'état réel écrit par le provider après restauration et compensation automatique en cas d'échec partiel.
* **Continuité sur Shapefile après *repack* OGR** : suivi déterministe de la renumérotation des FIDs après suppressions successives, interdisant tout écrasement silencieux d'une entité voisine.
* **Mode Review Snapshot (état à l'instant `t`)** : reconstruction en couche mémoire (hors thread GUI) de l'état exact d'une couche à une date donnée, avec export direct en GeoPackage.

Documentation complète : **[yadda07.github.io/recoverland](https://yadda07.github.io/recoverland/)**

---

## Author & Professional Support

**Youcef ADDA | [GEODECI](https://geodeci.xyz/)**
* **FR** : Développement de plugins QGIS et de solutions SIG sur mesure, administration PostgreSQL/PostGIS, audit et automatisation de flux géomatiques.
* **EN** : Custom QGIS plugin and GIS solution development, PostgreSQL/PostGIS database administration, GIS audits and workflow automation.
* **AR** : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية، وإدارة قواعد بيانات PostgreSQL/PostGIS، والتدقيق وأتمتة سير العمل ومرافقة المؤسسات.

**Contact** : [youcef.geodesien@gmail.com](mailto:youcef.geodesien@gmail.com) | [contact@geodeci.xyz](mailto:contact@geodeci.xyz) | [geodeci.xyz](https://geodeci.xyz/)

## License

MIT License. See [LICENSE](LICENSE) and [SIGNATURE.md](SIGNATURE.md).
