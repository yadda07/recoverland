# God Object non-regression checks (golden_logs/)

Filet de non-regression **move-only** du chantier God Object (`BL-DIAG-P2-14`).
Voir `docs/backlog_god_object_recover_dialog_2026-06-21.md`.

## Modele retenu (decision D-GOD-01) : jalons ordonnes par mode

Plutot que geler une sequence de log complete (fragile : rotation, bruit paint,
capture manuelle), on verifie que les **jalons clefs de chaque mode** apparaissent
**dans le bon ordre**, en ignorant tout log non-jalon. Les jalons sont **derives
du code reel** de `recover_dialog.py` et vivent dans
`scripts/validation/god_object_check.py` (dict `MILESTONES`).

Avant ET apres chaque phase d'extraction, on exerce le mode et on verifie que la
meme sequence de jalons tient. Toute perte/inversion d'un jalon = FAIL.

### Modes couverts

| mode | jalons (dans l'ordre) |
|---|---|
| `event_search` | `recover_and_load: START` -> `recover_event: start` -> `_display_search_result: total_count=` -> `restore_event: start` -> `restore_event: done` |
| `version_rewind` | `recover_version: start` -> `on_version_fetch_done: raw=` -> `recover_version: done` |
| `review_snapshot` | `review: snapshot_mode_start` -> `snapshot_init_direct` -> `snapshot_bar_shown` -> `snapshot_ready` |
| `undo` | `undo_last: requested` -> `undo_done: trace invalidation` |

`dashboard` n'est **pas** couvert : le flux stats (`_request_stats_refresh`,
`_on_stats_ready`) n'emet aucun jalon `flog`. Le couvrir exigerait d'ajouter de
l'instrumentation d'abord (item separe), pas d'inventer une spec.

## Procedure (console Python QGIS, plugin charge)

```python
import sys
for m in [k for k in list(sys.modules) if k.startswith('scripts.validation')]:
    del sys.modules[m]                       # purge le cache QGIS (modules a jour)
sys.path.insert(0, r'C:\Users\yadda\AppData\Roaming\QGIS\QGIS4\profiles\default\python\plugins\recoverland')
from scripts.validation import god_object_check as gc

gc.mark('event_search')        # 1) pose un marqueur sentinelle dans le log
# 2) exercer le mode dans la GUI (cf. tableau ci-dessus)
gc.check_mode('event_search')  # 3) -> verdict=PASS si les jalons tiennent dans l'ordre
```

Le fenetrage utilise un marqueur (`GOLDEN_MARK id=<mode>` flogue par `mark()`),
robuste a la rotation du log (contrairement a une capture par offset). Si un
jalon manque, le message indique lequel : `milestone[i] /.../ not found`.

## Auto-test hors QGIS

```
python -m scripts.validation.god_object_check --selftest   # 10 cas: PASS/FAIL par mode
python -m scripts.validation.assert_log --selftest         # primitive assert_sequence_in_order
```

## Golden full-sequence (secondaire, optionnel)

`golden.py` + `diff_against_golden` conservent la comparaison de sequence complete
(avec garde-fous anti-vide / anti-rotation, `python -m scripts.validation.golden
--selftest`) pour un usage cible. Les `.golden` eventuels sont versionnes ; ils ne
sont **pas requis** par le modele D-GOD-01.
