# golden_logs/

References verrouillees pour la non-regression de comportement du chantier
God Object (`BL-DIAG-P2-14`). Voir
`docs/backlog_god_object_recover_dialog_2026-06-21.md` section 7.

Un fichier `<golden_id>.golden` contient la **sequence ordonnee des jalons de log
main-thread** d'un scenario de reference, tokens volatils masques (trace id,
uuid, durees, adresses). Apres chaque phase d'extraction, on rejoue le scenario
et `diff_against_golden` exige une sequence identique.

## Baselines attendues (5 modes)

| golden_id | Mode | Actions a executer dans QGIS |
|---|---|---|
| `dashboard` | Dashboard | ouvrir le dialog, changer la couche puis l'operation |
| `event_search` | Event search | rechercher des events, en restaurer un |
| `version_rewind` | Version rewind | rewind avec auto-undo, attendre le resume |
| `review_snapshot` | Review snapshot | toggle review, changer la date, pan/zoom |
| `undo` | Undo | undo last restore, puis undo session |

## Capture (depuis la console Python QGIS, plugin charge)

```python
import sys
from pathlib import Path
SCRIPTS = Path(r'C:\Users\yadda\AppData\Roaming\QGIS\QGIS4\profiles\default\python\plugins\recoverland')
sys.path.insert(0, str(SCRIPTS))
from scripts.validation import golden

off = golden.capture_start()          # 1) marque le debut
# 2) executer manuellement les actions du mode (cf. tableau)
golden.capture_finish('event_search', off)   # 3) ecrit golden_logs/event_search.golden
```

Recommencer pour chaque `golden_id`. Les `.golden` sont **versionnes** (reference
verrouillee), contrairement a `reports/` qui est gitignore.

## Verification (apres une phase d'extraction)

`diff_against_golden(records, '<golden_id>')` dans les assertions d'un scenario,
ou comparaison directe via `golden.compare_sequences(...)`.

Tooling auto-teste hors QGIS : `python -m scripts.validation.golden --selftest`.
