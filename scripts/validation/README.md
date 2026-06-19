# scripts/validation/

Outillage de validation **par scénarios runtime QGIS + logs structurés**.

> Référence : `docs/validation_strategy.md` et `docs/rewind_charter.md §11`
> (décision D-2026-05-14-01).

---

## Quick start

Dans la console Python QGIS, projet de test ouvert :

```python
from pathlib import Path
SCRIPTS = Path(r'C:\Users\yadda\AppData\Roaming\QGIS\QGIS4\profiles\default\python\plugins\recoverland\scripts')

# 1) Lancer un scénario unitaire
exec(compile(
    (SCRIPTS / 'validation/scenarios/i9_cutoff_inclusivity.py').read_text(),
    str(SCRIPTS / 'validation/scenarios/i9_cutoff_inclusivity.py'),
    'exec'))
# → imprime un verdict PASS/FAIL et écrit
#   scripts/validation/reports/i9_cutoff_inclusivity_<timestamp>.json

# 2) Lancer la suite régression complète (avant de marquer un item [DONE])
import sys; sys.path.insert(0, str(SCRIPTS.parent))
from scripts.validation.runner import run_regression_suite
run_regression_suite()
# → écrit scripts/validation/regression_report_<timestamp>.json
```

---

## Modules

| Module | Rôle |
|--------|------|
| `runner.py` | Lance un scénario, capture les logs entre T0 et fin, exécute les assertions, écrit le verdict JSON. |
| `parse_log.py` | Parse `recoverland_debug.log` en lignes structurées (timestamp, level, thread, event, fields key=value). |
| `assert_log.py` | Assertions de signature : `assert_log_contains(pattern)`, `assert_no_log_between(pattern, marker_start, marker_end)`, `diff_against_golden(scenario_id)`. |
| `cleanup.py` | Rotation des reports > 30 jours sauf 5 plus récents. |

---

## Structure

```
scripts/validation/
├── README.md
├── __init__.py
├── runner.py
├── parse_log.py
├── assert_log.py
├── cleanup.py
├── golden_logs/          # extraits de référence (versionnés)
├── fixtures/             # données test (versionnés)
├── reports/              # rapports JSON par exécution (gitignorés)
└── scenarios/            # un fichier par scénario (versionnés)
    ├── __init__.py
    ├── _template.py      # squelette à copier pour créer un nouveau scénario
    ├── i9_cutoff_inclusivity.py
    ├── ...
    ├── providers/
    ├── hypotheses/
    └── regression/
```

---

## Critères d'acceptation d'un scénario

Voir `docs/validation_strategy.md §5`.

---

## Hygiène

- `reports/*.json` : gitignored (volatile).
- `golden_logs/*.log` : versionnés (référence verrouillée).
- `fixtures/**` : versionnés (jeu de données minimal reproductible).
- Lancer `python scripts/validation/cleanup.py` périodiquement.
