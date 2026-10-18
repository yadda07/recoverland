"""maint_text_no_em_dash - RecoverLand validation runtime.

Invariant: regle typographique du projet (aucun tiret cadratin ni
demi-cadratin, ni dans le code ni dans les textes montres a
l'utilisateur).

Le mecanisme
------------
La regle de l'auteur interdit le tiret cadratin (U+2014) et le tiret
demi-cadratin (U+2013) partout : commentaires, chaines affichees, messages
de log. La maintenance du journal en affichait un dans une chaine traduite
(`_vacuum_journal`, "{count} donnees mortes ... purger d'abord") : la
source du .ts le reprend, la traduction anglaise aussi, et le caractere
reapparait a chaque passe i18n.

Le scenario d'echec concret
---------------------------
L'utilisateur clique "Compacter" sur un journal sans page libre mais plein
de traces mortes : l'etiquette d'etat affiche le tiret proscrit.

Ce que le scenario execute reellement
-------------------------------------
Lecture des fichiers de la zone stockage (maintenance du journal, gestion
du journal, integrite, chemins longs, ecritures du journal) : aucune ligne
ne contient U+2014 ni U+2013. Un fichier absent de la version testee est
ignore (dit dans le message).

Verdict attendu avant correctif : FAIL (journal_maintenance.py).
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "maint_text_no_em_dash"
INVARIANT = "TYPO-NO-DASH"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FILES = (
    "journal_maintenance.py",
    "core/journal_manager.py",
    "core/integrity.py",
    "core/long_paths.py",
    "core/journal_writes.py",
)
_FORBIDDEN = (chr(0x2014), chr(0x2013))  # em dash, en dash


def setup(ctx):
    return


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    hits, missing = [], []
    for rel in _FILES:
        path = _PLUGIN_ROOT / rel
        if not path.is_file():
            missing.append(rel)
            continue
        for no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if any(ch in line for ch in _FORBIDDEN):
                hits.append(f"{rel}:{no}: {line.strip()[:120]}")
    d["hits"] = hits
    d["missing"] = missing
    d["scanned"] = len(_FILES) - len(missing)
    flog(f"maint_text_no_em_dash run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, ensure_ascii=True)[:2000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("storage_area_files_scanned",
         d.get("scanned", 0) >= 3,
         f"fichiers lus={d.get('scanned')} absents de cette version="
         f"{d.get('missing')}"),
        ("no_em_or_en_dash_in_storage_area",
         d.get("scanned", 0) > 0 and not d.get("hits"),
         f"lignes avec un tiret cadratin ou demi-cadratin : {d.get('hits')} ; "
         f"l'utilisateur lit un caractere que la regle du projet proscrit, "
         f"et la passe i18n le recopie dans la traduction"),
        assert_log_contains(
            ctx.records, rf"maint_text_no_em_dash.*trace_id={ctx.trace_id}",
            name="trace_id_propagated", min_count=1),
    ]


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
