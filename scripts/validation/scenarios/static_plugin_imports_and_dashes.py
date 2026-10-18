"""static_plugin_imports_and_dashes - RecoverLand validation (lecture du source).

Invariant: I-CODE (le code du plugin ne porte ni reste mort ni typographie
proscrite).

Le mecanisme
------------
recover.py importait `evaluate_journal_health` depuis `.core` sans plus
l'appeler nulle part : l'indicateur v7 est calcule par
core/capture_health.evaluate_capture_health, a partir des faits d'execution.
Un import mort n'est pas anodin ici. Il laisse croire, a la lecture, que
l'etat affiche derive encore de la sante du FICHIER journal (taille, age),
ce qui etait precisement le defaut de l'ancien indicateur ("vert" calcule
d'un reglage) ; et il garde recover.py couple a une API qu'il n'utilise pas.

Deuxieme regle, celle du proprietaire du depot : aucun tiret cadratin
(U+2014) ni demi-cadratin (U+2013), nulle part. core/observability.py en
portait dans ses docstrings et commentaires.

Ce que le scenario execute reellement
-------------------------------------
Analyse AST de recover.py : chaque nom qu'il importe doit y etre utilise
(nom ou base d'attribut). Lecture brute de recover.py, status_bar_widget.py,
core/capture_health.py et core/observability.py : aucun U+2014 / U+2013.
`_SOURCES` peut etre repointe (mesure avant correctif sur une copie).

Verdict attendu avant correctif : FAIL (evaluate_journal_health importe et
inutilise ; tirets dans core/observability.py).
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

SCENARIO_ID = "static_plugin_imports_and_dashes"
INVARIANT = "I-CODE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_SOURCES = {
    "recover.py": _PLUGIN_ROOT / "recover.py",
    "status_bar_widget.py": _PLUGIN_ROOT / "status_bar_widget.py",
    "core/capture_health.py": _PLUGIN_ROOT / "core" / "capture_health.py",
    "core/observability.py": _PLUGIN_ROOT / "core" / "observability.py",
}
# Built from their code points: this file must not carry them either.
_DASHES = (chr(0x2014), chr(0x2013))


def _unused_imports(src: str) -> list:
    tree = ast.parse(src)
    imported = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    imported[alias.asname or alias.name] = node.lineno
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported[(alias.asname or alias.name).split(".")[0]] = node.lineno
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            used.add(node.id)
        elif isinstance(node, ast.Attribute):
            base = node
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name):
                used.add(base.id)
    exported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets):
            if isinstance(node.value, (ast.List, ast.Tuple)):
                exported.update(e.value for e in node.value.elts
                                if isinstance(e, ast.Constant))
    return sorted(f"{name} (ligne {line})" for name, line in imported.items()
                  if name not in used and name not in exported)


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    texts = {name: path.read_text(encoding="utf-8")
             for name, path in _SOURCES.items()}
    ctx.data["unused"] = _unused_imports(texts["recover.py"])
    dashes = {}
    for name, text in texts.items():
        lines = [i for i, line in enumerate(text.splitlines(), 1)
                 if any(d in line for d in _DASHES)]
        if lines:
            dashes[name] = lines
    ctx.data["dashes"] = dashes
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} unused="
         f"{ctx.data['unused']} dashes={dashes}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    return [
        ("recover_imports_are_all_used",
         ctx.data.get("unused") == [],
         f"imports inutilises de recover.py={ctx.data.get('unused')} : un reste "
         f"mort fait croire que l'indicateur derive encore de ce qu'il n'appelle "
         f"plus"),
        ("no_em_or_en_dash",
         ctx.data.get("dashes") == {},
         f"lignes portant un tiret cadratin ou demi-cadratin={ctx.data.get('dashes')}"),
        assert_log_contains(
            ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
            name="trace_id_propagated", min_count=2),
    ]


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
