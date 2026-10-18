"""i18n_catalogue_complete - RecoverLand validation runtime.

Invariant: toute chaine que le plugin fait passer par la traduction a une
traduction anglaise fidele dans recoverland_en.ts, et le .qm livre la rend.

Le mecanisme
------------
Le plugin est ecrit en francais. Une chaine montree a l'utilisateur passe
par l'un de ces chemins, et QTranslator la cherche par le couple exact
(contexte, source) :

- ``self.tr("...")`` dans une classe : le contexte est le NOM DE LA CLASSE
  DE L'INSTANCE (PyQt passe ``type(self).__name__``, mesure : une sous-
  classe perd la traduction de sa classe mere) ;
- ``QCoreApplication.translate("Ctx", "...")``, direct ou via un alias
  ``tr = QCoreApplication.translate`` ;
- un assistant de module ``_tr(msg)`` qui appelle ``translate("Ctx", msg)``
  (core/time_format.py, core/health_monitor.py, ...) ;
- les libelles de defaillance de recover.py (``@_plugin_slot("...")``,
  ``_guarded_call(cle, "...")``, ``_slot_failed(cle, "...")``), traduits au
  moment de l'alerte par ``translate("RecoverPlugin", label)``.

Un couple absent du .ts n'est pas une erreur visible : QTranslator rend la
source, et l'interface anglaise affiche du francais. Les passes v7 ont
ajoute des centaines de messages (refus de Rewind, alertes de capture,
maintenance, Review) sans toucher au .ts. A l'inverse, des messages dont
la source a disparu du code restent dans le .ts et font croire qu'une
chaine est traduite alors que le code en affiche une autre.

Le scenario d'echec concret
---------------------------
Un utilisateur dont QGIS est en anglais lance un Rewind pendant qu'une
restauration tourne : le refus s'affiche en francais ("Recherche ou Rewind
refuse(e) : un Rewind est en cours..."). Une alerte de perte de capture,
un refus de restauration, la boite de compactage : tout s'affiche en
francais. Et l'infobulle Review montre un tiret cadratin, proscrit par la
regle typographique du projet, recopie dans la traduction.

Ce que le scenario execute reellement
-------------------------------------
Analyse syntaxique (ast) de tout le code du plugin hors scripts/ et i18n/,
avec les quatre chemins ci-dessus ; lecture du .ts ; chargement du .qm dans
un QTranslator installe le temps du test, et resolution de chaque message
par QCoreApplication.translate (le chemin reel de self.tr).

Verdict attendu avant correctif : FAIL (messages absents, messages morts,
doublons, tiret cadratin dans trois traductions).
"""
from __future__ import annotations

import ast
import json
import re
import xml.etree.ElementTree as ET  # nosec B405 - the plugin's own .ts file
from collections import Counter
from pathlib import Path

SCENARIO_ID = "i18n_catalogue_complete"
INVARIANT = "I18N-CATALOGUE-COMPLETE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_TS_PATH = _PLUGIN_ROOT / "i18n" / "recoverland_en.ts"
_QM_PATH = _PLUGIN_ROOT / "i18n" / "recoverland_en.qm"

# Never shipped as user interface: validation tooling, the catalogue itself,
# and the release staging copies (build/, dist/) of an older tree.
_EXCLUDED_PARTS = {"scripts", "i18n", "__pycache__", "docs", "build", "dist"}
# recover.py: labels translated later by translate("RecoverPlugin", label).
_LABEL_CALLS = {"_plugin_slot": 0, "_guarded_call": 1, "_slot_failed": 1}
_LABEL_CONTEXT = "RecoverPlugin"
_FORBIDDEN = (chr(0x2014), chr(0x2013))  # em dash, en dash
_PLACEHOLDER = re.compile(r"\{[^{}]*\}|%\d+|%n")


def _code_files():
    for path in sorted(_PLUGIN_ROOT.rglob("*.py")):
        rel = path.relative_to(_PLUGIN_ROOT)
        if any(part in _EXCLUDED_PARTS for part in rel.parts):
            continue
        yield path, rel.as_posix()


def _literal(node, consts):
    """The string a node always evaluates to, or None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in consts:
        return consts[node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _literal(node.left, consts), _literal(node.right, consts)
        if left is not None and right is not None:
            return left + right
    return None


def _is_translate(func) -> bool:
    return isinstance(func, ast.Attribute) and func.attr == "translate"


def _scan(path: Path, rel: str, out: dict) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def enclosing(node, kinds):
        node = parents.get(node)
        while node is not None and not isinstance(node, kinds):
            node = parents.get(node)
        return node

    consts = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            value = _literal(node.value, {})
            if value is not None:
                consts[node.targets[0].id] = value
    # def _tr(msg): return QCoreApplication.translate("Ctx", msg)
    helpers = {}
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        params = [a.arg for a in fn.args.args]
        for ret in ast.walk(fn):
            call = getattr(ret, "value", None) if isinstance(ret, ast.Return) else None
            if (isinstance(call, ast.Call) and _is_translate(call.func)
                    and len(call.args) >= 2 and isinstance(call.args[1], ast.Name)
                    and call.args[1].id in params):
                context = _literal(call.args[0], consts)
                if context is not None:
                    index = params.index(call.args[1].id)
                    helpers[fn.name] = (context, index - (params[:1] == ["self"]))
    # tr = QCoreApplication.translate
    aliases = {node.targets[0].id for node in ast.walk(tree)
               if isinstance(node, ast.Assign) and len(node.targets) == 1
               and isinstance(node.targets[0], ast.Name)
               and _is_translate(node.value)}

    def add(line, context, arg, kind):
        source = _literal(arg, consts)
        where = f"{rel}:{line}"
        if source is None:
            # A variable forwarded by a helper is the helper's own body;
            # anything else (f-string, .format before tr) cannot match.
            if not isinstance(arg, ast.Name):
                out["non_literal"].append(f"{where} {kind} {ast.unparse(arg)[:80]}")
            return
        out["strings"].setdefault((context, source), where)
        out["kinds"][kind] += 1

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            out["bases"][node.name] = [ast.unparse(b).split(".")[-1] for b in node.bases]
        if not isinstance(node, ast.Call):
            continue
        func, args = node.func, node.args
        if (isinstance(func, ast.Attribute) and func.attr == "tr"
                and isinstance(func.value, ast.Name) and func.value.id == "self"):
            cls = enclosing(node, ast.ClassDef)
            if cls is not None and args:
                out["tr_classes"].add(cls.name)
                add(node.lineno, cls.name, args[0], "self.tr")
        elif _is_translate(func) or (isinstance(func, ast.Name) and func.id in aliases):
            if len(args) >= 2:
                context = _literal(args[0], consts)
                if context is not None:
                    add(node.lineno, context, args[1], "translate")
        else:
            # f(...) or self.f(...): a module helper, a method helper, or
            # (recover.py only) a failure label.
            if isinstance(func, ast.Name):
                name = func.id
            elif (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                  and func.value.id == "self"):
                name = func.attr
            else:
                continue
            index = _LABEL_CALLS.get(name) if rel == "recover.py" else None
            if index is not None:
                if len(args) > index:
                    add(node.lineno, _LABEL_CONTEXT, args[index], "slot_label")
            elif name in helpers:
                context, index = helpers[name]
                if len(args) > index:
                    add(node.lineno, context, args[index], "helper")


def _read_ts():
    """(active {(ctx, src): translation}, duplicates, translations incl. vanished)."""
    tree = ET.parse(_TS_PATH)  # nosec B314 - the plugin's own catalogue
    active, duplicates, every = {}, [], []
    for ctx in tree.getroot().iter("context"):
        name = ctx.findtext("name") or ""
        for msg in ctx.iter("message"):
            source = msg.findtext("source") or ""
            tr = msg.find("translation")
            text = (tr.text or "") if tr is not None else ""
            every.append((name, source, text))
            if tr is not None and tr.get("type") in ("vanished", "obsolete"):
                continue
            if (name, source) in active:
                duplicates.append(f"{name}/{source[:50]!r}")
            active[(name, source)] = text
    return active, duplicates, every


def _resolve_qm(active: dict):
    """Messages the shipped .qm does not give back, through the real lookup."""
    from qgis.PyQt.QtCore import QCoreApplication, QTranslator
    app = QCoreApplication.instance()
    translator = QTranslator()
    if app is None or not translator.load(str(_QM_PATH)):
        return None, "qm_unloadable"
    app.installTranslator(translator)
    try:
        wrong = []
        for (context, source), text in active.items():
            if not text:
                continue
            got = QCoreApplication.translate(context, source)
            if got != text:
                wrong.append(f"{context}/{source[:40]!r}->{got[:40]!r}")
        return wrong, ""
    finally:
        app.removeTranslator(translator)


def setup(ctx):
    return


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    out = {"strings": {}, "kinds": Counter(), "non_literal": [],
           "bases": {}, "tr_classes": set()}
    for path, rel in _code_files():
        _scan(path, rel, out)
    code = out["strings"]
    active, duplicates, every = _read_ts()

    d["n_code"] = len(code)
    d["kinds"] = dict(out["kinds"])
    d["anchors_found"] = [
        pair for pair in (("RecoverDialog", "Rewind"),
                          ("RecoverPlugin", "Journal corrompu"),
                          ("TimeFormat", "a l'instant"))
        if pair in code]
    d["missing"] = sorted(f"{code[k]} ctx={k[0]} src={k[1][:60]!r}"
                          for k in code if not active.get(k, "").strip())
    d["stale"] = sorted(f"{k[0]}/{k[1][:60]!r}" for k in active if k not in code)
    d["duplicates"] = duplicates
    d["placeholder_drift"] = sorted(
        f"{c}/{s[:50]!r}" for (c, s), t in active.items()
        if t and (Counter(_PLACEHOLDER.findall(s)) != Counter(_PLACEHOLDER.findall(t))
                  or s.count("\n") != t.count("\n")))
    d["dash_translations"] = sorted(
        f"{c}/{t[:50]!r}" for c, _s, t in every if any(ch in t for ch in _FORBIDDEN))
    d["dash_sources_in_code"] = sorted(
        where for (c, s), where in code.items() if any(ch in s for ch in _FORBIDDEN))
    # PyQt's tr() takes the context from the instance's class: a subclass
    # of a class that calls self.tr() looks its strings up under its own
    # name, which no .ts entry carries.
    d["subclassed_tr_classes"] = sorted(
        f"{name}({base})" for name, bases in out["bases"].items()
        for base in bases if base in out["tr_classes"])
    d["non_literal"] = out["non_literal"]
    wrong, why = _resolve_qm(active)
    d["qm_error"] = why
    d["qm_unresolved"] = wrong if wrong is not None else []
    d["n_active"] = len(active)
    flog(f"i18n_catalogue_complete run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, ensure_ascii=True)[:3000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("code_scan_sees_every_path",
         d.get("n_code", 0) >= 200 and len(d.get("anchors_found", [])) == 3,
         f"chaines traduisibles trouvees={d.get('n_code')} par chemin="
         f"{d.get('kinds')} reperes={d.get('anchors_found')} ; un scan aveugle "
         f"validerait un catalogue vide"),
        ("every_code_string_translated",
         d.get("n_code", 0) > 0 and not d.get("missing"),
         f"{len(d.get('missing', []))} chaine(s) sans traduction anglaise, "
         f"affichee(s) en francais dans une interface anglaise : "
         f"{d.get('missing', [])[:8]}"),
        ("no_stale_ts_message",
         not d.get("stale"),
         f"{len(d.get('stale', []))} message(s) du .ts dont la source n'existe "
         f"plus dans le code (jamais affiches, ils font croire qu'une chaine "
         f"est traduite) : {d.get('stale', [])[:8]}"),
        ("no_duplicate_ts_message",
         not d.get("duplicates"),
         f"doublons (contexte, source) : {d.get('duplicates')} ; une seule "
         f"traduction est prise, l'autre est un piege de relecture"),
        ("placeholders_and_lines_preserved",
         not d.get("placeholder_drift"),
         f"traductions dont les champs {{...}}/%n ou les retours a la ligne "
         f"different de la source (format() leve ou perd une valeur) : "
         f"{d.get('placeholder_drift')}"),
        ("no_em_or_en_dash_in_translation",
         not d.get("dash_translations"),
         f"traductions avec un tiret cadratin ou demi-cadratin (proscrit par la "
         f"regle du projet) : {d.get('dash_translations')} ; sources du code qui "
         f"en portent encore (hors perimetre i18n, a corriger dans le code) : "
         f"{d.get('dash_sources_in_code')}"),
        ("self_tr_classes_not_subclassed",
         not d.get("subclassed_tr_classes"),
         f"sous-classes d'une classe qui appelle self.tr() (PyQt cherche sous "
         f"le nom de la sous-classe : traduction perdue) : "
         f"{d.get('subclassed_tr_classes')}"),
        ("shipped_qm_resolves_every_message",
         not d.get("qm_error") and d.get("n_active", 0) > 0
         and not d.get("qm_unresolved"),
         f"erreur={d.get('qm_error')!r} messages actifs={d.get('n_active')} "
         f"non rendus par le .qm livre={len(d.get('qm_unresolved', []))} "
         f"{d.get('qm_unresolved', [])[:6]} ; le .qm est plus ancien que le .ts "
         f"par son contenu, quelle que soit sa date"),
        assert_log_contains(
            ctx.records, rf"i18n_catalogue_complete.*trace_id={ctx.trace_id}",
            name="trace_id_propagated", min_count=1),
    ]


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
