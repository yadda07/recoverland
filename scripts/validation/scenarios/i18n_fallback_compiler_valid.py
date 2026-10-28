# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""i18n_fallback_compiler_valid - RecoverLand validation runtime.

Invariant: i18n/compile_translations.py ecrit un .qm que QTranslator lit,
qu'il passe par lrelease ou par son compilateur Python de repli, et le
nombre de messages qu'il annonce est celui que le .qm rend.

Le mecanisme
------------
compile_translations.py prefere lrelease et retombe sinon sur un
compilateur Python. lrelease n'est livre ni avec QGIS 4.x ni avec OSGeo4W :
sur le poste d'un mainteneur qui n'a que QGIS, c'est le repli qui ecrit le
.qm. Ce repli ecrivait les champs du format Qt 3 (Context16, SourceText16)
et hachait "contexte\\nsource\\n" au lieu de "source + commentaire".
QTranslator (qtranslator.cpp, getMessage) refuse tout message qui porte
une balise qu'il ne connait pas, et cherche par le hachage de la source
seule : aucun message n'est jamais trouve. Le fichier se charge pourtant,
isEmpty() vaut False, et le script annonce "(339 messages)".

Le scenario d'echec concret
---------------------------
Un mainteneur sans lrelease met a jour le .ts puis lance
``python i18n/compile_translations.py`` : le script annonce 339 messages,
i18n_coverage voit un .qm plus recent que le .ts (qm_fresh PASS), et le
plugin livre parle francais a tous les utilisateurs anglophones, sans un
seul message d'erreur. (Le build de release le refuse par son controle de
contenu ; tout ce qui precede le build est trompe.)

Ce que le scenario execute reellement
-------------------------------------
Compilation du vrai .ts par le repli (lrelease masque), dans un dossier
temporaire ; chargement du .qm obtenu dans un QTranslator installe le
temps du test ; resolution, par QCoreApplication.translate (le chemin de
self.tr), de chaque message actif dont la traduction differe de la source
(sinon un echec ne se verrait pas). Meme verification sur compile_ts_to_qm
sans lrelease, le point d'entree du script. Deux .ts synthetiques couvrent
ce que le catalogue actuel n'emploie pas : une source distinguee par un
commentaire de desambiguisation (chaque sens doit garder sa traduction), et
un message au pluriel, que le repli doit refuser (il n'ecrit pas les regles
de pluriel) au lieu d'ecrire une traduction faite d'espaces.

Verdict attendu avant correctif : FAIL (0 message resolu sur le .qm de
repli, 339 annonces ; pluriel ecrit sans erreur).
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import tempfile
import xml.etree.ElementTree as ET  # nosec B405 - the plugin's own .ts file
from pathlib import Path

SCENARIO_ID = "i18n_fallback_compiler_valid"
INVARIANT = "I18N-QM-FALLBACK-READABLE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_TS_PATH = _PLUGIN_ROOT / "i18n" / "recoverland_en.ts"
_COMPILER = _PLUGIN_ROOT / "i18n" / "compile_translations.py"

# Two shapes today's catalogue does not use: the same source told apart by
# a disambiguation comment, and a plural (numerus) message.
_SYNTHETIC_HEAD = ('<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n'
                   '<TS version="2.1" language="en" sourcelanguage="fr">\n'
                   '<context>\n    <name>RlSyntheticCtx</name>\n')
_SYNTHETIC_COMMENT = _SYNTHETIC_HEAD + (
    '    <message>\n        <source>Ouvrir</source>\n'
    '        <comment>fichier</comment>\n'
    '        <translation>Open file</translation>\n    </message>\n'
    '    <message>\n        <source>Ouvrir</source>\n'
    '        <translation>Open</translation>\n    </message>\n'
    '</context>\n</TS>\n')
_SYNTHETIC_NUMERUS = _SYNTHETIC_HEAD + (
    '    <message numerus="yes">\n        <source>%n couche(s)</source>\n'
    '        <translation>\n            <numerusform>%n layer</numerusform>\n'
    '            <numerusform>%n layers</numerusform>\n'
    '        </translation>\n    </message>\n'
    '</context>\n</TS>\n')


def _load_compiler():
    """The compiler module of THIS tree, loaded by path (i18n/ is no package).

    No bytecode is written: the scenario leaves no __pycache__ in i18n/.
    """
    import sys
    spec = importlib.util.spec_from_file_location(
        "recoverland_compile_translations_under_test", str(_COMPILER))
    module = importlib.util.module_from_spec(spec)
    previous, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


def _checkable_messages():
    """Active messages whose translation differs from the source."""
    tree = ET.parse(_TS_PATH)  # nosec B314 - the plugin's own catalogue
    pairs = {}
    for ctx in tree.getroot().iter("context"):
        name = ctx.findtext("name") or ""
        for msg in ctx.iter("message"):
            tr = msg.find("translation")
            if tr is None or tr.get("type") in ("vanished", "obsolete"):
                continue
            source = msg.findtext("source") or ""
            if tr.text and tr.text != source:
                pairs[(name, source)] = tr.text
    return pairs


def _resolve(qm_path: Path, pairs: dict):
    """(loaded, resolved, failures) of *qm_path* through the real lookup."""
    from qgis.PyQt.QtCore import QCoreApplication, QTranslator
    app = QCoreApplication.instance()
    translator = QTranslator()
    if app is None or not translator.load(str(qm_path)):
        return False, 0, ["load_failed"]
    app.installTranslator(translator)
    try:
        failures = []
        for (context, source), text in pairs.items():
            got = QCoreApplication.translate(context, source)
            if got != text:
                failures.append(f"{context}/{source[:30]!r}->{got[:30]!r}")
        return True, len(pairs) - len(failures), failures
    finally:
        app.removeTranslator(translator)


def setup(ctx):
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_i18n_")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    tmp = Path(d["tmp"])
    compiler = _load_compiler()
    pairs = _checkable_messages()
    d["n_checkable"] = len(pairs)

    fallback_qm = tmp / "fallback.qm"
    d["fallback_reported"] = compiler._compile_with_custom(str(_TS_PATH), str(fallback_qm))
    d["fallback_loaded"], d["fallback_resolved"], failures = _resolve(fallback_qm, pairs)
    d["fallback_failures"] = failures[:6]

    # The script's own entry point on a machine without lrelease.
    compiler._find_lrelease = lambda: None
    entry_qm = tmp / "entry.qm"
    d["entry_reported"] = compiler.compile_ts_to_qm(str(_TS_PATH), str(entry_qm))
    d["entry_loaded"], d["entry_resolved"], failures = _resolve(entry_qm, pairs)
    d["entry_failures"] = failures[:6]

    from qgis.PyQt.QtCore import QCoreApplication, QTranslator
    synth_ts, synth_qm = tmp / "comment.ts", tmp / "comment.qm"
    synth_ts.write_text(_SYNTHETIC_COMMENT, encoding="utf-8")
    compiler._compile_with_custom(str(synth_ts), str(synth_qm))
    translator = QTranslator()
    d["comment_got"] = None
    if translator.load(str(synth_qm)):
        app = QCoreApplication.instance()
        app.installTranslator(translator)
        try:
            d["comment_got"] = [
                QCoreApplication.translate("RlSyntheticCtx", "Ouvrir", "fichier"),
                QCoreApplication.translate("RlSyntheticCtx", "Ouvrir")]
        finally:
            app.removeTranslator(translator)
    plural_ts = tmp / "numerus.ts"
    plural_ts.write_text(_SYNTHETIC_NUMERUS, encoding="utf-8")
    try:
        compiler._compile_with_custom(str(plural_ts), str(tmp / "numerus.qm"))
        d["numerus_outcome"] = "written"
    except ValueError as exc:
        d["numerus_outcome"] = f"refused: {exc}"
    flog(f"i18n_fallback_compiler_valid run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, ensure_ascii=True)[:2000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    n = d.get("n_checkable", 0)
    return [
        ("catalogue_has_checkable_messages",
         n >= 100,
         f"messages dont la traduction differe de la source={n} ; sans eux un "
         f".qm mort passerait pour bon"),
        ("fallback_qm_resolves_every_message",
         d.get("fallback_loaded") and n > 0 and d.get("fallback_resolved") == n,
         f"charge={d.get('fallback_loaded')} resolus={d.get('fallback_resolved')}/{n} "
         f"exemples={d.get('fallback_failures')} ; l'interface anglaise "
         f"s'afficherait en francais"),
        ("fallback_count_is_what_the_qm_gives",
         d.get("fallback_reported") is not None
         and d.get("fallback_resolved") == n and d.get("fallback_reported", 0) >= n,
         f"annonces={d.get('fallback_reported')} resolus={d.get('fallback_resolved')}"
         f"/{n} ; le script annoncerait des messages qu'aucun utilisateur ne voit"),
        ("entry_point_without_lrelease_resolves",
         d.get("entry_loaded") and n > 0 and d.get("entry_resolved") == n,
         f"compile_ts_to_qm sans lrelease : charge={d.get('entry_loaded')} "
         f"annonces={d.get('entry_reported')} resolus={d.get('entry_resolved')}/{n} "
         f"exemples={d.get('entry_failures')}"),
        ("fallback_honours_disambiguation_comment",
         d.get("comment_got") == ["Open file", "Open"],
         f"obtenu={d.get('comment_got')} attendu=['Open file', 'Open'] ; deux "
         f"sens d'une meme source recevraient la meme traduction ou aucune"),
        ("fallback_refuses_plural_message",
         str(d.get("numerus_outcome", "")).startswith("refused"),
         f"issue={d.get('numerus_outcome')!r} ; un message au pluriel ecrit sans "
         f"regles de pluriel s'afficherait vide ou faux, sans erreur"),
        assert_log_contains(
            ctx.records, rf"i18n_fallback_compiler_valid.*trace_id={ctx.trace_id}",
            name="trace_id_propagated", min_count=1),
    ]


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
