"""Scenario runtime : contrat de compatibilite QGIS 3.40 (Qt5) -> 4.2 (Qt6).

Doit etre execute dans un environnement QGIS (console Python du logiciel ou
runner headless), sur CHAQUE version de la matrice CI. Hors-QGIS l'import
echoue : c'est attendu (charte : preuve par logs runtime QGIS, pas de mocks).

Lancement (console QGIS) ::

    import recoverland.scripts.validation.scenarios.compat_cross_version as s
    s.run()

THESE validee : le plugin se charge et se comporte identiquement sur toute la
plage declaree. Les quatre axes verifies sont ceux qui ont deja casse :

A1  Arithmetique de drapeaux Qt/QGIS. `flags & ~flag` retourne un `int` nu sous
    PyQt6 et sip le refuse ; sous PyQt5 le wrapper `Flags` l'absorbe. C'est ce
    qui a tue silencieusement le report de legende de la 5.0.1 sur QGIS 4 :
    le TypeError etait avale par un `except Exception`. Le garde teste
    `compat.clear_flag` sur un VRAI QgsLayerTreeModel.

A2  Resolution du pont d'enums. Chaque membre public de QtCompat / QgisCompat
    doit resoudre vers une valeur non nulle. `_resolve_enum` retombe sur la
    forme courte quand le namespace scope manque : un membre a None signifie
    que les deux branches ont echoue et que le site d'appel recevra un None.

A3  Porte Qt6 du gestionnaire d'extensions. QGIS 3.x compile en Qt6 refuse au
    chargement toute extension dont metadata.txt ne declare pas
    supportsQt6=TRUE/YES (pyplugin_installer/installer_data.py). QGIS 4.x a
    retire le controle. Le garde rejoue la porte exacte sur la liaison courante.

A4  Plancher Python. Les unions PEP 604 (`str | None`) en annotation sont
    evaluees a la definition et levent TypeError sur Python 3.9, que certains
    paquets Linux de QGIS 3.40 embarquent. Balayage AST de tout l'arbre suivi.

Le verdict (PASS/FAIL par assertion) est imprime ET ecrit dans
recoverland_debug.log via flog, prefixe par le trace_id du scenario.
"""
from __future__ import annotations

import ast
import configparser
import os
import sys
import uuid

from qgis.core import Qgis, QgsLayerTree, QgsLayerTreeModel
from qgis.PyQt.QtCore import PYQT_VERSION_STR, QT_VERSION_STR

from recoverland import compat
from recoverland.compat import QgisCompat, QtCompat, clear_flag
from recoverland.core.logger import flog

_PLUGIN_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# _vendor est du code tiers (defusedxml) : il n'est pas soumis au plancher
# Python du plugin. build/ et dist/ sont des artefacts de packaging.
_SKIP_DIRS = frozenset({"_vendor", "build", "dist", "__pycache__", ".git"})


class _Results(list):
    """Collect (name, passed, brutal, detail) tuples."""

    trace_id = ""

    def check(self, name: str, passed: bool, detail: str = "", brutal: bool = False) -> None:
        self.append((name, passed, brutal, detail))
        status = "PASS" if passed else "FAIL"
        line = f"[{self.trace_id}] compat: {name} {status} {detail}".rstrip()
        flog(line, "INFO" if passed else "ERROR")
        print(f"  {status:4s} {name} {detail}".rstrip())


# --------------------------------------------------------------------- #
# A1 : arithmetique de drapeaux sur un vrai modele                        #
# --------------------------------------------------------------------- #
def _check_flag_arithmetic(r: _Results) -> None:
    tree = QgsLayerTree()
    model = QgsLayerTreeModel(tree)
    original = model.flags()
    flag_ns = getattr(QgsLayerTreeModel, "Flag", QgsLayerTreeModel)
    show_legend = getattr(flag_ns, "ShowLegend", None)

    if show_legend is None:
        r.check("A1.enum_present", False, "QgsLayerTreeModel ShowLegend introuvable")
        return
    r.check("A1.enum_present", True, f"ShowLegend={int(show_legend)}")

    if not bool(int(original) & int(show_legend)):
        r.check("A1.precondition", False, "ShowLegend deja absent du modele neuf")
        return
    r.check("A1.precondition", True, f"flags={int(original)}")

    # Antithese brutale : la forme naive est exactement celle qui a casse.
    # On ne l'assert PAS comme devant lever -- on documente ce qu'elle produit,
    # puis on exige que clear_flag, elle, survive sur les deux liaisons.
    naive = original & ~show_legend
    r.check(
        "A1.naive_form_observed", True,
        f"(flags & ~flag) -> type={type(naive).__name__} int={int(naive)}",
    )

    try:
        cleared = clear_flag(original, show_legend)
        model.setFlags(cleared)
        ok = not bool(int(model.flags()) & int(show_legend))
        r.check("A1.clear_flag_accepted", ok,
                f"setFlags(clear_flag(...)) -> flags={int(model.flags())}", brutal=True)
    except Exception as exc:  # noqa: BLE001
        r.check("A1.clear_flag_accepted", False,
                f"{type(exc).__name__}: {exc}", brutal=True)
        return

    try:
        model.setFlags(original)
        back = bool(int(model.flags()) & int(show_legend))
        r.check("A1.restore_round_trip", back, f"flags={int(model.flags())}", brutal=True)
    except Exception as exc:  # noqa: BLE001
        r.check("A1.restore_round_trip", False, f"{type(exc).__name__}: {exc}", brutal=True)


# --------------------------------------------------------------------- #
# A2 : chaque membre du pont d'enums resout                               #
# --------------------------------------------------------------------- #
def _check_compat_members(r: _Results) -> None:
    unresolved = []
    total = 0
    for ns_name, ns in (("QtCompat", QtCompat), ("QgisCompat", QgisCompat)):
        for name in sorted(n for n in dir(ns) if not n.startswith("_")):
            total += 1
            if getattr(ns, name) is None:
                unresolved.append(f"{ns_name}.{name}")
    r.check("A2.all_members_resolve", not unresolved,
            f"{total - len(unresolved)}/{total} resolus"
            + (f" ; None: {', '.join(unresolved)}" if unresolved else ""),
            brutal=True)

    qv = compat.qgis_version_info()
    r.check("A2.version_parsed", qv.major > 0,
            f"qgis_version_info={tuple(qv)} QGIS_VERSION={Qgis.QGIS_VERSION}")

    expected_qt6 = int(PYQT_VERSION_STR.split(".")[0]) >= 6
    r.check("A2.is_qt6_agrees", compat.is_qt6() == expected_qt6,
            f"is_qt6()={compat.is_qt6()} PyQt={PYQT_VERSION_STR}")


# --------------------------------------------------------------------- #
# A3 : porte Qt6 du gestionnaire d'extensions                             #
# --------------------------------------------------------------------- #
def _check_qt6_gate(r: _Results) -> None:
    meta = os.path.join(_PLUGIN_ROOT, "metadata.txt")
    if not os.path.exists(meta):
        r.check("A3.metadata_found", False, meta)
        return
    parser = configparser.ConfigParser()
    parser.optionxform = str
    parser.read(meta, encoding="utf-8")
    raw = parser.get("general", "supportsQt6", fallback="")
    supports = raw.strip().upper() in ("TRUE", "YES")
    qt_major = int(QT_VERSION_STR.split(".")[0])
    refused = qt_major == 6 and not supports
    r.check("A3.qt6_gate_passes", not refused,
            f"supportsQt6={raw.strip()!r} qt_major={qt_major}", brutal=True)


# --------------------------------------------------------------------- #
# A4 : plancher Python 3.9                                                #
# --------------------------------------------------------------------- #
def _check_python_floor(r: _Results) -> None:
    # os.walk, pas `git ls-files` : git n'est pas sur le PATH de l'environnement
    # Python embarque par QGIS (Windows notamment), et le scenario doit tourner
    # dans la console du logiciel comme dans le conteneur CI.
    files = []
    for root, dirs, names in os.walk(_PLUGIN_ROOT):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in names:
            if name.endswith(".py"):
                files.append(os.path.relpath(os.path.join(root, name), _PLUGIN_ROOT))
    if not files:
        r.check("A4.file_listing", False, f"aucun .py sous {_PLUGIN_ROOT}")
        return

    offenders = []
    for rel in files:
        path = os.path.join(_PLUGIN_ROOT, rel)
        try:
            tree = ast.parse(open(path, encoding="utf-8").read())
        except Exception:  # noqa: BLE001
            continue
        if any(
            isinstance(n, ast.ImportFrom) and n.module == "__future__"
            and any(a.name == "annotations" for a in n.names)
            for n in tree.body
        ):
            continue
        for node in ast.walk(tree):
            anns = []
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                anns = [a.annotation for a in node.args.args] + [node.returns]
            elif isinstance(node, ast.AnnAssign):
                anns = [node.annotation]
            for ann in anns:
                if ann is None:
                    continue
                if any(isinstance(s, ast.BinOp) and isinstance(s.op, ast.BitOr)
                       for s in ast.walk(ann)):
                    offenders.append(f"{rel}:{node.lineno}")
                    break

    r.check("A4.no_evaluated_pep604", not offenders,
            f"{len(files)} fichiers balayes"
            + (f" ; PEP604 evalue: {', '.join(sorted(set(offenders))[:10])}"
               if offenders else ""),
            brutal=True)


def run() -> dict:
    """Execute the cross-version compatibility contract. Returns a verdict dict."""
    results = _Results()
    results.trace_id = uuid.uuid4().hex[:8]
    flog(f"[{results.trace_id}] compat: scenario_start", "INFO")
    print(f"=== compat_cross_version trace_id={results.trace_id} ===")
    print(f"    QGIS {Qgis.QGIS_VERSION} | Qt {QT_VERSION_STR} | "
          f"PyQt {PYQT_VERSION_STR} | Python {sys.version.split()[0]}")

    for phase in (_check_flag_arithmetic, _check_compat_members,
                  _check_qt6_gate, _check_python_floor):
        try:
            phase(results)
        except Exception as exc:  # noqa: BLE001
            results.check(f"{phase.__name__}.crashed", False,
                          f"{type(exc).__name__}: {exc}")

    n_total = len(results)
    n_pass = sum(1 for _, p, _, _ in results if p)
    n_fail = n_total - n_pass
    n_brutal = sum(1 for _, _, b, _ in results if b)
    n_brutal_pass = sum(1 for _, p, b, _ in results if b and p)
    verdict = "PASS" if n_fail == 0 else "FAIL"
    synthese = (
        f"SYNTHESE: {verdict} -- {n_pass}/{n_total} assertions, "
        f"dont {n_brutal_pass}/{n_brutal} antitheses brutales."
    )
    flog(f"[{results.trace_id}] compat: {synthese}",
         "INFO" if verdict == "PASS" else "ERROR")
    print(synthese)
    return {
        "verdict": verdict,
        "trace_id": results.trace_id,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "n_total": n_total,
        "failed": [name for name, p, _, _ in results if not p],
    }


if __name__ == "__main__":
    run()
