# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""maint_open_folder_long_path - RecoverLand validation runtime.

Invariant: I-JRN-01 (un projet a un journal, le meme a chaque ouverture)
et "rien de cache a l'utilisateur" : ce que la maintenance montre et ouvre
est le journal reel.

Le mecanisme
------------
Un journal dont le chemin depasse 250 caracteres est ouvert sous sa forme
etendue (deux barres obliques inverses, un point d'interrogation, une
barre oblique inverse, puis le chemin absolu : `core/long_paths.when_needed`).
Cette ecriture est celle des appels Win32, pas celle d'une adresse : le
bouton "Dossier" de la maintenance passait `dirname(path)` tel quel a
`QUrl.fromLocalFile`, qui en fait une adresse reseau vers un serveur nomme
"?" ; l'explorateur n'ouvre rien (ou autre chose), et l'echec n'etait
qu'une ligne de log. L'etiquette du chemin affichait elle aussi le prefixe
etendu, que l'utilisateur ne peut ni coller dans l'explorateur ni
reconnaitre.

Le scenario d'echec concret
---------------------------
Un projet range dans une arborescence profonde d'un serveur de fichiers.
L'utilisateur ouvre la maintenance pour retrouver son journal, clique
"Dossier" : rien ne s'ouvre, aucun message. Le chemin affiche commence par
des caracteres qu'il ne reconnait pas.

Ce que le scenario execute reellement
-------------------------------------
Deux vrais journaux (`initialize_schema`) sous leur forme etendue : l'un de
~256 caracteres (dossier sous la limite de 260), l'autre de ~300 (dossier
au-dela). Le vrai `JournalMaintenanceDialog`, avec un journal minimal qui
expose ce chemin, et `QDesktopServices.openUrl` remplace par un
enregistreur (le vrai ouvrirait l'explorateur). Pour chacun : l'adresse
passee a l'ouverture doit designer le dossier du journal (chemin simple,
sans hote) ; si l'ouverture echoue, l'utilisateur recoit un message qui
nomme le dossier ; l'etiquette montre le chemin simple.

Verdict attendu avant correctif : FAIL (adresse vers un hote "?", aucun
message en cas d'echec, prefixe etendu affiche).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "maint_open_folder_long_path"
INVARIANT = "I-JRN-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PFX = "\\\\?\\"


def _ext(path: str) -> str:
    p = os.path.abspath(path)
    return p if p.startswith(_PFX) else _PFX + p


def _plain(path: str) -> str:
    return path[len(_PFX):] if path and path.startswith(_PFX) else path


def _norm(path: str) -> str:
    return os.path.normcase(os.path.normpath(_plain(path or "")))


def _deep_journal(base: str, total: int) -> str:
    """A journal path of about *total* characters, created (extended form)."""
    name = os.path.join(".recoverland", "recoverland_audit.sqlite")
    deep = base
    while len(os.path.join(deep, "dossier_de_projet_au_nom_long", name)) <= total:
        deep = os.path.join(deep, "dossier_de_projet_au_nom_long")
    pad = total - len(os.path.join(deep, "x", name)) + 1
    if pad > 0:
        deep = os.path.join(deep, "d" * pad)
    journal = os.path.join(deep, name)
    os.makedirs(_ext(os.path.dirname(journal)), exist_ok=True)
    conn = sqlite3.connect(_ext(journal))
    try:
        from recoverland.core.sqlite_schema import initialize_schema
        initialize_schema(conn)
    finally:
        conn.close()
    return _ext(journal)


class _Journal:
    """What the dialog reads of the journal manager, on a real file."""

    def __init__(self, path: str):
        self.path = path
        self.is_open = True

    def create_read_connection(self):
        return sqlite3.connect(self.path)

    def get_connection(self):
        return sqlite3.connect(self.path)


class _Desktop:
    """Stands in for QDesktopServices: records, answers *result*."""

    def __init__(self):
        self.urls = []
        self.result = True

    def openUrl(self, url):  # noqa: N802 - Qt name
        self.urls.append(url)
        return self.result


def _case(journal_path: str, desktop: _Desktop, shown: list) -> dict:
    import recoverland.journal_maintenance as jm_mod

    out = {"journal": _plain(journal_path), "journal_len": len(_plain(journal_path)),
           "folder": _plain(os.path.dirname(journal_path)),
           "folder_len": len(_plain(os.path.dirname(journal_path)))}
    dlg = jm_mod.JournalMaintenanceDialog(_Journal(journal_path))
    try:
        out["label"] = dlg._path_label.text()
        desktop.result = True
        n = len(desktop.urls)
        dlg._open_journal_folder()
        urls = desktop.urls[n:]
        out["opened"] = len(urls)
        if urls:
            url = urls[-1]
            out["url"] = url.toString()
            out["url_valid"] = bool(url.isValid())
            out["url_host"] = url.host()
            out["url_local"] = url.toLocalFile()
        # The desktop cannot open it: the user must be told where it is.
        desktop.result = False
        m = len(shown)
        dlg._open_journal_folder()
        out["messages_on_failure"] = [text for _kind, text in shown[m:]]
    finally:
        dlg.close()
        dlg.deleteLater()
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.mkdtemp(prefix="rl_mofl_")
    ctx.data["tmp"] = tmp
    ctx.data["journals"] = [_deep_journal(tmp, 256), _deep_journal(tmp, 300)]
    flog(f"maint_open_folder_long_path setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from qgis.PyQt.QtWidgets import QMessageBox
    from recoverland.core.logger import flog
    import recoverland.journal_maintenance as jm_mod

    d = ctx.data
    desktop = _Desktop()
    shown = []
    saved_desktop = jm_mod.QDesktopServices
    saved_boxes = (QMessageBox.information, QMessageBox.warning,
                   QMessageBox.critical)
    QMessageBox.information = staticmethod(
        lambda *a, **k: shown.append(("info", str(a[2]) if len(a) > 2 else "")))
    QMessageBox.warning = staticmethod(
        lambda *a, **k: shown.append(("warn", str(a[2]) if len(a) > 2 else "")))
    QMessageBox.critical = staticmethod(
        lambda *a, **k: shown.append(("crit", str(a[2]) if len(a) > 2 else "")))
    jm_mod.QDesktopServices = desktop
    try:
        d["cases"] = [_case(p, desktop, shown) for p in d["journals"]]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        jm_mod.QDesktopServices = saved_desktop
        (QMessageBox.information, QMessageBox.warning,
         QMessageBox.critical) = saved_boxes
    flog(f"maint_open_folder_long_path run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp", "")
    if tmp:
        shutil.rmtree(_ext(tmp), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    cases = d.get("cases") or []
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_two_long_journals",
        len(cases) == 2 and cases[0]["journal_len"] > 250
        and cases[0]["folder_len"] < 260 and cases[1]["folder_len"] > 260,
        f"journaux={[(c.get('journal_len'), c.get('folder_len')) for c in cases]} "
        f"(attendu : un journal > 250 dont le dossier < 260, un dossier > 260){crash}",
    ))
    for i, c in enumerate(cases):
        out.append((
            f"case{i}_folder_url_names_the_journal_folder",
            c.get("opened") == 1 and c.get("url_valid") is True
            and not c.get("url_host")
            and _norm(c.get("url_local", "")) == _norm(c["folder"]),
            f"journal de {c['journal_len']} caracteres : adresse ouverte="
            f"{c.get('url')!r} hote={c.get('url_host')!r} fichier local="
            f"{c.get('url_local')!r} ; attendu le dossier {c['folder']!r}. "
            f"Sinon le bouton Dossier n'ouvre pas le dossier du journal",
        ))
        msgs = c.get("messages_on_failure") or []
        out.append((
            f"case{i}_open_failure_names_the_folder",
            any(_norm(c["folder"]) in _norm(m) or c["folder"] in m for m in msgs)
            and not any(_PFX in m for m in msgs),
            f"ouverture refusee par le bureau : messages={msgs} ; attendu un "
            f"message qui nomme {c['folder']!r} (sans prefixe etendu). Sinon "
            f"le clic ne fait rien et l'utilisateur ne sait pas ou chercher",
        ))
        out.append((
            f"case{i}_path_label_is_the_plain_path",
            c.get("label") == c["journal"],
            f"etiquette={c.get('label')!r} ; attendu {c['journal']!r} : le "
            f"prefixe etendu n'est pas un chemin que l'utilisateur peut coller",
        ))
    out.append(assert_log_contains(
        ctx.records, rf"maint_open_folder_long_path.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
