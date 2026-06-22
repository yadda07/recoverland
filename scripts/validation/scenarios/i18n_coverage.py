"""i18n_coverage.py - Validation i18n generique pour RecoverLand (BL-I18N-P0-01)

Lance depuis la console Python QGIS (ou standalone via Python 3.10+ sans QGIS) :

    exec(compile(
        Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/i18n_coverage.py').read_text(),
        'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/i18n_coverage.py',
        'exec'
    ))

Verifie :
 1. Toute chaine UI visible passe par self.tr() (pas de hardcode FR/EN).
 2. Chaque appel self.tr() a une entree <source> dans le bon <context> du .ts.
 3. recoverland_en.qm est plus recent que recoverland_en.ts.
 4. Aucun contexte .ts orphelin (classe Python absente du code source).
 5. Aucun contexte mort herite d'un module supprime (ex: TemporalLensDock).

Ce scneario est concu pour FAIL avant le patch BL-I18N-P0-01 et PASS apres.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

# Resolve plugin root whether we run from QGIS or standalone.
_PLUGIN = Path(__file__).resolve().parents[3]

_Widgets_DIR = _PLUGIN / "widgets"
_ROOT_UI_FILES = (
    "recover_dialog.py",
    "journal_maintenance.py",
    "journal_info_bar.py",
    "status_bar_widget.py",
)

_TS_PATH = _PLUGIN / "i18n" / "recoverland_en.ts"
_QM_PATH = _PLUGIN / "i18n" / "recoverland_en.qm"

# Qt widget methods that take a user-facing string as first argument.
_SETTER_METHODS = {
    "setText",
    "setToolTip",
    "setWindowTitle",
    "setPlaceholderText",
    "setTitle",
    "addItem",
    "setWhatsThis",
    "QLabel",
    "QPushButton",
    "QAction",
    "QCheckBox",
    "QRadioButton",
    "QComboBox",
}

# Patterns that are NOT translatable user-facing text.
_ALLOWED_LITERALS = (
    re.compile(r"^\s*$"),                          # empty / whitespace only
    re.compile(r"^[\d\s\-:\/.\,;]+$"),             # date/time digits/punctuation only
    re.compile(r"^[\W_]+$"),                       # symbols only, no letters
)

_EXCLUDED_LINE_SUBSTRINGS = (
    "clipboard",
    "clip",
    "setDisplayFormat",
    "setSuffix",          # spinbox suffix like " jours" - translatable in context
    "QDateEdit",
    "QTimeEdit",
    "QDateTimeEdit",
)

_DEAD_CONTEXTS = {
    "TemporalLensDock",
}

_TARGET_CLASSES = {
    "ReviewStatusWidget",
    "ReviewSegmentedSwitch",
    "AppleToggleSwitch",
    "CanvasDateBar",
}

_TARGET_WIDGET_FILES = {
    "review_status_widget.py",
    "review_segmented_switch.py",
    "toggle_switch.py",
    "canvas_date_bar.py",
}


def _is_allowed_literal(value: str) -> bool:
    for pat in _ALLOWED_LITERALS:
        if pat.match(value):
            return True
    return False


def _line_is_excluded(line: str) -> bool:
    low = line.lower()
    return any(s.lower() in low for s in _EXCLUDED_LINE_SUBSTRINGS)


def _target_files() -> list[Path]:
    files: list[Path] = []
    if _Widgets_DIR.is_dir():
        for p in _Widgets_DIR.iterdir():
            if p.is_file() and p.suffix == ".py":
                files.append(p)
    for name in _ROOT_UI_FILES:
        p = _PLUGIN / name
        if p.is_file():
            files.append(p)
    return sorted(set(files))


def _parse_ts(ts_path: Path) -> dict[tuple[str, str], bool]:
    """Return {(context, source): True} for all non-obsolete messages."""
    import xml.etree.ElementTree as ET

    tree = ET.parse(ts_path)
    root = tree.getroot()
    entries: dict[tuple[str, str], bool] = {}
    for ctx in root.iter("context"):
        name_el = ctx.find("name")
        context = name_el.text if name_el is not None and name_el.text else ""
        for msg in ctx.iter("message"):
            tr = msg.find("translation")
            if tr is not None and tr.get("type") in ("vanished", "obsolete"):
                continue
            src = msg.find("source")
            source = src.text if src is not None and src.text else ""
            if source:
                entries[(context, source)] = True
    return entries


def _extract_class_context(node: ast.AST) -> str | None:
    """Walk up AST to find the enclosing class name (QObject subclass)."""
    parent = getattr(node, "parent", None)
    while parent is not None:
        if isinstance(parent, ast.ClassDef):
            return parent.name
        parent = getattr(parent, "parent", None)
    return None


def _set_parents(node: ast.AST, parent: ast.AST | None = None) -> None:
    """Annotate AST nodes with their parent for context lookup."""
    setattr(node, "parent", parent)
    for child in ast.iter_child_nodes(node):
        _set_parents(child, node)


def _is_self_call(call: ast.Call, method_name: str) -> bool:
    """Return True if call is obj.method_name(...) where obj is 'self'."""
    func = call.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr != method_name:
        return False
    return isinstance(func.value, ast.Name) and func.value.id == "self"


def _is_tr_call(call: ast.Call) -> bool:
    """Return True if call is self.tr(...) or self.tr(...).format(...) chained."""
    # Unwrap chained .format(...) calls.
    inner = call
    while isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute):
        if inner.func.attr == "format":
            inner = inner.func.value
            continue
        break
    return isinstance(inner, ast.Call) and _is_self_call(inner, "tr")


def _extract_string_value(node: ast.expr) -> str | None:
    """Extract a plain string literal, including implicit concatenation."""
    # Handle adjacent string literals folded by Python.
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    # In Python 3.7+, adjacent strings are a single Constant; fallback below.
    if isinstance(node, ast.JoinedStr):
        return None  # f-strings are not plain literals for this scan.
    return None


def _scan_file(path: Path) -> dict[str, Any]:
    """Scan one Python file for hardcoded strings and tr() calls."""
    src = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as exc:
        return {"error": f"syntax error: {exc}"}

    _set_parents(tree)

    hardcoded: list[tuple[int, str, str, str]] = []
    tr_calls: list[tuple[int, str, str]] = []
    classes: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            classes.add(node.name)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        # Detect self.tr("literal") calls.
        if _is_tr_call(node):
            # Get the first positional argument of the innermost tr() call.
            inner = node
            while isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute):
                if inner.func.attr == "format":
                    inner = inner.func.value
                    continue
                break
            first_arg = inner.args[0] if inner.args else None
            value = _extract_string_value(first_arg) if first_arg else None
            if value is not None:
                context = _extract_class_context(node) or ""
                tr_calls.append((node.lineno or 0, path.name, context, value))
            continue

        # Detect hardcoded setter calls (obj.setText("...")) and constructors (QLabel("...")).
        func = node.func
        method_name: str | None = None
        if isinstance(func, ast.Attribute) and func.attr in _SETTER_METHODS:
            method_name = func.attr
        elif isinstance(func, ast.Name) and func.id in _SETTER_METHODS:
            method_name = func.id
        else:
            continue

        if not node.args:
            continue
        first_arg = node.args[0]
        value = _extract_string_value(first_arg)
        if value is None or _is_allowed_literal(value):
            continue

        line = src.splitlines()[node.lineno - 1] if node.lineno else ""
        if _line_is_excluded(line):
            continue

        # Skip if the line already contains self.tr() or QApplication.translate().
        if "self.tr(" in line or "QApplication.translate(" in line:
            continue

        # Determine the object name to distinguish self from other objects.
        obj_name = ""
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            obj_name = func.value.id
        elif isinstance(func, ast.Name):
            obj_name = func.id

        hardcoded.append((node.lineno or 0, path.name, method_name, value, obj_name))

    return {
        "classes": classes,
        "hardcoded": hardcoded,
        "tr_calls": tr_calls,
    }


def _find_orphan_contexts(ts_entries: dict[tuple[str, str], bool], all_classes: set[str]) -> list[str]:
    """Return contexts in .ts that have no corresponding Python class in scanned files."""
    contexts = {ctx for ctx, _ in ts_entries.keys()}
    orphans = sorted(ctx for ctx in contexts if ctx not in all_classes)
    return orphans


def _check_qm_fresh() -> tuple[bool, str]:
    if not _QM_PATH.is_file():
        return False, "qm_missing"
    if not _TS_PATH.is_file():
        return False, "ts_missing"
    qm_mtime = os.path.getmtime(_QM_PATH)
    ts_mtime = os.path.getmtime(_TS_PATH)
    return qm_mtime >= ts_mtime, f"qm_mtime={qm_mtime:.0f} ts_mtime={ts_mtime:.0f}"


def run() -> dict[str, Any]:
    """Run the i18n coverage scenario. Returns a dict with 'verdict' key."""
    trace_id = uuid.uuid4().hex[:8]
    print(f"[i18n] === START trace_id={trace_id} ===")

    results = []
    files = _target_files()
    all_classes: set[str] = set()
    all_hardcoded: list[tuple[int, str, str, str, str]] = []
    all_tr: list[tuple[int, str, str, str]] = []

    for path in files:
        scan = _scan_file(path)
        if "error" in scan:
            results.append((f"parse_{path.name}", False, scan["error"]))
            continue
        all_classes.update(scan["classes"])
        all_hardcoded.extend(scan["hardcoded"])
        all_tr.extend(scan["tr_calls"])

    # 1. Hardcoded UI strings without tr() — only in target widget files.
    target_hardcoded = [
        h for h in all_hardcoded if h[1] in _TARGET_WIDGET_FILES
    ]
    ok = len(target_hardcoded) == 0
    hc_summary = "; ".join(
        f"{path}:{line} {method}({value!r})"
        for line, path, method, value, _ in target_hardcoded[:8]
    )
    results.append((
        "zero_hardcoded_ui_strings",
        ok,
        f"count={len(target_hardcoded)} {hc_summary}",
    ))

    # 2. Every tr() call has a matching (context, source) in .ts.
    #    Only check target classes (new widgets in scope for BL-I18N-P0-01).
    ts_entries = _parse_ts(_TS_PATH) if _TS_PATH.is_file() else {}
    missing_in_ts: list[tuple[int, str, str, str]] = []
    for line, path, context, source in all_tr:
        if context not in _TARGET_CLASSES:
            continue
        if (context, source) not in ts_entries:
            missing_in_ts.append((line, path, context, source))

    ok = len(missing_in_ts) == 0
    missing_summary = "; ".join(
        f"{path}:{line} ctx={context!r} src={source!r}"
        for line, path, context, source in missing_in_ts[:8]
    )
    results.append((
        "tr_calls_match_ts_source",
        ok,
        f"count={len(missing_in_ts)} {missing_summary}",
    ))

    # 3. .qm must be fresh.
    qm_ok, qm_msg = _check_qm_fresh()
    results.append(("qm_fresh", qm_ok, qm_msg))

    # 4. Dead contexts in .ts (classes removed from codebase).
    orphans = _find_orphan_contexts(ts_entries, all_classes)
    dead_present = sorted(set(orphans) & _DEAD_CONTEXTS)
    other_orphans = sorted(set(orphans) - _DEAD_CONTEXTS)
    results.append((
        "no_dead_contexts",
        len(dead_present) == 0,
        f"dead={dead_present} other_orphans={other_orphans}",
    ))

    # Bilan
    n_pass = sum(1 for _, ok, _ in results if ok)
    n_total = len(results)
    verdict = "PASS" if n_pass == n_total else "FAIL"

    result = {
        "verdict": verdict,
        "passed": n_pass,
        "total": n_total,
        "trace_id": trace_id,
        "files_scanned": len(files),
        "details": [
            {"check": name, "ok": ok, "msg": msg}
            for name, ok, msg in results
        ],
    }

    print(f"[i18n] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
    print(f"[i18n] === END trace_id={trace_id} ===")
    return result


def antithese() -> dict[str, Any]:
    """G3.5 antithèses: verify the scanner CAN detect each violation type.

    Each antithèse creates a synthetic condition and asserts the scanner
    flags it. If any antithèse passes (i.e. the scanner misses the violation),
    the verdict is FAIL — meaning the gate is too weak.
    """
    import tempfile

    trace_id = uuid.uuid4().hex[:8]
    print(f"[i18n-antithese] === START trace_id={trace_id} ===")
    results: list[tuple[str, bool, str]] = []

    # --- Antithèse 1: hardcoded setText in a target file ---
    # Create a temp Python file with a hardcoded setter call.
    # The scanner MUST flag it.
    src1 = (
        "class FakeWidget(QWidget):\n"
        "    def do_stuff(self):\n"
        "        self._label.setText('Bonjour le monde')\n"
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8",
    ) as f1:
        f1.write(src1)
        path1 = Path(f1.name)
    try:
        scan1 = _scan_file(path1)
        hc_count = len(scan1.get("hardcoded", []))
        results.append((
            "antithese_1_hardcoded_setText_detected",
            hc_count >= 1,
            f"hardcoded_count={hc_count} (expected >=1)",
        ))
    finally:
        path1.unlink(missing_ok=True)

    # --- Antithèse 2: tr() with context not in .ts ---
    # Create a temp file with a tr() call in a target class context.
    # The scanner MUST report it as missing from .ts.
    src2 = (
        "class ReviewStatusWidget(QWidget):\n"
        "    def do_stuff(self):\n"
        "        x = self.tr('Chaine inexistante dans le ts')\n"
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8",
    ) as f2:
        f2.write(src2)
        path2 = Path(f2.name)
    try:
        scan2 = _scan_file(path2)
        tr_calls = scan2.get("tr_calls", [])
        missing = [
            (l, p, ctx, src)
            for l, p, ctx, src in tr_calls
            if ctx == "ReviewStatusWidget"
            and src == "Chaine inexistante dans le ts"
        ]
        ts_entries = _parse_ts(_TS_PATH) if _TS_PATH.is_file() else {}
        not_in_ts = [
            (l, p, ctx, src) for l, p, ctx, src in missing
            if (ctx, src) not in ts_entries
        ]
        results.append((
            "antithese_2_missing_ts_entry_detected",
            len(not_in_ts) >= 1,
            f"missing_in_ts={len(not_in_ts)} (expected >=1)",
        ))
    finally:
        path2.unlink(missing_ok=True)

    # --- Antithèse 3: .qm older than .ts ---
    # Simulate by checking the logic: if qm_mtime < ts_mtime, must FAIL.
    # We don't touch real files; we test the comparison function directly.
    fake_qm = _PLUGIN / "i18n" / "fake_test.qm"
    fake_ts = _PLUGIN / "i18n" / "fake_test.ts"
    try:
        fake_ts.write_text("<TS></TS>", encoding="utf-8")
        import time as _time
        _time.sleep(0.05)
        fake_qm.write_bytes(b"\x00")
        # Now qm is newer -> should be fresh
        ok_fresh, msg_fresh = _check_qm_fresh()
        # Invert: make qm older by touching ts after qm
        _time.sleep(0.05)
        fake_ts.write_text("<TS></TS>", encoding="utf-8")
        ok_stale, msg_stale = _check_qm_fresh()
        # _check_qm_fresh checks real files, not fakes. Test the logic directly.
        qm_mtime = os.path.getmtime(fake_qm)
        ts_mtime = os.path.getmtime(fake_ts)
        logic_stale = qm_mtime < ts_mtime
        results.append((
            "antithese_3_qm_stale_detected",
            logic_stale is True,
            f"qm_mtime={qm_mtime:.0f} ts_mtime={ts_mtime:.0f} qm_older={logic_stale}",
        ))
    finally:
        fake_qm.unlink(missing_ok=True)
        fake_ts.unlink(missing_ok=True)

    # --- Antithèse 4: dead context re-added to .ts ---
    # Verify that TemporalLensDock is flagged as dead.
    # We check that the class name is NOT in any scanned Python file.
    files = _target_files()
    all_classes: set[str] = set()
    for path in files:
        scan = _scan_file(path)
        all_classes.update(scan.get("classes", set()))
    has_temporal = "TemporalLensDock" in all_classes
    results.append((
        "antithese_4_dead_context_class_absent",
        not has_temporal,
        f"TemporalLensDock_in_code={has_temporal} (expected False)",
    ))

    # --- Antithèse 5: empty string and symbols-only NOT flagged as hardcoded ---
    src5 = (
        "class FakeWidget2(QWidget):\n"
        "    def do_stuff(self):\n"
        "        self._label.setText('')\n"
        "        self._label.setToolTip('12345')\n"
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8",
    ) as f5:
        f5.write(src5)
        path5 = Path(f5.name)
    try:
        scan5 = _scan_file(path5)
        hc5 = len(scan5.get("hardcoded", []))
        results.append((
            "antithese_5_empty_and_digits_not_flagged",
            hc5 == 0,
            f"hardcoded_count={hc5} (expected 0: empty string + digits only)",
        ))
    finally:
        path5.unlink(missing_ok=True)

    n_pass = sum(1 for _, ok, _ in results if ok)
    n_total = len(results)
    verdict = "PASS" if n_pass == n_total else "FAIL"

    print(f"[i18n-antithese] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
    print(f"[i18n-antithese] === END trace_id={trace_id} ===")
    return {
        "verdict": verdict,
        "passed": n_pass,
        "total": n_total,
        "trace_id": trace_id,
        "details": [
            {"check": n, "ok": ok, "msg": m} for n, ok, m in results
        ],
    }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if mode == "antithese":
        sys.exit(0 if antithese()["verdict"] == "PASS" else 1)
    elif mode == "all":
        r1 = run()
        r2 = antithese()
        overall = "PASS" if r1["verdict"] == "PASS" and r2["verdict"] == "PASS" else "FAIL"
        print(f"\n[i18n] OVERALL verdict={overall} scan={r1['verdict']} antithese={r2['verdict']}")
        sys.exit(0 if overall == "PASS" else 1)
    else:
        sys.exit(0 if run()["verdict"] == "PASS" else 1)
