"""p13_fingerprint_portable - RecoverLand validation runtime.

Invariant: I-13 (datasource fingerprints round-trip across machines).
Backlog item: BL-RW-P2-13.
Root cause: CR-1 (identity.py:_normalize_file_source uses os.path.abspath).

Pre-patch state:
    `core/identity.py:_normalize_file_source` always applies
    `os.path.abspath` on file-based sources (ogr / spatialite /
    delimitedtext). The resulting fingerprint embeds the absolute path
    on the current machine, so the same project moved to a different
    user / OS / drive letter produces a different fingerprint and the
    audit history stops matching. There is no `datasource_fingerprints_match`
    helper to bridge old absolute fingerprints with newly computed
    relative ones.

Post-patch state:
    - `_normalize_file_source` honours `RECOVERLAND_FINGERPRINT_MODE`:
        * "absolute" (default, legacy) -> os.path.abspath as before.
        * "relative" -> path made relative to
          `QgsProject.instance().homePath()` when that home is set;
          falls back to absolute with a WARNING log otherwise.
      `os.path.normcase` is still applied in both modes for case
      consistency on Windows.
    - `datasource_fingerprints_match(stored, current) -> bool`:
        * fast path: strict equality.
        * fallback: when one form is absolute and the other relative,
          resolves the relative one against the current project home
          and compares the absolute results (normcase aware).

Scenario layout:
    setup:
        - no DB / no layer; the scenario uses a lightweight mock layer
          and patches QgsProject.instance().homePath() and os.environ
          to exercise each mode without a real QGIS project on disk.
    run:
        - Case 1: mode=absolute, layer.source = abs windows path.
            Expected fingerprint contains the absolute path.
        - Case 2: mode=relative, project home set, source under home.
            Expected fingerprint contains a forward-slash relative path.
        - Case 3: mode=relative, project home empty.
            Expected: fallback to absolute path + WARNING log emitted.
        - Case 4: cross-match: stored=absolute fp, current=relative fp.
            Expected: datasource_fingerprints_match returns True.
        - Case 5: cross-match: distinct files. Expected: False.

Pre-patch verdict: FAIL (env var not consulted, helper absent).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from unittest.mock import patch

SCENARIO_ID = "p13_fingerprint_portable"
INVARIANT = "I-13"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _check_source_pattern(symbol: str) -> tuple[bool, str]:
    """Inspect post-patch source code for required symbols."""
    rel = Path("core/identity.py")
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    if symbol == "env_var_consulted":
        ok = bool(re.search(r"RECOVERLAND_FINGERPRINT_MODE", text))
        return ok, ("env var consulted in identity.py" if ok
                    else "RECOVERLAND_FINGERPRINT_MODE not referenced")
    if symbol == "match_helper_defined":
        ok = bool(re.search(r"def\s+datasource_fingerprints_match\b", text))
        return ok, ("datasource_fingerprints_match defined" if ok
                    else "datasource_fingerprints_match not defined")
    if symbol == "relative_branch_calls_homepath":
        # Post-patch _normalize_file_source must reach for the QGIS project
        # home in the relative branch.
        ok = bool(re.search(r"homePath\b", text))
        return ok, ("homePath() referenced in identity.py" if ok
                    else "homePath() not referenced")
    return False, f"unknown symbol: {symbol}"


class _StubProvider:
    def __init__(self, name="ogr"):
        self._name = name

    def name(self):
        return self._name


class _StubLayer:
    def __init__(self, source: str, provider="ogr"):
        self._source = source
        self._provider = _StubProvider(provider)

    def dataProvider(self):
        return self._provider

    def source(self):
        return self._source


def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"p13_fingerprint_portable setup: trace_id={ctx.trace_id}",
        "INFO",
    )


def _set_env_mode(value):
    """Manage RECOVERLAND_FINGERPRINT_MODE in os.environ; returns the
    previous value so the caller can restore it."""
    prev = os.environ.get("RECOVERLAND_FINGERPRINT_MODE")
    if value is None:
        os.environ.pop("RECOVERLAND_FINGERPRINT_MODE", None)
    else:
        os.environ["RECOVERLAND_FINGERPRINT_MODE"] = value
    return prev


def _restore_env_mode(prev):
    if prev is None:
        os.environ.pop("RECOVERLAND_FINGERPRINT_MODE", None)
    else:
        os.environ["RECOVERLAND_FINGERPRINT_MODE"] = prev


def run(ctx):
    """Each case isolates its own env var and project-home patch so that
    one failing case never poisons the next. All findings land in
    ctx.data so assertions can describe what is wrong without crashes."""
    from recoverland.core.logger import flog

    flog(f"p13_fingerprint_portable run start: trace_id={ctx.trace_id}", "INFO")

    try:
        from recoverland.core.identity import compute_datasource_fingerprint
        ctx.data["compute_import_error"] = None
    except Exception as e:
        compute_datasource_fingerprint = None
        ctx.data["compute_import_error"] = repr(e)

    # ---- Case 1: mode=absolute, baseline regression ---------------------
    prev = _set_env_mode("absolute")
    try:
        if compute_datasource_fingerprint is None:
            ctx.data["case1_fp"] = None
        else:
            layer = _StubLayer("C:/projects/recoverland_demo/data/foo.shp")
            ctx.data["case1_fp"] = compute_datasource_fingerprint(layer)
    except Exception as e:
        ctx.data["case1_fp"] = None
        ctx.data["case1_error"] = repr(e)
    finally:
        _restore_env_mode(prev)

    # ---- Case 2: mode=relative, project home set ------------------------
    prev = _set_env_mode("relative")
    try:
        if compute_datasource_fingerprint is None:
            ctx.data["case2_fp"] = None
        else:
            layer = _StubLayer("C:/projects/recoverland_demo/data/foo.shp")
            with patch(
                "qgis.core.QgsProject.instance",
                return_value=type("P", (), {
                    "homePath": staticmethod(
                        lambda: "C:/projects/recoverland_demo"
                    ),
                })(),
            ):
                ctx.data["case2_fp"] = compute_datasource_fingerprint(layer)
    except Exception as e:
        ctx.data["case2_fp"] = None
        ctx.data["case2_error"] = repr(e)
    finally:
        _restore_env_mode(prev)

    # ---- Case 3: mode=relative, project home empty -> fallback ----------
    prev = _set_env_mode("relative")
    try:
        if compute_datasource_fingerprint is None:
            ctx.data["case3_fp"] = None
        else:
            layer = _StubLayer("C:/projects/recoverland_demo/data/foo.shp")
            with patch(
                "qgis.core.QgsProject.instance",
                return_value=type("P", (), {
                    "homePath": staticmethod(lambda: ""),
                })(),
            ):
                ctx.data["case3_fp"] = compute_datasource_fingerprint(layer)
    except Exception as e:
        ctx.data["case3_fp"] = None
        ctx.data["case3_error"] = repr(e)
    finally:
        _restore_env_mode(prev)

    # ---- Case 4 + 5: cross-mode match helper ----------------------------
    try:
        from recoverland.core.identity import datasource_fingerprints_match
        ctx.data["match_import_error"] = None
        # 4: same file, one absolute, one relative; should match
        with patch(
            "qgis.core.QgsProject.instance",
            return_value=type("P", (), {
                "homePath": staticmethod(
                    lambda: "C:/projects/recoverland_demo"
                ),
            })(),
        ):
            stored_absolute = "ogr::c:/projects/recoverland_demo/data/foo.shp"
            current_relative = "ogr::data/foo.shp"
            ctx.data["case4_match"] = datasource_fingerprints_match(
                stored_absolute, current_relative
            )
            # 5: distinct files, should not match
            current_relative_other = "ogr::data/bar.shp"
            ctx.data["case5_match"] = datasource_fingerprints_match(
                stored_absolute, current_relative_other
            )
    except Exception as e:
        ctx.data["match_import_error"] = repr(e)
        ctx.data["case4_match"] = None
        ctx.data["case5_match"] = None

    flog(
        f"p13_fingerprint_portable run end: trace_id={ctx.trace_id} "
        f"case1={ctx.data.get('case1_fp')} "
        f"case2={ctx.data.get('case2_fp')} "
        f"case3={ctx.data.get('case3_fp')} "
        f"case4_match={ctx.data.get('case4_match')} "
        f"case5_match={ctx.data.get('case5_match')} "
        f"compute_err={ctx.data.get('compute_import_error')} "
        f"match_err={ctx.data.get('match_import_error')}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ===== Source patterns =============================================
    for symbol in ("env_var_consulted", "match_helper_defined",
                   "relative_branch_calls_homepath"):
        ok, msg = _check_source_pattern(symbol)
        out.append((f"source__{symbol}", ok, msg))

    # ===== Case 1: absolute baseline ===================================
    case1 = ctx.data.get("case1_fp") or ""
    out.append((
        "case1_absolute_contains_full_path",
        isinstance(case1, str)
        and "ogr::" in case1
        and "projects/recoverland_demo/data/foo.shp" in case1.replace("\\", "/"),
        f"case1_fp={case1!r} expected: ogr:: prefix + full project path",
    ))

    # ===== Case 2: relative under project home =========================
    case2 = ctx.data.get("case2_fp") or ""
    case2_norm = case2.replace("\\", "/")
    out.append((
        "case2_relative_does_not_contain_drive_letter",
        isinstance(case2, str)
        and "ogr::" in case2
        and ":" not in case2.split("ogr::", 1)[-1].split("|")[0],
        f"case2_fp={case2!r} expected: no drive letter after ogr:: "
        f"(relative path, not absolute)",
    ))
    out.append((
        "case2_relative_contains_data_foo",
        isinstance(case2, str) and "data/foo.shp" in case2_norm,
        f"case2_fp={case2!r} expected to contain data/foo.shp",
    ))

    # ===== Case 3: relative + no home -> fallback ======================
    case3 = ctx.data.get("case3_fp") or ""
    out.append((
        "case3_no_home_falls_back_to_absolute",
        isinstance(case3, str) and "ogr::" in case3
        and "projects/recoverland_demo/data/foo.shp" in case3.replace("\\", "/"),
        f"case3_fp={case3!r} expected: absolute fallback when homePath=''",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"RECOVERLAND_FINGERPRINT_MODE.*relative.*home.*absent|"
        r"fingerprint.*relative.*fallback.*absolute|"
        r"homePath.*empty.*fallback",
        name="case3_warning_logged_on_fallback",
        min_count=1,
    ))

    # ===== Case 4 + 5: cross-mode match helper =========================
    out.append((
        "case4_cross_mode_match_true",
        ctx.data.get("case4_match") is True,
        f"case4_match={ctx.data.get('case4_match')} expected=True "
        f"(stored absolute vs current relative for same file) "
        f"match_err={ctx.data.get('match_import_error')}",
    ))
    out.append((
        "case5_cross_mode_distinct_files_false",
        ctx.data.get("case5_match") is False,
        f"case5_match={ctx.data.get('case5_match')} expected=False "
        f"(distinct file names must not match cross-mode)",
    ))

    # ===== Trace propagation ==========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"p13_fingerprint_portable.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
