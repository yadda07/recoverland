"""Scenario IL-3 - Read-only journal connection (Time Lens invariant IL-I1).

Verifies that JournalManager exposes an open_readonly_connection() helper
which enforces TWO independent layers of write protection:

    1. URI mode=ro     - filesystem-level refusal (SQLITE_OPEN_READONLY).
    2. PRAGMA query_only=ON - session-level refusal (any DML rejected).

Any single layer of protection can be defeated by a confused caller
(URI mishandled, pragma reset). Two layers buy us defence-in-depth.

Cause racine: CR-IL-7 (cf. backlog). Without a dedicated read-only
connection, Lens would share JournalManager._conn and could observe
partial WAL state OR worst case, write through it.

Acceptance assertions (each one is also an antithesis):
    1. JournalManager has attribute `open_readonly_connection`.
    2. open_readonly_connection() returns a sqlite3.Connection.
    3. PRAGMA query_only returns 1 on the returned connection.
    4. INSERT INTO audit_event raises sqlite3.OperationalError.
    5. UPDATE audit_event raises sqlite3.OperationalError.
    6. DELETE FROM audit_event raises sqlite3.OperationalError.
    7. SELECT works (sanity: connection isn't useless).
    8. JournalManager._conn is NOT shared (separate connection object).

Initial expected verdict: FAIL (open_readonly_connection does not exist
yet on JournalManager).
Post-patch expected verdict: PASS.

This scenario runs without QGIS. Pure sqlite3 + tempfile.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "il3_readonly_isolation"
INVARIANT = "BL-IL-P0-03"
EXPECTED_SIGNATURE = r""

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_JOURNAL_MANAGER_PATH = _PLUGIN_ROOT / "core" / "journal_manager.py"


def _import_journal_manager_class():
    """Import JournalManager without booting the full plugin (no QGIS).

    Strategy: directly stub the two relative imports of journal_manager
    (`from .sqlite_schema import ...`, `from .logger import flog`) with
    minimal no-op replacements. Avoids dragging compat.py, qgis, PyQt.
    """
    import types

    # 1. Stub the `core` package skeleton.
    core_stub = sys.modules.get("core")
    if core_stub is None or not hasattr(core_stub, "__path__"):
        core_stub = types.ModuleType("core")
        core_stub.__path__ = [str(_PLUGIN_ROOT / "core")]  # type: ignore[attr-defined]
        sys.modules["core"] = core_stub

    # 2. Stub core.logger with flog = no-op print.
    logger_stub = types.ModuleType("core.logger")

    def _noop_flog(msg, level="INFO", **kwargs):  # noqa: ARG001 - test stub
        # Visible only when debugging the scenario itself.
        return None

    logger_stub.flog = _noop_flog
    sys.modules["core.logger"] = logger_stub

    # 3. Stub core.sqlite_schema with no-op DDL helpers.
    schema_stub = types.ModuleType("core.sqlite_schema")

    def _noop_initialize_schema(_conn):
        return None

    def _noop_apply_pragmas(_conn):
        return None

    def _noop_get_schema_version(_conn):
        return 99  # arbitrary "current" version; journal_manager only logs it

    schema_stub.initialize_schema = _noop_initialize_schema
    schema_stub.apply_pragmas = _noop_apply_pragmas
    schema_stub.get_schema_version = _noop_get_schema_version
    sys.modules["core.sqlite_schema"] = schema_stub

    # 4. Now safely load journal_manager.
    mod_name = "core.journal_manager"
    sys.modules.pop(mod_name, None)
    spec = importlib.util.spec_from_file_location(
        mod_name, str(_JOURNAL_MANAGER_PATH)
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot import journal_manager.py")
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "core"
    sys.modules[mod_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - we surface as scenario error
        raise RuntimeError(f"journal_manager import failed: {exc!r}") from exc
    return getattr(module, "JournalManager", None)


def _build_minimal_audit_db(path: Path) -> None:
    """Create a tiny SQLite file with a minimal audit_event-like schema."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE audit_event ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "created_at TEXT NOT NULL, "
            "operation_type TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO audit_event(created_at, operation_type) "
            "VALUES ('2026-05-16T00:00:00Z', 'INSERT')"
        )
        conn.commit()
    finally:
        conn.close()


def setup(ctx):
    # Build a tempfile-backed audit DB. Survives until run() cleans up.
    tmp_dir = Path(tempfile.mkdtemp(prefix="rl_il3_"))
    db_path = tmp_dir / "recoverland_audit.sqlite"
    _build_minimal_audit_db(db_path)
    ctx.data["tmp_dir"] = tmp_dir
    ctx.data["db_path"] = db_path

    try:
        ctx.data["JournalManager"] = _import_journal_manager_class()
        ctx.data["import_error"] = None
    except Exception as exc:  # noqa: BLE001
        ctx.data["JournalManager"] = None
        ctx.data["import_error"] = repr(exc)


def _safe_exec(conn, sql, params=()):
    """Try sql, return (ok_or_error_type, message). ok=True means it ran."""
    try:
        conn.execute(sql, params)
        conn.commit()
        return True, "executed without error"
    except sqlite3.OperationalError as exc:
        return False, f"OperationalError: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def run(ctx):
    """Open the journal via JournalManager and grab a read-only connection."""
    jm_class = ctx.data.get("JournalManager")
    if jm_class is None:
        ctx.data["jm_instance"] = None
        ctx.data["ro_conn"] = None
        return

    jm = jm_class()
    ctx.data["jm_instance"] = jm

    # We bypass open_for_project (it has lock-file logic) and set the path
    # directly. The scenario only exercises the read-side helper.
    jm._path = str(ctx.data["db_path"])  # noqa: SLF001 - test helper

    if not hasattr(jm, "open_readonly_connection"):
        ctx.data["ro_conn"] = None
        ctx.data["open_ro_error"] = "method open_readonly_connection missing"
        return

    try:
        ro_conn = jm.open_readonly_connection()
    except Exception as exc:  # noqa: BLE001
        ctx.data["ro_conn"] = None
        ctx.data["open_ro_error"] = repr(exc)
        return

    ctx.data["ro_conn"] = ro_conn
    ctx.data["open_ro_error"] = None


def assertions(ctx):
    results = []

    jm = ctx.data.get("jm_instance")
    ro_conn = ctx.data.get("ro_conn")
    import_error = ctx.data.get("import_error")

    if import_error is not None:
        results.append((
            "open_readonly_connection_exists", False,
            f"journal_manager import failed: {import_error}",
        ))
        for name in (
            "ro_conn_is_sqlite_connection",
            "ro_conn_has_query_only_pragma",
            "ro_conn_refuses_insert",
            "ro_conn_refuses_update",
            "ro_conn_refuses_delete",
            "ro_conn_accepts_select",
            "ro_conn_is_separate_from_writer",
        ):
            results.append((name, False, "skipped: import failed"))
        return results

    # 1. Method exists?
    has_method = jm is not None and hasattr(jm, "open_readonly_connection")
    results.append((
        "open_readonly_connection_exists", has_method,
        f"hasattr(JournalManager, 'open_readonly_connection') = {has_method}",
    ))

    if not has_method or ro_conn is None:
        open_error = ctx.data.get("open_ro_error", "<no error captured>")
        for name in (
            "ro_conn_is_sqlite_connection",
            "ro_conn_has_query_only_pragma",
            "ro_conn_refuses_insert",
            "ro_conn_refuses_update",
            "ro_conn_refuses_delete",
            "ro_conn_accepts_select",
            "ro_conn_is_separate_from_writer",
        ):
            results.append((name, False, f"skipped: {open_error}"))
        return results

    # 2. Type check
    is_conn = isinstance(ro_conn, sqlite3.Connection)
    results.append((
        "ro_conn_is_sqlite_connection", is_conn,
        f"isinstance result = {is_conn}, got {type(ro_conn).__name__}",
    ))

    # 3. PRAGMA query_only check
    try:
        cur = ro_conn.execute("PRAGMA query_only")
        row = cur.fetchone()
        query_only = row[0] if row else None
    except Exception as exc:  # noqa: BLE001
        query_only = f"<pragma error: {exc!r}>"
    results.append((
        "ro_conn_has_query_only_pragma", query_only == 1,
        f"PRAGMA query_only = {query_only!r} (expected 1)",
    ))

    # 4-6. DML refusal
    ok, msg = _safe_exec(
        ro_conn,
        "INSERT INTO audit_event(created_at, operation_type) "
        "VALUES('2026-05-17T00:00:00Z','DELETE')",
    )
    results.append((
        "ro_conn_refuses_insert", not ok,
        f"INSERT result: {msg}",
    ))

    ok, msg = _safe_exec(
        ro_conn,
        "UPDATE audit_event SET operation_type='X' WHERE id=1",
    )
    results.append((
        "ro_conn_refuses_update", not ok,
        f"UPDATE result: {msg}",
    ))

    ok, msg = _safe_exec(ro_conn, "DELETE FROM audit_event WHERE id=1")
    results.append((
        "ro_conn_refuses_delete", not ok,
        f"DELETE result: {msg}",
    ))

    # 7. SELECT works
    try:
        cur = ro_conn.execute("SELECT COUNT(*) FROM audit_event")
        n = cur.fetchone()[0]
        select_ok = n == 1
        select_msg = f"SELECT COUNT(*) returned {n}"
    except Exception as exc:  # noqa: BLE001
        select_ok = False
        select_msg = f"SELECT failed: {exc!r}"
    results.append(("ro_conn_accepts_select", select_ok, select_msg))

    # 8. Separate from writer (compare with _conn)
    writer_conn = getattr(jm, "_conn", None)
    is_separate = ro_conn is not writer_conn
    results.append((
        "ro_conn_is_separate_from_writer", is_separate,
        f"ro_conn is writer_conn = {not is_separate}",
    ))

    # Cleanup
    try:
        ro_conn.close()
    except Exception:  # noqa: BLE001
        pass

    return results


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
