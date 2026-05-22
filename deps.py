"""Automatic dependency installer for RecoverLand.

Called once from ``classFactory`` at plugin load time.
Strategy: pip in-process first (zero subprocess), then silent subprocess
fallback.  If all fail the plugin still loads (stdlib fallbacks exist).

Runs at most **once per QGIS session** — the ``_already_ran`` flag is
module-level and survives plugin reloads (Python caches imported modules).

QGIS 3.40 — 4.x compatible.
"""
import importlib
import logging
import os
import sys

_log = logging.getLogger("RecoverLand.deps")

_VENDOR_DIR = os.path.join(os.path.dirname(__file__), "_vendor")

_already_ran = False

_DEPS = {
    "defusedxml": "defusedxml",
}


def _is_importable(name: str) -> bool:
    """Return True if *name* can be imported."""
    try:
        importlib.import_module(name)
        return True
    except Exception:  # noqa: BLE001
        return False


def _ensure_vendor_on_path() -> None:
    """Add plugin-local vendor dir to sys.path if present."""
    if os.path.isdir(_VENDOR_DIR) and _VENDOR_DIR not in sys.path:
        sys.path.insert(0, _VENDOR_DIR)


def _install_inprocess(pip_name: str) -> bool:
    """Install via pip's internal API — no subprocess, no shell window."""
    try:
        from pip._internal.commands.install import InstallCommand  # noqa: PLC0415
        cmd = InstallCommand("install", "Install packages.")
        ret = cmd.main(["--user", "--quiet", "--quiet", pip_name])
        return ret == 0
    except Exception as exc:  # noqa: BLE001
        _log.debug("RecoverLand.deps: in-process pip failed: %s", exc)
        return False


def _install_inprocess_target(pip_name: str) -> bool:
    """Install via pip internal API into plugin-local _vendor dir."""
    try:
        os.makedirs(_VENDOR_DIR, exist_ok=True)
        from pip._internal.commands.install import InstallCommand  # noqa: PLC0415
        cmd = InstallCommand("install", "Install packages.")
        ret = cmd.main(["--target", _VENDOR_DIR, "--quiet", "--quiet", pip_name])
        return ret == 0
    except Exception as exc:  # noqa: BLE001
        _log.debug("RecoverLand.deps: in-process target pip failed: %s", exc)
        return False


def ensure_dependencies() -> dict:
    """Install missing packages. Return ``{pkg: status}`` report.

    Runs at most once per QGIS process to avoid retry loops.
    """
    global _already_ran
    if _already_ran:
        return {}
    _already_ran = True

    _ensure_vendor_on_path()

    report = {}
    failures = []

    for import_name, pip_name in _DEPS.items():
        if _is_importable(import_name):
            report[import_name] = "already_installed"
            _log.debug("RecoverLand.deps: %s already available", import_name)
            continue

        _log.info("RecoverLand.deps: %s missing, attempting install", import_name)

        installed = False
        for installer in (_install_inprocess, _install_inprocess_target):
            if installer(pip_name):
                _ensure_vendor_on_path()
                if _is_importable(import_name):
                    installed = True
                    report[import_name] = "installed"
                    _log.info("RecoverLand.deps: %s installed OK via %s",
                              import_name, installer.__name__)
                    break

        if not installed:
            report[import_name] = "failed"
            failures.append(import_name)
            _log.warning(
                "RecoverLand.deps: %s install failed "
                "(stdlib fallback active)", import_name,
            )

    if failures:
        _notify_user(failures)

    return report


def _notify_user(failures: list) -> None:
    """Show a QGIS message bar warning (non-blocking, delayed)."""
    try:
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415
        from qgis.utils import iface  # noqa: PLC0415
        if iface is None:
            return

        names = ", ".join(failures)
        msg = (
            f"RecoverLand : {names} non disponible. "
            f"Le plugin fonctionne normalement. "
            f"Pour une securite XML optimale, executez dans la "
            f"console Python de QGIS : "
            f"import subprocess, sys; "
            f"subprocess.check_call([sys.executable, '-m', 'pip', "
            f"'install', '--user', '{names}'])"
        )

        def _show():
            try:
                bar = iface.messageBar()
                if bar:
                    bar.pushWarning("RecoverLand", msg)
            except Exception:  # noqa: BLE001
                pass

        QTimer.singleShot(3000, _show)
    except Exception:  # noqa: BLE001
        pass
