"""Run a snippet in a child QGIS python and let it die like QGIS dies.

Crash-safety claims cannot be proven in-process: a ``finally`` block, an
``atexit`` hook or a destructor would run and tidy up exactly what a real
crash leaves behind. The child process is killed with ``os._exit`` at a
named fault point (``RECOVERLAND_FAULT``, see ``core/capture_outbox.py``),
which skips all of them.

The child inherits the environment of the QGIS python running the suite
(PATH, QGIS_PREFIX_PATH, PYTHONPATH set by ``python-qgis.bat``), plus the
plugin root on ``sys.path`` and an offscreen Qt platform.
"""
from __future__ import annotations

import os
import subprocess  # nosec B404: fixed argv, our own interpreter
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Dict, Optional, Tuple

_PLUGIN_ROOT = Path(__file__).resolve().parents[2]

FAULT_EXIT_CODE = 86

_PRELUDE = """
import os, sys, time, json
sys.path.insert(0, {plugin_parent!r})
sys.path.insert(0, {plugin_root!r})
from qgis.core import QgsApplication
_app = QgsApplication([], False)
_app.initQgis()
"""


def run_child(body: str, fault: str = "", timeout: float = 120.0,
              extra_env: Optional[Dict[str, str]] = None,
              workdir: Optional[str] = None) -> Tuple[int, str, str]:
    """Run *body* in a fresh QGIS python; returns (returncode, stdout, stderr).

    ``returncode == FAULT_EXIT_CODE`` proves the fault point was reached.
    """
    code = _PRELUDE.format(plugin_parent=str(_PLUGIN_ROOT.parent),
                           plugin_root=str(_PLUGIN_ROOT)) + textwrap.dedent(body)
    tmpdir = workdir or tempfile.mkdtemp(prefix="rl_crash_child_")
    script = os.path.join(tmpdir, "child.py")
    with open(script, "w", encoding="utf-8") as fh:
        fh.write(code)
    env = dict(os.environ)
    env["QT_QPA_PLATFORM"] = "offscreen"
    env["PYTHONUNBUFFERED"] = "1"
    env.pop("RECOVERLAND_FAULT", None)
    if fault:
        env["RECOVERLAND_FAULT"] = fault
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(  # nosec B603: our interpreter, our script
        [sys.executable, script], capture_output=True, text=True,
        timeout=timeout, env=env, cwd=tmpdir, check=False,
        encoding="utf-8", errors="replace")
    return proc.returncode, proc.stdout, proc.stderr
