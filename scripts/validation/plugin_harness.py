"""Drive the real RecoverPlugin without a QGIS main window.

The plugin wiring (project signals, watchdog, status indicator, message
bar alerts) lives in recover.py, which needs an ``iface``. This fake
records everything the plugin shows to the user, so a scenario can assert
what the user would have SEEN, not only what was logged.
"""
from __future__ import annotations

from typing import List, Tuple


class _MessageBarRecorder:
    def __init__(self):
        self.messages: List[Tuple[str, str, object, int]] = []

    def pushMessage(self, title, text, level=None, duration=0):  # noqa: N802
        self.messages.append((str(title), str(text), level, int(duration or 0)))

    def texts(self) -> List[str]:
        return [m[1] for m in self.messages]


class _StatusBarRecorder:
    def __init__(self):
        self.widgets = []

    def addPermanentWidget(self, widget, stretch=0):  # noqa: N802
        self.widgets.append(widget)

    def removeWidget(self, widget):  # noqa: N802
        if widget in self.widgets:
            self.widgets.remove(widget)


class FakeIface:
    def __init__(self):
        from qgis.PyQt.QtWidgets import QMainWindow
        self._main = QMainWindow()
        self.bar = _MessageBarRecorder()
        self.status = _StatusBarRecorder()
        self.menus = []
        self.toolbar = []

    def mainWindow(self):  # noqa: N802
        return self._main

    def messageBar(self):  # noqa: N802
        return self.bar

    def statusBarIface(self):  # noqa: N802
        return self.status

    def addPluginToMenu(self, name, action):  # noqa: N802
        self.menus.append((name, action))

    def removePluginMenu(self, name, action):  # noqa: N802
        self.menus = [m for m in self.menus if m[1] is not action]

    def addToolBarIcon(self, action):  # noqa: N802
        self.toolbar.append(action)

    def removeToolBarIcon(self, action):  # noqa: N802
        if action in self.toolbar:
            self.toolbar.remove(action)

    def mapCanvas(self):  # noqa: N802
        from qgis.gui import QgsMapCanvas
        if not hasattr(self, "_canvas"):
            self._canvas = QgsMapCanvas()
        return self._canvas


def pump(seconds: float = 0.0) -> None:
    import time
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def writer_threads() -> int:
    import threading
    return sum(1 for t in threading.enumerate()
               if t.name == "RecoverLand-Writer" and t.is_alive())
