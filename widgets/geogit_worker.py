"""Background worker for GeoGit session — initial cache load.

Fetches ALL events (no bbox filter) for the time range into a dict
keyed by fingerprint. Emits progress per layer and final results.
Overlay creation + viewport render happen on the UI thread after.
"""
from __future__ import annotations

import time
import uuid
from typing import Dict, List

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog


class GeoGitCacheWorker(QThread):
    """Load event cache in background for a GeoGitSession.

    Signals
    -------
    progress : str, int, int
        (layer_name, n_events_loaded, layer_index)
    finished_ok : str, dict
        (trace_id, {fingerprint: [events]})
    finished_err : str, str
        (trace_id, error_message)
    """

    progress = pyqtSignal(str, int, int)
    finished_ok = pyqtSignal(str, object)
    finished_err = pyqtSignal(str, str)

    def __init__(
        self,
        journal,
        layer_infos: List[dict],
        t_min: str,
        t_max: str,
        max_events: int = 10000,
        parent=None,
    ):
        super().__init__(parent)
        self._journal = journal
        self._layer_infos = layer_infos
        self._t_min = t_min
        self._t_max = t_max
        self._max_events = max_events
        self._cancelled = False
        self.trace_id = uuid.uuid4().hex[:8]

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def run(self) -> None:
        from ..core.event_stream_repository import fetch_events_in_zone

        t0 = time.monotonic()
        tid = self.trace_id
        cache: Dict[str, list] = {}
        total = 0
        conn = None

        try:
            conn = self._journal.create_read_connection()
            no_bbox = (float('-inf'), float('-inf'), float('inf'), float('inf'))

            for idx, info in enumerate(self._layer_infos):
                if self._cancelled:
                    flog(f"[{tid}] geogit_cache: cancelled at layer {idx}", "INFO")
                    return

                fp = info["fingerprint"]
                events, stats = fetch_events_in_zone(
                    conn, fp, no_bbox,
                    self._t_min, self._t_max,
                    limit=self._max_events,
                    trace_id=tid,
                )
                cache[fp] = events
                total += len(events)

                flog(
                    f"[{tid}] geogit_cache: layer={info['layer_name']} "
                    f"n_events={len(events)} "
                    f"truncated={stats.n_events_truncated}",
                    "INFO",
                )
                self.progress.emit(info["layer_name"], len(events), idx)

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            flog(
                f"[{tid}] geogit_cache: done "
                f"n_layers={len(cache)} total_events={total} "
                f"elapsed_ms={elapsed_ms}",
                "INFO",
            )
            self.finished_ok.emit(tid, cache)

        except Exception as exc:  # noqa: BLE001
            flog(f"[{tid}] geogit_cache: fatal error={exc!r}", "ERROR")
            self.finished_err.emit(tid, str(exc))

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


__all__ = ["GeoGitCacheWorker"]
