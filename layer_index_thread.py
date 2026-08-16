"""Off-thread geometry-index build for RecoverLand (BL-RW-P5-29).

Why this exists
---------------
The rewind used to hold the GUI thread for the whole of a layer group
(118 s measured); chunking it to 40 ms slices removed the long freeze but
replaced it with a permanent one. Measured under QGIS 4.0.3 with a
heartbeat that PAINTS, i.e. that behaves like the logo's paintEvent:

    idle                                16.0 ms median, 0 % frames > 32 ms
    apply chunked on the GUI thread     43.2 ms median, 100 % frames > 32 ms
    same read done in a QThread         16.0 ms median, 0 % frames > 32 ms

23 fps instead of 62, on every frame -- "l'animation roule lentement",
exactly as reported. And chunking has no winning setting: below an 8 ms
budget the loop hits its floor (one feature is 5.2 ms) and the run takes
2.9x longer while still stuttering.

The GIL is not the obstacle. 100 % of the ~800 ms is inside OGR/GEOS,
where sip releases it: iterating while touching every geometry costs the
same as iterating without touching any, and the painting heartbeat is
perturbed by +0.0 to +0.4 ms. For comparison, a thread running pure
Python bytecode perturbs it by +11 ms.

The two ways this kills the process, measured
---------------------------------------------
`QgsVectorLayerFeatureSource` has no thread affinity under QGIS 4.0.3 and
survives commitChanges/rollBack/reload and even the destruction of its
layer. But:

  1. A full-scan iterator left UNCONSUMED that outlives its source and its
     layer SEGFAULTS the process (reproduced 3/3, exit 139).
  2. Destroying the source while the worker iterates truncates the result
     to 1 feature out of 210, silently, with no exception.

Hence the discipline below, which is not negotiable:
  - the source is created and OWNED by this thread object,
  - the iterator is fully consumed inside run(), never stored,
  - both die together when run() returns.

And the snapshot rule: a feature source is frozen at construction. It will
never see a later buffer write, and after a rollBack it still serves the
undone state -- with no API to detect it. So the index it feeds is only
valid until the first write of the batch, which is why the runners drop it
at every commit and rollback.
"""
from typing import Dict, Optional, Tuple

from qgis.PyQt.QtCore import pyqtSignal

from .core.layer_geom_index import _geom_digest
from .core.logger import flog
from .qgs_task_support import TaskEnabledThread, trace_prefix


def _build_digests(source, is_cancelled, label: str, trace_id: str = ""):
    """Iterate *source* fully and return ``{fid: (md5, bbox)}``.

    Pure: takes a feature source, returns plain tuples. Nothing Qt-owned
    crosses back to the GUI thread.
    """
    from qgis.core import QgsFeature, QgsFeatureRequest

    prefix = trace_prefix(trace_id)
    out: Dict[int, Tuple[bytes, Tuple]] = {}
    n_feats = 0
    request = QgsFeatureRequest()
    try:
        request.setSubsetOfAttributes([])
    except (AttributeError, TypeError):
        pass

    iterator = source.getFeatures(request)
    feature = QgsFeature()
    try:
        while iterator.nextFeature(feature):
            n_feats += 1
            if is_cancelled():
                # The iterator MUST be closed before we let go of it: an
                # unconsumed one outliving its source segfaults QGIS.
                break
            digest = _geom_digest(feature.geometry())
            if digest is not None:
                out[feature.id()] = digest
    finally:
        try:
            iterator.close()
        except (AttributeError, RuntimeError):
            pass
    flog(f"{prefix}layer_index_thread: scanned layer={label!r} "
         f"n_features={n_feats} n_indexed={len(out)}")
    return out


class LayerIndexThread(TaskEnabledThread):
    """Build one layer's geometry digests off the GUI thread."""

    index_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def __init__(self, layer, label: str = "", trace_id: str = ""):
        super().__init__(trace_id=trace_id)
        self._label = label or getattr(layer, "name", lambda: "?")()
        # Built here, on the caller's thread, and owned by this object for
        # its whole life. Costs ~0.1 ms whatever the buffer holds, so
        # rebuilding one per batch is free.
        self._source = None
        try:
            from qgis.core import QgsVectorLayerFeatureSource
            self._source = QgsVectorLayerFeatureSource(layer)
        except Exception as exc:  # noqa: BLE001 - fail open, caller scans
            flog(f"layer_index_thread: cannot snapshot layer={self._label!r}: "
                 f"{exc}", "WARNING")

    def run(self):
        if self._source is None:
            self.index_ready.emit({})
            self._clear_task()
            return
        try:
            digests = _build_digests(
                self._source, lambda: self._stopped, self._label,
                self._trace_id)
            if not self._stopped:
                self.index_ready.emit(digests)
        except Exception as exc:  # noqa: BLE001
            flog(f"{trace_prefix(self._trace_id)}layer_index_thread: "
                 f"error: {exc}", "ERROR")
            if not self._stopped:
                self.error_occurred.emit(str(exc))
        finally:
            # The source dies with the job, never before it: destroying it
            # while the iterator is live truncates the scan to one feature
            # without raising.
            self._source = None
            self._clear_task()

    def _start_task(self) -> None:
        self._submit_task(
            "RecoverLand geometry index",
            _build_digests_task,
            on_finished=self._on_task_finished,
            source=self._source,
            label=self._label,
            trace_id=self._trace_id,
        )

    def _on_task_finished(self, exception, result=None) -> None:
        self._source = None
        self._handle_task_finished(
            exception, result, self.index_ready, "LayerIndexThread")


def _build_digests_task(task, source, label, trace_id=""):
    return _build_digests(source, task.isCanceled, label, trace_id)


def build_index_async(layer, label: str, trace_id: str,
                      on_ready, on_failed=None) -> Optional[LayerIndexThread]:
    """Start an off-thread index build. Returns the thread, or None.

    ``None`` means the caller must proceed WITHOUT an index -- never that
    the layer is empty. TaskEnabledThread.start() answers False when a job
    is already in flight, and a caller that ignored it would wait forever
    for a signal nobody will emit.
    """
    worker = LayerIndexThread(layer, label=label, trace_id=trace_id)
    worker.index_ready.connect(on_ready)
    if on_failed is not None:
        worker.error_occurred.connect(on_failed)
    if not worker.start():
        flog(f"{trace_prefix(trace_id)}layer_index_thread: start refused "
             f"layer={label!r}, falling back to the live scan", "WARNING")
        return None
    return worker
