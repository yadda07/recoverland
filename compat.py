"""
compat.py : couche de compatibilite Qt5/Qt6 et QGIS 3.40 LTR -> 4.x.

Baseline declaree (metadata.txt) : QGIS 3.40. Le code reste defensif pour
les builds dev, nightly et installations non standard ou certaines API
peuvent etre absentes ou renommees. Les resolveurs essaient toujours la
forme scopee (Qt6 / QGIS 4.x), puis la forme courte (Qt5 / QGIS 3.x),
puis un fallback documente.

Divergences d'API couvertes :
- PyQt5 (Qt.AlignCenter) vs PyQt6 (Qt.AlignmentFlag.AlignCenter)
- Qgis.GeometryType (3.30+) vs QgsWkbTypes.<Name>Geometry (legacy 3.x)
- Qgis.WkbType (3.30+) vs QgsWkbTypes (legacy 3.x)
- Qgis.MessageLevel scope (3.34+) vs short (legacy 3.x)
- QgsVectorDataProvider.Capability scope (3.36+) vs short (legacy 3.x)
- QgsFeatureRequest.Flag.NoGeometry (4.x) vs QgsFeatureRequest.NoGeometry (3.x)

Acces directs Qt.X / Qgis.X / QgsXxx.Y interdits hors de ce module.

Usage :
    from .compat import QtCompat, QgisCompat
    widget.setAlignment(QtCompat.ALIGN_CENTER)
    band = QgsRubberBand(canvas, QgisCompat.GEOM_POLYGON)
"""
import sys
from typing import NamedTuple

from qgis.PyQt.QtCore import Qt, QEvent, QEasingCurve, QAbstractAnimation
from qgis.PyQt.QtGui import QPainter, QPalette
from qgis.PyQt.QtWidgets import (
    QSizePolicy, QAbstractItemView, QMessageBox, QVBoxLayout,
    QSlider, QFrame,
)
from qgis.core import Qgis, QgsFeatureRequest, QgsVectorDataProvider

try:
    from qgis.core import QgsWkbTypes
    _HAS_QGS_WKB_TYPES = True
except ImportError:  # pragma: no cover - QgsWkbTypes existe depuis QGIS 2.x
    QgsWkbTypes = None
    _HAS_QGS_WKB_TYPES = False


def _resolve_enum(parent, scoped_attr, fallback_attr):
    """Resolve an enum value with Qt6 scoped name, falling back to Qt5 short name.

    Args:
        parent: The class containing the enum (e.g. Qt, QSizePolicy)
        scoped_attr: Qt6 scoped intermediate (e.g. 'AlignmentFlag')
        fallback_attr: The enum value name (e.g. 'AlignCenter')

    Returns:
        The resolved enum value.
    """
    scoped_ns = getattr(parent, scoped_attr, None)
    if scoped_ns is not None:
        val = getattr(scoped_ns, fallback_attr, None)
        if val is not None:
            return val
    return getattr(parent, fallback_attr)


class QtCompat:
    """Namespace for all cross-version Qt enum constants used by RecoverLand."""

    # --- Qt.AlignmentFlag ---
    ALIGN_CENTER = _resolve_enum(Qt, 'AlignmentFlag', 'AlignCenter')

    # --- Qt.TransformationMode ---
    SMOOTH_TRANSFORMATION = _resolve_enum(Qt, 'TransformationMode', 'SmoothTransformation')

    # --- Qt.AspectRatioMode ---
    KEEP_ASPECT_RATIO = _resolve_enum(Qt, 'AspectRatioMode', 'KeepAspectRatio')

    # --- Qt.WindowType ---
    WINDOW_MAXIMIZE_BUTTON_HINT = _resolve_enum(Qt, 'WindowType', 'WindowMaximizeButtonHint')

    # --- QSizePolicy.Policy ---
    SIZE_PREFERRED = _resolve_enum(QSizePolicy, 'Policy', 'Preferred')
    SIZE_FIXED = _resolve_enum(QSizePolicy, 'Policy', 'Fixed')
    SIZE_EXPANDING = _resolve_enum(QSizePolicy, 'Policy', 'Expanding')

    # --- QVBoxLayout.SizeConstraint ---
    SET_MINIMUM_SIZE = _resolve_enum(QVBoxLayout, 'SizeConstraint', 'SetMinimumSize')

    # --- QAbstractItemView.SelectionBehavior ---
    SELECT_ROWS = _resolve_enum(QAbstractItemView, 'SelectionBehavior', 'SelectRows')

    # --- QEvent.Type ---
    EVENT_ENTER = _resolve_enum(QEvent, 'Type', 'Enter')
    EVENT_LEAVE = _resolve_enum(QEvent, 'Type', 'Leave')
    EVENT_PALETTE_CHANGE = _resolve_enum(QEvent, 'Type', 'PaletteChange')

    # --- QEasingCurve.Type ---
    EASE_IN_OUT_QUAD = _resolve_enum(QEasingCurve, 'Type', 'InOutQuad')

    # --- Qt.AlignmentFlag (additional) ---
    ALIGN_LEFT = _resolve_enum(Qt, 'AlignmentFlag', 'AlignLeft')
    ALIGN_RIGHT = _resolve_enum(Qt, 'AlignmentFlag', 'AlignRight')
    ALIGN_HCENTER = _resolve_enum(Qt, 'AlignmentFlag', 'AlignHCenter')
    ALIGN_VCENTER = _resolve_enum(Qt, 'AlignmentFlag', 'AlignVCenter')

    # --- Qt.CursorShape ---
    POINTING_HAND_CURSOR = _resolve_enum(Qt, 'CursorShape', 'PointingHandCursor')

    # --- Qt.ContextMenuPolicy ---
    CUSTOM_CONTEXT_MENU = _resolve_enum(Qt, 'ContextMenuPolicy', 'CustomContextMenu')

    # --- Qt.TextInteractionFlag ---
    TEXT_SELECTABLE_BY_MOUSE = _resolve_enum(Qt, 'TextInteractionFlag', 'TextSelectableByMouse')
    USER_ROLE = _resolve_enum(Qt, 'ItemDataRole', 'UserRole')

    # --- Qt.MouseButton ---
    LEFT_BUTTON = _resolve_enum(Qt, 'MouseButton', 'LeftButton')
    RIGHT_BUTTON = _resolve_enum(Qt, 'MouseButton', 'RightButton')

    # --- Qt.PenStyle ---
    NO_PEN = _resolve_enum(Qt, 'PenStyle', 'NoPen')

    # --- Qt.WidgetAttribute ---
    WA_TRANSPARENT_FOR_MOUSE = _resolve_enum(Qt, 'WidgetAttribute', 'WA_TransparentForMouseEvents')

    # --- Qt.Orientation ---
    HORIZONTAL = _resolve_enum(Qt, 'Orientation', 'Horizontal')

    # --- Qt.ItemFlag ---
    ITEM_IS_USER_CHECKABLE = _resolve_enum(Qt, 'ItemFlag', 'ItemIsUserCheckable')

    # --- Qt.CheckState ---
    CHECKED = _resolve_enum(Qt, 'CheckState', 'Checked')
    UNCHECKED = _resolve_enum(Qt, 'CheckState', 'Unchecked')

    # --- QPainter.RenderHint ---
    ANTIALIAS = _resolve_enum(QPainter, 'RenderHint', 'Antialiasing')
    SMOOTH_PIXMAP = _resolve_enum(QPainter, 'RenderHint', 'SmoothPixmapTransform')

    # --- QPainter.CompositionMode ---
    COMPOSITION_SCREEN = _resolve_enum(QPainter, 'CompositionMode', 'CompositionMode_Screen')

    # --- QPalette.ColorRole ---
    PALETTE_WINDOW_TEXT = _resolve_enum(QPalette, 'ColorRole', 'WindowText')
    PALETTE_HIGHLIGHT = _resolve_enum(QPalette, 'ColorRole', 'Highlight')
    PALETTE_MID = _resolve_enum(QPalette, 'ColorRole', 'Mid')

    # --- QMessageBox.StandardButton ---
    MSG_YES = _resolve_enum(QMessageBox, 'StandardButton', 'Yes')
    MSG_NO = _resolve_enum(QMessageBox, 'StandardButton', 'No')

    # --- QSlider.TickPosition ---
    TICK_BELOW = _resolve_enum(QSlider, 'TickPosition', 'TicksBelow')

    # --- QFrame.Shape ---
    HLINE = _resolve_enum(QFrame, 'Shape', 'HLine')

    # --- Qt.PenStyle ---
    DASH_LINE = _resolve_enum(Qt, 'PenStyle', 'DashLine')

    # --- QAbstractAnimation.State ---
    ANIM_STATE_RUNNING = _resolve_enum(QAbstractAnimation, 'State', 'Running')
    ANIM_STATE_STOPPED = _resolve_enum(QAbstractAnimation, 'State', 'Stopped')

    # --- Qt.TimeSpec ---
    UTC = _resolve_enum(Qt, 'TimeSpec', 'UTC')


def _resolve_geometry_type(name: str):
    """Resolve a geometry type enum across QGIS versions.

    Resolution order:
    1. ``Qgis.GeometryType.<Name>`` (QGIS 3.30+)
    2. ``QgsWkbTypes.GeometryType.<Name>`` (QGIS 3.x scoped binding, rare)
    3. ``QgsWkbTypes.<NameGeometry>`` short form (QGIS 3.0 - 3.28)
    4. Documented int fallback. Only reached when the user injects a stub.
       In real QGIS, branches 1 or 3 always succeed.

    Args:
        name: 'Point' | 'Line' | 'Polygon' | 'Unknown' | 'Null'.
    """
    geom_type_ns = getattr(Qgis, 'GeometryType', None)
    if geom_type_ns is not None:
        val = getattr(geom_type_ns, name, None)
        if val is not None:
            return val
    if _HAS_QGS_WKB_TYPES and QgsWkbTypes is not None:
        scoped_ns = getattr(QgsWkbTypes, 'GeometryType', None)
        if scoped_ns is not None:
            val = getattr(scoped_ns, name, None)
            if val is not None:
                return val
        short_name = name + 'Geometry' if name in ('Point', 'Line', 'Polygon', 'Unknown', 'Null') else name
        val = getattr(QgsWkbTypes, short_name, None)
        if val is not None:
            return val
    fallback = {'Point': 0, 'Line': 1, 'Polygon': 2, 'Unknown': 3, 'Null': 4}
    return fallback.get(name, 0)


def _resolve_wkb_no_geometry():
    """Resolve the 'NoGeometry' WKB type marker across QGIS versions.

    Resolution order:
    1. ``Qgis.WkbType.NoGeometry`` (QGIS 3.30+)
    2. ``QgsWkbTypes.NoGeometry`` (QGIS 3.0 - 3.28)
    3. Int 100 (QgsWkbTypes::NoGeometry historical value, never reached in practice).
    """
    wkb_ns = getattr(Qgis, 'WkbType', None)
    if wkb_ns is not None:
        val = getattr(wkb_ns, 'NoGeometry', None)
        if val is not None:
            return val
    if _HAS_QGS_WKB_TYPES and QgsWkbTypes is not None:
        val = getattr(QgsWkbTypes, 'NoGeometry', None)
        if val is not None:
            return val
    return 100


class QgisCompat:
    """Namespace for cross-version QGIS enum constants."""

    # --- QgsFeatureRequest flags ---
    try:
        NO_GEOMETRY = QgsFeatureRequest.Flag.NoGeometry
    except AttributeError:
        NO_GEOMETRY = QgsFeatureRequest.NoGeometry

    # --- QgsVectorDataProvider.Capability ---
    try:
        CAP_ADD_FEATURES = QgsVectorDataProvider.Capability.AddFeatures
        CAP_DELETE_FEATURES = QgsVectorDataProvider.Capability.DeleteFeatures
        CAP_CHANGE_ATTRIBUTE_VALUES = QgsVectorDataProvider.Capability.ChangeAttributeValues
        CAP_CHANGE_GEOMETRIES = QgsVectorDataProvider.Capability.ChangeGeometries
    except AttributeError:
        CAP_ADD_FEATURES = QgsVectorDataProvider.AddFeatures
        CAP_DELETE_FEATURES = QgsVectorDataProvider.DeleteFeatures
        CAP_CHANGE_ATTRIBUTE_VALUES = QgsVectorDataProvider.ChangeAttributeValues
        CAP_CHANGE_GEOMETRIES = QgsVectorDataProvider.ChangeGeometries

    # --- Qgis.MessageLevel (scoped in 3.34+/4.0, short in 3.x) ---
    MSG_INFO = _resolve_enum(Qgis, 'MessageLevel', 'Info')
    MSG_WARNING = _resolve_enum(Qgis, 'MessageLevel', 'Warning')
    MSG_CRITICAL = _resolve_enum(Qgis, 'MessageLevel', 'Critical')
    MSG_SUCCESS = _resolve_enum(Qgis, 'MessageLevel', 'Success')

    # --- Qgis.GeometryType (introduced 3.30, fallback via QgsWkbTypes) ---
    GEOM_POINT = _resolve_geometry_type('Point')
    GEOM_LINE = _resolve_geometry_type('Line')
    GEOM_POLYGON = _resolve_geometry_type('Polygon')
    GEOM_UNKNOWN = _resolve_geometry_type('Unknown')
    GEOM_NULL = _resolve_geometry_type('Null')

    # --- Qgis.WkbType.NoGeometry / QgsWkbTypes.NoGeometry ---
    WKB_NO_GEOMETRY = _resolve_wkb_no_geometry()


class QgisVersion(NamedTuple):
    """Parsed QGIS version (major, minor, patch)."""
    major: int
    minor: int
    patch: int

    def at_least(self, major: int, minor: int) -> bool:
        return (self.major, self.minor) >= (major, minor)


def qgis_version_info() -> QgisVersion:
    """Return parsed QGIS version. Falls back to (0, 0, 0) if unparseable."""
    try:
        version_str = Qgis.QGIS_VERSION
        parts = version_str.split('-')[0].split('.')
        major = int(parts[0]) if len(parts) >= 1 else 0
        minor = int(parts[1]) if len(parts) >= 2 else 0
        patch_str = parts[2] if len(parts) >= 3 else '0'
        patch_digits = ''
        for ch in patch_str:
            if ch.isdigit():
                patch_digits += ch
            else:
                break
        patch = int(patch_digits) if patch_digits else 0
        return QgisVersion(major, minor, patch)
    except Exception:
        return QgisVersion(0, 0, 0)


def is_qt6() -> bool:
    """Return True if running under PyQt6/Qt6, False under PyQt5/Qt5.

    PyQt 5.15 exposes Qt.AlignmentFlag as a forward-compat alias,
    so hasattr(Qt, 'AlignmentFlag') alone is unreliable.
    Primary check: PYQT_VERSION_STR major digit.
    Fallback: attribute presence (for unknown builds).
    """
    try:
        from qgis.PyQt.QtCore import PYQT_VERSION_STR
        return int(PYQT_VERSION_STR.split('.')[0]) >= 6
    except Exception:
        return hasattr(Qt, 'AlignmentFlag')


def get_environment_info() -> str:
    """Return a formatted string with QGIS/Qt/Python version info for logging."""
    lines = []
    lines.append(f"Python:  {sys.version.split()[0]}")
    lines.append(f"QGIS:    {Qgis.QGIS_VERSION}")

    try:
        from qgis.PyQt.QtCore import QT_VERSION_STR
        lines.append(f"Qt:      {QT_VERSION_STR}")
    except ImportError:
        lines.append("Qt:      unknown")

    try:
        from qgis.PyQt.QtCore import PYQT_VERSION_STR
        lines.append(f"PyQt:    {PYQT_VERSION_STR}")
    except ImportError:
        lines.append("PyQt:    unknown")

    qt6 = is_qt6()
    lines.append(f"Qt enum style: {'scoped (Qt6)' if qt6 else 'short (Qt5)'}")

    return "\n".join(lines)
