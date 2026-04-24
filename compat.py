"""
compat.py : Couche de compatibilite Qt5/Qt6 pour QGIS 3.28 a 4.x

Centralise l'accès aux enums Qt qui ont changé de syntaxe entre PyQt5 et PyQt6.
- PyQt5 : enums courts (Qt.AlignCenter)
- PyQt6 : enums scopés (Qt.AlignmentFlag.AlignCenter)

Le shim qgis.PyQt gère certains cas, mais pas tous sur les versions
intermédiaires (3.36-3.38). Ce module fournit un fallback fiable.

Usage dans le plugin :
    from .compat import QtCompat
    widget.setAlignment(QtCompat.ALIGN_CENTER)
"""
import sys

from qgis.PyQt.QtCore import Qt, QEvent, QEasingCurve, QAbstractAnimation
from qgis.PyQt.QtGui import QPainter, QPalette
from qgis.PyQt.QtWidgets import (
    QSizePolicy, QAbstractItemView, QMessageBox, QVBoxLayout,
    QSlider, QFrame,
)
from qgis.core import Qgis, QgsFeatureRequest, QgsVectorDataProvider


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

    # --- Qgis.MessageLevel (scoped in 4.0, short in 3.x) ---
    MSG_INFO = _resolve_enum(Qgis, 'MessageLevel', 'Info')
    MSG_WARNING = _resolve_enum(Qgis, 'MessageLevel', 'Warning')
    MSG_CRITICAL = _resolve_enum(Qgis, 'MessageLevel', 'Critical')
    MSG_SUCCESS = _resolve_enum(Qgis, 'MessageLevel', 'Success')


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

    # Detect Qt major version from enum style
    qt_major = 6 if hasattr(Qt, 'AlignmentFlag') else 5
    lines.append(f"Qt enum style: {'scoped (Qt6)' if qt_major == 6 else 'short (Qt5)'}")

    return "\n".join(lines)
