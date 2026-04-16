"""Test configuration: mock qgis module for pure-logic tests."""
import sys
import types
import os

# Create mock qgis module hierarchy so imports don't fail outside QGIS
if 'qgis' not in sys.modules:
    qgis_mock = types.ModuleType('qgis')
    qgis_core = types.ModuleType('qgis.core')

    class _FakeQgsMessageLog:
        INFO = 0
        WARNING = 1
        CRITICAL = 2
        SUCCESS = 3

        @staticmethod
        def logMessage(msg, tag='', level=0):
            pass

    class _FakeQgis:
        QGIS_VERSION = '3.44.0-mock'
        Info = 0
        Warning = 1
        Critical = 2
        Success = 3

    class _FakeQgsApplication:
        @staticmethod
        def qgisSettingsDirPath():
            return os.path.join(os.path.expanduser('~'), '.qgis_mock')

        @staticmethod
        def nullRepresentation():
            return 'NULL'

        @staticmethod
        def authManager():
            return None

    class _FakeQgsSettings:
        def __init__(self):
            self._groups = []

        def beginGroup(self, name):
            self._groups.append(name)

        def endGroup(self):
            if self._groups:
                self._groups.pop()

        def childGroups(self):
            return []

        def value(self, key, default=''):
            return default

    class _FakeQgsProject:
        _instance = None

        @classmethod
        def instance(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def absoluteFilePath(self):
            return ''

        def mapLayers(self):
            return {}

    class _FakeQgsFeatureRequest:
        class Flag:
            NoGeometry = 1
        NoGeometry = 1

        def __init__(self, *args):
            pass

        def setFilterFids(self, fids):
            return self

        def setSubsetOfAttributes(self, *args):
            return self

        def setFlags(self, *args):
            return self

        def setLimit(self, n):
            return self

    class _FakeQgsWkbTypes:
        NoGeometry = 100
        NullGeometry = 100

        @staticmethod
        def displayString(wkb_type):
            return 'Unknown'

    class _FakeQgsExpression:
        def __init__(self, expr_str=''):
            self._expr = expr_str

        @staticmethod
        def quotedColumnRef(col):
            return f'"{col}"'

        @staticmethod
        def quotedValue(val):
            return f"'{val}'"

        def hasParserError(self):
            return False

        def parserErrorString(self):
            return ''

        def prepare(self, fields):
            return True

    class _FakeQgsAuthMethodConfig:
        def configMap(self):
            return {}

    class _FakeQgsGeometry:
        def __init__(self):
            pass

        def isNull(self):
            return True

        def isEmpty(self):
            return True

        def asWkb(self):
            return b''

        @staticmethod
        def fromWkb(data):
            g = _FakeQgsGeometry()
            return g

    class _FakeQgsFields:
        def __init__(self):
            self._fields = []

        def __iter__(self):
            return iter(self._fields)

        def count(self):
            return len(self._fields)

        def at(self, idx):
            return self._fields[idx]

        def indexOf(self, name):
            return -1

    class _FakeQgsFeature:
        def __init__(self, fields=None):
            self._attrs = {}
            self._fid = 0

        def id(self):
            return self._fid

        def geometry(self):
            return _FakeQgsGeometry()

        def setAttribute(self, idx, val):
            self._attrs[idx] = val

        def __getitem__(self, key):
            return self._attrs.get(key)

    class _FakeQgsVectorLayer:
        pass

    class _FakeQgsDataSourceUri:
        pass

    qgis_core.QgsMessageLog = _FakeQgsMessageLog
    qgis_core.Qgis = _FakeQgis
    qgis_core.QgsApplication = _FakeQgsApplication
    qgis_core.QgsSettings = _FakeQgsSettings
    qgis_core.QgsProject = _FakeQgsProject
    qgis_core.QgsFeatureRequest = _FakeQgsFeatureRequest
    qgis_core.QgsWkbTypes = _FakeQgsWkbTypes
    qgis_core.QgsExpression = _FakeQgsExpression
    qgis_core.QgsAuthMethodConfig = _FakeQgsAuthMethodConfig
    qgis_core.QgsGeometry = _FakeQgsGeometry
    qgis_core.QgsFields = _FakeQgsFields
    qgis_core.QgsFeature = _FakeQgsFeature

    class _FakeQgsVectorDataProvider:
        class Capability:
            AddFeatures = 1
            DeleteFeatures = 2
            ChangeAttributeValues = 4
            AddAttributes = 8
            DeleteAttributes = 16
            ChangeGeometries = 0x800
        AddFeatures = 1
        DeleteFeatures = 2
        ChangeAttributeValues = 4
        AddAttributes = 8
        DeleteAttributes = 16
        ChangeGeometries = 0x800

    qgis_core.QgsVectorLayer = _FakeQgsVectorLayer
    qgis_core.QgsVectorDataProvider = _FakeQgsVectorDataProvider
    qgis_core.QgsDataSourceUri = _FakeQgsDataSourceUri

    def _core_getattr(name):
        """Auto-stub any missing qgis.core type as an empty class."""
        cls = type(name, (), {'__init__': lambda self, *a, **kw: None})
        setattr(qgis_core, name, cls)
        return cls

    qgis_core.__getattr__ = _core_getattr
    qgis_mock.core = qgis_core
    sys.modules['qgis'] = qgis_mock
    sys.modules['qgis.core'] = qgis_core

    # Mock qgis.PyQt using real PyQt if available, else create stubs
    qgis_pyqt = types.ModuleType('qgis.PyQt')
    sys.modules['qgis.PyQt'] = qgis_pyqt
    qgis_mock.PyQt = qgis_pyqt

    # Create QtCore stub with all types compat.py needs
    qtcore = types.ModuleType('qgis.PyQt.QtCore')
    qtcore.QT_VERSION_STR = '6.0.0-mock'
    qtcore.PYQT_VERSION_STR = '6.0.0-mock'

    class _Qt:
        class AlignmentFlag:
            AlignCenter = 0x0004
            AlignLeft = 0x0001
            AlignRight = 0x0002
            AlignHCenter = 0x0004
            AlignVCenter = 0x0080
        AlignCenter = 0x0004
        AlignLeft = 0x0001
        AlignRight = 0x0002
        AlignHCenter = 0x0004
        AlignVCenter = 0x0080

        class TransformationMode:
            SmoothTransformation = 1
        SmoothTransformation = 1

        class AspectRatioMode:
            KeepAspectRatio = 1
        KeepAspectRatio = 1

        class WindowType:
            WindowMaximizeButtonHint = 0x00010000
        WindowMaximizeButtonHint = 0x00010000

        class CursorShape:
            PointingHandCursor = 13
        PointingHandCursor = 13

        class ContextMenuPolicy:
            CustomContextMenu = 3
        CustomContextMenu = 3

        class TextInteractionFlag:
            TextSelectableByMouse = 1
        TextSelectableByMouse = 1

        class ItemDataRole:
            UserRole = 0x0100
        UserRole = 0x0100

        class PenStyle:
            NoPen = 0
        NoPen = 0

        class WidgetAttribute:
            WA_TransparentForMouseEvents = 76
        WA_TransparentForMouseEvents = 76

        class Orientation:
            Horizontal = 1
            Vertical = 2
        Horizontal = 1
        Vertical = 2

        class DateFormat:
            ISODate = 1
        ISODate = 1

        class ItemFlag:
            ItemIsUserCheckable = 16
        ItemIsUserCheckable = 16

        class CheckState:
            Unchecked = 0
            Checked = 2
        Unchecked = 0
        Checked = 2

    class _QEvent:
        class Type:
            Enter = 10
            Leave = 11
            PaletteChange = 39
        Enter = 10
        Leave = 11
        PaletteChange = 39

    class _QEasingCurve:
        class Type:
            InOutQuad = 3
        InOutQuad = 3

    class _QThread:
        @staticmethod
        def msleep(ms):
            pass

    class _Signal:
        def __init__(self, *args):
            pass

        def connect(self, *args):
            pass

        def disconnect(self, *args):
            pass

        def emit(self, *args):
            pass

    class _QDateTime:
        pass

    class _QDate:
        pass

    class _QTime:
        pass

    class _QTimer:
        @staticmethod
        def singleShot(ms, callback):
            pass

    class _QByteArray:
        pass

    class _QCoreApplication:
        @staticmethod
        def translate(context, text, *args):
            return text

        @staticmethod
        def installTranslator(translator):
            pass

    class _QTranslator:
        def load(self, path):
            return False

    class _QLocale:
        @staticmethod
        def system():
            return _QLocale()

        def name(self):
            return 'en_US'

    class _QVariantAnimation:
        pass

    class _QRectF:
        pass

    qtcore.Qt = _Qt
    qtcore.QEvent = _QEvent
    qtcore.QEasingCurve = _QEasingCurve
    qtcore.QThread = _QThread
    qtcore.pyqtSignal = _Signal
    qtcore.QDateTime = _QDateTime
    qtcore.QDate = _QDate
    qtcore.QTime = _QTime
    qtcore.QTimer = _QTimer
    qtcore.QByteArray = _QByteArray
    qtcore.QCoreApplication = _QCoreApplication
    qtcore.QTranslator = _QTranslator
    qtcore.QSettings = _FakeQgsSettings
    qtcore.QLocale = _QLocale
    qtcore.QVariantAnimation = _QVariantAnimation
    qtcore.QRectF = _QRectF
    sys.modules['qgis.PyQt.QtCore'] = qtcore

    # QtWidgets stub
    qtwidgets = types.ModuleType('qgis.PyQt.QtWidgets')

    class _QSizePolicy:
        class Policy:
            Preferred = 0
            Fixed = 1
            Expanding = 2
        Preferred = 0
        Fixed = 1
        Expanding = 2

    class _QAbstractItemView:
        class SelectionBehavior:
            SelectRows = 1
        SelectRows = 1

    class _QMessageBox:
        class StandardButton:
            Yes = 0x00004000
            No = 0x00010000
        Yes = 0x00004000
        No = 0x00010000

    class _QVBoxLayout:
        class SizeConstraint:
            SetMinimumSize = 1
        SetMinimumSize = 1

    qtwidgets.QSizePolicy = _QSizePolicy
    qtwidgets.QAbstractItemView = _QAbstractItemView
    qtwidgets.QMessageBox = _QMessageBox
    qtwidgets.QVBoxLayout = _QVBoxLayout
    for cls_name in ('QDialog', 'QHBoxLayout', 'QLabel', 'QPushButton',
                     'QComboBox', 'QProgressBar', 'QFormLayout', 'QCheckBox',
                     'QApplication', 'QTableWidget', 'QTableWidgetItem',
                     'QLineEdit', 'QFileDialog', 'QGraphicsDropShadowEffect',
                     'QWidget', 'QAction', 'QSpinBox', 'QGroupBox',
                     'QButtonGroup', 'QScrollArea', 'QFrame', 'QMenu',
                     'QShortcut', 'QStackedWidget', 'QSlider'):
        setattr(qtwidgets, cls_name, type(cls_name, (), {}))

    def _widgets_getattr(name):
        cls = type(name, (), {'__init__': lambda self, *a, **kw: None})
        setattr(qtwidgets, name, cls)
        return cls
    qtwidgets.__getattr__ = _widgets_getattr
    sys.modules['qgis.PyQt.QtWidgets'] = qtwidgets

    # QtGui stub
    qtgui = types.ModuleType('qgis.PyQt.QtGui')

    class _QPainter:
        class RenderHint:
            Antialiasing = 1
            SmoothPixmapTransform = 4
        Antialiasing = 1
        SmoothPixmapTransform = 4

        class CompositionMode:
            CompositionMode_Screen = 14
        CompositionMode_Screen = 14

    class _QPalette:
        class ColorRole:
            WindowText = 0
            Highlight = 12
            Mid = 5
        WindowText = 0
        Highlight = 12
        Mid = 5
    qtgui.QPainter = _QPainter
    qtgui.QPalette = _QPalette

    class _FakeQColor:
        def __init__(self, *args, **kwargs):
            self._r = args[0] if len(args) > 0 else 0
            self._g = args[1] if len(args) > 1 else 0
            self._b = args[2] if len(args) > 2 else 0
            self._a = args[3] if len(args) > 3 else 255

        def red(self): return self._r
        def green(self): return self._g
        def blue(self): return self._b
        def alpha(self): return self._a
        def name(self): return '#%02x%02x%02x' % (self._r, self._g, self._b)
    qtgui.QColor = _FakeQColor
    for cls_name in ('QIcon', 'QLinearGradient', 'QAction'):
        setattr(qtgui, cls_name, type(cls_name, (), {'__init__': lambda self, *a, **kw: None}))
    sys.modules['qgis.PyQt.QtGui'] = qtgui

    # QtSvg stub
    qtsvg = types.ModuleType('qgis.PyQt.QtSvg')
    qtsvg.QSvgRenderer = type('QSvgRenderer', (), {})
    sys.modules['qgis.PyQt.QtSvg'] = qtsvg

    # qgis.gui stub
    qgis_gui = types.ModuleType('qgis.gui')
    for cls_name in ('QgsCollapsibleGroupBox', 'QgsDateTimeEdit', 'QgsMessageBar'):
        setattr(qgis_gui, cls_name, type(cls_name, (), {}))

    def _gui_getattr(name):
        cls = type(name, (), {'__init__': lambda self, *a, **kw: None})
        setattr(qgis_gui, name, cls)
        return cls

    qgis_gui.__getattr__ = _gui_getattr
    sys.modules['qgis.gui'] = qgis_gui
    qgis_mock.gui = qgis_gui

# Ensure the plugin root is on sys.path
plugin_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_of_plugin = os.path.dirname(plugin_root)
if parent_of_plugin not in sys.path:
    sys.path.insert(0, parent_of_plugin)
