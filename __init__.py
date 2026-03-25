import os
from qgis.PyQt.QtCore import QCoreApplication, QTranslator, QSettings, QLocale

_translator = None


def classFactory(iface):
    global _translator
    locale = QSettings().value('locale/userLocale', QLocale.system().name())
    i18n_dir = os.path.join(os.path.dirname(__file__), 'i18n')
    base = 'recoverland_{}'.format(locale[:2])
    qm_path = os.path.join(i18n_dir, base + '.qm')
    ts_path = os.path.join(i18n_dir, base + '.ts')

    if not os.path.exists(qm_path) and os.path.exists(ts_path):
        try:
            from .i18n.compile_translations import compile_ts_to_qm
            compile_ts_to_qm(ts_path, qm_path)
        except Exception:
            pass

    if os.path.exists(qm_path):
        _translator = QTranslator()
        _translator.load(qm_path)
        QCoreApplication.installTranslator(_translator)

    from .recover import RecoverPlugin
    return RecoverPlugin(iface)
