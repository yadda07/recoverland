import os
import sys

try:
    from qgis.PyQt.QtCore import QCoreApplication, QTranslator, QSettings, QLocale
    _HAS_QGIS = True
except ImportError:
    # Keep the PACKAGE importable without Qt. classFactory still needs Qt and
    # would raise NameError -- that is unchanged and fine, it only ever runs
    # inside QGIS. What this guard buys is offline tooling: anything doing
    # ``from recoverland.core... import x`` imports this module first, and a
    # hard import here makes the whole package unusable under a plain python.
    # Measured: without the guard, ``import recoverland`` raises
    # ModuleNotFoundError('qgis') on the dev machine's python 3.14.
    _HAS_QGIS = False

_translator = None


def _resolve_locale() -> str:
    """Return the locale QGIS itself would use for the UI.

    QGIS's own two-step convention (pyplugin_installer/installer_data.py, same
    code in 3.40 and 4.x): ``locale/userLocale`` is only authoritative when
    ``locale/overrideFlag`` is set. Reading userLocale unconditionally picks up
    a stale value left in the profile ini and translates the plugin into a
    different language than the rest of the interface.
    """
    settings = QSettings()
    if settings.value('locale/overrideFlag', False, type=bool):
        return settings.value('locale/userLocale', '') or QLocale.system().name()
    return QLocale.system().name()


def classFactory(iface):
    from .deps import ensure_dependencies
    ensure_dependencies()

    global _translator
    locale = _resolve_locale()
    i18n_dir = os.path.join(os.path.dirname(__file__), 'i18n')
    # (locale or 'en'): an empty locale would yield 'recoverland_' and look for
    # a file that cannot exist.
    base = 'recoverland_{}'.format((locale or 'en')[:2])
    qm_path = os.path.join(i18n_dir, base + '.qm')

    # We NEVER compile a .qm at runtime. The catalogue shipped in the package is
    # the only one we load, and it is guaranteed by content at build time
    # (scripts/build_release.py refuses to produce a ZIP otherwise).
    # Two measured facts about the recompile branch that used to live here:
    #  - The trigger was "the .qm is missing while a .ts sits next to it". On a
    #    normal install that is unreachable, since the package ships the .qm --
    #    so the branch was latent, not routinely firing. It is removed anyway
    #    because of what it did WHEN it fired, below. (An earlier reading of
    #    this code claimed the trigger compared mtimes; it never did. Recorded
    #    here because the mtime idea is tempting and would be wrong: QGIS
    #    restores no mtime on install -- unzip.py uses extractall in 4.0.3 and
    #    an open/write loop in 3.40, and zipfile never calls os.utime -- so any
    #    date comparison between two shipped files is a coin toss.)
    #  - The fallback compiler it invoked (lrelease is absent from QGIS 4.0.3 and
    #    OSGeo4W here) overwrote a correct catalogue with a dead one: measured
    #    339/339 messages resolving before the call, 18/339 after -- and none of
    #    the 18 was a real translation, only sources whose English equals the
    #    French. So the branch never repaired anything; it destroyed the English
    #    UI of the plugin.
    #
    # Install only a translator that actually loaded and carries messages: an
    # unchecked load() silently installs an inert translator, and the whole UI
    # then falls back to the untranslated source strings with no diagnostic.
    # This check catches an unreadable or truncated file; it cannot recognise a
    # well-formed but empty-of-translations catalogue (the dead .qm above loaded
    # fine and reported isEmpty() False), which is exactly why the real content
    # check lives in the build, not here.
    #
    # A locale with no shipped catalogue (no recoverland_<xx>.qm) installs
    # nothing and the plugin speaks French: unchanged behaviour.
    if os.path.exists(qm_path):
        candidate = QTranslator()
        if candidate.load(qm_path) and not candidate.isEmpty():
            _translator = candidate
            QCoreApplication.installTranslator(_translator)
        else:
            print(
                f"[RecoverLand] classFactory: unusable translation file {qm_path}; "
                "continuing untranslated",
                file=sys.stderr,
            )

    from .recover import RecoverPlugin
    return RecoverPlugin(iface)
