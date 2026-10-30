# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_slider_bounds_local_time - les deux bornes du curseur Rewind dans le meme fuseau.

Constat du 29/10/2026 (7.1.4, mode Rewind) : la borne gauche du curseur
affichait 06:52 pour des editions faites a 07:52 heure locale (UTC+1),
la borne droite 07:53 heure locale.

Cause : `_refresh_slider_bounds` lit la date la plus ancienne du journal
(UTC) avec `_parse_iso_datetime`, qui la marque UTC ; la borne droite est
`QDateTime.currentDateTime()`, en heure locale. `TimeSliderWidget.set_bounds`
ecrivait chaque libelle avec `toString()`, qui n'en convertit aucun : la
borne gauche sortait en heure UTC, la droite en heure locale.

Prouve ici par la vraie chaine, de la chaine ISO du journal au libelle :
`RecoverDialog._refresh_slider_bounds` sur un porteur qui n'a que ce
qu'elle lit (le cache de statistiques et un vrai TimeSliderWidget).

  A  borne gauche : le libelle est l'heure locale de (plus ancien - 1 s),
     pas son heure UTC.
  B  borne droite : le libelle est l'heure locale de "maintenant".
  C  l'instant ne bouge pas : la coupure initiale reste (plus ancien - 1 s)
     en secondes depuis l'epoque, avant comme apres le correctif.

Sur une machine en UTC (le conteneur de la CI), A ne distingue rien : le
scenario impose alors un fuseau a decalage non nul (TZ, regle POSIX qui
ne demande pas tzdata) et le rend en teardown. S'il ne peut pas en
imposer, l'assertion "offset_nonzero" le dit (FAIL) au lieu de passer
sans rien prouver.

Verdict avant correctif : FAIL (A). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

SCENARIO_ID = "rw_slider_bounds_local_time"
INVARIANT = "I-SLIDER-CLOCK"

EXPECTED_SIGNATURE = r"rw_slider_bounds_local_time run: trace_id="

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

# A winter instant: Europe/Paris is UTC+1 there, whatever today's DST.
_OLDEST_ISO = "2026-01-15T06:52:30Z"
_FORCED_TZ = "CET-1CEST,M3.5.0,M10.5.0/3"
_FMT = "dd/MM/yy HH:mm"


def _utc(iso):
    from recoverland.recover_dialog import RecoverDialog
    return RecoverDialog._parse_iso_datetime(iso)


def _offset_at(iso) -> int:
    return int(_utc(iso).toLocalTime().offsetFromUtc())


def _set_tz(value):
    if value is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = value
    if hasattr(time, "tzset"):
        time.tzset()


def setup(ctx):
    ctx.data.clear()
    ctx.data["tz_before"] = os.environ.get("TZ")
    ctx.data["tz_forced"] = False
    if _offset_at(_OLDEST_ISO) == 0:
        _set_tz(_FORCED_TZ)
        ctx.data["tz_forced"] = True


def _carrier(oldest_iso, newest_iso):
    from recoverland.recover_dialog import RecoverDialog
    from recoverland.widgets import TimeSliderWidget

    class _Stats:
        @staticmethod
        def is_empty():
            return False

        @staticmethod
        def global_min_date():
            return oldest_iso

        @staticmethod
        def global_max_date():
            return newest_iso

    class _Carrier:
        _SLIDER_DEFAULT_OFFSET_SECS = RecoverDialog._SLIDER_DEFAULT_OFFSET_SECS
        _parse_iso_datetime = staticmethod(RecoverDialog._parse_iso_datetime)

        def __init__(self):
            self._stats_cache = _Stats()
            self.time_slider = TimeSliderWidget()

    return _Carrier()


def run(ctx):
    from qgis.PyQt.QtCore import QDateTime
    from recoverland.core.logger import flog
    from recoverland.recover_dialog import RecoverDialog
    d = ctx.data
    d["offset_s"] = _offset_at(_OLDEST_ISO)
    oldest = _utc(_OLDEST_ISO).addSecs(-1)
    d["want_left_local"] = oldest.toLocalTime().toString(_FMT)
    d["left_utc"] = oldest.toUTC().toString(_FMT)
    d["want_cutoff_s"] = int(oldest.toSecsSinceEpoch())
    carrier = _carrier(_OLDEST_ISO, "")
    try:
        before = QDateTime.currentDateTime()
        RecoverDialog._refresh_slider_bounds(carrier)
        after = QDateTime.currentDateTime()
        slider = carrier.time_slider
        d["left"] = slider._oldest_label.text()
        d["right"] = slider._newest_label.text()
        d["right_ok"] = sorted({before.toString(_FMT), after.toString(_FMT)})
        d["cutoff_s"] = int(slider.cutoff_datetime().toSecsSinceEpoch())
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        d["crash"] = f"{type(exc).__name__}: {exc}"
    flog(f"rw_slider_bounds_local_time run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:2000]}", "INFO")


def teardown(ctx):
    if ctx.data.get("tz_forced"):
        _set_tz(ctx.data.get("tz_before"))


def assertions(ctx):
    d = ctx.data
    if d.get("crash"):
        return [("no_crash", False, d["crash"])]
    return [
        ("offset_nonzero", d["offset_s"] != 0,
         f"local offset at {_OLDEST_ISO} = {d['offset_s']} s "
         f"(TZ forced: {d['tz_forced']})"),
        ("A_left_bound_local", d["left"] == d["want_left_local"],
         f"left label {d['left']!r}, local {d['want_left_local']!r}, "
         f"UTC {d['left_utc']!r}"),
        ("B_right_bound_local", d["right"] in d["right_ok"],
         f"right label {d['right']!r}, local now {d['right_ok']}"),
        ("C_cutoff_instant_unchanged", d["cutoff_s"] == d["want_cutoff_s"],
         f"cutoff {d['cutoff_s']} s, expected {d['want_cutoff_s']} s"),
    ]
