# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Constants and configuration for RecoverLand plugin."""

PLUGIN_NAME = "RecoverLand"

# BL-RW-P1-08 (CR-8): bounding-box L_inf tolerance for makeValid() drift
# detection. Measured in CRS units of the geometry, so the absolute size
# depends on the layer projection:
#   EPSG:4326 (degrees)   -> 1e-6 deg ~= 11 cm at the equator
#   projected metres CRS  -> 1e-6 m   ~= 1 micrometre
# When the post-makeValid bbox deviates by more than this from the input
# bbox on any of (xmin, ymin, xmax, ymax), _buffer_update skips the apply
# step with status=SKIPPED_GEOMETRY_DRIFT instead of silently storing the
# repaired-but-drifted geometry.
# Geometry side of an UPDATE whose geometry went from NULL to a value (or
# from a value to NULL). SQL NULL in geometry_wkb / new_geometry_wkb means
# "the geometry did not change"; it cannot also mean "there was no
# geometry": a rewind then left an added geometry in place and reported
# APPLIED. An empty BLOB is that explicit "no geometry" state.
GEOMETRY_NULL_MARKER = b""
# The capture writes the marker since journal schema 7. No earlier version
# ever wrote a zero-length geometry BLOB (a NULL geometry was SQL NULL), so
# in an older event an empty BLOB can only be corruption and must be
# refused, never read as "no geometry".
NULL_MARKER_SINCE_SCHEMA = 7


def is_null_geometry_marker(wkb, event_schema_version) -> bool:
    """True when *wkb* is the explicit "no geometry" state of an event.

    *event_schema_version* is the event's own version (None for rows
    written before the column existed).
    """
    try:
        version = int(event_schema_version or 0)
    except (TypeError, ValueError):
        version = 0
    return (wkb is not None and len(wkb) == 0
            and version >= NULL_MARKER_SINCE_SCHEMA)


MAKEVALID_DRIFT_TOLERANCE = 1e-6
