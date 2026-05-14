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
MAKEVALID_DRIFT_TOLERANCE = 1e-6
