"""Tests for TimeSliderWidget and RestoreModeSelector."""
from recoverland.widgets.time_slider import _compute_granularity, _SECS_24H, _SECS_30D


class TestComputeGranularity:
    """Unit tests for adaptive granularity computation."""

    def test_short_range_uses_seconds(self):
        assert _compute_granularity(3600) == 1
        assert _compute_granularity(_SECS_24H) == 1

    def test_medium_range_uses_minutes(self):
        assert _compute_granularity(_SECS_24H + 1) == 60
        assert _compute_granularity(_SECS_30D) == 60

    def test_large_range_uses_hours(self):
        assert _compute_granularity(_SECS_30D + 1) == 3600
        assert _compute_granularity(365 * 86400) == 3600

    def test_zero_range(self):
        assert _compute_granularity(0) == 1

    def test_negative_range(self):
        assert _compute_granularity(-100) == 1


class TestRestoreModeSelectorPalette:
    """Verify RestoreModeSelector uses no hard-coded colors."""

    def test_no_hardcoded_hex_colors(self):
        import inspect
        from recoverland.widgets.restore_mode_selector import RestoreModeSelector
        source = inspect.getsource(RestoreModeSelector)
        assert "#2a7de1" not in source
        assert "#e67e22" not in source
        assert "rgb(" not in source.lower().replace("_rgba", "").replace("rgba(", "")

    def test_apply_styles_uses_palette(self):
        import inspect
        from recoverland.widgets.restore_mode_selector import RestoreModeSelector
        source = inspect.getsource(RestoreModeSelector._apply_styles)
        assert "self.palette()" in source
        assert "highlight" in source


class TestTimeSliderGranularityMapping:
    """Test datetime-to-int mapping logic."""

    def test_granularity_seconds_for_1h(self):
        assert _compute_granularity(3600) == 1

    def test_granularity_minutes_for_7d(self):
        assert _compute_granularity(7 * 86400) == 60

    def test_granularity_hours_for_90d(self):
        assert _compute_granularity(90 * 86400) == 3600

    def test_steps_for_1h_range(self):
        range_secs = 3600
        gran = _compute_granularity(range_secs)
        steps = range_secs // gran
        assert steps == 3600

    def test_steps_for_7d_range(self):
        range_secs = 7 * 86400
        gran = _compute_granularity(range_secs)
        steps = range_secs // gran
        assert steps == 7 * 24 * 60

    def test_steps_for_1y_range(self):
        range_secs = 365 * 86400
        gran = _compute_granularity(range_secs)
        steps = range_secs // gran
        assert steps == 365 * 24


class TestBugRegressionModeSelector:
    """Verify BL-TS-00 regression: mode selector source code checks."""

    def test_mode_selector_file_has_no_hardcoded_colors(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "widgets", "restore_mode_selector.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        assert "#2a7de1" not in src, "Hard-coded blue still present"
        assert "#e67e22" not in src, "Hard-coded orange still present"

    def test_time_slider_file_has_no_hardcoded_colors(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "widgets", "time_slider.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        hex_colors = [m for m in src.split() if m.startswith("#") and len(m) == 7]
        assert len(hex_colors) == 0, f"Hard-coded hex colors found: {hex_colors}"


class TestRecoverDialogPeriodStack:
    """Verify BL-TS-02: period stack is wired in recover_dialog.py."""

    def test_period_stack_exists_in_source(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "recover_dialog.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        assert "_period_stack" in src
        assert "QStackedWidget" in src
        assert "_on_period_mode_changed" in src
        assert "TimeSliderWidget" in src

    def test_version_mode_pipeline_exists(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "recover_dialog.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        assert "_recover_version_mode" in src
        assert "_recover_event_mode" in src
        assert "VersionFetchThread" in src
