"""Custom widgets for RecoverLand plugin."""
from .toggle_switch import AppleToggleSwitch
from .review_segmented_switch import ReviewSegmentedSwitch
from .themed_logo import ThemedLogoWidget
from .restore_mode_selector import RestoreModeSelector
from .restore_preflight_dialog import RestorePreflightDialog
from .time_slider import TimeSliderWidget
from .canvas_date_bar import CanvasDateBar
from .temporal_timeline_widget import TemporalTimelineWidget
from .action_button_bar import ActionButtonBar

# Backward compat alias
GeoGitSegmentedSwitch = ReviewSegmentedSwitch

__all__ = (
    "AppleToggleSwitch",
    "CanvasDateBar",
    "TemporalTimelineWidget",
    "ReviewSegmentedSwitch",
    "GeoGitSegmentedSwitch",
    "ThemedLogoWidget",
    "RestoreModeSelector",
    "RestorePreflightDialog",
    "TimeSliderWidget",
    "ActionButtonBar",
)
