"""Custom widgets and VTT-inspired design system for RecoverLand plugin."""
from .toggle_switch import AppleToggleSwitch
from .review_segmented_switch import ReviewSegmentedSwitch
from .themed_logo import ThemedLogoWidget
from .restore_mode_selector import RestoreModeSelector
from .restore_preflight_dialog import RestorePreflightDialog
from .time_slider import TimeSliderWidget
from .canvas_date_bar import CanvasDateBar
from .temporal_timeline_widget import TemporalTimelineWidget
from .action_button_bar import ActionButtonBar
from .theme import ThemeTokens, resolve_theme, build_dialog_stylesheet
from .icons import theme_icon, theme_pixmap, icone_etat, couleur_etat

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
    "ThemeTokens",
    "resolve_theme",
    "build_dialog_stylesheet",
    "theme_icon",
    "theme_pixmap",
    "icone_etat",
    "couleur_etat",
)
