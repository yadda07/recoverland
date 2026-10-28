# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Jetons visuels et calcul de contraste WCAG inspires de VTT (hote/apparence.py, Theme.qml).

Garantit un contraste mesure >= 4.55 pour les textes et >= 3.05 pour les bordures
de controle sur l'ensemble des surfaces claires et sombres de QPalette.
Compatible Qt 5 (QGIS 3.40) et Qt 6 (QGIS 4.x).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from qgis.PyQt.QtCore import QSettings
from qgis.PyQt.QtGui import QColor, QFont, QFontDatabase, QPalette
from qgis.PyQt.QtWidgets import QApplication, QWidget

from ..compat import QtCompat, is_qt6
from ..core.logger import flog

ICONE_XS, ICONE_SM, ICONE_MD, ICONE_LG = 16, 18, 20, 24
TRAIT_ICONE = 1.5
HAUTEUR_COMPACTE, HAUTEUR_CONTROLE, HAUTEUR_CONFORTABLE, HAUTEUR_PARCOURS = 34, 42, 48, 52
ESPACE_SERRE, ESPACE_XS, ESPACE_SM, ESPACE_MD = 2, 4, 8, 12
ESPACE_GROUPE, ESPACE_SECTION, ESPACE_LARGE = 16, 24, 32
RAYON_DOUX, RAYON_BLOC, RAYON_CONTROLE, RAYON_BULLE, RAYON_PANNEAU = 2, 4, 6, 8, 10
MICRO_DUREE, NAVIGATION_DUREE, TRANSITION_DUREE = 140, 180, 240

_SEUIL_TEXTE, _SEUIL_ACCENT, _SEUIL_DOUX, _SEUIL_LIGNE = 4.55, 5.2, 4.6, 3.05
_SEUIL_SOMBRE, _PAS_MELANGE = 0.30, 100
_CANDIDATS_TEXTE = ("Segoe UI", "DejaVu Sans", "Noto Sans")
_CANDIDATS_TITRE = ("Bahnschrift", "Segoe UI Variable Display", "Segoe UI Semibold", "Segoe UI", "DejaVu Sans")
_CANDIDATS_MONO = ("Consolas", "DejaVu Sans Mono", "Courier New")


def luminance(couleur: QColor) -> float:
    """Luminance relative sRGB selon WCAG 2.1."""
    def _lin(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    return 0.2126 * _lin(couleur.redF()) + 0.7152 * _lin(couleur.greenF()) + 0.0722 * _lin(couleur.blueF())


def contrast_ratio(a: QColor, b: QColor) -> float:
    """Ratio de contraste WCAG entre deux couleurs (1.0 a 21.0)."""
    la, lb = luminance(a), luminance(b)
    return (max(la, lb) + 0.05) / (min(la, lb) + 0.05)


def blend(a: QColor, b: QColor, ratio: float) -> QColor:
    """Melange lineaire de deux couleurs (ratio dans [0.0, 1.0])."""
    p = max(0.0, min(1.0, float(ratio)))
    q = 1.0 - p
    return QColor.fromRgbF(
        a.redF() * q + b.redF() * p, a.greenF() * q + b.greenF() * p, a.blueF() * q + b.blueF() * p,
    )


def compose(fond: QColor, encre: QColor, opacite: float) -> QColor:
    """Resout une couche translucide sur un fond reel avant mesure du contraste."""
    return blend(fond, encre, max(0.0, min(1.0, float(opacite))))


def readable_on(couleur: QColor, fond: QColor, seuil: float = _SEUIL_TEXTE) -> QColor:
    """Garde la teinte et renforce uniquement le contraste necessaire sur fond."""
    noir, blanc = QColor("#000000"), QColor("#ffffff")
    cible = blanc if contrast_ratio(blanc, fond) >= contrast_ratio(noir, fond) else noir
    for etape in range(_PAS_MELANGE + 1):
        candidat = blend(couleur, cible, etape / _PAS_MELANGE)
        if contrast_ratio(candidat, fond) >= seuil:
            return candidat
    return cible


def readable_on_all(couleur: QColor, fonds: Sequence[QColor], seuil: float = _SEUIL_TEXTE) -> QColor:
    """Garantit une meme encre lisible sur toutes les surfaces passees."""
    if not fonds:
        return couleur
    noir, blanc = QColor("#000000"), QColor("#ffffff")
    cible = max((noir, blanc), key=lambda c: min(contrast_ratio(c, f) for f in fonds))
    for etape in range(_PAS_MELANGE + 1):
        candidat = blend(couleur, cible, etape / _PAS_MELANGE)
        if all(contrast_ratio(candidat, f) >= seuil for f in fonds):
            return candidat
    return cible


def rgba_css(couleur: QColor, alpha: Optional[int] = None) -> str:
    """Formate une QColor en rgba(r, g, b, a) pour QSS."""
    a = couleur.alpha() if alpha is None else max(0, min(255, int(alpha)))
    return f"rgba({couleur.red()}, {couleur.green()}, {couleur.blue()}, {a})"


def first_available_font(candidates: Sequence[str]) -> str:
    """Premiere famille reellement installee parmi les candidats."""
    try:
        installed = set(QFontDatabase.families())
    except Exception:
        installed = set()
    for name in candidates:
        if name.strip() and name.strip() in installed:
            return name.strip()
    return candidates[0] if candidates else "Segoe UI"


def apply_named_weight(font: QFont, role: str) -> None:
    """Applique une graisse nommee compatible Qt 5 (0-99) et Qt 6 (100-900)."""
    if role == "bold":
        font.setBold(True)
        return
    if role == "demi":
        if is_qt6():
            w_enum = getattr(QFont, "Weight", None)
            demi = getattr(w_enum, "DemiBold", None) if w_enum else None
            if demi is not None:
                font.setWeight(demi)
                return
        font.setBold(True)
        return
    font.setBold(False)


def should_animate() -> bool:
    """Indique si les transitions visuelles sont actives (hors mouvement reduit)."""
    try:
        return not bool(QSettings().value("RecoverLand/mouvement_reduit", False, type=bool))
    except Exception:
        return True


@dataclass(frozen=True)
class ThemeTokens:
    """Ensemble complet des jetons visuels resolus depuis QPalette."""

    sombre: bool
    fond: QColor
    surface: QColor
    surface_douce: QColor
    surface_champ: QColor
    surface_survol: QColor
    surface_pressee: QColor
    texte: QColor
    secondaire: QColor
    tertiaire: QColor
    ligne: QColor
    ligne_controle: QColor
    ligne_selection: QColor
    accent: QColor
    texte_accent: QColor
    accent_doux: QColor
    accent_survol: QColor
    accent_presse: QColor
    ambre: QColor
    ambre_doux: QColor
    erreur: QColor
    erreur_douce: QColor
    acheve: QColor
    acheve_doux: QColor
    planifie: QColor
    planifie_doux: QColor
    police: str
    police_titre: str
    police_mono: str
    facteur_police: float


_CACHE_TOKENS: Dict[Tuple[int, int, int, int], ThemeTokens] = {}
_LAST_LOGGED_KEY: Optional[Tuple[int, int, int, int]] = None


def _palette_key(palette: QPalette) -> Tuple[int, int, int, int]:
    grp = QtCompat.PALETTE_ACTIVE
    return (
        palette.color(grp, QtCompat.PALETTE_WINDOW).rgba(),
        palette.color(grp, QtCompat.PALETTE_BASE).rgba(),
        palette.color(grp, QtCompat.PALETTE_WINDOW_TEXT).rgba(),
        palette.color(grp, QtCompat.PALETTE_HIGHLIGHT).rgba(),
    )


def _compute_semantics(sombre: bool, surface: QColor) -> Dict[str, QColor]:
    bruts = {
        "ambre": "#f6c177" if sombre else "#956000",
        "erreur": "#ff7b86" if sombre else "#b32639",
        "acheve": "#74d99f" if sombre else "#237843",
        "planifie": "#a78bfa" if sombre else "#7045ac",
    }
    res: Dict[str, QColor] = {}
    for cle, hex_val in bruts.items():
        teinte = readable_on(QColor(hex_val), surface, _SEUIL_ACCENT)
        doux = blend(surface, teinte, 0.12 if sombre else 0.10)
        res[cle] = readable_on(teinte, doux, _SEUIL_DOUX)
        res["erreur_douce" if cle == "erreur" else f"{cle}_doux"] = doux
    return res


def _build_tokens_from_palette(palette: QPalette) -> ThemeTokens:
    grp = QtCompat.PALETTE_ACTIVE
    fond = palette.color(grp, QtCompat.PALETTE_WINDOW)
    surface = palette.color(grp, QtCompat.PALETTE_BASE)
    sombre = luminance(fond) < _SEUIL_SOMBRE
    texte = readable_on(palette.color(grp, QtCompat.PALETTE_WINDOW_TEXT), fond)
    secondaire = readable_on(readable_on(blend(texte, fond, 0.24), fond), surface)
    tertiaire = readable_on(readable_on(blend(texte, fond, 0.42), fond), surface)
    accent = readable_on(palette.color(grp, QtCompat.PALETTE_HIGHLIGHT), surface, _SEUIL_ACCENT)
    noir, blanc = QColor("#000000"), QColor("#ffffff")
    texte_accent = blanc if contrast_ratio(blanc, accent) >= contrast_ratio(noir, accent) else noir
    accent_doux = blend(surface, accent, 0.12 if sombre else 0.10)
    accent = readable_on(accent, accent_doux, _SEUIL_DOUX)
    surface_douce = blend(surface, texte, 0.055)
    surface_survol = compose(surface, texte, 0.08)
    surface_pressee = compose(surface, texte, 0.13)
    sem = _compute_semantics(sombre, surface)
    fonds = (
        fond, surface, surface_douce, surface_survol, surface_pressee,
        accent_doux, sem["ambre_doux"], sem["erreur_douce"], sem["acheve_doux"], sem["planifie_doux"],
    )
    app = QApplication.instance()
    pts = app.font().pointSizeF() if app is not None else 9.0
    return ThemeTokens(
        sombre=sombre, fond=fond, surface=surface, surface_douce=surface_douce,
        surface_champ=surface, surface_survol=surface_survol, surface_pressee=surface_pressee,
        texte=readable_on_all(texte, fonds), secondaire=readable_on_all(secondaire, fonds),
        tertiaire=readable_on_all(tertiaire, fonds), ligne=blend(fond, texte, 0.22),
        ligne_controle=readable_on_all(blend(fond, texte, 0.32), fonds, _SEUIL_LIGNE),
        ligne_selection=readable_on_all(compose(surface, accent, 0.72), fonds, _SEUIL_LIGNE),
        accent=readable_on_all(accent, fonds), texte_accent=texte_accent, accent_doux=accent_doux,
        accent_survol=readable_on(compose(accent, texte_accent, 0.08), texte_accent),
        accent_presse=readable_on(compose(accent, texte_accent, 0.15), texte_accent),
        ambre=readable_on_all(sem["ambre"], fonds), ambre_doux=sem["ambre_doux"],
        erreur=readable_on_all(sem["erreur"], fonds), erreur_douce=sem["erreur_douce"],
        acheve=readable_on_all(sem["acheve"], fonds), acheve_doux=sem["acheve_doux"],
        planifie=readable_on_all(sem["planifie"], fonds), planifie_doux=sem["planifie_doux"],
        police=first_available_font(_CANDIDATS_TEXTE), police_titre=first_available_font(_CANDIDATS_TITRE),
        police_mono=first_available_font(_CANDIDATS_MONO), facteur_police=max(1.0, pts / 9.0) if pts > 0 else 1.0,
    )


def resolve_theme(widget: Optional[QWidget] = None) -> ThemeTokens:
    """Calcule ou retourne en cache les ThemeTokens depuis la palette QApplication."""
    global _LAST_LOGGED_KEY
    app = QApplication.instance()
    palette = app.palette() if app is not None else (widget.palette() if widget is not None else QPalette())
    key = _palette_key(palette)
    if key in _CACHE_TOKENS:
        return _CACHE_TOKENS[key]
    tokens = _build_tokens_from_palette(palette)
    if len(_CACHE_TOKENS) >= 16:
        _CACHE_TOKENS.clear()
    _CACHE_TOKENS[key] = tokens
    if key != _LAST_LOGGED_KEY:
        _LAST_LOGGED_KEY = key
        c_txt = contrast_ratio(tokens.texte, tokens.surface)
        flog(
            f"module=widgets.theme fn=resolve_theme sombre={tokens.sombre} "
            f"fond={tokens.fond.name()} surface={tokens.surface.name()} "
            f"texte={tokens.texte.name()} contrast_texte={c_txt:.2f}",
            "DEBUG",
        )
    return tokens


def build_dialog_stylesheet(t: ThemeTokens) -> str:
    """Construit la feuille QSS unifiee d'un dialogue selon les jetons VTT."""
    return (
        f"QDialog {{ background-color: {t.fond.name()}; color: {t.texte.name()}; font-family: '{t.police}'; }}"
        f"QLabel {{ background: transparent; border: none; color: {t.texte.name()}; }}"
        f"QFrame#statusFrame {{ background-color: transparent; border: none; }}"
        f"QPushButton {{ background-color: {t.surface.name()}; color: {t.texte.name()}; "
        f"border: 1px solid {t.ligne_controle.name()}; border-radius: {RAYON_CONTROLE}px; "
        f"padding: 4px 10px; font-weight: 600; }}"
        f"QPushButton#rlToolBtn {{ padding: 0px; background-color: {t.surface_douce.name()}; "
        f"border: 1px solid {t.ligne_controle.name()}; border-radius: {RAYON_CONTROLE}px; }}"
        f"QPushButton#rlToolBtn:hover {{ background-color: {t.accent_doux.name()}; border-color: {t.accent.name()}; }}"
        f"QPushButton:hover {{ background-color: {t.surface_survol.name()}; border-color: {t.accent.name()}; }}"
        f"QPushButton:pressed, QPushButton:checked {{ background-color: {t.accent_doux.name()}; "
        f"color: {t.accent.name()}; border: 1px solid {t.accent.name()}; }}"
        f"QPushButton:disabled {{ background-color: {t.surface_douce.name()}; "
        f"color: {t.tertiaire.name()}; border: 1px solid {t.ligne.name()}; }}"
        f"QComboBox, QLineEdit, QSpinBox, QDateEdit, QTimeEdit, QDateTimeEdit {{ "
        f"background-color: {t.surface_champ.name()}; color: {t.texte.name()}; "
        f"border: 1px solid {t.ligne_controle.name()}; "
        f"border-radius: {RAYON_CONTROLE}px; padding: 3px 8px; min-height: 22px; "
        f"selection-background-color: {t.accent.name()}; selection-color: {t.texte_accent.name()}; }}"
        f"QComboBox:hover, QLineEdit:hover, QSpinBox:hover, QDateEdit:hover, "
        f"QTimeEdit:hover, QDateTimeEdit:hover {{ border-color: {t.accent.name()}; }}"
        f"QComboBox:disabled, QLineEdit:disabled, QSpinBox:disabled, "
        f"QDateEdit:disabled, QTimeEdit:disabled, QDateTimeEdit:disabled {{ "
        f"background-color: {t.surface_douce.name()}; color: {t.tertiaire.name()}; border-color: {t.ligne.name()}; }}"
        f"QTableWidget, QListWidget {{ background-color: {t.surface.name()}; color: {t.texte.name()}; "
        f"border: 1px solid {t.ligne.name()}; border-radius: {RAYON_CONTROLE}px; gridline-color: {t.ligne.name()}; "
        f"selection-background-color: {t.accent_doux.name()}; selection-color: {t.texte.name()}; }}"
        f"QHeaderView::section {{ background-color: {t.surface_douce.name()}; color: {t.secondaire.name()}; "
        f"border: none; border-bottom: 1px solid {t.ligne.name()}; border-right: 1px solid {t.ligne.name()}; "
        f"padding: 5px 8px; font-weight: 600; }}"
        f"QProgressBar {{ background-color: {t.surface_douce.name()}; color: {t.texte.name()}; "
        f"border: 1px solid {t.ligne.name()}; border-radius: {RAYON_BLOC}px; "
        f"text-align: center; min-height: 8px; max-height: 14px; }}"
        f"QProgressBar::chunk {{ background-color: {t.accent.name()}; border-radius: 3px; }}"
        f"QCheckBox {{ color: {t.texte.name()}; spacing: 6px; }} QCheckBox:disabled {{ color: {t.tertiaire.name()}; }}"
        f"QToolTip {{ background-color: {t.surface_douce.name()}; color: {t.texte.name()}; "
        f"border: 1px solid {t.ligne.name()}; border-radius: {RAYON_CONTROLE}px; padding: 4px 8px; }}"
    )
