# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""SQL fragment safety assertion for defense-in-depth (SEC-04).

All dynamic SQL fragments (WHERE clauses, column lists) that are
interpolated via f-strings MUST pass through assert_safe_fragment()
before being used in conn.execute(). This catches accidental
injection of user-controlled data into SQL structure.
"""
import re

_SAFE_SQL_RE = re.compile(
    r"^[a-zA-Z0-9_\s,.*?=><!()|]+\Z"
)

_FORBIDDEN_KEYWORDS = frozenset({
    "UNION", "DROP", "DELETE", "INSERT", "UPDATE", "ALTER",
    "CREATE", "ATTACH", "DETACH", "PRAGMA", "VACUUM", "REPLACE",
    "TRIGGER", "INDEX", "REINDEX", "ANALYZE",
})

_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def assert_safe_fragment(fragment: str) -> str:
    """Validate that a SQL fragment contains only safe characters and tokens.

    Raises ValueError if the fragment contains suspicious characters or
    mutation/DDL/compound-query keywords. Returns the fragment unchanged.
    """
    if not fragment:
        return fragment
    if not _SAFE_SQL_RE.match(fragment):
        raise ValueError(
            f"Unsafe SQL fragment detected: {fragment[:80]!r}"
        )
    for token in _WORD_RE.findall(fragment):
        if token.upper() in _FORBIDDEN_KEYWORDS:
            raise ValueError(
                f"Forbidden SQL keyword {token!r} in fragment: {fragment[:80]!r}"
            )
    return fragment
