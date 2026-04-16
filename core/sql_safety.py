"""SQL fragment safety assertion for defense-in-depth (SEC-04).

All dynamic SQL fragments (WHERE clauses, column lists) that are
interpolated via f-strings MUST pass through assert_safe_fragment()
before being used in conn.execute(). This catches accidental
injection of user-controlled data into SQL structure.
"""
import re

_SAFE_SQL_RE = re.compile(
    r"^[a-zA-Z0-9_\s,.*?=><!()|]+$"
)

_ALLOWED_KEYWORDS = frozenset({
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN",
    "IS", "NULL", "ORDER", "BY", "ASC", "DESC", "LIMIT",
    "OFFSET", "COUNT", "CASE", "WHEN", "THEN", "ELSE", "END",
    "SUM", "DISTINCT", "AS",
})


def assert_safe_fragment(fragment: str) -> str:
    """Validate that a SQL fragment contains only safe characters.

    Raises ValueError if the fragment contains suspicious patterns.
    Returns the fragment unchanged for chaining.
    """
    if not fragment:
        return fragment
    if not _SAFE_SQL_RE.match(fragment):
        raise ValueError(
            f"Unsafe SQL fragment detected: {fragment[:80]!r}"
        )
    return fragment
