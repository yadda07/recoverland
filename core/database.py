"""Database operations mixin for RecoverLand plugin."""
from typing import Dict

from .constants import (
    PLUGIN_NAME, DB_CONNECT_TIMEOUT, DB_STATEMENT_TIMEOUT,
    SCHEMA_AUDIT_MAPPING, HAS_PSYCOPG2, psycopg2
)
from .logger import flog


class DatabaseMixin:
    """Mixin for database operations."""
    
    @staticmethod
    def create_connection(db_params: Dict[str, str], app_suffix: str = "") -> 'psycopg2.connection':
        """Create DB connection with standard params."""
        if not HAS_PSYCOPG2:
            raise ImportError("Le module psycopg2 est requis mais non installé. Installez-le via : pip install psycopg2-binary")
        flog(f"create_connection: start ({app_suffix}) host={db_params.get('host','?')} db={db_params.get('dbname','?')} user={db_params.get('user','?')}")
        required_keys = ("host", "port", "dbname", "user")
        missing = [k for k in required_keys if k not in db_params]
        if missing:
            flog(f"create_connection: MISSING KEYS {missing}", "ERROR")
            raise ValueError(f"Paramètres de connexion manquants: {', '.join(missing)}")
        app_name = f"{PLUGIN_NAME} {app_suffix}".strip()
        conn = psycopg2.connect(
            host=db_params["host"],
            port=db_params["port"],
            dbname=db_params["dbname"],
            user=db_params["user"],
            password=db_params.get("password", ""),
            connect_timeout=DB_CONNECT_TIMEOUT,
            application_name=app_name
        )
        flog(f"create_connection: OK ({app_suffix})")
        return conn
    
    @staticmethod
    def set_statement_timeout(cur: 'psycopg2.cursor', timeout_ms: int = DB_STATEMENT_TIMEOUT) -> None:
        """Set statement timeout on cursor."""
        cur.execute("SET statement_timeout = %s", (str(timeout_ms),))

    @staticmethod
    def get_audit_table(schema: str) -> str:
        """Get audit table name for schema."""
        return SCHEMA_AUDIT_MAPPING.get(schema, f"{schema}_json")
