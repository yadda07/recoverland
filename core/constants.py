"""Constants and configuration for RecoverLand plugin."""
from typing import Dict

# Database timeouts
DB_CONNECT_TIMEOUT = 30
DB_STATEMENT_TIMEOUT = 300000  # 5 min in ms
THREAD_STOP_TIMEOUT = 5000  # 5 sec

# Plugin identity
PLUGIN_NAME = "RecoverLand"

# Protected psycopg2 import
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    psycopg2 = None
    HAS_PSYCOPG2 = False

# Schema to audit table mapping
SCHEMA_AUDIT_MAPPING: Dict[str, str] = {
    'rip_avg_nge': 'rip_avg_json',
    'rbal': 'rbal_json',
    'geofibre': 'geofibre_json',
    'aerien': 'aerien_json',
    'gc_exe': 'gc_exe_json',
    'gc': 'gc_json',
    'aiguillage et POT': 'aig_pot_json'
}

AVAILABLE_SCHEMAS = list(SCHEMA_AUDIT_MAPPING.keys())
