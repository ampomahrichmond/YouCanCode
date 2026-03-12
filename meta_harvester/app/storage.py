"""
Persistence layer — saves connections, scan history, settings as JSON.
"""
import json, os
from pathlib import Path
from typing import List, Optional, Dict, Any
from app.models import ConnectionConfig, ScanResult, TableMeta, ColumnMeta

DATA_DIR = Path.home() / ".metaharvest"
CONN_FILE    = DATA_DIR / "connections.json"
SCANS_FILE   = DATA_DIR / "scans.json"
SETTINGS_FILE= DATA_DIR / "settings.json"


def _ensure():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


# ── Connections ────────────────────────────────────────────────
def load_connections() -> List[ConnectionConfig]:
    _ensure()
    if not CONN_FILE.exists(): return []
    try:
        data = json.loads(CONN_FILE.read_text())
        return [ConnectionConfig.from_dict(d) for d in data]
    except Exception:
        return []

def save_connections(conns: List[ConnectionConfig]):
    _ensure()
    CONN_FILE.write_text(json.dumps([c.to_dict() for c in conns], indent=2))

def upsert_connection(conn: ConnectionConfig):
    conns = load_connections()
    idx   = next((i for i, c in enumerate(conns) if c.id == conn.id), None)
    if idx is not None: conns[idx] = conn
    else:               conns.append(conn)
    save_connections(conns)

def delete_connection(conn_id: str):
    conns = [c for c in load_connections() if c.id != conn_id]
    save_connections(conns)


# ── Settings ───────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "collibra_url":      "",
    "collibra_username": "",
    "collibra_password": "",
    "default_community": "",
    "default_domain":    "",
    "ingest_columns":    True,
    "auto_transform_names": True,
    "scan_timeout":      300,
    "theme":             "dark",
}

def load_settings() -> Dict[str, Any]:
    _ensure()
    if not SETTINGS_FILE.exists(): return DEFAULT_SETTINGS.copy()
    try:
        saved = json.loads(SETTINGS_FILE.read_text())
        merged = DEFAULT_SETTINGS.copy()
        merged.update(saved)
        return merged
    except Exception:
        return DEFAULT_SETTINGS.copy()

def save_settings(settings: Dict[str, Any]):
    _ensure()
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2))


# ── Scan History ───────────────────────────────────────────────
def save_scan_summary(result: ScanResult):
    _ensure()
    history = []
    if SCANS_FILE.exists():
        try: history = json.loads(SCANS_FILE.read_text())
        except Exception: pass
    history.insert(0, {
        "scan_id":          result.scan_id,
        "connection_name":  result.connection_name,
        "source_type":      result.source_type,
        "started_at":       result.started_at,
        "finished_at":      result.finished_at,
        "status":           result.status,
        "table_count":      result.table_count,
        "column_count":     result.column_count,
        "error_count":      len(result.errors),
    })
    history = history[:100]  # keep last 100
    SCANS_FILE.write_text(json.dumps(history, indent=2))

def load_scan_history() -> List[Dict]:
    _ensure()
    if not SCANS_FILE.exists(): return []
    try: return json.loads(SCANS_FILE.read_text())
    except Exception: return []
