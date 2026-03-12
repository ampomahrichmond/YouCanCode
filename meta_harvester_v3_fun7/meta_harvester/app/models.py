"""
Data models for MetaHarvest v3
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


@dataclass
class ConnectionConfig:
    id:           str  = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name:         str  = ""
    source_type:  str  = ""
    environment:  str  = "dev"      # dev | sit | uat | prod | dr
    # ── Universal / SQL ─────────────────────────────────────────
    host:         str  = ""
    port:         int  = 0
    database:     str  = ""
    username:     str  = ""
    password:     str  = ""
    schema:       str  = ""
    # ── Databricks ──────────────────────────────────────────────
    token:        str  = ""
    http_path:    str  = ""
    catalog:      str  = ""
    # ── Azure / ADLS ────────────────────────────────────────────
    account_name:  str = ""
    account_key:   str = ""
    container:     str = ""
    root_path:     str = ""        # ADLS: root folder to begin scanning from
    path_depth:    int = 2         # ADLS: how many path levels to use as schema
    tenant_id:     str = ""
    client_id:     str = ""
    client_secret: str = ""
    # ── Files ───────────────────────────────────────────────────
    folder_path:   str = ""
    file_pattern:  str = "*.parquet"
    delimiter:     str = ","       # CSV
    # ── Oracle ──────────────────────────────────────────────────
    service_name:  str = ""
    sid:           str = ""
    # ── DB2 ─────────────────────────────────────────────────────
    db2_database:  str = ""
    # ── Snowflake ───────────────────────────────────────────────
    account:       str = ""
    warehouse:     str = ""
    role:          str = ""
    # ── ODBC ────────────────────────────────────────────────────
    dsn:           str = ""
    odbc_driver:   str = ""
    extra_params:  str = ""        # key=value;key2=value2
    # ── Metadata ────────────────────────────────────────────────
    created_at:   str  = field(default_factory=lambda: datetime.now().isoformat())
    last_tested:  Optional[str] = None
    status:       str  = "untested"
    notes:        str  = ""

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: dict) -> "ConnectionConfig":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class ColumnMeta:
    name:           str
    data_type:      str
    nullable:       bool  = True
    description:    str   = ""
    is_primary_key: bool  = False
    is_partition:   bool  = False
    ordinal:        int   = 0
    collibra_name:  str   = ""
    char_length:    Optional[int] = None
    numeric_prec:   Optional[int] = None
    numeric_scale:  Optional[int] = None


@dataclass
class TableMeta:
    source_id:    str  = ""
    source_type:  str  = ""
    environment:  str  = "dev"
    database:     str  = ""
    schema:       str  = ""
    table_name:   str  = ""
    full_path:    str  = ""
    object_type:  str  = "Table"
    row_count:    Optional[int] = None
    size_bytes:   Optional[int] = None
    description:  str  = ""
    owner:        str  = ""
    created_at:   Optional[str] = None
    updated_at:   Optional[str] = None
    columns:      List[ColumnMeta] = field(default_factory=list)
    tags:         Dict[str, str]   = field(default_factory=dict)
    properties:   Dict[str, Any]   = field(default_factory=dict)
    collibra_name:      str  = ""
    collibra_community: str  = ""
    collibra_domain:    str  = ""
    collibra_asset_id:  str  = ""
    selected:     bool = True

    @property
    def display_name(self) -> str:
        parts = [p for p in [self.database, self.schema, self.table_name] if p]
        return ".".join(parts)

    @property
    def col_count(self) -> int:
        return len(self.columns)


@dataclass
class ScanResult:
    connection_id:   str
    connection_name: str
    source_type:     str
    environment:     str  = "dev"
    scan_id:         str  = field(default_factory=lambda: str(uuid.uuid4())[:12])
    started_at:      str  = field(default_factory=lambda: datetime.now().isoformat())
    finished_at:     Optional[str] = None
    status:          str  = "running"
    tables:          List[TableMeta] = field(default_factory=list)
    errors:          List[str]       = field(default_factory=list)
    warnings:        List[str]       = field(default_factory=list)

    @property
    def table_count(self)  -> int: return len(self.tables)
    @property
    def column_count(self) -> int: return sum(t.col_count for t in self.tables)
    @property
    def duration_sec(self) -> float:
        if not self.finished_at: return 0
        s = datetime.fromisoformat(self.started_at)
        e = datetime.fromisoformat(self.finished_at)
        return (e - s).total_seconds()


# ── DQ Models ─────────────────────────────────────────────────
@dataclass
class DQColumnResult:
    column_name:     str
    source_type:     str   = ""
    target_type:     str   = ""
    source_nullable: Optional[bool] = None
    target_nullable: Optional[bool] = None
    type_compatible: bool  = True
    in_source:       bool  = True
    in_target:       bool  = True
    null_rate:       Optional[float] = None
    issues:          List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if not self.in_source: return "dropped"
        if not self.in_target: return "new"
        if not self.type_compatible: return "type_mismatch"
        if self.issues:            return "warn"
        return "ok"


@dataclass
class DQTableResult:
    table_name:       str
    source_path:      str   = ""
    target_path:      str   = ""
    source_row_count: Optional[int]  = None
    target_row_count: Optional[int]  = None
    source_col_count: int   = 0
    target_col_count: int   = 0
    row_variance_pct: Optional[float] = None
    columns:          List[DQColumnResult] = field(default_factory=list)
    checks_passed:    int   = 0
    checks_warned:    int   = 0
    checks_failed:    int   = 0
    status:           str   = "pending"   # ok | warn | fail | pending

    @property
    def dropped_cols(self) -> List[str]:
        return [c.column_name for c in self.columns if c.status == "dropped"]

    @property
    def new_cols(self) -> List[str]:
        return [c.column_name for c in self.columns if c.status == "new"]

    @property
    def type_mismatches(self) -> List[DQColumnResult]:
        return [c for c in self.columns if c.status == "type_mismatch"]


@dataclass
class DQRunResult:
    run_id:       str  = field(default_factory=lambda: str(uuid.uuid4())[:12])
    scan_id:      str  = ""
    source_env:   str  = ""
    target_env:   str  = ""
    started_at:   str  = field(default_factory=lambda: datetime.now().isoformat())
    finished_at:  Optional[str] = None
    status:       str  = "running"
    tables:       List[DQTableResult] = field(default_factory=list)
    summary_pass: int  = 0
    summary_warn: int  = 0
    summary_fail: int  = 0

    @property
    def total_tables(self) -> int: return len(self.tables)
    @property
    def total_dropped_cols(self) -> int:
        return sum(len(t.dropped_cols) for t in self.tables)
    @property
    def total_type_issues(self) -> int:
        return sum(len(t.type_mismatches) for t in self.tables)


@dataclass
class IngestionResult:
    scan_id:          str
    collibra_url:     str
    community_name:   str
    domain_name:      str
    started_at:       str  = field(default_factory=lambda: datetime.now().isoformat())
    finished_at:      Optional[str] = None
    status:           str  = "running"
    assets_created:   int  = 0
    assets_updated:   int  = 0
    assets_failed:    int  = 0
    relations_created: int = 0
    errors:           List[str] = field(default_factory=list)
    log_entries:      List[str] = field(default_factory=list)


@dataclass
class CollibraCommunity:
    id:          str
    name:        str
    description: str = ""


@dataclass
class CollibraDomain:
    id:           str
    name:         str
    type_id:      str = ""
    type_name:    str = ""
    community_id: str = ""
    description:  str = ""
