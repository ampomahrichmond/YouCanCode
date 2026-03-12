"""
Data models for MetaHarvest
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


@dataclass
class ConnectionConfig:
    id:          str   = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name:        str   = ""
    source_type: str   = ""
    # Databricks
    host:        str   = ""
    token:       str   = ""
    http_path:   str   = ""
    catalog:     str   = ""
    # ADLS / Azure
    account_name:    str = ""
    account_key:     str = ""
    container:       str = ""
    tenant_id:       str = ""
    client_id:       str = ""
    client_secret:   str = ""
    # Synapse / SQL
    server:      str   = ""
    database:    str   = ""
    username:    str   = ""
    password:    str   = ""
    # Parquet
    folder_path: str   = ""
    # Hive
    hive_host:   str   = ""
    hive_port:   int   = 10000
    # Metadata
    created_at:  str   = field(default_factory=lambda: datetime.now().isoformat())
    last_tested: Optional[str] = None
    status:      str   = "untested"   # untested | ok | error

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: dict) -> "ConnectionConfig":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class ColumnMeta:
    name:          str
    data_type:     str
    nullable:      bool  = True
    description:   str   = ""
    is_primary_key: bool = False
    is_partition:  bool  = False
    ordinal:       int   = 0
    collibra_name: str   = ""   # transformed name for Collibra


@dataclass
class TableMeta:
    source_id:    str   = ""
    source_type:  str   = ""
    database:     str   = ""
    schema:       str   = ""
    table_name:   str   = ""
    full_path:    str   = ""
    object_type:  str   = "Table"      # Table | View | File | Directory
    row_count:    Optional[int] = None
    size_bytes:   Optional[int] = None
    description:  str   = ""
    owner:        str   = ""
    created_at:   Optional[str] = None
    updated_at:   Optional[str] = None
    columns:      List[ColumnMeta] = field(default_factory=list)
    tags:         Dict[str, str]   = field(default_factory=dict)
    properties:   Dict[str, Any]   = field(default_factory=dict)
    # Collibra mapping
    collibra_name:      str = ""
    collibra_community: str = ""
    collibra_domain:    str = ""
    collibra_asset_id:  str = ""
    selected:           bool = True

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
    scan_id:         str  = field(default_factory=lambda: str(uuid.uuid4())[:12])
    started_at:      str  = field(default_factory=lambda: datetime.now().isoformat())
    finished_at:     Optional[str] = None
    status:          str  = "running"   # running | complete | error | cancelled
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


@dataclass
class IngestionResult:
    scan_id:         str
    collibra_url:    str
    community_name:  str
    domain_name:     str
    started_at:      str  = field(default_factory=lambda: datetime.now().isoformat())
    finished_at:     Optional[str] = None
    status:          str  = "running"
    assets_created:  int  = 0
    assets_updated:  int  = 0
    assets_failed:   int  = 0
    relations_created: int = 0
    errors:          List[str] = field(default_factory=list)
    log_entries:     List[str] = field(default_factory=list)


@dataclass
class CollibraCommunity:
    id:   str
    name: str
    description: str = ""

@dataclass
class CollibraDomain:
    id:          str
    name:        str
    type_id:     str = ""
    type_name:   str = ""
    community_id: str = ""
    description: str = ""
