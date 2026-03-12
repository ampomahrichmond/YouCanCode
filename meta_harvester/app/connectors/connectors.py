"""
Connectors  ·  Databricks · ADLS · Synapse · Parquet · Hive · Azure SQL
Each connector returns List[TableMeta] and yields progress via a callback.
"""
from __future__ import annotations
import os, re, hashlib
from abc import ABC, abstractmethod
from typing import Callable, List, Optional
from pathlib import Path

from app.models import ConnectionConfig, TableMeta, ColumnMeta
from app.config import NAME_TRANSFORM_ABBREVS, HIVE_MAX_LEN


# ──────────────────────────────────────────────────────────────
#  Name Transformer  (handles Hive 18-char limit + clean names)
# ──────────────────────────────────────────────────────────────
class NameTransformer:
    @staticmethod
    def clean(name: str, max_len: int = 255, apply_hive: bool = False) -> str:
        if not name:
            return name
        n = name.strip()
        # Replace spaces / special chars with underscore
        n = re.sub(r"[^a-zA-Z0-9_]", "_", n)
        # Remove consecutive underscores
        n = re.sub(r"_+", "_", n).strip("_")
        if apply_hive or max_len == HIVE_MAX_LEN:
            n = NameTransformer._abbreviate(n)
            if len(n) > max_len:
                suffix = hashlib.md5(name.encode()).hexdigest()[:4]
                n = n[: max_len - 5] + "_" + suffix
        return n[:max_len]

    @staticmethod
    def _abbreviate(name: str) -> str:
        words = name.lower().split("_")
        out   = []
        for w in words:
            out.append(NAME_TRANSFORM_ABBREVS.get(w, w))
        return "_".join(out)

    @staticmethod
    def suggest_collibra_name(name: str) -> str:
        """Human-readable, untruncated version for Collibra."""
        n = re.sub(r"[^a-zA-Z0-9_ ]", " ", name).strip()
        n = re.sub(r"[ _]+", " ", n)
        return n.title()


# ──────────────────────────────────────────────────────────────
#  Base Connector
# ──────────────────────────────────────────────────────────────
class BaseConnector(ABC):
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._cancel = False

    def cancel(self): self._cancel = True

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Returns (ok, message)."""

    @abstractmethod
    def scan(
        self,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        filter_pattern: str = "*",
    ) -> List[TableMeta]:
        """Scan source and return TableMeta list.
        progress_cb(message, current, total)
        """

    def _emit(self, cb, msg, cur, tot):
        if cb:
            cb(msg, cur, tot)


# ──────────────────────────────────────────────────────────────
#  Databricks Connector
# ──────────────────────────────────────────────────────────────
class DatabricksConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            from databricks.sdk import WorkspaceClient
            w = self._client()
            _ = list(w.catalogs.list())
            return True, "Connected to Databricks workspace successfully."
        except ImportError:
            return False, "databricks-sdk not installed. Run: pip install databricks-sdk"
        except Exception as e:
            return False, str(e)

    def _client(self):
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=self.config.host.rstrip("/"),
            token=self.config.token,
        )

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        from databricks.sdk import WorkspaceClient
        w      = self._client()
        tables = []

        try:
            catalogs = [c.name for c in w.catalogs.list()
                        if not self.config.catalog or c.name == self.config.catalog]
        except Exception as e:
            raise RuntimeError(f"Cannot list catalogs: {e}")

        total_est = len(catalogs) * 10
        cur       = 0

        for cat_name in catalogs:
            if self._cancel: break
            try:
                schemas = list(w.schemas.list(catalog_name=cat_name))
            except Exception:
                continue

            for schema in schemas:
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {cat_name}.{schema.name}…", cur, total_est)
                try:
                    tbls = list(w.tables.list(catalog_name=cat_name, schema_name=schema.name))
                except Exception:
                    cur += 1
                    continue

                for tbl in tbls:
                    if self._cancel: break
                    tm = TableMeta(
                        source_id    = self.config.id,
                        source_type  = "databricks",
                        database     = cat_name,
                        schema       = schema.name,
                        table_name   = tbl.name or "",
                        full_path    = f"{cat_name}.{schema.name}.{tbl.name}",
                        object_type  = "View" if (tbl.table_type and "VIEW" in str(tbl.table_type).upper()) else "Table",
                        description  = tbl.comment or "",
                        owner        = tbl.owner or "",
                        created_at   = str(tbl.created_at) if tbl.created_at else None,
                        updated_at   = str(tbl.updated_at) if tbl.updated_at else None,
                    )
                    # Columns
                    if tbl.columns:
                        for i, col in enumerate(tbl.columns):
                            cm = ColumnMeta(
                                name         = col.name or "",
                                data_type    = str(col.type_name) if col.type_name else "STRING",
                                nullable     = col.nullable if col.nullable is not None else True,
                                description  = col.comment or "",
                                is_partition = col.partition_index is not None,
                                ordinal      = i,
                                collibra_name= NameTransformer.suggest_collibra_name(col.name or ""),
                            )
                            tm.columns.append(cm)
                    # Collibra name
                    tm.collibra_name = NameTransformer.suggest_collibra_name(tbl.name or "")
                    tables.append(tm)
                    cur += 1
                    if cur > total_est: total_est = cur + 10

        self._emit(progress_cb, f"Done — {len(tables)} objects found.", len(tables), len(tables))
        return tables


# ──────────────────────────────────────────────────────────────
#  ADLS Gen2 Connector
# ──────────────────────────────────────────────────────────────
class ADLSConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            svc = self._service_client()
            list(svc.list_containers(max_results=1))
            return True, "Connected to ADLS Gen2 successfully."
        except ImportError:
            return False, "azure-storage-blob not installed."
        except Exception as e:
            return False, str(e)

    def _service_client(self):
        from azure.storage.blob import BlobServiceClient
        if self.config.account_key:
            url = f"https://{self.config.account_name}.blob.core.windows.net"
            return BlobServiceClient(account_url=url, credential=self.config.account_key)
        from azure.identity import ClientSecretCredential
        cred = ClientSecretCredential(
            self.config.tenant_id, self.config.client_id, self.config.client_secret
        )
        url = f"https://{self.config.account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=cred)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import pyarrow.parquet as pq
        from io import BytesIO

        svc    = self._service_client()
        tables = []
        containers = (
            [self.config.container] if self.config.container
            else [c.name for c in svc.list_containers()]
        )

        total_est = 50
        cur       = 0

        for container_name in containers:
            if self._cancel: break
            cc = svc.get_container_client(container_name)
            try:
                blobs = list(cc.list_blobs())
            except Exception as ex:
                continue

            parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
            dirs_seen     = set()

            self._emit(progress_cb, f"Container {container_name}: {len(parquet_blobs)} parquet files…", cur, total_est)

            for blob in parquet_blobs:
                if self._cancel: break
                dir_path = str(Path(blob.name).parent)
                if dir_path in dirs_seen:
                    cur += 1
                    continue
                dirs_seen.add(dir_path)

                self._emit(progress_cb, f"Reading schema: {blob.name}", cur, len(parquet_blobs))

                columns = []
                try:
                    bc   = cc.get_blob_client(blob.name)
                    data = bc.download_blob().readall()
                    pf   = pq.read_schema(BytesIO(data))
                    for i, field in enumerate(pf):
                        columns.append(ColumnMeta(
                            name      = field.name,
                            data_type = str(field.type),
                            nullable  = field.nullable,
                            ordinal   = i,
                            collibra_name = NameTransformer.suggest_collibra_name(field.name),
                        ))
                except Exception:
                    pass

                file_name = Path(blob.name).stem
                tm = TableMeta(
                    source_id   = self.config.id,
                    source_type = "adls",
                    database    = self.config.account_name,
                    schema      = container_name,
                    table_name  = file_name,
                    full_path   = f"abfss://{container_name}@{self.config.account_name}.dfs.core.windows.net/{blob.name}",
                    object_type = "File",
                    size_bytes  = blob.size,
                    description = "",
                    columns     = columns,
                    collibra_name = NameTransformer.suggest_collibra_name(file_name),
                )
                tables.append(tm)
                cur += 1

        self._emit(progress_cb, f"Done — {len(tables)} objects found.", len(tables), len(tables))
        return tables


# ──────────────────────────────────────────────────────────────
#  Azure Synapse / SQL Connector
# ──────────────────────────────────────────────────────────────
class SynapseConnector(BaseConnector):
    def _conn_str(self):
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.config.server};"
            f"DATABASE={self.config.database};"
            f"UID={self.config.username};"
            f"PWD={self.config.password};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            import pyodbc
            conn = pyodbc.connect(self._conn_str(), timeout=10)
            conn.close()
            return True, "Connected to Azure Synapse / SQL successfully."
        except ImportError:
            return False, "pyodbc not installed."
        except Exception as e:
            return False, str(e)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import pyodbc
        conn   = pyodbc.connect(self._conn_str())
        cursor = conn.cursor()
        tables = []

        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        rows = cursor.fetchall()
        total = len(rows)

        for i, (schema, tname, ttype) in enumerate(rows):
            if self._cancel: break
            self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)

            # Get columns
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION,
                       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
                ORDER BY ORDINAL_POSITION
            """, schema, tname)

            cols = []
            for row in cursor.fetchall():
                dt = row.DATA_TYPE
                if row.CHARACTER_MAXIMUM_LENGTH:
                    dt += f"({row.CHARACTER_MAXIMUM_LENGTH})"
                elif row.NUMERIC_PRECISION:
                    dt += f"({row.NUMERIC_PRECISION})"
                cols.append(ColumnMeta(
                    name      = row.COLUMN_NAME,
                    data_type = dt,
                    nullable  = row.IS_NULLABLE == "YES",
                    ordinal   = row.ORDINAL_POSITION - 1,
                    collibra_name = NameTransformer.suggest_collibra_name(row.COLUMN_NAME),
                ))

            # Row count estimate
            row_count = None
            try:
                cursor.execute(f"SELECT SUM(p.rows) FROM sys.partitions p "
                               f"JOIN sys.tables t ON p.object_id=t.object_id "
                               f"JOIN sys.schemas s ON t.schema_id=s.schema_id "
                               f"WHERE s.name=? AND t.name=? AND p.index_id<2", schema, tname)
                rc = cursor.fetchone()
                row_count = int(rc[0]) if rc and rc[0] else None
            except Exception:
                pass

            tm = TableMeta(
                source_id   = self.config.id,
                source_type = "synapse",
                database    = self.config.database,
                schema      = schema,
                table_name  = tname,
                full_path   = f"{self.config.server}/{self.config.database}/{schema}/{tname}",
                object_type = "View" if ttype == "VIEW" else "Table",
                row_count   = row_count,
                columns     = cols,
                collibra_name = NameTransformer.suggest_collibra_name(tname),
            )
            tables.append(tm)

        conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects found.", total, total)
        return tables


# ──────────────────────────────────────────────────────────────
#  Parquet / Local Files Connector
# ──────────────────────────────────────────────────────────────
class ParquetConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        path = Path(self.config.folder_path)
        if not path.exists():
            return False, f"Path not found: {path}"
        files = list(path.rglob("*.parquet"))
        return True, f"Found {len(files)} parquet file(s) in {path}"

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import pyarrow.parquet as pq
        root   = Path(self.config.folder_path)
        files  = list(root.rglob("*.parquet"))
        tables = []

        for i, fpath in enumerate(files):
            if self._cancel: break
            self._emit(progress_cb, f"Reading {fpath.name}…", i, len(files))
            try:
                schema  = pq.read_schema(str(fpath))
                pf_file = pq.ParquetFile(str(fpath))
                meta    = pf_file.metadata

                cols = [
                    ColumnMeta(
                        name          = f.name,
                        data_type     = str(f.type),
                        nullable      = f.nullable,
                        ordinal       = j,
                        collibra_name = NameTransformer.suggest_collibra_name(f.name),
                    )
                    for j, f in enumerate(schema)
                ]
                tm = TableMeta(
                    source_id    = self.config.id,
                    source_type  = "parquet",
                    database     = root.name,
                    schema       = str(fpath.parent.relative_to(root)),
                    table_name   = fpath.stem,
                    full_path    = str(fpath),
                    object_type  = "File",
                    size_bytes   = fpath.stat().st_size,
                    row_count    = meta.num_rows,
                    columns      = cols,
                    collibra_name = NameTransformer.suggest_collibra_name(fpath.stem),
                )
                tables.append(tm)
            except Exception as ex:
                pass  # skip unreadable files

        self._emit(progress_cb, f"Done — {len(tables)} files scanned.", len(files), len(files))
        return tables


# ──────────────────────────────────────────────────────────────
#  Hive Metastore Connector  (with 18-char name transform)
# ──────────────────────────────────────────────────────────────
class HiveConnector(BaseConnector):
    def _conn(self):
        from pyhive import hive
        return hive.connect(
            host=self.config.hive_host,
            port=self.config.hive_port,
            username=self.config.username or "hive",
            auth="NONE",
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn   = self._conn()
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            conn.close()
            return True, "Connected to Hive Metastore successfully."
        except ImportError:
            return False, "pyhive not installed. Run: pip install pyhive[hive]"
        except Exception as e:
            return False, str(e)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._conn()
        cursor = conn.cursor()
        tables = []

        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]

        cur = 0
        for db in databases:
            if self._cancel: break
            try:
                cursor.execute(f"USE {db}")
                cursor.execute("SHOW TABLES")
                tbl_names = [row[0] for row in cursor.fetchall()]
            except Exception:
                continue

            for tname in tbl_names:
                if self._cancel: break
                self._emit(progress_cb, f"Describing {db}.{tname}…", cur, cur + 10)

                cols = []
                properties = {}
                try:
                    cursor.execute(f"DESCRIBE FORMATTED {tname}")
                    rows = cursor.fetchall()
                    in_cols = True
                    for row in rows:
                        col_name, col_type = (row[0] or "").strip(), (row[1] or "").strip()
                        if not col_name or col_name.startswith("#"):
                            in_cols = False
                            continue
                        if in_cols and col_type:
                            is_partition = False
                            hive_name    = NameTransformer.clean(col_name, HIVE_MAX_LEN, apply_hive=True)
                            cols.append(ColumnMeta(
                                name          = col_name,
                                data_type     = col_type,
                                is_partition  = is_partition,
                                collibra_name = NameTransformer.suggest_collibra_name(col_name),
                            ))
                except Exception:
                    pass

                # Original Hive name may exceed limit
                hive_safe_name = NameTransformer.clean(tname, HIVE_MAX_LEN, apply_hive=True)
                collibra_name  = NameTransformer.suggest_collibra_name(tname)

                tm = TableMeta(
                    source_id    = self.config.id,
                    source_type  = "hive",
                    database     = db,
                    schema       = db,
                    table_name   = tname,
                    full_path    = f"hive://{self.config.hive_host}:{self.config.hive_port}/{db}/{tname}",
                    object_type  = "Table",
                    columns      = cols,
                    collibra_name = collibra_name,
                    properties   = {
                        "hive_safe_name": hive_safe_name,
                        "original_name":  tname,
                        "name_truncated": len(tname) > HIVE_MAX_LEN,
                    },
                )
                tables.append(tm)
                cur += 1

        conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} tables found.", len(tables), len(tables))
        return tables


# ──────────────────────────────────────────────────────────────
#  Connector Factory
# ──────────────────────────────────────────────────────────────
_REGISTRY = {
    "databricks": DatabricksConnector,
    "adls":       ADLSConnector,
    "synapse":    SynapseConnector,
    "azure_sql":  SynapseConnector,
    "parquet":    ParquetConnector,
    "hive":       HiveConnector,
}

def get_connector(config: ConnectionConfig) -> BaseConnector:
    cls = _REGISTRY.get(config.source_type)
    if not cls:
        raise ValueError(f"Unknown source type: {config.source_type}")
    return cls(config)
