"""
Connectors v3  ─  Universal multi-source metadata harvester
Supports: Databricks · ADLS Gen2 · Synapse · Azure SQL · Oracle · SQL Server
          PostgreSQL · MySQL/MariaDB · IBM DB2 · Teradata · Snowflake
          Hive · Impala · Parquet · CSV · Generic ODBC
"""
from __future__ import annotations
import os, re, hashlib
from abc import ABC, abstractmethod
from typing import Callable, List, Optional
from pathlib import Path

from app.models import ConnectionConfig, TableMeta, ColumnMeta
from app.config import NAME_TRANSFORM_ABBREVS, HIVE_MAX_LEN


# ══════════════════════════════════════════════════════════════
#  Name Transformer
# ══════════════════════════════════════════════════════════════
class NameTransformer:
    @staticmethod
    def clean(name: str, max_len: int = 255, apply_hive: bool = False) -> str:
        if not name: return name
        n = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
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
        return "_".join(NAME_TRANSFORM_ABBREVS.get(w, w) for w in words)

    @staticmethod
    def suggest_collibra_name(name: str) -> str:
        n = re.sub(r"[^a-zA-Z0-9_ ]", " ", name).strip()
        n = re.sub(r"[ _]+", " ", n)
        return n.title()

    @staticmethod
    def normalize_adls_path(path: str, root_path: str = "", path_depth: int = 2) -> tuple[str, str, str]:
        """
        Given a full blob path and a root to strip, return (schema, table_name, display_path).
        path_depth controls how many levels below root are used as the schema.
        e.g. root=/raw/finance, depth=2, path=/raw/finance/2024/Q1/transactions.parquet
             => schema="2024/Q1", table="transactions"
        """
        # Strip root prefix
        rel = path
        if root_path:
            root_clean = root_path.strip("/")
            rel = re.sub(rf"^/?{re.escape(root_clean)}/?", "", path).strip("/")
        parts = Path(rel).parts
        if not parts:
            return "", Path(path).stem, path
        # Schema = first `path_depth` directory components (joined, safe)
        schema_parts = parts[: max(len(parts) - 1, path_depth)][:path_depth]
        schema = "/".join(schema_parts) if schema_parts else parts[0] if len(parts) > 1 else "root"
        table  = Path(parts[-1]).stem
        return schema, table, rel


# ══════════════════════════════════════════════════════════════
#  Base Connector
# ══════════════════════════════════════════════════════════════
class BaseConnector(ABC):
    def __init__(self, config: ConnectionConfig):
        self.config  = config
        self._cancel = False

    def cancel(self): self._cancel = True

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]: ...

    @abstractmethod
    def scan(self, progress_cb: Optional[Callable] = None,
             filter_pattern: str = "*") -> List[TableMeta]: ...

    def _emit(self, cb, msg, cur, tot):
        if cb: cb(msg, cur, tot)

    # ── Generic SQL helpers ────────────────────────────────────
    def _sql_columns(self, cursor, schema: str, table: str,
                     col_query: str, params: tuple) -> List[ColumnMeta]:
        try:
            cursor.execute(col_query, params)
            rows = cursor.fetchall()
            cols = []
            for r in rows:
                name     = r[0] or ""
                dtype    = r[1] or "UNKNOWN"
                nullable = True
                if len(r) > 2:
                    v = r[2]
                    nullable = str(v).upper() not in ("N", "NOT NULL", "NO", "0", "FALSE")
                ordinal  = r[3] if len(r) > 3 else len(cols)
                cols.append(ColumnMeta(
                    name          = name,
                    data_type     = dtype,
                    nullable      = nullable,
                    ordinal       = int(ordinal),
                    collibra_name = NameTransformer.suggest_collibra_name(name),
                ))
            return cols
        except Exception:
            return []

    def _make_table(self, source_type, db, schema, tname,
                    full_path, ttype="Table", cols=None,
                    row_count=None, size_bytes=None, desc="", owner="") -> TableMeta:
        return TableMeta(
            source_id    = self.config.id,
            source_type  = source_type,
            environment  = self.config.environment,
            database     = db,
            schema       = schema,
            table_name   = tname,
            full_path    = full_path,
            object_type  = ttype,
            row_count    = row_count,
            size_bytes   = size_bytes,
            description  = desc,
            owner        = owner,
            columns      = cols or [],
            collibra_name= NameTransformer.suggest_collibra_name(tname),
        )


# ══════════════════════════════════════════════════════════════
#  Generic SQL/ODBC base  (used by SQL Server, Oracle, etc.)
# ══════════════════════════════════════════════════════════════
class GenericSQLConnector(BaseConnector):
    """Sub-classes implement _get_connection() and the query strings."""

    INFO_SCHEMA_TABLES = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    INFO_SCHEMA_COLS = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION,
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
        ORDER BY ORDINAL_POSITION
    """
    ROWCOUNT_QUERY = None   # override per dialect

    def _get_connection(self):
        raise NotImplementedError

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection()
            conn.close()
            return True, f"Connected to {self.config.source_type.upper()} successfully."
        except ImportError as e:
            return False, f"Driver not installed: {e}"
        except Exception as e:
            return False, str(e)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        try:
            cursor.execute(self.INFO_SCHEMA_TABLES)
            rows  = cursor.fetchall()
            total = len(rows)
            for i, row in enumerate(rows):
                if self._cancel: break
                schema = row[0] or ""
                tname  = row[1] or ""
                ttype  = "View" if "VIEW" in str(row[2]).upper() else "Table"
                self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)

                # Columns
                cols = []
                try:
                    cursor.execute(self.INFO_SCHEMA_COLS, (schema, tname))
                    for r in cursor.fetchall():
                        dt = r[1] or "UNKNOWN"
                        if r[4]: dt += f"({r[4]})"
                        elif r[5] and r[6]: dt += f"({r[5]},{r[6]})"
                        elif r[5]: dt += f"({r[5]})"
                        cols.append(ColumnMeta(
                            name      = r[0], data_type = dt,
                            nullable  = str(r[2]).upper() not in ("NO","N","0"),
                            ordinal   = int(r[3]) - 1,
                            collibra_name = NameTransformer.suggest_collibra_name(r[0]),
                        ))
                except Exception: pass

                # Row count
                rc = None
                if self.ROWCOUNT_QUERY:
                    try:
                        cursor.execute(self.ROWCOUNT_QUERY, (schema, tname))
                        res = cursor.fetchone()
                        rc  = int(res[0]) if res and res[0] else None
                    except Exception: pass

                tm = self._make_table(
                    self.config.source_type, self.config.database,
                    schema, tname,
                    f"{self.config.host}/{self.config.database}/{schema}/{tname}",
                    ttype, cols, rc,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Databricks
# ══════════════════════════════════════════════════════════════
class DatabricksConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            from databricks.sdk import WorkspaceClient
            w = self._client()
            list(w.catalogs.list())
            return True, "Connected to Databricks workspace."
        except ImportError:
            return False, "databricks-sdk not installed."
        except Exception as e:
            return False, str(e)

    def _client(self):
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(host=self.config.host.rstrip("/"), token=self.config.token)

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
        cur = 0
        for cat_name in catalogs:
            if self._cancel: break
            try:    schemas = list(w.schemas.list(catalog_name=cat_name))
            except: continue
            for schema in schemas:
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {cat_name}.{schema.name}…", cur, total_est)
                try:    tbls = list(w.tables.list(catalog_name=cat_name, schema_name=schema.name))
                except: cur += 1; continue
                for tbl in tbls:
                    if self._cancel: break
                    cols = []
                    if tbl.columns:
                        for j, col in enumerate(tbl.columns):
                            cols.append(ColumnMeta(
                                name         = col.name or "",
                                data_type    = str(col.type_name) if col.type_name else "STRING",
                                nullable     = col.nullable if col.nullable is not None else True,
                                description  = col.comment or "",
                                is_partition = col.partition_index is not None,
                                ordinal      = j,
                                collibra_name= NameTransformer.suggest_collibra_name(col.name or ""),
                            ))
                    tm = self._make_table(
                        "databricks", cat_name, schema.name, tbl.name or "",
                        f"{cat_name}.{schema.name}.{tbl.name}",
                        "View" if tbl.table_type and "VIEW" in str(tbl.table_type).upper() else "Table",
                        cols, desc=tbl.comment or "", owner=tbl.owner or "",
                    )
                    tables.append(tm)
                    cur += 1
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  ADLS Gen2  (with deep-path handling)
# ══════════════════════════════════════════════════════════════
class ADLSConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            svc = self._service_client()
            list(svc.list_containers(max_results=1))
            return True, "Connected to ADLS Gen2."
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
            self.config.tenant_id, self.config.client_id, self.config.client_secret)
        url = f"https://{self.config.account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=cred)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import pyarrow.parquet as pq
        from io import BytesIO
        svc    = self._service_client()
        tables = []
        containers = ([self.config.container] if self.config.container
                      else [c.name for c in svc.list_containers()])
        root_path  = self.config.root_path.strip("/") if self.config.root_path else ""
        path_depth = max(1, self.config.path_depth or 2)

        for container_name in containers:
            if self._cancel: break
            cc = svc.get_container_client(container_name)
            try:
                prefix = root_path + "/" if root_path else None
                blobs  = list(cc.list_blobs(name_starts_with=prefix))
            except Exception:
                continue
            parquet_blobs = [b for b in blobs if b.name.lower().endswith(".parquet")]
            # Deduplicate by (schema, table_name) — only read schema from first file per partition
            seen_tables: dict[tuple, bool] = {}
            self._emit(progress_cb,
                       f"Container '{container_name}': {len(parquet_blobs)} parquet files…",
                       0, len(parquet_blobs))
            for idx, blob in enumerate(parquet_blobs):
                if self._cancel: break
                schema_str, tname, rel_path = NameTransformer.normalize_adls_path(
                    blob.name, root_path, path_depth)
                key = (container_name, schema_str, tname)
                if key in seen_tables:
                    continue
                seen_tables[key] = True
                self._emit(progress_cb, f"Reading schema: {rel_path}", idx, len(parquet_blobs))
                cols = []
                try:
                    bc   = cc.get_blob_client(blob.name)
                    data = bc.download_blob().readall()
                    pf   = pq.read_schema(BytesIO(data))
                    for i, f in enumerate(pf):
                        cols.append(ColumnMeta(
                            name      = f.name, data_type = str(f.type),
                            nullable  = f.nullable, ordinal = i,
                            collibra_name = NameTransformer.suggest_collibra_name(f.name),
                        ))
                except Exception: pass
                full_path = (f"abfss://{container_name}@{self.config.account_name}"
                             f".dfs.core.windows.net/{blob.name}")
                tm = self._make_table(
                    "adls", self.config.account_name, schema_str, tname,
                    full_path, "File", cols, size_bytes=blob.size,
                )
                tables.append(tm)
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Azure Synapse / SQL Server (pyodbc)
# ══════════════════════════════════════════════════════════════
class SynapseConnector(GenericSQLConnector):
    def _get_connection(self):
        import pyodbc
        cs = (f"DRIVER={{ODBC Driver 18 for SQL Server}};"
              f"SERVER={self.config.host};"
              f"DATABASE={self.config.database};"
              f"UID={self.config.username};PWD={self.config.password};"
              "Encrypt=yes;TrustServerCertificate=no;")
        return pyodbc.connect(cs, timeout=15)

    ROWCOUNT_QUERY = """
        SELECT SUM(p.rows) FROM sys.partitions p
        JOIN sys.tables t ON p.object_id=t.object_id
        JOIN sys.schemas s ON t.schema_id=s.schema_id
        WHERE s.name=? AND t.name=? AND p.index_id<2
    """


class SQLServerConnector(GenericSQLConnector):
    def _get_connection(self):
        import pyodbc
        driver = self.config.odbc_driver or "ODBC Driver 17 for SQL Server"
        cs = (f"DRIVER={{{driver}}};"
              f"SERVER={self.config.host},{self.config.port or 1433};"
              f"DATABASE={self.config.database};"
              f"UID={self.config.username};PWD={self.config.password};")
        return pyodbc.connect(cs, timeout=15)

    ROWCOUNT_QUERY = """
        SELECT SUM(p.rows) FROM sys.partitions p
        JOIN sys.tables t ON p.object_id=t.object_id
        JOIN sys.schemas s ON t.schema_id=s.schema_id
        WHERE s.name=? AND t.name=? AND p.index_id<2
    """


# ══════════════════════════════════════════════════════════════
#  Oracle
# ══════════════════════════════════════════════════════════════
class OracleConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection()
            conn.close()
            return True, "Connected to Oracle DB."
        except ImportError:
            return False, "cx_Oracle / oracledb not installed. Run: pip install oracledb"
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        try:
            import oracledb as cx
        except ImportError:
            import cx_Oracle as cx
        dsn = (self.config.service_name or self.config.database)
        if self.config.host:
            port = self.config.port or 1521
            if self.config.service_name:
                dsn = cx.makedsn(self.config.host, port,
                                 service_name=self.config.service_name)
            elif self.config.sid:
                dsn = cx.makedsn(self.config.host, port, sid=self.config.sid)
        return cx.connect(user=self.config.username,
                          password=self.config.password, dsn=dsn)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        target_schema = (self.config.schema or self.config.username or "").upper()
        try:
            if target_schema:
                cursor.execute(
                    "SELECT OWNER, TABLE_NAME, 'TABLE' FROM ALL_TABLES WHERE OWNER=:s "
                    "UNION ALL SELECT OWNER, VIEW_NAME, 'VIEW' FROM ALL_VIEWS WHERE OWNER=:s "
                    "ORDER BY 1, 2", s=target_schema)
            else:
                cursor.execute(
                    "SELECT OWNER, TABLE_NAME, 'TABLE' FROM ALL_TABLES "
                    "UNION ALL SELECT OWNER, VIEW_NAME, 'VIEW' FROM ALL_VIEWS ORDER BY 1, 2")
            rows  = cursor.fetchall()
            total = len(rows)
            for i, (owner, tname, ttype) in enumerate(rows):
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {owner}.{tname}…", i, total)
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE||
                        CASE WHEN DATA_PRECISION IS NOT NULL THEN '('||DATA_PRECISION||','||NVL(DATA_SCALE,0)||')'
                             WHEN CHAR_LENGTH > 0 THEN '('||CHAR_LENGTH||')' ELSE '' END,
                        NULLABLE, COLUMN_ID
                    FROM ALL_TAB_COLUMNS WHERE OWNER=:o AND TABLE_NAME=:t ORDER BY COLUMN_ID
                """, o=owner, t=tname)
                cols = [
                    ColumnMeta(name=r[0], data_type=r[1],
                               nullable=r[2]=="Y", ordinal=int(r[3])-1,
                               collibra_name=NameTransformer.suggest_collibra_name(r[0]))
                    for r in cursor.fetchall()
                ]
                rc = None
                try:
                    cursor.execute(
                        "SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER=:o AND TABLE_NAME=:t",
                        o=owner, t=tname)
                    res = cursor.fetchone()
                    rc  = int(res[0]) if res and res[0] else None
                except Exception: pass
                tm = self._make_table(
                    "oracle", self.config.database or owner, owner, tname,
                    f"{self.config.host}/{owner}/{tname}",
                    "View" if ttype == "VIEW" else "Table", cols, rc,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  PostgreSQL
# ══════════════════════════════════════════════════════════════
class PostgreSQLConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection(); conn.close()
            return True, "Connected to PostgreSQL."
        except ImportError:
            return False, "psycopg2 not installed. Run: pip install psycopg2-binary"
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        import psycopg2
        return psycopg2.connect(
            host=self.config.host, port=self.config.port or 5432,
            dbname=self.config.database, user=self.config.username,
            password=self.config.password, connect_timeout=15)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        target_schema = self.config.schema or None
        try:
            q = """SELECT table_schema, table_name, table_type
                   FROM information_schema.tables
                   WHERE table_schema NOT IN ('pg_catalog','information_schema')"""
            params = []
            if target_schema:
                q += " AND table_schema=%s"; params.append(target_schema)
            q += " ORDER BY table_schema, table_name"
            cursor.execute(q, params)
            rows = cursor.fetchall(); total = len(rows)
            for i, (schema, tname, ttype) in enumerate(rows):
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)
                cursor.execute("""
                    SELECT column_name, udt_name||
                        CASE WHEN character_maximum_length IS NOT NULL
                             THEN '('||character_maximum_length||')' ELSE '' END,
                        is_nullable, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position
                """, (schema, tname))
                cols = [
                    ColumnMeta(name=r[0], data_type=r[1],
                               nullable=r[2]=="YES", ordinal=int(r[3])-1,
                               collibra_name=NameTransformer.suggest_collibra_name(r[0]))
                    for r in cursor.fetchall()
                ]
                rc = None
                try:
                    cursor.execute(
                        "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n "
                        "ON c.relnamespace=n.oid WHERE n.nspname=%s AND c.relname=%s",
                        (schema, tname))
                    res = cursor.fetchone()
                    rc  = int(res[0]) if res and res[0] and res[0] > 0 else None
                except Exception: pass
                tm = self._make_table(
                    "postgresql", self.config.database, schema, tname,
                    f"{self.config.host}/{self.config.database}/{schema}/{tname}",
                    "View" if "VIEW" in ttype.upper() else "Table", cols, rc,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  MySQL / MariaDB
# ══════════════════════════════════════════════════════════════
class MySQLConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection(); conn.close()
            return True, "Connected to MySQL/MariaDB."
        except ImportError:
            return False, "mysql-connector-python not installed. Run: pip install mysql-connector-python"
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        import mysql.connector
        return mysql.connector.connect(
            host=self.config.host, port=self.config.port or 3306,
            database=self.config.database, user=self.config.username,
            password=self.config.password, connection_timeout=15)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        try:
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TABLE_ROWS
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME
            """)
            rows = cursor.fetchall(); total = len(rows)
            for i, (schema, tname, ttype, trows) in enumerate(rows):
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)
                cursor.execute("""
                    SELECT COLUMN_NAME,
                        COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION
                """, (schema, tname))
                cols = [
                    ColumnMeta(name=r[0], data_type=r[1],
                               nullable=r[2]=="YES", ordinal=int(r[3])-1,
                               collibra_name=NameTransformer.suggest_collibra_name(r[0]))
                    for r in cursor.fetchall()
                ]
                tm = self._make_table(
                    "mysql", schema, schema, tname,
                    f"{self.config.host}/{schema}/{tname}",
                    "View" if "VIEW" in ttype.upper() else "Table",
                    cols, trows,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  IBM DB2
# ══════════════════════════════════════════════════════════════
class DB2Connector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection(); conn.close()
            return True, "Connected to IBM DB2."
        except ImportError:
            return False, "ibm_db_dbi not installed. Run: pip install ibm_db"
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        import ibm_db_dbi as db2
        cs = (f"DATABASE={self.config.database};"
              f"HOSTNAME={self.config.host};"
              f"PORT={self.config.port or 50000};"
              f"PROTOCOL=TCPIP;"
              f"UID={self.config.username};"
              f"PWD={self.config.password};")
        return db2.connect(cs, "", "")

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        target_schema = (self.config.schema or self.config.username or "").upper()
        try:
            q = ("SELECT TABSCHEMA, TABNAME, TYPE FROM SYSCAT.TABLES WHERE TYPE IN ('T','V')")
            if target_schema:
                q += f" AND TABSCHEMA='{target_schema}'"
            q += " ORDER BY TABSCHEMA, TABNAME"
            cursor.execute(q)
            rows = cursor.fetchall(); total = len(rows)
            for i, (schema, tname, ttype) in enumerate(rows):
                schema = schema.strip(); tname = tname.strip()
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)
                cursor.execute(f"""
                    SELECT COLNAME, TYPENAME||
                        CASE WHEN LENGTH>0 THEN '('||CAST(LENGTH AS VARCHAR(20))||')' ELSE '' END,
                        NULLS, COLNO
                    FROM SYSCAT.COLUMNS WHERE TABSCHEMA='{schema}' AND TABNAME='{tname}'
                    ORDER BY COLNO
                """)
                cols = [
                    ColumnMeta(name=r[0].strip(), data_type=r[1].strip(),
                               nullable=r[2]=="Y", ordinal=int(r[3]),
                               collibra_name=NameTransformer.suggest_collibra_name(r[0].strip()))
                    for r in cursor.fetchall()
                ]
                tm = self._make_table(
                    "db2", self.config.database, schema, tname,
                    f"{self.config.host}/{self.config.database}/{schema}/{tname}",
                    "View" if ttype == "V" else "Table", cols,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Teradata
# ══════════════════════════════════════════════════════════════
class TeradataConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection(); conn.close()
            return True, "Connected to Teradata."
        except ImportError:
            return False, "teradatasql not installed. Run: pip install teradatasql"
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        import teradatasql
        return teradatasql.connect(
            host=self.config.host, user=self.config.username,
            password=self.config.password, database=self.config.database or None)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        target_db = (self.config.database or "").strip()
        try:
            q = "SELECT DatabaseName, TableName, TableKind FROM DBC.TablesV WHERE TableKind IN ('T','V')"
            if target_db:
                q += f" AND DatabaseName='{target_db}'"
            q += " ORDER BY DatabaseName, TableName"
            cursor.execute(q)
            rows = cursor.fetchall(); total = len(rows)
            for i, (dbname, tname, tkind) in enumerate(rows):
                dbname = (dbname or "").strip(); tname = (tname or "").strip()
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {dbname}.{tname}…", i, total)
                cursor.execute(f"""
                    SELECT ColumnName, ColumnType, Nullable, ColumnId
                    FROM DBC.ColumnsV
                    WHERE DatabaseName='{dbname}' AND TableName='{tname}'
                    ORDER BY ColumnId
                """)
                cols = [
                    ColumnMeta(name=(r[0] or "").strip(),
                               data_type=(r[1] or "").strip(),
                               nullable=r[2]=="Y", ordinal=int(r[3]),
                               collibra_name=NameTransformer.suggest_collibra_name((r[0] or "").strip()))
                    for r in cursor.fetchall()
                ]
                tm = self._make_table(
                    "teradata", dbname, dbname, tname,
                    f"td://{self.config.host}/{dbname}/{tname}",
                    "View" if tkind == "V" else "Table", cols,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Snowflake
# ══════════════════════════════════════════════════════════════
class SnowflakeConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._get_connection(); conn.close()
            return True, "Connected to Snowflake."
        except ImportError:
            return False, "snowflake-connector-python not installed."
        except Exception as e:
            return False, str(e)

    def _get_connection(self):
        import snowflake.connector
        params = dict(
            user=self.config.username, password=self.config.password,
            account=self.config.account,
        )
        if self.config.warehouse: params["warehouse"] = self.config.warehouse
        if self.config.database:  params["database"]  = self.config.database
        if self.config.schema:    params["schema"]     = self.config.schema
        if self.config.role:      params["role"]       = self.config.role
        return snowflake.connector.connect(**params)

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._get_connection()
        cursor = conn.cursor()
        tables = []
        target_db  = (self.config.database or "").upper()
        target_sch = (self.config.schema   or "").upper()
        try:
            q = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT FROM INFORMATION_SCHEMA.TABLES"
            conds = []
            if target_db:  conds.append(f"TABLE_CATALOG='{target_db}'")
            if target_sch: conds.append(f"TABLE_SCHEMA='{target_sch}'")
            if conds: q += " WHERE " + " AND ".join(conds)
            q += " ORDER BY TABLE_SCHEMA, TABLE_NAME"
            cursor.execute(q)
            rows = cursor.fetchall(); total = len(rows)
            for i, (cat, schema, tname, ttype, rc) in enumerate(rows):
                if self._cancel: break
                self._emit(progress_cb, f"Scanning {schema}.{tname}…", i, total)
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE||
                        CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                             THEN '('||CHARACTER_MAXIMUM_LENGTH||')' ELSE '' END,
                        IS_NULLABLE, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG='{cat}' AND TABLE_SCHEMA='{schema}' AND TABLE_NAME='{tname}'
                    ORDER BY ORDINAL_POSITION
                """)
                cols = [
                    ColumnMeta(name=r[0], data_type=r[1],
                               nullable=r[2]=="YES", ordinal=int(r[3])-1,
                               collibra_name=NameTransformer.suggest_collibra_name(r[0]))
                    for r in cursor.fetchall()
                ]
                tm = self._make_table(
                    "snowflake", cat or target_db, schema, tname,
                    f"snowflake://{self.config.account}/{cat}/{schema}/{tname}",
                    "View" if "VIEW" in (ttype or "").upper() else "Table",
                    cols, int(rc) if rc else None,
                )
                tables.append(tm)
        finally:
            conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} objects.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Hive Metastore  (18-char fix)
# ══════════════════════════════════════════════════════════════
class HiveConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn(); conn.cursor().execute("SHOW DATABASES"); conn.close()
            return True, "Connected to Hive Metastore."
        except ImportError:
            return False, "pyhive not installed. Run: pip install pyhive[hive]"
        except Exception as e:
            return False, str(e)

    def _conn(self):
        from pyhive import hive
        return hive.connect(host=self.config.host, port=self.config.port or 10000,
                            username=self.config.username or "hive", auth="NONE")

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        conn   = self._conn()
        cursor = conn.cursor()
        tables = []
        cursor.execute("SHOW DATABASES")
        databases = [r[0] for r in cursor.fetchall()]
        cur = 0
        for db in databases:
            if self._cancel: break
            try:
                cursor.execute(f"USE {db}")
                cursor.execute("SHOW TABLES")
                tbl_names = [r[0] for r in cursor.fetchall()]
            except Exception: continue
            for tname in tbl_names:
                if self._cancel: break
                self._emit(progress_cb, f"Describing {db}.{tname}…", cur, cur + 10)
                cols = []
                try:
                    cursor.execute(f"DESCRIBE FORMATTED {tname}")
                    in_cols = True
                    for row in cursor.fetchall():
                        cn, ct = (row[0] or "").strip(), (row[1] or "").strip()
                        if not cn or cn.startswith("#"): in_cols = False; continue
                        if in_cols and ct:
                            cols.append(ColumnMeta(
                                name=cn, data_type=ct,
                                collibra_name=NameTransformer.suggest_collibra_name(cn)))
                except Exception: pass
                hive_safe = NameTransformer.clean(tname, HIVE_MAX_LEN, apply_hive=True)
                tm = self._make_table(
                    "hive", db, db, tname,
                    f"hive://{self.config.host}:{self.config.port or 10000}/{db}/{tname}",
                    "Table", cols,
                )
                tm.properties["hive_safe_name"]  = hive_safe
                tm.properties["original_name"]   = tname
                tm.properties["name_truncated"]  = len(tname) > HIVE_MAX_LEN
                tables.append(tm)
                cur += 1
        conn.close()
        self._emit(progress_cb, f"Done — {len(tables)} tables.", len(tables), len(tables))
        return tables


# ══════════════════════════════════════════════════════════════
#  Parquet Files
# ══════════════════════════════════════════════════════════════
class ParquetConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        path = Path(self.config.folder_path)
        if not path.exists(): return False, f"Path not found: {path}"
        n = len(list(path.rglob("*.parquet")))
        return True, f"Found {n} parquet file(s) in {path}"

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import pyarrow.parquet as pq
        root  = Path(self.config.folder_path)
        files = list(root.rglob("*.parquet"))
        tables = []
        for i, fpath in enumerate(files):
            if self._cancel: break
            self._emit(progress_cb, f"Reading {fpath.name}…", i, len(files))
            try:
                schema  = pq.read_schema(str(fpath))
                pf_file = pq.ParquetFile(str(fpath))
                cols = [
                    ColumnMeta(name=f.name, data_type=str(f.type),
                               nullable=f.nullable, ordinal=j,
                               collibra_name=NameTransformer.suggest_collibra_name(f.name))
                    for j, f in enumerate(schema)
                ]
                rel_schema = str(fpath.parent.relative_to(root)) or "root"
                tm = self._make_table(
                    "parquet", root.name, rel_schema, fpath.stem,
                    str(fpath), "File", cols,
                    row_count=pf_file.metadata.num_rows,
                    size_bytes=fpath.stat().st_size,
                )
                tables.append(tm)
            except Exception: pass
        self._emit(progress_cb, f"Done — {len(tables)} files.", len(files), len(files))
        return tables


# ══════════════════════════════════════════════════════════════
#  CSV Files
# ══════════════════════════════════════════════════════════════
class CSVConnector(BaseConnector):
    def test_connection(self) -> tuple[bool, str]:
        path = Path(self.config.folder_path)
        if not path.exists(): return False, f"Path not found: {path}"
        n = len(list(path.rglob("*.csv")))
        return True, f"Found {n} CSV file(s) in {path}"

    def scan(self, progress_cb=None, filter_pattern="*") -> List[TableMeta]:
        import csv
        root   = Path(self.config.folder_path)
        files  = list(root.rglob("*.csv"))
        tables = []
        delim  = self.config.delimiter or ","
        for i, fpath in enumerate(files):
            if self._cancel: break
            self._emit(progress_cb, f"Reading {fpath.name}…", i, len(files))
            try:
                with open(fpath, newline="", encoding="utf-8-sig", errors="ignore") as f:
                    reader = csv.reader(f, delimiter=delim)
                    headers = next(reader, [])
                    rows    = list(reader)
                cols = [
                    ColumnMeta(name=h.strip(), data_type="VARCHAR",
                               ordinal=j,
                               collibra_name=NameTransformer.suggest_collibra_name(h.strip()))
                    for j, h in enumerate(headers) if h.strip()
                ]
                rel_schema = str(fpath.parent.relative_to(root)) or "root"
                tm = self._make_table(
                    "csv", root.name, rel_schema, fpath.stem,
                    str(fpath), "File", cols,
                    row_count=len(rows), size_bytes=fpath.stat().st_size,
                )
                tables.append(tm)
            except Exception: pass
        self._emit(progress_cb, f"Done — {len(tables)} files.", len(files), len(files))
        return tables


# ══════════════════════════════════════════════════════════════
#  Generic ODBC
# ══════════════════════════════════════════════════════════════
class ODBCConnector(GenericSQLConnector):
    def _get_connection(self):
        import pyodbc
        if self.config.dsn:
            cs = f"DSN={self.config.dsn};UID={self.config.username};PWD={self.config.password};"
        else:
            cs = (f"DRIVER={{{self.config.odbc_driver}}};"
                  f"SERVER={self.config.host},{self.config.port or 1433};"
                  f"DATABASE={self.config.database};"
                  f"UID={self.config.username};PWD={self.config.password};")
        if self.config.extra_params:
            cs += self.config.extra_params.rstrip(";") + ";"
        return pyodbc.connect(cs, timeout=15)


# ══════════════════════════════════════════════════════════════
#  Connector Factory
# ══════════════════════════════════════════════════════════════
_REGISTRY = {
    "databricks":  DatabricksConnector,
    "adls":        ADLSConnector,
    "synapse":     SynapseConnector,
    "azure_sql":   SynapseConnector,
    "sqlserver":   SQLServerConnector,
    "oracle":      OracleConnector,
    "postgresql":  PostgreSQLConnector,
    "mysql":       MySQLConnector,
    "db2":         DB2Connector,
    "teradata":    TeradataConnector,
    "snowflake":   SnowflakeConnector,
    "hive":        HiveConnector,
    "impala":      HiveConnector,    # Impala uses same HiveServer2 protocol
    "parquet":     ParquetConnector,
    "csv":         CSVConnector,
    "odbc":        ODBCConnector,
}

def get_connector(config: ConnectionConfig) -> BaseConnector:
    cls = _REGISTRY.get(config.source_type)
    if not cls:
        raise ValueError(f"Unknown source type: {config.source_type}")
    return cls(config)
