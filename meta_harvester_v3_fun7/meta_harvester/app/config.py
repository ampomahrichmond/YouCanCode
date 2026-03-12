# ─────────────────────────────────────────────────────────────
#  ENTERPRISE METADATA HARVESTER  ·  Config & Theme  v3.0
# ─────────────────────────────────────────────────────────────

APP_NAME    = "MetaHarvest"
APP_SUBTITLE = "Enterprise Metadata Discovery & Collibra Ingestion"
APP_VERSION = "3.0.0"

# ── Color Palette ─────────────────────────────────────────────
C = {
    "bg_deep":      "#080C14",
    "bg_main":      "#0D1320",
    "bg_panel":     "#111827",
    "bg_card":      "#162032",
    "bg_hover":     "#1E2D45",
    "bg_input":     "#0A1628",
    "border":       "#1E3A5F",
    "border_light": "#2A4A70",

    "accent":       "#00C2FF",
    "accent_dim":   "#007EB8",
    "accent_glow":  "#0D2D3F",
    "purple":       "#7C5CFC",
    "purple_dim":   "#5A3FD4",
    "teal":         "#00D4AA",
    "amber":        "#F5A623",

    "success":      "#10D98A",
    "warning":      "#F5A623",
    "error":        "#FF4D6A",
    "info":         "#00C2FF",

    "text_primary": "#E8F4FF",
    "text_sec":     "#8BA8C8",
    "text_dim":     "#4A6580",
    "text_inv":     "#080C14",

    "databricks":   "#FF3621",
    "azure":        "#0078D4",
    "adls":         "#00B4D8",
    "synapse":      "#5C2D91",
    "parquet":      "#18A0FB",
    "hive":         "#FDEE21",
    "collibra":     "#F26522",
}

# ── Source Types (grouped) ─────────────────────────────────────
SOURCE_TYPES = [
    # Cloud / Big Data
    {"id": "databricks",  "label": "Databricks Unity",  "icon": "🧱", "color": C["databricks"], "group": "Cloud"},
    {"id": "adls",        "label": "ADLS Gen2",          "icon": "🌊", "color": C["adls"],       "group": "Cloud"},
    {"id": "synapse",     "label": "Azure Synapse",      "icon": "⚡", "color": C["synapse"],    "group": "Cloud"},
    {"id": "azure_sql",   "label": "Azure SQL / MI",     "icon": "🗄", "color": C["azure"],      "group": "Cloud"},
    {"id": "snowflake",   "label": "Snowflake",          "icon": "❄",  "color": "#29B5E8",       "group": "Cloud"},
    # Enterprise RDBMS
    {"id": "oracle",      "label": "Oracle DB",          "icon": "🔶", "color": "#F80000",       "group": "RDBMS"},
    {"id": "sqlserver",   "label": "SQL Server",         "icon": "🔷", "color": "#CC2927",       "group": "RDBMS"},
    {"id": "postgresql",  "label": "PostgreSQL",         "icon": "🐘", "color": "#336791",       "group": "RDBMS"},
    {"id": "mysql",       "label": "MySQL / MariaDB",    "icon": "🐬", "color": "#00758F",       "group": "RDBMS"},
    {"id": "db2",         "label": "IBM DB2",            "icon": "🔵", "color": "#052FAD",       "group": "RDBMS"},
    {"id": "teradata",    "label": "Teradata",           "icon": "🔺", "color": "#F37440",       "group": "RDBMS"},
    # Files
    {"id": "parquet",     "label": "Parquet Files",      "icon": "📦", "color": C["parquet"],    "group": "Files"},
    {"id": "csv",         "label": "CSV / Delimited",    "icon": "📄", "color": "#88C0D0",       "group": "Files"},
    # Hadoop
    {"id": "hive",        "label": "Hive Metastore",     "icon": "🐝", "color": C["hive"],       "group": "Hadoop"},
    {"id": "impala",      "label": "Cloudera Impala",    "icon": "⬡",  "color": "#FF7518",       "group": "Hadoop"},
    # Generic
    {"id": "odbc",        "label": "Generic ODBC",       "icon": "🔌", "color": "#7C5CFC",       "group": "Generic"},
]

# ── Environment Profiles ───────────────────────────────────────
ENVIRONMENTS = [
    {"id": "dev",  "label": "DEV",  "color": "#10D98A", "desc": "Development"},
    {"id": "sit",  "label": "SIT",  "color": "#00C2FF", "desc": "System Integration Test"},
    {"id": "uat",  "label": "UAT",  "color": "#F5A623", "desc": "User Acceptance Test"},
    {"id": "prod", "label": "PROD", "color": "#FF4D6A", "desc": "Production"},
    {"id": "dr",   "label": "DR",   "color": "#7C5CFC", "desc": "Disaster Recovery"},
]
ENV_COLORS = {e["id"]: e["color"] for e in ENVIRONMENTS}
ENV_IDS    = [e["id"]    for e in ENVIRONMENTS]
ENV_LABELS = [e["label"] for e in ENVIRONMENTS]

# ── Collibra Asset Type Mappings ───────────────────────────────
COLLIBRA_ASSET_TYPES = {
    "Database":     "00000000-0000-0000-0000-000000031006",
    "Schema":       "00000000-0000-0000-0000-000000031007",
    "Table":        "00000000-0000-0000-0001-000400000001",
    "Column":       "00000000-0000-0000-0000-000000031008",
    "View":         "00000000-0000-0000-0001-000400000002",
    "File":         "00000000-0000-0000-0001-000400000010",
    "Directory":    "00000000-0000-0000-0001-000400000011",
    "Data Asset":   "00000000-0000-0000-0001-000400000001",
}

COLLIBRA_RELATION_TYPES = {
    "table_in_schema":    "00000000-0000-0000-0000-000000007042",
    "schema_in_database": "00000000-0000-0000-0000-000000007043",
    "column_in_table":    "00000000-0000-0000-0000-000000007044",
}

# ── Name Transform Rules (Hive 18-char limit etc.) ─────────────
NAME_TRANSFORM_ABBREVS = {
    "description": "desc",   "information": "info",
    "department":  "dept",   "management":  "mgmt",
    "application": "app",    "transaction": "txn",
    "customer":    "cust",   "account":     "acct",
    "address":     "addr",   "reference":   "ref",
    "number":      "num",    "identifier":  "id",
    "created":     "crtd",   "updated":     "upd",
    "timestamp":   "ts",     "effective":   "eff",
    "expiration":  "exp",    "business":    "biz",
    "enterprise":  "ent",    "analytics":   "anlyt",
}

HIVE_MAX_LEN = 18

# ── DQ Check Definitions ───────────────────────────────────────
DQ_CHECKS = [
    {"id": "null_rate",       "label": "Null / Empty Rate",      "category": "Completeness"},
    {"id": "field_count",     "label": "Field Count Match",       "category": "Structural"},
    {"id": "field_names",     "label": "Field Name Match",        "category": "Structural"},
    {"id": "type_compat",     "label": "Data Type Compatibility", "category": "Structural"},
    {"id": "row_count",       "label": "Row Count Variance",      "category": "Volume"},
    {"id": "pk_uniqueness",   "label": "PK / Key Uniqueness",     "category": "Integrity"},
    {"id": "orphan_cols",     "label": "Dropped Columns",         "category": "Structural"},
    {"id": "new_cols",        "label": "New / Unexpected Columns","category": "Structural"},
    {"id": "collibra_drift",  "label": "Collibra Schema Drift",   "category": "Governance"},
]

DQ_THRESHOLDS = {
    "null_rate_warn":   0.05,   # >5% nulls = warn
    "null_rate_fail":   0.20,   # >20% nulls = fail
    "row_count_warn":   0.10,   # >10% row variance = warn
    "row_count_fail":   0.30,   # >30% row variance = fail
}
