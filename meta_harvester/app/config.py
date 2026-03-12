# ─────────────────────────────────────────────────────────────
#  ENTERPRISE METADATA HARVESTER  ·  Config & Theme
# ─────────────────────────────────────────────────────────────

APP_NAME    = "MetaHarvest"
APP_SUBTITLE = "Enterprise Metadata Discovery & Collibra Ingestion"
APP_VERSION = "2.0.0"

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
    "accent_glow":  "#00C2FF30",
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

# ── Source Types ───────────────────────────────────────────────
SOURCE_TYPES = [
    {"id": "databricks",   "label": "Databricks",        "icon": "🧱", "color": C["databricks"]},
    {"id": "adls",         "label": "ADLS Gen2",          "icon": "🌊", "color": C["adls"]},
    {"id": "synapse",      "label": "Azure Synapse",      "icon": "⚡", "color": C["synapse"]},
    {"id": "parquet",      "label": "Parquet Files",      "icon": "📦", "color": C["parquet"]},
    {"id": "hive",         "label": "Hive Metastore",     "icon": "🐝", "color": C["hive"]},
    {"id": "azure_sql",    "label": "Azure SQL / MI",     "icon": "🗄️", "color": C["azure"]},
]

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
