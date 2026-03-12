# ⬡ MetaHarvest  
### Enterprise Metadata Discovery & Collibra Ingestion Platform

> **Harvest · Discover · Govern**  
> Databricks · ADLS Gen2 · Azure Synapse · Parquet · Hive Metastore → Collibra Data Catalog

---

## Features

| Feature | Details |
|---|---|
| **Multi-Source Connectors** | Databricks Unity Catalog, ADLS Gen2, Azure Synapse/SQL, Parquet files, Hive Metastore |
| **Hive 18-char Fix** | Auto-abbreviates long names to fit the Hive Metastore 18-character limit, with MD5 hash suffix for uniqueness |
| **Collibra REST v2** | Full asset + attribute + relation ingestion (Database → Schema → Table → Column hierarchy) |
| **Community/Domain Picker** | Browse and select any Collibra community & domain from a live dropdown |
| **Metadata Preview** | Review, filter, and rename every table before ingestion — select/deselect individually |
| **Scan History** | Persistent audit log of all scans (last 100) stored in `~/.metaharvest/` |
| **Beautiful Dark UI** | CustomTkinter with cyan/purple accent palette, progress bars, live log output |

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Minimum required (lightweight mode):
```bash
pip install customtkinter pyarrow requests
```

Optional per connector:
```bash
pip install databricks-sdk               # Databricks
pip install azure-storage-blob azure-identity  # ADLS Gen2
pip install pyodbc                       # Synapse / Azure SQL
pip install pyhive[hive]                 # Hive Metastore
```

### 2. Run the app

```bash
python main.py
```

---

## Connector Setup

### Databricks
- **Host**: `https://adb-1234567890.12.azuredatabricks.net`
- **Token**: Personal Access Token (`dapi...`)
- **HTTP Path**: Optional — your SQL warehouse path
- **Catalog**: Optional — leave blank to scan all Unity Catalog catalogs

### ADLS Gen2
- **Account Name**: Storage account name (without `.blob.core.windows.net`)
- **Account Key**: Storage account key (or use Service Principal below)
- **Service Principal**: Tenant ID + Client ID + Client Secret
- **Container**: Optional — leave blank to scan all containers

### Azure Synapse / SQL
- **Server**: `workspace.sql.azuresynapse.net` or `server.database.windows.net`
- **Database**: Target database name
- **Username / Password**: SQL credentials

### Parquet Files
- **Folder Path**: Local or mounted path to folder containing `.parquet` files
- Recursively scans all subdirectories

### Hive Metastore
- **Host**: Hive server hostname/IP
- **Port**: Default `10000`
- Automatically handles the **18-character name limit** via abbreviation + hash suffix

---

## Hive 18-Character Limit

MetaHarvest automatically transforms names that exceed the Hive Metastore limit:

| Original Name | Hive-Safe Name | Collibra Name |
|---|---|---|
| `customer_transaction_description` | `cust_txn_desc` | `Customer Transaction Description` |
| `my_very_long_table_name_in_hive` | `my_very_long__527c` | `My Very Long Table Name In Hive` |
| `sales_data` | `sales_data` | `Sales Data` |

The Collibra-ingested name always uses the full, human-readable form.

---

## Collibra Ingestion Flow

```
Scan Source
    ↓
Review & Edit (Metadata Preview page)
    ↓
Configure Target (Collibra URL + Community + Domain)
    ↓
Ingest (creates/updates assets + relations)
    ↓
Hierarchy:  Database → Schema → Table → Column
```

Asset types used:
- `Database`, `Schema`, `Table`, `View`, `Column`, `File`

Relations created:
- `Column → Table`, `Table → Schema`, `Schema → Database`

---

## Data Storage

All local data is stored in `~/.metaharvest/`:
```
~/.metaharvest/
  connections.json   ← saved connection configs (credentials included)
  settings.json      ← app settings
  scans.json         ← scan history (last 100 runs)
```

> ⚠️ Credentials are stored in plain JSON. Consider encrypting `~/.metaharvest/` at the OS level for production use.

---

## File Structure

```
meta_harvester/
├── main.py                          ← Entry point
├── requirements.txt
└── app/
    ├── config.py                    ← Colors, constants, asset type IDs
    ├── models.py                    ← Data classes (TableMeta, ColumnMeta, etc.)
    ├── storage.py                   ← JSON persistence
    ├── connectors/
    │   └── connectors.py            ← All source connectors + NameTransformer
    ├── collibra/
    │   └── client.py                ← Collibra REST API v2 client
    └── ui/
        └── app_window.py            ← Full UI (Sidebar, all pages, widgets)
```

---

## Extending

### Add a new connector
1. Create a class extending `BaseConnector` in `connectors.py`
2. Implement `test_connection()` and `scan()`
3. Register it in `_REGISTRY` at the bottom of `connectors.py`
4. Add it to `SOURCE_TYPES` in `config.py`

### Add a new Collibra asset type
1. Add the UUID to `COLLIBRA_ASSET_TYPES` in `config.py`
2. Use it in `client.py` `ingest_tables()`

---

*Built for the Data Discovery & Metadata Harvesting Team*  
*v2.0.0*
