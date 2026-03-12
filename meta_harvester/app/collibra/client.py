"""
Collibra REST API v2 Client
Handles communities, domains, asset types, assets, attributes, and relations.
"""
from __future__ import annotations
import requests, time, logging
from typing import List, Optional, Dict, Any, Callable
from app.models import (
    TableMeta, ColumnMeta, CollibraCommunity, CollibraDomain,
    IngestionResult
)
from app.config import COLLIBRA_ASSET_TYPES, COLLIBRA_RELATION_TYPES

log = logging.getLogger(__name__)


class CollibraClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url  = base_url.rstrip("/")
        self.api_base  = f"{self.base_url}/rest/2.0"
        self.session   = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })
        self._cancel = False

    def cancel(self): self._cancel = True

    # ── Auth ───────────────────────────────────────────────────
    def test_connection(self) -> tuple[bool, str]:
        try:
            r = self.session.get(f"{self.api_base}/communities", params={"limit": 1}, timeout=15)
            r.raise_for_status()
            return True, "Connected to Collibra successfully."
        except requests.exceptions.ConnectionError:
            return False, f"Cannot reach Collibra at {self.base_url}"
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                return False, "Invalid credentials."
            return False, f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        except Exception as e:
            return False, str(e)

    # ── Communities ────────────────────────────────────────────
    def get_communities(self) -> List[CollibraCommunity]:
        r = self._get("/communities", {"limit": 500})
        return [
            CollibraCommunity(id=c["id"], name=c["name"],
                              description=c.get("description",""))
            for c in r.get("results", [])
        ]

    # ── Domains ────────────────────────────────────────────────
    def get_domains(self, community_id: str) -> List[CollibraDomain]:
        r = self._get("/domains", {"communityId": community_id, "limit": 500})
        return [
            CollibraDomain(
                id           = d["id"],
                name         = d["name"],
                type_id      = d.get("type", {}).get("id", ""),
                type_name    = d.get("type", {}).get("name", ""),
                community_id = community_id,
                description  = d.get("description", ""),
            )
            for d in r.get("results", [])
        ]

    def get_or_create_domain(self, community_id: str, domain_name: str,
                              type_id: str = "00000000-0000-0000-0000-000000030023") -> str:
        domains = self.get_domains(community_id)
        existing = next((d for d in domains if d.name.lower() == domain_name.lower()), None)
        if existing:
            return existing.id
        r = self._post("/domains", {
            "name":        domain_name,
            "communityId": community_id,
            "typeId":      type_id,
        })
        return r["id"]

    # ── Assets ─────────────────────────────────────────────────
    def find_asset(self, domain_id: str, name: str, type_id: str) -> Optional[str]:
        """Find existing asset by name+type in domain. Returns asset id or None."""
        r = self._get("/assets", {
            "domainId":   domain_id,
            "name":       name,
            "typeId":     type_id,
            "limit":      1,
        })
        results = r.get("results", [])
        return results[0]["id"] if results else None

    def upsert_asset(self, domain_id: str, name: str, type_id: str,
                     display_name: str = "", status_id: str = "") -> tuple[str, bool]:
        """Returns (asset_id, created)."""
        existing_id = self.find_asset(domain_id, name, type_id)
        if existing_id:
            self._patch(f"/assets/{existing_id}", {"displayName": display_name or name})
            return existing_id, False
        payload: Dict[str, Any] = {
            "name":     name,
            "typeId":   type_id,
            "domainId": domain_id,
        }
        if display_name: payload["displayName"] = display_name
        r = self._post("/assets", payload)
        return r["id"], True

    def add_attribute(self, asset_id: str, type_id: str, value: str):
        """Add or update a string attribute on an asset."""
        if not value: return
        # Check existing
        existing = self._get("/attributes", {"assetId": asset_id, "typeId": type_id})
        if existing.get("results"):
            attr_id = existing["results"][0]["id"]
            self._patch(f"/attributes/{attr_id}", {"value": value})
        else:
            self._post("/attributes", {
                "assetId": asset_id,
                "typeId":  type_id,
                "value":   value,
            })

    def add_relation(self, source_id: str, target_id: str, type_id: str):
        """Create a relation between two assets if it doesn't exist."""
        try:
            existing = self._get("/relations", {
                "sourceId":  source_id,
                "targetId":  target_id,
                "typeId":    type_id,
                "limit":     1,
            })
            if existing.get("results"):
                return existing["results"][0]["id"]
            r = self._post("/relations", {
                "sourceId": source_id,
                "targetId": target_id,
                "typeId":   type_id,
            })
            return r["id"]
        except Exception as e:
            log.warning(f"Relation failed: {e}")
            return None

    # ── Bulk Ingestion ─────────────────────────────────────────
    def ingest_tables(
        self,
        tables:       List[TableMeta],
        community_id: str,
        domain_id:    str,
        result:       IngestionResult,
        progress_cb:  Optional[Callable[[str, int, int], None]] = None,
        ingest_cols:  bool = True,
    ):
        """Ingest list of TableMeta into Collibra community/domain."""
        total = len(tables)
        db_id_cache: Dict[str, str]     = {}
        schema_id_cache: Dict[str, str] = {}

        for i, table in enumerate(tables):
            if self._cancel: break
            if not table.selected: continue

            label = f"{table.database}.{table.schema}.{table.table_name}"
            if progress_cb:
                progress_cb(f"Ingesting {label}…", i, total)

            try:
                # ── Database asset ─────────────────────────────
                db_key = f"{table.source_id}_{table.database}"
                if db_key not in db_id_cache:
                    db_aid, db_created = self.upsert_asset(
                        domain_id, table.database,
                        COLLIBRA_ASSET_TYPES["Database"],
                        display_name=table.database,
                    )
                    db_id_cache[db_key] = db_aid
                    if db_created: result.assets_created += 1
                    else:          result.assets_updated += 1
                db_asset_id = db_id_cache[db_key]

                # ── Schema asset ───────────────────────────────
                schema_key = f"{db_key}_{table.schema}"
                if schema_key not in schema_id_cache:
                    sch_aid, sch_created = self.upsert_asset(
                        domain_id, table.schema,
                        COLLIBRA_ASSET_TYPES["Schema"],
                        display_name=table.schema,
                    )
                    schema_id_cache[schema_key] = sch_aid
                    if sch_created: result.assets_created += 1
                    else:           result.assets_updated += 1
                    # Relation: schema in database
                    rel_id = self.add_relation(
                        sch_aid, db_asset_id,
                        COLLIBRA_RELATION_TYPES["schema_in_database"]
                    )
                    if rel_id: result.relations_created += 1
                schema_asset_id = schema_id_cache[schema_key]

                # ── Table asset ────────────────────────────────
                tbl_name = table.collibra_name or table.table_name
                tbl_type = COLLIBRA_ASSET_TYPES.get(table.object_type, COLLIBRA_ASSET_TYPES["Table"])
                tbl_aid, tbl_created = self.upsert_asset(
                    domain_id, tbl_name, tbl_type, display_name=tbl_name
                )
                if tbl_created: result.assets_created += 1
                else:           result.assets_updated += 1

                # Relation: table in schema
                rel_id = self.add_relation(
                    tbl_aid, schema_asset_id,
                    COLLIBRA_RELATION_TYPES["table_in_schema"]
                )
                if rel_id: result.relations_created += 1

                # Attributes
                DESC_TYPE = "00000000-0000-0000-0000-000000000219"
                if table.description:
                    self.add_attribute(tbl_aid, DESC_TYPE, table.description)
                if table.full_path:
                    PATH_TYPE = "00000000-0000-0000-0001-000500000005"
                    self.add_attribute(tbl_aid, PATH_TYPE, table.full_path)

                # ── Columns ────────────────────────────────────
                if ingest_cols:
                    for col in table.columns:
                        col_name = col.collibra_name or col.name
                        col_aid, col_created = self.upsert_asset(
                            domain_id,
                            f"{tbl_name}.{col_name}",
                            COLLIBRA_ASSET_TYPES["Column"],
                            display_name=col_name,
                        )
                        if col_created: result.assets_created += 1
                        else:           result.assets_updated += 1
                        rel_id = self.add_relation(
                            col_aid, tbl_aid,
                            COLLIBRA_RELATION_TYPES["column_in_table"]
                        )
                        if rel_id: result.relations_created += 1
                        if col.data_type:
                            DT_TYPE = "00000000-0000-0000-0000-000000000220"
                            self.add_attribute(col_aid, DT_TYPE, col.data_type)
                        if col.description:
                            self.add_attribute(col_aid, DESC_TYPE, col.description)

                result.log_entries.append(f"✔  {label}  ({table.col_count} columns)")

            except Exception as e:
                result.assets_failed += 1
                result.errors.append(f"✖  {label}: {e}")
                log.error(f"Ingestion error for {label}: {e}")

            time.sleep(0.05)   # polite rate-limit

        from datetime import datetime
        result.finished_at = datetime.now().isoformat()
        result.status      = "complete" if not result.errors else "partial"
        if progress_cb:
            progress_cb("Ingestion complete.", total, total)
        return result

    # ── HTTP Helpers ───────────────────────────────────────────
    def _get(self, path: str, params: dict = None) -> dict:
        r = self.session.get(f"{self.api_base}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: dict) -> dict:
        r = self.session.post(f"{self.api_base}{path}", json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    def _patch(self, path: str, payload: dict) -> dict:
        r = self.session.patch(f"{self.api_base}{path}", json=payload, timeout=30)
        r.raise_for_status()
        return r.json()
