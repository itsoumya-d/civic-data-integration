"""
Microsoft Fabric OneLake, Medallion Architecture, and DirectLake Semantic Engine.

Provides enterprise Lakehouse management, Delta Lake ACID transaction handling,
Medallion (Bronze/Silver/Gold) distributed data curation, and DirectLake-ready
semantic modeling for Microsoft Fabric analytics workloads.
"""

from __future__ import annotations

import datetime
import hashlib
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field


class LakehouseLayer(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class PipelineStatus(str, Enum):
    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"


class DeltaCommitAction(str, Enum):
    APPEND = "append"
    MERGE = "merge"
    OVERWRITE = "overwrite"
    DELETE = "delete"


class DeltaCommitLog(BaseModel):
    version: int
    timestamp: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    action: DeltaCommitAction
    records_affected: int
    commit_sha: str
    schema_version: int
    operation_metrics: Dict[str, Any] = Field(default_factory=dict)


class FabricTableMetadata(BaseModel):
    workspace: str
    lakehouse_name: str
    table_name: str
    layer: LakehouseLayer
    format: str = "delta"
    location: str
    current_version: int = 0
    schema_fields: Dict[str, str] = Field(default_factory=dict)
    commit_history: List[DeltaCommitLog] = Field(default_factory=list)


class SemanticMeasure(BaseModel):
    name: str
    expression: str
    format_string: str = "#,##0.00"
    description: Optional[str] = None


class DirectLakeSemanticModel(BaseModel):
    model_name: str
    workspace: str
    lakehouse_source: str
    tables: List[str]
    mode: str = "DirectLake"
    measures: List[SemanticMeasure] = Field(default_factory=list)
    relationships: List[Dict[str, str]] = Field(default_factory=list)
    generated_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    def to_tmdl(self) -> str:
        """Serializes semantic model to Fabric Tabular Model Definition Language (TMDL) format."""
        lines = [
            f"model {self.model_name}",
            f"\tcompatibilityLevel: 1600",
            f"\tdefaultPowerBIDataSourceVersion: powerBI_V3",
            f"\tsource: directLake",
            f"\tlakehouse: {self.lakehouse_source}",
            "",
            "tables:",
        ]
        for tbl in self.tables:
            lines.append(f"\ttable {tbl}")
        lines.append("")
        lines.append("measures:")
        for m in self.measures:
            lines.append(f"\tmeasure '{m.name}' = {m.expression}")
            lines.append(f"\t\tformatString = \"{m.format_string}\"")
        return "\n".join(lines)


class FabricLakehouseManager:
    """Manages Microsoft Fabric OneLake workspaces, lakehouses, and Delta Lake tables."""

    def __init__(self, default_workspace: str = "provoke-analytics-ws") -> None:
        self.default_workspace = default_workspace
        # workspace -> lakehouse -> table_name -> records
        self._storage: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
        # metadata registry
        self._metadata: Dict[str, FabricTableMetadata] = {}

    def _table_key(self, workspace: str, lakehouse: str, table_name: str) -> str:
        return f"{workspace}/{lakehouse}/{table_name}"

    def create_lakehouse(self, workspace: str, lakehouse_name: str) -> Dict[str, Any]:
        if workspace not in self._storage:
            self._storage[workspace] = {}
        if lakehouse_name not in self._storage[workspace]:
            self._storage[workspace][lakehouse_name] = {}
        onelake_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"
        return {
            "workspace": workspace,
            "lakehouse_name": lakehouse_name,
            "onelake_uri": onelake_path,
            "status": "Active",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    def list_tables(self, workspace: str, lakehouse: str) -> List[Dict[str, Any]]:
        ws = self._storage.get(workspace, {})
        lh = ws.get(lakehouse, {})
        result = []
        for tbl_name in lh.keys():
            key = self._table_key(workspace, lakehouse, tbl_name)
            meta = self._metadata.get(key)
            result.append({
                "table_name": tbl_name,
                "layer": meta.layer.value if meta else LakehouseLayer.BRONZE.value,
                "records_count": len(lh[tbl_name]),
                "version": meta.current_version if meta else 0,
                "location": meta.location if meta else f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{tbl_name}",
            })
        return result

    def get_table_records(self, workspace: str, lakehouse: str, table_name: str) -> List[Dict[str, Any]]:
        return self._storage.get(workspace, {}).get(lakehouse, {}).get(table_name, [])

    def ingest_bronze(
        self,
        workspace: str,
        lakehouse: str,
        table_name: str,
        records: List[Dict[str, Any]],
        source_system: str,
    ) -> Tuple[int, DeltaCommitLog]:
        """Ingests raw records into Bronze lakehouse layer with audit metadata and Delta commit."""
        self.create_lakehouse(workspace, lakehouse)
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        key = self._table_key(workspace, lakehouse, table_name)

        enriched_records = []
        for r in records:
            payload_str = json.dumps(r, sort_keys=True, default=str)
            record_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
            enriched_records.append({
                **r,
                "_fabric_source_system": source_system,
                "_fabric_ingested_at": timestamp.isoformat(),
                "_fabric_record_hash": record_hash,
                "_fabric_layer": LakehouseLayer.BRONZE.value,
            })

        if table_name not in self._storage[workspace][lakehouse]:
            self._storage[workspace][lakehouse][table_name] = []

        self._storage[workspace][lakehouse][table_name].extend(enriched_records)

        # Delta metadata commit
        meta = self._metadata.get(key)
        if not meta:
            meta = FabricTableMetadata(
                workspace=workspace,
                lakehouse_name=lakehouse,
                table_name=table_name,
                layer=LakehouseLayer.BRONZE,
                location=f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{table_name}",
            )
            self._metadata[key] = meta

        meta.current_version += 1
        commit = DeltaCommitLog(
            version=meta.current_version,
            timestamp=timestamp,
            action=DeltaCommitAction.APPEND,
            records_affected=len(enriched_records),
            commit_sha=hashlib.sha1(f"{key}:{meta.current_version}:{len(enriched_records)}".encode()).hexdigest(),
            schema_version=1,
            operation_metrics={"bytes_written": sum(len(json.dumps(r)) for r in enriched_records)},
        )
        meta.commit_history.append(commit)
        return len(enriched_records), commit

    def transform_silver(
        self,
        workspace: str,
        lakehouse: str,
        bronze_table: str,
        silver_table: str,
        dedupe_key: str,
        required_fields: Optional[List[str]] = None,
    ) -> Tuple[int, int, DeltaCommitLog]:
        """Curates Bronze records to Silver Delta table: cleanses nulls, deduplicates, and validates schemas."""
        bronze_records = self.get_table_records(workspace, lakehouse, bronze_table)
        req_fields = required_fields or [dedupe_key]

        seen_keys = set()
        clean_records = []
        rejected_count = 0
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        for rec in bronze_records:
            # Check required fields
            if any(rec.get(f) is None or str(rec.get(f)).strip() == "" for f in req_fields):
                rejected_count += 1
                continue

            b_key = str(rec[dedupe_key])
            if b_key in seen_keys:
                # Deduplication - take most recent record
                clean_records = [r for r in clean_records if str(r[dedupe_key]) != b_key]
            seen_keys.add(b_key)

            curated = {k: v for k, v in rec.items() if not k.startswith("_fabric_")}
            curated["_fabric_layer"] = LakehouseLayer.SILVER.value
            curated["_fabric_curated_at"] = timestamp.isoformat()
            curated["_fabric_dedupe_key"] = b_key
            clean_records.append(curated)

        self.create_lakehouse(workspace, lakehouse)
        self._storage[workspace][lakehouse][silver_table] = clean_records

        key = self._table_key(workspace, lakehouse, silver_table)
        meta = self._metadata.get(key)
        if not meta:
            meta = FabricTableMetadata(
                workspace=workspace,
                lakehouse_name=lakehouse,
                table_name=silver_table,
                layer=LakehouseLayer.SILVER,
                location=f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{silver_table}",
            )
            self._metadata[key] = meta

        meta.current_version += 1
        commit = DeltaCommitLog(
            version=meta.current_version,
            timestamp=timestamp,
            action=DeltaCommitAction.MERGE,
            records_affected=len(clean_records),
            commit_sha=hashlib.sha1(f"{key}:{meta.current_version}:{len(clean_records)}".encode()).hexdigest(),
            schema_version=1,
            operation_metrics={"cleansed": len(clean_records), "rejected": rejected_count},
        )
        meta.commit_history.append(commit)
        return len(clean_records), rejected_count, commit

    def build_gold_mart(
        self,
        workspace: str,
        lakehouse: str,
        silver_table: str,
        gold_table: str,
        dimension_keys: List[str],
        metric_column: str,
        aggregation: str = "sum",
    ) -> Tuple[int, DeltaCommitLog]:
        """Aggregates Silver records into a high-performance Gold dimensional star-schema mart."""
        silver_records = self.get_table_records(workspace, lakehouse, silver_table)
        buckets: Dict[Tuple[Any, ...], List[float]] = {}

        for rec in silver_records:
            dim_values = tuple(rec.get(k, "Unknown") for k in dimension_keys)
            val = float(rec.get(metric_column, 0.0) or 0.0)
            buckets.setdefault(dim_values, []).append(val)

        gold_records = []
        timestamp = datetime.datetime.now(datetime.timezone.utc)

        for dim_vals, vals in buckets.items():
            row = {dimension_keys[i]: dim_vals[i] for i in range(len(dimension_keys))}
            if aggregation == "sum":
                row[f"{metric_column}_total"] = round(sum(vals), 2)
            elif aggregation == "avg":
                row[f"{metric_column}_avg"] = round(sum(vals) / max(len(vals), 1), 2)
            elif aggregation == "count":
                row[f"{metric_column}_count"] = len(vals)
            else:
                row[f"{metric_column}_total"] = round(sum(vals), 2)

            row["record_count"] = len(vals)
            row["_fabric_layer"] = LakehouseLayer.GOLD.value
            row["_fabric_aggregated_at"] = timestamp.isoformat()
            gold_records.append(row)

        self.create_lakehouse(workspace, lakehouse)
        self._storage[workspace][lakehouse][gold_table] = gold_records

        key = self._table_key(workspace, lakehouse, gold_table)
        meta = self._metadata.get(key)
        if not meta:
            meta = FabricTableMetadata(
                workspace=workspace,
                lakehouse_name=lakehouse,
                table_name=gold_table,
                layer=LakehouseLayer.GOLD,
                location=f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{gold_table}",
            )
            self._metadata[key] = meta

        meta.current_version += 1
        commit = DeltaCommitLog(
            version=meta.current_version,
            timestamp=timestamp,
            action=DeltaCommitAction.OVERWRITE,
            records_affected=len(gold_records),
            commit_sha=hashlib.sha1(f"{key}:{meta.current_version}:{len(gold_records)}".encode()).hexdigest(),
            schema_version=1,
            operation_metrics={"groups_aggregated": len(gold_records)},
        )
        meta.commit_history.append(commit)
        return len(gold_records), commit

    def generate_directlake_model(
        self,
        workspace: str,
        lakehouse: str,
        model_name: str,
        tables: List[str],
        custom_measures: Optional[List[SemanticMeasure]] = None,
    ) -> DirectLakeSemanticModel:
        """Constructs a Fabric Direct Lake semantic model referencing gold/silver tables."""
        measures = custom_measures or [
            SemanticMeasure(
                name="Total Records",
                expression="COUNTROWS('dim_gold_summary')",
                format_string="#,##0",
                description="Total count of aggregated gold entities",
            ),
            SemanticMeasure(
                name="Total Value NZD",
                expression="SUM('dim_gold_summary'[amount_total])",
                format_string="$#,##0.00",
                description="Total business transaction value in NZD",
            ),
        ]
        return DirectLakeSemanticModel(
            model_name=model_name,
            workspace=workspace,
            lakehouse_source=lakehouse,
            tables=tables,
            measures=measures,
            relationships=[],
        )
