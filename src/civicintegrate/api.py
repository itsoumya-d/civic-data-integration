from __future__ import annotations

from typing import Any, Dict, List, Optional
import uuid
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

import datetime
from .enterprise_adapter import MuleSoftEnvelope, ResilientDeadLetterMediator, SAPS4HANAAssetAdapter
from .fabric_lakehouse import FabricLakehouseManager, SemanticMeasure
from .pipeline import IntegrationPipeline
from .project_orchestrator import (
    IctProjectDeliveryOrchestrator,
    ProjectStatus,
    RiskSeverity,
    SmeIntegrationPipelineManager,
)


class ProjectCreateIn(BaseModel):
    client_name: str
    project_type: str = "SYSTEMS_INTEGRATION"
    hourly_rate_nzd: Optional[float] = 74.0
    allocated_hours: Optional[float] = 160.0


class MilestoneCreateIn(BaseModel):
    name: str
    target_date: datetime.date
    deliverables: Optional[List[str]] = Field(default_factory=list)


class RiskCreateIn(BaseModel):
    category: str
    description: str
    severity: RiskSeverity = RiskSeverity.MEDIUM
    mitigation_strategy: str


class SmeReconcileIn(BaseModel):
    crm_customers: List[Dict[str, Any]] = Field(default_factory=list)
    finance_invoices: List[Dict[str, Any]] = Field(default_factory=list)
    ops_jobs: List[Dict[str, Any]] = Field(default_factory=list)


class IngestIn(BaseModel):
    source_system: str
    records: list[dict[str, Any]]


class ReconcileIn(BaseModel):
    ledger: list[dict[str, Any]] = Field(default_factory=list)


class SAPBatchIngestIn(BaseModel):
    records: List[Dict[str, Any]]
    company_code: Optional[str] = "AKLC"


class MuleSoftPayloadIn(BaseModel):
    apiVersion: Optional[str] = "v1.0"
    clientAppId: Optional[str] = "mule-internal-gateway"
    correlationId: Optional[str] = None
    data: Dict[str, Any]
    status: Optional[str] = "SUCCESS"
    errors: Optional[List[str]] = Field(default_factory=list)


class FabricLakehouseCreateIn(BaseModel):
    workspace: str = "provoke-analytics-ws"
    lakehouse_name: str = "provoke_lakehouse"


class FabricBronzeIngestIn(BaseModel):
    workspace: str = "provoke-analytics-ws"
    lakehouse: str = "provoke_lakehouse"
    table_name: str
    records: List[Dict[str, Any]]
    source_system: str = "CRM_FABRIC"


class FabricSilverCurateIn(BaseModel):
    workspace: str = "provoke-analytics-ws"
    lakehouse: str = "provoke_lakehouse"
    bronze_table: str
    silver_table: str
    dedupe_key: str
    required_fields: Optional[List[str]] = None


class FabricGoldAggregateIn(BaseModel):
    workspace: str = "provoke-analytics-ws"
    lakehouse: str = "provoke_lakehouse"
    silver_table: str
    gold_table: str
    dimension_keys: List[str]
    metric_column: str
    aggregation: str = "sum"


class FabricSemanticModelIn(BaseModel):
    workspace: str = "provoke-analytics-ws"
    lakehouse: str = "provoke_lakehouse"
    model_name: str = "FabricDirectLakeModel"
    tables: List[str]


def _summary(summary) -> dict[str, Any]:
    return {
        "source_system": summary.source_system,
        "received": summary.received,
        "created": summary.created,
        "updated": summary.updated,
        "unchanged": summary.unchanged,
        "rejected": summary.rejected,
        "dead_letters": [
            {
                "letter_id": letter.letter_id,
                "source_system": letter.source_system,
                "errors": letter.errors,
                "attempts": letter.attempts,
            }
            for letter in summary.dead_letters
        ],
        "audit": [
            {
                "at": entry.at.isoformat(),
                "action": entry.action,
                "source_system": entry.source_system,
                "asset_id": entry.asset_id,
                "detail": entry.detail,
            }
            for entry in summary.audit
        ],
    }


def create_app(pipeline: IntegrationPipeline | None = None) -> FastAPI:
    state = pipeline or IntegrationPipeline()
    mediator = ResilientDeadLetterMediator()
    app = FastAPI(title="Civic data integration", version="0.2.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/ingest")
    def ingest(payload: IngestIn) -> dict[str, Any]:
        return _summary(state.ingest(payload.records, payload.source_system))

    @app.post("/api/v1/integrations/sap-odata/assets")
    def ingest_sap_odata(
        payload: SAPBatchIngestIn,
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id")
    ) -> dict[str, Any]:
        corr_id = x_correlation_id or str(uuid.uuid4())
        converted_records = []
        errors = []
        for rec in payload.records:
            asset, err = SAPS4HANAAssetAdapter.transform_fixed_asset(rec)
            if err:
                errors.append(err)
                mediator.record_failure("SAP_S4HANA", str(rec.get("MasterFixedAsset", "UNKNOWN")), rec, err, corr_id)
            elif asset:
                converted_records.append({
                    "id": asset.source_id,
                    "asset_id": asset.asset_id,
                    "name": asset.name,
                    "type": asset.asset_type,
                    "status": asset.status,
                    "value": str(asset.replacement_value),
                    "lat": asset.latitude,
                    "lon": asset.longitude,
                    "last_inspected": asset.last_inspected.isoformat() if asset.last_inspected else None,
                })
        summary = state.ingest(converted_records, "SAP_S4HANA") if converted_records else None
        return {
            "correlation_id": corr_id,
            "sap_records_received": len(payload.records),
            "successfully_transformed": len(converted_records),
            "transformation_errors": errors,
            "pipeline_summary": _summary(summary) if summary else None,
        }

    @app.post("/api/v1/integrations/mulesoft/envelope")
    def ingest_mulesoft_envelope(
        payload: MuleSoftPayloadIn,
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id")
    ) -> dict[str, Any]:
        try:
            envelope = MuleSoftEnvelope.parse(payload.model_dump(), x_correlation_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {
            "status": "ACCEPTED",
            "correlation_id": envelope.correlation_id,
            "client_app_id": envelope.client_app_id,
            "timestamp": envelope.timestamp.isoformat(),
            "data_keys": list(envelope.data.keys()),
        }

    @app.post("/reconcile")
    def reconcile(payload: ReconcileIn) -> dict[str, Any]:
        rows = [
            {
                "kind": item.kind,
                "asset_id": item.asset_id,
                "detail": item.detail,
                "asset_value": item.asset_value,
                "ledger_value": item.ledger_value,
            }
            for item in state.reconcile_finance(payload.ledger)
        ]
        return {"discrepancies": rows}

    @app.get("/dead-letters")
    def dead_letters() -> dict[str, Any]:
        return {
            "dead_letters": [
                {
                    "letter_id": letter.letter_id,
                    "source_system": letter.source_system,
                    "payload": letter.payload,
                    "errors": letter.errors,
                    "attempts": letter.attempts,
                }
                for letter in state.dead_letters
            ]
        }

    @app.post("/dead-letters/retry")
    def retry() -> dict[str, Any]:
        return _summary(state.retry_dead_letters())

    @app.get("/audit")
    def audit() -> dict[str, Any]:
        return {
            "audit": [
                {
                    "at": entry.at.isoformat(),
                    "action": entry.action,
                    "source_system": entry.source_system,
                    "asset_id": entry.asset_id,
                    "detail": entry.detail,
                }
                for entry in state.audit
            ]
        }

    orchestrator = IctProjectDeliveryOrchestrator()

    @app.post("/api/v1/ict-projects/create")
    def create_project(payload: ProjectCreateIn) -> dict[str, Any]:
        p = orchestrator.create_project(
            client_name=payload.client_name,
            project_type=payload.project_type,
            hourly_rate_nzd=payload.hourly_rate_nzd or 74.0,
            allocated_hours=payload.allocated_hours or 160.0,
        )
        return {
            "project_id": p.project_id,
            "client_name": p.client_name,
            "project_type": p.project_type,
            "hourly_rate_nzd": p.hourly_rate_nzd,
            "budget_total_nzd": p.budget_total_nzd,
            "status": p.status.value,
        }

    @app.get("/api/v1/ict-projects/{project_id}")
    def get_project(project_id: str) -> dict[str, Any]:
        p = orchestrator.get_project(project_id)
        if not p:
            raise HTTPException(status_code=404, detail="Project not found")
        return {
            "project_id": p.project_id,
            "client_name": p.client_name,
            "project_type": p.project_type,
            "hourly_rate_nzd": p.hourly_rate_nzd,
            "budget_total_nzd": p.budget_total_nzd,
            "hours_logged": p.hours_logged,
            "cost_to_date_nzd": p.cost_to_date_nzd,
            "status": p.status.value,
            "overall_progress_percentage": p.overall_progress_percentage,
            "milestones_count": len(p.milestones),
            "risks_count": len(p.risk_register),
        }

    @app.post("/api/v1/ict-projects/{project_id}/milestones")
    def add_milestone(project_id: str, payload: MilestoneCreateIn) -> dict[str, Any]:
        m = orchestrator.add_milestone(
            project_id=project_id,
            name=payload.name,
            target_date=payload.target_date,
            deliverables=payload.deliverables,
        )
        if not m:
            raise HTTPException(status_code=404, detail="Project not found")
        return {
            "milestone_id": m.id,
            "name": m.name,
            "target_date": m.target_date.isoformat(),
            "deliverables": m.deliverables,
        }

    @app.post("/api/v1/ict-projects/{project_id}/risks")
    def add_risk(project_id: str, payload: RiskCreateIn) -> dict[str, Any]:
        r = orchestrator.add_risk(
            project_id=project_id,
            category=payload.category,
            description=payload.description,
            severity=payload.severity,
            mitigation_strategy=payload.mitigation_strategy,
        )
        if not r:
            raise HTTPException(status_code=404, detail="Project not found")
        return {
            "risk_id": r.id,
            "category": r.category,
            "description": r.description,
            "severity": r.severity.value,
            "mitigation_strategy": r.mitigation_strategy,
        }

    @app.post("/api/v1/integrations/sme/reconcile")
    def reconcile_sme_data(payload: SmeReconcileIn) -> dict[str, Any]:
        return SmeIntegrationPipelineManager.reconcile_tripartite_data(
            crm_customers=payload.crm_customers,
            finance_invoices=payload.finance_invoices,
            ops_jobs=payload.ops_jobs,
        )

    fabric_mgr = FabricLakehouseManager()

    @app.post("/api/v1/fabric/lakehouses")
    def create_fabric_lakehouse(payload: FabricLakehouseCreateIn) -> dict[str, Any]:
        return fabric_mgr.create_lakehouse(payload.workspace, payload.lakehouse_name)

    @app.get("/api/v1/fabric/lakehouses/{workspace}/{lakehouse}/tables")
    def list_fabric_tables(workspace: str, lakehouse: str) -> dict[str, Any]:
        return {
            "workspace": workspace,
            "lakehouse": lakehouse,
            "tables": fabric_mgr.list_tables(workspace, lakehouse),
        }

    @app.post("/api/v1/fabric/medallion/bronze")
    def ingest_fabric_bronze(payload: FabricBronzeIngestIn) -> dict[str, Any]:
        count, commit = fabric_mgr.ingest_bronze(
            workspace=payload.workspace,
            lakehouse=payload.lakehouse,
            table_name=payload.table_name,
            records=payload.records,
            source_system=payload.source_system,
        )
        return {
            "ingested_count": count,
            "table": payload.table_name,
            "version": commit.version,
            "commit_sha": commit.commit_sha,
        }

    @app.post("/api/v1/fabric/medallion/silver")
    def curate_fabric_silver(payload: FabricSilverCurateIn) -> dict[str, Any]:
        clean_count, rejected_count, commit = fabric_mgr.transform_silver(
            workspace=payload.workspace,
            lakehouse=payload.lakehouse,
            bronze_table=payload.bronze_table,
            silver_table=payload.silver_table,
            dedupe_key=payload.dedupe_key,
            required_fields=payload.required_fields,
        )
        return {
            "clean_count": clean_count,
            "rejected_count": rejected_count,
            "table": payload.silver_table,
            "version": commit.version,
            "commit_sha": commit.commit_sha,
        }

    @app.post("/api/v1/fabric/medallion/gold")
    def aggregate_fabric_gold(payload: FabricGoldAggregateIn) -> dict[str, Any]:
        gold_count, commit = fabric_mgr.build_gold_mart(
            workspace=payload.workspace,
            lakehouse=payload.lakehouse,
            silver_table=payload.silver_table,
            gold_table=payload.gold_table,
            dimension_keys=payload.dimension_keys,
            metric_column=payload.metric_column,
            aggregation=payload.aggregation,
        )
        return {
            "gold_groups_count": gold_count,
            "table": payload.gold_table,
            "version": commit.version,
            "commit_sha": commit.commit_sha,
        }

    @app.post("/api/v1/fabric/semantic-models/directlake")
    def create_directlake_semantic_model(payload: FabricSemanticModelIn) -> dict[str, Any]:
        model = fabric_mgr.generate_directlake_model(
            workspace=payload.workspace,
            lakehouse=payload.lakehouse,
            model_name=payload.model_name,
            tables=payload.tables,
        )
        return {
            "model_name": model.model_name,
            "workspace": model.workspace,
            "lakehouse": model.lakehouse_source,
            "mode": model.mode,
            "tables": model.tables,
            "tmdl_definition": model.to_tmdl(),
        }

    return app


app = create_app()
