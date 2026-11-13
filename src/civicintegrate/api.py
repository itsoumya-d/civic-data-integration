from __future__ import annotations

from typing import Any, Dict, List, Optional
import uuid
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .enterprise_adapter import MuleSoftEnvelope, ResilientDeadLetterMediator, SAPS4HANAAssetAdapter
from .pipeline import IntegrationPipeline


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

    return app


app = create_app()
