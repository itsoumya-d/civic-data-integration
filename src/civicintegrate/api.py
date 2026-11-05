from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .pipeline import IntegrationPipeline


class IngestIn(BaseModel):
    source_system: str
    records: list[dict[str, Any]]


class ReconcileIn(BaseModel):
    ledger: list[dict[str, Any]] = Field(default_factory=list)


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
    app = FastAPI(title="Civic data integration", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/ingest")
    def ingest(payload: IngestIn) -> dict[str, Any]:
        return _summary(state.ingest(payload.records, payload.source_system))

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
