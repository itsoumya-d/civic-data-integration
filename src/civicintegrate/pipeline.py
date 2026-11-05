from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .mapping import normalize_asset, parse_money
from .models import AuditEntry, DeadLetter, Discrepancy, RunSummary
from .store import AssetStore
from .validation import validate_asset


class IntegrationPipeline:
    def __init__(self, store: AssetStore | None = None) -> None:
        self.store = store or AssetStore()
        self.dead_letters: list[DeadLetter] = []
        self.audit: list[AuditEntry] = []

    def _record(self, action: str, source_system: str, asset_id: str, detail: str) -> None:
        self.audit.append(
            AuditEntry(
                at=datetime.now(timezone.utc),
                action=action,
                source_system=source_system,
                asset_id=asset_id,
                detail=detail,
            )
        )

    def ingest(self, records: list[dict[str, Any]], source_system: str) -> RunSummary:
        summary = RunSummary(source_system=source_system, received=len(records))
        seen: set[tuple[str, str]] = set()

        for raw in records:
            raw_id = str(raw.get("asset_id") or raw.get("id") or "").strip()
            try:
                asset = normalize_asset(raw, source_system)
                errors = validate_asset(asset)
                if not errors and (asset.source_system, asset.source_id) in seen:
                    errors.append(f"duplicate source record in batch: {asset.source_id}")
                if errors:
                    self._reject(summary, raw, source_system, raw_id, errors)
                    continue

                seen.add((asset.source_system, asset.source_id))
                action = self.store.upsert(asset)
                setattr(summary, action, getattr(summary, action) + 1)
                self._record(action, source_system, asset.asset_id, f"{asset.name} ({asset.asset_type})")
            except ValueError as exc:
                self._reject(summary, raw, source_system, raw_id, [str(exc)])

        return summary

    def _reject(
        self,
        summary: RunSummary,
        raw: dict[str, Any],
        source_system: str,
        asset_id: str,
        errors: list[str],
    ) -> None:
        summary.rejected += 1
        letter = DeadLetter(
            letter_id=uuid.uuid4().hex[:12],
            source_system=source_system,
            payload=dict(raw),
            errors=errors,
        )
        self.dead_letters.append(letter)
        summary.dead_letters.append(letter)
        self._record("rejected", source_system, asset_id, "; ".join(errors))

    def retry_dead_letters(self) -> RunSummary:
        pending = list(self.dead_letters)
        self.dead_letters.clear()
        summary = RunSummary(source_system="dead-letter-retry", received=len(pending))
        for letter in pending:
            before = len(self.dead_letters)
            retried = self.ingest([letter.payload], letter.source_system)
            summary.created += retried.created
            summary.updated += retried.updated
            summary.unchanged += retried.unchanged
            summary.rejected += retried.rejected
            if len(self.dead_letters) > before:
                failed = self.dead_letters[-1]
                failed.attempts = letter.attempts + 1
                failed.first_seen = letter.first_seen
        summary.audit = list(self.audit)
        return summary

    def reconcile_finance(self, ledger_rows: list[dict[str, Any]]) -> list[Discrepancy]:
        ledger: dict[str, dict[str, Any]] = {}
        for row in ledger_rows:
            asset_id = str(row.get("asset_id") or row.get("asset_number") or row.get("id") or "").strip()
            if asset_id:
                ledger[asset_id] = row

        assets = self.store.by_asset_id()
        discrepancies: list[Discrepancy] = []
        for asset_id, asset in assets.items():
            row = ledger.get(asset_id)
            if row is None:
                discrepancies.append(
                    Discrepancy("missing_in_ledger", asset_id, "asset exists but finance ledger row is missing")
                )
                continue
            try:
                ledger_value = parse_money(
                    row.get("capital_value", row.get("replacement_value", row.get("value"))),
                    "capital_value",
                )
            except ValueError as exc:
                discrepancies.append(
                    Discrepancy("invalid_ledger_value", asset_id, str(exc), str(asset.replacement_value))
                )
                continue
            if abs(asset.replacement_value - ledger_value) > Decimal("0.01"):
                discrepancies.append(
                    Discrepancy(
                        "value_mismatch",
                        asset_id,
                        "replacement value differs from finance ledger",
                        str(asset.replacement_value),
                        str(ledger_value),
                    )
                )
            ledger_status = str(row.get("status") or "").strip().lower()
            if ledger_status and ledger_status != asset.status:
                discrepancies.append(
                    Discrepancy(
                        "status_mismatch",
                        asset_id,
                        "lifecycle status differs from finance ledger",
                        asset.status,
                        ledger_status,
                    )
                )
        for asset_id in sorted(set(ledger) - set(assets)):
            discrepancies.append(
                Discrepancy("missing_in_assets", asset_id, "finance ledger row has no canonical asset")
            )
        return discrepancies
