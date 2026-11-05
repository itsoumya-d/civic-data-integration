from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Asset:
    source_system: str
    source_id: str
    asset_id: str
    name: str
    asset_type: str
    status: str
    replacement_value: Decimal
    latitude: float | None = None
    longitude: float | None = None
    last_inspected: date | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def fingerprint(self) -> str:
        payload = {
            "asset_id": self.asset_id,
            "name": self.name,
            "asset_type": self.asset_type,
            "status": self.status,
            "replacement_value": str(self.replacement_value),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "last_inspected": self.last_inspected.isoformat() if self.last_inspected else None,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()[:24]


@dataclass(frozen=True)
class AuditEntry:
    at: datetime
    action: str
    source_system: str
    asset_id: str
    detail: str


@dataclass
class DeadLetter:
    letter_id: str
    source_system: str
    payload: dict[str, Any]
    errors: list[str]
    attempts: int = 1
    first_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Discrepancy:
    kind: str
    asset_id: str
    detail: str
    asset_value: str = ""
    ledger_value: str = ""


@dataclass
class RunSummary:
    source_system: str
    received: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0
    dead_letters: list[DeadLetter] = field(default_factory=list)
    audit: list[AuditEntry] = field(default_factory=list)
