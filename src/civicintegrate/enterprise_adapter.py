"""Enterprise integration adapter for SAP S/4HANA OData and MuleSoft Anypoint Platform.

Provides canonical envelope normalization, SAP S/4HANA fixed asset entity transformation,
MuleSoft API contract enforcement, and resilient retry mediation for civic systems.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from civicintegrate.models import Asset, DeadLetter


@dataclass(frozen=True)
class MuleSoftEnvelope:
    api_version: str
    correlation_id: str
    client_app_id: str
    timestamp: datetime
    data: Dict[str, Any]
    status: str = "SUCCESS"
    errors: List[str] = field(default_factory=list)

    @classmethod
    def parse(cls, payload: Dict[str, Any], correlation_header: Optional[str] = None) -> "MuleSoftEnvelope":
        corr = correlation_header or payload.get("correlationId") or str(uuid.uuid4())
        data = payload.get("data")
        if data is None or not isinstance(data, dict):
            raise ValueError("MuleSoft envelope must contain a valid 'data' object")
        ts_str = payload.get("timestamp")
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
        return cls(
            api_version=str(payload.get("apiVersion", "v1.0")),
            correlation_id=corr,
            client_app_id=str(payload.get("clientAppId", "mule-internal-gateway")),
            timestamp=ts,
            data=data,
            status=str(payload.get("status", "SUCCESS")),
            errors=list(payload.get("errors", [])),
        )


class SAPS4HANAAssetAdapter:
    """Adapts SAP S/4HANA Fixed Asset OData structures (A_FixedAsset) to canonical civic Asset models."""

    @staticmethod
    def parse_sap_date(raw_date: Optional[str]) -> Optional[date]:
        if not raw_date:
            return None
        # Handle SAP OData /Date(1600000000000)/ format
        if raw_date.startswith("/Date(") and raw_date.endswith(")/"):
            try:
                ms = int(raw_date[6:-2].split("+")[0].split("-")[0])
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date()
            except Exception:
                return None
        # Handle ISO date YYYY-MM-DD
        try:
            return date.fromisoformat(raw_date[:10])
        except Exception:
            return None

    @classmethod
    def transform_fixed_asset(cls, item: Dict[str, Any]) -> Tuple[Optional[Asset], Optional[str]]:
        """Transforms a single SAP S/4HANA Fixed Asset OData record into a canonical Asset."""
        company_code = item.get("CompanyCode", "AKLC")
        master_asset = item.get("MasterFixedAsset")
        sub_number = item.get("FixedAsset", "0000")
        description = item.get("FixedAssetDescription") or item.get("AssetDescription")

        if not master_asset or not description:
            return None, "Missing mandatory SAP key: MasterFixedAsset or description"

        source_id = f"{company_code}-{master_asset}-{sub_number}"
        asset_id = f"SAP-{company_code}-{master_asset}"

        raw_val = (
            item.get("NetBookValueAmountInCoCodeCrcy")
            or item.get("AcquisitionAndProductionCost")
            or item.get("ReplacementValue")
            or "0.00"
        )
        try:
            replacement_val = Decimal(str(raw_val)).quantize(Decimal("0.01"))
            if replacement_val < Decimal("0.00"):
                return None, f"Negative replacement value rejected: {replacement_val}"
        except InvalidOperation:
            return None, f"Invalid financial value format: {raw_val}"

        raw_class = str(item.get("AssetClass", "INFRA")).upper()
        if "PARK" in raw_class or "RECR" in raw_class:
            asset_type = "park"
        elif "ROAD" in raw_class or "TRAN" in raw_class:
            asset_type = "road"
        elif "WATER" in raw_class or "STOR" in raw_class:
            asset_type = "water"
        elif "BLDG" in raw_class or "PROP" in raw_class:
            asset_type = "building"
        else:
            asset_type = "infrastructure"

        raw_status = str(item.get("AssetDeactivationDate") or item.get("Status", "ACTIVE")).upper()
        status = "decommissioned" if item.get("AssetDeactivationDate") or "DEACT" in raw_status else "active"

        cap_date = cls.parse_sap_date(item.get("AssetCapitalizationDate") or item.get("LastInventoryDate"))

        lat = float(item["Latitude"]) if "Latitude" in item and item["Latitude"] is not None else None
        lon = float(item["Longitude"]) if "Longitude" in item and item["Longitude"] is not None else None

        asset = Asset(
            source_system="SAP_S4HANA",
            source_id=source_id,
            asset_id=asset_id,
            name=str(description)[:120],
            asset_type=asset_type,
            status=status,
            replacement_value=replacement_val,
            latitude=lat,
            longitude=lon,
            last_inspected=cap_date,
        )
        return asset, None


class ResilientDeadLetterMediator:
    """Mediates integration dead-letters with exponential backoff and forensic tracing."""

    def __init__(self, max_attempts: int = 3):
        self.max_attempts = max_attempts
        self.dead_letters: List[DeadLetter] = []

    def record_failure(
        self,
        source_system: str,
        source_id: str,
        raw_payload: Dict[str, Any],
        reason: str,
        correlation_id: str,
    ) -> DeadLetter:
        letter_id = f"DL-{source_system}-{source_id}"
        now = datetime.now(timezone.utc)
        for dl in self.dead_letters:
            if dl.letter_id == letter_id:
                dl.attempts += 1
                dl.errors.append(f"[{correlation_id}] {reason}")
                dl.last_seen = now
                return dl

        dl = DeadLetter(
            letter_id=letter_id,
            source_system=source_system,
            payload=raw_payload,
            errors=[f"[{correlation_id}] {reason}"],
            attempts=1,
            first_seen=now,
            last_seen=now,
        )
        self.dead_letters.append(dl)
        return dl

    def retryable_letters(self) -> List[DeadLetter]:
        return [dl for dl in self.dead_letters if dl.attempts < self.max_attempts]

    def remove_dead_letter(self, source_system: str, source_id: str) -> bool:
        letter_id = f"DL-{source_system}-{source_id}"
        initial = len(self.dead_letters)
        self.dead_letters = [
            dl for dl in self.dead_letters
            if dl.letter_id != letter_id
        ]
        return len(self.dead_letters) < initial
