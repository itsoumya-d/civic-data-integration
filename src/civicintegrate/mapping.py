from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .models import Asset

ALIASES: dict[str, tuple[str, ...]] = {
    "source_id": ("source_id", "record_id", "objectid", "fme_feature_id", "id"),
    "asset_id": ("asset_id", "asset_number", "asset_code", "asset_no", "id"),
    "name": ("name", "asset_name", "description", "label"),
    "asset_type": ("asset_type", "type", "category", "asset_class", "class"),
    "status": ("status", "lifecycle_status", "state", "operational_status"),
    "replacement_value": (
        "replacement_value",
        "replacement_cost",
        "capital_value",
        "value",
        "current_value",
    ),
    "latitude": ("latitude", "lat", "y"),
    "longitude": ("longitude", "lon", "lng", "long", "x"),
    "last_inspected": ("last_inspected", "inspection_date", "last_inspection", "inspected_on"),
    "updated_at": ("updated_at", "updated", "modified_at", "last_modified", "modified"),
}

STATUS_MAP = {
    "in service": "active",
    "in_service": "active",
    "in-service": "active",
    "operational": "active",
    "active": "active",
    "out of service": "inactive",
    "out_of_service": "inactive",
    "inactive": "inactive",
    "disposed": "retired",
    "retired": "retired",
    "decommissioned": "retired",
    "planned": "planned",
    "proposed": "planned",
}


def _pick(raw: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in raw and raw[name] not in (None, ""):
            return raw[name]
    return None


def _slug(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def parse_money(value: Any, field: str) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    cleaned = str(value).replace("$", "").replace(",", "").replace(" ", "").strip()
    try:
        amount = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc
    if amount < 0:
        raise ValueError(f"negative {field}: {value!r}")
    return amount.quantize(Decimal("0.01"))


def parse_date(value: Any, field: str) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc


def parse_coord(value: Any, field: str) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc


def parse_timestamp(value: Any) -> datetime:
    if value in (None, ""):
        return datetime.now(timezone.utc)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.combine(parse_date(text, "updated_at") or date.today(), datetime.min.time())
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def normalize_asset(raw: dict[str, Any], source_system: str) -> Asset:
    asset_id = str(_pick(raw, ALIASES["asset_id"]) or "").strip()
    source_id = str(_pick(raw, ALIASES["source_id"]) or asset_id).strip()
    name = str(_pick(raw, ALIASES["name"]) or "").strip()
    raw_type = _pick(raw, ALIASES["asset_type"])
    asset_type = _slug(raw_type) if raw_type not in (None, "") else "unknown"
    raw_status = _pick(raw, ALIASES["status"])
    status = STATUS_MAP.get(_slug(raw_status), _slug(raw_status) or "active")

    latitude = parse_coord(_pick(raw, ALIASES["latitude"]), "latitude")
    longitude = parse_coord(_pick(raw, ALIASES["longitude"]), "longitude")
    if (latitude is None) != (longitude is None):
        raise ValueError("latitude and longitude must both be supplied")

    return Asset(
        source_system=source_system,
        source_id=source_id,
        asset_id=asset_id,
        name=name,
        asset_type=asset_type,
        status=status,
        replacement_value=parse_money(_pick(raw, ALIASES["replacement_value"]), "replacement_value"),
        latitude=latitude,
        longitude=longitude,
        last_inspected=parse_date(_pick(raw, ALIASES["last_inspected"]), "last_inspected"),
        updated_at=parse_timestamp(_pick(raw, ALIASES["updated_at"])),
    )
