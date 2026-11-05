from __future__ import annotations

from datetime import date

from .models import Asset


def validate_asset(asset: Asset) -> list[str]:
    errors: list[str] = []
    if not asset.asset_id:
        errors.append("asset_id is required")
    if not asset.name:
        errors.append("name is required")
    if asset.latitude is not None and not -90 <= asset.latitude <= 90:
        errors.append(f"latitude out of range: {asset.latitude}")
    if asset.longitude is not None and not -180 <= asset.longitude <= 180:
        errors.append(f"longitude out of range: {asset.longitude}")
    if asset.replacement_value < 0:
        errors.append("replacement_value must be non-negative")
    if asset.last_inspected and asset.last_inspected > date.today():
        errors.append(f"last_inspected is in the future: {asset.last_inspected}")
    if asset.status not in {"active", "inactive", "retired", "planned"}:
        errors.append(f"unsupported status: {asset.status}")
    return errors
