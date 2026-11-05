from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from civicintegrate.mapping import normalize_asset
from civicintegrate.models import Asset
from civicintegrate.pipeline import IntegrationPipeline
from civicintegrate.validation import validate_asset


def raw_asset(**overrides):
    data = {
        "asset_id": "WAT-001",
        "asset_name": "Water main - Queen Street",
        "category": "water_pipe",
        "status": "In Service",
        "replacement_cost": "$1,250,000.50",
        "lat": -41.2865,
        "lng": 174.7762,
        "last_inspection": "2026-10-01",
        "modified": "2026-10-02T09:30:00Z",
    }
    data.update(overrides)
    return data


def test_normalize_maps_council_asset_fields():
    asset = normalize_asset(raw_asset(), "council-ams")
    assert asset.asset_id == "WAT-001"
    assert asset.name == "Water main - Queen Street"
    assert asset.asset_type == "water_pipe"
    assert asset.status == "active"
    assert asset.replacement_value == Decimal("1250000.50")
    assert asset.last_inspected == date(2026, 10, 1)
    assert asset.updated_at == datetime(2026, 10, 2, 9, 30, tzinfo=timezone.utc)


def test_normalize_rejects_bad_money_and_partial_coordinates():
    with pytest.raises(ValueError, match="invalid replacement_value"):
        normalize_asset(raw_asset(replacement_cost="not-money"), "council-ams")
    with pytest.raises(ValueError, match="both be supplied"):
        normalize_asset(raw_asset(lng=None), "council-ams")


def test_validation_rejects_out_of_range_and_future_dates():
    asset = Asset(
        source_system="council-ams",
        source_id="1",
        asset_id="WAT-001",
        name="Water main",
        asset_type="water_pipe",
        status="active",
        replacement_value=Decimal("10"),
        latitude=-91,
        longitude=200,
        last_inspected=date(2099, 1, 1),
    )
    errors = validate_asset(asset)
    assert any("latitude" in error for error in errors)
    assert any("longitude" in error for error in errors)
    assert any("future" in error for error in errors)


def test_ingest_creates_updates_and_skips_unchanged():
    pipeline = IntegrationPipeline()
    first = pipeline.ingest([raw_asset()], "council-ams")
    assert (first.created, first.updated, first.unchanged, first.rejected) == (1, 0, 0, 0)

    same = pipeline.ingest([raw_asset()], "council-ams")
    assert (same.created, same.updated, same.unchanged) == (0, 0, 1)

    changed = pipeline.ingest([raw_asset(status="Out of Service")], "council-ams")
    assert (changed.created, changed.updated) == (0, 1)
    assert pipeline.store.get("council-ams", "WAT-001").status == "inactive"


def test_ingest_rejects_invalid_records_into_dead_letter_queue():
    pipeline = IntegrationPipeline()
    summary = pipeline.ingest([raw_asset(asset_id="", replacement_cost="-1")], "council-ams")
    assert summary.rejected == 1
    assert len(pipeline.dead_letters) == 1
    assert pipeline.dead_letters[0].errors
    assert pipeline.dead_letters[0].payload["asset_id"] == ""


def test_dead_letter_retry_increments_attempts_when_still_invalid():
    pipeline = IntegrationPipeline()
    pipeline.ingest([raw_asset(replacement_cost="bad")], "council-ams")
    retry = pipeline.retry_dead_letters()
    assert retry.rejected == 1
    assert pipeline.dead_letters[0].attempts == 2


def test_reconcile_reports_missing_value_and_status_discrepancies():
    pipeline = IntegrationPipeline()
    pipeline.ingest([raw_asset()], "council-ams")
    rows = [
        {"asset_id": "WAT-001", "capital_value": "1000000", "status": "inactive"},
        {"asset_id": "UNKNOWN-9", "capital_value": "500"},
    ]
    found = pipeline.reconcile_finance(rows)
    kinds = {item.kind for item in found}
    assert {"value_mismatch", "status_mismatch", "missing_in_assets"} <= kinds
    assert not any(item.kind == "missing_in_ledger" for item in found)


def test_reconcile_reports_asset_missing_from_ledger():
    pipeline = IntegrationPipeline()
    pipeline.ingest([raw_asset()], "council-ams")
    found = pipeline.reconcile_finance([])
    assert [item.kind for item in found] == ["missing_in_ledger"]


def test_same_asset_id_can_live_in_two_source_systems():
    pipeline = IntegrationPipeline()
    pipeline.ingest([raw_asset()], "council-ams")
    pipeline.ingest([raw_asset(name="Water main - council GIS")], "council-gis")
    assert len(pipeline.store.all()) == 2
