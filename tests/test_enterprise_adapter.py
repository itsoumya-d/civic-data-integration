from datetime import date
from decimal import Decimal
import pytest
from fastapi.testclient import TestClient

from civicintegrate.api import create_app
from civicintegrate.enterprise_adapter import (
    MuleSoftEnvelope,
    ResilientDeadLetterMediator,
    SAPS4HANAAssetAdapter,
)


def test_sap_date_parsing():
    assert SAPS4HANAAssetAdapter.parse_sap_date("/Date(1600000000000)/") == date(2020, 9, 13)
    assert SAPS4HANAAssetAdapter.parse_sap_date("2026-11-13T00:00:00Z") == date(2026, 11, 13)
    assert SAPS4HANAAssetAdapter.parse_sap_date(None) is None
    assert SAPS4HANAAssetAdapter.parse_sap_date("invalid-date") is None


def test_sap_fixed_asset_transformation_success():
    rec = {
        "CompanyCode": "AKLC",
        "MasterFixedAsset": "100452",
        "FixedAsset": "0000",
        "FixedAssetDescription": "Queen Street Smart Stormwater Sensor Station",
        "AssetClass": "WATER_SENSOR",
        "NetBookValueAmountInCoCodeCrcy": "45200.50",
        "AssetCapitalizationDate": "2024-03-15",
        "Latitude": -36.8509,
        "Longitude": 174.7645,
    }
    asset, err = SAPS4HANAAssetAdapter.transform_fixed_asset(rec)
    assert err is None
    assert asset is not None
    assert asset.source_system == "SAP_S4HANA"
    assert asset.source_id == "AKLC-100452-0000"
    assert asset.asset_id == "SAP-AKLC-100452"
    assert asset.name == "Queen Street Smart Stormwater Sensor Station"
    assert asset.asset_type == "water"
    assert asset.status == "active"
    assert asset.replacement_value == Decimal("45200.50")
    assert asset.latitude == -36.8509
    assert asset.longitude == 174.7645
    assert asset.last_inspected == date(2024, 3, 15)


def test_sap_fixed_asset_decommissioned():
    rec = {
        "CompanyCode": "AKLC",
        "MasterFixedAsset": "100999",
        "FixedAssetDescription": "Decommissioned Victoria Park Light Pole",
        "AssetClass": "PARK_LIGHT",
        "ReplacementValue": "1200.00",
        "AssetDeactivationDate": "2026-01-10",
    }
    asset, err = SAPS4HANAAssetAdapter.transform_fixed_asset(rec)
    assert err is None
    assert asset is not None
    assert asset.asset_type == "park"
    assert asset.status == "decommissioned"


def test_sap_fixed_asset_negative_value_rejected():
    rec = {
        "CompanyCode": "AKLC",
        "MasterFixedAsset": "100453",
        "FixedAssetDescription": "Invalid Negative Value Asset",
        "NetBookValueAmountInCoCodeCrcy": "-500.00",
    }
    asset, err = SAPS4HANAAssetAdapter.transform_fixed_asset(rec)
    assert asset is None
    assert "Negative replacement value rejected" in err


def test_sap_fixed_asset_missing_key_rejected():
    rec = {"CompanyCode": "AKLC"}
    asset, err = SAPS4HANAAssetAdapter.transform_fixed_asset(rec)
    assert asset is None
    assert "Missing mandatory SAP key" in err


def test_mulesoft_envelope_parsing_success():
    payload = {
        "apiVersion": "v2.1",
        "correlationId": "mule-corr-9876",
        "clientAppId": "auckland-council-citizen-portal",
        "timestamp": "2026-11-13T07:00:00Z",
        "data": {
            "entity": "ParkBooking",
            "reference": "PB-8821",
        },
        "status": "SUCCESS",
    }
    env = MuleSoftEnvelope.parse(payload)
    assert env.api_version == "v2.1"
    assert env.correlation_id == "mule-corr-9876"
    assert env.client_app_id == "auckland-council-citizen-portal"
    assert env.data["reference"] == "PB-8821"
    assert env.status == "SUCCESS"


def test_mulesoft_envelope_header_fallback():
    payload = {"data": {"foo": "bar"}}
    env = MuleSoftEnvelope.parse(payload, correlation_header="header-corr-1122")
    assert env.correlation_id == "header-corr-1122"
    assert env.data == {"foo": "bar"}


def test_mulesoft_envelope_missing_data_rejected():
    with pytest.raises(ValueError, match="MuleSoft envelope must contain a valid 'data' object"):
        MuleSoftEnvelope.parse({"apiVersion": "v1.0"})


def test_resilient_dead_letter_mediator_recording_and_retry():
    mediator = ResilientDeadLetterMediator(max_attempts=3)
    dl = mediator.record_failure(
        source_system="SAP_S4HANA",
        source_id="AKLC-999-0000",
        raw_payload={"foo": "bar"},
        reason="OData network timeout",
        correlation_id="corr-test-1",
    )
    assert dl.attempts == 1
    assert len(mediator.retryable_letters()) == 1

    # Retry attempt increment
    mediator.record_failure(
        source_system="SAP_S4HANA",
        source_id="AKLC-999-0000",
        raw_payload={"foo": "bar"},
        reason="OData gateway timeout again",
        correlation_id="corr-test-2",
    )
    assert dl.attempts == 2

    # Exceed maximum attempts
    mediator.record_failure(
        source_system="SAP_S4HANA",
        source_id="AKLC-999-0000",
        raw_payload={"foo": "bar"},
        reason="Third failure",
        correlation_id="corr-test-3",
    )
    assert dl.attempts == 3
    assert len(mediator.retryable_letters()) == 0

    # Removal on eventual success
    removed = mediator.remove_dead_letter("SAP_S4HANA", "AKLC-999-0000")
    assert removed is True
    assert len(mediator.dead_letters) == 0


def test_api_sap_odata_endpoint():
    client = TestClient(create_app())
    payload = {
        "company_code": "AKLC",
        "records": [
            {
                "CompanyCode": "AKLC",
                "MasterFixedAsset": "300101",
                "FixedAsset": "0000",
                "FixedAssetDescription": "Albert Park Solar Light Cluster",
                "AssetClass": "PARK_LIGHT",
                "NetBookValueAmountInCoCodeCrcy": "18500.00",
                "Latitude": -36.8500,
                "Longitude": 174.7670,
            },
            {
                "CompanyCode": "AKLC",
                "FixedAssetDescription": "Incomplete record missing master asset",
            },
        ],
    }
    resp = client.post(
        "/api/v1/integrations/sap-odata/assets",
        json=payload,
        headers={"X-Correlation-Id": "test-sap-run-42"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["correlation_id"] == "test-sap-run-42"
    assert data["sap_records_received"] == 2
    assert data["successfully_transformed"] == 1
    assert len(data["transformation_errors"]) == 1
    assert data["pipeline_summary"]["created"] == 1


def test_api_mulesoft_envelope_endpoint():
    client = TestClient(create_app())
    payload = {
        "apiVersion": "v1.2",
        "clientAppId": "auckland-transport-bus-telemetry",
        "data": {
            "route_id": "NX1",
            "status": "ON_TIME",
        },
    }
    resp = client.post(
        "/api/v1/integrations/mulesoft/envelope",
        json=payload,
        headers={"X-Correlation-Id": "mule-corr-5501"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ACCEPTED"
    assert data["correlation_id"] == "mule-corr-5501"
    assert data["client_app_id"] == "auckland-transport-bus-telemetry"
    assert "route_id" in data["data_keys"]
