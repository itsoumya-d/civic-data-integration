from fastapi.testclient import TestClient

from civicintegrate.api import create_app


def test_api_ingest_reconcile_and_audit():
    client = TestClient(create_app())
    payload = {
        "source_system": "council-ams",
        "records": [
            {
                "asset_id": "WAT-001",
                "name": "Water main",
                "type": "water_pipe",
                "status": "active",
                "replacement_value": "250000",
                "latitude": -41.28,
                "longitude": 174.77,
            }
        ],
    }
    response = client.post("/ingest", json=payload)
    assert response.status_code == 200
    assert response.json()["created"] == 1

    reconcile = client.post("/reconcile", json={"ledger": [{"asset_id": "WAT-001", "capital_value": "200000"}]})
    assert reconcile.status_code == 200
    assert reconcile.json()["discrepancies"][0]["kind"] == "value_mismatch"

    audit = client.get("/audit")
    assert audit.status_code == 200
    assert audit.json()["audit"][0]["action"] == "created"


def test_api_dead_letter_retry():
    client = TestClient(create_app())
    response = client.post(
        "/ingest",
        json={"source_system": "council-ams", "records": [{"asset_id": "", "name": ""}]},
    )
    assert response.json()["rejected"] == 1
    retry = client.post("/dead-letters/retry")
    assert retry.status_code == 200
    assert retry.json()["rejected"] == 1
    assert client.get("/dead-letters").json()["dead_letters"][0]["attempts"] == 2
