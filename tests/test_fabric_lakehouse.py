import pytest
from fastapi.testclient import TestClient
from civicintegrate.api import create_app
from civicintegrate.fabric_lakehouse import (
    DeltaCommitAction,
    FabricLakehouseManager,
    LakehouseLayer,
    SemanticMeasure,
)


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


@pytest.fixture
def fabric_mgr():
    return FabricLakehouseManager(default_workspace="test-fabric-ws")


def test_fabric_lakehouse_creation(fabric_mgr):
    res = fabric_mgr.create_lakehouse("test-fabric-ws", "nz_water_lakehouse")
    assert res["status"] == "Active"
    assert "abfss://test-fabric-ws@onelake.dfs.fabric.microsoft.com/nz_water_lakehouse.Lakehouse" in res["onelake_uri"]


def test_bronze_ingestion_and_audit_hash(fabric_mgr):
    sample_records = [
        {"asset_id": "ASSET-001", "name": "Wellington Valve Station A", "region": "Wellington", "flow_rate": 12.5},
        {"asset_id": "ASSET-002", "name": "Lower Hutt Reservoir 3", "region": "Hutt Valley", "flow_rate": 45.0},
    ]
    count, commit = fabric_mgr.ingest_bronze(
        workspace="test-fabric-ws",
        lakehouse="nz_water_lakehouse",
        table_name="raw_scada_telemetry",
        records=sample_records,
        source_system="SCADA_IOT_BROKER",
    )
    assert count == 2
    assert commit.version == 1
    assert commit.action == DeltaCommitAction.APPEND
    assert commit.records_affected == 2

    # Check stored records have fabric audit metadata
    records = fabric_mgr.get_table_records("test-fabric-ws", "nz_water_lakehouse", "raw_scada_telemetry")
    assert len(records) == 2
    assert records[0]["_fabric_source_system"] == "SCADA_IOT_BROKER"
    assert records[0]["_fabric_layer"] == LakehouseLayer.BRONZE.value
    assert "_fabric_record_hash" in records[0]


def test_silver_cleansing_deduplication_and_null_rejection(fabric_mgr):
    sample_records = [
        {"asset_id": "ASSET-001", "name": "Valve A", "region": "Wellington", "status": "ACTIVE"},
        {"asset_id": "ASSET-001", "name": "Valve A Updated", "region": "Wellington", "status": "ACTIVE"}, # Duplicate
        {"asset_id": "ASSET-002", "name": None, "region": "Auckland", "status": "INACTIVE"}, # Null name
        {"asset_id": "ASSET-003", "name": "Pump C", "region": "Christchurch", "status": "ACTIVE"},
    ]
    fabric_mgr.ingest_bronze(
        workspace="test-fabric-ws",
        lakehouse="nz_water_lakehouse",
        table_name="bronze_assets",
        records=sample_records,
        source_system="ERP",
    )

    clean_count, rejected_count, commit = fabric_mgr.transform_silver(
        workspace="test-fabric-ws",
        lakehouse="nz_water_lakehouse",
        bronze_table="bronze_assets",
        silver_table="silver_assets",
        dedupe_key="asset_id",
        required_fields=["asset_id", "name"],
    )
    assert clean_count == 2
    assert rejected_count == 1
    assert commit.version == 1
    assert commit.action == DeltaCommitAction.MERGE

    silver_recs = fabric_mgr.get_table_records("test-fabric-ws", "nz_water_lakehouse", "silver_assets")
    assert len(silver_recs) == 2
    asset_ids = [r["asset_id"] for r in silver_recs]
    assert "ASSET-001" in asset_ids
    assert "ASSET-003" in asset_ids
    # Latest record for ASSET-001 retained
    asset_1 = next(r for r in silver_recs if r["asset_id"] == "ASSET-001")
    assert asset_1["name"] == "Valve A Updated"


def test_gold_dimensional_mart_aggregation(fabric_mgr):
    sample_silver = [
        {"region": "Wellington", "category": "PUMP", "cost_nzd": 12500.0},
        {"region": "Wellington", "category": "PUMP", "cost_nzd": 8500.0},
        {"region": "Wellington", "category": "VALVE", "cost_nzd": 3200.0},
        {"region": "Auckland", "category": "PUMP", "cost_nzd": 24000.0},
    ]
    fabric_mgr._storage.setdefault("test-fabric-ws", {}).setdefault("nz_water_lakehouse", {})["silver_expenses"] = sample_silver

    gold_count, commit = fabric_mgr.build_gold_mart(
        workspace="test-fabric-ws",
        lakehouse="nz_water_lakehouse",
        silver_table="silver_expenses",
        gold_table="gold_regional_spend_mart",
        dimension_keys=["region", "category"],
        metric_column="cost_nzd",
        aggregation="sum",
    )
    assert gold_count == 3
    assert commit.version == 1
    assert commit.action == DeltaCommitAction.OVERWRITE

    gold_records = fabric_mgr.get_table_records("test-fabric-ws", "nz_water_lakehouse", "gold_regional_spend_mart")
    wlg_pump = next(r for r in gold_records if r["region"] == "Wellington" and r["category"] == "PUMP")
    assert wlg_pump["cost_nzd_total"] == 21000.0
    assert wlg_pump["record_count"] == 2


def test_directlake_semantic_model_and_tmdl(fabric_mgr):
    measures = [
        SemanticMeasure(name="Total Spend NZD", expression="SUM('gold_regional_spend_mart'[cost_nzd_total])", format_string="$#,##0.00"),
        SemanticMeasure(name="Active Marts", expression="DISTINCTCOUNT('gold_regional_spend_mart'[region])", format_string="#,##0"),
    ]
    model = fabric_mgr.generate_directlake_model(
        workspace="test-fabric-ws",
        lakehouse="nz_water_lakehouse",
        model_name="WellingtonWaterAnalytics",
        tables=["silver_expenses", "gold_regional_spend_mart"],
        custom_measures=measures,
    )
    assert model.mode == "DirectLake"
    tmdl = model.to_tmdl()
    assert "source: directLake" in tmdl
    assert "lakehouse: nz_water_lakehouse" in tmdl
    assert "measure 'Total Spend NZD'" in tmdl


def test_api_fabric_medallion_end_to_end(client):
    # 1. Create lakehouse
    lh_res = client.post("/api/v1/fabric/lakehouses", json={
        "workspace": "provoke-wellington-ws",
        "lakehouse_name": "provoke_fabric_lakehouse"
    })
    assert lh_res.status_code == 200
    assert lh_res.json()["status"] == "Active"

    # 2. Ingest bronze
    bronze_res = client.post("/api/v1/fabric/medallion/bronze", json={
        "workspace": "provoke-wellington-ws",
        "lakehouse": "provoke_fabric_lakehouse",
        "table_name": "bronze_invoices",
        "records": [
            {"invoice_id": "INV-101", "client": "Meridian Energy", "amount_nzd": 45000.0, "status": "PAID"},
            {"invoice_id": "INV-102", "client": "Contact Energy", "amount_nzd": 28000.0, "status": "PENDING"},
            {"invoice_id": "INV-101", "client": "Meridian Energy", "amount_nzd": 45000.0, "status": "PAID"}, # dup
        ],
        "source_system": "XERO_BILLING",
    })
    assert bronze_res.status_code == 200
    assert bronze_res.json()["ingested_count"] == 3

    # 3. Curate silver
    silver_res = client.post("/api/v1/fabric/medallion/silver", json={
        "workspace": "provoke-wellington-ws",
        "lakehouse": "provoke_fabric_lakehouse",
        "bronze_table": "bronze_invoices",
        "silver_table": "silver_invoices",
        "dedupe_key": "invoice_id",
        "required_fields": ["invoice_id", "amount_nzd"],
    })
    assert silver_res.status_code == 200
    assert silver_res.json()["clean_count"] == 2

    # 4. Aggregate gold
    gold_res = client.post("/api/v1/fabric/medallion/gold", json={
        "workspace": "provoke-wellington-ws",
        "lakehouse": "provoke_fabric_lakehouse",
        "silver_table": "silver_invoices",
        "gold_table": "gold_client_revenue_mart",
        "dimension_keys": ["client"],
        "metric_column": "amount_nzd",
        "aggregation": "sum",
    })
    assert gold_res.status_code == 200
    assert gold_res.json()["gold_groups_count"] == 2

    # 5. List tables
    tables_res = client.get("/api/v1/fabric/lakehouses/provoke-wellington-ws/provoke_fabric_lakehouse/tables")
    assert tables_res.status_code == 200
    table_names = [t["table_name"] for t in tables_res.json()["tables"]]
    assert "bronze_invoices" in table_names
    assert "silver_invoices" in table_names
    assert "gold_client_revenue_mart" in table_names

    # 6. DirectLake semantic model
    semantic_res = client.post("/api/v1/fabric/semantic-models/directlake", json={
        "workspace": "provoke-wellington-ws",
        "lakehouse": "provoke_fabric_lakehouse",
        "model_name": "ProvokeClientRevenueSemanticModel",
        "tables": ["silver_invoices", "gold_client_revenue_mart"],
    })
    assert semantic_res.status_code == 200
    data = semantic_res.json()
    assert data["mode"] == "DirectLake"
    assert "source: directLake" in data["tmdl_definition"]
