"""Unit tests for SAP Transportation Management (TM) and Sales & Distribution (SD) integration engine."""
from decimal import Decimal
import pytest

from civicintegrate.sap_tm_sd import (
    APPROVED_CARRIERS,
    ExportDeclaration,
    FreightOrder,
    FreightOrderStatus,
    SalesOrderDelivery,
    SapTmSdManager,
    StagingQaStatus,
    StagingReconciliation,
    TransportationMode,
)


@pytest.fixture
def manager() -> SapTmSdManager:
    return SapTmSdManager(default_plant="DUNSANDEL-01")


def test_create_valid_freight_order_road(manager: SapTmSdManager):
    fo = manager.create_freight_order(
        order_id="FO-904821",
        carrier_code="MAINFRT-NZ",
        origin_plant="DUNSANDEL-01",
        destination_port="NZLYT",
        mode=TransportationMode.ROAD,
        containers=["MSKU1234567"],
        gross_weight_kg="24500.50",
    )
    assert fo.order_id == "FO-904821"
    assert fo.carrier_code == "MAINFRT-NZ"
    assert fo.origin_plant == "DUNSANDEL-01"
    assert fo.destination_port == "NZLYT"
    assert fo.mode == TransportationMode.ROAD
    assert fo.gross_weight_kg == Decimal("24500.50")
    assert fo.status == FreightOrderStatus.PLANNED
    assert fo.temperature_control_required is False


def test_create_freight_order_temperature_controlled(manager: SapTmSdManager):
    fo = manager.create_freight_order(
        order_id="FO-778899",
        carrier_code="TIL-NZ",
        origin_plant="DUNSANDEL-01",
        destination_port="NZTRG",
        mode=TransportationMode.RAIL,
        containers=["TCLU7654321"],
        gross_weight_kg=22000,
        temperature_control_required=True,
        target_temperature_celsius=4.0,
    )
    assert fo.temperature_control_required is True
    assert fo.target_temperature_celsius == 4.0
    assert fo.mode == TransportationMode.RAIL


def test_create_freight_order_unapproved_carrier(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="not in the approved carrier registry"):
        manager.create_freight_order(
            order_id="FO-111",
            carrier_code="UNKNOWN-LOGISTICS",
            origin_plant="DUNSANDEL-01",
            destination_port="NZLYT",
            mode=TransportationMode.ROAD,
            containers=["MSKU1234567"],
            gross_weight_kg=15000,
        )


def test_create_freight_order_invalid_container_format(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="Invalid ISO 6346 container identifier"):
        manager.create_freight_order(
            order_id="FO-112",
            carrier_code="MAINFRT-NZ",
            origin_plant="DUNSANDEL-01",
            destination_port="NZLYT",
            mode=TransportationMode.ROAD,
            containers=["INVALID_BOX"],
            gross_weight_kg=15000,
        )


def test_create_freight_order_overweight_solas(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="exceeds SOLAS maximum"):
        manager.create_freight_order(
            order_id="FO-113",
            carrier_code="MAINFRT-NZ",
            origin_plant="DUNSANDEL-01",
            destination_port="NZLYT",
            mode=TransportationMode.ROAD,
            containers=["MSKU1234567"],
            gross_weight_kg=35000,  # Max is 32,500 per container
        )


def test_create_freight_order_temperature_bounds(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="Target temperature 25.0C outside dairy cold-chain bounds"):
        manager.create_freight_order(
            order_id="FO-114",
            carrier_code="MAINFRT-NZ",
            origin_plant="DUNSANDEL-01",
            destination_port="NZLYT",
            mode=TransportationMode.ROAD,
            containers=["MSKU1234567"],
            gross_weight_kg=15000,
            temperature_control_required=True,
            target_temperature_celsius=25.0,
        )


def test_validate_export_declaration_success(manager: SapTmSdManager):
    decl = manager.validate_export_declaration(
        container_id="MSKU9876543",
        seal_number="NZCUSTOMS88192",
        mpi_cert_number="NZ-MPI-DAIRY-449102",
        tare_weight_kg=3800,
        cargo_weight_kg=24200,
        destination_country="CN",
    )
    assert decl.container_id == "MSKU9876543"
    assert decl.seal_number == "NZCUSTOMS88192"
    assert decl.mpi_cert_number == "NZ-MPI-DAIRY-449102"
    assert decl.tare_weight_kg == Decimal("3800")
    assert decl.cargo_weight_kg == Decimal("24200")
    assert decl.vgm_weight_kg == Decimal("28000")
    assert decl.destination_country == "CN"
    assert decl.customs_approved is True
    assert len(decl.audit_hash) == 64


def test_validate_export_declaration_invalid_mpi(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="MPI Certificate 'BAD-MPI-123' invalid"):
        manager.validate_export_declaration(
            container_id="MSKU9876543",
            seal_number="SEAL1234",
            mpi_cert_number="BAD-MPI-123",
            tare_weight_kg=3800,
            cargo_weight_kg=20000,
            destination_country="CN",
        )


def test_validate_export_declaration_short_seal(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="Container seal 'S12' must be at least 6"):
        manager.validate_export_declaration(
            container_id="MSKU9876543",
            seal_number="S12",
            mpi_cert_number="NZ-MPI-DAIRY-112233",
            tare_weight_kg=3800,
            cargo_weight_kg=20000,
            destination_country="CN",
        )


def test_validate_export_declaration_solas_vgm_breach(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="Calculated VGM 33800 kg exceeds maximum"):
        manager.validate_export_declaration(
            container_id="MSKU9876543",
            seal_number="SEAL998877",
            mpi_cert_number="NZ-MPI-DAIRY-112233",
            tare_weight_kg=3800,
            cargo_weight_kg=30000,  # 30000 + 3800 = 33800 > 32500
            destination_country="SG",
        )


def test_reconcile_ewm_staging_cleared(manager: SapTmSdManager):
    rec = manager.reconcile_ewm_staging(
        staging_bay="EWM-BAY-B04",
        delivery_id="DEL-990123",
        batch_lot="SYN-LOT-202611-01",
        pallet_count=20,
        qa_status=StagingQaStatus.QA_RELEASED,
    )
    assert rec.staging_bay == "EWM-BAY-B04"
    assert rec.delivery_id == "DEL-990123"
    assert rec.batch_lot == "SYN-LOT-202611-01"
    assert rec.pallet_count == 20
    assert rec.qa_status == StagingQaStatus.QA_RELEASED
    assert rec.is_cleared_for_loading is True
    assert "Cleared for container loading" in rec.notes


def test_reconcile_ewm_staging_blocked_qa(manager: SapTmSdManager):
    rec = manager.reconcile_ewm_staging(
        staging_bay="EWM-BAY-C12",
        delivery_id="DEL-990124",
        batch_lot="SYN-LOT-202611-02",
        pallet_count=18,
        qa_status=StagingQaStatus.PENDING_MICRO,
    )
    assert rec.is_cleared_for_loading is False
    assert "Loading blocked" in rec.notes
    assert "PENDING_MICRO" in rec.notes


def test_reconcile_ewm_staging_invalid_bay(manager: SapTmSdManager):
    with pytest.raises(ValueError, match="must match format 'EWM-BAY-"):
        manager.reconcile_ewm_staging(
            staging_bay="INVALID-BAY-99",
            delivery_id="DEL-1",
            batch_lot="LOT-1",
            pallet_count=10,
            qa_status=StagingQaStatus.QA_RELEASED,
        )


def test_generate_s4hana_odata_freight_payload(manager: SapTmSdManager):
    fo = manager.create_freight_order(
        order_id="FO-881122",
        carrier_code="KIWIRAIL-NZ",
        origin_plant="DUNSANDEL-01",
        destination_port="NZLYT",
        mode=TransportationMode.RAIL,
        containers=["KIRU1122334", "KIRU5566778"],
        gross_weight_kg=48000,
        temperature_control_required=False,
    )
    payload = manager.generate_s4hana_odata_freight_payload(fo)
    assert payload["@odata.context"] == "$metadata#A_FreightOrder/$entity"
    assert payload["FreightOrder"] == "FO-881122"
    assert payload["Carrier"] == "KIWIRAIL-NZ"
    assert payload["Shipper"] == "DUNSANDEL-01"
    assert payload["Consignee"] == "NZLYT"
    assert payload["TransportationMode"] == "RAIL"
    assert payload["TotalGrossWeight"] == "48000"
    assert len(payload["to_ContainerItem"]) == 2
    assert payload["to_ContainerItem"][0]["ContainerID"] == "KIRU1122334"


def test_generate_s4hana_odata_delivery_payload(manager: SapTmSdManager):
    sod = SalesOrderDelivery(
        sales_order_id="SO-550123",
        customer_id="CUST-GLOBAL-NUTRITION",
        delivery_id="DEL-880011",
        product_code="SYN-INFANT-BASE-01",
        batch_lot="DUN-202611-B4",
        quantity_mt=Decimal("24.50"),
        export_destination="CN",
        mpi_clearance_required=True,
    )
    payload = manager.generate_s4hana_odata_delivery_payload(sod)
    assert payload["@odata.context"] == "$metadata#A_OutboundDeliveryHeader/$entity"
    assert payload["DeliveryDocument"] == "DEL-880011"
    assert payload["DestinationCountry"] == "CN"
    assert payload["MpiClearanceRequired"] is True
    item = payload["to_DeliveryDocumentItem"][0]
    assert item["ReferenceSDDocument"] == "SO-550123"
    assert item["Material"] == "SYN-INFANT-BASE-01"
    assert item["ActualDeliveryQuantity"] == "24.50"
