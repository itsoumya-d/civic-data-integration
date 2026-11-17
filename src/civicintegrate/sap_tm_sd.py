"""SAP Transportation Management (TM) and Sales & Distribution (SD) integration engine.

Designed for SAP S/4HANA (2023 FPS03 PCE) environments supporting manufacturing,
dairy processing, freight forwarding, MPI export declarations, and EWM warehouse staging.
"""
from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set


class TransportationMode(str, enum.Enum):
    ROAD = "ROAD"
    RAIL = "RAIL"
    OCEAN = "OCEAN"
    MULTI = "MULTI"


class FreightOrderStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    PLANNED = "PLANNED"
    DISPATCHED = "DISPATCHED"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class StagingQaStatus(str, enum.Enum):
    QA_RELEASED = "QA_RELEASED"
    PENDING_MICRO = "PENDING_MICRO"
    ON_HOLD = "ON_HOLD"
    REJECTED = "REJECTED"


CONTAINER_REGEX = re.compile(r"^[A-Z]{4}\d{7}$")
MPI_CERT_REGEX = re.compile(r"^NZ-MPI-DAIRY-\d{6}$")
STAGING_BAY_REGEX = re.compile(r"^EWM-BAY-[A-Z]\d{2}$")

APPROVED_CARRIERS: Set[str] = {
    "MAINFRT-NZ",
    "TIL-NZ",
    "KIWIRAIL-NZ",
    "MAERSK-OC",
    "MSC-OC",
    "CMA-CGM",
}

SOLAS_MAX_CONTAINER_WEIGHT_KG = Decimal("32500.00")
SOLAS_TARE_MIN_KG = Decimal("2000.00")
SOLAS_TARE_MAX_KG = Decimal("4800.00")


@dataclass(frozen=True)
class FreightOrder:
    order_id: str
    carrier_code: str
    origin_plant: str
    destination_port: str
    mode: TransportationMode
    containers: List[str]
    gross_weight_kg: Decimal
    status: FreightOrderStatus
    temperature_control_required: bool
    target_temperature_celsius: Optional[float]
    created_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExportDeclaration:
    declaration_id: str
    container_id: str
    seal_number: str
    mpi_cert_number: str
    tare_weight_kg: Decimal
    cargo_weight_kg: Decimal
    vgm_weight_kg: Decimal
    destination_country: str
    customs_approved: bool
    audit_hash: str
    certified_timestamp: datetime


@dataclass(frozen=True)
class SalesOrderDelivery:
    sales_order_id: str
    customer_id: str
    delivery_id: str
    product_code: str
    batch_lot: str
    quantity_mt: Decimal
    export_destination: str
    mpi_clearance_required: bool


@dataclass(frozen=True)
class StagingReconciliation:
    staging_bay: str
    delivery_id: str
    batch_lot: str
    pallet_count: int
    qa_status: StagingQaStatus
    is_cleared_for_loading: bool
    notes: str


class SapTmSdManager:
    """Orchestrates SAP S/4HANA TM & SD workflows, freight orders, and export compliance."""

    def __init__(self, default_plant: str = "DUNSANDEL-01") -> None:
        self.default_plant = default_plant

    def create_freight_order(
        self,
        order_id: str,
        carrier_code: str,
        origin_plant: str,
        destination_port: str,
        mode: TransportationMode,
        containers: List[str],
        gross_weight_kg: Decimal | float | str,
        temperature_control_required: bool = False,
        target_temperature_celsius: Optional[float] = None,
    ) -> FreightOrder:
        """Validates and instantiates an S/4HANA TM Freight Order."""
        if not order_id or not order_id.strip():
            raise ValueError("Freight Order ID cannot be empty")
        order_id = order_id.strip().upper()

        if carrier_code not in APPROVED_CARRIERS:
            raise ValueError(f"Carrier '{carrier_code}' is not in the approved carrier registry: {APPROVED_CARRIERS}")

        if not containers:
            raise ValueError("At least one container must be assigned to the freight order")

        for c in containers:
            if not CONTAINER_REGEX.match(c.strip().upper()):
                raise ValueError(f"Invalid ISO 6346 container identifier: {c}")

        try:
            w_dec = Decimal(str(gross_weight_kg))
        except (InvalidOperation, TypeError):
            raise ValueError(f"Invalid gross weight: {gross_weight_kg}")

        if w_dec <= Decimal("0"):
            raise ValueError("Gross weight must be positive")

        max_allowed = SOLAS_MAX_CONTAINER_WEIGHT_KG * len(containers)
        if w_dec > max_allowed:
            raise ValueError(f"Gross weight {w_dec} kg exceeds SOLAS maximum ({max_allowed} kg) for {len(containers)} container(s)")

        if temperature_control_required:
            if target_temperature_celsius is None:
                raise ValueError("Target temperature must be specified when temperature control is enabled")
            if not (-25.0 <= target_temperature_celsius <= 15.0):
                raise ValueError(f"Target temperature {target_temperature_celsius}C outside dairy cold-chain bounds (-25C to +15C)")

        return FreightOrder(
            order_id=order_id,
            carrier_code=carrier_code,
            origin_plant=origin_plant.strip().upper(),
            destination_port=destination_port.strip().upper(),
            mode=mode,
            containers=[c.strip().upper() for c in containers],
            gross_weight_kg=w_dec,
            status=FreightOrderStatus.PLANNED,
            temperature_control_required=temperature_control_required,
            target_temperature_celsius=target_temperature_celsius,
            created_at=datetime.now(timezone.utc),
            metadata={"source_erp": "SAP_S4HANA_2023_PCE"},
        )

    def validate_export_declaration(
        self,
        container_id: str,
        seal_number: str,
        mpi_cert_number: str,
        tare_weight_kg: Decimal | float | str,
        cargo_weight_kg: Decimal | float | str,
        destination_country: str,
    ) -> ExportDeclaration:
        """Validates container integrity, MPI export certification, and SOLAS VGM compliance."""
        c_clean = container_id.strip().upper()
        if not CONTAINER_REGEX.match(c_clean):
            raise ValueError(f"Container identifier '{container_id}' fails ISO 6346 validation")

        s_clean = seal_number.strip().upper()
        if len(s_clean) < 6:
            raise ValueError(f"Container seal '{seal_number}' must be at least 6 alphanumeric characters")

        m_clean = mpi_cert_number.strip().upper()
        if not MPI_CERT_REGEX.match(m_clean):
            raise ValueError(f"MPI Certificate '{mpi_cert_number}' invalid; must match 'NZ-MPI-DAIRY-XXXXXX'")

        tare = Decimal(str(tare_weight_kg))
        cargo = Decimal(str(cargo_weight_kg))

        if not (SOLAS_TARE_MIN_KG <= tare <= SOLAS_TARE_MAX_KG):
            raise ValueError(f"Container tare weight {tare} kg out of plausible standard range [{SOLAS_TARE_MIN_KG}, {SOLAS_TARE_MAX_KG}]")

        if cargo <= Decimal("0"):
            raise ValueError(f"Cargo weight {cargo} kg must be greater than zero")

        vgm = tare + cargo
        if vgm > SOLAS_MAX_CONTAINER_WEIGHT_KG:
            raise ValueError(f"Calculated VGM {vgm} kg exceeds maximum permissible container gross mass {SOLAS_MAX_CONTAINER_WEIGHT_KG} kg")

        dest_clean = destination_country.strip().upper()
        if len(dest_clean) != 2:
            raise ValueError(f"Destination country code '{destination_country}' must be ISO 3166-1 alpha-2")

        # Generate cryptographic audit hash for NZ Customs & Lyttelton Port pre-lodgement
        raw_token = f"{c_clean}:{s_clean}:{m_clean}:{vgm}:{dest_clean}"
        audit_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

        return ExportDeclaration(
            declaration_id=f"EXP-{c_clean[-6:]}-{dest_clean}",
            container_id=c_clean,
            seal_number=s_clean,
            mpi_cert_number=m_clean,
            tare_weight_kg=tare,
            cargo_weight_kg=cargo,
            vgm_weight_kg=vgm,
            destination_country=dest_clean,
            customs_approved=True,
            audit_hash=audit_hash,
            certified_timestamp=datetime.now(timezone.utc),
        )

    def reconcile_ewm_staging(
        self,
        staging_bay: str,
        delivery_id: str,
        batch_lot: str,
        pallet_count: int,
        qa_status: StagingQaStatus,
    ) -> StagingReconciliation:
        """Verifies warehouse staging bay allocation and enforces dairy microbiology release."""
        bay_clean = staging_bay.strip().upper()
        if not STAGING_BAY_REGEX.match(bay_clean):
            raise ValueError(f"Staging bay identifier '{staging_bay}' must match format 'EWM-BAY-[A-Z][0-9]{{2}}'")

        if pallet_count <= 0:
            raise ValueError(f"Pallet count {pallet_count} must be greater than zero")

        if not batch_lot or not batch_lot.strip():
            raise ValueError("Batch lot identifier is required for dairy export traceability")

        cleared = qa_status == StagingQaStatus.QA_RELEASED
        notes = (
            "Cleared for container loading and freight dispatch"
            if cleared
            else f"Loading blocked: product batch {batch_lot} has quality status '{qa_status.value}'"
        )

        return StagingReconciliation(
            staging_bay=bay_clean,
            delivery_id=delivery_id.strip().upper(),
            batch_lot=batch_lot.strip().upper(),
            pallet_count=pallet_count,
            qa_status=qa_status,
            is_cleared_for_loading=cleared,
            notes=notes,
        )

    def generate_s4hana_odata_freight_payload(self, fo: FreightOrder) -> Dict[str, Any]:
        """Serializes FreightOrder into canonical SAP S/4HANA OData v4 payload format."""
        return {
            "@odata.context": "$metadata#A_FreightOrder/$entity",
            "FreightOrder": fo.order_id,
            "TransportationOrderType": "TO01",
            "Carrier": fo.carrier_code,
            "Shipper": self.default_plant,
            "Consignee": fo.destination_port,
            "TransportationMode": fo.mode.value,
            "FreightOrderStatus": fo.status.value,
            "TotalGrossWeight": str(fo.gross_weight_kg),
            "WeightUnit": "KGM",
            "TemperatureControlled": fo.temperature_control_required,
            "TargetTemperature": fo.target_temperature_celsius,
            "TemperatureUnit": "CEL" if fo.temperature_control_required else None,
            "to_ContainerItem": [
                {
                    "ContainerID": cid,
                    "ContainerType": "40HR" if fo.temperature_control_required else "40GP",
                }
                for cid in fo.containers
            ],
            "CreationDateTime": fo.created_at.isoformat(),
        }

    def generate_s4hana_odata_delivery_payload(self, sod: SalesOrderDelivery) -> Dict[str, Any]:
        """Serializes SalesOrderDelivery into SAP S/4HANA OData delivery structure."""
        return {
            "@odata.context": "$metadata#A_OutboundDeliveryHeader/$entity",
            "DeliveryDocument": sod.delivery_id,
            "DeliveryDocumentType": "LF",
            "SoldToParty": sod.customer_id,
            "ShippingPoint": self.default_plant,
            "DestinationCountry": sod.export_destination,
            "MpiClearanceRequired": sod.mpi_clearance_required,
            "to_DeliveryDocumentItem": [
                {
                    "DeliveryDocumentItem": "000010",
                    "ReferenceSDDocument": sod.sales_order_id,
                    "Material": sod.product_code,
                    "Batch": sod.batch_lot,
                    "ActualDeliveryQuantity": str(sod.quantity_mt),
                    "DeliveryQuantityUnit": "TNE",
                }
            ],
        }
