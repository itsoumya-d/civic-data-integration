"""Cloud ERP Migration Validator, Business Systems Integration Hub, and Operations KPI Engine.

Designed for modernizing nationwide business operations: migrating server-based legacy ERPs
to cloud ERP hubs (e.g. Odoo, TechnologyOne), orchestrating multi-system workflow integrations
(ERP, CRM, Job Management, Payroll/HR), generating real-time operational KPI dashboards,
and analyzing business automation proposals (AI vs script vs workflow vs template).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class MigrationStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    VALIDATED = "VALIDATED"
    CUTOVER_READY = "CUTOVER_READY"
    FAILED = "FAILED"


class SolutionType(str, Enum):
    TEMPLATE = "TEMPLATE"
    SCRIPT = "SCRIPT"
    STANDARD_WORKFLOW = "STANDARD_WORKFLOW"
    AI_AGENT = "AI_AGENT"


@dataclass
class MigrationValidationResult:
    entity_type: str
    total_records: int
    valid_records: int
    invalid_records: int
    errors: List[Dict[str, Any]]
    reconciliation_hash: str
    status: MigrationStatus
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "errors": self.errors,
            "reconciliation_hash": self.reconciliation_hash,
            "status": self.status.value,
            "timestamp": self.timestamp,
        }


class ErpMigrationValidator:
    """Validates data structures and integrity during on-premise to cloud ERP migration."""

    REQUIRED_FIELDS = {
        "inventory_item": ["sku", "name", "category", "unit_cost", "quantity_on_hand"],
        "customer": ["customer_id", "legal_name", "email", "billing_address", "credit_limit"],
        "vendor": ["vendor_id", "company_name", "nzbn", "payment_terms_days"],
        "work_order": ["order_id", "site_name", "assigned_crew", "start_date", "status"],
    }

    def __init__(self, target_system: str = "Cloud_ERP_Odoo"):
        self.target_system = target_system

    def compute_record_hash(self, record: Dict[str, Any]) -> str:
        serialized = json.dumps(record, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def validate_dataset(self, entity_type: str, records: List[Dict[str, Any]]) -> MigrationValidationResult:
        if entity_type not in self.REQUIRED_FIELDS:
            raise ValueError(f"Unsupported entity type for migration validation: {entity_type}")

        required = self.REQUIRED_FIELDS[entity_type]
        errors: List[Dict[str, Any]] = []
        valid_count = 0
        hasher = hashlib.sha256()

        for idx, rec in enumerate(records):
            rec_errors = []
            for field_name in required:
                if field_name not in rec or rec[field_name] is None or str(rec[field_name]).strip() == "":
                    rec_errors.append(f"Missing required field: '{field_name}'")

            # Numerical field sanity
            if entity_type == "inventory_item":
                qty = rec.get("quantity_on_hand")
                cost = rec.get("unit_cost")
                if isinstance(qty, (int, float)) and qty < 0:
                    rec_errors.append("quantity_on_hand cannot be negative")
                if isinstance(cost, (int, float)) and cost < 0:
                    rec_errors.append("unit_cost cannot be negative")

            elif entity_type == "customer":
                limit = rec.get("credit_limit")
                if isinstance(limit, (int, float)) and limit < 0:
                    rec_errors.append("credit_limit cannot be negative")

            if rec_errors:
                errors.append({"record_index": idx, "identifier": rec.get("sku") or rec.get("customer_id") or idx, "errors": rec_errors})
            else:
                valid_count += 1
                hasher.update(self.compute_record_hash(rec).encode("utf-8"))

        invalid_count = len(records) - valid_count
        status = MigrationStatus.CUTOVER_READY if invalid_count == 0 and len(records) > 0 else MigrationStatus.FAILED
        if invalid_count > 0 and valid_count > 0:
            status = MigrationStatus.VALIDATED

        return MigrationValidationResult(
            entity_type=entity_type,
            total_records=len(records),
            valid_records=valid_count,
            invalid_records=invalid_count,
            errors=errors,
            reconciliation_hash=hasher.hexdigest(),
            status=status,
        )


class BusinessSystemsIntegrationHub:
    """Orchestrates API event flows between Cloud ERP, CRM, Job Management, and Payroll systems."""

    def __init__(self, hub_name: str = "Enterprise_Integration_Core"):
        self.hub_name = hub_name
        self.subscribers: Dict[str, List[str]] = {}
        self.event_log: List[Dict[str, Any]] = []
        self.idempotency_cache: set[str] = set()

    def register_subscriber(self, topic: str, system_name: str) -> None:
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if system_name not in self.subscribers[topic]:
            self.subscribers[topic].append(system_name)

    def publish_event(self, topic: str, event_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if event_id in self.idempotency_cache:
            return {
                "status": "DUPLICATE_IGNORED",
                "event_id": event_id,
                "topic": topic,
                "message": "Event has already been processed idempotently.",
            }

        self.idempotency_cache.add(event_id)
        targets = self.subscribers.get(topic, [])
        dispatched_deliveries = []

        for target in targets:
            # Simulate dispatch with signature
            sig = hashlib.sha256(f"{event_id}:{target}:{json.dumps(payload)}".encode()).hexdigest()
            dispatched_deliveries.append({"target_system": target, "delivery_signature": sig, "status": "DELIVERED"})

        record = {
            "event_id": event_id,
            "topic": topic,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload_summary": list(payload.keys()),
            "deliveries": dispatched_deliveries,
        }
        self.event_log.append(record)
        return {"status": "SUCCESS", "event_id": event_id, "subscribers_notified": len(targets), "deliveries": dispatched_deliveries}


class OperationsKpiDashboardEngine:
    """Aggregates multi-system metrics into unified departmental and executive KPI summaries."""

    @staticmethod
    def calculate_nursery_operations_kpi(inventory_batches: List[Dict[str, Any]], work_orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_plants = sum(b.get("plant_count", 0) for b in inventory_batches)
        ready_for_dispatch = sum(b.get("plant_count", 0) for b in inventory_batches if b.get("stage") == "READY")
        growing_plants = sum(b.get("plant_count", 0) for b in inventory_batches if b.get("stage") == "GROWING")

        completed_orders = sum(1 for o in work_orders if o.get("status") == "COMPLETED")
        on_time_orders = sum(1 for o in work_orders if o.get("status") == "COMPLETED" and not o.get("delayed", False))

        dispatch_readiness_pct = (ready_for_dispatch / total_plants * 100.0) if total_plants > 0 else 0.0
        on_time_fulfillment_pct = (on_time_orders / completed_orders * 100.0) if completed_orders > 0 else 0.0

        return {
            "total_plants_in_cultivation": total_plants,
            "ready_for_dispatch_count": ready_for_dispatch,
            "growing_count": growing_plants,
            "dispatch_readiness_percentage": round(dispatch_readiness_pct, 2),
            "completed_work_orders": completed_orders,
            "on_time_fulfillment_percentage": round(on_time_fulfillment_pct, 2),
            "healthy_operations": on_time_fulfillment_pct >= 90.0 and dispatch_readiness_pct >= 15.0,
        }


class AutomationDecisionMatrix:
    """Evaluates business operational bottlenecks and prescribes the optimal automation tier."""

    @staticmethod
    def evaluate_case(
        problem_title: str,
        estimated_hours_saved_weekly: float,
        structured_data: bool,
        multi_system_orchestration: bool,
        unstructured_reasoning_required: bool,
        company_data_risk_level: str = "LOW",
    ) -> Dict[str, Any]:
        """Recommends whether to solve via TEMPLATE, SCRIPT, STANDARD_WORKFLOW, or AI_AGENT."""
        annual_hours_saved = estimated_hours_saved_weekly * 52.0

        if not unstructured_reasoning_required and not multi_system_orchestration and structured_data and estimated_hours_saved_weekly < 2.0:
            recommended = SolutionType.TEMPLATE
            rationale = "Bottleneck is purely formatting or standardized input; resolved immediately with a pre-configured template with zero maintenance."
        elif not unstructured_reasoning_required and not multi_system_orchestration and structured_data:
            recommended = SolutionType.SCRIPT
            rationale = "Deterministic data processing on a single platform; lightweight Python/SQL script delivers optimal speed and zero cloud overhead."
        elif not unstructured_reasoning_required and multi_system_orchestration:
            recommended = SolutionType.STANDARD_WORKFLOW
            rationale = "Cross-system event synchronization between ERP, CRM, and job platforms; best handled by robust workflow automation with built-in retries."
        else:
            if company_data_risk_level.upper() in ["HIGH", "CRITICAL"]:
                recommended = SolutionType.STANDARD_WORKFLOW
                rationale = "High data sensitivity prevents external LLM exposure; restricted to on-premise deterministic workflow and audited rule-based logic."
            else:
                recommended = SolutionType.AI_AGENT
                rationale = "Complex unstructured reasoning or ambiguous documents; best solved with localized LLM agent/automation pipeline with strict data guardrails."

        return {
            "problem_title": problem_title,
            "recommended_solution": recommended.value,
            "weekly_hours_saved": estimated_hours_saved_weekly,
            "annual_hours_saved": annual_hours_saved,
            "rationale": rationale,
            "governance_passed": True,
        }
