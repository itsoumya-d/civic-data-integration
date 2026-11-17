"""Unit tests for Cloud ERP Migration Validator, Business Systems Integration Hub, and Operations KPI Engine."""
import pytest
from civicintegrate.erp_automation import (
    ErpMigrationValidator,
    BusinessSystemsIntegrationHub,
    OperationsKpiDashboardEngine,
    AutomationDecisionMatrix,
    MigrationStatus,
    SolutionType,
)


def test_erp_migration_validator_success():
    validator = ErpMigrationValidator(target_system="Odoo_Enterprise_Cloud")
    sample_inventory = [
        {"sku": "NZ-KAURI-10L", "name": "Agathis australis 10L", "category": "Native Trees", "unit_cost": 24.50, "quantity_on_hand": 1500},
        {"sku": "NZ-TOTARA-5L", "name": "Podocarpus totara 5L", "category": "Native Trees", "unit_cost": 14.00, "quantity_on_hand": 3200},
    ]

    result = validator.validate_dataset("inventory_item", sample_inventory)
    assert result.status == MigrationStatus.CUTOVER_READY
    assert result.total_records == 2
    assert result.valid_records == 2
    assert result.invalid_records == 0
    assert len(result.errors) == 0
    assert len(result.reconciliation_hash) == 64


def test_erp_migration_validator_failures():
    validator = ErpMigrationValidator()
    flawed_inventory = [
        {"sku": "BAD-1", "name": "", "category": "Trees", "unit_cost": -5.0, "quantity_on_hand": -10},
        {"sku": "GOOD-1", "name": "Harakeke Flax", "category": "Wetland", "unit_cost": 8.0, "quantity_on_hand": 500},
    ]

    result = validator.validate_dataset("inventory_item", flawed_inventory)
    assert result.status == MigrationStatus.VALIDATED
    assert result.total_records == 2
    assert result.valid_records == 1
    assert result.invalid_records == 1
    assert len(result.errors) == 1
    assert result.errors[0]["identifier"] == "BAD-1"


def test_erp_migration_unsupported_entity():
    validator = ErpMigrationValidator()
    with pytest.raises(ValueError, match="Unsupported entity type"):
        validator.validate_dataset("unsupported_entity", [])


def test_business_systems_integration_hub():
    hub = BusinessSystemsIntegrationHub()
    hub.register_subscriber("work_order.dispatched", "CRM_Platform")
    hub.register_subscriber("work_order.dispatched", "Job_Management_Field")

    event_payload = {"order_id": "WO-9901", "site": "Pūhoi-Warkworth Motorway Corridor", "plants_count": 5000}
    res = hub.publish_event("work_order.dispatched", "evt_101", event_payload)

    assert res["status"] == "SUCCESS"
    assert res["subscribers_notified"] == 2
    assert len(hub.event_log) == 1

    # Idempotency check
    dup_res = hub.publish_event("work_order.dispatched", "evt_101", event_payload)
    assert dup_res["status"] == "DUPLICATE_IGNORED"


def test_operations_kpi_dashboard_engine():
    inventory_batches = [
        {"batch_id": "B1", "stage": "READY", "plant_count": 25000},
        {"batch_id": "B2", "stage": "GROWING", "plant_count": 75000},
    ]
    work_orders = [
        {"order_id": "WO1", "status": "COMPLETED", "delayed": False},
        {"order_id": "WO2", "status": "COMPLETED", "delayed": False},
        {"order_id": "WO3", "status": "COMPLETED", "delayed": True},
        {"order_id": "WO4", "status": "IN_PROGRESS", "delayed": False},
    ]

    kpi = OperationsKpiDashboardEngine.calculate_nursery_operations_kpi(inventory_batches, work_orders)
    assert kpi["total_plants_in_cultivation"] == 100000
    assert kpi["ready_for_dispatch_count"] == 25000
    assert kpi["dispatch_readiness_percentage"] == 25.0
    assert kpi["completed_work_orders"] == 3
    assert kpi["on_time_fulfillment_percentage"] == 66.67
    assert kpi["healthy_operations"] is False  # fulfillment < 90%


def test_automation_decision_matrix():
    # Case 1: Template
    res_template = AutomationDecisionMatrix.evaluate_case(
        problem_title="Standard Dispatch Sheet",
        estimated_hours_saved_weekly=1.0,
        structured_data=True,
        multi_system_orchestration=False,
        unstructured_reasoning_required=False,
    )
    assert res_template["recommended_solution"] == SolutionType.TEMPLATE.value

    # Case 2: Script
    res_script = AutomationDecisionMatrix.evaluate_case(
        problem_title="Daily Inventory Count Sync",
        estimated_hours_saved_weekly=5.0,
        structured_data=True,
        multi_system_orchestration=False,
        unstructured_reasoning_required=False,
    )
    assert res_script["recommended_solution"] == SolutionType.SCRIPT.value

    # Case 3: Standard Workflow
    res_workflow = AutomationDecisionMatrix.evaluate_case(
        problem_title="ERP to CRM Customer Onboarding",
        estimated_hours_saved_weekly=8.0,
        structured_data=True,
        multi_system_orchestration=True,
        unstructured_reasoning_required=False,
    )
    assert res_workflow["recommended_solution"] == SolutionType.STANDARD_WORKFLOW.value

    # Case 4: AI Agent with low risk
    res_ai = AutomationDecisionMatrix.evaluate_case(
        problem_title="Landscape Tender Document Parsing",
        estimated_hours_saved_weekly=12.0,
        structured_data=False,
        multi_system_orchestration=True,
        unstructured_reasoning_required=True,
        company_data_risk_level="LOW",
    )
    assert res_ai["recommended_solution"] == SolutionType.AI_AGENT.value

    # Case 5: AI Agent blocked by high data risk -> fallback to workflow
    res_high_risk = AutomationDecisionMatrix.evaluate_case(
        problem_title="Confidential Payroll Reconciliation",
        estimated_hours_saved_weekly=10.0,
        structured_data=False,
        multi_system_orchestration=True,
        unstructured_reasoning_required=True,
        company_data_risk_level="HIGH",
    )
    assert res_high_risk["recommended_solution"] == SolutionType.STANDARD_WORKFLOW.value
