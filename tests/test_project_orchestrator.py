import datetime
from fastapi.testclient import TestClient
import pytest

from civicintegrate.api import create_app
from civicintegrate.project_orchestrator import (
    IctProjectDeliveryOrchestrator,
    ProjectStatus,
    RiskSeverity,
    SmeIntegrationPipelineManager,
    TokenBucketRateLimiter,
)


def test_ict_project_creation_and_budgeting():
    orchestrator = IctProjectDeliveryOrchestrator()
    project = orchestrator.create_project(
        client_name="Auckland Commercial Logistics",
        project_type="SYSTEMS_INTEGRATION",
        hourly_rate_nzd=74.0,
        allocated_hours=160.0,
    )

    assert project.project_id.startswith("PRJ-")
    assert project.client_name == "Auckland Commercial Logistics"
    assert project.hourly_rate_nzd == 74.0
    assert project.budget_total_nzd == 160.0 * 74.0
    assert project.status == ProjectStatus.INITIATED
    assert project.overall_progress_percentage == 0.0

    # Log hours and check cost
    orchestrator.log_hours(project.project_id, 40.0)
    assert project.cost_to_date_nzd == 40.0 * 74.0


def test_milestone_and_risk_lifecycle():
    orchestrator = IctProjectDeliveryOrchestrator()
    project = orchestrator.create_project(
        client_name="Wellington Health Supplies",
        project_type="CUSTOM_SOFTWARE",
    )

    # Add milestones
    m1 = orchestrator.add_milestone(
        project.project_id,
        name="Architecture & Systems Audit",
        target_date=datetime.date(2026, 11, 20),
        deliverables=["API Schema", "Risk Assessment"],
    )
    m2 = orchestrator.add_milestone(
        project.project_id,
        name="Core Integration & UAT",
        target_date=datetime.date(2026, 12, 15),
        deliverables=["HubSpot Connector", "Xero Sync"],
    )
    assert len(project.milestones) == 2

    # Update milestone progress
    orchestrator.update_milestone_progress(project.project_id, m1.id, 100.0)
    orchestrator.update_milestone_progress(project.project_id, m2.id, 50.0)
    assert project.overall_progress_percentage == 75.0
    assert m1.completed is True
    assert m2.completed is False

    # Add risk
    risk = orchestrator.add_risk(
        project.project_id,
        category="THIRD_PARTY_API",
        description="Xero 60 req/min rate limit throttles bulk reconciliation",
        severity=RiskSeverity.HIGH,
        mitigation_strategy="Implement TokenBucketRateLimiter and exponential backoff queue",
    )
    assert risk is not None
    assert risk.severity == RiskSeverity.HIGH
    assert len(project.risk_register) == 1


def test_token_bucket_rate_limiter():
    limiter = TokenBucketRateLimiter(capacity=5, refill_rate_per_sec=10.0)
    # Acquire 5 tokens
    for _ in range(5):
        assert limiter.acquire() is True
    # 6th immediate token should fail
    assert limiter.acquire() is False


def test_sme_cross_platform_reconciliation():
    crm_customers = [
        {"id": "CRM-001", "name": "Apex Transport Ltd", "email": "admin@apextransport.co.nz"},
        {"id": "CRM-002", "name": "Kiwi Builders Group", "email": "accounts@kiwibuilders.nz"},
    ]

    finance_invoices = [
        {"invoice_id": "INV-101", "customer_id": "CRM-001", "job_id": "JOB-501", "total_amount": 12500.0, "amount_due": 0.0},
        # Orphaned invoice (no matching CRM customer)
        {"invoice_id": "INV-102", "customer_id": "CRM-999", "job_id": "JOB-502", "total_amount": 4200.0, "amount_due": 4200.0},
        # Amount mismatch with job
        {"invoice_id": "INV-103", "customer_id": "CRM-002", "job_id": "JOB-503", "total_amount": 8000.0, "amount_due": 8000.0},
    ]

    ops_jobs = [
        {"job_id": "JOB-501", "customer_id": "CRM-001", "quote_amount": 12500.0, "status": "COMPLETED"},
        {"job_id": "JOB-503", "customer_id": "CRM-002", "quote_amount": 7500.0, "status": "COMPLETED"},
        # Completed job without invoice
        {"job_id": "JOB-504", "customer_id": "CRM-002", "quote_amount": 3100.0, "status": "COMPLETED"},
    ]

    result = SmeIntegrationPipelineManager.reconcile_tripartite_data(
        crm_customers=crm_customers,
        finance_invoices=finance_invoices,
        ops_jobs=ops_jobs,
    )

    assert result["crm_customers_count"] == 2
    assert result["finance_invoices_count"] == 3
    assert result["ops_jobs_count"] == 3
    assert result["reconciliation_healthy"] is False

    discrepancy_types = [d["discrepancy_type"] for d in result["discrepancies"]]
    assert "ORPHANED_FINANCE_INVOICE" in discrepancy_types
    assert "UNINVOICED_COMPLETED_JOB" in discrepancy_types
    assert "AMOUNT_MISMATCH_JOB_VS_INVOICE" in discrepancy_types


def test_fastapi_project_endpoints():
    client = TestClient(create_app())

    # Create project
    res = client.post(
        "/api/v1/ict-projects/create",
        json={
            "client_name": "Pacific Freight Solutions",
            "project_type": "AI_WORKFLOW_AUTOMATION",
            "hourly_rate_nzd": 74.0,
            "allocated_hours": 120.0,
        },
    )
    assert res.status_code == 200
    p_data = res.json()
    proj_id = p_data["project_id"]
    assert p_data["budget_total_nzd"] == 120.0 * 74.0

    # Add milestone
    m_res = client.post(
        f"/api/v1/ict-projects/{proj_id}/milestones",
        json={
            "name": "Intake Workflow Automation",
            "target_date": "2026-11-30",
            "deliverables": ["AI Extraction Pipeline", "Webhook Handlers"],
        },
    )
    assert m_res.status_code == 200
    assert m_res.json()["name"] == "Intake Workflow Automation"

    # Add risk
    r_res = client.post(
        f"/api/v1/ict-projects/{proj_id}/risks",
        json={
            "category": "SCOPE_MANAGEMENT",
            "description": "Client requested custom PDF parsing beyond initial scope",
            "severity": "MEDIUM",
            "mitigation_strategy": "Agree change request and allocate additional sprint points",
        },
    )
    assert r_res.status_code == 200
    assert r_res.json()["severity"] == "MEDIUM"

    # Get project details
    get_res = client.get(f"/api/v1/ict-projects/{proj_id}")
    assert get_res.status_code == 200
    details = get_res.json()
    assert details["milestones_count"] == 1
    assert details["risks_count"] == 1

    # Call SME reconcile endpoint
    recon_res = client.post(
        "/api/v1/integrations/sme/reconcile",
        json={
            "crm_customers": [{"id": "C-1", "name": "Client 1"}],
            "finance_invoices": [{"invoice_id": "INV-1", "customer_id": "C-1"}],
            "ops_jobs": [{"job_id": "J-1", "status": "IN_PROGRESS"}],
        },
    )
    assert recon_res.status_code == 200
    assert recon_res.json()["reconciliation_healthy"] is True
