from __future__ import annotations

import datetime
from enum import Enum
import math
import time
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field


class ProjectStatus(str, Enum):
    INITIATED = "INITIATED"
    SCOPING = "SCOPING"
    IN_DEVELOPMENT = "IN_DEVELOPMENT"
    UAT = "UAT"
    DEPLOYED = "DEPLOYED"
    COMPLETED = "COMPLETED"


class RiskSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Milestone(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str
    target_date: datetime.date
    completed: bool = False
    deliverables: List[str] = Field(default_factory=list)
    completion_percentage: float = 0.0


class RiskItem(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    category: str
    description: str
    severity: RiskSeverity
    mitigation_strategy: str
    resolved: bool = False


class IctProject(BaseModel):
    project_id: str = Field(default_factory=lambda: f"PRJ-{uuid.uuid4().hex[:6].upper()}")
    client_name: str
    project_type: str
    hourly_rate_nzd: float = 74.0
    allocated_hours: float = 160.0
    hours_logged: float = 0.0
    status: ProjectStatus = ProjectStatus.INITIATED
    milestones: List[Milestone] = Field(default_factory=list)
    risk_register: List[RiskItem] = Field(default_factory=list)
    created_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    @property
    def budget_total_nzd(self) -> float:
        return self.allocated_hours * self.hourly_rate_nzd

    @property
    def cost_to_date_nzd(self) -> float:
        return self.hours_logged * self.hourly_rate_nzd

    @property
    def overall_progress_percentage(self) -> float:
        if not self.milestones:
            return 0.0
        return sum(m.completion_percentage for m in self.milestones) / len(self.milestones)


class ReconciliationDiscrepancy(BaseModel):
    entity_id: str
    discrepancy_type: str
    crm_value: Optional[Any] = None
    finance_value: Optional[Any] = None
    ops_value: Optional[Any] = None
    severity: str
    action_required: str


class SmeIntegrationPipelineManager:
    """Manages cross-platform integration between CRM, Finance (Xero), and Operations."""

    @staticmethod
    def reconcile_tripartite_data(
        crm_customers: List[Dict[str, Any]],
        finance_invoices: List[Dict[str, Any]],
        ops_jobs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        discrepancies: List[ReconciliationDiscrepancy] = []

        # Index CRM customers by email / id
        crm_by_id = {c.get("id"): c for c in crm_customers if c.get("id")}
        crm_by_email = {c.get("email", "").lower(): c for c in crm_customers if c.get("email")}

        # Index Ops jobs by customer_id or job_id
        jobs_by_id = {j.get("job_id"): j for j in ops_jobs if j.get("job_id")}

        # 1. Check for orphaned invoices (Invoice without known CRM customer)
        for inv in finance_invoices:
            cust_id = inv.get("customer_id")
            cust_email = inv.get("customer_email", "").lower()
            inv_id = inv.get("invoice_id", "UNKNOWN")

            matched = False
            if cust_id and cust_id in crm_by_id:
                matched = True
            elif cust_email and cust_email in crm_by_email:
                matched = True

            if not matched:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        entity_id=inv_id,
                        discrepancy_type="ORPHANED_FINANCE_INVOICE",
                        crm_value=None,
                        finance_value=inv.get("amount_due"),
                        ops_value=None,
                        severity="HIGH",
                        action_required=f"Invoice {inv_id} lacks corresponding CRM customer profile; link or create CRM entity.",
                    )
                )

        # 2. Check for completed operations jobs without invoices
        invoiced_job_ids = {inv.get("job_id") for inv in finance_invoices if inv.get("job_id")}
        for job in ops_jobs:
            job_id = job.get("job_id")
            status = job.get("status", "").upper()
            if status in ("COMPLETED", "DELIVERED") and job_id not in invoiced_job_ids:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        entity_id=job_id,
                        discrepancy_type="UNINVOICED_COMPLETED_JOB",
                        crm_value=job.get("customer_id"),
                        finance_value=None,
                        ops_value=status,
                        severity="CRITICAL",
                        action_required=f"Job {job_id} marked {status} but has no corresponding Xero invoice.",
                    )
                )

        # 3. Check for amount discrepancies between job quote and invoice
        for inv in finance_invoices:
            job_id = inv.get("job_id")
            if job_id and job_id in jobs_by_id:
                job = jobs_by_id[job_id]
                job_quote = float(job.get("quote_amount", 0.0))
                inv_total = float(inv.get("total_amount", 0.0))
                if math.fabs(job_quote - inv_total) > 0.01:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            entity_id=job_id,
                            discrepancy_type="AMOUNT_MISMATCH_JOB_VS_INVOICE",
                            crm_value=None,
                            finance_value=inv_total,
                            ops_value=job_quote,
                            severity="MEDIUM",
                            action_required=f"Job {job_id} quoted at ${job_quote:.2f} differs from invoice total ${inv_total:.2f}.",
                        )
                    )

        return {
            "crm_customers_count": len(crm_customers),
            "finance_invoices_count": len(finance_invoices),
            "ops_jobs_count": len(ops_jobs),
            "discrepancies_count": len(discrepancies),
            "reconciliation_healthy": len(discrepancies) == 0,
            "discrepancies": [d.model_dump() for d in discrepancies],
        }


class TokenBucketRateLimiter:
    """Rate limiter ensuring third-party API quotas (e.g. Xero 60 requests/min) are not breached."""

    def __init__(self, capacity: int = 60, refill_rate_per_sec: float = 1.0):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.refill_rate = refill_rate_per_sec
        self.last_update = time.monotonic()

    def acquire(self, tokens: int = 1) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_update
        self.last_update = now

        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class IctProjectDeliveryOrchestrator:
    """Manages active ICT projects, risk mitigation, and milestone progression."""

    def __init__(self):
        self._projects: Dict[str, IctProject] = {}

    def create_project(
        self,
        client_name: str,
        project_type: str,
        hourly_rate_nzd: float = 74.0,
        allocated_hours: float = 160.0,
    ) -> IctProject:
        project = IctProject(
            client_name=client_name,
            project_type=project_type,
            hourly_rate_nzd=hourly_rate_nzd,
            allocated_hours=allocated_hours,
        )
        self._projects[project.project_id] = project
        return project

    def get_project(self, project_id: str) -> Optional[IctProject]:
        return self._projects.get(project_id)

    def add_milestone(
        self,
        project_id: str,
        name: str,
        target_date: datetime.date,
        deliverables: Optional[List[str]] = None,
    ) -> Optional[Milestone]:
        project = self.get_project(project_id)
        if not project:
            return None
        milestone = Milestone(
            name=name,
            target_date=target_date,
            deliverables=deliverables or [],
        )
        project.milestones.append(milestone)
        return milestone

    def update_milestone_progress(
        self,
        project_id: str,
        milestone_id: str,
        completion_percentage: float,
        completed: bool = False,
    ) -> bool:
        project = self.get_project(project_id)
        if not project:
            return False
        for m in project.milestones:
            if m.id == milestone_id:
                m.completion_percentage = max(0.0, min(100.0, completion_percentage))
                m.completed = completed or (completion_percentage >= 100.0)
                return True
        return False

    def add_risk(
        self,
        project_id: str,
        category: str,
        description: str,
        severity: RiskSeverity,
        mitigation_strategy: str,
    ) -> Optional[RiskItem]:
        project = self.get_project(project_id)
        if not project:
            return None
        risk = RiskItem(
            category=category,
            description=description,
            severity=severity,
            mitigation_strategy=mitigation_strategy,
        )
        project.risk_register.append(risk)
        return risk

    def log_hours(self, project_id: str, hours: float) -> bool:
        project = self.get_project(project_id)
        if not project:
            return False
        project.hours_logged += max(0.0, hours)
        return True

    def advance_status(self, project_id: str, new_status: ProjectStatus) -> bool:
        project = self.get_project(project_id)
        if not project:
            return False
        project.status = new_status
        return True
