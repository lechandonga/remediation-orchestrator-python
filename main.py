"""FastAPI 入口：暴露告警受理与执行查询接口。"""
from __future__ import annotations

import os
import time

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from orchestrator.authz import AuthzError
from orchestrator.models import AlertEvent, RemediationPolicy
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

DB_PATH = os.environ.get("ORCHESTRATOR_DB", "orchestrator.db")

# 示例策略：生产环境 web 应用告警 -> 重启动作
DEFAULT_POLICIES = [
    RemediationPolicy(
        policy_id="restart-web",
        version=1,
        priority=10,
        match_labels={"app": "web"},
        action="restart",
        max_retries=3,
        cooldown_seconds=300.0,
    ),
]

service = OrchestratorService(Store(DB_PATH), DEFAULT_POLICIES, SuppressionRegistry())
app = FastAPI(title="Remediation Orchestrator")


class AlertIn(BaseModel):
    alert_id: str
    fingerprint: str
    resource: str
    severity: str
    labels: dict[str, str] = {}


class RunIn(BaseModel):
    succeed: bool = True


@app.get("/")
def read_root():
    return {"service": "remediation-orchestrator", "status": "ok"}


@app.post("/alerts", status_code=201)
def ingest_alert(body: AlertIn, authorization: str | None = Header(default=None)):
    alert = AlertEvent(
        alert_id=body.alert_id,
        fingerprint=body.fingerprint,
        resource=body.resource,
        severity=body.severity,
        labels=body.labels,
        occurred_at=time.time(),
    )
    try:
        result = service.ingest_alert(alert, now=time.time(), token=authorization)
    except AuthzError as e:
        raise HTTPException(status_code=401 if e.reason.name == "UNAUTHENTICATED" else 403,
                            detail=e.reason.value)
    return {
        "accepted": result.accepted,
        "decision": result.decision,
        "reason": result.reason,
        "execution_id": result.execution_id,
    }


@app.post("/executions/{execution_id}/run")
def run_execution(execution_id: str, body: RunIn):
    try:
        record = service.run_execution(execution_id, succeed=body.succeed, now=time.time())
    except KeyError:
        raise HTTPException(status_code=404, detail="execution not found")
    except TransitionError as e:
        raise HTTPException(status_code=409, detail=f"{e.reason.value}: {e}")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt, "detail": record.detail}


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str):
    record = service.get_execution(execution_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution not found")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt, "policy_id": record.policy_id,
            "policy_version": record.policy_version, "detail": record.detail}
