"""FastAPI 入口：暴露告警受理与执行查询接口。"""
from __future__ import annotations

import os
import time

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from orchestrator.authz import AuthzError
from orchestrator.models import AlertEvent, RecordVersionError, RemediationPolicy
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
# 启动即恢复：悬挂的 RUNNING 记录收尾为 ABANDONED 并写审计；
# 未到期的 WAITING_RETRY 记录排期原样保留
service.recover_interrupted(time.time())
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
    except RecordVersionError as e:
        raise HTTPException(status_code=422, detail=f"UNSUPPORTED_RECORD_VERSION: {e}")
    except TransitionError as e:
        raise HTTPException(status_code=409, detail=f"{e.reason.value}: {e}")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt, "detail": record.detail}


@app.get("/retries/due")
def list_due_retries():
    """列出当前所有已到期的等待重试记录，供后台任务批量消费。

    到期集合中若存在版本高于当前支持的记录，返回 422
    UNSUPPORTED_RECORD_VERSION（携带全部违规记录），绝不静默跳过。
    """
    try:
        records = service.list_due_retries(now=time.time())
    except RecordVersionError as e:
        raise HTTPException(status_code=422, detail=f"UNSUPPORTED_RECORD_VERSION: {e}")
    return [
        {"execution_id": r.execution_id, "next_attempt_at": r.next_attempt_at,
         "remaining_attempts": r.remaining_attempts, "attempt": r.attempt}
        for r in records
    ]


@app.post("/retries/{execution_id}/claim")
def claim_due_retry(execution_id: str):
    """原子接管一条到期重试记录；并发下只有一个消费方成功。"""
    try:
        record = service.claim_due_retry(execution_id, now=time.time())
    except KeyError:
        raise HTTPException(status_code=404, detail="execution not found")
    except RecordVersionError as e:
        raise HTTPException(status_code=422, detail=f"UNSUPPORTED_RECORD_VERSION: {e}")
    except TransitionError as e:
        raise HTTPException(status_code=409, detail=f"{e.reason.value}: {e}")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt}


@app.post("/executions/{execution_id}/finish")
def finish_execution(execution_id: str, body: RunIn):
    """对已被接管（RUNNING）的记录上报执行结果。"""
    try:
        record = service.finish_execution(execution_id, succeed=body.succeed, now=time.time())
    except KeyError:
        raise HTTPException(status_code=404, detail="execution not found")
    except RecordVersionError as e:
        raise HTTPException(status_code=422, detail=f"UNSUPPORTED_RECORD_VERSION: {e}")
    except TransitionError as e:
        raise HTTPException(status_code=409, detail=f"{e.reason.value}: {e}")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt, "detail": record.detail}


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str):
    try:
        record = service.get_execution(execution_id)
    except RecordVersionError as e:
        raise HTTPException(status_code=422, detail=f"UNSUPPORTED_RECORD_VERSION: {e}")
    if record is None:
        raise HTTPException(status_code=404, detail="execution not found")
    return {"execution_id": record.execution_id, "status": record.status.value,
            "attempt": record.attempt, "policy_id": record.policy_id,
            "policy_version": record.policy_version, "detail": record.detail,
            "record_version": record.record_version,
            "next_attempt_at": record.next_attempt_at,
            "remaining_attempts": record.remaining_attempts}
