"""FastAPI 入口：暴露告警受理与执行查询接口。"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from orchestrator.authz import AuthzError
from orchestrator.models import (
    RECORD_VERSION_V2,
    AlertEvent,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.record_version import UnsupportedRecordVersionError
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

DB_PATH = os.environ.get("ORCHESTRATOR_DB", "orchestrator.db")
# 生产形态写入 v2 记录（失败后定时等待重试）；历史 v1 记录仍可被读懂并执行。
RECORD_VERSION = int(os.environ.get("ORCHESTRATOR_RECORD_VERSION", str(RECORD_VERSION_V2)))
BASE_BACKOFF = float(os.environ.get("ORCHESTRATOR_BASE_BACKOFF", "1.0"))

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

service = OrchestratorService(
    Store(DB_PATH),
    DEFAULT_POLICIES,
    SuppressionRegistry(),
    base_backoff=BASE_BACKOFF,
    record_version=RECORD_VERSION,
)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动恢复：把重启前悬置的 RUNNING 记录确定性收尾。
    recovered = service.recover_pending(now=time.time())
    for rec in recovered:
        print(f"[startup-recovery] {rec.execution_id} -> ABANDONED")
    yield


app = FastAPI(title="Remediation Orchestrator", lifespan=lifespan)


def _transition_http_status(reason: TransitionRejection) -> int:
    """把可区分拒绝原因映射为可区分 HTTP 状态。"""
    if reason in (
        TransitionRejection.RETRY_NOT_DUE,
        TransitionRejection.RETRY_ALREADY_EXHAUSTED,
    ):
        return 423  # Locked：重试被时间/次数挡住，区别于一般冲突
    return 409  # TERMINAL_STATE / ILLEGAL_TRANSITION / 并发接管失败


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
    except UnsupportedRecordVersionError as e:
        # 与 404（不存在）/ 409（状态不允许）分开：记录版本不受支持
        raise HTTPException(status_code=422, detail=str(e))
    except TransitionError as e:
        raise HTTPException(
            status_code=_transition_http_status(e.reason),
            detail={"reason": e.reason.value, "message": str(e)},
        )
    return _record_payload(record)


@app.get("/retries/due")
def list_due_retries():
    """只读列出当前所有已到期、等待重试的记录。"""
    now = time.time()
    records = service.list_due_retries(now)
    return {"now": now, "due": [_record_payload(r) for r in records]}


class ClaimIn(BaseModel):
    consumer: str = "background"
    limit: int | None = None


@app.post("/retries/claim")
def claim_due_retries(body: ClaimIn):
    """原子批量接管到期重试；并发消费同一条只有一方成功。"""
    now = time.time()
    records = service.claim_due_retries(now, consumer=body.consumer, limit=body.limit)
    return {"now": now, "claimed": [_record_payload(r) for r in records]}


@app.post("/executions/{execution_id}/complete")
def complete_execution(execution_id: str, body: RunIn):
    """回报已接管（RUNNING）记录的执行结果。"""
    try:
        record = service.complete_execution(execution_id, succeed=body.succeed, now=time.time())
    except KeyError:
        raise HTTPException(status_code=404, detail="execution not found")
    except UnsupportedRecordVersionError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except TransitionError as e:
        raise HTTPException(
            status_code=_transition_http_status(e.reason),
            detail={"reason": e.reason.value, "message": str(e)},
        )
    return _record_payload(record)


def _record_payload(record) -> dict:
    return {
        "execution_id": record.execution_id,
        "status": record.status.value,
        "attempt": record.attempt,
        "policy_id": record.policy_id,
        "policy_version": record.policy_version,
        "record_version": record.record_version,
        "next_run_at": record.next_run_at,
        "remaining_attempts": record.remaining_attempts,
        "detail": record.detail,
    }


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str):
    try:
        record = service.get_execution(execution_id)
    except UnsupportedRecordVersionError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if record is None:
        raise HTTPException(status_code=404, detail="execution not found")
    return _record_payload(record)
