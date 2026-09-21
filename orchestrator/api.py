"""FastAPI 接入层：把编排能力以 HTTP 暴露（本地模拟运行）。

鉴权以请求头模拟：
    X-Principal: 用户标识
    X-Roles: 逗号分隔角色；包含 remediation:execute 才允许执行类操作。
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header
from pydantic import BaseModel, Field

from .errors import OrchestratorError
from .models import (
    AlertEvent,
    AlertStatus,
    Policy,
    Principal,
    SuppressionReason,
    TaskState,
)
from .service import Orchestrator


class AlertIn(BaseModel):
    alert_id: str
    source: str
    resource_id: str
    severity: str
    title: str
    fired_at: float
    labels: dict[str, str] = Field(default_factory=dict)
    dedup_window_seconds: float = 300.0


class PolicyIn(BaseModel):
    policy_id: str
    version: int = 1
    name: str
    match_labels: dict[str, str]
    action: str
    max_retries: int = 2
    backoff_base_seconds: float = 1.0
    priority: int = 100
    enabled: bool = True


def _principal(x_principal: str | None, x_roles: str | None) -> Principal | None:
    if not x_principal:
        return None
    roles = frozenset(r.strip() for r in (x_roles or "").split(",") if r.strip())
    return Principal(x_principal, roles)


def _enum(v: Any) -> str | None:
    return v.value if isinstance(v, (AlertStatus, TaskState, SuppressionReason)) else v


def create_app(orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="remediation-orchestrator", version="1.0.0")

    @app.exception_handler(OrchestratorError)
    async def _on_error(_request, exc: OrchestratorError):  # noqa: ANN001
        from fastapi.responses import JSONResponse

        # 不同拒绝原因映射到可区分的稳定 HTTP 响应体（code 即可审计原因码）。
        status = 409
        if exc.reason.value in ("unauthenticated",):
            status = 401
        elif exc.reason.value == "forbidden":
            status = 403
        elif exc.reason.value in ("task_not_found",):
            status = 404
        return JSONResponse(
            status_code=status,
            content={"rejected": True, "code": exc.reason.value, "detail": exc.detail},
        )

    @app.post("/policies")
    def add_policy(body: PolicyIn) -> dict:
        policy = Policy(**body.model_dump())
        orchestrator._registry.add(policy)
        return {"added": policy.policy_id, "version": policy.version}

    @app.post("/alerts")
    def receive(body: AlertIn) -> dict:
        event = AlertEvent(**body.model_dump())
        record = orchestrator.receive_alert(event)
        return {
            "alert_id": record.alert_id,
            "status": record.status.value,
            "fingerprint": record.fingerprint,
            "task_id": record.task_id,
            "selected_policy_id": record.selected_policy_id,
            "suppress_reason": _enum(record.suppress_reason),
            "suppress_detail": record.suppress_detail,
            "reject_code": record.reject_reason.value if record.reject_reason else None,
            "reject_detail": record.reject_detail,
        }

    @app.post("/tasks/{task_id}/execute")
    def execute(task_id: str, x_principal: str | None = Header(default=None),
                x_roles: str | None = Header(default=None)) -> dict:
        t = orchestrator.execute_task(task_id, _principal(x_principal, x_roles))
        return {"task_id": t.task_id, "state": t.state.value, "attempts": t.attempts,
                "revision": t.revision, "not_before_ts": t.not_before_ts,
                "last_error": t.last_error}

    @app.post("/tasks/{task_id}/retry")
    def retry(task_id: str, x_principal: str | None = Header(default=None),
              x_roles: str | None = Header(default=None)) -> dict:
        t = orchestrator.retry_task(task_id, _principal(x_principal, x_roles))
        return {"task_id": t.task_id, "state": t.state.value, "attempts": t.attempts,
                "not_before_ts": t.not_before_ts}

    @app.post("/tasks/{task_id}/transition/{target}")
    def transition(task_id: str, target: TaskState,
                   x_principal: str | None = Header(default=None),
                   x_roles: str | None = Header(default=None)) -> dict:
        t = orchestrator.transition(task_id, target, _principal(x_principal, x_roles))
        return {"task_id": t.task_id, "state": t.state.value, "revision": t.revision}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        t = orchestrator.get_task(task_id)
        if t is None:
            return {"found": False}
        return {"found": True, "task_id": t.task_id, "state": t.state.value,
                "attempts": t.attempts, "revision": t.revision, "max_retries": t.max_retries,
                "not_before_ts": t.not_before_ts, "last_error": t.last_error}

    @app.get("/alerts/{alert_id}")
    def get_alert(alert_id: str) -> dict:
        r = orchestrator.get_alert(alert_id)
        if r is None:
            return {"found": False}
        return {"found": True, "alert_id": r.alert_id, "status": r.status.value,
                "fingerprint": r.fingerprint, "task_id": r.task_id}

    return app
