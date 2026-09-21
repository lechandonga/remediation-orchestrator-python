"""HTTP 集成测试：接收去重、执行、重试、鉴权头与可区分错误码。"""

from __future__ import annotations

import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from orchestrator.api import create_app  # noqa: E402
from orchestrator.models import Policy  # noqa: E402


@pytest.fixture
def client(orchestrator):
    return TestClient(create_app(orchestrator))


POLICY = {
    "policy_id": "p-restart", "version": 1, "name": "restart-db",
    "match_labels": {"kind": "db"}, "action": "restart",
    "max_retries": 1, "backoff_base_seconds": 1.0, "priority": 100,
}


def _alert(aid):
    return {
        "alert_id": aid, "source": "prom", "resource_id": "db-1",
        "severity": "critical", "title": "disk full", "fired_at": 1000.0,
        "labels": {"kind": "db"}, "dedup_window_seconds": 300.0,
    }


def test_alert_dedup_and_execute_flow(client):
    # p-restart 已由 orchestrator 夹具预置；直接收发告警。
    a1 = client.post("/alerts", json=_alert("a-1")).json()
    a2 = client.post("/alerts", json=_alert("a-2")).json()
    assert a1["status"] == "dispatched" and a2["status"] == "deduped"
    assert a1["task_id"] == a2["task_id"]

    # 未认证 -> 401 unauthenticated
    assert client.post(f"/tasks/{a1['task_id']}/execute").status_code == 401
    # 权限不足 -> 403 forbidden
    denied = client.post(
        f"/tasks/{a1['task_id']}/execute",
        headers={"X-Principal": "reader", "X-Roles": "read"},
    )
    assert denied.status_code == 403 and denied.json()["code"] == "forbidden"
    # 授权执行 -> 成功
    ok = client.post(
        f"/tasks/{a1['task_id']}/execute",
        headers={"X-Principal": "op", "X-Roles": "remediation:execute"},
    )
    assert ok.status_code == 200 and ok.json()["state"] == "succeeded"


def test_no_policy_returns_explicit_code(store, clock, audit):
    # 独立编排器（不预置任何策略），避免其它用例注册的策略跨用例可见。
    from orchestrator.service import Orchestrator

    isolated = TestClient(create_app(Orchestrator(store, clock=clock, audit=audit)))
    body = isolated.post("/alerts", json=_alert("lonely")).json()
    assert body["status"] == "no_policy"
    assert body["reject_code"] == "no_policy_matched"


def test_illegal_transition_distinct_http_error(client):
    a1 = client.post("/alerts", json=_alert("a-1")).json()
    tid = a1["task_id"]
    headers = {"X-Principal": "op", "X-Roles": "remediation:execute"}
    # PENDING -> SUCCEEDED 非法：409 + illegal_transition
    r = client.post(f"/tasks/{tid}/transition/succeeded", headers=headers)
    assert r.status_code == 409 and r.json()["code"] == "illegal_transition"
