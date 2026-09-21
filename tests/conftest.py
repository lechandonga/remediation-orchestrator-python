"""测试共享夹具与构造助手。"""

from __future__ import annotations

import threading

import pytest

from orchestrator.audit import AuditLog
from orchestrator.clock import FakeClock
from orchestrator.models import AlertEvent, Policy, Principal
from orchestrator.service import Orchestrator
from orchestrator.store import Store


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(1_000.0)


@pytest.fixture
def audit(clock) -> AuditLog:
    # echo=True：单测日志中打印事件、策略、状态流转与判定依据。
    return AuditLog(clock=clock, echo=True)


@pytest.fixture
def store(tmp_path) -> Store:
    return Store(tmp_path / "data")


@pytest.fixture
def policy() -> Policy:
    return Policy(
        policy_id="p-restart",
        version=1,
        name="restart-db",
        match_labels={"kind": "db"},
        action="restart",
        max_retries=2,
        backoff_base_seconds=1.0,
        priority=100,
    )


@pytest.fixture
def orchestrator(store, policy, clock, audit) -> Orchestrator:
    return Orchestrator(store, policies=[policy], clock=clock, audit=audit)


@pytest.fixture
def operator() -> Principal:
    return Principal("operator-1", frozenset({"remediation:execute"}))


@pytest.fixture
def readonly_user() -> Principal:
    return Principal("reader-1", frozenset({"read"}))


def make_alert(
    alert_id: str,
    *,
    resource_id: str = "db-1",
    labels=None,
    ts: float = 1000.0,
    window: float = 300.0,
    title: str = "disk full",
    source: str = "prometheus",
    severity: str = "critical",
) -> AlertEvent:
    return AlertEvent(
        alert_id=alert_id,
        source=source,
        resource_id=resource_id,
        severity=severity,
        title=title,
        fired_at=ts,
        labels=dict(labels if labels is not None else {"kind": "db"}),
        dedup_window_seconds=window,
    )


def run_threads(target, n: int, args_list=None) -> list:
    """并发运行 n 个线程；args_list 为每个线程提供参数元组（缺省为空）。"""
    results: list = []
    errors: list[BaseException] = []

    def worker(args) -> None:
        try:
            results.append(target(*args))
        except BaseException as exc:  # 捕获断言/业务异常用于主测试线程判定
            errors.append(exc)

    if args_list is None:
        args_list = [()] * n
    threads = [threading.Thread(target=worker, args=(args_list[i],)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors, f"thread raised: {errors!r}"
    return results
