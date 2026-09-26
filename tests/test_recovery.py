"""重启恢复：悬置 RUNNING 确定性收尾、等待排期原样保留、不重复计数且幂等。"""
from __future__ import annotations

import logging

from orchestrator.models import (
    AlertEvent,
    ExecutionStatus as S,
    RemediationPolicy,
)
from orchestrator.service import OrchestratorService
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

log = logging.getLogger("test.recovery")

TOKEN = "alice:operator"


def make_policy(max_retries=2) -> RemediationPolicy:
    return RemediationPolicy(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=max_retries, cooldown_seconds=60.0,
    )


def make_service(store, record_version=2) -> OrchestratorService:
    return OrchestratorService(
        store, [make_policy()], SuppressionRegistry(),
        base_backoff=10.0, record_version=record_version,
    )


def alert(alert_id, fingerprint) -> AlertEvent:
    return AlertEvent(alert_id, fingerprint, "res1", "high", {"app": "web"})


def test_interrupted_running_abandoned_with_basis_and_no_recount(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(Store(db))
    r = svc1.ingest_alert(alert("a1", "fp-1"), now=0.0, token=TOKEN)
    eid = r.execution_id
    # 完成一次失败进入等待，再接管为 RUNNING，模拟崩溃前已开始但无结果
    svc1.run_execution(eid, succeed=False, now=0.0)   # WAITING@10, attempt=1
    svc1.claim_due_retries(10.0)                       # RUNNING, attempt=2
    assert svc1.get_execution(eid).status is S.RUNNING

    # 重启：重新打开同一数据库并执行恢复
    svc2 = make_service(Store(db))
    recovered = svc2.recover_pending(now=50.0)
    assert [x.execution_id for x in recovered] == [eid]
    rec = svc2.get_execution(eid)
    assert rec.status is S.ABANDONED
    assert rec.attempt == 2                 # attempt 原样保留，未重复计数
    assert rec.remaining_attempts == 0 and rec.next_run_at == 0.0

    audit = svc2._store.audit_log()
    basis = [a for a in audit if a[1] == "RECOVERY_ABANDONED"]
    assert len(basis) == 1 and "RECOVERY_INTERRUPTED" in basis[0][3]
    assert "attempt 保持 2" in basis[0][3]
    log.info("恢复依据: %s", basis[0][3])

    # 恢复后进入终态，再推进被终态规则拒绝
    from orchestrator.statemachine import TransitionError
    from orchestrator.models import TransitionRejection
    try:
        svc2.run_execution(eid, succeed=True, now=51.0)
        assert False, "终态应拒绝推进"
    except TransitionError as e:
        assert e.reason is TransitionRejection.TERMINAL_STATE


def test_waiting_schedule_preserved_across_restart(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(Store(db))
    r = svc1.ingest_alert(alert("a1", "fp-1"), now=0.0, token=TOKEN)
    eid = r.execution_id
    svc1.run_execution(eid, succeed=False, now=0.0)  # next_run_at=10, remain=2

    # 重启恢复：等待记录不被提前/推迟/清零
    svc2 = make_service(Store(db))
    recovered = svc2.recover_pending(now=5.0)
    assert recovered == []  # 等待记录不需要收尾
    rec = svc2.get_execution(eid)
    assert rec.status is S.WAITING_RETRY
    assert rec.next_run_at == 10.0
    assert rec.remaining_attempts == 2
    assert rec.attempt == 1

    # 恢复后仍按原排期：未到期被挡，到期才能接管
    from orchestrator.statemachine import TransitionError
    from orchestrator.models import TransitionRejection
    try:
        svc2.run_execution(eid, succeed=True, now=9.0)
        assert False
    except TransitionError as e:
        assert e.reason is TransitionRejection.RETRY_NOT_DUE
    claimed = svc2.claim_due_retries(10.0)
    assert [x.execution_id for x in claimed] == [eid]


def test_recovery_is_idempotent(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(Store(db))
    r = svc1.ingest_alert(alert("a1", "fp-1"), now=0.0, token=TOKEN)
    svc1.run_execution(r.execution_id, succeed=False, now=0.0)
    svc1.claim_due_retries(10.0)  # RUNNING

    svc2 = make_service(Store(db))
    first = svc2.recover_pending(now=50.0)
    second = svc2.recover_pending(now=60.0)
    third = svc2.recover_pending(now=70.0)
    assert len(first) == 1 and second == [] and third == []
    basis = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY_ABANDONED"]
    assert len(basis) == 1  # 不重复收尾、不重复写审计
    rec = svc2.get_execution(r.execution_id)
    assert rec.attempt == 2  # 计数不漂移


def test_recovery_does_not_duplicate_execute_terminal_untouched(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(Store(db))
    r = svc1.ingest_alert(alert("a1", "fp-1"), now=0.0, token=TOKEN)
    svc1.run_execution(r.execution_id, succeed=True, now=1.0)  # SUCCEEDED

    svc2 = make_service(Store(db))
    assert svc2.recover_pending(now=10.0) == []  # 终态不受影响
    rec = svc2.get_execution(r.execution_id)
    assert rec.status is S.SUCCEEDED and rec.attempt == 1
