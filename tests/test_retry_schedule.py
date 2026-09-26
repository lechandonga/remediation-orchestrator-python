"""v2 定时排期重试：等待阶段、最早可执行时刻、剩余次数与三类可区分拒绝。"""
from __future__ import annotations

import logging

import pytest

from orchestrator.models import (
    AlertEvent,
    ExecutionStatus as S,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.retry import schedule_next_attempt
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

log = logging.getLogger("test.retry-schedule")

TOKEN = "alice:operator"


def make_policy(**kw) -> RemediationPolicy:
    base = dict(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=2, cooldown_seconds=60.0,
    )
    base.update(kw)
    return RemediationPolicy(**base)


def make_service(base_backoff=10.0, record_version=2, store=None) -> OrchestratorService:
    return OrchestratorService(
        store or Store(),
        [make_policy()],
        SuppressionRegistry(),
        base_backoff=base_backoff,
        record_version=record_version,
    )


def alert(alert_id="a1", fingerprint="fp-1") -> AlertEvent:
    return AlertEvent(alert_id, fingerprint, "res1", "high", {"app": "web"})


def fail_once(svc, eid, now):
    """受理后完成一次失败，返回进入等待阶段的记录。"""
    return svc.run_execution(eid, succeed=False, now=now)


# ---------- 纯函数：排期与剩余次数 ----------

def test_schedule_next_attempt_budget_and_exponential_backoff():
    # max_retries=2 -> 总预算 3 次
    s1 = schedule_next_attempt(1, 2, 10.0, now=100.0)
    assert s1.should_retry and s1.remaining_attempts == 2
    assert s1.backoff_seconds == 10.0 and s1.next_run_at == 110.0

    s2 = schedule_next_attempt(2, 2, 10.0, now=100.0)
    assert s2.should_retry and s2.remaining_attempts == 1
    assert s2.backoff_seconds == 20.0 and s2.next_run_at == 120.0

    s3 = schedule_next_attempt(3, 2, 10.0, now=100.0)
    assert not s3.should_retry and s3.remaining_attempts == 0
    assert s3.next_run_at == 0.0 and "RETRY_EXHAUSTED" in s3.reason
    log.info("排期纯函数: %s | %s | %s", s1.reason, s2.reason, s3.reason)


# ---------- 失败进入等待阶段，带明确排期 ----------

def test_failure_enters_waiting_with_schedule_and_remaining():
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    rec = fail_once(svc, r.execution_id, now=0.0)
    assert rec.status is S.WAITING_RETRY
    assert rec.record_version == 2
    assert rec.remaining_attempts == 2          # 总预算 3，已用 1
    assert rec.next_run_at == 10.0             # base_backoff=10
    assert rec.attempt == 1                    # 失败尝试只计一次
    log.info("等待排期: status=%s next=%s remain=%s",
             rec.status.value, rec.next_run_at, rec.remaining_attempts)


# ---------- 三类拒绝彼此可区分 ----------

def test_advance_before_due_rejected_with_retry_not_due():
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    fail_once(svc, eid, now=0.0)  # next_run_at=10

    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=False, now=9.99)
    log.info("未到期推进 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.RETRY_NOT_DUE

    # 被挡下时状态与计数不变，也没有新增执行
    rec = svc.get_execution(eid)
    assert rec.status is S.WAITING_RETRY and rec.attempt == 1
    assert rec.next_run_at == 10.0 and rec.remaining_attempts == 2
    running = [a for a in svc._store.audit_log() if "-> RUNNING" in a[3]]
    assert len(running) == 1  # 只有第一次 PENDING->RUNNING


def test_terminal_advance_rejected_with_terminal_state():
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    fail_once(svc, eid, now=0.0)
    svc.claim_due_retries(10.0)
    svc.complete_execution(eid, succeed=False, now=10.0)
    svc.claim_due_retries(40.0)
    final = svc.complete_execution(eid, succeed=False, now=40.0)  # 第 3 次失败
    assert final.status is S.ABANDONED

    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=False, now=1000.0)
    log.info("终态推进 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE
    assert ei.value.reason is not TransitionRejection.RETRY_NOT_DUE


def test_running_record_advance_rejected_with_illegal_transition():
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    fail_once(svc, eid, now=0.0)
    claimed = svc.claim_due_retries(10.0)
    assert len(claimed) == 1 and claimed[0].status is S.RUNNING

    # RUNNING 尚未回报结果，再次"推进执行"属于状态不允许
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=False, now=10.0)
    log.info("RUNNING 再推进 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.ILLEGAL_TRANSITION
    rec = svc.get_execution(eid)
    assert rec.status is S.RUNNING and rec.attempt == 2  # 未重复计数


def test_not_due_distinct_from_not_found_and_version_error():
    svc = make_service()
    # 不存在 -> KeyError，与任何状态/版本拒绝都不同
    with pytest.raises(KeyError):
        svc.run_execution("exec-missing", succeed=True, now=0.0)

    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    fail_once(svc, r.execution_id, now=0.0)
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(r.execution_id, succeed=True, now=1.0)
    assert ei.value.reason is TransitionRejection.RETRY_NOT_DUE  # 明确不是 404


# ---------- 到期才允许执行，批量列出 ----------

def test_due_listing_only_contains_due_waiting_records():
    svc = make_service()
    ids = []
    for i, fp in enumerate(["fp-a", "fp-b", "fp-c"]):
        r = svc.ingest_alert(alert(f"a{i}", fp), now=0.0, token=TOKEN)
        fail_once(svc, r.execution_id, now=float(i))  # next_run_at = i+10
        ids.append(r.execution_id)

    assert svc.list_due_retries(9.0) == []
    due10 = {x.execution_id for x in svc.list_due_retries(10.0)}
    assert due10 == {ids[0]}
    due11 = {x.execution_id for x in svc.list_due_retries(11.0)}
    assert due11 == {ids[0], ids[1]}
    due12 = {x.execution_id for x in svc.list_due_retries(12.0)}
    assert due12 == set(ids)
    log.info("到期集合@12=%s", due12)


def test_due_record_can_run_after_schedule_time_and_succeeds():
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    fail_once(svc, eid, now=0.0)  # next_run_at=10

    with pytest.raises(TransitionError):
        svc.run_execution(eid, succeed=True, now=5.0)
    rec = svc.run_execution(eid, succeed=True, now=10.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2
    assert rec.remaining_attempts == 0 and rec.next_run_at == 0.0


def test_exhausted_waiting_rejected_with_distinct_reason(tmp_path=None):
    """构造一条 remaining=0 的等待记录，验证 RETRY_ALREADY_EXHAUSTED 与未到期可区分。"""
    svc = make_service()
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    # 直接在存储层构造异常但合法的边界形态：WAITING_RETRY 且 remaining=0、已到期
    row = svc._store.get_execution(eid)
    svc._store.compare_and_set_status(
        eid, (S.PENDING,), S.WAITING_RETRY, 3, "边界：次数耗尽", now=0.0,
        next_run_at=0.0, remaining_attempts=0,
    )
    assert row is not None
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=True, now=1000.0)
    log.info("次数耗尽但仍在等待 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.RETRY_ALREADY_EXHAUSTED
    assert ei.value.reason is not TransitionRejection.RETRY_NOT_DUE
    assert ei.value.reason is not TransitionRejection.TERMINAL_STATE
