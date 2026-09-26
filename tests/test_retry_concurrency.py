"""到期重试的并发消费：单接管、重复消费幂等、计数不漂移。"""
from __future__ import annotations

import logging
import threading

from orchestrator.models import (
    AlertEvent,
    ExecutionStatus as S,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

log = logging.getLogger("test.retry-concurrency")

TOKEN = "alice:operator"


def make_policy() -> RemediationPolicy:
    return RemediationPolicy(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=2, cooldown_seconds=60.0,
    )


def make_service(store=None) -> OrchestratorService:
    return OrchestratorService(
        store or Store(), [make_policy()], SuppressionRegistry(),
        base_backoff=10.0, record_version=2,
    )


def alert(alert_id="a1", fingerprint="fp-1") -> AlertEvent:
    return AlertEvent(alert_id, fingerprint, "res1", "high", {"app": "web"})


def prepare_waiting(svc, fingerprint="fp-1", alert_id="a1", now=0.0):
    r = svc.ingest_alert(alert(alert_id, fingerprint), now=now, token=TOKEN)
    svc.run_execution(r.execution_id, succeed=False, now=now)  # -> WAITING_RETRY@10
    return r.execution_id


def test_concurrent_batch_claim_single_owner():
    svc = make_service()
    n = 6
    for i in range(n):
        prepare_waiting(svc, fingerprint=f"fp-{i}", alert_id=f"a{i}")

    claimed_all: list = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def consumer():
        barrier.wait()
        got = svc.claim_due_retries(20.0, consumer="bg")
        with lock:
            claimed_all.extend(g.execution_id for g in got)

    threads = [threading.Thread(target=consumer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 每条记录恰好被一个消费方接管一次
    assert len(claimed_all) == n
    assert len(set(claimed_all)) == n
    # 再次消费（含到期判定）不得取走任何记录
    assert svc.claim_due_retries(20.0, consumer="bg2") == []
    log.info("并发批量接管: 唯一接管=%d 重复=%d", len(set(claimed_all)),
             len(claimed_all) - len(set(claimed_all)))

    for eid in claimed_all:
        rec = svc.get_execution(eid)
        assert rec.status is S.RUNNING and rec.attempt == 2  # 各递增一次，无漂移


def test_repeated_claim_is_idempotent_no_double_take():
    svc = make_service()
    eid = prepare_waiting(svc)
    first = svc.claim_due_retries(10.0)
    second = svc.claim_due_retries(10.0)
    third = svc.claim_due_retries(999.0)
    assert [x.execution_id for x in first] == [eid]
    assert second == [] and third == []
    rec = svc.get_execution(eid)
    assert rec.attempt == 2  # 重复消费不重复计数


def test_concurrent_single_run_only_one_promotes():
    svc = make_service()
    eid = prepare_waiting(svc)
    outcomes, errors = [], []
    lock = threading.Lock()
    barrier = threading.Barrier(4)

    def runner():
        barrier.wait()
        try:
            rec = svc.run_execution(eid, succeed=False, now=20.0)
            with lock:
                outcomes.append(rec)
        except TransitionError as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=runner) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("并发单条推进: 成功=%d 拒绝=%d 原因集合=%s",
             len(outcomes), len(errors), {e.reason.value for e in errors})
    assert len(outcomes) == 1 and len(errors) == 3
    # 失败方拿到明确结论（状态已被并发接管），而不是也执行一遍
    assert all(e.reason in (TransitionRejection.ILLEGAL_TRANSITION,
                            TransitionRejection.RETRY_NOT_DUE,
                            TransitionRejection.TERMINAL_STATE) for e in errors)
    rec = svc.get_execution(eid)
    # 只有一次接管把 attempt 从 1 推到 2；随后失败再进入等待
    assert rec.attempt == 2
    run_transitions = [a for a in svc._store.audit_log() if a[1] == "CLAIM_RETRY"]
    assert len(run_transitions) == 1  # 仅一次成功接管


def test_claim_then_complete_counts_each_attempt_once():
    svc = make_service()
    eid = prepare_waiting(svc)
    svc.claim_due_retries(10.0)                       # attempt 1 -> 2
    rec = svc.complete_execution(eid, succeed=False, now=10.0)
    assert rec.status is S.WAITING_RETRY and rec.attempt == 2
    assert rec.remaining_attempts == 1 and rec.next_run_at == 30.0  # 20s 退避

    svc.claim_due_retries(30.0)                       # attempt 2 -> 3
    rec = svc.complete_execution(eid, succeed=False, now=30.0)
    assert rec.status is S.ABANDONED and rec.attempt == 3
    assert rec.remaining_attempts == 0

    running = [a for a in svc._store.audit_log() if a[1] == "CLAIM_RETRY"]
    assert len(running) == 2  # 两次接管，各计一次，无重复执行
