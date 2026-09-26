"""编排流程测试：重复告警、并发执行、非法状态流转与重试耗尽。

日志中打印事件、策略、状态流转与判定依据，便于审计与排查。
"""
from __future__ import annotations

import logging
import threading

import pytest

from orchestrator.authz import AuthzError
from orchestrator.models import (
    AlertEvent,
    AuthzRejection,
    ExecutionStatus as S,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.policy import PolicyError, select_policy, validate_policy
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError, validate_transition
from orchestrator.store import Store
from orchestrator.suppression import MaintenanceWindow, SuppressionRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("test.orchestrator")

TOKEN = "alice:operator"


def make_policy(**kw) -> RemediationPolicy:
    base = dict(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=2, cooldown_seconds=60.0,
    )
    base.update(kw)
    return RemediationPolicy(**base)


def make_service(store=None, policies=None, suppression=None) -> OrchestratorService:
    return OrchestratorService(
        store or Store(),
        policies if policies is not None else [make_policy()],
        suppression or SuppressionRegistry(),
    )


def alert(alert_id="a1", fingerprint="fp-1", resource="res1", labels=None) -> AlertEvent:
    a = AlertEvent(alert_id, fingerprint, resource, "high", labels or {"app": "web"})
    log.info("事件: alert_id=%s fingerprint=%s resource=%s labels=%s",
             a.alert_id, a.fingerprint, a.resource, a.labels)
    return a


# ---------- 重复告警 ----------

def test_duplicate_alerts_deduped_with_stable_reason():
    svc = make_service()
    r1 = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    r2 = svc.ingest_alert(alert("a2"), now=1.0, token=TOKEN)
    r3 = svc.ingest_alert(alert("a3"), now=2.0, token=TOKEN)
    log.info("判定依据: first=%s second=%s third=%s", r1.decision, r2.decision, r3.decision)
    assert r1.accepted and r1.decision == "ACCEPTED"
    assert not r2.accepted and r2.decision == "DUPLICATE_ACTIVE"
    assert not r3.accepted and r3.decision == "DUPLICATE_ACTIVE"
    assert r2.reason == r3.reason  # 去重判定稳定且可解释


def test_dedup_survives_restart(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    r1 = svc1.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    assert r1.accepted

    # 模拟服务重启：同一数据库文件重新打开
    svc2 = make_service(store=Store(db))
    r2 = svc2.ingest_alert(alert("a2"), now=1.0, token=TOKEN)
    log.info("判定依据: 重启后重复告警 decision=%s reason=%s", r2.decision, r2.reason)
    assert not r2.accepted and r2.decision == "DUPLICATE_ACTIVE"


def test_cooldown_blocks_then_allows_after_expiry():
    svc = make_service()
    r1 = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    svc.run_execution(r1.execution_id, succeed=True, now=1.0)

    r2 = svc.ingest_alert(alert("a2"), now=30.0, token=TOKEN)
    log.info("判定依据: 冷却期内 decision=%s", r2.decision)
    assert not r2.accepted and r2.decision == "DUPLICATE_COOLDOWN"

    r3 = svc.ingest_alert(alert("a3"), now=61.1, token=TOKEN)
    log.info("判定依据: 冷却期后 decision=%s", r3.decision)
    assert r3.accepted


# ---------- 并发与幂等 ----------

def test_concurrent_ingest_single_execution():
    svc = make_service()
    barrier = threading.Barrier(8)
    results: list = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        r = svc.ingest_alert(alert(f"a{i}"), now=0.0, token=TOKEN)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.accepted]
    log.info("判定依据: 并发受理 accepted=%d rejected=%d", len(accepted), len(results) - len(accepted))
    assert len(accepted) == 1
    assert all(r.decision == "DUPLICATE_ACTIVE" for r in results if not r.accepted)


def test_concurrent_run_execution_idempotent():
    svc = make_service()
    r = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    barrier = threading.Barrier(2)
    outcomes: list = []
    errors: list = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        try:
            rec = svc.run_execution(r.execution_id, succeed=True, now=1.0)
            with lock:
                outcomes.append(rec)
        except TransitionError as e:
            with lock:
                errors.append(e)

    t1, t2 = threading.Thread(target=worker), threading.Thread(target=worker)
    t1.start(); t2.start(); t1.join(); t2.join()

    log.info("判定依据: 并发执行 成功=%d 冲突拒绝=%d", len(outcomes), len(errors))
    assert len(outcomes) == 1 and len(errors) == 1
    # 失败方拿到明确的失败结论：读到中间态 -> CAS 冲突（ILLEGAL_TRANSITION）；
    # 读到已完成态 -> 终态拒绝（TERMINAL_STATE），两者均可区分且不会重复执行
    assert errors[0].reason in (
        TransitionRejection.ILLEGAL_TRANSITION,
        TransitionRejection.TERMINAL_STATE,
    )
    rec = svc.get_execution(r.execution_id)
    assert rec.status is S.SUCCEEDED and rec.attempt == 1  # 无重复计数
    transitions = [a for a in svc._store.audit_log() if a[1] == "TRANSITION"]
    log.info("状态流转: %s", [a[3] for a in transitions])
    assert sum("-> RUNNING" in a[3] for a in transitions) == 1  # 无重复执行


# ---------- 非法状态流转 ----------

def test_illegal_transitions_have_distinguishable_reasons():
    with pytest.raises(TransitionError) as ei:
        validate_transition(S.PENDING, S.SUCCEEDED)
    log.info("判定依据: PENDING->SUCCEEDED 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.ILLEGAL_TRANSITION

    with pytest.raises(TransitionError) as ei:
        validate_transition(S.SUCCEEDED, S.RUNNING)
    log.info("判定依据: SUCCEEDED->RUNNING 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE
    assert ei.value.reason is not TransitionRejection.ILLEGAL_TRANSITION


def test_run_execution_on_terminal_state_rejected():
    svc = make_service()
    r = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    svc.run_execution(r.execution_id, succeed=True, now=1.0)
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(r.execution_id, succeed=True, now=2.0)
    log.info("判定依据: 终态再执行 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE
    rec = svc.get_execution(r.execution_id)
    assert rec.status is S.SUCCEEDED and rec.attempt == 1  # 拒绝不改变状态


# ---------- 重试与退避 ----------

def test_retry_exhaustion_reaches_terminal_without_drift():
    svc = make_service()
    r = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    eid = r.execution_id

    rec = svc.run_execution(eid, succeed=False, now=1.0)
    log.info("状态流转: attempt=%d status=%s detail=%s", rec.attempt, rec.status, rec.detail)
    # 新语义：失败后进入等待重试阶段，携带最早可执行时刻与剩余次数
    assert rec.status is S.WAITING_RETRY and rec.attempt == 1
    assert rec.next_attempt_at == 2.0 and rec.remaining_attempts == 2
    rec = svc.run_execution(eid, succeed=False, now=2.0)  # 到期后允许继续
    assert rec.status is S.WAITING_RETRY and rec.attempt == 2
    assert rec.next_attempt_at == 4.0 and rec.remaining_attempts == 1
    rec = svc.run_execution(eid, succeed=False, now=4.0)
    log.info("状态流转: attempt=%d status=%s detail=%s", rec.attempt, rec.status, rec.detail)
    assert rec.status is S.ABANDONED and rec.attempt == 3  # max_retries=2 -> 共 3 次尝试

    # 重试耗尽后进入终态，再次执行被拒绝
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=False, now=4.0)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE

    # 无重复计数：RUNNING 流转次数 == 实际尝试次数
    transitions = [a for a in svc._store.audit_log() if a[1] == "TRANSITION"]
    assert sum("-> RUNNING" in a[3] for a in transitions) == 3
    retry_logs = [a for a in svc._store.audit_log() if a[1] == "RETRY_DECISION"]
    log.info("判定依据: 重试决策=%s", [a[3] for a in retry_logs])
    assert any("RETRY_EXHAUSTED" in a[3] for a in retry_logs)


# ---------- 策略匹配与优先级 ----------

def test_policy_priority_deterministic_and_conflict_flagged():
    p_low = make_policy(policy_id="p-low", priority=10)
    p_high = make_policy(policy_id="p-high", priority=1, match_labels={"app": "web"})
    m = select_policy(alert("a1"), [p_low, p_high])
    log.info("策略: 命中=%s 依据=priority 数值小者优先", m.candidates)
    assert m.reason == "MATCHED" and m.policy.policy_id == "p-high"

    p_tie = make_policy(policy_id="p-tie", priority=1)
    m = select_policy(alert("a1"), [p_high, p_tie])
    log.info("策略: 冲突候选=%s", m.candidates)
    assert m.reason == "CONFLICT" and m.policy is None

    svc = make_service(policies=[p_high, p_tie])
    r = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    assert not r.accepted and r.decision == "POLICY_CONFLICT"


def test_no_match_flagged_not_dropped():
    svc = make_service()
    r = svc.ingest_alert(alert("a1", labels={"app": "db"}), now=0.0, token=TOKEN)
    log.info("判定依据: 无匹配 decision=%s reason=%s", r.decision, r.reason)
    assert not r.accepted and r.decision == "POLICY_NO_MATCH"


def test_policy_version_compatibility():
    validate_policy(make_policy(version=1))  # 旧版本策略仍可解释
    with pytest.raises(PolicyError):
        validate_policy(make_policy(version=99))  # 不兼容变更被明确拒绝
    with pytest.raises(PolicyError):
        make_service(policies=[make_policy(version=99)])


# ---------- 维护窗口与抑制 ----------

def test_maintenance_window_and_manual_suppression_distinguishable():
    reg = SuppressionRegistry()
    reg.add_window(MaintenanceWindow("w1", "res1", 100.0, 200.0))
    reg.suppress_fingerprint("fp-manual", "rule-7")
    svc = make_service(suppression=reg)

    r1 = svc.ingest_alert(alert("a1", fingerprint="fp-1"), now=150.0, token=TOKEN)
    log.info("判定依据: 维护窗口抑制 reason=%s", r1.reason)
    assert not r1.accepted and "MAINTENANCE_WINDOW" in r1.reason and "w1" in r1.reason

    r2 = svc.ingest_alert(alert("a2", fingerprint="fp-manual", resource="res2"),
                          now=300.0, token=TOKEN)
    log.info("判定依据: 手动抑制 reason=%s", r2.reason)
    assert not r2.accepted and "MANUAL_SUPPRESSION" in r2.reason and "rule-7" in r2.reason

    r3 = svc.ingest_alert(alert("a3", fingerprint="fp-1"), now=250.0, token=TOKEN)
    assert r3.accepted  # 窗口结束后正常受理


# ---------- 权限校验 ----------

def test_authz_rejection_distinguishable_and_state_unchanged():
    svc = make_service()
    r = svc.ingest_alert(alert("a1"), now=0.0, token=TOKEN)
    assert r.accepted

    with pytest.raises(AuthzError) as ei:
        svc.ingest_alert(alert("a2", fingerprint="fp-2"), now=1.0, token=None)
    log.info("判定依据: 无令牌 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is AuthzRejection.UNAUTHENTICATED

    with pytest.raises(AuthzError) as ei:
        svc.ingest_alert(alert("a3", fingerprint="fp-3"), now=1.0, token="bob:viewer")
    log.info("判定依据: 角色不足 拒绝原因=%s", ei.value.reason.value)
    assert ei.value.reason is AuthzRejection.FORBIDDEN_ROLE

    rec = svc.get_execution(r.execution_id)
    assert rec.status is S.PENDING and rec.attempt == 0  # 拒绝不改变已有执行状态
