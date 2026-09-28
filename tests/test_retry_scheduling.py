"""定时重试排期、重启恢复、并发消费与记录版本兼容的测试。

日志中打印排期字段、拒绝原因与恢复依据，便于审计与排查。
"""
from __future__ import annotations

import logging
import sqlite3
import threading

import pytest

from orchestrator.models import (
    AlertEvent,
    ExecutionStatus as S,
    RecordVersionError,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("test.retry_scheduling")

TOKEN = "alice:operator"


def make_policy(**kw) -> RemediationPolicy:
    base = dict(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=2, cooldown_seconds=60.0,
    )
    base.update(kw)
    return RemediationPolicy(**base)


def make_service(store=None) -> OrchestratorService:
    return OrchestratorService(store or Store(), [make_policy()], SuppressionRegistry())


def alert(alert_id="a1", fingerprint="fp-1") -> AlertEvent:
    return AlertEvent(alert_id, fingerprint, "res1", "high", {"app": "web"})


def ingest(svc, alert_id="a1", fingerprint="fp-1", now=0.0) -> str:
    r = svc.ingest_alert(alert(alert_id, fingerprint), now=now, token=TOKEN)
    assert r.accepted
    return r.execution_id


def insert_raw_execution(store: Store, execution_id: str, status: str, **kw) -> None:
    """直接写入指定版本的执行记录（模拟历史/未来版本的存量数据）。"""
    cols = {
        "fingerprint": "fp-raw", "policy_id": "p1", "policy_version": 1,
        "attempt": 0, "detail": "", "updated_at": 0.0,
        "record_version": 2, "next_attempt_at": 0.0, "remaining_attempts": -1,
    }
    cols.update(kw)
    with store._lock, store._conn:
        store._conn.execute(
            "INSERT INTO executions (execution_id, fingerprint, policy_id, policy_version,"
            " status, attempt, detail, updated_at, record_version, next_attempt_at,"
            " remaining_attempts) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                execution_id, cols["fingerprint"], cols["policy_id"], cols["policy_version"],
                status, cols["attempt"], cols["detail"], cols["updated_at"],
                cols["record_version"], cols["next_attempt_at"], cols["remaining_attempts"],
            ),
        )


# ---------- 定时排期 ----------

def test_failure_enters_waiting_with_schedule():
    svc = make_service()
    eid = ingest(svc)
    rec = svc.run_execution(eid, succeed=False, now=10.0)
    log.info("排期: status=%s next_attempt_at=%s remaining=%s",
             rec.status, rec.next_attempt_at, rec.remaining_attempts)
    assert rec.status is S.WAITING_RETRY
    assert rec.next_attempt_at == 11.0  # base_backoff=1.0，第 1 次失败退避 1s
    assert rec.remaining_attempts == 2


def test_rejection_reasons_are_distinguishable():
    svc = make_service()
    eid = ingest(svc)
    svc.run_execution(eid, succeed=False, now=10.0)  # -> WAITING_RETRY, due at 11.0

    # 1) 还没到时间
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid, succeed=True, now=10.5)
    log.info("拒绝原因(未到期): %s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.RETRY_NOT_DUE

    # 2) 当前状态根本不允许推进（终态）
    eid2 = ingest(svc, "a2", "fp-2")
    svc.run_execution(eid2, succeed=True, now=10.0)
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(eid2, succeed=True, now=10.5)
    log.info("拒绝原因(终态): %s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE

    # 3) 已经不能再试（等待重试但剩余次数为 0）
    insert_raw_execution(svc._store, "exec-exhausted", "WAITING_RETRY",
                         next_attempt_at=0.0, remaining_attempts=0)
    with pytest.raises(TransitionError) as ei:
        svc.run_execution("exec-exhausted", succeed=True, now=10.5)
    log.info("拒绝原因(次数耗尽): %s", ei.value.reason.value)
    assert ei.value.reason is TransitionRejection.RETRY_EXHAUSTED

    # 三类原因两两不同
    assert len({TransitionRejection.RETRY_NOT_DUE, TransitionRejection.TERMINAL_STATE,
                TransitionRejection.RETRY_EXHAUSTED}) == 3
    # 记录不存在是另一种可区分结论
    with pytest.raises(KeyError):
        svc.run_execution("exec-missing", succeed=True, now=10.5)

    # 时间到了之后才允许继续尝试
    rec = svc.run_execution(eid, succeed=True, now=11.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2


def test_not_due_rejection_has_no_side_effects():
    svc = make_service()
    eid = ingest(svc)
    svc.run_execution(eid, succeed=False, now=10.0)
    with pytest.raises(TransitionError):
        svc.run_execution(eid, succeed=True, now=10.5)
    rec = svc.get_execution(eid)
    # 挡下不改变状态、不计数
    assert rec.status is S.WAITING_RETRY and rec.attempt == 1
    assert rec.next_attempt_at == 11.0 and rec.remaining_attempts == 2
    transitions = [a for a in svc._store.audit_log() if a[1] == "TRANSITION"]
    assert sum("-> RUNNING" in a[3] for a in transitions) == 1


# ---------- 到期列表与并发消费 ----------

def test_list_due_retries_only_returns_due():
    svc = make_service()
    eid_due = ingest(svc, "a1", "fp-1")
    svc.run_execution(eid_due, succeed=False, now=10.0)   # due at 11.0
    eid_later = ingest(svc, "a2", "fp-2")
    svc.run_execution(eid_later, succeed=False, now=10.0)
    # 把第二条人为推迟到 100.0
    svc._store.compare_and_set_status(
        eid_later, (S.WAITING_RETRY,), S.WAITING_RETRY, 1, "推迟", 10.0,
        next_attempt_at=100.0, remaining_attempts=2,
    )

    due = svc.list_due_retries(now=11.0)
    log.info("到期记录: %s", [r.execution_id for r in due])
    assert [r.execution_id for r in due] == [eid_due]
    assert [r.execution_id for r in svc.list_due_retries(now=101.0)] == [eid_due, eid_later]


def test_concurrent_claim_single_winner_no_drift():
    svc = make_service()
    eid = ingest(svc)
    svc.run_execution(eid, succeed=False, now=10.0)  # WAITING_RETRY, due at 11.0, attempt=1

    barrier = threading.Barrier(8)
    wins: list = []
    errors: list = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        try:
            rec = svc.claim_due_retry(eid, now=11.0)
            with lock:
                wins.append(rec)
        except TransitionError as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("并发接管: 成功=%d 失败=%d", len(wins), len(errors))
    assert len(wins) == 1 and len(errors) == 7
    assert all(e.reason is TransitionRejection.ILLEGAL_TRANSITION for e in errors)

    rec = svc.get_execution(eid)
    assert rec.status is S.RUNNING and rec.attempt == 2  # 计数只增加一次

    # 反复消费不得再次取走同一条记录
    with pytest.raises(TransitionError) as ei:
        svc.claim_due_retry(eid, now=12.0)
    assert ei.value.reason is TransitionRejection.ILLEGAL_TRANSITION
    assert svc.list_due_retries(now=12.0) == []  # 已不在到期列表中

    # 接管方上报结果后计数不漂移
    rec = svc.finish_execution(eid, succeed=True, now=12.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2
    transitions = [a for a in svc._store.audit_log() if a[1] == "TRANSITION"]
    assert sum("-> RUNNING" in a[3] for a in transitions) == 2  # 首次执行 + 到期接管


def test_claim_before_due_rejected():
    svc = make_service()
    eid = ingest(svc)
    svc.run_execution(eid, succeed=False, now=10.0)
    with pytest.raises(TransitionError) as ei:
        svc.claim_due_retry(eid, now=10.5)
    assert ei.value.reason is TransitionRejection.RETRY_NOT_DUE
    rec = svc.get_execution(eid)
    assert rec.status is S.WAITING_RETRY and rec.attempt == 1  # 无副作用


# ---------- 重启恢复 ----------

def test_recovery_finalizes_interrupted_and_preserves_schedule(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    eid_running = ingest(svc1, "a1", "fp-1")
    # 模拟执行开始但未给出结果（RUNNING 悬挂）
    svc1._store.compare_and_set_status(
        eid_running, (S.PENDING,), S.RUNNING, 1, "开始执行", 5.0
    )
    eid_waiting = ingest(svc1, "a2", "fp-2")
    svc1.run_execution(eid_waiting, succeed=False, now=6.0)  # WAITING_RETRY, due at 7.0
    before = svc1.get_execution(eid_waiting)
    svc1._store.close()

    # 模拟服务重启：同一数据库文件重新打开并执行恢复
    svc2 = make_service(store=Store(db))
    recovered = svc2.recover_interrupted(now=50.0)
    log.info("恢复: recovered=%s", recovered)
    assert recovered == [eid_running]

    rec = svc2.get_execution(eid_running)
    assert rec.status is S.ABANDONED and rec.attempt == 1  # 不重复计数
    audits = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    log.info("恢复依据: %s", [a[3] for a in audits])
    assert len(audits) == 1 and "RECOVERY_INTERRUPTED" in audits[0][3]

    # 未到期的等待重试记录：排期字段原样保留
    after = svc2.get_execution(eid_waiting)
    assert after.status is S.WAITING_RETRY
    assert after.next_attempt_at == before.next_attempt_at == 7.0
    assert after.remaining_attempts == before.remaining_attempts == 2
    assert after.attempt == before.attempt == 1

    # 恢复幂等：再次执行不产生新的收尾或审计
    assert svc2.recover_interrupted(now=51.0) == []
    audits = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    assert len(audits) == 1

    # 恢复后到期记录仍可被正常消费，且不重复执行
    rec = svc2.run_execution(eid_waiting, succeed=True, now=7.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2


# ---------- 记录版本兼容 ----------

def test_legacy_v1_record_keeps_old_semantics():
    svc = make_service()
    insert_raw_execution(svc._store, "exec-v1", "PENDING", record_version=1)

    rec = svc.run_execution("exec-v1", succeed=False, now=10.0)
    log.info("v1 记录失败后: status=%s", rec.status)
    # v1 旧语义：失败后立即回到 PENDING，而不是进入定时排期
    assert rec.status is S.PENDING and rec.attempt == 1

    # 立即可再次推进（无排期门控）
    rec = svc.run_execution("exec-v1", succeed=True, now=10.5)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2


def test_old_schema_db_migrated_and_readable(tmp_path):
    db = str(tmp_path / "legacy.db")
    # 构造 v1 时代的历史库（executions 表无排期/版本列）
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE executions (
            execution_id   TEXT PRIMARY KEY,
            fingerprint    TEXT NOT NULL,
            policy_id      TEXT NOT NULL,
            policy_version INTEGER NOT NULL,
            status         TEXT NOT NULL,
            attempt        INTEGER NOT NULL DEFAULT 0,
            detail         TEXT NOT NULL DEFAULT '',
            updated_at     REAL NOT NULL DEFAULT 0
        );
        INSERT INTO executions (execution_id, fingerprint, policy_id, policy_version,
                                status, attempt, detail, updated_at)
        VALUES ('exec-legacy', 'fp-legacy', 'p1', 1, 'PENDING', 0, '', 0.0);
        """
    )
    conn.close()

    svc = make_service(store=Store(db))  # 启动时幂等迁移
    rec = svc.get_execution("exec-legacy")
    log.info("迁移后读取: version=%d status=%s", rec.record_version, rec.status)
    assert rec.record_version == 1 and rec.status is S.PENDING
    assert rec.next_attempt_at == 0.0 and rec.remaining_attempts == -1

    # 历史记录按原语义继续参与执行，不被当作数据缺失或失败
    rec = svc.run_execution("exec-legacy", succeed=True, now=1.0)
    assert rec.status is S.SUCCEEDED


def test_unsupported_future_version_rejected_distinctly():
    svc = make_service()
    insert_raw_execution(svc._store, "exec-v3", "WAITING_RETRY", record_version=3)

    # 读取与推进都明确拒绝，且与"记录不存在"、"状态不允许推进"分得开
    with pytest.raises(RecordVersionError):
        svc.get_execution("exec-v3")
    with pytest.raises(RecordVersionError):
        svc.run_execution("exec-v3", succeed=True, now=0.0)
    with pytest.raises(KeyError):
        svc.run_execution("exec-missing", succeed=True, now=0.0)
    with pytest.raises(TransitionError):
        insert_raw_execution(svc._store, "exec-terminal", "SUCCEEDED")
        svc.run_execution("exec-terminal", succeed=True, now=0.0)

    # 不受支持版本不会混入到期列表被静默批量消费：批量口径与单条一致，明确拒绝
    # 且能从异常中拿到是哪条记录、哪个版本被拒绝
    with pytest.raises(RecordVersionError) as ei:
        svc.list_due_retries(now=100.0)
    log.info("批量拒绝依据: %s", ei.value)
    assert ei.value.execution_id == "exec-v3"
    assert ei.value.record_version == 3
    assert ei.value.unsupported == (("exec-v3", 3),)


# ---------- 批量列遇到不支持版本：明确拒绝而非静默跳过 ----------

def test_list_due_retries_single_unsupported_explicitly_rejected():
    """只有一条到期的高版本记录：批量口径必须明确报错，而不是返回空列表。"""
    svc = make_service()
    insert_raw_execution(
        svc._store, "exec-v3", "WAITING_RETRY", record_version=3,
        next_attempt_at=5.0, remaining_attempts=2,
    )

    with pytest.raises(RecordVersionError) as ei:
        svc.list_due_retries(now=10.0)
    log.info("单条高版本批量拒绝: %s", ei.value)
    # 明确、可区分、可定位到具体记录
    assert ei.value.unsupported == (("exec-v3", 3),)
    assert "exec-v3" in str(ei.value)

    # 读操作失败不留副作用：记录仍是原状，未被消费、计数不变
    with pytest.raises(RecordVersionError):
        svc.get_execution("exec-v3")


def test_list_due_retries_mixed_unsupported_and_normal_due():
    """正常到期记录与高版本到期记录混在一起：明确拒绝并点名违规记录，
    正常记录不受影响、仍可被消费；与不存在/状态不允许推进分得开。"""
    svc = make_service()
    eid_normal = ingest(svc, "a1", "fp-1")
    svc.run_execution(eid_normal, succeed=False, now=10.0)  # v2，due at 11.0
    insert_raw_execution(
        svc._store, "exec-v3", "WAITING_RETRY", record_version=3,
        next_attempt_at=10.0, remaining_attempts=1,
    )
    insert_raw_execution(
        svc._store, "exec-v3-future", "WAITING_RETRY", record_version=3,
        next_attempt_at=999.0, remaining_attempts=1,  # 未到期，不参与批量口径
    )

    with pytest.raises(RecordVersionError) as ei:
        svc.list_due_retries(now=11.0)
    log.info("混合场景批量拒绝: %s", ei.value)
    # 只点到期的那条高版本记录；未到期的不在拒绝集合里
    assert ei.value.unsupported == (("exec-v3", 3),)

    # 报错是只读失败：正常记录没有被连带消费，仍在到期集合里
    # 把高版本记录的排期推到未来后，列表只剩正常记录，语义与当前版本完全一致
    with svc._store._lock, svc._store._conn:
        svc._store._conn.execute(
            "UPDATE executions SET next_attempt_at = 999.0 WHERE execution_id = ?",
            ("exec-v3",),
        )
    due = svc.list_due_retries(now=11.0)
    assert [r.execution_id for r in due] == [eid_normal]

    # 正常记录照常被接管并完成，行为不受高版本记录影响
    rec = svc.claim_due_retry(eid_normal, now=11.0)
    assert rec.status is S.RUNNING and rec.attempt == 2
    rec = svc.finish_execution(eid_normal, succeed=True, now=11.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2


def test_list_due_retries_three_conclusions_remain_distinguishable():
    """批量拒绝结论必须始终与"不存在""状态不允许推进"区分得开。"""
    svc = make_service()
    insert_raw_execution(
        svc._store, "exec-v3", "WAITING_RETRY", record_version=3, next_attempt_at=0.0
    )
    insert_raw_execution(svc._store, "exec-terminal", "SUCCEEDED")

    # 1) 版本不支持
    with pytest.raises(RecordVersionError):
        svc.list_due_retries(now=100.0)
    # 2) 记录不存在（单条口径，KeyError）
    with pytest.raises(KeyError):
        svc.run_execution("exec-missing", succeed=True, now=100.0)
    # 3) 状态不允许推进（终态，TransitionError/TERMINAL_STATE）
    with pytest.raises(TransitionError) as ei:
        svc.run_execution("exec-terminal", succeed=True, now=100.0)
    assert ei.value.reason is TransitionRejection.TERMINAL_STATE


# ---------- 重启恢复遇到不支持版本：明确终态 + 审计依据，不静默悬置 ----------

def test_recovery_unsupported_version_finalized_with_evidence(tmp_path):
    """唯一一条悬挂记录是高版本：恢复后落到明确终态、留下依据，
    attempt 不增加、不重复执行，且后续仍按"版本不支持"被明确拒绝。"""
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    insert_raw_execution(
        svc1._store, "exec-v3-running", "RUNNING", record_version=3, attempt=2
    )
    svc1._store.close()

    svc2 = make_service(store=Store(db))
    recovered = svc2.recover_interrupted(now=50.0)
    log.info("高版本恢复: recovered=%s", recovered)
    assert recovered == ["exec-v3-running"]

    # 落到明确合法终态，且不重复计数（attempt 保持 2）
    row = svc2._store._conn.execute(
        "SELECT status, attempt, detail FROM executions WHERE execution_id = ?",
        ("exec-v3-running",),
    ).fetchone()
    assert row["status"] == S.ABANDONED.value and row["attempt"] == 2
    assert "RECOVERY_UNSUPPORTED_VERSION" in row["detail"]

    audits = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    log.info("高版本恢复依据: %s", [a[3] for a in audits])
    assert len(audits) == 1
    assert "RECOVERY_UNSUPPORTED_VERSION" in audits[0][3]
    assert "v3" in audits[0][3] and "v2" in audits[0][3]  # 依据里写清版本边界

    # 恢复幂等：再来一次不重复收尾、不重复计数、不重复审计
    assert svc2.recover_interrupted(now=51.0) == []
    assert len([a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]) == 1

    # 终态 + 高版本：单条口径仍明确报"版本不支持"，与不存在/终态拒绝分得开
    with pytest.raises(RecordVersionError) as ei:
        svc2.get_execution("exec-v3-running")
    assert ei.value.record_version == 3
    with pytest.raises(RecordVersionError):
        svc2.run_execution("exec-v3-running", succeed=True, now=52.0)
    with pytest.raises(RecordVersionError):
        svc2.claim_due_retry("exec-v3-running", now=52.0)
    with pytest.raises(KeyError):
        svc2.run_execution("exec-missing", succeed=True, now=52.0)


def test_recovery_mixed_versions_each_explicit(tmp_path):
    """正常悬挂记录、高版本悬挂记录与等待重试记录混在一起：
    两类悬挂记录都被明确收尾且依据可区分，等待重试排期原样保留。"""
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    eid_running = ingest(svc1, "a1", "fp-1")
    svc1._store.compare_and_set_status(
        eid_running, (S.PENDING,), S.RUNNING, 1, "开始执行", 5.0
    )  # 正常悬挂（v2，attempt=1）
    insert_raw_execution(
        svc1._store, "exec-v3-running", "RUNNING", record_version=3, attempt=4
    )
    eid_waiting = ingest(svc1, "a2", "fp-2")
    svc1.run_execution(eid_waiting, succeed=False, now=6.0)  # WAITING_RETRY due 7.0
    before = svc1.get_execution(eid_waiting)
    svc1._store.close()

    svc2 = make_service(store=Store(db))
    recovered = svc2.recover_interrupted(now=50.0)
    log.info("混合恢复: recovered=%s", recovered)
    assert recovered == [eid_running, "exec-v3-running"]

    # 正常记录：常规恢复依据；高版本记录：版本不支持恢复依据
    audits = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    by_subject = {a[2]: a[3] for a in audits}
    assert "RECOVERY_INTERRUPTED" in by_subject[eid_running]
    assert "RECOVERY_UNSUPPORTED_VERSION" in by_subject["exec-v3-running"]

    # 两条都落到 ABANDONED，attempt 各自保持，不重复计数、不重复执行
    assert svc2._store._conn.execute(
        "SELECT status FROM executions WHERE execution_id = ?", (eid_running,)
    ).fetchone()["status"] == S.ABANDONED.value
    row = svc2._store._conn.execute(
        "SELECT status, attempt FROM executions WHERE execution_id = ?",
        ("exec-v3-running",),
    ).fetchone()
    assert row["status"] == S.ABANDONED.value and row["attempt"] == 4

    # 幂等：第二次恢复没有任何新收尾
    assert svc2.recover_interrupted(now=51.0) == []
    assert len([a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]) == 2

    # 等待重试记录不受恢复影响，到期后仍按当前版本语义正常消费
    after = svc2.get_execution(eid_waiting)
    assert after.status is S.WAITING_RETRY
    assert after.next_attempt_at == before.next_attempt_at == 7.0
    assert after.remaining_attempts == before.remaining_attempts == 2
    rec = svc2.run_execution(eid_waiting, succeed=True, now=7.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 2
