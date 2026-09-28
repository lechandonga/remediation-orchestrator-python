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

    # 不受支持版本不会混入到期列表被批量消费：批量口径与单条口径一致，
    # 明确拒绝（RecordVersionError）而不是静默跳过、返回残缺列表
    with pytest.raises(RecordVersionError):
        svc.list_due_retries(now=100.0)


# ---------- 版本兼容：批量列出到期记录的口径 ----------

def read_raw_status(store: Store, execution_id: str) -> tuple[str, int]:
    """绕过版本校验直接读库（高版本记录无法经 get_execution 读取）。"""
    with store._lock:
        row = store._conn.execute(
            "SELECT status, attempt FROM executions WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
    return row["status"], row["attempt"]


def test_list_due_retries_rejects_single_unsupported_record():
    svc = make_service()
    insert_raw_execution(svc._store, "exec-v3", "WAITING_RETRY",
                         record_version=3, next_attempt_at=5.0, remaining_attempts=1)

    # 未到期时不影响列表（本就不该出现在"已到期"口径里）
    assert svc.list_due_retries(now=4.0) == []

    # 到期后明确拒绝，且与"记录不存在""状态不允许推进"分得开
    with pytest.raises(RecordVersionError) as ei:
        svc.list_due_retries(now=5.0)
    log.info("批量拒绝(单条高版本): %s", ei.value)
    assert ei.value.execution_id == "exec-v3" and ei.value.record_version == 3
    assert not isinstance(ei.value, (KeyError, TransitionError))


def test_list_due_retries_rejects_mixed_with_normal_records():
    svc = make_service()
    eid_ok = ingest(svc)
    svc.run_execution(eid_ok, succeed=False, now=10.0)  # 正常记录，due at 11.0
    insert_raw_execution(svc._store, "exec-v9", "WAITING_RETRY",
                         record_version=9, next_attempt_at=20.0, remaining_attempts=2)

    # 高版本记录未到期时，正常记录的列出不受影响
    due = svc.list_due_retries(now=11.0)
    log.info("到期记录(高版本未到期): %s", [r.execution_id for r in due])
    assert [r.execution_id for r in due] == [eid_ok]

    # 高版本记录也到期后：明确拒绝，而不是返回"看起来正常"的残缺列表
    with pytest.raises(RecordVersionError) as ei:
        svc.list_due_retries(now=20.0)
    log.info("批量拒绝(混合): %s", ei.value)
    assert ei.value.execution_id == "exec-v9" and ei.value.record_version == 9

    # 正常记录本身并未被消费或改动，拒绝无副作用
    rec = svc.get_execution(eid_ok)
    assert rec.status is S.WAITING_RETRY and rec.attempt == 1


# ---------- 版本兼容：重启恢复的口径 ----------

def test_recovery_finalizes_unsupported_running_record(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    # 重启前已开始、未出结果、版本高于当前支持的悬挂记录
    insert_raw_execution(svc1._store, "exec-v3", "RUNNING",
                         record_version=3, attempt=2)
    svc1._store.close()

    svc2 = make_service(store=Store(db))
    recovered = svc2.recover_interrupted(now=50.0)
    log.info("恢复(高版本悬挂): recovered=%s", recovered)
    assert recovered == ["exec-v3"]

    # 落到明确的合法终态，且已计数的尝试不重复计数
    status, attempt = read_raw_status(svc2._store, "exec-v3")
    assert status == "ABANDONED" and attempt == 2

    # 留下可区分的审计依据
    audits = [a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    log.info("恢复依据: %s", [a[3] for a in audits])
    assert len(audits) == 1
    assert "RECOVERY_UNSUPPORTED_VERSION" in audits[0][3] and "v3" in audits[0][3]

    # 恢复幂等：再次执行不产生新的收尾或审计
    assert svc2.recover_interrupted(now=51.0) == []
    assert len([a for a in svc2._store.audit_log() if a[1] == "RECOVERY"]) == 1

    # 不会被重复执行：单条口径仍明确拒绝（版本不受支持，而非不存在）
    with pytest.raises(RecordVersionError):
        svc2.run_execution("exec-v3", succeed=True, now=52.0)
    with pytest.raises(RecordVersionError):
        svc2.claim_due_retry("exec-v3", now=52.0)


def test_recovery_mixed_supported_and_unsupported(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    eid_ok = ingest(svc1, "a1", "fp-1")
    svc1._store.compare_and_set_status(
        eid_ok, (S.PENDING,), S.RUNNING, 1, "开始执行", 5.0
    )
    insert_raw_execution(svc1._store, "exec-v3", "RUNNING",
                         record_version=3, attempt=4)
    svc1._store.close()

    svc2 = make_service(store=Store(db))
    recovered = svc2.recover_interrupted(now=50.0)
    log.info("恢复(混合): recovered=%s", recovered)
    assert sorted(recovered) == sorted([eid_ok, "exec-v3"])

    # 两条都落到终态，计数均未漂移
    rec_ok = svc2.get_execution(eid_ok)
    assert rec_ok.status is S.ABANDONED and rec_ok.attempt == 1
    status, attempt = read_raw_status(svc2._store, "exec-v3")
    assert status == "ABANDONED" and attempt == 4

    # 两类依据可区分：正常悬挂 vs 版本不受支持
    audits = [a[3] for a in svc2._store.audit_log() if a[1] == "RECOVERY"]
    log.info("恢复依据: %s", audits)
    assert len(audits) == 2
    assert sum("RECOVERY_INTERRUPTED" in d for d in audits) == 1
    assert sum("RECOVERY_UNSUPPORTED_VERSION" in d for d in audits) == 1

    # 幂等
    assert svc2.recover_interrupted(now=51.0) == []


def test_recovery_preserves_unsupported_waiting_record_for_due_list(tmp_path):
    db = str(tmp_path / "state.db")
    svc1 = make_service(store=Store(db))
    # 高版本但处于等待重试（非 RUNNING 悬挂）：恢复不触碰，排期字段原样保留
    insert_raw_execution(svc1._store, "exec-v3-wait", "WAITING_RETRY",
                         record_version=3, next_attempt_at=7.0, remaining_attempts=1)
    svc1._store.close()

    svc2 = make_service(store=Store(db))
    assert svc2.recover_interrupted(now=50.0) == []
    assert read_raw_status(svc2._store, "exec-v3-wait") == ("WAITING_RETRY", 0)
    assert svc2._store.audit_log() == []  # 无任何静默处置

    # 到期后由批量列出明确拒绝，而不是被静默跳过
    with pytest.raises(RecordVersionError):
        svc2.list_due_retries(now=7.0)
