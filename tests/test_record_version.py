"""记录级版本兼容：v1 历史记录按原语义执行；不支持的新版本明确拒绝并与其它错误区分。"""
from __future__ import annotations

import logging

import pytest

from orchestrator.models import (
    AlertEvent,
    ExecutionStatus as S,
    RemediationPolicy,
    TransitionRejection,
)
from orchestrator.record_version import UnsupportedRecordVersionError
from orchestrator.service import OrchestratorService
from orchestrator.statemachine import TransitionError
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

log = logging.getLogger("test.record-version")

TOKEN = "alice:operator"


def make_policy() -> RemediationPolicy:
    return RemediationPolicy(
        policy_id="p1", version=1, priority=5,
        match_labels={"app": "web"}, action="restart",
        max_retries=2, cooldown_seconds=60.0,
    )


def make_service(record_version=2, store=None) -> OrchestratorService:
    return OrchestratorService(
        store or Store(), [make_policy()], SuppressionRegistry(),
        base_backoff=10.0, record_version=record_version,
    )


def alert(alert_id="a1", fingerprint="fp-1") -> AlertEvent:
    return AlertEvent(alert_id, fingerprint, "res1", "high", {"app": "web"})


def test_legacy_v1_record_read_and_runs_with_immediate_retry():
    """升级前的 v1 记录：无排期字段，失败立即回到 PENDING，按原语义继续执行。"""
    svc = make_service(record_version=1)
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    rec = svc.run_execution(eid, succeed=False, now=1.0)
    assert rec.record_version == 1
    assert rec.status is S.PENDING            # v1 立即重新排队，而非 WAITING_RETRY
    assert rec.next_run_at == 0.0 and rec.remaining_attempts == 0
    rec = svc.run_execution(eid, succeed=False, now=2.0)
    assert rec.status is S.PENDING and rec.attempt == 2
    rec = svc.run_execution(eid, succeed=False, now=3.0)
    assert rec.status is S.ABANDONED and rec.attempt == 3
    log.info("v1 历史记录按原语义完成立即重试并耗尽")


def test_legacy_v1_row_from_old_db_defaults_on_migration(tmp_path):
    """模拟旧库：executions 缺新列。迁移后历史行默认 record_version=1 且可读懂。"""
    import sqlite3

    db = str(tmp_path / "old.db")
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE fingerprints (
            fingerprint TEXT PRIMARY KEY, execution_id TEXT NOT NULL,
            state TEXT NOT NULL, claimed_at REAL NOT NULL, cooldown_until REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE executions (
            execution_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, policy_id TEXT NOT NULL,
            policy_version INTEGER NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
            detail TEXT NOT NULL DEFAULT '', updated_at REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
            event_type TEXT NOT NULL, subject TEXT NOT NULL, detail TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO executions VALUES ('exec-old','fp-old','p1',1,'PENDING',0,'',0.0)"
    )
    conn.commit()
    conn.close()

    store = Store(db)  # 触发迁移
    rec = store.get_execution("exec-old")
    assert rec is not None
    assert rec.record_version == 1                       # 历史行默认 v1
    assert rec.status is S.PENDING and rec.attempt == 0  # 不被当作数据缺失

    svc = make_service(record_version=2, store=store)    # 新代码读旧记录
    rec = svc.run_execution("exec-old", succeed=True, now=1.0)
    assert rec.status is S.SUCCEEDED and rec.attempt == 1
    log.info("旧库历史记录迁移后可正常执行")


def test_unsupported_newer_version_rejected_not_silently_interpreted():
    svc = make_service(record_version=2)
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    eid = r.execution_id
    # 在存储层写入一个当前不支持的更高版本（模拟未来版本记录），
    # 同时置为 WAITING_RETRY，使其进入到期列表的扫描/解码路径
    with svc._store._conn:  # noqa: SLF001
        svc._store._conn.execute(
            "UPDATE executions SET record_version = 99, status = 'WAITING_RETRY',"
            " next_run_at = 0, remaining_attempts = 1 WHERE execution_id = ?",
            (eid,),
        )

    # 与"不存在"(None)、"状态不允许"(TransitionError) 分得开：专属异常类型
    assert svc.get_execution("exec-missing") is None
    with pytest.raises(UnsupportedRecordVersionError) as ei:
        svc.get_execution(eid)
    assert ei.value.version == 99 and ei.value.execution_id == eid
    log.info("不支持版本拒绝: %s", ei.value)

    with pytest.raises(UnsupportedRecordVersionError):
        svc.run_execution(eid, succeed=True, now=1.0)
    with pytest.raises(UnsupportedRecordVersionError):
        svc.list_due_retries(1.0)
    with pytest.raises(UnsupportedRecordVersionError):
        svc.claim_due_retries(1.0)


def test_unsupported_version_distinct_from_state_rejection():
    svc = make_service(record_version=2)
    r = svc.ingest_alert(alert(), now=0.0, token=TOKEN)
    with svc._store._conn:  # noqa: SLF001
        svc._store._conn.execute(
            "UPDATE executions SET record_version = 99 WHERE execution_id = ?",
            (r.execution_id,),
        )
    # 不是 TransitionError（状态问题），也不是 KeyError（不存在）
    with pytest.raises(UnsupportedRecordVersionError):
        svc.run_execution(r.execution_id, succeed=True, now=1.0)

    # 对照：一条正常 v2 等待记录未到期 -> TransitionError/RETRY_NOT_DUE，类型不同
    r2 = svc.ingest_alert(alert("a2", "fp-2"), now=0.0, token=TOKEN)
    svc.run_execution(r2.execution_id, succeed=False, now=0.0)
    with pytest.raises(TransitionError) as ei:
        svc.run_execution(r2.execution_id, succeed=True, now=1.0)
    assert ei.value.reason is TransitionRejection.RETRY_NOT_DUE
