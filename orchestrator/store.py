"""基于 SQLite 的本地持久化存储。

负责：去重指纹、执行记录、审计日志的持久化，保证服务重启后状态不丢失。
所有写操作均在事务内完成，指纹占用与状态更新使用原子操作以支持并发。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import (
    RECORD_SCHEMA_VERSION,
    ExecutionRecord,
    ExecutionStatus,
    InterruptedRecord,
    RecordVersionError,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fingerprints (
    fingerprint    TEXT PRIMARY KEY,
    execution_id   TEXT NOT NULL,
    state          TEXT NOT NULL,           -- 'active' | 'cooldown'
    claimed_at     REAL NOT NULL,
    cooldown_until REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS executions (
    execution_id   TEXT PRIMARY KEY,
    fingerprint    TEXT NOT NULL,
    policy_id      TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    status         TEXT NOT NULL,
    attempt        INTEGER NOT NULL DEFAULT 0,
    detail         TEXT NOT NULL DEFAULT '',
    updated_at     REAL NOT NULL DEFAULT 0,
    record_version INTEGER NOT NULL DEFAULT 1,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    remaining_attempts INTEGER NOT NULL DEFAULT -1
);
CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    event_type TEXT NOT NULL,
    subject    TEXT NOT NULL,
    detail     TEXT NOT NULL
);
"""

# 历史库（v1 结构）缺少的列，启动时幂等补齐；已存在行保持默认值，
# 即 record_version=1、next_attempt_at=0、remaining_attempts=-1（旧语义）
_MIGRATIONS = (
    "ALTER TABLE executions ADD COLUMN record_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE executions ADD COLUMN next_attempt_at REAL NOT NULL DEFAULT 0",
    "ALTER TABLE executions ADD COLUMN remaining_attempts INTEGER NOT NULL DEFAULT -1",
)


class Store:
    """本地持久化存储，所有写操作均在事务内完成。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._conn:
            self._conn.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """幂等补齐历史库缺失的列（记录级版本兼容，不改写已有行）。"""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(executions)")}
        for stmt in _MIGRATIONS:
            col = stmt.split("ADD COLUMN ")[1].split(" ")[0]
            if col not in cols:
                self._conn.execute(stmt)

    def close(self) -> None:
        self._conn.close()

    # ---- 去重 ----
    def claim_fingerprint(
        self, fingerprint: str, execution_id: str, now: float, cooldown_seconds: float
    ) -> str:
        """尝试占用指纹；返回 'claimed' / 'active' / 'cooldown'。

        判定规则（稳定且可解释）：
        - 无记录或冷却已过期 -> 占用成功（claimed）；
        - 已有活跃执行占用 -> active（重复告警，不得再次执行）；
        - 冷却窗口未过期 -> cooldown（重复告警，不得再次执行）。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "SELECT state, cooldown_until FROM fingerprints WHERE fingerprint = ?",
                (fingerprint,),
            )
            row = cur.fetchone()
            if row is None or (row["state"] == "cooldown" and row["cooldown_until"] <= now):
                self._conn.execute(
                    "INSERT OR REPLACE INTO fingerprints"
                    " (fingerprint, execution_id, state, claimed_at, cooldown_until)"
                    " VALUES (?, ?, 'active', ?, 0)",
                    (fingerprint, execution_id, now),
                )
                return "claimed"
            return row["state"]  # 'active' 或 'cooldown'

    def release_fingerprint(self, fingerprint: str, now: float, cooldown_seconds: float) -> None:
        """执行进入终态后释放占用，并记录冷却截止时间。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE fingerprints SET state = 'cooldown', cooldown_until = ?"
                " WHERE fingerprint = ?",
                (now + cooldown_seconds, fingerprint),
            )

    # ---- 执行记录 ----
    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> ExecutionRecord:
        record_version = row["record_version"]
        if record_version > RECORD_SCHEMA_VERSION:
            raise RecordVersionError(
                ((row["execution_id"], record_version),),
                context="读取执行记录",
            )
        return ExecutionRecord(
            execution_id=row["execution_id"],
            fingerprint=row["fingerprint"],
            policy_id=row["policy_id"],
            policy_version=row["policy_version"],
            status=ExecutionStatus(row["status"]),
            attempt=row["attempt"],
            detail=row["detail"],
            record_version=record_version,
            next_attempt_at=row["next_attempt_at"],
            remaining_attempts=row["remaining_attempts"],
        )

    def create_execution(self, record: ExecutionRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO executions"
                " (execution_id, fingerprint, policy_id, policy_version, status, attempt,"
                " detail, record_version, next_attempt_at, remaining_attempts)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.execution_id,
                    record.fingerprint,
                    record.policy_id,
                    record.policy_version,
                    record.status.value,
                    record.attempt,
                    record.detail,
                    record.record_version,
                    record.next_attempt_at,
                    record.remaining_attempts,
                ),
            )

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        """读取执行记录；版本不受支持时抛出 RecordVersionError（与不存在区分）。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?", (execution_id,)
            )
            row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def compare_and_set_status(
        self,
        execution_id: str,
        expected: tuple[ExecutionStatus, ...],
        new_status: ExecutionStatus,
        attempt: int,
        detail: str,
        now: float,
        next_attempt_at: float | None = None,
        remaining_attempts: int | None = None,
    ) -> bool:
        """CAS 更新状态：仅当当前状态属于 expected 集合时更新成功。

        并发下只有一个调用方能成功，避免半更新状态与计数漂移。
        next_attempt_at / remaining_attempts 为 None 时保持原值不变。
        """
        placeholders = ",".join("?" for _ in expected)
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE executions SET status = ?, attempt = ?, detail = ?, updated_at = ?,"
                f" next_attempt_at = COALESCE(?, next_attempt_at),"
                f" remaining_attempts = COALESCE(?, remaining_attempts)"
                f" WHERE execution_id = ? AND status IN ({placeholders})",
                (
                    new_status.value,
                    attempt,
                    detail,
                    now,
                    next_attempt_at,
                    remaining_attempts,
                    execution_id,
                    *(s.value for s in expected),
                ),
            )
            return cur.rowcount == 1

    # ---- 定时重试排期 ----
    def list_due_retries(self, now: float) -> list[ExecutionRecord]:
        """列出所有已到期的等待重试记录（按最早可执行时刻排序，供批量消费）。

        批量口径与单条读取一致：只要到期集合中存在版本高于当前支持的记录，
        就抛出 RecordVersionError（携带全部违规记录），绝不静默过滤成一个
        "看似正常"的列表；受支持的 v1/v2 到期记录照常返回。
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM executions WHERE status = ? AND next_attempt_at <= ?"
                " ORDER BY next_attempt_at, execution_id",
                (ExecutionStatus.WAITING_RETRY.value, now),
            )
            rows = cur.fetchall()
        unsupported = [
            (r["execution_id"], r["record_version"])
            for r in rows
            if r["record_version"] > RECORD_SCHEMA_VERSION
        ]
        if unsupported:
            raise RecordVersionError.for_batch(unsupported)
        return [self._row_to_record(r) for r in rows]

    def claim_due_retry(self, execution_id: str, now: float) -> bool:
        """原子接管一条已到期的等待重试记录：WAITING_RETRY -> RUNNING。

        单条 SQL 内完成状态、到期条件校验与尝试计数自增，
        多路并发消费时只有一个消费方能成功，且计数只增加一次。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE executions SET status = ?, attempt = attempt + 1,"
                " detail = '接管到期重试，开始执行', updated_at = ?"
                " WHERE execution_id = ? AND status = ? AND next_attempt_at <= ?",
                (
                    ExecutionStatus.RUNNING.value,
                    now,
                    execution_id,
                    ExecutionStatus.WAITING_RETRY.value,
                    now,
                ),
            )
            return cur.rowcount == 1

    # ---- 重启恢复 ----
    def list_interrupted(self) -> list[InterruptedRecord]:
        """列出重启前已开始但未给出结果的记录（RUNNING 悬挂），不做版本过滤。

        高版本行同样返回（原始视图），由恢复服务明确处置，
        既不静默跳过让它永远悬着，也不在此按旧语义物化成 ExecutionRecord。
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM executions WHERE status = ?"
                " ORDER BY execution_id",
                (ExecutionStatus.RUNNING.value,),
            )
            rows = cur.fetchall()
        return [
            InterruptedRecord(
                execution_id=r["execution_id"],
                fingerprint=r["fingerprint"],
                policy_id=r["policy_id"],
                attempt=r["attempt"],
                record_version=r["record_version"],
            )
            for r in rows
        ]

    # ---- 审计 ----
    def audit(self, event_type: str, subject: str, detail: str, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO audit_log (ts, event_type, subject, detail) VALUES (?, ?, ?, ?)",
                (now, event_type, subject, detail),
            )

    def audit_log(self) -> list[tuple]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, event_type, subject, detail FROM audit_log ORDER BY id"
            )
            return [tuple(r) for r in cur.fetchall()]
