"""基于 SQLite 的本地持久化存储。

负责：去重指纹、执行记录、审计日志的持久化，保证服务重启后状态不丢失。
所有写操作均在事务内完成，指纹占用与状态更新使用原子操作以支持并发。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import ExecutionRecord, ExecutionStatus

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
    updated_at     REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    event_type TEXT NOT NULL,
    subject    TEXT NOT NULL,
    detail     TEXT NOT NULL
);
"""


class Store:
    """本地持久化存储，所有写操作均在事务内完成。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._conn:
            self._conn.executescript(_SCHEMA)

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
    def create_execution(self, record: ExecutionRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO executions"
                " (execution_id, fingerprint, policy_id, policy_version, status, attempt, detail)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    record.execution_id,
                    record.fingerprint,
                    record.policy_id,
                    record.policy_version,
                    record.status.value,
                    record.attempt,
                    record.detail,
                ),
            )

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        cur = self._conn.execute(
            "SELECT * FROM executions WHERE execution_id = ?", (execution_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return ExecutionRecord(
            execution_id=row["execution_id"],
            fingerprint=row["fingerprint"],
            policy_id=row["policy_id"],
            policy_version=row["policy_version"],
            status=ExecutionStatus(row["status"]),
            attempt=row["attempt"],
            detail=row["detail"],
        )

    def compare_and_set_status(
        self,
        execution_id: str,
        expected: tuple[ExecutionStatus, ...],
        new_status: ExecutionStatus,
        attempt: int,
        detail: str,
        now: float,
    ) -> bool:
        """CAS 更新状态：仅当当前状态属于 expected 集合时更新成功。

        并发下只有一个调用方能成功，避免半更新状态与计数漂移。
        """
        placeholders = ",".join("?" for _ in expected)
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE executions SET status = ?, attempt = ?, detail = ?, updated_at = ?"
                f" WHERE execution_id = ? AND status IN ({placeholders})",
                (
                    new_status.value,
                    attempt,
                    detail,
                    now,
                    execution_id,
                    *(s.value for s in expected),
                ),
            )
            return cur.rowcount == 1

    # ---- 审计 ----
    def audit(self, event_type: str, subject: str, detail: str, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO audit_log (ts, event_type, subject, detail) VALUES (?, ?, ?, ?)",
                (now, event_type, subject, detail),
            )

    def audit_log(self) -> list[tuple]:
        cur = self._conn.execute(
            "SELECT ts, event_type, subject, detail FROM audit_log ORDER BY id"
        )
        return [tuple(r) for r in cur.fetchall()]
