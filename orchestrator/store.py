"""基于 SQLite 的本地持久化存储。

负责：去重指纹、执行记录、审计日志的持久化，保证服务重启后状态不丢失。
所有写操作均在事务内完成，指纹占用与状态更新使用原子操作以支持并发。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import ExecutionRecord, ExecutionStatus
from .record_version import ensure_supported_version

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

# v2 记录形态新增列；老库通过 ALTER TABLE 补齐，历史行默认 record_version=1，
# 从而按 v1 原语义继续参与执行（向前兼容历史记录）。
_EXTRA_COLUMNS = (
    ("record_version", "INTEGER NOT NULL DEFAULT 1"),
    ("next_run_at", "REAL NOT NULL DEFAULT 0"),
    ("remaining_attempts", "INTEGER NOT NULL DEFAULT 0"),
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
        """补齐 v2 新增列；历史行默认落为 v1 记录。"""
        existing = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(executions)")
        }
        for name, decl in _EXTRA_COLUMNS:
            if name not in existing:
                self._conn.execute(
                    f"ALTER TABLE executions ADD COLUMN {name} {decl}"
                )

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _to_record(row: sqlite3.Row) -> ExecutionRecord:
        ensure_supported_version(row["execution_id"], row["record_version"])
        return ExecutionRecord(
            execution_id=row["execution_id"],
            fingerprint=row["fingerprint"],
            policy_id=row["policy_id"],
            policy_version=row["policy_version"],
            status=ExecutionStatus(row["status"]),
            attempt=row["attempt"],
            detail=row["detail"],
            record_version=row["record_version"],
            next_run_at=row["next_run_at"],
            remaining_attempts=row["remaining_attempts"],
        )

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
                " (execution_id, fingerprint, policy_id, policy_version, status, attempt,"
                "  detail, record_version, next_run_at, remaining_attempts)"
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
                    record.next_run_at,
                    record.remaining_attempts,
                ),
            )

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?", (execution_id,)
            )
            row = cur.fetchone()
        if row is None:
            return None
        return self._to_record(row)

    def list_by_status(self, statuses: tuple[ExecutionStatus, ...]) -> list[ExecutionRecord]:
        """按状态扫描记录（供重启恢复）；只读，不改变任何计数。"""
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with self._lock:
            cur = self._conn.execute(
                f"SELECT * FROM executions WHERE status IN ({placeholders})",
                tuple(s.value for s in statuses),
            )
            rows = cur.fetchall()
        return [self._to_record(r) for r in rows]

    def list_due_waiting(self, now: float) -> list[ExecutionRecord]:
        """只读列出已到期（next_run_at <= now）的等待重试记录。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM executions WHERE status = ? AND next_run_at <= ?"
                " ORDER BY next_run_at, execution_id",
                (ExecutionStatus.WAITING_RETRY.value, now),
            )
            rows = cur.fetchall()
        return [self._to_record(r) for r in rows]

    def claim_due_waiting(
        self, now: float, detail: str, limit: int | None = None
    ) -> list[ExecutionRecord]:
        """原子地批量接管到期的等待重试记录：WAITING_RETRY -> RUNNING。

        先查到期记录，再逐条以"状态仍为 WAITING_RETRY"为条件做 CAS UPDATE，
        接管时尝试次数按各记录自身 attempt+1 递增。
        条件 UPDATE 保证并发/多进程下同一条记录只有一个消费方接管成功，
        重复消费不会取到同一条，也不会让尝试次数发生漂移（attempt 仅在
        成功接管时写入一次）。
        """
        claimed: list[ExecutionRecord] = []
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT * FROM executions WHERE status = ? AND next_run_at <= ?"
                " ORDER BY next_run_at, execution_id",
                (ExecutionStatus.WAITING_RETRY.value, now),
            ).fetchall()
            for row in rows:
                if limit is not None and len(claimed) >= limit:
                    break
                cur = self._conn.execute(
                    "UPDATE executions SET status = ?, attempt = ?, detail = ?,"
                    " remaining_attempts = ?, updated_at = ?"
                    " WHERE execution_id = ? AND status = ?",
                    (
                        ExecutionStatus.RUNNING.value,
                        row["attempt"] + 1,
                        detail,
                        row["remaining_attempts"] - 1,
                        now,
                        row["execution_id"],
                        ExecutionStatus.WAITING_RETRY.value,
                    ),
                )
                if cur.rowcount == 1:
                    updated = self._conn.execute(
                        "SELECT * FROM executions WHERE execution_id = ?",
                        (row["execution_id"],),
                    ).fetchone()
                    claimed.append(self._to_record(updated))
        return claimed

    def compare_and_set_status(
        self,
        execution_id: str,
        expected: tuple[ExecutionStatus, ...],
        new_status: ExecutionStatus,
        attempt: int,
        detail: str,
        now: float,
        next_run_at: float | None = None,
        remaining_attempts: int | None = None,
        require_due: bool = False,
    ) -> bool:
        """CAS 更新状态：仅当当前状态属于 expected 集合时更新成功。

        并发下只有一个调用方能成功，避免半更新状态与计数漂移。
        - next_run_at / remaining_attempts 非 None 时一并写入（用于进入等待排期）；
        - require_due=True 时额外要求 next_run_at <= now（用于单条接管到期记录）。
        """
        placeholders = ",".join("?" for _ in expected)
        sets = ["status = ?", "attempt = ?", "detail = ?", "updated_at = ?"]
        params: list = [new_status.value, attempt, detail, now]
        if next_run_at is not None:
            sets.append("next_run_at = ?")
            params.append(next_run_at)
        if remaining_attempts is not None:
            sets.append("remaining_attempts = ?")
            params.append(remaining_attempts)
        extra = " AND next_run_at <= ?" if require_due else ""
        sql = (
            f"UPDATE executions SET {', '.join(sets)}"
            f" WHERE execution_id = ? AND status IN ({placeholders}){extra}"
        )
        params.extend([execution_id, *(s.value for s in expected)])
        if require_due:
            params.append(now)
        with self._lock, self._conn:
            cur = self._conn.execute(sql, tuple(params))
            return cur.rowcount == 1

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
