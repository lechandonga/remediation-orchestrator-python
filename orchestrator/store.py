"""基于 SQLite 的本地持久化存储（骨架）。

负责：去重指纹、执行记录、审计日志的持久化，保证服务重启后状态不丢失。
"""
from __future__ import annotations

import sqlite3

from .models import ExecutionRecord, ExecutionStatus


class Store:
    """本地持久化存储，所有写操作均在事务内完成。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    # ---- 去重 ----
    def claim_fingerprint(self, fingerprint: str, execution_id: str, now: float, cooldown_seconds: float) -> str:
        """尝试占用指纹；返回 'claimed' / 'active' / 'cooldown'。"""
        raise NotImplementedError

    def release_fingerprint(self, fingerprint: str, now: float) -> None:
        """执行终态后释放占用并记录冷却截止时间。"""
        raise NotImplementedError

    # ---- 执行记录 ----
    def create_execution(self, record: ExecutionRecord) -> None:
        raise NotImplementedError

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        raise NotImplementedError

    def compare_and_set_status(
        self, execution_id: str, expected: tuple[ExecutionStatus, ...], new_status: ExecutionStatus,
        attempt: int, detail: str,
    ) -> bool:
        """CAS 更新状态，保证并发下不产生半更新状态。"""
        raise NotImplementedError

    # ---- 审计 ----
    def audit(self, event_type: str, subject: str, detail: str, now: float) -> None:
        raise NotImplementedError

    def audit_log(self) -> list[tuple]:
        raise NotImplementedError
