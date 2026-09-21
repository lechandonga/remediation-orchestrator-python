"""失败重试与指数退避。

计数约定（防止重复计数 / 漂移）：
    attempts      = 已经发生的副作用执行次数（首次执行为 1）
    max_retries   = 首次之外允许的追加执行次数
    总执行上限    = 1 + max_retries；attempts 达到上限后失败即耗尽。
每次"真正执行副作用"时 attempts 才 +1；被退避/上限拒绝的请求不计数。
"""

from __future__ import annotations

from .errors import RetryError
from .models import ExecutionRecord, RejectReason, TaskState


def backoff_delay(record: ExecutionRecord, attempt: int) -> float:
    """指数退避：base * 2**(attempt-1)。attempt 从 1 起，结果确定。"""
    if attempt < 1:
        raise RetryError(RejectReason.RETRY_NOT_ALLOWED, f"invalid attempt {attempt}")
    return record.backoff_base_seconds * (2 ** (attempt - 1))


def can_retry(record: ExecutionRecord, now_ts: float) -> tuple[bool, RejectReason | None]:
    """返回 (是否允许立即重试, 拒绝原因码)。判定顺序固定，结论可解释。"""
    exhausted = record.attempts >= record.max_retries + 1
    if record.state is TaskState.FAILED:
        if exhausted:
            return False, RejectReason.RETRY_EXHAUSTED
        if now_ts < record.not_before_ts:
            return False, RejectReason.RETRY_BACKOFF_NOT_ELAPSED
        return True, None
    if record.state is TaskState.ABANDONED:
        # 因耗尽而放弃 -> RETRY_EXHAUSTED；被人工放弃 -> 终态拒绝。
        if exhausted:
            return False, RejectReason.RETRY_EXHAUSTED
        return False, RejectReason.TASK_ALREADY_TERMINAL
    # SUCCEEDED / PENDING / RUNNING 均不允许重试。
    return False, RejectReason.RETRY_NOT_ALLOWED


def schedule_backoff(record: ExecutionRecord, failed_at: float) -> float:
    """失败后登记下一次最早可执行时间（不改变状态、不计数）。"""
    delay = backoff_delay(record, record.attempts)
    record.not_before_ts = failed_at + delay
    return record.not_before_ts
