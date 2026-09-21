"""重试与退避策略（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    next_attempt: int
    backoff_seconds: float
    reason: str  # RETRY_SCHEDULED / RETRY_EXHAUSTED


def decide_retry(attempt: int, max_retries: int, base_backoff: float = 1.0) -> RetryDecision:
    """根据当前尝试次数决定是否重试及退避时长。"""
    raise NotImplementedError
