"""重试与退避策略。

重试上限由策略的 max_retries 配置；达到上限后由上层将执行置为
ABANDONED 终态。退避时长按指数增长：base * 2^(attempt-1)，
判定为纯函数，不产生副作用，保证重试期间不重复计数。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    next_attempt: int
    backoff_seconds: float
    reason: str  # RETRY_SCHEDULED / RETRY_EXHAUSTED


def decide_retry(attempt: int, max_retries: int, base_backoff: float = 1.0) -> RetryDecision:
    """根据当前已失败次数（attempt，从 1 计）决定是否重试及退避时长。"""
    if attempt <= max_retries:
        backoff = base_backoff * (2 ** (attempt - 1))
        return RetryDecision(
            True,
            attempt + 1,
            backoff,
            f"RETRY_SCHEDULED: 第 {attempt} 次失败，{backoff}s 后进行第 {attempt + 1} 次尝试",
        )
    return RetryDecision(
        False,
        attempt,
        0.0,
        f"RETRY_EXHAUSTED: 已达重试上限 {max_retries} 次，进入终态 ABANDONED",
    )
