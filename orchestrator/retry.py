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


@dataclass(frozen=True)
class RetrySchedule:
    """v2 定时排期判定结论（纯值，无副作用）。

    should_retry      是否还应继续尝试；
    remaining_attempts 本次失败之后剩余的尝试次数（不含本次）；
    backoff_seconds   下一次尝试前需等待的秒数；
    next_run_at       最早可执行时刻（epoch 秒）；
    reason            可审计依据（RETRY_SCHEDULED / RETRY_EXHAUSTED）。
    """

    should_retry: bool
    remaining_attempts: int
    backoff_seconds: float
    next_run_at: float
    reason: str


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


def schedule_next_attempt(
    attempt: int, max_retries: int, base_backoff: float, now: float
) -> RetrySchedule:
    """v2 排期：给定本次失败后的已尝试次数，计算剩余次数与最早可执行时刻。

    总尝试预算为 max_retries + 1 次（首次执行 + max_retries 次重试）。
    判定为纯函数，不产生副作用，也不改变任何计数。
    """
    total_budget = max_retries + 1
    remaining = total_budget - attempt
    if remaining > 0:
        backoff = base_backoff * (2 ** (attempt - 1))
        next_run_at = now + backoff
        return RetrySchedule(
            should_retry=True,
            remaining_attempts=remaining,
            backoff_seconds=backoff,
            next_run_at=next_run_at,
            reason=(
                f"RETRY_SCHEDULED: 第 {attempt} 次失败，剩余 {remaining} 次尝试，"
                f"等待 {backoff}s，最早可执行时刻 {next_run_at}"
            ),
        )
    return RetrySchedule(
        should_retry=False,
        remaining_attempts=0,
        backoff_seconds=0.0,
        next_run_at=0.0,
        reason=f"RETRY_EXHAUSTED: 已用完 {total_budget} 次尝试预算，进入终态 ABANDONED",
    )
