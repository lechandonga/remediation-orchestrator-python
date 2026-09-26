"""执行状态机：定义合法流转与非法流转的可区分拒绝原因。"""
from __future__ import annotations

from .models import ExecutionStatus, TransitionRejection

TERMINAL_STATES: frozenset[ExecutionStatus] = frozenset(
    {ExecutionStatus.SUCCEEDED, ExecutionStatus.ABANDONED, ExecutionStatus.SUPPRESSED}
)

# 合法流转表：current -> 允许的 target 集合
ALLOWED: dict[ExecutionStatus, frozenset[ExecutionStatus]] = {
    ExecutionStatus.PENDING: frozenset(
        {ExecutionStatus.RUNNING, ExecutionStatus.ABANDONED, ExecutionStatus.SUPPRESSED}
    ),
    ExecutionStatus.RUNNING: frozenset(
        {ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED, ExecutionStatus.ABANDONED}
    ),
    # FAILED -> WAITING_RETRY 表示按时间排期等待重试（v2 记录）；
    # FAILED -> PENDING 保留给 v1 历史记录的"立即重排"旧语义
    ExecutionStatus.FAILED: frozenset(
        {
            ExecutionStatus.PENDING,
            ExecutionStatus.WAITING_RETRY,
            ExecutionStatus.ABANDONED,
        }
    ),
    # 等待重试到期后可进入 RUNNING；也可能被恢复/人工置为 ABANDONED
    ExecutionStatus.WAITING_RETRY: frozenset(
        {ExecutionStatus.RUNNING, ExecutionStatus.ABANDONED}
    ),
    ExecutionStatus.SUCCEEDED: frozenset(),
    ExecutionStatus.ABANDONED: frozenset(),
    ExecutionStatus.SUPPRESSED: frozenset(),
}


class TransitionError(Exception):
    """非法状态流转异常，携带可区分原因。"""

    def __init__(self, reason: TransitionRejection, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason


def validate_transition(current: ExecutionStatus, target: ExecutionStatus) -> None:
    """校验状态流转是否合法，非法时抛出 TransitionError。

    拒绝原因可区分：
    - TERMINAL_STATE：当前状态已是终态，任何流转均被拒绝；
    - ILLEGAL_TRANSITION：当前状态非终态，但目标状态不在允许集合内。
    """
    if current in TERMINAL_STATES:
        raise TransitionError(
            TransitionRejection.TERMINAL_STATE,
            f"状态 {current.value} 为终态，不允许流转到 {target.value}",
        )
    if target not in ALLOWED[current]:
        raise TransitionError(
            TransitionRejection.ILLEGAL_TRANSITION,
            f"不允许从 {current.value} 流转到 {target.value}",
        )
