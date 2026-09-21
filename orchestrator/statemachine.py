"""执行状态机：定义合法流转与非法流转的可区分拒绝原因（骨架）。"""
from __future__ import annotations

from .models import ExecutionStatus, TransitionRejection


class TransitionError(Exception):
    """非法状态流转异常，携带可区分原因。"""

    def __init__(self, reason: TransitionRejection, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason


def validate_transition(current: ExecutionStatus, target: ExecutionStatus) -> None:
    """校验状态流转是否合法，非法时抛出 TransitionError。"""
    raise NotImplementedError
