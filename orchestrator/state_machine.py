"""执行状态机：只允许合法流转，拒绝时给出可区分原因。

合法转换：
    PENDING   -> RUNNING | ABANDONED
    RUNNING   -> SUCCEEDED | FAILED
    FAILED    -> PENDING（重试，退避检查由 retry 模块负责） | ABANDONED（重试耗尽）
    SUCCEEDED / ABANDONED：终态，不接受任何转换

任何非法转换抛 StateTransitionError：
- 当前态为终态        -> TASK_ALREADY_TERMINAL
- 其它非法转换        -> ILLEGAL_TRANSITION
"""

from __future__ import annotations

from .errors import StateTransitionError
from .models import ExecutionRecord, RejectReason, TaskState

TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.RUNNING, TaskState.ABANDONED}),
    TaskState.RUNNING: frozenset({TaskState.SUCCEEDED, TaskState.FAILED}),
    TaskState.FAILED: frozenset({TaskState.PENDING, TaskState.ABANDONED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.ABANDONED: frozenset(),
}


def assert_can_transition(record: ExecutionRecord, target: TaskState) -> None:
    current = record.state
    if current in (TaskState.SUCCEEDED, TaskState.ABANDONED):
        raise StateTransitionError(
            RejectReason.TASK_ALREADY_TERMINAL,
            f"task={record.task_id} is in terminal state {current.value}; "
            f"transition to {target.value} refused",
        )
    allowed = TRANSITIONS[current]
    if target not in allowed:
        raise StateTransitionError(
            RejectReason.ILLEGAL_TRANSITION,
            f"task={record.task_id} transition {current.value}->{target.value} "
            f"is not allowed; allowed={sorted(s.value for s in allowed)}",
        )


def apply_transition(
    record: ExecutionRecord, target: TaskState, ts: float, error: str = ""
) -> ExecutionRecord:
    """校验并就地应用转换。仅维护状态字段；revision 由持久层 CAS 递增，
    避免内存对象与持久化计数出现漂移。"""
    assert_can_transition(record, target)
    record.state = target
    record.updated_at = ts
    if error:
        record.last_error = error
    elif target == TaskState.SUCCEEDED:
        record.last_error = ""
    return record
