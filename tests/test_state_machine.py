"""状态流转合法性：合法流转通过，非法流转拒绝且原因可区分。"""

from __future__ import annotations

import pytest

from orchestrator.errors import StateTransitionError
from orchestrator.models import ExecutionRecord, RejectReason, TaskState
from orchestrator.state_machine import TRANSITIONS, apply_transition, assert_can_transition


def _rec(state: TaskState) -> ExecutionRecord:
    return ExecutionRecord(
        task_id="t1", alert_id="a1", fingerprint="fp", resource_id="r1",
        policy_id="p1", policy_version=1, state=state, created_at=0.0, updated_at=0.0,
    )


@pytest.mark.parametrize(
    "current,target",
    [
        (TaskState.PENDING, TaskState.RUNNING),
        (TaskState.PENDING, TaskState.ABANDONED),
        (TaskState.RUNNING, TaskState.SUCCEEDED),
        (TaskState.RUNNING, TaskState.FAILED),
        (TaskState.FAILED, TaskState.PENDING),
        (TaskState.FAILED, TaskState.ABANDONED),
    ],
)
def test_legal_transitions_accepted(current, target):
    rec = _rec(current)
    apply_transition(rec, target, 12.0)
    assert rec.state is target
    assert rec.updated_at == 12.0


@pytest.mark.parametrize(
    "current,target",
    [
        (TaskState.PENDING, TaskState.SUCCEEDED),
        (TaskState.RUNNING, TaskState.PENDING),
        (TaskState.SUCCEEDED, TaskState.RUNNING),
        (TaskState.ABANDONED, TaskState.PENDING),
        (TaskState.SUCCEEDED, TaskState.FAILED),
    ],
)
def test_illegal_transitions_rejected(current, target):
    rec = _rec(current)
    with pytest.raises(StateTransitionError) as exc:
        assert_can_transition(rec, target)
    # 终态与一般非法转换必须给出不同原因码。
    if current in (TaskState.SUCCEEDED, TaskState.ABANDONED):
        assert exc.value.reason is RejectReason.TASK_ALREADY_TERMINAL
    else:
        assert exc.value.reason is RejectReason.ILLEGAL_TRANSITION
    assert rec.state is current  # 被拒绝不得改动状态


def test_transition_table_completeness():
    assert set(TRANSITIONS) == set(TaskState)
    assert TRANSITIONS[TaskState.SUCCEEDED] == frozenset()
    assert TRANSITIONS[TaskState.ABANDONED] == frozenset()


def test_failed_must_use_retry_channel(orchestrator, operator, clock):
    """FAILED 态的通用执行/流转被导向重试通道，原因码可区分。"""
    from orchestrator.errors import OrchestratorError
    from tests.conftest import make_alert

    orchestrator._executor.set_script("db-1", ["boom"])
    r = orchestrator.receive_alert(make_alert("a-1"))
    t = orchestrator.execute_task(r.task_id, operator)
    assert t.state is TaskState.FAILED

    with pytest.raises(OrchestratorError) as e1:
        orchestrator.execute_task(r.task_id, operator)
    assert e1.value.reason is RejectReason.RETRY_NOT_ALLOWED

    with pytest.raises(OrchestratorError) as e2:
        orchestrator.transition(r.task_id, TaskState.SUCCEEDED, operator)
    assert e2.value.reason is RejectReason.RETRY_NOT_ALLOWED
    # 被拒绝后状态保持 FAILED 不变。
    assert orchestrator.get_task(r.task_id).state is TaskState.FAILED
