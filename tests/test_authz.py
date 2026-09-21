"""权限校验：未认证/权限不足分别拒绝，拒绝不改变已记录状态。"""

from __future__ import annotations

import pytest

from orchestrator.errors import AuthzError, OrchestratorError
from orchestrator.models import AlertStatus, RejectReason, TaskState
from tests.conftest import make_alert


def test_unauthenticated_execute_rejected(orchestrator):
    r = orchestrator.receive_alert(make_alert("a-1"))
    before = orchestrator.get_task(r.task_id)
    with pytest.raises(OrchestratorError) as e:
        orchestrator.execute_task(r.task_id, None)
    assert e.value.reason is RejectReason.UNAUTHENTICATED
    after = orchestrator.get_task(r.task_id)
    assert after.state is before.state is TaskState.PENDING
    assert after.revision == before.revision  # 拒绝不产生任何状态变更
    assert orchestrator._executor.side_effect_count("db-1") == 0


def test_forbidden_execute_rejected(orchestrator, readonly_user):
    r = orchestrator.receive_alert(make_alert("a-1"))
    with pytest.raises(OrchestratorError) as e:
        orchestrator.execute_task(r.task_id, readonly_user)
    assert e.value.reason is RejectReason.FORBIDDEN
    assert orchestrator.get_task(r.task_id).state is TaskState.PENDING


def test_unauthenticated_retry_and_transition_rejected(orchestrator, operator, clock):
    orchestrator._executor.set_script("db-1", ["boom"])
    r = orchestrator.receive_alert(make_alert("a-1"))
    orchestrator.execute_task(r.task_id, operator)
    clock.advance(1.1)
    with pytest.raises(OrchestratorError) as e1:
        orchestrator.retry_task(r.task_id, None)
    assert e1.value.reason is RejectReason.UNAUTHENTICATED
    with pytest.raises(OrchestratorError) as e2:
        orchestrator.transition(r.task_id, TaskState.ABANDONED, None)
    assert e2.value.reason is RejectReason.UNAUTHENTICATED
    assert orchestrator.get_task(r.task_id).state is TaskState.FAILED


def test_authorized_execute_succeeds(orchestrator, operator):
    r = orchestrator.receive_alert(make_alert("a-1"))
    t = orchestrator.execute_task(r.task_id, operator)
    assert t.state is TaskState.SUCCEEDED


def test_rejection_precedes_state_lookup_for_missing_task(orchestrator):
    """权限校验先于任务查找：未认证访问不存在任务仍是 UNAUTHENTICATED。"""
    with pytest.raises(OrchestratorError) as e:
        orchestrator.execute_task("task-does-not-exist", None)
    assert e.value.reason is RejectReason.UNAUTHENTICATED
