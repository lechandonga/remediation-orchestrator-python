"""失败重试、指数退避、重试上限与终态；无重复副作用/计数漂移。"""

from __future__ import annotations

import pytest

from orchestrator.errors import OrchestratorError
from orchestrator.models import RejectReason, TaskState
from tests.conftest import make_alert


def _dispatch_and_fail(orc, operator, script, resource="db-1", alert="a-1"):
    orc._executor.set_script(resource, script)
    rec = orc.receive_alert(make_alert(alert, resource_id=resource))
    return rec.task_id


def test_retry_respects_exponential_backoff(orchestrator, operator, clock):
    tid = _dispatch_and_fail(orchestrator, operator, ["e1", "e2", None])
    t = orchestrator.execute_task(tid, operator)
    assert t.state is TaskState.FAILED
    assert t.attempts == 1

    # 退避未到：retry 与 execute 都被拒绝。
    clock.advance(0.5)  # base=1s
    with pytest.raises(OrchestratorError) as e:
        orchestrator.retry_task(tid, operator)
    assert e.value.reason is RejectReason.RETRY_BACKOFF_NOT_ELAPSED

    # 退避满足后重试，仅改状态不产生副作用、不计数。
    clock.advance(0.6)
    t = orchestrator.retry_task(tid, operator)
    assert t.state is TaskState.PENDING and t.attempts == 1
    t = orchestrator.execute_task(tid, operator)  # 第 2 次副作用
    assert t.attempts == 2

    # 第二次失败退避翻倍：base*2 = 2s。
    clock.advance(1.9)
    with pytest.raises(OrchestratorError):
        orchestrator.retry_task(tid, operator)
    clock.advance(0.2)
    orchestrator.retry_task(tid, operator)
    t = orchestrator.execute_task(tid, operator)  # 第 3 次成功
    assert t.state is TaskState.SUCCEEDED and t.attempts == 3
    assert orchestrator._executor.side_effect_count("db-1") == 3


def test_retry_exhaustion_enters_abandoned(orchestrator, operator, clock):
    # max_retries=2 -> 共 3 次机会，全部失败 -> 终态 ABANDONED。
    tid = _dispatch_and_fail(orchestrator, operator, ["e1", "e2", "e3"])
    orchestrator.execute_task(tid, operator)
    clock.advance(1.1)
    orchestrator.retry_task(tid, operator)
    orchestrator.execute_task(tid, operator)
    clock.advance(2.1)
    orchestrator.retry_task(tid, operator)
    final = orchestrator.execute_task(tid, operator)  # 第 3 次失败 -> 耗尽
    assert final.state is TaskState.ABANDONED
    assert final.attempts == 3
    assert orchestrator._executor.side_effect_count("db-1") == 3
    # 资源锁在终态释放。
    assert orchestrator._store.lock_resource("db-1", "other-task") is True

    # 终态后任何重试都被明确拒绝，原因稳定为 retry_exhausted。
    with pytest.raises(OrchestratorError) as e:
        orchestrator.retry_task(tid, operator)
    assert e.value.reason is RejectReason.RETRY_EXHAUSTED


def test_retry_request_after_exhaustion_is_idempotent_no_extra_side_effect(
    orchestrator, operator, clock
):
    tid = _dispatch_and_fail(orchestrator, operator, ["e1", "e2", "e3"])
    orchestrator.execute_task(tid, operator)
    clock.advance(1.1); orchestrator.retry_task(tid, operator)
    orchestrator.execute_task(tid, operator)
    clock.advance(2.1); orchestrator.retry_task(tid, operator)
    orchestrator.execute_task(tid, operator)  # 耗尽 -> ABANDONED
    before = orchestrator._executor.side_effect_count("db-1")
    for _ in range(5):  # 耗尽后反复请求：结论一致、零新增副作用
        with pytest.raises(OrchestratorError):
            orchestrator.retry_task(tid, operator)
    assert orchestrator._executor.side_effect_count("db-1") == before == 3
    assert orchestrator.get_task(tid).state is TaskState.ABANDONED


def test_retry_on_non_failed_state_rejected(orchestrator, operator):
    tid = _dispatch_and_fail(orchestrator, operator, [None])
    with pytest.raises(OrchestratorError) as e:
        orchestrator.retry_task(tid, operator)  # 仍为 PENDING
    assert e.value.reason is RejectReason.RETRY_NOT_ALLOWED


def test_retry_does_not_increment_attempts(orchestrator, operator, clock):
    tid = _dispatch_and_fail(orchestrator, operator, ["e1", None])
    orchestrator.execute_task(tid, operator)
    clock.advance(1.1)
    rev_before = orchestrator.get_task(tid).revision
    t = orchestrator.retry_task(tid, operator)
    assert t.attempts == 1  # 重试调度不计数
    assert t.revision == rev_before + 1  # 但 revision 合法递增
