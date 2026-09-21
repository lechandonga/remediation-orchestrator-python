"""并发与幂等：同一告警/资源并发接收、执行、重试结论一致，

无半更新、无重复执行、无计数漂移。
"""

from __future__ import annotations

import threading

import pytest

from orchestrator.errors import OrchestratorError
from orchestrator.models import AlertStatus, RejectReason, TaskState
from tests.conftest import make_alert, run_threads


def test_concurrent_identical_alerts_dispatch_once(orchestrator, operator):
    """同一指纹告警并发到达：恰好一个 DISPATCHED，其余 DEDUPED，任务唯一。"""
    start = threading.Barrier(8)

    def receive(i: int):
        start.wait()
        return orchestrator.receive_alert(make_alert(f"dup-{i}"))

    results = run_threads(receive, 8, [(i,) for i in range(8)])
    statuses = [r.status for r in results]
    assert statuses.count(AlertStatus.DISPATCHED) == 1
    assert statuses.count(AlertStatus.DEDUPED) == 7
    task_ids = {r.task_id for r in results if r.task_id}
    assert task_ids == {results[0].task_id} and len(task_ids) == 1
    assert len(orchestrator._store.all_tasks()) == 1


def test_concurrent_execute_same_task_single_side_effect(orchestrator, operator):
    """同一任务并发执行：只发生一次副作用，所有调用结论一致为成功。"""
    orchestrator._executor.set_delay("db-1", 0.05)
    r = orchestrator.receive_alert(make_alert("a-1"))
    start = threading.Barrier(6)

    def execute(_=None):
        start.wait()
        return orchestrator.execute_task(r.task_id, operator)

    finals = run_threads(execute, 6)
    assert all(t.state is TaskState.SUCCEEDED for t in finals)
    task = orchestrator.get_task(r.task_id)
    assert task.attempts == 1
    assert orchestrator._executor.side_effect_count("db-1") == 1
    assert task.revision == 3  # 认领(PENDING->RUNNING) + 结果(RUNNING->SUCCEEDED)


def test_concurrent_execute_failing_task_single_side_effect(orchestrator, operator, clock):
    orchestrator._executor.set_script("db-1", ["boom"])
    orchestrator._executor.set_delay("db-1", 0.05)
    r = orchestrator.receive_alert(make_alert("a-1"))
    start = threading.Barrier(6)

    def execute(_=None):
        start.wait()
        return orchestrator.execute_task(r.task_id, operator)

    finals = run_threads(execute, 6)
    assert all(t.state is TaskState.FAILED for t in finals)
    assert orchestrator._executor.side_effect_count("db-1") == 1
    assert orchestrator.get_task(r.task_id).attempts == 1


def test_concurrent_dispatch_same_resource_only_one_active(orchestrator):
    """同一资源、不同指纹的告警并发：一个 DISPATCHED，其余 RESOURCE_BUSY。"""
    start = threading.Barrier(6)

    def receive(i: int):
        start.wait()
        # title 不同 -> 指纹不同（不被去重），但资源相同。
        return orchestrator.receive_alert(make_alert(f"r-{i}", title=f"issue-{i}"))

    results = run_threads(receive, 6, [(i,) for i in range(6)])
    dispatched = [x for x in results if x.status is AlertStatus.DISPATCHED]
    busy = [x for x in results if x.status is AlertStatus.REJECTED]
    assert len(dispatched) == 1
    assert len(busy) == 5
    assert all(x.reject_reason is RejectReason.RESOURCE_BUSY for x in busy)
    active = [t for t in orchestrator._store.all_tasks()
              if t.state not in (TaskState.SUCCEEDED, TaskState.ABANDONED)]
    assert len(active) == 1


def test_concurrent_retry_only_one_requeue(orchestrator, operator, clock):
    """FAILED 任务并发重试：恰好一次回到 PENDING，attempts 不漂移。"""
    orchestrator._executor.set_script("db-1", ["boom", None])
    r = orchestrator.receive_alert(make_alert("a-1"))
    orchestrator.execute_task(r.task_id, operator)
    clock.advance(1.1)
    start = threading.Barrier(6)
    outcomes: list[str] = []

    def retry(_=None):
        start.wait()
        try:
            t = orchestrator.retry_task(r.task_id, operator)
            outcomes.append(f"ok:{t.state.value}")
        except OrchestratorError as exc:
            outcomes.append(f"err:{exc.reason.value}")

    run_threads(retry, 6)
    ok = [o for o in outcomes if o.startswith("ok")]
    err = [o for o in outcomes if o.startswith("err")]
    assert len(ok) == 1
    assert all(o == f"err:{RejectReason.RETRY_NOT_ALLOWED.value}" for o in err)
    task = orchestrator.get_task(r.task_id)
    assert task.state is TaskState.PENDING and task.attempts == 1
    # 唯一一次重试之后再执行，只产生第二次副作用。
    final = orchestrator.execute_task(r.task_id, operator)
    assert final.state is TaskState.SUCCEEDED and final.attempts == 2
    assert orchestrator._executor.side_effect_count("db-1") == 2


def test_cas_rejects_stale_write(store):
    """直接验证乐观锁：过期 revision 的写入整体被拒绝，无半更新。"""
    from orchestrator.models import ExecutionRecord

    t = ExecutionRecord(
        task_id="t1", alert_id="a1", fingerprint="fp", resource_id="r1",
        policy_id="p1", policy_version=1, state=TaskState.PENDING,
        created_at=0.0, updated_at=0.0,
    )
    store.create_task(t)
    fresh = store.get_task("t1")
    store.update_task(fresh, 1)  # revision 1 -> 2
    stale = ExecutionRecord(
        task_id="t1", alert_id="a1", fingerprint="fp", resource_id="r1",
        policy_id="p1", policy_version=1, state=TaskState.RUNNING,
        created_at=0.0, updated_at=0.0, revision=1,
    )
    with pytest.raises(OrchestratorError) as e:
        store.update_task(stale, 1)
    assert e.value.reason is RejectReason.CONCURRENT_MODIFICATION
    after = store.get_task("t1")
    assert after.revision == 2  # 陈旧写入未生效


def test_resource_reusable_after_terminal_state(orchestrator, operator, clock):
    """任务成功/放弃并释放锁后，同资源新告警可再次派发。"""
    r1 = orchestrator.receive_alert(make_alert("a-1"))
    t = orchestrator.execute_task(r1.task_id, operator)
    assert t.state is TaskState.SUCCEEDED

    # 同指纹、窗口外（成功后的窗口内重复仍属去重噪声），需要不同指纹验证资源闸门。
    clock.advance(400.0)
    r2 = orchestrator.receive_alert(make_alert("a-2", title="a-different-issue"))
    assert r2.status is AlertStatus.DISPATCHED
    assert r2.task_id != r1.task_id


def test_failed_in_progress_blocks_same_resource(orchestrator, operator):
    """失败后处于重试窗口的任务仍持有资源锁：同资源不同问题 -> resource_busy。"""
    orchestrator._executor.set_script("db-1", ["boom"])
    r1 = orchestrator.receive_alert(make_alert("a-1"))
    orchestrator.execute_task(r1.task_id, operator)  # FAILED，仍持锁
    r2 = orchestrator.receive_alert(make_alert("a-2", title="other-issue"))
    assert r2.status is AlertStatus.REJECTED
    assert r2.reject_reason is RejectReason.RESOURCE_BUSY
