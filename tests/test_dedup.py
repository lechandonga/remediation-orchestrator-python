"""重复告警去重 + 重启后不重复执行。"""

from __future__ import annotations

from orchestrator.models import AlertStatus
from orchestrator.service import Orchestrator
from orchestrator.store import Store
from tests.conftest import make_alert


def test_duplicate_alert_within_window_dispatches_once(orchestrator) -> None:
    r1 = orchestrator.receive_alert(make_alert("a-1"))
    r2 = orchestrator.receive_alert(make_alert("a-2"))
    r3 = orchestrator.receive_alert(make_alert("a-3"))

    assert r1.status is AlertStatus.DISPATCHED
    assert r2.status is AlertStatus.DEDUPED
    assert r3.status is AlertStatus.DEDUPED
    # 重复告警关联到同一个任务，绝不产生第二个执行。
    assert r1.task_id == r2.task_id == r3.task_id
    tasks = orchestrator._store.all_tasks()
    assert len(tasks) == 1


def test_dedup_fingerprint_is_stable_and_explainable(orchestrator) -> None:
    from orchestrator.fingerprint import alert_fingerprint

    # 不同 alert_id / 不同 fired_at 仍是同一问题 -> 同指纹。
    f1 = alert_fingerprint(make_alert("x", ts=1.0))
    f2 = alert_fingerprint(make_alert("y", ts=2.0))
    assert f1 == f2 and f1.startswith("fp1:")

    # 标签不同 -> 不同指纹（可解释：指纹输入发生变化）。
    f3 = alert_fingerprint(make_alert("z", labels={"kind": "web"}))
    assert f3 != f1


def test_alert_after_window_expires_dispatches_again(orchestrator, clock, operator) -> None:
    r1 = orchestrator.receive_alert(make_alert("a-1", window=10.0))
    assert r1.status is AlertStatus.DISPATCHED
    # 首次修复成功，任务到终态并释放资源锁。
    orchestrator.execute_task(r1.task_id, operator)
    # 重复告警不续期：窗口始终从最初派发时刻(1000)起算。
    clock.advance(9.9)
    assert orchestrator.receive_alert(make_alert("a-2", window=10.0)).status is AlertStatus.DEDUPED
    clock.advance(0.2)  # 距最初派发已 10.1s > 10s
    r3 = orchestrator.receive_alert(make_alert("a-3", window=10.0))
    assert r3.status is AlertStatus.DISPATCHED
    assert r3.task_id != r1.task_id


def test_restart_does_not_reexecute_for_duplicate(tmp_path, policy, clock, audit) -> None:
    data_dir = tmp_path / "data"
    store = Store(data_dir)
    orc = Orchestrator(store, policies=[policy], clock=clock, audit=audit)
    r1 = orc.receive_alert(make_alert("a-1"))
    assert r1.status is AlertStatus.DISPATCHED

    # 模拟服务重启：全新 Store 重放 JSONL，内存状态从零开始。
    store2 = Store(data_dir)
    orc2 = Orchestrator(store2, policies=[policy], clock=clock, audit=audit)
    r2 = orc2.receive_alert(make_alert("a-2"))  # 窗口内重复
    assert r2.status is AlertStatus.DEDUPED
    assert r2.task_id == r1.task_id
    assert len(store2.all_tasks()) == 1


def test_suppressed_history_is_not_dedup_anchor(orchestrator, clock, tmp_path, policy, audit) -> None:
    from orchestrator.models import MaintenanceWindow

    # 仅有维护窗口配置的编排器：首次被抑制。
    window = MaintenanceWindow(
        window_id="mw-1", resource_id="db-1", start_ts=0.0, end_ts=2000.0, reason="deploy"
    )
    store = Store(tmp_path / "d2")
    orc = Orchestrator(store, policies=[policy], windows=[window], clock=clock, audit=audit)
    s1 = orc.receive_alert(make_alert("a-1"))
    assert s1.status is AlertStatus.SUPPRESSED
    # 窗口结束后再来：不应把"被抑制"记录当作去重锚点，应正常派发。
    clock.advance(2000.0)
    s2 = orc.receive_alert(make_alert("a-2"))
    assert s2.status is AlertStatus.DISPATCHED
