"""审计可追溯：事件、策略、状态流转与判定依据必须留痕且结论可区分。"""

from __future__ import annotations

from orchestrator.models import AuditKind
from tests.conftest import make_alert


def test_audit_trail_covers_event_policy_transition_and_basis(orchestrator, operator):
    orchestrator._executor.set_script("db-1", ["boom", None])
    r = orchestrator.receive_alert(make_alert("a-1"))
    orchestrator.execute_task(r.task_id, operator)

    kinds = {e.kind for e in orchestrator._audit.events()}
    # 事件接收、策略选择、任务创建、状态流转四类留痕齐全。
    assert AuditKind.ALERT_RECEIVED in kinds
    assert AuditKind.POLICY_SELECTED in kinds
    assert AuditKind.TASK_CREATED in kinds
    assert AuditKind.STATE_TRANSITION in kinds

    by_kind = {e.kind: e for e in orchestrator._audit.events()}
    received = by_kind[AuditKind.ALERT_RECEIVED]
    assert received.detail["fingerprint"].startswith("fp1:")
    assert "basis" in received.detail  # 判定依据可解释

    selected = by_kind[AuditKind.POLICY_SELECTED]
    assert selected.detail["policy_id"] == "p-restart"
    assert selected.detail["priority"] == 100

    transition = by_kind[AuditKind.STATE_TRANSITION]
    assert transition.detail["result"] == "failed"
    assert transition.detail["attempts"] == 1
    assert transition.detail["side_effects_on_resource"] == 1
    # 审计序号严格单调。
    seqs = [e.seq for e in orchestrator._audit.events()]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_audit_distinguishes_rejection_reasons(orchestrator, tmp_path, clock, audit):
    from orchestrator.models import MaintenanceWindow, Policy
    from orchestrator.service import Orchestrator
    from orchestrator.store import Store

    # 无策略 -> no_policy 留痕。
    empty = Orchestrator(Store(tmp_path / "d1"), policies=[], clock=clock, audit=audit)
    empty.receive_alert(make_alert("x-1", resource_id="web-1"))
    # 维护窗口 -> suppressed(maintenance_window) 留痕。
    mw = MaintenanceWindow("mw", "db-1", 0.0, 5000.0)
    policy = Policy(
        policy_id="p-restart", version=1, name="restart-db",
        match_labels={"kind": "db"}, action="restart", priority=100,
    )
    suppressed = Orchestrator(
        Store(tmp_path / "d2"), policies=[policy], windows=[mw], clock=clock, audit=audit,
    )
    suppressed.receive_alert(make_alert("x-2"))

    no_policy_events = [e for e in audit.events() if e.kind is AuditKind.ALERT_NO_POLICY]
    suppressed_events = [e for e in audit.events() if e.kind is AuditKind.ALERT_SUPPRESSED]
    assert no_policy_events and suppressed_events
    assert no_policy_events[0].detail["reject_reason"] == "no_policy_matched"
    assert suppressed_events[0].detail["suppress_reason"] == "maintenance_window"


