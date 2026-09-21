"""策略匹配、确定性优先级、无匹配/冲突的明确标记。"""

from __future__ import annotations

import pytest

from orchestrator.errors import PolicyError
from orchestrator.models import AlertStatus, Policy, RejectReason
from orchestrator.policies import match_policies, select_policy
from tests.conftest import make_alert


def p(pid, prio, labels=None, enabled=True, version=1):
    return Policy(
        policy_id=pid, version=version, name=pid,
        match_labels=labels or {"kind": "db"}, action="act", priority=prio, enabled=enabled,
    )


def test_no_policy_matched_is_marked_not_silently_dropped(orchestrator, tmp_path, clock, audit):
    from orchestrator.service import Orchestrator
    from orchestrator.store import Store

    orc = Orchestrator(Store(tmp_path / "d"), policies=[], clock=clock, audit=audit)
    rec = orc.receive_alert(make_alert("a-1"))
    assert rec.status is AlertStatus.NO_POLICY
    assert rec.reject_reason is RejectReason.NO_POLICY_MATCHED
    assert rec.reject_detail  # 必须可解释


def test_highest_priority_wins_deterministically():
    event = make_alert("a-1")
    low = p("p-low", 50)
    high = p("p-high", 200)
    chosen = select_policy(event, [low, high])
    assert chosen.policy_id == "p-high"
    # 与插入顺序无关：调换顺序结论一致。
    assert select_policy(event, [high, low]).policy_id == "p-high"


def test_equal_priority_is_conflict_not_arbitrary_pick():
    event = make_alert("a-1")
    with pytest.raises(PolicyError) as exc:
        select_policy(event, [p("p-a", 100), p("p-b", 100)])
    assert exc.value.reason is RejectReason.POLICY_PRIORITY_CONFLICT
    assert "p-a" in exc.value.detail and "p-b" in exc.value.detail


def test_conflict_alert_is_marked(orchestrator, store):
    orchestrator._registry.add(p("p-a", 100))
    # p-restart(夹具, priority=100) 与 p-a 并列 -> 冲突。
    rec = orchestrator.receive_alert(make_alert("a-1"))
    assert rec.status is AlertStatus.CONFLICT
    assert rec.reject_reason is RejectReason.POLICY_PRIORITY_CONFLICT
    assert store.all_tasks() == []  # 冲突不得创建任何执行任务


def test_disabled_policies_are_ignored():
    event = make_alert("a-1")
    chosen = select_policy(event, [p("p-off", 999, enabled=False), p("p-on", 100)])
    assert chosen.policy_id == "p-on"


def test_label_match_is_and_semantics():
    event = make_alert("a-1", labels={"kind": "db", "env": "prod"})
    exact = p("p-exact", 100, labels={"kind": "db", "env": "prod"})
    partial = p("p-partial", 200, labels={"kind": "db", "env": "dev"})
    matched = match_policies(event, [exact, partial])
    assert [x.policy_id for x in matched] == ["p-exact"]


def test_policy_registry_rejects_duplicate_same_version():
    from orchestrator.models import Policy as P
    with pytest.raises(PolicyError) as exc:
        from orchestrator.policies import PolicyRegistry
        reg = PolicyRegistry([p("p1", 1, version=3)])
        reg.add(p("p1", 1, version=3))
    assert exc.value.reason is RejectReason.INCOMPATIBLE_CHANGE
