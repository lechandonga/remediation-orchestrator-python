"""维护窗口与抑制规则：不触发执行，原因可区分、可追溯。"""

from __future__ import annotations

from orchestrator.models import (
    AlertStatus,
    MaintenanceWindow,
    SuppressionReason,
    SuppressionRule,
)
from orchestrator.service import Orchestrator
from orchestrator.store import Store
from tests.conftest import make_alert


def _orc(tmp_path, policy, clock, audit, windows=None, rules=None):
    return Orchestrator(
        Store(tmp_path / "d"), policies=[policy],
        windows=windows, rules=rules, clock=clock, audit=audit,
    )


def test_maintenance_window_blocks_execution(tmp_path, policy, clock, audit):
    mw = MaintenanceWindow("mw-1", "db-1", 0.0, 2000.0, reason="rollout")
    orc = _orc(tmp_path, policy, clock, audit, windows=[mw])
    rec = orc.receive_alert(make_alert("a-1"))
    assert rec.status is AlertStatus.SUPPRESSED
    assert rec.suppress_reason is SuppressionReason.MAINTENANCE_WINDOW
    assert "mw-1" in rec.suppress_detail
    assert orc._store.all_tasks() == []  # 绝不创建执行任务


def test_suppression_rule_blocks_execution(tmp_path, policy, clock, audit):
    rule = SuppressionRule("rule-1", {"env": "canary"}, reason="canary noise")
    orc = _orc(tmp_path, policy, clock, audit, rules=[rule])
    rec = orc.receive_alert(make_alert("a-1", labels={"kind": "db", "env": "canary"}))
    assert rec.status is AlertStatus.SUPPRESSED
    assert rec.suppress_reason is SuppressionReason.SUPPRESSION_RULE
    assert "rule-1" in rec.suppress_detail
    assert orc._store.all_tasks() == []


def test_maintenance_window_takes_precedence_over_rule(tmp_path, policy, clock, audit):
    mw = MaintenanceWindow("mw-1", "db-1", 0.0, 2000.0)
    rule = SuppressionRule("rule-1", {"kind": "db"})
    orc = _orc(tmp_path, policy, clock, audit, windows=[mw], rules=[rule])
    rec = orc.receive_alert(make_alert("a-1"))
    assert rec.suppress_reason is SuppressionReason.MAINTENANCE_WINDOW


def test_window_boundary_is_inclusive_and_expires(tmp_path, policy, clock, audit):
    mw = MaintenanceWindow("mw-1", "db-1", 1000.0, 1010.0)
    orc = _orc(tmp_path, policy, clock, audit, windows=[mw])
    # now=1000 恰为起点：在窗口内。
    assert orc.receive_alert(make_alert("a-1")).status is AlertStatus.SUPPRESSED
    # 窗口结束（>1010）后：正常派发。
    clock.advance(10.1)
    rec = orc.receive_alert(make_alert("a-2"))
    assert rec.status is AlertStatus.DISPATCHED


def test_global_window_matches_all_resources(tmp_path, policy, clock, audit):
    mw = MaintenanceWindow("mw-all", "*", 0.0, 2000.0)
    orc = _orc(tmp_path, policy, clock, audit, windows=[mw])
    rec = orc.receive_alert(make_alert("a-1", resource_id="any-host-9"))
    assert rec.status is AlertStatus.SUPPRESSED


def test_unmatched_alert_is_not_suppressed(tmp_path, policy, clock, audit):
    rule = SuppressionRule("rule-1", {"env": "dev"})
    orc = _orc(tmp_path, policy, clock, audit, rules=[rule])
    rec = orc.receive_alert(make_alert("a-1", labels={"kind": "db"}))
    assert rec.status is AlertStatus.DISPATCHED
