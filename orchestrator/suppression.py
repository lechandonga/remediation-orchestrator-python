"""维护窗口与抑制规则。

优先级固定且可解释：维护窗口 > 显式抑制规则。
两种命中返回不同的 SuppressionReason，审计中可区分、可追溯。
"""

from __future__ import annotations

from .models import (
    AlertEvent,
    MaintenanceWindow,
    SuppressionReason,
    SuppressionRule,
)


def _labels_match(event_labels: dict[str, str], required: dict[str, str]) -> bool:
    """AND 语义：要求中的每个 label 都必须与事件标签相等。"""
    return all(event_labels.get(k) == v for k, v in required.items())


class SuppressionService:
    def __init__(
        self,
        windows: list[MaintenanceWindow] | None = None,
        rules: list[SuppressionRule] | None = None,
    ) -> None:
        # 按开始时间排序，命中解释时给出确定的 window_id/rule_id。
        self._windows = sorted(windows or [], key=lambda w: (w.start_ts, w.window_id))
        self._rules = list(rules or [])

    def _match_window(self, event: AlertEvent, ts: float) -> MaintenanceWindow | None:
        for w in self._windows:
            resource_ok = w.resource_id == "*" or w.resource_id == event.resource_id
            in_time = w.start_ts <= ts <= w.end_ts
            if resource_ok and in_time:
                return w
        return None

    def _match_rule(self, event: AlertEvent) -> SuppressionRule | None:
        for r in self._rules:
            if _labels_match(event.labels, r.match_labels):
                return r
        return None

    def check_detail(self, event: AlertEvent, ts: float) -> tuple[SuppressionReason, str] | None:
        window = self._match_window(event, ts)
        if window is not None:
            return (
                SuppressionReason.MAINTENANCE_WINDOW,
                f"window={window.window_id} resource={window.resource_id} "
                f"range=[{window.start_ts},{window.end_ts}] reason={window.reason!r}",
            )
        rule = self._match_rule(event)
        if rule is not None:
            return (
                SuppressionReason.SUPPRESSION_RULE,
                f"rule={rule.rule_id} match={rule.match_labels} reason={rule.reason!r}",
            )
        return None

    def check(self, event: AlertEvent, ts: float) -> SuppressionReason | None:
        hit = self.check_detail(event, ts)
        return hit[0] if hit else None
