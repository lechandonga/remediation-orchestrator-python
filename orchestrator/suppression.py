"""维护窗口与抑制规则（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass

from .models import AlertEvent, SuppressionReason


@dataclass(frozen=True)
class MaintenanceWindow:
    window_id: str
    resource: str  # "*" 表示全部
    start: float
    end: float


@dataclass(frozen=True)
class SuppressionCheck:
    suppressed: bool
    reason: SuppressionReason | None = None
    source: str = ""  # 可追溯：窗口 ID 或抑制规则 ID


class SuppressionRegistry:
    def __init__(self) -> None:
        raise NotImplementedError

    def add_window(self, window: MaintenanceWindow) -> None:
        raise NotImplementedError

    def suppress_fingerprint(self, fingerprint: str, rule_id: str) -> None:
        raise NotImplementedError

    def check(self, alert: AlertEvent, now: float) -> SuppressionCheck:
        raise NotImplementedError
