"""维护窗口与抑制规则。

处于维护窗口或已被手动抑制的告警不得触发修复执行；
抑制原因可区分（MAINTENANCE_WINDOW / MANUAL_SUPPRESSION），
且 source 字段记录来源（窗口 ID 或规则 ID）以便追溯。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from .models import AlertEvent, SuppressionReason


@dataclass(frozen=True)
class MaintenanceWindow:
    window_id: str
    resource: str  # "*" 表示全部资源
    start: float
    end: float


@dataclass(frozen=True)
class SuppressionCheck:
    suppressed: bool
    reason: SuppressionReason | None = None
    source: str = ""  # 可追溯：窗口 ID 或抑制规则 ID


class SuppressionRegistry:
    def __init__(self) -> None:
        self._windows: list[MaintenanceWindow] = []
        self._manual: dict[str, str] = {}  # fingerprint -> rule_id
        self._lock = threading.Lock()

    def add_window(self, window: MaintenanceWindow) -> None:
        with self._lock:
            self._windows.append(window)

    def suppress_fingerprint(self, fingerprint: str, rule_id: str) -> None:
        with self._lock:
            self._manual[fingerprint] = rule_id

    def check(self, alert: AlertEvent, now: float) -> SuppressionCheck:
        with self._lock:
            windows = list(self._windows)
            rule_id = self._manual.get(alert.fingerprint)
        for w in windows:
            if w.start <= now < w.end and w.resource in ("*", alert.resource):
                return SuppressionCheck(True, SuppressionReason.MAINTENANCE_WINDOW, w.window_id)
        if rule_id is not None:
            return SuppressionCheck(True, SuppressionReason.MANUAL_SUPPRESSION, rule_id)
        return SuppressionCheck(False)
