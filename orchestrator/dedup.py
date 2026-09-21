"""告警去重判定（骨架）。

判定结论稳定且可解释：基于指纹 + 冷却窗口，状态持久化于 Store。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import DedupDecision
from .store import Store


@dataclass(frozen=True)
class DedupResult:
    decision: DedupDecision
    reason: str


class Deduplicator:
    def __init__(self, store: Store) -> None:
        self._store = store
        raise NotImplementedError

    def check_and_claim(self, fingerprint: str, execution_id: str, now: float, cooldown_seconds: float) -> DedupResult:
        raise NotImplementedError
