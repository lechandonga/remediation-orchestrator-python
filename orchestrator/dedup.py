"""告警去重判定。

判定结论稳定且可解释：以告警指纹为键，状态持久化于 Store，
服务重启后占用与冷却窗口仍然有效，不会因状态丢失而重复执行。
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

    def check_and_claim(
        self, fingerprint: str, execution_id: str, now: float, cooldown_seconds: float
    ) -> DedupResult:
        """原子地判定并占用指纹，返回可解释的去重结论。"""
        outcome = self._store.claim_fingerprint(fingerprint, execution_id, now, cooldown_seconds)
        if outcome == "claimed":
            return DedupResult(
                DedupDecision.ACCEPTED_NEW,
                f"指纹 {fingerprint} 无活跃执行且不在冷却期，受理新执行 {execution_id}",
            )
        if outcome == "active":
            return DedupResult(
                DedupDecision.DUPLICATE_ACTIVE,
                f"指纹 {fingerprint} 已有活跃执行，重复告警被去重",
            )
        return DedupResult(
            DedupDecision.DUPLICATE_COOLDOWN,
            f"指纹 {fingerprint} 处于冷却窗口（{cooldown_seconds}s），重复告警被去重",
        )
