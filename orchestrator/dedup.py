"""去重判定：短时间窗口内同一指纹只允许产生一次修复。

判定依据全部来自 Store 中持久化的 DISPATCHED 根记录，因此服务重启后
仍然能够识别重复告警，不会因内存状态丢失而重复执行。

窗口语义（固定而非滑动，稳定可解释）：
    锚点 = 同指纹、状态为 DISPATCHED、received_at 落在
           [now-window, now] 内的最新记录；窗口从最初派发时刻起算，
           DEDUPED 记录不续期。
被抑制 / 无策略 / 冲突 / 被拒绝的历史不作为锚点——这些情形应被
重新评估（例如维护窗口结束、策略补齐后）。
"""

from __future__ import annotations

from .clock import Clock, SystemClock
from .models import AlertEvent, AlertRecord
from .store import Store


class Deduplicator:
    def __init__(self, store: Store, clock: Clock | None = None) -> None:
        self._store = store
        self._clock = clock or SystemClock()

    def is_duplicate(self, event: AlertEvent, fingerprint: str) -> AlertRecord | None:
        """返回去重锚点（最初派发记录）；非重复返回 None。"""
        now_ts = self._clock.now()
        return self._store.find_active_fingerprint(
            fingerprint, now_ts, event.dedup_window_seconds
        )
