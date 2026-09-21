"""时间源：测试可替换为可控时钟。"""

from __future__ import annotations

import time
from collections import deque


class Clock:
    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> float:
        return time.time()


class FakeClock(Clock):
    """测试时钟：可推进、可按脚本取值。"""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._t = start
        self._script: deque[float] = deque()

    def now(self) -> float:
        if self._script:
            return self._script.popleft()
        return self._t

    def advance(self, seconds: float) -> float:
        self._t += seconds
        return self._t

    def set(self, ts: float) -> None:
        self._t = ts

    def script(self, values: list[float]) -> None:
        self._script.extend(values)
