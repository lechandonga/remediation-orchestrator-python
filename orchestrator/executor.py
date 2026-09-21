"""修复执行器（本地模拟）。

- 按资源预置"失败脚本"：例如 ["err1", "err2", None] 表示前两次失败、
  第三次成功；脚本耗尽后默认成功。
- 仅在真正被调用执行时递增副作用计数，供测试断言"重试期间无重复
  副作用"（被退避/上限拒绝不会产生计数）。
- 调用线程持有 store 锁的时间不包含在这里：编排层先取状态快照与
  资源锁，再执行动作，再 CAS 提交，模拟真实执行的临界区划分。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from .models import ExecutionRecord, Policy


class ActionExecutor:
    def __init__(self) -> None:
        self._scripts: dict[str, deque[str | None]] = {}
        self._delays: dict[str, float] = {}
        self._side_effects: dict[str, int] = defaultdict(int)
        self._calls: list[tuple[str, str, int]] = []
        self._lock = threading.Lock()

    def set_script(self, resource_id: str, outcomes: list[str | None]) -> None:
        """预置某资源的执行结果序列（字符串=失败原因，None=成功）。"""
        self._scripts[resource_id] = deque(outcomes)

    def set_delay(self, resource_id: str, seconds: float) -> None:
        """为某资源的每次副作用增加耗时（用于并发测试放大竞态窗口）。"""
        self._delays[resource_id] = seconds

    def run(self, record: ExecutionRecord, policy: Policy) -> str | None:
        with self._lock:
            # 副作用在此刻发生，且每次调用恰好计数一次。
            self._side_effects[record.resource_id] += 1
            self._calls.append((record.task_id, policy.action, record.attempts))
            script = self._scripts.get(record.resource_id)
            delay = self._delays.get(record.resource_id, 0.0)
            outcome = script.popleft() if script else None
        if delay:
            time.sleep(delay)  # 在锁外等待，模拟真实修复耗时但不阻塞计数
        return outcome  # None 表示成功

    def side_effect_count(self, resource_id: str) -> int:
        with self._lock:
            return self._side_effects.get(resource_id, 0)

    def calls(self) -> list[tuple[str, str, int]]:
        with self._lock:
            return list(self._calls)
