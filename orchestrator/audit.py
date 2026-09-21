"""结构化审计日志。

单测日志中打印：事件、命中的策略、状态流转与每一步判定依据。
同时在内存保留全部 AuditEvent，便于测试断言"结论一致且原因可区分"。
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from .clock import Clock, SystemClock
from .models import AuditEvent, AuditKind


class AuditLog:
    def __init__(
        self,
        logger_name: str = "orchestrator.audit",
        echo: bool = True,
        clock: Clock | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._events: list[AuditEvent] = []
        self._lock = threading.Lock()
        self._logger = logging.getLogger(logger_name)
        self._echo = echo
        if echo and not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[AUDIT] %(message)s"))
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
            self._logger.propagate = False

    def emit(self, kind: AuditKind, summary: str, **detail: Any) -> AuditEvent:
        with self._lock:
            seq = len(self._events) + 1
            event = AuditEvent(
                seq=seq, ts=self._clock.now(), kind=kind, summary=summary, detail=dict(detail)
            )
            self._events.append(event)
        if self._echo:
            payload = json.dumps(
                {"seq": event.seq, "kind": event.kind.value, "summary": summary, **detail},
                ensure_ascii=False,
                default=str,
                sort_keys=True,
            )
            self._logger.info(payload)
        return event

    def events(self) -> list[AuditEvent]:
        with self._lock:
            return list(self._events)
