"""编排服务：串联去重、抑制、策略匹配、状态机、执行与重试（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass

from .authz import authorize
from .dedup import Deduplicator
from .models import AlertEvent, ExecutionRecord, RemediationPolicy
from .policy import select_policy
from .statemachine import validate_transition
from .store import Store
from .suppression import SuppressionRegistry


@dataclass(frozen=True)
class IntakeResult:
    """告警受理结论（可审计）。"""

    accepted: bool
    decision: str
    reason: str
    execution_id: str | None = None


class OrchestratorService:
    def __init__(self, store: Store, policies: list[RemediationPolicy], suppression: SuppressionRegistry) -> None:
        self._store = store
        self._policies = policies
        self._suppression = suppression
        raise NotImplementedError

    def ingest_alert(self, alert: AlertEvent, now: float, token: str | None = None) -> IntakeResult:
        """接收告警：鉴权 -> 去重 -> 抑制 -> 策略匹配 -> 创建执行记录。"""
        raise NotImplementedError

    def run_execution(self, execution_id: str, succeed: bool, now: float) -> ExecutionRecord:
        """推进一次执行：状态流转 + 失败重试/放弃。"""
        raise NotImplementedError

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        raise NotImplementedError
