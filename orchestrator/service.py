"""编排服务：串联鉴权、抑制、去重、策略匹配、状态机、执行与重试。

受理顺序（确定且可审计）：
  鉴权 -> 抑制检查（只读）-> 去重占用（原子）-> 策略匹配 -> 创建 PENDING 执行记录。
每一步的结论都会写入审计日志；任何一步拒绝都不会改变已有执行记录的状态。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from .authz import AuthzError, authorize
from .dedup import Deduplicator
from .models import (
    AlertEvent,
    ExecutionRecord,
    ExecutionStatus,
    RemediationPolicy,
    TransitionRejection,
)
from .policy import PolicyMatch, select_policy, validate_policy
from .retry import decide_retry
from .statemachine import TransitionError, validate_transition
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
    def __init__(
        self,
        store: Store,
        policies: list[RemediationPolicy],
        suppression: SuppressionRegistry,
        base_backoff: float = 1.0,
    ) -> None:
        self._store = store
        self._policies: dict[str, RemediationPolicy] = {}
        for p in policies:
            validate_policy(p)  # 不兼容策略在加载时即被明确拒绝
            self._policies[p.policy_id] = p
        self._suppression = suppression
        self._dedup = Deduplicator(store)
        self._base_backoff = base_backoff

    # ---- 告警受理 ----
    def ingest_alert(
        self, alert: AlertEvent, now: float, token: str | None = None
    ) -> IntakeResult:
        """接收告警：鉴权 -> 抑制 -> 去重 -> 策略匹配 -> 创建执行记录。"""
        try:
            subject = authorize(token)
        except AuthzError as e:
            self._store.audit("AUTHZ_REJECTED", alert.alert_id, e.reason.value, now)
            raise

        check = self._suppression.check(alert, now)
        if check.suppressed:
            reason = f"{check.reason.value} (source={check.source})"
            self._store.audit("SUPPRESSED", alert.alert_id, reason, now)
            return IntakeResult(False, "SUPPRESSED", reason)

        execution_id = f"exec-{uuid.uuid4().hex[:12]}"
        # 先匹配策略以获取冷却窗口配置，再进行去重占用
        match: PolicyMatch = select_policy(alert, list(self._policies.values()))
        cooldown = match.policy.cooldown_seconds if match.policy else 0.0

        dedup = self._dedup.check_and_claim(alert.fingerprint, execution_id, now, cooldown)
        if dedup.decision.name != "ACCEPTED_NEW":
            self._store.audit("DEDUP_REJECTED", alert.alert_id, dedup.reason, now)
            return IntakeResult(False, dedup.decision.name, dedup.reason)

        if match.reason != "MATCHED":
            # 无匹配或冲突：释放指纹占用，明确标记，绝不静默丢弃
            self._store.release_fingerprint(alert.fingerprint, now, 0.0)
            detail = (
                f"无可用策略命中告警 {alert.alert_id}"
                if match.reason == "NO_MATCH"
                else f"策略冲突，候选：{','.join(match.candidates)}"
            )
            self._store.audit(f"POLICY_{match.reason}", alert.alert_id, detail, now)
            return IntakeResult(False, f"POLICY_{match.reason}", detail)

        record = ExecutionRecord(
            execution_id=execution_id,
            fingerprint=alert.fingerprint,
            policy_id=match.policy.policy_id,
            policy_version=match.policy.version,
        )
        self._store.create_execution(record)
        self._store.audit(
            "ACCEPTED",
            execution_id,
            f"主体 {subject} 受理告警 {alert.alert_id}，命中策略 {match.policy.policy_id}",
            now,
        )
        return IntakeResult(True, "ACCEPTED", "已创建待执行记录", execution_id)

    # ---- 执行推进 ----
    def _transition(
        self,
        record: ExecutionRecord,
        target: ExecutionStatus,
        attempt: int,
        detail: str,
        now: float,
    ) -> ExecutionRecord:
        """校验并原子地推进状态；并发冲突时给出可区分原因。"""
        validate_transition(record.status, target)
        ok = self._store.compare_and_set_status(
            record.execution_id, (record.status,), target, attempt, detail, now
        )
        if not ok:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {record.execution_id} 状态被并发修改，流转到 {target.value} 失败",
            )
        self._store.audit(
            "TRANSITION",
            record.execution_id,
            f"{record.status.value} -> {target.value}: {detail}",
            now,
        )
        record.status = target
        record.attempt = attempt
        record.detail = detail
        return record

    def run_execution(self, execution_id: str, succeed: bool, now: float) -> ExecutionRecord:
        """推进一次执行：PENDING->RUNNING->(SUCCEEDED|FAILED)，失败按策略重试或放弃。"""
        record = self._store.get_execution(execution_id)
        if record is None:
            raise KeyError(f"执行记录 {execution_id} 不存在")
        policy = self._policies[record.policy_id]

        record = self._transition(
            record, ExecutionStatus.RUNNING, record.attempt + 1, "开始执行", now
        )
        if succeed:
            record = self._transition(record, ExecutionStatus.SUCCEEDED, record.attempt, "执行成功", now)
            self._store.release_fingerprint(record.fingerprint, now, policy.cooldown_seconds)
            return record

        record = self._transition(record, ExecutionStatus.FAILED, record.attempt, "执行失败", now)
        decision = decide_retry(record.attempt, policy.max_retries, self._base_backoff)
        self._store.audit("RETRY_DECISION", execution_id, decision.reason, now)
        if decision.should_retry:
            record = self._transition(
                record, ExecutionStatus.PENDING, record.attempt, decision.reason, now
            )
        else:
            record = self._transition(
                record, ExecutionStatus.ABANDONED, record.attempt, decision.reason, now
            )
            self._store.release_fingerprint(record.fingerprint, now, policy.cooldown_seconds)
        return record

    # ---- 查询 ----
    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        return self._store.get_execution(execution_id)
