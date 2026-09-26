"""编排服务：串联鉴权、抑制、去重、策略匹配、状态机、执行与重试。

受理顺序（确定且可审计）：
  鉴权 -> 抑制检查（只读）-> 去重占用（原子）-> 策略匹配 -> 创建 PENDING 执行记录。
每一步的结论都会写入审计日志；任何一步拒绝都不会改变已有执行记录的状态。

重试排期（v2 记录）：失败后进入 WAITING_RETRY 并携带 next_attempt_at 与
remaining_attempts；到期前任何推进请求都以可区分原因被拒绝，到期后由
run_execution 或后台批量消费（list_due_retries + claim_due_retry）推进。
重启恢复（recover_interrupted）：悬挂的 RUNNING 记录收尾为 ABANDONED 并写审计；
WAITING_RETRY 记录的排期字段原样保留。
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
from .statemachine import TERMINAL_STATES, TransitionError, validate_transition
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
        next_attempt_at: float | None = None,
        remaining_attempts: int | None = None,
    ) -> ExecutionRecord:
        """校验并原子地推进状态；并发冲突时给出可区分原因。"""
        validate_transition(record.status, target)
        ok = self._store.compare_and_set_status(
            record.execution_id,
            (record.status,),
            target,
            attempt,
            detail,
            now,
            next_attempt_at=next_attempt_at,
            remaining_attempts=remaining_attempts,
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
        if next_attempt_at is not None:
            record.next_attempt_at = next_attempt_at
        if remaining_attempts is not None:
            record.remaining_attempts = remaining_attempts
        return record

    def _load_runnable(self, execution_id: str) -> ExecutionRecord:
        """读取记录；不存在 -> KeyError，版本不受支持 -> RecordVersionError。"""
        record = self._store.get_execution(execution_id)
        if record is None:
            raise KeyError(f"执行记录 {execution_id} 不存在")
        return record

    def _claim_waiting(self, record: ExecutionRecord, now: float) -> ExecutionRecord:
        """接管一条 WAITING_RETRY 记录并推进到 RUNNING。

        拒绝原因可区分：
        - RETRY_EXHAUSTED：剩余尝试次数已耗尽；
        - RETRY_NOT_DUE：还没到最早可执行时刻；
        - ILLEGAL_TRANSITION：被并发消费方抢先接管（CAS 失败）。
        """
        if record.remaining_attempts == 0:
            raise TransitionError(
                TransitionRejection.RETRY_EXHAUSTED,
                f"执行 {record.execution_id} 剩余尝试次数已耗尽，不能再试",
            )
        if now < record.next_attempt_at:
            raise TransitionError(
                TransitionRejection.RETRY_NOT_DUE,
                f"执行 {record.execution_id} 未到最早可执行时刻 "
                f"{record.next_attempt_at}（当前 {now}）",
            )
        if not self._store.claim_due_retry(record.execution_id, now):
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {record.execution_id} 已被其他消费方接管或状态被并发修改",
            )
        self._store.audit(
            "TRANSITION",
            record.execution_id,
            f"{ExecutionStatus.WAITING_RETRY.value} -> RUNNING: 到期接管，开始第 "
            f"{record.attempt + 1} 次尝试",
            now,
        )
        record.status = ExecutionStatus.RUNNING
        record.attempt += 1
        return record

    def _claim_for_run(self, execution_id: str, now: float) -> ExecutionRecord:
        """把一条可执行记录推进到 RUNNING（PENDING 立即执行；WAITING_RETRY 需到期）。"""
        record = self._load_runnable(execution_id)
        if record.status is ExecutionStatus.WAITING_RETRY:
            return self._claim_waiting(record, now)
        # PENDING -> RUNNING；其余状态由状态机给出 TERMINAL_STATE / ILLEGAL_TRANSITION
        return self._transition(
            record, ExecutionStatus.RUNNING, record.attempt + 1, "开始执行", now
        )

    def _finish(self, record: ExecutionRecord, succeed: bool, now: float) -> ExecutionRecord:
        """对 RUNNING 记录给出执行结果：成功收尾，失败按版本语义排期或放弃。"""
        policy = self._policies[record.policy_id]
        if succeed:
            record = self._transition(
                record, ExecutionStatus.SUCCEEDED, record.attempt, "执行成功", now
            )
            self._store.release_fingerprint(record.fingerprint, now, policy.cooldown_seconds)
            return record

        record = self._transition(record, ExecutionStatus.FAILED, record.attempt, "执行失败", now)
        decision = decide_retry(record.attempt, policy.max_retries, self._base_backoff)
        self._store.audit("RETRY_DECISION", record.execution_id, decision.reason, now)
        if not decision.should_retry:
            record = self._transition(
                record, ExecutionStatus.ABANDONED, record.attempt, decision.reason, now
            )
            self._store.release_fingerprint(record.fingerprint, now, policy.cooldown_seconds)
        elif record.record_version >= 2:
            # v2：进入等待重试阶段，携带最早可执行时刻与剩余尝试次数
            record = self._transition(
                record,
                ExecutionStatus.WAITING_RETRY,
                record.attempt,
                decision.reason,
                now,
                next_attempt_at=now + decision.backoff_seconds,
                remaining_attempts=decision.remaining_attempts,
            )
        else:
            # v1 历史记录：保持"失败后立即回到 PENDING"的旧语义
            record = self._transition(
                record, ExecutionStatus.PENDING, record.attempt, decision.reason, now
            )
        return record

    def run_execution(self, execution_id: str, succeed: bool, now: float) -> ExecutionRecord:
        """推进一次执行：(PENDING|到期的 WAITING_RETRY)->RUNNING->(SUCCEEDED|FAILED)。

        失败按策略重试（v2 记录进入定时排期的 WAITING_RETRY）或放弃。
        """
        record = self._claim_for_run(execution_id, now)
        return self._finish(record, succeed, now)

    # ---- 到期重试的批量消费 ----
    def list_due_retries(self, now: float) -> list[ExecutionRecord]:
        """列出当前所有已到期的等待重试记录，供后台任务或操作者批量消费。"""
        return self._store.list_due_retries(now)

    def claim_due_retry(self, execution_id: str, now: float) -> ExecutionRecord:
        """原子接管一条到期重试记录（WAITING_RETRY -> RUNNING）。

        多路并发消费同一记录时只有一个消费方成功；其余拿到可区分的失败结论，
        不会重复执行，也不会让尝试计数漂移。
        """
        record = self._load_runnable(execution_id)
        if record.status is not ExecutionStatus.WAITING_RETRY:
            if record.status in TERMINAL_STATES:
                raise TransitionError(
                    TransitionRejection.TERMINAL_STATE,
                    f"执行 {execution_id} 已处于终态 {record.status.value}，不能接管",
                )
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {execution_id} 当前状态 {record.status.value} 不是等待重试，不能接管",
            )
        return self._claim_waiting(record, now)

    def finish_execution(self, execution_id: str, succeed: bool, now: float) -> ExecutionRecord:
        """对已被接管（RUNNING）的记录上报执行结果。"""
        record = self._load_runnable(execution_id)
        if record.status is not ExecutionStatus.RUNNING:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {execution_id} 当前状态 {record.status.value} 不是 RUNNING，不能上报结果",
            )
        return self._finish(record, succeed, now)

    # ---- 重启恢复 ----
    def recover_interrupted(self, now: float) -> list[str]:
        """把重启前已开始但未完结（RUNNING 悬挂）的记录收尾为 ABANDONED。

        - 收尾经由 CAS 完成并写 RECOVERY 审计（含依据），重复调用幂等；
        - 不增加 attempt，不对已计数的尝试重复计数；
        - WAITING_RETRY 记录的排期字段原样保留，不在此触碰。
        """
        recovered: list[str] = []
        for record in self._store.list_interrupted():
            detail = (
                "RECOVERY_INTERRUPTED: 服务重启前执行未给出结果，"
                "按恢复口径收尾为终态 ABANDONED"
            )
            ok = self._store.compare_and_set_status(
                record.execution_id,
                (ExecutionStatus.RUNNING,),
                ExecutionStatus.ABANDONED,
                record.attempt,
                detail,
                now,
            )
            if not ok:
                continue  # 已被并发恢复者处理，不重复计数
            policy = self._policies.get(record.policy_id)
            cooldown = policy.cooldown_seconds if policy else 0.0
            self._store.release_fingerprint(record.fingerprint, now, cooldown)
            self._store.audit("RECOVERY", record.execution_id, detail, now)
            recovered.append(record.execution_id)
        return recovered

    # ---- 查询 ----
    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        return self._store.get_execution(execution_id)
