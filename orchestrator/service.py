"""编排服务：串联鉴权、抑制、去重、策略匹配、状态机、执行与重试。

受理顺序（确定且可审计）：
  鉴权 -> 抑制检查（只读）-> 去重占用（原子）-> 策略匹配 -> 创建 PENDING 执行记录。
每一步的结论都会写入审计日志；任何一步拒绝都不会改变已有执行记录的状态。
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass

from .authz import AuthzError, authorize
from .dedup import Deduplicator
from .models import (
    RECORD_VERSION_V1,
    RECORD_VERSION_V2,
    AlertEvent,
    ExecutionRecord,
    ExecutionStatus,
    RemediationPolicy,
    TransitionRejection,
)
from .policy import PolicyMatch, select_policy, validate_policy
from .retry import decide_retry, schedule_next_attempt
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
        record_version: int = RECORD_VERSION_V1,
    ) -> None:
        self._store = store
        self._policies: dict[str, RemediationPolicy] = {}
        for p in policies:
            validate_policy(p)  # 不兼容策略在加载时即被明确拒绝
            self._policies[p.policy_id] = p
        self._suppression = suppression
        self._dedup = Deduplicator(store)
        self._base_backoff = base_backoff
        self._record_version = record_version
        # 进程内按记录的"执行槽"：同一条记录同一时刻只允许一个执行流。
        # 用非阻塞获取来确定地区分"并发抢占失败"与"终态后再执行"。
        self._slots_guard = threading.Lock()
        self._slots: dict[str, threading.Lock] = {}

    def _try_acquire_slot(self, execution_id: str) -> bool:
        """原子地"获取或创建"该记录的执行槽并尝试非阻塞占用。

        槽的创建与占用必须在同一把 guard 临界区内完成，否则并发线程可能
        各自创建出不同的锁对象而同时占用成功。
        """
        with self._slots_guard:
            slot = self._slots.get(execution_id)
            if slot is None:
                slot = threading.Lock()
                self._slots[execution_id] = slot
            return slot.acquire(blocking=False)

    def _release_slot(self, execution_id: str) -> None:
        with self._slots_guard:
            slot = self._slots.get(execution_id)
        if slot is not None:
            slot.release()

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
            record_version=self._record_version,
            # v2 记录在创建时即写入完整尝试预算（首次 + 重试次数）；
            # v1 记录不使用该字段，保持 0。
            remaining_attempts=(
                match.policy.max_retries + 1
                if self._record_version >= RECORD_VERSION_V2
                else 0
            ),
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

    def _assert_runnable(self, record: ExecutionRecord, now: float) -> None:
        """推进前闸门：把三类拒绝情形明确分开。

        - RETRY_NOT_DUE：WAITING_RETRY 且未到最早可执行时刻；
        - RETRY_ALREADY_EXHAUSTED：WAITING_RETRY 但剩余尝试次数为 0；
        - TERMINAL_STATE：已是终态；
        - ILLEGAL_TRANSITION：当前状态（如 RUNNING）根本不允许推进。
        """
        st = record.status
        if st in TERMINAL_STATES:
            raise TransitionError(
                TransitionRejection.TERMINAL_STATE,
                f"执行 {record.execution_id} 状态 {st.value} 为终态，不允许推进",
            )
        if st is ExecutionStatus.WAITING_RETRY:
            if record.remaining_attempts <= 0:
                raise TransitionError(
                    TransitionRejection.RETRY_ALREADY_EXHAUSTED,
                    f"执行 {record.execution_id} 剩余尝试次数为 0，不能再试",
                )
            if record.next_run_at > now:
                raise TransitionError(
                    TransitionRejection.RETRY_NOT_DUE,
                    f"执行 {record.execution_id} 尚未到最早可执行时刻 "
                    f"{record.next_run_at}（当前 {now}）",
                )
            return
        if st is ExecutionStatus.PENDING:
            return
        raise TransitionError(
            TransitionRejection.ILLEGAL_TRANSITION,
            f"执行 {record.execution_id} 当前状态 {st.value} 不允许直接推进执行",
        )

    def _claim_waiting_to_running(
        self, record: ExecutionRecord, now: float, detail: str
    ) -> ExecutionRecord:
        """原子接管一条到期的 WAITING_RETRY：CAS 要求状态不变且已到期。

        并发下只有一个调用方成功；失败方得到 ILLEGAL_TRANSITION，
        不会重复执行，尝试次数也只在成功接管时递增一次。
        """
        validate_transition(ExecutionStatus.WAITING_RETRY, ExecutionStatus.RUNNING)
        attempt = record.attempt + 1
        remaining_after = record.remaining_attempts - 1
        ok = self._store.compare_and_set_status(
            record.execution_id,
            (ExecutionStatus.WAITING_RETRY,),
            ExecutionStatus.RUNNING,
            attempt,
            detail,
            now,
            require_due=True,
            remaining_attempts=remaining_after,
        )
        if not ok:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {record.execution_id} 的到期重试被并发方接管或状态已变更，接管失败",
            )
        self._store.audit(
            "CLAIM_RETRY",
            record.execution_id,
            f"WAITING_RETRY -> RUNNING: {detail}，第 {attempt} 次尝试，"
            f"接管后剩余 {remaining_after} 次",
            now,
        )
        record.status = ExecutionStatus.RUNNING
        record.attempt = attempt
        record.detail = detail
        record.remaining_attempts = remaining_after
        return record

    def _to_terminal(
        self,
        record: ExecutionRecord,
        target: ExecutionStatus,
        attempt: int,
        detail: str,
        now: float,
    ) -> ExecutionRecord:
        """原子地进入终态，并清空排期字段（计数卫生，避免残留旧排期）。"""
        validate_transition(record.status, target)
        ok = self._store.compare_and_set_status(
            record.execution_id,
            (record.status,),
            target,
            attempt,
            detail,
            now,
            next_run_at=0.0,
            remaining_attempts=0,
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
        record.next_run_at = 0.0
        record.remaining_attempts = 0
        return record

    def _claim_pending_to_running(
        self, record: ExecutionRecord, now: float
    ) -> ExecutionRecord:
        """原子接管一条 PENDING：CAS 要求状态仍为 PENDING。

        并发推进同一条记录时只有一个调用方成功接管；其它方在 DB 条件更新上
        失败并得到 ILLEGAL_TRANSITION，而不是随后读到终态而误报 TERMINAL_STATE。
        """
        validate_transition(ExecutionStatus.PENDING, ExecutionStatus.RUNNING)
        attempt = record.attempt + 1
        detail = "开始执行"
        ok = self._store.compare_and_set_status(
            record.execution_id,
            (ExecutionStatus.PENDING,),
            ExecutionStatus.RUNNING,
            attempt,
            detail,
            now,
        )
        if not ok:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {record.execution_id} 被并发方接管或状态已变更，开始执行失败",
            )
        self._store.audit(
            "TRANSITION",
            record.execution_id,
            f"PENDING -> RUNNING: {detail}",
            now,
        )
        record.status = ExecutionStatus.RUNNING
        record.attempt = attempt
        record.detail = detail
        return record

    def _finish_running(
        self, record: ExecutionRecord, succeed: bool, now: float
    ) -> ExecutionRecord:
        """RUNNING -> (SUCCEEDED|FAILED)，失败后按记录版本走不同重试口径。"""
        policy = self._policies[record.policy_id]

        if succeed:
            record = self._to_terminal(
                record, ExecutionStatus.SUCCEEDED, record.attempt, "执行成功", now
            )
            self._store.release_fingerprint(
                record.fingerprint, now, policy.cooldown_seconds
            )
            return record

        record = self._transition(
            record, ExecutionStatus.FAILED, record.attempt, "执行失败", now
        )

        if record.record_version >= RECORD_VERSION_V2:
            return self._schedule_or_abandon_v2(record, policy, now)
        return self._retry_or_abandon_v1(record, policy, now)

    def _retry_or_abandon_v1(
        self, record: ExecutionRecord, policy: RemediationPolicy, now: float
    ) -> ExecutionRecord:
        """v1 历史口径：失败后立即回到 PENDING（无排期），或放弃。"""
        decision = decide_retry(record.attempt, policy.max_retries, self._base_backoff)
        self._store.audit("RETRY_DECISION", record.execution_id, decision.reason, now)
        if decision.should_retry:
            record = self._transition(
                record, ExecutionStatus.PENDING, record.attempt, decision.reason, now
            )
        else:
            record = self._to_terminal(
                record, ExecutionStatus.ABANDONED, record.attempt, decision.reason, now
            )
            self._store.release_fingerprint(
                record.fingerprint, now, policy.cooldown_seconds
            )
        return record

    def _schedule_or_abandon_v2(
        self, record: ExecutionRecord, policy: RemediationPolicy, now: float
    ) -> ExecutionRecord:
        """v2 口径：失败后进入定时等待 WAITING_RETRY，或放弃。"""
        schedule = schedule_next_attempt(
            record.attempt, policy.max_retries, self._base_backoff, now
        )
        self._store.audit("RETRY_DECISION", record.execution_id, schedule.reason, now)

        if not schedule.should_retry:
            record = self._to_terminal(
                record, ExecutionStatus.ABANDONED, record.attempt, schedule.reason, now
            )
            self._store.release_fingerprint(
                record.fingerprint, now, policy.cooldown_seconds
            )
            return record

        validate_transition(record.status, ExecutionStatus.WAITING_RETRY)
        ok = self._store.compare_and_set_status(
            record.execution_id,
            (ExecutionStatus.FAILED,),
            ExecutionStatus.WAITING_RETRY,
            record.attempt,
            schedule.reason,
            now,
            next_run_at=schedule.next_run_at,
            remaining_attempts=schedule.remaining_attempts,
        )
        if not ok:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {record.execution_id} 状态被并发修改，写入重试排期失败",
            )
        self._store.audit(
            "TRANSITION",
            record.execution_id,
            f"FAILED -> WAITING_RETRY: {schedule.reason}",
            now,
        )
        record.status = ExecutionStatus.WAITING_RETRY
        record.detail = schedule.reason
        record.next_run_at = schedule.next_run_at
        record.remaining_attempts = schedule.remaining_attempts
        return record

    def run_execution(self, execution_id: str, succeed: bool, now: float) -> ExecutionRecord:
        """推进一次执行。

        - PENDING：PENDING->RUNNING->(SUCCEEDED|FAILED)；
        - WAITING_RETRY（v2）：未到期/已耗尽被明确拒绝，到期后原子接管为 RUNNING
          再执行；并发接管只有一方成功。
        """
        # 先抢执行槽再读状态：并发推进的输家在此即得到 ILLEGAL_TRANSITION，
        # 不会读到自己的旧快照而把"已被并发完成"误判为终态。
        if not self._try_acquire_slot(execution_id):
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {execution_id} 正被另一执行流接管，并发推进被拒绝",
            )
        try:
            record = self._store.get_execution(execution_id)
            if record is None:
                raise KeyError(f"执行记录 {execution_id} 不存在")
            self._assert_runnable(record, now)

            if record.status is ExecutionStatus.WAITING_RETRY:
                record = self._claim_waiting_to_running(record, now, "单条推进接管到期重试")
            else:  # PENDING：原子接管，并发抢占失败为 ILLEGAL_TRANSITION
                record = self._claim_pending_to_running(record, now)
            return self._finish_running(record, succeed, now)
        finally:
            self._release_slot(execution_id)

    # ---- 到期重试的批量消费 ----
    def list_due_retries(self, now: float) -> list[ExecutionRecord]:
        """只读列出当前所有已到期、等待重试的记录，供后台任务/操作者挑选。"""
        return self._store.list_due_waiting(now)

    def claim_due_retries(
        self, now: float, consumer: str = "background", limit: int | None = None
    ) -> list[ExecutionRecord]:
        """原子批量接管到期重试（WAITING_RETRY -> RUNNING）。

        多路并发消费时，同一条记录同一时刻只有一个消费方接管成功；
        重复调用不会取到同一条，尝试次数随接管各递增一次、不漂移。
        接管后由消费方调用 complete_execution 回报结果。
        """
        claimed = self._store.claim_due_waiting(
            now, detail=f"消费方 {consumer} 接管到期重试", limit=limit
        )
        for rec in claimed:
            self._store.audit(
                "CLAIM_RETRY",
                rec.execution_id,
                f"消费方 {consumer} 批量接管到期重试，第 {rec.attempt} 次尝试，"
                f"接管后剩余 {rec.remaining_attempts} 次",
                now,
            )
        return claimed

    def complete_execution(
        self, execution_id: str, succeed: bool, now: float
    ) -> ExecutionRecord:
        """回报一条已被接管（RUNNING）记录的执行结果；非 RUNNING 不允许回报。"""
        record = self._store.get_execution(execution_id)
        if record is None:
            raise KeyError(f"执行记录 {execution_id} 不存在")
        if record.status is not ExecutionStatus.RUNNING:
            raise TransitionError(
                TransitionRejection.ILLEGAL_TRANSITION,
                f"执行 {execution_id} 当前状态 {record.status.value}，"
                "只有 RUNNING 记录可回报结果",
            )
        return self._finish_running(record, succeed, now)

    # ---- 重启恢复 ----
    def recover_pending(self, now: float) -> list[ExecutionRecord]:
        """重启恢复：把重启前已开始（RUNNING）但无结果的记录收尾为 ABANDONED。

        确定口径：
        - RUNNING -> ABANDONED，依据 RECOVERY_INTERRUPTED 写入审计；
        - 不新增任何尝试计数（attempt 原样保留），因此不会重复计数/重复执行；
        - WAITING_RETRY 记录原样保留：最早可执行时刻与剩余次数不被提前、推迟或清零；
        - 幂等：重复调用时这些记录已非 RUNNING，不会再次收尾。
        """
        recovered: list[ExecutionRecord] = []
        for record in self._store.list_by_status((ExecutionStatus.RUNNING,)):
            basis = (
                f"RECOVERY_INTERRUPTED: 重启时该记录处于 RUNNING 但无结果，"
                f"按确定恢复口径收尾为 ABANDONED；attempt 保持 {record.attempt}，不重复计数"
            )
            ok = self._store.compare_and_set_status(
                record.execution_id,
                (ExecutionStatus.RUNNING,),
                ExecutionStatus.ABANDONED,
                record.attempt,
                basis,
                now,
                next_run_at=0.0,
                remaining_attempts=0,
            )
            if not ok:
                # 并发下已被其它恢复/推进处理，跳过，绝不重复收尾
                continue
            self._store.audit("RECOVERY_ABANDONED", record.execution_id, basis, now)
            policy = self._policies.get(record.policy_id)
            cooldown = policy.cooldown_seconds if policy else 0.0
            self._store.release_fingerprint(record.fingerprint, now, cooldown)
            record.status = ExecutionStatus.ABANDONED
            record.detail = basis
            record.next_run_at = 0.0
            record.remaining_attempts = 0
            recovered.append(record)
        return recovered

    # ---- 查询 ----
    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        return self._store.get_execution(execution_id)
