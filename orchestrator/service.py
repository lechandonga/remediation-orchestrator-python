"""编排门面：接收去重 -> 抑制 -> 策略匹配 -> 派发 -> 状态机 -> 执行 -> 重试。

并发模型（保证幂等、无半更新）：
- receive_alert 的全部"判定 + 落库"在 store.transaction() 内完成
  （线程锁 + fcntl 跨进程锁），同一告警/资源并发订阅只有一个胜出，
  其余分别得到 DEDUPED / REJECTED(resource_busy) 的确定结论。
- 动作副作用在临界区外执行；执行前快照 revision，执行后 CAS 提交，
  revision 不匹配即 CONCURRENT_MODIFICATION 整体拒绝。
- 资源锁从派发持有到任务终态，使同一资源在重试/退避期间不会被
  第二个任务重复修复。
"""

from __future__ import annotations

import uuid

from .authz import authorize
from .clock import Clock, SystemClock
from .dedup import Deduplicator
from .errors import OrchestratorError
from .executor import ActionExecutor
from .fingerprint import alert_fingerprint
from .models import (
    AlertEvent,
    AlertRecord,
    AlertStatus,
    AuditKind,
    ExecutionRecord,
    MaintenanceWindow,
    Policy,
    Principal,
    RejectReason,
    SuppressionRule,
    TaskState,
)
from .policies import PolicyRegistry, select_policy
from .retry import can_retry, schedule_backoff
from .state_machine import apply_transition, assert_can_transition
from .store import Store
from .suppression import SuppressionService

# 显式状态流转开放给调用方的目标态；FAILED->PENDING 只能走 retry_task，
# 防止绕过退避与上限检查。
_ALLOWED_EXPLICIT_TARGETS = frozenset(
    {TaskState.RUNNING, TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ABANDONED}
)
# FAILED 态不允许通过通用接口显式转入，强制走重试通道。
_EXPLICIT_DENIED_CURRENT = frozenset({TaskState.FAILED})


class Orchestrator:
    def __init__(
        self,
        store: Store,
        policies: list[Policy] | PolicyRegistry | None = None,
        windows: list[MaintenanceWindow] | None = None,
        rules: list[SuppressionRule] | None = None,
        clock: Clock | None = None,
        audit: AuditLog | None = None,
        executor: ActionExecutor | None = None,
    ) -> None:
        self._store = store
        self._registry = (
            policies if isinstance(policies, PolicyRegistry) else PolicyRegistry(policies or [])
        )
        self._suppression = SuppressionService(windows, rules)
        self._clock = clock or SystemClock()
        from .audit import AuditLog

        self._audit = audit or AuditLog(clock=self._clock)
        self._executor = executor or ActionExecutor()
        self._dedup = Deduplicator(store, self._clock)

    # ---------------------------------------------------------------- 接收
    def receive_alert(self, event: AlertEvent) -> AlertRecord:
        fingerprint = event.fingerprint or alert_fingerprint(event)
        now_ts = self._clock.now()

        self._audit.emit(
            AuditKind.ALERT_RECEIVED,
            f"alert received id={event.alert_id} resource={event.resource_id}",
            alert_id=event.alert_id,
            fingerprint=fingerprint,
            resource_id=event.resource_id,
            severity=event.severity,
            labels=event.labels,
            dedup_window_seconds=event.dedup_window_seconds,
            basis="fingerprint=sha256(source,resource,severity,title,sorted-labels)",
        )

        with self._store.transaction():
            # 1) 去重预检（权威表在锁内读取）。命中则记录 DEDUPED 并返回。
            anchor = self._dedup.is_duplicate(event, fingerprint)
            if anchor is not None:
                record = AlertRecord(
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    resource_id=event.resource_id,
                    received_at=now_ts,
                    status=AlertStatus.DEDUPED,
                    event=event,
                    task_id=anchor.task_id,
                    selected_policy_id=anchor.selected_policy_id,
                )
                self._store.put_alert(record)
                self._audit.emit(
                    AuditKind.ALERT_DEDUPED,
                    f"duplicate alert suppressed id={event.alert_id}",
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    anchor_alert_id=anchor.alert_id,
                    anchor_task_id=anchor.task_id,
                    anchor_received_at=anchor.received_at,
                    age_seconds=now_ts - anchor.received_at,
                    window_seconds=event.dedup_window_seconds,
                    basis="same fingerprint as active dispatched root within window",
                )
                return record

            # 2) 维护窗口 / 抑制规则（维护窗口优先，原因可区分）。
            hit = self._suppression.check_detail(event, now_ts)
            if hit is not None:
                reason, detail = hit
                record = AlertRecord(
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    resource_id=event.resource_id,
                    received_at=now_ts,
                    status=AlertStatus.SUPPRESSED,
                    event=event,
                    suppress_reason=reason,
                    suppress_detail=detail,
                )
                self._store.put_alert(record)
                self._audit.emit(
                    AuditKind.ALERT_SUPPRESSED,
                    f"alert suppressed id={event.alert_id} by {reason.value}",
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    suppress_reason=reason.value,
                    detail=detail,
                    basis="maintenance windows evaluated before suppression rules",
                )
                return record

            # 3) 策略匹配 + 确定性优先级。
            try:
                policy = select_policy(event, self._registry.all_policies())
            except OrchestratorError as exc:
                status = (
                    AlertStatus.NO_POLICY
                    if exc.reason == RejectReason.NO_POLICY_MATCHED
                    else AlertStatus.CONFLICT
                )
                kind = (
                    AuditKind.ALERT_NO_POLICY
                    if status == AlertStatus.NO_POLICY
                    else AuditKind.ALERT_CONFLICT
                )
                record = AlertRecord(
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    resource_id=event.resource_id,
                    received_at=now_ts,
                    status=status,
                    event=event,
                    reject_reason=exc.reason,
                    reject_detail=exc.detail,
                )
                self._store.put_alert(record)
                self._audit.emit(
                    kind,
                    f"alert {status.value} id={event.alert_id}: {exc.reason.value}",
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    reject_reason=exc.reason.value,
                    detail=exc.detail,
                    basis="AND label match; highest unique priority wins; ties are conflicts",
                )
                return record

            # 4)+5) 原子派发：锁内完成去重终判 + 资源锁抢占 + 锚点登记。
            task_id = f"task-{uuid.uuid4().hex[:12]}"
            won, deny_reason, deny_detail = self._store.claim_dispatch(
                fingerprint=fingerprint,
                resource_id=event.resource_id,
                task_id=task_id,
                alert_id=event.alert_id,
                policy_id=policy.policy_id,
                now_ts=now_ts,
                window=event.dedup_window_seconds,
            )
            if not won:
                if deny_reason is RejectReason.DUPLICATE_ALERT:
                    # 预检与抢占之间被其它线程/进程抢先：结论仍为确定的 DEDUPED。
                    entry = self._store.dispatch_entry(fingerprint) or {}
                    record = AlertRecord(
                        alert_id=event.alert_id,
                        fingerprint=fingerprint,
                        resource_id=event.resource_id,
                        received_at=now_ts,
                        status=AlertStatus.DEDUPED,
                        event=event,
                        selected_policy_id=policy.policy_id,
                        task_id=entry.get("task_id"),
                    )
                    self._store.put_alert(record)
                    self._audit.emit(
                        AuditKind.ALERT_DEDUPED,
                        f"lost dispatch race, deduped id={event.alert_id}",
                        alert_id=event.alert_id,
                        fingerprint=fingerprint,
                        anchor_task_id=entry.get("task_id"),
                        basis="atomic claim_dispatch lost to concurrent dispatcher",
                    )
                    return record

                # RESOURCE_BUSY
                record = AlertRecord(
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    resource_id=event.resource_id,
                    received_at=now_ts,
                    status=AlertStatus.REJECTED,
                    event=event,
                    selected_policy_id=policy.policy_id,
                    reject_reason=RejectReason.RESOURCE_BUSY,
                    reject_detail=deny_detail,
                )
                self._store.put_alert(record)
                self._audit.emit(
                    AuditKind.EXECUTION_REJECTED,
                    f"dispatch rejected id={event.alert_id}: resource busy",
                    alert_id=event.alert_id,
                    fingerprint=fingerprint,
                    reject_reason=RejectReason.RESOURCE_BUSY.value,
                    detail=deny_detail,
                    basis="single active remediation per resource until terminal state",
                )
                return record

            # 抢占成功：创建 PENDING 任务并落库（锚点与资源锁已在 claim 中持久化）。
            task = ExecutionRecord(
                task_id=task_id,
                alert_id=event.alert_id,
                fingerprint=fingerprint,
                resource_id=event.resource_id,
                policy_id=policy.policy_id,
                policy_version=policy.version,
                state=TaskState.PENDING,
                created_at=now_ts,
                updated_at=now_ts,
                max_retries=policy.max_retries,
                backoff_base_seconds=policy.backoff_base_seconds,
            )
            self._store.create_task(task)
            record = AlertRecord(
                alert_id=event.alert_id,
                fingerprint=fingerprint,
                resource_id=event.resource_id,
                received_at=now_ts,
                status=AlertStatus.DISPATCHED,
                event=event,
                selected_policy_id=policy.policy_id,
                task_id=task_id,
            )
            self._store.put_alert(record)

        self._audit.emit(
            AuditKind.POLICY_SELECTED,
            f"policy {policy.policy_id} selected for alert {event.alert_id}",
            alert_id=event.alert_id,
            fingerprint=fingerprint,
            policy_id=policy.policy_id,
            policy_version=policy.version,
            priority=policy.priority,
            action=policy.action,
            task_id=task_id,
            basis="unique highest-priority enabled match",
        )
        self._audit.emit(
            AuditKind.TASK_CREATED,
            f"task created {task_id} state=pending",
            task_id=task_id,
            alert_id=event.alert_id,
            resource_id=event.resource_id,
            max_retries=policy.max_retries,
            backoff_base_seconds=policy.backoff_base_seconds,
        )
        return record

    # ------------------------------------------------- 执行驱动（内部工具）
    def _finish_failure(self, snap: ExecutionRecord, ts: float, error: str) -> ExecutionRecord:
        """FAILED 落库；若重试已耗尽则同次提交内继续流转到 ABANDONED。

        attempts 不在这里增加——调用方在认领执行时已 +1。
        """
        apply_transition(snap, TaskState.FAILED, ts, error=error)
        exhausted = snap.attempts >= snap.max_retries + 1
        if exhausted:
            apply_transition(snap, TaskState.ABANDONED, ts)
            snap.not_before_ts = 0.0
        else:
            schedule_backoff(snap, ts)
        return snap

    def _await_terminal(self, task_id: str, timeout: float = 5.0) -> ExecutionRecord:
        """并发后来者观察到 RUNNING：等待先到者把任务推进到终态，

        从而多个并发调用拿到一致结论，且不产生第二个副作用。
        """
        import time

        deadline = time.monotonic() + timeout
        while True:
            rec = self._store.get_task(task_id)
            if rec is None or rec.state is not TaskState.RUNNING:
                return rec  # type: ignore[return-value]
            if time.monotonic() >= deadline:
                return rec  # type: ignore[return-value]
            time.sleep(0.001)

    def execute_task(self, task_id: str, principal: Principal | None) -> ExecutionRecord:
        """驱动一次执行，保证并发幂等：同一任务同时只执行一次副作用。

        两阶段提交：
          阶段A（事务内）：校验权限/状态/退避/资源锁，把 PENDING 认领为
                          RUNNING 并 CAS 提交、attempts+1（"认领"即占座）；
          阶段B（锁外）  ：仅认领者执行一次副作用；
          阶段C（事务内）：把结果 CAS 提交为 SUCCEEDED/FAILED(/ABANDONED)。
        并发后来者在阶段A看到 RUNNING，只等待终态结果，不重复副作用、不计数。
        """
        authorize(principal)

        # ---- 阶段 A：认领 ----
        with self._store.transaction():
            snap = self._store.get_task(task_id)
            if snap is None:
                raise OrchestratorError(RejectReason.TASK_NOT_FOUND, f"task {task_id} not found")
            # 已终态的重复执行：幂等返回同一结论，不产生副作用、不改状态。
            if snap.state in (TaskState.SUCCEEDED, TaskState.ABANDONED):
                return snap
            if snap.state == TaskState.RUNNING:
                already_running = True
                claim_revision = -1
                claimed = None
                policy_snap = None
            else:
                already_running = False
                policy_snap = self._registry.get(snap.policy_id)
                if policy_snap is None:
                    raise OrchestratorError(
                        RejectReason.INCOMPATIBLE_CHANGE,
                        f"policy {snap.policy_id} referenced by task {task_id} is unavailable",
                    )
                if snap.state == TaskState.FAILED:
                    raise OrchestratorError(
                        RejectReason.RETRY_NOT_ALLOWED,
                        f"task {task_id} is failed; call retry_task after backoff instead",
                    )
                assert_can_transition(snap, TaskState.RUNNING)
                if snap.state == TaskState.PENDING and self._clock.now() < snap.not_before_ts:
                    raise OrchestratorError(
                        RejectReason.RETRY_BACKOFF_NOT_ELAPSED,
                        f"task {task_id} not before {snap.not_before_ts} "
                        f"(now={self._clock.now()})",
                    )
                if not self._store.lock_resource(snap.resource_id, task_id):
                    raise OrchestratorError(
                        RejectReason.RESOURCE_BUSY,
                        f"resource {snap.resource_id} locked by another task",
                    )
                claimed = ExecutionRecord(**{**snap.__dict__})
                claim_revision = snap.revision
                apply_transition(claimed, TaskState.RUNNING, self._clock.now())
                claimed.attempts += 1  # 认领成功即计数：并发下全局只有一次
                self._store.update_task(claimed, claim_revision)

        # 并发后来者：等待先到者完成，返回一致的终态结论。
        if already_running:
            final = self._await_terminal(task_id)
            self._audit.emit(
                AuditKind.STATE_TRANSITION,
                f"concurrent execute joined in-progress task {task_id}",
                task_id=task_id,
                state=final.state.value if final else "missing",
                basis="observed RUNNING claim; waited for owner's result; no extra side effect",
            )
            return final  # type: ignore[return-value]

        # ---- 阶段 B：锁外执行唯一一次副作用 ----
        error = self._executor.run(claimed, policy_snap)  # type: ignore[arg-type]
        ts = self._clock.now()

        # ---- 阶段 C：提交结果 ----
        with self._store.transaction():
            if error is None:
                apply_transition(claimed, TaskState.SUCCEEDED, ts)  # type: ignore[union-attr]
                self._store.update_task(claimed, claim_revision + 1)
                self._store.unlock_resource(claimed.resource_id, task_id)  # type: ignore[union-attr]
            else:
                self._finish_failure(claimed, ts, error)  # type: ignore[arg-type]
                self._store.update_task(claimed, claim_revision + 1)
                if claimed.state == TaskState.ABANDONED:  # type: ignore[union-attr]
                    self._store.unlock_resource(claimed.resource_id, task_id)  # type: ignore[union-attr]
            final = claimed

        self._audit.emit(
            AuditKind.STATE_TRANSITION,
            f"task {task_id} executed attempt={final.attempts} state={final.state.value}",
            task_id=task_id,
            attempts=final.attempts,
            result=final.state.value,
            error=final.last_error,
            revision=final.revision,
            not_before_ts=final.not_before_ts,
            side_effects_on_resource=self._executor.side_effect_count(final.resource_id),
            basis="claim RUNNING before side effect; exactly one side effect; CAS result by revision",
        )
        if final.state == TaskState.ABANDONED:
            self._audit.emit(
                AuditKind.TASK_ABANDONED,
                f"task {task_id} abandoned after {final.attempts} attempts",
                task_id=task_id,
                attempts=final.attempts,
                max_retries=final.max_retries,
                basis="attempts reached 1 + max_retries",
            )
        return final

    # ---------------------------------------------------------------- 重试
    def retry_task(self, task_id: str, principal: Principal | None) -> ExecutionRecord:
        """FAILED -> PENDING（退避满足、未达上限时）。不执行副作用、不计数。"""
        authorize(principal)
        ts = self._clock.now()
        with self._store.transaction():
            snap = self._store.get_task(task_id)
            if snap is None:
                raise OrchestratorError(RejectReason.TASK_NOT_FOUND, f"task {task_id} not found")
            revision = snap.revision
            ok, deny = can_retry(snap, ts)
            if not ok:
                self._audit.emit(
                    AuditKind.RETRY_DENIED,
                    f"retry denied task={task_id} reason={deny.value if deny else 'n/a'}",
                    task_id=task_id,
                    reject_reason=deny.value if deny else "",
                    attempts=snap.attempts,
                    max_retries=snap.max_retries,
                    not_before_ts=snap.not_before_ts,
                    now=ts,
                    basis="retry gates: state==failed, attempts<limit, backoff elapsed",
                )
                if deny == RejectReason.RETRY_EXHAUSTED:
                    # 达上限进入明确终态（仅当尚未进入时）。
                    if snap.state != TaskState.ABANDONED:
                        apply_transition(snap, TaskState.ABANDONED, ts)
                        self._store.update_task(snap, revision)
                        self._store.unlock_resource(snap.resource_id, task_id)
                        self._audit.emit(
                            AuditKind.TASK_ABANDONED,
                            f"task {task_id} abandoned after {snap.attempts} attempts",
                            task_id=task_id,
                            attempts=snap.attempts,
                            max_retries=snap.max_retries,
                        )
                raise OrchestratorError(
                    deny,
                    f"retry refused for task {task_id}; attempts={snap.attempts} "
                    f"max_retries={snap.max_retries} not_before={snap.not_before_ts} now={ts}",
                )
            apply_transition(snap, TaskState.PENDING, ts)
            self._store.update_task(snap, revision)

        self._audit.emit(
            AuditKind.RETRY_SCHEDULED,
            f"retry scheduled task={task_id} -> pending",
            task_id=task_id,
            attempts=snap.attempts,
            not_before_ts=snap.not_before_ts,
            revision=snap.revision,
            basis="backoff elapsed and attempts below cap; no side effect performed",
        )
        return snap

    # -------------------------------------------------------- 显式状态流转
    def transition(
        self,
        task_id: str,
        target: TaskState,
        principal: Principal | None,
        error: str = "",
    ) -> ExecutionRecord:
        """显式流转：权限 -> 通道限制 -> 状态机合法性 -> CAS。"""
        authorize(principal)
        if target not in _ALLOWED_EXPLICIT_TARGETS:
            raise OrchestratorError(
                RejectReason.ILLEGAL_TRANSITION,
                f"explicit transition to {target.value} is not exposed; use retry_task for retry",
            )
        ts = self._clock.now()
        with self._store.transaction():
            snap = self._store.get_task(task_id)
            if snap is None:
                raise OrchestratorError(RejectReason.TASK_NOT_FOUND, f"task {task_id} not found")
            if snap.state in _EXPLICIT_DENIED_CURRENT:
                raise OrchestratorError(
                    RejectReason.RETRY_NOT_ALLOWED,
                    f"task {task_id} is {snap.state.value}; FAILED tasks must use retry_task",
                )
            revision = snap.revision
            apply_transition(snap, target, ts, error=error)
            if target in (TaskState.SUCCEEDED, TaskState.ABANDONED):
                self._store.unlock_resource(snap.resource_id, task_id)
            self._store.update_task(snap, revision)

        self._audit.emit(
            AuditKind.STATE_TRANSITION,
            f"explicit transition task={task_id} -> {target.value}",
            task_id=task_id,
            target=target.value,
            revision=snap.revision,
            basis="authorized explicit transition",
        )
        return snap

    # ---------------------------------------------------------------- 查询
    def get_alert(self, alert_id: str) -> AlertRecord | None:
        return self._store.get_alert(alert_id)

    def get_task(self, task_id: str) -> ExecutionRecord | None:
        return self._store.get_task(task_id)
