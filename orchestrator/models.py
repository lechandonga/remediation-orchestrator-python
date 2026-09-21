"""领域模型：告警、策略、执行记录、维护窗口、抑制规则、审计事件。

本模块只放类型定义，不含业务逻辑。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


# ---- 版本兼容 -------------------------------------------------------------
# 当前语义版本。读取旧版本记录时按 migration 解释；
# 读取高于 CURRENT_VERSION 的记录一律拒绝（不静默沿用旧语义）。
CURRENT_RECORD_VERSION = 2
SUPPORTED_RECORD_VERSIONS = (1, 2)
SCHEMA_KIND = "remediation-orchestrator-record"


class AlertStatus(str, enum.Enum):
    """告警事件的生命周期结论。"""

    RECEIVED = "received"            # 已接收，尚未编排
    DEDUPED = "deduped"              # 命中去重窗口，未产生新执行
    SUPPRESSED = "suppressed"        # 被维护窗口/抑制规则拦截
    NO_POLICY = "no_policy"          # 无任何策略匹配
    CONFLICT = "conflict"            # 多策略同优先级冲突，无法唯一选择
    DISPATCHED = "dispatched"        # 已选出唯一策略并创建执行
    REJECTED = "rejected"            # 权限等原因被拒绝（不改执行状态）


class TaskState(str, enum.Enum):
    """执行任务状态机。

    合法流转（见 state_machine.py）：
        PENDING   -> RUNNING / ABANDONED
        RUNNING   -> SUCCEEDED / FAILED
        FAILED    -> PENDING(重试, 退避后) / ABANDONED(重试耗尽)
        SUCCEEDED / ABANDONED 为终态，不接受任何转换
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABANDONED = "abandoned"


TERMINAL_STATES = frozenset({TaskState.SUCCEEDED, TaskState.ABANDONED})


class SuppressionReason(str, enum.Enum):
    MAINTENANCE_WINDOW = "maintenance_window"
    SUPPRESSION_RULE = "suppression_rule"


class RejectReason(str, enum.Enum):
    """稳定的拒绝原因码：测试与日志据此区分结论。"""

    # 去重
    DUPLICATE_ALERT = "duplicate_alert"
    # 策略
    NO_POLICY_MATCHED = "no_policy_matched"
    POLICY_PRIORITY_CONFLICT = "policy_priority_conflict"
    # 状态机
    ILLEGAL_TRANSITION = "illegal_transition"
    TASK_NOT_FOUND = "task_not_found"
    TASK_ALREADY_TERMINAL = "task_already_terminal"
    # 抑制
    IN_MAINTENANCE_WINDOW = "in_maintenance_window"
    SUPPRESSED_BY_RULE = "suppressed_by_rule"
    # 重试
    RETRY_NOT_ALLOWED = "retry_not_allowed"
    RETRY_BACKOFF_NOT_ELAPSED = "retry_backoff_not_elapsed"
    RETRY_EXHAUSTED = "retry_exhausted"
    # 并发
    CONCURRENT_MODIFICATION = "concurrent_modification"
    RESOURCE_BUSY = "resource_busy"
    # 权限
    UNAUTHENTICATED = "unauthenticated"
    FORBIDDEN = "forbidden"
    # 版本
    UNSUPPORTED_VERSION = "unsupported_version"
    INCOMPATIBLE_CHANGE = "incompatible_change"


class AuditKind(str, enum.Enum):
    ALERT_RECEIVED = "alert_received"
    ALERT_DEDUPED = "alert_deduped"
    ALERT_SUPPRESSED = "alert_suppressed"
    ALERT_NO_POLICY = "alert_no_policy"
    ALERT_CONFLICT = "alert_conflict"
    POLICY_SELECTED = "policy_selected"
    TASK_CREATED = "task_created"
    STATE_TRANSITION = "state_transition"
    ILLEGAL_TRANSITION_REJECTED = "illegal_transition_rejected"
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_DENIED = "retry_denied"
    TASK_ABANDONED = "task_abandoned"
    EXECUTION_REJECTED = "execution_rejected"


# ---- 入站事件 -------------------------------------------------------------

@dataclass(frozen=True)
class AlertEvent:
    """外部告警（入站事件，不可变）。"""

    alert_id: str
    source: str
    resource_id: str
    severity: str
    title: str
    fired_at: float
    labels: dict[str, str] = field(default_factory=dict)
    dedup_window_seconds: float = 300.0
    # 幂等键：由接收方指纹派生；同一指纹短时间重复即视为重复告警。
    fingerprint: str | None = None


@dataclass
class AlertRecord:
    """告警编排结论（持久化，重启后据此去重）。"""

    alert_id: str
    fingerprint: str
    resource_id: str
    received_at: float
    status: AlertStatus
    event: AlertEvent
    suppress_reason: SuppressionReason | None = None
    suppress_detail: str = ""
    selected_policy_id: str | None = None
    task_id: str | None = None
    reject_reason: RejectReason | None = None
    reject_detail: str = ""
    version: int = CURRENT_RECORD_VERSION


# ---- 策略 -----------------------------------------------------------------

@dataclass(frozen=True)
class Policy:
    """修复策略。

    priority 数值越大优先级越高；同优先级同时命中即冲突（CONFLICT），
    必须明确标记，禁止用"先到先得"等不确定方式选择。
    """

    policy_id: str
    version: int
    name: str
    match_labels: dict[str, str]          # 全部相等才算命中（AND 语义）
    action: str                           # 模拟执行的动作名
    max_retries: int = 2                  # 重试上限（不含首次执行）
    backoff_base_seconds: float = 1.0     # 指数退避基数
    priority: int = 100
    enabled: bool = True


# ---- 维护窗口 / 抑制规则 ---------------------------------------------------

@dataclass(frozen=True)
class MaintenanceWindow:
    window_id: str
    resource_id: str                       # 精确资源；"*" 表示全局
    start_ts: float
    end_ts: float
    reason: str = ""


@dataclass(frozen=True)
class SuppressionRule:
    rule_id: str
    match_labels: dict[str, str]
    reason: str = ""


# ---- 执行记录 -------------------------------------------------------------

@dataclass
class ExecutionRecord:
    """修复执行任务（持久化 + 版本化）。"""

    task_id: str
    alert_id: str
    fingerprint: str
    resource_id: str
    policy_id: str
    policy_version: int
    state: TaskState
    created_at: float
    updated_at: float
    attempts: int = 0                      # 实际副作用执行次数（严格单调）
    max_retries: int = 2
    backoff_base_seconds: float = 1.0
    not_before_ts: float = 0.0             # 退避：最早允许下一次执行的时间
    last_error: str = ""
    revision: int = 1                      # 乐观锁版本，每次合法修改 +1
    version: int = CURRENT_RECORD_VERSION  # 记录语义版本


# ---- 审计事件 -------------------------------------------------------------

@dataclass
class AuditEvent:
    seq: int
    ts: float
    kind: AuditKind
    summary: str
    detail: dict[str, Any] = field(default_factory=dict)


# ---- 权限模型（本地模拟） --------------------------------------------------

@dataclass(frozen=True)
class Principal:
    """请求主体。roles 包含 'remediation:execute' 才允许执行修复。"""

    principal_id: str
    roles: frozenset[str] = frozenset()


EXECUTE_PERMISSION = "remediation:execute"
