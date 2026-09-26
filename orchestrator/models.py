"""核心数据模型与枚举定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


SCHEMA_VERSION = 1

# ---- 执行记录版本（记录形态的前向/后向兼容） ----
RECORD_VERSION_V1 = 1          # 历史形态：失败后立刻回到 PENDING，无排期字段
RECORD_VERSION_V2 = 2          # 新形态：失败后进入 WAITING_RETRY，带排期时刻与剩余次数
CURRENT_RECORD_VERSION = RECORD_VERSION_V2
SUPPORTED_RECORD_VERSIONS: frozenset[int] = frozenset(
    {RECORD_VERSION_V1, RECORD_VERSION_V2}
)


class ExecutionStatus(str, enum.Enum):
    """执行记录状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"
    SUPPRESSED = "SUPPRESSED"
    # v2 新增：失败后的定时等待重试阶段，最早可执行时刻由 next_run_at 决定
    WAITING_RETRY = "WAITING_RETRY"


class TransitionRejection(str, enum.Enum):
    """非法状态流转的可区分原因。"""

    TERMINAL_STATE = "TERMINAL_STATE"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    # v2 新增：推进 WAITING_RETRY 记录时的三类可区分拒绝
    RETRY_NOT_DUE = "RETRY_NOT_DUE"                  # 还没到最早可执行时刻
    RETRY_ALREADY_EXHAUSTED = "RETRY_ALREADY_EXHAUSTED"  # 剩余尝试次数为 0，不能再试


class DedupDecision(str, enum.Enum):
    """告警去重判定结论。"""

    ACCEPTED_NEW = "ACCEPTED_NEW"
    DUPLICATE_ACTIVE = "DUPLICATE_ACTIVE"
    DUPLICATE_COOLDOWN = "DUPLICATE_COOLDOWN"


class SuppressionReason(str, enum.Enum):
    """抑制原因（可区分、可追溯）。"""

    MAINTENANCE_WINDOW = "MAINTENANCE_WINDOW"
    MANUAL_SUPPRESSION = "MANUAL_SUPPRESSION"


class AuthzRejection(str, enum.Enum):
    """权限校验拒绝原因。"""

    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN_ROLE = "FORBIDDEN_ROLE"


@dataclass(frozen=True)
class AlertEvent:
    """告警事件。"""

    alert_id: str
    fingerprint: str
    resource: str
    severity: str
    labels: dict[str, str] = field(default_factory=dict)
    occurred_at: float = 0.0


@dataclass(frozen=True)
class RemediationPolicy:
    """修复策略。"""

    policy_id: str
    version: int
    priority: int
    match_labels: dict[str, str] = field(default_factory=dict)
    action: str = ""
    max_retries: int = 3
    cooldown_seconds: float = 300.0


@dataclass
class ExecutionRecord:
    """执行记录。

    record_version 决定记录形态：
    - v1：历史形态，失败后立即回到 PENDING，不携带排期字段；
    - v2：失败后进入 WAITING_RETRY，携带 next_run_at（最早可执行时刻）与
      remaining_attempts（剩余尝试次数）。
    """

    execution_id: str
    fingerprint: str
    policy_id: str
    policy_version: int
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    detail: str = ""
    record_version: int = CURRENT_RECORD_VERSION
    next_run_at: float = 0.0
    remaining_attempts: int = 0
