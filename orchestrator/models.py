"""核心数据模型与枚举定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


SCHEMA_VERSION = 1  # 策略版本兼容上限

# 执行记录结构版本：
# - v1：历史记录，无定时重试字段，失败后立即回到 PENDING（旧语义）；
# - v2：引入 WAITING_RETRY 排期字段（next_attempt_at / remaining_attempts）。
RECORD_SCHEMA_VERSION = 2


class RecordVersionError(Exception):
    """执行记录声明了当前不支持的版本。

    与"记录不存在"（KeyError）和"状态不允许推进"（TransitionError）
    明确区分开，绝不硬按旧语义解释。
    """

    def __init__(self, execution_id: str, record_version: int) -> None:
        super().__init__(
            f"执行记录 {execution_id} 的版本 v{record_version} 高于当前支持的 "
            f"v{RECORD_SCHEMA_VERSION}，拒绝按旧语义解释"
        )
        self.execution_id = execution_id
        self.record_version = record_version


class ExecutionStatus(str, enum.Enum):
    """执行记录状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_RETRY = "WAITING_RETRY"  # 等待重试：未到 next_attempt_at 不可推进
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"
    SUPPRESSED = "SUPPRESSED"


class TransitionRejection(str, enum.Enum):
    """非法状态流转的可区分原因。"""

    TERMINAL_STATE = "TERMINAL_STATE"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    RETRY_NOT_DUE = "RETRY_NOT_DUE"  # 等待重试但还没到最早可执行时刻
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"  # 剩余尝试次数已耗尽


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

    版本兼容：
    - record_version=1（历史记录）：无排期字段，next_attempt_at 恒为 0
      （立即可执行），remaining_attempts 为 -1（未排期，按策略实时判定）；
    - record_version=2：失败后进入 WAITING_RETRY 并携带排期字段。
    """

    execution_id: str
    fingerprint: str
    policy_id: str
    policy_version: int
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    detail: str = ""
    record_version: int = RECORD_SCHEMA_VERSION
    next_attempt_at: float = 0.0  # 最早可执行时刻（WAITING_RETRY 时生效）
    remaining_attempts: int = -1  # 剩余尝试次数；-1 表示未排期（v1 语义）
