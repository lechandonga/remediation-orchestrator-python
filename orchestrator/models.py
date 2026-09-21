"""核心数据模型与枚举定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


SCHEMA_VERSION = 1


class ExecutionStatus(str, enum.Enum):
    """执行记录状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"
    SUPPRESSED = "SUPPRESSED"


class TransitionRejection(str, enum.Enum):
    """非法状态流转的可区分原因。"""

    TERMINAL_STATE = "TERMINAL_STATE"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"


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
    """执行记录。"""

    execution_id: str
    fingerprint: str
    policy_id: str
    policy_version: int
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    detail: str = ""
