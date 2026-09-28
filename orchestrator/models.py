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
    明确区分开，绝不硬按旧语义解释，也绝不静默跳过。

    单条链路（查询/推进/接管）携带 execution_id / record_version；
    批量链路（list_due_retries）一次返回所有不支持记录的 (id, version)，
    消费方据此明确知道有哪些记录被拒绝，而不是只拿到一个"看似正常"的列表。
    """

    def __init__(
        self,
        unsupported: tuple[tuple[str, int], ...],
        context: str = "读取",
    ) -> None:
        if not unsupported:
            raise ValueError("RecordVersionError 至少要携带一条不支持的记录")
        self.unsupported = unsupported
        self.context = context
        first_id, first_version = unsupported[0]
        self.execution_id = first_id
        self.record_version = first_version
        detail = ", ".join(f"{eid}(v{ver})" for eid, ver in unsupported)
        super().__init__(
            f"{context}时发现 {len(unsupported)} 条执行记录的版本高于当前支持的 "
            f"v{RECORD_SCHEMA_VERSION}，明确拒绝且不静默跳过：{detail}"
        )

    @classmethod
    def for_batch(
        cls, unsupported: list[tuple[str, int]], context: str = "批量列出到期重试"
    ) -> "RecordVersionError":
        """批量口径：携带全部不受支持记录（稳定排序，便于断言与审计）。"""
        return cls(tuple(sorted(unsupported)), context)


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


@dataclass(frozen=True)
class InterruptedRecord:
    """重启恢复扫描到的悬挂记录（未经版本门控的原始视图）。

    恢复链路必须自己对 record_version 给出明确处置（拒绝或落终态），
    既不能把高版本行物化成 ExecutionRecord 按旧语义解释，也不能静默跳过。
    """

    execution_id: str
    fingerprint: str
    policy_id: str
    attempt: int
    record_version: int
