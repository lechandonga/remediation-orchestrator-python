"""修复策略匹配与优先级判定（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass

from .models import AlertEvent, RemediationPolicy


class PolicyError(Exception):
    """策略相关错误（版本不兼容 / 冲突）。"""


@dataclass(frozen=True)
class PolicyMatch:
    policy: RemediationPolicy | None
    reason: str  # MATCHED / NO_MATCH / CONFLICT


def validate_policy(policy: RemediationPolicy) -> None:
    """校验策略版本兼容性，不兼容则抛出 PolicyError。"""
    raise NotImplementedError


def select_policy(alert: AlertEvent, policies: list[RemediationPolicy]) -> PolicyMatch:
    """按确定优先级选出唯一策略；无匹配或冲突需明确标记。"""
    raise NotImplementedError
