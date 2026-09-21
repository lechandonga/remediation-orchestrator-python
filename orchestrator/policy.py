"""修复策略匹配与优先级判定。

规则（确定性）：
1. 策略的 match_labels 必须全部出现在告警 labels 中才视为命中；
2. 多个策略命中时，priority 数值小者优先；
3. 若最高优先级存在并列且 policy_id 不同，则判定为冲突（CONFLICT），
   不明确选择，交由上层标记处理，绝不静默丢弃；
4. 策略版本高于当前 SCHEMA_VERSION 时拒绝加载（不静默沿用旧语义）。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import SCHEMA_VERSION, AlertEvent, RemediationPolicy


class PolicyError(Exception):
    """策略相关错误（版本不兼容 / 冲突）。"""


@dataclass(frozen=True)
class PolicyMatch:
    policy: RemediationPolicy | None
    reason: str  # MATCHED / NO_MATCH / CONFLICT
    candidates: tuple[str, ...] = ()


def validate_policy(policy: RemediationPolicy) -> None:
    """校验策略版本兼容性，不兼容则抛出 PolicyError（明确拒绝）。"""
    if policy.version > SCHEMA_VERSION:
        raise PolicyError(
            f"策略 {policy.policy_id} 版本 v{policy.version} 高于当前支持的 "
            f"v{SCHEMA_VERSION}，拒绝加载"
        )
    if policy.version < 1:
        raise PolicyError(f"策略 {policy.policy_id} 版本 v{policy.version} 非法")
    if policy.max_retries < 0:
        raise PolicyError(f"策略 {policy.policy_id} 的 max_retries 不能为负")


def _matches(alert: AlertEvent, policy: RemediationPolicy) -> bool:
    return all(alert.labels.get(k) == v for k, v in policy.match_labels.items())


def select_policy(alert: AlertEvent, policies: list[RemediationPolicy]) -> PolicyMatch:
    """按确定优先级选出唯一策略；无匹配或冲突需明确标记。"""
    hits = [p for p in policies if _matches(alert, p)]
    if not hits:
        return PolicyMatch(None, "NO_MATCH")
    best_priority = min(p.priority for p in hits)
    best = sorted(
        (p for p in hits if p.priority == best_priority), key=lambda p: p.policy_id
    )
    if len(best) > 1:
        return PolicyMatch(
            None,
            "CONFLICT",
            tuple(p.policy_id for p in best),
        )
    return PolicyMatch(best[0], "MATCHED", (best[0].policy_id,))
