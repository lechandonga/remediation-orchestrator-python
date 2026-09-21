"""策略匹配与确定性优先级选择。

选择规则完全确定、可解释：
1. 仅在 enabled 策略中，按 match_labels 的 AND 语义匹配；
2. priority 数值越大优先级越高；
3. 最高优先级出现并列（多个策略同分）即判定冲突，抛
   POLICY_PRIORITY_CONFLICT，绝不随机/按插入顺序静默选择；
4. 一个都没命中抛 NO_POLICY_MATCHED。
"""

from __future__ import annotations

from .errors import PolicyError
from .models import AlertEvent, Policy, RejectReason


def _matches(event: AlertEvent, policy: Policy) -> bool:
    if not policy.enabled:
        return False
    return all(event.labels.get(k) == v for k, v in policy.match_labels.items())


def match_policies(event: AlertEvent, policies: list[Policy]) -> list[Policy]:
    """返回所有命中且启用的策略，按 (priority 降序, policy_id 升序) 确定排序。"""
    hits = [p for p in policies if _matches(event, p)]
    return sorted(hits, key=lambda p: (-p.priority, p.policy_id))


def select_policy(event: AlertEvent, policies: list[Policy]) -> Policy:
    matched = match_policies(event, policies)
    if not matched:
        raise PolicyError(
            RejectReason.NO_POLICY_MATCHED,
            f"no enabled policy matches labels={event.labels} resource={event.resource_id}",
        )
    top = matched[0]
    ties = [p for p in matched if p.priority == top.priority]
    if len(ties) > 1:
        ids = sorted(p.policy_id for p in ties)
        raise PolicyError(
            RejectReason.POLICY_PRIORITY_CONFLICT,
            f"priority={top.priority} tied among policies={ids} "
            f"for labels={event.labels}; refusing to pick arbitrarily",
        )
    return top


class PolicyRegistry:
    """策略注册表：同 id 以版本高者为准；版本相同的重复定义被拒绝。"""

    def __init__(self, policies: list[Policy] | None = None) -> None:
        self._policies: dict[str, Policy] = {}
        for p in policies or []:
            self.add(p)

    def add(self, policy: Policy) -> None:
        existing = self._policies.get(policy.policy_id)
        if existing is not None:
            if existing.version == policy.version:
                raise PolicyError(
                    RejectReason.INCOMPATIBLE_CHANGE,
                    f"policy {policy.policy_id} version {policy.version} already registered",
                )
            if policy.version < existing.version:
                raise PolicyError(
                    RejectReason.INCOMPATIBLE_CHANGE,
                    f"refusing to downgrade policy {policy.policy_id} "
                    f"{existing.version} -> {policy.version}",
                )
        self._policies[policy.policy_id] = policy

    def all_policies(self) -> list[Policy]:
        return sorted(self._policies.values(), key=lambda p: (p.policy_id, p.version))

    def get(self, policy_id: str) -> Policy | None:
        return self._policies.get(policy_id)
