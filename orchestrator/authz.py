"""权限校验。

令牌格式（本地模拟）："<subject>:<role>"，例如 "alice:operator"。
未携带令牌 -> UNAUTHENTICATED；角色不满足 -> FORBIDDEN_ROLE。
拒绝只读不写，不会改变任何已记录的执行状态。
"""
from __future__ import annotations

from .models import AuthzRejection

# 角色等级：数值越大权限越高
_ROLE_RANK = {"viewer": 1, "operator": 2, "admin": 3}


class AuthzError(Exception):
    """权限拒绝，携带可区分原因。"""

    def __init__(self, reason: AuthzRejection, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason


def authorize(token: str | None, required_role: str = "operator") -> str:
    """校验令牌与角色，返回主体标识；拒绝时抛出 AuthzError。"""
    if not token or ":" not in token:
        raise AuthzError(AuthzRejection.UNAUTHENTICATED, "缺少或非法的访问令牌")
    subject, _, role = token.partition(":")
    have = _ROLE_RANK.get(role, 0)
    need = _ROLE_RANK.get(required_role, 99)
    if have < need:
        raise AuthzError(
            AuthzRejection.FORBIDDEN_ROLE,
            f"主体 {subject} 角色 {role} 不足以执行需要 {required_role} 的操作",
        )
    return subject
