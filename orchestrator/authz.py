"""权限校验（骨架）。"""
from __future__ import annotations

from .models import AuthzRejection


class AuthzError(Exception):
    """权限拒绝，携带可区分原因。"""

    def __init__(self, reason: AuthzRejection, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason


def authorize(token: str | None, required_role: str = "operator") -> str:
    """校验令牌与角色，返回主体标识；拒绝时抛出 AuthzError。"""
    raise NotImplementedError
