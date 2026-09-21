"""权限校验（本地模拟）。

约定：
    principal is None        -> UNAUTHENTICATED（未携带身份）
    缺少所需权限角色         -> FORBIDDEN（已认证但权限不足）
authorize 只负责抛异常，不读写任何执行记录，因此"拒绝本身不改变状态"。
"""

from __future__ import annotations

from .errors import AuthzError
from .models import EXECUTE_PERMISSION, Principal, RejectReason


def authorize(principal: Principal | None, permission: str = EXECUTE_PERMISSION) -> None:
    if principal is None:
        raise AuthzError(
            RejectReason.UNAUTHENTICATED,
            "no principal supplied; authentication required",
        )
    if permission not in principal.roles:
        raise AuthzError(
            RejectReason.FORBIDDEN,
            f"principal={principal.principal_id} lacks permission {permission!r}; "
            f"roles={sorted(principal.roles)}",
        )
