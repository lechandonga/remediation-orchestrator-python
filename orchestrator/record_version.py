"""执行记录的版本形态兼容。

记录级版本兼容口径：
- v1（历史形态）：无排期字段，失败后立即回到 PENDING，仍按原语义参与执行；
- v2（当前形态）：失败后进入 WAITING_RETRY，带最早可执行时刻与剩余尝试次数；
- 读到声明了当前不支持的更高版本 -> 明确抛出 UnsupportedRecordVersionError，
  绝不硬按旧语义解释。该错误与"记录不存在"（None / KeyError）及状态拒绝
  （TransitionError）彼此可区分。
"""
from __future__ import annotations

from .models import SUPPORTED_RECORD_VERSIONS


class UnsupportedRecordVersionError(Exception):
    """记录声明的版本不在当前支持范围内（前向兼容边界）。"""

    def __init__(self, execution_id: str, version: int) -> None:
        super().__init__(
            f"执行记录 {execution_id} 的记录版本 v{version} 不受支持"
            f"（支持版本：{sorted(SUPPORTED_RECORD_VERSIONS)}），拒绝按旧语义解释"
        )
        self.execution_id = execution_id
        self.version = version


def ensure_supported_version(execution_id: str, version: int) -> None:
    """校验记录版本，越新（或非法）则明确拒绝。"""
    if version not in SUPPORTED_RECORD_VERSIONS:
        raise UnsupportedRecordVersionError(execution_id, version)
