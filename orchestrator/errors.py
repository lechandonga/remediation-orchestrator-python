"""可区分的拒绝原因异常体系。

每一类业务拒绝都映射到 errors.RejectReason 中的一个稳定码，
保证"结论一致且原因可区分、可审计"。
"""

from __future__ import annotations

from .models import RejectReason


class OrchestratorError(Exception):
    """所有编排异常的基类，携带稳定的拒绝原因码与可解释细节。"""

    reason: RejectReason = RejectReason.ILLEGAL_TRANSITION

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}" if detail else reason.value)


class DedupError(OrchestratorError):
    pass


class PolicyError(OrchestratorError):
    pass


class StateTransitionError(OrchestratorError):
    pass


class SuppressionError(OrchestratorError):
    pass


class RetryError(OrchestratorError):
    pass


class AuthzError(OrchestratorError):
    pass


class VersionError(OrchestratorError):
    pass


class ConcurrencyError(OrchestratorError):
    pass
