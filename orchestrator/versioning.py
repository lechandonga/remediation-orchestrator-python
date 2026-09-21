"""记录与策略的版本兼容：旧版本可解释，新版本明确拒绝。

兼容策略（可审计）：
- 信封必须带 kind 与 version；kind 未知            -> INCOMPATIBLE_CHANGE
- version 为受支持的旧版本：按顺序迁移补字段后解释（v1 -> v2）
- version 高于 CURRENT_RECORD_VERSION（升级前的新数据）
  -> UNSUPPORTED_VERSION，绝不静默沿用旧语义
- version <= 0 / 缺失                              -> INCOMPATIBLE_CHANGE

v1 -> v2 的语义差异：
- ExecutionRecord 新增 backoff_base_seconds（默认 1.0）
- Policy 新增 enabled（默认 True）
- AlertRecord 的抑制/拒绝细节字段缺失时补空串
"""

from __future__ import annotations

from typing import Any

from .errors import VersionError
from .models import (
    CURRENT_RECORD_VERSION,
    SUPPORTED_RECORD_VERSIONS,
    AlertEvent,
    AlertRecord,
    AlertStatus,
    ExecutionRecord,
    Policy,
    RejectReason,
    SCHEMA_KIND,
    SuppressionReason,
    TaskState,
)

RECORD_KIND = SCHEMA_KIND
POLICY_KIND = "remediation-policy"

_EXECUTION_DEFAULTS_V1: dict[str, Any] = {"backoff_base_seconds": 1.0}
_POLICY_DEFAULTS_V1: dict[str, Any] = {"enabled": True}
_ALERT_DEFAULTS_V1: dict[str, Any] = {
    "suppress_detail": "",
    "reject_reason": None,
    "reject_detail": "",
    "selected_policy_id": None,
    "task_id": None,
    "suppress_reason": None,
}


def validate_record_envelope(raw: dict[str, Any]) -> None:
    kind = raw.get("kind")
    if kind != RECORD_KIND:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"unknown record kind {kind!r}; expected {RECORD_KIND!r}",
        )
    version = raw.get("version")
    if not isinstance(version, int) or version <= 0:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"missing/invalid record version: {version!r}",
        )
    if version > CURRENT_RECORD_VERSION:
        raise VersionError(
            RejectReason.UNSUPPORTED_VERSION,
            f"record version {version} is newer than supported max "
            f"{CURRENT_RECORD_VERSION}; refusing silent reinterpretation",
        )
    if version not in SUPPORTED_RECORD_VERSIONS:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"record version {version} has no migration path; supported={SUPPORTED_RECORD_VERSIONS}",
        )


def migrate_record(raw: dict[str, Any]) -> dict[str, Any]:
    """校验信封并把内嵌 data 迁移到当前版本，返回新信封（不改输入）。"""
    validate_record_envelope(raw)
    version = raw["version"]
    data = dict(raw.get("data", {}))
    rtype = raw.get("type")

    if version == 1:
        defaults = {
            "execution": _EXECUTION_DEFAULTS_V1,
            "alert": _ALERT_DEFAULTS_V1,
        }.get(rtype, {})
        for k, v in defaults.items():
            data.setdefault(k, v)
        version = 2
    return {"kind": RECORD_KIND, "version": version, "type": rtype, "data": data}


def _hydrate_alert(data: dict[str, Any]) -> AlertRecord:
    ev = data["event"]
    event = AlertEvent(
        alert_id=ev["alert_id"],
        source=ev["source"],
        resource_id=ev["resource_id"],
        severity=ev["severity"],
        title=ev["title"],
        fired_at=ev["fired_at"],
        labels=dict(ev.get("labels", {})),
        dedup_window_seconds=ev.get("dedup_window_seconds", 300.0),
        fingerprint=ev.get("fingerprint"),
    )
    sr = data.get("suppress_reason")
    rr = data.get("reject_reason")
    return AlertRecord(
        alert_id=data["alert_id"],
        fingerprint=data["fingerprint"],
        resource_id=data["resource_id"],
        received_at=data["received_at"],
        status=AlertStatus(data["status"]),
        event=event,
        suppress_reason=SuppressionReason(sr) if sr else None,
        suppress_detail=data.get("suppress_detail", ""),
        selected_policy_id=data.get("selected_policy_id"),
        task_id=data.get("task_id"),
        reject_reason=RejectReason(rr) if rr else None,
        reject_detail=data.get("reject_detail", ""),
        version=CURRENT_RECORD_VERSION,
    )


def _hydrate_execution(data: dict[str, Any]) -> ExecutionRecord:
    return ExecutionRecord(
        task_id=data["task_id"],
        alert_id=data["alert_id"],
        fingerprint=data["fingerprint"],
        resource_id=data["resource_id"],
        policy_id=data["policy_id"],
        policy_version=data["policy_version"],
        state=TaskState(data["state"]),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        attempts=data.get("attempts", 0),
        max_retries=data.get("max_retries", 2),
        backoff_base_seconds=data.get("backoff_base_seconds", 1.0),
        not_before_ts=data.get("not_before_ts", 0.0),
        last_error=data.get("last_error", ""),
        revision=data.get("revision", 1),
        version=CURRENT_RECORD_VERSION,
    )


_HYDRATORS = {"alert": _hydrate_alert, "execution": _hydrate_execution}


def interpret_record(raw: dict[str, Any]) -> AlertRecord | ExecutionRecord:
    """解释任意受支持版本的持久化记录为当前领域对象。"""
    envelope = migrate_record(raw)
    hydrator = _HYDRATORS.get(envelope["type"])
    if hydrator is None:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"unknown record type {envelope['type']!r}",
        )
    try:
        return hydrator(envelope["data"])
    except (KeyError, ValueError) as exc:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"corrupt record after migration ({envelope['type']}): {exc}",
        ) from exc


def interpret_policy(raw: dict[str, Any]) -> Policy:
    """解释（可能是历史版本的）策略；不兼容变更抛 VersionError。"""
    if raw.get("kind") != POLICY_KIND:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"unknown policy kind {raw.get('kind')!r}; expected {POLICY_KIND!r}",
        )
    version = raw.get("version")
    if not isinstance(version, int) or version <= 0:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE, f"invalid policy version: {version!r}"
        )
    if version > CURRENT_RECORD_VERSION:
        raise VersionError(
            RejectReason.UNSUPPORTED_VERSION,
            f"policy version {version} newer than supported {CURRENT_RECORD_VERSION}",
        )
    data = dict(raw.get("data", raw))
    if version == 1:
        for k, v in _POLICY_DEFAULTS_V1.items():
            data.setdefault(k, v)
    required = ("policy_id", "name", "match_labels", "action")
    missing = [k for k in required if k not in data]
    if missing:
        raise VersionError(
            RejectReason.INCOMPATIBLE_CHANGE,
            f"policy {data.get('policy_id')!r} missing required fields: {missing}",
        )
    return Policy(
        policy_id=data["policy_id"],
        version=version,
        name=data["name"],
        match_labels=dict(data["match_labels"]),
        action=data["action"],
        max_retries=int(data.get("max_retries", 2)),
        backoff_base_seconds=float(data.get("backoff_base_seconds", 1.0)),
        priority=int(data.get("priority", 100)),
        enabled=bool(data.get("enabled", True)),
    )
