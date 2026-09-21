"""策略与执行记录的版本兼容。"""

from __future__ import annotations

import copy
import json

import pytest

from orchestrator.errors import VersionError
from orchestrator.models import (
    CURRENT_RECORD_VERSION,
    RejectReason,
    TaskState,
)
from orchestrator.versioning import interpret_policy, interpret_record
from orchestrator.store import _alert_envelope, _task_envelope
from orchestrator.models import AlertRecord, AlertStatus, ExecutionRecord
from tests.conftest import make_alert


def test_v1_execution_record_is_migrated_and_interpreted(policy):
    task = ExecutionRecord(
        task_id="t1", alert_id="a1", fingerprint="fp", resource_id="r1",
        policy_id="p-restart", policy_version=1, state=TaskState.FAILED,
        created_at=1.0, updated_at=2.0, attempts=1, max_retries=2,
        backoff_base_seconds=1.0,
    )
    envelope = _task_envelope(task)
    v1 = copy.deepcopy(envelope)
    v1["version"] = 1
    del v1["data"]["backoff_base_seconds"]  # v1 没有该字段

    interpreted = interpret_record(v1)
    assert isinstance(interpreted, ExecutionRecord)
    assert interpreted.version == CURRENT_RECORD_VERSION
    assert interpreted.backoff_base_seconds == 1.0  # 迁移补默认值
    assert interpreted.state is TaskState.FAILED


def test_newer_version_record_is_rejected():
    raw = {"kind": "remediation-orchestrator-record", "version": 999, "type": "execution", "data": {}}
    with pytest.raises(VersionError) as e:
        interpret_record(raw)
    assert e.value.reason is RejectReason.UNSUPPORTED_VERSION


def test_unknown_kind_is_incompatible():
    raw = {"kind": "something-else", "version": 1, "type": "execution", "data": {}}
    with pytest.raises(VersionError) as e:
        interpret_record(raw)
    assert e.value.reason is RejectReason.INCOMPATIBLE_CHANGE


def test_missing_version_is_incompatible():
    raw = {"kind": "remediation-orchestrator-record", "type": "execution", "data": {}}
    with pytest.raises(VersionError) as e:
        interpret_record(raw)
    assert e.value.reason is RejectReason.INCOMPATIBLE_CHANGE


def test_v1_policy_interpreted_with_enabled_default():
    raw = {
        "kind": "remediation-policy",
        "version": 1,
        "data": {
            "policy_id": "p1", "name": "n", "match_labels": {"k": "v"},
            "action": "act",
        },
    }
    policy = interpret_policy(raw)
    assert policy.enabled is True  # v1 语义补全
    assert policy.policy_id == "p1"


def test_future_policy_version_rejected():
    raw = {"kind": "remediation-policy", "version": 42, "data": {"policy_id": "p"}}
    with pytest.raises(VersionError) as e:
        interpret_policy(raw)
    assert e.value.reason is RejectReason.UNSUPPORTED_VERSION


def test_roundtrip_through_jsonl_preserves_state(store, policy):
    """写入 -> JSONL 落盘 -> 新 Store 重放，状态/计数/revision 完全一致。"""
    from orchestrator.models import Principal
    from orchestrator.service import Orchestrator
    from orchestrator.clock import FakeClock
    from orchestrator.audit import AuditLog
    from orchestrator.store import Store

    clock = FakeClock(100.0)
    operator = Principal("op", frozenset({"remediation:execute"}))
    orc = Orchestrator(store, policies=[policy], clock=clock, audit=AuditLog(clock=clock, echo=False))
    orc._executor.set_script("db-1", ["boom"])
    r = orc.receive_alert(make_alert("a-1", ts=100.0))
    orc.execute_task(r.task_id, operator)

    # 直接从磁盘文件读一行并解释，确认信封合法。
    line = (store.tasks_path).read_text(encoding="utf-8").strip().splitlines()[-1]
    raw = json.loads(line)
    interpreted = interpret_record(raw)
    assert interpreted.state is TaskState.FAILED
    assert interpreted.attempts == 1

    # 新 store 重放后结论一致。
    from orchestrator.store import Store
    store2 = Store(store.dir)
    replayed = store2.get_task(r.task_id)
    assert replayed.state is TaskState.FAILED
    assert replayed.attempts == 1
    assert replayed.revision == interpreted.revision
