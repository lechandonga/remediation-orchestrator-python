"""持久化存储：JSONL 追加日志 + fcntl 文件锁 + revision 乐观并发。

设计要点：
- alerts.jsonl / tasks.jsonl 为只追加事件日志（upsert 快照），进程启动时
  顺序重放得到当前状态，因此服务重启后去重锚点、attempts、revision 全部
  保留，不会重复执行或计数漂移。
- 进程内 threading.RLock + fcntl 排他锁串行化所有写；并且**在持锁后从
  磁盘重建/读取权威注册表**（dispatch.json、locks.json），因此多个独立
  进程并发时也能看到彼此刚提交的去重锚点与资源锁，跨进程同样幂等。
- 任务更新走 revision CAS：过期 revision 整体拒绝（CONCURRENT_MODIFICATION），
  不存在半更新。
- 资源锁在重启重放时仅承认"非终态任务"的持有关系，崩溃残留自动回收。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import threading
from dataclasses import asdict
from pathlib import Path

from .errors import OrchestratorError
from .models import (
    AlertRecord,
    AlertStatus,
    RejectReason,
    ExecutionRecord,
    TaskState,
)
from .versioning import CURRENT_RECORD_VERSION, RECORD_KIND, interpret_record


def _alert_envelope(record: AlertRecord) -> dict:
    data = asdict(record)
    data["status"] = record.status.value
    data["suppress_reason"] = (
        record.suppress_reason.value if record.suppress_reason else None
    )
    data["reject_reason"] = record.reject_reason.value if record.reject_reason else None
    return {"kind": RECORD_KIND, "version": CURRENT_RECORD_VERSION, "type": "alert", "data": data}


def _task_envelope(record: ExecutionRecord) -> dict:
    data = asdict(record)
    data["state"] = record.state.value
    return {"kind": RECORD_KIND, "version": CURRENT_RECORD_VERSION, "type": "execution", "data": data}


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return default


def _atomic_write_json(path: Path, payload) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class Store:
    def __init__(self, path: str | Path) -> None:
        self.dir = Path(path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.alerts_path = self.dir / "alerts.jsonl"
        self.tasks_path = self.dir / "tasks.jsonl"
        self.locks_path = self.dir / "locks.json"
        self.dispatch_path = self.dir / "dispatch.json"
        self._lockpath = self.dir / ".lock"
        self._tlock = threading.RLock()
        self._local = threading.local()
        # 单个持久 fd + 线程深度计数：嵌套加锁可重入，避免同进程 fcntl 自锁。
        self._lockfh = open(self._lockpath, "a+")
        self._alerts: dict[str, AlertRecord] = {}
        self._tasks: dict[str, ExecutionRecord] = {}
        self._dispatch: dict[str, dict] = {}   # fingerprint -> 根派发信息（权威在磁盘）
        self._resource_locks: dict[str, str] = {}
        self._load()

    # ---- 内部：可重入的线程锁 + 跨进程文件锁 ----
    @contextlib.contextmanager
    def _locked(self):
        with self._tlock:
            depth = getattr(self._local, "depth", 0)
            if depth == 0:
                fcntl.flock(self._lockfh.fileno(), fcntl.LOCK_EX)
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
                if self._local.depth == 0:
                    fcntl.flock(self._lockfh.fileno(), fcntl.LOCK_UN)

    @contextlib.contextmanager
    def transaction(self):
        """编排层"判定-写入"原子区。动作副作用不得在该临界区内执行。"""
        with self._locked():
            yield

    def _append(self, path: Path, envelope: dict) -> None:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(envelope, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _save_dispatch(self) -> None:
        _atomic_write_json(self.dispatch_path, self._dispatch)

    def _save_locks(self) -> None:
        _atomic_write_json(self.locks_path, self._resource_locks)

    def _refresh_durable_tables(self) -> None:
        """持锁后从磁盘读取权威注册表（对跨进程并发至关重要）。"""
        self._dispatch = _read_json(self.dispatch_path, {})
        self._resource_locks = _read_json(self.locks_path, {})

    # ---- 启动重放 ----
    def _load(self) -> None:
        with self._locked():
            for path, bucket in ((self.alerts_path, "alert"), (self.tasks_path, "execution")):
                if not path.exists():
                    continue
                with open(path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        record = interpret_record(json.loads(line))
                        if bucket == "alert":
                            self._alerts[record.alert_id] = record  # type: ignore[assignment]
                        else:
                            self._tasks[record.task_id] = record  # type: ignore[assignment]
            # 重建派发锚点表：每个指纹取最初的 DISPATCHED 根记录。
            roots: dict[str, AlertRecord] = {}
            for rec in self._alerts.values():
                if rec.status is not AlertStatus.DISPATCHED:
                    continue
                cur = roots.get(rec.fingerprint)
                if cur is None or (rec.received_at, rec.alert_id) < (
                    cur.received_at, cur.alert_id
                ):
                    roots[rec.fingerprint] = rec
            self._dispatch = {
                fp: {
                    "alert_id": rec.alert_id,
                    "task_id": rec.task_id,
                    "policy_id": rec.selected_policy_id,
                    "received_at": rec.received_at,
                }
                for fp, rec in roots.items()
            }
            self._save_dispatch()
            # 资源锁仅承认非终态任务持有；崩溃残留（任务已终态）自动回收。
            raw_locks = _read_json(self.locks_path, {})
            self._resource_locks = {
                res: tid
                for res, tid in raw_locks.items()
                if (task := self._tasks.get(tid)) is not None
                and task.state not in (TaskState.SUCCEEDED, TaskState.ABANDONED)
            }
            self._save_locks()

    # ---- 告警 ----
    def put_alert(self, record: AlertRecord) -> None:
        envelope = _alert_envelope(record)
        with self._locked():
            self._refresh_durable_tables()
            self._append(self.alerts_path, envelope)
            self._alerts[record.alert_id] = record

    def get_alert(self, alert_id: str) -> AlertRecord | None:
        with self._locked():
            return self._alerts.get(alert_id)

    def find_active_fingerprint(
        self, fingerprint: str, now_ts: float, window: float
    ) -> AlertRecord | None:
        """窗口内同指纹的"派发根记录"(DISPATCHED)。

        锚点直接取自锁内读取的持久 dispatch 表，跨进程/重启均有效；
        DEDUPED 记录不续期，窗口固定从最初派发时刻起算。
        """
        with self._locked():
            self._refresh_durable_tables()
            entry = self._dispatch.get(fingerprint)
            if entry is None:
                return None
            age = now_ts - float(entry["received_at"])
            if not (0 <= age < window):
                return None
            # 去重只看时间窗口：短时间内的重复（即便首任务已成功）一律抑制；
            # 是否允许"窗口过期后的新派发"由资源锁闸门独立决定。
            return self._alerts.get(entry["alert_id"])

    def register_dispatch(
        self, fingerprint: str, alert_id: str, task_id: str, policy_id: str, ts: float
    ) -> None:
        """（内部）写入/覆盖派发锚点；权威状态在磁盘 dispatch 表。"""
        with self._locked():
            self._refresh_durable_tables()
            self._dispatch[fingerprint] = {
                "alert_id": alert_id,
                "task_id": task_id,
                "policy_id": policy_id,
                "received_at": ts,
            }
            self._save_dispatch()

    def claim_dispatch(
        self,
        fingerprint: str,
        resource_id: str,
        task_id: str,
        alert_id: str,
        policy_id: str,
        now_ts: float,
        window: float,
    ) -> tuple[bool, RejectReason | None, str]:
        """原子地完成"去重判定位 + 资源锁抢占 + 锚点登记"。

        在同一把锁内基于磁盘权威表判定，跨进程/线程并发下：
        - 窗口内已有活跃锚点            -> (False, DUPLICATE_ALERT, 说明)
        - 资源被其它活跃任务持有        -> (False, RESOURCE_BUSY, 说明)
        - 否则抢占成功并登记新锚点      -> (True, None, "")
        窗口过期或老锚点任务已终态时允许新派发（覆盖为新根记录）。
        """
        with self._locked():
            self._refresh_durable_tables()

            # 1) 活跃去重锚点：窗口内（不看任务终态——成功后的短时重复也是噪声）。
            entry = self._dispatch.get(fingerprint)
            if entry is not None:
                fresh = 0 <= now_ts - float(entry["received_at"]) < window
                if fresh:
                    return (
                        False,
                        RejectReason.DUPLICATE_ALERT,
                        f"active anchor alert={entry['alert_id']} task={entry['task_id']} "
                        f"age={now_ts - float(entry['received_at']):.3f}s window={window}s",
                    )

            # 2) 资源互斥。
            holder = self._resource_locks.get(resource_id)
            if holder is not None and holder != task_id:
                return (
                    False,
                    RejectReason.RESOURCE_BUSY,
                    f"resource {resource_id} held by active task {holder}",
                )

            # 3) 抢占：登记锚点 + 资源锁（同一磁盘状态内完成）。
            self._dispatch[fingerprint] = {
                "alert_id": alert_id,
                "task_id": task_id,
                "policy_id": policy_id,
                "received_at": now_ts,
            }
            self._save_dispatch()
            self._resource_locks[resource_id] = task_id
            self._save_locks()
            return True, None, ""

    # ---- 执行任务 ----
    def create_task(self, record: ExecutionRecord) -> None:
        envelope = _task_envelope(record)
        with self._locked():
            if record.task_id in self._tasks:
                raise OrchestratorError(
                    RejectReason.CONCURRENT_MODIFICATION,
                    f"task {record.task_id} already exists",
                )
            self._append(self.tasks_path, envelope)
            self._tasks[record.task_id] = record

    def get_task(self, task_id: str) -> ExecutionRecord | None:
        with self._locked():
            return self._tasks.get(task_id)

    def update_task(self, record: ExecutionRecord, expected_revision: int) -> None:
        """CAS 更新：revision 不匹配则在写之前整体拒绝（无半更新）。"""
        with self._locked():
            current = self._tasks.get(record.task_id)
            if current is None:
                raise OrchestratorError(
                    RejectReason.TASK_NOT_FOUND, f"task {record.task_id} not found"
                )
            if current.revision != expected_revision:
                raise OrchestratorError(
                    RejectReason.CONCURRENT_MODIFICATION,
                    f"task {record.task_id} revision {current.revision} != "
                    f"expected {expected_revision}; update refused (no partial write)",
                )
            record.revision = expected_revision + 1
            self._append(self.tasks_path, _task_envelope(record))
            self._tasks[record.task_id] = record

    # ---- 资源互斥 ----
    def lock_resource(self, resource_id: str, task_id: str) -> bool:
        with self._locked():
            self._refresh_durable_tables()
            holder = self._resource_locks.get(resource_id)
            if holder is not None and holder != task_id:
                return False
            if holder != task_id:
                self._resource_locks[resource_id] = task_id
                self._save_locks()
            return True

    def unlock_resource(self, resource_id: str, task_id: str) -> None:
        with self._locked():
            self._refresh_durable_tables()
            if self._resource_locks.get(resource_id) == task_id:
                del self._resource_locks[resource_id]
                self._save_locks()

    def dispatch_entry(self, fingerprint: str) -> dict | None:
        """只读返回某指纹当前派发锚点（锁内读取磁盘权威表）。"""
        with self._locked():
            self._refresh_durable_tables()
            entry = self._dispatch.get(fingerprint)
            return dict(entry) if entry is not None else None

    def all_tasks(self) -> list[ExecutionRecord]:
        with self._locked():
            return list(self._tasks.values())
