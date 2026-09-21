"""跨进程并发与重启持久化：fcntl + 磁盘权威表保证多进程幂等。"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.models import Policy
from orchestrator.service import Orchestrator
from orchestrator.store import Store
from tests.conftest import make_alert

_WORKER = """
import sys
sys.path.insert(0, %r)
from orchestrator.store import Store
from orchestrator.service import Orchestrator
from orchestrator.models import Policy, AlertEvent
from orchestrator.clock import SystemClock
data_dir, aid = sys.argv[1], sys.argv[2]
orc = Orchestrator(Store(data_dir),
    policies=[Policy("p",1,"n",{"k":"v"},"act",priority=1)],
    clock=SystemClock(), audit=None)
ev = AlertEvent(aid,"src","host-x","crit","same-title",1000.0,labels={"k":"v"})
r = orc.receive_alert(ev)
print(f"{r.status.value}|{r.task_id or '-'}")
"""


def test_cross_process_concurrent_dispatch_is_idempotent(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    worker = tmp_path / "worker.py"
    worker.write_text(_WORKER % str(repo_root), encoding="utf-8")

    procs = [
        subprocess.Popen(
            [sys.executable, str(worker), str(tmp_path / "data"), f"a-{i}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for i in range(8)
    ]
    outputs = []
    for p in procs:
        out, err = p.communicate(timeout=30)
        assert p.returncode == 0, err
        outputs.append(out.strip())

    dispatched = [o for o in outputs if o.startswith("dispatched")]
    deduped = [o for o in outputs if o.startswith("deduped")]
    assert len(dispatched) == 1
    assert len(deduped) == 7
    # 所有去重都指向同一个任务 id。
    winner_task = dispatched[0].split("|")[1]
    assert all(o.endswith(winner_task) for o in deduped)

    tasks_file = tmp_path / "data" / "tasks.jsonl"
    lines = [ln for ln in tasks_file.read_text().splitlines() if ln.strip()]
    assert len(lines) == 1


def test_new_process_after_dispatch_dedupes(tmp_path):
    """派发完成后，再启动一个全新进程接收重复告警，仍被判重（重启不重复执行）。"""
    repo_root = Path(__file__).resolve().parents[1]
    data = tmp_path / "data"
    worker = tmp_path / "worker.py"
    worker.write_text(_WORKER % str(repo_root), encoding="utf-8")

    p1 = subprocess.run(
        [sys.executable, str(worker), str(data), "first"],
        capture_output=True, text=True, timeout=30,
    )
    assert p1.returncode == 0 and p1.stdout.strip().startswith("dispatched")
    p2 = subprocess.run(
        [sys.executable, str(worker), str(data), "second"],
        capture_output=True, text=True, timeout=30,
    )
    assert p2.returncode == 0 and p2.stdout.strip().startswith("deduped")
