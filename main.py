"""服务入口：装配持久化 Store、时钟、审计、执行器与 FastAPI。

运行（开发）：uv run fastapi dev main.py
数据默认写入 ./data（已在 .gitignore 之外，用于本地模拟）。
"""

from __future__ import annotations

import os

from orchestrator.api import create_app
from orchestrator.audit import AuditLog
from orchestrator.clock import SystemClock
from orchestrator.service import Orchestrator
from orchestrator.store import Store

DATA_DIR = os.environ.get("ORCH_DATA_DIR", "data")

clock = SystemClock()
store = Store(DATA_DIR)
audit = AuditLog(clock=clock, echo=True)
orchestrator = Orchestrator(store=store, clock=clock, audit=audit)
app = create_app(orchestrator)


@app.get("/")
def read_root() -> dict:
    return {
        "service": "remediation-orchestrator",
        "data_dir": DATA_DIR,
        "endpoints": [
            "POST /policies",
            "POST /alerts",
            "POST /tasks/{id}/execute",
            "POST /tasks/{id}/retry",
            "POST /tasks/{id}/transition/{state}",
            "GET  /tasks/{id}",
            "GET  /alerts/{id}",
        ],
    }
