from fastapi import FastAPI

from orchestrator.service import OrchestratorService
from orchestrator.store import Store
from orchestrator.suppression import SuppressionRegistry

app = FastAPI(title="Remediation Orchestrator")


@app.get("/")
def read_root():
    return {"service": "remediation-orchestrator", "status": "ok"}
