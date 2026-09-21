def test_skeleton_imports() -> None:
    """骨架冒烟：所有模块可导入，核心类型存在。"""
    from orchestrator import models
    from orchestrator.service import Orchestrator
    from orchestrator.store import Store

    assert models.TaskState.PENDING.value == "pending"
    assert Orchestrator is not None
    assert Store is not None
