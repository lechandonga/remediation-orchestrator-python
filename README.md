# Remediation Orchestrator

告警驱动的自动化修复编排服务。使用本地事件与任务记录（SQLite）模拟告警、
策略与执行结果，保证重复告警、并发执行与执行失败时的处置结论**可预期且可审计**。

## 架构与编排流程

```
orchestrator/
  models.py        # 数据模型与枚举（状态、去重结论、抑制/拒绝原因、SCHEMA_VERSION）
  store.py         # SQLite 持久化：指纹占用、执行记录、审计日志（重启不丢状态）
  dedup.py         # 告警去重判定（指纹 + 冷却窗口）
  policy.py        # 策略匹配、优先级判定与版本兼容校验
  statemachine.py  # 执行状态机（合法流转表 + 可区分拒绝原因）
  suppression.py   # 维护窗口与手动抑制规则
  retry.py         # 重试与指数退避（纯函数，无副作用）
  authz.py         # 权限校验（令牌 + 角色等级）
  service.py       # 编排服务：串联以上各模块
main.py            # FastAPI 入口
```

告警受理顺序（确定且可审计，每一步均写审计日志）：

```
鉴权 -> 抑制检查（只读）-> 去重占用（原子）-> 策略匹配 -> 创建 PENDING 执行记录
```

任何一步被拒绝都不会改变已有执行记录的状态；拒绝原因均可区分并写入审计日志。

## 状态机规则

```
PENDING  -> RUNNING | ABANDONED | SUPPRESSED
RUNNING  -> SUCCEEDED | FAILED | ABANDONED
FAILED   -> PENDING（重试）| ABANDONED（重试耗尽）
SUCCEEDED / ABANDONED / SUPPRESSED 为终态
```

非法流转被拒绝并给出可区分原因（`TransitionError.reason`）：

- `TERMINAL_STATE`：当前状态已是终态；
- `ILLEGAL_TRANSITION`：目标状态不在允许集合内，或状态被并发修改（CAS 失败）。

状态更新通过 SQLite 的条件 UPDATE（compare-and-set）完成，并发下只有一个
调用方能推进状态，不会产生半更新状态或重复计数。

## 去重与冷却

- 以告警 `fingerprint` 为键，原子地"检查并占用"（`INSERT OR REPLACE` 于事务内）；
- 已有活跃执行 -> `DUPLICATE_ACTIVE`；冷却窗口未过期 -> `DUPLICATE_COOLDOWN`；
- 执行进入终态后释放占用并进入 `cooldown_seconds` 冷却期；
- 去重状态持久化于 SQLite，**服务重启后不会重复执行**。

## 策略匹配与优先级

1. `match_labels` 全部命中告警 labels 才视为匹配；
2. 多个策略命中时 `priority` 数值小者优先（确定性）；
3. 最高优先级并列 -> 标记 `POLICY_CONFLICT`（附候选列表），绝不静默丢弃；
4. 无命中 -> 标记 `POLICY_NO_MATCH`；
5. 策略 `version > SCHEMA_VERSION` 时加载即抛出 `PolicyError`，
   不兼容变更被明确拒绝而不是静默沿用旧语义；旧版本（v1）记录仍可解释。

## 抑制与维护窗口

- `MaintenanceWindow(window_id, resource, start, end)`：`resource` 为 `"*"` 或精确匹配；
- `suppress_fingerprint(fingerprint, rule_id)`：手动抑制；
- 抑制原因可区分：`MAINTENANCE_WINDOW` / `MANUAL_SUPPRESSION`，
  且 `source` 记录窗口 ID 或规则 ID 以便追溯；
- 被抑制的告警不创建执行记录，仅写审计日志。

## 重试与退避

- 上限由策略 `max_retries` 配置；第 `attempt` 次失败后退避
  `base_backoff * 2^(attempt-1)` 秒；
- 达到上限后进入终态 `ABANDONED`，审计日志记录 `RETRY_EXHAUSTED`；
- 重试判定为纯函数，副作用仅发生在状态 CAS 成功之后，重试期间无重复计数。

## 权限校验

令牌格式：`<subject>:<role>`（如 `alice:operator`），角色等级
`viewer < operator < admin`。拒绝原因可区分：

- `UNAUTHENTICATED`：缺少或非法令牌（HTTP 401）；
- `FORBIDDEN_ROLE`：角色等级不足（HTTP 403）。

鉴权发生在任何状态变更之前，拒绝本身不改变已记录的执行状态。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/alerts` | 受理告警（需 `Authorization: <subject>:<role>` 头） |
| POST | `/executions/{id}/run` | 推进一次执行（`{"succeed": true/false}` 模拟执行结果） |
| GET  | `/executions/{id}` | 查询执行记录 |

数据库路径由环境变量 `ORCHESTRATOR_DB` 指定（默认 `orchestrator.db`）。

## 验证方法

```bash
uv sync
uv run pytest tests/ -v        # 13 个测试：重复告警/并发/非法流转/重试耗尽等
uv run uvicorn main:app        # 启动服务
```

测试日志（`-v` 或 `--log-cli-level=INFO`）会打印事件、策略、状态流转与判定依据，
例如去重结论、重试决策（`RETRY_SCHEDULED` / `RETRY_EXHAUSTED`）与每次状态流转。
