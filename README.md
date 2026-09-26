# Remediation Orchestrator

告警驱动的自动化修复编排服务。使用本地事件与任务记录（SQLite）模拟告警、
策略与执行结果，保证重复告警、并发执行与执行失败时的处置结论**可预期且可审计**。

## 架构与编排流程

```
orchestrator/
  models.py        # 数据模型与枚举（状态、去重结论、抑制/拒绝原因、记录版本）
  store.py         # SQLite 持久化：指纹占用、执行记录、审计日志（重启不丢状态）
  dedup.py         # 告警去重判定（指纹 + 冷却窗口）
  policy.py        # 策略匹配、优先级判定与版本兼容校验
  statemachine.py  # 执行状态机（合法流转表 + 可区分拒绝原因）
  suppression.py   # 维护窗口与手动抑制规则
  retry.py         # 重试预算、指数退避与 v2 定时排期（纯函数，无副作用）
  record_version.py# 执行记录版本兼容（v1/v2 可读边界，新版本明确拒绝）
  authz.py         # 权限校验（令牌 + 角色等级）
  service.py       # 编排服务：串联以上各模块（排期、批量消费、重启恢复）
main.py            # FastAPI 入口（生产形态写 v2，启动自动恢复）
```

告警受理顺序（确定且可审计，每一步均写审计日志）：

```
鉴权 -> 抑制检查（只读）-> 去重占用（原子）-> 策略匹配 -> 创建 PENDING 执行记录
```

任何一步被拒绝都不会改变已有执行记录的状态；拒绝原因均可区分并写入审计日志。

## 状态机规则

```
PENDING       -> RUNNING | ABANDONED | SUPPRESSED
RUNNING       -> SUCCEEDED | FAILED | ABANDONED
FAILED        -> PENDING（v1 立即重试）| WAITING_RETRY（v2 定时等待）| ABANDONED（耗尽）
WAITING_RETRY -> RUNNING（到期被接管）| ABANDONED（恢复/人工放弃）
SUCCEEDED / ABANDONED / SUPPRESSED 为终态
```

非法流转被拒绝并给出可区分原因（`TransitionError.reason`）：

- `TERMINAL_STATE`：当前状态已是终态；
- `ILLEGAL_TRANSITION`：目标状态不在允许集合内，或状态被并发修改 / 被并发方先接管（CAS 失败）；
- `RETRY_NOT_DUE`：`WAITING_RETRY` 记录尚未到最早可执行时刻；
- `RETRY_ALREADY_EXHAUSTED`：`WAITING_RETRY` 记录剩余尝试次数为 0，不能再试。

状态更新通过 SQLite 的条件 UPDATE（compare-and-set）完成，并发下只有一个
调用方能推进状态，不会产生半更新状态或重复计数。

## 定时排期重试（v2）

失败后**不会**立刻变回可执行，而是进入 `WAITING_RETRY` 等待阶段，携带：

- `next_run_at`：最早可执行时刻（epoch 秒），时刻到来前任何推进都被挡下；
- `remaining_attempts`：剩余尝试次数（不含已完成的本次）。

重试预算与退避：

- 总尝试预算 = `max_retries + 1` 次（首次执行 + `max_retries` 次重试）。
  例如 `max_retries=2` 时共 3 次尝试；
- 第 `attempt` 次失败后退避 `base_backoff * 2^(attempt-1)` 秒，
  `next_run_at = now + backoff`，由纯函数 `schedule_next_attempt` 计算，无副作用；
- 预算用完 -> 进入终态 `ABANDONED`，审计写明 `RETRY_EXHAUSTED`，并清空排期字段。

推进一条等待记录时，三类拒绝一眼可分（HTTP 也给出不同状态码）：

| 情形 | `TransitionRejection` | HTTP |
| --- | --- | --- |
| 还没到最早可执行时刻 | `RETRY_NOT_DUE` | 423 |
| 剩余尝试次数已为 0 | `RETRY_ALREADY_EXHAUSTED` | 423 |
| 当前状态不允许推进（终态 / RUNNING 等） | `TERMINAL_STATE` / `ILLEGAL_TRANSITION` | 409 |
| 记录不存在 | `KeyError` | 404 |
| 记录版本不受支持 | `UnsupportedRecordVersionError` | 422 |

到期记录的批量消费（供后台任务 / 操作者）：

- `GET /retries/due`：只读列出当前所有已到期的 `WAITING_RETRY` 记录；
- `POST /retries/claim`：**原子批量接管**到期记录（`WAITING_RETRY -> RUNNING`）。
  底层对每条做"状态仍为 WAITING_RETRY"的条件 UPDATE，多路并发消费时同一条
  只有一个消费方成功接管，其它方拿到空结果 / 明确失败；反复消费不会取走两次，
  `attempt` 随接管各递增一次，不发生计数漂移；
- `POST /executions/{id}/complete`：消费方回报已接管（RUNNING）记录的结果；
  非 RUNNING 状态回报会被 `ILLEGAL_TRANSITION` 拒绝。

此外单条 `POST /executions/{id}/run` 同样遵守排期闸门：未到期 / 已耗尽被挡，
到期则原子接管后执行。服务内还有一层"按记录的执行槽"，确保同一条记录在
同一时刻只被一个执行流推进（并发抢占失败为 `ILLEGAL_TRANSITION`）。

## 重启恢复口径

服务启动时调用 `OrchestratorService.recover_pending(now)`（FastAPI 入口在
`lifespan` 启动钩子里自动执行）：

- **重启前已开始但无结果（RUNNING）的记录**：确定性收尾为 `ABANDONED`，
  审计事件 `RECOVERY_ABANDONED`，依据写明
  `RECOVERY_INTERRUPTED ... attempt 保持 N，不重复计数`；
  同时释放指纹占用。`attempt` 原样保留，**不新增尝试计数、不重复执行**；
- **重启前未到期的 WAITING_RETRY 记录**：`next_run_at` 与 `remaining_attempts`
  原样保留，不被提前、推迟或清零；恢复后仍按原排期，未到期被挡、到期才接管；
- 终态（SUCCEEDED / ABANDONED / SUPPRESSED）不受影响；
- 恢复幂等：重复执行不会再次收尾，也不会重复写审计。

## 记录版本兼容范围

执行记录带 `record_version`，由 `orchestrator/record_version.py` 在读边界统一处理：

- **v1（历史形态）**：无排期字段，失败后立即回到 `PENDING`。旧库经
  `ALTER TABLE` 迁移补齐新列，历史行默认 `record_version=1`，仍被正确读懂并
  按原语义继续参与执行，不会被当作数据缺失或直接失败；
- **v2（当前形态）**：失败后进入 `WAITING_RETRY`，带排期时刻与剩余次数；
- **更高版本（前向边界）**：读到声明了当前不支持的版本（如 v99）立即抛出
  `UnsupportedRecordVersionError`，**绝不硬按旧语义解释**。该异常与
  "记录不存在（None / KeyError → 404）"、"状态不允许推进（TransitionError →
  409/423）"类型与 HTTP 状态码都可区分（422）。

> 说明：为保证历史行为零回归，`OrchestratorService` 的库默认 `record_version`
> 仍是 v1；生产入口 `main.py` 默认写入 v2（可用环境变量
> `ORCHESTRATOR_RECORD_VERSION` 覆盖）。无论新记录写哪一版，v1/v2 历史记录
> 都能被同一套代码读取和执行。

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

- 上限由策略 `max_retries` 配置（总预算 `max_retries + 1` 次）；第 `attempt`
  次失败后退避 `base_backoff * 2^(attempt-1)` 秒；
- v1 记录：失败后立即回到 `PENDING`（历史口径）；
- v2 记录：失败后进入 `WAITING_RETRY`，按上面"定时排期重试"规则到期再执行；
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
| POST | `/executions/{id}/run` | 推进一次执行（`{"succeed": true/false}`）；遵守排期闸门 |
| GET  | `/retries/due` | 只读列出所有已到期、等待重试的记录 |
| POST | `/retries/claim` | 原子批量接管到期重试（`{"consumer":"bg","limit":N}`） |
| POST | `/executions/{id}/complete` | 回报已接管记录的执行结果 |
| GET  | `/executions/{id}` | 查询执行记录（含 `record_version/next_run_at/remaining_attempts`） |

环境变量：

- `ORCHESTRATOR_DB`：SQLite 数据库路径（默认 `orchestrator.db`）；
- `ORCHESTRATOR_RECORD_VERSION`：新写入记录的版本（默认 `2`，设为 `1` 可回退旧口径）；
- `ORCHESTRATOR_BASE_BACKOFF`：指数退避基准秒数（默认 `1.0`）。

## 验证方法

```bash
uv sync
uv run pytest tests/ -v        # 34 个测试：原有 13 例 + 排期/并发消费/重启恢复/版本兼容 21 例
uv run uvicorn main:app        # 启动服务（启动时自动恢复悬置 RUNNING 记录）
```

测试文件与覆盖点：

- `tests/test_orchestrator.py`（13 例，**零改动**）：去重、既有并发、非法流转、
  v1 重试耗尽等——作为历史行为不回归的保障；
- `tests/test_retry_schedule.py`：失败进入等待并带 `next_run_at/remaining_attempts`；
  `RETRY_NOT_DUE` / `RETRY_ALREADY_EXHAUSTED` / `TERMINAL_STATE` /
  `ILLEGAL_TRANSITION` 与"不存在"五类结论可区分；到期前后批量列出与执行；
- `tests/test_retry_concurrency.py`：多路并发批量接管同一条只有一方成功、
  重复消费幂等不双取、单条并发推进只一次成功接管，`attempt` / 接管审计不漂移；
- `tests/test_recovery.py`：重启后悬置 RUNNING 确定性 `ABANDONED` 且审计写明依据、
  等待排期原样保留、恢复幂等不重复计数、终态不受影响；
- `tests/test_record_version.py`：v1 历史记录按原语义执行、旧库迁移默认 v1、
  不受支持新版本抛 `UnsupportedRecordVersionError` 且与不存在 / 状态拒绝可区分。

本地手工复现（文件库 + curl）：

```bash
export ORCHESTRATOR_DB=/tmp/orch.db ORCHESTRATOR_BASE_BACKOFF=60
uv run uvicorn main:app &
# 1) 受理  2) 制造一次失败 -> WAITING_RETRY，next_run_at 为约 60s 后
curl -s -X POST localhost:8000/alerts -H 'Authorization: alice:operator' \
  -H 'Content-Type: application/json' \
  -d '{"alert_id":"a1","fingerprint":"fp1","resource":"r1","severity":"high","labels":{"app":"web"}}'
curl -s -X POST localhost:8000/executions/<id>/run -d '{"succeed":false}' -H 'Content-Type: application/json'
# 3) 未到期推进 -> 423 RETRY_NOT_DUE
curl -s -X POST localhost:8000/executions/<id>/run -d '{"succeed":true}' -H 'Content-Type: application/json'
# 4) 到期后批量接管（只有一个 consumer 能取到），再回报结果
curl -s -X POST localhost:8000/retries/claim -d '{"consumer":"bg"}' -H 'Content-Type: application/json'
curl -s -X POST localhost:8000/executions/<id>/complete -d '{"succeed":true}' -H 'Content-Type: application/json'
```

重启恢复复现：在某条记录处于 `RUNNING`（已 `/retries/claim` 但尚未 `/complete`）
时杀掉服务再重启，启动日志出现 `[startup-recovery] <id> -> ABANDONED`，
查询该记录为 `ABANDONED`、`attempt` 不变；处于等待期的记录其
`next_run_at/remaining_attempts` 在重启前后完全一致。

测试日志（`-v` 或 `--log-cli-level=INFO`）会打印事件、策略、状态流转、接管
（`CLAIM_RETRY`）、恢复依据（`RECOVERY_ABANDONED`）与每次重试决策
（`RETRY_SCHEDULED` / `RETRY_EXHAUSTED`）。
