# Remediation Orchestrator

告警驱动的自动化修复编排服务。使用本地事件与任务记录（SQLite）模拟告警、
策略与执行结果，保证重复告警、并发执行与执行失败时的处置结论**可预期且可审计**。

## 架构与编排流程

```
orchestrator/
  models.py        # 数据模型与枚举（状态、去重结论、抑制/拒绝原因、记录/策略版本）
  store.py         # SQLite 持久化：指纹占用、执行记录、审计日志、幂等迁移（重启不丢状态）
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
PENDING       -> RUNNING | ABANDONED | SUPPRESSED
RUNNING       -> SUCCEEDED | FAILED | ABANDONED
FAILED        -> WAITING_RETRY（v2 定时排期）| PENDING（v1 旧语义）| ABANDONED（重试耗尽）
WAITING_RETRY -> RUNNING（到期后）| ABANDONED
SUCCEEDED / ABANDONED / SUPPRESSED 为终态
```

非法流转被拒绝并给出可区分原因（`TransitionError.reason`）：

- `TERMINAL_STATE`：当前状态已是终态；
- `ILLEGAL_TRANSITION`：目标状态不在允许集合内，或状态被并发修改（CAS 失败）；
- `RETRY_NOT_DUE`：记录处于等待重试阶段，还没到最早可执行时刻；
- `RETRY_EXHAUSTED`：等待重试记录的剩余尝试次数已耗尽，不能再试。

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

## 重试与退避（定时排期）

- 上限由策略 `max_retries` 配置（总尝试次数 = `max_retries + 1`）；第 `attempt`
  次失败后退避 `base_backoff * 2^(attempt-1)` 秒；
- **失败后不会立刻重新变成可执行**：v2 记录进入 `WAITING_RETRY` 阶段，并持久化
  `next_attempt_at`（最早可执行时刻）与 `remaining_attempts`（剩余尝试次数）；
- 在该时刻到来之前，任何推进请求都会被挡下，且拒绝原因可区分：
  `RETRY_NOT_DUE`（还没到时间）/ `RETRY_EXHAUSTED`（已经不能再试）/
  `TERMINAL_STATE`、`ILLEGAL_TRANSITION`（当前状态根本不允许推进）；
- 达到上限后进入终态 `ABANDONED`，审计日志记录 `RETRY_EXHAUSTED`；
- 重试判定为纯函数，副作用仅发生在状态 CAS 成功之后，重试期间无重复计数。

### 到期重试的批量消费

- `GET /retries/due`（或服务方法 `list_due_retries(now)`）一次列出当前所有
  已到期的等待重试记录，供后台任务或操作者批量消费；
- **批量口径与单条一致，绝不静默跳过**：到期集合中若存在版本高于当前支持的
  记录，整个请求被明确拒绝（`RecordVersionError` / HTTP 422
  `UNSUPPORTED_RECORD_VERSION`），错误信息点名全部违规记录（如
  `exec-v3(v3)`）。不会把这些记录悄悄过滤掉再返回一个"看似正常"的空列表；
  未到期的高版本记录不参与该口径（它们本来就还不该被消费）。受支持的
  v1/v2 到期记录行为完全不受影响；
- 消费方通过 `POST /retries/{id}/claim`（或 `claim_due_retry`）原子接管：
  单条 SQL 内完成状态、到期条件校验与尝试计数自增，多路并发消费同一记录时
  **只有一个消费方成功**，其余拿到 `ILLEGAL_TRANSITION` 的明确失败结论；
  反复消费不会重复取走同一条记录，尝试/成功计数不会漂移；
- 接管成功后由 `POST /executions/{id}/finish`（或 `finish_execution`）上报结果。

## 重启恢复口径

服务启动时（`main.py`）及服务方法 `recover_interrupted(now)` 执行确定恢复：

- 重启前已开始但未给出结果（`RUNNING` 悬挂）的记录，经 CAS 收尾为终态
  `ABANDONED`，审计日志写入 `RECOVERY` 事件及依据
  （`RECOVERY_INTERRUPTED: 服务重启前执行未给出结果...`）；恢复幂等，
  不增加 `attempt`，不对已计数的尝试重复计数；
- **版本高于当前支持的悬挂记录同样不会被无声跳过**：它们不能按旧语义继续
  执行，恢复时经同一个 CAS 收尾为明确终态 `ABANDONED`，并写入可区分的审计
  依据 `RECOVERY_UNSUPPORTED_VERSION`（记录其版本与当前支持上限）。因此这类
  记录既不会永远悬着，也不会被重复执行或重复计数（`attempt` 保持原值）；
  恢复后单条查询/推进/接管仍会按"版本不支持"被明确拒绝，三类结论始终分得开；
- 重启前还没到时间的 `WAITING_RETRY` 记录，其 `next_attempt_at` 与
  `remaining_attempts` **原样保留**，不会被提前安排、推迟或清零，
  到期后仍可正常被消费。

## 记录版本兼容

- 执行记录结构版本由 `RECORD_SCHEMA_VERSION = 2` 标识（与策略版本
  `SCHEMA_VERSION` 相互独立）；兼容边界明确：**低版本记录迁移后按原语义继续
  参与执行，当前版本记录的排期/到期消费/并发接管行为不受影响，只有确实高于
  当前支持版本（`record_version > 2`）的记录才走"明确拒绝"**；
- **v1（历史记录）**：无排期字段。启动时存储层幂等迁移（`ALTER TABLE` 补列，
  默认值 `record_version=1 / next_attempt_at=0 / remaining_attempts=-1`），
  已有行不被改写；v1 记录按原语义继续参与执行——失败后立即回到 `PENDING`，
  无排期门控，不会被当作数据缺失或直接失败；
- **v2（当前版本）**：失败后进入 `WAITING_RETRY` 定时排期；到期列出、
  到期消费、并发接管均为受支持行为，不因兼容检查改变任何语义；
- **高于当前版本的记录**：单条链路（查询、推进、接管、上报结果）抛出
  `RecordVersionError`（HTTP 422，`UNSUPPORTED_RECORD_VERSION`）；
  批量列到期记录时同样抛出，异常的 `unsupported` 属性携带全部违规记录
  `(execution_id, record_version)` 列表并在错误信息中点名，绝不静默跳过；
  重启恢复时悬挂的高版本 `RUNNING` 记录被收尾为明确终态 `ABANDONED` 并留下
  `RECOVERY_UNSUPPORTED_VERSION` 审计依据，不悬置、不重复计数、不重复执行；
- 三类结论在任何链路上都始终可区分：**"版本不支持"** `RecordVersionError`
  （HTTP 422）、**"记录不存在"** `KeyError`（HTTP 404）、**"状态不允许推进"**
  `TransitionError`（HTTP 409，原因取 `TERMINAL_STATE` / `ILLEGAL_TRANSITION` /
  `RETRY_NOT_DUE` / `RETRY_EXHAUSTED`）。

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
| GET  | `/executions/{id}` | 查询执行记录（含排期字段与记录版本） |
| GET  | `/retries/due` | 列出当前所有已到期的等待重试记录 |
| POST | `/retries/{id}/claim` | 原子接管一条到期重试记录（并发下仅一方成功） |
| POST | `/executions/{id}/finish` | 对已接管（RUNNING）的记录上报执行结果 |

错误映射：记录不存在 -> 404；版本不受支持 -> 422
（`UNSUPPORTED_RECORD_VERSION`）；状态/排期拒绝 -> 409
（`TERMINAL_STATE` / `ILLEGAL_TRANSITION` / `RETRY_NOT_DUE` / `RETRY_EXHAUSTED`）。

数据库路径由环境变量 `ORCHESTRATOR_DB` 指定（默认 `orchestrator.db`）。

## 验证方法

```bash
uv sync
uv run pytest tests/ -v        # 28 个测试：去重/并发/非法流转/定时重试/恢复/版本兼容等
uv run uvicorn main:app        # 启动服务（启动时自动执行重启恢复）
```

测试日志（`-v` 或 `--log-cli-level=INFO`）会打印事件、策略、状态流转与判定依据，
例如去重结论、重试决策（`RETRY_SCHEDULED` / `RETRY_EXHAUSTED`）、排期字段
（`next_attempt_at` / `remaining_attempts`）与恢复依据（`RECOVERY_INTERRUPTED`）。

### 本地复现关键行为

- **定时排期**：`POST /alerts` 受理后 `POST /executions/{id}/run`
  `{"succeed": false}`，记录进入 `WAITING_RETRY`；立即再次 run 会收到
  409 `RETRY_NOT_DUE`，退避时长（默认指数退避）到期后再 run 才放行；
- **批量消费**：`GET /retries/due` 查到期记录，`POST /retries/{id}/claim`
  接管后 `POST /executions/{id}/finish` 上报结果；并发 claim 同一记录仅一方成功；
- **重启恢复**：让一条记录停在 `RUNNING`（如 claim 后不 finish），重启进程，
  启动日志/审计中可见 `RECOVERY` 事件，该记录被收尾为 `ABANDONED`；
  未到期的 `WAITING_RETRY` 记录排期字段重启前后保持一致；
- **批量列遇到高版本不静默跳过**：直接往 `executions` 表写入一条
  `status='WAITING_RETRY'` 且 `next_attempt_at` 已到期的 `record_version=3`
  记录（可参考测试辅助函数 `insert_raw_execution`），再 `GET /retries/due`，
  收到 422 `UNSUPPORTED_RECORD_VERSION` 且响应中点名 `exec-xxx(v3)`；
  把它与一条正常的到期记录混写时，请求同样被明确拒绝；把其排期推到未来后，
  正常记录照常返回与消费；
- **重启恢复遇到高版本**：写入一条 `status='RUNNING'` 的 `record_version=3`
  悬挂记录后重新打开同一数据库并调用 `recover_interrupted`（或重启进程），
  该记录被收尾为 `ABANDONED`、`attempt` 不变，审计中出现
  `RECOVERY_UNSUPPORTED_VERSION` 依据；再调用一次恢复不产生重复审计；此后
  单条查询/推进仍得到 422 `UNSUPPORTED_RECORD_VERSION`，与 404/409 分得开；
- **版本兼容**：测试 `test_old_schema_db_migrated_and_readable` 构造 v1 时代的
  历史库验证幂等迁移与旧语义执行；
  `test_unsupported_future_version_rejected_distinctly` 验证未来版本记录在单条
  与批量口径上都被明确拒绝（422）且与 404/409 分得开；
  `test_list_due_retries_*` 覆盖批量链路的单条/混合/三类结论区分，
  `test_recovery_*` 覆盖重启恢复链路的单条/混合/幂等/计数不漂移。
