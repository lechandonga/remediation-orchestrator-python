# 告警驱动的自动化修复编排服务（本地模拟）

一套以**本地事件与任务记录**模拟"告警 → 策略 → 执行 → 重试"的自动化修复编排流程。
目标是让**重复告警、并发执行、执行失败/重试耗尽**等场景下的处置结论**可预期、可区分、可审计**。

- 语言：Python 3.14，FastAPI（HTTP 接入），pytest（验证）
- 持久化：`data/` 下的 JSONL 追加日志 + `fcntl` 文件锁，重启后状态不丢失
- 无外部中间件依赖，单进程/多线程即可验证并发与幂等语义

---

## 1. 编排流程

```
告警入站 AlertEvent
   │
   ├─(1) 指纹去重  ── 窗口内同指纹已有 DISPATCHED 根记录 ──▶ DEDUPED（关联原任务，不再执行）
   │
   ├─(2) 抑制判定  ── 命中维护窗口 ─────────────────────▶ SUPPRESSED(maintenance_window)
   │                └─ 命中抑制规则 ─────────────────────▶ SUPPRESSED(suppression_rule)
   │
   ├─(3) 策略匹配  ── 无任何启用策略命中 ────────────────▶ NO_POLICY(no_policy_matched)
   │                └─ 最高优先级并列冲突 ───────────────▶ CONFLICT(policy_priority_conflict)
   │
   ├─(4) 资源互斥  ── 同资源已有活跃任务 ────────────────▶ REJECTED(resource_busy)
   │
   └─(5) 派发执行  ── 选定唯一策略，创建 PENDING 任务 ───▶ DISPATCHED
                         │
                         ▼
              权限校验 → 认领(RUNNING) → 执行一次副作用
                         │
              成功 ─────▶ SUCCEEDED（终态，释放资源锁）
              失败 ─────▶ FAILED（按指数退避登记 not_before）
                         ├─ 退避满足且未达上限：retry → PENDING（再执行）
                         └─ attempts == 1+max_retries：ABANDONED（终态）
```

每一种结论都会写入 `alerts.jsonl` / `tasks.jsonl`，并在审计日志中记录**事件、命中策略、
状态流转、拒绝原因码与判定依据（basis）**。

### 去重指纹（稳定且可解释）

指纹 = `fp1:` + sha256(规范化 JSON：`source, resource_id, severity, title, 排序后的 labels`)。
刻意**不包含** `alert_id` 与 `fired_at`：同一问题被反复上报（事件 id、时间戳都不同）仍得同一指纹。

- 去重窗口固定从**最初 DISPATCHED 根记录**的 `received_at` 起算；`DEDUPED` 记录不续期，
  避免"重复告警把窗口无限滑动"。
- 锚点来自持久化记录，**服务重启后重放 JSONL 仍能判重**，不会因内存丢失而重复执行。
- 被抑制/无策略/冲突/拒绝的历史不作为锚点：维护窗口结束、策略补齐后允许重新评估。

---

## 2. 状态机规则

任务状态：`PENDING / RUNNING / SUCCEEDED / FAILED / ABANDONED`

| 当前态 | 允许的目标态 | 说明 |
|---|---|---|
| `PENDING` | `RUNNING`, `ABANDONED` | 认领执行；或人工放弃 |
| `RUNNING` | `SUCCEEDED`, `FAILED` | 执行结果回写 |
| `FAILED` | `PENDING`, `ABANDONED` | `PENDING` 仅能经 `retry_task`（退避/上限闸门） |
| `SUCCEEDED` | （终态，无） | 任何转换 → `task_already_terminal` |
| `ABANDONED` | （终态，无） | 耗尽放弃；再重试稳定返回 `retry_exhausted` |

非法转换的**原因码可区分**：

- 当前为终态 → `task_already_terminal`
- `FAILED` 走通用执行/流转接口 → `retry_not_allowed`（强制走重试通道）
- 其余非法转换 → `illegal_transition`

所有拒绝都**不改动记录**（状态、attempts、revision 保持不变）。

---

## 3. 抑制与维护窗口

- 判定优先级固定：**维护窗口 > 显式抑制规则**（两者原因码不同，可追溯到具体 `window_id`/`rule_id`）。
- 维护窗口按 `resource_id` 精确匹配，`"*"` 为全局；时间区间为**闭区间** `[start, end]`。
- 抑制规则按 `labels` 的 **AND** 语义匹配。
- 命中抑制一律**不创建执行任务**。

---

## 4. 失败重试与指数退避

- `attempts` = 已实际发生的副作用次数（首次执行为 1）；`max_retries` = 首次之外的追加次数；
  总执行机会 = `1 + max_retries`。
- 失败后退避：`not_before = failed_at + backoff_base * 2**(attempts-1)`。
- 重试闸门（顺序固定，原因可区分）：
  1. 非 `FAILED` → `retry_not_allowed`
  2. `attempts >= 1+max_retries` → `retry_exhausted`，进入终态 `ABANDONED`
  3. `now < not_before` → `retry_backoff_not_elapsed`
- **重试调度本身不执行副作用、不增加 attempts**；只有"认领 RUNNING"成功才令 `attempts +1`。
- 达到上限后进入明确终态；耗尽后反复请求结论一致（`retry_exhausted`）且**零新增副作用**。

---

## 5. 并发与幂等

- 接收/派发的"判定 + 落库"整体处于 `store.transaction()`（线程 `RLock` + `fcntl` 跨进程排他锁）内。
- 执行采用**认领（claim）两阶段**：
  1. 事务内 `PENDING → RUNNING` 并 CAS 提交、`attempts+1`；
  2. 锁外执行**唯一一次**副作用；
  3. 事务内以 CAS(revision) 回写结果。

  并发后来者观察到 `RUNNING` 时只等待先到者的终态结果，不产生第二个副作用、不重复计数。
- 资源锁：同一 `resource_id` 同时只允许一个活跃修复任务，直到 `SUCCEEDED/ABANDONED` 释放；
  崩溃残留的锁在重启重放时按"任务是否终态"自动回收。
- 所有更新走 **revision 乐观锁**：过期 revision 的写入整体被拒（`concurrent_modification`），
  不存在半更新或计数漂移。

---

## 6. 权限校验

- 请求未携带身份 → `unauthenticated`（HTTP 401）
- 已认证但缺少角色 `remediation:execute` → `forbidden`（HTTP 403）
- 鉴权在任何状态读取/写入之前完成；**拒绝本身不改变任何已记录状态**。

---

## 7. 版本兼容

记录信封 `{kind, version, type, data}`，当前 `version=2`，受支持 `(1, 2)`。

- 旧版本（v1）按迁移规则补默认字段后解释（如执行记录补 `backoff_base_seconds`，策略补 `enabled`）。
- `version > 当前版本` → `unsupported_version`；未知 `kind` / 非法 version → `incompatible_change`。
  **明确拒绝，绝不静默沿用旧语义。**

---

## 8. 拒绝原因码（稳定、可区分、可审计）

| 代码 | 含义 |
|---|---|
| `duplicate_alert` | 窗口内重复告警 |
| `no_policy_matched` / `policy_priority_conflict` | 无策略 / 优先级冲突 |
| `illegal_transition` / `task_already_terminal` / `task_not_found` | 状态机相关 |
| `in_maintenance_window` / `suppressed_by_rule` | 抑制相关（记录中为 `maintenance_window`/`suppression_rule`） |
| `retry_not_allowed` / `retry_backoff_not_elapsed` / `retry_exhausted` | 重试相关 |
| `concurrent_modification` / `resource_busy` | 并发相关 |
| `unauthenticated` / `forbidden` | 权限相关 |
| `unsupported_version` / `incompatible_change` | 版本相关 |

---

## 9. 模块结构

```
orchestrator/
  models.py         领域类型（告警/策略/任务/窗口/抑制/审计/原因码枚举）
  errors.py         可区分原因码异常体系
  fingerprint.py    稳定去重指纹
  dedup.py          基于持久化根记录的去重判定
  policies.py       匹配 + 确定性优先级 + 冲突检测 + 策略注册表
  state_machine.py  合法转换表与转换应用
  suppression.py    维护窗口 / 抑制规则
  retry.py          退避计算与重试资格闸门
  authz.py          权限校验
  versioning.py     信封校验、v1→v2 迁移、记录/策略解释
  store.py          JSONL 持久化 + fcntl 锁 + revision CAS + 资源锁
  executor.py       本地模拟执行器（脚本化成败、副作用计数）
  audit.py          结构化审计日志（控制台打印 + 内存留痕）
  clock.py          时间源（SystemClock / 测试用 FakeClock）
  service.py        编排门面（串联全流程、认领两阶段执行）
  api.py            FastAPI HTTP 接入
main.py             应用入口
tests/              60 个用例（去重/并发/状态机/重试为核心）
```

---

## 10. 验证方法

### 运行全部测试（含审计日志打印）

```bash
uv run pytest -q                      # 简洁输出
uv run pytest -s                      # 打印每条 [AUDIT] 事件/策略/流转/判定依据
uv run pytest -k "dedup or concurrency or state_machine or retry"
```

核心测试文件：

- `tests/test_dedup.py`：重复告警只派发一次、窗口语义、**重启后不重复执行**
- `tests/test_concurrency.py`：同告警/同资源并发、同任务并发执行、并发重试、CAS 陈旧写入
- `tests/test_state_machine.py`：全部合法/非法转换的参数化覆盖与原因码区分
- `tests/test_retry.py`：指数退避、重试耗尽进入 `ABANDONED`、无重复副作用/计数漂移
- 另有 `test_suppression.py / test_policies.py / test_authz.py / test_versioning.py / test_api.py / test_audit.py`

### 启动 HTTP 服务并手工验证

```bash
uv run fastapi dev main.py
```

```bash
# 注册策略
curl -s -X POST localhost:8000/policies -H 'Content-Type: application/json' -d '{
  "policy_id":"p-restart","version":1,"name":"restart-db",
  "match_labels":{"kind":"db"},"action":"restart",
  "max_retries":2,"backoff_base_seconds":1,"priority":100}'

# 同一问题连发两次（alert_id 不同、内容相同）-> dispatched / deduped
curl -s -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "alert_id":"a1","source":"prom","resource_id":"db-1","severity":"critical",
  "title":"disk full","fired_at":1000,"labels":{"kind":"db"}}'
curl -s -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "alert_id":"a2","source":"prom","resource_id":"db-1","severity":"critical",
  "title":"disk full","fired_at":1001,"labels":{"kind":"db"}}'

# 未认证 -> 401；带角色执行 -> 成功
curl -s -X POST localhost:8000/tasks/<task_id>/execute
curl -s -X POST localhost:8000/tasks/<task_id>/execute \
  -H 'X-Principal: op' -H 'X-Roles: remediation:execute'
```

### 审计样例（重试耗尽，max_retries=1）

```
alert_received   fingerprint=fp1:5f19... basis=sha256(source,resource,severity,title,sorted-labels)
policy_selected  policy=p priority=1 task=task-9201...
task_created     state=pending max_retries=1 backoff_base=1.0
state_transition attempt=1 result=failed not_before=1001 side_effects=1
retry_scheduled  -> pending attempts=1 basis=backoff elapsed; no side effect
state_transition attempt=2 result=abandoned side_effects=2
task_abandoned   attempts=2 max_retries=1 basis=attempts reached 1 + max_retries
```
