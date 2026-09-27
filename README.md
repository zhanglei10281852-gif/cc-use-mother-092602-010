# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时提供面向地面运营团队的事件中心，把发射、入轨、热降额、辐射告警、任务失败和人工处置串成一条可检索时间线，并保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。
- 地面运营事件中心：多来源事件接入、去重、游标时间线、关联对象与事件、确认/升级/解决/关闭、更正删除留痕、批量归档和按卫星/任务聚合的运行摘要。

## 地面运营事件中心

事件中心接口统一使用 `/api/events` 前缀，所有接口都需要 Bearer 会话令牌，并按 `events.read`、`events.write`、`events.lifecycle`、`events.manage` 四类权限控制；初始化时管理员角色自动获得这些权限。

- 事件接入 `POST /api/events`：内置类型 `launch`、`orbit_insertion`、`thermal_derating`、`radiation_alert`、`mission_failure`、`manual_action`，可通过 `POST /api/events/types` 受控扩展新类型及其扩展字段规则（类型、必填、范围、枚举）。写入支持两种去重：显式 `idempotency_key`（同来源同键重放返回同一事件，内容不同返回 409），以及无键时的 `dedup_window_seconds` 时间窗加载荷指纹折叠。
- 时间线 `GET /api/events`：以单调递增的接入序号 `seq` 为游标的键集分页（`cursor` + `limit` + `forward/backward`），可按类型、卫星、任务、状态、严重度、来源和发生时间过滤。游标是无状态不透明令牌，只承载位置和查询指纹，因此翻页期间新事件写入不会重复或漏项，服务重启后旧游标仍可继续使用；查询条件变化时游标会失效。
- 关联：事件可携带多个关联对象（如火箭、载荷、地面站），通过 `GET /api/events/objects/{type}/{key}` 反查对象时间线；事件之间通过 `POST /api/events/{seq}/links` 建立有向关系（related/caused_by/duplicates/succeeds/blocks），从任一端都能看到入向/出向关联。
- 生命周期：`acknowledge` → `escalate`（带升级级别）→ `resolve` → `close`，非法迁移返回 409；通告类事件允许直接关闭。
- 更正与删除：`PATCH /api/events/{seq}` 更正内容，`DELETE` 删除事件，均在变更日志中记录操作者、原因和前后值；删除是保留墓碑的硬删除，事件本身不可再查，但 `GET /api/events/{seq}/changes` 凭序号仍可追溯删除记录。
- 批量归档 `POST /api/events/archive`：只允许归档 resolved/closed 等终态事件，支持批次键幂等重放，归档动作逐条留痕。
- 运行摘要：`GET /api/events/summary` 总览，`/summary/satellites/{code}` 按卫星聚合状态、类型与当前告警，`/summary/missions/{code}` 按任务聚合严重度和完整时间线。


## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。地面运营事件统一使用 `/api/events` 前缀，事件接入后可在 `/api/events` 按游标追溯，按卫星和任务的摘要位于 `/api/events/summary/...`。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复；事件中心测试覆盖权限校验、幂等写入与冲突、时间窗去重、受控扩展字段、游标分页稳定性与重启恢复、生命周期非法迁移、更正删除留痕、有向关联、关联对象、批量归档幂等和卫星/任务摘要，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
python -m app.cli events-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路；`events-demo` 会引导管理员并写入发射、入轨和辐射告警三条事件，验证鉴权、时间线和卫星摘要，重复执行时通过幂等键重放。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  events/          地面运营事件中心：接入去重、游标时间线、关联、流转、留痕、归档与摘要
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营、事件中心和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。事件中心的所有写入同样在 `BEGIN IMMEDIATE` 事务内完成：事件序号 `seq` 在事务内单调分配，作为游标分页的唯一稳定排序键；幂等键在数据库层有唯一约束，历史事件允许先入库（时间线按接入序号排列，发生时刻通过 `occurred_at` 过滤）；更正、删除、状态流转、关联和归档都追加不可改的变更日志，删除事件后日志凭 `seq` 保留。
