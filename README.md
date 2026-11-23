# 特征工坊 · 特征工程与标注数据管理平台

面向特征注册与检索、衍生特征工程、时间窗口计算、特征回溯填充、样本分层抽取、标注任务派发、标注质量审核、特征血缘追踪与在线/离线特征服务的数据特征工程与标注管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 特征注册 | `feature_reg` | 特征定义 | 特征编号、特征名称、数据类型 |
| 衍生工程 | `derive_rule` | 衍生规则 | 规则编号、输入特征、衍生函数 |
| 时间窗口 | `time_window` | 窗口模板 | 模板编号、窗口类型、窗口宽度 |
| 特征回溯 | `backfill_job` | 回溯任务 | 任务编号、目标特征、起始日期 |
| 样本分层 | `sample_strata` | 分层策略 | 策略编号、特征范围、分层字段 |
| 标注派发 | `label_task` | 标注任务 | 任务编号、样本范围、标注类型 |
| 标注审核 | `label_review` | 审核批次 | 批次编号、标注任务、审核人 |
| 一致性计算 | `consent_calc` | 一致性报告 | 报告编号、标注任务、评估指标 |
| 特征存储 | `feature_store` | 存储配置 | 配置编号、特征列表、存储引擎 |
| 在线服务 | `online_serv` | 服务端点 | 端点编号、特征列表、QPS上限 |
| 离线导出 | `offline_exp` | 导出管道 | 管道编号、特征范围、导出格式 |
| 特征血缘 | `feature_line` | 血缘链路 | 链路编号、上游特征、下游特征 |
| 特征监控 | `feature_mon` | 监控规则 | 规则编号、目标特征、监控类型 |
| 版本管理 | `version_mgr` | 特征版本 | 版本编号、关联特征、版本标签 |
| 数据切分 | `data_split` | 切分配置 | 配置编号、数据集、切分方式 |
| 特征目录 | `feat_catalog` | 目录索引 | 索引编号、目录名称、父目录 |
| 质量规则 | `quality_rule` | 质量检查 | 规则编号、目标特征、检查类型 |
| 标注词典 | `label_dict` | 标注项 | 词典编号、标签名称、标签值 |
| 批量接入 | `batch_load` | 接入任务 | 任务编号、数据源、目标特征 |
| 特征报告 | `report_task` | 分析报表 | 报告编号、报告类型、特征范围 |
| 调度编排 | `schedule_job` | 调度计划 | 计划编号、任务类型、关联对象 |
| 治理审计 | `govern_audit` | 审计记录 | 记录编号、审计对象、审计类型 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 血缘下游重算（调度员入口）

特征血缘在通用资源路由之外提供一组专用入口（`backend/app/routers/lineage.py`，
领域逻辑在 `backend/app/services/lineage_recompute.py`）：

| 接口 | 说明 |
| --- | --- |
| `POST /api/lineage/recompute/query` | 在已确认血缘边集合上查询下游重算数据：从选中的输入特征和确认版本出发，给出需要重算的边界；已废弃边不参与检索，环路提示实际闭合的关系并停止扩展 |
| `POST /api/lineage/recompute/plans` | 提交下游重算计划；同一汇合节点只保留一份有效计划，并标明关联的不同数据版本 |
| `GET /api/lineage/recompute/plans[/{id}]` | 查看计划与派发的重算/追赶任务 |
| `POST /api/lineage/recompute/plans/{id}/activate` | 激活计划：放行重算任务，追赶任务保持可领 |
| `POST /api/lineage/recompute/plans/{id}/execute` | 执行所有已放行的重算任务；未完成的追赶任务保持可领 |
| `POST /api/lineage/recompute/tasks/{id}/claim`、`/complete` | 领取并完成追赶任务，分支版本追平并留快照 |
| `GET /api/lineage/confluence/{feature}/read` | 下游按可用状态读取汇合特征：临时发布（注明来源）/ 等待齐版 / 已交付，前一次齐版交付始终可读 |
| `GET /api/lineage/versions/{feature}` | 特征实际版本与全部快照（旧版可复查） |

调度编排激活计划（`schedule_job/激活计划`，关联对象填重算计划编号或汇合特征）会经
跨模块数据流自动派发对应的重算与追赶任务。汇合结果最终结果必须两支齐版，同一对齐
只交付一个齐版结果；临时结果不会把未完成的分支误记为完成。

验收：`make test`（`backend/tests/test_lineage_recompute.py`，端到端跑通
左支新版/右支旧版的钻石形血缘重算全流程）。

领域：数据工程
子方向：数仓与数据湖
