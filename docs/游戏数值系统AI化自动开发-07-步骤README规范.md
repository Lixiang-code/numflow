# 07 — 步骤 README 规范

> 本文档配套 [03 数值模型首次构建流程](./游戏数值系统AI化自动开发-03-数值模型首次构建流程.md) 的 11 个流水线步骤，规定**每个步骤都必须有持续维护的 README 资产**。  
> 引用关系：步骤 ID 与表名映射见 [真实Agent与全流水线测试-2026-04-24.md](./真实Agent与全流水线测试-2026-04-24.md)；默认细则归属见 [02 系统与子系统默认细则](./游戏数值系统AI化自动开发-02-系统与子系统默认细则.md)。

## 1. 为什么要资产化步骤 README

- 03 文档每个步骤的设计要点是**持续演化**的（公式选择、防御常数、分配策略等），仅靠提示词带不进上下文。
- 步骤 README 既是**给用户/策划看的设计说明**，也是**给 Agent 的提示词延伸**：每次 advance 后由 Agent 回填 acceptance 实际完成情况，下一次 init/maintain 调用时被读到，避免重复推导。
- 区别于：
  - **全局 README**（`project_settings.global_readme`）：项目级唯一一份，覆盖所有玩法的核心规则。
  - **表级 README**（`_table_registry.readme`）：单表的列含义/公式说明。
  - **步骤 README**（`project_settings.step_readme.<step_id>`，本规范）：对应 11 个 pipeline step 之一，覆盖该步骤跨表的设计意图与验收。

## 2. StepSpec 字段释义

机读规格定义在 `backend/app/data/pipeline_step_specs.py`：

| 字段 | 含义 | 写作要点 |
|------|------|---------|
| `step_id` | 与 `PIPELINE_STEPS` 一致的稳定 ID | 不允许改名，前后端共享 |
| `title_zh` | 中文标题，与 `frontend/src/data/pipelineSteps.ts` 对齐 | 改名时双向同步 |
| `goal` | 本步要解决的设计问题（2~3 句） | 必含：解决什么、为下游谁服务 |
| `inputs` | 上游 step 产物 + 关键配置 | 含数据来源 + 配置项名 |
| `outputs` | 表名 / README 段落 / settings key | 与 `required_tables` 呼应 |
| `required_tables` | 落库必须存在的表（参考 04-24 表级映射） | 表名以 `num_*` / `cult_*` 前缀为准 |
| `required_columns` | 每张表的关键列 | Agent 自检与前端列校验都用 |
| `acceptance` | 可勾选的验收标准 | 每条独立、可二值判断 |
| `agent_hint` | 给 Agent 的执行提示 | **必须强调 design→review→execute 三阶段，并列出 README 必含字段** |
| `common_pitfalls` | 已知坑 | 来自历史 E2E 失败案例与 02/03 红线 |
| `upstream_steps` | 显式上游步骤列表 | 用于 review 阶段的依赖检查 |

## 3. README 维护规范

### 3.1 自动落模板

`POST /pipeline/advance` 推进成功后，若 `step_readme.<expected>` 为空，会调用 `render_spec_markdown(spec)` 把 spec 渲染为 Markdown 写入。`source` 字段标记为 `spec_template`。

### 3.2 维护责任

| 时机 | 责任方 | 必做 |
|------|--------|------|
| advance 成功后立即 | Agent | 回填 acceptance 勾选项的实际结果（`- [ ]` → `- [x]` 或追加备注），并补充本次的关键决策 |
| 用户审核 / 调整 | 用户 | 直接 `PUT /pipeline/step/<id>/readme` 覆盖；source 自动转为 `user`，后续 advance 不再覆盖 |
| 下一次 init/maintain Agent 调用 | Agent | 读取 `GET /pipeline/step/<id>/readme` 作为提示词延伸，**禁止脱离 README 自由发挥** |

### 3.3 README 必含字段

由 `agent_hint` 强约束：设计目标、关键决策、列含义、与上游表关系、未决问题/TODO、本次 acceptance 勾选结果。

## 4. API 概览

| 方法 | 路径 | 权限 | 用途 |
|------|------|------|------|
| GET | `/pipeline/specs` | read | 一次拉 11 个 spec |
| GET | `/pipeline/step/{step_id}/spec` | read | 单个 spec dict |
| GET | `/pipeline/step/{step_id}/readme` | read | 当前 README（找不到时返回 spec 模板渲染结果） |
| PUT | `/pipeline/step/{step_id}/readme` | write | 覆盖写入用户自定义 README |
| POST | `/pipeline/advance` | write | 推进步骤；错序时 `detail` 为 dict（含 `expected_step` / `expected_goal`） |

## 5. 与 Agent 提示词的关系

- `backend/app/services/agent_runner.py` 的 system prompt 仍然作为**通用**约束。
- 步骤 README 是**当前步**的特化扩展：当 init Agent 推进到某 step 时，应优先把 `step_readme.<step_id>` 作为 user 上下文塞入，再做工具调用。
- 当 `common_pitfalls` 与 system prompt 冲突时，以**更严**的一方为准（例如「禁止落地表共用模板」必须遵守）。

## 6. 与 docs/03、docs/02 的引用关系

- 03 是**流程顺序**的权威源，新增/调整步骤必须先改 03 再改 spec。
- 02 是**默认细则**的权威源，spec 中的 `acceptance` 与 `common_pitfalls` 引用了 02 的开放等级、子系统轴等约定。
- 04-24 测试报告是**表级映射**的权威源，spec 的 `required_tables` / `required_columns` 与之对齐。

## 7. 校验

```bash
cd /www/wwwroot/numflow/backend
./venv/bin/python -c "from app.data.pipeline_step_specs import list_step_specs; print(len(list_step_specs()))"
# 应输出 11
```

---

**上一/下一**：[← 06 Agent 流程与工具集](./游戏数值系统AI化自动开发-06-Agent流程与工具集.md)
