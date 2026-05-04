# numflow 第 4 轮优化 — UI 补齐与体验细节

> 本轮聚焦：将第 3 轮在后端落地的「表目录 / matrix 表 / calculator / 暴露参数 / 流水线 6 步」能力，在前端做到与后端能力等同的可用性与可视化。

## 背景

第 3 轮已经做完：

- 数据库迁移（`_table_registry.directory` / `_table_registry.matrix_meta_json` / `_calculators` / `_step_exposed_params`）。
- matrix 表存储/读写 API（`/meta/matrix` 与 `/meta/matrix/{table_name}`）。
- calculator 注册中心 + 自动 `<table>_lookup`。
- 表目录 `directory` 字段、`set_table_directory` 工具。
- 参数暴露 `expose_param_to_subsystems` + `list_exposed_params`；agent_runner 自动注入。
- 流水线由 11 步精简到 6 步：`environment_global_readme → base_attribute_framework → gameplay_allocation → cultivation_resource_framework → cultivation_allocation → gameplay_landing_tables`。
- 工作台已新增「按目录分组的表列表」与目录前缀的 ⇆ matrix 标记；`/meta/tables` 返回 `directory`/`is_matrix`。

第 3 轮前端只做了最小可用：tables 按 directory 分组列表。其余 UI 留到本轮补齐。

## 本轮目标（前端为主）

### 1. matrix 表可视化与编辑

- 选中 `is_matrix=true` 的表时，工作台主区从 Univer 表格切换为 **Matrix 编辑器**：
  - 行=row_axis_value，列=col_axis_value；每个等级 `level` 一张子表，提供等级 tab 切换。
  - 单元格只读视图先做完，写入路径再做（写时调用 `/agent` 的 `write_matrix_cells` 工具或新增专门 PUT 接口）。
- `display_name` 用作行/列表头；hover 提示展示 `brief`。
- 顶栏显示 `kind`（matrix_attr / matrix_resource）、`value_dtype`、`value_format`、`directory`。

### 2. 目录树与拖拽

- 现在的"按 directory 分组"是平面渲染。第 4 轮升级为左侧 **目录树**：
  - 支持折叠/展开。
  - 支持拖拽表到另一目录，落点调用新 API `PUT /meta/tables/{table_name}/directory`。
  - 新建目录通过右键菜单或 ＋ 按钮（实际只是给 directory 字段输入新值）。

### 3. calculator 浏览器

- 新增 `/meta/calculators` 列表 API（前端已可消费 backend `list_calculators`）。
- 工作台右侧新增 "Calculators" 面板：
  - 列出全部 calculator（name / kind / table / brief）。
  - 点击展开 axes，提供 "试算" 表单（前端表单根据 axes 渲染输入框，调用 `/compute/call-calculator?name=...&args=...`）。
  - 后端需要补齐 `POST /compute/call-calculator`（路由层封装，禁止直接读 _calculators）。

### 4. 暴露参数视图

- 子系统步骤（`gameplay_landing_tables.*`）对应工作台需要展示来自父步骤的暴露参数：
  - 顶部一栏 banner：`本子系统继承的参数 N 项：…`。
  - 调用新 API `GET /meta/exposed-params?target_step=...`。
  - 编辑暂不开放，仅展示。

### 5. 双语 / `$name$` 渲染

- 后端 README / cell note 可能含 `$name$` 引用。
- 前端在 README 视图与 cell tooltip 中实现替换：
  - 读取 `/meta/glossary` 获取 `name → term_zh / term_en`。
  - 默认显示 `term_zh`；项目设置 `display_lang = en` 时显示 `term_en`。
  - 找不到对应术语时保留原文 `$name$` 并标红提示。

### 6. 流水线 UI 跟进 6 步

- `pages/Workbench.tsx` 中的流水线步骤标签 `pipelineStepLabel(...)` 当前可能仍包含旧的 11 步标签；需要按 `pipeline_step_specs.list_step_specs` 同步：
  - environment_global_readme → 环境与全局说明
  - base_attribute_framework → 基础属性框架
  - gameplay_allocation → 玩法属性分配（matrix）
  - cultivation_resource_framework → 养成资源框架
  - cultivation_allocation → 养成属性分配（matrix）
  - gameplay_landing_tables.{sub} → 落地：{子系统}

### 7. 流水线生成的"AI 设计文档" 浏览面板

- 每步 design_text 现在更长，建议在工作台增加只读侧抽屉（`<aside>`）按 step_id 切换查看历史。

## 后端配套（轻量）

- `PUT /meta/tables/{table_name}/directory`（写入 directory）。
- `GET /meta/calculators`、`POST /compute/call-calculator`。
- `GET /meta/exposed-params?target_step=...`。

> 这些是极轻接口，建议合并到本轮一并实现，避免前端绕道走 agent 工具。

## 验收

- 工作台选中任一 matrix 表能看到正确的行 × 列 × 等级矩阵视图。
- 目录拖拽后，刷新页仍在新目录；`_table_registry.directory` 与展示一致。
- calculator 试算结果与 `call_calculator` Python API 一致。
- 子系统步骤 banner 正确展示父步骤暴露参数。
- README / 单元格 note 中的 `$xxx$` 默认渲染中文；切到 en 渲染英文。
- pipeline 进度条文案与 6 步一致。

## 备注

- 测试覆盖：保留 `tests/test_round3_features.py`，第 4 轮新加 API 补 1-2 个最小 round-trip 测试即可。
- 不要再扩展 11 步老 step；老术语只在迁移注释中保留。
