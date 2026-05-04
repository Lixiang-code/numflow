# 迁移说明：be12b8a → 当前版本

> 适用场景：在新环境全新部署，或从 `be12b8a` 升级到本版本的运维人员。  
> 本次**不需要执行任何数据库 DDL**，所有变更均为代码默认值，重启服务即生效。

---

## 一、本次变更概览

| 模块 | 变更类型 | 说明 |
|------|----------|------|
| `agent_tools.py` | 功能增强 | 大表保护：`get_table_list` 精简返回、`read_table` 200行上限 |
| `agent_runner.py` | 提示词升级 | 四段式 CoT、init/maintain 分支、工具规范更新 |
| `prompt_router.py` | 提示词升级 | `gameplay_planning` / `gameplay_table` 步骤提示词 |
| `skill_library.py` | 重大更新 | 8个SKILL内容升级 + 新增6个数值设计SKILL |
| `tool_envelope.py` | 行为变更 | `read_table` 返回>200行时拒绝并报错 |

---

## 二、全新部署步骤（新机器）

### 1. 拉取代码
```bash
git clone <repo_url>
cd numflow
git checkout main   # 确保是最新
```

### 2. 后端环境
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. 初始化数据目录
```bash
mkdir -p data/projects
# 不需要从旧机器拷贝 project.db —— 项目数据库会在用户首次创建项目时自动初始化
```

### 4. 配置与启动
```bash
cp .env.example .env   # 按实际填写 API keys、端口等
# 使用 systemd（参考 deploy/ 目录下的 service 文件）：
systemctl enable numflow-backend
systemctl start numflow-backend
```

### 5. 前端构建
```bash
cd frontend
npm install
npm run build
# 确认 nginx/caddy 指向 frontend/dist
```

---

## 三、从 be12b8a 升级（已有环境）

```bash
cd /www/wwwroot/numflow
git pull origin main
systemctl restart numflow-backend
```

升级后**无需手动操作数据库**，但已有项目中由旧代码播种的 SKILL 不会自动更新（`ensure_default_skills` 仅在 `template_key` 不存在时插入）。  
若希望已有项目也使用最新 SKILL 内容，对每个项目执行一次：

```sql
-- 连接到 data/projects/<slug>/project.db
DELETE FROM _skill_modules WHERE skill_id IN (SELECT id FROM _skills WHERE template_key IS NOT NULL);
DELETE FROM _skills WHERE template_key IS NOT NULL;
-- 重启服务后首次访问该项目即自动重新播种
```

---

## 四、重要变更详解

### 4.1 大表保护（read_table / get_table_list）

**变更前：** `get_table_list` 返回所有字段；`read_table` 无行数上限。  
**变更后：**
- `get_table_list` 仅返回 `table_name`、`display_name`、`view_slice_only`
  - 总行数 > 300 的表，`view_slice_only = true`
- `read_table` 若筛选后命中行数 > 200，**直接拒绝**并返回：
  ```
  数据规模过大，请修改查询范围
  ```
- `read_3d_table_full` 在 `view_slice_only=true` 的表上**禁止使用**，应改用 `read_3d_table` 指定切片
- `read_matrix` 在 `view_slice_only=true` 的表上必须传 `rows`/`cols`

**Agent 行为指引（已写入系统提示词）：**
1. 先 `get_table_list` 确认 `view_slice_only`
2. 大表先 `get_table_schema` 确认结构和查询范围
3. 用 `columns` / `filters` / `level_range` 缩小 `read_table` 范围
4. 或改用 `sparse_sample` 查看代表性样本

### 4.2 设计阶段 CoT 升级：三段式 → 四段式

design 阶段现在要求输出四段式思维链：

```
## 1. 我对用户需求的理解
## 2. 我对游戏性的设计理解
## 3. 我对表格设计的理解（参考环境中的SKILL，若本次不生产表则忽略）
## 4. 我的最终设计
```

同时 `init` 模式与 `maintain` 模式的 **gather 阶段提示词**现在有所区别：
- `init`：design/review 阶段无需重复读取工具，直接基于 gather 结果推进
- `maintain`：可以继续获取需要的其他信息

### 4.3 gameplay_planning 步骤提示词更新

新增**顺序**块（5步工作流）和 SKILL 读取提示，Agent 会在规划阶段优先读取可用 SKILL 再制定计划。

### 4.4 gameplay_table 步骤提示词更新

新增**步骤 4 前置检查**：若前置条件（属性框架、资源框架等）缺失，Agent 会发起维护任务后退出，而非盲目继续。

### 4.5 SKILL 库重大更新

#### 修改的 SKILL（原有8个中有2个内容变更）

**`landing_common`（玩法落地通用制作说明）**
- `production_rules`：从 3 条扩展到 7 条，新增「补齐上游缺失内容」「数值全关联」「推翻上游设计须发布联动任务」「扩展上游过简内容」
- `structure_rules`：简化（移除"不能退化成"条款，更聚焦核心约束）
- `acceptance`：拆分为**制作阶段**（执行验收）和**收尾阶段**（重写README，删除过时执行注释）
- `pitfalls`：内容替换为更直接的两条误区
- 新增 `project` 模块：要求玩法必须输出最终可供程序使用的配置表，禁止仅有过程表

**`skill_landing`（技能制作说明）**
- `positioning`：新增技能强度量化框架（输出强度/生存强度）及数值框架要求
- `outputs`：替换为回合制/即时制技能系数计算方法（冷却时间占用强度、控制类技能折算）
- `core_rules`：替换为多技能强度分配方法（均分原则、减法/除法拆分）

#### 新增 SKILL（6个，均为 `default_exposed=False`）

| SKILL | template_key | step_id |
|-------|-------------|---------|
| 数值框架设计 | `numerical_framework_design` | `base_attribute_framework` |
| 玩法属性分配 | `gameplay_attr_allocation` | `gameplay_allocation` |
| 养成资源框架 | `cultivation_resource_framework` | `cultivation_resource_framework` |
| 怪物及关卡设计 | `monster_stage_design` | `gameplay_table` |
| 资源产出设计与读取 | `resource_production_design` | `gameplay_table` |
| 玩家模型设计 | `player_model_design` | `gameplay_table` |

#### SKILL 可见性设置变更

| SKILL | enabled | default_exposed |
|-------|---------|----------------|
| 玩法落地通用制作说明 | ✅ | ❌ |
| 技能制作说明 | ✅ | ❌ |
| 装备/宝石/坐骑/翅膀/时装/副本制作说明 | ❌ | ❌ |
| 以上6个新SKILL | ✅ | ❌ |

> **注意**：所有 SKILL 均不再默认暴露给 Agent。  
> 用户需要在项目设置中手动启用并暴露，或在 Agent 对话中手动引用，避免不相关 SKILL 干扰上下文。

---

## 五、无需迁移的内容

- **项目数据库**（表数据、README、公式、常量等）：完全不需要迁移，新环境从空白项目重新开始
- **全局数据库**（`server.sqlite` / `numflow.db`）：若是全新部署则自动创建；若升级则保留原有即可
- **提示词覆盖**（`_prompt_overrides`）：旧版「侠客世界」项目的提示词覆盖已全部合并进代码默认值，新项目无需任何项目级覆盖

---

## 六、验证部署正确

```bash
# 运行单元测试（96个，全部应通过）
cd /www/wwwroot/numflow
python3 -m pytest backend/tests/ -q

# 检查服务状态
systemctl status numflow-backend

# 快速验证 API
curl http://127.0.0.1:8000/api/health
```

---

_文档生成于 2026-05-02，对应代码版本见本次提交 commit hash。_
