# 武侠世界5 — 修改 def_k_constant 导致 CPU 跑满问题分析

## 问题概述

修改常量 `def_k_constant`（防御K常数，值 3500）后，后端 CPU 跑满 20~64 秒，接口阻塞。

## 相关 Git 提交

| Commit | 时间 | 说明 |
|---|---|---|
| `0a7151e` | 5/5 00:17 | **perf: 第5轮性能优化** — 新增常量修改后自动 DAG 重算机制，声称"常量修改端到端 10s→228ms" |
| `af66e25` | 5/5 00:42 | fix: 修复性能回归 — 提取 recalc_lock 模块，为 DAG 种子加入 `execute_seeds` 参数 |
| `9351665` | 5/5 01:08 | perf: B2 DuckDB 跨表计算 — 但 VLOOKUP/CONCAT 类公式不在白名单内，无条件 fallback |

**关键事实**：`0a7151e` 提交的 "10s→228ms" 测试数据大概率来自简单项目（项目2或4），在项目5（武侠世界5）中不成立。

## 触发链路

```
用户修改 def_k_constant (值 3500)
  → patch_constant (meta.py:168) _cascade_update_formula_consts()
  → patch_constant (meta.py:183) recalculate_downstream_dag(seeds, execute_seeds=True)
      → 全表扫描 _formula_registry 找引用 def_k_constant 的公式
      → 找到 4 个种子节点
      → BFS 反向遍历 _dependency_graph 收集受影响节点
      → 共 13 个唯一节点
      → Kahn 拓扑排序
      → 逐个执行 _execute_node
```

## DAG 级联图（13 个节点）

```
def_k_constant 变更
│
├─ 深度1 (4 seeds)
│   ├── num_base_framework.def_reduction     (200行, 同表)
│   ├── num_base_framework.hp                 (200行, 同表)
│   ├── monster_verification.player_kill_time_actual  (600行, VLOOKUP跨3表)
│   └── monster_verification.monster_kill_time_actual  (600行, VLOOKUP跨3表)
│
├─ 深度2 (4个)
│   ├── hero_base.hp                          (200行)
│   ├── equip_base.hp                         (6000行)
│   ├── monster_verification.player_kill_deviation
│   └── monster_verification.monster_kill_deviation
│
├─ 深度3 (5个)
│   ├── player_model_equip_summary.hp         (200行)
│   ├── player_model_standard.hp              (2000行, VLOOKUP跨8表)
│   ├── player_model_paid.hp                  (2000行, VLOOKUP跨8表)  ← 最大瓶颈
│   ├── player_model_free.hp                  (2000行, VLOOKUP跨8表)
│   └── monster_verification.verdict
│
└─ 深度4+ (monster_verification收敛路径，已有回边)
```

## 性能数据（来自 _perf_log）

### 项目规模

| 指标 | 数量 |
|---|---|
| 总表数 | 35 |
| 总常量数 | 177（其中公式常量 1 个）|
| 公式注册数 | 359 |
| 依赖图边数 | 790 |
| DAG 受影响节点 | 13 |

### 两次典型 DAG 执行的逐节点耗时

| 节点 (表.列) | 行数 | 跨表数 | Run A (ms) | Run B (ms) | 慢在哪里 |
|---|---|---|---|---|---|
| num_base_framework.def_reduction | 200 | 1 | 5.6 | 5.6 | 同表四则，很快 |
| num_base_framework.hp | 200 | 1 | 18.2 | 17.7 | 同表四则，很快 |
| monster_verification.player_kill_deviation | 600 | 2 | 192.3 | 186.9 | 正常 |
| monster_verification.player_kill_time_actual | 600 | 3 | 1811.7 | 1728.4 | ⚠️ VLOOKUP跨 monster_model/player_model_standard |
| hero_base.hp | 200 | 1 | 7.1 | 5.7 | 快 |
| equip_base.hp | 6000 | 3 | 208.9 | 199.4 | 6k行但快（同表为主） |
| player_model_equip_summary.hp | 200 | 2 | 1838.5 | 1723.9 | 正常 |
| **player_model_paid.hp** | 2000 | **8** | **13564.5** | **10816.3** | 🔴 超级公式！嵌套IF+20次VLOOKUP |
| player_model_standard.hp | 2000 | 8 | 3019.1 | 2374.8 | 同结构，略轻 |
| player_model_free.hp | 2000 | 8 | 7594.2 | 2329.3 | 同结构 |
| monster_verification.monster_kill_time_actual | 600 | 3 | 4450.5 | 1422.0 | ⚠️ 波动大 |
| monster_verification.monster_kill_deviation | 600 | 2 | 1360.4 | 188.6 | ⚠️ 波动大 |
| monster_verification.verdict | 600 | 1 | **30060.5** | 18.1 | 🔴 异常！平时18ms突然30s |
| **DAG 总耗时** | | | **64141** | **21038** | |

### 瓶颈排名（按正常情况 Run B）

| 排名 | 节点 | 耗时 | 占 DAG 总时间 |
|---|---|---|---|
| 1 | player_model_paid.hp | 10.8s | **51%** |
| 2 | player_model_standard.hp | 2.4s | 11% |
| 3 | player_model_free.hp | 2.3s | 11% |
| 4 | player_model_equip_summary.hp | 1.7s | 8% |
| 5 | monster_verification.monster_kill_time_actual | 1.4s | 7% |
| 6 | monster_verification.player_kill_time_actual | 1.7s | 8% |
| — | 其余 7 个节点合计 | <1s | 4% |

## 根因分析

### 1. 超级公式：player_model_paid.hp（占 50%+ 耗时）

该公式是一段深度嵌套 IF + 20 余次 VLOOKUP 的单条 SQL 公式：
- 按 `sub_system` 字段分 9 个分支（hero/equip/enhance/refine/skill/partner_base/partner_equip/partner_bond/partner_total）
- 每个分支触发对应子系统的 VLOOKUP 横表查询
- **2000 行 × 8 个跨表 DataFrame 加载** = 每次执行加载 8 张表
- player_model_standard.hp / player_model_free.hp 同结构

### 2. 跨表 VLOOKUP 公式（占 15~20%）

`monster_verification.*` 系列公式使用 `VLOOKUP(CONCAT(...), @@table[row_id], @@table[col])`：
- 600 行 × 每次 VLOOKUP 都要 CONCAT 字符串拼接作键
- 跨表多次 VLOOKUP 无法 DuckDB 加速（VLOOKUP 在 DISABLED_TOKENS 黑名单中）

### 3. verdict 列偶发 30 秒异常（占不正常 Run 的 47%）

`monster_verification.verdict` 公式极简单（同表引用 + 常量比较，平时 18ms），但近期一次跑了 30 秒。可能原因：
- SQLite WAL 锁争用（前序 12 个节点大量写入后 commit 冲突）
- Python GC 在 DAG 尾段触发
- 系统 I/O 颠簸

### 4. DuckDB 未启用

`project_settings` 中无 `perf` 配置，`use_duckdb_compute` 默认 `False`。即使开启，VLOOKUP/CONCAT 类公式也不在白名单内，无法受益。

## 建议修复方向

### 短期止血（restart 后立即生效）

1. **跳过常量值变更的自动 DAG**：`meta.py:168` 中修改 `value` 型常量（非 formula 型）时跳过 `recalculate_downstream_dag`，让用户手动触发重算。

### 中期优化

2. **拆分超级公式**：`player_model_paid.hp` / `player_model_standard.hp` / `player_model_free.hp` 拆为多个中间列：
   - `player_model_equip_power.hp` = 装备子系统HP
   - `player_model_partner_power.hp` = 伙伴子系统HP
   - 然后 `player_model_paid.hp` = IF(sub_system, ...引用中间列...)
   - 好处：def_k_constant 变更时中间列不变，只需重算引用 def_k 的叶子列

3. **VLOOKUP 预索引**：monster_verification 的 VLOOKUP(CONCAT(...)) 在每行重复构建 lookup 数组，可以预先构建 lookup dict 缓存。

4. **启用 perf 开关**：在项目 5 的 project_settings 中写入 `perf.use_duckdb_compute = true`，至少加速 `num_base_framework.def_reduction/hp` 这种同表纯四则公式。

### 长期

5. **DAG 增量重算**：只重算值真正发生变化的节点（当前是拓扑一律重算）。
6. **DuckDB B3 支持 VLOOKUP/CONCAT**：将 VLOOKUP 翻译为 DuckDB JOIN。

---

生成时间：2026-05-07  
数据来源：`/www/wwwroot/numflow/data/projects/5/project.db` 中 `_perf_log`、`_formula_registry`、`_dependency_graph` 表
