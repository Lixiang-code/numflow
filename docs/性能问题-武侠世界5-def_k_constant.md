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

---

## 优化实施与实测结果（2026-05-07）

### 优化策略（commit `bf46c79`）

| 策略 | 文件 | 原理 |
|---|---|---|
| **VLOOKUP/XLOOKUP/MATCH 结果缓存** | `formula_engine.py` | 首次 lookup 构建 hashmap，后续 O(1) 命中 |
| **写前值对比** | `formula_exec.py` | 新值 vs DB 当前值 (float 容差 1e-9)，只写真正变更的行 |
| **DAG 上游未变跳过** | `formula_exec.py` | 追踪每个节点 `rows_changed`，上游全 0 则跳过本节点 |
| **按需 commit** | `formula_exec.py` | 无变更不 commit，减少锁争用 |

### 实测数据（project_id=36 武侠世界5）

| 场景 | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| 值变更 (3500→4500) | 64.1s | **2.37s** | **27x** |
| 值变更 (4500→3500) | — | 2.34s | — |
| 同值重复 (3500→3500) | 64.1s | **0.20s** | **328x** |

### 逐策略生效验证

| 策略 | 验证 |
|---|---|
| VLOOKUP 缓存 | `monster_kill_time_actual` 1400ms→75ms (**18x**) |
| 写前值对比 | 值变更场景仅写 10134 行（而非全量计算行数） |
| DAG skip | 同值场景 4/13 执行 (**9 个 upstream_unchanged 跳过**) |
| 按需 commit | 同值场景 4 种子 rows_changed=0，无 commit |

### 残留瓶颈（通往 <1s 的路径）

当前值变更仍需 2.3s，主要耗时仍集中在 3 个 player_model 公式：

| 节点 | 行数 | 跨表 | 预估仍有 ~500ms/个 | 可优化手段 |
|---|---|---|---|---|
| player_model_paid.hp | 2000 | 8 | ~800ms | 拆分子系统中间列，消除嵌套 IF 中的冗余 VLOOKUP |
| player_model_standard.hp | 2000 | 8 | ~400ms | 同上 |
| player_model_free.hp | 2000 | 8 | ~400ms | 同上 |
| monster_verification.player_kill_time_actual | 600 | 3 | ~80ms | VLOOKUP 缓存已优化，剩余为 CONCAT 行级拼接开销 |
| monster_verification.monster_kill_time_actual | 600 | 3 | ~80ms | 同上 |
| equip_base.hp | 6000 | 3 | ~100ms | 6k 行同表四则已很快 |

#### 进一步优化建议

1. **拆分超级公式（预估收益 ~1s）**：
   - `player_model_paid.hp` 当前是单条嵌套 IF 分 9 个分支，每分支重复 VLOOKUP
   - 拆为 `player_model_equip_power.hp` / `player_model_partner_power.hp` 等中间列
   - def_k_constant 变更时，equip/partner 中间列的上游不变 → DAG skip 跳过
   - player_model 只需 IF(sub_system, ...引用中间列)，无 VLOOKUP

2. **CONCAT 预计算列（预估收益 ~100ms）**：
   - `monster_verification` 的 VLOOKUP(CONCAT(level, monster_type), ...) 每行重复拼字符串
   - 加一个 `row_key` 列预存拼接结果，VLOOKUP 直接用

3. **DuckDB 介入（预估收益 ~200ms）**：
   - 即使 VLOOKUP 不进白名单，同表纯四则 (`num_base_framework.*`, `equip_base.hp` 6k 行) 可用
   - 需在 project_settings 写入 `perf.use_duckdb_compute = true`

4. **并行执行无依赖节点（预估收益 ~0.5s）**：
   - DAG 拓扑排序后同一层的节点无互相依赖，可并行
   - 4 个 seed 节点和 4 个深度2节点无互相依赖

---

生成时间：2026-05-07，更新于 2026-05-08（DuckDB B3 实测 + B4 方案）

---

## DuckDB B3 实测（commit `a82ffc9`，2026-05-08）

### 变更内容

- VLOOKUP/XLOOKUP 精确匹配进入 DuckDB 白名单
- CONCAT 进入 DuckDB 白名单
- 数组值规范化（自动检测数值/文本类型）

### 命中情况

DuckDB 命中 8/13 节点，全为同表 `@self[col]` + 全表 `@@other[col]` 引用型公式：

| DuckDB 命中节点 | 原始 | V1(缓存) | B3(DuckDB) | 累计加速 |
|---|---|---|---|---|
| player_model_equip_summary.hp | 1700ms | 1700ms | **2.2ms** | **770x** |
| monster_verification.player_kill_time_actual | 1700ms | 75ms | **3.7ms** | **460x** |
| monster_verification.monster_kill_time_actual | 1400ms | 75ms | **3.6ms** | **390x** |
| monster_verification.monster_kill_deviation | 190ms | 190ms | **3.5ms** | **54x** |
| monster_verification.player_kill_deviation | 190ms | 190ms | **3.7ms** | **51x** |
| monster_verification.verdict | 18ms | 18ms | **2.6ms** | **7x** |
| num_base_framework.hp | 18ms | 18ms | **5.6ms** | **3x** |
| num_base_framework.def_reduction | 5.6ms | 5.6ms | **2.2ms** | **2.5x** |

### 未命中 5 节点（瓶颈转移）

| 未命中节点 | 行数 | B3 耗时 | 根因 |
|---|---|---|---|
| player_model_paid.hp | 2000 | 811ms | 含跨表 `@hero_base[hp]` |
| player_model_standard.hp | 2000 | 580ms | 同上 |
| player_model_free.hp | 2000 | 503ms | 同上 |
| equip_base.hp | 6000 | 185ms | 含跨表 `@num_base_framework[hp]` |
| hero_base.hp | 200 | 7ms | 同上（可忽略） |

全部卡在同一行：`duckdb_compute.py:201`
```python
if tbl != table_name:
    raise NotSupported(f"B2 暂不支持跨表 @{tbl}[{col}]（请用 @@ + INDEX）")
```

### 三版本汇总

| 版本 | Commit | 策略 | 值变更 | 同值 | 累计提升 |
|---|---|---|---|---|---|
| 原始 | `—` | 无 | 64.1s | 64.1s | — |
| V1 | `bf46c79` | 缓存+对比+skip | 2.37s | 0.20s | **27x** |
| V2 | `a82ffc9` | DuckDB B3 | 2.46s | 0.20s | —（瓶颈转移） |

---

## DuckDB B4：跨表 @ref 支持（推荐下一步，直通 <1s）

### 原理

放开 `duckdb_compute.py:201` 的跨表 `@table[col]` 限制。在 SQL 翻译阶段，将 `@other_table[col]` 转为对该表的 JOIN + 列选择。

`player_model_paid.hp` 引用 8 张表，一条 DuckDB SQL 完成多表 JOIN：

```sql
SELECT ... FROM main_table
JOIN hero_base ON main.row_id = hero_base.row_id
JOIN player_model_equip_summary ON ...
-- ...
```

### 预期收益

| 节点 | 当前(pandas) | DuckDB 预估 | 收益 |
|---|---|---|---|
| player_model_paid.hp | 811ms | ~50ms | **16x** |
| player_model_standard.hp | 580ms | ~50ms | **12x** |
| player_model_free.hp | 503ms | ~50ms | **10x** |
| equip_base.hp | 185ms | ~10ms | **18x** |
| hero_base.hp | 7ms | ~2ms | 3x |
| **DAG 总耗时** | **2.4s** | **~300ms** | **✓ < 1s** |

### 为什么选 B4 而非拆分公式

| 维度 | DuckDB B4 | 拆分公式 |
|---|---|---|
| 覆盖面 | 一次实现，全项目受益 | 逐项目改公式 |
| 维护成本 | 零（不动公式） | 公式膨胀、中间列增多 |
| 预估总耗时 | **< 1s** | ~1.4s（仅 partner 分支可 skip） |
| 通用性 | **高** | 低 |

### 实现注意事项

- **简单 case 优先**：同 `row_id` JOIN 覆盖 `hero_base.hp` / `equip_base.hp` 场景
- **复杂 case**：player_model 按 `sub_system` 匹配不同子表，需条件 JOIN
- **保底方案**：标量子查询 `(SELECT col FROM other_table WHERE row_id = main.row_id)` 兼容所有场景

数据来源：`/www/wwwroot/numflow/data/projects/5/project.db` 中 `_perf_log`、`_formula_registry`、`_dependency_graph` 表
