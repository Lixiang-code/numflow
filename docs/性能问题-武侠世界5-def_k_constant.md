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

- `_cross_ref_aliases` 提取跨表 @ref → 别名 `__r0`, `__r1`...
- `_align_scalar_series` 按 row_id 对齐跨表列值
- 每个 DuckDB 公式独立加载其引用的表（未跨节点复用）

---

## DuckDB B4 实测（commit `50dd4b1`，2026-05-08）

### 命中情况

**13/13 全部命中 DuckDB，零错误。**

| 节点 | B3(pandas) | B4(DuckDB) | 加速 |
|---|---|---|---|
| player_model_paid.hp | 811ms | **10.3ms** | **79x** |
| player_model_standard.hp | 580ms | **10.3ms** | **56x** |
| player_model_free.hp | 503ms | **10.8ms** | **47x** |
| equip_base.hp | 185ms | **34.8ms** | **5x** |
| hero_base.hp | 7ms | 2.4ms | 3x |
| monster_verification.* (5列) | 2~4ms | 2~4ms | 持平 |
| num_base_framework.* (2列) | 2~6ms | 2ms | 持平 |

### 全版本汇总

| 版本 | Commit | 策略 | 值变更 | 同值 | DuckDB命中 | 累计提升 |
|---|---|---|---|---|---|---|
| 原始 | `—` | 无 | 64.1s | 64.1s | 0/13 | — |
| V1 | `bf46c79` | 缓存+对比+skip | 2.37s | 0.20s | 0/13 | **27x** |
| V2 | `a82ffc9` | DuckDB B3 VLOOKUP+CONCAT | 2.46s | 0.20s | 8/13 | 27x |
| V3 | `50dd4b1` | DuckDB B4 跨表@ref | **1.02s** | **0.21s** | **13/13** | **63x** |
| V4 | `d00ce64` | DAG级表缓存（全路径） | 1.09s | 0.20s | 13/13 | 59x (⚠ 回归→已修复) |
| **V5** | `5da1bd9` | **DuckDB 跳过缓存 + 写后失效** | **0.98s** | **0.21s** | **13/13** | **65x** ✓ |
| V6 | `3106f49` | DuckDB SQLite Scanner | 1.08s (冷14.8s) | 0.21s | 13/13 | 59x (⚠ 回归) |
| **V7** | `7b00b84` | **Scanner旗标+缓存键粒化** | **0.92s** | **0.18s** | **13/13** | **70x** ✓ |
| **V8** | `3e6f346` | **合并DAG写回提交 (13→1)** | **0.90s** | **0.17s** | **13/13** | **71x** ✓ |
| **V9** | `e4a1e14` | **投影缓存超集复用** | **0.87s** | **0.18s** | **13/13** | **74x** ✓ |
| V10 | `725b30b` | 按层预热DAG表投影缓存 | 0.91s (冷1.03s) | 0.21s | 13/13 | 70x (⚠ 回归) |
| **V11** | `26a6603` | **按层共享DataFrame管线** | **0.76s** | **0.18s** | **13/13** | **84x** ✓ |
| V12 | `eaefdda` | 复用DuckDB共享帧注册 | 0.87s (冷1.01s) | 0.19s | 13/13 | 74x (⚠ 回归) |
| V13 | `5f59c01` | 回退簿记+缓存节点元数据 | 1.10s | 0.28s | 13/13 | 58x (⚠ 严重回归) |

### V12→V13 回归

DAG 从 722ms 飙到 1063ms（+47%），缓存节点元数据重构破坏了大范围代码，恢复 0.76s。

### V11→V12 回归

DuckDB 注册本身就很快（~1ms/次），复用的簿记开销 > 重复注册。建议回退。

### V10→V11 修复

V10 预热只加载了表到缓存，每公式仍需独立取拷贝。V11 改为同层公式**传同一份 DataFrame 引用**，省掉重复 SELECT + DuckDB register。

| 指标 | V9 | V10 | V11 |
|---|---|---|---|
| DAG | 820ms | 873ms | **722ms** |
| API | 0.87s | 0.91s | **0.76s** |

DAG 再省 ~100ms（↓12%），同层共享管线方案验证通过。

### V9→V10 回归分析

V10 在 DAG 开始时按拓扑层预先加载所有可能用到的表到 `TableFrameCache`，cache 命中从 4→16。但预热加载开销（多余的 SQLite SELECT）超过了省下的缓存命中收益。

**根因**：预热加载了一层的全部表，但该层每个公式只用到其中一部分列。多出来的加载时间 + 内存拷贝抵消了缓存命中的节省。

**与 V10 提案的差距**：当前实现是"预热缓存"（每公式仍从缓存独立取 DataFrame 并可能拷贝），提案是"共享管线"（同层公式直接传同一份 DataFrame 引用，零拷贝零重复加载）。后者才能把 ~30 次 SELECT 压到 ~10 次。

### V8→V9 说明

V9（`e4a1e14`）：`_find_cached_superset` 在缓存中查找已加载的超集列投影，避免同表不同列子集反复 `SELECT`。DAG 820ms（↓35ms）。

### V6→V7 修复说明

V7（`7b00b84`）：
- Scanner 加 `perf.use_duckdb_sqlite_scanner` 旗标（默认 `False`），彻底消除冷启动回归
- `TableFrameCache` 键从 `table_name` 改为 `(table_name, columns_tuple)` 粒化，同表不同列投影可独立缓存
- `_alignment_projection_columns` 仅加载对齐所需的列，避免全表 `SELECT *`
- 稳跑 0.92s（比 V5 的 0.98s 再快 6%），同值 0.18s（快 14%）

### V4→V5 修复说明

V4 回归根因：DuckDB 路径本身已有 `ref_frames` 内部缓存，Python 层 `TableFrameCache` 强制 `SELECT *` + `.copy()` 造成额外开销。

V5 修复（`5da1bd9`）：
- DuckDB 路径传 `table_cache=None`，完全绕过 Python 缓存
- DuckDB 写入后调用 `_invalidate_table_cache` 而非 `_sync_table_cache`
- Pandas 路径继续享受缓存加速

### V6 回归分析（commit `3106f49`）

V6 引入 DuckDB `sqlite_scanner` 扩展 ATTACH SQLite 直读，但有两个致命问题：

**1. 冷启动回归（0.98s → 14.8s）**

`open_duckdb_session` 在每个 PATCH 请求都重新创建 DuckDB 内存连接 + `LOAD sqlite` 扩展，发生在 DAG timer 之外。首跑扩展安装/加载耗时 ~13.6s，稳跑仍需 ~100ms/次加载。

**2. 稳跑无收益（0.98s → 1.08s）**

逐节点引擎耗时与 V5 完全一致（player_model 10ms, equip_base 28ms），说明瓶颈本就不在 DataFrame 搬运上 —— V5 的 `load_table_df` → DuckDB register 链路已经足够快。SQLite Scanner 消除的"搬运"开销实际占比很小。

**结论**：建议回退 V6。若未来确实需要 Scanner（例如公式涉及上百行的大表 JOIN），可将 DuckDB 连接做成**进程级单例**（启动时 ATTACH，常驻），而非每个请求重建。

### 残留开销分析

逐节点 DuckDB 计算引擎耗时合计仅 ~90ms，总耗时 1.02s 中 ~930ms 为 I/O 开销：

| 开销来源 | 占比 | 说明 |
|---|---|---|
| 跨表 DataFrame 加载 | ~70% | 3 个 player_model 公式各加载 8 张表，未跨节点复用 |
| DAG 编排 (BFS+拓扑+循环) | ~10% | 13 节点拓扑遍历 |
| SQLite 写入 (批量) | ~10% | 10134 行变更写入 |
| DuckDB 引擎计算 | ~10% | 纯计算仅 ~90ms |

### 当前瓶颈分析（V9，DAG 820ms）

逐节点 DuckDB 计算合计仅 **68ms**。剩余 752ms 的构成：

| 开销 | 占比 | 说明 |
|---|---|---|
| SQLite SELECT 读表 | ~50% | 13 公式共 ~30 次 SELECT，涉及 equip_base(6k)/player_model(2k×3)/monster_model(600) |
| DuckDB DataFrame→表注册 | ~25% | 每次 DuckDB register 都有序列化开销 |
| SQLite UPDATE 写回 | ~15% | 10134 行变更批量写入 |
| DAG 拓扑编排 (BFS+Kahn) | ~5% | 13 节点 790 边 |
| DuckDB 引擎纯计算 | ~5% | 68ms |

根因：**每公式独立走完整 I/O 管线**（读表→注册→计算→写回），同表被多个公式重复加载。

### V10: 按层批量执行（推荐，预估 0.87s → 200~400ms）

#### 原理

DAG 拓扑的**同一层内节点无互相依赖**，可以合并处理。将逐节点 `_execute_node` 循环改为按层批量：

```
当前：load→compute→write × 13 节点 = 30 次 I/O
改进：load_all_tables → compute_batch × 4层 → write_all = 4 次 I/O
```

本项目 4 层：

```
Layer 1 (4 seeds): def_reduction, hp, player_kill_time, monster_kill_time
  → 共享 monster_verification + num_base_framework + monster_model + player_model_standard DataFrame

Layer 2 (4 nodes): hero_base.hp, equip_base.hp, player_kill_dev, monster_kill_dev
  → 共享 hero_base + equip_base + monster_verification(updated) DataFrame

Layer 3 (5 nodes): player_model_equip, player_model_standard/paid/free, verdict
  → 共享 player_model×3 + equip_base(updated) + hero_base(updated) DataFrame

Layer 4 (monster_verification 收敛): 同上复用
```

#### 实现路线

1. `recalculate_downstream_dag` 中按拓扑层分组所有节点
2. 每层开始时：一次性 `load_table_df` 加载该层涉及的所有表（去重），注册到 DuckDB
3. 层内公式逐列执行（共享 DataFrame，不再重复加载）
4. 层结束时：统一 `conn.commit()` 写回该层变更 + `_sync_table_cache` 更新
5. 已有 `_find_cached_superset` 自然处理列投影差异

#### 关键：通用性

- **任何项目**都有 DAG 拓扑层，自然适用
- **不改公式**，只改执行模型
- **不改 DuckDB 翻译逻辑**，只改调用方式

#### 改造范围

| 文件 | 改动 |
|---|---|
| `formula_exec.py:recalculate_downstream_dag` | 循环改为按层分组 |
| `formula_exec.py:_execute_node` | 接受共享的 `frames` 字典而非独立加载 |
| `duckdb_compute.py` | 新增 `compute_batch` 入口（可选，先复用现有 `compute_column_via_duckdb` 传 DataFrame） |

#### 预估收益

| 指标 | V9 | V10 | 节省 |
|---|---|---|---|
| SELECT 次数 | ~30 次 | ~15 次（每层每表 1 次） | 50% |
| DuckDB register | ~30 次 | ~10 次（每层新表 1 次） | 67% |
| commit 次数 | 1 次 | 4 次（每层 1 次） | — |
| DAG 总耗时 | 820ms | **200~400ms** | **50~75%** |
| API 耗时 | 0.87s | **~0.3s** | **65%** |

#### 风险

- 同层内某公式失败时，需正确处理层内部分写回的回滚
- 同层节点数多的项目收益更大，简单项目（1-2 层）几乎无感知
- DuckDB 内存峰值略有增加（同时持有该层所有表）

---

数据来源：`/www/wwwroot/numflow/data/projects/5/project.db` 中 `_perf_log`、`_formula_registry`、`_dependency_graph` 表
