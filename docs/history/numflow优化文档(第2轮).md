# 优化问题

项目路径：[/www/wwwroot/numflow/]
这是一个AGENT自动化游戏数值项目，现在已经初具雏形，先提交下列问题请逐个处理，保证处理质量，需要提交并推送github，需要写中文日志，需要原地参数自我审查，不允许遗漏任务

## 超长写入问题
观察到AI有时候还是在做超长输入
例子：🔨 write_cellscall_id:jkO6we4l
{"table_name": "cult_res_cost_per_level", "updates": [{"row_id":"hero_gold_31","column":"system_id","value":"hero"},{"row_id":"hero_gold_31","column":"resource_id","value":"gold"},{"row_id":"hero_gold_31","column":"level","value":31},{"row_id":"hero_gold_31","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_32","column":"system_id","value":"hero"},{"row_id":"hero_gold_32","column":"resource_id","value":"gold"},{"row_id":"hero_gold_32","column":"level","value":32},{"row_id":"hero_gold_32","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_33","column":"system_id","value":"hero"},{"row_id":"hero_gold_33","column":"resource_id","value":"gold"},{"row_id":"hero_gold_33","column":"level","value":33},{"row_id":"hero_gold_33","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_34","column":"system_id","value":"hero"},{"row_id":"hero_gold_34","column":"resource_id","value":"gold"},{"row_id":"hero_gold_34","column":"level","value":34},{"row_id":"hero_gold_34","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_35","column":"system_id","value":"hero"},{"row_id":"hero_gold_35","column":"resource_id","value":"gold"},{"row_id":"hero_gold_35","column":"level","value":35},{"row_id":"hero_gold_35","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_36","column":"system_id","value":"hero"},{"row_id":"hero_gold_36","column":"resource_id","value":"gold"},{"row_id":"hero_gold_36","column":"level","value":36},{"row_id":"hero_gold_36","column":"cost_type","value":"per_level"},{"row_id":"hero_gold_37","column":"system_id","value":"hero"},{"row_id":"hero_gold_37","column":"resource_id","value":"gold"},{"row_id":"hero_gold_37","column":"level","value":37},
这个例子不是全部输入，太长了不给你展示了，分析一下这个超长输入可能的原因，是没有给AI合适的工具，还是提示词问题，想办法改一下。感觉可能是字符串+序号输入工具缺失？

## 常量系统增强

现在AI已经会创建常量了，但是我要增强
常量（魔法数字）需要打标签，系统标签：属于什么系统，可以有多个，可以是‘全局常量’等，每个常量都需要有至少1个所属标签
标签需要预注册

### 常量提示词增强

注意到以下实际的AI使用情况：
const_register
const_register
17:24:05
▲
调用参数
{"name_en": "tier_size", "name_zh": "装备等阶等级数", "value": 10, "brief": "每10级为一阶"}

这里的问题在于brief作为说明，不应该提及‘10’这个明明是一个变量的内容，不应该在简介里面被提及，简介形式应该是：‘本项定义每多少级会划分出一个等阶’这种不带值的描述

### 标签系统预注册工具
每个单元创建时，要考虑是否要新增标签，标签用来管理和标记常量，主系统默认具有跟主系统同名的标签（不需要额外创建），但是如果需要额外设置子标签，则需要创建，子标签需要明确指定自己的父标签（指系统名称）是什么
提示词中需要注入标签注册和常量标签的说明

## 常量专属页面
在工作台（表格区域），需要有一个专门的常量页，按照标签展示所有常量，另外系统表右侧的系统所属常量也需要保留

## 关于重试的问题

现在如果出现网络错误重试，会导致整个步骤要从来
正常的来说网络错误原地重发就行，为什么会导致整个步骤（包括已完成的）需要从来呢？看看问题

## 关于前端依赖

现在AGENT流程一旦前端网页关掉就停了
哎呀停了也没问题
但是你要解决那个重试问题，不要说停了，回来就整个模块重置了
现在一个模块的时间可能都超过10分钟了，这样重置10分钟白干不行，想想办法
另外关于‘进入工作台’（其实就是进入表格页）后AGENT就会停，这个情况不能接受，这里前端没有消失呀，理论上AGENT应当继续推进
但是这个状态工作台应该是只读的，避免冲突就是了

## 关于前端表格性能优化问题

现在前端表格一旦键入了一个格子，前端就有一种卡住的感觉，点哪都没有，也无法取消键入，看看问题在哪

## 关于run_validation

我在延迟资源定量表（城市猎人项目）的最后，看到这个内容：
{"status": "partial", "data": {"passed": false, "warnings": ["规则违反 10 条"], "tables": [{"table_name": "cultivation_resource_summary", "validation_status": "unknown"}], "per_table": {"cultivation_resource_summary": "warn"}, "violations": [{"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "gold", "column": "balance_ratio", "message": "percent_bounds: 6.65 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "exp", "column": "balance_ratio", "message": "percent_bounds: 1.01 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "mount_feed", "column": "balance_ratio", "message": "percent_bounds: 28.0 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "mount_gear_stone", "column": "balance_ratio", "message": "percent_bounds: 21.5 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "pet_feed", "column": "balance_ratio", "message": "percent_bounds: 26.1 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "artifact_exp", "column": "balance_ratio", "message": "percent_bounds: 5.93 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "dust", "column": "balance_ratio", "message": "percent_bounds: 66.7 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "reputation_exp", "column": "balance_ratio", "message": "percent_bounds: 3.42 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "bound_diamond", "column": "balance_ratio", "message": "percent_bounds: 1.67 不在 [0.0, 0.95]"}, {"table": "cultivation_resource_summary", "rule_id": "pct_balance_ratio_bounds", "row_id": "talent_point", "column": "bala

首先，这个内容为什么不全，中断了
其次，这个内容我在AI对话中没看到，这是为什么，这个检查是不是应该主动提交给AI，但事实上这个返回出现的时候，AGENT已经结束了
看看问题在哪，是否应该主动补发修正要求（在结束时如果触发）

## 符号支持

### 指数算法

我发现AI有时候喜欢用 a ^ b来表示指数，这个写法目前是不支持的会返回错误，但其实挺常用的我希望我们支持这个写法，降低错误率

### T写法

我注意到AI有这个写法，以下是AI的操作实例（返回错误了，因为不认识@T）
‘{"table_name": "gem_attr", "items": [{"column_name":"base_attr_pct","formula_string":"${gem_base_attr_pct} * (${gem_base_attr_growth} ^ (@T[tier] - 1))"},{"column_name":"crit_rate","formula_string":"${gem_crit_rate_base} * (${gem_crit_rate_growth} ^ (@T[tier] - 1))"},{"column_name":"crit_dmg","formula_string":"${gem_crit_dmg_base} * (${gem_crit_dmg_growth} ^ (@T[tier] - 1))"},{"column_name":"dodge","formula_string":"${gem_dodge_base} * (${gem_dodge_growth} ^ (@T[tier] - 1))"},{"column_name":"spd","formula_string":"${gem_spd_base} * (${gem_spd_growth} ^ (@T[tier] - 1))"}]}’

这里的 @T 写法你要判断一下，是否常用或者应该支持的写法，其含义是否明确，若是就尝试去支持它，若不是就不管了

临时的我还看到有@this的写法，实例：
{"table_name": "cult_res_cost_per_level", "column_name": "cost_qty", "formula_string": "IF(@this[system_id]='hero' AND @this[resource_id]='gold', CEIL(VLOOKUP(@this[level], @@level_growth[level], @@level_growth[exp_to_next]) * ${hero_gold_ratio}), IF(@this[system_id]='equipment' AND @this[resource_id]='equipment_stone', CEIL(${equip_stone_tier1_base} * POWER(${equip_stone_tier_growth}, FLOOR((@this[level]-1)/10)) / 10), IF(@this[system_id]='equipment' AND @this[resource_id]='gold', CEIL(${equip_stone_tier1_base} * POWER(${equip_stone_tier_growth}, FLOOR((@this[level]-1)/10)) / 10) * ${equip_gold_per_stone}, IF(@this[system_id]='talent' AND @this[resource_id]='talent_point', 1, IF(@this[system_id]='reputation' AND @this[resource_id]='reputation_exp', CEIL(${reputation_cost_base} + @this[level] * ${reputation_cost_slope}), IF(@this[system_id]='formation' AND @this[resource_id]='gold', CEIL(${formation_cost_base} + @this[level] * ${formation_cost_slope}), IF(@this[system_id]='mount' AND @this[resource_id]='mount_feed', CEIL(${mount_feed_base} + (@this[level]-29) * ${mount_feed_slope}), IF(@this[system_id]='mount' AND @this[resource_id]='gold', CEIL(${mount_feed_base} + (@this[level]-29) * ${mount_feed_slope}) * ${mount_gold_per_feed}, IF(@this[system_id]='mount_equipment' AND @this[resource_id]='mount_gear_stone', CEIL(${mount_gear_base} + (@this[level]-34) * ${mount_gear_slope}), IF(@this[system_id]='mount_equipment' AND @this[resource_id]='gold', CEIL(${mount_gear_base} + (@this[level]-34) * ${mount_gear_slope}) * ${mount_gear_gold_per_stone}, IF(@this[system_id]='pet' AND @this[resource_id]='pet_feed', CEIL(${pet_feed_base} + (@this[level]-34) * ${pet_feed_slope}), IF(@this[system_id]='pet' AND @this[resource_id]='gold', CEIL(${pet_feed_base} + (@this[level]-34) * ${pet_feed_slope}) * ${pet_gold_per_feed}, IF(@this[system_id]='pet_equipment' AND @this[resource_id]='gold', CEIL(${pet_equip_cost_base} + (@this[level]-59) * ${pet_equip_cost_slope}), NULL)))))))))))"}

同样的原则，你判断是否应该支持

## 关于AGENT错误

AGENT错误在AGENT页面应该浮窗显示而不是显示在顶部（比如网络错误等），顶部有时候看不到

## 关于execute硬顶

我注意到这次已经出发到24轮次硬顶了
大的系统是完全可能超过24轮的，这次又会增加标签注册，轮数还会增加，取消硬顶吧

## 关于工作台的头部校验区域

这个区域有问题，我现在打开城市猎人项目时，顶部238条校验内容直接把页面撑爆了，表格完全看不到
修改一下，这个内容至少要是可收起的
另外对于0条违反的条目就没必要在这里显示了，无意义呀