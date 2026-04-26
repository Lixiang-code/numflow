/** 与 backend `PIPELINE_STEPS`（文档 03）顺序与 id 一致 */
export const PIPELINE_STEP_LABELS: Record<string, string> = {
  environment_global_readme: '整体环境确认与全局 README',
  base_attribute_framework: '基本属性基础框架表',
  gameplay_attribute_scheme: '玩法系统属性方案',
  gameplay_allocation_tables: '玩法系统属性分配表',
  second_order_framework: '基本属性二阶框架表',
  gameplay_attribute_tables: '玩法系统属性表',
  cultivation_resource_design: '养成资源设计',
  cultivation_resource_framework: '养成资源基础框架表',
  cultivation_allocation_tables: '养成资源分配表',
  cultivation_quant_tables: '养成资源定量表',
  gameplay_landing_tables: '玩法系统落地表',
  // step 11 per-system 子步
  'gameplay_landing_tables.equip': '11.装备 落地表',
  'gameplay_landing_tables.gem': '11.宝石 落地表',
  'gameplay_landing_tables.mount': '11.坐骑 落地表',
  'gameplay_landing_tables.wing': '11.翅膀 落地表',
  'gameplay_landing_tables.fashion': '11.时装 落地表',
  'gameplay_landing_tables.dungeon': '11.副本 落地表',
  'gameplay_landing_tables.skill': '11.技能 落地表',
}

export function pipelineStepLabel(stepId: string | null | undefined): string {
  if (!stepId) return '—'
  if (PIPELINE_STEP_LABELS[stepId]) return PIPELINE_STEP_LABELS[stepId]
  // 未注册的子步：根据 ID 推导
  if (stepId.startsWith('gameplay_landing_tables.')) {
    const sub = stepId.slice('gameplay_landing_tables.'.length)
    return `11.${sub} 落地表`
  }
  return stepId
}

/** 初始化 Agent 与「下一步」流水线步骤绑定的预填说明（可一键插入输入框） */
export function getInitAgentPrompt(stepId: string): string {
  const label = pipelineStepLabel(stepId)
  return [
    '【初始化 Agent｜当前流水线步骤】',
    `步骤 ID：${stepId}`,
    `步骤名称：${label}`,
    '',
    '请阅读全局 README 与已有表结构，完成本步交付物：给出建议新建或修改的表名、关键列、数据来源假设。',
    '若信息不足，请列出需要我补充的 2～4 个具体问题；结尾给出「验收清单」小项便于勾选确认。',
  ].join('\n')
}
