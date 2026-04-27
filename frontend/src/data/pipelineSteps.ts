/** 与 backend `PIPELINE_STEPS_BASE`（第3轮精简 6 步）顺序与 id 一致 */
export const PIPELINE_STEP_LABELS: Record<string, string> = {
  environment_global_readme: '环境与全局说明',
  base_attribute_framework: '基础属性框架',
  hp_formula_derivation: 'HP 反推公式',
  gameplay_allocation: '玩法属性分配（matrix）',
  cultivation_resource_framework: '养成资源框架',
  cultivation_allocation: '养成属性分配（matrix）',
  gameplay_landing_tables: '落地表（子系统）',
  // per-system 子步
  'gameplay_landing_tables.equip': '落地：装备',
  'gameplay_landing_tables.gem': '落地：宝石',
  'gameplay_landing_tables.mount': '落地：坐骑',
  'gameplay_landing_tables.wing': '落地：翅膀',
  'gameplay_landing_tables.fashion': '落地：时装',
  'gameplay_landing_tables.dungeon': '落地：副本',
  'gameplay_landing_tables.skill': '落地：技能',
}

export function pipelineStepLabel(stepId: string | null | undefined): string {
  if (!stepId) return '—'
  if (PIPELINE_STEP_LABELS[stepId]) return PIPELINE_STEP_LABELS[stepId]
  // 未注册的子步：根据 ID 推导
  if (stepId.startsWith('gameplay_landing_tables.')) {
    const sub = stepId.slice('gameplay_landing_tables.'.length)
    return `落地：${sub}`
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
