/** 与 backend `PIPELINE_STEPS_BASE` 顺序与 id 一致 */
export const PIPELINE_STEP_LABELS: Record<string, string> = {
  environment_global_readme: '环境与全局说明',
  gameplay_planning: '玩法规划',
  base_attribute_framework: '基础属性框架',
  hp_formula_derivation: 'HP 反推公式',
  gameplay_allocation: '玩法属性分配（matrix）',
  cultivation_resource_framework: '养成资源框架',
  cultivation_allocation: '养成属性分配（matrix）',
  // gameplay_table.* 步骤由后端动态生成
}

export function pipelineStepLabel(stepId: string | null | undefined): string {
  if (!stepId) return '—'
  if (PIPELINE_STEP_LABELS[stepId]) return PIPELINE_STEP_LABELS[stepId]
  // 动态玩法表步骤
  if (stepId.startsWith('gameplay_table.')) {
    const sub = stepId.slice('gameplay_table.'.length)
    return `落地：${sub}`
  }
  // 向后兼容旧的 gameplay_landing_tables.* 前缀
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
