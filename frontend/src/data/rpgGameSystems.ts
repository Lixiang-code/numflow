/**
 * 对齐文档 01「游戏系统树」「属性系统」的简化配置（RPG 回合 MVP）。
 * 路径 id 用于持久化到 project_settings。
 */

export type RpgTreeNode = {
  id: string
  label: string
  defaultOn?: boolean
  children?: RpgTreeNode[]
}

/** 子系统维度（勾选后写入 game_systems.subsystems_by_path[nodeId]） */
export const SUBSYSTEM_OPTIONS = [
  { id: 'base_attrs', label: '基础属性', defaultOn: true },
  { id: 'upgrade', label: '升级', defaultOn: true },
  { id: 'gem_socket', label: '宝石/孔位', defaultOn: false },
  { id: 'refine', label: '精炼/强化', defaultOn: false },
  { id: 'skill', label: '技能', defaultOn: false },
  { id: 'drop', label: '掉落/产出', defaultOn: false },
] as const

export const RPG_GAME_TREE: RpgTreeNode[] = [
  {
    id: 'player',
    label: '玩家',
    defaultOn: true,
    children: [
      {
        id: 'player.concepts',
        label: '概念',
        children: [
          { id: 'player.concepts.hero', label: '主角（战斗）', defaultOn: true },
          { id: 'player.concepts.lord', label: '领主', defaultOn: false },
        ],
      },
      { id: 'player.equipment', label: '装备', defaultOn: true },
      { id: 'player.artifact', label: '神器', defaultOn: false },
      { id: 'player.mount', label: '坐骑', defaultOn: false },
      { id: 'player.pet', label: '宠物', defaultOn: false },
      { id: 'player.hero_partner', label: '英雄伙伴', defaultOn: false },
      { id: 'player.consumable', label: '消耗品', defaultOn: false },
    ],
  },
  {
    id: 'world',
    label: '世界 / PVE',
    defaultOn: false,
    children: [
      { id: 'world.stage', label: '关卡', defaultOn: false },
      { id: 'world.monster', label: '怪物', defaultOn: false },
      { id: 'world.boss', label: 'Boss', defaultOn: false },
    ],
  },
  {
    id: 'economy',
    label: '经济 / 养成',
    defaultOn: false,
    children: [
      { id: 'economy.currency', label: '货币', defaultOn: false },
      { id: 'economy.shop', label: '商店', defaultOn: false },
      { id: 'economy.gacha', label: '抽卡', defaultOn: false },
    ],
  },
]

export type GameSystemsDraft = {
  checkedPaths: string[]
  /** 每个已选节点 id → 启用的子系统 id 列表 */
  subsystemsByPath: Record<string, string[]>
}

export type AttributesDraft = {
  /** 基础战斗属性 id */
  basics: string[]
  /** 进阶 / 概率类 */
  extras: string[]
}

export const ATTRIBUTE_BASIC = [
  { id: 'hp', label: '生命' },
  { id: 'atk', label: '攻击' },
  { id: 'def', label: '防御' },
  { id: 'spd', label: '速度' },
  { id: 'hp_amp', label: '生命加成' },
  { id: 'atk_amp', label: '攻击加成' },
  { id: 'def_amp', label: '防御加成' },
  { id: 'spd_amp', label: '速度加成' },
] as const

export const ATTRIBUTE_EXTRA = [
  { id: 'crit_rate', label: '暴击率' },
  { id: 'crit_dmg', label: '暴击伤害' },
  { id: 'hit', label: '命中' },
  { id: 'dodge', label: '闪避' },
  { id: 'block', label: '格挡' },
  { id: 'tenacity', label: '韧性' },
  { id: 'heal_amp', label: '治疗加成' },
  { id: 'dmg_amp', label: '伤害加成' },
] as const

function collectIds(node: RpgTreeNode, acc: string[]): void {
  acc.push(node.id)
  for (const c of node.children || []) collectIds(c, acc)
}

function allTreeIds(): string[] {
  const acc: string[] = []
  for (const n of RPG_GAME_TREE) collectIds(n, acc)
  return acc
}

const ALL_IDS = new Set(allTreeIds())

function defaultSubsystems(): string[] {
  return SUBSYSTEM_OPTIONS.filter((s) => s.defaultOn).map((s) => s.id)
}

function walkDefaults(node: RpgTreeNode, checked: Set<string>): void {
  if (node.defaultOn) checked.add(node.id)
  for (const c of node.children || []) walkDefaults(c, checked)
}

export function defaultGameSystems(): GameSystemsDraft {
  const checked = new Set<string>()
  for (const n of RPG_GAME_TREE) walkDefaults(n, checked)
  const checkedPaths = [...checked]
  const subsystemsByPath: Record<string, string[]> = {}
  const sub = defaultSubsystems()
  for (const id of checkedPaths) subsystemsByPath[id] = [...sub]
  return { checkedPaths, subsystemsByPath }
}

export function defaultAttributes(): AttributesDraft {
  return {
    basics: ATTRIBUTE_BASIC.map((a) => a.id),
    extras: [],
  }
}

export function pruneUnknownPaths(draft: GameSystemsDraft): GameSystemsDraft {
  const checkedPaths = draft.checkedPaths.filter((id) => ALL_IDS.has(id))
  const subsystemsByPath: Record<string, string[]> = {}
  for (const id of checkedPaths) {
    subsystemsByPath[id] = draft.subsystemsByPath[id] ?? defaultSubsystems()
  }
  return { checkedPaths, subsystemsByPath }
}
