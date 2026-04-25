/**
 * 文档 01 — 游戏系统树 / 子系统 / 属性系统完整数据
 * RPG 回合（MVP）— 其他游戏类型第二阶段
 */

/* ─────────────────────────────────────────────────────────────
   游戏系统树
   ───────────────────────────────────────────────────────────── */
export type RpgTreeNode = {
  id: string
  label: string
  badge?: string       // 右侧灰色徽章，如 "30级开放"
  defaultOn?: boolean
  children?: RpgTreeNode[]
}

export const RPG_GAME_TREE: RpgTreeNode[] = [
  {
    id: 'player',
    label: '玩家',
    defaultOn: true,
    children: [
      {
        id: 'player.concepts',
        label: '概念（角色模式）',
        children: [
          { id: 'player.concepts.hero',  label: '主角（战斗）', defaultOn: true, badge: '1级' },
          { id: 'player.concepts.lord',  label: '领主（不参战，属性加全体）', badge: '1级' },
        ],
      },
      {
        id: 'player.equipment',
        label: '装备',
        defaultOn: true,
        badge: '1级',
        children: [
          { id: 'player.equipment.main_hand',  label: '主手武器', defaultOn: true },
          { id: 'player.equipment.off_hand',   label: '副手武器', defaultOn: true },
          { id: 'player.equipment.armor',      label: '铠甲', defaultOn: true },
          { id: 'player.equipment.pants',      label: '下装', defaultOn: true },
          { id: 'player.equipment.shoes',      label: '鞋子', defaultOn: true },
          { id: 'player.equipment.accessory',  label: '饰品', defaultOn: true },
        ],
      },
      { id: 'player.artifact',      label: '神器',   badge: '50/70/90级' },
      { id: 'player.formation',     label: '阵法',   badge: '20级' },
      {
        id: 'player.others',
        label: '其他养成',
        children: [
          { id: 'player.others.talent',     label: '天赋',   badge: '25级' },
          { id: 'player.others.reputation', label: '声望',   badge: '10级' },
          { id: 'player.others.title',      label: '头衔',   badge: '28级' },
        ],
      },
      {
        id: 'player.consumable',
        label: '消耗品',
        badge: '10级',
        children: [
          { id: 'player.consumable.attr_potion', label: '属性药水', defaultOn: true },
          { id: 'player.consumable.hp_potion',   label: '治疗药水', defaultOn: true },
          { id: 'player.consumable.mp_potion',   label: '魔法药水' },
        ],
      },
    ],
  },
  {
    id: 'mount',
    label: '坐骑',
    badge: '30级',
    children: [
      { id: 'mount.equipment', label: '坐骑装备' },
    ],
  },
  {
    id: 'pet',
    label: '宠物',
    badge: '15级',
    children: [
      { id: 'pet.equipment', label: '宠物装备' },
    ],
  },
  {
    id: 'hero_partner',
    label: '英雄 / 伙伴',
    badge: '主角22级 / 领主1级',
    children: [
      { id: 'hero_partner.equipment', label: '英雄装备' },
    ],
  },
  {
    id: 'world',
    label: '世界 / PVE',
    children: [
      { id: 'world.stage',   label: '关卡' },
      { id: 'world.monster', label: '怪物' },
      { id: 'world.boss',    label: 'Boss' },
    ],
  },
  {
    id: 'economy',
    label: '经济 / 养成',
    children: [
      { id: 'economy.currency', label: '货币' },
      { id: 'economy.shop',     label: '商店' },
      { id: 'economy.gacha',    label: '抽卡' },
    ],
  },
]

/* ─────────────────────────────────────────────────────────────
   子系统维度（文档 01 § 游戏系统 — 子系统列表）
   ───────────────────────────────────────────────────────────── */
export const SUBSYSTEM_OPTIONS = [
  { id: 'base_attrs', label: '基础属性', defaultOn: true  },
  { id: 'upgrade',    label: '升级',     defaultOn: true  },
  { id: 'gem',        label: '宝石',     defaultOn: false },
  { id: 'refine',     label: '增幅',     defaultOn: false },
  { id: 'star',       label: '升星',     defaultOn: false },
  { id: 'tier',       label: '升阶',     defaultOn: false },
  { id: 'quality',    label: '品质',     defaultOn: false },
  { id: 'skill',      label: '技能',     defaultOn: false },
  { id: 'enchant',    label: '附魔',     defaultOn: false },
  { id: 'reforgee',   label: '洗练',     defaultOn: false },
  { id: 'atlas',      label: '图鉴',     defaultOn: false },
] as const

/** 世界/PVE系统（关卡/怪物/Boss）可用的子系统维度 */
export const WORLD_SUBSYSTEM_OPTIONS = [
  { id: 'base_attrs', label: '基础属性（HP/ATK/DEF等）',  defaultOn: true  },
  { id: 'skill',      label: '技能',                       defaultOn: false },
  { id: 'tier',       label: '难度阶层',                   defaultOn: false },
  { id: 'drop',       label: '掉落设计',                   defaultOn: false },
  { id: 'behavior',   label: '行为模式',                   defaultOn: false },
] as const

/** 经济系统（货币/商店/抽卡）可用的子系统维度 */
export const ECONOMY_SUBSYSTEM_OPTIONS = [
  { id: 'currency',    label: '货币体系',    defaultOn: true  },
  { id: 'pricing',     label: '定价策略',    defaultOn: false },
  { id: 'daily_limit', label: '日产/日限',   defaultOn: false },
  { id: 'exchange',    label: '兑换率',      defaultOn: false },
  { id: 'pool',        label: '奖池设计',    defaultOn: false },
  { id: 'pity',        label: '保底机制',    defaultOn: false },
  { id: 'sink',        label: '消耗回收',    defaultOn: false },
] as const

/** 根据系统路径返回对应的子系统选项列表 */
export function getSubsystemOptionsForPath(pathId: string): ReadonlyArray<{ id: string; label: string; defaultOn: boolean }> {
  if (
    pathId === 'world' ||
    pathId.startsWith('world.')
  ) return WORLD_SUBSYSTEM_OPTIONS
  if (
    pathId === 'economy' ||
    pathId.startsWith('economy.')
  ) return ECONOMY_SUBSYSTEM_OPTIONS
  // 默认（玩家/装备/宠物/坐骑/英雄等养成系统）
  return SUBSYSTEM_OPTIONS
}

/** 返回指定路径的默认已选子系统 id 列表 */
export function defaultSubsystemsForPath(pathId: string): string[] {
  return getSubsystemOptionsForPath(pathId)
    .filter((o) => o.defaultOn)
    .map((o) => o.id)
}

export type SubsystemId = (typeof SUBSYSTEM_OPTIONS)[number]['id']

/* ─────────────────────────────────────────────────────────────
   属性树（文档 01 § 属性系统）
   ───────────────────────────────────────────────────────────── */
export type AttrNode = {
  id: string
  label: string
  defaultOn?: boolean
  /** true = 该游戏类型下强制选中，不可取消（如即时制必选的攻速间隔）*/
  requiredFor?: string[]   // game_type ids
  /** 仅在指定游戏类型下显示/生效 */
  onlyFor?: string[]
  tooltip?: string
  children?: AttrNode[]
}

export type AttrGroup = {
  id: string
  label: string
  nodes: AttrNode[]
}

const AMP_TOOLTIP = '多级增幅相互乘算，内部加算。\n最终值 = 基础 × (1+增幅) × (1+增幅+) × (1+增幅++)'

export const ATTR_GROUPS: AttrGroup[] = [
  {
    id: 'basic',
    label: '基础属性',
    nodes: [
      {
        id: 'hp', label: '生命', defaultOn: true,
        children: [
          { id: 'hp_amp',   label: '生命增幅',    defaultOn: true,  tooltip: AMP_TOOLTIP },
          { id: 'hp_amp2',  label: '生命增幅+',   defaultOn: false, tooltip: AMP_TOOLTIP },
          { id: 'hp_amp3',  label: '生命增幅++',  defaultOn: false, tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'atk', label: '攻击', defaultOn: true,
        children: [
          { id: 'atk_amp',  label: '攻击增幅',    defaultOn: true,  tooltip: AMP_TOOLTIP },
          { id: 'atk_amp2', label: '攻击增幅+',   defaultOn: false, tooltip: AMP_TOOLTIP },
          { id: 'atk_amp3', label: '攻击增幅++',  defaultOn: false, tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'def', label: '防御', defaultOn: true,
        children: [
          { id: 'def_amp',  label: '防御增幅',    defaultOn: true,  tooltip: AMP_TOOLTIP },
          { id: 'def_amp2', label: '防御增幅+',   defaultOn: false, tooltip: AMP_TOOLTIP },
          { id: 'def_amp3', label: '防御增幅++',  defaultOn: false, tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'spd', label: '速度（回合制）', defaultOn: true, onlyFor: ['rpg_turn'],
        children: [
          { id: 'spd_amp',  label: '速度增幅',    defaultOn: true,  tooltip: AMP_TOOLTIP },
          { id: 'spd_amp2', label: '速度增幅+',   defaultOn: false, tooltip: AMP_TOOLTIP },
          { id: 'spd_amp3', label: '速度增幅++',  defaultOn: false, tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'move_spd', label: '移动速度（即时制）', onlyFor: ['rpg_realtime'],
        children: [
          { id: 'move_spd_amp', label: '移动速度增幅', tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'atk_spd', label: '攻击速度（即时制）', onlyFor: ['rpg_realtime'],
        children: [
          { id: 'atk_spd_amp', label: '攻击速度增幅', tooltip: AMP_TOOLTIP },
        ],
      },
      {
        id: 'base_atk_interval', label: '基础攻击间隔（即时制）',
        onlyFor: ['rpg_realtime'], requiredFor: ['rpg_realtime'],
      },
      {
        id: 'min_atk_interval', label: '最低攻击间隔（即时制）',
        onlyFor: ['rpg_realtime'], requiredFor: ['rpg_realtime'],
      },
    ],
  },
  {
    id: 'advanced',
    label: '高级属性',
    nodes: [
      {
        id: 'crit_rate', label: '暴击率', defaultOn: true,
        children: [
          {
            id: 'crit_dmg', label: '暴击伤害', defaultOn: true,
            tooltip: '默认2倍；勾选后可养成',
          },
          {
            id: 'crit2',
            label: '二级暴击',
            children: [
              {
                id: 'crit2_dmg', label: '二级暴击伤害', defaultOn: true,
                tooltip: '默认2倍；可与一级暴击同时触发，触发后暴击伤害乘算',
              },
            ],
          },
        ],
      },
      { id: 'anti_crit',     label: '抗暴击' },
      {
        id: 'anti_crit_dmg', label: '抗暴击伤害',
        children: [
          {
            id: 'min_crit_dmg', label: '最低暴击伤害（全局参数）', defaultOn: true,
            tooltip: '勾选抗暴击伤害时必选。默认1.1，即对抗后暴击伤害不低于1.1倍',
          },
        ],
      },
      {
        id: 'block_rate', label: '格挡率', defaultOn: true,
        children: [
          {
            id: 'block_effect', label: '格挡效果',
            tooltip: '默认格挡时承受50%伤害。使用收敛函数计算',
          },
        ],
      },
      {
        id: 'anti_block', label: '反格挡率',
        children: [
          {
            id: 'anti_block_effect', label: '反格挡效果',
            tooltip: '降低受击方格挡效果，但受击方格挡后受到伤害不超过原伤害90%',
          },
        ],
      },
      { id: 'ignore_def', label: '忽视防御', tooltip: '按高级→低级逐级抵扣防御增幅；最低级增幅可进入负数' },
      {
        id: 'dodge', label: '闪避率',
        children: [
          { id: 'max_dodge_cultivate', label: '最大闪避率（可养成）', tooltip: '默认初始50%，可养成' },
          { id: 'max_dodge_global',    label: '最大闪避率（全局参数）', defaultOn: true, tooltip: '全局最大闪避率，默认95%' },
        ],
      },
      { id: 'hit', label: '命中率', tooltip: '最终命中率 = 1 + 攻方命中率 - 守方闪避率' },
      {
        id: 'dmg_amp_universal', label: '通用伤害加深', defaultOn: true,
        children: [
          { id: 'dmg_amp_normal',  label: '普攻伤害加深' },
          { id: 'dmg_amp_skill',   label: '技能伤害加深' },
          { id: 'dmg_amp_phys',    label: '物理伤害加深' },
          { id: 'dmg_amp_magic',   label: '魔法伤害加深' },
          { id: 'dmg_amp_minion',  label: '对小怪伤害加深' },
          { id: 'dmg_amp_boss',    label: '对BOSS伤害加深' },
        ],
      },
      {
        id: 'dmg_red_universal', label: '通用受伤减免', defaultOn: true,
        children: [
          { id: 'dmg_red_normal', label: '普攻受击减伤' },
          { id: 'dmg_red_skill',  label: '技能受击减伤' },
          { id: 'dmg_red_phys',   label: '物理受击减伤' },
          { id: 'dmg_red_magic',  label: '魔法受击减伤' },
          { id: 'dmg_red_minion', label: '小怪受击减伤' },
          { id: 'dmg_red_boss',   label: 'BOSS受击减伤' },
          {
            id: 'max_single_red', label: '最大单级减伤（全局参数）', defaultOn: true,
            tooltip: '对抗后若单级减伤超过95%，改为95%',
          },
        ],
      },
    ],
  },
]

/* ─────────────────────────────────────────────────────────────
   Draft 类型
   ───────────────────────────────────────────────────────────── */
export type GameSystemsDraft = {
  checkedPaths: string[]
  subsystemsByPath: Record<string, string[]>
  /** 自定义系统：每项有唯一 id（custom_<timestamp>）、label、parentId */
  customNodes: Array<{ id: string; label: string; parentId: string | null }>
  /** 每个系统路径下的自定义子系统维度 */
  customSubsByPath: Record<string, Array<{ id: string; label: string }>>
}

export type AttributesDraft = {
  /** 扁平选中的属性 id 列表 */
  selectedAttrs: string[]
  /** 对抗属性等级化（独立全局开关） */
  combatLevelized: boolean
  /** 自定义属性节点（顶级 parentId=null，次级 parentId=父id） */
  customAttrs: Array<{ id: string; label: string; parentId: string | null }>
}

/* ─────────────────────────────────────────────────────────────
   工具函数
   ───────────────────────────────────────────────────────────── */
function collectAllIds(nodes: RpgTreeNode[]): Set<string> {
  const s = new Set<string>()
  function walk(n: RpgTreeNode) {
    s.add(n.id)
    for (const c of n.children ?? []) walk(c)
  }
  for (const n of nodes) walk(n)
  return s
}
const ALL_TREE_IDS = collectAllIds(RPG_GAME_TREE)

function walkDefaultChecked(nodes: RpgTreeNode[], out: Set<string>) {
  for (const n of nodes) {
    if (n.defaultOn) out.add(n.id)
    if (n.children) walkDefaultChecked(n.children, out)
  }
}

export function defaultGameSystems(): GameSystemsDraft {
  const checked = new Set<string>()
  walkDefaultChecked(RPG_GAME_TREE, checked)
  const checkedPaths = [...checked]
  const subsystemsByPath: Record<string, string[]> = {}
  for (const id of checkedPaths) subsystemsByPath[id] = defaultSubsystemsForPath(id)
  return { checkedPaths, subsystemsByPath, customNodes: [], customSubsByPath: {} }
}

/** 收集属性树中默认选中的节点 id */
function collectDefaultAttrs(nodes: AttrNode[], gameType: string, out: Set<string>) {
  for (const n of nodes) {
    if (n.onlyFor && !n.onlyFor.includes(gameType)) continue
    if (n.requiredFor?.includes(gameType) || n.defaultOn) out.add(n.id)
    if (n.children) collectDefaultAttrs(n.children, gameType, out)
  }
}

export function defaultAttributes(gameType = 'rpg_turn'): AttributesDraft {
  const sel = new Set<string>()
  for (const g of ATTR_GROUPS) collectDefaultAttrs(g.nodes, gameType, sel)
  return { selectedAttrs: [...sel], combatLevelized: false, customAttrs: [] }
}

export function pruneUnknownPaths(draft: GameSystemsDraft): GameSystemsDraft {
  const validPaths = draft.checkedPaths.filter((id) => ALL_TREE_IDS.has(id))
  const subsystemsByPath: Record<string, string[]> = {}
  for (const id of validPaths) {
    subsystemsByPath[id] = draft.subsystemsByPath[id] ?? defaultSubsystemsForPath(id)
  }
  return {
    checkedPaths: validPaths,
    subsystemsByPath,
    customNodes: draft.customNodes ?? [],
    customSubsByPath: draft.customSubsByPath ?? {},
  }
}

/** 兼容旧版 basics/extras 格式升级 */
export function migrateAttributesDraft(raw: unknown): AttributesDraft {
  if (!raw || typeof raw !== 'object') return defaultAttributes()
  const d = raw as Record<string, unknown>
  // 新格式
  if (Array.isArray(d.selectedAttrs)) {
    return {
      selectedAttrs: d.selectedAttrs as string[],
      combatLevelized: Boolean(d.combatLevelized),
      customAttrs: Array.isArray(d.customAttrs) ? (d.customAttrs as AttributesDraft['customAttrs']) : [],
    }
  }
  // 旧格式 {basics, extras}
  const basics = Array.isArray(d.basics) ? (d.basics as string[]) : []
  const extras = Array.isArray(d.extras) ? (d.extras as string[]) : []
  return { selectedAttrs: [...basics, ...extras], combatLevelized: false, customAttrs: [] }
}

/** 找树节点 label（用于展示） */
function findLabel(nodes: RpgTreeNode[], id: string): string | undefined {
  for (const n of nodes) {
    if (n.id === id) return n.label
    if (n.children) {
      const r = findLabel(n.children, id)
      if (r !== undefined) return r
    }
  }
  return undefined
}

export function getTreeNodeLabel(id: string): string {
  return findLabel(RPG_GAME_TREE, id) ?? id
}
