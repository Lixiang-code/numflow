import {
  useCallback, useEffect, useMemo, useState,
  type FormEvent, type ReactNode,
} from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'
import {
  ATTR_GROUPS,
  RPG_GAME_TREE,
  defaultAttributes,
  defaultGameSystems,
  defaultSubsystemsForPath,
  getSubsystemOptionsForPath,
  getTreeNodeLabel,
  migrateAttributesDraft,
  pruneUnknownPaths,
  type AttrNode,
  type AttributesDraft,
  type GameSystemsDraft,
  type RpgTreeNode,
} from '../data/rpgGameSystems'

/* ── 持久化 key ──────────────────────────────────────────────── */
const DRAFT_KEY = 'numflow_new_project_draft_v3'

/* ── CoreDraft ────────────────────────────────────────────── */
type CoreDraft = {
  game_type: string
  business_model: string
  theme: string
  magnitude: string
  magnitude_custom: string
  inflation_mode: string
  inflation_rate: string
  inflation_rate_custom: string
  level_cap: number
  lifecycle_days: number
  defense_formula: string
  play_pace: string
  combat_rhythm: string
  combat_rhythm_custom: string
}

const defaultCore = (): CoreDraft => ({
  game_type: 'rpg_turn',
  business_model: 'item_mall',
  theme: '',
  magnitude: '10',
  magnitude_custom: '100',
  inflation_mode: 'mul',
  inflation_rate: 'mid',
  inflation_rate_custom: '0.3',
  level_cap: 200,
  lifecycle_days: 60,
  defense_formula: 'subtract',
  play_pace: 'standard',
  combat_rhythm: 'mid',
  combat_rhythm_custom: '5',
})

type DraftBundle = {
  name: string
  promptText: string
  core: CoreDraft
  tab: 'options' | 'prompt'
  wizardStep: number
  gameSystems: GameSystemsDraft
  attributes: AttributesDraft
}

function loadDraft(): DraftBundle {
  const fallback: DraftBundle = {
    name: '', promptText: '',
    core: defaultCore(), tab: 'options', wizardStep: 0,
    gameSystems: defaultGameSystems(), attributes: defaultAttributes(),
  }
  try {
    const raw = localStorage.getItem(DRAFT_KEY)
    if (!raw) return fallback
    const d = JSON.parse(raw) as Partial<DraftBundle> & Record<string, unknown>
    const tab: 'options' | 'prompt' = d.tab === 'prompt' ? 'prompt' : 'options'
    const wizardStep = typeof d.wizardStep === 'number' ? Math.min(2, Math.max(0, d.wizardStep)) : 0
    return {
      name:       d.name ?? '',
      promptText: d.promptText ?? '',
      core:       d.core ? { ...defaultCore(), ...d.core } : defaultCore(),
      tab, wizardStep,
      gameSystems: d.gameSystems ? pruneUnknownPaths(d.gameSystems as GameSystemsDraft) : defaultGameSystems(),
      attributes:  migrateAttributesDraft(d.attributes),
    }
  } catch { return fallback }
}

/* ─────────────────────────────────────────────────────────────
   子组件：顶层自定义系统追加按钮
   ───────────────────────────────────────────────────────────── */
function TopLevelCustomAdder({ onAdd }: { onAdd: (label: string) => void }) {
  const [adding, setAdding] = useState(false)
  const [val, setVal] = useState('')
  function commit() {
    const t = val.trim()
    if (t) { onAdd(t); setVal(''); setAdding(false) }
  }
  if (adding) {
    return (
      <div className="custom-add-row" style={{ paddingLeft: '6px', marginTop: 4 }}>
        <input autoFocus placeholder="输入顶级系统名称…" value={val}
          onChange={(e) => setVal(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') commit()
            if (e.key === 'Escape') setAdding(false)
          }} />
        <button type="button" className="add-btn" onClick={commit}>确定</button>
        <button type="button" className="add-btn"
          style={{ background: '#efefef', borderColor: '#ccc', color: '#666' }}
          onClick={() => setAdding(false)}>取消</button>
      </div>
    )
  }
  return (
    <div style={{ paddingLeft: '6px', paddingBottom: '4px', paddingTop: '4px' }}>
      <button type="button"
        style={{ border: '1px dashed #c6e0b4', background: 'transparent', color: '#217346',
          fontSize: '0.8rem', padding: '0.25rem 0.7rem', borderRadius: '4px', cursor: 'pointer' }}
        onClick={() => setAdding(true)}>
        + 自定义顶级系统
      </button>
    </div>
  )
}

/* ─────────────────────────────────────────────────────────────
   子组件：游戏系统树节点（步骤 2）
   ───────────────────────────────────────────────────────────── */
type CustomNode = GameSystemsDraft['customNodes'][number]

function TreeNodeRow({
  node, depth, checked, onToggle, customNodes, onAddCustom, onRemoveCustom,
}: {
  node: RpgTreeNode
  depth: number
  checked: Set<string>
  onToggle: (id: string, on: boolean) => void
  customNodes: CustomNode[]
  onAddCustom: (parentId: string, label: string) => void
  onRemoveCustom: (id: string) => void
}): ReactNode {
  const [expanded, setExpanded] = useState(true)
  const [addingCustom, setAddingCustom] = useState(false)
  const [customInput, setCustomInput] = useState('')
  const hasChildren = (node.children?.length ?? 0) > 0
  const myCustomChildren = customNodes.filter((c) => c.parentId === node.id)

  function commitCustom() {
    const t = customInput.trim()
    if (t) {
      onAddCustom(node.id, t)
      setCustomInput('')
      setAddingCustom(false)
    }
  }

  return (
    <div>
      <div className="tree-row" style={{ paddingLeft: `${depth * 14 + 6}px` }}>
        <input
          type="checkbox"
          checked={checked.has(node.id)}
          onChange={(e) => onToggle(node.id, e.target.checked)}
        />
        <span
          className="node-label"
          style={{ cursor: hasChildren ? 'pointer' : 'default' }}
          onClick={() => hasChildren && setExpanded((v) => !v)}
        >
          {node.label}
        </span>
        {node.badge && <span className="node-badge">{node.badge}</span>}
        {hasChildren && (
          <span
            style={{ fontSize: '0.7rem', color: '#999', cursor: 'pointer', marginLeft: 2 }}
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? '▾' : '▸'}
          </span>
        )}
      </div>

      {expanded && hasChildren && (
        <>
          {node.children!.map((c) => (
            <TreeNodeRow
              key={c.id} node={c} depth={depth + 1}
              checked={checked} onToggle={onToggle}
              customNodes={customNodes}
              onAddCustom={onAddCustom}
              onRemoveCustom={onRemoveCustom}
            />
          ))}
          {myCustomChildren.map((cn) => (
            <div key={cn.id} className="tree-row" style={{ paddingLeft: `${(depth + 1) * 14 + 6}px` }}>
              <input
                type="checkbox"
                checked={checked.has(cn.id)}
                onChange={(e) => onToggle(cn.id, e.target.checked)}
              />
              <span className="node-label" style={{ fontStyle: 'italic' }}>{cn.label}</span>
              <span className="node-badge" style={{ color: '#217346' }}>自定义</span>
              <button
                type="button"
                style={{ marginLeft: 4, border: 'none', background: 'none', cursor: 'pointer', color: '#c00', fontSize: '0.7rem' }}
                onClick={() => onRemoveCustom(cn.id)}
                title="删除"
              >x</button>
            </div>
          ))}
          {addingCustom ? (
            <div className="custom-add-row" style={{ paddingLeft: `${(depth + 1) * 14 + 6}px` }}>
              <input
                autoFocus
                placeholder="输入系统名称…"
                value={customInput}
                onChange={(e) => setCustomInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') commitCustom()
                  if (e.key === 'Escape') setAddingCustom(false)
                }}
              />
              <button type="button" className="add-btn" onClick={commitCustom}>确定</button>
              <button type="button" className="add-btn"
                style={{ background: '#efefef', borderColor: '#ccc', color: '#666' }}
                onClick={() => setAddingCustom(false)}>取消</button>
            </div>
          ) : (
            <div style={{ paddingLeft: `${(depth + 1) * 14 + 6}px` }}>
              <button
                type="button"
                style={{ border: '1px dashed #c6e0b4', background: 'transparent', color: '#217346',
                  fontSize: '0.75rem', marginTop: 2, padding: '0.2rem 0.5rem',
                  borderRadius: '4px', cursor: 'pointer' }}
                onClick={() => setAddingCustom(true)}>
                + 自定义子系统
              </button>
            </div>
          )}
        </>
      )}
    </div>
  )
}

/* ─────────────────────────────────────────────────────────────
   子组件：步骤2 子系统展开块
   ───────────────────────────────────────────────────────────── */
type CustomSub = { id: string; label: string }

function SubsystemBlock({
  pathId, pathLabel, subs, customSubs, onToggle, onAddCustomSub, onRemoveCustomSub,
}: {
  pathId: string
  pathLabel: string
  subs: string[]
  customSubs: CustomSub[]
  onToggle: (subId: string, on: boolean) => void
  onAddCustomSub: (label: string) => void
  onRemoveCustomSub: (id: string) => void
}) {
  const [open, setOpen] = useState(false)
  const [adding, setAdding] = useState(false)
  const subsystemOpts = getSubsystemOptionsForPath(pathId)
  const [customInput, setCustomInput] = useState('')

  function commitCustomSub() {
    const t = customInput.trim()
    if (t) { onAddCustomSub(t); setCustomInput(''); setAdding(false) }
  }

  const totalOptions = subsystemOpts.length + customSubs.length

  return (
    <div className="subsystem-expand">
      <div className={`subsystem-expand-head${open ? ' open' : ''}`} onClick={() => setOpen((v) => !v)}>
        <span className="chevron">▶</span>
        <span>{pathLabel}</span>
        <span style={{ marginLeft: 'auto', fontWeight: 400, fontSize: '0.75rem', color: '#999' }}>
          已选 {subs.length}/{totalOptions}
        </span>
      </div>
      {open && (
        <div className="subsystem-grid">
          {subsystemOpts.map((opt) => (
            <label key={opt.id}>
              <input
                type="checkbox"
                checked={subs.includes(opt.id)}
                onChange={(e) => onToggle(opt.id, e.target.checked)}
              />
              {opt.label}
            </label>
          ))}
          {customSubs.map((cs) => (
            <label key={cs.id} style={{ color: '#217346', fontStyle: 'italic' }}>
              <input
                type="checkbox"
                checked={subs.includes(cs.id)}
                onChange={(e) => onToggle(cs.id, e.target.checked)}
              />
              {cs.label}
              <button
                type="button"
                onClick={(e) => { e.preventDefault(); onRemoveCustomSub(cs.id) }}
                style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#c00', fontSize: '0.7rem', marginLeft: 2, padding: 0 }}
                title="删除"
              >×</button>
            </label>
          ))}
          {adding ? (
            <div className="custom-add-row" style={{ gridColumn: '1 / -1', marginTop: 4 }}>
              <input
                autoFocus
                placeholder="自定义子系统名称…"
                value={customInput}
                onChange={(e) => setCustomInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') commitCustomSub()
                  if (e.key === 'Escape') setAdding(false)
                }}
              />
              <button type="button" className="add-btn" onClick={commitCustomSub}>确定</button>
              <button type="button" className="add-btn"
                style={{ background: '#efefef', borderColor: '#ccc', color: '#666' }}
                onClick={() => setAdding(false)}>取消</button>
            </div>
          ) : (
            <button
              type="button"
              style={{
                gridColumn: '1 / -1', border: '1px dashed #c6e0b4', background: 'transparent',
                color: '#217346', fontSize: '0.75rem', padding: '0.2rem 0.5rem',
                borderRadius: '4px', cursor: 'pointer', marginTop: 4,
              }}
              onClick={() => setAdding(true)}>
              + 自定义维度
            </button>
          )}
        </div>
      )}
    </div>
  )
}

/* ─────────────────────────────────────────────────────────────
   子组件：自定义属性追加按钮
   ───────────────────────────────────────────────────────────── */
function AttrCustomAdder({ label, indent, onAdd }: { label: string; indent: number; onAdd: (l: string) => void }) {
  const [adding, setAdding] = useState(false)
  const [val, setVal] = useState('')
  function commit() {
    const t = val.trim()
    if (t) { onAdd(t); setVal(''); setAdding(false) }
  }
  if (adding) {
    return (
      <div className="custom-add-row" style={{ paddingLeft: indent }}>
        <input autoFocus placeholder="属性名称…" value={val}
          onChange={(e) => setVal(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') setAdding(false) }} />
        <button type="button" className="add-btn" onClick={commit}>确定</button>
        <button type="button" className="add-btn"
          style={{ background: '#efefef', borderColor: '#ccc', color: '#666' }}
          onClick={() => setAdding(false)}>取消</button>
      </div>
    )
  }
  return (
    <div style={{ paddingLeft: indent, paddingBottom: 3, paddingTop: 3 }}>
      <button type="button"
        style={{ border: '1px dashed #c6e0b4', background: 'transparent', color: '#217346',
          fontSize: '0.75rem', padding: '0.2rem 0.5rem', borderRadius: '4px', cursor: 'pointer' }}
        onClick={() => setAdding(true)}>{label}</button>
    </div>
  )
}

/* ─────────────────────────────────────────────────────────────
   子组件：属性树节点（步骤 3）
   ───────────────────────────────────────────────────────────── */
function AttrNodeRow({
  node, depth, selected, onToggle, gameType,
}: {
  node: AttrNode
  depth: number
  selected: Set<string>
  onToggle: (id: string, on: boolean) => void
  gameType: string
}): ReactNode {
  const applicable = !node.onlyFor || node.onlyFor.includes(gameType)
  const required   = node.requiredFor?.includes(gameType) ?? false
  const isChecked  = selected.has(node.id) || required
  const showChildren = isChecked && (node.children?.length ?? 0) > 0

  if (!applicable && !required) return null

  function handleChange(on: boolean) {
    if (required) return
    onToggle(node.id, on)
  }

  const depthClass = depth === 1 ? ' child-1' : depth === 2 ? ' child-2' : ''

  return (
    <>
      <div
        className={`attr-row${depthClass}${required ? ' required' : ''}${!applicable ? ' disabled' : ''}`}
        style={{ paddingLeft: `${depth * 14 + 8}px` }}
        onClick={() => handleChange(!isChecked)}
      >
        <input
          type="checkbox"
          checked={isChecked}
          disabled={required}
          onChange={(e) => handleChange(e.target.checked)}
          onClick={(e) => e.stopPropagation()}
        />
        <span className="attr-label">
          {node.label}{required ? ' *' : ''}
        </span>
        {node.tooltip && (
          <span className="tooltip-wrap">
            <button
              type="button"
              className="tooltip-trigger"
              onClick={(e) => e.stopPropagation()}
            >?</button>
            <span className="tooltip-box">{node.tooltip}</span>
          </span>
        )}
      </div>
      {showChildren && node.children?.map((child) => (
        <AttrNodeRow
          key={child.id} node={child} depth={depth + 1}
          selected={selected} onToggle={onToggle}
          gameType={gameType}
        />
      ))}
    </>
  )
}

/* ─────────────────────────────────────────────────────────────
   主组件：NewProject
   ───────────────────────────────────────────────────────────── */
export default function NewProject() {
  const nav = useNavigate()
  const initialDraft = useMemo(() => loadDraft(), [])

  const [tab,         setTab]         = useState<'options' | 'prompt'>(initialDraft.tab)
  const [wizardStep,  setWizardStep]  = useState(initialDraft.wizardStep)
  const [name,        setName]        = useState(initialDraft.name)
  const [promptText,  setPromptText]  = useState(initialDraft.promptText)
  const [core,        setCore]        = useState<CoreDraft>(initialDraft.core)
  const [gameSystems, setGameSystems] = useState<GameSystemsDraft>(initialDraft.gameSystems)
  const [attributes,  setAttributes]  = useState<AttributesDraft>(initialDraft.attributes)
  const [err, setErr] = useState<string | null>(null)

  const persist = useCallback(() => {
    localStorage.setItem(DRAFT_KEY, JSON.stringify({
      name, promptText, core, tab, wizardStep, gameSystems, attributes,
    }))
  }, [name, promptText, core, tab, wizardStep, gameSystems, attributes])
  useEffect(() => { persist() }, [persist])

  function resetDraft() {
    setName(''); setPromptText(''); setCore(defaultCore()); setTab('options')
    setWizardStep(0); setGameSystems(defaultGameSystems()); setAttributes(defaultAttributes())
    localStorage.removeItem(DRAFT_KEY)
  }

  /* ── 游戏系统 操作 ─────────────────────────────────────── */
  const checkedSet = new Set(gameSystems.checkedPaths)

  function toggleGamePath(id: string, on: boolean) {
    setGameSystems((gs) => {
      const s = new Set(gs.checkedPaths)
      const sub = { ...gs.subsystemsByPath }
      if (on) {
        s.add(id)
        if (!sub[id]) sub[id] = defaultSubsystemsForPath(id)
      } else {
        s.delete(id)
        delete sub[id]
      }
      return { ...gs, checkedPaths: [...s], subsystemsByPath: sub }
    })
  }

  function toggleSubsystem(pathId: string, subId: string, on: boolean) {
    setGameSystems((gs) => {
      const cur = new Set(gs.subsystemsByPath[pathId] ?? [])
      if (on) cur.add(subId); else cur.delete(subId)
      return { ...gs, subsystemsByPath: { ...gs.subsystemsByPath, [pathId]: [...cur] } }
    })
  }

  function addCustomSubsystem(pathId: string, label: string) {
    const id = `customsub_${Date.now()}`
    setGameSystems((gs) => {
      const cur = new Set(gs.subsystemsByPath[pathId] ?? [])
      cur.add(id)
      return {
        ...gs,
        subsystemsByPath: { ...gs.subsystemsByPath, [pathId]: [...cur] },
        customSubsByPath: {
          ...gs.customSubsByPath,
          [pathId]: [...(gs.customSubsByPath[pathId] ?? []), { id, label }],
        },
      }
    })
  }

  function removeCustomSubsystem(pathId: string, subId: string) {
    setGameSystems((gs) => {
      const cur = new Set(gs.subsystemsByPath[pathId] ?? [])
      cur.delete(subId)
      return {
        ...gs,
        subsystemsByPath: { ...gs.subsystemsByPath, [pathId]: [...cur] },
        customSubsByPath: {
          ...gs.customSubsByPath,
          [pathId]: (gs.customSubsByPath[pathId] ?? []).filter((c) => c.id !== subId),
        },
      }
    })
  }

  function addCustomNode(parentId: string | null, label: string) {
    const id = `custom_${Date.now()}`
    setGameSystems((gs) => ({
      ...gs,
      customNodes: [...gs.customNodes, { id, label, parentId }],
    }))
  }

  function removeCustomNode(id: string) {
    setGameSystems((gs) => ({
      ...gs,
      checkedPaths: gs.checkedPaths.filter((p) => p !== id),
      subsystemsByPath: Object.fromEntries(
        Object.entries(gs.subsystemsByPath).filter(([k]) => k !== id)
      ),
      customNodes: gs.customNodes.filter((c) => c.id !== id),
    }))
  }

  /* ── 属性 操作 ─────────────────────────────────────────── */
  const selectedAttrsSet = new Set(attributes.selectedAttrs)

  function toggleAttr(id: string, on: boolean) {
    setAttributes((a) => {
      const s = new Set(a.selectedAttrs)
      if (on) {
        s.add(id)
        // 父选中时自动勾选第一个子属性
        for (const g of ATTR_GROUPS) {
          for (const node of g.nodes) {
            if (node.id === id && node.children?.[0]) {
              s.add(node.children[0].id)
            }
          }
        }
      } else {
        s.delete(id)
        // 取消父时同步取消所有子孙
        function removeChildren(nodes: AttrNode[]) {
          for (const n of nodes) { s.delete(n.id); if (n.children) removeChildren(n.children) }
        }
        function findAndRemove(nodes: AttrNode[]): boolean {
          for (const n of nodes) {
            if (n.id === id && n.children) { removeChildren(n.children); return true }
            if (n.children && findAndRemove(n.children)) return true
          }
          return false
        }
        for (const g of ATTR_GROUPS) findAndRemove(g.nodes)
      }
      return { ...a, selectedAttrs: [...s] }
    })
  }

  function addCustomAttr(parentId: string | null, label: string) {
    const id = `customattr_${Date.now()}`
    setAttributes((a) => {
      const s = new Set(a.selectedAttrs)
      s.add(id)
      return {
        ...a,
        selectedAttrs: [...s],
        customAttrs: [...a.customAttrs, { id, label, parentId }],
      }
    })
  }

  function removeCustomAttr(id: string) {
    setAttributes((a) => {
      const s = new Set(a.selectedAttrs)
      s.delete(id)
      return {
        ...a,
        selectedAttrs: [...s],
        customAttrs: a.customAttrs.filter((c) => c.id !== id),
      }
    })
  }

  /* ── 提交 ─────────────────────────────────────────────── */
  async function createFromOptions(e: FormEvent) {
    e.preventDefault(); setErr(null)
    try {
      const settings = {
        mode: 'options', core, prompt_text: '',
        game_systems: gameSystems, attribute_systems: attributes,
      }
      const res = await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '未命名项目', settings }),
      }) as { id: number }
      localStorage.removeItem(DRAFT_KEY)
      nav(`/project-setup/${res.id}`)
    } catch (x) { setErr(x instanceof Error ? x.message : String(x)) }
  }

  async function createFromPrompt(e: FormEvent) {
    e.preventDefault(); setErr(null)
    try {
      const settings = {
        mode: 'prompt', core: defaultCore(), prompt_text: promptText,
        game_systems: defaultGameSystems(), attribute_systems: defaultAttributes(),
      }
      const res = await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '提示词项目', settings }),
      }) as { id: number }
      localStorage.removeItem(DRAFT_KEY)
      nav(`/project-setup/${res.id}`)
    } catch (x) { setErr(x instanceof Error ? x.message : String(x)) }
  }

  /* ── 战斗节奏选项（依游戏类型变化）─────────────────── */
  const combatRhythmOptions = core.game_type === 'rpg_turn'
    ? [
        { v: 'fast', l: '快（3 回合）' },
        { v: 'mid',  l: '中（5 回合）' },
        { v: 'slow', l: '慢（10 回合）' },
        { v: 'custom', l: '自定义' },
      ]
    : [
        { v: 'fast', l: '快（5 秒）' },
        { v: 'mid',  l: '中（10 秒）' },
        { v: 'slow', l: '慢（30 秒）' },
        { v: 'custom', l: '自定义' },
      ]

  const stepLabels = ['核心定义', '游戏系统', '属性系统']

  return (
    <div className="new-project-page">
      <header className="topbar">
        <h1>新建项目</h1>
        <div className="topbar-right">
          <Link to="/projects" className="link-btn">← 返回列表</Link>
        </div>
      </header>

      <div className="new-project-body">
        {err && <p className="err" style={{ marginBottom: '0.75rem' }}>{err}</p>}

        <div className="tabs">
          <button
            type="button"
            className={tab === 'options' ? 'active' : ''}
            onClick={() => setTab('options')}
          >选项创建</button>
          <button
            type="button"
            className={tab === 'prompt' ? 'active' : ''}
            onClick={() => setTab('prompt')}
          >提示词创建</button>
        </div>

        {/* ══ 选项创建 ══ */}
        {tab === 'options' && (
          <>
            <div className="wizard-stepper">
              {stepLabels.map((lbl, i) => (
                <span
                  key={lbl}
                  className={i === wizardStep ? 'active' : i < wizardStep ? 'done' : ''}
                >
                  {i + 1}. {lbl}
                </span>
              ))}
            </div>

            {/* 步骤 1：核心定义 */}
            {wizardStep === 0 && (
              <div className="wizard-card">
                <div className="step-notice">
                  本页选项在创建项目后仍可修改（文档 01）。草稿已自动保存到浏览器。
                </div>

                <div className="form-section">
                  <div className="form-section-title">基本信息</div>
                  <div className="form-grid">
                    <label>
                      项目名称
                      <input
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="未命名项目"
                      />
                    </label>
                    <label>
                      游戏类型
                      <select
                        value={core.game_type}
                        onChange={(e) => setCore({ ...core, game_type: e.target.value })}
                      >
                        <option value="rpg_turn">RPG（回合）</option>
                        <option value="rpg_realtime">RPG（即时）</option>
                        <option value="moba"         disabled>MOBA — 第二阶段</option>
                        <option value="sim"          disabled>模拟经营 — 第二阶段</option>
                      </select>
                    </label>
                    <label>
                      商业模式
                      <select
                        value={core.business_model}
                        onChange={(e) => setCore({ ...core, business_model: e.target.value })}
                      >
                        <option value="item_mall">道具付费</option>
                        <option value="buy_once">一次性买断</option>
                      </select>
                    </label>
                    <label>
                      题材（供 AI 取名）
                      <input
                        value={core.theme}
                        onChange={(e) => setCore({ ...core, theme: e.target.value })}
                        placeholder="如：仙侠、赛博朋克…"
                      />
                    </label>
                  </div>
                </div>

                <div className="form-section">
                  <div className="form-section-title">数值框架</div>
                  <div className="form-grid">
                    <label>
                      基本数量级
                      <select
                        value={core.magnitude}
                        onChange={(e) => setCore({ ...core, magnitude: e.target.value })}
                      >
                        <option value="1">1</option>
                        <option value="10">10</option>
                        <option value="100">100</option>
                        <option value="custom">自定义</option>
                      </select>
                    </label>
                    {core.magnitude === 'custom' && (
                      <label>
                        自定义数量级（1–100000）
                        <input
                          type="number" min={1} max={100000}
                          value={core.magnitude_custom}
                          onChange={(e) => setCore({ ...core, magnitude_custom: e.target.value })}
                        />
                      </label>
                    )}
                    <label>
                      膨胀模式
                      <select
                        value={core.inflation_mode}
                        onChange={(e) => setCore({ ...core, inflation_mode: e.target.value })}
                      >
                        <option value="add">加法</option>
                        <option value="mul">乘法</option>
                      </select>
                    </label>
                    <label>
                      膨胀速率
                      <select
                        value={core.inflation_rate}
                        onChange={(e) => setCore({ ...core, inflation_rate: e.target.value })}
                      >
                        <option value="low">低（0.1）</option>
                        <option value="mid">中（0.3）</option>
                        <option value="high">高（1.0）</option>
                        <option value="custom">自定义</option>
                      </select>
                    </label>
                    {core.inflation_rate === 'custom' && (
                      <label>
                        自定义速率（0.1–10）
                        <input
                          type="number" min={0.1} max={10} step={0.1}
                          value={core.inflation_rate_custom}
                          onChange={(e) => setCore({ ...core, inflation_rate_custom: e.target.value })}
                        />
                      </label>
                    )}
                    <label>
                      等级上限（整数 ≥ 1）
                      <input
                        type="number" min={1}
                        value={core.level_cap}
                        onChange={(e) => setCore({ ...core, level_cap: Number(e.target.value) })}
                      />
                    </label>
                    <label>
                      游戏生命周期（天，允许小数）
                      <input
                        type="number" min={1} step={0.5}
                        value={core.lifecycle_days}
                        onChange={(e) => setCore({ ...core, lifecycle_days: Number(e.target.value) })}
                      />
                    </label>
                  </div>
                </div>

                <div className="form-section">
                  <div className="form-section-title">战斗设计</div>
                  <div className="form-grid">
                    <label>
                      防御公式
                      <select
                        value={core.defense_formula}
                        onChange={(e) => setCore({ ...core, defense_formula: e.target.value })}
                      >
                        <option value="subtract">减法（伤害 = 攻击 - 防御）</option>
                        <option value="divide">除法（伤害 = 攻击 × K / (防御 + K)）</option>
                      </select>
                    </label>
                    <label>
                      战斗节奏
                      <select
                        value={core.combat_rhythm}
                        onChange={(e) => setCore({ ...core, combat_rhythm: e.target.value })}
                      >
                        {combatRhythmOptions.map((o) => (
                          <option key={o.v} value={o.v}>{o.l}</option>
                        ))}
                      </select>
                    </label>
                    {core.combat_rhythm === 'custom' && (
                      <label>
                        自定义节奏值（回合数 / 秒）
                        <input
                          type="number" min={1} step={1}
                          value={core.combat_rhythm_custom}
                          onChange={(e) => setCore({ ...core, combat_rhythm_custom: e.target.value })}
                        />
                      </label>
                    )}
                    <label>
                      玩法节奏
                      <select
                        value={core.play_pace}
                        onChange={(e) => setCore({ ...core, play_pace: e.target.value })}
                      >
                        <option value="very_fast">特别快（80% 玩法第1天开放，3天100%）</option>
                        <option value="fast">快（60% 第1天，7天100%）</option>
                        <option value="standard">标准（40% 第1天，20天100%）</option>
                        <option value="slow">慢（25% 第1天，35天100%）</option>
                      </select>
                    </label>
                  </div>
                </div>

                <div className="wizard-actions">
                  <button type="button" className="btn ghost" onClick={resetDraft}>重置草稿</button>
                  <div className="right">
                    <button
                      type="button" className="btn primary"
                      onClick={() => setWizardStep(1)}
                    >下一步：游戏系统 →</button>
                  </div>
                </div>
              </div>
            )}

            {/* 步骤 2：游戏系统 */}
            {wizardStep === 1 && (
              <div className="wizard-card">
                <div className="step-notice">
                  勾选参与数值设计的系统；展开每个系统可独立配置子系统维度（增幅/升星等）。
                </div>

                <div className="form-section">
                  <div className="form-section-title">RPG 系统树</div>
                  <p className="muted small" style={{ marginBottom: '0.5rem' }}>
                    勾选的系统将参与属性分配计算；默认开启「基础属性」和「升级」两个子维度。
                  </p>
                  <div className="game-tree">
                    {RPG_GAME_TREE.map((node) => (
                      <TreeNodeRow
                        key={node.id} node={node} depth={0}
                        checked={checkedSet} onToggle={toggleGamePath}
                        customNodes={gameSystems.customNodes}
                        onAddCustom={addCustomNode}
                        onRemoveCustom={removeCustomNode}
                      />
                    ))}
                    {gameSystems.customNodes
                      .filter((c) => c.parentId === null)
                      .map((cn) => (
                        <div key={cn.id} className="tree-row" style={{ paddingLeft: '6px' }}>
                          <input
                            type="checkbox"
                            checked={checkedSet.has(cn.id)}
                            onChange={(e) => toggleGamePath(cn.id, e.target.checked)}
                          />
                          <span className="node-label" style={{ fontStyle: 'italic' }}>{cn.label}</span>
                          <span className="node-badge" style={{ color: '#217346' }}>自定义</span>
                          <button
                            type="button"
                            style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#c00', fontSize: '0.7rem', marginLeft: 4 }}
                            onClick={() => removeCustomNode(cn.id)}
                            title="删除"
                          >x</button>
                        </div>
                      ))}
                    <TopLevelCustomAdder onAdd={(label) => addCustomNode(null, label)} />
                  </div>
                </div>

                {gameSystems.checkedPaths.length > 0 && (
                  <div className="form-section">
                    <div className="form-section-title">子系统维度配置</div>
                    <p className="muted small" style={{ marginBottom: '0.5rem' }}>
                      点击展开每个系统独立配置；经济/世界/怪物系统已配置专属维度选项。
                    </p>
                    {gameSystems.checkedPaths.map((pathId) => {
                        const customNode = gameSystems.customNodes.find((c) => c.id === pathId)
                        const pathLabel = customNode ? customNode.label : getTreeNodeLabel(pathId)
                        return (
                          <SubsystemBlock
                            key={pathId}
                            pathId={pathId}
                            pathLabel={pathLabel}
                            subs={gameSystems.subsystemsByPath[pathId] ?? []}
                            customSubs={gameSystems.customSubsByPath[pathId] ?? []}
                            onToggle={(subId, on) => toggleSubsystem(pathId, subId, on)}
                            onAddCustomSub={(label) => addCustomSubsystem(pathId, label)}
                            onRemoveCustomSub={(subId) => removeCustomSubsystem(pathId, subId)}
                          />
                        )
                      })}
                  </div>
                )}

                <div className="wizard-actions">
                  <button type="button" className="btn ghost" onClick={() => setWizardStep(0)}>← 上一步</button>
                  <div className="right">
                    <button
                      type="button" className="btn primary"
                      onClick={() => setWizardStep(2)}
                    >下一步：属性系统 →</button>
                  </div>
                </div>
              </div>
            )}

            {/* 步骤 3：属性系统 */}
            {wizardStep === 2 && (
              <form className="wizard-card" onSubmit={createFromOptions}>
                <div className="step-notice">
                  本页选项在创建项目后仍可修改（文档 01）。勾选父属性自动展开并默认选中第一子属性。* 标记为必选项。
                </div>

                <label
                  className="combat-level-row"
                  onClick={() => setAttributes((a) => ({ ...a, combatLevelized: !a.combatLevelized }))}
                >
                  <input
                    type="checkbox"
                    checked={attributes.combatLevelized}
                    onChange={(e) => setAttributes((a) => ({ ...a, combatLevelized: e.target.checked }))}
                    onClick={(e) => e.stopPropagation()}
                  />
                  <span>
                    <strong>对抗属性等级化</strong>
                    {' — '}勾选后暴击率等对抗属性以「等级值」描述，并基于对抗者（非持有者）等级做修正（文档 01）
                  </span>
                </label>

                <div className="form-section">
                  <div className="form-section-title">属性选择</div>
                  <div className="attr-tree-wrap">
                    {ATTR_GROUPS.map((group) => (
                      <div key={group.id}>
                        <div className="attr-group-title">{group.label}</div>
                        {group.nodes.map((node) => (
                          <AttrNodeRow
                            key={node.id} node={node} depth={0}
                            selected={selectedAttrsSet} onToggle={toggleAttr}
                            gameType={core.game_type}
                          />
                        ))}
                      </div>
                    ))}
                    {/* 自定义属性区块 */}
                    <div>
                      <div className="attr-group-title">自定义属性</div>
                      {attributes.customAttrs.filter((c) => c.parentId === null).map((ca) => (
                        <div key={ca.id}>
                          <div className="attr-row" style={{ paddingLeft: 8 }}>
                            <input
                              type="checkbox"
                              checked={selectedAttrsSet.has(ca.id)}
                              onChange={(e) => {
                                const s = new Set(attributes.selectedAttrs)
                                if (e.target.checked) s.add(ca.id); else {
                                  s.delete(ca.id)
                                  attributes.customAttrs.filter(c2 => c2.parentId === ca.id).forEach(c2 => s.delete(c2.id))
                                }
                                setAttributes(a => ({ ...a, selectedAttrs: [...s] }))
                              }}
                            />
                            <span className="attr-label" style={{ flex: 1, fontStyle: 'italic' }}>{ca.label}</span>
                            <span className="node-badge" style={{ color: '#217346', fontSize: '0.7rem' }}>自定义</span>
                            <button
                              type="button"
                              onClick={() => removeCustomAttr(ca.id)}
                              style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#c00', fontSize: '0.75rem', padding: '0 4px' }}
                              title="删除">×</button>
                          </div>
                          {/* 次级自定义 */}
                          {attributes.customAttrs.filter((c) => c.parentId === ca.id).map((ca2) => (
                            <div key={ca2.id} className="attr-row child-1" style={{ paddingLeft: 28 }}>
                              <input
                                type="checkbox"
                                checked={selectedAttrsSet.has(ca2.id)}
                                onChange={(e) => {
                                  const s = new Set(attributes.selectedAttrs)
                                  if (e.target.checked) s.add(ca2.id); else s.delete(ca2.id)
                                  setAttributes(a => ({ ...a, selectedAttrs: [...s] }))
                                }}
                              />
                              <span className="attr-label" style={{ flex: 1, fontStyle: 'italic' }}>{ca2.label}</span>
                              <button
                                type="button"
                                onClick={() => removeCustomAttr(ca2.id)}
                                style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#c00', fontSize: '0.75rem', padding: '0 4px' }}
                                title="删除">×</button>
                            </div>
                          ))}
                          <AttrCustomAdder
                            label="+ 添加次级属性"
                            indent={28}
                            onAdd={(lbl) => addCustomAttr(ca.id, lbl)}
                          />
                        </div>
                      ))}
                      <AttrCustomAdder
                        label="+ 添加顶级自定义属性"
                        indent={8}
                        onAdd={(lbl) => addCustomAttr(null, lbl)}
                      />
                    </div>
                  </div>
                </div>

                <div className="wizard-actions">
                  <button type="button" className="btn ghost" onClick={() => setWizardStep(1)}>← 上一步</button>
                  <div className="right">
                    <button type="button" className="btn ghost" onClick={resetDraft}>重置草稿</button>
                    <button type="submit" className="btn primary">确定并创建项目</button>
                  </div>
                </div>
              </form>
            )}
          </>
        )}

        {/* ══ 提示词创建 ══ */}
        {tab === 'prompt' && (
          <form className="wizard-card" onSubmit={createFromPrompt}>
            <div className="step-notice">直接输入提示词描述你想做的数值项目，AI 将自动推断设置。</div>
            <div className="form-section">
              <div className="form-section-title">基本信息</div>
              <label>
                项目名称（可选）
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="提示词项目"
                  style={{ width: '100%' }}
                />
              </label>
            </div>
            <div className="form-section" style={{ marginTop: '0.75rem' }}>
              <div className="form-section-title">项目描述</div>
              <textarea
                rows={12}
                value={promptText}
                onChange={(e) => setPromptText(e.target.value)}
                style={{ width: '100%' }}
                placeholder="描述你想做的数值项目，例如：我要做一个回合制 RPG，玩法有装备、坐骑和天赋，攻防暴击体系，60天生命周期…"
              />
            </div>
            <div className="wizard-actions">
              <button type="button" className="btn ghost" onClick={resetDraft}>重置草稿</button>
              <button
                type="submit" className="btn primary"
                disabled={!promptText.trim()}
              >创建项目</button>
            </div>
          </form>
        )}
      </div>
    </div>
  )
}
