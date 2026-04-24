import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'
import {
  ATTRIBUTE_BASIC,
  ATTRIBUTE_EXTRA,
  RPG_GAME_TREE,
  SUBSYSTEM_OPTIONS,
  defaultAttributes,
  defaultGameSystems,
  pruneUnknownPaths,
  type AttributesDraft,
  type GameSystemsDraft,
  type RpgTreeNode,
} from '../data/rpgGameSystems'

const DRAFT_KEY_V2 = 'numflow_new_project_draft_v2'
const DRAFT_KEY_V1 = 'numflow_new_project_draft_v1'

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
})

function defaultSubsystemsList(): string[] {
  return SUBSYSTEM_OPTIONS.filter((s) => s.defaultOn).map((s) => s.id)
}

type DraftBundle = {
  name: string
  promptText: string
  core: CoreDraft
  tab: 'options' | 'prompt'
  wizardStep: number
  gameSystems: GameSystemsDraft
  attributes: AttributesDraft
}

function loadDraftBundleFromStorage(): DraftBundle {
  const fallback: DraftBundle = {
    name: '',
    promptText: '',
    core: defaultCore(),
    tab: 'options',
    wizardStep: 0,
    gameSystems: defaultGameSystems(),
    attributes: defaultAttributes(),
  }
  try {
    const raw = localStorage.getItem(DRAFT_KEY_V2) || localStorage.getItem(DRAFT_KEY_V1)
    if (!raw) return fallback
    const d = JSON.parse(raw) as {
      name?: string
      promptText?: string
      core?: CoreDraft
      tab?: 'options' | 'prompt'
      wizardStep?: number
      gameSystems?: GameSystemsDraft
      attributes?: AttributesDraft
    }
    const tab: 'options' | 'prompt' = d.tab === 'prompt' ? 'prompt' : 'options'
    const wizardStep =
      typeof d.wizardStep === 'number' && d.wizardStep >= 0 && d.wizardStep <= 2 ? d.wizardStep : 0
    return {
      name: d.name ?? '',
      promptText: d.promptText ?? '',
      core: d.core ? { ...defaultCore(), ...d.core } : defaultCore(),
      tab,
      wizardStep,
      gameSystems: d.gameSystems ? pruneUnknownPaths(d.gameSystems) : defaultGameSystems(),
      attributes: d.attributes
        ? {
            basics: d.attributes.basics?.length ? d.attributes.basics : defaultAttributes().basics,
            extras: d.attributes.extras ?? [],
          }
        : defaultAttributes(),
    }
  } catch {
    return fallback
  }
}

function TreeRows(props: {
  nodes: RpgTreeNode[]
  depth: number
  checked: Set<string>
  onToggle: (id: string, next: boolean) => void
}): ReactNode {
  const { nodes, depth, checked, onToggle } = props
  return (
    <>
      {nodes.map((n) => (
        <div key={n.id}>
          <label className="tree-row" style={{ paddingLeft: `${depth * 0.85}rem` }}>
            <input
              type="checkbox"
              checked={checked.has(n.id)}
              onChange={(e) => onToggle(n.id, e.target.checked)}
            />
            <span>{n.label}</span>
          </label>
          {n.children && n.children.length > 0 && (
            <TreeRows nodes={n.children} depth={depth + 1} checked={checked} onToggle={onToggle} />
          )}
        </div>
      ))}
    </>
  )
}

export default function NewProject() {
  const nav = useNavigate()
  const initialDraft = useMemo(() => loadDraftBundleFromStorage(), [])

  const [tab, setTab] = useState<'options' | 'prompt'>(initialDraft.tab)
  const [wizardStep, setWizardStep] = useState(initialDraft.wizardStep)
  const [name, setName] = useState(initialDraft.name)
  const [promptText, setPromptText] = useState(initialDraft.promptText)
  const [core, setCore] = useState<CoreDraft>(initialDraft.core)
  const [gameSystems, setGameSystems] = useState<GameSystemsDraft>(initialDraft.gameSystems)
  const [attributes, setAttributes] = useState<AttributesDraft>(initialDraft.attributes)
  const [err, setErr] = useState<string | null>(null)

  const persist = useCallback(() => {
    localStorage.setItem(
      DRAFT_KEY_V2,
      JSON.stringify({
        name,
        promptText,
        core,
        tab,
        wizardStep,
        gameSystems,
        attributes,
      }),
    )
  }, [name, promptText, core, tab, wizardStep, gameSystems, attributes])

  useEffect(() => {
    persist()
  }, [persist])

  function resetDraft() {
    setName('')
    setPromptText('')
    setCore(defaultCore())
    setTab('options')
    setWizardStep(0)
    setGameSystems(defaultGameSystems())
    setAttributes(defaultAttributes())
    localStorage.removeItem(DRAFT_KEY_V2)
    localStorage.removeItem(DRAFT_KEY_V1)
  }

  const checkedSet = new Set(gameSystems.checkedPaths)

  function toggleGamePath(id: string, on: boolean) {
    setGameSystems((gs) => {
      const s = new Set(gs.checkedPaths)
      const sub = { ...gs.subsystemsByPath }
      if (on) {
        s.add(id)
        if (!sub[id]) sub[id] = defaultSubsystemsList()
      } else {
        s.delete(id)
        delete sub[id]
      }
      return { checkedPaths: [...s], subsystemsByPath: sub }
    })
  }

  function toggleSubsystem(pathId: string, subId: string, on: boolean) {
    setGameSystems((gs) => {
      const cur = new Set(gs.subsystemsByPath[pathId] ?? defaultSubsystemsList())
      if (on) cur.add(subId)
      else cur.delete(subId)
      return { ...gs, subsystemsByPath: { ...gs.subsystemsByPath, [pathId]: [...cur] } }
    })
  }

  function toggleBasic(id: string, on: boolean) {
    setAttributes((a) => ({
      ...a,
      basics: on ? [...new Set([...a.basics, id])] : a.basics.filter((x) => x !== id),
    }))
  }

  function toggleExtra(id: string, on: boolean) {
    setAttributes((a) => ({
      ...a,
      extras: on ? [...new Set([...a.extras, id])] : a.extras.filter((x) => x !== id),
    }))
  }

  async function createFromOptions(e: FormEvent) {
    e.preventDefault()
    setErr(null)
    try {
      const settings = {
        mode: 'options',
        core,
        prompt_text: '',
        game_systems: gameSystems,
        attribute_systems: attributes,
      }
      const res = (await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '未命名项目', settings }),
      })) as { id: number }
      localStorage.removeItem(DRAFT_KEY_V2)
      localStorage.removeItem(DRAFT_KEY_V1)
      nav(`/workbench/${res.id}`)
    } catch (x) {
      setErr(x instanceof Error ? x.message : String(x))
    }
  }

  async function createFromPrompt(e: FormEvent) {
    e.preventDefault()
    setErr(null)
    try {
      const settings = {
        mode: 'prompt',
        core: defaultCore(),
        prompt_text: promptText,
        game_systems: defaultGameSystems(),
        attribute_systems: defaultAttributes(),
      }
      const res = (await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '提示词项目', settings }),
      })) as { id: number }
      localStorage.removeItem(DRAFT_KEY_V2)
      localStorage.removeItem(DRAFT_KEY_V1)
      nav(`/workbench/${res.id}`)
    } catch (x) {
      setErr(x instanceof Error ? x.message : String(x))
    }
  }

  const stepLabels = ['核心定义', '游戏系统', '属性系统']

  return (
    <div className="new-project-page">
      <header className="topbar">
        <h1>新建项目</h1>
        <Link to="/projects" className="link-btn">
          返回列表
        </Link>
      </header>
      <p className="banner">本页选项在创建项目后仍可修改（文档 01）。草稿自动保存在浏览器。</p>
      <div className="tabs">
        <button type="button" className={tab === 'options' ? 'active' : ''} onClick={() => setTab('options')}>
          选项创建
        </button>
        <button type="button" className={tab === 'prompt' ? 'active' : ''} onClick={() => setTab('prompt')}>
          提示词创建
        </button>
      </div>
      {err && <p className="err">{err}</p>}

      {tab === 'options' && (
        <>
          <div className="wizard-stepper">
            {stepLabels.map((label, i) => (
              <span key={label} className={i === wizardStep ? 'active' : i < wizardStep ? 'done' : ''}>
                {i + 1}. {label}
              </span>
            ))}
          </div>

          {wizardStep === 0 && (
            <div className="wizard">
              <label>
                项目名称
                <input value={name} onChange={(e) => setName(e.target.value)} />
              </label>
              <fieldset>
                <legend>核心定义</legend>
                <label>
                  游戏类型（当前仅 RPG 回合）
                  <select value={core.game_type} onChange={(e) => setCore({ ...core, game_type: e.target.value })}>
                    <option value="rpg_turn">RPG（回合）</option>
                    <option value="rpg_realtime" disabled>
                      RPG（即时）— 二阶段
                    </option>
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
                  <input value={core.theme} onChange={(e) => setCore({ ...core, theme: e.target.value })} />
                </label>
                <label>
                  基本数量级
                  <select value={core.magnitude} onChange={(e) => setCore({ ...core, magnitude: e.target.value })}>
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
                    <option value="high">高（1）</option>
                    <option value="custom">自定义</option>
                  </select>
                </label>
                {core.inflation_rate === 'custom' && (
                  <label>
                    自定义速率（0.1–10）
                    <input
                      value={core.inflation_rate_custom}
                      onChange={(e) => setCore({ ...core, inflation_rate_custom: e.target.value })}
                    />
                  </label>
                )}
                <label>
                  等级上限
                  <input
                    type="number"
                    min={1}
                    value={core.level_cap}
                    onChange={(e) => setCore({ ...core, level_cap: Number(e.target.value) })}
                  />
                </label>
                <label>
                  游戏生命周期（天）
                  <input
                    type="number"
                    min={1}
                    step={0.1}
                    value={core.lifecycle_days}
                    onChange={(e) => setCore({ ...core, lifecycle_days: Number(e.target.value) })}
                  />
                </label>
                <label>
                  防御公式
                  <select
                    value={core.defense_formula}
                    onChange={(e) => setCore({ ...core, defense_formula: e.target.value })}
                  >
                    <option value="subtract">减法</option>
                    <option value="divide">除法</option>
                  </select>
                </label>
                <label>
                  玩法节奏
                  <select value={core.play_pace} onChange={(e) => setCore({ ...core, play_pace: e.target.value })}>
                    <option value="very_fast">特别快</option>
                    <option value="fast">快</option>
                    <option value="standard">标准</option>
                    <option value="slow">慢</option>
                  </select>
                </label>
              </fieldset>
              <div className="actions">
                <button type="button" className="btn ghost" onClick={resetDraft}>
                  重置草稿
                </button>
                <button type="button" className="btn primary" onClick={() => setWizardStep(1)}>
                  下一步：游戏系统
                </button>
              </div>
            </div>
          )}

          {wizardStep === 1 && (
            <div className="wizard">
              <fieldset>
                <legend>游戏系统树（文档 01）</legend>
                <p className="muted small">勾选参与数值设计的系统；可为每个系统勾选子系统维度。</p>
                <div className="game-tree">
                  <TreeRows nodes={RPG_GAME_TREE} depth={0} checked={checkedSet} onToggle={toggleGamePath} />
                </div>
              </fieldset>
              <fieldset>
                <legend>子系统维度</legend>
                {gameSystems.checkedPaths.length === 0 && <p className="muted small">请先在上方勾选至少一个系统。</p>}
                {gameSystems.checkedPaths
                  .slice()
                  .sort()
                  .map((pathId) => (
                    <div key={pathId} className="subsystem-block">
                      <strong>{pathId}</strong>
                      <div className="subsystem-grid">
                        {SUBSYSTEM_OPTIONS.map((opt) => (
                          <label key={opt.id}>
                            <input
                              type="checkbox"
                              checked={(gameSystems.subsystemsByPath[pathId] ?? []).includes(opt.id)}
                              onChange={(e) => toggleSubsystem(pathId, opt.id, e.target.checked)}
                            />
                            {opt.label}
                          </label>
                        ))}
                      </div>
                    </div>
                  ))}
              </fieldset>
              <div className="actions">
                <button type="button" className="btn ghost" onClick={() => setWizardStep(0)}>
                  上一步
                </button>
                <button type="button" className="btn primary" onClick={() => setWizardStep(2)}>
                  下一步：属性系统
                </button>
              </div>
            </div>
          )}

          {wizardStep === 2 && (
            <form className="wizard" onSubmit={createFromOptions}>
              <fieldset>
                <legend>属性系统（MVP）</legend>
                <p className="muted small">基础属性默认全选；可按项目勾选进阶属性。</p>
                <div className="attr-grid">
                  <div>
                    <h4 className="attr-heading">基础</h4>
                    {ATTRIBUTE_BASIC.map((a) => (
                      <label key={a.id} className="attr-line">
                        <input
                          type="checkbox"
                          checked={attributes.basics.includes(a.id)}
                          onChange={(e) => toggleBasic(a.id, e.target.checked)}
                        />
                        {a.label}
                      </label>
                    ))}
                  </div>
                  <div>
                    <h4 className="attr-heading">进阶</h4>
                    {ATTRIBUTE_EXTRA.map((a) => (
                      <label key={a.id} className="attr-line">
                        <input
                          type="checkbox"
                          checked={attributes.extras.includes(a.id)}
                          onChange={(e) => toggleExtra(a.id, e.target.checked)}
                        />
                        {a.label}
                      </label>
                    ))}
                  </div>
                </div>
              </fieldset>
              <div className="actions">
                <button type="button" className="btn ghost" onClick={() => setWizardStep(1)}>
                  上一步
                </button>
                <button type="submit" className="btn primary">
                  确定并创建
                </button>
              </div>
            </form>
          )}
        </>
      )}

      {tab === 'prompt' && (
        <form className="wizard" onSubmit={createFromPrompt}>
          <label>
            项目名称（可选）
            <input value={name} onChange={(e) => setName(e.target.value)} />
          </label>
          <label>
            提示词
            <textarea
              rows={14}
              value={promptText}
              onChange={(e) => setPromptText(e.target.value)}
              placeholder="描述你想做的数值项目…"
            />
          </label>
          <div className="actions">
            <button type="button" className="btn ghost" onClick={resetDraft}>
              重置草稿
            </button>
            <button type="submit" className="btn primary" disabled={!promptText.trim()}>
              创建
            </button>
          </div>
        </form>
      )}
    </div>
  )
}
