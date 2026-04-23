import { useEffect, useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'

const DRAFT_KEY = 'numflow_new_project_draft_v1'

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

export default function NewProject() {
  const nav = useNavigate()
  const [tab, setTab] = useState<'options' | 'prompt'>('options')
  const [name, setName] = useState('')
  const [promptText, setPromptText] = useState('')
  const [core, setCore] = useState<CoreDraft>(defaultCore)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    try {
      const raw = localStorage.getItem(DRAFT_KEY)
      if (!raw) return
      const d = JSON.parse(raw) as {
        name?: string
        promptText?: string
        core?: CoreDraft
        tab?: 'options' | 'prompt'
      }
      if (d.name) setName(d.name)
      if (d.promptText) setPromptText(d.promptText)
      if (d.core) setCore({ ...defaultCore(), ...d.core })
      if (d.tab) setTab(d.tab)
    } catch {
      /* ignore */
    }
  }, [])

  function persist() {
    localStorage.setItem(
      DRAFT_KEY,
      JSON.stringify({ name, promptText, core, tab }),
    )
  }

  useEffect(() => {
    persist()
  }, [name, promptText, core, tab])

  function resetDraft() {
    setName('')
    setPromptText('')
    setCore(defaultCore())
    setTab('options')
    localStorage.removeItem(DRAFT_KEY)
  }

  async function createFromOptions(e: FormEvent) {
    e.preventDefault()
    setErr(null)
    try {
      const settings = {
        mode: 'options',
        core,
        prompt_text: '',
      }
      const res = (await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '未命名项目', settings }),
      })) as { id: number }
      localStorage.removeItem(DRAFT_KEY)
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
      }
      const res = (await apiFetch('/projects', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim() || '提示词项目', settings }),
      })) as { id: number }
      localStorage.removeItem(DRAFT_KEY)
      nav(`/workbench/${res.id}`)
    } catch (x) {
      setErr(x instanceof Error ? x.message : String(x))
    }
  }

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
        <form className="wizard" onSubmit={createFromOptions}>
          <label>
            项目名称
            <input value={name} onChange={(e) => setName(e.target.value)} required />
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
            <button type="submit" className="btn primary">
              确定并创建
            </button>
          </div>
        </form>
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
