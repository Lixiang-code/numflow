/**
 * Agent Monitor — 全功能 Agent 调试与监控窗口
 * 支持：三阶段面板(design/review/execute)、工具调用时间线、实时流、会话历史、指标面板
 * 可从 Workbench 以 window.open popup 方式打开
 */
import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactElement } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'
import AutoTextarea from '../components/AutoTextarea'

const LS_KEY = 'numflow_agent_monitor_v2'
const MAX_HISTORY = 50

// ─── 类型 ──────────────────────────────────────────────────────────────────
type SseEvent = { ts: string; raw: Record<string, unknown> }

type PhaseState = {
  started: string | null
  finished: string | null
  text: string
  logs: string[]
  error: string | null
  hasContent: boolean
}

type ToolEntry = {
  idx: number
  ts: string
  kind: 'call' | 'result' | 'error'
  name: string
  body: string
  expanded: boolean
}

type RouteInfo = {
  step: string | null
  template_key: string | null
  log: string | null
  hit: boolean | null
  prompt: string | null
  gatherHint: string | null
  routeSystem: string | null
  rationale: string | null
  skills: Array<{ id?: number; slug: string; title: string }>
}

// ─── 完整对话类型 ──────────────────────────────────────────────────────────
type ToolSchema = {
  name: string
  description: string
  parameters: unknown
}

type LlmMessage = {
  role: 'system' | 'user' | 'assistant' | 'tool'
  content: string | null
  tool_calls?: Array<{ id: string; type?: string; function: { name: string; arguments: string } }>
  tool_call_id?: string
  name?: string
}

type ConversationTurn = {
  phase: string
  round?: number
  messages: LlmMessage[]
}

type SkillMeta = {
  slug: string
  title: string
  summary?: string
  step_id?: string
}

type ToolsMeta = {
  phase: string
  tools: string[]
  tool_schemas: ToolSchema[]
  parallel_tool_calls: boolean
  tool_choice: string
  skills_meta: SkillMeta[]
}

type PromptSource = {
  prompt_key: string
  title: string
  override: boolean
  content: string
}

type Metrics = {
  startedAt: string
  finishedAt: string | null
  totalMs: number | null
  designMs: number | null
  reviewMs: number | null
  executeMs: number | null
  toolCalls: number
  tokenHint: string | null
  status: 'running' | 'done' | 'error'
}

type Session = {
  id: string
  projectId: number
  mode: 'init' | 'maintain'
  userMessage: string
  events: SseEvent[]
  metrics: Metrics
}

// ─── 工具函数 ──────────────────────────────────────────────────────────────
function genId() { return `m_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}` }

function msDiff(a: string | null, b: string | null): number | null {
  if (!a || !b) return null
  return new Date(b).getTime() - new Date(a).getTime()
}

function fmtMs(ms: number | null): string {
  if (ms === null) return '—'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function downloadJson(name: string, obj: unknown) {
  const b = new Blob([JSON.stringify(obj, null, 2)], { type: 'application/json' })
  const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = name; a.click()
  URL.revokeObjectURL(a.href)
}

/** 把含 ## H2 行的纯文本渲染为简单 HTML */
function PhaseText({ text, live }: { text: string; live?: boolean }) {
  if (!text) return <span className="muted small">（暂无内容）</span>
  const lines = text.split('\n')
  const parts: ReactElement[] = []
  lines.forEach((ln, i) => {
    if (ln.startsWith('## ')) {
      parts.push(<h2 key={i}>{ln.slice(3)}</h2>)
    } else if (ln.startsWith('# ')) {
      parts.push(<h2 key={i}>{ln.slice(2)}</h2>)
    } else {
      parts.push(<p key={i}>{ln || '\u00a0'}</p>)
    }
  })
  return (
    <div className="am-phase-text-formatted">
      {parts}
      {live && <span className="am-cursor" />}
    </div>
  )
}

// ─── 阶段面板 ──────────────────────────────────────────────────────────────
function PhasePanel({ phase, label, state, toolEntries, live }:{
  phase: string; label: string; state: PhaseState;
  toolEntries?: ToolEntry[]; live?: boolean;
}) {
  const [open, setOpen] = useState(true)
  const badge = phase as 'design'|'review'|'execute'|'route'|'done'
  const hasTools = toolEntries && toolEntries.length > 0
  const count = state.logs.length + (toolEntries?.length ?? 0)

  return (
    <div className="am-phase-panel">
      <div className="am-phase-header" onClick={() => setOpen(o => !o)}>
        <span className={`am-phase-badge ${badge}`}>{label}</span>
        <span className="am-phase-title">
          {state.error ? `❌ 错误` : state.hasContent ? `✓ 完成（${state.text.length} chars）` : live ? '⟳ 进行中…' : '—'}
        </span>
        {count > 0 && <span className="muted small">{count} 事件</span>}
        {state.started && <span className="am-phase-meta">{fmtMs(msDiff(state.started, state.finished ?? new Date().toISOString()))}</span>}
        <span className={`am-phase-chevron${open ? ' open' : ''}`}>▶</span>
      </div>
      {open && (
        <div className="am-phase-content">
          {state.logs.length > 0 && (
            <div className="am-route-block" style={{ marginBottom: '0.4rem', background: '#f5f5f5', borderColor: '#ddd' }}>
              <h4>日志</h4>
              {state.logs.map((l, i) => <p key={i}><code>{l}</code></p>)}
            </div>
          )}
          {state.error && (
            <div className="am-route-block" style={{ background: '#ffebee', borderColor: '#ef9a9a' }}>
              <h4>错误</h4><p>{state.error}</p>
            </div>
          )}
          {(state.hasContent || live) && (
            <PhaseText text={state.text} live={live} />
          )}
          {hasTools && (
            <>
              <div className="am-section-label">工具调用 ({toolEntries.length})</div>
              <ToolTimeline entries={toolEntries} />
            </>
          )}
        </div>
      )}
    </div>
  )
}

// ─── 工具时间线 ────────────────────────────────────────────────────────────
function ToolTimeline({ entries }: { entries: ToolEntry[] }) {
  const [exp, setExp] = useState<Set<number>>(new Set())
  function toggle(idx: number) {
    setExp((s) => {
      const n = new Set(s)
      if (n.has(idx)) n.delete(idx)
      else n.add(idx)
      return n
    })
  }
  return (
    <div className="am-tool-timeline">
      {entries.map((e) => (
        <div key={e.idx} className="am-tool-item">
          <div className="am-tool-item-head" onClick={() => toggle(e.idx)}>
            <span className={`am-tool-badge ${e.kind}`}>{e.kind}</span>
            <span className="am-tool-name">{e.name}</span>
            <span className="am-tool-time">{e.ts.slice(11, 19)}</span>
            <span style={{ fontSize: '0.65rem', color: '#aaa', marginLeft: '4px' }}>{exp.has(e.idx) ? '▲' : '▼'}</span>
          </div>
          {exp.has(e.idx) && <div className="am-tool-item-body">{e.body}</div>}
        </div>
      ))}
    </div>
  )
}

function rebuildFromEvents(events: SseEvent[]) {
  const phases: Record<string, PhaseState> = {}
  const tools: ToolEntry[] = []
  const turns: ConversationTurn[] = []
  const toolsMeta: Record<string, ToolsMeta> = {}
  const promptSources: Record<string, PromptSource[]> = {}
  let routeInfo: RouteInfo | null = null
  let toolCalls = 0

  for (const ev of events) {
    const phase = String(ev.raw.phase ?? '')
    const type = String(ev.raw.type ?? '')
    if (phase && !phases[phase]) {
      phases[phase] = { started: ev.ts, finished: null, text: '', logs: [], error: null, hasContent: false }
    }

    if (type === 'token') {
      if (phase) {
        phases[phase].text += String(ev.raw.text ?? '')
        phases[phase].hasContent = true
      }
    } else if (type === 'log') {
      if (phase) phases[phase].logs.push(String(ev.raw.message ?? ''))
    } else if (type === 'error') {
      if (phase) phases[phase].error = String(ev.raw.message ?? '')
    } else if (type === 'tool_call') {
      toolCalls += 1
      tools.push({ idx: tools.length, ts: ev.ts, kind: 'call', name: String(ev.raw.name ?? ''), body: String(ev.raw.arguments ?? ''), expanded: false })
    } else if (type === 'tool_result') {
      tools.push({ idx: tools.length, ts: ev.ts, kind: 'result', name: String(ev.raw.name ?? ''), body: String(ev.raw.preview ?? ''), expanded: false })
    } else if (type === 'phase_messages') {
      const messages = ev.raw.messages as LlmMessage[] | undefined
      if (Array.isArray(messages) && messages.length > 0) {
        turns.push({
          phase: String(ev.raw.phase ?? phase),
          round: ev.raw.round !== undefined ? Number(ev.raw.round) : undefined,
          messages,
        })
      }
    } else if (type === 'tools_meta') {
      const metaPhase = String(ev.raw.phase ?? phase)
      toolsMeta[metaPhase] = {
        phase: metaPhase,
        tools: Array.isArray(ev.raw.tools) ? ev.raw.tools as string[] : [],
        tool_schemas: Array.isArray(ev.raw.tool_schemas) ? ev.raw.tool_schemas as ToolSchema[] : [],
        parallel_tool_calls: Boolean(ev.raw.parallel_tool_calls),
        tool_choice: String(ev.raw.tool_choice ?? 'auto'),
        skills_meta: Array.isArray(ev.raw.skills_meta) ? ev.raw.skills_meta as SkillMeta[] : [],
      }
    } else if (type === 'prompt_sources') {
      const srcPhase = String(ev.raw.phase ?? phase)
      promptSources[srcPhase] = Array.isArray(ev.raw.sources)
        ? ev.raw.sources as PromptSource[]
        : []
    } else if (type === 'prompt_route') {
      routeInfo = {
        step: String(ev.raw.step_id ?? ev.raw.step ?? ''),
        template_key: String(ev.raw.template_key ?? ''),
        log: String(ev.raw.message ?? ''),
        hit: ev.raw.hit != null ? Boolean(ev.raw.hit) : null,
        prompt: ev.raw.prompt ? String(ev.raw.prompt) : null,
        gatherHint: ev.raw.gather_hint ? String(ev.raw.gather_hint) : null,
        routeSystem: ev.raw.route_system ? String(ev.raw.route_system) : null,
        rationale: ev.raw.rationale ? String(ev.raw.rationale) : null,
        skills: Array.isArray(ev.raw.skills) ? ev.raw.skills as Array<{ id?: number; slug: string; title: string }> : [],
      }
    } else if (type === 'done') {
      if (phase && phases[phase]) {
        phases[phase].finished = ev.ts
      }
    }
  }

  return { phases, tools, turns, toolsMeta, promptSources, routeInfo, toolCalls }
}

// ─── 进度条 ────────────────────────────────────────────────────────────────
function PhaseProgress({ current, status }: { current: string; status: Metrics['status'] }) {
  const phases = ['route', 'design', 'review', 'execute']
  const labels = ['路由', '设计', '审核', '执行']
  const cur = phases.indexOf(current)
  return (
    <div className="am-progress">
      {phases.map((p, i) => {
        let dotClass = ''
        if (status === 'error' && i === cur) dotClass = 'error'
        else if (i < cur || status === 'done') dotClass = 'done'
        else if (i === cur) dotClass = 'active'
        return (
          <div key={p} className="am-progress-phase">
            <div className={`am-progress-dot ${dotClass}`}>{i + 1}</div>
            <span className={`am-progress-label ${dotClass === 'active' ? 'active' : ''}`}>{labels[i]}</span>
          </div>
        )
      })}
    </div>
  )
}

// ─── 会话历史侧边栏 ────────────────────────────────────────────────────────
function HistorySidebar({
  history, currentId, onSelect, onClear, onRefresh,
}: {
  history: Session[]; currentId: string | null
  onSelect: (s: Session) => void; onClear: () => void
  onRefresh?: () => void
}) {
  return (
    <aside className="am-sidebar">
      <div className="am-sidebar-head">
        <span>会话历史</span>
        <span style={{ display: 'inline-flex', gap: '0.4rem' }}>
          {onRefresh && (
            <a href="#" onClick={(e) => { e.preventDefault(); onRefresh() }} title="拉取项目历史">刷新</a>
          )}
          <a href="#" onClick={(e) => { e.preventDefault(); onClear() }} title="清空本地历史">清空</a>
        </span>
      </div>
      {history.length === 0 && <p className="muted small" style={{ padding: '0.75rem' }}>暂无记录</p>}
      <ul className="am-history-list">
        {history.map((s) => (
          <li key={s.id} className={`am-history-item${s.id === currentId ? ' active' : ''}`}
            onClick={() => onSelect(s)}>
            <div className="am-history-item-title" title={s.userMessage}>
              {(() => {
                const m = s.userMessage.match(/【(.+?)】/)
                return m ? m[1] : s.userMessage
              })()}
            </div>
            <div className="am-history-item-meta">
              {s.mode} · {s.metrics.startedAt.slice(0, 16).replace('T', ' ')}
              <span className={`am-history-item-status ${s.metrics.status === 'error' ? 'err' : s.metrics.status === 'running' ? 'running' : 'ok'}`}>
                {s.metrics.status === 'error' ? '失败' : s.metrics.status === 'running' ? '运行中' : '完成'}
              </span>
            </div>
          </li>
        ))}
      </ul>
    </aside>
  )
}

// ─── 完整对话视图 ──────────────────────────────────────────────────────────
function CollapsibleText({ text, previewLen = 300 }: { text: string; previewLen?: number }) {
  const [open, setOpen] = useState(false)
  if (text.length <= previewLen) return <pre style={{ whiteSpace: 'pre-wrap', margin: 0, fontSize: '0.78rem' }}>{text}</pre>
  return (
    <div>
      <pre style={{ whiteSpace: 'pre-wrap', margin: 0, fontSize: '0.78rem' }}>{open ? text : text.slice(0, previewLen) + '…'}</pre>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{ marginTop: '4px', fontSize: '0.7rem', background: 'none', border: 'none', color: '#888', cursor: 'pointer', padding: 0 }}>
        {open ? '▲ 折叠' : `▼ 展开全文（${text.length} 字符）`}
      </button>
    </div>
  )
}

function LlmMessageBubble({ msg, index }: { msg: LlmMessage; index: number }) {
  const [collapsed, setCollapsed] = useState(msg.role === 'system')
  const content = msg.content ?? ''
  const isLong = content.length > 400

  const roleConfig: Record<string, { label: string; bg: string; color: string; border: string }> = {
    system:    { label: '⚙ SYSTEM', bg: '#1e2233', color: '#a0b0d0', border: '#2d3a5a' },
    user:      { label: '👤 USER',   bg: '#1a2f4a', color: '#7ec8e3', border: '#2a5080' },
    assistant: { label: '🤖 AI',     bg: '#1e2820', color: '#90d498', border: '#2a4030' },
    tool:      { label: '🔧 TOOL结果', bg: '#2a2010', color: '#d4b060', border: '#503a10' },
  }
  const rc = roleConfig[msg.role] ?? { label: msg.role, bg: '#222', color: '#ccc', border: '#444' }

  return (
    <div key={index} style={{ marginBottom: '8px', borderRadius: '6px', border: `1px solid ${rc.border}`, background: rc.bg, overflow: 'hidden' }}>
      <div
        style={{ padding: '5px 10px', display: 'flex', alignItems: 'center', gap: '8px', cursor: isLong || msg.role === 'system' ? 'pointer' : 'default', userSelect: 'none' }}
        onClick={() => (isLong || msg.role === 'system') && setCollapsed(!collapsed)}
      >
        <span style={{ fontWeight: 700, fontSize: '0.7rem', color: rc.color, minWidth: '90px' }}>{rc.label}</span>
        {msg.tool_call_id && <code style={{ fontSize: '0.65rem', color: '#888' }}>id:{msg.tool_call_id.slice(-8)}</code>}
        {msg.name && <code style={{ fontSize: '0.65rem', color: '#aaa' }}>{msg.name}</code>}
        {(isLong || msg.role === 'system') && (
          <span style={{ marginLeft: 'auto', fontSize: '0.65rem', color: '#666' }}>
            {collapsed ? `▼ 展开（${content.length}字）` : '▲ 折叠'}
          </span>
        )}
      </div>
      {!collapsed && (
        <div style={{ padding: '6px 10px 8px', borderTop: `1px solid ${rc.border}` }}>
          {content ? (
            <CollapsibleText text={content} previewLen={2000} />
          ) : (
            <span style={{ color: '#666', fontSize: '0.75rem' }}>（无文本内容）</span>
          )}
          {msg.tool_calls && msg.tool_calls.length > 0 && (
            <div style={{ marginTop: '6px' }}>
              {msg.tool_calls.map((tc, tci) => (
                <div key={tci} style={{ background: '#2a2010', border: '1px solid #504010', borderRadius: '4px', padding: '5px 8px', marginBottom: '4px', fontSize: '0.75rem' }}>
                  <span style={{ color: '#f0a830', fontWeight: 600 }}>🔨 {tc.function.name}</span>
                  <code style={{ color: '#888', marginLeft: '8px', fontSize: '0.65rem' }}>call_id:{tc.id.slice(-8)}</code>
                  <CollapsibleText text={tc.function.arguments} previewLen={300} />
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/** Convert a JSON Schema params object to a compact one-line example string. */
function schemaToCompactExample(params: Record<string, unknown>): string {
  const props = (params as any)?.properties as Record<string, any> | undefined
  if (!props) return '{}'
  const required = new Set<string>((params as any).required ?? [])
  const reqEntries: [string, unknown][] = []
  const optEntries: [string, unknown][] = []
  for (const [key, schema] of Object.entries(props)) {
    let val: unknown
    if (schema.enum !== undefined && schema.enum.length > 0) val = schema.enum[0]
    else if (schema.default !== undefined) val = schema.default
    else if (schema.type === 'number' || schema.type === 'integer') val = 0
    else if (schema.type === 'boolean') val = false
    else if (schema.type === 'array') val = []
    else if (schema.type === 'object') val = {}
    else val = 'string'
    ;(required.has(key) ? reqEntries : optEntries).push([key, val])
  }
  const obj: Record<string, unknown> = {}
  for (const [k, v] of [...reqEntries, ...optEntries]) obj[k] = v
  return JSON.stringify(obj)
}

function ToolsMetaBadge({ meta }: { meta: ToolsMeta }) {
  const [open, setOpen] = useState(false)
  const skills = meta.skills_meta ?? []
  return (
    <div style={{ marginBottom: '8px', borderRadius: '6px', border: '1px solid #2d3a5a', background: '#0e1525', overflow: 'hidden' }}>
      <div
        style={{ padding: '5px 10px', display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', userSelect: 'none' }}
        onClick={() => setOpen(!open)}
      >
        <span style={{ fontWeight: 700, fontSize: '0.7rem', color: '#60a0e0', minWidth: '90px' }}>🔧 TOOLS配置</span>
        <span style={{ fontSize: '0.68rem', color: meta.parallel_tool_calls ? '#50d080' : '#e06060' }}>
          parallel_tool_calls: {meta.parallel_tool_calls ? '✅ true' : '❌ false'}
        </span>
        <span style={{ fontSize: '0.68rem', color: '#888' }}>tool_choice: {meta.tool_choice}</span>
        <span style={{ fontSize: '0.68rem', color: '#aaa' }}>{meta.tools.length} 个工具</span>
        {skills.length > 0 && (
          <span style={{ fontSize: '0.68rem', color: '#c0a060' }}>✨ {skills.length} 个默认SKILL</span>
        )}
        <span style={{ marginLeft: 'auto', fontSize: '0.65rem', color: '#666' }}>{open ? '▲ 折叠' : '▼ 展开'}</span>
      </div>
      {open && (
        <div style={{ padding: '6px 10px 8px', borderTop: '1px solid #1a2a4a' }}>
          {/* ── Skills section ── */}
          {skills.length > 0 && (
            <div style={{ marginBottom: '10px' }}>
              <div style={{ fontSize: '0.68rem', color: '#c0a060', fontWeight: 700, marginBottom: '5px' }}>✨ 默认暴露 SKILL</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                {skills.map(s => (
                  <div key={s.slug} style={{ display: 'flex', alignItems: 'baseline', gap: '6px', background: '#1a1808', border: '1px solid #3a3010', borderRadius: '4px', padding: '4px 8px' }}>
                    <code style={{ fontSize: '0.65rem', color: '#e8c840', minWidth: '120px', flexShrink: 0 }}>{s.slug}</code>
                    <span style={{ fontSize: '0.7rem', color: '#d0b860', fontWeight: 600 }}>{s.title}</span>
                    {s.summary && <span style={{ fontSize: '0.66rem', color: '#888', marginLeft: '4px' }}>{s.summary}</span>}
                    {s.step_id && <code style={{ fontSize: '0.6rem', color: '#666', marginLeft: 'auto' }}>{s.step_id}</code>}
                  </div>
                ))}
              </div>
            </div>
          )}
          {/* ── Tool name badges ── */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginBottom: meta.tool_schemas.length > 0 ? '8px' : 0 }}>
            {meta.tools.map(t => (
              <code key={t} style={{ fontSize: '0.65rem', background: '#1a2030', padding: '2px 6px', borderRadius: '3px', color: '#9ab0d0' }}>{t}</code>
            ))}
          </div>
          {/* ── Tool schemas (compact) ── */}
          {meta.tool_schemas.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              {meta.tool_schemas.map((schema) => (
                <div key={schema.name} style={{ border: '1px solid #223455', borderRadius: '6px', background: '#111827', padding: '6px 8px' }}>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px', marginBottom: '3px' }}>
                    <code style={{ color: '#7ec8e3', fontSize: '0.72rem', fontWeight: 700, flexShrink: 0 }}>{schema.name}</code>
                    {schema.description && (
                      <span style={{ color: '#8898aa', fontSize: '0.67rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={schema.description}>
                        {schema.description.split('\n')[0]}
                      </span>
                    )}
                  </div>
                  <code style={{ fontSize: '0.65rem', color: '#a0c0a0', wordBreak: 'break-all', lineHeight: 1.4 }}>
                    {schemaToCompactExample(schema.parameters as Record<string, unknown>)}
                  </code>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function PromptSourcesBlock({ sources }: { sources: PromptSource[] }) {
  if (!sources.length) return null
  return (
    <div style={{ marginBottom: '8px', borderRadius: '6px', border: '1px solid #d6e4f0', background: '#f8fbff', overflow: 'hidden' }}>
      <div style={{ padding: '6px 10px', fontWeight: 700, fontSize: '0.75rem', color: '#355c7d' }}>
        🧩 运行时提示词来源
      </div>
      <div style={{ padding: '0 10px 8px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
        {sources.map((src) => (
          <details key={src.prompt_key} style={{ border: '1px solid #dfe8f2', borderRadius: '6px', background: '#fff' }}>
            <summary style={{ cursor: 'pointer', padding: '8px 10px', display: 'flex', alignItems: 'center', gap: '8px' }}>
              <code style={{ fontSize: '0.72rem', color: '#355c7d', fontWeight: 700 }}>{src.prompt_key}</code>
              <span style={{ fontSize: '0.75rem', color: '#445' }}>{src.title}</span>
              <span style={{
                marginLeft: 'auto',
                fontSize: '0.68rem',
                padding: '0.12rem 0.4rem',
                borderRadius: 999,
                background: src.override ? 'rgba(56,142,60,.12)' : 'rgba(230,81,0,.12)',
                color: src.override ? '#2e7d32' : '#e65100',
              }}>
                {src.override ? 'override 生效' : '默认内容'}
              </span>
            </summary>
            <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: '0.78rem', background: '#f8fbff', margin: 0, padding: '0 10px 10px' }}>
              {src.content || '（空）'}
            </pre>
          </details>
        ))}
      </div>
    </div>
  )
}

function ConversationView({ turns, toolsMeta, promptSources }: { turns: ConversationTurn[]; toolsMeta: Record<string, ToolsMeta>; promptSources: Record<string, PromptSource[]> }) {
  if (turns.length === 0) {
    return (
      <div style={{ padding: '2rem', textAlign: 'center', color: '#666' }}>
        <p>📭 暂无完整对话记录</p>
        <p style={{ fontSize: '0.8rem', marginTop: '0.5rem' }}>新会话运行后会自动记录所有提示词和消息。历史会话需重新运行才能查看。</p>
      </div>
    )
  }

  // Deduplicate: for gather/execute, keep last round; for design/review, keep the last one per phase
  const phaseLastTurn: Record<string, ConversationTurn> = {}
  const executeRounds: ConversationTurn[] = []
  const gatherRounds: ConversationTurn[] = []
  for (const t of turns) {
    if (t.phase === 'execute') executeRounds.push(t)
    else if (t.phase === 'gather') gatherRounds.push(t)
    else phaseLastTurn[t.phase] = t
  }

  const sections: Array<{ title: string; messages: LlmMessage[]; key: string }> = []
  if (gatherRounds.length > 0) {
    const lastGather = gatherRounds[gatherRounds.length - 1]
    sections.push({
      title: `🔍 Gather 阶段完整对话（${gatherRounds.length - 1} 轮工具调用，最终快照）`,
      messages: lastGather.messages,
      key: 'gather',
    })
  }
  if (phaseLastTurn['design']) sections.push({ title: '📐 Design 阶段（发送给 AI 的完整消息）', messages: phaseLastTurn['design'].messages, key: 'design' })
  if (phaseLastTurn['review']) sections.push({ title: '🔍 Review 阶段（发送给 AI 的完整消息）', messages: phaseLastTurn['review'].messages, key: 'review' })
  if (executeRounds.length > 0) {
    const lastRound = executeRounds[executeRounds.length - 1]
    sections.push({
      title: `⚙️ Execute 阶段完整对话（共 ${executeRounds.length} 轮，最终第 ${lastRound.round ?? executeRounds.length} 轮快照，含最后一次 AI 回复）`,
      messages: lastRound.messages,
      key: 'execute',
    })
  }

  return (
    <div style={{ padding: '0.5rem' }}>
      <p style={{ fontSize: '0.75rem', color: '#666', marginBottom: '1rem' }}>
        💡 系统提示词默认折叠，点击标题展开。这里展示的是实际对话快照；执行阶段最后一轮会包含 AI 的最终回复。
      </p>
      {sections.map(sec => (
        <div key={sec.key} style={{ marginBottom: '1.5rem' }}>
          <h4 style={{ fontSize: '0.85rem', color: '#aaa', marginBottom: '0.5rem', padding: '6px 8px', background: '#1a1a2e', borderRadius: '4px' }}>
            {sec.title}
          </h4>
          {promptSources[sec.key] && <PromptSourcesBlock sources={promptSources[sec.key]} />}
          {toolsMeta[sec.key] && <ToolsMetaBadge meta={toolsMeta[sec.key]} />}
          {sec.messages.map((msg, i) => <LlmMessageBubble key={i} msg={msg} index={i} />)}
        </div>
      ))}
    </div>
  )
}

// ─── 主组件 ────────────────────────────────────────────────────────────────
export default function AgentTest() {
  const [search, setSearch] = useSearchParams()
  const qp = Number(search.get('project') || '')
  const [localPid, setLocalPid] = useState(
    () => Number(localStorage.getItem(LS_KEY + '_pid') || '0') || 0
  )
  const projectId = Number.isFinite(qp) && qp > 0 ? Math.floor(qp) : localPid
  const [mode, setMode] = useState<'init' | 'maintain'>('maintain')
  const [message, setMessage] = useState('请说明本项目的表结构与下一步建议。')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [formCollapsed, setFormCollapsed] = useState(true)

  // 错误浮层 6 秒后自动消失（用户也可点 × 关闭）
  useEffect(() => {
    if (!err) return
    const t = setTimeout(() => setErr(null), 6000)
    return () => clearTimeout(t)
  }, [err])

  // 当前会话实时状态
  const [curSession, setCurSession] = useState<Session | null>(null)
  const [livePhase, setLivePhase] = useState<string>('')
  const [phases, setPhases] = useState<Record<string, PhaseState>>({})
  const [tools, setTools] = useState<ToolEntry[]>([])
  const [routeInfo, setRouteInfo] = useState<RouteInfo | null>(null)
  const [metrics, setMetrics] = useState<Metrics | null>(null)
  const [conversationTurns, setConversationTurns] = useState<ConversationTurn[]>([])
  const [toolsMeta, setToolsMeta] = useState<Record<string, ToolsMeta>>({})
  const [promptSources, setPromptSources] = useState<Record<string, PromptSource[]>>({})
  const [activeView, setActiveView] = useState<'phases' | 'conversation'>('phases')

  // 历史
  const [history, setHistory] = useState<Session[]>(() => {
    try {
      const s = localStorage.getItem(LS_KEY + '_history')
      return s ? (JSON.parse(s) as Session[]) : []
    } catch { return [] }
  })
  const [viewSession, setViewSession] = useState<Session | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  const headers = useMemo(
    () => (projectId > 0 ? projectHeaders(projectId) : ({} as Record<string, string>)),
    [projectId]
  )

  function syncQuery(pid: number) {
    const n = new URLSearchParams(search)
    if (pid > 0) n.set('project', String(pid))
    else n.delete('project')
    setSearch(n, { replace: true })
  }

  /** 从服务端拉取项目历史会话（init/maintain 任意模式） */
  const loadServerHistory = useCallback(async () => {
    if (projectId <= 0) return
    try {
        const d = (await apiFetch('/agent/sessions?limit=50', { headers })) as {
          sessions?: {
            id: number
            step_id: string
            status: string
          started_at: string
            finished_at: string | null
            design_text: string
            review_text: string
            execute_text: string
            tools: Array<{
              callId?: string
              name?: string
              label?: string
              arguments?: string
              status?: string
              resultPreview?: string | null
            }>
            error_text: string | null
            user_message: string
          }[]
        }
      const list = Array.isArray(d.sessions) ? d.sessions : []
      const serverSessions: Session[] = list.map((s) => {
        const evs: SseEvent[] = []
        const stepId = s.step_id || ''
        const isInit = !!stepId && stepId !== 'maintain' && stepId !== 'init'
        const mode: 'init' | 'maintain' = (stepId === 'maintain') ? 'maintain' : (isInit ? 'init' : (stepId === 'init' ? 'init' : 'maintain'))
        const phaseEntries: [string, string][] = [
          ['design', s.design_text || ''],
          ['review', s.review_text || ''],
          ['execute', s.execute_text || ''],
        ]
        for (const [phase, text] of phaseEntries) {
          if (text) evs.push({ ts: s.started_at, raw: { phase, type: 'token', text } })
        }
          for (const t of (Array.isArray(s.tools) ? s.tools : [])) {
            evs.push({
              ts: s.started_at,
              raw: {
                phase: 'execute',
                type: 'tool_call',
                name: t.name || '',
                arguments: t.arguments ?? '',
              },
            })
            if (t.resultPreview != null) {
              evs.push({
                ts: s.finished_at || s.started_at,
                raw: {
                  phase: 'execute',
                  type: 'tool_result',
                  name: t.name || '',
                  preview: t.resultPreview ?? '',
                },
              })
            }
          }
        if (s.error_text) {
          evs.push({ ts: s.finished_at || s.started_at, raw: { phase: 'execute', type: 'error', message: s.error_text } })
        }
        const totalMs = s.finished_at ? new Date(s.finished_at).getTime() - new Date(s.started_at).getTime() : null
        const status: Metrics['status'] = s.status === 'error' ? 'error' : s.status === 'done' ? 'done' : 'running'
          return {
            id: `srv_${s.id}`,
            projectId,
            mode,
            userMessage: s.user_message || `[${stepId || mode}] 服务端记录 #${s.id}`,
            events: evs,
            metrics: {
            startedAt: s.started_at, finishedAt: s.finished_at,
            totalMs, designMs: null, reviewMs: null, executeMs: null,
            toolCalls: 0, tokenHint: null, status,
          },
        }
      })
      // 合并：本地 in-memory 会话置顶，服务端补充剩余（去重）
      setHistory((prev) => {
        const ids = new Set(prev.map((p) => p.id))
        const merged = [...prev, ...serverSessions.filter((s) => !ids.has(s.id))]
        return merged.slice(0, 50)
      })
    } catch (e) {
      // 静默：没有数据库或权限时忽略
      console.warn('[agent-monitor] load server sessions failed', e)
    }
  }, [projectId, headers])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadServerHistory()
    }, 0)
    return () => window.clearTimeout(timer)
  }, [loadServerHistory])

  function initPhase(): PhaseState {
    return { started: null, finished: null, text: '', logs: [], error: null, hasContent: false }
  }

  function startPhaseState(p: string) {
    const now = new Date().toISOString()
    setPhases(prev => ({
      ...prev,
      [p]: { ...(prev[p] ?? initPhase()), started: prev[p]?.started ?? now },
    }))
    setLivePhase(p)
  }

  function appendPhaseText(p: string, chunk: string) {
    setPhases(prev => ({
      ...prev,
      [p]: { ...(prev[p] ?? initPhase()), text: (prev[p]?.text ?? '') + chunk, hasContent: true },
    }))
  }

  function appendPhaseLog(p: string, msg: string) {
    setPhases(prev => ({
      ...prev,
      [p]: { ...(prev[p] ?? initPhase()), logs: [...(prev[p]?.logs ?? []), msg] },
    }))
  }

  function finishPhase(p: string, error?: string) {
    const now = new Date().toISOString()
    setPhases(prev => ({
      ...prev,
      [p]: { ...(prev[p] ?? initPhase()), finished: now, error: error ?? null, hasContent: prev[p]?.hasContent || false },
    }))
  }

  function pushTool(entry: Omit<ToolEntry, 'idx' | 'expanded'>) {
    setTools(prev => {
      const idx = prev.length
      return [...prev, { ...entry, idx, expanded: false }]
    })
  }

  const resetView = useCallback(() => {
    setLivePhase(''); setPhases({}); setTools([]); setRouteInfo(null)
    setMetrics(null); setErr(null); setCurSession(null); setViewSession(null)
    setConversationTurns([])
    setToolsMeta({})
    setPromptSources({})
  }, [])

  const runAgent = async (e: FormEvent) => {
    e.preventDefault()
    if (projectId <= 0 || !message.trim() || busy) return
    resetView()
    setBusy(true)
    localStorage.setItem(LS_KEY + '_pid', String(projectId))

    const now = new Date().toISOString()
    const sessionId = genId()
    const accEvents: SseEvent[] = []
    const startMetrics: Metrics = {
      startedAt: now, finishedAt: null, totalMs: null,
      designMs: null, reviewMs: null, executeMs: null,
      toolCalls: 0, tokenHint: null, status: 'running',
    }
    setMetrics({ ...startMetrics })

    const phaseTimes: Record<string, string> = {}
    let toolCount = 0

    const abort = new AbortController()
    abortRef.current = abort

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: message.trim(), mode }),
        signal: abort.signal,
      })
      if (!res.ok) throw new Error(await res.text())
      const reader = res.body?.getReader()
      if (!reader) throw new Error('无响应流')
      const dec = new TextDecoder()
      let buf = ''

      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        const parts = buf.split('\n\n')
        buf = parts.pop() || ''
        for (const block of parts) {
          if (!block.startsWith('data:')) continue
          const line = block.replace(/^data:\s*/i, '').trim()
          let raw: Record<string, unknown>
          try { raw = JSON.parse(line) as Record<string, unknown> }
          catch { raw = { type: 'parse_error', line } }

          const ev: SseEvent = { ts: new Date().toISOString(), raw }
          accEvents.push(ev)

          const phase = String(raw.phase ?? '')
          const type = String(raw.type ?? '')
          const msg = String(raw.message ?? '')

          // 记录阶段开始时间
          if (phase && !phaseTimes[phase + '_start']) {
            phaseTimes[phase + '_start'] = ev.ts
            startPhaseState(phase)
          }

          if (type === 'token') {
            appendPhaseText(phase, String(raw.text ?? ''))
          } else if (type === 'log') {
            appendPhaseLog(phase, msg)
          } else if (type === 'error') {
            finishPhase(phase, msg)
          } else if (type === 'tool_call') {
            toolCount++
            pushTool({
              ts: ev.ts, kind: 'call', name: String(raw.name ?? ''),
              body: String(raw.arguments ?? ''),
            })
          } else if (type === 'tool_result') {
            pushTool({
              ts: ev.ts, kind: 'result', name: String(raw.name ?? ''),
              body: String(raw.preview ?? ''),
            })
          } else if (type === 'phase_messages') {
            const msgs = raw.messages as LlmMessage[] | undefined
            if (Array.isArray(msgs) && msgs.length > 0) {
              setConversationTurns(prev => [...prev, {
                phase: String(raw.phase ?? phase),
                round: raw.round !== undefined ? Number(raw.round) : undefined,
                messages: msgs,
              }])
            }
          } else if (type === 'tools_meta') {
            const metaPhase = String(raw.phase ?? phase)
            setToolsMeta(prev => ({
              ...prev,
              [metaPhase]: {
                phase: metaPhase,
                tools: (raw.tools as string[] | undefined) ?? [],
                tool_schemas: Array.isArray(raw.tool_schemas) ? raw.tool_schemas as ToolSchema[] : [],
                parallel_tool_calls: Boolean(raw.parallel_tool_calls),
                tool_choice: String(raw.tool_choice ?? 'auto'),
                skills_meta: Array.isArray(raw.skills_meta) ? raw.skills_meta as SkillMeta[] : [],
              },
            }))
          } else if (type === 'prompt_sources') {
            const srcPhase = String(raw.phase ?? phase)
            setPromptSources(prev => ({
              ...prev,
              [srcPhase]: Array.isArray(raw.sources) ? raw.sources as PromptSource[] : [],
            }))
          } else if (type === 'done') {
            const fin = new Date().toISOString()
            setMetrics({
              ...startMetrics,
              finishedAt: fin,
              totalMs: msDiff(now, fin),
              designMs: msDiff(phaseTimes['design_start'] ?? null, phaseTimes['design_end'] ?? fin),
              reviewMs: msDiff(phaseTimes['review_start'] ?? null, phaseTimes['review_end'] ?? fin),
              executeMs: msDiff(phaseTimes['execute_start'] ?? null, fin),
              toolCalls: toolCount,
              status: 'done',
            })
            finishPhase(phase)
            setLivePhase('')
          } else if (type === 'prompt_route') {
            setRouteInfo({
              step: String(raw.step_id ?? raw.step ?? ''),
              template_key: String(raw.template_key ?? ''),
              log: msg,
              hit: raw.hit != null ? Boolean(raw.hit) : null,
              prompt: raw.prompt ? String(raw.prompt) : null,
              gatherHint: raw.gather_hint ? String(raw.gather_hint) : null,
              routeSystem: raw.route_system ? String(raw.route_system) : null,
              rationale: raw.rationale ? String(raw.rationale) : null,
              skills: Array.isArray(raw.skills) ? raw.skills as Array<{ id?: number; slug: string; title: string }> : [],
            })
          }
        }
      }
    } catch (x) {
      if ((x as Error).name !== 'AbortError') {
        const errMsg = x instanceof Error ? x.message : String(x)
        setErr(errMsg)
        setMetrics(prev => prev ? { ...prev, status: 'error', finishedAt: new Date().toISOString() } : null)
      }
    } finally {
      setBusy(false)
      abortRef.current = null
    }

    // 保存会话到历史
    const finalMetrics: Metrics = {
      ...startMetrics,
      finishedAt: new Date().toISOString(),
      totalMs: Date.now() - new Date(now).getTime(),
      toolCalls: toolCount,
      status: err ? 'error' : 'done',
    }
    const sess: Session = {
      id: sessionId, projectId, mode, userMessage: message.trim(),
      events: accEvents, metrics: finalMetrics,
    }
    setCurSession(sess)
    setHistory(prev => {
      const next = [sess, ...prev].slice(0, MAX_HISTORY)
      try { localStorage.setItem(LS_KEY + '_history', JSON.stringify(next)) } catch { /* quota */ }
      return next
    })
  }

  function stopAgent() {
    abortRef.current?.abort()
    setBusy(false)
    setLivePhase('')
  }

  function loadSession(s: Session) {
    resetView()
    setViewSession(s)
    const rebuilt = rebuildFromEvents(s.events)
    setPhases(rebuilt.phases)
    setTools(rebuilt.tools)
    setMetrics({ ...s.metrics, toolCalls: rebuilt.toolCalls })
    setRouteInfo(rebuilt.routeInfo)
    setConversationTurns(rebuilt.turns)
    setToolsMeta(rebuilt.toolsMeta)
    setPromptSources(rebuilt.promptSources)

    // 若是服务端会话，尝试拉取完整对话记录
    if (s.id.startsWith('srv_')) {
      const numId = s.id.replace('srv_', '')
      void apiFetch(`/agent/sessions/${numId}`, { headers })
        .then((d: unknown) => {
          const data = d as { messages?: ConversationTurn[]; events?: Record<string, unknown>[] }
          const rawEvents = Array.isArray(data?.events)
            ? data.events.map((raw) => ({ ts: s.metrics.startedAt, raw }))
            : []
          if (rawEvents.length > 0) {
            const next = rebuildFromEvents(rawEvents)
            // 服务器端 events_json 不含 token 事件，所以 rebuild 出的阶段文本可能为空
            // 只在服务器端数据有实质内容时才覆盖本地重建结果
            const hasServerPhaseData = Object.values(next.phases).some(
              (p) => (p.text && p.text.length > 0) || p.hasContent
            )
            if (hasServerPhaseData) {
              setPhases(next.phases)
              setTools(next.tools)
              setRouteInfo(next.routeInfo)
              setToolsMeta(next.toolsMeta)
              setPromptSources(next.promptSources)
            }
            // 无论是否有阶段文本，conversation 和 metrics 始终使用服务器端数据
            setConversationTurns(next.turns)
            setMetrics(prev => prev ? { ...prev, toolCalls: next.toolCalls } : prev)
          } else {
            const msgs = data?.messages
            if (Array.isArray(msgs) && msgs.length > 0) {
              setConversationTurns(msgs as ConversationTurn[])
            }
          }
        })
        .catch(() => {/* 静默失败 */})
    }
  }

  // 当前展示：活跃会话 or 历史回看
  const displayPhases = phases
  const displayTools = tools
  const displayMetrics = metrics
  const isViewing = !!viewSession && !busy

  const phaseOrder: Array<{ key: string; label: string }> = [
    { key: 'route', label: '路由' },
    { key: 'design', label: '设计 CoT' },
    { key: 'review', label: '二次审核' },
    { key: 'execute', label: '执行' },
  ]

  return (
    <div className="agent-monitor">
      <HistorySidebar
        history={history}
        currentId={isViewing ? viewSession!.id : (curSession?.id ?? null)}
        onSelect={loadSession}
        onClear={() => {
          setHistory([])
          localStorage.removeItem(LS_KEY + '_history')
        }}
        onRefresh={() => void loadServerHistory()}
      />

      <div className="am-main">
        {/* 顶部栏 */}
        <div className="am-topbar">
          <h1>⚡ Agent Monitor</h1>
          <span className="muted">Numflow Agent 全链路监控</span>
          <Link to="/projects" style={{ marginLeft: 'auto', fontSize: '0.78rem', color: 'var(--green)' }}>← 项目</Link>
          {projectId > 0 && (
            <Link to={`/workbench/${projectId}`} style={{ fontSize: '0.78rem', color: 'var(--green)' }}>
              工作台 #{projectId}
            </Link>
          )}
          <a
            href="#"
            style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}
            onClick={(e) => { e.preventDefault(); downloadJson(`agent-monitor-${Date.now()}.json`, { history }) }}
          >导出历史</a>
        </div>

        <div className="am-body">
          {/* 调用表单 */}
          <div className="am-form-card">
            <h2 onClick={() => setFormCollapsed(!formCollapsed)} style={{ cursor: 'pointer', userSelect: 'none' }}>
              {formCollapsed ? '▸' : '▾'} 调用参数
            </h2>
            {!formCollapsed && (
            <>
            <form onSubmit={runAgent}>
              <div className="am-form-row">
                <label>
                  项目 ID
                  <input type="number" min={1} value={projectId > 0 ? projectId : ''}
                    placeholder="如 12"
                    onChange={(e) => { const v = Math.floor(Number(e.target.value) || 0); setLocalPid(v); if (v > 0) syncQuery(v) }} />
                </label>
                <label>
                  模式
                  <select value={mode} onChange={(e) => setMode(e.target.value as 'init' | 'maintain')}>
                    <option value="maintain">维护 Agent</option>
                    <option value="init">初始化 Agent</option>
                  </select>
                </label>
              </div>
              <p className="muted small" style={{ margin: '0 0 0.6rem' }}>
                当前监控项目：<code>{projectId > 0 ? `#${projectId}` : '未选择'}</code>
              </p>
              <AutoTextarea
                className="am-form-textarea"
                maxRows={12}
                markdown
                value={message}
                onChange={(e) => setMessage(e.target.value)}
                placeholder="用户消息…"
              />
              <div className="am-form-actions">
                <button type="submit" className="btn primary" disabled={busy || projectId <= 0}>
                  {busy ? '⟳ 流式接收中…' : '▶ 开始调用'}
                </button>
                {busy && <button type="button" className="btn danger" onClick={stopAgent}>■ 停止</button>}
                <button type="button" className="btn ghost" onClick={resetView} disabled={busy}>清空</button>
                {(curSession || viewSession) && (
                  <button type="button" className="btn ghost"
                    onClick={() => downloadJson(`agent-sess-${Date.now()}.json`, curSession ?? viewSession)}>
                    导出 JSON
                  </button>
                )}
              </div>
            </form>
            {err && (
              <div
                className="err banner"
                role="alert"
                style={{
                  position: 'fixed',
                  top: 16,
                  right: 16,
                  zIndex: 9999,
                  maxWidth: 480,
                  padding: '0.75rem 1rem',
                  background: '#3a1f1f',
                  color: '#ffd6d6',
                  border: '1px solid #c0392b',
                  borderRadius: 6,
                  boxShadow: '0 6px 20px rgba(0,0,0,0.4)',
                  display: 'flex',
                  alignItems: 'flex-start',
                  gap: '0.5rem',
                }}
              >
                <span style={{ flex: 1, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                  ⚠ {err}
                </span>
                <button
                  type="button"
                  onClick={() => setErr(null)}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    color: '#ffd6d6',
                    cursor: 'pointer',
                    fontSize: '1.1rem',
                    lineHeight: 1,
                  }}
                  aria-label="关闭"
                >
                  ×
                </button>
              </div>
            )}
            {isViewing && (
              <p className="muted small" style={{ marginTop: '0.4rem' }}>
                📋 正在查看历史会话：{viewSession!.userMessage.slice(0, 60)} ({viewSession!.metrics.startedAt.slice(0, 16).replace('T', ' ')})
              </p>
            )}
            </>
            )}
          </div>

          {/* 进度 + 指标 */}
          {(busy || displayMetrics) && (
            <>
              {busy && <PhaseProgress current={livePhase} status="running" />}
              {displayMetrics && (
                <div className="am-metrics">
                  <div className="am-metric">
                    <span className="am-metric-label">总时长</span>
                    <span className={`am-metric-value ${displayMetrics.status === 'error' ? 'red' : displayMetrics.status === 'done' ? 'green' : 'orange'}`}>
                      {fmtMs(displayMetrics.totalMs ?? msDiff(displayMetrics.startedAt, new Date().toISOString()))}
                    </span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">设计阶段</span>
                    <span className="am-metric-value">{fmtMs(displayMetrics.designMs)}</span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">审核阶段</span>
                    <span className="am-metric-value">{fmtMs(displayMetrics.reviewMs)}</span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">执行阶段</span>
                    <span className="am-metric-value">{fmtMs(displayMetrics.executeMs)}</span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">工具调用</span>
                    <span className="am-metric-value">{displayMetrics.toolCalls}</span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">状态</span>
                    <span className={`am-metric-value ${displayMetrics.status === 'error' ? 'red' : displayMetrics.status === 'done' ? 'green' : 'orange'}`}>
                      {displayMetrics.status === 'running' ? '运行中' : displayMetrics.status === 'done' ? '完成' : '失败'}
                    </span>
                  </div>
                  {displayMetrics.startedAt && (
                    <div className="am-metric">
                      <span className="am-metric-label">开始时间</span>
                      <span className="am-metric-value" style={{ fontSize: '0.75rem' }}>{displayMetrics.startedAt.slice(11, 19)}</span>
                    </div>
                  )}
                </div>
              )}
            </>
          )}

          {/* 视图切换 Tab 栏 */}
          {(displayMetrics || busy || conversationTurns.length > 0) && (
            <div style={{ display: 'flex', gap: '4px', marginBottom: '0.5rem', borderBottom: '1px solid #2a2a3e', paddingBottom: '4px' }}>
              <button
                type="button"
                onClick={() => setActiveView('phases')}
                style={{
                  padding: '5px 14px', fontSize: '0.8rem', border: 'none', borderRadius: '4px 4px 0 0', cursor: 'pointer',
                  background: activeView === 'phases' ? '#2a3060' : 'transparent',
                  color: activeView === 'phases' ? '#7ec8e3' : '#888',
                  fontWeight: activeView === 'phases' ? 700 : 400,
                }}>
                📊 三阶段面板
              </button>
              <button
                type="button"
                onClick={() => setActiveView('conversation')}
                style={{
                  padding: '5px 14px', fontSize: '0.8rem', border: 'none', borderRadius: '4px 4px 0 0', cursor: 'pointer',
                  background: activeView === 'conversation' ? '#2a3060' : 'transparent',
                  color: activeView === 'conversation' ? '#7ec8e3' : '#888',
                  fontWeight: activeView === 'conversation' ? 700 : 400,
                }}>
                💬 完整对话 {conversationTurns.length > 0 ? `(${conversationTurns.length}段)` : ''}
              </button>
            </div>
          )}

          {/* 路由信息 */}
          {routeInfo && activeView === 'phases' && (
            <div className="am-route-block">
              <h4>🔀 提示词路由</h4>
              {routeInfo.step && <p>步骤：<code>{routeInfo.step}</code></p>}
              {routeInfo.hit != null && (
                <p>命中默认模板：<code style={{ color: routeInfo.hit ? '#388e3c' : '#e65100' }}>{routeInfo.hit ? '✅ 是' : '❌ 否（LLM 生成）'}</code></p>
              )}
              {routeInfo.rationale && <p style={{ fontSize: '0.85rem', color: '#666' }}>理由：{routeInfo.rationale}</p>}
              {routeInfo.skills.length > 0 && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginTop: '0.45rem' }}>
                  {routeInfo.skills.map((skill) => (
                    <span key={skill.slug} style={{ fontSize: '0.72rem', padding: '0.16rem 0.45rem', borderRadius: 999, background: 'rgba(64,158,255,.12)', color: '#1f6fb2' }}>
                      SKILL · {skill.title} ({skill.slug})
                    </span>
                  ))}
                </div>
              )}
              {routeInfo.routeSystem && (
                <details style={{ marginTop: '0.4rem' }}>
                  <summary style={{ cursor: 'pointer', fontWeight: 600, fontSize: '0.85rem' }}>🤖 路由 Agent System Prompt</summary>
                  <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: '0.8rem', background: '#f8f8f8', padding: '0.6rem', borderRadius: '4px', marginTop: '0.3rem' }}>{routeInfo.routeSystem}</pre>
                </details>
              )}
              {routeInfo.prompt && routeInfo.prompt !== 'recovery' && (
                <details style={{ marginTop: '0.4rem' }}>
                  <summary style={{ cursor: 'pointer', fontWeight: 600, fontSize: '0.85rem' }}>📋 注入 execute/design/review 的路由提示词</summary>
                  <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: '0.8rem', background: '#f8f8f8', padding: '0.6rem', borderRadius: '4px', marginTop: '0.3rem' }}>{routeInfo.prompt}</pre>
                </details>
              )}
              {routeInfo.gatherHint && (
                <details style={{ marginTop: '0.4rem' }}>
                  <summary style={{ cursor: 'pointer', fontWeight: 600, fontSize: '0.85rem' }}>🔍 注入 gather 的轻量提示（已过滤写操作）</summary>
                  <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: '0.8rem', background: '#f0f8ff', padding: '0.6rem', borderRadius: '4px', marginTop: '0.3rem' }}>{routeInfo.gatherHint}</pre>
                </details>
              )}
            </div>
          )}

          {activeView === 'phases' && (
            <>
              <PromptSourcesBlock
                sources={[
                  ...(promptSources.gather ?? []),
                  ...(promptSources.design ?? []),
                  ...(promptSources.review ?? []),
                  ...(promptSources.execute ?? []),
                ]}
              />
              <div style={{ marginBottom: '1rem' }}>
                {toolsMeta.gather && <ToolsMetaBadge meta={toolsMeta.gather} />}
                {toolsMeta.execute && <ToolsMetaBadge meta={toolsMeta.execute} />}
              </div>
            </>
          )}

          {/* 三阶段面板 */}
          {activeView === 'phases' && phaseOrder.map(({ key, label }) => {
            const ps = displayPhases[key]
            if (!ps && livePhase !== key) return null
            const phaseTool = key === 'execute' ? displayTools : undefined
            const isLive = busy && livePhase === key
            return (
              <PhasePanel
                key={key} phase={key} label={label}
                state={ps ?? initPhase()}
                toolEntries={phaseTool}
                live={isLive}
              />
            )
          })}

          {/* 完整对话视图 */}
          {activeView === 'conversation' && (
            <ConversationView turns={conversationTurns} toolsMeta={toolsMeta} promptSources={promptSources} />
          )}

          {/* 等待开始提示 */}
          {!busy && !displayMetrics && (
            <p className="muted" style={{ textAlign: 'center', padding: '2rem' }}>
              填写参数后点击「▶ 开始调用」，Agent 三阶段流程将在此实时展示。
            </p>
          )}
        </div>
      </div>
    </div>
  )
}
