/**
 * Agent Monitor — 全功能 Agent 调试与监控窗口
 * 支持：三阶段面板(design/review/execute)、工具调用时间线、实时流、会话历史、指标面板
 * 可从 Workbench 以 window.open popup 方式打开
 */
import { useCallback, useMemo, useRef, useState, type FormEvent, type ReactElement } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { projectHeaders } from '../api'

const LS_KEY = 'numflow_agent_monitor_v2'
const MAX_HISTORY = 12

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

type RouteInfo = { step: string | null; template_key: string | null; log: string | null }

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
    setExp(s => { const n = new Set(s); n.has(idx) ? n.delete(idx) : n.add(idx); return n })
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
  history, currentId, onSelect, onClear,
}: {
  history: Session[]; currentId: string | null
  onSelect: (s: Session) => void; onClear: () => void
}) {
  return (
    <aside className="am-sidebar">
      <div className="am-sidebar-head">
        <span>会话历史</span>
        <a href="#" onClick={(e) => { e.preventDefault(); onClear() }} title="清空历史">清空</a>
      </div>
      {history.length === 0 && <p className="muted small" style={{ padding: '0.75rem' }}>暂无记录</p>}
      <ul className="am-history-list">
        {history.map((s) => (
          <li key={s.id} className={`am-history-item${s.id === currentId ? ' active' : ''}`}
            onClick={() => onSelect(s)}>
            <div className="am-history-item-title" title={s.userMessage}>
              #{s.projectId} {s.userMessage.slice(0, 28)}{s.userMessage.length > 28 ? '…' : ''}
            </div>
            <div className="am-history-item-meta">
              {s.mode} · {s.metrics.startedAt.slice(0, 16).replace('T', ' ')}
            </div>
            <span className={`am-history-item-status ${s.metrics.status === 'error' ? 'err' : s.metrics.status === 'running' ? 'running' : 'ok'}`}>
              {s.metrics.status === 'error' ? '失败' : s.metrics.status === 'running' ? '运行中' : '完成'}
            </span>
          </li>
        ))}
      </ul>
    </aside>
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

  // 当前会话实时状态
  const [curSession, setCurSession] = useState<Session | null>(null)
  const [livePhase, setLivePhase] = useState<string>('')
  const [phases, setPhases] = useState<Record<string, PhaseState>>({})
  const [tools, setTools] = useState<ToolEntry[]>([])
  const [routeInfo, setRouteInfo] = useState<RouteInfo | null>(null)
  const [metrics, setMetrics] = useState<Metrics | null>(null)

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
    pid > 0 ? n.set('project', String(pid)) : n.delete('project')
    setSearch(n, { replace: true })
  }

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
              step: String(raw.step ?? ''),
              template_key: String(raw.template_key ?? ''),
              log: msg,
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
    // rebuild phase/tool state from events
    const ph: Record<string, PhaseState> = {}
    const tl: ToolEntry[] = []
    let tc = 0
    for (const ev of s.events) {
      const phase = String(ev.raw.phase ?? '')
      const type = String(ev.raw.type ?? '')
      if (!ph[phase]) ph[phase] = initPhase()
      if (type === 'token') {
        ph[phase].text += String(ev.raw.text ?? '')
        ph[phase].hasContent = true
      } else if (type === 'log') {
        ph[phase].logs.push(String(ev.raw.message ?? ''))
      } else if (type === 'error') {
        ph[phase].error = String(ev.raw.message ?? '')
      } else if (type === 'tool_call') {
        tc++
        tl.push({ idx: tl.length, ts: ev.ts, kind: 'call', name: String(ev.raw.name ?? ''), body: String(ev.raw.arguments ?? ''), expanded: false })
      } else if (type === 'tool_result') {
        tl.push({ idx: tl.length, ts: ev.ts, kind: 'result', name: String(ev.raw.name ?? ''), body: String(ev.raw.preview ?? ''), expanded: false })
      }
    }
    setPhases(ph)
    setTools(tl)
    setMetrics({ ...s.metrics, toolCalls: tc })
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
            <h2>调用参数</h2>
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
              <textarea className="am-form-textarea" rows={3} value={message}
                onChange={(e) => setMessage(e.target.value)} placeholder="用户消息…" />
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
            {err && <p className="err banner" style={{ marginTop: '0.5rem' }}>{err}</p>}
            {isViewing && (
              <p className="muted small" style={{ marginTop: '0.4rem' }}>
                📋 正在查看历史会话：{viewSession!.userMessage.slice(0, 60)} ({viewSession!.metrics.startedAt.slice(0, 16).replace('T', ' ')})
              </p>
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

          {/* 路由信息 */}
          {routeInfo && (
            <div className="am-route-block">
              <h4>🔀 提示词路由</h4>
              {routeInfo.step && <p>当前步骤：<code>{routeInfo.step}</code></p>}
              {routeInfo.template_key && <p>路由结果：<code>{routeInfo.template_key}</code></p>}
              {routeInfo.log && <p>{routeInfo.log}</p>}
            </div>
          )}

          {/* 三阶段面板 */}
          {phaseOrder.map(({ key, label }) => {
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
