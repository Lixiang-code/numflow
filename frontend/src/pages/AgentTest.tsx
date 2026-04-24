import { useCallback, useMemo, useState, type FormEvent } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { projectHeaders } from '../api'

const LS_KEY = 'numflow_agent_test_settings_v1'

type SseItem = { t: string; raw: Record<string, unknown> }

type SessionRecord = {
  id: string
  projectId: number
  mode: 'init' | 'maintain'
  userMessage: string
  startedAt: string
  finishedAt: string
  eventCount: number
  error?: string
  events: SseItem[]
}

function newSessionId() {
  return `sess_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
}

function downloadJson(filename: string, obj: unknown) {
  const blob = new Blob([JSON.stringify(obj, null, 2)], { type: 'application/json;charset=utf-8' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

function downloadNdjson(filename: string, events: SseItem[]) {
  const lines = events.map((e) => JSON.stringify({ ...e.raw, _ts: e.t }))
  const blob = new Blob([lines.join('\n') + '\n'], { type: 'application/x-ndjson;charset=utf-8' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

export default function AgentTest() {
  const [search, setSearch] = useSearchParams()
  const qp = Number(search.get('project') || '')
  const [localProjectId, setLocalProjectId] = useState(
    () => Number(localStorage.getItem(LS_KEY + '_pid') || '0') || 0,
  )
  const projectId =
    Number.isFinite(qp) && qp > 0 ? Math.floor(qp) : localProjectId
  const [mode, setMode] = useState<'init' | 'maintain'>('maintain')
  const [message, setMessage] = useState('请说明本项目的表结构与下一步建议。')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [events, setEvents] = useState<SseItem[]>([])
  const [lastSession, setLastSession] = useState<SessionRecord | null>(null)

  const headers = useMemo(
    () => (projectId > 0 ? projectHeaders(projectId) : {} as Record<string, string>),
    [projectId],
  )

  const syncQuery = useCallback(
    (pid: number) => {
      const n = new URLSearchParams(search)
      if (pid > 0) n.set('project', String(pid))
      else n.delete('project')
      setSearch(n, { replace: true })
    },
    [search, setSearch],
  )

  const runAgent = async (e: FormEvent) => {
    e.preventDefault()
    if (projectId <= 0 || !message.trim() || busy) return
    setErr(null)
    setBusy(true)
    setEvents([])
    localStorage.setItem(LS_KEY + '_pid', String(projectId))
    const startedAt = new Date().toISOString()
    const acc: SseItem[] = []
    const sessionId = newSessionId()
    let streamErr: string | undefined

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: message.trim(), mode }),
      })
      if (!res.ok) {
        const t = await res.text()
        throw new Error(t || res.statusText)
      }
      const reader = res.body?.getReader()
      if (!reader) throw new Error('无响应流')
      const decoder = new TextDecoder()
      let buf = ''
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const parts = buf.split('\n\n')
        buf = parts.pop() || ''
        for (const block of parts) {
          if (!block.startsWith('data:')) continue
          const line = block.replace(/^data:\s*/i, '').trim()
          try {
            const raw = JSON.parse(line) as Record<string, unknown>
            const item: SseItem = { t: new Date().toISOString(), raw }
            acc.push(item)
            setEvents((prev) => [...prev, item])
          } catch {
            acc.push({ t: new Date().toISOString(), raw: { type: 'parse_error', line } })
            setEvents((prev) => [
              ...prev,
              { t: new Date().toISOString(), raw: { type: 'parse_error', line } },
            ])
          }
        }
      }
    } catch (x) {
      streamErr = x instanceof Error ? x.message : String(x)
      setErr(streamErr)
    } finally {
      setBusy(false)
    }

    const finishedAt = new Date().toISOString()
    const rec: SessionRecord = {
      id: sessionId,
      projectId,
      mode,
      userMessage: message.trim(),
      startedAt,
      finishedAt,
      eventCount: acc.length,
      error: streamErr,
      events: acc,
    }
    setLastSession(rec)
    try {
      localStorage.setItem(LS_KEY + '_last', JSON.stringify(rec))
    } catch {
      /* 超大时存不下则跳过 */
    }
  }

  const exportFull = () => {
    if (!lastSession) return
    const payload = {
      exportVersion: 1,
      generator: 'Numflow AgentTest',
      session: {
        id: lastSession.id,
        projectId: lastSession.projectId,
        mode: lastSession.mode,
        userMessage: lastSession.userMessage,
        startedAt: lastSession.startedAt,
        finishedAt: lastSession.finishedAt,
        eventCount: lastSession.eventCount,
        error: lastSession.error,
      },
      events: lastSession.events,
    }
    downloadJson(`numflow-agent-test-p${lastSession.projectId}-${Date.now()}.json`, payload)
  }

  const exportNdjson = () => {
    if (!lastSession?.events.length) return
    downloadNdjson(`numflow-agent-test-p${lastSession.projectId}-${Date.now()}.ndjson`, lastSession.events)
  }

  const loadLastFromStorage = () => {
    try {
      const s = localStorage.getItem(LS_KEY + '_last')
      if (!s) return
      const p = JSON.parse(s) as SessionRecord
      setLastSession(p)
      setEvents(p.events || [])
      setLocalProjectId(p.projectId)
    } catch {
      setErr('无法从本地恢复上次记录')
    }
  }

  const clearView = () => {
    setEvents([])
    setLastSession(null)
    setErr(null)
  }

  return (
    <div className="shell agent-test">
      <header className="agent-test-header">
        <h1>AGENT TEST</h1>
        <p className="muted small">
          独立监控 Agent 全量 SSE：可导出 JSON / NDJSON，与 Workbench 内 Agent 行为一致。需已登录并选择有权限的项目。
        </p>
        <nav className="agent-test-nav">
          <Link to="/projects">项目列表</Link>
          <Link to="/dev">开发诊断</Link>
          {projectId > 0 ? <Link to={`/workbench/${projectId}`}>进入工作台 (当前 #{projectId})</Link> : null}
        </nav>
      </header>

      <section className="agent-test-form card-block">
        <h2>调用参数</h2>
        <form onSubmit={runAgent}>
          <label>
            项目 ID（<code>?project=</code> 同步地址栏）
            <input
              type="number"
              min={1}
              value={projectId > 0 ? projectId : ''}
              onChange={(ev) => {
                const v = Math.floor(Number(ev.target.value) || 0)
                setLocalProjectId(v)
                if (v > 0) syncQuery(v)
              }}
              placeholder="如 3，或地址栏 ?project="
            />
          </label>
          <label>
            模式
            <select value={mode} onChange={(e) => setMode(e.target.value as 'init' | 'maintain')}>
              <option value="maintain">维护 Agent</option>
              <option value="init">初始化 Agent</option>
            </select>
          </label>
          <label className="block full">
            用户消息
            <textarea
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              rows={4}
            />
          </label>
          <div className="form-actions">
            <button type="submit" disabled={busy || projectId <= 0} className="btn primary">
              {busy ? '流式接收中…' : '开始调用并记录'}
            </button>
            <button type="button" className="btn" onClick={loadLastFromStorage}>
              从本机恢复上次
            </button>
            <button type="button" className="btn" onClick={clearView} disabled={busy}>
              清空显示
            </button>
            <button
              type="button"
              className="btn"
              onClick={exportFull}
              disabled={!lastSession || !lastSession.events.length}
            >
              导出 JSON
            </button>
            <button
              type="button"
              className="btn"
              onClick={exportNdjson}
              disabled={!lastSession || !lastSession.events.length}
            >
              导出 NDJSON
            </button>
          </div>
        </form>
        {err && <p className="err banner">{err}</p>}
        {lastSession && (
          <p className="muted small">
            上次：{lastSession.id} | 事件 {lastSession.eventCount} |{' '}
            {lastSession.startedAt} → {lastSession.finishedAt}
            {lastSession.error ? ` | 流错误: ${lastSession.error}` : ''}
          </p>
        )}
      </section>

      <section className="agent-test-tools">
        <h2>工具调用时间线</h2>
        {events.every((e) => e.raw.type !== 'tool_call' && e.raw.type !== 'tool_result') ? (
          <p className="muted">尚无 tool_call / tool_result 事件</p>
        ) : (
          <div className="table-wrap">
            <table className="grid">
              <thead>
                <tr>
                  <th>#</th>
                  <th>时间</th>
                  <th>类型</th>
                  <th>name</th>
                  <th>参数 / 摘要</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e, i) => {
                  const ty = String(e.raw.type ?? '')
                  if (ty !== 'tool_call' && ty !== 'tool_result') return null
                  return (
                    <tr key={i}>
                      <td>{i}</td>
                      <td className="mono small">{e.t}</td>
                      <td>{ty}</td>
                      <td className="mono small">{String(e.raw.name ?? '')}</td>
                      <td className="mono prewrap">
                        {ty === 'tool_call'
                          ? String(e.raw.arguments ?? '')
                          : String(e.raw.preview ?? '').slice(0, 2000)}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="agent-test-raw">
        <h2>全量事件（可滚动）</h2>
        <p className="muted small">每条含 ISO 时间戳 t 与 raw（与导出 JSON 中 events 结构一致）。</p>
        <pre className="agent-test-pre">
          {events.length
            ? events
                .map(
                  (e) =>
                    `${e.t} ${JSON.stringify(e.raw, null, 0)}\n`,
                )
                .join('')
            : '（无）'}
        </pre>
      </section>
    </div>
  )
}
