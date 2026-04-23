import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'

type TableInfo = { table_name: string; validation_status: string; layer: string }

export default function Workbench() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  /** 必须 memo：否则每次 render 新 headers 对象会触发 effect/useCallback 无限循环 → 浏览器 ERR_INSUFFICIENT_RESOURCES */
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [tables, setTables] = useState<TableInfo[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [readme, setReadme] = useState('')
  const [globalReadme, setGlobalReadme] = useState('')
  const [pipeline, setPipeline] = useState<{
    next_expected_step: string | null
    completed_steps: string[]
    finished?: boolean
  } | null>(null)
  const [agentLog, setAgentLog] = useState<string[]>([])
  const [agentStream, setAgentStream] = useState('')
  const [agentInput, setAgentInput] = useState('')
  const [agentBusy, setAgentBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const loadTables = useCallback(async () => {
    const d = (await apiFetch('/meta/tables', { headers })) as { tables: TableInfo[] }
    setTables(d.tables)
    setSelected((sel) => sel ?? (d.tables[0]?.table_name ?? null))
  }, [headers])

  const loadGlobal = useCallback(async () => {
    const cfg = (await apiFetch('/meta/project-config', { headers })) as {
      settings: Record<string, { text?: string } | unknown>
    }
    const g = cfg.settings.global_readme as { text?: string } | undefined
    setGlobalReadme(g?.text || '')
  }, [headers])

  const loadPipeline = useCallback(async () => {
    const s = (await apiFetch('/pipeline/status', { headers })) as {
      next_expected_step: string | null
      completed_steps: string[]
      finished: boolean
    }
    setPipeline(s)
  }, [headers])

  useEffect(() => {
    if (!Number.isFinite(pid)) return
    let cancelled = false
    setErr(null)
    setTables([])
    setSelected(null)
    void Promise.all([loadTables(), loadGlobal(), loadPipeline()]).catch((e) => {
      if (!cancelled) setErr(String(e))
    })
    return () => {
      cancelled = true
    }
  }, [pid, loadTables, loadGlobal, loadPipeline])

  useEffect(() => {
    if (!selected) {
      setRows([])
      setReadme('')
      return
    }
    let cancelled = false
    ;(async () => {
      try {
        const r = (await apiFetch(`/data/tables/${encodeURIComponent(selected)}/rows?limit=200`, {
          headers,
        })) as { rows: Record<string, unknown>[] }
        const m = (await apiFetch(`/meta/tables/${encodeURIComponent(selected)}/readme`, {
          headers,
        })) as { readme: string }
        if (!cancelled) {
          setRows(r.rows)
          setReadme(m.readme || '')
        }
      } catch (e) {
        if (!cancelled) setErr(String(e))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [selected, headers])

  async function advancePipeline() {
    if (!pipeline?.next_expected_step) return
    setErr(null)
    try {
      await apiFetch('/pipeline/advance', {
        method: 'POST',
        headers,
        body: JSON.stringify({ step: pipeline.next_expected_step }),
      })
      await loadPipeline()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function runAgent(e: FormEvent) {
    e.preventDefault()
    if (!agentInput.trim() || agentBusy) return
    setAgentBusy(true)
    setAgentLog((l) => [...l, `> ${agentInput}`])
    setAgentStream('')
    const msg = agentInput
    setAgentInput('')
    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg }),
      })
      if (!res.ok) throw new Error(await res.text())
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
            const ev = JSON.parse(line) as {
              type: string
              message?: string
              text?: string
              name?: string
              preview?: string
            }
            if (ev.type === 'log' && ev.message) setAgentLog((l) => [...l, `[log] ${ev.message}`])
            if (ev.type === 'tool_call') setAgentLog((l) => [...l, `[tool] ${ev.name}`])
            if (ev.type === 'tool_result' && ev.preview)
              setAgentLog((l) => [...l, `[result] ${ev.preview}`])
            if (ev.type === 'token' && ev.text) setAgentStream((s) => s + ev.text)
            if (ev.type === 'done') {
              const ft = (ev as { full_text?: string }).full_text || ''
              setAgentLog((l) => [...l, ft])
              setAgentStream('')
            }
          } catch {
            /* ignore parse */
          }
        }
      }
    } catch (x) {
      setAgentLog((l) => [...l, `错误: ${x instanceof Error ? x.message : String(x)}`])
    } finally {
      setAgentBusy(false)
    }
  }

  const cols =
    rows.length > 0
      ? Object.keys(rows[0])
      : selected
        ? ['(空表)']
        : []

  return (
    <div className="workbench">
      <header className="wb-top">
        <Link to="/projects" className="link-btn">
          项目列表
        </Link>
        <span className="muted">项目 #{pid}</span>
      </header>
      {err && <p className="err banner">{err}</p>}

      <div className="wb-body">
        <aside className="wb-left">
          <h3>表</h3>
          <button
            type="button"
            className="linkish"
            onClick={() => {
              void Promise.all([loadTables(), loadGlobal(), loadPipeline()]).catch((e) =>
                setErr(String(e)),
              )
            }}
          >
            刷新
          </button>
          <ul>
            {tables.map((t) => (
              <li key={t.table_name}>
                <button
                  type="button"
                  className={selected === t.table_name ? 'sel' : ''}
                  onClick={() => setSelected(t.table_name)}
                >
                  {t.table_name}
                  <small>{t.validation_status}</small>
                </button>
              </li>
            ))}
          </ul>
          {pipeline && (
            <div className="pipe-box">
              <h4>流水线（03）</h4>
              <p className="muted small">已完成: {pipeline.completed_steps.length} 步</p>
              <p className="small">下一步: {pipeline.next_expected_step || '—'}</p>
              <button type="button" className="btn tiny" disabled={!pipeline.next_expected_step} onClick={advancePipeline}>
                推进当前步
              </button>
            </div>
          )}
        </aside>

        <section className="wb-center">
          <h3>{selected || '未选择表'}</h3>
          <div className="table-wrap">
            <table className="grid">
              <thead>
                <tr>
                  {cols.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i}>
                    {cols.map((c) => (
                      <td key={c}>{String(r[c] ?? '')}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="wb-right">
          <h3>README</h3>
          <div className="readme-tabs">
            <strong>{selected ? `表: ${selected}` : '全局'}</strong>
          </div>
          <pre className="readme-pre">{selected ? readme || '（空）' : globalReadme || '（空）'}</pre>
        </aside>
      </div>

      <footer className="wb-agent">
        <form onSubmit={runAgent}>
          <input
            value={agentInput}
            onChange={(e) => setAgentInput(e.target.value)}
            placeholder="输入自然语言，调用维护 Agent（需配置 DASHSCOPE_API_KEY）"
            disabled={agentBusy}
          />
          <button type="submit" disabled={agentBusy}>
            {agentBusy ? '执行中…' : '发送'}
          </button>
        </form>
        {agentStream && <pre className="agent-stream">{agentStream}</pre>}
        <div className="agent-log">
          {agentLog.map((line, i) => (
            <div key={i} className="log-line">
              {line}
            </div>
          ))}
        </div>
      </footer>
    </div>
  )
}
