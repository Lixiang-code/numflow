import { useEffect, useState } from 'react'
import './App.css'

type Health = { status: string }

type DiagnosticsConfig = {
  dashscope_api_key_configured: boolean
  model: string
  hint?: string
}

type CacheSummary = {
  round1_cache_creation_input_tokens?: number | null
  round1_cached_tokens_read?: number | null
  round2_cache_creation_input_tokens?: number | null
  round2_cached_tokens_read?: number | null
  explicit_ephemeral_cache_hit?: boolean
  note_round1_zero_creation?: string
}

type DiagnosticsRun = {
  model?: string
  cache_summary?: CacheSummary
  connectivity?: { ok: boolean; assistant_preview?: string; error?: string }
  cache_rounds?: { ok: boolean; error?: string }
}

function App() {
  const [health, setHealth] = useState<Health | null>(null)
  const [error, setError] = useState<string | null>(null)

  const [cfg, setCfg] = useState<DiagnosticsConfig | null>(null)
  const [run, setRun] = useState<DiagnosticsRun | null>(null)
  const [runErr, setRunErr] = useState<string | null>(null)
  const [runBusy, setRunBusy] = useState(false)

  useEffect(() => {
    fetch('/api/health')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data: Health) => setHealth(data))
      .catch((e: Error) => setError(e.message))
  }, [])

  useEffect(() => {
    fetch('/api/agent/diagnostics')
      .then((r) => r.json())
      .then((data: DiagnosticsConfig) => setCfg(data))
      .catch(() => setCfg(null))
  }, [])

  const runQwenDiagnostics = () => {
    setRunBusy(true)
    setRunErr(null)
    setRun(null)
    fetch('/api/agent/diagnostics/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
      .then(async (r) => {
        const data = await r.json()
        if (!r.ok) throw new Error((data as { detail?: string }).detail || `HTTP ${r.status}`)
        return data as DiagnosticsRun
      })
      .then((data) => setRun(data))
      .catch((e: Error) => setRunErr(e.message))
      .finally(() => setRunBusy(false))
  }

  return (
    <main className="shell">
      <h1>Numflow</h1>
      <p className="muted">FastAPI + React（Vite）· 千问 Agent 诊断</p>
      <section className="card">
        <h2>后端 /api/health</h2>
        {error && <p className="err">{error}</p>}
        {!error && health === null && <p>加载中…</p>}
        {health && <pre>{JSON.stringify(health, null, 2)}</pre>}
      </section>

      <section className="card">
        <h2>千问（DashScope）配置</h2>
        {!cfg && <p>加载中…</p>}
        {cfg && (
          <ul className="kv">
            <li>
              <span>API Key 已配置</span>
              <strong>{cfg.dashscope_api_key_configured ? '是' : '否'}</strong>
            </li>
            <li>
              <span>模型</span>
              <strong>{cfg.model}</strong>
            </li>
          </ul>
        )}
        <p className="muted small">
          密钥来自 <code>backend/.env</code> 的 <code>DASHSCOPE_API_KEY</code>（勿提交仓库）。
        </p>
        <button type="button" className="btn" disabled={runBusy} onClick={runQwenDiagnostics}>
          {runBusy ? '正在调用百炼（约半分钟）…' : '运行连通性 + 显式缓存自检'}
        </button>
        {runErr && <p className="err">{runErr}</p>}
        {run?.cache_summary && (
          <div className="cachebox">
            <h3>缓存摘要</h3>
            <ul className="kv">
              <li>
                <span>第 1 轮创建缓存（输入 token）</span>
                <strong>{String(run.cache_summary.round1_cache_creation_input_tokens ?? '—')}</strong>
              </li>
              <li>
                <span>第 1 轮读缓存（cached_tokens）</span>
                <strong>{String(run.cache_summary.round1_cached_tokens_read ?? '—')}</strong>
              </li>
              <li>
                <span>第 2 轮命中缓存（cached_tokens）</span>
                <strong>{String(run.cache_summary.round2_cached_tokens_read ?? '—')}</strong>
              </li>
              <li>
                <span>显式 ephemeral 命中</span>
                <strong>{run.cache_summary.explicit_ephemeral_cache_hit ? '是' : '否'}</strong>
              </li>
            </ul>
            {run.cache_summary.note_round1_zero_creation && (
              <p className="muted small">{run.cache_summary.note_round1_zero_creation}</p>
            )}
          </div>
        )}
        {run?.connectivity && (
          <p className="preview">
            <span className="muted">连通性回复节选：</span>
            {run.connectivity.ok
              ? run.connectivity.assistant_preview
              : run.connectivity.error}
          </p>
        )}
        {run && (
          <details className="raw">
            <summary>完整 JSON 响应</summary>
            <pre>{JSON.stringify(run, null, 2)}</pre>
          </details>
        )}
      </section>
    </main>
  )
}

export default App
