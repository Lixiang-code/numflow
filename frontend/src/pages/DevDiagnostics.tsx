import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiFetch } from '../api'

export default function DevDiagnostics() {
  const [health, setHealth] = useState<unknown>(null)
  const [cfg, setCfg] = useState<unknown>(null)
  const [run, setRun] = useState<unknown>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    apiFetch('/health').then(setHealth).catch(setErr)
    apiFetch('/agent/diagnostics').then(setCfg).catch(() => {})
  }, [])

  async function runDiag() {
    setBusy(true)
    setErr(null)
    try {
      const d = await apiFetch('/agent/diagnostics/run', { method: 'POST', body: '{}' })
      setRun(d)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="shell">
      <p>
        <Link to="/projects">返回项目</Link> · <Link to="/agent-test">AGENT TEST 监控/导出</Link>
      </p>
      <h1>开发诊断</h1>
      <pre>{JSON.stringify(health, null, 2)}</pre>
      <pre>{JSON.stringify(cfg, null, 2)}</pre>
      <button type="button" onClick={runDiag} disabled={busy}>
        {busy ? '运行中…' : '运行千问诊断'}
      </button>
      {err && <p className="err">{err}</p>}
      {run != null ? <pre>{JSON.stringify(run, null, 2)}</pre> : null}
    </div>
  )
}
