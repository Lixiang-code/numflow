import { useEffect, useState } from 'react'
import './App.css'

type Health = { status: string }

function App() {
  const [health, setHealth] = useState<Health | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/health')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data: Health) => setHealth(data))
      .catch((e: Error) => setError(e.message))
  }, [])

  return (
    <main className="shell">
      <h1>Numflow</h1>
      <p className="muted">FastAPI + React（Vite）基础框架</p>
      <section className="card">
        <h2>后端 /api/health</h2>
        {error && <p className="err">{error}</p>}
        {!error && health === null && <p>加载中…</p>}
        {health && <pre>{JSON.stringify(health, null, 2)}</pre>}
      </section>
    </main>
  )
}

export default App
