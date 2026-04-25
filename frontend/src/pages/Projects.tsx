import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'

type Project = {
  id: number
  name: string
  slug: string
  is_template: boolean
  can_write: boolean
}

export default function Projects() {
  const nav = useNavigate()
  const [list, setList] = useState<Project[]>([])
  const [user, setUser] = useState<{ username: string; is_admin: boolean } | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    apiFetch('/auth/me')
      .then((d) => setUser(d as { username: string; is_admin: boolean }))
      .catch(() => nav('/login', { replace: true }))
    apiFetch('/projects')
      .then((d) => {
        const raw = (d as { projects?: unknown }).projects
        const arr = Array.isArray(raw) ? raw : []
        setList(
          arr.filter((p): p is Project => {
            if (p == null || typeof p !== 'object') return false
            const o = p as { id?: unknown; name?: unknown }
            return typeof o.id === 'number' && typeof o.name === 'string'
          }),
        )
      })
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
  }, [nav])

  async function logout() {
    await apiFetch('/auth/logout', { method: 'POST' })
    nav('/login')
  }

  return (
    <div className="projects-page">
      <header className="topbar">
        <h1>Numflow</h1>
        {user && (
          <div className="topbar-right">
            <span className="muted">
              {user.username}
              {user.is_admin ? '（管理员）' : ''}
            </span>
            <Link to="/projects/new" className="link-btn">+ 新建项目</Link>
            <button type="button" className="btn ghost" onClick={logout}>退出</button>
          </div>
        )}
      </header>
      <div className="projects-body">
        {err && <p className="err">{err}</p>}
        <div className="card-grid">
          {list.map((p) => (
            <Link key={p.id} to={`/workbench/${p.id}`} className="project-card">
              <h2>{p.name}</h2>
              <p className="muted" style={{ marginTop: '0.25rem' }}>
                {p.is_template ? '模板项目' : `slug: ${p.slug}`}
              </p>
              <span className={`tag${p.can_write ? '' : ' readonly'}`}>
                {p.can_write ? '可编辑' : '只读'}
              </span>
            </Link>
          ))}
        </div>
      </div>
    </div>
  )
}
