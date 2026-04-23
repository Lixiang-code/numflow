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
      .then((d) => setList((d as { projects: Project[] }).projects))
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
  }, [nav])

  async function logout() {
    await apiFetch('/auth/logout', { method: 'POST' })
    nav('/login')
  }

  return (
    <div className="projects-page">
      <header className="topbar">
        <h1>Numflow 项目</h1>
        {user && (
          <div className="topbar-right">
            <span className="muted">
              {user.username}
              {user.is_admin ? '（管理员）' : ''}
            </span>
            <Link to="/projects/new" className="link-btn">
              新建项目
            </Link>
            <button type="button" className="btn ghost" onClick={logout}>
              退出
            </button>
          </div>
        )}
      </header>
      {err && <p className="err">{err}</p>}
      <div className="card-grid">
        {list.map((p) => (
          <Link key={p.id} to={`/workbench/${p.id}`} className="project-card">
            <h2>{p.name}</h2>
            <p className="muted">{p.is_template ? '模板项目' : `slug: ${p.slug}`}</p>
            <p className="tag">{p.can_write ? '可编辑' : '只读'}</p>
          </Link>
        ))}
      </div>
    </div>
  )
}
