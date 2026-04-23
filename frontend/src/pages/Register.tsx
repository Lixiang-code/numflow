import { useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'

export default function Register() {
  const nav = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [invite, setInvite] = useState('')
  const [err, setErr] = useState<string | null>(null)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setErr(null)
    try {
      await apiFetch('/auth/register', {
        method: 'POST',
        body: JSON.stringify({ username, password, invite_code: invite }),
      })
      nav('/projects', { replace: true })
    } catch (x) {
      setErr(x instanceof Error ? x.message : String(x))
    }
  }

  return (
    <div className="auth-page">
      <h1>注册</h1>
      <form onSubmit={onSubmit} className="auth-form">
        <label>
          用户名
          <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" />
        </label>
        <label>
          密码（至少 6 位）
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
          />
        </label>
        <label>
          邀请码
          <input value={invite} onChange={(e) => setInvite(e.target.value)} />
        </label>
        {err && <p className="err">{err}</p>}
        <button type="submit" className="btn primary">
          注册并登录
        </button>
      </form>
      <p className="muted">
        已有账号？<Link to="/login">登录</Link>
      </p>
    </div>
  )
}
