import { useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'

export default function Login() {
  const nav = useNavigate()
  const [username, setUsername] = useState('lixiang')
  const [password, setPassword] = useState('e8cTY7er')
  const [err, setErr] = useState<string | null>(null)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setErr(null)
    try {
      await apiFetch('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ username, password }),
      })
      nav('/projects', { replace: true })
    } catch (x) {
      setErr(x instanceof Error ? x.message : String(x))
    }
  }

  return (
    <div className="auth-page">
      <h1>登录</h1>
      <form onSubmit={onSubmit} className="auth-form">
        <label>
          用户名
          <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" />
        </label>
        <label>
          密码
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
          />
        </label>
        {err && <p className="err">{err}</p>}
        <button type="submit">进入</button>
      </form>
      <p className="muted">
        没有账号？<Link to="/register">注册</Link>
      </p>
    </div>
  )
}
