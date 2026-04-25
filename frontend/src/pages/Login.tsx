import { useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch } from '../api'

export default function Login() {
  const nav = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
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
    <div className="auth-wrap">
      <div className="auth-page">
        <h1>Numflow</h1>
        <p className="sub-title">游戏数值 AI 自动开发平台</p>
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
          <button type="submit" className="btn primary">登录</button>
        </form>
        <p className="auth-footer">
          没有账号？<Link to="/register">注册</Link>
        </p>
      </div>
    </div>
  )
}
