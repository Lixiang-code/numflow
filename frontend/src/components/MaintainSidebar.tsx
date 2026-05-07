/**
 * ============================================================
 * 这里才是「维护 Agent 侧边栏」——右侧覆盖式可收起面板
 * 不是底部 Agent 面板（底部那个在 Workbench.tsx <footer> 里）
 * ============================================================
 */
import React, { useState, useEffect, useRef, useCallback } from 'react'
import { projectHeaders } from '../api'
import './MaintainSidebar.css'

interface SessionSummary {
  id: number
  session_name: string
  created_at: string
  updated_at: string
}

interface ToolDetail {
  type: 'tool_call' | 'tool_result'
  name: string
  arguments?: string
  result?: string
}

interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  tool_details?: ToolDetail[]
}

/** 实时流式事件（SSE 过程中逐条追加） */
type LiveEvent =
  | { id: number; type: 'log'; message: string }
  | { id: number; type: 'tool_call'; name: string; label: string; callId: string; args: string }
  | { id: number; type: 'tool_result'; name: string; callId: string; result: string }
  | { id: number; type: 'error'; message: string }

interface Props {
  projectId: number | null
  currentTable: string | null
  cellSelection: string | null
}

/** 单条实时事件行 */
const MsLiveEventRow: React.FC<{ ev: LiveEvent }> = ({ ev }) => {
  const [open, setOpen] = useState(false)
  if (ev.type === 'log') {
    return <div className="ms-live-log">📋 {ev.message}</div>
  }
  if (ev.type === 'error') {
    return <div className="ms-live-error">❌ {ev.message}</div>
  }
  if (ev.type === 'tool_call') {
    return (
      <div className="ms-live-tool-call">
        <div className="ms-live-tool-head" onClick={() => setOpen((v) => !v)}>
          <span className="ms-live-tool-icon">🔨</span>
          <span className="ms-live-tool-name">{ev.label || ev.name}</span>
          <span className="ms-live-tool-chevron">{open ? '▾' : '▸'}</span>
        </div>
        {open && (
          <pre className="ms-live-tool-body">
            {(() => {
              try { return JSON.stringify(JSON.parse(ev.args), null, 2) }
              catch { return ev.args }
            })()}
          </pre>
        )}
      </div>
    )
  }
  if (ev.type === 'tool_result') {
    return (
      <div className="ms-live-tool-result">
        <div className="ms-live-tool-head" onClick={() => setOpen((v) => !v)}>
          <span className="ms-live-tool-icon">📋</span>
          <span className="ms-live-tool-name">{ev.name} 返回</span>
          <span className="ms-live-tool-chevron">{open ? '▾' : '▸'}</span>
        </div>
        {open && (
          <pre className="ms-live-tool-body">{ev.result.slice(0, 3000)}</pre>
        )}
      </div>
    )
  }
  return null
}

const MaintainSidebar: React.FC<Props> = ({ projectId, currentTable, cellSelection }) => {
  const [isOpen, setIsOpen] = useState(false)
  const [sessions, setSessions] = useState<SessionSummary[]>([])
  const [activeSessionId, setActiveSessionId] = useState<number | null>(null)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [streamingText, setStreamingText] = useState('')
  const [liveEvents, setLiveEvents] = useState<LiveEvent[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [loadingSessions, setLoadingSessions] = useState(false)
  const chatBodyRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const liveEventIdRef = useRef(0)

  const headers = React.useMemo(
    () => (projectId ? projectHeaders(projectId) : {}),
    [projectId],
  )

  const loadSessions = useCallback(async () => {
    if (!projectId) return
    setLoadingSessions(true)
    try {
      const res = await fetch('/api/agent/maintain/sessions', {
        credentials: 'include',
        headers,
      })
      if (res.ok) {
        const data = await res.json()
        setSessions(data.sessions || [])
      }
    } catch { /* ignore */ }
    finally { setLoadingSessions(false) }
  }, [projectId, headers])

  useEffect(() => {
    if (isOpen) loadSessions()
  }, [isOpen, loadSessions])

  const loadSessionMessages = async (sid: number) => {
    try {
      const res = await fetch(`/api/agent/maintain/sessions/${sid}`, {
        credentials: 'include',
        headers,
      })
      if (res.ok) {
        const data = await res.json()
        setMessages(data.messages || [])
      }
    } catch { /* ignore */ }
  }

  const handleNewSession = () => {
    setActiveSessionId(null)
    setMessages([])
    setStreamingText('')
    setLiveEvents([])
  }

  const handleSelectSession = async (sid: number) => {
    setActiveSessionId(sid)
    setStreamingText('')
    setLiveEvents([])
    await loadSessionMessages(sid)
  }

  const handleDeleteSession = async (sid: number) => {
    if (!window.confirm('确定删除此会话？')) return
    try {
      await fetch(`/api/agent/maintain/sessions/${sid}`, {
        method: 'DELETE',
        credentials: 'include',
        headers,
      })
      // 立即从 state 移除，无需等待服务端重新拉取
      setSessions((prev) => prev.filter((s) => s.id !== sid))
      if (activeSessionId === sid) handleNewSession()
    } catch { /* ignore */ }
  }

  /** 调用后端为指定会话生成 AI 标题，成功后更新列表显示 */
  const generateSessionTitle = useCallback(async (sid: number) => {
    if (!projectId) return
    try {
      const res = await fetch(`/api/agent/maintain/sessions/${sid}/generate_title`, {
        method: 'POST',
        credentials: 'include',
        headers,
      })
      if (res.ok) {
        const data = await res.json() as { session_id: number; session_name: string }
        if (data.session_name) {
          setSessions((prev) =>
            prev.map((s) => (s.id === data.session_id ? { ...s, session_name: data.session_name } : s))
          )
        }
      }
    } catch { /* ignore */ }
  }, [projectId, headers])

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || busy || !projectId) return

    const userMsg = input.trim()
    setInput('')
    setBusy(true)
    setLiveEvents([])
    liveEventIdRef.current = 0

    // 记录发送时是否为新会话（用于 done 后重载列表）
    const wasNewSession = !activeSessionId

    const userChat: ChatMessage = { role: 'user', content: userMsg }
    setMessages((prev) => [...prev, userChat])

    let assistantContent = ''
    const toolDetails: ToolDetail[] = []
    let freshSessionId: number | null = null   // 由 session_init 填入
    setMessages((prev) => [...prev, { role: 'assistant', content: '', tool_details: [] }])

    try {
      const body: Record<string, unknown> = {
        message: userMsg,
        current_table: currentTable || null,
        cell_selection: cellSelection || null,
      }
      if (activeSessionId) body.session_id = activeSessionId

      const res = await fetch('/api/agent/maintain/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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
            const ev = JSON.parse(line) as Record<string, unknown>
            const etype = ev.type as string

            if (etype === 'session_init') {
              // 后端返回 session_id，更新本地状态以保证多轮对话连续
              const sid = ev.session_id as number
              if (sid) {
                freshSessionId = sid
                setActiveSessionId(sid)
              }

            } else if (etype === 'token') {
              assistantContent += (ev.text as string) || ''
              setStreamingText(assistantContent)

            } else if (etype === 'tool_call') {
              const eid = liveEventIdRef.current++
              const name = (ev.name as string) || ''
              const label = (ev.label as string) || name
              const callId = (ev.call_id as string) || String(eid)
              const args = (ev.arguments as string) || '{}'
              setLiveEvents((prev) => [...prev, { id: eid, type: 'tool_call', name, label, callId, args }])
              toolDetails.push({ type: 'tool_call', name, arguments: args })

            } else if (etype === 'tool_result') {
              const eid = liveEventIdRef.current++
              const name = (ev.name as string) || ''
              const callId = (ev.call_id as string) || String(eid)
              const result = (ev.result as string) || ''
              setLiveEvents((prev) => [...prev, { id: eid, type: 'tool_result', name, callId, result }])
              toolDetails.push({ type: 'tool_result', name, result })

            } else if (etype === 'log') {
              const eid = liveEventIdRef.current++
              const message = (ev.message as string) || ''
              setLiveEvents((prev) => [...prev, { id: eid, type: 'log', message }])

            } else if (etype === 'error') {
              const eid = liveEventIdRef.current++
              const message = (ev.message as string) || ''
              setLiveEvents((prev) => [...prev, { id: eid, type: 'error', message }])

            } else if (etype === 'done') {
              setMessages((prev) => {
                const copy = [...prev]
                const last = copy[copy.length - 1]
                if (last && last.role === 'assistant') {
                  last.content = assistantContent
                  last.tool_details = toolDetails
                }
                return copy
              })
              setStreamingText('')
              setLiveEvents([])
              if (wasNewSession) {
                // 先刷新列表（加入新会话），再异步生成 AI 标题并更新
                const sidForTitle = freshSessionId
                loadSessions().then(() => {
                  if (sidForTitle) generateSessionTitle(sidForTitle)
                })
              }
            }
          } catch { /* ignore */ }
        }
      }
    } catch (err) {
      setMessages((prev) => {
        const copy = [...prev]
        const last = copy[copy.length - 1]
        if (last && last.role === 'assistant') {
          last.content = `错误：${err instanceof Error ? err.message : String(err)}`
        }
        return copy
      })
      setStreamingText('')
      setLiveEvents([])
    } finally { setBusy(false) }
  }

  // textarea 自动拉高（最高5行）
  const adjustTextarea = useCallback(() => {
    const ta = textareaRef.current
    if (!ta) return
    ta.style.height = 'auto'
    const lineH = 20
    const maxH = lineH * 5 + 14
    ta.style.height = Math.min(ta.scrollHeight, maxH) + 'px'
    ta.style.overflowY = ta.scrollHeight > maxH ? 'auto' : 'hidden'
  }, [])

  useEffect(() => { adjustTextarea() }, [input, adjustTextarea])

  useEffect(() => {
    if (chatBodyRef.current) chatBodyRef.current.scrollTop = chatBodyRef.current.scrollHeight
  }, [messages, streamingText, liveEvents])

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === 'm') { e.preventDefault(); setIsOpen((v) => !v) }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  useEffect(() => {
    if (isOpen && textareaRef.current) textareaRef.current.focus()
  }, [isOpen])

  return (
    <>
      {!isOpen && (
        <div className="ms-toggle-bar" onClick={() => setIsOpen(true)} title="维护 Agent (Ctrl+M)">
          <span className="ms-toggle-icon">◀</span>
          <span className="ms-toggle-label">维<br/>护</span>
        </div>
      )}

      {isOpen && (
        <div className="ms-sidebar">
          <div className="ms-header">
            <div className="ms-header-top">
              <h3 className="ms-title">维护 Agent</h3>
              <div className="ms-header-actions">
                <button className="ms-btn ms-btn-new" onClick={handleNewSession} disabled={busy} title="新建会话">
                  ＋新会话
                </button>
                <button className="ms-btn ms-btn-collapse" onClick={() => setIsOpen(false)} title="收起面板 (Ctrl+M)">
                  ▶
                </button>
              </div>
            </div>
            {sessions.length > 0 && (
              <div className="ms-session-row">
                <select
                  className="ms-session-select"
                  value={activeSessionId ?? ''}
                  onChange={(e) => {
                    const v = e.target.value
                    if (v === '') handleNewSession()
                    else handleSelectSession(Number(v))
                  }}
                  disabled={busy}
                >
                  <option value="">-- 切换会话 --</option>
                  {sessions.map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.session_name || `会话 #${s.id}`} ({s.updated_at?.slice(0, 10)})
                    </option>
                  ))}
                </select>
                {activeSessionId && (
                  <button
                    className="ms-btn ms-btn-delete"
                    onClick={() => handleDeleteSession(activeSessionId)}
                    disabled={busy}
                    title="删除当前会话"
                  >
                    删除
                  </button>
                )}
              </div>
            )}
            {loadingSessions && <div className="ms-loading-hint">加载中…</div>}
          </div>

          <div className="ms-chat-body" ref={chatBodyRef}>
            {messages.length === 0 && !streamingText && liveEvents.length === 0 && (
              <div className="ms-empty">
                <p>维护 Agent 帮助修改已有数值表。</p>
                <p>输入你的需求，例如：</p>
                <ul>
                  <li>"把 equip_base 的 atk 列膨胀率从 10% 改成 12%"</li>
                  <li>"给 monster_model 新增一个 boss 模型"</li>
                  <li>"检查 num_resource_framework 的 gold 曲线是否合理"</li>
                </ul>
              </div>
            )}
            {messages.map((msg, i) => (
              <div key={i} className={`ms-msg ms-msg-${msg.role}`}>
                <div className="ms-msg-role">{msg.role === 'user' ? '👤 你' : '🤖 维护 Agent'}</div>
                <div className="ms-msg-content">
                  {msg.content}
                  {msg.tool_details && msg.tool_details.length > 0 && (
                    <div className="ms-tool-details">
                      {msg.tool_details.map((td, j) => (
                        <details key={j} className="ms-tool-item">
                          <summary className={`ms-tool-summary ms-tool-${td.type}`}>
                            {td.type === 'tool_call' ? `🔨 ${td.name}` : `📋 ${td.name} 返回`}
                          </summary>
                          <pre className="ms-tool-body">
                            {td.type === 'tool_call' ? td.arguments : td.result?.slice(0, 2000)}
                          </pre>
                        </details>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ))}

            {/* 实时流式区域（执行过程中逐条展示） */}
            {busy && (streamingText || liveEvents.length > 0) && (
              <div className="ms-msg ms-msg-assistant">
                <div className="ms-msg-role">
                  🤖 维护 Agent
                  <span className="ms-busy-dot" />
                </div>
                <div className="ms-msg-content">
                  {streamingText && (
                    <div className="ms-stream-text">{streamingText}</div>
                  )}
                  {liveEvents.length > 0 && (
                    <div className="ms-live-events">
                      {liveEvents.map((ev) => (
                        <MsLiveEventRow key={ev.id} ev={ev} />
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}
            {busy && !streamingText && liveEvents.length === 0 && (
              <div className="ms-thinking">思考中…</div>
            )}
          </div>

          <form className="ms-footer" onSubmit={handleSend}>
            <div className="ms-footer-meta">
              {currentTable && (
                <span className="ms-table-tag" title={`当前表：${currentTable}`}>📋 {currentTable}</span>
              )}
              <button className="ms-btn ms-btn-send" type="submit" disabled={busy || !input.trim()}>
                {busy ? '⏳ 执行中…' : '发送'}
              </button>
            </div>
            <textarea
              ref={textareaRef}
              className="ms-input"
              value={input}
              onChange={(e) => { setInput(e.target.value); adjustTextarea() }}
              onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(e as unknown as React.FormEvent) } }}
              placeholder={currentTable ? `对 ${currentTable} 做什么修改？` : '描述你需要修改的内容…（Shift+Enter 换行）'}
              disabled={busy}
              rows={1}
            />
          </form>
        </div>
      )}
    </>
  )

}

export default MaintainSidebar
