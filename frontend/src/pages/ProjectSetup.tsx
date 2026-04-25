/**
 * ProjectSetup — 项目初始化中心
 *
 * 流程：
 * 1. 加载项目配置 & pipeline 状态
 * 2. 自动启动 init agent，逐步完成所有 11 个 pipeline 步骤
 * 3. 每步完成后自动调用 /api/pipeline/advance 推进
 * 4. 11 步全部完成 → 显示"进入工作台"按钮
 *
 * 界面：
 * - 顶栏：项目名、进入工作台按钮
 * - 左侧：pipeline 11步列表（已完成✓ / 当前▶ / 待办○）
 * - 右侧主体：当前步骤的 agent 运行实况
 *   - 进度条（route/design/review/execute）
 *   - 指标（时长/工具次数/状态）
 *   - 三阶段面板（可折叠）
 *   - 工具时间线
 */
import { useCallback, useEffect, useMemo, useRef, useState, type ReactElement } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'
import { pipelineStepLabel, PIPELINE_STEP_LABELS } from '../data/pipelineSteps'

// ─── types ──────────────────────────────────────────────────────────────────
type PhaseState = {
  started: string | null
  finished: string | null
  text: string
  logs: string[]
  error: string | null
  hasContent: boolean
}
type ToolEntry = { idx: number; ts: string; kind: 'call' | 'result'; name: string; body: string }
type Metrics = {
  startedAt: string
  finishedAt: string | null
  totalMs: number | null
  toolCalls: number
  status: 'idle' | 'running' | 'done' | 'error'
}
type PipelineStatus = {
  steps_order: string[]
  completed_steps: string[]
  next_expected_step: string | null
  finished?: boolean
}
type ProjectInfo = {
  name: string
  game_type?: string
  mode?: string
}

// ─── util ────────────────────────────────────────────────────────────────────
function msDiff(a: string | null, b: string | null): number | null {
  if (!a || !b) return null
  return new Date(b).getTime() - new Date(a).getTime()
}
function fmtMs(ms: number | null): string {
  if (ms === null) return '—'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}
function nowIso() { return new Date().toISOString() }

function PhaseText({ text, live }: { text: string; live?: boolean }) {
  if (!text && !live) return <span className="muted small">（暂无内容）</span>
  const parts: ReactElement[] = text.split('\n').map((ln, i) => {
    if (ln.startsWith('## ') || ln.startsWith('# ')) return <h3 key={i}>{ln.replace(/^#+ /, '')}</h3>
    return <p key={i}>{ln || '\u00a0'}</p>
  })
  return (
    <div className="am-phase-text-formatted">
      {parts}
      {live && <span className="am-cursor" />}
    </div>
  )
}

function PhasePanel({ phaseKey, label, state, tools, live }: {
  phaseKey: string; label: string; state: PhaseState
  tools?: ToolEntry[]; live?: boolean
}) {
  const [open, setOpen] = useState(true)
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const badge = phaseKey as 'route' | 'design' | 'review' | 'execute'
  return (
    <div className="am-phase-panel">
      <div className="am-phase-header" onClick={() => setOpen(o => !o)}>
        <span className={`am-phase-badge ${badge}`}>{label}</span>
        <span className="am-phase-title">
          {state.error ? '❌ 错误' : state.hasContent ? `✓ ${state.text.length} chars` : live ? '⟳ 进行中…' : '—'}
        </span>
        {state.started && (
          <span className="am-phase-meta">
            {fmtMs(msDiff(state.started, state.finished ?? nowIso()))}
          </span>
        )}
        <span className={`am-phase-chevron${open ? ' open' : ''}`}>▶</span>
      </div>
      {open && (
        <div className="am-phase-content">
          {state.logs.length > 0 && (
            <div style={{ background: '#f5f5f5', borderRadius: '4px', padding: '0.4rem 0.6rem', marginBottom: '0.4rem', fontSize: '0.78rem' }}>
              {state.logs.map((l, i) => <div key={i}><code>{l}</code></div>)}
            </div>
          )}
          {state.error && (
            <div style={{ background: '#ffebee', borderRadius: '4px', padding: '0.4rem 0.6rem', color: '#c62828', marginBottom: '0.4rem' }}>
              {state.error}
            </div>
          )}
          {(state.hasContent || live) && <PhaseText text={state.text} live={live} />}
          {tools && tools.length > 0 && (
            <>
              <div className="am-section-label">工具调用 ({tools.length})</div>
              <div className="am-tool-timeline">
                {tools.map(e => (
                  <div key={e.idx} className="am-tool-item">
                    <div className="am-tool-item-head" onClick={() => setExpanded(s => { const n = new Set(s); n.has(e.idx) ? n.delete(e.idx) : n.add(e.idx); return n })}>
                      <span className={`am-tool-badge ${e.kind}`}>{e.kind}</span>
                      <span className="am-tool-name">{e.name}</span>
                      <span className="am-tool-time">{e.ts.slice(11, 19)}</span>
                      <span style={{ fontSize: '0.65rem', color: '#aaa', marginLeft: '4px' }}>{expanded.has(e.idx) ? '▲' : '▼'}</span>
                    </div>
                    {expanded.has(e.idx) && <div className="am-tool-item-body">{e.body}</div>}
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}

function PhaseProgress({ current, status }: { current: string; status: string }) {
  const phases = ['route', 'design', 'review', 'execute']
  const labels = ['路由', '设计', '审核', '执行']
  const cur = phases.indexOf(current)
  return (
    <div className="am-progress" style={{ marginBottom: '0.5rem' }}>
      {phases.map((p, i) => {
        let cls = ''
        if (status === 'error' && i === cur) cls = 'error'
        else if (i < cur || status === 'done') cls = 'done'
        else if (i === cur) cls = 'active'
        return (
          <div key={p} className="am-progress-phase">
            <div className={`am-progress-dot ${cls}`}>{i + 1}</div>
            <span className={`am-progress-label${cls === 'active' ? ' active' : ''}`}>{labels[i]}</span>
          </div>
        )
      })}
    </div>
  )
}

// ─── step init messages ───────────────────────────────────────────────────────
const STEP_INIT_MESSAGES: Record<string, string> = {
  environment_global_readme: '请初始化本项目，阅读项目配置，建立全局 README，说明游戏数值体系的整体结构、设计目标与各模块关系。',
  base_attribute_framework: '请根据游戏类型和属性配置，构建基本属性基础框架表（level_growth、stat_base 等），包含等级范围、成长系数基础定义。',
  gameplay_attribute_scheme: '请构建玩法系统属性方案表，明确各玩法子系统（战斗/养成/经济等）的属性分配策略，写出方案 README。',
  gameplay_allocation_tables: '请构建玩法系统属性分配表，将各玩法属性方案落地为具体数值，每个子系统一张表，含 README。',
  second_order_framework: '请构建基本属性二阶框架表（stat_scale、stat_delta），用于描述属性随等级变化的增长阶段曲线。',
  gameplay_attribute_tables: '请构建玩法系统属性表，将基础框架与分配方案合并为可执行的各玩法系统属性数值表。',
  cultivation_resource_design: '请设计养成资源体系：确定货币、材料、碎片等各类资源的类型、用途、获取来源，写出资源设计 README。',
  cultivation_resource_framework: '请构建养成资源基础框架表，按资源类型建立基础数量与稀有度梯度框架。',
  cultivation_allocation_tables: '请构建养成资源分配表，针对各养成路径（等级升级、技能升级、装备强化等）给出详细的资源消耗数值。',
  cultivation_quant_tables: '请构建养成资源定量表，整合所有养成路径的资源消耗，给出全生命周期的资源总量预算。',
  gameplay_landing_tables: '请构建玩法系统落地表，将各玩法属性表与养成数值整合，输出最终可供验证的完整数值落地表。',
}

function buildInitMessage(stepId: string, projectInfo: ProjectInfo, completedSteps: string[]): string {
  const custom = STEP_INIT_MESSAGES[stepId]
  const prefix = [
    `【初始化 Agent｜流水线步骤：${pipelineStepLabel(stepId)}】`,
    `项目：${projectInfo.name}（${projectInfo.game_type ?? '未知类型'}）`,
    completedSteps.length > 0
      ? `已完成步骤：${completedSteps.map(pipelineStepLabel).join('、')}`
      : '这是第一步',
    '',
  ].join('\n')
  return prefix + (custom ?? `请完成当前步骤（${stepId}）的交付物，建立相关表格并更新 README。`)
}

// ─── main ─────────────────────────────────────────────────────────────────────
export default function ProjectSetup() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  const nav = useNavigate()
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [projectInfo, setProjectInfo] = useState<ProjectInfo>({ name: `项目 #${pid}` })
  const [pipeline, setPipeline] = useState<PipelineStatus | null>(null)
  const [loadErr, setLoadErr] = useState<string | null>(null)

  // ── 当前 agent 运行状态 ───────────────────────────────────────────
  const [livePhase, setLivePhase] = useState('')
  const [phases, setPhases] = useState<Record<string, PhaseState>>({})
  const [tools, setTools] = useState<ToolEntry[]>([])
  const [metrics, setMetrics] = useState<Metrics | null>(null)
  const [agentErr, setAgentErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [autoMode, setAutoMode] = useState(true) // 是否自动推进每步

  // ── 当前正在运行的步骤 ────────────────────────────────────────────
  const [currentStep, setCurrentStep] = useState<string | null>(null)
  const [allDone, setAllDone] = useState(false)

  // ── 每步历史（stepId → 阶段面板状态）────────────────────────────
  const [stepHistory, setStepHistory] = useState<Array<{
    stepId: string
    phases: Record<string, PhaseState>
    tools: ToolEntry[]
    metrics: Metrics
  }>>([])

  const abortRef = useRef<AbortController | null>(null)
  // Refs so callbacks always see fresh values without stale closures
  const busyRef = useRef(false)
  const projectInfoRef = useRef(projectInfo)
  useEffect(() => { projectInfoRef.current = projectInfo }, [projectInfo])

  // ── 加载 pipeline 状态 ───────────────────────────────────────────
  const loadPipeline = useCallback(async () => {
    try {
      const [cfgData, plData] = await Promise.all([
        apiFetch('/meta/project-config', { headers }) as Promise<{ project: { name: string }; settings: Record<string, unknown> }>,
        apiFetch('/pipeline/status', { headers }) as Promise<PipelineStatus>,
      ])
      setProjectInfo({
        name: cfgData.project?.name ?? `项目 #${pid}`,
        game_type: (cfgData.settings?.core as { game_type?: string })?.game_type ?? undefined,
        mode: cfgData.settings?.mode as string ?? undefined,
      })
      setPipeline(plData)
      if (plData.finished || !plData.next_expected_step) {
        setAllDone(true)
      } else {
        setCurrentStep(plData.next_expected_step)
      }
    } catch (e) {
      setLoadErr(String(e))
    }
  }, [headers, pid])

  useEffect(() => { void loadPipeline() }, [loadPipeline])

  // ── 初始化 phase state ───────────────────────────────────────────
  function initPhase(): PhaseState {
    return { started: null, finished: null, text: '', logs: [], error: null, hasContent: false }
  }

  function resetAgentState() {
    setPhases({})
    setTools([])
    setMetrics(null)
    setAgentErr(null)
    setLivePhase('')
  }

  // ── advance pipeline ─────────────────────────────────────────────
  async function advancePipeline(stepId: string) {
    try {
      await apiFetch('/pipeline/advance', {
        method: 'POST',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ step: stepId }),
      })
    } catch (e) {
      console.warn('pipeline advance failed:', e)
    }
    // reload pipeline status
    await loadPipeline()
  }

  // ── run single step ──────────────────────────────────────────────
  const runStep = useCallback(async (stepId: string, projInfo: ProjectInfo, completedSteps: string[]) => {
    if (busyRef.current) return
    busyRef.current = true
    resetAgentState()
    setBusy(true)
    setCurrentStep(stepId)

    const startTime = nowIso()
    const phaseTimes: Record<string, string> = {}
    let toolCount = 0
    const localPhases: Record<string, PhaseState> = {}
    const localTools: ToolEntry[] = []

    function getOrInitPhase(p: string): PhaseState {
      if (!localPhases[p]) localPhases[p] = initPhase()
      return localPhases[p]
    }

    const abort = new AbortController()
    abortRef.current = abort

    const message = buildInitMessage(stepId, projInfo, completedSteps)

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, mode: 'init' }),
        signal: abort.signal,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)

      const reader = res.body?.getReader()
      if (!reader) throw new Error('无响应流')
      const dec = new TextDecoder()
      let buf = ''

      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        const parts = buf.split('\n\n')
        buf = parts.pop() ?? ''

        for (const block of parts) {
          if (!block.startsWith('data:')) continue
          const line = block.replace(/^data:\s*/i, '').trim()
          let raw: Record<string, unknown>
          try { raw = JSON.parse(line) as Record<string, unknown> }
          catch { continue }

          const phase = String(raw.phase ?? '')
          const type = String(raw.type ?? '')

          if (phase && !phaseTimes[phase + '_start']) {
            phaseTimes[phase + '_start'] = nowIso()
            getOrInitPhase(phase).started = phaseTimes[phase + '_start']
            setLivePhase(phase)
            setPhases(prev => ({ ...prev, [phase]: { ...getOrInitPhase(phase) } }))
          }

          if (type === 'token') {
            getOrInitPhase(phase).text += String(raw.text ?? '')
            getOrInitPhase(phase).hasContent = true
          } else if (type === 'log') {
            getOrInitPhase(phase).logs.push(String(raw.message ?? ''))
          } else if (type === 'error') {
            getOrInitPhase(phase).error = String(raw.message ?? '')
          } else if (type === 'tool_call') {
            toolCount++
            localTools.push({ idx: localTools.length, ts: nowIso(), kind: 'call', name: String(raw.name ?? ''), body: String(raw.arguments ?? '') })
          } else if (type === 'tool_result') {
            localTools.push({ idx: localTools.length, ts: nowIso(), kind: 'result', name: String(raw.name ?? ''), body: String(raw.preview ?? '') })
          } else if (type === 'done') {
            const fin = nowIso()
            getOrInitPhase(phase).finished = fin
            setMetrics({
              startedAt: startTime,
              finishedAt: fin,
              totalMs: msDiff(startTime, fin),
              toolCalls: toolCount,
              status: 'done',
            })
          }

          // batch update all phases + tools every event
          setPhases({ ...localPhases })
          setTools([...localTools])
          setMetrics({
            startedAt: startTime,
            finishedAt: null,
            totalMs: msDiff(startTime, nowIso()),
            toolCalls: toolCount,
            status: 'running',
          })
        }
      }
    } catch (e) {
      if ((e as Error).name !== 'AbortError') {
        const msg = e instanceof Error ? e.message : String(e)
        setAgentErr(msg)
        setMetrics({ startedAt: startTime, finishedAt: nowIso(), totalMs: msDiff(startTime, nowIso()), toolCalls: toolCount, status: 'error' })
        setBusy(false)
        return
      }
    }

    // save to history
    const finalMetrics: Metrics = {
      startedAt: startTime,
      finishedAt: nowIso(),
      totalMs: msDiff(startTime, nowIso()),
      toolCalls: toolCount,
      status: 'done',
    }
    setStepHistory(prev => [{ stepId, phases: { ...localPhases }, tools: [...localTools], metrics: finalMetrics }, ...prev])
    setLivePhase('')
    busyRef.current = false
    setBusy(false)
    abortRef.current = null

    // advance pipeline
    await advancePipeline(stepId)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [headers])

  // ── auto-start: triggers when pipeline.next_expected_step changes (including initial load).
  // For brand-new projects, completed_steps is always [] so .length=0 wouldn't re-trigger;
  // using next_expected_step as dep ensures firing when pipeline goes null → loaded.
  useEffect(() => {
    if (!pipeline || allDone || !autoMode) return
    const step = pipeline.next_expected_step
    if (!step) return
    if (busyRef.current) return
    const completedSteps = pipeline.completed_steps ?? []
    const t = setTimeout(() => {
      if (busyRef.current) return
      void runStep(step, projectInfoRef.current, completedSteps)
    }, 600)
    return () => clearTimeout(t)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipeline?.next_expected_step, allDone, autoMode])

  // ── stop ─────────────────────────────────────────────────────────
  function stop() {
    setAutoMode(false)
    abortRef.current?.abort()
    setBusy(false)
    setLivePhase('')
  }

  // ── view history step ─────────────────────────────────────────────
  const [viewHistoryIdx, setViewHistoryIdx] = useState<number | null>(null)
  const viewingHistory = viewHistoryIdx !== null ? stepHistory[viewHistoryIdx] : null

  const displayPhases = viewingHistory?.phases ?? phases
  const displayTools = viewingHistory?.tools ?? tools
  const displayMetrics = viewingHistory?.metrics ?? metrics

  const allSteps = pipeline?.steps_order ?? Object.keys(PIPELINE_STEP_LABELS)
  const completed = new Set(pipeline?.completed_steps ?? [])
  const totalSteps = allSteps.length
  const doneCount = completed.size
  const progressPct = totalSteps > 0 ? Math.round((doneCount / totalSteps) * 100) : 0

  const phaseOrder = [
    { key: 'route', label: '路由' },
    { key: 'design', label: '设计 CoT' },
    { key: 'review', label: '二次审核' },
    { key: 'execute', label: '执行' },
  ]

  return (
    <div className="ps-root">
      {/* ── 顶部导航栏 ─────────────────────────────────────────── */}
      <header className="ps-header">
        <div className="ps-header-left">
          <Link to="/projects" className="ps-nav-link">← 项目列表</Link>
          <h1>{projectInfo.name}</h1>
          {projectInfo.game_type && <span className="ps-tag">{projectInfo.game_type}</span>}
          {loadErr && <span className="ps-err-tag">⚠ {loadErr}</span>}
        </div>
        <div className="ps-header-right">
          {busy && (
            <button type="button" className="btn ghost small" onClick={stop}>
              ■ 暂停自动推进
            </button>
          )}
          {!busy && !allDone && pipeline?.next_expected_step && (
            <button
              type="button"
              className="btn secondary small"
              onClick={() => {
                setAutoMode(true)
                if (pipeline?.next_expected_step) {
                  void runStep(pipeline.next_expected_step, projectInfo, pipeline.completed_steps ?? [])
                }
              }}
            >
              ▶ 继续下一步
            </button>
          )}
          {allDone && (
            <span className="ps-done-badge">✓ 全部完成！</span>
          )}
          <button
            type="button"
            className="btn primary"
            onClick={() => nav(`/workbench/${pid}`)}
          >
            进入工作台 →
          </button>
        </div>
      </header>

      <div className="ps-body">
        {/* ── 左侧：pipeline 步骤列表 ─────────────────────────── */}
        <aside className="ps-sidebar">
          <div className="ps-sidebar-head">
            <span>初始化进度</span>
            <span className="ps-progress-text">{doneCount}/{totalSteps}</span>
          </div>
          {/* 总进度条 */}
          <div className="ps-overall-bar">
            <div className="ps-overall-bar-fill" style={{ width: `${progressPct}%` }} />
          </div>

          <ul className="ps-step-list">
            {allSteps.map((stepId, i) => {
              const isDone = completed.has(stepId)
              const isCurrent = stepId === currentStep
              const histIdx = stepHistory.findIndex(h => h.stepId === stepId)
              return (
                <li
                  key={stepId}
                  className={`ps-step-item${isDone ? ' done' : isCurrent ? ' current' : ''}`}
                  onClick={() => {
                    if (histIdx >= 0) setViewHistoryIdx(histIdx)
                    else setViewHistoryIdx(null)
                  }}
                >
                  <span className="ps-step-icon">
                    {isDone ? '✓' : isCurrent ? (busy ? '⟳' : '▶') : String(i + 1)}
                  </span>
                  <span className="ps-step-label" title={stepId}>
                    {pipelineStepLabel(stepId)}
                  </span>
                  {histIdx >= 0 && stepHistory[histIdx].metrics.status === 'done' && (
                    <span className="ps-step-time">{fmtMs(stepHistory[histIdx].metrics.totalMs)}</span>
                  )}
                </li>
              )
            })}
          </ul>

          {/* 已完成步骤快速回顾 */}
          {stepHistory.length > 0 && (
            <div className="ps-history-note muted small" style={{ padding: '0.5rem 0.75rem', borderTop: '1px solid var(--border)' }}>
              点击步骤查看详情
            </div>
          )}
        </aside>

        {/* ── 主体：当前步骤 agent 输出 ──────────────────────── */}
        <main className="ps-main">
          {/* 正在查看历史 banner */}
          {viewingHistory && (
            <div className="ps-viewing-banner">
              📋 查看历史步骤：{pipelineStepLabel(viewingHistory.stepId)}
              <button type="button" className="btn ghost small" onClick={() => setViewHistoryIdx(null)}>
                回到当前
              </button>
            </div>
          )}

          {/* 当前步骤标题 */}
          {!viewingHistory && currentStep && (
            <div className="ps-step-header">
              <span className="ps-step-num">步骤 {(pipeline?.completed_steps.length ?? 0) + 1}/{totalSteps}</span>
              <h2>{pipelineStepLabel(currentStep)}</h2>
              <code className="muted small">{currentStep}</code>
            </div>
          )}

          {/* allDone 状态 */}
          {allDone && !viewingHistory && (
            <div className="ps-all-done">
              <div className="ps-all-done-icon">🎉</div>
              <h2>所有 {totalSteps} 个步骤已完成！</h2>
              <p className="muted">项目初始化完成，所有基础表格已由 Agent 生成，现在可以进入工作台查看和编辑。</p>
              <button type="button" className="btn primary large" onClick={() => nav(`/workbench/${pid}`)}>
                进入工作台 →
              </button>
            </div>
          )}

          {/* 错误提示 */}
          {agentErr && !viewingHistory && (
            <div className="ps-agent-err">
              <strong>Agent 出错：</strong>{agentErr}
              <button type="button" className="btn ghost small" style={{ marginLeft: '1rem' }}
                onClick={() => {
                  setAgentErr(null)
                  if (pipeline?.next_expected_step) {
                    void runStep(pipeline.next_expected_step, projectInfo, pipeline.completed_steps ?? [])
                  }
                }}>
                重试
              </button>
            </div>
          )}

          {/* Agent phase 进度 + 指标 */}
          {(busy || displayMetrics) && !allDone && (
            <div className="ps-agent-status">
              {busy && <PhaseProgress current={livePhase} status={busy ? 'running' : 'done'} />}
              {displayMetrics && (
                <div className="am-metrics" style={{ marginTop: '0.4rem' }}>
                  <div className="am-metric">
                    <span className="am-metric-label">时长</span>
                    <span className={`am-metric-value ${displayMetrics.status === 'error' ? 'red' : displayMetrics.status === 'done' ? 'green' : 'orange'}`}>
                      {fmtMs(displayMetrics.totalMs ?? msDiff(displayMetrics.startedAt, nowIso()))}
                    </span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">工具调用</span>
                    <span className="am-metric-value">{displayMetrics.toolCalls}</span>
                  </div>
                  <div className="am-metric">
                    <span className="am-metric-label">状态</span>
                    <span className={`am-metric-value ${displayMetrics.status === 'error' ? 'red' : displayMetrics.status === 'done' ? 'green' : 'orange'}`}>
                      {displayMetrics.status === 'running' ? '运行中' : displayMetrics.status === 'done' ? '完成' : displayMetrics.status === 'error' ? '失败' : '等待'}
                    </span>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* 等待开始 */}
          {!busy && !displayMetrics && !allDone && !agentErr && !loadErr && (
            <div className="ps-waiting">
              <div className="ps-spinner" />
              <p>正在加载项目信息，稍后自动启动初始化…</p>
            </div>
          )}

          {/* 三阶段面板 */}
          <div className="ps-phases">
            {phaseOrder.map(({ key, label }) => {
              const ps = displayPhases[key]
              if (!ps && livePhase !== key) return null
              const phaseTool = key === 'execute' ? displayTools : undefined
              const isLive = busy && livePhase === key && !viewingHistory
              return (
                <PhasePanel
                  key={key}
                  phaseKey={key}
                  label={label}
                  state={ps ?? initPhase()}
                  tools={phaseTool}
                  live={isLive}
                />
              )
            })}
          </div>

          {/* 直接打开 Agent Monitor 的链接 */}
          <div style={{ margin: '1rem', textAlign: 'right' }}>
            <button
              type="button"
              className="btn ghost small"
              onClick={() => window.open(`/agent-test?project=${pid}`, 'agent_monitor', 'width=1280,height=860,resizable=yes,scrollbars=yes')}
            >
              高级 Agent 监控 ↗
            </button>
          </div>
        </main>
      </div>
    </div>
  )
}
