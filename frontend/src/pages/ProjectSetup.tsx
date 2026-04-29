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
type ToolEntry = {
  idx: number
  ts: string
  kind: 'call' | 'result'
  name: string
  label: string
  body: string
  callId: string
  status: 'pending' | 'done' | 'error'
}
type PairedTool = {
  idx: number
  callId: string
  name: string
  label: string
  ts: string
  status: 'pending' | 'done' | 'error'
  arguments: string
  resultPreview?: string
}
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
type GameplayTable = {
  table_id: string
  display_name: string
  readme: string
  status: '未开始' | '进行中' | '已完成' | '待修订'
  order_num: number
  dependencies: string[]
}
type RevisionRequest = {
  id: number
  table_id: string
  reason: string
  requested_by_step: string
  status: 'pending' | 'in_progress' | 'done'
  created_at: string
}
type ProjectInfo = {
  name: string
  game_type?: string
  mode?: string
}
// Status of a previously persisted server-side agent session
type SessionStatus = 'none' | 'loading' | 'done' | 'error' | 'interrupted'
type ServerSession = {
  id: number
  step_id: string
  status: string
  started_at: string
  finished_at: string | null
  design_text: string
  review_text: string
  execute_text: string
  tools: { callId: string; name: string; label: string; arguments: string; status: string; resultPreview: string | null }[]
  error_text: string | null
}

function pairTools(tools: ToolEntry[]): PairedTool[] {
  const map = new Map<string, PairedTool>()
  const ordered: PairedTool[] = []
  for (const e of tools) {
    if (e.kind === 'call') {
      const p: PairedTool = {
        idx: e.idx, callId: e.callId, name: e.name, label: e.label || e.name,
        ts: e.ts, status: e.status, arguments: e.body,
      }
      map.set(e.callId, p)
      ordered.push(p)
    } else if (e.kind === 'result') {
      const existing = map.get(e.callId)
      if (existing) {
        existing.status = e.status
        existing.resultPreview = e.body
      }
    }
  }
  return ordered
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

function StatusDot({ status }: { status: 'pending' | 'done' | 'error' }) {
  const cls = status === 'done' ? 'tool-dot done' : status === 'error' ? 'tool-dot error' : 'tool-dot pending'
  return <span className={cls} />
}

function ToolRow({ tool, isLive }: { tool: PairedTool; isLive?: boolean }) {
  const [open, setOpen] = useState(false)
  const status = isLive && tool.status === 'pending' ? 'pending' : tool.status
  return (
    <div className="am-tool-item">
      <div className="am-tool-item-head" onClick={() => setOpen(o => !o)}>
        <StatusDot status={status} />
        <span className="am-tool-name">{tool.label}</span>
        <span className="am-tool-func-name">{tool.name}</span>
        <span className="am-tool-time">{tool.ts.slice(11, 19)}</span>
        <span style={{ fontSize: '0.65rem', color: '#aaa', marginLeft: '4px' }}>{open ? '▲' : '▼'}</span>
      </div>
      {open && (
        <div className="am-tool-item-body">
          <div style={{ marginBottom: '0.3rem' }}>
            <span className="am-tool-body-label">调用参数</span>
            <pre>{tool.arguments}</pre>
          </div>
          {tool.resultPreview !== undefined && (
            <div>
              <span className="am-tool-body-label">{status === 'error' ? '❌ 返回' : '✓ 返回'}</span>
              <pre>{tool.resultPreview}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function PhasePanel({ phaseKey, label, state, tools, live }: {
  phaseKey: string; label: string; state: PhaseState
  tools?: ToolEntry[]; live?: boolean
}) {
  // 默认：进行中的阶段展开，已完成/历史阶段折叠
  const [manualOpen, setManualOpen] = useState<boolean | null>(null)
  const open = manualOpen ?? !!live

  const paired = useMemo(() => pairTools(tools ?? []), [tools])
  const badge = phaseKey as 'route' | 'design' | 'review' | 'execute'
  const pendingCount = paired.filter(t => t.status === 'pending').length
  const doneCount = paired.filter(t => t.status === 'done').length
  const errorCount = paired.filter(t => t.status === 'error').length

  // 提取 AI 阶段标题：文本第一行（##STEP: ... 或普通首行）
  const titleLine = useMemo(() => {
    const firstLine = state.text.split('\n')[0] ?? ''
    if (firstLine.startsWith('##STEP:')) return firstLine.replace('##STEP:', '').trim()
    if (firstLine.startsWith('##')) return firstLine.replace(/^#+\s*/, '').trim()
    return ''
  }, [state.text])

  return (
    <div className="am-phase-panel">
      <div className="am-phase-header" onClick={() => setManualOpen((prev) => !(prev ?? !!live))}>
        <span className={`am-phase-badge ${badge}`}>{label}</span>
        <span className="am-phase-title">
          {state.error
            ? '❌ 错误'
            : live
              ? <><span className="am-live-dot" />{titleLine || '进行中…'}</>
              : titleLine || (state.hasContent ? `✓ ${state.text.length} chars` : '—')
          }
        </span>
        {paired.length > 0 && (
          <span className="am-tool-summary">
            {doneCount > 0 && <span className="am-ts-dot done">{doneCount}</span>}
            {errorCount > 0 && <span className="am-ts-dot error">{errorCount}</span>}
            {pendingCount > 0 && <span className="am-ts-dot pending">{pendingCount}</span>}
          </span>
        )}
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
          {paired.length > 0 && (
            <>
              <div className="am-section-label">工具调用 ({paired.length})</div>
              <div className="am-tool-timeline">
                {paired.map(tool => (
                  <ToolRow key={tool.callId || tool.idx} tool={tool} isLive={live && tool.status === 'pending'} />
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
  gameplay_planning: '请分析游戏配置，规划所有需要单独出落地表的玩法系统，使用 register_gameplay_table 工具注册每张表（含设计目标 README 和推荐顺序）。注意：本步仅注册规划，不创建任何数值表。',
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
  gameplay_table: '请查看本步骤需要执行的玩法落地表（通过 get_gameplay_table_list 确认），先标记为「进行中」，完成完整数值设计后标记为「已完成」。',
}

function buildInitMessage(stepId: string, projectInfo: ProjectInfo, completedSteps: string[]): string {
  let custom: string | undefined
  if (stepId.startsWith('gameplay_table.')) {
    const tableId = stepId.slice('gameplay_table.'.length)
    custom = `请执行玩法规划中已注册的「${tableId}」落地表：\n1. 先调用 get_gameplay_table_list 查看该表的 readme 和依赖关系\n2. 调用 set_gameplay_table_status('${tableId}', '进行中') 标记开始\n3. 完成完整数值设计（参考该表 readme 中的设计目标和关键列）\n4. 完成后调用 set_gameplay_table_status('${tableId}', '已完成') 标记完成\n5. 若完成后发现列表中有 status='待修订' 的表且与本步工作相关，可顺带处理`
  } else {
    custom = STEP_INIT_MESSAGES[stepId]
  }
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

  // ── Server-side session state ────────────────────────────────────
  const [sessionStatus, setSessionStatus] = useState<SessionStatus>('none')

  // ── Recovery Agent 状态 ──────────────────────────────────────────
  const [recoveryMode, setRecoveryMode] = useState(false)
  const [recoveryLivePhase, setRecoveryLivePhase] = useState('')
  const [recoveryPhases, setRecoveryPhases] = useState<Record<string, PhaseState>>({})
  const [recoveryTools, setRecoveryTools] = useState<ToolEntry[]>([])
  const [recoveryStatus, setRecoveryStatus] = useState<'idle' | 'running' | 'done' | 'partial' | 'failed'>('idle')
  const [recoveryMsg, setRecoveryMsg] = useState('')
  const recoveryCountRef = useRef(0) // 每步最多3次自动修复
  const networkRetryRef = useRef(0)  // 网络错误直接重试计数（最多2次）

  // ── 当前正在运行的步骤 ────────────────────────────────────────────
  const [currentStep, setCurrentStep] = useState<string | null>(null)
  const [allDone, setAllDone] = useState(false)
  const [gameplayTables, setGameplayTables] = useState<GameplayTable[]>([])
  const [revisionRequests, setRevisionRequests] = useState<RevisionRequest[]>([])

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
        // Check if there's a persisted session for this step
        void checkExistingSession(plData.next_expected_step)
      }
      void loadGameplayTables()
      void loadRevisionRequests()
    } catch (e) {
      setLoadErr(String(e))
    }
  }, [headers, pid]) // eslint-disable-line react-hooks/exhaustive-deps

  const loadGameplayTables = useCallback(async () => {
    try {
      const data = await apiFetch('/pipeline/gameplay-tables', { headers }) as { tables: GameplayTable[] }
      setGameplayTables(data.tables ?? [])
    } catch {
      // 忽略（玩法表可能尚未规划）
    }
  }, [headers])

  const loadRevisionRequests = useCallback(async () => {
    try {
      const data = await apiFetch('/pipeline/revision-requests', { headers }) as { items: RevisionRequest[] }
      setRevisionRequests((data.items ?? []).filter(r => r.status !== 'done'))
    } catch {
      // 忽略
    }
  }, [headers])

  /** Restore a server-persisted session for the current step */
  const checkExistingSession = useCallback(async (stepId: string) => {
    setSessionStatus('loading')
    try {
      const data = await apiFetch(`/pipeline/step/${stepId}/session`, { headers }) as { session: ServerSession | null }
      const sess = data.session
      if (!sess) {
        setSessionStatus('none')
        return
      }
      // Reconstruct phases display from stored session
      const ts = sess.started_at
      const restoredPhases: Record<string, PhaseState> = {}
      const addPhase = (key: string, text: string, error?: string | null) => {
        if (text || error) {
          restoredPhases[key] = {
            started: ts, finished: sess.finished_at,
            text, logs: [], error: error ?? null,
            hasContent: !!text,
          }
        }
      }
      addPhase('design', sess.design_text)
      addPhase('review', sess.review_text)
      addPhase('execute', sess.execute_text, sess.error_text)

      // Reconstruct tool entries
      const restoredTools: ToolEntry[] = []
      sess.tools.forEach((t, i) => {
        restoredTools.push({
          idx: i * 2, ts, kind: 'call',
          name: t.name, label: t.label || t.name,
          body: t.arguments, callId: t.callId, status: t.status as ToolEntry['status'],
        })
        if (t.resultPreview != null) {
          restoredTools.push({
            idx: i * 2 + 1, ts, kind: 'result',
            name: t.name, label: '',
            body: t.resultPreview, callId: t.callId, status: t.status as ToolEntry['status'],
          })
        }
      })

      setPhases(restoredPhases)
      setTools(restoredTools)
      setMetrics({
        startedAt: sess.started_at,
        finishedAt: sess.finished_at,
        totalMs: msDiff(sess.started_at, sess.finished_at ?? sess.started_at),
        toolCalls: sess.tools.length,
        status: sess.status === 'done' ? 'done' : sess.status === 'error' ? 'error' : 'running',
      })
      if (sess.error_text) setAgentErr(sess.error_text)

      const sStatus: SessionStatus = sess.status === 'done' ? 'done'
        : sess.status === 'error' ? 'error' : 'interrupted'
      setSessionStatus(sStatus)

      // If the session was successfully completed but pipeline not yet advanced, advance now
      if (sess.status === 'done') {
        void advancePipeline(stepId)
      }
    } catch {
      // Failed to fetch session → fall back to normal auto-run
      setSessionStatus('none')
    }
  }, [headers]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadPipeline()
    }, 0)
    return () => window.clearTimeout(timer)
  }, [loadPipeline])

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

  function resetRecoveryState() {
    setRecoveryPhases({})
    setRecoveryTools([])
    setRecoveryStatus('idle')
    setRecoveryMsg('')
    setRecoveryLivePhase('')
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
    await loadPipeline()
    await loadGameplayTables()
    await loadRevisionRequests()
  }

  // ── 通用 SSE 读取：返回 { localPhases, localTools, toolCount, hasError, recoveryStatus }
  type SseResult = {
    localPhases: Record<string, PhaseState>
    localTools: ToolEntry[]
    toolCount: number
    hasError: boolean
    recoveryStatus?: string
  }

  async function readAgentStream(
    res: Response,
    startTime: string,
    setLive: (p: string) => void,
    setPs: (p: Record<string, PhaseState>) => void,
    setTs: (t: ToolEntry[]) => void,
    setMet: (m: Metrics) => void,
    signal?: AbortSignal,
  ): Promise<SseResult> {
    const reader = res.body?.getReader()
    if (!reader) throw new Error('无响应流')
    const dec = new TextDecoder()
    let buf = ''
    let toolCount = 0
    let hasError = false
    let lastRecoveryStatus: string | undefined
    const localPhases: Record<string, PhaseState> = {}
    const localTools: ToolEntry[] = []
    const phaseTimes: Record<string, string> = {}

    function getOrInit(p: string): PhaseState {
      if (!localPhases[p]) localPhases[p] = initPhase()
      return localPhases[p]
    }

    for (;;) {
      if (signal?.aborted) break
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
          getOrInit(phase).started = phaseTimes[phase + '_start']
          setLive(phase)
        }

        if (type === 'token') {
          getOrInit(phase).text += String(raw.text ?? '')
          getOrInit(phase).hasContent = true
        } else if (type === 'log') {
          getOrInit(phase).logs.push(String(raw.message ?? ''))
        } else if (type === 'error') {
          getOrInit(phase).error = String(raw.message ?? '')
          hasError = true
        } else if (type === 'tool_call') {
          toolCount++
          const callId = String(raw.call_id ?? String(toolCount))
          localTools.push({
            idx: localTools.length, ts: nowIso(), kind: 'call',
            name: String(raw.name ?? ''),
            label: String(raw.label ?? raw.name ?? ''),
            body: String(raw.arguments ?? ''),
            callId,
            status: 'pending',
          })
        } else if (type === 'tool_result') {
          const callId = String(raw.call_id ?? '')
          const resultStatus = raw.status === 'error' ? 'error' : 'done'
          // Update matching call entry status
          for (let i = localTools.length - 1; i >= 0; i--) {
            if (localTools[i].kind === 'call' && localTools[i].callId === callId) {
              localTools[i] = { ...localTools[i], status: resultStatus }
              break
            }
          }
          localTools.push({
            idx: localTools.length, ts: nowIso(), kind: 'result',
            name: String(raw.name ?? ''), label: '',
            body: String(raw.preview ?? ''),
            callId, status: resultStatus,
          })
        } else if (type === 'done') {
          getOrInit(phase).finished = nowIso()
          lastRecoveryStatus = raw.recovery_status as string | undefined
          setMet({
            startedAt: startTime, finishedAt: nowIso(),
            totalMs: msDiff(startTime, nowIso()), toolCalls: toolCount, status: 'done',
          })
        }

        setPs({ ...localPhases })
        setTs([...localTools])
        setMet({
          startedAt: startTime, finishedAt: null,
          totalMs: msDiff(startTime, nowIso()), toolCalls: toolCount, status: 'running',
        })
      }
    }
    return { localPhases, localTools, toolCount, hasError, recoveryStatus: lastRecoveryStatus }
  }

  // ── run recovery agent ───────────────────────────────────────────
  const runRecovery = useCallback(async (
    stepId: string,
    failureContext: { step_id: string; error: string; tool_history: { name: string; arguments: string; result: string }[]; partial_design: string },
    projInfo: ProjectInfo,
    completedSteps: string[],
  ) => {
    recoveryCountRef.current++
    setRecoveryMode(true)
    setRecoveryStatus('running')
    resetRecoveryState()
    setRecoveryStatus('running')

    const startTime = nowIso()
    const recoveryMessage = (
      `【修复 Agent】步骤 ${stepId} 执行失败，请分析根本原因并执行修复操作，使该步骤能够在下次重试时成功。\n` +
      `项目：${projInfo.name}（${projInfo.game_type ?? '未知'}）`
    )

    const abort = new AbortController()
    abortRef.current = abort

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: recoveryMessage,
          mode: 'recovery',
          failure_context: failureContext,
        }),
        signal: abort.signal,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)

      const recMetSetter = (m: Metrics) => {
        // unused but required by readAgentStream signature
        void m
      }

      const result = await readAgentStream(
        res, startTime,
        setRecoveryLivePhase,
        setRecoveryPhases,
        setRecoveryTools,
        recMetSetter,
        abort.signal,
      )

      const rs = result.recoveryStatus ?? (result.hasError ? 'failed' : 'done')
      setRecoveryStatus(rs as 'done' | 'partial' | 'failed')

      // Extract last text from execute phase for message
      const execText = result.localPhases['execute']?.text ?? ''
      setRecoveryMsg(execText.slice(-600))

      // retry / done / partial → 重试原步骤
      if ((rs === 'retry' || rs === 'done' || rs === 'partial') && autoMode && recoveryCountRef.current <= 3) {
        setRecoveryMode(false)
        busyRef.current = false
        setBusy(false)
        setTimeout(() => {
          void runStep(stepId, projInfo, completedSteps)
        }, 2000)
      } else if (rs === 'failed') {
        // 修复彻底失败：停止自动推进，等待手动介入
        setRecoveryMode(false)
        setAgentErr(`步骤 ${stepId} 修复失败（已尝试 ${recoveryCountRef.current} 次），需要手动检查并重试。`)
        busyRef.current = false
        setBusy(false)
      }
    } catch (e) {
      if ((e as Error).name !== 'AbortError') {
        setRecoveryStatus('failed')
        setRecoveryMsg(e instanceof Error ? e.message : String(e))
        setRecoveryMode(false)
        setAgentErr(`修复 Agent 自身出错：${e instanceof Error ? e.message : String(e)}`)
        busyRef.current = false
        setBusy(false)
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [headers, autoMode])

  // ── run single step ──────────────────────────────────────────────
  const runStep = useCallback(async (stepId: string, projInfo: ProjectInfo, completedSteps: string[]) => {
    if (busyRef.current) return
    busyRef.current = true
    recoveryCountRef.current = 0  // 重置修复计数（新步骤）
    networkRetryRef.current = 0   // 重置网络重试计数（新步骤）
    resetAgentState()
    resetRecoveryState()
    setBusy(true)
    setCurrentStep(stepId)
    setSessionStatus('none') // clear any restored-session state

    const startTime = nowIso()
    const abort = new AbortController()
    abortRef.current = abort
    const message = buildInitMessage(stepId, projInfo, completedSteps)

    let sseResult: SseResult | null = null
    let catchErr: string | null = null

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, mode: 'init', step_id: stepId }),
        signal: abort.signal,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)

      const metSetter = (m: Metrics) => setMetrics(m)
      sseResult = await readAgentStream(
        res, startTime,
        setLivePhase, setPhases, setTools, metSetter,
        abort.signal,
      )
    } catch (e) {
      if ((e as Error).name === 'AbortError') {
        busyRef.current = false
        setBusy(false)
        return
      }
      catchErr = e instanceof Error ? e.message : String(e)
    }

    const hasError = catchErr !== null || (sseResult?.hasError ?? false)
    const localTools = sseResult?.localTools ?? []
    const localPhases = sseResult?.localPhases ?? {}
    const toolCount = sseResult?.toolCount ?? 0

    // 网络错误判断：fetch 失败 / SSE 连接中断 → 不需要 recovery，直接重试
    const isNetworkError = (msg: string): boolean => {
      const low = msg.toLowerCase()
      return (
        low.includes('failed to fetch') ||
        low.includes('networkerror') ||
        low.includes('network error') ||
        low.includes('fetch') ||
        low.includes('econnreset') ||
        low.includes('econnrefused') ||
        low.includes('connection') ||
        low.startsWith('http 5') // 5xx 服务端临时错误也直接重试
      )
    }

    if (hasError) {
      const errorMsg = catchErr ?? Object.values(localPhases).map(p => p.error).filter(Boolean).join('; ')
      setAgentErr(errorMsg)
      setMetrics({
        startedAt: startTime, finishedAt: nowIso(),
        totalMs: msDiff(startTime, nowIso()), toolCalls: toolCount, status: 'error',
      })
      setLivePhase('')

      // 网络错误：直接重试，不触发 recovery agent
      if (autoMode && isNetworkError(errorMsg) && networkRetryRef.current < 2) {
        networkRetryRef.current++
        setAgentErr(`网络错误，自动重试（第 ${networkRetryRef.current} 次）：${errorMsg}`)
        busyRef.current = false
        setBusy(false)
        setTimeout(() => {
          void runStep(stepId, projInfo, completedSteps)
        }, 2500)
        return
      }

      // 判断是否有成功的写操作（状态可能已污染）
      // 只读工具集（与后端保持一致）
      const READ_TOOL_NAMES = new Set([
        'get_project_config','get_table_list','read_table','read_cell','get_protected_cells',
        'get_dependency_graph','get_table_readme','get_algorithm_api_list','run_validation',
        'list_snapshots','compare_snapshot','run_balance_check','get_validation_history',
        'get_default_system_rules','glossary_lookup','glossary_list','const_list',
      ])
      const hasWriteToolCalls = localTools.some(t => !READ_TOOL_NAMES.has(t.name))

      if (!hasWriteToolCalls) {
        // 没有写操作 → 直接重试，无需 Recovery（工具逻辑错误由主 Agent 处理）
        if (autoMode && networkRetryRef.current < 3) {
          networkRetryRef.current++
          setAgentErr(`自动重试（第 ${networkRetryRef.current} 次）：${errorMsg}`)
          busyRef.current = false
          setBusy(false)
          setTimeout(() => void runStep(stepId, projInfo, completedSteps), 2000)
        } else {
          busyRef.current = false
          setBusy(false)
        }
        return
      }

      // 有写操作后崩溃 → 可能状态污染，触发 Recovery Agent（每步最多3次）
      if (autoMode && recoveryCountRef.current < 3) {
        const failCtx = {
          step_id: stepId,
          error: errorMsg,
          tool_history: localTools.map(t => ({
            name: t.name,
            arguments: t.body,
            result: '',
          })),
          partial_design: localPhases['design']?.text ?? '',
        }
        busyRef.current = false
        setBusy(false)
        setTimeout(() => {
          void runRecovery(stepId, failCtx, projInfo, completedSteps)
        }, 1500)
      } else {
        busyRef.current = false
        setBusy(false)
      }
      return
    }

    // 成功完成
    const finalMetrics: Metrics = {
      startedAt: startTime, finishedAt: nowIso(),
      totalMs: msDiff(startTime, nowIso()), toolCalls: toolCount, status: 'done',
    }
    setStepHistory(prev => [{ stepId, phases: { ...localPhases }, tools: [...localTools], metrics: finalMetrics }, ...prev])
    setLivePhase('')
    busyRef.current = false
    setBusy(false)
    abortRef.current = null

    await advancePipeline(stepId)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [headers, autoMode, runRecovery])

  // ── auto-start: triggers when pipeline.next_expected_step changes (including initial load).
  // Guard: do NOT auto-run if sessionStatus is still loading, or if a session is already done/interrupted.
  useEffect(() => {
    if (!pipeline || allDone || !autoMode) return
    // Still checking for existing session → wait
    if (sessionStatus === 'loading') return
    // A prior session was found (done or interrupted) → don't re-run automatically
    if (sessionStatus === 'done' || sessionStatus === 'interrupted' || sessionStatus === 'error') return
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
  }, [pipeline?.next_expected_step, allDone, autoMode, sessionStatus])

  // ── stop ─────────────────────────────────────────────────────────
  function stop() {
    setAutoMode(false)
    abortRef.current?.abort()
    busyRef.current = false
    setBusy(false)
    setLivePhase('')
    setRecoveryMode(false)
    setRecoveryStatus('idle')
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
          {!busy && !allDone && pipeline?.next_expected_step && sessionStatus !== 'done' && (
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
          {!busy && (sessionStatus === 'interrupted' || sessionStatus === 'error') && pipeline?.next_expected_step && (
            <button
              type="button"
              className="btn warn small"
              onClick={async () => {
                const stepId = pipeline?.next_expected_step
                if (!stepId) return
                try {
                  await fetch(`/api/pipeline/step/${stepId}/session`, {
                    method: 'DELETE',
                    credentials: 'include',
                    headers,
                  })
                } catch { /* ignore */ }
                setSessionStatus('none')
                resetAgentState()
                setAutoMode(true)
                void runStep(stepId, projectInfo, pipeline?.completed_steps ?? [])
              }}
            >
              ↺ 重新运行此步
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

          {/* 玩法规划表清单（gameplay_planning 步骤完成后展示） */}
          {gameplayTables.length > 0 && (
            <div className="ps-gameplay-tables">
              <div className="ps-sidebar-section-head">
                <span>玩法落地表规划</span>
                <span className="ps-progress-text">
                  {gameplayTables.filter(t => t.status === '已完成').length}/{gameplayTables.length}
                </span>
              </div>
              <ul className="ps-gameplay-table-list">
                {gameplayTables.map(t => {
                  const cls = t.status === '已完成' ? 'done'
                    : t.status === '进行中' ? 'active'
                    : t.status === '待修订' ? 'revision'
                    : 'pending'
                  return (
                    <li key={t.table_id} className={`ps-gameplay-table-item ps-gt-${cls}`}>
                      <span className="ps-gt-icon">
                        {t.status === '已完成' ? '✓' : t.status === '进行中' ? '⟳' : t.status === '待修订' ? '↺' : String(t.order_num)}
                      </span>
                      <span className="ps-gt-name" title={t.table_id}>{t.display_name}</span>
                      <span className={`ps-gt-badge ${cls}`}>
                        {t.status}
                      </span>
                    </li>
                  )
                })}
              </ul>
            </div>
          )}

          {/* 修订请求队列（有待处理修订时展示） */}
          {revisionRequests.length > 0 && (
            <div className="ps-revision-queue">
              <div className="ps-sidebar-section-head">
                <span>⚠ 待修订队列</span>
                <span className="ps-progress-text">{revisionRequests.length}</span>
              </div>
              <ul className="ps-revision-list">
                {revisionRequests.map(r => (
                  <li key={r.id} className="ps-revision-item">
                    <div className="ps-revision-item-head">
                      <span className="ps-revision-table-id">{r.table_id}</span>
                      <span className={`ps-gt-badge ${r.status === 'in_progress' ? 'active' : ''}`}>{r.status === 'in_progress' ? '进行中' : '待处理'}</span>
                    </div>
                    <div className="ps-revision-reason" title={r.reason}>{r.reason}</div>
                    {r.requested_by_step && (
                      <div className="ps-revision-by">来自 {r.requested_by_step}</div>
                    )}
                  </li>
                ))}
              </ul>
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

          {/* Session restore banners */}
          {!viewingHistory && sessionStatus === 'loading' && (
            <div className="ps-viewing-banner">⏳ 正在检查已有会话...</div>
          )}
          {!viewingHistory && sessionStatus === 'done' && (
            <div className="ps-viewing-banner" style={{ background: 'var(--success-bg, #e6f4ea)', color: 'var(--success, #1a7f37)' }}>
              ✅ 已恢复此步骤的完成会话（服务端执行记录）。若需重新运行，点击"重新运行此步"按钮。
            </div>
          )}
          {!viewingHistory && sessionStatus === 'interrupted' && (
            <div className="ps-viewing-banner" style={{ background: 'var(--warn-bg, #fff8e1)', color: 'var(--warn, #b45309)' }}>
              ⚠ 检测到上次运行被中断（可能因页面刷新）。已恢复进度，可继续查看或点击"重新运行此步"重新执行。
            </div>
          )}
          {!viewingHistory && sessionStatus === 'error' && (
            <div className="ps-viewing-banner" style={{ background: 'var(--err-bg, #fff2f0)', color: 'var(--err, #cf1322)' }}>
              ✗ 上次运行出错（已恢复错误记录）。可点击"重新运行此步"重试。
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

          {/* 错误提示（无修复进行中时显示） */}
          {agentErr && !viewingHistory && !recoveryMode && (
            <div className="ps-agent-err">
              <div className="ps-err-header">
                <span>⚠ Agent 出错
                  {recoveryCountRef.current > 0 && `（已尝试修复 ${recoveryCountRef.current} 次）`}
                  {networkRetryRef.current > 0 && recoveryCountRef.current === 0 && `（已自动重试 ${networkRetryRef.current} 次）`}
                </span>
              </div>
              <div className="ps-err-body">{agentErr}</div>
              <div className="ps-err-actions">
                <button type="button" className="btn ghost small"
                  onClick={() => {
                    setAgentErr(null)
                    recoveryCountRef.current = 0
                    if (pipeline?.next_expected_step) {
                      void runStep(pipeline.next_expected_step, projectInfo, pipeline.completed_steps ?? [])
                    }
                  }}>
                  🔄 手动重试步骤
                </button>
                {pipeline?.next_expected_step && (
                  <button type="button" className="btn ghost small"
                    onClick={() => {
                      setAgentErr(null)
                      recoveryCountRef.current = 0
                      const failCtx = {
                        step_id: pipeline.next_expected_step!,
                        error: agentErr,
                        tool_history: tools.map(t => ({ name: t.name, arguments: t.body, result: '' })),
                        partial_design: phases['design']?.text ?? '',
                      }
                      void runRecovery(pipeline.next_expected_step!, failCtx, projectInfo, pipeline.completed_steps ?? [])
                    }}>
                    🔧 手动启动修复 Agent
                  </button>
                )}
              </div>
            </div>
          )}

          {/* Recovery Agent 面板 */}
          {recoveryMode && !viewingHistory && (
            <div className="ps-recovery-panel">
              <div className="ps-recovery-header">
                <span className="ps-recovery-badge">
                  {recoveryStatus === 'running' ? '⟳ 修复 Agent 运行中…' :
                   recoveryStatus === 'done' ? '✓ 修复完成，即将重试' :
                   recoveryStatus === 'partial' ? '⚠ 部分修复，即将重试' :
                   '✗ 修复失败'}
                </span>
                <span className="muted small">第 {recoveryCountRef.current} 次修复尝试（最多 3 次）</span>
              </div>
              <div className="ps-recovery-phases">
                {[
                  { key: 'design', label: '修复分析' },
                  { key: 'execute', label: '修复执行' },
                ].map(({ key, label }) => {
                  const ps = recoveryPhases[key]
                  if (!ps && recoveryLivePhase !== key) return null
                  const phaseTool = key === 'execute' ? recoveryTools : undefined
                  const isLive = recoveryStatus === 'running' && recoveryLivePhase === key
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
              {recoveryMsg && (
                <div className="ps-recovery-summary">
                  <strong>修复报告（摘要）：</strong>
                  <pre>{recoveryMsg}</pre>
                </div>
              )}
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
