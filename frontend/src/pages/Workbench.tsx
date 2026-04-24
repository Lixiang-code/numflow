import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'
import { getInitAgentPrompt, pipelineStepLabel } from '../data/pipelineSteps'

type TableInfo = { table_name: string; validation_status: string; layer: string; purpose?: string }

type RuleSummary = {
  table?: string
  rule_id?: string
  type?: string
  column?: string | null
  passed?: boolean
  violation_count?: number
}

type ValidateReport = {
  passed: boolean
  warnings: string[]
  per_table: Record<string, string>
  violations?: { table?: string; message?: string; row_id?: string; column?: string }[]
  rule_summaries?: RuleSummary[]
}

type SnapshotRow = { id: number; label: string; created_at: string; note?: string }

export default function Workbench() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  /** 必须 memo：否则每次 render 新 headers 对象会触发 effect/useCallback 无限循环 → 浏览器 ERR_INSUFFICIENT_RESOURCES */
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [tables, setTables] = useState<TableInfo[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [tableReadmeDraft, setTableReadmeDraft] = useState('')
  const [globalReadmeDraft, setGlobalReadmeDraft] = useState('')
  const [readmeTab, setReadmeTab] = useState<'table' | 'global'>('table')
  const [sideTab, setSideTab] = useState<'readme' | 'validation' | 'snapshots'>('readme')
  const [canWrite, setCanWrite] = useState(false)
  const [validateReport, setValidateReport] = useState<ValidateReport | null>(null)
  const [validationRulesDraft, setValidationRulesDraft] = useState('')
  const [snapshots, setSnapshots] = useState<SnapshotRow[]>([])
  const [compareSnapshotId, setCompareSnapshotId] = useState<number | null>(null)
  const [compareText, setCompareText] = useState('')
  const [pipeline, setPipeline] = useState<{
    next_expected_step: string | null
    completed_steps: string[]
    finished?: boolean
  } | null>(null)
  const [agentLog, setAgentLog] = useState<string[]>([])
  const [agentStream, setAgentStream] = useState('')
  const [agentInput, setAgentInput] = useState('')
  const [agentBusy, setAgentBusy] = useState(false)
  const [agentMode, setAgentMode] = useState<'init' | 'maintain'>('maintain')
  const [err, setErr] = useState<string | null>(null)
  /** 列名 -> 公式（用于表头悬停） */
  const [columnFormulas, setColumnFormulas] = useState<Record<string, string>>({})

  const loadTables = useCallback(async () => {
    const d = (await apiFetch('/meta/tables', { headers })) as { tables?: unknown }
    const raw = Array.isArray(d.tables) ? d.tables : []
    const tables = raw.filter((t): t is TableInfo => {
      if (t == null || typeof t !== 'object') return false
      const o = t as { table_name?: unknown }
      return typeof o.table_name === 'string'
    })
    setTables(tables)
    setSelected((sel) => sel ?? (tables[0]?.table_name ?? null))
  }, [headers])

  const loadProjectConfig = useCallback(async () => {
    const cfg = (await apiFetch('/meta/project-config', { headers })) as {
      settings: Record<string, { text?: string } | unknown>
      can_write?: boolean
    }
    setCanWrite(Boolean(cfg.can_write))
    const g = cfg.settings?.global_readme as { text?: string } | undefined
    const text = g?.text || ''
    setGlobalReadmeDraft(text)
  }, [headers])

  const loadValidation = useCallback(async () => {
    const v = (await apiFetch('/validate/run', { method: 'POST', headers })) as Partial<ValidateReport>
    setValidateReport({
      passed: Boolean(v.passed),
      warnings: Array.isArray(v.warnings) ? v.warnings : [],
      per_table:
        v.per_table != null && typeof v.per_table === 'object' && !Array.isArray(v.per_table)
          ? (v.per_table as Record<string, string>)
          : {},
      violations: Array.isArray(v.violations) ? v.violations : [],
      rule_summaries: Array.isArray(v.rule_summaries) ? (v.rule_summaries as RuleSummary[]) : [],
    })
  }, [headers])

  const loadPipeline = useCallback(async () => {
    const s = (await apiFetch('/pipeline/status', { headers })) as {
      next_expected_step: string | null
      completed_steps: string[]
      finished: boolean
    }
    setPipeline(s)
  }, [headers])

  const loadSnapshots = useCallback(async () => {
    const d = (await apiFetch('/meta/snapshots', { headers })) as { snapshots?: unknown }
    const raw = Array.isArray(d.snapshots) ? d.snapshots : []
    const list = raw.filter((x): x is SnapshotRow => {
      if (x == null || typeof x !== 'object') return false
      const o = x as { id?: unknown }
      return typeof o.id === 'number'
    })
    setSnapshots(list)
    setCompareSnapshotId((cur) => {
      if (cur != null && list.some((s) => s.id === cur)) return cur
      return list[0]?.id ?? null
    })
  }, [headers])

  useEffect(() => {
    if (!Number.isFinite(pid)) return
    let cancelled = false
    setErr(null)
    setTables([])
    setSelected(null)
    void Promise.all([
      loadTables(),
      loadProjectConfig(),
      loadPipeline(),
      loadValidation(),
      loadSnapshots(),
    ]).catch((e) => {
      if (!cancelled) setErr(String(e))
    })
    return () => {
      cancelled = true
    }
  }, [pid, loadTables, loadProjectConfig, loadPipeline, loadValidation, loadSnapshots])

  useEffect(() => {
    if (!selected) {
      setRows([])
      setTableReadmeDraft('')
      setValidationRulesDraft('')
      setColumnFormulas({})
      return
    }
    let cancelled = false
    ;(async () => {
      try {
        const r = (await apiFetch(`/data/tables/${encodeURIComponent(selected)}/rows?limit=200`, {
          headers,
        })) as { rows?: unknown }
        const m = (await apiFetch(`/meta/tables/${encodeURIComponent(selected)}/readme`, {
          headers,
        })) as { readme: string }
        const desc = (await apiFetch(`/data/tables/${encodeURIComponent(selected)}`, {
          headers,
        })) as {
          validation_rules?: { rules?: unknown[] } | null
          column_formulas?: Record<string, string> | null
        }
        if (!cancelled) {
          const rawRows = Array.isArray(r.rows) ? r.rows : []
          const normalized = rawRows.filter(
            (row): row is Record<string, unknown> =>
              row != null && typeof row === 'object' && !Array.isArray(row),
          )
          setRows(normalized)
          const tr = m.readme || ''
          setTableReadmeDraft(tr)
          const vr = desc.validation_rules && typeof desc.validation_rules === 'object' ? desc.validation_rules : { rules: [] }
          setValidationRulesDraft(JSON.stringify(vr, null, 2))
          const cf = desc.column_formulas
          setColumnFormulas(cf != null && typeof cf === 'object' && !Array.isArray(cf) ? cf : {})
        }
      } catch (e) {
        if (!cancelled) setErr(String(e))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [selected, headers])

  async function advancePipeline() {
    if (!pipeline?.next_expected_step) return
    setErr(null)
    try {
      await apiFetch('/pipeline/advance', {
        method: 'POST',
        headers,
        body: JSON.stringify({ step: pipeline.next_expected_step }),
      })
      await loadPipeline()
      await loadSnapshots()
      await loadValidation()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function saveTableReadme() {
    if (!selected || !canWrite) return
    setErr(null)
    try {
      await apiFetch(`/meta/tables/${encodeURIComponent(selected)}/readme`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({ content: tableReadmeDraft }),
      })
      await loadValidation()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function saveGlobalReadme() {
    if (!canWrite) return
    setErr(null)
    try {
      await apiFetch('/meta/global-readme', {
        method: 'PUT',
        headers,
        body: JSON.stringify({ content: globalReadmeDraft }),
      })
      await loadProjectConfig()
      await loadValidation()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function saveValidationRules() {
    if (!selected || !canWrite) return
    setErr(null)
    try {
      const parsed = JSON.parse(validationRulesDraft) as { rules?: unknown[] }
      if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.rules)) {
        throw new Error('JSON 须为对象且含 rules 数组，例如 {"rules":[]}')
      }
      await apiFetch(`/meta/tables/${encodeURIComponent(selected)}/validation-rules`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({ rules: parsed.rules }),
      })
      await loadValidation()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function runSnapshotCompare() {
    if (compareSnapshotId == null) return
    setErr(null)
    try {
      const c = (await apiFetch(`/meta/snapshots/${compareSnapshotId}/compare`, { headers })) as Record<string, unknown>
      setCompareText(JSON.stringify(c, null, 2))
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function runAgent(e: FormEvent) {
    e.preventDefault()
    if (!agentInput.trim() || agentBusy) return
    setAgentBusy(true)
    setAgentLog((l) => [...l, `> ${agentInput}`])
    setAgentStream('')
    const msg = agentInput
    setAgentInput('')
    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        credentials: 'include',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, mode: agentMode }),
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
            const ev = JSON.parse(line) as {
              type: string
              message?: string
              text?: string
              name?: string
              preview?: string
            }
            if (ev.type === 'log' && ev.message) setAgentLog((l) => [...l, `[log] ${ev.message}`])
            if (ev.type === 'tool_call') setAgentLog((l) => [...l, `[tool] ${ev.name}`])
            if (ev.type === 'tool_result' && ev.preview)
              setAgentLog((l) => [...l, `[result] ${ev.preview}`])
            if (ev.type === 'token' && ev.text) setAgentStream((s) => s + ev.text)
            if (ev.type === 'done') {
              const ft = (ev as { full_text?: string }).full_text || ''
              setAgentLog((l) => [...l, ft])
              setAgentStream('')
            }
          } catch {
            /* ignore parse */
          }
        }
      }
    } catch (x) {
      setAgentLog((l) => [...l, `错误: ${x instanceof Error ? x.message : String(x)}`])
    } finally {
      setAgentBusy(false)
    }
  }

  const { tableCols, tableRows } = useMemo(() => {
    const safe = rows.filter(
      (row): row is Record<string, unknown> =>
        row != null && typeof row === 'object' && !Array.isArray(row),
    )
    if (safe.length === 0) {
      return { tableCols: selected ? ['(空表)'] : ([] as string[]), tableRows: [] as Record<string, unknown>[] }
    }
    return { tableCols: Object.keys(safe[0]), tableRows: safe }
  }, [rows, selected])

  const agentPlaceholder = useMemo(() => {
    if (agentMode === 'init' && pipeline?.next_expected_step) {
      return `例如：完成「${pipelineStepLabel(pipeline.next_expected_step)}」…`
    }
    return '自然语言指令（需 DASHSCOPE_API_KEY）'
  }, [agentMode, pipeline?.next_expected_step])

  const initHintStep = pipeline?.next_expected_step

  return (
    <div className="workbench">
      <header className="wb-top">
        <Link to="/projects" className="link-btn">
          项目列表
        </Link>
        <Link to={`/agent-test?project=${pid}`} className="link-btn">
          AGENT TEST
        </Link>
        <span className="muted">项目 #{pid}</span>
      </header>
      {err && <p className="err banner">{err}</p>}
      {validateReport && !validateReport.passed && (
        <p className="banner warn" style={{ margin: '0.5rem 1rem' }}>
          校验：{(validateReport.warnings ?? []).join('；')}
          {(validateReport.violations?.length ?? 0) > 0
            ? `（规则违反 ${validateReport.violations!.length} 条）`
            : ''}
        </p>
      )}
      {validateReport && (validateReport.rule_summaries?.length ?? 0) > 0 && (
        <ul className="wb-rule-sum muted small" style={{ margin: '0 1rem 0.5rem' }}>
          {(validateReport.rule_summaries ?? []).map((s, i) => (
            <li key={i}>
              {s.table}.{s.rule_id} [{s.type}] {s.passed ? '通过' : '未通过'}
              {typeof s.violation_count === 'number' ? `（${s.violation_count} 条违反）` : ''}
            </li>
          ))}
        </ul>
      )}

      <div className="wb-body">
        <aside className="wb-left">
          <h3>表</h3>
          <button
            type="button"
            className="linkish"
            onClick={() => {
              void Promise.all([
                loadTables(),
                loadProjectConfig(),
                loadPipeline(),
                loadValidation(),
                loadSnapshots(),
              ]).catch((e) => setErr(String(e)))
            }}
          >
            刷新
          </button>
          <ul>
            {tables.map((t) => {
              const warn = validateReport?.per_table?.[t.table_name] === 'warn'
              const cls = [selected === t.table_name ? 'sel' : '', warn ? 'row-warn' : ''].filter(Boolean).join(' ')
              return (
                <li key={t.table_name}>
                  <button type="button" className={cls || undefined} onClick={() => setSelected(t.table_name)}>
                    <span className="tbl-name">{t.table_name}</span>
                    {t.purpose ? (
                      <small className="tbl-purpose" title={t.purpose}>
                        {t.purpose}
                      </small>
                    ) : null}
                    <small>{t.validation_status}</small>
                  </button>
                </li>
              )
            })}
          </ul>
          {pipeline && (
            <div className="pipe-box">
              <h4>流水线（03）</h4>
              <p className="muted small">已完成: {pipeline.completed_steps.length} 步</p>
              <p className="small pipe-next-title">{pipelineStepLabel(pipeline.next_expected_step)}</p>
              <p className="muted small mono">{pipeline.next_expected_step || '—'}</p>
              <button type="button" className="btn tiny" disabled={!pipeline.next_expected_step} onClick={advancePipeline}>
                推进当前步
              </button>
              <p className="muted small" style={{ marginTop: '0.5rem' }}>
                推进成功后会自动创建快照（label 前缀为 pipeline: 加当前步骤 ID）。
              </p>
            </div>
          )}
        </aside>

        <section className="wb-center">
          <h3>{selected || '未选择表'}</h3>
          <div className="table-wrap">
            <table className="grid">
              <thead>
                <tr>
                  {tableCols.map((c) => {
                    const f = columnFormulas[c]
                    return (
                      <th
                        key={c}
                        className={f ? 'col-has-formula' : undefined}
                        title={f ? `公式：${f}` : c}
                      >
                        {c}
                        {f ? <span className="formula-mark" aria-hidden="true" /> : null}
                      </th>
                    )
                  })}
                </tr>
              </thead>
              <tbody>
                {tableRows.map((r, i) => (
                  <tr key={i}>
                    {tableCols.map((c) => (
                      <td key={c}>{String(r[c] ?? '')}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="wb-right">
          <div className="readme-tabs readme-tab-btns">
            <button type="button" className={sideTab === 'readme' ? 'active' : ''} onClick={() => setSideTab('readme')}>
              README
            </button>
            <button
              type="button"
              className={sideTab === 'validation' ? 'active' : ''}
              onClick={() => setSideTab('validation')}
            >
              校验规则
            </button>
            <button
              type="button"
              className={sideTab === 'snapshots' ? 'active' : ''}
              onClick={() => setSideTab('snapshots')}
            >
              快照
            </button>
          </div>

          {sideTab === 'readme' && (
            <>
              <h3>README 内容</h3>
              <div className="readme-tabs readme-tab-btns">
                <button type="button" className={readmeTab === 'table' ? 'active' : ''} onClick={() => setReadmeTab('table')}>
                  当前表
                </button>
                <button
                  type="button"
                  className={readmeTab === 'global' ? 'active' : ''}
                  onClick={() => setReadmeTab('global')}
                >
                  全局
                </button>
              </div>
              {!canWrite && <p className="muted small">只读项目：无法保存 README。</p>}
              {readmeTab === 'table' && (
                <>
                  {!selected && <p className="muted small">请在左侧选择一张表。</p>}
                  {selected && (
                    <>
                      <p className="readme-tabs">
                        <strong>表: {selected}</strong>
                      </p>
                      <textarea
                        className="readme-textarea"
                        value={tableReadmeDraft}
                        onChange={(e) => setTableReadmeDraft(e.target.value)}
                        disabled={!canWrite}
                        spellCheck={false}
                      />
                      {canWrite && (
                        <div className="readme-save-row">
                          <button type="button" className="btn tiny primary" onClick={() => void saveTableReadme()}>
                            保存表 README
                          </button>
                        </div>
                      )}
                    </>
                  )}
                </>
              )}
              {readmeTab === 'global' && (
                <>
                  <textarea
                    className="readme-textarea"
                    value={globalReadmeDraft}
                    onChange={(e) => setGlobalReadmeDraft(e.target.value)}
                    disabled={!canWrite}
                    spellCheck={false}
                  />
                  {canWrite && (
                    <div className="readme-save-row">
                      <button type="button" className="btn tiny primary" onClick={() => void saveGlobalReadme()}>
                        保存全局 README
                      </button>
                    </div>
                  )}
                </>
              )}
            </>
          )}

          {sideTab === 'validation' && (
            <>
              <h3>校验规则 JSON</h3>
              {!selected && <p className="muted small">请选择表后编辑 rules。</p>}
              {selected && (
                <>
                  <p className="muted small">
                    支持 type: <code>not_null</code>、<code>min_max</code>（min/max）、<code>regex</code>（pattern，可选 full_match）。
                  </p>
                  <textarea
                    className="readme-textarea"
                    value={validationRulesDraft}
                    onChange={(e) => setValidationRulesDraft(e.target.value)}
                    disabled={!canWrite}
                    spellCheck={false}
                  />
                  {canWrite && (
                    <div className="readme-save-row">
                      <button type="button" className="btn tiny primary" onClick={() => void saveValidationRules()}>
                        保存到表
                      </button>
                    </div>
                  )}
                </>
              )}
            </>
          )}

          {sideTab === 'snapshots' && (
            <>
              <h3>快照列表</h3>
              <button type="button" className="btn tiny" onClick={() => void loadSnapshots()}>
                刷新列表
              </button>
              {snapshots.length === 0 ? (
                <p className="muted small">暂无快照。</p>
              ) : (
                <ul className="wb-snap-list">
                  {snapshots.map((s) => (
                    <li key={s.id}>
                      <label className="wb-snap-row">
                        <input
                          type="radio"
                          name="snapPick"
                          checked={compareSnapshotId === s.id}
                          onChange={() => setCompareSnapshotId(s.id)}
                        />
                        <span>
                          #{s.id} {s.label}
                          <small className="muted"> {s.created_at}</small>
                        </span>
                      </label>
                    </li>
                  ))}
                </ul>
              )}
              <div className="readme-save-row">
                <button type="button" className="btn tiny" disabled={compareSnapshotId == null} onClick={() => void runSnapshotCompare()}>
                  与当前库对比
                </button>
              </div>
              {compareText && (
                <pre className="wb-compare-pre" style={{ marginTop: '0.5rem', fontSize: '0.75rem', overflow: 'auto' }}>
                  {compareText}
                </pre>
              )}
            </>
          )}
        </aside>
      </div>

      <footer className="wb-agent">
        {agentMode === 'init' && initHintStep && (
          <div className="pipe-agent-hint muted small" style={{ marginBottom: '0.35rem' }}>
            <span>与流水线「下一步」联动：</span>
            <button
              type="button"
              className="btn tiny"
              disabled={agentBusy}
              onClick={() => setAgentInput(getInitAgentPrompt(initHintStep))}
            >
              插入初始化模板
            </button>
          </div>
        )}
        <form className="wb-agent-form" onSubmit={runAgent}>
          <select
            className="agent-mode"
            value={agentMode}
            onChange={(e) => setAgentMode(e.target.value as 'init' | 'maintain')}
            disabled={agentBusy}
            aria-label="Agent 模式"
          >
            <option value="maintain">维护 Agent</option>
            <option value="init">初始化 Agent</option>
          </select>
          <input
            value={agentInput}
            onChange={(e) => setAgentInput(e.target.value)}
            placeholder={agentPlaceholder}
            disabled={agentBusy}
          />
          <button type="submit" disabled={agentBusy}>
            {agentBusy ? '执行中…' : '发送'}
          </button>
        </form>
        {agentStream && <pre className="agent-stream">{agentStream}</pre>}
        <div className="agent-log">
          {agentLog.map((line, i) => (
            <div key={i} className="log-line">
              {line}
            </div>
          ))}
        </div>
      </footer>
    </div>
  )
}
