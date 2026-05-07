import { useCallback, useEffect, useMemo, useRef, useState, type DragEvent, type FormEvent } from 'react'
import React from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import { apiFetch, projectHeaders } from '../api'
import { pipelineStepLabel } from '../data/pipelineSteps'
import { createUniver, LocaleType, defaultTheme, type Univer } from '@univerjs/presets'
import type { FUniver } from '@univerjs/core/lib/facade'
import { UniverSheetsCorePreset, type FWorkbook } from '@univerjs/preset-sheets-core'
import UniverZhCN from '@univerjs/preset-sheets-core/locales/zh-CN'
import '@univerjs/preset-sheets-core/lib/index.css'
import MatrixEditor from './MatrixEditor'
import ThreeDimTableEditor from './ThreeDimTableEditor'
import AutoTextarea from '../components/AutoTextarea'
import MaintainSidebar from '../components/MaintainSidebar'

type TableInfo = {
  table_name: string
  validation_status: string
  layer: string
  purpose?: string
  display_name?: string
  directory?: string
  is_matrix?: boolean
  is_3d_matrix?: boolean
  matrix_kind?: string
  table_kind?: string  // config / compute / mixed / ""
}
type ColumnMeta = { name: string; sql_type?: string; display_name?: string; dtype?: string; number_format?: string; is_dim?: boolean }
/** 列公式信息（含类型：sql / row / row_template） */
type FormulaInfo = { formula: string; type: string }
type FormulaMap = Record<string, FormulaInfo>

const MIN_UNIVER_SHEET_ROWS = 1000
const MIN_UNIVER_SHEET_COLUMNS = 50

type SheetCapacityHandle = {
  getMaxRows?: () => number
  getMaxColumns?: () => number
  setRowCount?: (rowCount: number) => unknown
  setColumnCount?: (columnCount: number) => unknown
}

type CalculatorAxis = { name: string; source: string; default?: string }
type CalculatorItem = {
  name: string
  kind: string
  table_name: string
  axes: CalculatorAxis[]
  value_column: string
  brief: string
  updated_at?: string
}

type ExposedParam = {
  owner_step: string
  target_step: string
  key: string
  value: unknown
  brief: string
}

type GlossaryItem = { term_en: string; term_zh: string }

/** AI 设计文档历史条目（来自 /pipeline/design-history 或 completed_steps） */
type DesignEntry = {
  step_id: string
  design_text?: string
  completed_at?: string
}

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

/** Workbench Agent 面板：结构化日志条目 */
type WbAgentEntry =
  | { id: number; kind: 'user'; text: string }
  | { id: number; kind: 'phase_text'; phase: string; text: string }
  | { id: number; kind: 'tool_call'; phase: string; name: string; args: string }
  | { id: number; kind: 'tool_result'; phase: string; name: string; preview: string }
  | { id: number; kind: 'log'; phase: string; text: string }
  | { id: number; kind: 'error'; text: string }

/** pushEntry 时不含 id 的输入类型 */
type WbAgentInput =
  | { kind: 'user'; text: string }
  | { kind: 'phase_text'; phase: string; text: string }
  | { kind: 'tool_call'; phase: string; name: string; args: string }
  | { kind: 'tool_result'; phase: string; name: string; preview: string }
  | { kind: 'log'; phase: string; text: string }
  | { kind: 'error'; text: string }

/** Agent 阶段标签映射 */
const WB_PHASE_LABELS: Record<string, string> = {
  route: '路由', design: '设计', review: '审核', execute: '执行',
}

/** Agent 阶段对应的 CSS class */
function wbPhaseCls(phase: string): string {
  if (phase === 'route') return 'route'
  if (phase === 'design') return 'design'
  if (phase === 'review') return 'review'
  if (phase === 'execute') return 'execute'
  return ''
}

/** 渲染单条 WbAgentEntry */
function WbAgentEntryRow({ entry }: { entry: WbAgentEntry }) {
  const [open, setOpen] = React.useState(false)
  if (entry.kind === 'user') {
    return (
      <div className="agent-entry agent-entry-user">
        <span className="agent-entry-icon">👤</span>
        <span className="agent-entry-text">{entry.text}</span>
      </div>
    )
  }
  if (entry.kind === 'phase_text') {
    return (
      <div className={`agent-entry agent-entry-phase`}>
        <div className="agent-entry-phase-head" onClick={() => setOpen(o => !o)}>
          <span className={`agent-msg-phase ${wbPhaseCls(entry.phase)}`}>{WB_PHASE_LABELS[entry.phase] ?? entry.phase}</span>
          <span className="agent-entry-phase-preview">{entry.text.slice(0, 80).replace(/\n/g, ' ')}</span>
          <span className="agent-entry-chevron">{open ? '▲' : '▼'}</span>
        </div>
        {open && <pre className="agent-entry-phase-body">{entry.text}</pre>}
      </div>
    )
  }
  if (entry.kind === 'tool_call') {
    return (
      <div className="agent-entry agent-entry-tool-call" onClick={() => setOpen(o => !o)}>
        <div className="agent-entry-tool-head">
          <span className="agent-entry-tool-icon">🔧</span>
          <span className="agent-entry-tool-name">{entry.name}</span>
          {entry.phase && <span className={`agent-msg-phase ${wbPhaseCls(entry.phase)}`} style={{ fontSize: '0.62rem' }}>{WB_PHASE_LABELS[entry.phase] ?? entry.phase}</span>}
          <span className="agent-entry-chevron">{open ? '▲' : '▼'}</span>
        </div>
        {open && entry.args && <pre className="agent-entry-tool-args">{(() => { try { return JSON.stringify(JSON.parse(entry.args), null, 2) } catch { return entry.args } })()}</pre>}
      </div>
    )
  }
  if (entry.kind === 'tool_result') {
    return (
      <div className="agent-entry agent-entry-tool-result">
        <span className="agent-entry-tool-icon">✓</span>
        <span className="agent-entry-tool-name">{entry.name}</span>
        <span className="agent-entry-result-preview">{entry.preview}</span>
      </div>
    )
  }
  if (entry.kind === 'log') {
    return (
      <div className="agent-entry agent-entry-log">
        {entry.phase && <span className={`agent-msg-phase ${wbPhaseCls(entry.phase)}`} style={{ fontSize: '0.62rem' }}>{WB_PHASE_LABELS[entry.phase] ?? entry.phase}</span>}
        <span className="agent-entry-text">{entry.text}</span>
      </div>
    )
  }
  if (entry.kind === 'error') {
    return (
      <div className="agent-entry agent-entry-error">
        <span className="agent-entry-icon">❌</span>
        <span className="agent-entry-text">{entry.text}</span>
      </div>
    )
  }
  return null
}

/**
 * 按 Excel 风格的 number_format 格式化数值（仅用于表格阅读展示）。
 * 存储值和公式计算始终使用原始数值，格式不影响任何计算。
 */
function applyNumberFormat(value: unknown, fmt: string): string | number {
  if (!fmt || value == null || value === '') return value as string | number
  if (typeof value === 'string' && isNaN(Number(value))) return value // 非数字字符串原样
  const num = Number(value)
  if (isNaN(num)) return value as string

  if (fmt === '@') return String(value) // 强制文本

  // 百分比
  if (fmt.endsWith('%')) {
    const decimals = (fmt.match(/\.(\d+)%/) || ['', ''])[1].length
    return (num * 100).toFixed(decimals) + '%'
  }

  // 千分位
  const useComma = fmt.includes(',')
  // 小数位数
  const decimalMatch = fmt.match(/\.(\d+)/)
  const decimals = decimalMatch ? decimalMatch[1].length : 0

  let result = decimals > 0 ? num.toFixed(decimals) : Math.round(num).toString()
  if (useComma) {
    const parts = result.split('.')
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',')
    result = parts.join('.')
  }
  return result
}

function ensureSheetCapacity(sheet: SheetCapacityHandle | null | undefined, requiredRows: number, requiredCols: number): void {
  if (!sheet) return
  const targetRows = Math.max(MIN_UNIVER_SHEET_ROWS, requiredRows)
  const targetCols = Math.max(MIN_UNIVER_SHEET_COLUMNS, requiredCols)

  if ((sheet.getMaxRows?.() ?? 0) < targetRows) {
    sheet.setRowCount?.(targetRows)
  }
  if ((sheet.getMaxColumns?.() ?? 0) < targetCols) {
    sheet.setColumnCount?.(targetCols)
  }
}

type ConstantItem = {
  name_en: string
  name_zh: string
  value: unknown
  formula?: string | null
  brief?: string
  design_intent?: string
  scope_table?: string | null
  tags: string[]
}
type ConstTagItem = { name: string; parent?: string | null; brief?: string | null }

function ConstantsPanel({
  constants,
  tags,
  canWrite,
  headers,
  onRefresh,
}: {
  constants: ConstantItem[]
  tags: ConstTagItem[]
  canWrite?: boolean
  headers?: Record<string, string>
  onRefresh: () => void
}) {
  const [filter, setFilter] = useState('')
  const [editingKey, setEditingKey] = useState<string | null>(null)
  const [editDraft, setEditDraft] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveErr, setSaveErr] = useState<string | null>(null)

  const groups = useMemo(() => {
    const f = filter.trim().toLowerCase()
    const filtered = f
      ? constants.filter((c) =>
          [c.name_en, c.name_zh, c.brief ?? '', c.scope_table ?? '', ...(c.tags || [])]
            .join(' ')
            .toLowerCase()
            .includes(f),
        )
      : constants
    const map = new Map<string, ConstantItem[]>()
    for (const c of filtered) {
      const tagList = c.tags && c.tags.length > 0 ? c.tags : ['（未分类）']
      for (const t of tagList) {
        const arr = map.get(t) ?? []
        arr.push(c)
        map.set(t, arr)
      }
    }
    return Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0], 'zh'))
  }, [constants, filter])

  const formatValue = (v: unknown): string => {
    if (v === null || v === undefined) return '—'
    if (typeof v === 'number' || typeof v === 'boolean') return String(v)
    if (typeof v === 'string') return v
    try {
      return JSON.stringify(v)
    } catch {
      return String(v)
    }
  }

  const startEdit = (c: ConstantItem) => {
    if (!canWrite) return
    setEditingKey(c.name_en)
    // 公式常量用 = 前缀填入；纯值常量直接填值
    setEditDraft(c.formula ? `=${c.formula}` : formatValue(c.value))
    setSaveErr(null)
  }

  const cancelEdit = () => {
    setEditingKey(null)
    setEditDraft('')
    setSaveErr(null)
  }

  const saveEdit = async (name_en: string) => {
    if (!canWrite || !headers) return
    setSaving(true)
    setSaveErr(null)
    try {
      let body: Record<string, unknown>
      const trimmed = editDraft.trim()
      if (trimmed.startsWith('=')) {
        // 公式模式
        body = { formula: trimmed.slice(1).trim() }
      } else {
        // 数值模式：尝试解析为数字，否则按字符串处理
        const numVal = Number(trimmed)
        body = { value: trimmed !== '' && !isNaN(numVal) ? numVal : trimmed }
      }
      await apiFetch(`/meta/constants/${encodeURIComponent(name_en)}`, {
        method: 'PATCH',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      setEditingKey(null)
      onRefresh()
    } catch (e) {
      setSaveErr(String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ padding: '0.5rem 0.25rem', overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
        <h3 style={{ margin: 0 }}>📐 常量（{constants.length}）</h3>
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="搜索名称 / 标签 / 简介"
          style={{ flex: 1, maxWidth: 320, padding: '0.25rem 0.5rem' }}
        />
        <button type="button" className="btn tiny" onClick={onRefresh}>刷新</button>
      </div>
      {saveErr && <p style={{ color: 'red', fontSize: '0.8rem' }}>保存失败：{saveErr}</p>}
      {tags.length > 0 && (
        <p className="muted small" style={{ marginBottom: '0.75rem' }}>
          共 {tags.length} 个标签：{tags.map((t) => t.name).join('、')}
        </p>
      )}
      {groups.length === 0 ? (
        <p className="muted">暂无常量。AI 在 02 阶段使用 <code>const_register</code> 工具登记后会显示在这里。</p>
      ) : (
        groups.map(([tag, items]) => (
          <details key={tag} open style={{ marginBottom: '0.75rem', border: '1px solid #2a2a2a', borderRadius: 6, padding: '0.5rem 0.75rem' }}>
            <summary style={{ cursor: 'pointer', fontWeight: 600 }}>
              {tag} <span className="muted small">（{items.length}）</span>
            </summary>
            <table style={{ width: '100%', marginTop: '0.5rem', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ textAlign: 'left', borderBottom: '1px solid #333' }}>
                  <th style={{ padding: '0.25rem 0.5rem' }}>名称 (en)</th>
                  <th style={{ padding: '0.25rem 0.5rem' }}>中文</th>
                  <th style={{ padding: '0.25rem 0.5rem' }}>值 / 公式 {canWrite && <span className="muted small">（点击编辑；=公式）</span>}</th>
                  <th style={{ padding: '0.25rem 0.5rem' }}>简介</th>
                  <th style={{ padding: '0.25rem 0.5rem' }}>设计意图</th>
                  <th style={{ padding: '0.25rem 0.5rem' }}>范围表</th>
                </tr>
              </thead>
              <tbody>
                {items.map((c) => (
                  <tr key={c.name_en} style={{ borderBottom: '1px solid #222' }}>
                    <td style={{ padding: '0.25rem 0.5rem', fontFamily: 'monospace' }}>{c.name_en}</td>
                    <td style={{ padding: '0.25rem 0.5rem' }}>{c.name_zh}</td>
                    <td style={{ padding: '0.25rem 0.5rem', fontFamily: 'monospace' }}>
                      {editingKey === c.name_en ? (
                        <span style={{ display: 'flex', gap: '0.3rem', alignItems: 'center' }}>
                          <input
                            autoFocus
                            style={{ width: 140, padding: '0.15rem 0.3rem', fontFamily: 'monospace', fontSize: '0.8rem' }}
                            value={editDraft}
                            placeholder={c.formula ? '=公式 或 数值' : '数值 或 =公式'}
                            onChange={(e) => setEditDraft(e.target.value)}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') void saveEdit(c.name_en)
                              if (e.key === 'Escape') cancelEdit()
                            }}
                          />
                          <button type="button" className="btn tiny primary" disabled={saving} onClick={() => void saveEdit(c.name_en)}>
                            {saving ? '…' : '✓'}
                          </button>
                          <button type="button" className="btn tiny" onClick={cancelEdit}>✕</button>
                        </span>
                      ) : (
                        <span
                          style={{ cursor: canWrite ? 'pointer' : undefined, textDecoration: canWrite ? 'underline dotted' : undefined }}
                          title={canWrite ? (c.formula ? `公式：${c.formula}（点击编辑）` : '点击编辑值') : undefined}
                          onClick={() => startEdit(c)}
                        >
                          {formatValue(c.value)}
                          {c.formula && (
                            <small style={{ marginLeft: '0.35rem', color: '#81c784', fontStyle: 'italic', fontFamily: 'monospace' }}
                              title={`公式：${c.formula}`}>
                              ={c.formula}
                            </small>
                          )}
                        </span>
                      )}
                    </td>
                    <td style={{ padding: '0.25rem 0.5rem' }} className="small">{c.brief || '—'}</td>
                    <td style={{ padding: '0.25rem 0.5rem' }} className="small muted">{c.design_intent || '—'}</td>
                    <td style={{ padding: '0.25rem 0.5rem' }} className="small muted">{c.scope_table || '全局'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        ))
      )}
    </div>
  )
}

function InlineConstRow({ c, showEn, canWrite, headers, onRefresh }: {
  c: { name_en: string; name_zh: string; value: unknown; formula?: string | null }
  showEn: boolean
  canWrite: boolean
  headers: Record<string, string> | undefined
  onRefresh: () => void
}) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState('')
  const [saving, setSaving] = useState(false)

  const displayValue = (v: unknown) => {
    if (v === null || v === undefined) return '—'
    if (typeof v === 'number' || typeof v === 'boolean') return String(v)
    if (typeof v === 'string') return v
    try { return JSON.stringify(v) } catch { return String(v) }
  }

  const startEdit = () => {
    if (!canWrite || !headers) return
    setDraft(c.formula ? `=${c.formula}` : displayValue(c.value))
    setEditing(true)
  }

  const saveEdit = async () => {
    if (!headers) return
    setSaving(true)
    try {
      const trimmed = draft.trim()
      let body: Record<string, unknown>
      if (trimmed.startsWith('=')) {
        body = { formula: trimmed.slice(1).trim() }
      } else {
        const n = Number(trimmed)
        body = { value: trimmed !== '' && !isNaN(n) ? n : trimmed }
      }
      await apiFetch(`/meta/constants/${encodeURIComponent(c.name_en)}`, {
        method: 'PATCH', headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      setEditing(false)
      onRefresh()
    } catch { /* ignore */ }
    finally { setSaving(false) }
  }

  return (
    <tr key={c.name_en}>
      <td title={c.name_en}>
        <code>{showEn ? c.name_en : (c.name_zh || c.name_en)}</code>
      </td>
      <td
        title={canWrite ? '点击编辑' : ''}
        style={canWrite ? { cursor: 'pointer', textDecoration: 'underline dotted' } : {}}
        onClick={startEdit}>
        {editing ? (
          <>
            <input value={draft} onChange={e => setDraft(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') saveEdit(); if (e.key === 'Escape') setEditing(false) }}
              style={{ width: 100, fontSize: 12 }} autoFocus disabled={saving} />
            <button type="button" className="btn tiny" onClick={saveEdit} disabled={saving}>✓</button>
            <button type="button" className="btn tiny" onClick={() => setEditing(false)}>✕</button>
          </>
        ) : displayValue(c.value)}
      </td>
    </tr>
  )
}

/** 把文本中 $name$ 替换为词汇表中文，${name} 替换为常量中文名，找不到时标红 */
function renderGlossaryText(
  text: string,
  glossaryMap: Map<string, GlossaryItem>,
  constMap?: Map<string, { name_zh?: string; value?: unknown }>,
  lang: 'zh' | 'en' = 'zh',
): React.ReactNode[] {
  const parts = text.split(/(\$\{[a-zA-Z0-9_]+\}|\$[a-zA-Z0-9_]+\$)/g)
  return parts.map((part, i) => {
    // ${name} 常量引用
    const cm = part.match(/^\$\{([a-zA-Z0-9_]+)\}$/)
    if (cm && constMap) {
      const key = cm[1]
      const c = constMap.get(key)
      if (c) {
        const label = lang === 'en' ? key : (c.name_zh || key)
        return <span key={i} title={`${key}=${c.value ?? '?'}`} style={{ color: '#2e7d32', fontWeight: 500 }}>{label}</span>
      }
      return <span key={i} title={`未找到常量：${key}`} style={{ color: '#ef9a9a', textDecoration: 'underline dotted' }}>{part}</span>
    }
    // $name$ 词汇表引用
    const m = part.match(/^\$([a-zA-Z0-9_]+)\$$/)
    if (!m) return part
    const key = m[1]
    const g = glossaryMap.get(key)
    if (g) return <span key={i} title={key} style={{ color: '#0277bd', fontWeight: 500 }}>{lang === 'en' ? g.term_en : g.term_zh}</span>
    // 降级：也尝试从常量中查找
    if (constMap) {
      const c = constMap.get(key)
      if (c) {
        const label = lang === 'en' ? key : (c.name_zh || key)
        return <span key={i} title={`常量 ${key}=${c.value ?? '?'}`} style={{ color: '#2e7d32', fontWeight: 500 }}>{label}</span>
      }
    }
    return <span key={i} title={`未找到术语：${key}`} style={{ color: '#ef9a9a', textDecoration: 'underline dotted' }}>{part}</span>
  })
}

// ---- CalculatorsPanel ----
function CalculatorsPanel({
  calculators,
  headers,
  onRefresh,
}: {
  calculators: CalculatorItem[]
  headers: Record<string, string>
  onRefresh: () => void
}) {
  const [expanded, setExpanded] = useState<string | null>(null)
  const [trialArgs, setTrialArgs] = useState<Record<string, Record<string, string>>>({})
  const [trialResult, setTrialResult] = useState<Record<string, unknown>>({})
  const [trialLoading, setTrialLoading] = useState<string | null>(null)

  async function runTrial(name: string) {
    const args = trialArgs[name] || {}
    setTrialLoading(name)
    try {
      const r = (await apiFetch('/compute/call-calculator', {
        method: 'POST',
        headers,
        body: JSON.stringify({ name, args }),
      })) as Record<string, unknown>
      setTrialResult((prev) => ({ ...prev, [name]: r }))
    } catch (e) {
      setTrialResult((prev) => ({ ...prev, [name]: { error: String(e) } }))
    }
    setTrialLoading(null)
  }

  return (
    <div style={{ padding: '0.5rem 0.25rem', overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
        <h3 style={{ margin: 0 }}>🧮 Calculators（{calculators.length}）</h3>
        <button type="button" className="btn tiny" onClick={onRefresh}>刷新</button>
      </div>
      {calculators.length === 0 ? (
        <p className="muted small">暂无 Calculator。AI 在 matrix 表创建或 register_calculator 后会出现在这里。</p>
      ) : (
        calculators.map((c) => (
          <details
            key={c.name}
            open={expanded === c.name}
            onToggle={(e) => {
              if ((e.target as HTMLDetailsElement).open) setExpanded(c.name)
              else if (expanded === c.name) setExpanded(null)
            }}
            style={{ marginBottom: '0.5rem', border: '1px solid #2a2a2a', borderRadius: 6, padding: '0.4rem 0.6rem' }}
          >
            <summary style={{ cursor: 'pointer' }}>
              <strong style={{ fontFamily: 'monospace' }}>{c.name}</strong>{' '}
              <span className="muted small">({c.kind}) → {c.table_name}</span>
            </summary>
            <p className="small" style={{ margin: '0.3rem 0' }}>{c.brief}</p>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginBottom: '0.5rem' }}>
              {c.axes.filter((a) => a.name !== 'grain').map((a) => (
                <label key={a.name} style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', fontSize: '0.8rem' }}>
                  <span className="muted">{a.name}:</span>
                  <input
                    type="text"
                    value={(trialArgs[c.name] || {})[a.name] ?? ''}
                    onChange={(e) =>
                      setTrialArgs((prev) => ({
                        ...prev,
                        [c.name]: { ...prev[c.name], [a.name]: e.target.value },
                      }))
                    }
                    placeholder={a.source}
                    style={{ width: 80, fontSize: '0.8rem', padding: '0.15rem 0.3rem' }}
                  />
                </label>
              ))}
            </div>
            <button
              type="button"
              className="btn tiny primary"
              disabled={trialLoading === c.name}
              onClick={() => void runTrial(c.name)}
            >
              {trialLoading === c.name ? '计算中…' : '试算'}
            </button>
            {trialResult[c.name] != null && (() => {
              const res = trialResult[c.name] as Record<string, unknown>
              if (res.error) return <span className="err small" style={{ marginLeft: 8 }}>{String(res.error)}</span>
              return (
                <span style={{ marginLeft: 8, fontSize: '0.85rem' }}>
                  结果：<strong>{res.value == null ? '—' : String(res.value)}</strong>
                  {res.found === false && <span className="muted small"> (未匹配)</span>}
                  {Boolean(res.fallback) && <span className="muted small"> (fallback)</span>}
                </span>
              )
            })()}
          </details>
        ))
      )}
    </div>
  )
}

export default function Workbench() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  const [searchParams] = useSearchParams()
  /** ?ro=1 → 只读模式（被 ProjectSetup 等场景嵌入时锁定单元格编辑） */
  const readOnly = searchParams.get('ro') === '1'
  /** 必须 memo：否则每次 render 新 headers 对象会触发 effect/useCallback 无限循环 → 浏览器 ERR_INSUFFICIENT_RESOURCES */
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [tables, setTables] = useState<TableInfo[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  /** 用户在 Univer 中当前选中的单元格/区域上下文（传给维护 Agent） */
  const [cellSelection, setCellSelection] = useState<string | null>(null)
  // 当前活动表的行数据（仅用于缓存反向写入；展示由 Univer 接管）
  const [, setRows] = useState<Record<string, unknown>[]>([])
  const [tableReadmeDraft, setTableReadmeDraft] = useState('')
  const [globalReadmeDraft, setGlobalReadmeDraft] = useState('')
  const [readmeTab, setReadmeTab] = useState<'table' | 'global' | 'calculators' | 'design'>('table')
  /** README 编辑/预览模式（每个 tab 独立） */
  const [readmeViewMode, setReadmeViewMode] = useState<'preview' | 'edit'>('preview')
  const [canWrite, setCanWrite] = useState(false)
  const [validateReport, setValidateReport] = useState<ValidateReport | null>(null)
  const [validationRulesDraft, setValidationRulesDraft] = useState('')
  const [snapshots, setSnapshots] = useState<SnapshotRow[]>([])
  const [compareSnapshotId, setCompareSnapshotId] = useState<number | null>(null)
  const [compareText, setCompareText] = useState('')
  /** 当前表关联的常数（来自 _constants） */
  const [relatedConstants, setRelatedConstants] = useState<Array<{
    name_en: string
    name_zh: string
    value: unknown
    brief?: string
    scope_table?: string | null
  }>>([])
  const [showConstEn, setShowConstEn] = useState(false)
  const [kindFilter, setKindFilter] = useState<string>('all')  // all / config / compute
  const [columnKinds, setColumnKinds] = useState<Record<string, string>>({})
  const [activeTableKind, setActiveTableKind] = useState('')
  const tableDispMap = useMemo(
    () => new Map(tables.filter(t => t.display_name).map(t => [t.table_name, t.display_name!])),
    [tables],
  )
  const [showEnNames, setShowEnNames] = useState(false)  // false=显示中文，true=显示英文
  const [showValDetail, setShowValDetail] = useState(false)

  /** 全部常量（用于"📐 常量"专属页） */
  const [allConstants, setAllConstants] = useState<Array<{
    name_en: string
    name_zh: string
    value: unknown
    formula?: string | null
    brief?: string
    scope_table?: string | null
    tags: string[]
  }>>([])
  const [allConstTags, setAllConstTags] = useState<Array<{
    name: string
    parent?: string | null
    brief?: string | null
  }>>([])
  const [pipeline, setPipeline] = useState<{
    next_expected_step: string | null
    completed_steps: string[]
    finished?: boolean
  } | null>(null)
  const [agentEntries, setAgentEntries] = useState<WbAgentEntry[]>([])
  const [agentStream, setAgentStream] = useState('')
  const [agentLivePhase, setAgentLivePhase] = useState('')
  const [agentInput, setAgentInput] = useState('')
  const [agentBusy, setAgentBusy] = useState(false)
  const [agentMode, setAgentMode] = useState<'init' | 'maintain'>('maintain')
  const [agentOpen, setAgentOpen] = useState(() => localStorage.getItem('wb_agent_open') !== 'false')
  const agentEntryIdRef = useRef(0)
  const agentLogEndRef = useRef<HTMLDivElement | null>(null)
  const [err, setErr] = useState<string | null>(null)
  /** 列名 -> 公式信息（用于公式栏显示与编辑） */
  const [columnFormulas, setColumnFormulas] = useState<FormulaMap>({})
  /** 公式栏：当前选中列名 */
  const [formulaBarCol, setFormulaBarCol] = useState<string | null>(null)
  /** 公式栏：正在编辑的公式文本 */
  const [formulaBarText, setFormulaBarText] = useState('')
  /** 公式栏：是否有未保存的改动 */
  const [formulaBarDirty, setFormulaBarDirty] = useState(false)
  /** 公式栏：是否正在保存 */
  const [formulaBarSaving, setFormulaBarSaving] = useState(false)
  /** 公式栏：常量编辑状态 */
  const [fabConstEditName, setFabConstEditName] = useState<string | null>(null)
  const [fabConstEditVal, setFabConstEditVal] = useState('')
  const [fabConstSaving, setFabConstSaving] = useState(false)
  /** 当前活动表的列顺序（用于将 Univer 行/列索引映射回 row_id/列名） */
  const [activeCols, setActiveCols] = useState<string[]>([])
  /** 当前活动表的列元信息（中文名/数据类型，用于 3 行表头） */
  const [, setActiveColMeta] = useState<ColumnMeta[]>([])
  /** 当前活动表的中文显示名 */
  const [activeDisplayName, setActiveDisplayName] = useState<string>('')
  /** 当前项目绑定的 AI 模型 */
  const [aiModel, setAiModel] = useState<string>('')
  /** 可用 AI 模型分组列表 */
  const [aiModelGroups, setAiModelGroups] = useState<Array<{ label: string; models: string[] }>>([])
  const [modelSwitching, setModelSwitching] = useState(false)

  // -------- 第4轮新增状态 --------
  /** Calculators 列表 */
  const [calculators, setCalculators] = useState<CalculatorItem[]>([])
  /** 词汇表 */
  const [glossary, setGlossary] = useState<GlossaryItem[]>([])
  /** 目录树折叠状态 */
  const [collapsedDirs, setCollapsedDirs] = useState<Set<string>>(new Set())
  /** 拖拽中的表名 */
  const [draggingTable, setDraggingTable] = useState<string | null>(null)
  /** 拖拽悬停的目录 */
  const [dragOverDir, setDragOverDir] = useState<string | null>(null)
  /** 当前选中表的暴露参数（仅 gameplay_landing_tables.* 时加载） */
  const [exposedParams, setExposedParams] = useState<ExposedParam[]>([])
  /** 设计文档历史（completed_steps 对应的 design_text） */
  const [designHistory, setDesignHistory] = useState<DesignEntry[]>([])
  /** 设计文档抽屉选中步骤 */
  const [designStep, setDesignStep] = useState<string | null>(null)
  /** 当前选中表的 matrix_meta_json（已解析） */
  const [selectedMatrixMeta, setSelectedMatrixMeta] = useState<Record<string, unknown> | null>(null)

  // -------- Univer 相关 --------
  const univerHostRef = useRef<HTMLDivElement | null>(null)
  const univerRef = useRef<Univer | null>(null)
  const univerAPIRef = useRef<FUniver | null>(null)
  const workbookRef = useRef<FWorkbook | null>(null)
  /** 已加载到 Univer 的 sheet（按 table_name 记录） */
  const loadedSheetsRef = useRef<Set<string>>(new Set())
  /** 每张已加载表的行数据缓存（用于 row 索引→row_id 映射） */
  const tableRowsCacheRef = useRef<Map<string, Record<string, unknown>[]>>(new Map())
  /** 每张已加载表的列顺序缓存 */
  const tableColsCacheRef = useRef<Map<string, string[]>>(new Map())
  /** 每张已加载表的列公式缓存 */
  const tableFormulasCacheRef = useRef<Map<string, FormulaMap>>(new Map())
  /** 每张已加载表的列元信息缓存（display_name/dtype） */
  const tableColMetaCacheRef = useRef<Map<string, ColumnMeta[]>>(new Map())
  /** 当前活动 table_name（事件回调内引用最新值） */
  const activeTableRef = useRef<string | null>(null)
  /** 标记内部 setValues 写入，避免触发回写 API */
  const suppressEditRef = useRef(false)
  /** 持久指向最新的写回函数（避免 SheetEditEnded 闭包过期） */
  const writeCellManualRef = useRef<
    (tableName: string, rowId: string, colName: string, value: unknown) => Promise<void>
  >(async () => {})
  /** 单元格写入后的"重算+刷新"防抖计时器（避免连续编辑触发整表重渲染卡顿） */
  const recalcDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pendingRecalcTablesRef = useRef<Set<string>>(new Set())
  /** 当前选中表是否是传统 2D 矩阵表（基于 tables 列表同步判断，避免 selectedMatrixMeta 异步更新时序问题） */
  const selectedIsMatrix = useMemo(
    () => Boolean(tables.find((t) => t.table_name === selected)?.is_matrix),
    [tables, selected],
  )
  /** 当前选中表是否是三维矩阵表（可通过多 sheet 视图展示） */
  const selectedIs3DMatrix = useMemo(
    () => {
      const info = tables.find((t) => t.table_name === selected)
      return Boolean(info?.is_3d_matrix || selectedMatrixMeta?.kind === '3d_matrix')
    },
    [tables, selected, selectedMatrixMeta],
  )

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

  const loadAllConstants = useCallback(async () => {
    try {
      const d = (await apiFetch('/meta/constants', { headers })) as {
        constants?: Array<{
          name_en: string
          name_zh: string
          value: unknown
          formula?: string | null
          brief?: string
          scope_table?: string | null
          tags?: string[]
        }>
        tags?: Array<{ name: string; parent?: string | null; brief?: string | null }>
      }
      const list = Array.isArray(d.constants) ? d.constants : []
      setAllConstants(
        list.map((c) => ({
          name_en: String(c.name_en),
          name_zh: String(c.name_zh ?? ''),
          value: c.value,
          formula: c.formula ?? null,
          brief: c.brief,
          scope_table: c.scope_table,
          tags: Array.isArray(c.tags) ? c.tags.map(String) : [],
        })),
      )
      setAllConstTags(Array.isArray(d.tags) ? d.tags : [])
    } catch (e) {
      console.warn('加载常量失败', e)
    }
  }, [headers])

  const loadRelatedConstants = useCallback(async (tableName: string) => {
    if (!headers || !tableName) return
    try {
      const desc = (await apiFetch(`/data/tables/${encodeURIComponent(tableName)}`, { headers })) as {
        related_constants?: Array<{ name_en: string; name_zh: string; value: unknown; brief?: string; scope_table?: string | null }>
      }
      setRelatedConstants(Array.isArray(desc.related_constants) ? desc.related_constants : [])
    } catch { /* ignore */ }
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

  const loadAiModel = useCallback(async () => {
    try {
      const r = (await apiFetch('/meta/ai-model', { headers })) as { model: string }
      setAiModel(r.model || '')
    } catch { /* ignore */ }
  }, [headers])

  const loadAiModels = useCallback(async () => {
    try {
      const r = (await apiFetch('/meta/ai-models', { headers })) as {
        models?: string[]
        groups?: Array<{ label: string; models: string[] }>
      }
      if (Array.isArray(r.groups) && r.groups.length > 0) {
        setAiModelGroups(r.groups)
      } else if (Array.isArray(r.models)) {
        setAiModelGroups([{ label: '模型', models: r.models }])
      }
    } catch { /* ignore */ }
  }, [headers])

  const switchAiModel = useCallback(async (model: string) => {
    setModelSwitching(true)
    try {
      await apiFetch('/meta/ai-model', { method: 'PUT', headers, body: JSON.stringify({ model }) })
      setAiModel(model)
    } catch { /* ignore */ }
    setModelSwitching(false)
  }, [headers])

  const loadCalculators = useCallback(async () => {
    try {
      const r = (await apiFetch('/meta/calculators', { headers })) as { calculators?: unknown[] }
      setCalculators(Array.isArray(r.calculators) ? (r.calculators as CalculatorItem[]) : [])
    } catch { /* ignore */ }
  }, [headers])

  const loadGlossary = useCallback(async () => {
    try {
      const r = (await apiFetch('/meta/glossary', { headers })) as { glossary?: GlossaryItem[] }
      setGlossary(Array.isArray(r.glossary) ? r.glossary : [])
    } catch { /* ignore */ }
  }, [headers])

  const loadExposedParams = useCallback(async (stepId: string) => {
    if (!stepId.startsWith('gameplay_landing_tables.')) {
      setExposedParams([])
      return
    }
    try {
      const r = (await apiFetch(
        `/meta/exposed-params?target_step=${encodeURIComponent(stepId)}`,
        { headers },
      )) as { items?: ExposedParam[] }
      setExposedParams(Array.isArray(r.items) ? r.items : [])
    } catch { setExposedParams([]) }
  }, [headers])

  const loadDesignHistory = useCallback(async () => {
    try {
      const r = (await apiFetch('/pipeline/design-history', { headers })) as { entries?: DesignEntry[] }
      const entries = Array.isArray(r.entries) ? r.entries : []
      setDesignHistory(entries)
      if (entries.length > 0 && !designStep) {
        setDesignStep(entries[entries.length - 1].step_id)
      }
    } catch { /* design history may not exist yet */ }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [headers])

  /** 更新表的 directory（拖拽落点） */
  const updateTableDirectory = useCallback(async (tableName: string, newDir: string) => {
    try {
      await apiFetch(`/meta/tables/${encodeURIComponent(tableName)}/directory`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({ directory: newDir }),
      })
      await loadTables()
    } catch (e) {
      setErr(String(e))
    }
  }, [headers, loadTables])

  const loadValidation = useCallback(async () => {
    try {
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
    } catch {
      setValidateReport(null)
    }
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

  // -------- Univer 初始化（每个 pid 独立工作簿） --------
  useEffect(() => {
    if (!Number.isFinite(pid)) return
    const host = univerHostRef.current
    if (!host) return
    const { univer, univerAPI } = createUniver({
      locale: LocaleType.ZH_CN,
      locales: { [LocaleType.ZH_CN]: UniverZhCN },
      theme: defaultTheme,
      presets: [UniverSheetsCorePreset({ container: host })],
    })
    const wb = univerAPI.createWorkbook({
      id: `wb_${pid}`,
      name: `项目 ${pid}`,
      sheets: {
        __placeholder__: {
          id: '__placeholder__',
          name: '加载中…',
          rowCount: MIN_UNIVER_SHEET_ROWS,
          columnCount: MIN_UNIVER_SHEET_COLUMNS,
          cellData: {},
        },
      },
      sheetOrder: ['__placeholder__'],
    })
    univerRef.current = univer
    univerAPIRef.current = univerAPI
    workbookRef.current = wb
    loadedSheetsRef.current = new Set()
    tableRowsCacheRef.current = new Map()
    tableColsCacheRef.current = new Map()
    activeTableRef.current = null

    const disposable = univerAPI.addEvent(univerAPI.Event.SheetEditEnded, (params) => {
      if (suppressEditRef.current) return
      if (readOnly) return
      if (!params.isConfirm) return
      const tname = activeTableRef.current
      if (!tname) return
      const cols = tableColsCacheRef.current.get(tname) || []
      const rowsArr = tableRowsCacheRef.current.get(tname) || []
      // 行 0/1/2 = 中文名 / 英文名 / 数据类型；数据从第 3 行开始
      const dataRowOffset = 3
      const dataRowIdx = params.row - dataRowOffset
      const colName = cols[params.column]
      if (!colName || dataRowIdx < 0 || dataRowIdx >= rowsArr.length) return
      const rowObj = rowsArr[dataRowIdx]
      const rid = rowObj?.row_id
      if (rid == null) return
      const newCell = params.worksheet.getRange(params.row, params.column).getValue()
      const newVal = typeof newCell === 'object' && newCell != null ? (newCell as { v?: unknown }).v ?? null : newCell
      void writeCellManualRef.current(tname, String(rid), colName, newVal)
    })

    // 列点击检测：mouseup 时读取 Univer 当前选区的列索引并更新公式栏 & 单元格上下文
    const onUniverMouseUp = () => {
      try {
        const sh = workbookRef.current?.getActiveSheet()
        if (!sh) return
        const range = (sh as unknown as { getActiveRange?: () => { getColumn?: () => number; getRow?: () => number } | null }).getActiveRange?.()
        if (!range) { setCellSelection(null); return }
        const col = range.getColumn?.()
        const row = range.getRow?.()
        const tname = activeTableRef.current
        if (!tname) { setCellSelection(null); return }

        // 始终更新公式栏（不依赖是否有行信息）
        if (col != null && col >= 0) {
          const cols = tableColsCacheRef.current.get(tname) || []
          if (col < cols.length) {
            const colName = cols[col]
            if (colName) {
              setFormulaBarCol(colName)
              const formulas = tableFormulasCacheRef.current.get(tname) || {}
              const fi = formulas[colName]
              setFormulaBarText(fi?.formula || '')
              setFormulaBarDirty(false)
            }
          }
        }

        // 更新 cellSelection 上下文
        if (col != null && col >= 0 && row != null && row >= 0) {
          const cols = tableColsCacheRef.current.get(tname) || []
          const rowsArr = tableRowsCacheRef.current.get(tname) || []
          const dataRowOffset = 3
          const dataRowIdx = row - dataRowOffset
          if (col < cols.length && dataRowIdx >= 0 && dataRowIdx < rowsArr.length) {
            const colName = cols[col]
            const rowObj = rowsArr[dataRowIdx]
            const rid = rowObj?.row_id
            const val = rowObj?.[colName]
            if (rid && colName) {
              const valStr = val !== undefined && val !== null ? `，值: ${val}` : ''
              setCellSelection(`表 ${tname} 的 ${rid} 行, ${colName} 列${valStr}`)
            } else {
              setCellSelection(null)
            }
          } else {
            setCellSelection(`表 ${tname} 的 ${String.fromCharCode(65 + col)}${row + 1} 单元格`)
          }
        } else {
          setCellSelection(null)
        }
      } catch { setCellSelection(null) }
    }
    host.addEventListener('mouseup', onUniverMouseUp)
    const loadedSheets = loadedSheetsRef.current
    const tableRowsCache = tableRowsCacheRef.current
    const tableColsCache = tableColsCacheRef.current
    const tableFormulasCache = tableFormulasCacheRef.current

    return () => {
      host.removeEventListener('mouseup', onUniverMouseUp)
      disposable.dispose()
      // Delay dispose by one rAF to let Univer's internal async loops (e.g.
      // calculateAutoHeightInRange) finish their current tick before teardown.
      const univerToDispose = univer
      requestAnimationFrame(() => {
        try { univerToDispose.dispose() } catch { /* suppress Univer internal disposal errors */ }
      })
      univerRef.current = null
      univerAPIRef.current = null
      workbookRef.current = null
      loadedSheets.clear()
      tableRowsCache.clear()
      tableColsCache.clear()
      tableFormulasCache.clear()
      activeTableRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pid])

  // 每个 pid 切换时重置本地编辑态（E3）
  useEffect(() => {
    if (!Number.isFinite(pid)) return
    let cancelled = false
    setErr(null)
    setTables([])
    setSelected(null)
    setCellSelection(null)
    setRows([])
    setActiveCols([])
    setTableReadmeDraft('')
    setGlobalReadmeDraft('')
    setValidationRulesDraft('')
    setColumnFormulas({})
    setValidateReport(null)
    setSnapshots([])
    setCompareSnapshotId(null)
    setCompareText('')
    setPipeline(null)
    setCalculators([])
    setGlossary([])
    setExposedParams([])
    setDesignHistory([])
    setSelectedMatrixMeta(null)
    void Promise.all([
      loadTables(),
      loadProjectConfig(),
      loadPipeline(),
      loadValidation(),
      loadSnapshots(),
      loadAiModel(),
      loadAiModels(),
      loadCalculators(),
      loadGlossary(),
      loadDesignHistory(),
      loadAllConstants(),
    ]).catch((e) => {
      if (!cancelled) setErr(String(e))
    })
    return () => {
      cancelled = true
    }
  }, [pid, loadTables, loadProjectConfig, loadPipeline, loadValidation, loadSnapshots, loadAiModel, loadAiModels, loadCalculators, loadGlossary, loadDesignHistory, loadAllConstants])

  /** 把一张表的数据写入对应 Univer sheet（首次或刷新调用） */
   const populateSheet = useCallback(
    (tableName: string, rowsArr: Record<string, unknown>[], cols: string[], formulas: FormulaMap, colMeta: ColumnMeta[] = [], displayName = '', columnKinds: Record<string, string> = {}, tableKind = '') => {
      const wb = workbookRef.current
      if (!wb) return
      const sheetTitle = displayName ? `${displayName}（${tableName}）` : tableName
      let sheet = wb.getSheetByName(sheetTitle) ?? wb.getSheetByName(tableName)
      if (!sheet) {
        sheet = wb.insertSheet(sheetTitle, {
          sheet: {
            rowCount: MIN_UNIVER_SHEET_ROWS,
            columnCount: MIN_UNIVER_SHEET_COLUMNS,
            cellData: {},
          },
        }) ?? wb.getSheetByName(sheetTitle)
        try {
          const placeholder = wb.getSheetByName('加载中…')
          if (placeholder) wb.deleteSheet(placeholder)
        } catch { /* ignore */ }
      }
      if (!sheet) return
      tableRowsCacheRef.current.set(tableName, rowsArr)
      tableColsCacheRef.current.set(tableName, cols)
      tableFormulasCacheRef.current.set(tableName, formulas)
      tableColMetaCacheRef.current.set(tableName, colMeta)

      const metaByName = new Map(colMeta.map((m) => [m.name, m]))
      const dispRow: (string | number)[] = cols.map((c) => metaByName.get(c)?.display_name || c)
      const nameRow: (string | number)[] = cols.length === 0 ? ['(空表)'] : cols
      // 第3行：数值格式（有格式显示格式字符串，无格式降级到 dtype 或 sql_type）
      const fmtRow: (string | number)[] = cols.map((c) => {
        const m = metaByName.get(c)
        return m?.number_format || m?.dtype || m?.sql_type || ''
      })

      // 3 行表头：中文名 / 英文名 / 数值格式。数据行按 number_format 格式化显示（原始值存储不变）。
      const matrix: (string | number)[][] = [dispRow, nameRow, fmtRow]
      for (const r of rowsArr) {
        matrix.push(cols.map((c) => {
          const v = r[c]
          if (v == null) return ''
          if (typeof v === 'object') return JSON.stringify(v)
          const fmt = metaByName.get(c)?.number_format || ''
          if (fmt && (typeof v === 'number' || (typeof v === 'string' && !isNaN(Number(v)) && v !== ''))) {
            return applyNumberFormat(v, fmt)
          }
          if (typeof v === 'number' || typeof v === 'string') return v
          return String(v)
        }))
      }
      const numCols = Math.max(1, cols.length)
      ensureSheetCapacity(sheet as SheetCapacityHandle, matrix.length, numCols)
      suppressEditRef.current = true
      try {
        try {
          const usedRange = sheet.getDataRange?.()
          if (usedRange) usedRange.clearContent()
        } catch { /* ignore */ }
        sheet.getRange(0, 0, matrix.length, numCols).setValues(matrix)
        // 设置列宽（避免 Univer 打印布局计算 "column width < 0" 警告）
        try {
          const sheetCols = sheet as unknown as { setColumnWidth?: (col: number, width: number) => unknown }
          if (sheetCols.setColumnWidth) {
            const MIN_COL_WIDTH = 90
            for (let ci = 0; ci < numCols; ci++) {
              sheetCols.setColumnWidth(ci, MIN_COL_WIDTH)
            }
          }
        } catch { /* ignore column width errors */ }
        // 表头样式（尽力而为，不同 Univer 版本 API 略有差异）
        try {
          const headRange = sheet.getRange(0, 0, 3, numCols)
          const styleSetters = headRange as unknown as {
            setBackgroundColor?: (c: string) => unknown
            setBackground?: (c: string) => unknown
            setFontWeight?: (w: string) => unknown
          }
          styleSetters.setBackgroundColor?.('#e8f5e9')
          styleSetters.setBackground?.('#e8f5e9')
          styleSetters.setFontWeight?.('bold')
        } catch { /* ignore styling errors */ }
        // ── 列分类着色：计算列浅灰(#f0f0f0)、配置列浅棕(#faf0e6) ──
        // 纯配置表/纯计算表：全部列着色（无需 column_kinds 逐列标注）
        // 混合表：仅 column_kinds 中标明的列着色
        try {
          const dataStartRow = 3
          const dataEndRow = matrix.length - 1
          if (dataEndRow >= dataStartRow) {
            const allCompute = tableKind === 'compute'
            const allConfig = tableKind === 'config'
            for (let ci = 0; ci < numCols; ci++) {
              const colName = cols[ci] || ''
              let kind = columnKinds[colName] || ''
              if (!kind && allCompute) kind = 'compute'
              if (!kind && allConfig) kind = 'config'
              if (kind !== 'config' && kind !== 'compute') continue
              const bg = kind === 'compute' ? '#f0f0f0' : '#faf0e6'
              // 使用 sheet.getRange 单行批量着色比逐单元格快
              const colRange = sheet.getRange(dataStartRow, ci, dataEndRow - dataStartRow + 1, 1)
              try {
                const cs = colRange as unknown as { setBackground?: (c: string) => unknown; setBackgroundColor?: (c: string) => unknown }
                cs.setBackground?.(bg)
                cs.setBackgroundColor?.(bg)
              } catch {
                // fallback: 逐单元格
                for (let ri = dataStartRow; ri <= dataEndRow; ri++) {
                  try { (sheet.getRange(ri, ci, 1, 1) as unknown as { setBackground?: (c: string) => unknown }).setBackground?.(bg) } catch {}
                }
              }
            }
          }
        } catch { /* ignore */ }
        try {
          const freezer = sheet as unknown as { setFrozenRows?: (n: number) => unknown; setFrozen?: (o: { ySplit?: number; xSplit?: number }) => unknown }
          freezer.setFrozenRows?.(3)
          freezer.setFrozen?.({ ySplit: 3, xSplit: 0 })
        } catch { /* ignore freeze errors */ }
        // 3D matrix 维度列（dim1/dim2）高亮为蓝色
        try {
          const dimColIdxs = colMeta
            .filter((m) => m.is_dim)
            .map((m) => cols.indexOf(m.name))
            .filter((ci) => ci >= 0)
          for (const ci of dimColIdxs) {
            const headerColRange = sheet.getRange(0, ci, 3, 1)
            const hs = headerColRange as unknown as { setBackgroundColor?: (c: string) => void; setBackground?: (c: string) => void }
            hs.setBackgroundColor?.('#e3f2fd')
            hs.setBackground?.('#e3f2fd')
            if (rowsArr.length > 0) {
              const dataColRange = sheet.getRange(3, ci, rowsArr.length, 1)
              const ds = dataColRange as unknown as { setBackgroundColor?: (c: string) => void; setBackground?: (c: string) => void }
              ds.setBackgroundColor?.('#f1f8ff')
              ds.setBackground?.('#f1f8ff')
            }
          }
        } catch { /* ignore dim styling errors */ }
        // 公式模板列（row_template）高亮 + 自动检测含 @col 的单元格
        try {
          const rowPatn = /(?<!@)@(?!@)[\u4e00-\u9fffA-Za-z_]/
          // 已注册的 row_template 公式列 → 列背景色
          for (const [colName, fi] of Object.entries(formulas)) {
            if (fi.type !== 'row_template') continue
            const ci = cols.indexOf(colName)
            if (ci < 0 || rowsArr.length === 0) continue
            const colRange = sheet.getRange(3, ci, rowsArr.length, 1)
            const s = colRange as unknown as { setBackgroundColor?: (c: string) => void; setBackground?: (c: string) => void }
            s.setBackgroundColor?.('#fff9e6')
            s.setBackground?.('#fff9e6')
          }
          // 未注册但含公式文本的列（如 calc_expr）→ 按列整体标黄
          const colsWithFormulaCells = new Set<number>()
          for (const row of rowsArr) {
            for (let ci = 0; ci < cols.length; ci++) {
              if (colsWithFormulaCells.has(ci)) continue
              const v = row[cols[ci]]
              if (typeof v === 'string' && rowPatn.test(v)) colsWithFormulaCells.add(ci)
            }
          }
          for (const ci of colsWithFormulaCells) {
            if (formulas[cols[ci]]) continue // 已注册的已处理
            if (rowsArr.length === 0) continue
            const colRange = sheet.getRange(3, ci, rowsArr.length, 1)
            const s = colRange as unknown as { setBackgroundColor?: (c: string) => void; setBackground?: (c: string) => void }
            s.setBackgroundColor?.('#fff9e6')
            s.setBackground?.('#fff9e6')
          }
        } catch { /* ignore formula styling errors */ }
      } finally {
        suppressEditRef.current = false
      }
      loadedSheetsRef.current.add(tableName)
    },
    [],
  )

  useEffect(() => {
    if (selected === '__constants__') {
      void loadAllConstants()
    }
    // 加载暴露参数（仅 gameplay_landing_tables.* 步骤）
    if (selected && selected.startsWith('gameplay_landing_tables.')) {
      void loadExposedParams(selected)
    } else {
      setExposedParams([])
    }
  }, [selected, loadAllConstants, loadExposedParams])

  useEffect(() => {
    if (!selected || selected === '__constants__') {
      setRows([])
      setActiveCols([])
      setActiveColMeta([])
      setActiveDisplayName('')
      setTableReadmeDraft('')
      setValidationRulesDraft('')
      setColumnFormulas({})
      setFormulaBarCol(null)
      setFormulaBarText('')
      setFormulaBarDirty(false)
      setSelectedMatrixMeta(null)
      activeTableRef.current = null
      return
    }
    // 查找表是否 matrix
    const tableInfo = tables.find((t) => t.table_name === selected)
    const isMatrix = Boolean(tableInfo?.is_matrix)
    const is3DMatrix = Boolean(tableInfo?.is_3d_matrix)

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
          column_formulas?: Record<string, FormulaInfo | string> | null
          schema?: { columns?: { name?: string; sql_type?: string; display_name?: string; dtype?: string; number_format?: string }[] }
          display_name?: string
          related_constants?: Array<{ name_en: string; name_zh: string; value: unknown; brief?: string; scope_table?: string | null }>
          matrix_meta_json?: string | null
          table_kind?: string
          column_kinds?: Record<string, string>
        }
        if (cancelled) return

        // 解析 matrix_meta_json（传统 2D matrix 和 3d_matrix 均处理）
        let parsedMM: Record<string, unknown> | null = null
        try {
          parsedMM = typeof desc.matrix_meta_json === 'string'
            ? JSON.parse(desc.matrix_meta_json)
            : (desc.matrix_meta_json != null ? desc.matrix_meta_json as Record<string, unknown> : null)
        } catch { parsedMM = null }
        const is3DMatrixFromMeta = parsedMM?.kind === '3d_matrix'
        if (isMatrix || is3DMatrix || is3DMatrixFromMeta) {
          setSelectedMatrixMeta(parsedMM)
        } else {
          setSelectedMatrixMeta(null)
        }

        const rawRows = Array.isArray(r.rows) ? r.rows : []
        const normalized = rawRows.filter(
          (row): row is Record<string, unknown> =>
            row != null && typeof row === 'object' && !Array.isArray(row),
        )
        const cf: FormulaMap = {}
        if (desc.column_formulas && typeof desc.column_formulas === 'object' && !Array.isArray(desc.column_formulas)) {
          for (const [k, v] of Object.entries(desc.column_formulas)) {
            if (v && typeof v === 'object' && 'formula' in v) {
              cf[k] = v as FormulaInfo
            } else if (typeof v === 'string') {
              cf[k] = { formula: v, type: 'sql' }
            }
          }
        }
        const schemaCols = Array.isArray(desc.schema?.columns) ? desc.schema!.columns! : []

        // 3d_matrix 维度列标记：从 matrix_meta_json 提取 dim1/dim2 col_name
        const dim3dCols = new Set<string>()
        if (parsedMM && (parsedMM as { kind?: string }).kind === '3d_matrix') {
          const mm3d = parsedMM as { dim1?: { col_name?: string }; dim2?: { col_name?: string } }
          if (mm3d.dim1?.col_name) dim3dCols.add(mm3d.dim1.col_name)
          if (mm3d.dim2?.col_name) dim3dCols.add(mm3d.dim2.col_name)
        }

        const colMeta: ColumnMeta[] = schemaCols.map((c) => ({
          name: String(c?.name ?? ''),
          sql_type: c?.sql_type,
          display_name: c?.display_name || '',
          dtype: c?.dtype || '',
          number_format: c?.number_format || '',
          is_dim: dim3dCols.has(String(c?.name ?? '')),
        })).filter((m) => m.name)
        let cols: string[] = []
        if (normalized.length > 0) cols = Object.keys(normalized[0])
        else if (colMeta.length) cols = colMeta.map((m) => m.name)
        const displayName = desc.display_name || ''
        setRows(normalized)
        setActiveCols(cols)
        setActiveColMeta(colMeta)
        setActiveDisplayName(displayName)
        setTableReadmeDraft(m.readme || '')
        const vr = desc.validation_rules && typeof desc.validation_rules === 'object' ? desc.validation_rules : { rules: [] }
        setValidationRulesDraft(JSON.stringify(vr, null, 2))
        setColumnFormulas(cf)
        setRelatedConstants(Array.isArray(desc.related_constants) ? desc.related_constants : [])
        setColumnKinds(desc.column_kinds || {})
        setActiveTableKind(desc.table_kind || '')

        // 写入 Univer 并切换到该 sheet（2D/3D matrix 专用视图不需要写入 Univer）
        if (!isMatrix && !is3DMatrix && !is3DMatrixFromMeta) {
          populateSheet(selected, normalized, cols, cf, colMeta, displayName, desc.column_kinds || {}, desc.table_kind || '')
          activeTableRef.current = selected
          const wb = workbookRef.current
          if (wb) {
            try {
              const sheetTitle = displayName ? `${displayName}（${selected}）` : selected
              const sh = wb.getSheetByName(sheetTitle) ?? wb.getSheetByName(selected)
              if (sh) wb.setActiveSheet(sh)
            } catch {
              /* ignore */
            }
          }
        } else {
          activeTableRef.current = null
        }
      } catch (e) {
        if (!cancelled) setErr(String(e))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [selected, headers, populateSheet, tables])

  /** 重新拉取并刷新当前活动 sheet 的数据（写失败时回退用） */
  const reloadActiveTable = useCallback(async () => {
    if (!selected) return
    try {
      const r = (await apiFetch(`/data/tables/${encodeURIComponent(selected)}/rows?limit=200`, {
        headers,
      })) as { rows?: unknown }
      const rawRows = Array.isArray(r.rows) ? r.rows : []
      const normalized = rawRows.filter(
        (row): row is Record<string, unknown> =>
          row != null && typeof row === 'object' && !Array.isArray(row),
      )
      const cols = normalized.length > 0 ? Object.keys(normalized[0]) : tableColsCacheRef.current.get(selected) || []
      const formulas = tableFormulasCacheRef.current.get(selected) || {}
      const colMeta = tableColMetaCacheRef.current.get(selected) || []
      setRows(normalized)
      setActiveCols(cols)
      populateSheet(selected, normalized, cols, formulas, colMeta, activeDisplayName, columnKinds, activeTableKind)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }, [selected, headers, populateSheet, activeDisplayName])

  /** B2: 用户在 Univer 中编辑后回写后端（user_manual 标记） */
  const writeCellManual = useCallback(
    async (
      tableName: string,
      rowId: string,
      colName: string,
      value: unknown,
    ) => {
      try {
        // 后端 /data/cells/write 仅接受 ai_generated/algorithm_derived/formula_computed
        // 因此先写值，再调用 /data/cells/mark-manual 把 provenance 翻为 user_manual。
        await apiFetch('/data/cells/write', {
          method: 'POST',
          headers,
          body: JSON.stringify({
            table_name: tableName,
            updates: [{ row_id: rowId, column: colName, value }],
            source_tag: 'algorithm_derived',
          }),
        })
        await apiFetch('/data/cells/mark-manual', {
          method: 'POST',
          headers,
          body: JSON.stringify({ table_name: tableName, row_id: rowId, column: colName }),
        })
        // 同步本地缓存
        const cache = tableRowsCacheRef.current.get(tableName)
        if (cache) {
          for (const r of cache) {
            if (String(r.row_id) === rowId) {
              r[colName] = value as never
              break
            }
          }
        }
        // 如果当前表有 row 类型公式列，重算后刷新——但用防抖避免连续编辑触发整表重渲染卡顿
        const tableFormulas = tableFormulasCacheRef.current.get(tableName) || {}
        const hasRowFormulas = Object.values(tableFormulas).some((fi) => fi.type === 'row')
        if (hasRowFormulas) {
          pendingRecalcTablesRef.current.add(tableName)
          if (recalcDebounceRef.current) clearTimeout(recalcDebounceRef.current)
          recalcDebounceRef.current = setTimeout(() => {
            const tables = Array.from(pendingRecalcTablesRef.current)
            pendingRecalcTablesRef.current.clear()
            recalcDebounceRef.current = null
            void (async () => {
              for (const tn of tables) {
                try {
                  await apiFetch(
                    `/compute/column-formula/recalculate-table?table_name=${encodeURIComponent(tn)}`,
                    { method: 'POST', headers },
                  )
                  const r2 = (await apiFetch(`/data/tables/${encodeURIComponent(tn)}/rows?limit=200`, { headers })) as { rows?: unknown }
                  const rawRows2 = Array.isArray(r2.rows) ? r2.rows : []
                  const normalized2 = rawRows2.filter((row): row is Record<string, unknown> => row != null && typeof row === 'object' && !Array.isArray(row))
                  const cols2 = normalized2.length > 0 ? Object.keys(normalized2[0]) : tableColsCacheRef.current.get(tn) || []
                  const colMeta2 = tableColMetaCacheRef.current.get(tn) || []
                  const tf = tableFormulasCacheRef.current.get(tn) || {}
                  const dn2 = activeDisplayName
                  tableRowsCacheRef.current.set(tn, normalized2)
                  if (tn === activeTableRef.current) {
                    setRows(normalized2)
                    populateSheet(tn, normalized2, cols2, tf, colMeta2, dn2, columnKinds, activeTableKind)
                  }
                } catch { /* 重算失败不影响写入 */ }
              }
            })()
          }, 500)
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e)
        setErr(`单元格保存失败：${msg}`)
        if (typeof window !== 'undefined') {
          window.alert(`单元格保存失败：${msg}`)
        }
        await reloadActiveTable()
      }
    },
    [headers, reloadActiveTable, populateSheet, activeDisplayName],
  )

  useEffect(() => {
    writeCellManualRef.current = writeCellManual
  }, [writeCellManual])

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
    const msg = agentInput
    setAgentInput('')
    setAgentStream('')
    setAgentLivePhase('')

    const nextId = () => ++agentEntryIdRef.current
    const pushEntry = (entry: WbAgentInput) =>
      setAgentEntries((prev) => [...prev, { ...entry, id: nextId() } as WbAgentEntry])

    pushEntry({ kind: 'user', text: msg })

    // 按阶段累积流式文本（done 时归档）
    const phaseTextBuf: Record<string, string> = {}
    let currentPhase = ''

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
            const raw = JSON.parse(line) as Record<string, unknown>
            const phase = String(raw.phase ?? '')
            const type = String(raw.type ?? '')

            // 追踪当前阶段
            if (phase && phase !== currentPhase) {
              currentPhase = phase
              setAgentLivePhase(phase)
            }

            if (type === 'token' && raw.text) {
              const text = String(raw.text)
              phaseTextBuf[phase] = (phaseTextBuf[phase] ?? '') + text
              setAgentStream((s) => s + text)
            } else if (type === 'log' && raw.message) {
              pushEntry({ kind: 'log', phase, text: String(raw.message) })
            } else if (type === 'tool_call') {
              pushEntry({ kind: 'tool_call', phase, name: String(raw.name ?? ''), args: String(raw.arguments ?? '') })
            } else if (type === 'tool_result') {
              pushEntry({ kind: 'tool_result', phase, name: String(raw.name ?? ''), preview: String(raw.preview ?? '') })
            } else if (type === 'error') {
              pushEntry({ kind: 'error', text: String(raw.message ?? raw.error ?? '未知错误') })
              setAgentStream('')
            } else if (type === 'done') {
              // 把各阶段流式文本归档为条目
              const phaseOrder = ['route', 'design', 'review', 'execute']
              for (const p of phaseOrder) {
                if (phaseTextBuf[p]) {
                  pushEntry({ kind: 'phase_text', phase: p, text: phaseTextBuf[p] })
                }
              }
              // 处理未知阶段
              for (const [p, t] of Object.entries(phaseTextBuf)) {
                if (!phaseOrder.includes(p) && t) pushEntry({ kind: 'phase_text', phase: p, text: t })
              }
              setAgentStream('')
              setAgentLivePhase('')
            }
          } catch {
            /* ignore parse error */
          }
        }
      }
    } catch (x) {
      pushEntry({ kind: 'error', text: x instanceof Error ? x.message : String(x) })
      setAgentStream('')
    } finally {
      setAgentBusy(false)
      setAgentLivePhase('')
      // 滚动到底
      requestAnimationFrame(() => agentLogEndRef.current?.scrollIntoView({ behavior: 'smooth' }))
    }
  }

  function clearAgentConversation() {
    setAgentEntries([])
    setAgentStream('')
    setAgentLivePhase('')
  }

  const formulaCols = useMemo(
    () => activeCols.filter((c) => columnFormulas[c]),
    [activeCols, columnFormulas],
  )

  const formulaTypeLabel = (type: string) => {
    if (type === 'row') return '行公式'
    if (type === 'row_template') return '运行时模板'
    return 'SQL公式'
  }

  function translateFormulaDisplay(
    text: string,
    colMetaArr: ColumnMeta[],
    constMap: Map<string, { name_zh?: string }>,
    tableDispMap: Map<string, string>,
  ): string {
    if (!text) return text
    let out = text
    const curTable = selected || ''
    // 替换 @table[col] / @@table[col] / @T[col] / @this[col]
    out = out.replace(/(@{1,2})(T|this|[\w]+)\[(\w+)\]/g, (_, at, tname, cname) => {
      const isSelf = tname === 'T' || tname === 'this' || tname === curTable
      // 本表引用 → 省略表名，仅保留 @[列中文] 或 @@[列中文]
      if (isSelf) {
        const cm = colMetaArr.find((m) => m.name === cname)
        const cd = cm?.display_name || cname
        return `${at}[${cd}]`
      }
      // 外部表 → @表中文[列英文]
      const td = tableDispMap.get(tname) || tname
      return `${at}${td}[${cname}]`
    })
    // 替换 @col → @显示名（裸引用，无表名前缀）
    for (const cm of colMetaArr) {
      if (cm.display_name && cm.name && out.includes(`@${cm.name}`)) {
        out = out.replace(new RegExp(`@${cm.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![\\w])`, 'g'), `@${cm.display_name}`)
      }
    }
    // 替换 ${const} → ${常量中文名}
    out = out.replace(/\$\{(\w+)\}/g, (_, name) => {
      const c = constMap.get(name)
      return c?.name_zh ? `\${${c.name_zh}}` : `\${${name}}`
    })
    return out
  }

  function parseFabRefs(text: string): { colRefs: Set<string>; constRefs: string[] } {
    const colRefs = new Set<string>()
    const constRefs: string[] = []
    const seen = new Set<string>()
    if (!text) return { colRefs, constRefs }
    const colRe = /@(?!@)\w+/g
    let m: RegExpExecArray | null
    while ((m = colRe.exec(text)) !== null) colRefs.add(m[0].slice(1))
    const constRe = /\$\{(\w+)\}/g
    while ((m = constRe.exec(text)) !== null) {
      if (!seen.has(m[1])) { seen.add(m[1]); constRefs.push(m[1]) }
    }
    return { colRefs, constRefs }
  }

  async function saveFabConstant(nameEn: string) {
    if (!fabConstEditVal.trim()) return
    setFabConstSaving(true)
    setErr(null)
    try {
      const numVal = Number(fabConstEditVal)
      await apiFetch(`/meta/constants/${encodeURIComponent(nameEn)}`, {
        method: 'PATCH',
        headers,
        body: JSON.stringify(isNaN(numVal) ? { value: fabConstEditVal } : { value: numVal }),
      })
      setFabConstEditName(null)
      setFabConstEditVal('')
      void loadAllConstants()
      // 如果当前正在编辑公式列，立即重算该列
      if (formulaBarCol && columnFormulas[formulaBarCol]) {
        await apiFetch(
          `/compute/column-formula/recalculate?table_name=${encodeURIComponent(selected!)}&column_name=${encodeURIComponent(formulaBarCol)}`,
          { method: 'POST', headers },
        )
      }
      // 重算当前表所有公式列 + 级联下游
      if (selected && selected !== '__constants__') {
        await apiFetch(
          `/compute/column-formula/recalculate-table?table_name=${encodeURIComponent(selected)}`,
          { method: 'POST', headers },
        )
        // 级联重算每个公式列的下游
        for (const col of activeCols) {
          if (columnFormulas[col]) {
            await apiFetch(
              `/compute/recalculate-downstream?table_name=${encodeURIComponent(selected)}&column_name=${encodeURIComponent(col)}`,
              { method: 'POST', headers },
            ).catch(() => {})
          }
        }
      }
      await reloadActiveTable()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setFabConstSaving(false)
    }
  }

  async function saveColumnFormula() {
    if (!selected || !formulaBarCol || !formulaBarText.trim()) return
    setFormulaBarSaving(true)
    setErr(null)
    try {
      await apiFetch('/compute/column-formula', {
        method: 'PUT',
        headers,
        body: JSON.stringify({ table_name: selected, column_name: formulaBarCol, formula: formulaBarText.trim() }),
      })
      setFormulaBarDirty(false)
      // 刷新公式与表格数据
      const desc = (await apiFetch(`/data/tables/${encodeURIComponent(selected)}`, { headers })) as {
        column_formulas?: Record<string, FormulaInfo | string>
      }
      const cf: FormulaMap = {}
      if (desc.column_formulas) {
        for (const [k, v] of Object.entries(desc.column_formulas)) {
          if (v && typeof v === 'object' && 'formula' in v) cf[k] = v as FormulaInfo
          else if (typeof v === 'string') cf[k] = { formula: v, type: 'sql' }
        }
      }
      setColumnFormulas(cf)
      tableFormulasCacheRef.current.set(selected, cf)
      await reloadActiveTable()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
    setFormulaBarSaving(false)
  }

  async function deleteColumnFormula() {
    if (!selected || !formulaBarCol) return
    setErr(null)
    try {
      await apiFetch(
        `/compute/column-formula?table_name=${encodeURIComponent(selected)}&column_name=${encodeURIComponent(formulaBarCol)}`,
        { method: 'DELETE', headers },
      )
      setFormulaBarText('')
      setFormulaBarDirty(false)
      const newCf = { ...columnFormulas }
      delete newCf[formulaBarCol]
      setColumnFormulas(newCf)
      tableFormulasCacheRef.current.set(selected, newCf)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function recalculateColumnFormula() {
    if (!selected || !formulaBarCol) return
    setErr(null)
    try {
      await apiFetch(
        `/compute/column-formula/recalculate?table_name=${encodeURIComponent(selected)}&column_name=${encodeURIComponent(formulaBarCol)}`,
        { method: 'POST', headers },
      )
      await reloadActiveTable()
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  const agentPlaceholder = useMemo(() => {
    if (agentMode === 'init' && pipeline?.next_expected_step) {
      return `例如：完成「${pipelineStepLabel(pipeline.next_expected_step)}」；相关默认 SKILL 会自动暴露`
    }
    return '自然语言指令（需 DASHSCOPE_API_KEY）'
  }, [agentMode, pipeline?.next_expected_step])

  return (
    <div className="workbench">
      <header className="wb-top">
        <Link to="/projects" className="link-btn">
          项目列表
        </Link>
        <Link
          to={`/project-setup/${pid}`}
          className="link-btn"
          style={pipeline && !pipeline.finished ? { background: 'rgba(255,180,0,.25)' } : undefined}
          title="查看 / 继续 Agent 初始化进程"
        >
          ⚙ Agent 进程{pipeline && !pipeline.finished ? '（未完成）' : ''}
        </Link>
        <Link to={`/skills/${pid}`} className="link-btn">
          提示词
        </Link>
        <button
          type="button"
          className="link-btn"
          onClick={() => window.open(
            `/agent-test?project=${pid}`,
            'agent_monitor',
            'width=1280,height=860,resizable=yes,scrollbars=yes'
          )}
        >
          AGENT TEST ↗
        </button>
        <span className="muted">项目 #{pid}{readOnly ? '（只读）' : ''}</span>
        <span className="wb-model-selector">
          <label htmlFor="ai-model-sel" style={{ fontSize: '0.78rem', opacity: 0.7 }}>模型：</label>
          <select
            id="ai-model-sel"
            value={aiModel}
            disabled={modelSwitching || aiModelGroups.length === 0}
            onChange={(e) => void switchAiModel(e.target.value)}
            style={{ fontSize: '0.78rem', maxWidth: 180 }}
          >
            {aiModel && !aiModelGroups.some((g) => g.models.includes(aiModel)) && (
              <option value={aiModel}>{aiModel}</option>
            )}
            {aiModelGroups.map((g) => (
              <optgroup key={g.label} label={g.label}>
                {g.models.map((m) => (
                  <option key={m} value={m}>{m}</option>
                ))}
              </optgroup>
            ))}
          </select>
          {modelSwitching && <span style={{ marginLeft: 4, fontSize: '0.7rem' }}>切换中…</span>}
        </span>
        <label style={{ fontSize: '0.78rem', cursor: 'pointer', userSelect: 'none', marginLeft: 12, display: 'flex', alignItems: 'center', gap: 4, color: '#fff', whiteSpace: 'nowrap', flexShrink: 0 }}>
          <input type="checkbox" checked={showEnNames} onChange={e => setShowEnNames(e.target.checked)} style={{ margin: 0 }} />
          英文名
        </label>
      </header>
      {err && <p className="err banner">{err}</p>}
      {validateReport && (validateReport.rule_summaries?.length ?? 0) > 0 && (() => {
        const allSummaries = validateReport.rule_summaries ?? []
        const failed = allSummaries.filter(
          (s) => !s.passed || (typeof s.violation_count === 'number' && s.violation_count > 0),
        )
        const showFailedOnly = failed.length > 0 && failed.length < allSummaries.length
        return (
          <details className="wb-rule-sum muted small" style={{ margin: '0 1rem 0.5rem' }} open={showValDetail}>
            <summary onClick={e => { e.preventDefault(); setShowValDetail(!showValDetail) }}>
              校验明细（共 {allSummaries.length} 条规则
              {failed.length > 0 ? `，未通过 ${failed.length} 条` : '，全部通过'}）
            </summary>
            <ul style={{ marginTop: '0.35rem' }}>
              {(showFailedOnly ? failed : allSummaries).map((s, i) => (
                <li key={i}>
                  {s.table}.{s.rule_id} [{s.type}] {s.passed ? '通过' : '未通过'}
                  {typeof s.violation_count === 'number' ? `（${s.violation_count} 条违反）` : ''}
                </li>
              ))}
            </ul>
          </details>
        )
      })()}

      <div className="wb-body">
        <aside className="wb-left">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <h3 style={{ margin: 0 }}>表</h3>
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
                  loadCalculators(),
                ]).catch((e) => setErr(String(e)))
              }}
            >
              刷新
            </button>
          </div>
          <ul>
            <li key="__filter__" style={{ padding: '4px 8px', display: 'flex', gap: 4 }}>
              {(['all', 'config', 'compute'] as const).map(k => (
                <button key={k}
                  type="button"
                  className={`btn tiny ${kindFilter === k ? 'sel' : ''}`}
                  style={{ fontSize: 11, padding: '2px 6px' }}
                  onClick={() => setKindFilter(k)}>
                  {k === 'all' ? '全部' : k === 'config' ? '配置表' : '计算表'}
                </button>
              ))}
            </li>
            <li key="__constants__">
              <button
                type="button"
                className={selected === '__constants__' ? 'sel' : undefined}
                onClick={() => setSelected('__constants__')}
                title="查看项目内所有常量（按标签分组）"
              >
                <span className="tbl-name">📐 常量</span>
                <small className="tbl-purpose">全局/分表常数清单</small>
              </button>
            </li>
            {(() => {
              const filteredTables = tables.filter(t => {
                if (kindFilter === 'all') return true
                const tk = t.table_kind || ''
                if (kindFilter === 'config') return tk === 'config' || tk === 'mixed'
                if (kindFilter === 'compute') return tk === 'compute' || tk === 'mixed'
                return true
              })
              const groups: Record<string, TableInfo[]> = {}
              filteredTables.forEach((t) => {
                const dir = t.directory || '（未分组）'
                ;(groups[dir] ||= []).push(t)
              })
              const dirNames = Object.keys(groups).sort()
              return dirNames.map((dir) => {
                const collapsed = collapsedDirs.has(dir)
                const isDragTarget = dragOverDir === dir
                return (
                  <li
                    key={`__dir__${dir}`}
                    className={`dir-group${isDragTarget ? ' dir-drag-over' : ''}`}
                    onDragOver={(e: DragEvent<HTMLLIElement>) => { e.preventDefault(); setDragOverDir(dir) }}
                    onDragLeave={() => setDragOverDir(null)}
                    onDrop={(e: DragEvent<HTMLLIElement>) => {
                      e.preventDefault()
                      setDragOverDir(null)
                      if (draggingTable) void updateTableDirectory(draggingTable, dir === '（未分组）' ? '' : dir)
                    }}
                  >
                    <button
                      type="button"
                      className={`dir-name${collapsed ? '' : ' open'}`}
                      onClick={() => setCollapsedDirs((prev) => {
                        const next = new Set(prev)
                        if (next.has(dir)) next.delete(dir)
                        else next.add(dir)
                        return next
                      })}
                      title={collapsed ? '展开' : '折叠'}
                    >
                      <span className="dir-arrow">▶</span>
                      <span>{dir}</span>
                      <small className="muted">{groups[dir].length}</small>
                    </button>
                    {!collapsed && (
                      <ul className="dir-children">
                        {groups[dir].map((t) => {
                          const warn = validateReport?.per_table?.[t.table_name] === 'warn'
                          const cls = [selected === t.table_name ? 'sel' : '', warn ? 'row-warn' : ''].filter(Boolean).join(' ')
                          const tag = t.is_matrix ? '⇆ ' : ''
                          return (
                            <li
                              key={t.table_name}
                              draggable
                              onDragStart={() => setDraggingTable(t.table_name)}
                              onDragEnd={() => { setDraggingTable(null); setDragOverDir(null) }}
                              title="拖拽到其他目录"
                            >
                              <button type="button" className={cls || undefined} onClick={() => setSelected(t.table_name)}>
                                <span className="tbl-name">{tag}{t.display_name || t.table_name}</span>
                                {t.display_name ? (
                                  <small className="tbl-en" title={t.table_name}>{t.table_name}</small>
                                ) : null}
                                {t.purpose ? (
                                  <small className="tbl-purpose" title={t.purpose}>
                                    {t.purpose}
                                  </small>
                                ) : null}
                                {t.table_kind ? (
                                  <small className="tbl-kind" title={t.table_kind === 'mixed' ? '混合表（配置+计算）' : t.table_kind === 'config' ? '配置表' : '计算表'}>
                                    {t.table_kind === 'mixed' ? '混合' : t.table_kind === 'config' ? '配置' : '计算'}
                                  </small>
                                ) : null}
                                <small>{t.validation_status}</small>
                              </button>
                            </li>
                          )
                        })}
                      </ul>
                    )}
                  </li>
                )
              })
            })()}
          </ul>
          {pipeline && (
            <div className="pipe-box">
              <h4>流水线</h4>
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
          {selected === '__constants__' ? (
            <ConstantsPanel
              constants={allConstants}
              tags={allConstTags}
              canWrite={canWrite}
              headers={headers}
              onRefresh={() => void loadAllConstants()}
            />
          ) : selected && selectedIsMatrix && selectedMatrixMeta != null ? (
            <>
              {/* 暴露参数 banner（仅 gameplay_landing_tables.* 子步） */}
              {exposedParams.length > 0 && (
                <div className="exposed-params-banner">
                  <strong>📎 本子系统继承的参数 {exposedParams.length} 项：</strong>{' '}
                  {exposedParams.map((p, i) => (
                    <span key={i} className="exposed-param-chip" title={p.brief}>
                      <code>{p.key}</code>
                      {p.value != null && <span className="muted"> = {JSON.stringify(p.value)}</span>}
                      <span className="muted small"> ({p.owner_step})</span>
                    </span>
                  ))}
                </div>
              )}
              <MatrixEditor
                tableName={selected}
                matrixMeta={selectedMatrixMeta}
                headers={headers}
                glossary={glossary}
                canWrite={!readOnly}
                columnKinds={columnKinds}
                tableKind={activeTableKind}
                showEnNames={showEnNames}
              />
            </>
          ) : selected && selectedIs3DMatrix ? (
            <>
              {exposedParams.length > 0 && (
                <div className="exposed-params-banner">
                  <strong>📎 本子系统继承的参数 {exposedParams.length} 项：</strong>{' '}
                  {exposedParams.map((p, i) => (
                    <span key={i} className="exposed-param-chip" title={p.brief}>
                      <code>{p.key}</code>
                      {p.value != null && <span className="muted"> = {JSON.stringify(p.value)}</span>}
                      <span className="muted small"> ({p.owner_step})</span>
                    </span>
                  ))}
                </div>
              )}
              <ThreeDimTableEditor
                key={selected}
                tableName={selected}
                headers={headers}
                glossary={glossary}
                allConstants={allConstants}
                canRecalculate={!readOnly}
                canWrite={!readOnly}
                onConstantsChanged={() => void loadAllConstants()}
                columnKinds={columnKinds}
                tableKind={activeTableKind}
                showEnNames={showEnNames}
                tableDisplayMap={tableDispMap}
              />
            </>
          ) : selected !== '__constants__' && !selectedIsMatrix ? (
          <>
          {/* 暴露参数 banner */}
          {exposedParams.length > 0 && (
            <div className="exposed-params-banner">
              <strong>📎 本子系统继承的参数 {exposedParams.length} 项：</strong>{' '}
              {exposedParams.map((p, i) => (
                <span key={i} className="exposed-param-chip" title={p.brief}>
                  <code>{p.key}</code>
                  {p.value != null && <span className="muted"> = {JSON.stringify(p.value)}</span>}
                </span>
              ))}
            </div>
          )}
          <h3>{selected || '未选择表'}</h3>
          <div className="wb-formula-bar">
            <span className="wb-formula-bar-label">
              {formulaBarCol ? (
                <>
                  <strong>fx</strong>:{' '}
                  {showEnNames ? formulaBarCol : ((() => {
                    const meta = tableColMetaCacheRef.current.get(selected ?? '') || []
                    const cm = meta.find((m: ColumnMeta) => m.name === formulaBarCol)
                    return cm?.display_name || formulaBarCol
                  })())}
                  {columnFormulas[formulaBarCol] && (
                    <span className={`wb-formula-type-badge wb-ftype-${columnFormulas[formulaBarCol].type}`}>
                      {formulaTypeLabel(columnFormulas[formulaBarCol].type)}
                    </span>
                  )}
                </>
              ) : (
                <span className="muted">点击单元格列以选择</span>
              )}
            </span>
            {formulaBarCol && (
              <>
                {!showEnNames ? (
                  <span className="wb-formula-translated" style={{ fontSize: 13, fontFamily: 'monospace', fontWeight: 600, color: '#333', padding: '4px 0' }}>
                    {(() => {
                      const meta = tableColMetaCacheRef.current.get(selected ?? '') || []
                      const constMap = new Map(allConstants.map((c: any) => [c.name_en, c]))
                      return translateFormulaDisplay(formulaBarText || columnFormulas[formulaBarCol]?.formula || '', meta, constMap, tableDispMap)
                    })()}
                  </span>
                ) : (
                  <>
                    <input
                      className="wb-formula-bar-input"
                      value={formulaBarText}
                      onChange={(e) => { setFormulaBarText(e.target.value); setFormulaBarDirty(true) }}
                      onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); void saveColumnFormula() } }}
                      placeholder="输入公式，例如: @delta_val * (1 - @blend_weight)"
                      disabled={!canWrite || readOnly}
                      spellCheck={false}
                    />
                    {canWrite && !readOnly && (
                      <>
                    <button
                      type="button"
                      className="btn tiny primary"
                      onClick={() => void saveColumnFormula()}
                      disabled={!formulaBarDirty || formulaBarSaving || !formulaBarText.trim()}
                      title="保存公式（Enter）"
                    >
                      {formulaBarSaving ? '…' : '保存'}
                    </button>
                    {columnFormulas[formulaBarCol]?.type === 'row' && (
                      <button
                        type="button"
                        className="btn tiny"
                        onClick={() => void recalculateColumnFormula()}
                        title="重新计算此列所有行"
                      >
                        重算
                      </button>
                    )}
                    {columnFormulas[formulaBarCol] && (
                      <button
                        type="button"
                        className="btn tiny danger"
                        onClick={() => void deleteColumnFormula()}
                        title="删除列公式"
                      >
                        删除
                      </button>
                    )}
                  </>
                )}
              </>
            )}
            </>
          )}
          {formulaCols.length > 0 && !formulaBarCol && (
              <span className="muted small" style={{ marginLeft: '0.5rem' }}>
                {formulaCols.length} 个公式列
              </span>
            )}
          </div>
          {formulaBarCol && formulaBarText && (() => {
            const refs = parseFabRefs(formulaBarText)
            const constMap = new Map(allConstants.map((c) => [c.name_en, c]))
            return (
              <>
                {refs.colRefs.size > 0 && (
                  <div className="wb-formula-ref-info">
                    本表引用列：{[...refs.colRefs].map((c) => {
                      const meta = tableColMetaCacheRef.current.get(selected ?? '') || []
                      const colMeta = meta.find((m: ColumnMeta) => m.name === c)
                      const label = showEnNames ? c : (colMeta?.display_name || c)
                      return <code key={c} style={{margin:'0 3px'}} title={c}>{label}</code>
                    })}
                  </div>
                )}
                {refs.constRefs.length > 0 && (
                  <div className="wb-formula-const-chips">
                    <span className="muted small" style={{ marginRight: 4 }}>常量引用:</span>
                    {refs.constRefs.map((name) => {
                      const c = constMap.get(name)
                      const isEditing = fabConstEditName === name
                      return (
                        <span key={name} style={{ display: 'inline-flex' }}>
                          {isEditing ? (
                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '.2rem', background: '#fff', border: '1px solid #1976d2', borderRadius: 6, padding: '.15rem .4rem' }}>
                               <code style={{ fontSize: '.72rem', fontWeight: 600, color: '#1565c0' }}>{showEnNames ? name : (c?.name_zh || name)}</code>
                              <input
                                style={{ fontSize: '.75rem', fontFamily: 'monospace', padding: '.1rem .3rem', border: '1px solid #90caf9', borderRadius: 3, width: 80, outline: 'none' }}
                                value={fabConstEditVal}
                                onChange={(e) => setFabConstEditVal(e.target.value)}
                                onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); void saveFabConstant(name) } }}
                                autoFocus
                              />
                              <button type="button" className="btn tiny primary" onClick={() => void saveFabConstant(name)} disabled={fabConstSaving}>✓</button>
                              <button type="button" className="btn tiny" onClick={() => { setFabConstEditName(null); setFabConstEditVal('') }}>✕</button>
                            </span>
                          ) : (
                            <button
                              type="button"
                              style={{ display: 'inline-flex', alignItems: 'center', gap: '.25rem', border: '1px solid #b2dfdb', background: '#e0f2f1', borderRadius: 12, padding: '.1rem .5rem', fontSize: '.72rem', cursor: 'pointer', color: '#00695c', whiteSpace: 'nowrap' }}
                              onClick={() => { setFabConstEditName(name); setFabConstEditVal(c?.value != null ? String(c.value) : c?.formula || '') }}
                              title={c ? `当前值: ${c.value ?? c.formula ?? '未设置'}${c.brief ? ' — ' + c.brief : ''}` : '未找到常量'}
                            >
                               <code style={{ fontWeight: 600 }}>{showEnNames ? name : (c?.name_zh || name)}</code>
                              <span style={{ fontWeight: 600, color: '#004d40' }}>{c?.value != null ? String(c.value) : c?.formula || '?'}</span>
                            </button>
                          )}
                        </span>
                      )
                    })}
                  </div>
                )}
              </>
            )
          })()}
          </>
          ) : null}
          {/* wb-univer-host 始终挂载在 DOM，避免 Univer 宿主容器被卸载后引用断裂；
               在常量页 / 矩阵页时用 absolute+hidden 替代 display:none，保持容器始终有真实尺寸，
               避免 Univer 内部列宽计算因容器宽度为 0 而报 "column width < 0" */}
          <div
            className="wb-univer-host"
            ref={univerHostRef}
            style={(selected === '__constants__' || selectedIsMatrix || selectedIs3DMatrix) ? {
              position: 'absolute',
              visibility: 'hidden',
              pointerEvents: 'none',
              top: 0,
              left: 0,
              width: '100%',
              height: '100%',
            } : undefined}
          />
          {readOnly && selected !== '__constants__' && !selectedIsMatrix && !selectedIs3DMatrix && (
            <div className="wb-readonly-overlay" title="只读模式">
              🔒 只读模式（在 Agent 进程页中查看，请回到完整工作台编辑）
            </div>
          )}
        </section>

        <aside className="wb-right">
          <div className="wb-right-pane">
            <div className="readme-tabs readme-tab-btns">
              <button type="button" className={readmeTab === 'table' ? 'active' : ''} onClick={() => setReadmeTab('table')}>
                表 README
              </button>
              <button
                type="button"
                className={readmeTab === 'global' ? 'active' : ''}
                onClick={() => setReadmeTab('global')}
              >
                全局
              </button>
              <button
                type="button"
                className={readmeTab === 'calculators' ? 'active' : ''}
                onClick={() => setReadmeTab('calculators')}
              >
                🧮
              </button>
              <button
                type="button"
                className={readmeTab === 'design' ? 'active' : ''}
                onClick={() => { setReadmeTab('design'); void loadDesignHistory() }}
                title="AI 设计文档"
              >
                📄
              </button>
            </div>
            {(readmeTab === 'table' || readmeTab === 'global') && (
              <div className="readme-mode-row">
                <button type="button"
                  className={`btn tiny${readmeViewMode === 'preview' ? ' primary' : ''}`}
                  onClick={() => setReadmeViewMode('preview')}>预览</button>
                <button type="button"
                  className={`btn tiny${readmeViewMode === 'edit' ? ' primary' : ''}`}
                  onClick={() => setReadmeViewMode('edit')} disabled={!canWrite || readOnly}>编辑</button>
                {(!canWrite || readOnly) && <span className="muted small">（只读）</span>}
              </div>
            )}
            {readmeTab === 'table' && (
              <>
                {!selected && <p className="muted small">请在左侧选择一张表。</p>}
                {selected && readmeViewMode === 'preview' && (() => {
                  const glossaryMap = new Map(glossary.map((g) => [g.term_en, g]))
                  const constMap = new Map(allConstants.map((c: any) => [c.name_en, c]))
                  const hasGlossaryRefs = tableReadmeDraft.includes('$')
                  return (
                    <div className="markdown-preview">
                      {tableReadmeDraft.trim() ? (
                        hasGlossaryRefs ? (
                          <div>
                            {tableReadmeDraft.split('\n').map((line, i) => (
                              <p key={i}>{renderGlossaryText(line, glossaryMap, constMap)}</p>
                            ))}
                          </div>
                        ) : (
                          <ReactMarkdown>{tableReadmeDraft}</ReactMarkdown>
                        )
                      ) : <p className="muted small">（此表暂无 README）</p>}
                    </div>
                  )
                })()}
                {selected && readmeViewMode === 'edit' && (
                  <>
                    <AutoTextarea
                      className="readme-textarea"
                      maxRows={28}
                      markdown
                      value={tableReadmeDraft}
                      onChange={(e) => setTableReadmeDraft(e.target.value)}
                      disabled={!canWrite || readOnly}
                      spellCheck={false}
                    />
                    {canWrite && !readOnly && (
                      <div className="readme-save-row">
                        <button type="button" className="btn tiny primary" onClick={() => void saveTableReadme()}>
                          保存
                        </button>
                      </div>
                    )}
                  </>
                )}
              </>
            )}
            {readmeTab === 'global' && (
              <>
                {readmeViewMode === 'preview' && (() => {
                  const glossaryMap = new Map(glossary.map((g) => [g.term_en, g]))
                  const constMap = new Map(allConstants.map((c: any) => [c.name_en, c]))
                  const hasRefs = globalReadmeDraft.includes('$')
                  return (
                    <div className="markdown-preview">
                      {globalReadmeDraft.trim() ? (
                        hasRefs ? (
                          <div>
                            {globalReadmeDraft.split('\n').map((line, i) => (
                              <p key={i}>{renderGlossaryText(line, glossaryMap, constMap)}</p>
                            ))}
                          </div>
                        ) : (
                          <ReactMarkdown>{globalReadmeDraft}</ReactMarkdown>
                        )
                      ) : <p className="muted small">（暂无全局 README）</p>}
                    </div>
                  )
                })()}
                {readmeViewMode === 'edit' && (
                  <>
                    <AutoTextarea
                      className="readme-textarea"
                      maxRows={28}
                      markdown
                      value={globalReadmeDraft}
                      onChange={(e) => setGlobalReadmeDraft(e.target.value)}
                      disabled={!canWrite || readOnly}
                      spellCheck={false}
                    />
                    {canWrite && !readOnly && (
                      <div className="readme-save-row">
                        <button type="button" className="btn tiny primary" onClick={() => void saveGlobalReadme()}>
                          保存
                        </button>
                      </div>
                    )}
                  </>
                )}
              </>
            )}
            {readmeTab === 'calculators' && (
              <CalculatorsPanel
                calculators={calculators}
                headers={headers}
                onRefresh={() => void loadCalculators()}
              />
            )}
            {readmeTab === 'design' && (
              <div style={{ overflow: 'auto', height: '100%', padding: '0.25rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <h4 style={{ margin: 0 }}>📄 AI 设计文档</h4>
                  <button type="button" className="btn tiny" onClick={() => void loadDesignHistory()}>刷新</button>
                </div>
                {designHistory.length === 0 ? (
                  <p className="muted small">暂无设计文档。流水线步骤完成后会在此展示。</p>
                ) : (
                  <>
                    <div className="design-step-tabs">
                      {designHistory.map((e) => (
                        <button
                          key={e.step_id}
                          type="button"
                          className={`btn tiny${designStep === e.step_id ? ' primary' : ''}`}
                          onClick={() => setDesignStep(e.step_id)}
                          style={{ marginRight: 3, marginBottom: 3 }}
                        >
                          {pipelineStepLabel(e.step_id)}
                        </button>
                      ))}
                    </div>
                    {designStep && (() => {
                      const entry = designHistory.find((e) => e.step_id === designStep)
                      if (!entry) return null
                      return (
                        <div className="markdown-preview" style={{ marginTop: '0.5rem' }}>
                          {entry.design_text ? (
                            <ReactMarkdown>{entry.design_text}</ReactMarkdown>
                          ) : (
                            <p className="muted small">（此步骤暂无 design_text）</p>
                          )}
                        </div>
                      )
                    })()}
                  </>
                )}
              </div>
            )}

            <details className="wb-adv-section">
              <summary>校验规则 JSON</summary>
              {!selected && <p className="muted small">请选择表后编辑 rules。</p>}
              {selected && (
                <>
                  <p className="muted small">
                    支持 type: <code>not_null</code>、<code>min_max</code>、<code>regex</code>。
                  </p>
                  <textarea
                    className="readme-textarea"
                    value={validationRulesDraft}
                    onChange={(e) => setValidationRulesDraft(e.target.value)}
                    disabled={!canWrite || readOnly}
                    spellCheck={false}
                  />
                  {canWrite && !readOnly && (
                    <div className="readme-save-row">
                      <button type="button" className="btn tiny primary" onClick={() => void saveValidationRules()}>
                        保存
                      </button>
                    </div>
                  )}
                </>
              )}
            </details>
            <details className="wb-adv-section">
              <summary>相关常数（{relatedConstants.length}）</summary>
              {relatedConstants.length === 0 ? (
                <p className="muted small">暂无项目级 / 本表常数。可在 Agent 会话中通过 const_register 注册，或从 README 中识别 ${'${name}'} 引用。</p>
              ) : (
                <div className="wb-const-wrap">
                <table className="wb-const-table small">
                  <thead>
                    <tr>
                      <th style={{cursor:'pointer'}}
                        onClick={() => setShowConstEn(!showConstEn)}
                        title="点击切换中/英文">
                        {showConstEn ? 'name_en' : '中文'}
                      </th>
                      <th>value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {relatedConstants.map((c) => (
                      <InlineConstRow
                        key={c.name_en}
                        c={c}
                        showEn={showConstEn}
                        canWrite={canWrite}
                        headers={headers}
                        onRefresh={() => { if (selected) loadRelatedConstants(selected) }} />
                    ))}
                  </tbody>
                </table>
                </div>
              )}
              <p className="muted small">公式中可用 <code>${'${name_en}'}</code> 引用；执行时自动替换为数值。</p>
            </details>
            <details className="wb-adv-section">
              <summary>快照（{snapshots.length}）</summary>
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
                <pre className="wb-compare-pre">{compareText}</pre>
              )}
            </details>
          </div>
        </aside>
      </div>

      <footer className={`wb-agent${agentOpen ? '' : ' wb-agent--collapsed'}`}>
        {/* 面板头部：标题 + 阶段指示 + 操作按钮 */}
        <div className="wb-agent-header">
          <span className="wb-agent-title">
            🤖 Agent
            {agentLivePhase && (
              <span className={`agent-msg-phase ${wbPhaseCls(agentLivePhase)}`} style={{ marginLeft: '0.4rem', fontSize: '0.65rem' }}>
                {WB_PHASE_LABELS[agentLivePhase] ?? agentLivePhase}
              </span>
            )}
            {agentBusy && <span className="agent-busy-dot" />}
          </span>
          <span style={{ flex: 1 }} />
          {!agentBusy && agentEntries.length > 0 && (
            <button
              type="button"
              className="btn tiny ghost"
              onClick={clearAgentConversation}
              title="清空当前对话，开始新会话"
            >
              新对话
            </button>
          )}
          <button
            type="button"
            className="btn tiny ghost wb-agent-toggle"
            onClick={() => {
              const next = !agentOpen
              setAgentOpen(next)
              localStorage.setItem('wb_agent_open', String(next))
            }}
            title={agentOpen ? '收起 Agent 面板' : '展开 Agent 面板'}
          >
            {agentOpen ? '▼' : '▲'}
          </button>
        </div>

        {agentOpen && (
          <>
            {/* 对话记录区 */}
            <div className="agent-log wb-agent-log">
              {agentEntries.map((entry) => (
                <WbAgentEntryRow key={entry.id} entry={entry} />
              ))}
              {/* 实时流式输出 */}
              {agentStream && (
                <div className="agent-stream-live">
                  {agentLivePhase && (
                    <span className={`agent-msg-phase ${wbPhaseCls(agentLivePhase)}`} style={{ fontSize: '0.65rem', marginBottom: '0.2rem', display: 'inline-block' }}>
                      {WB_PHASE_LABELS[agentLivePhase] ?? agentLivePhase} ⟳
                    </span>
                  )}
                  <pre className="agent-stream-text">{agentStream}</pre>
                </div>
              )}
              <div ref={agentLogEndRef} />
            </div>

            {/* 输入表单 */}
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
          </>
        )}
      </footer>
      <MaintainSidebar projectId={pid} currentTable={selected} cellSelection={cellSelection} />
    </div>
  )
}
