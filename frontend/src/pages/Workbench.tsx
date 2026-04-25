import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import { apiFetch, projectHeaders } from '../api'
import { getInitAgentPrompt, pipelineStepLabel } from '../data/pipelineSteps'
import { createUniver, LocaleType, defaultTheme, type Univer } from '@univerjs/presets'
import type { FUniver } from '@univerjs/core/lib/facade'
import { UniverSheetsCorePreset, type FWorkbook } from '@univerjs/preset-sheets-core'
import UniverZhCN from '@univerjs/preset-sheets-core/locales/zh-CN'
import '@univerjs/preset-sheets-core/lib/index.css'

type TableInfo = { table_name: string; validation_status: string; layer: string; purpose?: string; display_name?: string }
type ColumnMeta = { name: string; sql_type?: string; display_name?: string; dtype?: string }

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
  const [searchParams] = useSearchParams()
  /** ?ro=1 → 只读模式（被 ProjectSetup 等场景嵌入时锁定单元格编辑） */
  const readOnly = searchParams.get('ro') === '1'
  /** 必须 memo：否则每次 render 新 headers 对象会触发 effect/useCallback 无限循环 → 浏览器 ERR_INSUFFICIENT_RESOURCES */
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [tables, setTables] = useState<TableInfo[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  // 当前活动表的行数据（仅用于缓存反向写入；展示由 Univer 接管）
  const [, setRows] = useState<Record<string, unknown>[]>([])
  const [tableReadmeDraft, setTableReadmeDraft] = useState('')
  const [globalReadmeDraft, setGlobalReadmeDraft] = useState('')
  const [readmeTab, setReadmeTab] = useState<'table' | 'global'>('table')
  /** README 编辑/预览模式（每个 tab 独立） */
  const [readmeViewMode, setReadmeViewMode] = useState<'preview' | 'edit'>('preview')
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
  /** 当前活动表的列顺序（用于将 Univer 行/列索引映射回 row_id/列名） */
  const [activeCols, setActiveCols] = useState<string[]>([])
  /** 当前活动表的列元信息（中文名/数据类型，用于 3 行表头） */
  const [, setActiveColMeta] = useState<ColumnMeta[]>([])
  /** 当前活动表的中文显示名 */
  const [activeDisplayName, setActiveDisplayName] = useState<string>('')

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
  const tableFormulasCacheRef = useRef<Map<string, Record<string, string>>>(new Map())
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
      sheets: { __placeholder__: { id: '__placeholder__', name: '加载中…', cellData: {} } },
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

    return () => {
      disposable.dispose()
      univer.dispose()
      univerRef.current = null
      univerAPIRef.current = null
      workbookRef.current = null
      loadedSheetsRef.current.clear()
      tableRowsCacheRef.current.clear()
      tableColsCacheRef.current.clear()
      tableFormulasCacheRef.current.clear()
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

  /** 把一张表的数据写入对应 Univer sheet（首次或刷新调用） */
  const populateSheet = useCallback(
    (tableName: string, rowsArr: Record<string, unknown>[], cols: string[], formulas: Record<string, string>, colMeta: ColumnMeta[] = [], displayName = '') => {
      const wb = workbookRef.current
      if (!wb) return
      const sheetTitle = displayName ? `${displayName}（${tableName}）` : tableName
      let sheet = wb.getSheetByName(sheetTitle) ?? wb.getSheetByName(tableName)
      if (!sheet) {
        sheet = wb.insertSheet(sheetTitle) ?? wb.getSheetByName(sheetTitle)
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
      const dtypeRow: (string | number)[] = cols.map((c) => metaByName.get(c)?.dtype || metaByName.get(c)?.sql_type || '')

      // 3 行表头：中文名 / 英文名 / 数据类型。公式不再占用一行，统一在顶部公式栏展示。
      const matrix: (string | number)[][] = [dispRow, nameRow, dtypeRow]
      for (const r of rowsArr) {
        matrix.push(cols.map((c) => {
          const v = r[c]
          if (v == null) return ''
          if (typeof v === 'object') return JSON.stringify(v)
          if (typeof v === 'number' || typeof v === 'string') return v
          return String(v)
        }))
      }
      const numCols = Math.max(1, cols.length)
      suppressEditRef.current = true
      try {
        try {
          const usedRange = sheet.getDataRange?.()
          if (usedRange) usedRange.clearContent()
        } catch { /* ignore */ }
        sheet.getRange(0, 0, matrix.length, numCols).setValues(matrix)
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
        try {
          const freezer = sheet as unknown as { setFrozenRows?: (n: number) => unknown; setFrozen?: (o: { ySplit?: number; xSplit?: number }) => unknown }
          freezer.setFrozenRows?.(3)
          freezer.setFrozen?.({ ySplit: 3, xSplit: 0 })
        } catch { /* ignore freeze errors */ }
      } finally {
        suppressEditRef.current = false
      }
      loadedSheetsRef.current.add(tableName)
    },
    [],
  )

  useEffect(() => {
    if (!selected) {
      setRows([])
      setActiveCols([])
      setActiveColMeta([])
      setActiveDisplayName('')
      setTableReadmeDraft('')
      setValidationRulesDraft('')
      setColumnFormulas({})
      activeTableRef.current = null
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
          schema?: { columns?: { name?: string; sql_type?: string; display_name?: string; dtype?: string }[] }
          display_name?: string
        }
        if (cancelled) return
        const rawRows = Array.isArray(r.rows) ? r.rows : []
        const normalized = rawRows.filter(
          (row): row is Record<string, unknown> =>
            row != null && typeof row === 'object' && !Array.isArray(row),
        )
        const cf = desc.column_formulas && typeof desc.column_formulas === 'object' && !Array.isArray(desc.column_formulas)
          ? (desc.column_formulas as Record<string, string>)
          : {}
        const schemaCols = Array.isArray(desc.schema?.columns) ? desc.schema!.columns! : []
        const colMeta: ColumnMeta[] = schemaCols.map((c) => ({
          name: String(c?.name ?? ''),
          sql_type: c?.sql_type,
          display_name: c?.display_name || '',
          dtype: c?.dtype || '',
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

        // 写入 Univer 并切换到该 sheet
        populateSheet(selected, normalized, cols, cf, colMeta, displayName)
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
      } catch (e) {
        if (!cancelled) setErr(String(e))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [selected, headers, populateSheet])

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
      populateSheet(selected, normalized, cols, formulas, colMeta, activeDisplayName)
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
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e)
        setErr(`单元格保存失败：${msg}`)
        if (typeof window !== 'undefined') {
          window.alert(`单元格保存失败：${msg}`)
        }
        await reloadActiveTable()
      }
    },
    [headers, reloadActiveTable],
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

  const formulaCols = useMemo(
    () => activeCols.filter((c) => columnFormulas[c]),
    [activeCols, columnFormulas],
  )

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
        <Link
          to={`/project-setup/${pid}`}
          className="link-btn"
          style={pipeline && !pipeline.finished ? { background: 'rgba(255,180,0,.25)' } : undefined}
          title="查看 / 继续 Agent 初始化进程"
        >
          ⚙ Agent 进程{pipeline && !pipeline.finished ? '（未完成）' : ''}
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
                    <span className="tbl-name">{t.display_name || t.table_name}</span>
                    {t.display_name ? (
                      <small className="tbl-en" title={t.table_name}>{t.table_name}</small>
                    ) : null}
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
          {formulaCols.length > 0 && (
            <div className="wb-formula-bar" title="列公式（只读）">
              <strong className="wb-formula-bar-label">列公式：</strong>
              {formulaCols.map((c) => (
                <span key={c} className="wb-formula-chip">
                  <code>{c}</code>
                  <span className="wb-formula-eq"> = </span>
                  <code>{columnFormulas[c]}</code>
                </span>
              ))}
            </div>
          )}
          <div className="wb-univer-host" ref={univerHostRef} />
          {readOnly && (
            <div className="wb-readonly-overlay" title="只读模式">
              🔒 只读模式（在 Agent 进程页中查看，请回到完整工作台编辑）
            </div>
          )}
        </section>

        <aside className="wb-right">
          <div className="wb-right-pane">
            <div className="readme-tabs readme-tab-btns">
              <button type="button" className={readmeTab === 'table' ? 'active' : ''} onClick={() => setReadmeTab('table')}>
                当前表 README
              </button>
              <button
                type="button"
                className={readmeTab === 'global' ? 'active' : ''}
                onClick={() => setReadmeTab('global')}
              >
                全局 README
              </button>
            </div>
            <div className="readme-mode-row">
              <button type="button"
                className={`btn tiny${readmeViewMode === 'preview' ? ' primary' : ''}`}
                onClick={() => setReadmeViewMode('preview')}>预览</button>
              <button type="button"
                className={`btn tiny${readmeViewMode === 'edit' ? ' primary' : ''}`}
                onClick={() => setReadmeViewMode('edit')} disabled={!canWrite || readOnly}>编辑</button>
              {(!canWrite || readOnly) && <span className="muted small">（只读）</span>}
            </div>
            {readmeTab === 'table' && (
              <>
                {!selected && <p className="muted small">请在左侧选择一张表。</p>}
                {selected && readmeViewMode === 'preview' && (
                  <div className="markdown-preview">
                    {tableReadmeDraft.trim()
                      ? <ReactMarkdown>{tableReadmeDraft}</ReactMarkdown>
                      : <p className="muted small">（此表暂无 README）</p>}
                  </div>
                )}
                {selected && readmeViewMode === 'edit' && (
                  <>
                    <textarea
                      className="readme-textarea"
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
                {readmeViewMode === 'preview' && (
                  <div className="markdown-preview">
                    {globalReadmeDraft.trim()
                      ? <ReactMarkdown>{globalReadmeDraft}</ReactMarkdown>
                      : <p className="muted small">（暂无全局 README）</p>}
                  </div>
                )}
                {readmeViewMode === 'edit' && (
                  <>
                    <textarea
                      className="readme-textarea"
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
