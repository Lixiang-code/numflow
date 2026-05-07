import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { apiFetch } from '../api'

type GlossaryItem = { term_en: string; term_zh: string }
type DimKeyItem = { key: string | number; display_name?: string }
type DimMeta = {
  col_name: string
  display_name?: string
  keys?: DimKeyItem[]
}
type ValueCol = {
  key: string
  display_name?: string
  dtype?: string
  number_format?: string
  formula?: string
}
type FormulaInfo = { formula: string; type: string }
type ThreeDimSnapshot = {
  ok: boolean
  table_name: string
  display_name: string
  dim1: DimMeta
  dim2: DimMeta
  cols: ValueCol[]
  column_formulas: Record<string, FormulaInfo>
  row_count: number
  data: Record<string, Record<string, Record<string, unknown>>>
}
type AxisKey = 'dim1' | 'dim2' | 'metric'

type ConstantItem = {
  name_en: string
  name_zh: string
  value: unknown
  formula?: string | null
  brief?: string
  design_intent?: string
}

const REF_COLORS = [
  { base: '#1976d2', bg: '#e3f2fd' },
  { base: '#c62828', bg: '#ffebee' },
  { base: '#2e7d32', bg: '#e8f5e9' },
  { base: '#e65100', bg: '#fff3e0' },
  { base: '#6a1b9a', bg: '#f3e5f5' },
  { base: '#00695c', bg: '#e0f2f1' },
  { base: '#ad1457', bg: '#fce4ec' },
  { base: '#4e342e', bg: '#efebe9' },
]

function formatValue(value: unknown, fmt?: string): string {
  if (value == null || value === '') return '—'
  if (typeof value !== 'number') return String(value)
  if (!fmt) return String(value)
  if (fmt.endsWith('%')) {
    const decimals = (fmt.match(/\.(\d+)%/) || ['', '0'])[1].length
    return `${(value * 100).toFixed(decimals)}%`
  }
  const decimals = (fmt.match(/\.(\d+)/) || ['', '0'])[1].length
  return value.toFixed(decimals)
}

function parseFormulaRefs(text: string): { colRefs: string[]; constRefs: string[]; refColorMap: Map<string, string> } {
  const colRefs: string[] = []
  const constRefs: string[] = []
  const refColorMap = new Map<string, string>()
  const colSeen = new Set<string>()
  const constSeen = new Set<string>()
  let colorIdx = 0
  if (!text) return { colRefs, constRefs, refColorMap }

  const allRefs: Array<{ kind: 'col' | 'const'; name: string }> = []
  const colRe = /@(?!@)(\w+)/g
  let m: RegExpExecArray | null
  while ((m = colRe.exec(text)) !== null) {
    allRefs.push({ kind: 'col', name: m[1] })
  }
  const constRe = /\$\{(\w+)\}/g
  while ((m = constRe.exec(text)) !== null) {
    allRefs.push({ kind: 'const', name: m[1] })
  }

  for (const ref of allRefs) {
    if (ref.kind === 'col') {
      if (!colSeen.has(ref.name)) {
        colSeen.add(ref.name)
        colRefs.push(ref.name)
        if (!refColorMap.has(ref.name)) {
          refColorMap.set(ref.name, REF_COLORS[colorIdx % REF_COLORS.length].base)
          colorIdx++
        }
      }
    } else {
      if (!constSeen.has(ref.name)) {
        constSeen.add(ref.name)
        constRefs.push(ref.name)
      }
    }
  }
  return { colRefs, constRefs, refColorMap }
}

function renderColoredFormula(formula: string, colorMap: Map<string, string>): React.ReactNode[] {
  const parts = formula.split(/(@\w+|\$\{\w+\})/g)
  return parts.map((part, i) => {
    const colMatch = part.match(/^@(\w+)$/)
    if (colMatch) {
      const color = colorMap.get(colMatch[1])
      if (color) return <span key={i} style={{ color, fontWeight: 700 }}>{part}</span>
    }
    const constMatch = part.match(/^\$\{(\w+)\}$/)
    if (constMatch) return <span key={i} style={{ color: '#7b1fa2', fontWeight: 600 }}>{part}</span>
    return <span key={i}>{part}</span>
  })
}

export default function ThreeDimTableEditor({
  tableName,
  headers,
  glossary,
  allConstants,
  canRecalculate = false,
  canWrite = false,
  onConstantsChanged,
  columnKinds = {},
  tableKind = '',
}: {
  tableName: string
  headers: Record<string, string>
  glossary: GlossaryItem[]
  allConstants?: ConstantItem[]
  canRecalculate?: boolean
  canWrite?: boolean
  onConstantsChanged?: () => void
  columnKinds?: Record<string, string>
  tableKind?: string
}) {
  const [snapshot, setSnapshot] = useState<ThreeDimSnapshot | null>(null)
  const [loadedRequestKey, setLoadedRequestKey] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [rowAxis, setRowAxis] = useState<AxisKey>('dim1')
  const [colAxis, setColAxis] = useState<AxisKey>('metric')
  const [fixedValue, setFixedValue] = useState<string | null>(null)
  const [recalculating, setRecalculating] = useState(false)
  const [showFormulaPanel, setShowFormulaPanel] = useState(false)
  const axisInitializedRef = useRef(false)
  const requestKey = tableName
  const [editingFormulaCol, setEditingFormulaCol] = useState<string | null>(null)
  const [editingFormulaText, setEditingFormulaText] = useState('')
  const [formulaSaving, setFormulaSaving] = useState(false)
  const [showAddFormula, setShowAddFormula] = useState(false)
  const [newFormulaCol, setNewFormulaCol] = useState('')
  const [newFormulaText, setNewFormulaText] = useState('')
  const [newFormulaType, setNewFormulaType] = useState('row')
  const addFormulaColSelectRef = useRef<HTMLSelectElement>(null)

  const [editingConstName, setEditingConstName] = useState<string | null>(null)
  const [editingConstValue, setEditingConstValue] = useState('')
  const [constSaving, setConstSaving] = useState(false)

  const [editingCell, setEditingCell] = useState<{ dim1Key: string; dim2Key: string; metricKey: string } | null>(null)
  const [editingCellValue, setEditingCellValue] = useState('')
  const [cellSaving, setCellSaving] = useState(false)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = (await apiFetch(`/meta/3d-matrix/${encodeURIComponent(tableName)}`, { headers })) as ThreeDimSnapshot
        if (cancelled) return
        setSnapshot(res)
        setErr(null)
        setLoadedRequestKey(requestKey)
        if (!axisInitializedRef.current) {
          const counts: [AxisKey, number][] = [
            ['dim1', (res.dim1.keys || []).length],
            ['dim2', (res.dim2.keys || []).length],
            ['metric', (res.cols || []).length],
          ]
          counts.sort((a, b) => b[1] - a[1])
          setRowAxis(counts[0][0])
          setColAxis(counts[1][0])
          axisInitializedRef.current = true
        }
      } catch (e) {
        if (cancelled) return
        setErr(String(e))
        setLoadedRequestKey(requestKey)
      }
    })()
    return () => { cancelled = true }
  }, [headers, requestKey, tableName])

  const refreshSnapshot = useCallback(async () => {
    const res = (await apiFetch(`/meta/3d-matrix/${encodeURIComponent(tableName)}`, { headers })) as ThreeDimSnapshot
    setSnapshot(res)
    setErr(null)
    setLoadedRequestKey(requestKey)
  }, [headers, requestKey, tableName])

  const glossaryMap = useMemo(
    () => new Map(glossary.map((item) => [item.term_en, item.term_zh])),
    [glossary],
  )

  const constMap = useMemo(
    () => new Map((allConstants || []).map((c) => [c.name_en, c])),
    [allConstants],
  )

  const dim1Keys = useMemo(
    () => (snapshot?.dim1.keys || []).map((item) => String(item.key)),
    [snapshot],
  )
  const dim2Keys = useMemo(
    () => (snapshot?.dim2.keys || []).map((item) => String(item.key)),
    [snapshot],
  )
  const metricKeys = useMemo(
    () => (snapshot?.cols || []).map((item) => item.key),
    [snapshot],
  )

  const dim1DisplayMap = useMemo(
    () => new Map((snapshot?.dim1.keys || []).map((item) => [String(item.key), item.display_name || glossaryMap.get(String(item.key)) || String(item.key)])),
    [glossaryMap, snapshot],
  )
  const dim2DisplayMap = useMemo(
    () => new Map((snapshot?.dim2.keys || []).map((item) => [String(item.key), item.display_name || glossaryMap.get(String(item.key)) || String(item.key)])),
    [glossaryMap, snapshot],
  )
  const metricMetaMap = useMemo(
    () => new Map((snapshot?.cols || []).map((item) => [item.key, item])),
    [snapshot],
  )

  const fixedAxis = useMemo<AxisKey | null>(() => {
    const all: AxisKey[] = ['dim1', 'dim2', 'metric']
    return all.find((axis) => axis !== rowAxis && axis !== colAxis) ?? null
  }, [colAxis, rowAxis])

  const axisOptions = useMemo(() => {
    if (!snapshot) return null
    return {
      dim1: {
        label: snapshot.dim1.display_name || snapshot.dim1.col_name,
        keys: dim1Keys,
        title: (key: string) => dim1DisplayMap.get(key) || glossaryMap.get(key) || key,
        subTitle: (key: string) => key,
      },
      dim2: {
        label: snapshot.dim2.display_name || snapshot.dim2.col_name,
        keys: dim2Keys,
        title: (key: string) => dim2DisplayMap.get(key) || glossaryMap.get(key) || key,
        subTitle: (key: string) => key,
      },
      metric: {
        label: '属性列',
        keys: metricKeys,
        title: (key: string) => metricMetaMap.get(key)?.display_name || glossaryMap.get(key) || key,
        subTitle: (key: string) => key,
      },
    } satisfies Record<AxisKey, { label: string; keys: string[]; title: (key: string) => string; subTitle: (key: string) => string }>
  }, [dim1DisplayMap, dim1Keys, dim2DisplayMap, dim2Keys, glossaryMap, metricKeys, metricMetaMap, snapshot])
  const fixedKeys = fixedAxis && axisOptions ? axisOptions[fixedAxis].keys : []
  const effectiveFixedValue = fixedValue && fixedKeys.includes(fixedValue) ? fixedValue : (fixedKeys[0] ?? null)

  const formulaEntries = useMemo(
    () => (snapshot?.cols || [])
      .map((col) => ({ col, formula: snapshot?.column_formulas?.[col.key] }))
      .filter((item): item is { col: ValueCol; formula: FormulaInfo } => Boolean(item.formula)),
    [snapshot],
  )

  const relevantFormulaEntries = useMemo(() => {
    if (!fixedAxis) return formulaEntries
    if (fixedAxis === 'metric' && effectiveFixedValue) {
      return formulaEntries.filter(({ col }) => col.key === effectiveFixedValue)
    }
    if (rowAxis === 'metric' || colAxis === 'metric') return formulaEntries
    return formulaEntries
  }, [colAxis, effectiveFixedValue, fixedAxis, formulaEntries, rowAxis])

  const activeFormulaText = editingFormulaCol ? editingFormulaText : showAddFormula ? newFormulaText : ''
  const { colRefs, constRefs: activeConstRefs, refColorMap } = useMemo(
    () => parseFormulaRefs(activeFormulaText),
    [activeFormulaText],
  )

  const pickRowAxis = (next: AxisKey) => {
    if (next === colAxis) {
      const replacement = (['dim1', 'dim2', 'metric'] as AxisKey[]).find((axis) => axis !== next && axis !== rowAxis) || 'metric'
      setColAxis(replacement)
    }
    setRowAxis(next)
  }

  const pickColAxis = (next: AxisKey) => {
    if (next === rowAxis) return
    setColAxis(next)
  }

  async function recalculateTable() {
    if (!canRecalculate) return
    setRecalculating(true)
    setErr(null)
    try {
      await apiFetch(`/compute/column-formula/recalculate-table?table_name=${encodeURIComponent(tableName)}`, {
        method: 'POST',
        headers,
      })
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    } finally {
      setRecalculating(false)
    }
  }

  async function saveFormula(columnName: string, formula: string) {
    setFormulaSaving(true)
    setErr(null)
    try {
      await apiFetch('/compute/column-formula', {
        method: 'PUT',
        headers,
        body: JSON.stringify({ table_name: tableName, column_name: columnName, formula: formula.trim() }),
      })
      setEditingFormulaCol(null)
      setEditingFormulaText('')
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    } finally {
      setFormulaSaving(false)
    }
  }

  async function deleteFormula(columnName: string) {
    setErr(null)
    try {
      await apiFetch(`/compute/column-formula?table_name=${encodeURIComponent(tableName)}&column_name=${encodeURIComponent(columnName)}`, {
        method: 'DELETE',
        headers,
      })
      setEditingFormulaCol(null)
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    }
  }

  async function recalculateSingleFormula(columnName: string) {
    setErr(null)
    try {
      await apiFetch(`/compute/column-formula/recalculate?table_name=${encodeURIComponent(tableName)}&column_name=${encodeURIComponent(columnName)}`, {
        method: 'POST',
        headers,
      })
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    }
  }

  async function addFormula() {
    if (!newFormulaCol || !newFormulaText.trim()) return
    setFormulaSaving(true)
    setErr(null)
    try {
      await apiFetch('/compute/column-formula', {
        method: 'PUT',
        headers,
        body: JSON.stringify({ table_name: tableName, column_name: newFormulaCol, formula: newFormulaText.trim() }),
      })
      setShowAddFormula(false)
      setNewFormulaCol('')
      setNewFormulaText('')
      setNewFormulaType('row')
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    } finally {
      setFormulaSaving(false)
    }
  }

  async function saveConstant(nameEn: string) {
    if (!editingConstValue.trim()) return
    setConstSaving(true)
    setErr(null)
    try {
      const numVal = Number(editingConstValue)
      await apiFetch(`/meta/constants/${encodeURIComponent(nameEn)}`, {
        method: 'PATCH',
        headers,
        body: JSON.stringify(isNaN(numVal) ? { value: editingConstValue } : { value: numVal }),
      })
      setEditingConstName(null)
      setEditingConstValue('')
      // 1. 重算当前表所有行公式（常量值变更后重新求值）
      await apiFetch(`/compute/column-formula/recalculate-table?table_name=${encodeURIComponent(tableName)}`, { method: 'POST', headers })
      // 2. 沿依赖图级联重算下游表（连锁反应）
      for (const entry of formulaEntries) {
        await apiFetch(
          `/compute/recalculate-downstream?table_name=${encodeURIComponent(tableName)}&column_name=${encodeURIComponent(entry.col.key)}`,
          { method: 'POST', headers },
        ).catch(() => { /* 无下游依赖则跳过 */ })
      }
      await refreshSnapshot()
      onConstantsChanged?.()
    } catch (e) {
      setErr(String(e))
    } finally {
      setConstSaving(false)
    }
  }

  async function saveCellEdit(dim1Key: string, dim2Key: string, metricKey: string) {
    if (!editingCellValue.trim()) return
    setCellSaving(true)
    setErr(null)
    const rowId = `${dim1Key}_${dim2Key}`
    try {
      const numVal = Number(editingCellValue)
      await apiFetch('/data/cells/write', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          table_name: tableName,
          updates: [{ row_id: rowId, column: metricKey, value: isNaN(numVal) ? editingCellValue : numVal }],
          source_tag: 'ai_generated',
        }),
      })
      await apiFetch('/data/cells/mark-manual', {
        method: 'POST',
        headers,
        body: JSON.stringify({ table_name: tableName, row_id: rowId, column: metricKey }),
      })
      setEditingCell(null)
      setEditingCellValue('')
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    } finally {
      setCellSaving(false)
    }
  }

  function startConstEdit(nameEn: string) {
    const c = constMap.get(nameEn)
    setEditingConstName(nameEn)
    setEditingConstValue(c?.value != null ? String(c.value) : c?.formula || '')
  }

  const startEditFormula = (colKey: string, formula: string) => {
    setEditingFormulaCol(colKey)
    setEditingFormulaText(formula)
    setShowAddFormula(false)
    setEditingConstName(null)
  }

  const cancelEdit = () => {
    setEditingFormulaCol(null)
    setEditingFormulaText('')
    setEditingConstName(null)
  }

  const unformulaedCols = useMemo(
    () => (snapshot?.cols || []).filter((c) => !snapshot?.column_formulas?.[c.key]),
    [snapshot],
  )

  const loading = loadedRequestKey !== requestKey

  if (loading) return <div className="muted small" style={{ padding: '1rem' }}>加载三维表中…</div>
  if (err) return <div className="err small" style={{ padding: '1rem' }}>加载失败：{err}</div>
  if (!snapshot || !axisOptions || !fixedAxis) return null

  const rowKeys = axisOptions[rowAxis].keys
  const colKeys = axisOptions[colAxis].keys
  const isHighlightingActive = Boolean(activeFormulaText && colRefs.length > 0)

  const getColColor = (colKey: string): string | undefined => {
    if (!isHighlightingActive) return undefined
    if (colAxis === 'metric') return refColorMap.get(colKey)
    return undefined
  }
  const getRowColor = (rowKey: string): string | undefined => {
    if (!isHighlightingActive) return undefined
    if (rowAxis === 'metric') return refColorMap.get(rowKey)
    return undefined
  }
  const getCellBg = (colKey: string, rowKey: string): string | undefined => {
    const cc = getColColor(colKey)
    const rc = getRowColor(rowKey)
    if (cc) {
      const entry = REF_COLORS.find((c) => c.base === cc)
      if (entry) return entry.bg
    }
    if (rc) {
      const entry = REF_COLORS.find((c) => c.base === rc)
      if (entry) return entry.bg
    }
    return undefined
  }

  const getColumnKindBg = (colKey: string): string | undefined => {
    let kind = columnKinds[colKey]
    if (!kind) {
      if (tableKind === 'compute') kind = 'compute'
      else if (tableKind === 'config') kind = 'config'
    }
    if (kind === 'config') return '#faf0e6'
    if (kind === 'compute') return '#f0f0f0'
    return undefined
  }

  const getValue = (rowKey: string, colKey: string): unknown => {
    if (!effectiveFixedValue) return null
    const values: Record<AxisKey, string> = {
      dim1: '',
      dim2: '',
      metric: '',
      [rowAxis]: rowKey,
      [colAxis]: colKey,
      [fixedAxis]: effectiveFixedValue,
    }
    return snapshot.data?.[values.dim1]?.[values.dim2]?.[values.metric]
  }

  const getCellCoords = (rowKey: string, colKey: string): { dim1Key: string; dim2Key: string; metricKey: string } | null => {
    if (!effectiveFixedValue) return null
    const coords: Record<AxisKey, string> = {
      dim1: '', dim2: '', metric: '',
      [rowAxis]: rowKey,
      [colAxis]: colKey,
      [fixedAxis]: effectiveFixedValue,
    }
    return { dim1Key: coords.dim1, dim2Key: coords.dim2, metricKey: coords.metric }
  }

  const getMetricFormula = (metricKey: string): FormulaInfo | undefined => {
    const reg = snapshot.column_formulas?.[metricKey]
    if (reg) return reg
    const colDef = metricMetaMap.get(metricKey)
    if (colDef?.formula) return { formula: colDef.formula, type: 'row' }
    return undefined
  }

  const FAB_STYLE: React.CSSProperties = {
    fontFamily: '"Cascadia Code","Fira Code",ui-monospace,monospace',
    fontSize: '.78rem',
    lineHeight: '1.5',
    padding: '.3rem .4rem',
    borderRadius: 4,
    border: '1px solid #90caf9',
    color: '#333',
  }

  const renderFormulaOverlay = (text: string, map: Map<string, string>, rows: number) => {
    const clampedRows = Math.min(rows, 12)
    return (
      <pre style={{
        position: 'absolute', top: 0, left: 0, right: 0,
        margin: 0, overflow: 'hidden',
        whiteSpace: 'pre-wrap', wordBreak: 'break-word',
        pointerEvents: 'none', zIndex: 0,
        fontFamily: FAB_STYLE.fontFamily,
        fontSize: FAB_STYLE.fontSize,
        lineHeight: FAB_STYLE.lineHeight,
        padding: FAB_STYLE.padding,
        border: '1px solid transparent',
        minHeight: `calc(${FAB_STYLE.lineHeight} * ${clampedRows} + .6rem)`,
      }}>
        <span style={{ color: FAB_STYLE.color }}>
          {renderColoredFormula(text, map)}
          {!text && <span>&nbsp;</span>}
          {'\n'}
        </span>
      </pre>
    )
  }

  const renderConstChips = (refs: string[]) => {
    if (refs.length === 0) return null
    return (
      <div className="matrix-const-chips">
        <span className="muted small" style={{ marginRight: 4 }}>常量引用:</span>
        {refs.map((name) => {
          const c = constMap.get(name)
          const isEditing = editingConstName === name
          return (
            <span key={name} style={{ display: 'inline-flex' }}>
              {isEditing ? (
                <span className="matrix-const-chip-edit-row">
                  <code className="matrix-const-chip-name">{name}</code>
                  <input
                    className="matrix-const-chip-input"
                    value={editingConstValue}
                    onChange={(e) => setEditingConstValue(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); void saveConstant(name) } }}
                    autoFocus
                    size={10}
                  />
                  <button type="button" className="btn tiny primary" onClick={() => void saveConstant(name)} disabled={constSaving}>✓</button>
                  <button type="button" className="btn tiny" onClick={() => { setEditingConstName(null); setEditingConstValue('') }}>✕</button>
                </span>
              ) : (
                <button
                  type="button"
                  className="matrix-const-chip-btn"
                  onClick={() => startConstEdit(name)}
                  title={c ? `当前值: ${c.value ?? c.formula ?? '未设置'}${c.brief ? ' — ' + c.brief : ''}` : '未找到常量'}
                >
                  <code>{name}</code>
                  <span className="matrix-const-chip-val">{c?.value != null ? String(c.value) : c?.formula || '?'}</span>
                </button>
              )}
            </span>
          )
        })}
      </div>
    )
  }

  const renderRefLegend = (refs: string[], map: Map<string, string>) => {
    if (refs.length === 0) return null
    return (
      <div className="matrix-ref-legend">
        {refs.map((name) => {
          const color = map.get(name)
          if (!color) return null
          return (
            <span key={name} className="matrix-ref-legend-chip" style={{ borderColor: color }}>
              <span className="matrix-ref-legend-dot" style={{ background: color }} />
              <code style={{ color }}>@{name}</code>
            </span>
          )
        })}
      </div>
    )
  }

  return (
    <div className="matrix-editor">
      <div className="matrix-topbar">
        <span className="matrix-kind-badge">三维数据表</span>
        <span className="matrix-meta-chip muted" style={{ fontWeight: 500 }}>
          {snapshot.dim1.display_name || snapshot.dim1.col_name} × {snapshot.dim2.display_name || snapshot.dim2.col_name} × 属性列
        </span>
        <span className="matrix-meta-chip" style={{ background: '#e3f2fd', borderColor: '#bbdefb', color: '#1565c0' }}>
          {snapshot.row_count} 组合 · {snapshot.cols.length} 属性
        </span>
        <div style={{ flex: 1 }} />
        <button
          type="button"
          className={`btn tiny${showFormulaPanel ? ' primary' : ''}`}
          onClick={() => { setShowFormulaPanel(!showFormulaPanel); setShowAddFormula(false); setEditingFormulaCol(null); setEditingConstName(null) }}
          style={{ marginRight: 4 }}
        >
          {showFormulaPanel ? '收起公式' : `公式 (${formulaEntries.length})`}
        </button>
        {canRecalculate && formulaEntries.length > 0 && (
          <button type="button" className="btn tiny primary" onClick={() => void recalculateTable()} disabled={recalculating}>
            {recalculating ? '重算中…' : '重算全部'}
          </button>
        )}
      </div>

      <div className="matrix-axis-bar">
          <div className="matrix-axis-row">
            <span className="matrix-axis-label">行轴</span>
            <div className="matrix-axis-btns">
              {(['dim1', 'dim2', 'metric'] as AxisKey[]).map((axis) => (
                <button
                  key={`row-${axis}`}
                  type="button"
                  className={`btn tiny${rowAxis === axis ? ' primary' : ''}`}
                  onClick={() => pickRowAxis(axis)}
                >
                  {axisOptions[axis].label}
                </button>
              ))}
            </div>
          </div>
          <div className="matrix-axis-row">
            <span className="matrix-axis-label">列轴</span>
            <div className="matrix-axis-btns">
              {(['dim1', 'dim2', 'metric'] as AxisKey[]).map((axis) => (
                <button
                  key={`col-${axis}`}
                  type="button"
                  className={`btn tiny${colAxis === axis ? ' primary' : ''}`}
                  onClick={() => pickColAxis(axis)}
                  disabled={axis === rowAxis}
                >
                  {axisOptions[axis].label}
                </button>
              ))}
            </div>
          </div>
          <div className="matrix-axis-row">
            <span className="matrix-axis-label">第三维</span>
            <span className="matrix-meta-chip" style={{ marginRight: 6 }}>{axisOptions[fixedAxis].label}</span>
            <select
              value={effectiveFixedValue || ''}
              onChange={(e) => setFixedValue(e.target.value)}
              className="matrix-axis-select"
            >
              {fixedKeys.map((key) => (
                <option key={key} value={key}>
                  {axisOptions[fixedAxis].title(key)} · {axisOptions[fixedAxis].subTitle(key)}
                </option>
              ))}
            </select>
          </div>
        </div>

      <div className="matrix-scroll">
        <table className="matrix-table">
          <thead>
            <tr>
              <th className="matrix-corner">
                {axisOptions[rowAxis].label} \ {axisOptions[colAxis].label}
              </th>
              {colKeys.map((colKey) => {
                const formula = colAxis === 'metric' ? getMetricFormula(colKey) : undefined
                const hlColor = getColColor(colKey)
                return (
                  <th
                    key={colKey}
                    className={`matrix-col-head${hlColor ? ' matrix-col-hl' : ''}`}
                    title={formula?.formula || axisOptions[colAxis].subTitle(colKey)}
                    style={hlColor ? { borderBottom: `3px solid ${hlColor}`, background: REF_COLORS.find((c) => c.base === hlColor)?.bg } : undefined}
                  >
                    <span className="matrix-head-zh">
                      {axisOptions[colAxis].title(colKey)}
                      {formula && <span className="sl-preview-tag" style={{ marginLeft: 6 }}>公式</span>}
                    </span>
                    <span className="matrix-head-en muted">{axisOptions[colAxis].subTitle(colKey)}</span>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {rowKeys.map((rowKey) => {
              const rowFormula = rowAxis === 'metric' ? getMetricFormula(rowKey) : undefined
              const rowHlColor = getRowColor(rowKey)
              return (
                <tr key={rowKey}>
                  <td
                    className={`matrix-row-head${rowHlColor ? ' matrix-row-hl' : ''}`}
                    title={rowFormula?.formula || axisOptions[rowAxis].subTitle(rowKey)}
                    style={rowHlColor ? { borderLeft: `3px solid ${rowHlColor}`, background: REF_COLORS.find((c) => c.base === rowHlColor)?.bg } : undefined}
                  >
                    <span className="matrix-head-zh">
                      {axisOptions[rowAxis].title(rowKey)}
                      {rowFormula && <span className="sl-preview-tag" style={{ marginLeft: 6 }}>公式</span>}
                    </span>
                    <span className="matrix-head-en muted">{axisOptions[rowAxis].subTitle(rowKey)}</span>
                  </td>
                  {colKeys.map((colKey) => {
                    const metricKey = rowAxis === 'metric'
                      ? rowKey
                      : colAxis === 'metric'
                        ? colKey
                        : effectiveFixedValue
                    const metricMeta = metricKey ? metricMetaMap.get(metricKey) : undefined
                    const value = getValue(rowKey, colKey)
                    const cellBg = getCellBg(colKey, rowKey)
                    const kindBg = getColumnKindBg(colKey)
                    const bg = cellBg || kindBg
                    const coords = getCellCoords(rowKey, colKey)
                    const isEditingCell = editingCell?.dim1Key === coords?.dim1Key
                      && editingCell?.dim2Key === coords?.dim2Key
                      && editingCell?.metricKey === coords?.metricKey
                    const cellFormula = coords ? getMetricFormula(coords.metricKey) : undefined
                    const cellEditable = canWrite && !cellFormula && coords && !cellSaving
                    return (
                      <td
                        key={colKey}
                        className={`matrix-cell${value == null ? ' matrix-cell-empty' : ''}${cellEditable ? ' matrix-cell-editable' : ''}`}
                        style={bg ? { background: bg } : undefined}
                        title={cellFormula ? '公式列值，自动计算不可编辑' : cellEditable ? '点击编辑' : undefined}
                        onClick={() => {
                          if (!cellEditable || !coords) return
                          setEditingCell(coords)
                          setEditingCellValue(value != null ? (typeof value === 'number' ? String(value) : String(value)) : '')
                        }}
                      >
                        {isEditingCell ? (
                          <input
                            className="matrix-cell-edit-input"
                            value={editingCellValue}
                            onChange={(e) => setEditingCellValue(e.target.value)}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') { e.preventDefault(); void saveCellEdit(coords!.dim1Key, coords!.dim2Key, coords!.metricKey) }
                              if (e.key === 'Escape') { setEditingCell(null); setEditingCellValue('') }
                            }}
                            onBlur={() => { setEditingCell(null); setEditingCellValue('') }}
                            autoFocus
                          />
                        ) : (
                          formatValue(value, metricMeta?.number_format)
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {showFormulaPanel && (
        <div className="matrix-formula-panel">
          <div className="matrix-formula-panel-header">
            <span>
              <strong>公式管理</strong>
              <span className="muted small" style={{ marginLeft: 8 }}>{formulaEntries.length} 个公式</span>
            </span>
            <div style={{ display: 'flex', gap: 4 }}>
              {canWrite && !showAddFormula && (
                <button type="button" className="btn tiny" onClick={() => { setShowAddFormula(true); setEditingFormulaCol(null); setEditingConstName(null) }}>添加公式</button>
              )}
              <button type="button" className="btn tiny" onClick={() => { setShowFormulaPanel(false); setEditingFormulaCol(null); setEditingConstName(null) }}>收起</button>
            </div>
          </div>

          {showAddFormula && (
            <div className="matrix-formula-add">
              <div className="matrix-formula-add-row">
                <span className="muted small" style={{ minWidth: 48 }}>目标列</span>
                <select
                  ref={addFormulaColSelectRef}
                  value={newFormulaCol}
                  onChange={(e) => setNewFormulaCol(e.target.value)}
                  className="matrix-axis-select"
                  style={{ flex: 1, maxWidth: 200 }}
                >
                  <option value="">请选择列</option>
                  {unformulaedCols.map((c) => (
                    <option key={c.key} value={c.key}>{c.display_name || c.key}</option>
                  ))}
                  {formulaEntries.map(({ col }) => (
                    <option key={col.key} value={col.key} disabled>{col.display_name || col.key} (已有公式)</option>
                  ))}
                </select>
                <span className="muted small" style={{ minWidth: 48, marginLeft: 12 }}>类型</span>
                <select value={newFormulaType} onChange={(e) => setNewFormulaType(e.target.value)} className="matrix-axis-select" style={{ width: 100 }}>
                  <option value="row">行公式</option>
                  <option value="row_template">运行时模板</option>
                </select>
              </div>
              <div className="matrix-formula-add-row" style={{ marginTop: 6 }}>
                <span className="muted small" style={{ minWidth: 48, alignSelf: 'flex-start', marginTop: 6 }}>公式</span>
                <div style={{ position: 'relative', flex: 1 }}>
                  {renderFormulaOverlay(newFormulaText, refColorMap, 2)}
                  <textarea
                    className="matrix-formula-input"
                    value={newFormulaText}
                    onChange={(e) => setNewFormulaText(e.target.value)}
                    placeholder="例如: @hp * 1.2 + @def * 0.5"
                    rows={2}
                    style={{ flex: 1, resize: 'vertical', position: 'relative', zIndex: 1, background: 'transparent', color: 'transparent', caretColor: '#333', border: '1px solid #90caf9' }}
                  />
                </div>
              </div>
              {renderRefLegend(colRefs, refColorMap)}
              {renderConstChips(activeConstRefs)}
              <div style={{ display: 'flex', gap: 4, marginTop: 8 }}>
                <button type="button" className="btn tiny primary" onClick={() => void addFormula()} disabled={formulaSaving || !newFormulaCol || !newFormulaText.trim()}>
                  {formulaSaving ? '保存中…' : '保存'}
                </button>
                <button type="button" className="btn tiny" onClick={() => { setShowAddFormula(false); setNewFormulaCol(''); setNewFormulaText(''); setEditingConstName(null) }}>取消</button>
              </div>
            </div>
          )}

          {relevantFormulaEntries.length === 0 && formulaEntries.length > 0 ? (
            <div className="muted small" style={{ padding: '0.75rem' }}>
              当前视图相关公式：共 {formulaEntries.length} 个公式，当前切片视角下显示 {relevantFormulaEntries.length} 个。
            </div>
          ) : null}

          <div className="matrix-formula-list">
            {formulaEntries.length === 0 && (
              <div className="muted small" style={{ padding: '0.75rem', textAlign: 'center' }}>暂无公式</div>
            )}
            {formulaEntries.map(({ col, formula }) => {
              const isEditing = editingFormulaCol === col.key
              const isRelevant = relevantFormulaEntries.some((e) => e.col.key === col.key)
              const editRefs = isEditing ? parseFormulaRefs(editingFormulaText) : { colRefs: [] as string[], constRefs: [] as string[], refColorMap: new Map<string, string>() }
              const displayRefs = isEditing ? { colRefs: editRefs.colRefs, refColorMap: editRefs.refColorMap, constRefs: editRefs.constRefs }
                : parseFormulaRefs(formula.formula)
              return (
                <div
                  key={col.key}
                  className={`matrix-formula-item${isRelevant ? '' : ' matrix-formula-item-irrelevant'}`}
                >
                  <div className="matrix-formula-item-header">
                    <span className="matrix-formula-col-name">
                      {col.display_name || col.key}
                      <code className="muted small" style={{ marginLeft: 6 }}>{col.key}</code>
                    </span>
                    <span className={`matrix-formula-type-badge matrix-formula-type-${formula.type}`}>
                      {formula.type === 'row' ? '行公式' : formula.type === 'row_template' ? '运行时模板' : formula.type}
                    </span>
                    {!isRelevant && <span className="muted small" style={{ marginLeft: 4 }}>(当前视图无影响)</span>}
                    <div style={{ flex: 1 }} />
                    {canWrite && !isEditing && (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <button type="button" className="btn tiny" onClick={() => startEditFormula(col.key, formula.formula)}>编辑</button>
                        <button type="button" className="btn tiny" onClick={() => void recalculateSingleFormula(col.key)} title="重算此列">重算</button>
                        <button type="button" className="btn tiny danger" onClick={() => void deleteFormula(col.key)} title="删除公式">删除</button>
                      </div>
                    )}
                  </div>
                  {isEditing ? (
                    <div style={{ marginTop: 6 }}>
                      <div style={{ position: 'relative' }}>
                        {renderFormulaOverlay(editingFormulaText, editRefs.refColorMap, Math.min(editingFormulaText.split('\n').length, 6))}
                        <textarea
                          className="matrix-formula-input"
                          value={editingFormulaText}
                          onChange={(e) => setEditingFormulaText(e.target.value)}
                          rows={Math.min(editingFormulaText.split('\n').length, 6)}
                          style={{ width: '100%', resize: 'vertical', position: 'relative', zIndex: 1, background: 'transparent', color: 'transparent', caretColor: '#333', border: '1px solid #90caf9' }}
                          autoFocus
                        />
                      </div>
                      {renderRefLegend(editRefs.colRefs, editRefs.refColorMap)}
                      {renderConstChips(editRefs.constRefs)}
                      <div style={{ display: 'flex', gap: 4, marginTop: 6 }}>
                        <button
                          type="button"
                          className="btn tiny primary"
                          onClick={() => void saveFormula(col.key, editingFormulaText)}
                          disabled={formulaSaving || !editingFormulaText.trim()}
                        >
                          {formulaSaving ? '保存中…' : '保存'}
                        </button>
                        <button type="button" className="btn tiny" onClick={cancelEdit}>取消</button>
                      </div>
                    </div>
                  ) : (
                    <code className="matrix-formula-text matrix-formula-text-colored">
                      {renderColoredFormula(formula.formula, displayRefs.refColorMap)}
                    </code>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
