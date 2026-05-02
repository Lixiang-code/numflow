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

export default function ThreeDimTableEditor({
  tableName,
  headers,
  glossary,
  canRecalculate = false,
  canWrite = false,
}: {
  tableName: string
  headers: Record<string, string>
  glossary: GlossaryItem[]
  canRecalculate?: boolean
  canWrite?: boolean
}) {
  const [snapshot, setSnapshot] = useState<ThreeDimSnapshot | null>(null)
  const [loadedRequestKey, setLoadedRequestKey] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [rowAxis, setRowAxis] = useState<AxisKey>('dim1')
  const [colAxis, setColAxis] = useState<AxisKey>('metric')
  const [fixedValue, setFixedValue] = useState<string | null>(null)
  const [recalculating, setRecalculating] = useState(false)
  const [showFormulaPanel, setShowFormulaPanel] = useState(false)
  const requestKey = tableName

  const [editingFormulaCol, setEditingFormulaCol] = useState<string | null>(null)
  const [editingFormulaText, setEditingFormulaText] = useState('')
  const [formulaSaving, setFormulaSaving] = useState(false)
  const [showAddFormula, setShowAddFormula] = useState(false)
  const [newFormulaCol, setNewFormulaCol] = useState('')
  const [newFormulaText, setNewFormulaText] = useState('')
  const [newFormulaType, setNewFormulaType] = useState('row')
  const [showAxisSettings, setShowAxisSettings] = useState(false)
  const addFormulaColSelectRef = useRef<HTMLSelectElement>(null)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = (await apiFetch(`/meta/3d-matrix/${encodeURIComponent(tableName)}`, { headers })) as ThreeDimSnapshot
        if (cancelled) return
        setSnapshot(res)
        setErr(null)
        setLoadedRequestKey(requestKey)
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

  const startEditFormula = (colKey: string, formula: string) => {
    setEditingFormulaCol(colKey)
    setEditingFormulaText(formula)
    setShowAddFormula(false)
  }

  const cancelEdit = () => {
    setEditingFormulaCol(null)
    setEditingFormulaText('')
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

  const getMetricFormula = (metricKey: string): FormulaInfo | undefined => snapshot.column_formulas?.[metricKey]

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
          className={`btn tiny${showAxisSettings ? ' primary' : ''}`}
          onClick={() => setShowAxisSettings(!showAxisSettings)}
          style={{ marginRight: 4 }}
        >
          {showAxisSettings ? '隐藏切片' : '切片设置'}
        </button>
        <button
          type="button"
          className={`btn tiny${showFormulaPanel ? ' primary' : ''}`}
          onClick={() => { setShowFormulaPanel(!showFormulaPanel); setShowAddFormula(false); setEditingFormulaCol(null) }}
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

      {showAxisSettings && (
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
      )}

      <div className="matrix-scroll">
        <table className="matrix-table">
          <thead>
            <tr>
              <th className="matrix-corner">
                {axisOptions[rowAxis].label} \ {axisOptions[colAxis].label}
              </th>
              {colKeys.map((colKey) => {
                const formula = colAxis === 'metric' ? getMetricFormula(colKey) : undefined
                return (
                  <th
                    key={colKey}
                    className="matrix-col-head"
                    title={formula?.formula || axisOptions[colAxis].subTitle(colKey)}
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
              return (
                <tr key={rowKey}>
                  <td className="matrix-row-head" title={rowFormula?.formula || axisOptions[rowAxis].subTitle(rowKey)}>
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
                    return (
                      <td key={colKey} className={`matrix-cell${value == null ? ' matrix-cell-empty' : ''}`}>
                        {formatValue(value, metricMeta?.number_format)}
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
                <button type="button" className="btn tiny" onClick={() => setShowAddFormula(true)}>添加公式</button>
              )}
              <button type="button" className="btn tiny" onClick={() => setShowFormulaPanel(false)}>收起</button>
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
                <textarea
                  className="matrix-formula-input"
                  value={newFormulaText}
                  onChange={(e) => setNewFormulaText(e.target.value)}
                  placeholder="例如: @hp * 1.2 + @def * 0.5"
                  rows={2}
                  style={{ flex: 1, resize: 'vertical' }}
                />
              </div>
              <div style={{ display: 'flex', gap: 4, marginTop: 8 }}>
                <button type="button" className="btn tiny primary" onClick={() => void addFormula()} disabled={formulaSaving || !newFormulaCol || !newFormulaText.trim()}>
                  {formulaSaving ? '保存中…' : '保存'}
                </button>
                <button type="button" className="btn tiny" onClick={() => { setShowAddFormula(false); setNewFormulaCol(''); setNewFormulaText('') }}>取消</button>
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
                      <textarea
                        className="matrix-formula-input"
                        value={editingFormulaText}
                        onChange={(e) => setEditingFormulaText(e.target.value)}
                        rows={Math.min(editingFormulaText.split('\n').length, 6)}
                        style={{ width: '100%', resize: 'vertical' }}
                        autoFocus
                      />
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
                    <code className="matrix-formula-text">{formula.formula}</code>
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
