import { useCallback, useEffect, useMemo, useState } from 'react'
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
}: {
  tableName: string
  headers: Record<string, string>
  glossary: GlossaryItem[]
  canRecalculate?: boolean
}) {
  const [snapshot, setSnapshot] = useState<ThreeDimSnapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  const [rowAxis, setRowAxis] = useState<AxisKey>('dim1')
  const [colAxis, setColAxis] = useState<AxisKey>('metric')
  const [fixedValue, setFixedValue] = useState<string | null>(null)
  const [recalculating, setRecalculating] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    ;(async () => {
      try {
        const res = (await apiFetch(`/meta/3d-matrix/${encodeURIComponent(tableName)}`, { headers })) as ThreeDimSnapshot
        if (cancelled) return
        setSnapshot(res)
        setErr(null)
      } catch (e) {
        if (cancelled) return
        setErr(String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [headers, tableName])

  const refreshSnapshot = useCallback(async () => {
    const res = (await apiFetch(`/meta/3d-matrix/${encodeURIComponent(tableName)}`, { headers })) as ThreeDimSnapshot
    setSnapshot(res)
    setErr(null)
  }, [headers, tableName])

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

  useEffect(() => {
    if (!fixedAxis || !axisOptions) return
    const keys = axisOptions[fixedAxis].keys
    if (fixedValue && keys.includes(fixedValue)) return
    setFixedValue(keys[0] ?? null)
  }, [axisOptions, fixedAxis, fixedValue])

  const formulaEntries = useMemo(
    () => (snapshot?.cols || [])
      .map((col) => ({ col, formula: snapshot?.column_formulas?.[col.key] }))
      .filter((item): item is { col: ValueCol; formula: FormulaInfo } => Boolean(item.formula)),
    [snapshot],
  )
  const sliceExamples = useMemo(() => {
    if (!axisOptions) return []
    const firstDim1 = dim1Keys[0]
    const firstDim2 = dim2Keys[0]
    const firstMetric = metricKeys[0]
    if (!firstDim1 || !firstDim2 || !firstMetric) return []
    return [
      {
        title: '查看某个分类的全部属性',
        description: `固定 ${axisOptions.dim2.label} = ${axisOptions.dim2.title(firstDim2)}，保留 ${axisOptions.dim1.label} × ${axisOptions.metric.label}`,
        tool: `read_3d_table keep_axes=['dim1','metric'] + dim2_keys=['${firstDim2}']`,
      },
      {
        title: '查看某个等级/档位的全部分类属性',
        description: `固定 ${axisOptions.dim1.label} = ${axisOptions.dim1.title(firstDim1)}，保留 ${axisOptions.dim2.label} × ${axisOptions.metric.label}`,
        tool: `read_3d_table keep_axes=['dim2','metric'] + dim1_keys=['${firstDim1}']`,
      },
      {
        title: '查看单个三维点的完整属性列',
        description: `固定 ${axisOptions.dim1.label} = ${axisOptions.dim1.title(firstDim1)} 且 ${axisOptions.dim2.label} = ${axisOptions.dim2.title(firstDim2)}，只保留 ${axisOptions.metric.label}`,
        tool: `read_3d_table keep_axes=['metric'] + dim1_keys=['${firstDim1}'] + dim2_keys=['${firstDim2}']`,
      },
    ]
  }, [axisOptions, dim1Keys, dim2Keys, metricKeys])

  const relevantFormulaEntries = useMemo(() => {
    if (!fixedAxis) return formulaEntries
    if (fixedAxis === 'metric' && fixedValue) {
      return formulaEntries.filter(({ col }) => col.key === fixedValue)
    }
    if (rowAxis === 'metric' || colAxis === 'metric') return formulaEntries
    return formulaEntries
  }, [fixedAxis, fixedValue, formulaEntries, rowAxis, colAxis])

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

  if (loading) return <div className="muted small" style={{ padding: '1rem' }}>加载三维表中…</div>
  if (err) return <div className="err small" style={{ padding: '1rem' }}>加载失败：{err}</div>
  if (!snapshot || !axisOptions || !fixedAxis) return null

  const rowKeys = axisOptions[rowAxis].keys
  const colKeys = axisOptions[colAxis].keys
  const fixedKeys = axisOptions[fixedAxis].keys
  const effectiveFixedValue = fixedValue && fixedKeys.includes(fixedValue) ? fixedValue : (fixedKeys[0] ?? null)

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
        <span className="matrix-meta-chip muted">
          {snapshot.dim1.display_name || snapshot.dim1.col_name} × {snapshot.dim2.display_name || snapshot.dim2.col_name} × 属性列
        </span>
        <span className="matrix-meta-chip muted">{snapshot.row_count} 组合</span>
        <span className="matrix-meta-chip muted">{snapshot.cols.length} 属性列</span>
        {formulaEntries.length > 0 && <span className="matrix-meta-chip mono">{formulaEntries.length} 个公式列</span>}
        {canRecalculate && formulaEntries.length > 0 && (
          <button type="button" className="btn tiny" onClick={() => void recalculateTable()} disabled={recalculating}>
            {recalculating ? '重算中…' : '重算公式'}
          </button>
        )}
      </div>

      {sliceExamples.length > 0 && (
        <div className="panel" style={{ padding: '0.75rem 0.9rem', marginBottom: '0.75rem' }}>
          <div className="muted small" style={{ marginBottom: '0.45rem' }}>
            前端的“行轴 / 列轴 / 第三维”与 AI 工具 <code>read_3d_table</code> 的切片语义一致：保留的两个轴就是 <code>keep_axes</code>，固定项就是其余维度的 key 过滤。
          </div>
          <div style={{ display: 'grid', gap: '0.45rem' }}>
            {sliceExamples.map((example) => (
              <div key={example.tool}>
                <strong>{example.title}</strong>
                <div className="muted small">{example.description}</div>
                <code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{example.tool}</code>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="matrix-level-tabs" style={{ marginBottom: '0.5rem', flexWrap: 'wrap' }}>
        <span className="muted small" style={{ marginRight: '0.5rem' }}>行轴：</span>
        {(['dim1', 'dim2', 'metric'] as AxisKey[]).map((axis) => (
          <button
            key={`row-${axis}`}
            type="button"
            className={`btn tiny${rowAxis === axis ? ' primary' : ''}`}
            onClick={() => pickRowAxis(axis)}
            style={{ marginRight: 4, marginBottom: 4 }}
          >
            {axisOptions[axis].label}
          </button>
        ))}
      </div>

      <div className="matrix-level-tabs" style={{ marginBottom: '0.5rem', flexWrap: 'wrap' }}>
        <span className="muted small" style={{ marginRight: '0.5rem' }}>列轴：</span>
        {(['dim1', 'dim2', 'metric'] as AxisKey[]).map((axis) => (
          <button
            key={`col-${axis}`}
            type="button"
            className={`btn tiny${colAxis === axis ? ' primary' : ''}`}
            onClick={() => pickColAxis(axis)}
            disabled={axis === rowAxis}
            style={{ marginRight: 4, marginBottom: 4 }}
          >
            {axisOptions[axis].label}
          </button>
        ))}
      </div>

      <div className="matrix-level-tabs" style={{ marginBottom: '0.75rem', flexWrap: 'wrap', alignItems: 'center' }}>
        <span className="muted small" style={{ marginRight: '0.5rem' }}>第三维：</span>
        <span className="matrix-meta-chip muted" style={{ marginRight: '0.5rem' }}>{axisOptions[fixedAxis].label}</span>
        <select
          value={effectiveFixedValue || ''}
          onChange={(e) => setFixedValue(e.target.value)}
          style={{ minWidth: 240 }}
        >
          {fixedKeys.map((key) => (
            <option key={key} value={key}>
              {axisOptions[fixedAxis].title(key)} · {axisOptions[fixedAxis].subTitle(key)}
            </option>
          ))}
        </select>
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

      {relevantFormulaEntries.length > 0 && (
        <div style={{ marginTop: '1rem' }}>
          <h4 style={{ marginBottom: '0.5rem' }}>当前视图相关公式</h4>
          <div style={{ display: 'grid', gap: '0.5rem' }}>
            {relevantFormulaEntries.map(({ col, formula }) => (
              <div key={col.key} className="panel" style={{ padding: '0.75rem' }}>
                <div style={{ marginBottom: '0.25rem' }}>
                  <strong>{col.display_name || col.key}</strong>
                  <span className="muted small" style={{ marginLeft: '0.5rem' }}>{formula.type}</span>
                </div>
                <code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{formula.formula}</code>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
