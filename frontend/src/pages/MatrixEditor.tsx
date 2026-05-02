/**
 * Matrix / 伪三维表可视化编辑器（含单元格内联编辑 + 公式面板）。
 * matrix_resource 在 scale_mode='fallback' 下把第三维改为"公式维"。
 */
import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '../api'

type MatrixMeta = {
  table_name: string
  kind?: string
  scale_mode?: string
  row_axis: string
  col_axis: string
  rows: string[]
  cols: string[]
  levels: number[]
  value_dtype?: string
  value_format?: string
  directory?: string
}

type CellData = {
  value: number | null
  note?: string | null
  source?: string | null
}

type FormulaInfo = {
  formula: string
  type: string
}

type MatrixData = Record<string, Record<string, Record<string, CellData>>>
type MatrixFormulaCells = Record<string, Record<string, FormulaInfo>>

type MatrixSnapshot = {
  ok: boolean
  table_name: string
  kind?: string
  row_axis: string
  col_axis: string
  rows: string[]
  cols: string[]
  levels: number[]
  preview_level?: number | null
  formula_cells?: MatrixFormulaCells
  data: MatrixData
}

type GlossaryItem = { term_en: string; term_zh: string }

function formatVal(v: number | null | undefined, fmt?: string): string {
  if (v == null) return '—'
  if (!fmt) return String(v)
  if (fmt.endsWith('%')) {
    const decimals = (fmt.match(/\.(\d+)%/) || ['', '0'])[1].length
    return (v * 100).toFixed(decimals) + '%'
  }
  const dec = (fmt.match(/\.(\d+)/) || ['', '2'])[1].length
  return v.toFixed(dec)
}

export default function MatrixEditor({
  tableName,
  matrixMeta,
  headers,
  glossary,
  canWrite = false,
}: {
  tableName: string
  matrixMeta: Record<string, unknown>
  headers: Record<string, string>
  glossary: GlossaryItem[]
  canWrite?: boolean
}) {
  const [snapshot, setSnapshot] = useState<MatrixSnapshot | null>(null)
  const [loadedRequestKey, setLoadedRequestKey] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [activeLevel, setActiveLevel] = useState<string | null>(null)
  const [previewLevel, setPreviewLevel] = useState<string>('')
  const [pendingLevel, setPendingLevel] = useState<string>('')
  const [showFormulaPanel, setShowFormulaPanel] = useState(false)
  const [editingCell, setEditingCell] = useState<{ row: string; col: string; level: string } | null>(null)
  const [editingCellValue, setEditingCellValue] = useState('')
  const [cellSaving, setCellSaving] = useState(false)
  const requestKey = `${tableName}::${previewLevel}`

  const glossaryMap = new Map(glossary.map((g) => [g.term_en, g.term_zh]))
  const meta = matrixMeta as Partial<MatrixMeta>
  const kind = meta.kind || ''
  const kindLabel = kind === 'matrix_resource' ? '伪三维表' : kind === 'matrix_attr' ? '矩阵表' : kind || '矩阵表'
  const valueFormat = meta.value_format || ''
  const valueDtype = meta.value_dtype || ''
  const directory = meta.directory || ''

  const metaRows: Array<{ key: string; display_name?: string; brief?: string }> =
    (matrixMeta.rows as Array<{ key: string; display_name?: string; brief?: string }>) || []
  const metaCols: Array<{ key: string; display_name?: string; brief?: string }> =
    (matrixMeta.cols as Array<{ key: string; display_name?: string; brief?: string }>) || []

  const rowDisplayMap = new Map(metaRows.map((r) => [r.key, r.display_name || r.key]))
  const colDisplayMap = new Map(metaCols.map((c) => [c.key, c.display_name || c.key]))
  const rowBriefMap = new Map(metaRows.map((r) => [r.key, r.brief || '']))
  const colBriefMap = new Map(metaCols.map((c) => [c.key, c.brief || '']))

  useEffect(() => {
    let cancelled = false
    const query = previewLevel ? `?level=${encodeURIComponent(previewLevel)}` : ''
    apiFetch(`/meta/matrix/${encodeURIComponent(tableName)}${query}`, { headers })
      .then((d) => {
        if (cancelled) return
        const snap = d as MatrixSnapshot
        setSnapshot(snap)
        const appliedLevel = snap.preview_level != null ? String(snap.preview_level) : ''
        setPendingLevel(appliedLevel)
        const lvls = snap.levels || []
        setActiveLevel(
          appliedLevel ||
            (lvls.length > 0
              ? String(lvls[0])
              : snap.data
              ? Object.values(snap.data)
                  .flatMap((cols) => Object.values(cols).flatMap((lv) => Object.keys(lv)))
                  .find((l) => l !== '_') || '_'
                : '_'),
        )
        setErr(null)
        setLoadedRequestKey(requestKey)
      })
      .catch((e) => {
        if (cancelled) return
        setErr(String(e))
        setLoadedRequestKey(requestKey)
      })
    return () => {
      cancelled = true
    }
  }, [headers, previewLevel, requestKey, tableName])

  const refreshSnapshot = async () => {
    const query = previewLevel ? `?level=${encodeURIComponent(previewLevel)}` : ''
    const snap = (await apiFetch(`/meta/matrix/${encodeURIComponent(tableName)}${query}`, { headers })) as MatrixSnapshot
    setSnapshot(snap)
    setErr(null)
    setLoadedRequestKey(requestKey)
  }

  const formulaEntries = useMemo(() => {
    const out: Array<{ row: string; col: string; formula: FormulaInfo }> = []
    const cells = snapshot?.formula_cells || {}
    for (const [rowKey, cols] of Object.entries(cells)) {
      for (const [colKey, info] of Object.entries(cols)) {
        out.push({ row: rowKey, col: colKey, formula: info })
      }
    }
    return out
  }, [snapshot])

  const loading = loadedRequestKey !== requestKey

  if (loading) return <div className="muted small" style={{ padding: '1rem' }}>加载 Matrix 数据中…</div>
  if (err) return <div className="err small" style={{ padding: '1rem' }}>加载失败：{err}</div>
  if (!snapshot) return null

  const rows = snapshot.rows.length > 0 ? snapshot.rows : Object.keys(snapshot.data)
  const cols =
    snapshot.cols.length > 0
      ? snapshot.cols
      : rows.length > 0
      ? Object.keys(snapshot.data[rows[0]] || {})
      : []

  const levelKeys: string[] = []
  if (snapshot.levels && snapshot.levels.length > 0) {
    for (const lv of snapshot.levels) levelKeys.push(String(lv))
  } else {
    const lvSet = new Set<string>()
    for (const r of rows) {
      for (const c of cols) {
        const cell = snapshot.data?.[r]?.[c]
        if (!cell) continue
        for (const lk of Object.keys(cell)) lvSet.add(lk)
      }
    }
    levelKeys.push(...Array.from(lvSet).sort((a, b) => {
      const an = a === '_' ? -1 : Number(a)
      const bn = b === '_' ? -1 : Number(b)
      return an - bn
    }))
  }

  const displayLevel = previewLevel || activeLevel || levelKeys[0] || '_'

  function getCellVal(row: string, col: string): CellData | null {
    const cell = snapshot?.data?.[row]?.[col]
    if (!cell) return null
    return cell[displayLevel] ?? cell['_'] ?? null
  }

  function termDisplay(key: string): string {
    return glossaryMap.get(key) || rowDisplayMap.get(key) || colDisplayMap.get(key) || key
  }

  async function saveCellEdit(r: string, c: string, level: string) {
    if (!editingCellValue.trim() || cellSaving) return
    setCellSaving(true)
    setErr(null)
    try {
      const numVal = Number(editingCellValue)
      const lvKey = level === '_' ? 'na' : level
      const rid = `${r}__${c}__${lvKey}`
      const rowAxisCol = snapshot?.row_axis || ''
      const colAxisCol = snapshot?.col_axis || ''
      await apiFetch('/data/cells/write', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          table_name: tableName,
          updates: [
            { row_id: rid, column: 'value', value: isNaN(numVal) ? editingCellValue : numVal },
            { row_id: rid, column: rowAxisCol, value: r },
            { row_id: rid, column: colAxisCol, value: c },
            { row_id: rid, column: 'level', value: level === '_' ? null : Number(level) },
          ],
          source_tag: 'ai_generated',
        }),
      })
      await apiFetch('/data/cells/mark-manual', {
        method: 'POST',
        headers,
        body: JSON.stringify({ table_name: tableName, row_id: rid, column: 'value' }),
      })
      // 触发连锁重算
      if (snapshot?.formula_cells && Object.keys(snapshot.formula_cells).length > 0) {
        await apiFetch(`/compute/column-formula/recalculate-table?table_name=${encodeURIComponent(tableName)}`, {
          method: 'POST', headers,
        }).catch(() => {})
      }
      setEditingCell(null)
      setEditingCellValue('')
      await refreshSnapshot()
    } catch (e) {
      setErr(String(e))
    } finally {
      setCellSaving(false)
    }
  }

  return (
    <div className="matrix-editor">
      <div className="matrix-topbar">
        <span className="matrix-kind-badge">{kindLabel}</span>
        {valueDtype && <span className="matrix-meta-chip">{valueDtype}</span>}
        {valueFormat && <span className="matrix-meta-chip mono">{valueFormat}</span>}
        {directory && <span className="matrix-meta-chip muted">📁 {directory}</span>}
        <span className="matrix-meta-chip" style={{ background: '#e3f2fd', borderColor: '#bbdefb', color: '#1565c0' }}>
          {rows.length} 行 × {cols.length} 列
        </span>
        <div style={{ flex: 1 }} />
        <button
          type="button"
          className={`btn tiny${showFormulaPanel ? ' primary' : ''}`}
          onClick={() => setShowFormulaPanel(!showFormulaPanel)}
        >
          {showFormulaPanel ? '收起公式' : `公式 (${formulaEntries.length})`}
        </button>
      </div>

      {formulaEntries.length > 0 ? (
        <div className="matrix-level-tabs" style={{ flexWrap: 'wrap', alignItems: 'center' }}>
          <span className="muted small" style={{ marginRight: '0.5rem' }}>第三维（等级）查看：</span>
          <button
            type="button"
            className={`btn tiny${previewLevel === '' ? ' primary' : ''}`}
            onClick={() => { setPreviewLevel(''); setPendingLevel('') }}
            style={{ marginRight: 4 }}
          >
            仅看基准值
          </button>
          <input
            type="number"
            min={0}
            step={1}
            value={pendingLevel}
            onChange={(e) => setPendingLevel(e.target.value)}
            placeholder="输入等级"
            style={{ width: 120, marginRight: 6 }}
          />
          <button
            type="button"
            className="btn tiny"
            onClick={() => setPreviewLevel(pendingLevel.trim())}
          >
            查看该等级
          </button>
          {previewLevel && (
            <span className="matrix-meta-chip muted" style={{ marginLeft: 6 }}>
              当前切片：Lv{previewLevel}
            </span>
          )}
        </div>
      ) : levelKeys.length > 1 ? (
        <div className="matrix-level-tabs">
          <span className="muted small" style={{ marginRight: '0.5rem' }}>等级：</span>
          {levelKeys.map((lk) => (
            <button
              key={lk}
              type="button"
              className={`btn tiny${activeLevel === lk ? ' primary' : ''}`}
              onClick={() => setActiveLevel(lk)}
              style={{ marginRight: 3 }}
            >
              {lk === '_' ? '基准' : `Lv${lk}`}
            </button>
          ))}
        </div>
      ) : levelKeys.length === 0 ? (
        <div className="muted small" style={{ marginBottom: '0.5rem' }}>（无等级维度，仅基准值）</div>
      ) : null}

      <div className="matrix-scroll">
        <table className="matrix-table">
          <thead>
            <tr>
              <th className="matrix-corner">{snapshot.row_axis} \ {snapshot.col_axis}</th>
              {cols.map((c) => (
                <th
                  key={c}
                  title={colBriefMap.get(c) || c}
                  className="matrix-col-head"
                >
                  <span className="matrix-head-zh">{colDisplayMap.get(c) || termDisplay(c)}</span>
                  <span className="matrix-head-en muted">{c}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r}>
                <td
                  className="matrix-row-head"
                  title={rowBriefMap.get(r) || r}
                >
                  <span className="matrix-head-zh">{rowDisplayMap.get(r) || termDisplay(r)}</span>
                  <span className="matrix-head-en muted">{r}</span>
                </td>
                {cols.map((c) => {
                  const cell = getCellVal(r, c)
                  const val = cell?.value ?? null
                  const note = cell?.note || ''
                  const formula = snapshot.formula_cells?.[r]?.[c]
                  const hasNote = Boolean(note)
                  const isEmpty = val == null
                  const hasFormula = Boolean(formula)
                  const isEditingCell = editingCell?.row === r && editingCell?.col === c && editingCell?.level === displayLevel
                  const cellEditable = canWrite && !hasFormula && !cellSaving
                  return (
                    <td
                      key={c}
                      className={`matrix-cell${isEmpty ? ' matrix-cell-empty' : ''}${hasNote ? ' matrix-cell-noted' : ''}${cellEditable ? ' matrix-cell-editable' : ''}`}
                      title={formula?.formula || (hasNote ? note : cellEditable ? '点击编辑' : undefined)}
                      onClick={() => {
                        if (!cellEditable) return
                        setEditingCell({ row: r, col: c, level: displayLevel })
                        setEditingCellValue(val != null ? String(val) : '')
                      }}
                    >
                      {isEditingCell ? (
                        <input
                          className="matrix-cell-edit-input"
                          value={editingCellValue}
                          onChange={(e) => setEditingCellValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') { e.preventDefault(); void saveCellEdit(r, c, displayLevel) }
                            if (e.key === 'Escape') { setEditingCell(null); setEditingCellValue('') }
                          }}
                          onBlur={() => { setEditingCell(null); setEditingCellValue('') }}
                          autoFocus
                        />
                      ) : isEmpty ? (
                        <span className="muted">—</span>
                      ) : (
                        <span>{formatVal(val, valueFormat)}</span>
                      )}
                      {!isEditingCell && formula && (
                        <div className="muted small" style={{ marginTop: 4 }}>
                          {cell?.source === 'formula' ? '公式切片' : '有公式'}
                        </div>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showFormulaPanel && (
        <div className="matrix-formula-panel">
          <div className="matrix-formula-panel-header">
            <span>
              <strong>第三维公式</strong>
              <span className="muted small" style={{ marginLeft: 8 }}>{formulaEntries.length} 个单元格公式</span>
            </span>
            <button type="button" className="btn tiny" onClick={() => setShowFormulaPanel(false)}>收起</button>
          </div>
          {formulaEntries.length === 0 ? (
            <div className="muted small" style={{ padding: '0.75rem', textAlign: 'center' }}>暂无公式</div>
          ) : (
            <div className="matrix-formula-list">
              {formulaEntries.map(({ row, col, formula }) => (
                <div key={`${row}-${col}`} className="matrix-formula-item">
                  <div className="matrix-formula-item-header">
                    <strong>{rowDisplayMap.get(row) || termDisplay(row)}</strong>
                    <span className="muted small"> / {colDisplayMap.get(col) || termDisplay(col)}</span>
                    <span className={`matrix-formula-type-badge matrix-formula-type-${formula.type}`}>
                      {formula.type}
                    </span>
                  </div>
                  <code className="matrix-formula-text">{formula.formula}</code>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
