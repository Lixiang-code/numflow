/**
 * Matrix 表只读可视化编辑器。
 * matrix_resource 在 scale_mode='fallback' 下把第三维改为“公式维”：
 * 二维基准值仍直接展示；指定等级时，后端会优先返回公式计算结果。
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
}: {
  tableName: string
  matrixMeta: Record<string, unknown>
  headers: Record<string, string>
  glossary: GlossaryItem[]
}) {
  const [snapshot, setSnapshot] = useState<MatrixSnapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  const [activeLevel, setActiveLevel] = useState<string | null>(null)
  const [previewLevel, setPreviewLevel] = useState<string>('')
  const [pendingLevel, setPendingLevel] = useState<string>('')

  const glossaryMap = new Map(glossary.map((g) => [g.term_en, g.term_zh]))
  const meta = matrixMeta as Partial<MatrixMeta>
  const kind = meta.kind || ''
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
    setLoading(true)
    setErr(null)
    const query = previewLevel ? `?level=${encodeURIComponent(previewLevel)}` : ''
    apiFetch(`/meta/matrix/${encodeURIComponent(tableName)}${query}`, { headers })
      .then((d) => {
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
      })
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [tableName, headers, previewLevel])

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

  return (
    <div className="matrix-editor">
      <div className="matrix-topbar">
        <span className="matrix-kind-badge">{kind}</span>
        {valueDtype && <span className="matrix-meta-chip">{valueDtype}</span>}
        {valueFormat && <span className="matrix-meta-chip mono">{valueFormat}</span>}
        {directory && <span className="matrix-meta-chip muted">📁 {directory}</span>}
        <span className="matrix-meta-chip muted small">
          {rows.length} 行 × {cols.length} 列
        </span>
        {formulaEntries.length > 0 && (
          <span className="matrix-meta-chip mono">{formulaEntries.length} 个第三维公式</span>
        )}
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
                  return (
                    <td
                      key={c}
                      className={`matrix-cell${isEmpty ? ' matrix-cell-empty' : ''}${hasNote ? ' matrix-cell-noted' : ''}`}
                      title={formula?.formula || (hasNote ? note : undefined)}
                    >
                      {isEmpty ? (
                        <span className="muted">—</span>
                      ) : (
                        <span>{formatVal(val, valueFormat)}</span>
                      )}
                      {formula && (
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

      {formulaEntries.length > 0 && (
        <div style={{ marginTop: '1rem' }}>
          <h4 style={{ marginBottom: '0.5rem' }}>第三维公式</h4>
          <div style={{ display: 'grid', gap: '0.5rem' }}>
            {formulaEntries.map(({ row, col, formula }) => (
              <div key={`${row}-${col}`} className="panel" style={{ padding: '0.75rem' }}>
                <div style={{ marginBottom: '0.25rem' }}>
                  <strong>{rowDisplayMap.get(row) || termDisplay(row)}</strong>
                  <span className="muted small"> / {colDisplayMap.get(col) || termDisplay(col)}</span>
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
