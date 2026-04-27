/**
 * Matrix 表只读可视化编辑器。
 * 行=row_axis_value，列=col_axis_value，每个 level 一张子表（tab 切换）。
 */
import { useEffect, useState } from 'react'
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
}

// data shape: { [rowKey]: { [colKey]: { [levelKey]: CellData } } }
type MatrixData = Record<string, Record<string, Record<string, CellData>>>

type MatrixSnapshot = {
  ok: boolean
  table_name: string
  row_axis: string
  col_axis: string
  rows: string[]
  cols: string[]
  levels: number[]
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

  // Build glossary lookup for display_name resolution
  const glossaryMap = new Map(glossary.map((g) => [g.term_en, g.term_zh]))

  // Parse meta
  const meta = matrixMeta as Partial<MatrixMeta>
  const kind = meta.kind || ''
  const valueFormat = meta.value_format || ''
  const valueDtype = meta.value_dtype || ''
  const directory = meta.directory || meta.directory || ''

  // Row/Col display info from meta
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
    apiFetch(`/meta/matrix/${encodeURIComponent(tableName)}`, { headers })
      .then((d) => {
        const snap = d as MatrixSnapshot
        setSnapshot(snap)
        const lvls = snap.levels || []
        setActiveLevel(
          lvls.length > 0
            ? String(lvls[0])
            : snap.data
            ? Object.values(snap.data)
                .flatMap((cols) => Object.values(cols).flatMap((lv) => Object.keys(lv)))
                .find((l) => l !== '_') || '_'
            : '_',
        )
      })
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [tableName, headers])

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

  // Determine available levels
  const levelKeys: string[] = []
  if (snapshot.levels && snapshot.levels.length > 0) {
    for (const lv of snapshot.levels) levelKeys.push(String(lv))
  } else {
    // derive from data
    const lvSet = new Set<string>()
    for (const r of rows) {
      for (const c of cols) {
        const cell = snapshot.data?.[r]?.[c]
        if (cell) {
          for (const lk of Object.keys(cell)) lvSet.add(lk)
        }
      }
    }
    levelKeys.push(...Array.from(lvSet).sort((a, b) => {
      const an = a === '_' ? -1 : Number(a)
      const bn = b === '_' ? -1 : Number(b)
      return an - bn
    }))
  }

  const displayLevel = activeLevel ?? levelKeys[0] ?? '_'

  function getCellVal(row: string, col: string): CellData | null {
    const cell = snapshot?.data?.[row]?.[col]
    if (!cell) return null
    // Try exact level key, then '_' (NULL)
    return cell[displayLevel] ?? cell['_'] ?? null
  }

  function termDisplay(key: string): string {
    return glossaryMap.get(key) || rowDisplayMap.get(key) || colDisplayMap.get(key) || key
  }

  return (
    <div className="matrix-editor">
      {/* Top info bar */}
      <div className="matrix-topbar">
        <span className="matrix-kind-badge">{kind}</span>
        {valueDtype && <span className="matrix-meta-chip">{valueDtype}</span>}
        {valueFormat && <span className="matrix-meta-chip mono">{valueFormat}</span>}
        {directory && <span className="matrix-meta-chip muted">📁 {directory}</span>}
        <span className="matrix-meta-chip muted small">
          {rows.length} 行 × {cols.length} 列
        </span>
      </div>

      {/* Level tabs */}
      {levelKeys.length > 1 && (
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
      )}
      {levelKeys.length === 0 && (
        <div className="muted small" style={{ marginBottom: '0.5rem' }}>（无等级维度，仅基准值）</div>
      )}

      {/* Matrix table */}
      <div className="matrix-scroll">
        <table className="matrix-table">
          <thead>
            <tr>
              <th className="matrix-corner">{snapshot.row_axis} \\ {snapshot.col_axis}</th>
              {cols.map((c) => (
                <th
                  key={c}
                  title={colBriefMap.get(c) || c}
                  className="matrix-col-head"
                >
                  <span className="matrix-head-zh">{colDisplayMap.get(c) || termDisplay(c)}</span>
                  <br />
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
                  <br />
                  <span className="matrix-head-en muted">{r}</span>
                </td>
                {cols.map((c) => {
                  const cell = getCellVal(r, c)
                  const val = cell?.value ?? null
                  const note = cell?.note || ''
                  const hasNote = Boolean(note)
                  const isEmpty = val == null
                  return (
                    <td
                      key={c}
                      className={`matrix-cell${isEmpty ? ' matrix-cell-empty' : ''}${hasNote ? ' matrix-cell-noted' : ''}`}
                      title={hasNote ? note : undefined}
                    >
                      {isEmpty ? (
                        <span className="muted">—</span>
                      ) : (
                        <span>{formatVal(val, valueFormat)}</span>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
