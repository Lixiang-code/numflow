import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'

type SkillModule = {
  id?: number
  module_key?: string
  title: string
  content: string
  required: boolean
  enabled: boolean
  sort_order: number
}

type SkillItem = {
  id?: number
  slug?: string
  title: string
  step_id: string
  summary: string
  description: string
  source: string
  default_exposed: boolean
  enabled: boolean
  usage_count: number
  generated_file_path?: string
  generated_content?: string
  modules: SkillModule[]
}

function cloneSkill<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function blankSkill(): SkillItem {
  return {
    title: '',
    slug: '',
    step_id: '',
    summary: '',
    description: '',
    source: 'user',
    default_exposed: false,
    enabled: true,
    usage_count: 0,
    generated_file_path: '',
    generated_content: '',
    modules: [],
  }
}

export default function SkillLibrary() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  const headers = useMemo(() => projectHeaders(pid), [pid])
  const [skills, setSkills] = useState<SkillItem[]>([])
  const [editingId, setEditingId] = useState<number | 'new' | null>(null)
  const [draft, setDraft] = useState<SkillItem | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [showAllModules, setShowAllModules] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const loadSkills = useCallback(async (selectId?: number | 'new' | null, fromEffect = false) => {
    if (!pid) return
    if (!fromEffect) {
      setLoading(true)
      setErr(null)
    }
    try {
      const data = await apiFetch('/skills', { headers }) as { skills?: SkillItem[] }
      const next = data.skills ?? []
      setSkills(next)
      if (selectId === 'new') {
        setEditingId('new')
        setDraft(blankSkill())
      } else if (typeof selectId === 'number') {
        const found = next.find((s) => s.id === selectId)
        if (found) {
          setEditingId(selectId)
          setDraft(cloneSkill(found))
        }
      } else if (next.length > 0) {
        const first = next[0]
        setEditingId(first.id ?? null)
        setDraft(cloneSkill(first))
      } else {
        setEditingId('new')
        setDraft(blankSkill())
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [headers, pid])

  useEffect(() => {
    void loadSkills(undefined, true)
  }, [loadSkills])

  function selectSkill(skillId: number) {
    const found = skills.find((s) => s.id === skillId)
    if (!found) return
    setEditingId(skillId)
    setDraft(cloneSkill(found))
    setShowAllModules(false)
  }

  function createSkill() {
    setEditingId('new')
    setDraft(blankSkill())
    setShowAllModules(true)
  }

  function updateDraft<K extends keyof SkillItem>(key: K, value: SkillItem[K]) {
    setDraft((cur) => (cur ? { ...cur, [key]: value } : cur))
  }

  function updateModule(index: number, patch: Partial<SkillModule>) {
    setDraft((cur) => {
      if (!cur) return cur
      const modules = cur.modules.map((item, idx) => {
        if (idx !== index) return item
        const next = { ...item, ...patch }
        if (next.required) next.enabled = true
        return next
      })
      return { ...cur, modules }
    })
  }

  function addModule() {
    setDraft((cur) => {
      if (!cur) return cur
      const next: SkillModule = {
        title: '新模块',
        module_key: '',
        content: '',
        required: false,
        enabled: false,
        sort_order: cur.modules.length + 1,
      }
      return { ...cur, modules: [...cur.modules, next] }
    })
    setShowAllModules(true)
  }

  function removeModule(index: number) {
    setDraft((cur) => {
      if (!cur) return cur
      return {
        ...cur,
        modules: cur.modules.filter((_, idx) => idx !== index).map((item, idx) => ({ ...item, sort_order: idx + 1 })),
      }
    })
  }

  async function saveSkill() {
    if (!draft) return
    setSaving(true)
    setErr(null)
    try {
      const body = {
        slug: draft.slug ?? '',
        title: draft.title,
        step_id: draft.step_id,
        summary: draft.summary,
        description: draft.description,
        source: draft.source || 'user',
        default_exposed: draft.default_exposed,
        enabled: draft.enabled,
        modules: draft.modules.map((item, idx) => ({
          ...item,
          module_key: item.module_key || item.title,
          sort_order: idx + 1,
        })),
      }
      const saved = await apiFetch(
        draft.id ? `/skills/${draft.id}` : '/skills',
        { method: draft.id ? 'PUT' : 'POST', headers, body: JSON.stringify(body) },
      ) as SkillItem
      await loadSkills(saved.id ?? null)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  async function regenerateSkill() {
    if (!draft?.id) return
    setGenerating(true)
    setErr(null)
    try {
      const result = await apiFetch(`/skills/${draft.id}/generate`, { method: 'POST', headers }) as {
        generated_file_path?: string
        generated_content?: string
      }
      setDraft((cur) => cur ? { ...cur, ...result } : cur)
      setSkills((cur) => cur.map((item) => (
        item.id === draft.id ? { ...item, ...result } : item
      )))
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setGenerating(false)
    }
  }

  const visibleModules = useMemo(() => {
    if (!draft) return []
    return draft.modules.filter((item) => showAllModules || item.required || item.enabled)
  }, [draft, showAllModules])

  const hiddenOptionalCount = useMemo(() => {
    if (!draft || showAllModules) return 0
    return draft.modules.filter((item) => !item.required && !item.enabled).length
  }, [draft, showAllModules])

  return (
    <div className="workbench" style={{ gap: '1rem' }}>
      <header className="wb-top">
        <Link to={`/workbench/${pid}`} className="link-btn">
          返回工作台
        </Link>
        <button type="button" className="link-btn" onClick={() => void loadSkills(editingId)}>
          刷新
        </button>
        <button type="button" className="link-btn" onClick={createSkill}>
          新增 SKILL
        </button>
        <button type="button" className="link-btn" disabled={!draft || saving} onClick={() => void saveSkill()}>
          {saving ? '保存中…' : '保存 SKILL'}
        </button>
        <button type="button" className="link-btn" disabled={!draft?.id || generating} onClick={() => void regenerateSkill()}>
          {generating ? '生成中…' : '生成实际文件'}
        </button>
        <span className="muted">项目 #{pid}</span>
      </header>

      {err && <p className="err banner">{err}</p>}
      {loading && <p className="muted" style={{ margin: '0 1rem' }}>加载中…</p>}

      <div style={{ display: 'grid', gridTemplateColumns: '320px minmax(0, 1fr)', gap: '1rem', padding: '0 1rem 1rem' }}>
        <aside style={{ border: '1px solid #e6e8eb', borderRadius: 12, background: '#fff', overflow: 'hidden' }}>
          <div style={{ padding: '0.9rem 1rem', borderBottom: '1px solid #eef1f4', fontWeight: 700 }}>
            [SKILL] 库
          </div>
          <div style={{ maxHeight: 'calc(100vh - 180px)', overflow: 'auto' }}>
            {skills.map((skill) => {
              const active = skill.id === editingId
              return (
                <button
                  key={skill.id ?? skill.slug}
                  type="button"
                  onClick={() => skill.id && selectSkill(skill.id)}
                  style={{
                    width: '100%',
                    textAlign: 'left',
                    padding: '0.85rem 1rem',
                    border: 0,
                    borderBottom: '1px solid #f1f3f5',
                    background: active ? 'rgba(64, 158, 255, 0.1)' : '#fff',
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.75rem', alignItems: 'center' }}>
                    <strong style={{ fontSize: '0.95rem' }}>{skill.title}</strong>
                    <span className="muted" style={{ fontSize: '0.74rem' }}>调用 {skill.usage_count}</span>
                  </div>
                  <div className="muted" style={{ fontSize: '0.75rem', marginTop: '0.3rem' }}>
                    {skill.step_id || '未绑定步骤'}
                  </div>
                  <div style={{ display: 'flex', gap: '0.4rem', marginTop: '0.45rem', flexWrap: 'wrap' }}>
                    <span style={{ fontSize: '0.7rem', padding: '0.12rem 0.4rem', borderRadius: 999, background: '#f4f6f8' }}>
                      {skill.source === 'system' ? '默认' : '用户'}
                    </span>
                    {skill.default_exposed && (
                      <span style={{ fontSize: '0.7rem', padding: '0.12rem 0.4rem', borderRadius: 999, background: 'rgba(39,174,96,.14)', color: '#1b7f43' }}>
                        默认暴露
                      </span>
                    )}
                    {!skill.enabled && (
                      <span style={{ fontSize: '0.7rem', padding: '0.12rem 0.4rem', borderRadius: 999, background: 'rgba(235,87,87,.12)', color: '#c0392b' }}>
                        已停用
                      </span>
                    )}
                  </div>
                </button>
              )
            })}
          </div>
        </aside>

        <main style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          {draft ? (
            <>
              <section style={{ border: '1px solid #e6e8eb', borderRadius: 12, background: '#fff', padding: '1rem' }}>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.9rem' }}>
                  <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                    <span>标题</span>
                    <input value={draft.title} onChange={(e) => updateDraft('title', e.target.value)} />
                  </label>
                  <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                    <span>Slug</span>
                    <input value={draft.slug ?? ''} onChange={(e) => updateDraft('slug', e.target.value)} />
                  </label>
                  <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                    <span>绑定步骤 ID</span>
                    <input value={draft.step_id} onChange={(e) => updateDraft('step_id', e.target.value)} />
                  </label>
                  <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                    <span>来源</span>
                    <input value={draft.source} onChange={(e) => updateDraft('source', e.target.value)} />
                  </label>
                </div>

                <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem', marginTop: '0.9rem' }}>
                  <span>摘要</span>
                  <textarea value={draft.summary} onChange={(e) => updateDraft('summary', e.target.value)} rows={3} />
                </label>

                <label style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem', marginTop: '0.9rem' }}>
                  <span>说明</span>
                  <textarea value={draft.description} onChange={(e) => updateDraft('description', e.target.value)} rows={4} />
                </label>

                <div style={{ display: 'flex', gap: '1rem', marginTop: '0.9rem', flexWrap: 'wrap' }}>
                  <label style={{ display: 'flex', gap: '0.4rem', alignItems: 'center' }}>
                    <input
                      type="checkbox"
                      checked={draft.default_exposed}
                      onChange={(e) => updateDraft('default_exposed', e.target.checked)}
                    />
                    默认暴露给 AI
                  </label>
                  <label style={{ display: 'flex', gap: '0.4rem', alignItems: 'center' }}>
                    <input
                      type="checkbox"
                      checked={draft.enabled}
                      onChange={(e) => updateDraft('enabled', e.target.checked)}
                    />
                    启用
                  </label>
                  <span className="muted">被调用次数：{draft.usage_count}</span>
                </div>
              </section>

              <section style={{ border: '1px solid #e6e8eb', borderRadius: 12, background: '#fff', padding: '1rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', marginBottom: '0.8rem' }}>
                  <div>
                    <strong>内容模块</strong>
                    <div className="muted" style={{ fontSize: '0.78rem', marginTop: '0.2rem' }}>
                      默认仅显示必要模块与已启用的可选模块。
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: '0.6rem' }}>
                    <button type="button" className="btn tiny" onClick={() => setShowAllModules((v) => !v)}>
                      {showAllModules ? '仅看必要/启用模块' : '显示全部模块'}
                    </button>
                    <button type="button" className="btn tiny" onClick={addModule}>
                      新增模块
                    </button>
                  </div>
                </div>

                {!showAllModules && hiddenOptionalCount > 0 && (
                  <p className="muted" style={{ marginBottom: '0.8rem' }}>
                    当前还有 {hiddenOptionalCount} 个未启用的可选模块，可点击“显示全部模块”查看。
                  </p>
                )}

                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.9rem' }}>
                  {visibleModules.map((module) => {
                    const realIndex = draft.modules.indexOf(module)
                    return (
                      <div key={`${module.id ?? 'new'}-${realIndex}`} style={{ border: '1px solid #eef1f4', borderRadius: 10, padding: '0.9rem', background: module.required ? 'rgba(64,158,255,.05)' : '#fafbfc' }}>
                        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto auto auto', gap: '0.6rem', alignItems: 'center' }}>
                          <input
                            value={module.title}
                            onChange={(e) => updateModule(realIndex, { title: e.target.value })}
                            placeholder="模块标题"
                          />
                          <label style={{ display: 'flex', gap: '0.35rem', alignItems: 'center', fontSize: '0.82rem' }}>
                            <input
                              type="checkbox"
                              checked={module.required}
                              onChange={(e) => updateModule(realIndex, { required: e.target.checked, enabled: e.target.checked ? true : module.enabled })}
                            />
                            必要
                          </label>
                          <label style={{ display: 'flex', gap: '0.35rem', alignItems: 'center', fontSize: '0.82rem', opacity: module.required ? 0.5 : 1 }}>
                            <input
                              type="checkbox"
                              checked={module.enabled || module.required}
                              disabled={module.required}
                              onChange={(e) => updateModule(realIndex, { enabled: e.target.checked })}
                            />
                            启用
                          </label>
                          <button type="button" className="btn tiny" disabled={module.required} onClick={() => removeModule(realIndex)}>
                            删除
                          </button>
                        </div>
                        <textarea
                          style={{ marginTop: '0.7rem' }}
                          rows={7}
                          value={module.content}
                          onChange={(e) => updateModule(realIndex, { content: e.target.value })}
                          placeholder="模块内容（支持中文分块说明）"
                        />
                      </div>
                    )
                  })}
                </div>
              </section>

              <section style={{ border: '1px solid #e6e8eb', borderRadius: 12, background: '#fff', padding: '1rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center' }}>
                  <strong>实际 SKILL 文件</strong>
                  <span className="muted" style={{ fontSize: '0.8rem' }}>{draft.generated_file_path || '尚未生成'}</span>
                </div>
                <pre style={{ marginTop: '0.8rem', whiteSpace: 'pre-wrap', background: '#0f172a', color: '#e2e8f0', borderRadius: 10, padding: '0.9rem', maxHeight: '32rem', overflow: 'auto' }}>
                  {draft.generated_content || '保存后将自动生成 Markdown + YAML 头格式的实际 SKILL 文件。'}
                </pre>
              </section>
            </>
          ) : (
            <section style={{ border: '1px solid #e6e8eb', borderRadius: 12, background: '#fff', padding: '1rem' }}>
              <span className="muted">暂无可编辑的 SKILL。</span>
            </section>
          )}
        </main>
      </div>
    </div>
  )
}
