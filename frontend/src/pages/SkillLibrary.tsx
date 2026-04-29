import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'

type PromptModule = {
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
  modules: PromptModule[]
}

type PromptItem = {
  id?: number
  prompt_key: string
  title: string
  summary: string
  description: string
  reference_note: string
  enabled: boolean
  override?: boolean
  display_order?: number
  modules: PromptModule[]
}

type PromptTab = 'skill' | 'system' | 'tool'

type ToastItem = { id: number; type: 'success' | 'error'; message: string }

function cloneValue<T>(value: T): T {
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

function blankPrompt(): PromptItem {
  return {
    prompt_key: '',
    title: '',
    summary: '',
    description: '',
    reference_note: '',
    enabled: true,
    override: false,
    modules: [],
  }
}

function renderModules(modules: PromptModule[]): string {
  return modules
    .filter((module) => module.required || module.enabled)
    .map((module) => module.content.trim())
    .filter(Boolean)
    .join('\n')
}

export default function SkillLibrary() {
  const { projectId } = useParams()
  const pid = Number(projectId)
  const headers = useMemo(() => projectHeaders(pid), [pid])

  const [tab, setTab] = useState<PromptTab>('skill')

  const [skills, setSkills] = useState<SkillItem[]>([])
  const [editingSkillId, setEditingSkillId] = useState<number | 'new' | null>(null)
  const [skillDraft, setSkillDraft] = useState<SkillItem | null>(null)
  const [skillLoading, setSkillLoading] = useState(true)
  const [skillSaving, setSkillSaving] = useState(false)
  const [skillGenerating, setSkillGenerating] = useState(false)
  const [showAllSkillModules, setShowAllSkillModules] = useState(false)

  const [promptItems, setPromptItems] = useState<PromptItem[]>([])
  const [editingPromptKey, setEditingPromptKey] = useState<string | null>(null)
  const [promptDraft, setPromptDraft] = useState<PromptItem | null>(null)
  const [promptLoading, setPromptLoading] = useState(false)
  const [promptSaving, setPromptSaving] = useState(false)
  const [promptResetting, setPromptResetting] = useState(false)
  const [showAllPromptModules, setShowAllPromptModules] = useState(false)

  const [err, setErr] = useState<string | null>(null)
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const toastId = useRef(0)

  const pushToast = useCallback((type: ToastItem['type'], message: string) => {
    toastId.current += 1
    const id = toastId.current
    setToasts((cur) => [...cur, { id, type, message }])
    window.setTimeout(() => {
      setToasts((cur) => cur.filter((t) => t.id !== id))
    }, 2600)
  }, [])

  const loadSkills = useCallback(async (selectId?: number | 'new' | null, fromEffect = false) => {
    if (!pid) return
    if (!fromEffect) {
      setSkillLoading(true)
      setErr(null)
    }
    try {
      const data = (await apiFetch('/skills', { headers })) as { skills?: SkillItem[] }
      const next = data.skills ?? []
      setSkills(next)
      if (selectId === 'new') {
        setEditingSkillId('new')
        setSkillDraft(blankSkill())
      } else if (typeof selectId === 'number') {
        const found = next.find((item) => item.id === selectId)
        if (found) {
          setEditingSkillId(selectId)
          setSkillDraft(cloneValue(found))
        }
      } else if (next.length > 0) {
        const first = next[0]
        setEditingSkillId(first.id ?? null)
        setSkillDraft(cloneValue(first))
      } else {
        setEditingSkillId('new')
        setSkillDraft(blankSkill())
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSkillLoading(false)
    }
  }, [headers, pid])

  const loadPromptItems = useCallback(async (category: 'system' | 'tool', selectKey?: string | null, fromEffect = false) => {
    if (!pid) return
    if (!fromEffect) {
      setPromptLoading(true)
      setErr(null)
    }
    try {
      const data = (await apiFetch(`/prompts?category=${category}`, { headers })) as { items?: PromptItem[] }
      const next = data.items ?? []
      setPromptItems(next)
      if (selectKey) {
        const found = next.find((item) => item.prompt_key === selectKey)
        if (found) {
          setEditingPromptKey(found.prompt_key)
          setPromptDraft(cloneValue(found))
          return
        }
      }
      const first = next[0] ?? null
      setEditingPromptKey(first?.prompt_key ?? null)
      setPromptDraft(first ? cloneValue(first) : blankPrompt())
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setPromptLoading(false)
    }
  }, [headers, pid])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (tab === 'skill') {
        void loadSkills(undefined, true)
      } else {
        void loadPromptItems(tab, undefined, true)
      }
    }, 0)
    return () => window.clearTimeout(timer)
  }, [loadPromptItems, loadSkills, tab])

  function selectSkill(skillId: number) {
    const found = skills.find((item) => item.id === skillId)
    if (!found) return
    setEditingSkillId(skillId)
    setSkillDraft(cloneValue(found))
    setShowAllSkillModules(false)
  }

  function createSkill() {
    setEditingSkillId('new')
    setSkillDraft(blankSkill())
    setShowAllSkillModules(true)
  }

  function updateSkillDraft<K extends keyof SkillItem>(key: K, value: SkillItem[K]) {
    setSkillDraft((cur) => (cur ? { ...cur, [key]: value } : cur))
  }

  function updateSkillModule(index: number, patch: Partial<PromptModule>) {
    setSkillDraft((cur) => {
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

  function addSkillModule() {
    setSkillDraft((cur) => {
      if (!cur) return cur
      return {
        ...cur,
        modules: [
          ...cur.modules,
          {
            title: '新模块',
            module_key: '',
            content: '',
            required: false,
            enabled: false,
            sort_order: cur.modules.length + 1,
          },
        ],
      }
    })
    setShowAllSkillModules(true)
  }

  function removeSkillModule(index: number) {
    setSkillDraft((cur) => {
      if (!cur) return cur
      return {
        ...cur,
        modules: cur.modules
          .filter((_, idx) => idx !== index)
          .map((item, idx) => ({ ...item, sort_order: idx + 1 })),
      }
    })
  }

  const saveSkill = useCallback(async () => {
    if (!skillDraft) return
    setSkillSaving(true)
    setErr(null)
    try {
      const body = {
        slug: skillDraft.slug ?? '',
        title: skillDraft.title,
        step_id: skillDraft.step_id,
        summary: skillDraft.summary,
        description: skillDraft.description,
        source: skillDraft.source || 'user',
        default_exposed: skillDraft.default_exposed,
        enabled: skillDraft.enabled,
        modules: skillDraft.modules.map((item, idx) => ({
          ...item,
          module_key: item.module_key || item.title,
          sort_order: idx + 1,
        })),
      }
      const saved = (await apiFetch(skillDraft.id ? `/skills/${skillDraft.id}` : '/skills', {
        method: skillDraft.id ? 'PUT' : 'POST',
        headers,
        body: JSON.stringify(body),
      })) as SkillItem
      await loadSkills(saved.id ?? null)
      pushToast('success', `SKILL「${saved.title || skillDraft.title || '未命名'}」已保存`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `保存失败：${msg}`)
    } finally {
      setSkillSaving(false)
    }
  }, [headers, loadSkills, pushToast, skillDraft])

  async function regenerateSkill() {
    if (!skillDraft?.id) return
    setSkillGenerating(true)
    setErr(null)
    try {
      const result = (await apiFetch(`/skills/${skillDraft.id}/generate`, { method: 'POST', headers })) as {
        generated_file_path?: string
        generated_content?: string
      }
      setSkillDraft((cur) => (cur ? { ...cur, ...result } : cur))
      setSkills((cur) => cur.map((item) => (item.id === skillDraft.id ? { ...item, ...result } : item)))
      pushToast('success', '已生成实际 SKILL 文件')
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `生成失败：${msg}`)
    } finally {
      setSkillGenerating(false)
    }
  }

  function selectPrompt(promptKey: string) {
    const found = promptItems.find((item) => item.prompt_key === promptKey)
    if (!found) return
    setEditingPromptKey(promptKey)
    setPromptDraft(cloneValue(found))
    setShowAllPromptModules(false)
  }

  function updatePromptDraft<K extends keyof PromptItem>(key: K, value: PromptItem[K]) {
    setPromptDraft((cur) => (cur ? { ...cur, [key]: value } : cur))
  }

  function updatePromptModule(index: number, patch: Partial<PromptModule>) {
    setPromptDraft((cur) => {
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

  const savePrompt = useCallback(async () => {
    if (!promptDraft || tab === 'skill') return
    setPromptSaving(true)
    setErr(null)
    try {
      const category = tab
      await apiFetch(`/prompts/${category}/${encodeURIComponent(promptDraft.prompt_key)}`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({
          title: promptDraft.title,
          summary: promptDraft.summary,
          description: promptDraft.description,
          reference_note: promptDraft.reference_note,
          enabled: promptDraft.enabled,
          modules: promptDraft.modules.map((item, idx) => ({
            module_key: item.module_key || item.title,
            title: item.title,
            content: item.content,
            required: item.required,
            enabled: item.enabled || item.required,
            sort_order: idx + 1,
          })),
        }),
      })
      await loadPromptItems(category, promptDraft.prompt_key)
      pushToast('success', `提示词「${promptDraft.title || promptDraft.prompt_key}」已保存`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `保存失败：${msg}`)
    } finally {
      setPromptSaving(false)
    }
  }, [headers, loadPromptItems, promptDraft, pushToast, tab])

  async function resetPrompt() {
    if (!promptDraft || tab === 'skill') return
    if (!window.confirm('确认还原为系统默认内容？此操作不可撤销。')) return
    setPromptResetting(true)
    setErr(null)
    try {
      await apiFetch(`/prompts/${tab}/${encodeURIComponent(promptDraft.prompt_key)}`, {
        method: 'DELETE',
        headers,
      })
      await loadPromptItems(tab, promptDraft.prompt_key)
      pushToast('success', '已还原为系统默认内容')
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `还原失败：${msg}`)
    } finally {
      setPromptResetting(false)
    }
  }

  // Ctrl/Cmd + S 保存
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const isMac = navigator.platform.toLowerCase().includes('mac')
      const mod = isMac ? e.metaKey : e.ctrlKey
      if (mod && (e.key === 's' || e.key === 'S')) {
        e.preventDefault()
        if (tab === 'skill') {
          if (skillDraft && !skillSaving) void saveSkill()
        } else {
          if (promptDraft && !promptSaving) void savePrompt()
        }
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [promptDraft, promptSaving, savePrompt, saveSkill, skillDraft, skillSaving, tab])

  const visibleSkillModules = useMemo(() => {
    if (!skillDraft) return []
    return skillDraft.modules.filter((item) => showAllSkillModules || item.required || item.enabled)
  }, [showAllSkillModules, skillDraft])

  const hiddenSkillModuleCount = useMemo(() => {
    if (!skillDraft || showAllSkillModules) return 0
    return skillDraft.modules.filter((item) => !item.required && !item.enabled).length
  }, [showAllSkillModules, skillDraft])

  const visiblePromptModules = useMemo(() => {
    if (!promptDraft) return []
    return promptDraft.modules.filter((item) => showAllPromptModules || item.required || item.enabled)
  }, [promptDraft, showAllPromptModules])

  const hiddenPromptModuleCount = useMemo(() => {
    if (!promptDraft || showAllPromptModules) return 0
    return promptDraft.modules.filter((item) => !item.required && !item.enabled).length
  }, [promptDraft, showAllPromptModules])

  const runtimePreview = useMemo(() => renderModules(promptDraft?.modules || []), [promptDraft])

  const loading = tab === 'skill' ? skillLoading : promptLoading
  const isMac = typeof navigator !== 'undefined' && navigator.platform.toLowerCase().includes('mac')
  const shortcut = isMac ? '⌘S' : 'Ctrl+S'

  const canSave = tab === 'skill' ? !!skillDraft && !skillSaving : !!promptDraft && !promptSaving
  const saving = tab === 'skill' ? skillSaving : promptSaving
  const handleSave = () => {
    if (tab === 'skill') void saveSkill()
    else void savePrompt()
  }

  return (
    <div className="sl-page">
      <header className="sl-top">
        <Link to={`/workbench/${pid}`} className="link-btn">← 返回工作台</Link>
        <div className="sl-title">提示词库</div>
        <span className="muted">项目 #{pid}</span>
        <div className="sl-spacer" />
        <button
          type="button"
          className="link-btn"
          onClick={() => {
            if (tab === 'skill') void loadSkills(editingSkillId)
            else void loadPromptItems(tab, editingPromptKey)
          }}
        >
          刷新
        </button>
        {tab === 'skill' && (
          <>
            <button type="button" className="link-btn" onClick={createSkill}>+ 新增 SKILL</button>
            <button
              type="button"
              className="link-btn"
              disabled={!skillDraft?.id || skillGenerating}
              onClick={() => void regenerateSkill()}
            >
              {skillGenerating ? '生成中…' : '生成实际文件'}
            </button>
          </>
        )}
        {tab !== 'skill' && (
          <button
            type="button"
            className="link-btn"
            disabled={!promptDraft || promptResetting}
            onClick={() => void resetPrompt()}
          >
            {promptResetting ? '还原中…' : '还原默认'}
          </button>
        )}
        <button
          type="button"
          className="link-btn primary"
          disabled={!canSave}
          onClick={handleSave}
          title={`保存（${shortcut}）`}
        >
          {saving ? '保存中…' : '保存'} <kbd>{shortcut}</kbd>
        </button>
      </header>

      {err && <p className="sl-banner">{err}</p>}

      <div className="sl-body">
        <aside className="sl-sidebar">
          <div className="sl-sidebar-head">
            <div className="sl-h">提示词库</div>
            <div className="sl-sub">统一管理 SKILL、系统提示词、工具提示词</div>
          </div>
          <div className="sl-tabs">
            {([
              ['skill', 'SKILL'],
              ['system', '系统'],
              ['tool', '工具'],
            ] as Array<[PromptTab, string]>).map(([key, label]) => (
              <button
                key={key}
                type="button"
                onClick={() => setTab(key)}
                className={tab === key ? 'active' : ''}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="sl-list">
            {loading && <div className="sl-list-empty">加载中…</div>}
            {!loading && tab === 'skill' && skills.length === 0 && (
              <div className="sl-list-empty">暂无 SKILL，点击右上角"新增 SKILL"</div>
            )}
            {!loading && tab !== 'skill' && promptItems.length === 0 && (
              <div className="sl-list-empty">暂无可编辑的提示词</div>
            )}
            {tab === 'skill'
              ? skills.map((skill) => {
                  const active = skill.id === editingSkillId
                  return (
                    <button
                      key={skill.id ?? skill.slug}
                      type="button"
                      onClick={() => skill.id && selectSkill(skill.id)}
                      className={`sl-list-item${active ? ' active' : ''}`}
                    >
                      <div className="sl-row">
                        <span className="sl-name">{skill.title || '(未命名)'}</span>
                        <span className="sl-meta">调用 {skill.usage_count}</span>
                      </div>
                      <div className="sl-key">{skill.step_id || '未绑定步骤'}</div>
                      <div className="sl-chips">
                        <span className="sl-chip">{skill.source === 'system' ? '默认' : '用户'}</span>
                        {skill.default_exposed && <span className="sl-chip green">默认暴露</span>}
                        {!skill.enabled && <span className="sl-chip amber">已停用</span>}
                      </div>
                    </button>
                  )
                })
              : promptItems.map((item) => {
                  const active = item.prompt_key === editingPromptKey
                  return (
                    <button
                      key={item.prompt_key}
                      type="button"
                      onClick={() => selectPrompt(item.prompt_key)}
                      className={`sl-list-item${active ? ' active' : ''}`}
                    >
                      <div className="sl-row">
                        <span className="sl-name">{item.title}</span>
                        <span className="sl-meta">{item.override ? '已覆盖' : '默认'}</span>
                      </div>
                      <div className="sl-key">{item.prompt_key}</div>
                    </button>
                  )
                })}
          </div>
        </aside>

        <main className="sl-main">
          <div className="sl-main-inner">
            {tab === 'skill' ? (
              skillDraft ? (
                <>
                  <section className="sl-card">
                    <div className="sl-card-head">
                      <div>
                        <h3>基本信息</h3>
                        <div className="sl-sub">SKILL 元数据，将作为 Markdown 头部 YAML 写入。</div>
                      </div>
                    </div>
                    <div className="sl-grid cols-2">
                      <label className="sl-field">
                        <span className="sl-label">标题</span>
                        <input value={skillDraft.title} onChange={(e) => updateSkillDraft('title', e.target.value)} placeholder="例如：表格补全规则" />
                      </label>
                      <label className="sl-field">
                        <span className="sl-label">Slug</span>
                        <input value={skillDraft.slug ?? ''} onChange={(e) => updateSkillDraft('slug', e.target.value)} placeholder="kebab-case-id" />
                      </label>
                      <label className="sl-field">
                        <span className="sl-label">绑定步骤 ID</span>
                        <input value={skillDraft.step_id} onChange={(e) => updateSkillDraft('step_id', e.target.value)} placeholder="step.execute / step.review …" />
                      </label>
                      <label className="sl-field">
                        <span className="sl-label">来源</span>
                        <input value={skillDraft.source} onChange={(e) => updateSkillDraft('source', e.target.value)} placeholder="user / system" />
                      </label>
                    </div>

                    <div className="sl-field sl-field-row area-md">
                      <span className="sl-label">摘要</span>
                      <textarea value={skillDraft.summary} onChange={(e) => updateSkillDraft('summary', e.target.value)} placeholder="一两句话说明这个 SKILL 解决的问题" />
                    </div>

                    <div className="sl-field sl-field-row area-lg">
                      <span className="sl-label">说明</span>
                      <textarea value={skillDraft.description} onChange={(e) => updateSkillDraft('description', e.target.value)} placeholder="详细描述使用场景、输入输出约束等" />
                    </div>

                    <div className="sl-checks">
                      <label>
                        <input type="checkbox" checked={skillDraft.default_exposed} onChange={(e) => updateSkillDraft('default_exposed', e.target.checked)} />
                        默认暴露给 AI
                      </label>
                      <label>
                        <input type="checkbox" checked={skillDraft.enabled} onChange={(e) => updateSkillDraft('enabled', e.target.checked)} />
                        启用
                      </label>
                      <span className="muted">被调用次数：{skillDraft.usage_count}</span>
                    </div>
                  </section>

                  <section className="sl-card">
                    <div className="sl-card-head">
                      <div>
                        <h3>内容模块</h3>
                        <div className="sl-sub">默认仅显示必要模块与已启用的可选模块。</div>
                      </div>
                      <div className="sl-card-actions">
                        <button type="button" className="btn tiny" onClick={() => setShowAllSkillModules((v) => !v)}>
                          {showAllSkillModules ? '仅看必要/启用' : '显示全部'}
                        </button>
                        <button type="button" className="btn tiny" onClick={addSkillModule}>+ 新增模块</button>
                      </div>
                    </div>

                    {!showAllSkillModules && hiddenSkillModuleCount > 0 && (
                      <p className="muted" style={{ marginBottom: '0.8rem', fontSize: '0.82rem' }}>
                        当前还有 {hiddenSkillModuleCount} 个未启用的可选模块，可点击"显示全部"查看。
                      </p>
                    )}

                    <div className="sl-modules">
                      {visibleSkillModules.map((module) => {
                        const realIndex = skillDraft.modules.indexOf(module)
                        return (
                          <div key={`${module.id ?? 'new'}-${realIndex}`} className={`sl-module${module.required ? ' required' : ''}`}>
                            <div className="sl-module-head">
                              <input
                                className="sl-mtitle"
                                value={module.title}
                                onChange={(e) => updateSkillModule(realIndex, { title: e.target.value })}
                                placeholder="模块标题"
                              />
                              <div className="sl-flags">
                                <label>
                                  <input
                                    type="checkbox"
                                    checked={module.required}
                                    onChange={(e) => updateSkillModule(realIndex, { required: e.target.checked, enabled: e.target.checked ? true : module.enabled })}
                                  />
                                  必要
                                </label>
                                <label style={{ opacity: module.required ? 0.55 : 1 }}>
                                  <input
                                    type="checkbox"
                                    checked={module.enabled || module.required}
                                    disabled={module.required}
                                    onChange={(e) => updateSkillModule(realIndex, { enabled: e.target.checked })}
                                  />
                                  启用
                                </label>
                              </div>
                            </div>
                            <textarea
                              className="sl-module-content"
                              value={module.content}
                              onChange={(e) => updateSkillModule(realIndex, { content: e.target.value })}
                              placeholder="模块内容（支持 Markdown）"
                            />
                            <div className="sl-module-foot">
                              <button type="button" className="btn tiny danger" disabled={module.required} onClick={() => removeSkillModule(realIndex)}>
                                删除模块
                              </button>
                            </div>
                          </div>
                        )
                      })}
                      {visibleSkillModules.length === 0 && (
                        <div className="sl-empty">尚未添加内容模块，点击"新增模块"开始。</div>
                      )}
                    </div>
                  </section>

                  <section className="sl-card">
                    <div className="sl-card-head">
                      <div>
                        <h3>实际 SKILL 文件</h3>
                        <div className="sl-sub">{skillDraft.generated_file_path || '保存后将自动生成 Markdown + YAML 头格式'}</div>
                      </div>
                    </div>
                    <pre className="sl-preview">
                      {skillDraft.generated_content || '尚未生成。点击右上角"生成实际文件"按钮即可生成。'}
                    </pre>
                  </section>
                </>
              ) : (
                <section className="sl-card">
                  <div className="sl-empty">暂无可编辑的 SKILL。</div>
                </section>
              )
            ) : promptDraft ? (
              <>
                <section className="sl-card">
                  <div className="sl-card-head">
                    <div>
                      <h3>提示词信息</h3>
                      <div className="sl-sub">{tab === 'system' ? '系统提示词' : '工具提示词'}的元数据与说明。</div>
                    </div>
                  </div>
                  <div className="sl-grid cols-2">
                    <label className="sl-field">
                      <span className="sl-label">标题</span>
                      <input value={promptDraft.title} onChange={(e) => updatePromptDraft('title', e.target.value)} />
                    </label>
                    <label className="sl-field">
                      <span className="sl-label">引用 Key</span>
                      <input value={promptDraft.prompt_key} readOnly style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)' }} />
                    </label>
                  </div>

                  <div className="sl-field sl-field-row area-md">
                    <span className="sl-label">摘要</span>
                    <textarea value={promptDraft.summary} onChange={(e) => updatePromptDraft('summary', e.target.value)} placeholder="简要说明此提示词的用途" />
                  </div>

                  <div className="sl-field sl-field-row area-md">
                    <span className="sl-label">引用说明</span>
                    <textarea value={promptDraft.reference_note} onChange={(e) => updatePromptDraft('reference_note', e.target.value)} placeholder="说明在哪些场景被引用、注入位置等" />
                  </div>

                  <div className="sl-field sl-field-row area-md">
                    <span className="sl-label">说明</span>
                    <textarea value={promptDraft.description} onChange={(e) => updatePromptDraft('description', e.target.value)} placeholder="补充信息、注意事项" />
                  </div>

                  <div className="sl-checks">
                    <label>
                      <input type="checkbox" checked={promptDraft.enabled} onChange={(e) => updatePromptDraft('enabled', e.target.checked)} />
                      启用
                    </label>
                    <span className="muted">{promptDraft.override ? '当前已覆盖默认内容' : '当前使用系统默认内容'}</span>
                  </div>
                </section>

                <section className="sl-card">
                  <div className="sl-card-head">
                    <div>
                      <h3>内容模块</h3>
                      <div className="sl-sub">系统/工具提示词按模块保存；工具提示词模块 key 对应 schema 中的 description 路径。</div>
                    </div>
                    <div className="sl-card-actions">
                      <button type="button" className="btn tiny" onClick={() => setShowAllPromptModules((v) => !v)}>
                        {showAllPromptModules ? '仅看必要/启用' : '显示全部'}
                      </button>
                    </div>
                  </div>

                  {!showAllPromptModules && hiddenPromptModuleCount > 0 && (
                    <p className="muted" style={{ marginBottom: '0.8rem', fontSize: '0.82rem' }}>
                      当前还有 {hiddenPromptModuleCount} 个未启用的模块，可点击"显示全部"查看。
                    </p>
                  )}

                  <div className="sl-modules">
                    {visiblePromptModules.map((module) => {
                      const realIndex = promptDraft.modules.indexOf(module)
                      return (
                        <div key={`${module.id ?? 'default'}-${module.module_key ?? realIndex}`} className={`sl-module${module.required ? ' required' : ''}`}>
                          <div className="sl-module-head">
                            <input className="sl-mkey" value={module.module_key ?? ''} readOnly title={module.module_key ?? ''} />
                            <input
                              className="sl-mtitle"
                              value={module.title}
                              onChange={(e) => updatePromptModule(realIndex, { title: e.target.value })}
                              placeholder="模块标题"
                            />
                            <div className="sl-flags">
                              <label>
                                <input type="checkbox" checked={module.required} disabled />
                                必要
                              </label>
                              <label>
                                <input
                                  type="checkbox"
                                  checked={module.enabled || module.required}
                                  disabled={module.required}
                                  onChange={(e) => updatePromptModule(realIndex, { enabled: e.target.checked })}
                                />
                                启用
                              </label>
                            </div>
                          </div>
                          <textarea
                            className="sl-module-content"
                            value={module.content}
                            onChange={(e) => updatePromptModule(realIndex, { content: e.target.value })}
                            placeholder="模块内容（支持 Markdown）"
                          />
                        </div>
                      )
                    })}
                    {visiblePromptModules.length === 0 && (
                      <div className="sl-empty">该提示词暂无可见模块。</div>
                    )}
                  </div>
                </section>

                <section className="sl-card">
                  <div className="sl-card-head">
                    <div>
                      <h3>运行时引用预览</h3>
                      <div className="sl-sub" style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)' }}>{promptDraft.prompt_key}</div>
                    </div>
                  </div>
                  <pre className="sl-preview">
                    {runtimePreview || '当前无启用模块内容。'}
                  </pre>
                </section>
              </>
            ) : (
              <section className="sl-card">
                <div className="sl-empty">请选择左侧列表中的一项进行编辑。</div>
              </section>
            )}
          </div>
        </main>
      </div>

      <div className="sl-toast-container">
        {toasts.map((t) => (
          <div key={t.id} className={`sl-toast ${t.type}`}>
            <span className="sl-toast-icon">{t.type === 'success' ? '✓' : '!'}</span>
            <span>{t.message}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
