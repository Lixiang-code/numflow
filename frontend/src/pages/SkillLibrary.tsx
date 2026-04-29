import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'
import AutoTextarea from '../components/AutoTextarea'

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
  default_title?: string
  default_summary?: string
  default_description?: string
  default_reference_note?: string
  default_enabled?: boolean
  default_modules?: PromptModule[]
  tool_group_key?: string
  tool_group_label?: string
  tool_group_order?: number
  tool_group_hint?: string
  tool_name_zh?: string
  tool_summary_zh?: string
  diagnostics?: {
    default_module_keys?: string[]
    extra_module_keys?: string[]
    orphan_module_keys?: string[]
  }
  modules: PromptModule[]
}

type PromptTab = 'skill' | 'system' | 'tool'
type ToolPromptGroup = {
  key: string
  label: string
  hint: string
  order: number
  items: PromptItem[]
}

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

/** 工具提示词：每个模块是 JSON schema 里的一个 description 字段，用分段格式展示 */
function renderToolModules(modules: PromptModule[]): string {
  const active = modules.filter((m) => m.required || m.enabled)
  if (!active.length) return ''
  return active
    .map((m) => {
      // 将 function.parameters.properties.foo.description → 参数 foo
      const key = m.module_key ?? ''
      let label = m.title || key
      if (key === 'function.description') {
        label = '函数说明（function.description）'
      } else {
        const paramMatch = key.match(/function\.parameters\.properties\.([^.]+)\.description$/)
        if (paramMatch) label = `参数 ${paramMatch[1]}（${key}）`
      }
      return `▸ ${label}\n${m.content.trim()}`
    })
    .join('\n\n' + '─'.repeat(48) + '\n\n')
}

// ── Draft cache types ─────────────────────────────────────────────────────
type SkillCacheEntry  = { draft: SkillItem;  baseline: SkillItem;  serverConflict?: SkillItem  }
type PromptCacheEntry = { draft: PromptItem; baseline: PromptItem; serverConflict?: PromptItem }

/** 稳定比较：对对象 key 排序后再 JSON.stringify，避免后端字段顺序变化导致的假冲突。 */
function deepEqual(a: unknown, b: unknown): boolean {
  const stable = (v: unknown): unknown => {
    if (v === null || typeof v !== 'object') return v
    if (Array.isArray(v)) return v.map(stable)
    const obj = v as Record<string, unknown>
    const out: Record<string, unknown> = {}
    for (const k of Object.keys(obj).sort()) out[k] = stable(obj[k])
    return out
  }
  return JSON.stringify(stable(a)) === JSON.stringify(stable(b))
}

/**
 * 在前端复刻后端 render_skill_markdown 逻辑，用于"实际 SKILL 文件"的实时预览。
 * 必须与 backend/app/services/skill_library.py::render_skill_markdown 保持一致。
 */
function renderSkillMarkdown(skill: SkillItem): string {
  const chosen = skill.modules.filter((m) => m.required || m.enabled)
  const lines: string[] = ['---']
  lines.push(`skill_slug: ${JSON.stringify(skill.slug ?? '')}`)
  lines.push(`title: ${JSON.stringify(skill.title ?? '')}`)
  lines.push(`step_id: ${JSON.stringify(skill.step_id ?? '')}`)
  lines.push(`source: ${JSON.stringify(skill.source || 'user')}`)
  lines.push(`default_exposed: ${skill.default_exposed ? 'true' : 'false'}`)
  lines.push('enabled_module_keys:')
  for (const m of chosen) lines.push(`  - ${JSON.stringify(m.module_key || m.title)}`)
  lines.push('---')
  lines.push(`# ${skill.title || ''}`)
  if (skill.summary) { lines.push(''); lines.push(`> ${skill.summary}`) }
  if (skill.description) { lines.push(''); lines.push(skill.description) }
  for (const m of chosen) {
    lines.push('')
    lines.push(`## ${m.title}`)
    lines.push((m.content || '').trim())
  }
  return lines.join('\n').trim() + '\n'
}

/** 简易 token 估算：中英文混排时按 3.5 字符 ≈ 1 token 粗略估计。 */
function estimateTokens(text: string): number {
  if (!text) return 0
  return Math.max(1, Math.round(text.length / 3.5))
}
function skillCacheKey(id: number | 'new'): string   { return String(id) }
function promptCacheKey(tabKey: string, key: string): string { return `${tabKey}::${key}` }

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
  const [skillBasicCollapsed,  setSkillBasicCollapsed]  = useState(false)
  const [promptBasicCollapsed, setPromptBasicCollapsed] = useState(false)

  const [err, setErr] = useState<string | null>(null)
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const toastId = useRef(0)

  // ── Draft cache: preserves edits across item / tab switches ──────────────
  const skillCache  = useRef(new Map<string, SkillCacheEntry>())
  const promptCache = useRef(new Map<string, PromptCacheEntry>())
  const [skillCacheView, setSkillCacheView] = useState<Record<string, SkillCacheEntry>>({})
  const [promptCacheView, setPromptCacheView] = useState<Record<string, PromptCacheEntry>>({})
  const [activeSkillBaseline, setActiveSkillBaseline] = useState<SkillItem | null>(null)
  const [activePromptBaseline, setActivePromptBaseline] = useState<PromptItem | null>(null)
  const [dirtySkillIds,   setDirtySkillIds]   = useState<Set<string>>(new Set())
  const [dirtyPromptKeys, setDirtyPromptKeys] = useState<Set<string>>(new Set())
  const [conflictSkill,  setConflictSkill]  = useState<{ key: string; local: SkillItem;  server: SkillItem  } | null>(null)
  const [conflictPrompt, setConflictPrompt] = useState<{ key: string; local: PromptItem; server: PromptItem } | null>(null)
  // 侧边栏"⚡冲突"提示用：记录哪些条目存在未解决的服务端冲突
  const [conflictSkillKeys,  setConflictSkillKeys]  = useState<Set<string>>(new Set())
  const [conflictPromptKeys, setConflictPromptKeys] = useState<Set<string>>(new Set())
  const markSkillConflict   = useCallback((key: string) => setConflictSkillKeys(p   => { const n = new Set(p); n.add(key); return n }),    [])
  const clearSkillConflict  = useCallback((key: string) => setConflictSkillKeys(p   => { const n = new Set(p); n.delete(key); return n }), [])
  const markPromptConflict  = useCallback((key: string) => setConflictPromptKeys(p => { const n = new Set(p); n.add(key); return n }),    [])
  const clearPromptConflict = useCallback((key: string) => setConflictPromptKeys(p => { const n = new Set(p); n.delete(key); return n }), [])

  // 侧边栏"未启用"分组折叠状态（按 tab 区分，持久化到 localStorage）
  const DISABLED_COLLAPSE_KEY = 'sl_disabled_collapsed_v1'
  const [disabledCollapsed, setDisabledCollapsed] = useState<Record<PromptTab, boolean>>(() => {
    if (typeof window === 'undefined') return { skill: false, system: false, tool: false }
    try {
      const raw = window.localStorage.getItem(DISABLED_COLLAPSE_KEY)
      if (raw) return { skill: false, system: false, tool: false, ...JSON.parse(raw) }
    } catch { /* ignore */ }
    return { skill: false, system: false, tool: false }
  })
  const toggleDisabledCollapsed = useCallback((t: PromptTab) => {
    setDisabledCollapsed((prev) => {
      const next = { ...prev, [t]: !prev[t] }
      try { window.localStorage.setItem(DISABLED_COLLAPSE_KEY, JSON.stringify(next)) } catch { /* ignore */ }
      return next
    })
  }, [])

  // 分组折叠状态（key = `${tab}::${groupKey}`）
  const [collapsedPromptGroups, setCollapsedPromptGroups] = useState<Record<string, boolean>>({})
  const togglePromptGroup = useCallback((tabKey: string, groupKey: string) => {
    setCollapsedPromptGroups((prev) => ({ ...prev, [`${tabKey}::${groupKey}`]: !prev[`${tabKey}::${groupKey}`] }))
  }, [])

  const markSkillDirty   = useCallback((key: string) => setDirtySkillIds(p   => { const n = new Set(p); n.add(key); return n }),    [])
  const clearSkillDirty  = useCallback((key: string) => setDirtySkillIds(p   => { const n = new Set(p); n.delete(key); return n }), [])
  const markPromptDirty  = useCallback((key: string) => setDirtyPromptKeys(p => { const n = new Set(p); n.add(key); return n }),    [])
  const clearPromptDirty = useCallback((key: string) => setDirtyPromptKeys(p => { const n = new Set(p); n.delete(key); return n }), [])

  const pushToast = useCallback((type: ToastItem['type'], message: string) => {
    toastId.current += 1
    const id = toastId.current
    setToasts((cur) => [...cur, { id, type, message }])
    window.setTimeout(() => {
      setToasts((cur) => cur.filter((t) => t.id !== id))
    }, 2600)
  }, [])

  const applySkillSelection = useCallback((skillId: number | 'new' | null, draft: SkillItem | null, baseline: SkillItem | null) => {
    setEditingSkillId(skillId)
    setSkillDraft(draft)
    setActiveSkillBaseline(baseline)
  }, [])

  const applyPromptSelection = useCallback((promptKey: string | null, draft: PromptItem | null, baseline: PromptItem | null) => {
    setEditingPromptKey(promptKey)
    setPromptDraft(draft)
    setActivePromptBaseline(baseline)
  }, [])

  const syncSkillCacheView = useCallback(() => {
    const next: Record<string, SkillCacheEntry> = {}
    skillCache.current.forEach((entry, key) => {
      next[key] = {
        draft: cloneValue(entry.draft),
        baseline: cloneValue(entry.baseline),
        serverConflict: entry.serverConflict ? cloneValue(entry.serverConflict) : undefined,
      }
    })
    setSkillCacheView(next)
  }, [])

  const syncPromptCacheView = useCallback(() => {
    const next: Record<string, PromptCacheEntry> = {}
    promptCache.current.forEach((entry, key) => {
      next[key] = {
        draft: cloneValue(entry.draft),
        baseline: cloneValue(entry.baseline),
        serverConflict: entry.serverConflict ? cloneValue(entry.serverConflict) : undefined,
      }
    })
    setPromptCacheView(next)
  }, [])

  const loadSkills = useCallback(async (selectId?: number | 'new' | null, fromEffect = false) => {
    if (!pid) return
    if (!fromEffect) { setSkillLoading(true); setErr(null) }
    try {
      const data = (await apiFetch('/skills', { headers })) as { skills?: SkillItem[] }
      const next = data.skills ?? []
      setSkills(next)
      // Update draft cache: detect conflicts for dirty items, update baseline otherwise
      for (const item of next) {
        const key = skillCacheKey(item.id!)
        const server = cloneValue(item)
        const entry = skillCache.current.get(key)
        if (!entry) {
          skillCache.current.set(key, { draft: server, baseline: server })
        } else if (!deepEqual(entry.baseline, server)) {
          if (!deepEqual(entry.draft, entry.baseline)) {
            // Server changed while we had local edits → conflict
            skillCache.current.set(key, { ...entry, serverConflict: server })
            markSkillConflict(key)
          } else {
            // No local edits, just update to latest server data
            skillCache.current.set(key, { draft: server, baseline: server })
            clearSkillConflict(key)
          }
        }
      }
      syncSkillCacheView()
      // Resolve selected item
      const resolveSkillDraft = (id: number): SkillItem => {
        const entry = skillCache.current.get(skillCacheKey(id))
        const found = next.find((s) => s.id === id)!
        return entry ? cloneValue(entry.draft) : cloneValue(found)
      }
      const resolveSkillBaseline = (id: number): SkillItem => {
        const entry = skillCache.current.get(skillCacheKey(id))
        const found = next.find((s) => s.id === id)!
        return entry ? cloneValue(entry.baseline) : cloneValue(found)
      }
      if (selectId === 'new') {
        const blank = blankSkill()
        applySkillSelection('new', blank, cloneValue(blank))
      } else if (typeof selectId === 'number') {
        const found = next.find((item) => item.id === selectId)
        if (found) applySkillSelection(selectId, resolveSkillDraft(selectId), resolveSkillBaseline(selectId))
      } else if (next.length > 0) {
        const first = next[0]
        if (first.id != null) {
          applySkillSelection(first.id, resolveSkillDraft(first.id), resolveSkillBaseline(first.id))
        } else {
          const firstDraft = cloneValue(first)
          applySkillSelection(null, firstDraft, cloneValue(firstDraft))
        }
      } else {
        const blank = blankSkill()
        applySkillSelection('new', blank, cloneValue(blank))
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSkillLoading(false)
    }
  }, [applySkillSelection, headers, pid, markSkillConflict, clearSkillConflict, syncSkillCacheView])

  const loadPromptItems = useCallback(async (category: 'system' | 'tool', selectKey?: string | null, fromEffect = false) => {
    if (!pid) return
    if (!fromEffect) { setPromptLoading(true); setErr(null) }
    try {
      const data = (await apiFetch(`/prompts?category=${category}`, { headers })) as { items?: PromptItem[] }
      const next = data.items ?? []
      setPromptItems(next)
      // Update draft cache
      for (const item of next) {
        const key = promptCacheKey(category, item.prompt_key)
        const server = cloneValue(item)
        const entry = promptCache.current.get(key)
        if (!entry) {
          promptCache.current.set(key, { draft: server, baseline: server })
        } else if (!deepEqual(entry.baseline, server)) {
          if (!deepEqual(entry.draft, entry.baseline)) {
            promptCache.current.set(key, { ...entry, serverConflict: server })
            markPromptConflict(key)
          } else {
            promptCache.current.set(key, { draft: server, baseline: server })
            clearPromptConflict(key)
          }
        }
      }
      syncPromptCacheView()
      const resolvePromptDraft = (pk: string): PromptItem => {
        const key = promptCacheKey(category, pk)
        const entry = promptCache.current.get(key)
        const found = next.find((i) => i.prompt_key === pk)!
        return entry ? cloneValue(entry.draft) : cloneValue(found)
      }
      const resolvePromptBaseline = (pk: string): PromptItem => {
        const key = promptCacheKey(category, pk)
        const entry = promptCache.current.get(key)
        const found = next.find((i) => i.prompt_key === pk)!
        return entry ? cloneValue(entry.baseline) : cloneValue(found)
      }
      if (selectKey) {
        const found = next.find((item) => item.prompt_key === selectKey)
        if (found) {
          applyPromptSelection(selectKey, resolvePromptDraft(selectKey), resolvePromptBaseline(selectKey))
          return
        }
      }
      const first = next[0] ?? null
      if (first) {
        applyPromptSelection(first.prompt_key, resolvePromptDraft(first.prompt_key), resolvePromptBaseline(first.prompt_key))
      } else {
        const blank = blankPrompt()
        applyPromptSelection(null, blank, cloneValue(blank))
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setPromptLoading(false)
    }
  }, [applyPromptSelection, headers, pid, markPromptConflict, clearPromptConflict, syncPromptCacheView])

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

  function createSkill() {
    const blank = blankSkill()
    applySkillSelection('new', blank, cloneValue(blank))
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
      // 关键：保存成功后，强制把本地草稿与缓存基线同步为服务端返回值。
      // 否则后端可能对字段做归一化（去空白、补默认 sort_order 等），
      // 导致 useEffect 比较 draft ≠ baseline 又把"编辑中"标记重新点上。
      const savedKey = skillCacheKey(saved.id ?? 'new')
      const savedSnap = cloneValue(saved)
      skillCache.current.set(savedKey, { draft: savedSnap, baseline: cloneValue(saved) })
      syncSkillCacheView()
      setSkillDraft(cloneValue(saved))
      setActiveSkillBaseline(cloneValue(saved))
      clearSkillDirty(savedKey)
      clearSkillConflict(savedKey)
      pushToast('success', `SKILL「${saved.title || skillDraft.title || '未命名'}」已保存`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `保存失败：${msg}`)
    } finally {
      setSkillSaving(false)
    }
  }, [headers, loadSkills, pushToast, skillDraft, clearSkillDirty, clearSkillConflict, syncSkillCacheView])

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
      // 关键：保存成功后，把本地草稿与缓存基线同步为服务端数据，避免归一化字段差异触发"编辑中"。
      const savedPKey = promptCacheKey(category, promptDraft.prompt_key)
      const savedPEntry = promptCache.current.get(savedPKey)
      // 优先使用 loadPromptItems 在缓存上写入的 serverConflict (即最新服务端值)，回退到 draft。
      const serverItem = savedPEntry?.serverConflict ?? savedPEntry?.draft ?? promptDraft
      const serverSnap = cloneValue(serverItem)
      promptCache.current.set(savedPKey, { draft: serverSnap, baseline: cloneValue(serverItem) })
      syncPromptCacheView()
      setPromptDraft(cloneValue(serverItem))
      setActivePromptBaseline(cloneValue(serverItem))
      clearPromptDirty(savedPKey)
      clearPromptConflict(savedPKey)
      pushToast('success', `提示词「${promptDraft.title || promptDraft.prompt_key}」已保存`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      pushToast('error', `保存失败：${msg}`)
    } finally {
      setPromptSaving(false)
    }
  }, [headers, loadPromptItems, promptDraft, pushToast, tab, clearPromptDirty, clearPromptConflict, syncPromptCacheView])

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
          if (!skillDraft) { pushToast('error', '请先选择或新建一个 SKILL'); return }
          if (skillSaving) return
          if (!skillDraft.title.trim()) { pushToast('error', '请先填写标题再保存'); return }
          void saveSkill()
        } else {
          if (!promptDraft) { pushToast('error', '请先选择一个提示词'); return }
          if (promptSaving) return
          if (!promptDraft.title.trim()) { pushToast('error', '请先填写标题再保存'); return }
          void savePrompt()
        }
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [promptDraft, promptSaving, savePrompt, saveSkill, skillDraft, skillSaving, tab, pushToast])

  // 离开页面前警告：存在未保存草稿时阻止刷新/关闭
  useEffect(() => {
    const hasDirty = dirtySkillIds.size > 0 || dirtyPromptKeys.size > 0
    if (!hasDirty) return
    function onBeforeUnload(e: BeforeUnloadEvent) {
      e.preventDefault()
      e.returnValue = ''
    }
    window.addEventListener('beforeunload', onBeforeUnload)
    return () => window.removeEventListener('beforeunload', onBeforeUnload)
  }, [dirtySkillIds, dirtyPromptKeys])

  // ── Sync active draft to cache → update dirty badges ─────────────────────
  useEffect(() => {
    if (!skillDraft || editingSkillId == null) return
    const key = skillCacheKey(editingSkillId)
    const entry = skillCache.current.get(key)
    if (!entry) return
    entry.draft = cloneValue(skillDraft)
    if (deepEqual(skillDraft, entry.baseline)) clearSkillDirty(key)
    else markSkillDirty(key)
  }, [clearSkillDirty, editingSkillId, markSkillDirty, skillDraft])

  useEffect(() => {
    if (!promptDraft || !editingPromptKey || tab === 'skill') return
    const key = promptCacheKey(tab, editingPromptKey)
    const entry = promptCache.current.get(key)
    if (!entry) return
    entry.draft = cloneValue(promptDraft)
    if (deepEqual(promptDraft, entry.baseline)) clearPromptDirty(key)
    else markPromptDirty(key)
  }, [clearPromptDirty, editingPromptKey, markPromptDirty, promptDraft, tab])

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

  const runtimePreview = useMemo(
    () => tab === 'tool'
      ? renderToolModules(promptDraft?.modules || [])
      : renderModules(promptDraft?.modules || []),
    [promptDraft, tab],
  )
  const defaultRuntimePreview = useMemo(
    () => tab === 'tool'
      ? renderToolModules(promptDraft?.default_modules || [])
      : renderModules(promptDraft?.default_modules || []),
    [promptDraft, tab],
  )

  // ── Per-module and section dirty detection ────────────────────────────────
  const dirtySkillModuleIndices = useMemo((): Set<number> => {
    if (!editingSkillId || !skillDraft || !activeSkillBaseline) return new Set()
    const result = new Set<number>()
    const bMods = activeSkillBaseline.modules
    skillDraft.modules.forEach((mod, i) => {
      if (i >= bMods.length || !deepEqual(mod, bMods[i])) result.add(i)
    })
    return result
  }, [activeSkillBaseline, editingSkillId, skillDraft])

  const dirtyPromptModuleIndices = useMemo((): Set<number> => {
    if (!editingPromptKey || !promptDraft || !activePromptBaseline || tab === 'skill') return new Set()
    const result = new Set<number>()
    const bMods = activePromptBaseline.modules
    promptDraft.modules.forEach((mod, i) => {
      if (i >= bMods.length || !deepEqual(mod, bMods[i])) result.add(i)
    })
    return result
  }, [activePromptBaseline, editingPromptKey, promptDraft, tab])

  const skillBasicDirty = useMemo(() => {
    if (!editingSkillId || !skillDraft || !activeSkillBaseline) return false
    const b = activeSkillBaseline
    return skillDraft.title !== b.title || skillDraft.slug !== b.slug ||
      skillDraft.step_id !== b.step_id || skillDraft.source !== b.source ||
      skillDraft.summary !== b.summary || skillDraft.description !== b.description ||
      skillDraft.default_exposed !== b.default_exposed || skillDraft.enabled !== b.enabled
  }, [activeSkillBaseline, editingSkillId, skillDraft])

  const promptBasicDirty = useMemo(() => {
    if (!editingPromptKey || !promptDraft || !activePromptBaseline || tab === 'skill') return false
    const b = activePromptBaseline
    return promptDraft.title !== b.title || promptDraft.summary !== b.summary ||
      promptDraft.description !== b.description || promptDraft.reference_note !== b.reference_note ||
      promptDraft.enabled !== b.enabled
  }, [activePromptBaseline, editingPromptKey, promptDraft, tab])

  const loading = tab === 'skill' ? skillLoading : promptLoading
  const isMac = typeof navigator !== 'undefined' && navigator.platform.toLowerCase().includes('mac')
  const shortcut = isMac ? '⌘S' : 'Ctrl+S'

  // ── Dirty / conflict helpers (reactive off dirtySkillIds / dirtyPromptKeys) ──
  const isSkillDirty   = (id: number | 'new') => dirtySkillIds.has(skillCacheKey(id))
  const isPromptDirty  = (pk: string) => dirtyPromptKeys.has(promptCacheKey(tab as 'system' | 'tool', pk))
  const tabHasDirty    = (t: PromptTab) =>
    t === 'skill'
      ? dirtySkillIds.size > 0
      : [...dirtyPromptKeys].some((k) => k.startsWith(t + '::'))
  // currentDirty retained for potential future use (sidebar highlights etc.)
  const _currentDirty =
    tab === 'skill'
      ? (editingSkillId != null && isSkillDirty(editingSkillId))
      : (editingPromptKey != null && isPromptDirty(editingPromptKey))
  void _currentDirty

  const canSave = tab === 'skill' ? !!skillDraft && !skillSaving : !!promptDraft && !promptSaving
  const saving = tab === 'skill' ? skillSaving : promptSaving
  const handleSave = () => {
    if (tab === 'skill') {
      if (!skillDraft) { pushToast('error', '请先选择或新建一个 SKILL'); return }
      if (!skillDraft.title.trim()) { pushToast('error', '请先填写标题再保存'); return }
      void saveSkill()
    } else {
      if (!promptDraft) { pushToast('error', '请先选择一个提示词'); return }
      if (!promptDraft.title.trim()) { pushToast('error', '请先填写标题再保存'); return }
      void savePrompt()
    }
  }

  // ── 页面级控制栏脏状态（启用 / 默认暴露 与 baseline 不一致时高亮）──
  const skillControlsDirty = useMemo(() => {
    if (!editingSkillId || !skillDraft || !activeSkillBaseline) return false
    return skillDraft.enabled !== activeSkillBaseline.enabled
      || skillDraft.default_exposed !== activeSkillBaseline.default_exposed
  }, [activeSkillBaseline, editingSkillId, skillDraft])

  const promptControlsDirty = useMemo(() => {
    if (!editingPromptKey || !promptDraft || !activePromptBaseline || tab === 'skill') return false
    return promptDraft.enabled !== activePromptBaseline.enabled
  }, [activePromptBaseline, editingPromptKey, promptDraft, tab])
  const promptGroups = useMemo((): ToolPromptGroup[] => {
    if (tab === 'skill') return []
    const groups = new Map<string, ToolPromptGroup>()
    for (const item of promptItems) {
      const key = item.tool_group_key || 'other'
      const current = groups.get(key) ?? {
        key,
        label: item.tool_group_label || '其他',
        hint: item.tool_group_hint || '',
        order: item.tool_group_order ?? 999,
        items: [],
      }
      current.items.push(item)
      groups.set(key, current)
    }
    return Array.from(groups.values())
      .map((group) => ({
        ...group,
        items: [...group.items].sort((a, b) => {
          if (tab === 'tool') {
            if (a.enabled !== b.enabled) return a.enabled ? -1 : 1
            return (a.tool_name_zh || a.title).localeCompare((b.tool_name_zh || b.title), 'zh-CN')
          }
          return (a.display_order ?? 0) - (b.display_order ?? 0) ||
            (a.tool_name_zh || a.title).localeCompare((b.tool_name_zh || b.title), 'zh-CN')
        }),
      }))
      .sort((a, b) => a.order - b.order || a.label.localeCompare(b.label, 'zh-CN'))
  }, [promptItems, tab])

  return (
    <div className="sl-page">
      {/* 沉浸聚焦遮罩：当任意 AI 可见输入框获得焦点时由 CSS body:has() 控制显隐 */}
      <div className="sl-focus-overlay" aria-hidden="true" />
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
                {tabHasDirty(key) && <span className="sl-dirty-dot" title="有未保存内容" />}
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
              ? (() => {
                  const enabled  = skills.filter((s) => s.enabled)
                  const disabled = skills.filter((s) => !s.enabled)
                  const renderSkill = (skill: SkillItem) => {
                    const active = skill.id === editingSkillId
                    const dirty  = skill.id != null && isSkillDirty(skill.id)
                    const conflict = skill.id != null && conflictSkillKeys.has(skillCacheKey(skill.id))
                    return (
                      <button
                        key={skill.id ?? skill.slug}
                        type="button"
                        onClick={() => {
                          if (skill.id == null) return
                          const nextSkillCacheView = { ...skillCacheView }
                          if (editingSkillId != null && skillDraft && activeSkillBaseline) {
                            const currentKey = skillCacheKey(editingSkillId)
                            nextSkillCacheView[currentKey] = {
                              draft: cloneValue(skillDraft),
                              baseline: cloneValue(activeSkillBaseline),
                              serverConflict: nextSkillCacheView[currentKey]?.serverConflict,
                            }
                            setSkillCacheView(nextSkillCacheView)
                          }
                          const key = skillCacheKey(skill.id)
                          const entry = nextSkillCacheView[key]
                          if (entry?.serverConflict) {
                            setConflictSkill({ key, local: entry.draft, server: entry.serverConflict })
                            return
                          }
                          const draft = entry ? cloneValue(entry.draft) : cloneValue(skill)
                          const baseline = entry ? cloneValue(entry.baseline) : cloneValue(skill)
                          applySkillSelection(skill.id, draft, baseline)
                          setShowAllSkillModules(false)
                        }}
                        className={`sl-list-item${active ? ' active' : ''}${dirty ? ' sl-dirty-item' : ''}${conflict ? ' sl-conflict-item' : ''}`}
                      >
                        <div className="sl-row">
                          <span className="sl-name">
                            {conflict && <span className="sl-conflict-icon-inline" title="存在未解决的服务端冲突">⚡</span>}
                            {skill.title || '(未命名)'}
                          </span>
                          <span className="sl-meta">{dirty ? <span className="sl-dirty-tag">编辑中</span> : `调用 ${skill.usage_count}`}</span>
                        </div>
                        <div className="sl-key">{skill.step_id || '未绑定步骤'}</div>
                        <div className="sl-chips">
                          <span className="sl-chip">{skill.source === 'system' ? '默认' : '用户'}</span>
                          {skill.default_exposed && <span className="sl-chip green">默认暴露</span>}
                        </div>
                      </button>
                    )
                  }
                  const collapsed = disabledCollapsed.skill
                  return (
                    <>
                      {enabled.length > 0 && (
                        <>
                          <div className="sl-group-label">启用中 ({enabled.length})</div>
                          {enabled.map(renderSkill)}
                        </>
                      )}
                      {disabled.length > 0 && (
                        <>
                          <button
                            type="button"
                            className="sl-group-label muted sl-group-toggle"
                            onClick={() => toggleDisabledCollapsed('skill')}
                            title={collapsed ? '展开未启用分组' : '折叠未启用分组'}
                          >
                            <span className="sl-group-caret">{collapsed ? '▸' : '▾'}</span>
                            未启用 ({disabled.length})
                          </button>
                          {!collapsed && disabled.map(renderSkill)}
                        </>
                      )}
                    </>
                  )
                })()
              : (() => {
                  const renderPrompt = (item: PromptItem) => {
                    const active = item.prompt_key === editingPromptKey
                    const dirty  = isPromptDirty(item.prompt_key)
                    const conflict = conflictPromptKeys.has(promptCacheKey(tab as 'system' | 'tool', item.prompt_key))
                    const displayTitle = (tab === 'tool' || tab === 'system')
                      ? (item.tool_name_zh || item.title)
                      : item.title
                    const displaySummary = (tab === 'tool' || tab === 'system')
                      ? (item.tool_summary_zh || item.summary || '')
                      : (item.summary || '')
                    return (
                      <button
                        key={item.prompt_key}
                        type="button"
                        onClick={() => {
                          const nextPromptCacheView = { ...promptCacheView }
                          if (editingPromptKey && promptDraft && activePromptBaseline) {
                            const currentKey = promptCacheKey(tab as 'system' | 'tool', editingPromptKey)
                            nextPromptCacheView[currentKey] = {
                              draft: cloneValue(promptDraft),
                              baseline: cloneValue(activePromptBaseline),
                              serverConflict: nextPromptCacheView[currentKey]?.serverConflict,
                            }
                            setPromptCacheView(nextPromptCacheView)
                          }
                          const key = promptCacheKey(tab as 'system' | 'tool', item.prompt_key)
                          const entry = nextPromptCacheView[key]
                          if (entry?.serverConflict) {
                            setConflictPrompt({ key, local: entry.draft, server: entry.serverConflict })
                            return
                          }
                          const draft = entry ? cloneValue(entry.draft) : cloneValue(item)
                          const baseline = entry ? cloneValue(entry.baseline) : cloneValue(item)
                          applyPromptSelection(item.prompt_key, draft, baseline)
                          setShowAllPromptModules(false)
                        }}
                        className={`sl-list-item${active ? ' active' : ''}${dirty ? ' sl-dirty-item' : ''}${conflict ? ' sl-conflict-item' : ''}${!item.enabled ? ' sl-list-item-disabled' : ''}`}
                      >
                        <div className="sl-row">
                          <span className="sl-name">
                            {conflict && <span className="sl-conflict-icon-inline" title="存在未解决的服务端冲突">⚡</span>}
                            {displayTitle}
                          </span>
                          <span className="sl-meta">{dirty ? <span className="sl-dirty-tag">编辑中</span> : (item.override ? '已覆盖' : '默认')}</span>
                        </div>
                        {displaySummary && <div className="sl-desc">{displaySummary}</div>}
                        <div className="sl-key">{item.prompt_key}</div>
                        {(tab === 'tool' || tab === 'system') && (
                          <div className="sl-chips">
                            {!item.enabled && <span className="sl-chip amber">未启用</span>}
                            {item.override && <span className="sl-chip">已覆盖默认</span>}
                          </div>
                        )}
                      </button>
                    )
                  }
                  if (tab === 'tool' || tab === 'system') {
                    return (
                      <>
                        {promptGroups.map((group) => {
                          const groupCollapsed = collapsedPromptGroups[`${tab}::${group.key}`] ?? false
                          return (
                            <div key={group.key} className="sl-group-block">
                              <button
                                type="button"
                                className="sl-group-label sl-group-toggle"
                                onClick={() => togglePromptGroup(tab, group.key)}
                              >
                                <span className="sl-group-caret">{groupCollapsed ? '▸' : '▾'}</span>
                                {group.label}
                                <span className="sl-group-count">({group.items.length})</span>
                              </button>
                              {!groupCollapsed && group.hint ? <div className="sl-group-help">{group.hint}</div> : null}
                              {!groupCollapsed && group.items.map(renderPrompt)}
                            </div>
                          )
                        })}
                      </>
                    )
                  }
                  const enabled  = promptItems.filter((i) => i.enabled)
                  const disabled = promptItems.filter((i) => !i.enabled)
                  const collapsed = disabledCollapsed[tab]
                  return (
                    <>
                      {enabled.length > 0 && (
                        <>
                          <div className="sl-group-label">启用中 ({enabled.length})</div>
                          {enabled.map(renderPrompt)}
                        </>
                      )}
                      {disabled.length > 0 && (
                        <>
                          <button
                            type="button"
                            className="sl-group-label muted sl-group-toggle"
                            onClick={() => toggleDisabledCollapsed(tab)}
                            title={collapsed ? '展开未启用分组' : '折叠未启用分组'}
                          >
                            <span className="sl-group-caret">{collapsed ? '▸' : '▾'}</span>
                            未启用 ({disabled.length})
                          </button>
                          {!collapsed && disabled.map(renderPrompt)}
                        </>
                      )}
                    </>
                  )
                })()}
          </div>
        </aside>

        <main className="sl-main">
          {tab === 'skill' ? (
            <>
              <div className="sl-editor-pane">
                <div className="sl-main-inner">
                  {skillDraft ? (
                    <>
                      {/* ── 页面级控制栏（启用/暴露，始终可见）────────── */}
                      <div className={`sl-page-controls${skillControlsDirty ? ' sl-pc-dirty' : ''}`}>
                        <label className="sl-pc-check">
                          <input type="checkbox" checked={skillDraft.enabled} onChange={(e) => updateSkillDraft('enabled', e.target.checked)} />
                          启用
                        </label>
                        <label className="sl-pc-check">
                          <input type="checkbox" checked={skillDraft.default_exposed} onChange={(e) => updateSkillDraft('default_exposed', e.target.checked)} />
                          默认暴露给 AI
                        </label>
                        <span className="sl-pc-meta">调用次数：{skillDraft.usage_count}</span>
                      </div>

                      {/* ── 基本信息（可收起） ───────────────────────── */}
                      <section className={`sl-card${skillBasicDirty ? ' sl-section-dirty' : ''}`}>
                        <div className="sl-card-head">
                          <div>
                            <h3>基本信息<span className="sl-vis-tag ai">AI 可见</span></h3>
                            {!skillBasicCollapsed && (
                              <div className="sl-sub">这些字段会作为 YAML 头部 + Markdown 标题/摘要写入实际 SKILL 文件，AI 加载该文件时即可读取。</div>
                            )}
                          </div>
                          <div className="sl-card-actions">
                            <button type="button" className="btn tiny" onClick={() => setSkillBasicCollapsed((v) => !v)}>
                              {skillBasicCollapsed ? '展开 ▾' : '收起 ▴'}
                            </button>
                          </div>
                        </div>
                        {skillBasicCollapsed ? (
                          <div className="sl-collapsed-summary">
                            <span className="sl-cs-title">{skillDraft.title || '(未命名)'}</span>
                            <span className="sl-cs-sep">·</span>
                            <span className="sl-cs-meta">{skillDraft.step_id || '未绑定步骤'}</span>
                          </div>
                        ) : (
                          <>
                            <div className="sl-grid cols-2">
                              <label className="sl-field">
                                <span className="sl-label">标题</span>
                                <input className="sl-input-ai" value={skillDraft.title} onChange={(e) => updateSkillDraft('title', e.target.value)} placeholder="例如：表格补全规则" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">Slug</span>
                                <input className="sl-input-ai" value={skillDraft.slug ?? ''} onChange={(e) => updateSkillDraft('slug', e.target.value)} placeholder="kebab-case-id" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">绑定步骤 ID</span>
                                <input className="sl-input-ai" value={skillDraft.step_id} onChange={(e) => updateSkillDraft('step_id', e.target.value)} placeholder="step.execute / step.review …" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">来源</span>
                                <input className="sl-input-ai" value={skillDraft.source} onChange={(e) => updateSkillDraft('source', e.target.value)} placeholder="user / system" />
                              </label>
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">摘要</span>
                              <AutoTextarea markdown className="sl-input-ai" value={skillDraft.summary} onChange={(e) => updateSkillDraft('summary', e.target.value)} placeholder="一两句话说明这个 SKILL 解决的问题（写入文件 > 引言）" />
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">说明</span>
                              <AutoTextarea markdown className="sl-input-ai" value={skillDraft.description} onChange={(e) => updateSkillDraft('description', e.target.value)} placeholder="详细描述使用场景、输入输出约束等（写入文件正文）" />
                            </div>
                          </>
                        )}
                      </section>

                      {/* ── SKILL 文件预览（基本信息后、模块前）：实时根据当前草稿渲染 ────── */}
                      {(() => {
                        const livePreview = renderSkillMarkdown(skillDraft)
                        const persistedDiff = !!skillDraft.generated_content && skillDraft.generated_content.trim() !== livePreview.trim()
                        return (
                          <div className="sl-preview-inline">
                            <div className="sl-preview-head">
                              <div className="sl-preview-title">
                                📄 实际 SKILL 文件
                                <span className="sl-preview-tag">实时预览</span>
                                {persistedDiff && <span className="sl-preview-tag warn" title="已生成的文件与当前内容不一致，保存或点击「生成实际文件」可同步">未同步</span>}
                              </div>
                              <div className="sl-preview-meta">
                                {skillDraft.generated_file_path || `skills/${skillDraft.slug || '<slug>'}.md`}
                                <span className="sl-preview-tokens" title="按 3.5 字符 ≈ 1 token 粗略估算">
                                  · 约 {estimateTokens(livePreview)} tokens
                                </span>
                              </div>
                            </div>
                            <pre className="sl-preview-body">{livePreview}</pre>
                          </div>
                        )
                      })()}

                      {/* ── 内容模块 ─────────────────────────────────── */}
                      <section className="sl-card">
                        <div className="sl-card-head">
                          <div>
                            <h3>内容模块<span className="sl-vis-tag ai">AI 可见</span></h3>
                            <div className="sl-sub">
                              模块内容会被实际拼装注入到 AI 上下文；模块标题仅供开发者识别。<strong>必要</strong> = 总是发送给 AI；
                              <strong>启用</strong> = 仅对可选模块生效，勾选后才发送。
                            </div>
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
                            const modDirty = dirtySkillModuleIndices.has(realIndex)
                            const singleModule = visibleSkillModules.length === 1
                            return (
                              <div key={`${module.id ?? 'new'}-${realIndex}`} className={`sl-module${module.required ? ' required' : ''}${modDirty ? ' sl-module-dirty' : ''}`}>
                                <div className="sl-module-head">
                                  <input
                                    className="sl-mtitle sl-input-dev"
                                    value={module.title}
                                    onChange={(e) => updateSkillModule(realIndex, { title: e.target.value })}
                                    placeholder="模块标题（仅开发者）"
                                    title="模块标题仅供开发者识别，不会发送给 AI"
                                  />
                                  {modDirty && <span className="sl-dirty-tag">编辑中</span>}
                                  <div className="sl-flags">
                                    <label>
                                      <input
                                        type="checkbox"
                                        checked={module.required}
                                        onChange={(e) => updateSkillModule(realIndex, { required: e.target.checked, enabled: e.target.checked ? true : module.enabled })}
                                      />
                                      必要（总是发送）
                                    </label>
                                    <label style={{ opacity: module.required ? 0.55 : 1 }}>
                                      <input
                                        type="checkbox"
                                        checked={module.enabled || module.required}
                                        disabled={module.required}
                                        onChange={(e) => updateSkillModule(realIndex, { enabled: e.target.checked })}
                                      />
                                      启用（仅可选模块）
                                    </label>
                                  </div>
                                </div>
                                <AutoTextarea
                                  className="sl-module-content sl-input-ai"
                                  value={module.content}
                                  maxRows={singleModule ? 30 : 10}
                                  markdown
                                  onChange={(e) => updateSkillModule(realIndex, { content: e.target.value })}
                                  placeholder="模块内容（AI 可见，支持 Markdown；Tab 缩进 / Enter 续列表 / Ctrl+B 粗体 / Ctrl+I 斜体）"
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
                    </>
                  ) : (
                    <section className="sl-card">
                      <div className="sl-empty">暂无可编辑的 SKILL。</div>
                    </section>
                  )}
                </div>
              </div>
            </>
          ) : (
            <>
              <div className="sl-editor-pane">
                <div className="sl-main-inner">
                  {promptDraft ? (
                    <>
                      {/* ── 页面级控制栏 ─────────────────────────────── */}
                      <div className={`sl-page-controls${promptControlsDirty ? ' sl-pc-dirty' : ''}`}>
                        <label className="sl-pc-check">
                          <input type="checkbox" checked={promptDraft.enabled} onChange={(e) => updatePromptDraft('enabled', e.target.checked)} />
                          启用
                        </label>
                        <span className="sl-pc-meta">{promptDraft.override ? '已覆盖默认内容' : '使用系统默认内容'}</span>
                      </div>

                      <section className={`sl-card${promptBasicDirty ? ' sl-section-dirty' : ''}`}>
                        <div className="sl-card-head">
                          <div>
                            <h3>提示词信息<span className="sl-vis-tag dev">仅开发者</span></h3>
                            {!promptBasicCollapsed && (
                              <div className="sl-sub">{tab === 'system' ? '系统提示词' : '工具提示词'}的元数据与说明，仅用于团队协作记录。</div>
                            )}
                          </div>
                          <div className="sl-card-actions">
                            <button type="button" className="btn tiny" onClick={() => setPromptBasicCollapsed((v) => !v)}>
                              {promptBasicCollapsed ? '展开 ▾' : '收起 ▴'}
                            </button>
                          </div>
                        </div>
                        {promptBasicCollapsed ? (
                          <div className="sl-collapsed-summary">
                            <span className="sl-cs-title">{promptDraft.title}</span>
                            <span className="sl-cs-sep">·</span>
                            <span className="sl-cs-meta">{promptDraft.prompt_key}</span>
                          </div>
                        ) : (
                          <>
                            <div className="sl-grid cols-2">
                              <label className="sl-field">
                                <span className="sl-label">标题</span>
                                <input
                                  className="sl-input-dev"
                                  value={(tab === 'tool' || tab === 'system') ? (promptDraft.tool_name_zh || promptDraft.title) : promptDraft.title}
                                  onChange={(tab === 'tool' || tab === 'system') ? undefined : (e) => updatePromptDraft('title', e.target.value)}
                                  readOnly={tab === 'tool' || tab === 'system'}
                                />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">引用 Key</span>
                                <input className="sl-input-dev" value={promptDraft.prompt_key} readOnly style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)' }} />
                              </label>
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">摘要</span>
                              <AutoTextarea className="sl-input-dev" value={promptDraft.summary} onChange={(e) => updatePromptDraft('summary', e.target.value)} placeholder="简要说明此提示词的用途" />
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">引用说明</span>
                              <AutoTextarea className="sl-input-dev" value={promptDraft.reference_note} onChange={(e) => updatePromptDraft('reference_note', e.target.value)} placeholder="说明在哪些场景被引用、注入位置等" />
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">说明</span>
                              <AutoTextarea className="sl-input-dev" value={promptDraft.description} onChange={(e) => updatePromptDraft('description', e.target.value)} placeholder="补充信息、注意事项" />
                            </div>
                          </>
                        )}
                      </section>

                      {/* ── 运行时预览（基本信息后、模块前）────────── */}
                      <div className="sl-preview-inline">
                        <div className="sl-preview-head">
                          <div className="sl-preview-title">
                            {tab === 'tool' ? '🔧 工具字段预览（发送给 AI 的 description 内容）' : '🔭 运行时拼装预览（发送给 AI 的文本）'}
                          </div>
                          <div className="sl-preview-meta">
                            {promptDraft.prompt_key || '—'}
                            {runtimePreview && (
                              <span className="sl-preview-tokens" title="按 3.5 字符 ≈ 1 token 粗略估算">
                                · 约 {estimateTokens(runtimePreview)} tokens
                              </span>
                            )}
                          </div>
                        </div>
                        <pre className={`sl-preview-body${runtimePreview ? '' : ' empty'}`}>
                          {runtimePreview || '当前无启用模块内容。'}
                        </pre>
                      </div>
                      {promptDraft.default_modules?.length ? (
                        <details className="sl-card">
                          <summary style={{ cursor: 'pointer', fontWeight: 600 }}>查看系统默认版本</summary>
                          <div className="sl-sub" style={{ marginTop: '0.65rem' }}>
                            这里展示未覆盖时真正会发送给 AI 的默认内容，用来对照当前草稿是否偏离。
                          </div>
                          <div className="sl-preview-inline" style={{ marginTop: '0.75rem' }}>
                            <div className="sl-preview-head">
                              <div className="sl-preview-title">默认版本</div>
                              <div className="sl-preview-meta">
                                {promptDraft.prompt_key || '—'}
                                {defaultRuntimePreview && (
                                  <span className="sl-preview-tokens" title="按 3.5 字符 ≈ 1 token 粗略估算">
                                    · 约 {estimateTokens(defaultRuntimePreview)} tokens
                                  </span>
                                )}
                              </div>
                            </div>
                            <pre className={`sl-preview-body${defaultRuntimePreview ? '' : ' empty'}`}>
                              {defaultRuntimePreview || '默认版本当前无启用模块内容。'}
                            </pre>
                          </div>
                        </details>
                      ) : null}

                      <section className="sl-card">
                        <div className="sl-card-head">
                          <div>
                            <h3>内容模块<span className="sl-vis-tag ai">AI 可见</span></h3>
                            <div className="sl-sub">
                              {tab === 'tool'
                                ? '每个模块对应工具 JSON schema 里的一个 description 字段（路径即 module_key）。AI 收到工具列表时，这些文字就是它理解该工具和参数用途的依据。'
                                : '模块内容会被运行时拼装注入；模块 key / 标题仅供开发者识别。'}
                            </div>
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
                            const modDirty = dirtyPromptModuleIndices.has(realIndex)
                            const singleModule = visiblePromptModules.length === 1
                            return (
                              <div key={`${module.id ?? 'default'}-${module.module_key ?? realIndex}`} className={`sl-module${module.required ? ' required' : ''}${modDirty ? ' sl-module-dirty' : ''}`}>
                                <div className="sl-module-head">
                                  <input className="sl-mkey sl-input-dev" value={module.module_key ?? ''} readOnly title={module.module_key ?? ''} />
                                  <input
                                    className="sl-mtitle sl-input-dev"
                                    value={module.title}
                                    onChange={(e) => updatePromptModule(realIndex, { title: e.target.value })}
                                    placeholder="模块标题（仅开发者）"
                                  />
                                  {modDirty && <span className="sl-dirty-tag">编辑中</span>}
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
                                <AutoTextarea
                                  className="sl-module-content sl-input-ai"
                                  value={module.content}
                                  maxRows={singleModule ? 30 : 10}
                                  markdown
                                  onChange={(e) => updatePromptModule(realIndex, { content: e.target.value })}
                                  placeholder="模块内容（AI 可见，支持 Markdown；Tab 缩进 / Enter 续列表 / Ctrl+B 粗体 / Ctrl+I 斜体）"
                                />
                              </div>
                            )
                          })}
                          {visiblePromptModules.length === 0 && (
                            <div className="sl-empty">该提示词暂无可见模块。</div>
                          )}
                        </div>
                      </section>
                    </>
                  ) : (
                    <section className="sl-card">
                      <div className="sl-empty">请选择左侧列表中的一项进行编辑。</div>
                    </section>
                  )}
                </div>
              </div>
            </>
          )}
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

      {/* ── 冲突对话框：SKILL ───────────────────────────────────────────── */}
      {conflictSkill && (
        <div className="sl-conflict-overlay">
          <div className="sl-conflict-dialog">
            <div className="sl-conflict-icon">⚠️</div>
            <h3>编辑冲突</h3>
            <p>
              你对 <strong>{conflictSkill.local.title || conflictSkill.key}</strong> 有未保存的本地编辑，
              同时服务端该条目已被更新。请选择保留哪个版本：
            </p>
            <div className="sl-conflict-diff">
              <div className="sl-conflict-side local">
                <div className="sl-conflict-side-title">📝 本地编辑版本</div>
                <div className="sl-conflict-side-body">{conflictSkill.local.title}<br />{conflictSkill.local.summary}</div>
              </div>
              <div className="sl-conflict-side server">
                <div className="sl-conflict-side-title">☁️ 服务端最新版本</div>
                <div className="sl-conflict-side-body">{conflictSkill.server.title}<br />{conflictSkill.server.summary}</div>
              </div>
            </div>
            <div className="sl-conflict-actions">
              <button
                type="button"
                className="btn primary"
                onClick={() => {
                  const entry = skillCache.current.get(conflictSkill.key)
                  if (entry) delete entry.serverConflict
                  syncSkillCacheView()
                  clearSkillConflict(conflictSkill.key)
                  setEditingSkillId(Number(conflictSkill.key))
                  setSkillDraft(cloneValue(conflictSkill.local))
                  setActiveSkillBaseline(entry ? cloneValue(entry.baseline) : cloneValue(conflictSkill.local))
                  setConflictSkill(null)
                }}
              >保留本地编辑</button>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  const entry = skillCache.current.get(conflictSkill.key)
                  if (entry) {
                    entry.draft = cloneValue(conflictSkill.server)
                    entry.baseline = cloneValue(conflictSkill.server)
                    delete entry.serverConflict
                  }
                  syncSkillCacheView()
                  clearSkillDirty(conflictSkill.key)
                  clearSkillConflict(conflictSkill.key)
                  setEditingSkillId(Number(conflictSkill.key))
                  setSkillDraft(cloneValue(conflictSkill.server))
                  setActiveSkillBaseline(cloneValue(conflictSkill.server))
                  setConflictSkill(null)
                }}
              >使用服务端版本</button>
              <button type="button" className="btn ghost" onClick={() => setConflictSkill(null)}>稍后再说</button>
            </div>
          </div>
        </div>
      )}

      {/* ── 冲突对话框：提示词 ──────────────────────────────────────────── */}
      {conflictPrompt && (
        <div className="sl-conflict-overlay">
          <div className="sl-conflict-dialog">
            <div className="sl-conflict-icon">⚠️</div>
            <h3>编辑冲突</h3>
            <p>
              你对 <strong>{conflictPrompt.local.title || conflictPrompt.local.prompt_key}</strong> 有未保存的本地编辑，
              同时服务端该条目已被更新。请选择保留哪个版本：
            </p>
            <div className="sl-conflict-diff">
              <div className="sl-conflict-side local">
                <div className="sl-conflict-side-title">📝 本地编辑版本</div>
                <div className="sl-conflict-side-body">{conflictPrompt.local.title}<br />{conflictPrompt.local.summary}</div>
              </div>
              <div className="sl-conflict-side server">
                <div className="sl-conflict-side-title">☁️ 服务端最新版本</div>
                <div className="sl-conflict-side-body">{conflictPrompt.server.title}<br />{conflictPrompt.server.summary}</div>
              </div>
            </div>
            <div className="sl-conflict-actions">
              <button
                type="button"
                className="btn primary"
                onClick={() => {
                  const entry = promptCache.current.get(conflictPrompt.key)
                  if (entry) delete entry.serverConflict
                  syncPromptCacheView()
                  clearPromptConflict(conflictPrompt.key)
                  setEditingPromptKey(conflictPrompt.local.prompt_key)
                  setPromptDraft(cloneValue(conflictPrompt.local))
                  setActivePromptBaseline(entry ? cloneValue(entry.baseline) : cloneValue(conflictPrompt.local))
                  setConflictPrompt(null)
                }}
              >保留本地编辑</button>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  const entry = promptCache.current.get(conflictPrompt.key)
                  if (entry) {
                    entry.draft = cloneValue(conflictPrompt.server)
                    entry.baseline = cloneValue(conflictPrompt.server)
                    delete entry.serverConflict
                  }
                  syncPromptCacheView()
                  clearPromptDirty(conflictPrompt.key)
                  clearPromptConflict(conflictPrompt.key)
                  setEditingPromptKey(conflictPrompt.server.prompt_key)
                  setPromptDraft(cloneValue(conflictPrompt.server))
                  setActivePromptBaseline(cloneValue(conflictPrompt.server))
                  setConflictPrompt(null)
                }}
              >使用服务端版本</button>
              <button type="button" className="btn ghost" onClick={() => setConflictPrompt(null)}>稍后再说</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
