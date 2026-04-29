import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { TextareaHTMLAttributes } from 'react'
import { Link, useParams } from 'react-router-dom'
import { apiFetch, projectHeaders } from '../api'

/**
 * 自适应高度文本框：默认显示「内容行数 + 1」行，超过 10 行则定高 + 出现滚动条。
 * 同时禁用拖动 resize，以保证页面节奏稳定。
 */
type AutoTextareaProps = TextareaHTMLAttributes<HTMLTextAreaElement> & { value: string; maxRows?: number }
const DEFAULT_MAX_ROWS = 10

function AutoTextarea({ value, className, onInput, maxRows = DEFAULT_MAX_ROWS, ...rest }: AutoTextareaProps) {
  const ref = useRef<HTMLTextAreaElement | null>(null)

  const resize = useCallback(() => {
    const el = ref.current
    if (!el) return
    const cs = window.getComputedStyle(el)
    const lh = parseFloat(cs.lineHeight) || parseFloat(cs.fontSize) * 1.4 || 20
    const pt = parseFloat(cs.paddingTop) || 0
    const pb = parseFloat(cs.paddingBottom) || 0
    const bt = parseFloat(cs.borderTopWidth) || 0
    const bb = parseFloat(cs.borderBottomWidth) || 0
    const maxH = lh * maxRows + pt + pb + bt + bb
    el.style.height = 'auto'
    // scrollHeight 含 padding，加 1 行 buffer 让用户继续输入
    const desired = el.scrollHeight + lh + bt + bb
    const finalH = Math.min(desired, maxH)
    el.style.height = `${finalH}px`
    el.style.overflowY = desired > maxH ? 'auto' : 'hidden'
  }, [maxRows])

  useLayoutEffect(() => {
    resize()
  }, [resize, value])

  useEffect(() => {
    const handle = () => resize()
    window.addEventListener('resize', handle)
    return () => window.removeEventListener('resize', handle)
  }, [resize])

  return (
    <textarea
      ref={ref}
      value={value}
      className={['sl-autoresize', className].filter(Boolean).join(' ')}
      onInput={(e) => {
        resize()
        onInput?.(e)
      }}
      rows={1}
      {...rest}
    />
  )
}

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

function deepEqual(a: unknown, b: unknown): boolean { return JSON.stringify(a) === JSON.stringify(b) }
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
  const [dirtySkillIds,   setDirtySkillIds]   = useState<Set<string>>(new Set())
  const [dirtyPromptKeys, setDirtyPromptKeys] = useState<Set<string>>(new Set())
  const [conflictSkill,  setConflictSkill]  = useState<{ key: string; local: SkillItem;  server: SkillItem  } | null>(null)
  const [conflictPrompt, setConflictPrompt] = useState<{ key: string; local: PromptItem; server: PromptItem } | null>(null)

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
          } else {
            // No local edits, just update to latest server data
            skillCache.current.set(key, { draft: server, baseline: server })
          }
        }
      }
      // Resolve selected item
      const resolveSkillDraft = (id: number): SkillItem => {
        const entry = skillCache.current.get(skillCacheKey(id))
        const found = next.find((s) => s.id === id)!
        return entry ? cloneValue(entry.draft) : cloneValue(found)
      }
      if (selectId === 'new') {
        setEditingSkillId('new')
        setSkillDraft(blankSkill())
      } else if (typeof selectId === 'number') {
        const found = next.find((item) => item.id === selectId)
        if (found) { setEditingSkillId(selectId); setSkillDraft(resolveSkillDraft(selectId)) }
      } else if (next.length > 0) {
        const first = next[0]
        setEditingSkillId(first.id ?? null)
        setSkillDraft(first.id != null ? resolveSkillDraft(first.id) : cloneValue(first))
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
          } else {
            promptCache.current.set(key, { draft: server, baseline: server })
          }
        }
      }
      const resolvePromptDraft = (pk: string): PromptItem => {
        const key = promptCacheKey(category, pk)
        const entry = promptCache.current.get(key)
        const found = next.find((i) => i.prompt_key === pk)!
        return entry ? cloneValue(entry.draft) : cloneValue(found)
      }
      if (selectKey) {
        const found = next.find((item) => item.prompt_key === selectKey)
        if (found) { setEditingPromptKey(selectKey); setPromptDraft(resolvePromptDraft(selectKey)); return }
      }
      const first = next[0] ?? null
      setEditingPromptKey(first?.prompt_key ?? null)
      setPromptDraft(first ? resolvePromptDraft(first.prompt_key) : blankPrompt())
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
    const key = skillCacheKey(skillId)
    const entry = skillCache.current.get(key)
    if (entry?.serverConflict) {
      setConflictSkill({ key, local: entry.draft, server: entry.serverConflict })
      return
    }
    const draft = entry ? cloneValue(entry.draft) : cloneValue(skills.find((s) => s.id === skillId)!)
    setEditingSkillId(skillId)
    setSkillDraft(draft)
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
      // Clear dirty state for the saved item
      const savedKey = skillCacheKey(saved.id ?? 'new')
      const savedEntry = skillCache.current.get(savedKey)
      if (savedEntry) { savedEntry.baseline = cloneValue(saved); delete savedEntry.serverConflict }
      clearSkillDirty(savedKey)
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
    const key = promptCacheKey(tab as 'system' | 'tool', promptKey)
    const entry = promptCache.current.get(key)
    if (entry?.serverConflict) {
      setConflictPrompt({ key, local: entry.draft, server: entry.serverConflict })
      return
    }
    const draft = entry ? cloneValue(entry.draft) : cloneValue(promptItems.find((i) => i.prompt_key === promptKey)!)
    setEditingPromptKey(promptKey)
    setPromptDraft(draft)
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
      // Clear dirty state for the saved prompt
      const savedPKey = promptCacheKey(category, promptDraft.prompt_key)
      const savedPEntry = promptCache.current.get(savedPKey)
      if (savedPEntry) { savedPEntry.baseline = cloneValue(promptDraft); delete savedPEntry.serverConflict }
      clearPromptDirty(savedPKey)
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

  // ── Per-module and section dirty detection ────────────────────────────────
  const dirtySkillModuleIndices = useMemo((): Set<number> => {
    if (!editingSkillId || !skillDraft) return new Set()
    const entry = skillCache.current.get(skillCacheKey(editingSkillId))
    if (!entry) return new Set()
    const result = new Set<number>()
    const bMods = entry.baseline.modules
    skillDraft.modules.forEach((mod, i) => {
      if (i >= bMods.length || !deepEqual(mod, bMods[i])) result.add(i)
    })
    return result
  }, [editingSkillId, skillDraft])

  const dirtyPromptModuleIndices = useMemo((): Set<number> => {
    if (!editingPromptKey || !promptDraft || tab === 'skill') return new Set()
    const entry = promptCache.current.get(promptCacheKey(tab, editingPromptKey))
    if (!entry) return new Set()
    const result = new Set<number>()
    const bMods = entry.baseline.modules
    promptDraft.modules.forEach((mod, i) => {
      if (i >= bMods.length || !deepEqual(mod, bMods[i])) result.add(i)
    })
    return result
  }, [editingPromptKey, promptDraft, tab])

  const skillBasicDirty = useMemo(() => {
    if (!editingSkillId || !skillDraft) return false
    const entry = skillCache.current.get(skillCacheKey(editingSkillId))
    if (!entry) return false
    const b = entry.baseline
    return skillDraft.title !== b.title || skillDraft.slug !== b.slug ||
      skillDraft.step_id !== b.step_id || skillDraft.source !== b.source ||
      skillDraft.summary !== b.summary || skillDraft.description !== b.description ||
      skillDraft.default_exposed !== b.default_exposed || skillDraft.enabled !== b.enabled
  }, [editingSkillId, skillDraft])

  const promptBasicDirty = useMemo(() => {
    if (!editingPromptKey || !promptDraft || tab === 'skill') return false
    const entry = promptCache.current.get(promptCacheKey(tab, editingPromptKey))
    if (!entry) return false
    const b = entry.baseline
    return promptDraft.title !== b.title || promptDraft.summary !== b.summary ||
      promptDraft.description !== b.description || promptDraft.reference_note !== b.reference_note ||
      promptDraft.enabled !== b.enabled
  }, [editingPromptKey, promptDraft, tab])

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
                    return (
                      <button
                        key={skill.id ?? skill.slug}
                        type="button"
                        onClick={() => skill.id && selectSkill(skill.id)}
                        className={`sl-list-item${active ? ' active' : ''}${dirty ? ' sl-dirty-item' : ''}`}
                      >
                        <div className="sl-row">
                          <span className="sl-name">{skill.title || '(未命名)'}</span>
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
                          <div className="sl-group-label muted">未启用 ({disabled.length})</div>
                          {disabled.map(renderSkill)}
                        </>
                      )}
                    </>
                  )
                })()
              : (() => {
                  const enabled  = promptItems.filter((i) => i.enabled)
                  const disabled = promptItems.filter((i) => !i.enabled)
                  const renderPrompt = (item: PromptItem) => {
                    const active = item.prompt_key === editingPromptKey
                    const dirty  = isPromptDirty(item.prompt_key)
                    return (
                      <button
                        key={item.prompt_key}
                        type="button"
                        onClick={() => selectPrompt(item.prompt_key)}
                        className={`sl-list-item${active ? ' active' : ''}${dirty ? ' sl-dirty-item' : ''}`}
                      >
                        <div className="sl-row">
                          <span className="sl-name">{item.title}</span>
                          <span className="sl-meta">{dirty ? <span className="sl-dirty-tag">编辑中</span> : (item.override ? '已覆盖' : '默认')}</span>
                        </div>
                        <div className="sl-key">{item.prompt_key}</div>
                      </button>
                    )
                  }
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
                          <div className="sl-group-label muted">未启用 ({disabled.length})</div>
                          {disabled.map(renderPrompt)}
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
                      <div className="sl-page-controls">
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
                            <h3>基本信息<span className="sl-vis-tag dev">仅开发者</span></h3>
                            {!skillBasicCollapsed && (
                              <div className="sl-sub">SKILL 元数据，将作为 Markdown 头部 YAML 写入；不会单独发送给 AI。</div>
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
                                <input className="sl-input-dev" value={skillDraft.title} onChange={(e) => updateSkillDraft('title', e.target.value)} placeholder="例如：表格补全规则" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">Slug</span>
                                <input className="sl-input-dev" value={skillDraft.slug ?? ''} onChange={(e) => updateSkillDraft('slug', e.target.value)} placeholder="kebab-case-id" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">绑定步骤 ID</span>
                                <input className="sl-input-dev" value={skillDraft.step_id} onChange={(e) => updateSkillDraft('step_id', e.target.value)} placeholder="step.execute / step.review …" />
                              </label>
                              <label className="sl-field">
                                <span className="sl-label">来源</span>
                                <input className="sl-input-dev" value={skillDraft.source} onChange={(e) => updateSkillDraft('source', e.target.value)} placeholder="user / system" />
                              </label>
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">摘要</span>
                              <AutoTextarea className="sl-input-dev" value={skillDraft.summary} onChange={(e) => updateSkillDraft('summary', e.target.value)} placeholder="一两句话说明这个 SKILL 解决的问题" />
                            </div>

                            <div className="sl-field sl-field-row">
                              <span className="sl-label">说明</span>
                              <AutoTextarea className="sl-input-dev" value={skillDraft.description} onChange={(e) => updateSkillDraft('description', e.target.value)} placeholder="详细描述使用场景、输入输出约束等" />
                            </div>
                          </>
                        )}
                      </section>

                      {/* ── SKILL 文件预览（基本信息后、模块前）────── */}
                      <div className="sl-preview-inline">
                        <div className="sl-preview-head">
                          <div className="sl-preview-title">📄 实际 SKILL 文件</div>
                          <div className="sl-preview-meta">{skillDraft.generated_file_path || '保存后将自动生成 Markdown + YAML'}</div>
                        </div>
                        <pre className={`sl-preview-body${skillDraft.generated_content ? '' : ' empty'}`}>
                          {skillDraft.generated_content || '尚未生成。点击右上角"生成实际文件"按钮即可生成。'}
                        </pre>
                      </div>

                      {/* ── 内容模块 ─────────────────────────────────── */}
                      <section className="sl-card">
                        <div className="sl-card-head">
                          <div>
                            <h3>内容模块<span className="sl-vis-tag ai">AI 可见</span></h3>
                            <div className="sl-sub">模块内容会被实际拼装注入到 AI 上下文；模块标题仅供开发者识别。</div>
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
                                <AutoTextarea
                                  className="sl-module-content sl-input-ai"
                                  value={module.content}
                                  maxRows={singleModule ? 30 : 10}
                                  onChange={(e) => updateSkillModule(realIndex, { content: e.target.value })}
                                  placeholder="模块内容（AI 可见，支持 Markdown）"
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
                      <div className="sl-page-controls">
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
                                <input className="sl-input-dev" value={promptDraft.title} onChange={(e) => updatePromptDraft('title', e.target.value)} />
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
                          <div className="sl-preview-meta">{promptDraft.prompt_key || '—'}</div>
                        </div>
                        <pre className={`sl-preview-body${runtimePreview ? '' : ' empty'}`}>
                          {runtimePreview || '当前无启用模块内容。'}
                        </pre>
                      </div>

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
                                  onChange={(e) => updatePromptModule(realIndex, { content: e.target.value })}
                                  placeholder="模块内容（AI 可见，支持 Markdown）"
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
                  setEditingSkillId(Number(conflictSkill.key))
                  setSkillDraft(cloneValue(conflictSkill.local))
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
                  clearSkillDirty(conflictSkill.key)
                  setEditingSkillId(Number(conflictSkill.key))
                  setSkillDraft(cloneValue(conflictSkill.server))
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
                  setEditingPromptKey(conflictPrompt.local.prompt_key)
                  setPromptDraft(cloneValue(conflictPrompt.local))
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
                  clearPromptDirty(conflictPrompt.key)
                  setEditingPromptKey(conflictPrompt.server.prompt_key)
                  setPromptDraft(cloneValue(conflictPrompt.server))
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
