import { useCallback, useEffect, useLayoutEffect, useRef, type KeyboardEvent as ReactKeyboardEvent, type TextareaHTMLAttributes } from 'react'

const INDENT = '  '
const DEFAULT_MAX_ROWS = 10

function mdInsert(el: HTMLTextAreaElement, text: string): void {
  if (!document.execCommand || !document.execCommand('insertText', false, text)) {
    const { selectionStart: s, selectionEnd: e } = el
    el.setRangeText(text, s, e, 'end')
    el.dispatchEvent(new Event('input', { bubbles: true }))
  }
}

function handleMarkdownKeyDown(e: ReactKeyboardEvent<HTMLTextAreaElement>): void {
  const el = e.currentTarget
  const { selectionStart: s, selectionEnd: ee, value } = el

  if (e.key === 'Tab') {
    e.preventDefault()
    const lineStart = value.lastIndexOf('\n', s - 1) + 1
    const probeEnd = ee > s ? ee - 1 : ee
    const lineEndIdx = value.indexOf('\n', probeEnd)
    const lineEnd = lineEndIdx === -1 ? value.length : lineEndIdx
    const block = value.slice(lineStart, lineEnd)
    const lines = block.split('\n')
    let delta = 0
    let firstDelta = 0
    const next = lines.map((l, i) => {
      if (e.shiftKey) {
        let removed = 0
        let out = l
        if (l.startsWith(INDENT)) { out = l.slice(INDENT.length); removed = INDENT.length }
        else if (l.startsWith(' ')) { out = l.slice(1); removed = 1 }
        if (i === 0) firstDelta = -removed
        delta -= removed
        return out
      }
      if (i === 0) firstDelta = INDENT.length
      delta += INDENT.length
      return INDENT + l
    })
    const replaced = next.join('\n')
    el.setSelectionRange(lineStart, lineEnd)
    mdInsert(el, replaced)
    if (s === ee) {
      const cursor = Math.max(lineStart, s + firstDelta)
      el.setSelectionRange(cursor, cursor)
    } else {
      const newStart = Math.max(lineStart, s + firstDelta)
      const newEnd = lineStart + block.length + delta
      el.setSelectionRange(newStart, newEnd)
    }
    return
  }

  if (e.key === 'Enter' && !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey) {
    if (s !== ee) return
    const lineStart = value.lastIndexOf('\n', s - 1) + 1
    const lineEndIdx = value.indexOf('\n', s)
    const lineEnd = lineEndIdx === -1 ? value.length : lineEndIdx
    const line = value.slice(lineStart, lineEnd)
    const m = line.match(/^(\s*)(?:([-*+>])|(\d+)\.)(\s+)(.*)$/)
    if (!m) return
    const indent = m[1] ?? ''
    const space = m[4]
    const rest = m[5]
    const beforeCursor = value.slice(lineStart, s)
    const prefixLen = (m[1] ?? '').length + (m[2] ? 1 : (m[3]!.length + 1)) + space.length
    if (beforeCursor.length < prefixLen) return
    if (rest.trim() === '') {
      e.preventDefault()
      el.setSelectionRange(lineStart, lineEnd)
      mdInsert(el, '')
      return
    }
    e.preventDefault()
    const newBullet = m[2] ? m[2] : `${Number(m[3]) + 1}.`
    mdInsert(el, '\n' + indent + newBullet + space)
    return
  }

  if ((e.metaKey || e.ctrlKey) && !e.altKey && (e.key === 'b' || e.key === 'B' || e.key === 'i' || e.key === 'I')) {
    const isBold = e.key === 'b' || e.key === 'B'
    e.preventDefault()
    const sel = value.slice(s, ee)
    const wrap = isBold ? '**' : '*'
    const placeholder = isBold ? '粗体' : '斜体'
    const inner = sel || placeholder
    mdInsert(el, `${wrap}${inner}${wrap}`)
    if (!sel) {
      const cursor = s + wrap.length
      el.setSelectionRange(cursor, cursor + placeholder.length)
    }
  }
}

export type AutoTextareaProps = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  value: string
  maxRows?: number
  markdown?: boolean
}

export default function AutoTextarea({
  value,
  className,
  onInput,
  onKeyDown,
  maxRows = DEFAULT_MAX_ROWS,
  markdown,
  ...rest
}: AutoTextareaProps) {
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
      className={['app-autoresize', className].filter(Boolean).join(' ')}
      onInput={(e) => {
        resize()
        onInput?.(e)
      }}
      onKeyDown={(e) => {
        if (markdown) handleMarkdownKeyDown(e)
        if (!e.defaultPrevented) onKeyDown?.(e)
      }}
      rows={1}
      {...rest}
    />
  )
}
