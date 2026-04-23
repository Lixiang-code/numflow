const API = '/api'

export async function apiFetch(path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers)
  if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }
  const res = await fetch(`${API}${path}`, {
    ...init,
    headers,
    credentials: 'include',
  })
  const text = await res.text()
  let data: unknown = null
  if (text) {
    try {
      data = JSON.parse(text) as unknown
    } catch {
      data = text
    }
  }
  if (!res.ok) {
    const msg =
      typeof data === 'object' && data !== null && 'detail' in data
        ? String((data as { detail: unknown }).detail)
        : res.statusText
    throw new Error(msg || `HTTP ${res.status}`)
  }
  return data
}

export function projectHeaders(projectId: number): HeadersInit {
  return { 'X-Project-Id': String(projectId) }
}
