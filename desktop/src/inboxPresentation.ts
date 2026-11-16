const DEFAULT_PREVIEW_LENGTH = 96

/** Keep inbox cards concise; the expanded card still shows the full body. */
export function compactInboxPreview(value: string | null | undefined, limit = DEFAULT_PREVIEW_LENGTH): string {
  const text = String(value || '').replace(/\s+/g, ' ').trim()
  if (!text) return '没有摘要'
  if (text.length <= limit) return text
  return `${text.slice(0, Math.max(1, limit)).trimEnd()}…`
}
