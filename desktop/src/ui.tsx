import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { LucideIcon } from 'lucide-react'
import { AlertCircle, Inbox, RefreshCw, X } from 'lucide-react'

export type ToastTone = 'success' | 'danger' | 'info'
export type Toast = { id: number; message: string; tone: ToastTone }

type ToastContextValue = {
  pushToast(message: string, tone?: ToastTone): void
  dismissToast(id: number): void
}

const ToastContext = createContext<ToastContextValue | null>(null)

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = useState<Toast[]>([])
  const pushToast = useCallback((message: string, tone: ToastTone = 'info') => {
    const id = Date.now() + Math.random()
    setItems(current => [...current.slice(-3), { id, message, tone }])
    window.setTimeout(() => setItems(current => current.filter(item => item.id !== id)), 5000)
  }, [])
  const dismissToast = useCallback((id: number) => setItems(current => current.filter(item => item.id !== id)), [])
  const value = useMemo(() => ({ pushToast, dismissToast }), [dismissToast, pushToast])
  return <ToastContext.Provider value={value}>{children}<ToastViewport items={items} dismiss={dismissToast} /></ToastContext.Provider>
}

export function useToast() {
  const value = useContext(ToastContext)
  if (!value) throw new Error('useToast must be used inside ToastProvider')
  return value
}

export function ToastViewport({ items, dismiss }: { items: Toast[]; dismiss(id: number): void }) {
  return <div className="toast-viewport" aria-live="polite" aria-atomic="false">
    {items.map(item => <div className={`toast toast-${item.tone}`} role="status" key={item.id}>
      <span>{item.message}</span>
      <button aria-label="关闭通知" onClick={() => dismiss(item.id)}><X size={14} /></button>
    </div>)}
  </div>
}

export function PageHeader({ title, subtitle, actions }: { title: string; subtitle?: string; actions?: React.ReactNode }) {
  return <header className="page-head"><div><h1>{title}</h1>{subtitle && <p>{subtitle}</p>}</div>{actions}</header>
}

export function Page({ title, subtitle, actions, children, className = '' }: { title: string; subtitle?: string; actions?: React.ReactNode; children: React.ReactNode; className?: string }) {
  return <section className={`page ${className}`.trim()}><PageHeader title={title} subtitle={subtitle} actions={actions} /><div className="page-body">{children}</div></section>
}

export function LoadingState({ label = '正在加载…' }: { label?: string }) {
  return <div className="busy" role="status" aria-live="polite"><RefreshCw size={18} /><span>{label}</span></div>
}

function errorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  if (typeof error === 'string') return error
  if (error && typeof error === 'object' && 'message' in error) return String((error as { message?: unknown }).message)
  return '暂时无法读取数据，请稍后重试。'
}

export function ErrorState({ error, retry, title = '加载失败' }: { error: unknown; retry?: () => void; title?: string }) {
  return <div className="error-box" role="alert"><div className="error-box-content"><AlertCircle size={17} /><div><strong>{title}</strong><p>{errorMessage(error)}</p></div></div>{retry && <button onClick={retry}>重试</button>}</div>
}

export function QueryState({ loading, error, retry, empty, icon, emptyTitle = '暂无数据', emptyHint, children }: { loading?: boolean; error?: unknown; retry?: () => void; empty?: boolean; icon?: LucideIcon; emptyTitle?: string; emptyHint?: string; children: React.ReactNode }) {
  if (loading) return <LoadingState />
  if (error) return <ErrorState error={error} retry={retry} />
  if (empty) return <EmptyState icon={icon} title={emptyTitle} hint={emptyHint} />
  return <>{children}</>
}

export function EmptyState({ icon: Icon = Inbox, title, hint, action }: { icon?: LucideIcon; title: string; hint?: string; action?: React.ReactNode }) {
  return <div className="empty"><Icon size={28} /><h3>{title}</h3>{hint && <p>{hint}</p>}{action}</div>
}

export function HostOfflineBanner({ visible, status }: { visible: boolean; status: string }) {
  if (!visible || status !== 'offline') return null
  return <div className="host-banner" role="status"><span />Host 连接已断开，正在重连…</div>
}

export function statusChinese(value: string) {
  return ({ pending: '待处理', pending_review: '待确认', active: '进行中', running: '运行中', blocked: '受阻', completed: '已完成', failed: '失败', interrupted: '已中断', cancelled: '已取消', paused: '已暂停', approved: '已允许', denied: '已拒绝', available: '可用', unavailable: '不可用', restart_required: '需重启', archived: '已归档', healthy: '正常', degraded: '降级', disconnected: '未连接', awaiting_approval: '等待审批', succeeded: '已成功', partial_success: '部分成功', uncertain: '结果未知' } as Record<string, string>)[value] || value
}

export function StatusBadge({ value, label }: { value: string; label?: string }) {
  return <span className={`status ${value}`}>{label || statusChinese(value)}</span>
}

// Keep a single effect for future consumers that need to announce transient UI state.
export function useMounted() {
  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])
  return mounted
}
