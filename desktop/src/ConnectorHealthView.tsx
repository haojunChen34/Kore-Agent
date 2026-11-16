import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { CheckCircle2, History, KeyRound, RefreshCw, RotateCw, ShieldAlert, Trash2, Wrench } from 'lucide-react'
import { rpc } from './rpc'
import { EmptyState, ErrorState, LoadingState, Page, StatusBadge, useToast } from './ui'

type Health = { status?: string; connected?: boolean; last_successful_sync?: string | null; last_error?: string | null; last_error_at?: string | null; token_expiring_soon?: boolean; token_expired?: boolean; reauthorize_required?: boolean; retry_count?: number; next_retry_at?: string | null; imap_idle_enabled?: boolean; imap_idle_last_at?: string | null; webhook_enabled?: boolean; last_webhook_at?: string | null }
type Connector = { provider: string; label: string; health: Health; imap_idle_enabled?: boolean; imap_idle_last_at?: string | null; webhook_enabled?: boolean; last_webhook_at?: string | null }
type ErrorItem = { id: string; source: string; provider?: string; category?: string; severity: string; message: string; status: string; recovery_status?: string; occurrence_count: number; first_seen_at: string; last_seen_at: string; acknowledged_at?: string | null; resolved_at?: string | null; cleaned_at?: string | null }
type HealthSnapshot = { connections: Connector[]; current_errors: ErrorItem[]; history_errors: ErrorItem[] }
type Operation = { id: string; tool_name?: string; status?: string; error?: string | null; updated_at?: string; started_at?: string }

const time = (value?: string | null) => value ? new Date(value).toLocaleString() : '暂无'
const healthLabel = (value?: string) => ({ healthy: '正常', degraded: '降级', reauthorize_required: '需要重新授权', disconnected: '未连接' } as Record<string, string>)[value || ''] || '未知'
const errorLabel = (item: ErrorItem) => item.recovery_status === 'recovered' ? '已恢复' : item.status === 'cleaned' ? '已清理' : item.status === 'resolved' ? '已处理' : item.acknowledged_at ? '已确认' : '待处理'

export default function ConnectorHealthView() {
  const qc = useQueryClient()
  const { pushToast } = useToast()
  const [errorTab, setErrorTab] = useState<'current' | 'history'>('current')
  const health = useQuery({ queryKey: ['connector-health'], queryFn: () => rpc.call<HealthSnapshot>('connector.health', {}), refetchInterval: 30000 })
  const operations = useQuery({ queryKey: ['external-operations'], queryFn: () => rpc.call<{ operations: Operation[] }>('external.operation.list', { limit: 50 }).then(value => value.operations), refetchInterval: 15000 })
  const refresh = () => { void health.refetch(); void operations.refetch() }
  const invalidate = () => { void qc.invalidateQueries({ queryKey: ['connector-health'] }); void qc.invalidateQueries({ queryKey: ['connector-inbox'] }); void qc.invalidateQueries({ queryKey: ['external-operations'] }) }
  const sync = useMutation({ mutationFn: (provider: string) => rpc.call('connector.sync', { provider }), onSuccess: () => { pushToast('同步完成，健康状态已更新', 'success'); invalidate() } })
  const reauthorize = useMutation({ mutationFn: () => rpc.call<any>('connector.oauth.begin', { provider: 'feishu' }), onSuccess: result => { if (result.authorization_url) { if (window.koreNext?.openExternal) void window.koreNext.openExternal(result.authorization_url); else window.open(result.authorization_url, '_blank', 'noopener,noreferrer') } } })
  const acknowledge = useMutation({ mutationFn: (id: string) => rpc.call('error.acknowledge', { error_id: id }), onSuccess: invalidate })
  const resolve = useMutation({ mutationFn: (id: string) => rpc.call('error.resolve', { error_id: id }), onSuccess: invalidate })
  const cleanup = useMutation({ mutationFn: (id: string) => rpc.call('error.cleanup', { error_id: id }), onSuccess: invalidate })
  const cleanupAll = useMutation({ mutationFn: () => rpc.call('error.cleanup_all', {}), onSuccess: result => { pushToast(`已清理 ${Number(result.cleaned || 0)} 条历史错误`, 'success'); invalidate() } })
  const loading = health.isPending || operations.isPending
  const error = health.error || operations.error
  const snapshot = health.data
  const errors = errorTab === 'current' ? (snapshot?.current_errors || []) : (snapshot?.history_errors || [])
  const resolvedHistory = (snapshot?.history_errors || []).filter(item => item.status === 'resolved')

  return <Page title="连接器健康" subtitle="统一查看邮箱和飞书的授权、同步、错误与恢复状态。" actions={<button onClick={refresh} disabled={loading}><RefreshCw size={14} />刷新</button>}>
    {loading && <LoadingState label="正在读取连接器状态…" />}
    {error && <ErrorState error={error} retry={refresh} />}
    {!loading && !error && <div className="stack">
      <section className="card-grid">{(snapshot?.connections || []).map(item => {
        const state = item.health || {}
        const isFeishu = item.provider !== 'email'
        const needsReauthorization = isFeishu && Boolean(state.reauthorize_required || state.token_expired || state.token_expiring_soon)
        return <article className="card" key={item.provider}>
          <div className="section-heading"><div><h2>{item.label || item.provider}</h2><small>{item.provider}</small></div><StatusBadge value={state.status || 'disconnected'} label={healthLabel(state.status)} /></div>
          <dl><dt>当前连接</dt><dd>{state.connected ? '已连接' : '未连接'}</dd><dt>最后成功同步</dt><dd>{time(state.last_successful_sync)}</dd><dt>最近错误</dt><dd>{state.last_error || '无'}</dd><dt>重试状态</dt><dd>{state.retry_count ? `${state.retry_count} 次` : '暂无重试'}{state.next_retry_at ? ` · 下次 ${time(state.next_retry_at)}` : ''}</dd><dt>授权状态</dt><dd>{state.token_expired ? 'Token 已过期' : state.token_expiring_soon ? 'Token 即将过期' : state.reauthorize_required ? '需要重新授权' : state.connected ? '正常' : '未配置'}</dd></dl>
          <small>{item.provider === 'email' ? (item.imap_idle_enabled ? `IMAP IDLE 已启用 · ${time(item.imap_idle_last_at)}` : 'IMAP IDLE 未启用，使用轮询') : (item.webhook_enabled ? `Webhook 已启用 · ${time(item.last_webhook_at)}` : 'Webhook 未启用，使用轮询')}</small>
          <div className="row-actions"><button onClick={() => sync.mutate(item.provider)} disabled={sync.isPending || !state.connected}><RotateCw size={14} />{sync.isPending ? '同步中…' : '立即同步'}</button>{isFeishu && <button onClick={() => reauthorize.mutate()} disabled={reauthorize.isPending}><KeyRound size={14} />{needsReauthorization ? '重新授权' : '授权飞书'}</button>}</div>
        </article>
      })}</section>

      <section className="card"><div className="section-heading"><div><h2><ShieldAlert size={17} />统一错误中心</h2><small>当前错误需要处理，历史错误用于追溯；成功恢复后会自动进入历史。</small></div>{resolvedHistory.length > 0 && <button onClick={() => cleanupAll.mutate()} disabled={cleanupAll.isPending}><Trash2 size={14} />清理已处理记录</button>}</div>
        <div className="memory-tabs" role="tablist" aria-label="错误状态"><button role="tab" aria-selected={errorTab === 'current'} className={errorTab === 'current' ? 'active' : ''} onClick={() => setErrorTab('current')}>当前错误 {snapshot?.current_errors?.length || 0}</button><button role="tab" aria-selected={errorTab === 'history'} className={errorTab === 'history' ? 'active' : ''} onClick={() => setErrorTab('history')}><History size={14} />历史错误 {snapshot?.history_errors?.length || 0}</button></div>
        {!errors.length ? <EmptyState icon={CheckCircle2} title={errorTab === 'current' ? '当前没有未解决错误' : '暂无历史错误'} hint={errorTab === 'current' ? '连接器恢复正常后，错误会自动从当前列表移入历史。' : '处理过的连接器错误会保留在这里，便于追踪。'} /> : <div className="stack">{errors.map(item => <div className="timeline-row" key={item.id}><StatusBadge value={item.status === 'open' ? (item.acknowledged_at ? 'pending' : 'failed') : item.status} label={errorLabel(item)} /><span><strong>{item.provider || item.source}</strong> · {item.message} · {item.occurrence_count} 次<br /><small>{item.category || '系统错误'} · 首次 {time(item.first_seen_at)} · 最近 {time(item.last_seen_at)}</small></span><div className="row-actions">{item.status === 'open' && !item.acknowledged_at && <button onClick={() => acknowledge.mutate(item.id)} disabled={acknowledge.isPending}>确认已知晓</button>}{item.status === 'open' && <button onClick={() => resolve.mutate(item.id)} disabled={resolve.isPending}><Wrench size={14} />标记已恢复</button>}{item.status === 'resolved' && <button onClick={() => cleanup.mutate(item.id)} disabled={cleanup.isPending}><Trash2 size={14} />清理记录</button>}</div></div>)}</div>}
      </section>

      <section className="card"><div className="section-heading"><div><h2>外部动作时间线</h2><small>邮件、日历、任务和文档写入的持久化结果。</small></div></div>{!(operations.data || []).length ? <EmptyState title="暂无外部动作" hint="Agent 执行外部写入后，结果会显示在这里。" /> : <div className="stack">{(operations.data || []).map(item => <div className="timeline-row" key={item.id}><StatusBadge value={item.status || 'pending'} /><span>{item.tool_name || '外部动作'}{item.error ? ` · ${item.error}` : ''}</span><time>{time(item.updated_at || item.started_at)}</time></div>)}</div>}</section>
    </div>}
  </Page>
}
