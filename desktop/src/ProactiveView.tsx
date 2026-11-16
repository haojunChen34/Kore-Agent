import { useMemo, useState } from 'react'
import { AlertTriangle, Ban, Check, History, Play, RefreshCw, RotateCcw, ShieldAlert, Sparkles } from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { rpc } from './rpc'
import { EmptyState, ErrorState, LoadingState, PageHeader, StatusBadge } from './ui'

type Suggestion = { id: string; title: string; reason: string; risk: string; status: string; action?: { type?: string; title?: string; body?: string }; source?: string; event_type?: string; failure?: string | null; created_at: string }
type Job = { id: string; suggestion_id: string; risk: string; status: string; attempts: number; max_attempts: number; action?: { title?: string; type?: string }; error?: string | null; updated_at: string }
type Audit = { id: string; action: string; entity_type: string; entity_id?: string; actor: string; created_at: string }
type Trigger = { id: string; name: string; description: string; source: string; event_type: string; risk: string; status: string; auto_execute: boolean }

const riskLabel: Record<string, string> = { low: '低风险', medium: '中风险', high: '高风险' }
const statusLabel: Record<string, string> = { pending: '待处理', accepted: '已接受', awaiting_approval: '等待审批', running: '执行中', recovering: '恢复中', completed: '已完成', failed: '失败', partial_success: '部分成功', uncertain: '结果未知', blocked: '已阻塞', dismissed: '已忽略', cancelled: '已取消' }
const formatDate = (value?: string) => value ? new Date(value).toLocaleString() : '—'

export default function ProactiveView() {
  const qc = useQueryClient(); const [tab, setTab] = useState<'suggestions'|'jobs'|'triggers'|'audit'>('suggestions')
  const suggestions = useQuery({ queryKey: ['proactive-suggestions'], queryFn: () => rpc.call<{ suggestions: Suggestion[] }>('suggestion.list', { limit: 200 }).then(x => x.suggestions), refetchInterval: 15000 })
  const jobs = useQuery({ queryKey: ['execution-jobs'], queryFn: () => rpc.call<{ jobs: Job[] }>('execution.list', { limit: 200 }).then(x => x.jobs), refetchInterval: 10000 })
  const triggers = useQuery({ queryKey: ['proactive-triggers'], queryFn: () => rpc.call<{ triggers: Trigger[] }>('proactive.trigger.list', { limit: 100 }).then(x => x.triggers) })
  const audit = useQuery({ queryKey: ['audit-log'], queryFn: () => rpc.call<{ entries: Audit[] }>('audit.list', { limit: 100 }).then(x => x.entries), enabled: tab === 'audit', refetchInterval: 15000 })
  const refresh = () => { void qc.invalidateQueries({ queryKey: ['proactive-suggestions'] }); void qc.invalidateQueries({ queryKey: ['execution-jobs'] }); void qc.invalidateQueries({ queryKey: ['proactive-triggers'] }); void qc.invalidateQueries({ queryKey: ['audit-log'] }) }
  const accept = useMutation({ mutationFn: (id: string) => rpc.call('suggestion.accept', { suggestion_id: id }), onSuccess: refresh })
  const dismiss = useMutation({ mutationFn: (id: string) => rpc.call('suggestion.dismiss', { suggestion_id: id }), onSuccess: refresh })
  const retry = useMutation({ mutationFn: (id: string) => rpc.call('execution.retry', { job_id: id }), onSuccess: refresh })
  const cancel = useMutation({ mutationFn: (id: string) => rpc.call('execution.cancel', { job_id: id }), onSuccess: refresh })
  const toggleTrigger = useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => rpc.call('proactive.trigger.update', { trigger_id: id, status: enabled ? 'active' : 'paused' }),
    onSuccess: refresh,
  })
  const busy = accept.isPending || dismiss.isPending || retry.isPending || cancel.isPending || toggleTrigger.isPending
  const pendingCount = useMemo(() => (suggestions.data || []).filter(item => ['pending', 'awaiting_approval', 'failed'].includes(item.status)).length, [suggestions.data])
  const initialLoading = suggestions.isPending || jobs.isPending || triggers.isPending
  const initialError = suggestions.error || jobs.error || triggers.error
  if (initialError && !suggestions.data && !jobs.data && !triggers.data) return <section className="page"><PageHeader title="主动执行" subtitle="事件触发建议、风险控制、恢复和审计。" actions={<button onClick={refresh}><RefreshCw size={14}/>重试</button>} /><div className="page-body"><ErrorState error={initialError} retry={refresh} /></div></section>
  if (initialLoading && !suggestions.data && !jobs.data && !triggers.data) return <section className="page"><PageHeader title="主动执行" subtitle="事件触发建议、风险控制、恢复和审计。" /><div className="page-body"><LoadingState label="正在读取主动执行状态…" /></div></section>
  return <section className="page"><PageHeader title="主动执行" subtitle="事件触发建议、风险控制、恢复和审计。" actions={<button onClick={refresh} disabled={busy}><RefreshCw size={14}/>刷新</button>} /><div className="page-body">
    <div className="proactive-metrics"><div><strong>{pendingCount}</strong><span>待处理建议</span></div><div><strong>{(jobs.data || []).filter(x => x.status === 'running').length}</strong><span>执行中</span></div><div><strong>{(jobs.data || []).filter(x => x.status === 'awaiting_approval').length}</strong><span>等待审批</span></div><div><strong>{(jobs.data || []).filter(x => ['failed', 'partial_success', 'uncertain', 'blocked'].includes(x.status)).length}</strong><span>需恢复任务</span></div></div>
    <div className="proactive-tabs"><button className={tab === 'suggestions' ? 'active' : ''} onClick={() => setTab('suggestions')}><Sparkles size={14}/>建议</button><button className={tab === 'jobs' ? 'active' : ''} onClick={() => setTab('jobs')}><Play size={14}/>执行任务</button><button className={tab === 'triggers' ? 'active' : ''} onClick={() => setTab('triggers')}><AlertTriangle size={14}/>触发器</button><button className={tab === 'audit' ? 'active' : ''} onClick={() => setTab('audit')}><History size={14}/>审计</button></div>
    {tab === 'suggestions' && <div className="proactive-list"><QueryState loading={suggestions.isPending} error={suggestions.error} retry={() => void suggestions.refetch()} empty={!suggestions.data?.length} icon={Sparkles} title="暂无主动建议" hint="连接器同步、运行失败或定时扫描后，建议会出现在这里。">{suggestions.data?.map(item => <article className="proactive-card" key={item.id}><div className="proactive-card-main"><div className={`risk-icon risk-${item.risk}`}>{item.risk === 'high' ? <ShieldAlert size={18}/> : <Sparkles size={18}/>}</div><div><div className="proactive-title"><h3>{item.title}</h3><span className={`risk-badge risk-${item.risk}`}>{riskLabel[item.risk] || item.risk}</span><StatusBadge value={item.status} label={statusLabel[item.status] || item.status}/></div><p>{item.reason}</p><small>{item.event_type || item.source || '事件'} · {formatDate(item.created_at)}</small>{item.failure && <div className="proactive-failure">失败：{item.failure}</div>}</div></div><div className="row-actions">{['pending', 'failed'].includes(item.status) && <button className="primary" onClick={() => accept.mutate(item.id)} disabled={busy}><Check size={14}/>接受并执行</button>}{['pending', 'failed'].includes(item.status) && <button onClick={() => dismiss.mutate(item.id)} disabled={busy}><Ban size={14}/>忽略</button>}{item.status === 'awaiting_approval' && <span className="approval-hint">请在审批页面确认高风险动作</span>}</div></article>)}</QueryState></div>}
    {tab === 'jobs' && <div className="proactive-list"><QueryState loading={jobs.isPending} error={jobs.error} retry={() => void jobs.refetch()} empty={!jobs.data?.length} icon={Play} title="暂无执行任务" hint="接受建议后，后台执行任务会显示在这里。">{jobs.data?.map(job => <article className="proactive-card" key={job.id}><div className="proactive-card-main"><div className="risk-icon"><Play size={17}/></div><div><div className="proactive-title"><h3>{job.action?.title || `执行任务 ${job.id.slice(0, 8)}`}</h3><StatusBadge value={job.status} label={statusLabel[job.status] || job.status}/></div><p>风险：{riskLabel[job.risk] || job.risk} · 尝试 {job.attempts}/{job.max_attempts}</p><small>更新于 {formatDate(job.updated_at)}</small>{job.error && <div className="proactive-failure">失败：{job.error}</div>}{['partial_success', 'uncertain', 'blocked'].includes(job.status) && <div className="form-warning">请查看外部操作时间线，并按提示恢复。</div>}</div></div><div className="row-actions">{['failed', 'pending'].includes(job.status) && <button onClick={() => retry.mutate(job.id)} disabled={busy}><RotateCcw size={14}/>重试</button>}{['pending', 'running', 'awaiting_approval'].includes(job.status) && <button className="danger" onClick={() => cancel.mutate(job.id)} disabled={busy}><Ban size={14}/>取消</button>}</div></article>)}</QueryState></div>}
    {tab === 'triggers' && <div className="trigger-list">{(triggers.data || []).map(item => { const enabled = item.status === 'active'; return <article className="trigger-card" key={item.id}><div><h3>{item.name}</h3><p>{item.description}</p></div><div><span className={`risk-badge risk-${item.risk}`}>{riskLabel[item.risk] || item.risk}</span><span className="status">{enabled ? '已启用' : '已停用'}</span><small>{item.source} / {item.event_type}</small><small>{item.auto_execute ? '低风险自动执行' : '需要用户确认'}</small><button onClick={() => toggleTrigger.mutate({ id: item.id, enabled: !enabled })} disabled={busy}>{enabled ? '停用触发器' : '启用触发器'}</button></div></article> })}</div>}
    {tab === 'audit' && <div className="audit-list"><QueryState loading={audit.isPending} error={audit.error} retry={() => void audit.refetch()} empty={!audit.data?.length} icon={History} title="暂无审计记录" hint="主动执行的创建、审批、执行、失败和恢复都会记录在这里。">{audit.data?.map(item => <article className="audit-row" key={item.id}><span className="audit-dot"/><div><strong>{item.action}</strong><small>{item.entity_type}{item.entity_id ? ` · ${item.entity_id.slice(0, 12)}` : ''} · {item.actor}</small></div><time>{formatDate(item.created_at)}</time></article>)}</QueryState></div>}
  </div></section>
}

function QueryState({ loading, error, retry, empty, icon, title, hint, children }: { loading: boolean; error: unknown; retry: () => void; empty: boolean; icon: typeof Sparkles; title: string; hint: string; children: React.ReactNode }) {
  if (loading) return <LoadingState label="正在加载…" />
  if (error) return <ErrorState error={error} retry={retry} />
  if (empty) return <EmptyState icon={icon} title={title} hint={hint} />
  return <>{children}</>
}
