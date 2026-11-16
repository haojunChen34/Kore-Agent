import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Clock3, Eye, FileText, Mail, RefreshCw, X } from 'lucide-react'
import { rpc } from './rpc'
import type { ExternalInformation } from './types'
import { EmptyState, ErrorState, LoadingState, StatusBadge } from './ui'

const labels: Record<string, string> = { email: '新邮件', calendar: '新日历事件', todo: '新飞书任务', notes: '新文档' }
const kindLabel = (item: ExternalInformation) => labels[item.provider] || labels[item.kind] || '新信息'
const suggestionLabels: Record<string, string> = { create_action: '创建行动', external_operation: '外部操作', link_goal: '关联目标', reply: '外部操作', review: '需要核实' }
const targetLabels: Record<string, string> = { local_action: '本地行动', calendar_event: '创建日历', feishu_task: '创建飞书任务', feishu_document: '写入文档', email_reply: '回复邮件', goal: '指定目标', user_confirmation: '用户确认' }
const suggestionLabel = (action: any) => suggestionLabels[String(action?.action_type || 'create_action')] || '处理建议'
const suggestionTarget = (action: any) => targetLabels[String(action?.action_target || (action?.action_type === 'reply' ? 'email_reply' : ''))] || ''

export default function ExternalInformationView() {
  const qc = useQueryClient()
  const [provider, setProvider] = useState('email')
  const [selected, setSelected] = useState<ExternalInformation | null>(null)
  const [message, setMessage] = useState('')
  const [chatError, setChatError] = useState('')
  const [checkedActions, setCheckedActions] = useState<Record<number, boolean>>({})
  const [pendingExternalActions, setPendingExternalActions] = useState<any[]>([])
  const query = useQuery({ queryKey: ['external-information', provider], queryFn: () => rpc.call<{ items: ExternalInformation[]; counts: Record<string, number> }>('external.information.list', { provider, status: 'pending', limit: 100 }) })
  const refresh = () => void query.refetch()
  const invalidate = () => { void qc.invalidateQueries({ queryKey: ['external-information'] }); void qc.invalidateQueries({ queryKey: ['personal-context'] }) }
  const accept = useMutation({
    mutationFn: (payload: { item: ExternalInformation; actions: any[]; keepOpen?: boolean }) => rpc.call<{ approvals?: any[]; item?: ExternalInformation }>('external.information.accept', { information_id: payload.item.id, actions: payload.actions }),
    onSuccess: (result, payload) => { if (result.item) setSelected(result.item); if (result.approvals?.length) setPendingExternalActions(result.approvals.map((approval: any) => ({ ...approval.action, approval_id: approval.id, approval_status: approval.status }))); if (!payload.keepOpen) { setSelected(null); setPendingExternalActions([]) }; invalidate() },
  })
  const ignore = useMutation({ mutationFn: (item: ExternalInformation) => rpc.call('external.information.ignore', { information_id: item.id }), onSuccess: () => { setSelected(null); invalidate() } })
  const snooze = useMutation({ mutationFn: (item: ExternalInformation) => rpc.call('external.information.snooze', { information_id: item.id }), onSuccess: () => { setSelected(null); invalidate() } })
  const analyze = useMutation({ mutationFn: (item: ExternalInformation) => rpc.call<{ item: ExternalInformation }>('external.information.reanalyze', { information_id: item.id }), onSuccess: result => { setSelected(result.item); setCheckedActions(Object.fromEntries((result.item.analysis?.suggested_actions || []).map((_item: any, index: number) => [index, true]))); setPendingExternalActions([]) } })
  const chat = useMutation({
    mutationFn: (payload: { informationId: string; message: string }) => rpc.call<{ analysis: Record<string, any>; item?: ExternalInformation }>('external.information.chat', { information_id: payload.informationId, message: payload.message }),
    onMutate: () => setChatError(''),
    onSuccess: (result, payload) => {
      const next = result.item || (selected && selected.id === payload.informationId ? { ...selected, analysis: result.analysis } : null)
      if (next) {
        setSelected(next)
        setCheckedActions(Object.fromEntries((next.analysis?.suggested_actions || []).map((_item: any, index: number) => [index, true])))
        qc.setQueryData<{ items: ExternalInformation[]; counts: Record<string, number> }>(['external-information', provider], current => current ? { ...current, items: current.items.map(item => item.id === next.id ? next : item) } : current)
      }
      setMessage('')
      void qc.invalidateQueries({ queryKey: ['external-information', provider] })
    },
    onError: error => setChatError(error instanceof Error ? error.message : String(error)),
  })
  const openDetail = (item: ExternalInformation) => { setSelected(item); setMessage(''); setChatError(''); setPendingExternalActions([]); setCheckedActions(Object.fromEntries((item.analysis?.suggested_actions || []).map((_item: any, index: number) => [index, true]))) }
  const closeDetail = () => { setSelected(null); setMessage(''); setChatError(''); setPendingExternalActions([]) }
  const selectedSuggestions = selected ? [...(selected.analysis?.suggested_actions || []), ...(selected.analysis?.suggested_goal && !(selected.analysis?.suggested_actions || []).some((action: any) => action.action_type === 'link_goal') ? [{ action_type: 'link_goal', action_target: 'goal', title: selected.analysis.suggested_goal, description: '可将这条信息关联到该目标，便于后续集中查看。', priority: 'normal' }] : [])] : []
  const acceptSelected = () => {
    if (!selected) return
    const selectedActions = selectedSuggestions.filter((_item: any, index: number) => checkedActions[index] !== false)
    const externalActions = selectedActions.filter((item: any) => item.action_type === 'external_operation')
    if (externalActions.length) setPendingExternalActions(externalActions)
    accept.mutate({ item: selected, actions: selectedActions, keepOpen: externalActions.length > 0 })
  }
  const counts = query.data?.counts || {}
  const items = useMemo(() => query.data?.items || [], [query.data])
  return <section className="external-information card">
    <div className="section-heading"><div><span className="eyebrow">外部信息</span><h2>需要你关注的新内容</h2></div><button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14} />刷新</button></div>
    <p className="field-note">这里是工作台的分析视图。收件箱中的原始内容不会被删除，也不会因为查看分析而自动标记已读。</p>
    <div className="external-information-tabs">{(['email', 'calendar', 'todo', 'notes'] as const).map(key => <button key={key} className={provider === key ? 'active' : ''} onClick={() => setProvider(key)}><span>{key === 'email' ? <Mail size={15} /> : key === 'calendar' ? <Clock3 size={15} /> : key === 'notes' ? <FileText size={15} /> : <Check size={15} />}</span>{labels[key]} <b>{counts[key] || 0}</b></button>)}</div>
    {query.isPending && <LoadingState label="正在分析外部信息…" />}
    {query.error && <ErrorState error={query.error} retry={refresh} title="外部信息加载失败" />}
    {!query.isPending && !query.error && !items.length && <EmptyState icon={Eye} title={`暂无${labels[provider]}`} hint="新同步内容会先由 Agent 分析，值得关注的内容才会出现在这里。" />}
    <div className="external-information-list">{items.map(item => { const analysis = item.analysis || {}; return <article className="external-information-card" key={item.id}>
      <div className="row-actions"><StatusBadge value={String(analysis.importance || 'normal')} label={String(analysis.category || '待分析')} /><strong>{item.original?.title || analysis.summary || '未命名信息'}</strong><small>{item.original?.sender || item.original?.starts_at || item.updated_at || ''}</small></div>
      <p>{analysis.summary || '正在生成摘要…'}</p><div className="row-actions"><button onClick={() => openDetail(item)}><Eye size={14} />详情</button><button onClick={() => openDetail(item)} disabled={accept.isPending}>选择建议</button><button onClick={() => ignore.mutate(item)} disabled={ignore.isPending}>忽略</button></div>
    </article> })}</div>
    {selected && <div className="inbox-drawer-back" onClick={event => { if (event.target === event.currentTarget) closeDetail() }}><aside className="inbox-detail-drawer external-information-detail">
      <header><div><span className="eyebrow">{kindLabel(selected)}</span><h2>{selected.original?.title || selected.analysis?.summary || '外部信息详情'}</h2><small>{selected.original?.sender || selected.original?.starts_at || selected.updated_at || ''}</small></div><button onClick={closeDetail} aria-label="关闭"><X size={18} /></button></header>
      <div className="inbox-detail-content"><section><h3>Agent 分析</h3><p>{selected.analysis?.summary || '暂无分析'}</p><div className="external-analysis-facts"><span>分类：{selected.analysis?.category || '未知'}</span><span>重要性：{selected.analysis?.importance || '普通'}</span><span>置信度：{Math.round(Number(selected.analysis?.confidence || 0) * 100)}%</span></div>{Array.isArray(selected.analysis?.evidence) && selected.analysis.evidence.length > 0 && <p className="field-note">依据：{selected.analysis.evidence.join('；')}</p>}</section>
        <section className="memory-detail-section"><h3>原始信息</h3><div className="inbox-body">{selected.original?.body || selected.original?.summary || selected.original?.description || '暂无正文'}</div></section>
        {selectedSuggestions.length > 0 && <section className="memory-detail-section"><h3>建议</h3><p className="field-note">默认勾选全部建议。取消勾选后，该建议不会创建行动，也不会发邮件或修改飞书。</p><ol className="external-suggestions">{selectedSuggestions.map((action: any, index: number) => <li className="external-suggestion" key={index}><label className="external-suggestion-check"><input type="checkbox" checked={checkedActions[index] !== false} onChange={event => setCheckedActions(current => ({ ...current, [index]: event.target.checked }))} /><span><div><strong>{suggestionLabel(action)}{suggestionTarget(action) ? ` → ${suggestionTarget(action)}` : ''}：</strong><span>{action.title}</span></div>{action.description && <small>{action.description}</small>}{(action.due_at || action.priority) && <small className="external-suggestion-meta">{action.due_at ? `截止：${action.due_at}` : ''}{action.due_at && action.priority ? ' · ' : ''}{action.priority ? `优先级：${action.priority}` : ''}</small>}</span></label></li>)}</ol></section>}
        {pendingExternalActions.length > 0 && <section className="memory-detail-section external-action-preview"><h3>外部操作预览</h3><p className="field-note">以下操作尚未执行。请到“执行中心 → 审批”中确认，允许后才会发送邮件或修改飞书。</p>{pendingExternalActions.map((action: any, index: number) => <div className="external-suggestion" key={index}><strong>外部操作 → {suggestionTarget(action)}</strong><span>{action.title}</span>{action.description && <small>{action.description}</small>}<small>状态：等待审批</small>{action.preview && <details className="action-preview"><summary>查看预览</summary><div className="preview-box"><pre>{JSON.stringify(action.preview, null, 2)}</pre></div></details>}</div>)}</section>}
        <section className="memory-detail-section"><h3>修改建议</h3><div className="row-actions external-edit-suggestion"><textarea rows={5} value={message} onChange={event => setMessage(event.target.value)} placeholder="告诉 Agent 你希望如何调整建议，例如：只保留会议相关行动，回复邮件但不要创建文档" /><button onClick={() => { if (selected && message.trim()) chat.mutate({ informationId: selected.id, message: message.trim() }) }} disabled={!message.trim() || chat.isPending}>{chat.isPending ? '生成中…' : '发送'}</button></div>{chat.isPending && <p className="field-note">Agent 正在根据当前原始信息重新整理建议，请稍候…</p>}{chatError && <p className="form-error" role="alert">{chatError}</p>}</section>
        <div className="modal-actions row-actions"><button className="primary" onClick={acceptSelected} disabled={accept.isPending || !selectedSuggestions.some((_item: any, index: number) => checkedActions[index] !== false)}>{accept.isPending ? '处理中…' : '接受选中建议'}</button><button onClick={() => snooze.mutate(selected)}>稍后处理</button><button onClick={() => ignore.mutate(selected)}>忽略</button><button onClick={() => analyze.mutate(selected)} disabled={analyze.isPending}>重新分析</button></div>
      </div>
    </aside></div>}
  </section>
}
