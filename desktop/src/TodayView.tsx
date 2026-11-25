import ActionDetail from './ActionDetail'
﻿import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, CheckCircle2, Clock3, Target, Bell, Plus, RefreshCw, X } from 'lucide-react'
import { rpc } from './rpc'
import type { DailyBriefContext, PersonalAction, PersonalContext } from './types'
import { EmptyState, ErrorState, LoadingState, PageHeader, StatusBadge } from './ui'
import ExternalInformationView from './ExternalInformationView'
import WebMonitorUpdatesView from './WebMonitorUpdatesView'

function statusLabel(value: string) {
  return ({ next: '下一步', doing: '进行中', waiting: '等待中', done: '已完成', cancelled: '已取消' } as Record<string, string>)[value] || value
}
function Empty({ text }: { text: string }) { return <EmptyState title={text} /> }

function actionDueLabel(dueAt: string | null, timezone: string) {
  if (!dueAt) return '未定时间'
  const date = new Date(dueAt)
  if (Number.isNaN(date.getTime())) return '时间无效'
  try {
    return date.toLocaleString('zh-CN', { timeZone: timezone, dateStyle: 'medium', timeStyle: 'short' })
  } catch (error) {
    if (!(error instanceof RangeError)) throw error
    return date.toLocaleString('zh-CN', { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' })
  }
}

function BriefDetailSection({ title, items, timezone, empty = '暂无内容。' }: { title: string; items: Array<Record<string, any>> | undefined; timezone:string; empty?: string }) {
  return <section className="daily-brief-detail-section"><h3>{title}</h3>{items?.length ? <ul>{items.map((item, index) => {
    const suggested = item.suggested_actions as Array<Record<string, any>> | undefined
    return <li key={String(item.id || index)}><strong>{String(item.title || item.label || item.provider || '未命名')}</strong>{(item.summary || item.description || item.body || item.last_error || item.status) && <small>{String(item.summary || item.description || item.body || item.last_error || item.status)}</small>}{suggested?.length && <small>建议：{suggested.map(action => String(action.title || '')).filter(Boolean).join('、')}</small>}{item.starts_at && <small>{actionDueLabel(String(item.starts_at),timezone)}{item.ends_at ? ` · ${actionDueLabel(String(item.ends_at),timezone)}` : ''}</small>}</li>
  })}</ul> : <p className="muted">{empty}</p>}</section>
}

function ActionRows({ items, timezone, complete, openDetail }: { items: PersonalAction[]; timezone: string; complete: (id: string) => void; openDetail(id:string):void }) {
  return <div className="today-actions">{items.map(item => {
    const overdue = item.status !== 'done' && item.status !== 'cancelled' && !!item.due_at && new Date(item.due_at).getTime() < Date.now()
    const due = actionDueLabel(item.due_at, timezone)
    return <article className="today-action" key={item.id}>
      <button className={`check-button${item.status === 'done' ? ' completed' : ''}`} aria-label={`${item.status === 'done' ? '已完成' : '完成'} ${item.title}`} title={item.status === 'done' ? '已完成' : '标记为已完成'} onClick={() => complete(item.id)} disabled={item.status === 'done'}><CheckCircle2 size={18} /><span>{item.status === 'done' ? '已完成' : '完成'}</span></button>
      <div><strong>{item.title}</strong><small>{item.description || statusLabel(item.status)} · {due}</small></div>
      <button onClick={()=>openDetail(item.id)} aria-label={`查看行动 ${item.title}`}>详情与来源</button>
      {overdue ? <StatusBadge value="failed" label="已逾期" /> : <StatusBadge value={item.status} label={statusLabel(item.status)} />}
    </article>
  })}</div>
}

function ActionForm({ timezone, todayDate, onCancel, onSaved }: { timezone: string; todayDate: string; onCancel(): void; onSaved(showLater: boolean): void }) {
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [dueLocal, setDueLocal] = useState('')
  const create = useMutation({
    mutationFn: (values: { title: string; description: string; dueLocal: string }) => rpc.call('action.create', {
      title: values.title.trim(), description: values.description.trim(), due_local: values.dueLocal || null,
    }),
    onSuccess: (_result, values) => onSaved(!values.dueLocal || values.dueLocal.slice(0, 10) > todayDate),
  })
  return <section className="page today-page">
    <PageHeader title="新建行动" actions={<button onClick={onCancel} disabled={create.isPending}><ArrowLeft size={15} />返回工作台</button>} />
    <div className="page-body"><form className="action-editor" onSubmit={event => { event.preventDefault(); if (title.trim()) create.mutate({ title, description, dueLocal }) }}>
      <label>主题<input value={title} onChange={event => setTitle(event.target.value)} maxLength={240} required autoFocus disabled={create.isPending} /></label>
      <label>内容<textarea value={description} onChange={event => setDescription(event.target.value)} rows={5} disabled={create.isPending} /></label>
      <label>截止时间<input type="datetime-local" value={dueLocal} onChange={event => setDueLocal(event.target.value)} disabled={create.isPending} /><small>个人时区：{timezone} · 不填写则为未定时间</small></label>
      <div className="action-editor-preview"><strong>保存预览</strong><span>{title.trim() || '未填写主题'}</span><small>{dueLocal ? `${dueLocal.replace('T', ' ')} · ${timezone}` : '未定时间'}</small></div>
      {create.error && <ErrorState error={create.error} />}
      <div className="action-editor-actions"><button type="button" onClick={onCancel} disabled={create.isPending}>取消</button><button className="primary" type="submit" disabled={!title.trim() || create.isPending}>{create.isPending ? '保存中…' : '保存行动'}</button></div>
    </form></div>
  </section>
}

function GoalForm({ onCancel, onSaved, existingGoalId, initialRequest, initialPlan }: { onCancel(): void; onSaved(): void; existingGoalId?: string; initialRequest?: string; initialPlan?: any }) {
  type GoalPlan = {
    plan_id?: string
    version?: number
    title: string
    description: string
    priority: string
    actions: Array<{ title: string; description?: string; due_local?: string; stage_id?: string }>
    task: { title: string; goal: string; description?: string; steps?: Array<{ title: string; description?: string }> } | null
    automation: { title: string; prompt: string; kind: string; run_at?: string | null; interval_seconds?: number | null; cron_expr?: string | null; timezone?: string } | null
  }
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [priority, setPriority] = useState('normal')
  const [mode, setMode] = useState<'quick' | 'agent'>(initialRequest || initialPlan || existingGoalId ? 'agent' : 'quick')
  const [request, setRequest] = useState(initialRequest || '')
  const [plan, setPlan] = useState<GoalPlan | null>(initialPlan || null)
  const [includeActions, setIncludeActions] = useState(true)
  const [includeTask, setIncludeTask] = useState(true)
  const [includeAutomation, setIncludeAutomation] = useState(true)
  const [discussion, setDiscussion] = useState('')
  const create = useMutation({
    mutationFn: () => rpc.call('goal.create', {
      title: title.trim(),
      description: description.trim(),
      priority,
    }),
    onSuccess: onSaved,
  })
  const preview = useMutation({
    mutationFn: () => rpc.call<{ plan: GoalPlan }>('goal.plan.preview', { request: request.trim() }),
    onSuccess: result => {
      setPlan(result.plan)
      setIncludeActions(Boolean(result.plan.actions?.length))
      setIncludeTask(Boolean(result.plan.task))
      setIncludeAutomation(Boolean(result.plan.automation))
    },
  })
  const apply = useMutation({
    mutationFn: () => rpc.call('goal.plan.apply', { plan, existing_goal_id: existingGoalId, include_actions: includeActions, include_task: includeTask, include_automation: includeAutomation }),
    onSuccess: onSaved,
  })
  const discuss = useMutation({
    mutationFn: () => rpc.call<{ plan: GoalPlan }>('goal.plan.discuss', { plan, request: discussion.trim() }),
    onSuccess: result => { setPlan(result.plan); setDiscussion(''); setIncludeActions(Boolean(result.plan.actions?.length)); setIncludeTask(Boolean(result.plan.task)); setIncludeAutomation(Boolean(result.plan.automation)) },
  })
  const pending = create.isPending || preview.isPending || apply.isPending || discuss.isPending
  return <div className="modal-back" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget && !pending) onCancel() }}>
    <form className="modal goal-editor" role="dialog" aria-modal="true" aria-labelledby="new-goal-title" onSubmit={event => { event.preventDefault(); if (mode === 'quick' && title.trim()) create.mutate() }}>
      <div className="section-heading"><div><h2 id="new-goal-title">新建目标</h2><small className="muted">可以快速保存，也可以让 Agent 帮你规划关联内容。</small></div><button type="button" aria-label="关闭" onClick={onCancel} disabled={pending}><X size={16} /></button></div>
      <div className="goal-mode-tabs" role="tablist" aria-label="创建方式"><button type="button" role="tab" aria-selected={mode === 'quick'} className={mode === 'quick' ? 'active' : ''} onClick={() => { setMode('quick'); setPlan(null) }} disabled={pending}>快速创建</button><button type="button" role="tab" aria-selected={mode === 'agent'} className={mode === 'agent' ? 'active' : ''} onClick={() => setMode('agent')} disabled={pending}>让 Agent 帮我规划</button></div>
      {mode === 'quick' ? <>
        <label>目标名称<input value={title} onChange={event => setTitle(event.target.value)} maxLength={160} required autoFocus disabled={pending} placeholder="例如：求职准备" /></label>
        <label>目标说明<textarea value={description} onChange={event => setDescription(event.target.value)} rows={4} disabled={pending} placeholder="说明你希望长期推进的方向" /></label>
        <label>优先级<select value={priority} onChange={event => setPriority(event.target.value)} disabled={pending}><option value="low">低</option><option value="normal">普通</option><option value="high">高</option></select></label>
        {create.error && <ErrorState error={create.error} />}
        <div className="modal-actions"><button type="button" onClick={onCancel} disabled={pending}>取消</button><button className="primary" type="submit" disabled={!title.trim() || pending}>{create.isPending ? '保存中…' : '保存并设为活跃目标'}</button></div>
      </> : <>
        {!plan ? <>
          <label>告诉 Agent 你的长期目标<textarea value={request} onChange={event => setRequest(event.target.value)} rows={6} disabled={pending} autoFocus placeholder="例如：接下来准备求职，未来两个月每天学习算法，并在每周日晚上提醒我复盘。" /></label>
          <p className="field-note">Agent 只会先生成目标、行动、任务和提醒的预览；确认后才会保存。</p>
          {preview.error && <ErrorState error={preview.error} />}
          <div className="modal-actions"><button type="button" onClick={onCancel} disabled={pending}>取消</button><button type="button" className="primary" onClick={() => preview.mutate()} disabled={!request.trim() || pending}>{preview.isPending ? '正在规划…' : '生成规划预览'}</button></div>
        </> : <>
          <div className="goal-plan-preview"><div className="goal-plan-heading"><div><span className="eyebrow">规划预览 · 第 {plan.version || 1} 版</span><h3>{plan.title}</h3><p>{plan.description || '暂无说明'}</p></div><button type="button" onClick={() => setPlan(null)} disabled={pending}>重新描述</button></div>
            <label className="goal-plan-option"><input type="checkbox" checked={includeActions} onChange={event => setIncludeActions(event.target.checked)} disabled={!plan.actions.length || pending} /><span><strong>行动 · {plan.actions.length} 项</strong>{plan.actions.length ? plan.actions.map((item, index) => <small key={`${item.stage_id || index}`}>阶段 {index + 1}：{item.title}{item.description ? ` · ${item.description}` : ''}{item.due_local ? ` · ${item.due_local}` : ''}</small>) : <small>没有建议行动</small>}</span></label>
            <label className="goal-plan-option"><input type="checkbox" checked={includeTask} onChange={event => setIncludeTask(event.target.checked)} disabled={!plan.task || pending} /><span><strong>任务计划</strong>{plan.task ? <><small>{plan.task.title} · {plan.task.description || '暂无说明'}</small>{plan.task.steps?.map((step, index) => <small key={index}>步骤 {index + 1}：{step.title}{step.description ? ` · ${step.description}` : ''}</small>)}</> : <small>没有建议任务计划</small>}</span></label>
            <label className="goal-plan-option"><input type="checkbox" checked={includeAutomation} onChange={event => setIncludeAutomation(event.target.checked)} disabled={!plan.automation || pending} /><span><strong>自动化提醒</strong>{plan.automation ? <><small>{plan.automation.title} · {plan.automation.prompt}</small><small>{plan.automation.kind} · {plan.automation.timezone || '默认时区'}{plan.automation.cron_expr ? ` · ${plan.automation.cron_expr}` : ''}</small></> : <small>没有建议提醒</small>}</span></label>
            <div className="goal-plan-discussion"><label>继续和 Agent 讨论<textarea rows={3} value={discussion} onChange={event => setDiscussion(event.target.value)} disabled={pending} placeholder="例如：把第二阶段改成动态规划专项训练，不要创建自动化提醒。" /></label><button type="button" onClick={() => discuss.mutate()} disabled={!discussion.trim() || pending}>{discuss.isPending ? '正在调整…' : '更新规划预览'}</button></div>
            <p className="field-note">外部邮件、日历、飞书任务和文档不会由此规划直接执行。</p>
          </div>
          {apply.error && <ErrorState error={apply.error} />}
          <div className="modal-actions"><button type="button" onClick={onCancel} disabled={pending}>取消</button><button type="button" className="primary" onClick={() => apply.mutate()} disabled={pending}>{apply.isPending ? '保存中…' : '确认并创建选中内容'}</button></div>
        </>}
      </>}
    </form>
  </div>
}

function GoalDetail({ goalId, onClose, onChanged, onDiscuss }: { goalId: string; onClose(): void; onChanged(): void; onDiscuss(plan: any): void }) {
  const detail = useQuery({ queryKey: ['goal-detail', goalId], queryFn: () => rpc.call<any>('goal.detail', { goal_id: goalId }) })
  return <div className="modal-back" role="presentation" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}><section className="modal goal-detail" role="dialog" aria-modal="true"><div className="section-heading"><div><h2>{detail.data?.goal?.title || '目标详情'}</h2><small>{detail.data?.goal?.description || '暂无目标说明'}</small></div><button onClick={onClose}>关闭</button></div>{detail.isPending ? <LoadingState label="正在读取目标详情…" /> : detail.error ? <ErrorState error={detail.error} retry={() => detail.refetch()} /> : <div className="goal-detail-body"><section><h3>完整计划</h3><p className="goal-detail-text">{detail.data?.plan?.description || detail.data?.goal?.description || '暂无完整规划说明'}</p></section><section><h3>关联行动</h3>{detail.data?.actions?.length ? detail.data.actions.map((item: any) => <article className="goal-detail-item" key={item.id}><strong>{item.title}</strong><p>{item.description || '暂无说明'}</p><small>{item.status} · {item.due_at || '未定时间'}</small></article>) : <p className="muted">暂无关联行动</p>}</section><section><h3>任务计划</h3>{detail.data?.tasks?.length ? detail.data.tasks.map((task: any) => <article className="goal-detail-item" key={task.id}><strong>{task.title}</strong><p>{task.description || task.goal}</p><small>{(task.steps || []).filter((step: any) => step.status === 'completed').length}/{(task.steps || []).length} 个步骤已完成</small></article>) : <p className="muted">暂无关联任务</p>}</section><section><h3>自动化提醒</h3>{detail.data?.automations?.length ? detail.data.automations.map((item: any) => <article className="goal-detail-item" key={item.id}><strong>{item.title}</strong><p>{item.prompt}</p><small>{item.status} · {item.schedule?.timezone || '默认时区'}</small></article>) : <p className="muted">暂无关联提醒</p>}</section><div className="modal-actions"><button onClick={onClose}>关闭</button><button className="primary" onClick={() => onDiscuss(detail.data?.plan)}>与 Agent 讨论</button></div></div>}</section></div>
}

export default function TodayView({ onOpenConversation,onOpenSource }: { onOpenConversation?: (conversationId: string) => void; onOpenSource?(type:string,id:string):void }) {
  const qc = useQueryClient()
  const query = useQuery({ queryKey: ['personal-context'], queryFn: () => rpc.call<PersonalContext>('personal.context.get', {}) })
  const brief = useMutation({ mutationFn: () => rpc.call('daily_brief.generate', {}), onSuccess: () => qc.invalidateQueries({ queryKey: ['personal-context'] }) })
  const complete = useMutation({ mutationFn: (id: string) => rpc.call('action.complete', { action_id: id }), onSuccess: () => qc.invalidateQueries({ queryKey: ['personal-context'] }) })
  const dismiss = useMutation({ mutationFn: (id: string) => rpc.call('notification.dismiss', { notification_id: id }), onSuccess: () => qc.invalidateQueries({ queryKey: ['personal-context'] }) })
  const [showAll, setShowAll] = useState(false)
  const [showBriefDetails, setShowBriefDetails] = useState(false)
  const [showActionForm, setShowActionForm] = useState(false)
  const [showGoalForm, setShowGoalForm] = useState(false)
  const [detailActionId,setDetailActionId]=useState<string|null>(null)
  const [detailGoalId, setDetailGoalId] = useState<string | null>(null)
  const [discussionGoalId, setDiscussionGoalId] = useState<string | null>(null)
  const [discussionPlan, setDiscussionPlan] = useState<any>(null)
  const data = query.data
  const timezone = data?.profile.timezone || 'Asia/Shanghai'
  const actions = (showAll ? data?.today.next_actions : data?.today.actions) || []
  const overdueActions = data?.today.overdue_actions || []
  const briefContext: DailyBriefContext | undefined = data?.brief?.context
  const counts = data?.brief?.counts || {}
  if (query.isPending) return <section className="page today-page"><PageHeader title="今天" subtitle="把注意力放在下一步行动上。" /><div className="page-body"><LoadingState label="正在读取今日工作台…" /></div></section>
  if (query.error) return <section className="page today-page"><PageHeader title="今天" subtitle="把注意力放在下一步行动上。" actions={<button onClick={() => query.refetch()}><RefreshCw size={14} />重试</button>} /><div className="page-body"><ErrorState error={query.error} retry={() => query.refetch()} /></div></section>
  if (showActionForm) return <ActionForm timezone={timezone} todayDate={data?.today.date || ''} onCancel={() => setShowActionForm(false)} onSaved={showLater => { setShowAll(showLater); setShowActionForm(false); void qc.invalidateQueries({ queryKey: ['personal-context'] }) }} />
  return <section className="page today-page">
    <PageHeader title="今天" subtitle={`${data?.today.date || '今天'} · 把注意力放在下一步行动上`} actions={<button onClick={() => brief.mutate()} disabled={brief.isPending}><RefreshCw size={14} />生成摘要</button>} />
    <div className="page-body"><div className="today-grid">
      <section className="today-hero card"><div className="section-heading"><div><span className="eyebrow">每日摘要{data?.brief?.ai_generated ? ' · AI' : ''}</span><h2>{data?.brief?.title || '准备好开始今天'}</h2></div><Clock3 size={22} /></div><p>{data?.brief?.content || '生成今日摘要后，这里会汇总行动、目标和待处理事项。'}</p><div className="today-metrics"><span><strong>{counts.actions ?? data?.today.actions.length ?? 0}</strong>项行动</span>{overdueActions.length > 0 && <span><strong>{overdueActions.length}</strong>项逾期</span>}<span><strong>{counts.next_actions ?? data?.today.next_actions.length ?? 0}</strong>项后续行动</span><span><strong>{counts.goals ?? data?.today.active_goals.length ?? 0}</strong>个目标</span><span><strong>{counts.notifications ?? data?.today.notifications.length ?? 0}</strong>条未读通知</span><span><strong>{counts.external_information ?? 0}</strong>条重要外部信息</span><span><strong>{counts.calendar_events ?? 0}</strong>个日历事件</span></div><div className="daily-brief-actions"><button onClick={() => setShowBriefDetails(true)}>查看详情</button>{data?.brief?.next_step && <small>下一步：{data.brief.next_step}</small>}</div></section>
      <section className="card today-actions-panel"><div className="section-heading"><h2>{showAll ? '后续行动' : '今日行动'}</h2><div className="action-panel-controls"><div className="action-view-tabs" role="tablist" aria-label="行动范围"><button role="tab" aria-selected={!showAll} className={!showAll ? 'active' : ''} onClick={() => setShowAll(false)}>今日行动</button><button role="tab" aria-selected={showAll} className={showAll ? 'active' : ''} onClick={() => setShowAll(true)}>后续行动</button></div><button className="primary" onClick={() => setShowActionForm(true)}><Plus size={15} />新建行动</button></div></div>{!showAll && overdueActions.length > 0 && <div className="today-overdue-group"><h3>已逾期 · {overdueActions.length}</h3><ActionRows openDetail={setDetailActionId} items={overdueActions} timezone={timezone} complete={id => complete.mutate(id)} /></div>}{actions.length ? <ActionRows openDetail={setDetailActionId} items={actions} timezone={timezone} complete={id => complete.mutate(id)} /> : (showAll || !overdueActions.length) && <Empty text={showAll ? '暂无后续行动。' : '还没有安排好的行动。'} />}</section>
      <section className="card today-side-card"><div className="section-heading"><h2><Target size={17} /> 活跃目标</h2><button onClick={() => setShowGoalForm(true)}><Plus size={14} />新建目标</button></div>{data?.today.active_goals.length ? <div className="stack">{data.today.active_goals.map(goal => <article className="today-goal" key={goal.id}><strong>{goal.title}</strong><small>{goal.description || '持续推进中'}</small><button onClick={() => setDetailGoalId(goal.id)}>查看详情</button></article>)}</div> : <div className="today-goal-empty"><Empty text="暂无活跃目标。" /><button onClick={() => setShowGoalForm(true)}>新建目标</button></div>}</section>
      <section className="card today-side-card"><div className="section-heading"><h2><Bell size={17} /> 待处理通知</h2></div>{data?.today.notifications.length ? <div className="stack today-notifications-list">{data.today.notifications.map(item => <article className="today-goal" key={item.id}><strong>{item.title}</strong><small>{item.body}</small><button onClick={() => dismiss.mutate(item.id)}>忽略</button></article>)}</div> : <Empty text="没有待处理通知。" />}</section>
      {detailActionId&&<ActionDetail id={detailActionId} close={()=>setDetailActionId(null)} openSource={onOpenSource}/>}
      <ExternalInformationView />
      <WebMonitorUpdatesView onOpenConversation={onOpenConversation} />
    </div></div>
    {showGoalForm && <GoalForm onCancel={() => setShowGoalForm(false)} onSaved={() => { setShowGoalForm(false); void qc.invalidateQueries({ queryKey: ['personal-context'] }) }} />}
    {detailGoalId && <GoalDetail goalId={detailGoalId} onClose={() => setDetailGoalId(null)} onChanged={() => { void qc.invalidateQueries({ queryKey: ['personal-context'] }); setDetailGoalId(null) }} onDiscuss={plan => { setDiscussionPlan(plan); setDiscussionGoalId(detailGoalId); setDetailGoalId(null) }} />}
    {discussionGoalId && <GoalForm existingGoalId={discussionGoalId} initialRequest="" initialPlan={discussionPlan} onCancel={() => { setDiscussionGoalId(null); setDiscussionPlan(null) }} onSaved={() => { setDiscussionGoalId(null); setDiscussionPlan(null); void qc.invalidateQueries({ queryKey: ['personal-context'] }) }} />}
    {showBriefDetails && <div className="modal-back" role="presentation" onClick={event => { if (event.target === event.currentTarget) setShowBriefDetails(false) }}><div className="modal daily-brief-details" role="dialog" aria-modal="true" aria-labelledby="daily-brief-details-title"><div className="section-heading"><h2 id="daily-brief-details-title">今日摘要详情</h2><button aria-label="关闭详情" onClick={() => setShowBriefDetails(false)}><X size={16} /></button></div><p className="muted">以下内容是生成摘要时整理的只读上下文。</p><BriefDetailSection timezone={timezone} title="今日行动" items={briefContext?.today_actions || data?.today.actions} /><BriefDetailSection timezone={timezone} title="后续行动" items={briefContext?.next_actions || data?.today.next_actions} /><BriefDetailSection timezone={timezone} title="活跃目标" items={briefContext?.active_goals || data?.today.active_goals} /><BriefDetailSection timezone={timezone} title="未读通知" items={briefContext?.unread_notifications || data?.today.notifications} /><BriefDetailSection timezone={timezone} title="重要外部信息" items={briefContext?.important_external_information} /><BriefDetailSection timezone={timezone} title="即将开始的日历事件" items={briefContext?.upcoming_calendar_events} /><BriefDetailSection timezone={timezone} title="连接器提醒" items={briefContext?.connector_alerts} empty="当前没有连接器异常或授权提醒。" /><div className="modal-actions"><button onClick={() => setShowBriefDetails(false)}>关闭</button></div></div></div>}
  </section>
}
