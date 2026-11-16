import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { AlertTriangle, Plus, RefreshCw, RotateCcw, Trash2 } from 'lucide-react'
import { rpc } from './rpc'
import { useConfirmAction } from './ConfirmDialog'
import { ErrorState, LoadingState, PageHeader, StatusBadge as Status, useToast } from './ui'

function PreferencesView() {
  const confirmAction = useConfirmAction()
  const { pushToast } = useToast()
  const info = useQuery({ queryKey: ['system'], queryFn: () => rpc.call<any>('system.info') })
  const settings = useQuery({ queryKey: ['model-settings'], queryFn: () => rpc.call<any>('model_settings.get') })
  const extensions = useQuery({ queryKey: ['extensions'], queryFn: () => rpc.call<any>('extension.list') })
  const notificationSettings = useQuery({ queryKey: ['notification-settings'], queryFn: () => rpc.call<any>('notification.settings.get') })
  const [provider, setProvider] = useState('openai')
  const current = useMemo(() => settings.data?.providers?.find((p: any) => p.provider === provider), [settings.data, provider])
  const [draft, setDraft] = useState<any>({})
  const [roles, setRoles] = useState<any>({})
  useEffect(() => { if (current) setDraft({ ...current, api_key: '' }) }, [current])
  useEffect(() => { if (settings.data) setRoles({ reflection: settings.data.reflection, maintenance: settings.data.maintenance, summary: settings.data.summary, skill_learning: settings.data.skill_learning }) }, [settings.data])

  const input = () => ({ provider, model: draft.model, base_url: draft.base_url || null, api_style: draft.api_style, context_window: draft.context_window || undefined, api_key: draft.api_key || undefined })
  const save = useMutation({
    mutationFn: () => rpc.call('model_settings.update', {
      default_provider: provider,
      providers: settings.data.providers.map((p: any) => p.provider === provider ? input() : ({ provider: p.provider, model: p.model, base_url: p.base_url, api_style: p.api_style, context_window: p.context_window || undefined })),
      reflection: roles.reflection, maintenance: roles.maintenance, summary: roles.summary, skill_learning: roles.skill_learning,
    }),
    onSuccess: () => settings.refetch(),
  })
  const test = useMutation({ mutationFn: () => rpc.call<any>('model_settings.test', input()) })
  const restart = useMutation({ mutationFn: () => rpc.call('system.restart', {}), onSuccess: () => { settings.refetch(); extensions.refetch() } })
  const workspaceChange = useMutation({
    mutationFn: async () => {
      const path = await window.koreNext?.pickDirectory()
      return path ? rpc.call<{ workspace: string }>('workspace.set', { path }) : null
    },
    onSuccess: result => { if (result) { pushToast('工作区已切换', 'success'); void info.refetch() } },
  })
  const revokeAll = useMutation({
    mutationFn: () => rpc.call('connector.revoke_all', {}),
    onSuccess: () => { pushToast('已撤销全部外部授权', 'success'); void notificationSettings.refetch() },
  })
  const initialize = useMutation({
    mutationFn: () => rpc.call('app.initialize', {}),
    onSuccess: () => { pushToast('应用已恢复为初始状态', 'success'); refresh() },
  })
  const deleteAll = useMutation({
    mutationFn: () => rpc.call('data.delete_all', { include_workspace: false }),
    onSuccess: () => { pushToast('本地数据已删除', 'success'); refresh() },
  })
  const backup = useMutation({
    mutationFn: () => rpc.call<any>('database.backup.export', {}),
    onSuccess: result => {
      const blob = new Blob([JSON.stringify(result)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `koreagent-backup-${new Date().toISOString().slice(0, 10)}.json`
      anchor.click()
      URL.revokeObjectURL(url)
      pushToast('加密备份已下载', 'success')
    },
  })
  const refresh = () => { void info.refetch(); void settings.refetch(); void extensions.refetch(); void notificationSettings.refetch() }
  const loading = info.isPending || settings.isPending || extensions.isPending || notificationSettings.isPending
  const error = info.error || settings.error || extensions.error || notificationSettings.error
  const activeContextWindow = Number(settings.data?.active_context_window)
  const hasActiveContextWindow = Number.isFinite(activeContextWindow) && activeContextWindow > 0
  const contextSourceLabel = settings.data?.active_context_window_source === 'configured' ? '手动配置' : settings.data?.active_context_window_source === 'model_name' ? '模型名推断' : settings.data?.active_context_window_source === 'known_model_family' ? '模型系列推断' : settings.data?.active_context_window_source === 'provider_default' ? '提供商默认' : '保守回退'
  const budgetPercent = (value: unknown) => {
    const tokens = Number(value)
    const budgetBase = Number(settings.data?.active_run_budget?.token_budget_limit) || activeContextWindow
    return budgetBase > 0 && Number.isFinite(tokens) ? `（${Math.round(tokens / budgetBase * 100)}%）` : ''
  }

  return <section className="page">
    <PageHeader title="设置" subtitle="配置模型服务、运行环境和扩展能力。" actions={<button onClick={refresh} disabled={loading}><RefreshCw size={14}/>刷新</button>} />
    <div className="page-body">{loading ? <LoadingState label="正在读取设置…" /> : error ? <ErrorState error={error} retry={refresh} /> : <div className="settings-grid">
      <section className="settings-section"><div className="section-heading"><h2>Host</h2>{settings.data?.restart_required&&<Status value="restart_required"/>}</div><dl>
        <dt>版本</dt><dd>{info.data?.version}</dd><dt>当前模型</dt><dd>{settings.data?.active_provider || '—'} / {settings.data?.active_model || '—'}</dd><dt>上下文窗口</dt><dd>{hasActiveContextWindow ? `${activeContextWindow.toLocaleString()} tokens（${contextSourceLabel}）` : '未读取（请重启 Host）'}</dd><dt>本次运行累计预算</dt><dd>{settings.data?.active_run_budget?.enabled === false ? '提醒 / 收尾 / 上限：无限制' : settings.data?.active_run_budget ? `提醒 ${Number(settings.data.active_run_budget.warning_tokens).toLocaleString()}${budgetPercent(settings.data.active_run_budget.warning_tokens)} / ${settings.data.active_run_budget.warning_model_calls} 次调用 · 收尾 ${Number(settings.data.active_run_budget.finalization_tokens).toLocaleString()}${budgetPercent(settings.data.active_run_budget.finalization_tokens)} / ${settings.data.active_run_budget.finalization_model_calls} 次 · 上限 ${Number(settings.data.active_run_budget.hard_tokens).toLocaleString()}${budgetPercent(settings.data.active_run_budget.hard_tokens)} / ${settings.data.active_run_budget.hard_model_calls} 次` : '未读取（请重启 Host）'}</dd><dt>数据库</dt><dd className="mono">{info.data?.database}</dd>
        <dt>工作区</dt><dd className="mono">{info.data?.workspace}</dd><dt>RPC 方法</dt><dd>{info.data?.rpc_methods?.length || 0}</dd>
        <dt>工作区沙箱</dt><dd>{info.data?.execution_capabilities?.workspace_sandbox==='unavailable'?'不可用：Shell 与默认 MCP 将拒绝启动':info.data?.execution_capabilities?.workspace_sandbox||'未知'}</dd>
      </dl><div className="row-actions"><button onClick={() => workspaceChange.mutate()} disabled={workspaceChange.isPending}><Plus size={14}/>{workspaceChange.isPending ? '切换中…' : '选择工作区'}</button><button onClick={() => backup.mutate()} disabled={backup.isPending}>导出加密备份</button></div>{workspaceChange.error&&<p className="form-error">工作区切换失败：{String(workspaceChange.error)}</p>}{backup.error&&<p className="form-error">备份导出失败：{String(backup.error)}</p>}{info.data?.legacy_permission_rules_needing_review>0&&<p className="field-note">有 {info.data.legacy_permission_rules_needing_review} 条旧授权规则已停用；再次执行相应操作时需要重新审批。</p>}{settings.data?.restart_required&&<div className="restart-panel"><p>扩展配置已改变，需要重启 Host 内部服务才能生效。</p><button disabled={!settings.data?.can_restart||restart.isPending} onClick={()=>restart.mutate()}><RotateCcw size={14}/>{restart.isPending?'重启中':'安全重启服务'}</button>{!settings.data?.can_restart&&<small>有运行中的任务，完成或中断后才能重启。</small>}</div>}{restart.error&&<p className="form-error">{String(restart.error)}</p>}</section>
      <section className="settings-section"><h2>模型</h2>
        <div className="provider-tabs">{settings.data?.providers.map((p: any) => <button className={provider === p.provider ? 'active' : ''} key={p.provider} onClick={() => setProvider(p.provider)}>{p.label}</button>)}</div>
        {current && <div className="form-grid">
          <label>模型<input value={draft.model || ''} onChange={e => setDraft({ ...draft, model: e.target.value })}/></label>
          <label>Base URL<input value={draft.base_url || ''} onChange={e => setDraft({ ...draft, base_url: e.target.value })}/></label>
          <label>上下文窗口（可选，Token）<input type="number" min="8192" max="2000000" placeholder={`${current.effective_context_window || '自动识别'}`} value={draft.context_window || ''} onChange={e => setDraft({ ...draft, context_window: e.target.value ? Number(e.target.value) : '' })}/></label>
          <label>API 风格<select value={draft.api_style || 'chat_completions'} onChange={e => setDraft({ ...draft, api_style: e.target.value })}><option value="chat_completions">Chat Completions</option><option value="responses">Responses</option><option value="anthropic_messages">Anthropic Messages</option></select></label>
          <label>API Key<input type="password" value={draft.api_key || ''} placeholder={current.configured ? '已配置（留空保持）' : '请输入 API Key'} onChange={e => setDraft({ ...draft, api_key: e.target.value })}/></label>
          <p className="field-note">上下文预算：{Number(current.effective_context_window || 0) > 0 ? `${Number(current.effective_context_window).toLocaleString()} tokens（${current.context_window_source==='configured'?'手动配置':current.context_window_source==='model_name'?'根据模型名推断':current.context_window_source==='known_model_family'?'根据模型系列推断':current.context_window_source==='provider_default'?'提供商默认值':'保守回退'}）` : '未读取（请重启 Host）'}。密钥来源：{current.key_source==='dpapi'?'Windows DPAPI':current.key_source==='keychain'?'macOS Keychain':current.key_source==='environment'?'环境变量':'未配置'}</p>
          <div className="inline-actions"><button onClick={() => test.mutate()}>{test.isPending ? <RefreshCw size={14}/> : null}测试连接</button><button className="primary" onClick={() => save.mutate()}>保存并设为默认</button></div>
          {test.data && <><Status value={test.data.success ? 'available' : 'failed'}/>{test.data.error&&<p className="form-error">{test.data.error}</p>}</>} {save.error && <p className="form-error">{String(save.error)}</p>}
        </div>}
        <div className="role-settings"><h3>后台模型角色</h3>{(['reflection','maintenance','summary','skill_learning'] as const).map(role=>{const value=roles[role]||{};return <div className="role-row" key={role}><span><strong>{{reflection:'运行后反思',maintenance:'记忆维护',summary:'上下文摘要',skill_learning:'技能学习'}[role]}</strong><small>实际使用：{settings.data?.active_roles?.[role]?.provider||'—'} / {settings.data?.active_roles?.[role]?.model||'—'}</small></span><label><input type="checkbox" checked={value.enabled??true} onChange={e=>setRoles({...roles,[role]:{...value,enabled:e.target.checked}})}/>启用</label><label><input type="checkbox" checked={value.inherit_main??true} onChange={e=>setRoles({...roles,[role]:{...value,inherit_main:e.target.checked}})}/>继承主模型</label>{value.inherit_main===false&&<div className="role-override"><select aria-label={`${role} Provider`} value={value.provider||provider} onChange={e=>setRoles({...roles,[role]:{...value,provider:e.target.value}})}>{settings.data?.providers.map((item:any)=><option key={item.provider} value={item.provider}>{item.label}</option>)}</select><input aria-label={`${role} 模型`} placeholder="模型名" value={value.model||''} onChange={e=>setRoles({...roles,[role]:{...value,model:e.target.value}})}/></div>}</div>})}</div>
      </section>
      <section className="settings-section"><RunBudgetSettings data={settings.data?.run_budget} contextWindow={activeContextWindow} refresh={() => settings.refetch()}/></section>
      <section className="settings-section"><PersonalSettings/></section>
      <section className="settings-section"><NotificationSettings data={notificationSettings.data?.settings} save={() => notificationSettings.refetch()} /></section>
      <section className="settings-section"><DataControls confirmAction={confirmAction} revokeAll={revokeAll} initialize={initialize} deleteAll={deleteAll} /></section>
      <section className="settings-section span-2"><ExtensionManager data={extensions.data} refresh={() => extensions.refetch()}/></section>
    </div>}</div>
  </section>
}

export function RunBudgetSettings({ data, contextWindow, refresh }: { data?: { mode: string; multiplier: number; model_call_budget: number }; contextWindow: number; refresh(): unknown }) {
  const { pushToast } = useToast()
  const [mode, setMode] = useState('unlimited')
  const [multiplier, setMultiplier] = useState('3')
  const [calls, setCalls] = useState('24')
  useEffect(() => {
    if (data) { setMode(data.mode); setMultiplier(String(data.multiplier)); setCalls(String(data.model_call_budget)) }
  }, [data])
  const valid = Number(multiplier) >= 0.25 && Number(multiplier) <= 100 && Number.isInteger(Number(calls)) && Number(calls) >= 3 && Number(calls) <= 100000
  const save = useMutation({
    mutationFn: () => rpc.call('run_budget.update', { mode, multiplier: Number(multiplier), model_call_budget: Number(calls) }),
    onSuccess: () => { pushToast('运行预算已保存，将应用于新任务', 'success'); void refresh() },
  })
  const total = contextWindow * Number(multiplier)
  return <><h2>单次任务运行预算</h2>
    <p className="field-note">累计预算统计本次任务主模型每次调用的输入和输出，包含重复发送的历史消息。上下文压缩和外部动作审批始终保留。</p>
    <div className="form-grid">
      <label>预算模式<select value={mode} onChange={e => setMode(e.target.value)}><option value="unlimited">无限制</option><option value="custom">自定义限制</option></select></label>
      {mode==='custom'&&<>
        <label>上下文窗口倍数<input type="number" min="0.25" max="100" step="any" value={multiplier} onChange={e => setMultiplier(e.target.value)}/></label>
        <label>模型调用预算（含最终答复）<input type="number" min="3" max="100000" step="1" value={calls} onChange={e => setCalls(e.target.value)}/></label>
      </>}
    </div>
    {mode==='custom'?<>
      {valid&&Number.isFinite(total)&&total>0&&<p className="field-note">Token 预算基数：{contextWindow.toLocaleString()} × {multiplier} = {Math.floor(total).toLocaleString()}。提醒（80%）：{Math.floor(total*0.8).toLocaleString()}；收尾（90%）：{Math.floor(total*0.9).toLocaleString()}；上限（95%）：{Math.floor(total*0.95).toLocaleString()}。模型调用上限：{calls} 次。</p>}
      <p className="field-note">达到提醒后允许最后一轮必要工具调用。有新的有效资料或已确认动作时，最多自动扩展一次（原额度的 50%）；重复结果、失败和空结果不算进展，否则直接收尾。达到硬上限后停止后续调用。</p>
      {!valid&&<p className="form-error" role="alert">倍数须为 0.25～100，调用次数须为 3～100000 的整数。</p>}
    </>:<p className="field-note">默认无限制：不触发运行预算提醒、收尾、自动扩展或模型调用次数上限；仍统计用量，并保留取消、重复工具调用检测、权限控制和独立的 Host 安全步数保护。</p>}
    <p className="field-note">保存后新任务生效。正在运行或从检查点恢复的任务沿用原预算。</p>
    <div className="row-actions"><button className="primary" onClick={() => save.mutate()} disabled={save.isPending||!valid}>{save.isPending?'保存中…':'保存运行预算'}</button></div>
    {save.error&&<p className="form-error" role="alert">保存失败：{String(save.error)}</p>}
  </>
}

function NotificationSettings({ data, save }: { data: any; save(): unknown }) {
  const [draft, setDraft] = useState({ enabled: true, quiet_hours_start: '22:00', quiet_hours_end: '08:00', priority: 'normal' })
  const [error, setError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  useEffect(() => { if (data) setDraft(current => ({ ...current, ...data })) }, [data])
  const submit = async () => {
    setSaving(true); setError(null)
    try { await rpc.call('notification.settings.update', draft); await save() } catch (value) { setError(value) } finally { setSaving(false) }
  }
  return <section><h2>通知设置</h2><div className="form-grid"><label>桌面通知<select value={draft.enabled ? 'enabled' : 'disabled'} onChange={event => setDraft({ ...draft, enabled: event.target.value === 'enabled' })}><option value="enabled">开启</option><option value="disabled">关闭</option></select></label><label>通知优先级<select value={draft.priority} onChange={event => setDraft({ ...draft, priority: event.target.value })}><option value="low">低</option><option value="normal">普通</option><option value="high">高</option></select></label><label>免打扰开始<input type="time" value={draft.quiet_hours_start} onChange={event => setDraft({ ...draft, quiet_hours_start: event.target.value })} /></label><label>免打扰结束<input type="time" value={draft.quiet_hours_end} onChange={event => setDraft({ ...draft, quiet_hours_end: event.target.value })} /></label></div><div className="row-actions"><button className="primary" onClick={() => void submit()} disabled={saving}>{saving ? '保存中…' : '保存通知设置'}</button></div>{Boolean(error)&&<p className="form-error">通知设置保存失败：{String(error)}</p>}</section>
}

function DataControls({ confirmAction, revokeAll, initialize, deleteAll }: { confirmAction: ReturnType<typeof useConfirmAction>; revokeAll: any; initialize: any; deleteAll: any }) {
  const ask = (action: () => void, title: string, message: string, confirmLabel: string) => { void confirmAction({ title, message, confirmLabel, destructive: true }).then(yes => { if (yes) action() }) }
  return <section><h2>数据与授权</h2><p className="field-note">这些操作只影响本地数据和授权状态，不会删除项目目录中的 .env 配置。</p><div className="row-actions"><button className="danger" onClick={() => ask(() => revokeAll.mutate(), '撤销全部授权？', '邮箱和飞书的本地授权令牌将被清除，之后需要重新连接账号。', '撤销全部授权')} disabled={revokeAll.isPending}>撤销全部授权</button><button className="danger" onClick={() => ask(() => initialize.mutate(), '初始化应用？', '将清除会话、运行记录、记忆、连接器缓存和本地授权，保留 .env 与工作区文件。', '初始化应用')} disabled={initialize.isPending}>初始化应用</button><button className="danger" onClick={() => ask(() => deleteAll.mutate(), '删除所有本地数据？', '将删除本地数据库中的个人数据、连接器缓存和授权。此操作无法撤销。', '删除所有本地数据')} disabled={deleteAll.isPending}>删除所有本地数据</button></div>{revokeAll.error&&<p className="form-error">撤销授权失败：{String(revokeAll.error)}</p>}{initialize.error&&<p className="form-error">初始化失败：{String(initialize.error)}</p>}{deleteAll.error&&<p className="form-error">删除本地数据失败：{String(deleteAll.error)}</p>}</section>
}

function PersonalSettings(){
  const profile=useQuery({queryKey:['profile'],queryFn:()=>rpc.call<any>('profile.get')})
  const privacy=useQuery({queryKey:['privacy'],queryFn:()=>rpc.call<any>('privacy.get')})
  const [draft,setDraft]=useState<any>(null)
  useEffect(()=>{if(profile.data?.profile&&!draft)setDraft(profile.data.profile)},[profile.data,draft])
  const save=useMutation({mutationFn:()=>rpc.call('profile.update',draft),onSuccess:()=>profile.refetch()})
  const savePrivacy=useMutation({mutationFn:(value:any)=>rpc.call('privacy.update',value),onSuccess:()=>privacy.refetch()})
  if(profile.error || privacy.error)return <ErrorState error={profile.error || privacy.error} retry={() => { void profile.refetch(); void privacy.refetch() }} />
  if(profile.isPending || privacy.isPending || !draft)return <LoadingState label="正在读取个人设置…" />
  return <><h2>个人工作台</h2><div className="form-grid"><label>称呼<input value={draft.name||''} onChange={e=>setDraft({...draft,name:e.target.value})}/></label><label>助手名称<input value={draft.assistant_name||''} onChange={e=>setDraft({...draft,assistant_name:e.target.value})}/></label><label>时区<input value={draft.timezone||''} onChange={e=>setDraft({...draft,timezone:e.target.value})}/></label><label>沟通风格<input value={draft.communication_style||''} onChange={e=>setDraft({...draft,communication_style:e.target.value})}/></label></div><div className="inline-actions"><button className="primary" onClick={()=>save.mutate()} disabled={save.isPending}>保存档案</button></div><div className="role-settings"><h3>隐私</h3>{privacy.data?.privacy&&Object.entries(privacy.data.privacy).map(([key,value])=>key==='memory_review_mode'?<label key={key}>长期记忆保存方式<select value={String(value)} onChange={e=>savePrivacy.mutate({memory_review_mode:e.target.value})}><option value="review">保存前需要确认</option><option value="automatic">自动保存</option></select></label>:<label key={key}><input type="checkbox" checked={Boolean(value)} onChange={e=>savePrivacy.mutate({[key]:e.target.checked})}/>{({auto_memory_enabled:'自动记忆',memory_in_context_enabled:'将记忆注入上下文',save_run_trace:'保存运行轨迹',desktop_notifications_enabled:'桌面通知',cloud_processing_allowed:'允许云端模型处理个人内容'} as Record<string,string>)[key]||key}</label>)}</div></>
}

function ExtensionManager({ data, refresh }: { data: any; refresh(): unknown }) {
  const confirmAction = useConfirmAction()
  const { pushToast } = useToast()
  const [skill, setSkill] = useState({ name: '', description: '', instructions: '', scope: 'project' })
  const [mcp, setMcp] = useState({ name: '', command: '', args: '', cwd: '', env: '', permission: 'human_approval', filesystem: 'workspace', network: 'denied', readable_roots: '', writable_roots: '', allowed_domains: '' })
  const [source, setSource] = useState('')
  const [importPlan, setImportPlan] = useState<any>(null)
  const install = useMutation({ mutationFn: () => rpc.call('skill.install', skill), onSuccess: () => { setSkill({ ...skill, name: '', description: '', instructions: '' }); refresh() } })
  const toggle = useMutation({ mutationFn: ({ item, enabled }: any) => rpc.call('skill.set_enabled', { name: item.name, scope: item.scope, enabled }), onSuccess: refresh })
  const deleteSkill = useMutation({ mutationFn: (item: any) => rpc.call('skill.delete', { name: item.name, scope: item.scope, enabled: item.enabled }), onSuccess: refresh })
  const env=Object.fromEntries(mcp.env.split(/\r?\n/).map(line=>line.split('=',2).map(part=>part.trim())).filter(parts=>parts.length===2&&parts[0]))
  const roots=(value:string)=>value.split(/\r?\n|,/).map(item=>item.trim()).filter(Boolean)
  const mcpPayload={ name:mcp.name,command:mcp.command,args:mcp.args.split(/\s+/).filter(Boolean),cwd:mcp.cwd||null,env,enabled:true,permission:mcp.permission,sandbox:{filesystem:mcp.filesystem,network:mcp.network,readable_roots:roots(mcp.readable_roots),writable_roots:roots(mcp.writable_roots),allowed_domains:roots(mcp.allowed_domains)} }
  const addMcp = useMutation({ mutationFn: () => rpc.call('mcp.add', mcpPayload), onSuccess: () => { setMcp({ ...mcp, name: '', command: '', args: '', cwd: '', env: '' }); refresh() } })
  const toggleMcp = useMutation({ mutationFn: ({name,enabled}:{name:string;enabled:boolean}) => rpc.call('mcp.set_enabled', { name, enabled }), onSuccess: refresh })
  const deleteMcp = useMutation({ mutationFn: (name: string) => rpc.call('mcp.delete', { name }), onSuccess: refresh })
  const previewExtension = useMutation({ mutationFn: () => rpc.call<any>('extension.import.preview', { input: source, skill_scope: 'project', mcp_permission: 'human_approval' }), onSuccess: result=>setImportPlan(result.plan) })
  const importExtension = useMutation({ mutationFn: () => rpc.call('extension.import.apply', { input: source, skill_scope: 'project', mcp_permission: 'human_approval', fingerprint: importPlan.fingerprint, confirmed: true }), onSuccess: () => { setSource(''); setImportPlan(null); refresh() } })
  const promote = useMutation({ mutationFn: (candidate:any) => rpc.call('skill.install', {name:candidate.name,description:candidate.description,instructions:candidate.instructions,scope:'project',candidate_id:candidate.id}), onSuccess: refresh })
  const dismissCandidate = useMutation({ mutationFn: (candidate: any) => rpc.call('skill_candidate.dismiss', { candidate_id: candidate.id }), onSuccess: () => { pushToast('已舍弃该候选 Skill', 'success'); setSelectedCandidate(null); refresh() } })
  const [candidateTab, setCandidateTab] = useState<'ready' | 'observing'>('ready')
  const [selectedCandidate, setSelectedCandidate] = useState<any | null>(null)
  const candidates = (data?.skill_candidates || []).filter((candidate: any) => candidate.status === 'ready' || candidate.status === 'observing')
  const visibleCandidates = candidates.filter((candidate: any) => candidate.status === candidateTab)
  const candidateCount = (status: 'ready' | 'observing') => candidates.filter((candidate: any) => candidate.status === status).length
  const formatCandidateJson = (value: unknown) => JSON.stringify(value ?? {}, null, 2)
  const askDismissCandidate = (candidate: any) => {
    void confirmAction({
      title: `舍弃候选 Skill “${candidate.title || candidate.name}”？`,
      message: '舍弃后它会从候选列表中移除，也不会删除已经安装的 Skill。',
      confirmLabel: '舍弃候选',
      destructive: true,
    }).then(yes => { if (yes) dismissCandidate.mutate(candidate) })
  }
  return <><h2>扩展</h2>
    <div className="import-row"><input value={source} onChange={e => {setSource(e.target.value);setImportPlan(null)}} placeholder="扩展路径、GitHub URL 或 mcpServers JSON"/><button disabled={!source||previewExtension.isPending} onClick={() => previewExtension.mutate()}><Plus size={14}/>预览导入</button></div>
    {importPlan&&<div className="import-plan"><strong>将执行 {importPlan.actions?.length||0} 项变更</strong><ul>{importPlan.actions?.map((action:string)=><li key={action}>{action}</li>)}</ul>{importPlan.warnings?.map((warning:string)=><p key={warning}>{warning}</p>)}<div className="row-actions"><button onClick={()=>setImportPlan(null)}>取消</button><button className="primary" disabled={importExtension.isPending} onClick={()=>importExtension.mutate()}>确认并导入</button></div></div>}
    {(previewExtension.error||importExtension.error)&&<p className="form-error">{String(previewExtension.error||importExtension.error)}</p>}
    {data?.skill_diagnostics?.length>0&&<div className="diagnostic-list"><AlertTriangle size={17}/><div><strong>Skill 诊断</strong>{data.skill_diagnostics.map((item:any)=><p key={`${item.scope}:${item.name}`}>{item.name}：{item.error}</p>)}</div></div>}
    <div className="extension-columns"><div><h3>Skills</h3>
      {data?.skills?.map((s: any) => <div className="extension-row" key={`${s.scope}:${s.name}`}><div><strong>{s.name}</strong><small>{s.description}</small></div><div className="row-actions"><button onClick={() => toggle.mutate({ item: s, enabled: !s.enabled })}>{s.enabled ? '停用' : '启用'}</button><button className="danger" aria-label={`删除 Skill ${s.name}`} onClick={() => {void confirmAction({title:`删除 Skill “${s.name}”？`,message:'这项 Skill 将从当前作用域移除，后续运行无法使用。',confirmLabel:'删除 Skill',destructive:true}).then(yes=>{if(yes)deleteSkill.mutate(s)})}}><Trash2 size={13}/></button></div></div>)}
      <div className="mini-form"><input placeholder="Skill 名称" value={skill.name} onChange={e => setSkill({ ...skill, name: e.target.value })}/><input placeholder="说明" value={skill.description} onChange={e => setSkill({ ...skill, description: e.target.value })}/><textarea placeholder="Skill 指令" value={skill.instructions} onChange={e => setSkill({ ...skill, instructions: e.target.value })}/><button className="primary" disabled={!skill.name || !skill.instructions} onClick={() => install.mutate()}>安装 Skill</button></div>
      {candidates.length > 0 && <div className="candidate-list"><div className="candidate-list-head"><div><h3>从成功任务学习</h3><p className="field-note">重复成功的工具流程会先进入候选区，查看详情后再决定安装或舍弃。</p></div><div className="candidate-tabs" role="tablist" aria-label="候选 Skill 状态"><button role="tab" aria-selected={candidateTab === 'ready'} className={candidateTab === 'ready' ? 'active' : ''} onClick={() => setCandidateTab('ready')}>READY <span>{candidateCount('ready')}</span></button><button role="tab" aria-selected={candidateTab === 'observing'} className={candidateTab === 'observing' ? 'active' : ''} onClick={() => setCandidateTab('observing')}>OBSERVING <span>{candidateCount('observing')}</span></button></div></div>{visibleCandidates.length ? visibleCandidates.map((candidate:any)=><div className="candidate-row" key={candidate.id}><div><strong>{candidate.title || candidate.name}</strong><small>{candidate.tool_sequence?.join(' → ') || '暂无工具路线'} · 成功 {candidate.success_count || 0} 次 · 置信度 {Math.round(Number(candidate.confidence || 0) * 100)}%</small></div><div className="row-actions"><Status value={candidate.status} label={candidate.status.toUpperCase()}/><button onClick={() => setSelectedCandidate(candidate)}>查看详情</button>{candidate.status==='ready'&&<><button disabled={promote.isPending} onClick={()=>promote.mutate(candidate)}>审核后安装</button><button className="danger" disabled={dismissCandidate.isPending} onClick={() => askDismissCandidate(candidate)}>舍弃</button></>}</div></div>) : <p className="candidate-empty">这个分类暂时没有候选 Skill。</p>}{(promote.error || dismissCandidate.error) && <p className="form-error">{String(promote.error || dismissCandidate.error)}</p>}</div>}
    </div><div><h3>MCP Servers</h3>
      {data?.mcp?.servers?.map((s: any) => <div className="extension-row" key={s.name}><div><strong>{s.name}</strong><small>{s.command} {s.args?.join(' ')} · {s.sandbox_backend}</small>{s.error&&<small className="form-error">{s.error}</small>}<small>{s.tool_names?.length||0} 个工具 · 重启 {s.restart_count||0} 次</small></div><div className="row-actions"><Status value={s.state}/><button onClick={()=>toggleMcp.mutate({name:s.name,enabled:!s.enabled})}>{s.enabled?'停用':'启用'}</button><button className="danger" aria-label={`删除 MCP ${s.name}`} onClick={() => {void confirmAction({title:`删除 MCP Server “${s.name}”？`,message:'服务器配置将被删除，之后的运行无法调用它提供的工具。',confirmLabel:'删除 Server',destructive:true}).then(yes=>{if(yes)deleteMcp.mutate(s.name)})}}><Trash2 size={13}/></button></div></div>)}
      <div className="mini-form"><input placeholder="Server 名称" value={mcp.name} onChange={e => setMcp({ ...mcp, name: e.target.value })}/><input placeholder="命令" value={mcp.command} onChange={e => setMcp({ ...mcp, command: e.target.value })}/><input placeholder="参数（空格分隔）" value={mcp.args} onChange={e => setMcp({ ...mcp, args: e.target.value })}/><input placeholder="工作目录（可选）" value={mcp.cwd} onChange={e=>setMcp({...mcp,cwd:e.target.value})}/><textarea placeholder={'环境变量，每行 NAME=value'} value={mcp.env} onChange={e=>setMcp({...mcp,env:e.target.value})}/><div className="mcp-options"><label>权限<select value={mcp.permission} onChange={e => setMcp({ ...mcp, permission: e.target.value })}><option value="human_approval">每次审批</option><option value="allowed">允许</option><option value="forbidden">禁止</option></select></label><label>文件系统<select value={mcp.filesystem} onChange={e=>setMcp({...mcp,filesystem:e.target.value})}><option value="workspace">仅工作区</option><option value="readonly">只读</option><option value="host">主机</option></select></label><label>网络<select value={mcp.network} onChange={e=>setMcp({...mcp,network:e.target.value})}><option value="denied">禁止</option><option value="allowed_domains">仅允许域名</option><option value="allowed">允许</option></select></label></div><textarea placeholder="可读目录（逗号或换行分隔）" value={mcp.readable_roots} onChange={e=>setMcp({...mcp,readable_roots:e.target.value})}/><textarea placeholder="可写目录（逗号或换行分隔）" value={mcp.writable_roots} onChange={e=>setMcp({...mcp,writable_roots:e.target.value})}/><textarea placeholder="允许域名（逗号或换行分隔）" value={mcp.allowed_domains} onChange={e=>setMcp({...mcp,allowed_domains:e.target.value})}/><details className="json-preview"><summary>配置预览</summary><pre>{JSON.stringify(mcpPayload,null,2)}</pre></details><button className="primary" disabled={!mcp.name || !mcp.command} onClick={() => addMcp.mutate()}>添加 MCP</button></div>
    </div></div>
    {selectedCandidate && <div className="modal-back" onMouseDown={event => { if (event.target === event.currentTarget) setSelectedCandidate(null) }}><section className="modal candidate-detail" role="dialog" aria-modal="true" aria-labelledby="candidate-detail-title"><div className="candidate-detail-head"><div><h2 id="candidate-detail-title">{selectedCandidate.title || selectedCandidate.name}</h2><p>{selectedCandidate.description || '暂无描述'}</p></div><Status value={selectedCandidate.status} label={String(selectedCandidate.status || '').toUpperCase()}/></div><dl className="candidate-detail-facts"><div><dt>Skill 名称</dt><dd>{selectedCandidate.name || '—'}</dd></div><div><dt>成功次数</dt><dd>{selectedCandidate.success_count || 0}</dd></div><div><dt>置信度</dt><dd>{Math.round(Number(selectedCandidate.confidence || 0) * 100)}%</dd></div><div><dt>首次运行</dt><dd>{selectedCandidate.first_run_id || '—'}</dd></div><div><dt>最近运行</dt><dd>{selectedCandidate.last_run_id || '—'}</dd></div><div><dt>创建时间</dt><dd>{selectedCandidate.created_at || '—'}</dd></div></dl><div className="candidate-detail-section"><h3>适用场景</h3><p>{selectedCandidate.applicability || '尚未生成额外适用场景说明。'}</p></div><div className="candidate-detail-section"><h3>工具调用路线</h3><p className="candidate-tool-sequence">{selectedCandidate.tool_sequence?.join(' → ') || '暂无工具路线'}</p><pre>{formatCandidateJson(selectedCandidate.tool_counts)}</pre></div><div className="candidate-detail-section"><h3>完整执行指令</h3><pre>{selectedCandidate.instructions || '暂无执行指令'}</pre></div><div className="candidate-detail-section"><h3>学习证据</h3>{selectedCandidate.evidence?.length ? <div className="candidate-evidence">{selectedCandidate.evidence.map((item: any, index: number) => <details key={`${item.run_id || 'evidence'}-${index}`}><summary>{item.tool_name || '工具调用'} · Run {item.run_id || '—'}</summary><p>参数：{item.argument_names?.join(', ') || '无'}</p>{item.outcome_excerpt && <pre>{item.outcome_excerpt}</pre>}</details>)}</div> : <p>暂无证据。</p>}</div><div className="candidate-detail-section"><h3>与已有 Skill 的关系</h3><p>{selectedCandidate.action?.kind === 'update' ? `建议更新已有 Skill：${selectedCandidate.action.skill_name || '—'}（重合度 ${Math.round(Number(selectedCandidate.action.overlap || 0) * 100)}%）` : '建议创建新的 Skill。'}</p>{selectedCandidate.example_run_ids?.length > 0 && <p>示例运行：{selectedCandidate.example_run_ids.join('、')}</p>}</div><div className="modal-actions"><button onClick={() => setSelectedCandidate(null)}>关闭</button>{selectedCandidate.status === 'ready' && <><button disabled={promote.isPending} onClick={() => { promote.mutate(selectedCandidate); setSelectedCandidate(null) }}>审核后安装</button><button className="confirm-danger" disabled={dismissCandidate.isPending} onClick={() => askDismissCandidate(selectedCandidate)}>舍弃候选</button></>}</div></section></div>}
  </>
}

export default PreferencesView

