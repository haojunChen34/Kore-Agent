import { recoveryGuidance } from './recoveryGuidance'
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import ReactMarkdown from 'react-markdown'

import remarkGfm from 'remark-gfm'

import {

  Archive, Bot, Brain, CheckCircle2, ChevronRight, Clock3, ExternalLink, Inbox,

  FileOutput, Menu, MessageSquare, PauseCircle, Pencil, Pin, PinOff,

  Plus, RefreshCw, Search, Settings, ShieldCheck, Trash2,

  XCircle, Zap,

} from 'lucide-react'

import { artifactContentUrl } from './apiConfig'

import { approvalFacts, approvalTitle, permissionScopeLabel } from './approvalPresentation'

import { queryKeysForAgentEvent } from './eventInvalidation'

import { ConfirmProvider, useConfirmAction } from './ConfirmDialog'

import { liveRunPlaceholder, type LiveRunState } from './liveState'

import { conversationActivity } from './conversationActivity'

import PreferencesView from './PreferencesView'

import TodayView from './TodayView'

import InboxView from './InboxView'

import ProactiveView from './ProactiveView'

import RunExplorer from './RunExplorer'

import UnifiedSearchView, { type SearchResult } from './UnifiedSearchView'

import ResearchView from './ResearchView'

import ConnectorHealthView from './ConnectorHealthView'

import { EmptyState as Empty, ErrorState as ErrorBox, ErrorState, LoadingState as Busy, LoadingState, Page, StatusBadge as Status, ToastProvider, useToast, HostOfflineBanner, statusChinese, type ToastTone } from './ui'

import { rpc } from './rpc'

import type { Approval, Artifact, Automation, Conversation, MemoryItem, Message, PermissionScope, Run, Task } from './types'



type View='today'|'inbox'|'search'|'research'|'proactive'|'external'|'connector-health'|'chat'|'runs'|'tasks'|'automations'|'approvals'|'artifacts'|'memory'|'settings'

type NavItem={view:View;label:string;Icon:typeof MessageSquare}

type NavGroup={id:'workbench'|'chat'|'information'|'research'|'execution'|'settings';label:string;Icon:typeof MessageSquare;defaultView:View;items:NavItem[];badgeCounts?:Partial<Record<View,number>>}

const navGroups:NavGroup[]=[

  {id:'workbench',label:'工作台',Icon:Clock3,defaultView:'today',items:[{view:'today',label:'今日',Icon:Clock3}]},

  {id:'chat',label:'对话',Icon:MessageSquare,defaultView:'chat',items:[{view:'chat',label:'对话',Icon:MessageSquare}]},

  {id:'information',label:'信息中心',Icon:Inbox,defaultView:'inbox',items:[{view:'inbox',label:'收件箱',Icon:Inbox},{view:'search',label:'统一搜索',Icon:Search},{view:'memory',label:'长期记忆',Icon:Brain}]},

  {id:'research',label:'研究中心',Icon:FileOutput,defaultView:'research',items:[{view:'research',label:'研究',Icon:FileOutput},{view:'proactive',label:'主动执行',Icon:Zap}]},

  {id:'execution',label:'执行中心',Icon:Archive,defaultView:'runs',items:[{view:'runs',label:'运行记录',Icon:Archive},{view:'tasks',label:'任务与计划',Icon:CheckCircle2},{view:'automations',label:'自动化',Icon:Clock3},{view:'approvals',label:'审批',Icon:ShieldCheck},{view:'external',label:'外部操作',Icon:RefreshCw},{view:'artifacts',label:'交付物',Icon:FileOutput}]},

  {id:'settings',label:'设置',Icon:Settings,defaultView:'settings',items:[{view:'settings',label:'设置',Icon:Settings},{view:'connector-health',label:'连接器健康',Icon:RefreshCw}]},

]

const groupForView=(view:View)=>navGroups.find(group=>group.items.some(item=>item.view===view))||navGroups[1]

const displayConversationTitle=(title?:string|null)=>title==='新对话'?'未命名会话':(title||'未命名会话')



function useHostEvents(pushToast:(message:string,tone:ToastTone)=>void){

  const query=useQueryClient()

  useEffect(()=>{

    rpc.connect()

    // Durable events invalidate only the views they affect; token deltas are

    // rendered by the live reducer without repeatedly fetching the full trace.

    const invalidate=(...keys:string[])=>keys.forEach(key=>query.invalidateQueries({queryKey:[key]}))

    const agent=rpc.on('agent.event',(event:any)=>{

      const kind=String(event?.type||'')

      const conversationId=String(event?.conversation_id||'')

      const history=query.getQueryData<{messages:Message[]}>(['conversation',conversationId])

      conversationActivity.event(event,history?.messages.length)

      invalidate(...queryKeysForAgentEvent(kind))

      if(conversationId&&['agent_completed','agent_failed','run_interrupted','run_cancelled'].includes(kind)){

        void query.invalidateQueries({queryKey:['conversation',conversationId]})

        void query.invalidateQueries({queryKey:['artifacts',conversationId]})

      }

      if(kind==='agent_completed'){window.koreNext?.notify('KoreAgent Next','任务已完成');pushToast('任务已完成','success')}

      if(kind==='agent_failed'){

        const message=String(event?.error?.message||'')

        const budget=message.toLowerCase().includes('budget')||message.includes('棰勭畻')

        const notice=budget?'读取内容过多，运行预算已耗尽，请缩小文件范围后重试。':'任务执行失败，请查看运行记录'

        window.koreNext?.notify('KoreAgent Next',notice);pushToast(notice,'danger')

      }

    })

    const run=rpc.on('run.status',(event:any)=>{

      const conversationId=conversationActivity.status(String(event?.run_id||''),String(event?.status||''))

      invalidate('runs')

      if(conversationId)void query.invalidateQueries({queryKey:['conversation',conversationId]})

    })

    const required=rpc.on('approval.required',()=>{invalidate('approvals')})

    const resolved=rpc.on('approval.resolved',()=>invalidate('approvals'))
    const approvalDeleted=rpc.on('approval.deleted',()=>invalidate('approvals'))
    const operationDeleted=rpc.on('external.operation.deleted',()=>invalidate('external-operations','external-workflow'))

    const artifact=rpc.on('artifact.created',(item:any)=>{invalidate('artifacts');const message=`已生成交付物：${item?.title||'未命名'}`;window.koreNext?.notify('KoreAgent Next',message);pushToast(message,'success')})

    const personal=rpc.on('personal.notification',(item:any)=>{invalidate('personal-context','notifications');if(item?.title)window.koreNext?.notify(item.title,item.body||'');pushToast(item.title||'新通知','info')})

    const connectorSynced=rpc.on('connector.synced',()=>{invalidate('connector-inbox','connector-connections','connector-events')})

    const connectorUpdated=rpc.on('connector.updated',()=>{invalidate('connector-inbox','connector-connections')})

    const connectorItemUpdated=rpc.on('connector.item.updated',()=>{invalidate('connector-inbox')})
    const connectorItemDeleted=rpc.on('connector.item.deleted',()=>{invalidate('connector-inbox')})
    const externalInformation=rpc.on('external_information.updated',()=>{invalidate('external-information','personal-context')})
    const monitorChanged=rpc.on('research.monitor.changed',()=>{invalidate('research-monitor-updates','personal-context','notifications')})
    const monitorFailed=rpc.on('research.monitor.failed',()=>{invalidate('research-monitor-updates','personal-context','notifications')})

    const proactive=rpc.on('proactive.suggestion.created',()=>{invalidate('proactive-suggestions');invalidate('execution-jobs');invalidate('audit-log');invalidate('notifications')})

    const job=rpc.on('execution.job.updated',()=>{invalidate('proactive-suggestions');invalidate('execution-jobs');invalidate('audit-log')})

    return()=>{agent();run();required();resolved();approvalDeleted();operationDeleted();artifact();personal();connectorSynced();connectorUpdated();connectorItemUpdated();connectorItemDeleted();externalInformation();monitorChanged();monitorFailed();proactive();job()}

  },[pushToast,query])

}



export default function App(){return <ToastProvider><ConfirmProvider><AppContent/></ConfirmProvider></ToastProvider>}



function AppContent(){

  const query=useQueryClient()

  const [view,setView]=useState<View>('chat');const [status,setStatus]=useState(rpc.status);const [hadConnection,setHadConnection]=useState(false);const [focusedConversationId,setFocusedConversationId]=useState<string|null>(null);const [focusedRunId,setFocusedRunId]=useState<string|null>(null);const [focusedInboxItemId,setFocusedInboxItemId]=useState<string|null>(null);const [focusedMemoryId,setFocusedMemoryId]=useState<string|null>(null);const [focusedTaskId,setFocusedTaskId]=useState<string|null>(null);const [requestedMode,setRequestedMode]=useState<'normal'|'plan'|null>(null)

  const { pushToast }=useToast()

  useHostEvents(pushToast)

  const runSummary=useQuery({queryKey:['runs','rail'],queryFn:()=>rpc.call<{runs:Run[]}>('run.list',{limit:100}).then(value=>value.runs),refetchInterval:15000,retry:false})

  const approvalSummary=useQuery({queryKey:['approvals','rail'],queryFn:()=>rpc.call<{approvals:Approval[]}>('approval.list',{status:'pending',limit:100}).then(value=>value.approvals),refetchInterval:15000,retry:false})

  const pendingPlanSummary=useQuery({queryKey:['tasks','rail'],queryFn:()=>rpc.call<{count:number}>('task.list',{status:'pending',limit:500}).then(value=>value.count),refetchInterval:15000,retry:false})

  const runningCount=runSummary.data?.filter(item=>['pending','running'].includes(item.status)).length||0

  const approvalCount=approvalSummary.data?.length||0

  const pendingPlanCount=pendingPlanSummary.data||0

  useEffect(()=>{rpc.onStatus=value=>{setStatus(value);if(value==='connected'){setHadConnection(true);void query.invalidateQueries({queryKey:['runs']});void query.invalidateQueries({queryKey:['conversation']});void query.invalidateQueries({queryKey:['approvals']});void query.invalidateQueries({queryKey:['tasks','rail']})}};rpc.connect();return()=>{rpc.onStatus=undefined}},[query])

  const activeGroup={...groupForView(view),badgeCounts:{runs:runningCount,tasks:pendingPlanCount,approvals:approvalCount}}
  const openSearchSource=(item:SearchResult)=>{const kind=item.detail_kind||item.type;if(kind==='conversation'){setFocusedConversationId(String(item.source_ref?.conversation_id||item.entity_id||''));setView('chat');return}if(item.source==='connector'){setFocusedInboxItemId(String(item.source_ref?.item_id||item.entity_id||''));setView('inbox');return}if(item.source==='memory'){setFocusedMemoryId(item.entity_id||item.id);setView('memory');return}if(item.source==='personal'){setFocusedTaskId(item.entity_id||item.id);setView('tasks')}}

  return <div className="app-shell"><aside className="sidebar" aria-label="主导航"><div className="brand" title="KoreAgent Next"><Bot size={20}/></div><nav>{navGroups.map(group=>{const count=group.id==='execution'?(runningCount+approvalCount+pendingPlanCount):0;return <button key={group.id} aria-label={group.label} aria-current={activeGroup.id===group.id?'page':undefined} title={group.label} className={activeGroup.id===group.id?'active':''} onClick={()=>setView(group.defaultView)}><group.Icon size={19}/><span>{group.label}</span>{count>0&&<b className="nav-badge" aria-label={`${count} 项待处理`}>{count>99?'99+':count}</b>}</button>})}</nav><div className={`host-dot ${status}`} role="status" aria-label={`Host ${status}`} title={`Host: ${status}`}/></aside><main><div className="main-frame"><HostOfflineBanner visible={hadConnection} status={status}/><NavigationTabs group={activeGroup} view={view} onChange={setView}/>{view==='today'?<TodayView onOpenConversation={id=>{setFocusedConversationId(id);setView('chat')}}/>:view==='inbox'?<InboxView initialItemId={focusedInboxItemId} onOpenConversation={id=>{setFocusedConversationId(id);setView('chat')}}/>:view==='search'?<UnifiedSearchView onOpenSource={openSearchSource}/>:view==='research'?<ResearchView/>:view==='proactive'?<ProactiveView/>:view==='external'?<ExternalOperations/>:view==='connector-health'?<ConnectorHealthView/>:view==='chat'?<Chat initialConversationId={focusedConversationId} onSelectConversation={setFocusedConversationId} requestedMode={requestedMode} onOpenRun={id=>{setFocusedRunId(id);setView('runs')}}/>:view==='runs'?<RunExplorer initialRunId={focusedRunId} onOpenConversation={id=>{setFocusedConversationId(id);setView('chat')}}/>:view==='tasks'?<TasksAndPlans initialTaskId={focusedTaskId}/>:view==='automations'?<Automations/>:view==='approvals'?<Approvals/>:view==='artifacts'?<Artifacts/>:view==='memory'?<Memory initialMemoryId={focusedMemoryId}/>:<PreferencesView/>}</div></main></div>

}



function NavigationTabs({group,view,onChange}:{group:NavGroup;view:View;onChange(value:View):void}){if(group.items.length<2)return null;return <div className="group-tabs" role="tablist" aria-label={`${group.label}页面`}><span className="group-tabs-title">{group.label}</span>{group.items.map(item=>{const count=group.id==='execution'?group.badgeCounts?.[item.view]||0:0;const countLabel=item.view==='approvals'?'待审批':item.view==='tasks'?'待确认计划':'运行中或待启动';return <button key={item.view} role="tab" aria-selected={view===item.view} aria-label={count>0?`${item.label}，${count} 项${countLabel}`:item.label} className={view===item.view?'active':''} onClick={()=>onChange(item.view)}><item.Icon size={14}/>{item.label}{count>0&&<span className="tab-badge" aria-hidden="true">{count>99?'99+':count}</span>}</button>})}</div>}



type ExternalOperation = { id: string; workflow_id?: string; tool_name?: string; status?: string; error?: string | null; updated_at?: string; started_at?: string; deleted_at?: string | null; preview?: { kind?: string; [key: string]: unknown }; verification?: { status?: string; message?: string; [key: string]: unknown } }
type WorkflowOperation = ExternalOperation & { arguments?: Record<string, unknown>; result?: unknown; recovery_ignored?: boolean; superseded?: boolean }
type WorkflowDetail = { workflow: { id: string; status?: string }; operations: WorkflowOperation[] }

function ExternalOperations(){
  const confirmAction=useConfirmAction()
  const queryClient=useQueryClient()
  const {pushToast}=useToast()
  const query=useQuery({queryKey:['external-operations'],queryFn:()=>rpc.call<{operations:ExternalOperation[]}>('external.operation.list',{limit:100}).then(x=>x.operations)})
  const [selected,setSelected]=useState<ExternalOperation|null>(null)
  const [retry,setRetry]=useState<WorkflowOperation|null>(null)
  const [retryDraft,setRetryDraft]=useState<Record<string,unknown>>({})
  const workflow=useQuery({queryKey:['external-workflow',selected?.workflow_id],enabled:Boolean(selected?.workflow_id),queryFn:()=>rpc.call<WorkflowDetail>('external.workflow.get',{workflow_id:selected?.workflow_id})})
  const clearRecoveryErrors=()=>{verify.reset();compensate.reset();continueMutation.reset();ignoreRecovery.reset()}
  const refreshWorkflow=()=>{clearRecoveryErrors();void query.refetch();if(selected?.workflow_id)void workflow.refetch()}
  const verify=useMutation({mutationFn:(operationId:string)=>rpc.call<{operation:ExternalOperation}>('external.operation.verify',{operation_id:operationId}),onSuccess:result=>{const status=result.operation.verification?.status;pushToast(status==='confirmed'||status==='confirmed_absent'?'已确认远端结果':'暂未确认远端结果','info');refreshWorkflow()}})
  const compensate=useMutation({mutationFn:(operationId:string)=>rpc.call<{compensation:{status:string;message?:string}}>('external.operation.compensate',{operation_id:operationId}),onSuccess:result=>{const status=result.compensation?.status;if(status==='succeeded'||status==='unsupported')pushToast(status==='succeeded'?'补偿操作已完成':result.compensation?.message||'暂不支持该动作的自动补偿','info');refreshWorkflow()}})
  const continueMutation=useMutation({mutationFn:(payload:{workflow_id:string;operation_id:string;arguments?:Record<string,unknown>;confirmed_not_executed?:boolean})=>rpc.call('external.workflow.continue',payload),onSuccess:()=>{setRetry(null);refreshWorkflow()}})
  const remove=useMutation({
    mutationFn:(operationId:string)=>rpc.call('external.operation.delete',{operation_id:operationId}),
    onSuccess:async(_,operationId)=>{
      queryClient.setQueryData<ExternalOperation[]>(['external-operations'],items=>items?.filter(item=>item.id!==operationId))
      if(selected?.id===operationId){setSelected(null);setRetry(null)}
      else if(retry?.id===operationId)setRetry(null)
      pushToast('外部操作记录已删除','success')
      await Promise.all([queryClient.invalidateQueries({queryKey:['external-operations']}),queryClient.invalidateQueries({queryKey:['external-workflow']})])
    }
  })
  const ignoreRecovery=useMutation({mutationFn:(step:WorkflowOperation)=>rpc.call('external.operation.ignore',{operation_id:step.id,ignored:!step.recovery_ignored}),onSuccess:refreshWorkflow})
  const requestContinue=(step:WorkflowOperation,confirmedAbsent=false)=>{void confirmAction({title:confirmedAbsent?'确认远端未执行并重试？':'确认重试此操作？',message:confirmedAbsent?'请先到远端确认该操作没有执行。仅搜索不到不能证明未执行；确认错误可能产生重复操作。':`请核对操作参数：${JSON.stringify(step.preview||{})}`,confirmLabel:'确认并执行'}).then(yes=>{if(yes&&selected?.workflow_id)continueMutation.mutate({workflow_id:selected.workflow_id,operation_id:step.id,confirmed_not_executed:confirmedAbsent})})}
  const operationBusy=remove.isPending||verify.isPending||compensate.isPending||continueMutation.isPending||ignoreRecovery.isPending
  const requestDelete=(item:ExternalOperation)=>{void confirmAction({title:'删除这条外部操作记录？',message:'记录将从列表移除，不会撤销已发邮件或修改飞书对象。为保证任务恢复安全，工作流详情和防止重复执行的凭据会保留。',confirmLabel:'删除记录',destructive:true}).then(yes=>{if(yes)remove.mutate(item.id)})}
  const refresh=()=>{void query.refetch();if(selected?.workflow_id)void workflow.refetch()}
  const openRetry=(step:WorkflowOperation)=>{clearRecoveryErrors();setRetry(step);setRetryDraft({...step.arguments})}
  const submitRetry=()=>{if(!selected?.workflow_id||!retry)return;continueMutation.mutate({workflow_id:selected.workflow_id,operation_id:retry.id,arguments:retryDraft})}
  const requestCompensation=(step:WorkflowOperation)=>{void confirmAction({title:'执行补偿操作？',message:'系统将调用对应的删除接口撤销已创建的飞书对象。只有在已知远端 ID 时才可用。',confirmLabel:'执行补偿',destructive:true}).then(yes=>{if(yes)compensate.mutate(step.id)})}
  return <Page title="外部操作" subtitle="查看外部写入结果，恢复失败的工作流。" actions={<button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14}/>刷新</button>}>
    <div className="stack">
      {query.isPending&&<Busy label="正在读取外部操作..."/>}{query.error&&<ErrorBox error={query.error} retry={refresh} title="外部操作加载失败"/>}
      {remove.error&&<ErrorBox error={remove.error} title="删除外部操作记录失败"/>}
      {verify.error&&<ErrorBox error={verify.error} title="远端核实失败"/>}
      {compensate.error&&<ErrorBox error={compensate.error} title="补偿操作失败"/>}
      {continueMutation.error&&<ErrorBox error={continueMutation.error} title="重试失败"/>}
      {ignoreRecovery.error&&<ErrorBox error={ignoreRecovery.error} title="更新处理状态失败"/>}
      {!query.isPending&&!query.error&&!query.data?.length&&<Empty icon={RefreshCw} title="暂无外部操作" hint="Agent 执行外部写入后，结果会显示在这里。"/>}
      {query.data?.map(item=><article className="card" key={item.id}>
        <div className="row-actions"><Status value={item.status||'unknown'}/><strong>{item.tool_name||'External action'}</strong><small>{item.updated_at||''}</small><button onClick={()=>{clearRecoveryErrors();setRetry(null);setSelected(item)}}>查看详情</button>{item.status==='uncertain'&&<button onClick={()=>verify.mutate(item.id)} disabled={operationBusy}>查询远端结果</button>}<button className="danger" disabled={operationBusy||!['succeeded','failed','denied','uncertain','partial_success'].includes(item.status||'')} title={['succeeded','failed','denied','uncertain','partial_success'].includes(item.status||'')?'删除这条列表记录':'请先处理审批或等待操作结束'} onClick={()=>requestDelete(item)}><Trash2 size={15}/>{remove.isPending&&remove.variables===item.id?'删除中…':'删除'}</button></div>
        {item.workflow_id&&<small>工作流：{item.workflow_id}</small>}
        {item.preview&&<details className="action-preview"><summary>动作预览</summary><div className="preview-box"><pre>{JSON.stringify(item.preview,null,2)}</pre></div></details>}
        {item.error&&<p className="form-error">{item.error}</p>}
        {item.status==='partial_success'&&<p className="form-warning">部分已成功。请只继续失败，避免重复写入。</p>}
        {item.status==='uncertain'&&<p className="form-warning">结果未知：请先查询远端状态，再重试。</p>}
        {item.verification?.message&&<small className="verification-note">核实：{item.verification.message}</small>}
      </article>)}
      {selected&&<section className="card external-detail"><div className="section-heading"><h2>工作流详情</h2><button onClick={()=>{setSelected(null);setRetry(null)}}>关闭</button></div>{workflow.isPending&&<Busy label="正在读取工作流..."/>}{workflow.error&&<ErrorBox error={workflow.error} retry={()=>void workflow.refetch()} title="工作流加载失败"/>}{workflow.data&&<><div className="row-actions"><Status value={workflow.data.workflow.status||'unknown'}/><span>{workflow.data.workflow.id}</span></div><ol className="timeline-list">{workflow.data.operations.map(step=><li key={step.id} className="timeline-row"><Status value={step.status||'unknown'}/><span><strong>{step.tool_name||''}</strong>{step.error&&<small>{step.error}</small>}{step.preview&&<details className="action-preview"><summary>预览</summary><div className="preview-box"><pre>{JSON.stringify(step.preview,null,2)}</pre></div></details>}{step.verification?.message&&<small className="verification-note">核实：{step.verification.message}</small>}</span><time>{step.updated_at||step.started_at||''}</time><div className="row-actions">{!step.superseded&&step.tool_name!=='connector_workflow'&&['failed','awaiting_approval','approved'].includes(step.status||'')&&<><button disabled={continueMutation.isPending} onClick={()=>requestContinue(step)}>{continueMutation.isPending?'继续中...':step.arguments?.compensation_for?'重试补偿':'重试此操作'}</button><button disabled={continueMutation.isPending} onClick={()=>openRetry(step)}>修改后重试</button></>}{!step.superseded&&step.tool_name!=='connector_workflow'&&step.status==='uncertain'&&<><button disabled={verify.isPending} onClick={()=>verify.mutate(step.id)}>查询远端结果</button><button disabled={continueMutation.isPending} onClick={()=>requestContinue(step,true)}>确认未执行并重试</button></>}{!step.superseded&&['failed','uncertain','partial_success'].includes(step.status||'')&&<button disabled={ignoreRecovery.isPending} onClick={()=>ignoreRecovery.mutate(step)}>{step.recovery_ignored?'恢复处理':'暂不处理'}</button>}{step.superseded&&<small>已有新的重试记录，此处保留历史结果。</small>}{step.recovery_ignored&&<small>已标记暂不处理，原始失败或未知状态仍保留。</small>}{['feishu_task_create','feishu_calendar_event_create','feishu_document_create'].includes(step.tool_name||'')&&['succeeded','partial_success'].includes(step.status||'')&&<button className="danger" disabled={compensate.isPending} onClick={()=>requestCompensation(step)}>撤销已创建对象</button>}</div></li>)}</ol>{retry&&<RetryEditor step={retry} draft={retryDraft} setDraft={setRetryDraft} submit={submitRetry} cancel={()=>setRetry(null)} pending={continueMutation.isPending}/>} {recoveryGuidance(workflow.data.operations).map(note=><p className="form-warning" key={note}>{note}</p>)}</>}</section>}
    </div>
  </Page>
}

const retryFieldLabels:Record<string,string>={to:'To',cc:'Cc',bcc:'Bcc',subject:'Subject',body:'Body',title:'Title',description:'Description',starts_at:'Start time',ends_at:'End time',location:'Location',due_at:'Due time',status:'Task status',task_id:'Task ID',event_id:'Event ID',document_id:'Document ID',content:'Content',block_id:'Block ID'}
function RetryEditor({step,draft,setDraft,submit,cancel,pending}:{step:WorkflowOperation;draft:Record<string,unknown>;setDraft:React.Dispatch<React.SetStateAction<Record<string,unknown>>>;submit():void;cancel():void;pending:boolean}){
  const fields=Object.keys(draft).filter(key=>!['idempotency_key','external_operation_id','compensation_for','_retry_of'].includes(key))
  const compensationTargetField=step.arguments?.compensation_for?({feishu_task_delete:'task_id',feishu_calendar_event_delete:'event_id',feishu_document_delete:'document_id'} as Record<string,string>)[step.tool_name||'']:undefined
  return <section className="external-retry"><div className="section-heading"><div><h3>修改后重试</h3><small>{step.tool_name||'外部'} - {compensationTargetField?'补偿目标固定为原创建对象，不能修改。':'修改参数后会生成新的幂等键'}</small></div><button onClick={cancel}>取消</button></div><div className="form-grid">{fields.map(field=>{const multiline=['body','content','description'].includes(field);return <label key={field}>{retryFieldLabels[field]||field}{typeof draft[field]==='object'&&draft[field]!==null?<textarea readOnly value={JSON.stringify(draft[field],null,2)} aria-label={`${retryFieldLabels[field]||field}（保留原值）`}/>:multiline?<textarea value={String(draft[field]??'')} onChange={event=>setDraft(current=>({...current,[field]:event.target.value}))}/>:<input readOnly={field===compensationTargetField} value={String(draft[field]??'')} onChange={event=>setDraft(current=>({...current,[field]:event.target.value}))}/>}</label>})}</div><div className="row-actions"><button className="primary" onClick={submit} disabled={pending}>{pending?'提交中...':'确认并重试'}</button></div></section>
}

const starterPrompts=['整理这个项目并给出可执行计划','检查当前实现的风险与缺口','完成一个任务并把结果发布为交付物']

function Chat({initialConversationId,onSelectConversation,requestedMode,onOpenRun}:{initialConversationId:string|null;onSelectConversation(id:string):void;requestedMode:'normal'|'plan'|null;onOpenRun(id:string):void}){

  const confirmAction=useConfirmAction()

  const { pushToast }=useToast()

  const qc=useQueryClient();const selected=initialConversationId||'';const setSelected=onSelectConversation;const [input,setInput]=useState('');const [mode,setMode]=useState<'normal'|'plan'>('normal');const [planSelection,setPlanSelection]=useState<{conversationId:string;taskId:string}|null>(null);const [drawer,setDrawer]=useState(false);const [renaming,setRenaming]=useState(false)

  const live=useSyncExternalStore(conversationActivity.subscribe,()=>conversationActivity.get(selected))

  const running=live?.status==='starting'||live?.status==='running'

  const [pinned,setPinned]=useState<string[]>(()=>JSON.parse(localStorage.getItem('koreagent:pinned-conversations')||'[]'))

  useEffect(()=>{if(requestedMode)setMode(requestedMode)},[requestedMode])

  useEffect(()=>{setRenaming(false)},[selected])

  const conversations=useQuery({queryKey:['conversations'],queryFn:()=>rpc.call<{conversations:Conversation[]}>('conversation.list',{limit:100}).then(x=>x.conversations)})

  const ordered=useMemo(()=>[...(conversations.data||[])].sort((a,b)=>Number(pinned.includes(b.id))-Number(pinned.includes(a.id))),[conversations.data,pinned])

  useEffect(()=>{if(!selected&&ordered[0])setSelected(ordered[0].id)},[ordered,selected,setSelected])

  const detail=useQuery({queryKey:['conversation',selected],enabled:!!selected,queryFn:()=>rpc.call<{conversation:Conversation;messages:Message[]}>('conversation.get',{conversation_id:selected})})

  const tasks=useQuery({queryKey:['tasks',selected],enabled:!!selected,queryFn:()=>rpc.call<{tasks:Task[]}>('task.list',{conversation_id:selected,limit:100}).then(x=>x.tasks)})

  const runs=useQuery({queryKey:['runs',selected],enabled:!!selected,queryFn:async()=>{

    const observed=conversationActivity.get(selected)

    const result=await rpc.call<{runs:Run[]}>('run.list',{conversation_id:selected,limit:20})

    const history=qc.getQueryData<{messages:Message[]}>(['conversation',selected])

    conversationActivity.reconcile(selected,result.runs,observed,history?.messages.length)

    if(observed&&!conversationActivity.get(selected))void qc.invalidateQueries({queryKey:['conversation',selected]})

    return result.runs

  },refetchOnMount:'always',refetchInterval:query=>conversationActivity.get(selected)||query.state.data?.some(item=>['running','pending'].includes(item.status))?2500:false})

  const artifacts=useQuery({queryKey:['artifacts',selected],enabled:!!selected,queryFn:()=>rpc.call<{artifacts:Artifact[]}>('artifact.list',{conversation_id:selected,limit:50}).then(x=>x.artifacts)})

  const create=useMutation({mutationFn:()=>rpc.call<{conversation:Conversation}>('conversation.create',{}),onSuccess:r=>{setSelected(r.conversation.id);setDrawer(false);qc.invalidateQueries({queryKey:['conversations']})}})

  const remove=useMutation({mutationFn:(id:string)=>rpc.call('conversation.delete',{conversation_id:id}),onSuccess:(_result,id)=>{conversationActivity.forget(id);qc.setQueryData<Conversation[]>(['conversations'],items=>items?.filter(item=>item.id!==id));setSelected('');qc.invalidateQueries({queryKey:['conversations']})}})

  const rename=useMutation({mutationFn:({id,title}:{id:string;title:string})=>rpc.call('conversation.rename',{conversation_id:id,title}),onSuccess:(_result,{id})=>{setRenaming(false);qc.invalidateQueries({queryKey:['conversation',id]});qc.invalidateQueries({queryKey:['conversations']})}})

  const approvals=useQuery({queryKey:['approvals','chat'],queryFn:()=>rpc.call<{approvals:Approval[]}>('approval.list',{status:'pending',limit:50}).then(x=>x.approvals),refetchInterval:15000})

  const send=useMutation({

    mutationFn:({id,text,mode:sendMode}:{id:string;text:string;mode:'normal'|'plan'})=>rpc.call<any>('conversation.send',{conversation_id:id,content:text,mode:sendMode}),

    onMutate:async({id,text})=>{

      // Show the user's message as soon as the request is accepted by the

      // client. The Host keeps the RPC open while the Agent works, so waiting

      // for onSuccess made the composer look stuck and hid what was sent.

      setInput('')

      await qc.cancelQueries({queryKey:['conversation',id]})

      qc.setQueryData<{conversation:Conversation;messages:Message[]}>(['conversation',id],current=>current?{

        ...current,

        messages:[...current.messages,{role:'user',content:text}],

      }:current)

      const history=qc.getQueryData<{messages:Message[]}>(['conversation',id])

      return conversationActivity.begin(id,history?.messages.length)

    },

    onSuccess:async (result,variables,request)=>{

      setPlanSelection(result.plan_task_id?{conversationId:variables.id,taskId:result.plan_task_id}:null)

      await Promise.all([qc.invalidateQueries({queryKey:['conversation',variables.id]}),qc.invalidateQueries({queryKey:['conversations']}),qc.invalidateQueries({queryKey:['tasks',variables.id]}),qc.invalidateQueries({queryKey:['artifacts',variables.id]})])

      if(request)conversationActivity.finish(request)

    },

    onError:(_error,variables,request)=>{

      // Reconcile the optimistic message with the durable conversation. This

      // also handles a connection failure after the Host may have persisted it.

      void qc.invalidateQueries({queryKey:['conversation',variables.id]})

      void qc.invalidateQueries({queryKey:['runs',variables.id]})

      if(request)conversationActivity.fail(request)

    },

  })

  const pendingApproval=approvals.data?.find(item=>item.conversation_id===selected||(!!live?.runId&&item.run_id===live.runId))

  const approval=useMutation({mutationFn:({id,yes,scope}:{id:string;yes:boolean;scope:PermissionScope})=>rpc.call(yes?'approval.approve':'approval.deny',{approval_id:id,scope}),onSuccess:()=>approvals.refetch()})

  const interrupt=useMutation({mutationFn:(id:string)=>rpc.call('run.interrupt',{run_id:id}),onSuccess:(_result,id)=>{send.reset();pushToast('运行已中断，检查点已保存','info');const conversationId=conversationActivity.status(id,'interrupted');if(conversationId){void qc.invalidateQueries({queryKey:['conversation',conversationId]});void qc.invalidateQueries({queryKey:['runs',conversationId]})}}})

  const recover=useMutation({mutationFn:({runId}:{runId:string;conversationId:string})=>rpc.call('run.recover',{run_id:runId}),onMutate:({conversationId})=>conversationActivity.begin(conversationId,qc.getQueryData<{messages:Message[]}>(['conversation',conversationId])?.messages.length),onSuccess:async(_result,{conversationId},request)=>{await Promise.all(['runs','conversation','tasks','artifacts'].map(key=>qc.invalidateQueries({queryKey:[key,conversationId]})));if(request)conversationActivity.finish(request)},onError:(_error,{conversationId},request)=>{if(request)conversationActivity.fail(request);void qc.invalidateQueries({queryKey:['runs',conversationId]})}})

  const pendingTaskId=tasks.data?.find(item=>item.status==='pending'&&item.owner_conversation_id===selected)?.id||null

  const displayedPlanTaskId=(planSelection?.conversationId===selected?planSelection.taskId:null)||pendingTaskId

  const plan=useQuery({queryKey:['task',selected,displayedPlanTaskId],enabled:Boolean(selected&&displayedPlanTaskId),queryFn:()=>rpc.call<{task:Task}>('task.get',{task_id:displayedPlanTaskId!}).then(x=>x.task)})

  const decidePlan=useMutation({

    mutationFn:({accepted,taskId}:{accepted:boolean;taskId:string;conversationId:string})=>rpc.call<{task:Task;run?:Run}>(accepted?'task.plan_accept':'task.plan_reject',{task_id:taskId}),

    onMutate:({accepted,conversationId})=>accepted?conversationActivity.begin(conversationId,qc.getQueryData<{messages:Message[]}>(['conversation',conversationId])?.messages.length):undefined,

    onSuccess:async(result,{conversationId,taskId},request)=>{

      qc.setQueryData(['task',conversationId,taskId],result.task)

      await Promise.all(['tasks','runs','conversation','artifacts'].map(key=>qc.invalidateQueries({queryKey:[key,conversationId]})))

      void qc.invalidateQueries({queryKey:['tasks','rail']})

      if(request)conversationActivity.finish(request)

    },

    onError:(error,{conversationId,taskId},request)=>{

      if(request)conversationActivity.fail(request)

      pushToast(`计划执行失败：${error instanceof Error?error.message:String(error)}`,'danger')

      void qc.invalidateQueries({queryKey:['task',conversationId,taskId]})

      void qc.invalidateQueries({queryKey:['tasks',conversationId]})

      void qc.invalidateQueries({queryKey:['runs',conversationId]})

    },

  })

  const decide=(accepted:boolean)=>{if(displayedPlanTaskId&&!conversationActivity.get(selected))decidePlan.mutate({accepted,taskId:displayedPlanTaskId,conversationId:selected})}

  const submit=()=>{const text=input.trim();if(selected&&text&&!conversationActivity.get(selected))send.mutate({id:selected,text,mode})}

  const pin=(id:string)=>{const next=pinned.includes(id)?pinned.filter(item=>item!==id):[...pinned,id];setPinned(next);localStorage.setItem('koreagent:pinned-conversations',JSON.stringify(next))}

  const currentRun=runs.data?.find(item=>['running','pending'].includes(item.status))||runs.data?.[0]

  const activeTask=tasks.data?.find(item=>['active','blocked','pending'].includes(item.status))||tasks.data?.[0]

  return <div className="chat-layout">
    {drawer&&<button className="drawer-scrim" aria-label="关闭会话列表" onClick={()=>setDrawer(false)}/>}
    <aside className={`conversation-list ${drawer?'open':''}`} aria-label="会话列表"><div className="conversation-title"><strong>会话</strong><button aria-label="新建会话" onClick={()=>create.mutate()}><Plus size={16}/></button></div><div className="conversation-scroll">{ordered.map(item=><div className={`conversation-row ${selected===item.id?'selected':''}`} key={item.id}><button onClick={()=>{setSelected(item.id);setDrawer(false)}}><span>{displayConversationTitle(item.title)}</span><small>{item.message_count} 条消息</small></button><button aria-label="置顶会话" onClick={()=>pin(item.id)}>{pinned.includes(item.id)?<PinOff size={13}/>:<Pin size={13}/>}</button></div>)}</div></aside>
    <section className="workspace">{!selected?<Empty icon={MessageSquare} title="开始新对话" hint="创建会话，然后告诉 KoreAgent Next 你想完成的结果。" action={<button className="primary" onClick={()=>create.mutate()}><Plus size={15}/>新建会话</button>}/>:<>
      <header className="workspace-head"><div>{renaming?<RenameForm key={selected} initial={detail.data?.conversation.title||''} pending={rename.isPending} save={title=>rename.mutate({id:selected,title})} cancel={()=>setRenaming(false)}/>:<h2>{detail.data?.conversation.title||'会话'}</h2>}<small>{send.isPending?'Agent 正在工作':`${tasks.data?.filter(item=>['active','blocked'].includes(item.status)).length||0} 个持续任务`}</small></div><div className="row-actions"><button className="icon" aria-label="重命名会话" disabled={!detail.data||rename.isPending} onClick={()=>{rename.reset();setRenaming(true)}}><Pencil size={15}/></button><button className="icon danger" aria-label="删除会话" onClick={()=>{void confirmAction({title:'删除这个会话？',message:'会话及其消息将被删除。',confirmLabel:'删除会话',destructive:true}).then(yes=>{if(yes)remove.mutate(selected)})}}><Trash2 size={16}/></button></div></header>
      {renaming&&rename.error&&rename.variables?.id===selected&&<ErrorBox error={rename.error} title="重命名失败"/>}
      <TaskPanel task={activeTask} run={currentRun} onOpenRun={onOpenRun} recovering={recover.isPending} onRecover={id=>recover.mutate({runId:id,conversationId:selected})}/>
      <div className="messages" aria-live="polite">{detail.isLoading?<Busy/>:detail.error?<ErrorBox error={detail.error} retry={()=>detail.refetch()} title="会话加载失败"/>:detail.data?.messages.length?<ConversationMessages messages={detail.data.messages.slice(0,live?.historyLength)}/>:<div className="starter-state"><Empty icon={Bot} title="空白会话" hint="选择一个起点，或直接输入你的目标。"/><div className="starter-prompts">{starterPrompts.map(prompt=><button key={prompt} onClick={()=>setInput(prompt)}>{prompt}<ChevronRight size={14}/></button>)}</div></div>}{live&&<LiveRun live={live}/>} {plan.data&&<PlanDecision task={plan.data} pending={decidePlan.isPending} decide={accepted=>decidePlan.mutate({accepted,taskId:plan.data!.id,conversationId:selected})}/>} {artifacts.data?.map(item=><ArtifactInline key={item.id} artifact={item}/>)}</div>
      {pendingApproval&&<InlineApproval approval={pendingApproval} pending={approval.isPending} decide={(yes,scope)=>approval.mutate({id:pendingApproval.id,yes,scope})}/>} {send.error&&!(send.error instanceof Error&&/request cancelled/i.test(send.error.message))&&<ErrorBox error={send.error} retry={()=>send.mutate(send.variables!)}/>}
      {live?.runId&&['running','starting'].includes(live.status)&&<div className="composer-head"><button className="composer-stop" disabled={interrupt.isPending} onClick={()=>{void confirmAction({title:'中断当前运行？',message:'当前执行将停止，已有进度会保存为检查点，可稍后恢复。',confirmLabel:'中断并保存'}).then(yes=>{if(yes)interrupt.mutate(live.runId!)})}}><PauseCircle size={15}/>中断并保存检查点</button></div>}
      <div className="composer"><textarea value={input} onChange={e=>setInput(e.target.value)} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();submit()}}} placeholder="输入消息；Enter 发送，Shift+Enter 换行" aria-label="消息内容"/><div className="composer-row"><select aria-label="运行模式" value={mode} onChange={e=>setMode(e.target.value as 'normal'|'plan')}><option value="normal">执行模式</option><option value="plan">计划模式</option></select><button className="primary" disabled={!input.trim()||running} onClick={submit}>{running?'运行中':'发送'}</button></div></div>
    </>}</section>
  </div>
}
function TaskPanel({task,run,onOpenRun,onRecover,recovering}:{task?:Task;run?:Run;onOpenRun(id:string):void;onRecover(id:string):void;recovering:boolean}){
  if(!task&&!run)return null
  const done=task?.steps.filter(step=>step.status==='completed').length||0
  const total=task?.steps.length||0
  const current=task?.steps.find(step=>['active','running','blocked'].includes(step.status))||task?.steps.find(step=>step.status==='pending')
  return <section className="chat-task-panel" aria-label="当前任务进度"><div className="chat-task-heading"><div><strong>{task?.title||'当前运行'}</strong><small>{task?`${done}/${total} 步已完成${current?` · 当前：${current.title}`:''}`:`状态：${statusChinese(run?.status||'未知')}`}</small></div><div className="row-actions">{run&&<button onClick={()=>onOpenRun(run.id)}>查看运行 <ChevronRight size={14}/></button>}{run&&['interrupted','failed','cancelled'].includes(run.status)&&<button disabled={recovering} onClick={()=>onRecover(run.id)}><RefreshCw size={14}/>恢复</button>}</div></div>{task&&total>0&&<><progress max={total} value={done} aria-label="任务完成进度"/><ol>{task.steps.map(step=><li key={step.id}><Status value={step.status}/><span>{step.title}</span></li>)}</ol></>}</section>
}
function RenameForm({initial,save,cancel,pending}:{initial:string;save(value:string):void;cancel():void;pending:boolean}){const [value,setValue]=useState(initial);return <form className="rename-form" onSubmit={e=>{e.preventDefault();if(value.trim()&&!pending)save(value.trim())}} onKeyDown={e=>{if(e.key==='Escape'&&!pending){e.preventDefault();cancel()}}}><input autoFocus value={value} onChange={e=>setValue(e.target.value)} aria-label="会话标题" maxLength={120} disabled={pending}/><button className="primary" type="submit" disabled={pending||!value.trim()}>{pending?'保存中…':'保存'}</button><button type="button" disabled={pending} onClick={cancel}>取消</button></form>}

function ConversationMessages({messages}:{messages:Message[]}){
  const rendered:JSX.Element[]=[]
  let index=0
  while(index<messages.length){
    const message=messages[index]
    // The durable history stores one assistant message for every tool round,
    // followed by one assistant message containing the final answer. Keep
    // that whole contiguous run in one visual bubble after completion.
    if(message.role==='assistant'&&message.tool_calls?.length){
      const group:Message[]=[message]
      index+=1
      while(index<messages.length){
        const next=messages[index]
        if(next.role==='tool'||(next.role==='assistant'&&next.tool_calls?.length)){
          group.push(next)
          index+=1
          continue
        }
        if(next.role==='assistant'){
          group.push(next)
          index+=1
        }
        break
      }
      rendered.push(<AgentRunBubble key={`agent-run-${index}`} messages={group}/>)
      continue
    }
    rendered.push(<MessageBubble key={`message-${index}`} message={message}/>)
    index+=1
  }
  return <>{rendered}</>
}

function AgentRunBubble({messages}:{messages:Message[]}){
  const assistants=messages.filter(message=>message.role==='assistant')
  const toolMessages=messages.filter(message=>message.role==='tool')
  const toolCalls=assistants.flatMap(message=>message.tool_calls||[])
  const finalMessage=[...assistants].reverse().find(message=>!message.tool_calls?.length)
  const reasoning=assistants.map(message=>message.reasoning).filter(Boolean).join('\n\n')
  return <article className="message assistant completed-run-message"><div className="avatar">K</div><div className="message-body">
    <details className="completed-run-process">
      <summary><span>执行过程</span><small>{toolCalls.length?`${toolCalls.length} 次工具调用`:'模型思考'}</small></summary>
      <div className="completed-run-process-body">
        {reasoning&&<section><strong>思考</strong><pre>{reasoning}</pre></section>}
        {!!toolCalls.length&&<section><strong>工具调用</strong><div className="completed-run-tool-calls">{toolCalls.map((call,index)=><div key={`${call.id||call.name}-${index}`}><span>{toolLabel(call.name)}</span><small>已调用</small></div>)}</div></section>}
        {!!toolMessages.length&&<section><strong>工具结果</strong><div className="completed-run-tool-results">{toolMessages.map((message,index)=><details className="tool-preview" key={`${message.tool_call_id||message.name||'tool'}-${index}`}><summary><span>{toolLabel(message.name||'tool')}</span><small>点击展开</small></summary><pre className="tool-output-preview">{message.content||'（无返回内容）'}</pre></details>)}</div></section>}
      </div>
    </details>
    {finalMessage?.content?<div className="completed-run-final"><ReactMarkdown remarkPlugins={[remarkGfm]}>{finalMessage.content}</ReactMarkdown></div>:!toolMessages.length&&<ReactMarkdown remarkPlugins={[remarkGfm]}>{assistants.map(message=>message.content).filter(Boolean).join('\n\n')}</ReactMarkdown>}
  </div></article>
}

function MessageBubble({message}:{message:Message}){const isTool=message.role==='tool';return <article className={`message ${message.role}`}><div className="avatar">{message.role==='user'?'我':message.role==='assistant'?'K':'T'}</div><div className="message-body">{message.name&&<small>{message.name}</small>}{message.reasoning&&<details><summary>思考过程</summary><pre>{message.reasoning}</pre></details>}{isTool?<details className="tool-preview"><summary><span>工具结果 · {toolLabel(message.name||'tool')}</span><small>点击展开查看完整内容</small></summary><pre className="tool-output-preview">{message.content||'（无返回内容）'}</pre></details>:<ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content||''}</ReactMarkdown>}</div></article>}

function toolLabel(name:string):string { const labels:Record<string,string>={read_file:'读取文件',list_files:'列出文件',write_file:'写入文件',shell:'运行命令',web_search:'网页搜索',web_research:'网页研究',http_request:'访问网页',history_search:'搜索历史',history_read:'读取历史',unified_search:'统一搜索',email_send:'发送邮件',feishu_calendar_event_create:'创建日历事件',feishu_task_create:'创建飞书任务',feishu_document_create:'创建飞书文档',feishu_document_delete:'删除飞书文档',connector_workflow:'执行连接器流程'}; return labels[name]||name }

function ToolActivity预览({tools}:{tools:LiveRunState['toolActivity']}){
  const running=tools.filter(tool=>tool.status==='running'); const latest=tools.at(-1); const headline=running.length?`正在${toolLabel(running.at(-1)?.name||'工具')}`:`已完成 ${tools.length} 次工具调用`;
  return <details className="tool-preview"><summary><span>工具调用 · {tools.length} 次</span><small>{headline}</small></summary><div className="tool-activity" aria-label="工具调用详情">{tools.map(tool=><div key={tool.id}><Status value={tool.status}/><span>{toolLabel(tool.name)}</span>{tool.summary&&<small title={tool.summary}>{tool.summary}</small>}</div>)}{latest?.status==='failed'&&<p className="tool-preview-note">工具调用失败，展开运行记录查看完整错误和恢复方式。</p>}</div></details>
}
  function LiveRun({live}:{live:LiveRunState}){const placeholder=liveRunPlaceholder(live);return <article className="message assistant live-message"><div className="avatar">K</div><div className="message-body">{live.reasoning&&<details><summary>正在推理</summary><pre>{live.reasoning}</pre></details>}{live.content?<ReactMarkdown remarkPlugins={[remarkGfm]}>{live.content}</ReactMarkdown>:placeholder&&<div className={live.status==='running'||live.status==='starting'?'thinking':'live-status'}>{(live.status==='running'||live.status==='starting')&&<><span/><span/><span/></>} {placeholder}</div>}{live.budgetStatus&&<div className="budget-notice">运行预算：{live.budgetStatus}</div>}{live.toolActivity.length>0&&<ToolActivity预览 tools={live.toolActivity}/>}</div></article>}

function ArtifactInline({artifact}:{artifact:Artifact}){return <article className="artifact-inline"><FileOutput size={18}/><div><strong>{artifact.title}</strong><small>{artifact.description||artifact.filename||'Agent 交付物'}</small></div>{artifact.kind==='file'?<a className="button" href={artifactContentUrl(artifact.id)}>下载</a>:<button onClick={()=>artifact.source_url&&openUrl(artifact.source_url)}>打开</button>}</article>}

function InlineApproval({approval,pending,decide}:{approval:Approval;pending:boolean;decide(yes:boolean,scope:PermissionScope):void}){const [scope,setScope]=useState<PermissionScope>('once');return <section className="inline-approval" role="alert"><div><strong>等待允许：{approvalTitle(approval)}</strong><p>{approval.reason}</p>{approval.preview&&<details className="action-preview" open><summary>查看动作预览</summary><div className="preview-box"><pre>{JSON.stringify(approval.preview,null,2)}</pre></div></details>}<small>授权只适用于工具和完整参数都相同的操作。</small></div><div><select value={scope} onChange={e=>setScope(e.target.value as PermissionScope)} aria-label="允许范围">{(approval.available_scopes||['once']).map(item=><option value={item} key={item}>{permissionScopeLabel(item)}</option>)}</select><button disabled={pending} onClick={()=>decide(false,'once')}>拒绝</button><button className="primary" disabled={pending} onClick={()=>decide(true,scope)}>允许</button></div></section>}

function PlanDecision({task,pending,decide}:{task:Task;pending:boolean;decide(accepted:boolean):void}){const executable=['pending','active','blocked'].includes(task.status);const subtitle=task.status==='pending'?'请确认后开始执行':task.status==='active'?'计划已接受，可以继续执行':task.status==='blocked'?'上次执行失败，可以重试':'计划状态';return <section className="plan-decision"><header><div><strong>{task.title}</strong><small>{subtitle}</small></div><Status value={task.status}/></header><ol>{task.steps.map((step,index)=><li key={step.id}><div><strong>{index+1}. {step.title}</strong>{step.description&&<small>{step.description}</small>}</div></li>)}</ol>{executable&&<footer>{task.status==='pending'&&<button disabled={pending} onClick={()=>decide(false)}>退回计划</button>}<button className="primary" disabled={pending} onClick={()=>decide(true)}>{pending?'执行中':task.status==='active'?'继续执行':task.status==='blocked'?'重试执行':'接受并执行'}</button></footer>}</section>}





function Automations(){
  const confirmAction=useConfirmAction()
  const queryClient=useQueryClient()
  const query=useQuery({queryKey:['automations'],queryFn:()=>rpc.call<{automations:Automation[]}>('automation.list',{}).then(x=>x.automations),refetchInterval:30000})
  const [open,setOpen]=useState(false)
  const action=useMutation({
    mutationFn:({method,id}:{method:string;id:string})=>rpc.call(method,{automation_id:id}),
    onSuccess:async(_,variables)=>{
      if(variables.method==='automation.delete'){
        queryClient.setQueryData<Automation[]>(['automations'],items=>items?.filter(item=>item.id!==variables.id))
      }
      await Promise.all([query.refetch(),queryClient.invalidateQueries({queryKey:['goal-detail']})])
    }
  })
  const refresh=()=>{void query.refetch()}
  const confirmRemoval=async(item:Automation,canDelete:boolean)=>{
    const yes=await confirmAction(canDelete?{
      title:`删除“${item.title}”？`,
      message:'将删除这条自动化记录。已经启动的任务、会话和历史运行记录会保留。',
      confirmLabel:'删除自动化',
      destructive:true
    }:{
      title:`取消“${item.title}”？`,
      message:'这项自动化将停止后续触发，已经创建的运行不受影响。',
      confirmLabel:'取消自动化',
      destructive:true
    })
    if(yes)action.mutate({method:canDelete?'automation.delete':'automation.cancel',id:item.id})
  }
  return <Page title="自动化" subtitle="按时区定时或周期运行 Agent 工作。" actions={<div className="row-actions"><button className="primary" onClick={()=>setOpen(true)}><Plus size={15}/>新建</button><button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14}/>刷新</button></div>}>
    {open&&<AutomationForm close={()=>setOpen(false)} done={()=>{setOpen(false);void query.refetch()}}/>}
    {query.isPending&&<Busy label="正在读取自动化…"/>}
    {query.error&&<ErrorBox error={query.error} retry={refresh} title="自动化加载失败"/>}
    {action.error&&<ErrorBox error={action.error} title="自动化操作失败"/>}
    {!query.isPending&&!query.error&&!query.data?.length&&<Empty icon={Clock3} title="暂无自动化" hint="创建一次性、间隔或 Cron 工作。"/>}
    <div className="card-grid">{query.data?.map(item=>{
      const canDelete=item.status==='completed'||item.status==='cancelled'
      const canCancel=item.status==='active'||item.status==='paused'
      const deleting=action.isPending&&action.variables?.method==='automation.delete'&&action.variables.id===item.id
      return <article className="card" key={item.id}>
        <div className="card-head"><Clock3/><Status value={item.status}/></div><h3>{item.title}</h3><p>{item.prompt}</p>
        <dl><dt>计划</dt><dd>{scheduleLabel(item)}</dd><dt>时区</dt><dd>{item.schedule.timezone}</dd><dt>下次运行</dt><dd>{item.next_run_at?new Date(item.next_run_at).toLocaleString():'—'}</dd><dt>上次运行</dt><dd>{item.last_run_at?new Date(item.last_run_at).toLocaleString():'尚未运行'}</dd></dl>
        <div className="card-actions">
          {item.status==='active'?<button disabled={action.isPending} onClick={()=>action.mutate({method:'automation.pause',id:item.id})}>暂停</button>:item.status==='paused'?<button disabled={action.isPending} onClick={()=>action.mutate({method:'automation.resume',id:item.id})}>恢复</button>:null}
          {(canDelete||canCancel)&&<button className="danger" disabled={action.isPending} onClick={()=>{void confirmRemoval(item,canDelete)}}>{canDelete?<><Trash2 size={15}/>{deleting?'删除中…':'删除'}</>:'取消'}</button>}
        </div>
      </article>
    })}</div>
  </Page>
}

function scheduleLabel(item:Automation){if(item.schedule.kind==='once')return`一次 · ${item.schedule.run_at?new Date(item.schedule.run_at).toLocaleString():'未设置'}`;if(item.schedule.kind==='interval')return`每 ${item.schedule.interval_seconds} 秒`;return`Cron ${item.schedule.cron_expr}`}

function AutomationForm({close,done}:{close():void;done():void}){const [form,setForm]=useState({title:'',prompt:'',kind:'once',run_at:'',interval_seconds:'3600',cron_expr:'0 9 * * *',timezone:Intl.DateTimeFormat().resolvedOptions().timeZone||'Asia/Shanghai'});const title=useRef<HTMLInputElement>(null);useEffect(()=>{title.current?.focus();const esc=(event:KeyboardEvent)=>{if(event.key==='Escape')close()};window.addEventListener('keydown',esc);return()=>window.removeEventListener('keydown',esc)},[close]);const mutation=useMutation({mutationFn:()=>rpc.call('automation.create',{...form,run_at:form.run_at?new Date(form.run_at).toISOString():undefined,interval_seconds:Number(form.interval_seconds)||undefined}),onSuccess:done});return <div className="modal-back" onMouseDown={event=>{if(event.target===event.currentTarget)close()}}><form className="modal" role="dialog" aria-modal="true" aria-labelledby="automation-title" onSubmit={event=>{event.preventDefault();mutation.mutate()}}><h2 id="automation-title">新建自动化</h2><label>标题<input ref={title} value={form.title} onChange={e=>setForm({...form,title:e.target.value})} required/></label><label>任务<textarea value={form.prompt} onChange={e=>setForm({...form,prompt:e.target.value})} required/></label><label>类型<select value={form.kind} onChange={e=>setForm({...form,kind:e.target.value})}><option value="once">一次</option><option value="interval">间隔</option><option value="cron">Cron</option></select></label>{form.kind==='once'&&<label>运行时间<input type="datetime-local" value={form.run_at} onChange={e=>setForm({...form,run_at:e.target.value})} required/></label>}{form.kind==='interval'&&<label>间隔秒数<input type="number" min="1" value={form.interval_seconds} onChange={e=>setForm({...form,interval_seconds:e.target.value})} required/></label>}{form.kind==='cron'&&<label>Cron 表达式<input value={form.cron_expr} onChange={e=>setForm({...form,cron_expr:e.target.value})} required/></label>}<label>IANA 时区<input value={form.timezone} onChange={e=>setForm({...form,timezone:e.target.value})} required/><small>例如 Asia/Shanghai</small></label><div className="modal-actions"><button type="button" onClick={close}>取消</button><button className="primary" type="submit" disabled={mutation.isPending}>{mutation.isPending?'创建中':'创建'}</button></div>{mutation.error&&<ErrorBox error={mutation.error}/>}</form></div>}



function Approvals(){
  const confirmAction=useConfirmAction()
  const queryClient=useQueryClient()
  const {pushToast}=useToast()
  const query=useQuery({queryKey:['approvals'],queryFn:()=>rpc.call<{approvals:Approval[]}>('approval.list',{limit:100}).then(x=>x.approvals),refetchInterval:15000})
  const [scopes,setScopes]=useState<Record<string,PermissionScope>>({})
  const action=useMutation({mutationFn:({id,yes,scope}:{id:string;yes:boolean;scope:PermissionScope})=>rpc.call(yes?'approval.approve':'approval.deny',{approval_id:id,scope}),onSuccess:()=>query.refetch()})
  const remove=useMutation({
    mutationFn:(approvalId:string)=>rpc.call('approval.delete',{approval_id:approvalId}),
    onSuccess:async(_,approvalId)=>{
      queryClient.setQueryData<Approval[]>(['approvals'],items=>items?.filter(item=>item.id!==approvalId))
      pushToast('审批记录已删除','success')
      await queryClient.invalidateQueries({queryKey:['approvals']})
    }
  })
  const busy=action.isPending||remove.isPending
  const refresh=()=>{void query.refetch()}
  const requestDelete=(item:Approval)=>{void confirmAction({title:'删除这条审批记录？',message:'记录将从审批列表移除。已经作出的审批决定、相关任务和已保存的权限规则会保留。',confirmLabel:'删除记录',destructive:true}).then(yes=>{if(yes)remove.mutate(item.id)})}
  return <Page title="审批" subtitle="用可读信息审查敏感操作，并选择权限持续范围。" actions={<button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14}/>刷新</button>}>
    <div className="stack">
      {query.isPending&&<Busy label="正在读取审批请求…"/>}
      {query.error&&<ErrorBox error={query.error} retry={refresh} title="审批请求加载失败"/>}
      {action.error&&<ErrorBox error={action.error} title="审批操作失败"/>}
      {remove.error&&<ErrorBox error={remove.error} title="删除审批记录失败"/>}
      {!query.isPending&&!query.error&&!query.data?.length&&<Empty icon={ShieldCheck} title="没有审批请求" hint="需要确认的工具操作会出现在这里。"/>}
      {query.data?.map(item=><article className="approval" key={item.id}>
        <div><div className="approval-heading"><Status value={item.status}/><code>{item.tool_name}</code></div><h3>{approvalTitle(item)}</h3><p>{item.reason}</p><small>Run {item.run_id?.slice(0,8)||'—'} · {new Date(item.created_at).toLocaleString()}</small><div className="approval-preview"><strong>操作预览</strong>{item.preview?<pre style={{whiteSpace:'pre-wrap',overflowWrap:'anywhere',maxHeight:320,overflow:'auto'}}>{JSON.stringify(item.preview,null,2)}</pre>:<p>当前没有完整预览，请先核实操作内容。</p>}</div><dl className="approval-facts">{approvalFacts(item.arguments).map(([key,value])=><div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}</dl></div>
        {item.status==='cancelled'&&<p className="form-warning">此审批已失效，不会执行；如仍需操作，请重新发起任务。</p>}
        <div className="approval-actions">
          {item.status==='pending'&&<><select value={scopes[item.id]||'once'} onChange={e=>setScopes({...scopes,[item.id]:e.target.value as PermissionScope})} aria-label="允许范围">{(item.available_scopes||['once','run','conversation','global']).map(scope=><option value={scope} key={scope}>{permissionScopeLabel(scope)}</option>)}</select><button className="danger" disabled={busy} onClick={()=>action.mutate({id:item.id,yes:false,scope:'once'})}><XCircle size={15}/>拒绝</button><button className="primary" disabled={busy} onClick={()=>action.mutate({id:item.id,yes:true,scope:scopes[item.id]||'once'})}><CheckCircle2 size={15}/>允许</button></>}
          <button className="danger" disabled={busy||!['approved','denied','cancelled'].includes(item.status)} title={item.status==='pending'?'请先允许或拒绝，再删除记录':'删除这条审批记录'} onClick={()=>requestDelete(item)}><Trash2 size={15}/>{remove.isPending&&remove.variables===item.id?'删除中…':'删除'}</button>
        </div>
      </article>)}
    </div>
  </Page>
}



function Artifacts(){const query=useQuery({queryKey:['artifacts'],queryFn:()=>rpc.call<{artifacts:Artifact[]}>('artifact.list',{limit:100}).then(x=>x.artifacts)});const refresh=()=>{void query.refetch()};return <Page title="交付物" subtitle="查看 Agent 发布的文件和链接。" actions={<button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14}/>刷新</button>}>{query.isPending?<Busy label="正在读取交付物…"/>:query.error?<ErrorBox error={query.error} retry={refresh} title="交付物加载失败"/>:!query.data?.length?<div className="artifact-empty"><Empty icon={FileOutput} title="暂无交付物" hint="Agent 发布的文件和链接会显示在这里。"/></div>:<div className="card-grid">{query.data.map(item=><article className="card" key={item.id}><FileOutput/><h3>{item.title}</h3><p>{item.description||item.filename||item.source_url}</p><small>{item.kind==='file'?formatBytes(item.size_bytes):'外部链接'} · {new Date(item.created_at).toLocaleString()}</small>{item.kind==='url'&&item.source_url?<button className="primary" onClick={()=>openUrl(item.source_url!)}><ExternalLink size={14}/>打开链接</button>:<a className="button primary" href={artifactContentUrl(item.id)}>下载文件</a>}</article>)}</div>}</Page>}

function openUrl(url:string){if(window.koreNext?.openExternal)window.koreNext.openExternal(url);else window.open(url,'_blank','noopener,noreferrer')}

function formatBytes(value:number){if(value<1024)return`${value} B`;if(value<1048576)return`${(value/1024).toFixed(1)} KB`;return`${(value/1048576).toFixed(1)} MB`}



function TasksAndPlans({initialTaskId}:{initialTaskId?:string|null}){
  const queryClient=useQueryClient();
  const query=useQuery({queryKey:['tasks','execution-center'],queryFn:()=>rpc.call<{tasks:Task[]}>('task.list',{limit:200}).then(x=>x.tasks)});
  const [expanded,setExpanded]=useState<string|null>(null);
  const [selectedStatus,setSelectedStatus]=useState<Task['status']>('pending');
  const decision=useMutation({mutationFn:({taskId,accepted}:{taskId:string;accepted:boolean})=>rpc.call(accepted?'task.plan_accept':'task.plan_reject',{task_id:taskId}),onSuccess:()=>{void query.refetch();void queryClient.invalidateQueries({queryKey:['tasks','rail']})}});
  const completeStep=useMutation({mutationFn:({taskId,stepId}:{taskId:string;stepId:string})=>rpc.call('task.step_complete',{task_id:taskId,step_id:stepId}),onSuccess:()=>{void query.refetch();void queryClient.invalidateQueries({queryKey:['tasks','rail']})}});
  const refresh=()=>{void query.refetch()};
  const tasks=query.data||[];
  useEffect(()=>{if(initialTaskId&&tasks.some(task=>task.id===initialTaskId))setExpanded(initialTaskId)},[initialTaskId,tasks.length])
  const groups:Array<{status:Task['status'];title:string;hint:string}>=[
    {status:'pending',title:'待确认计划',hint:'计划需要你接受后才会执行。'},
    {status:'active',title:'进行中任务',hint:'正在执行或等待继续处理的任务。'},
    {status:'blocked',title:'已阻塞任务',hint:'需要解决问题后才能继续的任务。'},
    {status:'completed',title:'已完成任务',hint:'已经完成的计划和任务。'},
    {status:'cancelled',title:'已取消任务',hint:'被拒绝或取消的计划。'},
  ];
  if(query.isPending)return <Page title="任务与计划" subtitle="管理 Agent 保存的计划、步骤和执行进度。"><LoadingState label="正在读取任务与计划…"/></Page>;
  if(query.error)return <Page title="任务与计划" actions={<button onClick={refresh}>重试</button>}><ErrorState error={query.error} retry={refresh}/></Page>;
  const selectedGroup=groups.find(group=>group.status===selectedStatus)||groups[0];
  const selectedItems=tasks.filter(task=>task.status===selectedGroup.status);
  return <Page title="任务与计划" subtitle="管理 Agent 保存的计划、步骤和执行进度。" actions={<button onClick={refresh} disabled={query.isFetching}><RefreshCw size={14}/>刷新</button>}>
    <div className="task-plan-page">
      <div className="task-plan-tabs" role="tablist" aria-label="任务状态">
        {groups.map(group=><button key={group.status} role="tab" aria-selected={selectedStatus===group.status} className={selectedStatus===group.status?'active':''} onClick={()=>{setSelectedStatus(group.status);setExpanded(null)}}>{group.title} <span>{tasks.filter(task=>task.status===group.status).length}</span></button>)}
      </div>
      <section className="card task-plan-section">
      <div className="section-heading"><div><h2>{selectedGroup.title}</h2><small>{selectedGroup.hint}</small></div><Status value={selectedGroup.status} label={`${selectedItems.length} 项`}/></div>
      {!selectedItems.length?<Empty title={`暂无${selectedGroup.title}`} hint="符合此状态的内容会显示在这里。"/>:<div className="stack">{selectedItems.map(task=><article className="memory-card task-plan-card" key={task.id}>
        <div className="task-plan-card-head"><div><Status value={task.status}/><h3>{task.title}</h3><p>{task.description||task.goal}</p></div><button onClick={()=>setExpanded(expanded===task.id?null:task.id)}>{expanded===task.id?'收起':'查看详情'}</button></div>
        {expanded===task.id&&<div className="task-plan-details"><p><strong>目标：</strong>{task.goal}</p>{task.goal_id&&<p><strong>关联目标：</strong>{task.goal_id}</p>}{task.steps?.length?<ol>{task.steps.map(step=><li key={step.id}><Status value={step.status}/><span>{step.title}</span>{step.description&&<small>{step.description}</small>}{step.status!=='completed'&&step.status!=='skipped'&&<button onClick={()=>completeStep.mutate({taskId:task.id,stepId:step.id})} disabled={completeStep.isPending}>完成步骤</button>}</li>)}</ol>:<p className="muted">暂无步骤</p>}</div>}
        {selectedGroup.status==='pending'&&<div className="row-actions"><button onClick={()=>decision.mutate({taskId:task.id,accepted:false})} disabled={decision.isPending}>拒绝计划</button><button className="primary" onClick={()=>decision.mutate({taskId:task.id,accepted:true})} disabled={decision.isPending}>接受并执行</button></div>}
      </article>)}</div>}
      </section>
    </div>{decision.error&&<p className="form-error" role="alert">计划处理失败：{String(decision.error)}</p>}</Page>
}

function decayReasonLabel(reason?:string){return ({long_term_unused:'长期未使用',recently_reused:'最近被复用',became_stale:'已进入衰减状态',recovered_from_stale:'复用后恢复活跃',periodic_decay_check:'定期检查'} as Record<string,string>)[reason||'']||reason||'首次计算'}
function formatDecayDelta(value?:number|null){if(value==null)return '首次计算';const percent=Math.round(value*100);return `${percent>0?'+':''}${percent}%`}
function memoryUsageLabel(state?:string){return ({current:'当前事实',historical:'历史事实',qualified:'有使用条件',disputed:'存在争议',awaiting_confirmation:'等待确认',rejected:'已拒绝',assessment_pending:'判断未完成',not_adopted:'本次信息未采用'} as Record<string,string>)[state||'']||'历史记录'}
function memoryRelationLabel(relation?:string){return ({duplicate:'重复支持',supplement:'补充内容',replacement:'明确更新',keep_old:'保留原事实',coexist:'分别保留',unresolved:'无法确定',incomplete:'判断未完成'} as Record<string,string>)[relation||'']||relation||'无关联裁决'}
function canReassessMemory(item:MemoryItem){return ['active','stale','pending_review'].includes(item.status)&&!item.user_resolution&&!item.conflicts?.length&&item.acceptance_source!=='rejected'&&(item.adjudication?.state==='incomplete'||item.status==='pending_review')}
function memoryNeedsReplacementScope(item:MemoryItem){
  if(item.conflicts?.length)return true;
  const qualified=['plan','hypothesis','reported'].includes(item.fact?.kind||'');
  if(item.fact_state==='not_adopted'&&item.retained_ids?.length&&!qualified)return true;
  return item.status==='pending_review'&&Boolean(item.adjudication?.decisions?.some(decision=>decision.relation==='unresolved'||(decision.relation==='keep_old'&&!qualified)));
}
type MemoryReplacementPreview={chosen:Pick<MemoryItem,'id'|'title'|'content'|'revision'|'status'|'fact'>;other:Pick<MemoryItem,'id'|'title'|'content'|'revision'|'status'|'fact'>;review_token:string};
function MemoryReplacementDialog({memoryId,confirm,close,onChanged}:{memoryId:string;confirm:boolean;close():void;onChanged():void}){
  const preview=useQuery({queryKey:['memory-replacement-preview',memoryId,confirm],queryFn:()=>rpc.call<MemoryReplacementPreview>('memory.conflict.preview',{memory_id:memoryId,confirm}),staleTime:0,refetchOnWindowFocus:false});
  const [mode,setMode]=useState<'partial'|'whole'>('partial');
  const [fragments,setFragments]=useState<string[]>(['']);
  const [checked,setChecked]=useState(false);
  const decision=useMutation({mutationFn:()=>{
    if(!preview.data||!checked)throw new Error('请先核对替换范围');
    return rpc.call(confirm?'memory.confirm':'memory.conflict.resolve',{
      memory_id:memoryId,...(!confirm?{action:'accept_new'}:{}),
      replacement_scope:{chosen_id:preview.data.chosen.id,target_id:preview.data.other.id,review_token:preview.data.review_token,confirmed:true,
        replace_entire_record:mode==='whole',superseded_fragments:mode==='partial'?fragments:[]},
    });
  },onSuccess:()=>{onChanged();close()}});
  const refresh=()=>{setChecked(false);setFragments(['']);decision.reset();void preview.refetch()};
  const changeMode=(value:'partial'|'whole')=>{setMode(value);setChecked(false);decision.reset()};
  const changeFragments=(value:string[])=>{setFragments(value);setChecked(false);decision.reset()};
  return <div className="modal-back" onMouseDown={event=>{if(event.target===event.currentTarget&&!decision.isPending)close()}}>
    <section className="modal memory-detail" role="dialog" aria-modal="true" aria-labelledby="memory-replacement-title">
      <h2 id="memory-replacement-title">确认记忆替换范围</h2>
      {preview.isFetching?<LoadingState label="正在读取两条记忆…"/>:preview.error?<ErrorState error={preview.error} retry={refresh}/>:preview.data&&<div className="memory-detail-content">
        <section><h3>准备采用：{preview.data.chosen.title} · v{preview.data.chosen.revision}</h3><p className="memory-detail-full-content">{preview.data.chosen.content||'暂无正文'}</p></section>
        <section className="memory-detail-section"><h3>待替换：{preview.data.other.title} · v{preview.data.other.revision}</h3><p className="memory-detail-full-content">{preview.data.other.content||'暂无正文'}</p></section>
        <fieldset disabled={decision.isPending} style={{border:0,padding:0,marginTop:16}}>
          <legend>选择替换范围</legend>
          <label style={{flexDirection:'row',alignItems:'center'}}><input type="radio" name="memory-replacement-mode" checked={mode==='partial'} onChange={()=>changeMode('partial')}/>只替换指定事实，保留其他内容</label>
          <label style={{flexDirection:'row',alignItems:'center'}}><input type="radio" name="memory-replacement-mode" checked={mode==='whole'} onChange={()=>changeMode('whole')}/>替换整条旧记忆，旧正文全部转为历史</label>
          {mode==='partial'?<>
            <p className="muted">从待替换正文复制完整的独立句或分句，每框一段（最多 8 段）。不要只填写某个名称或数值；未选中的事实将继续保留。</p>
            {fragments.map((fragment,index)=><div key={index}><label>旧事实片段 {index+1}<textarea value={fragment} onChange={event=>changeFragments(fragments.map((value,i)=>i===index?event.target.value:value))}/></label>{fragments.length>1&&<button type="button" onClick={()=>changeFragments(fragments.filter((_,i)=>i!==index))}>移除此片段</button>}</div>)}
            {fragments.length<8&&<button type="button" onClick={()=>changeFragments([...fragments,''])}>增加片段</button>}
          </>:<p className="form-warning">旧记忆中的所有事实都将退出当前事实列表；历史正文和版本仍会保留。</p>}
          <label style={{flexDirection:'row',alignItems:'flex-start'}}><input type="checkbox" checked={checked} onChange={event=>setChecked(event.target.checked)}/>{mode==='partial'?'我已核对上述片段，剩余内容仍能独立表达原事实及其适用条件。':'我已核对整条旧记忆，确认其中全部内容都应被新记忆替代。'}</label>
        </fieldset>
        <small>提交时会再次检查版本和原文；记录已变化或片段不符合要求时，不会执行替换。</small>
      </div>}
      {decision.error&&<p className="form-error" role="alert">处理失败：{String(decision.error)}</p>}
      <div className="modal-actions"><button onClick={close} disabled={decision.isPending}>取消</button><button onClick={refresh} disabled={decision.isPending||preview.isFetching}>重新读取</button><button className="primary" onClick={()=>decision.mutate()} disabled={!preview.data||!!preview.error||preview.isFetching||!checked||decision.isPending||(mode==='partial'&&fragments.some(value=>!value.trim()))}>{decision.isPending?'正在处理…':'确认采用'}</button></div>
    </section>
  </div>;
}
function MemoryDetail({item,close,confidenceSource,onChanged}:{item:MemoryItem;close():void;confidenceSource:(item:MemoryItem)=>string;onChanged():void}){
  const versions=useQuery({queryKey:['memory-versions',item.id],queryFn:()=>rpc.call<any>('memory.versions',{memory_id:item.id}).then(result=>result.versions||[])});
  const source=useQuery({queryKey:['memory-source',item.id],queryFn:()=>rpc.call<any>('memory.source.get',{memory_id:item.id}).then(result=>result.source),enabled:Boolean(item.source_id)});
  const rollback=useMutation({mutationFn:(revision:number)=>rpc.call('memory.rollback',{memory_id:item.id,revision}),onSuccess:()=>{onChanged();void versions.refetch()}});
  const reassess=useMutation({mutationFn:()=>rpc.call('memory.reassess',{memory_id:item.id}),onSuccess:onChanged});
  const verifiedEvidence=(item.evidence||[]).filter(value=>value.length>=3&&item.provenance?.some(entry=>entry.quote.includes(value)));
  const unverifiedEvidence=[...new Set([...(item.unverified_evidence||[]),...(item.evidence||[]).filter(value=>!verifiedEvidence.includes(value))])];
  return <div className="modal-back" onMouseDown={event=>{if(event.target===event.currentTarget)close()}}><section className="modal memory-detail" role="dialog" aria-modal="true" aria-labelledby="memory-detail-title">
    <header className="memory-detail-header"><div><Status value={item.status}/><h2 id="memory-detail-title">{item.title}</h2><p>{item.summary}</p></div><button type="button" onClick={close}>关闭</button></header>
    <div className="memory-detail-content">
      <section><h3>完整内容</h3><p className="memory-detail-full-content">{item.content||item.summary||'暂无完整内容'}</p></section>
      {item.adjudication?.state==='incomplete'&&<section className="memory-detail-section"><h3>关系判断未完成</h3><p>新增内容暂不作为回答依据。调用超时、服务异常或证据校验未通过，都不会据此判定旧事实存在矛盾。</p><p>{item.relation_reason}</p></section>}
      {canReassessMemory(item)&&<section className="memory-detail-section"><button onClick={()=>reassess.mutate()} disabled={reassess.isPending||rollback.isPending}>{reassess.isPending?'正在重新判断…':'重新判断'}</button><small>使用当前模型重新核对关联关系，保留这条记录；待确认模式仍需要确认。</small>{reassess.error&&<p className="form-error">重新判断失败：{String(reassess.error)}</p>}</section>}
      {!!item.partial_updates?.length&&<section className="memory-detail-section"><h3>局部事实更新</h3><p>本条保留未发生变化的内容，已替代部分另存历史记录。</p>{item.partial_updates.map((part,index)=><p key={index}>新事实：{part.replacement_id}<br/>旧事实片段：{part.history_id}<br/>{part.at}</p>)}</section>}
      {item.origin_memory_id&&<section className="memory-detail-section"><h3>旧事实片段来源</h3><p>原记忆：{item.origin_memory_id} · 原版本：{item.origin_revision}</p></section>}
      <section className="memory-detail-section"><h3>使用范围 · {memoryUsageLabel(item.usage_state)}</h3><p>{item.usage_guidance||'历史记录没有结构化事实信息，使用时仍需核对原文和来源。'}</p>{item.fact&&<><p>主体：{item.fact.subject||'未知'} · 主题：{item.fact.topic||'未知'} · 属性：{item.fact.attribute||'未知'}</p><small>事实类型：{({current:'当前事实',historical:'历史事实',plan:'计划',hypothesis:'假设或未核实推断',reported:'转述',unknown:'未确定'} as Record<string,string>)[item.fact.kind||'unknown']} · 时间范围：{item.fact.time_scope||'未明确'}</small><small>生效时间：{item.fact.valid_from||'未明确'} · 失效时间：{item.fact.valid_until||'未明确'}（不以写入时间推定）</small></>}{item.superseded_by&&<small>已被替代：{item.superseded_by}</small>}{!!item.supersedes?.length&&<small>替代的旧记忆：{item.supersedes.join('、')}</small>}{!!item.retained_ids?.length&&<small>本次保留的原记忆：{item.retained_ids.join('、')}</small>}</section>
      {item.adjudication&&<section className="memory-detail-section"><h3>关系裁决记录</h3><p>{item.adjudication.mode==='automatic'?'自动保存判断':'需确认的关系判断'} · {item.adjudication.at||'时间未知'}</p>{item.adjudication.decisions?.length?item.adjudication.decisions.map((decision,index)=><div key={`${decision.target_id}-${index}`}><p><b>{memoryRelationLabel(decision.relation)}</b> · {decision.reason}</p><small>对应记忆：{decision.target_id}{decision.confidence!=null?` · 模型判断置信度 ${Math.round(decision.confidence*100)}%`:''}</small>{decision.partial_update_safe&&<small>只更新以下旧事实片段：{decision.superseded_fragments?.join('；')}</small>}{decision.incoming_quote&&<p className="memory-detail-full-content">新增来源原文：{decision.incoming_quote}</p>}{decision.existing_quote&&<p className="memory-detail-full-content">原记忆依据：{decision.existing_quote}</p>}</div>):<small>未发现需要比较的相关记忆。</small>}{item.user_resolution&&<p>后续人工处理：{({confirm:'确认',confirm_qualified:'按条件性信息保留',duplicate:'确认重复支持',supplement:'确认补充',reject:'拒绝',keep_old:'保留原记忆',accept_new:'采用指定记忆',partial_replacement:'仅替换指定片段',merge:'合并',rollback:'回滚'} as Record<string,string>)[item.user_resolution.action||'']||item.user_resolution.action} · {item.user_resolution.at}</p>}<small>以上保留当时的判断；当前能否使用以“使用范围”为准。自动判断不等于事实必然正确。</small></section>}
      {!!item.provenance?.length&&<section className="memory-detail-section"><h3>核对过的原文引用</h3>{item.provenance.map((entry,index)=><div key={`${entry.id}-${index}`}><small>{entry.role==='user'?'用户原话':entry.role==='assistant'?'助手输出':entry.role||'来源'} · {entry.observed_at||'时间未记录'}{entry.message_id!=null?` · 消息 ${entry.message_id}`:''}</small><p className="memory-detail-full-content">{entry.quote}</p></div>)}</section>}
      <section className="memory-detail-section"><h3>置信度</h3><p>最终置信度：{item.confidence!=null?`${Math.round(item.confidence*100)}%`:'未提供'} · 模型估计：{item.model_confidence!=null?`${Math.round(item.model_confidence*100)}%`:'未提供'} · 来源：{confidenceSource(item)}</p></section>
      {item.extraction_reason&&<section className="memory-detail-section"><h3>提取依据</h3><p>{item.extraction_reason}</p></section>}
      {!!verifiedEvidence.length&&<section className="memory-detail-section"><h3>已核对原文的证据</h3><ul>{verifiedEvidence.map((evidence,index)=><li key={`${item.id}-evidence-${index}`}>{evidence}</li>)}</ul><small>已核对表示引用来自记录的原文，不代表内容已被证实为真。</small></section>}
      {!!unverifiedEvidence.length&&<section className="memory-detail-section"><h3>未核验说明</h3><ul>{unverifiedEvidence.map((note,index)=><li key={`${item.id}-note-${index}`}>{note}</li>)}</ul><small>这些说明保留供用户查看，不作为已核验证据返回给 Agent。</small></section>}
      {(item.source_type||item.source_label||item.source_id||item.source_ids?.length)&&<section className="memory-detail-section"><h3>来源</h3><p>{item.source_label||item.source_type||'conversation'}{item.source_id?` · ${item.source_id}`:''}</p>{item.source_ids?.length ? <small>关联来源：{item.source_ids.join('、')}</small> : null}{source.isPending&&<small>正在读取原始来源…</small>}{source.data?.run ? <><small>来源运行：{source.data.run.id} · {source.data.run.created_at}</small>{source.data.messages?.length ? <details><summary>查看原始会话片段</summary><div className="memory-source-messages">{source.data.messages.map((message:any,index:number)=><p key={`${item.id}-source-${index}`}><b>{message.role}：</b>{message.content}</p>)}</div></details> : null}</> : null}</section>}
      {(!!item.conflicts?.length||item.decision_state==='unresolved')&&<section className="memory-detail-section memory-detail-conflict"><h3>事实更新与争议</h3><p>{item.relation_reason||'现有依据不足以确定事实关系。'}</p><p>{item.status==='pending_review'?'保存方式要求确认，尚未允许 Agent 使用正文。':'已保留争议，不阻塞后续运行；默认不作为当前事实注入，可选人工处理。'}</p>{!!item.conflicts?.length&&<small>关联记忆：{item.conflicts.join('、')}</small>}<small>来源记录时间：{item.observed_at||'未知'}</small></section>}
      <section className="memory-detail-section"><h3>状态信息</h3><p>保存方式：{item.acceptance_source==='automatic'?'自动保存':item.acceptance_source==='user'?'用户确认':item.acceptance_source==='pending_review'?'等待确认':'历史记录'} · 用户确认：{item.user_confirmed?'已确认':'未确认'} · 支持次数：{item.support_count||1}</p><small>创建时间：{item.created_at||'未知'} · 更新时间：{item.updated_at||'未知'}</small>{item.retrieval_priority!=null&&<small>当前检索优先级：{Math.round(item.retrieval_priority*100)}%{item.decay_score!=null?'（包含时间衰减）':''}</small>}{item.last_update_reason&&<small>最近更新原因：{item.last_update_reason}</small>}{item.archive_reason&&<small>归档原因：{item.archive_reason}</small>}</section>
      <section className="memory-detail-section"><h3>老化与复用</h3><p>当前有效性：{item.decay_score!=null?`${Math.round(item.decay_score*100)}%`:'尚未计算'} · 最近变化：{formatDecayDelta(item.decay_delta)}</p><small>提供给模型：{item.injection_count||0} 次 · 读取详情：{item.access_count||0} 次 · 回答引用：{item.citation_count||0} 次</small><small>最近读取：{item.last_read_at||item.last_accessed_at||'未知'} · 最近引用：{item.last_cited_at||'暂无'}</small><small>最近计算：{item.last_decay_calculated_at||'尚未计算'} · 原因：{decayReasonLabel(item.decay_reason)}</small><small>同一运行重复读取只计一次，每天最多强化 5 次；仅提供摘要不会强化。引用记录不代表回答正确。老化不会降低事实置信度。</small></section>
      <section className="memory-detail-section"><h3>版本记录</h3>{versions.isPending?<small>正在读取版本…</small>:!versions.data?.length?<small>暂无版本记录</small>:<div className="memory-versions">{versions.data.map((version:any)=><div className="memory-version" key={`${item.id}-${version.revision}`}><span>v{version.revision} · {version.reason||'更新'} · {version.version_created_at||version.created_at}</span><button disabled={rollback.isPending||version.revision===item.revision} onClick={()=>rollback.mutate(Number(version.revision))}>回滚到此版本</button></div>)}</div>}</section>
    </div>
    <div className="modal-actions"><button type="button" onClick={close}>关闭</button>{rollback.error&&<span className="form-error">回滚失败：{String(rollback.error)}</span>}</div>
  </section></div>
}

function Memory({initialMemoryId}:{initialMemoryId?:string|null}){
  const queryClient=useQueryClient();
  const query=useQuery({queryKey:['memory'],queryFn:()=>rpc.call<any>('memory.list',{}),refetchInterval:60000});
  type MemoryTab='pending_review'|'active'|'archived';
  const [tab,setTab]=useState<MemoryTab>('pending_review');
  const [detail,setDetail]=useState<MemoryItem|null>(null);
  const [replacement,setReplacement]=useState<{id:string;confirm:boolean}|null>(null);
  const [searchText,setSearchText]=useState('');
  const [searching,setSearching]=useState('');
  const [sourceFilter,setSourceFilter]=useState('');
  const [projectFilter,setProjectFilter]=useState('');
  const [selected,setSelected]=useState<string[]>([]);
  const [capacity,setCapacity]=useState<number|null>(null);
  const confirmAction=useConfirmAction();
  const pendingReviews:MemoryItem[]=query.data?.pending_review||[];
  const activeMemories:MemoryItem[]=query.data?.active||[];
  const archivedMemories:MemoryItem[]=query.data?.archived||[];
  const memories=tab==='pending_review'?pendingReviews:tab==='active'?activeMemories:archivedMemories;
  const memoryDecision=useMutation({mutationFn:({id,accepted}:{id:string;accepted:boolean})=>rpc.call(accepted?'memory.confirm':'memory.reject',{memory_id:id}),onSuccess:()=>query.refetch()});
  const confirmSelected=useMutation({mutationFn:async()=>{
    const items=[...pendingReviews,...activeMemories,...archivedMemories,...displayedMemories];
    for(const id of selected){
      const item=items.find(item=>item.id===id);
      if(!item)throw new Error('选中的记忆已变化，请刷新列表后重新选择');
      if(memoryNeedsReplacementScope(item))throw new Error('选中记录包含需要指定替换范围的记忆，请先逐条点击“确认此记忆”处理；本次未提交批量确认');
      if(item.adjudication?.state==='incomplete')throw new Error('选中记录的关系判断尚未完成，请先重新判断；本次未提交批量确认');
    }
    for(const id of selected)await rpc.call('memory.confirm',{memory_id:id});
  },onSuccess:()=>{setSelected([]);void query.refetch()},onError:()=>void query.refetch()});
  const rejectSelected=useMutation({mutationFn:async()=>{for(const id of selected) await rpc.call('memory.reject',{memory_id:id})},onSuccess:()=>{setSelected([]);void query.refetch()}});
  const conflictDecision=useMutation({mutationFn:({id,action}:{id:string;action:string})=>rpc.call('memory.conflict.resolve',{memory_id:id,action}),onSuccess:()=>query.refetch()});
  const deleteSelected=useMutation({mutationFn:()=>rpc.call('memory.batch_delete',{memory_ids:selected}),onSuccess:()=>{setSelected([]);void query.refetch()}});
  const clearMemories=useMutation({mutationFn:()=>rpc.call('memory.clear',{include_core:false}),onSuccess:()=>void query.refetch()});
  const evaluateMemory=useMutation({mutationFn:()=>rpc.call<any>('memory.evaluate',{}),onSuccess:(result)=>{queryClient.setQueryData(['memory'],(current:any)=>({...current,evaluation:result}));void query.refetch()}});
  const saveCapacity=useMutation({mutationFn:()=>rpc.call('memory.policy.update',{max_active:capacity??query.data?.max_active??50}),onSuccess:()=>{setCapacity(null);void query.refetch()}});
  const searchQuery=useQuery({queryKey:['memory-search',searching,tab,sourceFilter,projectFilter],queryFn:()=>rpc.call<any>('memory.search',{query:searching,status:tab,limit:50,source_type:sourceFilter||undefined,project_id:projectFilter||undefined}),enabled:Boolean(searching||sourceFilter||projectFilter)});
  const refresh=()=>{void query.refetch()};
  const displayedMemories=(searching||sourceFilter||projectFilter)?(searchQuery.data?.results||[]):memories;
  const maintenance=query.data?.maintenance||{};
  const latestEvaluation=query.data?.evaluation;
  const maintenanceNote=maintenance.reason==='maintenance_model_unavailable'?'超出容量的记忆已延后处理，未自动归档。':String(maintenance.reason||'').startsWith('model_failed:')?'模型维护未完成，已保留原记忆。':'';
  const formatMemoryTime=(value?:string)=>value?new Date(value).toLocaleString('zh-CN',{hour12:false}):'尚未运行';
  const exportMemory=async()=>{const result=await rpc.call<any>('memory.export',{});const url=URL.createObjectURL(new Blob([JSON.stringify(result,null,2)],{type:'application/json'}));const anchor=document.createElement('a');anchor.href=url;anchor.download=`koreagent-memory-${new Date().toISOString().slice(0,10)}.json`;anchor.click();URL.revokeObjectURL(url)};
  const confidenceSource=(item:MemoryItem)=>({model_returned:'模型返回',rule_adjusted:'规则修正',default_fallback:'默认兜底',manual:'用户或 Agent 明确设置'}[item.confidence_source||'']||item.confidence_source||'未说明');
  const tabTitle=tab==='pending_review'?'待确认记忆':tab==='active'?'活跃记忆':'归档记忆';
  const visibleIds=displayedMemories.map((item:MemoryItem)=>item.id);
  useEffect(()=>{if(initialMemoryId){const found=[...pendingReviews,...activeMemories,...archivedMemories].find(item=>item.id===initialMemoryId);if(found)setDetail(found)}},[initialMemoryId,pendingReviews.length,activeMemories.length,archivedMemories.length])
  const toggleSelected=(id:string)=>setSelected(current=>current.includes(id)?current.filter(value=>value!==id):[...current,id]);
  const selectAll=()=>setSelected((current:string[])=>visibleIds.every((id:string)=>current.includes(id))?current.filter((id:string)=>!visibleIds.includes(id)):[...new Set([...current,...visibleIds])]);
  const requestClear=()=>{void confirmAction({title:'清空长期记忆？',message:'这会删除长期记忆记录，但不会删除核心记忆。操作不可撤销。',confirmLabel:'清空记忆',destructive:true}).then(yes=>{if(yes)clearMemories.mutate()})};
  if(query.isPending)return <Page title="记忆" subtitle="查看长期记忆和核心记忆。"><LoadingState label="正在读取长期记忆…"/></Page>;
  if(query.error)return <Page title="记忆" actions={<button onClick={refresh}>重试</button>}><ErrorState error={query.error} retry={refresh}/></Page>;
  return <Page title="记忆" subtitle="查看长期记忆和核心记忆。" actions={<div className="row-actions"><button onClick={()=>void exportMemory()}><FileOutput size={14}/>导出</button><button onClick={refresh}><RefreshCw size={14}/>刷新</button><button onClick={()=>evaluateMemory.mutate()} disabled={evaluateMemory.isPending}>{evaluateMemory.isPending?'检查中…':'立即检查'}</button></div>}>
    <div className="memory-layout">
      <section>
        <h2>核心记忆</h2>
        {query.data?.core_entries?.length?<dl className="core-entries">{query.data.core_entries.map((entry:any)=><div key={entry.key}><dt>{entry.key}</dt><dd>{entry.value}</dd></div>)}</dl>:<div className="core-memory">尚未形成核心记忆。</div>}
        <div className="memory-tabs" role="tablist" aria-label="记忆状态">
          <button role="tab" aria-selected={tab==='pending_review'} className={tab==='pending_review'?'active':''} onClick={()=>setTab('pending_review')}>待确认 {pendingReviews.length}</button>
          <button role="tab" aria-selected={tab==='active'} className={tab==='active'?'active':''} onClick={()=>setTab('active')}>活跃 {query.data?.active_count||0}</button>
          <button role="tab" aria-selected={tab==='archived'} className={tab==='archived'?'active':''} onClick={()=>setTab('archived')}>归档 {archivedMemories.length}</button>
        </div>
        <section className={tab==='pending_review'?'card memory-review':''}>
          <div className="section-heading"><div><h2>{tabTitle}</h2>{tab==='pending_review'&&<small>Agent 提取的长期事实需要你确认后才会保留。</small>}</div>{tab==='pending_review'&&<Status value="pending_review" label={`${pendingReviews.length} 条待处理`}/>}</div>
           <div className="memory-management"><button onClick={selectAll}>{visibleIds.length&&visibleIds.every((id:string)=>selected.includes(id))?'取消全选':'全选当前'}</button>{tab==='pending_review'?<><button onClick={()=>confirmSelected.mutate()} disabled={!selected.length||confirmSelected.isPending||rejectSelected.isPending}>确认选中{selected.length?` (${selected.length})`:''}</button><button onClick={()=>rejectSelected.mutate()} disabled={!selected.length||confirmSelected.isPending||rejectSelected.isPending}>拒绝选中{selected.length?` (${selected.length})`:''}</button></>:<button onClick={()=>deleteSelected.mutate()} disabled={!selected.length||deleteSelected.isPending}>删除选中{selected.length?` (${selected.length})`:''}</button>}<button onClick={requestClear} disabled={clearMemories.isPending}>清空长期记忆</button></div>
           <form className="memory-search-row" onSubmit={event=>{event.preventDefault();setSearching(searchText.trim())}}><input value={searchText} onChange={event=>setSearchText(event.target.value)} placeholder="搜索标题、摘要或正文"/><select value={sourceFilter} onChange={event=>setSourceFilter(event.target.value)} aria-label="按来源筛选"><option value="">全部来源</option><option value="conversation">对话</option><option value="email">邮件</option><option value="feishu">飞书</option><option value="web">网页</option></select><input value={projectFilter} onChange={event=>setProjectFilter(event.target.value)} placeholder="项目 ID（可选）" aria-label="按项目筛选"/><button type="submit" disabled={!searchText.trim()}>搜索</button>{searching&&<button type="button" onClick={()=>{setSearchText('');setSearching('');setSourceFilter('');setProjectFilter('')}}>清除</button>}</form>
          <div className="stack">
             {displayedMemories.map((item:MemoryItem)=><article className="memory-card" key={item.id}>
              <label className="memory-select"><input type="checkbox" checked={selected.includes(item.id)} onChange={()=>toggleSelected(item.id)} aria-label={`选择${item.title}`}/><Status value={item.status}/></label><h3>{item.title}</h3><p>{item.summary}</p>{item.usage_state&&<small>{memoryUsageLabel(item.usage_state)}{item.status==='stale'?' · 长期未使用，已降低检索优先级':''}{item.usage_state==='disputed'?' · 不作为当前事实自动注入，可选人工处理':''}</small>}
              <div className="row-actions"><button onClick={()=>setDetail(item)}>查看详情</button>{(tab==='pending_review'||item.usage_state==='disputed')&&<><button onClick={()=>memoryDecision.mutate({id:item.id,accepted:false})} disabled={memoryDecision.isPending}>拒绝</button><button className="primary" onClick={()=>memoryNeedsReplacementScope(item)?setReplacement({id:item.id,confirm:true}):memoryDecision.mutate({id:item.id,accepted:true})} disabled={memoryDecision.isPending||item.adjudication?.state==='incomplete'}>确认此记忆</button></>}{!!item.conflicts?.length&&<><button onClick={()=>conflictDecision.mutate({id:item.id,action:'keep_old'})} disabled={conflictDecision.isPending}>保留旧记忆</button><button className="primary" onClick={()=>setReplacement({id:item.id,confirm:false})} disabled={conflictDecision.isPending}>采用新记忆</button><button onClick={()=>conflictDecision.mutate({id:item.id,action:'merge'})} disabled={conflictDecision.isPending}>合并</button></>}</div>
            </article>)}
          </div>
           {!displayedMemories.length&&<Empty icon={Brain} title={searching?'没有匹配记忆':tab==='pending_review'?'没有待确认记忆':tab==='active'?'没有活跃记忆':'没有归档记忆'} hint={searching?'可以换一个关键词再试。':tab==='pending_review'?'新的长期记忆会在运行结束后显示在这里。':'稳定偏好与长期事实会在运行结束后写入。'}/>}
          {memoryDecision.error&&<p className="form-error" role="alert">记忆处理失败：{String(memoryDecision.error)}</p>}
          {conflictDecision.error&&<p className="form-error" role="alert">争议处理失败：{String(conflictDecision.error)}</p>}
          {confirmSelected.error&&<p className="form-error" role="alert">批量确认未全部完成：{String(confirmSelected.error)}</p>}
        </section>
      </section>
      <aside className="card memory-status-panel">
        <div className="section-heading"><h2>记忆运行状态</h2><Status value="active" label="自动运行"/></div>
        <p className="muted">Host 运行期间，每 6 小时维护一次，每 15 分钟检查一次。</p>
        <label>活跃记忆维护目标<input type="number" min={10} max={2000} step={1} value={capacity??query.data?.max_active??50} onChange={event=>setCapacity(Number(event.target.value))}/></label><button onClick={()=>saveCapacity.mutate()} disabled={saveCapacity.isPending||capacity===null||!Number.isInteger(capacity)||capacity<10||capacity>2000}>保存维护目标</button><p className="muted">超出目标后优先整理重复、被替代或过时的事实；不确定时保留。每轮仍最多自动注入 5 条。</p>{saveCapacity.error&&<p className="form-error">{String(saveCapacity.error)}</p>}
        <dl className="memory-status-list">
          <dt>上次维护时间</dt><dd>{formatMemoryTime(maintenance.last_run_at)}</dd>
          <dt>维护结果</dt><dd>{maintenance.last_run_at?<>{maintenance.decay_updated||0} 条状态更新，{maintenance.archived||0} 条归档，{maintenance.deferred||0} 条延后{maintenanceNote&&<p className="form-warning">{maintenanceNote}</p>}</>:'等待自动维护'}</dd>
          <dt>上次检查时间</dt><dd>{formatMemoryTime(latestEvaluation?.checked_at)}</dd>
          <dt>记忆检查结果</dt><dd>{latestEvaluation?.checked_at?`${latestEvaluation.passed||0}/${(latestEvaluation.passed||0)+(latestEvaluation.failed||0)} 项通过`:'等待自动检查'}</dd>
        </dl>
        {latestEvaluation?.cases?.length?<ul className="memory-check-results">{latestEvaluation.cases.map((item:any)=><li key={item.name}><Status value={item.passed?'completed':'failed'} label={item.passed?'通过':'失败'}/><span>{item.detail}</span></li>)}</ul>:null}
        {evaluateMemory.error&&<ErrorState error={evaluateMemory.error} title="记忆检查失败" retry={()=>evaluateMemory.mutate()}/>}
      </aside>
    </div>
    {detail&&<MemoryDetail item={detail} close={()=>setDetail(null)} confidenceSource={confidenceSource} onChanged={()=>{void query.refetch();setDetail(null)}}/>}
    {replacement&&<MemoryReplacementDialog key={`${replacement.id}-${replacement.confirm}`} memoryId={replacement.id} confirm={replacement.confirm} close={()=>setReplacement(null)} onChanged={()=>{setSelected([]);void query.refetch();void queryClient.invalidateQueries({queryKey:['memory-search']})}}/>}</Page>
}
