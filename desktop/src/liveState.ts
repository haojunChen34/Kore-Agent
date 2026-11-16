import type { Approval, ResearchReport } from './types'

export type LiveTool={id:string;name:string;status:'running'|'completed'|'failed';summary?:string}
export type ResearchSource={id:string;title?:string;url:string;snippet?:string;page_title?:string;content_excerpt?:string;read?:boolean;fetch_error?:string;published_at?:string;final_score?:number;trust?:{level:string;label:string;reasons?:string[]};content_role?:string}
export type ResearchPacket={query:string;mode:string;question_type?:string;provider?:string;source_count:number;read_count:number;sources:ResearchSource[];errors?:string[];plan?:{queries?:string[];query_budget?:number;source_budget?:number;page_budget?:number;parallel_search?:boolean}}
export type LiveRunState={runId:string;content:string;reasoning:string;tools:string[];toolActivity:LiveTool[];status:string;historyLength?:number;budgetStatus?:string;research?:ResearchPacket;researchReport?:ResearchReport}

export function liveRunPlaceholder(state:Pick<LiveRunState,'status'|'content'>):string|null{
  if(state.content)return null
  if(state.status==='starting'||state.status==='running')return '正在组织下一步'
  return null
}

export function reduceConversationLiveEvent(previous:LiveRunState|null,event:any,requestPending:boolean):LiveRunState|null{
  // A terminal event can arrive after conversation.send has already resolved
  // and cleared the live buffer. It belongs to the completed request and must
  // not recreate a new loading bubble. Also keep late events from an older run
  // from replacing the currently active run in the same conversation.
  if(!previous&&!requestPending)return null
  if(previous&&event.run_id&&previous.runId&&event.run_id!==previous.runId)return previous
  return reduceAgentEvent(previous,event)
}

export function reduceAgentEvent(previous:LiveRunState|null,event:any):LiveRunState{
  // Start a clean live buffer when the event belongs to a different run.
  const terminalStatus=event.type==='agent_completed'?'completed':event.type==='agent_failed'?'failed':event.type==='run_interrupted'?'interrupted':event.type==='run_cancelled'?'cancelled':undefined
  const current:LiveRunState=previous&&previous.runId===event.run_id
    ? previous
    : {runId:String(event.run_id||''),content:'',reasoning:'',tools:[],toolActivity:[],status:terminalStatus||'running'}
  if(event.type==='assistant_delta')return{...current,content:current.content+String(event.delta||'')}
  if(event.type==='reasoning_delta')return{...current,reasoning:current.reasoning+String(event.delta||'')}
  if(event.type==='tool_call'){
    const name=String(event.tool_call?.name||'tool')
    const id=String(event.tool_call?.id||`${name}:${current.toolActivity.length}`)
    return{...current,tools:[...current.tools,name],toolActivity:[...current.toolActivity,{id,name,status:'running'}]}
  }
  if(event.type==='tool_result'){
    const id=String(event.tool_result?.tool_call_id||event.tool_call_id||'')
    const success=event.tool_result?.success!==false
    // Match by call ID: tools with the same name can be active in one run.
    const raw=event.tool_result?.output
    let research=current.research
    if(success&&event.tool_result?.tool_name==='web_research'&&typeof raw==='string'){
      try{
        const parsed=JSON.parse(raw)
        if(parsed?.type==='web_research')research=parsed
      }catch{/* The model-facing output can be truncated or non-JSON. */}
    }
    return{...current,toolActivity:current.toolActivity.map(item=>item.id===id?{...item,status:success?'completed':'failed',summary:String(event.tool_result?.error||event.tool_result?.output||'').slice(0,160)}:item),research}
  }
  if(event.type?.startsWith('run_budget_')){
    if(event.run_budget_enabled===false || event.run_budget_mode==='unlimited')return {...current,budgetStatus:undefined}
    const labels:Record<string,string>={configured:'',warning:'接近预算，允许最后一轮必要工具调用',tool_round:'最后一轮必要工具调用',extension_requested:'正在根据进展评估预算扩展',extended:'已有新进展，预算已自动扩展一次（+50%）',extension_denied:'未获扩展，准备收尾',finalizing:'正在整理阶段性结果',exceeded:'预算已耗尽',finalized:'最终答复已完成'}
    return {...current,budgetStatus:labels[event.type.replace('run_budget_','')]||undefined}
  }
  if(event.type==='agent_failed'){
    const message=String(event.error?.message||event.message?.content||'')
    const budget=message.toLowerCase().includes('budget')||message.includes('预算')
    return{...current,status:'failed',content:current.content|| (budget?'读取内容过多，运行预算已耗尽，请缩小文件范围后重试。':'任务执行失败，请查看运行记录了解详情。')}
  }
  if(event.type==='agent_completed')return{...current,status:'completed',content:current.content||String(event.message?.content||''),researchReport:event.research_report||event.result?.research_report||current.researchReport}
  return current
}
