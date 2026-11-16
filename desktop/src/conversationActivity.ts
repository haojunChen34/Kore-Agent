import { reduceAgentEvent, type LiveRunState } from './liveState'
import type { Run } from './types'

type RequestHandle={conversationId:string;token:number}
type Activity={live:LiveRunState;token?:number}
const activeStatus=(status:string)=>status==='pending'||status==='running'
const terminalEvents=new Set(['agent_completed','agent_failed','run_interrupted','run_cancelled'])
const startingEvents=new Set(['agent_started','assistant_delta','reasoning_delta','tool_call'])

// Owned by the app, rather than a mounted Chat. Streaming must continue while
// another conversation or module is visible. Nothing here is written to disk.
export class ConversationActivityStore {
  private activities=new Map<string,Activity>()
  private closedRuns=new Set<string>()
  private listeners=new Set<()=>void>()
  private sequence=0

  subscribe=(listener:()=>void)=>{
    this.listeners.add(listener)
    return()=>{this.listeners.delete(listener)}
  }
  get=(conversationId:string):LiveRunState|null=>this.activities.get(conversationId)?.live||null
  private publish(){this.listeners.forEach(listener=>listener())}
  private close(runId:string){
    if(!runId)return
    this.closedRuns.add(runId)
    if(this.closedRuns.size>500)this.closedRuns.delete(this.closedRuns.values().next().value!)
  }
  private empty(runId:string,status:string,historyLength?:number):LiveRunState{
    return {runId,status,historyLength,content:'',reasoning:'',tools:[],toolActivity:[]}
  }
  begin(conversationId:string,historyLength?:number):RequestHandle{
    const handle={conversationId,token:++this.sequence}
    this.activities.set(conversationId,{live:this.empty('','starting',historyLength),token:handle.token})
    this.publish()
    return handle
  }
  finish(handle:RequestHandle){
    const current=this.activities.get(handle.conversationId)
    // A response for A must never clear B, or a newer request in A.
    if(current?.token!==handle.token)return
    this.close(current.live.runId)
    this.activities.delete(handle.conversationId)
    this.publish()
  }
  fail(handle:RequestHandle){
    const current=this.activities.get(handle.conversationId)
    if(current?.token!==handle.token)return
    // Losing the RPC connection does not mean the background run stopped.
    // The run-list reconciliation will recover its actual terminal status.
    if(current.live.runId){this.activities.set(handle.conversationId,{live:current.live});return}
    this.finish(handle)
  }
  event(event:any,historyLength?:number){
    const conversationId=String(event?.conversation_id||'')
    const runId=String(event?.run_id||'')
    if(!conversationId||!runId||this.closedRuns.has(runId))return
    const current=this.activities.get(conversationId)
    if(terminalEvents.has(event.type)){
      this.close(runId)
      if(current?.live.runId===runId){this.activities.delete(conversationId);this.publish()}
      return
    }
    if(!current&&!startingEvents.has(event.type))return
    if(current?.live.runId&&current.live.runId!==runId){
      if(event.type!=='agent_started')return
      this.close(current.live.runId)
    }
    const sameRun=current&&(!current.live.runId||current.live.runId===runId)
    // Bind the first backend run ID without dropping the optimistic history
    // boundary or the request identity used by delayed RPC callbacks.
    const previous=sameRun?{...current.live,runId,status:'running'}:this.empty(runId,'running',historyLength)
    this.activities.set(conversationId,{live:reduceAgentEvent(previous,event),token:sameRun?current.token:undefined})
    this.publish()
  }
  status(runId:string,status:string):string|undefined{
    if(!runId||!status||activeStatus(status))return
    this.close(runId)
    for(const [conversationId,current] of this.activities){
      if(current.live.runId!==runId)continue
      this.activities.delete(conversationId)
      this.publish()
      return conversationId
    }
  }
  reconcile(conversationId:string,runs:Run[],observed:LiveRunState|null,historyLength?:number){
    const current=this.get(conversationId)
    // Do not let a stale run-list response overwrite events received while
    // that request was in flight, or clear a newly submitted request.
    if(current!==observed)return
    if(current){
      if(!current.runId)return
      const run=runs.find(item=>item.id===current.runId)
      if(run&&!activeStatus(run.status))this.status(run.id,run.status)
      return
    }
    const run=runs.find(item=>activeStatus(item.status)&&!this.closedRuns.has(item.id))
    if(!run)return
    this.activities.set(conversationId,{live:this.empty(run.id,'running',historyLength)})
    this.publish()
  }
  forget(conversationId:string){
    const current=this.get(conversationId)
    if(current)this.close(current.runId)
    this.activities.delete(conversationId)
    this.publish()
  }
}

export const conversationActivity=new ConversationActivityStore()
