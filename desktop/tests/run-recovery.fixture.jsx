import React from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import RunExplorer from '../src/RunExplorer'
import { ConfirmProvider } from '../src/ConfirmDialog'
import { rpc, RpcError } from '../src/rpc'

const root=createRoot(document.getElementById('root'));
const make=(id,status='interrupted')=>({id,status,user_message:id,mode:'normal',conversation_id:null,created_at:'2026-01-01T00:00:00Z',updated_at:'2026-01-01T00:00:00Z',error:null});
window.start=(mode,initialRunId)=>{
 const parent=make('parent'), other=make('other'), child=make('child','completed');child.recovered_from_run_id='parent';
 if(mode==='lifecycle'||mode==='interrupt-error')parent.status='running';
 if(mode==='lifecycle')child.status='interrupted';
 const all=[parent,other];if(mode==='existing'||mode==='deleted'||mode==='offpage'){parent.recovery_run_id='child';parent.recovery_status=mode==='deleted'?null:'completed';if(mode==='existing')all.push(child)}
 const client=new QueryClient({defaultOptions:{queries:{retry:false}}});
 const f=window.fixture={all,mode,calls:[],release:null,client};
 rpc.call=async(method,params={})=>{
  f.calls.push({method,params});
  if(method==='run.list')return {runs:all.map(r=>({...r}))};
  if(method==='run.get')return {run:{...(params.run_id==='child'?child:all.find(r=>r.id===params.run_id))}};
  if(method==='artifact.list')return {artifacts:[]};
  if(method==='trace.get'){const r=params.run_id==='child'?child:all.find(r=>r.id===params.run_id);return {trace:{...r,run_id:r.id,started_at:r.created_at,completed_at:r.created_at,provider:'fixture',model:'fixture',steps:0,event_count:0},events:[],usage_summary:{}}}
  if(method==='run.interrupt'){
   if(mode!=='lifecycle')throw new RpcError(-32001,'interrupt failure');
   parent.status='interrupted';return {run:{...parent}};
  }
  if(method==='run.recover'){
   if(mode==='lifecycle'&&params.run_id==='child'){
    const next=make('grandchild','completed');next.recovered_from_run_id='child';all.unshift(next);
    child.recovery_run_id=next.id;child.recovery_status=next.status;return {run:{...next}};
   }
   if(mode==='error')throw new RpcError(-32001,'workspace mismatch');
   if(mode==='late-error')await new Promise((_resolve,reject)=>{f.release=()=>reject(new RpcError(-32001,'late workspace mismatch'))});
   if(mode==='late-success')await new Promise(resolve=>{f.release=resolve});
   parent.recovery_run_id='child';parent.recovery_status=child.status;if(!all.some(r=>r.id==='child'))all.unshift(child);
   if(mode==='stale')throw new RpcError(-32001,'already recovered',{reason:'already_recovered',run_id:'child'});
   if(mode==='existing'||mode==='deleted'||mode==='offpage')throw new RpcError(-32001,'already recovered',{reason:'already_recovered',run_id:'child'});
   return {run:{...child}};
  }
  throw Error('Unexpected RPC '+method);
 };
 root.render(React.createElement(QueryClientProvider,{client},React.createElement(ConfirmProvider,null,React.createElement(RunExplorer,{key:mode,initialRunId,onOpenConversation:()=>{}}))));
};window.ready=true;
