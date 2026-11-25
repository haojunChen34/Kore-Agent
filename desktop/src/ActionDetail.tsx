import {useQuery} from '@tanstack/react-query'
import {rpc} from './rpc'
import {ErrorState,LoadingState} from './ui'

export default function ActionDetail({id,close,openSource}:{id:string;close():void;openSource?(type:string,id:string):void}){
  const detail=useQuery({queryKey:['action-detail',id],queryFn:()=>rpc.call<{action:{title:string;description?:string;status:string};sources:Array<{type:string;id:string;title:string;found:boolean}>}>('action.get',{action_id:id})})
  return <div className="modal-back" onClick={event=>{if(event.currentTarget===event.target)close()}}><section className="modal" role="dialog" aria-modal="true" aria-label="行动详情"><div className="section-heading"><h2>{detail.data?.action.title||'行动详情'}</h2><button onClick={close}>关闭</button></div>{detail.isPending?<LoadingState label="正在读取行动…"/>:detail.error?<ErrorState error={detail.error}/>:<><p>{detail.data?.action.description||'暂无说明'}</p><h3>来源关联</h3>{detail.data?.sources.length?detail.data.sources.map(source=><article className="card" key={`${source.type}:${source.id}`}><strong>{source.title}</strong><small>{source.type==='connector'?'收件箱信息':source.type==='conversation'?'对话':'记忆'}</small><button disabled={!source.found||!openSource} onClick={()=>openSource?.(source.type,source.id)}>打开来源</button></article>):<p>没有已保存的来源关联。</p>}</>}</section></div>
}
