export function queryKeysForAgentEvent(kind:string):string[]{
  // Streamed deltas live in the app's conversation buffers; refetching data on
  // every token would make the UI lag behind the stream.
  if(kind==='assistant_delta'||kind==='reasoning_delta')return []
  if(kind==='tool_call'||kind==='tool_result'||kind==='context_prepared')return ['trace']
  if(kind.startsWith('approval_'))return ['approvals','trace']
  if(kind.startsWith('post_run')||kind==='auxiliary_model_completed')return ['trace','memory','extensions']
  if(kind.startsWith('task_'))return ['tasks','trace']
  if(kind.startsWith('personal_')||kind.startsWith('notification_'))return ['personal-context','notifications']
  if(kind.startsWith('connector.'))return ['connector-inbox','connector-connections','connector-events','personal-context','external-information']
  if(kind.startsWith('external_information.'))return ['external-information','personal-context']
  if(kind==='run.deleted'||kind==='runs.deleted')return ['runs','trace','artifacts','conversations','tasks']
  if(kind.startsWith('proactive.')||kind.startsWith('execution.'))return ['proactive-suggestions','execution-jobs','proactive-triggers','audit-log','notifications']
  if(kind==='agent_completed'||kind==='agent_failed')return ['runs','trace','conversations','tasks','memory','extensions','research']
  if(kind==='research.monitor.changed')return ['research','research-monitors','research-monitor-updates','personal-context','notifications','audit-log']
  if(kind==='research.monitor.update.updated')return ['research-monitor-updates','personal-context']
  return ['trace']
}
