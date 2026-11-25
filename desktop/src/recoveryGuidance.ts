type RecoveryStep = {status?:string; superseded?:boolean; recovery_ignored?:boolean; tool_name?:string; arguments?:Record<string,unknown>}

export function recoveryGuidance(steps:RecoveryStep[]):string[] {
  const active=steps.filter(step=>!step.superseded&&!step.recovery_ignored&&step.tool_name!=='connector_workflow')
  const notes:string[]=[]
  if(active.some(step=>step.status==='uncertain'))notes.push('结果未知的操作请先查询远端；仅搜索不到不代表未执行。确认未执行后才能重试。')
  if(active.some(step=>['failed','awaiting_approval','approved'].includes(step.status||'')&&step.arguments?.compensation_for))notes.push('失败的删除补偿可重试原目标，不能改为其他对象；已核实成功的补偿不会再次执行。')
  if(active.some(step=>['failed','awaiting_approval','approved'].includes(step.status||'')&&!step.arguments?.compensation_for))notes.push('仅重试尚未完成的操作，必要时核对并修改参数；已经成功的步骤保留。')
  if(steps.some(step=>step.recovery_ignored&&!step.superseded))notes.push('暂不处理的步骤仍保留失败或未知状态，未视为成功。')
  return notes
}
