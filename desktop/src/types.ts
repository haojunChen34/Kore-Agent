export interface Conversation { id: string; title: string; created_at: string; updated_at: string; message_count: number }
export interface Message { role: 'system'|'user'|'assistant'|'tool'; content: string|null; name?: string|null; reasoning?: string|null; tool_call_id?:string|null; tool_calls?:Array<{id:string;name:string;arguments:Record<string,unknown>}> }
export interface Run { id:string; conversation_id:string|null; status:string; user_message:string; created_at:string; updated_at:string; error:string|null; stop_reason?:string|null; mode:'normal'|'plan'; source?:string|null; recovered_from_run_id?:string|null }
export interface Approval { id:string; run_id:string|null; conversation_id?:string|null; tool_name:string; arguments:Record<string,unknown>; preview?:Record<string,unknown>; reason:string; status:string; created_at:string; resolved_at?:string|null; resolution_scope?:PermissionScope; ui_scope?:string; available_scopes?:PermissionScope[] }
export type PermissionScope='once'|'run'|'conversation'|'global'
export interface Automation { id:string; title:string; prompt:string; goal_id?:string|null; status:string; schedule:{kind:string;run_at:string|null;interval_seconds:number|null;cron_expr:string|null;timezone:string}; next_run_at:string|null; last_run_at:string|null; last_run_id?:string|null }
export interface Artifact { id:string;kind:'file'|'url';title:string;description:string|null;filename:string|null;mime_type:string|null;size_bytes:number;created_at:string;source_url?:string|null;run_id?:string|null;conversation_id?:string|null }
export interface Task { id:string;title:string;description:string|null;goal?:string|null;goal_id?:string|null;long_term?:boolean;status:string;owner_conversation_id?:string|null;steps:Array<{id:string;title:string;description?:string;status:string}>;updated_at:string }
export interface ModelUsage {
  input_tokens:number; output_tokens:number; total_tokens:number; model_calls:number;
  cached_input_tokens:number|null; uncached_input_tokens:number|null;
  cache_read_input_tokens:number|null; cache_write_input_tokens:number|null;
}
export interface RunBudgetSummary {
  run_budget_enabled?:boolean; run_budget_mode?:string; run_budget_multiplier?:number|null;
  run_budget_token_limit?:number|null; run_budget_model_call_limit?:number|null; run_budget_extension_count?:number;
  run_budget_status:string; run_budget_reason:string|null;
  run_budget_chargeable_tokens:number; run_budget_model_calls:number;
  run_budget_warning_tokens:number; run_budget_finalization_tokens:number; run_budget_hard_tokens:number;
  run_budget_warning_model_calls:number; run_budget_finalization_model_calls:number; run_budget_hard_model_calls:number;
}
export interface RunUsageSummary {
  main_agent:ModelUsage; context_summary:ModelUsage; memory_reflection:ModelUsage;
  memory_maintenance?:ModelUsage; skill_learning:ModelUsage; provider_total:ModelUsage; budget:RunBudgetSummary;
}
export interface ResearchClaim { text:string; source_ids:string[]; supported:boolean }
export interface ResearchReport {
  type:'research_report'; answer:string; query:string; mode:string; question_type?:string|null;
  claims:ResearchClaim[]; sources:Array<{id:string;title?:string;page_title?:string;url:string;snippet?:string;content_excerpt?:string;published_at?:string;final_score?:number;read?:boolean;fetch_error?:string;trust?:{level:string;label:string;reasons?:string[]};content_role?:string}>;
  cited_source_ids:string[]; invalid_citations:string[]; citation_coverage:number; uncertainties:string[]; warnings:string[];
}
export interface ResearchSession {
  id:string; run_id:string|null; conversation_id:string|null; query:string; mode:string;
  question_type?:string|null; current_version:number; created_at:string; updated_at:string;
  citation_coverage:number; source_count:number;
  related?:{conversation_id?:string|null;run_id?:string|null;local_matches?:Array<Record<string,unknown>>;personal_context?:{next_actions?:Array<Record<string,unknown>>;active_goals?:Array<Record<string,unknown>>}};
}
export interface ResearchMonitor {
  id:string; name:string; query:string; mode:string; schedule:{kind:string;run_at?:string|null;interval_seconds?:number|null;cron_expr?:string|null;cron_exprs?:string[];timezone?:string}; status:string;
  allowed_domains:string[]; source_url?:string|null; notify_on_change:boolean; next_run_at?:string|null; last_run_at?:string|null; last_change?:Record<string,unknown>; last_error?:string|null; retry_count?:number; next_retry_at?:string|null; error_kind?:string|null; recovery_required?:boolean; created_at:string; updated_at:string;
}
export interface ResearchMonitorUpdate {
  id:string; monitor_id:string; snapshot_id:string; version:number; status:'unread'|'read'|'ignored'; is_favorite:boolean;
  discussion_conversation_id?:string|null; created_at:string; updated_at:string; kind?:string; initial?:boolean; url?:string; title?:string;
  summary?:string; importance?:string; similarity?:number|null; monitor_name?:string; query?:string;
  trust?:{level?:string;label?:string;reasons?:string[]}; page_title?:string; description?:string; snippet?:string;
  content_excerpt?:string; before_content?:string; after_content?:string; published_at?:string; final_url?:string; source_id?:string;
}
export interface ContextSnapshot {
  estimated_tokens:number; original_estimated_tokens:number; prepared_input_tokens:number;
  context_window:number|null; context_window_source?:string; max_input_tokens:number|null; working_input_budget:number|null;
  tool_result_budget_tokens:number|null; tool_schema_tokens_estimated:number;
  tool_result_tokens_before:number; tool_result_tokens_after:number;
  breakdown:{system:number;messages:number;tool_results:number;tool_schemas:number};
  compacted:boolean; compaction_stage:string; summary_validated:boolean;
  summary_updated:boolean; dropped_messages:number;
  memory_context?:{used:boolean;count:number;memory_ids:string[];reasons:Record<string,{search_score?:number;confidence?:number;status?:string;source_type?:string}>};
}
export interface TraceEvent {
  event_id:string; run_id:string; type:string; event_time:string; sequence:number;
  step?:number|null; provider?:string; model?:string; role?:string;
  usage?:Partial<ModelUsage>|null; context?:ContextSnapshot;
  tool_call?:{id?:string;name:string;arguments?:Record<string,unknown>}|null;
  tool_result?:{tool_call_id?:string;tool_name?:string;success?:boolean;output?:string;error?:string;duration_ms?:number}|null;
  message?:{role?:string;content?:string|null;tool_calls?:unknown[]}|null;
  error?:{type?:string;message?:string}|string|null;
  [key:string]:unknown;
}
export interface RunTracePayload {
  trace:{run_id:string;conversation_id:string|null;status:string;started_at:string;completed_at:string|null;
    provider:string;model:string;steps:number;stop_reason:string|null;input_tokens:number;output_tokens:number;
    total_tokens:number;event_count:number;research_report?:ResearchReport|null};
  events:TraceEvent[];usage_summary:RunUsageSummary;
}
export interface MemoryItem { acceptance_source?:string; relation_kind?:string; relation_reason?:string; replaces_id?:string; injection_count?:number; citation_count?:number; last_cited_at?:string; id:string; title:string; summary:string; content?:string; status:string; access_count:number; revision:number; created_at:string; updated_at:string; last_accessed_at?:string; source?:string; source_type?:string; source_id?:string; source_label?:string; observed_at?:string; contacts?:string[]; project_id?:string; confidence?:number|null; model_confidence?:number|null; confidence_source?:string; user_confirmed?:boolean; confirmed_by_user?:boolean; evidence?:string[]; extraction_reason?:string; support_count?:number; source_ids?:string[]; sensitive?:boolean; sensitive_fields?:string[]; conflict_status?:string; conflicts?:string[]; retrieval_priority?:number; decay_score?:number; previous_decay_score?:number|null; decay_delta?:number|null; last_decay_calculated_at?:string; decay_reason?:string; last_update_reason?:string; archive_reason?:string|null }
export interface PersonalProfile { name:string; preferred_language:string; timezone:string; communication_style:string; work_start_time:string; work_end_time:string; quiet_hours_start:string; quiet_hours_end:string; assistant_name:string; daily_brief_enabled:boolean; desktop_notifications_enabled:boolean }
export interface PersonalAction { id:string; title:string; description:string; status:string; priority:string; due_at:string|null; goal_id:string|null; project_id:string|null; source:string; created_at:string; updated_at:string; completed_at:string|null }
export interface PersonalGoal { id:string; title:string; description:string; priority:string; status:string; created_at:string; updated_at:string }
export interface PersonalNotification { id:string; title:string; body:string; kind:string; entity_id:string|null; status:string; created_at:string }
export interface DailyBriefContext {
  today_actions: Array<Record<string, any>>;
  next_actions: Array<Record<string, any>>;
  active_goals: Array<Record<string, any>>;
  unread_notifications: Array<Record<string, any>>;
  important_external_information: Array<Record<string, any>>;
  upcoming_calendar_events: Array<Record<string, any>>;
  connector_alerts: Array<Record<string, any>>;
}
export interface DailyBrief {
  date:string; title:string; content:string; ai_generated?:boolean;
  priorities?:string[]; risks?:string[]; next_step?:string;
  context?:DailyBriefContext;
  counts?:Record<string, number>; generated_at?:string; timezone?:string;
}
export interface PersonalContext { profile:PersonalProfile; privacy:Record<string,boolean>; today:{date:string;actions:PersonalAction[];overdue_actions:PersonalAction[];active_goals:PersonalGoal[];notifications:PersonalNotification[];next_actions:PersonalAction[]}; brief:DailyBrief|null }
export interface ExternalInformation {
  id:string; provider:string; source_id:string; connector_item_id?:string|null; kind:string;
  relevance:string; status:string; analysis:Record<string,any>; original:Record<string,any>;
  created_at:string; updated_at:string; analyzed_at?:string|null; handled_at?:string|null; snoozed_until?:string|null;
}

