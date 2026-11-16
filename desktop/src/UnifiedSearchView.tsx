import { useEffect, useState } from 'react'
import { FileText, ExternalLink, Search, Sparkles, X } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { rpc } from './rpc'
import { EmptyState, ErrorState, LoadingState, Page } from './ui'

export type SearchResult = {
  id: string; type: string; source: string; conversation_id?: string; provider?: string; title: string; snippet?: string; timestamp?: string; path?: string; status?: string
  source_type?: string; entity_id?: string; detail_kind?: string; can_view_detail?: boolean; can_open_source?: boolean
  source_ref?: { conversation_id?: string; message_id?: string; [key: string]: unknown }
}
function typeLabel(item: SearchResult) { if (item.source === 'connector') return item.provider === 'email' ? '邮件' : item.provider === 'calendar' ? '日历' : item.provider === 'todo' ? '飞书任务' : '飞书文档'; if (item.source === 'memory') return '长期记忆'; if (item.source === 'workspace') return '本地文件'; if (item.source === 'personal') return item.type === 'goal' ? '目标' : item.type === 'project' ? '项目' : '行动'; return '会话' }
function detailText(value: unknown) { return value == null || value === '' ? '' : typeof value === 'string' ? value : JSON.stringify(value, null, 2) }

function normalizeResult(item: SearchResult): SearchResult {
  const sourceType = item.source_type || (item.source === 'message' ? 'conversation' : item.source)
  const entityId = item.entity_id || item.conversation_id || item.id
  const detailKind = item.detail_kind || (item.source === 'connector' ? ({ email: 'email', calendar: 'calendar', todo: 'todo' } as Record<string, string>)[item.provider || ''] || 'document' : item.type)
  return { ...item, source_type: sourceType, entity_id: entityId, detail_kind: detailKind, can_view_detail: item.can_view_detail !== false, can_open_source: item.can_open_source !== false, source_ref: item.source_ref || (item.conversation_id ? { conversation_id: item.conversation_id, message_id: item.id } : { item_id: item.id }) }
}

export default function UnifiedSearchView({ onOpenSource }: { onOpenSource?: (item: SearchResult) => void }) {
  const [query, setQuery] = useState(''); const [submittedQuery, setSubmittedQuery] = useState(''); const [scope, setScope] = useState('all'); const [submittedScope, setSubmittedScope] = useState('all'); const [selected, setSelected] = useState<SearchResult | null>(null); const [filePath, setFilePath] = useState<string | null>(null)
  const search = useQuery({ queryKey: ['unified-search', submittedQuery, submittedScope], enabled: submittedQuery.length > 0, retry: false, refetchOnWindowFocus: false, queryFn: () => rpc.call<{ results: SearchResult[]; count: number }>('search.unified', { query: submittedQuery, scope: submittedScope, limit: 50 }) })
  const loadDetail = async () => {
    const item = selected
    if (!item?.source_type || !item.entity_id) return { found: false, message: '详情暂不可用' }
    try {
      return await rpc.call<any>('search.unified.detail', { source_type: item.source_type, entity_id: item.entity_id, detail_kind: item.detail_kind, message_id: item.source_ref?.message_id })
    } catch (error: any) {
      // A renderer can briefly be newer than an already-running local Host.
      // Fall back to the stable read APIs instead of showing Method not found.
      if (error?.code !== -32601) throw error
      if (item.source_type === 'conversation') {
        const value = await rpc.call<any>('conversation.get', { conversation_id: item.source_ref?.conversation_id || item.entity_id })
        return { found: true, source_type: 'conversation', conversation: value.conversation, messages: (value.messages || []).slice(-7) }
      }
      if (item.source_type === 'connector') {
        const value = await rpc.call<any>('connector.inbox', { provider: item.provider, limit: 500 })
        const found = (value.items || []).find((candidate: any) => candidate.id === item.entity_id || candidate.source_id === item.source_ref?.source_id)
        return found ? { found: true, source_type: 'connector', item: found, provider: item.provider, source_id: found.source_id } : { found: false, message: '来源已不存在' }
      }
      if (item.source_type === 'memory') {
        const value = await rpc.call<any>('memory.list', { limit: 500 })
        const all = [...(value.pending_review || []), ...(value.active || []), ...(value.archived || [])]
        const found = all.find((candidate: any) => candidate.id === item.entity_id)
        return found ? { found: true, source_type: 'memory', memory: found } : { found: false, message: '来源已不存在' }
      }
      if (item.source_type === 'personal') {
        const method = item.detail_kind === 'goal' ? 'goal.list' : item.detail_kind === 'project' ? 'project.list' : item.detail_kind === 'action' ? 'action.list' : 'task.list'
        const key = item.detail_kind === 'goal' ? 'goals' : item.detail_kind === 'project' ? 'projects' : item.detail_kind === 'action' ? 'actions' : 'tasks'
        const value = await rpc.call<any>(method, { limit: 500 })
        const found = (value[key] || []).find((candidate: any) => candidate.id === item.entity_id)
        return found ? { found: true, source_type: 'personal', item: found, detail_kind: item.detail_kind } : { found: false, message: '来源已不存在' }
      }
      if (item.source_type === 'workspace') return { found: true, source_type: 'workspace', path: item.path || item.entity_id, can_extract: true }
      throw error
    }
  }
  const detail = useQuery({ queryKey: ['unified-search-detail', selected?.source_type, selected?.entity_id, selected?.source_ref?.message_id], enabled: Boolean(selected?.can_view_detail && selected?.source_type && selected?.entity_id), queryFn: loadDetail })
  const extracted = useQuery({ queryKey: ['file-extract', filePath], enabled: Boolean(filePath), queryFn: () => rpc.call<{ document: { text: string } }>('file.extract', { path: filePath, max_chars: 60000 }) })
  useEffect(() => { if (!selected) return; const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') setSelected(null) }; window.addEventListener('keydown', onKey); return () => window.removeEventListener('keydown', onKey) }, [selected])
  const results = (search.data?.results || []).map(normalizeResult); const selectedItem = detail.data?.item || detail.data?.memory
  return <Page title="统一搜索" subtitle="搜索会话、邮件、飞书信息、长期记忆、个人任务和工作区文件。"><form className="search-field unified-search-form" onSubmit={event => { event.preventDefault(); const next = query.trim(); if (next) { setSubmittedQuery(next); setSubmittedScope(scope) } }}><Search size={17} /><input value={query} onChange={event => setQuery(event.target.value)} placeholder="搜索关键词、联系人、项目或文件内容" autoFocus /><select value={scope} onChange={event => setScope(event.target.value)}><option value="all">全部来源</option><option value="messages">会话</option><option value="connectors">邮箱 / 飞书</option><option value="memory">长期记忆</option><option value="tasks">目标 / 任务</option><option value="files">本地文件</option></select><button className="primary" type="submit">搜索</button></form>{!submittedQuery ? <EmptyState icon={Sparkles} title="从一个关键词开始" hint="例如“毕业设计”“会议”“报销”或联系人姓名。" /> : search.isPending ? <LoadingState label="正在搜索…" /> : search.error ? <ErrorState error={search.error} retry={() => search.refetch()} /> : !results.length ? <EmptyState icon={Search} title="没有找到结果" hint="尝试更短的关键词，或切换搜索范围。" /> : <section className="search-results">{results.map(item => <article className="search-result" key={`${item.source}:${item.id}`}><div className="search-result-icon"><FileText size={17} /></div><div className="search-result-content"><div className="search-result-heading"><span className="status source-status">{typeLabel(item)}</span><h3>{item.title}</h3></div><p>{item.snippet || '没有摘要'}</p><small>{item.path || item.provider || item.status || ''}{item.timestamp ? ` · ${new Date(item.timestamp).toLocaleString()}` : ''}</small><div className="row-actions search-result-actions">{item.can_view_detail !== false && <button onClick={() => setSelected(normalizeResult(item))}>查看详情</button>}{item.source === 'workspace' && item.path && <button onClick={() => setFilePath(item.path!)}>提取文件内容</button>}</div></div></article>)}</section>}{filePath && <section className="file-preview card"><div className="section-heading"><h2>{filePath}</h2><button onClick={() => setFilePath(null)}>关闭</button></div>{extracted.isPending ? <LoadingState label="正在提取文件…" /> : extracted.error ? <ErrorState error={extracted.error} retry={() => extracted.refetch()} /> : <pre>{extracted.data?.document.text || '没有可提取文本'}</pre>}</section>}{selected && <div className="modal-back" onMouseDown={event => { if (event.target === event.currentTarget) setSelected(null) }}><aside className="search-detail-drawer" role="dialog" aria-modal="true" aria-labelledby="search-detail-title"><header className="section-heading"><div><span className="status source-status">{typeLabel(selected)}</span><h2 id="search-detail-title">{selected.title}</h2></div><button onClick={() => setSelected(null)} aria-label="关闭"><X size={17} /></button></header>{detail.isFetching ? <LoadingState label="正在读取详情…" /> : detail.error ? <ErrorState error={detail.error} retry={() => detail.refetch()} /> : detail.data?.found === false ? <EmptyState icon={FileText} title="来源已不存在" hint="这个对象可能已被删除或不再同步。" /> : !detail.data ? <EmptyState icon={FileText} title={"\u8be6\u60c5\u6682\u4e0d\u53ef\u7528"} hint={"\u8bf7\u786e\u8ba4 Host \u5df2\u91cd\u542f\uff0c\u5e76\u91cd\u65b0\u641c\u7d22\uff1b\u539f\u59cb\u641c\u7d22\u7ed3\u679c\u4ecd\u7136\u4fdd\u7559\u3002"} /> : <div className="search-detail-body">{detail.data?.messages && <section><h3>匹配消息及上下文</h3>{detail.data.messages.map((message: any) => <article className="search-message-context" key={message.id}><small>{message.role} · {message.created_at ? new Date(message.created_at).toLocaleString() : ''}</small><p>{message.content}</p></article>)}</section>}{selectedItem && <pre className="search-detail-pre">{detailText(selectedItem)}</pre>}{selected.source === 'workspace' && selected.path && <button onClick={() => setFilePath(selected.path!)}>提取文件内容</button>}</div>}<footer className="modal-actions">{selected.can_open_source && <button className="primary" onClick={() => selected.source === 'workspace' && selected.path ? setFilePath(selected.path) : onOpenSource?.(selected)}><ExternalLink size={14} />打开来源</button>}<button onClick={() => setSelected(null)}>关闭</button></footer></aside></div>}</Page>
}
