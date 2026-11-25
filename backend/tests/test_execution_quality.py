import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from kore_next.agent import ModelReply, ToolExecutor
from kore_next.application import Application, RpcFault
from kore_next.config import Settings
from kore_next.execution_quality import explicitly_read_only
from kore_next.file_processing import FileProcessingService
from kore_next.mcp import McpManager
from kore_next.research import build_research_report, extract_article, semantic_diff_summary
from kore_next.util import dumps, now


class ExecutionQualityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.app = Application(Settings(data_dir=root/'data', workspace_dir=root/'workspace', user_skills_dir=root/'skills', max_steps=12))
        await self.app.db.initialize()
        await self.app.privacy_update({'memory_in_context_enabled': False, 'auto_memory_enabled': False})
        self.app._active_model_config = AsyncMock(return_value={'provider': 'openai', 'model': 'test', 'context_window': 128000})
        self.app._post_run = AsyncMock()
        self.mcp = McpManager(root)
        self.tools = ToolExecutor(self.app.settings, AsyncMock(return_value=True), AsyncMock(), AsyncMock(), self.mcp, self.app)

    async def asyncTearDown(self):
        if self.app.background_tasks:
            await asyncio.gather(*self.app.background_tasks, return_exceptions=True)
        self.app.db.release_lock()
        self.tmp.cleanup()

    def server(self, name, tools):
        server = SimpleNamespace(config={'enabled': True, 'permission': 'allowed', 'description': name}, error=None, tools=tools, request=AsyncMock(return_value={'content': []}))
        self.mcp.servers[name] = server
        return server

    async def test_model_failure_explains_durable_external_outcomes(self):
        cv = (await self.app.conversation_create({}))['conversation']['id']
        run = await self.app._new_run(cv, '创建任务后总结', 'normal')
        for key, status in [('created', 'succeeded'), ('uncertain', 'uncertain')]:
            await self.app.prepare_external_operation(run['id'], key, 'feishu_task_create',
                                                     {'title': key, 'idempotency_key': key})
            await self.app.db.finish_external_operation(idempotency_key=key, status=status, updated_at=now(),
                                                       result={'task': {'guid': 'task-confirmed'}} if status == 'succeeded' else None)
        with patch('kore_next.application.ModelClient.complete', AsyncMock(side_effect=RuntimeError('model unavailable'))):
            with self.assertRaises(RpcFault):
                await self.app._execute_run(run)
        saved = (await self.app.run_get({'run_id': run['id']}))['run']
        self.assertEqual(saved['status'], 'failed')
        self.assertIn('已成功', saved['error'])
        self.assertIn('task-confirmed', saved['error'])
        self.assertIn('结果未知', saved['error'])
        messages = await self.app.db.fetchall("SELECT content FROM messages WHERE conversation_id=? AND role='assistant'", (cv,))
        self.assertTrue(any('后续执行或收尾失败' in row['content'] for row in messages))

    async def test_catalog_disambiguates_and_paginates_without_permission_bypass(self):
        self.server('weather', [{'name': 'lookup', 'description': 'Weather query', 'inputSchema': {'type': 'object'}}])
        self.server('shipping', [{'name': 'lookup', 'description': 'Package delivery', 'inputSchema': {'type': 'object'}}] + [{'name': f'operation{i}', 'description': 'Package query'} for i in range(9)])
        first = self.tools.activate_tools('mcp', 'normal')
        second = self.tools.activate_tools('mcp', 'normal', first['next_offset'])
        names = [t['name'] for t in first['tools'] + second['tools']]
        self.assertIn('mcp__shipping__lookup', names)
        self.assertIn('mcp__weather__lookup', names)
        self.assertEqual(len(names), len(set(names)))
        self.assertIsNone(second['next_offset'])
        self.assertTrue(all('parameters' in t and 'description' in t for t in first['tools']))

    async def test_invalid_mcp_arguments_never_reach_server(self):
        server = self.server('inventory', [{'name': 'lookup', 'inputSchema': {'type': 'object', 'properties': {'id': {'type': 'string'}}, 'required': ['id'], 'additionalProperties': False}}])
        for arguments in ({}, {'id': 17}, {'id': 'X', 'guessed': True}):
            with self.assertRaisesRegex(ValueError, 'schema error'):
                await self.mcp.call('mcp__inventory__lookup', arguments)
        server.request.assert_not_awaited()
        await self.mcp.call('mcp__inventory__lookup', {'id': 'X'})
        server.request.assert_awaited_once()

    async def test_read_only_filters_and_rejects_direct_write_and_shell(self):
        self.tools.read_only = True
        self.tools.activate_tools('', 'normal')
        names = {d['function']['name'] for group in self.tools.tool_definitions('normal') for d in group}
        self.assertNotIn('write_file', names)
        self.assertNotIn('shell', names)
        for name, args in [('write_file', {'path': 'x', 'content': 'bad'}), ('shell', {'command': 'echo bad'}), ('action_create', {'title': 'bad'}), ('http_request', {'method': 'POST'})]:
            with self.assertRaises(PermissionError):
                await self.tools.execute('run', name, args, 'call')

    async def test_read_only_mcp_requires_explicit_read_annotation(self):
        self.server('docs', [{'name': 'get', 'annotations': {'readOnlyHint': True}}, {'name': 'edit'}])
        self.tools.read_only = True
        self.assertTrue(self.tools.permits('mcp__docs__get'))
        self.assertFalse(self.tools.permits('mcp__docs__edit'))

    async def test_long_file_tail_can_be_read_without_shell(self):
        self.app.settings.workspace_dir.mkdir(parents=True, exist_ok=True)
        (self.app.settings.workspace_dir/'long.txt').write_text('a'*17000 + 'TAIL RESTRICTION', encoding='utf8')
        result = json.loads(await self.tools.execute('run', 'read_file', {'path': 'long.txt'}, 'c1'))
        self.assertTrue(result['truncated'])
        tail = json.loads(await self.tools.execute('run', 'read_file', {'path': 'long.txt', 'offset': result['next_offset']}, 'c2'))
        self.assertIn('TAIL RESTRICTION', tail['text'])
        self.assertIsNone(tail['next_offset'])

    async def test_spreadsheet_uncached_formula_is_not_silently_empty(self):
        from openpyxl import Workbook
        path = Path(self.tmp.name)/'sheet.xlsx'
        book = Workbook()
        book.active.append([3, 7, '=SUM(A1:B1)'])
        book.save(path)
        result = await FileProcessingService().extract(path)
        self.assertIn('=SUM(A1:B1)', result['text'])
        self.assertIn('missing (not evaluated)', result['text'])
        self.assertFalse(result['metadata']['formulas_evaluated'])

    def test_navigation_changes_are_excluded_but_body_changes_survive(self):
        before = '<html><nav>Old menu</nav><main><article>One result.</article></main><footer>Old footer</footer></html>'
        after = before.replace('Old menu', 'Entirely new navigation').replace('Old footer', 'New legal')
        self.assertEqual(extract_article(before)['text'], extract_article(after)['text'])
        self.assertTrue(semantic_diff_summary(extract_article(before)['text'], extract_article(after.replace('One result.', 'Two results.'))['text'])['changed'])
        self.assertEqual(extract_article('<div>A complete fallback</div>', content_type='text/html')['extraction_scope'], 'page_text_fallback')

    def test_citation_id_does_not_prove_claim(self):
        report = build_research_report('Invented conclusion [S1]', [{'sources': [{'id': 'S1', 'content_text': 'The measured number is 7.'}]}])
        self.assertTrue(report['claims'][0]['citation_resolved'])
        self.assertFalse(report['claims'][0]['supported'])
        self.assertTrue(report['claims'][0]['evidence'])

    async def test_finished_run_does_not_complete_unverified_task(self):
        conversation = await self.app.conversation_create({})
        task = await self.app.task_service.create(title='Deliver', goal='Verify two things', steps=[{'title': 'Read source'}, {'title': 'Publish output'}])
        run = await self.app._new_run(conversation['conversation']['id'], 'Do the work', 'normal', plan_task_id=task['id'])
        with patch('kore_next.application.ModelClient.complete', AsyncMock(return_value=ModelReply('Not done', None, [], {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}))):
            result = await self.app._execute_run(run)
        self.assertIsNone(result['error'])
        after = await self.app.task_service.get(task['id'])
        self.assertNotEqual(after['status'], 'completed')
        self.assertFalse(any(s['status'] == 'completed' for s in after['steps']))

    async def test_delivery_review_runs_once_and_remains_budgeted(self):
        conversation = await self.app.conversation_create({})
        run = await self.app._new_run(conversation['conversation']['id'], 'List local files', 'normal')
        usage = {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}
        replies = [ModelReply(None, None, [{'id': 'c1', 'name': 'list_files', 'arguments': {}}], usage), ModelReply('Listed', None, [], usage), ModelReply('Verified list', None, [], usage)]
        with patch('kore_next.application.ModelClient.complete', AsyncMock(side_effect=replies)) as model:
            result = await self.app._execute_run(run)
        self.assertIsNone(result['error'])
        self.assertEqual(model.await_count, 3)
        self.assertEqual(result['usage']['total_tokens'], 6)
        self.assertEqual(len(await self.app.db.fetchall("SELECT * FROM events WHERE type='delivery_review'")), 1)

    async def test_same_tool_route_distinct_conditions_remain_separate(self):
        service = self.app.skill_learning
        candidates = []
        for index, status in enumerate(['completed', 'blocked', 'completed', 'blocked']):
            run = {'id': f'r{index}', 'status': 'completed', 'mode': 'normal', 'user_message': 'When documents are ready complete; otherwise block. Outcome ' + status}
            for sequence, (kind, data) in enumerate([
                ('tool_call', {'tool_call': {'id': 'a', 'name': 'task_get', 'arguments': {'task_id': 'id'}}}),
                ('tool_result', {'tool_result': {'tool_call_id': 'a', 'success': True, 'output': '{}'}}),
                ('tool_call', {'tool_call': {'id': 'b', 'name': 'task_update', 'arguments': {'task_id': 'id', 'status': status}}}),
                ('tool_result', {'tool_result': {'tool_call_id': 'b', 'success': True, 'output': dumps({'status': status})}}),
            ], 1):
                await self.app.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?)', (f'e{index}-{sequence}', run['id'], sequence, kind, now(), dumps(data)))
            candidates.append(await service.learn(run))
        self.assertEqual(candidates[0]['id'], candidates[2]['id'])
        self.assertEqual(candidates[1]['id'], candidates[3]['id'])
        self.assertNotEqual(candidates[0]['id'], candidates[1]['id'])
        self.assertTrue(all(candidates[i]['success_count'] == 2 for i in [2, 3]))

    def test_generic_read_only_detection_does_not_depend_on_subject(self):
        for prompt in ['全程只读检查保险资料', 'read-only review of telescope inventory']:
            self.assertTrue(explicitly_read_only(prompt))
        self.assertFalse(explicitly_read_only('Create a report about telescopes'))
