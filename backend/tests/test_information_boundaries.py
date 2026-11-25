import asyncio
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import test_workbench_consistency as fixtures

from kore_next.external_information import analysis_prompt
from kore_next.server import trusted_local_origin
from kore_next.util import dumps


class InformationBoundariesTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.WorkbenchConsistencyTests.asyncSetUp
    asyncTearDown = fixtures.WorkbenchConsistencyTests.asyncTearDown
    source = fixtures.WorkbenchConsistencyTests.source
    change_source = fixtures.WorkbenchConsistencyTests.change_source

    async def test_cross_provider_counts_keep_mailbox_window_and_handled_filter(self):
        _, info = await self.source()
        await self.app.db.execute('UPDATE connector_connections SET data_json=?',
                                  (dumps({'provider': 'email', 'sync_days': 30}),))
        await self.app.db.execute('UPDATE external_information SET status=?,analysis_json=? WHERE id=?',
                                  ('needs_review', dumps({'category': 'action_required', 'importance': 'high',
                                    'needs_reply': True, 'suggested_actions': [{'title': 'Reply', 'action_type': 'create_action'}]}), info['id']))
        for provider in ('email', 'calendar', 'todo', 'notes', 'feishu', None):
            with self.subTest(provider=provider):
                result = await self.app.external_information_list({'provider': provider})
                self.assertEqual(result['counts']['email'], 1)
                self.assertEqual(len(result['items']), 1 if provider in ('email', None) else 0)
        await self.app.external_information_ignore({'information_id': info['id']})
        for provider in ('email', 'calendar', 'todo', 'notes'):
            self.assertEqual((await self.app.external_information_list({'provider': provider}))['counts']['email'], 0)

    async def test_history_source_link_survives_completion_and_reports_deletion(self):
        source, _ = await self.source()
        action = (await self.app.tool_call('action_create', {'title': 'Follow up', 'source_type': 'connector_item', 'source_id': source['id']}, run_id=None, mode='normal'))['action']
        await self.change_source(source, is_done=True)
        detail = await self.app.action_get({'action_id': action['id']})
        self.assertEqual(detail['sources'][0]['id'], source['id'])
        self.assertTrue(detail['sources'][0]['found'])
        await self.app.db.execute("DELETE FROM connector_items WHERE id=?", (source['id'],))
        self.assertFalse((await self.app.action_get({'action_id': action['id']}))['sources'][0]['found'])

    async def test_reanalysis_does_not_execute_or_duplicate_accepted_action(self):
        _, info = await self.source()
        accepted = await self.app.external_information_accept({'information_id': info['id'], 'actions': [{'action_type': 'create_action', 'title': 'Follow up', 'action_target': 'local_action'}]})
        self.assertTrue((await self.app.action_get({'action_id': accepted['actions'][0]['id']}))['sources'][0]['found'])
        before = [dict(r) for r in await self.app.db.fetchall('SELECT * FROM actions')]
        await self.app.external_information_reanalyze({'information_id': info['id']})
        if self.app.background_tasks:
            await asyncio.gather(*self.app.background_tasks)
        self.assertEqual(before, [dict(r) for r in await self.app.db.fetchall('SELECT * FROM actions')])
        self.assertEqual(await self.app.db.fetchall('SELECT * FROM approvals'), [])

    async def test_today_calendar_uses_user_day_not_utc_day(self):
        items = [
            {'id': 'later', 'starts_at': '2030-01-01T16:30:00Z'},
            {'id': 'tomorrow', 'starts_at': '2030-01-02T16:30:00Z'},
            {'id': 'past', 'starts_at': '2030-01-01T15:00:00Z'},
            {'id': 'started', 'starts_at': '2030-01-01T16:00:00Z'},
            {'id': 'cancelled', 'starts_at': '2030-01-01T17:00:00Z', 'status': 'cancelled'},
        ]
        self.app.connector_service.inbox = AsyncMock(return_value={'items': items})
        self.app.connector_health = AsyncMock(return_value={'connections': []})
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromisoformat('2030-01-02T00:10:00+08:00').astimezone(tz)
        with patch('kore_next.application.datetime', Clock):
            context = await self.app._daily_brief_context({}, {'timezone': 'Asia/Shanghai'})
        self.assertEqual([i['id'] for i in context['upcoming_calendar_events']], ['later'])

    async def test_late_analysis_cannot_replace_new_source_version(self):
        source, info = await self.source()
        self.app._role_model_config = AsyncMock(return_value={'provider': 'test'})
        self.app.broadcast = AsyncMock()
        async def reply(*args, **kwargs):
            await self.app.db.execute("UPDATE external_information SET content_fingerprint='newer',analysis_json=? WHERE id=?", (dumps({'summary': 'New source'}), info['id']))
            from types import SimpleNamespace
            return SimpleNamespace(content=dumps({'summary': 'Late old source'}))
        with patch('kore_next.application.ModelClient.complete', side_effect=reply):
            result = await self.app.external_information_chat({'information_id': info['id'], 'message': 'Adjust'})
        row = await self.app.db.fetchone('SELECT analysis_json FROM external_information WHERE id=?', (info['id'],))
        self.assertIn('New source', row['analysis_json'])
        self.assertTrue(result['stale'])
        self.assertEqual(result['analysis']['summary'], 'New source')
        self.app.broadcast.assert_not_awaited()

    async def test_analysis_deleted_while_model_waits_returns_clear_error(self):
        _, info = await self.source()
        self.app._role_model_config = AsyncMock(return_value={'provider': 'test'})
        self.app.broadcast = AsyncMock()
        async def reply(*args, **kwargs):
            await self.app.db.execute('DELETE FROM external_information WHERE id=?', (info['id'],))
            from types import SimpleNamespace
            return SimpleNamespace(content=dumps({'summary': 'Late result'}))
        from kore_next.application import RpcFault
        with patch('kore_next.application.ModelClient.complete', side_effect=reply):
            with self.assertRaisesRegex(RpcFault, '未保存'):
                await self.app.external_information_chat({'information_id': info['id'], 'message': 'Adjust'})
        self.app.broadcast.assert_not_awaited()

    def test_source_date_is_separate_from_analysis_date(self):
        prompt = analysis_prompt({'sent_at': '2030-03-04T09:00:00+09:00', 'received_at': '2030-03-06T09:00:00Z', 'body': 'Tomorrow at 18:00', '_analysis_timezone': 'Asia/Tokyo'})
        self.assertIn('2030-03-04T09:00:00+09:00', prompt)
        self.assertIn('不能替代原文日期', prompt)

    def test_remote_browser_origin_cannot_access_download_capability(self):
        for origin in ('https://example.com', 'https://localhost.example.com', 'https://localhost@example.com'):
            self.assertFalse(trusted_local_origin(origin))
        for origin in ('http://localhost:5173', 'http://127.0.0.1:15177', 'null', None):
            self.assertTrue(trusted_local_origin(origin))
