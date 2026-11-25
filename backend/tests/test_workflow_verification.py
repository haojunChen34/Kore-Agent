import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from kore_next.application import Application
from kore_next.config import Settings
from kore_next.util import now


class WorkflowVerificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.app = Application(Settings(data_dir=root/'data', workspace_dir=root/'workspace', user_skills_dir=root/'skills'))
        await self.app.db.initialize()
        self.app.broadcast = AsyncMock()
        self.app.connector_service.verify_external_operation = AsyncMock(return_value={'status': 'confirmed', 'provider': 'notes'})

    async def asyncTearDown(self):
        self.app.db.release_lock()
        self.tmp.cleanup()

    async def operation(self, key, tool, args, status):
        await self.app.prepare_external_operation(None, key, tool, {**args, 'idempotency_key': key}, workflow_id='workflow')
        await self.app.db.update_external_operation(key, status=status, error='old response lost', updated_at=now())

    async def seed(self, second='uncertain', extra=None):
        steps = [{'id': key, 'tool': 'feishu_document_append', 'arguments': {'document_id': 'doc', 'content': key}} for key in ['A', 'B']]
        if extra:
            steps.append({'id': 'C', 'tool': 'feishu_document_append', 'arguments': {'document_id': 'doc', 'content': 'C'}})
        await self.operation('root', 'connector_workflow', {'steps': steps}, 'partial_success')
        await self.operation('root:step:A', 'feishu_document_append', steps[0]['arguments'], 'succeeded')
        await self.operation('root:step:B', 'feishu_document_append', steps[1]['arguments'], second)
        if extra and extra != 'missing':
            await self.operation('root:step:C', 'feishu_document_append', steps[2]['arguments'], extra)

    async def test_read_only_verification_completes_parent_and_preserves_step_results(self):
        await self.seed()
        await self.app.external_operation_verify({'operation_id': 'root:step:B'})
        root = await self.app._operation_get('root')
        self.assertEqual(root['status'], 'succeeded')
        self.assertIsNone(root['error'])
        self.assertEqual([s['status'] for s in root['result']['steps']], ['succeeded', 'succeeded'])
        self.assertEqual(root['result']['steps'][1]['result']['verification']['status'], 'confirmed')
        self.assertEqual((await self.app.db.fetchone("SELECT status FROM external_workflows WHERE id='workflow'"))['status'], 'completed')
        self.app.connector_service.verify_external_operation.assert_awaited_once()

    async def test_missing_or_unfinished_steps_prevent_completion(self):
        for extra in ['missing', 'failed', 'awaiting_approval']:
            with self.subTest(extra=extra):
                await self.app.db.execute('DELETE FROM external_operations')
                await self.seed(extra=extra)
                await self.app.external_operation_verify({'operation_id': 'root:step:B'})
                self.assertEqual((await self.app._operation_get('root'))['status'], 'partial_success')

    async def test_inconclusive_lookup_does_not_promote_child_or_parent(self):
        await self.seed()
        self.app.connector_service.verify_external_operation.return_value = {'status': 'not_found'}
        await self.app.external_operation_verify({'operation_id': 'root:step:B'})
        self.assertEqual((await self.app._operation_get('root:step:B'))['status'], 'uncertain')
        self.assertEqual((await self.app._operation_get('root'))['status'], 'partial_success')

    async def test_retry_lineage_resolves_only_its_own_root(self):
        await self.seed(second='failed')
        await self.operation('retry-B', 'feishu_document_append', {'document_id': 'doc', 'content': 'B', '_retry_of': 'root:step:B'}, 'uncertain')
        await self.operation('other', 'connector_workflow', {'steps': [{'id': 'missing', 'tool': 'email_send', 'arguments': {}}]}, 'partial_success')
        await self.app.external_operation_verify({'operation_id': 'retry-B'})
        self.assertEqual((await self.app._operation_get('root'))['status'], 'succeeded')
        self.assertEqual((await self.app._operation_get('other'))['status'], 'partial_success')
        self.assertEqual((await self.app._operation_get('root:step:B'))['status'], 'failed')

    async def test_parent_update_failure_rolls_back_child_status(self):
        await self.seed()
        await self.app.db.execute("CREATE TRIGGER block_parent BEFORE UPDATE OF status ON external_operations WHEN NEW.idempotency_key='root' AND NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaisesRegex(Exception, 'injected'):
            await self.app.external_operation_verify({'operation_id': 'root:step:B'})
        self.assertEqual((await self.app._operation_get('root:step:B'))['status'], 'uncertain')
        self.assertEqual((await self.app._operation_get('root'))['status'], 'partial_success')

    async def test_active_denied_and_ambiguous_roots_are_not_promoted(self):
        for status in ['running', 'denied', 'awaiting_approval']:
            with self.subTest(status=status):
                await self.app.db.execute('DELETE FROM external_operations')
                await self.seed()
                await self.app.db.update_external_operation('root', status=status, updated_at=now())
                await self.app.external_operation_verify({'operation_id': 'root:step:B'})
                self.assertEqual((await self.app._operation_get('root'))['status'], status)
        await self.app.db.execute('DELETE FROM external_operations')
        await self.seed(second='failed')
        for key in ['retry-1', 'retry-2']:
            await self.operation(key, 'feishu_document_append', {'document_id': 'doc', 'content': key, '_retry_of': 'root:step:B'}, 'uncertain')
        await self.app.external_operation_verify({'operation_id': 'retry-1'})
        self.assertEqual((await self.app._operation_get('root'))['status'], 'partial_success')
