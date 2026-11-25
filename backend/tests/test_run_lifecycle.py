import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

from kore_next.application import Application, RpcFault
from kore_next.config import Settings
from kore_next.run_lifecycle import claim_recovery
from kore_next.util import dumps, now


class RunLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.app = Application(Settings(data_dir=root/'data', workspace_dir=root/'A', user_skills_dir=root/'skills'))
        await self.app.db.initialize()
        self.app.broadcast = AsyncMock()
        self.app.permission_rules.set_signing_key(b'x'*32)

    async def asyncTearDown(self):
        for task in self.app.running.values():
            task.cancel()
        if self.app.running:
            await asyncio.gather(*self.app.running.values(), return_exceptions=True)
        self.app.db.release_lock()
        self.tmp.cleanup()

    async def seed(self, status='interrupted'):
        run = await self.app._new_run(None, 'create one action after reading', 'normal')
        run['status'] = status
        await self.app._save_run(run)
        await self.app.checkpoints.save(run_id=run['id'], conversation_id=None, sequence=3,
                                        state={'messages':[{'role':'tool','content':'A budget 800'}], 'marker':'original'})
        return run

    async def approval(self, run, operation=None):
        a = {'id':'card', 'run_id':run['id'], 'tool_name':'email_send', 'status':'pending',
             'arguments':{'to':'fixture@example.invalid', **({'external_operation_id':operation['id']} if operation else {})}}
        await self.app.db.execute('INSERT INTO approvals VALUES(?,?,?,?)', ('card', dumps(a), 'pending', now()))
        return a

    async def test_concurrent_recovery_creates_one_chain_and_side_effect(self):
        parent = await self.seed()
        async def execute(run, state):
            self.assertEqual(state['marker'], 'original')
            action = await self.app.personal_service.action_create('review materials', source='test')
            run['status']='completed'
            await self.app._save_run(run)
            return action
        self.app._execute_run = execute
        results = await asyncio.gather(*(self.app.run_recover({'run_id':parent['id']}) for _ in range(8)), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertEqual(sum(isinstance(r, RpcFault) for r in results), 7)
        rows = await self.app.db.fetchall('SELECT * FROM runs WHERE recovered_from_run_id=?',(parent['id'],))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM actions')), 1)
        self.assertEqual((await self.app.checkpoints.get(parent['id']))['state']['marker'],'original')

    async def test_workspace_mismatch_then_switch_back(self):
        run = await self.seed()
        other = Path(self.tmp.name)/'B'
        other.mkdir()
        with self.assertRaisesRegex(RpcFault, '原工作区'):
            await claim_recovery(self.app.db, run['id'], other)
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM runs')),1)
        child,_ = await claim_recovery(self.app.db, run['id'], self.app.settings.workspace_dir)
        self.assertEqual(child['recovered_from_run_id'],run['id'])

    async def test_legacy_unknown_workspace_is_explicit(self):
        run=await self.seed()
        await self.app.db.execute('DELETE FROM run_context WHERE run_id=?',(run['id'],))
        with self.assertRaisesRegex(RpcFault,'未记录工作区'):
            await self.app.run_recover({'run_id':run['id']})

    async def test_recovery_transaction_rolls_back_all_records(self):
        run=await self.seed()
        await self.app.db.execute("CREATE TRIGGER reject_recovery BEFORE INSERT ON run_recoveries BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(Exception):
            await claim_recovery(self.app.db,run['id'],self.app.settings.workspace_dir)
        for table in ('runs','run_context','checkpoints'):
            self.assertEqual(len(await self.app.db.fetchall(f'SELECT * FROM {table}')),1)
        await self.app.db.execute('DROP TRIGGER reject_recovery')
        await claim_recovery(self.app.db,run['id'],self.app.settings.workspace_dir)

    async def test_durable_claim_survives_application_replacement(self):
        run=await self.seed()
        child,_=await claim_recovery(self.app.db,run['id'],self.app.settings.workspace_dir)
        second=Application(self.app.settings)
        try:
            with self.assertRaises(RpcFault):
                await second.run_recover({'run_id':run['id']})
            await second.db.execute("UPDATE runs SET status='interrupted' WHERE id=?",(child['id'],))
            grandchild,_=await claim_recovery(second.db,child['id'],second.settings.workspace_dir)
            self.assertEqual(grandchild['recovered_from_run_id'],child['id'])
        finally:
            second.db.release_lock()

    async def test_pending_run_blocks_workspace_change(self):
        await self.seed('pending')
        other=Path(self.tmp.name)/'B'
        other.mkdir()
        with self.assertRaises(RpcFault):
            await self.app.workspace_set({'path':str(other)})

    async def test_cancel_invalidates_approval_and_prevents_rule_creation(self):
        run=await self.seed('running')
        operation=await self.app.prepare_external_operation(run['id'],'call','email_send',{'to':'fixture@example.invalid'})
        await self.approval(run,operation)
        await self.app.run_cancel({'run_id':run['id']})
        for scope in ('once','global'):
            with self.assertRaises(RpcFault):
                await self.app._resolve_approval('card',True,scope=scope)
        self.assertEqual((await self.app._approval_payload('card',redact_output=False))['status'],'cancelled')
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM permission_rules')),0)
        self.assertEqual((await self.app._operation_get(operation['id']))['status'],'denied')

    async def test_active_approval_still_resolves_once_and_creates_rule(self):
        run=await self.seed('running')
        await self.approval(run)
        results=await asyncio.gather(*(self.app._resolve_approval('card',True,scope='global') for _ in range(3)),return_exceptions=True)
        self.assertEqual(sum(isinstance(r,RpcFault) for r in results),2)
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM permission_rules')),1)
        self.assertEqual((await self.app._approval_payload('card',redact_output=False))['status'],'approved')

    async def test_explicit_refusal_after_interrupt_is_preserved_for_recovery(self):
        run = await self.seed('running')
        await self.approval(run)
        await self.app.run_interrupt({'run_id': run['id']})
        with self.assertRaises(RpcFault):
            await self.app._resolve_approval('card', True)
        result = await self.app._resolve_approval('card', False)
        self.assertEqual(result['approval']['status'], 'denied')
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM permission_rules')), 0)
        _, state = await claim_recovery(self.app.db, run['id'], self.app.settings.workspace_dir)
        self.assertIn('email_send', state['model_messages'][-1]['content'])
        self.assertIn('user denied', state['model_messages'][-1]['content'])

    async def test_approval_and_rule_rollback_together(self):
        run=await self.seed('running')
        await self.approval(run)
        await self.app.db.execute("CREATE TRIGGER reject_card BEFORE UPDATE ON approvals BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(Exception):
            await self.app._resolve_approval('card',True,scope='global')
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM permission_rules')),0)
        self.assertEqual((await self.app._approval_payload('card',redact_output=False))['status'],'pending')

    async def test_cancel_racing_with_new_approval_leaves_no_pending_card(self):
        run=await self.seed('running')
        task=asyncio.create_task(self.app._request_approval(run['id'],'call',{'tool_name':'email_send'},'test'))
        await self.app.run_cancel({'run_id':run['id']})
        self.assertFalse(await asyncio.wait_for(task,2))
        self.assertEqual(len(await self.app.db.fetchall("SELECT * FROM approvals WHERE status='pending'")),0)

    async def test_approval_after_stop_does_not_dispatch(self):
        run=await self.seed('running')
        operation=await self.app.prepare_external_operation(run['id'],'call','email_send',{'to':'fixture@example.invalid'})
        await self.approval(run,operation)
        await self.app._resolve_approval('card',True)
        await self.app.run_cancel({'run_id':run['id']})
        self.app.connector_service.send_email=AsyncMock()
        with self.assertRaises(RuntimeError):
            await self.app.tool_call('email_send',{'external_operation_id':operation['id'],'to':'fixture@example.invalid'},run_id=run['id'],mode='normal')
        self.app.connector_service.send_email.assert_not_called()

    async def test_cancel_during_external_dispatch_keeps_uncertain_state(self):
        run=await self.seed('running')
        operation=await self.app.prepare_external_operation(run['id'],'call','email_send',{'to':'fixture@example.invalid'})
        await self.app.resolve_external_operation(operation['id'],approved=True)
        dispatched=asyncio.Event()
        async def remote(**kwargs):
            dispatched.set()
            await asyncio.Future()
        self.app.connector_service.send_email=remote
        task=asyncio.create_task(self.app.tool_call('email_send',{'external_operation_id':operation['id'], 'to':'fixture@example.invalid',
                                                               'subject':'test','body':'test'},run_id=run['id'],mode='normal'))
        self.app.running[run['id']]=task
        await asyncio.wait_for(dispatched.wait(),2)
        await self.app.run_cancel({'run_id':run['id']})
        self.assertEqual((await self.app._operation_get(operation['id']))['status'],'uncertain')

    async def test_cancelled_run_cannot_be_restarted_by_delayed_task(self):
        run=await self.seed('pending')
        await self.app.run_cancel({'run_id':run['id']})
        with self.assertRaises(RpcFault):
            await self.app._execute_run(run)
        self.assertEqual((await self.app.run_get({'run_id':run['id']}))['run']['status'],'cancelled')

    async def test_workspace_guard_does_not_allow_delayed_old_run(self):
        run=await self.seed('pending')
        other=Path(self.tmp.name)/'B'
        other.mkdir()
        self.app.settings=replace(self.app.settings,workspace_dir=other)
        with self.assertRaisesRegex(RpcFault,'工作区'):
            await self.app._execute_run(run)


if __name__ == '__main__':
    unittest.main()
