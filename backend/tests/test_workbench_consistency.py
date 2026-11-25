import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from kore_next.application import Application, RpcFault
from kore_next.config import Settings
from kore_next.external_information import default_analysis, fingerprint
from kore_next.real_connectors import FeishuClient
from kore_next.util import dumps, loads, now


class WorkbenchConsistencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        root=Path(self.tmp.name)
        self.app=Application(Settings(data_dir=root/'data',workspace_dir=root/'workspace',user_skills_dir=root/'skills'))
        await self.app.db.initialize()
        self.app._role_model_config=AsyncMock(return_value=None)

    async def asyncTearDown(self):
        if self.app.background_tasks:
            await asyncio.gather(*self.app.background_tasks,return_exceptions=True)
        self.app.db.release_lock()
        self.tmp.cleanup()

    async def plan(self):
        task=await self.app.task_service.create(title='Project',goal='review',plan_id='plan-a',steps=[
            {'id':'one','stage_id':'stage-a','title':'First review'},{'id':'two','stage_id':'stage-b','title':'Second review'}])
        first=await self.app.personal_service.action_create('Review',plan_id='plan-a',stage_id='stage-a')
        second=await self.app.personal_service.action_create('Review',plan_id='plan-a',stage_id='stage-b')
        unrelated=await self.app.personal_service.action_create('Review',plan_id='plan-b',stage_id='stage-a')
        return task,first,second,unrelated

    async def test_action_completion_and_reopening_sync_exact_stage(self):
        task,first,second,unrelated=await self.plan()
        await self.app.action_complete({'action_id':first['id']})
        updated=await self.app.task_service.get(task['id'])
        self.assertEqual([s['status'] for s in updated['steps']],['completed','pending'])
        self.assertEqual(updated['status'],'active')
        await self.app.action_complete({'action_id':first['id']})
        self.assertEqual((await self.app.task_service.get(task['id']))['revision'],updated['revision'])
        await self.app.action_update({'action_id':first['id'],'status':'next'})
        self.assertEqual((await self.app.task_service.get(task['id']))['steps'][0]['status'],'pending')
        for action in [first,second,unrelated]:
            value=loads((await self.app.db.fetchone('SELECT data_json FROM actions WHERE id=?',(action['id'],)))['data_json'],{})
            self.assertEqual(value['status'],'next')
            self.assertIsNone(value['completed_at'])

    async def test_task_step_completion_and_reopening_sync_exact_action(self):
        task,first,second,unrelated=await self.plan()
        await self.app.task_service.complete_step(task['id'],'two')
        actions={a['id']:a for a in await self.app.personal_service.actions()}
        self.assertEqual(actions[second['id']]['status'],'done')
        self.assertEqual(actions[first['id']]['status'],'next')
        self.assertEqual(actions[unrelated['id']]['status'],'next')
        await self.app.task_service.update(task['id'],{'steps':[{'id':'two','status':'pending'}]})
        actions={a['id']:a for a in await self.app.personal_service.actions()}
        self.assertEqual(actions[second['id']]['status'],'next')

    async def test_same_title_stages_keep_their_distinct_bindings(self):
        task=await self.app.task_service.create(title='Repeated review',goal='two rounds',plan_id='plan',steps=[
            {'id':'one','stage_id':'first','title':'Review'},{'id':'two','stage_id':'second','title':'Review'}])
        self.assertEqual(len(task['steps']),2)
        await self.app.task_service.complete_step(task['id'],'one')
        await self.app.task_service.complete_step(task['id'],'two')
        updated=await self.app.task_service.update(task['id'],{'steps':[{'id':'one','status':'pending'}]})
        self.assertEqual(updated['status'],'active')
        self.assertIsNone(updated['completed_at'])

    async def test_unrelated_notification_merge_preserves_event_count(self):
        first=await self.app.personal_service.notify('Monitor','First change',entity_id='monitor',dedupe_key='event1',group_key='monitor',metadata={'changes':['first']})
        second=await self.app.personal_service.notify('Monitor','Second change',entity_id='monitor',dedupe_key='event2',group_key='monitor',metadata={'changes':['second']})
        self.assertEqual(first['id'],second['id'])
        self.assertEqual(second['count'],2)
        self.assertEqual(second['entity_ids'],['monitor'])
        self.assertIn('First change',second['body'])
        self.assertIn('Second change',second['body'])
        self.assertEqual(second['metadata']['changes'],['first','second'])

    async def test_multiple_actions_require_all_to_complete_stage(self):
        task,first,_,_=await self.plan()
        sibling=await self.app.personal_service.action_create('Different subtask',plan_id='plan-a',stage_id='stage-a')
        await self.app.personal_service.action_complete(first['id'])
        self.assertEqual((await self.app.task_service.get(task['id']))['steps'][0]['status'],'pending')
        await self.app.personal_service.action_complete(sibling['id'])
        self.assertEqual((await self.app.task_service.get(task['id']))['steps'][0]['status'],'completed')

    async def test_concurrent_different_stage_updates_preserve_both(self):
        task,first,_,_=await self.plan()
        await asyncio.gather(self.app.personal_service.action_complete(first['id']),self.app.task_service.complete_step(task['id'],'two'))
        result=await self.app.task_service.get(task['id'])
        self.assertEqual([s['status'] for s in result['steps']],['completed','completed'])
        self.assertEqual(result['status'],'completed')

    async def test_sync_failure_rolls_back_origin(self):
        _,first,_,_=await self.plan()
        await self.app.db.execute("CREATE TRIGGER reject_task BEFORE UPDATE ON tasks BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(Exception):
            await self.app.personal_service.action_complete(first['id'])
        value=await self.app.db.fetchone('SELECT status FROM actions WHERE id=?',(first['id'],))
        self.assertEqual(value['status'],'next')

    async def test_merged_notice_resolves_only_matching_typed_entity(self):
        action=await self.app.personal_service.action_create('Review')
        entries=[{'entity_type':'action','entity_id':action['id'],'body':'Local review pending'},
                 {'entity_type':'connector_item','entity_id':action['id'],'body':'Remote review pending'}]
        notice=await self.app.personal_service.notify('Deadlines','',kind='deadline',group_key='deadlines',entities=entries)
        await self.app.action_complete({'action_id':action['id']})
        result=loads((await self.app.db.fetchone('SELECT data_json FROM notifications WHERE id=?',(notice['id'],)))['data_json'],{})
        self.assertEqual(result['body'],'Remote review pending')
        self.assertEqual(result['count'],1)
        self.assertEqual(result['entity_refs'],[{'type':'connector_item','id':action['id']}])

    async def test_completed_action_not_reminded_by_stale_scan_or_brief(self):
        action=await self.app.personal_service.action_create('Prepare',due_at='2000-01-01T00:00:00Z')
        job={'action':{'type':'deadline_scan'}}
        result=await self.app._execute_proactive_action(job)
        self.assertEqual(result['count'],1)
        await self.app.action_complete({'action_id':action['id']})
        result=await self.app._execute_proactive_action(job)
        self.assertEqual(result['count'],0)
        stale=await self.app.personal_service.notify('Due','Prepare still pending',kind='action_due',entity_id=action['id'])
        self.assertEqual(stale['status'],'resolved')
        self.assertNotIn('still pending',stale['body'])
        self.assertEqual((await self.app.personal_service.today())['notifications'],[])

    async def source(self, kind='email'):
        item={'id':'source','source_id':'remote-source','provider':kind,'kind':kind,'title':'Please review',
              'body':'Please reply to this invitation.','sender':'person@example.invalid','updated_at':now(),'is_done':False}
        await self.app.db.execute('INSERT INTO connector_connections VALUES(?,?,?,?,?,?)',('connection',kind,'{}','connected',now(),now()))
        await self.app.db.execute('INSERT INTO connector_items(id,connection_id,provider,source_id,kind,data_json,status,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                                  ('source','connection',kind,'remote-source',kind,dumps(item),'active',now()))
        await self.app._index_external_information({'provider':kind,'source_id':'remote-source','item_id':'source','item':item})
        await asyncio.gather(*self.app.background_tasks)
        row=await self.app.db.fetchone('SELECT * FROM external_information')
        return item,dict(row)

    async def change_source(self,item,**changes):
        item={**item,**changes}
        await self.app.db.execute('UPDATE connector_items SET data_json=? WHERE id=?',(dumps(item),item['id']))
        return item

    async def test_missing_source_blocks_acceptance_without_replacement(self):
        _,info=await self.source()
        await self.app.db.execute("DELETE FROM connector_items WHERE id='source'")
        with self.assertRaisesRegex(RpcFault,'来源'):
            await self.app.external_information_accept({'information_id':info['id']})
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM approvals')),0)
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM actions')),0)

    async def make_source_approval(self):
        item,info=await self.source()
        action={'title':'Reply','description':'Confirmed','action_type':'external_operation','action_target':'email_reply'}
        approval=await self.app._create_external_information_approval(info['id'],action,item,loads(info['analysis_json'],{}),0)
        return item,info,approval

    async def test_changed_source_invalidates_old_card(self):
        item,_,approval=await self.make_source_approval()
        await self.change_source(item,sender='other@example.invalid')
        self.app.connector_service.send_email=AsyncMock()
        with self.assertRaisesRegex(RpcFault,'来源'):
            await self.app._resolve_approval(approval['id'],True)
        self.assertEqual((await self.app._approval_payload(approval['id'],redact_output=False))['status'],'cancelled')
        self.app.connector_service.send_email.assert_not_called()

    async def test_deletion_between_approval_and_dispatch_blocks_remote(self):
        _,_,approval=await self.make_source_approval()
        operation_id=approval['arguments']['external_operation_id']
        await self.app.resolve_external_operation(operation_id,approved=True)
        await self.app.db.execute("DELETE FROM connector_items WHERE id='source'")
        self.app.connector_service.send_email=AsyncMock()
        with self.assertRaisesRegex(RpcFault,'来源'):
            await self.app.tool_call('email_send',approval['arguments'],run_id=None,mode='normal')
        self.app.connector_service.send_email.assert_not_called()

    async def test_unchanged_source_can_execute_and_read_flag_does_not_invalidate(self):
        item,_,approval=await self.make_source_approval()
        await self.change_source(item,is_read=True)
        self.app.connector_service.send_email=AsyncMock(return_value={'message_id':'sent'})
        await self.app._resolve_approval(approval['id'],True)
        self.app.connector_service.send_email.assert_awaited_once()
        self.assertNotIn('_source_guard',self.app.connector_service.send_email.call_args.kwargs)

    async def test_completed_source_invalidates_pending_analysis(self):
        item,info=await self.source('todo')
        item=await self.change_source(item,is_done=True)
        await self.app._index_external_information({'provider':'todo','source_id':item['source_id'],'item_id':item['id'],'item':item})
        await asyncio.gather(*self.app.background_tasks)
        row=await self.app.db.fetchone('SELECT * FROM external_information WHERE id=?',(info['id'],))
        analysis=loads(row['analysis_json'],{})
        self.assertEqual(analysis['suggested_actions'],[])
        self.assertIn('已完成',analysis['summary'])
        self.assertNotEqual(fingerprint({**item,'is_done':False}),fingerprint(item))

    async def test_completion_mapping_and_sync_merge(self):
        client=object.__new__(FeishuClient)
        client.request=AsyncMock(return_value={'data':{'items':[{'guid':'remote-task','summary':'Review','completed_at':'1790000000000'}]}})
        records=await client._task_records('fixture',limit=100)
        self.assertTrue(records[0]['is_done'])
        service=self.app.connector_service
        connection={'id':'connection','provider':'todo','adapter':'feishu_task','status':'connected'}
        service.feishu=client
        await self.app.db.execute('INSERT INTO connector_connections VALUES(?,?,?,?,?,?)',('connection','todo',dumps(connection),'connected',now(),now()))
        old={**records[0],'is_done':False}
        service._real_feishu_records=AsyncMock(return_value=[old])
        await service._sync_connection(connection)
        service._real_feishu_records=AsyncMock(return_value=copy.deepcopy(records))
        update=await service._sync_connection(connection)
        self.assertEqual(update['updated'],1)
        row=await self.app.db.fetchone('SELECT data_json FROM connector_items')
        self.assertTrue(loads(row['data_json'],{})['is_done'])
        again=await service._sync_connection(connection)
        self.assertEqual(again['updated'],0)

    async def test_unsupported_connector_arguments_reject_before_approval(self):
        for tool,args in [('feishu_task_create',{'title':'Review','labels':['work']}),
                          ('connector_workflow',{'steps':[{'tool':'feishu_task_create','arguments':{'title':'Review','labels':['work']}}]})]:
            with self.assertRaisesRegex(RpcFault,'不支持标签'):
                await self.app.prepare_external_operation(None,'call',tool,args)
        self.assertEqual(len(await self.app.db.fetchall('SELECT * FROM external_operations')),0)
        preview=await self.app._external_preview('feishu_task_create',{'title':'Review','labels':['work']})
        self.assertEqual(preview['labels'],['work'])
        self.assertTrue(preview['labels_support'])
        supported=await self.app.prepare_external_operation(None,'call','feishu_task_create',{'title':'Review'})
        self.assertEqual(supported['status'],'awaiting_approval')

    async def test_fingerprint_ignores_read_state_but_covers_target_and_deadline(self):
        item={'title':'Review','body':'Please reply','sender':'first@example.invalid','due_at':'2026-11-27'}
        self.assertEqual(fingerprint(item),fingerprint({**item,'is_read':True,'updated_at':now()}))
        self.assertNotEqual(fingerprint(item),fingerprint({**item,'sender':'second@example.invalid'}))
        self.assertNotEqual(fingerprint(item),fingerprint({**item,'due_at':'2026-12-02'}))
        self.assertEqual(default_analysis({'kind':'todo','title':'Review','is_done':True})['suggested_actions'],[])


if __name__=='__main__':
    unittest.main()
