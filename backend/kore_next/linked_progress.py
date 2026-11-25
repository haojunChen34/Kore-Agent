"""Synchronize explicit plan/stage identities in the caller's transaction."""
from datetime import UTC, datetime, timedelta
from typing import Any

from .util import dumps, loads, now, uid


async def create_notice(database: Any, title: str, body: str, *, kind: str, entity_id: str | None,
                        dedupe_key: str | None, group_key: str | None, merge_window_seconds: int,
                        metadata: dict[str, Any] | None, priority: str, entity_type: str | None,
                        entities: list[dict[str, Any]] | None) -> dict[str, Any]:
    stamp = now()
    identity = entity_type or ("action" if kind == "action_due" else "notification_source")
    entries = [dict(entry, resolved=False) for entry in entities or []]
    if not entries:
        entries = [{"entity_type":identity, "entity_id":entity_id, "entry_key":dedupe_key or uid(), "body":body.strip(), "resolved":False}]
    async with database.connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        existing = None
        if dedupe_key or group_key:
            since = (datetime.now(UTC) - timedelta(seconds=max(1, int(merge_window_seconds)))).isoformat()
            rows = await (await db.execute("SELECT data_json FROM notifications WHERE status='unread' AND created_at>? ORDER BY created_at DESC LIMIT 50", (since,))).fetchall()
            for row in rows:
                candidate = loads(row["data_json"], {})
                if (dedupe_key and candidate.get("dedupe_key") == dedupe_key) or (group_key and candidate.get("group_key") == group_key):
                    existing = candidate
                    break
        item = existing or {"id":uid(), "created_at":stamp, "status":"unread", "read_at":None, "dismissed_at":None,
                            "kind":kind, "entity_id":entity_id, "dedupe_key":dedupe_key, "group_key":group_key, "metadata":{}}
        prior = list(item.get("entries") or [])
        if existing and not prior and item.get("body"):
            prior = [{"entity_type":"notification_source", "entity_id":item.get("entity_id") or item["id"], "body":item["body"], "resolved":False}]
        def key(entry: dict[str, Any]) -> tuple[str, Any]:
            return (entry["entity_type"], entry.get("entry_key") or entry["entity_id"])
        by_id = {key(entry):entry for entry in prior}
        for entry in entries:
            # Reject stale reminder creation after a completion transaction.
            if entry["entity_type"] == "action":
                action = await (await db.execute("SELECT status FROM actions WHERE id=?", (entry["entity_id"],))).fetchone()
                entry["resolved"] = bool(action and action["status"] in {"done", "cancelled"})
            elif entry["entity_type"] == "connector_item":
                source = await (await db.execute("SELECT status,data_json FROM connector_items WHERE id=?", (entry["entity_id"],))).fetchone()
                entry["resolved"] = bool(source and (source["status"] != "active" or loads(source["data_json"],{}).get("is_done")))
            by_id[key(entry)] = entry
        item["event_count"] = int(item.get("event_count", item.get("count",0))) + 1
        item.update(title=title.strip()[:160], updated_at=stamp, entries=list(by_id.values()), priority=priority if priority in {"low","normal","high"} else "normal")
        old_metadata = item.get("metadata") or {}
        item["metadata"] = {**old_metadata, **(metadata or {})}
        changes = list(old_metadata.get("changes") or []) + list((metadata or {}).get("changes") or [])
        if changes:
            item["metadata"]["changes"] = changes[-40:]
        render_notice(item)
        if not item["count"]:
            item["status"] = "resolved"
        if existing:
            await db.execute("UPDATE notifications SET data_json=?,status=? WHERE id=?", (dumps(item),item["status"],item["id"]))
        else:
            await db.execute("INSERT INTO notifications VALUES(?,?,?,?)", (item["id"],dumps(item),item["status"],stamp))
        await db.commit()
    return item


async def resolve_notifications(db: Any, entity_type: str, entity_id: str) -> None:
    rows = await (await db.execute("SELECT id,data_json FROM notifications WHERE status!='dismissed'")).fetchall()
    for row in rows:
        item = loads(row["data_json"], {})
        entries = item.get("entries") or []
        changed = False
        for entry in entries:
            if entry.get("entity_type") == entity_type and entry.get("entity_id") == entity_id and not entry.get("resolved"):
                entry["resolved"] = True
                entry["resolved_at"] = now()
                changed = True
        if not entries and entity_type == "action" and item.get("kind") == "action_due" and item.get("entity_id") == entity_id:
            entries = [{"entity_type":"action", "entity_id":entity_id, "body":item.get("body", ""), "resolved":True}]
            changed = True
        if changed:
            item["entries"] = entries
            render_notice(item)
            item["updated_at"] = now()
            if not any(not entry.get("resolved") for entry in entries):
                item["status"] = "resolved"
            await db.execute("UPDATE notifications SET data_json=?,status=? WHERE id=?", (dumps(item), item["status"], row["id"]))


def render_notice(item: dict[str, Any]) -> None:
    entries = item.get("entries") or []
    active = [entry for entry in entries if not entry.get("resolved")]
    item["entity_refs"] = [{"type":entry["entity_type"], "id":entry["entity_id"]} for entry in active if entry.get("entity_id")]
    item["entity_ids"] = list(dict.fromkeys(entry["entity_id"] for entry in active if entry.get("entity_id")))
    typed = any(entry["entity_type"] in {"action", "connector_item"} for entry in entries)
    item["count"] = len(active) if typed else int(item.get("event_count",len(active)))
    item["body"] = "\n\n".join(str(entry.get("body") or "") for entry in active) if active else "关联事项已完成，无待处理提醒。"
    if not active:
        item["title"] = "关联提醒已处理"


async def action_to_tasks(db: Any, action: dict[str, Any]) -> None:
    if action.get("status") == "done":
        await resolve_notifications(db, "action", action["id"])
    plan, stage = action.get("plan_id"), action.get("stage_id")
    if not plan or not stage:
        return
    rows = await (await db.execute("SELECT data_json FROM actions WHERE json_extract(data_json,'$.plan_id')=? "
                                  "AND json_extract(data_json,'$.stage_id')=?", (plan,stage))).fetchall()
    bound = [loads(row["data_json"], {}) for row in rows]
    done = bool(bound) and all(item.get("status") == "done" for item in bound)
    tasks = await (await db.execute("SELECT id,data_json FROM tasks WHERE json_extract(data_json,'$.plan_id')=?", (plan,))).fetchall()
    for row in tasks:
        task = loads(row["data_json"], {})
        if task.get("status") == "cancelled":
            continue
        changed = False
        for step in task.get("steps") or []:
            if step.get("stage_id") != stage or step.get("status") == "skipped":
                continue
            target = "completed" if done else ("pending" if step.get("status") == "completed" else step.get("status"))
            if step.get("status") != target:
                step.update(status=target, updated_at=now())
                changed = True
        if changed:
            all_done = all(step.get("status") in {"completed", "skipped"} for step in task.get("steps") or [])
            if all_done:
                task.update(status="completed", completed_at=now())
            elif task.get("status") == "completed":
                task.update(status="active", completed_at=None)
            task.update(updated_at=now(), revision=int(task.get("revision",1))+1)
            await db.execute("UPDATE tasks SET data_json=?,updated_at=? WHERE id=?", (dumps(task),task["updated_at"],row["id"]))


async def task_to_actions(db: Any, task: dict[str, Any], previous: dict[str, Any]) -> None:
    plan = task.get("plan_id")
    if not plan or task.get("status") == "cancelled":
        return
    old_steps = {step.get("id"):step for step in previous.get("steps") or []}
    for step in task.get("steps") or []:
        stage, status = step.get("stage_id"), step.get("status")
        prior = (old_steps.get(step.get("id")) or {}).get("status")
        if not stage or status == prior or (status != "completed" and prior != "completed"):
            continue
        rows = await (await db.execute("SELECT id,data_json FROM actions WHERE json_extract(data_json,'$.plan_id')=? "
                                      "AND json_extract(data_json,'$.stage_id')=?", (plan,stage))).fetchall()
        for row in rows:
            action = loads(row["data_json"], {})
            if action.get("status") == "cancelled":
                continue
            target = "done" if status == "completed" else "next"
            if action.get("status") == target:
                continue
            action.update(status=target, completed_at=now() if target == "done" else None, updated_at=now())
            await db.execute("UPDATE actions SET data_json=?,status=?,updated_at=? WHERE id=?",
                             (dumps(action),target,action["updated_at"],action["id"]))
            if target == "done":
                await resolve_notifications(db, "action", action["id"])
