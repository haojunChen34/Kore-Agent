from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .database import Database
from .linked_progress import action_to_tasks, create_notice
from .util import dumps, loads, now, uid

DEFAULT_PROFILE: dict[str, Any] = {
    "name": "",
    "preferred_language": "zh-CN",
    "timezone": "Asia/Shanghai",
    "communication_style": "清晰、直接、简洁",
    "work_start_time": "09:00",
    "work_end_time": "18:00",
    "quiet_hours_start": "22:00",
    "quiet_hours_end": "08:00",
    "assistant_name": "Kore",
    "daily_brief_enabled": True,
    "desktop_notifications_enabled": True,
    "notification_priority": "normal",
}

DEFAULT_PRIVACY: dict[str, Any] = {
    "auto_memory_enabled": True,
    "memory_in_context_enabled": True,
    # Newly extracted memories normally wait for user confirmation.
    "memory_review_mode": "review",
    "save_run_trace": True,
    "desktop_notifications_enabled": True,
    "cloud_processing_allowed": True,
}


class ProfileService:
    def __init__(self, db: Database):
        self.db = db

    async def get(self) -> dict[str, Any]:
        value = await self.db.get_json("personal_profile", {})
        return {**DEFAULT_PROFILE, **(value if isinstance(value, dict) else {})}

    async def update(self, values: dict[str, Any]) -> dict[str, Any]:
        current = await self.get()
        for key in DEFAULT_PROFILE:
            if key in values and values[key] is not None:
                current[key] = values[key]
        await self.db.set_json("personal_profile", current)
        return current

    async def privacy(self) -> dict[str, Any]:
        value = await self.db.get_json("privacy_settings", {})
        return {**DEFAULT_PRIVACY, **(value if isinstance(value, dict) else {})}

    async def update_privacy(self, values: dict[str, Any]) -> dict[str, Any]:
        current = await self.privacy()
        for key in DEFAULT_PRIVACY:
            if key in values and values[key] is not None:
                if key == "memory_review_mode":
                    mode = str(values[key]).strip().lower()
                    if mode in {"review", "automatic"}:
                        current[key] = mode
                    continue
                current[key] = bool(values[key])
        await self.db.set_json("privacy_settings", current)
        return current

    async def notification_settings(self) -> dict[str, Any]:
        profile = await self.get()
        return {
            "enabled": bool(profile.get("desktop_notifications_enabled", True)),
            "quiet_hours_start": str(profile.get("quiet_hours_start") or "22:00"),
            "quiet_hours_end": str(profile.get("quiet_hours_end") or "08:00"),
            "priority": str(profile.get("notification_priority") or "normal"),
        }

    async def update_notification_settings(self, values: dict[str, Any]) -> dict[str, Any]:
        current = await self.notification_settings()
        if "enabled" in values:
            current["enabled"] = bool(values["enabled"])
        for key in ("quiet_hours_start", "quiet_hours_end"):
            if key in values and values[key] is not None:
                value = str(values[key])
                if not __import__("re").fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
                    raise ValueError(f"{key} must use HH:MM")
                current[key] = value
        if "priority" in values and str(values["priority"]) in {"low", "normal", "high"}:
            current["priority"] = str(values["priority"])
        await self.update(
            {
                "desktop_notifications_enabled": current["enabled"],
                "quiet_hours_start": current["quiet_hours_start"],
                "quiet_hours_end": current["quiet_hours_end"],
                "notification_priority": current["priority"],
            }
        )
        return current


class PersonalService:
    def __init__(self, db: Database):
        self.db = db

    async def _list(self, table: str, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = f"SELECT data_json FROM {table}"
        args: tuple[Any, ...] = ()
        if status:
            query += " WHERE status=?"
            args = (status,)
        order_column = "created_at" if table == "notifications" else "updated_at"
        query += f" ORDER BY {order_column} DESC LIMIT ?"
        rows = await self.db.fetchall(query, (*args, max(1, min(limit, 500))))
        return [loads(row["data_json"], {}) for row in rows]

    async def goals(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return await self._list("goals", status=status, limit=limit)

    async def goal_create(
        self,
        title: str,
        description: str = "",
        *,
        priority: str = "normal",
        plan_id: str | None = None,
        plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        stamp = now()
        item = {
            "id": uid(),
            "title": title.strip()[:160],
            "description": description.strip(),
            "priority": priority,
            "status": "active",
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
        }
        if plan_id:
            item["plan_id"] = plan_id
        if isinstance(plan, dict):
            item["plan"] = plan
            item["plan_history"] = [{"version": 1, "created_at": stamp, "plan": plan}]
        await self.db.execute("INSERT INTO goals VALUES(?,?,?,?)", (item["id"], dumps(item), item["status"], stamp))
        return item

    async def goal_update(self, goal_id: str, values: dict[str, Any]) -> dict[str, Any]:
        item = await self._get("goals", goal_id, "Goal")
        for key in ("title", "description", "priority", "status", "plan_id", "plan"):
            if key in values and values[key] is not None:
                item[key] = str(values[key]).strip() if isinstance(values[key], str) else values[key]
        if isinstance(values.get("plan"), dict):
            history = item.setdefault("plan_history", [])
            history.append({"version": len(history) + 1, "created_at": now(), "plan": values["plan"]})
        item["updated_at"] = now()
        if item.get("status") == "completed":
            item["completed_at"] = item["updated_at"]
        await self.db.execute(
            "UPDATE goals SET data_json=?,status=?,updated_at=? WHERE id=?",
            (dumps(item), item["status"], item["updated_at"], goal_id),
        )
        return item

    async def projects(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return await self._list("projects", status=status, limit=limit)

    async def project_create(self, title: str, description: str = "", goal_id: str | None = None) -> dict[str, Any]:
        stamp = now()
        item = {
            "id": uid(),
            "title": title.strip()[:160],
            "description": description.strip(),
            "goal_id": goal_id,
            "status": "active",
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
        }
        await self.db.execute("INSERT INTO projects VALUES(?,?,?,?)", (item["id"], dumps(item), item["status"], stamp))
        return item

    async def project_update(self, project_id: str, values: dict[str, Any]) -> dict[str, Any]:
        item = await self._get("projects", project_id, "Project")
        for key in ("title", "description", "goal_id", "status"):
            if key in values and values[key] is not None:
                item[key] = values[key]
        item["updated_at"] = now()
        if item.get("status") == "completed":
            item["completed_at"] = item["updated_at"]
        await self.db.execute(
            "UPDATE projects SET data_json=?,status=?,updated_at=? WHERE id=?",
            (dumps(item), item["status"], item["updated_at"], project_id),
        )
        return item

    async def actions(self, *, status: str | None = None, due_date: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        items = await self._list("actions", status=status, limit=limit)
        if due_date:
            items = [item for item in items if str(item.get("due_at") or "").startswith(due_date)]
        return items

    async def action_create(
        self,
        title: str,
        description: str = "",
        *,
        due_at: str | None = None,
        goal_id: str | None = None,
        project_id: str | None = None,
        priority: str = "normal",
        source: str = "user",
        plan_id: str | None = None,
        stage_id: str | None = None,
    ) -> dict[str, Any]:
        stamp = now()
        item = {
            "id": uid(),
            "title": title.strip()[:240],
            "description": description.strip(),
            "status": "next",
            "priority": priority,
            "due_at": due_at,
            "goal_id": goal_id,
            "project_id": project_id,
            "source": source,
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
        }
        if plan_id:
            item["plan_id"] = plan_id
        if stage_id:
            item["stage_id"] = stage_id
        await self.db.execute("INSERT INTO actions VALUES(?,?,?,?,?)", (item["id"], dumps(item), item["status"], due_at, stamp))
        return item

    async def action_update(self, action_id: str, values: dict[str, Any]) -> dict[str, Any]:
        async with self.db.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute("SELECT data_json FROM actions WHERE id=?", (action_id,))).fetchone()
            if not row:
                raise KeyError("Action not found")
            item = loads(row["data_json"], {})
            for key in ("title", "description", "status", "priority", "due_at", "goal_id", "project_id"):
                if key in values:
                    item[key] = values[key]
            item["updated_at"] = now()
            item["completed_at"] = (item.get("completed_at") or item["updated_at"]) if item.get("status") == "done" else None
            await db.execute("UPDATE actions SET data_json=?,status=?,due_at=?,updated_at=? WHERE id=?",
                             (dumps(item), item["status"], item.get("due_at"), item["updated_at"], action_id))
            await action_to_tasks(db, item)
            await db.commit()
            return item

    async def action_complete(self, action_id: str) -> dict[str, Any]:
        return await self.action_update(action_id, {"status": "done"})

    async def notifications(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return await self._list("notifications", status=status, limit=limit)

    async def notify(
        self,
        title: str,
        body: str,
        *,
        kind: str = "agent_suggestion",
        entity_id: str | None = None,
        dedupe_key: str | None = None,
        group_key: str | None = None,
        merge_window_seconds: int = 600,
        metadata: dict[str, Any] | None = None,
        priority: str = "normal",
        entity_type: str | None = None,
        entities: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return await create_notice(self.db, title, body, kind=kind, entity_id=entity_id, dedupe_key=dedupe_key,
                                   group_key=group_key, merge_window_seconds=merge_window_seconds,
                                   metadata=metadata, priority=priority, entity_type=entity_type, entities=entities)

    async def notification_update(self, notification_id: str, status: str) -> dict[str, Any]:
        item = await self._get("notifications", notification_id, "Notification")
        item["status"] = status
        item["read_at"] = item.get("read_at") or (now() if status in {"read", "dismissed"} else None)
        item["dismissed_at"] = item.get("dismissed_at") or (now() if status == "dismissed" else None)
        await self.db.execute("UPDATE notifications SET data_json=?,status=? WHERE id=?", (dumps(item), status, notification_id))
        return item

    async def today(self, *, timezone: str = "Asia/Shanghai") -> dict[str, Any]:
        try:
            tz = ZoneInfo(timezone)
        except Exception:
            tz = UTC
        local_today = datetime.now(tz).date()
        dated_actions = []
        for item in await self.actions(limit=500):
            raw_due = item.get("due_at")
            try:
                due = datetime.fromisoformat(str(raw_due).replace("Z", "+00:00")) if raw_due else None
                if due is not None:
                    due = (due if due.tzinfo else due.replace(tzinfo=tz)).astimezone(tz)
            except ValueError:
                due = None
            dated_actions.append((item, due))
        open_statuses = {"next", "doing", "waiting"}
        today_actions = [(item, due) for item, due in dated_actions if due and due.date() == local_today]
        overdue = [(item, due) for item, due in dated_actions if due and due.date() < local_today and item.get("status") in open_statuses]
        next_actions = [(item, due) for item, due in dated_actions if item.get("status") in open_statuses and (due is None or due.date() > local_today)]
        today_actions.sort(key=lambda entry: entry[1])
        overdue.sort(key=lambda entry: entry[1], reverse=True)
        next_actions.sort(key=lambda entry: (entry[1] is None, entry[1] or datetime.max.replace(tzinfo=tz)))
        active = [item for item in await self.goals(limit=20) if item.get("status") == "active"]
        pending = await self.notifications(status="unread", limit=20)
        return {
            "date": local_today.isoformat(),
            "actions": [item for item, _ in today_actions],
            "overdue_actions": [item for item, _ in overdue],
            "active_goals": active,
            "notifications": pending,
            "next_actions": [item for item, _ in next_actions],
        }

    async def _get(self, table: str, item_id: str, label: str) -> dict[str, Any]:
        row = await self.db.fetchone(f"SELECT data_json FROM {table} WHERE id=?", (item_id,))
        if not row:
            raise KeyError(f"{label} not found")
        return loads(row["data_json"], {})


def build_daily_brief(
    today: dict[str, Any], profile: dict[str, Any], context: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build a deterministic fallback brief when the summary model is unavailable."""
    actions = today.get("actions", [])
    goals = today.get("active_goals", [])
    notifications = today.get("notifications", [])
    context = context or {
        "today_actions": actions,
        "next_actions": today.get("next_actions", []),
        "active_goals": goals,
        "unread_notifications": notifications,
        "important_external_information": [],
        "upcoming_calendar_events": [],
        "connector_alerts": [],
    }
    priority_titles = [str(item.get("title") or "").strip() for item in actions[:3] if item.get("title")]
    content_parts = [
        f"\u4eca\u5929\u662f {today.get('date')}\u3002",
        f"\u4eca\u65e5\u6709 {len(actions)} \u9879\u884c\u52a8\uff0c{len(goals)} \u4e2a\u6d3b\u8dc3\u76ee\u6807\uff0c{len(notifications)} \u6761\u672a\u8bfb\u901a\u77e5\u3002",
    ]
    if priority_titles:
        content_parts.append("\u4f18\u5148\u5904\u7406\uff1a" + "\u3001".join(priority_titles) + "\u3002")
    if context.get("upcoming_calendar_events"):
        content_parts.append(f"\u63a5\u4e0b\u6765\u6709 {len(context['upcoming_calendar_events'])} \u4e2a\u65e5\u5386\u4e8b\u4ef6\u3002")
    if context.get("connector_alerts"):
        content_parts.append(f"\u6709 {len(context['connector_alerts'])} \u4e2a\u8fde\u63a5\u5668\u63d0\u9192\u9700\u8981\u7559\u610f\u3002")
    counts = {
        "actions": len(actions),
        "next_actions": len(context.get("next_actions", [])),
        "goals": len(goals),
        "notifications": len(notifications),
        "external_information": len(context.get("important_external_information", [])),
        "calendar_events": len(context.get("upcoming_calendar_events", [])),
        "connector_alerts": len(context.get("connector_alerts", [])),
    }
    return {
        "date": today.get("date"),
        "title": "\u4eca\u65e5\u6458\u8981",
        "content": " ".join(content_parts),
        "priorities": priority_titles,
        "risks": [str(item.get("last_error") or "") for item in context.get("connector_alerts", []) if item.get("last_error")],
        "next_step": priority_titles[0] if priority_titles else "\u5148\u67e5\u770b\u4eca\u5929\u7684\u672a\u8bfb\u901a\u77e5\u3002",
        "ai_generated": False,
        "context": context,
        "counts": counts,
        "generated_at": now(),
        "timezone": profile.get("timezone", "Asia/Shanghai"),
    }
