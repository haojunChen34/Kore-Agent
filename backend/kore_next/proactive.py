from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from .database import Database
from .security import redact
from .util import dumps, loads, now, uid

RISK_LEVELS = {"low", "medium", "high"}


def _time(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class ProactiveService:
    """Durable event -> suggestion -> execution pipeline for the personal agent."""

    def __init__(self, db: Database):
        self.db = db

    async def audit(
        self, action: str, *, entity_type: str, entity_id: str | None = None, actor: str = "system", details: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        item = {
            "id": uid(),
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "actor": actor,
            "details": redact(details or {}),
            "created_at": now(),
        }
        await self.db.execute(
            "INSERT INTO audit_log(id,entity_type,entity_id,action,actor,data_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (item["id"], entity_type, entity_id, action, actor, dumps(item), item["created_at"]),
        )
        return item

    async def audit_list(self, *, entity_type: str | None = None, entity_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = "SELECT data_json FROM audit_log WHERE 1=1"
        args: list[Any] = []
        if entity_type:
            query += " AND entity_type=?"
            args.append(entity_type)
        if entity_id:
            query += " AND entity_id=?"
            args.append(entity_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        return [loads(row["data_json"], {}) for row in await self.db.fetchall(query, tuple(args))]

    async def triggers(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = "SELECT data_json FROM proactive_triggers"
        args: list[Any] = []
        if status:
            query += " WHERE status=?"
            args.append(status)
        query += " ORDER BY updated_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        return [loads(row["data_json"], {}) for row in await self.db.fetchall(query, tuple(args))]

    async def trigger_get(self, trigger_id: str) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM proactive_triggers WHERE id=?", (trigger_id,))
        if not row:
            raise KeyError("trigger not found")
        return loads(row["data_json"], {})

    async def trigger_create(self, values: dict[str, Any], *, actor: str = "user") -> dict[str, Any]:
        stamp = now()
        item = {
            "id": str(values.get("id") or uid()),
            "name": str(values.get("name") or "主动触发器").strip()[:160],
            "description": str(values.get("description") or ""),
            "source": str(values.get("source") or "*").strip(),
            "event_type": str(values.get("event_type") or "*").strip(),
            "status": str(values.get("status") or "active"),
            "risk": str(values.get("risk") or "low"),
            "action": dict(values.get("action") or {"type": "notify"}),
            "auto_execute": bool(values.get("auto_execute", False)),
            "cooldown_seconds": max(0, int(values.get("cooldown_seconds", 300))),
            "created_at": stamp,
            "updated_at": stamp,
        }
        if item["risk"] not in RISK_LEVELS or item["status"] not in {"active", "paused", "disabled"}:
            raise ValueError("invalid trigger risk or status")
        await self.db.execute(
            "INSERT INTO proactive_triggers(id,data_json,status,source,event_type,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (item["id"], dumps(item), item["status"], item["source"], item["event_type"], stamp, stamp),
        )
        await self.audit("trigger.created", entity_type="trigger", entity_id=item["id"], actor=actor, details=item)
        return item

    async def trigger_update(self, trigger_id: str, values: dict[str, Any], *, actor: str = "user") -> dict[str, Any]:
        item = await self.trigger_get(trigger_id)
        for key in ("name", "description", "source", "event_type", "status", "risk", "action", "auto_execute", "cooldown_seconds"):
            if key in values and values[key] is not None:
                item[key] = values[key]
        if item.get("risk") not in RISK_LEVELS or item.get("status") not in {"active", "paused", "disabled"}:
            raise ValueError("invalid trigger risk or status")
        item["updated_at"] = now()
        await self.db.execute(
            "UPDATE proactive_triggers SET data_json=?,status=?,source=?,event_type=?,updated_at=? WHERE id=?",
            (dumps(item), item["status"], item["source"], item["event_type"], item["updated_at"], trigger_id),
        )
        await self.audit("trigger.updated", entity_type="trigger", entity_id=trigger_id, actor=actor, details=values)
        return item

    async def trigger_delete(self, trigger_id: str, *, actor: str = "user") -> None:
        await self.trigger_get(trigger_id)
        await self.db.execute("DELETE FROM proactive_triggers WHERE id=?", (trigger_id,))
        await self.audit("trigger.deleted", entity_type="trigger", entity_id=trigger_id, actor=actor)

    async def ensure_defaults(self) -> None:
        existing = {item.get("event_type") for item in await self.triggers(limit=500)}
        defaults = [
            {
                "name": "新邮件提醒",
                "description": "新邮件进入收件箱时生成处理建议",
                "source": "connector",
                "event_type": "connector.item.created",
                "risk": "low",
                "action": {"type": "notify", "title": "有新的邮件", "body": "收件箱收到新邮件，建议尽快查看。", "kind": "inbox"},
                "auto_execute": True,
                "cooldown_seconds": 300,
            },
            {
                "name": "运行失败恢复",
                "description": "Agent 运行失败时提示恢复或重试",
                "source": "run",
                "event_type": "run.failed",
                "risk": "medium",
                "action": {"type": "notify", "title": "Agent 运行失败", "body": "有一次运行失败，可以查看审计记录并重试。", "kind": "run_failed"},
                "auto_execute": False,
                "cooldown_seconds": 60,
            },
            {
                "name": "待办截止提醒",
                "description": "扫描临近截止的个人行动",
                "source": "schedule",
                "event_type": "schedule.scan",
                "risk": "low",
                "action": {"type": "deadline_scan"},
                "auto_execute": True,
                "cooldown_seconds": 900,
            },
            {
                "name": "连接器同步失败",
                "description": "连接器同步失败时提示检查授权或稍后重试",
                "source": "connector",
                "event_type": "connector.sync.failed",
                "risk": "medium",
                "action": {
                    "type": "notify",
                    "title": "连接器同步失败",
                    "body": "请检查连接器授权和网络后重试。",
                    "kind": "connector_failed",
                },
                "auto_execute": False,
                "cooldown_seconds": 300,
            },
        ]
        for item in defaults:
            if item["event_type"] not in existing:
                await self.trigger_create(item, actor="system")

    async def ensure_additional_defaults(self) -> None:
        """Add newer reliability triggers without resetting existing rules."""
        existing = {item.get("event_type") for item in await self.triggers(limit=500)}
        defaults = [
            {
                "name": "外部操作部分成功提醒",
                "description": "邮件、日历、飞书任务或文档操作只完成了一部分时，提醒检查已完成步骤并继续失败步骤。",
                "source": "external_operation",
                "event_type": "external_operation.partial_success",
                "risk": "medium",
                "action": {
                    "type": "notify",
                    "title": "外部操作部分成功",
                    "body": "外部操作只完成了一部分，请查看执行时间线，继续失败步骤或执行补偿。",
                    "kind": "external_operation_partial",
                },
                "auto_execute": False,
                "cooldown_seconds": 300,
            },
            {
                "name": "外部操作结果未知提醒",
                "description": "外部服务可能已经接受请求但本地未确认结果时，提醒先查询远端状态再重试。",
                "source": "external_operation",
                "event_type": "external_operation.uncertain",
                "risk": "medium",
                "action": {
                    "type": "notify",
                    "title": "外部操作结果未知",
                    "body": "外部服务可能已经接受请求，请先查询远端结果，确认后再重试。",
                    "kind": "external_operation_uncertain",
                },
                "auto_execute": False,
                "cooldown_seconds": 300,
            },
            {
                "name": "日历和飞书任务截止提醒",
                "description": "未来 24 小时内开始的日历事件或到期的未完成飞书任务，提醒提前处理。",
                "source": "schedule",
                "event_type": "schedule.connector_deadline",
                "risk": "low",
                "action": {
                    "type": "notify",
                    "title": "日历或飞书任务即将到期",
                    "kind": "connector_deadline",
                },
                "auto_execute": False,
                "cooldown_seconds": 900,
            },
        ]
        for item in defaults:
            if item["event_type"] not in existing:
                await self.trigger_create(item, actor="system")

    @staticmethod
    def _matches(trigger: dict[str, Any], source: str, event_type: str) -> bool:
        return trigger.get("status") == "active" and trigger.get("source") in {"*", source} and trigger.get("event_type") in {"*", event_type}

    async def emit(self, source: str, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        suggestions: list[dict[str, Any]] = []
        for trigger in await self.triggers(status="active", limit=500):
            if not self._matches(trigger, source, event_type):
                continue
            dedupe = f"{trigger['id']}:{payload.get('item_id') or payload.get('run_id') or event_type}"
            recent = await self.db.fetchone(
                "SELECT id FROM proactive_suggestions WHERE trigger_id=? AND dedupe_key=? AND created_at>? LIMIT 1",
                (trigger["id"], dedupe, (datetime.now(UTC) - timedelta(seconds=int(trigger.get("cooldown_seconds", 300)))).isoformat()),
            )
            if recent:
                continue
            action = dict(trigger.get("action") or {})
            action["event"] = payload
            item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
            item_title = str(item.get("title") or "").strip()
            item_due = str(item.get("due_at") or item.get("starts_at") or "").strip()
            if action.get("type") == "notify" and item_title:
                details = f"（时间：{item_due}）" if item_due else ""
                action.setdefault("body", f"{item_title}{details} 需要你的关注。")
            reason = trigger.get("description") or f"事件 {event_type} 触发了该建议。"
            if item_title:
                reason = f"{reason} 当前事项：{item_title}。"
            stamp = now()
            suggestion = {
                "id": uid(),
                "trigger_id": trigger["id"],
                "title": str(action.get("title") or trigger["name"]),
                "reason": reason,
                "risk": trigger.get("risk", "low"),
                "status": "pending",
                "action": action,
                "source": source,
                "event_type": event_type,
                "payload": payload,
                "dedupe_key": dedupe,
                "created_at": stamp,
                "updated_at": stamp,
                "accepted_at": None,
                "completed_at": None,
                "failure": None,
            }
            await self.db.execute(
                "INSERT INTO proactive_suggestions(id,trigger_id,data_json,status,risk,dedupe_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (suggestion["id"], trigger["id"], dumps(suggestion), "pending", suggestion["risk"], dedupe, stamp, stamp),
            )
            await self.audit(
                "suggestion.created",
                entity_type="suggestion",
                entity_id=suggestion["id"],
                details={"event_type": event_type, "risk": suggestion["risk"]},
            )
            suggestions.append(suggestion)
        return suggestions

    async def suggestions(self, *, status: str | None = None, risk: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = "SELECT data_json FROM proactive_suggestions WHERE 1=1"
        args: list[Any] = []
        if status:
            query += " AND status=?"
            args.append(status)
        if risk:
            query += " AND risk=?"
            args.append(risk)
        query += " ORDER BY created_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        return [loads(row["data_json"], {}) for row in await self.db.fetchall(query, tuple(args))]

    async def suggestion_get(self, suggestion_id: str) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM proactive_suggestions WHERE id=?", (suggestion_id,))
        if not row:
            raise KeyError("suggestion not found")
        return loads(row["data_json"], {})

    async def _save_suggestion(self, item: dict[str, Any]) -> None:
        item["updated_at"] = now()
        await self.db.execute(
            "UPDATE proactive_suggestions SET data_json=?,status=?,updated_at=? WHERE id=?",
            (dumps(item), item["status"], item["updated_at"], item["id"]),
        )

    async def accept(
        self,
        suggestion_id: str,
        *,
        execute: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        request_approval: Callable[[dict[str, Any]], Awaitable[str]],
        broadcast: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        actor: str = "user",
    ) -> dict[str, Any]:
        suggestion = await self.suggestion_get(suggestion_id)
        if suggestion.get("status") not in {"pending", "failed"}:
            raise ValueError("suggestion is not actionable")
        suggestion["status"] = "accepted"
        suggestion["accepted_at"] = now()
        suggestion["failure"] = None
        await self._save_suggestion(suggestion)
        await self.audit("suggestion.accepted", entity_type="suggestion", entity_id=suggestion_id, actor=actor)
        job = await self._create_job(suggestion, request_approval=request_approval, broadcast=broadcast)
        if suggestion["risk"] == "low":
            result = await self.run_job(job["id"], execute=execute, broadcast=broadcast)
            return {"suggestion": await self.suggestion_get(suggestion_id), "job": result}
        return {"suggestion": await self.suggestion_get(suggestion_id), "job": job}

    async def dismiss(self, suggestion_id: str, *, actor: str = "user") -> dict[str, Any]:
        suggestion = await self.suggestion_get(suggestion_id)
        suggestion["status"] = "dismissed"
        await self._save_suggestion(suggestion)
        await self.audit("suggestion.dismissed", entity_type="suggestion", entity_id=suggestion_id, actor=actor)
        return suggestion

    async def _create_job(
        self,
        suggestion: dict[str, Any],
        *,
        request_approval: Callable[[dict[str, Any]], Awaitable[str]],
        broadcast: Callable[[str, dict[str, Any]], Awaitable[None]] | None,
    ) -> dict[str, Any]:
        stamp = now()
        job = {
            "id": uid(),
            "suggestion_id": suggestion["id"],
            "action": suggestion["action"],
            "risk": suggestion["risk"],
            "status": "pending",
            "attempts": 0,
            "max_attempts": 3,
            "next_retry_at": stamp,
            "error": None,
            "approval_id": None,
            "run_id": None,
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
        }
        await self.db.execute(
            "INSERT INTO execution_jobs(id,suggestion_id,data_json,status,risk,attempts,max_attempts,next_retry_at,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (job["id"], suggestion["id"], dumps(job), job["status"], job["risk"], 0, 3, stamp, stamp, stamp),
        )
        if suggestion["risk"] == "high":
            job["status"] = "awaiting_approval"
            job["approval_id"] = await request_approval({"job": job, "suggestion": suggestion})
            suggestion["status"] = "awaiting_approval"
            await self._save_suggestion(suggestion)
            await self._save_job(job)
        await self.audit("job.created", entity_type="job", entity_id=job["id"], details={"risk": job["risk"], "status": job["status"]})
        if broadcast:
            await broadcast("execution.job.updated", job)
        return job

    async def jobs(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = "SELECT data_json FROM execution_jobs"
        args: list[Any] = []
        if status:
            query += " WHERE status=?"
            args.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        return [loads(row["data_json"], {}) for row in await self.db.fetchall(query, tuple(args))]

    async def job_get(self, job_id: str) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM execution_jobs WHERE id=?", (job_id,))
        if not row:
            raise KeyError("execution job not found")
        return loads(row["data_json"], {})

    async def run_job(
        self,
        job_id: str,
        *,
        execute: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        broadcast: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        job = await self.job_get(job_id)
        if job["status"] in {"completed", "cancelled", "awaiting_approval"}:
            return job
        # The scheduler and manual retry endpoint can invoke this method at
        # the same time.  Claim the durable row atomically before running any
        # external side effect; a process-local set cannot protect against
        # this race (or against a second host process).
        if job.get("status") != "pending":
            return job
        job, claimed = await self.db.claim_execution_job(job_id, updated_at=now())
        if not claimed:
            return job
        await self.audit("job.started", entity_type="job", entity_id=job_id, details={"attempt": job["attempts"]})
        try:
            result = await execute(job)
            job["status"] = "completed"
            job["result"] = result
            job["completed_at"] = now()
            job["error"] = None
            suggestion = await self.suggestion_get(job["suggestion_id"])
            suggestion["status"] = "completed"
            suggestion["completed_at"] = job["completed_at"]
            await self._save_suggestion(suggestion)
            await self.audit("job.completed", entity_type="job", entity_id=job_id, details=result)
        except Exception as exc:
            job["error"] = str(exc)[:1000]
            suggestion = await self.suggestion_get(job["suggestion_id"])
            # A remote side effect marked uncertain or partially successful
            # must never be replayed automatically.  The user must inspect the
            # workflow and explicitly continue a safe failed step.
            no_auto_retry = any(
                marker in job["error"].lower() for marker in ("uncertain", "may have accepted", "partial_success", "workflow stopped")
            )
            if job["attempts"] < job["max_attempts"] and not no_auto_retry:
                delay = 60 * (5 ** (job["attempts"] - 1))
                job["status"] = "pending"
                job["next_retry_at"] = (datetime.now(UTC) + timedelta(seconds=delay)).isoformat()
                suggestion["status"] = "failed"
                suggestion["failure"] = job["error"]
                await self.audit(
                    "job.retry_scheduled", entity_type="job", entity_id=job_id, details={"error": job["error"], "next_retry_at": job["next_retry_at"]}
                )
            else:
                job["status"] = "failed"
                suggestion["status"] = "failed"
                suggestion["failure"] = job["error"]
                await self.audit("job.failed", entity_type="job", entity_id=job_id, details={"error": job["error"]})
                await self.audit("recovery.suggested", entity_type="job", entity_id=job_id, details={"message": "请检查失败原因后重试或忽略该建议"})
            await self._save_suggestion(suggestion)
        await self._save_job(job)
        if broadcast:
            await broadcast("execution.job.updated", job)
        return job

    async def _save_job(self, job: dict[str, Any]) -> None:
        job["updated_at"] = now()
        await self.db.execute(
            "UPDATE execution_jobs SET data_json=?,status=?,risk=?,attempts=?,next_retry_at=?,updated_at=? WHERE id=?",
            (dumps(job), job["status"], job["risk"], job["attempts"], job.get("next_retry_at"), job["updated_at"], job["id"]),
        )

    async def retry(
        self,
        job_id: str,
        *,
        execute: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        broadcast: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        job = await self.job_get(job_id)
        job["status"] = "pending"
        job["next_retry_at"] = now()
        job["error"] = None
        await self._save_job(job)
        suggestion = await self.suggestion_get(job["suggestion_id"])
        suggestion["status"] = "accepted"
        suggestion["failure"] = None
        await self._save_suggestion(suggestion)
        await self.audit("job.retry_requested", entity_type="job", entity_id=job_id, actor="user")
        return await self.run_job(job_id, execute=execute, broadcast=broadcast)

    async def cancel(self, job_id: str, *, actor: str = "user") -> dict[str, Any]:
        job = await self.job_get(job_id)
        job["status"] = "cancelled"
        await self._save_job(job)
        suggestion = await self.suggestion_get(job["suggestion_id"])
        suggestion["status"] = "dismissed"
        await self._save_suggestion(suggestion)
        await self.audit("job.cancelled", entity_type="job", entity_id=job_id, actor=actor)
        return job

    async def restore_running(self) -> None:
        rows = await self.db.fetchall("SELECT data_json FROM execution_jobs WHERE status='running'")
        for row in rows:
            job = loads(row["data_json"], {})
            job["status"] = "pending"
            job["error"] = "host_restart"
            await self._save_job(job)
            await self.audit("job.recovered", entity_type="job", entity_id=job["id"], details={"reason": "host_restart"})
