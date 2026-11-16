from __future__ import annotations

import re
from typing import Any

from .database import Database
from .util import dumps, loads, now, uid

TASK_STATUSES = {"pending", "active", "blocked", "completed", "cancelled"}
STEP_STATUSES = {"pending", "in_progress", "completed", "blocked", "skipped"}


class TaskService:
    def __init__(self, db: Database):
        self.db = db

    async def list(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = await self.db.fetchall("SELECT data_json FROM tasks ORDER BY updated_at DESC LIMIT ?", (limit,))
        tasks = [loads(row["data_json"], {}) for row in rows]
        return [
            task
            for task in tasks
            if (status is None or task.get("status") == status) and (conversation_id is None or task.get("owner_conversation_id") == conversation_id)
        ]

    async def get(self, task_id: str) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM tasks WHERE id=?", (task_id,))
        if not row:
            raise KeyError("task not found")
        return loads(row["data_json"], {})

    async def create(
        self,
        *,
        title: str,
        goal: str,
        description: str = "",
        steps: list[dict[str, Any]] | None = None,
        conversation_id: str | None = None,
        run_id: str | None = None,
        status: str = "active",
        goal_id: str | None = None,
        long_term: bool = False,
        plan_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in TASK_STATUSES:
            raise ValueError("invalid task status")
        stamp = now()
        normalized_steps = []
        task_title = title.strip()[:160] or "Task"
        task_description = description.strip()
        for index, step in enumerate(steps or [], 1):
            if not isinstance(step, dict):
                raise ValueError("each step must be an object")
            step_title, step_description = self._step_text(step, index)
            item = {
                "id": str(step.get("id") or uid()),
                "title": step_title,
                "description": step_description,
                "status": str(step.get("status") or "pending"),
                "result": step.get("result"),
                "updated_at": stamp,
                **({"stage_id": str(step["stage_id"])} if step.get("stage_id") else {}),
            }
            self._append_normalized_step(normalized_steps, item)
        if status == "pending":
            self._collapse_verification_tail(normalized_steps)
        task = {
            "id": uid(),
            "title": task_title,
            "description": task_description,
            "goal": goal.strip(),
            "goal_id": goal_id,
            # A task attached to a personal goal is persistent by definition;
            # this also protects against a model forgetting the explicit flag.
            "long_term": bool(long_term or goal_id),
            "status": status,
            "priority": "normal",
            "constraints": [],
            "state": [],
            "key_facts": [],
            "steps": normalized_steps,
            "owner_conversation_id": conversation_id,
            "run_ids": [run_id] if run_id else [],
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": stamp if status == "completed" else None,
            "revision": 1,
        }
        if plan_id:
            task["plan_id"] = plan_id
        await self.db.execute("INSERT INTO tasks(id,data_json,updated_at) VALUES(?,?,?)", (task["id"], dumps(task), stamp))
        return task

    async def repair_legacy_plan_tasks(self) -> int:
        """Restore plans accidentally completed by the old plan-run finalizer.

        Older versions marked every task discovered during a plan run as
        completed. Those tasks never had an accepted execution run, so they
        must remain pending and visible to the user for confirmation.
        """
        rows = await self.db.fetchall("SELECT id,data_json FROM tasks")
        repaired = 0
        for row in rows:
            task = loads(row["data_json"], {})
            if task.get("status") != "completed":
                continue
            run_ids = [str(item) for item in task.get("run_ids") or [] if item]
            if not run_ids:
                continue
            placeholders = ",".join("?" for _ in run_ids)
            runs = await self.db.fetchall(f"SELECT mode,source FROM runs WHERE id IN ({placeholders})", tuple(run_ids))
            if not runs or not all(str(item["mode"]) == "plan" and not item["source"] for item in runs):
                continue
            steps = task.get("steps") or []
            # Drop generic placeholders when a later task_update supplied the
            # real step descriptions, then leave all plan steps pending.
            concrete = [step for step in steps if not self._is_placeholder_step(step)]
            task["steps"] = [
                {
                    **step,
                    "status": "pending" if str(step.get("status")) == "completed" else step.get("status", "pending"),
                    "result": None if str(step.get("status")) == "completed" else step.get("result"),
                    "updated_at": now(),
                }
                for step in (concrete or steps)
            ]
            task["status"] = "pending"
            task["completed_at"] = None
            task["revision"] = int(task.get("revision", 1)) + 1
            task["updated_at"] = now()
            await self._save(task)
            repaired += 1
        return repaired

    async def update(self, task_id: str, changes: dict[str, Any], *, run_id: str | None = None) -> dict[str, Any]:
        task = await self.get(task_id)
        for key in ("title", "description", "goal", "goal_id", "long_term", "priority", "constraints", "state", "key_facts"):
            if key in changes:
                task[key] = changes[key]
        if "status" in changes:
            status = str(changes["status"])
            if status not in TASK_STATUSES:
                raise ValueError("invalid task status")
            task["status"] = status
            task["completed_at"] = now() if status == "completed" else None
        if "steps" in changes:
            task["steps"] = self._merge_steps(task.get("steps", []), changes["steps"])
            if task.get("status") == "pending":
                self._collapse_verification_tail(task["steps"])
        if run_id and run_id not in task.get("run_ids", []):
            task.setdefault("run_ids", []).append(run_id)
        task["revision"] = int(task.get("revision", 1)) + 1
        task["updated_at"] = now()
        await self._save(task)
        return task

    async def decide(self, task_id: str, accepted: bool) -> dict[str, Any]:
        task = await self.get(task_id)
        if task.get("status") != "pending":
            raise ValueError("task plan is already resolved")
        task["status"] = "active" if accepted else "cancelled"
        task["updated_at"] = now()
        task["revision"] = int(task.get("revision", 1)) + 1
        await self._save(task)
        return task

    async def complete_step(self, task_id: str, step_id: str) -> dict[str, Any]:
        task = await self.get(task_id)
        steps = task.get("steps") or []
        target = next((step for step in steps if str(step.get("id")) == step_id), None)
        if target is None:
            raise KeyError("step not found")
        target["status"] = "completed"
        target["result"] = target.get("result") or "Completed by user"
        target["updated_at"] = now()
        if steps and all(str(step.get("status")) in {"completed", "skipped"} for step in steps):
            task["status"] = "completed"
            task["completed_at"] = now()
        task["revision"] = int(task.get("revision", 1)) + 1
        task["updated_at"] = now()
        await self._save(task)
        return task

    async def active_context(self, conversation_id: str | None) -> str:
        tasks = await self.list(limit=100)
        active = [
            task
            for task in tasks
            if task.get("status") in {"active", "blocked"} and (conversation_id is None or task.get("owner_conversation_id") == conversation_id)
        ]
        if not active:
            return ""
        lines = []
        for task in active[:3]:
            lines.append(f"Task {task['id']}: {task['title']} [{task['status']}]\nGoal: {task.get('goal', '')}")
            for step in task.get("steps", []):
                lines.append(f"  - {step['id']}: {step['title']} [{step['status']}] {step.get('result') or ''}")
        return "\n".join(lines)

    async def _save(self, task: dict[str, Any]) -> None:
        await self.db.execute(
            "UPDATE tasks SET data_json=?,updated_at=? WHERE id=?",
            (dumps(task), task["updated_at"], task["id"]),
        )

    @staticmethod
    def _merge_steps(existing: list[dict[str, Any]], incoming: Any) -> list[dict[str, Any]]:
        if not isinstance(incoming, list):
            raise ValueError("steps must be a list")
        incoming_items = []
        for step in incoming:
            if not isinstance(step, dict):
                raise ValueError("each step must be an object")
            incoming_items.append(dict(step))
        # Models often create generic ``Step N`` entries when they send the
        # fields ``step/action/expected`` instead of ``title/description``.
        # A following task_update is intended to replace those placeholders,
        # rather than append a second list underneath them.
        replace_placeholders = bool(incoming_items) and all(not str(step.get("id") or "").strip() for step in incoming_items) and existing and all(
            TaskService._is_placeholder_step(step) for step in existing
        )
        base = [] if replace_placeholders else existing
        by_id = {str(step.get("id")): dict(step) for step in base}
        result = list(base)
        for index, update in enumerate(incoming_items, 1):
            step_id = str(update.get("id") or "")
            if step_id and step_id in by_id:
                target = by_id[step_id]
                if "status" in update and update["status"] not in STEP_STATUSES:
                    raise ValueError("invalid step status")
                target.update({key: value for key, value in update.items() if key in {"title", "description", "status", "result", "stage_id"}})
                target["updated_at"] = now()
                for position, item in enumerate(result):
                    if item.get("id") == step_id:
                        result[position] = target
                        break
            else:
                status = str(update.get("status") or "pending")
                if status not in STEP_STATUSES:
                    raise ValueError("invalid step status")
                title, description = TaskService._step_text(update, index)
                result.append(
                    {
                        "id": step_id or uid(),
                        "title": title,
                        "description": description,
                        "status": status,
                        "result": update.get("result"),
                        "updated_at": now(),
                        **({"stage_id": str(update["stage_id"])} if update.get("stage_id") else {}),
                    }
                )
        # A model may send the same step twice while refining a plan.  Keep
        # the first stable identity and merge only an improved description;
        # this avoids duplicate rows without changing the task's meaning.
        deduplicated: list[dict[str, Any]] = []
        for item in result:
            TaskService._append_normalized_step(deduplicated, item)
        return deduplicated

    @classmethod
    def _append_normalized_step(cls, target: list[dict[str, Any]], item: dict[str, Any]) -> None:
        """Append a step after removing model-only numbering and duplicates.

        The UI adds its own ordered-list number.  Models sometimes put another
        number (or ``Step N``) in the title, which produces rows such as
        ``1. Step 2``.  Treat those labels as formatting and use the actual
        action text when it is available.  Exact repeated actions are merged,
        while distinct actions remain separate.
        """
        title, description = cls._step_text(item, len(target) + 1)
        item["title"] = title
        item["description"] = description
        key = cls._step_key(title, description)
        for existing in target:
            if cls._step_key(str(existing.get("title") or ""), str(existing.get("description") or "")) != key:
                continue
            if len(description) > len(str(existing.get("description") or "")):
                existing["description"] = description
            if not existing.get("result") and item.get("result"):
                existing["result"] = item["result"]
            if existing.get("status") in {None, "pending"} and item.get("status") not in {None, "pending"}:
                existing["status"] = item["status"]
            return
        target.append(item)

    @staticmethod
    def _step_key(title: str, description: str) -> str:
        return re.sub(r"\s+", " ", f"{title} {description}".strip()).casefold()

    @classmethod
    def _collapse_verification_tail(cls, steps: list[dict[str, Any]]) -> None:
        """Fold a write-then-verify tail into the write step for short plans.

        A planning model commonly expands a simple three-part request into
        ``inspect -> summarize -> write -> verify``.  Verification is still
        preserved in the write step's description, but the plan remains a
        compact list of user-visible deliverables.  Longer or unrelated plans
        are left untouched.
        """
        if len(steps) != 4:
            return
        tail = str(steps[3].get("title") or "")
        write = str(steps[2].get("title") or "")
        verification = re.compile(r"(?:校验|验证|复核|检查|回读|review|verify|validate)", re.IGNORECASE)
        write_action = re.compile(r"(?:写入|生成|创建|发布|保存|write|create|publish|save)", re.IGNORECASE)
        if not verification.search(tail) or not write_action.search(write):
            return
        tail_text = "；完成后" + tail
        current = str(steps[2].get("description") or "").strip()
        steps[2]["description"] = (current + tail_text).strip("；")[:2_000]
        steps.pop(3)

    @staticmethod
    def _is_placeholder_step(step: dict[str, Any]) -> bool:
        return bool(re.fullmatch(r"step\s+\d+", str(step.get("title") or "").strip(), re.IGNORECASE)) and not str(step.get("description") or "").strip()

    @staticmethod
    def _step_text(step: dict[str, Any], index: int) -> tuple[str, str]:
        raw_title = str(step.get("title") or step.get("step") or step.get("action") or f"Step {index}").strip()
        description = str(step.get("description") or "").strip()
        expected = str(step.get("expected") or step.get("success_criteria") or "").strip()
        action = str(step.get("action") or "").strip()
        # Strip numbering that belongs to the model's presentation.  The
        # frontend supplies the visible list number itself.
        title = re.sub(r"^(?:step|步骤)\s*\d+\s*[:.)、-]?\s*", "", raw_title, flags=re.IGNORECASE).strip()
        title = re.sub(r"^\d+\s*[.)、:-]\s*", "", title).strip()
        generic_title = not title or bool(re.fullmatch(r"(?:step|步骤)?\s*\d+", title, re.IGNORECASE))
        if generic_title:
            # Prefer the model's action field.  If it omitted that field, use
            # the first meaningful line of the description as a readable
            # title, while keeping the full description below it.
            title = action or next((line.strip(" -*\t") for line in description.splitlines() if line.strip()), "")
        if not description and action and action != title:
            description = action
        if expected:
            description = f"{description}；预期：{expected}" if description else f"预期：{expected}"
        return title[:300] or f"Step {index}", description[:2_000]
