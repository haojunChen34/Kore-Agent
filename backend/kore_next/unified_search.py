from __future__ import annotations

import re
from typing import Any

from .database import Database
from .memory_protocol import FactProtocol
from .util import loads


class UnifiedSearchService:
    """Search the local personal context from one small, predictable API."""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def search(self, query: str, *, scope: str = "all", limit: int = 50, for_agent: bool = False) -> list[dict[str, Any]]:
        text = query.strip()
        if not text:
            raise ValueError("search query is required")
        limit = max(1, min(int(limit), 100))
        scopes = {scope} if scope != "all" else {"messages", "connectors", "memory", "tasks"}
        needle = f"%{text}%"
        results: list[dict[str, Any]] = []
        if "messages" in scopes:
            rows = await self.db.fetchall(
                "SELECT m.id,m.conversation_id,m.role,m.content,m.created_at,c.title "
                "FROM messages m JOIN conversations c ON c.id=m.conversation_id "
                "WHERE m.content LIKE ? AND m.role IN ('user','assistant') ORDER BY m.id DESC LIMIT ?",
                (needle, limit),
            )
            for row in rows:
                content = str(row["content"] or "")
                results.append(
                    {
                        "id": str(row["id"]),
                        "type": "conversation",
                        "source": "message",
                        "title": str(row["title"] or "会话"),
                        "snippet": self._snippet(content, text),
                        "timestamp": row["created_at"],
                        "conversation_id": row["conversation_id"],
                        "role": row["role"],
                        "source_type": "conversation",
                        "entity_id": str(row["conversation_id"]),
                        "detail_kind": "conversation",
                        "can_view_detail": True,
                        "can_open_source": True,
                        "source_ref": {"conversation_id": str(row["conversation_id"]), "message_id": str(row["id"])},
                    }
                )
        if "connectors" in scopes:
            rows = await self.db.fetchall(
                "SELECT id,provider,kind,data_json,updated_at FROM connector_items "
                "WHERE status='active' AND data_json LIKE ? ORDER BY updated_at DESC LIMIT ?",
                (needle, limit),
            )
            for row in rows:
                item = loads(row["data_json"], {})
                results.append(
                    {
                        "id": str(row["id"]),
                        "type": str(row["kind"]),
                        "source": "connector",
                        "provider": row["provider"],
                        "title": str(item.get("title") or item.get("subject") or "信息"),
                        "snippet": self._snippet(str(item.get("body") or item.get("summary") or ""), text),
                        "timestamp": item.get("created_at") or item.get("updated_at") or row["updated_at"],
                        "source_id": item.get("source_id"),
                        "source_type": "connector",
                        "entity_id": str(row["id"]),
                        "detail_kind": {"email": "email", "event": "calendar", "todo": "todo", "note": "document"}.get(str(row["kind"]), str(row["kind"])),
                        "can_view_detail": True,
                        "can_open_source": True,
                        "source_ref": {"item_id": str(row["id"]), "source_id": item.get("source_id"), "provider": row["provider"]},
                    }
                )
        if "memory" in scopes:
            statuses = "'active','stale'" if for_agent else "'active','pending_review'"
            order = "updated_at DESC,id DESC" if for_agent else "updated_at DESC"
            memory_results: list[dict[str, Any]] = []
            offset = 0
            # Filter before consuming the result quota. Otherwise a page of
            # blocked matches can hide valid memories beyond the first LIMIT.
            while len(memory_results) < limit:
                rows = await self.db.fetchall(
                    f"SELECT id,data_json,updated_at FROM memories WHERE status IN ({statuses}) "
                    f"AND data_json LIKE ? ORDER BY {order} LIMIT ? OFFSET ?",
                    (needle, limit, offset),
                )
                for row in rows:
                    item = loads(row["data_json"], {})
                    visible = FactProtocol.agent_record(item, purpose="current") if for_agent else item
                    if visible is None:
                        continue
                    # Agent search exposes only the approved summary. Full
                    # content still goes through memory_read's usage checks.
                    snippet = visible.get("summary") if for_agent else item.get("content") or item.get("summary")
                    result = {
                        "id": str(row["id"]),
                        "type": "memory",
                        "source": "memory",
                        "title": str(visible.get("title") or "长期记忆"),
                        "snippet": self._snippet(str(snippet or ""), text),
                        "timestamp": item.get("updated_at") or row["updated_at"],
                        "status": visible.get("status"),
                        "source_type": "memory",
                        "entity_id": str(row["id"]),
                        "detail_kind": "memory",
                        "can_view_detail": True,
                        "can_open_source": True,
                        "source_ref": {"memory_id": str(row["id"])},
                    }
                    if for_agent:
                        result.update(usage_state=visible["usage_state"], usage_guidance=visible["usage_guidance"])
                    memory_results.append(result)
                    if len(memory_results) == limit:
                        break
                if not for_agent or len(rows) < limit:
                    break
                offset += len(rows)
            results.extend(memory_results)
        if "tasks" in scopes:
            for table, kind in (("tasks", "task"), ("goals", "goal"), ("projects", "project"), ("actions", "action")):
                rows = await self.db.fetchall(
                    f"SELECT id,data_json,updated_at FROM {table} WHERE data_json LIKE ? ORDER BY updated_at DESC LIMIT ?",
                    (needle, limit),
                )
                for row in rows:
                    item = loads(row["data_json"], {})
                    results.append(
                        {
                            "id": str(row["id"]),
                            "type": kind,
                            "source": "personal",
                            "title": str(item.get("title") or item.get("name") or kind),
                            "snippet": self._snippet(str(item.get("description") or item.get("goal") or item.get("content") or ""), text),
                            "timestamp": item.get("updated_at") or row["updated_at"],
                            "status": item.get("status"),
                        "source_type": "personal",
                        "entity_id": str(row["id"]),
                        "detail_kind": kind,
                        "can_view_detail": True,
                        "can_open_source": True,
                        "source_ref": {"object_id": str(row["id"]), "object_type": kind},
                        }
                    )
        results.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
        return results[:limit]

    @staticmethod
    def _snippet(content: str, query: str, width: int = 280) -> str:
        value = re.sub(r"\s+", " ", content).strip()
        if len(value) <= width:
            return value
        index = value.lower().find(query.lower())
        start = max(0, index - width // 3) if index >= 0 else 0
        return ("…" if start else "") + value[start : start + width] + ("…" if start + width < len(value) else "")
