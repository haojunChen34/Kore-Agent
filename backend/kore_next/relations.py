from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from .database import Database
from .util import dumps, loads, now, uid


class ContextRelationService:
    """Create explainable links between synced emails, events, tasks and notes."""

    async def rebuild(self, db: Database) -> dict[str, int]:
        rows = await db.fetchall("SELECT id,provider,kind,data_json FROM connector_items WHERE status='active'")
        items = [(str(row["id"]), str(row["provider"]), str(row["kind"]), loads(row["data_json"], {})) for row in rows]
        await db.execute("DELETE FROM context_links WHERE source_type='connector_item'")
        created = 0
        for index, left in enumerate(items):
            for right in items[index + 1 :]:
                relation, score = self._relation(left, right)
                if not relation:
                    continue
                stamp = now()
                await db.execute(
                    "INSERT OR REPLACE INTO context_links("
                    "id,source_type,source_id,target_type,target_id,relation,score,data_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (uid(), "connector_item", left[0], "connector_item", right[0], relation, score, dumps({"reason": relation}), stamp, stamp),
                )
                created += 1
        return {"items": len(items), "links": created}

    async def related(self, db: Database, item_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = await db.fetchall(
            "SELECT l.relation,l.score,l.source_id,l.target_id,a.data_json AS source_json,b.data_json AS target_json "
            "FROM context_links l JOIN connector_items a ON a.id=l.source_id JOIN connector_items b ON b.id=l.target_id "
            "WHERE (l.source_type='connector_item' AND l.source_id=?) OR (l.target_type='connector_item' AND l.target_id=?) "
            "ORDER BY l.score DESC,l.updated_at DESC LIMIT ?",
            (item_id, item_id, max(1, min(int(limit), 100))),
        )
        result = []
        for row in rows:
            other_id = row["target_id"] if row["source_id"] == item_id else row["source_id"]
            raw = row["target_json"] if row["source_id"] == item_id else row["source_json"]
            item = loads(raw, {})
            result.append({"item_id": other_id, "relation": row["relation"], "score": row["score"], "provider": item.get("provider"), **item})
        return result

    def _relation(self, left: tuple[str, str, str, dict[str, Any]], right: tuple[str, str, str, dict[str, Any]]) -> tuple[str | None, float]:
        _, left_provider, left_kind, left_item = left
        _, right_provider, right_kind, right_item = right
        if left_provider == right_provider and left_kind == right_kind:
            return None, 0
        left_terms = self._terms(left_item)
        right_terms = self._terms(right_item)
        overlap = len(left_terms & right_terms) / max(1, len(left_terms | right_terms))
        date_score = self._date_score(left_item, right_item)
        if {left_kind, right_kind} == {"email", "event"}:
            if overlap >= 0.08 or date_score >= 0.75:
                return "email_calendar", max(overlap, date_score)
        if {left_kind, right_kind} == {"email", "todo"}:
            if overlap >= 0.12 or date_score >= 0.75:
                return "email_task", max(overlap, date_score)
        if {left_kind, right_kind} == {"event", "todo"}:
            if overlap >= 0.15 or date_score >= 0.8:
                return "calendar_task", max(overlap, date_score)
        if "note" in {left_kind, right_kind} and overlap >= 0.2:
            return "shared_topic", overlap
        return None, 0

    @staticmethod
    def _terms(item: dict[str, Any]) -> set[str]:
        text = " ".join(str(item.get(key) or "") for key in ("title", "summary", "body", "sender", "location"))
        terms = set(re.findall(r"[A-Za-z0-9@._-]{3,}|[\u4e00-\u9fff]{2,8}", text.lower()))
        return {term for term in terms if term not in {"会议", "邮件", "任务", "文档", "日历"}}

    @staticmethod
    def _date_score(left: dict[str, Any], right: dict[str, Any]) -> float:
        values_left = [left.get(key) for key in ("created_at", "starts_at", "due_at") if left.get(key)]
        values_right = [right.get(key) for key in ("created_at", "starts_at", "due_at") if right.get(key)]
        if not values_left or not values_right:
            return 0
        try:
            a = datetime.fromisoformat(str(values_left[0]).replace("Z", "+00:00")).astimezone(UTC)
            b = datetime.fromisoformat(str(values_right[0]).replace("Z", "+00:00")).astimezone(UTC)
        except (TypeError, ValueError):
            return 0
        days = abs((a - b).total_seconds()) / 86400
        return max(0.0, 1.0 - days / 7.0)
