from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from weakref import WeakValueDictionary

from .database import Database
from .memory_matching import lexical_match, normalized, query_terms
from .memory_protocol import (
    INDEPENDENT_RELATIONS,
    READ_GUIDANCE,
    FactProtocol,
    MemoryDatabase,
    atomic_memory,
    bind_memory_evidence,
    decision_revision,
    evidence_records,
    fact_fields,
    is_rejected,
    normalize_memory_evidence,
    operation_id,
    partial_replacement,
    unique_records,
    usage_state,
)
from .memory_quality import (
    EXTRACTION_RULES,
    MAX_EXTRACTION_ITEMS,
    MemoryQuality,
    bind_fact_details,
    canonical_subject,
    detail_sources,
    extraction_issues,
    fact_search_text,
    rank_current_candidates,
    render_memory_index,
    source_search_text,
    subject_evidence_conflict,
)
from .util import dumps, loads, now, uid

ReflectionModel = Callable[[str], Awaitable[str]]


class MemoryService(MemoryQuality, FactProtocol):
    def __init__(self, db: Database, *, max_active: int = 50):
        self.db = MemoryDatabase(db)
        self.max_active = max_active
        self._reflection_locks = WeakValueDictionary()
        self._assessment_locks = WeakValueDictionary()

    async def policy(self) -> dict[str, Any]:
        saved = await self.db.get_json("memory_policy", {})
        def seconds(key, default, low, high):
            try:
                return max(low, min(high, float(saved.get(key, default))))
            except (TypeError, ValueError):
                return default
        total = seconds("relation_timeout_seconds", 600, 30, 900)
        return {"max_active": int(saved.get("max_active", self.max_active)),
                "relation_timeout_seconds": total,
                "relation_http_read_seconds": min(total, seconds("relation_http_read_seconds", 300, 10, 600))}

    async def update_policy(self, max_active: int) -> dict[str, Any]:
        if isinstance(max_active, bool) or not 10 <= int(max_active) <= 2000:
            raise ValueError("活跃记忆维护目标必须在 10～2000 条之间")
        policy = {**await self.policy(), "max_active": int(max_active)}
        await self.db.set_json("memory_policy", policy)
        return policy

    async def _save_status(self) -> str:
        privacy = await self.db.get_json("privacy_settings", {})
        return "active" if privacy.get("memory_review_mode") == "automatic" else "pending_review"

    async def _add_support(self, memory: dict[str, Any], source_ids: list[str], evidence: str | list[str] | None) -> bool:
        previous = set(memory.get("source_ids") or [])
        sources = list(dict.fromkeys([*(memory.get("source_ids") or []), *source_ids]))
        new_count = len(set(sources) - previous)
        memory["source_ids"] = sources
        memory["support_count"] = max(int(memory.get("support_count") or 1), len(previous), 1) + new_count
        memory["evidence"] = list(dict.fromkeys([*(memory.get("evidence") or []), *self._normalize_evidence(evidence)]))[:8]
        return new_count > 0

    @atomic_memory
    async def list(self, *, status: str | None = None, sensitive: bool | None = None, limit: int = 500) -> dict[str, Any]:
        query = "SELECT data_json FROM memories"
        args: list[Any] = []
        clauses: list[str] = []
        if status:
            clauses.append("status=?")
            args.append(status)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 2000)))
        rows = await self.db.fetchall(query, tuple(args))
        items: list[dict[str, Any]] = []
        for row in rows:
            item = loads(row["data_json"], {})
            if await self._upgrade_protocol_record(item):
                await self._save(item)
            state = usage_state(item)
            normalize_memory_evidence(item)
            items.append({**item, "usage_state": state, "usage_guidance": READ_GUIDANCE[state]})
        if sensitive is not None:
            items = [item for item in items if bool(item.get("sensitive")) is sensitive]
        active = [item for item in items if item.get("status") in {"active", "stale"}]
        pending = [item for item in items if item.get("status") == "pending_review"]
        archived = [item for item in items if item.get("status") == "archived"]
        return {
            "core": await self.render_core(),
            "core_entries": await self.core_entries_detailed(),
            "active": active,
            "pending_review": pending,
            "archived": archived,
            "active_count": len(active),
            "pending_count": len(pending),
            "max_active": (await self.policy())["max_active"],
            "maintenance": await self.db.get_json("memory_maintenance_state", {"last_run_at": None, "archived": 0}),
            "evaluation": await self.db.get_json(
                "memory_evaluation_state",
                {"checked_at": None, "passed": 0, "failed": 0, "cases": []},
            ),
            "pending_core_updates": await self.db.get_json("pending_core_memory_updates", {}),
        }

    async def create(
        self, *, title: str, summary: str, content: str, reason: str = "agent_tool",
        status: str | None = None, confidence: float | None = None,
        source_type: str | None = None, source_id: str | None = None, source_label: str | None = None,
        observed_at: str | None = None, contacts: list[str] | None = None, project_id: str | None = None,
        sensitive: bool | None = None, sensitive_fields: list[str] | None = None,
        model_confidence: float | None = None, confidence_source: str | None = None,
        user_confirmed: bool | None = None, evidence: str | list[str] | None = None,
        extraction_reason: str | None = None, support_count: int = 1, source_ids: list[str] | None = None,
        relation_model: ReflectionModel | None = None, fact: dict[str, Any] | None = None,
        provenance: list[dict[str, Any]] | None = None, _target_id: str | None = None,
        _operation_id: str | None = None,
        _source_records: list[dict[str, Any]] | None = None,
        _correction_key: str | None = None,
        _extraction_context_key: str | None = None,
        _extraction_model: ReflectionModel | None = None,
        _extraction_failures: list[str] | None = None,
    ) -> dict[str, Any]:
        stamp = now()
        manual = reason in {"manual", "user_confirmed"}
        provenance, evidence, unverified_evidence = bind_memory_evidence(provenance, evidence, _source_records)
        mode = await self._save_status()
        status = status if status in {"active", "pending_review", "archived"} else "active" if manual else mode
        automatic = not manual and mode == "active" and status == "active"
        # Callers cannot bypass the user's review mode by suggesting active.
        if not manual and mode == "pending_review" and status == "active":
            status = "pending_review"
        supplied = model_confidence if model_confidence is not None else confidence
        model_value, parsed = self._parse_model_confidence(supplied)
        final, _ = self._parse_model_confidence(confidence if confidence is not None else supplied)
        if not manual and confidence_source != "rule_adjusted":
            adjusted = self._apply_confidence_rules(final, source_type=source_type)
            if adjusted != final:
                confidence_source = "rule_adjusted"
            final = adjusted
        source_list = list(dict.fromkeys(str(x) for x in [source_id, *(source_ids or [])] if x))
        fact = fact_fields(fact)
        fact["subject"] = canonical_subject(fact.get("subject"), provenance, _source_records or [])
        fact = bind_fact_details(fact, _source_records or [], evidence_records)
        provenance = unique_records([*provenance, *detail_sources(fact)])
        fields = sorted(set((sensitive_fields or []) + self._sensitive_fields(" ".join(
            [title, summary, content, fact_search_text({"fact": fact}), *self._normalize_evidence(evidence), *(str(p.get("quote") or "") for p in provenance or [])]))))
        confirmed = manual and bool(user_confirmed if user_confirmed is not None else True)
        fact = fact_fields(fact)
        if not manual and reason in {"agent_tool", "post_run_reflection", "remember_tool"}:
            if not provenance:
                fact = fact_fields(None)
                fact["kind"] = "hypothesis"
            elif not any(p.get("role") == "user" for p in provenance):
                fact["kind"] = "hypothesis"
        # Interpret explicit date-only/local time boundaries in the user's
        # configured zone; never use the machine's timezone or recording date.
        profile = await self.db.get_json("personal_profile", {})
        fact["timezone"] = str(profile.get("timezone") or "Asia/Shanghai")
        key = operation_id(_operation_id, "", {}) if _operation_id else operation_id(source_id, content, {**fact, "project_id": project_id, "target_id": _target_id})
        incoming = {
            "id": uid(), "title": title.strip()[:120] or "Memory", "summary": summary.strip(), "content": content.strip(),
            "created_at": stamp, "updated_at": stamp, "last_accessed_at": stamp, "access_count": 0, "revision": 1,
            "status": status, "source": reason, "source_type": source_type or reason, "source_id": source_id,
            "source_ids": source_list, "source_label": source_label or reason, "observed_at": observed_at or stamp,
            "contacts": list(contacts or []), "project_id": project_id, "confidence": final, "model_confidence": model_value,
            "confidence_source": "default_fallback" if parsed == "default_fallback" else confidence_source or ("manual" if manual else parsed),
            "acceptance_source": "user" if confirmed else "pending_review" if status == "pending_review" else "automatic",
            "user_confirmed": confirmed, "confirmed_by_user": confirmed, "evidence": self._normalize_evidence(evidence),
            "unverified_evidence": unverified_evidence,
            "extraction_reason": extraction_reason or reason, "support_count": max(1, len(source_list)),
            "scope": "personal", "sensitivity": "sensitive" if fields or sensitive else "normal",
            "sensitive": bool(fields or sensitive), "sensitive_fields": fields, "conflict_status": "none", "conflicts": [],
            "relation_kind": None, "relation_reason": None, "replaces_id": None, "expires_at": None,
            "last_update_reason": reason, "archive_reason": None, "fact": fact,
            "provenance": list(provenance or []), "fact_state": "qualified" if fact["kind"] in {"plan", "hypothesis", "reported"} else "current",
            "decision_state": "awaiting_confirmation" if status == "pending_review" else "resolved",
        }
        saved = await self._operation_result(key)
        if saved is not None:
            return saved
        incoming["_target_id"] = _target_id
        if _correction_key:
            incoming["correction_budget_key"] = _correction_key
        if _extraction_context_key:
            incoming["extraction_context_key"] = _extraction_context_key
        if _extraction_failures:
            incoming["extraction_validation_failures"] = list(_extraction_failures)
        candidates, decisions = await self._adjudicate(incoming, relation_model)
        # No database transaction spans model inference. Candidate revisions are
        # checked under the commit lock; concurrent changes require reassessment.
        result = await self._commit_fact(incoming, candidates, decisions, automatic=automatic, operation_key=key)
        if automatic:
            result = await self.recover_assessment(result, relation_model, extraction_model=_extraction_model)
        await self.maintain()
        return result

    @atomic_memory
    async def get(self, memory_id: str, *, touch: bool = True, run_id: str | None = None) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (memory_id,))
        if not row:
            raise KeyError("memory not found")
        memory = loads(row["data_json"], {})
        if await self._upgrade_protocol_record(memory):
            await self._save(memory)
        if touch:
            await self.record_usage(memory_id, "read", run_id=run_id)
            memory = await self.get(memory_id, touch=False)
        return memory

    @atomic_memory
    async def record_usage(self, memory_id: str, kind: str, *, run_id: str | None) -> None:
        if kind not in {"injected", "read", "cited"}:
            raise ValueError("Unknown memory usage kind")
        stamp = now()
        run_key = run_id or f"manual:{stamp[:10]}"
        def event_id(action: str) -> str:
            return "memory-use-" + hashlib.sha256(f"{memory_id}:{run_key}:{action}".encode()).hexdigest()
        db = self.db.connection()
        if db is not None:
            row = await (await db.execute("SELECT data_json FROM memories WHERE id=?", (memory_id,))).fetchone()
            if not row:
                return
            memory = loads(row["data_json"], {})
            reinforce = memory.get("status") in {"active", "stale"} and usage_state({**memory, "status": "active"}) == "current" and float(memory.get("confidence") or 0) >= .6
            cursor = await db.execute("INSERT OR IGNORE INTO audit_log(id,entity_type,entity_id,action,actor,data_json,created_at) VALUES(?,?,?,?,?,?,?)", (event_id(kind), "memory_usage", memory_id, kind, "agent" if run_id else "user", dumps({"run_id": run_id}), stamp))
            if cursor.rowcount:
                count_key = {"injected": "injection_count", "read": "access_count", "cited": "citation_count"}[kind]
                memory[count_key] = int(memory.get(count_key) or 0) + 1
                if kind == "read":
                    memory["last_read_at"] = stamp
                if kind != "read" or reinforce:
                    memory[{"injected": "last_injected_at", "read": "last_accessed_at", "cited": "last_cited_at"}[kind]] = stamp
                # Exposure alone never refreshes a fact. Repeated reads/citations
                # in one run count once; daily reinforcement is bounded.
                if kind in {"read", "cited"} and reinforce:
                    reinforced = await db.execute("INSERT OR IGNORE INTO audit_log(id,entity_type,entity_id,action,actor,data_json,created_at) VALUES(?,?,?,?,?,?,?)", (event_id("reinforced"), "memory_usage", memory_id, "reinforced", "agent", dumps({"run_id": run_id}), stamp))
                    day_count = int(memory.get("daily_reinforcements") or 0) if memory.get("reinforcement_day") == stamp[:10] else 0
                    if reinforced.rowcount and day_count < 5:
                        memory["reinforcement_count"] = int(memory.get("reinforcement_count", max(0, int(memory.get("access_count") or 0) - (1 if kind == "read" else 0)))) + 1
                        memory["last_reinforced_at"] = stamp
                        memory["reinforcement_day"] = stamp[:10]
                        memory["daily_reinforcements"] = day_count + 1
                await db.execute("UPDATE memories SET data_json=? WHERE id=?", (dumps(memory), memory_id))

    async def record_citations(self, text: str, *, run_id: str) -> list[str]:
        seen = await self.db.fetchall("SELECT DISTINCT entity_id FROM audit_log WHERE entity_type='memory_usage' AND action IN ('injected','read') AND json_extract(data_json,'$.run_id')=?", (run_id,))
        allowed = {str(row["entity_id"]) for row in seen}
        cited = sorted(set(re.findall(r"\[M:([A-Za-z0-9_-]+)\]", text)) & allowed)
        for memory_id in cited:
            await self.record_usage(memory_id, "cited", run_id=run_id)
        return cited

    async def update(
        self,
        memory_id: str,
        *,
        title: str | None = None,
        summary: str | None = None,
        content: str | None = None,
        reason: str = "agent_tool",
        confidence: float | None = None,
        contacts: list[str] | None = None,
        project_id: str | None = None,
        model_confidence: float | None = None,
        confidence_source: str | None = None,
        source_type: str | None = None,
        source_id: str | None = None,
        source_label: str | None = None,
        observed_at: str | None = None,
        evidence: str | list[str] | None = None,
        extraction_reason: str | None = None,
        support_count: int | None = None,
        source_ids: list[str] | None = None,
        relation_model: ReflectionModel | None = None,
        fact: dict[str, Any] | None = None,
        provenance: list[dict[str, Any]] | None = None,
        _operation_id: str | None = None,
        _source_records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        saved = await self._operation_result(operation_id(_operation_id, "", {})) if _operation_id else None
        if saved is not None:
            return saved
        memory = await self.get(memory_id, touch=False)
        material = any(value is not None and normalized(str(memory.get(key) or "")) != normalized(value) for key, value in (("title", title), ("summary", summary), ("content", content)))
        material = material or any(value is not None and value != memory.get(key) for key, value in (
            ("fact", fact), ("project_id", project_id), ("confidence", confidence), ("model_confidence", model_confidence),
            ("contacts", contacts), ("source_type", source_type), ("observed_at", observed_at)))
        if reason not in {"manual", "user_confirmed"}:
            incoming = {"title": title if title is not None else memory["title"], "summary": summary if summary is not None else memory["summary"], "content": content if content is not None else memory["content"], "project_id": project_id if project_id is not None else memory.get("project_id")}
            return await self.create(**incoming, reason=reason, confidence=confidence if confidence is not None else memory.get("confidence"), model_confidence=model_confidence if model_confidence is not None else memory.get("model_confidence"), confidence_source=confidence_source, source_type=source_type or memory.get("source_type"), source_id=source_id, source_ids=source_ids, evidence=evidence, extraction_reason=extraction_reason, contacts=contacts if contacts is not None else memory.get("contacts"), observed_at=observed_at, relation_model=relation_model, fact=fact, provenance=provenance, _target_id=memory_id, _operation_id=_operation_id, _source_records=_source_records)
        expected_revision = decision_revision(memory)
        provenance, evidence, notes = bind_memory_evidence(provenance, evidence, _source_records)
        memory["unverified_evidence"] = list(dict.fromkeys([*(memory.get("unverified_evidence") or []), *notes]))[:32]
        if fact is not None:
            memory["fact"] = fact_fields(fact)
            memory["fact"]["timezone"] = str((await self.db.get_json("personal_profile", {})).get("timezone") or "Asia/Shanghai")
        elif material:
            memory["fact"] = fact_fields(None)
        if provenance is not None:
            memory["provenance"] = unique_records([*(memory.get("provenance") or []), *provenance])
        if title is not None:
            memory["title"] = title.strip()[:120]
        if summary is not None:
            memory["summary"] = summary.strip()
        if content is not None:
            memory["content"] = content.strip()
        if confidence is not None:
            memory["confidence"], memory["confidence_source"] = self._parse_model_confidence(confidence)
        if model_confidence is not None:
            memory["model_confidence"], parsed_source = self._parse_model_confidence(model_confidence)
            if parsed_source == "default_fallback":
                memory["confidence_source"] = parsed_source
        if confidence_source is not None and memory.get("confidence_source") != "default_fallback":
            memory["confidence_source"] = str(confidence_source)[:80]
        if source_type is not None:
            memory["source_type"] = str(source_type)[:80]
        await self._add_support(memory, list(dict.fromkeys(([source_id] if source_id else []) + list(source_ids or []))), evidence)
        if source_id is not None:
            memory["source_id"] = str(source_id)[:200]
            current_ids = [str(value) for value in (memory.get("source_ids") or []) if str(value).strip()]
            if str(source_id).strip() not in current_ids:
                current_ids.append(str(source_id).strip())
            memory["source_ids"] = current_ids
        if source_label is not None:
            memory["source_label"] = str(source_label)[:200]
        if observed_at is not None:
            memory["observed_at"] = str(observed_at)[:80]
        if extraction_reason is not None:
            memory["extraction_reason"] = str(extraction_reason)[:1000]
        if contacts is not None:
            memory["contacts"] = list(contacts)
        if project_id is not None:
            memory["project_id"] = project_id
        detected = self._sensitive_fields(
            " ".join([str(memory.get("title") or ""), str(memory.get("summary") or ""), str(memory.get("content") or ""),
                      *(memory.get("evidence") or []), *(str(p.get("quote") or "") for p in memory.get("provenance") or [])])
        )
        memory["sensitive_fields"] = sorted(set((memory.get("sensitive_fields") or []) + detected))
        memory["sensitive"] = bool(memory.get("sensitive") or memory["sensitive_fields"])
        memory["sensitivity"] = "sensitive" if memory["sensitive"] else "normal"
        memory["revision"] = int(memory.get("revision", 1)) + 1
        memory["updated_at"] = now()
        memory["last_update_reason"] = reason
        async with self.db.atomic():
            current = await self.get(memory_id, touch=False)
            if decision_revision(current) != expected_revision:
                raise ValueError("记忆已被其他操作更新，请刷新后重试")
            # Preserve usage accounting written while this edit was prepared.
            for key in ("access_count", "injection_count", "citation_count", "last_accessed_at", "last_read_at", "last_injected_at", "last_cited_at",
                        "reinforcement_count", "last_reinforced_at", "reinforcement_day", "daily_reinforcements"):
                if key in current:
                    memory[key] = current[key]
            await self._save(memory, version=True)
            await self._audit("updated", memory_id, {"revision": memory.get("revision"), "reason": reason})
        return memory

    @atomic_memory
    async def archive(self, memory_id: str, reason: str = "agent_tool") -> dict[str, Any]:
        memory = await self.get(memory_id, touch=False)
        if is_rejected(memory):
            memory["acceptance_source"] = "rejected"
        memory.update(status="archived", archive_reason=reason, updated_at=now())
        memory["revision"] = int(memory.get("revision", 1)) + 1
        await self._save(memory, version=True)
        await self._audit("archived", memory_id, {"reason": reason})
        return memory

    async def _detach_conflict(self, memory_id: str, *, reason: str) -> None:
        """Remove one rejected/deleted side without approving the other side."""
        rows = await self.db.fetchall("SELECT data_json FROM memories")
        for row in rows:
            peer = loads(row["data_json"], {})
            if peer["id"] == memory_id or memory_id not in (peer.get("conflicts") or []):
                continue
            peer["conflicts"] = [value for value in peer["conflicts"] if value != memory_id]
            if not peer["conflicts"]:
                peer["conflict_status"] = "none"
                peer["decision_state"] = "awaiting_confirmation" if peer.get("status") == "pending_review" else "resolved"
            peer.update(updated_at=now(), last_update_reason=reason)
            peer["revision"] = int(peer.get("revision", 1)) + 1
            await self._save(peer, version=True)

    @atomic_memory
    async def confirm(self, memory_id: str, *, replacement_scope: dict[str, Any] | None = None) -> dict[str, Any]:
        memory = await self.get(memory_id, touch=False)
        if replacement_scope is not None:
            # A peer may have been deleted/resolved since the dialog opened,
            # leaving no conflict for the later resolver to validate.
            pair = await self._conflict_pair(memory, include_proposals=True)
            if pair is None:
                raise ValueError("关联记忆已变化或删除，请刷新列表后重新处理")
            other = next(item for item in pair if item["id"] != memory_id)
            self._manual_replacement_decision(memory, other, replacement_scope)
        proposal = await self._confirm_proposal(memory, replacement_scope=replacement_scope)
        if proposal is not None:
            return proposal
        if (memory.get("fact_state") == "not_adopted" and memory.get("retained_ids")
                and (memory.get("fact") or {}).get("kind") not in {"plan", "hypothesis", "reported"}):
            existing = []
            for retained_id in memory["retained_ids"]:
                row = await self.db.fetchone("SELECT 1 FROM memories WHERE id=?", (retained_id,))
                if row:
                    existing.append(retained_id)
            if existing:
                memory.update(conflicts=existing, conflict_status="needs_review", replaces_id=existing[0])
                await self._save(memory)
        if memory.get("conflicts"):
            return await self.resolve_conflict(memory_id, "accept_new", _chosen_id=memory_id, replacement_scope=replacement_scope)
        memory.update(status="active", user_confirmed=True, confirmed_by_user=True, acceptance_source="user",
                      decision_state="resolved", conflict_status="none", conflicts=[], archive_reason=None,
                      fact_state="qualified" if (memory.get("fact") or {}).get("kind") in {"plan", "hypothesis", "reported"} else "current",
                      user_resolution={"action": "confirm", "at": now()}, updated_at=now(), last_update_reason="user_confirmed")
        # A superseded version cannot be made current behind its successor's back.
        if memory.get("superseded_by"):
            successor = await self.db.fetchone("SELECT 1 FROM memories WHERE id=?", (memory["superseded_by"],))
            if successor:
                raise ValueError("此记忆已被替代，请先回滚并处理与当前版本的关系")
            memory["superseded_by"] = None
        memory["revision"] = int(memory.get("revision", 1)) + 1
        await self._save(memory, version=True)
        await self._audit("confirmed", memory_id, {"status": "active"})
        return memory

    @atomic_memory
    async def reject(self, memory_id: str) -> dict[str, Any]:
        memory = await self.get(memory_id, touch=False)
        memory.update(status="archived", user_confirmed=False, confirmed_by_user=False,
                      acceptance_source="rejected", archive_reason="user_rejected", decision_state="resolved",
                      conflict_status="none", conflicts=[], user_resolution={"action": "reject", "at": now()},
                      updated_at=now(), last_update_reason="user_rejected")
        memory["revision"] = int(memory.get("revision", 1)) + 1
        await self._save(memory, version=True)
        await self._detach_conflict(memory_id, reason="conflict:peer_rejected")
        await self._audit("rejected", memory_id, {"status": "archived"})
        return memory

    async def _conflict_pair(self, target, *, include_proposals=False):
        ids = target.get("conflicts") or []
        if include_proposals and not ids:
            if (target.get("adjudication") or {}).get("state") == "incomplete":
                raise ValueError("关系判断尚未完成，请先重新判断")
            if target.get("status") == "pending_review":
                ids = [d["target_id"] for d in target.get("relations") or [] if d.get("relation") not in INDEPENDENT_RELATIONS]
            if not ids and target.get("fact_state") == "not_adopted":
                ids = target.get("retained_ids") or []
        related = []
        for memory_id in dict.fromkeys(ids):
            row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (memory_id,))
            if row:
                related.append(loads(row["data_json"], {}))
        if not related:
            return None
        if len(related) != 1 or any(set(m.get("conflicts") or []) - {target["id"]} for m in related):
            raise ValueError("存在多条关联争议，请先逐条处理不适用记录；不会自动归档整个关联组")
        peer = related[0]
        if target.get("replaces_id") == peer["id"] or (include_proposals and not target.get("conflicts")):
            return peer, target
        if peer.get("replaces_id") == target["id"]:
            return target, peer
        return tuple(sorted([target, peer], key=lambda m: (str(m.get("created_at") or ""), m["id"])))

    @staticmethod
    def _replacement_review_token(chosen, other):
        # Internal confirmation may add dispute links before resolving them.
        # IDs, revisions and content still identify exactly what was reviewed.
        return hashlib.sha256(dumps([[m["id"], m.get("revision", 1), m.get("content")] for m in (chosen, other)]).encode()).hexdigest()

    @atomic_memory
    async def conflict_preview(self, memory_id: str, *, confirm: bool = False) -> dict[str, Any]:
        target = await self.get(memory_id, touch=False)
        pair = await self._conflict_pair(target, include_proposals=True)
        if pair is None:
            raise ValueError("没有需要指定替换范围的关联记忆，请刷新列表")
        old, new = pair
        chosen = target if confirm else new
        other = old if chosen["id"] == new["id"] else new
        keys = ("id", "title", "content", "revision", "status", "fact")
        return {"chosen": {k: chosen.get(k) for k in keys}, "other": {k: other.get(k) for k in keys},
                "review_token": self._replacement_review_token(chosen, other)}

    def _manual_replacement_decision(self, chosen, other, scope):
        if not isinstance(scope, dict) or scope.get("confirmed") is not True:
            raise ValueError("请先查看替换范围，选择旧事实片段或明确确认替换整条记忆")
        if (scope.get("target_id") != other["id"] or scope.get("chosen_id") != chosen["id"]
                or scope.get("review_token") != self._replacement_review_token(chosen, other)):
            raise ValueError("待处理记忆已发生变化，请重新打开范围预览后确认")
        fragments = scope.get("superseded_fragments") or []
        if scope.get("replace_entire_record") is True:
            if fragments:
                raise ValueError("不能同时选择整条替换和局部片段")
            return {"partial_update_safe": False, "covers_entire_record": True}
        decision = {"partial_update_safe": True, "superseded_fragments": fragments, "covers_entire_record": False}
        if not partial_replacement(other, decision):
            raise ValueError("请填写旧正文中唯一且完整的独立句或分句；剩余内容必须非空，不能只替换单个值")
        return decision

    @atomic_memory
    async def resolve_conflict(
        self, memory_id: str, action: str, *, merged_title: str | None = None,
        merged_summary: str | None = None, merged_content: str | None = None,
        _chosen_id: str | None = None,
        replacement_scope: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """A user decision is explicit and versioned; never infer it from reads."""
        target = await self.get(memory_id, touch=False)
        action = str(action or "").strip().lower()
        if action not in {"keep_old", "accept_new", "merge", "dismiss"}:
            raise ValueError("action must be keep_old, accept_new, merge, or dismiss")
        if action == "dismiss":
            return await self.reject(memory_id)
        pair = await self._conflict_pair(target)
        if pair is None:
            if action == "accept_new" and replacement_scope is not None:
                raise ValueError("关联记忆已变化或删除，请刷新列表后重新处理")
            if action == "accept_new" and target.get("conflict_status") == "needs_review":
                target["conflicts"] = []
                await self._save(target)
                return await self.confirm(memory_id)
            raise ValueError("Memory has no unresolved conflict")
        old, new = pair
        peer = old if old["id"] != target["id"] else new
        if action == "keep_old":
            await self.reject(new["id"])
            retained = await self.get(old["id"], touch=False)
            retained["user_resolution"] = {"action": "keep_old", "at": now(), "rejected_id": new["id"]}
            retained["revision"] = int(retained.get("revision", 1)) + 1
            retained.update(updated_at=now(), last_update_reason="conflict:keep_old_retained")
            await self._save(retained, version=True)
            return retained
        chosen = target if _chosen_id == target["id"] else new
        other = peer if chosen["id"] == target["id"] else target
        if action == "accept_new":
            decision = self._manual_replacement_decision(chosen, other, replacement_scope)
            history_id = await self._replace_record(other, chosen, decision, actor="user")
            chosen.update(status="active", user_confirmed=True, confirmed_by_user=True, acceptance_source="user",
                          decision_state="resolved", conflict_status="none", conflicts=[], archive_reason=None,
                          superseded_by=None, retained_ids=[], replaces_id=history_id,
                          fact_state="qualified" if (chosen.get("fact") or {}).get("kind") in {"plan", "hypothesis", "reported"} else "current",
                          supersedes=list(dict.fromkeys([*(chosen.get("supersedes") or []), history_id])),
                          user_resolution={"action": "accept_new", "chosen_id": chosen["id"], "at": now(),
                                           "replacement_scope": replacement_scope},
                          updated_at=now(), last_update_reason="conflict:accept_new")
            chosen["revision"] = int(chosen.get("revision", 1)) + 1
            await self._save(chosen, version=True)
            await self._audit("conflict_resolved", chosen["id"], {"action": action, "related_ids": [other["id"]],
                                                               "history_id": history_id, "replacement_scope": replacement_scope})
            return chosen
        if action == "merge":
            chosen["title"] = (merged_title or chosen.get("title") or "Memory").strip()[:120]
            chosen["summary"] = (merged_summary or "；".join(m.get("summary", "") for m in (old, new))).strip()
            chosen["content"] = (merged_content or "\n\n[另一来源的内容]\n".join(m.get("content", "") for m in (old, new))).strip()
            chosen["provenance"] = unique_records([*(old.get("provenance") or []), *(new.get("provenance") or [])])
            chosen["source_ids"] = list(dict.fromkeys([*(old.get("source_ids") or []), *(new.get("source_ids") or [])]))
            chosen["evidence"] = list(dict.fromkeys([*(old.get("evidence") or []), *(new.get("evidence") or [])]))[:8]
            chosen["unverified_evidence"] = list(dict.fromkeys([*(old.get("unverified_evidence") or []), *(new.get("unverified_evidence") or [])]))[:32]
            chosen["support_count"] = max(1, len(chosen["source_ids"]))
            chosen["fact"] = fact_fields(None)
            # Concatenating conflicting text is not a newly established fact.
            chosen["fact_state"] = "current" if merged_content else "qualified"
            fields = self._sensitive_fields(" ".join((chosen["title"], chosen["summary"], chosen["content"])))
            chosen["sensitive_fields"] = sorted(set((chosen.get("sensitive_fields") or []) + (other.get("sensitive_fields") or []) + fields))
            chosen["sensitive"] = bool(chosen.get("sensitive") or other.get("sensitive") or chosen["sensitive_fields"])
        else:
            chosen["fact_state"] = "qualified" if (chosen.get("fact") or {}).get("kind") in {"plan", "hypothesis", "reported"} else "current"
        chosen.update(status="active", user_confirmed=True, confirmed_by_user=True, acceptance_source="user",
                      decision_state="resolved", conflict_status="none", conflicts=[], archive_reason=None,
                      superseded_by=None, retained_ids=[], supersedes=list(dict.fromkeys([*(chosen.get("supersedes") or []), other["id"]])))
        other.update(status="archived", conflict_status="none", conflicts=[], decision_state="resolved",
                     fact_state="superseded", superseded_by=chosen["id"], archive_reason=f"conflict:{action}")
        other["supersedes"] = [value for value in (other.get("supersedes") or []) if value != chosen["id"]]
        for item in (chosen, other):
            item["user_resolution"] = {"action": action, "chosen_id": chosen["id"], "at": now()}
            item.update(updated_at=now(), last_update_reason=f"conflict:{action}")
            item["revision"] = int(item.get("revision", 1)) + 1
            await self._save(item, version=True)
        await self._audit("conflict_resolved", chosen["id"], {"action": action, "related_ids": [other["id"]]})
        return chosen

    @atomic_memory
    async def delete(self, memory_id: str) -> None:
        await self._detach_conflict(memory_id, reason="conflict:peer_deleted")
        await self.db.execute("DELETE FROM memories WHERE id=?", (memory_id,))
        await self.db.execute("DELETE FROM memories_fts WHERE memory_id=?", (memory_id,))
        if await self.db.fetchone("SELECT name FROM sqlite_master WHERE name='memory_source_fts'"):
            await self.db.execute("DELETE FROM memory_source_fts WHERE memory_id=?", (memory_id,))
        # Version/audit references intentionally survive deletion for history;
        # they never make a deleted or superseded fact current again.
        await self._audit("deleted", memory_id, {})

    @atomic_memory
    async def batch_delete(self, memory_ids: list[str]) -> int:
        ids = [str(item).strip() for item in memory_ids if str(item).strip()]
        deleted = 0
        for memory_id in ids:
            row = await self.db.fetchone("SELECT 1 FROM memories WHERE id=?", (memory_id,))
            if row:
                await self.delete(memory_id)
                deleted += 1
        if deleted:
            await self._audit("batch_deleted", None, {"count": deleted, "memory_ids": ids[:100]})
        return deleted

    async def clear(self, *, include_core: bool = False) -> dict[str, int]:
        rows = await self.db.fetchall("SELECT id FROM memories WHERE status IN ('active','stale','pending_review','archived')")
        deleted = await self.batch_delete([str(row["id"]) for row in rows])
        core_deleted = 0
        if include_core:
            entries = await self.db.get_json("core_memory_entries", {})
            core_deleted = len(entries) if isinstance(entries, dict) else 0
            await self.db.set_json("core_memory_entries", {})
        await self._audit("cleared", None, {"deleted": deleted, "core_deleted": core_deleted})
        return {"deleted": deleted, "core_deleted": core_deleted}

    async def export(self, *, status: str | None = None, include_core: bool = True) -> dict[str, Any]:
        result = await self.list(status=status, limit=2000)
        payload: dict[str, Any] = {
            "exported_at": now(),
            "version": 1,
            "memories": result["active"] + result["pending_review"] + result["archived"],
        }
        if include_core:
            payload["core_entries"] = await self.core_entries_detailed()
        return payload

    async def search(
        self,
        query: str,
        *,
        limit: int = 50,
        status: str | None = "active",
        include_sensitive: bool = False,
        source_type: str | None = None,
        project_id: str | None = None,
        contact: str | None = None,
        since: str | None = None,
        until: str | None = None,
        context_only: bool = False,
        agent_purpose: str | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_evidence_index()
        text = str(query or "").strip()
        evidence_query = not context_only and agent_purpose not in {"current", "notices"}
        has_filters = any(value is not None and str(value).strip() for value in (source_type, project_id, contact, since, until))
        if not text and not has_filters:
            return []
        terms = query_terms(text)
        if text and not terms:
            return []
        clauses, filter_args = [], []
        if status:
            clauses.append("m.status=?")
            filter_args.append(status)
        if not include_sensitive or context_only:
            clauses.append("COALESCE(json_extract(m.data_json,'$.sensitive'),0)=0")
        if context_only:
            clauses.extend(("m.status='active'", "COALESCE(json_extract(m.data_json,'$.confidence'),0)>=0.6", "COALESCE(json_extract(m.data_json,'$.conflict_status'),'none')!='needs_review'"))
        if context_only or agent_purpose:
            if agent_purpose != "notices":
                clauses.extend(("m.status!='pending_review'", "COALESCE(json_extract(m.data_json,'$.acceptance_source'),'')!='pending_review'",
                                "COALESCE(json_extract(m.data_json,'$.confidence'),0)>=0.6"))
            clauses.append("COALESCE(json_extract(m.data_json,'$.archive_reason'),'') NOT IN ('user_rejected','conflict:dismiss','conflict:keep_old')")
            clauses.append("COALESCE(json_extract(m.data_json,'$.acceptance_source'),'')!='rejected'")
            if context_only or agent_purpose == "current":
                clauses.extend(("m.status='active'" if context_only else "m.status IN ('active','stale')", "COALESCE(json_extract(m.data_json,'$.conflict_status'),'none')!='needs_review'",
                                "COALESCE(json_extract(m.data_json,'$.decision_state'),'') NOT IN ('unresolved','incomplete')",
                                "COALESCE(json_extract(m.data_json,'$.fact_state'),'current') NOT IN ('superseded','qualified','not_adopted')",
                                "COALESCE(json_extract(m.data_json,'$.fact.kind'),'unknown') NOT IN ('historical','plan','hypothesis','reported')"))
            elif agent_purpose == "notices":
                clauses.append("(m.status='pending_review' OR json_extract(m.data_json,'$.conflict_status')='needs_review' "
                               "OR json_extract(m.data_json,'$.decision_state') IN ('unresolved','incomplete') "
                               "OR json_extract(m.data_json,'$.fact_state') IN ('not_adopted','qualified') "
                               "OR json_extract(m.data_json,'$.fact.temporal_uncertain')=1 "
                               "OR json_extract(m.data_json,'$.fact.kind') IN ('plan','hypothesis','reported') "
                               "OR json_extract(m.data_json,'$.fact.valid_from') IS NOT NULL)")
            elif agent_purpose == "history":
                clauses.append("(m.status='archived' OR json_extract(m.data_json,'$.fact_state') IN ('superseded','not_adopted') "
                               "OR json_extract(m.data_json,'$.superseded_by') IS NOT NULL OR json_extract(m.data_json,'$.fact.kind')='historical' "
                               "OR json_extract(m.data_json,'$.fact.valid_until') IS NOT NULL)")
            elif agent_purpose == "disputes":
                clauses.append("(json_extract(m.data_json,'$.conflict_status')='needs_review' OR json_extract(m.data_json,'$.decision_state')='unresolved')")
            elif agent_purpose == "qualified":
                clauses.append("(json_extract(m.data_json,'$.fact_state')='qualified' OR json_extract(m.data_json,'$.fact.kind') IN ('plan','hypothesis','reported') "
                               "OR json_extract(m.data_json,'$.fact.temporal_uncertain')=1 "
                               "OR json_extract(m.data_json,'$.fact.valid_from') IS NOT NULL OR json_extract(m.data_json,'$.fact.valid_until') IS NOT NULL)")
        for key, value in (("source_type", source_type), ("project_id", project_id)):
            if value:
                clauses.append(f"LOWER(COALESCE(json_extract(m.data_json,'$.{key}'),''))=LOWER(?)" if key == "source_type" else f"json_extract(m.data_json,'$.{key}')=?")
                filter_args.append(str(value))
        if contact:
            clauses.append("EXISTS (SELECT 1 FROM json_each(m.data_json,'$.contacts') WHERE instr(LOWER(value),LOWER(?))>0)")
            filter_args.append(str(contact))
        for operator, value in ((">=", since), ("<=", until)):
            if value:
                clauses.append(f"COALESCE(json_extract(m.data_json,'$.observed_at'),json_extract(m.data_json,'$.created_at'),'') {operator} ?")
                filter_args.append(str(value))
        where = " AND ".join(clauses) or "1=1"
        candidate_limit = max(75, min(int(limit) * 5, 500))
        rows, evidence_ranks = [], {}
        fts_terms = sorted(term for term in terms if len(term) >= 3)[:96]
        if text and fts_terms:
            match = " OR ".join('"' + term.replace('"', '""') + '"' for term in fts_terms)
            sql = "SELECT m.data_json, bm25(memories_fts, 0.0, 5.0, 3.0, 2.0, 3.0, 1.5, 1.0) AS rank FROM memories_fts JOIN memories m ON m.id=memories_fts.memory_id WHERE memories_fts MATCH ?"
            sql += f" AND {where} ORDER BY rank ASC, m.id ASC LIMIT ?"
            args: list[Any] = [match, *filter_args, candidate_limit]
            try:
                rows = await self.db.fetchall(sql, tuple(args))
            except Exception:
                # FTS MATCH syntax should never make memory management unusable.
                rows = []
            if evidence_query:
                evidence_rows = await self.db.fetchall(
                    "SELECT m.data_json, bm25(memory_source_fts) AS evidence_rank FROM memory_source_fts "
                    "JOIN memories m ON m.id=memory_source_fts.memory_id WHERE memory_source_fts MATCH ? "
                    f"AND {where} ORDER BY evidence_rank ASC,m.id ASC LIMIT ?", tuple(args))
                existing = {loads(row["data_json"], {}).get("id") for row in rows}
                for row in evidence_rows:
                    mid = loads(row["data_json"], {}).get("id")
                    evidence_ranks[mid] = float(row["evidence_rank"])
                    if mid not in existing:
                        rows.append({"data_json": row["data_json"], "rank": 0.0})
        elif not text:
            rows = await self.db.fetchall(f"SELECT m.data_json, 0.0 AS rank FROM memories m WHERE {where} ORDER BY m.updated_at DESC LIMIT 500", tuple(filter_args))
        if text and (not rows or any(len(term) == 2 for term in terms)):
            # Two-character Chinese terms cannot use trigram FTS. Rank literal
            # matches before LIMIT, and search only semantic fields, not JSON keys.
            haystack = "LOWER(COALESCE(json_extract(m.data_json,'$.title'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.summary'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.content'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.fact.subject'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.fact.topic'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.fact.attribute'),'') || ' ' || COALESCE(json_extract(m.data_json,'$.fact.value'),''))"
            if evidence_query:
                haystack = "LOWER(" + haystack + " || ' ' || COALESCE((SELECT group_concat(json_extract(value,'$.quote'),' ') FROM json_each(m.data_json,'$.provenance')),''))"
            short_terms = sorted(terms)[:96]
            hits = " + ".join(f"(instr({haystack},?)>0)" for _ in short_terms)
            fallback_rows = await self.db.fetchall(
                f"SELECT m.data_json, 0.0 AS rank, ({hits}) AS hits FROM memories m WHERE {where} AND hits>0 ORDER BY hits DESC,m.updated_at DESC LIMIT ?",
                (*short_terms, *filter_args, candidate_limit),
            )
            existing = {loads(row["data_json"], {}).get("id") for row in rows}
            rows = [*rows, *(row for row in fallback_rows if loads(row["data_json"], {}).get("id") not in existing)]
        now_dt = datetime.now(UTC)
        ranked_candidates: list[tuple[dict[str, Any], float]] = []
        for row in rows:
            item = loads(row["data_json"], {})
            if agent_purpose == "notices" and usage_state(item) not in {"disputed", "qualified", "awaiting_confirmation", "assessment_pending"}:
                continue
            if (context_only and not self._eligible_for_context(item)) or (agent_purpose and agent_purpose != "notices" and
                    self.agent_record(item, purpose=agent_purpose, include_sensitive=include_sensitive) is None):
                continue
            if not include_sensitive and bool(item.get("sensitive")):
                continue
            if source_type and str(item.get("source_type") or "").lower() != str(source_type).lower():
                continue
            if project_id and str(item.get("project_id") or "") != str(project_id):
                continue
            if contact and not any(str(contact).lower() in str(value).lower() for value in (item.get("contacts") or [])):
                continue
            if since and str(item.get("observed_at") or item.get("created_at") or "") < str(since):
                continue
            if until and str(item.get("observed_at") or item.get("created_at") or "") > str(until):
                continue
            ranked_candidates.append((item, float(row["rank"] or 0.0)))
        # FTS5 BM25 is negative and lower is better; values can be as small as
        # 1e-6. Normalize against the strongest eligible match so differences
        # survive rounding without changing the other scoring weights. Zero
        # ranks come from metadata-only or substring fallback searches and
        # retain their existing neutral contribution.
        strongest_match = max((-rank for _, rank in ranked_candidates), default=0.0)
        strongest_evidence = max((-evidence_ranks.get(item["id"], 0.0) for item, _ in ranked_candidates), default=0.0)
        scored: list[dict[str, Any]] = []
        for item, fts_rank in ranked_candidates:
            try:
                age_days = max(0.0, (now_dt - datetime.fromisoformat(str(item.get("updated_at") or "").replace("Z", "+00:00"))).total_seconds() / 86400)
            except (TypeError, ValueError):
                age_days = 365.0
            searchable = " ".join(str(item.get(key) or "") for key in ("title", "summary", "content", "source_label", "project_id")) + " " + fact_search_text(item)
            primary_coverage, _ = lexical_match(text, searchable) if text else (1.0, True)
            if evidence_query:
                searchable += " " + source_search_text(item)
            coverage, credible = lexical_match(text, searchable) if text else (0.0, True)
            if not credible:
                continue
            primary = -fts_rank / strongest_match if strongest_match > 0.0 else primary_coverage
            evidence_score = -evidence_ranks.get(item["id"], 0.0) / strongest_evidence if strongest_evidence > 0 else 0.0
            # Quoted old/wrong values cannot alter the current-fact lane. In
            # history/source searches evidence is useful, but capped below
            # direct fact relevance instead of concatenated into its BM25.
            if evidence_query:
                evidence_score = max(evidence_score, lexical_match(text, source_search_text(item))[0])
                relevance = .8 * primary + .2 * evidence_score
                coverage = .8 * primary_coverage + .2 * coverage
            else:
                relevance = primary
            recency = math.exp(-age_days / 180.0)
            confidence = float(item.get("confidence", 0.5))
            access = min(1.0, math.log1p(int(item.get("access_count") or 0)) / 5.0)
            retrieval_priority = min(1.0, max(0.0, float(item.get("retrieval_priority", recency))))
            score = relevance * 0.5 + coverage * 0.2 + recency * 0.1 + confidence * 0.1 + access * 0.05 + retrieval_priority * 0.05
            if str(item.get("status")) == "stale":
                score *= 0.5
            item["search_score"] = round(score, 6)
            item["score_breakdown"] = {"fts": round(relevance, 4), "metadata": round(coverage, 4), "recency": round(recency, 4), "confidence": round(confidence, 4), "access": round(access, 4), "retrieval_priority": round(retrieval_priority, 4)}
            item["score_breakdown"].update(primary_fts=round(primary, 4), evidence_fts=round(evidence_score, 4), evidence_weight=.2 if evidence_query else 0.0)
            item["usage_state"] = usage_state(item)
            item["usage_guidance"] = READ_GUIDANCE[item["usage_state"]]
            scored.append(item)
        return sorted(scored, key=lambda value: (float(value.get("search_score") or 0), str(value.get("updated_at") or "")), reverse=True)[: max(1, min(int(limit), 200))]

    @staticmethod
    def _eligible_for_context(item: dict[str, Any]) -> bool:
        return (
            str(item.get("status")) == "active"
            and float(item.get("confidence") or 0.0) >= 0.6
            and not bool(item.get("sensitive"))
            and str(item.get("conflict_status") or "none") != "needs_review"
            and usage_state(item) == "current"
        )

    async def relevant_items(self, query: str, *, limit: int = 5) -> list[dict[str, Any]]:
        items = await self.search(query, limit=30, status="active", context_only=True)
        items = rank_current_candidates(query, items)
        selected: list[dict[str, Any]] = []
        for item in items:
            if not self._eligible_for_context(item):
                continue
            if any(item.get("project_id") == other.get("project_id") and (
                normalized(str(item.get("content") or "")) == normalized(str(other.get("content") or ""))
            ) for other in selected):
                continue
            selected.append(item)
            if len(selected) >= max(1, min(limit, 5)):
                break
        return selected

    async def evaluate(self) -> dict[str, Any]:
        """Run a small deterministic health check over the current memory corpus."""
        rows = await self.db.fetchall("SELECT data_json FROM memories")
        items = [loads(row["data_json"], {}) for row in rows]
        eligible = [item for item in items if self._eligible_for_context(item)]
        eligible_ids = {str(item.get("id")) for item in eligible}
        cases = [
            {"name": "active_high_confidence_only", "passed": all(str(item.get("status")) == "active" and float(item.get("confidence") or 0) >= 0.6 for item in eligible), "detail": f"{len(eligible)} 条可注入记忆"},
            {"name": "sensitive_memory_excluded", "passed": all(str(item.get("id")) not in eligible_ids for item in items if item.get("sensitive")), "detail": "敏感记忆不会进入上下文"},
            {"name": "low_confidence_excluded", "passed": all(str(item.get("id")) not in eligible_ids for item in items if float(item.get("confidence") or 0) < 0.6), "detail": "低置信度记忆不会进入上下文"},
            {"name": "conflict_memory_excluded", "passed": all(str(item.get("id")) not in eligible_ids for item in items if str(item.get("conflict_status") or "none") == "needs_review"), "detail": "冲突待处理记忆不会进入上下文"},
            {"name": "archived_memory_excluded", "passed": all(str(item.get("id")) not in eligible_ids for item in items if str(item.get("status")) == "archived"), "detail": "归档记忆不会进入上下文"},
        ]
        return {"passed": sum(1 for case in cases if case["passed"]), "failed": sum(1 for case in cases if not case["passed"]), "cases": cases, "checked_at": now()}

    async def versions(self, memory_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetchall("SELECT id,memory_id,revision,data_json,reason,created_at FROM memory_versions WHERE memory_id=? ORDER BY revision DESC", (memory_id,))
        return [{**loads(row["data_json"], {}), "version_id": row["id"], "memory_id": row["memory_id"], "revision": row["revision"], "reason": row["reason"], "version_created_at": row["created_at"]} for row in rows]

    async def _rollback_peers(self, current: dict[str, Any], restored: dict[str, Any]) -> list[dict[str, Any]]:
        """Guard facts reintroduced by rollback, not every historical link."""
        if restored.get("status") not in {"active", "stale", "pending_review"} or is_rejected(restored):
            return []
        linked_ids = set(current.get("conflicts") or []) | set(restored.get("conflicts") or [])
        # Reactivating a superseded record must be checked against its successor.
        # Backward edges (supersedes/replaces_id) do not reactivate their targets.
        # In legacy partial updates replaces_id can even point at the remainder.
        if current.get("superseded_by") and not restored.get("superseded_by") and restored.get("fact_state") != "superseded":
            linked_ids.add(current["superseded_by"])

        def text(value):
            return " ".join(str(value or "").split()).strip(" 。；;，,!?！？.")

        current_text, restored_text = text(current.get("content")), text(restored.get("content"))
        previous_parts = {part.get("history_id") for part in restored.get("partial_updates") or []}
        for part in current.get("partial_updates") or []:
            if not part.get("replacement_id") or current_text == restored_text:
                continue
            row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (part.get("history_id"),))
            history = loads(row["data_json"], {}) if row else {}
            if history.get("content"):
                # Historical fragments retain the exact removed clauses. Only
                # clauses actually brought back by this version require review.
                fragments = [text(value) for value in history["content"].splitlines() if text(value)]
                reintroduced = any(value in restored_text and value not in current_text for value in fragments)
            else:
                # The owner may have deleted the historical fragment. Preserve
                # the safety check when restoring a version before that edit.
                reintroduced = part.get("history_id") not in previous_parts
            if reintroduced:
                linked_ids.add(part["replacement_id"])

        peers, seen = {}, {current["id"]}
        pending = sorted(linked_ids)
        while pending:
            linked_id = pending.pop()
            if linked_id in seen:
                continue
            seen.add(linked_id)
            row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (linked_id,))
            peer = loads(row["data_json"], {}) if row else None
            if not peer or is_rejected(peer):
                continue
            if peer.get("superseded_by"):
                pending.append(peer["superseded_by"])
            elif peer.get("status") in {"active", "stale", "pending_review"}:
                peers[peer["id"]] = peer
        return [peers[key] for key in sorted(peers)]

    @atomic_memory
    async def rollback(self, memory_id: str, revision: int) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT data_json FROM memory_versions WHERE memory_id=? AND revision=?", (memory_id, int(revision)))
        if not row:
            raise KeyError("memory version not found")
        current = await self.get(memory_id, touch=False)
        restored = loads(row["data_json"], {})
        restored["id"] = memory_id
        restored["revision"] = int(current.get("revision", 1)) + 1
        restored["updated_at"] = now()
        restored["last_update_reason"] = f"rollback:{revision}"
        peers = await self._rollback_peers(current, restored)
        # Keep the edit lineage for later rollbacks, even when the selected
        # snapshot predates a partial replacement. This does not restore facts.
        if current.get("partial_updates"):
            restored["partial_updates"] = unique_records([*(restored.get("partial_updates") or []), *current["partial_updates"]])
        if peers:
            restored.update(conflicts=[peer["id"] for peer in peers], conflict_status="needs_review", decision_state="unresolved",
                            superseded_by=None, user_resolution={"action": "rollback", "revision": revision, "at": now()})
            for peer in peers:
                peer.update(conflict_status="needs_review", decision_state="unresolved", updated_at=now(), last_update_reason="conflict:peer_rollback")
                peer["conflicts"] = list(dict.fromkeys([*(peer.get("conflicts") or []), memory_id]))
                peer["revision"] = int(peer.get("revision", 1)) + 1
                await self._save(peer, version=True)
        await self._save(restored, version=True)
        await self._audit("rollback", memory_id, {"revision": revision, "conflicts": restored.get("conflicts") or []})
        return restored

    @atomic_memory
    async def maintain_decay(self) -> int:
        rows = await self.db.fetchall("SELECT data_json FROM memories WHERE status IN ('active','stale')")
        changed = 0
        now_dt = datetime.now(UTC)
        for row in rows:
            item = loads(row["data_json"], {})
            try:
                age_days = max(0.0, (now_dt - datetime.fromisoformat(str(item.get("last_reinforced_at") or item.get("last_accessed_at") or item.get("updated_at") or "").replace("Z", "+00:00"))).total_seconds() / 86400)
            except (TypeError, ValueError):
                age_days = 365.0
            score = math.exp(-age_days / 180.0) * (0.5 + min(0.5, math.log1p(int(item.get("reinforcement_count", item.get("access_count") or 0))) / 10.0)) * float(item.get("confidence", 0.5))
            score = round(score, 6)
            previous_score = item.get("decay_score")
            try:
                previous_score = round(float(previous_score), 6) if previous_score is not None else None
            except (TypeError, ValueError):
                previous_score = None
            delta = round(score - previous_score, 6) if previous_score is not None else None
            target = "stale" if score < 0.15 and item.get("status") == "active" else "active" if score >= 0.2 and item.get("status") == "stale" else item.get("status")
            reason = "periodic_decay_check"
            if target != item.get("status"):
                reason = "became_stale" if target == "stale" else "recovered_from_stale"
            elif delta is not None and delta < -0.01:
                reason = "long_term_unused"
            elif delta is not None and delta > 0.01:
                reason = "recently_reused"
            # Persist the calculated values even when the status remains the
            # same. This keeps retrieval ordering and the UI's aging details
            # in sync with the actual maintenance calculation.
            item["previous_decay_score"] = previous_score
            item["decay_score"] = score
            item["decay_delta"] = delta
            item["retrieval_priority"] = score
            item["last_decay_calculated_at"] = now()
            item["decay_reason"] = reason
            status_changed = target != item.get("status")
            if status_changed:
                item["status"] = target
                item["updated_at"] = now()
            await self._save(item)
            changed += 1
        return changed

    async def index(self) -> str:
        rows = await self.db.fetchall("SELECT data_json FROM memories WHERE status='active' ORDER BY updated_at DESC")
        lines = []
        for row in rows:
            memory = loads(row["data_json"], {})
            if not self._eligible_for_context(memory):
                continue
            lines.append(f"- {memory['id']}: {memory.get('title', '')} — {memory.get('summary', '')}")
        return "\n".join(lines)

    async def relevant_index(self, query: str, *, limit: int = 5) -> str:
        """Return only safe, active memories relevant to the current request."""
        selected = await self.relevant_items(query, limit=limit)
        return render_memory_index(selected)

    async def core_entries(self) -> dict[str, str]:
        data = await self.db.get_json("core_memory_entries", {})
        if not isinstance(data, dict):
            return {}
        return {str(key): str(value.get("value", "")) if isinstance(value, dict) else str(value) for key, value in data.items()}

    async def core_entries_detailed(self) -> list[dict[str, Any]]:
        data = await self.db.get_json("core_memory_entries", {})
        if not isinstance(data, dict):
            return []
        result = []
        for key, raw in sorted(data.items()):
            if isinstance(raw, dict):
                result.append({"key": str(key), **raw})
            else:
                result.append({"key": str(key), "value": str(raw), "revision": 1, "updated_at": None})
        return result

    async def render_core(self) -> str:
        entries = await self.core_entries()
        return "\n".join(f"- {key}: {value}" for key, value in sorted(entries.items()))

    async def update_core(self, key: str, value: str) -> dict[str, str]:
        normalized = key.strip().lower().replace(" ", "_")[:80]
        if not normalized:
            raise ValueError("core memory key is required")
        raw = await self.db.get_json("core_memory_entries", {})
        entries = raw if isinstance(raw, dict) else {}
        previous = entries.get(normalized)
        revision = int(previous.get("revision", 0)) + 1 if isinstance(previous, dict) else 1
        entries[normalized] = {"value": value.strip(), "revision": revision, "updated_at": now()}
        await self.db.set_json("core_memory_entries", entries)
        return await self.core_entries()

    async def remove_core(self, key: str) -> dict[str, str]:
        raw = await self.db.get_json("core_memory_entries", {})
        entries = raw if isinstance(raw, dict) else {}
        entries.pop(key.strip().lower().replace(" ", "_"), None)
        await self.db.set_json("core_memory_entries", entries)
        return await self.core_entries()

    async def maintain(self, model: ReflectionModel | None = None) -> list[dict[str, Any]]:
        max_active = (await self.policy())["max_active"]
        rows = await self.db.fetchall("SELECT data_json FROM memories WHERE status='active'")
        memories = [loads(row["data_json"], {}) for row in rows]
        if len(memories) <= max_active:
            await self.db.set_json(
                "memory_maintenance_state",
                {
                    "last_run_at": now(),
                    "archived": 0,
                    "deferred": 0,
                    "reason": "within_capacity",
                },
            )
            return []
        memories.sort(key=self._retention_score)
        candidates = [item for item in memories if usage_state(item) not in {"disputed", "assessment_pending"}][: min(len(memories), max(20, len(memories) - max_active + 10))]
        if model is None:
            await self.db.set_json(
                "memory_maintenance_state",
                {
                    "last_run_at": now(),
                    "archived": 0,
                    "deferred": len(memories) - max_active,
                    "reason": "maintenance_model_unavailable",
                },
            )
            return []
        prompt = (
            "Memory capacity is full. Decide which of the listed candidate memories to archive or defer. "
            "Never invent IDs. Prefer defer when uncertain. Return strict JSON: "
            "Prioritize duplicates, explicitly superseded facts and expired facts. Mere age or low access is not sufficient to archive a durable fact. "
            '{"decisions":[{"id":"candidate-id","action":"archive|defer","reason":"..."}]}. '
            f"Archive at most {len(memories) - max_active} memories. Archived memories remain recoverable.\n\n"
            + dumps(
                [
                    {
                        "id": item["id"],
                        "title": item.get("title"),
                        "summary": item.get("summary"),
                        "access_count": item.get("access_count"),
                        "updated_at": item.get("updated_at"),
                        "observed_at": item.get("observed_at"),
                        "expires_at": item.get("expires_at"),
                        "relation_kind": item.get("relation_kind"),
                        "user_confirmed": item.get("user_confirmed"),
                    }
                    for item in candidates
                ]
            )
        )
        try:
            decisions = self._json_object(await model(prompt)).get("decisions", [])
        except Exception as exc:
            await self.db.set_json(
                "memory_maintenance_state",
                {
                    "last_run_at": now(),
                    "archived": 0,
                    "deferred": len(memories) - max_active,
                    "reason": f"model_failed: {type(exc).__name__}",
                },
            )
            return []
        candidate_ids = {item["id"]: item for item in candidates}
        archived = []
        seen: set[str] = set()
        for decision in decisions if isinstance(decisions, list) else []:
            if not isinstance(decision, dict):
                continue
            memory_id = str(decision.get("id") or "")
            if memory_id in seen or memory_id not in candidate_ids:
                continue
            seen.add(memory_id)
            if decision.get("action") == "archive" and str(decision.get("reason") or "").strip() and len(archived) < len(memories) - max_active:
                async with self.db.atomic():
                    row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (memory_id,))
                    current = loads(row["data_json"], {}) if row else {}
                    if current.get("revision") != candidate_ids[memory_id].get("revision") or current.get("status") != "active" or usage_state(current) in {"disputed", "assessment_pending"}:
                        continue
                    archived.append(await self.archive(memory_id, f"capacity_maintenance: {str(decision.get('reason') or '')[:200]}"))
        await self.db.set_json(
            "memory_maintenance_state",
            {
                "last_run_at": now(),
                "archived": len(archived),
                "deferred": max(0, len(memories) - len(archived) - max_active),
                "reason": "capacity_maintenance",
            },
        )
        return archived

    @staticmethod
    def should_reflect(user_text: str, assistant_text: str) -> bool:
        # Eligibility to inspect a completed turn is not permission to save it.
        # Length, subject vocabulary, and grammatical paraphrases cannot decide
        # whether a short correction, withdrawal or attributed decision matters.
        # The extractor still filters durability and binds current user evidence.
        return bool(user_text.strip() and assistant_text.strip())

    @staticmethod
    def _normalize_confidence(value: Any, default: float = 0.45) -> float:
        if isinstance(value, bool):
            return default
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = default
        if not math.isfinite(number):
            number = default
        return round(max(0.0, min(1.0, number)), 4)

    @classmethod
    def _parse_model_confidence(cls, value: Any) -> tuple[float, str]:
        """Parse a model-returned score and preserve whether it was valid.

        A malformed model value must not look like a deliberate model estimate
        in the UI. Values outside the protocol range are treated as missing so
        the record is explicitly marked as using the fallback.
        """
        if isinstance(value, bool):
            return cls._normalize_confidence(None), "default_fallback"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return cls._normalize_confidence(None), "default_fallback"
        if not math.isfinite(number) or number < 0.0 or number > 1.0:
            return cls._normalize_confidence(None), "default_fallback"
        return cls._normalize_confidence(number), "model_returned"

    @classmethod
    def _apply_confidence_rules(
        cls,
        value: Any,
        *,
        source_type: str | None = None,
        support_count: int = 1,
        source_count: int = 1,
        conflict: bool = False,
        explicit: bool = False,
        stale: bool = False,
    ) -> float:
        source_adjustment = {
            "user": 0.08,
            "user_message": 0.08,
            "conversation": 0.04,
            "email": -0.08,
            "calendar": -0.06,
            "feishu": -0.08,
            "web": -0.12,
            "document": -0.08,
        }.get(str(source_type or "").lower(), 0.0)
        adjusted = cls._normalize_confidence(value) + source_adjustment
        adjusted += min(0.15, max(0, int(support_count) - 1) * 0.05)
        adjusted += min(0.1, max(0, int(source_count) - 1) * 0.05)
        if explicit:
            adjusted += 0.1
        if conflict:
            adjusted -= 0.2
        if stale:
            adjusted -= 0.1
        return cls._normalize_confidence(adjusted)

    @staticmethod
    def _normalize_evidence(value: str | list[str] | None) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip()[:500] for item in value if str(item).strip()][:8]
        if value is None:
            return []
        text = str(value).strip()
        return [text[:500]] if text else []

    @classmethod
    def _confidence_for_reflection(
        cls,
        *,
        model_confidence: Any,
        user_text: str,
        content: str,
        source_type: str | None = None,
        support_count: int = 1,
        source_count: int = 1,
        conflict: bool = False,
    ) -> tuple[float, str]:
        explicit_markers = ("remember", "always", "\u4ee5\u540e", "\u8bb0\u4f4f", "\u8bf7\u8bb0\u4f4f", "\u504f\u597d")
        explicit = any(marker in f"{user_text}\n{content}".lower() for marker in explicit_markers)
        try:
            value = float(model_confidence)
            source = "model_returned"
        except (TypeError, ValueError):
            value = 0.45
            source = "default_fallback"
        adjusted = cls._apply_confidence_rules(
            value,
            source_type=source_type,
            support_count=support_count,
            source_count=source_count,
            conflict=conflict,
            explicit=explicit,
        )
        if adjusted != cls._normalize_confidence(value) and source == "model_returned":
            source = "rule_adjusted"
        return adjusted, source

    async def reflect(
        self,
        *,
        user_text: str,
        assistant_text: str,
        model: ReflectionModel,
        status: str = "active",
        source_id: str | None = None,
        source_label: str | None = None,
        source_records: list[dict[str, Any]] | None = None,
        relation_model: ReflectionModel | None = None,
    ) -> list[dict[str, Any]]:
        # Persist the first extraction response before applying any item. A
        # retry reuses it, rather than asking the model to invent another set
        # of wording/slots. Different runs retain independent identities.
        identity = source_id or hashlib.sha256(dumps([user_text, assistant_text, source_label]).encode()).hexdigest()
        key = "memory-reflection:" + hashlib.sha256(identity.encode()).hexdigest()
        input_digest = hashlib.sha256(dumps([user_text, assistant_text]).encode()).hexdigest()
        lock = self._reflection_locks.setdefault(key, asyncio.Lock())
        async with lock:
            completed = await self.db.get_json(key + ":completed", None)
            if completed is not None:
                if completed.get("input_digest") != input_digest:
                    raise ValueError("同一运行的提取材料已变化，不能复用旧处理编号")
                result = []
                for memory_id in completed.get("ids", []):
                    row = await self.db.fetchone("SELECT data_json FROM memories WHERE id=?", (memory_id,))
                    if row:
                        result.append(loads(row["data_json"], {}))
                return result
            return await self._reflect_once(user_text=user_text, assistant_text=assistant_text, model=model, status=status,
                                            source_id=source_id, source_label=source_label, source_records=source_records,
                                            relation_model=relation_model, _batch_key=key)

    async def _reflect_once(
        self, *, user_text: str, assistant_text: str, model: ReflectionModel, status: str,
        source_id: str | None, source_label: str | None, source_records: list[dict[str, Any]] | None,
        relation_model: ReflectionModel | None, _batch_key: str,
    ) -> list[dict[str, Any]]:
        sources = source_records or [
            {"id": f"{source_id or 'run'}:user", "role": "user", "run_id": source_id, "text": user_text[-12000:]},
            {"id": f"{source_id or 'run'}:assistant", "role": "assistant", "run_id": source_id, "text": assistant_text[-12000:]},
        ]
        frozen = await self.db.get_json(_batch_key, None)
        if frozen is None:
            from .context import TokenEstimator
            estimator = TokenEstimator()
            remaining = max(0, 1500 - sum(estimator.count_text(str(s.get("text") or ""))
                                          for s in sources if s.get("context_only")))
            memory_context = []
            for row in await self.agent_views(await self.relevant_items(user_text, limit=2)):
                anchor = {k: row.get(k) for k in ("id", "title", "summary", "fact", "usage_state", "usage_guidance", "current_status_verified")}
                size = estimator.count_text(dumps(anchor))
                if size <= remaining:
                    memory_context.append(anchor)
                    remaining -= size
        else:
            memory_context = frozen.get("memory_context", [])
        prompt = (
            "Extract only stable user preferences, durable facts, important decisions, or reusable knowledge from this run. "
            "Do not store transient task details. For every memory, return a model confidence between 0 and 1, "
            "a short evidence list, a reason, and the source type. Never use a fixed confidence for all items. "
            "Keep each memory about one subject, topic and attribute where possible. Unknown scope stays null. "
            f"Return at most {MAX_EXTRACTION_ITEMS} memories; retain distinct supported facts without multiplying "
            "redundant inventory or ownership restatements. This is an output safety limit, not a top-five ranking. "
            "Keep entity and property labels stable across updates: the subject is the thing whose property changes, "
            "not the new value. For ownership use the asset as subject, owner as attribute and the person's name as value. "
            "Do not conflate people/projects; distinguish actual current facts, historical facts, plans, hypotheses and reported claims. "
            "Do not treat assistant suggestions as user confirmation. Effective time is NOT recording time. "
            "Copy supporting source quotes verbatim and reference only supplied source IDs. Do not invent project IDs or dates. "
            "Return strict JSON: "
            '{"memories":[{"title":"...","summary":"...","content":"...","confidence":0.0,"evidence":["..."],'
            '"reason":"...","fact":{"subject":null,"topic":null,"attribute":null,"value":null,"kind":"current|historical|plan|hypothesis|reported|unknown",'
            '"time_scope":null,"valid_from":null,"valid_until":null,"atomic":true},'
            '"provenance":[{"source_id":"supplied ID","quote":"verbatim supporting text"}]}],"core_updates":{"key":"value"}}.\n\n'
            + EXTRACTION_RULES + "\n"
            + dumps({"sources": sources, "memory_context": memory_context})
        )
        source_digest = hashlib.sha256(dumps(sources).encode()).hexdigest()
        if frozen is None:
            raw = await model(prompt)
            payload = self._json_object(raw)
            if not isinstance(payload.get("memories"), list):
                raise ValueError("Invalid memory extraction result")
            async with self.db.atomic():
                await self.db.execute("INSERT INTO kv(key,value_json) VALUES(?,?) ON CONFLICT(key) DO NOTHING",
                                      (_batch_key, dumps({"payload": payload, "raw_response": raw, "sources": sources, "source_digest": source_digest,
                                                         "memory_context": memory_context, "at": now()})))
                frozen = await self.db.get_json(_batch_key)
        if frozen.get("source_digest") != source_digest:
            raise ValueError("同一运行的提取材料已变化，不能复用旧处理编号")
        payload = frozen["payload"]
        issues = extraction_issues(payload, sources)
        if issues:
            correction = await self.quality_call(
                _batch_key + ":correction", "extraction", prompt[:prompt.index('{"sources":')] +
                "\nReview the original extraction against the sources. Preserve correct items and return the same JSON schema. "
                "These checks identify missing or inconsistent evidence, not proof that anything must be saved. "
                "Keep an empty memories list with skip_reason for transient, unsupported or otherwise ineligible assertions. "
                "Do not force an update when evidence is uncertain.\n" + dumps({"sources": sources, "memory_context": memory_context,
                    "original_extraction": payload, "review_reasons": issues}), model)
            if correction:
                try:
                    revised = self._json_object(correction)
                    if isinstance(revised.get("memories"), list):
                        payload = revised
                except (ValueError, TypeError):
                    pass
        items = payload.get("memories", [])
        if len(items) > MAX_EXTRACTION_ITEMS:
            await self._audit("extraction_batch_rejected", None, {"batch": _batch_key,
                              "reason": "item_limit_exceeded", "count": len(items), "limit": MAX_EXTRACTION_ITEMS})
            # Keep the first response/review for diagnosis and explicit recovery.
            # Do not partially save a prefix or mark the batch as completed.
            raise ValueError(f"Memory extraction exceeds {MAX_EXTRACTION_ITEMS} items; no prefix was saved")
        current_user_ids = {str(s["id"]) for s in sources if s.get("role") == "user" and not s.get("context_only")}
        created: list[dict[str, Any]] = []
        unresolved_subject = False
        for slot, item in enumerate(items):
            if not isinstance(item, dict) or not item.get("content"):
                continue
            model_confidence, confidence_source = self._parse_model_confidence(item.get("confidence"))
            if model_confidence < 0.45:
                continue
            source_type = "conversation"
            provenance, _, _ = bind_memory_evidence(item.get("provenance"), item.get("evidence"), sources)
            if not any(str(p.get("id")) in current_user_ids for p in provenance):
                await self._audit("extraction_skipped", None, {"batch": _batch_key, "slot": slot,
                                                            "reason": "no_current_user_evidence"})
                continue
            fact = fact_fields(item.get("fact"))
            subject_conflict = subject_evidence_conflict(item, sources)
            unresolved_subject |= subject_conflict
            if isinstance(item.get("subject_evidence"), list):
                fact["subject_evidence"] = item["subject_evidence"]
            if not provenance:
                fact = fact_fields(None)
            elif not any(p.get("role") == "user" for p in provenance):
                fact["kind"] = "hypothesis"
            confidence, rule_source = self._confidence_for_reflection(
                model_confidence=model_confidence,
                user_text=user_text,
                content=str(item["content"]),
                source_type=source_type,
                source_count=1,
            )
            confidence_source = confidence_source if confidence_source == "default_fallback" else rule_source
            created.append(await self.create(
                title=str(item.get("title", "Memory")), summary=str(item.get("summary", "")),
                content=str(item["content"]), reason="post_run_reflection", status=status,
                confidence=confidence, model_confidence=model_confidence, confidence_source=confidence_source,
                user_confirmed=False, evidence=item.get("evidence"), extraction_reason=item.get("reason"),
                source_type=source_type, source_id=source_id, source_label=source_label,
                relation_model=relation_model or model, fact=fact, provenance=item.get("provenance"),
                _operation_id=f"{_batch_key}:item:{slot}",
                _source_records=sources,
                _correction_key=_batch_key + ":correction",
                _extraction_context_key=_batch_key,
                _extraction_model=model,
                _extraction_failures=["subject_conflicts_with_current_speaker"] if subject_conflict else None,
            ))
        async with self.db.atomic():
            if await self.db.get_json(_batch_key + ":completed", None) is None:
                updates = payload.get("core_updates", {})
                if unresolved_subject:
                    await self._audit("core_update_withheld", None, {"batch": _batch_key,
                                      "reason": "unresolved_extraction_subject", "updates_applied": False})
                if isinstance(updates, dict) and not unresolved_subject:
                    pending = await self.db.get_json("pending_core_memory_updates", {})
                    if not isinstance(pending, dict):
                        pending = {}
                    for key, value in list(updates.items())[:10]:
                        if status == "pending_review":
                            pending[str(key).strip().lower().replace(" ", "_")] = {"value": str(value), "created_at": now()}
                        else:
                            await self.update_core(str(key), str(value))
                    if status == "pending_review":
                        await self.db.set_json("pending_core_memory_updates", pending)
                await self.db.set_json(_batch_key + ":completed", {"ids": [m["id"] for m in created if m.get("status") != "deleted"],
                                                                  "input_digest": hashlib.sha256(dumps([user_text, assistant_text]).encode()).hexdigest(), "at": now()})
        return created

    @atomic_memory
    async def _save(self, memory: dict[str, Any], *, version: bool = False) -> None:
        normalize_memory_evidence(memory)
        await self.db.execute(
            "UPDATE memories SET data_json=?,status=?,updated_at=? WHERE id=?",
            (dumps(memory), memory["status"], memory["updated_at"], memory["id"]),
        )
        await self._index(memory)
        if version:
            await self._record_version(memory, reason=str(memory.get("last_update_reason") or "update"))

    async def _audit(self, action: str, memory_id: str | None, details: dict[str, Any]) -> None:
        await self.db.execute(
            "INSERT INTO audit_log(id,entity_type,entity_id,action,actor,data_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (uid(), "memory", memory_id, action, "system", dumps(details), now()),
        )

    async def _upgrade_protocol_record(self, memory: dict[str, Any]) -> bool:
        """Backfill the confidence protocol for memories created by v0.1."""
        changed = False
        had_confidence = memory.get("confidence") is not None or memory.get("model_confidence") is not None
        model_value = memory.get("model_confidence", memory.get("confidence"))
        normalized = self._normalize_confidence(model_value)
        if memory.get("model_confidence") != normalized:
            memory["model_confidence"] = normalized
            changed = True
        final_value = self._normalize_confidence(memory.get("confidence", normalized))
        if memory.get("confidence") != final_value:
            memory["confidence"] = final_value
            changed = True
        if not memory.get("confidence_source"):
            memory["confidence_source"] = "manual" if had_confidence else "default_fallback"
            changed = True
        if "user_confirmed" not in memory:
            memory["user_confirmed"] = bool(memory.get("confirmed_by_user", False))
            changed = True
        if "evidence" not in memory:
            memory["evidence"] = []
            changed = True
        if "extraction_reason" not in memory:
            memory["extraction_reason"] = memory.get("source") or "legacy_memory"
            changed = True
        if "support_count" not in memory:
            memory["support_count"] = 1
            changed = True
        if "source_ids" not in memory:
            memory["source_ids"] = []
            changed = True
        return changed

    async def _record_version(self, memory: dict[str, Any], *, reason: str) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO memory_versions(id,memory_id,revision,data_json,reason,created_at) VALUES(?,?,?,?,?,?)",
            (uid(), memory["id"], int(memory.get("revision", 1)), dumps(memory), reason[:200], now()),
        )

    async def _ensure_evidence_index(self) -> None:
        """Rebuild derived search data once; never re-adjudicate old records."""
        if await self.db.get_json("memory_evidence_index_version", 0) == 2:
            return
        async with self.db.atomic():
            if await self.db.get_json("memory_evidence_index_version", 0) == 2:
                return
            await self.db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS memory_source_fts USING fts5(memory_id UNINDEXED, source_text, tokenize='trigram')")
            await self.db.execute("DELETE FROM memory_source_fts")
            for row in await self.db.fetchall("SELECT data_json FROM memories"):
                await self._index(loads(row["data_json"], {}))
            await self.db.set_json("memory_evidence_index_version", 2)

    async def _index(self, memory: dict[str, Any]) -> None:
        await self.db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS memory_source_fts USING fts5(memory_id UNINDEXED, source_text, tokenize='trigram')")
        await self.db.execute("DELETE FROM memory_source_fts WHERE memory_id=?", (memory["id"],))
        await self.db.execute("INSERT INTO memory_source_fts(memory_id,source_text) VALUES(?,?)", (memory["id"], source_search_text(memory)))
        await self.db.execute("DELETE FROM memories_fts WHERE memory_id=?", (memory["id"],))
        contacts = memory.get("contacts") or []
        if isinstance(contacts, list | tuple):
            contacts = " ".join(str(item) for item in contacts)
        await self.db.execute(
            "INSERT INTO memories_fts(memory_id,title,summary,content,source_text,contacts,project) VALUES(?,?,?,?,?,?,?)",
            (
                memory["id"],
                str(memory.get("title") or ""),
                str(memory.get("summary") or ""),
                str(memory.get("content") or ""),
                str(memory.get("source_label") or memory.get("source") or "") + " " + fact_search_text(memory),
                str(contacts),
                str(memory.get("project_id") or ""),
            ),
        )

    @staticmethod
    def _sensitive_fields(text: str) -> list[str]:
        fields: list[str] = []
        if re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text):
            fields.append("email")
        if re.search(r"(?<!\d)1[3-9]\d{9}(?!\d)", text):
            fields.append("phone")
        if re.search(r"(?<!\d)\d{17}[\dXx](?!\d)", text):
            fields.append("id_number")
        if re.search(r"(密码|授权码|token|secret|身份证|健康|病历|住址|家庭地址)", text, re.I):
            fields.append("sensitive_text")
        return fields

    @staticmethod
    def _retention_score(memory: dict[str, Any]) -> tuple[float, str]:
        access = int(memory.get("access_count", 0))
        updated = str(memory.get("updated_at", ""))
        try:
            age_days = max(0.0, (datetime.now(UTC) - datetime.fromisoformat(updated.replace("Z", "+00:00"))).total_seconds() / 86400)
        except (TypeError, ValueError):
            age_days = 365.0
        score = math.log1p(access) * 2.0 + max(0.0, 4.0 - age_days / 30.0) + min(2.0, int(memory.get("revision", 1)) / 3)
        return score, updated

    @staticmethod
    def _json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0]
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end < start:
            return {}
        try:
            value = json.loads(text[start : end + 1])
            return value if isinstance(value, dict) else {}
        except json.JSONDecodeError:
            return {}
