from __future__ import annotations

import asyncio
import html
import ipaddress
import json
import os
import re
import shlex
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx

from .config import Settings
from .mcp import McpManager, McpToolError
from .research import WebResearchService, isolate_web_content, source_trust
from .sandbox import SandboxSupervisor

EventSink = Callable[[str, dict[str, Any]], Awaitable[None]]
ApprovalGate = Callable[[str, str, dict[str, Any], str], Awaitable[bool]]
DeltaSink = Callable[[str], Awaitable[None]]


def _search_result_url(raw: str) -> str | None:
    """Return an HTTP result URL, unwrapping DuckDuckGo's redirect links."""
    candidate = raw.strip()
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    parsed = urlsplit(candidate)
    if parsed.hostname in {"duckduckgo.com", "www.duckduckgo.com"} and parsed.path.startswith("/l/"):
        candidate = parse_qs(parsed.query).get("uddg", [""])[0]
        parsed = urlsplit(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return None
    return candidate


MEMORY_PURPOSE = {"type": "string", "enum": ["current", "history", "qualified", "disputes", "all"],
                  "description": "Default current. Select history for past/superseded facts and model-not-adopted claims with their sources, qualified for plans or reports, disputes to compare unresolved evidence; all preserves every usage label. Not-adopted claims are not current facts. Pending/user-rejected contents are never available."}
MEMORY_FACT = {"type": "object", "description": "One grounded subject/topic/attribute. Use null for unknown scope or effective times; recording time is not effective time.",
               "properties": {**{key: {"type": ["string", "null"]} for key in ("subject", "topic", "attribute", "value", "time_scope", "valid_from", "valid_until")},
                              "kind": {"type": "string", "enum": ["current", "historical", "plan", "hypothesis", "reported", "unknown"]},
                              "atomic": {"type": "boolean"}}}
MEMORY_PROVENANCE = {"type": "array", "description": "Verbatim supporting quotes from the current user's message. Omit source_id if unknown; the backend binds quotes to actual messages. Never quote your own inference as user evidence.",
                     "items": {"type": "object", "properties": {"source_id": {"type": "string"}, "quote": {"type": "string"}}, "required": ["quote"]}}
MEMORY_FACT["properties"].update({
    "subject_evidence": MEMORY_PROVENANCE,
    "attribution": {"type": "object", "description": "Origin of the claim, not the message role relaying it. Acceptance is scoped to this fact's subject/property/value: confirming another person's choice does not mean the user adopted that choice. It is separate from permission to store memory.",
        "properties": {"origin": {"type": "string", "enum": ["user", "third_party", "assistant", "document", "unknown"]},
                       "speaker": {"type": ["string", "null"]},
                       "adoption": {"type": "string", "enum": ["accepted", "rejected", "not_stated", "unknown"]},
                       **{key: {"type": ["string", "null"], "description": "Exact supporting clause within provenance; null if this aspect is unknown."}
                          for key in ("claim_quote", "origin_quote", "adoption_quote")},
                       "provenance": MEMORY_PROVENANCE}},
    "event": {"type": "object", "description": "Explicit change context only. Quotes are extractive; unknown cause/time stays null.",
        "properties": {**{k: {"type": ["string", "null"]} for k in ("trigger_quote", "reason_quote", "time_quote")},
                       "provenance": MEMORY_PROVENANCE}},
})

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a UTF-8 text file inside the workspace.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write a UTF-8 text file inside the workspace.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "List files in a workspace directory.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "shell",
            "description": "Run a command in the workspace after human approval.",
            "parameters": {
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "publish_file",
            "description": "Publish an existing workspace file as a deliverable.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                },
                "required": ["path", "title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remember",
            "description": "Store a durable memory with fact scope, a considered confidence and verbatim source quotes. "
                           "Automatic mode adjudicates relationships; review mode requires confirmation. Check returned status before claiming acceptance.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "content": {"type": "string"},
                    "fact": {**MEMORY_FACT, "required": ["kind"]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "model_confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "provenance": {**MEMORY_PROVENANCE, "minItems": 1},
                    "evidence": {"type": "array", "items": {"type": "string"}, "description": "Optional verbatim quotes, verified against original messages."},
                    "project_id": {"type": ["string", "null"]},
                },
                "required": ["title", "summary", "content", "fact", "confidence", "provenance"],
            },
        },
    },
]


def _definition(name: str, description: str, properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required or []},
        },
    }


TOOLS.extend(
    [
        _definition(
            "artifact_publish",
            "Publish exactly one workspace file or public http(s) URL as a deliverable.",
            {"path": {"type": "string"}, "url": {"type": "string"}, "title": {"type": "string"}, "description": {"type": "string"}},
            ["title"],
        ),
        _definition("get_current_time", "Return the current time in an IANA timezone.", {"timezone": {"type": "string"}}),
        _definition(
            "web_search",
            "Search the web and return titles, URLs and snippets.",
            {"query": {"type": "string"}, "limit": {"type": "integer"}},
            ["query"],
        ),
        _definition(
            "web_research",
            (
                "Research a question across multiple web sources, read public pages, and return cited source packets. "
                "Use for comparisons or questions that need evidence."
            ),
            {
                "query": {"type": "string"},
                "mode": {"type": "string", "enum": ["quick", "deep", "compare"]},
                "max_sources": {"type": "integer"},
                "fetch_pages": {"type": "boolean"},
                "max_queries": {"type": "integer"},
                "max_pages": {"type": "integer"},
            },
            ["query"],
        ),
        _definition(
            "research_monitor_create",
            (
                "Prepare and, after explicit user confirmation, create a persistent web research monitor. "
                "Parse natural-language schedules into a timezone-aware schedule. If no source URL is provided, "
                "search for candidate websites and return a confirmation preview; never create before confirmation."
            ),
            {
                "name": {"type": "string"},
                "query": {"type": "string"},
                "schedule": {"type": "object", "description": "可直接传结构化时间表；也可以改用 schedule_text，二选一"},
                "schedule_text": {"type": "string", "description": "自然语言时间，例如每周五早上七点"},
                "timezone": {"type": "string"},
                "source_url": {"type": "string"},
                "allowed_domains": {"type": "array", "items": {"type": "string"}},
                "notify_on_change": {"type": "boolean"},
                "confirmation_token": {"type": "string"},
                "confirmed": {"type": "boolean"},
            },
            ["name", "query"],
        ),
        _definition(
            "http_request",
            "Perform an HTTP request to a public URL.",
            {"url": {"type": "string"}, "method": {"type": "string"}, "headers": {"type": "object"}, "body": {"type": "string"}},
            ["url"],
        ),
        _definition(
            "history_search", "Search prior messages in this conversation.", {"query": {"type": "string"}, "limit": {"type": "integer"}}, ["query"]
        ),
        _definition("history_read", "Read a range of prior conversation messages.", {"offset": {"type": "integer"}, "limit": {"type": "integer"}}),
        _definition(
            "session_search",
            "Search messages across past conversations on this device.",
            {"query": {"type": "string"}, "limit": {"type": "integer"}},
            ["query"],
        ),
        _definition(
            "unified_search",
            "Search conversations, emails, Feishu items, memories, tasks and workspace files. "
            "Memory hits contain eligible current-fact summaries only; use memory_search/read for historical, "
            "conditional or disputed facts and full content.",
            {
                "query": {"type": "string"},
                "scope": {"type": "string", "enum": ["all", "messages", "connectors", "memory", "tasks", "files"]},
                "limit": {"type": "integer"},
            },
            ["query"],
        ),
        _definition(
            "file_extract",
            "Extract text and metadata from a workspace text, PDF, Word, Excel or image file.",
            {"path": {"type": "string"}, "attachment_id": {"type": "string"}, "max_chars": {"type": "integer"}},
        ),
        _definition(
            "connector_related",
            "Find emails, calendar events, Feishu tasks and notes related to one inbox item.",
            {"item_id": {"type": "string"}, "limit": {"type": "integer"}},
            ["item_id"],
        ),
        _definition("memory_list", "List eligible memory summaries with usage labels. Reading does not approve or resolve a fact.", {"status": {"type": "string"}, "sensitive": {"type": "boolean"}, "limit": {"type": "integer"}, "purpose": MEMORY_PURPOSE}),
        _definition(
            "memory_search",
            "Search long-term memory summaries. Use purpose to retrieve past facts, plans or disputes; never present those as established current facts.",
            {"query": {"type": "string"}, "purpose": MEMORY_PURPOSE, "limit": {"type": "integer"}, "status": {"type": "string", "enum": ["active", "pending_review", "stale", "archived"]}, "include_sensitive": {"type": "boolean"}, "source_type": {"type": "string"}, "project_id": {"type": "string"}, "contact": {"type": "string"}, "since": {"type": "string"}, "until": {"type": "string"}},
            ["query"],
        ),
        _definition("memory_read", "Read eligible memory content with sources and usage guidance. Pending/user-rejected content is withheld; history (including model-not-adopted claims), plans and disputes require the matching purpose. Reading a claim does not adopt it. Sensitive details require explicit opt-in. Cite [M:full-memory-id] only if used.", {"memory_id": {"type": "string"}, "include_sensitive": {"type": "boolean"}, "purpose": MEMORY_PURPOSE}, ["memory_id"]),
        _definition(
            "memory_create",
            "Propose a durable memory with grounded evidence and fact scope. Automatic mode adjudicates relationships; review mode requires user confirmation. Inspect returned status/decision_state before claiming acceptance or replacement.",
            {
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "content": {"type": "string"},
                "fact": MEMORY_FACT,
                "provenance": MEMORY_PROVENANCE,
                "source_type": {"type": "string"},
                "source_id": {"type": "string"},
                "source_label": {"type": "string"},
                "observed_at": {"type": "string"},
                "contacts": {"type": "array", "items": {"type": "string"}},
                "project_id": {"type": "string"},
                "confidence": {"type": "number"},
                "model_confidence": {"type": "number"},
                "confidence_source": {"type": "string"},
                "user_confirmed": {"type": "boolean"},
                "status": {"type": "string", "enum": ["active", "pending_review", "archived"]},
                "evidence": {"type": "array", "items": {"type": "string"}},
                "extraction_reason": {"type": "string"},
                "support_count": {"type": "integer"},
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "sensitive": {"type": "boolean"},
                "sensitive_fields": {"type": "array", "items": {"type": "string"}},
            },
            ["title", "summary", "content"],
        ),
        _definition(
            "memory_update",
            "Propose an update with verbatim evidence. Relationship adjudication may return another ID, unresolved or pending status. Check the result before claiming replacement; newer recording time alone never overrides facts.",
            {
                "memory_id": {"type": "string"},
                "fact": MEMORY_FACT,
                "provenance": MEMORY_PROVENANCE,
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "content": {"type": "string"},
                "confidence": {"type": "number"},
                "model_confidence": {"type": "number"},
                "confidence_source": {"type": "string"},
                "evidence": {"type": "array", "items": {"type": "string"}},
                "extraction_reason": {"type": "string"},
                "support_count": {"type": "integer"},
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "source_type": {"type": "string"},
                "source_id": {"type": "string"},
                "source_label": {"type": "string"},
                "observed_at": {"type": "string"},
                "contacts": {"type": "array", "items": {"type": "string"}},
                "project_id": {"type": "string"},
            },
            ["memory_id"],
        ),
        _definition("memory_archive", "Archive a long-term memory.", {"memory_id": {"type": "string"}, "reason": {"type": "string"}}, ["memory_id"]),
        _definition(
            "core_memory_update", "Set one stable core-memory entry.", {"key": {"type": "string"}, "value": {"type": "string"}}, ["key", "value"]
        ),
        _definition("core_memory_remove", "Remove one core-memory entry.", {"key": {"type": "string"}}, ["key"]),
        _definition(
            "goal_create",
            "Create an ongoing personal goal. Use this for long-term goals that remain active until the user explicitly completes, pauses, or archives them.",
            {
                "title": {"type": "string"},
                "description": {"type": "string"},
                "priority": {"type": "string", "enum": ["low", "normal", "high"]},
            },
            ["title"],
        ),
        _definition("goal_list", "List personal goals.", {"status": {"type": "string"}, "limit": {"type": "integer"}}),
        _definition("action_create", "Create a local workbench action (今日行动/后续行动), not a tracked task plan. Preserve the source ID and timezone-aware deadline.", {
            "title": {"type": "string"}, "description": {"type": "string"}, "due_at": {"type": "string"},
            "goal_id": {"type": "string"}, "source_type": {"type": "string", "enum": ["connector_item", "research_monitor_update", "conversation"]},
            "source_id": {"type": "string"}, "priority": {"type": "string"}}, ["title"]),
        _definition("action_list", "List local workbench actions.", {"status": {"type": "string"}, "limit": {"type": "integer"}}),
        _definition("action_complete", "Complete a local workbench action explicitly requested by the user.", {"action_id": {"type": "string"}}, ["action_id"]),
        _definition("research_monitor_updates", "Read recent monitored website changes and their source IDs.", {"limit": {"type": "integer"}, "status": {"type": "string"}}),
        _definition(
            "goal_update",
            "Update a personal goal when the user explicitly changes its title, description, priority, or status.",
            {"goal_id": {"type": "string"}, "title": {"type": "string"}, "description": {"type": "string"}, "priority": {"type": "string"}, "status": {"type": "string"}},
            ["goal_id"],
        ),
        _definition("goal_archive", "Archive a personal goal when the user explicitly ends it.", {"goal_id": {"type": "string"}}, ["goal_id"]),
        _definition(
            "task_create",
            "Create a tracked task with structured steps. For a long-term plan, pass the goal_id returned by goal_create and set long_term=true.",
            {
                "title": {"type": "string"},
                "goal": {"type": "string"},
                "description": {"type": "string"},
                "steps": {"type": "array", "items": {"type": "object"}},
                "goal_id": {"type": "string"},
                "long_term": {"type": "boolean"},
            },
            ["title", "goal"],
        ),
        _definition(
            "task_update",
            "Update task state, facts or steps.",
            {
                "task_id": {"type": "string"},
                "status": {"type": "string"},
                "description": {"type": "string"},
                "goal_id": {"type": "string"},
                "long_term": {"type": "boolean"},
                "state": {"type": "array"},
                "key_facts": {"type": "array"},
                "steps": {"type": "array", "items": {"type": "object"}},
            },
            ["task_id"],
        ),
        _definition("task_get", "Read a tracked task.", {"task_id": {"type": "string"}}, ["task_id"]),
        _definition("task_list", "List tracked tasks.", {"status": {"type": "string"}, "limit": {"type": "integer"}}),
        _definition(
            "skill_read", "Read the instructions for an available skill.", {"name": {"type": "string"}, "scope": {"type": "string"}}, ["name"]
        ),
        _definition(
            "skill_resource_read",
            "Read a resource inside an installed skill.",
            {"name": {"type": "string"}, "path": {"type": "string"}, "scope": {"type": "string"}},
            ["name", "path"],
        ),
        _definition(
            "evidence_search",
            "Search durable run evidence.",
            {"query": {"type": "string"}, "run_id": {"type": "string"}, "kind": {"type": "string"}, "limit": {"type": "integer"}},
            ["query"],
        ),
        _definition("evidence_read", "Read one evidence record.", {"evidence_id": {"type": "string"}}, ["evidence_id"]),
        _definition(
            "automation_create",
            "Create a once, interval or cron automation. For a reminder belonging to an ongoing goal, pass its goal_id.",
            {
                "title": {"type": "string"},
                "prompt": {"type": "string"},
                "kind": {"type": "string"},
                "run_at": {"type": "string"},
                "interval_seconds": {"type": "integer"},
                "cron_expr": {"type": "string"},
                "timezone": {"type": "string"},
                "goal_id": {"type": "string"},
            },
            ["title", "prompt", "kind"],
        ),
        _definition("automation_list", "List automations.", {}),
        _definition("automation_get", "Read an automation.", {"automation_id": {"type": "string"}}, ["automation_id"]),
        _definition("automation_pause", "Pause an automation.", {"automation_id": {"type": "string"}}, ["automation_id"]),
        _definition("automation_resume", "Resume an automation.", {"automation_id": {"type": "string"}}, ["automation_id"]),
        _definition("automation_cancel", "Cancel an automation.", {"automation_id": {"type": "string"}}, ["automation_id"]),
        _definition(
            "feishu_task_create",
            "Create a task in the connected Feishu account after user approval.",
            {"title": {"type": "string"}, "description": {"type": "string"}, "due_at": {"type": "string"}, "labels": {"type": "array", "items": {"type": "string"}, "description": "Not supported by the current adapter; ask the user to omit labels instead of silently dropping them."}},
            ["title"],
        ),
        _definition(
            "feishu_task_update",
            "Update a task in the connected Feishu account after user approval.",
            {
                "task_id": {"type": "string"},
                "title": {"type": "string"},
                "description": {"type": "string"},
                "due_at": {"type": "string"},
                "status": {"type": "string", "enum": ["todo", "done"]},
            },
            ["task_id"],  # noqa: E501
        ),
        _definition("feishu_task_delete", "Delete a Feishu task after user approval.", {"task_id": {"type": "string"}}, ["task_id"]),
        _definition(
            "feishu_document_create",
            "Create a cloud document in the connected Feishu account after user approval.",
            {"title": {"type": "string"}, "content": {"type": "string"}},
            ["title"],
        ),
        _definition(
            "feishu_document_append",
            "Append content to an existing Feishu cloud document after user approval.",
            {"document_id": {"type": "string"}, "content": {"type": "string"}},
            ["document_id", "content"],
        ),
        _definition(
            "feishu_document_delete",
            "Move an existing Feishu cloud document to trash after user approval.",
            {"document_id": {"type": "string"}},
            ["document_id"],
        ),
        _definition(
            "feishu_document_block_update",
            "Replace text in one known Feishu document text block after user approval.",
            {"document_id": {"type": "string"}, "block_id": {"type": "string"}, "content": {"type": "string"}},
            ["document_id", "block_id", "content"],  # noqa: E501
        ),
        _definition(
            "feishu_calendar_event_create",
            "Create a calendar event in the connected Feishu account after user approval.",
            {
                "title": {"type": "string"},
                "starts_at": {"type": "string"},
                "ends_at": {"type": "string"},
                "timezone": {"type": "string", "description": "IANA timezone, e.g. Asia/Shanghai"},
                "participants": {"type": "array", "items": {"type": "object", "properties": {"email": {"type": "string"}, "user_id": {"type": "string"}}}},
                "description": {"type": "string"},
                "location": {"type": "string"},
            },
            ["title", "starts_at", "ends_at"],
        ),
        _definition(
            "feishu_calendar_event_update",
            "Update a Feishu calendar event after user approval.",
            {
                "event_id": {"type": "string"},
                "title": {"type": "string"},
                "starts_at": {"type": "string"},
                "ends_at": {"type": "string"},
                "timezone": {"type": "string", "description": "IANA timezone, e.g. Asia/Shanghai"},
                "description": {"type": "string"},
                "location": {"type": "string"},
            },  # noqa: E501
            ["event_id"],
        ),
        _definition(
            "feishu_calendar_event_delete", "Delete a Feishu calendar event after user approval.", {"event_id": {"type": "string"}}, ["event_id"]
        ),  # noqa: E501
        _definition(
            "connector_workflow",
            "Run several Feishu/email side effects as one durable approval-gated workflow. Report partial or uncertain results.",  # noqa: E501
            {
                "steps": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tool": {"type": "string"},
                            "arguments": {"type": "object"},
                            "id": {"type": "string"},
                        },
                        "required": ["tool", "arguments"],
                    },
                },
                "idempotency_key": {"type": "string"},
            },
            ["steps"],
        ),
        _definition(
            "email_send",
            "Send an email through the configured QQ/163 mailbox after user approval.",
            {
                "to": {"type": "string"},
                "subject": {"type": "string"},
                "body": {"type": "string"},
                "cc": {"type": "string"},
                "bcc": {"type": "string"},
                "html_body": {"type": "string"},
                "attachments": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "filename": {"type": "string"},
                            "content_type": {"type": "string"},
                            "content_base64": {"type": "string"},
                        },
                        "required": ["filename", "content_base64"],
                    },
                },
                "reply_to_message_id": {"type": "string"},
                "forward_message_id": {"type": "string"},
            },
            ["to", "subject", "body"],
        ),
    ]
)

PLAN_ALLOWED_TOOLS = {
    "read_file",
    "list_files",
    "web_search",
    "web_research",
    "research_monitor_create",
    "get_current_time",
    "history_search",
    "history_read",
    "session_search",
    "unified_search",
    "file_extract",
    "connector_related",
    "memory_list",
    "memory_search",
    "memory_read",
    "task_create",
    "task_update",
    "task_get",
    "task_list",
    "evidence_search",
    "evidence_read",
    "tool_catalog",
}

CORE_TOOLS = {
    "read_file",
    "list_files",
    "write_file",
    "artifact_publish",
    "action_create",
    "action_list",
    "action_complete",
    "research_monitor_updates",
    "shell",
    "web_search",
    "web_research",
    "research_monitor_create",
    "get_current_time",
    "unified_search",
    "file_extract",
    "connector_related",
    "goal_create",
    "goal_list",
    "goal_update",
    "goal_archive",
    "task_create",
    "task_update",
    "task_get",
    "task_list",
    "tool_catalog",
    "feishu_task_create",
    "feishu_task_update",
    "feishu_task_delete",
    "feishu_document_create",
    "feishu_document_append",
    "feishu_document_delete",
    "feishu_document_block_update",
    "feishu_calendar_event_create",
    "feishu_calendar_event_update",
    "feishu_calendar_event_delete",
    "email_send",
    "connector_workflow",
}

TOOL_CATALOG_DEFINITION = _definition(
    "tool_catalog",
    "Find and activate additional tools for this run by capability or name.",
    {"query": {"type": "string", "description": "Capability to find, for example memory, session search, HTTP, evidence, automation, MCP."}},
    ["query"],
)
TOOLS.append(TOOL_CATALOG_DEFINITION)


def builtin_tool_definitions(mode: str = "normal") -> list[dict[str, Any]]:
    if mode != "plan":
        return TOOLS
    return [item for item in TOOLS if item["function"]["name"] in PLAN_ALLOWED_TOOLS]


@dataclass
class ModelReply:
    content: str | None
    reasoning: str | None
    tool_calls: list[dict[str, Any]]
    usage: dict[str, int]


def _model_usage(
    input_tokens: int,
    output_tokens: int,
    total_tokens: int,
    *,
    cached_input_tokens: int | None = None,
    cache_write_input_tokens: int | None = None,
    uncached_input_tokens: int | None = None,
) -> dict[str, int]:
    usage = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }
    if cached_input_tokens is not None:
        usage["cached_input_tokens"] = cached_input_tokens
        usage["cache_read_input_tokens"] = cached_input_tokens
        usage["uncached_input_tokens"] = uncached_input_tokens if uncached_input_tokens is not None else max(0, input_tokens - cached_input_tokens)
    if cache_write_input_tokens is not None:
        usage["cache_write_input_tokens"] = cache_write_input_tokens
    return usage


def _openai_cache_usage(usage: dict[str, Any], details_name: str) -> dict[str, int | None]:
    details = usage.get(details_name) or {}
    cached = next(
        (
            int(value)
            for value in (
                usage.get("prompt_cache_hit_tokens"),
                details.get("cached_tokens"),
                usage.get("cached_tokens"),
            )
            if value is not None
        ),
        None,
    )
    cache_write = details.get("cache_creation_input_tokens")
    cache_miss = usage.get("prompt_cache_miss_tokens")
    return {
        "cached_input_tokens": cached if cached is not None else (0 if cache_write is not None else None),
        "cache_write_input_tokens": int(cache_write) if cache_write is not None else None,
        "uncached_input_tokens": int(cache_miss) if cache_miss is not None else None,
    }


class ModelClient:
    def __init__(self, settings: Settings, model_config: dict[str, Any] | None = None):
        self.settings = settings
        self.config = model_config or {}
        self.http_timeout: float | httpx.Timeout = 120

    async def complete(
        self,
        messages: list[dict[str, Any]],
        extra_tools: list[dict[str, Any]] | None = None,
        base_tools: list[dict[str, Any]] | None = None,
        on_text_delta: DeltaSink | None = None,
        on_reasoning_delta: DeltaSink | None = None,
    ) -> ModelReply:
        emitted = False

        async def text_sink(delta: str) -> None:
            nonlocal emitted
            emitted = True
            if on_text_delta:
                await on_text_delta(delta)

        async def reasoning_sink(delta: str) -> None:
            nonlocal emitted
            emitted = True
            if on_reasoning_delta:
                await on_reasoning_delta(delta)

        for attempt in range(3):
            try:
                return await self._complete_once(
                    messages,
                    extra_tools,
                    base_tools,
                    on_text_delta=text_sink if on_text_delta else None,
                    on_reasoning_delta=reasoning_sink if on_reasoning_delta else None,
                )
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                transient = status is None or status == 429 or status >= 500
                if attempt >= 2 or emitted or not transient:
                    raise RuntimeError(self._model_error(exc)) from exc
                await asyncio.sleep(0.4 * (2**attempt))
        raise RuntimeError("model request failed")

    async def _complete_once(
        self,
        messages: list[dict[str, Any]],
        extra_tools: list[dict[str, Any]] | None = None,
        base_tools: list[dict[str, Any]] | None = None,
        on_text_delta: DeltaSink | None = None,
        on_reasoning_delta: DeltaSink | None = None,
    ) -> ModelReply:
        provider = self.config.get("provider", self.settings.provider)
        if provider == "anthropic":
            return await self._anthropic(
                messages,
                extra_tools or [],
                base_tools,
                on_text_delta=on_text_delta,
                on_reasoning_delta=on_reasoning_delta,
            )
        return await self._openai_compatible(
            messages,
            extra_tools or [],
            base_tools,
            on_text_delta=on_text_delta,
            on_reasoning_delta=on_reasoning_delta,
        )

    def _credentials(self) -> tuple[str, str, str]:
        api_key = self.config.get("api_key") or self.settings.api_key
        model = self.config.get("model") or self.settings.model
        base_url = (self.config.get("base_url") or self.settings.base_url).rstrip("/")
        return api_key, model, base_url

    @staticmethod
    async def _raise_for_stream_status(response: httpx.Response) -> None:
        """Raise an HTTP error without losing the body of a streamed response.

        ``httpx.AsyncClient.stream`` does not read the response body before
        returning control to us.  Calling ``response.text`` from a later
        error handler therefore raises ``ResponseNotRead`` and hides the
        provider's actual status and error message.  Read the body while the
        stream is still open, then re-raise the original ``HTTPStatusError``.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            try:
                await response.aread()
            except Exception:
                # Keep the original HTTPStatusError.  The formatter below has
                # its own fallback when a provider closes the stream early.
                pass
            raise

    async def _openai_compatible(
        self,
        messages: list[dict[str, Any]],
        extra_tools: list[dict[str, Any]],
        base_tools: list[dict[str, Any]] | None,
        *,
        on_text_delta: DeltaSink | None = None,
        on_reasoning_delta: DeltaSink | None = None,
    ) -> ModelReply:
        api_key, model, base_url = self._credentials()
        if not api_key:
            return ModelReply(
                content="模型尚未配置。请在“设置”中填写 API Key、模型和服务地址。",
                reasoning=None,
                tool_calls=[],
                usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            )
        if self.config.get("api_style") == "responses":
            return await self._responses(
                api_key,
                model,
                base_url,
                messages,
                extra_tools,
                base_tools,
                on_text_delta=on_text_delta,
                on_reasoning_delta=on_reasoning_delta,
            )
        all_tools = (TOOLS if base_tools is None else base_tools) + extra_tools
        body: dict[str, Any] = {"model": model, "messages": messages}
        if all_tools:
            body.update(tools=all_tools, tool_choice="auto")
        if on_text_delta is not None or on_reasoning_delta is not None:
            return await self._stream_chat(
                api_key,
                base_url,
                body,
                on_text_delta=on_text_delta,
                on_reasoning_delta=on_reasoning_delta,
            )
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=body,
            )
            response.raise_for_status()
        payload = response.json()
        message = payload["choices"][0]["message"]
        calls: list[dict[str, Any]] = []
        for item in message.get("tool_calls") or []:
            raw = item.get("function", {}).get("arguments", "{}")
            try:
                arguments = json.loads(raw)
            except json.JSONDecodeError:
                arguments = {}
            calls.append(
                {
                    "id": item.get("id") or os.urandom(8).hex(),
                    "name": item.get("function", {}).get("name", ""),
                    "arguments": arguments,
                }
            )
        usage = payload.get("usage") or {}
        return ModelReply(
            content=message.get("content"),
            reasoning=message.get("reasoning_content"),
            tool_calls=calls,
            usage=_model_usage(
                int(usage.get("prompt_tokens", 0)),
                int(usage.get("completion_tokens", 0)),
                int(usage.get("total_tokens", 0)),
                **_openai_cache_usage(usage, "prompt_tokens_details"),
            ),
        )

    async def _stream_chat(
        self,
        api_key: str,
        base_url: str,
        body: dict[str, Any],
        *,
        on_text_delta: DeltaSink | None,
        on_reasoning_delta: DeltaSink | None,
    ) -> ModelReply:
        request_body = {**body, "stream": True}
        # Some OpenAI-compatible gateways (including older DashScope/Qwen
        # deployments) reject stream_options even though their SSE stream is valid.
        provider = str(self.config.get("provider") or self.settings.provider).lower()
        if provider not in {"qwen"} and "dashscope" not in base_url.lower():
            request_body["stream_options"] = {"include_usage": True}
        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_parts: dict[int, dict[str, str]] = {}
        usage: dict[str, Any] = {}
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=request_body,
            ) as response:
                await self._raise_for_stream_status(response)
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if raw == "[DONE]":
                        break
                    try:
                        chunk = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if chunk.get("usage"):
                        usage = chunk["usage"]
                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta") or {}
                    content = delta.get("content")
                    if content:
                        text_parts.append(str(content))
                        if on_text_delta:
                            await on_text_delta(str(content))
                    reasoning = delta.get("reasoning_content") or delta.get("reasoning")
                    if reasoning:
                        reasoning_parts.append(str(reasoning))
                        if on_reasoning_delta:
                            await on_reasoning_delta(str(reasoning))
                    for call in delta.get("tool_calls") or []:
                        index = int(call.get("index", 0))
                        function = call.get("function") or {}
                        part = tool_parts.setdefault(index, {"id": "", "name": "", "arguments": ""})
                        part["id"] += str(call.get("id") or "")
                        part["name"] += str(function.get("name") or "")
                        part["arguments"] += str(function.get("arguments") or "")
        calls = []
        for index in sorted(tool_parts):
            part = tool_parts[index]
            try:
                arguments = json.loads(part["arguments"] or "{}")
            except json.JSONDecodeError:
                arguments = {}
            calls.append({"id": part["id"] or os.urandom(8).hex(), "name": part["name"], "arguments": arguments})
        inp = int(usage.get("prompt_tokens", 0))
        out = int(usage.get("completion_tokens", 0))
        return ModelReply(
            content="".join(text_parts) or None,
            reasoning="".join(reasoning_parts) or None,
            tool_calls=calls,
            usage=_model_usage(
                inp,
                out,
                int(usage.get("total_tokens", inp + out)),
                **_openai_cache_usage(usage, "prompt_tokens_details"),
            ),
        )

    async def _responses(
        self,
        api_key: str,
        model: str,
        base_url: str,
        messages: list[dict[str, Any]],
        extra_tools: list[dict[str, Any]],
        base_tools: list[dict[str, Any]] | None,
        *,
        on_text_delta: DeltaSink | None = None,
        on_reasoning_delta: DeltaSink | None = None,
    ) -> ModelReply:
        tools = [
            {
                "type": "function",
                "name": item["function"]["name"],
                "description": item["function"]["description"],
                "parameters": item["function"]["parameters"],
                "strict": False,
            }
            for item in (TOOLS if base_tools is None else base_tools) + extra_tools
        ]
        request_body: dict[str, Any] = {"model": model, "input": messages}
        if tools:
            request_body["tools"] = tools
        if on_text_delta is not None or on_reasoning_delta is not None:
            return await self._stream_responses(
                api_key,
                base_url,
                request_body,
                on_text_delta=on_text_delta,
                on_reasoning_delta=on_reasoning_delta,
            )
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            response = await client.post(
                f"{base_url}/responses",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=request_body,
            )
            response.raise_for_status()
        payload = response.json()
        texts: list[str] = []
        calls: list[dict[str, Any]] = []
        for item in payload.get("output", []):
            if item.get("type") == "message":
                for part in item.get("content", []):
                    if part.get("type") in {"output_text", "text"}:
                        texts.append(part.get("text", ""))
            elif item.get("type") == "function_call":
                raw = item.get("arguments", "{}")
                try:
                    arguments = json.loads(raw)
                except json.JSONDecodeError:
                    arguments = {}
                calls.append(
                    {
                        "id": item.get("call_id") or item.get("id") or os.urandom(8).hex(),
                        "name": item.get("name", ""),
                        "arguments": arguments,
                    }
                )
        usage = payload.get("usage") or {}
        inp = int(usage.get("input_tokens", 0))
        out = int(usage.get("output_tokens", 0))
        return ModelReply(
            "\n".join(texts) or None,
            None,
            calls,
            _model_usage(
                inp,
                out,
                int(usage.get("total_tokens", inp + out)),
                **_openai_cache_usage(usage, "input_tokens_details"),
            ),
        )

    async def _stream_responses(
        self,
        api_key: str,
        base_url: str,
        body: dict[str, Any],
        *,
        on_text_delta: DeltaSink | None,
        on_reasoning_delta: DeltaSink | None,
    ) -> ModelReply:
        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        calls: dict[str, dict[str, Any]] = {}
        final: dict[str, Any] | None = None
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/responses",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={**body, "stream": True},
            ) as response:
                await self._raise_for_stream_status(response)
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    kind = str(event.get("type") or "")
                    if kind == "response.output_text.delta":
                        delta = str(event.get("delta") or "")
                        text_parts.append(delta)
                        if delta and on_text_delta:
                            await on_text_delta(delta)
                    elif kind in {"response.reasoning_summary_text.delta", "response.reasoning_text.delta"}:
                        delta = str(event.get("delta") or "")
                        reasoning_parts.append(delta)
                        if delta and on_reasoning_delta:
                            await on_reasoning_delta(delta)
                    elif kind == "response.output_item.added":
                        item = event.get("item") or {}
                        if item.get("type") == "function_call":
                            key = str(item.get("call_id") or item.get("id") or os.urandom(8).hex())
                            calls[key] = {"id": key, "name": item.get("name", ""), "raw": str(item.get("arguments") or "")}
                    elif kind == "response.function_call_arguments.delta":
                        key = str(event.get("call_id") or event.get("item_id") or "")
                        call = calls.setdefault(key, {"id": key, "name": event.get("name", ""), "raw": ""})
                        call["raw"] += str(event.get("delta") or "")
                    elif kind == "response.completed":
                        final = event.get("response") or {}
        usage = (final or {}).get("usage") or {}
        parsed_calls = []
        for call in calls.values():
            try:
                arguments = json.loads(call["raw"] or "{}")
            except json.JSONDecodeError:
                arguments = {}
            parsed_calls.append({"id": call["id"], "name": call["name"], "arguments": arguments})
        inp, out = int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))
        return ModelReply(
            "".join(text_parts) or None,
            "".join(reasoning_parts) or None,
            parsed_calls,
            _model_usage(
                inp,
                out,
                int(usage.get("total_tokens", inp + out)),
                **_openai_cache_usage(usage, "input_tokens_details"),
            ),
        )

    async def _anthropic(
        self,
        messages: list[dict[str, Any]],
        extra_tools: list[dict[str, Any]],
        base_tools: list[dict[str, Any]] | None,
        *,
        on_text_delta: DeltaSink | None = None,
        on_reasoning_delta: DeltaSink | None = None,
    ) -> ModelReply:
        api_key, model, base_url = self._credentials()
        if not api_key:
            return ModelReply(
                "模型尚未配置。请在“设置”中填写 API Key。",
                None,
                [],
                {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                },
            )
        system = "You are KoreAgent Next, a careful local AI agent."
        clean: list[dict[str, Any]] = []
        for message in messages:
            role = message.get("role")
            if role == "system":
                system = str(message.get("content") or system)
                continue
            if role == "tool":
                clean.append(
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": message.get("tool_call_id"),
                                "content": message.get("content") or "",
                            }
                        ],
                    }
                )
                continue
            if role == "assistant" and message.get("tool_calls"):
                blocks: list[dict[str, Any]] = []
                if message.get("content"):
                    blocks.append({"type": "text", "text": message["content"]})
                for call in message["tool_calls"]:
                    function = call.get("function", {})
                    raw = function.get("arguments", "{}")
                    try:
                        arguments = json.loads(raw) if isinstance(raw, str) else raw
                    except json.JSONDecodeError:
                        arguments = {}
                    blocks.append(
                        {
                            "type": "tool_use",
                            "id": call.get("id"),
                            "name": function.get("name"),
                            "input": arguments,
                        }
                    )
                clean.append({"role": "assistant", "content": blocks})
            else:
                clean.append({"role": role, "content": message.get("content") or ""})
        anthropic_tools = [
            {
                "name": t["function"]["name"],
                "description": t["function"]["description"],
                "input_schema": t["function"]["parameters"],
            }
            for t in (TOOLS if base_tools is None else base_tools) + extra_tools
        ]
        request_body: dict[str, Any] = {
            "model": model,
            "max_tokens": 4096,
            "system": system,
            "messages": clean,
        }
        if anthropic_tools:
            request_body["tools"] = anthropic_tools
        if on_text_delta is not None or on_reasoning_delta is not None:
            return await self._stream_anthropic(
                api_key,
                base_url,
                request_body,
                on_text_delta=on_text_delta,
                on_reasoning_delta=on_reasoning_delta,
            )
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            response = await client.post(
                f"{base_url}/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
                json=request_body,
            )
            response.raise_for_status()
        payload = response.json()
        texts: list[str] = []
        calls: list[dict[str, Any]] = []
        for block in payload.get("content", []):
            if block.get("type") == "text":
                texts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                calls.append({"id": block["id"], "name": block["name"], "arguments": block.get("input", {})})
        usage = payload.get("usage") or {}
        base_input, out = int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))
        cache_read = usage.get("cache_read_input_tokens")
        cache_write = usage.get("cache_creation_input_tokens")
        cache_reported = cache_read is not None or cache_write is not None
        inp = base_input + int(cache_read or 0) + int(cache_write or 0) if cache_reported else base_input
        return ModelReply(
            "\n".join(texts) or None,
            None,
            calls,
            _model_usage(
                inp,
                out,
                inp + out,
                cached_input_tokens=int(cache_read or 0) if cache_reported else None,
                cache_write_input_tokens=int(cache_write) if cache_write is not None else None,
                uncached_input_tokens=base_input + int(cache_write or 0) if cache_reported else None,
            ),
        )

    async def _stream_anthropic(
        self,
        api_key: str,
        base_url: str,
        body: dict[str, Any],
        *,
        on_text_delta: DeltaSink | None,
        on_reasoning_delta: DeltaSink | None,
    ) -> ModelReply:
        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        tools: dict[int, dict[str, Any]] = {}
        usage = {"input_tokens": 0, "output_tokens": 0}
        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                json={**body, "stream": True},
            ) as response:
                await self._raise_for_stream_status(response)
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue
                    kind = event.get("type")
                    if kind == "message_start":
                        usage.update((event.get("message") or {}).get("usage") or {})
                    elif kind == "content_block_start":
                        block = event.get("content_block") or {}
                        if block.get("type") == "tool_use":
                            tools[int(event.get("index", 0))] = {
                                "id": block.get("id") or os.urandom(8).hex(),
                                "name": block.get("name", ""),
                                "raw": "",
                                "input": block.get("input") or {},
                            }
                    elif kind == "content_block_delta":
                        delta = event.get("delta") or {}
                        delta_type = delta.get("type")
                        if delta_type == "text_delta":
                            text = str(delta.get("text") or "")
                            text_parts.append(text)
                            if text and on_text_delta:
                                await on_text_delta(text)
                        elif delta_type == "thinking_delta":
                            text = str(delta.get("thinking") or "")
                            reasoning_parts.append(text)
                            if text and on_reasoning_delta:
                                await on_reasoning_delta(text)
                        elif delta_type == "input_json_delta":
                            tool = tools.setdefault(
                                int(event.get("index", 0)),
                                {"id": os.urandom(8).hex(), "name": "", "raw": "", "input": {}},
                            )
                            tool["raw"] += str(delta.get("partial_json") or "")
                    elif kind == "message_delta":
                        usage.update(event.get("usage") or {})
        calls = []
        for item in tools.values():
            try:
                arguments = json.loads(item["raw"]) if item["raw"] else item["input"]
            except json.JSONDecodeError:
                arguments = item["input"]
            calls.append({"id": item["id"], "name": item["name"], "arguments": arguments})
        base_input, out = int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))
        cache_read = usage.get("cache_read_input_tokens")
        cache_write = usage.get("cache_creation_input_tokens")
        cache_reported = cache_read is not None or cache_write is not None
        inp = base_input + int(cache_read or 0) + int(cache_write or 0) if cache_reported else base_input
        return ModelReply(
            "".join(text_parts) or None,
            "".join(reasoning_parts) or None,
            calls,
            _model_usage(
                inp,
                out,
                inp + out,
                cached_input_tokens=int(cache_read or 0) if cache_reported else None,
                cache_write_input_tokens=int(cache_write) if cache_write is not None else None,
                uncached_input_tokens=base_input + int(cache_write or 0) if cache_reported else None,
            ),
        )

    @staticmethod
    def _model_error(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            request_id = exc.response.headers.get("x-request-id") or exc.response.headers.get("request-id")
            try:
                detail = exc.response.text[:800]
            except httpx.ResponseNotRead:
                detail = "<provider response body was not available>"
            except Exception as body_error:
                detail = f"<unable to read provider response body: {body_error}>"
            suffix = f" request_id={request_id}" if request_id else ""
            return f"Model provider returned HTTP {status}.{suffix} {detail}".strip()
        if isinstance(exc, httpx.TimeoutException):
            return "Model provider request timed out"
        return f"Model provider network error: {exc}"


class ToolExecutor:
    def __init__(
        self,
        settings: Settings,
        approve: ApprovalGate,
        publish: Callable[..., Awaitable[Any]],
        remember: Callable[..., Awaitable[Any]],
        mcp: McpManager,
        services: Any | None = None,
    ):
        self.settings = settings
        self.approve = approve
        self.publish = publish
        self.remember = remember
        self.mcp = mcp
        self.services = services
        self.sandbox = SandboxSupervisor(settings.workspace_dir)
        self.activated_tools: set[str] = set()

    def tool_definitions(self, mode: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        available = builtin_tool_definitions(mode)
        base = [definition for definition in available if definition["function"]["name"] in CORE_TOOLS | self.activated_tools]
        extra = []
        if mode != "plan":
            extra = [definition for definition in self.mcp.tool_definitions() if definition["function"]["name"] in self.activated_tools]
        return base, extra

    def activate_tools(self, query: str, mode: str) -> dict[str, Any]:
        options = builtin_tool_definitions(mode)
        if mode != "plan":
            options = [*options, *self.mcp.tool_definitions()]
        terms = [term.lower() for term in re.split(r"\s+", query.strip()) if term]
        matches = []
        for definition in options:
            name = definition["function"]["name"]
            searchable = f"{name} {definition['function'].get('description', '')}".lower()
            if name != "tool_catalog" and (not terms or any(term in searchable for term in terms)):
                matches.append(definition)
        chosen = matches[:8]
        self.activated_tools.update(definition["function"]["name"] for definition in chosen)
        return {
            "activated": [definition["function"]["name"] for definition in chosen],
            "matching_count": len(matches),
            "message": "Activated tools are available on the next model step.",
        }

    def resolve(self, raw: str) -> Path:
        return self.sandbox.resolve(raw)

    async def execute(
        self,
        run_id: str,
        name: str,
        arguments: dict[str, Any],
        call_id: str,
        *,
        mode: str = "normal",
    ) -> str:
        if mode == "plan" and name not in PLAN_ALLOWED_TOOLS:
            raise PermissionError(f"Tool {name} is not available in plan mode")
        if name == "tool_catalog":
            return json.dumps(self.activate_tools(str(arguments.get("query") or ""), mode), ensure_ascii=False)
        if name.startswith("mcp__"):
            server = self.mcp.servers.get(name.split("__", 2)[1])
            if server and not server.config.get("enabled", True):
                raise PermissionError("MCP server is disabled")
        visible = {definition["function"]["name"] for group in self.tool_definitions(mode) for definition in group}
        if name not in visible:
            raise PermissionError(f"Tool {name} is not active; call tool_catalog first")
        if name.startswith("mcp__"):
            permission = self.mcp.permission(name)
            if permission == "forbidden":
                raise PermissionError("MCP tool is forbidden by policy")
            if permission == "human_approval":
                allowed = await self.approve(run_id, call_id, {**arguments, "tool_name": name}, f"调用 MCP 工具 {name}")
                if not allowed:
                    return "Denied by user"
            try:
                result = await self.mcp.call(name, arguments)
                if json.loads(result).get("isError"):
                    raise McpToolError(result)
            except Exception as exc:
                if self.services is not None and not self.mcp.audit:
                    await self.services.proactive.audit("mcp.call.failed", entity_type="mcp", entity_id=name, details={"error": str(exc)})
                raise
            return result
        if name == "read_file":
            path = self.sandbox.resolve(str(arguments.get("path", "")), must_exist=True)
            if not path.is_file():
                raise IsADirectoryError(path)
            return path.read_text(encoding="utf-8")
        if name == "list_files":
            path = self.sandbox.resolve(str(arguments.get("path", ".")), must_exist=True)
            if not path.is_dir():
                raise NotADirectoryError(path)
            entries = []
            for item in sorted(path.iterdir()):
                suffix = "/" if item.is_dir() else ""
                # Use the sandbox's resolved root for both sides. On Windows
                # TEMP paths can appear once as ``Administrator`` and once as
                # the 8.3 ``ADMINI~1`` form; relative_to() rejects those
                # equivalent paths when they are mixed.
                entries.append(str(item.relative_to(self.sandbox.workspace)) + suffix)
            return "\n".join(entries)
        if name == "write_file":
            await self._require_approval(run_id, call_id, name, arguments, "写入工作区文件")
            path = self.resolve(str(arguments.get("path", "")))
            path.parent.mkdir(parents=True, exist_ok=True)
            content = str(arguments.get("content", ""))
            path.write_text(content, encoding="utf-8")
            return f"Wrote {len(content.encode('utf-8'))} bytes to {path.name}"
        if name == "shell":
            await self._require_approval(run_id, call_id, name, arguments, "执行本地命令")
            command = str(arguments.get("command", ""))
            argv = shlex.split(command, posix=os.name != "nt")
            if not argv:
                return "Empty command"
            result = await self.sandbox.run(
                argv[0],
                argv[1:],
                timeout=float(arguments.get("timeout_seconds", 120)),
                filesystem="workspace",
                network="allowed",
            )
            output = result.stdout + (f"\n[stderr]\n{result.stderr}" if result.stderr else "")
            return f"exit_code={result.returncode}\n{output[-40_000:]}"
        if name == "get_current_time":
            timezone = str(arguments.get("timezone") or "UTC")
            try:
                current = datetime.now(ZoneInfo(timezone))
            except Exception as exc:
                raise ValueError(f"Unknown timezone: {timezone}") from exc
            return json.dumps({"timezone": timezone, "iso": current.isoformat(), "utc": datetime.now(UTC).isoformat()})
        if name == "web_search":
            if self.settings.web_require_approval:
                await self._require_approval(run_id, call_id, name, arguments, "读取公开网页搜索结果")
            await self._audit_web_access("web_search", arguments)
            return await self._web_search(str(arguments.get("query", "")), int(arguments.get("limit", 5)))
        if name == "web_research":
            if self.settings.web_require_approval:
                await self._require_approval(run_id, call_id, name, arguments, "读取公开网页并提取研究资料")
            await self._audit_web_access("web_research", arguments)
            service = WebResearchService(self._web_search, self._fetch_public_page, allow_url=self._domain_allowed)
            result = await service.run(
                str(arguments.get("query", "")),
                mode=str(arguments.get("mode") or "quick"),
                max_sources=int(arguments.get("max_sources", 5)),
                fetch_pages=bool(arguments.get("fetch_pages", True)),
                max_queries=int(arguments["max_queries"]) if arguments.get("max_queries") is not None else None,
                max_pages=int(arguments["max_pages"]) if arguments.get("max_pages") is not None else None,
                allowed_domains=self.settings.web_allowed_domains,
            )
            return json.dumps(result, ensure_ascii=False)
        if name == "http_request":
            method = str(arguments.get("method") or "GET").upper()
            if method not in {"GET", "HEAD"} or self.settings.web_require_approval:
                await self._require_approval(run_id, call_id, name, arguments, f"发送 {method} 网络请求")
            await self._audit_web_access("http_request", arguments)
            return await self._http_request(method, arguments)
        if name == "publish_file":
            path = self.resolve(str(arguments.get("path", "")))
            artifact = await self.publish(
                path=path,
                title=str(arguments.get("title", path.name)),
                description=arguments.get("description"),
                run_id=run_id,
            )
            return f"Published artifact {artifact['id']}"
        if name == "artifact_publish":
            path_value, url_value = arguments.get("path"), arguments.get("url")
            if bool(path_value) == bool(url_value):
                raise ValueError("Provide exactly one of path or url")
            if path_value:
                artifact = await self.publish(
                    path=self.resolve(str(path_value)),
                    title=str(arguments.get("title") or Path(str(path_value)).name),
                    description=arguments.get("description"),
                    run_id=run_id,
                )
            else:
                await self._validate_public_url(str(url_value))
                artifact = await self.services.publish_url_artifact(
                    url=str(url_value),
                    title=str(arguments.get("title") or url_value),
                    description=arguments.get("description"),
                    run_id=run_id,
                )
            return json.dumps({"artifact": artifact}, ensure_ascii=False)
        if name == "remember":
            memory_arguments = {key: arguments.get(key) for key in
                                ("fact", "confidence", "model_confidence", "provenance", "evidence", "project_id")}
            if self.services is not None:
                memory = await self.services.remember(title=str(arguments.get("title", "Memory")), summary=str(arguments.get("summary", "")), content=str(arguments.get("content", "")), run_id=run_id, memory_operation_id=f"tool:{run_id}:{call_id}:{name}", **memory_arguments)
                return json.dumps({"memory": memory}, ensure_ascii=False)
            memory = await self.remember(
                title=str(arguments.get("title", "Memory")),
                summary=str(arguments.get("summary", "")),
                content=str(arguments.get("content", "")),
                **memory_arguments,
            )
            return json.dumps({"memory": memory}, ensure_ascii=False)
        if self.services is not None and name in {
            "feishu_task_create",
            "feishu_task_update",
            "feishu_task_delete",
            "feishu_document_create",
            "feishu_document_append",
            "feishu_document_delete",
            "feishu_document_block_update",
            "feishu_calendar_event_create",
            "feishu_calendar_event_update",
            "feishu_calendar_event_delete",
            "email_send",
            "connector_workflow",
            "history_search",
            "history_read",
            "session_search",
            "unified_search",
            "file_extract",
            "connector_related",
            "research_monitor_create",
            "memory_list",
            "memory_search",
            "memory_read",
            "memory_create",
            "memory_update",
            "memory_archive",
            "core_memory_update",
            "core_memory_remove",
            "goal_create",
            "goal_list",
            "goal_update",
            "goal_archive",
            "action_create",
            "action_list",
            "action_complete",
            "research_monitor_updates",
            "task_create",
            "task_update",
            "task_get",
            "task_list",
            "skill_read",
            "skill_resource_read",
            "evidence_search",
            "evidence_read",
            "automation_create",
            "automation_list",
            "automation_get",
            "automation_pause",
            "automation_resume",
            "automation_cancel",
        }:
            if name in {
                "feishu_task_create",
                "feishu_task_update",
                "feishu_task_delete",
                "feishu_document_create",
                "feishu_document_append",
                "feishu_document_delete",
                "feishu_document_block_update",
                "feishu_calendar_event_create",
                "feishu_calendar_event_update",
                "feishu_calendar_event_delete",
                "email_send",  # noqa: E501
                "connector_workflow",
            }:
                operation = await self.services.prepare_external_operation(run_id, call_id, name, arguments)
                state = operation.get("status")
                if state == "succeeded":
                    return json.dumps(operation.get("result") or {}, ensure_ascii=False)
                if state in {"uncertain", "running", "partial_success", "denied"}:
                    raise RuntimeError(operation.get("error") or f"External operation is {state}; inspect its workflow before proceeding")
                if state == "awaiting_approval" and not operation.get("needs_approval", True):
                    raise RuntimeError("This external operation is awaiting the existing approval decision")
                if state != "approved" and operation.get("needs_approval", True):
                    try:
                        await self._require_approval(
                            run_id,
                            call_id,
                            name,
                            {**arguments, "external_operation_id": operation["id"]},
                            "Approve this external account operation.",
                        )
                    except PermissionError:
                        await self.services.resolve_external_operation(operation["id"], approved=False)
                        raise
                    await self.services.resolve_external_operation(operation["id"], approved=True)
                result = await self.services.tool_call(name, {**arguments, "external_operation_id": operation["id"]}, run_id=run_id, mode=mode)
                return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
            if name in {
                "memory_create",
                "memory_update",
                "memory_archive",
                "core_memory_update",
                "core_memory_remove",
                "automation_create",
                "automation_pause",
                "automation_resume",
                "automation_cancel",
                "research_monitor_create",
            }:
                # The first call is a non-persistent preview.  The second call
                # carries the short-lived confirmation token, which is the
                # user's explicit approval for this monitor and should not
                # trigger a second generic approval dialog.
                if name == "research_monitor_create":
                    result = await self.services.tool_call(name, arguments, run_id=run_id, mode=mode)
                    return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
                await self._require_approval(run_id, call_id, name, arguments, f"执行持久化操作 {name}")
            if name in {"memory_create", "memory_update"}:
                result = await self.services.tool_call(name, arguments, run_id=run_id, mode=mode, memory_operation_id=f"tool:{run_id}:{call_id}:{name}")
            else:
                result = await self.services.tool_call(name, arguments, run_id=run_id, mode=mode)
            return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
        return f"Unknown tool: {name}"

    async def _require_approval(
        self,
        run_id: str,
        call_id: str,
        name: str,
        arguments: dict[str, Any],
        reason: str,
    ) -> None:
        allowed = await self.approve(run_id, call_id, {**arguments, "tool_name": name}, reason)
        if not allowed:
            raise PermissionError("Denied by user")

    async def _audit_web_access(self, tool_name: str, arguments: dict[str, Any]) -> None:
        if self.services is None or not hasattr(self.services, "proactive"):
            return
        try:
            await self.services.proactive.audit(
                "web.access",
                entity_type="web_request",
                actor="agent",
                details={
                    "tool_name": tool_name,
                    "url": str(arguments.get("url") or "")[:500] if arguments.get("url") else None,
                    "query": str(arguments.get("query") or "")[:300] if arguments.get("query") else None,
                    "allowed_domains": list(self.settings.web_allowed_domains),
                },
            )
        except Exception:
            return

    async def _web_search(self, query: str, limit: int) -> str:
        if not query.strip():
            raise ValueError("query is required")
        limit = max(1, min(limit, 10))
        provider = self.settings.search_provider.lower()
        if provider not in {"auto", "tavily", "duckduckgo"}:
            raise ValueError("KORENEXT_SEARCH_PROVIDER must be auto, tavily, or duckduckgo")
        if provider == "tavily" and not self.settings.tavily_api_key:
            raise RuntimeError("Tavily 搜索已指定但未配置 KORENEXT_TAVILY_API_KEY")
        if provider in {"auto", "tavily"} and self.settings.tavily_api_key:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.post(
                        "https://api.tavily.com/search",
                        json={"api_key": self.settings.tavily_api_key, "query": query, "max_results": limit},
                    )
                    response.raise_for_status()
                payload = response.json()
                results = []
                for item in payload.get("results", []):
                    if not isinstance(item, dict):
                        continue
                    title = str(item.get("title") or "").strip()
                    url = _search_result_url(str(item.get("url") or ""))
                    if not title or not url or not self._domain_allowed(url):
                        continue
                    results.append({"title": title, "url": url, "snippet": isolate_web_content(str(item.get("content") or "")), "content_role": "reference_only", "trust": source_trust(url, title), "score": item.get("score")})
                    if len(results) >= limit:
                        break
                return json.dumps({"query": query, "provider": "tavily", "results": results}, ensure_ascii=False)
            except (httpx.TransportError, httpx.TimeoutException, ValueError) as exc:
                if provider == "tavily":
                    raise RuntimeError(f"Tavily search failed: {exc}") from exc
            except httpx.HTTPStatusError as exc:
                if provider == "tavily" or exc.response.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise
        headers = {"User-Agent": "Mozilla/5.0 KoreAgentNext/0.1"}
        duck_error: Exception | None = None
        try:
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                response = await client.post("https://html.duckduckgo.com/html/", data={"q": query})
                response.raise_for_status()
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            duck_error = exc
        except httpx.HTTPStatusError as exc:
            duck_error = exc
        if duck_error is not None and provider == "auto":
            return await self._bing_search(query, limit, headers=headers, previous_error=duck_error)
        if duck_error is not None:
            raise RuntimeError(
                "网页搜索失败：DuckDuckGo 搜索端点无法连接或返回错误。请配置有效的 Tavily API Key，"
                "或检查当前网络、代理和防火墙是否允许访问 html.duckduckgo.com。"
            ) from duck_error
        pattern = re.compile(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.I | re.S)
        snippet_pattern = re.compile(r'class="result__snippet"[^>]*>(.*?)</', re.I | re.S)
        links = pattern.findall(response.text)
        snippets = snippet_pattern.findall(response.text)
        results = []
        for index, (url, title) in enumerate(links):
            clean_title = html.unescape(re.sub(r"<[^>]+>", "", title)).strip()
            clean_snippet = html.unescape(re.sub(r"<[^>]+>", "", snippets[index] if index < len(snippets) else "")).strip()
            clean_url = _search_result_url(html.unescape(url))
            if not clean_title or not clean_url or not self._domain_allowed(clean_url):
                continue
            results.append({"title": clean_title, "url": clean_url, "snippet": isolate_web_content(clean_snippet), "content_role": "reference_only", "trust": source_trust(clean_url, clean_title)})
            if len(results) >= limit:
                break
        return json.dumps({"query": query, "provider": "duckduckgo", "results": results}, ensure_ascii=False)

    async def _bing_search(
        self,
        query: str,
        limit: int,
        *,
        headers: dict[str, str],
        previous_error: Exception | None = None,
    ) -> str:
        """Fallback search for auto mode when the default endpoint is blocked."""
        try:
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                response = await client.get("https://cn.bing.com/search", params={"q": query, "count": limit})
                response.raise_for_status()
        except (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            detail = "DuckDuckGo 与 Bing 搜索端点均不可用。"
            if not self.settings.tavily_api_key:
                detail += "请配置 KORENEXT_TAVILY_API_KEY，或检查当前网络、代理和防火墙。"
            raise RuntimeError(detail) from exc
        blocks = re.findall(r'<li[^>]+class="b_algo"[^>]*>(.*?)</li>', response.text, re.I | re.S)
        results: list[dict[str, Any]] = []
        for block in blocks:
            match = re.search(r'<h2>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', block, re.I | re.S)
            if not match:
                continue
            clean_title = html.unescape(re.sub(r"<[^>]+>", "", match.group(2))).strip()
            clean_url = _search_result_url(html.unescape(match.group(1)))
            snippet_match = re.search(r'<p[^>]*>(.*?)</p>', block, re.I | re.S)
            clean_snippet = html.unescape(re.sub(r"<[^>]+>", "", snippet_match.group(1) if snippet_match else "")).strip()
            if not clean_title or not clean_url or not self._domain_allowed(clean_url):
                continue
            results.append({
                "title": clean_title,
                "url": clean_url,
                "snippet": isolate_web_content(clean_snippet),
                "content_role": "reference_only",
                "trust": source_trust(clean_url, clean_title),
            })
            if len(results) >= limit:
                break
        return json.dumps({"query": query, "provider": "bing", "results": results, "fallback": "duckduckgo_unavailable"}, ensure_ascii=False)

    async def _http_request(self, method: str, arguments: dict[str, Any]) -> str:
        url = str(arguments.get("url", ""))
        headers = {str(key): str(value) for key, value in dict(arguments.get("headers") or {}).items()}
        body = arguments.get("body")
        result = await self._fetch_public_page(url, method=method, headers=headers, body=body)
        return json.dumps(
            {"status": result["status"], "headers": result["headers"], "body": result["body"]},
            ensure_ascii=False,
        )

    async def _fetch_public_page(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> dict[str, Any]:
        request_headers = dict(headers or {})
        async with httpx.AsyncClient(timeout=30, follow_redirects=False) as client:
            for _ in range(10):
                await self._validate_public_url(url)
                response = await client.request(method, url, headers=request_headers, content=body)
                if response.status_code not in {301, 302, 303, 307, 308} or not response.headers.get("location"):
                    break
                destination = urljoin(url, response.headers["location"])
                await self._validate_public_url(destination)
                origin, target = urlsplit(url), urlsplit(destination)
                if (origin.scheme, origin.hostname, origin.port) != (target.scheme, target.hostname, target.port):
                    request_headers = {
                        key: value for key, value in request_headers.items() if key.lower() not in {"authorization", "cookie", "proxy-authorization"}
                    }
                if response.status_code == 303 or (response.status_code in {301, 302} and method not in {"GET", "HEAD"}):
                    method = "GET"
                    body = None
                url = destination
            else:
                raise ValueError("Too many HTTP redirects")
        content_type = response.headers.get("content-type", "")
        body = response.text[:50_000] if "text" in content_type or "json" in content_type else f"<{len(response.content)} binary bytes>"
        return {
            "status": response.status_code,
            "headers": dict(response.headers),
            "body": body,
            "content_type": content_type,
            "url": str(response.url),
        }

    async def _validate_public_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Only public http(s) URLs are allowed")
        host = parsed.hostname.lower()
        if not self._domain_allowed(url):
            raise PermissionError(f"Domain is not allowed by web policy: {host}")
        if self.settings.http_allow_private_network:
            return
        if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
            raise PermissionError("Local network URLs are not allowed")
        try:
            addresses = await asyncio.to_thread(socket.getaddrinfo, host, parsed.port or (443 if parsed.scheme == "https" else 80))
        except OSError as exc:
            raise ValueError(f"Cannot resolve host: {host}") from exc
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
                raise PermissionError("Private or reserved network addresses are not allowed")

    def _domain_allowed(self, url: str) -> bool:
        host = (urlsplit(url).hostname or "").lower().rstrip(".")
        blocked = tuple(self.settings.web_blocked_domains)
        if any(host == domain or host.endswith("." + domain) for domain in blocked):
            return False
        allowed = tuple(self.settings.web_allowed_domains)
        if not allowed:
            return True
        return any(host == domain or host.endswith("." + domain) for domain in allowed)
