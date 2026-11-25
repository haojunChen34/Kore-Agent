"""Bounded execution checks; none of these infer business facts from topic words."""
from __future__ import annotations

import json
import re
from typing import Any


def explicitly_read_only(text: str) -> bool:
    return bool(re.search(r'(?:本轮|全程|仅|只进行)?只读(?:检查|分析|审查|操作|模式|任务|[，。；\s]|$)|\bread[- ]only\b|不要(?:创建|修改|写入)[^。；\n]{0,30}文件', text, re.I))


def execution_observations(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations = []
    for message in messages:
        if message.get('role') != 'tool':
            continue
        name = message.get('name') or ''
        content = str(message.get('content') or '')
        try:
            payload = json.loads(content)
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if payload.get('error') or payload.get('isError') or content.startswith('Error:'):
            observations.append({'tool': name, 'state': 'failed; report unresolved or verify recovery'})
        if name in {'unified_search', 'tool_catalog', 'web_search'} and (payload.get('count') == 0 or payload.get('matching_count') == 0 or any(payload.get(key) == [] for key in ('results', 'matches'))):
            observations.append({'tool': name, 'state': 'empty result is not proof the source does not exist; use a bounded alternative query or list the known source'})
        document = payload.get('document') or payload
        if isinstance(document, dict) and document.get('truncated'):
            observations.append({'tool': name, 'state': 'partial read; hidden content not checked', 'next_offset': document.get('next_offset')})
    return observations[-12:]


def completion_instruction(messages: list[dict[str, Any]], task: dict[str, Any] | None) -> str:
    return (
        'Perform the one bounded delivery review before finishing. Match every user request and prerequisite to actual results. '
        'If a required item is missing, use available tools within the remaining budget or explicitly report it incomplete. '
        'Do not repeat successful side effects. Do not turn a conditional request into authorization when its prerequisite is unmet. '
        'A draft or preview is not a send. For sources, cite only claims supported by the actual text; distinguish inference. '
        'Check task steps with task_get/task_update and actual object/file results; do not mark a step complete merely because the conversation ends. '
        'Do not add goals, actions, reminders or files beyond the user request. If everything is verified, return the final answer.\n'
        + json.dumps({'observations': execution_observations(messages), 'tracked_task': task}, ensure_ascii=False)
    )
