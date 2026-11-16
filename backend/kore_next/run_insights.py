"""Build a run's usage report from persisted model-call events.

Completion and budget events can repeat the same usage payload. Only one event
per main-model step is counted, so opening an old trace cannot inflate totals.
"""

from __future__ import annotations

from typing import Any

from .runtime_policy import RunBudget, RunBudgetConfig

USAGE_FIELDS = ("input_tokens", "output_tokens", "total_tokens")
CACHE_FIELDS = (
    "cached_input_tokens",
    "uncached_input_tokens",
    "cache_read_input_tokens",
    "cache_write_input_tokens",
)
AUXILIARY_ROLES = ("context_summary", "memory_reflection", "memory_maintenance", "skill_learning")


def _empty_usage() -> dict[str, int | None]:
    return {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "model_calls": 0,
        **{field: None for field in CACHE_FIELDS},
    }


def _aggregate_usages(raw_usages: list[dict[str, Any]]) -> dict[str, int | None]:
    total = _empty_usage()
    total["model_calls"] = len(raw_usages)
    for raw in raw_usages:
        for field in USAGE_FIELDS:
            value = int(raw.get(field) or 0)
            if field == "total_tokens" and value == 0:
                value = int(raw.get("input_tokens") or 0) + int(raw.get("output_tokens") or 0)
            total[field] = int(total[field] or 0) + value
    for field in CACHE_FIELDS:
        if raw_usages and all(raw.get(field) is not None for raw in raw_usages):
            total[field] = sum(int(raw[field]) for raw in raw_usages)
    return total


def summarize_run_usage(events: list[dict[str, Any]], budget: RunBudget) -> dict[str, Any]:
    auxiliary_calls: dict[str, list[dict[str, Any]]] = {role: [] for role in AUXILIARY_ROLES}
    main_by_step: dict[int, dict[str, Any]] = {}
    inherited_usage: dict[str, Any] = {}
    inherited_model_calls = 0
    effective_budget = budget
    for event in events:
        if event.get("type") in {"run_budget_configured", "run_budget_extended"}:
            fields = event
            try:
                if fields.get("run_budget_enabled") is False or fields.get("run_budget_mode") == "unlimited":
                    effective_budget = RunBudget(RunBudgetConfig.unlimited())
                else:
                    effective_budget = RunBudget(
                        RunBudgetConfig(
                            warning_tokens=int(fields["run_budget_warning_tokens"]),
                            finalization_tokens=int(fields["run_budget_finalization_tokens"]),
                            hard_tokens=int(fields["run_budget_hard_tokens"]),
                            warning_model_calls=int(fields["run_budget_warning_model_calls"]),
                            finalization_model_calls=int(fields["run_budget_finalization_model_calls"]),
                            hard_model_calls=int(fields["run_budget_hard_model_calls"]),
                            enabled=True,
                            budget_multiplier=fields.get("run_budget_multiplier"),
                            model_call_budget=fields.get("run_budget_model_call_limit"),
                            token_budget_limit=fields.get("run_budget_token_limit"),
                            extension_count=int(fields.get("run_budget_extension_count") or 0),
                        )
                    )
            except (KeyError, TypeError, ValueError):
                effective_budget = budget
        elif event.get("type") == "run_recovered":
            inherited_usage = event.get("inherited_usage") or {}
            inherited_model_calls = int(event.get("inherited_model_calls") or 0)
        elif event.get("type") == "assistant_message" and isinstance(event.get("usage"), dict):
            main_by_step[int(event.get("step") or 0)] = event["usage"]
        elif event.get("type") == "run_budget_exceeded" and isinstance(event.get("usage"), dict):
            main_by_step.setdefault(int(event.get("step") or 0), event["usage"])
        elif event.get("type") == "auxiliary_model_completed" and isinstance(event.get("usage"), dict):
            role = event.get("role")
            if role in auxiliary_calls:
                auxiliary_calls[role].append(event["usage"])

    main = _aggregate_usages(list(main_by_step.values()))
    auxiliary = {role: _aggregate_usages(calls) for role, calls in auxiliary_calls.items()}
    provider_total = _aggregate_usages([*main_by_step.values(), *(call for calls in auxiliary_calls.values() for call in calls)])

    budget_usage = {field: int(main[field] or 0) + int(inherited_usage.get(field) or 0) for field in USAGE_FIELDS}
    decision = effective_budget.evaluate(budget_usage, int(main["model_calls"] or 0) + inherited_model_calls)
    budget_info = effective_budget.event_fields(decision)
    # Lifecycle events describe an earlier boundary, not the final usage. Never
    # replace totals or extended limits with a stale warning snapshot.
    lifecycle = [event for event in events if event.get("type", "").startswith("run_budget_")]
    budget_info["run_budget_last_action"] = lifecycle[-1]["type"] if lifecycle else None
    extension = next((event for event in reversed(events) if event.get("type") == "run_budget_extended"), None)
    if extension:
        budget_info["run_budget_extension_count"] = extension.get("run_budget_extension_count", 1)
        budget_info["run_budget_extension_assessment"] = extension.get("assessment")

    return {
        "main_agent": main,
        **auxiliary,
        "provider_total": provider_total,
        "budget": budget_info,
    }
