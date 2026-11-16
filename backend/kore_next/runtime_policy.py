from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, replace
from typing import Any

# Cumulative main-model usage is independent of the per-request context window.
# UI preferences are opt-in. Legacy explicit thresholds remain readable in traces.
DEFAULT_WARNING_CONTEXT_RATIO = 0.80
DEFAULT_FINALIZATION_CONTEXT_RATIO = 0.90
DEFAULT_HARD_CONTEXT_RATIO = 0.95


class RuntimePolicyError(RuntimeError):
    def __init__(self, message: str, *, stop_reason: str) -> None:
        super().__init__(message)
        self.stop_reason = stop_reason


@dataclass(frozen=True)
class BudgetDecision:
    status: str
    reason: str | None
    chargeable_tokens: int
    model_calls: int

    @property
    def should_warn(self) -> bool:
        return self.status == "warning"

    @property
    def should_finalize(self) -> bool:
        return self.status == "finalizing"

    @property
    def exceeded(self) -> bool:
        return self.status == "exceeded"


@dataclass(frozen=True)
class RunBudgetConfig:
    warning_tokens: int = 80_000
    finalization_tokens: int = 100_000
    hard_tokens: int = 120_000
    warning_model_calls: int = 16
    finalization_model_calls: int = 20
    hard_model_calls: int = 24
    # User-facing run budget mode.  ``unlimited`` disables this cumulative
    # accounting policy; context compaction and tool safety remain active.
    enabled: bool = True
    budget_multiplier: float | None = None
    model_call_budget: int | None = None
    context_window: int | None = None
    token_budget_limit: int | None = None
    extension_count: int = 0

    @classmethod
    def unlimited(cls) -> RunBudgetConfig:
        return cls(
            warning_tokens=0,
            finalization_tokens=0,
            hard_tokens=0,
            warning_model_calls=0,
            finalization_model_calls=0,
            hard_model_calls=0,
            enabled=False,
        )

    @classmethod
    def custom_for_context_window(
        cls,
        context_window: int,
        *,
        multiplier: float = 3.0,
        model_call_budget: int = 24,
        extension_count: int = 0,
    ) -> RunBudgetConfig:
        """Build a cumulative budget from a model's context window.

        The configured multiplier is the total token allowance.  Warning,
        finalization and hard thresholds use 80%, 90% and 95% of that
        allowance.  Model-call thresholds follow the same percentages, with
        the configured call budget as the hard limit.
        """
        window = max(8_192, int(context_window))
        preferences = validate_budget_preferences({"mode": "custom", "multiplier": multiplier, "model_call_budget": model_call_budget})
        multiplier = preferences["multiplier"]
        calls = preferences["model_call_budget"]
        token_limit = max(3, int(window * multiplier))
        warning_tokens = max(1, int(token_limit * DEFAULT_WARNING_CONTEXT_RATIO))
        finalization_tokens = max(warning_tokens + 1, int(token_limit * DEFAULT_FINALIZATION_CONTEXT_RATIO))
        hard_tokens = max(finalization_tokens + 1, int(token_limit * DEFAULT_HARD_CONTEXT_RATIO))
        # A hard call limit must remain exactly what the user selected. Clamp
        # the lower thresholds so even the smallest allowed value (3) keeps
        # the strict warning < finalization < hard invariant.
        hard_calls = calls
        finalization_calls = max(2, min(hard_calls - 1, int(calls * DEFAULT_FINALIZATION_CONTEXT_RATIO)))
        warning_calls = max(1, min(finalization_calls - 1, int(calls * DEFAULT_WARNING_CONTEXT_RATIO)))
        return cls(
            warning_tokens=warning_tokens,
            finalization_tokens=finalization_tokens,
            hard_tokens=hard_tokens,
            warning_model_calls=warning_calls,
            finalization_model_calls=finalization_calls,
            hard_model_calls=hard_calls,
            enabled=True,
            budget_multiplier=multiplier,
            model_call_budget=calls,
            context_window=window,
            token_budget_limit=token_limit,
            extension_count=extension_count,
        )

    def extended_once(self) -> RunBudgetConfig:
        """Return one additional allowance for a demonstrably progressing run."""
        if not self.enabled or self.extension_count:
            return self
        # Add 50% of the original allowance once. Never go through user-input
        # clamps here: a valid large budget must still receive its full extension.
        return replace(
            self,
            warning_tokens=math.ceil(self.warning_tokens * 1.5),
            finalization_tokens=math.ceil(self.finalization_tokens * 1.5),
            hard_tokens=math.ceil(self.hard_tokens * 1.5),
            warning_model_calls=math.ceil(self.warning_model_calls * 1.5),
            finalization_model_calls=math.ceil(self.finalization_model_calls * 1.5),
            hard_model_calls=math.ceil(self.hard_model_calls * 1.5),
            token_budget_limit=math.ceil(self.token_budget_limit * 1.5) if self.token_budget_limit else None,
            extension_count=1,
        )

    def scaled_for_context_window(self, context_window: int) -> RunBudgetConfig:
        """Scale default run thresholds with the selected model's context size.

        The existing environment values remain explicit overrides when they
        already fit below the selected window. Oversized explicit token
        thresholds are proportionally clamped so the safety invariant still
        holds for every model.
        """
        if not self.enabled:
            return self
        # Custom budgets already contain their model-specific thresholds.
        if self.budget_multiplier is not None:
            return self
        defaults = RunBudgetConfig()
        window = max(8_192, int(context_window))
        if self != defaults:
            if self.hard_tokens < window:
                return self
            # Preserve a custom budget's relative thresholds while fitting its
            # hard limit below the model context window.
            target_hard = max(3, window - 1)
            scale = target_hard / self.hard_tokens
            warning_tokens = max(1, round(self.warning_tokens * scale))
            finalization_tokens = max(warning_tokens + 1, round(self.finalization_tokens * scale))
            hard_tokens = target_hard
            finalization_tokens = min(finalization_tokens, hard_tokens - 1)
            warning_tokens = min(warning_tokens, finalization_tokens - 1)
            return RunBudgetConfig(
                warning_tokens=warning_tokens,
                finalization_tokens=finalization_tokens,
                hard_tokens=hard_tokens,
                warning_model_calls=self.warning_model_calls,
                finalization_model_calls=self.finalization_model_calls,
                hard_model_calls=self.hard_model_calls,
            )
        # Keep token thresholds below one context window.  A run may still
        # make several requests, but the default policy will stop before its
        # cumulative chargeable token usage reaches the model's single-window
        # capacity.  The model-call thresholds remain scaled from the existing
        # defaults because they protect against tool/model loops independently
        # of token size.
        warning_tokens = max(1, int(window * DEFAULT_WARNING_CONTEXT_RATIO))
        finalization_tokens = max(warning_tokens + 1, int(window * DEFAULT_FINALIZATION_CONTEXT_RATIO))
        hard_tokens = max(finalization_tokens + 1, int(window * DEFAULT_HARD_CONTEXT_RATIO))
        # Guard the strict "below context window" invariant against rounding
        # and unusually small manually supplied windows.
        hard_tokens = min(hard_tokens, window - 1)
        finalization_tokens = min(finalization_tokens, hard_tokens - 1)
        warning_tokens = min(warning_tokens, finalization_tokens - 1)
        ratio = max(0.5, min(4.0, window / 64_000))
        return RunBudgetConfig(
            warning_tokens=warning_tokens,
            finalization_tokens=finalization_tokens,
            hard_tokens=hard_tokens,
            warning_model_calls=max(4, round(self.warning_model_calls * ratio)),
            finalization_model_calls=max(6, round(self.finalization_model_calls * ratio)),
            hard_model_calls=max(8, round(self.hard_model_calls * ratio)),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __post_init__(self) -> None:
        if not self.enabled:
            return
        if not (0 < self.warning_tokens < self.finalization_tokens < self.hard_tokens):
            raise ValueError("token budget thresholds must increase")
        if not (0 < self.warning_model_calls < self.finalization_model_calls < self.hard_model_calls):
            raise ValueError("model call budget thresholds must increase")


class RunBudget:
    def __init__(self, config: RunBudgetConfig | None = None) -> None:
        self.config = config or RunBudgetConfig()

    def evaluate(self, usage: dict[str, int], model_calls: int) -> BudgetDecision:
        tokens = int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0))
        if not self.config.enabled:
            return BudgetDecision("unlimited", None, tokens, model_calls)
        if tokens >= self.config.hard_tokens or model_calls >= self.config.hard_model_calls:
            reason = "tokens" if tokens >= self.config.hard_tokens else "model_calls"
            return BudgetDecision("exceeded", reason, tokens, model_calls)
        if tokens >= self.config.finalization_tokens or model_calls >= self.config.finalization_model_calls:
            reason = "tokens" if tokens >= self.config.finalization_tokens else "model_calls"
            return BudgetDecision("finalizing", reason, tokens, model_calls)
        if tokens >= self.config.warning_tokens or model_calls >= self.config.warning_model_calls:
            reason = "tokens" if tokens >= self.config.warning_tokens else "model_calls"
            return BudgetDecision("warning", reason, tokens, model_calls)
        return BudgetDecision("normal", None, tokens, model_calls)

    def event_fields(self, decision: BudgetDecision) -> dict[str, Any]:
        return {
            "run_budget_enabled": self.config.enabled,
            "run_budget_mode": "custom" if self.config.enabled else "unlimited",
            "run_budget_status": decision.status,
            "run_budget_reason": decision.reason,
            "run_budget_chargeable_tokens": decision.chargeable_tokens,
            "run_budget_model_calls": decision.model_calls,
            "run_budget_warning_tokens": self.config.warning_tokens,
            "run_budget_finalization_tokens": self.config.finalization_tokens,
            "run_budget_hard_tokens": self.config.hard_tokens,
            "run_budget_warning_model_calls": self.config.warning_model_calls,
            "run_budget_finalization_model_calls": self.config.finalization_model_calls,
            "run_budget_hard_model_calls": self.config.hard_model_calls,
            "run_budget_multiplier": self.config.budget_multiplier,
            "run_budget_token_limit": self.config.token_budget_limit,
            "run_budget_model_call_limit": self.config.model_call_budget,
            "run_budget_extension_count": self.config.extension_count,
        }

    def extend_once(self) -> RunBudget:
        return RunBudget(self.config.extended_once())


def validate_budget_preferences(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("运行预算必须是有效配置")
    mode = value.get("mode", "unlimited")
    if mode not in {"unlimited", "custom"}:
        raise ValueError("预算模式只能是无限制或自定义限制")
    try:
        multiplier = float(value.get("multiplier", 3))
        calls = float(value.get("model_call_budget", 24))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("请输入有效的预算倍数和模型调用次数") from exc
    if isinstance(value.get("multiplier"), bool) or not math.isfinite(multiplier) or not 0.25 <= multiplier <= 100:
        raise ValueError("上下文窗口倍数须在 0.25 到 100 之间")
    if isinstance(value.get("model_call_budget"), bool) or not math.isfinite(calls) or not calls.is_integer() or not 3 <= calls <= 100000:
        raise ValueError("模型调用预算须为 3 到 100000 的整数（包含最终答复）")
    return {"mode": mode, "multiplier": multiplier, "model_call_budget": int(calls)}


class RunProgress:
    """Conservative evidence heuristic, independent of model self-assessment.

    New successful read results or confirmed writes count; planning, catalog
    discovery, time queries, errors and duplicate evidence do not. Only the
    warning round is used to grant an extension, so stale progress cannot help.
    Store hashes rather than personal tool output in the checkpoint.
    """

    READ_TOOLS = {
        "read_file", "file_extract", "web_search", "web_research", "http_request",
        "history_search", "history_read", "session_search", "unified_search",
        "memory_read", "memory_list", "connector_related",
    }
    WRITE_TOOLS = {
        "write_file", "publish_file", "artifact_publish", "email_send",
        "feishu_task_create", "feishu_task_update", "feishu_task_delete",
        "feishu_document_create", "feishu_document_append", "feishu_document_delete", "feishu_document_block_update",
        "feishu_calendar_event_create", "feishu_calendar_event_update", "feishu_calendar_event_delete",
        "connector_workflow", "research_monitor_create",
    }
    BAD_STATUSES = {"failed", "error", "denied", "cancelled", "uncertain", "unknown", "partial_success", "running", "pending", "awaiting_approval"}
    VOLATILE = {"created_at", "updated_at", "fetched_at", "timestamp", "request_id", "duration_ms", "query", "provider"}

    def __init__(self, state: dict[str, Any] | None = None):
        state = state or {}
        self.seen = set(state.get("seen", []))
        self.writes = set(state.get("writes", []))
        self.round_evidence = list(state.get("round_evidence", []))
        self.round_failures = int(state.get("round_failures", 0))

    @staticmethod
    def _hash(value: Any) -> str:
        return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()

    @classmethod
    def _stable(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: cls._stable(v) for k, v in value.items() if k not in cls.VOLATILE}
        if isinstance(value, list):
            return [cls._stable(v) for v in value]
        return " ".join(value.split()) if isinstance(value, str) else value

    @classmethod
    def _failed(cls, value: Any) -> bool:
        if isinstance(value, dict):
            if value.get("success") is False or value.get("isError") is True or value.get("error"):
                return True
            if str(value.get("status", "")).lower() in cls.BAD_STATUSES:
                return True
            code = value.get("status_code", value.get("http_status"))
            if isinstance(code, int | float) and code >= 400:
                return True
            if value.get("code") not in (None, 0, "0"):
                return True
        return False

    def observe(self, name: str, arguments: dict[str, Any], output: Any, success: bool) -> dict[str, Any]:
        result = {"progress": False, "reason": "unverified_tool"}
        payload = output
        if isinstance(output, str):
            try:
                payload = json.loads(output)
            except (ValueError, TypeError):
                payload = output.strip()
        failed = not success or self._failed(payload)
        if isinstance(payload, str) and re.match(r"^(error\b|failed\b|denied\b|unknown tool|no results|not found|错误|失败|未找到|没有结果)", payload, re.I):
            failed = True
        if failed:
            self.round_failures += 1
            return {"progress": False, "reason": "failed_or_unconfirmed"}
        if name not in self.READ_TOOLS | self.WRITE_TOOLS:
            return result
        if name in self.WRITE_TOOLS:
            if name == "write_file":
                confirmed = isinstance(payload, str) and payload.startswith("Wrote ")
            elif name == "publish_file":
                confirmed = isinstance(payload, str) and payload.startswith("Published artifact ")
            elif isinstance(payload, dict):
                confirmed = any(payload.get(k) for k in ("message_id", "task_id", "event_id", "document_id", "artifact", "monitor"))
                confirmed = confirmed or payload.get("status") in {"sent", "created", "updated", "deleted", "completed", "succeeded"}
                confirmed = confirmed or any(isinstance(payload.get(k), dict) and (payload[k].get("id") or payload[k].get("guid")) for k in ("task", "event", "document"))
            else:
                confirmed = False
            call_hash = self._hash({"name": name, "arguments": {k: v for k, v in arguments.items() if k not in {"idempotency_key", "external_operation_id"}}})
            if not confirmed:
                return {"progress": False, "reason": "no_confirmation_receipt"}
            if call_hash in self.writes:
                return {"progress": False, "reason": "repeated_action"}
            self.writes.add(call_hash)
            evidence = {"tool": name, "kind": "confirmed_action"}
        else:
            if isinstance(payload, dict):
                # Ignore wrappers (e.g. different search queries or URLs) when
                # deciding whether the returned information is actually new.
                for key in ("results", "sources", "document", "body", "text", "content", "matches", "items", "memories"):
                    if key in payload:
                        payload = payload[key]
                        break
            if not payload or isinstance(payload, bool | int | float):
                return {"progress": False, "reason": "empty_result"}
            if isinstance(payload, list):
                payload = [item for item in payload if item and not self._failed(item)]
                if not payload:
                    return {"progress": False, "reason": "empty_result"}
            if isinstance(payload, str) and (len(payload.strip()) < 8 or payload.lower().strip() in {"ok", "success", "done", "no results found", "no matches found"}):
                return {"progress": False, "reason": "empty_result"}
            evidence = {"tool": name, "kind": "new_information"}
        fingerprint = self._hash(self._stable(payload))
        if fingerprint in self.seen:
            return {"progress": False, "reason": "duplicate_result"}
        self.seen.add(fingerprint)
        self.round_evidence.append(evidence)
        return {"progress": True, "reason": evidence["kind"]}

    def begin_round(self) -> None:
        self.round_evidence = []
        self.round_failures = 0

    def assessment(self) -> dict[str, Any]:
        # A mixed batch ending in errors needs recovery, not more automatic
        # spending. This deliberately prefers false negatives to false positives.
        return {"has_progress": bool(self.round_evidence) and self.round_failures == 0,
                "evidence": self.round_evidence, "failures": self.round_failures}

    def snapshot(self) -> dict[str, Any]:
        return {"seen": sorted(self.seen), "writes": sorted(self.writes),
                "round_evidence": self.round_evidence, "round_failures": self.round_failures}


class RunBudgetController:
    """Checkpointable warning -> last tools -> assess -> extend/close policy."""

    def __init__(self, budget: RunBudget, state: dict[str, Any] | None = None):
        state = state or {}
        self.budget = RunBudget(RunBudgetConfig(**state["config"])) if state.get("config") else budget
        self.phase = str(state.get("phase", "active"))
        self.progress = RunProgress(state.get("progress"))

    def before_request(self, usage: dict[str, int], calls: int, *, max_steps: int | None = None) -> tuple[str, list[dict[str, Any]]]:
        if not self.budget.config.enabled:
            return "normal", []
        events = []
        decision = self.budget.evaluate(usage, calls)
        if self.phase == "review":
            assessment = self.progress.assessment()
            if assessment["has_progress"] and self.budget.config.extension_count == 0 and self.budget.config.budget_multiplier is not None:
                events.append({"type": "run_budget_extension_requested", "assessment": assessment, **self.budget.event_fields(decision)})
                self.budget = self.budget.extend_once()
                self.phase = "active"
                decision = self.budget.evaluate(usage, calls)
                events.append({"type": "run_budget_extended", "assessment": assessment, **self.budget.event_fields(decision)})
            else:
                self.phase = "closing"
                events.append({"type": "run_budget_extension_denied", "assessment": assessment,
                               "extension_reason": "hard_limit" if decision.exceeded else "already_extended" if self.budget.config.extension_count else "policy_disabled" if self.budget.config.budget_multiplier is None else "no_progress",
                               **self.budget.event_fields(decision)})
        if decision.exceeded and self.phase != "closing":
            events.append({"type": "run_budget_exceeded", **self.budget.event_fields(decision)})
            return "stop", events
        if self.phase == "active" and decision.status in {"warning", "finalizing"}:
            events.append({"type": "run_budget_warning", **self.budget.event_fields(decision)})
            self.phase = "last_tools"
        # Reserve one model request for a final answer, even for tiny limits.
        if calls >= self.budget.config.hard_model_calls - 1:
            self.phase = "closing"
        if self.phase == "closing":
            events.append({"type": "run_budget_finalizing", **self.budget.event_fields(decision)})
            return "final", events
        if self.phase == "last_tools":
            events.append({"type": "run_budget_tool_round", **self.budget.event_fields(decision)})
            return "last_tools", events
        return "normal", events

    def finish_round(self) -> None:
        if self.phase == "last_tools":
            self.phase = "review"

    def snapshot(self) -> dict[str, Any]:
        return {"config": self.budget.config.as_dict(), "phase": self.phase, "progress": self.progress.snapshot()}


class RepeatedToolCallGuard:
    """Stops the model after three identical consecutive tool rounds."""

    def __init__(self, limit: int = 3) -> None:
        self.limit = limit
        self._last_signature: str | None = None
        self._count = 0

    def observe(self, calls: list[dict[str, Any]]) -> None:
        normalized = [
            {
                "name": str(call.get("name") or ""),
                "arguments": call.get("arguments") if isinstance(call.get("arguments"), dict) else {},
            }
            for call in calls
        ]
        encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        signature = hashlib.sha256(encoded.encode()).hexdigest()
        if signature == self._last_signature:
            self._count += 1
        else:
            self._last_signature = signature
            self._count = 1
        if self._count >= self.limit:
            names = ", ".join(item["name"] for item in normalized) or "unknown"
            raise RuntimePolicyError(
                f"Repeated tool call loop detected: {names}",
                stop_reason="repeated_tool_call",
            )
