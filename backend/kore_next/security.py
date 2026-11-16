from __future__ import annotations

import base64
import hashlib
import re
import secrets
from collections.abc import Mapping
from typing import Any

EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
TOKEN = re.compile(
    r"(?i)\b(?:bearer\s+|token[=:]\s*|access[_-]?token[=:]\s*|refresh[_-]?token[=:]\s*|api[_-]?key[=:]\s*|secret[=:]\s*)[A-Za-z0-9._~+/=-]{8,}"
)
PHONE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")


def redact_text(value: str, *, keep_email: bool = False, max_chars: int | None = None) -> str:
    text = str(value or "")
    text = TOKEN.sub("[REDACTED_TOKEN]", text)
    if keep_email:
        # Preserve complete addresses in user-facing action previews. Protect
        # the address while masking standalone phone numbers in surrounding text.
        addresses: list[str] = []

        def protect(match: re.Match[str]) -> str:
            addresses.append(match.group(0))
            return f"__KORE_EMAIL_{len(addresses) - 1}__"

        text = EMAIL.sub(protect, text)
        text = PHONE.sub("[REDACTED_PHONE]", text)
        for index, address in enumerate(addresses):
            text = text.replace(f"__KORE_EMAIL_{index}__", address)
    else:
        text = PHONE.sub("[REDACTED_PHONE]", text)
        text = EMAIL.sub("[REDACTED_EMAIL]", text)
    if max_chars is not None and len(text) > max_chars:
        text = text[: max(0, max_chars - 3)] + "..."
    return text


def redact(value: Any, *, key: str = "") -> Any:
    """Return a JSON-safe redacted copy suitable for logs, audit and diagnostics."""
    lower = key.lower()
    secret_keys = {"token", "access_token", "refresh_token", "id_token", "api_key", "secret", "password", "app_password", "authorization"}
    if lower in secret_keys or lower.endswith("_access_token") or lower.endswith("_refresh_token"):
        return "[REDACTED]"
    if any(part in lower for part in ("body", "html_body", "content_base64", "raw_content", "attachment_content", "reasoning")):
        return "[REDACTED_CONTENT]"
    if isinstance(value, Mapping):
        return {str(k): redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [redact(item, key=key) for item in value]
    if isinstance(value, str):
        return redact_text(value, max_chars=2_000)
    return value


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_download_token() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
