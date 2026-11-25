from __future__ import annotations

import asyncio
import base64
import binascii
import email
import email.policy
import email.utils
import hashlib
import html
import imaplib
import os
import re
import smtplib
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.header import decode_header
from email.message import EmailMessage, Message
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx

from .secrets import SecretStore
from .util import now

FEISHU_BASE = "https://open.feishu.cn/open-apis"


class ConnectorConfigurationError(RuntimeError):
    """A provider is not configured or needs user action before it can sync."""


class ConnectorRemoteError(RuntimeError):
    """A provider rejected a request or returned an invalid response."""


class ConnectorPartialSuccess(RuntimeError):
    """A multi-request connector operation completed only some remote steps."""

    def __init__(self, message: str, result: dict[str, Any]):
        super().__init__(message)
        self.result = result


@dataclass(frozen=True)
class FeishuConfig:
    app_id: str
    app_secret: str
    redirect_uri: str
    scopes: str

    @property
    def enabled(self) -> bool:
        return bool(self.app_id and self.app_secret and self.redirect_uri)


class FeishuClient:
    """Small, dependency-light Lark/Feishu OAuth and read-sync client.

    The client deliberately keeps provider credentials in SecretStore and returns
    the project's neutral connector item contract instead of exposing raw tokens.
    Endpoint responses are parsed defensively because Feishu has changed response
    envelopes between API generations.
    """

    DEFAULT_SCOPES = " ".join(
        (
            "calendar:calendar",
            "task:task:write",
            "task:comment:write",
            # Listing and moving the user's cloud-space files to trash require
            # the read/write drive scope. Existing tokens keep their old
            # grants until the user authorizes again.
            "drive:drive",
            # Full document scope includes create, read and block mutations.
            # The narrower readonly/write_only scopes cannot create a document.
            "docx:document",
            "contact:user.base:readonly",
            # Required for a refresh_token. Without this scope Feishu only
            # returns a short-lived user_access_token.
            "offline_access",
        )
    )

    def __init__(self, secrets: SecretStore, config: FeishuConfig):
        self.secrets = secrets
        self.config = config
        # Task v2 returns a short public task_id for display/listing but its
        # mutation endpoints require the UUID guid. Keep the mapping for tasks
        # created during this host session; synced records also expose guid.
        self._task_guids: dict[str, str] = {}

    @staticmethod
    def from_environment(secrets: SecretStore, *, redirect_uri: str | None = None) -> FeishuClient:
        import os

        configured_scopes = os.getenv("KORENEXT_FEISHU_SCOPES", "").strip() or FeishuClient.DEFAULT_SCOPES
        # Persistent connector use requires Feishu's offline_access grant even
        # when a custom scope list is supplied in .env.
        scopes = list(dict.fromkeys([*configured_scopes.split(), "offline_access"]))
        return FeishuClient(
            secrets,
            FeishuConfig(
                app_id=os.getenv("KORENEXT_FEISHU_APP_ID", "").strip(),
                app_secret=os.getenv("KORENEXT_FEISHU_APP_SECRET", "").strip(),
                redirect_uri=(redirect_uri or os.getenv("KORENEXT_FEISHU_REDIRECT_URI", "")).strip(),
                scopes=" ".join(scopes),
            ),
        )

    def authorize_url(self, state: str, *, provider: str, redirect_uri: str | None = None) -> str:
        if not self.config.enabled:
            raise ConnectorConfigurationError(
                "飞书真实连接器未配置。请设置 KORENEXT_FEISHU_APP_ID、KORENEXT_FEISHU_APP_SECRET 和 KORENEXT_FEISHU_REDIRECT_URI。"
            )
        scopes = self.config.scopes
        if provider == "calendar":
            scopes = " ".join(item for item in scopes.split() if item == "offline_access" or item.startswith("calendar:") or item.startswith("contact:"))
        elif provider == "todo":
            scopes = " ".join(item for item in scopes.split() if item == "offline_access" or item.startswith("task:") or item.startswith("contact:"))
        elif provider == "notes":
            scopes = " ".join(item for item in scopes.split() if item == "offline_access" or item.startswith(("drive:", "docx:", "contact:")))
        return "https://open.feishu.cn/open-apis/authen/v1/authorize?" + urlencode(
            {
                "app_id": self.config.app_id,
                "response_type": "code",
                "redirect_uri": redirect_uri or self.config.redirect_uri,
                "scope": scopes,
                "prompt": "consent",
                "state": state,
            }
        )

    async def exchange_code(self, code: str, *, redirect_uri: str | None = None) -> dict[str, Any]:
        if not self.config.enabled:
            raise ConnectorConfigurationError("飞书 OAuth 尚未配置")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{FEISHU_BASE}/authen/v2/oauth/token",
                json={
                    "grant_type": "authorization_code",
                    "client_id": self.config.app_id,
                    "client_secret": self.config.app_secret,
                    "code": code,
                    "redirect_uri": redirect_uri or self.config.redirect_uri,
                },
            )
        payload = self._payload(response)
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        access_token = str(data.get("access_token") or "")
        if not access_token:
            raise ConnectorRemoteError("飞书 OAuth 返回中没有 access_token")
        return data

    async def refresh(self, refresh_token: str) -> dict[str, Any]:
        if not self.config.enabled:
            raise ConnectorConfigurationError("飞书 OAuth 尚未配置")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{FEISHU_BASE}/authen/v2/oauth/token",
                json={
                    "grant_type": "refresh_token",
                    "client_id": self.config.app_id,
                    "client_secret": self.config.app_secret,
                    "refresh_token": refresh_token,
                },
            )
        payload = self._payload(response)
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        if not data.get("access_token"):
            raise ConnectorRemoteError("飞书刷新令牌失败")
        return data

    async def user_info(self, access_token: str) -> dict[str, Any]:
        return await self.request("GET", "/authen/v1/user_info", access_token=access_token)

    async def request(self, method: str, path: str, *, access_token: str, **kwargs: Any) -> dict[str, Any]:
        headers = dict(kwargs.pop("headers", {}) or {})
        operation_id = kwargs.pop("operation_id", None)
        headers["Authorization"] = f"Bearer {access_token}"
        headers.setdefault("Content-Type", "application/json; charset=utf-8")
        if operation_id:
            headers["X-KoreAgent-Operation-ID"] = str(operation_id)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(method, f"{FEISHU_BASE}{path}", headers=headers, **kwargs)
        return self._payload(response)

    @staticmethod
    def _payload(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ConnectorRemoteError(f"飞书返回了无法解析的响应（HTTP {response.status_code}）") from exc
        if response.status_code >= 400 or (isinstance(payload, dict) and payload.get("code", 0) not in (0, None)):
            message = payload.get("msg") if isinstance(payload, dict) else None
            raise ConnectorRemoteError(str(message or f"飞书请求失败（HTTP {response.status_code}）"))
        return payload if isinstance(payload, dict) else {"data": payload}

    async def records(self, provider: str, access_token: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if provider == "calendar":
            return await self._calendar_records(access_token, limit=limit)
        if provider == "todo":
            return await self._task_records(access_token, limit=limit)
        if provider == "notes":
            return await self._document_records(access_token, limit=limit)
        raise ConnectorConfigurationError(f"飞书不支持连接器类型：{provider}")

    async def records_page(
        self,
        provider: str,
        access_token: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Fetch one provider page and return a durable continuation token.

        ``records`` remains the compatibility API used by integrations and
        tests. New sync code uses this method so a failed page can be resumed
        without importing the previous pages again.
        """
        if provider == "calendar":
            return await self._calendar_records(access_token, limit=limit, page_token=cursor, return_page=True)
        if provider == "todo":
            return await self._task_records(access_token, limit=limit, page_token=cursor, return_page=True)
        if provider == "notes":
            return await self._document_records(access_token, limit=limit, page_token=cursor, return_page=True)
        raise ConnectorConfigurationError(f"飞书不支持连接器类型：{provider}")

    async def primary_calendar(self, access_token: str) -> str:
        calendars = await self.request("GET", "/calendar/v4/calendars", access_token=access_token, params={"page_size": 50})
        data = calendars.get("data") or {}
        items = data.get("calendar_list") or data.get("items") or []
        selected = next((item for item in items if item.get("is_primary") or item.get("type") == "primary"), None) or (items[0] if items else None)
        calendar_id = str((selected or {}).get("calendar_id") or "")
        if not calendar_id:
            raise ConnectorRemoteError("飞书账号没有可用的主日历")
        return calendar_id

    async def create_calendar_event(
        self,
        access_token: str,
        *,
        title: str,
        starts_at: str,
        ends_at: str,
        description: str = "",
        location: str = "",
        timezone: str | None = None,
        participants: list[Any] | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        calendar_id = await self.primary_calendar(access_token)
        payload: dict[str, Any] = {
            "summary": title,
            "description": description,
            "start_time": self._calendar_time(starts_at, timezone),
            "end_time": self._calendar_time(ends_at, timezone),
        }
        if location:
            payload["location"] = {"name": location}
        if timezone:
            ZoneInfo(timezone)
            payload["start_time"]["timezone"] = timezone
            payload["end_time"]["timezone"] = timezone
        attendees = self._calendar_attendees(participants or [])
        result = await self.request(
            "POST", f"/calendar/v4/calendars/{calendar_id}/events", access_token=access_token, json=payload, operation_id=operation_id
        )  # noqa: E501
        if attendees:
            event = (result.get("data") or {}).get("event") or result.get("event") or {}
            event_id = str(event.get("event_id") or event.get("id") or "")
            try:
                if not event_id:
                    raise ConnectorRemoteError("创建结果没有返回事件 ID，未发送邀请")
                await self.request("POST", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees", access_token=access_token,
                                   json={"attendees": attendees, "need_notification": True}, operation_id=f"{operation_id}:attendees" if operation_id else None)
            except Exception as exc:
                raise ConnectorPartialSuccess("日历已创建，但参与人邀请未确认；请核实邀请结果，不要重复创建事件。", {
                    "status": "partial_success", "event": event, "calendar_id": calendar_id, "invitation_status": "uncertain" if isinstance(exc, (httpx.TimeoutException, OSError)) else "failed",
                    "participants": participants, "error": str(exc),
                }) from exc
        return result

    @staticmethod
    def _calendar_time(value: str, timezone: str | None) -> dict[str, str]:
        zone = ZoneInfo(timezone) if timezone else None
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            if zone is None:
                raise ValueError("日历时间需要 UTC 偏移或明确的时区")
            stamp = stamp.replace(tzinfo=zone)
        result = {"timestamp": str(int(stamp.timestamp()))}
        if timezone:
            result["timezone"] = timezone
        return result

    @staticmethod
    def _calendar_attendees(participants: list[Any]) -> list[dict[str, str]]:
        attendees = []
        for person in participants:
            if isinstance(person, str):
                person = {"email": person} if "@" in person else {"user_id": person}
            if not isinstance(person, dict):
                raise ValueError("参与人必须是邮箱或飞书用户 ID")
            email_address = str(person.get("email") or person.get("third_party_email") or "")
            user_id = str(person.get("user_id") or person.get("open_id") or "")
            if email_address:
                if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email_address):
                    raise ValueError("参与人邮箱无效")
                attendees.append({"type": "third_party", "third_party_email": email_address})
            elif user_id:
                if not re.fullmatch(r"[A-Za-z0-9_-]+", user_id):
                    raise ValueError("参与人需要实际飞书用户 ID，不能使用姓名或角色名称")
                attendees.append({"type": "user", "user_id": user_id})
            else:
                raise ValueError("参与人缺少邮箱或飞书用户 ID；请先澄清")
        return attendees

    async def update_calendar_event(
        self,
        access_token: str,
        *,
        event_id: str,
        title: str | None = None,
        starts_at: str | None = None,
        ends_at: str | None = None,
        description: str | None = None,
        location: str | None = None,
        timezone: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        calendar_id = await self.primary_calendar(access_token)
        payload: dict[str, Any] = {}
        if title is not None:
            payload["summary"] = title
        if description is not None:
            payload["description"] = description
        if starts_at is not None:
            payload["start_time"] = self._calendar_time(starts_at, timezone)
        if ends_at is not None:
            payload["end_time"] = self._calendar_time(ends_at, timezone)
        if location is not None:
            payload["location"] = {"name": location} if location else {}
        if timezone and starts_at is None and ends_at is None:
            raise ValueError("调整时区时请同时提供开始或结束时间")
        if not payload:
            raise ValueError("at least one event field must be supplied")
        return await self.request(
            "PATCH", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}", access_token=access_token, json=payload, operation_id=operation_id
        )  # noqa: E501

    async def delete_calendar_event(self, access_token: str, *, event_id: str, operation_id: str | None = None) -> dict[str, Any]:
        calendar_id = await self.primary_calendar(access_token)
        return await self.request(
            "DELETE", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}", access_token=access_token, operation_id=operation_id
        )  # noqa: E501

    async def create_task(
        self, access_token: str, *, title: str, description: str = "", due_at: str | None = None, operation_id: str | None = None
    ) -> dict[str, Any]:  # noqa: E501
        payload: dict[str, Any] = {"summary": title, "description": description}
        if due_at:
            payload["due"] = self._task_time(due_at)
        result = await self.request("POST", "/task/v2/tasks", access_token=access_token, json=payload, operation_id=operation_id)
        data = result.get("data") if isinstance(result.get("data"), dict) else result
        task = data.get("task") if isinstance(data.get("task"), dict) else data
        task_id = str(task.get("task_id") or task.get("id") or "")
        guid = str(task.get("guid") or "")
        if task_id and guid:
            self._task_guids[task_id] = guid
        return result

    async def update_task(
        self,
        access_token: str,
        *,
        task_id: str,
        title: str | None = None,
        description: str | None = None,
        due_at: str | None = None,
        status: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        update_fields: list[str] = []
        if title is not None:
            payload["summary"] = title
            update_fields.append("summary")
        if description is not None:
            payload["description"] = description
            update_fields.append("description")
        if due_at is not None:
            payload["due"] = self._task_time(due_at) if due_at else {}
            update_fields.append("due")
        if status is not None:
            if status not in {"todo", "done"}:
                raise ValueError("task status must be 'todo' or 'done'")
            # Feishu Task v2 updates completion through agent_task_status:
            # 1 is active and 4 is completed. The read model still exposes
            # the friendlier todo/done values to the rest of the application.
            payload["agent_task_status"] = 4 if status == "done" else 1
            update_fields.append("agent_task_status")
        if not payload:
            raise ValueError("at least one task field must be supplied")
        guid = await self.task_guid(access_token, task_id)
        return await self.request(
            "PATCH",
            f"/task/v2/tasks/{guid}",
            access_token=access_token,
            json={"task": payload, "update_fields": update_fields},
            operation_id=operation_id,
        )

    async def delete_task(self, access_token: str, *, task_id: str, operation_id: str | None = None) -> dict[str, Any]:
        guid = await self.task_guid(access_token, task_id)
        return await self.request("DELETE", f"/task/v2/tasks/{guid}", access_token=access_token, operation_id=operation_id)

    async def task_guid(self, access_token: str, task_id: str) -> str:
        """Resolve either Feishu's public task_id (for example t100001) or its UUID guid."""
        candidate = str(task_id).strip()
        if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", candidate):
            return candidate
        cached = self._task_guids.get(candidate)
        if cached:
            return cached
        response = await self.request("GET", "/task/v2/tasks", access_token=access_token, params={"page_size": 100})
        rows = (response.get("data") or {}).get("items") or (response.get("data") or {}).get("tasks") or []
        match = next((item for item in rows if str(item.get("task_id") or item.get("id") or "") == candidate), None)
        guid = str((match or {}).get("guid") or "")
        if not guid:
            raise ConnectorRemoteError(f"飞书任务不存在: {candidate}")
        return guid

    @staticmethod
    def _task_time(value: str) -> dict[str, Any]:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        return {"timestamp": str(int(stamp.timestamp() * 1000)), "is_all_day": False}

    async def create_document(self, access_token: str, *, title: str, content: str = "", operation_id: str | None = None) -> dict[str, Any]:
        created = await self.request("POST", "/docx/v1/documents", access_token=access_token, json={"title": title}, operation_id=operation_id)
        data = created.get("data") if isinstance(created.get("data"), dict) else created
        document = data.get("document") if isinstance(data.get("document"), dict) else data
        document_id = str(document.get("document_id") or document.get("token") or "")
        if not document_id:
            raise ConnectorRemoteError("飞书云文档创建成功但没有返回文档 ID")
        if content.strip():
            try:
                await self.append_document(access_token, document_id=document_id, content=content, operation_id=operation_id)
            except Exception as exc:
                raise ConnectorPartialSuccess(
                    f"Feishu document was created (document_id={document_id}) but its initial content was not confirmed: {exc}",
                    {"status": "partial_success", "document_id": document_id, "content_appended": False},
                ) from exc
        return created

    async def append_document(self, access_token: str, *, document_id: str, content: str, operation_id: str | None = None) -> dict[str, Any]:
        block = {"block_type": 2, "text": {"elements": [{"text_run": {"content": content}}]}}
        return await self.request(
            "POST",
            f"/docx/v1/documents/{document_id}/blocks/{document_id}/children",
            access_token=access_token,
            json={"children": [block], "index": -1},
            operation_id=operation_id,
        )

    async def delete_document(self, access_token: str, *, document_id: str, operation_id: str | None = None) -> dict[str, Any]:
        """Move a Feishu cloud document to the user's trash."""
        token = str(document_id).strip()
        if not token:
            raise ValueError("document_id cannot be empty")
        return await self.request(
            "DELETE",
            f"/drive/v1/files/{token}",
            access_token=access_token,
            params={"type": "docx"},
            operation_id=operation_id,
        )

    async def update_document_block(
        self,
        access_token: str,
        *,
        document_id: str,
        block_id: str,
        content: str,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        if not content:
            raise ValueError("document block content cannot be empty")
        payload = {"update_text_elements": {"elements": [{"text_run": {"content": content}}]}}
        return await self.request(
            "PATCH",
            f"/docx/v1/documents/{document_id}/blocks/{block_id}",
            access_token=access_token,
            json=payload,
            operation_id=operation_id,
        )

    async def _calendar_records(
        self, token: str, *, limit: int, page_token: str | None = None, return_page: bool = False
    ) -> list[dict[str, Any]] | dict[str, Any]:
        calendars = await self.request("GET", "/calendar/v4/calendars", access_token=token, params={"page_size": 50})
        data = calendars.get("data") or {}
        items = data.get("calendar_list") or data.get("items") or []
        selected = next((item for item in items if item.get("is_primary") or item.get("type") == "primary"), None) or (items[0] if items else None)
        if not selected or not selected.get("calendar_id"):
            return []
        start = int((datetime.now(UTC) - timedelta(days=30)).timestamp())
        end = int((datetime.now(UTC) + timedelta(days=180)).timestamp())
        event_params: dict[str, Any] = {"start_time": start, "end_time": end, "page_size": min(500, max(1, limit))}
        if page_token:
            event_params["page_token"] = page_token
        events = await self.request(
            "GET",
            f"/calendar/v4/calendars/{selected['calendar_id']}/events",
            access_token=token,
            params=event_params,
        )
        rows = (events.get("data") or {}).get("items") or []
        result = []
        for event in rows[:limit]:
            start_value = (event.get("start_time") or {}).get("timestamp") or event.get("start_time")
            end_value = (event.get("end_time") or {}).get("timestamp") or event.get("end_time")
            result.append(
                {
                    "kind": "event",
                    "source_id": str(event.get("event_id") or event.get("id") or ""),
                    "title": str(event.get("summary") or event.get("summary_i18n") or "飞书日历事件"),
                    "summary": str(event.get("description") or ""),
                    "body": str(event.get("description") or ""),
                    "starts_at": self._timestamp(start_value),
                    "ends_at": self._timestamp(end_value),
                    "location": self._location(event.get("location")),
                    "is_read": True,
                    "is_done": False,
                    "updated_at": now(),
                    "created_at": now(),
                }
            )
        result = [item for item in result if item["source_id"]]
        if return_page:
            data = events.get("data") or {}
            next_cursor = str(data.get("page_token") or data.get("next_page_token") or "") or None
            return {
                "records": result,
                "next_cursor": next_cursor,
                "has_more": bool(data.get("has_more") and next_cursor),
                "complete": not bool(data.get("has_more") and next_cursor),
            }
        return result

    async def _task_records(
        self, token: str, *, limit: int, page_token: str | None = None, return_page: bool = False
    ) -> list[dict[str, Any]] | dict[str, Any]:
        params: dict[str, Any] = {"page_size": min(500, max(1, limit))}
        if page_token:
            params["page_token"] = page_token
        response = await self.request("GET", "/task/v2/tasks", access_token=token, params=params)
        data = response.get("data") or {}
        rows = data.get("items") or data.get("tasks") or []
        result = []
        for task in rows[:limit]:
            status = str(task.get("status") or task.get("state") or "todo").lower()
            result.append(
                {
                    "kind": "todo",
                    "source_id": str(task.get("task_id") or task.get("guid") or task.get("id") or ""),
                    "task_guid": str(task.get("guid") or ""),
                    "title": str(task.get("summary") or task.get("title") or "飞书任务"),
                    "summary": str(task.get("description") or ""),
                    "body": str(task.get("description") or ""),
                    "due_at": self._timestamp(task.get("due") or task.get("due_time") or task.get("due_timestamp")),
                    "priority": str(task.get("priority") or "normal"),
                    "is_read": True,
                    "is_done": status in {"done", "completed", "closed"} or task.get("completed") is True or str(task.get("completed_at") or "0") not in {"0", ""} or task.get("agent_task_status") == 4,
                    "updated_at": self._timestamp(task.get("updated_at") or task.get("update_time")) or now(),
                    "created_at": self._timestamp(task.get("created_at") or task.get("create_time")) or now(),
                }
            )
        result = [item for item in result if item["source_id"]]
        if return_page:
            next_cursor = str(data.get("page_token") or data.get("next_page_token") or "") or None
            return {
                "records": result,
                "next_cursor": next_cursor,
                "has_more": bool(data.get("has_more") and next_cursor),
                "complete": not bool(data.get("has_more") and next_cursor),
            }
        return result

    async def _document_records(
        self, token: str, *, limit: int, page_token: str | None = None, return_page: bool = False
    ) -> list[dict[str, Any]] | dict[str, Any]:
        params: dict[str, Any] = {"page_size": min(200, max(1, limit)), "order_by": "EditedTime", "direction": "DESC"}
        if page_token:
            params["page_token"] = page_token
        response = await self.request(
            "GET",
            "/drive/v1/files",
            access_token=token,
            params=params,
        )
        data = response.get("data") or {}
        rows = data.get("files") or data.get("items") or []
        result = []
        for document in rows[:limit]:
            file_type = str(document.get("type") or document.get("file_type") or "")
            if file_type and file_type not in {"doc", "docx", "sheet", "bitable", "file"}:
                continue
            token_value = str(document.get("token") or document.get("file_token") or document.get("document_id") or "")
            body = str(document.get("description") or "")
            if token_value and file_type in {"doc", "docx"}:
                try:
                    raw = await self.request("GET", f"/docx/v1/documents/{token_value}/raw_content", access_token=token)
                    raw_data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
                    body = str(raw_data.get("content") or raw_data.get("text") or body)
                except ConnectorRemoteError:
                    # Listing remains useful when a document is shared without
                    # raw-content permission; the URL is still retained below.
                    pass
            result.append(
                {
                    "kind": "note",
                    "source_id": token_value,
                    "title": str(document.get("name") or document.get("title") or "飞书云文档"),
                    "summary": body[:240],
                    "body": body[:20_000] or str(document.get("url") or document.get("web_url") or ""),
                    "url": document.get("url") or document.get("web_url"),
                    "is_read": True,
                    "is_done": False,
                    "updated_at": self._timestamp(document.get("modified_time") or document.get("update_time")) or now(),
                    "created_at": self._timestamp(document.get("created_time") or document.get("create_time")) or now(),
                }
            )
        result = [item for item in result if item["source_id"]]
        if return_page:
            next_cursor = str(data.get("page_token") or data.get("next_page_token") or "") or None
            return {
                "records": result,
                "next_cursor": next_cursor,
                "has_more": bool(data.get("has_more") and next_cursor),
                "complete": not bool(data.get("has_more") and next_cursor),
            }
        return result

    @staticmethod
    def _timestamp(value: Any) -> str | None:
        if isinstance(value, dict):
            value = value.get("timestamp") or value.get("date")
        if value is None or value == "":
            return None
        try:
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000
            return datetime.fromtimestamp(number, UTC).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError):
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).isoformat().replace("+00:00", "Z")
                except ValueError:
                    return value
            return None

    @staticmethod
    def _location(value: Any) -> str:
        if isinstance(value, Mapping):
            return str(value.get("name") or value.get("address") or value.get("location") or "")
        return str(value or "")


class EmailAdapter:
    PRESETS: dict[str, dict[str, Any]] = {
        "qq": {"label": "QQ 邮箱", "imap_host": "imap.qq.com", "smtp_host": "smtp.qq.com", "imap_port": 993, "smtp_port": 465},
        "163": {"label": "网易 163 邮箱", "imap_host": "imap.163.com", "smtp_host": "smtp.163.com", "imap_port": 993, "smtp_port": 465},
        "126": {"label": "网易 126 邮箱", "imap_host": "imap.126.com", "smtp_host": "smtp.126.com", "imap_port": 993, "smtp_port": 465},
        "139": {"label": "139 邮箱", "imap_host": "imap.139.com", "smtp_host": "smtp.139.com", "imap_port": 993, "smtp_port": 465},
    }

    def __init__(self, secrets: SecretStore, attachments_dir: Any | None = None):
        self.secrets = secrets
        self.attachments_dir = attachments_dir.resolve() if attachments_dir else None
        if self.attachments_dir:
            self.attachments_dir.mkdir(parents=True, exist_ok=True)

    async def fetch(self, connection: dict[str, Any], *, limit: int = 50) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._fetch_sync, connection, limit, None, False)

    async def fetch_page(
        self,
        connection: dict[str, Any],
        *,
        limit: int = 50,
        cursor: str | None = None,
        mode: str = "new",
    ) -> dict[str, Any]:
        """Fetch one bounded mailbox page.

        ``initial`` selects only messages inside the configured recent-mail
        window. ``new`` continues from the newest UID already imported and
        ``older`` walks backwards from the oldest imported UID when the user
        explicitly asks to load history.
        """
        return await asyncio.to_thread(self._fetch_sync_page, connection, limit, cursor, mode)

    async def find_sent_operation(self, connection: dict[str, Any], operation_id: str) -> dict[str, Any]:
        """Look for a sent message carrying the durable operation header.

        SMTP writes the operation id into ``X-KoreAgent-Operation-ID``.  A
        later IMAP lookup is read-only and is therefore safe to use after a
        timeout: it never sends the message again.  Providers use different
        names for the sent mailbox, so common names are tried and failures in
        one mailbox do not prevent checking the next one.
        """
        return await asyncio.to_thread(self._find_sent_operation_sync, connection, operation_id)

    def _find_sent_operation_sync(self, connection: dict[str, Any], operation_id: str) -> dict[str, Any]:
        password = self.secrets.get(str(connection["secret_ref"]), allow_environment=True)
        if not password:
            raise ConnectorConfigurationError("邮箱授权码不存在")
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id cannot be empty")
        # Windows imaplib defaults to ASCII for string command arguments.  A
        # localized mailbox such as 已发送 therefore raises before the server
        # can answer.  Discover the actual mailbox names with LIST and keep
        # them as bytes so modified UTF-7 is passed through unchanged.
        folders: list[str | bytes] = [
            "Sent",
            "Sent Items",
            "Sent Messages",
            str(connection.get("sent_folder") or ""),
        ]
        seen: set[bytes] = set()
        found: list[dict[str, Any]] = []
        mailbox = imaplib.IMAP4_SSL(str(connection.get("imap_host") or ""), int(connection.get("imap_port") or 993))
        try:
            mailbox.login(str(connection["account_email"]), password)
            try:
                list_status, listed = mailbox.list()
                if list_status == "OK":
                    for raw in listed or []:
                        if isinstance(raw, bytes):
                            mailbox_name = self._imap_list_mailbox(raw)
                            if mailbox_name:
                                folders.append(mailbox_name)
            except (imaplib.IMAP4.error, OSError, UnicodeError):
                pass

            for folder in folders:
                if isinstance(folder, bytes):
                    target = folder.strip()
                else:
                    try:
                        target = folder.strip().encode("ascii")
                    except UnicodeEncodeError:
                        continue
                if not target or target in seen:
                    continue
                seen.add(target)
                try:
                    # imaplib does not quote bytes arguments.  QQ requires
                    # quotes for mailbox names containing spaces.
                    select_target = self._imap_select_argument(target)
                    status, _ = mailbox.select(select_target, readonly=True)
                    if status != "OK":
                        continue
                    status, data = mailbox.uid("search", None, "HEADER", "X-KoreAgent-Operation-ID", operation_id)
                    if status == "OK":
                        # A few domestic IMAP gateways ignore the HEADER
                        # search key and return every UID in the mailbox.  Do
                        # not treat that as proof of delivery: fetch only the
                        # lightweight headers and require an exact operation
                        # id match before confirming the message.
                        candidates = [item for item in (data[0] or b"").split() if item]
                        matches: list[str] = []
                        for uid_value in candidates[-100:]:
                            fetch_status, message_data = mailbox.uid(
                                "fetch", uid_value, "(BODY.PEEK[HEADER.FIELDS (X-KoreAgent-Operation-ID)])"
                            )
                            if fetch_status != "OK":
                                continue
                            raw_header = next(
                                (part[1] for part in message_data if isinstance(part, tuple) and len(part) > 1),
                                None,
                            )
                            if not raw_header:
                                continue
                            parsed = email.message_from_bytes(raw_header, policy=email.policy.default)
                            header_value = str(parsed.get("X-KoreAgent-Operation-ID") or "").strip()
                            if header_value == operation_id:
                                matches.append(uid_value.decode(errors="ignore"))
                        if matches:
                            found.append({"folder": target.decode("ascii", errors="replace"), "message_uids": matches[-20:]})
                except (imaplib.IMAP4.error, OSError, UnicodeError):
                    continue
            return {"found": bool(found), "matches": found}
        finally:
            try:
                mailbox.logout()
            except Exception:
                pass

    @staticmethod
    def _imap_list_mailbox(raw: bytes) -> bytes | None:
        """Extract a mailbox token from one raw IMAP LIST response."""
        value = bytes(raw).strip()
        if not value:
            return None
        match = re.search(rb'(?:^|\s)("(?:[^"\\]|\\.)*"|[^\s]+)\s*$', value)
        if not match:
            return None
        token = match.group(1)
        if len(token) >= 2 and token[:1] == b'"' and token[-1:] == b'"':
            token = token[1:-1].replace(b'\\"', b'"').replace(b'\\\\', b'\\')
        return token or None

    @staticmethod
    def _imap_select_argument(mailbox_name: bytes) -> bytes:
        value = bytes(mailbox_name).strip()
        if len(value) >= 2 and value[:1] == b'"' and value[-1:] == b'"':
            return value
        if any(char in value for char in (b" ", b"(", b")", b"{", b"\\")):
            return b'"' + value.replace(b'\\', b'\\\\').replace(b'"', b'\\"') + b'"'
        return value

    async def idle_wait(self, connection: dict[str, Any], *, timeout: int = 25) -> bool:
        """Wait for an IMAP EXISTS event. A false return means the timeout expired.

        IDLE is deliberately optional: some domestic mailbox gateways disable
        the command, in which case the caller keeps its normal polling loop.
        """
        return await asyncio.to_thread(self._idle_wait_sync, connection, timeout)

    def _idle_wait_sync(self, connection: dict[str, Any], timeout: int) -> bool:
        password = self.secrets.get(str(connection["secret_ref"]), allow_environment=True)
        if not password:
            raise ConnectorConfigurationError("邮箱授权码不存在")
        mailbox = imaplib.IMAP4_SSL(str(connection.get("imap_host") or ""), int(connection.get("imap_port") or 993))
        try:
            mailbox.login(str(connection["account_email"]), password)
            mailbox.select(str(connection.get("folder") or "INBOX"), readonly=True)
            mailbox.send(b"IDLE\r\n")
            response = mailbox.readline()  # server continuation (+ idling)
            if response and not response.startswith(b"+"):
                raise ConnectorRemoteError("IMAP server does not support IDLE")
            mailbox.sock.settimeout(max(1, int(timeout)))
            line = mailbox.readline()
            if line and b"EXISTS" in line.upper():
                mailbox.send(b"DONE\r\n")
                return True
            mailbox.send(b"DONE\r\n")
            return False
        except TimeoutError:
            # A quiet IDLE connection reaching its read timeout is healthy;
            # return control to the loop so it can re-enter IDLE and keep the
            # ordinary polling task as a fallback.
            try:
                mailbox.send(b"DONE\r\n")
            except Exception:
                pass
            return False
        except (imaplib.IMAP4.error, OSError) as exc:
            raise ConnectorRemoteError(f"IMAP IDLE unavailable: {exc}") from exc
        finally:
            try:
                mailbox.logout()
            except Exception:
                pass

    def _fetch_sync(
        self,
        connection: dict[str, Any],
        limit: int,
        cursor: str | None = None,
        incremental: bool = False,
        mode: str = "new",
    ) -> list[dict[str, Any]]:
        password = self.secrets.get(str(connection["secret_ref"]), allow_environment=True)
        if not password:
            raise ConnectorConfigurationError("邮箱授权码不存在，请重新配置邮箱连接器")
        host = str(connection.get("imap_host") or "")
        if not host:
            raise ConnectorConfigurationError("邮箱 IMAP 地址未配置")
        mailbox = imaplib.IMAP4_SSL(host, int(connection.get("imap_port") or 993))
        try:
            mailbox.login(str(connection["account_email"]), password)
            mailbox.select(str(connection.get("folder") or "INBOX"), readonly=True)
            _, unseen_data = mailbox.uid("search", None, "UNSEEN")
            unseen = set((unseen_data[0] or b"").split())
            _, all_data = mailbox.uid("search", None, "ALL")
            all_uids = (all_data[0] or b"").split()
            if mode == "initial":
                sync_days = connection.get("sync_days", 30)
                if sync_days not in (None, "", "unlimited", "无限制"):
                    try:
                        days = max(1, min(int(sync_days), 3650))
                    except (TypeError, ValueError):
                        days = 30
                    cutoff = (datetime.now(UTC) - timedelta(days=days)).strftime("%d-%b-%Y")
                    _, recent_data = mailbox.uid("search", None, "SINCE", cutoff)
                    all_uids = (recent_data[0] or b"").split()
            # IMAP UIDs are monotonically increasing. A cursor is the last
            # successfully processed UID, so a reconnect resumes after it.
            after = int(cursor or 0) if str(cursor or "").isdigit() else 0
            if mode == "older":
                before = int(cursor or max((int(item) for item in all_uids), default=0))
                candidates = [item for item in all_uids if int(item) < before]
            else:
                candidates = [item for item in all_uids if int(item) > after]
            # Normal fetch keeps the newest messages. Incremental pages walk
            # forward from the cursor so a retry cannot skip an intermediate
            # UID.
            page_size = max(1, min(limit, 100))
            if mode == "older":
                uids = candidates[-page_size:]
            elif mode == "initial":
                if str(connection.get("sync_days") or "").lower() in {"unlimited", "infinite", "无限制"}:
                    uids = candidates[:page_size]
                else:
                    # Start at the newest end of the configured window. The
                    # explicit history action will walk backwards from here.
                    uids = candidates[-page_size:]
            elif incremental:
                uids = candidates[:page_size]
            else:
                uids = candidates[-page_size:]
            result = []
            for uid_value in reversed(uids):
                _, message_data = mailbox.uid("fetch", uid_value, "(BODY.PEEK[])")
                raw = next((part[1] for part in message_data if isinstance(part, tuple) and len(part) > 1), None)
                if not raw:
                    continue
                message = email.message_from_bytes(raw, policy=email.policy.default)
                subject = self._decode(str(message.get("Subject") or "(无主题)"))
                sender = self._decode(str(message.get("From") or ""))
                body = self._body(message)
                message_id = str(message.get("Message-ID") or "").strip()
                source_id = message_id or f"imap:{uid_value.decode(errors='ignore')}"
                stamp = email.utils.parsedate_to_datetime(str(message.get("Date") or "")) if message.get("Date") else datetime.now(UTC)
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=UTC)
                result.append(
                    {
                        "kind": "email",
                        "source_id": source_id,
                        "remote_uid": uid_value.decode(errors="ignore"),
                        "title": subject[:240],
                        "summary": body[:240],
                        "body": body[:20_000],
                        "sender": sender,
                        "recipients": [str(message.get("To") or "")],
                        "message_id": message_id,
                        "in_reply_to": str(message.get("In-Reply-To") or "").strip(),
                        "references": str(message.get("References") or "").strip(),
                        "attachments": self._save_attachments(message, source_id),
                        "is_read": uid_value not in unseen,
                        "is_done": False,
                        "sent_at": stamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                        "updated_at": stamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                        "created_at": stamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                    }
                )
            return result
        finally:
            try:
                mailbox.logout()
            except Exception:
                pass

    def _fetch_sync_page(
        self,
        connection: dict[str, Any],
        limit: int,
        cursor: str | None,
        mode: str = "new",
    ) -> dict[str, Any]:
        mode = mode if mode in {"initial", "new", "older"} else "new"
        # Keep the existing parser as the source of truth while collecting the
        # remote UID set for deletion detection. Initial sync is intentionally
        # bounded by a date window; history is only fetched on request.
        records = self._fetch_sync(connection, limit, cursor, True, mode=mode)
        # A lightweight second search gives us the complete remote UID set.
        # It is used only for deletion reconciliation after the final page and
        # does not download message bodies twice.
        remote_uids: list[str] = []
        password = self.secrets.get(str(connection["secret_ref"]), allow_environment=True)
        mailbox = imaplib.IMAP4_SSL(str(connection.get("imap_host") or ""), int(connection.get("imap_port") or 993))
        try:
            mailbox.login(str(connection["account_email"]), password or "")
            mailbox.select(str(connection.get("folder") or "INBOX"), readonly=True)
            _, all_data = mailbox.uid("search", None, "ALL")
            remote_uids = [item.decode(errors="ignore") for item in (all_data[0] or b"").split()]
        finally:
            try:
                mailbox.logout()
            except Exception:
                pass
        numeric_uids = [int(item) for item in remote_uids if str(item).isdigit()]
        if mode == "older":
            before = int(cursor or max(numeric_uids, default=0))
            fetched = [int(record.get("remote_uid")) for record in records if str(record.get("remote_uid") or "").isdigit()]
            next_cursor = min(fetched, default=None)
            remaining = any(item < int(next_cursor or before) for item in numeric_uids)
        elif mode == "initial":
            fetched = [int(record.get("remote_uid")) for record in records if str(record.get("remote_uid") or "").isdigit()]
            next_cursor = max(fetched, default=max(numeric_uids, default=None))
            unlimited = str(connection.get("sync_days") or "").lower() in {"unlimited", "infinite", "无限制"}
            remaining = unlimited and any(item > int(next_cursor or 0) for item in numeric_uids)
        else:
            after = int(cursor or 0)
            fetched = [int(record.get("remote_uid")) for record in records if str(record.get("remote_uid") or "").isdigit()]
            next_cursor = max(fetched, default=None)
            remaining = any(item > int(next_cursor or after) for item in numeric_uids)
        return {
            "records": records,
            "next_cursor": str(next_cursor) if next_cursor is not None else None,
            "has_more": bool(remaining),
            "complete": not remaining,
            # Initial and older pages are bounded views. They must never
            # trigger deletion reconciliation for records outside that page;
            # only a normal forward sync has a complete remote listing.
            "reconcile": mode not in {"initial", "older"},
            "mode": mode,
            "remote_uids": remote_uids,
        }

    async def send(
        self,
        connection: dict[str, Any],
        *,
        to: str | list[str],
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        html_body: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        in_reply_to: str = "",
        references: str = "",
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._send_sync,
            connection,
            to,
            subject,
            body,
            cc,
            bcc,
            html_body,
            attachments or [],
            in_reply_to,
            references,
            operation_id,
        )

    def _send_sync(
        self,
        connection: dict[str, Any],
        to: str | list[str],
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        html_body: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        in_reply_to: str = "",
        references: str = "",
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        password = self.secrets.get(str(connection["secret_ref"]), allow_environment=True)
        if not password:
            raise ConnectorConfigurationError("邮箱授权码不存在")
        message = EmailMessage(policy=email.policy.default)
        message["From"] = str(connection["account_email"])
        recipients = self._addresses(to)
        copies = self._addresses(cc)
        blind_copies = self._addresses(bcc)
        if not recipients:
            raise ConnectorConfigurationError("at least one recipient is required")
        message["To"] = ", ".join(recipients)
        if copies:
            message["Cc"] = ", ".join(copies)
        if in_reply_to:
            message["In-Reply-To"] = in_reply_to.strip()
        if references:
            message["References"] = references.strip()
        message["Subject"] = subject
        message.set_content(body)
        if html_body:
            message.add_alternative(html_body, subtype="html")
        total_attachment_bytes = 0
        attachments = attachments or []
        if len(attachments) > 10:
            raise ConnectorConfigurationError("at most 10 attachments may be sent at once")
        for item in attachments:
            filename = str(item.get("filename") or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
            if not filename or filename in {".", ".."}:
                raise ConnectorConfigurationError("every attachment needs a filename")
            try:
                content = base64.b64decode(str(item.get("content_base64") or ""), validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ConnectorConfigurationError(f"attachment {filename!r} is not valid base64") from exc
            total_attachment_bytes += len(content)
            if total_attachment_bytes > 20 * 1024 * 1024:
                raise ConnectorConfigurationError("attachments may not exceed 20 MiB in total")
            maintype, subtype = str(item.get("content_type") or "application/octet-stream").split("/", 1)
            if not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+", maintype) or not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+", subtype):
                raise ConnectorConfigurationError(f"attachment {filename!r} has an invalid content type")
            message.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)
        if operation_id:
            message["X-KoreAgent-Operation-ID"] = operation_id
            safe_message_id = re.sub(r"[^A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]", "-", operation_id)
            message["Message-ID"] = f"<{safe_message_id}@koreagent.local>"
        else:
            message["Message-ID"] = email.utils.make_msgid()
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(str(connection["smtp_host"]), int(connection.get("smtp_port") or 465), context=context) as server:
            server.login(str(connection["account_email"]), password)
            server.send_message(message, to_addrs=[*recipients, *copies, *blind_copies])
        return {
            "status": "sent",
            "from": connection["account_email"],
            "to": recipients,
            "cc": copies,
            "subject": subject,
            "message_id": str(message["Message-ID"]),
            "attachment_count": len(attachments),
        }

    @staticmethod
    def _addresses(value: str | list[str]) -> list[str]:
        raw = value.split(",") if isinstance(value, str) else value
        return [item.strip() for item in raw if item and item.strip()]

    def _save_attachments(self, message: Message, source_id: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for index, part in enumerate(message.walk()):
            filename = part.get_filename()
            if part.get_content_disposition() != "attachment" or not filename:
                continue
            safe_name = os.path.basename(self._decode(str(filename))).strip() or "attachment"
            attachment_id = hashlib.sha256(f"{source_id}\0{index}\0{safe_name}".encode("utf-8", errors="replace")).hexdigest()[:32]
            content = part.get_payload(decode=True) or b""
            metadata: dict[str, Any] = {
                "attachment_id": attachment_id,
                "filename": safe_name[:255],
                "content_type": part.get_content_type(),
                "size_bytes": len(content),
                "download_available": False,
            }
            if self.attachments_dir and len(content) <= 50 * 1024 * 1024:
                path = (self.attachments_dir / attachment_id).resolve()
                try:
                    path.relative_to(self.attachments_dir.resolve())
                    path.write_bytes(content)
                    metadata["download_available"] = True
                except (OSError, ValueError):
                    pass
            result.append(metadata)
        return result

    @classmethod
    def preset(cls, provider: str) -> dict[str, Any]:
        if provider in cls.PRESETS:
            return dict(cls.PRESETS[provider])
        raise ConnectorConfigurationError("不支持的邮箱预设，请使用 qq、163、126、139 或 custom")

    @staticmethod
    def _decode(value: str) -> str:
        parts = []
        for fragment, encoding in decode_header(value):
            if isinstance(fragment, bytes):
                parts.append(fragment.decode(encoding or "utf-8", errors="replace"))
            else:
                parts.append(fragment)
        return "".join(parts)

    @classmethod
    def _body(cls, message: Message) -> str:
        chunks: list[str] = []
        for part in message.walk() if message.is_multipart() else [message]:
            if part.get_content_maintype() == "multipart" or part.get("Content-Disposition", "").startswith("attachment"):
                continue
            content_type = part.get_content_type()
            if content_type not in {"text/plain", "text/html"}:
                continue
            try:
                content = part.get_content()
            except Exception:
                payload = part.get_payload(decode=True) or b""
                content = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            # Do not render arbitrary HTML in the desktop inbox. Plain text is
            # safer and keeps long messages within the card layout.
            if content_type == "text/html":
                content = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", str(content))
                content = re.sub(r"(?is)<br\s*/?>", "\n", content)
                content = re.sub(r"(?is)</p\s*>|</div\s*>", "\n", content)
                content = re.sub(r"(?is)<[^>]+>", " ", content)
                content = html.unescape(content)
            chunks.append(str(content))
            if content_type == "text/plain":
                break
        return "\n".join(chunks).strip()
