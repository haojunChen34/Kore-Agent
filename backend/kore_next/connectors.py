from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from .database import Database
from .real_connectors import ConnectorConfigurationError, ConnectorRemoteError, EmailAdapter, FeishuClient
from .relations import ContextRelationService
from .secrets import SecretStore
from .security import hash_token, new_download_token, redact, redact_text
from .util import dumps, loads, now, uid

CONNECTOR_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "provider": "calendar",
        "label": "日历",
        "description": "同步会议、安排和时间块。",
        "item_kind": "event",
        "icon": "calendar",
    },
    {
        "provider": "email",
        "label": "邮件",
        "description": "同步收件箱邮件和未读状态。",
        "item_kind": "email",
        "icon": "mail",
    },
    {
        "provider": "todo",
        "label": "待办",
        "description": "同步待处理任务和截止时间。",
        "item_kind": "todo",
        "icon": "todo",
    },
    {
        "provider": "notes",
        "label": "笔记",
        "description": "同步近期笔记和知识片段。",
        "item_kind": "note",
        "icon": "note",
    },
)
PROVIDERS = {item["provider"]: item for item in CONNECTOR_DEFINITIONS}


def _parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class ConnectorService:
    """Local-first connector hub backed by real provider adapters.

    Feishu OAuth connects calendar, tasks and cloud documents. QQ/163-style
    mailboxes use IMAP/SMTP with an app-specific authorization code.
    """

    def __init__(self, db: Database, secrets: SecretStore | None = None, attachments_dir: Any | None = None):
        self.db = db
        self.secrets = secrets
        self.feishu = FeishuClient.from_environment(secrets) if secrets else None
        self.email = EmailAdapter(secrets, attachments_dir=attachments_dir) if secrets else None
        self.relations = ContextRelationService()
        self._sync_lock = asyncio.Lock()

    def definitions(self) -> list[dict[str, Any]]:
        return [
            dict(
                item,
                oauth={"type": "oauth2", "mode": "feishu" if item["provider"] in {"calendar", "todo", "notes"} else "imap_smtp"},
                real_available=(self.feishu.config.enabled if item["provider"] in {"calendar", "todo", "notes"} else self.email is not None),
                real_setup_hint=(
                    "请在 backend/.env 填写飞书 App ID、App Secret 和回调地址"
                    if item["provider"] in {"calendar", "todo", "notes"} and not (self.feishu and self.feishu.config.enabled)
                    else ""
                ),
                real_adapters={
                    "calendar": ["feishu_calendar"],
                    "todo": ["feishu_task"],
                    "notes": ["feishu_docs"],
                    "email": ["imap_smtp"],
                }[item["provider"]],
            )
            for item in CONNECTOR_DEFINITIONS
        ]

    def _validate_provider(self, provider: str) -> str:
        provider = provider.strip().lower()
        if provider not in PROVIDERS:
            raise ValueError(f"unsupported connector: {provider}")
        return provider

    async def list_connections(self) -> list[dict[str, Any]]:
        rows = await self.db.fetchall("SELECT data_json FROM connector_connections ORDER BY updated_at DESC")
        result = []
        for row in rows:
            item = loads(row["data_json"], {})
            item["health"] = self._health(item)
            result.append(item)
        return result

    @staticmethod
    def _health(connection: dict[str, Any]) -> dict[str, Any]:
        metadata = connection.get("auth_metadata") or {}
        expires_at = _parse_time(str(metadata.get("expires_at") or ""))
        now_value = datetime.now(UTC)
        expiring = bool(metadata.get("expires_at")) and expires_at <= now_value + timedelta(minutes=15)
        expired = bool(metadata.get("expires_at")) and expires_at <= now_value
        error = connection.get("last_sync_error")
        status = str(connection.get("status") or "disconnected")
        reauthorize = (
            bool(connection.get("reauthorize_required")) or expired or str(connection.get("oauth_status")) in {"expired", "reauthorize_required"}
        )
        if status == "connected" and reauthorize:
            health_status = "reauthorize_required"
        elif status == "connected" and error:
            health_status = "degraded"
        elif status == "connected":
            health_status = "healthy"
        else:
            health_status = "disconnected"
        return {
            "status": health_status,
            "connected": status == "connected" and not reauthorize,
            "last_successful_sync": connection.get("last_sync_at"),
            "last_error": error,
            "last_error_at": connection.get("last_sync_error_at"),
            "token_expiring_soon": expiring and not expired,
            "token_expired": expired,
            "reauthorize_required": reauthorize,
            "retry_count": int(connection.get("sync_retry_count") or 0),
            "next_retry_at": connection.get("next_retry_at"),
            "cursor": connection.get("sync_cursor"),
            "imap_idle_enabled": bool(connection.get("imap_idle_enabled")),
            "imap_idle_last_at": connection.get("imap_idle_last_at"),
            "webhook_enabled": bool(connection.get("webhook_enabled")),
            "last_webhook_at": connection.get("last_webhook_at"),
        }

    async def get_connection(self, provider: str) -> dict[str, Any] | None:
        provider = self._validate_provider(provider)
        row = await self.db.fetchone("SELECT data_json FROM connector_connections WHERE provider=?", (provider,))
        return loads(row["data_json"], {}) if row else None

    async def begin_oauth(self, provider: str, *, redirect_uri: str | None = None) -> dict[str, Any]:
        provider = provider.strip().lower()
        bundle = provider == "feishu"
        if not bundle:
            provider = self._validate_provider(provider)
        if provider == "email":
            raise ConnectorConfigurationError("Email accounts use IMAP/SMTP app passwords; configure the mailbox directly.")
        if not self.feishu or not self.feishu.config.enabled:
            raise ConnectorConfigurationError(
                "Feishu is not configured. Set KORENEXT_FEISHU_APP_ID, KORENEXT_FEISHU_APP_SECRET and KORENEXT_FEISHU_REDIRECT_URI in backend/.env."
            )

        state = uid()
        stamp = now()
        expires_at = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
        await self.db.execute(
            "INSERT INTO oauth_states(state,provider,redirect_uri,expires_at,used_at,created_at) VALUES(?,?,?,?,?,?)",
            (state, provider, redirect_uri, expires_at, None, stamp),
        )
        callback = redirect_uri or self.feishu.config.redirect_uri
        if bundle:
            return {
                "state": state,
                "provider": provider,
                "authorization_url": self.feishu.authorize_url(state, provider="all", redirect_uri=callback),
                "redirect_uri": callback,
                "expires_at": expires_at,
                "mode": "real",
                "adapter": "feishu_bundle",
                "providers": ["calendar", "todo", "notes"],
            }
        return {
            "state": state,
            "provider": provider,
            "authorization_url": self.feishu.authorize_url(state, provider=provider, redirect_uri=callback),
            "redirect_uri": callback,
            "expires_at": expires_at,
            "mode": "real",
            "adapter": {"calendar": "feishu_calendar", "todo": "feishu_task", "notes": "feishu_docs"}[provider],
        }

    async def complete_oauth(self, provider: str, state: str, code: str) -> dict[str, Any]:
        provider = provider.strip().lower()
        bundle = provider == "feishu"
        if not bundle:
            provider = self._validate_provider(provider)
        if provider == "email":
            raise ConnectorConfigurationError("Email accounts use IMAP/SMTP app passwords, not OAuth.")
        if not code.strip():
            raise ValueError("oauth code is required")
        row = await self.db.fetchone("SELECT * FROM oauth_states WHERE state=?", (state,))
        if not row or row["provider"] != provider:
            raise ValueError("oauth state is invalid")
        if row["used_at"]:
            raise ValueError("oauth state has already been used")
        if _parse_time(row["expires_at"]) < datetime.now(UTC):
            raise ValueError("oauth state has expired")
        if not self.feishu or not self.feishu.config.enabled:
            raise ConnectorConfigurationError("Feishu OAuth is not configured.")

        target_provider = "calendar" if bundle else provider
        token = await self.feishu.exchange_code(code, redirect_uri=row["redirect_uri"] or self.feishu.config.redirect_uri)
        user = await self.feishu.user_info(str(token["access_token"]))
        secret_prefix = "connector.feishu"
        existing_refresh_token = self.secrets.get(f"{secret_prefix}.refresh_token") if self.secrets else ""
        refresh_token = str(token.get("refresh_token") or existing_refresh_token).strip()
        granted_scopes = {item for item in str(token.get("scope") or "").split() if item}
        # Feishu only returns refresh_token fields when offline_access was
        # granted. Do not silently create a connection that will expire in two
        # hours when the user expected background synchronization.
        if not refresh_token or (granted_scopes and "offline_access" not in granted_scopes):
            raise ConnectorConfigurationError(
                "飞书授权未包含 offline_access，无法自动续期。请在飞书应用权限管理中开通 offline_access，发布应用后重新授权。"
            )
        if self.secrets:
            self.secrets.set(f"{secret_prefix}.access_token", str(token["access_token"]))
            self.secrets.set(f"{secret_prefix}.refresh_token", refresh_token)
        user_data = user.get("data") if isinstance(user.get("data"), dict) else user
        connection_data = {
            "display_name": str(user_data.get("name") or "Feishu account"),
            "account_email": str(user_data.get("email") or user_data.get("en_name") or ""),
            "auth_type": "oauth2",
            "auth_metadata": {
                "expires_at": (datetime.now(UTC) + timedelta(seconds=int(token.get("expires_in") or 7200))).isoformat(),
                "refresh_expires_at": (
                    datetime.now(UTC) + timedelta(seconds=int(token["refresh_token_expires_in"]))
                ).isoformat()
                if token.get("refresh_token_expires_in")
                else None,
                "scope": str(token.get("scope") or ""),
                "refresh_token_configured": True,
                "secret_prefix": secret_prefix,
            },
        }
        adapters = {"calendar": "feishu_calendar", "todo": "feishu_task", "notes": "feishu_docs"}
        connections = [await self.connect(provider_name, adapter=adapter, **connection_data) for provider_name, adapter in adapters.items()]
        await self.db.execute("UPDATE oauth_states SET used_at=? WHERE state=?", (now(), state))
        return {
            "connection": next(item for item in connections if item["provider"] == target_provider),
            "connections": connections,
            "code_type": "feishu",
            "mode": "real",
        }

    async def remove_legacy_demo_data(self) -> dict[str, int]:
        """Remove data created by the retired local demo connector."""
        connections = await self.db.fetchall("SELECT id,data_json FROM connector_connections")
        legacy_connection_ids = {str(row["id"]) for row in connections if loads(row["data_json"], {}).get("adapter") == "demo"}
        items = await self.db.fetchall("SELECT id,connection_id,source_id FROM connector_items")
        legacy_item_ids = {
            str(row["id"]) for row in items if str(row["connection_id"]) in legacy_connection_ids or str(row["source_id"]).startswith("demo-")
        }
        async with self.db.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            for connection_id in legacy_connection_ids:
                await db.execute("DELETE FROM sync_events WHERE connection_id=?", (connection_id,))
                await db.execute("DELETE FROM connector_items WHERE connection_id=?", (connection_id,))
                await db.execute("DELETE FROM connector_connections WHERE id=?", (connection_id,))
            if legacy_item_ids:
                placeholders = ",".join("?" for _ in legacy_item_ids)
                item_ids = tuple(legacy_item_ids)
                await db.execute(f"DELETE FROM sync_events WHERE item_id IN ({placeholders})", item_ids)
                await db.execute(f"DELETE FROM connector_items WHERE id IN ({placeholders})", item_ids)
            cursor = await db.execute("DELETE FROM oauth_states WHERE mode='demo'")
            removed_states = cursor.rowcount
            await db.commit()
        return {
            "connections": len(legacy_connection_ids),
            "items": len(legacy_item_ids),
            "oauth_states": int(removed_states),
        }

    async def connect(
        self,
        provider: str,
        *,
        display_name: str | None = None,
        account_email: str | None = None,
        auth_type: str = "oauth2",
        adapter: str,
        auth_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        provider = self._validate_provider(provider)
        stamp = now()
        existing = await self.get_connection(provider)
        connection = {
            "id": existing.get("id") if existing else uid(),
            "provider": provider,
            "label": PROVIDERS[provider]["label"],
            "display_name": display_name or (existing or {}).get("display_name") or f"本地 {PROVIDERS[provider]['label']}",
            "account_email": account_email or (existing or {}).get("account_email") or "",
            "auth_type": auth_type,
            "adapter": adapter,
            "auth_metadata": auth_metadata or (existing or {}).get("auth_metadata") or {},
            "oauth_status": "authorized",
            "status": "connected",
            "last_sync_at": (existing or {}).get("last_sync_at"),
            "sync_cursor": (existing or {}).get("sync_cursor"),
            "sync_oldest_uid": (existing or {}).get("sync_oldest_uid"),
            "sync_days": (existing or {}).get("sync_days") or 30,
            "sync_initialized": bool((existing or {}).get("sync_initialized", False)),
            "sync_scan_seen": (existing or {}).get("sync_scan_seen", []),
            "sync_retry_count": 0,
            "next_retry_at": None,
            "reauthorize_required": False,
            "created_at": (existing or {}).get("created_at") or stamp,
            "updated_at": stamp,
        }
        await self.db.execute(
            """INSERT INTO connector_connections(id,provider,data_json,status,created_at,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(provider) DO UPDATE SET data_json=excluded.data_json,status=excluded.status,updated_at=excluded.updated_at""",
            (connection["id"], provider, dumps(connection), "connected", connection["created_at"], stamp),
        )
        return connection

    async def configure_email(
        self,
        provider: str,
        account_email: str,
        app_password: str,
        *,
        imap_host: str | None = None,
        smtp_host: str | None = None,
        imap_port: int | None = None,
        smtp_port: int | None = None,
        display_name: str | None = None,
        sync_days: int | str | None = 30,
    ) -> dict[str, Any]:
        if not self.secrets or not self.email:
            raise ConnectorConfigurationError("密钥存储不可用")
        preset = self.email.preset(provider) if provider != "custom" else {}
        if not account_email.strip() or not app_password.strip():
            raise ConnectorConfigurationError("邮箱地址和授权码不能为空")
        secret_ref = f"connector.email.{provider}.{account_email.strip().lower()}.app_password"
        self.secrets.set(secret_ref, app_password.strip())
        connection = await self.connect(
            "email",
            display_name=display_name or preset.get("label") or account_email,
            account_email=account_email.strip(),
            auth_type="app_password",
            adapter="imap_smtp",
            auth_metadata={"preset": provider, "secret_ref": secret_ref},
        )
        connection.update(
            {
                "imap_host": imap_host or preset.get("imap_host"),
                "smtp_host": smtp_host or preset.get("smtp_host"),
                "imap_port": int(imap_port or preset.get("imap_port") or 993),
                "smtp_port": int(smtp_port or preset.get("smtp_port") or 465),
                "secret_ref": secret_ref,
                "adapter": "imap_smtp",
                "sync_days": self._normalize_sync_days(sync_days),
                # The first sync starts from the configured recent window.
                # Older messages are loaded only after an explicit request.
                "sync_oldest_uid": None,
                "sync_cursor": None,
                "sync_initialized": False,
            }
        )
        await self.db.execute(
            "UPDATE connector_connections SET data_json=?,updated_at=? WHERE provider='email'",
            (dumps(connection), now()),
        )
        return connection

    @staticmethod
    def _normalize_sync_days(value: int | str | None) -> int | str:
        if value is None or str(value).strip().lower() in {"unlimited", "infinite", "无限制"}:
            return "unlimited"
        try:
            days = int(value)
        except (TypeError, ValueError):
            return 30
        return max(7, min(days, 3650))

    async def disconnect(self, provider: str) -> dict[str, Any]:
        provider = self._validate_provider(provider)
        connection = await self.get_connection(provider)
        if not connection:
            return {"provider": provider, "status": "disconnected"}
        stamp = now()
        connection.update({"status": "disconnected", "oauth_status": "revoked", "updated_at": stamp})
        await self.db.execute(
            "UPDATE connector_connections SET data_json=?,status=?,updated_at=? WHERE provider=?",
            (dumps(connection), "disconnected", stamp, provider),
        )
        await self.db.execute("DELETE FROM connector_items WHERE connection_id=?", (connection["id"],))
        return connection

    async def revoke_all(self) -> dict[str, Any]:
        """Revoke every local connector session and remove provider tokens."""
        connections = await self.list_connections()
        stamp = now()
        for connection in connections:
            connection.update({"status": "disconnected", "oauth_status": "revoked", "reauthorize_required": False, "updated_at": stamp})
            await self.db.execute(
                "UPDATE connector_connections SET data_json=?,status='disconnected',updated_at=? WHERE id=?",
                (dumps(connection), stamp, connection["id"]),
            )
            secret_ref = str(connection.get("secret_ref") or (connection.get("auth_metadata") or {}).get("secret_ref") or "")
            if secret_ref and self.secrets:
                self.secrets.delete(secret_ref)
        await self.db.execute("DELETE FROM connector_items")
        await self.db.execute("DELETE FROM sync_events")
        await self.db.execute("DELETE FROM oauth_states")
        if self.secrets:
            self.secrets.delete("connector.feishu.access_token")
            self.secrets.delete("connector.feishu.refresh_token")
        return {"revoked": len(connections), "providers": [item.get("provider") for item in connections]}

    async def sync(self, provider: str | None = None, *, mode: str = "new") -> dict[str, Any]:
        async with self._sync_lock:
            return await self._sync_locked(provider, mode=mode)

    async def _sync_locked(self, provider: str | None = None, *, mode: str = "new") -> dict[str, Any]:
        await self.cleanup_expired_attachments()
        connections = await self.list_connections()
        connected = [item for item in connections if item.get("status") == "connected"]
        if provider:
            target = self._validate_provider(provider)
            connected = [item for item in connected if item.get("provider") == target]
        summaries: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        new_events: list[dict[str, Any]] = []
        for connection in connected:
            try:
                if connection.get("provider") == "email" and not connection.get("sync_initialized") and mode == "new":
                    connection_mode = "initial"
                else:
                    connection_mode = mode
                summary = await self._sync_connection_with_retry(connection, mode=connection_mode)
            except Exception as exc:
                error = redact_text(str(exc), max_chars=500)
                stamp = now()
                is_auth = any(term in error.lower() for term in ("token", "授权", "access", "credential", "permission", "权限"))
                connection = {
                    **connection,
                    "last_sync_error": error,
                    "last_sync_error_at": stamp,
                    "updated_at": stamp,
                    "reauthorize_required": is_auth,
                    "oauth_status": "reauthorize_required" if is_auth else connection.get("oauth_status"),
                }
                await self.db.execute(
                    "UPDATE connector_connections SET data_json=?,updated_at=? WHERE id=?",
                    (dumps(connection), stamp, connection["id"]),
                )
                await self.db.record_error(
                    source="connector_sync",
                    provider=str(connection.get("provider") or ""),
                    category="sync",
                    message=error,
                    severity="error" if is_auth else "warning",
                    details={"connection_id": connection.get("id")},
                )
                failure = {
                    "provider": connection["provider"],
                    "label": PROVIDERS[str(connection["provider"])]["label"],
                    "connection_id": connection["id"],
                    "status": "failed",
                    "error": error,
                    "synced_at": stamp,
                }
                summaries.append(failure)
                errors.append(failure)
                continue
            new_events.extend(summary.pop("events", []) or [])
            summaries.append(summary)
        links = await self.relations.rebuild(self.db)
        return {
            "synced_at": now(),
            "providers": summaries,
            "errors": errors,
            "events": new_events,
            "total_created": sum(int(item.get("created", 0)) for item in summaries),
            "total_updated": sum(int(item.get("updated", 0)) for item in summaries),
            "total_deleted": sum(int(item.get("deleted", 0)) for item in summaries),
            "total_items": sum(int(item.get("items", 0)) for item in summaries),
            "context_links": links.get("links", 0),
        }

    async def _sync_connection_with_retry(self, connection: dict[str, Any], *, mode: str = "new") -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                result = await self._sync_connection(connection, mode=mode)
                return result
            except Exception as exc:
                last_error = exc
                if attempt >= 3:
                    break
                delay = min(30.0, 0.25 * (2 ** (attempt - 1)))
                stamp = now()
                connection["sync_retry_count"] = attempt
                connection["next_retry_at"] = (datetime.now(UTC) + timedelta(seconds=delay)).isoformat()
                connection["last_sync_error"] = str(exc)[:500]
                connection["last_sync_error_at"] = stamp
                await self.db.execute(
                    "UPDATE connector_connections SET data_json=?,updated_at=? WHERE id=?", (dumps(connection), stamp, connection["id"])
                )
                await asyncio.sleep(delay)
        assert last_error is not None
        raise last_error

    async def _sync_connection(self, connection: dict[str, Any], *, mode: str = "new") -> dict[str, Any]:
        provider = str(connection["provider"])
        adapter = str(connection.get("adapter") or "")
        if adapter.startswith("feishu_"):
            if not self.feishu or not self.secrets:
                raise ConnectorConfigurationError("飞书真实连接器未配置，无法同步已有真实连接")
            if getattr(self._real_feishu_records, "__self__", None) is self and self.feishu and hasattr(self.feishu, "records_page"):
                page = await self._real_feishu_records_page(provider, connection)
            else:
                page = {"records": await self._real_feishu_records(provider, connection), "next_cursor": None, "has_more": False, "complete": True}
        elif adapter == "imap_smtp":
            if not self.email:
                raise ConnectorConfigurationError("邮箱真实连接器未配置，无法同步已有真实连接")
            if getattr(self.email.fetch, "__self__", None) is self.email and hasattr(self.email, "fetch_page"):
                cursor = connection.get("sync_oldest_uid") if mode == "older" else connection.get("sync_cursor")
                page = await self.email.fetch_page(connection, limit=100, cursor=cursor, mode=mode)
            else:
                page = {"records": await self.email.fetch(connection, limit=100), "next_cursor": None, "has_more": False, "complete": True}
        else:
            raise ConnectorConfigurationError(f"未知连接器适配器：{adapter}")
        records = list(page.get("records") or [])
        created = updated = deleted = 0
        events: list[dict[str, Any]] = []
        for record in records:
            record["provider"] = provider
            if provider == "email" and mode == "older":
                # Explicitly loaded history may be surfaced by the workbench
                # even when it falls outside the initial window.
                record["sync_loaded_historic"] = True
            source_id = str(record["source_id"])
            existing = await self.db.fetchone(
                "SELECT id,data_json,status FROM connector_items WHERE connection_id=? AND source_id=?",
                (connection["id"], source_id),
            )
            if existing:
                # Keep a local tombstone when a provider listing is briefly
                # stale after a successful delete. Re-activating it here would
                # make the inbox show an object that the remote service has
                # already removed.
                if str(existing["status"] or "") == "deleted":
                    continue
                old = loads(existing["data_json"], {})
                record = {
                    **record,
                    "id": existing["id"],
                    "is_read": old.get("is_read", record.get("is_read", False)),
                    "is_done": record.get("is_done", old.get("is_done", False)) if provider == "todo" else old.get("is_done", record.get("is_done", False)),
                    "sync_loaded_historic": bool(old.get("sync_loaded_historic")) or bool(record.get("sync_loaded_historic")),
                    "created_at": old.get("created_at", record["updated_at"]),
                }
                changed = self._record_changed(old, record)
                if not changed:
                    continue
                await self.db.execute(
                    "UPDATE connector_items SET data_json=?,status=?,updated_at=? WHERE id=?",
                    (dumps(record), "active", record["updated_at"], record["id"]),
                )
                event_type = "updated"
                updated += 1
            else:
                record["id"] = uid()
                await self.db.execute(
                    "INSERT INTO connector_items(id,connection_id,provider,source_id,kind,data_json,status,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (record["id"], connection["id"], provider, source_id, record["kind"], dumps(record), "active", record["updated_at"]),
                )
                event_type = "created"
                created += 1
            event = {
                "id": uid(),
                "provider": provider,
                "connection_id": connection["id"],
                "event_type": event_type,
                "item_id": record["id"],
                "source_id": source_id,
                "occurred_at": now(),
                "item": record,
            }
            await self.db.execute(
                "INSERT INTO sync_events(id,connection_id,provider,event_type,item_id,data_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (event["id"], connection["id"], provider, event_type, record["id"], dumps(redact(event)), event["occurred_at"]),
            )
            events.append(event)
        # Keep a set across pages. Once the provider says the scan is complete,
        # anything absent from that scan is a remote deletion.
        seen = set(str(item) for item in (connection.get("sync_scan_seen") or []))
        for record in records:
            if record.get("source_id"):
                seen.add(str(record["source_id"]))
            if record.get("remote_uid"):
                seen.add(str(record["remote_uid"]))
        if page.get("remote_uids"):
            page_seen = {str(item) for item in page.get("remote_uids") or []}
            seen.update(page_seen)
        if page.get("complete") and page.get("reconcile", True):
            rows = await self.db.fetchall(
                "SELECT id,source_id,data_json FROM connector_items WHERE connection_id=? AND status='active'", (connection["id"],)
            )
            # For a page based sync, accumulated remote IDs represent the
            # complete remote scan. Empty scans are valid and remove all stale
            # records.
            current_ids = seen
            for row in rows:
                old = loads(row["data_json"], {})
                remote_id = str(old.get("remote_uid") or row["source_id"])
                if remote_id in current_ids:
                    continue
                old["status"] = "deleted"
                old["deleted_at"] = now()
                await self.db.execute(
                    "UPDATE connector_items SET data_json=?,status='deleted',updated_at=? WHERE id=?", (dumps(old), old["deleted_at"], row["id"])
                )
                event = {
                    "id": uid(),
                    "provider": provider,
                    "connection_id": connection["id"],
                    "event_type": "deleted",
                    "item_id": row["id"],
                    "source_id": row["source_id"],
                    "occurred_at": old["deleted_at"],
                    "item": old,
                }
                await self.db.execute(
                    "INSERT INTO sync_events(id,connection_id,provider,event_type,item_id,data_json,created_at) VALUES(?,?,?,?,?,?,?)",
                    (event["id"], connection["id"], provider, "deleted", row["id"], dumps(redact(event)), event["occurred_at"]),
                )
                events.append(event)
                deleted += 1
            connection["sync_scan_seen"] = []
        else:
            connection["sync_scan_seen"] = sorted(seen)
        stamp = now()
        if provider == "email" and records:
            uids = [int(item.get("remote_uid")) for item in records if str(item.get("remote_uid") or "").isdigit()]
            if uids and mode in {"initial", "older"}:
                current_oldest = int(connection.get("sync_oldest_uid") or max(uids))
                connection["sync_oldest_uid"] = str(min(current_oldest, min(uids)))
        next_cursor = page.get("next_cursor")
        if provider == "email":
            if mode == "initial":
                # The initial window is complete, but keep the mailbox head as
                # the forward cursor so future polls do not reopen history.
                next_cursor = page.get("next_cursor") or connection.get("sync_cursor")
            elif mode == "older":
                next_cursor = connection.get("sync_cursor")
            elif mode == "new":
                next_cursor = page.get("next_cursor") or connection.get("sync_cursor")
        connection.update(
            {
                "last_sync_at": stamp,
                "updated_at": stamp,
                "last_sync_items": len(records),
                "last_sync_error": None,
                "last_sync_error_at": None,
                "sync_retry_count": 0,
                "next_retry_at": None,
                "reauthorize_required": False,
                "sync_cursor": next_cursor if provider == "email" else (None if page.get("complete") else next_cursor),
                "sync_complete": bool(page.get("complete")),
                "sync_initialized": True,
            }
        )
        await self.db.execute(
            "UPDATE connector_connections SET data_json=?,updated_at=? WHERE id=?",
            (dumps(connection), stamp, connection["id"]),
        )
        # A successful full sync is the source of truth for connector health.
        # Move matching open errors into history so the health panel does not
        # continue showing a stale failure after recovery.
        await self.db.recover_errors(source="connector_sync", provider=provider, category="sync")
        return {
            "provider": provider,
            "label": PROVIDERS[provider]["label"],
            "connection_id": connection["id"],
            "created": created,
            "updated": updated,
            "deleted": deleted,
            "items": len(records),
            "synced_at": stamp,
            "cursor": connection.get("sync_cursor"),
            "complete": bool(page.get("complete")),
            "events": events,
        }

    @staticmethod
    def _record_changed(old: dict[str, Any], new: dict[str, Any]) -> bool:
        # Provider adapters may refresh a remote item's modified timestamp on
        # every poll.  Do not turn that into a local update/event unless the
        # meaningful payload changed.
        ignored = {"id", "created_at", "updated_at", "is_read"}
        if new.get("provider") != "todo":
            ignored.add("is_done")
        return {key: value for key, value in old.items() if key not in ignored} != {key: value for key, value in new.items() if key not in ignored}

    async def _real_feishu_records(self, provider: str, connection: dict[str, Any]) -> list[dict[str, Any]]:
        access_token = await self._feishu_access_token(connection)
        return await self.feishu.records(provider, access_token, limit=100)

    async def _real_feishu_records_page(self, provider: str, connection: dict[str, Any]) -> dict[str, Any]:
        access_token = await self._feishu_access_token(connection)
        return await self.feishu.records_page(provider, access_token, limit=100, cursor=connection.get("sync_cursor"))

    async def _feishu_access_token(self, connection: dict[str, Any] | None = None) -> str:
        if not self.feishu or not self.secrets:
            raise ConnectorConfigurationError("飞书真实连接器未配置")
        connection = connection or next(
            (item for item in await self.list_connections() if item.get("adapter", "").startswith("feishu_") and item.get("status") == "connected"),
            None,
        )
        if not connection:
            raise ConnectorConfigurationError("请先完成飞书 OAuth 授权")
        metadata = connection.get("auth_metadata") or {}
        prefix = str(metadata.get("secret_prefix") or "connector.feishu")
        access_token = self.secrets.get(f"{prefix}.access_token")
        if not access_token:
            raise ConnectorConfigurationError("飞书 access token 不存在，请重新授权")
        expires_at = _parse_time(str(metadata.get("expires_at") or ""))
        if expires_at <= datetime.now(UTC) + timedelta(minutes=2):
            refresh_token = self.secrets.get(f"{prefix}.refresh_token")
            if not refresh_token:
                raise ConnectorConfigurationError("飞书授权已过期，请重新授权")
            token = await self.feishu.refresh(refresh_token)
            # Feishu rotates refresh tokens: the old token becomes invalid as
            # soon as a refresh succeeds. Always persist the replacement.
            rotated_refresh_token = str(token.get("refresh_token") or "").strip()
            if not rotated_refresh_token:
                raise ConnectorConfigurationError(
                    "飞书刷新未返回新的 refresh_token，请在应用后台开通 offline_access 并重新授权。"
                )
            access_token = str(token["access_token"])
            self.secrets.set(f"{prefix}.access_token", access_token)
            self.secrets.set(f"{prefix}.refresh_token", rotated_refresh_token)
            metadata["refresh_token_configured"] = True
            metadata["expires_at"] = (datetime.now(UTC) + timedelta(seconds=int(token.get("expires_in") or 7200))).isoformat()
            if token.get("refresh_token_expires_in"):
                metadata["refresh_expires_at"] = (
                    datetime.now(UTC) + timedelta(seconds=int(token["refresh_token_expires_in"]))
                ).isoformat()
            if token.get("scope"):
                metadata["scope"] = str(token["scope"])
            for item in await self.list_connections():
                if item.get("adapter", "").startswith("feishu_"):
                    item["auth_metadata"] = metadata
                    await self.db.execute("UPDATE connector_connections SET data_json=?,updated_at=? WHERE id=?", (dumps(item), now(), item["id"]))
        return access_token

    async def create_feishu_task(
        self, *, title: str, description: str = "", due_at: str | None = None, operation_id: str | None = None
    ) -> dict[str, Any]:  # noqa: E501
        token = await self._feishu_access_token()
        return await self.feishu.create_task(token, title=title, description=description, due_at=due_at, operation_id=operation_id)

    async def update_feishu_task(
        self,
        *,
        task_id: str,
        title: str | None = None,
        description: str | None = None,
        due_at: str | None = None,
        status: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        token = await self._feishu_access_token()
        task_guid = await self._local_feishu_task_guid(task_id)
        return await self.feishu.update_task(
            token, task_id=task_guid or task_id, title=title, description=description, due_at=due_at, status=status, operation_id=operation_id
        )  # noqa: E501

    async def delete_feishu_task(self, *, task_id: str, operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        task_guid = await self._local_feishu_task_guid(task_id)
        return await self._delete_feishu_item(
            "todo", task_id,
            lambda: self.feishu.delete_task(token, task_id=task_guid or task_id, operation_id=operation_id),
        )

    async def _mark_feishu_item_deleted(self, provider: str, remote_id: str) -> int:
        """Reconcile a remote delete immediately in the local read model."""
        candidate = str(remote_id or "").strip()
        if not candidate:
            return 0
        rows = await self.db.fetchall(
            "SELECT id,connection_id,data_json FROM connector_items WHERE provider=? AND status='active'",
            (provider,),
        )
        marked = 0
        for row in rows:
            item = loads(row["data_json"], {})
            identifiers = {
                str(item.get("source_id") or ""),
                str(item.get("id") or ""),
                str(item.get("task_guid") or ""),
                str(item.get("remote_uid") or ""),
            }
            if candidate not in identifiers:
                continue
            stamp = now()
            item["status"] = "deleted"
            item["deleted_at"] = stamp
            await self.db.execute(
                "UPDATE connector_items SET data_json=?,status='deleted',updated_at=? WHERE id=?",
                (dumps(item), stamp, row["id"]),
            )
            event = {
                "id": uid(),
                "provider": provider,
                "connection_id": row["connection_id"],
                "event_type": "deleted",
                "item_id": row["id"],
                "source_id": item.get("source_id") or candidate,
                "occurred_at": stamp,
                "item": item,
            }
            await self.db.execute(
                "INSERT INTO sync_events(id,connection_id,provider,event_type,item_id,data_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (event["id"], row["connection_id"], provider, "deleted", row["id"], dumps(redact(event)), stamp),
            )
            marked += 1
        return marked

    async def _delete_feishu_item(self, provider: str, remote_id: str, operation):
        """Treat an already absent remote object as an idempotent success."""
        try:
            result = await operation()
        except ConnectorRemoteError as exc:
            message = str(exc).lower()
            if not any(token in message for token in ("deleted", "not found", "not exist", "不存在", "已删除")):
                raise
            result = {"code": 0, "msg": "already deleted", "already_deleted": True}
        await self._mark_feishu_item_deleted(provider, remote_id)
        return result

    async def _local_feishu_task_guid(self, task_id: str) -> str | None:
        """Find the mutation UUID captured during a previous task sync."""
        candidate = str(task_id).strip()
        if not candidate or re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", candidate):
            return candidate or None
        rows = await self.db.fetchall("SELECT data_json FROM connector_items WHERE provider='todo' AND status='active'")
        for row in rows:
            item = loads(row["data_json"], {})
            if str(item.get("source_id") or "") == candidate or str(item.get("id") or "") == candidate:
                guid = str(item.get("task_guid") or "")
                if guid:
                    return guid
        return None

    async def create_feishu_document(self, *, title: str, content: str = "", operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        return await self.feishu.create_document(token, title=title, content=content, operation_id=operation_id)

    async def append_feishu_document(self, *, document_id: str, content: str, operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        return await self.feishu.append_document(token, document_id=document_id, content=content, operation_id=operation_id)

    async def delete_feishu_document(self, *, document_id: str, operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        return await self._delete_feishu_item(
            "notes", document_id,
            lambda: self.feishu.delete_document(token, document_id=document_id, operation_id=operation_id),
        )

    async def update_feishu_document_block(self, *, document_id: str, block_id: str, content: str, operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        return await self.feishu.update_document_block(token, document_id=document_id, block_id=block_id, content=content, operation_id=operation_id)

    async def create_feishu_event(
        self, *, title: str, starts_at: str, ends_at: str, description: str = "", location: str = "", timezone: str | None = None, participants: list[Any] | None = None, operation_id: str | None = None
    ) -> dict[str, Any]:  # noqa: E501
        token = await self._feishu_access_token()
        return await self.feishu.create_calendar_event(
            token,
            title=title,
            starts_at=starts_at,
            ends_at=ends_at,
            description=description,
            location=location,
            timezone=timezone,
            participants=participants,
            operation_id=operation_id,
        )

    async def update_feishu_event(
        self,
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
        token = await self._feishu_access_token()
        return await self.feishu.update_calendar_event(
            token,
            event_id=event_id,
            timezone=timezone,
            title=title,
            starts_at=starts_at,
            ends_at=ends_at,
            description=description,
            location=location,
            operation_id=operation_id,
        )

    async def delete_feishu_event(self, *, event_id: str, operation_id: str | None = None) -> dict[str, Any]:
        token = await self._feishu_access_token()
        return await self._delete_feishu_item(
            "calendar", event_id,
            lambda: self.feishu.delete_calendar_event(token, event_id=event_id, operation_id=operation_id),
        )

    async def send_email(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        cc: str = "",
        bcc: str = "",
        html_body: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        reply_to_message_id: str | None = None,
        forward_message_id: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        if not self.email:
            raise ConnectorConfigurationError("邮箱真实连接器未配置")
        connection = await self.get_connection("email")
        if not connection or connection.get("status") != "connected" or connection.get("adapter") != "imap_smtp":
            raise ConnectorConfigurationError("请先配置并连接 QQ/163 邮箱")
        in_reply_to = references = ""
        if reply_to_message_id:
            row = await self.db.fetchone(
                "SELECT data_json FROM connector_items WHERE id=? OR source_id=?", (reply_to_message_id, reply_to_message_id)
            )  # noqa: E501
            if not row:
                raise ConnectorConfigurationError("reply target email was not found in the local inbox")
            original = loads(row["data_json"], {})
            in_reply_to = str(original.get("message_id") or original.get("source_id") or "")
            references = " ".join(item for item in (str(original.get("references") or ""), in_reply_to) if item)
            if not subject.lower().startswith("re:"):
                subject = f"Re: {subject}"
        if forward_message_id:
            row = await self.db.fetchone("SELECT data_json FROM connector_items WHERE id=? OR source_id=?", (forward_message_id, forward_message_id))
            if not row:
                raise ConnectorConfigurationError("forward target email was not found in the local inbox")
            original = loads(row["data_json"], {})
            quoted = str(original.get("body") or "")
            body = f"{body}\n\n---------- Forwarded message ----------\n{quoted}" if body else quoted
            if not subject.lower().startswith("fwd:"):
                subject = f"Fwd: {subject}"
        return await self.email.send(
            connection,
            to=to,
            subject=subject,
            body=body,
            cc=cc,
            bcc=bcc,
            html_body=html_body,
            attachments=attachments,
            in_reply_to=in_reply_to,
            references=references,
            operation_id=operation_id,
        )

    @staticmethod
    def _nested_values(value: Any, keys: set[str]) -> list[str]:
        values: list[str] = []
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key) in keys and child not in (None, ""):
                    values.append(str(child))
                values.extend(ConnectorService._nested_values(child, keys))
        elif isinstance(value, list):
            for child in value:
                values.extend(ConnectorService._nested_values(child, keys))
        return values

    @staticmethod
    def _safe_remote_item(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_id": item.get("source_id"),
            "kind": item.get("kind"),
            "title": item.get("title"),
            "updated_at": item.get("updated_at"),
        }

    async def verify_external_operation(
        self,
        *,
        tool_name: str,
        arguments: dict[str, Any],
        result: Any,
        operation_id: str,
    ) -> dict[str, Any]:
        """Read provider state after an uncertain write without replaying it."""
        canonical = {
            "send_email": "email_send",
            "create_task": "feishu_task_create",
            "update_task": "feishu_task_update",
            "delete_task": "feishu_task_delete",
            "create_calendar_event": "feishu_calendar_event_create",
            "add_calendar_event": "feishu_calendar_event_create",
            "update_calendar_event": "feishu_calendar_event_update",
            "delete_calendar_event": "feishu_calendar_event_delete",
            "create_document": "feishu_document_create",
            "publish_document": "feishu_document_create",
            "append_document": "feishu_document_append",
            "delete_document": "feishu_document_delete",
        }.get(tool_name, tool_name)
        if canonical == "email_send":
            connection = await self.get_connection("email")
            if not connection or connection.get("status") != "connected" or not self.email:
                raise ConnectorConfigurationError("邮箱未连接，无法查询发送结果")
            state = await self.email.find_sent_operation(connection, operation_id)
            if state.get("found"):
                return {
                    "status": "confirmed",
                    "provider": "email",
                    "message": "已在发件箱找到带有本次操作标识的邮件。",
                    "matches": state.get("matches") or [],
                }
            return {
                "status": "not_found",
                "provider": "email",
                "message": "发件箱暂未找到本次操作标识，不能确认邮件是否已发送。",
            }

        provider = "calendar" if "calendar" in canonical else "todo" if "task" in canonical else "notes" if "document" in canonical else ""
        if not provider:
            return {"status": "unsupported", "message": f"暂不支持查询 {tool_name} 的远端状态。"}
        connection = next(
            (
                item
                for item in await self.list_connections()
                if item.get("adapter", "").startswith("feishu_") and item.get("status") == "connected"
            ),
            None,
        )
        if not connection:
            raise ConnectorConfigurationError("飞书未连接，无法查询远端结果")
        records = await self._real_feishu_records(provider, connection)
        id_keys = {
            "calendar": {"event_id", "id"},
            "todo": {"task_id", "guid", "id"},
            "notes": {"document_id", "token", "file_token", "id"},
        }[provider]
        remote_ids = set(self._nested_values(result, id_keys)) | set(self._nested_values(arguments, id_keys))
        exact = [item for item in records if str(item.get("source_id") or "") in remote_ids]
        is_delete = canonical.endswith("_delete")
        if is_delete and remote_ids:
            if exact:
                return {
                    "status": "still_present",
                    "provider": "feishu",
                    "message": "远端列表中仍能找到目标，删除结果尚未确认。",
                    "matches": [self._safe_remote_item(item) for item in exact[:3]],
                }
            return {
                "status": "confirmed_absent",
                "provider": "feishu",
                "message": "远端列表中已找不到目标，可以确认删除已生效。",
                "remote_ids": sorted(remote_ids),
            }
        if exact:
            return {
                "status": "confirmed",
                "provider": "feishu",
                "message": "已在飞书远端列表中找到本次操作对应的对象。",
                "matches": [self._safe_remote_item(item) for item in exact[:3]],
            }

        title = str(arguments.get("title") or "").strip()
        content = str(arguments.get("content") or "").strip()
        starts_at = str(arguments.get("starts_at") or "").strip()
        candidates = []
        for item in records:
            if title and str(item.get("title") or "").strip() != title:
                continue
            if starts_at and not str(item.get("starts_at") or "").startswith(starts_at[:16]):
                continue
            if content and canonical == "feishu_document_append" and content not in str(item.get("body") or ""):
                continue
            if title or content:
                candidates.append(item)
        if candidates:
            return {
                "status": "confirmed",
                "provider": "feishu",
                "message": "已根据操作参数在飞书远端列表中找到匹配对象。",
                "matches": [self._safe_remote_item(item) for item in candidates[:3]],
            }
        return {
            "status": "not_found",
            "provider": "feishu",
            "message": "当前远端列表中没有找到确定匹配，不能安全重试。",
            "searched_items": len(records),
        }

    async def list_items(
        self,
        *,
        provider: str | None = None,
        include_done: bool = True,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT i.data_json FROM connector_items i JOIN connector_connections c "
            "ON c.id=i.connection_id WHERE c.status='connected' AND i.status='active'"
        )
        args: list[Any] = []
        if provider:
            query += " AND i.provider=?"
            args.append(self._validate_provider(provider))
        query += " ORDER BY i.updated_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        rows = await self.db.fetchall(query, tuple(args))
        items = [loads(row["data_json"], {}) for row in rows]
        if not include_done:
            items = [item for item in items if not item.get("is_done")]
        return items

    async def inbox(
        self,
        *,
        provider: str | None = None,
        limit: int = 100,
        calendar_period: str = "all",
        completed_only: bool = False,
    ) -> dict[str, Any]:
        items = await self.list_items(provider=provider, include_done=completed_only, limit=limit)
        if completed_only:
            items = [item for item in items if bool(item.get("is_done"))]
        if not completed_only and (provider or "") == "calendar" and calendar_period in {"past", "future"}:
            current = datetime.now(UTC)
            def is_past(item: dict[str, Any]) -> bool | None:
                raw_value = item.get("ends_at") or item.get("starts_at")
                if not raw_value:
                    return None
                return _parse_time(raw_value) < current
            items = [item for item in items if is_past(item) is not None and is_past(item) == (calendar_period == "past")]
        items.sort(
            key=lambda item: _parse_time(
                item.get("created_at") or item.get("updated_at")
                if item.get("kind") == "email"
                else item.get("starts_at") or item.get("due_at") or item.get("updated_at") or item.get("created_at")
            ),
            reverse=True,
        )
        for item in items:
            item["related_items"] = await self.relations.related(self.db, str(item.get("id") or ""), limit=8)
        connections = await self.list_connections()
        connected = [item for item in connections if item.get("status") == "connected"]
        events = await self.sync_events(limit=12)
        counts = {
            "total": len(items),
            "unread": sum(1 for item in items if not item.get("is_read", False)),
            "pending": sum(1 for item in items if not item.get("is_done", False)),
            "connected": len(connected),
        }
        by_provider = {provider_name: sum(1 for item in items if item.get("provider") == provider_name) for provider_name in PROVIDERS}
        return {"items": items, "counts": counts, "by_provider": by_provider, "connections": connected, "sync_events": events}

    async def delete_item(self, item_id: str) -> dict[str, Any]:
        """Hide a completed item locally while preserving a sync tombstone."""
        row = await self.db.fetchone("SELECT id,data_json FROM connector_items WHERE id=?", (item_id,))
        if not row:
            raise KeyError("connector item not found")
        item = loads(row["data_json"], {})
        if not bool(item.get("is_done")):
            raise ValueError("Only completed inbox items can be deleted")
        item["deleted_at"] = now()
        async with self.db.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "DELETE FROM context_links WHERE (source_type='connector_item' AND source_id=?) OR (target_type='connector_item' AND target_id=?)",
                (item_id, item_id),
            )
            await db.execute("DELETE FROM sync_events WHERE item_id=?", (item_id,))
            await db.execute("DELETE FROM external_information WHERE connector_item_id=?", (item_id,))
            await db.execute("UPDATE connector_items SET data_json=?,status='deleted',updated_at=? WHERE id=?", (dumps(item), item["deleted_at"], item_id))
            await db.commit()
        return {"item_id": item_id, "deleted": True}

    async def related_items(self, item_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        return await self.relations.related(self.db, item_id, limit=limit)

    async def attachment_path(self, attachment_id: str) -> tuple[Any, dict[str, Any]] | None:
        """Resolve an inbox attachment without exposing filesystem paths to the client."""
        if not self.email or not self.email.attachments_dir:
            return None
        if not re.fullmatch(r"[0-9a-f]{32}", attachment_id):
            return None
        rows = await self.db.fetchall("SELECT data_json FROM connector_items WHERE provider='email' AND status='active'")
        for row in rows:
            item = loads(row["data_json"], {})
            for attachment in item.get("attachments") or []:
                if str(attachment.get("attachment_id") or "") != attachment_id:
                    continue
                path = (self.email.attachments_dir / attachment_id).resolve()
                try:
                    path.relative_to(self.email.attachments_dir.resolve())
                except ValueError:
                    return None
                if not path.is_file() or not bool(attachment.get("download_available", False)):
                    return None
                return path, attachment
        return None

    async def issue_attachment_token(self, attachment_id: str, *, ttl_seconds: int = 600) -> str:
        resolved = await self.attachment_path(attachment_id)
        if not resolved:
            raise KeyError("attachment not found")
        token = new_download_token()
        stamp = datetime.now(UTC)
        await self.db.execute("DELETE FROM attachment_download_tokens WHERE expires_at<=?", (stamp.isoformat(),))
        await self.db.execute(
            "INSERT INTO attachment_download_tokens(token_hash,attachment_id,expires_at,created_at,used_at) VALUES(?,?,?,?,NULL)",
            (hash_token(token), attachment_id, (stamp + timedelta(seconds=max(30, min(int(ttl_seconds), 3600)))).isoformat(), stamp.isoformat()),
        )
        return token

    async def attachment_path_from_token(self, attachment_id: str, token: str) -> tuple[Any, dict[str, Any]] | None:
        if not token or len(token) < 24:
            return None
        stamp = datetime.now(UTC).isoformat()
        row = await self.db.fetchone(
            "SELECT attachment_id FROM attachment_download_tokens WHERE token_hash=? AND expires_at>? AND used_at IS NULL",
            (hash_token(token), stamp),
        )
        if not row or str(row["attachment_id"]) != attachment_id:
            return None
        await self.db.execute("UPDATE attachment_download_tokens SET used_at=? WHERE token_hash=?", (stamp, hash_token(token)))
        return await self.attachment_path(attachment_id)

    async def cleanup_expired_attachments(self, *, max_age_days: int = 30) -> dict[str, int]:
        """Remove old cached files and mark their inbox metadata unavailable."""
        if not self.email or not self.email.attachments_dir:
            return {"removed": 0, "marked": 0}
        cutoff = datetime.now(UTC).timestamp() - max(1, int(max_age_days)) * 86400
        removed = 0
        for path in self.email.attachments_dir.iterdir():
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
                removed += 1
        marked = 0
        rows = await self.db.fetchall("SELECT id,data_json FROM connector_items WHERE provider='email' AND status='active'")
        for row in rows:
            item = loads(row["data_json"], {})
            changed = False
            for attachment in item.get("attachments") or []:
                attachment_id = str(attachment.get("attachment_id") or "")
                if not attachment_id or not bool(attachment.get("download_available", False)):
                    continue
                path = self.email.attachments_dir / attachment_id
                if not path.is_file():
                    attachment["download_available"] = False
                    attachment["expired_at"] = now()
                    changed = True
                    marked += 1
            if changed:
                await self.db.execute("UPDATE connector_items SET data_json=?,updated_at=? WHERE id=?", (dumps(item), now(), row["id"]))
        await self.db.execute("DELETE FROM attachment_download_tokens WHERE expires_at<=? OR used_at IS NOT NULL", (datetime.now(UTC).isoformat(),))
        return {"removed": removed, "marked": marked}

    async def update_item(self, item_id: str, values: dict[str, Any]) -> dict[str, Any]:
        row = await self.db.fetchone("SELECT provider,data_json FROM connector_items WHERE id=?", (item_id,))
        if not row:
            raise KeyError("connector item not found")
        item = loads(row["data_json"], {})
        for key in ("is_read", "is_done"):
            if key in values:
                item[key] = bool(values[key])
        # Completing an email is intentionally a two-step operation.  The UI
        # disables this action for unread mail, but the RPC endpoint must also
        # enforce the rule so callers cannot bypass it by invoking the backend
        # directly.  Marking it read and done in the same request is allowed.
        provider = str(row["provider"] or item.get("provider") or "")
        if provider == "email" and bool(item.get("is_done")) and not bool(item.get("is_read")):
            raise ValueError("Unread email must be marked as read before it can be completed")
        item["updated_at"] = now()
        await self.db.execute("UPDATE connector_items SET data_json=?,updated_at=? WHERE id=?", (dumps(item), item["updated_at"], item_id))
        return item

    async def sync_events(self, *, provider: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        query = "SELECT data_json FROM sync_events"
        args: list[Any] = []
        if provider:
            query += " WHERE provider=?"
            args.append(self._validate_provider(provider))
        query += " ORDER BY created_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 200)))
        rows = await self.db.fetchall(query, tuple(args))
        return [loads(row["data_json"], {}) for row in rows]
