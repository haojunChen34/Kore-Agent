from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

from . import __version__
from .application import Application, RpcFault
from .config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("koreagent_next.server")
HEX_ID = re.compile(r"^[0-9a-f]{32}$")
LOOPBACK = {"127.0.0.1", "::1", "localhost", "testclient"}


def trusted_local_origin(origin: str | None) -> bool:
    # Native Electron file pages use null; non-browser local clients omit Origin.
    if origin in (None, "", "null", "file://"):
        return True
    parsed = urlsplit(origin)
    return parsed.scheme in {"http", "https"} and parsed.hostname in LOOPBACK and not parsed.username


class Hub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def add(self, ws: WebSocket) -> None:
        async with self.lock:
            self.clients.add(ws)

    async def remove(self, ws: WebSocket) -> None:
        async with self.lock:
            self.clients.discard(ws)

    async def broadcast(self, method: str, params: dict[str, Any]) -> None:
        payload = {"jsonrpc": "2.0", "method": method, "params": params}
        dead: list[WebSocket] = []
        async with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                await client.send_json(payload)
            except Exception:
                dead.append(client)
        for client in dead:
            await self.remove(client)


def create_app(application: Application | None = None) -> FastAPI:
    application = application or Application()
    hub = Hub()
    application.broadcast = hub.broadcast

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await application.start()
        logger.info("KoreAgent Next Host started")
        try:
            yield
        finally:
            await application.close()

    app = FastAPI(title="KoreAgent Next Host", version=__version__, lifespan=lifespan)
    app.state.application, app.state.hub = application, hub

    @app.get("/health")
    async def health() -> dict[str, Any]:
        model_view = await application.model_settings_get({})
        connector_view = await application.connector_health({})
        return {
            "status": "ok",
            "provider": model_view.get("active_provider") or application.settings.provider,
            "model": model_view.get("active_model") or application.settings.model,
            "version": __version__,
            # The launcher uses this local-only capability list to avoid
            # reusing a healthy Host process that was started from an older
            # checkout and therefore does not know newly added RPC methods.
            "rpc_methods": sorted(application.methods()),
            "connectors": [
                {
                    "provider": item.get("provider"),
                    "status": (item.get("health") or {}).get("status") or "disconnected",
                    "connected": bool((item.get("health") or {}).get("connected")),
                    "reauthorize_required": bool((item.get("health") or {}).get("reauthorize_required")),
                    "last_successful_sync": (item.get("health") or {}).get("last_successful_sync"),
                }
                for item in connector_view.get("connections", [])
            ],
            "current_error_count": len(connector_view.get("current_errors", [])),
        }

    @app.post("/webhooks/feishu")
    async def feishu_webhook(request: Request) -> dict[str, Any]:
        """Receive Feishu event callbacks; polling remains the fallback."""
        try:
            raw_body = await request.body()
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            raise HTTPException(400, "invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise HTTPException(400, "payload must be an object")
        verification = os.getenv("KORENEXT_FEISHU_VERIFICATION_TOKEN", "").strip()
        supplied = str(payload.get("token") or payload.get("header", {}).get("token") or request.headers.get("x-feishu-token") or "")
        if verification and supplied and supplied != verification:
            raise HTTPException(403, "invalid webhook token")
        if verification and not supplied:
            raise HTTPException(403, "webhook token is missing")
        # Feishu sends this challenge during endpoint verification. Echo it
        # without touching connector state; the verification token above is
        # sufficient for this handshake.
        if payload.get("challenge"):
            return {"challenge": payload["challenge"]}
        # Feishu can sign each callback with the app encrypt key.  Keep token
        # validation for compatibility, and verify the stronger signature when
        # the key and signature headers are configured.  The signature covers
        # the exact request bytes, so preserve the raw body before parsing it.
        encrypt_key = os.getenv("KORENEXT_FEISHU_ENCRYPT_KEY", "").strip()
        signature = request.headers.get("x-lark-signature") or request.headers.get("x-feishu-signature")
        if encrypt_key and signature:
            timestamp = request.headers.get("x-lark-request-timestamp") or request.headers.get("x-feishu-request-timestamp") or ""
            nonce = request.headers.get("x-lark-request-nonce") or request.headers.get("x-feishu-request-nonce") or ""
            expected = hashlib.sha256(f"{timestamp}{nonce}{encrypt_key}".encode() + raw_body).hexdigest()
            if not hmac.compare_digest(signature.strip(), expected):
                raise HTTPException(403, "invalid webhook signature")
        elif encrypt_key and payload.get("header", {}).get("signature"):
            # Some gateways place the signature in the event envelope.
            supplied_signature = str(payload["header"].get("signature") or "")
            timestamp = str(payload["header"].get("create_time") or "")
            nonce = str(payload["header"].get("nonce") or "")
            expected = hashlib.sha256(f"{timestamp}{nonce}{encrypt_key}".encode() + raw_body).hexdigest()
            if not hmac.compare_digest(supplied_signature, expected):
                raise HTTPException(403, "invalid webhook signature")
        elif encrypt_key:
            raise HTTPException(403, "webhook signature is missing")
        try:
            return await application.connector_webhook({"provider": "feishu", "payload": payload})
        except Exception as exc:
            logger.exception("Feishu webhook processing failed")
            raise HTTPException(500, "webhook processing failed") from exc

    @app.get("/oauth/{provider}/callback", response_class=HTMLResponse)
    async def connector_oauth_callback(request: Request, provider: str, code: str = "", state: str = "", error: str = "") -> str:
        """Complete a connector OAuth flow opened by the desktop app."""
        require_local(request)
        if error:
            return f"<html><body><h2>授权未完成</h2><p>{html.escape(error)}</p><p>可以关闭此窗口并返回 KoreAgent。</p></body></html>"
        if not code or not state:
            return "<html><body><h2>授权参数不完整</h2><p>可以关闭此窗口并重新发起授权。</p></body></html>"
        try:
            state_row = await application.db.fetchone("SELECT provider FROM oauth_states WHERE state=?", (state,))
            oauth_provider = str(state_row["provider"]) if state_row else provider
            result = await application.connector_oauth_complete({"provider": oauth_provider, "state": state, "code": code})
            await application.broadcast("connector.updated", result.get("connection") or {})
            sync_errors = result.get("sync_errors") or []
            if sync_errors:
                details = "".join(
                    f"<li>{html.escape(str(item.get('provider') or 'unknown'))}: {html.escape(str(item.get('error') or 'sync failed'))}</li>"
                    for item in sync_errors
                )
                return (
                    "<html><body><h2>授权成功，但首次同步部分失败</h2>"
                    "<p>飞书账号已经连接并保存。可以关闭此窗口，回到 KoreAgent 后重新同步。"
                    "这不会影响已经成功连接的日历和任务。</p>"
                    f"<ul>{details}</ul></body></html>"
                )
            return "<html><body><h2>授权成功</h2><p>连接器已经保存，可以关闭此窗口并返回 KoreAgent。</p></body></html>"
        except Exception as exc:
            logger.exception("Connector OAuth callback failed")
            return f"<html><body><h2>授权失败</h2><p>{html.escape(str(exc))}</p><p>可以关闭此窗口并重试。</p></body></html>"

    def require_local(request: Request) -> None:
        host = request.client.host if request.client else ""
        if host not in LOOPBACK or not trusted_local_origin(request.headers.get("origin")):
            raise HTTPException(403, "forbidden")

    @app.get("/artifacts/{artifact_id}/content")
    async def artifact_content(artifact_id: str, request: Request) -> Response:
        require_local(request)
        if not HEX_ID.fullmatch(artifact_id):
            raise HTTPException(404, "not found")
        path = await application.artifact_path(artifact_id)
        if not path:
            raise HTTPException(404, "not found")
        result = await application.artifact_get({"id": artifact_id})
        artifact = result["artifact"]
        return FileResponse(
            path,
            media_type=artifact.get("mime_type") or "application/octet-stream",
            filename=artifact.get("filename") or "artifact",
            headers={"Cache-Control": "no-store"},
        )

    @app.post("/connectors/email/attachments/{attachment_id}/token")
    async def email_attachment_token(attachment_id: str, request: Request) -> dict[str, Any]:
        require_local(request)
        try:
            token = await application.connector_service.issue_attachment_token(attachment_id)
        except KeyError as exc:
            raise HTTPException(404, "not found") from exc
        return {"token": token, "expires_in": 600}

    @app.get("/connectors/email/attachments/{attachment_id}/download")
    async def email_attachment_download(attachment_id: str, request: Request, token: str = "") -> Response:
        require_local(request)
        resolved = await application.connector_service.attachment_path_from_token(attachment_id, token)
        if not resolved:
            raise HTTPException(403, "download token is missing or expired")
        path, attachment = resolved
        return FileResponse(
            path,
            media_type=attachment.get("content_type") or "application/octet-stream",
            filename=attachment.get("filename") or "attachment",
            headers={"Cache-Control": "no-store"},
        )

    async def handle(ws: WebSocket, payload: Any) -> None:
        request_id: str | int | None = payload.get("id") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0":
            await ws.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32600, "message": "Invalid Request"},
                }
            )
            return
        method, params = payload.get("method"), payload.get("params", {})
        if not isinstance(method, str) or not isinstance(params, dict):
            await ws.send_json(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32600, "message": "Invalid Request"},
                }
            )
            return
        fn = application.methods().get(method)
        if fn is None:
            if request_id is not None:
                await ws.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": f"Method not found: {method}"},
                    }
                )
            return
        try:
            result = await fn(params)
            if request_id is not None:
                await ws.send_json({"jsonrpc": "2.0", "id": request_id, "result": result})
        except RpcFault as exc:
            if request_id is not None:
                error = {"code": exc.code, "message": exc.message}
                if exc.data is not None:
                    error["data"] = exc.data
                await ws.send_json({"jsonrpc": "2.0", "id": request_id, "error": error})
        except asyncio.CancelledError:
            if request_id is not None:
                await ws.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32001, "message": "Request cancelled"},
                    }
                )
        except Exception:
            logger.exception("RPC method %s failed", method)
            if request_id is not None:
                await ws.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32603, "message": "Internal error"},
                    }
                )

    @app.websocket("/rpc")
    async def rpc(ws: WebSocket) -> None:
        if not ws.client or ws.client.host not in LOOPBACK or not trusted_local_origin(ws.headers.get("origin")):
            await ws.close(code=1008)
            return
        await ws.accept()
        await hub.add(ws)
        tasks: set[asyncio.Task[Any]] = set()
        try:
            while True:
                text = await ws.receive_text()
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    await ws.send_json(
                        {
                            "jsonrpc": "2.0",
                            "id": None,
                            "error": {"code": -32700, "message": "Parse error"},
                        }
                    )
                    continue
                task = asyncio.create_task(handle(ws, payload))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        except WebSocketDisconnect:
            pass
        finally:
            await hub.remove(ws)
            for task in tasks:
                task.cancel()

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="KoreAgent Next local host")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()
    settings = Settings()
    uvicorn.run(
        create_app(Application(settings)),
        host=args.host or settings.host,
        port=args.port or settings.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
