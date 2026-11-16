from __future__ import annotations

import base64
import binascii
import ctypes
import json
import os
import secrets
import subprocess
import sys
from ctypes import wintypes
from pathlib import Path

from cryptography.fernet import Fernet

SERVICE = "com.koreagent-next.desktop.model-api-key"


class SecretStore:
    """Store model credentials in Keychain or Windows DPAPI.

    Plain-text JSON written by early development builds is read for migration but is
    never written again.
    """

    def __init__(self, settings_dir: Path):
        self.path = settings_dir / "secrets.json"
        self._ephemeral_encryption_key: bytes | None = None

    def permission_signing_key(self) -> bytes | None:
        """Return a stable OS-protected key, or disable persistent rules on unsupported hosts."""
        if sys.platform not in {"darwin", "win32"}:
            return None
        account = "__permission_rules_signing_key_v2"
        encoded = self.get(account, allow_environment=False)
        if not encoded:
            self.set(account, base64.b64encode(secrets.token_bytes(32)).decode("ascii"))
            encoded = self.get(account, allow_environment=False)
        try:
            key = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise RuntimeError("permission signing key is invalid") from exc
        if len(key) < 32:
            raise RuntimeError("permission signing key is too short")
        return key

    def get(self, provider: str, *, allow_environment: bool = True) -> str:
        env = os.getenv(f"KORENEXT_{provider.upper()}_API_KEY", "") if allow_environment else ""
        if env:
            return env
        if sys.platform == "darwin":
            result = subprocess.run(
                ["/usr/bin/security", "find-generic-password", "-s", SERVICE, "-a", provider, "-w"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.stdout.strip() if result.returncode == 0 else ""
        try:
            value = json.loads(self.path.read_text(encoding="utf-8")).get(provider, "")
        except (OSError, json.JSONDecodeError):
            return ""
        if sys.platform == "win32" and isinstance(value, dict) and value.get("encoding") == "dpapi":
            try:
                return self._dpapi_unprotect(str(value.get("value") or ""))
            except OSError:
                return ""
        return value if isinstance(value, str) else ""

    def source(self, provider: str) -> str:
        if os.getenv(f"KORENEXT_{provider.upper()}_API_KEY", ""):
            return "environment"
        if sys.platform == "darwin":
            return "keychain" if self.get(provider) else "none"
        if sys.platform == "win32":
            try:
                value = json.loads(self.path.read_text(encoding="utf-8")).get(provider)
            except (OSError, json.JSONDecodeError):
                return "none"
            return "dpapi" if isinstance(value, dict) and value.get("encoding") == "dpapi" else ("legacy_plaintext" if value else "none")
        return "none"

    def set(self, provider: str, secret: str) -> None:
        if sys.platform == "darwin":
            subprocess.run(
                ["/usr/bin/security", "add-generic-password", "-U", "-s", SERVICE, "-a", provider, "-w", secret],
                check=True,
                capture_output=True,
            )
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {}
        if sys.platform == "win32":
            data[provider] = {"encoding": "dpapi", "value": self._dpapi_protect(secret)}
        else:
            raise RuntimeError("Persistent model API keys require macOS Keychain or Windows DPAPI; use an environment variable")
        self.path.write_text(json.dumps(data), encoding="utf-8")
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def delete(self, provider: str) -> None:
        """Remove one stored secret without raising when it is already absent."""
        if sys.platform == "darwin":
            subprocess.run(
                ["/usr/bin/security", "delete-generic-password", "-s", SERVICE, "-a", provider],
                check=False,
                capture_output=True,
            )
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if provider in data:
            data.pop(provider, None)
            if data:
                self.path.write_text(json.dumps(data), encoding="utf-8")
            else:
                try:
                    self.path.unlink()
                except FileNotFoundError:
                    pass

    def clear(self) -> None:
        """Clear locally managed credentials (model and connector secrets)."""
        if sys.platform == "darwin":
            # Known keys are removed explicitly; Keychain has no safe
            # namespace enumeration API for an application-owned service.
            for provider in ("openai", "deepseek", "qwen", "anthropic", "connector.feishu.access_token", "connector.feishu.refresh_token"):
                self.delete(provider)
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def encryption_key(self) -> bytes:
        """Return an OS-protected key used for local encrypted exports.

        The key itself is kept in the same protected store as model and
        connector credentials.  It is intentionally separate from OAuth
        values so rotating credentials does not invalidate local backups.
        """
        provider = "__local_data_encryption_key_v1"
        try:
            encoded = self.get(provider, allow_environment=False)
        except TypeError:  # compatibility with injected test stores
            encoded = self.get(provider)
        if not encoded:
            encoded = Fernet.generate_key().decode("ascii")
            try:
                self.set(provider, encoded)
            except (OSError, RuntimeError):
                # Linux and other hosts without an OS credential vault cannot
                # safely persist a key here. Keep it only for this process so
                # shutdown encryption remains best-effort without writing the
                # key beside the encrypted database.
                self._ephemeral_encryption_key = encoded.encode("ascii")
                return self._ephemeral_encryption_key
        try:
            Fernet(encoded.encode("ascii"))
        except Exception:
            # A custom/in-memory SecretStore used by callers may not persist
            # this reserved key. Keep a process-local fallback rather than
            # allowing shutdown encryption to break the host.
            if self._ephemeral_encryption_key is None:
                self._ephemeral_encryption_key = Fernet.generate_key()
            return self._ephemeral_encryption_key
        return encoded.encode("ascii")

    def encrypt(self, payload: bytes) -> bytes:
        return Fernet(self.encryption_key()).encrypt(payload)

    def decrypt(self, payload: bytes) -> bytes:
        return Fernet(self.encryption_key()).decrypt(payload)

    @staticmethod
    def _dpapi_protect(secret: str) -> str:
        return base64.b64encode(SecretStore._crypt_protect(secret.encode("utf-8"))).decode("ascii")

    @staticmethod
    def _dpapi_unprotect(encoded: str) -> str:
        return SecretStore._crypt_unprotect(base64.b64decode(encoded)).decode("utf-8")

    @staticmethod
    def _crypt_protect(raw: bytes) -> bytes:
        if sys.platform != "win32":
            raise OSError("DPAPI is only available on Windows")
        source, source_buffer = SecretStore._blob(raw)
        entropy, entropy_buffer = SecretStore._blob(SERVICE.encode("utf-8"))
        output = _DATA_BLOB()
        if not ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(source),
            SERVICE,
            ctypes.byref(entropy),
            None,
            None,
            0x01,
            ctypes.byref(output),
        ):
            raise ctypes.WinError()
        try:
            return ctypes.string_at(output.pbData, output.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(output.pbData)

    @staticmethod
    def _crypt_unprotect(raw: bytes) -> bytes:
        if sys.platform != "win32":
            raise OSError("DPAPI is only available on Windows")
        source, source_buffer = SecretStore._blob(raw)
        entropy, entropy_buffer = SecretStore._blob(SERVICE.encode("utf-8"))
        output = _DATA_BLOB()
        if not ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(source),
            None,
            ctypes.byref(entropy),
            None,
            None,
            0x01,
            ctypes.byref(output),
        ):
            raise ctypes.WinError()
        try:
            return ctypes.string_at(output.pbData, output.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(output.pbData)

    @staticmethod
    def _blob(raw: bytes) -> tuple[_DATA_BLOB, ctypes.Array]:
        buffer = ctypes.create_string_buffer(raw)
        blob = _DATA_BLOB(len(raw), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
        return blob, buffer


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]
