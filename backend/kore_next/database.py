from __future__ import annotations

import gc
import json
import os
import sqlite3
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .security import redact, redact_text

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS conversations(
 id TEXT PRIMARY KEY, title TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages(
 id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT NOT NULL, role TEXT NOT NULL,
 content TEXT, name TEXT, tool_call_id TEXT, tool_calls_json TEXT, reasoning TEXT, created_at TEXT NOT NULL,
 FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
 content, content='messages', content_rowid='id', tokenize='trigram'
);
CREATE TRIGGER IF NOT EXISTS messages_fts_insert AFTER INSERT ON messages BEGIN
 INSERT INTO messages_fts(rowid,content) VALUES(new.id,new.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_fts_delete AFTER DELETE ON messages BEGIN
 INSERT INTO messages_fts(messages_fts,rowid,content) VALUES('delete',old.id,old.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_fts_update AFTER UPDATE OF content ON messages BEGIN
 INSERT INTO messages_fts(messages_fts,rowid,content) VALUES('delete',old.id,old.content);
 INSERT INTO messages_fts(rowid,content) VALUES(new.id,new.content);
END;
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY, conversation_id TEXT, status TEXT NOT NULL, user_message TEXT NOT NULL,
 created_at TEXT NOT NULL, started_at TEXT, updated_at TEXT NOT NULL, completed_at TEXT,
 error TEXT, stop_reason TEXT, recovered_from_run_id TEXT, source TEXT, source_id TEXT,
 scheduled_for TEXT, triggered_at TEXT, mode TEXT NOT NULL DEFAULT 'normal'
);
CREATE TABLE IF NOT EXISTS events(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL, sequence INTEGER NOT NULL, type TEXT NOT NULL,
 event_time TEXT NOT NULL, data_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS approvals(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS automations(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, next_run_at TEXT, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS automation_occurrences(
 trigger_id TEXT PRIMARY KEY, automation_id TEXT NOT NULL, scheduled_for TEXT NOT NULL,
 run_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(automation_id, scheduled_for)
);
CREATE INDEX IF NOT EXISTS idx_automation_occurrences_automation ON automation_occurrences(automation_id, created_at);
CREATE TABLE IF NOT EXISTS memories(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
 memory_id UNINDEXED, title, summary, content, source_text, contacts, project,
 tokenize='trigram'
);
CREATE TABLE IF NOT EXISTS checkpoints(
 run_id TEXT PRIMARY KEY, conversation_id TEXT, sequence INTEGER NOT NULL,
 data_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
 data_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_evidence_run ON evidence(run_id, created_at);
CREATE TABLE IF NOT EXISTS permission_rules(
 id TEXT PRIMARY KEY, tool_name TEXT NOT NULL, effect TEXT NOT NULL,
 scope TEXT NOT NULL, scope_id TEXT NOT NULL, matcher_json TEXT NOT NULL,
 description TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS skill_candidates(
 id TEXT PRIMARY KEY, status TEXT NOT NULL, data_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS goals(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS projects(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actions(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, due_at TEXT,
 updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_actions_due ON actions(status, due_at);
CREATE TABLE IF NOT EXISTS notifications(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status, created_at);
CREATE TABLE IF NOT EXISTS connector_connections(
 id TEXT PRIMARY KEY, provider TEXT NOT NULL UNIQUE, data_json TEXT NOT NULL,
 status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_states(
 state TEXT PRIMARY KEY, provider TEXT NOT NULL, redirect_uri TEXT,
 mode TEXT NOT NULL DEFAULT 'real',
 expires_at TEXT NOT NULL, used_at TEXT, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oauth_states_expires ON oauth_states(expires_at);
CREATE TABLE IF NOT EXISTS connector_items(
 id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, provider TEXT NOT NULL,
 source_id TEXT NOT NULL, kind TEXT NOT NULL, data_json TEXT NOT NULL,
 status TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY(connection_id) REFERENCES connector_connections(id) ON DELETE CASCADE,
 UNIQUE(connection_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_connector_items_provider ON connector_items(provider, updated_at);
CREATE TABLE IF NOT EXISTS external_information(
 id TEXT PRIMARY KEY, provider TEXT NOT NULL, source_id TEXT NOT NULL,
 connector_item_id TEXT, kind TEXT NOT NULL, relevance TEXT NOT NULL DEFAULT 'needs_review',
 status TEXT NOT NULL DEFAULT 'new', analysis_json TEXT NOT NULL DEFAULT '{}',
 source_version TEXT, content_fingerprint TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 analyzed_at TEXT, handled_at TEXT, snoozed_until TEXT,
 UNIQUE(provider, source_id)
);
CREATE INDEX IF NOT EXISTS idx_external_information_status ON external_information(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_external_information_provider ON external_information(provider, status, updated_at);
CREATE TABLE IF NOT EXISTS context_links(
 id TEXT PRIMARY KEY, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
 target_type TEXT NOT NULL, target_id TEXT NOT NULL, relation TEXT NOT NULL,
 score REAL NOT NULL DEFAULT 0, data_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(source_type, source_id, target_type, target_id, relation)
);
CREATE INDEX IF NOT EXISTS idx_context_links_source ON context_links(source_type, source_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_context_links_target ON context_links(target_type, target_id, updated_at);
CREATE TABLE IF NOT EXISTS sync_events(
 id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, provider TEXT NOT NULL,
 event_type TEXT NOT NULL, item_id TEXT NOT NULL, data_json TEXT NOT NULL,
 created_at TEXT NOT NULL,
 FOREIGN KEY(connection_id) REFERENCES connector_connections(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sync_events_created ON sync_events(provider, created_at);
CREATE TABLE IF NOT EXISTS proactive_triggers(
 id TEXT PRIMARY KEY, data_json TEXT NOT NULL, status TEXT NOT NULL, source TEXT NOT NULL,
 event_type TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_proactive_triggers_match ON proactive_triggers(status, source, event_type);
CREATE TABLE IF NOT EXISTS proactive_suggestions(
 id TEXT PRIMARY KEY, trigger_id TEXT, data_json TEXT NOT NULL, status TEXT NOT NULL,
 risk TEXT NOT NULL, dedupe_key TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY(trigger_id) REFERENCES proactive_triggers(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_proactive_suggestions_status ON proactive_suggestions(status, created_at);
CREATE INDEX IF NOT EXISTS idx_proactive_suggestions_dedupe ON proactive_suggestions(trigger_id, dedupe_key, created_at);
CREATE TABLE IF NOT EXISTS execution_jobs(
 id TEXT PRIMARY KEY, suggestion_id TEXT NOT NULL, data_json TEXT NOT NULL, status TEXT NOT NULL,
 risk TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
 next_retry_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY(suggestion_id) REFERENCES proactive_suggestions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_execution_jobs_due ON execution_jobs(status, next_retry_at);
CREATE TABLE IF NOT EXISTS audit_log(
 id TEXT PRIMARY KEY, entity_type TEXT NOT NULL, entity_id TEXT, action TEXT NOT NULL,
 actor TEXT NOT NULL, data_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);
CREATE TABLE IF NOT EXISTS attachment_download_tokens(
 token_hash TEXT PRIMARY KEY, attachment_id TEXT NOT NULL, expires_at TEXT NOT NULL,
 created_at TEXT NOT NULL, used_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_attachment_download_tokens_expiry ON attachment_download_tokens(expires_at);
CREATE TABLE IF NOT EXISTS external_workflows(
 id TEXT PRIMARY KEY, status TEXT NOT NULL, data_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_external_workflows_updated ON external_workflows(updated_at);
CREATE TABLE IF NOT EXISTS external_operations(
 idempotency_key TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, tool_name TEXT NOT NULL,
 request_hash TEXT NOT NULL, arguments_json TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL,
 result_json TEXT, error TEXT, approval_id TEXT, started_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 deleted_at TEXT,
 FOREIGN KEY(workflow_id) REFERENCES external_workflows(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_external_operations_workflow ON external_operations(workflow_id, started_at);
CREATE TABLE IF NOT EXISTS research_sessions(
 id TEXT PRIMARY KEY, run_id TEXT, conversation_id TEXT, query TEXT NOT NULL, mode TEXT NOT NULL,
 question_type TEXT, current_version INTEGER NOT NULL DEFAULT 1, related_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_sessions_updated ON research_sessions(updated_at);
CREATE INDEX IF NOT EXISTS idx_research_sessions_conversation ON research_sessions(conversation_id, updated_at);
CREATE TABLE IF NOT EXISTS research_snapshots(
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL, version INTEGER NOT NULL, report_json TEXT NOT NULL,
 created_at TEXT NOT NULL, UNIQUE(session_id, version),
 FOREIGN KEY(session_id) REFERENCES research_sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_research_snapshots_session ON research_snapshots(session_id, version);
CREATE TABLE IF NOT EXISTS research_sources(
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, source_id TEXT NOT NULL,
 data_json TEXT NOT NULL, created_at TEXT NOT NULL,
 FOREIGN KEY(session_id) REFERENCES research_sessions(id) ON DELETE CASCADE,
 FOREIGN KEY(snapshot_id) REFERENCES research_snapshots(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_research_sources_snapshot ON research_sources(snapshot_id);
CREATE TABLE IF NOT EXISTS research_claims(
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, claim_index INTEGER NOT NULL,
 data_json TEXT NOT NULL, created_at TEXT NOT NULL,
 FOREIGN KEY(session_id) REFERENCES research_sessions(id) ON DELETE CASCADE,
 FOREIGN KEY(snapshot_id) REFERENCES research_snapshots(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_research_claims_snapshot ON research_claims(snapshot_id, claim_index);
CREATE TABLE IF NOT EXISTS research_monitors(
 id TEXT PRIMARY KEY, name TEXT NOT NULL, query TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'quick',
 schedule_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', allowed_domains_json TEXT NOT NULL DEFAULT '[]',
 source_url TEXT,
 notify_on_change INTEGER NOT NULL DEFAULT 1, next_run_at TEXT, last_run_at TEXT, last_snapshot_id TEXT,
 last_fingerprint TEXT, last_change_json TEXT NOT NULL DEFAULT '{}', last_error TEXT,
 retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, error_kind TEXT, recovery_required INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_monitors_due ON research_monitors(status, updated_at);
CREATE TABLE IF NOT EXISTS research_monitor_snapshots(
 id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, research_session_id TEXT, version INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, diff_json TEXT NOT NULL DEFAULT '{}', report_json TEXT NOT NULL,
 created_at TEXT NOT NULL, UNIQUE(monitor_id, version),
 FOREIGN KEY(monitor_id) REFERENCES research_monitors(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_research_monitor_snapshots_monitor ON research_monitor_snapshots(monitor_id, version);
CREATE TABLE IF NOT EXISTS research_monitor_updates(
 id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, snapshot_id TEXT NOT NULL,
 version INTEGER NOT NULL, change_index INTEGER NOT NULL, data_json TEXT NOT NULL DEFAULT '{}',
 status TEXT NOT NULL DEFAULT 'unread', is_favorite INTEGER NOT NULL DEFAULT 0,
 discussion_conversation_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(snapshot_id, change_index),
 FOREIGN KEY(monitor_id) REFERENCES research_monitors(id) ON DELETE CASCADE,
 FOREIGN KEY(snapshot_id) REFERENCES research_monitor_snapshots(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_research_monitor_updates_status ON research_monitor_updates(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_research_monitor_updates_monitor ON research_monitor_updates(monitor_id, created_at);
CREATE TABLE IF NOT EXISTS research_monitor_drafts(
 id TEXT PRIMARY KEY, token TEXT NOT NULL UNIQUE, data_json TEXT NOT NULL,
 expires_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_monitor_drafts_expiry ON research_monitor_drafts(status, expires_at);
CREATE TABLE IF NOT EXISTS memory_versions(
 id TEXT PRIMARY KEY, memory_id TEXT NOT NULL, revision INTEGER NOT NULL,
 data_json TEXT NOT NULL, reason TEXT, created_at TEXT NOT NULL,
 UNIQUE(memory_id, revision),
 FOREIGN KEY(memory_id) REFERENCES memories(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_memory_versions_memory ON memory_versions(memory_id, revision);
CREATE TABLE IF NOT EXISTS connector_webhook_events(
 event_id TEXT PRIMARY KEY, provider TEXT NOT NULL, event_type TEXT,
 payload_json TEXT NOT NULL, received_at TEXT NOT NULL, processed_at TEXT,
 status TEXT NOT NULL, error TEXT
);
CREATE INDEX IF NOT EXISTS idx_connector_webhook_events_provider ON connector_webhook_events(provider, received_at);
CREATE TABLE IF NOT EXISTS system_errors(
 id TEXT PRIMARY KEY, source TEXT NOT NULL, provider TEXT, category TEXT NOT NULL,
 severity TEXT NOT NULL, message TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}',
 status TEXT NOT NULL DEFAULT 'open', first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
 occurrence_count INTEGER NOT NULL DEFAULT 1, resolved_at TEXT,
 acknowledged_at TEXT, cleaned_at TEXT,
 recovery_status TEXT NOT NULL DEFAULT 'unresolved'
);
CREATE INDEX IF NOT EXISTS idx_system_errors_status ON system_errors(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_system_errors_provider ON system_errors(provider, last_seen_at);
CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
"""

SCHEMA_VERSION = 20


class DatabaseAlreadyInUseError(RuntimeError):
    """Raised when another Host already owns the same local database."""


class DatabaseIntegrityError(RuntimeError):
    """Raised when the local SQLite file fails an integrity check."""


class _DatabaseLock:
    """Small cross-platform process lock for the local Host database.

    SQLite serializes individual writes, but it cannot protect the encrypted
    snapshot lifecycle: a second Host could open the same database and remove
    the live file while the first Host was still using it.  The lock is held
    for the whole Host lifetime and is released by the operating system after
    a crash, so a stale ``.lock`` file does not block the next launch.
    """

    _process_handles: dict[str, list[Any]] = {}

    def __init__(self, path: Path):
        self.path = path
        self.handle: Any | None = None
        self._key = str(path.resolve())
        self._shared = False

    def acquire(self) -> None:
        if self.handle is not None:
            return
        # A few in-process recovery paths construct a replacement Application
        # before the old object is garbage-collected. Reuse the process-local
        # handle in that case; the OS-level lock still excludes every other
        # Host process.
        existing = self._process_handles.get(self._key)
        if existing:
            existing[1] += 1
            self.handle = existing[0]
            self._shared = True
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise DatabaseAlreadyInUseError(
                        "The local Host is already using this data directory. Close the other Host or reuse its window."
                    ) from exc
            else:
                import fcntl

                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise DatabaseAlreadyInUseError(
                        "The local Host is already using this data directory. Close the other Host or reuse its window."
                    ) from exc
            self.handle = handle
            self._process_handles[self._key] = [handle, 1]
            stamp = f"pid={os.getpid()}\n"
            handle.seek(0)
            handle.truncate(0)
            handle.write(stamp.encode("ascii", "replace"))
            handle.flush()
        except Exception:
            handle.close()
            raise

    def release(self) -> None:
        handle, self.handle = self.handle, None
        if handle is None:
            return
        entry = self._process_handles.get(self._key)
        if entry and entry[0] is handle:
            entry[1] -= 1
            if entry[1] > 0:
                return
            self._process_handles.pop(self._key, None)
        try:
            if os.name != "nt":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            # msvcrt releases the byte lock when the file handle closes.
        finally:
            handle.close()

    def is_exclusive(self) -> bool:
        entry = self._process_handles.get(self._key)
        return self.handle is not None and bool(entry) and entry[1] <= 1


class Database:
    def __init__(self, path: Path, encryption_store: Any | None = None):
        self.path = path
        self.encryption_store = encryption_store
        self.encrypted_path = path.with_suffix(path.suffix + ".enc")
        self._host_lock = _DatabaseLock(path.with_suffix(path.suffix + ".lock"))

    async def initialize(self) -> None:
        self._host_lock.acquire()
        initialized = False
        try:
            await self._initialize_locked()
            initialized = True
        finally:
            if not initialized:
                self._host_lock.release()

    async def _initialize_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A crash can leave both the live SQLite file and the last encrypted
        # snapshot behind.  Never blindly restore the snapshot in that case:
        # the live database (or its WAL) may contain newer committed data.
        # We only restore when the encrypted snapshot is the newest durable
        # copy, or when no usable database exists at all.
        live_exists = self.path.is_file() and self.path.stat().st_size > 0
        if self.encryption_store and self.encrypted_path.is_file() and not live_exists:
            try:
                restored = self.encryption_store.decrypt(self.encrypted_path.read_bytes())
                temporary = self.path.with_suffix(self.path.suffix + ".restore.tmp")
                temporary.write_bytes(restored)
                try:
                    # A stale WAL/SHM pair without its main database cannot
                    # safely be applied to the restored snapshot.
                    self.path.with_name(self.path.name + "-wal").unlink(missing_ok=True)
                    self.path.with_name(self.path.name + "-shm").unlink(missing_ok=True)
                    self.path.unlink(missing_ok=True)
                    temporary.replace(self.path)
                except PermissionError:
                    # Another host instance still owns the old file. Use it
                    # for this run and leave the encrypted snapshot intact.
                    temporary.unlink(missing_ok=True)
            except Exception as exc:
                raise RuntimeError("encrypted database could not be opened") from exc
        existing = self.path.is_file() and self.path.stat().st_size > 0
        connection = sqlite3.connect(self.path)
        try:
            integrity = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0])
            if integrity.lower() != "ok":
                raise DatabaseIntegrityError(
                    f"The local database failed SQLite integrity check: {integrity[:300]}"
                )
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"Database schema version {version} is newer than this app")
            if existing and version < SCHEMA_VERSION:
                # Snapshot before any schema or FTS rebuild touches existing data.
                stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
                backup = self.path.with_name(f"{self.path.stem}.backup-v{version}-{stamp}.db")
                destination = sqlite3.connect(backup)
                try:
                    connection.backup(destination)
                finally:
                    destination.close()
                if self.encryption_store:
                    encrypted_backup = backup.with_suffix(backup.suffix + ".enc")
                    encrypted_backup.write_bytes(self.encryption_store.encrypt(backup.read_bytes()))
                    try:
                        backup.unlink(missing_ok=True)
                    except PermissionError:
                        # The encrypted backup remains the authoritative copy;
                        # a locked temporary plaintext file is harmless and is
                        # retried on the next start.
                        pass
        finally:
            connection.close()
        async with self.connect() as db:
            await db.executescript(SCHEMA)
            if version < 2:
                await db.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
            if version < 6:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(oauth_states)")}
                if "mode" not in columns:
                    await db.execute("ALTER TABLE oauth_states ADD COLUMN mode TEXT NOT NULL DEFAULT 'real'")
            if version < 9:
                # The memory table predates FTS. Rebuild it from the JSON
                # payload so an upgrade keeps every existing memory searchable.
                await db.execute("DELETE FROM memories_fts")
                rows = await db.execute_fetchall("SELECT id,data_json FROM memories")
                for row in rows:
                    try:
                        payload = json.loads(row[1])
                    except (TypeError, json.JSONDecodeError):
                        payload = {}
                    contacts = payload.get("contacts") or []
                    if isinstance(contacts, list | tuple):
                        contacts = " ".join(str(item) for item in contacts)
                    await db.execute(
                        "INSERT INTO memories_fts(memory_id,title,summary,content,source_text,contacts,project) VALUES(?,?,?,?,?,?,?)",
                        (
                            str(row[0]),
                            str(payload.get("title") or ""),
                            str(payload.get("summary") or ""),
                            str(payload.get("content") or ""),
                            str(payload.get("source_label") or payload.get("source") or ""),
                            str(contacts),
                            str(payload.get("project_id") or payload.get("project") or ""),
                        ),
                    )
            if version < 12:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(research_monitors)")}
                if columns and "next_run_at" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN next_run_at TEXT")
                if columns and "notify_on_change" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN notify_on_change INTEGER NOT NULL DEFAULT 1")
                if columns and "last_error" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN last_error TEXT")
            if version < 13:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(research_monitors)")}
                if columns and "source_url" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN source_url TEXT")
            if version < 15:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(research_monitors)")}
                if columns and "retry_count" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
                if columns and "next_retry_at" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN next_retry_at TEXT")
                if columns and "error_kind" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN error_kind TEXT")
                if columns and "recovery_required" not in columns:
                    await db.execute("ALTER TABLE research_monitors ADD COLUMN recovery_required INTEGER NOT NULL DEFAULT 0")
            if version < 17:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(system_errors)")}
                if columns and "acknowledged_at" not in columns:
                    await db.execute("ALTER TABLE system_errors ADD COLUMN acknowledged_at TEXT")
                if columns and "cleaned_at" not in columns:
                    await db.execute("ALTER TABLE system_errors ADD COLUMN cleaned_at TEXT")
                if columns and "recovery_status" not in columns:
                    await db.execute("ALTER TABLE system_errors ADD COLUMN recovery_status TEXT NOT NULL DEFAULT 'unresolved'")
            if version < 20:
                columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(external_operations)")}
                if "deleted_at" not in columns:
                    await db.execute("ALTER TABLE external_operations ADD COLUMN deleted_at TEXT")
            # External information is a derived workbench view over connector
            # items. It is deliberately separate so inbox state is untouched.
            await db.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(?,?)",
                (SCHEMA_VERSION, datetime.now(UTC).isoformat()),
            )
            await db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            await db.commit()

    def seal_encrypted(self) -> None:
        """Encrypt the SQLite file at rest when the host is not running."""
        try:
            if not self.encryption_store or not self.path.is_file() or not self._host_lock.is_exclusive():
                return
            # Checkpoint WAL first so a raw file read cannot omit committed rows.
            # If another SQLite handle is still closing, the consistent backup
            # below remains the fallback source.
            try:
                connection = sqlite3.connect(self.path, timeout=2)
                try:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                finally:
                    connection.close()
            except (OSError, sqlite3.Error):
                pass
            encrypted = self.encryption_store.encrypt(self.snapshot_bytes())
            temporary = self.encrypted_path.with_suffix(self.encrypted_path.suffix + ".tmp")
            try:
                with temporary.open("wb") as handle:
                    handle.write(encrypted)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.encrypted_path)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
            for _ in range(4):
                try:
                    self.path.unlink(missing_ok=True)
                    # These sidecars are no longer needed after the encrypted
                    # snapshot has been atomically published.
                    self.path.with_name(self.path.name + "-wal").unlink(missing_ok=True)
                    self.path.with_name(self.path.name + "-shm").unlink(missing_ok=True)
                    return
                except PermissionError:
                    # A cancelled async query may briefly retain SQLite's Windows
                    # file handle. Give finalizers a short window to release it.
                    gc.collect()
                    time.sleep(0.05)
        finally:
            self._host_lock.release()

    def release_lock(self) -> None:
        """Release the lifetime lock for unencrypted test or shutdown paths."""
        self._host_lock.release()

    async def claim_external_operation(
        self,
        *,
        idempotency_key: str,
        workflow_id: str,
        tool_name: str,
        request_hash: str,
        arguments: dict[str, Any],
        created_at: str,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically reserve a side effect, or return its durable prior state."""
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "INSERT OR IGNORE INTO external_workflows(id,status,data_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                (workflow_id, "in_progress", json.dumps({"id": workflow_id}, ensure_ascii=False), created_at, created_at),
            )
            row = await (await db.execute("SELECT * FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            claimed = False
            if row is None:
                await db.execute(
                    "INSERT INTO external_operations(idempotency_key,workflow_id,tool_name,request_hash,arguments_json,status,result_json,error,approval_id,started_at,updated_at) "  # noqa: E501
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        idempotency_key,
                        workflow_id,
                        tool_name,
                        request_hash,
                        json.dumps(arguments, ensure_ascii=False),
                        "awaiting_approval",
                        None,
                        None,
                        None,
                        created_at,
                        created_at,
                    ),  # noqa: E501
                )
                row = await (await db.execute("SELECT * FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
                claimed = True
            elif row["request_hash"] != request_hash or row["tool_name"] != tool_name:
                await db.rollback()
                raise ValueError("idempotency key collision for an external operation")
            elif row["status"] == "failed":
                await db.execute(
                    "UPDATE external_operations SET status='awaiting_approval',error=NULL,result_json=NULL,deleted_at=NULL,started_at=?,updated_at=? WHERE idempotency_key=?",  # noqa: E501
                    (created_at, created_at, idempotency_key),
                )
                row = await (await db.execute("SELECT * FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
                claimed = True
            if row["status"] not in {"succeeded", "uncertain", "partial_success", "denied"}:
                await db.execute("UPDATE external_workflows SET status='in_progress',updated_at=? WHERE id=?", (created_at, workflow_id))
            await db.commit()
            return dict(row), claimed

    async def start_external_operation(self, idempotency_key: str, *, updated_at: str) -> dict[str, Any]:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute("SELECT * FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            if row is None:
                await db.rollback()
                raise KeyError("external operation not found")
            status = str(row["status"])
            if status == "awaiting_approval" or status == "approved":
                await db.execute(
                    "UPDATE external_operations SET status='running',updated_at=? WHERE idempotency_key=?", (updated_at, idempotency_key)
                )  # noqa: E501
                await db.execute("UPDATE external_workflows SET status='in_progress',updated_at=? WHERE id=?", (updated_at, row["workflow_id"]))
                row = await (await db.execute("SELECT * FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            elif status == "running":
                await db.rollback()
                raise RuntimeError("This external operation is already in progress")
            await db.commit()
            return dict(row)

    async def update_external_operation(
        self,
        idempotency_key: str,
        *,
        status: str,
        updated_at: str,
        error: str | None = None,
        approval_id: str | None = None,
    ) -> None:
        if status not in {"awaiting_approval", "approved", "denied", "running", "succeeded", "failed", "uncertain", "partial_success"}:
            raise ValueError("invalid external operation status")
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "UPDATE external_operations SET status=?,error=?,approval_id=COALESCE(?,approval_id),updated_at=? WHERE idempotency_key=?",
                (status, error, approval_id, updated_at, idempotency_key),
            )
            row = await (await db.execute("SELECT workflow_id FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            if row:
                await db.execute("UPDATE external_workflows SET status=?,updated_at=? WHERE id=?", (status, updated_at, row["workflow_id"]))
            await db.commit()

    async def finish_external_operation(
        self,
        *,
        idempotency_key: str,
        status: str,
        updated_at: str,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        if status not in {"succeeded", "failed", "uncertain", "partial_success"}:
            raise ValueError("invalid external operation status")
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "UPDATE external_operations SET status=?,result_json=?,error=?,updated_at=? WHERE idempotency_key=?",
                (status, json.dumps(result, ensure_ascii=False) if result is not None else None, error, updated_at, idempotency_key),
            )
            row = await (await db.execute("SELECT workflow_id FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            if row:
                workflow_id = row["workflow_id"]
                rows = await (await db.execute("SELECT idempotency_key,status,arguments_json FROM external_operations WHERE workflow_id=?", (workflow_id,))).fetchall()
                states = [item["status"] for item in self.effective_external_operations(list(rows))]
                if "uncertain" in states or "running" in states:
                    workflow_status = "uncertain" if "uncertain" in states else "in_progress"
                elif "partial_success" in states or ("failed" in states and "succeeded" in states):
                    workflow_status = "partial_success"
                elif "failed" in states:
                    workflow_status = "failed"
                elif "denied" in states and "succeeded" in states:
                    workflow_status = "partial_success"
                elif "denied" in states:
                    workflow_status = "denied"
                else:
                    # A later tool call may still join this run-level workflow.
                    workflow_status = "in_progress"
                await db.execute("UPDATE external_workflows SET status=?,updated_at=? WHERE id=?", (workflow_status, updated_at, workflow_id))
            await db.commit()

    async def set_external_verification(
        self,
        idempotency_key: str,
        *,
        verification: dict[str, Any],
        updated_at: str,
    ) -> None:
        """Persist the last provider-side verification without replaying a write."""
        async with self.connect() as db:
            row = await (await db.execute("SELECT result_json FROM external_operations WHERE idempotency_key=?", (idempotency_key,))).fetchone()
            current: Any = None
            if row and row[0]:
                try:
                    current = json.loads(row[0])
                except (TypeError, json.JSONDecodeError):
                    current = None
            payload = dict(current) if isinstance(current, dict) else {}
            payload["_kore_verification"] = verification
            await db.execute(
                "UPDATE external_operations SET result_json=?,updated_at=? WHERE idempotency_key=?",
                (json.dumps(payload, ensure_ascii=False), updated_at, idempotency_key),
            )
            await db.commit()

    @staticmethod
    def effective_external_operations(rows: list[Any]) -> list[Any]:
        ids = {row["idempotency_key"] for row in rows}
        superseded = set()
        for row in rows:
            try:
                previous = json.loads(row["arguments_json"] or "{}").get("_retry_of")
            except (TypeError, ValueError):
                previous = None
            if previous in ids and previous != row["idempotency_key"]:
                superseded.add(previous)
        return [row for row in rows if row["idempotency_key"] not in superseded]

    async def finish_external_workflow(self, workflow_id: str, *, run_status: str, updated_at: str) -> None:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            rows = await (await db.execute("SELECT idempotency_key,status,arguments_json FROM external_operations WHERE workflow_id=?", (workflow_id,))).fetchall()
            states = [item["status"] for item in self.effective_external_operations(list(rows))]
            if states:
                if "uncertain" in states or "running" in states:
                    status = "uncertain"
                elif "partial_success" in states or ("failed" in states and "succeeded" in states):
                    status = "partial_success"
                elif "denied" in states and "succeeded" in states:
                    status = "partial_success"
                elif "denied" in states:
                    status = "denied"
                elif "failed" in states:
                    status = "failed"
                elif "awaiting_approval" in states or "approved" in states:
                    status = "interrupted"
                elif run_status == "completed":
                    status = "completed"
                elif "succeeded" in states:
                    status = "partial_success"
                else:
                    status = "interrupted"
                await db.execute("UPDATE external_workflows SET status=?,updated_at=? WHERE id=?", (status, updated_at, workflow_id))
            await db.commit()

    async def recover_running_external_operations(self, *, updated_at: str) -> int:
        """Mark side effects interrupted by a host restart as uncertain."""
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "UPDATE external_operations SET status='uncertain',error=COALESCE(error,'host_restart_before_remote_result'),updated_at=? WHERE status='running'",  # noqa: E501
                (updated_at,),
            )
            count = cursor.rowcount
            await db.execute(
                "UPDATE external_workflows SET status='uncertain',updated_at=? WHERE id IN (SELECT workflow_id FROM external_operations WHERE status='uncertain')",  # noqa: E501
                (updated_at,),
            )
            await db.commit()
            return int(count)

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            yield db
        finally:
            await db.close()

    async def fetchone(self, sql: str, args: tuple[Any, ...] = ()) -> aiosqlite.Row | None:
        async with self.connect() as db:
            cursor = await db.execute(sql, args)
            return await cursor.fetchone()

    async def fetchall(self, sql: str, args: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        async with self.connect() as db:
            cursor = await db.execute(sql, args)
            return list(await cursor.fetchall())

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
        async with self.connect() as db:
            await db.execute(sql, args)
            await db.commit()

    async def claim_execution_job(self, job_id: str, *, updated_at: str) -> tuple[dict[str, Any], bool]:
        """Atomically move a pending proactive job to running.

        Scheduler ticks, manual retries, and recovery callbacks can all reach
        ``run_job`` at the same time.  The in-memory running set is only a
        best-effort optimisation, so the durable claim must happen inside a
        SQLite write transaction.  Exactly one caller receives ``claimed``;
        other callers get the current durable job state and must not execute
        its side effect.
        """
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute("SELECT * FROM execution_jobs WHERE id=?", (job_id,))).fetchone()
            if row is None:
                await db.rollback()
                raise KeyError("execution job not found")
            current = json.loads(row["data_json"] or "{}")
            status = str(row["status"])
            if status != "pending":
                await db.commit()
                return current, False
            attempts = int(row["attempts"] or current.get("attempts", 0)) + 1
            current["status"] = "running"
            current["attempts"] = attempts
            current["updated_at"] = updated_at
            current["next_retry_at"] = None
            await db.execute(
                "UPDATE execution_jobs SET data_json=?,status='running',attempts=?,next_retry_at=NULL,updated_at=? WHERE id=? AND status='pending'",
                (json.dumps(current, ensure_ascii=False), attempts, updated_at, job_id),
            )
            await db.commit()
            return current, True

    async def set_json(self, key: str, value: Any) -> None:
        await self.execute(
            "INSERT INTO kv(key,value_json) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
            (key, json.dumps(value, ensure_ascii=False)),
        )

    async def get_json(self, key: str, default: Any = None) -> Any:
        row = await self.fetchone("SELECT value_json FROM kv WHERE key=?", (key,))
        return json.loads(row["value_json"]) if row else default

    async def record_error(
        self,
        *,
        source: str,
        category: str,
        message: str,
        provider: str | None = None,
        severity: str = "error",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Upsert a deduplicated error for the unified error center."""
        stamp = datetime.now(UTC).isoformat()
        normalized = redact_text(str(message).strip(), max_chars=2000)
        safe_details = redact(details or {})
        row = await self.fetchone(
            "SELECT * FROM system_errors WHERE source=? AND category=? AND provider IS ? AND message=? AND status='open'",
            (source, category, provider, normalized),
        )
        if row:
            await self.execute(
                "UPDATE system_errors SET status='open',last_seen_at=?,occurrence_count=occurrence_count+1,details_json=?,"
                "acknowledged_at=NULL,resolved_at=NULL,cleaned_at=NULL,recovery_status='unresolved' WHERE id=?",
                (stamp, json.dumps(safe_details, ensure_ascii=False), row["id"]),
            )
            result = dict(row)
            result.update(
                status="open",
                last_seen_at=stamp,
                occurrence_count=int(row["occurrence_count"]) + 1,
                acknowledged_at=None,
                resolved_at=None,
                cleaned_at=None,
                recovery_status="unresolved",
            )
            return result
        error_id = __import__("uuid").uuid4().hex
        await self.execute(
            "INSERT INTO system_errors(id,source,provider,category,severity,message,details_json,status,first_seen_at,last_seen_at,occurrence_count,resolved_at,acknowledged_at,cleaned_at,recovery_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (error_id, source, provider, category, severity, normalized, json.dumps(safe_details, ensure_ascii=False), "open", stamp, stamp, 1, None, None, None, "unresolved"),
        )
        return {"id": error_id, "source": source, "provider": provider, "category": category, "severity": severity, "message": normalized, "status": "open", "first_seen_at": stamp, "last_seen_at": stamp, "occurrence_count": 1, "recovery_status": "unresolved"}

    async def list_errors(self, *, status: str | None = None, provider: str | None = None, view: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        clauses, args = [], []
        if view == "current":
            clauses.append("status='open'")
        elif view == "history":
            clauses.append("status IN ('resolved','cleaned')")
        elif status:
            clauses.append("status=?"); args.append(status)
        if provider:
            clauses.append("provider=?"); args.append(provider)
        sql = "SELECT * FROM system_errors" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY last_seen_at DESC LIMIT ?"
        args.append(max(1, min(int(limit), 1000)))
        rows = await self.fetchall(sql, tuple(args))
        result = []
        for row in rows:
            item = dict(row)
            try: item["details"] = json.loads(item.pop("details_json") or "{}")
            except json.JSONDecodeError: item["details"] = {}
            result.append(item)
        return result

    async def resolve_error(self, error_id: str) -> bool:
        stamp = datetime.now(UTC).isoformat()
        await self.execute("UPDATE system_errors SET status='resolved',resolved_at=?,recovery_status='resolved_manual' WHERE id=?", (stamp, error_id))
        return bool(await self.fetchone("SELECT 1 FROM system_errors WHERE id=? AND status='resolved'", (error_id,)))

    async def acknowledge_error(self, error_id: str) -> bool:
        stamp = datetime.now(UTC).isoformat()
        await self.execute("UPDATE system_errors SET acknowledged_at=?,recovery_status='acknowledged' WHERE id=? AND status='open'", (stamp, error_id))
        return bool(await self.fetchone("SELECT 1 FROM system_errors WHERE id=? AND status='open' AND acknowledged_at IS NOT NULL", (error_id,)))

    async def cleanup_error(self, error_id: str) -> bool:
        stamp = datetime.now(UTC).isoformat()
        await self.execute("UPDATE system_errors SET status='cleaned',cleaned_at=? WHERE id=? AND status='resolved'", (stamp, error_id))
        return bool(await self.fetchone("SELECT 1 FROM system_errors WHERE id=? AND status='cleaned'", (error_id,)))

    async def cleanup_resolved_errors(self, *, provider: str | None = None) -> int:
        stamp = datetime.now(UTC).isoformat()
        if provider:
            rows = await self.fetchall("SELECT id FROM system_errors WHERE status='resolved' AND provider=?", (provider,))
            await self.execute("UPDATE system_errors SET status='cleaned',cleaned_at=? WHERE status='resolved' AND provider=?", (stamp, provider))
        else:
            rows = await self.fetchall("SELECT id FROM system_errors WHERE status='resolved'")
            await self.execute("UPDATE system_errors SET status='cleaned',cleaned_at=? WHERE status='resolved'", (stamp,))
        return len(rows)

    async def recover_errors(self, *, source: str, provider: str | None = None, category: str | None = None) -> int:
        stamp = datetime.now(UTC).isoformat()
        clauses = ["source=?", "status='open'"]
        args: list[Any] = [source]
        if provider:
            clauses.append("provider=?"); args.append(provider)
        if category:
            clauses.append("category=?"); args.append(category)
        rows = await self.fetchall(f"SELECT id FROM system_errors WHERE {' AND '.join(clauses)}", tuple(args))
        await self.execute(
            f"UPDATE system_errors SET status='resolved',resolved_at=?,recovery_status='recovered' WHERE {' AND '.join(clauses)}",
            (stamp, *args),
        )
        return len(rows)

    async def claim_webhook_event(self, *, event_id: str, provider: str, event_type: str, payload: dict[str, Any], received_at: str) -> tuple[bool, str]:
        """Atomically claim a webhook event so retries cannot run twice."""
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "INSERT OR IGNORE INTO connector_webhook_events(event_id,provider,event_type,payload_json,received_at,status,error) VALUES(?,?,?,?,?,?,?)",
                (event_id, provider, event_type, json.dumps(redact(payload), ensure_ascii=False), received_at, "received", None),
            )
            row = await (await db.execute("SELECT status FROM connector_webhook_events WHERE event_id=?", (event_id,))).fetchone()
            status = str(row["status"] if row else "received")
            if status in {"processed", "processing"}:
                await db.commit()
                return False, status
            await db.execute("UPDATE connector_webhook_events SET status='processing',error=NULL WHERE event_id=?", (event_id,))
            await db.commit()
            return True, "processing"

    def snapshot_bytes(self) -> bytes:
        """Create a consistent SQLite snapshot for encrypted backup export."""
        target = sqlite3.connect(":memory:")
        try:
            source = sqlite3.connect(self.path)
            try:
                source.backup(target)
            finally:
                source.close()
            # SQLite's serialize API is available on supported Python builds.
            return target.serialize()
        finally:
            target.close()
