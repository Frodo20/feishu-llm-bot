from __future__ import annotations

import errno
import fcntl
import json
import os
import secrets
import sqlite3
import stat
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

_SCHEMA_VERSION = 6
_STATUSES = (
    "acquiring",
    "accepted",
    "dispatching",
    "delivered",
    "replying",
    "replied",
    "failed",
)
_PERMISSION_STATUSES = ("pending", "allowed", "denied", "expired")
_REQUIRED_COLUMNS = {
    "sequence",
    "message_id",
    "correlation_id",
    "chat_id",
    "message_type",
    "user_text",
    "status",
    "reply_text",
    "reply_format",
    "reply_chunks",
    "chunks_sent",
    "attempts",
    "updated_at",
    "failure_reason",
    "attachment_token",
    "attachment_mime",
    "attachment_size",
    "attachment_sha256",
    "attachment_width",
    "attachment_height",
    "attachment_cleaned_at",
}


@contextmanager
def _process_lease(path: Path, purpose: str, *, exclusive: bool = True) -> Iterator[None]:
    lease_path = path.with_name(f"{path.name}.{purpose}.lock")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        file_fd = os.open(lease_path, flags, 0o600)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError(f"{purpose} lease must not be a symlink") from exc
        raise
    try:
        lease_stat = os.fstat(file_fd)
        if not stat.S_ISREG(lease_stat.st_mode) or lease_stat.st_uid != os.getuid():
            raise PermissionError(f"{purpose} lease must be a current-user-owned regular file")
        if stat.S_IMODE(lease_stat.st_mode) & 0o077:
            raise PermissionError(f"{purpose} lease must not be accessible by group or others")
        os.fchmod(file_fd, 0o600)
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(file_fd, operation)
        yield
    finally:
        os.close(file_fd)


@contextmanager
def reply_lease(path: Path) -> Iterator[None]:
    """Serialize outbound sends for one database across bridge processes."""
    with _process_lease(path, "reply"):
        yield


@contextmanager
def acquisition_lease(path: Path) -> Iterator[None]:
    """Mark one live acquisition while allowing other acquisitions in sibling processes."""
    with _process_lease(path, "acquisition", exclusive=False):
        yield


@contextmanager
def startup_lease(path: Path) -> Iterator[None]:
    """Exclude startup recovery from image acquisition in every bridge process."""
    with _process_lease(path, "acquisition"):
        yield


@dataclass(frozen=True)
class PermissionRequestRecord:
    request_id: str
    session_id: str
    chat_id: str
    tool_name: str
    summary: str
    token_hash: str
    status: str
    created_at: int
    expires_at: int
    resolved_at: int | None
    resolution_message_id: str | None
    card_message_id: str | None = None


@dataclass(frozen=True)
class EventRecord:
    sequence: int
    message_id: str
    correlation_id: str
    chat_id: str
    message_type: str
    user_text: str | None
    status: str
    reply_text: str | None
    reply_format: str | None
    reply_chunks: tuple[str, ...] | None
    chunks_sent: int
    attempts: int
    failure_reason: str | None
    attachment_token: str | None
    attachment_mime: str | None
    attachment_size: int | None
    attachment_sha256: str | None
    attachment_width: int | None
    attachment_height: int | None
    attachment_cleaned_at: int | None


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.parent.chmod(0o700)
        self._connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._migrate()
        self.path.chmod(0o600)

    def _migrate(self) -> None:
        with self._lock:
            row = self._connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'"
            ).fetchone()
            if row is None:
                self._connection.execute("BEGIN IMMEDIATE")
                try:
                    self._create_events_table()
                    self._create_events_indexes()
                    self._create_permission_requests_table()
                    self._connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                    self._connection.execute("COMMIT")
                except Exception:
                    self._connection.execute("ROLLBACK")
                    raise
                return
            else:
                columns = {
                    item["name"]
                    for item in self._connection.execute("PRAGMA table_info(events)").fetchall()
                }
                schema_version = self._connection.execute("PRAGMA user_version").fetchone()[0]
                if schema_version > _SCHEMA_VERSION:
                    raise RuntimeError("database schema is newer than this bridge version")
                table_sql = row["sql"] or ""
                needs_rebuild = not _REQUIRED_COLUMNS.issubset(columns) or not all(
                    marker in table_sql
                    for marker in (
                        "'acquiring'",
                        "'image'",
                        "'card_v1'",
                        "CHECK (chunks_sent >= 0)",
                        "json_array_length(reply_chunks)",
                    )
                )
                if needs_rebuild:
                    self._rebuild_events(columns, row["sql"] or "")
                    return
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._create_events_indexes()
                self._create_permission_requests_table()
                self._connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    def _rebuild_events(self, columns: set[str], table_sql: str) -> None:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            previous_exists = self._connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events_previous'"
            ).fetchone()
            if previous_exists is not None:
                raise RuntimeError(
                    "incomplete previous database migration requires operator review"
                )
            self._connection.execute("ALTER TABLE events RENAME TO events_previous")
            self._connection.execute("DROP INDEX IF EXISTS events_status_sequence")
            self._create_events_table()

            def source(name: str, fallback: str = "NULL") -> str:
                return name if name in columns else fallback

            correlation = source("correlation_id", "'fs_' || lower(hex(randomblob(16)))")
            source_status = source("status", "'failed'")
            status = f"""
                CASE {source_status}
                    WHEN 'pending' THEN 'accepted'
                    WHEN 'processing' THEN 'delivered'
                    WHEN 'acquiring' THEN 'acquiring'
                    WHEN 'accepted' THEN 'accepted'
                    WHEN 'dispatching' THEN 'dispatching'
                    WHEN 'delivered' THEN 'delivered'
                    WHEN 'replying' THEN 'replying'
                    WHEN 'replied' THEN 'replied'
                    WHEN 'failed' THEN 'failed'
                    ELSE 'failed'
                END
            """
            message_type = f"""
                CASE
                    WHEN {source("message_type", "'text'")} = 'image' THEN 'image'
                    ELSE 'text'
                END
            """
            reply_format = f"""
                CASE
                    WHEN {source("reply_format")} IN ('plain_v1', 'post_v2', 'card_v1')
                        THEN {source("reply_format")}
                    WHEN {source("reply_text")} IS NOT NULL OR {source("chunks_sent", "0")} > 0
                        THEN 'plain_v1'
                    ELSE NULL
                END
            """
            reply_chunks = source("reply_chunks")
            self._connection.execute(
                f"""
                INSERT INTO events(
                    sequence, message_id, correlation_id, chat_id, message_type, user_text,
                    status, reply_text, reply_format, reply_chunks, chunks_sent, attempts,
                    updated_at, failure_reason, attachment_token, attachment_mime, attachment_size,
                    attachment_sha256, attachment_width, attachment_height,
                    attachment_cleaned_at
                )
                SELECT {source("sequence")}, {source("message_id")}, {correlation},
                       {source("chat_id")}, {message_type}, {source("user_text")},
                       {status}, {source("reply_text")}, {reply_format}, {reply_chunks},
                       {source("chunks_sent", "0")}, {source("attempts", "0")},
                       {source("updated_at", "0")}, {source("failure_reason")},
                       {source("attachment_token")}, {source("attachment_mime")},
                       {source("attachment_size")}, {source("attachment_sha256")},
                       {source("attachment_width")}, {source("attachment_height")},
                       {source("attachment_cleaned_at")}
                FROM events_previous
                """
            )
            self._connection.execute("DROP TABLE events_previous")
            self._create_events_indexes()
            self._create_permission_requests_table()
            self._connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            self._connection.execute("COMMIT")
        except Exception:
            self._connection.execute("ROLLBACK")
            raise

    def _create_events_table(self) -> None:
        statuses = ", ".join(repr(status) for status in _STATUSES)
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                correlation_id TEXT NOT NULL UNIQUE,
                chat_id TEXT NOT NULL,
                message_type TEXT NOT NULL CHECK (message_type IN ('text', 'image')),
                user_text TEXT,
                status TEXT NOT NULL CHECK (status IN ({statuses})),
                reply_text TEXT,
                reply_format TEXT CHECK (reply_format IN ('plain_v1', 'post_v2', 'card_v1')),
                reply_chunks TEXT CHECK (
                    reply_chunks IS NULL
                    OR (json_valid(reply_chunks)
                        AND json_type(reply_chunks) = 'array'
                        AND json_array_length(reply_chunks) > 0)
                ),
                chunks_sent INTEGER NOT NULL DEFAULT 0 CHECK (chunks_sent >= 0),
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                failure_reason TEXT,
                attachment_token TEXT UNIQUE,
                attachment_mime TEXT,
                attachment_size INTEGER,
                attachment_sha256 TEXT,
                attachment_width INTEGER,
                attachment_height INTEGER,
                attachment_cleaned_at INTEGER,
                CHECK (
                    (message_type = 'text' AND user_text IS NOT NULL
                     AND attachment_token IS NULL AND attachment_mime IS NULL
                     AND attachment_size IS NULL AND attachment_sha256 IS NULL
                     AND attachment_width IS NULL AND attachment_height IS NULL)
                    OR message_type = 'image'
                ),
                CHECK (
                    reply_chunks IS NULL
                    OR chunks_sent <= json_array_length(reply_chunks)
                )
            )
            """
        )

    def _create_events_indexes(self) -> None:
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS events_status_sequence ON events(status, sequence)"
        )

    def _create_permission_requests_table(self) -> None:
        statuses = ", ".join(repr(status) for status in _PERMISSION_STATUSES)
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS permission_requests (
                request_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                summary TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE CHECK (
                    length(token_hash) = 64 AND token_hash = lower(token_hash)
                ),
                status TEXT NOT NULL CHECK (status IN ({statuses})),
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL CHECK (expires_at > created_at),
                resolved_at INTEGER,
                resolution_message_id TEXT UNIQUE,
                card_message_id TEXT,
                CHECK (
                    (status = 'pending' AND resolved_at IS NULL
                     AND resolution_message_id IS NULL)
                    OR (status != 'pending' AND resolved_at IS NOT NULL)
                )
            )
            """
        )
        permission_columns = {
            row["name"]
            for row in self._connection.execute("PRAGMA table_info(permission_requests)")
        }
        if "card_message_id" not in permission_columns:
            self._connection.execute(
                "ALTER TABLE permission_requests ADD COLUMN card_message_id TEXT"
            )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS permission_requests_status_expiry
            ON permission_requests(status, expires_at)
            """
        )

    @staticmethod
    def _new_correlation_id() -> str:
        return f"fs_{secrets.token_hex(16)}"

    @staticmethod
    def _new_attachment_token() -> str:
        return f"att_{secrets.token_hex(16)}"

    @staticmethod
    def _decode_reply_chunks(value: object) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise RuntimeError("stored reply chunk plan is invalid")
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError("stored reply chunk plan is invalid") from exc
        if (
            not isinstance(decoded, list)
            or not decoded
            or any(not isinstance(chunk, str) or not chunk for chunk in decoded)
        ):
            raise RuntimeError("stored reply chunk plan is invalid")
        return tuple(decoded)

    @staticmethod
    def _validated_reply_chunks(row: sqlite3.Row) -> tuple[str, ...] | None:
        chunks = Store._decode_reply_chunks(row["reply_chunks"])
        chunks_sent = row["chunks_sent"]
        if (
            isinstance(chunks_sent, bool)
            or not isinstance(chunks_sent, int)
            or chunks_sent < 0
            or (chunks is not None and chunks_sent > len(chunks))
        ):
            raise RuntimeError("stored reply chunk checkpoint is invalid")
        return chunks

    @staticmethod
    def _record(row: sqlite3.Row) -> EventRecord:
        return EventRecord(
            sequence=row["sequence"],
            message_id=row["message_id"],
            correlation_id=row["correlation_id"],
            chat_id=row["chat_id"],
            message_type=row["message_type"],
            user_text=row["user_text"],
            status=row["status"],
            reply_text=row["reply_text"],
            reply_format=row["reply_format"],
            reply_chunks=Store._validated_reply_chunks(row),
            chunks_sent=row["chunks_sent"],
            attempts=row["attempts"],
            failure_reason=row["failure_reason"],
            attachment_token=row["attachment_token"],
            attachment_mime=row["attachment_mime"],
            attachment_size=row["attachment_size"],
            attachment_sha256=row["attachment_sha256"],
            attachment_width=row["attachment_width"],
            attachment_height=row["attachment_height"],
            attachment_cleaned_at=row["attachment_cleaned_at"],
        )

    @staticmethod
    def _select_columns() -> str:
        return """
            sequence, message_id, correlation_id, chat_id, message_type, user_text,
            status, reply_text, reply_format, reply_chunks, chunks_sent, attempts,
            failure_reason,
            attachment_token, attachment_mime, attachment_size, attachment_sha256,
            attachment_width, attachment_height, attachment_cleaned_at
        """

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def accept_event(self, message_id: str, chat_id: str, user_text: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO events(
                    message_id, correlation_id, chat_id, message_type, user_text,
                    status, updated_at
                ) VALUES (?, ?, ?, 'text', ?, 'accepted', ?)
                """,
                (message_id, self._new_correlation_id(), chat_id, user_text, int(time.time())),
            )
            return cursor.rowcount == 1

    def reserve_image(
        self, message_id: str, chat_id: str, user_text: str | None = None,
    ) -> EventRecord | None:
        correlation_id = self._new_correlation_id()
        attachment_token = self._new_attachment_token()
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO events(
                    message_id, correlation_id, chat_id, message_type, user_text,
                    status, updated_at, attachment_token
                ) VALUES (?, ?, ?, 'image', ?, 'acquiring', ?, ?)
                """,
                (message_id, correlation_id, chat_id, user_text, int(time.time()),
                 attachment_token),
            )
            if cursor.rowcount != 1:
                return None
            return self.get_by_correlation(correlation_id)

    def finish_image_acquisition(
        self,
        correlation_id: str,
        *,
        mime_type: str,
        byte_size: int,
        sha256: str,
        width: int,
        height: int,
    ) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events
                SET status = 'accepted', attachment_mime = ?, attachment_size = ?,
                    attachment_sha256 = ?, attachment_width = ?, attachment_height = ?,
                    updated_at = ?
                WHERE correlation_id = ? AND message_type = 'image' AND status = 'acquiring'
                """,
                (
                    mime_type,
                    byte_size,
                    sha256,
                    width,
                    height,
                    int(time.time()),
                    correlation_id,
                ),
            )
            return cursor.rowcount == 1

    def claim_next_accepted(self) -> EventRecord | None:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    f"""
                    SELECT {self._select_columns()}
                    FROM events
                    WHERE status = 'accepted'
                      AND NOT EXISTS (
                          SELECT 1 FROM events AS active
                          WHERE active.status IN (
                              'acquiring', 'dispatching', 'delivered', 'replying'
                          )
                      )
                    ORDER BY sequence
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    self._connection.execute("COMMIT")
                    return None
                self._connection.execute(
                    """
                    UPDATE events
                    SET status = 'dispatching', attempts = attempts + 1, updated_at = ?
                    WHERE correlation_id = ? AND status = 'accepted'
                    """,
                    (int(time.time()), row["correlation_id"]),
                )
                claimed = self._connection.execute(
                    f"SELECT {self._select_columns()} FROM events WHERE correlation_id = ?",
                    (row["correlation_id"],),
                ).fetchone()
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self._record(claimed)

    def mark_delivered(self, correlation_id: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET status = 'delivered', updated_at = ?
                WHERE correlation_id = ? AND status = 'dispatching'
                """,
                (int(time.time()), correlation_id),
            )
            return cursor.rowcount == 1

    def get_by_correlation(self, correlation_id: str) -> EventRecord | None:
        with self._lock:
            row = self._connection.execute(
                f"SELECT {self._select_columns()} FROM events WHERE correlation_id = ?",
                (correlation_id,),
            ).fetchone()
            return None if row is None else self._record(row)

    def begin_reply(
        self,
        correlation_id: str,
        reply_text: str,
        reply_format: str = "post_v2",
        reply_chunks: Sequence[str] | None = None,
    ) -> EventRecord:
        if reply_format not in {"plain_v1", "post_v2", "card_v1"}:
            raise ValueError("invalid reply format")
        encoded_chunks: str | None = None
        if reply_chunks is not None:
            if not reply_chunks or any(
                not isinstance(chunk, str) or not chunk for chunk in reply_chunks
            ):
                raise ValueError("reply_chunks must contain non-empty strings")
            encoded_chunks = json.dumps(
                list(reply_chunks), ensure_ascii=False, separators=(",", ":")
            )
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    f"SELECT {self._select_columns()} FROM events WHERE correlation_id = ?",
                    (correlation_id,),
                ).fetchone()
                if row is None:
                    raise ValueError("unknown correlation_id")
                if row["status"] == "replied":
                    self._connection.execute("COMMIT")
                    return self._record(row)
                if row["status"] not in {"dispatching", "delivered", "replying"}:
                    raise ValueError(f"correlation is not replyable (status={row['status']})")
                if row["reply_text"] is not None and row["reply_text"] != reply_text:
                    raise ValueError("correlation already has a different reply")
                existing_chunks = self._decode_reply_chunks(row["reply_chunks"])
                if (
                    existing_chunks is not None
                    and reply_chunks is not None
                    and existing_chunks != tuple(reply_chunks)
                ):
                    raise ValueError("correlation already has a different reply chunk plan")
                # Pre-v3 partial replies have only an ordinal checkpoint. Bootstrap
                # once with the caller's current splitter, then persist that plan.
                # Earlier chunk boundaries cannot be reconstructed more precisely.
                if (
                    row["reply_chunks"] is None
                    and row["chunks_sent"] > 0
                    and (encoded_chunks is None or row["chunks_sent"] > len(reply_chunks or ()))
                ):
                    raise ValueError("legacy partial reply requires a compatible chunk plan")
                effective_format = row["reply_format"] or reply_format
                self._connection.execute(
                    """
                    UPDATE events
                    SET reply_text = COALESCE(reply_text, ?),
                        reply_format = COALESCE(reply_format, ?),
                        reply_chunks = COALESCE(reply_chunks, ?), status = 'replying',
                        updated_at = ?
                    WHERE correlation_id = ?
                    """,
                    (
                        reply_text,
                        effective_format,
                        encoded_chunks,
                        int(time.time()),
                        correlation_id,
                    ),
                )
                updated = self._connection.execute(
                    f"SELECT {self._select_columns()} FROM events WHERE correlation_id = ?",
                    (correlation_id,),
                ).fetchone()
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self._record(updated)

    def mark_chunk_sent(self, correlation_id: str, chunks_sent: int) -> None:
        if isinstance(chunks_sent, bool) or not isinstance(chunks_sent, int) or chunks_sent <= 0:
            raise ValueError("chunks_sent must be a positive integer")
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET chunks_sent = ?, updated_at = ?
                WHERE correlation_id = ? AND status = 'replying' AND chunks_sent = ?
                  AND reply_chunks IS NOT NULL
                  AND ? <= json_array_length(reply_chunks)
                """,
                (
                    chunks_sent,
                    int(time.time()),
                    correlation_id,
                    chunks_sent - 1,
                    chunks_sent,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("reply chunk checkpoint state changed unexpectedly")

    def finish_reply(self, correlation_id: str) -> None:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET status = 'replied', updated_at = ?
                WHERE correlation_id = ? AND status = 'replying'
                  AND reply_chunks IS NOT NULL
                  AND chunks_sent = json_array_length(reply_chunks)
                """,
                (int(time.time()), correlation_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("reply completion state changed unexpectedly")

    def mark_failed(self, correlation_id: str, reason: str | None = None) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET status = 'failed', failure_reason = ?, updated_at = ?
                WHERE correlation_id = ? AND status != 'replied'
                """,
                (reason, int(time.time()), correlation_id),
            )
            return cursor.rowcount == 1

    def mark_attachment_cleaned(self, correlation_id: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET attachment_cleaned_at = ?, updated_at = ?
                WHERE correlation_id = ? AND message_type = 'image'
                  AND status IN ('replied', 'failed') AND attachment_cleaned_at IS NULL
                """,
                (int(time.time()), int(time.time()), correlation_id),
            )
            return cursor.rowcount == 1

    def list_attachment_cleanup_pending(self) -> list[EventRecord]:
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT {self._select_columns()} FROM events
                WHERE message_type = 'image' AND status IN ('replied', 'failed')
                  AND attachment_token IS NOT NULL AND attachment_cleaned_at IS NULL
                ORDER BY sequence
                """
            ).fetchall()
            return [self._record(row) for row in rows]

    def list_accepted_images(self) -> list[EventRecord]:
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT {self._select_columns()} FROM events
                WHERE message_type = 'image' AND status = 'accepted'
                ORDER BY sequence
                """
            ).fetchall()
            return [self._record(row) for row in rows]

    def fail_interrupted_deliveries(self) -> int:
        """Release a resident restart's ambiguous work without replaying actions."""
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events SET status = 'failed',
                    failure_reason = 'resident_restart_interrupted', updated_at = ?
                WHERE status IN ('dispatching', 'delivered', 'replying')
                  AND NOT (status = 'replying' AND COALESCE(reply_format, '') = 'card_v1'
                           AND reply_text IS NOT NULL)
                """,
                (int(time.time()),),
            )
            return cursor.rowcount

    def pending_card_replies(self) -> list[EventRecord]:
        with self._lock:
            rows = self._connection.execute(
                f"""SELECT {self._select_columns()} FROM events
                    WHERE status='replying' AND reply_format='card_v1' ORDER BY sequence""",
            ).fetchall()
            return [self._record(row) for row in rows]

    def fail_interrupted_acquisitions(self) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE events
                SET status = 'failed', failure_reason = 'interrupted_image_acquisition',
                    updated_at = ?
                WHERE status = 'acquiring'
                """,
                (int(time.time()),),
            )
            return cursor.rowcount

    def mark_sequences_failed(self, sequences: tuple[int, ...], reason: str) -> int:
        if not sequences:
            return 0
        placeholders = ",".join("?" for _ in sequences)
        with self._lock:
            cursor = self._connection.execute(
                f"""
                UPDATE events SET status = 'failed', failure_reason = ?, updated_at = ?
                WHERE sequence IN ({placeholders}) AND status = 'delivered'
                """,
                (reason, int(time.time()), *sequences),
            )
            return cursor.rowcount

    @staticmethod
    def _permission_record(row: sqlite3.Row) -> PermissionRequestRecord:
        return PermissionRequestRecord(
            request_id=row["request_id"],
            session_id=row["session_id"],
            chat_id=row["chat_id"],
            tool_name=row["tool_name"],
            summary=row["summary"],
            token_hash=row["token_hash"],
            status=row["status"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            resolved_at=row["resolved_at"],
            resolution_message_id=row["resolution_message_id"],
            card_message_id=row["card_message_id"],
        )

    def create_permission_request(
        self,
        *,
        request_id: str,
        session_id: str,
        chat_id: str,
        tool_name: str,
        summary: str,
        token_hash: str,
        created_at: int,
        expires_at: int,
        max_pending: int,
    ) -> PermissionRequestRecord:
        values = (request_id, session_id, chat_id, tool_name, summary)
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("permission request string fields must not be empty")
        if (
            not isinstance(token_hash, str)
            or len(token_hash) != 64
            or any(character not in "0123456789abcdef" for character in token_hash)
        ):
            raise ValueError("permission request token_hash must be lowercase SHA-256 hex")
        if (
            isinstance(created_at, bool)
            or not isinstance(created_at, int)
            or isinstance(expires_at, bool)
            or not isinstance(expires_at, int)
            or expires_at <= created_at
        ):
            raise ValueError("permission request expiry must be after creation")
        if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._connection.execute(
                    """
                    UPDATE permission_requests
                    SET status = 'expired', resolved_at = ?
                    WHERE status = 'pending' AND expires_at <= ?
                    """,
                    (created_at, created_at),
                )
                pending = self._connection.execute(
                    "SELECT COUNT(*) FROM permission_requests WHERE status = 'pending'"
                ).fetchone()[0]
                if pending >= max_pending:
                    raise RuntimeError("too many pending permission requests")
                self._connection.execute(
                    """
                    INSERT INTO permission_requests(
                        request_id, session_id, chat_id, tool_name, summary, token_hash,
                        status, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        request_id,
                        session_id,
                        chat_id,
                        tool_name,
                        summary,
                        token_hash,
                        created_at,
                        expires_at,
                    ),
                )
                row = self._connection.execute(
                    "SELECT * FROM permission_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self._permission_record(row)

    def bind_permission_card(self, request_id: str, message_id: str) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE permission_requests SET card_message_id = ? WHERE request_id = ?",
                (message_id, request_id),
            )

    def resolve_permission_card(
        self, *, request_id: str, token_hash: str, chat_id: str, message_id: str,
        event_id: str, decision: str, now: int,
    ) -> tuple[PermissionRequestRecord | None, bool]:
        """Bounded transaction for Feishu's 3-second callback deadline.

        Separate connection avoids waiting on a worker's Python lock. Identity,
        card message, expiry and single-use state are checked in one transaction.
        """
        if decision not in {"allowed", "denied"}:
            raise ValueError("invalid card decision")
        connection = sqlite3.connect(self.path, timeout=0.25, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM permission_requests WHERE request_id = ? AND token_hash = ?
                   AND chat_id = ? AND card_message_id = ?""",
                (request_id, token_hash, chat_id, message_id),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None, False
            changed = row["status"] == "pending" and row["expires_at"] > now
            if row["status"] == "pending":
                connection.execute(
                    """UPDATE permission_requests
                       SET status = ?, resolved_at = ?, resolution_message_id = ?
                       WHERE request_id = ?""",
                    (decision if changed else "expired", now,
                     f"card:{event_id}" if changed else None, request_id),
                )
                row = connection.execute(
                    "SELECT * FROM permission_requests WHERE request_id = ?", (request_id,),
                ).fetchone()
            connection.execute("COMMIT")
            return self._permission_record(row), changed
        finally:
            connection.close()

    def get_permission_request(self, request_id: str) -> PermissionRequestRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM permission_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return None if row is None else self._permission_record(row)

    def resolve_permission_request(
        self,
        *,
        token_hash: str,
        chat_id: str,
        resolution_message_id: str,
        decision: str,
        now: int,
    ) -> PermissionRequestRecord | None:
        if decision not in {"allowed", "denied"}:
            raise ValueError("permission decision must be allowed or denied")
        if not chat_id or not resolution_message_id:
            raise ValueError("permission resolution identifiers must not be empty")
        if (
            not isinstance(token_hash, str)
            or len(token_hash) != 64
            or any(character not in "0123456789abcdef" for character in token_hash)
        ):
            return None
        if isinstance(now, bool) or not isinstance(now, int):
            raise ValueError("permission resolution time must be an integer")

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._connection.execute(
                    """
                    UPDATE permission_requests
                    SET status = 'expired', resolved_at = ?
                    WHERE status = 'pending' AND expires_at <= ?
                    """,
                    (now, now),
                )
                duplicate_message = self._connection.execute(
                    """
                    SELECT 1 FROM permission_requests
                    WHERE resolution_message_id = ?
                    """,
                    (resolution_message_id,),
                ).fetchone()
                if duplicate_message is not None:
                    self._connection.execute("COMMIT")
                    return None
                cursor = self._connection.execute(
                    """
                    UPDATE permission_requests
                    SET status = ?, resolved_at = ?, resolution_message_id = ?
                    WHERE token_hash = ? AND chat_id = ? AND status = 'pending'
                      AND expires_at > ?
                    """,
                    (
                        decision,
                        now,
                        resolution_message_id,
                        token_hash,
                        chat_id,
                        now,
                    ),
                )
                if cursor.rowcount != 1:
                    self._connection.execute("COMMIT")
                    return None
                row = self._connection.execute(
                    "SELECT * FROM permission_requests WHERE token_hash = ?",
                    (token_hash,),
                ).fetchone()
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self._permission_record(row)

    def expire_permission_requests(self, *, now: int) -> int:
        if isinstance(now, bool) or not isinstance(now, int):
            raise ValueError("permission expiry time must be an integer")
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE permission_requests SET status = 'expired', resolved_at = ?
                WHERE status = 'pending' AND expires_at <= ?
                """,
                (now, now),
            )
            return cursor.rowcount

    def expire_all_pending_permission_requests(self, *, now: int) -> int:
        if isinstance(now, bool) or not isinstance(now, int):
            raise ValueError("permission expiry time must be an integer")
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE permission_requests SET status = 'expired', resolved_at = ?
                WHERE status = 'pending'
                """,
                (now,),
            )
            return cursor.rowcount

    def status_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT status, COUNT(*) AS count FROM events GROUP BY status"
            ).fetchall()
        return {row["status"]: row["count"] for row in rows}

    def prune_events(self, *, older_than_seconds: int = 7 * 24 * 3600) -> int:
        cutoff = int(time.time()) - older_than_seconds
        with self._lock:
            cursor = self._connection.execute(
                """
                DELETE FROM events
                WHERE updated_at < ? AND status IN ('replied', 'failed')
                  AND (message_type = 'text' OR attachment_cleaned_at IS NOT NULL)
                """,
                (cutoff,),
            )
            return cursor.rowcount
