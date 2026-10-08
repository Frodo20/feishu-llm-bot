import sqlite3
from pathlib import Path

import pytest

from feishu_llm_bot.store import Store


def make_store(path: Path) -> Store:
    return Store(path)


def accept_claim_and_deliver(store: Store, message_id: str = "m1"):
    assert store.accept_event(message_id, "c1", "hello")
    event = store.claim_next_accepted()
    assert event is not None
    assert event.status == "dispatching"
    assert store.mark_delivered(event.correlation_id)
    return event


def test_event_is_deduplicated(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    assert store.accept_event("m1", "c1", "hello")
    assert not store.accept_event("m1", "c1", "hello")
    event = store.claim_next_accepted()
    assert event and event.message_id == "m1"
    assert event.correlation_id.startswith("fs_")
    assert event.attempts == 1
    store.close()


def test_dispatching_event_is_not_replayed_after_reopen(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    store = make_store(path)
    assert store.accept_event("m1", "c1", "hello")
    event = store.claim_next_accepted()
    assert event is not None
    store.close()

    reopened = make_store(path)
    assert reopened.claim_next_accepted() is None
    saved = reopened.get_by_correlation(event.correlation_id)
    assert saved and saved.status == "dispatching"
    reopened.close()


def test_delivered_event_is_not_replayed_after_reopen(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    store = make_store(path)
    event = accept_claim_and_deliver(store)
    store.close()

    reopened = make_store(path)
    assert reopened.claim_next_accepted() is None
    saved = reopened.get_by_correlation(event.correlation_id)
    assert saved and saved.status == "delivered"
    reopened.close()


def test_active_event_blocks_next_claim_until_reply_finishes(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    first = accept_claim_and_deliver(store)
    assert store.accept_event("m2", "c1", "second")
    assert store.claim_next_accepted() is None

    store.begin_reply(first.correlation_id, "answer", reply_chunks=["answer"])
    assert store.claim_next_accepted() is None
    store.mark_chunk_sent(first.correlation_id, 1)
    store.finish_reply(first.correlation_id)

    second = store.claim_next_accepted()
    assert second is not None
    assert second.message_id == "m2"
    store.close()


def test_reply_is_idempotent_and_text_is_immutable(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    replying = store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    assert replying.status == "replying"
    store.mark_chunk_sent(event.correlation_id, 1)
    store.finish_reply(event.correlation_id)

    completed = store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    assert completed.status == "replied"
    assert completed.chunks_sent == 1
    store.close()


def test_different_retry_reply_is_rejected(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store.begin_reply(event.correlation_id, "answer one", reply_chunks=["answer one"])
    with pytest.raises(ValueError, match="different reply"):
        store.begin_reply(event.correlation_id, "answer two", reply_chunks=["answer two"])
    store.close()


def test_existing_database_is_migrated_without_replaying_processing_event(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            chat_id TEXT NOT NULL,
            user_text TEXT NOT NULL,
            status TEXT NOT NULL,
            reply_text TEXT,
            chunks_sent INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            persist_turn INTEGER NOT NULL DEFAULT 1,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO events(message_id, chat_id, user_text, status, updated_at)
        VALUES ('old', 'c1', 'already sent to the model', 'processing', 1);
        """
    )
    connection.close()

    store = make_store(path)
    assert store.claim_next_accepted() is None
    row = store._connection.execute(  # noqa: SLF001 - migration assertion
        "SELECT correlation_id, status FROM events WHERE message_id = 'old'"
    ).fetchone()
    assert row["correlation_id"].startswith("fs_")
    assert row["status"] == "delivered"
    store.close()


def test_accepted_event_from_previous_process_is_claimed_after_reopen(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    store = make_store(path)
    assert store.accept_event("m1", "c1", "not yet emitted")
    store.close()

    reopened = make_store(path)
    claimed = reopened.claim_next_accepted()
    assert claimed is not None
    assert claimed.message_id == "m1"
    assert claimed.status == "dispatching"
    reopened.close()


def test_failed_event_retains_reason_and_status_counts(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    assert store.mark_failed(event.correlation_id, "blocked legacy channel delivery")
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None
    assert saved.status == "failed"
    assert saved.failure_reason == "blocked legacy channel delivery"
    assert store.status_counts() == {"failed": 1}
    store.close()


def test_known_channel_deliveries_can_be_failed_without_replay(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)

    assert store.mark_sequences_failed((event.sequence,), "provider blocked channel delivery") == 1
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None
    assert saved.status == "failed"
    assert saved.failure_reason == "provider blocked channel delivery"
    assert store.mark_sequences_failed((event.sequence,), "again") == 0
    assert store.claim_next_accepted() is None
    store.close()


def test_partial_legacy_reply_keeps_plain_format_after_migration(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            correlation_id TEXT NOT NULL UNIQUE,
            chat_id TEXT NOT NULL,
            user_text TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN (
                    'accepted', 'dispatching', 'delivered',
                    'replying', 'replied', 'failed'
                )
            ),
            reply_text TEXT,
            chunks_sent INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            failure_reason TEXT
        );
        INSERT INTO events(
            message_id, correlation_id, chat_id, user_text, status,
            reply_text, chunks_sent, updated_at
        ) VALUES ('old', 'fs_old', 'c1', 'prompt', 'replying', 'abcdefgh', 1, 1);
        """
    )
    connection.close()

    store = make_store(path)
    saved = store.get_by_correlation("fs_old")
    assert saved is not None
    assert saved.status == "replying"
    assert saved.reply_format == "plain_v1"
    assert saved.chunks_sent == 1
    resumed = store.begin_reply(
        "fs_old",
        "abcdefgh",
        "post_v2",
        ["abcd", "efgh"],
    )
    assert resumed.reply_format == "plain_v1"
    assert resumed.reply_chunks == ("abcd", "efgh")
    store.close()


def test_rebuild_preserves_image_rows_and_attachment_metadata(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            correlation_id TEXT NOT NULL UNIQUE,
            chat_id TEXT NOT NULL,
            message_type TEXT NOT NULL CHECK (message_type IN ('text', 'image')),
            user_text TEXT,
            status TEXT NOT NULL CHECK (status IN (
                'acquiring', 'accepted', 'dispatching', 'delivered',
                'replying', 'replied', 'failed'
            )),
            reply_text TEXT,
            reply_format TEXT CHECK (reply_format IN ('plain_v1', 'post_v2')),
            chunks_sent INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            attachment_token TEXT UNIQUE,
            attachment_mime TEXT,
            attachment_size INTEGER,
            attachment_sha256 TEXT,
            attachment_width INTEGER,
            attachment_height INTEGER,
            attachment_cleaned_at INTEGER
        );
        INSERT INTO events(
            message_id, correlation_id, chat_id, message_type, user_text, status,
            updated_at, attachment_token, attachment_mime, attachment_size,
            attachment_sha256, attachment_width, attachment_height
        ) VALUES (
            'image-old', 'fs_image_old', 'c1', 'image', NULL, 'delivered', 1,
            'att_0123456789abcdef0123456789abcdef', 'image/png', 68,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1
        );
        """
    )
    connection.execute("PRAGMA user_version = 1")
    connection.close()

    store = make_store(path)
    saved = store.get_by_correlation("fs_image_old")
    assert saved is not None
    assert saved.message_type == "image"
    assert saved.user_text is None
    assert saved.status == "delivered"
    assert saved.attachment_token == "att_0123456789abcdef0123456789abcdef"
    assert saved.attachment_mime == "image/png"
    assert saved.attachment_size == 68
    assert saved.attachment_width == 1
    assert saved.attachment_height == 1
    assert saved.failure_reason is None
    store.close()


def test_correlated_processing_status_is_mapped_during_rebuild(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            correlation_id TEXT NOT NULL UNIQUE,
            chat_id TEXT NOT NULL,
            user_text TEXT NOT NULL,
            status TEXT NOT NULL,
            reply_text TEXT,
            chunks_sent INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO events(
            message_id, correlation_id, chat_id, user_text, status, updated_at
        ) VALUES ('old', 'fs_old', 'c1', 'prompt', 'processing', 1);
        """
    )
    connection.close()

    store = make_store(path)
    saved = store.get_by_correlation("fs_old")
    assert saved is not None
    assert saved.status == "delivered"
    store.close()


def test_dispatching_event_can_be_completed_without_replay(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    assert store.accept_event("m1", "c1", "hello")
    event = store.claim_next_accepted()
    assert event is not None and event.status == "dispatching"

    replying = store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    assert replying.status == "replying"
    store.mark_chunk_sent(event.correlation_id, 1)
    store.finish_reply(event.correlation_id)
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "replied"
    store.close()


def test_stale_chunk_checkpoint_cannot_resurrect_terminal_event(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    store.mark_chunk_sent(event.correlation_id, 1)
    store.finish_reply(event.correlation_id)

    with pytest.raises(RuntimeError, match="checkpoint state changed"):
        store.mark_chunk_sent(event.correlation_id, 2)
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "replied" and saved.chunks_sent == 1
    store.close()


def test_reply_chunk_plan_is_persisted_and_immutable(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)

    replying = store.begin_reply(
        event.correlation_id,
        "abcdefgh",
        "post_v2",
        ["abcd", "efgh"],
    )
    assert replying.reply_chunks == ("abcd", "efgh")

    with pytest.raises(ValueError, match="different reply chunk plan"):
        store.begin_reply(
            event.correlation_id,
            "abcdefgh",
            "post_v2",
            ["ab", "cd", "ef", "gh"],
        )
    store.close()


def test_reply_cannot_finish_before_all_planned_chunks_are_checkpointed(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store.begin_reply(
        event.correlation_id,
        "abcdefgh",
        reply_chunks=["abcd", "efgh"],
    )
    store.mark_chunk_sent(event.correlation_id, 1)

    with pytest.raises(RuntimeError, match="completion state changed"):
        store.finish_reply(event.correlation_id)
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "replying" and saved.chunks_sent == 1
    store.close()


def test_chunk_checkpoint_cannot_exceed_persisted_plan(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    store.mark_chunk_sent(event.correlation_id, 1)

    with pytest.raises(RuntimeError, match="checkpoint state changed"):
        store.mark_chunk_sent(event.correlation_id, 2)
    store.close()


def test_replied_legacy_row_without_plan_remains_idempotent(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store._connection.execute(  # noqa: SLF001 - model a migrated legacy row
        """
        UPDATE events
        SET status = 'replied', reply_text = 'answer', reply_format = 'plain_v1',
            reply_chunks = NULL, chunks_sent = 1
        WHERE correlation_id = ?
        """,
        (event.correlation_id,),
    )

    completed = store.begin_reply(event.correlation_id, "answer", reply_chunks=["answer"])
    assert completed.status == "replied"
    assert completed.reply_chunks is None
    assert completed.chunks_sent == 1
    store.close()


def test_invalid_stored_plan_checkpoint_is_rejected(tmp_path: Path) -> None:
    store = make_store(tmp_path / "bot.db")
    event = accept_claim_and_deliver(store)
    store._connection.execute("PRAGMA ignore_check_constraints = ON")  # noqa: SLF001
    store._connection.execute(  # noqa: SLF001 - model corrupt storage
        """
        UPDATE events
        SET status = 'replying', reply_text = 'answer', reply_format = 'post_v2',
            reply_chunks = '[\"answer\"]', chunks_sent = 2
        WHERE correlation_id = ?
        """,
        (event.correlation_id,),
    )
    store._connection.execute("PRAGMA ignore_check_constraints = OFF")  # noqa: SLF001

    with pytest.raises(RuntimeError, match="chunk checkpoint is invalid"):
        store.get_by_correlation(event.correlation_id)
    store.close()


def test_v2_migration_adds_empty_chunk_plan_without_losing_partial_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bot.db"
    store = make_store(path)
    event = accept_claim_and_deliver(store)
    store._connection.execute(  # noqa: SLF001 - construct a v2 fixture
        """
        UPDATE events
        SET status = 'replying', reply_text = 'abcdefgh', reply_format = 'post_v2',
            chunks_sent = 1
        WHERE correlation_id = ?
        """,
        (event.correlation_id,),
    )
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA writable_schema = ON")
    schema = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'"
    ).fetchone()[0]
    schema = schema.replace(
        """
                reply_chunks TEXT CHECK (
                    reply_chunks IS NULL
                    OR (json_valid(reply_chunks)
                        AND json_type(reply_chunks) = 'array'
                        AND json_array_length(reply_chunks) > 0)
                ),
""",
        "",
    ).replace(
        """,
                CHECK (
                    reply_chunks IS NULL
                    OR chunks_sent <= json_array_length(reply_chunks)
                )
""",
        "",
    )
    connection.execute(
        "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = 'events'",
        (schema,),
    )
    connection.execute("PRAGMA writable_schema = OFF")
    connection.execute("PRAGMA user_version = 2")
    connection.close()

    reopened = make_store(path)
    saved = reopened.get_by_correlation(event.correlation_id)
    assert saved is not None
    assert saved.status == "replying"
    assert saved.reply_text == "abcdefgh"
    assert saved.reply_format == "post_v2"
    assert saved.reply_chunks is None
    assert saved.chunks_sent == 1
    assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 6  # noqa: SLF001
    reopened.close()


def test_permission_request_lifecycle_is_atomic_and_stores_only_token_hash(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path / "bot.db")
    token_hash = "a" * 64
    created = store.create_permission_request(
        request_id="pr_1",
        session_id="session-1",
        chat_id="chat-1",
        tool_name="Edit",
        summary="path: /safe/file",
        token_hash=token_hash,
        created_at=100,
        expires_at=200,
        max_pending=2,
    )
    assert created.status == "pending"
    assert created.token_hash == token_hash
    columns = store._connection.execute(  # noqa: SLF001 - storage assertion
        "SELECT * FROM permission_requests WHERE request_id = 'pr_1'"
    ).fetchone()
    assert "raw-secret-token" not in repr(dict(columns))

    resolved = store.resolve_permission_request(
        token_hash=token_hash,
        chat_id="chat-1",
        resolution_message_id="message-1",
        decision="allowed",
        now=150,
    )
    assert resolved is not None
    assert resolved.status == "allowed"
    assert resolved.resolved_at == 150
    assert resolved.resolution_message_id == "message-1"
    assert (
        store.resolve_permission_request(
            token_hash=token_hash,
            chat_id="chat-1",
            resolution_message_id="message-2",
            decision="denied",
            now=151,
        )
        is None
    )
    store.close()


def test_permission_resolution_rejects_wrong_chat_expired_and_duplicate_message(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path / "bot.db")
    first_hash = "b" * 64
    second_hash = "c" * 64
    for request_id, token_hash in (("pr_1", first_hash), ("pr_2", second_hash)):
        store.create_permission_request(
            request_id=request_id,
            session_id="session-1",
            chat_id="chat-1",
            tool_name="Edit",
            summary="safe summary",
            token_hash=token_hash,
            created_at=100,
            expires_at=200,
            max_pending=3,
        )

    assert (
        store.resolve_permission_request(
            token_hash=first_hash,
            chat_id="chat-other",
            resolution_message_id="message-wrong-chat",
            decision="allowed",
            now=150,
        )
        is None
    )
    first = store.resolve_permission_request(
        token_hash=first_hash,
        chat_id="chat-1",
        resolution_message_id="message-1",
        decision="denied",
        now=150,
    )
    assert first is not None and first.status == "denied"
    assert (
        store.resolve_permission_request(
            token_hash=second_hash,
            chat_id="chat-1",
            resolution_message_id="message-1",
            decision="allowed",
            now=151,
        )
        is None
    )
    assert store.expire_permission_requests(now=200) == 1
    assert store.get_permission_request("pr_2").status == "expired"
    assert (
        store.resolve_permission_request(
            token_hash=second_hash,
            chat_id="chat-1",
            resolution_message_id="message-2",
            decision="allowed",
            now=201,
        )
        is None
    )
    store.close()


def test_permission_pending_limit_and_startup_expiry(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    store = make_store(path)
    store.create_permission_request(
        request_id="pr_1",
        session_id="session-1",
        chat_id="chat-1",
        tool_name="Edit",
        summary="safe summary",
        token_hash="d" * 64,
        created_at=100,
        expires_at=200,
        max_pending=1,
    )
    with pytest.raises(RuntimeError, match="too many pending"):
        store.create_permission_request(
            request_id="pr_2",
            session_id="session-1",
            chat_id="chat-1",
            tool_name="Write",
            summary="safe summary",
            token_hash="e" * 64,
            created_at=101,
            expires_at=201,
            max_pending=1,
        )
    store.close()

    reopened = make_store(path)
    assert reopened.expire_all_pending_permission_requests(now=150) == 1
    assert reopened.get_permission_request("pr_1").status == "expired"
    assert reopened.expire_all_pending_permission_requests(now=151) == 0
    assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 6  # noqa: SLF001
    reopened.close()


def test_previous_channel_schema_is_upgraded_for_dispatching_state(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            correlation_id TEXT NOT NULL UNIQUE,
            chat_id TEXT NOT NULL,
            user_text TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN ('accepted', 'delivered', 'replying', 'replied', 'failed')
            ),
            reply_text TEXT,
            chunks_sent INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO events(
            message_id, correlation_id, chat_id, user_text, status, updated_at
        ) VALUES ('old', 'fs_old', 'c1', 'not yet sent', 'accepted', 1);
        """
    )
    connection.close()

    store = make_store(path)
    event = store.claim_next_accepted()
    assert event is not None
    assert event.correlation_id == "fs_old"
    assert event.status == "dispatching"
    store.close()
