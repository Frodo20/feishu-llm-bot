from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing

import pytest

from feishu_llm_bot.migrate_runtime import backup_database, migrate
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.store import Store


def test_backup_includes_committed_wal_records(tmp_path):
    source = tmp_path / "source.sqlite3"
    with closing(sqlite3.connect(source)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE example (value TEXT)")
        db.execute("INSERT INTO example VALUES ('durable')")
        db.commit()
        backup_database(source, tmp_path / "backup.sqlite3")
        with closing(sqlite3.connect(tmp_path / "backup.sqlite3")) as saved:
            assert saved.execute("SELECT value FROM example").fetchone()[0] == "durable"


def test_migration_preserves_existing_document_and_queued_controls(tmp_path):
    source = tmp_path / "bot.sqlite3"
    store = Store(source)
    store.accept_event("old", "chat", "create document")
    event = store.claim_next_accepted()
    store.mark_delivered(event.correlation_id)
    store.accept_event("query", "chat", "你还在嘛")
    store.accept_event("next", "chat", "independent work")
    store.close()
    progress = tmp_path / "progress"
    progress.mkdir()
    with closing(sqlite3.connect(progress / "progress.sqlite3")) as db:
        db.execute("CREATE TABLE tasks (correlation_id TEXT PRIMARY KEY, state TEXT)")
        db.execute(
            "INSERT INTO tasks VALUES (?,?)",
            (event.correlation_id, json.dumps({"card_id": "existing-card"})),
        )
        db.commit()
    config = {
        "database_path": str(source),
        "progress_state_dir": str(progress),
        "state_dir": str(tmp_path / "resident"),
        "session_id": "previous",
    }
    runtime_path = migrate(
        config,
        tmp_path / "backup",
        verified={
            "correlation_id": event.correlation_id,
            "answer": "verified document URL",
        },
    )
    assert json.loads(runtime_path.read_text())["runtime_enabled"]
    result = RuntimeStore(source)
    try:
        assert result.task(event.correlation_id)["state"] == "succeeded"
        assert result.get_by_correlation(event.correlation_id).reply_text == "verified document URL"
        assert result._connection.execute(
            "SELECT reply_text FROM events WHERE message_id='query'"
        ).fetchone()[0]
        assert (
            json.loads(result._connection.execute("SELECT state FROM tasks").fetchone()[0])[
                "card_id"
            ]
            == "existing-card"
        )
        a = result.claim(time.time())
        assert result.get_by_correlation(a["correlation_id"]).message_id == "next"
        with pytest.raises(RuntimeError, match="already migrated"):
            migrate(config, tmp_path / "second-backup")
    finally:
        result.close()


def test_unverified_legacy_execution_is_suspended_not_replayed(tmp_path):
    source = tmp_path / "bot.sqlite3"
    store = Store(source)
    store.accept_event("old", "chat", "may have performed a write")
    event = store.claim_next_accepted()
    store.mark_delivered(event.correlation_id)
    store.accept_event("next", "chat", "next")
    store.close()
    migrate(
        {
            "database_path": str(source),
            "progress_state_dir": str(tmp_path / "progress"),
            "state_dir": str(tmp_path / "resident"),
            "session_id": "previous",
        },
        tmp_path / "backup",
    )
    result = RuntimeStore(source)
    try:
        assert result.task(event.correlation_id)["state"] == "suspended"
        a = result.claim(time.time())
        assert result.get_by_correlation(a["correlation_id"]).message_id == "next"
    finally:
        result.close()
