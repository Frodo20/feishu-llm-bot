import json
import time

import pytest

from feishu_llm_bot.runtime_admin import reconcile_document, register_schedule
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.schedules import tick_schedules


def test_receipt_persisted_before_crash_can_be_verified_without_recreate(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    try:
        store.accept_event("message", "chat", "create a document")
        a = store.claim(time.time())
        op = store.operation_begin(a["attempt_id"], a["token"], "doc", "create_document", {})
        directory = (
            tmp_path
            / "tasks"
            / a["correlation_id"]
            / a["attempt_id"]
            / "operations"
            / op["operation_id"]
        )
        directory.mkdir(parents=True)
        (directory / "result.json").write_text(
            json.dumps({"document": {"document_id": "doc1", "url": "https://example.test/doc1"}})
        )
        store.drain(a["attempt_id"])
        assert store.finish(a["attempt_id"], now=time.time(), reason="worker_exit") == "suspended"
        calls = []

        def verify(_cli, document_id, _directory, _env):
            calls.append(document_id)
            return {"document_id": document_id, "content": "weekly content", "revision_id": 3}

        monkeypatch.setattr("feishu_llm_bot.runtime_admin.verify_document", verify)
        result = reconcile_document(
            store, op["operation_id"], {"state_dir": str(tmp_path / "resident"), "path": "/usr/bin"}
        )
        assert calls == ["doc1"] and result["verified"]
        assert store.operations(a["correlation_id"])[0]["state"] == "succeeded"
        assert store.task(a["correlation_id"])["state"] == "suspended"
    finally:
        store.close()


def test_invalid_schedule_does_not_block_other_tasks_or_schedules(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    try:
        with pytest.raises(ValueError, match="weekday"):
            register_schedule(store, "invalid", {"weekday": 8})
        register_schedule(
            store,
            "weekly",
            {"weekday": 3, "hour": 19, "chat_id": "chat", "prompt": "update {week}"},
        )
        store._connection.execute("INSERT INTO runtime_schedules VALUES ('broken','{}',NULL)")
        tick_schedules(store, time.time())
        assert store.claim(time.time())
        tick_schedules(store, time.time())
        assert store._connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    finally:
        store.close()
