from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.orchestrator import Orchestrator
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_tools import execute_command
from feishu_llm_bot.worker import consume_event


@pytest.fixture
def db(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    yield store
    store.close()


def task(db, name="m1", text="do work"):
    db.accept_event(name, "chat", text)
    attempt = db.claim(time.time())
    assert attempt
    db.started(attempt["attempt_id"])
    return attempt


def finish(db, a, reason=None, **kwargs):
    db.drain(a["attempt_id"])
    return db.finish(a["attempt_id"], now=time.time(), reason=reason, **kwargs)


def test_saved_answer_releases_execution_before_feishu_delivery(db):
    a = task(db)
    db.accept_event("m2", "chat", "next")
    db.submit_answer(a["attempt_id"], a["token"], "saved answer")
    assert finish(db, a) == "succeeded"
    assert db.get_by_correlation(a["correlation_id"]).status == "replying"
    second = db.claim(time.time())
    assert second and second["correlation_id"] != a["correlation_id"]


@pytest.mark.parametrize("reason", [None, "task_timeout"])
def test_gateway_restart_preserves_old_runtime_history_and_results(tmp_path, reason):
    from feishu_llm_bot.service import SessionBridgeService

    path = tmp_path / "bot.sqlite3"
    store = RuntimeStore(path)
    a = task(store)
    if reason is None:
        store.submit_answer(a["attempt_id"], a["token"], "confirmed history")
    finish(store, a, reason)
    with store.transaction() as connection:
        connection.execute("UPDATE events SET status='replied',updated_at=?",
                           (int(time.time()) - 30 * 86400,))
    store.close()
    store = RuntimeStore(path)
    service = SessionBridgeService(store=store, replies=object(), emit_inbound=lambda _: None,
                                   dispatch_enabled=False, reply_chunk_chars=4000,
                                   max_inbound_chars=100000, queue_size=128)
    try:
        service.start()
        assert store.prune_events(older_than_seconds=1) == 0
        assert store.get_by_correlation(a["correlation_id"]) is not None
        assert store.context()
        assert store._connection.execute("SELECT count(*) FROM runtime_results").fetchone()[0] == 1
        if reason:
            assert store.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
            assert store.claim(time.time())["correlation_id"] == a["correlation_id"]
    finally:
        service.stop()
        store.close()


def test_model_failure_budget_and_new_tasks_not_starved(db):
    a = task(db)
    cid = a["correlation_id"]
    assert finish(db, a, "model_error", max_retries=1) == "retry_wait"
    db.accept_event("m2", "chat", "new independent task")
    b = db.claim(time.time() + 20)
    assert b["correlation_id"] != cid
    db.submit_answer(b["attempt_id"], b["token"], "new task done")
    finish(db, b)
    retry = db.claim(time.time() + 20)
    assert retry["correlation_id"] == cid
    assert finish(db, retry, "model_error", max_retries=1) == "failed"
    assert db.active() == []
    assert db.get_by_correlation(cid).reply_text


def test_error_after_unmanaged_write_is_not_replayed(db):
    a = task(db)
    db.record_activity(a["attempt_id"], time.time(), unsafe=True)
    assert finish(db, a, "model_error") == "failed"
    assert db.claim(time.time() + 100) is None


def test_cancel_is_out_of_band_idempotent_and_fences_old_worker(db):
    a = task(db)
    message = IncomingMessage.text("cancel", "chat", "/cancel")
    assert db.handle_control(message)
    assert db.handle_control(message)
    with pytest.raises(PermissionError):
        db.submit_answer(a["attempt_id"], a["token"], "late result")
    assert finish(db, a, "cancelled") == "cancelled"
    db.accept_event("m2", "chat", "next")
    b = db.claim(time.time())
    assert b
    with pytest.raises(PermissionError):
        db.operation_begin(a["attempt_id"], a["token"], "late", "run", {})


def test_control_lookup_does_not_cross_chat(db):
    task(db)
    assert db.handle_control(IncomingMessage.text("c", "other-chat", "/cancel 1"))
    assert not db.task(db.active()[0]["correlation_id"])["cancel_requested"]


def test_natural_status_query_does_not_wait_on_active_task(db):
    a = task(db)
    db.accept_event("m2", "chat", "你还在嘛")
    assert db.handle_control(IncomingMessage.text("m2", "chat", "你还在嘛"), existing=True)
    with db._lock:
        row = db._connection.execute("SELECT * FROM events WHERE message_id='m2'").fetchone()
    assert row["reply_text"] and row["status"] == "replying"
    assert db.active()[0]["attempt_id"] == a["attempt_id"]


def test_operation_reuses_success_but_never_replays_unknown(db):
    a = task(db)
    op = db.operation_begin(
        a["attempt_id"], a["token"], "doc", "create_document", {"title": "oneone"}
    )
    db.operation_end(
        a["attempt_id"], a["token"], op["operation_id"], "succeeded", {"document_id": "doc1"}
    )
    same = db.operation_begin(
        a["attempt_id"], a["token"], "doc", "create_document", {"title": "oneone"}
    )
    assert same["operation_id"] == op["operation_id"]
    assert json.loads(same["result"])["document_id"] == "doc1"
    with pytest.raises(ValueError, match="different"):
        db.operation_begin(
            a["attempt_id"], a["token"], "doc", "create_document", {"title": "changed"}
        )
    pending = db.operation_begin(a["attempt_id"], a["token"], "write", "run", {"command": "write"})
    assert pending["state"] == "running"
    assert finish(db, a, "worker_exit") == "suspended"
    assert db.operations(a["correlation_id"])[-1]["state"] == "unknown"
    assert db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    assert db.claim(time.time()) is None


def test_structured_final_without_reply_tool_completes_task(db):
    a = task(db)
    result = consume_event(
        db, a, {"type": "result", "is_error": False, "result": "final answer"}, []
    )
    assert result == "completed"
    assert finish(db, a) == "succeeded"


def test_resumed_orphan_notification_does_not_end_the_new_task(db):
    a = task(db)
    result = consume_event(
        db,
        a,
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "",
            "num_turns": 0,
        },
        [],
    )
    assert result is None
    assert db.attempt(a["attempt_id"])["answer"] is None
    assert (
        consume_event(
            db,
            a,
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "result": "actual new task answer",
                "num_turns": 1,
            },
            [],
        )
        == "completed"
    )
    assert finish(db, a) == "succeeded"


def test_empty_answer_after_a_real_turn_is_still_a_failure(db):
    a = task(db)
    assert (
        consume_event(
            db,
            a,
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "result": "",
                "num_turns": 1,
            },
            [],
        )
        == "missing_final_result"
    )


def test_image_cannot_complete_without_reading(db):
    # Reserve a real image-shaped event without granting the read proof.
    db.reserve_image("img", "chat", "what is this")
    with db._lock:
        db._connection.execute("UPDATE events SET status='accepted'")
    a = db.claim(time.time())
    with pytest.raises(ValueError, match="Read"):
        db.submit_answer(a["attempt_id"], a["token"], "made up description")


def test_complete_existing_result_survives_model_failure(db):
    a = task(db)
    db.submit_answer(a["attempt_id"], a["token"], "https://example.com/already-created-document")
    assert finish(db, a, "model_error") == "succeeded"
    assert db.claim(time.time() + 100) is None


def test_verified_incident_import_is_idempotent(db):
    db.accept_event("old", "chat", "create oneone")
    old = db.claim_next_accepted()
    db.mark_delivered(old.correlation_id)
    db.import_result(old.correlation_id, "existing document link", time.time())
    db.import_result(old.correlation_id, "existing document link", time.time())
    db.accept_event("next", "chat", "new task")
    assert db.claim(time.time())
    with db._lock:
        assert db._connection.execute("SELECT count(*) FROM runtime_results").fetchone()[0] == 1


def test_successful_background_output_is_durable(tmp_path):
    result = execute_command(
        ["/bin/sh", "-c", "sleep 0.05; printf document-id"], tmp_path, timeout=2
    )
    assert result["exit_code"] == 0
    assert Path(result["stdout_path"]).read_text() == "document-id"


def test_command_timeout_stops_process_group_and_retains_output(tmp_path):
    result = execute_command(["/bin/sh", "-c", "printf started; sleep 20"], tmp_path, timeout=0.05)
    assert result["timed_out"]
    assert Path(result["stdout_path"]).read_text() == "started"


class Workers:
    def __init__(self):
        self.states = {}
        self.started = []
        self.stopped = []
        self.refuse_stop = False

    def start(self, a, path, config):
        self.states[a["unit_name"]] = {"SubState": "running"}
        self.started.append(a)

    def state(self, unit):
        return self.states.get(unit, {"SubState": "dead"})

    def stop(self, unit):
        if self.refuse_stop:
            raise RuntimeError("still alive")
        self.stopped.append(unit)
        self.states[unit] = {"SubState": "dead"}


def engine(db, tmp_path):
    workers = Workers()
    config = {
        "state_dir": str(tmp_path / "resident"),
        "session_id": None,
        "idle_timeout_seconds": 5,
        "task_timeout_seconds": 1200,
    }
    return Orchestrator(config, db, workers), workers


def test_unresponsive_worker_is_stopped_and_next_task_runs(db, tmp_path):
    e, workers = engine(db, tmp_path)
    db.accept_event("m1", "chat", "first")
    db.accept_event("m2", "chat", "second")
    e.tick()
    e.tick(time.time() + 6)
    assert workers.stopped
    assert db.active() == []
    e.tick()
    assert len(workers.started) == 2


def test_expired_permission_suspends_task_and_releases_next(db, tmp_path):
    e, workers = engine(db, tmp_path)
    e.config["session_id"] = "permission-session"
    db.accept_event("m1", "chat", "first")
    db.accept_event("m2", "chat", "second")
    e.tick()
    a = db.active()[0]
    created = int(a["started_at"])
    db.create_permission_request(
        request_id="pr_test",
        session_id=a["session_id"],
        chat_id="chat",
        tool_name="Edit",
        summary="edit",
        token_hash="a" * 64,
        created_at=created,
        expires_at=created + 2,
        max_pending=1,
    )
    e.tick(created + 3)
    assert db.task(a["correlation_id"])["state"] == "suspended"
    assert db.attempt(a["attempt_id"])["failure_reason"] == "permission_expired"
    e.tick()
    assert len(workers.started) == 2


def test_failed_isolation_never_reuses_slot(db, tmp_path):
    e, workers = engine(db, tmp_path)
    db.accept_event("m1", "chat", "first")
    e.tick()
    workers.refuse_stop = True
    with pytest.raises(RuntimeError, match="still alive"):
        e.tick(time.time() + 6)
    assert db.active()[0]["state"] == "draining"
    assert db.claim(time.time() + 10) is None


def test_restarting_supervisor_stops_orphan_before_dispatch(db, tmp_path):
    e, workers = engine(db, tmp_path)
    db.accept_event("m1", "chat", "first")
    e.tick()
    e.recover()
    assert workers.stopped
    assert not db.active()
    assert db.get_by_correlation(workers.started[0]["correlation_id"]).reply_text


def test_two_supervisors_cannot_claim_two_workers(db, tmp_path):
    db.accept_event("m1", "chat", "first")
    db.accept_event("m2", "chat", "second")
    other = RuntimeStore(db.path)
    try:
        assert db.claim(time.time())
        assert other.claim(time.time()) is None
    finally:
        other.close()


def test_old_card_cannot_cancel_new_attempt(db):
    a = task(db)
    finish(db, a, "model_error", max_retries=0)
    assert db.handle_control(IncomingMessage.text("resume", "chat", "/continue 1"))
    with pytest.raises(ValueError, match="older"):
        db.handle_control(
            IncomingMessage.text("callback:before-claim", "chat", "/cancel 1"),
            expected_attempt=a["attempt_id"],
        )
    b = db.claim(time.time())
    with pytest.raises(ValueError, match="older"):
        db.handle_control(
            IncomingMessage.text("callback:old", "chat", "/cancel 1"),
            expected_attempt=a["attempt_id"],
        )
    assert not db.task(b["correlation_id"])["cancel_requested"]


def test_weekly_schedule_survives_restart_and_deduplicates(db):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from feishu_llm_bot.schedules import tick_schedules

    config = {"weekday": 3, "hour": 19, "chat_id": "chat", "prompt": "summarize {week}"}
    with db._lock:
        db._connection.execute(
            "INSERT INTO runtime_schedules VALUES (?,?,?)",
            ("oneone", json.dumps(config), "2026-09-17"),
        )
    now = datetime(2026, 9, 24, 20, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    tick_schedules(db, now)
    tick_schedules(db, now + 60)
    with db._lock:
        rows = db._connection.execute("SELECT message_id,user_text FROM events").fetchall()
    assert len(rows) == 1 and rows[0]["user_text"] == "summarize 2026-09-24"


def test_runtime_failed_card_does_not_claim_success(db, tmp_path):
    from test_progress import Client

    from feishu_llm_bot.progress import ProgressMonitor, progress_card

    a = task(db)
    client = Client()
    client.fail_update = True
    monitor = ProgressMonitor(
        database=db.path,
        state_path=tmp_path / "progress.sqlite3",
        transcript=tmp_path / "absent.jsonl",
        session_id="test",
        health_path=tmp_path / "health.json",
        resident_path=tmp_path / "resident.json",
        client=client,
        runtime_mode=True,
    )
    try:
        monitor.tick()
        assert finish(db, a, "model_error", max_retries=0) == "failed"
        monitor.tick(time.time() + 4)
        state = monitor.tasks[a["correlation_id"]]
        assert state["task_state"] == "failed"
        card = progress_card(state, now=time.time(), connection="桥接在线")
        assert card["header"]["template"] == "red"
        db.accept_event("m2", "chat", "next")
        assert db.claim(time.time())
    finally:
        monitor.close()


def test_continuing_task_clears_old_answer_from_card(db, tmp_path):
    from test_progress import Client

    from feishu_llm_bot.progress import ProgressMonitor

    a = task(db)
    monitor = ProgressMonitor(
        database=db.path,
        state_path=tmp_path / "progress.sqlite3",
        transcript=tmp_path / "absent.jsonl",
        session_id="test",
        health_path=tmp_path / "health.json",
        resident_path=tmp_path / "resident.json",
        client=Client(),
        runtime_mode=True,
    )
    try:
        monitor.tick()
        finish(db, a, "model_error", max_retries=0)
        monitor.tick(time.time() + 4)
        assert db.get_by_correlation(a["correlation_id"]).status == "replied"
        db.handle_control(IncomingMessage.text("resume", "chat", "/continue 1"))
        monitor.tick(time.time() + 8)
        assert "answer" not in monitor.tasks[a["correlation_id"]]
    finally:
        monitor.close()


def test_control_reply_failure_keeps_durable_outbox(db):
    from feishu_llm_bot.progress import _send_controls

    class Offline:
        def reply_markdown(self, *_args):
            raise RuntimeError("offline")

    db.handle_control(IncomingMessage.text("status", "chat", "/status"))
    _send_controls(db, Offline())
    with db._lock:
        row = db._connection.execute("SELECT * FROM runtime_controls").fetchone()
    assert not row["sent"] and row["attempts"] == 1 and row["retry_at"] > time.time()


def test_cancel_accepted_before_finish_wins_over_saved_answer(db):
    a = task(db)
    db.submit_answer(a["attempt_id"], a["token"], "early answer")
    db.handle_control(IncomingMessage.text("cancel", "chat", "/cancel 1"))
    assert finish(db, a) == "cancelled"
    assert db.get_by_correlation(a["correlation_id"]).reply_text != "early answer"


def test_late_sender_cannot_complete_continued_task(db):
    a = task(db)
    finish(db, a, "model_error", max_retries=0)
    old = db.get_by_correlation(a["correlation_id"]).reply_text
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    newer = db.claim(time.time())
    assert not db.acknowledge_result(a["correlation_id"], old, "old-card", time.time(), 1)
    assert db.task(newer["correlation_id"])["state"] == "running"


def test_identical_answer_does_not_let_an_old_delivery_acknowledge_a_new_result(db):
    a = task(db)
    finish(db, a, "model_error", max_retries=0)
    old = db.get_by_correlation(a["correlation_id"]).reply_text
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    newer = db.claim(time.time())
    finish(db, newer, "model_error", max_retries=0)
    assert db.get_by_correlation(a["correlation_id"]).reply_text == old
    assert not db.acknowledge_result(a["correlation_id"], old, "old-card", time.time(), 1)
    assert db.get_by_correlation(a["correlation_id"]).status == "replying"
    assert db.acknowledge_result(a["correlation_id"], old, "new-card", time.time(), 2)


def test_sender_completes_outbox_with_gateway_offline(db, tmp_path):
    from test_progress import Client

    from feishu_llm_bot.progress import ProgressMonitor

    a = task(db)
    db.submit_answer(a["attempt_id"], a["token"], "durable final")
    finish(db, a)
    options = dict(
        database=db.path,
        state_path=db.path,
        transcript=tmp_path / "unused",
        session_id="session",
        health_path=tmp_path / "health.json",
        resident_path=tmp_path / "resident.json",
        client=Client(),
        runtime_mode=True,
    )
    monitor = ProgressMonitor(**options)
    monitor.tick()
    monitor.close()
    with db._lock:
        outbox = db._connection.execute("SELECT * FROM runtime_outbox").fetchone()
    assert outbox["state"] == "sent"
    assert outbox["card_id"] == "card-1"
    assert db.get_by_correlation(a["correlation_id"]).status == "replied"
    restarted = ProgressMonitor(**options)
    restarted.tick(time.time() + 5)
    restarted.close()
    assert len(options["client"].cards) == 1


def test_terminal_error_detected_while_worker_process_still_alive(db, tmp_path):
    e, workers = engine(db, tmp_path)
    db.accept_event("first", "chat", "first")
    db.accept_event("next", "chat", "next")
    e.tick()
    a = db.active()[0]
    with db._lock:
        db._connection.execute("UPDATE runtime_attempts SET failure_reason='model_error'")
    e.tick()
    assert workers.stopped == [a["unit_name"]]
    e.tick()
    assert len(workers.started) == 2


def test_model_outage_pauses_calls_without_exhausting_waiting_tasks(db, tmp_path):
    e, workers = engine(db, tmp_path)
    e.config["max_retries"] = 0
    for n in range(4):
        db.accept_event("m" + str(n), "chat", "work")
    for _ in range(3):
        e.tick()
        with db._lock:
            db._connection.execute(
                "UPDATE runtime_attempts SET failure_reason='model_error' WHERE state='starting'"
            )
        e.tick()
    assert db.meta("model_retry_at") > time.time()
    e.tick()
    assert len(workers.started) == 3
    db.handle_control(IncomingMessage.text("status", "chat", "/status"))
    with db._lock:
        row = db._connection.execute(
            "SELECT answer FROM runtime_controls WHERE message_id='status'"
        ).fetchone()
        queued = db._connection.execute(
            "SELECT count(*) FROM runtime_tasks WHERE state='queued'"
        ).fetchone()[0]
    assert "模型服务暂不可用" in row[0] and queued == 1


def test_hard_budget_uses_monotonic_even_when_wall_clock_moves_back(db, tmp_path, monkeypatch):
    e, workers = engine(db, tmp_path)
    e.config["task_timeout_seconds"] = 10
    db.accept_event("first", "chat", "first")
    db.accept_event("next", "chat", "next")
    now = time.time()
    e.tick(now)
    a = db.active()[0]
    monkeypatch.setattr(
        "feishu_llm_bot.orchestrator.time.monotonic",
        lambda: e.monotonic_starts[a["attempt_id"]] + 11,
    )
    e.tick(now - 3600)
    saved = db.task(a["correlation_id"])
    assert saved["state"] == "suspended"
    assert saved["total_seconds"] >= 11
    assert workers.stopped


def test_cumulative_budget_bounds_next_attempt(db, tmp_path):
    e, workers = engine(db, tmp_path)
    e.config["total_budget_seconds"] = 30
    a = task(db)
    finish(db, a, "model_error")
    with db._lock:
        db._connection.execute("UPDATE runtime_tasks SET total_seconds=25,retry_at=0")
    e.tick()
    assert workers.started[0]["timeout_seconds"] == 5


def test_lookup_old_task_by_number_is_not_limited_to_latest_hundred(db):
    a = task(db)
    finish(db, a, "model_error", max_retries=0)
    for n in range(105):
        db.accept_event("later-" + str(n), "chat", "later")
    db.handle_control(IncomingMessage.text("lookup", "chat", "/result 1"))
    with db._lock:
        answer = db._connection.execute(
            "SELECT answer FROM runtime_controls WHERE message_id='lookup'"
        ).fetchone()[0]
    assert "任务 #1" in answer


def test_document_receipt_is_delivered_even_after_recovery_budget_exhausted(db):
    a = task(db)
    op = db.operation_begin(a["attempt_id"], a["token"], "doc", "create_document", {})
    db.operation_end(
        a["attempt_id"],
        a["token"],
        op["operation_id"],
        "succeeded",
        {"verified": True, "document": {"url": "https://example.test/doc1"}},
    )
    finish(db, a, "model_error", max_retries=0)
    assert "https://example.test/doc1" in db.get_by_correlation(a["correlation_id"]).reply_text


def test_invalid_tool_inputs_do_not_create_unknown_operations(db, tmp_path):
    from feishu_llm_bot.task_tools import invoke

    a = task(db)
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps({**a, "config": {"database_path": str(db.path)}}))
    with pytest.raises(ValueError, match="Invalid command"):
        invoke(
            request_path,
            "run",
            {"operation_key": "one", "command": "echo hi", "timeout_seconds": "forever"},
        )
    assert db.operations(a["correlation_id"]) == []


def test_document_creation_readback_and_retry_never_creates_twice(db, tmp_path, monkeypatch):
    from feishu_llm_bot.task_tools import invoke

    a = task(db)
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps({**a, "config": {"database_path": str(db.path), "path": "/usr/bin:/bin"}})
    )
    calls = []

    def command(args, directory, **_kwargs):
        calls.append(args[4])
        document = {"document_id": "doc1", "url": "https://example.test/doc1"}
        if args[4] == "fetch":
            document.update(content="weekly content", revision_id=1)
        output = directory / "stdout.txt"
        output.write_text(json.dumps({"ok": True, "data": {"document": document}}))
        return {"exit_code": 0, "stdout_path": str(output)}

    monkeypatch.setattr("feishu_llm_bot.task_tools.execute_command", command)
    inputs = {"operation_key": "doc", "title": "weekly", "content": "weekly content"}
    result = invoke(request_path, "create_document", inputs)
    assert result["verified"]
    invoke(request_path, "create_document", inputs)
    assert calls == ["create", "fetch"]


def test_verification_failure_retains_document_receipt_without_recreate(db, tmp_path, monkeypatch):
    from feishu_llm_bot.task_tools import invoke

    a = task(db)
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps({**a, "config": {"database_path": str(db.path), "path": "/usr/bin:/bin"}})
    )

    def command(args, directory, **_kwargs):
        output = directory / "stdout.txt"
        output.write_text(
            json.dumps(
                {
                    "ok": True,
                    "data": {
                        "document": {"document_id": "doc1", "url": "https://example.test/doc1"}
                    },
                }
            )
        )
        return {"exit_code": 1 if args[4] == "fetch" else 0, "stdout_path": str(output)}

    monkeypatch.setattr("feishu_llm_bot.task_tools.execute_command", command)
    inputs = {"operation_key": "doc", "title": "weekly", "content": "weekly"}
    result = invoke(request_path, "create_document", inputs)
    assert result["state"] == "unknown" and result["document"]["document_id"] == "doc1"
    with pytest.raises(ValueError, match="already attempted"):
        invoke(request_path, "create_document", inputs)
