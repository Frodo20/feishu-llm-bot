from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest
from test_feishu_client import Messages, Response, client
from test_progress import Client

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.progress import ProgressMonitor
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_reactions import RECEIPT_EMOJIS, advance_status_reaction, receipt_emoji


@pytest.fixture
def runtime(tmp_path):
    db = RuntimeStore(tmp_path / "bot.sqlite3")
    db.accept_event("message-1", "chat", "perform my task")
    a = db.claim(time.time(), "session")
    remote = Client()
    options = dict(
        database=db.path,
        state_path=db.path,
        transcript=tmp_path / "unused",
        session_id="session",
        health_path=tmp_path / "health",
        resident_path=tmp_path / "resident",
        client=remote,
        runtime_mode=True,
    )
    monitor = ProgressMonitor(**options)
    yield db, a, remote, monitor, options
    monitor.close()
    db.close()


def finish(db, a, outcome="completed"):
    db.submit_answer(a["attempt_id"], a["token"], "result text", outcome)
    db.drain(a["attempt_id"])
    db.finish(a["attempt_id"], now=time.time())


def test_receipt_palette_is_stable_and_varied():
    assert {receipt_emoji(f"message-{i}") for i in range(30)} == set(RECEIPT_EMOJIS)
    assert receipt_emoji("message-1") == receipt_emoji("message-1")


def test_receipt_and_status_progression_survive_restart_without_duplicate_receipt(runtime):
    db, a, remote, monitor, options = runtime
    now = time.time()
    monitor.tick(now)
    assert remote.receipt_emojis == [receipt_emoji("message-1")]
    monitor.tick(now + 4)
    assert remote.status_reactions[-1] == ("message-1", "THINKING")
    ident = monitor.tasks[a["correlation_id"]]["status_reaction_id"]
    finish(db, a)
    monitor.tick(now + 8)
    # The final card/outbox succeeds even while reaction replacement still needs work.
    assert db.get_by_correlation(a["correlation_id"]).status == "replied"
    assert remote.removed_reactions == [("message-1", ident)]
    monitor._checkpoint()
    restarted = ProgressMonitor(**options)
    try:
        restarted.tick(now + 12)
        restarted.tick(now + 16)
        assert remote.status_reactions[-1] == ("message-1", "DONE")
        assert remote.reactions == ["message-1"]
        assert remote.status_reactions.count(("message-1", "DONE")) == 1
        assert len(remote.cards) == 1
        assert not restarted.tasks
    finally:
        restarted.close()


def test_failed_status_reaction_never_blocks_final_result_or_next_task(runtime):
    db, a, remote, monitor, _ = runtime
    remote.fail_status_reaction = True
    finish(db, a)
    now = time.time()
    for offset in [0, 4, 35, 66]:
        monitor.tick(now + offset)
    assert db.get_by_correlation(a["correlation_id"]).status == "replied"
    assert len(remote.status_reactions) == 3
    assert not monitor.tasks
    db.accept_event("next", "chat", "next independent task")
    assert db.claim(time.time())


def test_permission_wait_and_partial_result_use_thinking(runtime):
    db, a, remote, monitor, _ = runtime
    finish(db, a, "partial")
    now = time.time()
    monitor.tick(now)
    monitor.tick(now + 4)
    assert remote.status_reactions == [("message-1", "THINKING")]


def test_cancelled_result_uses_thanks_and_unknown_failure_does_not_show_done(runtime):
    db, a, remote, monitor, _ = runtime
    db.handle_control(IncomingMessage.text("cancel", "chat", "/cancel 1"))
    db.drain(a["attempt_id"])
    db.finish(a["attempt_id"], now=time.time(), reason="cancelled")
    now = time.time()
    monitor.tick(now)
    monitor.tick(now + 4)
    assert remote.status_reactions == [("message-1", "THANKS")]


def test_error_reaction_replaced_when_task_is_continued(runtime):
    db, a, remote, monitor, _ = runtime
    finish(db, a, "unanswered")
    now = time.time()
    monitor.tick(now)
    monitor.tick(now + 4)
    assert remote.status_reactions[-1][1] == "ERROR"
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    monitor.tick(now + 8)
    monitor.tick(now + 12)
    assert remote.removed_reactions
    assert remote.status_reactions[-1][1] == "THINKING"


def test_schedule_has_no_real_message_reactions(tmp_path):
    db = RuntimeStore(tmp_path / "bot.sqlite3")
    db.accept_event("schedule:weekly:2026-09-26", "chat", "scheduled task")
    a = db.claim(time.time())
    finish(db, a)
    remote = Client()
    monitor = ProgressMonitor(
        database=db.path,
        state_path=db.path,
        transcript=tmp_path / "unused",
        session_id="session",
        health_path=tmp_path / "health",
        resident_path=tmp_path / "resident",
        client=remote,
        runtime_mode=True,
    )
    try:
        monitor.tick()
        assert not remote.reactions and not remote.status_reactions
    finally:
        monitor.close()
        db.close()


def test_status_api_returns_id_and_deletes_only_saved_reaction():
    response = Response()
    response.data = SimpleNamespace(reaction_id="own-id")
    reactions = Messages([response])
    deleted = []
    reactions.delete = lambda req: deleted.append(req) or Response()
    target = client(Messages())
    target._client.im.v1.message_reaction = reactions
    assert target.add_reaction("original-message", "DONE") == "own-id"
    assert reactions.creates[0].request_body.reaction_type.emoji_type == "DONE"
    target.remove_reaction("original-message", "own-id")
    assert deleted[0].message_id == "original-message"
    assert deleted[0].reaction_id == "own-id"


def test_status_delete_failure_is_bounded_and_preserves_tracking():
    remote = Client()
    remote.fail_remove_reaction = True
    state = {
        "message_id": "m1",
        "created_at": 0,
        "status": "replied",
        "task_state": "succeeded",
        "status_reactions_enabled": True,
        "status_reaction_id": "owned-id",
        "status_reaction_emoji": "THINKING",
    }
    for now in [0, 1, 30, 60, 90]:
        advance_status_reaction(remote, state, now)
    assert len(remote.removed_reactions) == 3
    assert not remote.status_reactions
    assert state["status_reaction_disabled"] and state["status_reaction_id"] == "owned-id"
    assert json.loads(json.dumps(state)) == state
