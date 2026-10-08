from __future__ import annotations

import json
import time

import pytest
from test_permissions import FakeReplies, finish, receive_decision

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.permission_relay import PermissionRelay
from feishu_llm_bot.runtime_store import RuntimeStore


@pytest.mark.parametrize("cancel", [True, False])
def test_worker_permission_rejects_wrong_identity_and_cancelled_execution(tmp_path, cancel):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    store.accept_event("m1", "chat", "write")
    a = store.claim(time.time(), "626a1b0a-6f8a-49f3-9c24-46e0059cf7e6")
    store.started(a["attempt_id"])
    replies = FakeReplies()
    relay = PermissionRelay(
        store=store,
        replies=replies,
        socket_path=tmp_path / "private" / "relay.sock",
        session_id=a["session_id"],
        chat_id="chat",
        timeout_seconds=2,
        max_pending=1,
        all_sessions=True,
        worker_only=True,
    )
    relay.start()
    try:
        payload = {
            "session_id": a["session_id"],
            "tool_name": "mcp__feishu__run",
            "cwd_context": "probe",
            "summary": "probe",
            "attempt_id": a["attempt_id"],
            "token": a["token"] if cancel else "wrong-token",
        }
        connection, _ = receive_decision(relay.socket_path, json.dumps(payload).encode() + b"\n")
        if cancel:
            assert replies.ready.wait(2)
            store.handle_control(IncomingMessage.text("cancel", "chat", "/cancel 1"))
        assert finish(connection) == {"decision": "deny"}
        if not cancel:
            assert not replies.sent
    finally:
        relay.stop()
        store.close()


def test_cgroup_v1_and_v2_child_detection(tmp_path):
    from feishu_llm_bot.orchestrator import SystemdWorkers

    legacy = tmp_path / "systemd" / "worker" / "child"
    legacy.mkdir(parents=True)
    (legacy / "tasks").write_text("123\n")
    assert SystemdWorkers.populated("/worker", tmp_path)
    (legacy / "tasks").write_text("")
    assert not SystemdWorkers.populated("/worker", tmp_path)
    (tmp_path / "cgroup.controllers").touch()
    group = tmp_path / "worker"
    group.mkdir()
    (group / "cgroup.events").write_text("populated 1\nfrozen 0\n")
    assert SystemdWorkers.populated("/worker", tmp_path)
