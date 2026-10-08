from __future__ import annotations

import io
import json
import os
import sys
import time
from collections import deque
from types import SimpleNamespace

import pytest

from feishu_llm_bot import traex_backend as backend
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.worker import consume_event


def test_rpc_preserves_notifications_arriving_before_call_response():
    read, write = os.pipe()
    os.write(write, b'{"method":"item/completed","params":{"item":{"text":"answer"}}}\n'
                   b'{"id":1,"result":{"turn":{"id":"turn-1"}}}\n')
    os.close(write)
    with os.fdopen(read, "rb") as stdout:
        child = SimpleNamespace(stdout=stdout, stdin=io.BytesIO())
        rpc = backend.RpcConnection(child, lambda: None)
        try:
            result = rpc.call("turn/start", {})
            assert result["turn"]["id"] == "turn-1"
            assert rpc.receive()["params"]["item"]["text"] == "answer"
            assert json.loads(child.stdin.getvalue())["method"] == "turn/start"
        finally:
            rpc.close()


def test_rpc_checks_identity_even_when_notification_is_buffered():
    read, write = os.pipe()
    os.close(write)
    with os.fdopen(read, "rb") as stdout:
        rpc = backend.RpcConnection(SimpleNamespace(stdout=stdout),
                                    lambda: (_ for _ in ()).throw(PermissionError()))
        rpc.pending.append(({"method": "turn/completed"}, 1))
        try:
            with pytest.raises(PermissionError):
                rpc.receive()
        finally:
            rpc.close()


@pytest.mark.parametrize("wire", [b'not-json\n', b'[]\n', b'12345678901234567890\n'])
def test_rpc_rejects_malformed_and_oversized_records(wire, monkeypatch):
    monkeypatch.setattr(backend, "MAX_LINE", 16)
    read, write = os.pipe()
    os.write(write, wire)
    os.close(write)
    with os.fdopen(read, "rb") as stdout:
        rpc = backend.RpcConnection(SimpleNamespace(stdout=stdout), lambda: None)
        try:
            with pytest.raises((ValueError, backend.RpcError)):
                rpc.receive()
        finally:
            rpc.close()


def event(method, **params):
    return {"method": method, "params": {"threadId": "forked", "turnId": "turn", **params}}


@pytest.fixture
def fake_runtime(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    store.accept_event("task", "local", "a task")
    attempt = store.claim(time.time(), "seed")
    config = {"project_dir": str(tmp_path), "cwd": str(tmp_path), "name": "test",
              "node_command": sys.executable, "traex_command": sys.executable}
    request = {**attempt, "config": config, "deadline_monotonic": time.monotonic() + 60}
    calls, sent, messages = [], [], deque()

    class Rpc:
        def __init__(self, child, check):
            self.check = check

        def call(self, method, params):
            self.check()
            calls.append((method, params))
            if method == "initialize":
                return {}
            if method == "thread/fork":
                return {"thread": {"id": "forked"}}
            if method == "turn/start":
                return {"turn": {"id": "turn"}}
            raise AssertionError(method)

        def send(self, message):
            sent.append(message)

        def receive(self):
            self.check()
            assert messages, "adapter consumed past terminal event"
            return messages.popleft()

        def close(self):
            pass

    child = SimpleNamespace(terminate=lambda: None, wait=lambda **_: 0)
    monkeypatch.setattr(backend.subprocess, "Popen", lambda *_, **__: child)
    monkeypatch.setattr(backend, "RpcConnection", Rpc)
    steps = []

    def run():
        return backend.run_traex(store, request, tmp_path / "request.json", "prompt", {},
                                 lambda e: consume_event(store, request, e, steps))

    yield SimpleNamespace(store=store, request=request, calls=calls, sent=sent,
                          messages=messages, run=run, steps=steps)
    store.close()


def test_forked_thread_filters_cross_thread_events_and_commits_only_final(fake_runtime):
    f = fake_runtime
    f.messages.extend([
        event("turn/completed", threadId="unrelated", turn={"id": "turn", "status": "completed"}),
        event("turn/completed", turn={"id": "old-turn", "status": "completed"}),
        event("item/completed", item={"type": "agentMessage", "phase": "commentary",
                                     "id": "comment", "text": "still working"}),
        event("item/started", item={"type": "mcpToolCall", "id": "call", "server": "feishu",
                                   "tool": "operations", "arguments": {}}),
        event("item/completed", item={"type": "mcpToolCall", "id": "call", "server": "feishu",
                                     "tool": "operations", "status": "completed"}),
        event("item/completed", item={"type": "agentMessage", "id": "answer",
                                     "phase": "final_answer", "text": "完成"}),
        event("turn/completed", turn={"id": "turn", "status": "completed"}),
    ])
    assert f.run() == "completed"
    saved = f.store.attempt(f.request["attempt_id"])
    assert saved["session_id"] == "forked"
    assert saved["answer"] == "完成"
    assert saved["unsafe_tools"] == 1
    assert f.calls[1][0] == "thread/fork"
    options = f.calls[1][1]
    assert options["threadId"] == "seed"
    assert options["deferGoalContinuation"] is True
    assert options["config"]["mcp_servers.feishu"]["default_tools_approval_mode"] == "approve"
    assert f.calls[2][1]["capabilities"] == {"mcpServers": ["feishu"], "skills": None}
    assert f.steps[-1]["status"] == "done"


@pytest.mark.parametrize("access,decision", [("workspace", "decline"), ("full", "accept")])
def test_approval_is_one_shot_and_bound_to_current_execution(fake_runtime, access, decision):
    f = fake_runtime
    f.request["config"]["worker_access"] = access
    f.messages.extend([
        {**event("item/commandExecution/requestApproval"), "id": 11},
        {**event("item/commandExecution/requestApproval", threadId="other"), "id": 12},
        event("turn/completed", turn={"id": "turn", "status": "completed"}),
    ])
    assert f.run() == "missing_final_result"
    assert next(m for m in f.sent if m.get("id") == 11)["result"] == {"decision": decision}
    assert "error" in next(m for m in f.sent if m.get("id") == 12)


def test_cancelled_attempt_never_starts_model_or_submits_a_result(fake_runtime):
    f = fake_runtime
    f.store.drain(f.request["attempt_id"], "cancelled")
    assert f.run() == "cancelled"
    assert f.calls == []
    assert f.store.attempt(f.request["attempt_id"])["answer"] is None


def test_interrupted_turn_with_text_is_not_success(fake_runtime):
    f = fake_runtime
    f.messages.extend([
        event("item/completed", item={"type": "agentMessage", "id": "a", "text": "partial"}),
        event("turn/completed", turn={"id": "turn", "status": "interrupted"}),
    ])
    assert f.run() == "model_error"
    assert f.store.attempt(f.request["attempt_id"])["answer"] is None


def test_reasoning_is_not_written_to_normalized_events():
    item = {"type": "reasoning", "text": "private"}
    assert backend.normalize_item("item/completed", item) is None
    unknown = backend.normalize_item("item/started", {"type": "futureExecutor", "id": "x"})
    assert unknown["message"]["content"][0]["name"] == "TraeXTool"


def test_cancel_after_fork_prevents_starting_a_turn(fake_runtime):
    f = fake_runtime
    original = f.store.record_activity

    def cancel_after_start(*args, **kwargs):
        original(*args, **kwargs)
        if kwargs.get("unsafe"):
            f.store.drain(f.request["attempt_id"], "cancelled")

    f.store.record_activity = cancel_after_start
    assert f.run() == "cancelled"
    assert f.store.attempt(f.request["attempt_id"])["answer"] is None
    assert not any(method == "turn/start" for method, _ in f.calls)


def test_cancel_in_active_turn_interrupts_owned_turn_and_never_commits_answer(fake_runtime):
    f = fake_runtime
    original = f.store.record_activity

    def cancel_on_tool(*args, **kwargs):
        original(*args, **kwargs)
        if any(method == "turn/start" for method, _ in f.calls):
            f.store.drain(f.request["attempt_id"], "cancelled")

    f.store.record_activity = cancel_on_tool
    f.messages.extend([
        event("item/started", item={"type": "mcpToolCall", "id": "call", "server": "feishu",
                                    "tool": "operations", "arguments": {}}),
        event("item/completed", item={"type": "agentMessage", "text": "late answer"}),
    ])
    assert f.run() == "cancelled"
    assert f.store.attempt(f.request["attempt_id"])["answer"] is None
    interrupts = [m for m in f.sent if m.get("method") == "turn/interrupt"]
    assert interrupts[-1]["params"] == {"threadId": "forked", "turnId": "turn"}


def test_hard_deadline_rejects_model_start(fake_runtime):
    f = fake_runtime
    f.request["deadline_monotonic"] = time.monotonic() - 1
    assert f.run() == "task_timeout"
    assert f.calls == []
