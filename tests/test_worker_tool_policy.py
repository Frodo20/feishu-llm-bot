from __future__ import annotations

import io
import json
import time

import pytest
from test_permission_hook import hook, json_line, request

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.tool_policy import WORKER_AUTO_ALLOW_TOOLS, worker_allowed_tools_argument


@pytest.fixture
def worker(tmp_path, monkeypatch):
    db = RuntimeStore(tmp_path / "bot.sqlite3")
    db.accept_event("message", "chat", "perform my task")
    a = db.claim(time.time(), "target-session")
    path = tmp_path / "request.json"
    path.write_text(json.dumps({**a, "config": {"database_path": str(db.path)}}))
    monkeypatch.setenv("FEISHU_WORKER_REQUEST", str(path))
    yield db, a, path
    db.close()


def call_hook(monkeypatch, capsys, tool, event, session="target-session"):
    raw = json_line(request(tool_name=tool, hook_event_name=event, session_id=session))
    monkeypatch.setattr(hook.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    assert hook.main() == 0
    output = capsys.readouterr().out
    return json.loads(output)["hookSpecificOutput"] if output else None


@pytest.mark.parametrize(
    "tool",
    [
        "mcp__feishu__run",
        "mcp__feishu__create_document",
        "Edit",
        "Write",
        "NotebookEdit",
        "Skill",
    ],
)
@pytest.mark.parametrize("event", ["PreToolUse", "PermissionRequest"])
def test_approved_worker_tools_do_not_contact_permission_relay(
    worker, monkeypatch, capsys, tool, event
):
    db, a, _ = worker

    def no_relay(*args):
        raise AssertionError("Already authorized tool must not ask again")

    monkeypatch.setattr(hook, "_request_decision", no_relay)
    result = call_hook(monkeypatch, capsys, tool, event)
    if event == "PreToolUse":
        assert result["permissionDecision"] == "allow"
        assert bool(db.attempt(a["attempt_id"])["unsafe_tools"]) == (
            not tool.startswith("mcp__feishu__")
        )
    else:
        assert result["decision"] == {"behavior": "allow"}
    assert db._connection.execute("SELECT count(*) FROM permission_requests").fetchone()[0] == 0


@pytest.mark.parametrize("invalid", ["cancelled", "wrong_token", "wrong_session"])
@pytest.mark.parametrize("event", ["PreToolUse", "PermissionRequest"])
def test_auto_allowed_tools_still_require_current_worker_identity(
    worker, monkeypatch, capsys, invalid, event
):
    db, a, path = worker
    session = "target-session"
    if invalid == "cancelled":
        db.handle_control(IncomingMessage.text("cancel", "chat", "/cancel 1"))
    elif invalid == "wrong_token":
        saved = json.loads(path.read_text())
        saved["token"] = "invalid"
        path.write_text(json.dumps(saved))
    else:
        session = "other-session"
    result = call_hook(monkeypatch, capsys, "mcp__feishu__run", event, session)
    decision = result.get("permissionDecision") or result["decision"]["behavior"]
    assert decision == "deny"


def test_new_policy_does_not_change_unrelated_local_sessions(monkeypatch, capsys):
    monkeypatch.delenv("FEISHU_WORKER_REQUEST", raising=False)
    assert call_hook(monkeypatch, capsys, "Edit", "PreToolUse") is None
    assert call_hook(monkeypatch, capsys, "mcp__feishu__run", "PreToolUse") is None


def test_new_enabled_mcp_is_automatically_authorized(worker, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        hook,
        "_request_decision",
        lambda payload, env: calls.append(payload["tool_name"]) or hook._deny("not approved"),
    )
    result = call_hook(monkeypatch, capsys, "mcp__external__publish", "PermissionRequest")
    assert result["decision"]["behavior"] == "allow"
    assert calls == []


def test_worker_cli_and_hook_share_exact_allowlist():
    flag, tools = worker_allowed_tools_argument().split("=", 1)
    assert flag == "--allowedTools"
    assert tools == "*"
    assert "mcp__feishu__run" in WORKER_AUTO_ALLOW_TOOLS


def test_invalid_worker_policy_fails_without_relay(worker, monkeypatch, capsys):
    db, a, path = worker
    saved = json.loads(path.read_text())
    saved["config"]["worker_permission_policy"] = "invalid"
    path.write_text(json.dumps(saved))
    result = call_hook(monkeypatch, capsys, "mcp__feishu__run", "PreToolUse")
    assert result["permissionDecision"] == "deny"
    assert db.attempt(a["attempt_id"])["failure_reason"] == "permission_policy_error"
    assert db._connection.execute("SELECT count(*) FROM permission_requests").fetchone()[0] == 0


@pytest.mark.parametrize("event", ["PreToolUse", "PermissionRequest"])
def test_finalization_blocks_new_work_but_allows_saved_result_reads(
    worker, monkeypatch, capsys, event
):
    db, a, path = worker
    db.started(a["attempt_id"])
    saved = json.loads(path.read_text())
    saved["soft_deadline_monotonic"] = time.monotonic() - 1
    path.write_text(json.dumps(saved))
    denied = call_hook(monkeypatch, capsys, "mcp__feishu__run", event)
    assert (denied.get("permissionDecision") or denied["decision"]["behavior"]) == "deny"
    allowed = call_hook(monkeypatch, capsys, "mcp__feishu__read_artifact", event)
    assert (allowed.get("permissionDecision") or allowed["decision"]["behavior"]) == "allow"
    assert db.attempt(a["attempt_id"])["state"] == "running"
    assert db._connection.execute("SELECT count(*) FROM permission_requests").fetchone()[0] == 0
