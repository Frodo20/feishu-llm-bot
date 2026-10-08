from __future__ import annotations

import importlib.util
import io
import json
import socket
import threading
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "claude_permission_hook.py"
SPEC = importlib.util.spec_from_file_location("claude_permission_hook", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
hook = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(hook)


def request(**overrides):
    value = {
        "session_id": "target-session",
        "transcript_path": "/secret/transcript.jsonl",
        "cwd": "/home/test-user/project",
        "permission_mode": "default",
        "hook_event_name": "PermissionRequest",
        "tool_name": "Edit",
        "tool_input": {"file_path": "/home/test-user/project/src/main.py"},
        "permission_suggestions": [{"secret": "must-not-leak"}],
    }
    value.update(overrides)
    return value


def json_line(value) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


def test_parse_request_is_strict() -> None:
    parsed = hook._parse_request(json_line(request()))
    assert parsed["session_id"] == "target-session"

    for invalid in (
        [],
        request(hook_event_name="Stop"),
        request(tool_input="not-an-object"),
        {**request(), "unexpected": True},
    ):
        with pytest.raises(ValueError, match="invalid shape"):
            hook._parse_request(json_line(invalid))


def test_current_cli_permission_metadata_is_accepted_without_relaying_it():
    parsed = hook._parse_request(json_line(request(effort="high", prompt_id="prompt-id")))
    assert parsed["session_id"] == "target-session"
    assert "effort" not in hook._relay_payload(parsed)
    assert "prompt_id" not in hook._relay_payload(parsed)


def test_stdin_limit_is_enforced() -> None:
    class Stream:
        buffer = io.BytesIO(b"x" * (hook._MAX_STDIN_BYTES + 1))

    with pytest.raises(ValueError, match="input limit"):
        hook._read_stdin(Stream())


def test_relay_payload_contains_only_sanitized_fields() -> None:
    payload = hook._relay_payload(request())
    assert set(payload) == {"session_id", "tool_name", "cwd_context", "summary"}
    encoded = json.dumps(payload)
    assert "transcript" not in encoded
    assert "must-not-leak" not in encoded
    assert "main.py" in encoded


def test_bash_command_and_description_are_hidden() -> None:
    payload = hook._relay_payload(
        request(
            tool_name="Bash",
            tool_input={
                "command": "curl https://example.test/?token=very-secret",
                "description": "upload token very-secret",
            },
        )
    )
    encoded = json.dumps(payload)
    assert "very-secret" not in encoded
    assert "curl" not in encoded
    assert payload["summary"] == "Bash command (details hidden)"


def test_url_summary_removes_query_and_fragment() -> None:
    payload = hook._relay_payload(
        request(
            tool_name="WebFetch",
            tool_input={"url": "https://example.test/a/path?token=secret#private"},
        )
    )
    assert payload["summary"] == "WebFetch: url=https://example.test/a/path"


def test_unknown_mcp_arguments_are_hidden() -> None:
    payload = hook._relay_payload(
        request(tool_name="mcp__server__tool", tool_input={"password": "secret"})
    )
    assert payload["summary"] == "mcp__server__tool (arguments hidden)"


def serve_once(path: Path, response: bytes, captured: list[dict]) -> threading.Thread:
    ready = threading.Event()

    def serve() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(path))
            server.listen(1)
            ready.set()
            connection, _ = server.accept()
            with connection:
                raw = b""
                while not raw.endswith(b"\n"):
                    raw += connection.recv(4096)
                captured.append(json.loads(raw))
                connection.sendall(response)

    thread = threading.Thread(target=serve)
    thread.start()
    assert ready.wait(2)
    return thread


@pytest.mark.parametrize(
    ("relay_decision", "expected"),
    [
        ("allow", {"behavior": "allow"}),
        (
            "deny",
            {
                "behavior": "deny",
                "message": "Denied through Feishu",
                "interrupt": False,
            },
        ),
    ],
)
def test_socket_decisions_are_translated_exactly(
    tmp_path: Path, relay_decision: str, expected: dict
) -> None:
    path = tmp_path / "relay.sock"
    captured: list[dict] = []
    thread = serve_once(path, json_line({"decision": relay_decision}), captured)
    result = hook._request_decision(
        hook._relay_payload(request()),
        {
            "FEISHU_PERMISSION_SOCKET_PATH": str(path),
            "FEISHU_PERMISSION_TIMEOUT_SECONDS": "2",
        },
    )
    thread.join(2)
    assert not thread.is_alive()
    assert result["hookSpecificOutput"]["decision"] == expected
    assert captured == [hook._relay_payload(request())]


@pytest.mark.parametrize(
    "response",
    [
        b"not json\n",
        json_line({"decision": "ask"}),
        json_line({"decision": "allow", "extra": True}),
        b"{\"decision\":\"allow\"}",
    ],
)
def test_invalid_socket_responses_are_rejected(tmp_path: Path, response: bytes) -> None:
    path = tmp_path / "relay.sock"
    captured: list[dict] = []
    thread = serve_once(path, response, captured)
    with pytest.raises(ValueError):
        hook._request_decision(
            hook._relay_payload(request()),
            {
                "FEISHU_PERMISSION_SOCKET_PATH": str(path),
                "FEISHU_PERMISSION_TIMEOUT_SECONDS": "2",
            },
        )
    thread.join(2)


def test_timeout_configuration_is_bounded() -> None:
    for raw in ("0", "3601", "invalid"):
        with pytest.raises(ValueError):
            hook._timeout({"FEISHU_PERMISSION_TIMEOUT_SECONDS": raw})
    assert hook._timeout({"FEISHU_PERMISSION_TIMEOUT_SECONDS": "10"}) == 10


def test_delegate_preserves_stdin_and_exit_code(tmp_path: Path, capfd) -> None:
    output = tmp_path / "stdin"
    delegate = tmp_path / "delegate.py"
    delegate.write_text(
        "#!/usr/bin/env python3\n"
        "import pathlib,sys\n"
        f"pathlib.Path({str(output)!r}).write_bytes(sys.stdin.buffer.read())\n"
        "sys.stdout.write('delegate-output')\n"
        "raise SystemExit(7)\n"
    )
    delegate.chmod(0o700)
    raw = json_line(request(session_id="other-session"))
    result = hook._delegate_to_flux(raw, {"FEISHU_PERMISSION_FLUX_HOOK": str(delegate)})
    assert result == 7
    assert output.read_bytes() == raw
    assert capfd.readouterr().out == "delegate-output"


def test_main_fails_closed_on_missing_socket(monkeypatch, capsys) -> None:
    raw = json_line(request())

    class Stream:
        buffer = io.BytesIO(raw)

    monkeypatch.setattr(hook.sys, "stdin", Stream())
    monkeypatch.setattr(
        hook.os,
        "environ",
        {
            "FEISHU_PERMISSION_SESSION_ID": "target-session",
            "FEISHU_PERMISSION_SOCKET_PATH": "/does/not/exist",
            "FEISHU_PERMISSION_TIMEOUT_SECONDS": "1",
        },
    )
    assert hook.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": {
                "behavior": "deny",
                "message": "Feishu permission relay unavailable; request denied",
                "interrupt": False,
            },
        }
    }


@pytest.mark.parametrize("event", ["PreToolUse", "PermissionRequest"])
def test_all_bash_is_allowed_without_relay_configuration(monkeypatch, capsys, event):
    raw = json_line(request(
        hook_event_name=event, tool_name="Bash", tool_use_id="tool-123",
        tool_input={"command": """python3 - <<'PY'
print('ok')
PY"""},
    ))
    monkeypatch.setattr(hook.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    monkeypatch.setattr(hook.os, "environ", {})
    assert hook.main() == 0
    output = json.loads(capsys.readouterr().out)["hookSpecificOutput"]
    assert output["hookEventName"] == event
    if event == "PreToolUse":
        assert output["permissionDecision"] == "allow"
    else:
        assert output["decision"] == {"behavior": "allow"}


def test_all_sessions_routes_other_session_to_socket(monkeypatch, capsys, tmp_path):
    path = tmp_path / "relay.sock"
    captured = []
    thread = serve_once(path, json_line({"decision": "allow"}), captured)
    raw = json_line(request(session_id="other-session"))
    monkeypatch.setattr(hook.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    monkeypatch.setattr(hook.os, "environ", {
        "FEISHU_PERMISSION_ALL_SESSIONS": "true",
        "FEISHU_PERMISSION_SOCKET_PATH": str(path),
        "FEISHU_PERMISSION_TIMEOUT_SECONDS": "2",
    })
    assert hook.main() == 0
    thread.join(2)
    assert captured[0]["session_id"] == "other-session"
    assert json.loads(capsys.readouterr().out)["hookSpecificOutput"]["decision"] == {
        "behavior": "allow"
    }


def test_non_bash_pretool_does_not_allow_or_send(monkeypatch, capsys):
    raw = json_line(request(hook_event_name="PreToolUse"))
    monkeypatch.setattr(hook.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    monkeypatch.setattr(hook.os, "environ", {})
    assert hook.main() == 0
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("tool", [
    "Read", "Glob", "Grep", "WebFetch", "WebSearch", "ReadMcpResourceTool",
    "mcp__feishu__cli_help", "mcp__feishu__search_documents", "mcp__feishu__fetch_document",
])
@pytest.mark.parametrize("event", ["PreToolUse", "PermissionRequest"])
def test_read_tools_do_not_wait_for_remote_approval(monkeypatch, capsys, tool, event):
    raw = json_line(request(tool_name=tool, hook_event_name=event))
    monkeypatch.setattr(hook.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    monkeypatch.setattr(hook.os, "environ", {})
    assert hook.main() == 0
    output = json.loads(capsys.readouterr().out)["hookSpecificOutput"]
    if event == "PreToolUse":
        assert output["permissionDecision"] == "allow"
    else:
        assert output["decision"] == {"behavior": "allow"}
