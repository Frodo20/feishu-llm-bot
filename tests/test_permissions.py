from __future__ import annotations

import json
import re
import socket
import threading
import time
from pathlib import Path

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.permission_relay import PermissionRelay
from feishu_llm_bot.store import Store

SESSION_ID = "64362c4a-5246-489b-bbb9-4e3922196538"


class FakeReplies:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.replied: list[tuple[str, str, str]] = []
        self.ready = threading.Event()
        self.fail_send = False

    def send_text(self, chat_id: str, text: str, send_uuid: str) -> None:
        if self.fail_send:
            raise RuntimeError("send failed")
        self.sent.append((chat_id, text, send_uuid))
        self.ready.set()

    def reply_text(self, message_id: str, text: str, send_uuid: str) -> None:
        self.replied.append((message_id, text, send_uuid))


def make_relay(tmp_path: Path, *, timeout: int = 3):
    store = Store(tmp_path / "bot.db")
    replies = FakeReplies()
    relay = PermissionRelay(
        store=store,
        replies=replies,
        socket_path=tmp_path / "private" / "permission.sock",
        session_id=SESSION_ID,
        chat_id="chat-1",
        timeout_seconds=timeout,
        max_pending=2,
    )
    relay.start()
    return store, replies, relay


def request() -> bytes:
    return (
        json.dumps(
            {
                "session_id": SESSION_ID,
                "tool_name": "Edit",
                "cwd_context": "…/home/test-user",
                "summary": "Edit: file_path=…/project/src/main.py",
            },
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )


def receive_decision(path: Path, payload: bytes = b"") -> tuple[socket.socket, dict[str, str]]:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(5)
    connection.connect(str(path))
    connection.sendall(payload or request())
    return connection, {}


def finish(connection: socket.socket) -> dict[str, str]:
    with connection:
        raw = b""
        while not raw.endswith(b"\n"):
            raw += connection.recv(4096)
    return json.loads(raw)


def token_from(text: str) -> str:
    match = re.search(r"^同意 ([A-Za-z0-9_-]+)$", text, re.MULTILINE)
    assert match is not None
    return match.group(1)


def wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met")


def test_allow_command_resolves_one_request_without_entering_normal_queue(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    token = token_from(replies.sent[0][1])

    assert relay.handle_control(IncomingMessage.text("message-1", "chat-1", f"同意 {token}"))
    assert finish(connection) == {"decision": "allow"}
    assert replies.replied[0][0] == "message-1"
    assert "已同意" in replies.replied[0][1]

    row = store._connection.execute(  # noqa: SLF001 - security assertion
        "SELECT status, token_hash FROM permission_requests"
    ).fetchone()
    assert row["status"] == "allowed"
    assert token not in row["token_hash"]
    relay.stop()
    store.close()


def test_english_deny_alias_and_replay_are_consumed(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    token = token_from(replies.sent[0][1])

    assert relay.handle_control(IncomingMessage.text("message-1", "chat-1", f"/deny {token}"))
    assert finish(connection) == {"decision": "deny"}
    assert relay.handle_control(IncomingMessage.text("message-2", "chat-1", f"同意 {token}"))
    assert "无效" in replies.replied[-1][1]
    assert store._connection.execute(  # noqa: SLF001 - security assertion
        "SELECT status FROM permission_requests"
    ).fetchone()[0] == "denied"
    relay.stop()
    store.close()


def test_wrong_chat_and_malformed_control_commands_cannot_authorize(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    token = token_from(replies.sent[0][1])

    assert relay.handle_control(IncomingMessage.text("wrong-chat", "chat-2", f"同意 {token}"))
    assert "无效" in replies.replied[-1][1]
    assert relay.handle_control(IncomingMessage.text("malformed", "chat-1", "同意 not!valid"))
    assert not relay.handle_control(IncomingMessage.text("ordinary", "chat-1", "please allow it"))
    assert relay.handle_control(IncomingMessage.text("valid", "chat-1", f"拒绝 {token}"))
    assert finish(connection) == {"decision": "deny"}
    relay.stop()
    store.close()


def test_images_never_act_as_permission_controls(tmp_path: Path) -> None:
    store, _replies, relay = make_relay(tmp_path)
    assert not relay.handle_control(IncomingMessage.image("image-1", "chat-1", "image-key"))
    relay.stop()
    store.close()


def test_send_failure_denies_and_closes_pending_request(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    replies.fail_send = True
    connection, _ = receive_decision(relay.socket_path)
    assert finish(connection) == {"decision": "deny"}
    wait_until(
        lambda: store._connection.execute(  # noqa: SLF001 - state assertion
            "SELECT status FROM permission_requests"
        ).fetchone()[0]
        == "denied"
    )
    relay.stop()
    store.close()


def test_invalid_socket_request_is_denied_without_feishu_send(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    connection, _ = receive_decision(relay.socket_path, b'{"session_id":"wrong"}\n')
    assert finish(connection) == {"decision": "deny"}
    assert replies.sent == []
    relay.stop()
    store.close()


def test_disconnected_hook_invalidates_request(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    connection.close()
    wait_until(
        lambda: store._connection.execute(  # noqa: SLF001 - state assertion
            "SELECT status FROM permission_requests"
        ).fetchone()[0]
        == "denied"
    )
    relay.stop()
    store.close()


def test_socket_directory_and_socket_are_owner_only(tmp_path: Path) -> None:
    store, _replies, relay = make_relay(tmp_path)
    assert relay.socket_path.parent.stat().st_mode & 0o777 == 0o700
    assert relay.socket_path.stat().st_mode & 0o777 == 0o600
    relay.stop()
    store.close()


def test_multiple_sessions_require_their_own_approval_tokens(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    relay.all_sessions = True
    other_id = "06f80c84-0d63-4cdd-85bf-de3d9e2186ae"
    first, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    first_token = token_from(replies.sent[0][1])
    payload = json.loads(request())
    payload["session_id"] = other_id
    second, _ = receive_decision(relay.socket_path, json.dumps(payload).encode() + b"\n")
    wait_until(lambda: len(replies.sent) == 2)
    second_token = token_from(replies.sent[1][1])
    assert first_token != second_token
    assert SESSION_ID in replies.sent[0][1]
    assert other_id in replies.sent[1][1]
    relay.handle_control(IncomingMessage.text("allow-second", "chat-1", f"同意 {second_token}"))
    assert finish(second) == {"decision": "allow"}
    relay.handle_control(IncomingMessage.text("deny-first", "chat-1", f"拒绝 {first_token}"))
    assert finish(first) == {"decision": "deny"}
    relay.stop()
    store.close()


def test_expired_request_cannot_be_approved(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path, timeout=1)
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    token = token_from(replies.sent[0][1])
    assert finish(connection) == {"decision": "deny"}
    relay.handle_control(IncomingMessage.text("too-late", "chat-1", f"同意 {token}"))
    assert "无效" in replies.replied[-1][1]
    relay.stop()
    store.close()


def test_other_session_rejected_by_default(tmp_path: Path) -> None:
    store, replies, relay = make_relay(tmp_path)
    payload = json.loads(request())
    payload["session_id"] = "06f80c84-0d63-4cdd-85bf-de3d9e2186ae"
    connection, _ = receive_decision(relay.socket_path, json.dumps(payload).encode() + b"\n")
    assert finish(connection) == {"decision": "deny"}
    assert not replies.sent
    relay.stop()
    store.close()
