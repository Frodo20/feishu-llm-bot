from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTrigger
from lark_oapi.ws.pb.pbbp2_pb2 import Frame
from test_feishu_client import Messages, Response, client
from test_permissions import SESSION_ID, FakeReplies, finish, receive_decision, wait_until

from feishu_llm_bot.feishu import FeishuClient, build_websocket_client
from feishu_llm_bot.permission_relay import PermissionRelay
from feishu_llm_bot.store import Store


class CardReplies(FakeReplies):
    def __init__(self):
        super().__init__()
        self.cards = []
        self.updates = []

    def send_card(self, chat_id, card, send_uuid):
        if self.fail_send:
            raise RuntimeError("send failed")
        self.cards.append(card)
        self.ready.set()
        return "om_card_1"

    def update_card(self, message_id, card):
        self.updates.append((message_id, card))


@pytest.fixture
def approval(tmp_path):
    store = Store(tmp_path / "bot.db")
    replies = CardReplies()
    relay = PermissionRelay(
        store=store, replies=replies, socket_path=tmp_path / "private/relay.sock",
        session_id=SESSION_ID, chat_id="chat-1", timeout_seconds=3, max_pending=4,
        cards_enabled=True, allowed_sender_open_id="ou_owner", app_id="cli_test",
    )
    relay.start()
    connection, _ = receive_decision(relay.socket_path)
    assert replies.ready.wait(2)
    value = replies.cards[0]["elements"][1]["actions"][0]["value"]
    wait_until(
        lambda: store.get_permission_request(value["request_id"]).card_message_id is not None
    )
    yield store, replies, relay, connection, value
    connection.close()
    relay.stop()
    store.close()


def callback(value, *, user="ou_owner", chat="chat-1", message="om_card_1", event_id="evt1"):
    return {
        "schema": "2.0", "header": {
            "event_id": event_id, "event_type": "card.action.trigger", "app_id": "cli_test",
        },
        "event": {
            "operator": {"open_id": user}, "host": "im_message",
            "context": {"open_chat_id": chat, "open_message_id": message},
            "action": {"tag": "button", "value": value},
        },
    }


@pytest.mark.parametrize("decision,expected", [("allowed", "allow"), ("denied", "deny")])
def test_click_returns_to_waiting_hook_and_closes_card(approval, decision, expected):
    store, replies, relay, connection, value = approval
    value = {**value, "decision": decision}
    start = time.monotonic()
    response = relay.handle_card(P2CardActionTrigger(callback(value)))
    assert time.monotonic() - start < 1
    assert response.toast.type == "success"
    assert finish(connection) == {"decision": expected}
    assert not any(e["tag"] == "action" for e in response.card.data["elements"])
    opposite = {**value, "decision": "denied" if decision == "allowed" else "allowed"}
    repeated = relay.handle_card(P2CardActionTrigger(callback(opposite, event_id="evt2")))
    assert repeated.toast.type == "info"
    assert store.get_permission_request(value["request_id"]).status == decision
    wait_until(lambda: bool(replies.updates))


@pytest.mark.parametrize("overrides", [
    {"user": "ou_intruder"}, {"chat": "other"}, {"message": "forwarded-card"},
])
def test_card_identity_and_message_binding(approval, overrides):
    store, _, relay, _, value = approval
    response = relay.handle_card(P2CardActionTrigger(callback(value, **overrides)))
    assert response.toast.type == "error"
    assert store.get_permission_request(value["request_id"]).status == "pending"


@pytest.mark.parametrize("change", [
    {"token": "bad"}, {"decision": "always_allow"}, {"request_id": "pr_wrong"},
    {"extra": "injected"}, {"token": "x" * 16},
])
def test_malformed_or_forged_card_cannot_authorize(approval, change):
    store, _, relay, _, value = approval
    response = relay.handle_card(P2CardActionTrigger(callback({**value, **change})))
    assert response.toast.type == "error"
    assert store.get_permission_request(value["request_id"]).status == "pending"


def test_timeout_card_is_closed_and_late_click_does_not_allow(approval):
    store, replies, relay, connection, value = approval
    store.expire_permission_requests(now=int(time.time()) + 10)
    response = relay.handle_card(P2CardActionTrigger(callback(value)))
    assert response.toast.type == "info"
    assert finish(connection) == {"decision": "deny"}
    wait_until(lambda: bool(replies.updates))
    assert not any(e["tag"] == "action" for e in replies.updates[-1][1]["elements"])


def test_busy_database_returns_before_feishu_callback_deadline(approval):
    store, _, relay, _, value = approval
    with sqlite3.connect(store.path) as busy:
        busy.execute("BEGIN IMMEDIATE")
        start = time.monotonic()
        response = relay.handle_card(P2CardActionTrigger(callback(value)))
        assert time.monotonic() - start < 1
        assert response.toast.type == "error"


def test_concurrent_allow_and_deny_resolve_only_once(approval):
    store, _, relay, connection, value = approval
    callbacks = [
        P2CardActionTrigger(callback({**value, "decision": decision}, event_id=decision))
        for decision in ["allowed", "denied"]
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(relay.handle_card, callbacks))
    assert sum(response.toast.type == "success" for response in responses) == 1
    saved = store.get_permission_request(value["request_id"])
    assert finish(connection) == {"decision": "allow" if saved.status == "allowed" else "deny"}


def test_wrong_app_or_non_message_callback_is_rejected(approval):
    store, _, relay, _, value = approval
    payload = callback(value)
    payload["header"]["app_id"] = "other_app"
    assert relay.handle_card(P2CardActionTrigger(payload)).toast.type == "error"
    payload = callback(value)
    payload["event"]["host"] = "url_preview"
    assert relay.handle_card(P2CardActionTrigger(payload)).toast.type == "error"
    assert store.get_permission_request(value["request_id"]).status == "pending"


def test_card_delivery_failure_returns_deny_without_text_fallback(tmp_path):
    store = Store(tmp_path / "bot.db")
    replies = CardReplies()
    replies.fail_send = True
    relay = PermissionRelay(
        store=store, replies=replies, socket_path=tmp_path / "private/relay.sock",
        session_id=SESSION_ID, chat_id="chat-1", timeout_seconds=2, max_pending=2,
        cards_enabled=True, allowed_sender_open_id="ou_owner", app_id="cli_test",
    )
    relay.start()
    connection, _ = receive_decision(relay.socket_path)
    assert finish(connection) == {"decision": "deny"}
    assert replies.cards == [] and replies.sent == []
    relay.stop()
    store.close()


def test_card_callback_roundtrip_through_actual_sdk_websocket_frame(approval):
    _, _, relay, connection, value = approval
    ws = build_websocket_client(
        app_id="cli_test", app_secret="secret", callback=lambda _: None,
        card_callback=relay.handle_card,
    )
    frame = Frame(SeqID=1, LogID=2, service=1, method=1)
    for key, val in {"message_id": "frame1", "trace_id": "trace1", "sum": "1",
                     "seq": "0", "type": "event"}.items():
        header = frame.headers.add()
        header.key, header.value = key, val
    frame.payload = json.dumps(callback(value)).encode()
    written = []

    async def write(data):
        written.append(data)

    ws._write_message = write  # noqa: SLF001 - actual SDK transport test
    asyncio.run(ws._handle_data_frame(frame))  # noqa: SLF001
    result = Frame()
    result.ParseFromString(written[0])
    response = json.loads(result.payload)
    assert response["code"] == 200
    body = json.loads(base64.b64decode(response["data"]))
    assert body["toast"]["type"] == "success"
    assert finish(connection) == {"decision": "allow"}


def test_card_send_uses_interactive_message_and_returns_message_id():
    response = Response()
    response.data = type("Data", (), {"message_id": "om_new"})()
    messages = Messages([response])
    target: FeishuClient = client(messages)
    assert target.send_card("chat", {"elements": []}, "unique") == "om_new"
    request = messages.creates[0]
    assert request.request_body.msg_type == "interactive"
    assert request.request_body.receive_id == "chat"
    assert request.request_body.uuid == "unique"


def test_permission_config_is_idempotent_and_removes_duplicate_flux_gate():
    path = Path(__file__).parents[1] / "scripts/configure_claude_permissions.py"
    spec = importlib.util.spec_from_file_location("configure", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original = {"hooks": {
        "PermissionRequest": [{"hooks": [{"command": "/bin/flux-hooks-claude"}]}],
        "PostToolUse": [{"hooks": [{"command": "/bin/flux-hooks-claude"}]}],
    }}
    result = module.configure(original, path.parents[1])
    assert module.configure(result, path.parents[1]) == result
    assert len(result["hooks"]["PermissionRequest"]) == 1
    assert result["hooks"]["PostToolUse"] == original["hooks"]["PostToolUse"]
    assert {"Bash", "Read", "WebFetch", "WebSearch", "Edit"} <= set(
        result["permissions"]["allow"]
    )
    assert result["permissions"]["defaultMode"] == "acceptEdits"


def test_existing_v4_permission_requests_migrate_without_losing_state(tmp_path):
    path = tmp_path / "old.db"
    store = Store(path)
    from feishu_llm_bot.permission_relay import _token_hash
    record = store.create_permission_request(
        request_id="pr_" + "a" * 32, session_id=SESSION_ID, chat_id="chat-1",
        tool_name="Edit", summary="old request", token_hash=_token_hash("x" * 16),
        created_at=100, expires_at=200, max_pending=4,
    )
    store.close()
    with sqlite3.connect(path) as old:
        old.execute("ALTER TABLE permission_requests DROP COLUMN card_message_id")
        old.execute("PRAGMA user_version=4")
    store = Store(path)
    restored = store.get_permission_request(record.request_id)
    assert restored == record
    store.bind_permission_card(record.request_id, "om_bound")
    assert store.get_permission_request(record.request_id).card_message_id == "om_bound"
    store.close()
