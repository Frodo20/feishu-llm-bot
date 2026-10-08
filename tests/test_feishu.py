import json
from types import SimpleNamespace

import pytest

from feishu_llm_bot.feishu import FeishuEventHandler


def event(
    *,
    sender: str = "ou_allowed",
    sender_type: str = "user",
    chat_type: str = "p2p",
    message_type: str = "text",
    content: str | None = None,
):
    default_content = {"image_key": "img_v3_safe"} if message_type == "image" else {"text": "hello"}
    return SimpleNamespace(
        event=SimpleNamespace(
            sender=SimpleNamespace(
                sender_type=sender_type,
                sender_id=SimpleNamespace(open_id=sender),
            ),
            message=SimpleNamespace(
                message_id="m1",
                chat_id="c1",
                chat_type=chat_type,
                message_type=message_type,
                content=content if content is not None else json.dumps(default_content),
            ),
        )
    )


def test_allowed_private_text_is_forwarded() -> None:
    received = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
    )
    handler.handle(event())
    assert len(received) == 1
    assert received[0].message_type == "text"
    assert received[0].user_text == "hello"


def test_control_message_is_consumed_before_normal_sink() -> None:
    received = []
    controlled = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
        control_handler=lambda item: controlled.append(item) or True,
    )

    handler.handle(event(content=json.dumps({"text": "同意 safe-token"})))

    assert received == []
    assert len(controlled) == 1
    assert controlled[0].user_text == "同意 safe-token"


def test_image_is_offered_to_control_handler_but_cannot_be_text_authorization() -> None:
    received = []
    controlled = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
        control_handler=lambda item: controlled.append(item) or False,
    )

    handler.handle(event(message_type="image"))

    assert len(controlled) == 1
    assert len(received) == 1
    assert received[0].message_type == "image"


def test_allowed_private_image_is_forwarded() -> None:
    received = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
    )
    handler.handle(event(message_type="image"))
    assert len(received) == 1
    assert received[0].message_type == "image"
    assert received[0].image_key == "img_v3_safe"
    assert received[0].user_text is None


def test_disallowed_messages_are_ignored_before_sink() -> None:
    received = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
    )
    handler.handle(event(sender="ou_other", message_type="image"))
    handler.handle(event(sender_type="bot", message_type="image"))
    handler.handle(event(chat_type="group", message_type="image"))
    handler.handle(event(message_type="file"))
    handler.handle(event(content="invalid"))
    handler.handle(SimpleNamespace(event=None))
    assert received == []


def test_sink_failure_is_propagated_for_feishu_retry() -> None:
    def fail(_item) -> bool:
        raise RuntimeError("database unavailable")

    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=fail,
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        handler.handle(event())


def test_malformed_image_keys_are_ignored() -> None:
    received = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed",
        sink=lambda item: received.append(item) or True,
    )
    for content in (
        "invalid",
        json.dumps({}),
        json.dumps({"image_key": ""}),
        json.dumps({"image_key": " bad"}),
        json.dumps({"image_key": "bad\nkey"}),
        json.dumps({"image_key": "x" * 513}),
    ):
        handler.handle(event(message_type="image", content=content))
    assert received == []
