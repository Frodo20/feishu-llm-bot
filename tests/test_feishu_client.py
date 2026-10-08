from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pytest

from feishu_llm_bot.feishu import FeishuClient, outbound_message_uuid


class Response:
    def __init__(self, code: int = 0, data: bytes | None = None) -> None:
        self.code = code
        self.file = None if data is None else SimpleNamespace(read=lambda: data)

    def success(self) -> bool:
        return self.code == 0

    def get_log_id(self) -> str:
        return "log"


class Messages:
    def __init__(self, responses: list[Response] | None = None) -> None:
        self.replies = []
        self.creates = []
        self.responses = list(responses or [Response()])

    def reply(self, request):
        self.replies.append(request)
        return self.responses.pop(0)

    def create(self, request):
        self.creates.append(request)
        return self.responses.pop(0)


class Resources:
    def __init__(self, response: Response) -> None:
        self.response = response
        self.requests = []

    def get(self, request):
        self.requests.append(request)
        return self.response


def client(messages: Messages, resources: Resources | None = None) -> FeishuClient:
    result = object.__new__(FeishuClient)
    result._client = SimpleNamespace(  # noqa: SLF001
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message=messages,
                message_resource=resources,
            )
        )
    )
    result._send_lock = threading.Lock()  # noqa: SLF001
    return result


def test_reply_and_send_markdown_use_native_posts() -> None:
    messages = Messages([Response(), Response()])
    target = client(messages)

    target.reply_markdown("m1", "# Title\n\n**bold**", "uuid-reply")
    target.send_markdown("c1", "- one\n- two", "uuid-create")

    reply = messages.replies[0]
    assert reply.message_id == "m1"
    assert reply.request_body.msg_type == "post"
    assert reply.request_body.uuid == "uuid-reply"
    reply_post = json.loads(reply.request_body.content)
    assert set(reply_post) == {"zh_cn"}
    assert reply_post["zh_cn"]["content"][0][0]["tag"] == "md"

    create = messages.creates[0]
    assert create.receive_id_type == "chat_id"
    assert create.request_body.receive_id == "c1"
    assert create.request_body.msg_type == "post"
    assert create.request_body.uuid == "uuid-create"


def test_progress_card_replies_to_original_message_with_stable_uuid() -> None:
    response = Response()
    response.data = SimpleNamespace(message_id="card-id")
    messages = Messages([response])
    target = client(messages)
    card = {"header": {"title": {"tag": "plain_text", "content": "处理中"}}}
    assert target.reply_card("user-message", card, "progress-uuid") == "card-id"
    request = messages.replies[0]
    assert request.message_id == "user-message"
    assert request.request_body.msg_type == "interactive"
    assert request.request_body.uuid == "progress-uuid"
    assert json.loads(request.request_body.content) == card


def test_received_reaction_uses_original_message_and_known_emoji() -> None:
    reactions = Messages()
    target = client(Messages())
    target._client.im.v1.message_reaction = reactions  # noqa: SLF001
    target.add_received_reaction("user-message")
    request = reactions.creates[0]
    assert request.message_id == "user-message"
    assert request.request_body.reaction_type.emoji_type == "OK"


def test_progress_api_failures_propagate_for_background_retry() -> None:
    target = client(Messages([Response(230001)]))
    with pytest.raises(RuntimeError, match="230001"):
        target.reply_card("message", {}, "uuid")
    target._client.im.v1.message_reaction = Messages([Response(99991672)])  # noqa: SLF001
    with pytest.raises(RuntimeError, match="99991672"):
        target.add_received_reaction("message")


def test_format_error_retries_with_structured_post() -> None:
    messages = Messages([Response(230001), Response()])
    target = client(messages)

    target.reply_markdown("m1", "**bold**", "same-uuid")

    assert len(messages.replies) == 2
    assert [request.request_body.uuid for request in messages.replies] == [
        "same-uuid",
        "same-uuid",
    ]
    first = json.loads(messages.replies[0].request_body.content)
    second = json.loads(messages.replies[1].request_body.content)
    assert first["zh_cn"]["content"][0][0]["tag"] == "md"
    assert second["zh_cn"]["content"][0][0] == {
        "tag": "text",
        "text": "bold",
        "style": ["bold"],
    }


def test_length_and_policy_errors_do_not_retry_post() -> None:
    for code in (230021, 230022):
        messages = Messages([Response(code)])
        target = client(messages)

        try:
            target.reply_markdown("m1", "**bold**", "same-uuid")
        except RuntimeError as exc:
            assert f"code={code}" in str(exc)
        else:
            raise AssertionError("expected post send failure")
        assert len(messages.replies) == 1


def test_outbound_message_uuid_is_stable_and_bounded() -> None:
    first = outbound_message_uuid("fs_0123456789abcdef0123456789abcdef", 7)
    assert first == outbound_message_uuid("fs_0123456789abcdef0123456789abcdef", 7)
    assert first != outbound_message_uuid("fs_0123456789abcdef0123456789abcdef", 8)
    assert len(first) <= 50


def test_text_sends_include_uuid() -> None:
    messages = Messages([Response(), Response()])
    target = client(messages)

    target.reply_text("m1", "reply", "uuid-reply")
    target.send_text("c1", "followup", "uuid-create")

    assert messages.replies[0].request_body.uuid == "uuid-reply"
    assert messages.creates[0].request_body.uuid == "uuid-create"


def test_download_image_uses_message_resource_endpoint() -> None:
    messages = Messages()
    resources = Resources(Response(data=b"image"))
    target = client(messages, resources)

    data = target.download_image("message-id", "image-key")

    assert data == b"image"
    request = resources.requests[0]
    assert request.type == "image"
    assert request.message_id == "message-id"
    assert request.file_key == "image-key"
    assert request.paths == {"message_id": "message-id", "file_key": "image-key"}
    assert request.queries == [("type", "image")]


def test_download_image_failure_does_not_include_key() -> None:
    messages = Messages()
    target = client(messages, Resources(Response(code=99991672)))

    try:
        target.download_image("message-id", "secret-image-key")
    except RuntimeError as exc:
        assert "secret-image-key" not in str(exc)
    else:
        raise AssertionError("expected download failure")
