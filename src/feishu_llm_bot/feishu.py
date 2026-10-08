from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageReactionRequest,
    CreateMessageReactionRequestBody,
    CreateMessageRequest,
    CreateMessageRequestBody,
    DeleteMessageReactionRequest,
    Emoji,
    GetMessageResourceRequest,
    PatchMessageRequest,
    PatchMessageRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from .text import (
    UnsupportedPost,
    markdown_post_content,
    parse_image_content,
    parse_post_content,
    parse_text_content,
)

LOGGER = logging.getLogger(__name__)
_NATIVE_MARKDOWN_FORMAT_ERROR_CODES = {230001}


def outbound_message_uuid(correlation_id: str, chunk_index: int) -> str:
    if not isinstance(correlation_id, str) or not correlation_id:
        raise ValueError("correlation_id must not be empty")
    if isinstance(chunk_index, bool) or not isinstance(chunk_index, int) or chunk_index < 0:
        raise ValueError("chunk_index must be a non-negative integer")
    digest = hashlib.sha256(f"{correlation_id}\0{chunk_index}".encode()).hexdigest()
    return f"fs-{digest[:40]}"


@dataclass(frozen=True)
class IncomingMessage:
    message_id: str
    chat_id: str
    user_text: str | None = None
    message_type: Literal["text", "image"] = "text"
    image_key: str | None = None

    @classmethod
    def text(cls, message_id: str, chat_id: str, user_text: str) -> IncomingMessage:
        return cls(message_id=message_id, chat_id=chat_id, user_text=user_text)

    @classmethod
    def image(
        cls, message_id: str, chat_id: str, image_key: str, user_text: str | None = None,
    ) -> IncomingMessage:
        return cls(
            message_id=message_id,
            chat_id=chat_id,
            message_type="image",
            image_key=image_key,
            user_text=user_text,
        )


class MessageSink(Protocol):
    def __call__(self, message: IncomingMessage) -> bool: ...


class ControlMessageHandler(Protocol):
    def __call__(self, message: IncomingMessage) -> bool: ...


class FeishuEventHandler:
    def __init__(
        self,
        *,
        allowed_sender_open_id: str,
        sink: MessageSink,
        control_handler: ControlMessageHandler | None = None,
        reject_message: Callable[[str, str], None] | None = None,
    ) -> None:
        self.allowed_sender_open_id = allowed_sender_open_id
        self.sink = sink
        self.control_handler = control_handler
        self.reject_message = reject_message

    def handle(self, data: lark.im.v1.P2ImMessageReceiveV1) -> None:
        event = getattr(data, "event", None)
        sender = getattr(event, "sender", None)
        sender_id = getattr(sender, "sender_id", None)
        message = getattr(event, "message", None)
        if sender is None or sender_id is None or message is None:
            LOGGER.warning("ignored malformed Feishu event")
            return
        if getattr(sender, "sender_type", None) != "user":
            LOGGER.info("ignored non-user Feishu event")
            return
        if getattr(sender_id, "open_id", None) != self.allowed_sender_open_id:
            LOGGER.warning("ignored message from non-allowlisted sender")
            return
        if getattr(message, "chat_type", None) != "p2p":
            LOGGER.info("ignored non-private Feishu message")
            return

        message_id = getattr(message, "message_id", None)
        chat_id = getattr(message, "chat_id", None)
        message_type = getattr(message, "message_type", None)
        content = getattr(message, "content", None)
        if not message_id or not chat_id:
            LOGGER.warning("ignored malformed Feishu message")
            return
        if message_type == "text":
            user_text = parse_text_content(content)
            if user_text is None:
                LOGGER.warning("ignored malformed Feishu text message")
                return
            incoming = IncomingMessage.text(message_id, chat_id, user_text)
        elif message_type == "image":
            image_key = parse_image_content(content)
            if image_key is None:
                LOGGER.warning("ignored malformed Feishu image message")
                return
            incoming = IncomingMessage.image(message_id, chat_id, image_key)
        elif message_type == "post":
            try:
                parsed = parse_post_content(content)
            except UnsupportedPost as exc:
                if self.reject_message is not None:
                    self.reject_message(message_id, str(exc))
                return
            if parsed is None:
                LOGGER.warning("ignored malformed Feishu post")
                return
            text, image_key = parsed
            incoming = (
                IncomingMessage.image(message_id, chat_id, image_key, text or None)
                if image_key else IncomingMessage.text(message_id, chat_id, text)
            )
        else:
            LOGGER.info("ignored unsupported Feishu message type")
            return

        try:
            if self.control_handler is not None and self.control_handler(incoming):
                LOGGER.info("consumed Feishu control message message_id=%s", message_id)
                return
            accepted = self.sink(incoming)
        except Exception:
            LOGGER.error("failed to persist Feishu event")
            raise
        LOGGER.info(
            "accepted Feishu event message_id=%s type=%s queued=%s",
            message_id,
            message_type,
            str(accepted).lower(),
        )


class FeishuClient:
    def __init__(self, *, app_id: str, app_secret: str, timeout: int = 10) -> None:
        self._client = (
            lark.Client.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .timeout(timeout)
            .log_level(lark.LogLevel.WARNING)
            .build()
        )
        self._send_lock = threading.Lock()

    def download_image(self, message_id: str, image_key: str) -> bytes:
        request = (
            GetMessageResourceRequest.builder()
            .type("image")
            .message_id(message_id)
            .file_key(image_key)
            .build()
        )
        response = self._client.im.v1.message_resource.get(request)
        if not response.success() or response.file is None:
            raise RuntimeError(
                f"Feishu image download failed code={response.code} log_id={response.get_log_id()}"
            )
        data = response.file.read()
        if not isinstance(data, bytes):
            raise RuntimeError("Feishu image download returned invalid data")
        return data

    def reply_markdown(self, message_id: str, markdown: str, send_uuid: str) -> None:
        self._send_post(
            reply_to=message_id,
            chat_id=None,
            markdown=markdown,
            send_uuid=send_uuid,
        )

    def send_markdown(self, chat_id: str, markdown: str, send_uuid: str) -> None:
        self._send_post(
            reply_to=None,
            chat_id=chat_id,
            markdown=markdown,
            send_uuid=send_uuid,
        )

    def reply_text(self, message_id: str, text: str, send_uuid: str) -> None:
        request = (
            ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .uuid(send_uuid)
                .build()
            )
            .build()
        )
        with self._send_lock:
            response = self._client.im.v1.message.reply(request)
        if not response.success():
            raise RuntimeError(
                f"Feishu reply failed code={response.code} log_id={response.get_log_id()}"
            )

    def send_text(self, chat_id: str, text: str, send_uuid: str) -> None:
        request = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .uuid(send_uuid)
                .build()
            )
            .build()
        )
        with self._send_lock:
            response = self._client.im.v1.message.create(request)
        if not response.success():
            raise RuntimeError(
                f"Feishu send failed code={response.code} log_id={response.get_log_id()}"
            )

    def send_card(self, chat_id: str, card: dict, send_uuid: str) -> str:
        request = (
            CreateMessageRequest.builder().receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder().receive_id(chat_id)
                .msg_type("interactive").content(json.dumps(card, ensure_ascii=False))
                .uuid(send_uuid).build()
            ).build()
        )
        with self._send_lock:
            response = self._client.im.v1.message.create(request)
        if not response.success():
            raise RuntimeError(
                f"Feishu card send failed code={response.code} log_id={response.get_log_id()}"
            )
        message_id = getattr(response.data, "message_id", None)
        if not isinstance(message_id, str) or not message_id:
            raise RuntimeError("Feishu card send returned no message ID")
        return message_id

    def add_received_reaction(self, message_id: str, emoji_type: str = "OK") -> None:
        self._add_reaction(message_id, emoji_type)

    def add_reaction(self, message_id: str, emoji_type: str) -> str:
        reaction_id = self._add_reaction(message_id, emoji_type)
        if not reaction_id:
            raise RuntimeError("Feishu reaction returned no reaction ID")
        return reaction_id

    def _add_reaction(self, message_id: str, emoji_type: str) -> str | None:
        if emoji_type not in {"OK", "THUMBSUP", "FISTBUMP", "THINKING", "DONE", "ERROR", "THANKS"}:
            raise ValueError("Unsupported task reaction")
        request = (
            CreateMessageReactionRequest.builder().message_id(message_id)
            .request_body(CreateMessageReactionRequestBody.builder()
                          .reaction_type(Emoji.builder().emoji_type(emoji_type).build()).build())
            .build()
        )
        with self._send_lock:
            response = self._client.im.v1.message_reaction.create(request)
        if not response.success():
            raise RuntimeError(f"Feishu reaction failed code={response.code}")
        ident = getattr(getattr(response, "data", None), "reaction_id", None)
        return ident if isinstance(ident, str) and ident else None

    def remove_reaction(self, message_id: str, reaction_id: str) -> None:
        request = (DeleteMessageReactionRequest.builder().message_id(message_id)
                   .reaction_id(reaction_id).build())
        with self._send_lock:
            response = self._client.im.v1.message_reaction.delete(request)
        if not response.success():
            raise RuntimeError(f"Feishu reaction removal failed code={response.code}")

    def reply_card(self, message_id: str, card: dict, send_uuid: str) -> str:
        request = (
            ReplyMessageRequest.builder().message_id(message_id)
            .request_body(ReplyMessageRequestBody.builder().msg_type("interactive")
                          .content(json.dumps(card, ensure_ascii=False))
                          .uuid(send_uuid).build()).build()
        )
        with self._send_lock:
            response = self._client.im.v1.message.reply(request)
        if not response.success():
            raise RuntimeError(f"Feishu progress card failed code={response.code}")
        result = getattr(response.data, "message_id", None)
        if not isinstance(result, str) or not result:
            raise RuntimeError("Feishu progress card returned no message ID")
        return result

    def update_card(self, message_id: str, card: dict) -> None:
        request = (
            PatchMessageRequest.builder().message_id(message_id)
            .request_body(
                PatchMessageRequestBody.builder().content(json.dumps(card, ensure_ascii=False))
                .build()
            ).build()
        )
        with self._send_lock:
            response = self._client.im.v1.message.patch(request)
        if not response.success():
            raise RuntimeError(
                f"Feishu card update failed code={response.code} log_id={response.get_log_id()}"
            )

    def _send_post(
        self,
        *,
        reply_to: str | None,
        chat_id: str | None,
        markdown: str,
        send_uuid: str,
    ) -> None:
        with self._send_lock:
            response = self._post_request(
                reply_to=reply_to,
                chat_id=chat_id,
                content=markdown_post_content(markdown, native=True),
                send_uuid=send_uuid,
            )
            if not response.success() and response.code in _NATIVE_MARKDOWN_FORMAT_ERROR_CODES:
                response = self._post_request(
                    reply_to=reply_to,
                    chat_id=chat_id,
                    content=markdown_post_content(markdown, native=False),
                    send_uuid=send_uuid,
                )
        if not response.success():
            operation = "reply" if reply_to else "send"
            raise RuntimeError(
                f"Feishu {operation} failed code={response.code} log_id={response.get_log_id()}"
            )

    def _post_request(
        self,
        *,
        reply_to: str | None,
        chat_id: str | None,
        content: str,
        send_uuid: str,
    ):
        if reply_to is not None:
            request = (
                ReplyMessageRequest.builder()
                .message_id(reply_to)
                .request_body(
                    ReplyMessageRequestBody.builder()
                    .msg_type("post")
                    .content(content)
                    .uuid(send_uuid)
                    .build()
                )
                .build()
            )
            return self._client.im.v1.message.reply(request)
        if chat_id is None:
            raise ValueError("chat_id is required for a new message")
        request = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("post")
                .content(content)
                .uuid(send_uuid)
                .build()
            )
            .build()
        )
        return self._client.im.v1.message.create(request)


FeishuReplyClient = FeishuClient


def build_websocket_client(
    *,
    app_id: str,
    app_secret: str,
    callback: Callable[[lark.im.v1.P2ImMessageReceiveV1], None],
    card_callback: Callable[[P2CardActionTrigger], P2CardActionTriggerResponse] | None = None,
) -> lark.ws.Client:
    builder = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(callback)
    )
    if card_callback is not None:
        builder.register_p2_card_action_trigger(card_callback)
    dispatcher = builder.build()
    return lark.ws.Client(
        app_id,
        app_secret,
        event_handler=dispatcher,
        log_level=lark.LogLevel.ERROR,
        domain=lark.FEISHU_DOMAIN,
    )
