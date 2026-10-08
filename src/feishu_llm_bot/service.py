from __future__ import annotations

import base64
import contextlib
import logging
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .attachments import AttachmentStore
from .card_delivery import answer_pages, card_answer_delivered
from .feishu import IncomingMessage, outbound_message_uuid
from .store import EventRecord, Store, acquisition_lease, reply_lease, startup_lease
from .text import split_markdown_reply, split_reply

LOGGER = logging.getLogger(__name__)
_IMAGE_FAILURE_MESSAGE = (
    "抱歉，这张图片无法处理。请发送 JPEG、PNG 或 WebP 图片，并确认图片未超过限制。"
)


class FeishuClientProtocol(Protocol):
    def download_image(self, message_id: str, image_key: str) -> bytes: ...

    def reply_markdown(self, message_id: str, markdown: str, send_uuid: str) -> None: ...

    def send_markdown(self, chat_id: str, markdown: str, send_uuid: str) -> None: ...

    def reply_text(self, message_id: str, text: str, send_uuid: str) -> None: ...

    def send_text(self, chat_id: str, text: str, send_uuid: str) -> None: ...


@dataclass(frozen=True)
class ImageResult:
    data: str
    mime_type: str
    byte_length: int


class SessionBridgeService:
    def __init__(
        self,
        *,
        store: Store,
        replies: FeishuClientProtocol,
        emit_inbound: Callable[[EventRecord], None],
        reply_chunk_chars: int,
        max_inbound_chars: int,
        queue_size: int,
        attachments: AttachmentStore | None = None,
        progress_state_path: Path | None = None,
        dispatch_enabled: bool = True,
    ) -> None:
        self.store = store
        self.replies = replies
        self.emit_inbound = emit_inbound
        self.reply_chunk_chars = reply_chunk_chars
        self.max_inbound_chars = max_inbound_chars
        self.attachments = attachments
        self.progress_state_path = progress_state_path
        self.dispatch_enabled = dispatch_enabled
        self._wakeups: queue.Queue[object] = queue.Queue(maxsize=queue_size)
        self._reply_lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self._acquisition_executor: ThreadPoolExecutor | None = None
        self._acquisition_futures: set[Future[None]] = set()
        self._closed = False

    def start(self, *, fail_interrupted: bool = False) -> None:
        if self._closed or self._worker is not None:
            raise RuntimeError("session bridge service cannot be started twice")
        with startup_lease(self.store.path):
            if fail_interrupted:
                count = self.store.fail_interrupted_deliveries()
                if count:
                    LOGGER.warning("Resident restart marked %d interrupted requests failed", count)
            self._recover_attachments()
        self.store.prune_events()
        self._acquisition_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="feishu-image-acquisition",
        )
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="feishu-session-bridge-worker",
            daemon=True,
        )
        self._worker.start()
        self._notify()
        LOGGER.info("Feishu session bridge worker started")

    def stop(self, timeout: float = 15.0) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._stop.set()
            self._notify()
            executor = self._acquisition_executor
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)

        deadline = time.monotonic() + timeout
        if self._worker is not None:
            self._worker.join(max(0.0, deadline - time.monotonic()))
            if self._worker.is_alive():
                raise TimeoutError("session bridge worker did not stop")

        futures = tuple(self._acquisition_futures)
        if futures:
            _done, pending = wait(futures, timeout=max(0.0, deadline - time.monotonic()))
            if pending:
                raise TimeoutError("image acquisition did not stop")

        with self._lifecycle_lock:
            self.store.close()
            self._closed = True

    def accept(self, message: IncomingMessage) -> bool:
        if message.message_type == "text":
            if not message.user_text:
                raise ValueError("Feishu text message must not be empty")
            if len(message.user_text) > self.max_inbound_chars:
                raise ValueError(f"Feishu message exceeds {self.max_inbound_chars} characters")
            accepted = self.store.accept_event(
                message.message_id,
                message.chat_id,
                message.user_text,
            )
        elif message.message_type == "image":
            if message.user_text is not None and len(message.user_text) > self.max_inbound_chars:
                raise ValueError(
                    f"Feishu image caption exceeds {self.max_inbound_chars} characters"
                )
            accepted = self._reserve_image(message)
        else:
            raise ValueError("unsupported Feishu message type")
        if accepted:
            self._notify()
        return accepted

    def _reserve_image(self, message: IncomingMessage) -> bool:
        if not message.image_key:
            raise ValueError("Feishu image message has no image key")
        if self.attachments is None:
            raise RuntimeError("image attachments are not configured")
        with self._lifecycle_lock:
            if self._closed or self._stop.is_set() or self._acquisition_executor is None:
                raise RuntimeError("session bridge service is not accepting images")
            event = self.store.reserve_image(
                message.message_id, message.chat_id, message.user_text,
            )
            if event is None:
                return False
            if event.attachment_token is None:
                raise RuntimeError("reserved image has no attachment token")
            future = self._acquisition_executor.submit(
                self._acquire_image,
                message,
                event,
            )
            self._acquisition_futures.add(future)
            future.add_done_callback(self._acquisition_done)
        return True

    def reject_message(self, message_id: str, text: str) -> None:
        # Never block the WebSocket thread (card callbacks share its 3s deadline).
        with self._lifecycle_lock:
            if self._closed or self._stop.is_set() or self._acquisition_executor is None:
                return
            future = self._acquisition_executor.submit(
                self.replies.reply_markdown, message_id, text,
                outbound_message_uuid(f"unsupported:{message_id}", 0),
            )
            self._acquisition_futures.add(future)
            future.add_done_callback(self._acquisition_done)

    def _acquisition_done(self, future: Future[None]) -> None:
        with contextlib.suppress(Exception):
            future.result()
        with self._lifecycle_lock:
            self._acquisition_futures.discard(future)

    def _acquire_image(self, message: IncomingMessage, event: EventRecord) -> None:
        if self.attachments is None or event.attachment_token is None:
            return
        attachment_saved = False
        try:
            with acquisition_lease(self.store.path):
                data = self.replies.download_image(message.message_id, message.image_key or "")
                if len(data) > self.attachments.max_image_bytes:
                    raise ValueError("image exceeds configured byte limit")
                metadata = self.attachments.save(event.attachment_token, data)
                attachment_saved = True
                if not self.store.finish_image_acquisition(
                    event.correlation_id,
                    mime_type=metadata.mime_type,
                    byte_size=metadata.byte_size,
                    sha256=metadata.sha256,
                    width=metadata.width,
                    height=metadata.height,
                ):
                    raise RuntimeError("image acquisition state changed unexpectedly")
                self._notify()
        except Exception:
            LOGGER.warning(
                "Feishu image acquisition failed correlation_id=%s",
                event.correlation_id,
            )
            try:
                self.store.mark_failed(event.correlation_id, "image_acquisition_failed")
            finally:
                if attachment_saved:
                    try:
                        self._cleanup_attachment(event.correlation_id, event.attachment_token)
                    except Exception:
                        LOGGER.exception(
                            "attachment cleanup checkpoint failed correlation_id=%s",
                            event.correlation_id,
                        )
            with contextlib.suppress(Exception):
                self.replies.reply_markdown(
                    message.message_id,
                    _IMAGE_FAILURE_MESSAGE,
                    outbound_message_uuid(f"{event.correlation_id}:rejection", 0),
                )

    def mark_delivered(self, correlation_id: str) -> bool:
        delivered = self.store.mark_delivered(correlation_id)
        if delivered:
            self._notify()
        return delivered

    def read_image(self, correlation_id: str) -> ImageResult:
        if self.attachments is None:
            raise ValueError("image unavailable for this correlation")
        event = self.store.get_by_correlation(correlation_id)
        if (
            event is None
            or event.message_type != "image"
            or event.status not in {"dispatching", "delivered", "replying"}
            or event.attachment_token is None
            or event.attachment_mime not in {"image/jpeg", "image/png", "image/webp"}
            or event.attachment_size is None
            or event.attachment_sha256 is None
        ):
            raise ValueError("image unavailable for this correlation")
        try:
            data = self.attachments.read(
                event.attachment_token,
                expected_size=event.attachment_size,
                expected_sha256=event.attachment_sha256,
            )
        except Exception as exc:
            LOGGER.warning(
                "attachment integrity check failed correlation_id=%s",
                correlation_id,
            )
            raise ValueError("image unavailable for this correlation") from exc
        return ImageResult(
            data=base64.b64encode(data).decode("ascii"),
            mime_type=event.attachment_mime,
            byte_length=len(data),
        )

    def reply(self, correlation_id: str, text: str) -> str:
        if not text:
            raise ValueError("reply text must not be empty")
        with self._reply_lock, reply_lease(self.store.path):
            existing = self.store.get_by_correlation(correlation_id)
            if existing is None:
                raise ValueError("unknown correlation_id")
            if existing.status == "replied":
                if existing.reply_text is not None and existing.reply_text != text:
                    raise ValueError("correlation already has a different reply")
                return "already sent"

            reply_text = existing.reply_text or text
            reply_format = existing.reply_format or (
                "card_v1" if self.progress_state_path is not None else "post_v2"
            )
            if reply_format == "card_v1":
                answer_pages(text)  # Validate before persisting an immutable reply plan.
                self.store.begin_reply(correlation_id, text, "card_v1", [text])
                self._notify()
                return "Final answer saved; delivery in the progress card is queued."
            if existing.reply_chunks is not None:
                chunks = list(existing.reply_chunks)
            elif reply_format == "plain_v1":
                chunks = split_reply(reply_text, self.reply_chunk_chars)
            else:
                chunks = split_markdown_reply(reply_text, self.reply_chunk_chars)
            event = self.store.begin_reply(
                correlation_id,
                text,
                "post_v2",
                chunks,
            )
            persisted_chunks = event.reply_chunks
            if persisted_chunks is None:
                raise RuntimeError("reply chunk plan was not persisted")
            chunks = list(persisted_chunks)
            if event.reply_format == "plain_v1":
                send_reply = self.replies.reply_text
                send_followup = self.replies.send_text
            else:
                send_reply = self.replies.reply_markdown
                send_followup = self.replies.send_markdown

            for index, chunk in enumerate(chunks[event.chunks_sent :], start=event.chunks_sent):
                send_uuid = outbound_message_uuid(correlation_id, index)
                if index == 0:
                    send_reply(event.message_id, chunk, send_uuid)
                else:
                    send_followup(event.chat_id, chunk, send_uuid)
                self.store.mark_chunk_sent(correlation_id, index + 1)

            self.store.finish_reply(correlation_id)
            if event.message_type == "image" and event.attachment_token is not None:
                self._cleanup_attachment(correlation_id, event.attachment_token)
            self._notify()
            LOGGER.info(
                "Feishu reply completed correlation_id=%s chunks=%d",
                correlation_id,
                len(chunks),
            )
            return f"sent {len(chunks)} part(s)"

    def _cleanup_attachment(self, correlation_id: str, token: str) -> None:
        if self.attachments is None:
            return
        try:
            self.attachments.delete(token)
        except FileNotFoundError:
            pass
        except Exception:
            LOGGER.exception("attachment cleanup failed correlation_id=%s", correlation_id)
            return
        self.store.mark_attachment_cleaned(correlation_id)

    def _recover_attachments(self) -> None:
        if self.attachments is None:
            return
        self.attachments.cleanup_temporary_files()
        self.store.fail_interrupted_acquisitions()
        for event in self.store.list_attachment_cleanup_pending():
            if event.attachment_token is not None:
                self._cleanup_attachment(event.correlation_id, event.attachment_token)
        for event in self.store.list_accepted_images():
            try:
                if (
                    event.attachment_token is None
                    or event.attachment_size is None
                    or event.attachment_sha256 is None
                ):
                    raise ValueError("image metadata is incomplete")
                self.attachments.read(
                    event.attachment_token,
                    expected_size=event.attachment_size,
                    expected_sha256=event.attachment_sha256,
                )
            except Exception:
                LOGGER.warning(
                    "accepted image attachment is unavailable correlation_id=%s",
                    event.correlation_id,
                )
                self.store.mark_failed(event.correlation_id, "attachment_unavailable")
                if event.attachment_token is not None:
                    self._cleanup_attachment(event.correlation_id, event.attachment_token)

    def _notify(self) -> None:
        with contextlib.suppress(queue.Full):
            self._wakeups.put_nowait(object())

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            with contextlib.suppress(queue.Empty):
                self._wakeups.get(timeout=1.0)
            try:
                if self.dispatch_enabled:
                    self._finish_card_replies()
            except Exception:
                LOGGER.exception("card delivery checkpoint failed; will retry")
            while not self._stop.is_set():
                if not self.dispatch_enabled:
                    break
                event = self.store.claim_next_accepted()
                if event is None:
                    break
                try:
                    self.emit_inbound(event)
                except Exception:
                    LOGGER.exception(
                        "session inbox dispatch is ambiguous correlation_id=%s; not retrying",
                        event.correlation_id,
                    )
                break

    def _finish_card_replies(self) -> None:
        if self.progress_state_path is None:
            return
        with self._reply_lock, reply_lease(self.store.path):
            for event in self.store.pending_card_replies():
                if not event.reply_text or not card_answer_delivered(
                    self.progress_state_path, event.correlation_id, event.reply_text,
                ):
                    continue
                if event.chunks_sent == 0:
                    self.store.mark_chunk_sent(event.correlation_id, 1)
                self.store.finish_reply(event.correlation_id)
                if event.message_type == "image" and event.attachment_token:
                    self._cleanup_attachment(event.correlation_id, event.attachment_token)
                self._notify()
                LOGGER.info("Final answer delivered in progress card")
