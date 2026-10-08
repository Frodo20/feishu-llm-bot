from __future__ import annotations

import base64
import io
import threading
import time
from pathlib import Path

import pytest
from PIL import Image

from feishu_llm_bot.attachments import AttachmentStore
from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.service import SessionBridgeService
from feishu_llm_bot.store import EventRecord, Store


def png_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (2, 2), (10, 20, 30)).save(output, format="PNG")
    return output.getvalue()


class FakeFeishu:
    def __init__(self, image: bytes) -> None:
        self.image = image
        self.downloads: list[tuple[str, str]] = []
        self.sent: list[tuple[str, str]] = []
        self.send_uuids: list[str] = []
        self.fail_after: int | None = None
        self.download_started: threading.Event | None = None
        self.release_download: threading.Event | None = None

    def download_image(self, message_id: str, image_key: str) -> bytes:
        self.downloads.append((message_id, image_key))
        if self.download_started is not None:
            self.download_started.set()
        if self.release_download is not None:
            self.release_download.wait()
        return self.image

    def reply_markdown(self, target: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(target, text)

    def send_markdown(self, target: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(target, text)

    def reply_text(self, target: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(target, text)

    def send_text(self, target: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(target, text)

    def _send(self, target: str, text: str) -> None:
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise RuntimeError("send failed")
        self.sent.append((target, text))


class Inbound:
    def __init__(self) -> None:
        self.events: list[EventRecord] = []
        self.ready = threading.Event()

    def __call__(self, event: EventRecord) -> None:
        self.events.append(event)
        self.ready.set()


def make_service(tmp_path: Path, image: bytes | None = None):
    store = Store(tmp_path / "bot.db")
    attachments = AttachmentStore(
        tmp_path / "attachments",
        max_image_bytes=1024 * 1024,
        max_image_pixels=1000,
        max_image_side=100,
        max_total_bytes=2 * 1024 * 1024,
    )
    feishu = FakeFeishu(image if image is not None else png_bytes())
    inbound = Inbound()
    service = SessionBridgeService(
        store=store,
        replies=feishu,
        emit_inbound=inbound,
        reply_chunk_chars=3500,
        max_inbound_chars=100_000,
        queue_size=10,
        attachments=attachments,
    )
    return store, attachments, feishu, inbound, service


def wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met")


def test_image_is_staged_dispatched_read_and_cleaned_after_reply(tmp_path: Path) -> None:
    store, attachments, feishu, inbound, service = make_service(tmp_path)
    service.start()
    message = IncomingMessage.image("m1", "c1", "img_key")

    assert service.accept(message)
    assert not service.accept(message)
    wait_for(lambda: feishu.downloads == [("m1", "img_key")])
    wait_for(lambda: len(inbound.events) == 1)
    event = inbound.events[0]
    assert event.message_type == "image"
    assert event.user_text is None
    assert event.attachment_token is not None
    assert service.mark_delivered(event.correlation_id)

    image = service.read_image(event.correlation_id)
    assert base64.b64decode(image.data) == png_bytes()
    assert image.mime_type == "image/png"
    assert image.byte_length == len(png_bytes())

    assert service.reply(event.correlation_id, "**看到了**") == "sent 1 part(s)"
    assert feishu.sent == [("m1", "**看到了**")]
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "replied"
    assert saved.attachment_cleaned_at is not None
    with pytest.raises(FileNotFoundError):
        attachments.read(
            event.attachment_token,
            expected_size=event.attachment_size or 0,
            expected_sha256=event.attachment_sha256 or "",
        )
    with pytest.raises(ValueError, match="unavailable"):
        service.read_image(event.correlation_id)
    service.stop()


def test_invalid_image_is_failed_once_and_not_dispatched(tmp_path: Path) -> None:
    store, _attachments, feishu, inbound, service = make_service(tmp_path, b"not-image")
    service.start()
    message = IncomingMessage.image("m1", "c1", "img_key")

    assert service.accept(message)
    assert not service.accept(message)
    wait_for(lambda: store.status_counts() == {"failed": 1})
    assert feishu.downloads == [("m1", "img_key")]
    assert len(feishu.sent) == 1
    assert inbound.events == []
    service.stop()


def test_partial_reply_keeps_image_available(tmp_path: Path) -> None:
    store, attachments, feishu, inbound, service = make_service(tmp_path)
    service.reply_chunk_chars = 4
    service.start()
    assert service.accept(IncomingMessage.image("m1", "c1", "img_key"))
    wait_for(lambda: len(inbound.events) == 1)
    event = inbound.events[0]
    assert service.mark_delivered(event.correlation_id)
    feishu.fail_after = 1

    with pytest.raises(RuntimeError, match="send failed"):
        service.reply(event.correlation_id, "abcdefgh")
    assert service.read_image(event.correlation_id).byte_length == len(png_bytes())
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "replying"
    assert saved.attachment_token is not None
    assert (attachments.root / saved.attachment_token).exists()

    feishu.fail_after = None
    assert service.reply(event.correlation_id, "abcdefgh") == "sent 2 part(s)"
    assert not (attachments.root / saved.attachment_token).exists()
    service.stop()


def test_image_callback_returns_before_blocked_download_finishes(tmp_path: Path) -> None:
    store, _attachments, feishu, inbound, service = make_service(tmp_path)
    feishu.download_started = threading.Event()
    feishu.release_download = threading.Event()
    service.start()

    started = time.monotonic()
    assert service.accept(IncomingMessage.image("m1", "c1", "img_key"))
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert feishu.download_started.wait(1)
    row = store._connection.execute(  # noqa: SLF001 - lifecycle assertion
        "SELECT status FROM events WHERE message_id = 'm1'"
    ).fetchone()
    assert row["status"] == "acquiring"
    assert inbound.events == []

    feishu.release_download.set()
    wait_for(lambda: len(inbound.events) == 1)
    service.stop()


def test_stop_times_out_without_closing_store_during_image_download(tmp_path: Path) -> None:
    store, attachments, feishu, _inbound, service = make_service(tmp_path)
    feishu.download_started = threading.Event()
    feishu.release_download = threading.Event()
    service.start()
    assert service.accept(IncomingMessage.image("m1", "c1", "img_key"))
    assert feishu.download_started.wait(1)

    with pytest.raises(TimeoutError, match="image acquisition"):
        service.stop(timeout=0.05)
    assert store.status_counts() == {"acquiring": 1}

    feishu.release_download.set()
    wait_for(lambda: store.status_counts() == {"accepted": 1})
    service.stop()
    assert any(attachments.root.iterdir())


def test_startup_waits_for_live_acquisition_process_lease(tmp_path: Path) -> None:
    first_store, _attachments, first_feishu, _inbound, first_service = make_service(tmp_path)
    first_feishu.download_started = threading.Event()
    first_feishu.release_download = threading.Event()
    first_service.start()
    assert first_service.accept(IncomingMessage.image("m1", "c1", "img_key"))
    assert first_feishu.download_started.wait(1)

    second_store = Store(tmp_path / "bot.db")
    second_attachments = AttachmentStore(
        tmp_path / "attachments",
        max_image_bytes=1024 * 1024,
        max_image_pixels=1000,
        max_image_side=100,
        max_total_bytes=2 * 1024 * 1024,
    )
    second_service = SessionBridgeService(
        store=second_store,
        replies=FakeFeishu(png_bytes()),
        emit_inbound=Inbound(),
        reply_chunk_chars=3500,
        max_inbound_chars=100_000,
        queue_size=10,
        attachments=second_attachments,
    )
    startup_finished = threading.Event()

    def start_second() -> None:
        second_service.start()
        startup_finished.set()

    thread = threading.Thread(target=start_second)
    thread.start()
    assert not startup_finished.wait(0.1)
    assert first_store.status_counts() == {"acquiring": 1}

    first_feishu.release_download.set()
    assert startup_finished.wait(3)
    assert second_store.status_counts() in ({"accepted": 1}, {"dispatching": 1})
    first_service.stop()
    second_service.stop()
    thread.join()


def test_restart_fails_interrupted_acquisition_and_cleans_terminal_file(tmp_path: Path) -> None:
    store, attachments, _feishu, _inbound, service = make_service(tmp_path)
    event = store.reserve_image("m1", "c1")
    assert event is not None and event.attachment_token is not None
    metadata = attachments.save(event.attachment_token, png_bytes())
    assert metadata.byte_size > 0

    service.start()
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.status == "failed"
    assert saved.attachment_cleaned_at is not None
    assert not (attachments.root / event.attachment_token).exists()
    service.stop()
