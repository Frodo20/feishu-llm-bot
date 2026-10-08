import threading
import time
from pathlib import Path

import pytest

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.service import SessionBridgeService
from feishu_llm_bot.store import EventRecord, Store


class FakeReplies:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.send_uuids: list[str] = []
        self.fail_after: int | None = None

    def reply_text(self, message_id: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(message_id, text)

    def send_text(self, chat_id: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(chat_id, text)

    def reply_markdown(self, message_id: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(message_id, text)

    def send_markdown(self, chat_id: str, text: str, send_uuid: str) -> None:
        self.send_uuids.append(send_uuid)
        self._send(chat_id, text)

    def download_image(self, message_id: str, image_key: str) -> bytes:
        raise NotImplementedError

    def _send(self, target: str, text: str) -> None:
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise RuntimeError("send failed")
        self.sent.append((target, text))


class InboundSink:
    def __init__(self) -> None:
        self.events: list[EventRecord] = []
        self.ready = threading.Event()

    def __call__(self, event: EventRecord) -> None:
        self.events.append(event)
        self.ready.set()


def make_service(tmp_path: Path, chunk: int = 3500):
    store = Store(tmp_path / "bot.db")
    replies = FakeReplies()
    inbound = InboundSink()
    service = SessionBridgeService(
        store=store,
        replies=replies,
        emit_inbound=inbound,
        reply_chunk_chars=chunk,
        max_inbound_chars=100_000,
        queue_size=10,
    )
    return store, replies, inbound, service


def wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met")


def deliver(inbound: InboundSink, service: SessionBridgeService, index: int = -1) -> EventRecord:
    event = inbound.events[index]
    assert service.mark_delivered(event.correlation_id)
    return event


def test_accepted_event_before_start_is_forwarded(tmp_path: Path) -> None:
    _store, _replies, inbound, service = make_service(tmp_path)
    assert service.accept(IncomingMessage.text("m1", "c1", "before startup"))
    service.start()
    wait_until(lambda: len(inbound.events) == 1)
    assert inbound.events[0].user_text == "before startup"
    service.stop()


def test_service_forwards_messages_in_order(tmp_path: Path) -> None:
    _store, _replies, inbound, service = make_service(tmp_path)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "one"))
    service.accept(IncomingMessage("m2", "c1", "two"))
    wait_until(lambda: len(inbound.events) == 1)
    assert inbound.events[0].user_text == "one"
    first = deliver(inbound, service)
    time.sleep(0.05)
    assert len(inbound.events) == 1
    assert service.reply(first.correlation_id, "first answer") == "sent 1 part(s)"
    wait_until(lambda: len(inbound.events) == 2)
    assert inbound.events[1].user_text == "two"
    service.stop()


def test_failed_dispatch_is_not_replayed(tmp_path: Path) -> None:
    store = Store(tmp_path / "bot.db")
    attempted = threading.Event()

    def fail(_event: EventRecord) -> None:
        attempted.set()
        raise RuntimeError("stdout write failed")

    service = SessionBridgeService(
        store=store,
        replies=FakeReplies(),
        emit_inbound=fail,
        reply_chunk_chars=3500,
        max_inbound_chars=100_000,
        queue_size=10,
    )
    service.start()
    service.accept(IncomingMessage("m1", "c1", "do this once"))
    assert attempted.wait(3)
    wait_until(lambda: store.claim_next_accepted() is None)
    row = store._connection.execute(  # noqa: SLF001 - state assertion
        "SELECT status FROM events WHERE message_id = 'm1'"
    ).fetchone()
    assert row["status"] == "dispatching"
    service.stop()


def test_reset_is_forwarded_as_plain_text(tmp_path: Path) -> None:
    _store, _replies, inbound, service = make_service(tmp_path)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "/reset"))
    wait_until(lambda: len(inbound.events) == 1)
    assert inbound.events[0].user_text == "/reset"
    service.stop()


def test_oversized_event_is_rejected_before_persisting(tmp_path: Path) -> None:
    store, _replies, _inbound, service = make_service(tmp_path)
    service.max_inbound_chars = 3
    with pytest.raises(ValueError, match="exceeds 3"):
        service.accept(IncomingMessage("m1", "c1", "four"))
    assert store.claim_next_accepted() is None
    store.close()


def test_duplicate_event_is_ignored(tmp_path: Path) -> None:
    _store, _replies, inbound, service = make_service(tmp_path)
    service.start()
    message = IncomingMessage("m1", "c1", "hello")
    assert service.accept(message)
    assert not service.accept(message)
    wait_until(lambda: len(inbound.events) == 1)
    service.stop()


def test_reply_chunks_and_is_idempotent(tmp_path: Path) -> None:
    _store, replies, inbound, service = make_service(tmp_path, chunk=4)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "hello"))
    wait_until(lambda: len(inbound.events) == 1)
    event = deliver(inbound, service)

    assert service.reply(event.correlation_id, "abcdefghij") == "sent 3 part(s)"
    assert replies.sent == [("m1", "abcd"), ("c1", "efgh"), ("c1", "ij")]
    assert len(set(replies.send_uuids)) == 3
    first_uuids = replies.send_uuids.copy()
    assert service.reply(event.correlation_id, "abcdefghij") == "already sent"
    assert replies.send_uuids == first_uuids
    assert len(replies.sent) == 3
    service.stop()


def test_concurrent_reply_calls_do_not_repeat_chunks(tmp_path: Path) -> None:
    _store, replies, inbound, service = make_service(tmp_path, chunk=4)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "hello"))
    wait_until(lambda: len(inbound.events) == 1)
    event = deliver(inbound, service)
    barrier = threading.Barrier(3)
    results: list[str] = []

    def send() -> None:
        barrier.wait()
        results.append(service.reply(event.correlation_id, "abcdefgh"))

    threads = [threading.Thread(target=send) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert sorted(results) == ["already sent", "sent 2 part(s)"]
    assert replies.sent == [("m1", "abcd"), ("c1", "efgh")]
    service.stop()


def test_dispatching_event_can_be_replied_after_ambiguous_inbox_write(tmp_path: Path) -> None:
    store, replies, inbound, service = make_service(tmp_path)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "hello"))
    wait_until(lambda: len(inbound.events) == 1)
    event = inbound.events[0]
    assert store.get_by_correlation(event.correlation_id).status == "dispatching"

    assert service.reply(event.correlation_id, "answer") == "sent 1 part(s)"
    assert replies.sent == [("m1", "answer")]
    assert store.get_by_correlation(event.correlation_id).status == "replied"
    service.stop()


def test_migrated_partial_plain_reply_resumes_with_original_chunking(tmp_path: Path) -> None:
    store, replies, _inbound, service = make_service(tmp_path, chunk=4)
    assert store.accept_event("m1", "c1", "hello")
    event = store.claim_next_accepted()
    assert event is not None
    assert store.mark_delivered(event.correlation_id)
    store.begin_reply(
        event.correlation_id,
        "abcdefgh",
        "plain_v1",
        ["abcd", "efgh"],
    )
    store.mark_chunk_sent(event.correlation_id, 1)

    assert service.reply(event.correlation_id, "abcdefgh") == "sent 2 part(s)"
    assert replies.sent == [("c1", "efgh")]
    service.stop()


def test_partial_reply_uses_persisted_chunks_after_limit_changes(tmp_path: Path) -> None:
    path = tmp_path / "bot.db"
    store, replies, inbound, service = make_service(tmp_path, chunk=4)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "hello"))
    wait_until(lambda: len(inbound.events) == 1)
    event = deliver(inbound, service)
    replies.fail_after = 1

    with pytest.raises(RuntimeError, match="send failed"):
        service.reply(event.correlation_id, "abcdefghij")
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None
    assert saved.reply_chunks == ("abcd", "efgh", "ij")
    service.stop()

    reopened = Store(path)
    retry_replies = FakeReplies()
    retry = SessionBridgeService(
        store=reopened,
        replies=retry_replies,
        emit_inbound=InboundSink(),
        reply_chunk_chars=2,
        max_inbound_chars=100_000,
        queue_size=10,
    )
    assert retry.reply(event.correlation_id, "abcdefghij") == "sent 3 part(s)"
    assert retry_replies.sent == [("c1", "efgh"), ("c1", "ij")]
    retry.stop()


def test_legacy_partial_reply_bootstraps_and_persists_current_chunk_plan(
    tmp_path: Path,
) -> None:
    store, replies, _inbound, service = make_service(tmp_path, chunk=4)
    assert store.accept_event("m1", "c1", "hello")
    event = store.claim_next_accepted()
    assert event is not None
    assert store.mark_delivered(event.correlation_id)
    store._connection.execute(  # noqa: SLF001 - model a migrated pre-v3 row
        """
        UPDATE events
        SET status = 'replying', reply_text = 'abcdefgh', reply_format = 'plain_v1',
            reply_chunks = NULL, chunks_sent = 1
        WHERE correlation_id = ?
        """,
        (event.correlation_id,),
    )

    assert service.reply(event.correlation_id, "abcdefgh") == "sent 2 part(s)"
    assert replies.sent == [("c1", "efgh")]
    saved = store.get_by_correlation(event.correlation_id)
    assert saved is not None and saved.reply_chunks == ("abcd", "efgh")
    service.stop()


def test_partial_reply_resumes_without_repeating_chunks(tmp_path: Path) -> None:
    _store, replies, inbound, service = make_service(tmp_path, chunk=4)
    service.start()
    service.accept(IncomingMessage("m1", "c1", "hello"))
    wait_until(lambda: len(inbound.events) == 1)
    event = deliver(inbound, service)
    replies.fail_after = 1

    with pytest.raises(RuntimeError, match="send failed"):
        service.reply(event.correlation_id, "abcdefgh")
    assert replies.sent == [("m1", "abcd")]

    first_uuid = replies.send_uuids[0]
    replies.fail_after = None
    assert service.reply(event.correlation_id, "abcdefgh") == "sent 2 part(s)"
    assert replies.sent == [("m1", "abcd"), ("c1", "efgh")]
    assert replies.send_uuids[0] == first_uuid
    assert len(set(replies.send_uuids)) == 2
    service.stop()
