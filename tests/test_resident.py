from pathlib import Path
from types import SimpleNamespace

import pytest

from feishu_llm_bot.health import websocket_connected
from feishu_llm_bot.resident import health_error, load_health, write_json
from feishu_llm_bot.store import Store


def test_health_does_not_accept_previous_instance_or_dead_python() -> None:
    health = {"instance": "new", "ready": True, "timestamp": 100, "python_health_at": 100}
    assert health_error(health, "new", 110) is None
    assert health_error(health, "old", 110) is not None
    assert health_error(health, "new", 160) is not None
    health["timestamp"] = 160
    assert health_error(health, "new", 160) == "stale python_health_at"
    health["python_health_at"] = 160
    health["ready"] = False
    assert health_error(health, "new", 160) is not None


def test_health_file_is_private_and_tolerates_partial_file(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    assert load_health(path) == {}
    path.write_text("{")
    assert load_health(path) == {}
    write_json(path, {"instance": "new"})
    assert load_health(path) == {"instance": "new"}
    assert path.stat().st_mode & 0o777 == 0o600


def test_sdk_health_reports_initial_connection_and_disconnect() -> None:
    client = SimpleNamespace(_conn=None)
    assert not websocket_connected(client)
    client._conn = SimpleNamespace(open=True)
    assert websocket_connected(client)
    client._conn.open = False
    assert not websocket_connected(client)
    client._conn = SimpleNamespace(state=SimpleNamespace(name="OPEN"))
    assert websocket_connected(client)
    client._conn.state.name = "CLOSED"
    assert not websocket_connected(client)


@pytest.mark.parametrize("state", ["dispatching", "delivered", "replying"])
def test_restart_releases_queue_without_replaying_ambiguous_work(
    tmp_path: Path, state: str,
) -> None:
    path = tmp_path / "bot.db"
    store = Store(path)
    store.accept_event("interrupted", "chat", "possibly destructive action")
    first = store.claim_next_accepted()
    assert first is not None
    if state != "dispatching":
        store.mark_delivered(first.correlation_id)
    if state == "replying":
        store.begin_reply(first.correlation_id, "a\nb", reply_chunks=["a", "b"])
        store.mark_chunk_sent(first.correlation_id, 1)
    store.accept_event("next", "chat", "next request")
    store.close()
    store = Store(path)
    assert store.claim_next_accepted() is None
    assert store.fail_interrupted_deliveries() == 1
    previous = store.get_by_correlation(first.correlation_id)
    assert previous is not None and previous.status == "failed"
    assert previous.failure_reason == "resident_restart_interrupted"
    if state == "replying":
        assert previous.chunks_sent == 1
    assert not store.accept_event("interrupted", "chat", "duplicate")
    second = store.claim_next_accepted()
    assert second is not None and second.message_id == "next"
    store.close()
