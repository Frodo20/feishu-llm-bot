from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from feishu_llm_bot.card_delivery import CardPager, answer_pages, card_answer_delivered
from feishu_llm_bot.progress import ProgressMonitor, progress_card
from feishu_llm_bot.progress_events import tool_label
from feishu_llm_bot.service import SessionBridgeService
from feishu_llm_bot.store import Store

SESSION = "8dd1586b-77ab-4221-ad2d-4874292f110c"


class Client:
    def __init__(self) -> None:
        self.reactions = []
        self.cards = []
        self.updates = []
        self.fail_reaction = False
        self.fail_card = False
        self.fail_update = False
        self.receipt_emojis = []
        self.status_reactions = []
        self.removed_reactions = []
        self.fail_status_reaction = False
        self.fail_remove_reaction = False

    def add_received_reaction(self, message_id, emoji_type="OK"):
        self.reactions.append(message_id)
        self.receipt_emojis.append(emoji_type)
        if self.fail_reaction:
            raise RuntimeError("reaction scope missing")

    def add_reaction(self, message_id, emoji_type):
        self.status_reactions.append((message_id, emoji_type))
        if self.fail_status_reaction:
            raise RuntimeError("reaction unavailable")
        return "reaction-" + str(len(self.status_reactions))

    def remove_reaction(self, message_id, reaction_id):
        self.removed_reactions.append((message_id, reaction_id))
        if self.fail_remove_reaction:
            raise RuntimeError("reaction removal unavailable")

    def send_card(self, chat_id, card, send_uuid):
        self.cards.append((chat_id, card, send_uuid))
        if self.fail_card:
            raise RuntimeError("network unavailable")
        return "card-" + str(len(self.cards))

    def update_card(self, message_id, card):
        self.updates.append((message_id, card))
        if self.fail_update:
            raise RuntimeError("network unavailable")


class Harness:
    def __init__(self, root: Path) -> None:
        self.store = Store(root / "bot.db")
        self.client = Client()
        self.now = time.time()
        self.transcript = root / "session.jsonl"
        self.transcript.write_text("")
        self.health = root / "health.json"
        self.resident = root / "resident.json"
        self.resident.write_text(json.dumps({"instance": "current"}))
        self.heartbeat()
        self.kwargs = dict(
            database=self.store.path, state_path=root / "progress" / "state.db",
            transcript=self.transcript, session_id=SESSION, health_path=self.health,
            resident_path=self.resident, client=self.client,
            session_name="test-assistant", model="gpt-5.6-sol",
        )
        self.monitor = ProgressMonitor(**self.kwargs)

    def heartbeat(self, connected=True):
        self.health.write_text(json.dumps({
            "instance": "current", "ready": True, "timestamp": self.now,
            "python_health_at": self.now, "websocket_connected": connected, "mcp_pid": 123,
        }))

    def tick(self, seconds=0):
        self.now += seconds
        self.monitor.tick(self.now)

    def task(self, name="m1"):
        self.store.accept_event(name, "chat", "private user request")
        event = self.store.claim_next_accepted()
        assert event is not None
        self.store.mark_delivered(event.correlation_id)
        self.tick()
        return event.correlation_id

    def append(self, kind, content=None, **extra):
        entry = {"type": kind, "sessionId": SESSION, **extra}
        if content is not None:
            entry["message"] = {"content": content}
        with self.transcript.open("a") as stream:
            stream.write(json.dumps(entry) + "\n")

    def start(self, cid, pid=123):
        self.append("user", [{"type": "text", "text": (
            "Another Claude session sent a message:\n[Feishu bridge message]\n"
            'FEISHU_ENVELOPE_JSON=' + json.dumps({"correlation_id": cid,
                                                   "untrusted_user_text": "hello"})
        )}], origin={"kind": "peer", "verifiedPeerPid": pid})

    def tool(self, tool_id="t1", name="Skill", inputs=None):
        self.append("assistant", [{"type": "tool_use", "id": tool_id, "name": name,
                                   "input": inputs or {"skill": "forge-cli"}}])

    def result(self, tool_id="t1", error=False, content="Successfully loaded skill"):
        self.append("user", [{"type": "tool_result", "tool_use_id": tool_id,
                              "is_error": error, "content": content}])

    def finish(self, cid):
        self.store.begin_reply(cid, "answer", reply_chunks=["answer"])
        self.store.mark_chunk_sent(cid, 1)
        self.store.finish_reply(cid)

    def reopen(self):
        self.monitor.close()
        self.monitor = ProgressMonitor(**self.kwargs)

    def close(self):
        self.monitor.close()
        self.store.close()


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    harness.close()


def test_receipt_card_steps_and_final_answer_are_independent(h):
    cid = h.task()
    assert h.client.reactions == ["m1"]
    assert len(h.client.cards) == 1
    assert "等待开始" in str(h.client.cards[0][1])
    h.start(cid)
    h.tool()
    h.tick(3)
    assert "Skill(forge-cli)" in str(h.client.updates[-1])
    assert "⏳" in str(h.client.updates[-1])
    h.result()
    h.tick(3)
    assert "✓ Skill(forge-cli)" in str(h.client.updates[-1])
    h.finish(cid)
    h.tick(3)
    assert "已完成" in str(h.client.updates[-1])
    assert not h.monitor.tasks
    assert len(h.client.cards) == 1
    assert h.store.get_by_correlation(cid).reply_text == "answer"


def test_duplicates_and_monitor_restart_reuse_original_card(h):
    cid = h.task()
    h.start(cid)
    h.tool()
    h.tick(3)
    h.reopen()
    assert not h.store.accept_event("m1", "chat", "duplicate")
    h.result()
    h.tick(3)
    assert "✓ Skill(forge-cli)" in str(h.client.updates[-1])
    assert h.client.reactions == ["m1"]
    assert len(h.client.cards) == 1
    assert all(card_id == "card-1" for card_id, _ in h.client.updates)


def test_fast_reply_before_first_poll_is_still_acknowledged(h):
    h.store.accept_event("fast", "chat", "hi")
    event = h.store.claim_next_accepted()
    h.finish(event.correlation_id)
    h.tick()
    assert h.client.reactions == ["fast"]
    assert "已完成" in str(h.client.cards[-1])


def test_waiting_messages_get_receipts_without_stealing_current_tools(h):
    cid = h.task()
    h.start(cid)
    h.store.accept_event("m2", "chat", "next task")
    h.tick(3)
    h.tool()
    h.tick(3)
    assert h.client.reactions == ["m1", "m2"]
    assert "排队中" in str(h.client.cards[-1])
    second = next(s for s in h.monitor.tasks.values() if s["message_id"] == "m2")
    assert not second["steps"]


def test_unrelated_sessions_prompts_and_sidechains_cannot_report_steps(h):
    cid = h.task()
    h.start(cid, pid=999)
    h.tool()
    h.tick(3)
    assert not h.monitor.tasks[cid]["steps"]
    h.start(cid)
    h.append("assistant", [{"type": "tool_use", "id": "other", "name": "Bad"}],
             sessionId="another-session")
    h.append("assistant", [{"type": "tool_use", "id": "side", "name": "Bad"}],
             isSidechain=True)
    h.append("user", "local terminal prompt")
    h.tool()
    h.tick(3)
    assert not h.monitor.tasks[cid]["steps"]


def test_incomplete_json_line_is_retried_and_not_lost(h):
    cid = h.task()
    h.start(cid)
    h.tick(3)
    entry = json.dumps({"type": "assistant", "sessionId": SESSION, "message": {
        "content": [{"type": "tool_use", "id": "partial", "name": "Read", "input": {}}]
    }})
    with h.transcript.open("a") as stream:
        stream.write(entry[:30])
    h.tick(3)
    assert not h.monitor.tasks[cid]["steps"]
    with h.transcript.open("a") as stream:
        stream.write(entry[30:] + "\n")
    h.tick(3)
    assert len(h.monitor.tasks[cid]["steps"]) == 1


def test_errors_are_visible_without_raw_commands_outputs_or_reasoning(h):
    cid = h.task()
    h.start(cid)
    h.append("assistant", [{"type": "thinking", "thinking": "private-thoughts"}])
    h.tool(name="Bash", inputs={"command": "TOKEN=secret forge job list --token secret"})
    h.result(error=True, content="Hook returned incorrect event name; secret-output")
    h.append("attachment", attachment={"type": "hook_non_blocking_error",
                                       "stderr": "Hook returned incorrect event name: secret"})
    h.tick(3)
    card = str(h.client.updates[-1])
    assert "Bash · forge job list" in card
    assert "Hook 事件类型不匹配" in card
    assert "失败" in card
    assert "secret" not in card and "private-thoughts" not in card


def test_receipt_api_failure_does_not_block_card_or_task(h):
    h.client.fail_reaction = True
    cid = h.task()
    assert len(h.client.cards) == 1
    assert h.store.get_by_correlation(cid).status == "delivered"
    h.tick(30)
    h.tick(30)
    h.tick(30)
    assert len(h.client.reactions) == 3
    assert len(h.client.cards) == 1


def test_card_create_retry_uses_same_uuid_and_final_patch_retries(h):
    h.client.fail_card = True
    cid = h.task()
    h.tick(3)
    assert len(h.client.cards) == 1
    h.client.fail_card = False
    h.tick(8)
    assert len(h.client.cards) == 2
    assert h.client.cards[0][2] == h.client.cards[1][2]
    h.client.fail_update = True
    h.finish(cid)
    h.tick(3)
    assert cid in h.monitor.tasks
    h.reopen()
    h.client.fail_update = False
    h.tick(11)
    assert cid not in h.monitor.tasks
    assert "已完成" in str(h.client.updates[-1])


def test_heartbeat_reports_disconnection_and_restart_interruption(h):
    cid = h.task()
    h.start(cid)
    h.tick(3)
    count = len(h.client.updates)
    h.tick(1)
    assert len(h.client.updates) == count  # Coalesce fast events; no message flood.
    h.tick(15)
    assert "等待模型响应" in str(h.client.updates[-1])
    h.tick(60)
    assert "连接异常" in str(h.client.updates[-1])
    assert "没有新执行记录" in str(h.client.updates[-1])
    h.heartbeat()
    h.tick(3)
    assert "桥接在线" in str(h.client.updates[-1])
    h.store.fail_interrupted_deliveries()
    h.tick(3)
    assert "任务中断" in str(h.client.updates[-1])
    assert "会话重启" in str(h.client.updates[-1])


def test_first_install_does_not_send_historical_cards(tmp_path):
    store = Store(tmp_path / "bot.db")
    store.accept_event("old", "chat", "old task")
    event = store.claim_next_accepted()
    store.mark_failed(event.correlation_id)
    store.close()
    h = Harness(tmp_path)
    try:
        h.tick()
        assert not h.client.reactions and not h.client.cards
    finally:
        h.close()


def test_completed_card_does_not_claim_unfinished_tool_is_still_running(h):
    cid = h.task()
    h.start(cid)
    h.tool()
    h.tick(3)
    h.store.mark_failed(cid, "resident_restart_interrupted")
    h.tick(3)
    assert "已中断" in str(h.client.updates[-1])


def test_pending_permission_is_reported_only_for_own_session(h):
    h.task()
    for name, session in [("other", "other-session"), ("current", SESSION)]:
        h.store.create_permission_request(
            request_id=name, session_id=session, chat_id="chat", tool_name="Write",
            summary="file", token_hash=("a" if name == "other" else "b") * 64,
            created_at=int(h.now), expires_at=int(h.now) + 600, max_pending=8,
        )
        h.tick(3)
        if name == "other":
            assert "等待授权" not in str(h.client.cards[-1])
        else:
            assert "等待授权" in str(h.client.updates[-1])


def test_summaries_are_bounded_and_markdown_special_characters_are_escaped(h):
    cid = h.task()
    h.start(cid)
    for i in range(20):
        h.tool(tool_id=f"t{i}")
        h.result(tool_id=f"t{i}")
    h.tick(3)
    state = h.monitor.tasks[cid]
    assert len(state["steps"]) == 8
    card = progress_card(state, now=h.now, connection="桥接在线")
    assert card["body"]["elements"][0]["tag"] == "markdown"
    assert card["schema"] == "2.0"
    assert tool_label("Skill", {"skill": "<at id=all>"}) == "Skill"
    assert tool_label("Bash", {"command": "curl https://secret/token"}) == "Bash · curl"


def card_service(h):
    return SessionBridgeService(
        store=h.store, replies=h.client, emit_inbound=lambda event: None,
        reply_chunk_chars=3500, max_inbound_chars=100000, queue_size=10,
        progress_state_path=h.kwargs["state_path"],
    )


def test_final_markdown_is_appended_to_same_unquoted_card_and_then_acknowledged(h):
    cid = h.task()
    service = card_service(h)
    answer = "# 结果\n\n**成功**，查看 `code`。\n\n```python\nprint(1)\n```\n"
    assert "queued" in service.reply(cid, answer)
    assert "queued" in service.reply(cid, answer)
    with pytest.raises(ValueError, match="different reply"):
        service.reply(cid, "different")
    service._finish_card_replies()  # noqa: SLF001
    assert h.store.get_by_correlation(cid).status == "replying"
    assert not card_answer_delivered(h.kwargs["state_path"], cid, answer)
    h.tick(3)
    assert len(h.client.cards) == 1
    assert h.client.cards[0][0] == "chat"  # create by chat ID, not reply by message ID
    card_id, card = h.client.updates[-1]
    assert card_id == "card-1"
    assert card["header"]["title"]["content"] == "test-assistant · gpt-5.6-sol"
    elements = card["body"]["elements"]
    assert elements[1]["tag"] == "hr"
    assert elements[2] == {"tag": "markdown", "content": answer.strip()}
    assert card_answer_delivered(h.kwargs["state_path"], cid, answer)
    assert not card_answer_delivered(h.kwargs["state_path"], cid, answer + "tampered")
    service._finish_card_replies()  # noqa: SLF001
    assert h.store.get_by_correlation(cid).status == "replied"
    assert service.reply(cid, answer) == "already sent"
    h.tick(3)
    assert not h.monitor.tasks


def test_failed_card_patch_cannot_complete_reply_and_saved_answer_survives_restart(h):
    cid = h.task()
    service = card_service(h)
    answer = "**最终答案**"
    service.reply(cid, answer)
    h.client.fail_update = True
    h.tick(3)
    service._finish_card_replies()  # noqa: SLF001
    assert h.store.get_by_correlation(cid).status == "replying"
    assert h.store.fail_interrupted_deliveries() == 0
    h.reopen()
    h.client.fail_update = False
    h.tick(11)
    service._finish_card_replies()  # noqa: SLF001
    assert h.store.get_by_correlation(cid).status == "replied"
    assert len(h.client.cards) == 1


def test_card_mode_preserves_legacy_partial_reply_format(tmp_path):
    # Enabling cards must not change a partially sent legacy reply's chunk plan.
    from test_service import make_service

    store, replies, _inbound, service = make_service(tmp_path, chunk=4)
    store.accept_event("message", "chat", "hello")
    event = store.claim_next_accepted()
    store.begin_reply(event.correlation_id, "abcdefgh", "post_v2", ["abcd", "efgh"])
    store.mark_chunk_sent(event.correlation_id, 1)
    service.progress_state_path = tmp_path / "progress.db"
    assert service.reply(event.correlation_id, "abcdefgh") == "sent 2 part(s)"
    assert replies.sent == [("chat", "efgh")]
    service.stop()


def page_callback(state, page=1, operator="owner", token=None):
    return SimpleNamespace(
        header=SimpleNamespace(app_id="app", event_type="card.action.trigger"),
        event=SimpleNamespace(
            operator=SimpleNamespace(open_id=operator), host="im_message",
            context=SimpleNamespace(
                open_message_id=state["card_id"], open_chat_id=state["chat_id"],
            ),
            action=SimpleNamespace(tag="button", value={
                "kind": "progress_page", "page": page,
                "token": state["page_token"] if token is None else token,
            }),
        ),
    )


def test_long_answer_navigation_is_owner_bound_and_updates_finished_card(h):
    cid = h.task()
    service = card_service(h)
    answer = ("**测试段落**\n\n" * 1200) + "最后一段"
    service.reply(cid, answer)
    h.tick(3)
    service._finish_card_replies()  # noqa: SLF001
    h.tick(3)
    assert not h.monitor.tasks
    state = json.loads(h.monitor.db.execute(
        "SELECT state FROM tasks WHERE correlation_id=?", (cid,),
    ).fetchone()[0])
    pager = CardPager(h.kwargs["state_path"], app_id="app", allowed_sender="owner")
    for callback in [page_callback(state, operator="intruder"),
                     page_callback(state, page=100000), page_callback(state, token="wrong")]:
        pager.handle(callback)
    assert h.monitor.db.execute("SELECT count(*) FROM page_requests").fetchone()[0] == 0
    pager.handle(page_callback(state))
    h.tick(3)
    card_id, card = h.client.updates[-1]
    assert card_id == state["card_id"]
    assert card["body"]["elements"][2]["content"] == answer_pages(answer)[1]
    assert "第 2 /" in str(card)
    assert len(h.client.cards) == 1
    assert h.monitor.db.execute("SELECT count(*) FROM page_requests").fetchone()[0] == 0


def test_card_pages_preserve_code_fences_and_bound_utf8_payload(h):
    cid = h.task()
    state = h.monitor.tasks[cid]
    answer = "# 标题\n\n```python\n" + "print('你好😀')\n" * 1000 + "```"
    state["answer"] = answer
    pages = answer_pages(answer)
    assert len(pages) > 1
    assert sum(page.count("print('你好😀')") for page in pages) == 1000
    for index, page in enumerate(pages):
        state["page"] = index
        card = progress_card(state, now=h.now, connection="桥接在线")
        assert page.count("```") % 2 == 0
        assert len(json.dumps(card, ensure_ascii=False).encode()) < 30000


def test_background_worker_releases_next_task_only_after_card_delivery(h):
    from test_service import wait_until

    cid = h.task()
    service = card_service(h)
    service.start()
    try:
        service.reply(cid, "**ready**")
        h.store.accept_event("next", "chat", "next request")
        assert h.store.claim_next_accepted() is None
        h.tick(3)
        wait_until(lambda: h.store.get_by_correlation(cid).status == "replied")
        wait_until(lambda: h.store.status_counts().get("dispatching") == 1)
        assert len(h.client.cards) == 2  # One card for each distinct user request.
    finally:
        service.stop()
