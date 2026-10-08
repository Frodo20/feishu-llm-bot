"""Durable acknowledgements and owner-only navigation for the single reply card."""
from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path

from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse

from .text import split_markdown_reply


def answer_digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def answer_pages(text: str) -> list[str]:
    # Reserve room for the progress area and JSON escaping within Feishu's size limit.
    return split_markdown_reply(text, 4000)


def card_answer_delivered(path: Path, correlation_id: str, text: str) -> bool:
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.2)) as db:
            row = db.execute(
                "SELECT state FROM tasks WHERE correlation_id=?", (correlation_id,),
            ).fetchone()
        if row is None:
            return False
        state = json.loads(row[0])
        return bool(state.get("card_id")) and (
            state.get("answer_delivered_hash") == answer_digest(text)
        )
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return False


class CardPager:
    def __init__(self, path: Path, *, app_id: str, allowed_sender: str) -> None:
        self.path = path
        self.app_id = app_id
        self.allowed_sender = allowed_sender

    def handle(self, data) -> P2CardActionTriggerResponse:
        invalid = {"toast": {"type": "error", "content": "无法切换此卡片的页码。"}}
        try:
            header, event = data.header, data.event
            value = event.action.value
            page = value.get("page")
            if (
                header.app_id != self.app_id or header.event_type != "card.action.trigger"
                or event.operator.open_id != self.allowed_sender or event.host != "im_message"
                or event.action.tag != "button" or not isinstance(value, dict)
                or set(value) != {"kind", "page", "token"} or value["kind"] != "progress_page"
                or not isinstance(page, int) or isinstance(page, bool) or page < 0
                or not isinstance(value["token"], str)
            ):
                return P2CardActionTriggerResponse(invalid)
            # mode=rw prevents an early callback from creating an uninitialized database.
            with closing(sqlite3.connect(
                self.path.as_uri() + "?mode=rw", uri=True, timeout=0.2,
            )) as db, db:
                row = db.execute(
                    "SELECT state FROM tasks WHERE json_extract(state,'$.card_id')=?",
                    (event.context.open_message_id,),
                ).fetchone()
                state = json.loads(row[0]) if row else {}
                if (
                    state.get("chat_id") != event.context.open_chat_id
                    or not hmac.compare_digest(state.get("page_token", ""), value["token"])
                    or not state.get("answer") or page >= len(answer_pages(state["answer"]))
                ):
                    return P2CardActionTriggerResponse(invalid)
                db.execute(
                    "INSERT OR REPLACE INTO page_requests VALUES (?,?,?,?)",
                    (state["correlation_id"], page, uuid.uuid4().hex, time.time()),
                )
            return P2CardActionTriggerResponse({
                "toast": {"type": "info", "content": f"正在切换到第 {page + 1} 页"},
            })
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            return P2CardActionTriggerResponse(invalid)
