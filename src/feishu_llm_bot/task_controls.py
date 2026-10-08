"""Authenticated task-card controls; no model invocation or network wait in callbacks."""

from __future__ import annotations

import hmac
import json
import sqlite3
from contextlib import closing

from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse

from .feishu import IncomingMessage


class TaskControls:
    def __init__(self, store, progress_path, app_id, owner):
        self.store, self.path, self.app_id, self.owner = store, progress_path, app_id, owner

    def handle(self, data):
        invalid = {"toast": {"type": "error", "content": "任务操作已过期或不属于当前用户。"}}
        try:
            event = data.event
            value = event.action.value
            if (
                data.header.app_id != self.app_id
                or event.operator.open_id != self.owner
                or data.header.event_type != "card.action.trigger"
                or event.host != "im_message"
                or event.action.tag != "button"
                or not isinstance(value, dict)
                or set(value) != {"kind", "action", "token", "attempt"}
                or value["kind"] != "task_control"
                or value["action"] not in {"cancel", "continue"}
                or not isinstance(value["token"], str)
            ):
                return P2CardActionTriggerResponse(invalid)
            with closing(
                sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=0.2)
            ) as db:
                row = db.execute(
                    "SELECT state FROM tasks WHERE json_extract(state,'$.card_id')=?",
                    (event.context.open_message_id,),
                ).fetchone()
            saved = json.loads(row[0]) if row else {}
            if saved.get("chat_id") != event.context.open_chat_id or not hmac.compare_digest(
                saved.get("page_token", ""), value["token"]
            ):
                return P2CardActionTriggerResponse(invalid)
            task = self.store.get_by_correlation(saved["correlation_id"])
            if task is None:
                return P2CardActionTriggerResponse(invalid)
            self.store.handle_control(
                IncomingMessage.text(
                    "callback:" + data.header.event_id,
                    task.chat_id,
                    f"/{value['action']} {task.sequence}",
                ),
                expected_attempt=value["attempt"],
            )
            return P2CardActionTriggerResponse(
                {"toast": {"type": "info", "content": "操作已记录，请查看任务卡状态。"}}
            )
        except (AttributeError, KeyError, ValueError, TypeError, sqlite3.Error):
            return P2CardActionTriggerResponse(invalid)
