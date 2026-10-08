"""Persistent weekly schedules independent of Claude's session-local cron."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)


def tick_schedules(store, now):
    with store._lock:
        schedules = store._connection.execute("SELECT * FROM runtime_schedules").fetchall()
    for row in schedules:
        try:
            _tick_one(store, row, now)
        except (ValueError, KeyError, TypeError) as exc:
            LOGGER.error("Invalid schedule %s (%s)", row["schedule_id"], type(exc).__name__)


def _tick_one(store, row, now):
    config = json.loads(row["config"])
    local = datetime.fromtimestamp(now, ZoneInfo(config.get("timezone", "Asia/Shanghai")))
    occurrence = (local - timedelta(days=(local.weekday() - config["weekday"]) % 7)).replace(
        hour=config["hour"], minute=config.get("minute", 0), second=0, microsecond=0
    )
    if occurrence > local:
        occurrence -= timedelta(days=7)
    period = occurrence.date().isoformat()
    if row["last_period"] and period <= row["last_period"]:
        return
    prompt = config["prompt"].replace("{week}", period)
    ident = f"schedule:{row['schedule_id']}:{period}"
    # Input deduplication covers a crash between insert and advancing the schedule cursor.
    store.accept_event(ident, config["chat_id"], prompt)
    with store._lock:
        store._connection.execute(
            "UPDATE runtime_schedules SET last_period=? WHERE schedule_id=?",
            (period, row["schedule_id"]),
        )
