"""Independent receipt/progress service; never calls a model or dispatches a task."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import re
import secrets
import signal
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .card_delivery import answer_digest, answer_pages
from .config import Settings
from .feishu import FeishuClient, outbound_message_uuid
from .progress_events import apply_entry, envelope_correlation
from .resident import DEFAULT_CONFIG, health_error, load_health, read_json
from .task_reactions import advance_status_reaction, reaction_pending, receipt_emoji

LOGGER = logging.getLogger(__name__)
_TERMINAL = {"replied", "failed"}
_MAX_LINE = 8 * 1024 * 1024


def read_environment(path: Path) -> dict[str, str]:
    values = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def progress_card(state: dict, *, now: float, connection: str) -> dict:
    answer = state.get("answer")
    terminal = state["status"] in _TERMINAL or bool(answer)
    failed = state["status"] == "failed" or state.get("task_state") in {
        "failed",
        "cancelled",
        "suspended",
    }
    partial = (terminal and state.get("business_outcome") == "partial"
               and state.get("task_state") != "cancelled")
    title = (
        "Claude · 已完成"
        if terminal and not failed
        else ("Claude · 任务中断" if failed else "Claude · 正在处理")
    )
    phase = state["phase"]
    if terminal:
        current = (
            ("已完成 · 回复见下方。" if answer else "已完成。")
            if not failed
            else {
                "resident_restart_interrupted": "会话重启，任务已中断；请发消息检查并继续。",
                "image_acquisition_failed": "图片读取失败，请重新发送图片。",
            }.get(state.get("failure_reason"), "任务未能完成，请查看回复或重新发送请求。")
        )
    elif state["status"] == "acquiring":
        current = "已收到，正在读取图片。"
    elif state["status"] == "accepted":
        title, current = "Claude · 已收到，排队中", "消息已入队，等待前一个任务结束。"
    elif state.get("permission_pending"):
        title, current = "Claude · 等待授权", "请在授权卡片上同意或拒绝本次操作。"
    elif phase == "queued":
        current = "已交给 Claude，等待开始处理。"
    elif phase == "working":
        current = "正在执行工具，具体步骤见下方。"
    elif phase == "awaiting_reply":
        current = "本轮执行已结束，等待最终回复。"
    else:
        current = "正在等待模型响应。"
    if not terminal and connection != "桥接在线":
        title = "Claude · 连接异常"
    if partial:
        title, current = "Claude · 部分完成", "已有成果见下方；证据缺口和继续方式已保留。"
    elapsed = max(0, int((state.get("ended_at") or now) - state["created_at"]))
    lines = [
        title.removeprefix("Claude · "),
        current,
        f"用时：{elapsed // 60} 分 {elapsed % 60} 秒",
    ]
    if not terminal:
        lines.append(f"连接：{connection}")
        idle = max(0, int(now - state["activity_at"]))
        if idle >= 60 and state["status"] not in {"accepted", "acquiring"}:
            lines.append(f"最近 {idle} 秒没有新执行记录；心跳正常不代表任务已有新进展。")
    if state.get("warning"):
        lines.append(f"最近异常：{state['warning']}")
    if state.get("observer_warning"):
        lines.append(state["observer_warning"])
    if state["steps"]:
        lines.append("\n最近步骤")
        icons = {"running": "⏳", "done": "✓", "error": "⚠"}
        for step in state["steps"]:
            status = step["status"]
            icon = icons[status]
            label = (
                "（已中断）"
                if failed and status == "running"
                else ("（失败）" if status == "error" else "")
            )
            if terminal and status == "running":
                icon = "⚠" if failed else "•"
                if not failed:
                    label = "（未记录工具结果）"
                if not failed and step["label"] == "发送最终回复":
                    icon, label = "✓", "（已发送）"
            lines.append(f"{icon} {step['label']}{label}")
    stamp = datetime.fromtimestamp(now, ZoneInfo("Asia/Shanghai"))
    footer = f"更新于 {stamp:%H:%M:%S}（北京时间）"
    if not terminal:
        footer += " · 约每 15 秒刷新；超过 60 秒未刷新，连接可能异常。"
    # Escape observational text; the final answer intentionally uses real Markdown.
    summary = re.sub(r"([\\`*_{}\[\]<>#])", r"\\\1", "\n".join(lines))
    elements = [{"tag": "markdown", "content": summary}]
    if answer:
        pages = answer_pages(answer)
        page = min(max(0, state.get("page", 0)), len(pages) - 1)
        elements.extend([{"tag": "hr"}, {"tag": "markdown", "content": pages[page]}])
        if len(pages) > 1:
            elements.append({"tag": "markdown", "content": f"第 {page + 1} / {len(pages)} 页"})
            for index, label in ((page - 1, "上一页"), (page + 1, "下一页")):
                if 0 <= index < len(pages):
                    elements.append(
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": label},
                            "type": "default",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": {
                                        "kind": "progress_page",
                                        "page": index,
                                        "token": state["page_token"],
                                    },
                                }
                            ],
                        }
                    )
    action = None
    actions = state.get("recovery", {}).get("available_actions", [])
    if "cancel" in actions or (
        "recovery" not in state and state.get("task_state") in {"running", "queued", "retry_wait"}
    ):
        action, label = "cancel", "取消任务"
    elif "continue" in actions or (
        "recovery" not in state and state.get("task_state") in {"failed", "cancelled"}
    ):
        action, label = "continue", "核查进度并继续"
    if action:
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": label},
                "type": "default",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": {
                            "kind": "task_control",
                            "action": action,
                            "token": state["page_token"],
                            "attempt": state.get("attempt_id"),
                        },
                    }
                ],
            }
        )
    elements.append({"tag": "markdown", "content": footer, "text_size": "notation"})
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "enable_forward": False},
        "header": {
            "template": "orange" if partial else "red"
            if failed
            else "green"
            if terminal
            else ("orange" if connection != "桥接在线" or state.get("warning") else "blue"),
            "title": {
                "tag": "plain_text",
                "content": (
                    f"{state.get('session_name', 'Claude')} · {state.get('model', '未知模型')}"
                ),
            },
        },
        "body": {"elements": elements},
    }


class ProgressMonitor:
    def __init__(
        self,
        *,
        database: Path,
        state_path: Path,
        transcript: Path,
        session_id: str,
        health_path: Path,
        resident_path: Path,
        client: Any,
        session_name: str = "Claude",
        model: str = "未知模型",
        runtime_mode: bool = False,
    ) -> None:
        self.runtime_mode = runtime_mode
        self.delivery = None
        if runtime_mode:
            from .runtime_store import RuntimeStore

            self.delivery = RuntimeStore(database)
        self.client = client
        self.transcript = transcript
        self.session_id = session_id
        self.session_name = session_name
        self.model = model
        self.health_path = health_path
        self.resident_path = resident_path
        self.source = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
        self.source.row_factory = sqlite3.Row
        state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(state_path.parent, 0o700)
        self.db = sqlite3.connect(state_path)
        state_path.chmod(0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        self.db.execute("""CREATE TABLE IF NOT EXISTS tasks (
            correlation_id TEXT PRIMARY KEY, state TEXT NOT NULL,
            finished INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS page_requests (
            correlation_id TEXT PRIMARY KEY, page INTEGER, version TEXT, created_at REAL
        )""")
        self.meta = {k: json.loads(v) for k, v in self.db.execute("SELECT key,value FROM meta")}
        self.tasks = {
            cid: json.loads(raw)
            for cid, raw in self.db.execute(
                "SELECT correlation_id,state FROM tasks WHERE finished=0 ORDER BY created_at"
            )
        }
        if not self.meta:
            # Enable prospectively: do not add reactions/cards to old conversations.
            maximum = self.source.execute(
                "SELECT COALESCE(max(sequence),0) FROM events"
            ).fetchone()[0]
            pending = self.source.execute(
                "SELECT min(sequence) FROM events WHERE status NOT IN ('replied','failed')"
            ).fetchone()[0]
            self.meta = {
                "sequence": maximum if pending is None else pending - 1,
                "offset": 0,
                "inode": None,
                "active": None,
                "initial_until": maximum,
            }
            if self.transcript.exists():
                stat = self.transcript.stat()
                self.meta.update(offset=stat.st_size, inode=stat.st_ino)
        self._checkpoint()

    def close(self) -> None:
        self._checkpoint()
        self.db.close()
        self.source.close()
        if self.delivery:
            self.delivery.close()

    def _checkpoint(self) -> None:
        with self.db:
            for key, value in self.meta.items():
                self.db.execute(
                    "INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value))
                )
            for cid, state in self.tasks.items():
                self.db.execute(
                    "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?)",
                    (
                        cid,
                        json.dumps(state, ensure_ascii=False),
                        int(state.get("finished", False)),
                        state["created_at"],
                    ),
                )
            self.db.execute(
                "DELETE FROM tasks WHERE finished=1 AND created_at<?", (time.time() - 7 * 86400,)
            )

    def _discover(self, now: float) -> None:
        rows = self.source.execute(
            """SELECT sequence,correlation_id,message_id,chat_id,status,updated_at FROM events
               WHERE sequence>? ORDER BY sequence LIMIT 100""",
            (self.meta["sequence"],),
        ).fetchall()
        for row in rows:
            self.meta["sequence"] = row["sequence"]
            if row["sequence"] <= self.meta["initial_until"] and row["status"] in _TERMINAL:
                continue
            cid = row["correlation_id"]
            self.tasks[cid] = {
                "correlation_id": cid,
                "message_id": row["message_id"],
                "chat_id": row["chat_id"],
                "status": row["status"],
                "phase": "queued",
                "steps": [],
                "warning": None,
                "created_at": now,
                "activity_at": now,
                "started_at": None,
                "card_id": None,
                "sent_at": 0,
                "digest": None,
                "reaction_done": row["message_id"].startswith("schedule:"),
                "reaction_attempts": 0,
                "reaction_retry_at": 0,
                "receipt_emoji": receipt_emoji(row["message_id"]) if self.runtime_mode else "OK",
                "status_reactions_enabled": self.runtime_mode
                and not row["message_id"].startswith("schedule:"),
                "card_retry_at": 0,
                "card_attempts": 0,
            }

    def _read_transcript(self, now: float, mcp_pid: int | None) -> None:
        try:
            stat = self.transcript.stat()
            if stat.st_ino != self.meta["inode"] or stat.st_size < self.meta["offset"]:
                self.meta.update(offset=0, inode=stat.st_ino, active=None)
            with self.transcript.open("rb") as stream:
                stream.seek(self.meta["offset"])
                for _ in range(128):
                    start = stream.tell()
                    line = stream.readline(_MAX_LINE + 1)
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        if len(line) > _MAX_LINE:
                            # Skip an oversized record in bounded reads; never parse fragments.
                            while line and not line.endswith(b"\n"):
                                line = stream.readline(_MAX_LINE + 1)
                            if not line:
                                self.meta["offset"] = start
                                break
                            self.meta["offset"] = stream.tell()
                            self._observer_warning("执行记录过大，已跳过一个步骤详情。")
                            continue
                        break  # Writer has not finished the line; retry from its beginning.
                    self.meta["offset"] = stream.tell()
                    try:
                        entry = json.loads(line)
                        if isinstance(entry, dict):
                            self._entry(entry, now, mcp_pid)
                    except (ValueError, TypeError, AttributeError):
                        self._observer_warning("部分执行记录无法解析，任务状态仍以回复结果为准。")
        except OSError:
            self._observer_warning("暂时无法读取执行记录，等待恢复。")

    def _observer_warning(self, message: str) -> None:
        for state in self.tasks.values():
            if state["status"] not in _TERMINAL:
                state["observer_warning"] = message

    def _entry(self, entry: dict, now: float, mcp_pid: int | None) -> None:
        if entry.get("sessionId") != self.session_id or entry.get("isSidechain"):
            return
        cid = envelope_correlation(entry, mcp_pid) if entry.get("type") == "user" else None
        if cid in self.tasks and self.tasks[cid]["status"] not in _TERMINAL:
            state = self.tasks[cid]
            self.meta["active"] = cid
            state.update(phase="thinking", activity_at=now, started_at=now)
            return
        state = self.tasks.get(self.meta["active"])
        if state is None or state["status"] in _TERMINAL:
            return
        # Local or other peer prompts must not be attributed to a Feishu task.
        if entry.get("type") == "user" and not entry.get("isMeta"):
            blocks = entry.get("message", {}).get("content", [])
            if isinstance(blocks, str) or (
                isinstance(blocks, list)
                and blocks
                and all(isinstance(b, dict) and b.get("type") != "tool_result" for b in blocks)
            ):
                self.meta["active"] = None
                return
        state.pop("observer_warning", None)
        apply_entry(state, entry, now)

    def _refresh(self, now: float) -> None:
        waiting_chats = {
            row[0]
            for row in self.source.execute(
                """SELECT chat_id FROM permission_requests
               WHERE session_id=? AND status='pending' AND expires_at>?""",
                (self.session_id, int(now)),
            )
        }
        for cid, state in self.tasks.items():
            row = self.source.execute(
                """SELECT status,failure_reason,updated_at,reply_format,reply_text
                   FROM events WHERE correlation_id=?""",
                (cid,),
            ).fetchone()
            status = row["status"] if row else "failed"
            state["status"] = status
            if self.runtime_mode:
                version = self.source.execute(
                    "SELECT result_id FROM runtime_outbox "
                    "WHERE correlation_id=? AND state='pending'",
                    (cid,),
                ).fetchone()
                if version:
                    state["result_id"] = version[0]
                elif status != "replied":
                    state["result_id"] = None
            state.setdefault("session_name", self.session_name)
            state.setdefault("model", self.model)
            state.setdefault("page_token", secrets.token_urlsafe(24))
            if row and row["reply_format"] == "card_v1" and row["reply_text"]:
                state["answer"] = row["reply_text"]
                state["answer_hash"] = answer_digest(row["reply_text"])
                state.setdefault("ended_at", max(state["created_at"], row["updated_at"]))
            elif self.runtime_mode:
                for key in ("answer", "answer_hash", "answer_delivered_hash", "ended_at"):
                    state.pop(key, None)
            state["permission_pending"] = state["chat_id"] in waiting_chats and status in {
                "dispatching",
                "delivered",
                "replying",
            }
            if status in _TERMINAL:
                state["failure_reason"] = row["failure_reason"] if row else "event_expired"
                state.setdefault(
                    "ended_at", max(state["created_at"], row["updated_at"] if row else now)
                )
                if self.meta["active"] == cid:
                    self.meta["active"] = None

    def _runtime_refresh(self):
        from .runtime_recovery import recovery_options

        # Tasks may be explicitly continued after their previous card was finalized.
        rows = self.source.execute(
            "SELECT * FROM runtime_tasks ORDER BY updated_at DESC LIMIT 256"
        ).fetchall()
        for row in rows:
            cid = row["correlation_id"]
            recovery = recovery_options(self.source, cid)
            if cid not in self.tasks:
                saved = self.db.execute(
                    "SELECT state FROM tasks WHERE correlation_id=?", (cid,)
                ).fetchone()
                if saved:
                    previous = json.loads(saved[0])
                    if (
                        row["state"] in {"queued", "running", "retry_wait"}
                        or previous.get("recovery") != recovery
                    ):
                        self.tasks[cid] = previous
                        self.tasks[cid]["finished"] = False
            state = self.tasks.get(cid)
            if state is None:
                continue
            state.update(
                task_state=row["state"], steps=json.loads(row["steps"]), warning=row["warning"]
            )
            state["business_outcome"] = row["business_outcome"]
            state["recovery"] = recovery
            if state.get("attempt_id") != row["attempt_id"]:
                state["page_token"] = secrets.token_urlsafe(24)
                state.pop("reaction_waiting", None)
            state["attempt_id"] = row["attempt_id"]
            if row["activity_at"]:
                state["activity_at"] = row["activity_at"]
            state["phase"] = (
                "working" if any(s["status"] == "running" for s in state["steps"]) else "thinking"
            )
            if row["state"] == "retry_wait":
                state["phase"] = "queued"
            if row["attempt_id"]:
                attempt = self.source.execute(
                    "SELECT session_id FROM runtime_attempts WHERE attempt_id=?",
                    (row["attempt_id"],),
                ).fetchone()
                state["permission_pending"] = bool(
                    attempt
                    and self.source.execute(
                        "SELECT 1 FROM permission_requests WHERE session_id=? "
                        "AND status='pending' AND expires_at>?",
                        (attempt[0], int(time.time())),
                    ).fetchone()
                )

    def _connection(self, now: float) -> tuple[str, int | None]:
        health = load_health(self.health_path)
        resident = load_health(self.resident_path)
        error = health_error(health, resident.get("instance", ""), now)
        if error:
            return "桥接心跳中断，等待恢复", health.get("mcp_pid")
        return (
            "桥接在线"
            if health.get("websocket_connected") is True
            else "飞书接收连接中断，正在重连"
        ), health.get("mcp_pid")

    def tick(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self._discover(now)
        for cid, page, version in self.db.execute(
            "SELECT correlation_id,page,version FROM page_requests"
        ).fetchall():
            if cid not in self.tasks:
                row = self.db.execute(
                    "SELECT state FROM tasks WHERE correlation_id=?",
                    (cid,),
                ).fetchone()
                if row:
                    self.tasks[cid] = json.loads(row[0])
            if cid in self.tasks:
                self.tasks[cid].update(page=page, page_request_version=version, finished=False)
        connection, mcp_pid = self._connection(now)
        if not self.runtime_mode:
            self._read_transcript(now, mcp_pid)
        else:
            self._runtime_refresh()
        self._refresh(now)
        if self.runtime_mode:
            self._runtime_refresh()
        self._checkpoint()  # Cursor and extracted state commit together before network I/O.
        calls = 0
        # New receipts take priority over heartbeat patches for older tasks.
        states = sorted(self.tasks.values(), key=lambda s: (s["reaction_done"], s["sent_at"]))
        max_calls = 2 if self.runtime_mode else 6
        for state in states:
            if calls >= max_calls:
                break
            if not state["reaction_done"] and now >= state["reaction_retry_at"]:
                calls += 1
                try:
                    if self.runtime_mode:
                        self.client.add_received_reaction(
                            state["message_id"], state.get("receipt_emoji", "OK")
                        )
                    else:
                        self.client.add_received_reaction(state["message_id"])
                    state["reaction_done"] = True
                except Exception as exc:
                    state["reaction_attempts"] += 1
                    state["reaction_retry_at"] = now + 30
                    state["receipt_emoji"] = "OK"
                    if state["reaction_attempts"] >= 3:
                        state["reaction_done"] = True  # The card remains an explicit receipt.
                    LOGGER.warning("Receipt reaction unavailable (%s)", type(exc).__name__)
                self._checkpoint()
            key = {
                k: state.get(k)
                for k in (
                    "status",
                    "phase",
                    "steps",
                    "warning",
                    "observer_warning",
                    "permission_pending",
                    "answer_hash",
                    "result_id",
                    "page",
                    "session_name",
                    "model",
                    "page_request_version",
                    "task_state",
                    "recovery",
                )
            }
            key["connection"] = connection
            digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
            terminal = state["status"] in _TERMINAL
            due = state["card_id"] is None or (
                now - state["sent_at"] >= 3
                and (digest != state["digest"] or (not terminal and now - state["sent_at"] >= 15))
            )
            if due and now >= state["card_retry_at"] and calls < max_calls:
                calls += 1
                card = progress_card(state, now=now, connection=connection)
                try:
                    if state["card_id"] is None:
                        state["card_id"] = self.client.send_card(
                            state["chat_id"],
                            card,
                            outbound_message_uuid(state["correlation_id"] + ":progress", 0),
                        )
                    else:
                        self.client.update_card(state["card_id"], card)
                    state.update(sent_at=now, digest=digest, card_attempts=0, card_retry_at=0)
                    if state.get("answer_hash"):
                        state["answer_delivered_hash"] = state["answer_hash"]
                        if self.delivery and self.delivery.acknowledge_result(
                            state["correlation_id"],
                            state["answer"],
                            state["card_id"],
                            now,
                            state.get("result_id"),
                        ):
                            state["status"] = "replied"
                            terminal = True
                    if state.get("page_request_version"):
                        with self.db:
                            self.db.execute(
                                "DELETE FROM page_requests WHERE correlation_id=? AND version=?",
                                (state["correlation_id"], state["page_request_version"]),
                            )
                    if terminal:
                        waiting = reaction_pending(state, now)
                        state["finished"] = not waiting
                        if waiting:
                            state["reaction_waiting"] = True
                    LOGGER.info("Progress card updated status=%s", state["status"])
                except Exception as exc:
                    state["card_attempts"] += 1
                    state["card_retry_at"] = now + min(60, 5 * 2 ** min(state["card_attempts"], 4))
                    LOGGER.warning("Progress card unavailable (%s)", type(exc).__name__)
                self._checkpoint()
        # Status reactions run after cards. A delivered result is never delayed by emoji APIs.
        for state in states:
            if calls < max_calls and advance_status_reaction(self.client, state, now):
                calls += 1
            if state.get("finished") and reaction_pending(state, now):
                state["finished"] = False
                state["reaction_waiting"] = True
            elif (state.get("reaction_waiting") and state.get("status") in _TERMINAL
                  and not reaction_pending(state, now)):
                state["finished"] = True
                state.pop("reaction_waiting", None)
        self._checkpoint()
        self.tasks = {cid: state for cid, state in self.tasks.items() if not state.get("finished")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = read_json(args.config)
    runtime_mode = config.get("runtime_enabled", False)
    settings = Settings.from_env(read_environment(Path(config["bridge_env_file"])))
    if settings.database_path.resolve() != Path(config["database_path"]).resolve():
        raise ValueError("progress database must match the configured bridge database")
    state_dir = Path(config["progress_state_dir"])
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(state_dir, 0o700)
    with (state_dir / "progress.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = ProgressMonitor(
            database=settings.database_path,
            state_path=settings.database_path if runtime_mode else state_dir / "progress.sqlite3",
            transcript=Path(config.get("transcript_path") or state_dir / "unused-transcript"),
            session_id=config.get("session_id") or "",
            health_path=Path(config["state_dir"]) / "health.json",
            resident_path=Path(config["state_dir"]) / "resident.json",
            client=FeishuClient(app_id=settings.app_id, app_secret=settings.app_secret, timeout=5),
            session_name=config["name"],
            model=config.get("model") or "默认模型",
            runtime_mode=runtime_mode,
        )
        runtime = None
        if runtime_mode:
            from .runtime_store import RuntimeStore

            runtime = RuntimeStore(settings.database_path)
        stopped = threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopped.set())
        LOGGER.info("Feishu progress monitor started")
        from .runtime_common import notify

        notify("READY=1")
        try:
            while not stopped.is_set():
                try:
                    if runtime:
                        _send_controls(runtime, monitor.client)
                    monitor.tick()
                    notify()
                except Exception as exc:
                    LOGGER.error("Progress observation failed (%s)", type(exc).__name__)
                stopped.wait(0.5)
        finally:
            with contextlib.suppress(Exception):
                monitor.close()
            if runtime:
                runtime.close()


def _send_controls(store, client):
    # Priority capacity is reserved for control replies, independent of failed progress cards.
    now = time.time()
    with store._lock:
        rows = store._connection.execute(
            "SELECT * FROM runtime_controls WHERE sent=0 AND retry_at<=? "
            "ORDER BY retry_at,rowid LIMIT 1",
            (now,),
        ).fetchall()
    for row in rows:
        try:
            client.reply_markdown(
                row["message_id"],
                row["answer"],
                outbound_message_uuid(row["message_id"] + ":control", 0),
            )
            with store._lock:
                store._connection.execute(
                    "UPDATE runtime_controls SET sent=1 WHERE message_id=?", (row["message_id"],)
                )
        except Exception:
            with store._lock:
                store._connection.execute(
                    "UPDATE runtime_controls SET attempts=attempts+1,retry_at=? WHERE message_id=?",
                    (now + min(300, 5 * 2 ** min(row["attempts"], 6)), row["message_id"]),
                )


if __name__ == "__main__":
    main()
