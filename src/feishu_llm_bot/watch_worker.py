"""Read-only local monitor for supervised Claude and TraeX workers."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .progress_events import tool_label

DEFAULT_CONFIG = Path.home() / ".local/state/feishu-llm-bot/runtime.json"
ACTIVE_STATES = ("starting", "running", "draining")


@dataclass(frozen=True)
class ActiveAttempt:
    attempt_id: str
    correlation_id: str
    state: str
    started_at: float
    session_id: str | None
    unit_name: str | None


def open_runtime_database(path: Path) -> sqlite3.Connection:
    """Open the runtime database without initializing or mutating its schema."""
    resolved = path.expanduser().resolve(strict=True)
    connection = sqlite3.connect(
        resolved.as_uri() + "?mode=ro",
        uri=True,
        timeout=1,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=1000")
    return connection


def active_attempt(connection: sqlite3.Connection) -> ActiveAttempt | None:
    placeholders = ",".join("?" for _ in ACTIVE_STATES)
    row = connection.execute(
        "SELECT attempt_id,correlation_id,state,started_at,session_id,unit_name "
        f"FROM runtime_attempts WHERE state IN ({placeholders}) "
        "ORDER BY started_at DESC LIMIT 1",
        ACTIVE_STATES,
    ).fetchone()
    return ActiveAttempt(**dict(row)) if row else None


def attempt_outcome(connection: sqlite3.Connection, attempt_id: str) -> tuple[str, str | None]:
    row = connection.execute(
        "SELECT state,failure_reason FROM runtime_attempts WHERE attempt_id=?",
        (attempt_id,),
    ).fetchone()
    return (row["state"], row["failure_reason"]) if row else ("unknown", None)


def _tail_offset(stream: BinaryIO, line_count: int) -> int:
    stream.seek(0, 2)
    size = stream.tell()
    if line_count <= 0 or size == 0:
        return size
    stream.seek(size - 1)
    target = line_count + (stream.read(1) == b"\n")
    found = 0
    position = size
    while position:
        length = min(8192, position)
        position -= length
        stream.seek(position)
        block = stream.read(length)
        for index in range(length - 1, -1, -1):
            if block[index] == 10:
                found += 1
                if found >= target:
                    return position + index + 1
    return 0


class TranscriptFollower:
    def __init__(self, path: Path, history: int):
        self.path = path
        self.stream = path.open("rb")
        self.stream.seek(_tail_offset(self.stream, history))
        self.pending = b""

    def close(self) -> None:
        self.stream.close()

    def read_lines(self) -> list[str]:
        data = self.stream.read()
        if not data:
            return []
        chunks = (self.pending + data).split(b"\n")
        self.pending = chunks.pop()
        return [chunk.decode("utf-8", errors="replace") for chunk in chunks if chunk]


class EntryFormatter:
    def __init__(self, *, details: bool = False, raw: bool = False):
        self.details = details
        self.raw = raw
        self.tools: dict[str, str] = {}

    @staticmethod
    def _stamp(entry: dict) -> str:
        value = entry.get("timestamp")
        if isinstance(value, str) and len(value) >= 19:
            return value[11:19]
        return time.strftime("%H:%M:%S")

    @staticmethod
    def _detail(value: object) -> str:
        if isinstance(value, str):
            rendered = value
        else:
            rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return rendered[:4000] + ("…" if len(rendered) > 4000 else "")

    def format(self, line: str) -> list[str]:
        if self.raw:
            return [line]
        try:
            entry = json.loads(line)
        except (TypeError, ValueError):
            return ["[watch-worker] 跳过无法解析的 JSONL 记录"]
        if not isinstance(entry, dict):
            return []
        stamp = self._stamp(entry)
        kind = entry.get("type")
        message = entry.get("message")
        content = message.get("content", []) if isinstance(message, dict) else []
        if not isinstance(content, list):
            content = []
        rendered: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if kind == "assistant" and block_type == "thinking" and block.get("thinking"):
                rendered.append(f"[{stamp}] 助手正在思考…")
            elif kind == "assistant" and block_type == "text" and block.get("text"):
                rendered.append(f"[{stamp}] 助手\n{block['text']}")
            elif kind == "assistant" and block_type == "tool_use":
                name = str(block.get("name", "工具"))
                label = tool_label(name, block.get("input"))
                tool_id = block.get("id")
                if isinstance(tool_id, str):
                    self.tools[tool_id] = label
                text = f"[{stamp}] 工具调用 · {label}"
                if self.details:
                    text += "\n" + self._detail(block.get("input", {}))
                rendered.append(text)
            elif kind == "user" and block_type == "tool_result":
                tool_id = block.get("tool_use_id")
                label = self.tools.get(str(tool_id), str(tool_id or "未知工具"))
                status = "失败" if block.get("is_error") else "完成"
                text = f"[{stamp}] 工具{status} · {label}"
                if self.details:
                    text += "\n" + self._detail(block.get("content", ""))
                rendered.append(text)
        if kind == "system" and entry.get("subtype") == "init":
            model = entry.get("model") or "unknown"
            rendered.append(f"[{stamp}] 助手会话已启动 · model={model}")
        elif kind == "system" and entry.get("subtype") == "turn_duration":
            rendered.append(f"[{stamp}] 助手本轮处理结束")
        elif kind == "result":
            result = entry.get("result")
            if result:
                rendered.append(f"[{stamp}] 最终结果\n{result}")
            else:
                rendered.append(f"[{stamp}] 助手执行结束 · {entry.get('subtype', 'unknown')}")
        return rendered


def _project_slug(cwd: str) -> str:
    return str(Path(cwd).expanduser().resolve()).replace("/", "-")


def find_transcript(claude_dir: Path, cwd: str, session_id: str) -> Path | None:
    try:
        canonical = str(uuid.UUID(session_id))
    except (ValueError, AttributeError):
        return None
    if canonical != session_id.lower():
        return None
    projects = claude_dir.expanduser() / "projects"
    direct = projects / _project_slug(cwd) / f"{session_id}.jsonl"
    if direct.is_file():
        return direct
    matches = list(projects.glob(f"*/{session_id}.jsonl"))
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


class WorkerMonitor:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        cwd: str,
        claude_dir: Path,
        history: int,
        formatter: EntryFormatter,
        runtime_state_dir: Path | None = None,
        backend: str = "claude",
    ):
        self.connection = connection
        self.cwd = cwd
        self.claude_dir = claude_dir
        self.runtime_state_dir = runtime_state_dir
        self.backend = backend
        self.history = history
        self.formatter = formatter
        self.attempt: ActiveAttempt | None = None
        self.follower: TranscriptFollower | None = None
        self.waiting_signature: tuple[str, str] | None = None

    def close(self) -> None:
        if self.follower:
            self.follower.close()
            self.follower = None

    def _drain(self) -> list[str]:
        if not self.follower:
            return []
        rendered: list[str] = []
        for line in self.follower.read_lines():
            rendered.extend(self.formatter.format(line))
        return rendered

    def _finish_current(self) -> list[str]:
        if not self.attempt:
            return []
        rendered = self._drain()
        state, reason = attempt_outcome(self.connection, self.attempt.attempt_id)
        suffix = f" · reason={reason}" if reason else ""
        rendered.append(
            f"[watch-worker] attempt={self.attempt.attempt_id} 已结束 · state={state}{suffix}"
        )
        self.close()
        self.attempt = None
        return rendered

    def poll(self) -> list[str]:
        active = active_attempt(self.connection)
        rendered: list[str] = []
        if self.attempt and (not active or active.attempt_id != self.attempt.attempt_id):
            rendered.extend(self._finish_current())
        if active is None:
            signature = ("idle", "")
            if self.waiting_signature != signature:
                rendered.append("[watch-worker] 当前没有运行中的 worker，等待新任务…")
                self.waiting_signature = signature
            return rendered
        if active.state == "starting":
            signature = (active.attempt_id, "starting")
            if self.waiting_signature != signature:
                rendered.append(
                    f"[watch-worker] attempt={active.attempt_id} 正在启动，等待物理会话…"
                )
                self.waiting_signature = signature
            return rendered
        if not active.session_id:
            signature = (active.attempt_id, "session")
            if self.waiting_signature != signature:
                rendered.append(
                    f"[watch-worker] attempt={active.attempt_id} 尚未报告 session_id…"
                )
                self.waiting_signature = signature
            return rendered
        changed = (
            self.attempt is None
            or self.attempt.attempt_id != active.attempt_id
            or self.attempt.session_id != active.session_id
        )
        if changed:
            self.close()
            self.attempt = active
            self.formatter.tools.clear()
            rendered.append(
                "[watch-worker] 开始监视"
                f" · task={active.correlation_id}"
                f" · attempt={active.attempt_id}"
                f" · session={active.session_id}"
                f" · unit={active.unit_name or 'unknown'}"
            )
        if self.backend == "traex" and self.runtime_state_dir is not None:
            candidate = (self.runtime_state_dir.parent / "tasks" / active.correlation_id
                         / active.attempt_id / "events.jsonl")
            transcript = candidate if candidate.is_file() else None
        else:
            transcript = find_transcript(self.claude_dir, self.cwd, active.session_id)
        if transcript is None:
            signature = (active.attempt_id, "transcript")
            if self.waiting_signature != signature:
                rendered.append("[watch-worker] 会话已建立，等待 transcript JSONL…")
                self.waiting_signature = signature
            return rendered
        if self.follower is None:
            self.follower = TranscriptFollower(transcript, self.history)
            rendered.append(f"[watch-worker] transcript={transcript}")
        self.waiting_signature = None
        rendered.extend(self._drain())
        return rendered


def _read_config(path: Path) -> dict:
    value = json.loads(path.expanduser().read_text())
    if not isinstance(value, dict):
        raise ValueError("runtime config must be a JSON object")
    for field in ("database_path", "cwd"):
        if not isinstance(value.get(field), str) or not value[field]:
            raise ValueError(f"runtime config is missing {field}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only monitor for the currently active Claude or TraeX worker."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--claude-dir", type=Path, default=Path.home() / ".claude")
    parser.add_argument("--history", type=int, default=30, help="recent JSONL records on attach")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--once", action="store_true", help="show one snapshot and exit")
    detail = parser.add_mutually_exclusive_group()
    detail.add_argument(
        "--details",
        action="store_true",
        help="show truncated tool inputs/results; may contain sensitive data",
    )
    detail.add_argument("--raw", action="store_true", help="print raw transcript JSONL")
    args = parser.parse_args()
    if args.history < 0:
        parser.error("--history must be non-negative")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    try:
        config = _read_config(args.config)
        formatter = EntryFormatter(details=args.details, raw=args.raw)
        with open_runtime_database(Path(config["database_path"])) as connection:
            monitor = WorkerMonitor(
                connection,
                cwd=config["cwd"],
                claude_dir=args.claude_dir,
                history=args.history,
                formatter=formatter,
                runtime_state_dir=Path(config["state_dir"]) if config.get("state_dir") else None,
                backend=config.get("agent_backend", "claude"),
            )
            print("[watch-worker] 只读监视已启动；按 Ctrl-C 退出", flush=True)
            if args.details:
                print("[watch-worker] details 模式可能显示敏感的工具参数和结果", flush=True)
            try:
                while True:
                    for line in monitor.poll():
                        print(line, flush=True)
                    if args.once:
                        break
                    time.sleep(args.poll_interval)
            finally:
                monitor.close()
    except KeyboardInterrupt:
        print("\n[watch-worker] 已退出", flush=True)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(2, f"watch-worker: {exc}\n")


if __name__ == "__main__":
    main()
