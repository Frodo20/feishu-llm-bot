from __future__ import annotations

import json
import sqlite3
import uuid

import pytest

from feishu_llm_bot.watch_worker import (
    EntryFormatter,
    TranscriptFollower,
    WorkerMonitor,
    open_runtime_database,
)


def _entry(kind, content=None, **extra):
    value = {"type": kind, "timestamp": "2026-09-28T21:30:00.000Z", **extra}
    if content is not None:
        value["message"] = {"content": content}
    return json.dumps(value, ensure_ascii=False)


def _database(path):
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE runtime_attempts (
        attempt_id TEXT PRIMARY KEY, correlation_id TEXT NOT NULL, state TEXT NOT NULL,
        started_at REAL NOT NULL, session_id TEXT, unit_name TEXT, failure_reason TEXT)"""
    )
    connection.commit()
    return connection


def _insert_attempt(connection, attempt_id, state, session_id, started_at=1):
    connection.execute(
        "INSERT INTO runtime_attempts VALUES (?,?,?,?,?,?,NULL)",
        (
            attempt_id,
            "fs_" + "a" * 32,
            state,
            started_at,
            session_id,
            "feishu-worker-" + attempt_id,
        ),
    )
    connection.commit()


def test_runtime_database_is_opened_query_only(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    writer = _database(path)
    writer.close()
    with open_runtime_database(path) as connection:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
            connection.execute("DELETE FROM runtime_attempts")


def test_formatter_hides_details_by_default_and_links_tool_results():
    formatter = EntryFormatter()
    call = _entry(
        "assistant",
        [
            {
                "type": "tool_use",
                "id": "call-1",
                "name": "Bash",
                "input": {"command": "curl https://example.test/?secret=hidden"},
            }
        ],
    )
    result = _entry(
        "user",
        [{"type": "tool_result", "tool_use_id": "call-1", "content": "secret-result"}],
    )
    output = formatter.format(call) + formatter.format(result)
    assert output == [
        "[21:30:00] 工具调用 · Bash · curl",
        "[21:30:00] 工具完成 · Bash · curl",
    ]
    assert "secret" not in "\n".join(output)


def test_formatter_details_are_explicit_and_truncated():
    formatter = EntryFormatter(details=True)
    output = formatter.format(
        _entry(
            "assistant",
            [{"type": "tool_use", "id": "call-1", "name": "Read", "input": {"value": "x" * 5000}}],
        )
    )
    assert '"value"' in output[0]
    assert output[0].endswith("…")
    assert len(output[0]) < 4100


def test_transcript_follower_reads_history_and_waits_for_complete_line(tmp_path):
    path = tmp_path / "session.jsonl"
    path.write_bytes(b'{"line":1}\n{"line":2}\n{"line":3}\n')
    follower = TranscriptFollower(path, history=2)
    try:
        assert follower.read_lines() == ['{"line":2}', '{"line":3}']
        with path.open("ab") as stream:
            stream.write(b'{"line":4')
        assert follower.read_lines() == []
        with path.open("ab") as stream:
            stream.write(b'}\n')
        assert follower.read_lines() == ['{"line":4}']
    finally:
        follower.close()


def test_traex_monitor_follows_owned_events_without_claude_files(tmp_path):
    path = tmp_path / "bot.sqlite3"
    writer = _database(path)
    _insert_attempt(writer, "attempt-1", "running", str(uuid.uuid4()))
    events = tmp_path / "tasks" / ("fs_" + "a" * 32) / "attempt-1" / "events.jsonl"
    events.parent.mkdir(parents=True)
    events.write_text(_entry("result", result="TraeX result") + "\n")
    with open_runtime_database(path) as connection:
        monitor = WorkerMonitor(connection, cwd=str(tmp_path), claude_dir=tmp_path / "absent",
                                runtime_state_dir=tmp_path / "resident", backend="traex",
                                history=10, formatter=EntryFormatter())
        try:
            assert "TraeX result" in "\n".join(monitor.poll())
        finally:
            monitor.close()
            writer.close()


def test_monitor_ignores_base_session_while_starting_and_follows_physical_session(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    writer = _database(database)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    claude_dir = tmp_path / ".claude"
    transcript_dir = claude_dir / "projects" / str(cwd.resolve()).replace("/", "-")
    transcript_dir.mkdir(parents=True)
    base_session = str(uuid.uuid4())
    physical_session = str(uuid.uuid4())
    (transcript_dir / f"{base_session}.jsonl").write_text(
        _entry("assistant", [{"type": "text", "text": "old history"}]) + "\n"
    )
    with open_runtime_database(database) as reader:
        monitor = WorkerMonitor(
            reader,
            cwd=str(cwd),
            claude_dir=claude_dir,
            history=10,
            formatter=EntryFormatter(),
        )
        try:
            assert "没有运行中的 worker" in "\n".join(monitor.poll())
            _insert_attempt(writer, "attempt-1", "starting", base_session)
            starting = "\n".join(monitor.poll())
            assert "等待物理会话" in starting
            assert "old history" not in starting

            transcript = transcript_dir / f"{physical_session}.jsonl"
            transcript.write_text(
                _entry("assistant", [{"type": "text", "text": "live answer"}]) + "\n"
            )
            writer.execute(
                "UPDATE runtime_attempts SET state='running',session_id=? WHERE attempt_id=?",
                (physical_session, "attempt-1"),
            )
            writer.commit()
            live = "\n".join(monitor.poll())
            assert physical_session in live
            assert "live answer" in live
            assert base_session not in live

            with transcript.open("a") as stream:
                stream.write(
                    _entry(
                        "assistant",
                        [{"type": "tool_use", "id": "tool-1", "name": "Read", "input": {}}],
                    )
                    + "\n"
                )
            assert "Read · 读取文件" in "\n".join(monitor.poll())

            writer.execute(
                "UPDATE runtime_attempts SET state='succeeded' WHERE attempt_id='attempt-1'"
            )
            writer.commit()
            ended = "\n".join(monitor.poll())
            assert "已结束 · state=succeeded" in ended
            assert "等待新任务" in ended
        finally:
            monitor.close()
            writer.close()
