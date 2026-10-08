from __future__ import annotations

import json
import os
import shlex
import sys
import time
from pathlib import Path

import pytest

from feishu_llm_bot.backends import current_python
from feishu_llm_bot.process_workers import ProcessWorkers, identity, session_processes, stop_session
from feishu_llm_bot.runtime_common import private_json
from feishu_llm_bot.runtime_store import RuntimeStore


def test_process_runner_cleans_descendants_after_worker_exit_and_supervisor_restart(tmp_path):
    project = Path(__file__).resolve().parents[1]
    cli = tmp_path / "fake-claude"
    script = tmp_path / "fake-claude.py"
    script.write_text(
        "import json, subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], process_group=0)\n"
        "print(json.dumps({'type':'system','subtype':'init',"
        "'session_id':'local-test'}), flush=True)\n"
        "print(json.dumps({'type':'result','result':'done'}), flush=True)\n"
        "time.sleep(60)\n"
    )
    cli.write_text("#!/bin/sh\nexec " + shlex.join([sys.executable, str(script)]) + "\n")
    cli.chmod(0o700)
    config = {
        "project_dir": str(project), "cwd": str(tmp_path), "name": "fake-local",
        "python_command": current_python(), "node_command": sys.executable,
        "claude_command": str(cli), "state_dir": str(tmp_path / "resident"),
        "database_path": str(tmp_path / "bot.sqlite3"), "worker_runner": "process",
        "integrations": [], "task_timeout_seconds": 15,
    }
    store = RuntimeStore(Path(config["database_path"]))
    runner = ProcessWorkers(config)
    try:
        store.accept_event("local", "no-feishu", "test")
        attempt = store.claim(time.time())
        attempt["timeout_seconds"] = 15
        request = tmp_path / "task" / "request.json"
        private_json(request, {**attempt, "config": config})
        runner.start(attempt, request, config)
        unit = attempt["unit_name"]
        record = json.loads(runner.record(unit).read_text())
        limit = time.monotonic() + 8
        while time.monotonic() < limit and runner.state(unit)["SubState"] != "exited":
            time.sleep(0.05)
        assert runner.state(unit)["SubState"] == "exited"
        assert store.attempt(attempt["attempt_id"])["answer"] == "done"
        assert len(session_processes(record["pid"])) >= 2
        # Recovery has only persisted identity, not the original Popen object.
        ProcessWorkers(config).stop(unit)
        runner.children[unit].wait(timeout=3)
        assert session_processes(record["pid"]) == []
    finally:
        for unit in list(runner.children):
            runner.stop(unit)
        store.close()


def test_pid_identity_mismatch_cannot_signal_an_unrelated_process(monkeypatch):
    from feishu_llm_bot import process_workers

    monkeypatch.setattr(process_workers, "identity", lambda _: "a different process")
    monkeypatch.setattr(process_workers, "session_processes", lambda _: [1234])
    monkeypatch.setattr(os, "kill", lambda *_: pytest.fail("must not kill a reused PID"))
    with pytest.raises(RuntimeError, match="identity changed"):
        stop_session(1234, "old process")


def test_current_process_has_a_stable_identity(monkeypatch):
    assert identity(os.getpid())
    expected = identity(os.getpid())
    monkeypatch.setenv("COLUMNS", "20")
    assert identity(os.getpid()) == expected
