"""Isolated model/MCP/session probe; never starts ingress or sends Feishu messages."""

from __future__ import annotations

import json
import secrets
import tempfile
import time
from pathlib import Path

from .backends import backend_name, current_python
from .orchestrator import Orchestrator
from .runtime_common import private_json
from .runtime_store import RuntimeStore


def run_probe(source, report=print):
    root = Path(tempfile.mkdtemp(prefix=f"feishu-{backend_name(source)}-probe-"))
    # Copy only execution settings. Never copy credentials, routing, schedules or the live DB.
    keys = {
        "agent_backend", "project_dir", "node_command", "claude_command", "traex_command",
        "model", "model_provider", "session_id", "worker_access", "path", "agent_environment",
        "worker_runner",
    }
    config = {k: v for k, v in source.items() if k in keys}
    config.update({
        "cwd": str(root), "python_command": current_python(),
        "database_path": str(root / "bot.sqlite3"), "state_dir": str(root / "resident"),
        "name": "isolated-feishu-probe", "worker_permission_policy": "auto",
        "integrations": [], "recent_context_enabled": False, "max_retries": 0,
        "task_timeout_seconds": 150, "total_budget_seconds": 150,
        "startup_grace_seconds": 80, "idle_timeout_seconds": 100,
    })
    private_json(root / "config.json", config)
    store = RuntimeStore(root / "bot.sqlite3")
    engine = Orchestrator(config, store)
    marker = "FEISHU_PROBE_" + secrets.token_hex(5)
    report(json.dumps({"probe_directory": str(root), "backend": backend_name(config)}))
    results = []
    try:
        prompts = [
            f"Remember the token {marker}. Call the feishu MCP run tool exactly once with "
            f"operation_key=probe and command='printf {marker}'. "
            "Then return that token as the final answer. Do not call other tools or delegate.",
            "What was the exact FEISHU_PROBE token from the preceding user task? "
            "Reply with just that token. Do not use tools or delegate.",
        ]
        for index, prompt in enumerate(prompts):
            store.accept_event(f"probe-{index}", "local-probe-chat", prompt)
            limit = time.monotonic() + 175
            while time.monotonic() < limit:
                engine.tick()
                task = store._connection.execute(
                    "SELECT t.* FROM runtime_tasks t JOIN events e USING(correlation_id) "
                    "WHERE e.message_id=?", (f"probe-{index}",),
                ).fetchone()
                if task and task["state"] in {"succeeded", "failed", "suspended", "cancelled"}:
                    attempt = store.attempt(task["attempt_id"])
                    result = {
                        "case": "tool" if index == 0 else "fork", "state": task["state"],
                        "reason": attempt["failure_reason"], "session_id": attempt["session_id"],
                        "marker_in_answer": marker in (attempt["answer"] or ""),
                        "operations": [{"kind": o["kind"], "state": o["state"]}
                                       for o in store.operations(task["correlation_id"])],
                    }
                    results.append(result)
                    report(json.dumps(result))
                    break
                time.sleep(0.3)
            else:
                raise RuntimeError("probe deadline exceeded")
            if results[-1]["state"] != "succeeded":
                break
        passed = (
            len(results) == 2 and all(r["marker_in_answer"] for r in results)
            and results[0]["session_id"] != results[1]["session_id"]
            and results[0]["session_id"] != source.get("session_id")
            and results[0]["operations"] == [{"kind": "run", "state": "succeeded"}]
        )
        summary = {"passed": passed, "results": results}
        private_json(root / "summary.json", summary)
        report(json.dumps({"passed": passed, "summary": str(root / "summary.json")}))
        return summary
    finally:
        try:
            engine.recover()
        finally:
            store.close()
