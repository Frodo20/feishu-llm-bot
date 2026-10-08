#!/usr/bin/env python3
"""Exercise the real permission hook/card roundtrip without executing a tool."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    config = json.loads((project / "deploy/resident.json").read_text())
    environment = dict(os.environ)
    environment.update({
        "FEISHU_PERMISSION_ALL_SESSIONS": "true",
        "FEISHU_PERMISSION_SOCKET_PATH": config["permission_socket"],
        "FEISHU_PERMISSION_TIMEOUT_SECONDS": "610",
    })
    payload = {
        "session_id": config["session_id"], "cwd": config["cwd"],
        "hook_event_name": "PermissionRequest", "tool_name": "ApprovalCardTest",
        "tool_input": {"path": "测试卡片：任一按钮均可；只验证回传，不执行任何操作"},
    }
    print("Sending a no-op approval test through the real Claude permission hook", flush=True)
    result = subprocess.run(
        [sys.executable, str(project / "scripts/claude_permission_hook.py")],
        input=json.dumps(payload), text=True, env=environment, capture_output=True,
        timeout=620, check=True,
    )
    response = json.loads(result.stdout)
    decision = response["hookSpecificOutput"]["decision"]["behavior"]
    print(json.dumps({"probe": "permission_card", "hook_decision": decision}, ensure_ascii=False))
    output = Path(config["state_dir"]) / "card-probe-result.json"
    output.write_text(json.dumps({"hook_decision": decision}) + "\n")
    output.chmod(0o600)


if __name__ == "__main__":
    main()
