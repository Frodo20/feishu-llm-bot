#!/usr/bin/env python3
"""Real isolated backend/tool/fork probe. No Feishu connection or production state."""

import argparse
import os
import shutil
from pathlib import Path

from feishu_llm_bot.backend_probe import run_probe


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["claude", "traex"], required=True)
    parser.add_argument("--agent-command", required=True)
    parser.add_argument("--model")
    parser.add_argument("--model-provider")
    parser.add_argument("--session", help="Fork a seed session without modifying it")
    parser.add_argument("--node-command", default=shutil.which("node"))
    parser.add_argument("--path", default=os.environ["PATH"])
    parser.add_argument("--worker-access", choices=["workspace", "full"], default="workspace")
    parser.add_argument("--runner", choices=["systemd", "process"], default="process")
    args = parser.parse_args()
    config = {
        "agent_backend": args.backend, "project_dir": str(Path(__file__).resolve().parents[1]),
        "node_command": args.node_command, "model": args.model,
        "model_provider": args.model_provider, "session_id": args.session,
        "worker_runner": args.runner, "worker_access": args.worker_access,
        "path": args.path, f"{args.backend}_command": args.agent_command,
    }
    result = run_probe(config, report=lambda text: print(text, flush=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
