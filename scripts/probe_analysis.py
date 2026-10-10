#!/usr/bin/env python3
"""Real model + synthetic Libra evidence. No external data query or Feishu delivery."""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from feishu_llm_bot.orchestrator import Orchestrator
from feishu_llm_bot.runtime_common import private_json
from feishu_llm_bot.runtime_store import RuntimeStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["claude", "traex"], required=True)
    parser.add_argument("--agent-command", required=True)
    parser.add_argument("--model", required=True)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    root = Path(tempfile.mkdtemp(prefix="feishu-analysis-probe-"))
    cli = root / "libra"
    cli.write_text(f"#!{sys.executable}\nimport json\n" +
                   "print(json.dumps({'status':'success','data':{'code':200,'data':"
                   "{'has_stats':0,'has_full_stats':False,'merge_data':"
                   "{'20':{'11':{'value':None}}}}}}))\n")
    cli.chmod(0o700)
    config = {
        "agent_backend": args.backend, "model": args.model, "project_dir": str(project),
        "claude_command": args.agent_command, "traex_command": args.agent_command,
        "node_command": shutil.which("node"), "python_command": sys.executable,
        "cwd": str(root), "database_path": str(root / "bot.sqlite3"),
        "state_dir": str(root / "resident"), "name": "analysis-contract-probe",
        "worker_runner": "process", "worker_permission_policy": "auto",
        "integrations": ["libra"], "libra_cli_command": str(cli), "path": os.environ["PATH"],
        "task_timeout_seconds": 150, "idle_timeout_seconds": 100, "max_retries": 0,
        "recent_context_enabled": False,
    }
    store = RuntimeStore(root / "bot.sqlite3")
    engine = Orchestrator(config, store)
    prompt = (
        "This is an isolated completion-contract test using synthetic data. "
        "Call libra_read exactly once with action=report_data, operation_key=synthetic, "
        'arguments={"experiment_id":1,"metric_group":2,"base_vid":10,'
        '"selected_metric_ids":[20],"period_type":"d",'
        '"start_date":"2026-10-01","end_date":"2026-10-02"}. '
        "Then call checkpoint with text='SYNTHETIC_STAGE: no statistics available' and "
        "evidence_gaps=['ROI statistics missing']. Finally call reply with the current "
        "correlation_id, text='SYNTHETIC_FINAL: data missing; no verification possible', "
        "business_outcome=partial, completion_scope=verified and the same evidence_gaps. "
        "Do not run other queries or tools. End your turn."
    )
    print(json.dumps({"probe_directory": str(root), "backend": args.backend}), flush=True)
    try:
        store.accept_event("synthetic", "local-only", prompt)
        limit = time.monotonic() + 180
        while time.monotonic() < limit:
            engine.tick()
            row = store._connection.execute("SELECT * FROM runtime_tasks").fetchone()
            if row and row["state"] in {"succeeded", "failed", "suspended", "cancelled"}:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError("Probe supervision deadline exceeded")
        cid, aid = row["correlation_id"], row["attempt_id"]
        ops = store.operations(cid)
        event = store.get_by_correlation(cid)
        passed = (row["business_outcome"] == "partial" and len(ops) == 1
                  and ops[0]["state"] == "succeeded"
                  and json.loads(ops[0]["result"])["statistics_status"] == "missing"
                  and store.meta("checkpoint:" + aid) is not None
                  and "SYNTHETIC_FINAL" in event.reply_text and "证据缺口" in event.reply_text)
        result = {"passed": passed, "state": row["state"], "outcome": row["business_outcome"],
                  "checkpoint_saved": store.meta("checkpoint:" + aid) is not None}
        private_json(root / "summary.json", result)
        print(json.dumps(result), flush=True)
        return 0 if passed else 1
    finally:
        engine.recover()
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
