#!/usr/bin/env python3
"""Isolated real Libra/Claude acceptance. No gateway, sender or approval relay."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from feishu_llm_bot.orchestrator import Orchestrator
from feishu_llm_bot.runtime_common import private_json
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_budget import deadlines
from feishu_llm_bot.task_tools import invoke


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--experiment-id", required=True, type=int)
    parser.add_argument("--mode", choices=["reads", "worker"], required=True)
    parser.add_argument("--request-file", type=Path)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    root = Path(tempfile.mkdtemp(prefix="feishu-libra-probe-"))
    config.update(
        project_dir=str(Path(__file__).resolve().parents[1]),
        cwd=str(root),
        database_path=str(root / "bot.sqlite3"),
        state_dir=str(root / "resident"),
        permission_socket=str(root / "no-relay.sock"),
        session_id=None,
        runtime_enabled=True,
        name="feishu-libra-acceptance",
        max_retries=0,
        task_timeout_seconds=args.timeout,
        total_budget_seconds=args.timeout,
        worker_permission_policy="auto",
        idle_timeout_seconds=300,
    )
    store = RuntimeStore(root / "bot.sqlite3")
    prompt = (
        args.request_file.read_text()
        if args.request_file
        else f"使用本地 Libra 工具读取实验 {args.experiment_id} 的实验信息，查找 StayDuration/U "
        "和推荐页1min+时长占比，并获取实际报表数据后给出有证据的简要分析。"
    )
    store.accept_event("probe", "local-only", prompt)
    cid = store._connection.execute(
        "SELECT correlation_id FROM events WHERE message_id='probe'"
    ).fetchone()[0]
    event = store.get_by_correlation(cid)
    engine = None
    print(json.dumps({"directory": str(root), "mode": args.mode}), flush=True)
    try:
        if args.mode == "reads":
            attempt = store.claim(time.time())
            store.started(attempt["attempt_id"])
            directory = root / "tasks" / attempt["correlation_id"] / attempt["attempt_id"]
            request = directory / "request.json"
            private_json(request, {**attempt, "config": config, **deadlines(args.timeout)})
            cases = [
                {"action": "experiment_get", "arguments": {"experiment_id": args.experiment_id}},
                {
                    "action": "metric_search",
                    "arguments": {
                        "experiment_id": args.experiment_id,
                        "metric_keys": ["StayDuration/U", "推荐页1min+时长占比", "广告"],
                        "top": 5,
                    },
                },
            ]
            for index, case in enumerate(cases):
                result = invoke(request, "libra_read", {"operation_key": f"read-{index}", **case})
                print(
                    json.dumps(
                        {
                            "action": case["action"],
                            "state": result["state"],
                            "reason": result.get("reason_code"),
                            "seconds": result.get("process_seconds"),
                        }
                    ),
                    flush=True,
                )
                if result["state"] != "succeeded":
                    raise RuntimeError("Real Libra read did not pass its contract")
            store.submit_answer(attempt["attempt_id"], attempt["token"], "真实读取合同验收完成")
            store.drain(attempt["attempt_id"])
            store.finish(attempt["attempt_id"], now=time.time())
        else:
            engine = Orchestrator(config, store)
            until = time.monotonic() + args.timeout + 30
            last = None
            while time.monotonic() < until:
                engine.tick()
                task = store.task(event.correlation_id)
                if task:
                    ops = store.operations(event.correlation_id)
                    status = (task["state"], len(ops), sum(o["state"] == "succeeded" for o in ops))
                    if status != last:
                        print(
                            json.dumps(
                                {
                                    "state": status[0],
                                    "operations": status[1],
                                    "successful_operations": status[2],
                                }
                            ),
                            flush=True,
                        )
                        last = status
                    if task["state"] in {"succeeded", "failed", "suspended", "cancelled"}:
                        break
                time.sleep(0.5)
            else:
                raise TimeoutError("Worker probe exceeded its deadline")
        task = store.task(event.correlation_id)
        approvals = store._connection.execute(
            "SELECT count(*) FROM permission_requests"
        ).fetchone()[0]
        report = {
            "mode": args.mode,
            "directory": str(root),
            "state": task["state"],
            "business_outcome": task["business_outcome"],
            "permission_requests": approvals,
            "operations": [
                {
                    "kind": op["kind"],
                    "state": op["state"],
                    "reason": json.loads(op["result"] or "{}").get("reason_code"),
                }
                for op in store.operations(event.correlation_id)
            ],
        }
        # Private answer remains with the isolated task; only status leaves this script.
        private_json(root / "report.json", report)
        print(json.dumps(report), flush=True)
        if task["state"] != "succeeded" or approvals:
            raise RuntimeError("Expected a successful task without any human approval")
    finally:
        if engine:
            engine.recover()
        store.close()


if __name__ == "__main__":
    main()
