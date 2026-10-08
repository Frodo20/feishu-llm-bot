"""Exercise real typed reads in an isolated database; never starts a worker or sender."""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

from feishu_llm_bot.operation_contracts import DEFAULT_CLI
from feishu_llm_bot.runtime_common import private_json
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_tools import invoke


def probe(query, document_id, cli, executable_path):
    root = Path(tempfile.mkdtemp(prefix="feishu-typed-read-probe-"))
    store = RuntimeStore(root / "bot.sqlite3")
    try:
        store.accept_event("isolated-probe", "local-only", "Read contract verification")
        attempt = store.claim(time.time())
        request = root / "request.json"
        private_json(
            request,
            {
                **attempt,
                "config": {
                    "database_path": str(store.path),
                    "bytedcli_command": str(Path(cli).resolve()),
                    "path": executable_path,
                },
            },
        )
        report = {"isolated_directory": str(root), "operations": []}
        for name, args in (
            ("cli_help", {"topic": "search"}),
            ("search_documents", {"query": query}),
            ("fetch_document", {"document_id": document_id}),
        ):
            result = invoke(request, name, {"operation_key": name, **args})
            report["operations"].append(
                {
                    "kind": name,
                    "state": result["state"],
                    "process_seconds": result.get("process_seconds"),
                    "result_seconds": result.get("result_seconds"),
                    "reason": result.get("reason_code"),
                }
            )
        if all(op["state"] == "succeeded" for op in report["operations"]):
            store.submit_answer(attempt["attempt_id"], attempt["token"], "Read contracts verified")
        store.drain(attempt["attempt_id"])
        report["task_state"] = store.finish(attempt["attempt_id"], now=time.time())
        private_json(root / "report.json", report)
        return report
    finally:
        store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", required=True)
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--cli", default=DEFAULT_CLI)
    parser.add_argument(
        "--path",
        default=os.environ.get("PATH", "/usr/bin:/bin"),
        help="Use the same executable search path as the runtime configuration",
    )
    args = parser.parse_args()
    report = probe(args.query, args.document_id, args.cli, args.path)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["task_state"] == "succeeded" else 1)
