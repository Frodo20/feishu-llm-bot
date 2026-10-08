#!/usr/bin/env python3
"""Run a synthetic image or permission task; never connects to Feishu."""

from __future__ import annotations

import argparse
import io
import json
import re
import tempfile
import time
from pathlib import Path

from PIL import Image

from feishu_llm_bot.attachments import AttachmentStore
from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.orchestrator import Orchestrator
from feishu_llm_bot.permission_relay import PermissionRelay
from feishu_llm_bot.runtime_common import private_json
from feishu_llm_bot.runtime_store import RuntimeStore


class LocalApproval:
    """Only acknowledges the synthetic run-tool request over a local test socket."""

    def __init__(self):
        self.requests = []

    def send_text(self, chat_id, text, _uuid):
        assert chat_id == "probe-chat" and "mcp__feishu__run" in text
        token = re.search(r"^同意 ([A-Za-z0-9_-]+)$", text, re.MULTILINE).group(1)
        self.requests.append(token)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--case", choices=["image", "permission"], required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    root = Path(tempfile.mkdtemp(prefix="feishu-worker-probe-"))
    config.update(
        project_dir=str(Path(__file__).resolve().parents[1]),
        database_path=str(root / "bot.sqlite3"),
        state_dir=str(root / "resident"),
        bridge_env_file=str(root / "env"),
        progress_state_dir=str(root / "progress"),
        permission_socket=str(root / "permission" / "relay.sock"),
        session_id=None,
        runtime_enabled=True,
        name="feishu-runtime-probe",
        cwd=str(root),
        max_retries=0,
        task_timeout_seconds=120,
        total_budget_seconds=120,
        idle_timeout_seconds=90,
    )
    private_json(root / "credentials.json", {"app_id": "probe", "app_secret": "unused"})
    (root / "env").write_text(
        f"FEISHU_BOT_CREDENTIALS_FILE={root}/credentials.json\n"
        f"FEISHU_BOT_DB_PATH={root}/bot.sqlite3\nFEISHU_ALLOWED_SENDER_OPEN_ID=probe\n"
    )
    store = RuntimeStore(root / "bot.sqlite3")
    approvals = LocalApproval()
    relay = PermissionRelay(
        store=store,
        replies=approvals,
        socket_path=Path(config["permission_socket"]),
        session_id="probe",
        chat_id="probe-chat",
        timeout_seconds=60,
        max_pending=1,
        all_sessions=True,
        worker_only=True,
    )
    engine = Orchestrator(config, store)
    if args.case == "image":
        event = store.reserve_image(
            "probe-image", "probe-chat", "Read this image and describe its color in one sentence."
        )
        image = io.BytesIO()
        Image.new("RGB", (64, 64), (255, 0, 0)).save(image, format="PNG")
        attachments = AttachmentStore(
            root / "attachments",
            max_image_bytes=1024 * 1024,
            max_image_pixels=10000,
            max_image_side=100,
            max_total_bytes=2 * 1024 * 1024,
        )
        meta = attachments.save(event.attachment_token, image.getvalue())
        store.finish_image_acquisition(
            event.correlation_id,
            mime_type=meta.mime_type,
            byte_size=meta.byte_size,
            sha256=meta.sha256,
            width=meta.width,
            height=meta.height,
        )
    else:
        store.accept_event(
            "probe-permission",
            "probe-chat",
            "Call mcp__feishu__run exactly once with operation_key=probe and "
            "command=/usr/bin/printf RUNTIME_PERMISSION_OK. "
            "Do not use Bash or any other tools. Report the returned marker.",
        )
    relay.start()
    try:
        deadline = time.monotonic() + 140
        while time.monotonic() < deadline:
            engine.tick()
            if approvals.requests:
                token = approvals.requests.pop(0)
                relay.handle_control(
                    IncomingMessage.text("local-approval", "probe-chat", "同意 " + token)
                )
            tasks = store._connection.execute("SELECT * FROM runtime_tasks").fetchall()
            if tasks and tasks[0]["state"] in {"succeeded", "failed", "suspended"}:
                task = dict(tasks[0])
                attempt = store.attempt(task["attempt_id"])
                event = store.get_by_correlation(task["correlation_id"])
                report = {
                    "case": args.case,
                    "state": task["state"],
                    "answer": event.reply_text,
                    "image_read": bool(attempt["image_read"]),
                    "directory": str(root),
                    "permission_requests": store._connection.execute(
                        "SELECT count(*) FROM permission_requests"
                    ).fetchone()[0],
                    "managed_run_succeeded": any(
                        op["kind"] == "run" and op["state"] == "succeeded"
                        for op in store.operations(task["correlation_id"])
                    ),
                }
                private_json(root / "report.json", report)
                print(json.dumps(report, ensure_ascii=False), flush=True)
                if task["state"] != "succeeded":
                    raise RuntimeError("Synthetic worker probe failed")
                if args.case == "image" and not attempt["image_read"]:
                    raise RuntimeError("Image was not read")
                if args.case == "permission" and (
                    report["permission_requests"] != 0
                    or not report["managed_run_succeeded"]
                    or "RUNTIME_PERMISSION_OK" not in (event.reply_text or "")
                ):
                    raise RuntimeError(
                        "Expected managed command success without an approval request"
                    )
                return
            time.sleep(0.5)
        raise TimeoutError("Synthetic probe exceeded its deadline")
    finally:
        engine.recover()
        relay.stop()
        store.close()


if __name__ == "__main__":
    main()
