"""Offline, backed-up migration from the interactive bridge to supervised tasks."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
from contextlib import closing
from pathlib import Path

from .runtime_common import private_json
from .runtime_store import RuntimeStore
from .task_tools import verify_document

SERVICES = (
    "feishu-claude",
    "feishu-llm-bot",
    "feishu-progress",
    "feishu-gateway",
    "feishu-orchestrator",
    "feishu-sender",
)


def require_stopped(config):
    """No implicit stop/restart: never race another receiver or an external write."""
    result = subprocess.run(
        [
            "systemctl",
            "--user",
            "list-units",
            "--all",
            "--plain",
            "--no-legend",
            "feishu-worker-*.service",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    workers = [line.split()[0] for line in result.stdout.splitlines() if line.strip()]
    for unit in [s + ".service" for s in SERVICES] + workers:
        result = subprocess.run(
            ["systemctl", "--user", "show", unit, "-p", "ActiveState", "--value"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.stdout.strip() not in {"inactive", "failed", ""}:
            raise RuntimeError(f"Stop {unit} and its children before migration")
    from .resident import ensure_no_receiver

    ensure_no_receiver(Path(config["permission_socket"]))


def backup_database(source, destination):
    with (
        closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as src,
        closing(sqlite3.connect(destination)) as dest,
    ):
        src.backup(dest)
    destination.chmod(0o600)


def migrate(config, destination, *, verified=None, now=None):
    """Caller must stop services first; tests call this on isolated databases only."""
    now = time.time() if now is None else now
    source = Path(config["database_path"])
    destination = Path(destination)
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    backup_database(source, destination / "bot.sqlite3")
    progress = Path(config["progress_state_dir"]) / "progress.sqlite3"
    if progress.exists() and progress.resolve() != source.resolve():
        backup_database(progress, destination / "progress.sqlite3")
    private_json(destination / "resident-config.json", config)
    store = RuntimeStore(source)
    try:
        if store.meta("runtime_migrated_at"):
            raise RuntimeError("Database is already migrated; do not replay the migration")
        if store.active():
            raise RuntimeError("Reconcile active worker attempts before migration")
        with store._lock:
            pending = [
                dict(r)
                for r in store._connection.execute(
                    "SELECT sequence,correlation_id,message_id,chat_id,status,reply_format "
                    "FROM events WHERE status NOT IN ('replied','failed') ORDER BY sequence"
                ).fetchall()
            ]
        if any(r["status"] == "replying" and r["reply_format"] != "card_v1" for r in pending):
            raise RuntimeError("A partial legacy reply needs review before migration")
        # Carry over card IDs, paging tokens and cursors; all future state uses one WAL database.
        if (destination / "progress.sqlite3").exists():
            with (
                closing(sqlite3.connect(destination / "progress.sqlite3")) as old,
                store.transaction() as db,
            ):
                for table in ("meta", "tasks", "page_requests"):
                    schema = old.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
                    ).fetchone()
                    if schema is None:
                        continue
                    exists = db.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                    ).fetchone()
                    if not exists:
                        db.execute(schema[0])
                    for row in old.execute(f"SELECT * FROM {table}"):
                        placeholders = ",".join("?" for _ in row)
                        db.execute(f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", row)
        store.discover(now)
        if verified:
            cid = verified["correlation_id"]
            if re.fullmatch(r"fs_[0-9a-f]{32}", cid) is None:
                raise ValueError("Invalid incident task ID")
            task_dir = Path(config["state_dir"]).parent / "tasks" / cid / "imported"
            task_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
            for key in ("receipt_path", "draft_path"):
                if verified.get(key):
                    artifact = Path(verified[key])
                    target = task_dir / artifact.name
                    shutil.copyfile(artifact, target)
                    target.chmod(0o600)
            private_json(task_dir / "verification.json", verified)
            store.import_result(cid, verified["answer"], now)
        for item in pending:
            cid = item["correlation_id"]
            if verified and cid == verified["correlation_id"]:
                continue
            if item["status"] == "replying":
                # Preserve an already persisted answer without invoking the model again.
                event = store.get_by_correlation(cid)
                with store.transaction() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO runtime_tasks "
                        "(correlation_id,state,created_at,updated_at) VALUES (?,'succeeded',?,?)",
                        (cid, now, now),
                    )
                    if not db.execute(
                        "SELECT 1 FROM runtime_outbox WHERE correlation_id=?", (cid,)
                    ).fetchone():
                        store._save_result(db, cid, event.reply_text, event.failure_reason, now)
            elif item["status"] != "accepted":
                with store.transaction() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO runtime_tasks "
                        "(correlation_id,state,created_at,updated_at) VALUES (?,'suspended',?,?)",
                        (cid, now, now),
                    )
                    answer = (
                        f"任务 #{item['sequence']} 在旧执行器中中断，已保存记录并挂起。"
                        "请先核查已有成果，再使用 /continue 任务编号继续。"
                    )
                    store._save_result(db, cid, answer, "migration_interrupted", now)
        # Existing queued control requests bypass model execution.
        from .feishu import IncomingMessage

        for item in pending:
            event = store.get_by_correlation(item["correlation_id"])
            if event.status == "accepted" and event.message_type == "text":
                store.handle_control(
                    IncomingMessage.text(
                        event.message_id,
                        event.chat_id,
                        event.user_text,
                    ),
                    existing=True,
                )
        store.set_meta("session_id", config["session_id"])
        store.set_meta("runtime_migrated_at", now)
        private_json(destination / "migration.json", {"created_at": now, "input_tasks": pending})
    finally:
        store.close()
    runtime = {
        **config,
        "runtime_enabled": True,
        "task_timeout_seconds": 1200,
        "total_budget_seconds": 1800,
        "idle_timeout_seconds": 300,
        "max_retries": 2,
        "permission_mode": "default",
    }
    private_json(destination / "runtime.json", runtime)
    return destination / "runtime.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument(
        "--incident", type=Path, help="Manifest of the existing document and artifacts"
    )
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    require_stopped(config)
    verified = None
    if args.incident:
        verified = json.loads(args.incident.read_text())
        cli = config.get("bytedcli_command", "bytedcli")
        check_dir = args.backup_dir.with_name(args.backup_dir.name + "-verification")
        check_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
        env = {**os.environ, "PATH": config["path"]}
        document = verify_document(cli, verified["document_id"], check_dir, env)
        verified["verified_document"] = document
        verified["verified_at"] = time.time()
        private_json(check_dir / "verification.json", verified)
    print(migrate(config, args.backup_dir, verified=verified))


if __name__ == "__main__":
    main()
