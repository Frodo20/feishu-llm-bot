"""Inspect/reconcile durable operations and configure explicitly requested weekly schedules."""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import sqlite3
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from .libra_contracts import DEFAULT_LIBRA_CLI
from .operation_contracts import DEFAULT_CLI, cli_environment, document_matches
from .runtime_common import private_json
from .runtime_metrics import runtime_metrics
from .runtime_store import RuntimeStore
from .task_tools import verify_document


def register_schedule(store, name, config):
    if not name or len(name) > 100:
        raise ValueError("Schedule name must have 1-100 characters")
    for field, maximum in (("weekday", 6), ("hour", 23), ("minute", 59)):
        value = config.get(field, 0)
        if type(value) is not int or not 0 <= value <= maximum:
            raise ValueError(f"Invalid schedule {field}")
    ZoneInfo(config.get("timezone", "Asia/Shanghai"))
    if not isinstance(config.get("chat_id"), str) or not config["chat_id"]:
        raise ValueError("A destination chat is required")
    if not isinstance(config.get("prompt"), str) or "{week}" not in config["prompt"]:
        raise ValueError("Schedule prompt must select source material for {week}")
    with store.transaction() as db:
        db.execute(
            "INSERT INTO runtime_schedules VALUES (?,?,NULL) "
            "ON CONFLICT(schedule_id) DO UPDATE SET config=excluded.config",
            (name, json.dumps(config, ensure_ascii=False)),
        )
        store._audit(db, None, None, "schedule_configured", name)


def reconcile_document(store, operation_id, config):
    with store._lock:
        row = store._connection.execute(
            "SELECT * FROM runtime_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
    if row is None or row["kind"] != "create_document" or row["state"] != "unknown":
        raise ValueError("Only an uncertain document creation can be reconciled here")
    if store.active():
        raise RuntimeError("Stop/reconcile the active execution before operation verification")
    directory = (
        Path(config["state_dir"]).parent
        / "tasks"
        / row["correlation_id"]
        / row["attempt_id"]
        / "operations"
        / operation_id
    )
    legacy_directory = directory
    if row["operation_attempt_id"]:
        directory /= row["operation_attempt_id"]
    result = json.loads(row["result"] or "{}")
    if not result.get("document") and (directory / "result.json").exists():
        result = json.loads((directory / "result.json").read_text())
    elif not result.get("document") and (legacy_directory / "result.json").exists():
        # Transitional/legacy receipts predate operation-level attempt directories.
        result = json.loads((legacy_directory / "result.json").read_text())
    document = result.get("document", {})
    if not document.get("document_id"):
        raise ValueError("No durable remote ID; do not guess by title or repeat the creation")
    check = directory / ("reconcile-" + str(time.time_ns()))
    check.mkdir(parents=True, mode=0o700)
    verified = verify_document(
        config.get("bytedcli_command", DEFAULT_CLI),
        document["document_id"],
        check,
        cli_environment(config),
    )
    expected = json.loads(row["request"]).get("content")
    if not document_matches(verified, expected):
        raise ValueError("Readback does not match the saved content and revision contract")
    result.update(verified=True, verified_revision_id=verified.get("revision_id"))
    private_json(check / "result.json", result)
    with store.transaction() as db:
        if store.active():
            raise RuntimeError("An execution started during verification; stop it before retrying")
        changed = db.execute(
            "UPDATE runtime_operations SET state='succeeded',result=?,updated_at=? "
            "WHERE operation_id=? AND state='unknown'",
            (json.dumps(result), time.time(), operation_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("Operation state changed during verification")
        store._audit(
            db, row["correlation_id"], row["attempt_id"], "operation_verified", operation_id
        )
        refresh_recovery_warning(db, row["correlation_id"])
    return result


def reconcile_read_help(store, operation_id, config, reason):
    """Audit a strictly recognized legacy help command without executing or rewriting it."""
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
        raise ValueError("A short audit reason is required")
    with store.transaction() as db:
        if store.active():
            raise RuntimeError("Stop/reconcile active execution before verification")
        row = db.execute(
            "SELECT * FROM runtime_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if not row or row["kind"] != "run" or row["state"] != "unknown":
            raise ValueError("Only a legacy uncertain help command can be classified")
        request = json.loads(row["request"])
        allowed = {
            f"{cli} lark docs {topic} --help"
            for cli in {"bytedcli", config.get("bytedcli_command", DEFAULT_CLI)}
            for topic in {"search", "fetch", "create"}
        }
        if request.get("command") not in allowed:
            raise ValueError("Command is not an exact known CLI help invocation")
        # Keep the original timeout result and operation attempt unchanged as evidence.
        db.execute(
            "UPDATE runtime_operations SET state='failed',effect_kind='read',updated_at=? "
            "WHERE operation_id=?",
            (time.time(), operation_id),
        )
        db.execute(
            "UPDATE runtime_tasks SET updated_at=? WHERE correlation_id=?",
            (time.time(), row["correlation_id"]),
        )
        store._audit(
            db,
            row["correlation_id"],
            row["attempt_id"],
            "legacy_help_classified",
            operation_id + ": " + reason,
        )
        refresh_recovery_warning(db, row["correlation_id"])
    return {"operation_id": operation_id, "state": "failed", "effect_kind": "read"}


def refresh_recovery_warning(db, cid):
    if not db.execute(
        "SELECT 1 FROM runtime_operations WHERE correlation_id=? AND state='unknown'", (cid,)
    ).fetchone():
        db.execute(
            "UPDATE runtime_tasks SET warning='操作已核验，可以从保存进度继续',"
            "failure_operation_id=NULL,updated_at=? WHERE correlation_id=?",
            (time.time(), cid),
        )


def reconcile_libra_arguments(store, operation_id, config, reason):
    """Audit the exact historical metric-keys parse failure; never execute its command."""
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
        raise ValueError("A short audit reason confirming the CLI parser/version is required")
    with store.transaction() as db:
        if store.active():
            raise RuntimeError("Stop/reconcile active execution before verification")
        row = db.execute(
            "SELECT * FROM runtime_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if not row or row["kind"] != "run" or row["state"] != "unknown":
            raise ValueError("Only an uncertain legacy run can be audited")
        request = json.loads(row["request"])
        command = request.get("command", "")
        if not isinstance(command, str) or any(c in command for c in ";&|<>`$\n\r"):
            raise ValueError("Compound commands cannot be classified as a parser failure")
        args = shlex.split(command)
        if (
            len(args) != 10
            or args[0] not in {"libra-cli", config.get("libra_cli_command", DEFAULT_LIBRA_CLI)}
            or args[1:4] != ["--json", "metrics", "search"]
        ):
            raise ValueError("Not an exact supported Libra metrics search invocation")
        fields = dict(zip(args[4::2], args[5::2], strict=True))
        if set(fields) != {"--experiment-id", "--metric-keys", "--top"} or not (
            fields["--experiment-id"].isdigit()
            and int(fields["--experiment-id"]) > 0
            and fields["--top"].isdigit()
        ):
            raise ValueError("Unexpected historical CLI options")
        try:
            json.loads(fields["--metric-keys"])
        except ValueError:
            pass
        else:
            raise ValueError("metric-keys was valid JSON; this audit does not apply")
        result = json.loads(row["result"] or "{}")
        if (
            result.get("exit_code") != 2
            or result.get("timed_out")
            or result.get("background_children")
        ):
            raise ValueError("Receipt is not a clean parser exit")
        root = (
            Path(config["state_dir"]).parent
            / "tasks"
            / row["correlation_id"]
            / row["attempt_id"]
            / "operations"
            / operation_id
        ).resolve()
        error = Path(result.get("stderr_path", "")).resolve(strict=True)
        if not error.is_relative_to(root) or error.name != "stderr.txt":
            raise ValueError("Parser error artifact is outside the operation")
        raw = error.read_bytes()
        if raw.strip() != b"Error: Invalid value: --metric-keys must be valid JSON":
            raise ValueError("Saved stderr does not match the supported parser error")
        # Preserve original request, failure result, and operation attempt as evidence.
        db.execute(
            "UPDATE runtime_operations SET state='failed',effect_kind='read',updated_at=? "
            "WHERE operation_id=?",
            (time.time(), operation_id),
        )
        store._audit(
            db,
            row["correlation_id"],
            row["attempt_id"],
            "libra_parser_failure_verified",
            json.dumps(
                {
                    "operation_id": operation_id,
                    "reason": reason,
                    "stderr_sha256": hashlib.sha256(raw).hexdigest(),
                }
            ),
        )
        refresh_recovery_warning(db, row["correlation_id"])
    return {"operation_id": operation_id, "state": "failed", "effect_kind": "read"}


def schema_status(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        table = db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_meta'").fetchone()
        row = (
            db.execute(
                "SELECT value FROM runtime_meta WHERE key='runtime_schema_version'"
            ).fetchone()
            if table
            else None
        )
        active = (
            db.execute(
                "SELECT count(*) FROM runtime_attempts "
                "WHERE state IN ('starting','running','draining')"
            ).fetchone()[0]
            if table
            else 0
        )
        return {
            "version": json.loads(row[0]) if row else 1,
            "active_executions": active,
            "target_version": 2,
        }
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("operations")
    sub.add_parser("schema-check")
    sub.add_parser("migrate-schema")
    sub.add_parser("metrics")
    category = sub.add_parser("classify-task")
    category.add_argument("task_number", type=int)
    category.add_argument(
        "category",
        choices=[
            "business",
            "experiment_analysis",
            "control",
            "greeting",
            "verification",
            "recovery",
            "unclassified",
        ],
    )
    verify = sub.add_parser("verify-document")
    verify.add_argument("operation_id")
    help_check = sub.add_parser("verify-read-help")
    help_check.add_argument("operation_id")
    help_check.add_argument("--reason", required=True)
    libra_check = sub.add_parser("verify-libra-arguments")
    libra_check.add_argument("operation_id")
    libra_check.add_argument("--reason", required=True)
    schedule = sub.add_parser("schedule")
    schedule.add_argument("name")
    schedule.add_argument("definition", type=Path)
    sub.add_parser("schedules")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if args.command == "schema-check":
        print(json.dumps(schema_status(config["database_path"])))
        return
    store = RuntimeStore(Path(config["database_path"]))
    try:
        if args.command == "metrics":
            print(json.dumps(runtime_metrics(store._connection), ensure_ascii=False, indent=2))
        elif args.command == "classify-task":
            with store.transaction() as db:
                row = db.execute(
                    "SELECT correlation_id FROM events WHERE sequence=?", (args.task_number,)
                ).fetchone()
                if not row:
                    raise ValueError("Unknown task number")
                db.execute(
                    "UPDATE runtime_tasks SET task_category=? WHERE correlation_id=?",
                    (args.category, row[0]),
                )
                store._audit(db, row[0], None, "task_classified", args.category)
            print("Task category saved")
        elif args.command == "migrate-schema":
            print(json.dumps(schema_status(config["database_path"])))
        elif args.command == "verify-read-help":
            print(json.dumps(reconcile_read_help(store, args.operation_id, config, args.reason)))
        elif args.command == "verify-libra-arguments":
            print(
                json.dumps(reconcile_libra_arguments(store, args.operation_id, config, args.reason))
            )
        elif args.command == "verify-document":
            print(
                json.dumps(reconcile_document(store, args.operation_id, config), ensure_ascii=False)
            )
        elif args.command == "schedule":
            register_schedule(store, args.name, json.loads(args.definition.read_text()))
            print("Schedule saved; one task per configured week, with durable input deduplication.")
        elif args.command == "schedules":
            print(
                json.dumps(
                    [dict(r) for r in store._connection.execute("SELECT * FROM runtime_schedules")],
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(
                json.dumps(
                    [
                        dict(r)
                        for r in store._connection.execute(
                            "SELECT operation_id,correlation_id,operation_key,kind,state,result "
                            "FROM runtime_operations ORDER BY updated_at DESC LIMIT 100"
                        )
                    ],
                    ensure_ascii=False,
                    indent=2,
                )
            )
    finally:
        store.close()


if __name__ == "__main__":
    main()
