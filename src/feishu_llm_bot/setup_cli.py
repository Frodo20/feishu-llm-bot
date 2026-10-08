"""Initialize a new private bot instance on the current machine."""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import uuid
from pathlib import Path

from .backends import (
    backend_name,
    current_python,
    enabled_integrations,
    executable,
    python_command,
    validate_runtime_config,
)
from .config import Settings, load_credentials
from .runtime_common import load_config, private_json


def systemd_available():
    if platform.system() != "Linux" or not shutil.which("systemctl"):
        return False
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show-environment"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def initialize(args):
    if platform.system() not in {"Linux", "Darwin"}:
        raise ValueError("Native Windows is not supported; use Linux/WSL2")
    project = Path(__file__).resolve().parents[2]
    root = args.state_dir.expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("State directory is not empty; choose a new instance directory")
    cwd = args.cwd.expanduser().resolve(strict=True)
    if not cwd.is_dir():
        raise ValueError("Workspace must be a directory")
    if not re.fullmatch(r"ou_[A-Za-z0-9_-]+", args.owner):
        raise ValueError("--owner must be this bot application's user open_id (ou_...)")
    session = str(uuid.UUID(args.session)) if args.session else None
    runner = args.runner
    if runner == "auto":
        runner = "systemd" if systemd_available() else "process"
    config = {
        "runtime_enabled": True,
        "agent_backend": args.backend,
        "session_id": session,
        "name": args.name,
        "model": args.model,
        "model_provider": args.model_provider,
        "cwd": str(cwd),
        "project_dir": str(project),
        "python_command": current_python(),
        "state_dir": str(root / "resident"),
        "database_path": str(root / "bot.sqlite3"),
        "bridge_env_file": str(root / "env"),
        "progress_state_dir": str(root / "progress"),
        "worker_runner": runner,
        "worker_permission_policy": "auto",
        "worker_access": args.worker_access,
        "permission_mode": "default",
        "path": getattr(args, "path", None) or os.environ.get("PATH", "/usr/bin:/bin"),
        "agent_environment": {
            k: str(Path(os.environ[k]).expanduser().absolute())
            for k in ("TRAE_HOME", "TRAECLI_HOME", "CLAUDE_CONFIG_DIR") if os.environ.get(k)
        },
        "integrations": args.integration,
        "task_timeout_seconds": 1200,
        "total_budget_seconds": 1800,
        "startup_grace_seconds": 180,
        "idle_timeout_seconds": 300,
        "max_retries": 2,
        "finalization_reserve_seconds": 90,
        "release_id": "portable-20261008",
    }
    key = "claude_command" if args.backend == "claude" else "traex_command"
    if args.agent_command:
        config[key] = args.agent_command
    config[key] = executable(
        config, key, *(("claude", "claude-w") if args.backend == "claude" else ("traex", "traecli"))
    )
    config["node_command"] = executable(
        {"node_command": getattr(args, "node_command", None), "path": config["path"]},
        "node_command", "node"
    )
    validate_runtime_config(config)
    if args.credentials_file:
        credentials = load_credentials(args.credentials_file.expanduser())
    else:
        app_id = args.app_id or input("Feishu App ID: ").strip()
        secret = os.environ.get("FEISHU_APP_SECRET") or getpass.getpass("Feishu App Secret: ")
        if not app_id or not secret:
            raise ValueError("Feishu App ID and App Secret are required")
        credentials = {"app_id": app_id, "app_secret": secret}
    for key, value in credentials.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Credential field {key} is empty")
    if any("\n" in str(v) or "\r" in str(v) for v in (root, args.owner)):
        raise ValueError("Paths and owner may not contain newlines")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    private_json(root / "credentials.json", credentials)
    private_json(root / "runtime.json", config)
    with open(root / "env", "x", opener=lambda p, f: os.open(p, f, 0o600)) as stream:
        stream.write(
            f"FEISHU_BOT_CREDENTIALS_FILE={root / 'credentials.json'}\n"
            f"FEISHU_ALLOWED_SENDER_OPEN_ID={args.owner}\n"
            f"FEISHU_BOT_DB_PATH={root / 'bot.sqlite3'}\n"
            "FEISHU_PERMISSION_RELAY_ENABLED=false\n"
        )
    return root / "runtime.json"


def diagnose(config_path):
    config, env = load_config(config_path)
    checks = []

    def add(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    name = backend_name(config)
    from .backends import clean_environment
    from .operation_contracts import cli_environment

    command_env = clean_environment(cli_environment(config))
    Settings.from_env(env)
    add("credentials", True, "Private credentials and owner configuration validated")
    add("workspace", Path(config["cwd"]).is_dir(), config["cwd"])
    add("platform", platform.system() in {"Linux", "Darwin"}, platform.system())
    key = "claude_command" if name == "claude" else "traex_command"
    candidates = ("claude", "claude-w") if name == "claude" else ("traex", "traecli")
    for field, choices in [
        (key, candidates),
        ("node_command", ("node",)),
        ("python_command", ("python3",)),
    ]:
        try:
            command = executable(config, field, *choices)
            result = subprocess.run(
                [command, *(["--model", config["model"]] if name == "claude"
                           and field == key and config.get("model") else []), "--version"],
                capture_output=True, timeout=10, env=command_env,
            )
            add(
                field,
                result.returncode == 0,
                "Executable responds"
                if result.returncode == 0
                else "Executable failed; check login/wrapper environment",
            )
        except (OSError, ValueError, subprocess.SubprocessError):
            add(field, False, "Executable missing or timed out")
    try:
        command = executable(config, key, *candidates)
        args = ["app-server", "--help"] if name == "traex" else ["--help"]
        if name == "claude" and config.get("model"):
            args = ["--model", config["model"], *args]
        result = subprocess.run(
            [command, *args], capture_output=True, text=True, timeout=10, env=command_env,
        )
        flags = (
            ["--listen"] if name == "traex" else ["--fork-session", "--output-format", "--settings"]
        )
        add(
            name + "_protocol",
            result.returncode == 0 and all(f in result.stdout for f in flags),
            "CLI surface available; isolated probe verifies model, MCP and session forking",
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        add(name + "_protocol", False, "Backend unavailable or protocol help failed")
    project = Path(config["project_dir"])
    add(
        "mcp_dependencies",
        (project / "node-channel/node_modules/@modelcontextprotocol/sdk").is_dir(),
        "Install with npm ci --prefix node-channel",
    )
    runner = config.get("worker_runner", "systemd")
    add(
        "worker_runner",
        runner in {"process", "systemd"} and (runner == "process" or systemd_available()),
        runner,
    )
    for integration in enabled_integrations(config):
        command = (
            config.get("bytedcli_command", "bytedcli")
            if integration == "documents"
            else config.get("libra_cli_command", "libra-cli")
        )
        add(
            integration,
            bool(shutil.which(command, path=config.get("path"))),
            "Optional integration executable; its own login must also be valid",
        )
    database = Path(config["database_path"])
    if database.exists():
        try:
            with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
                db.execute("PRAGMA query_only=ON")
                row = db.execute(
                    "SELECT value FROM runtime_meta WHERE key='agent_backend'"
                ).fetchone()
                legacy = db.execute(
                    "SELECT value FROM runtime_meta WHERE key='session_id'"
                ).fetchone()
                bound = json.loads(row[0]) if row else "claude" if legacy else name
                add("backend_binding", bound == name, bound)
        except sqlite3.Error:
            add("database", False, "Runtime schema unavailable; use a new instance or migrate it")
    add(
        "authentication_scope",
        True,
        "No model/Feishu request sent. CLI login and selected-session access require the probe.",
    )
    return checks


def service_files(config_path, output):
    """Render only; installing/starting services is an explicit subsequent action."""
    config, _ = load_config(config_path)
    path = config_path.expanduser().resolve()
    digest = hashlib.sha256(str(path).encode()).hexdigest()[:12]
    name = f"feishu-bot-{digest}"
    output.mkdir(parents=True, exist_ok=True)
    command = [
        python_command(config),
        "-m",
        "feishu_llm_bot.setup_cli",
        "run",
        "--config",
        str(path),
    ]
    if platform.system() == "Darwin":
        import plistlib

        target = output / (name + ".plist")
        data = {
            "Label": name,
            "ProgramArguments": command,
            "RunAtLoad": True,
            "KeepAlive": True,
            "Umask": 0o077,
            "WorkingDirectory": config["project_dir"],
            "EnvironmentVariables": {
                "PYTHONPATH": str(Path(config["project_dir"]) / "src"),
                "PATH": config["path"],
            },
            "StandardOutPath": str(path.parent / "service.log"),
            "StandardErrorPath": str(path.parent / "service.log"),
        }
        target.write_bytes(plistlib.dumps(data))
    else:

        def quote(value, *, command=False):
            # systemd has its own escaping; shell shlex.quote is not sufficient.
            value = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
            if command:
                value = value.replace("$", "$$")
            return '"' + value + '"'

        target = output / (name + ".service")
        target.write_text(
            "[Unit]\nDescription=Private Feishu bot\nAfter=network-online.target\n\n[Service]\n"
            "Type=simple\nUMask=0077\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec=45\n"
            f"WorkingDirectory={config['project_dir'].replace('%', '%%')}\n"
            f"Environment={quote('PYTHONPATH=' + str(Path(config['project_dir']) / 'src'))}\n"
            f"Environment={quote('PATH=' + config['path'])}\n"
            f"ExecStart={' '.join(quote(part, command=True) for part in command)}\n\n"
            "[Install]\nWantedBy=default.target\n"
        )
    return target


def status(config_path):
    """Read an instance without starting services or initializing a runtime database."""
    config, _ = load_config(config_path)
    result = {
        "backend": backend_name(config),
        "session_id": config.get("session_id"),
        "active": [],
        "tasks": {},
        "outbox": {},
    }
    database = Path(config["database_path"])
    if database.exists():
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT value FROM runtime_meta WHERE key='session_id'").fetchone()
            if row:
                result["session_id"] = json.loads(row["value"])
            result["active"] = [
                dict(r)
                for r in db.execute(
                    "SELECT attempt_id,session_id,state,started_at FROM runtime_attempts "
                    "WHERE state IN ('starting','running','draining')"
                )
            ]
            for key, table in [("tasks", "runtime_tasks"), ("outbox", "runtime_outbox")]:
                result[key] = dict(db.execute(f"SELECT state,count(*) FROM {table} GROUP BY state"))
    health = Path(config["state_dir"]) / "health.json"
    if health.exists():
        import time

        snapshot = json.loads(health.read_text())
        result["gateway"] = {
            "websocket_connected": snapshot.get("websocket_connected"),
            "heartbeat_age_seconds": round(time.time() - health.stat().st_mtime, 1),
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser(
        "init", help="Create a new private instance; never starts a receiver"
    )
    init.add_argument("--backend", choices=["claude", "traex"], required=True)
    init.add_argument("--state-dir", type=Path, required=True)
    init.add_argument("--cwd", type=Path, default=Path.cwd())
    init.add_argument("--owner", required=True)
    session = init.add_mutually_exclusive_group(required=True)
    session.add_argument("--session")
    session.add_argument("--new-session", action="store_true")
    init.add_argument("--name", default="Feishu Assistant")
    init.add_argument("--model")
    init.add_argument("--model-provider")
    init.add_argument("--agent-command", help="CLI executable name or absolute path")
    init.add_argument("--node-command", help="Node executable name or absolute path")
    init.add_argument("--path", help="PATH for services, including wrapper dependencies")
    init.add_argument("--credentials-file", type=Path)
    init.add_argument("--app-id")
    init.add_argument("--runner", choices=["auto", "systemd", "process"], default="auto")
    init.add_argument(
        "--worker-access",
        choices=["workspace", "full"],
        default="workspace",
        help="TraeX native-tool access; host-managed MCP commands use the OS user",
    )
    init.add_argument("--integration", choices=["documents", "libra"], action="append", default=[])
    for name in ("doctor", "run", "service-files", "status", "probe"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name == "service-files":
            command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "init":
            print(initialize(args))
        elif args.command == "doctor":
            checks = diagnose(args.config)
            print(json.dumps(checks, ensure_ascii=False, indent=2))
            raise SystemExit(0 if all(c["ok"] for c in checks) else 1)
        elif args.command == "service-files":
            print(service_files(args.config, args.output))
        elif args.command == "status":
            print(json.dumps(status(args.config), ensure_ascii=False, indent=2))
        elif args.command == "probe":
            from .backend_probe import run_probe

            config, _ = load_config(args.config)
            raise SystemExit(0 if run_probe(config)["passed"] else 1)
        else:
            from .supervisor import run

            run(args.config)
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as exc:
        parser.exit(2, f"feishu-bot: {exc}\n")


if __name__ == "__main__":
    main()
