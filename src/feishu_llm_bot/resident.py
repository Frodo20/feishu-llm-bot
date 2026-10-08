"""Supervise an interactive Claude in screen; systemd owns restart and boot policy.

No prompts or credentials are injected into another session. The ordinary MCP
child receives its inbox credentials naturally from this exact Claude process.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import signal
import socket
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG = Path.home() / ".local/state/feishu-llm-bot/runtime.json"


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected an object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    with open(temporary, "w", opener=lambda p, f: os.open(p, f, 0o600)) as stream:
        json.dump(value, stream)
        stream.write("\n")
    temporary.replace(path)


def health_error(
    health: dict[str, Any], instance: str, now: float, max_age: float = 45,
) -> str | None:
    if health.get("instance") != instance:
        return "heartbeat belongs to a previous service instance"
    if not health.get("ready"):
        return "MCP or Python bridge is not ready"
    for key in ("timestamp", "python_health_at"):
        value = health.get(key)
        if not isinstance(value, (int, float)) or not -5 <= now - value <= max_age:
            return f"stale {key}"
    return None


def load_health(path: Path) -> dict[str, Any]:
    try:
        return read_json(path)
    except (OSError, ValueError):
        return {}


def pending_count(path: Path) -> int:
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        return connection.execute(
            "SELECT count(*) FROM events WHERE status NOT IN ('replied', 'failed')"
        ).fetchone()[0]


def ensure_no_receiver(socket_path: Path) -> None:
    if not socket_path.exists():
        return
    with socket.socket(socket.AF_UNIX) as probe:
        probe.settimeout(2)
        try:
            probe.connect(str(socket_path))
        except (ConnectionRefusedError, FileNotFoundError):
            return
        raise RuntimeError("A Feishu receiver is already running; close its owning Claude first")


def build_command(config: dict[str, Any], mcp_path: Path) -> list[str]:
    return [
        "/usr/bin/screen", "-D", "-m", "-U", "-S", config["screen_name"],
        "-c", str(Path(config["project_dir"]) / "deploy/resident.screenrc"),
        config["claude_command"], "--model", config["model"],
        "--permission-mode", "acceptEdits",
        "--resume", config["session_id"], "--name", config["name"],
        "--mcp-config", str(mcp_path),
        "--append-system-prompt",
        "This session is a persistent Feishu assistant managed by systemd. "
        "Stay in this interactive session. Do not background, exit, or replace your own "
        "session, or stop/restart feishu-claude.service unless the user explicitly asks. "
        "Feishu tool approvals are handled by the configured permission hook.",
    ]


def run(config: dict[str, Any]) -> int:
    state = Path(config["state_dir"])
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(state, 0o700)
    lock = (state / "resident.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError("Another resident supervisor is already running") from None
    ensure_no_receiver(Path(config["permission_socket"]))
    instance = str(uuid.uuid4())
    health_path = state / "health.json"
    mcp_path = state / "mcp.json"
    mcp = read_json(Path(config["mcp_config"]))
    mcp["mcpServers"]["feishu"].setdefault("env", {}).update({
        "FEISHU_RESIDENT_INSTANCE": instance,
        "FEISHU_BRIDGE_HEALTH_FILE": str(health_path),
        "FEISHU_PERMISSION_SESSION_ID": config["session_id"],
    })
    if config.get("progress_state_dir"):
        mcp["mcpServers"]["feishu"]["env"]["FEISHU_PROGRESS_STATE_PATH"] = str(
            Path(config["progress_state_dir"]) / "progress.sqlite3"
        )
    write_json(mcp_path, mcp)
    write_json(state / "resident.json", {
        "instance": instance, "session_id": config["session_id"],
        "supervisor_pid": os.getpid(), "started_at": time.time(),
        "screen_name": config["screen_name"],
    })
    environment = dict(os.environ)
    environment["PATH"] = config["path"]
    environment["TERM"] = "xterm-256color"
    # Do not inherit another agent's inbox, nested-session marker, or screen.
    for key in ("CLAUDECODE", "CLAUDE_CODE_MESSAGING_SOCKET",
                "CLAUDE_CODE_MESSAGING_TOKEN", "STY", "WINDOW"):
        environment.pop(key, None)
    child = subprocess.Popen(build_command(config, mcp_path), cwd=config["cwd"], env=environment)
    stopping = False

    def stop(_signal: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    ever_healthy = False
    unhealthy_since: float | None = None
    disconnected_since: float | None = None
    last_connection: bool | None = None
    LOGGER.info("Started screen pid=%d session=%s", child.pid, config["session_id"])
    try:
        while not stopping:
            if child.poll() is not None:
                LOGGER.error("screen/Claude exited code=%s", child.returncode)
                return 1
            health = load_health(health_path)
            error = health_error(health, instance, time.time())
            now = time.monotonic()
            if error:
                if unhealthy_since is None:
                    unhealthy_since = now
                grace = config.get("startup_grace_seconds", 180) if not ever_healthy else 30
                since = started if not ever_healthy else unhealthy_since
                if now - since > grace:
                    LOGGER.error("Restart required: %s", error)
                    return 1
            else:
                if not ever_healthy:
                    LOGGER.info("MCP and Python bridge healthy")
                ever_healthy = True
                unhealthy_since = None
                connected = health.get("websocket_connected") is True
                if connected != last_connection:
                    LOGGER.info("Feishu WebSocket connected=%s", connected)
                    last_connection = connected
                if connected:
                    disconnected_since = None
                elif disconnected_since is None:
                    disconnected_since = now
                elif now - disconnected_since > config.get("disconnect_grace_seconds", 600):
                    LOGGER.error("WebSocket reconnect grace exhausted; restarting session")
                    return 1
            time.sleep(2)
        return 0
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        lock.close()


def status(config: dict[str, Any]) -> int:
    state = Path(config["state_dir"])
    resident = load_health(state / "resident.json")
    health = load_health(state / "health.json")
    error = health_error(health, resident.get("instance", ""), time.time())
    result = {
        "session_id": config["session_id"],
        "healthy": error is None,
        "websocket_connected": error is None and health.get("websocket_connected") is True,
        "error": error,
        "mcp_pid": health.get("mcp_pid"),
        "bridge_pid": health.get("bridge_pid"),
        "pending_messages": pending_count(Path(config["database_path"])),
        "attach": f'screen -r {config["screen_name"]}',
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["healthy"] and result["websocket_connected"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = read_json(args.config)
    raise SystemExit(status(config) if args.status else run(config))


if __name__ == "__main__":
    main()
