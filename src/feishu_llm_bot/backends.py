"""Backend selection and portable executable/configuration helpers."""

from __future__ import annotations

import os
import shutil
import sys
import uuid
from pathlib import Path


def backend_name(config):
    name = config.get("agent_backend", "claude")
    if name not in {"claude", "traex"}:
        raise ValueError("agent_backend must be claude or traex")
    return name


def executable(config, key, *candidates):
    value = config.get(key)
    if value:
        expanded = os.path.expanduser(str(value))
        found = shutil.which(expanded, path=config.get("path", os.environ.get("PATH")))
        if found:
            return os.path.abspath(found)
        raise ValueError(f"{key} does not name an executable: {expanded}")
    for candidate in candidates:
        found = shutil.which(candidate, path=config.get("path", os.environ.get("PATH")))
        if found:
            return found
    raise ValueError(f"{key} is required; executable not found: {', '.join(candidates)}")


def python_command(config):
    return config.get("python_command") or str(Path(config["project_dir"]) / ".venv/bin/python")


def current_python():
    # Do not resolve a virtualenv symlink: resolving it loses the venv on invocation.
    return os.path.abspath(sys.executable)


def enabled_integrations(config):
    # Missing field preserves the existing deployment's tool set.
    values = config.get("integrations", ["documents", "libra"])
    if not isinstance(values, list) or any(v not in ("documents", "libra") for v in values):
        raise ValueError("integrations must be a list containing documents and/or libra")
    return values


def check_backend_binding(store, config):
    """Do not interpret a Claude session UUID as a TraeX thread (or vice versa)."""
    name = backend_name(config)
    saved = store.meta("agent_backend")
    if saved is None:
        # Legacy runtime databases contain Claude sessions.
        if name != "claude" and store.meta("session_id"):
            raise ValueError("Existing database contains a Claude session; use a new instance")
        store.set_meta("agent_backend", name)
    elif saved != name:
        raise ValueError("Changing an existing backend requires a new state directory")


def validate_runtime_config(config):
    backend_name(config)
    enabled_integrations(config)
    if config.get("worker_runner", "systemd") not in {"systemd", "process"}:
        raise ValueError("worker_runner must be systemd or process")
    if config.get("worker_access", "workspace") not in {"workspace", "full"}:
        raise ValueError("worker_access must be workspace or full")
    if config.get("session_id"):
        uuid.UUID(config["session_id"])
    agent_env = config.get("agent_environment", {})
    if not isinstance(agent_env, dict) or any(
        k not in {"TRAE_HOME", "TRAECLI_HOME", "CLAUDE_CONFIG_DIR"}
        or not isinstance(v, str) or not Path(v).is_absolute()
        for k, v in agent_env.items()
    ):
        raise ValueError("agent_environment only accepts absolute agent configuration directories")
    for field in (
        "cwd",
        "project_dir",
        "state_dir",
        "database_path",
        "bridge_env_file",
        "progress_state_dir",
    ):
        value = config.get(field)
        if (
            not isinstance(value, str)
            or not Path(value).is_absolute()
            or any(c in value for c in "\r\n\0")
        ):
            raise ValueError(f"{field} must be an absolute path without control characters")
    for field in (
        "task_timeout_seconds",
        "total_budget_seconds",
        "startup_grace_seconds",
        "idle_timeout_seconds",
    ):
        value = config.get(field, 1)
        if type(value) not in (int, float) or not 0 < value < float("inf"):
            raise ValueError(f"{field} must be a positive finite number")


def clean_environment(environment):
    environment = dict(environment)
    for key in (
        "CLAUDECODE",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "STY",
        "WINDOW",
        "NOTIFY_SOCKET",
        "WATCHDOG_USEC",
        "WATCHDOG_PID",
        "TRAECLI_THREAD_ID",
        "CODEX_THREAD_ID",
        "FEISHU_WORKER_REQUEST",
        "FEISHU_WORKER_RUNNER",
    ):
        environment.pop(key, None)
    return environment
