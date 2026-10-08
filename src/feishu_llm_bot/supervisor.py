"""Foreground service group, usable directly or under systemd/launchd."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import threading
from pathlib import Path

from .backends import check_backend_binding, clean_environment, python_command
from .config import Settings
from .runtime_common import load_config, singleton
from .runtime_store import RuntimeStore


def run(config_path):
    config_path = Path(config_path).expanduser().resolve()
    config, env = load_config(config_path)
    Settings.from_env(env)
    environment = clean_environment(os.environ)
    environment.update(
        PYTHONPATH=str(Path(config["project_dir"]) / "src"),
        PATH=config.get("path", os.environ.get("PATH", "")),
    )
    stopped = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopped.set())
    children = {}
    with singleton(Path(config["state_dir"]) / "services.lock"):
        store = RuntimeStore(Path(config["database_path"]))
        try:
            check_backend_binding(store, config)
        finally:
            store.close()
        try:
            while not stopped.is_set():
                for module in ("gateway", "orchestrator", "sender"):
                    child = children.get(module)
                    if child is None or child.poll() is not None:
                        children[module] = subprocess.Popen(
                            [
                                python_command(config),
                                "-m",
                                f"feishu_llm_bot.{module}",
                                "--config",
                                str(config_path),
                            ],
                            env=environment,
                            start_new_session=True,
                        )
                stopped.wait(5)
        finally:
            # Drain business execution before stopping ingress and delivery.
            for module in ("orchestrator", "gateway", "sender"):
                child = children.get(module)
                if child is None:
                    continue
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=30 if module == "orchestrator" else 5)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=3)
