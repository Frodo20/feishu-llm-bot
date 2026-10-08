"""Small shared utilities for the supervised services."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import socket
from pathlib import Path

from .progress import read_environment
from .resident import DEFAULT_CONFIG, read_json


def load_config(path=DEFAULT_CONFIG):
    config = read_json(Path(path))
    if config.get("runtime_enabled"):
        from .backends import validate_runtime_config

        validate_runtime_config(config)
    environment = read_environment(Path(config["bridge_env_file"]))
    environment["FEISHU_PROGRESS_STATE_PATH"] = str(
        Path(config["database_path"])
        if config.get("runtime_enabled")
        else Path(config["progress_state_dir"]) / "progress.sqlite3"
    )
    if (
        Path(environment.get("FEISHU_BOT_DB_PATH", config["database_path"])).resolve()
        != Path(config["database_path"]).resolve()
    ):
        raise ValueError("Gateway and scheduler database paths must match")
    return config, environment


def private_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", opener=lambda p, f: os.open(p, f, 0o600)) as stream:
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    tmp.replace(path)


@contextlib.contextmanager
def singleton(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with open(path, "a", opener=lambda p, f: os.open(p, f, 0o600)) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def notify(message="WATCHDOG=1"):
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.sendto(message.encode(), address)
