"""POSIX worker supervisor for Linux/macOS without systemd.

A session leader remains alive until cleanup, so a worker exiting cannot hide
ordinary grandchildren. Managed commands use new process groups in this session.
Programs deliberately daemonizing into another session are outside this runner's
guarantees; use the systemd runner for cgroup isolation on Linux.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import selectors
import signal
import subprocess
import time
from pathlib import Path

from .backends import python_command
from .runtime_common import private_json


def process_table():
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,pgid=,stat="],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    rows = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 4:
            rows.append((int(fields[0]), int(fields[1]), int(fields[2]), fields[3]))
    return rows


def session_processes(sid):
    found = []
    for pid, _ppid, _pgid, state in process_table():
        if state.startswith("Z"):
            continue
        with contextlib.suppress(ProcessLookupError):
            if os.getsid(pid) == sid:
                found.append(pid)
    return found


def identity(pid):
    result = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "lstart=,command="],
        capture_output=True, text=True, timeout=5,
    )
    if result.returncode not in {0, 1}:
        raise RuntimeError("Cannot inspect process identity")
    return result.stdout.strip()


def stop_session(pid, expected, child=None):
    if identity(pid) != expected:
        # An absent leader with surviving members is an uncertain cleanup, never
        # permission to signal a potentially unrelated reused PID.
        if session_processes(pid):
            raise RuntimeError("Worker leader identity changed; execution remains isolated")
        return
    for sig, seconds in ((signal.SIGTERM, 1), (signal.SIGKILL, 3)):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            members = session_processes(pid)
            if not members:
                if child:
                    child.poll()
                return
            # Keep the identity anchor alive until the final pass.
            for member in members:
                if member == pid:
                    continue
                with contextlib.suppress(ProcessLookupError):
                    os.kill(member, sig)
            if members == [pid] or sig == signal.SIGKILL:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, sig)
            if child:
                child.poll()
            time.sleep(0.05)
    if session_processes(pid):
        raise RuntimeError("Worker processes still alive; execution remains isolated")


class ProcessWorkers:
    def __init__(self, config):
        self.config = config
        self.root = Path(config["state_dir"]) / "workers"
        self.children = {}

    def record(self, unit):
        if (
            not unit.startswith("feishu-worker-")
            or not unit.removeprefix("feishu-worker-").isalnum()
        ):
            raise ValueError("Invalid worker unit")
        return self.root / (unit + ".json")

    def start(self, attempt, request_path, config):
        record = self.record(attempt["unit_name"])
        record.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        env = dict(
            os.environ,
            PYTHONPATH=str(Path(config["project_dir"]) / "src"),
            FEISHU_WORKER_RUNNER="process",
        )
        log_path = Path(request_path).parent / "guardian.log"
        with open(log_path, "ab", opener=lambda p, f: os.open(p, f, 0o600)) as log:
            child = subprocess.Popen(
                [
                    python_command(config),
                    "-m",
                    "feishu_llm_bot.process_workers",
                    str(request_path),
                    str(record),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=log,
                env=env,
                cwd=config["cwd"],
                start_new_session=True,
            )
        self.children[attempt["unit_name"]] = child
        expected = identity(child.pid)
        private_json(record, {"pid": child.pid, "identity": expected, "state": "starting"})
        # Until this byte is received the guardian cannot execute business work.
        child.stdin.write(b"1\n")
        child.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            if not selector.select(5) or child.stdout.readline() != b"ready\n":
                self.stop(attempt["unit_name"])
                raise RuntimeError("Worker guardian failed to start")
        child.stdin.close()
        child.stdout.close()

    def state(self, unit):
        path = self.record(unit)
        if not path.exists():
            return {"LoadState": "not-found", "ActiveState": "inactive", "SubState": "dead"}
        value = json.loads(path.read_text())
        live = identity(value["pid"]) == value["identity"]
        running = live and value["state"] in {"starting", "running"}
        return {
            "LoadState": "loaded",
            "ActiveState": "active" if live else "inactive",
            "SubState": "running" if running else "exited",
        }

    def stop(self, unit):
        path = self.record(unit)
        if not path.exists():
            return
        value = json.loads(path.read_text())
        stop_session(value["pid"], value["identity"], self.children.get(unit))
        child = self.children.pop(unit, None)
        if child:
            child.wait(timeout=3)
        private_json(path, {**value, "state": "stopped"})


def guardian(request_path, record):
    import sys

    if sys.stdin.readline() != "1\n":
        return
    value = json.loads(Path(record).read_text())
    if value["pid"] != os.getpid() or value["identity"] != identity(os.getpid()):
        raise RuntimeError("Guardian identity did not match the persisted startup record")
    request = json.loads(Path(request_path).read_text())
    config = request["config"]
    # A TERM first stops descendants; the guardian stays as the session identity anchor.
    signal.signal(signal.SIGTERM, lambda *_: None)
    child = subprocess.Popen(
        [python_command(config), "-m", "feishu_llm_bot.worker", str(request_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    private_json(record, {**value, "state": "running"})
    print("ready", flush=True)
    deadline = time.monotonic() + request.get("timeout_seconds", 1200) + 30
    while time.monotonic() < deadline:
        if child.poll() is not None and value["state"] != "exited":
            value["state"] = "exited"
            private_json(record, value)
        time.sleep(0.2)
    # Hard limit even if the parent orchestrator vanished.
    for pid in session_processes(os.getpid()):
        if pid != os.getpid():
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request")
    parser.add_argument("record")
    args = parser.parse_args()
    guardian(args.request, args.record)


if __name__ == "__main__":
    main()
