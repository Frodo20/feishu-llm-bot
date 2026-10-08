"""Bounded subprocess ownership with early, schema-checked business receipts."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path

MAX_OUTPUT_BYTES = 8 * 1024 * 1024


class CleanupError(RuntimeError):
    """The task must be drained before another operation can run."""


def group_alive(pgid):
    if not Path("/proc/self/stat").exists():
        from .process_workers import process_table

        return any(group == pgid and not state.startswith("Z")
                   for _pid, _ppid, group, state in process_table())
    # Ignore reparented zombies; they cannot execute and init may reap them asynchronously.
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            stat = path.read_text().rsplit(")", 1)[1].split()
            if int(stat[2]) == pgid and stat[0] != "Z":
                return True
        except (OSError, ValueError, IndexError):
            continue
    return False


def stop_group(process):
    for sig, grace in ((signal.SIGTERM, 0.3), (signal.SIGKILL, 2)):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, sig)
        until = time.monotonic() + grace
        while time.monotonic() < until:
            process.poll()
            if not group_alive(process.pid):
                process.wait(timeout=1)
                return
            time.sleep(0.02)
    raise CleanupError("Command process group could not be confirmed stopped")


def read_payload(path):
    if path.stat().st_size > MAX_OUTPUT_BYTES:
        return None
    try:
        return json.loads(path.read_text())
    except (ValueError, UnicodeError):
        return None


def execute_command(
    args, directory, *, timeout=300, env=None, result_validator=None, on_result=None, exit_grace=1.0
):
    directory = Path(directory)
    output, error = directory / "stdout.txt", directory / "stderr.txt"
    result = {
        "exit_code": None,
        "timed_out": False,
        "background_children": False,
        "stdout_path": str(output),
        "stderr_path": str(error),
        "first_output_seconds": None,
        "result_seconds": None,
    }
    start, received_at, receipt = time.monotonic(), None, None
    process = None
    with (
        open(output, "w", opener=lambda p, f: os.open(p, f, 0o600)) as out,
        open(error, "w", opener=lambda p, f: os.open(p, f, 0o600)) as err,
    ):
        try:
            try:
                process = subprocess.Popen(
                    args,
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=err,
                    env=env,
                    **({"process_group": 0} if (env or {}).get("FEISHU_WORKER_RUNNER") == "process"
                       else {"start_new_session": True}),
                )
            except OSError:
                result["reason_code"] = "spawn_failed"
            while process is not None:
                now, size = time.monotonic(), output.stat().st_size
                if size and result["first_output_seconds"] is None:
                    result["first_output_seconds"] = now - start
                if size > MAX_OUTPUT_BYTES or error.stat().st_size > MAX_OUTPUT_BYTES:
                    result["reason_code"] = "output_limit"
                    break
                if receipt is None and result_validator and size:
                    payload = read_payload(output)
                    if payload is not None and result_validator(payload):
                        receipt, received_at = payload, now
                        result.update(validated_response=payload, result_seconds=now - start)
                        if on_result:
                            on_result(dict(result))
                code = process.poll()
                if code is not None:
                    result["exit_code"] = code
                    result["background_children"] = group_alive(process.pid)
                    if result["background_children"]:
                        result["reason_code"] = "background_children"
                    break
                if received_at is not None and now >= received_at + exit_grace:
                    result["reason_code"] = "process_exit_timeout_after_result"
                    break
                if now >= start + timeout:
                    result["timed_out"] = True
                    result["reason_code"] = (
                        "process_exit_timeout_after_result" if receipt else "command_timeout"
                    )
                    break
                time.sleep(0.03)
        finally:
            if process is not None and (process.poll() is None or group_alive(process.pid)):
                stop_group(process)
    result["process_seconds"] = time.monotonic() - start
    result["cleanup_confirmed"] = True
    if receipt is not None and read_payload(output) != receipt:
        result.pop("validated_response", None)
        result["reason_code"] = "invalid_response"
    with output.open("rb") as stream:
        stream.seek(max(0, output.stat().st_size - 24000))
        result["output_tail"] = stream.read().decode("utf-8", errors="replace")
    with error.open("rb") as stream:
        stream.seek(max(0, error.stat().st_size - 4000))
        result["stderr_tail"] = stream.read().decode("utf-8", errors="replace")
    return result
