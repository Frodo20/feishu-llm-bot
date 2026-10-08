#!/usr/bin/env python3
"""Verify TERM/KILL cleanup of a real systemd worker including resistant children."""

import json
import subprocess
import time
import uuid

from feishu_llm_bot.orchestrator import SystemdWorkers


def main():
    unit = "feishu-isolation-probe-" + uuid.uuid4().hex
    child = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"
    command = (
        "import subprocess,time; subprocess.Popen(['/usr/bin/python3', '-c', "
        + repr(child)
        + "]); time.sleep(60)"
    )
    subprocess.run(
        [
            "systemd-run",
            "--user",
            "--quiet",
            "--unit=" + unit,
            "--property=Type=exec",
            "--property=RemainAfterExit=yes",
            "--property=KillMode=control-group",
            "--property=TimeoutStopSec=2",
            "--property=RuntimeMaxSec=60",
            "/usr/bin/python3",
            "-c",
            command,
        ],
        check=True,
        timeout=10,
    )
    workers = SystemdWorkers()
    try:
        time.sleep(0.5)
        group = workers.state(unit)["ControlGroup"]
        assert workers.populated(group), "Run outside a private PID namespace to verify cgroup v1"
        started = time.monotonic()
        workers.stop(unit)
        elapsed = time.monotonic() - started
        assert not workers.populated(group), "Worker descendants are still alive"
        print(
            json.dumps(
                {
                    "group": group,
                    "stop_seconds": round(elapsed, 3),
                    "populated_before": True,
                    "populated_after": False,
                }
            )
        )
    finally:
        workers.stop(unit)


if __name__ == "__main__":
    main()
