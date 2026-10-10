"""Bounded scheduling and cancellation; never waits for Feishu delivery."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import signal
import subprocess
import threading
import time
from pathlib import Path

from .backends import check_backend_binding, python_command
from .feishu import IncomingMessage
from .resident import DEFAULT_CONFIG
from .runtime_common import load_config, notify, private_json, singleton
from .runtime_store import RuntimeStore
from .schedules import tick_schedules
from .task_budget import deadlines

LOGGER = logging.getLogger(__name__)


class SystemdWorkers:
    @staticmethod
    def populated(group, root=Path("/sys/fs/cgroup")):
        if (root / "cgroup.controllers").exists():
            path = root / group.lstrip("/") / "cgroup.events"
            return path.exists() and "populated 1" in path.read_text()
        legacy = root / "systemd"
        if not legacy.is_dir():
            raise RuntimeError("Cannot verify worker isolation: cgroup hierarchy unavailable")
        directory = legacy / group.lstrip("/")
        return any(path.read_text().strip() for path in directory.rglob("tasks"))

    def start(self, attempt, request_path, config):
        command = [
            "systemd-run",
            "--user",
            "--quiet",
            "--unit=" + attempt["unit_name"],
            "--property=Type=exec",
            "--property=RemainAfterExit=yes",
            "--property=KillMode=control-group",
            "--property=TimeoutStopSec=10",
            "--property=SendSIGKILL=yes",
            "--property=UMask=0077",
            "--property=RuntimeMaxSec=" + str(int(attempt["timeout_seconds"]) + 30),
            "--property=MemoryMax=4G",
            "--property=TasksMax=128",
            "--property=WorkingDirectory=" + config["cwd"],
            "--setenv=PYTHONPATH=" + str(Path(config["project_dir"]) / "src"),
            python_command(config),
            "-m",
            "feishu_llm_bot.worker",
            str(request_path),
        ]
        subprocess.run(command, check=True, capture_output=True, timeout=10)

    def state(self, unit):
        result = subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                unit + ".service",
                "-p",
                "ActiveState",
                "-p",
                "SubState",
                "-p",
                "ControlGroup",
                "-p",
                "LoadState",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=3,
        )
        return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

    def stop(self, unit):
        previous = self.state(unit)
        if previous.get("LoadState") == "not-found":
            return
        subprocess.run(
            ["systemctl", "--user", "stop", unit + ".service"],
            check=True,
            capture_output=True,
            timeout=15,
        )
        state = self.state(unit)
        if state.get("ActiveState") not in {"inactive", "failed"}:
            raise RuntimeError("Worker has not stopped; execution slot remains isolated")
        group = previous.get("ControlGroup", "")
        if group and self.populated(group):
            raise RuntimeError("Worker descendants still alive; refusing to reuse execution slot")
        with contextlib.suppress(subprocess.SubprocessError):
            subprocess.run(
                ["systemctl", "--user", "reset-failed", unit + ".service"],
                capture_output=True,
                timeout=3,
            )


class Orchestrator:
    def __init__(self, config, store, workers=None):
        self.config, self.store = config, store
        check_backend_binding(store, config)
        if workers is None and config.get("worker_runner", "systemd") == "process":
            from .process_workers import ProcessWorkers

            workers = ProcessWorkers(config)
        self.workers = workers or SystemdWorkers()
        self.monotonic_starts = {}

    def recover(self):
        # A previous supervisor may have died while its worker remained alive.
        for attempt in self.store.active():
            self.end(attempt, attempt["failure_reason"] or "restart_interrupted", time.time())

    def end(self, attempt, reason, now):
        aid = attempt["attempt_id"]
        self.store.drain(aid, reason)
        self.workers.stop(attempt["unit_name"])
        latest = self.store.attempt(aid)
        started = self.monotonic_starts.get(aid)
        outcome = self.store.finish(
            aid,
            now=now,
            reason=reason,
            max_retries=int(self.config.get("max_retries", 2)),
            total_budget=int(self.config.get("total_budget_seconds", 1800)),
            elapsed=time.monotonic() - started if started is not None else None,
        )
        if outcome == "succeeded":
            if latest["session_id"]:
                self.store.set_meta("session_id", latest["session_id"])
                self.store.set_meta("session_input_tokens:" + latest["session_id"],
                                    self.store.meta("input_tokens:" + aid, 0))
            self.store.set_meta("consecutive_model_failures", 0)
        elif reason in {"model_error", "startup_error", "worker_exit"}:
            count = self.store.meta("consecutive_model_failures", 0) + 1
            self.store.set_meta("consecutive_model_failures", count)
            if count >= 3:
                self.store.set_meta("model_retry_at", now + min(300, 60 * 2 ** min(count - 3, 3)))
            # Isolate broken physical histories; confirmed task context is still durable.
            if count >= 2:
                self.store.set_meta("session_id", None)
                with self.store.transaction() as db:
                    self.store._audit(
                        db,
                        attempt["correlation_id"],
                        aid,
                        "session_recovery",
                        "Next execution will use saved confirmed context",
                    )
        self.monotonic_starts.pop(aid, None)
        LOGGER.info(
            "Attempt ended task=%s state=%s reason=%s", attempt["correlation_id"], outcome, reason
        )

    def tick(self, now=None):
        now = time.time() if now is None else now
        tick_schedules(self.store, now)
        self.store.discover(now)
        # Legacy queued status queries are also handled without invoking a model.
        with self.store._lock:
            pending = self.store._connection.execute(
                "SELECT message_id,chat_id,user_text FROM events "
                "WHERE status='accepted' AND message_type='text' ORDER BY sequence LIMIT 128"
            ).fetchall()
        for row in pending:
            self.store.handle_control(IncomingMessage.text(*row), existing=True)
        for attempt in self.store.active():
            aid = attempt["attempt_id"]
            task = self.store.task(attempt["correlation_id"])
            state = self.workers.state(attempt["unit_name"])
            with self.store._lock:
                expired_permission = self.store._connection.execute(
                    "SELECT 1 FROM permission_requests WHERE session_id=? "
                    "AND status IN ('pending','expired') AND expires_at<=? AND created_at>=?",
                    (attempt["session_id"], now, int(attempt["started_at"])),
                ).fetchone()
            reason = None
            if attempt["state"] == "draining":
                reason = attempt["failure_reason"] or "restart_interrupted"
            elif task["cancel_requested"]:
                reason = "cancelled"
            elif expired_permission:
                reason = "permission_expired"
            elif attempt["failure_reason"]:
                reason = attempt["failure_reason"]
            elif state.get("SubState") not in {"running", "start", "start-pre", "start-post"}:
                reason = attempt["failure_reason"] or (
                    "completed" if attempt["answer"] else "worker_exit"
                )
            else:
                started = self.monotonic_starts.setdefault(aid, time.monotonic())
                elapsed = time.monotonic() - started
                remaining = self.config.get("total_budget_seconds", 1800) - task["total_seconds"]
                budget = min(self.config.get("task_timeout_seconds", 1200), remaining)
                soft = deadlines(budget, self.config.get("finalization_reserve_seconds", 90),
                                 now=0)["soft_deadline_monotonic"]
                if elapsed >= soft:
                    # This runs even while the model is silent or compacting its context.
                    from .task_completion import mark_finalization

                    with self.store.transaction() as db:
                        current = db.execute(
                            "SELECT 1 FROM runtime_attempts a JOIN runtime_tasks t "
                            "USING(correlation_id) WHERE a.attempt_id=? AND t.attempt_id=? "
                            "AND a.state IN ('starting','running') AND t.cancel_requested=0",
                            (aid, aid),
                        ).fetchone()
                        if current:
                            mark_finalization(db, aid, attempt["correlation_id"],
                                              "soft_deadline", now)
                if elapsed >= budget:
                    reason = "task_timeout"
                elif attempt["state"] == "starting" and elapsed >= self.config.get(
                    "startup_grace_seconds", 180
                ):
                    reason = "startup_error"
                elif now - (task["activity_at"] or attempt["started_at"]) >= self.config.get(
                    "idle_timeout_seconds", 300
                ):
                    with self.store._lock:
                        permission = self.store._connection.execute(
                            "SELECT 1 FROM permission_requests "
                            "WHERE session_id=? AND status='pending' AND expires_at>?",
                            (attempt["session_id"], now),
                        ).fetchone()
                    if not permission:
                        reason = "idle_timeout"
            if reason:
                self.end(attempt, None if reason == "completed" else reason, now)
            return
        if now < self.store.meta("model_retry_at", 0):
            return
        session_id = self.store.meta("session_id", self.config.get("session_id"))
        if session_id and self.store.meta("session_input_tokens:" + session_id, 0) >= (
            self.config.get("max_resume_input_tokens", 100000)
        ):
            session_id = None  # The worker still receives bounded confirmed task context.
        attempt = self.store.claim(now, session_id)
        if attempt is None:
            return
        task = self.store.task(attempt["correlation_id"])
        attempt["timeout_seconds"] = max(
            1,
            min(
                self.config.get("task_timeout_seconds", 1200),
                self.config.get("total_budget_seconds", 1800) - task["total_seconds"],
            ),
        )
        directory = (
            Path(self.config["state_dir"]).parent
            / "tasks"
            / attempt["correlation_id"]
            / attempt["attempt_id"]
        )
        request_path = directory / "request.json"
        private_json(request_path, {**attempt, "config": self.config, **deadlines(
            attempt["timeout_seconds"], self.config.get("finalization_reserve_seconds", 90)
        )})
        self.monotonic_starts[attempt["attempt_id"]] = time.monotonic()
        try:
            self.workers.start(attempt, request_path, self.config)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            LOGGER.exception("Worker start failed")
            self.end(attempt, "startup_error", now)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    config, _ = load_config(args.config)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = RuntimeStore(Path(config["database_path"]))
    if args.status:
        active = [{k: v for k, v in a.items() if k != "token_hash"} for a in store.active()]
        print(
            json.dumps(
                {
                    "active": active,
                    "session_id": store.meta("session_id"),
                    "model_retry_at": store.meta("model_retry_at", 0),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        store.close()
        return
    with singleton(Path(config["state_dir"]) / "orchestrator.lock"):
        engine = Orchestrator(config, store)
        engine.recover()
        stopped = threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopped.set())
        notify("READY=1")
        LOGGER.info("Task orchestrator started")
        try:
            while not stopped.is_set():
                engine.tick()
                notify()
                stopped.wait(1)
        finally:
            engine.recover()
            store.close()


if __name__ == "__main__":
    main()
