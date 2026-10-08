"""One owned TraeX app-server over stdio. No shared daemon or live-thread injection.

The JSON-RPC v2 protocol is probed by doctor and exercised by the backend probe.
Only notifications matching our forked thread and turn can affect this attempt.
"""

from __future__ import annotations

import contextlib
import json
import os
import selectors
import subprocess
import time
from collections import deque
from pathlib import Path

from .backends import executable
from .runtime_common import private_json
from .task_budget import finalization_reason

MAX_LINE = 8 * 1024 * 1024


class RpcError(RuntimeError):
    def __init__(self, message, remote_error=None):
        super().__init__(message)
        self.remote_error = remote_error


class RpcConnection:
    def __init__(self, child, check):
        self.child, self.check = child, check
        self.selector = selectors.DefaultSelector()
        self.selector.register(child.stdout, selectors.EVENT_READ)
        self.buffer = bytearray()
        self.next_id = 0
        self.pending = deque()
        self.pending_bytes = 0

    def send(self, value):
        self.child.stdin.write(json.dumps(value, ensure_ascii=False).encode() + b"\n")
        self.child.stdin.flush()

    def receive(self):
        self.check()
        if self.pending:
            value, size = self.pending.popleft()
            self.pending_bytes -= size
            return value
        return self._receive_wire()

    def _receive_wire(self):
        while True:
            self.check()
            if b"\n" in self.buffer:
                line, _, rest = self.buffer.partition(b"\n")
                self.buffer = bytearray(rest)
                if len(line) > MAX_LINE:
                    raise RpcError("oversized protocol record")
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RpcError("invalid protocol object")
                return value
            if len(self.buffer) > MAX_LINE:
                raise RpcError("oversized protocol record")
            if not self.selector.select(0.2):
                continue
            data = os.read(self.child.stdout.fileno(), 65536)
            if not data:
                raise RpcError("app-server closed its output")
            self.buffer.extend(data)

    def call(self, method, params):
        self.next_id += 1
        ident = self.next_id
        self.send({"id": ident, "method": method, "params": params})
        while True:
            message = self._receive_wire()
            if message.get("id") == ident and "method" not in message:
                if "error" in message:
                    # Never forward remote error bodies containing paths/prompts/credentials.
                    raise RpcError(f"{method} rejected by app-server", message["error"])
                result = message.get("result", {})
                if not isinstance(result, dict):
                    raise RpcError(f"{method} returned a non-object result")
                return result
            if "id" in message and "method" in message:
                self.send(
                    {
                        "id": message["id"],
                        "error": {
                            "code": -32601,
                            "message": "No interactive input during thread setup",
                        },
                    }
                )
            elif "method" in message:
                size = len(json.dumps(message))
                if self.pending_bytes + size > MAX_LINE:
                    raise RpcError("too many notifications before RPC response")
                self.pending.append((message, size))
                self.pending_bytes += size

    def close(self):
        self.selector.close()


def normalize_item(method, item):
    kind, ident = item.get("type"), item.get("id")
    if kind == "agentMessage" and method == "item/completed":
        return {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": item.get("text", "")},
                ]
            },
        }
    if kind == "mcpToolCall":
        name = f"mcp__{item.get('server', '')}__{item.get('tool', '')}"
        inputs = item.get("arguments", {})
    elif kind == "commandExecution":
        name, inputs = "Bash", {"command": item.get("command", "")}
    elif kind == "fileChange":
        name, inputs = "Edit", {}
    elif kind in {"webSearch", "imageView"}:
        name, inputs = ("WebSearch" if kind == "webSearch" else "Read"), {}
    elif kind in {"reasoning", "userMessage", "contextCompaction", "agentMessage"}:
        return None
    else:
        # Unknown executable item types must not silently become safe to retry.
        name, inputs = "TraeXTool", {}
    if method == "item/started":
        return {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": ident,
                        "name": name,
                        "input": inputs,
                    }
                ]
            },
        }
    return {
        "type": "user",
        "message": {
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": ident,
                    "is_error": item.get("status") in {"failed", "declined"}
                    or bool(item.get("error")),
                }
            ]
        },
    }


def thread_options(config, request_path):
    project = Path(config["project_dir"])
    return {
        "cwd": config["cwd"],
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": "danger-full-access"
        if config.get("worker_access") == "full"
        else "workspace-write",
        **({"model": config["model"]} if config.get("model") else {}),
        **({"modelProvider": config["model_provider"]} if config.get("model_provider") else {}),
        "config": {
            "features.multi_agent": False,
            "disallowed_tools": ["spawn_agent", "send_input", "cron_create", "cron_delete"],
            "mcp_servers.feishu": {
                "command": executable(config, "node_command", "node"),
                "args": [str(project / "node-channel/worker-server.mjs")],
                "env": {"FEISHU_WORKER_REQUEST": str(request_path)},
                "required": True,
                "enabled": True,
                "startup_timeout_sec": 30,
                "tool_timeout_sec": 360,
                "default_tools_approval_mode": "approve",
            },
        },
    }


def run_traex(store, request, request_path, prompt, environment, emit):
    config = request["config"]
    directory = Path(request_path).parent
    deadline = request["deadline_monotonic"]
    aid, token = request["attempt_id"], request["token"]

    def check():
        store.authenticate(aid, token)
        if time.monotonic() >= deadline:
            raise TimeoutError("task_timeout")

    command = [
        executable(config, "traex_command", "traex", "traecli"),
        "app-server",
        "--listen",
        "stdio://",
    ]
    outcome, rpc, thread_id, turn_id = "worker_exit", None, None, None
    with open(directory / "stderr.log", "w", opener=lambda p, f: os.open(p, f, 0o600)) as err:
        child = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=err,
            env=environment,
            cwd=config["cwd"],
        )
        try:
            rpc = RpcConnection(child, check)
            rpc.call(
                "initialize",
                {
                    "clientInfo": {"name": "feishu_llm_bot", "version": "0.3.0"},
                    "capabilities": {"experimentalApi": True},
                },
            )
            rpc.send({"method": "initialized", "params": {}})
            params = thread_options(config, request_path)
            params["developerInstructions"] = (
                "This is an isolated Feishu bot worker. Do not delegate to other agents, "
                "schedule session cron jobs, or modify the bot runtime. "
                "Use only the provided feishu MCP server for managed operations."
            )
            method = "thread/start"
            if request.get("session_id"):
                method = "thread/fork"
                params.update(
                    threadId=request["session_id"],
                    ephemeral=False,
                    name=config["name"],
                    deferGoalContinuation=True,
                )
            result = rpc.call(method, params)
            thread_id = result["thread"]["id"]
            if not isinstance(thread_id, str) or thread_id == request.get("session_id"):
                raise RpcError("backend did not create an independent thread")
            emit(
                {
                    "type": "system",
                    "subtype": "init",
                    "session_id": thread_id,
                    "model": result.get("model", config.get("model")),
                }
            )
            private_json(
                directory / "session.json",
                {"backend": "traex", "session_id": thread_id, "model": result.get("model")},
            )
            # Native tool hooks vary by TraeX release. Conservatively fence automatic retries
            # before starting a turn; managed tools still authenticate every call themselves.
            store.record_activity(aid, time.time(), unsafe=True)
            result = rpc.call(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [
                        {"type": "text", "text": prompt, "text_elements": []},
                    ],
                    "capabilities": {"mcpServers": ["feishu"], "skills": None},
                },
            )
            turn_id = result["turn"]["id"]
            answer = ""
            while True:
                message = rpc.receive()
                method, params = message.get("method", ""), message.get("params", {})
                if "id" in message and method:
                    matches = (
                        params.get("threadId") == thread_id and params.get("turnId") == turn_id
                    )
                    if matches and method in {
                        "item/commandExecution/requestApproval",
                        "item/fileChange/requestApproval",
                    }:
                        check()
                        full = config.get("worker_access") == "full"
                        allow = full and not finalization_reason(store, request)
                        rpc.send(
                            {
                                "id": message["id"],
                                "result": {
                                    "decision": "accept" if allow else "decline",
                                },
                            }
                        )
                    else:
                        rpc.send(
                            {
                                "id": message["id"],
                                "error": {
                                    "code": -32601,
                                    "message": "Interactive request unavailable in bot worker",
                                },
                            }
                        )
                    continue
                if params.get("threadId") != thread_id:
                    continue
                if method == "turn/completed":
                    turn = params.get("turn", {})
                    if turn.get("id") != turn_id:
                        continue
                    outcome = emit(
                        {
                            "type": "result",
                            "result": answer,
                            "is_error": turn.get("status") != "completed",
                        }
                    )
                    break
                if params.get("turnId") != turn_id:
                    continue
                if method in {"item/started", "item/completed"}:
                    item = params.get("item", {})
                    if (
                        method == "item/completed"
                        and item.get("type") == "agentMessage"
                        and item.get("phase") != "commentary"
                    ):
                        answer = item.get("text", "")
                    entry = normalize_item(method, item)
                    if entry:
                        emit(entry)
                elif method.endswith("/delta"):
                    # Activity only; do not publish chain-of-thought or raw tool output.
                    store.record_activity(aid, time.time())
        except TimeoutError:
            outcome = "task_timeout"
        except PermissionError:
            outcome = "cancelled"
        except (OSError, ValueError, KeyError, TypeError, RpcError) as exc:
            private_json(
                directory / "backend-error.json",
                {
                    "backend": "traex",
                    "error_type": type(exc).__name__,
                    "stage": "turn" if turn_id else "thread" if thread_id else "initialize",
                    "reason": str(exc) if isinstance(exc, RpcError) else "Invalid backend protocol",
                    "remote_error": getattr(exc, "remote_error", None),
                },
            )
            outcome = "backend_protocol_error"
        finally:
            if rpc:
                if thread_id and turn_id:
                    with contextlib.suppress(OSError):
                        rpc.send(
                            {
                                "id": 999999,
                                "method": "turn/interrupt",
                                "params": {"threadId": thread_id, "turnId": turn_id},
                            }
                        )
                rpc.close()
            with contextlib.suppress(ProcessLookupError):
                child.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=3)
            # The supervisor owns cleanup of the full worker execution group.
    return outcome
