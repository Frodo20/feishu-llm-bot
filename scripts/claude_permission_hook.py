#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

_MAX_STDIN_BYTES = 64 * 1024
_MAX_SOCKET_LINE_BYTES = 16 * 1024
_DEFAULT_FLUX_HOOK = str(Path.home() / ".flux/bin/flux-hooks-claude")
_DEFAULT_TIMEOUT_SECONDS = 605.0
_ALLOWED_REQUEST_KEYS = {
    "session_id",
    "transcript_path",
    "cwd",
    "permission_mode",
    "hook_event_name",
    "tool_name",
    "tool_input",
    "tool_use_id",
    "permission_suggestions",
    "mcp_server",
    "effort",
    "prompt_id",
}
_PATH_FIELDS = ("file_path", "path", "notebook_path", "directory")
_HOST_FIELDS = ("url", "uri")
_AUTO_ALLOW_TOOLS = {
    "Bash", "Read", "Glob", "Grep", "LS", "WebFetch", "WebSearch",
    "ListMcpResourcesTool", "ReadMcpResourceTool",
    "mcp__feishu__reply", "mcp__feishu__read_image",
    "mcp__feishu__operations", "mcp__feishu__cli_help",
    "mcp__feishu__search_documents", "mcp__feishu__fetch_document",
}


def _deny(message: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": {
                "behavior": "deny",
                "message": message,
                "interrupt": False,
            },
        }
    }


def _allow() -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": {"behavior": "allow"},
        }
    }


def _read_stdin(stream: Any) -> bytes:
    data = stream.buffer.read(_MAX_STDIN_BYTES + 1)
    if len(data) > _MAX_STDIN_BYTES:
        raise ValueError("permission request exceeds the input limit")
    return data


def _parse_request(raw: bytes) -> dict[str, Any]:
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("permission request is not valid JSON") from exc
    if not isinstance(request, dict) or not set(request).issubset(_ALLOWED_REQUEST_KEYS):
        raise ValueError("permission request has an invalid shape")
    session_id = request.get("session_id")
    cwd = request.get("cwd")
    event = request.get("hook_event_name")
    tool_name = request.get("tool_name")
    tool_input = request.get("tool_input")
    if (
        not isinstance(session_id, str)
        or not session_id
        or not isinstance(cwd, str)
        or not cwd
        or event not in {"PermissionRequest", "PreToolUse"}
        or not isinstance(tool_name, str)
        or not tool_name
        or len(tool_name) > 256
        or not isinstance(tool_input, dict)
    ):
        raise ValueError("permission request has an invalid shape")
    return request


def _short_path(value: str) -> str:
    path = Path(value)
    parts = path.parts
    if len(parts) <= 3:
        return str(path)
    return str(Path("…", *parts[-3:]))


def _safe_host(value: str) -> str | None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    host = parsed.hostname
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parsed.scheme, host, parsed.path[:160], "", ""))


def _summary(tool_name: str, tool_input: dict[str, Any]) -> str:
    if tool_name == "Bash":
        return "Bash command (details hidden)"

    details: list[str] = []
    for field in _PATH_FIELDS:
        value = tool_input.get(field)
        if isinstance(value, str) and value:
            details.append(f"{field}={_short_path(value)[:200]}")
            break
    for field in _HOST_FIELDS:
        value = tool_input.get(field)
        if isinstance(value, str) and value:
            safe = _safe_host(value)
            if safe is not None:
                details.append(f"url={safe}")
            break
    if tool_name.startswith("mcp__") and not details:
        return f"{tool_name} (arguments hidden)"
    return f"{tool_name}: {', '.join(details)}" if details else f"{tool_name} (details hidden)"


def _relay_payload(request: dict[str, Any]) -> dict[str, str]:
    cwd = request["cwd"]
    return {
        "session_id": request["session_id"],
        "tool_name": request["tool_name"],
        "cwd_context": _short_path(cwd)[:200],
        "summary": _summary(request["tool_name"], request["tool_input"]),
    }


def _timeout(env: dict[str, str]) -> float:
    raw = env.get("FEISHU_PERMISSION_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("FEISHU_PERMISSION_TIMEOUT_SECONDS must be numeric") from exc
    if not 0 < value <= 3600:
        raise ValueError("FEISHU_PERMISSION_TIMEOUT_SECONDS is outside the allowed range")
    return value


def _receive_line(connection: socket.socket) -> bytes:
    chunks = bytearray()
    while len(chunks) <= _MAX_SOCKET_LINE_BYTES:
        chunk = connection.recv(min(4096, _MAX_SOCKET_LINE_BYTES + 1 - len(chunks)))
        if not chunk:
            break
        chunks.extend(chunk)
        newline = chunks.find(b"\n")
        if newline >= 0:
            if newline != len(chunks) - 1:
                raise ValueError("permission relay returned trailing data")
            return bytes(chunks[:newline])
    if len(chunks) > _MAX_SOCKET_LINE_BYTES:
        raise ValueError("permission relay response exceeds the limit")
    raise ValueError("permission relay returned an incomplete response")


def _request_decision(payload: dict[str, str], env: dict[str, str]) -> dict[str, Any]:
    socket_path = env.get("FEISHU_PERMISSION_SOCKET_PATH", "").strip()
    if not socket_path:
        raise ValueError("FEISHU_PERMISSION_SOCKET_PATH is required")
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
    if len(encoded) > _MAX_SOCKET_LINE_BYTES:
        raise ValueError("permission relay request exceeds the limit")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(_timeout(env))
        connection.connect(socket_path)
        connection.sendall(encoded)
        response_raw = _receive_line(connection)
    try:
        response = json.loads(response_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("permission relay returned invalid JSON") from exc
    if not isinstance(response, dict) or set(response) != {"decision"}:
        raise ValueError("permission relay returned an invalid response")
    decision = response["decision"]
    if decision == "allow":
        return _allow()
    if decision == "deny":
        return _deny("Denied through Feishu")
    raise ValueError("permission relay returned an invalid decision")


def _delegate_to_flux(raw: bytes, env: dict[str, str]) -> int:
    hook = env.get("FEISHU_PERMISSION_FLUX_HOOK", _DEFAULT_FLUX_HOOK)
    completed = subprocess.run([hook], input=raw, check=False)
    return completed.returncode


def main() -> int:
    env = dict(os.environ)
    request = {}
    worker, authenticated, raw = None, False, b"{}"
    try:
        raw = _read_stdin(sys.stdin)
        request = _parse_request(raw)
        worker = None
        allowed_tools = _AUTO_ALLOW_TOOLS
        if env.get("FEISHU_WORKER_REQUEST"):
            from feishu_llm_bot.runtime_store import RuntimeStore
            from feishu_llm_bot.task_budget import (
                FINAL_TOOLS,
                finalization_reason,
                finish_message,
            )
            from feishu_llm_bot.tool_policy import validate_worker_policy
            from feishu_llm_bot.worker import READ_TOOLS

            worker = json.loads(Path(env["FEISHU_WORKER_REQUEST"]).read_text())
            store = RuntimeStore(Path(worker["config"]["database_path"]))
            try:
                attempt = store.authenticate(worker["attempt_id"], worker["token"])
                if attempt["session_id"] != request["session_id"]:
                    raise PermissionError("This tool belongs to a different execution")
                authenticated = True
                validate_worker_policy(worker["config"])
                reason = finalization_reason(store, worker)
                if reason and request["tool_name"] not in FINAL_TOOLS:
                    decision = _deny(finish_message(reason))
                    if request["hook_event_name"] == "PreToolUse":
                        decision = {"hookSpecificOutput": {
                            "hookEventName": "PreToolUse", "permissionDecision": "deny",
                            "permissionDecisionReason": finish_message(reason),
                        }}
                    sys.stdout.write(json.dumps(decision) + "\n")
                    return 0
                if request["hook_event_name"] == "PreToolUse":
                    store.record_activity(
                        worker["attempt_id"], time.time(),
                        unsafe=request["tool_name"] not in READ_TOOLS,
                    )
            finally:
                store.close()
            env["FEISHU_PERMISSION_SOCKET_PATH"] = worker["config"].get("permission_socket", "")
            env["FEISHU_PERMISSION_ALL_SESSIONS"] = "true"
            # The CLI can only call enabled tools. User policy covers all of them,
            # including tools added later, after the execution identity is checked.
            allowed_tools = {request["tool_name"]}
        # Expanded policy only applies after authenticating the current worker.
        # Approval and side-effect/replay classification remain separate decisions.
        if request["hook_event_name"] == "PreToolUse":
            if request["tool_name"] in allowed_tools:
                sys.stdout.write(json.dumps({
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "permissionDecisionReason": "Allowed by the user's bot tool policy",
                    }
                }) + "\n")
            return 0
        if request["tool_name"] in allowed_tools:
            sys.stdout.write(json.dumps(_allow()) + "\n")
            return 0
        target_session = env.get("FEISHU_PERMISSION_SESSION_ID", "").strip()
        all_sessions = env.get("FEISHU_PERMISSION_ALL_SESSIONS", "false").lower() == "true"
        if not target_session and not all_sessions:
            raise ValueError("FEISHU_PERMISSION_SESSION_ID is required")
        if not all_sessions and request["session_id"] != target_session:
            return _delegate_to_flux(raw, env)
        payload = _relay_payload(request)
        if worker:
            payload.update(attempt_id=worker["attempt_id"], token=worker["token"])
        decision = _request_decision(payload, env)
    except Exception as exc:
        if authenticated:
            with contextlib.suppress(Exception):
                failed_store = RuntimeStore(Path(worker["config"]["database_path"]))
                try:
                    failed_store.authenticate(worker["attempt_id"], worker["token"])
                    failed_store.drain(worker["attempt_id"], "permission_policy_error")
                finally:
                    failed_store.close()
        if env.get("FEISHU_WORKER_REQUEST"):
            # Diagnose protocol/version mismatches without logging commands or credentials.
            with contextlib.suppress(Exception):
                from feishu_llm_bot.runtime_common import private_json

                private_json(Path(env["FEISHU_WORKER_REQUEST"]).parent / "permission-error.json", {
                    "error_type": type(exc).__name__, "reason": str(exc)[:200],
                    "fields": sorted(json.loads(raw)),
                })
        decision = _deny("Feishu permission relay unavailable; request denied")
        if request.get("hook_event_name") == "PreToolUse":
            decision = {"hookSpecificOutput": {
                "hookEventName": "PreToolUse", "permissionDecision": "deny",
                "permissionDecisionReason": "Execution no longer authorized or host unavailable",
            }}
    sys.stdout.write(json.dumps(decision, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
