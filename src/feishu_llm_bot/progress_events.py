"""Extract concise execution events, never reasoning or raw tool input/output."""
from __future__ import annotations

import json
import re
import shlex
from pathlib import Path
from typing import Any

_NAME = re.compile(r"[A-Za-z0-9_.:/-]{1,80}\Z")
_COMMANDS = {
    "git", "forge", "rg", "grep", "find", "ls", "pwd", "cat", "sed", "head", "tail",
    "python", "python3", "pytest", "ruff", "node", "npm", "uv", "pip", "curl",
    "codebase", "bytedcli", "lark-cli", "argos", "bash", "sleep", "systemctl",
}
_ACTIONS = {
    "status", "diff", "log", "show", "test", "check", "run", "build", "version",
    "code", "job", "compile", "get", "list", "create", "metrics", "config", "help",
}


def tool_label(name: str, inputs: object) -> str:
    name = name if _NAME.fullmatch(name) else "工具"
    inputs = inputs if isinstance(inputs, dict) else {}
    if name == "Skill":
        skill = inputs.get("skill", "")
        return f"Skill({skill})" if isinstance(skill, str) and _NAME.fullmatch(skill) else "Skill"
    if name == "Bash":
        command = inputs.get("command", "")
        try:
            words = shlex.split(command[:4096]) if isinstance(command, str) else []
        except ValueError:
            words = []
        # Never copy full commands, descriptions, environment values, or arguments.
        while words and "=" in words[0]:
            words.pop(0)
        program = Path(words[0]).name if words else ""
        if program in _COMMANDS:
            actions = []
            for word in words[1:4]:
                if word not in _ACTIONS:
                    break
                actions.append(word)
            return "Bash · " + " ".join([program, *actions])
        return "Bash · 执行命令"
    return {
        "Read": "Read · 读取文件", "Write": "Write · 写入文件",
        "Edit": "Edit · 修改文件", "Grep": "Grep · 搜索内容",
        "Glob": "Glob · 查找文件", "WebSearch": "WebSearch · 搜索资料",
        "WebFetch": "WebFetch · 读取网页", "Agent": "Agent · 子任务",
        "Task": "Task · 子任务", "mcp__feishu__read_image": "查看你发送的图片",
        "mcp__feishu__reply": "发送最终回复",
        "mcp__feishu__cli_help": "查询工具帮助",
        "mcp__feishu__search_documents": "搜索文档",
        "mcp__feishu__fetch_document": "读取文档正文",
        "mcp__feishu__create_document": "创建并核验文档",
        "mcp__feishu__operations": "查看已有操作和成果",
    }.get(name, name)


def result_warning(content: object) -> str | None:
    # Inspect only for known signatures. No output fragment is returned to Feishu.
    text = str(content)[:64 * 1024].lower()
    if "incorrect event name" in text:
        return "Hook 事件类型不匹配"
    if "hook error" in text or "hook failed" in text:
        return "Hook 执行异常"
    if "permission denied" in text or "permission request denied" in text:
        return "操作未获授权"
    return None


def envelope_correlation(entry: dict, mcp_pid: int | None) -> str | None:
    origin = entry.get("origin", {})
    if not isinstance(origin, dict) or origin.get("kind") != "peer":
        return None
    if mcp_pid is None or origin.get("verifiedPeerPid") != mcp_pid:
        return None
    content = entry.get("message", {}).get("content", [])
    if isinstance(content, str):
        content = [{"type": "text", "text": content}]
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "text":
            continue
        text = block.get("text", "")
        prefix = "Another Claude session sent a message:\n[Feishu bridge "
        if not isinstance(text, str) or not text.startswith(prefix):
            continue
        # The envelope is one JSON line; escaped user newlines cannot inject one.
        for line in text.splitlines():
            if line.startswith("FEISHU_ENVELOPE_JSON="):
                try:
                    envelope = json.loads(line.split("=", 1)[1])
                except ValueError:
                    return None
                cid = envelope.get("correlation_id") if isinstance(envelope, dict) else None
                if isinstance(cid, str) and re.fullmatch(r"fs_[0-9a-f]{32}", cid):
                    return cid
    return None


def apply_entry(state: dict[str, Any], entry: dict, now: float) -> None:
    kind = entry.get("type")
    message = entry.get("message", {})
    content = message.get("content", []) if isinstance(message, dict) else []
    if not isinstance(content, list):
        content = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if kind == "assistant" and block.get("type") == "tool_use":
            tool_id = block.get("id")
            if not isinstance(tool_id, str) or any(s["id"] == tool_id for s in state["steps"]):
                continue
            state["steps"].append({
                "id": tool_id[:160],
                "label": tool_label(str(block.get("name", "工具")), block.get("input")),
                "status": "running", "at": now,
            })
            state["steps"] = state["steps"][-8:]
            state.update(phase="working", activity_at=now)
        elif kind == "user" and block.get("type") == "tool_result":
            for step in state["steps"]:
                if step["id"] == block.get("tool_use_id"):
                    warning = result_warning(block.get("content"))
                    step["status"] = "error" if block.get("is_error") else "done"
                    if warning:
                        step["status"] = "error"
                        state["warning"] = warning
                    state["activity_at"] = now
                    state["phase"] = "working" if any(
                        s["status"] == "running" for s in state["steps"]
                    ) else "thinking"
    if kind == "system":
        if entry.get("subtype") == "turn_duration":
            state.update(phase="awaiting_reply", activity_at=now)
        elif entry.get("hookErrors") or entry.get("level") in {"error", "warning"}:
            state["warning"] = result_warning(entry.get("content")) or (
                "Hook 执行异常" if entry.get("hookErrors") else "会话报告异常，等待后续执行结果"
            )
            state["activity_at"] = now
    if kind == "assistant" and (entry.get("isApiErrorMessage") or entry.get("error")):
        state.update(warning="模型请求异常，等待重试或恢复", activity_at=now)
    attachment = entry.get("attachment", {})
    if (kind == "attachment" and isinstance(attachment, dict)
            and attachment.get("type") in {"hook_non_blocking_error", "hook_error"}):
        state.update(warning=result_warning(attachment) or "Hook 执行异常", activity_at=now)
