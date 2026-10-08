"""Feishu JSON 1.0 approval cards. No CLI commands, secrets, or file contents."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .store import PermissionRequestRecord


def permission_card(
    record: PermissionRequestRecord, *, cwd_context: str = "", token: str | None = None,
) -> dict[str, Any]:
    pending = record.status == "pending" and token is not None
    labels = {"allowed": "已同意", "denied": "已拒绝", "expired": "已过期"}
    title = "Claude 请求授权" if pending else f"Claude 授权 · {labels.get(record.status, '已结束')}"
    expires = datetime.fromtimestamp(record.expires_at, ZoneInfo("Asia/Shanghai"))
    details = [f"工具：{record.tool_name}", f"范围：{record.summary}"]
    if cwd_context:
        details.append(f"目录：{cwd_context}")
    details.extend([f"会话：{record.session_id}", f"有效至：{expires:%m-%d %H:%M:%S}（北京时间）"])
    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "plain_text", "content": "\n".join(details)}}
    ]
    if pending:
        elements.append({
            "tag": "action",
            "actions": [
                {
                    "tag": "button", "text": {"tag": "plain_text", "content": label},
                    "type": style,
                    "value": {"kind": "claude_permission", "request_id": record.request_id,
                              "token": token, "decision": decision},
                }
                for label, style, decision in [
                    ("同意一次", "primary", "allowed"), ("拒绝", "danger", "denied"),
                ]
            ],
        })
    elements.append({
        "tag": "note", "elements": [{"tag": "plain_text", "content": (
            "仅对本次操作有效；超时自动拒绝。" if pending
            else "本次请求已结束，按钮已关闭。"
        )}],
    })
    return {
        "config": {"wide_screen_mode": True, "update_multi": True, "enable_forward": False},
        "header": {"template": "blue" if pending else (
            "green" if record.status == "allowed" else "grey"
        ), "title": {"tag": "plain_text", "content": title}},
        "elements": elements,
    }
