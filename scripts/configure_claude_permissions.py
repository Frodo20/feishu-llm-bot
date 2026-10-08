#!/usr/bin/env python3
"""Install the user's broad local permissions and a single Feishu approval hook."""
from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

AUTO_TOOLS = [
    "Bash", "Read", "Glob", "Grep", "LS", "WebFetch", "WebSearch",
    "ListMcpResourcesTool", "ReadMcpResourceTool",
    "mcp__feishu__reply", "mcp__feishu__read_image",
]


def configure(settings: dict, project: Path) -> dict:
    settings = json.loads(json.dumps(settings))
    permissions = settings.setdefault("permissions", {})
    permissions["allow"] = list(dict.fromkeys([
        *permissions.get("allow", []), *AUTO_TOOLS, "Edit", "Write", "NotebookEdit", "Skill",
    ]))
    permissions["defaultMode"] = "acceptEdits"
    python = project / ".venv/bin/python"
    script = project / "scripts/claude_permission_hook.py"
    command = f"{shlex.quote(str(python))} {shlex.quote(str(script))}"
    hooks = settings.setdefault("hooks", {})
    # The target-aware router delegates other sessions to Flux itself. Registering
    # Flux separately in PermissionRequest makes two simultaneous approval gates.
    kept = []
    for group in hooks.get("PermissionRequest", []):
        remaining = [h for h in group.get("hooks", []) if not any(
            name in h.get("command", "")
            for name in ("claude_permission_hook.py", "flux-hooks-claude")
        )]
        if remaining:
            kept.append({**group, "hooks": remaining})
    kept.append({"matcher": "*", "hooks": [{
        "type": "command", "timeout": 620,
        "command": (
            "FEISHU_PERMISSION_ALL_SESSIONS=true "
            "FEISHU_PERMISSION_SOCKET_PATH="
            + shlex.quote(str(Path.home() / ".local/state/feishu-llm-bot/permission/relay.sock"))
            + f" FEISHU_PERMISSION_TIMEOUT_SECONDS=610 {command}"
        ),
    }]})
    hooks["PermissionRequest"] = kept
    kept = []
    for group in hooks.get("PreToolUse", []):
        remaining = [h for h in group.get("hooks", [])
                     if "claude_permission_hook.py" not in h.get("command", "")]
        if remaining:
            kept.append({**group, "hooks": remaining})
    kept.insert(0, {"matcher": "|".join(AUTO_TOOLS), "hooks": [{
        "type": "command", "command": command, "timeout": 10,
    }]})
    hooks["PreToolUse"] = kept
    return settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", type=Path,
                        default=Path.home() / ".claude/settings.json")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    settings = json.loads(args.settings.read_text())
    result = configure(settings, Path(__file__).resolve().parents[1])
    if args.apply:
        temporary = args.settings.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        temporary.chmod(0o600)
        temporary.replace(args.settings)
        print(f"Updated {args.settings}")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
