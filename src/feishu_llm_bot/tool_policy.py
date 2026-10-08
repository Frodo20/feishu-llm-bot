"""Explicit user policy for authenticated bot workers; separate from replay safety."""

import hashlib
import json
from pathlib import Path

POLICY_VERSION = "worker-auto-20260928"

MANAGED_TOOLS = frozenset(
    {
        "mcp__feishu__reply",
        "mcp__feishu__read_image",
        "mcp__feishu__operations",
        "mcp__feishu__run",
        "mcp__feishu__create_document",
        "mcp__feishu__cli_help",
        "mcp__feishu__search_documents",
        "mcp__feishu__fetch_document",
        "mcp__feishu__libra_read",
        "mcp__feishu__read_artifact",
    }
)
LOCAL_WORK_TOOLS = frozenset(
    {
        "Bash",
        "Read",
        "Glob",
        "Grep",
        "LS",
        "WebFetch",
        "WebSearch",
        "ListMcpResourcesTool",
        "ReadMcpResourceTool",
        "Edit",
        "Write",
        "NotebookEdit",
        "Skill",
    }
)
WORKER_AUTO_ALLOW_TOOLS = MANAGED_TOOLS | LOCAL_WORK_TOOLS


def validate_worker_policy(config):
    if config.get("worker_permission_policy", "auto") != "auto":
        raise ValueError("worker_permission_policy must be auto")


def worker_allowed_tools_argument(config=None):
    validate_worker_policy(config or {})
    return "--allowedTools=*"


def policy_manifest(config):
    from .backends import backend_name

    validate_worker_policy(config)
    project = Path(config["project_dir"])
    sources = sorted((project / "src/feishu_llm_bot").glob("*.py"))
    sources += [
        project / "scripts/claude_permission_hook.py",
        project / "node-channel/worker-server.mjs",
    ]
    digest = hashlib.sha256()
    for source in sources:
        digest.update(str(source.relative_to(project)).encode())
        digest.update(source.read_bytes())
    tools = sorted(WORKER_AUTO_ALLOW_TOOLS)
    return {
        "agent_backend": backend_name(config),
        "native_tool_access": config.get("worker_access", "workspace")
        if backend_name(config) == "traex" else "authenticated_auto",
        "permission_policy": "auto",
        "policy_version": POLICY_VERSION,
        "source_sha256": digest.hexdigest(),
        "module_path": str(Path(__file__).resolve()),
        "known_tools_sha256": hashlib.sha256(json.dumps(tools).encode()).hexdigest(),
        "known_tools": tools,
        "new_enabled_tools": "automatically_allowed",
    }
