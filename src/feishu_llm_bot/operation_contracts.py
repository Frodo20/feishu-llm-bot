"""Host-owned CLI contracts. Shell text and model flags never determine effect safety."""

from __future__ import annotations

import os
import re

from . import libra_contracts

READ_KINDS = frozenset({"cli_help", "search_documents", "fetch_document", "libra_read"})
READ_LIMITS = {"cli_help": (10, 2), "search_documents": (30, 3), "fetch_document": (30, 3),
               "libra_read": (60, 3)}
DEFAULT_CLI = "bytedcli"


def cli_environment(config, base=None):
    env = dict(os.environ if base is None else base)
    env["PATH"] = config.get("path", env.get("PATH", "/usr/bin:/bin"))
    env.update(config.get("agent_environment", {}))
    # Host policy applies to the worker and every managed CLI/readback/admin subprocess.
    env["BYTEDCLI_NO_AUTO_UPGRADE"] = "1"
    env["BYTEDCLI_TRACKING_DISABLED"] = "1"
    if config.get("worker_runner") == "process":
        env["FEISHU_WORKER_RUNNER"] = "process"
    return env


def read_request(kind, inputs):
    """Return canonical semantic inputs and bounded execution options, before any intent."""
    if kind == "libra_read":
        return libra_contracts.request(inputs)
    fields = {
        "cli_help": {"topic"},
        "search_documents": {"query", "page_size"},
        "fetch_document": {"document_id"},
    }[kind]
    if set(inputs) - fields - {"operation_key", "timeout_seconds"}:
        raise ValueError("Unsupported read arguments")
    timeout = inputs.get("timeout_seconds", READ_LIMITS[kind][0])
    if type(timeout) is not int or not 1 <= timeout <= 60:
        raise ValueError("Read timeout must be an integer within 1-60 seconds")
    if kind == "cli_help":
        topic = inputs.get("topic")
        if topic not in {"search", "fetch", "create"}:
            raise ValueError("Unknown CLI help topic")
        semantic = {"topic": topic}
    elif kind == "search_documents":
        query, size = inputs.get("query"), inputs.get("page_size", 20)
        if not isinstance(query, str) or not query.strip() or len(query) > 2000:
            raise ValueError("A nonempty query within 2000 characters is required")
        if type(size) is not int or not 1 <= size <= 20:
            raise ValueError("page_size must be within 1-20")
        semantic = {"query": query, "page_size": size}
    else:
        ident = inputs.get("document_id")
        if not isinstance(ident, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", ident):
            raise ValueError("A document ID (not a shell command or URL) is required")
        semantic = {"document_id": ident}
    return semantic, {"timeout_seconds": timeout}


def read_argv(cli, kind, semantic):
    if kind == "libra_read":
        return libra_contracts.argv(cli, semantic)
    if kind == "cli_help":
        return [cli, "lark", "docs", semantic["topic"], "--help"]
    if kind == "search_documents":
        return [
            cli,
            "--json",
            "lark",
            "docs",
            "search",
            "--as",
            "user",
            "--query",
            semantic["query"],
            "--page-size",
            str(semantic["page_size"]),
        ]
    return [
        cli,
        "--json",
        "lark",
        "docs",
        "fetch",
        "--as",
        "user",
        "--doc",
        semantic["document_id"],
        "--doc-format",
        "markdown",
    ]


def valid_document(document):
    return (
        isinstance(document, dict)
        and isinstance(document.get("document_id"), str)
        and bool(re.fullmatch(r"[A-Za-z0-9_-]{1,200}", document["document_id"]))
    )


def response_valid(kind, payload, semantic):
    if kind == "libra_read":
        return libra_contracts.response(payload, semantic) is not None
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    if kind == "search_documents":
        return isinstance(data.get("results"), list) and all(
            isinstance(item, dict) for item in data["results"]
        )
    doc = data.get("document")
    if not valid_document(doc):
        return False
    if kind == "create_document":
        return isinstance(doc.get("url"), str) and doc["url"].startswith("https://")
    return doc["document_id"] == semantic["document_id"] and isinstance(doc.get("content"), str)


def document_matches(document, expected_content):
    """Markdown readback may normalize whitespace, but must retain all expected text."""

    def normalize(text):
        return re.sub(r"\s+", " ", text).strip()

    content = document.get("content")
    revision = document.get("revision_id")
    if not isinstance(content, str) or not content.strip() or revision in (None, ""):
        return False
    return expected_content is None or normalize(content) == normalize(expected_content)
