"""Bounded views of the current task's recorded command outputs."""

import json
from pathlib import Path


def operation_summary(op):
    result = json.loads(op["result"] or "{}")
    return {
        "operation_id": op["operation_id"],
        "operation_key": op["operation_key"],
        "kind": op["kind"],
        "state": op["state"],
        "effect_kind": op["effect_kind"],
        "updated_at": op["updated_at"],
        "result": {
            key: result[key]
            for key in (
                "reason_code",
                "retryable",
                "exit_code",
                "process_seconds",
                "format",
                "data_available",
                "row_count",
                "retrieved_at",
                "action",
                "statistics_status",
                "coverage_complete",
            )
            if key in result
        },
        "artifacts": [name for name in ("stdout", "stderr") if result.get(name + "_path")],
    }


def operations(store, cid, *, offset=0, limit=20):
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError("operations offset >= 0 and limit within 1-50 are required")
    rows = store.operations(cid)
    return {
        "operations": [operation_summary(op) for op in rows[offset : offset + limit]],
        "total": len(rows),
        "next_offset": offset + limit if offset + limit < len(rows) else None,
    }


def read_artifact(store, request, request_path, inputs):
    if set(inputs) - {"operation_id", "artifact", "offset", "limit"}:
        raise ValueError("Unsupported artifact arguments")
    ident, artifact = inputs.get("operation_id"), inputs.get("artifact", "stdout")
    offset, limit = inputs.get("offset", 0), inputs.get("limit", 8000)
    if artifact not in {"stdout", "stderr"}:
        raise ValueError("artifact must be stdout or stderr")
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 16000:
        raise ValueError("artifact byte offset >= 0 and limit within 1-16000 are required")
    op = next(
        (op for op in store.operations(request["correlation_id"]) if op["operation_id"] == ident),
        None,
    )
    if op is None:
        raise PermissionError("No such operation in this task")
    result = json.loads(op["result"] or "{}")
    if not result.get(artifact + "_path"):
        raise ValueError("This operation has no saved artifact")
    path = Path(result[artifact + "_path"]).resolve(strict=True)
    roots = [
        Path(request_path).parent / "operations",
        Path(store.path).parent / "tasks" / request["correlation_id"],
    ]
    if not any(
        path.is_relative_to(root.resolve()) and ident in path.relative_to(root.resolve()).parts
        for root in roots
    ):
        raise PermissionError("Artifact is outside this task's operation directories")
    if path.name != artifact + ".txt" or not path.is_file():
        raise PermissionError("Invalid artifact file")
    with path.open("rb") as stream:
        stream.seek(offset)
        data = stream.read(limit)
    size = path.stat().st_size
    return {
        "operation_id": ident,
        "artifact": artifact,
        "offset": offset,
        "text": data.decode("utf-8", errors="replace"),
        "total_bytes": size,
        "next_offset": offset + len(data) if offset + len(data) < size else None,
    }


def public_result(result, op_id):
    view = {"operation_id": op_id, **result}
    if "evidence" in view:
        # Statistics are already projected to the requested metrics. Raw JSON stays on disk.
        for key in ("validated_response", "output_tail", "response_preview"):
            view.pop(key, None)
        view["read_full_output"] = "Use read_evidence for more rows; read_artifact for raw JSON"
    if "validated_response" in view:
        encoded = json.dumps(view["validated_response"], ensure_ascii=False)
        if len(encoded) > 8000:
            view.pop("validated_response")
            view["response_preview"] = encoded[:4000]
            view["read_full_output"] = "Use read_artifact with this operation_id"
    for key in ("output_tail", "stderr_tail"):
        if key in view:
            view[key] = view[key][-4000:]
    return view


def read_evidence(store, request, request_path, inputs):
    from .libra_evidence import statistics

    if set(inputs) - {"operation_id", "offset", "limit"}:
        raise ValueError("Unsupported evidence arguments")
    offset, limit = inputs.get("offset", 0), inputs.get("limit", 12)
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("evidence offset >= 0 and limit within 1-20 are required")
    ident = inputs.get("operation_id")
    # Reuse the existing task/path authorization before opening the whole bounded artifact.
    checked = read_artifact(store, request, request_path, {"operation_id": ident, "limit": 1})
    if checked["total_bytes"] > 16 * 1024 * 1024:
        raise ValueError("Artifact is too large for structured evidence")
    op = next(o for o in store.operations(request["correlation_id"]) if o["operation_id"] == ident)
    semantic = json.loads(op["request"])
    if op["kind"] != "libra_read" or semantic.get("action") != "report_data":
        raise ValueError("read_evidence requires a Libra report_data operation")
    result = json.loads(op["result"])
    payload = json.loads(Path(result["stdout_path"]).read_text())
    body = payload.get("data", {})
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or body.get("code") not in (0, 200):
        raise ValueError("No successful statistics response in artifact")
    return {"operation_id": ident, **statistics(data, semantic["arguments"],
                                               offset=offset, limit=limit)}
