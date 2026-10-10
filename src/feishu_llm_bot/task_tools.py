"""Fenced typed tools and durable results for one supervised worker."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import sys
import time
from pathlib import Path

from . import libra_contracts, task_artifacts
from .attachments import AttachmentStore
from .command_runner import CleanupError, execute_command, read_payload
from .config import Settings
from .operation_contracts import (
    DEFAULT_CLI,
    READ_KINDS,
    READ_LIMITS,
    cli_environment,
    document_matches,
    read_argv,
    read_request,
    response_valid,
)
from .progress import read_environment
from .runtime_common import private_json
from .runtime_store import RuntimeStore
from .task_budget import bounded_timeout, finalization_reason, finish_message, record_failure
from .task_completion import account_output, collection_gate, save_checkpoint


def validated_payload(result, kind, semantic):
    payload = result.get("validated_response")
    if payload is None and result.get("exit_code") == 0:
        payload = read_payload(Path(result["stdout_path"]))
    if response_valid(kind, payload, semantic) and result.get("reason_code") not in {
        "invalid_response",
        "output_limit",
    }:
        return payload
    return None


def verify_document(cli, document_id, directory, env, expected_content=None):
    semantic = {"document_id": document_id}
    result = execute_command(
        read_argv(cli, "fetch_document", semantic),
        directory,
        timeout=30,
        env=cli_environment({}, env),
        result_validator=lambda p: response_valid("fetch_document", p, semantic),
    )
    payload = validated_payload(result, "fetch_document", semantic)
    if not payload or not document_matches(payload["data"]["document"], expected_content):
        raise ValueError("Document readback did not confirm expected ID, revision and content")
    return payload["data"]["document"]


def classify_read(result, kind, semantic):
    if kind == "libra_read":
        payload = None
        if result.get("reason_code") not in {"output_limit", "invalid_response", "spawn_failed"}:
            if semantic["action"] == "metric_search" and result.get("exit_code") == 0:
                payload = libra_contracts.csv_response(
                    Path(result["stdout_path"]).read_text(), semantic
                )
            elif result.get("exit_code") in {0, None}:
                raw = validated_payload(result, kind, semantic)
                if raw is not None:
                    payload = libra_contracts.response(raw, semantic)
        if payload is not None:
            result.update({k: v for k, v in payload.items() if k not in {"data", "rows"}})
            if "results may be incomplete" in result.get("stderr_tail", ""):
                result.update(coverage_complete=False,
                              warning="Some metric groups could not be read; results are partial")
            result.update(action=semantic["action"], retrieved_at=time.time(), retryable=False)
            return "succeeded"
        raw = read_payload(Path(result["stdout_path"])) if result.get("stdout_path") else None
        return libra_contracts.classify_failure(result, raw)
    if kind == "cli_help":
        text = result.get("output_tail", "")
        good = (
            result.get("exit_code") == 0
            and not result.get("background_children")
            and ("Usage:" in text and "Options:" in text)
        )
    else:
        payload = validated_payload(result, kind, semantic)
        good = payload is not None
        if good:
            result["validated_response"] = payload
    if good:
        result["retryable"] = False
        return "succeeded"
    payload = read_payload(Path(result["stdout_path"])) if result.get("stdout_path") else None
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    code = str(error.get("code", "")).lower() if isinstance(error, dict) else ""
    reason = result.get("reason_code")
    if code in {"unauthorized", "unauthenticated", "token_expired", "needs_auth", "401"}:
        reason = "needs_auth"
    elif code in {"forbidden", "access_denied", "permission_denied", "403"}:
        reason = "access_denied"
    elif code == "lark_cli_install_failed":
        reason = "runtime_unavailable"
    reason = reason or "read_failed"
    result.update(
        reason_code=reason,
        retryable=reason
        not in {
            "needs_auth",
            "access_denied",
            "output_limit",
            "invalid_response",
            "spawn_failed",
            "runtime_unavailable",
        },
    )
    return "failed"


def invoke(request_path, name, inputs):
    result = _invoke(request_path, name, inputs)
    if name in {"libra_read", "read_artifact", "read_evidence", "operations"}:
        request = json.loads(Path(request_path).read_text())
        store = RuntimeStore(Path(request["config"]["database_path"]))
        try:
            result = account_output(store, request, name, result)
        finally:
            store.close()
    return result


def _invoke(request_path, name, inputs):
    request = json.loads(Path(request_path).read_text())
    config = request["config"]
    store = RuntimeStore(Path(config["database_path"]))
    aid, token = request["attempt_id"], request["token"]
    try:
        attempt = store.authenticate(aid, token)
        from .backends import enabled_integrations

        integrations = enabled_integrations(config)
        if ((name in {"create_document", "cli_help", "search_documents", "fetch_document"}
             and "documents" not in integrations)
                or (name in {"libra_read", "read_evidence"} and "libra" not in integrations)):
            raise ValueError("This integration is not enabled for this instance")
        cid = attempt["correlation_id"]
        if not isinstance(inputs, dict):
            raise ValueError("Tool arguments must be an object")
        if name in {"reply", "read_image"} and inputs.get("correlation_id") != cid:
            raise PermissionError("The requested task is not this execution")
        if name == "reply":
            store.submit_answer(
                aid, token, inputs["text"], inputs.get("business_outcome", "completed"),
                completion_scope=inputs.get("completion_scope", "verified"),
                evidence_gaps=inputs.get("evidence_gaps", []),
            )
            return {
                "status": "Final answer durably saved; host will deliver it after execution ends."
            }
        if name == "checkpoint":
            return save_checkpoint(store, request, inputs)
        if name == "operations":
            return task_artifacts.operations(store, cid, **inputs)
        if name in {"read_artifact", "read_evidence"}:
            gate = collection_gate(store, request, name, inputs)
            if gate:
                return gate
            reader = getattr(task_artifacts, name)
            return reader(store, request, request_path, inputs)
        if name == "libra_read" and inputs.get("action") == "help":
            return {"contract_version": libra_contracts.CONTRACT_VERSION,
                    "actions": libra_contracts.ACTIONS,
                    "instructions": "Use typed arguments; arrays must be JSON arrays. "
                    "Use aligned date windows and inspect actual metric definitions. "
                    "If the last experiment day is incomplete, compare earlier full-day windows."}
        reason = finalization_reason(store, request)
        if reason:
            return {"state": "failed", "reason_code": reason, "retryable": False,
                    "message": finish_message(reason)}
        if name == "read_image":
            settings = Settings.from_env(read_environment(Path(config["bridge_env_file"])))
            event = store.get_by_correlation(cid)
            if event.message_type != "image" or not event.attachment_token:
                raise ValueError("No image belongs to this task")
            attachments = AttachmentStore(
                settings.attachment_path,
                max_image_bytes=settings.max_image_bytes,
                max_image_pixels=settings.max_image_pixels,
                max_image_side=settings.max_image_side,
                max_total_bytes=settings.max_attachment_bytes_total,
            )
            data = attachments.read(
                event.attachment_token,
                expected_size=event.attachment_size,
                expected_sha256=event.attachment_sha256,
            )
            store.image_read(aid, token)
            return {"image": base64.b64encode(data).decode(), "mime_type": event.attachment_mime}
        if name not in READ_KINDS | {"run", "create_document"}:
            raise ValueError("Unknown tool")
        if name in READ_KINDS:
            semantic, execution = read_request(name, inputs)
            timeout = execution["timeout_seconds"]
            gate = collection_gate(store, request, name, inputs)
            if gate:
                return gate
        elif name == "run":
            command, timeout = inputs.get("command"), inputs.get("timeout_seconds", 120)
            if (
                not isinstance(command, str)
                or not 1 <= len(command) <= 100_000
                or type(timeout) is not int
                or not 1 <= timeout <= 300
            ):
                raise ValueError("Invalid command or timeout")
            semantic = {}
        else:
            title, content = inputs.get("title"), inputs.get("content")
            if (
                not isinstance(title, str)
                or not 1 <= len(title) <= 200
                or not isinstance(content, str)
                or not 1 <= len(content) <= 500_000
            ):
                raise ValueError("A title and Markdown content within size limits are required")
            semantic, timeout = {}, 300
        key = inputs.get("operation_key")
        env, cli = cli_environment(config), config.get("bytedcli_command", DEFAULT_CLI)
        if name == "libra_read":
            cli = config.get("libra_cli_command", libra_contracts.DEFAULT_LIBRA_CLI)
            env["LIBRA_CLI_METRIC_SEARCH_CACHE_DIR"] = str(
                Path(request_path).parent / "libra-cache"
            )
        max_tries = READ_LIMITS[name][1] if name in READ_KINDS else 1
        # The outer worker's monotonic hard deadline also bounds retries and readback.
        for retry in range(max_tries):
            reason = finalization_reason(store, request)
            if reason:
                return {"state": "failed", "reason_code": reason, "retryable": False,
                        "message": finish_message(reason)}
            op = store.operation_begin(aid, token, key, name, inputs)
            if op["state"] == "succeeded":
                return task_artifacts.public_result(
                    {"state": "succeeded", **json.loads(op["result"])}, op["operation_id"]
                )
            op_aid = op["operation_attempt_id"]
            directory = Path(request_path).parent / "operations" / op["operation_id"] / op_aid
            directory.mkdir(parents=True, mode=0o700)
            result, state = {}, "failed" if name in READ_KINDS else "unknown"

            def checkpoint(
                receipt, saved=result, folder=directory, operation=op, attempt_id=op_aid
            ):
                if name == "create_document":
                    receipt["document"] = receipt["validated_response"]["data"]["document"]
                saved.update(receipt)
                private_json(folder / "result.json", saved)
                store.operation_checkpoint(aid, token, operation["operation_id"], attempt_id, saved)

            try:
                if name in READ_KINDS:
                    result = execute_command(
                        read_argv(cli, name, semantic),
                        directory,
                        timeout=bounded_timeout(request, timeout),
                        env=env,
                        result_validator=(
                            None
                            if name == "cli_help" or (
                                name == "libra_read" and semantic["action"] == "metric_search"
                            )
                            else lambda p: response_valid(name, p, semantic)
                        ),
                        on_result=checkpoint,
                    )
                    state = classify_read(result, name, semantic)
                elif name == "run":
                    result = execute_command(
                        ["/bin/bash", "-c", command], directory,
                        timeout=bounded_timeout(request, timeout), env=env
                    )
                    state = (
                        "succeeded"
                        if (result["exit_code"] == 0 and not result.get("background_children"))
                        else "unknown"
                    )
                else:
                    source = directory / "document.md"
                    with open(source, "w", opener=lambda p, f: os.open(p, f, 0o600)) as stream:
                        stream.write(content)
                    executed = execute_command(
                        [
                            cli,
                            "--json",
                            "lark",
                            "docs",
                            "create",
                            "--as",
                            "user",
                            "--parent-position",
                            "my_library",
                            "--title",
                            title,
                            "--doc-format",
                            "markdown",
                            "--content",
                            "@" + str(source),
                        ],
                        directory,
                        env=env,
                        timeout=bounded_timeout(request, timeout),
                        result_validator=lambda p: response_valid(name, p, semantic),
                        on_result=checkpoint,
                    )
                    result.update(executed)
                    payload = validated_payload(result, name, semantic)
                    if payload:
                        result["document"] = payload["data"]["document"]
                        private_json(directory / "result.json", result)
                        store.operation_checkpoint(aid, token, op["operation_id"], op_aid, result)
                        verification = directory / "verification"
                        verification.mkdir(mode=0o700)
                        verified = verify_document(
                            cli, result["document"]["document_id"], verification, env, content
                        )
                        result.update(verified=True, verified_revision_id=verified["revision_id"])
                        state = "succeeded"
                    else:
                        result["reason_code"] = "write_receipt_missing"
            except CleanupError:
                store.drain(aid, "process_cleanup_failed")
                raise
            except (ValueError, OSError, RuntimeError) as exc:
                result.update(
                    reason_code="verification_failed"
                    if result.get("document")
                    else "command_error",
                    retryable=False,
                    error_type=type(exc).__name__,
                )
            private_json(directory / "result.json", result)
            store.operation_end(
                aid, token, op["operation_id"], state, result, operation_attempt_id=op_aid
            )
            if state != "failed" or not result.get("retryable") or retry + 1 == max_tries:
                if state in {"failed", "unknown"}:
                    record_failure(store, request, name, inputs, result.get("reason_code", state))
                return task_artifacts.public_result({"state": state, **result}, op["operation_id"])
            # Short bounded backoff; subsequent begin authenticates again after cancellation.
            delay = (1 if name == "libra_read" else 0.2) * (retry + 1)
            time.sleep(min(delay, bounded_timeout(request, delay)))
    except ValueError as exc:
        # CLI/parser errors are actionable without recording an unknown operation.
        record_failure(store, request, name, inputs, str(exc))
        raise
    finally:
        store.close()


def main():
    try:
        raw = sys.stdin.buffer.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("Tool request too large")
        data = json.loads(raw)
        result = invoke(
            os.environ["FEISHU_WORKER_REQUEST"], data["name"], data.get("arguments", {})
        )
        print(json.dumps({"result": result}, ensure_ascii=False))
    except Exception as exc:
        # No traceback or environment values go back through MCP.
        with contextlib.suppress(BrokenPipeError):
            print(json.dumps({"error": str(exc)[:500]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
