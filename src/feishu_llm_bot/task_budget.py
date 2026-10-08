"""Shared monotonic budgets and deterministic-error limits for one worker attempt."""

import hashlib
import json
import time

FINAL_TOOLS = frozenset(
    {
        "mcp__feishu__reply",
        "mcp__feishu__operations",
        "mcp__feishu__read_artifact",
        "Read",
        "Glob",
        "Grep",
        "LS",
    }
)


def deadlines(seconds, reserve=90, now=None):
    now = time.monotonic() if now is None else now
    reserve = min(max(0, float(reserve)), seconds / 5)
    return {"deadline_monotonic": now + seconds, "soft_deadline_monotonic": now + seconds - reserve}


def finalization_reason(store, request):
    reason = store.meta("finalize:" + request["attempt_id"])
    if reason:
        return reason
    soft = request.get("soft_deadline_monotonic")
    if soft is not None and time.monotonic() >= soft:
        request_finalization(store, request, "soft_deadline")
        return "soft_deadline"
    return None


def request_finalization(store, request, reason):
    key = "finalize:" + request["attempt_id"]
    with store.transaction() as db:
        store.authenticate(request["attempt_id"], request["token"], db)
        if db.execute("SELECT 1 FROM runtime_meta WHERE key=?", (key,)).fetchone():
            return
        db.execute("INSERT INTO runtime_meta VALUES (?,?)", (key, json.dumps(reason)))
        store._audit(
            db, request["correlation_id"], request["attempt_id"], "finalization_requested", reason
        )


def bounded_timeout(request, timeout):
    soft = request.get("soft_deadline_monotonic")
    return timeout if soft is None else max(0.01, min(timeout, soft - time.monotonic()))


def record_failure(store, request, name, inputs, reason):
    # No raw commands or business input in the audit stream.
    digest = hashlib.sha256(
        json.dumps([name, inputs, reason], sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    with store.transaction() as db:
        store.authenticate(request["attempt_id"], request["token"], db)
        store._audit(db, request["correlation_id"], request["attempt_id"], "tool_failure", digest)
        count = db.execute(
            "SELECT count(*) FROM runtime_events WHERE attempt_id=? "
            "AND kind='tool_failure' AND detail=?",
            (request["attempt_id"], digest),
        ).fetchone()[0]
    if count >= 3:
        request_finalization(store, request, "repeated_tool_error")


def finish_message(reason):
    return (
        f"{reason}: stop starting new work. Use operations/read_artifact to inspect saved "
        "evidence, then return a final answer or reply with business_outcome=partial/unanswered. "
        "State missing evidence explicitly; do not retry the same failed call."
    )
