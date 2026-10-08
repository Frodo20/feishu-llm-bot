"""One recovery decision for controls, cards and final notifications."""


def recovery_options(db, cid, *, state=None):
    task = db.execute("SELECT state FROM runtime_tasks WHERE correlation_id=?", (cid,)).fetchone()
    state = state or (task[0] if task else None)
    unknown = [
        r[0]
        for r in db.execute(
            "SELECT operation_id FROM runtime_operations WHERE correlation_id=? "
            "AND state='unknown'",
            (cid,),
        )
    ]
    can_retry = state in {"failed", "suspended", "cancelled"} and not unknown
    actions = ["status", "result"]
    if state in {"running", "queued", "retry_wait"}:
        actions.append("cancel")
    if unknown:
        actions.append("verify")
    elif can_retry:
        actions.append("continue")
    return {
        "can_retry": can_retry,
        "needs_reconciliation": bool(unknown),
        "uncertain_operations": unknown,
        "available_actions": actions,
    }
