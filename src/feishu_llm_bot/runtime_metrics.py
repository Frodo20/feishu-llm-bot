"""Read-only aggregate evidence; no prompts, URLs or credentials leave this report."""

import json
from collections import Counter


def runtime_metrics(db):
    categories, outcomes = Counter(), Counter()
    business_total = first_completed = manual_recovery = 0
    for task in db.execute("SELECT * FROM runtime_tasks"):
        categories[task["task_category"]] += 1
        if (
            task["task_category"] not in {"business", "experiment_analysis"}
            or not task["business_outcome"]
        ):
            continue
        outcomes[task["business_outcome"]] += 1
        business_total += 1
        cid = task["correlation_id"]
        continued = db.execute(
            "SELECT 1 FROM runtime_events WHERE correlation_id=? AND kind='continue_requested'",
            (cid,),
        ).fetchone()
        manual_recovery += bool(continued)
        first = db.execute(
            "SELECT business_outcome,state FROM runtime_attempts "
            "WHERE correlation_id=? ORDER BY started_at LIMIT 1",
            (cid,),
        ).fetchone()
        if first and first[0] == "completed" and first[1] == "succeeded" and not continued:
            first_completed += 1
    failures, exits, durations = Counter(), [], []
    attempts = db.execute("SELECT * FROM runtime_operation_attempts").fetchall()
    for attempt in attempts:
        if attempt["failure_reason"]:
            failures[attempt["failure_reason"]] += 1
        result = json.loads(attempt["result"] or "{}")
        if result.get("process_seconds") is not None:
            durations.append(result["process_seconds"])
        if result.get("result_seconds") is not None and result.get("process_seconds") is not None:
            exits.append(max(0, result["process_seconds"] - result["result_seconds"]))
    delivery = dict(db.execute("SELECT state,count(*) FROM runtime_outbox GROUP BY state"))
    delays = [
        row[0]
        for row in db.execute(
            "SELECT max(0,o.delivered_at-r.created_at) FROM runtime_outbox o "
            "JOIN runtime_results r USING(result_id) WHERE o.state='sent'"
        )
    ]
    repeats = db.execute(
        "SELECT count(*) FROM (SELECT operation_id FROM "
        "runtime_operation_attempts GROUP BY operation_id HAVING count(*)>1)"
    ).fetchone()[0]
    return {
        "task_categories": dict(categories),
        "business_outcomes": dict(outcomes),
        "finalized_business_tasks": business_total,
        "first_attempt_completed": first_completed,
        "first_attempt_completion_rate": first_completed / business_total
        if business_total
        else None,
        "manually_continued_business_tasks": manual_recovery,
        "delivery": delivery,
        "operation_attempts": len(attempts),
        "retried_operations": repeats,
        "operation_failure_reasons": dict(failures),
        "result_to_exit_seconds": summary(exits),
        "delivery_delay_seconds": summary(delays),
        "cli_seconds": summary(durations),
        "permission_wait_seconds": summary(
            [
                max(0, row[0])
                for row in db.execute(
                    "SELECT resolved_at-created_at FROM permission_requests "
                    "WHERE resolved_at IS NOT NULL"
                )
            ]
        ),
        "permission_requests": db.execute("SELECT count(*) FROM permission_requests").fetchone()[0],
        "finalization_reasons": dict(
            db.execute(
                "SELECT detail,count(*) FROM runtime_events WHERE kind='finalization_requested' "
                "GROUP BY detail"
            )
        ),
    }


def summary(values):
    return {
        "count": len(values),
        "mean": sum(values) / len(values) if values else None,
        "max": max(values) if values else None,
    }
