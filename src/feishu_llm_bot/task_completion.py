"""Durable partial findings and bounded evidence collection, scoped to one attempt."""

import json
import time


def _get(db, key, default=None):
    row = db.execute("SELECT value FROM runtime_meta WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def _put(db, key, value):
    db.execute("INSERT OR REPLACE INTO runtime_meta VALUES (?,?)",
               (key, json.dumps(value, ensure_ascii=False)))


def mark_finalization(db, aid, cid, reason, now=None):
    key = "finalize:" + aid
    if _get(db, key):
        return
    _put(db, key, reason)
    db.execute("INSERT INTO runtime_events(correlation_id,attempt_id,kind,created_at,detail) "
               "VALUES (?,?, 'finalization_requested',?,?)", (cid, aid, now or time.time(), reason))
    db.execute("UPDATE runtime_tasks SET warning=? WHERE attempt_id=?",
               ("正在整理已确认结果，准备交付。", aid))


def evidence_gaps(db, cid):
    gaps = []
    seen = set()
    for row in db.execute("SELECT request,result FROM runtime_operations "
                          "WHERE correlation_id=? AND kind='libra_read' AND state='succeeded' "
                          "ORDER BY updated_at DESC, operation_id DESC",
                          (cid,)):
        result = json.loads(row["result"] or "{}")
        query = json.loads(row["request"])
        if query.get("action") != "report_data":
            continue
        args = query.get("arguments", {})
        signature = json.dumps(args, sort_keys=True)
        if signature in seen:
            continue
        seen.add(signature)
        if result.get("statistics_status") in {"missing", "partial", "unknown"}:
            gaps.append(f"实验 {args.get('experiment_id', '')} 在 "
                        f"{args.get('start_date', '')} 至 {args.get('end_date', '')} "
                        "的查询指标缺少完整统计证据，仍需核对数据及查询口径。")
    return gaps[:20]


def validate_gaps(gaps):
    if (not isinstance(gaps, list) or len(gaps) > 20
            or any(not isinstance(x, str) or not 1 <= len(x) <= 500 for x in gaps)):
        raise ValueError("evidence_gaps: at most 20 nonempty strings, up to 500 characters each")
    return gaps


def save_checkpoint(store, request, inputs):
    if (set(inputs) - {"text", "evidence_gaps", "correlation_id"}
            or not {"text", "evidence_gaps"} <= set(inputs)):
        raise ValueError("checkpoint requires text and evidence_gaps")
    text = inputs["text"]
    if not isinstance(text, str) or not text.strip() or len(text) > 8000:
        raise ValueError("checkpoint text must be nonempty and at most 8000 characters")
    gaps = validate_gaps(inputs["evidence_gaps"])
    aid = request["attempt_id"]
    with store.transaction() as db:
        attempt = store.authenticate(aid, request["token"], db)
        if ("correlation_id" in inputs
                and inputs["correlation_id"] != attempt["correlation_id"]):
            raise PermissionError("The checkpoint belongs to a different task")
        event = db.execute("SELECT message_type FROM events WHERE correlation_id=?",
                           (attempt["correlation_id"],)).fetchone()
        if event["message_type"] == "image" and not attempt["image_read"]:
            raise ValueError("Read the attached image before saving findings")
        budget = _get(db, "evidence_budget:" + aid, {})
        _put(db, "checkpoint:" + aid, {
            "text": text, "evidence_gaps": gaps, "queries": budget.get("queries", 0),
        })
    return {"status": "Stage findings saved. These are partial, not a final answer."}


def collection_gate(store, request, name, inputs):
    """Reserve query capacity atomically; successful receipt reuse costs no remote query."""
    if name not in {"libra_read", "read_artifact", "read_evidence"}:
        return None
    if name == "libra_read" and inputs.get("action") == "help":
        return None
    aid, config = request["attempt_id"], request.get("config", {})
    with store.transaction() as db:
        attempt = store.authenticate(aid, request["token"], db)
        budget = _get(db, "evidence_budget:" + aid, {})
        reason = None
        if budget.get("characters", 0) >= config.get("max_evidence_characters", 120000):
            reason = "evidence_budget"
        elif name == "libra_read":
            existing = db.execute("SELECT state FROM runtime_operations WHERE correlation_id=? "
                                  "AND operation_key=?", (attempt["correlation_id"],
                                                          inputs.get("operation_key"))).fetchone()
            if not existing or existing["state"] != "succeeded":
                count = budget.get("queries", 0)
                if count >= config.get("max_analysis_queries", 24):
                    reason = "query_budget"
                else:
                    args = inputs.get("arguments", {})
                    missing = 0
                    if inputs.get("action") == "report_data":
                        for previous in db.execute(
                            "SELECT request,result FROM runtime_operations "
                            "WHERE correlation_id=? AND attempt_id=? "
                            "AND kind='libra_read' AND state='succeeded'",
                            (attempt["correlation_id"], aid),
                        ):
                            query, result = json.loads(previous[0]), json.loads(previous[1] or "{}")
                            prior = query.get("arguments", {})
                            if (result.get("statistics_status") == "missing"
                                    and all(prior.get(k) == args.get(k) for k in (
                                        "experiment_id", "metric_group", "selected_metric_ids",
                                    ))):
                                missing += 1
                    if missing >= 2:
                        return {"state": "evidence_missing", "message":
                                "These metrics were missing in two saved queries. Stop varying "
                                "parameters; disclose the gap and continue independent evidence. "
                                "A later explicit task continuation can recheck data availability."}
                    checkpoint = _get(db, "checkpoint:" + aid, {})
                    if count - checkpoint.get("queries", 0) >= config.get("checkpoint_every", 6):
                        return {"state": "needs_checkpoint", "message":
                                "Save confirmed findings with checkpoint(text, evidence_gaps) "
                                "before more queries. You can read saved evidence or reply now."}
                    budget["queries"] = count + 1
                    _put(db, "evidence_budget:" + aid, budget)
        if reason:
            mark_finalization(db, aid, attempt["correlation_id"], reason)
            return {"state": "finalizing", "reason_code": reason, "message":
                    "Evidence collection budget reached. Use your checkpoint and saved findings "
                    "to reply now; describe remaining evidence gaps."}
    return None


def account_output(store, request, name, result):
    if name not in {"libra_read", "read_artifact", "read_evidence", "operations"}:
        return result
    aid = request["attempt_id"]
    with store.transaction() as db:
        attempt = store.authenticate(aid, request["token"], db)
        key = "evidence_budget:" + aid
        budget = _get(db, key, {})
        budget["characters"] = budget.get("characters", 0) + len(
            json.dumps(result, ensure_ascii=False))
        _put(db, key, budget)
        if budget["characters"] >= request.get("config", {}).get("max_evidence_characters", 120000):
            mark_finalization(db, aid, attempt["correlation_id"], "evidence_budget")
    return result


def apply_completion_contract(db, cid, text, outcome, scope, gaps):
    if scope not in {"verified", "advisory"}:
        raise ValueError("completion_scope must be verified or advisory")
    gaps = list(dict.fromkeys([*validate_gaps(gaps), *evidence_gaps(db, cid)]))
    if gaps:
        text += "\n\n证据缺口：\n" + "\n".join("- " + gap for gap in gaps)
        if outcome == "completed":
            if scope == "verified":
                outcome = "partial"
            else:
                text += "\n本次完成的是建议；上述缺口尚未核验，不能作为定量验收或上线依据。"
    return text, outcome
