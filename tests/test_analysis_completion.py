import json
import time

import pytest

from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.libra_contracts import classify_failure, request, response
from feishu_llm_bot.libra_evidence import statistics
from feishu_llm_bot.orchestrator import Orchestrator
from feishu_llm_bot.progress import progress_card
from feishu_llm_bot.runtime_store import RuntimeStore, state_label
from feishu_llm_bot.task_artifacts import public_result, read_evidence
from feishu_llm_bot.task_completion import account_output, collection_gate, save_checkpoint
from feishu_llm_bot.task_tools import invoke
from feishu_llm_bot.worker import consume_event


@pytest.fixture
def case(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    store.accept_event("analysis", "local", "Analyze the evidence")
    a = store.claim(time.time())
    store.started(a["attempt_id"])
    a["config"] = {"database_path": str(store.path), "max_analysis_queries": 3,
                   "checkpoint_every": 2, "max_evidence_characters": 2000}
    path = tmp_path / "request.json"
    path.write_text(json.dumps(a))
    yield store, a, path
    store.close()


def report_inputs(key="stats"):
    return {"operation_key": key, "action": "report_data", "arguments": {
        "experiment_id": 1, "metric_group": 2, "period_type": "d", "base_vid": 10,
        "selected_metric_ids": [20], "start_date": "2026-10-01", "end_date": "2026-10-02",
    }}


def saved_report(case, data):
    store, a, path = case
    inputs = report_inputs()
    op = store.operation_begin(a["attempt_id"], a["token"], "stats", "libra_read", inputs)
    folder = path.parent / "operations" / op["operation_id"]
    folder.mkdir(parents=True)
    payload = {"status": "success", "data": {"code": 200, "data": data}}
    stdout = folder / "stdout.txt"
    stdout.write_text(json.dumps(payload))
    result = {**response(payload, request(inputs)[0]), "stdout_path": str(stdout)}
    store.operation_end(a["attempt_id"], a["token"], op["operation_id"], "succeeded", result)
    return op, result


def test_statistics_missing_zero_and_metric_filter():
    args = report_inputs()["arguments"]
    data = {"has_stats": 0, "merge_data": {"20": {"11": {"value": None}},
                                           "99": {"11": {"value": 12}}}}
    view = statistics(data, args)
    assert view["statistics_status"] == "missing" and not view["data_available"]
    assert len(view["rows"]) == 1 and view["rows"][0]["value"] is None
    data["has_stats"] = 1
    data["merge_data"]["20"]["11"] = {"value": 0, "relative_diff": {"10": -0.001},
                                      "p_val": {"10": 0.03}, "confidence": {"10": 1}}
    view = statistics(data, args)
    assert view["statistics_status"] == "available"
    assert view["rows"][0]["relative_diff"]["10"] == -0.001
    assert view["rows"][0]["p_val"]["10"] == 0.03
    args["selected_metric_ids"] = [20, 21]
    view = statistics(data, args)
    assert view["statistics_status"] == "partial" and view["absent_metric_ids"] == ["21"]
    data["has_full_stats"] = False
    assert statistics(data, args)["data_available"] is True


def test_compact_response_and_evidence_pagination_are_task_scoped(case):
    op, result = saved_report(case, {"merge_data": {"20": {
        "10": {"value": 1}, "11": {"value": 2}, "12": {"value": None},
    }}})
    result.update(validated_response={"huge": "x" * 20000}, output_tail="x" * 4000)
    wire = public_result(result, op["operation_id"])
    assert "validated_response" not in wire and "output_tail" not in wire
    store, a, path = case
    first = read_evidence(store, a, path, {"operation_id": op["operation_id"], "limit": 1})
    second = read_evidence(store, a, path, {"operation_id": op["operation_id"], "offset": 1})
    assert first["next_offset"] == 1 and second["rows"][0]["value"] == 2
    assert second["statistics_status"] == "partial"
    with pytest.raises(PermissionError):
        read_evidence(store, a, path, {"operation_id": "another-task"})


def test_series_never_silently_loses_date_alignment():
    view = statistics({"time_series_data": {"20": {"11": {"value": list(range(10000))}}}},
                      report_inputs()["arguments"])
    assert view["rows"][0]["requires_raw_artifact"]
    assert "value" not in view["rows"][0]
    assert len(json.dumps(view)) < 2000


def test_invalid_conclusion_and_hour_parameters_fail_before_cli():
    inputs = {"action": "important_impact", "arguments": {
        "experiment_id": 1, "app_id": 2, "bundle_id": 3, "base_version_id": 10,
        "version_ids": [10, 11], "start_date": "2026-10-01", "end_date": "2026-10-02",
    }}
    with pytest.raises(ValueError, match="exclude base_version_id"):
        request(inputs)
    inputs["arguments"]["version_ids"] = [11]
    assert request(inputs)[0]["arguments"]["version_ids"] == [11]
    inputs = report_inputs()
    inputs["arguments"]["experiment_id"] = "1"
    with pytest.raises(ValueError, match="positive integer"):
        request(inputs)
    inputs["arguments"]["experiment_id"] = 1
    inputs["arguments"]["period_type"] = "h"
    with pytest.raises(ValueError, match="%H:%M"):
        request(inputs)
    result = {"exit_code": 0}
    classify_failure(result, {"status": "success", "data": {"code": 400, "message": "bad group"}})
    assert result["reason_code"] == "invalid_arguments" and result["retryable"] is False
    assert result["message"] == "bad group"


@pytest.mark.parametrize("scope,expected", [("verified", "partial"), ("advisory", "completed")])
def test_missing_stats_cannot_become_verified_by_changing_outcome(case, scope, expected):
    store, a, _ = case
    saved_report(case, {"has_stats": 0, "merge_data": {"20": {"11": {"value": None}}}})
    store.submit_answer(a["attempt_id"], a["token"], "Recommendation", "completed",
                        completion_scope=scope)
    attempt = store.attempt(a["attempt_id"])
    assert attempt["business_outcome"] == expected
    assert "证据缺口" in attempt["answer"]
    if scope == "advisory":
        assert "不能作为定量验收或上线依据" in attempt["answer"]


@pytest.mark.parametrize("ending", ["task_timeout", "cancelled", "uncertain"])
def test_timeout_delivers_checkpoint_without_overriding_cancellation_or_unknown(case, ending):
    store, a, _ = case
    save_checkpoint(store, a, {"text": "Verified intermediate finding",
                               "evidence_gaps": ["Need ROI"]})
    if ending == "cancelled":
        store.handle_control(IncomingMessage.text("cancel", "local", "/cancel"))
    if ending == "uncertain":
        store.operation_begin(a["attempt_id"], a["token"], "write", "run", {"command": "write"})
    store.drain(a["attempt_id"])
    state = store.finish(a["attempt_id"], now=time.time(), reason=ending)
    event = store.get_by_correlation(a["correlation_id"])
    if ending == "cancelled":
        assert state == "cancelled" and "Verified intermediate finding" not in event.reply_text
    elif ending == "uncertain":
        assert state == "suspended" and "待核验" in event.reply_text
    else:
        assert state == "suspended" and "Verified intermediate finding" in event.reply_text
        assert "Need ROI" in event.reply_text and "部分成果" in event.reply_text
    with pytest.raises(PermissionError):
        save_checkpoint(store, a, {"text": "Late overwrite", "evidence_gaps": []})


def test_query_gate_requires_checkpoint_then_stops_at_budget(case):
    store, a, _ = case
    for i in range(2):
        assert collection_gate(store, a, "libra_read", report_inputs(str(i))) is None
    gate = collection_gate(store, a, "libra_read", report_inputs("2"))
    assert gate["state"] == "needs_checkpoint"
    save_checkpoint(store, a, {"text": "Known findings", "evidence_gaps": []})
    assert collection_gate(store, a, "libra_read", report_inputs("2")) is None
    gate = collection_gate(store, a, "libra_read", report_inputs("3"))
    assert gate["reason_code"] == "query_budget"


def test_missing_metric_requeries_are_bounded_but_other_metrics_can_continue(case):
    store, a, _ = case
    inputs = report_inputs("missing-1")
    for key in ("missing-1", "missing-2"):
        inputs["operation_key"] = key
        op = store.operation_begin(a["attempt_id"], a["token"], key, "libra_read", inputs)
        store.operation_end(a["attempt_id"], a["token"], op["operation_id"], "succeeded",
                            {"statistics_status": "missing"})
    assert collection_gate(store, a, "libra_read", report_inputs("third"))[
        "state"] == "evidence_missing"
    other = report_inputs("independent")
    other["arguments"]["selected_metric_ids"] = [21]
    assert collection_gate(store, a, "libra_read", other) is None


def test_checkpoint_does_not_bypass_native_image_requirement(case):
    store, a, _ = case
    with store.transaction() as db:
        db.execute("UPDATE events SET message_type='image' WHERE correlation_id=?",
                   (a["correlation_id"],))
    with pytest.raises(ValueError, match="Read the attached image"):
        save_checkpoint(store, a, {"text": "Unverified image description", "evidence_gaps": []})


def test_checkpoint_accepts_matching_explicit_task_and_rejects_cross_task(case):
    store, a, _ = case
    inputs = {"text": "Confirmed stage", "evidence_gaps": [],
              "correlation_id": a["correlation_id"]}
    save_checkpoint(store, a, inputs)
    inputs["correlation_id"] = "another-task"
    with pytest.raises(PermissionError, match="different task"):
        save_checkpoint(store, a, inputs)


def test_a_later_available_response_resolves_the_same_query_gap(case):
    store, a, _ = case
    inputs = report_inputs()
    for index, status in enumerate(("missing", "available")):
        key = f"query-{index}"
        op = store.operation_begin(a["attempt_id"], a["token"], key, "libra_read", inputs)
        store.operation_end(a["attempt_id"], a["token"], op["operation_id"], "succeeded",
                            {"statistics_status": status})
    store.submit_answer(a["attempt_id"], a["token"], "Now verified", "completed")
    assert store.attempt(a["attempt_id"])["business_outcome"] == "completed"


def test_output_budget_stops_more_raw_pages_but_allows_checkpoint_and_reply(case):
    store, a, path = case
    account_output(store, a, "read_artifact", {"text": "x" * 2100})
    blocked = invoke(path, "read_artifact", {"operation_id": "unused"})
    assert blocked["reason_code"] == "evidence_budget"
    invoke(path, "checkpoint", {"text": "Have findings", "evidence_gaps": []})
    invoke(path, "reply", {"correlation_id": a["correlation_id"], "text": "Partial findings",
                           "business_outcome": "partial"})
    assert store.attempt(a["attempt_id"])["answer"] == "Partial findings"


def test_disabled_integration_cannot_read_evidence(case):
    _, a, path = case
    a["config"]["integrations"] = []
    path.write_text(json.dumps(a))
    with pytest.raises(ValueError, match="not enabled"):
        invoke(path, "read_evidence", {"operation_id": "unused"})


class Workers:
    def __init__(self):
        self.started, self.stopped = [], []

    def start(self, a, path, config):
        self.started.append(a)

    def state(self, unit):
        return {"SubState": "running"}

    def stop(self, unit):
        self.stopped.append(unit)


def test_silent_model_finalizes_with_saved_checkpoint(tmp_path, monkeypatch):
    store, workers = RuntimeStore(tmp_path / "bot.sqlite3"), Workers()
    clock = [100.0]
    monkeypatch.setattr("feishu_llm_bot.orchestrator.time.monotonic", lambda: clock[0])
    engine = Orchestrator({"state_dir": str(tmp_path / "resident"), "task_timeout_seconds": 100,
                           "finalization_reserve_seconds": 20}, store, workers)
    try:
        store.accept_event("silent", "local", "Analyze")
        engine.tick()
        a = workers.started[0]
        save_checkpoint(store, a, {"text": "Survives compaction", "evidence_gaps": ["ROI"]})
        clock[0] = 181
        engine.tick()
        assert store.meta("finalize:" + a["attempt_id"]) == "soft_deadline"
        assert not workers.stopped
        engine.tick()
        count = store._connection.execute("SELECT count(*) FROM runtime_events "
                                          "WHERE kind='finalization_requested'").fetchone()[0]
        assert count == 1
        clock[0] = 201
        engine.tick()
        assert workers.stopped and not store.active()
        assert "Survives compaction" in store.get_by_correlation(a["correlation_id"]).reply_text
    finally:
        store.close()


def test_large_successful_history_rotates_but_recent_context_remains(tmp_path):
    store, workers = RuntimeStore(tmp_path / "bot.sqlite3"), Workers()
    engine = Orchestrator({"state_dir": str(tmp_path / "resident")}, store, workers)
    try:
        store.accept_event("one", "local", "First question")
        engine.tick()
        a = workers.started[0]
        consume_event(store, a, {"type": "system", "subtype": "init", "session_id": "large"}, [])
        consume_event(store, a, {"type": "assistant", "message": {"content": [],
            "usage": {"input_tokens": 1000, "cache_read_input_tokens": 120000}}}, [])
        store.submit_answer(a["attempt_id"], a["token"], "Confirmed first answer")
        engine.end(store.attempt(a["attempt_id"]), None, time.time())
        store.accept_event("two", "local", "Next question")
        engine.tick()
        assert workers.started[-1]["session_id"] is None
        assert "Confirmed first answer" in json.dumps(store.context())
    finally:
        store.close()


def test_partial_card_and_status_are_distinct_from_execution_error():
    state = {"status": "replied", "task_state": "failed", "business_outcome": "partial",
             "answer": "Useful analysis, missing ROI", "created_at": 1, "phase": "done",
             "steps": [], "recovery": {"available_actions": []}}
    card = progress_card(state, now=2, connection="桥接在线")
    assert card["header"]["template"] == "orange"
    assert "部分完成" in json.dumps(card, ensure_ascii=False)
    assert "部分完成" in state_label("failed", "partial")
    assert state_label("cancelled", "partial") == "已取消"
