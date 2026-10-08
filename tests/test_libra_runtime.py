from __future__ import annotations

import json
import sys
import time

import pytest

from feishu_llm_bot import libra_contracts
from feishu_llm_bot.runtime_admin import reconcile_libra_arguments
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_budget import deadlines
from feishu_llm_bot.task_tools import invoke
from feishu_llm_bot.worker import consume_event


@pytest.fixture
def case(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    store.accept_event("libra", "local", "analyze an experiment")
    attempt = store.claim(time.time(), "session")
    store.started(attempt["attempt_id"])
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                **attempt,
                "config": {
                    "database_path": str(store.path),
                    "path": "/usr/bin:/bin",
                },
            }
        )
    )
    yield store, attempt, request
    store.close()


def fake_cli(case, source):
    request = case[2]
    executable = request.parent / "libra"
    executable.write_text(f"#!{sys.executable}\nimport sys,json,time\n" + source)
    executable.chmod(0o700)
    data = json.loads(request.read_text())
    data["config"]["libra_cli_command"] = str(executable)
    request.write_text(json.dumps(data))
    return executable


def search(key="query", **arguments):
    return {
        "operation_key": key,
        "action": "metric_search",
        "arguments": {"experiment_id": 5754624, "metric_keys": ["StayDuration/U"], **arguments},
    }


CSV_SCRIPT = """
import csv
keys=json.loads(sys.argv[sys.argv.index('--metric-keys')+1])
w=csv.writer(sys.stdout)
w.writerow(['query','result_type','metric_group_id','metric_group_name','metric_id',
            'metric_name','match_field','match_score','resolution_status'])
for key in keys:
 w.writerow([key,'metric','1','group','2',key,'name','1','resolved'])
"""


def test_bad_parameters_corrected_with_same_key_without_unknown_or_approval(case):
    store, attempt, request = case
    fake_cli(case, CSV_SCRIPT)
    with pytest.raises(ValueError, match="metric_keys"):
        invoke(request, "libra_read", search(metric_keys="StayDuration/U,ads"))
    assert store.operations(attempt["correlation_id"]) == []
    result = invoke(request, "libra_read", search())
    assert result["state"] == "succeeded" and result["format"] == "csv"
    assert result["row_count"] == 1
    saved = store.operations(attempt["correlation_id"])
    assert saved[0]["effect_kind"] == "read"
    assert store._connection.execute("SELECT count(*) FROM permission_requests").fetchone()[0] == 0


def test_known_read_can_run_after_unknown_write_but_shell_cannot(case):
    store, attempt, request = case
    fake_cli(case, CSV_SCRIPT)
    op = store.operation_begin(attempt["attempt_id"], attempt["token"], "write", "run", {})
    store.operation_end(attempt["attempt_id"], attempt["token"], op["operation_id"], "unknown", {})
    assert invoke(request, "libra_read", search())["state"] == "succeeded"
    with pytest.raises(ValueError, match="uncertain"):
        invoke(request, "run", {"operation_key": "shell", "command": "/bin/true"})


@pytest.mark.parametrize(
    ("code", "exit_code", "reason"),
    [
        (401, 0, "needs_auth"),
        (403, 1, "access_denied"),
        (400, 2, "invalid_arguments"),
        (999, 0, "invalid_response"),
    ],
)
def test_backend_and_parser_errors_are_not_retried_or_marked_unknown(case, code, exit_code, reason):
    store, attempt, request = case
    fake_cli(
        case,
        f"print(json.dumps({{'status':'error','error':{{'code':{code}}}}}))\n"
        f"sys.exit({exit_code})\n",
    )
    result = invoke(request, "libra_read", search())
    assert result["state"] == "failed" and result["reason_code"] == reason
    assert result["retryable"] is False
    op = store.operations(attempt["correlation_id"])[0]
    assert len(store.operation_attempts(op["operation_id"])) == 1
    assert op["state"] == "failed"


def test_transient_read_retries_then_reuses_success(case):
    store, attempt, request = case
    counter = request.parent / "count"
    fake_cli(
        case,
        f"""
from pathlib import Path
p=Path({str(counter)!r})
n=int(p.read_text()) if p.exists() else 0
p.write_text(str(n+1))
if n == 0:
 print('429 Too many requests',file=sys.stderr)
 sys.exit(1)
"""
        + CSV_SCRIPT,
    )
    first = invoke(request, "libra_read", search())
    second = invoke(request, "libra_read", search())
    assert first["state"] == second["state"] == "succeeded"
    assert counter.read_text() == "2"
    assert len(store.operation_attempts(first["operation_id"])) == 2


def test_help_and_artifacts_do_not_start_shell_or_expose_other_files(case, tmp_path):
    store, attempt, request = case
    fake_cli(case, CSV_SCRIPT)
    assert "report_data" in invoke(request, "libra_read", {"action": "help"})["actions"]
    result = invoke(request, "libra_read", search())
    view = invoke(request, "operations", {})
    assert view["total"] == 1 and "output_tail" not in json.dumps(view)
    artifact = invoke(
        request, "read_artifact", {"operation_id": result["operation_id"], "limit": 20}
    )
    assert len(artifact["text"]) == 20 and artifact["next_offset"] == 20
    with pytest.raises(PermissionError):
        invoke(request, "read_artifact", {"operation_id": "other-task-operation"})
    path = tmp_path / "private-file"
    path.write_text("not a command artifact")
    with store.transaction() as db:
        db.execute(
            "UPDATE runtime_operations SET result=? WHERE operation_id=?",
            (json.dumps({"stdout_path": str(path)}), result["operation_id"]),
        )
    with pytest.raises(PermissionError, match="outside"):
        invoke(request, "read_artifact", {"operation_id": result["operation_id"]})
    assert store.attempt(attempt["attempt_id"])["unsafe_tools"] == 0


def test_soft_deadline_returns_saved_results_and_final_partial_answer(case):
    store, attempt, request = case
    fake_cli(case, CSV_SCRIPT)
    invoke(request, "libra_read", search())
    data = json.loads(request.read_text())
    data.update(deadlines(100, now=time.monotonic() - 90))
    request.write_text(json.dumps(data))
    result = invoke(request, "libra_read", search("new"))
    assert result["reason_code"] == "soft_deadline" and not result["retryable"]
    assert invoke(request, "operations", {})["total"] == 1
    invoke(
        request,
        "reply",
        {
            "correlation_id": attempt["correlation_id"],
            "text": "部分指标已取回，证据尚不完整。",
            "business_outcome": "partial",
        },
    )
    entry = {"type": "result", "result": "部分指标已取回，证据尚不完整。"}
    assert consume_event(store, data, entry, []) == "completed"
    store.drain(attempt["attempt_id"])
    store.finish(attempt["attempt_id"], now=time.time())
    task = store.task(attempt["correlation_id"])
    assert task["business_outcome"] == "partial"
    assert "已保存 1 项读取结果" in store.get_by_correlation(attempt["correlation_id"]).reply_text


def test_complete_answer_after_soft_deadline_is_not_automatically_failed(case):
    store, attempt, request = case
    data = json.loads(request.read_text())
    data.update(deadlines(100, now=time.monotonic() - 90))
    assert consume_event(store, data, {"type": "result", "result": "完整回答"}, []) == "completed"
    store.drain(attempt["attempt_id"])
    assert store.finish(attempt["attempt_id"], now=time.time()) == "succeeded"


def test_repeated_identical_error_triggers_finalization(case):
    store, attempt, request = case
    for _ in range(3):
        with pytest.raises(ValueError):
            invoke(request, "libra_read", search(metric_keys="bad array"))
    result = invoke(request, "libra_read", search("later"))
    assert result["reason_code"] == "repeated_tool_error"
    assert store.operations(attempt["correlation_id"]) == []


def test_interrupted_validated_read_receipt_is_kept(case):
    store, attempt, _ = case
    args = {
        "operation_key": "exp",
        "action": "experiment_get",
        "arguments": {"experiment_id": 5754624},
    }
    op = store.operation_begin(attempt["attempt_id"], attempt["token"], "exp", "libra_read", args)
    store.operation_checkpoint(
        attempt["attempt_id"],
        attempt["token"],
        op["operation_id"],
        op["operation_attempt_id"],
        {
            "validated_response": {
                "status": "success",
                "data": {"code": 200, "data": {"experiment": {"id": 5754624}}},
            }
        },
    )
    store.drain(attempt["attempt_id"])
    store.finish(attempt["attempt_id"], now=time.time(), reason="task_timeout")
    assert store.operations(attempt["correlation_id"])[0]["state"] == "succeeded"
    assert "已保存 1 项读取结果" in store.get_by_correlation(attempt["correlation_id"]).reply_text


@pytest.mark.parametrize(
    "arguments",
    [
        {"experiment_id": True},
        {"experiment_id": 1, "force_export": True},
        {"experiment_id": 1, "with": ["arbitrary"]},
    ],
)
def test_contract_rejects_invalid_ids_and_unregistered_parameters(arguments):
    with pytest.raises(ValueError):
        libra_contracts.request({"action": "experiment_get", "arguments": arguments})


def test_contract_serializes_array_as_single_literal_argv():
    semantic, _ = libra_contracts.request(search(metric_keys=["StayDuration/U", "$(touch nope)"]))
    args = libra_contracts.argv("libra-cli", semantic)
    assert json.loads(args[args.index("--metric-keys") + 1]) == ["StayDuration/U", "$(touch nope)"]
    assert "--no-track" in args


@pytest.mark.parametrize(
    "text", ["", "Usage: libra-cli\nOptions:\n", "{}\n", "query,metric_id\nx,1\n"]
)
def test_search_rejects_missing_or_wrong_csv_contract(text):
    semantic, _ = libra_contracts.request(search())
    assert libra_contracts.csv_response(text, semantic) is None


def test_search_accepts_real_cli_json_wrapped_csv(case):
    _, _, request = case
    fake_cli(
        case,
        """
import io
buffer=io.StringIO()
sys.stdout=buffer
"""
        + CSV_SCRIPT
        + "\nsys.stdout=sys.__stdout__\n"
        "print(json.dumps({'status':'success','data':buffer.getvalue().rstrip()}))\n",
    )
    result = invoke(request, "libra_read", search())
    assert result["state"] == "succeeded" and result["row_count"] == 1


@pytest.mark.parametrize("compound", [False, True])
def test_historical_parser_reconciliation_is_exact_and_preserves_receipt(case, compound):
    store, attempt, request = case
    command = (
        "libra-cli --json metrics search --experiment-id 5754624 "
        "--metric-keys 'StayDuration/U,ads' --top 20"
    )
    if compound:
        command += "; echo unsafe"
    op = store.operation_begin(
        attempt["attempt_id"], attempt["token"], "old", "run", {"command": command}
    )
    folder = (
        request.parent
        / "tasks"
        / attempt["correlation_id"]
        / attempt["attempt_id"]
        / "operations"
        / op["operation_id"]
    )
    folder.mkdir(parents=True)
    error = folder / "stderr.txt"
    error.write_text("Error: Invalid value: --metric-keys must be valid JSON\n")
    receipt = {
        "exit_code": 2,
        "timed_out": False,
        "background_children": False,
        "stderr_path": str(error),
    }
    store.operation_end(
        attempt["attempt_id"], attempt["token"], op["operation_id"], "unknown", receipt
    )
    store.drain(attempt["attempt_id"])
    store.finish(attempt["attempt_id"], now=time.time())
    config = {"state_dir": str(request.parent / "resident")}
    if compound:
        with pytest.raises(ValueError, match="Compound"):
            reconcile_libra_arguments(store, op["operation_id"], config, "reviewed parser")
    else:
        result = reconcile_libra_arguments(store, op["operation_id"], config, "reviewed parser")
        assert result["state"] == "failed"
        current = store.operations(attempt["correlation_id"])[0]
        assert json.loads(current["result"]) == receipt
        assert store.operation_attempts(op["operation_id"])[0]["state"] == "unknown"


def test_read_timeout_does_not_block_an_independent_query(case):
    store, attempt, request = case
    fake_cli(
        case,
        "if 'slow' in sys.argv[sys.argv.index('--metric-keys')+1]: time.sleep(20)\n" + CSV_SCRIPT,
    )
    failed = invoke(
        request, "libra_read", {**search("slow", metric_keys=["slow"]), "timeout_seconds": 1}
    )
    assert failed["state"] == "failed" and failed["reason_code"] == "command_timeout"
    assert invoke(request, "libra_read", search("working"))["state"] == "succeeded"
    assert all(op["state"] != "unknown" for op in store.operations(attempt["correlation_id"]))


def test_report_query_builds_typed_window_and_filters():
    semantic, _ = libra_contracts.request(
        {
            "action": "report_data",
            "arguments": {
                "experiment_id": 1,
                "metric_group": 2,
                "period_type": "d",
                "start_date": "2026-09-23",
                "end_date": "2026-09-27",
                "selected_metric_ids": [3, 4],
                "selected_vids": [5, 6],
            },
        }
    )
    args = libra_contracts.argv("libra-cli", semantic)
    assert args[args.index("--selected-metric-ids") + 1] == "3,4"
    assert json.loads(args[args.index("--extra-query") + 1]) == {"selected_vids": "5,6"}
    assert args[args.index("--mult-cmp-corr") + 1] == "true"


@pytest.mark.parametrize(
    "changes",
    [
        {"end_date": "2026-09-22"},
        {"period_type": "h"},
        {"selected_metric_ids": "3,4"},
        {"extra_query": {}},
    ],
)
def test_invalid_report_filters_do_not_reach_cli(changes):
    with pytest.raises(ValueError):
        libra_contracts.request(
            {
                "action": "report_data",
                "arguments": {
                    "experiment_id": 1,
                    "metric_group": 2,
                    "period_type": "d",
                    "start_date": "2026-09-23",
                    "end_date": "2026-09-27",
                    **changes,
                },
            }
        )


def test_saved_artifact_symlink_cannot_escape_task(case, tmp_path):
    store, attempt, request = case
    fake_cli(case, CSV_SCRIPT)
    result = invoke(request, "libra_read", search())
    source = tmp_path / "outside.txt"
    source.write_text("private")
    op = store.operations(attempt["correlation_id"])[0]
    path = __import__("pathlib").Path(json.loads(op["result"])["stdout_path"])
    path.unlink()
    path.symlink_to(source)
    with pytest.raises(PermissionError):
        invoke(request, "read_artifact", {"operation_id": result["operation_id"]})
