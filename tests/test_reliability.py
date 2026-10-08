from __future__ import annotations

import json
import sqlite3
import sys
import time

import pytest

from feishu_llm_bot.command_runner import execute_command, group_alive
from feishu_llm_bot.feishu import IncomingMessage
from feishu_llm_bot.operation_contracts import cli_environment
from feishu_llm_bot.runtime_admin import reconcile_read_help
from feishu_llm_bot.runtime_metrics import runtime_metrics
from feishu_llm_bot.runtime_recovery import recovery_options
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_tools import invoke


@pytest.fixture
def case(tmp_path):
    db = RuntimeStore(tmp_path / "bot.sqlite3")
    db.accept_event("request", "chat", "查询开放时间")
    a = db.claim(time.time())
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                **a,
                "config": {
                    "database_path": str(db.path),
                    "path": "/usr/bin:/bin",
                },
            }
        )
    )
    yield db, a, request
    db.close()


def end(db, a, reason=None):
    db.drain(a["attempt_id"])
    return db.finish(a["attempt_id"], now=time.time(), reason=reason)


def fake_cli(case, tmp_path, code):
    script = tmp_path / "cli"
    script.write_text(f"#!{sys.executable}\nimport sys,json,time,os\n" + code)
    script.chmod(0o700)
    request = case[2]
    config = json.loads(request.read_text())
    config["config"]["bytedcli_command"] = str(script)
    request.write_text(json.dumps(config))
    return script


def successful_search():
    return {"ok": True, "data": {"results": [], "total": 0, "has_more": False}}


def test_help_timeout_then_real_search_finishes_business(case, tmp_path):
    db, a, request = case
    fake_cli(
        case,
        tmp_path,
        "if '--help' in sys.argv:\n"
        " print('Usage: bytedcli\\nOptions: --help',flush=True)\n time.sleep(30)\n"
        f"else: print({json.dumps(successful_search())!r},flush=True)\n",
    )
    result = invoke(
        request, "cli_help", {"operation_key": "help", "topic": "search", "timeout_seconds": 1}
    )
    assert result["state"] == "failed" and result["reason_code"] == "command_timeout"
    help_op = db.operations(a["correlation_id"])[0]
    assert len(db.operation_attempts(help_op["operation_id"])) == 2
    found = invoke(request, "search_documents", {"operation_key": "search", "query": "gym"})
    assert found["state"] == "succeeded"
    db.submit_answer(a["attempt_id"], a["token"], "未发现匹配结果", "unanswered")
    assert end(db, a) == "failed"
    assert db.task(a["correlation_id"])["business_outcome"] == "unanswered"
    assert all(op["state"] != "unknown" for op in db.operations(a["correlation_id"]))
    assert "/continue" in db.get_by_correlation(a["correlation_id"]).reply_text


def test_valid_result_is_checkpointed_before_hung_process_cleanup(tmp_path):
    seen = []
    payload = successful_search()

    def receive(result):
        assert result["exit_code"] is None
        assert result["result_seconds"] < 2
        seen.append(result["validated_response"])

    result = execute_command(
        [
            sys.executable,
            "-c",
            f"import time; print({json.dumps(payload)!r},flush=True); time.sleep(30)",
        ],
        tmp_path,
        timeout=5,
        exit_grace=0.1,
        result_validator=lambda p: p == payload,
        on_result=receive,
    )
    assert seen == [payload]
    assert result["reason_code"] == "process_exit_timeout_after_result"
    assert result["cleanup_confirmed"] and result["process_seconds"] < 3


def test_hung_creation_retains_id_verifies_content_and_never_recreates(case, tmp_path):
    db, a, request = case
    count = tmp_path / "creates"
    fake_cli(
        case,
        tmp_path,
        "doc={'document_id':'doc1','url':'https://example.test/doc1'}\n"
        "if sys.argv[4]=='create':\n"
        f" open({str(count)!r},'a').write('create\\n')\n"
        " print(json.dumps({'ok':True,'data':{'document':doc}}),flush=True)\n"
        " time.sleep(30)\n"
        "else:\n doc.update(content='weekly content', revision_id=1)\n"
        " print(json.dumps({'ok':True,'data':{'document':doc}}))\n",
    )
    args = {"operation_key": "doc", "title": "weekly", "content": "weekly content"}
    result = invoke(request, "create_document", args)
    assert result["state"] == "succeeded" and result["verified"]
    assert result["reason_code"] == "process_exit_timeout_after_result"
    assert invoke(request, "create_document", args)["verified"]
    assert count.read_text() == "create\n"
    # A model failure after receipt delivery must not schedule a full business replay.
    assert end(db, a, "model_error") == "failed"
    task = db.task(a["correlation_id"])
    assert task["business_outcome"] == "partial" and task["retries"] == 0
    assert "https://example.test/doc1" in db.get_by_correlation(a["correlation_id"]).reply_text


@pytest.mark.parametrize("content,revision", [("wrong content", 1), ("expected", None)])
def test_wrong_content_or_missing_revision_preserves_unknown_write(
    case, tmp_path, content, revision
):
    db, a, request = case
    fake_cli(
        case,
        tmp_path,
        "doc={'document_id':'doc1','url':'https://example.test/doc1'}\n"
        f"if sys.argv[4]=='fetch': doc.update(content={content!r},revision_id={revision!r})\n"
        "print(json.dumps({'ok':True,'data':{'document':doc}}))\n",
    )
    inputs = {"operation_key": "doc", "title": "weekly", "content": "expected"}
    result = invoke(request, "create_document", inputs)
    assert result["state"] == "unknown" and result["document"]["document_id"] == "doc1"
    with pytest.raises(ValueError, match="already attempted"):
        invoke(request, "create_document", inputs)
    assert end(db, a) == "suspended"


def test_search_auth_failure_is_not_retried(case, tmp_path):
    db, a, request = case
    fake_cli(case, tmp_path, "print(json.dumps({'ok':False,'error':{'code':'unauthorized'}}))\n")
    result = invoke(request, "search_documents", {"operation_key": "read", "query": "gym"})
    assert result["reason_code"] == "needs_auth" and not result["retryable"]
    assert len(db.operation_attempts(db.operations(a["correlation_id"])[0]["operation_id"])) == 1
    db.submit_answer(a["attempt_id"], a["token"], "需要授权", "unanswered")
    end(db, a)
    assert db.task(a["correlation_id"])["business_outcome"] == "unanswered"


def test_changed_timeout_reuses_semantics_but_old_completion_is_fenced(case):
    db, a, _ = case
    args = {"query": "gym", "timeout_seconds": 1}
    op = db.operation_begin(a["attempt_id"], a["token"], "read", "search_documents", args)
    db.operation_end(
        a["attempt_id"],
        a["token"],
        op["operation_id"],
        "failed",
        {"retryable": True, "reason_code": "command_timeout"},
        operation_attempt_id=op["operation_attempt_id"],
    )
    newer = db.operation_begin(
        a["attempt_id"], a["token"], "read", "search_documents", {**args, "timeout_seconds": 5}
    )
    assert newer["operation_id"] == op["operation_id"]
    assert newer["operation_attempt_id"] != op["operation_attempt_id"]
    with pytest.raises(ValueError, match="already ended"):
        db.operation_end(
            a["attempt_id"],
            a["token"],
            op["operation_id"],
            "succeeded",
            {},
            operation_attempt_id=op["operation_attempt_id"],
        )
    attempts = db.operation_attempts(op["operation_id"])
    assert [json.loads(row["execution"])["timeout_seconds"] for row in attempts] == [1, 5]
    with pytest.raises(ValueError, match="different input"):
        db.operation_begin(
            a["attempt_id"], a["token"], "read", "search_documents", {"query": "other"}
        )


def test_unknown_write_allows_typed_verification_but_not_new_shell(case):
    db, a, _ = case
    op = db.operation_begin(a["attempt_id"], a["token"], "write", "run", {"command": "write"})
    db.operation_end(a["attempt_id"], a["token"], op["operation_id"], "unknown", {})
    with pytest.raises(ValueError, match="uncertain"):
        db.operation_begin(
            a["attempt_id"], a["token"], "shell", "run", {"command": "search --help"}
        )
    read = db.operation_begin(
        a["attempt_id"], a["token"], "verify", "fetch_document", {"document_id": "doc1"}
    )
    assert read["effect_kind"] == "read"
    db.submit_answer(a["attempt_id"], a["token"], "已保存解释，但待核验")
    assert end(db, a) == "suspended"
    text = db.get_by_correlation(a["correlation_id"]).reply_text
    assert "已保存解释" in text and "没有最终结果" not in text and "/continue" not in text
    recovery = recovery_options(db._connection, a["correlation_id"])
    assert not recovery["can_retry"] and "continue" not in recovery["available_actions"]


@pytest.mark.parametrize(
    "tool,inputs",
    [
        ("cli_help", {"topic": "search; touch x"}),
        ("search_documents", {"query": "gym", "read_only": True}),
        ("fetch_document", {"document_id": "doc1 && touch x"}),
        ("search_documents", {"query": "gym", "timeout_seconds": True}),
    ],
)
def test_malformed_typed_reads_record_no_intent(case, tool, inputs):
    db, a, request = case
    with pytest.raises(ValueError):
        invoke(request, tool, {"operation_key": "invalid", **inputs})
    assert not db.operations(a["correlation_id"])


def test_query_shell_characters_remain_literal_argument(case, tmp_path):
    db, a, request = case
    query = "$(touch SHOULD_NOT_EXIST); echo private"
    fake_cli(
        case,
        tmp_path,
        f"assert sys.argv[sys.argv.index('--query')+1] == {query!r}\n"
        f"print({json.dumps(successful_search())!r})\n",
    )
    assert (
        invoke(request, "search_documents", {"operation_key": "q", "query": query})["state"]
        == "succeeded"
    )
    assert not (tmp_path / "SHOULD_NOT_EXIST").exists()


def test_interrupted_read_does_not_become_unknown(case):
    db, a, _ = case
    db.operation_begin(a["attempt_id"], a["token"], "help", "cli_help", {"topic": "search"})
    db.submit_answer(a["attempt_id"], a["token"], "实际搜索尚未执行", "unanswered")
    assert end(db, a, "worker_exit") == "failed"
    op = db.operations(a["correlation_id"])[0]
    assert op["state"] == "failed"
    assert db.operation_attempts(op["operation_id"])[0]["state"] == "failed"
    assert "实际搜索尚未执行" in db.get_by_correlation(a["correlation_id"]).reply_text
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    assert db.claim(time.time())


def test_host_environment_cannot_inherit_tracking_or_upgrade_enabled():
    env = cli_environment({"path": "/fixed"}, {"BYTEDCLI_TRACKING_DISABLED": "0", "PATH": "/old"})
    assert env["PATH"] == "/fixed"
    assert env["BYTEDCLI_TRACKING_DISABLED"] == env["BYTEDCLI_NO_AUTO_UPGRADE"] == "1"


def test_legacy_help_audit_unlocks_continue_without_claiming_success(case):
    db, a, _ = case
    op = db.operation_begin(
        a["attempt_id"], a["token"], "help", "run", {"command": "bytedcli lark docs search --help"}
    )
    db.operation_end(
        a["attempt_id"],
        a["token"],
        op["operation_id"],
        "unknown",
        {"timed_out": True, "exit_code": None},
    )
    end(db, a)
    before = db.operations(a["correlation_id"])[0]["result"]
    result = reconcile_read_help(db, op["operation_id"], {}, "reviewed exact help invocation")
    assert result["state"] == "failed"
    assert db.operations(a["correlation_id"])[0]["result"] == before
    assert db.operation_attempts(op["operation_id"])[0]["state"] == "unknown"
    assert recovery_options(db._connection, a["correlation_id"])["can_retry"]
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    assert db.claim(time.time())


def test_shell_containing_help_cannot_be_reclassified(case):
    db, a, _ = case
    op = db.operation_begin(
        a["attempt_id"],
        a["token"],
        "help",
        "run",
        {"command": "bytedcli lark docs search --help; write"},
    )
    end(db, a)
    with pytest.raises(ValueError, match="exact known"):
        reconcile_read_help(db, op["operation_id"], {}, "review")


def test_metrics_separate_greeting_and_manual_recovery(case):
    db, a, _ = case
    db.submit_answer(a["attempt_id"], a["token"], "暂时没有答案", "unanswered")
    end(db, a)
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    retry = db.claim(time.time())
    db.submit_answer(retry["attempt_id"], retry["token"], "已找到依据")
    end(db, retry)
    db.accept_event("hi", "chat", "你好")
    hello = db.claim(time.time())
    db.submit_answer(hello["attempt_id"], hello["token"], "你好")
    end(db, hello)
    metrics = runtime_metrics(db._connection)
    assert metrics["finalized_business_tasks"] == 1
    assert metrics["first_attempt_completed"] == 0
    assert metrics["manually_continued_business_tasks"] == 1
    assert metrics["task_categories"]["greeting"] == 1


def test_background_descendant_is_stopped_even_after_leader_exits(tmp_path):
    pidfile = tmp_path / "pid"
    script = (
        "import os,signal,time\n"
        f'open({str(pidfile)!r},"w").write(str(os.getpid()))\n'
        "if os.fork()==0:\n signal.signal(signal.SIGTERM,signal.SIG_IGN)\n time.sleep(30)\n"
    )
    result = execute_command([sys.executable, "-c", script], tmp_path, timeout=3)
    assert result["background_children"] and result["cleanup_confirmed"]
    assert not group_alive(int(pidfile.read_text()))


def legacy_database(path, monkeypatch, *, active=False):
    import feishu_llm_bot.runtime_store as runtime

    with monkeypatch.context() as context:
        context.setattr(runtime, "migrate_runtime_schema", lambda _: None)
        db = RuntimeStore(path)
    db._connection.execute(
        "INSERT INTO runtime_operations VALUES "
        "('op','cid','key','hash','a','run','unknown','{}',NULL,1)"
    )
    if active:
        db._connection.execute(
            "INSERT INTO runtime_attempts "
            "(attempt_id,correlation_id,token_hash,state,started_at) "
            "VALUES ('a','cid','hash','running',1)"
        )
    db.close()


def test_schema_upgrade_preserves_legacy_unknown_and_is_repeatable(tmp_path, monkeypatch):
    path = tmp_path / "legacy.sqlite3"
    legacy_database(path, monkeypatch)
    for _ in range(2):
        db = RuntimeStore(path)
        assert db.meta("runtime_schema_version") == 2
        op = db.operations("cid")[0]
        assert op["state"] == "unknown" and op["effect_kind"] == "unclassified"
        assert op["semantic_hash"] is None and db.operation_attempts("op") == []
        db.close()


def test_schema_upgrade_refuses_live_old_worker_and_rolls_back(tmp_path, monkeypatch):
    path = tmp_path / "legacy.sqlite3"
    legacy_database(path, monkeypatch, active=True)
    with pytest.raises(RuntimeError, match="drain old workers"):
        RuntimeStore(path)
    with sqlite3.connect(path) as raw:
        assert "effect_kind" not in [
            r[1] for r in raw.execute("PRAGMA table_info(runtime_operations)")
        ]
        assert raw.execute("SELECT state FROM runtime_attempts").fetchone()[0] == "running"


def test_schema_upgrade_failure_rolls_back_earlier_alters(tmp_path, monkeypatch):
    path = tmp_path / "legacy.sqlite3"
    legacy_database(path, monkeypatch)
    with sqlite3.connect(path) as raw:
        raw.execute("ALTER TABLE runtime_tasks ADD COLUMN business_outcome TEXT")
    with pytest.raises(sqlite3.OperationalError):
        RuntimeStore(path)
    with sqlite3.connect(path) as raw:
        assert "effect_kind" not in [
            r[1] for r in raw.execute("PRAGMA table_info(runtime_operations)")
        ]
        assert not raw.execute(
            "SELECT value FROM runtime_meta WHERE key='runtime_schema_version'"
        ).fetchone()


def test_newer_schema_version_fails_closed(tmp_path):
    path = tmp_path / "future.sqlite3"
    db = RuntimeStore(path)
    db.set_meta("runtime_schema_version", 99)
    db.close()
    with pytest.raises(RuntimeError, match="Unsupported"):
        RuntimeStore(path)


def test_reconciliation_refreshes_delivered_card_after_sender_restart(case, tmp_path):
    from test_progress import Client

    from feishu_llm_bot.progress import ProgressMonitor

    db, a, _ = case
    op = db.operation_begin(
        a["attempt_id"], a["token"], "help", "run", {"command": "bytedcli lark docs search --help"}
    )
    end(db, a)
    client = Client()
    options = dict(
        database=db.path,
        state_path=db.path,
        transcript=tmp_path / "unused",
        session_id="session",
        health_path=tmp_path / "health",
        resident_path=tmp_path / "resident",
        client=client,
        runtime_mode=True,
    )
    monitor = ProgressMonitor(**options)
    monitor.tick()
    monitor.close()
    card = json.dumps(client.cards[-1][1], ensure_ascii=False)
    assert '"action": "continue"' not in card
    reconcile_read_help(db, op["operation_id"], {}, "reviewed")
    monitor = ProgressMonitor(**options)
    monitor.tick(time.time() + 5)
    monitor.close()
    assert len(client.cards) == 1
    assert '"action": "continue"' in json.dumps(client.updates[-1][1])
    db.handle_control(IncomingMessage.text("continue", "chat", "/continue 1"))
    assert db.claim(time.time())


def test_partial_json_and_missing_write_receipt_do_not_become_success(case, tmp_path):
    db, a, request = case
    fake_cli(case, tmp_path, "print('{\"ok\":true,')\n")
    result = invoke(
        request, "create_document", {"operation_key": "doc", "title": "weekly", "content": "text"}
    )
    assert result["state"] == "unknown" and not result.get("verified")
    assert end(db, a) == "suspended"


def test_cancel_during_early_receipt_prevents_further_side_effects(case, tmp_path, monkeypatch):
    import feishu_llm_bot.task_tools as tool_module

    db, a, request = case

    def command(args, directory, on_result, **_):
        db.handle_control(IncomingMessage.text("cancel", "chat", "/cancel 1"))
        on_result(
            {
                "validated_response": {
                    "ok": True,
                    "data": {
                        "document": {"document_id": "doc1", "url": "https://example.test/doc1"}
                    },
                }
            }
        )
        raise AssertionError("Cancellation must fence the receipt database update")

    monkeypatch.setattr(tool_module, "execute_command", command)
    with pytest.raises(PermissionError):
        invoke(
            request,
            "create_document",
            {"operation_key": "doc", "title": "weekly", "content": "text"},
        )
    assert end(db, a) == "cancelled"
    assert db.operations(a["correlation_id"])[0]["state"] == "unknown"


def test_failed_process_cleanup_drains_worker_and_fences_next_operation(case, monkeypatch):
    from feishu_llm_bot.command_runner import CleanupError

    db, a, request = case

    def command(*args, **kwargs):
        raise CleanupError("simulated unkillable child")

    monkeypatch.setattr("feishu_llm_bot.task_tools.execute_command", command)
    with pytest.raises(CleanupError):
        invoke(request, "cli_help", {"operation_key": "help", "topic": "search"})
    assert db.attempt(a["attempt_id"])["state"] == "draining"
    with pytest.raises(PermissionError):
        invoke(request, "cli_help", {"operation_key": "other", "topic": "search"})


def test_runtime_setup_error_is_actionable_and_not_retried(case, tmp_path):
    db, a, request = case
    fake_cli(
        case,
        tmp_path,
        "print(json.dumps({'status':'error','error':{'code':'LARK_CLI_INSTALL_FAILED'}}))\n",
    )
    result = invoke(request, "search_documents", {"operation_key": "q", "query": "gym"})
    assert result["reason_code"] == "runtime_unavailable" and not result["retryable"]
    assert len(db.operation_attempts(db.operations(a["correlation_id"])[0]["operation_id"])) == 1
