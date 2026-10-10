"""Execution state independent of Feishu delivery, in the existing durable database."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
import uuid
from contextlib import contextmanager

from .card_delivery import answer_pages
from .operation_contracts import READ_KINDS, READ_LIMITS, read_request, response_valid
from .runtime_recovery import recovery_options
from .runtime_schema import migrate_runtime_schema
from .store import Store

TERMINAL = {"succeeded", "failed", "cancelled", "suspended"}


class RuntimeStore(Store):
    def prune_events(self, *, older_than_seconds=7 * 24 * 3600):
        """Keep runtime history until tasks, receipts and attachments share a retention policy.

        The inherited gateway startup cleanup only knows the legacy events table.
        Deleting those rows breaks task lookup, continuation and confirmed context.
        """
        return 0

    def __init__(self, path):
        super().__init__(path)
        self._connection.execute("PRAGMA busy_timeout=1000")
        self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS runtime_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runtime_tasks (
                correlation_id TEXT PRIMARY KEY REFERENCES events(correlation_id),
                state TEXT NOT NULL DEFAULT 'queued', attempt_id TEXT,
                retries INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                steps TEXT NOT NULL DEFAULT '[]', warning TEXT, activity_at REAL,
                total_seconds REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS runtime_attempts (
                attempt_id TEXT PRIMARY KEY, correlation_id TEXT NOT NULL,
                token_hash TEXT NOT NULL, state TEXT NOT NULL,
                started_at REAL NOT NULL, ended_at REAL, session_id TEXT,
                unit_name TEXT, answer TEXT, image_read INTEGER NOT NULL DEFAULT 0,
                unsafe_tools INTEGER NOT NULL DEFAULT 0, failure_reason TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS runtime_one_active
                ON runtime_attempts((1)) WHERE state IN ('starting','running','draining');
            CREATE TABLE IF NOT EXISTS runtime_operations (
                operation_id TEXT PRIMARY KEY, correlation_id TEXT NOT NULL,
                operation_key TEXT NOT NULL, request_hash TEXT NOT NULL,
                attempt_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                request TEXT NOT NULL, result TEXT, updated_at REAL NOT NULL,
                UNIQUE(correlation_id,operation_key)
            );
            CREATE TABLE IF NOT EXISTS runtime_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                correlation_id TEXT, attempt_id TEXT, kind TEXT NOT NULL,
                created_at REAL NOT NULL, detail TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_controls (
                message_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL,
                answer TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS runtime_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT, correlation_id TEXT NOT NULL,
                answer TEXT NOT NULL, reason TEXT, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_schedules (
                schedule_id TEXT PRIMARY KEY, config TEXT NOT NULL, last_period TEXT
            );
            CREATE TABLE IF NOT EXISTS runtime_outbox (
                result_id INTEGER PRIMARY KEY REFERENCES runtime_results(result_id),
                correlation_id TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                card_id TEXT, delivered_at REAL
            );
        """)
        try:
            migrate_runtime_schema(self)
        except BaseException:
            self.close()
            raise

    @contextmanager
    def transaction(self):
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def meta(self, key, default=None):
        with self._lock:
            row = self._connection.execute(
                "SELECT value FROM runtime_meta WHERE key=?",
                (key,),
            ).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        with self._lock:
            self._connection.execute(
                "INSERT OR REPLACE INTO runtime_meta VALUES (?,?)",
                (key, json.dumps(value, ensure_ascii=False)),
            )

    def _audit(self, db, cid, aid, kind, detail=""):
        db.execute(
            "INSERT INTO runtime_events VALUES (NULL,?,?,?,?,?)",
            (cid, aid, kind, time.time(), detail[:1000]),
        )

    def discover(self, now):
        with self.transaction() as db:
            db.execute(
                """INSERT OR IGNORE INTO runtime_tasks
                (correlation_id,created_at,updated_at,task_category)
                SELECT correlation_id,updated_at,?,
                CASE WHEN lower(trim(user_text)) IN ('你好','嗨','hi','hello','hey')
                THEN 'greeting' ELSE 'business' END
                FROM events WHERE status='accepted'""",
                (now,),
            )

    def task(self, cid):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM runtime_tasks WHERE correlation_id=?",
                (cid,),
            ).fetchone()
        return dict(row) if row else None

    def attempt(self, aid):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM runtime_attempts WHERE attempt_id=?",
                (aid,),
            ).fetchone()
        return dict(row) if row else None

    def active(self):
        with self._lock:
            return [
                dict(r)
                for r in self._connection.execute(
                    "SELECT * FROM runtime_attempts "
                    "WHERE state IN ('starting','running','draining')",
                ).fetchall()
            ]

    def claim(self, now, session_id=None):
        self.discover(now)
        with self.transaction() as db:
            if db.execute(
                "SELECT 1 FROM runtime_attempts WHERE state IN ('starting','running','draining')"
            ).fetchone():
                return None
            row = db.execute(
                """SELECT t.*,e.sequence FROM runtime_tasks t
                JOIN events e USING(correlation_id)
                WHERE t.state IN ('queued','retry_wait') AND t.retry_at<=?
                    AND e.status='accepted'
                ORDER BY CASE t.state WHEN 'queued' THEN 0 ELSE 1 END, e.sequence LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None
            cid, aid, token = row["correlation_id"], uuid.uuid4().hex, secrets.token_urlsafe(32)
            unit = "feishu-worker-" + aid
            db.execute(
                """INSERT INTO runtime_attempts
                (attempt_id,correlation_id,token_hash,state,started_at,session_id,unit_name)
                VALUES (?,?,?,'starting',?,?,?)""",
                (aid, cid, hashlib.sha256(token.encode()).hexdigest(), now, session_id, unit),
            )
            db.execute(
                """UPDATE runtime_tasks SET state='running',attempt_id=?,
                updated_at=?,activity_at=?,cancel_requested=0,steps='[]' WHERE correlation_id=?""",
                (aid, now, now, cid),
            )
            db.execute(
                "UPDATE events SET status='delivered',attempts=attempts+1,updated_at=? "
                "WHERE correlation_id=?",
                (int(now), cid),
            )
            self._audit(db, cid, aid, "claimed")
        return {**self.attempt(aid), "token": token}

    def authenticate(self, aid, token, db=None):
        if db is None:
            with self._lock:
                return self.authenticate(aid, token, self._connection)
        row = db.execute(
            """SELECT a.*,t.cancel_requested,t.attempt_id AS current_attempt
            FROM runtime_attempts a JOIN runtime_tasks t USING(correlation_id)
            WHERE a.attempt_id=?""",
            (aid,),
        ).fetchone()
        if (
            row is None
            or row["current_attempt"] != aid
            or row["cancel_requested"]
            or row["state"] not in {"starting", "running"}
            or not hmac.compare_digest(
                row["token_hash"], hashlib.sha256(token.encode()).hexdigest()
            )
        ):
            raise PermissionError("Execution has ended or its authorization is no longer valid")
        return dict(row)

    def started(self, aid, session_id=None):
        with self._lock:
            self._connection.execute(
                "UPDATE runtime_attempts SET state='running',"
                "session_id=COALESCE(?,session_id) "
                "WHERE attempt_id=? AND state='starting'",
                (session_id, aid),
            )

    def record_activity(self, aid, now, steps=None, unsafe=False):
        with self.transaction() as db:
            row = db.execute(
                "SELECT correlation_id FROM runtime_attempts WHERE attempt_id=? "
                "AND state IN ('starting','running')",
                (aid,),
            ).fetchone()
            if not row:
                return
            db.execute(
                "UPDATE runtime_tasks SET activity_at=?,steps=COALESCE(?,steps),"
                "updated_at=? WHERE correlation_id=?",
                (
                    now,
                    json.dumps(steps, ensure_ascii=False) if steps is not None else None,
                    now,
                    row[0],
                ),
            )
            if unsafe:
                db.execute("UPDATE runtime_attempts SET unsafe_tools=1 WHERE attempt_id=?", (aid,))

    def image_read(self, aid, token):
        with self.transaction() as db:
            self.authenticate(aid, token, db)
            db.execute("UPDATE runtime_attempts SET image_read=1 WHERE attempt_id=?", (aid,))

    def submit_answer(self, aid, token, text, business_outcome="completed", *,
                      completion_scope="verified", evidence_gaps=None):
        if not isinstance(text, str) or not text.strip() or len(text) > 1_000_000:
            raise ValueError("Final answer must be nonempty and within the size limit")
        answer_pages(text)
        if business_outcome not in {"completed", "partial", "unanswered", "blocked"}:
            raise ValueError("Invalid business outcome")
        with self.transaction() as db:
            attempt = self.authenticate(aid, token, db)
            from .task_completion import apply_completion_contract

            text, business_outcome = apply_completion_contract(
                db, attempt["correlation_id"], text, business_outcome,
                completion_scope, [] if evidence_gaps is None else evidence_gaps,
            )
            message = db.execute(
                "SELECT message_type FROM events WHERE correlation_id=?",
                (attempt["correlation_id"],),
            ).fetchone()
            if message[0] == "image" and not attempt["image_read"]:
                raise ValueError("Read the attached image before submitting the result")
            if attempt["answer"] and (
                attempt["answer"] != text or attempt["business_outcome"] != business_outcome
            ):
                raise ValueError("A different final answer is already saved")
            db.execute(
                "UPDATE runtime_attempts SET answer=?,business_outcome=? WHERE attempt_id=?",
                (text, business_outcome, aid),
            )

    def drain(self, aid, reason=None):
        with self._lock:
            self._connection.execute(
                "UPDATE runtime_attempts SET state='draining',"
                "failure_reason=COALESCE(?,failure_reason) "
                "WHERE attempt_id=? AND state IN ('starting','running')",
                (reason, aid),
            )

    def _save_result(self, db, cid, text, reason, now, outcome=None, failure_operation_id=None):
        # Result and delivery intent are one atomic record. Delivery is not a scheduler lock.
        result = db.execute(
            "INSERT INTO runtime_results "
            "(correlation_id,answer,reason,created_at,business_outcome,failure_operation_id) "
            "VALUES (?,?,?,?,?,?)",
            (cid, text, reason, now, outcome, failure_operation_id),
        )
        db.execute(
            "UPDATE runtime_outbox SET state='superseded' "
            "WHERE correlation_id=? AND state='pending'",
            (cid,),
        )
        db.execute(
            "INSERT INTO runtime_outbox (result_id,correlation_id) VALUES (?,?)",
            (result.lastrowid, cid),
        )
        db.execute(
            """UPDATE events SET status='replying',reply_text=?,reply_format='card_v1',
            reply_chunks=?,chunks_sent=0,updated_at=?,failure_reason=? WHERE correlation_id=?""",
            (text, json.dumps([text], ensure_ascii=False), int(now), reason, cid),
        )

    def finish(self, aid, *, now, reason=None, max_retries=2, total_budget=1800, elapsed=None):
        """Only called AFTER the supervisor has confirmed the execution group is stopped."""
        with self.transaction() as db:
            row = db.execute(
                """SELECT a.*,t.retries,t.cancel_requested,t.total_seconds
                FROM runtime_attempts a JOIN runtime_tasks t USING(correlation_id)
                WHERE a.attempt_id=? AND a.state='draining' AND t.attempt_id=a.attempt_id""",
                (aid,),
            ).fetchone()
            if row is None:
                raise ValueError("Only the current drained attempt can be completed")
            cid = row["correlation_id"]
            spent = row["total_seconds"] + max(
                0, now - row["started_at"] if elapsed is None else elapsed
            )
            # A killed read is a failed attempt, never evidence of an uncertain write.
            pending = db.execute(
                "SELECT * FROM runtime_operations WHERE attempt_id=? AND state='running'", (aid,)
            ).fetchall()
            for operation in pending:
                op_state = "failed" if operation["effect_kind"] == "read" else "unknown"
                receipt = json.loads(operation["result"] or "{}")
                if operation["effect_kind"] == "read" and receipt.get("validated_response"):
                    semantic, _ = read_request(operation["kind"], json.loads(operation["request"]))
                    if response_valid(operation["kind"], receipt["validated_response"], semantic):
                        op_state = "succeeded"
                receipt.update(
                    reason_code="execution_interrupted_after_result"
                    if op_state == "succeeded" else "execution_interrupted",
                    retryable=operation["effect_kind"] == "read" and op_state != "succeeded",
                )
                encoded = json.dumps(receipt, ensure_ascii=False)
                db.execute(
                    "UPDATE runtime_operations SET state=?,result=?,updated_at=? "
                    "WHERE operation_id=?",
                    (op_state, encoded, now, operation["operation_id"]),
                )
                db.execute(
                    "UPDATE runtime_operation_attempts SET state=?,result=?,ended_at=?,"
                    "failure_reason='execution_interrupted' WHERE operation_attempt_id=? "
                    "AND state='running'",
                    (op_state, encoded, now, operation["operation_attempt_id"]),
                )
            operations = db.execute(
                "SELECT * FROM runtime_operations WHERE correlation_id=? ORDER BY updated_at",
                (cid,),
            ).fetchall()
            unknown = [op for op in operations if op["state"] == "unknown"]
            failed_reads = [
                op for op in operations if op["effect_kind"] == "read" and op["state"] == "failed"
            ]
            data_reads = [
                op
                for op in operations
                if op["kind"] in {"search_documents", "fetch_document", "libra_read"}
                and op["state"] == "succeeded"
            ]
            artifacts = []
            for op in operations:
                saved = json.loads(op["result"] or "{}")
                if (
                    op["kind"] == "create_document"
                    and op["state"] == "succeeded"
                    and saved.get("verified")
                    and saved.get("document", {}).get("url")
                ):
                    artifacts.append(saved["document"]["url"])
            cancelled = bool(row["cancel_requested"])
            answer = row["answer"]
            outcome = row["business_outcome"] or "unanswered"
            if (not answer and not cancelled
                    and (reason or row["failure_reason"]) != "permission_expired"):
                from .task_completion import _get, apply_completion_contract

                checkpoint = _get(db, "checkpoint:" + aid)
                if checkpoint:
                    answer, outcome = apply_completion_contract(
                        db, cid, checkpoint["text"], "partial", "verified",
                        checkpoint["evidence_gaps"],
                    )
            failure_op = unknown or (failed_reads if not data_reads else [])
            failure_op = failure_op[-1] if failure_op else None
            failure_id = failure_op["operation_id"] if failure_op else None
            failure_reason = reason or row["failure_reason"]
            finalization = db.execute("SELECT value FROM runtime_meta WHERE key=?",
                                      ("finalize:" + aid,)).fetchone()
            if finalization and outcome != "completed":
                reason = reason or json.loads(finalization[0])
            if unknown:
                outcome, reason = "blocked", "uncertain_operation"
            elif failed_reads and not data_reads and not artifacts:
                outcome = "unanswered"
                reason = json.loads(failed_reads[-1]["result"] or "{}").get(
                    "reason_code", "read_failed"
                )
            elif not answer and (artifacts or data_reads):
                outcome = "partial"
            if (
                answer
                and outcome == "completed"
                and not unknown
                and not cancelled
                and reason != "permission_expired"
            ):
                state = "succeeded"
            elif (
                not cancelled
                and not answer
                and not artifacts
                and reason in {"model_error", "worker_exit", "startup_error"}
                and not row["unsafe_tools"]
                and not unknown
                and row["retries"] < max_retries
                and spent < total_budget
            ):
                state = "retry_wait"
            else:
                state = (
                    "cancelled"
                    if cancelled
                    else "suspended"
                    if unknown or reason in {"task_timeout", "permission_expired"}
                    else "failed"
                )
                if outcome == "completed":
                    outcome = "partial" if artifacts or data_reads else "unanswered"
                reason = (
                    "cancelled"
                    if cancelled
                    else reason
                    or ("business_incomplete" if answer or artifacts else "missing_final_result")
                )
                if cancelled:
                    outcome = "partial" if artifacts else "unanswered"
                elif (artifacts or (data_reads and not row["answer"])) and outcome != "blocked":
                    outcome = "partial"
                summary = (
                    f"任务 #{self._sequence(db, cid)} 已保存部分成果，"
                    f"尚有证据或工作待补齐（{failure_label(reason)}）。"
                    if outcome == "partial" and not cancelled and not unknown
                    else f"任务 #{self._sequence(db, cid)} 已停止，原因：{failure_label(reason)}。"
                )
                if failure_op:
                    summary += "\n失败步骤：" + {
                        "cli_help": "查询工具帮助",
                        "search_documents": "搜索文档",
                        "fetch_document": "读取文档",
                        "libra_read": "查询实验数据",
                        "create_document": "创建文档",
                        "run": "执行命令",
                    }.get(failure_op["kind"], "外部操作")
                if answer:
                    summary += "\n\n已保存说明：\n" + answer
                if artifacts:
                    summary += "\n\n已确认的文档成果：\n" + "\n".join(artifacts)
                if data_reads:
                    summary += f"\n\n已保存 {len(data_reads)} 项读取结果，恢复时可复用。"
                    for operation in data_reads[-6:]:
                        detail = json.loads(operation["result"] or "{}")
                        summary += "\n- " + (detail.get("action") or operation["kind"])
                    if not answer:
                        summary += "\n这些结果尚未形成完整分析结论，不能据此判断实验放量。"
                recovery = recovery_options(db, cid, state=state)
                if recovery["needs_reconciliation"]:
                    summary += "\n\n存在待核验操作，请先核验已有成果；核验完成后再继续。"
                elif recovery["can_retry"]:
                    summary += (
                        f"\n\n可使用 `/continue {self._sequence(db, cid)}` 从已保存进度继续。"
                    )
                if row["unsafe_tools"]:
                    summary += "\n部分未受管操作可能已执行，继续时须先核查当前状态。"
                answer = summary
            if state == "succeeded" and artifacts:
                missing = [url for url in artifacts if url not in answer]
                if missing:
                    answer += "\n\n已确认的文档成果：\n" + "\n".join(missing)
            db.execute(
                "UPDATE runtime_attempts SET state=?,ended_at=?,failure_reason=?,"
                "business_outcome=? "
                "WHERE attempt_id=?",
                (state, now, failure_reason or reason, outcome, aid),
            )
            if state == "retry_wait":
                delay = (10, 30)[min(row["retries"], 1)]
                db.execute(
                    "UPDATE runtime_tasks SET state=?,retries=retries+1,retry_at=?,"
                    "warning=?,total_seconds=?,updated_at=? WHERE correlation_id=?",
                    (state, now + delay, "模型请求失败，正在有限次数恢复", spent, now, cid),
                )
                db.execute(
                    "UPDATE events SET status='accepted',updated_at=? WHERE correlation_id=?",
                    (int(now), cid),
                )
            else:
                self._save_result(db, cid, answer, reason, now, outcome, failure_id)
                db.execute(
                    "UPDATE runtime_tasks SET state=?,warning=?,total_seconds=?,updated_at=?,"
                    "business_outcome=?,failure_operation_id=? WHERE correlation_id=?",
                    (
                        state,
                        failure_label(reason) if reason else None,
                        spent,
                        now,
                        outcome,
                        failure_id,
                        cid,
                    ),
                )
            self._audit(db, cid, aid, state, reason or "")
        return state

    def acknowledge_result(self, cid, text, card_id, now, result_id):
        """A delayed sender must never complete a newer attempt/result."""
        with self.transaction() as db:
            changed = db.execute(
                "UPDATE events SET status='replied',chunks_sent=1,updated_at=? "
                "WHERE correlation_id=? AND status='replying' AND reply_text=? "
                "AND EXISTS (SELECT 1 FROM runtime_outbox WHERE correlation_id=? "
                "AND result_id=? AND state='pending')",
                (int(now), cid, text, cid, result_id),
            )
            if changed.rowcount:
                db.execute(
                    "UPDATE runtime_outbox SET state='sent',card_id=?,delivered_at=? "
                    "WHERE correlation_id=? AND result_id=? AND state='pending'",
                    (card_id, now, cid, result_id),
                )
            return bool(changed.rowcount)

    @staticmethod
    def _sequence(db, cid):
        return db.execute("SELECT sequence FROM events WHERE correlation_id=?", (cid,)).fetchone()[
            0
        ]

    def import_result(self, cid, text, now):
        answer_pages(text)
        with self.transaction() as db:
            if db.execute(
                "SELECT 1 FROM runtime_attempts WHERE state IN ('starting','running','draining')"
            ).fetchone():
                raise RuntimeError("Stop active workers before importing a verified result")
            event = db.execute(
                "SELECT reply_text FROM events WHERE correlation_id=?", (cid,)
            ).fetchone()
            if not event:
                raise ValueError("Unknown task")
            if event[0]:
                if event[0] != text:
                    raise ValueError("Existing result differs")
                return
            db.execute(
                "INSERT OR IGNORE INTO runtime_tasks "
                "(correlation_id,created_at,updated_at) VALUES (?,?,?)",
                (cid, now, now),
            )
            self._save_result(db, cid, text, None, now, "completed")
            db.execute(
                "UPDATE runtime_tasks SET state='succeeded',business_outcome='completed',"
                "task_category='recovery',updated_at=? "
                "WHERE correlation_id=?",
                (now, cid),
            )
            self._audit(db, cid, None, "verified_result_imported")

    def operation_begin(self, aid, token, key, kind, request, *, execution=None):
        if not isinstance(key, str) or not 1 <= len(key) <= 100:
            raise ValueError("operation_key must have 1-100 characters")
        encoded = json.dumps(request, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        if kind in READ_KINDS:
            semantic, options = read_request(kind, request)
            effect = "read"
        else:
            semantic = {
                k: v for k, v in request.items() if k not in {"operation_key", "timeout_seconds"}
            }
            options = execution or {"timeout_seconds": request.get("timeout_seconds", 120)}
            effect = "verified_write" if kind == "create_document" else "unclassified"
        semantic_hash = hashlib.sha256(
            json.dumps(semantic, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()
        with self.transaction() as db:
            attempt = self.authenticate(aid, token, db)
            cid = attempt["correlation_id"]
            if kind == "libra_read":
                db.execute("UPDATE runtime_tasks SET task_category='experiment_analysis' "
                           "WHERE correlation_id=?", (cid,))
            existing = db.execute(
                "SELECT * FROM runtime_operations WHERE correlation_id=? AND operation_key=?",
                (cid, key),
            ).fetchone()
            if existing:
                matches = (
                    existing["semantic_hash"] == semantic_hash
                    if existing["semantic_hash"]
                    else existing["request_hash"] == digest
                )
                if not matches or existing["kind"] != kind:
                    raise ValueError("Operation key already used with different input")
                if existing["state"] == "succeeded":
                    return dict(existing)
                saved_result = json.loads(existing["result"] or "{}")
                if (
                    effect != "read"
                    or existing["effect_kind"] != "read"
                    or existing["state"] != "failed"
                    or (not saved_result.get("retryable", True) and existing["attempt_id"] == aid)
                ):
                    raise ValueError(
                        "Operation was already attempted; inspect its saved result first"
                    )
            if (
                effect != "read"
                and db.execute(
                    "SELECT 1 FROM runtime_operations WHERE correlation_id=? AND state='unknown'",
                    (cid,),
                ).fetchone()
            ):
                raise ValueError("Resolve uncertain prior operations before new writes")
            op_id = existing["operation_id"] if existing else uuid.uuid4().hex
            now, op_aid = time.time(), uuid.uuid4().hex
            if existing:
                count = db.execute(
                    "SELECT count(*) FROM runtime_operation_attempts "
                    "WHERE operation_id=? AND attempt_id=?",
                    (op_id, aid),
                ).fetchone()[0]
                if count >= READ_LIMITS[kind][1]:
                    raise ValueError("Read retry budget exhausted for this task attempt")
                db.execute(
                    "UPDATE runtime_operations SET state='running',attempt_id=?,"
                    "operation_attempt_id=?,updated_at=? WHERE operation_id=?",
                    (aid, op_aid, now, op_id),
                )
            else:
                db.execute(
                    "INSERT INTO runtime_operations (operation_id,correlation_id,operation_key,"
                    "request_hash,attempt_id,kind,state,request,updated_at,effect_kind,"
                    "semantic_hash,operation_attempt_id) VALUES (?,?,?,?,?,?,'running',?,?,?,?,?)",
                    (
                        op_id,
                        cid,
                        key,
                        digest,
                        aid,
                        kind,
                        encoded,
                        now,
                        effect,
                        semantic_hash,
                        op_aid,
                    ),
                )
            db.execute(
                "INSERT INTO runtime_operation_attempts "
                "(operation_attempt_id,operation_id,attempt_id,state,execution,started_at) "
                "VALUES (?,?,?,'running',?,?)",
                (op_aid, op_id, aid, json.dumps(options), now),
            )
            return dict(
                db.execute(
                    "SELECT * FROM runtime_operations WHERE operation_id=?", (op_id,)
                ).fetchone()
            )

    def operation_checkpoint(self, aid, token, op_id, op_aid, result):
        with self.transaction() as db:
            self.authenticate(aid, token, db)
            encoded = json.dumps(result, ensure_ascii=False)
            changed = db.execute(
                "UPDATE runtime_operations SET result=?,updated_at=? WHERE operation_id=? "
                "AND attempt_id=? AND operation_attempt_id=? AND state='running'",
                (encoded, time.time(), op_id, aid, op_aid),
            )
            if changed.rowcount != 1:
                raise ValueError("Operation attempt has already ended")
            db.execute(
                "UPDATE runtime_operation_attempts SET result=? "
                "WHERE operation_attempt_id=? AND state='running'",
                (encoded, op_aid),
            )

    def operation_end(self, aid, token, op_id, state, result, *, operation_attempt_id=None):
        if state not in {"succeeded", "failed", "unknown"}:
            raise ValueError("Invalid operation outcome")
        with self.transaction() as db:
            self.authenticate(aid, token, db)
            current = db.execute(
                "SELECT * FROM runtime_operations WHERE operation_id=?", (op_id,)
            ).fetchone()
            if not current or (
                operation_attempt_id is not None
                and current["operation_attempt_id"] != operation_attempt_id
            ):
                raise ValueError("Operation attempt has already ended")
            if current["effect_kind"] == "read" and state == "unknown":
                raise ValueError("A trusted read failure cannot become an uncertain write")
            changed = db.execute(
                "UPDATE runtime_operations SET state=?,result=?,updated_at=? "
                "WHERE operation_id=? AND attempt_id=? AND state='running'",
                (state, json.dumps(result, ensure_ascii=False), time.time(), op_id, aid),
            )
            if changed.rowcount != 1:
                raise ValueError("Operation has already ended")
            db.execute(
                "UPDATE runtime_operation_attempts SET state=?,result=?,ended_at=?,"
                "failure_reason=? WHERE operation_attempt_id=? AND state='running'",
                (
                    state,
                    json.dumps(result, ensure_ascii=False),
                    time.time(),
                    result.get("reason_code"),
                    current["operation_attempt_id"],
                ),
            )

    def operation_attempts(self, op_id):
        with self._lock:
            return [
                dict(r)
                for r in self._connection.execute(
                    "SELECT * FROM runtime_operation_attempts WHERE operation_id=? "
                    "ORDER BY started_at",
                    (op_id,),
                )
            ]

    def operations(self, cid):
        with self._lock:
            return [
                dict(r)
                for r in self._connection.execute(
                    "SELECT * FROM runtime_operations WHERE correlation_id=? ORDER BY updated_at",
                    (cid,),
                ).fetchall()
            ]

    def context(self, limit=8):
        with self._lock:
            rows = self._connection.execute(
                """SELECT e.sequence,e.user_text,e.reply_text,t.state
                FROM runtime_tasks t JOIN events e USING(correlation_id)
                WHERE t.state IN ('succeeded','failed','suspended','cancelled')
                ORDER BY e.sequence DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [
            {
                "task": r["sequence"],
                "request": (r["user_text"] or "")[:2000],
                "result": (r["reply_text"] or "")[:4000],
                "state": r["state"],
            }
            for r in reversed(rows)
        ]

    def handle_control(self, message, *, existing=False, expected_attempt=...):
        """Owner/chat authentication is performed by FeishuEventHandler before this entry."""
        if message.message_type != "text" or not message.user_text:
            return False
        text = message.user_text.strip()
        natural = text.rstrip("？?！!。 ")
        if natural in {
            "你还在嘛",
            "你还在吗",
            "你在吗",
            "你在嘛",
            "这个任务你做完了吗",
            "任务完成了吗",
            "现在什么状态",
        }:
            text = "/status"
        words = text.split()
        if not words or words[0] not in {"/status", "/queue", "/cancel", "/continue", "/result"}:
            return False
        self.discover(time.time())
        with self.transaction() as db:
            if db.execute(
                "SELECT 1 FROM runtime_controls WHERE message_id=?", (message.message_id,)
            ).fetchone():
                return True
            command = words[0]
            number = words[1] if len(words) == 2 and words[1].isdigit() else None
            rows = db.execute(
                """SELECT t.*,e.sequence,e.reply_text,e.user_text FROM runtime_tasks t
                JOIN events e USING(correlation_id) WHERE e.chat_id=? AND e.message_id!=?
                AND (? IS NULL OR e.sequence=?)
                ORDER BY e.sequence DESC LIMIT 100""",
                (message.chat_id, message.message_id, number, number),
            ).fetchall()
            selected = (
                next((r for r in rows if str(r["sequence"]) == number), None)
                if number
                else (next((r for r in rows if r["state"] == "running"), rows[0] if rows else None))
            )
            if expected_attempt is not ... and (
                selected is None or selected["attempt_id"] != expected_attempt
            ):
                raise ValueError("This task action belongs to an older execution")
            if len(words) > 2 or (len(words) == 2 and number is None):
                answer = (
                    "格式：`/status`、`/queue`、`/result 任务编号`、"
                    "`/cancel 任务编号`、`/continue 任务编号`。"
                )
            elif command == "/queue":
                pending = [r for r in rows if r["state"] not in TERMINAL]
                answer = (
                    "当前没有排队任务。"
                    if not pending
                    else "\n".join(
                        f"#{r['sequence']} · {r['state']} · {(r['user_text'] or '')[:70]}"
                        for r in reversed(pending[:20])
                    )
                )
            elif not selected:
                answer = "机器人接收服务在线，当前没有匹配的任务。"
            elif command in {"/status", "/result"}:
                r = selected
                answer = (
                    f"机器人接收服务在线。\n\n任务 #{r['sequence']}："
                    f"{state_label(r['state'], r['business_outcome'])}。"
                    f"\n等待处理：{sum(x['state'] in {'queued', 'retry_wait'} for x in rows)} 条。"
                )
                recovery = recovery_options(db, r["correlation_id"])
                if recovery["needs_reconciliation"]:
                    answer += "\n存在待核验操作，核验完成后才可继续。"
                elif recovery["can_retry"]:
                    answer += f"\n可使用 `/continue {r['sequence']}` 继续。"
                if r["business_outcome"]:
                    answer += "\n业务结果：" + r["business_outcome"]
                if r["warning"]:
                    answer += "\n说明：" + r["warning"]
                if self.meta("model_retry_at", 0) > time.time():
                    answer += "\n模型服务暂不可用，队列已保留，稍后会自动探测恢复。"
                if r["reply_text"]:
                    answer += "\n\n已保存结果：\n" + r["reply_text"][:8000]
                elif r["state"] == "running":
                    answer += f"\n使用 `/cancel {r['sequence']}` 可停止本次执行。"
            elif command == "/cancel":
                r = selected
                if r["state"] in TERMINAL:
                    answer = f"任务 #{r['sequence']} 已结束，不需要取消。"
                elif r["state"] == "running":
                    db.execute(
                        "UPDATE runtime_tasks SET cancel_requested=1,updated_at=? "
                        "WHERE correlation_id=?",
                        (time.time(), r["correlation_id"]),
                    )
                    answer = f"正在取消任务 #{r['sequence']}；确认执行停止后会更新原任务卡。"
                else:
                    answer = f"任务 #{r['sequence']} 已取消。"
                    db.execute(
                        "UPDATE runtime_tasks SET state='cancelled',updated_at=? "
                        "WHERE correlation_id=?",
                        (time.time(), r["correlation_id"]),
                    )
                    self._save_result(db, r["correlation_id"], answer, "cancelled", time.time())
            else:
                r = selected
                if r["state"] not in {"failed", "cancelled", "suspended"}:
                    answer = (
                        f"任务 #{r['sequence']} 当前为{state_label(r['state'])}，无需重复启动。"
                    )
                elif recovery_options(db, r["correlation_id"])["needs_reconciliation"]:
                    answer = (
                        "该任务有结果不确定的外部操作，需先核验成果，不能直接重放。记录已保留。"
                    )
                else:
                    db.execute(
                        "UPDATE runtime_tasks SET state='queued',attempt_id=NULL,"
                        "cancel_requested=0,retries=0,"
                        "retry_at=0,total_seconds=0,business_outcome=NULL,failure_operation_id=NULL,"
                        "warning='从已保存记录继续',updated_at=? "
                        "WHERE correlation_id=?",
                        (time.time(), r["correlation_id"]),
                    )
                    db.execute(
                        "UPDATE events SET status='accepted',reply_text=NULL,reply_format=NULL,"
                        "reply_chunks=NULL,chunks_sent=0,failure_reason=NULL,updated_at=? "
                        "WHERE correlation_id=?",
                        (int(time.time()), r["correlation_id"]),
                    )
                    db.execute(
                        "UPDATE runtime_outbox SET state='superseded' "
                        "WHERE correlation_id=? AND state='pending'",
                        (r["correlation_id"],),
                    )
                    self._audit(db, r["correlation_id"], None, "continue_requested")
                    answer = f"任务 #{r['sequence']} 已重新排队，将先核查已有操作与成果。"
            if existing:
                row = db.execute(
                    "SELECT correlation_id FROM events WHERE message_id=?", (message.message_id,)
                ).fetchone()
                cid = row[0]
                db.execute(
                    "UPDATE runtime_tasks SET task_category='control',"
                    "business_outcome='completed' WHERE correlation_id=?",
                    (cid,),
                )
                self._save_result(db, cid, answer, None, time.time())
                db.execute(
                    "UPDATE runtime_tasks SET state='succeeded',updated_at=? "
                    "WHERE correlation_id=?",
                    (time.time(), cid),
                )
                db.execute(
                    "INSERT INTO runtime_controls VALUES (?,?,?,1,0,0)",
                    (message.message_id, message.chat_id, answer),
                )
            else:
                db.execute(
                    "INSERT INTO runtime_controls VALUES (?,?,?,?,0,0)",
                    (
                        message.message_id,
                        message.chat_id,
                        answer,
                        int(message.message_id.startswith("callback:")),
                    ),
                )
        return True


def state_label(state, business_outcome=None):
    if state in {"failed", "suspended"} and business_outcome == "partial":
        return "部分完成，已有成果可查看或继续补充"
    return {
        "queued": "排队中",
        "running": "执行中",
        "retry_wait": "等待自动恢复",
        "succeeded": "执行完成",
        "failed": "执行失败，后续任务可继续",
        "suspended": "已挂起",
        "cancelled": "已取消",
    }.get(state, state)


def failure_label(reason):
    return {
        "model_error": "模型服务请求失败",
        "worker_exit": "执行器异常退出",
        "startup_error": "执行器启动失败",
        "idle_timeout": "执行长时间无响应",
        "task_timeout": "超过本次执行时间预算",
        "cancelled": "用户取消",
        "restart_interrupted": "服务重启，执行已安全停止",
        "missing_final_result": "执行结束但没有最终结果",
        "invalid_result": "结果未通过核验",
        "permission_expired": "等待授权已超时",
        "permission_policy_error": "机器人自动授权策略异常，需要维护",
        "soft_deadline": "已到收尾时间，交付当前已确认结果",
        "repeated_tool_error": "同一步骤连续失败，已停止重复尝试",
        "invalid_arguments": "查询参数错误，需要纠正后继续",
        "rate_limited": "查询触发限流，重试预算已用完",
        "transient_read_error": "查询暂时不可用，重试预算已用完",
        "uncertain_operation": "外部操作结果不确定，需要核验",
        "business_incomplete": "业务尚未完成",
        "query_budget": "查询预算已用完，交付已确认成果",
        "evidence_budget": "证据阅读预算已用完，交付已确认成果",
        "read_failed": "读取未成功",
        "command_timeout": "读取命令超时",
        "execution_interrupted": "操作执行中断",
        "needs_auth": "需要重新授权，授权后可继续",
        "access_denied": "没有访问权限，请先确认权限",
        "spawn_failed": "工具启动失败",
        "runtime_unavailable": "工具运行环境不完整，需要维护运行环境",
        "output_limit": "工具输出超过限制",
        "invalid_response": "工具返回格式未通过验证",
        "command_error": "工具执行异常",
        "process_cleanup_failed": "工具进程未确认停止，执行已隔离",
    }.get(reason, "执行中断")
