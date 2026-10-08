"""Transactional runtime schema upgrades, independent of the legacy events user_version."""

import json

RUNTIME_SCHEMA_VERSION = 2


def migrate_runtime_schema(store):
    with store.transaction() as db:
        row = db.execute(
            "SELECT value FROM runtime_meta WHERE key='runtime_schema_version'"
        ).fetchone()
        version = json.loads(row[0]) if row else 1
        if type(version) is not int or version > RUNTIME_SCHEMA_VERSION or version < 1:
            raise RuntimeError("Unsupported runtime schema version")
        if version == RUNTIME_SCHEMA_VERSION:
            return
        if db.execute(
            "SELECT 1 FROM runtime_attempts WHERE state IN ('starting','running','draining')"
        ).fetchone():
            raise RuntimeError("Stop and drain old workers before upgrading the runtime schema")
        for table, columns in {
            "runtime_operations": [
                "effect_kind TEXT NOT NULL DEFAULT 'unclassified'",
                "semantic_hash TEXT",
                "operation_attempt_id TEXT",
            ],
            "runtime_attempts": ["business_outcome TEXT"],
            "runtime_tasks": [
                "business_outcome TEXT",
                "failure_operation_id TEXT",
                "task_category TEXT NOT NULL DEFAULT 'unclassified'",
            ],
            "runtime_results": ["business_outcome TEXT", "failure_operation_id TEXT"],
        }.items():
            for column in columns:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column}")
        db.execute("""CREATE TABLE runtime_operation_attempts (
            operation_attempt_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL, state TEXT NOT NULL,
            execution TEXT NOT NULL, started_at REAL NOT NULL, ended_at REAL,
            result TEXT, failure_reason TEXT,
            FOREIGN KEY(operation_id) REFERENCES runtime_operations(operation_id)
        )""")
        db.execute("""CREATE UNIQUE INDEX runtime_operation_one_active
            ON runtime_operation_attempts(operation_id) WHERE state='running'""")
        db.execute(
            "INSERT OR REPLACE INTO runtime_meta VALUES ('runtime_schema_version',?)",
            (json.dumps(RUNTIME_SCHEMA_VERSION),),
        )
        store._audit(
            db, None, None, "runtime_schema_upgraded", "1 -> 2; legacy effects unclassified"
        )
