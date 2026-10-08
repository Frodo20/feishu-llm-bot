from __future__ import annotations

import io
import json
import logging
import sys

import pytest

import feishu_llm_bot.bridge as bridge


def test_configure_logging_reserves_stdout_for_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)

    lark_logger = logging.getLogger("Lark")
    original_handlers = list(lark_logger.handlers)
    original_propagate = lark_logger.propagate
    try:
        lark_logger.handlers = [logging.StreamHandler(sys.stdout)]
        lark_logger.propagate = False

        bridge.configure_logging("secret")
        lark_logger.error("contains secret")
        bridge.ProtocolWriter().write({"event": "ready"})

        assert stdout.getvalue() == '{"event":"ready"}\n'
        assert "contains [REDACTED]" in stderr.getvalue()
        assert "secret" not in stderr.getvalue()
    finally:
        lark_logger.handlers = original_handlers
        lark_logger.propagate = original_propagate


def test_validate_correlation_id_is_strict() -> None:
    valid = "fs_0123456789abcdef0123456789abcdef"
    assert bridge._validate_correlation_id(valid) == valid  # noqa: SLF001

    for invalid in (None, 1, "fs_short", "fs_0123456789ABCDEF0123456789ABCDEF"):
        with pytest.raises(ValueError, match="correlation_id"):
            bridge._validate_correlation_id(invalid)  # noqa: SLF001


def test_protocol_writer_emits_one_json_line(monkeypatch: pytest.MonkeyPatch) -> None:
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)

    bridge.ProtocolWriter().write({"text": "你好"})

    assert json.loads(stdout.getvalue()) == {"text": "你好"}
    assert stdout.getvalue().count("\n") == 1
