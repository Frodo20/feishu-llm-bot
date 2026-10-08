from __future__ import annotations

import argparse
import json
import os
import plistlib
import stat
import time
from pathlib import Path

import pytest

from feishu_llm_bot import setup_cli
from feishu_llm_bot.backends import check_backend_binding, executable
from feishu_llm_bot.runtime_common import load_config
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.task_tools import invoke
from feishu_llm_bot.worker import build_prompt


@pytest.fixture
def options(tmp_path, monkeypatch):
    monkeypatch.setenv("FEISHU_APP_SECRET", "local-test-secret")
    fake = tmp_path / "agent executable"
    fake.write_text('#!/bin/sh\necho "--fork-session --output-format --settings --listen"\n')
    fake.chmod(0o700)
    return argparse.Namespace(
        backend="claude", state_dir=tmp_path / "state with spaces $ %",
        cwd=tmp_path, owner="ou_local_test", session=None, runner="process",
        name="Test bot", model=None, model_provider=None, worker_access="workspace",
        integration=[], agent_command=str(fake), node_command=str(fake),
        credentials_file=None, app_id="cli_local_test",
    )


@pytest.mark.parametrize("backend", ["claude", "traex"])
def test_new_instance_is_private_portable_and_has_no_runtime_side_effect(options, backend):
    options.backend = backend
    path = setup_cli.initialize(options)
    config, env = load_config(path)
    assert config["agent_backend"] == backend
    assert config["integrations"] == []
    assert config["model"] is None
    assert config[backend + "_command"] == options.agent_command
    assert env["FEISHU_PROGRESS_STATE_PATH"] == config["database_path"]
    assert not Path(config["database_path"]).exists()
    assert not (path.parent / "tasks").exists()
    for name in ["credentials.json", "runtime.json", "env"]:
        assert stat.S_IMODE((path.parent / name).stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert "local-test-secret" not in path.read_text()
    assert "local-test-secret" not in (path.parent / "env").read_text()
    before = path.read_bytes()
    with pytest.raises(ValueError, match="not empty"):
        setup_cli.initialize(options)
    assert path.read_bytes() == before
    assert setup_cli.status(path)["active"] == []
    assert not Path(config["database_path"]).exists()


def test_initialization_rejects_unknown_owner_and_invalid_seed_without_writing(options):
    options.owner = "another-user"
    with pytest.raises(ValueError, match="open_id"):
        setup_cli.initialize(options)
    assert not options.state_dir.exists()
    options.owner = "ou_test"
    options.session = "not-a-uuid"
    with pytest.raises(ValueError):
        setup_cli.initialize(options)
    assert not options.state_dir.exists()


def test_doctor_missing_backend_reports_failure_without_creating_database(options):
    path = setup_cli.initialize(options)
    Path(options.agent_command).unlink()
    checks = setup_cli.diagnose(path)
    assert not next(c for c in checks if c["check"] == "claude_command")["ok"]
    assert not next(c for c in checks if c["check"] == "claude_protocol")["ok"]
    assert not (path.parent / "bot.sqlite3").exists()


def test_service_rendering_quotes_paths_and_preserves_environment_dollars(options, monkeypatch):
    path = setup_cli.initialize(options)
    config = json.loads(path.read_text())
    config["path"] = "/tmp/literal$bin:/usr/bin"
    path.write_text(json.dumps(config))
    monkeypatch.setattr(setup_cli.platform, "system", lambda: "Linux")
    unit = setup_cli.service_files(path, path.parent / "units").read_text()
    assert 'Environment="PATH=/tmp/literal$bin:/usr/bin"' in unit
    assert f"WorkingDirectory={config['project_dir'].replace('%', '%%')}\n" in unit
    assert 'state with spaces $$ %%' in unit
    assert "local-test-secret" not in unit
    monkeypatch.setattr(setup_cli.platform, "system", lambda: "Darwin")
    plist = plistlib.loads(setup_cli.service_files(path, path.parent / "units").read_bytes())
    assert plist["ProgramArguments"][-1] == str(path)
    assert plist["EnvironmentVariables"]["PATH"] == config["path"]


def test_backend_cannot_reinterpret_legacy_or_existing_session(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    try:
        store.set_meta("session_id", "existing-claude")
        with pytest.raises(ValueError, match="Claude session"):
            check_backend_binding(store, {"agent_backend": "traex"})
        assert store.meta("agent_backend") is None
        check_backend_binding(store, {})
        with pytest.raises(ValueError, match="new state"):
            check_backend_binding(store, {"agent_backend": "traex"})
    finally:
        store.close()


def test_disabled_integrations_are_absent_from_prompt_and_refused_by_host(tmp_path):
    store = RuntimeStore(tmp_path / "bot.sqlite3")
    try:
        store.accept_event("one", "test-chat", "hello")
        attempt = store.claim(time.time())
        request = {**attempt, "config": {"integrations": [], "database_path": str(store.path)}}
        path = tmp_path / "request.json"
        path.write_text(json.dumps(request))
        prompt = build_prompt(store, request)
        assert "mcp__feishu__libra_read" not in prompt
        assert "mcp__feishu__create_document" not in prompt
        assert "mcp__feishu__run" in prompt
        for name in ["libra_read", "fetch_document", "create_document"]:
            with pytest.raises(ValueError, match="not enabled"):
                invoke(path, name, {})
        assert store.operations(attempt["correlation_id"]) == []
    finally:
        store.close()


def test_executable_discovery_uses_configured_path_without_shell(options):
    command = Path(options.agent_command)
    assert executable({"path": str(command.parent)}, "agent", command.name) == str(command)
    with pytest.raises(ValueError):
        executable({"agent": "agent; echo unwanted"}, "agent")
    assert os.path.isabs(executable({"agent": str(command)}, "agent"))


def test_custom_agent_home_is_retained_and_nested_worker_identity_is_cleared(options, monkeypatch):
    from feishu_llm_bot.backends import clean_environment
    from feishu_llm_bot.operation_contracts import cli_environment

    custom = options.cwd / "custom trae home"
    monkeypatch.setenv("TRAECLI_HOME", str(custom))
    path = setup_cli.initialize(options)
    config, _ = load_config(path)
    assert config["agent_environment"]["TRAECLI_HOME"] == str(custom)
    env = clean_environment(cli_environment(config, {
        "FEISHU_WORKER_REQUEST": "outer-request", "TRAECLI_THREAD_ID": "outer-thread",
    }))
    assert env["TRAECLI_HOME"] == str(custom)
    assert "FEISHU_WORKER_REQUEST" not in env
    assert "TRAECLI_THREAD_ID" not in env


def test_release_allowlist_does_not_copy_secrets_or_follow_local_dependencies(tmp_path):
    import importlib.util
    import tarfile

    project = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "release_builder", project / "scripts/build_portable_release.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    target = tmp_path / "release.tar.gz"
    result = module.build(project, target)
    assert result["files"] > 50
    with tarfile.open(target) as archive:
        names = archive.getnames()
        assert "feishu-llm-bot/scripts/install.py" in names
        assert "feishu-llm-bot/src/feishu_llm_bot/traex_backend.py" in names
        assert not any(any(x in n.split("/") for x in
                           (".venv", "node_modules", "credentials.json", "bot.sqlite3", ".env"))
                       for n in names)
        manifest = json.load(archive.extractfile("feishu-llm-bot/MANIFEST.json"))
        import hashlib

        for name, digest in manifest["files"].items():
            content = archive.extractfile("feishu-llm-bot/" + name).read()
            assert hashlib.sha256(content).hexdigest() == digest
    with pytest.raises(FileExistsError):
        module.build(project, target)
