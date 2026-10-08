import json
import os
from pathlib import Path

import pytest

from feishu_llm_bot.config import ConfigError, Settings, load_credentials


def credentials(tmp_path: Path, mode: int = 0o600) -> Path:
    path = tmp_path / "credentials.json"
    path.write_text(json.dumps({"app_id": "app-id", "app_secret": "app-secret"}))
    path.chmod(mode)
    return path


def environment(path: Path, db: Path) -> dict[str, str]:
    return {
        "FEISHU_BOT_CREDENTIALS_FILE": str(path),
        "FEISHU_ALLOWED_SENDER_OPEN_ID": "ou_allowed",
        "FEISHU_BOT_DB_PATH": str(db),
    }


def test_settings_loads_private_credentials_without_model_configuration(tmp_path: Path) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env.update(
        {
            "FEISHU_BOT_MODEL": "obsolete-model",
            "ANTHROPIC_BASE_URL": "https://example.test",
            "ANTHROPIC_AUTH_TOKEN": "obsolete-token",
        }
    )
    settings = Settings.from_env(env)
    assert settings.app_id == "app-id"
    assert settings.app_secret == "app-secret"
    assert settings.allowed_sender_open_id == "ou_allowed"
    assert settings.database_path == tmp_path / "bot.db"
    assert settings.attachment_path == tmp_path / "attachments"
    assert settings.max_inbound_chars == 100_000
    assert settings.max_image_bytes == 5 * 1024 * 1024
    assert settings.max_attachment_bytes_total == 50 * 1024 * 1024
    assert not hasattr(settings, "model")


def test_insecure_credentials_are_rejected(tmp_path: Path) -> None:
    path = credentials(tmp_path, mode=0o644)
    with pytest.raises(ConfigError, match="group or others"):
        load_credentials(path)


def test_missing_allowlist_is_rejected(tmp_path: Path) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    del env["FEISHU_ALLOWED_SENDER_OPEN_ID"]
    with pytest.raises(ConfigError, match="OPEN_ID"):
        Settings.from_env(env)


@pytest.mark.parametrize(
    "name",
    [
        "FEISHU_BOT_REPLY_CHUNK_CHARS",
        "FEISHU_BOT_MAX_INBOUND_CHARS",
        "FEISHU_BOT_MAX_IMAGE_BYTES",
        "FEISHU_BOT_MAX_IMAGE_PIXELS",
        "FEISHU_BOT_MAX_IMAGE_SIDE",
        "FEISHU_BOT_MAX_ATTACHMENT_BYTES_TOTAL",
        "FEISHU_BOT_QUEUE_SIZE",
        "FEISHU_PERMISSION_TIMEOUT_SECONDS",
        "FEISHU_PERMISSION_MAX_PENDING",
    ],
)
def test_invalid_positive_integer_is_rejected(tmp_path: Path, name: str) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env[name] = "0"
    with pytest.raises(ConfigError, match="positive"):
        Settings.from_env(env)


def test_permission_relay_configuration_is_validated(tmp_path: Path) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env["FEISHU_PERMISSION_RELAY_ENABLED"] = "true"
    with pytest.raises(ConfigError, match="SESSION_ID"):
        Settings.from_env(env)

    env["FEISHU_PERMISSION_SESSION_ID"] = "not-a-uuid"
    env["FEISHU_PERMISSION_CHAT_ID"] = "chat-1"
    env["FEISHU_PERMISSION_SOCKET_PATH"] = str(tmp_path / "relay" / "permission.sock")
    with pytest.raises(ConfigError, match="must be a UUID"):
        Settings.from_env(env)

    env["FEISHU_PERMISSION_SESSION_ID"] = "64362c4a-5246-489b-bbb9-4e3922196538"
    settings = Settings.from_env(env)
    assert settings.permission_relay_enabled
    assert settings.permission_chat_id == "chat-1"
    assert settings.permission_socket_path == tmp_path / "relay" / "permission.sock"
    assert settings.permission_timeout_seconds == 600
    assert settings.permission_max_pending == 8


def test_disabled_permission_relay_needs_no_extra_configuration(tmp_path: Path) -> None:
    settings = Settings.from_env(environment(credentials(tmp_path), tmp_path / "bot.db"))
    assert not settings.permission_relay_enabled
    assert settings.permission_session_id is None
    assert settings.permission_socket_path is None


def test_invalid_permission_relay_boolean_is_rejected(tmp_path: Path) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env["FEISHU_PERMISSION_RELAY_ENABLED"] = "sometimes"
    with pytest.raises(ConfigError, match="true or false"):
        Settings.from_env(env)


def test_image_limit_cannot_exceed_mcp_protocol_limit(tmp_path: Path) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env["FEISHU_BOT_MAX_IMAGE_BYTES"] = str(5 * 1024 * 1024 + 1)
    with pytest.raises(ConfigError, match="5 MiB MCP protocol limit"):
        Settings.from_env(env)


def test_total_attachment_limit_cannot_be_smaller_than_single_image_limit(
    tmp_path: Path,
) -> None:
    env = environment(credentials(tmp_path), tmp_path / "bot.db")
    env["FEISHU_BOT_MAX_IMAGE_BYTES"] = "10"
    env["FEISHU_BOT_MAX_ATTACHMENT_BYTES_TOTAL"] = "9"
    with pytest.raises(ConfigError, match="must be at least"):
        Settings.from_env(env)


def test_credential_file_owner_is_current_user(tmp_path: Path) -> None:
    assert credentials(tmp_path).stat().st_uid == os.getuid()
