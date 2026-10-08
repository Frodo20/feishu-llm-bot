from __future__ import annotations

import json
import os
import stat
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class ConfigError(ValueError):
    """Raised when service configuration is unsafe or incomplete."""


_MAX_MCP_IMAGE_BYTES = 5 * 1024 * 1024


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be positive")
    return value


def _enabled(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name, "true" if default else "false").strip().lower()
    if raw in {"1", "true", "yes"}:
        return True
    if raw in {"0", "false", "no"}:
        return False
    raise ConfigError(f"{name} must be true or false")


@dataclass(frozen=True)
class Settings:
    app_id: str
    app_secret: str
    allowed_sender_open_id: str
    database_path: Path
    attachment_path: Path
    reply_chunk_chars: int = 3500
    max_inbound_chars: int = 100_000
    max_image_bytes: int = 5 * 1024 * 1024
    max_image_pixels: int = 25_000_000
    max_image_side: int = 8192
    max_attachment_bytes_total: int = 50 * 1024 * 1024
    queue_size: int = 128
    permission_relay_enabled: bool = False
    permission_session_id: str | None = None
    permission_chat_id: str | None = None
    permission_socket_path: Path | None = None
    permission_timeout_seconds: int = 600
    permission_max_pending: int = 8
    permission_all_sessions: bool = False
    permission_cards_enabled: bool = False
    progress_state_path: Path | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        values = os.environ if env is None else env
        credential_file = values.get("FEISHU_BOT_CREDENTIALS_FILE", "")
        if not credential_file:
            raise ConfigError("FEISHU_BOT_CREDENTIALS_FILE is required")

        credentials = load_credentials(Path(credential_file))
        allowed_sender = values.get("FEISHU_ALLOWED_SENDER_OPEN_ID", "").strip()
        if not allowed_sender:
            raise ConfigError("FEISHU_ALLOWED_SENDER_OPEN_ID is required")

        database = values.get("FEISHU_BOT_DB_PATH", "").strip()
        if not database:
            raise ConfigError("FEISHU_BOT_DB_PATH is required")

        database_path = Path(database).expanduser()
        attachment = values.get("FEISHU_BOT_ATTACHMENT_DIR", "").strip()
        attachment_path = (
            Path(attachment).expanduser() if attachment else database_path.parent / "attachments"
        )
        max_image_bytes = _positive_int(values, "FEISHU_BOT_MAX_IMAGE_BYTES", _MAX_MCP_IMAGE_BYTES)
        if max_image_bytes > _MAX_MCP_IMAGE_BYTES:
            raise ConfigError(
                "FEISHU_BOT_MAX_IMAGE_BYTES must not exceed the 5 MiB MCP protocol limit"
            )
        max_total_bytes = _positive_int(
            values, "FEISHU_BOT_MAX_ATTACHMENT_BYTES_TOTAL", 50 * 1024 * 1024
        )
        if max_total_bytes < max_image_bytes:
            raise ConfigError(
                "FEISHU_BOT_MAX_ATTACHMENT_BYTES_TOTAL must be at least FEISHU_BOT_MAX_IMAGE_BYTES"
            )

        permission_relay_enabled = _enabled(values, "FEISHU_PERMISSION_RELAY_ENABLED")
        progress_path = values.get("FEISHU_PROGRESS_STATE_PATH", "").strip()
        progress_state_path = Path(progress_path).expanduser() if progress_path else None
        if progress_state_path is not None and not progress_state_path.is_absolute():
            raise ConfigError("FEISHU_PROGRESS_STATE_PATH must be absolute")
        permission_session_id = values.get("FEISHU_PERMISSION_SESSION_ID", "").strip() or None
        permission_chat_id = values.get("FEISHU_PERMISSION_CHAT_ID", "").strip() or None
        permission_socket = values.get("FEISHU_PERMISSION_SOCKET_PATH", "").strip()
        permission_socket_path = Path(permission_socket).expanduser() if permission_socket else None
        if permission_relay_enabled:
            if permission_session_id is None:
                raise ConfigError(
                    "FEISHU_PERMISSION_SESSION_ID is required when permission relay is enabled"
                )
            try:
                uuid.UUID(permission_session_id)
            except ValueError as exc:
                raise ConfigError("FEISHU_PERMISSION_SESSION_ID must be a UUID") from exc
            if permission_socket_path is None or not permission_socket_path.is_absolute():
                raise ConfigError(
                    "FEISHU_PERMISSION_SOCKET_PATH must be absolute when the relay is enabled"
                )
            if permission_socket_path.parent == permission_socket_path:
                raise ConfigError(
                    "FEISHU_PERMISSION_SOCKET_PATH must have a private parent directory"
                )

        return cls(
            app_id=credentials["app_id"],
            app_secret=credentials["app_secret"],
            allowed_sender_open_id=allowed_sender,
            database_path=database_path,
            attachment_path=attachment_path,
            reply_chunk_chars=_positive_int(values, "FEISHU_BOT_REPLY_CHUNK_CHARS", 3500),
            max_inbound_chars=_positive_int(values, "FEISHU_BOT_MAX_INBOUND_CHARS", 100_000),
            max_image_bytes=max_image_bytes,
            max_image_pixels=_positive_int(values, "FEISHU_BOT_MAX_IMAGE_PIXELS", 25_000_000),
            max_image_side=_positive_int(values, "FEISHU_BOT_MAX_IMAGE_SIDE", 8192),
            max_attachment_bytes_total=max_total_bytes,
            queue_size=_positive_int(values, "FEISHU_BOT_QUEUE_SIZE", 128),
            permission_relay_enabled=permission_relay_enabled,
            permission_session_id=permission_session_id,
            permission_chat_id=permission_chat_id,
            permission_socket_path=permission_socket_path,
            permission_timeout_seconds=_positive_int(
                values, "FEISHU_PERMISSION_TIMEOUT_SECONDS", 600
            ),
            permission_max_pending=_positive_int(values, "FEISHU_PERMISSION_MAX_PENDING", 8),
            permission_all_sessions=_enabled(values, "FEISHU_PERMISSION_ALL_SESSIONS"),
            permission_cards_enabled=_enabled(values, "FEISHU_PERMISSION_CARDS_ENABLED"),
            progress_state_path=progress_state_path,
        )


def load_credentials(path: Path) -> dict[str, str]:
    try:
        info = path.stat()
    except FileNotFoundError as exc:
        raise ConfigError("Feishu credential file does not exist") from exc
    if not stat.S_ISREG(info.st_mode):
        raise ConfigError("Feishu credential path is not a regular file")
    if info.st_uid != os.getuid():
        raise ConfigError("Feishu credential file must be owned by the current user")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise ConfigError("Feishu credential file must not be accessible by group or others")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError("Feishu credential file is not valid JSON") from exc

    app_id = payload.get("app_id")
    app_secret = payload.get("app_secret")
    if (
        not isinstance(app_id, str)
        or not app_id
        or not isinstance(app_secret, str)
        or not app_secret
    ):
        raise ConfigError("Feishu credential file must contain app_id and app_secret")
    return {"app_id": app_id, "app_secret": app_secret}
