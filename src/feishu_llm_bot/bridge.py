from __future__ import annotations

import _thread
import json
import logging
import os
import re
import signal
import sys
import threading
from typing import Any

from .attachments import AttachmentStore
from .card_delivery import CardPager
from .config import ConfigError, Settings
from .feishu import FeishuClient, FeishuEventHandler, build_websocket_client
from .health import websocket_connected
from .permission_relay import PermissionRelay
from .service import SessionBridgeService
from .store import EventRecord, Store

LOGGER = logging.getLogger(__name__)
_CORRELATION_ID = re.compile(r"fs_[0-9a-f]{32}\Z")
_MAX_REQUEST_ID = 2**53 - 1
_MAX_PROTOCOL_LINE_BYTES = 8 * 1024 * 1024
_MAX_REPLY_CHARS = 1_000_000
_EXPECTED_PARAMS = {
    "mark_delivered": {"correlation_id"},
    "read_image": {"correlation_id"},
    "reply": {"correlation_id", "text"},
}


class SensitiveDataFilter(logging.Filter):
    def __init__(self, secrets: tuple[str, ...]) -> None:
        super().__init__()
        self._secrets = tuple(secret for secret in secrets if secret)

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in self._secrets:
            message = message.replace(secret, "[REDACTED]")
        record.msg = message
        record.args = ()
        return True


def configure_logging(*secrets: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(SensitiveDataFilter(tuple(secrets)))
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    lark_logger = logging.getLogger("Lark")
    lark_logger.handlers.clear()
    lark_logger.propagate = True


def _validate_correlation_id(value: object) -> str:
    if not isinstance(value, str) or _CORRELATION_ID.fullmatch(value) is None:
        raise ValueError("bridge request has an invalid correlation_id")
    return value


class ProtocolWriter:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def write(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            sys.stdout.write(encoded + "\n")
            sys.stdout.flush()


def run() -> None:
    settings = Settings.from_env()
    configure_logging(settings.app_id, settings.app_secret)
    writer = ProtocolWriter()
    store = Store(settings.database_path)
    try:
        counts = store.status_counts()
        LOGGER.info(
            "delivery state accepted=%d ambiguous=%d waiting=%d replying=%d replied=%d failed=%d",
            counts.get("accepted", 0),
            counts.get("dispatching", 0),
            counts.get("delivered", 0),
            counts.get("replying", 0),
            counts.get("replied", 0),
            counts.get("failed", 0),
        )
        client = FeishuClient(app_id=settings.app_id, app_secret=settings.app_secret)
        attachments = AttachmentStore(
            settings.attachment_path,
            max_image_bytes=settings.max_image_bytes,
            max_image_pixels=settings.max_image_pixels,
            max_image_side=settings.max_image_side,
            max_total_bytes=settings.max_attachment_bytes_total,
        )

        def emit_inbound(event: EventRecord) -> None:
            payload = {
                "event": "inbound",
                "message_type": event.message_type,
                "correlation_id": event.correlation_id,
            }
            if event.message_type == "text":
                payload["content"] = event.user_text
            elif event.user_text:
                payload["caption"] = event.user_text
            writer.write(payload)

        service = SessionBridgeService(
            store=store,
            replies=client,
            emit_inbound=emit_inbound,
            reply_chunk_chars=settings.reply_chunk_chars,
            max_inbound_chars=settings.max_inbound_chars,
            queue_size=settings.queue_size,
            attachments=attachments,
            progress_state_path=settings.progress_state_path,
        )
        permission_relay = None
        if settings.permission_relay_enabled:
            if settings.permission_session_id is None or settings.permission_socket_path is None:
                raise RuntimeError("permission relay configuration is incomplete")
            permission_relay = PermissionRelay(
                store=store,
                replies=client,
                socket_path=settings.permission_socket_path,
                session_id=settings.permission_session_id,
                chat_id=settings.permission_chat_id,
                timeout_seconds=settings.permission_timeout_seconds,
                max_pending=settings.permission_max_pending,
                all_sessions=settings.permission_all_sessions,
                cards_enabled=settings.permission_cards_enabled,
                allowed_sender_open_id=settings.allowed_sender_open_id,
                app_id=settings.app_id,
            )
        handler = FeishuEventHandler(
            allowed_sender_open_id=settings.allowed_sender_open_id,
            sink=service.accept,
            control_handler=(
                permission_relay.handle_control if permission_relay is not None else None
            ),
            reject_message=service.reject_message,
        )
        pager = CardPager(
            settings.progress_state_path, app_id=settings.app_id,
            allowed_sender=settings.allowed_sender_open_id,
        ) if settings.progress_state_path is not None else None

        def handle_card(data):
            action = getattr(getattr(data, "event", None), "action", None)
            value = getattr(action, "value", None)
            if (pager is not None and isinstance(value, dict)
                    and value.get("kind") == "progress_page"):
                return pager.handle(data)
            if permission_relay is not None:
                return permission_relay.handle_card(data)
            return None

        websocket = build_websocket_client(
            app_id=settings.app_id,
            app_secret=settings.app_secret,
            callback=handler.handle,
            card_callback=(
                handle_card if pager is not None or permission_relay is not None else None
            ),
        )
    except Exception:
        store.close()
        raise

    stopped = threading.Event()

    def stop(_signum: int | None = None, _frame: object | None = None) -> None:
        if stopped.is_set():
            return
        stopped.set()
        if permission_relay is not None:
            permission_relay.stop()
        service.stop()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    service.start(fail_interrupted=os.environ.get("FEISHU_RESIDENT_INSTANCE") is not None)
    if permission_relay is not None:
        try:
            permission_relay.start()
        except Exception:
            service.stop()
            raise

    def run_websocket() -> None:
        try:
            websocket.start()
        except Exception:
            LOGGER.exception("Feishu WebSocket stopped unexpectedly")
        finally:
            if not stopped.is_set():
                _thread.interrupt_main()

    websocket_thread = threading.Thread(
        target=run_websocket,
        name="feishu-websocket",
        daemon=True,
    )
    websocket_thread.start()
    writer.write({"event": "ready"})

    def report_health() -> None:
        previous = None
        while not stopped.wait(5):
            connected = websocket_thread.is_alive() and websocket_connected(websocket)
            if connected != previous:
                LOGGER.info("Feishu WebSocket connected=%s", connected)
                previous = connected
            try:
                writer.write({"event": "health", "connected": connected})
            except (BrokenPipeError, OSError):
                return

    if os.environ.get("FEISHU_BRIDGE_HEALTH_FILE"):
        threading.Thread(target=report_health, name="feishu-health", daemon=True).start()

    try:
        for line in sys.stdin:
            request: object = None
            try:
                if len(line.encode("utf-8")) > _MAX_PROTOCOL_LINE_BYTES:
                    raise ValueError("bridge request exceeds the protocol line limit")
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError("bridge request must be a JSON object")
                request_id = request["id"]
                method = request["method"]
                params = request.get("params", {})
                if (
                    not isinstance(request_id, int)
                    or isinstance(request_id, bool)
                    or not 0 <= request_id <= _MAX_REQUEST_ID
                    or not isinstance(method, str)
                    or not isinstance(params, dict)
                ):
                    raise ValueError("bridge request has an invalid shape")
                if method not in _EXPECTED_PARAMS or set(params) != _EXPECTED_PARAMS[method]:
                    raise ValueError("bridge request has invalid parameters")
                correlation_id = _validate_correlation_id(params["correlation_id"])
                if method == "mark_delivered":
                    result = {
                        "marked": service.mark_delivered(correlation_id),
                    }
                elif method == "read_image":
                    image = service.read_image(correlation_id)
                    result = {
                        "data": image.data,
                        "mime_type": image.mime_type,
                        "byte_length": image.byte_length,
                    }
                elif method == "reply":
                    text = params["text"]
                    if not isinstance(text, str) or not text.strip():
                        raise ValueError("bridge reply text must be a non-empty string")
                    if len(text) > _MAX_REPLY_CHARS:
                        raise ValueError("bridge reply text exceeds the protocol limit")
                    result = {
                        "status": service.reply(
                            correlation_id,
                            text,
                        )
                    }
                else:
                    raise ValueError(f"unknown bridge method: {method}")
                writer.write({"id": request_id, "result": result})
            except Exception as exc:
                LOGGER.exception("bridge request failed")
                writer.write(
                    {
                        "id": request.get("id") if isinstance(request, dict) else None,
                        "error": str(exc),
                    }
                )
    except KeyboardInterrupt:
        if not stopped.is_set():
            raise RuntimeError("Feishu WebSocket stopped unexpectedly") from None
    finally:
        stop()


def main() -> None:
    try:
        run()
    except ConfigError as exc:
        raise SystemExit(f"configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
